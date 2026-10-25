import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { Worker } from 'node:worker_threads';
import { buildGuardedInsert, buildInsert, buildShardSidecarSchema } from '../../src/query';
import { buildFenceRaise } from '../../src/topology';

export interface FenceRaceReport {
	iterations: number;
	seed: number;
	/** Writer inserts that landed before the fence went up */
	landed: number;
	/** Writer inserts the fence turned away */
	rejected: number;
	orders: Record<string, number>;
	/** Rows that exist although the census taken after the fence raise did not see them */
	violations: string[];
}

const WORKER_SOURCE = `
const { parentPort, workerData } = require('node:worker_threads');
const { DatabaseSync } = require('node:sqlite');
const db = new DatabaseSync(workerData.path);
db.exec('PRAGMA busy_timeout = 30000');
const ctl = new Int32Array(workerData.sab);
const FENCE_DONE = 0, WRITER_DONE = 1;

function waitFor(index, ms) {
	const end = Date.now() + ms;
	while (Atomics.load(ctl, index) === 0 && Date.now() < end) Atomics.wait(ctl, index, 0, 2);
}
function set(index) {
	Atomics.store(ctl, index, 1);
	Atomics.notify(ctl, index);
}

parentPort.on('message', (job) => {
	const out = { id: job.id };
	try {
		if (job.role === 'fencer') {
			if (job.order === 'writer-first') waitFor(WRITER_DONE, 10000);
			for (const query of job.raise) db.prepare(query.sql).run(...query.bindings);
			out.seen = Number(db.prepare(job.census.sql).get(...job.census.bindings).seen);
			set(FENCE_DONE);
		} else {
			if (job.order === 'fence-first') waitFor(FENCE_DONE, 10000);
			try {
				out.changes = Number(db.prepare(job.stmt.sql).run(...job.stmt.bindings).changes);
			} catch (error) {
				out.error = String((error && error.message) || error);
			}
			set(WRITER_DONE);
		}
	} catch (error) {
		out.error = String((error && error.message) || error);
		set(FENCE_DONE);
		set(WRITER_DONE);
	}
	parentPort.postMessage(out);
});
`;

const ORDERS = ['writer-first', 'fence-first', 'simultaneous', 'simultaneous'] as const;

function mulberry32(seed: number): () => number {
	let state = seed >>> 0;
	return () => {
		state = (state + 0x6d2b79f5) >>> 0;
		let t = Math.imul(state ^ (state >>> 15), state | 1);
		t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
}

function request(worker: Worker, job: Record<string, unknown>): Promise<{ id: number; seen?: number; changes?: number; error?: string }> {
	return new Promise((resolve, reject) => {
		const onMessage = (message: { id: number }) => {
			if (message.id === job.id) {
				worker.off('message', onMessage);
				worker.off('error', reject);
				resolve(message);
			}
		};
		worker.on('message', onMessage);
		worker.once('error', reject);
		worker.postMessage(job);
	});
}

/**
 * Races a guarded insert from a writer on an older topology generation against
 * the fence raise and the census that follows it, in two threads that each own a
 * connection to one WAL file.
 *
 * The invariant is the one `pruneEpochs` relies on: once the fence is up and the
 * census has run, no row can appear that the census did not see. A row that exists
 * at the end although the census read zero rows for its key is a violation. With
 * `control: 'unfenced'` the writer uses a plain insert, the fence cannot stop it,
 * and violations must show up, or the check proves nothing.
 */
export async function runFenceRace(options: { iterations: number; seed?: number; control?: 'unfenced' }): Promise<FenceRaceReport> {
	const seed = options.seed ?? 20261025;
	const random = mulberry32(seed);
	const dir = mkdtempSync(join(tmpdir(), 'collegedb-fence-'));
	const path = join(dir, 'shard.db');

	const main = new DatabaseSync(path);
	main.exec('PRAGMA journal_mode = WAL');
	main.exec('PRAGMA busy_timeout = 30000');
	main.exec('CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT)');
	for (const statement of buildShardSidecarSchema('sqlite')) {
		main.exec(statement);
	}

	const sab = new SharedArrayBuffer(2 * 4);
	const ctl = new Int32Array(sab);
	const fencer = new Worker(WORKER_SOURCE, { eval: true, workerData: { path, sab } });
	const writer = new Worker(WORKER_SOURCE, { eval: true, workerData: { path, sab } });

	const report: FenceRaceReport = { iterations: options.iterations, seed, landed: 0, rejected: 0, orders: {}, violations: [] };

	try {
		for (let i = 0; i < options.iterations; i++) {
			const key = `fence-${i}`;
			const generation = i + 1;
			const order = ORDERS[Math.floor(random() * ORDERS.length)]!;
			report.orders[order] = (report.orders[order] ?? 0) + 1;
			ctl.fill(0);

			const stmt =
				options.control === 'unfenced'
					? buildInsert('items', { id: key, name: 'W' }, { dialect: 'sqlite' })
					: buildGuardedInsert(
							'items',
							{ id: key, name: 'W' },
							{ scope: 'default', table: 'items', key, generation },
							{ dialect: 'sqlite' }
						);

			const [fence, write] = await Promise.all([
				request(fencer, {
					id: i,
					role: 'fencer',
					order,
					raise: buildFenceRaise(generation + 1, 'sqlite'),
					census: { sql: 'SELECT COUNT(*) AS seen FROM items WHERE id = ?', bindings: [key] }
				}),
				request(writer, { id: i, role: 'writer', order, stmt })
			]);

			if (fence.error || write.error) {
				report.violations.push(`${order}/${key}: thread error ${fence.error ?? write.error}`);
				continue;
			}

			const exists = Number((main.prepare('SELECT COUNT(*) AS n FROM items WHERE id = ?').get(key) as { n: number }).n) > 0;
			const acked = write.changes === 1;
			if (acked) report.landed++;
			if (write.changes === 0) report.rejected++;

			if (exists && fence.seen === 0) {
				report.violations.push(`${order}/${key}: row landed after the census`);
			}
			if (acked !== exists) {
				report.violations.push(`${order}/${key}: writer reported ${write.changes} changes but the row ${exists ? 'exists' : 'is missing'}`);
			}
		}
	} finally {
		await Promise.all([fencer.terminate(), writer.terminate()]);
		main.close();
		rmSync(dir, { recursive: true, force: true });
	}

	return report;
}
