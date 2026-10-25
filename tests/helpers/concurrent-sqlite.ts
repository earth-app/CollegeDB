import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { Worker } from 'node:worker_threads';
import { bindable, encodeRow } from '../../src/migrations';
import { createSQLiteProvider, type SQLiteClientLike } from '../../src/providers';
import {
	buildCutoverStatements,
	buildDelete,
	buildGuardedInsert,
	buildGuardedUpsert,
	buildInsert,
	buildShardSidecarSchema,
	buildUpdate,
	type BuiltQuery
} from '../../src/query';
import type { SQLDatabase } from '../../src/types';

const SAFE_MIN = BigInt(Number.MIN_SAFE_INTEGER);
const SAFE_MAX = BigInt(Number.MAX_SAFE_INTEGER);

function narrow(row: Record<string, unknown>): Record<string, unknown> {
	const out: Record<string, unknown> = {};
	for (const [name, value] of Object.entries(row)) {
		out[name] = typeof value === 'bigint' && value >= SAFE_MIN && value <= SAFE_MAX ? Number(value) : value;
	}
	return out;
}

function clean(bindings: unknown[]): unknown[] {
	return bindings.map((value) => (value === undefined ? null : typeof value === 'boolean' ? Number(value) : value));
}

/**
 * Wraps a `node:sqlite` handle for `createSQLiteProvider`. Integers outside the
 * safe range come back as bigint, so 64-bit values survive a read and a bind.
 */
export function createNodeSqliteClient(db: DatabaseSync): SQLiteClientLike {
	return {
		prepare(sql: string) {
			const statement = db.prepare(sql);
			statement.setReadBigInts(true);
			return {
				run: (...bindings: any[]) => {
					const result = statement.run(...(clean(bindings) as any[]));
					return { changes: Number(result.changes), lastInsertRowid: Number(result.lastInsertRowid) };
				},
				all: (...bindings: any[]) => (statement.all(...(clean(bindings) as any[])) as Record<string, unknown>[]).map(narrow),
				get: (...bindings: any[]) => {
					const row = statement.get(...(clean(bindings) as any[])) as Record<string, unknown> | undefined;
					return row === undefined ? undefined : narrow(row);
				}
			};
		}
	};
}

/** A transactional SQLite shard over one `node:sqlite` handle. */
export function createNodeSqliteProvider(db: DatabaseSync): SQLDatabase {
	return createSQLiteProvider(createNodeSqliteClient(db), { singleConnection: true });
}

/** Opens a handle with a busy timeout, in memory unless a path is given. */
export function openNodeSqlite(path = ':memory:'): DatabaseSync {
	const db = new DatabaseSync(path);
	db.exec('PRAGMA busy_timeout = 10000');
	return db;
}

export type RaceKind = 'update' | 'delete' | 'insert' | 'upsert';

export interface RaceReport {
	kind: RaceKind;
	iterations: number;
	seed: number;
	/** Cutovers that moved the row (archive and tombstone written) */
	hits: number;
	/** Cutovers that missed because the writer got there first */
	misses: number;
	/** Writer statements that changed a row */
	writerAcked: number;
	/** Writer statements that changed nothing, which the invariant ties to a tombstone */
	writerFenced: number;
	/** Writer statements that errored (a duplicate key is expected on insert) */
	writerErrors: number;
	orders: Record<string, number>;
	violations: string[];
}

const WORKER_SOURCE = `
const { parentPort, workerData } = require('node:worker_threads');
const { DatabaseSync } = require('node:sqlite');
const db = new DatabaseSync(workerData.path);
db.exec('PRAGMA busy_timeout = 30000');
const ctl = new Int32Array(workerData.sab);
const LOCKED = 0, WRITER_STARTED = 1, WRITER_DONE = 2, CUTOVER_DONE = 3;

function waitFor(index, ms) {
	const end = Date.now() + ms;
	while (Atomics.load(ctl, index) === 0 && Date.now() < end) Atomics.wait(ctl, index, 0, 2);
}
function set(index) {
	Atomics.store(ctl, index, 1);
	Atomics.notify(ctl, index);
}
function exec(query) {
	return Number(db.prepare(query.sql).run(...query.bindings).changes);
}
function spin(ms) {
	const end = Date.now() + ms;
	while (Date.now() < end) {}
}

parentPort.on('message', (job) => {
	const out = { id: job.id };
	try {
		if (job.role === 'cutover') {
			if (job.order === 'writer-first') waitFor(WRITER_DONE, 10000);
			db.exec('BEGIN');
			try {
				out.archiveChanges = exec(job.archive);
				set(LOCKED);
				if (job.order === 'overlap') {
					waitFor(WRITER_STARTED, 3000);
					spin(job.spin);
				}
				out.deleteChanges = exec(job.del);
				db.exec('COMMIT');
			} catch (error) {
				try { db.exec('ROLLBACK'); } catch {}
				throw error;
			} finally {
				set(LOCKED);
				set(CUTOVER_DONE);
			}
		} else {
			if (job.order === 'overlap') {
				waitFor(LOCKED, 5000);
				set(WRITER_STARTED);
			} else if (job.order === 'cutover-first') {
				waitFor(CUTOVER_DONE, 10000);
			}
			try {
				out.changes = exec(job.stmt);
			} catch (error) {
				out.error = String(error && error.message || error);
			}
			set(WRITER_DONE);
		}
	} catch (error) {
		out.error = String(error && error.message || error);
		if (job.role === 'writer') set(WRITER_DONE);
	}
	parentPort.postMessage(out);
});
`;

const ORDERS = ['writer-first', 'overlap', 'overlap', 'cutover-first', 'simultaneous'] as const;
type Order = (typeof ORDERS)[number];

function mulberry32(seed: number): () => number {
	let state = seed >>> 0;
	return () => {
		state = (state + 0x6d2b79f5) >>> 0;
		let t = Math.imul(state ^ (state >>> 15), state | 1);
		t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
}

interface JobResult {
	id: number;
	archiveChanges?: number;
	deleteChanges?: number;
	changes?: number;
	error?: string;
}

function request(worker: Worker, job: Record<string, unknown>): Promise<JobResult> {
	return new Promise((resolve, reject) => {
		const onMessage = (message: JobResult) => {
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

const ITEMS_SCHEMA = 'CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, qty INTEGER, note TEXT, data BLOB)';

/**
 * Races a router-style write against the source cutover transaction, once per
 * iteration, in two worker threads that each own a connection to one WAL file.
 *
 * The SQL comes from the library builders, so what runs is what the router
 * would run. `node:sqlite` is synchronous, which is why threads are the only way
 * to get statements that really overlap. Barriers on a shared buffer pick one of
 * four orderings per iteration: writer first, writer blocked inside the open
 * cutover transaction, writer after the cutover, and both released together.
 *
 * Each iteration checks the write-ordering invariant on the final state: no live
 * row beside a tombstone, an archive equal to the original row when the cutover
 * hit, and every acknowledged write present on the owner.
 */
export async function runCutoverRace(options: {
	kind: RaceKind;
	iterations: number;
	seed?: number;
	/**
	 * Breaks the protocol on purpose so a run can show the checks would notice:
	 * `unguarded-insert` drops the tombstone guard from the insert, and
	 * `unconditional-cutover` compares only the key in the cutover predicate.
	 */
	control?: 'unguarded-insert' | 'unconditional-cutover';
}): Promise<RaceReport> {
	const seed = options.seed ?? 20261024;
	const random = mulberry32(seed);
	const dir = mkdtempSync(join(tmpdir(), 'collegedb-race-'));
	const path = join(dir, 'shard.db');

	const main = new DatabaseSync(path);
	main.exec('PRAGMA journal_mode = WAL');
	main.exec('PRAGMA busy_timeout = 30000');
	main.exec(ITEMS_SCHEMA);
	for (const statement of buildShardSidecarSchema('sqlite')) {
		main.exec(statement);
	}
	const client = createNodeSqliteClient(main);

	const sab = new SharedArrayBuffer(4 * 4);
	const ctl = new Int32Array(sab);
	const cutover = new Worker(WORKER_SOURCE, { eval: true, workerData: { path, sab } });
	const writer = new Worker(WORKER_SOURCE, { eval: true, workerData: { path, sab } });

	const report: RaceReport = {
		kind: options.kind,
		iterations: options.iterations,
		seed,
		hits: 0,
		misses: 0,
		writerAcked: 0,
		writerFenced: 0,
		writerErrors: 0,
		orders: {},
		violations: []
	};

	const original = {
		name: 'orig-é中',
		qty: 9223372036854775807n,
		note: null,
		data: new Uint8Array([0, 1, 2, 255])
	};

	try {
		for (let i = 0; i < options.iterations; i++) {
			const key = `race-${i}`;
			main
				.prepare('INSERT INTO items (id, name, qty, note, data) VALUES (?, ?, ?, ?, ?)')
				.run(key, original.name, original.qty, null, original.data);

			const row = client.prepare!('SELECT * FROM items WHERE id = ?').get!(key) as Record<string, unknown>;
			const encoded = encodeRow(row);
			const statements = buildCutoverStatements({
				table: 'items',
				pkColumn: 'id',
				columns:
					options.control === 'unconditional-cutover'
						? bindable(encoded, 'sqlite').filter(([name]) => name === 'id')
						: bindable(encoded, 'sqlite'),
				scope: 'default',
				key,
				opId: `op-${i}`,
				generation: 1,
				movedTo: 'db-b',
				payload: encoded.payload,
				archivedAt: 1,
				dialect: 'sqlite'
			});

			const guard = { scope: 'default', table: 'items', key };
			const stmt: BuiltQuery =
				options.kind === 'update'
					? buildUpdate('items', { name: 'W' }, { id: key }, { dialect: 'sqlite' })
					: options.kind === 'delete'
						? buildDelete('items', { id: key }, { dialect: 'sqlite' })
						: options.kind === 'insert'
							? options.control === 'unguarded-insert'
								? buildInsert('items', { id: key, name: 'I' }, { dialect: 'sqlite' })
								: buildGuardedInsert('items', { id: key, name: 'I' }, guard, { dialect: 'sqlite' })
							: buildGuardedUpsert('items', { id: key, name: 'U' }, 'id', guard, { dialect: 'sqlite' });

			const order: Order = ORDERS[Math.floor(random() * ORDERS.length)]!;
			report.orders[order] = (report.orders[order] ?? 0) + 1;
			ctl.fill(0);

			const [cut, write] = await Promise.all([
				request(cutover, {
					id: i,
					role: 'cutover',
					order,
					spin: Math.floor(random() * 3),
					archive: statements.archive,
					del: statements.delete
				}),
				request(writer, { id: i, role: 'writer', order, stmt })
			]);

			check(report, options.kind, key, order, main, client, encoded.payload, cut, write);
		}
	} finally {
		await Promise.all([cutover.terminate(), writer.terminate()]);
		main.close();
		rmSync(dir, { recursive: true, force: true });
	}

	return report;
}

function check(
	report: RaceReport,
	kind: RaceKind,
	key: string,
	order: Order,
	main: DatabaseSync,
	client: SQLiteClientLike,
	originalPayload: string,
	cut: JobResult,
	write: JobResult
): void {
	const fail = (message: string) => report.violations.push(`${kind}/${order}/${key}: ${message}`);

	const row = client.prepare!('SELECT * FROM items WHERE id = ?').get!(key) as Record<string, unknown> | undefined;
	const tomb = main.prepare('SELECT op_id, payload FROM collegedb_retired WHERE primary_key = ?').get(key) as
		{ op_id: string; payload: string } | undefined;

	if (cut.error) {
		fail(`cutover errored: ${cut.error}`);
		return;
	}

	const hit = tomb !== undefined;
	if (hit) {
		report.hits++;
	} else {
		report.misses++;
	}

	if (row && hit) {
		fail('ghost: a live row sits beside the tombstone');
	}
	if (hit) {
		if (tomb.payload !== originalPayload) {
			fail('archive payload differs from the original row');
		}
		if (cut.archiveChanges !== 1 || cut.deleteChanges !== 1) {
			fail(`hit but archive/delete changed ${cut.archiveChanges}/${cut.deleteChanges}`);
		}
	} else if (cut.archiveChanges !== 0 || cut.deleteChanges !== 0) {
		fail(`miss but archive/delete changed ${cut.archiveChanges}/${cut.deleteChanges}`);
	}

	const acked = write.error === undefined && (write.changes ?? 0) > 0;
	const fenced = write.error === undefined && write.changes === 0;
	if (acked) report.writerAcked++;
	if (fenced) report.writerFenced++;
	if (write.error !== undefined) {
		report.writerErrors++;
		if (!(kind === 'insert' && /UNIQUE|constraint/i.test(write.error))) {
			fail(`unexpected writer error: ${write.error}`);
		}
	}

	if (fenced && !hit) {
		fail('writer changed zero rows but no tombstone explains it');
	}
	if (!hit && !row && !(kind === 'delete' && acked)) {
		fail('row lost without an acknowledged delete');
	}

	if (kind === 'update' && acked && (hit || row?.name !== 'W')) {
		fail('acknowledged update is not on the owner');
	}
	if (kind === 'upsert' && acked && (hit || row?.name !== 'U')) {
		fail('acknowledged upsert is not on the owner');
	}
	if (kind === 'delete' && acked && (hit || row)) {
		fail('acknowledged delete left the row or archived it');
	}
	if (kind === 'insert' && acked) {
		fail('insert over an existing key was acknowledged');
	}
}
