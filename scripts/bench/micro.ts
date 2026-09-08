/**
 * @fileoverview In-process microbenchmark for CollegeDB's own overhead.
 *
 * The sandbox matrix measures end-to-end latency against real databases in
 * Docker, where run-to-run variance on a loaded host reaches 100% on the bulk
 * scenarios. That is fine for comparing backends and useless for answering
 * "did the router get slower", which is a question about CPU in this process.
 *
 * This harness answers that one. Providers are the thinnest possible fakes
 * defined right here rather than imported, so the number is CollegeDB's
 * routing cost and nothing else, and so the same file can run unchanged
 * against an older checkout:
 *
 * ```
 * bun run scripts/bench/micro.ts
 * COLLEGEDB_SRC=/tmp/baseline/src/index.ts bun run scripts/bench/micro.ts --json
 * ```
 *
 * @author Gregory Mitchell
 * @since 1.4.0
 */

import type { CollegeDBConfig, KVListResult, KVStorage, PreparedStatement, QueryResult, SQLDatabase } from '../../src/types';

const SRC = process.env.COLLEGEDB_SRC ?? new URL('../../src/index.ts', import.meta.url).pathname;
const REPS = Number(process.env.BENCH_REPS ?? 7);
const JSON_OUT = process.argv.includes('--json');
const ONLY = process.argv.find((arg) => arg.startsWith('--only='))?.slice('--only='.length);

const api = (await import(SRC)) as Record<string, any>;

const ROW = { id: 'row', name: 'Row', email: 'row@example.local', created_at: 1 };

/** Counts what the router asked of the outside world, so a win can be attributed. */
const io = { kvGet: 0, kvPut: 0, kvDelete: 0, kvList: 0, sqlPrepare: 0, sqlExec: 0 };

function resetIo(): void {
	io.kvGet = 0;
	io.kvPut = 0;
	io.kvDelete = 0;
	io.kvList = 0;
	io.sqlPrepare = 0;
	io.sqlExec = 0;
}

function fakeKV(): KVStorage {
	const store = new Map<string, string>();

	return {
		async get<T = unknown>(key: string, type?: 'text' | 'json'): Promise<any> {
			io.kvGet++;
			const raw = store.get(key);
			if (raw === undefined) return null;
			return type === 'json' ? (JSON.parse(raw) as T) : raw;
		},
		async put(key: string, value: string): Promise<void> {
			io.kvPut++;
			store.set(key, value);
		},
		async delete(key: string): Promise<void> {
			io.kvDelete++;
			store.delete(key);
		},
		async list(options?: { prefix?: string; cursor?: string; limit?: number }): Promise<KVListResult> {
			io.kvList++;
			const prefix = options?.prefix ?? '';
			const keys = [...store.keys()].filter((key) => key.startsWith(prefix)).map((name) => ({ name }));
			return { keys, list_complete: true };
		}
	};
}

function fakeShard(): SQLDatabase {
	const statement: PreparedStatement = {
		bind: () => statement,
		async run<T = Record<string, unknown>>(): Promise<QueryResult<T>> {
			io.sqlExec++;
			return { success: true, results: [ROW as T], meta: { duration: 0, last_row_id: 1, changes: 1 } };
		},
		async all<T = Record<string, unknown>>(): Promise<QueryResult<T>> {
			io.sqlExec++;
			return { success: true, results: [ROW as T], meta: { duration: 0 } };
		},
		async first<T = Record<string, unknown>>(): Promise<T | null> {
			io.sqlExec++;
			return ROW as T;
		}
	};

	return {
		prepare(): PreparedStatement {
			io.sqlPrepare++;
			return statement;
		}
	};
}

/** A shard whose reads return nothing, for the miss and fanout-fallback paths. */
function emptyShard(): SQLDatabase {
	const statement: PreparedStatement = {
		bind: () => statement,
		async run<T = Record<string, unknown>>(): Promise<QueryResult<T>> {
			io.sqlExec++;
			return { success: true, results: [], meta: { duration: 0, changes: 0 } };
		},
		async all<T = Record<string, unknown>>(): Promise<QueryResult<T>> {
			io.sqlExec++;
			return { success: true, results: [], meta: { duration: 0 } };
		},
		async first(): Promise<null> {
			io.sqlExec++;
			return null;
		}
	};

	return {
		prepare(): PreparedStatement {
			io.sqlPrepare++;
			return statement;
		}
	};
}

function shards(count: number, factory: () => SQLDatabase = fakeShard): Record<string, SQLDatabase> {
	const out: Record<string, SQLDatabase> = {};
	for (let i = 0; i < count; i++) {
		out[`shard-${String.fromCharCode(97 + i)}`] = factory();
	}
	return out;
}

function configure(overrides: Partial<CollegeDBConfig> = {}, shardFactory: () => SQLDatabase = fakeShard): void {
	api.resetConfig();
	api.initialize({
		kv: fakeKV(),
		shards: shards(5, shardFactory),
		strategy: 'hash',
		disableAutoMigration: true,
		...overrides
	} as CollegeDBConfig);
}

interface Case {
	name: string;
	/** Operations performed per timed iteration, so results are per-op. */
	opsPerIteration: number;
	setup: () => Promise<void> | void;
	iteration: (i: number) => Promise<void>;
}

const INSERT_SQL = 'INSERT INTO users (id, name, email, created_at) VALUES (?, ?, ?, ?)';
const SELECT_SQL = 'SELECT id, name, email FROM users WHERE id = ?';
const UPDATE_SQL = 'UPDATE users SET name = ? WHERE id = ?';

const cases: Case[] = [
	{
		// The bulk_crud insert shape: every key is new, so it pays a KV miss, a
		// strategy run, and a KV write.
		name: 'write_new_key',
		opsPerIteration: 1,
		setup: () => configure(),
		iteration: async (i) => {
			const key = `new-${i}`;
			await api.run(key, INSERT_SQL, [key, 'Name', 'a@b.local', 1]);
		}
	},
	{
		// The bulk_crud update and delete shape: the mapping is already known, so
		// this is the path a warm cache is supposed to make free.
		name: 'write_known_key',
		opsPerIteration: 1,
		setup: async () => {
			configure();
			await api.run('warm-key', INSERT_SQL, ['warm-key', 'Name', 'a@b.local', 1]);
		},
		iteration: async () => {
			await api.run('warm-key', UPDATE_SQL, ['Renamed', 'warm-key']);
		}
	},
	{
		name: 'read_known_key',
		opsPerIteration: 1,
		setup: async () => {
			configure();
			await api.run('warm-key', INSERT_SQL, ['warm-key', 'Name', 'a@b.local', 1]);
		},
		iteration: async () => {
			await api.first('warm-key', SELECT_SQL, ['warm-key']);
		}
	},
	{
		// A lookup for something that was never written. On a public API this is
		// the most common request shape there is.
		name: 'read_unmapped_key',
		opsPerIteration: 1,
		setup: () => configure({}, emptyShard),
		iteration: async (i) => {
			await api.first(`absent-${i}`, SELECT_SQL, [`absent-${i}`]);
		}
	},
	{
		name: 'direct_shard_read',
		opsPerIteration: 1,
		setup: () => configure(),
		iteration: async () => {
			await api.allShard('shard-a', 'SELECT name FROM users LIMIT 25');
		}
	},
	{
		name: 'count_all_shards',
		opsPerIteration: 1,
		setup: () => configure(),
		iteration: async () => {
			await api.countAllShards('users');
		}
	},
	{
		name: 'all_all_shards',
		opsPerIteration: 1,
		setup: () => configure(),
		iteration: async () => {
			await api.allAllShards('SELECT COUNT(*) AS count FROM users');
		}
	},
	{
		name: 'insert_generated_id',
		opsPerIteration: 1,
		setup: () => configure(),
		iteration: async () => {
			await api.insertShard('shard-a', 'INSERT INTO users (name) VALUES (?) RETURNING id', ['Name']);
		}
	},
	{
		// 20 statements over 5 shards, matching the concurrency the harness uses
		// for its bulk phases.
		name: 'batch_20_writes',
		opsPerIteration: 20,
		setup: () => configure(),
		iteration: async (i) => {
			if (typeof api.batch !== 'function') return;
			await api.batch(
				new Array(20).fill(null).map((_, idx) => ({
					key: `batch-${i}-${idx}`,
					sql: INSERT_SQL,
					bindings: [`batch-${i}-${idx}`, 'Name', 'a@b.local', 1]
				}))
			);
		}
	},
	{
		name: 'planner_write',
		opsPerIteration: 1,
		setup: () => configure(),
		iteration: async (i) => {
			if (typeof api.query !== 'function') return;
			await api.query(INSERT_SQL, [`planned-${i}`, 'Name', 'a@b.local', 1]);
		}
	},
	{
		name: 'computed_placement_write',
		opsPerIteration: 1,
		setup: () => configure({ placement: 'computed' } as Partial<CollegeDBConfig>),
		iteration: async (i) => {
			const key = `computed-${i}`;
			await api.run(key, INSERT_SQL, [key, 'Name', 'a@b.local', 1]);
		}
	}
];

interface Measurement {
	name: string;
	nsPerOp: number;
	opsPerSecond: number;
	samplesNs: number[];
	io: typeof io;
	iterations: number;
}

/**
 * Times one case, reporting the median of {@link REPS} repetitions.
 *
 * The median rather than the mean because a GC pause inside one repetition is
 * not information about the router.
 */
async function measure(testCase: Case): Promise<Measurement> {
	const warmup = 500;
	const samplesNs: number[] = [];
	let counted = { ...io };

	// A fixed iteration count cannot measure both a 0.15 us call and a 5 us one:
	// at 2000 iterations the cheap case runs for 300 us total, where one GC pause
	// moves the result by a factor of three. Calibrate instead, so every
	// repetition covers the same wall time and every case gets the same
	// signal-to-noise.
	const targetRepMs = Number(process.env.BENCH_TARGET_MS ?? 60);
	await testCase.setup();
	for (let i = 0; i < warmup; i++) {
		await testCase.iteration(i);
	}
	const pilotStarted = process.hrtime.bigint();
	for (let i = 0; i < 300; i++) {
		await testCase.iteration(warmup + i);
	}
	const pilotNsPerIteration = Math.max(1, Number(process.hrtime.bigint() - pilotStarted) / 300);
	const iterations = Math.max(1_000, Math.min(2_000_000, Math.round((targetRepMs * 1e6) / pilotNsPerIteration)));

	for (let rep = 0; rep < REPS; rep++) {
		await testCase.setup();

		for (let i = 0; i < warmup; i++) {
			await testCase.iteration(i);
		}

		resetIo();
		const started = process.hrtime.bigint();
		for (let i = 0; i < iterations; i++) {
			await testCase.iteration(warmup + i);
		}
		const elapsedNs = Number(process.hrtime.bigint() - started);

		if (rep === 0) counted = { ...io };
		samplesNs.push(elapsedNs / (iterations * testCase.opsPerIteration));
	}

	const sorted = [...samplesNs].sort((left, right) => left - right);
	const nsPerOp = sorted[Math.floor(sorted.length / 2)]!;

	return {
		name: testCase.name,
		nsPerOp,
		opsPerSecond: 1e9 / nsPerOp,
		samplesNs: sorted,
		io: counted,
		iterations
	};
}

const selected = ONLY ? cases.filter((entry) => entry.name.includes(ONLY)) : cases;
const results: Measurement[] = [];

for (const testCase of selected) {
	results.push(await measure(testCase));
}

api.resetConfig();

if (JSON_OUT) {
	console.log(JSON.stringify({ src: SRC, reps: REPS, results }, null, 2));
} else {
	console.log(`source: ${SRC}   median of ${REPS} reps`);
	console.log('');
	console.log(`${'case'.padEnd(26)}${'us/op'.padStart(9)}${'ops/sec'.padStart(12)}${'spread'.padStart(9)}   io per rep`);
	console.log('-'.repeat(100));

	for (const result of results) {
		const spread = ((result.samplesNs[result.samplesNs.length - 1]! / result.samplesNs[0]! - 1) * 100).toFixed(0);
		const counters = Object.entries(result.io)
			.filter(([, value]) => value > 0)
			.map(([name, value]) => `${name}=${value}`)
			.join(' ');

		console.log(
			`${result.name.padEnd(26)}${(result.nsPerOp / 1000).toFixed(2).padStart(9)}${Math.round(result.opsPerSecond)
				.toLocaleString('en-US')
				.padStart(12)}${`${spread}%`.padStart(9)}   ${counters}`
		);
	}
}
