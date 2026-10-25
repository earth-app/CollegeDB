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
 * Pass `--cpu` (or set `BENCH_CPU=1`) to report process CPU time per op instead
 * of wall time, which a busy neighbor on the host inflates far less. The
 * p50/p95 columns stay wall time.
 *
 * @author Gregory Mitchell
 * @since 1.3.0
 */

import type { CollegeDBConfig, KVListResult, KVStorage, PreparedStatement, QueryResult, SQLDatabase } from '../../src/types';

const SRC = process.env.COLLEGEDB_SRC ?? new URL('../../src/index.ts', import.meta.url).pathname;
const REPS = Number(process.env.BENCH_REPS ?? 7);
const LAT_SAMPLES = Number(process.env.BENCH_LAT_SAMPLES ?? 4000);
const JSON_OUT = process.argv.includes('--json');
const CPU_MODE = process.argv.includes('--cpu') || process.env.BENCH_CPU === '1';
const ONLY = process.argv.find((arg) => arg.startsWith('--only='))?.slice('--only='.length);

const api = (await import(SRC)) as Record<string, any>;

const ROW = { id: 'row', name: 'Row', email: 'row@example.local', created_at: 1 };

/** Counts what the router asked of the outside world, so a win can be attributed. */
const io = { kvGet: 0, kvPut: 0, kvDelete: 0, kvList: 0, sqlPrepare: 0, sqlExec: 0, errors: 0, backgroundRejections: 0 };

// background bookkeeping rejects while KV is down; count it instead of crashing
process.on('unhandledRejection', () => {
	io.backgroundRejections++;
});

function resetIo(): void {
	io.kvGet = 0;
	io.kvPut = 0;
	io.kvDelete = 0;
	io.kvList = 0;
	io.sqlPrepare = 0;
	io.sqlExec = 0;
	io.errors = 0;
	io.backgroundRejections = 0;
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
		},

		// Counted as one operation each, because that is what they are on the
		// stores that implement them: Redis and Valkey issue a single MGET or
		// MSET. Without these the batch path silently falls back to one key at a
		// time and the counters would not show it.
		getMany: (async (keys: string[], type?: 'text' | 'json') => {
			io.kvGet++;
			return keys.map((key) => {
				const raw = store.get(key);
				if (raw === undefined) return null;
				return type === 'json' ? JSON.parse(raw) : raw;
			});
		}) as KVStorage['getMany'],
		async putMany(entries: Array<{ key: string; value: string }>): Promise<void> {
			io.kvPut++;
			for (const entry of entries) {
				store.set(entry.key, entry.value);
			}
		},
		async deleteMany(keys: string[]): Promise<void> {
			io.kvDelete++;
			for (const key of keys) {
				store.delete(key);
			}
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

/** A KV whose reads and writes throw while `state.down` is set, to measure what survives an outage. */
function downableKV(state: { down: boolean }): KVStorage {
	const inner = fakeKV() as unknown as Record<string, (...args: unknown[]) => Promise<unknown>>;
	const guarded: Record<string, unknown> = {};

	for (const name of Object.keys(inner)) {
		guarded[name] = async (...args: unknown[]) => {
			if (state.down) throw new Error('kv unavailable');
			return await inner[name]!(...args);
		};
	}

	return guarded as unknown as KVStorage;
}

/** Every call returns a promise that never settles. */
function hungKV(): KVStorage {
	const hang = () => new Promise<never>(() => {});

	return {
		get: () => {
			io.kvGet++;
			return hang();
		},
		put: () => {
			io.kvPut++;
			return hang();
		},
		delete: hang,
		list: hang,
		getMany: () => {
			io.kvGet++;
			return hang();
		},
		putMany: () => {
			io.kvPut++;
			return hang();
		},
		deleteMany: hang
	} as unknown as KVStorage;
}

const HUNG_WAIT_MS = 50;

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
	/** Name of the case that does the same work straight against the provider, to report router overhead. */
	baseline?: string;
	/** Fixed count for warmup, timed iterations and latency samples; skips the calibration pilot. */
	iterationsOverride?: number;
	setup: () => Promise<void> | void;
	iteration: (i: number) => Promise<void>;
}

const MULTI_KEYS = new Array(100).fill(null).map((_, index) => `multi-${index}`);

async function seedMultiKeys(): Promise<void> {
	for (const key of MULTI_KEYS) {
		await api.run(key, INSERT_SQL, [key, 'Name', 'a@b.local', 1]);
	}
}

/** One shared KV across iterations, so a cold isolate finds the state a warm one left behind. */
let coldKv: KVStorage | null = null;
const kvState = { down: false };

const INSERT_SQL = 'INSERT INTO users (id, name, email, created_at) VALUES (?, ?, ?, ?)';
const SELECT_SQL = 'SELECT id, name, email FROM users WHERE id = ?';
const UPDATE_SQL = 'UPDATE users SET name = ? WHERE id = ?';

const directShard = fakeShard();

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
		baseline: 'direct_write',
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
		baseline: 'direct_read',
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
	},
	{
		// The same statements straight at the provider: the floor CollegeDB's
		// overhead is measured against.
		name: 'direct_write',
		opsPerIteration: 1,
		setup: () => undefined,
		iteration: async () => {
			await directShard.prepare(UPDATE_SQL).bind('Renamed', 'warm-key').run();
		}
	},
	{
		name: 'direct_read',
		opsPerIteration: 1,
		setup: () => undefined,
		iteration: async () => {
			await directShard.prepare(SELECT_SQL).bind('warm-key').first();
		}
	},
	{
		name: 'direct_100_reads',
		opsPerIteration: 100,
		setup: () => undefined,
		iteration: async () => {
			for (const key of MULTI_KEYS) {
				await directShard.prepare(SELECT_SQL).bind(key).first();
			}
		}
	},
	{
		name: 'multi_read_100_sequential',
		opsPerIteration: 100,
		baseline: 'direct_100_reads',
		setup: async () => {
			configure();
			await seedMultiKeys();
		},
		iteration: async () => {
			for (const key of MULTI_KEYS) {
				await api.first(key, SELECT_SQL, [key]);
			}
		}
	},
	{
		name: 'multi_read_100_concurrent',
		opsPerIteration: 100,
		baseline: 'direct_100_reads',
		setup: async () => {
			configure();
			await seedMultiKeys();
		},
		iteration: async () => {
			await Promise.all(MULTI_KEYS.map((key) => api.first(key, SELECT_SQL, [key])));
		}
	},
	{
		name: 'multi_read_100_batch',
		opsPerIteration: 100,
		baseline: 'direct_100_reads',
		setup: async () => {
			configure();
			await seedMultiKeys();
		},
		iteration: async () => {
			if (typeof api.batch !== 'function') return;
			await api.batch(MULTI_KEYS.map((key) => ({ key, sql: SELECT_SQL, bindings: [key] })));
		}
	},
	{
		// A fresh isolate against KV state a warm one left behind: pays the
		// initialize bookkeeping and the first mapping read.
		name: 'cold_isolate_read_kv_mapped',
		opsPerIteration: 1,
		setup: async () => {
			coldKv = fakeKV();
			configure({ kv: coldKv });
			await api.run('warm-key', INSERT_SQL, ['warm-key', 'Name', 'a@b.local', 1]);
		},
		iteration: async () => {
			api.resetConfig();
			api.initialize({ kv: coldKv!, shards: shards(5), strategy: 'hash', disableAutoMigration: true } as CollegeDBConfig);
			await api.first('warm-key', SELECT_SQL, ['warm-key']);
		}
	},
	{
		name: 'cold_isolate_read_computed',
		opsPerIteration: 1,
		setup: async () => {
			coldKv = fakeKV();
			configure({ kv: coldKv, placement: 'computed' } as Partial<CollegeDBConfig>);
			await api.run('warm-key', INSERT_SQL, ['warm-key', 'Name', 'a@b.local', 1]);
		},
		iteration: async () => {
			api.resetConfig();
			api.initialize({
				kv: coldKv!,
				shards: shards(5),
				strategy: 'hash',
				placement: 'computed',
				disableAutoMigration: true
			} as CollegeDBConfig);
			await api.first('warm-key', SELECT_SQL, ['warm-key']);
		}
	},
	...(['computed', 'mapped'] as const).map((layout): Case => {
		const placement = layout === 'computed' ? { placement: 'computed' } : {};
		let store: KVStorage | null = null;

		return {
			// fresh isolate with KV down, so every read reaches KV; errors counts failed reads
			name: `kv_down_cold_${layout}_read`,
			opsPerIteration: 1,
			setup: async () => {
				kvState.down = false;
				store = downableKV(kvState);
				configure({ kv: store, ...placement } as Partial<CollegeDBConfig>);
				await api.run('warm-key', INSERT_SQL, ['warm-key', 'Name', 'a@b.local', 1]);
				kvState.down = true;
			},
			iteration: async () => {
				api.resetConfig();
				try {
					api.initialize({ kv: store!, shards: shards(5), strategy: 'hash', disableAutoMigration: true, ...placement } as CollegeDBConfig);
					await api.first('warm-key', SELECT_SQL, ['warm-key']);
				} catch {
					io.errors++;
				}
			}
		};
	}),
	{
		// Every KV call hangs. The 50 ms race stands in for a caller-side timeout; a timer win counts as an error.
		name: 'kv_hung_cold_read',
		opsPerIteration: 1,
		iterationsOverride: 20,
		setup: () => {},
		iteration: async () => {
			api.resetConfig();
			api.initialize({ kv: hungKV(), shards: shards(5), strategy: 'hash', disableAutoMigration: true, kvTimeoutMs: 5 } as CollegeDBConfig);

			let timer: ReturnType<typeof setTimeout> | undefined;
			const timedOut = new Promise<'timeout'>((resolve) => {
				timer = setTimeout(() => resolve('timeout'), HUNG_WAIT_MS);
			});

			try {
				const outcome = await Promise.race([api.first('warm-key', SELECT_SQL, ['warm-key']).then(() => 'read'), timedOut]);
				if (outcome === 'timeout') io.errors++;
			} catch {
				io.errors++;
			} finally {
				clearTimeout(timer);
			}
		}
	},
	...[2, 4, 8].map((count): Case => ({
		// sql/op against the shard count is the pagination amplification.
		name: `paginate_${count}_shards`,
		opsPerIteration: 1,
		setup: () => configure({ shards: shards(count) }),
		iteration: async () => {
			if (typeof api.paginate !== 'function') return;
			await api.paginate('SELECT id, name FROM users ORDER BY id', [], { limit: 20 });
		}
	})),
	{
		name: 'concurrent_write_one_key',
		opsPerIteration: 8,
		setup: async () => {
			configure();
			await api.run('hot-key', INSERT_SQL, ['hot-key', 'Name', 'a@b.local', 1]);
		},
		iteration: async () => {
			await Promise.all(new Array(8).fill(null).map(() => api.run('hot-key', UPDATE_SQL, ['Renamed', 'hot-key'])));
		}
	},
	{
		// A caller that drops a request mid-flight must not leave state behind
		// that slows or breaks the next one.
		name: 'abandoned_read_then_read',
		opsPerIteration: 2,
		setup: async () => {
			configure();
			await api.run('warm-key', INSERT_SQL, ['warm-key', 'Name', 'a@b.local', 1]);
		},
		iteration: async () => {
			void api.first('warm-key', SELECT_SQL, ['warm-key']).catch(() => {
				io.errors++;
			});
			await api.first('warm-key', SELECT_SQL, ['warm-key']);
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
	baseline?: string;
	p50Ns: number;
	p95Ns: number;
	sqlPerOp: number;
	kvPerOp: number;
	overheadNs?: number;
}

function percentile(sorted: number[], p: number): number {
	return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))]!;
}

/** Median cost of two back-to-back clock reads; subtracted from every latency sample. */
const TIMER_FLOOR_NS = percentile(
	Array.from({ length: 20_000 }, () => {
		const first = process.hrtime.bigint();
		const second = process.hrtime.bigint();
		return Number(second - first);
	}).sort((left, right) => left - right),
	0.5
);

/**
 * Times one case, reporting the median of {@link REPS} repetitions.
 *
 * The median rather than the mean because a GC pause inside one repetition is
 * not information about the router.
 */
async function measure(testCase: Case): Promise<Measurement> {
	const warmup = testCase.iterationsOverride ?? 500;
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
	let iterations = testCase.iterationsOverride ?? 0;
	if (testCase.iterationsOverride === undefined) {
		const pilotStarted = process.hrtime.bigint();
		for (let i = 0; i < 300; i++) {
			await testCase.iteration(warmup + i);
		}
		const pilotNsPerIteration = Math.max(1, Number(process.hrtime.bigint() - pilotStarted) / 300);
		iterations = Math.max(1_000, Math.min(2_000_000, Math.round((targetRepMs * 1e6) / pilotNsPerIteration)));
	}

	for (let rep = 0; rep < REPS; rep++) {
		await testCase.setup();

		for (let i = 0; i < warmup; i++) {
			await testCase.iteration(i);
		}

		resetIo();
		const startedCpu = process.cpuUsage();
		const started = process.hrtime.bigint();
		for (let i = 0; i < iterations; i++) {
			await testCase.iteration(warmup + i);
		}
		const cpu = process.cpuUsage(startedCpu);
		const elapsedNs = CPU_MODE ? (cpu.user + cpu.system) * 1000 : Number(process.hrtime.bigint() - started);

		if (rep === 0) counted = { ...io };
		samplesNs.push(elapsedNs / (iterations * testCase.opsPerIteration));
	}

	const sorted = [...samplesNs].sort((left, right) => left - right);
	const nsPerOp = percentile(sorted, 0.5);

	// Separate pass: one timed call per iteration, so tail percentiles exist. Its io is not counted.
	await testCase.setup();
	for (let i = 0; i < warmup; i++) {
		await testCase.iteration(i);
	}
	const latencyNs: number[] = [];
	const latencySamples = testCase.iterationsOverride ?? LAT_SAMPLES;
	for (let i = 0; i < latencySamples; i++) {
		const started = process.hrtime.bigint();
		await testCase.iteration(warmup + i);
		const elapsed = Number(process.hrtime.bigint() - started);
		latencyNs.push(Math.max(0, elapsed - TIMER_FLOOR_NS) / testCase.opsPerIteration);
	}
	latencyNs.sort((left, right) => left - right);

	const totalOps = iterations * testCase.opsPerIteration;

	return {
		name: testCase.name,
		baseline: testCase.baseline,
		nsPerOp,
		opsPerSecond: 1e9 / nsPerOp,
		p50Ns: percentile(latencyNs, 0.5),
		p95Ns: percentile(latencyNs, 0.95),
		sqlPerOp: counted.sqlExec / totalOps,
		kvPerOp: (counted.kvGet + counted.kvPut) / totalOps,
		samplesNs: sorted,
		io: counted,
		iterations
	};
}

const onlyNames = ONLY?.split(',');
const selected = onlyNames ? cases.filter((entry) => onlyNames.some((name) => entry.name.includes(name))) : cases;
const results: Measurement[] = [];

for (const testCase of selected) {
	results.push(await measure(testCase));
}

api.resetConfig();

const byName = new Map(results.map((result) => [result.name, result]));
for (const result of results) {
	const base = result.baseline === undefined ? undefined : byName.get(result.baseline);
	if (base) result.overheadNs = result.nsPerOp - base.nsPerOp;
}

if (JSON_OUT) {
	console.log(JSON.stringify({ src: SRC, reps: REPS, mode: CPU_MODE ? 'cpu' : 'wall', results }, null, 2));
} else {
	console.log(
		`source: ${SRC}   median of ${REPS} reps (${CPU_MODE ? 'cpu time' : 'wall time'})   p50/p95 over ${LAT_SAMPLES} timed calls (wall)`
	);
	console.log('');
	console.log(
		`${'case'.padEnd(26)}${'us/op'.padStart(9)}${'p50 us'.padStart(9)}${'p95 us'.padStart(9)}${'ops/sec'.padStart(12)}${'spread'.padStart(9)}${'overhead us'.padStart(13)}${'sql/op'.padStart(8)}${'kv/op'.padStart(8)}   io per rep`
	);
	console.log('-'.repeat(130));

	for (const result of results) {
		const spread = ((result.samplesNs[result.samplesNs.length - 1]! / result.samplesNs[0]! - 1) * 100).toFixed(0);
		const overhead = result.overheadNs === undefined ? '' : (result.overheadNs / 1000).toFixed(2);
		const counters = Object.entries(result.io)
			.filter(([, value]) => value > 0)
			.map(([name, value]) => `${name}=${value}`)
			.join(' ');

		const ops = Math.round(result.opsPerSecond).toLocaleString('en-US');
		const cells = [
			result.name.padEnd(26),
			(result.nsPerOp / 1000).toFixed(2).padStart(9),
			(result.p50Ns / 1000).toFixed(2).padStart(9),
			(result.p95Ns / 1000).toFixed(2).padStart(9),
			ops.padStart(12),
			`${spread}%`.padStart(9),
			overhead.padStart(13),
			result.sqlPerOp.toFixed(2).padStart(8),
			result.kvPerOp.toFixed(2).padStart(8)
		];
		console.log(`${cells.join('')}   ${counters}`);
	}
}
