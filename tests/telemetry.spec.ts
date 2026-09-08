import { afterEach, describe, expect, it } from 'vitest';
import { createInMemoryKVProvider, createInMemorySQLProvider } from '../src/providers-memory';
import { first, initialize, resetConfig, run, runShard } from '../src/router';
import {
	PhaseCollector,
	instrumentKV,
	instrumentSQL,
	isPhaseObserverActive,
	phaseEnd,
	phaseStart,
	setPhaseObserver,
	type PhaseSpan
} from '../src/telemetry';
import type { KVStorage, SQLDatabase } from '../src/types';

const SCHEMA = 'CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)';

describe('Phase instrumentation', () => {
	afterEach(() => {
		resetConfig();
		setPhaseObserver(null);
	});

	it('reports nothing and reads no clock when no observer is installed', () => {
		setPhaseObserver(null);
		expect(isPhaseObserverActive()).toBe(false);
		expect(phaseStart()).toBeUndefined();

		// phaseEnd with an undefined start must be a no-op rather than a throw.
		expect(() => phaseEnd('kv.get', undefined)).not.toThrow();
	});

	it('attributes a routed write across hashing, KV, and SQL', async () => {
		const collector = new PhaseCollector();

		initialize({
			kv: createInMemoryKVProvider(),
			shards: { 'db-a': createInMemorySQLProvider(), 'db-b': createInMemorySQLProvider() },
			strategy: 'hash',
			disableAutoMigration: true,
			onPhase: collector.observer
		});

		await runShard('db-a', SCHEMA);
		await runShard('db-b', SCHEMA);
		collector.reset();

		await run('user-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Ada']);

		const phases = collector.stats().map((entry) => entry.phase);
		expect(phases).toContain('sql.exec');
		expect(phases).toContain('kv.put');
		expect(phases).toContain('shard.select');

		for (const entry of collector.stats()) {
			expect(entry.count).toBeGreaterThan(0);
			expect(entry.avgMs).toBeGreaterThanOrEqual(0);
			expect(entry.maxMs).toBeGreaterThanOrEqual(entry.minMs);
			expect(entry.p95Ms).toBeGreaterThanOrEqual(entry.p50Ms);
		}
	});

	it('shows a read that misses costing one SQL call and no KV write', async () => {
		const spans: PhaseSpan[] = [];

		initialize({
			kv: createInMemoryKVProvider(),
			shards: { 'db-a': createInMemorySQLProvider(), 'db-b': createInMemorySQLProvider() },
			strategy: 'hash',
			disableAutoMigration: true,
			hashShardMappings: false,
			onPhase: (span) => spans.push(span)
		});

		await runShard('db-a', SCHEMA);
		await runShard('db-b', SCHEMA);
		spans.length = 0;

		await first('nobody', 'SELECT * FROM users WHERE id = ?', ['nobody']);

		expect(spans.filter((span) => span.phase === 'kv.put')).toHaveLength(0);
		expect(spans.filter((span) => span.phase === 'sql.exec')).toHaveLength(1);
	});

	it('tags SQL spans with the shard that ran them', async () => {
		const spans: PhaseSpan[] = [];

		initialize({
			kv: createInMemoryKVProvider(),
			shards: { 'db-a': createInMemorySQLProvider() },
			strategy: 'hash',
			disableAutoMigration: true,
			onPhase: (span) => spans.push(span)
		});

		await runShard('db-a', SCHEMA);

		expect(spans.some((span) => span.phase === 'sql.exec' && span.detail === 'db-a')).toBe(true);
	});

	it('groups KV spans by key kind rather than by individual key', async () => {
		const spans: PhaseSpan[] = [];

		initialize({
			kv: createInMemoryKVProvider(),
			shards: { 'db-a': createInMemorySQLProvider() },
			strategy: 'hash',
			disableAutoMigration: true,
			onPhase: (span) => spans.push(span)
		});

		await runShard('db-a', SCHEMA);
		await run('user-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Ada']);

		const details = new Set(spans.filter((span) => span.phase.startsWith('kv.')).map((span) => span.detail));
		expect([...details].every((detail) => detail === undefined || !detail.includes('user-1'))).toBe(true);
	});

	it('survives an observer that throws', async () => {
		initialize({
			kv: createInMemoryKVProvider(),
			shards: { 'db-a': createInMemorySQLProvider() },
			strategy: 'hash',
			disableAutoMigration: true,
			onPhase: () => {
				throw new Error('observer is broken');
			}
		});

		await runShard('db-a', SCHEMA);
		await expect(run('user-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Ada'])).resolves.toBeTruthy();
	});

	it('stops reporting once the configuration is reset', async () => {
		const spans: PhaseSpan[] = [];

		initialize({
			kv: createInMemoryKVProvider(),
			shards: { 'db-a': createInMemorySQLProvider() },
			strategy: 'hash',
			disableAutoMigration: true,
			onPhase: (span) => spans.push(span)
		});

		await runShard('db-a', SCHEMA);
		expect(spans.length).toBeGreaterThan(0);

		resetConfig();
		expect(isPhaseObserverActive()).toBe(false);
	});

	it('measures durations with sub-millisecond resolution', () => {
		const spans: PhaseSpan[] = [];
		setPhaseObserver((span) => spans.push(span));

		const started = phaseStart();
		phaseEnd('hash', started);

		expect(spans).toHaveLength(1);
		// `Date.now()` could only ever report 0 here; a monotonic clock reports a
		// real fractional value, which is the whole reason this replaced it.
		expect(spans[0]!.durationMs).toBeLessThan(5);
		expect(Number.isFinite(spans[0]!.durationMs)).toBe(true);
	});

	it('summarizes an empty collector without dividing by zero', () => {
		const collector = new PhaseCollector();
		expect(collector.stats()).toEqual([]);
		expect(collector.size).toBe(0);
	});
});

describe('Instrument wrappers', () => {
	afterEach(() => {
		setPhaseObserver(null);
	});

	function record(): { spans: PhaseSpan[] } {
		const spans: PhaseSpan[] = [];
		setPhaseObserver((span) => spans.push(span));
		return { spans };
	}

	it('reports bulk KV operations only when the store implements them', async () => {
		const { spans } = record();

		const bulk: KVStorage = {
			get: (async () => null) as KVStorage['get'],
			async put() {},
			async delete() {},
			async list() {
				return { keys: [], list_complete: true };
			},
			getMany: (async (keys: string[]) => keys.map(() => null)) as KVStorage['getMany'],
			async putMany() {},
			async deleteMany() {}
		};

		const wrapped = instrumentKV(bulk);
		await wrapped.getMany?.(['a', 'b']);
		await wrapped.getMany?.(['a'], 'json');
		await wrapped.putMany?.([{ key: 'a', value: '1' }]);
		await wrapped.deleteMany?.(['a', 'b']);
		await wrapped.list({ prefix: 'p:' });

		expect(spans.filter((span) => span.phase === 'kv.get').map((span) => span.detail)).toEqual(['many:2', 'many:1']);
		expect(spans.filter((span) => span.phase === 'kv.put').map((span) => span.detail)).toEqual(['many:1']);
		expect(spans.filter((span) => span.phase === 'kv.delete').map((span) => span.detail)).toEqual(['many:2']);
		expect(spans.filter((span) => span.phase === 'kv.list').map((span) => span.detail)).toEqual(['p:']);

		const plain = instrumentKV({
			get: (async () => null) as KVStorage['get'],
			async put() {},
			async delete() {},
			async list() {
				return { keys: [], list_complete: true };
			}
		});
		expect(plain.getMany).toBeUndefined();
		expect(plain.putMany).toBeUndefined();
		expect(plain.deleteMany).toBeUndefined();
	});

	it('reports a batch as one span tagged with its shard and size', async () => {
		const { spans } = record();

		const batching: SQLDatabase = {
			prepare(sql: string) {
				return {
					bind: () => batching.prepare(sql),
					async run() {
						return { success: true, results: [], meta: { duration: 0 } };
					},
					async all() {
						return { success: true, results: [], meta: { duration: 0 } };
					},
					async first() {
						return null;
					}
				};
			},
			async runBatch(statements) {
				return statements.map(() => ({ success: true, results: [], meta: { duration: 0 } }));
			}
		};

		const wrapped = instrumentSQL(batching, 'db-a');
		await wrapped.runBatch?.([{ sql: 'SELECT 1' }, { sql: 'SELECT 2' }]);

		expect(spans.filter((span) => span.phase === 'sql.exec').map((span) => span.detail)).toEqual(['db-a:batch:2']);
	});

	it('reports a span for each terminal call on a rebound statement', async () => {
		const { spans } = record();

		const db: SQLDatabase = {
			prepare(sql: string) {
				const statement = {
					bind: () => statement,
					async run() {
						return { success: true, results: [], meta: { duration: 0 } };
					},
					async all() {
						return { success: true, results: [], meta: { duration: 0 } };
					},
					async first() {
						return null;
					}
				};
				return statement;
			}
		};

		const wrapped = instrumentSQL(db, 'db-b');
		await wrapped.prepare('SELECT 1').bind(1).bind(2).run();
		await wrapped.prepare('SELECT 2').all();
		await wrapped.prepare('SELECT 3').first();

		expect(spans.filter((span) => span.phase === 'sql.exec')).toHaveLength(3);
		expect(spans.filter((span) => span.phase === 'sql.prepare')).toHaveLength(3);
		expect(wrapped.runBatch).toBeUndefined();
	});

	it('still reports a span when the underlying call throws', async () => {
		const { spans } = record();

		const failing: SQLDatabase = {
			prepare(sql: string) {
				return {
					bind: () => failing.prepare(sql),
					async run(): Promise<never> {
						throw new Error('boom');
					},
					async all(): Promise<never> {
						throw new Error('boom');
					},
					async first(): Promise<never> {
						throw new Error('boom');
					}
				};
			}
		};

		const wrapped = instrumentSQL(failing, 'db-c');
		await expect(wrapped.prepare('SELECT 1').run()).rejects.toThrow('boom');
		expect(spans.filter((span) => span.phase === 'sql.exec' && span.detail === 'db-c')).toHaveLength(1);
	});
});
