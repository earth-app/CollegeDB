import { afterEach, describe, expect, it } from 'vitest';
import { cached, invalidate } from '../src/cache';
import { KVShardMapper } from '../src/kvmap';
import { hrwShard } from '../src/placement';
import { createInMemoryKVProvider, createInMemorySQLProvider } from '../src/providers-memory';
import {
	allAllShardsGlobal,
	batch,
	countAllShards,
	countShard,
	first,
	getDatabaseSizesAllShards,
	indexAllShards,
	initialize,
	insert,
	insertInto,
	insertShard,
	invalidateMappingCache,
	nextId,
	query,
	queryAll,
	queryFirst,
	reassignShard,
	rebalance,
	resetConfig,
	run,
	runShard
} from '../src/router';
import type { BatchStatement, KVStorage, QueryResult, SQLDatabase } from '../src/types';

const SCHEMA = 'CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)';

/** Wraps a KV store to count the operations that reach it. */
function countingKV(inner: KVStorage): { kv: KVStorage; counts: { get: number; put: number; delete: number; list: number } } {
	const counts = { get: 0, put: 0, delete: 0, list: 0 };

	const kv: KVStorage = {
		get: (async (key: string, type?: 'text' | 'json') => {
			counts.get++;
			return type === 'json' ? await inner.get(key, 'json') : await inner.get(key, type);
		}) as KVStorage['get'],
		async put(key, value) {
			counts.put++;
			await inner.put(key, value);
		},
		async delete(key) {
			counts.delete++;
			await inner.delete(key);
		},
		async list(options) {
			counts.list++;
			return await inner.list(options);
		}
	};

	// Forwarded so the counters measure round trips rather than keys, and so a
	// wrapper cannot silently drop the store back to one key at a time.
	if (inner.getMany) {
		kv.getMany = (async (keys: string[], type?: 'text' | 'json') => {
			counts.get++;
			return type === 'json' ? await inner.getMany!(keys, 'json') : await inner.getMany!(keys, type);
		}) as KVStorage['getMany'];
	}

	if (inner.putMany) {
		kv.putMany = async (entries) => {
			counts.put++;
			await inner.putMany!(entries);
		};
	}

	return { kv, counts };
}

async function makeShards(names: string[] = ['db-a', 'db-b']) {
	const shards: Record<string, SQLDatabase> = {};
	for (const name of names) {
		shards[name] = createInMemorySQLProvider();
	}
	return shards;
}

describe('Allocation strategies', () => {
	afterEach(() => {
		resetConfig();
	});

	it('round-robin distributes evenly without a coordinator', async () => {
		const kv = createInMemoryKVProvider();
		const shards = await makeShards(['db-a', 'db-b', 'db-c']);

		initialize({ kv, shards, strategy: 'round-robin', disableAutoMigration: true, hashShardMappings: false });
		for (const name of Object.keys(shards)) {
			await runShard(name, SCHEMA);
		}

		for (let i = 0; i < 6; i++) {
			await run(`user-${i}`, 'INSERT INTO users (id, name) VALUES (?, ?)', [`user-${i}`, `User ${i}`]);
		}

		const perShard = await Promise.all(
			Object.keys(shards).map(async (name) => {
				const result = await runShard<{ c: number }>(name, 'SELECT COUNT(*) AS c FROM users');
				return Number(result.results[0]?.c ?? 0);
			})
		);

		// Before this fix round-robin fell through to the hash branch, so the
		// distribution was whatever the hash produced rather than 2/2/2.
		expect(perShard.sort()).toEqual([2, 2, 2]);
	});

	it('routes hash placement identically regardless of binding declaration order', async () => {
		const keys = new Array(200).fill(null).map((_, index) => `user-${index}`);
		const forward = hrwShard(keys[0]!, ['db-a', 'db-b', 'db-c']);
		const reverse = hrwShard(keys[0]!, ['db-c', 'db-b', 'db-a']);
		expect(forward).toBe(reverse);

		for (const key of keys) {
			expect(hrwShard(key, ['db-a', 'db-b', 'db-c'])).toBe(hrwShard(key, ['db-b', 'db-c', 'db-a']));
		}
	});
});

describe('Reads do not allocate mappings', () => {
	afterEach(() => {
		resetConfig();
	});

	it('writes no KV mapping when a read finds no row', async () => {
		const { kv, counts } = countingKV(createInMemoryKVProvider());
		const shards = await makeShards();

		initialize({ kv, shards, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false });
		for (const name of Object.keys(shards)) {
			await runShard(name, SCHEMA);
		}

		const before = counts.put;
		const row = await first('missing-user', 'SELECT * FROM users WHERE id = ?', ['missing-user']);

		expect(row).toBeNull();
		expect(counts.put).toBe(before);
	});

	it('still records a mapping for a write', async () => {
		const { kv, counts } = countingKV(createInMemoryKVProvider());
		const shards = await makeShards();

		initialize({ kv, shards, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false });
		for (const name of Object.keys(shards)) {
			await runShard(name, SCHEMA);
		}

		const before = counts.put;
		await run('user-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Ada']);
		expect(counts.put).toBeGreaterThan(before);
	});

	it('records a mapping on read when allocateOnRead is enabled', async () => {
		const { kv, counts } = countingKV(createInMemoryKVProvider());
		const shards = await makeShards();

		initialize({ kv, shards, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false, allocateOnRead: true });
		for (const name of Object.keys(shards)) {
			await runShard(name, SCHEMA);
		}

		const before = counts.put;
		await first('missing-user', 'SELECT * FROM users WHERE id = ?', ['missing-user']);
		expect(counts.put).toBeGreaterThan(before);
	});

	it('reads a mapping with a single KV get instead of two', async () => {
		const { kv, counts } = countingKV(createInMemoryKVProvider());
		const shards = await makeShards();

		initialize({
			kv,
			shards,
			strategy: 'hash',
			disableAutoMigration: true,
			hashShardMappings: false,
			mappingCacheTtlMs: 0
		});
		for (const name of Object.keys(shards)) {
			await runShard(name, SCHEMA);
		}

		counts.get = 0;
		await first('absent', 'SELECT * FROM users WHERE id = ?', ['absent']);

		// The legacy multi-key probe used to double this.
		expect(counts.get).toBe(1);
	});

	it('still probes the legacy record when legacyMultiKeyLookup is enabled', async () => {
		const { kv, counts } = countingKV(createInMemoryKVProvider());
		const shards = await makeShards();

		initialize({
			kv,
			shards,
			strategy: 'hash',
			disableAutoMigration: true,
			hashShardMappings: false,
			mappingCacheTtlMs: 0,
			legacyMultiKeyLookup: true
		});
		for (const name of Object.keys(shards)) {
			await runShard(name, SCHEMA);
		}

		counts.get = 0;
		await first('absent', 'SELECT * FROM users WHERE id = ?', ['absent']);
		expect(counts.get).toBe(2);
	});
});

describe('Generated id extraction', () => {
	afterEach(() => {
		resetConfig();
	});

	it('throws rather than mapping an unrelated column as the routing key', async () => {
		const kv = createInMemoryKVProvider();
		const shards = await makeShards(['db-a']);

		initialize({ kv, shards, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false });
		await runShard('db-a', 'CREATE TABLE things (uuid TEXT PRIMARY KEY, label TEXT)');

		// The row comes back with `label` first and no `id`/`rowid` column, which
		// used to be silently accepted as the generated key.
		await expect(insert('INSERT INTO things (uuid, label) VALUES (?, ?) RETURNING label, uuid', ['abc', 'Widget'])).rejects.toThrow(
			/idColumn/
		);
	});

	it('refuses a generated id that another shard already minted', async () => {
		const kv = createInMemoryKVProvider();
		const shards = await makeShards(['db-a', 'db-b']);

		initialize({ kv, shards, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false });
		for (const name of Object.keys(shards)) {
			await runShard(name, 'CREATE TABLE auto_users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)');
		}

		// Each shard runs its own sequence, so both hand out id 1. Accepting the
		// second one silently stranded the first shard's row, which is what the
		// sandbox had been reporting as an intermittent auto_increment failure.
		// Both inserts are pinned so the collision is forced rather than left to
		// whichever shard the allocator happened to pick.
		const created = await insertShard('db-a', 'INSERT INTO auto_users (name) VALUES (?)', ['Ada']);
		expect(created.generatedId).toBe(1);

		await expect(insertShard('db-b', 'INSERT INTO auto_users (name) VALUES (?)', ['Grace'])).rejects.toThrow(
			/already mapped to shard db-a/
		);
	});

	it('does not read the mapping to rule out a collision on a single shard', async () => {
		const { kv, counts } = countingKV(createInMemoryKVProvider());
		const shards = await makeShards(['db-a']);

		initialize({ kv, shards, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false });
		await runShard('db-a', 'CREATE TABLE auto_users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)');

		const before = counts.get;
		await insertShard('db-a', 'INSERT INTO auto_users (name) VALUES (?)', ['Ada']);

		// One shard has no second sequence, so there is no collision to detect and
		// the guard's KV read would be spent proving something already known.
		expect(counts.get).toBe(before);
	});

	it('accepts repeated generated ids when they stay on one shard', async () => {
		const kv = createInMemoryKVProvider();
		const shards = await makeShards(['db-a']);

		initialize({ kv, shards, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false });
		await runShard('db-a', 'CREATE TABLE auto_users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)');

		expect((await insertShard('db-a', 'INSERT INTO auto_users (name) VALUES (?)', ['Ada'])).generatedId).toBe(1);
		expect((await insertShard('db-a', 'INSERT INTO auto_users (name) VALUES (?)', ['Grace'])).generatedId).toBe(2);
	});

	it('uses an explicitly named id column', async () => {
		const kv = createInMemoryKVProvider();
		const shards = await makeShards(['db-a']);

		initialize({ kv, shards, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false });
		await runShard('db-a', 'CREATE TABLE things (uuid TEXT PRIMARY KEY, label TEXT)');

		const created = await insert('INSERT INTO things (uuid, label) VALUES (?, ?) RETURNING label, uuid', ['abc', 'Widget'], {
			idColumn: 'uuid'
		});
		expect(created.generatedId).toBe('abc');
	});
});

describe('Sizing across backend families', () => {
	afterEach(() => {
		resetConfig();
	});

	it('uses a postgres sizing statement when pragmas are unavailable', async () => {
		const kv = createInMemoryKVProvider();
		const asked: string[] = [];

		const postgresLike: SQLDatabase = {
			prepare(sql: string) {
				asked.push(sql);
				return {
					bind: () => postgresLike.prepare(sql),
					async run() {
						return { success: true, results: [], meta: { duration: 0 } };
					},
					async all() {
						return { success: true, results: [], meta: { duration: 0 } };
					},
					async first<T = Record<string, unknown>>() {
						if (sql.includes('pragma')) {
							throw new Error('near "PRAGMA": syntax error');
						}
						if (sql.includes('pg_database_size')) {
							return { collegedb_size_bytes: 4096 } as T;
						}
						return null;
					}
				};
			}
		};

		initialize({ kv, shards: { 'db-a': postgresLike }, strategy: 'hash', disableAutoMigration: true });

		const sizes = await getDatabaseSizesAllShards();
		expect(sizes[0]?.size).toBe(4096);
		expect(asked.some((sql) => sql.includes('pg_database_size'))).toBe(true);
	});

	it('refuses a numeric answer that is not the named size column', async () => {
		const kv = createInMemoryKVProvider();

		// Answers every statement with an unrelated row, which a positional read
		// would have accepted as a database size.
		const chatty: SQLDatabase = {
			prepare(sql: string) {
				return {
					bind: () => chatty.prepare(sql),
					async run() {
						return { success: true, results: [], meta: { duration: 0 } };
					},
					async all() {
						return { success: true, results: [], meta: { duration: 0 } };
					},
					async first<T = Record<string, unknown>>() {
						return { score: 9 } as T;
					}
				};
			}
		};

		initialize({ kv, shards: { 'db-a': chatty }, strategy: 'hash', disableAutoMigration: true });

		const sizes = await getDatabaseSizesAllShards();
		expect(sizes[0]?.size).toBeNull();
		expect(sizes[0]?.success).toBe(false);
		expect(sizes[0]?.error).toMatch(/Failed to get database size/);
	});
});

describe('Cache helpers', () => {
	afterEach(() => {
		resetConfig();
	});

	it('runs the fetcher once for concurrent misses on the same key', async () => {
		const kv = createInMemoryKVProvider();
		initialize({ kv, shards: await makeShards(['db-a']), strategy: 'hash', disableAutoMigration: true });

		let calls = 0;
		const fetcher = async () => {
			calls++;
			await new Promise((resolve) => setTimeout(resolve, 5));
			return 'value';
		};

		const results = await Promise.all(new Array(10).fill(null).map(() => cached('hot-key', fetcher)));

		expect(results.every((value) => value === 'value')).toBe(true);
		expect(calls).toBe(1);
	});

	it('clears a prefix and reports how many keys went', async () => {
		const kv = createInMemoryKVProvider();
		initialize({ kv, shards: await makeShards(['db-a']), strategy: 'hash', disableAutoMigration: true });

		for (let i = 0; i < 50; i++) {
			await kv.put(`tickets:list:${i}`, JSON.stringify({ v: i }));
		}

		expect(await invalidate('tickets:list:')).toBe(50);
		expect((await kv.list({ prefix: 'tickets:list:' })).keys).toHaveLength(0);
	});
});

describe('Routed batch', () => {
	afterEach(() => {
		resetConfig();
	});

	it('groups statements by shard and preserves per-key order', async () => {
		const kv = createInMemoryKVProvider();
		const shards = await makeShards(['db-a', 'db-b', 'db-c']);

		initialize({ kv, shards, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false });
		for (const name of Object.keys(shards)) {
			await runShard(name, SCHEMA);
		}

		const entries = new Array(30).fill(null).map((_, index) => ({
			key: `user-${index}`,
			sql: 'INSERT INTO users (id, name) VALUES (?, ?)',
			bindings: [`user-${index}`, `User ${index}`]
		}));

		const groups = await batch(entries);

		expect(groups.length).toBeLessThanOrEqual(3);
		expect(groups.reduce((sum, group) => sum + group.indices.length, 0)).toBe(30);
		expect(groups.every((group) => group.error === undefined)).toBe(true);

		for (const group of groups) {
			expect([...group.indices]).toEqual([...group.indices].sort((a, b) => a - b));
		}

		for (let index = 0; index < 30; index++) {
			const row = await first<{ id: string }>(`user-${index}`, 'SELECT id FROM users WHERE id = ?', [`user-${index}`]);
			expect(row?.id).toBe(`user-${index}`);
		}
	});

	it('returns an empty result for no entries', async () => {
		const kv = createInMemoryKVProvider();
		initialize({ kv, shards: await makeShards(['db-a']), strategy: 'hash', disableAutoMigration: true });
		expect(await batch([])).toEqual([]);
	});

	it('reads and writes the mappings for a whole batch in one round trip each', async () => {
		const { kv, counts } = countingKV(createInMemoryKVProvider());
		const shards = await makeShards(['db-a', 'db-b']);

		initialize({ kv, shards, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false, mappingCacheTtlMs: 0 });
		for (const name of Object.keys(shards)) {
			await runShard(name, SCHEMA);
		}

		counts.get = 0;
		counts.put = 0;

		const entries = new Array(40).fill(null).map((_, index) => ({
			key: `bulk-${index}`,
			sql: 'INSERT INTO users (id, name) VALUES (?, ?)',
			bindings: [`bulk-${index}`, `User ${index}`]
		}));

		await batch(entries);

		// Routing these one at a time was 40 reads and 40 writes. The in-memory
		// store implements the bulk primitives, so it is now one of each.
		expect(counts.get).toBe(1);
		expect(counts.put).toBe(1);

		for (let index = 0; index < 40; index++) {
			const row = await first<{ id: string }>(`bulk-${index}`, 'SELECT id FROM users WHERE id = ?', [`bulk-${index}`]);
			expect(row?.id).toBe(`bulk-${index}`);
		}
	});

	it('reuses mappings that already exist rather than reallocating them', async () => {
		const { kv, counts } = countingKV(createInMemoryKVProvider());
		const shards = await makeShards(['db-a', 'db-b']);

		initialize({ kv, shards, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false, mappingCacheTtlMs: 0 });
		for (const name of Object.keys(shards)) {
			await runShard(name, SCHEMA);
		}

		await run('known-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['known-1', 'Ada']);

		counts.put = 0;
		await batch([{ key: 'known-1', sql: 'UPDATE users SET name = ? WHERE id = ?', bindings: ['Grace', 'known-1'] }]);

		// An existing mapping is read, never rewritten. The row is only reachable
		// through that mapping, so reading it back proves the batch went to the
		// shard the mapping already named.
		expect(counts.put).toBe(0);
		const row = await first<{ name: string }>('known-1', 'SELECT name FROM users WHERE id = ?', ['known-1']);
		expect(row?.name).toBe('Grace');
	});

	it('allocates a repeated key once so the second entry cannot overwrite the first', async () => {
		const kv = createInMemoryKVProvider();
		const shards = await makeShards(['db-a', 'db-b', 'db-c']);

		initialize({ kv, shards, strategy: 'round-robin', disableAutoMigration: true, hashShardMappings: false });
		for (const name of Object.keys(shards)) {
			await runShard(name, SCHEMA);
		}

		// round-robin would hand the same key two different shards, stranding the
		// first row where the surviving mapping does not point.
		const groups = await batch([
			{ key: 'same-key', sql: 'INSERT INTO users (id, name) VALUES (?, ?)', bindings: ['same-key', 'Ada'] },
			{ key: 'same-key', sql: 'UPDATE users SET name = ? WHERE id = ?', bindings: ['Grace', 'same-key'] }
		]);

		expect(groups).toHaveLength(1);
		expect(groups[0]!.indices).toEqual([0, 1]);

		const row = await first<{ name: string }>('same-key', 'SELECT name FROM users WHERE id = ?', ['same-key']);
		expect(row?.name).toBe('Grace');
	});

	it('does not record mappings for reads in a batch', async () => {
		const { kv, counts } = countingKV(createInMemoryKVProvider());
		const shards = await makeShards(['db-a', 'db-b']);

		initialize({ kv, shards, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false, mappingCacheTtlMs: 0 });
		for (const name of Object.keys(shards)) {
			await runShard(name, SCHEMA);
		}

		counts.put = 0;
		await batch([
			{ key: 'absent-1', sql: 'SELECT * FROM users WHERE id = ?', bindings: ['absent-1'] },
			{ key: 'absent-2', sql: 'SELECT * FROM users WHERE id = ?', bindings: ['absent-2'] }
		]);

		expect(counts.put).toBe(0);
	});

	it('runs each shard group in one call when the provider batches', async () => {
		const kv = createInMemoryKVProvider();
		const batched: number[] = [];

		function batchingShard(): SQLDatabase {
			const inner = createInMemorySQLProvider();
			return {
				prepare: (sql: string) => inner.prepare(sql),
				async runBatch<T = Record<string, unknown>>(statements: BatchStatement[]) {
					batched.push(statements.length);
					const results: QueryResult<T>[] = [];
					for (const statement of statements) {
						results.push(
							await inner
								.prepare(statement.sql)
								.bind(...(statement.bindings ?? []))
								.run<T>()
						);
					}
					return results;
				}
			};
		}

		const shards: Record<string, SQLDatabase> = { 'db-a': batchingShard(), 'db-b': batchingShard() };
		initialize({ kv, shards, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false });
		for (const name of Object.keys(shards)) {
			await runShard(name, SCHEMA);
		}

		batched.length = 0;
		await batch(
			new Array(12).fill(null).map((_, index) => ({
				key: `batched-${index}`,
				sql: 'INSERT INTO users (id, name) VALUES (?, ?)',
				bindings: [`batched-${index}`, `User ${index}`]
			}))
		);

		// One call per shard that received work, not one per statement.
		expect(batched.length).toBeGreaterThan(0);
		expect(batched.length).toBeLessThanOrEqual(2);
		expect(batched.reduce((sum, size) => sum + size, 0)).toBe(12);
	});
});

describe('Planner-routed API', () => {
	afterEach(() => {
		resetConfig();
	});

	async function setup(extra: Record<string, unknown> = {}) {
		const kv = createInMemoryKVProvider();
		const shards = await makeShards(['db-a', 'db-b']);
		initialize({ kv, shards, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false, ...extra });
		for (const name of Object.keys(shards)) {
			await runShard(name, SCHEMA);
		}
		return { kv, shards };
	}

	it('routes a write and a read to the same shard as the explicit-key API', async () => {
		await setup();

		await query('INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Ada']);

		expect(await queryFirst<{ name: string }>('SELECT * FROM users WHERE id = ?', ['user-1'])).toMatchObject({ name: 'Ada' });
		expect(await first<{ name: string }>('user-1', 'SELECT * FROM users WHERE id = ?', ['user-1'])).toMatchObject({ name: 'Ada' });
	});

	it('spreads a multi-row insert across shards and reads each row back', async () => {
		await setup();

		await query('INSERT INTO users (id, name) VALUES (?, ?), (?, ?), (?, ?)', ['user-1', 'Ada', 'user-2', 'Grace', 'user-3', 'Alan']);

		for (const [id, name] of [
			['user-1', 'Ada'],
			['user-2', 'Grace'],
			['user-3', 'Alan']
		]) {
			expect(await first<{ name: string }>(id!, 'SELECT * FROM users WHERE id = ?', [id])).toMatchObject({ name });
		}
	});

	it('throws by default when it cannot prove the routing key', async () => {
		await setup();
		await expect(query('SELECT * FROM users WHERE name = ?', ['Ada'])).rejects.toThrow(/Could not determine a routing key/);
	});

	it('fans out instead when onUnroutable is fanout', async () => {
		await setup({ onUnroutable: 'fanout' });

		await run('user-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Ada']);
		const result = await query<{ id: string }>('SELECT * FROM users WHERE name = ?', ['Ada']);
		expect(result.results.map((row) => row.id)).toContain('user-1');
	});

	it('issues an IN predicate on the shard of every listed key', async () => {
		// The in-memory emulator does not implement IN, so this asserts the
		// routing decision rather than the SQL semantics; IN parsing itself is
		// covered in planner.spec.ts.
		const kv = createInMemoryKVProvider();
		const base = await makeShards(['db-a', 'db-b']);
		const asked = new Map<string, string[]>();

		const instrumented: Record<string, SQLDatabase> = {};
		for (const [name, shard] of Object.entries(base)) {
			asked.set(name, []);
			instrumented[name] = {
				prepare(sql: string) {
					asked.get(name)!.push(sql);
					return shard.prepare(sql);
				}
			};
		}

		initialize({ kv, shards: instrumented, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false });
		for (const name of Object.keys(instrumented)) {
			await runShard(name, SCHEMA);
		}

		// Two keys that hash to different shards.
		const keys = ['user-1', 'user-2', 'user-3', 'user-4', 'user-5']
			.map((key) => ({ key, shard: hrwShard(key, ['db-a', 'db-b']) }))
			.reduce<Record<string, string>>((acc, entry) => {
				acc[entry.shard] ??= entry.key;
				return acc;
			}, {});

		const [left, right] = Object.values(keys);
		expect(left).toBeDefined();
		expect(right).toBeDefined();

		for (const name of asked.keys()) {
			asked.set(name, []);
		}

		await queryAll('SELECT * FROM users WHERE id IN (?, ?)', [left, right]);

		for (const [name, statements] of asked) {
			expect(statements.some((sql) => sql.includes('IN (?, ?)'))).toBe(true);
			expect(name).toBeTruthy();
		}
	});
});

describe('Global pagination pushdown', () => {
	afterEach(() => {
		resetConfig();
	});

	it('bounds each shard query without changing the page it returns', async () => {
		const kv = createInMemoryKVProvider();
		const shards = await makeShards(['db-a', 'db-b']);
		const asked: string[] = [];

		const instrumented: Record<string, SQLDatabase> = {};
		for (const [name, shard] of Object.entries(shards)) {
			instrumented[name] = {
				prepare(sql: string) {
					asked.push(sql);
					return shard.prepare(sql);
				}
			};
		}

		initialize({ kv, shards: instrumented, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false });
		for (const name of Object.keys(instrumented)) {
			await runShard(name, SCHEMA);
		}

		for (let i = 0; i < 20; i++) {
			await run(`user-${i}`, 'INSERT INTO users (id, name) VALUES (?, ?)', [`user-${i}`, `user-${String(i).padStart(2, '0')}`]);
		}

		asked.length = 0;
		const page = await allAllShardsGlobal<{ name: string }>('SELECT * FROM users', [], {
			sortBy: 'name',
			limit: 5
		});

		expect(page.results).toHaveLength(5);
		expect(page.results.map((row) => row.name)).toEqual(['user-00', 'user-01', 'user-02', 'user-03', 'user-04']);
		expect(asked.some((sql) => sql.includes('LIMIT 5'))).toBe(true);
	});

	it('leaves the statement alone when a JavaScript filter could promote a row', async () => {
		const kv = createInMemoryKVProvider();
		const shards = await makeShards(['db-a']);
		const asked: string[] = [];

		const instrumented: SQLDatabase = {
			prepare(sql: string) {
				asked.push(sql);
				return shards['db-a']!.prepare(sql);
			}
		};

		initialize({ kv, shards: { 'db-a': instrumented }, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false });
		await runShard('db-a', SCHEMA);
		await run('user-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Ada']);

		asked.length = 0;
		await allAllShardsGlobal('SELECT * FROM users', [], { sortBy: 'name', limit: 5, filter: () => true });
		expect(asked.every((sql) => !sql.includes('LIMIT'))).toBe(true);

		asked.length = 0;
		await allAllShardsGlobal('SELECT * FROM users', [], { sortBy: 'name', limit: 5, includeTotal: true });
		expect(asked.every((sql) => !sql.includes('LIMIT'))).toBe(true);

		asked.length = 0;
		await allAllShardsGlobal('SELECT * FROM users LIMIT 3', [], { sortBy: 'name', limit: 5 });
		expect(asked.every((sql) => !sql.includes('LIMIT 5'))).toBe(true);
	});
});

describe('Cross-shard id sequence', () => {
	afterEach(() => {
		resetConfig();
	});

	it('queries no shard once the coordinator sequence is seeded', async () => {
		const kv = createInMemoryKVProvider();
		const shards = await makeShards(['db-a', 'db-b']);
		const asked: string[] = [];

		const instrumented: Record<string, SQLDatabase> = {};
		for (const [name, shard] of Object.entries(shards)) {
			instrumented[name] = {
				prepare(sql: string) {
					asked.push(sql);
					return shard.prepare(sql);
				}
			};
		}

		const counters = new Map<string, number>();
		const coordinator = {
			idFromName: () => 'default',
			get: () => ({
				async fetch(_url: string, init: { body: string }) {
					const body = JSON.parse(init.body) as { name: string; min?: number; requireExisting?: boolean };
					if (body.requireExisting && !counters.has(body.name)) {
						return new Response(JSON.stringify({ needsSeed: true }), { status: 200 });
					}
					const next = Math.max((counters.get(body.name) ?? 0) + 1, body.min ?? 0);
					counters.set(body.name, next);
					return new Response(JSON.stringify({ value: next }), { status: 200 });
				}
			})
		};

		initialize({
			kv,
			coordinator: coordinator as any,
			shards: instrumented,
			strategy: 'hash',
			disableAutoMigration: true,
			hashShardMappings: false
		});
		for (const name of Object.keys(instrumented)) {
			await runShard(name, 'CREATE TABLE tickets (id INTEGER PRIMARY KEY, title TEXT)');
		}

		asked.length = 0;
		expect(await nextId('tickets')).toBe(1);
		const afterSeeding = asked.filter((sql) => sql.includes('MAX')).length;
		expect(afterSeeding).toBeGreaterThan(0);

		asked.length = 0;
		expect(await nextId('tickets')).toBe(2);
		expect(await nextId('tickets')).toBe(3);
		expect(asked.filter((sql) => sql.includes('MAX'))).toHaveLength(0);
	});
});

describe('Computed placement', () => {
	afterEach(() => {
		resetConfig();
	});

	async function setupComputed(extra: Record<string, unknown> = {}) {
		const { kv, counts } = countingKV(createInMemoryKVProvider());
		const shards = await makeShards(['db-a', 'db-b', 'db-c']);

		initialize({
			kv,
			shards,
			strategy: 'hash',
			placement: 'computed',
			disableAutoMigration: true,
			hashShardMappings: false,
			...extra
		});
		for (const name of Object.keys(shards)) {
			await runShard(name, SCHEMA);
		}

		return { kv, counts, shards };
	}

	it('reads and writes without touching KV once the manifest is loaded', async () => {
		const { counts } = await setupComputed();

		await run('user-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Ada']);
		expect(await first<{ name: string }>('user-1', 'SELECT * FROM users WHERE id = ?', ['user-1'])).toMatchObject({ name: 'Ada' });

		const settled = { get: counts.get, put: counts.put };

		for (let i = 2; i <= 20; i++) {
			await run(`user-${i}`, 'INSERT INTO users (id, name) VALUES (?, ?)', [`user-${i}`, `User ${i}`]);
			await first(`user-${i}`, 'SELECT * FROM users WHERE id = ?', [`user-${i}`]);
		}

		// 19 more keys, each written and read, with no mapping stored for any of
		// them. Under KV placement that is 19 writes and at least 19 reads.
		expect(counts.put).toBe(settled.put);
		expect(counts.get).toBe(settled.get);
	});

	it('routes every key to the shard the placement function computes', async () => {
		const { shards } = await setupComputed();
		const names = Object.keys(shards);

		for (let i = 0; i < 40; i++) {
			const key = `user-${i}`;
			await run(key, 'INSERT INTO users (id, name) VALUES (?, ?)', [key, `User ${i}`]);

			const expected = hrwShard(key, names);
			const row = await runShard<{ c: number }>(expected, 'SELECT COUNT(*) AS c FROM users WHERE id = ?', [key]);
			expect(Number(row.results[0]?.c ?? 0)).toBe(1);
		}
	});

	it('keeps a reassigned key reachable by recording it as an exception', async () => {
		const { shards } = await setupComputed();
		const names = Object.keys(shards);

		await run('user-7', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-7', 'Ada']);

		const computed = hrwShard('user-7', names);
		const target = names.find((name) => name !== computed)!;

		await reassignShard('user-7', target, 'users');

		// The computed shard no longer holds the row, so only the exception record
		// keeps it reachable.
		expect(await first<{ name: string }>('user-7', 'SELECT * FROM users WHERE id = ?', ['user-7'])).toMatchObject({ name: 'Ada' });

		const onTarget = await runShard<{ c: number }>(target, 'SELECT COUNT(*) AS c FROM users WHERE id = ?', ['user-7']);
		expect(Number(onTarget.results[0]?.c ?? 0)).toBe(1);
	});

	it('finds keys placed under the previous topology after a shard is added', async () => {
		const { kv, shards: original } = await setupComputed();
		const names = Object.keys(original);

		for (let i = 0; i < 30; i++) {
			const key = `user-${i}`;
			await run(key, 'INSERT INTO users (id, name) VALUES (?, ?)', [key, `User ${i}`]);
		}

		// Growing the cluster moves roughly 1/m of the keyspace, so for those keys
		// the newest epoch computes a shard that has no row. They stay reachable
		// only by walking back through the epoch that placed them, which is the
		// path a single-candidate shortcut must not swallow.
		const shards = { ...original, ...(await makeShards(['db-d'])) };

		initialize({
			kv,
			shards,
			strategy: 'hash',
			placement: 'computed',
			disableAutoMigration: true,
			hashShardMappings: false
		});
		await runShard('db-d', SCHEMA);

		let relocated = 0;
		for (let i = 0; i < 30; i++) {
			const key = `user-${i}`;
			if (hrwShard(key, [...names, 'db-d']) !== hrwShard(key, names)) {
				relocated++;
			}

			expect(await first<{ name: string }>(key, 'SELECT * FROM users WHERE id = ?', [key])).toMatchObject({
				name: `User ${i}`
			});
		}

		// A test that relocated nothing would pass without exercising the walk.
		expect(relocated).toBeGreaterThan(0);
	});

	it('falls back to KV for strategies that are not functions of the key', async () => {
		const { kv, counts } = countingKV(createInMemoryKVProvider());
		const shards = await makeShards(['db-a', 'db-b']);

		initialize({
			kv,
			shards,
			strategy: 'round-robin',
			placement: 'computed',
			disableAutoMigration: true,
			hashShardMappings: false
		});
		for (const name of Object.keys(shards)) {
			await runShard(name, SCHEMA);
		}

		const before = counts.put;
		await run('user-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Ada']);

		// round-robin cannot be recomputed from the key, so the mapping is stored.
		expect(counts.put).toBeGreaterThan(before);
	});

	it('reports how many stored mappings already agree with the placement function', async () => {
		const kv = createInMemoryKVProvider();
		const shards = await makeShards(['db-a', 'db-b']);

		initialize({ kv, shards, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false });
		for (const name of Object.keys(shards)) {
			await runShard(name, SCHEMA);
		}

		for (let i = 0; i < 10; i++) {
			await run(`user-${i}`, 'INSERT INTO users (id, name) VALUES (?, ?)', [`user-${i}`, `User ${i}`]);
		}

		// These mappings were written by the same HRW function, so a rebalance has
		// nothing to move. That is the gate on enabling computed placement.
		const result = await rebalance('users', { dryRun: true });
		expect(result.examined).toBeGreaterThan(0);
		expect(result.moved).toBe(0);
		expect(result.failed).toEqual([]);
		expect(result.agreed).toBe(result.examined);
	});
});

describe('Generated SQL respects each shard dialect', () => {
	afterEach(() => {
		resetConfig();
	});

	/** Records the SQL a shard is asked to prepare, and reports a dialect. */
	function recordingShard(dialect: 'sqlite' | 'postgres' | 'mysql' | undefined) {
		const asked: string[] = [];
		const inner = createInMemorySQLProvider();

		const shard: SQLDatabase = {
			dialect,
			prepare(sql: string) {
				asked.push(sql);
				return inner.prepare(sql);
			}
		};

		return { shard, asked };
	}

	it('quotes with backticks on a MySQL shard and double quotes elsewhere', async () => {
		const mysql = recordingShard('mysql');
		const postgres = recordingShard('postgres');

		initialize({
			kv: createInMemoryKVProvider(),
			shards: { 'db-mysql': mysql.shard, 'db-postgres': postgres.shard },
			strategy: 'hash',
			disableAutoMigration: true,
			hashShardMappings: false
		});

		await runShard('db-mysql', SCHEMA);
		await runShard('db-postgres', SCHEMA);

		// Pick a key per shard so each dialect is exercised through the routed path.
		const forMysql = ['k0', 'k1', 'k2', 'k3', 'k4', 'k5'].find((k) => hrwShard(k, ['db-mysql', 'db-postgres']) === 'db-mysql')!;
		const forPostgres = ['k0', 'k1', 'k2', 'k3', 'k4', 'k5'].find((k) => hrwShard(k, ['db-mysql', 'db-postgres']) === 'db-postgres')!;

		mysql.asked.length = 0;
		postgres.asked.length = 0;

		await insertInto(forMysql, 'users', { id: forMysql, name: 'Ada' });
		await insertInto(forPostgres, 'users', { id: forPostgres, name: 'Grace' });

		expect(mysql.asked.some((sql) => sql.includes('INSERT INTO `users` (`id`, `name`)'))).toBe(true);
		expect(mysql.asked.every((sql) => !sql.includes('"users"'))).toBe(true);
		expect(postgres.asked.some((sql) => sql.includes('INSERT INTO "users" ("id", "name")'))).toBe(true);
	});

	it('builds a MySQL-safe cross-shard MAX for nextId', async () => {
		const mysql = recordingShard('mysql');

		initialize({
			kv: createInMemoryKVProvider(),
			shards: { 'db-mysql': mysql.shard },
			strategy: 'hash',
			disableAutoMigration: true,
			hashShardMappings: false
		});

		await runShard('db-mysql', 'CREATE TABLE seq_items (id INTEGER PRIMARY KEY, title TEXT)');
		mysql.asked.length = 0;

		await nextId('seq_items');

		// The sandbox surfaced this as a MySQL syntax error on `SELECT MAX("id")
		// AS max_value FROM "seq_items"`.
		const max = mysql.asked.find((sql) => sql.includes('MAX('));
		expect(max).toBe('SELECT MAX(`id`) AS max_value FROM `seq_items`');
	});

	it('builds MySQL-safe count, index, and paginated statements', async () => {
		const mysql = recordingShard('mysql');

		initialize({
			kv: createInMemoryKVProvider(),
			shards: { 'db-mysql': mysql.shard },
			strategy: 'hash',
			disableAutoMigration: true,
			hashShardMappings: false
		});

		await runShard('db-mysql', SCHEMA);
		await run('user-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Ada']);

		mysql.asked.length = 0;
		await countShard('db-mysql', 'users');
		await countAllShards('users');
		await indexAllShards('users', 'name');
		await allAllShardsGlobal('SELECT * FROM users', [], { sortBy: 'name', limit: 5 });

		// Nothing generated for a MySQL shard may carry ANSI double quotes.
		const offenders = mysql.asked.filter((sql) => sql.includes('"'));
		expect(offenders).toEqual([]);
		expect(mysql.asked.some((sql) => sql.includes('COUNT(*) AS row_count FROM `users`'))).toBe(true);
		expect(mysql.asked.some((sql) => sql.includes('ON `users` (`name`)'))).toBe(true);
		expect(mysql.asked.some((sql) => sql.includes('ORDER BY `name` ASC LIMIT 5'))).toBe(true);
	});

	it('quotes per shard when one cluster mixes vendors', async () => {
		// The cross-shard helpers resolve the quoted table once when every shard
		// agrees on a dialect, which is the case that made the per-call rebuild
		// worth removing. A cluster that disagrees has to keep quoting per shard,
		// and this is the only thing that proves that branch still runs.
		const mysql = recordingShard('mysql');
		const postgres = recordingShard('postgres');

		initialize({
			kv: createInMemoryKVProvider(),
			shards: { 'db-mysql': mysql.shard, 'db-postgres': postgres.shard },
			strategy: 'hash',
			disableAutoMigration: true,
			hashShardMappings: false
		});

		await runShard('db-mysql', SCHEMA);
		await runShard('db-postgres', SCHEMA);
		mysql.asked.length = 0;
		postgres.asked.length = 0;

		await countAllShards('users');
		await indexAllShards('users', 'name');

		expect(mysql.asked.every((sql) => !sql.includes('"'))).toBe(true);
		expect(postgres.asked.every((sql) => !sql.includes('`'))).toBe(true);
		expect(mysql.asked.some((sql) => sql.includes('COUNT(*) AS row_count FROM `users`'))).toBe(true);
		expect(postgres.asked.some((sql) => sql.includes('COUNT(*) AS row_count FROM "users"'))).toBe(true);
		expect(mysql.asked.some((sql) => sql.includes('ON `users` (`name`)'))).toBe(true);
		expect(postgres.asked.some((sql) => sql.includes('ON "users" ("name")'))).toBe(true);
	});

	it('falls back to double quotes when a provider reports no dialect', async () => {
		const unknown = recordingShard(undefined);

		initialize({
			kv: createInMemoryKVProvider(),
			shards: { 'db-unknown': unknown.shard },
			strategy: 'hash',
			disableAutoMigration: true,
			hashShardMappings: false
		});

		await runShard('db-unknown', SCHEMA);
		unknown.asked.length = 0;

		await insertInto('user-1', 'users', { id: 'user-1', name: 'Ada' });
		expect(unknown.asked.some((sql) => sql.includes('INSERT INTO "users" ("id", "name")'))).toBe(true);
	});
});

describe('Repeated initialization', () => {
	afterEach(() => {
		resetConfig();
	});

	it('keeps the mapping cache across identical initialize calls', async () => {
		const { kv, counts } = countingKV(createInMemoryKVProvider());
		const shards = await makeShards();
		const config = {
			kv,
			shards,
			strategy: 'hash' as const,
			disableAutoMigration: true,
			hashShardMappings: false,
			mappingCacheTtlMs: 60_000
		};

		initialize(config);
		for (const name of Object.keys(shards)) {
			await runShard(name, SCHEMA);
		}
		await run('user-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Ada']);

		counts.get = 0;

		// The documented Workers pattern re-initializes on every request. Building
		// a fresh mapper each time threw the mapping cache away, which made
		// mappingCacheTtlMs dead there: every routed read paid a KV round trip.
		for (let i = 0; i < 5; i++) {
			initialize(config);
			expect(await first<{ name: string }>('user-1', 'SELECT * FROM users WHERE id = ?', ['user-1'])).toMatchObject({ name: 'Ada' });
		}

		expect(counts.get).toBe(0);
	});

	it('registers known shards once per shard set, not once per call', async () => {
		const { kv, counts } = countingKV(createInMemoryKVProvider());
		const shards = await makeShards();
		const config = { kv, shards, strategy: 'hash' as const, disableAutoMigration: true, hashShardMappings: false };

		initialize(config);
		await new Promise((resolve) => setTimeout(resolve, 20));

		const afterFirst = counts.put;
		for (let i = 0; i < 10; i++) {
			initialize(config);
		}
		await new Promise((resolve) => setTimeout(resolve, 20));

		expect(counts.put).toBe(afterFirst);
	});

	it('builds a fresh mapper when the KV store or the shards change', async () => {
		const shards = await makeShards();
		const firstKv = countingKV(createInMemoryKVProvider());

		initialize({ kv: firstKv.kv, shards, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false });
		for (const name of Object.keys(shards)) {
			await runShard(name, SCHEMA);
		}
		await run('user-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Ada']);

		// A different KV store must not inherit the previous mapper's cache.
		const secondKv = countingKV(createInMemoryKVProvider());
		initialize({ kv: secondKv.kv, shards, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false });

		secondKv.counts.get = 0;
		await first('user-1', 'SELECT * FROM users WHERE id = ?', ['user-1']);
		expect(secondKv.counts.get).toBeGreaterThan(0);
	});

	it('rebuilds placement state when the strategy changes', async () => {
		const kv = createInMemoryKVProvider();
		const shards = await makeShards(['db-a', 'db-b', 'db-c']);
		const base = { kv, shards, disableAutoMigration: true, hashShardMappings: false };

		initialize({ ...base, strategy: 'hash' });
		for (const name of Object.keys(shards)) {
			await runShard(name, SCHEMA);
		}
		await run('k-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['k-1', 'Ada']);

		// The strategy benchmark re-initializes per cell with the same bindings but
		// a different strategy, so a cached placement decision cannot carry over.
		initialize({ ...base, strategy: 'round-robin' });
		for (let i = 0; i < 6; i++) {
			await run(`rr-${i}`, 'INSERT INTO users (id, name) VALUES (?, ?)', [`rr-${i}`, `User ${i}`]);
		}

		const perShard = await Promise.all(
			Object.keys(shards).map(async (name) => {
				const result = await runShard<{ c: number }>(name, "SELECT COUNT(*) AS c FROM users WHERE id LIKE 'rr-%'");
				return Number(result.results[0]?.c ?? 0);
			})
		);

		expect(perShard.sort()).toEqual([2, 2, 2]);
	});
});

describe('Out-of-band mapping changes', () => {
	afterEach(() => {
		resetConfig();
	});

	it('sees a mapping changed through a separate mapper once invalidated', async () => {
		const kv = createInMemoryKVProvider();
		const shards = await makeShards();
		const config = {
			kv,
			shards,
			strategy: 'hash' as const,
			disableAutoMigration: true,
			hashShardMappings: true,
			mappingCacheTtlMs: 60_000
		};

		initialize(config);
		for (const name of Object.keys(shards)) {
			await runShard(name, SCHEMA);
		}

		await run('user-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Ada']);

		const mapper = new KVShardMapper(kv, { hashShardMappings: true });
		const source = (await mapper.getShardMapping('user-1'))!.shard;
		const target = source === 'db-a' ? 'db-b' : 'db-a';

		// Move the row and the mapping without going through reassignShard.
		await runShard(target, 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Ada']);
		await runShard(source, 'DELETE FROM users WHERE id = ?', ['user-1']);
		await mapper.updateShardMapping('user-1', target);

		// Still cached, so this process routes to the shard the row just left.
		expect(await first('user-1', 'SELECT * FROM users WHERE id = ?', ['user-1'])).toBeNull();

		await invalidateMappingCache('user-1');
		expect(await first<{ name: string }>('user-1', 'SELECT * FROM users WHERE id = ?', ['user-1'])).toMatchObject({ name: 'Ada' });
	});

	it('clears every cached mapping when called with no key', async () => {
		const kv = createInMemoryKVProvider();
		const shards = await makeShards();
		const config = { kv, shards, strategy: 'hash' as const, disableAutoMigration: true, hashShardMappings: true };

		initialize(config);
		for (const name of Object.keys(shards)) {
			await runShard(name, SCHEMA);
		}
		await run('user-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Ada']);

		await expect(invalidateMappingCache()).resolves.toBeUndefined();
		expect(await first<{ name: string }>('user-1', 'SELECT * FROM users WHERE id = ?', ['user-1'])).toMatchObject({ name: 'Ada' });
	});
});
