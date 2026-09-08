import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { cached, invalidate } from '../src/cache';
import { hrwShard } from '../src/placement';
import { createInMemoryKVProvider, createInMemorySQLProvider } from '../src/providers-memory';
import {
	allAllShardsGlobal,
	batch,
	first,
	getDatabaseSizesAllShards,
	initialize,
	insert,
	insertShard,
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
import type { KVStorage, SQLDatabase } from '../src/types';

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
