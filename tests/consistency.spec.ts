import { sql as drizzleSql } from 'drizzle-orm';
import { beforeEach, describe, expect, it } from 'vitest';
import type { CollegeDBConfig, PreparedStatement, QueryResult, SQLDatabase } from '../src/index';
import {
	all,
	allAllShards,
	allAllShardsGlobal,
	allByLookupKey,
	allShard,
	CollegeDBError,
	count,
	countAllShards,
	countShard,
	createDrizzleSQLProvider,
	createInMemoryKVProvider,
	createInMemorySQLProvider,
	createMySQLProvider,
	createPostgreSQLProvider,
	createSQLiteProvider,
	first,
	firstAllShards,
	firstResilient,
	firstShard,
	initialize,
	paginate,
	queryAll,
	queryFirst,
	resetConfig
} from '../src/index';
import { KVShardMapper } from '../src/kvmap';

const SCHEMA = `CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT)`;
const SELECT_ALL = 'SELECT * FROM items';
const SELECT_ONE = 'SELECT * FROM items WHERE id = ?';

interface FakeSession {
	constraint: string | undefined;
	sql: string[];
}

// a raw d1 binding stand-in: plain prepare() is logged, withSession() hands out recorded sessions
function fakeD1(id: string) {
	const inner = createInMemorySQLProvider();
	const plain: string[] = [];
	const sessions: FakeSession[] = [];
	let seq = 0;

	const wrap = (statement: PreparedStatement, onDone: () => void): PreparedStatement => ({
		bind: (...bindings: any[]) => wrap(statement.bind(...bindings), onDone),
		run: async <T>() => {
			const result = await statement.run<T>();
			onDone();
			return result;
		},
		all: async <T>() => {
			const result = await statement.all<T>();
			onDone();
			return result;
		},
		first: async <T>() => {
			const row = await statement.first<T>();
			onDone();
			return row;
		}
	});

	const db = {
		plain,
		sessions,
		inner,
		prepare: (sql: string) => {
			plain.push(sql);
			return inner.prepare(sql);
		},
		withSession: (constraint?: string) => {
			const record: FakeSession = { constraint, sql: [] };
			sessions.push(record);
			let bookmark: string | null = null;
			return {
				prepare: (sql: string) => {
					record.sql.push(sql);
					return wrap(inner.prepare(sql), () => {
						bookmark = `${id}:${++seq}`;
					});
				},
				getBookmark: () => bookmark
			};
		}
	};

	return { db: db as unknown as SQLDatabase & typeof db };
}

async function seededD1(id: string, ids: string[]) {
	const fake = fakeD1(id).db;
	await fake.inner.prepare(SCHEMA).run();
	for (const rowId of ids) {
		await fake.inner.prepare('INSERT INTO items (id, name) VALUES (?, ?)').bind(rowId, `name-${rowId}`).run();
	}
	return fake;
}

// meta.duration is wall-clock ms; drop it and keep every other field
function untimed(result: QueryResult) {
	const { duration: _ms, ...meta } = result.meta;
	return { ...result, meta };
}

function setup(shards: Record<string, SQLDatabase>, extra: Partial<CollegeDBConfig> = {}) {
	resetConfig();
	initialize({
		kv: createInMemoryKVProvider(),
		shards,
		strategy: 'hash',
		disableAutoMigration: true,
		hashShardMappings: false,
		...extra
	});
}

function adapters(): Record<string, SQLDatabase> {
	return {
		memory: createInMemorySQLProvider(),
		postgres: createPostgreSQLProvider({ query: async () => ({ rows: [{ id: 'x' }] as any[] }) }),
		mysql: createMySQLProvider({ execute: async () => [[{ id: 'x' }], []] }),
		sqlite: createSQLiteProvider({
			prepare: () => ({ all: () => [{ id: 'x' }], get: () => ({ id: 'x' }), run: () => ({ changes: 0 }) })
		}),
		drizzle: createDrizzleSQLProvider(
			{ all: async () => ({ rows: [{ id: 'x' }] }), run: async () => ({ rows: [] }) } as any,
			drizzleSql,
			'sqlite'
		)
	};
}

describe('read consistency', () => {
	beforeEach(() => {
		resetConfig();
	});

	describe('constraint mapping', () => {
		it('maps primary to first-primary', async () => {
			const a = await seededD1('uuid-a', ['k1']);
			setup({ 'd1-a': a }, { shardState: { 'd1-a': { id: 'uuid-a' } } });

			await firstShard('d1-a', SELECT_ONE, ['k1'], { consistency: { mode: 'primary' } });

			expect(a.sessions.map((s) => s.constraint)).toEqual(['first-primary']);
		});

		it('maps eventual to first-unconstrained', async () => {
			const a = await seededD1('uuid-a', ['k1']);
			setup({ 'd1-a': a });

			await firstShard('d1-a', SELECT_ONE, ['k1'], { consistency: { mode: 'eventual' } });

			expect(a.sessions.map((s) => s.constraint)).toEqual(['first-unconstrained']);
		});

		it('starts a session without bookmarks as first-unconstrained', async () => {
			const a = await seededD1('uuid-a', ['k1']);
			setup({ 'd1-a': a });

			await firstShard('d1-a', SELECT_ONE, ['k1'], { consistency: { mode: 'session' } });

			expect(a.sessions.map((s) => s.constraint)).toEqual(['first-unconstrained']);
		});

		it('resumes a session from the bookmark of its own database id', async () => {
			const a = await seededD1('uuid-a', ['k1']);
			const b = await seededD1('uuid-b', ['k2']);
			const c = await seededD1('uuid-c', ['k3']);
			setup(
				{ 'd1-a': a, 'd1-b': b, 'd1-c': c },
				{ shardState: { 'd1-a': { id: 'uuid-a' }, 'd1-b': { id: 'uuid-b' }, 'd1-c': { id: 'uuid-c' } } }
			);

			await allAllShards(SELECT_ALL, [], 50, { consistency: { mode: 'session', bookmarks: { 'uuid-a': 'bm-a', 'uuid-b': 'bm-b' } } });

			expect(a.sessions.map((s) => s.constraint)).toEqual(['bm-a']);
			expect(b.sessions.map((s) => s.constraint)).toEqual(['bm-b']);
			expect(c.sessions.map((s) => s.constraint)).toEqual(['first-unconstrained']);
		});

		it('does not apply a bookmark by binding name', async () => {
			const a = await seededD1('uuid-a', ['k1']);
			setup({ 'd1-a': a }, { shardState: { 'd1-a': { id: 'uuid-a' } } });

			await firstShard('d1-a', SELECT_ONE, ['k1'], { consistency: { mode: 'session', bookmarks: { 'd1-a': 'by-name' } } });

			expect(a.sessions.map((s) => s.constraint)).toEqual(['first-unconstrained']);
		});
	});

	describe('bookmark chaining', () => {
		it('returns the latest bookmark on meta and accepts it back', async () => {
			const a = await seededD1('uuid-a', ['k1']);
			setup({ 'd1-a': a }, { shardState: { 'd1-a': { id: 'uuid-a' } } });

			const first1 = await all('k1', SELECT_ONE, ['k1'], { consistency: { mode: 'session' } });
			expect(first1.meta.bookmark).toBe('uuid-a:1');

			await all('k1', SELECT_ONE, ['k1'], { consistency: { mode: 'session', bookmarks: { 'uuid-a': first1.meta.bookmark! } } });

			expect(a.sessions.map((s) => s.constraint)).toEqual(['first-unconstrained', 'uuid-a:1']);
		});

		it('returns a bookmark for an eventual read and none on the default path', async () => {
			const a = await seededD1('uuid-a', []);
			setup({ 'd1-a': a });

			const result = await allShard('d1-a', SELECT_ALL, [], { consistency: { mode: 'eventual' } });

			expect(result.meta.bookmark).toBe('uuid-a:1');
			expect(Object.keys((await allShard('d1-a', SELECT_ALL)).meta)).not.toContain('bookmark');
		});

		it('keys merged bookmarks by database id', async () => {
			const a = await seededD1('uuid-a', ['k1']);
			const b = await seededD1('uuid-b', ['k2']);
			setup({ 'd1-a': a, 'd1-b': b }, { shardState: { 'd1-a': { id: 'uuid-a' }, 'd1-b': { id: 'uuid-b' } } });

			const merged = await allAllShardsGlobal(SELECT_ALL, [], { consistency: { mode: 'session' } });

			expect(merged.meta.bookmarks).toEqual({ 'uuid-a': 'uuid-a:1', 'uuid-b': 'uuid-b:1' });
		});

		it('leaves merged bookmarks off when a shard has no verified id', async () => {
			const a = await seededD1('uuid-a', ['k1']);
			setup({ 'd1-a': a });

			const merged = await allAllShardsGlobal(SELECT_ALL, [], { consistency: { mode: 'session' } });

			expect(Object.keys(merged.meta)).not.toContain('bookmarks');
		});

		it('stamps each per-shard result of a fan-out with its bookmark', async () => {
			const a = await seededD1('uuid-a', ['k1']);
			const b = await seededD1('uuid-b', ['k2']);
			setup({ 'd1-a': a, 'd1-b': b });

			const results = await allAllShards(SELECT_ALL, [], 50, { consistency: { mode: 'session' } });

			expect(results.map((r) => r.meta.bookmark).sort()).toEqual(['uuid-a:1', 'uuid-b:1']);
		});
	});

	describe('one session per shard per operation', () => {
		it('reuses one session for every key a statement routes to on a shard', async () => {
			const a = await seededD1('uuid-a', ['k1', 'k2']);
			setup({ 'd1-a': a });

			const result = await queryAll('SELECT * FROM items WHERE id IN (?, ?)', ['k1', 'k2'], { consistency: { mode: 'session' } });

			expect(result.success).toBe(true);
			expect(a.sessions).toHaveLength(1);
			expect(a.sessions[0]!.sql).toHaveLength(2);
		});

		it('reuses one session across the routed read and the fan-out of firstResilient', async () => {
			const a = await seededD1('uuid-a', []);
			const b = await seededD1('uuid-b', []);
			setup({ 'd1-a': a, 'd1-b': b });

			expect(await firstResilient('missing', SELECT_ONE, ['missing'], { consistency: { mode: 'session' } })).toBeNull();

			expect(a.sessions.length + b.sessions.length).toBe(2);
			expect(a.sessions.length).toBeLessThanOrEqual(1);
			expect(b.sessions.length).toBeLessThanOrEqual(1);
			expect(a.sessions.concat(b.sessions).reduce((sum, s) => sum + s.sql.length, 0)).toBe(3);
		});

		it('reuses the mapped shard session when a lookup key falls back to fan-out', async () => {
			const a = await seededD1('uuid-a', ['k1']);
			const b = await seededD1('uuid-b', []);
			const kv = createInMemoryKVProvider();
			setup({ 'd1-a': a, 'd1-b': b }, { kv });
			await new KVShardMapper(kv, { hashShardMappings: false }).setShardMapping('email:ada@example.com', 'd1-b');

			const result = await allByLookupKey('email:ada@example.com', SELECT_ALL, [], 50, { consistency: { mode: 'session' } });

			expect(result.results).toHaveLength(1);
			expect(a.sessions).toHaveLength(1);
			expect(b.sessions).toHaveLength(1);
			expect(b.sessions[0]!.sql).toHaveLength(2);
		});

		it('opens one session per shard for paginate and returns per-database bookmarks, not one snapshot', async () => {
			const a = await seededD1('uuid-a', ['k1', 'k2', 'k3']);
			const b = await seededD1('uuid-b', ['k4', 'k5', 'k6']);
			setup({ 'd1-a': a, 'd1-b': b }, { shardState: { 'd1-a': { id: 'uuid-a' }, 'd1-b': { id: 'uuid-b' } } });

			const page = await paginate(SELECT_ALL, [], { limit: 2, sortBy: 'id', consistency: { mode: 'session' } });

			expect(a.sessions).toHaveLength(1);
			expect(b.sessions).toHaveLength(1);
			expect(page.total).toBe(6);
			expect(page.results).toHaveLength(2);
			expect(Object.keys(page.bookmarks ?? {}).sort()).toEqual(['uuid-a', 'uuid-b']);
			expect(page.bookmarks!['uuid-a']).not.toBe(page.bookmarks!['uuid-b']);
			expect(Object.keys(page).sort()).toEqual(['bookmarks', 'limit', 'page', 'pages', 'results', 'total']);
		});

		it('runs count through the session of its shard', async () => {
			const a = await seededD1('uuid-a', ['k1', 'k2']);
			setup({ 'd1-a': a });

			expect(await count('k1', 'items', { consistency: { mode: 'primary' } })).toBe(2);
			expect(await countShard('d1-a', 'items', { consistency: { mode: 'eventual' } })).toBe(2);
			expect((await countAllShards('items', 50, { consistency: { mode: 'session' } })).total).toBe(2);

			expect(a.sessions.map((s) => s.constraint)).toEqual(['first-primary', 'first-unconstrained', 'first-unconstrained']);
		});

		it('threads the option through first, queryFirst and the all-shards firsts', async () => {
			const a = await seededD1('uuid-a', ['k1']);
			setup({ 'd1-a': a });

			await first('k1', SELECT_ONE, ['k1'], { consistency: { mode: 'primary' } });
			await queryFirst(SELECT_ONE, ['k1'], { consistency: { mode: 'primary' } });
			await firstAllShards(SELECT_ONE, ['k1'], 50, { consistency: { mode: 'primary' } });

			expect(a.sessions.map((s) => s.constraint)).toEqual(['first-primary', 'first-primary', 'first-primary']);
		});
	});

	describe('providers without sessions', () => {
		it.each(['memory', 'postgres', 'mysql', 'sqlite', 'drizzle'])('rejects a session read on %s', async (name) => {
			setup({ shard: adapters()[name]! });

			await expect(allShard('shard', SELECT_ALL, [], { consistency: { mode: 'session' } })).rejects.toMatchObject({
				name: 'CollegeDBError',
				code: 'CONSISTENCY_UNSUPPORTED'
			});
		});

		it.each(['memory', 'postgres', 'mysql', 'sqlite', 'drizzle'])('accepts primary and eventual reads on %s', async (name) => {
			setup({ shard: adapters()[name]! });

			const primary = await allShard('shard', SELECT_ALL, [], { consistency: { mode: 'primary' } });
			const eventual = await allShard('shard', SELECT_ALL, [], { consistency: { mode: 'eventual' } });

			expect(primary.success).toBe(true);
			expect(eventual.success).toBe(true);
			expect(Object.keys(primary.meta)).not.toContain('bookmark');
		});

		it('rejects a session read in a fan-out when any shard cannot hold one', async () => {
			const a = await seededD1('uuid-a', ['k1']);
			setup({ 'd1-a': a, pg: adapters().postgres! });

			await expect(allAllShardsGlobal(SELECT_ALL, [], { consistency: { mode: 'session' } })).rejects.toMatchObject({
				code: 'CONSISTENCY_UNSUPPORTED'
			});
		});

		it('refuses a request the provider reports it cannot serve', async () => {
			const inner = createInMemorySQLProvider();
			const db: SQLDatabase = { prepare: (sql) => inner.prepare(sql), consistency: { primary: false, session: false, eventual: true } };
			setup({ shard: db });

			await expect(allShard('shard', SELECT_ALL, [], { consistency: { mode: 'primary' } })).rejects.toMatchObject({
				code: 'CONSISTENCY_UNSUPPORTED'
			});
			expect((await allShard('shard', SELECT_ALL, [], { consistency: { mode: 'eventual' } })).success).toBe(true);
		});

		it('refuses a provider that reports sessions but has no withSession', async () => {
			const inner = createInMemorySQLProvider();
			const db: SQLDatabase = { prepare: (sql) => inner.prepare(sql), consistency: { primary: true, session: true, eventual: true } };
			setup({ shard: db });

			await expect(allShard('shard', SELECT_ALL, [], { consistency: { mode: 'session' } })).rejects.toMatchObject({
				code: 'CONSISTENCY_UNSUPPORTED'
			});
		});
	});

	describe('bookmark identity', () => {
		it('rejects supplied bookmarks when the shard has no verified database id', async () => {
			const a = await seededD1('uuid-a', ['k1']);
			setup({ 'd1-a': a });

			const error = await firstShard('d1-a', SELECT_ONE, ['k1'], { consistency: { mode: 'session', bookmarks: { 'uuid-a': 'bm' } } }).catch(
				(e) => e
			);

			expect(error).toBeInstanceOf(CollegeDBError);
			expect(error.code).toBe('BOOKMARK_IDENTITY_UNVERIFIED');
			expect(a.sessions).toHaveLength(0);
		});

		it('rejects supplied bookmarks when shardState has an entry without an id', async () => {
			const a = await seededD1('uuid-a', ['k1']);
			setup({ 'd1-a': a }, { shardState: { 'd1-a': {} } });

			await expect(
				allShard('d1-a', SELECT_ALL, [], { consistency: { mode: 'session', bookmarks: { 'uuid-a': 'bm' } } })
			).rejects.toMatchObject({ code: 'BOOKMARK_IDENTITY_UNVERIFIED' });
		});

		it('does not require an id when no bookmark is supplied', async () => {
			const a = await seededD1('uuid-a', ['k1']);
			setup({ 'd1-a': a });

			await expect(allShard('d1-a', SELECT_ALL, [], { consistency: { mode: 'session', bookmarks: {} } })).resolves.toMatchObject({
				success: true
			});
		});
	});

	describe('input validation', () => {
		it('rejects an unknown mode', async () => {
			const a = await seededD1('uuid-a', ['k1']);
			setup({ 'd1-a': a });

			await expect(allShard('d1-a', SELECT_ALL, [], { consistency: { mode: 'strong' } as any })).rejects.toMatchObject({
				code: 'INVALID_CONSISTENCY'
			});
		});
	});

	describe('default path', () => {
		it('issues the same statements and opens no session without a consistency option', async () => {
			const a = await seededD1('uuid-a', ['k1']);
			const b = await seededD1('uuid-b', ['k2']);
			setup({ 'd1-a': a, 'd1-b': b }, { shardState: { 'd1-a': { id: 'uuid-a' }, 'd1-b': { id: 'uuid-b' } } });

			await all('k1', SELECT_ONE, ['k1']);
			await first('k1', SELECT_ONE, ['k1']);
			await allShard('d1-a', SELECT_ALL);
			await firstShard('d1-b', SELECT_ONE, ['k2']);
			await allAllShards(SELECT_ALL);
			await firstResilient('k1', SELECT_ONE, ['k1']);
			const merged = await allAllShardsGlobal(SELECT_ALL);
			await paginate(SELECT_ALL, [], { limit: 1 });

			expect(a.sessions).toHaveLength(0);
			expect(b.sessions).toHaveLength(0);
			expect(a.plain.length + b.plain.length).toBeGreaterThanOrEqual(8);
			expect(Object.keys(merged.meta)).not.toContain('bookmarks');
		});

		it('answers an empty options object the way it answers none', async () => {
			const a = await seededD1('uuid-a', ['k1']);
			const b = await seededD1('uuid-b', ['k2']);
			setup({ 'd1-a': a, 'd1-b': b });

			const bare = {
				shard: await allShard('d1-a', SELECT_ALL),
				fan: await allAllShards(SELECT_ALL),
				one: await first('k1', SELECT_ONE, ['k1']),
				many: await all('k1', SELECT_ONE, ['k1'])
			};
			const empty = {
				shard: await allShard('d1-a', SELECT_ALL, [], {}),
				fan: await allAllShards(SELECT_ALL, [], 50, {}),
				one: await first('k1', SELECT_ONE, ['k1'], {}),
				many: await all('k1', SELECT_ONE, ['k1'], {})
			};

			const comparable = (r: typeof bare) => ({
				shard: untimed(r.shard),
				fan: r.fan.map(untimed),
				one: r.one,
				many: untimed(r.many)
			});
			expect(comparable(empty)).toEqual(comparable(bare));
			expect(a.sessions).toHaveLength(0);
			expect(b.sessions).toHaveLength(0);
		});

		it('passes the result of a default read through untouched', async () => {
			const a = await seededD1('uuid-a', ['k1']);
			setup({ 'd1-a': a });

			const direct = (await a.inner.prepare(SELECT_ALL).all()) as QueryResult;
			const routed = await allShard('d1-a', SELECT_ALL);

			expect(routed.results).toEqual(direct.results);
			expect(Object.keys(routed.meta).sort()).toEqual(Object.keys(direct.meta).sort());
		});
	});
});
