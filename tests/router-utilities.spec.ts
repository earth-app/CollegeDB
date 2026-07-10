import { beforeEach, describe, expect, it } from 'vitest';
import type { InMemorySQLDatabase, SQLDatabase } from '../src/index';
import {
	createInMemoryKVProvider,
	createInMemorySQLProvider,
	ensureSchema,
	first,
	firstResilient,
	initialize,
	initializeFromEnv,
	insertInto,
	isInitialized,
	nextId,
	paginate,
	resetConfig
} from '../src/index';
import { KVShardMapper } from '../src/kvmap';

const ITEMS_SCHEMA = `CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, score INTEGER)`;
const SEQ_SCHEMA = `CREATE TABLE seq_items (id INTEGER PRIMARY KEY, name TEXT)`;

function makeShards(): { kv: ReturnType<typeof createInMemoryKVProvider>; east: InMemorySQLDatabase; west: InMemorySQLDatabase } {
	const kv = createInMemoryKVProvider();
	const east = createInMemorySQLProvider();
	const west = createInMemorySQLProvider();
	return { kv, east, west };
}

// Wraps a real in-memory provider and records CREATE TABLE statements so tests
// can assert whether schema DDL actually ran (the emulator auto-creates tables
// on INSERT, so table existence alone is not a reliable signal).
function countingProvider(): { db: SQLDatabase; creates: string[] } {
	const inner = createInMemorySQLProvider();
	const creates: string[] = [];
	const db: SQLDatabase = {
		prepare(sqlText: string) {
			if (/create\s+table/i.test(sqlText)) {
				creates.push(sqlText);
			}
			return inner.prepare(sqlText);
		}
	};
	return { db, creates };
}

describe('Router utility helpers', () => {
	beforeEach(() => {
		resetConfig();
	});

	describe('isInitialized', () => {
		it('reflects initialization state', () => {
			expect(isInitialized()).toBe(false);
			initialize({ kv: createInMemoryKVProvider(), shards: { a: createInMemorySQLProvider() }, disableAutoMigration: true });
			expect(isInitialized()).toBe(true);
			resetConfig();
			expect(isInitialized()).toBe(false);
		});
	});

	describe('nextId', () => {
		it('returns max(id)+1 across all shards (not just the routed shard)', async () => {
			const { kv, east, west } = makeShards();
			await east.prepare(SEQ_SCHEMA).run();
			await west.prepare(SEQ_SCHEMA).run();
			await east.prepare('INSERT INTO seq_items (id, name) VALUES (?, ?)').bind(5, 'e').run();
			await west.prepare('INSERT INTO seq_items (id, name) VALUES (?, ?)').bind(9, 'w').run();

			initialize({
				kv,
				shards: { 'db-east': east, 'db-west': west },
				strategy: 'hash',
				disableAutoMigration: true,
				hashShardMappings: false
			});

			expect(await nextId('seq_items')).toBe(10);
		});

		it('returns 1 for an empty table and honors the min floor', async () => {
			const { kv, east, west } = makeShards();
			await east.prepare(SEQ_SCHEMA).run();
			await west.prepare(SEQ_SCHEMA).run();
			initialize({
				kv,
				shards: { 'db-east': east, 'db-west': west },
				strategy: 'hash',
				disableAutoMigration: true,
				hashShardMappings: false
			});

			expect(await nextId('seq_items')).toBe(1);
			expect(await nextId('seq_items', { min: 100 })).toBe(100);
		});

		it('uses the coordinator sequence atomically when configured', async () => {
			const { kv, east, west } = makeShards();
			await east.prepare(SEQ_SCHEMA).run();
			await west.prepare(SEQ_SCHEMA).run();

			const counters = new Map<string, number>();
			const requests: Array<{ name: string; min?: number }> = [];
			const coordinator = {
				idFromName: () => 'default',
				get: () => ({
					async fetch(_url: string, init: { body: string }) {
						const body = JSON.parse(init.body) as { name: string; min?: number };
						requests.push(body);
						const current = counters.get(body.name) ?? 0;
						const next = Math.max(current + 1, body.min ?? 0);
						counters.set(body.name, next);
						return new Response(JSON.stringify({ value: next }), { status: 200 });
					}
				})
			};

			initialize({
				kv,
				coordinator: coordinator as any,
				shards: { 'db-east': east, 'db-west': west },
				strategy: 'hash',
				disableAutoMigration: true,
				hashShardMappings: false
			});

			const first = await nextId('seq_items');
			const second = await nextId('seq_items');
			expect(first).toBe(1);
			expect(second).toBe(2);
			expect(requests[0]).toEqual({ name: 'seq_items', min: 1 });
		});
	});

	describe('firstResilient', () => {
		it('falls back to a global scan when the routed shard misses (stale mapping)', async () => {
			const { kv, east, west } = makeShards();
			await east.prepare(ITEMS_SCHEMA).run();
			await west.prepare(ITEMS_SCHEMA).run();

			initialize({
				kv,
				shards: { 'db-east': east, 'db-west': west },
				strategy: 'hash',
				disableAutoMigration: true,
				hashShardMappings: false
			});

			// Point the mapping at the wrong shard, put the data on the other one.
			const mapper = new KVShardMapper(kv, { hashShardMappings: false });
			await mapper.setShardMapping('ghost', 'db-east');
			await west.prepare('INSERT INTO items (id, name, score) VALUES (?, ?, ?)').bind('ghost', 'Found', 1).run();

			const routed = await first('ghost', 'SELECT * FROM items WHERE id = ?', ['ghost']);
			expect(routed).toBeNull();

			const resilient = await firstResilient<{ name: string }>('ghost', 'SELECT * FROM items WHERE id = ?', ['ghost']);
			expect(resilient?.name).toBe('Found');
		});

		it('returns null when neither the routed read nor the scan match', async () => {
			const { kv, east } = makeShards();
			await east.prepare(ITEMS_SCHEMA).run();
			initialize({ kv, shards: { 'db-east': east }, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false });

			const row = await firstResilient('missing', 'SELECT * FROM items WHERE id = ?', ['missing']);
			expect(row).toBeNull();
		});
	});

	describe('paginate', () => {
		beforeEach(async () => {
			const { kv, east, west } = makeShards();
			await east.prepare(ITEMS_SCHEMA).run();
			await west.prepare(ITEMS_SCHEMA).run();
			initialize({
				kv,
				shards: { 'db-east': east, 'db-west': west },
				strategy: 'hash',
				disableAutoMigration: true,
				hashShardMappings: false
			});

			for (let i = 1; i <= 25; i++) {
				await insertInto(`item-${i}`, 'items', { id: `item-${i}`, name: `n${i}`, score: i });
			}
		});

		it('returns a page plus the pre-slice total and page count', async () => {
			const page1 = await paginate<{ score: number }>('SELECT * FROM items', [], {
				page: 1,
				limit: 10,
				sortBy: 'score',
				sortDirection: 'asc'
			});
			expect(page1.results.map((r) => r.score)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
			expect(page1.total).toBe(25);
			expect(page1.pages).toBe(3);
			expect(page1.page).toBe(1);
			expect(page1.limit).toBe(10);
		});

		it('returns the correct slice for a later page', async () => {
			const page3 = await paginate<{ score: number }>('SELECT * FROM items', [], {
				page: 3,
				limit: 10,
				sortBy: 'score',
				sortDirection: 'asc'
			});
			expect(page3.results.map((r) => r.score)).toEqual([21, 22, 23, 24, 25]);
			expect(page3.total).toBe(25);
		});
	});

	describe('ensureSchema', () => {
		it('runs the DDL on every configured shard', async () => {
			const kv = createInMemoryKVProvider();
			const east = countingProvider();
			const west = countingProvider();
			initialize({
				kv,
				shards: { 'db-east': east.db, 'db-west': west.db },
				strategy: 'hash',
				disableAutoMigration: true,
				hashShardMappings: false
			});

			await ensureSchema([`CREATE TABLE IF NOT EXISTS widgets (id TEXT PRIMARY KEY, label TEXT)`]);
			expect(east.creates).toHaveLength(1);
			expect(west.creates).toHaveLength(1);

			await insertInto('w-1', 'widgets', { id: 'w-1', label: 'ok' });
			const row = await first<{ label: string }>('w-1', 'SELECT label FROM widgets WHERE id = ?', ['w-1']);
			expect(row?.label).toBe('ok');
		});

		it('skips the DDL when the KV version marker already matches', async () => {
			const kv = createInMemoryKVProvider();
			const east = countingProvider();
			initialize({ kv, shards: { 'db-east': east.db }, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false });

			await kv.put('schema:version', '2');
			await ensureSchema([`CREATE TABLE IF NOT EXISTS gated (id TEXT PRIMARY KEY)`], { versionKey: 'schema:version', version: '2' });

			expect(east.creates).toHaveLength(0);
		});

		it('runs the DDL and writes the marker when the version differs', async () => {
			const kv = createInMemoryKVProvider();
			const east = countingProvider();
			initialize({ kv, shards: { 'db-east': east.db }, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false });

			await ensureSchema([`CREATE TABLE IF NOT EXISTS marked (id TEXT PRIMARY KEY)`], { versionKey: 'schema:v', version: '5' });
			expect(east.creates).toHaveLength(1);
			expect(await kv.get('schema:v', 'text')).toBe('5');
		});

		it('runs once per process with the once guard', async () => {
			const kv = createInMemoryKVProvider();
			const east = countingProvider();
			initialize({ kv, shards: { 'db-east': east.db }, strategy: 'hash', disableAutoMigration: true, hashShardMappings: false });

			const ddl = [`CREATE TABLE IF NOT EXISTS onced (id TEXT PRIMARY KEY)`];
			await ensureSchema(ddl, { once: true });
			await ensureSchema(ddl, { once: true });

			expect(east.creates).toHaveLength(1);
		});
	});

	describe('initializeFromEnv', () => {
		it('discovers DB_* shard bindings and the primary DB from env', async () => {
			const env: Record<string, unknown> = {
				KV: createInMemoryKVProvider(),
				CACHE: {},
				DB: createInMemorySQLProvider(),
				DB_EAST: createInMemorySQLProvider(),
				DB_WEST: createInMemorySQLProvider()
			};

			const shardNames = initializeFromEnv(env, { strategy: 'hash', disableAutoMigration: true, hashShardMappings: false });
			expect(shardNames.sort()).toEqual(['db-east', 'db-primary', 'db-west']);
			expect(isInitialized()).toBe(true);

			// A full write/read round-trip proves the shards are wired.
			await ensureSchema([`CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)`]);
			await insertInto('k1', 't', { id: 'k1', v: 'hello' });
			const row = await first<{ v: string }>('k1', 'SELECT v FROM t WHERE id = ?', ['k1']);
			expect(row?.v).toBe('hello');
		});

		it('throws when no KV binding can be resolved', () => {
			expect(() => initializeFromEnv({ DB_EAST: createInMemorySQLProvider() })).toThrow('No KV binding found');
		});

		it('throws when no shard bindings are found', () => {
			expect(() => initializeFromEnv({ KV: createInMemoryKVProvider() })).toThrow('No shard bindings found');
		});
	});
});
