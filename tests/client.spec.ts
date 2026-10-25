import type { DatabaseSync } from 'node:sqlite';
import { afterEach, describe, expect, expectTypeOf, it } from 'vitest';
import { setAsyncContextEnabled } from '../src/async-context';
import * as cacheModule from '../src/cache';
import { CollegeDBError } from '../src/errors';
import { KVShardMapper } from '../src/kvmap';
import { clearMigrationCache } from '../src/migrations';
import { swapPlacementCache } from '../src/placement';
import { createInMemoryKVProvider } from '../src/providers-memory';
import * as reconcileModule from '../src/reconcile';
import * as router from '../src/router';
import { collegedb, createClient, getActiveConfig, initialize, isInitialized, resetConfig, run, runShard } from '../src/router';
import { setPhaseObserver, type PhaseSpan } from '../src/telemetry';
import type { CollegeDBConfig, KVStorage, SQLDatabase } from '../src/types';
import { createNodeSqliteProvider, openNodeSqlite } from './helpers/concurrent-sqlite';

const SCHEMA = 'CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)';
const INSERT = 'INSERT INTO users (id, name) VALUES (?, ?)';

interface World {
	name: string;
	kv: KVStorage;
	raw: Record<string, DatabaseSync>;
	shards: Record<string, SQLDatabase>;
	log: Array<{ shard: string; sql: string }>;
	coordinatorCalls: string[];
	hold(): () => void;
	config(extra?: Partial<CollegeDBConfig>): CollegeDBConfig;
}

/** Wraps a shard so every statement it prepares lands in `log`. */
function traced(db: SQLDatabase, shard: string, log: World['log']): SQLDatabase {
	return new Proxy(db, {
		get(target, prop) {
			if (prop === 'prepare') {
				return (sql: string) => {
					log.push({ shard, sql });
					return target.prepare(sql);
				};
			}
			if (prop === 'runBatch' && target.runBatch) {
				return (statements: Array<{ sql: string }>) => {
					for (const statement of statements) {
						log.push({ shard, sql: statement.sql });
					}
					return target.runBatch!(statements as any);
				};
			}
			const value = Reflect.get(target, prop);
			return typeof value === 'function' ? value.bind(target) : value;
		}
	});
}

/** A KV store whose reads wait while `hold()` is active, so a test can park one client mid-operation. */
function gatedKV(inner: KVStorage): { kv: KVStorage; hold(): () => void } {
	let gate: Promise<void> = Promise.resolve();

	const kv: KVStorage = {
		get: (async (key: string, type?: 'text' | 'json') => {
			await gate;
			return type === 'json' ? await inner.get(key, 'json') : await inner.get(key, type);
		}) as KVStorage['get'],
		put: (key, value) => inner.put(key, value),
		delete: (key) => inner.delete(key),
		list: (options) => inner.list(options)
	};

	return {
		kv,
		hold() {
			let release!: () => void;
			gate = new Promise<void>((resolve) => (release = resolve));
			return release;
		}
	};
}

/** Fake coordinator namespace that records the paths it receives and answers the few the tests use. */
function fakeCoordinator(calls: string[], answers: Record<string, unknown> = {}): NonNullable<CollegeDBConfig['coordinator']> {
	return {
		idFromName: (name: string) => name,
		get: () => ({
			fetch: async (url: string) => {
				const path = new URL(url).pathname;
				calls.push(path);
				return new Response(JSON.stringify(answers[path] ?? { value: 1 }), { status: 200 });
			}
		})
	} as unknown as NonNullable<CollegeDBConfig['coordinator']>;
}

/** Two SQLite shards named `<name>-a` and `<name>-b`, a gated KV, and a coordinator that logs. */
function makeWorld(name: string, bindings: string[] = [`${name}-a`, `${name}-b`]): World {
	const log: World['log'] = [];
	const coordinatorCalls: string[] = [];
	const raw: Record<string, DatabaseSync> = {};
	const shards: Record<string, SQLDatabase> = {};

	for (const binding of bindings) {
		raw[binding] = openNodeSqlite();
		raw[binding]!.exec(SCHEMA);
		shards[binding] = traced(createNodeSqliteProvider(raw[binding]!), binding, log);
	}

	const gated = gatedKV(createInMemoryKVProvider());

	return {
		name,
		kv: gated.kv,
		raw,
		shards,
		log,
		coordinatorCalls,
		hold: gated.hold,
		config: (extra = {}) => ({ kv: gated.kv, shards, strategy: 'hash', disableAutoMigration: true, ...extra })
	};
}

function rowsOf(world: World): Array<[string, string, string]> {
	const out: Array<[string, string, string]> = [];
	for (const [binding, db] of Object.entries(world.raw)) {
		for (const row of db.prepare('SELECT id, name FROM users ORDER BY id').all() as Array<{ id: string; name: string }>) {
			out.push([binding, row.id, row.name]);
		}
	}
	return out;
}

const idsOf = (world: World): string[] => rowsOf(world).map((row) => row[1]);

function deferred(): { promise: Promise<void>; resolve: () => void } {
	let resolve!: () => void;
	const promise = new Promise<void>((done) => (resolve = done));
	return { promise, resolve };
}

/** Lets background work started by initialize finish before a test parks a client. */
const settle = () => new Promise<void>((done) => setTimeout(done, 5));

async function codeOf(work: Promise<unknown>): Promise<string | undefined> {
	try {
		await work;
		return undefined;
	} catch (error) {
		return error instanceof CollegeDBError ? error.code : `raw:${String(error)}`;
	}
}

afterEach(() => {
	setAsyncContextEnabled(true);
	setPhaseObserver(null);
	resetConfig();
});

describe.each([
	['with AsyncLocalStorage', true],
	['without AsyncLocalStorage', false]
])('createClient isolation (%s)', (_label, alsEnabled) => {
	afterEach(() => setAsyncContextEnabled(true));

	const open = () => setAsyncContextEnabled(alsEnabled);

	it('routes overlapping writes from two clients to their own shards and KV', async () => {
		open();
		const a = makeWorld('a');
		const b = makeWorld('b');
		const ca = createClient(a.config());
		const cb = createClient(b.config());
		await settle();

		const release = a.hold();
		const parked = ca.run('shared-key', INSERT, ['shared-key', 'from-a']);
		await cb.run('shared-key', INSERT, ['shared-key', 'from-b']);
		expect(rowsOf(a)).toEqual([]);

		release();
		await parked;

		expect(rowsOf(a).map((row) => row[2])).toEqual(['from-a']);
		expect(rowsOf(b).map((row) => row[2])).toEqual(['from-b']);
		expect(rowsOf(a)[0]![0]).toMatch(/^a-/);
		expect(rowsOf(b)[0]![0]).toMatch(/^b-/);
		expect(a.log.every((entry) => entry.shard.startsWith('a-'))).toBe(true);
		expect(b.log.every((entry) => entry.shard.startsWith('b-'))).toBe(true);
		expect(await new KVShardMapper(a.kv).getShardMapping('shared-key')).toMatchObject({ shard: expect.stringMatching(/^a-/) });
		expect(await new KVShardMapper(b.kv).getShardMapping('shared-key')).toMatchObject({ shard: expect.stringMatching(/^b-/) });
	});

	it('keeps reads, first, count and fan-out helpers on the client that issued them', async () => {
		open();
		const a = makeWorld('a');
		const b = makeWorld('b');
		const ca = createClient(a.config());
		const cb = createClient(b.config());

		await Promise.all([ca.run('k1', INSERT, ['k1', 'a1']), cb.run('k1', INSERT, ['k1', 'b1']), cb.run('k2', INSERT, ['k2', 'b2'])]);

		const [firstA, firstB] = await Promise.all([
			ca.first<{ name: string }>('k1', 'SELECT name FROM users WHERE id = ?', ['k1']),
			cb.first<{ name: string }>('k1', 'SELECT name FROM users WHERE id = ?', ['k1'])
		]);
		expect(firstA?.name).toBe('a1');
		expect(firstB?.name).toBe('b1');

		const [allA, allB] = await Promise.all([ca.all('k1', 'SELECT * FROM users WHERE id = ?', ['k1']), cb.all('k2', 'SELECT * FROM users')]);
		expect(allA.results).toHaveLength(1);
		expect(allB.results.length).toBeGreaterThan(0);

		const [countA, countB] = await Promise.all([ca.countAllShards('users'), cb.countAllShards('users')]);
		expect(countA.total).toBe(1);
		expect(countB.total).toBe(2);
		expect(countA.shards.every((entry) => entry.shard.startsWith('a-'))).toBe(true);
		expect(countB.shards.every((entry) => entry.shard.startsWith('b-'))).toBe(true);

		const [firstsA, firstsB] = await Promise.all([
			ca.firstAllShards<{ name: string }>('SELECT name FROM users ORDER BY id'),
			cb.firstAllShards<{ name: string }>('SELECT name FROM users ORDER BY id')
		]);
		expect(firstsA.filter(Boolean).map((row) => row!.name)).toEqual(['a1']);
		const namesB = firstsB.filter(Boolean).map((row) => row!.name);
		expect(namesB.length).toBeGreaterThan(0);
		expect(namesB.every((name) => name.startsWith('b'))).toBe(true);

		const [pageA, pageB] = await Promise.all([
			ca.paginate<{ id: string }>('SELECT id FROM users', [], { limit: 10, sortBy: 'id' }),
			cb.paginate<{ id: string }>('SELECT id FROM users', [], { limit: 10, sortBy: 'id' })
		]);
		expect(pageA.results.map((row) => row.id)).toEqual(['k1']);
		expect(pageB.results.map((row) => row.id)).toEqual(['k1', 'k2']);
		expect(pageA.total).toBe(1);
		expect(pageB.total).toBe(2);
	});

	it('runs overlapping batches against each client own shards', async () => {
		open();
		const a = makeWorld('a');
		const b = makeWorld('b');
		const ca = createClient(a.config());
		const cb = createClient(b.config());
		await settle();

		const release = a.hold();
		const entriesFor = (tag: string) => [1, 2, 3, 4].map((n) => ({ key: `k${n}`, sql: INSERT, bindings: [`k${n}`, `${tag}${n}`] }));
		const parked = ca.batch(entriesFor('a'));
		const groupsB = await cb.batch(entriesFor('b'));
		release();
		const groupsA = await parked;

		expect(groupsA.every((group) => group.shard.startsWith('a-') && !group.error)).toBe(true);
		expect(groupsB.every((group) => group.shard.startsWith('b-') && !group.error)).toBe(true);
		expect(idsOf(a)).toEqual(['k1', 'k2', 'k3', 'k4']);
		expect(rowsOf(a).every((row) => row[2].startsWith('a'))).toBe(true);
		expect(rowsOf(b).every((row) => row[2].startsWith('b'))).toBe(true);
	});

	it('moves a key between shards of its own cluster with a journal, from two clients at once', async () => {
		open();
		const a = makeWorld('a');
		const b = makeWorld('b');
		const ca = createClient(a.config({ journalShard: 'a-a' }));
		const cb = createClient(b.config({ journalShard: 'b-a' }));
		await Promise.all([ca.initializeAsync(a.config({ journalShard: 'a-a' })), cb.initializeAsync(b.config({ journalShard: 'b-a' }))]);

		for (const [world, tag] of [
			[a, 'a'],
			[b, 'b']
		] as const) {
			world.raw[`${world.name}-a`]!.prepare(INSERT).run('mover', `${tag}-row`);
			await new KVShardMapper(world.kv).setShardMapping('mover', `${world.name}-a`);
		}

		await Promise.all([ca.reassignShard('mover', 'a-b', 'users'), cb.reassignShard('mover', 'b-b', 'users')]);

		expect(rowsOf(a)).toEqual([['a-b', 'mover', 'a-row']]);
		expect(rowsOf(b)).toEqual([['b-b', 'mover', 'b-row']]);
		expect(await new KVShardMapper(a.kv).getShardMapping('mover')).toMatchObject({ shard: 'a-b' });
		expect(await new KVShardMapper(b.kv).getShardMapping('mover')).toMatchObject({ shard: 'b-b' });

		const journalRows = (world: World) =>
			(world.raw[`${world.name}-a`]!.prepare('SELECT COUNT(*) AS n FROM collegedb_migration_history').get() as { n: number }).n;
		expect(journalRows(a)).toBeGreaterThan(0);
		expect(journalRows(b)).toBeGreaterThan(0);
		expect(await ca.getMigrationStatus('mover', 'users')).toMatchObject({ destination: 'a-b' });
		expect(await cb.getMigrationStatus('mover', 'users')).toMatchObject({ destination: 'b-b' });
	});

	it('sends coordinator calls to the coordinator of the client that made them', async () => {
		open();
		const a = makeWorld('a');
		const b = makeWorld('b');
		const ca = createClient(
			a.config({ coordinator: fakeCoordinator(a.coordinatorCalls, { '/shards': ['a-a'], '/sequence': { value: 11 } }) })
		);
		const cb = createClient(
			b.config({ coordinator: fakeCoordinator(b.coordinatorCalls, { '/shards': ['b-a'], '/sequence': { value: 22 } }) })
		);

		const [shardsA, shardsB, idA, idB] = await Promise.all([
			ca.listKnownShards(),
			cb.listKnownShards(),
			ca.nextId('users'),
			cb.nextId('users')
		]);

		expect(shardsA).toEqual(['a-a']);
		expect(shardsB).toEqual(['b-a']);
		expect(idA).toBe(11);
		expect(idB).toBe(22);
		expect(a.coordinatorCalls).toEqual(expect.arrayContaining(['/shards', '/sequence']));
		expect(b.coordinatorCalls).toEqual(expect.arrayContaining(['/shards', '/sequence']));

		await ca.flush();
		expect(a.coordinatorCalls).toContain('/flush');
		expect(b.coordinatorCalls).not.toContain('/flush');
	});

	it('keeps the mapping cache per client even when both share one KV store and the same providers', async () => {
		open();
		const shared = makeWorld('s');
		const ca = createClient(shared.config({ hashShardMappings: false }));
		const cb = createClient(shared.config({ hashShardMappings: false }));
		await settle();

		const mapper = new KVShardMapper(shared.kv, { hashShardMappings: false });
		await mapper.setShardMapping('moved', 's-a');
		shared.raw['s-a']!.prepare(INSERT).run('moved', 'on-a');
		shared.raw['s-b']!.prepare(INSERT).run('moved', 'on-b');

		const read = (client: typeof ca) => client.first<{ name: string }>('moved', 'SELECT name FROM users WHERE id = ?', ['moved']);
		expect((await read(ca))?.name).toBe('on-a');

		await mapper.updateShardMapping('moved', 's-b');
		// A holds the old mapping; B never read it, so it sees the new one
		expect((await read(ca))?.name).toBe('on-a');
		expect((await read(cb))?.name).toBe('on-b');

		await ca.invalidateMappingCache('moved');
		expect((await read(ca))?.name).toBe('on-b');
	});

	it('keeps placement caches per client and leaves the default cache empty', async () => {
		open();
		const defaults = swapPlacementCache();
		try {
			const shared = makeWorld('p', ['p-a', 'p-b', 'p-c']);
			const countA = { get: 0 };
			const countB = { get: 0 };
			const counting = (counter: { get: number }): KVStorage => ({
				get: (async (key: string, type?: 'text' | 'json') => {
					if (key.includes('placement')) counter.get++;
					return type === 'json' ? await shared.kv.get(key, 'json') : await shared.kv.get(key, type);
				}) as KVStorage['get'],
				put: (key, value) => shared.kv.put(key, value),
				delete: (key) => shared.kv.delete(key),
				list: (options) => shared.kv.list(options)
			});

			const ca = createClient(shared.config({ kv: counting(countA), placement: 'computed' }));
			const cb = createClient(shared.config({ kv: counting(countB), placement: 'computed' }));

			await ca.run('one', INSERT, ['one', 'a']);
			expect(countA.get).toBeGreaterThan(0);
			expect(countB.get).toBe(0);

			await cb.run('two', INSERT, ['two', 'b']);
			expect(countB.get).toBeGreaterThan(0);

			expect(swapPlacementCache()).toEqual({ manifest: null, exceptions: null });
		} finally {
			swapPlacementCache(defaults);
		}
	});

	it('keeps computed placement decisions per client when their shard sets differ', async () => {
		open();
		const a = makeWorld('a');
		const b = makeWorld('b', ['b-a', 'b-b', 'b-c']);
		const ca = createClient(a.config({ placement: 'computed' }));
		const cb = createClient(b.config({ placement: 'computed' }));
		await settle();

		const release = a.hold();
		const parked = ca.run('k-a', INSERT, ['k-a', 'from-a']);
		await Promise.all([cb.run('k-b1', INSERT, ['k-b1', 'one']), cb.run('k-b2', INSERT, ['k-b2', 'two'])]);
		release();
		await parked;
		await cb.run('k-b3', INSERT, ['k-b3', 'three']);

		// computed placement stores no mapping; a client that fell back to another client's manifest would
		expect(await new KVShardMapper(b.kv).getShardMapping('k-b3')).toBeNull();
		expect(await new KVShardMapper(a.kv).getShardMapping('k-a')).toBeNull();

		expect(rowsOf(a).map((row) => row[1])).toEqual(['k-a']);
		expect(
			rowsOf(b)
				.map((row) => row[1])
				.sort()
		).toEqual(['k-b1', 'k-b2', 'k-b3']);
		expect(await ca.first<{ name: string }>('k-a', 'SELECT name FROM users WHERE id = ?', ['k-a'])).toEqual({ name: 'from-a' });
		expect(await cb.first<{ name: string }>('k-b2', 'SELECT name FROM users WHERE id = ?', ['k-b2'])).toEqual({ name: 'two' });

		const manifestShards = async (world: World) => {
			const manifest = (await world.kv.get('collegedb:placement:manifest', 'json')) as { epochs: Array<{ shards: string[] }> } | null;
			return manifest?.epochs[0]?.shards.sort();
		};
		expect(await manifestShards(a)).toEqual(['a-a', 'a-b']);
		expect(await manifestShards(b)).toEqual(['b-a', 'b-b', 'b-c']);
	});

	it('advances the round-robin position per client', async () => {
		open();
		const shared = makeWorld('r');
		const config = { strategy: 'round-robin' as const };
		const ca = createClient(shared.config(config));
		const cb = createClient(shared.config(config));

		await ca.run('one', INSERT, ['one', 'a']);
		await cb.run('two', INSERT, ['two', 'b']);
		await ca.run('three', INSERT, ['three', 'a']);

		const where = Object.fromEntries(rowsOf(shared).map((row) => [row[1], row[0]]));
		expect(where['one']).toBe('r-a');
		expect(where['two']).toBe('r-a');
		expect(where['three']).toBe('r-b');
	});

	it('registers the known shards in the KV store of each client', async () => {
		open();
		const shared = makeWorld('k');
		const kvA = createInMemoryKVProvider();
		const kvB = createInMemoryKVProvider();

		createClient(shared.config({ kv: kvA }));
		createClient(shared.config({ kv: kvB }));
		await settle();

		expect((await new KVShardMapper(kvA).getKnownShards()).sort()).toEqual(['k-a', 'k-b']);
		expect((await new KVShardMapper(kvB).getKnownShards()).sort()).toEqual(['k-a', 'k-b']);
	});

	it('does not share schema fingerprints between clients on the same shards', async () => {
		open();
		const shared = makeWorld('f');
		const ca = createClient(shared.config());
		const cb = createClient(shared.config());
		const ddl = 'CREATE TABLE IF NOT EXISTS widgets (id TEXT PRIMARY KEY)';
		const runs = () => shared.log.filter((entry) => entry.sql.includes('widgets')).length;

		await ca.ensureSchema(ddl, { once: true });
		const afterFirst = runs();
		expect(afterFirst).toBeGreaterThan(0);

		await ca.ensureSchema(ddl, { once: true });
		expect(runs()).toBe(afterFirst);

		await cb.ensureSchema(ddl, { once: true });
		expect(runs()).toBe(afterFirst * 2);
	});

	it('does not share shard size caches between clients on the same shards', async () => {
		open();
		const shared = makeWorld('z');
		const config = { maxDatabaseSize: 10_000_000_000, sizeCacheTtlMs: 60_000 };
		const ca = createClient(shared.config(config));
		const cb = createClient(shared.config(config));
		const probes = () => shared.log.filter((entry) => /page_count/i.test(entry.sql)).length;

		await ca.run('k1', INSERT, ['k1', 'x']);
		const afterFirst = probes();
		expect(afterFirst).toBeGreaterThan(0);

		await ca.run('k2', INSERT, ['k2', 'x']);
		expect(probes()).toBe(afterFirst);

		await cb.run('k3', INSERT, ['k3', 'x']);
		expect(probes()).toBeGreaterThan(afterFirst);
	});

	it('does not share migration caches between clients, or with the default clearMigrationCache', async () => {
		open();
		const shared = makeWorld('m');
		const config = shared.config({ disableAutoMigration: false });
		const ca = createClient(shared.config());
		const cb = createClient(shared.config());
		const scans = () => shared.log.filter((entry) => entry.sql.includes('sqlite_master')).length;

		await ca.initializeAsync(config);
		const afterFirst = scans();
		expect(afterFirst).toBeGreaterThan(0);

		await ca.initializeAsync(config);
		expect(scans()).toBe(afterFirst);

		clearMigrationCache();
		await ca.initializeAsync(config);
		expect(scans()).toBe(afterFirst);

		await cb.initializeAsync(config);
		expect(scans()).toBeGreaterThan(afterFirst);
	});

	it('reports phase spans only to the observer of the client that made the call', async () => {
		open();
		const a = makeWorld('a');
		const b = makeWorld('b');
		const spansA: PhaseSpan[] = [];
		const spansB: PhaseSpan[] = [];
		const spansDefault: PhaseSpan[] = [];
		setPhaseObserver((span) => spansDefault.push(span));

		const ca = createClient(a.config({ onPhase: (span) => spansA.push(span) }));
		const cb = createClient(b.config({ onPhase: (span) => spansB.push(span) }));

		await Promise.all([ca.run('k', INSERT, ['k', 'a']), cb.run('k', INSERT, ['k', 'b'])]);

		const sqlDetails = (spans: PhaseSpan[]) => spans.filter((span) => span.phase === 'sql.exec').map((span) => span.detail ?? '');
		expect(sqlDetails(spansA).length).toBeGreaterThan(0);
		expect(sqlDetails(spansB).length).toBeGreaterThan(0);
		expect(sqlDetails(spansA).every((detail) => detail.startsWith('a-'))).toBe(true);
		expect(sqlDetails(spansB).every((detail) => detail.startsWith('b-'))).toBe(true);
		expect(spansDefault).toEqual([]);
	});

	it('lets a client and the default instance both stay live, and resetConfig on one leaves the other', async () => {
		open();
		const a = makeWorld('a');
		const d = makeWorld('d');
		const defaultSpans: PhaseSpan[] = [];
		const clientSpans: PhaseSpan[] = [];
		const ca = createClient(a.config({ onPhase: (span) => clientSpans.push(span) }));
		initialize(d.config({ onPhase: (span) => defaultSpans.push(span) }));

		expect(isInitialized()).toBe(true);
		expect(ca.isInitialized()).toBe(true);
		expect(Object.keys(getActiveConfig()!.shards)).toEqual(['d-a', 'd-b']);
		expect(Object.keys(ca.getActiveConfig()!.shards)).toEqual(['a-a', 'a-b']);

		await Promise.all([run('k', INSERT, ['k', 'default']), ca.run('k', INSERT, ['k', 'client'])]);
		expect(rowsOf(d).map((row) => row[2])).toEqual(['default']);
		expect(rowsOf(a).map((row) => row[2])).toEqual(['client']);
		const execDetails = (spans: PhaseSpan[]) => spans.filter((span) => span.phase === 'sql.exec').map((span) => span.detail ?? '');
		expect(execDetails(defaultSpans).length).toBeGreaterThan(0);
		expect(execDetails(clientSpans).length).toBeGreaterThan(0);
		expect(execDetails(defaultSpans).every((detail) => detail.startsWith('d-'))).toBe(true);
		expect(execDetails(clientSpans).every((detail) => detail.startsWith('a-'))).toBe(true);

		resetConfig();
		expect(isInitialized()).toBe(false);
		expect(ca.isInitialized()).toBe(true);
		await ca.run('k2', INSERT, ['k2', 'still-here']);
		expect(idsOf(a)).toContain('k2');

		initialize(d.config());
		ca.resetConfig();
		expect(ca.isInitialized()).toBe(false);
		expect(isInitialized()).toBe(true);
		await runShard('d-a', 'SELECT 1');
	});

	it('reconfigures only the client it was called on', async () => {
		open();
		const a = makeWorld('a');
		const b = makeWorld('b');
		const ca = createClient(a.config());
		const cb = createClient(b.config());

		const next = makeWorld('n');
		ca.initialize(next.config());

		expect(Object.keys(ca.getActiveConfig()!.shards)).toEqual(['n-a', 'n-b']);
		expect(Object.keys(cb.getActiveConfig()!.shards)).toEqual(['b-a', 'b-b']);
		expect(isInitialized()).toBe(false);
	});

	it('throws NOT_INITIALIZED from a client that was reset', async () => {
		open();
		const ca = createClient(makeWorld('a').config());
		ca.resetConfig();
		expect(await codeOf(ca.run('k', INSERT, ['k', 'x']))).toBe('NOT_INITIALIZED');
	});
});

describe('collegedb(config, callback) scoping', () => {
	it('gives each overlapping callback its own configuration across awaits', async () => {
		const a = makeWorld('a');
		const b = makeWorld('b');
		const gate = deferred();
		const seen: Record<string, string[][]> = { a: [], b: [] };
		const shardsNow = () => Object.keys(getActiveConfig()?.shards ?? {});

		const first = collegedb(a.config(), async () => {
			seen.a!.push(shardsNow());
			await gate.promise;
			seen.a!.push(shardsNow());
			await run('k', INSERT, ['k', 'from-a']);
			seen.a!.push(shardsNow());
		});

		const second = collegedb(b.config(), async () => {
			seen.b!.push(shardsNow());
			await run('k', INSERT, ['k', 'from-b']);
			gate.resolve();
			await new Promise((done) => setTimeout(done, 5));
			seen.b!.push(shardsNow());
		});

		await Promise.all([first, second]);

		expect(seen.a).toEqual([
			['a-a', 'a-b'],
			['a-a', 'a-b'],
			['a-a', 'a-b']
		]);
		expect(seen.b).toEqual([
			['b-a', 'b-b'],
			['b-a', 'b-b']
		]);
		expect(rowsOf(a).map((row) => row[2])).toEqual(['from-a']);
		expect(rowsOf(b).map((row) => row[2])).toEqual(['from-b']);
		expect(isInitialized()).toBe(false);
	});

	it('keeps caches warm across calls with the same bindings and starts cold for other options', async () => {
		const world = makeWorld('w');
		const mappingReads: string[] = [];
		const kv: KVStorage = {
			...world.kv,
			get: (async (key: string, type?: 'text' | 'json') => {
				if (key.startsWith('shard:')) mappingReads.push(key);
				return type === 'json' ? await world.kv.get(key, 'json') : await world.kv.get(key, type);
			}) as KVStorage['get']
		};
		const config = world.config({ kv });

		await collegedb({ ...config }, async () => run('k', INSERT, ['k', 'x']));
		mappingReads.length = 0;

		await collegedb({ ...config }, async () => runShard('w-a', 'SELECT 1'));
		await collegedb({ ...config }, async () => router.first('k', 'SELECT name FROM users WHERE id = ?', ['k']));
		expect(mappingReads).toEqual([]);

		await collegedb({ ...config, mappingCacheTtlMs: 1 }, async () => router.first('k', 'SELECT name FROM users WHERE id = ?', ['k']));
		expect(mappingReads.length).toBeGreaterThan(0);
	});

	it('does not touch a live default instance', async () => {
		const d = makeWorld('d');
		const a = makeWorld('a');
		initialize(d.config());

		await collegedb(a.config(), async () => {
			await run('k', INSERT, ['k', 'scoped']);
		});

		expect(Object.keys(getActiveConfig()!.shards)).toEqual(['d-a', 'd-b']);
		expect(rowsOf(a).map((row) => row[2])).toEqual(['scoped']);
		expect(rowsOf(d)).toEqual([]);
	});

	it('refuses a different configuration while another callback is running when ALS is unavailable', async () => {
		setAsyncContextEnabled(false);
		const a = makeWorld('a');
		const b = makeWorld('b');
		const gate = deferred();

		const running = collegedb(a.config(), async () => {
			await gate.promise;
			await run('k', INSERT, ['k', 'from-a']);
		});
		await settle();

		expect(await codeOf(collegedb(b.config(), async () => undefined))).toBe('CONCURRENT_CONFIG');

		gate.resolve();
		await running;
		expect(rowsOf(a).map((row) => row[2])).toEqual(['from-a']);
		expect(rowsOf(b)).toEqual([]);
	});

	it('lets overlapping callbacks with the same configuration run when ALS is unavailable', async () => {
		setAsyncContextEnabled(false);
		const a = makeWorld('a');
		const config = a.config();
		const gate = deferred();

		const first = collegedb(config, async () => {
			await gate.promise;
			await run('k1', INSERT, ['k1', 'one']);
		});
		await settle();
		const second = collegedb({ ...config }, async () => {
			await run('k2', INSERT, ['k2', 'two']);
			gate.resolve();
		});

		await Promise.all([first, second]);
		expect(idsOf(a).sort()).toEqual(['k1', 'k2']);
	});

	it('accepts a different configuration once the earlier callback finished, without ALS', async () => {
		setAsyncContextEnabled(false);
		const a = makeWorld('a');
		const b = makeWorld('b');

		await collegedb(a.config(), async () => run('k', INSERT, ['k', 'from-a']));
		await collegedb(b.config(), async () => run('k', INSERT, ['k', 'from-b']));

		expect(rowsOf(a).map((row) => row[2])).toEqual(['from-a']);
		expect(rowsOf(b).map((row) => row[2])).toEqual(['from-b']);
	});

	it('releases the in-flight claim when the callback throws, without ALS', async () => {
		setAsyncContextEnabled(false);
		const a = makeWorld('a');
		const b = makeWorld('b');

		await expect(
			collegedb(a.config(), async () => {
				throw new Error('boom');
			})
		).rejects.toThrow('boom');
		await expect(collegedb(b.config(), async () => 'ok')).resolves.toBe('ok');
	});
});

describe('client surface', () => {
	const NOT_ON_CLIENT = new Set(['collegedb', 'createClient']);

	const moduleFunctions = (module: Record<string, unknown>): string[] =>
		Object.entries(module)
			.filter(([name, value]) => typeof value === 'function' && !NOT_ON_CLIENT.has(name))
			.map(([name]) => name);

	it('has a method with the same arity for every function the router exports', () => {
		const client = createClient(makeWorld('a').config()) as unknown as Record<string, (...args: unknown[]) => unknown>;
		const names = moduleFunctions(router as unknown as Record<string, unknown>);
		expect(names.length).toBeGreaterThan(40);

		const missing = names.filter((name) => typeof client[name] !== 'function');
		expect(missing).toEqual([]);

		const arity = names.filter(
			(name) => client[name]!.length !== (router as unknown as Record<string, (...args: unknown[]) => unknown>)[name]!.length
		);
		expect(arity).toEqual([]);
	});

	it('exports createClient from the package entry point', async () => {
		const entry = await import('../src/index');
		expect(entry.createClient).toBe(createClient);
	});

	it('binds cached, invalidate and reconcileAllocations to the client', async () => {
		const world = makeWorld('a');
		const client = createClient(world.config());

		for (const [name, module] of [
			['cached', cacheModule],
			['invalidate', cacheModule],
			['reconcileAllocations', reconcileModule]
		] as const) {
			const fn = (module as unknown as Record<string, (...args: unknown[]) => unknown>)[name]!;
			const bound = (client as unknown as Record<string, (...args: unknown[]) => unknown>)[name];
			expect(typeof bound).toBe('function');
			expect(bound!.length).toBe(fn.length);
		}

		let calls = 0;
		expect(await client.cached('answer', async () => ++calls)).toBe(1);
		expect(await client.cached('answer', async () => ++calls)).toBe(1);
		expect(await world.kv.get('answer', 'text')).toContain('"v":1');
		expect(await client.invalidate('answer')).toBe(1);
		expect(await world.kv.get('answer', 'text')).toBeNull();
		expect(await codeOf(client.reconcileAllocations())).toBe('COORDINATOR_NOT_CONFIGURED');
	});

	it('stores lookups in the KV store of the client that wrote them', async () => {
		const a = makeWorld('a');
		const b = makeWorld('b');
		const ca = createClient(a.config());
		const cb = createClient(b.config());

		await ca.setLookup('email:ada', 'a-id');
		expect(await ca.getLookup('email:ada')).toBe('a-id');
		expect(await cb.getLookup('email:ada')).toBeNull();
		await ca.deleteLookup('email:ada');
		expect(await ca.getLookup('email:ada')).toBeNull();
	});

	it('types collegedb as the awaited callback result', async () => {
		const pending = collegedb(makeWorld('typed').config(), async () => 1);
		expectTypeOf(pending).toEqualTypeOf<Promise<number>>();
		expect(await pending).toBe(1);
	});
});
