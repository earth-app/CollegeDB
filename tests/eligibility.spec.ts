import type { DatabaseSync } from 'node:sqlite';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
	batch,
	first,
	hrwShard,
	initialize,
	pruneEpochs,
	publishEligibility,
	rebalance,
	resetConfig,
	run,
	runShard,
	upsert
} from '../src/index';
import type { PlacementManifest } from '../src/placement';
import { resetPlacementState } from '../src/placement';
import { createInMemoryKVProvider, createInMemorySQLProvider } from '../src/providers-memory';
import type { CollegeDBConfig, EligibilityContext, KVStorage, ShardState, SQLDatabase } from '../src/types';
import { createNodeSqliteProvider, openNodeSqlite } from './helpers/concurrent-sqlite';

const SCHEMA = 'CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)';
const MANIFEST_KEY = 'collegedb:placement:manifest';
const NAMES = ['db-a', 'db-b', 'db-c'];
const KEYS = Array.from({ length: 200 }, (_, i) => `key-${i}`);

interface World {
	kv: KVStorage;
	shards: Record<string, SQLDatabase>;
	raw?: Record<string, DatabaseSync>;
}

function memoryWorld(names: string[] = NAMES): World {
	return {
		kv: createInMemoryKVProvider(),
		shards: Object.fromEntries(names.map((name) => [name, createInMemorySQLProvider()]))
	};
}

/** Real SQLite shards, which the journal (manifest swap, prune, moves) needs. */
function journalWorld(names: string[] = NAMES): World {
	const raw: Record<string, DatabaseSync> = {};
	const shards: Record<string, SQLDatabase> = {};
	for (const name of names) {
		raw[name] = openNodeSqlite();
		raw[name].exec(SCHEMA);
		shards[name] = createNodeSqliteProvider(raw[name]);
	}
	return { kv: createInMemoryKVProvider(), shards, raw };
}

/** Starts a fresh process over the world's databases. */
function boot(world: World, extra: Partial<CollegeDBConfig> = {}): void {
	resetConfig();
	resetPlacementState();
	initialize({
		kv: world.kv,
		shards: world.shards,
		strategy: 'hash',
		disableAutoMigration: true,
		hashShardMappings: false,
		...extra
	});
}

async function createSchemas(world: World): Promise<void> {
	for (const name of Object.keys(world.shards)) {
		await runShard(name, SCHEMA);
	}
}

async function holders(world: World, key: string): Promise<string[]> {
	const found: string[] = [];
	for (const [name, db] of Object.entries(world.shards)) {
		const result = await db.prepare('SELECT COUNT(*) AS c FROM users WHERE id = ?').bind(key).all<{ c: number }>();
		if (Number(result.results[0]?.c ?? 0) > 0) found.push(name);
	}
	return found;
}

async function tally(world: World, keys: string[]): Promise<Record<string, number>> {
	const counts: Record<string, number> = Object.fromEntries(Object.keys(world.shards).map((name) => [name, 0]));
	for (const key of keys) {
		for (const holder of await holders(world, key)) {
			counts[holder]!++;
		}
	}
	return counts;
}

async function insertAll(keys: string[]): Promise<void> {
	for (const key of keys) {
		await run(key, 'INSERT INTO users (id, name) VALUES (?, ?)', [key, `name of ${key}`]);
	}
}

async function codeOf(work: Promise<unknown>): Promise<string | undefined> {
	try {
		await work;
		return undefined;
	} catch (error) {
		return (error as { code?: string }).code ?? `raw:${(error as Error).message}`;
	}
}

function storedManifest(world: World): PlacementManifest {
	const row = world.raw!['db-a']!.prepare('SELECT body FROM collegedb_manifest').get() as { body: string };
	return JSON.parse(row.body) as PlacementManifest;
}

async function kvManifest(world: World): Promise<PlacementManifest | null> {
	return await world.kv.get<PlacementManifest>(MANIFEST_KEY, 'json');
}

/** A coordinator namespace that records what it was asked and always answers with `answer`. */
function recordingCoordinator(answer: (shards: string[]) => string) {
	const requests: Array<{ availableShards?: string[]; primaryKey: string }> = [];
	const namespace = {
		idFromName: (name: string) => name,
		get: () => ({
			fetch: async (_url: string, init: { body: string }) => {
				const body = JSON.parse(init.body) as { availableShards?: string[]; primaryKey: string };
				requests.push(body);
				return new Response(JSON.stringify({ shard: answer(body.availableShards ?? []) }));
			}
		})
	};
	return { namespace: namespace as unknown as CollegeDBConfig['coordinator'], requests };
}

afterEach(() => {
	resetConfig();
	resetPlacementState();
});

// #region characterization

describe('Without eligibility settings', () => {
	it('places kv-mapped hash keys exactly where the hash puts them', async () => {
		const world = memoryWorld();
		boot(world);
		await createSchemas(world);
		await insertAll(KEYS);

		for (const key of KEYS) {
			expect(await holders(world, key)).toEqual([hrwShard(key, NAMES)]);
		}
	});

	it('cycles round-robin in binding order', async () => {
		const world = memoryWorld();
		boot(world, { strategy: 'round-robin' });
		await createSchemas(world);
		await insertAll(KEYS.slice(0, 9));

		const order = await Promise.all(KEYS.slice(0, 9).map(async (key) => (await holders(world, key))[0]));
		expect(order).toEqual(['db-a', 'db-b', 'db-c', 'db-a', 'db-b', 'db-c', 'db-a', 'db-b', 'db-c']);
	});

	it('places computed keys exactly where the hash puts them', async () => {
		const world = memoryWorld();
		boot(world, { placement: 'computed' });
		await createSchemas(world);
		await insertAll(KEYS);

		for (const key of KEYS) {
			expect(await holders(world, key)).toEqual([hrwShard(key, NAMES)]);
		}
		expect((await kvManifest(world))!.epochs).toEqual([{ n: 1, algorithm: 'hrw', shards: NAMES }]);
	});

	it('sends location keys to the closest shard', async () => {
		const world = memoryWorld();
		boot(world, { strategy: 'location', targetRegion: 'weur', shardLocations: { 'db-a': 'enam', 'db-b': 'weur', 'db-c': 'apac' } });
		await createSchemas(world);
		await insertAll(KEYS.slice(0, 40));

		expect(await tally(world, KEYS.slice(0, 40))).toEqual({ 'db-a': 0, 'db-b': 40, 'db-c': 0 });
	});
});

// #endregion

// #region ineligible shards never receive a new key

type Ineligible = {
	name: string;
	config: (bad: string) => Partial<CollegeDBConfig>;
	prepare?: (world: World, bad: string) => Promise<void>;
};

const INELIGIBLE: Ineligible[] = [
	{ name: 'writable: false', config: (bad) => ({ shardState: { [bad]: { writable: false } } }) },
	{ name: "drain: 'draining'", config: (bad) => ({ shardState: { [bad]: { drain: 'draining' } } }) },
	{ name: "drain: 'retired'", config: (bad) => ({ shardState: { [bad]: { drain: 'retired' } } }) },
	{ name: 'a predicate that says no', config: (bad) => ({ eligible: (shard: string) => shard !== bad }) },
	{
		name: 'a size over maxDatabaseSize',
		config: () => ({ maxDatabaseSize: 1_000_000, sizeCacheTtlMs: 0 }),
		prepare: async (_world, bad) => {
			// the in-memory provider grows about 4 KB per row, so 300 rows clear the limit and 100 rows each do not
			for (let i = 0; i < 300; i++) {
				await runShard(bad, 'INSERT INTO users (id, name) VALUES (?, ?)', [`seed-${i}`, 'seed']);
			}
		}
	}
];

const STRATEGIES: Array<{ name: string; config: Partial<CollegeDBConfig>; bad: string; spread: boolean }> = [
	{ name: 'hash', config: { strategy: 'hash' }, bad: 'db-b', spread: true },
	{ name: 'round-robin', config: { strategy: 'round-robin' }, bad: 'db-b', spread: true },
	{ name: 'random', config: { strategy: 'random' }, bad: 'db-b', spread: true },
	{
		name: 'location (the ineligible shard is the closest)',
		config: { strategy: 'location', targetRegion: 'weur', shardLocations: { 'db-a': 'enam', 'db-b': 'weur', 'db-c': 'apac' } },
		bad: 'db-b',
		spread: false
	},
	{ name: 'computed placement', config: { strategy: 'hash', placement: 'computed' }, bad: 'db-b', spread: true }
];

describe('An ineligible shard never receives a new key', () => {
	for (const strategy of STRATEGIES) {
		for (const reason of INELIGIBLE) {
			it(`under ${strategy.name} with ${reason.name}`, async () => {
				const world = memoryWorld();
				boot(world, { ...strategy.config, ...reason.config(strategy.bad) });
				await createSchemas(world);
				await reason.prepare?.(world, strategy.bad);
				await insertAll(KEYS);

				const counts = await tally(world, KEYS);
				expect(counts[strategy.bad]).toBe(0);
				expect(Object.values(counts).reduce((a, b) => a + b, 0)).toBe(KEYS.length);

				if (strategy.spread) {
					expect(counts['db-a']).toBeGreaterThan(0);
					expect(counts['db-c']).toBeGreaterThan(0);
				}
			});
		}
	}

	it('handles generated-key inserts the same way', async () => {
		const world = memoryWorld();
		// one open shard keeps the generated ids unique, which the insert path requires across shards
		boot(world, { strategy: 'round-robin', shardState: { 'db-b': { drain: 'draining' }, 'db-c': { writable: false } } });
		for (const name of NAMES) {
			await runShard(name, 'CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT)');
		}

		const { insert } = await import('../src/router');
		for (let i = 0; i < 12; i++) {
			const result = await insert('INSERT INTO posts (title) VALUES (?)', [`post ${i}`], { idColumn: 'id' });
			expect(result.generatedId).toBeDefined();
		}

		for (const name of ['db-b', 'db-c']) {
			const rows = await world.shards[name]!.prepare('SELECT COUNT(*) AS c FROM posts').all<{ c: number }>();
			expect(Number(rows.results[0]!.c)).toBe(0);
		}
		const open = await world.shards['db-a']!.prepare('SELECT COUNT(*) AS c FROM posts').all<{ c: number }>();
		expect(Number(open.results[0]!.c)).toBe(12);
	});

	it('gives the coordinator only the eligible shards', async () => {
		const world = memoryWorld();
		const { namespace, requests } = recordingCoordinator((shards) => shards[0]!);
		boot(world, { strategy: 'round-robin', coordinator: namespace, shardState: { 'db-b': { writable: false } } });
		await createSchemas(world);
		await insertAll(KEYS.slice(0, 6));

		expect(requests.length).toBe(6);
		for (const request of requests) {
			expect(request.availableShards).toEqual(['db-a', 'db-c']);
		}
		expect((await tally(world, KEYS.slice(0, 6)))['db-b']).toBe(0);
	});

	it('picks the closest eligible shard for the location strategy', async () => {
		const world = memoryWorld();
		boot(world, {
			strategy: 'location',
			targetRegion: 'enam',
			shardLocations: { 'db-a': 'enam', 'db-b': 'weur', 'db-c': 'apac' },
			shardState: { 'db-a': { drain: 'draining' } }
		});
		await createSchemas(world);
		await insertAll(KEYS.slice(0, 30));

		// enam is closest to weur among what is left
		expect(await tally(world, KEYS.slice(0, 30))).toEqual({ 'db-a': 0, 'db-b': 30, 'db-c': 0 });
	});

	it('composes the size limit with the other rules and keeps its soft fallback', async () => {
		const world = memoryWorld();
		boot(world, { strategy: 'hash', maxDatabaseSize: 1, sizeCacheTtlMs: 0, shardState: { 'db-b': { drain: 'draining' } } });
		await createSchemas(world);
		await insertAll(KEYS.slice(0, 50));

		// every shard is over the limit, so the limit steps aside, but the drained shard stays out
		const counts = await tally(world, KEYS.slice(0, 50));
		expect(counts['db-b']).toBe(0);
		expect(counts['db-a']! + counts['db-c']!).toBe(50);
	});
});

// #endregion

// #region existing keys and the epoch walk

describe('Existing keys on a draining shard', () => {
	it('keep resolving, reading and writing under kv placement', async () => {
		const world = memoryWorld();
		boot(world);
		await createSchemas(world);
		await insertAll(KEYS.slice(0, 60));
		const before = Object.fromEntries(await Promise.all(KEYS.slice(0, 60).map(async (key) => [key, (await holders(world, key))[0]!])));
		const onC = KEYS.slice(0, 60).filter((key) => before[key] === 'db-c');
		expect(onC.length).toBeGreaterThan(0);

		boot(world, { shardState: { 'db-c': { drain: 'draining' } } });

		for (const key of onC) {
			expect(await first<{ name: string }>(key, 'SELECT * FROM users WHERE id = ?', [key])).toMatchObject({ name: `name of ${key}` });
			await run(key, 'UPDATE users SET name = ? WHERE id = ?', ['changed', key]);
			expect(await first<{ name: string }>(key, 'SELECT * FROM users WHERE id = ?', [key])).toMatchObject({ name: 'changed' });
			expect(await holders(world, key)).toEqual(['db-c']);
		}

		await insertAll(KEYS.slice(60, 120));
		expect((await tally(world, KEYS.slice(60, 120)))['db-c']).toBe(0);
	});

	it('keep resolving, reading and writing under computed placement, and the drain publishes an epoch', async () => {
		const world = journalWorld();
		boot(world, { placement: 'computed', journalShard: 'db-a' });
		await insertAll(KEYS.slice(0, 60));
		expect(storedManifest(world).epochs.map((epoch) => epoch.n)).toEqual([1]);

		const onC = (await Promise.all(KEYS.slice(0, 60).map(async (key) => ((await holders(world, key))[0] === 'db-c' ? key : null)))).filter(
			(key): key is string => key !== null
		);
		expect(onC.length).toBeGreaterThan(0);

		boot(world, { placement: 'computed', journalShard: 'db-a', shardState: { 'db-c': { drain: 'draining' } } });
		await first('warm', 'SELECT * FROM users WHERE id = ?', ['warm']);

		const manifest = storedManifest(world);
		expect(manifest.epochs.map((epoch) => epoch.n)).toEqual([2, 1]);
		expect([...manifest.epochs[0]!.shards].sort()).toEqual(['db-a', 'db-b']);
		expect([...manifest.epochs[1]!.shards].sort()).toEqual(NAMES);

		for (const key of onC) {
			expect(await first<{ name: string }>(key, 'SELECT * FROM users WHERE id = ?', [key])).toMatchObject({ name: `name of ${key}` });
			await run(key, 'UPDATE users SET name = ? WHERE id = ?', ['changed', key]);
			expect(await first<{ name: string }>(key, 'SELECT * FROM users WHERE id = ?', [key])).toMatchObject({ name: 'changed' });
		}

		await insertAll(KEYS.slice(60, 160));
		expect((await tally(world, KEYS.slice(60, 160)))['db-c']).toBe(0);
	});

	it('hold their epoch in place: pruneEpochs keeps it while a key depends on it', async () => {
		const world = journalWorld();
		boot(world, { placement: 'computed', journalShard: 'db-a' });
		await insertAll(KEYS.slice(0, 60));
		boot(world, { placement: 'computed', journalShard: 'db-a', shardState: { 'db-c': { drain: 'draining' } } });
		await first('warm', 'SELECT * FROM users WHERE id = ?', ['warm']);

		const report = await pruneEpochs({ tables: ['users'] });

		expect(report.removed).toEqual([]);
		expect(report.retained).toEqual([2, 1]);
		expect(report.dependents[1]).toBeGreaterThan(0);
		expect(storedManifest(world).epochs.map((epoch) => epoch.n)).toEqual([2, 1]);
	});

	it('let an epoch go once every key under it was moved or pinned', async () => {
		const world = journalWorld();
		boot(world, { placement: 'computed', journalShard: 'db-a' });
		await insertAll(KEYS.slice(0, 40));
		boot(world, { placement: 'computed', journalShard: 'db-a', shardState: { 'db-c': { drain: 'draining' } } });
		for (const key of KEYS.slice(0, 40)) {
			await first(key, 'SELECT * FROM users WHERE id = ?', [key]);
		}

		const report = await pruneEpochs({ tables: ['users'] });

		expect(report.removed).toEqual([1]);
		expect(storedManifest(world).epochs.map((epoch) => epoch.n)).toEqual([2]);
		for (const key of KEYS.slice(0, 40)) {
			expect(await first<{ name: string }>(key, 'SELECT * FROM users WHERE id = ?', [key])).toMatchObject({ name: `name of ${key}` });
		}
	});

	it('are moved off a draining shard by rebalance, which targets eligible shards only', async () => {
		const world = journalWorld();
		boot(world, { journalShard: 'db-a' });
		await insertAll(KEYS.slice(0, 60));
		const onC = (await tally(world, KEYS.slice(0, 60)))['db-c']!;
		expect(onC).toBeGreaterThan(0);

		boot(world, { journalShard: 'db-a', shardState: { 'db-c': { drain: 'draining' } } });
		const result = await rebalance('users');

		expect(result.moved).toBe(onC);
		expect(result.failed).toEqual([]);
		expect(await tally(world, KEYS.slice(0, 60))).toMatchObject({ 'db-c': 0 });
		for (const key of KEYS.slice(0, 60)) {
			expect(await first<{ name: string }>(key, 'SELECT * FROM users WHERE id = ?', [key])).toMatchObject({ name: `name of ${key}` });
		}
	});
});

describe('Reconfiguring a running process', () => {
	it('publishes the drain on the next operation without resetConfig', async () => {
		const world = journalWorld();
		boot(world, { placement: 'computed', journalShard: 'db-a' });
		await first('warm', 'SELECT * FROM users WHERE id = ?', ['warm']);
		expect(storedManifest(world).epochs.map((epoch) => epoch.n)).toEqual([1]);

		initialize({
			kv: world.kv,
			shards: world.shards,
			strategy: 'hash',
			placement: 'computed',
			journalShard: 'db-a',
			disableAutoMigration: true,
			hashShardMappings: false,
			shardState: { 'db-c': { drain: 'draining' } }
		});
		await first('warm', 'SELECT * FROM users WHERE id = ?', ['warm']);

		expect(storedManifest(world).epochs.map((epoch) => epoch.n)).toEqual([2, 1]);
	});
});

// #endregion

// #region the predicate

describe('The eligibility predicate', () => {
	it('receives the key, the tags and the state on allocation', async () => {
		const world = memoryWorld();
		const state: Record<string, ShardState> = { 'db-a': { tags: { zone: 'east' } }, 'db-b': { tags: { zone: 'west' }, id: 'uuid-b' } };
		const calls: Array<{ shard: string; context: EligibilityContext }> = [];
		boot(world, {
			shardState: state,
			eligible: (shard, context) => {
				calls.push({ shard, context });
				return true;
			}
		});
		await createSchemas(world);
		await run('only-key', 'INSERT INTO users (id, name) VALUES (?, ?)', ['only-key', 'x']);

		expect(calls.map((call) => call.shard).sort()).toEqual(NAMES);
		for (const { shard, context } of calls) {
			expect(context.operation).toBe('allocate');
			expect(context.key).toBe('only-key');
			expect(context.tags).toEqual(state[shard]?.tags ?? {});
			expect(context.state).toEqual(state[shard] ?? {});
		}
	});

	it('is not asked again for a key that already has a mapping', async () => {
		const world = memoryWorld();
		let asked = 0;
		boot(world, { eligible: () => (asked++, true) });
		await createSchemas(world);
		await run('k', 'INSERT INTO users (id, name) VALUES (?, ?)', ['k', 'x']);
		const afterFirst = asked;

		await first('k', 'SELECT * FROM users WHERE id = ?', ['k']);
		await run('k', 'UPDATE users SET name = ? WHERE id = ?', ['y', 'k']);

		expect(asked).toBe(afterFirst);
	});

	it('receives operation publish and no key when a snapshot is published', async () => {
		const world = memoryWorld();
		const calls: Array<{ shard: string; context: EligibilityContext }> = [];
		boot(world, {
			placement: 'computed',
			shardState: { 'db-c': { tags: { jurisdiction: 'eu' } } },
			eligible: (shard, context) => {
				calls.push({ shard, context });
				return true;
			}
		});
		await createSchemas(world);
		await first('warm', 'SELECT * FROM users WHERE id = ?', ['warm']);

		expect(calls.map((call) => call.shard).sort()).toEqual(NAMES);
		for (const { shard, context } of calls) {
			expect(context.operation).toBe('publish');
			expect(context.key).toBeUndefined();
			expect(context.tags).toEqual(shard === 'db-c' ? { jurisdiction: 'eu' } : {});
		}
	});

	it('may be async', async () => {
		const world = memoryWorld();
		boot(world, {
			eligible: async (shard) => {
				await Promise.resolve();
				return shard === 'db-c';
			}
		});
		await createSchemas(world);
		await insertAll(KEYS.slice(0, 20));

		expect(await tally(world, KEYS.slice(0, 20))).toEqual({ 'db-a': 0, 'db-b': 0, 'db-c': 20 });
	});

	it('fails an allocation closed when it throws, and writes nothing', async () => {
		const world = memoryWorld();
		boot(world, {
			eligible: (shard) => {
				if (shard === 'db-b') throw new Error('health probe down');
				return true;
			}
		});
		await createSchemas(world);

		const error = await run('k', 'INSERT INTO users (id, name) VALUES (?, ?)', ['k', 'x']).catch(
			(e: unknown) => e as Error & { code?: string }
		);

		expect(error).toMatchObject({ code: 'ELIGIBILITY_FAILED' });
		expect((error as Error).message).toContain('db-b');
		expect((error as Error).message).toContain('health probe down');
		expect(await holders(world, 'k')).toEqual([]);
	});

	it('fails an async rejection closed too', async () => {
		const world = memoryWorld();
		boot(world, { eligible: () => Promise.reject(new Error('nope')) });
		await createSchemas(world);

		expect(await codeOf(run('k', 'INSERT INTO users (id, name) VALUES (?, ?)', ['k', 'x']))).toBe('ELIGIBILITY_FAILED');
	});

	it('fails a publication closed when it throws, and publishes nothing', async () => {
		const world = memoryWorld();
		boot(world, {
			placement: 'computed',
			eligible: () => {
				throw new Error('health probe down');
			}
		});
		await createSchemas(world);

		expect(await codeOf(first('k', 'SELECT * FROM users WHERE id = ?', ['k']))).toBe('ELIGIBILITY_FAILED');
		expect(await kvManifest(world)).toBeNull();
	});

	it('never lets a read fail on the predicate when the key has no mapping', async () => {
		const world = memoryWorld();
		boot(world, {
			eligible: () => {
				throw new Error('health probe down');
			}
		});
		await createSchemas(world);

		expect(await first('unknown', 'SELECT * FROM users WHERE id = ?', ['unknown'])).toBeNull();
	});
});

// #endregion

// #region nothing eligible

describe('When no shard is eligible', () => {
	const ALL_DRAINING = { 'db-a': { drain: 'draining' }, 'db-b': { drain: 'retired' }, 'db-c': { writable: false } } as const;

	it('refuses a new key with NO_ELIGIBLE_SHARD and writes nothing', async () => {
		const world = memoryWorld();
		boot(world, { shardState: { ...ALL_DRAINING } });
		await createSchemas(world);

		expect(await codeOf(run('k', 'INSERT INTO users (id, name) VALUES (?, ?)', ['k', 'x']))).toBe('NO_ELIGIBLE_SHARD');
		expect(await holders(world, 'k')).toEqual([]);
	});

	it('refuses a new key when the predicate rejects everything', async () => {
		const world = memoryWorld();
		boot(world, { eligible: () => false });
		await createSchemas(world);

		expect(await codeOf(run('k', 'INSERT INTO users (id, name) VALUES (?, ?)', ['k', 'x']))).toBe('NO_ELIGIBLE_SHARD');
	});

	it('refuses an insert without a key', async () => {
		const world = memoryWorld();
		boot(world, { shardState: { ...ALL_DRAINING } });
		await createSchemas(world);
		const { insert } = await import('../src/router');

		expect(await codeOf(insert('INSERT INTO users (id, name) VALUES (?, ?)', ['k', 'x'], { idColumn: 'id' }))).toBe('NO_ELIGIBLE_SHARD');
	});

	it('refuses to publish a computed snapshot', async () => {
		const world = memoryWorld();
		boot(world, { placement: 'computed', shardState: { ...ALL_DRAINING } });
		await createSchemas(world);

		expect(await codeOf(first('k', 'SELECT * FROM users WHERE id = ?', ['k']))).toBe('NO_ELIGIBLE_SHARD');
		expect(await kvManifest(world)).toBeNull();
	});

	it('still serves an existing key and a read of a missing one', async () => {
		const world = memoryWorld();
		boot(world);
		await createSchemas(world);
		await run('kept', 'INSERT INTO users (id, name) VALUES (?, ?)', ['kept', 'x']);

		boot(world, { shardState: { ...ALL_DRAINING } });

		expect(await first<{ name: string }>('kept', 'SELECT * FROM users WHERE id = ?', ['kept'])).toMatchObject({ name: 'x' });
		await run('kept', 'UPDATE users SET name = ? WHERE id = ?', ['y', 'kept']);
		expect(await first('missing', 'SELECT * FROM users WHERE id = ?', ['missing'])).toBeNull();
	});
});

// #endregion

// #region publishEligibility

describe('publishEligibility', () => {
	function togglable() {
		const blocked = new Set<string>();
		return { blocked, eligible: (shard: string) => !blocked.has(shard) };
	}

	it('publishes nothing while the eligible set matches the newest epoch, then publishes once and is idempotent', async () => {
		const world = journalWorld();
		const { blocked, eligible } = togglable();
		boot(world, { placement: 'computed', journalShard: 'db-a', eligible });
		await first('warm', 'SELECT * FROM users WHERE id = ?', ['warm']);

		expect(await publishEligibility()).toEqual({ eligible: NAMES, published: false, epoch: 1 });

		blocked.add('db-c');
		expect(await publishEligibility()).toEqual({ eligible: ['db-a', 'db-b'], published: true, epoch: 2 });
		expect(await publishEligibility()).toEqual({ eligible: ['db-a', 'db-b'], published: false, epoch: 2 });
		expect(storedManifest(world).epochs.map((epoch) => epoch.n)).toEqual([2, 1]);
	});

	it('runs the predicate with operation publish', async () => {
		const world = journalWorld();
		const operations = new Set<string>();
		boot(world, {
			placement: 'computed',
			journalShard: 'db-a',
			eligible: (_shard, context) => {
				operations.add(context.operation);
				return true;
			}
		});
		await first('warm', 'SELECT * FROM users WHERE id = ?', ['warm']);
		operations.clear();

		await publishEligibility();

		expect([...operations]).toEqual(['publish']);
	});

	it('routes new keys away from a shard the predicate dropped at runtime, and keeps old keys readable', async () => {
		const world = journalWorld();
		const { blocked, eligible } = togglable();
		boot(world, { placement: 'computed', journalShard: 'db-a', eligible });
		await insertAll(KEYS.slice(0, 60));
		const onC = KEYS.slice(0, 60).filter((key) => hrwShard(key, NAMES) === 'db-c');
		expect(onC.length).toBeGreaterThan(0);

		blocked.add('db-c');
		await publishEligibility();
		await insertAll(KEYS.slice(60, 160));

		expect((await tally(world, KEYS.slice(60, 160)))['db-c']).toBe(0);
		for (const key of onC) {
			expect(await first<{ name: string }>(key, 'SELECT * FROM users WHERE id = ?', [key])).toMatchObject({ name: `name of ${key}` });
		}
	});

	it('works without a journal, through the KV manifest', async () => {
		const world = memoryWorld();
		const { blocked, eligible } = togglable();
		boot(world, { placement: 'computed', eligible });
		await createSchemas(world);
		await first('warm', 'SELECT * FROM users WHERE id = ?', ['warm']);

		blocked.add('db-b');
		expect(await publishEligibility()).toEqual({ eligible: ['db-a', 'db-c'], published: true, epoch: 2 });
		expect((await kvManifest(world))!.epochs.map((epoch) => epoch.n)).toEqual([2, 1]);
		expect(await publishEligibility()).toMatchObject({ published: false, epoch: 2 });
	});

	it('publishes on a cold process that has not touched placement yet', async () => {
		const world = journalWorld();
		boot(world, { placement: 'computed', journalShard: 'db-a', shardState: { 'db-b': { drain: 'draining' } } });

		expect(await publishEligibility()).toEqual({ eligible: ['db-a', 'db-c'], published: true, epoch: 1 });
		expect([...storedManifest(world).epochs[0]!.shards].sort()).toEqual(['db-a', 'db-c']);
	});

	it('does not call seeding a journal from the KV manifest a publication', async () => {
		const world = journalWorld();
		boot(world, { placement: 'computed' });
		await first('warm', 'SELECT * FROM users WHERE id = ?', ['warm']);
		expect((await kvManifest(world))!.epochs.map((epoch) => epoch.n)).toEqual([1]);

		boot(world, { placement: 'computed', journalShard: 'db-a' });

		expect(await publishEligibility()).toEqual({ eligible: NAMES, published: false, epoch: 1 });
		expect(storedManifest(world).epochs.map((epoch) => epoch.n)).toEqual([1]);
	});

	it('lets one of two concurrent callers publish and tells the other it published nothing', async () => {
		const world = journalWorld();
		const { blocked, eligible } = togglable();
		boot(world, { placement: 'computed', journalShard: 'db-a', eligible });
		await first('warm', 'SELECT * FROM users WHERE id = ?', ['warm']);

		blocked.add('db-c');
		const results = await Promise.all([publishEligibility(), publishEligibility()]);

		expect(results.map((result) => result.published).sort()).toEqual([false, true]);
		expect(results.map((result) => result.epoch)).toEqual([2, 2]);
		expect(storedManifest(world).epochs.map((epoch) => epoch.n)).toEqual([2, 1]);
	});

	it('surfaces TOO_MANY_EPOCHS at the ceiling and leaves the manifest alone', async () => {
		const world = journalWorld();
		const { blocked, eligible } = togglable();
		boot(world, { placement: 'computed', journalShard: 'db-a', eligible });
		await first('warm', 'SELECT * FROM users WHERE id = ?', ['warm']);

		for (let epoch = 2; epoch <= 8; epoch++) {
			if (blocked.has('db-c')) blocked.delete('db-c');
			else blocked.add('db-c');
			expect(await publishEligibility()).toMatchObject({ published: true, epoch });
		}

		if (blocked.has('db-c')) blocked.delete('db-c');
		else blocked.add('db-c');
		const before = storedManifest(world);

		await expect(publishEligibility()).rejects.toMatchObject({ code: 'TOO_MANY_EPOCHS' });
		expect(storedManifest(world)).toEqual(before);
		expect(before.epochs).toHaveLength(8);
	});

	it('surfaces PLACEMENT_UNVERIFIED when KV cannot be read', async () => {
		const world = memoryWorld();
		const inner = world.kv;
		let down = false;
		world.kv = {
			put: (key, value) => inner.put(key, value),
			delete: (key) => inner.delete(key),
			list: (options) => inner.list(options),
			get: (async (key: string, type?: 'text' | 'json') => {
				if (down) throw new Error('kv down');
				return await inner.get(key, type as 'json');
			}) as KVStorage['get']
		};
		boot(world, { placement: 'computed' });
		await createSchemas(world);
		await first('warm', 'SELECT * FROM users WHERE id = ?', ['warm']);
		const before = await kvManifest({ ...world, kv: inner });

		down = true;

		expect(await codeOf(publishEligibility())).toBe('PLACEMENT_UNVERIFIED');
		expect(await kvManifest({ ...world, kv: inner })).toEqual(before);
	});

	it('reports the eligible list and no epoch under kv placement', async () => {
		const world = memoryWorld();
		boot(world, { shardState: { 'db-a': { writable: false } } });
		await createSchemas(world);

		expect(await publishEligibility()).toEqual({ eligible: ['db-b', 'db-c'], published: false, epoch: 0 });
		expect(await kvManifest(world)).toBeNull();
	});

	it('refuses when nothing is eligible', async () => {
		const world = memoryWorld();
		boot(world, { placement: 'computed', eligible: () => false });

		expect(await codeOf(publishEligibility())).toBe('NO_ELIGIBLE_SHARD');
	});
});

// #endregion

// #region read-only maintenance mode

describe('Every shard draining under computed placement', () => {
	const ALL_DRAINING = { 'db-a': { drain: 'draining' }, 'db-b': { drain: 'draining' }, 'db-c': { drain: 'draining' } } as const;

	async function seeded() {
		const world = journalWorld();
		boot(world, { placement: 'computed', journalShard: 'db-a' });
		const keys = KEYS.slice(0, 30);
		await insertAll(keys);
		return { world, keys };
	}

	it('keeps serving reads on a cold start and publishes no new epoch', async () => {
		const { world, keys } = await seeded();

		boot(world, { placement: 'computed', journalShard: 'db-a', shardState: { ...ALL_DRAINING } });

		for (const key of keys) {
			expect(await first<{ name: string }>(key, 'SELECT * FROM users WHERE id = ?', [key])).toMatchObject({ name: `name of ${key}` });
		}
		expect(await first('missing', 'SELECT * FROM users WHERE id = ?', ['missing'])).toBeNull();
		expect(storedManifest(world).epochs.map((epoch) => epoch.n)).toEqual([1]);
	});

	it('refuses every write with NO_ELIGIBLE_SHARD and changes nothing', async () => {
		const { world, keys } = await seeded();
		boot(world, { placement: 'computed', journalShard: 'db-a', shardState: { ...ALL_DRAINING } });

		expect(await codeOf(run('fresh', 'INSERT INTO users (id, name) VALUES (?, ?)', ['fresh', 'x']))).toBe('NO_ELIGIBLE_SHARD');
		expect(await codeOf(run(keys[0]!, 'UPDATE users SET name = ? WHERE id = ?', ['changed', keys[0]!]))).toBe('NO_ELIGIBLE_SHARD');
		expect(await codeOf(run(keys[0]!, 'DELETE FROM users WHERE id = ?', [keys[0]!]))).toBe('NO_ELIGIBLE_SHARD');
		expect(await codeOf(upsert(keys[0]!, 'users', { id: keys[0]!, name: 'u' }, 'id'))).toBe('NO_ELIGIBLE_SHARD');
		expect(await codeOf(batch([{ key: 'fresh', sql: 'INSERT INTO users (id, name) VALUES (?, ?)', bindings: ['fresh', 'x'] }]))).toBe(
			'NO_ELIGIBLE_SHARD'
		);

		expect(await holders(world, 'fresh')).toEqual([]);
		expect(await first<{ name: string }>(keys[0]!, 'SELECT * FROM users WHERE id = ?', [keys[0]!])).toMatchObject({
			name: `name of ${keys[0]}`
		});
	});

	it('keeps refusing writes after a read finds a key on an older epoch', async () => {
		const world = journalWorld();
		boot(world, { placement: 'computed', journalShard: 'db-a', shardState: { 'db-c': { drain: 'draining' } } });
		await insertAll(KEYS.slice(0, 40));
		boot(world, { placement: 'computed', journalShard: 'db-a', shardState: { ...ALL_DRAINING } });

		for (const key of KEYS.slice(0, 40)) {
			await first(key, 'SELECT * FROM users WHERE id = ?', [key]);
		}

		expect(await codeOf(run(KEYS[0]!, 'UPDATE users SET name = ? WHERE id = ?', ['x', KEYS[0]!]))).toBe('NO_ELIGIBLE_SHARD');
	});

	it('accepts writes again once a shard is open and publishes the epoch then', async () => {
		const { world, keys } = await seeded();
		boot(world, { placement: 'computed', journalShard: 'db-a', shardState: { ...ALL_DRAINING } });
		await first(keys[0]!, 'SELECT * FROM users WHERE id = ?', [keys[0]!]);

		boot(world, {
			placement: 'computed',
			journalShard: 'db-a',
			shardState: { 'db-a': { drain: 'draining' }, 'db-b': { drain: 'draining' } }
		});
		await run('after', 'INSERT INTO users (id, name) VALUES (?, ?)', ['after', 'x']);

		expect(await holders(world, 'after')).toEqual(['db-c']);
		expect(storedManifest(world).epochs[0]!.shards).toEqual(['db-c']);
	});

	it('still has nothing to read from on a deployment with no manifest', async () => {
		const world = journalWorld();
		boot(world, { placement: 'computed', journalShard: 'db-a', shardState: { ...ALL_DRAINING } });

		expect(await codeOf(first('k', 'SELECT * FROM users WHERE id = ?', ['k']))).toBe('NO_ELIGIBLE_SHARD');
	});
});

// #endregion

// #region unreadable sizes

describe('maxDatabaseSize when a shard size cannot be read', () => {
	function blindSize(world: World, name: string): void {
		const healthy = world.shards[name]!;
		world.shards[name] = {
			...healthy,
			prepare: (sql: string) => {
				if (/page_count|page_size|pg_database_size|information_schema/i.test(sql)) throw new Error('size probe down');
				return healthy.prepare(sql);
			}
		} as SQLDatabase;
	}

	it('keeps the shard in a computed snapshot and does not throw', async () => {
		const world = memoryWorld();
		blindSize(world, 'db-b');
		boot(world, { placement: 'computed', maxDatabaseSize: 1_000_000, sizeCacheTtlMs: 0 });

		expect(await first('k', 'SELECT * FROM users WHERE id = ?', ['k'])).toBeNull();
		expect((await kvManifest(world))!.epochs[0]!.shards).toEqual(NAMES);
	});

	it('lets a kv-mapped key land on the shard', async () => {
		const world = memoryWorld();
		blindSize(world, 'db-b');
		boot(world, { maxDatabaseSize: 1_000_000, sizeCacheTtlMs: 0 });
		await createSchemas(world);
		await insertAll(KEYS.slice(0, 60));

		expect((await tally(world, KEYS.slice(0, 60)))['db-b']).toBeGreaterThan(0);
	});

	it('applies the limit only to the shards it can measure', async () => {
		const world = memoryWorld();
		blindSize(world, 'db-b');
		boot(world, { maxDatabaseSize: 1, sizeCacheTtlMs: 0 });
		await createSchemas(world);
		await insertAll(KEYS.slice(0, 40));

		expect(await tally(world, KEYS.slice(0, 40))).toEqual({ 'db-a': 0, 'db-b': 40, 'db-c': 0 });
	});

	it('reports the unreadable size once in the debug log', async () => {
		const world = memoryWorld();
		blindSize(world, 'db-b');
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
		try {
			boot(world, { maxDatabaseSize: 1_000_000, sizeCacheTtlMs: 0, debug: true });
			await createSchemas(world);
			await insertAll(KEYS.slice(0, 20));

			const reports = warn.mock.calls.filter((call) => String(call[0]).includes('db-b'));
			expect(reports).toHaveLength(1);
		} finally {
			warn.mockRestore();
		}
	});

	it('stays quiet without debug', async () => {
		const world = memoryWorld();
		blindSize(world, 'db-b');
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
		try {
			boot(world, { maxDatabaseSize: 1_000_000, sizeCacheTtlMs: 0 });
			await createSchemas(world);
			await insertAll(KEYS.slice(0, 20));

			expect(warn).not.toHaveBeenCalled();
		} finally {
			warn.mockRestore();
		}
	});
});

// #endregion
