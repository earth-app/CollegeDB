import type { DatabaseSync } from 'node:sqlite';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { first, initializeAsync, insertInto, KVShardMapper, pruneEpochs, reassignShard, resetConfig } from '../src/index';
import { withKVTimeout } from '../src/kv-timeout';
import {
	addPlacementException,
	candidateShards,
	createManifest,
	hrwShard,
	legacyModuloShard,
	loadPlacementExceptions,
	loadPlacementManifest,
	MAX_RETAINED_EPOCHS,
	publishPlacementTopology,
	resetPlacementState,
	savePlacementManifest,
	shardForEpoch,
	swapPlacementCache,
	withCurrentTopology,
	type PlacementManifest
} from '../src/placement';
import { createInMemoryKVProvider } from '../src/providers-memory';
import { buildFenceRaise, commitJournalManifest, loadJournalManifest, raiseFence, readFence, setPruneStepHook } from '../src/topology';
import type { KVStorage, PreparedStatement, SQLDatabase } from '../src/types';
import { restoreClock, setClock } from './helpers/clock';
import { createNodeSqliteProvider, openNodeSqlite } from './helpers/concurrent-sqlite';
import { runFenceRace } from './helpers/fence-race';

const SHARDS = ['db-east', 'db-west', 'db-central'];

function topology(count: number): string[] {
	return new Array(count).fill(null).map((_, index) => `shard-${index}`);
}

function thrownCode(fn: () => unknown): string | undefined {
	try {
		fn();
	} catch (error) {
		return (error as { code?: string }).code;
	}
}

function keys(count: number, prefix = 'user'): string[] {
	return new Array(count).fill(null).map((_, index) => `${prefix}-${index}`);
}

function distribution(sample: string[], shards: string[]): Record<string, number> {
	const counts: Record<string, number> = Object.fromEntries(shards.map((shard) => [shard, 0]));
	for (const key of sample) {
		counts[hrwShard(key, shards)]!++;
	}
	return counts;
}

describe('Rendezvous placement', () => {
	beforeEach(() => {
		resetPlacementState();
	});

	it('is deterministic for the same key and shard set', () => {
		for (const key of keys(50)) {
			expect(hrwShard(key, SHARDS)).toBe(hrwShard(key, SHARDS));
		}
	});

	it('does not depend on the order shards are declared in', () => {
		const permuted = ['db-central', 'db-east', 'db-west'];
		const reversed = [...SHARDS].reverse();

		for (const key of keys(500)) {
			const expected = hrwShard(key, SHARDS);
			expect(hrwShard(key, permuted)).toBe(expected);
			expect(hrwShard(key, reversed)).toBe(expected);
		}
	});

	it('distributes keys close to evenly', () => {
		const sample = keys(30_000);
		const counts = distribution(sample, SHARDS);
		const ideal = sample.length / SHARDS.length;

		// Chi-square with 2 degrees of freedom; 13.8 is the 0.001 critical value,
		// so a well-distributed hash clears it with room to spare.
		const chiSquare = Object.values(counts).reduce((sum, observed) => sum + (observed - ideal) ** 2 / ideal, 0);
		expect(chiSquare).toBeLessThan(13.8);

		for (const count of Object.values(counts)) {
			expect(count).toBeGreaterThan(ideal * 0.9);
			expect(count).toBeLessThan(ideal * 1.1);
		}
	});

	it('moves only about 1/m of keys when a shard is added', () => {
		const sample = keys(20_000);
		const grown = [...SHARDS, 'db-apac'];

		let moved = 0;
		for (const key of sample) {
			if (hrwShard(key, SHARDS) !== hrwShard(key, grown)) {
				moved++;
			}
		}

		// The theoretical minimum for a fourth shard is 1/4 of the keyspace.
		const ratio = moved / sample.length;
		expect(ratio).toBeGreaterThan(0.2);
		expect(ratio).toBeLessThan(0.3);
	});

	it('relocates only the removed shard when one goes away', () => {
		const sample = keys(20_000);
		const shrunk = ['db-east', 'db-west'];

		for (const key of sample) {
			const before = hrwShard(key, SHARDS);
			const after = hrwShard(key, shrunk);

			if (before !== 'db-central') {
				expect(after).toBe(before);
			} else {
				expect(shrunk).toContain(after);
			}
		}
	});

	it('beats modulo placement on disruption, which is why it replaced it', () => {
		const sample = keys(20_000);
		const grown = [...SHARDS, 'db-apac'];

		let hrwMoved = 0;
		let moduloMoved = 0;
		for (const key of sample) {
			if (hrwShard(key, SHARDS) !== hrwShard(key, grown)) hrwMoved++;
			if (legacyModuloShard(key, SHARDS) !== legacyModuloShard(key, grown)) moduloMoved++;
		}

		expect(moduloMoved / sample.length).toBeGreaterThan(0.6);
		expect(hrwMoved).toBeLessThan(moduloMoved / 2);
	});

	it('rejects an empty shard set instead of returning undefined', () => {
		expect(() => hrwShard('user-1', [])).toThrow(/at least one shard/);
		expect(() => legacyModuloShard('user-1', [])).toThrow(/at least one shard/);
	});

	it('handles a single shard and non-ascii keys', () => {
		expect(hrwShard('user-1', ['only'])).toBe('only');
		expect(hrwShard('ユーザー-1', SHARDS)).toBe(hrwShard('ユーザー-1', SHARDS));
		expect(SHARDS).toContain(hrwShard('', SHARDS));
	});
});

describe('Placement manifest', () => {
	beforeEach(() => {
		resetPlacementState();
	});

	it('keeps the newest epoch and does not churn on an unchanged topology', () => {
		const manifest = createManifest(SHARDS, 'hrw');
		expect(withCurrentTopology(manifest, SHARDS, 'hrw')).toBe(manifest);
		expect(withCurrentTopology(manifest, [...SHARDS].reverse(), 'hrw')).toBe(manifest);
	});

	it('prepends an epoch when the shard set changes', () => {
		const manifest = createManifest(SHARDS, 'hrw');
		const grown = withCurrentTopology(manifest, [...SHARDS, 'db-apac'], 'hrw');

		expect(grown).not.toBe(manifest);
		expect(grown.epochs).toHaveLength(2);
		expect(grown.epochs[0]!.n).toBe(2);
		expect(grown.epochs[0]!.shards).toContain('db-apac');
		expect(grown.epochs[1]!.shards).not.toContain('db-apac');
	});

	it('prepends an epoch when the algorithm changes, which is the upgrade path', () => {
		const legacy = createManifest(SHARDS, 'modulo');
		const upgraded = withCurrentTopology(legacy, SHARDS, 'hrw');

		expect(upgraded.epochs[0]!.algorithm).toBe('hrw');
		expect(upgraded.epochs[1]!.algorithm).toBe('modulo');

		// A key placed by the old algorithm is still reachable through the walk.
		const key = 'user-42';
		const oldShard = legacyModuloShard(key, SHARDS);
		expect(candidateShards(key, upgraded, SHARDS)).toContain(oldShard);
	});

	it('holds epochs up to the ceiling and refuses to drop one to make room', () => {
		let manifest: PlacementManifest = createManifest(['a'], 'hrw');
		for (let i = 2; i <= MAX_RETAINED_EPOCHS; i++) {
			manifest = withCurrentTopology(manifest, topology(i), 'hrw');
		}

		expect(manifest.epochs).toHaveLength(MAX_RETAINED_EPOCHS);
		expect(manifest.epochs[0]!.n).toBe(MAX_RETAINED_EPOCHS);
		expect(manifest.epochs.at(-1)!.n).toBe(1);
		expect(thrownCode(() => withCurrentTopology(manifest, topology(MAX_RETAINED_EPOCHS + 1), 'hrw'))).toBe('TOO_MANY_EPOCHS');
	});

	it('orders candidates newest first and drops unconfigured shards', () => {
		const manifest = withCurrentTopology(createManifest(SHARDS, 'hrw'), [...SHARDS, 'db-apac'], 'hrw');
		const key = 'user-7';
		const candidates = candidateShards(key, manifest, [...SHARDS, 'db-apac']);

		expect(candidates[0]).toBe(hrwShard(key, [...SHARDS, 'db-apac']));
		expect(new Set(candidates).size).toBe(candidates.length);

		// A shard this process does not hold cannot be queried, so it is excluded.
		const limited = candidateShards(key, manifest, ['db-east']);
		expect(limited.every((shard) => shard === 'db-east')).toBe(true);
	});

	it('returns null for an epoch with no shards', () => {
		expect(shardForEpoch('user-1', { n: 1, algorithm: 'hrw', shards: [] })).toBeNull();
	});
});

describe('Manifest persistence', () => {
	beforeEach(() => {
		resetPlacementState();
	});

	it('returns null until a manifest is written, then reads it back', async () => {
		const kv = createInMemoryKVProvider();
		expect(await loadPlacementManifest(kv)).toBeNull();

		const manifest = createManifest(SHARDS, 'hrw');
		await savePlacementManifest(kv, manifest);

		resetPlacementState();
		expect(await loadPlacementManifest(kv)).toEqual(manifest);
	});

	it('reuses a loaded manifest for the cache window and re-reads after it', async () => {
		const kv = createInMemoryKVProvider();
		let reads = 0;

		// Spread would drop the prototype methods, since the in-memory provider is
		// a class instance.
		const counted: KVStorage = {
			get: (async (key: string, type?: 'text' | 'json') => {
				reads++;
				return type === 'json' ? await kv.get(key, 'json') : await kv.get(key, type);
			}) as KVStorage['get'],
			put: (key, value) => kv.put(key, value),
			delete: (key) => kv.delete(key),
			list: (options) => kv.list(options)
		};

		await savePlacementManifest(counted, createManifest(SHARDS, 'hrw'));
		resetPlacementState();

		await loadPlacementManifest(counted);
		await loadPlacementManifest(counted);
		expect(reads).toBe(1);

		// A zero TTL disables reuse, which is what the tests and the cold path need.
		resetPlacementState();
		await loadPlacementManifest(counted, 0);
		await loadPlacementManifest(counted, 0);
		expect(reads).toBe(3);
	});

	it('rejects a stored manifest that is not the expected shape', async () => {
		const kv = createInMemoryKVProvider();

		await kv.put('collegedb:placement:manifest', JSON.stringify({ version: 2, epochs: [] }));
		resetPlacementState();
		expect(await loadPlacementManifest(kv)).toBeNull();

		await kv.put('collegedb:placement:manifest', JSON.stringify({ version: 1, epochs: [] }));
		resetPlacementState();
		expect(await loadPlacementManifest(kv)).toBeNull();
	});

	it('starts with no exceptions and records them with a version bump', async () => {
		const kv = createInMemoryKVProvider();
		let manifest = createManifest(SHARDS, 'hrw');
		await savePlacementManifest(kv, manifest);

		expect((await loadPlacementExceptions(kv, manifest.exceptionsVersion)).size).toBe(0);

		manifest = await addPlacementException(kv, manifest, 'hashed-key-1');
		expect(manifest.exceptionsVersion).toBe(1);
		expect((await loadPlacementExceptions(kv, manifest.exceptionsVersion)).has('hashed-key-1')).toBe(true);

		// The bump is what lets another process notice; recording the same key
		// twice must not bump it again.
		const unchanged = await addPlacementException(kv, manifest, 'hashed-key-1');
		expect(unchanged.exceptionsVersion).toBe(1);

		const bumped = await addPlacementException(kv, manifest, 'hashed-key-2');
		expect(bumped.exceptionsVersion).toBe(2);
		expect((await loadPlacementExceptions(kv, bumped.exceptionsVersion)).size).toBe(2);
	});

	it('treats a corrupt exception payload as empty rather than throwing', async () => {
		const kv = createInMemoryKVProvider();
		await kv.put('collegedb:placement:exceptions', JSON.stringify({ not: 'an array' }));
		resetPlacementState();
		expect((await loadPlacementExceptions(kv, 0)).size).toBe(0);
	});
});

describe('Epoch retention', () => {
	beforeEach(() => {
		resetPlacementState();
	});

	function grow(manifest: PlacementManifest, changes: number): PlacementManifest {
		let next = manifest;
		const start = next.epochs[0]!.shards.length;
		for (let i = 1; i <= changes; i++) {
			next = withCurrentTopology(next, topology(start + i), 'hrw');
		}
		return next;
	}

	it('keeps all four epochs across three topology changes', () => {
		const manifest = grow(createManifest(topology(2), 'hrw'), 3);

		expect(manifest.epochs.map((epoch) => epoch.n)).toEqual([4, 3, 2, 1]);
	});

	it('still resolves a key placed under epoch 1 after four topology changes', () => {
		const first = createManifest(topology(2), 'hrw');
		const manifest = grow(first, 4);
		const available = topology(6);

		// a key whose epoch 1 owner no later epoch picks, so only epoch 1 can find it
		let key = '';
		let owner = '';
		for (let i = 0; i < 5000 && key === ''; i++) {
			const candidate = `user-${i}`;
			const placedAt = hrwShard(candidate, first.epochs[0]!.shards);
			const later = manifest.epochs.slice(0, -1).map((epoch) => hrwShard(candidate, epoch.shards));
			if (!later.includes(placedAt)) {
				key = candidate;
				owner = placedAt;
			}
		}
		expect(key).not.toBe('');

		expect(manifest.epochs).toHaveLength(5);
		expect(candidateShards(key, manifest, available)).toContain(owner);
	});

	it('holds every epoch up to the ceiling', () => {
		const manifest = grow(createManifest(topology(2), 'hrw'), MAX_RETAINED_EPOCHS - 1);

		expect(manifest.epochs).toHaveLength(MAX_RETAINED_EPOCHS);
		expect(manifest.epochs.map((epoch) => epoch.n)).toEqual([8, 7, 6, 5, 4, 3, 2, 1]);
	});

	it('throws TOO_MANY_EPOCHS for the ninth epoch and leaves the manifest unchanged', () => {
		const manifest = grow(createManifest(topology(2), 'hrw'), MAX_RETAINED_EPOCHS - 1);
		const before = JSON.stringify(manifest);

		let message = '';
		let code: string | undefined;
		try {
			withCurrentTopology(manifest, topology(20), 'hrw');
		} catch (error) {
			message = (error as Error).message;
			code = (error as { code?: string }).code;
		}

		expect(code).toBe('TOO_MANY_EPOCHS');
		expect(message).toMatch(/converge/i);
		expect(message).toMatch(/prune/i);
		expect(JSON.stringify(manifest)).toBe(before);
	});

	it('accepts an unchanged topology at the ceiling', () => {
		const manifest = grow(createManifest(topology(2), 'hrw'), MAX_RETAINED_EPOCHS - 1);

		expect(withCurrentTopology(manifest, manifest.epochs[0]!.shards, 'hrw')).toBe(manifest);
	});
});

describe('Placement under KV failure', () => {
	type Mode = { state: 'up' | 'down' | 'hung' };

	/** Fails or hangs every call while `mode.state` says so; `calls` counts what reaches the store. */
	function switchableKV(mode: Mode, withGetMany = false): { kv: KVStorage; calls: { get: number; getMany: number } } {
		const inner = createInMemoryKVProvider();
		const calls = { get: 0, getMany: 0 };
		const gate = async <T>(work: () => Promise<T>): Promise<T> => {
			if (mode.state === 'down') throw new Error('kv down');
			if (mode.state === 'hung') return await new Promise<T>(() => {});
			return await work();
		};

		const kv: KVStorage = {
			get: (async (key: string, type?: 'text' | 'json') =>
				gate(async () => {
					calls.get++;
					return type === 'json' ? await inner.get(key, 'json') : await inner.get(key, type);
				})) as KVStorage['get'],
			put: (key, value) => gate(() => inner.put(key, value)),
			delete: (key) => gate(() => inner.delete(key)),
			list: (options) => gate(() => inner.list(options))
		};

		if (withGetMany) {
			kv.getMany = (async (keys: string[], type?: 'text' | 'json') =>
				gate(async () => {
					calls.getMany++;
					return type === 'json' ? await inner.getMany!(keys, 'json') : await inner.getMany!(keys, type);
				})) as KVStorage['getMany'];
		}

		return { kv, calls };
	}

	beforeEach(() => {
		resetPlacementState();
	});

	it('throws PLACEMENT_UNVERIFIED when the manifest read fails on a cold cache', async () => {
		const mode: Mode = { state: 'down' };
		const { kv } = switchableKV(mode);

		const error = await loadPlacementManifest(kv).catch((caught) => caught);

		expect(error).toMatchObject({ code: 'PLACEMENT_UNVERIFIED' });
		expect(error.message).toMatch(/retry/i);
		expect(error.message).toContain('kv down');
	});

	it('throws PLACEMENT_UNVERIFIED when the exception read fails on a cold cache', async () => {
		const mode: Mode = { state: 'down' };
		const { kv } = switchableKV(mode);

		await expect(loadPlacementExceptions(kv, 0)).rejects.toMatchObject({ code: 'PLACEMENT_UNVERIFIED' });
	});

	it('serves a stale manifest to a staleOk caller and refuses a caller that needs a verified one', async () => {
		const mode: Mode = { state: 'up' };
		const { kv } = switchableKV(mode);
		const manifest = createManifest(SHARDS, 'hrw');
		await savePlacementManifest(kv, manifest);

		setClock(Date.now() + 11_000);
		mode.state = 'down';

		expect(await loadPlacementManifest(kv, undefined, { staleOk: true })).toEqual(manifest);
		await expect(loadPlacementManifest(kv)).rejects.toMatchObject({ code: 'PLACEMENT_UNVERIFIED' });
	});

	it('serves a stale exception set to a staleOk caller and refuses a caller that needs a verified one', async () => {
		const mode: Mode = { state: 'up' };
		const { kv } = switchableKV(mode);
		let manifest = createManifest(SHARDS, 'hrw');
		await savePlacementManifest(kv, manifest);
		manifest = await addPlacementException(kv, manifest, 'hashed-key-1');

		setClock(Date.now() + 11_000);
		mode.state = 'down';

		const stale = await loadPlacementExceptions(kv, manifest.exceptionsVersion, undefined, { staleOk: true });
		expect(stale.has('hashed-key-1')).toBe(true);
		await expect(loadPlacementExceptions(kv, manifest.exceptionsVersion)).rejects.toMatchObject({ code: 'PLACEMENT_UNVERIFIED' });
	});

	it('does not serve a cached exception set that predates the manifest version', async () => {
		const mode: Mode = { state: 'up' };
		const { kv } = switchableKV(mode);
		let manifest = createManifest(SHARDS, 'hrw');
		await savePlacementManifest(kv, manifest);
		manifest = await addPlacementException(kv, manifest, 'hashed-key-1');

		setClock(Date.now() + 11_000);
		mode.state = 'down';

		await expect(loadPlacementExceptions(kv, manifest.exceptionsVersion + 1, undefined, { staleOk: true })).rejects.toMatchObject({
			code: 'PLACEMENT_UNVERIFIED'
		});
	});

	it('reads a hung KV as a failure through withKVTimeout, never as a missing manifest', async () => {
		const mode: Mode = { state: 'hung' };
		const { kv } = switchableKV(mode);
		const bounded = withKVTimeout(kv, 30);

		await expect(loadPlacementManifest(bounded)).rejects.toMatchObject({ code: 'PLACEMENT_UNVERIFIED' });
		await expect(loadPlacementExceptions(bounded, 0)).rejects.toMatchObject({ code: 'PLACEMENT_UNVERIFIED' });
	});

	it('still returns null for a manifest that is genuinely absent', async () => {
		const { kv } = switchableKV({ state: 'up' });

		expect(await loadPlacementManifest(kv)).toBeNull();
	});

	it('reads the manifest and the exception set in one getMany when the store has it', async () => {
		const { kv, calls } = switchableKV({ state: 'up' }, true);
		let manifest = createManifest(SHARDS, 'hrw');
		await savePlacementManifest(kv, manifest);
		manifest = await addPlacementException(kv, manifest, 'hashed-key-1');
		resetPlacementState();
		calls.get = 0;
		calls.getMany = 0;

		const loaded = await loadPlacementManifest(kv);
		const exceptions = await loadPlacementExceptions(kv, loaded!.exceptionsVersion);

		expect(exceptions.has('hashed-key-1')).toBe(true);
		expect(calls).toEqual({ get: 0, getMany: 1 });
	});

	it('falls back to two gets when the store has no getMany', async () => {
		const { kv, calls } = switchableKV({ state: 'up' });
		await savePlacementManifest(kv, createManifest(SHARDS, 'hrw'));
		resetPlacementState();

		const loaded = await loadPlacementManifest(kv);
		await loadPlacementExceptions(kv, loaded!.exceptionsVersion);

		expect(calls).toEqual({ get: 2, getMany: 0 });
	});
});

// #region journal-held manifest, fence and pruning

const MANIFEST_KEY = 'collegedb:placement:manifest';
const EXCEPTIONS_KEY = 'collegedb:placement:exceptions';
const JOURNAL = 'db-a';
const NAMES2 = ['db-a', 'db-b'];
const NAMES3 = ['db-a', 'db-b', 'db-c'];
const NAMES4 = [...NAMES3, 'db-d'];

interface JournalWorld {
	kv: KVStorage;
	blind: { on: boolean };
	puts: string[];
	raw: Record<string, DatabaseSync>;
	shards: Record<string, SQLDatabase>;
}

function makeWorld(): JournalWorld {
	const inner = createInMemoryKVProvider();
	const blind = { on: false };
	const puts: string[] = [];
	const hidden = (key: string) => blind.on && key === MANIFEST_KEY;

	const kv: KVStorage = {
		get: (async (key: string, type?: 'text' | 'json') => (hidden(key) ? null : await inner.get(key, type as 'json'))) as KVStorage['get'],
		put: async (key, value) => {
			puts.push(key);
			await inner.put(key, value);
		},
		delete: (key) => inner.delete(key),
		list: (options) => inner.list(options),
		getMany: (async (keys: string[], type?: 'text' | 'json') =>
			await Promise.all(keys.map(async (key) => (hidden(key) ? null : await inner.get(key, type as 'json'))))) as KVStorage['getMany']
	};

	const raw: Record<string, DatabaseSync> = {};
	const shards: Record<string, SQLDatabase> = {};
	for (const name of NAMES4) {
		raw[name] = openNodeSqlite();
		raw[name].exec('CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT)');
		shards[name] = createNodeSqliteProvider(raw[name]);
	}

	return { kv, blind, puts, raw, shards };
}

/** Starts a fresh process over the world's databases. */
async function boot(world: JournalWorld, names: string[] = NAMES3, extra: Record<string, unknown> = {}): Promise<void> {
	resetConfig();
	resetPlacementState();
	await initializeAsync({
		kv: world.kv,
		shards: Object.fromEntries(names.map((name) => [name, world.shards[name]!])),
		strategy: 'hash',
		placement: 'computed',
		journalShard: JOURNAL,
		disableAutoMigration: true,
		hashShardMappings: false,
		...extra
	});
}

/** Boots and makes one read, which is what publishes the topology. */
async function bootAndWarm(world: JournalWorld, names: string[] = NAMES3, extra: Record<string, unknown> = {}): Promise<void> {
	await boot(world, names, extra);
	await first('warm', 'SELECT * FROM items WHERE id = ?', ['warm']);
}

const journalOf = (world: JournalWorld) => ({ db: world.shards[JOURNAL]!, scope: 'default' });

function storedRow(world: JournalWorld): { revision: number; generation: number; manifest: PlacementManifest } {
	const row = world.raw[JOURNAL]!.prepare('SELECT revision, generation, body FROM collegedb_manifest').get() as {
		revision: number;
		generation: number;
		body: string;
	};
	return { revision: Number(row.revision), generation: Number(row.generation), manifest: JSON.parse(row.body) as PlacementManifest };
}

const epochNumbers = (manifest: PlacementManifest) => manifest.epochs.map((epoch) => epoch.n);

function rowsOn(world: JournalWorld, shard: string, key: string, table = 'items', column = 'id'): number {
	return Number((world.raw[shard]!.prepare(`SELECT COUNT(*) AS n FROM ${table} WHERE ${column} = ?`).get(key) as { n: number }).n);
}

function fenceOn(world: JournalWorld, shard: string): number {
	return Number(
		(world.raw[shard]!.prepare('SELECT COALESCE(MAX(min_generation), 0) AS fence FROM collegedb_topology').get() as { fence: number }).fence
	);
}

function findKey(matches: (key: string) => boolean, prefix = 'key'): string {
	for (let i = 0; i < 5000; i++) {
		if (matches(`${prefix}-${i}`)) return `${prefix}-${i}`;
	}
	throw new Error('no key matched');
}

async function codeOf(work: Promise<unknown>): Promise<string | undefined> {
	try {
		await work;
		return undefined;
	} catch (error) {
		return (error as { code?: string }).code ?? `raw:${(error as Error).message}`;
	}
}

afterEach(() => {
	setPruneStepHook(null);
	restoreClock();
	resetConfig();
});

describe('Journal-held manifest', () => {
	it('keeps a cold isolate whose KV forgot the manifest from publishing epoch 1 over it', async () => {
		const world = makeWorld();
		await bootAndWarm(world, NAMES2);
		await bootAndWarm(world, NAMES3);
		expect(epochNumbers(storedRow(world).manifest)).toEqual([2, 1]);
		expect(storedRow(world).revision).toBe(2);

		world.blind.on = true;
		world.puts.length = 0;
		await bootAndWarm(world, NAMES3);

		expect(epochNumbers(storedRow(world).manifest)).toEqual([2, 1]);
		expect(storedRow(world).revision).toBe(2);
		expect(world.puts).not.toContain(MANIFEST_KEY);
	});

	it('lets one of two racing publishers win the swap and the other keep both epochs', async () => {
		const world = makeWorld();
		const updates = { count: 0 };
		const db = world.shards[JOURNAL]!;
		const counted: SQLDatabase = {
			...db,
			prepare: (sql) => {
				if (sql.startsWith('UPDATE collegedb_manifest')) updates.count++;
				return db.prepare(sql);
			}
		};
		const journal = { db: counted, scope: 'default' };

		const seeded = await publishPlacementTopology(world.kv, journal, null, ['a', 'b']);
		updates.count = 0;

		const [first, second] = await Promise.all([
			publishPlacementTopology(world.kv, journal, seeded, ['a', 'b', 'c']),
			publishPlacementTopology(world.kv, journal, seeded, ['a', 'b', 'd'])
		]);

		const stored = (await loadJournalManifest(journal))!;
		expect(epochNumbers(stored.manifest)).toEqual([3, 2, 1]);
		expect(stored.revision).toBe(3);
		expect(new Set(stored.manifest.epochs.slice(0, 2).map((epoch) => epoch.shards.join()))).toEqual(new Set(['a,b,c', 'a,b,d']));
		expect(updates.count).toBeGreaterThan(2);
		expect([first.epochs.length, second.epochs.length].sort()).toEqual([2, 3]);
	});

	it('gives up with PLACEMENT_CONFLICT after a bounded number of lost swaps', async () => {
		const world = makeWorld();
		const attempts = { count: 0 };
		const db = world.shards[JOURNAL]!;
		const lose = (statement: PreparedStatement): PreparedStatement => ({
			bind: (...bindings: any[]) => lose(statement.bind(...bindings)),
			run: async () => ({ ...(await statement.run()), meta: { duration: 0, changes: 0 } }),
			all: () => statement.all(),
			first: () => statement.first()
		});
		const lossy: SQLDatabase = {
			...db,
			prepare: (sql) => {
				if (!sql.startsWith('UPDATE collegedb_manifest')) return db.prepare(sql);
				attempts.count++;
				return lose(db.prepare(sql));
			}
		};
		const journal = { db: lossy, scope: 'default' };
		await commitJournalManifest(journal, () => createManifest(['a', 'b'], 'hrw'));

		const bump = (current: PlacementManifest | null) => ({ ...current!, exceptionsVersion: current!.exceptionsVersion + 1 });
		expect(await codeOf(commitJournalManifest(journal, bump))).toBe('PLACEMENT_CONFLICT');
		expect(attempts.count).toBeGreaterThan(1);
		expect(attempts.count).toBeLessThan(20);
	});

	it('does not let a router holding an older manifest roll the journal back when it records a move', async () => {
		const world = makeWorld();
		await bootAndWarm(world, NAMES3);

		await commitJournalManifest(journalOf(world), (current) => withCurrentTopology(current!, NAMES4, 'hrw'));

		const key = 'move-1';
		const owner = hrwShard(key, NAMES3);
		const target = NAMES3.find((name) => name !== owner)!;
		world.raw[owner]!.prepare('INSERT INTO items (id, name) VALUES (?, ?)').run(key, 'one');

		await reassignShard(key, target, 'items');

		const stored = storedRow(world);
		expect(epochNumbers(stored.manifest)).toEqual([2, 1]);
		expect(stored.manifest.exceptionsVersion).toBeGreaterThan(0);
		expect(epochNumbers((await world.kv.get<PlacementManifest>(MANIFEST_KEY, 'json'))!)).toEqual([2, 1]);
	});

	it('bumps the exception version from the journal copy, not from the manifest the caller holds', async () => {
		const world = makeWorld();
		await boot(world, NAMES3);
		const journal = journalOf(world);
		const older = (await commitJournalManifest(journal, () => createManifest(NAMES3, 'hrw'))).manifest;
		await commitJournalManifest(journal, (current) => withCurrentTopology(current!, NAMES4, 'hrw'));

		const updated = await addPlacementException(world.kv, older, 'hashed-1', journal);

		expect(epochNumbers(updated)).toEqual([2, 1]);
		expect(epochNumbers(storedRow(world).manifest)).toEqual([2, 1]);
		expect(storedRow(world).manifest.exceptionsVersion).toBe(1);
	});

	it('keeps a KV-only router from rolling the stored manifest back to its older epoch when it records an exception', async () => {
		const world = makeWorld();
		await bootAndWarm(world, NAMES3, { journalShard: undefined });

		const key = 'pinned-1';
		const owner = hrwShard(key, NAMES3);
		const elsewhere = NAMES3.find((name) => name !== owner)!;
		await new KVShardMapper(world.kv, { hashShardMappings: false }).setShardMapping(key, elsewhere);

		const mine = swapPlacementCache();
		await savePlacementManifest(world.kv, withCurrentTopology(createManifest(NAMES3, 'hrw'), NAMES4, 'hrw'));
		swapPlacementCache(mine);

		await reassignShard(key, elsewhere, 'items');

		expect(epochNumbers((await world.kv.get<PlacementManifest>(MANIFEST_KEY, 'json'))!)).toEqual([2, 1]);
		expect((await world.kv.get<PlacementManifest>(MANIFEST_KEY, 'json'))!.exceptionsVersion).toBe(1);
	});

	it('survives losing the KV copy and writes it again on the next publish', async () => {
		const world = makeWorld();
		await bootAndWarm(world, NAMES3);
		await world.kv.delete(MANIFEST_KEY);
		await world.kv.delete(EXCEPTIONS_KEY);

		await bootAndWarm(world, NAMES3);
		expect(storedRow(world).revision).toBe(1);

		await bootAndWarm(world, NAMES4);
		expect(epochNumbers(storedRow(world).manifest)).toEqual([2, 1]);
		expect(await world.kv.get(MANIFEST_KEY, 'json')).toEqual(storedRow(world).manifest);
	});

	it('seeds a journal that has no manifest from the KV copy instead of starting over', async () => {
		const world = makeWorld();
		const existing = withCurrentTopology(createManifest(NAMES2, 'modulo'), NAMES3, 'hrw');
		await savePlacementManifest(world.kv, existing);

		await bootAndWarm(world, NAMES3);

		expect(epochNumbers(storedRow(world).manifest)).toEqual([2, 1]);
		expect(storedRow(world).revision).toBe(1);
	});

	it('refuses a ninth epoch through the journal and leaves the stored manifest alone', async () => {
		const world = makeWorld();
		const journal = journalOf(world);
		let full = createManifest(['x-0'], 'hrw');
		for (let i = 2; i <= MAX_RETAINED_EPOCHS; i++) {
			full = withCurrentTopology(
				full,
				Array.from({ length: i }, (_, index) => `x-${index}`)
			);
		}
		await commitJournalManifest(journal, () => full);

		expect(await codeOf(publishPlacementTopology(world.kv, journal, full, NAMES3))).toBe('TOO_MANY_EPOCHS');
		expect(storedRow(world).manifest).toEqual(full);
		expect(storedRow(world).revision).toBe(1);
	});

	it('treats an unreachable journal like an unreachable KV: stale reads, refused writes, a cold start that fails', async () => {
		const world = makeWorld();
		const down = { on: false };
		const db = world.shards[JOURNAL]!;
		world.shards[JOURNAL] = {
			...db,
			prepare: (sql) => {
				if (down.on && sql.includes('FROM collegedb_manifest')) throw new Error('journal is down');
				return db.prepare(sql);
			}
		};
		await bootAndWarm(world, NAMES3);
		await insertInto('a-1', 'items', { id: 'a-1', name: 'x' });

		setClock(Date.now() + 11_000);
		down.on = true;
		expect(await first<{ name: string }>('a-1', 'SELECT * FROM items WHERE id = ?', ['a-1'])).toMatchObject({ name: 'x' });
		expect(await codeOf(insertInto('a-2', 'items', { id: 'a-2', name: 'y' }))).toBe('PLACEMENT_UNVERIFIED');

		await boot(world, NAMES3);
		await expect(first('a-1', 'SELECT * FROM items WHERE id = ?', ['a-1'])).rejects.toMatchObject({
			code: 'PLACEMENT_UNVERIFIED',
			message: expect.stringMatching(/the journal failed/)
		});

		down.on = false;
		expect(await first<{ name: string }>('a-1', 'SELECT * FROM items WHERE id = ?', ['a-1'])).toMatchObject({ name: 'x' });
	});

	it('reads the manifest once for a burst of cold operations', async () => {
		const world = makeWorld();
		await bootAndWarm(world, NAMES3);
		await boot(world, NAMES3);

		const reads = { count: 0 };
		const db = world.shards[JOURNAL]!;
		world.shards[JOURNAL] = {
			...db,
			prepare: (sql) => {
				if (sql.includes('FROM collegedb_manifest')) reads.count++;
				return db.prepare(sql);
			}
		};
		await boot(world, NAMES3);

		await Promise.all(Array.from({ length: 12 }, (_, i) => first(`burst-${i}`, 'SELECT * FROM items WHERE id = ?', [`burst-${i}`])));

		// the load and the commit that checks it, once, not once per operation
		expect(reads.count).toBe(2);
	});
});

describe('Topology fence', () => {
	it('only ever moves the fence up', async () => {
		const world = makeWorld();
		await bootAndWarm(world, NAMES3);
		const db = world.shards['db-b']!;

		expect(await readFence(db)).toBe(0);
		await raiseFence(db, 5);
		await raiseFence(db, 3);
		expect(await readFence(db)).toBe(5);
		expect(Number((world.raw['db-b']!.prepare('SELECT COUNT(*) AS n FROM collegedb_topology').get() as { n: number }).n)).toBe(1);
		expect(buildFenceRaise(4, 'postgres')[0]!.sql).toContain('CAST(? AS BIGINT)');
	});

	it('turns away a writer on an older generation, which reloads the manifest and lands the row on the new owner', async () => {
		const world = makeWorld();
		await bootAndWarm(world, NAMES3);

		await commitJournalManifest(journalOf(world), (current) => withCurrentTopology(current!, NAMES3, 'modulo'));
		for (const name of NAMES3) await raiseFence(world.shards[name]!, 2);

		const key = findKey((candidate) => hrwShard(candidate, NAMES3) !== legacyModuloShard(candidate, NAMES3));
		const result = await insertInto(key, 'items', { id: key, name: 'late' });

		expect(result.meta.changes).toBe(1);
		expect(rowsOn(world, legacyModuloShard(key, NAMES3), key)).toBe(1);
		expect(rowsOn(world, hrwShard(key, NAMES3), key)).toBe(0);
	});

	it('refuses with STALE_TOPOLOGY when the reload does not bring the writer up to the fence', async () => {
		const world = makeWorld();
		await bootAndWarm(world, NAMES3);
		for (const name of NAMES3) await raiseFence(world.shards[name]!, 99);

		expect(await codeOf(insertInto('stale-1', 'items', { id: 'stale-1', name: 'x' }))).toBe('STALE_TOPOLOGY');
		expect(NAMES3.map((name) => rowsOn(world, name, 'stale-1'))).toEqual([0, 0, 0]);
	});

	it('reports a tombstone as a tombstone even when the fence is also up', async () => {
		const world = makeWorld();
		await bootAndWarm(world, NAMES3);

		const key = 'moved-1';
		const owner = hrwShard(key, NAMES3);
		world.raw[owner]!.prepare(
			'INSERT INTO collegedb_retired (scope, table_name, primary_key, pk_column, op_id, generation, moved_to, payload, archived_at) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 1)'
		).run(
			'default',
			'items',
			key,
			'id',
			'op-1',
			1,
			NAMES3.find((name) => name !== owner)!
		);
		for (const name of NAMES3) await raiseFence(world.shards[name]!, 99);

		expect(await codeOf(insertInto(key, 'items', { id: key, name: 'x' }))).toBe('MIGRATION_CUTOVER_PENDING');
	});

	it('returns an ignored conflict from an up-to-date writer instead of retrying it', async () => {
		const world = makeWorld();
		await bootAndWarm(world, NAMES3);

		const first = await insertInto('dup-1', 'items', { id: 'dup-1', name: 'one' }, { orIgnore: true });
		const again = await insertInto('dup-1', 'items', { id: 'dup-1', name: 'two' }, { orIgnore: true });

		expect(first.meta.changes).toBe(1);
		expect(again.meta.changes).toBe(0);
		expect(rowsOn(world, hrwShard('dup-1', NAMES3), 'dup-1')).toBe(1);
	});

	it('never turns away a writer that routes through KV mappings', async () => {
		const world = makeWorld();
		await boot(world, NAMES3, { placement: undefined });
		for (const name of NAMES3) await raiseFence(world.shards[name]!, 99);

		const result = await insertInto('mapped-1', 'items', { id: 'mapped-1', name: 'x' });
		expect(result.meta.changes).toBe(1);
	});

	it('keeps a writer racing the fence raise from landing a row the census cannot see', async () => {
		const report = await runFenceRace({ iterations: 24 });

		expect(report.violations).toEqual([]);
		expect(report.landed).toBeGreaterThan(0);
		expect(report.rejected).toBeGreaterThan(0);
		expect(report.landed + report.rejected).toBe(24);
	});

	it('shows the race check can fail: without the fence a late writer lands after the census', async () => {
		const report = await runFenceRace({ iterations: 24, control: 'unfenced' });

		expect(report.violations.length).toBeGreaterThan(0);
	});
});

describe('pruneEpochs', () => {
	/** Rows placed under epoch 1 over two shards, then a third shard joins as epoch 2. */
	async function relocatedWorld(count = 24) {
		const world = makeWorld();
		await bootAndWarm(world, NAMES2);

		const keys = Array.from({ length: count }, (_, i) => `k-${i}`);
		for (const key of keys) {
			await insertInto(key, 'items', { id: key, name: key });
		}

		await bootAndWarm(world, NAMES3);
		const relocated = keys.filter((key) => hrwShard(key, NAMES3) !== hrwShard(key, NAMES2));
		expect(relocated.length).toBeGreaterThan(0);

		return { world, keys, relocated };
	}

	const pin = async (keys: string[]) => {
		for (const key of keys) {
			expect(await first<{ name: string }>(key, 'SELECT * FROM items WHERE id = ?', [key])).toMatchObject({ name: key });
		}
	};

	it('refuses without a journal shard', async () => {
		const world = makeWorld();
		await bootAndWarm(world, NAMES3, { journalShard: undefined });

		expect(await codeOf(pruneEpochs({ tables: ['items'] }))).toBe('JOURNAL_NOT_CONFIGURED');
	});

	it('refuses an empty table list, an unknown table and a zero page size', async () => {
		const { world } = await relocatedWorld(6);
		const before = storedRow(world);

		expect(await codeOf(pruneEpochs({ tables: [] }))).toBe('PRUNE_REFUSED');
		expect(await codeOf(pruneEpochs({ tables: ['nothing_here'] }))).toBe('PRUNE_REFUSED');
		expect(await codeOf(pruneEpochs({ tables: ['items'], pageSize: 0 }))).toBe('PRUNE_REFUSED');
		expect(storedRow(world)).toEqual(before);
	});

	it('refuses when a shard cannot be read or fenced', async () => {
		const { world } = await relocatedWorld(6);
		const fail = (db: SQLDatabase, when: (sql: string) => boolean, on: 'all' | 'run'): SQLDatabase => ({
			...db,
			prepare: (sql) => {
				const statement = db.prepare(sql);
				const wrap = (inner: PreparedStatement): PreparedStatement => ({
					bind: (...bindings: any[]) => wrap(inner.bind(...bindings)),
					run: () => (on === 'run' && when(sql) ? Promise.reject(new Error('shard is down')) : inner.run()),
					all: () => (on === 'all' && when(sql) ? Promise.reject(new Error('shard is down')) : inner.all()),
					first: () => inner.first()
				});
				return wrap(statement);
			}
		});
		const healthy = world.shards['db-b']!;

		world.shards['db-b'] = fail(healthy, (sql) => sql.includes('collegedb_key'), 'all');
		await boot(world, NAMES3);
		await first('warm', 'SELECT * FROM items WHERE id = ?', ['warm']);
		await expect(pruneEpochs({ tables: ['items'] })).rejects.toThrow(/could not scan items on db-b/);

		world.shards['db-b'] = fail(healthy, (sql) => /^(INSERT INTO|UPDATE) "collegedb_topology"/.test(sql), 'run');
		await boot(world, NAMES3);
		await first('warm', 'SELECT * FROM items WHERE id = ?', ['warm']);
		await expect(pruneEpochs({ tables: ['items'] })).rejects.toThrow(/could not raise the fence on db-b/);
	});

	it('refuses when a shard speaks a dialect the fence is not proven on', async () => {
		const { world } = await relocatedWorld(6);
		world.shards['db-c'] = { ...world.shards['db-c']!, dialect: 'cockroach' as never };
		await boot(world, NAMES3);
		await first('warm', 'SELECT * FROM items WHERE id = ?', ['warm']);

		await expect(pruneEpochs({ tables: ['items'] })).rejects.toMatchObject({
			code: 'PRUNE_REFUSED',
			message: expect.stringMatching(/db-c.*not proven/)
		});
		expect(NAMES3.map((name) => fenceOn(world, name))).toEqual([0, 0, 0]);
	});

	it('reports the dependents of each epoch on a dry run and changes nothing, fence included', async () => {
		const { world, keys, relocated } = await relocatedWorld();
		const before = storedRow(world);

		const report = await pruneEpochs({ tables: ['items'], dryRun: true });

		expect(report).toEqual({
			removed: [],
			retained: [2, 1],
			dependents: { 1: relocated.length, 2: keys.length - relocated.length },
			scanned: keys.length
		});
		expect(storedRow(world)).toEqual(before);
		expect(NAMES3.map((name) => fenceOn(world, name))).toEqual([0, 0, 0]);
	});

	it('keeps an epoch while a row sits where only that epoch would look', async () => {
		const { world, relocated } = await relocatedWorld();
		const before = storedRow(world);

		const report = await pruneEpochs({ tables: ['items'] });

		expect(report.removed).toEqual([]);
		expect(report.retained).toEqual([2, 1]);
		expect(report.dependents[1]).toBe(relocated.length);
		expect(storedRow(world)).toEqual(before);
		expect(NAMES3.map((name) => fenceOn(world, name))).toEqual([2, 2, 2]);
	});

	it('removes an epoch once every row under it has an exception, and every key still resolves', async () => {
		const { world, keys, relocated } = await relocatedWorld();
		await pin(relocated);

		const report = await pruneEpochs({ tables: ['items'] });

		expect(report.removed).toEqual([1]);
		expect(report.retained).toEqual([2]);
		expect(report.dependents[1]).toBe(0);
		expect(epochNumbers(storedRow(world).manifest)).toEqual([2]);
		expect(epochNumbers((await world.kv.get<PlacementManifest>(MANIFEST_KEY, 'json'))!)).toEqual([2]);

		await boot(world, NAMES3);
		for (const key of keys) {
			expect(await first<{ name: string }>(key, 'SELECT * FROM items WHERE id = ?', [key])).toMatchObject({ name: key });
		}
	});

	it('does not count a KV mapping without an exception as a reason to keep reading old epochs', async () => {
		const { world, relocated } = await relocatedWorld();
		const mapper = new KVShardMapper(world.kv, { hashShardMappings: false });
		for (const key of relocated) {
			await mapper.setShardMapping(key, hrwShard(key, NAMES2));
		}

		const report = await pruneEpochs({ tables: ['items'], dryRun: true });

		expect(report.dependents[1]).toBe(relocated.length);
	});

	it('never removes the newest epoch, even with no rows at all', async () => {
		const world = makeWorld();
		await bootAndWarm(world, NAMES2);
		await bootAndWarm(world, NAMES3);

		const report = await pruneEpochs({ tables: ['items'] });

		expect(report).toEqual({ removed: [1], retained: [2], dependents: { 1: 0, 2: 0 }, scanned: 0 });
	});

	it('has nothing to prune with a single epoch', async () => {
		const world = makeWorld();
		await bootAndWarm(world, NAMES3);

		expect(await pruneEpochs({ tables: ['items'] })).toEqual({ removed: [], retained: [1], dependents: { 1: 0 }, scanned: 0 });
		expect(NAMES3.map((name) => fenceOn(world, name))).toEqual([0, 0, 0]);
	});

	it('reads each table in keyset pages that cover every row once and never use OFFSET', async () => {
		const { world, keys, relocated } = await relocatedWorld(20);
		await pin(relocated);

		const statements: string[] = [];
		for (const name of NAMES3) {
			const db = world.shards[name]!;
			world.shards[name] = {
				...db,
				prepare: (sql) => {
					if (sql.includes('collegedb_key')) statements.push(sql);
					return db.prepare(sql);
				}
			};
		}
		await bootAndWarm(world, NAMES3);

		const report = await pruneEpochs({ tables: ['items'], pageSize: 3, concurrency: 2 });

		expect(report.scanned).toBe(keys.length);
		expect(report.removed).toEqual([1]);
		expect(statements.length).toBeGreaterThan(NAMES3.length * 2);
		expect(statements.every((sql) => sql.includes('LIMIT') && !sql.includes('OFFSET'))).toBe(true);
		expect(statements.some((sql) => sql.includes('> ?'))).toBe(true);
	});

	it('reads the key column named for each table', async () => {
		const { world, relocated } = await relocatedWorld();
		for (const name of NAMES3) world.raw[name]!.exec('CREATE TABLE things (code TEXT PRIMARY KEY)');
		for (const key of relocated) world.raw[hrwShard(key, NAMES2)]!.prepare('INSERT INTO things (code) VALUES (?)').run(key);

		const report = await pruneEpochs({ tables: ['things'], pkColumns: { things: 'code' }, dryRun: true });

		expect(report.scanned).toBe(relocated.length);
		expect(report.dependents[1]).toBe(relocated.length);
	});

	it('refuses when a key written between the two censuses depends on an epoch', async () => {
		const { world, relocated } = await relocatedWorld();
		await pin(relocated);
		const before = storedRow(world);

		const late = findKey((key) => hrwShard(key, NAMES3) !== hrwShard(key, NAMES2), 'late');
		setPruneStepHook((step) => {
			if (step === 'census-1') {
				world.raw[hrwShard(late, NAMES2)]!.prepare('INSERT INTO items (id, name) VALUES (?, ?)').run(late, 'late');
			}
		});

		await expect(pruneEpochs({ tables: ['items'] })).rejects.toMatchObject({
			code: 'PRUNE_REFUSED',
			message: expect.stringMatching(/changed between/)
		});
		expect(storedRow(world)).toEqual(before);
	});

	it('stops a writer on the old generation from creating a dependency once the fence is up', async () => {
		const world = makeWorld();
		await bootAndWarm(world, NAMES3);
		await commitJournalManifest(journalOf(world), (current) => withCurrentTopology(current!, NAMES3, 'modulo'));

		const key = findKey((candidate) => hrwShard(candidate, NAMES3) !== legacyModuloShard(candidate, NAMES3), 'writer');
		let written: Awaited<ReturnType<typeof insertInto>> | undefined;
		setPruneStepHook(async (step) => {
			if (step === 'fenced') {
				written = await insertInto(key, 'items', { id: key, name: 'racing' });
			}
		});

		const report = await pruneEpochs({ tables: ['items'] });

		// the stale writer was turned away, reloaded, and wrote where the newest epoch looks
		expect(written?.meta.changes).toBe(1);
		expect(rowsOn(world, legacyModuloShard(key, NAMES3), key)).toBe(1);
		expect(rowsOn(world, hrwShard(key, NAMES3), key)).toBe(0);
		expect(report.removed).toEqual([1]);
		expect(report.dependents[1]).toBe(0);
		expect(await first<{ name: string }>(key, 'SELECT * FROM items WHERE id = ?', [key])).toMatchObject({ name: 'racing' });
	});
});

// #endregion
