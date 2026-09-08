import { beforeEach, describe, expect, it } from 'vitest';
import {
	addPlacementException,
	candidateShards,
	createManifest,
	hrwShard,
	legacyModuloShard,
	loadPlacementExceptions,
	loadPlacementManifest,
	resetPlacementState,
	savePlacementManifest,
	shardForEpoch,
	withCurrentTopology,
	type PlacementManifest
} from '../src/placement';
import { createInMemoryKVProvider } from '../src/providers-memory';
import type { KVStorage } from '../src/types';

const SHARDS = ['db-east', 'db-west', 'db-central'];

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

	it('caps retained epochs so the walk stays bounded', () => {
		let manifest: PlacementManifest = createManifest(['a'], 'hrw');
		for (let i = 2; i <= 10; i++) {
			manifest = withCurrentTopology(
				manifest,
				new Array(i).fill(null).map((_, index) => `shard-${index}`),
				'hrw'
			);
		}

		expect(manifest.epochs.length).toBeLessThanOrEqual(4);
		expect(manifest.epochs[0]!.n).toBe(10);
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
