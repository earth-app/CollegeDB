import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
	cached,
	createInMemoryKVProvider,
	createInMemorySQLProvider,
	deleteLookup,
	getLookup,
	initialize,
	invalidate,
	resetConfig,
	setLookup
} from '../src/index';

describe('KV read-through cache', () => {
	let kv: ReturnType<typeof createInMemoryKVProvider>;

	beforeEach(() => {
		resetConfig();
		kv = createInMemoryKVProvider();
		initialize({ kv, shards: { a: createInMemorySQLProvider() }, disableAutoMigration: true, hashShardMappings: false });
	});

	it('computes on miss and serves from cache on hit', async () => {
		const fetcher = vi.fn(async () => ({ n: 1 }));

		const first = await cached('k:1', fetcher);
		const second = await cached('k:1', fetcher);

		expect(first).toEqual({ n: 1 });
		expect(second).toEqual({ n: 1 });
		expect(fetcher).toHaveBeenCalledTimes(1);
	});

	it('recomputes when a stored entry has expired', async () => {
		// Seed an already-expired envelope directly.
		await kv.put('k:2', JSON.stringify({ v: 'stale', e: 1 }));

		const fetcher = vi.fn(async () => 'fresh');
		const value = await cached('k:2', fetcher, { ttl: 3600 });

		expect(value).toBe('fresh');
		expect(fetcher).toHaveBeenCalledTimes(1);
	});

	it('stores an expiry when a ttl is provided', async () => {
		const before = Date.now();
		await cached('k:ttl', async () => 'v', { ttl: 60 });
		const raw = await kv.get('k:ttl', 'text');
		const envelope = JSON.parse(raw as string) as { v: string; e: number };
		expect(envelope.v).toBe('v');
		expect(envelope.e).toBeGreaterThanOrEqual(before + 60_000);
	});

	it('self-heals a corrupt cache entry', async () => {
		await kv.put('k:3', 'not-json{');
		const value = await cached('k:3', async () => 'recovered');
		expect(value).toBe('recovered');
		// The corrupt entry was replaced with a valid envelope.
		const raw = await kv.get('k:3', 'text');
		expect(JSON.parse(raw as string)).toEqual({ v: 'recovered' });
	});

	it('invalidate clears every key under a prefix', async () => {
		await cached('list:1', async () => 'a');
		await cached('list:2', async () => 'b');
		await cached('other:1', async () => 'c');

		const deleted = await invalidate('list:');
		expect(deleted).toBe(2);
		expect(await kv.get('list:1', 'text')).toBeNull();
		expect(await kv.get('list:2', 'text')).toBeNull();
		expect(await kv.get('other:1', 'text')).not.toBeNull();
	});

	it('accepts an explicit kv override without global init', async () => {
		resetConfig();
		const standalone = createInMemoryKVProvider();
		const value = await cached('k:4', async () => 42, { kv: standalone });
		expect(value).toBe(42);
	});

	it('throws when not initialized and no kv is provided', async () => {
		resetConfig();
		await expect(cached('k:5', async () => 1)).rejects.toThrow('CollegeDB not initialized');
	});
});

describe('Secondary-index lookups', () => {
	let kv: ReturnType<typeof createInMemoryKVProvider>;

	beforeEach(() => {
		resetConfig();
		kv = createInMemoryKVProvider();
		initialize({ kv, shards: { a: createInMemorySQLProvider() }, disableAutoMigration: true, hashShardMappings: false });
	});

	it('round-trips a value through set/get/delete', async () => {
		await setLookup('email-hash-abc', '42');
		expect(await getLookup('email-hash-abc')).toBe('42');

		await deleteLookup('email-hash-abc');
		expect(await getLookup('email-hash-abc')).toBeNull();
	});

	it('namespaces keys so different indexes do not collide', async () => {
		await setLookup('key', 'customer-value', { namespace: 'idx:customer:' });
		await setLookup('key', 'user-value', { namespace: 'idx:user:' });

		expect(await getLookup('key', { namespace: 'idx:customer:' })).toBe('customer-value');
		expect(await getLookup('key', { namespace: 'idx:user:' })).toBe('user-value');
	});

	it('returns null for a missing lookup', async () => {
		expect(await getLookup('nope')).toBeNull();
	});

	it('writes under the default namespace prefix', async () => {
		await setLookup('slug-1', 'post-1');
		expect(await kv.get('collegedb:lookup:slug-1', 'text')).toBe('post-1');
	});

	it('throws when not initialized and no kv is provided', async () => {
		resetConfig();
		await expect(setLookup('k', 'v')).rejects.toThrow('CollegeDB not initialized');
	});
});
