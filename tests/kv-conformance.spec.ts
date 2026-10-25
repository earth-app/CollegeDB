import { afterEach, describe, expect, it, vi } from 'vitest';
import { CollegeDBError } from '../src/errors';
import { withKVTimeout } from '../src/kv-timeout';
import { KVShardMapper } from '../src/kvmap';
import { createNuxtHubKVProvider, createRedisKVProvider, createValkeyKVProvider, createWorkersKVProvider } from '../src/providers';
import { createInMemoryKVProvider } from '../src/providers-memory';
import type { KVStorage } from '../src/types';

/**
 * Every adapter has to behave the same way through the {@link KVStorage}
 * contract, because the router cannot tell them apart. The list pagination
 * cases exist because the NuxtHub adapter used to report `list_complete: true`
 * while silently truncating at `limit`, which lost keys for any caller that
 * paginated.
 */

function fakeRedis(): KVStorage {
	const store = new Map<string, string>();

	return createRedisKVProvider({
		async get(key) {
			return store.get(key) ?? null;
		},
		set(key, value) {
			store.set(key, value);
		},
		del(key) {
			store.delete(key);
		},
		scan(cursor: string, ...args: any[]) {
			const options = typeof args[0] === 'object' ? args[0] : { MATCH: args[1], COUNT: Number(args[3] ?? 10) };
			const pattern = String(options.MATCH ?? '*').replace(/\*$/, '');
			const count = Number(options.COUNT ?? 10);

			const keys = [...store.keys()].filter((key) => key.startsWith(pattern));
			const start = Number.parseInt(cursor, 10) || 0;
			const page = keys.slice(start, start + count);
			const next = start + count >= keys.length ? '0' : String(start + count);

			return { cursor: next, keys: page };
		}
	});
}

function fakeNuxtHub(): KVStorage {
	const store = new Map<string, string>();

	return createNuxtHubKVProvider({
		async get(key: string) {
			return (store.get(key) ?? null) as any;
		},
		async set(key: string, value: unknown) {
			store.set(key, typeof value === 'string' ? value : JSON.stringify(value));
		},
		async del(key: string) {
			store.delete(key);
		},
		async keys(prefix?: string) {
			return [...store.keys()].filter((key) => !prefix || key.startsWith(prefix));
		}
	});
}

function fakeWorkersKV(): KVStorage {
	const store = new Map<string, string>();

	return createWorkersKVProvider({
		async get(key: string) {
			return store.get(key) ?? null;
		},
		async put(key: string, value: string) {
			store.set(key, value);
		},
		async delete(key: string) {
			store.delete(key);
		},
		async list(options?: { prefix?: string; cursor?: string; limit?: number }) {
			const keys = [...store.keys()].filter((key) => !options?.prefix || key.startsWith(options.prefix));
			const start = Number.parseInt(options?.cursor ?? '0', 10) || 0;
			const limit = options?.limit ?? keys.length;
			const page = keys.slice(start, start + limit);
			const complete = start + limit >= keys.length;

			return {
				keys: page.map((name) => ({ name })),
				list_complete: complete,
				cursor: complete ? undefined : String(start + limit)
			};
		}
	} as any);
}

function fakeValkey(): KVStorage {
	const store = new Map<string, string>();

	return createValkeyKVProvider({
		async get(key) {
			return store.get(key) ?? null;
		},
		set(key, value) {
			store.set(key, value);
		},
		del(key) {
			store.delete(key);
		},
		scan(cursor: string) {
			const keys = [...store.keys()];
			const start = Number.parseInt(cursor, 10) || 0;
			return { cursor: start + 100 >= keys.length ? '0' : String(start + 100), keys: keys.slice(start, start + 100) };
		}
	});
}

const ADAPTERS: Array<[string, () => KVStorage]> = [
	['in-memory', createInMemoryKVProvider],
	['redis', fakeRedis],
	['valkey', fakeValkey],
	['nuxthub', fakeNuxtHub],
	['workers-kv', fakeWorkersKV]
];

describe.each(ADAPTERS)('KVStorage conformance: %s', (_name, make) => {
	it('round-trips text', async () => {
		const kv = make();
		await kv.put('a', 'value');
		expect(await kv.get('a')).toBe('value');
		expect(await kv.get('a', 'text')).toBe('value');
	});

	it('round-trips json', async () => {
		const kv = make();
		await kv.put('a', JSON.stringify({ shard: 'db-a', n: 1 }));
		expect(await kv.get<{ shard: string; n: number }>('a', 'json')).toEqual({ shard: 'db-a', n: 1 });
	});

	it('returns null for a missing key rather than throwing', async () => {
		const kv = make();
		expect(await kv.get('nope')).toBeNull();
		expect(await kv.get('nope', 'json')).toBeNull();
	});

	it('deletes a key and tolerates deleting it twice', async () => {
		const kv = make();
		await kv.put('a', '1');
		await kv.delete('a');
		await kv.delete('a');
		expect(await kv.get('a')).toBeNull();
	});

	it('lists by prefix without returning other prefixes', async () => {
		const kv = make();
		await kv.put('users:1', '1');
		await kv.put('users:2', '2');
		await kv.put('posts:1', '3');

		const result = await kv.list({ prefix: 'users:' });
		expect(result.keys.map((entry) => entry.name).sort()).toEqual(['users:1', 'users:2']);
	});

	it('paginates a list without losing keys', async () => {
		const kv = make();
		for (let i = 0; i < 25; i++) {
			await kv.put(`k:${String(i).padStart(2, '0')}`, String(i));
		}

		const seen: string[] = [];
		let cursor: string | undefined;
		let pages = 0;

		do {
			const page = await kv.list({ prefix: 'k:', cursor, limit: 10 });
			seen.push(...page.keys.map((entry) => entry.name));
			cursor = page.list_complete ? undefined : page.cursor;
			pages++;
			expect(pages).toBeLessThan(20);
		} while (cursor);

		// Every adapter must surrender all 25 keys across however many pages it
		// chooses to use, and must not claim completion while holding some back.
		expect(new Set(seen).size).toBe(25);
	});

	it('returns exactly the requested number of keys when more exist', async () => {
		const kv = make();
		for (let i = 0; i < 10; i++) {
			await kv.put(`k:${i}`, String(i));
		}

		const page = await kv.list({ prefix: 'k:', limit: 3 });
		expect(page.keys).toHaveLength(3);
		expect(page.list_complete).toBe(false);
	});

	it('reports an empty list for an unused prefix', async () => {
		const kv = make();
		const result = await kv.list({ prefix: 'absent:' });
		expect(result.keys).toEqual([]);
	});
});

describe('Redis bulk primitives', () => {
	it('uses mGet when the client provides it', async () => {
		const store = new Map([
			['a', '1'],
			['b', '2']
		]);
		let usedMulti = false;

		const kv = createRedisKVProvider({
			async get(key) {
				return store.get(key) ?? null;
			},
			set() {},
			del() {},
			scan() {
				return { cursor: '0', keys: [] };
			},
			async mGet(keys) {
				usedMulti = true;
				return keys.map((key) => store.get(key) ?? null);
			}
		});

		expect(await kv.getMany?.(['a', 'b', 'missing'])).toEqual(['1', '2', null]);
		expect(usedMulti).toBe(true);
	});

	it('omits getMany when the client cannot do it', () => {
		const kv = createRedisKVProvider({
			async get() {
				return null;
			},
			set() {},
			del() {},
			scan() {
				return { cursor: '0', keys: [] };
			}
		});

		expect(kv.getMany).toBeUndefined();
		expect(kv.putMany).toBeUndefined();
	});

	it('writes many entries through mSet', async () => {
		const store = new Map<string, string>();

		const kv = createRedisKVProvider({
			async get(key) {
				return store.get(key) ?? null;
			},
			set(key, value) {
				store.set(key, value);
			},
			del() {},
			scan() {
				return { cursor: '0', keys: [] };
			},
			mSet(entries) {
				for (const [key, value] of entries as Array<[string, string]>) {
					store.set(key, value);
				}
			}
		});

		await kv.putMany?.([
			{ key: 'a', value: '1' },
			{ key: 'b', value: '2' }
		]);

		expect(store.get('a')).toBe('1');
		expect(store.get('b')).toBe('2');
	});
});

describe('Workers KV cacheTtl', () => {
	it('passes a cacheTtl on reads by default', async () => {
		const seen: any[] = [];
		const kv = createWorkersKVProvider({
			async get(_key: string, options: any) {
				seen.push(options);
				return null;
			},
			async put() {},
			async delete() {},
			async list() {
				return { keys: [], list_complete: true };
			}
		} as any);

		await kv.get('collegedb:shard:abc', 'json');
		expect(seen[0]).toMatchObject({ type: 'text', cacheTtl: 3600 });
	});

	it('omits cacheTtl when set to zero', async () => {
		const seen: any[] = [];
		const kv = createWorkersKVProvider(
			{
				async get(_key: string, options: any) {
					seen.push(options);
					return null;
				},
				async put() {},
				async delete() {},
				async list() {
					return { keys: [], list_complete: true };
				}
			} as any,
			{ cacheTtl: 0 }
		);

		await kv.get('a');
		expect(seen[0]).toEqual({ type: 'text' });
	});
});

describe('Workers KV bulk reads and writes', () => {
	function fakeNamespace(seed: Record<string, string> = {}) {
		const store = new Map(Object.entries(seed));
		const bulkCalls: string[][] = [];
		return {
			store,
			bulkCalls,
			async get(keys: string | string[]) {
				if (Array.isArray(keys)) {
					bulkCalls.push(keys);
					const found = new Map<string, string | null>();
					for (const key of keys) {
						found.set(key, store.get(key) ?? null);
					}
					return found;
				}
				return store.get(keys) ?? null;
			},
			async put(key: string, value: string) {
				store.set(key, value);
			},
			async delete(key: string) {
				store.delete(key);
			},
			async list() {
				return { keys: [], list_complete: true };
			}
		};
	}

	it('reads in chunks of at most 100 keys', async () => {
		const ns = fakeNamespace();
		const kv = createWorkersKVProvider(ns as any);
		await kv.getMany!(Array.from({ length: 250 }, (_, i) => `k${i}`));
		expect(ns.bulkCalls.map((call) => call.length)).toEqual([100, 100, 50]);
	});

	it('keeps input order across chunk boundaries', async () => {
		const seed = Object.fromEntries(Array.from({ length: 250 }, (_, i) => [`k${i}`, `v${i}`]));
		const kv = createWorkersKVProvider(fakeNamespace(seed) as any);
		const keys = Array.from({ length: 250 }, (_, i) => `k${249 - i}`);
		expect(await kv.getMany!(keys)).toEqual(keys.map((key) => `v${key.slice(1)}`));
	});

	it('returns null for missing keys', async () => {
		const kv = createWorkersKVProvider(fakeNamespace({ a: '1', b: '2' }) as any);
		expect(await kv.getMany!(['b', 'missing', 'a'])).toEqual(['2', null, '1']);
	});

	it('surfaces a JSON parse failure with KV_JSON_PARSE_FAILED', async () => {
		const kv = createWorkersKVProvider(fakeNamespace({ bad: 'not-json' }) as any);
		await expect(kv.getMany!(['bad'], 'json')).rejects.toMatchObject({ code: 'KV_JSON_PARSE_FAILED' });
	});

	it('bounds putMany concurrency', async () => {
		const ns = fakeNamespace();
		let inFlight = 0;
		let peak = 0;
		ns.put = async (key: string, value: string) => {
			inFlight++;
			peak = Math.max(peak, inFlight);
			await new Promise((resolve) => setImmediate(resolve));
			ns.store.set(key, value);
			inFlight--;
		};
		const entries = Array.from({ length: 40 }, (_, i) => ({ key: `c${i}`, value: 'x' }));

		await createWorkersKVProvider(ns as any).putMany!(entries);
		expect(peak).toBe(16);

		peak = 0;
		await createWorkersKVProvider(ns as any, { putConcurrency: 4 }).putMany!(entries);
		expect(peak).toBe(4);
		expect(ns.store.size).toBe(40);
	});

	it('putMany is not atomic: a failed put names its key and earlier puts stay applied', async () => {
		const ns = fakeNamespace();
		const original = ns.put;
		ns.put = async (key: string, value: string) => {
			if (key === 'k3') {
				throw new Error('kv unavailable');
			}
			return original(key, value);
		};
		const kv = createWorkersKVProvider(ns as any, { putConcurrency: 1 });
		const entries = Array.from({ length: 5 }, (_, i) => ({ key: `k${i}`, value: `v${i}` }));

		await expect(kv.putMany!(entries)).rejects.toMatchObject({
			code: 'KV_PUT_MANY_FAILED',
			message: expect.stringContaining('k3')
		});
		expect([...ns.store.keys()]).toEqual(['k0', 'k1', 'k2']);
	});

	it('KVShardMapper.getShardMappings reads through bulk gets, not single gets', async () => {
		const ns = fakeNamespace();
		const mapper = new KVShardMapper(createWorkersKVProvider(ns as any), { hashShardMappings: false });
		const keys = Array.from({ length: 250 }, (_, i) => `user-${i}`);
		for (const key of keys) {
			ns.store.set(`shard:${key}`, JSON.stringify({ shard: 'a', createdAt: 1, updatedAt: 1 }));
		}

		const mappings = await mapper.getShardMappings(keys);
		expect(ns.bulkCalls.map((call) => call.length)).toEqual([100, 100, 50]);
		expect(mappings.get('user-7')?.shard).toBe('a');
	});
});

describe('KV timeout', () => {
	afterEach(() => {
		vi.useRealTimers();
	});

	const hungStore = (): KVStorage => ({
		get: () => new Promise<never>(() => {}),
		put: () => new Promise<never>(() => {}),
		delete: () => new Promise<never>(() => {}),
		list: () => new Promise<never>(() => {})
	});

	it('rejects a hung get with KV_TIMEOUT after the limit', async () => {
		const timed = withKVTimeout(hungStore(), 20);
		let settled = false;
		const pending = timed
			.get('k', 'text')
			.catch((error: unknown) => error)
			.finally(() => {
				settled = true;
			});

		await Promise.resolve();
		expect(settled).toBe(false);
		const error = await pending;

		expect(error).toBeInstanceOf(CollegeDBError);
		expect(error).toMatchObject({ code: 'KV_TIMEOUT', message: 'KV get timed out after 20 ms' });
	});

	it('races put, delete and list against the same limit', async () => {
		const timed = withKVTimeout(hungStore(), 20);
		const calls = [timed.put('k', 'v'), timed.delete('k'), timed.list()].map((call) => call.catch((error: unknown) => error));

		expect(await Promise.all(calls)).toMatchObject([
			{ code: 'KV_TIMEOUT', message: 'KV put timed out after 20 ms' },
			{ code: 'KV_TIMEOUT', message: 'KV delete timed out after 20 ms' },
			{ code: 'KV_TIMEOUT', message: 'KV list timed out after 20 ms' }
		]);
	});

	it('clears the timer when the inner call settles', async () => {
		vi.useFakeTimers();
		const inner = createInMemoryKVProvider();
		await inner.put('k', 'value');
		const timed = withKVTimeout(inner, 1000);

		await expect(timed.get('k', 'text')).resolves.toBe('value');
		expect(vi.getTimerCount()).toBe(0);
	});

	it('exposes the optional bulk methods only when the inner store has them', () => {
		const bare = withKVTimeout(hungStore(), 20);
		expect('getMany' in bare).toBe(false);
		expect('putMany' in bare).toBe(false);
		expect('deleteMany' in bare).toBe(false);

		const bulk = withKVTimeout(createInMemoryKVProvider(), 20);
		expect(typeof bulk.getMany).toBe('function');
		expect(typeof bulk.putMany).toBe('function');
		expect(typeof bulk.deleteMany).toBe('function');
	});

	it('returns the same store when the limit is not a positive finite number', () => {
		const kv = hungStore();
		for (const limit of [0, -5, Number.NaN, Number.POSITIVE_INFINITY]) {
			expect(withKVTimeout(kv, limit)).toBe(kv);
		}
	});

	it('returns the same wrapper for the same store and limit', () => {
		const kv = hungStore();
		expect(withKVTimeout(kv, 20)).toBe(withKVTimeout(kv, 20));
	});
});
