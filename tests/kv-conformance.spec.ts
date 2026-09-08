import { describe, expect, it } from 'vitest';
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
