import { sql as drizzleSql } from 'drizzle-orm';
import { describe, expect, it } from 'vitest';
import { KVShardMapper } from '../src/kvmap';
import {
	createDrizzleSQLProvider,
	createHyperdriveMySQLProvider,
	createHyperdrivePostgresProvider,
	createMySQLProvider,
	createNuxtHubKVProvider,
	createPostgreSQLProvider,
	createRedisKVProvider,
	createSQLiteProvider,
	createValkeyKVProvider,
	createWorkersKVProvider,
	isKVStorage,
	isSQLDatabase,
	toProvider,
	type SQLBatchOptions,
	type WorkersKVNamespaceLike
} from '../src/providers';
import { createNodeSqliteClient, openNodeSqlite } from './helpers/concurrent-sqlite';

class MockRedisClient {
	private readonly store = new Map<string, string>();
	private readonly tupleMode: boolean;

	constructor(tupleMode: boolean = true) {
		this.tupleMode = tupleMode;
	}

	async get(key: string): Promise<string | null> {
		return this.store.get(key) ?? null;
	}

	async set(key: string, value: string): Promise<void> {
		this.store.set(key, value);
	}

	async del(key: string): Promise<void> {
		this.store.delete(key);
	}

	async scan(cursor: string, ...args: any[]): Promise<any> {
		const pattern = extractPattern(args);
		const prefix = pattern.endsWith('*') ? pattern.slice(0, -1) : pattern;
		const keys = Array.from(this.store.keys()).filter((key) => key.startsWith(prefix));

		if (this.tupleMode) {
			return [cursor === '0' ? '0' : cursor, keys];
		}

		return {
			cursor: '0',
			keys
		};
	}
}

class InstrumentedKVStorage {
	private readonly store = new Map<string, string>();
	public getCalls = 0;

	async get<T = unknown>(key: string, type: 'json'): Promise<T | null>;
	async get(key: string, type?: 'text'): Promise<string | null>;
	async get<T = unknown>(key: string, type: 'text' | 'json' = 'text'): Promise<T | string | null> {
		this.getCalls += 1;
		const value = this.store.get(key);
		if (value === undefined) {
			return null;
		}
		if (type === 'json') {
			return JSON.parse(value) as T;
		}
		return value;
	}

	async put(key: string, value: string): Promise<void> {
		this.store.set(key, value);
	}

	async delete(key: string): Promise<void> {
		this.store.delete(key);
	}

	async list(options?: { prefix?: string }): Promise<{ keys: Array<{ name: string }> }> {
		const prefix = options?.prefix ?? '';
		const keys = Array.from(this.store.keys())
			.filter((key) => key.startsWith(prefix))
			.map((name) => ({ name }));
		return { keys };
	}
}

class MockNuxtHubKV {
	private readonly store = new Map<string, unknown>();

	async get<T = unknown>(key: string): Promise<T | null> {
		return (this.store.get(key) as T | undefined) ?? null;
	}

	async set(key: string, value: unknown): Promise<void> {
		this.store.set(key, value);
	}

	async del(key: string): Promise<void> {
		this.store.delete(key);
	}

	async keys(prefix: string = ''): Promise<string[]> {
		return Array.from(this.store.keys()).filter((key) => key.startsWith(prefix));
	}
}

class MockUnstorageKV {
	private readonly store = new Map<string, unknown>();

	async getItem<T = unknown>(key: string): Promise<T | null> {
		return (this.store.get(key) as T | undefined) ?? null;
	}

	async setItem(key: string, value: unknown): Promise<void> {
		this.store.set(key, value);
	}

	async removeItem(key: string): Promise<void> {
		this.store.delete(key);
	}

	async getKeys(prefix: string = ''): Promise<string[]> {
		return Array.from(this.store.keys()).filter((key) => key.startsWith(prefix));
	}
}

type MockDrizzleChunk = {
	text: string;
	params: any[];
	append(chunk: MockDrizzleChunk): void;
};

function createMockDrizzleSqlTag() {
	const createChunk = (text: string, params: any[] = []): MockDrizzleChunk => ({
		text,
		params: [...params],
		append(chunk: MockDrizzleChunk) {
			this.text += chunk.text;
			this.params.push(...chunk.params);
		}
	});

	const tag = ((strings: TemplateStringsArray, ...params: any[]) => {
		let text = strings[0] ?? '';
		for (let i = 0; i < params.length; i++) {
			text += `?${strings[i + 1] ?? ''}`;
		}

		return createChunk(text, params);
	}) as {
		(strings: TemplateStringsArray, ...params: any[]): MockDrizzleChunk;
		raw(sql: string): MockDrizzleChunk;
		empty(): MockDrizzleChunk;
	};

	tag.raw = (sql: string) => createChunk(sql, []);
	tag.empty = () => createChunk('', []);

	return tag;
}

function extractPattern(args: any[]): string {
	if (args.length > 0 && typeof args[0] === 'object' && args[0] !== null && 'MATCH' in args[0]) {
		return String((args[0] as { MATCH: string }).MATCH);
	}

	for (let i = 0; i < args.length - 1; i++) {
		if (args[i] === 'MATCH') {
			return String(args[i + 1]);
		}
	}

	return '*';
}

describe('Provider Adapters', () => {
	it('supports Redis KV adapter (tuple scan mode)', async () => {
		const redis = new MockRedisClient(true);
		const kv = createRedisKVProvider(redis as any);

		await kv.put('shard:user:1', JSON.stringify({ shard: 'db-east' }));
		await kv.put('shard:user:2', JSON.stringify({ shard: 'db-west' }));

		const value = await kv.get<{ shard: string }>('shard:user:1', 'json');
		expect(value?.shard).toBe('db-east');

		const listed = await kv.list({ prefix: 'shard:user:' });
		expect(listed.keys.map((k) => k.name).sort()).toEqual(['shard:user:1', 'shard:user:2']);

		await kv.delete('shard:user:1');
		const deleted = await kv.get('shard:user:1');
		expect(deleted).toBeNull();
	});

	it('supports Valkey KV adapter (object scan mode)', async () => {
		const valkey = new MockRedisClient(false);
		const kv = createValkeyKVProvider(valkey as any);

		await kv.put('known_shards', JSON.stringify(['db-east', 'db-west']));
		const shards = await kv.get<string[]>('known_shards', 'json');
		expect(shards).toEqual(['db-east', 'db-west']);
	});

	it('supports PostgreSQL adapter and rewrites ? placeholders', async () => {
		let capturedSql = '';
		let capturedBindings: any[] = [];
		const provider = createPostgreSQLProvider({
			async query<T = Record<string, unknown>>(sql: string, bindings: any[] = []) {
				capturedSql = sql;
				capturedBindings = bindings;
				return {
					rows: [{ id: 'user-1' }] as unknown as T[],
					rowCount: 1,
					command: 'SELECT'
				};
			}
		});

		const result = await provider
			.prepare("SELECT * FROM users WHERE id = ? AND note = '?' AND email = ?")
			.bind('user-1', 'alice@example.com')
			.all<{ id: string }>();

		expect(result.success).toBe(true);
		expect(result.results[0]?.id).toBe('user-1');
		expect(capturedSql).toBe("SELECT * FROM users WHERE id = $1 AND note = '?' AND email = $2");
		expect(capturedBindings).toEqual(['user-1', 'alice@example.com']);
	});

	it('supports MySQL adapter for row and write results', async () => {
		const provider = createMySQLProvider({
			async execute(sql: string, bindings: any[] = []) {
				if (sql.startsWith('SELECT')) {
					return [[{ id: bindings[0], name: 'Alice' }], []];
				}
				return [{ affectedRows: 1, insertId: 42 }, []];
			}
		});

		const read = await provider.prepare('SELECT * FROM users WHERE id = ?').bind('user-2').all<{ id: string; name: string }>();
		expect(read.results[0]?.id).toBe('user-2');
		expect(read.results[0]?.name).toBe('Alice');

		const write = await provider.prepare('INSERT INTO users (name) VALUES (?)').bind('Bob').run();
		expect(write.meta.changes).toBe(1);
		expect(write.meta.last_row_id).toBe(42);
	});

	it('supports SQLite prepare/get/all style adapter', async () => {
		const provider = createSQLiteProvider({
			prepare(sql: string) {
				return {
					all: (...bindings: any[]) => [{ sql, bindings, id: 'row-1' }],
					get: (...bindings: any[]) => ({ sql, bindings, id: 'row-1' }),
					run: (...bindings: any[]) => ({ changes: 1, lastInsertRowid: 7, sql, bindings })
				};
			}
		});

		const allRows = await provider.prepare('SELECT * FROM users WHERE id = ?').bind('row-1').all<{ id: string }>();
		expect(allRows.results[0]?.id).toBe('row-1');

		const firstRow = await provider.prepare('SELECT * FROM users WHERE id = ?').bind('row-1').first<{ id: string }>();
		expect(firstRow?.id).toBe('row-1');

		const write = await provider.prepare('INSERT INTO users (name) VALUES (?)').bind('Alice').run();
		expect(write.meta.changes).toBe(1);
		expect(write.meta.last_row_id).toBe(7);
	});

	it('supports Hyperdrive postgres helper', async () => {
		const lifecycle: string[] = [];
		const provider = createHyperdrivePostgresProvider({ connectionString: 'postgres://hyperdrive-host/db' }, (connectionString) => ({
			async connect() {
				lifecycle.push(`connect:${connectionString}`);
			},
			async query<T = Record<string, unknown>>() {
				lifecycle.push('query');
				return { rows: [{ id: 1 }] as unknown as T[], rowCount: 1 };
			},
			async end() {
				lifecycle.push('end');
			}
		}));

		const result = await provider.prepare('SELECT 1').all<{ id: number }>();
		expect(result.results[0]?.id).toBe(1);
		expect(lifecycle).toEqual(['connect:postgres://hyperdrive-host/db', 'query']);

		await provider.dispose();
		expect(lifecycle).toEqual(['connect:postgres://hyperdrive-host/db', 'query', 'end']);
	});

	it('supports Hyperdrive mysql helper', async () => {
		let capturedConnectionString = '';
		const provider = createHyperdriveMySQLProvider({ connectionString: 'mysql://hyperdrive-host/db' }, (connectionString) => {
			capturedConnectionString = connectionString;
			return {
				async execute() {
					return [[{ ok: true }], []];
				},
				async end() {
					return;
				}
			};
		});

		const result = await provider.prepare('SELECT 1').all<{ ok: boolean }>();
		expect(result.results[0]?.ok).toBe(true);
		expect(capturedConnectionString).toBe('mysql://hyperdrive-host/db');
	});

	it('supports Drizzle interop via createPostgreSQLProvider', async () => {
		const sqlTag = createMockDrizzleSqlTag();
		let capturedQuery: MockDrizzleChunk | undefined;

		const provider = createPostgreSQLProvider(
			{
				async execute(query: unknown) {
					capturedQuery = query as MockDrizzleChunk;
					return {
						rows: [{ id: 'user-1', email: 'alice@example.com' }],
						rowCount: 1
					};
				}
			},
			sqlTag
		);

		const result = await provider
			.prepare("SELECT * FROM users WHERE id = ? AND note = '?' AND email = ?")
			.bind('user-1', 'alice@example.com')
			.all<{ id: string; email: string }>();

		expect(result.results[0]?.id).toBe('user-1');
		expect(result.results[0]?.email).toBe('alice@example.com');
		expect(capturedQuery?.text).toBe("SELECT * FROM users WHERE id = ? AND note = '?' AND email = ?");
		expect(capturedQuery?.params).toEqual(['user-1', 'alice@example.com']);
	});

	it('supports Drizzle interop via createSQLiteProvider with real drizzle-orm sql tag', async () => {
		let capturedQuery: unknown;
		const provider = createSQLiteProvider(
			{
				async execute(query: unknown) {
					capturedQuery = query;
					return {
						rows: [{ id: 'drizzle-user-1' }],
						rowCount: 1
					};
				}
			},
			drizzleSql as any
		);

		const row = await provider.prepare('SELECT ? AS id').bind('drizzle-user-1').first<{ id: string }>();
		expect(row?.id).toBe('drizzle-user-1');
		expect(capturedQuery).toBeTruthy();
	});

	it('supports Drizzle first() reading a plain row via the normalized all() path', async () => {
		const sqlTag = createMockDrizzleSqlTag();

		const provider = createSQLiteProvider(
			{
				// get() is intentionally wrong; first() must read through the normalized all() path
				async get() {
					return null;
				},
				async all() {
					return [{ id: 'plain-row-1', email: 'plain@example.com' }];
				}
			},
			sqlTag
		);

		const row = await provider.prepare('SELECT id, email FROM users WHERE id = ?').bind('plain-row-1').first<{
			id: string;
			email: string;
		}>();

		expect(row?.id).toBe('plain-row-1');
		expect(row?.email).toBe('plain@example.com');
	});

	it('supports Drizzle first() selecting a blob column named "data" (regression: get() drops it)', async () => {
		const sqlTag = createMockDrizzleSqlTag();
		const blob = new Uint8Array([0, 1, 2, 250, 251, 252]);

		const provider = createSQLiteProvider(
			{
				async get() {
					return { id: 'blob-user-1', data: blob };
				},
				async all() {
					return [{ id: 'blob-user-1', data: blob }];
				}
			},
			sqlTag
		);

		const row = await provider.prepare('SELECT id, data FROM users WHERE id = ?').bind('blob-user-1').first<{
			id: string;
			data: Uint8Array;
		}>();

		expect(row).not.toBeNull();
		expect(row?.id).toBe('blob-user-1');
		expect(row?.data).toEqual(blob);
	});

	it('supports Drizzle interop via createMySQLProvider', async () => {
		const sqlTag = createMockDrizzleSqlTag();

		const provider = createMySQLProvider(
			{
				async run(query: unknown) {
					const sqlChunk = query as MockDrizzleChunk;
					if (sqlChunk.text.startsWith('INSERT')) {
						return { changes: 1, lastInsertId: 88 };
					}
					return { rows: [{ ok: true }] };
				}
			},
			sqlTag
		);

		const write = await provider.prepare('INSERT INTO users (id) VALUES (?)').bind('u-1').run();
		expect(write.meta.changes).toBe(1);
		expect(write.meta.last_row_id).toBe(88);
	});

	it('supports Drizzle all() when mysql driver returns [rows, fields] tuple', async () => {
		const sqlTag = createMockDrizzleSqlTag();

		const provider = createMySQLProvider(
			{
				async all() {
					return [[{ count: 3 }], [{ name: 'count' }]];
				}
			},
			sqlTag
		);

		const result = await provider.prepare('SELECT COUNT(*) AS count FROM users').all<{ count: number }>();
		expect(result.results).toEqual([{ count: 3 }]);
	});

	it('supports NuxtHub KV adapter (@nuxthub/kv style)', async () => {
		const nuxtHubKV = new MockNuxtHubKV();
		const kv = createNuxtHubKVProvider(nuxtHubKV as any);

		await kv.put('shard:user:1', JSON.stringify({ shard: 'db-east' }));
		const mapping = await kv.get<{ shard: string }>('shard:user:1', 'json');
		expect(mapping?.shard).toBe('db-east');

		await kv.put('shard:user:2', JSON.stringify({ shard: 'db-west' }));
		const listed = await kv.list({ prefix: 'shard:user:' });
		expect(listed.keys.map((k) => k.name).sort()).toEqual(['shard:user:1', 'shard:user:2']);

		await kv.delete('shard:user:1');
		expect(await kv.get('shard:user:1')).toBeNull();
	});

	it('supports NuxtHub KV adapter (unstorage style)', async () => {
		const storage = new MockUnstorageKV();
		const kv = createNuxtHubKVProvider(storage as any);

		await kv.put('known_shards', JSON.stringify(['db-east', 'db-west']));
		const shards = await kv.get<string[]>('known_shards', 'json');
		expect(shards).toEqual(['db-east', 'db-west']);
	});
});

describe('KVShardMapper Caching', () => {
	it('reuses cached mapping after initial lookup', async () => {
		const kv = new InstrumentedKVStorage();
		const writer = new KVShardMapper(kv, { hashShardMappings: true, mappingCacheTtlMs: 60_000 });
		await writer.setShardMapping('user-1', 'db-east');

		const reader = new KVShardMapper(kv, { hashShardMappings: true, mappingCacheTtlMs: 60_000 });
		const first = await reader.getShardMapping('user-1');
		const callsAfterFirstLookup = kv.getCalls;
		const second = await reader.getShardMapping('user-1');

		expect(first?.shard).toBe('db-east');
		expect(second?.shard).toBe('db-east');
		expect(callsAfterFirstLookup).toBeGreaterThan(0);
		expect(kv.getCalls).toBe(callsAfterFirstLookup);
	});

	it('reuses cached known shards list', async () => {
		const kv = new InstrumentedKVStorage();
		const mapper = new KVShardMapper(kv, {
			hashShardMappings: true,
			knownShardsCacheTtlMs: 60_000
		});

		await mapper.setKnownShards(['db-east', 'db-west']);
		const beforeReads = kv.getCalls;
		const first = await mapper.getKnownShards();
		const afterFirstRead = kv.getCalls;
		const second = await mapper.getKnownShards();

		expect(first).toEqual(['db-east', 'db-west']);
		expect(second).toEqual(['db-east', 'db-west']);
		expect(afterFirstRead - beforeReads).toBeLessThanOrEqual(1);
		expect(kv.getCalls).toBe(afterFirstRead);
	});
});

describe('Hyperdrive Postgres helper', () => {
	it('falls back to release() when end() is unavailable', async () => {
		const lifecycle: string[] = [];
		const provider = createHyperdrivePostgresProvider({ connectionString: 'postgres://example' }, () => ({
			async query() {
				lifecycle.push('query');
				return { rows: [], rowCount: 0 };
			},
			release() {
				lifecycle.push('release');
			}
		}));

		await provider.prepare('SELECT 1').run();
		expect(lifecycle).toEqual(['query']);

		await provider.dispose();
		expect(lifecycle).toEqual(['query', 'release']);
	});

	it('reuses one client and one connect across statements in a request', async () => {
		const lifecycle: string[] = [];
		let created = 0;

		const provider = createHyperdrivePostgresProvider({ connectionString: 'postgres://example' }, () => {
			created++;
			return {
				async connect() {
					lifecycle.push('connect');
				},
				async query() {
					lifecycle.push('query');
					return { rows: [], rowCount: 0 };
				},
				async end() {
					lifecycle.push('end');
				}
			};
		});

		await provider.prepare('SELECT 1').run();
		await provider.prepare('SELECT 2').run();
		await provider.prepare('SELECT 3').run();

		expect(created).toBe(1);
		expect(lifecycle).toEqual(['connect', 'query', 'query', 'query']);

		await provider.dispose();
		expect(lifecycle.at(-1)).toBe('end');
	});

	it('gives each request scope its own client', async () => {
		let created = 0;
		let scope = { id: 'request-1' };

		const provider = createHyperdrivePostgresProvider(
			{ connectionString: 'postgres://example' },
			() => {
				created++;
				return {
					async query() {
						return { rows: [], rowCount: 0 };
					},
					async end() {}
				};
			},
			{ scope: () => scope }
		);

		await provider.prepare('SELECT 1').run();
		await provider.prepare('SELECT 2').run();
		expect(created).toBe(1);

		scope = { id: 'request-2' };
		await provider.prepare('SELECT 3').run();
		expect(created).toBe(2);
	});

	it('closes each interleaved request its own connection and no other', async () => {
		// The sandbox Worker set the scope from a mutable module global, so a
		// second request arriving mid-flight repointed it and dispose() closed the
		// client the first request was still querying. Every connection then either
		// leaked or died under a live statement, which exhausted Postgres and hung
		// the lane instead of failing it. A per-request closure is the contract;
		// this holds two scopes open at once to prove it.
		const ended: string[] = [];
		const clientsByScope = new Map<string, string>();

		function providerFor(token: object, label: string) {
			return createHyperdrivePostgresProvider(
				{ connectionString: 'postgres://example' },
				() => {
					clientsByScope.set(label, label);
					return {
						async query() {
							return { rows: [], rowCount: 0 };
						},
						async end() {
							ended.push(label);
						}
					};
				},
				{ scope: () => token }
			);
		}

		const requestOne = { id: 1 };
		const requestTwo = { id: 2 };
		const first = providerFor(requestOne, 'request-1');
		const second = providerFor(requestTwo, 'request-2');

		await first.prepare('SELECT 1').run();
		await second.prepare('SELECT 2').run();
		// Interleaved: request one is still running after request two started.
		await first.prepare('SELECT 3').run();

		expect(clientsByScope.size).toBe(2);
		expect(ended).toEqual([]);

		await second.dispose();
		expect(ended).toEqual(['request-2']);

		// Request one's client survived its neighbour's disposal, so its in-flight
		// statement above could not have been cut off.
		await first.prepare('SELECT 4').run();
		await first.dispose();
		expect(ended).toEqual(['request-2', 'request-1']);
	});

	it('releases the client for the scope that is current when dispose is called', async () => {
		let created = 0;
		const ended: number[] = [];
		let scope = { id: 'request-1' };

		const provider = createHyperdrivePostgresProvider(
			{ connectionString: 'postgres://example' },
			() => {
				const id = ++created;
				return {
					async query() {
						return { rows: [], rowCount: 0 };
					},
					async end() {
						ended.push(id);
					}
				};
			},
			{ scope: () => scope }
		);

		await provider.prepare('SELECT 1').run();
		scope = { id: 'request-2' };
		await provider.prepare('SELECT 2').run();
		expect(created).toBe(2);

		// Disposing now targets the current scope only. The first client is left to
		// garbage collection with the token it was keyed on, which is why the scope
		// has to be a per-request closure rather than a value something else moves.
		await provider.dispose();
		expect(ended).toEqual([2]);
	});

	it('skips connect() when not provided', async () => {
		const lifecycle: string[] = [];
		const provider = createHyperdrivePostgresProvider({ connectionString: 'postgres://example' }, () => ({
			async query<T = Record<string, unknown>>() {
				lifecycle.push('query');
				return { rows: [{ id: 1 }] as unknown as T[], rowCount: 1 };
			}
		}));

		const result = await provider.prepare('SELECT 1').all();
		expect(result.results).toHaveLength(1);
		expect(lifecycle).toEqual(['query']);
	});
});

describe('Hyperdrive MySQL helper', () => {
	it('falls back to query() when execute is missing, then close()', async () => {
		const lifecycle: string[] = [];
		const provider = createHyperdriveMySQLProvider({ connectionString: 'mysql://example' }, () => ({
			async query() {
				lifecycle.push('query');
				return [[{ ok: 1 }], []];
			},
			close() {
				lifecycle.push('close');
			}
		}));

		const result = await provider.prepare('SELECT 1').all();
		expect(result.results).toEqual([{ ok: 1 }]);
		expect(lifecycle).toEqual(['query']);

		await provider.dispose();
		expect(lifecycle).toEqual(['query', 'close']);
	});

	it('falls back to destroy() when end+close are missing', async () => {
		const lifecycle: string[] = [];
		const provider = createHyperdriveMySQLProvider({ connectionString: 'mysql://example' }, () => ({
			async execute() {
				lifecycle.push('execute');
				return [[{ ok: true }], []];
			},
			destroy() {
				lifecycle.push('destroy');
			}
		}));

		await provider.prepare('SELECT 1').first();
		expect(lifecycle).toEqual(['execute']);

		await provider.dispose();
		expect(lifecycle).toEqual(['execute', 'destroy']);
	});

	it('reuses one client across statements in a request', async () => {
		let created = 0;
		const provider = createHyperdriveMySQLProvider({ connectionString: 'mysql://example' }, () => {
			created++;
			return {
				async execute() {
					return [[{ ok: true }], []];
				},
				async end() {}
			};
		});

		await provider.prepare('SELECT 1').first();
		await provider.prepare('SELECT 2').first();
		expect(created).toBe(1);
	});

	it('throws when the underlying client exposes nothing useful', async () => {
		const provider = createHyperdriveMySQLProvider({ connectionString: 'mysql://example' }, () => ({}));
		await expect(provider.prepare('SELECT 1').run()).rejects.toThrow(/missing execute\/query/);
	});
});

describe('MySQL adapter packet handling', () => {
	it('exposes affectedRows on all() when execute returns an OK packet', async () => {
		const provider = createMySQLProvider({
			async execute() {
				return { affectedRows: 3, warningStatus: 0 } as any;
			}
		});

		const result = await provider.prepare('DELETE FROM t WHERE x = ?').bind(1).all();
		expect(result.meta.changes).toBe(3);
		expect(result.results).toEqual([]);
	});

	it('returns null on first() when no rows are produced', async () => {
		const provider = createMySQLProvider({
			async execute() {
				return [[], []] as any;
			}
		});

		const result = await provider.prepare('SELECT 1').first();
		expect(result).toBeNull();
	});

	it('throws when the MySQL client lacks execute and query', async () => {
		const provider = createMySQLProvider({} as any);
		await expect(provider.prepare('SELECT 1').run()).rejects.toThrow(/execute\(\) or query\(\)/);
	});
});

describe('SQLite adapter validation', () => {
	it('throws when neither execute nor prepare.run is exposed', async () => {
		const provider = createSQLiteProvider({
			prepare() {
				return {};
			}
		});

		await expect(provider.prepare('SELECT 1').run()).rejects.toMatchObject({ code: 'SQLITE_CLIENT_INVALID' });
		await expect(provider.prepare('SELECT 1').all()).rejects.toMatchObject({ code: 'SQLITE_CLIENT_INVALID' });
	});

	it('reads rows via the execute()-style adapter shape', async () => {
		const provider = createSQLiteProvider({
			async execute(sql: string) {
				return { rows: [{ id: 'x', sql }] };
			}
		});

		const all = await provider.prepare('SELECT *').all<{ id: string }>();
		expect(all.results[0]?.id).toBe('x');

		const first = await provider.prepare('SELECT *').first<{ id: string }>();
		expect(first?.id).toBe('x');
	});

	it('first() falls back to prepare().all() when get() is missing', async () => {
		const provider = createSQLiteProvider({
			prepare() {
				return {
					all() {
						return [{ id: 'fallback' }];
					}
				};
			}
		});

		const first = await provider.prepare('SELECT id').first<{ id: string }>();
		expect(first).toEqual({ id: 'fallback' });
	});
});

describe('Drizzle adapter execute/run/all fallbacks', () => {
	const sqlTag = ((strings: TemplateStringsArray, ...params: any[]) => ({
		text: strings.join('?'),
		params,
		append() {}
	})) as any;
	sqlTag.raw = (sql: string) => ({ text: sql, params: [], append() {} });
	sqlTag.empty = () => ({ text: '', params: [], append() {} });

	it('uses all() when run() is missing on the Drizzle client', async () => {
		const calls: string[] = [];
		const provider = createDrizzleSQLProvider(
			{
				async all() {
					calls.push('all');
					return { rows: [{ ok: true }] };
				}
			},
			sqlTag
		);

		const result = await provider.prepare('UPDATE t SET x = ? WHERE id = ?').bind(1, 2).run();
		expect(result.success).toBe(true);
		expect(calls).toEqual(['all']);
	});

	it('uses execute() for all() when all is missing', async () => {
		const provider = createDrizzleSQLProvider(
			{
				async execute() {
					return [[{ id: 1 }], []];
				}
			},
			sqlTag
		);

		const result = await provider.prepare('SELECT * FROM t').all();
		expect(result.results).toEqual([{ id: 1 }]);
	});

	it('throws when Drizzle binding count mismatches placeholders', () => {
		const provider = createDrizzleSQLProvider(
			{
				async execute() {
					return { rows: [] };
				}
			},
			sqlTag
		);

		const statement = provider.prepare('SELECT ? FROM t WHERE x = ?').bind('only-one');
		return expect(statement.all()).rejects.toThrow(/binding mismatch/);
	});
});

describe('Redis adapter scan paging', () => {
	it('iterates through multiple cursor pages', async () => {
		const store = new Map<string, string>();
		for (let i = 0; i < 5; i++) {
			store.set(`shard:${i}`, JSON.stringify({ shard: 'east' }));
		}

		let callIndex = 0;
		const redis = {
			async get(key: string) {
				return store.get(key) ?? null;
			},
			async set(key: string, value: string) {
				store.set(key, value);
			},
			async del(key: string) {
				store.delete(key);
			},
			async scan() {
				const allKeys = Array.from(store.keys()).filter((k) => k.startsWith('shard:'));
				if (callIndex === 0) {
					callIndex++;
					return ['1', allKeys.slice(0, 3)];
				}
				return ['0', allKeys.slice(3)];
			}
		};

		const kv = createValkeyKVProvider(redis as any);
		const listed = await kv.list({ prefix: 'shard:' });
		expect(listed.keys).toHaveLength(5);
	});

	it('honors the limit parameter when listing', async () => {
		const store = new Map<string, string>();
		for (let i = 0; i < 4; i++) {
			store.set(`shard:${i}`, '1');
		}
		const redis = {
			async get(key: string) {
				return store.get(key) ?? null;
			},
			async set() {},
			async del() {},
			async scan() {
				return ['0', Array.from(store.keys())];
			}
		};

		const kv = createRedisKVProvider(redis as any);
		const limited = await kv.list({ prefix: 'shard:', limit: 2 });
		expect(limited.keys).toHaveLength(2);
	});
});

describe('NuxtHub adapter edge cases', () => {
	it('falls back to setItem/removeItem when set/del are missing', async () => {
		const store = new Map<string, unknown>();
		const kv = createNuxtHubKVProvider({
			async setItem(key: string, value: unknown) {
				store.set(key, value);
			},
			async removeItem(key: string) {
				store.delete(key);
			},
			async getItem(key: string) {
				return store.get(key) ?? null;
			},
			async getKeys(prefix: string = '') {
				return Array.from(store.keys()).filter((k) => k.startsWith(prefix));
			}
		} as any);

		await kv.put('hello', 'world');
		expect(await kv.get('hello')).toBe('world');
		await kv.delete('hello');
		expect(await kv.get('hello')).toBeNull();
	});

	it('NuxtHub KV adapter throws when client is missing get methods', async () => {
		const kv = createNuxtHubKVProvider({} as any);
		await expect(kv.get('a')).rejects.toThrow(/get\(\) or getItem\(\)/);
		await expect(kv.put('a', 'b')).rejects.toThrow(/set\(\) or setItem\(\)/);
		await expect(kv.delete('a')).rejects.toThrow(/del\(\) or removeItem\(\)/);
		await expect(kv.list({ prefix: 'a' })).rejects.toThrow(/keys\(\) or getKeys\(\)/);
	});

	it('NuxtHub KV adapter falls back to JSON.stringify for object values', async () => {
		const kv = createNuxtHubKVProvider({
			async get() {
				return { hello: 'world' };
			},
			async set() {}
		} as any);
		const value = await kv.get('a');
		expect(value).toBe(JSON.stringify({ hello: 'world' }));
	});
});

describe('Postgres adapter edge cases', () => {
	it('Postgres adapter surfaces JSON parse errors via KV adapter', async () => {
		const redis = {
			async get() {
				return 'not-json';
			},
			async set() {},
			async del() {},
			async scan() {
				return ['0', []];
			}
		};
		const kv = createRedisKVProvider(redis as any);
		await expect(kv.get('a', 'json')).rejects.toMatchObject({ code: 'KV_JSON_PARSE_FAILED' });
	});

	it('Postgres adapter constructor accepts Drizzle interop tag', () => {
		const adapter = createPostgreSQLProvider(
			{
				async execute() {
					return { rows: [], rowCount: 0 };
				}
			},
			((strings: TemplateStringsArray) => ({
				text: strings[0],
				params: [],
				append() {}
			})) as any
		);
		expect(typeof adapter.prepare).toBe('function');
	});
});

describe('Type guards', () => {
	it('isSQLDatabase / isKVStorage detect implementations', () => {
		expect(isSQLDatabase({ prepare: () => null })).toBe(true);
		expect(isSQLDatabase(null)).toBe(false);
		expect(isSQLDatabase({})).toBe(false);

		const kvLike = { get: () => null, put: () => undefined, delete: () => undefined, list: () => ({ keys: [] }) };
		expect(isKVStorage(kvLike)).toBe(true);
		expect(isKVStorage(null)).toBe(false);
		expect(isKVStorage({ get: () => null })).toBe(false);
	});
});

describe('Postgres placeholder rewriting', () => {
	it('caches rewritten SQL between invocations', async () => {
		let calls = 0;
		const provider = createPostgreSQLProvider({
			async query<T = Record<string, unknown>>() {
				calls++;
				return { rows: [{ ok: true }] as unknown as T[], rowCount: 1 };
			}
		});

		await provider.prepare('SELECT * FROM users WHERE id = ?').bind(1).run();
		await provider.prepare('SELECT * FROM users WHERE id = ?').bind(2).run();
		expect(calls).toBe(2);
	});

	it('handles -- line comments and /* block comments */ when rewriting', async () => {
		let captured = '';
		const provider = createPostgreSQLProvider({
			async query(sql: string) {
				captured = sql;
				return { rows: [], rowCount: 0 };
			}
		});

		await provider.prepare("-- ignore this ?\n/* and this ? too */ SELECT * FROM t WHERE id = ? AND label = '?'").bind('only-one').all();

		expect(captured).toContain('$1');
		expect(captured).toContain("'?'");
	});
});

describe('toProvider', () => {
	it('returns an SQLDatabase-shaped binding (e.g. D1) unchanged', () => {
		const d1 = { prepare: () => ({ bind: () => ({}), run: async () => ({}), all: async () => ({}), first: async () => null }) };
		expect(toProvider(d1)).toBe(d1);
	});

	it('wraps a Drizzle-style client when a sql tag is supplied', () => {
		const drizzleClient = {
			run: async () => ({ rows: [] }),
			all: async () => ({ rows: [] }),
			get: async () => null
		};
		const provider = toProvider(drizzleClient, { sql: drizzleSql });
		expect(provider).not.toBeNull();
		expect(provider).not.toBe(drizzleClient);
		expect(typeof provider?.prepare).toBe('function');
	});

	it('wraps a raw sqlite client exposing prepare via createSQLiteProvider', () => {
		const sqliteClient = {
			prepare: (_sql: string) => ({
				run: () => ({ success: true }),
				all: () => [],
				get: () => undefined
			})
		};
		// Has prepare, so isSQLDatabase returns it as-is (D1-compatible contract).
		const provider = toProvider(sqliteClient);
		expect(provider).not.toBeNull();
		expect(typeof provider?.prepare).toBe('function');
	});

	it('returns null for a Drizzle client without a sql tag', () => {
		const drizzleClient = { run: async () => ({}), all: async () => ({}), get: async () => null };
		expect(toProvider(drizzleClient)).toBeNull();
	});

	it('returns null for unrecognized values', () => {
		expect(toProvider(null)).toBeNull();
		expect(toProvider(undefined)).toBeNull();
		expect(toProvider(42)).toBeNull();
		expect(toProvider({})).toBeNull();
	});
});

describe('createWorkersKVProvider', () => {
	function createWorkersKVMock(): WorkersKVNamespaceLike {
		const store = new Map<string, string>();
		return {
			async get(key: string) {
				return store.has(key) ? store.get(key)! : null;
			},
			async put(key: string, value: string) {
				store.set(key, value);
			},
			async delete(key: string) {
				store.delete(key);
			},
			async list(options?: { prefix?: string; cursor?: string; limit?: number }) {
				const prefix = options?.prefix ?? '';
				const keys = Array.from(store.keys())
					.filter((name) => name.startsWith(prefix))
					.map((name) => ({ name }));
				return { keys, list_complete: true };
			}
		};
	}

	it('adapts a raw Workers KV namespace to the KVStorage contract', async () => {
		const provider = createWorkersKVProvider(createWorkersKVMock());
		expect(isKVStorage(provider)).toBe(true);

		await provider.put('a', 'hello');
		expect(await provider.get('a', 'text')).toBe('hello');

		await provider.put('j', JSON.stringify({ n: 5 }));
		expect(await provider.get<{ n: number }>('j', 'json')).toEqual({ n: 5 });

		await provider.delete('a');
		expect(await provider.get('a', 'text')).toBeNull();
	});

	it('normalizes list results and preserves list_complete', async () => {
		const provider = createWorkersKVProvider(createWorkersKVMock());
		await provider.put('user:1', '1');
		await provider.put('user:2', '2');
		await provider.put('post:1', '3');

		const result = await provider.list({ prefix: 'user:' });
		expect(result.keys.map((k) => k.name).sort()).toEqual(['user:1', 'user:2']);
		expect(result.list_complete).toBe(true);
	});

	it('throws a typed error on malformed JSON', async () => {
		const provider = createWorkersKVProvider(createWorkersKVMock());
		await provider.put('bad', 'not-json{');
		await expect(provider.get('bad', 'json')).rejects.toThrow('Failed to parse JSON from Workers KV');
	});
});

describe('Transactional batches', () => {
	/** Records every statement in order, so BEGIN/COMMIT placement is visible. */
	function recorder() {
		const statements: string[] = [];
		return {
			statements,
			query: async (sql: string, _bindings?: any[]) => {
				statements.push(sql);
				if (sql === 'BOOM') {
					throw new Error('statement failed');
				}
				return { rows: [] as any[], rowCount: 0 };
			}
		};
	}

	it('leases a connection from a pg pool and returns it', async () => {
		const leased = recorder();
		let released = 0;
		let connects = 0;

		// `pg.Pool` is recognized by its counters; `Client` has `connect` too but
		// resolves it to nothing, so the counters are what tell them apart.
		const pool = {
			totalCount: 1,
			idleCount: 1,
			async query() {
				throw new Error('the batch must not run on the pool itself');
			},
			async connect() {
				connects++;
				return { query: leased.query, release: () => released++ };
			}
		};

		const provider = createPostgreSQLProvider(pool as any);
		expect(provider.runBatch).toBeDefined();

		const results = await provider.runBatch!([
			{ sql: 'INSERT INTO users (id) VALUES (?)', bindings: ['a'] },
			{ sql: 'INSERT INTO users (id) VALUES (?)', bindings: ['b'] }
		]);

		expect(results).toHaveLength(2);
		expect(connects).toBe(1);
		expect(released).toBe(1);
		expect(leased.statements[0]).toBe('BEGIN');
		expect(leased.statements.at(-1)).toBe('COMMIT');
		expect(leased.statements.filter((sql) => sql.startsWith('INSERT'))).toHaveLength(2);
	});

	it('rolls back and releases when a statement in the group fails', async () => {
		const leased = recorder();
		let released = 0;

		const pool = {
			totalCount: 1,
			idleCount: 1,
			async connect() {
				return { query: leased.query, release: () => released++ };
			}
		};

		const provider = createPostgreSQLProvider(pool as any);

		await expect(provider.runBatch!([{ sql: 'INSERT INTO users (id) VALUES (?)', bindings: ['a'] }, { sql: 'BOOM' }])).rejects.toThrow(
			'statement failed'
		);

		expect(leased.statements).toContain('ROLLBACK');
		expect(leased.statements).not.toContain('COMMIT');
		// A leaked connection is worse than a failed batch.
		expect(released).toBe(1);
	});

	it('leases from a mysql2 pool through getConnection', async () => {
		const leased = recorder();
		let released = 0;

		const pool = {
			async getConnection() {
				return {
					execute: async (sql: string) => {
						await leased.query(sql);
						return [[], []];
					},
					release: () => released++
				};
			}
		};

		const provider = createMySQLProvider(pool as any);
		await provider.runBatch!([{ sql: 'INSERT INTO users (id) VALUES (?)', bindings: ['a'] }]);

		expect(leased.statements[0]).toBe('BEGIN');
		expect(leased.statements.at(-1)).toBe('COMMIT');
		expect(released).toBe(1);
	});

	it('sends mysql transaction control through query, not the prepared protocol', async () => {
		const executed: string[] = [];
		const queried: string[] = [];

		// This is what mysql2 actually does: execute() is the prepared-statement
		// protocol and answers BEGIN with "This command is not supported in the
		// prepared statement protocol yet". The sandbox lane found it; a mock that
		// accepts everything on execute() did not.
		const pool = {
			async getConnection() {
				return {
					execute: async (sql: string) => {
						if (/^(BEGIN|COMMIT|ROLLBACK)$/i.test(sql)) {
							throw new Error('This command is not supported in the prepared statement protocol yet');
						}
						executed.push(sql);
						return [{ affectedRows: 1 }, []];
					},
					query: async (sql: string) => {
						queried.push(sql);
						return [{ affectedRows: 0 }, []];
					},
					release: () => {}
				};
			}
		};

		const provider = createMySQLProvider(pool as any);
		const results = await provider.runBatch!([
			{ sql: 'INSERT INTO users (id) VALUES (?)', bindings: ['a'] },
			{ sql: 'INSERT INTO users (id) VALUES (?)', bindings: ['b'] }
		]);

		expect(results).toHaveLength(2);
		expect(queried).toEqual(['BEGIN', 'COMMIT']);
		expect(executed).toHaveLength(2);
	});

	it('refuses to batch a handle it cannot prove is a single connection', async () => {
		// Only `query`, which is all a pool wrapped in a thin adapter exposes.
		// Guessing here would send BEGIN down a different connection than the
		// statements, so the adapter declines instead.
		const provider = createPostgreSQLProvider({ query: async () => ({ rows: [], rowCount: 0 }) });
		expect(provider.runBatch).toBeUndefined();
	});

	it('batches through a handle the caller declares single', async () => {
		const client = recorder();
		const provider = createPostgreSQLProvider(client as any, { singleConnection: true });

		expect(provider.runBatch).toBeDefined();
		await provider.runBatch!([{ sql: 'INSERT INTO users (id) VALUES (?)', bindings: ['a'] }]);

		expect(client.statements).toEqual(['BEGIN', 'INSERT INTO users (id) VALUES ($1)', 'COMMIT']);
	});

	it('takes a caller-supplied lease over autodetection', async () => {
		const leased = recorder();
		let released = 0;

		// The handle looks like a pg pool, but an explicit lease wins so a caller
		// with its own pooling can always override what autodetection decided.
		const pool = {
			totalCount: 1,
			idleCount: 1,
			async connect() {
				throw new Error('autodetected lease must not be used');
			}
		};

		const provider = createPostgreSQLProvider(pool as any, {
			lease: async () => ({
				client: { query: leased.query },
				release: () => {
					released++;
				}
			})
		});

		await provider.runBatch!([{ sql: 'SELECT 1' }]);
		expect(leased.statements).toEqual(['BEGIN', 'SELECT 1', 'COMMIT']);
		expect(released).toBe(1);
	});

	it('batches a sqlite handle when told it is one connection', async () => {
		const statements: string[] = [];
		const db = {
			prepare(sql: string) {
				statements.push(sql);
				return { run: () => ({ changes: 1, lastInsertRowid: 1 }), all: () => [], get: () => null };
			}
		};

		const provider = createSQLiteProvider(db as any, { singleConnection: true });
		await provider.runBatch!([{ sql: 'INSERT INTO users (id) VALUES (?)', bindings: ['a'] }]);

		expect(statements[0]).toBe('BEGIN');
		expect(statements.at(-1)).toBe('COMMIT');
	});

	it('returns an empty result set without opening a transaction', async () => {
		const client = recorder();
		const provider = createPostgreSQLProvider(client as any, { singleConnection: true });

		expect(await provider.runBatch!([])).toEqual([]);
		expect(client.statements).toEqual([]);
	});

	it('keeps the drizzle overload working', async () => {
		const provider = createPostgreSQLProvider({ execute: async () => ({ rows: [] }) } as any, drizzleSql);
		expect(provider.dialect).toBe('postgres');
	});
});

describe('Drizzle batches', () => {
	function drizzleClient(overrides: Record<string, unknown> = {}) {
		const ran: string[] = [];
		const client: any = {
			async run(query: any) {
				ran.push(String(query?.text ?? query?.sql ?? query));
				return { rows: [], rowsAffected: 1 };
			},
			async all() {
				return { rows: [] };
			},
			...overrides
		};
		return { client, ran };
	}

	it('wraps a group in drizzle transaction when the driver has one', async () => {
		const inner = drizzleClient();
		let transactions = 0;

		inner.client.transaction = async (callback: (tx: unknown) => Promise<unknown>) => {
			transactions++;
			return await callback(inner.client);
		};

		const provider = createDrizzleSQLProvider(inner.client, drizzleSql, 'postgres');
		expect(provider.runBatch).toBeDefined();

		const results = await provider.runBatch!([{ sql: 'INSERT INTO users (id) VALUES (?)', bindings: ['a'] }, { sql: 'SELECT 1' }]);

		expect(results).toHaveLength(2);
		expect(transactions).toBe(1);
		// Drizzle issues its own BEGIN and COMMIT, so the group carries neither.
		expect(inner.ran.some((sql) => /begin|commit/i.test(sql))).toBe(false);
	});

	it('leaves a native-batch driver on the sequential path', async () => {
		const inner = drizzleClient({
			batch: async () => [],
			transaction: async (callback: (tx: unknown) => Promise<unknown>) => await callback(null)
		});

		// D1 through Drizzle: `transaction()` is present but a real D1 answers the
		// `begin` it emits with `Failed query: begin`, which the Cloudflare lane
		// reported before this guard existed.
		const provider = createDrizzleSQLProvider(inner.client, drizzleSql, 'sqlite');
		expect(provider.runBatch).toBeUndefined();
	});

	it('returns an empty result without opening a transaction', async () => {
		const inner = drizzleClient();
		let transactions = 0;
		inner.client.transaction = async (callback: (tx: unknown) => Promise<unknown>) => {
			transactions++;
			return await callback(inner.client);
		};

		const provider = createDrizzleSQLProvider(inner.client, drizzleSql, 'mysql');
		expect(await provider.runBatch!([])).toEqual([]);
		expect(transactions).toBe(0);
	});
});

describe('atomicBatch capability', () => {
	function recorder() {
		const statements: string[] = [];
		return {
			statements,
			query: async (sql: string, _bindings?: any[]) => {
				statements.push(sql);
				if (sql === 'BOOM') {
					throw new Error('statement failed');
				}
				return { rows: [] as any[], rowCount: 0 };
			}
		};
	}

	/** The text of a drizzle `sql` chunk built from raw strings */
	const textOf = (query: any): string => (query.queryChunks ?? []).flatMap((chunk: any) => chunk?.value ?? []).join('');

	it('is declared by every adapter whose runBatch holds a transaction on one connection', () => {
		const pgPool = { totalCount: 0, idleCount: 0, connect: async () => ({ query: async () => ({ rows: [] }), release() {} }) };
		const mysqlPool = { getConnection: async () => ({ execute: async () => [[], []], release() {} }) };
		const sqlite = { prepare: () => ({ run: () => ({ changes: 0 }), all: () => [], get: () => null }) };

		const declared = [
			createPostgreSQLProvider(pgPool as any),
			createPostgreSQLProvider({ query: async () => ({ rows: [] }) }, { singleConnection: true }),
			createMySQLProvider(mysqlPool as any),
			createMySQLProvider({ execute: async () => [[], []] }, { singleConnection: true }),
			createSQLiteProvider(sqlite as any, { singleConnection: true })
		];

		for (const provider of declared) {
			expect(provider.runBatch).toBeDefined();
			expect(provider.atomicBatch).toBe(true);
		}
	});

	it('is left unset where there is no transaction to roll back', () => {
		const providers = [
			createPostgreSQLProvider({ query: async () => ({ rows: [] }) }),
			createMySQLProvider({ execute: async () => [[], []] }),
			createSQLiteProvider({ prepare: () => ({ run: () => ({ changes: 0 }) }) } as any)
		];

		for (const provider of providers) {
			expect(provider.runBatch).toBeUndefined();
			expect(provider.atomicBatch).toBeUndefined();
		}
	});

	it('is declared through toProvider for a D1-style native batch and not for a plain binding', () => {
		const withBatch = toProvider({ prepare: () => ({}) as any, batch: async () => [] })!;
		const plain = toProvider({ prepare: () => ({}) as any })!;

		expect(withBatch.runBatch).toBeDefined();
		expect(withBatch.atomicBatch).toBe(true);
		expect(plain.runBatch).toBeUndefined();
		expect(plain.atomicBatch).toBeUndefined();
	});

	it('is declared for a drizzle transaction and not for a native-batch drizzle driver', () => {
		const run = async () => ({ rowsAffected: 0 });
		const transaction = async (callback: (tx: unknown) => Promise<unknown>) => await callback({ run });

		const transactional = createDrizzleSQLProvider({ run, transaction } as any, drizzleSql, 'postgres');
		const nativeBatch = createDrizzleSQLProvider({ run, transaction, batch: async () => [] } as any, drizzleSql, 'sqlite');

		expect(transactional.atomicBatch).toBe(true);
		expect(nativeBatch.runBatch).toBeUndefined();
		expect(nativeBatch.atomicBatch).toBeUndefined();
	});

	describe('Drizzle over a synchronous driver', () => {
		/** Mimics drizzle's bun:sqlite and better-sqlite3 sessions: begin, run the callback, commit, and no wait for a promise */
		function syncDrizzle() {
			const log: string[] = [];
			const client: any = {
				run(query: any) {
					const text = textOf(query);
					log.push(text);
					if (text === 'BOOM') {
						throw new Error('statement failed');
					}
					return { changes: 1, lastInsertRowid: 1 };
				},
				all() {
					return [];
				},
				transaction(callback: (tx: any) => unknown) {
					log.push('begin');
					try {
						const result = callback(client);
						log.push('commit');
						return result;
					} catch (error) {
						log.push('rollback');
						throw error;
					}
				}
			};
			return { client, log };
		}

		it('runs every statement inside the transaction, so the commit comes last', async () => {
			const { client, log } = syncDrizzle();
			const provider = createDrizzleSQLProvider(client, drizzleSql, 'sqlite');

			const results = await provider.runBatch!([{ sql: 'INSERT 1' }, { sql: 'INSERT 2' }]);

			expect(log).toEqual(['begin', 'INSERT 1', 'INSERT 2', 'commit']);
			expect(results).toHaveLength(2);
			expect(results[0]!.meta.changes).toBe(1);
		});

		it('rolls the whole group back when a later statement fails', async () => {
			const { client, log } = syncDrizzle();
			const provider = createDrizzleSQLProvider(client, drizzleSql, 'sqlite');

			await expect(provider.runBatch!([{ sql: 'INSERT 1' }, { sql: 'INSERT 2' }, { sql: 'BOOM' }])).rejects.toThrow('statement failed');

			expect(log).toEqual(['begin', 'INSERT 1', 'INSERT 2', 'BOOM', 'rollback']);
			expect(provider.atomicBatch).toBe(true);
		});

		it('lets no other statement into the open transaction', async () => {
			const { client, log } = syncDrizzle();
			const provider = createDrizzleSQLProvider(client, drizzleSql, 'sqlite');

			const batch = provider.runBatch!([{ sql: 'INSERT 1' }, { sql: 'INSERT 2' }]);
			const other = provider.prepare('SELECT 9').run();
			await Promise.all([batch, other]);

			expect(log.indexOf('SELECT 9')).toBeGreaterThan(log.indexOf('commit'));
		});

		it('still awaits statements on an asynchronous driver', async () => {
			const log: string[] = [];
			const client: any = {
				async run(query: any) {
					log.push(textOf(query));
					await new Promise((resolve) => setTimeout(resolve, 1));
					return { rowsAffected: 1 };
				},
				async transaction(callback: (tx: any) => Promise<unknown>) {
					log.push('begin');
					try {
						const result = await callback(client);
						log.push('commit');
						return result;
					} catch (error) {
						log.push('rollback');
						throw error;
					}
				}
			};

			const provider = createDrizzleSQLProvider(client, drizzleSql, 'postgres');
			await provider.runBatch!([{ sql: 'INSERT 1' }, { sql: 'INSERT 2' }]);

			expect(log).toEqual(['begin', 'INSERT 1', 'INSERT 2', 'commit']);
		});
	});

	describe('Hyperdrive providers', () => {
		it('runs a Postgres batch on the one client of the request and declares it atomic', async () => {
			const lifecycle: string[] = [];
			let created = 0;

			const provider = createHyperdrivePostgresProvider({ connectionString: 'postgres://example' }, () => {
				created++;
				return {
					async connect() {
						lifecycle.push('connect');
					},
					async query(sql: string) {
						lifecycle.push(sql);
						return { rows: [], rowCount: 0 };
					},
					async end() {
						lifecycle.push('end');
					}
				};
			});

			expect(provider.atomicBatch).toBe(true);
			await provider.prepare('SELECT 1').run();
			await provider.runBatch!([{ sql: 'INSERT INTO t VALUES (?)', bindings: [1] }]);

			expect(created).toBe(1);
			expect(lifecycle).toEqual(['connect', 'SELECT 1', 'BEGIN', 'INSERT INTO t VALUES ($1)', 'COMMIT']);
		});

		it('rolls a Postgres batch back on failure and frees the client for the next statement', async () => {
			const rec = recorder();
			const provider = createHyperdrivePostgresProvider({ connectionString: 'postgres://example' }, () => ({ query: rec.query }));

			await expect(provider.runBatch!([{ sql: 'INSERT 1' }, { sql: 'BOOM' }])).rejects.toThrow('statement failed');
			expect(rec.statements).toEqual(['BEGIN', 'INSERT 1', 'BOOM', 'ROLLBACK']);

			await provider.prepare('SELECT 2').run();
			expect(rec.statements.at(-1)).toBe('SELECT 2');
		});

		it('makes a plain statement wait for a batch in flight on the same client', async () => {
			const log: string[] = [];
			const provider = createHyperdrivePostgresProvider({ connectionString: 'postgres://example' }, () => ({
				async query(sql: string) {
					log.push(sql);
					await new Promise((resolve) => setTimeout(resolve, 2));
					return { rows: [], rowCount: 0 };
				}
			}));

			const batch = provider.runBatch!([{ sql: 'INSERT 1' }, { sql: 'INSERT 2' }]);
			const plain = provider.prepare('SELECT 9').run();
			await Promise.all([batch, plain]);

			expect(log).toEqual(['BEGIN', 'INSERT 1', 'INSERT 2', 'COMMIT', 'SELECT 9']);
		});

		it('serializes two batches on the same client', async () => {
			const log: string[] = [];
			const provider = createHyperdrivePostgresProvider({ connectionString: 'postgres://example' }, () => ({
				async query(sql: string) {
					log.push(sql);
					await new Promise((resolve) => setTimeout(resolve, 1));
					return { rows: [], rowCount: 0 };
				}
			}));

			await Promise.all([provider.runBatch!([{ sql: 'A1' }, { sql: 'A2' }]), provider.runBatch!([{ sql: 'B1' }, { sql: 'B2' }])]);

			expect(log).toEqual(['BEGIN', 'A1', 'A2', 'COMMIT', 'BEGIN', 'B1', 'B2', 'COMMIT']);
		});

		it('sends MySQL transaction control through query on the one connection and declares it atomic', async () => {
			const executed: string[] = [];
			const queried: string[] = [];
			let created = 0;

			const provider = createHyperdriveMySQLProvider({ connectionString: 'mysql://example' }, () => {
				created++;
				return {
					execute: async (sql: string) => {
						if (/^(BEGIN|COMMIT|ROLLBACK)$/i.test(sql)) {
							throw new Error('This command is not supported in the prepared statement protocol yet');
						}
						executed.push(sql);
						return [{ affectedRows: 1 }, []];
					},
					query: async (sql: string) => {
						queried.push(sql);
						return [{ affectedRows: 0 }, []];
					},
					end: async () => {}
				};
			});

			expect(provider.atomicBatch).toBe(true);
			const results = await provider.runBatch!([{ sql: 'INSERT 1' }, { sql: 'INSERT 2' }]);

			expect(results).toHaveLength(2);
			expect(created).toBe(1);
			expect(queried).toEqual(['BEGIN', 'COMMIT']);
			expect(executed).toEqual(['INSERT 1', 'INSERT 2']);
		});

		it('gives each request scope its own gate', async () => {
			const log: string[] = [];
			let release!: () => void;
			const held = new Promise<void>((resolve) => (release = resolve));
			const scopes = [{ id: 1 }, { id: 2 }];
			let current = scopes[0]!;

			const provider = createHyperdrivePostgresProvider(
				{ connectionString: 'postgres://example' },
				() => {
					const label = `client-${log.length}`;
					return {
						async query(sql: string) {
							log.push(`${label}:${sql}`);
							if (sql === 'HOLD') {
								await held;
							}
							return { rows: [], rowCount: 0 };
						}
					};
				},
				{ scope: () => current }
			);

			const slow = provider.runBatch!([{ sql: 'HOLD' }]);
			await new Promise((resolve) => setTimeout(resolve, 2));
			current = scopes[1]!;
			await provider.prepare('SELECT 2').run();
			expect(log.some((line) => line.endsWith(':SELECT 2'))).toBe(true);

			release();
			await slow;
		});
	});
});

describe('read consistency capabilities', () => {
	const singleNode = { primary: true, session: false, eventual: true };

	it('reports primary and eventual but no session on the single-node adapters', () => {
		const postgres = createPostgreSQLProvider({ query: async () => ({ rows: [] }) });
		const mysql = createMySQLProvider({ execute: async () => [[], []] });
		const sqlite = createSQLiteProvider({ prepare: () => ({ all: () => [], get: () => null, run: () => ({ changes: 0 }) }) });
		const drizzle = createDrizzleSQLProvider({ all: async () => ({ rows: [] }) } as any, drizzleSql, 'sqlite');

		for (const provider of [postgres, mysql, sqlite, drizzle]) {
			expect(provider.consistency).toEqual(singleNode);
			expect(provider.withSession).toBeUndefined();
		}
	});

	it('reports the same on the Hyperdrive wrappers', () => {
		const postgres = createHyperdrivePostgresProvider({ connectionString: 'postgres://example' }, () => ({
			query: async () => ({ rows: [] })
		}));
		const mysql = createHyperdriveMySQLProvider({ connectionString: 'mysql://example' }, () => ({ execute: async () => [[], []] }));

		expect(postgres.consistency).toEqual(singleNode);
		expect(mysql.consistency).toEqual(singleNode);
	});

	it('keeps withSession and its bookmark when a D1 binding goes through toProvider', () => {
		const constraints: Array<string | undefined> = [];
		const binding = {
			prepare: () => ({}) as any,
			batch: async () => [],
			withSession(constraint?: string) {
				constraints.push(constraint);
				return { prepare: () => ({}) as any, getBookmark: () => 'bm-1' };
			}
		};

		const provider = toProvider(binding)!;
		const session = provider.withSession!('first-primary');

		expect(constraints).toEqual(['first-primary']);
		expect(session.getBookmark()).toBe('bm-1');
		expect(provider.runBatch).toBeDefined();
	});

	it('adds no withSession to a binding that has none', () => {
		const provider = toProvider({ prepare: () => ({}) as any, batch: async () => [] })!;

		expect(provider.withSession).toBeUndefined();
		expect(provider.consistency).toBeUndefined();
	});
});

describe('Single-connection batch gate', () => {
	const INSERT_ONE = 'INSERT INTO users (id) VALUES (1)';
	const INSERT_TWO = 'INSERT INTO users (id) VALUES (2)';
	const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 0));
	const outcome = (work: Promise<unknown>) =>
		work.then(
			() => undefined,
			(error: unknown) => error
		);
	const within = <T>(promise: Promise<T>, ms = 250) =>
		Promise.race([promise, new Promise<never>((_, reject) => setTimeout(() => reject(new Error('timed out behind the gate')), ms))]);

	/** Fake clients that log each statement as it is dispatched and can hold or fail one. */
	function rig() {
		const log: string[] = [];
		const holds = new Map<string, Promise<void>>();
		const failures = new Map<string, Error>();
		const exec = async (sql: string) => {
			log.push(sql);
			await holds.get(sql);
			const failure = failures.get(sql);
			if (failure) {
				failures.delete(sql);
				throw failure;
			}
		};
		return {
			log,
			exec,
			hold(sql: string): () => void {
				let release!: () => void;
				holds.set(
					sql,
					new Promise<void>((resolve) => {
						release = resolve;
					})
				);
				return release;
			},
			failOnce(sql: string, message: string) {
				failures.set(sql, new Error(message));
			}
		};
	}

	type Kind = 'postgres' | 'mysql' | 'sqlite';

	function build(kind: Kind, options: SQLBatchOptions<any> = { singleConnection: true }) {
		const fake = rig();
		const { exec } = fake;
		const client =
			kind === 'postgres'
				? {
						query: async (sql: string) => {
							await exec(sql);
							return { rows: [] as any[], rowCount: 0 };
						}
					}
				: kind === 'mysql'
					? {
							execute: async (sql: string) => {
								await exec(sql);
								return [[], []];
							},
							query: async (sql: string) => {
								await exec(sql);
								return [[], []];
							}
						}
					: {
							prepare: (sql: string) => ({
								run: async () => {
									await exec(sql);
									return { changes: 0 };
								},
								all: async () => {
									await exec(sql);
									return [];
								},
								get: async () => {
									await exec(sql);
									return null;
								}
							})
						};
		const provider =
			kind === 'postgres'
				? createPostgreSQLProvider(client as any, options)
				: kind === 'mysql'
					? createMySQLProvider(client as any, options)
					: createSQLiteProvider(client as any, options);
		return { ...fake, client, provider };
	}

	describe.each<Kind>(['postgres', 'mysql', 'sqlite'])('%s', (kind) => {
		it('holds a plain statement until the batch commits', async () => {
			const { provider, log, hold } = build(kind);
			const release = hold(INSERT_ONE);

			const batch = provider.runBatch!([{ sql: INSERT_ONE }, { sql: INSERT_TWO }]);
			await flush();
			const plain = provider.prepare('SELECT 1').all();
			await flush();

			expect(log).toEqual(['BEGIN', INSERT_ONE]);

			release();
			await Promise.all([batch, plain]);
			expect(log).toEqual(['BEGIN', INSERT_ONE, INSERT_TWO, 'COMMIT', 'SELECT 1']);
		});

		it('holds every plain statement shape (run, all, first)', async () => {
			const { provider, log, hold } = build(kind);
			const release = hold(INSERT_ONE);

			const batch = provider.runBatch!([{ sql: INSERT_ONE }]);
			await flush();
			const waiting = [provider.prepare('SELECT 1').run(), provider.prepare('SELECT 2').all(), provider.prepare('SELECT 3').first()];
			await flush();

			expect(log).toEqual(['BEGIN', INSERT_ONE]);

			release();
			await Promise.all([batch, ...waiting]);
			expect(log.indexOf('COMMIT')).toBe(2);
			expect(log.slice(3).sort()).toEqual(['SELECT 1', 'SELECT 2', 'SELECT 3']);
		});

		it('runs two concurrent batches one after the other', async () => {
			const { provider, log, hold } = build(kind);
			const release = hold(INSERT_ONE);

			const first = provider.runBatch!([{ sql: INSERT_ONE }]);
			const second = provider.runBatch!([{ sql: INSERT_TWO }]);
			await flush();

			expect(log).toEqual(['BEGIN', INSERT_ONE]);

			release();
			await Promise.all([first, second]);
			expect(log).toEqual(['BEGIN', INSERT_ONE, 'COMMIT', 'BEGIN', INSERT_TWO, 'COMMIT']);
		});

		it('orders a plain statement issued between two batches after the first only', async () => {
			const { provider, log, hold } = build(kind);
			const release = hold(INSERT_ONE);

			const first = provider.runBatch!([{ sql: INSERT_ONE }]);
			const plain = provider.prepare('SELECT 1').all();
			const second = provider.runBatch!([{ sql: INSERT_TWO }]);
			await flush();
			release();
			await Promise.all([first, plain, second]);

			expect(log.indexOf('SELECT 1')).toBeGreaterThan(log.indexOf('COMMIT'));
			expect(log.filter((entry) => entry === 'BEGIN')).toHaveLength(2);
			expect(log.indexOf('SELECT 1')).not.toBeGreaterThan(log.lastIndexOf('BEGIN'));
		});

		it('rolls a failing batch back, then lets the waiting statement run', async () => {
			const { provider, log, hold, failOnce } = build(kind);
			const release = hold(INSERT_ONE);
			failOnce(INSERT_TWO, 'statement failed');

			const batch = provider.runBatch!([{ sql: INSERT_ONE }, { sql: INSERT_TWO }]);
			const failure = outcome(batch);
			await flush();
			const plain = provider.prepare('SELECT 1').all();
			await flush();
			expect(log).toEqual(['BEGIN', INSERT_ONE]);

			release();
			expect(await failure).toMatchObject({ message: 'statement failed' });
			await plain;
			expect(log).toEqual(['BEGIN', INSERT_ONE, INSERT_TWO, 'ROLLBACK', 'SELECT 1']);
		});

		it('releases the gate when the connection throws before BEGIN', async () => {
			const { provider, log, failOnce } = build(kind);
			failOnce('BEGIN', 'connection refused');

			await expect(provider.runBatch!([{ sql: INSERT_ONE }])).rejects.toThrow('connection refused');
			await within(provider.prepare('SELECT 1').all());
			await within(provider.runBatch!([{ sql: INSERT_TWO }]));

			expect(log).toEqual(['BEGIN', 'SELECT 1', 'BEGIN', INSERT_TWO, 'COMMIT']);
		});

		it('releases the gate when a caller lease throws', async () => {
			const { provider, log } = build(kind, {
				singleConnection: true,
				lease: async () => {
					throw new Error('no connection');
				}
			});

			await expect(provider.runBatch!([{ sql: INSERT_ONE }])).rejects.toThrow('no connection');
			await within(provider.prepare('SELECT 1').all());
			expect(log).toEqual(['SELECT 1']);
		});

		it('gates a caller lease when the handle is declared single, and leaves the lease callers alone', async () => {
			const fake = build(kind);
			let released = 0;
			const gated = build(kind, {
				singleConnection: true,
				lease: async () => ({ client: fake.client as any, release: () => void released++ })
			});
			const release = fake.hold(INSERT_ONE);

			const batch = gated.provider.runBatch!([{ sql: INSERT_ONE }, { sql: INSERT_TWO }]);
			await flush();
			const plain = gated.provider.prepare('SELECT 1').all();
			await flush();

			expect(fake.log).toEqual(['BEGIN', INSERT_ONE]);
			expect(gated.log).toEqual([]);

			release();
			await Promise.all([batch, plain]);
			expect(fake.log).toEqual(['BEGIN', INSERT_ONE, INSERT_TWO, 'COMMIT']);
			expect(gated.log).toEqual(['SELECT 1']);
			expect(released).toBe(1);
		});

		it('dispatches a plain statement in the same tick when no batch is open', async () => {
			const { provider, log } = build(kind);

			const plain = provider.prepare('SELECT 1').all();
			expect(log).toEqual(['SELECT 1']);
			await plain;

			await provider.runBatch!([{ sql: INSERT_ONE }]);
			const after = provider.prepare('SELECT 2').first();
			expect(log.at(-1)).toBe('SELECT 2');
			await after;
			expect(log).toEqual(['SELECT 1', 'BEGIN', INSERT_ONE, 'COMMIT', 'SELECT 2']);
		});
	});

	describe('real node:sqlite handle', () => {
		const PRE_BATCH = 'CREATE TABLE t (id INTEGER PRIMARY KEY)';

		/** One node:sqlite connection whose `run` of one statement waits for the test. */
		function heldSqlite(holdSql: string) {
			const db = openNodeSqlite();
			db.exec(PRE_BATCH);
			const inner = createNodeSqliteClient(db);
			let release!: () => void;
			const held = new Promise<void>((resolve) => {
				release = resolve;
			});
			const client = {
				prepare(sql: string) {
					const statement = inner.prepare!(sql);
					return {
						...statement,
						run: async (...bindings: any[]) => {
							if (sql === holdSql) {
								await held;
							}
							return statement.run!(...bindings);
						}
					};
				}
			};
			return { provider: createSQLiteProvider(client, { singleConnection: true }), release };
		}

		it('shows a statement queued behind a batch the rows the batch committed', async () => {
			const { provider, release } = heldSqlite('INSERT INTO t (id) VALUES (2)');

			const batch = provider.runBatch!([{ sql: 'INSERT INTO t (id) VALUES (1)' }, { sql: 'INSERT INTO t (id) VALUES (2)' }]);
			await flush();
			let settled = false;
			const read = provider
				.prepare('SELECT id FROM t ORDER BY id')
				.all()
				.finally(() => {
					settled = true;
				});
			await flush();
			expect(settled).toBe(false);

			release();
			await batch;
			expect((await read).results).toEqual([{ id: 1 }, { id: 2 }]);
		});

		it('shows a statement queued behind a failed batch the pre-batch state', async () => {
			const { provider, release } = heldSqlite('INSERT INTO t (id) SELECT 1');

			const batch = provider.runBatch!([{ sql: 'INSERT INTO t (id) VALUES (1)' }, { sql: 'INSERT INTO t (id) SELECT 1' }]);
			const failure = outcome(batch);
			await flush();
			const read = provider.prepare('SELECT id FROM t ORDER BY id').all();
			await flush();

			release();
			expect(await failure).toBeInstanceOf(Error);
			expect((await read).results).toEqual([]);
		});

		it('keeps the connection usable after a rolled-back batch', async () => {
			const { provider, release } = heldSqlite('INSERT INTO t (id) SELECT 1');
			release();

			await expect(
				provider.runBatch!([{ sql: 'INSERT INTO t (id) VALUES (1)' }, { sql: 'INSERT INTO t (id) SELECT 1' }])
			).rejects.toThrow();
			await provider.runBatch!([{ sql: 'INSERT INTO t (id) VALUES (7)' }]);

			expect((await provider.prepare('SELECT id FROM t').all()).results).toEqual([{ id: 7 }]);
		});
	});

	describe('pooled adapters', () => {
		it('sends a plain statement straight to the pool while a pg batch holds its own connection', async () => {
			const fake = rig();
			const seenBy: unknown[] = [];
			const pool = {
				totalCount: 1,
				idleCount: 1,
				query(this: unknown, sql: string) {
					seenBy.push(this);
					return fake.exec(sql).then(() => ({ rows: [] as any[], rowCount: 0 }));
				},
				async connect() {
					return { query: (sql: string) => fake.exec(sql).then(() => ({ rows: [] as any[], rowCount: 0 })), release: () => {} };
				}
			};

			for (const options of [undefined, { singleConnection: true }] as const) {
				fake.log.length = 0;
				seenBy.length = 0;
				const release = fake.hold(INSERT_ONE);
				const provider = createPostgreSQLProvider(pool as any, options);

				const batch = provider.runBatch!([{ sql: INSERT_ONE }]);
				await flush();
				const plain = provider.prepare('SELECT 1').all();

				// dispatched synchronously on the pool itself, with no wrapper in between
				expect(fake.log).toEqual(['BEGIN', INSERT_ONE, 'SELECT 1']);
				expect(seenBy).toEqual([pool]);

				release();
				await Promise.all([batch, plain]);
				expect(fake.log).toEqual(['BEGIN', INSERT_ONE, 'SELECT 1', 'COMMIT']);
			}
		});

		it('sends a plain statement straight to the pool while a mysql2 batch holds its own connection', async () => {
			const fake = rig();
			const seenBy: unknown[] = [];
			const pool = {
				execute(this: unknown, sql: string) {
					seenBy.push(this);
					return fake.exec(sql).then(() => [[], []]);
				},
				async getConnection() {
					return {
						execute: (sql: string) => fake.exec(sql).then(() => [[], []]),
						query: (sql: string) => fake.exec(sql).then(() => [[], []]),
						release: () => {}
					};
				}
			};

			for (const options of [undefined, { singleConnection: true }] as const) {
				fake.log.length = 0;
				seenBy.length = 0;
				const release = fake.hold(INSERT_ONE);
				const provider = createMySQLProvider(pool as any, options);

				const batch = provider.runBatch!([{ sql: INSERT_ONE }]);
				await flush();
				const plain = provider.prepare('SELECT 1').all();

				expect(fake.log).toEqual(['BEGIN', INSERT_ONE, 'SELECT 1']);
				expect(seenBy).toEqual([pool]);

				release();
				await Promise.all([batch, plain]);
				expect(fake.log).toEqual(['BEGIN', INSERT_ONE, 'SELECT 1', 'COMMIT']);
			}
		});

		it('does not gate a lease-only adapter, which is how the Hyperdrive providers order their own batches', async () => {
			const fake = rig();
			const client = { query: (sql: string) => fake.exec(sql).then(() => ({ rows: [] as any[], rowCount: 0 })) };
			const provider = createPostgreSQLProvider(client as any, { lease: async () => ({ client, release: () => {} }) });
			const release = fake.hold(INSERT_ONE);

			const batch = provider.runBatch!([{ sql: INSERT_ONE }]);
			await flush();
			const plain = provider.prepare('SELECT 1').all();

			expect(fake.log).toEqual(['BEGIN', INSERT_ONE, 'SELECT 1']);
			release();
			await Promise.all([batch, plain]);
		});
	});
});
