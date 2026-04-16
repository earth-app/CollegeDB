import { describe, expect, it } from 'vitest';
import { KVShardMapper } from '../src/kvmap.js';
import {
	createHyperdriveMySQLProvider,
	createHyperdrivePostgresProvider,
	createMySQLProvider,
	createPostgreSQLProvider,
	createRedisKVProvider,
	createSQLiteProvider,
	createValkeyKVProvider
} from '../src/providers.js';

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
