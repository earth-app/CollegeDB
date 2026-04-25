import { beforeEach, describe, expect, it } from 'vitest';
import {
	allAllShardsGlobal,
	allByLookupKey,
	count,
	countAllShards,
	explain,
	firstAllShardsGlobal,
	firstByLookupKey,
	getDatabaseSizesAllShards,
	getTotalDatabaseSize,
	indexAllShards,
	initialize,
	resetConfig
} from '../src/index';
import { KVShardMapper } from '../src/kvmap';

type Row = Record<string, unknown>;

class MockKVNamespace {
	private data = new Map<string, string>();

	async get<T = unknown>(key: string, type: 'json'): Promise<T | null>;
	async get(key: string, type?: 'text'): Promise<string | null>;
	async get<T = unknown>(key: string, type: 'json' | 'text' = 'text'): Promise<T | string | null> {
		const value = this.data.get(key);
		if (value === undefined) return null;
		if (type === 'json') return JSON.parse(value) as T;
		return value;
	}

	async put(key: string, value: string): Promise<void> {
		this.data.set(key, value);
	}

	async delete(key: string): Promise<void> {
		this.data.delete(key);
	}

	async list(options?: { prefix?: string }): Promise<{ keys: Array<{ name: string }> }> {
		const prefix = options?.prefix ?? '';
		const keys = Array.from(this.data.keys())
			.filter((key) => key.startsWith(prefix))
			.map((name) => ({ name }));
		return { keys };
	}
}

class MockDatabase {
	private rows: Row[];
	private failAll: boolean;
	private pageCount: number;
	private pageSize: number;
	public allCalls = 0;
	public firstCalls = 0;
	public runSql: string[] = [];

	constructor(rows: Row[], options?: { failAll?: boolean; pageCount?: number; pageSize?: number }) {
		this.rows = [...rows];
		this.failAll = options?.failAll ?? false;
		this.pageCount = options?.pageCount ?? Math.max(1, rows.length);
		this.pageSize = options?.pageSize ?? 4096;
	}

	prepare(sql: string) {
		const self = this;
		const createBound = (bindings: any[]) => ({
			async all<T = Row>() {
				self.allCalls += 1;
				if (self.failAll) {
					throw new Error('forced all() failure');
				}
				const results = self.select(sql, bindings) as T[];
				return {
					success: true,
					results,
					meta: { duration: 1 }
				};
			},
			async first<T = Row>() {
				self.firstCalls += 1;
				if (self.failAll) {
					throw new Error('forced first() failure');
				}
				const results = self.select(sql, bindings) as T[];
				return results[0] ?? null;
			},
			async run<T = Row>() {
				self.runSql.push(sql);
				return {
					success: true,
					results: [] as T[],
					meta: { duration: 1 }
				};
			}
		});

		return {
			bind(...bindings: any[]) {
				return createBound(bindings);
			},
			...createBound([])
		};
	}

	private select(sql: string, bindings: any[]): Row[] {
		const normalized = sql.toLowerCase();

		if (normalized.startsWith('pragma page_count')) {
			return [{ page_count: this.pageCount }];
		}

		if (normalized.startsWith('pragma page_size')) {
			return [{ page_size: this.pageSize }];
		}

		if (normalized.startsWith('select count(*) as row_count from')) {
			return [{ row_count: this.rows.length }];
		}

		if (normalized.startsWith('explain')) {
			return [{ detail: `plan:${sql}` }];
		}

		if (normalized.includes('where email = ?')) {
			return this.rows.filter((row) => row.email === bindings[0]);
		}

		if (normalized.includes('where username = ?')) {
			return this.rows.filter((row) => row.username === bindings[0]);
		}

		if (normalized.includes('where id = ?')) {
			return this.rows.filter((row) => row.id === bindings[0]);
		}

		return [...this.rows];
	}
}

describe('router helper APIs', () => {
	let kv: MockKVNamespace;
	let east: MockDatabase;
	let west: MockDatabase;

	beforeEach(() => {
		resetConfig();
		kv = new MockKVNamespace();
		east = new MockDatabase([
			{ id: 'u-east', username: 'alice', email: 'alice@example.com', score: 10 },
			{ id: 'u-east-2', username: 'bravo', email: 'bravo@example.com', score: 7 }
		]);
		west = new MockDatabase([
			{ id: 'u-west', username: 'charlie', email: 'charlie@example.com', score: 9 },
			{ id: 'u-west-2', username: 'delta', email: 'delta@example.com', score: 4 }
		]);

		initialize({
			kv: kv as any,
			shards: {
				'db-east': east as any,
				'db-west': west as any
			},
			strategy: 'hash',
			disableAutoMigration: true,
			hashShardMappings: false
		});
	});

	it('firstByLookupKey resolves mapped secondary key without fanout', async () => {
		const mapper = new KVShardMapper(kv as any, { hashShardMappings: false });
		await mapper.setShardMapping('u-east', 'db-east', ['email:alice@example.com']);

		const row = await firstByLookupKey<{ id: string }>('email:alice@example.com', 'SELECT * FROM users WHERE email = ?', [
			'alice@example.com'
		]);

		expect(row?.id).toBe('u-east');
		expect(east.firstCalls).toBe(1);
		expect(west.firstCalls).toBe(0);
	});

	it('firstByLookupKey falls back to fanout when no mapping exists', async () => {
		const row = await firstByLookupKey<{ id: string }>('email:charlie@example.com', 'SELECT * FROM users WHERE email = ?', [
			'charlie@example.com'
		]);

		expect(row?.id).toBe('u-west');
		expect(east.firstCalls).toBeGreaterThan(0);
		expect(west.firstCalls).toBeGreaterThan(0);
	});

	it('firstByLookupKey handles stale mapping by retrying with fanout', async () => {
		const mapper = new KVShardMapper(kv as any, { hashShardMappings: false });
		await mapper.setShardMapping('email:charlie@example.com', 'db-east');

		const row = await firstByLookupKey<{ id: string }>('email:charlie@example.com', 'SELECT * FROM users WHERE email = ?', [
			'charlie@example.com'
		]);

		expect(row?.id).toBe('u-west');
		expect(east.firstCalls).toBeGreaterThanOrEqual(2);
		expect(west.firstCalls).toBeGreaterThanOrEqual(1);
	});

	it('allByLookupKey returns globally merged rows when fallback fanout is needed', async () => {
		const result = await allByLookupKey<{ id: string }>('username:missing', 'SELECT * FROM users');

		expect(result.results).toHaveLength(4);
		expect(result.results.map((row) => row.id).sort()).toEqual(['u-east', 'u-east-2', 'u-west', 'u-west-2']);
	});

	it('allAllShardsGlobal performs global sort and pagination', async () => {
		const result = await allAllShardsGlobal<{ id: string; score: number }>('SELECT * FROM users', [], {
			sortBy: 'score',
			sortDirection: 'desc',
			offset: 1,
			limit: 2
		});

		expect(result.success).toBe(true);
		expect(result.results.map((row) => row.score)).toEqual([9, 7]);
	});

	it('firstAllShardsGlobal returns first row after global ordering/offset', async () => {
		const row = await firstAllShardsGlobal<{ id: string; score: number }>('SELECT * FROM users', [], {
			sortBy: 'score',
			sortDirection: 'asc',
			offset: 2
		});

		expect(row?.id).toBe('u-west');
		expect(row?.score).toBe(9);
	});

	it('allAllShardsGlobal returns partial data with failure metadata when one shard fails', async () => {
		const failing = new MockDatabase([{ id: 'u-bad', score: 1 }], { failAll: true });
		initialize({
			kv: kv as any,
			shards: {
				'db-east': east as any,
				'db-west': failing as any
			},
			strategy: 'hash',
			disableAutoMigration: true,
			hashShardMappings: false
		});

		const result = await allAllShardsGlobal<{ id: string }>('SELECT * FROM users');
		expect(result.success).toBe(false);
		expect(result.results.length).toBe(2);
		expect(result.error).toContain('forced all() failure');
	});

	it('indexAllShards creates deterministic index SQL on all shards', async () => {
		const results = await indexAllShards('users', ['email', 'username']);
		expect(results).toHaveLength(2);
		expect(results.every((r) => r.success)).toBe(true);
		expect(east.runSql[0]).toContain('CREATE INDEX IF NOT EXISTS "idx_users_email_username"');
		expect(east.runSql[0]).toContain('ON "users" ("email", "username")');
		expect(west.runSql[0]).toContain('CREATE INDEX IF NOT EXISTS "idx_users_email_username"');
	});

	it('indexAllShards rejects unsafe identifiers', async () => {
		await expect(indexAllShards('users; DROP TABLE users', ['email'])).rejects.toThrow('Invalid SQL identifier');
	});

	it('explain returns query-plan rows', async () => {
		const result = await explain<{ detail: string }>('u-east', 'SELECT * FROM users WHERE id = ?', ['u-east']);
		expect(result.success).toBe(true);
		expect(result.results[0]?.detail).toContain('EXPLAIN QUERY PLAN SELECT * FROM users WHERE id = ?');
	});

	it('count and countAllShards return table statistics', async () => {
		const routedCount = await count('u-east', 'users');
		expect(routedCount).toBe(2);

		const aggregate = await countAllShards('users');
		expect(aggregate.total).toBe(4);
		expect(aggregate.shards).toHaveLength(2);
		expect(aggregate.shards.every((s) => s.success)).toBe(true);
	});

	it('size helpers aggregate shard database sizes', async () => {
		east = new MockDatabase(
			[
				{ id: 'u-east', username: 'alice', email: 'alice@example.com', score: 10 },
				{ id: 'u-east-2', username: 'bravo', email: 'bravo@example.com', score: 7 }
			],
			{ pageCount: 10, pageSize: 1024 }
		);
		west = new MockDatabase(
			[
				{ id: 'u-west', username: 'charlie', email: 'charlie@example.com', score: 9 },
				{ id: 'u-west-2', username: 'delta', email: 'delta@example.com', score: 4 }
			],
			{ pageCount: 20, pageSize: 1024 }
		);

		initialize({
			kv: kv as any,
			shards: {
				'db-east': east as any,
				'db-west': west as any
			},
			strategy: 'hash',
			disableAutoMigration: true,
			hashShardMappings: false
		});

		const sizes = await getDatabaseSizesAllShards();
		expect(sizes).toHaveLength(2);
		expect(sizes.map((s) => s.size).sort((a, b) => (a || 0) - (b || 0))).toEqual([10240, 20480]);

		const total = await getTotalDatabaseSize();
		expect(total).toBe(30720);
	});
});
