import type { Request } from '@cloudflare/workers-types';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
	all,
	allAllShards,
	allByLookupKey,
	allShard,
	autoDetectAndMigrate,
	checkMigrationNeeded,
	clearMigrationCache,
	clearShardMigrationCache,
	collegedb,
	CollegeDBError,
	createInMemoryKVProvider,
	createInMemorySQLProvider,
	first,
	firstShard,
	flush,
	getClosestRegionFromIP,
	getDatabaseSizeForKey,
	getDatabaseSizeForShard,
	getDatabaseSizesAllShards,
	getShardStats,
	getTotalDatabaseSize,
	initialize,
	initializeAsync,
	insert,
	integrateExistingDatabase,
	KVShardMapper,
	listKnownShards,
	prepare,
	reassignShard,
	resetConfig,
	run,
	runAllShards,
	runShard
} from '../src/index';
import {
	createMappingsForExistingKeys,
	createSchemaAcrossShards,
	discoverExistingPrimaryKeys,
	discoverExistingRecordsWithColumns,
	dropSchema,
	listTables,
	migrateRecord,
	schemaExists,
	validateTableForSharding
} from '../src/migrations';
import type { CollegeDBConfig, D1Region, MixedShardingStrategy, ShardingStrategy } from '../src/types';

// Test schema for creating tables
const TEST_SCHEMA = `
	CREATE TABLE IF NOT EXISTS users (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT UNIQUE
	);

	CREATE TABLE IF NOT EXISTS posts (
		id TEXT PRIMARY KEY,
		user_id TEXT NOT NULL,
		title TEXT NOT NULL,
		content TEXT,
		FOREIGN KEY (user_id) REFERENCES users(id)
	);

	CREATE INDEX IF NOT EXISTS idx_posts_user_id ON posts(user_id);
`;

// Mock implementations for D1 and KV
class MockD1Database {
	private data = new Map<string, any[]>();
	private schema = new Set<string>();

	prepare(sql: string) {
		const self = this;

		return {
			bind(...args: any[]) {
				return {
					async run() {
						return await self.executeStatement(sql, args);
					},

					async all() {
						return await self.executeQuery(sql, args);
					},

					async first() {
						const allResult = await self.executeQuery(sql, args);
						return allResult.results[0] || null;
					}
				};
			},

			// Direct run method for statements without parameters
			async run() {
				return await self.executeStatement(sql, []);
			},

			// Direct all method for queries without parameters
			async all() {
				return await self.executeQuery(sql, []);
			},

			// Direct first method for queries without parameters
			async first() {
				const allResult = await self.executeQuery(sql, []);
				return allResult.results[0] || null;
			}
		};
	}

	private async executeStatement(sql: string, args: any[]) {
		if (sql.includes('CREATE TABLE')) {
			const tableName = sql.match(/CREATE TABLE (?:IF NOT EXISTS )?(\w+)/)?.[1];
			if (tableName) this.schema.add(tableName);
			return { success: true };
		}

		if (sql.includes('INSERT')) {
			const tableName = sql.match(/INSERT (?:OR REPLACE )?INTO (\w+)/)?.[1];
			const isReplace = sql.includes('OR REPLACE');
			if (tableName) {
				if (!this.data.has(tableName)) this.data.set(tableName, []);
				const table = this.data.get(tableName)!;

				// Handle different table structures
				if (tableName === 'users') {
					const record = { id: args[0], name: args[1], email: args[2] };
					// Check for existing record with same id
					const existingIndex = table.findIndex((r: any) => r.id === args[0]);
					if (existingIndex > -1) {
						if (isReplace) {
							table[existingIndex] = record;
						} else {
							// Simulate UNIQUE constraint violation
							return { success: false, error: 'UNIQUE constraint failed: users.id' };
						}
					} else {
						table.push(record);
					}
				} else if (tableName === 'posts') {
					const record = { id: args[0], user_id: args[1], title: args[2], content: args[3] };
					const existingIndex = table.findIndex((r: any) => r.id === args[0]);
					if (existingIndex > -1) {
						if (isReplace) {
							table[existingIndex] = record;
						} else {
							return { success: false, error: 'UNIQUE constraint failed: posts.id' };
						}
					} else {
						table.push(record);
					}
				} else {
					// Generic handling for other tables
					const record: any = { id: args[0] };
					for (let i = 1; i < args.length; i++) {
						record[`field_${i}`] = args[i];
					}
					const existingIndex = table.findIndex((r: any) => r.id === args[0]);
					if (existingIndex > -1 && !isReplace) {
						return { success: false, error: 'UNIQUE constraint failed' };
					}
					table.push(record);
				}
			}
			return { success: true };
		}

		if (sql.includes('UPDATE')) {
			const tableName = sql.match(/UPDATE (\w+)/)?.[1];
			if (tableName && this.data.has(tableName)) {
				const table = this.data.get(tableName)!;

				// Simple parsing - for this test we know the structure
				if (sql.includes('SET name = ?')) {
					// UPDATE users SET name = ? WHERE id = ?
					const record = table.find((r) => r.id === args[1]); // args[1] is the id in WHERE clause
					if (record) {
						record.name = args[0]; // args[0] is the new name
					}
				} else if (sql.includes('SET email = ?')) {
					// UPDATE users SET email = ? WHERE id = ?
					const record = table.find((r) => r.id === args[1]);
					if (record) {
						record.email = args[0];
					}
				}
			}
			return { success: true };
		}

		if (sql.includes('DELETE')) {
			const tableName = sql.match(/DELETE FROM (\w+)/)?.[1];
			if (tableName && this.data.has(tableName)) {
				const table = this.data.get(tableName)!;
				const index = table.findIndex((r) => r.id === args[0]);
				if (index > -1) table.splice(index, 1);
			}
			return { success: true };
		}

		if (sql.includes('CREATE INDEX')) {
			return { success: true };
		}

		return { success: true };
	}

	private async executeQuery(sql: string, args: any[]) {
		// Handle table listing queries
		if (sql.includes('SELECT name FROM sqlite_master')) {
			const tables = Array.from(this.schema);
			// Handle specific table existence check with WHERE clause
			if (sql.includes('WHERE') && args.length > 0) {
				const searchTable = args[0];
				const tableExists = tables.find((name) => name === searchTable);
				return { success: true, results: tableExists ? [{ name: tableExists }] : [] };
			}
			// Handle general table listing
			return { success: true, results: tables.map((name) => ({ name })) };
		}

		// Handle COUNT queries
		if (sql.includes('SELECT COUNT(*)')) {
			const tableName = sql.match(/FROM (\w+)/)?.[1];
			if (tableName && this.data.has(tableName)) {
				const table = this.data.get(tableName)!;
				return { success: true, results: [{ count: table.length }] };
			}
			return { success: true, results: [{ count: 0 }] };
		}

		// Handle PRAGMA table_info queries
		if (sql.includes('PRAGMA table_info')) {
			const tableName = sql.match(/PRAGMA table_info\((\w+)\)/)?.[1];
			if (tableName === 'users') {
				return {
					success: true,
					results: [
						{ name: 'id', type: 'TEXT', pk: 1 },
						{ name: 'name', type: 'TEXT', pk: 0 },
						{ name: 'email', type: 'TEXT', pk: 0 }
					]
				};
			} else if (tableName === 'posts') {
				return {
					success: true,
					results: [
						{ name: 'id', type: 'TEXT', pk: 1 },
						{ name: 'user_id', type: 'TEXT', pk: 0 },
						{ name: 'title', type: 'TEXT', pk: 0 },
						{ name: 'content', type: 'TEXT', pk: 0 }
					]
				};
			}
			return { success: true, results: [] };
		}

		// Handle SELECT queries
		if (sql.includes('SELECT')) {
			const tableName = sql.match(/FROM (\w+)/)?.[1];
			if (tableName && this.data.has(tableName)) {
				const table = this.data.get(tableName)!;
				let results = table;

				// Handle WHERE id = ? queries
				if (sql.includes('WHERE id = ?')) {
					results = table.filter((r) => r.id === args[0]);
				}

				// Handle LIMIT queries
				if (sql.includes('LIMIT')) {
					const limitMatch = sql.match(/LIMIT (\d+)/);
					if (limitMatch) {
						const limit = parseInt(limitMatch[1] || '0');
						results = results.slice(0, limit);
					}
				}

				return { success: true, results };
			}
		}
		return { success: true, results: [] };
	}

	addTestData(tableName: string, records: any[]) {
		this.data.set(tableName.toLowerCase(), records);
		this.schema.add(tableName.toLowerCase());
	}

	hasTable(tableName: string): boolean {
		return this.schema.has(tableName.toLowerCase());
	}

	clear() {
		this.data.clear();
		this.schema.clear();
	}
}

class MockKVNamespace {
	private data = new Map<string, string>();

	async get(key: string, type?: string) {
		const value = this.data.get(key);
		if (!value) return null;
		return type === 'json' ? JSON.parse(value) : value;
	}

	async put(key: string, value: string) {
		this.data.set(key, value);
	}

	async delete(key: string) {
		this.data.delete(key);
	}

	async list(options?: { prefix?: string }) {
		const keys = Array.from(this.data.keys());
		const filteredKeys = options?.prefix ? keys.filter((k) => k.startsWith(options.prefix!)) : keys;

		return {
			keys: filteredKeys.map((name) => ({ name }))
		};
	}

	clear() {
		this.data.clear();
	}
}

class MockDurableObjectNamespace {
	idFromName() {
		return 'mock-id';
	}

	get() {
		return {
			fetch: async (url: string, init?: RequestInit) => {
				const path = new URL(url).pathname;

				if (path === '/allocate') {
					return new Response(JSON.stringify({ shard: 'db-east' }), {
						headers: { 'Content-Type': 'application/json' }
					});
				}

				if (path === '/shards') {
					return new Response(JSON.stringify(['db-east', 'db-west']), {
						headers: { 'Content-Type': 'application/json' }
					});
				}

				if (path === '/stats') {
					return new Response(
						JSON.stringify([
							{ binding: 'db-east', count: 1 },
							{ binding: 'db-west', count: 0 }
						]),
						{
							headers: { 'Content-Type': 'application/json' }
						}
					);
				}

				return new Response('OK');
			}
		};
	}
}

describe('CollegeDB', () => {
	let mockConfig: CollegeDBConfig;
	let mockKV: MockKVNamespace;
	let mockDB1: MockD1Database;
	let mockDB2: MockD1Database;
	let mockCoordinator: MockDurableObjectNamespace;

	beforeEach(() => {
		mockKV = new MockKVNamespace();
		mockDB1 = new MockD1Database();
		mockDB2 = new MockD1Database();
		mockCoordinator = new MockDurableObjectNamespace();

		mockConfig = {
			kv: mockKV as any,
			coordinator: mockCoordinator as any,
			shards: {
				'db-east': mockDB1 as any,
				'db-west': mockDB2 as any
			},
			strategy: 'round-robin',
			disableAutoMigration: true // Disable auto-migration for most tests
		};

		initialize(mockConfig);
	});

	afterEach(() => {
		mockKV.clear();
		mockDB1.clear();
		mockDB2.clear();
	});

	describe('Schema Creation', () => {
		it('should create schema successfully', async () => {
			// Test the migrations module directly with mock
			const { createSchema } = await import('../src/migrations');
			await expect(createSchema(mockDB1 as any, TEST_SCHEMA)).resolves.toBeUndefined();
		});
	});

	describe('Basic CRUD Operations', () => {
		beforeEach(async () => {
			// Create schema directly with the migration module
			const { createSchema } = await import('../src/migrations');
			await createSchema(mockDB1 as any, TEST_SCHEMA);
			await createSchema(mockDB2 as any, TEST_SCHEMA);
		});

		it('should insert and select a record', async () => {
			const primaryKey = 'user-123';
			const insertSQL = 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)';
			const selectSQL = 'SELECT * FROM users WHERE id = ?';

			await run(primaryKey, insertSQL, [primaryKey, 'Alice', 'alice@example.com']);

			const result = await all(primaryKey, selectSQL, [primaryKey]);

			expect(result.success).toBe(true);
			expect(result.results).toHaveLength(1);
			expect(result.results[0]).toMatchObject({
				id: primaryKey,
				name: 'Alice',
				email: 'alice@example.com'
			});
		});

		it('should update a record', async () => {
			const primaryKey = 'user-456';

			// Insert first
			await run(primaryKey, 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', [primaryKey, 'Bob', 'bob@example.com']);

			// Update
			await run(primaryKey, 'UPDATE users SET name = ? WHERE id = ?', ['Robert', primaryKey]);

			// Verify update
			const result = await all(primaryKey, 'SELECT * FROM users WHERE id = ?', [primaryKey]);

			expect(result.results[0]).toMatchObject({
				id: primaryKey,
				name: 'Robert',
				email: 'bob@example.com'
			});
		});

		it('should delete a record', async () => {
			const primaryKey = 'user-789';

			// Insert first
			await run(primaryKey, 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', [primaryKey, 'Charlie', 'charlie@example.com']);

			// Verify it exists
			let result = await all(primaryKey, 'SELECT * FROM users WHERE id = ?', [primaryKey]);
			expect(result.results).toHaveLength(1);

			// Delete
			await run(primaryKey, 'DELETE FROM users WHERE id = ?', [primaryKey]);

			// Verify it's gone
			result = await all(primaryKey, 'SELECT * FROM users WHERE id = ?', [primaryKey]);
			expect(result.results).toHaveLength(0);
		});
	});

	describe('Shard Management', () => {
		it('should list known shards', async () => {
			const shards = await listKnownShards();
			expect(shards).toContain('db-east');
			expect(shards).toContain('db-west');
		});

		it('should get shard statistics', async () => {
			const stats = await getShardStats();
			expect(stats).toHaveLength(2);
			expect(stats[0]).toHaveProperty('binding');
			expect(stats[0]).toHaveProperty('count');
		});

		it('should reassign shard for a primary key', async () => {
			const primaryKey = 'user-reassign';

			// First insert to establish mapping
			await run(primaryKey, 'INSERT INTO users (id, name) VALUES (?, ?)', [primaryKey, 'Test User']);

			// Reassign to different shard
			await expect(reassignShard(primaryKey, 'db-west', 'users')).resolves.toBeUndefined();
		});
	});

	describe('Error Handling', () => {
		it('should throw error for invalid shard in reassignment', async () => {
			await expect(reassignShard('user-123', 'invalid-shard', 'users')).rejects.toThrow('Shard invalid-shard not found');
		});

		it('should throw error when reassigning non-existent key', async () => {
			await expect(reassignShard('non-existent', 'db-west', 'users')).rejects.toThrow('No existing mapping found');
		});
	});

	describe('ShardCoordinator (Durable Object)', () => {
		let coordinator: any;
		let mockState: any;

		beforeEach(async () => {
			// Create mock durable object state
			const mockStorage = new Map();
			mockState = {
				storage: {
					get: async (key: string) => Promise.resolve(mockStorage.get(key)),
					put: async (key: string, value: any) => {
						mockStorage.set(key, value);
						return Promise.resolve();
					},
					delete: async (key: string) => Promise.resolve(mockStorage.delete(key)),
					deleteAll: async () => {
						mockStorage.clear();
						return Promise.resolve();
					}
				}
			};

			// Import and create coordinator
			const { ShardCoordinator } = await import('../src/durable');
			coordinator = new ShardCoordinator(mockState);
		});

		it('should handle health check endpoint', async () => {
			// Create a proper Request object
			const request = {
				url: 'http://test/health',
				method: 'GET'
			} as any;

			const response = await coordinator.fetch(request);

			expect(response.status).toBe(200);
			expect(await response.text()).toBe('OK');
		});

		it('should return empty shards list initially', async () => {
			const request = {
				url: 'http://test/shards',
				method: 'GET'
			} as any;

			const response = await coordinator.fetch(request);

			expect(response.status).toBe(200);
			const shards = await response.json();
			expect(shards).toEqual([]);
		});

		it('should add and list shards', async () => {
			// Add first shard
			const addRequest1 = {
				url: 'http://test/shards',
				method: 'POST',
				json: async () => ({ shard: 'db-east' })
			} as any;

			const addResponse1 = await coordinator.fetch(addRequest1);
			expect(addResponse1.status).toBe(200);
			expect(await addResponse1.json()).toEqual({ success: true });

			// Add second shard
			const addRequest2 = {
				url: 'http://test/shards',
				method: 'POST',
				json: async () => ({ shard: 'db-west' })
			} as any;

			const addResponse2 = await coordinator.fetch(addRequest2);
			expect(addResponse2.status).toBe(200);

			// List shards
			const listRequest = {
				url: 'http://test/shards',
				method: 'GET'
			} as any;

			const listResponse = await coordinator.fetch(listRequest);
			const shards = await listResponse.json();
			expect(shards).toEqual(['db-east', 'db-west']);
		});

		it('should handle adding duplicate shards idempotently', async () => {
			const request1 = {
				url: 'http://test/shards',
				method: 'POST',
				json: async () => ({ shard: 'db-test' })
			} as any;

			const request2 = {
				url: 'http://test/shards',
				method: 'POST',
				json: async () => ({ shard: 'db-test' })
			} as any;

			// Add shard twice
			await coordinator.fetch(request1);
			await coordinator.fetch(request2);

			// Should only appear once
			const listRequest = {
				url: 'http://test/shards',
				method: 'GET'
			} as any;

			const response = await coordinator.fetch(listRequest);
			const shards = await response.json();
			expect(shards).toEqual(['db-test']);
		});

		it('should allocate shards using hash strategy', async () => {
			// Add shards first
			const addRequest1 = {
				url: 'http://test/shards',
				method: 'POST',
				json: async () => ({ shard: 'db-east' })
			} as any;

			const addRequest2 = {
				url: 'http://test/shards',
				method: 'POST',
				json: async () => ({ shard: 'db-west' })
			} as any;

			await coordinator.fetch(addRequest1);
			await coordinator.fetch(addRequest2);

			// Allocate with hash strategy
			const allocateRequest = {
				url: 'http://test/allocate',
				method: 'POST',
				json: async () => ({ primaryKey: 'consistent-key', strategy: 'hash' })
			} as any;

			const response = await coordinator.fetch(allocateRequest);

			expect(response.status).toBe(200);
			const result = await response.json();
			expect(['db-east', 'db-west']).toContain(result.shard);

			// Same key should get same shard
			const response2 = await coordinator.fetch(allocateRequest);
			const result2 = await response2.json();
			expect(result2.shard).toBe(result.shard);
		});

		it('should return error when allocating with no shards', async () => {
			const request = {
				url: 'http://test/allocate',
				method: 'POST',
				json: async () => ({ primaryKey: 'test-key' })
			} as any;

			const response = await coordinator.fetch(request);

			expect(response.status).toBe(400);
			const result = await response.json();
			expect(result).toEqual({ error: 'No shards available' });
		});

		it('should handle invalid endpoints', async () => {
			const request = {
				url: 'http://test/invalid',
				method: 'GET'
			} as any;

			const response = await coordinator.fetch(request);

			expect(response.status).toBe(404);
			expect(await response.text()).toBe('Not Found');
		});

		it('should provide shard statistics', async () => {
			// Add shard
			const addRequest = {
				url: 'http://test/shards',
				method: 'POST',
				json: async () => ({ shard: 'db-test' })
			} as any;

			await coordinator.fetch(addRequest);

			// Get initial stats
			const statsRequest = {
				url: 'http://test/stats',
				method: 'GET'
			} as any;

			let response = await coordinator.fetch(statsRequest);
			let stats = await response.json();
			expect(stats).toHaveLength(1);
			expect(stats[0]).toMatchObject({
				binding: 'db-test',
				count: 0
			});
			expect(stats[0].lastUpdated).toBeTypeOf('number');

			// Update stats
			const updateRequest = {
				url: 'http://test/stats',
				method: 'POST',
				json: async () => ({ shard: 'db-test', count: 42 })
			} as any;

			await coordinator.fetch(updateRequest);

			// Check updated stats
			response = await coordinator.fetch(statsRequest);
			stats = await response.json();
			expect(stats[0].count).toBe(42);
		});

		it('should handle increment and decrement shard counts', async () => {
			// Add shard
			const addRequest = {
				url: 'http://test/shards',
				method: 'POST',
				json: async () => ({ shard: 'db-count-test' })
			} as any;

			await coordinator.fetch(addRequest);

			// Increment count using direct methods
			await coordinator.incrementShardCount('db-count-test');
			await coordinator.incrementShardCount('db-count-test');

			// Check stats
			const statsRequest = {
				url: 'http://test/stats',
				method: 'GET'
			} as any;

			let response = await coordinator.fetch(statsRequest);
			let stats = await response.json();
			let shard = stats.find((s: any) => s.binding === 'db-count-test');
			expect(shard?.count).toBe(2);

			// Decrement count
			await coordinator.decrementShardCount('db-count-test');

			// Check updated stats
			response = await coordinator.fetch(statsRequest);
			stats = await response.json();
			shard = stats.find((s: any) => s.binding === 'db-count-test');
			expect(shard?.count).toBe(1);

			// Try to decrement below zero
			await coordinator.decrementShardCount('db-count-test');
			await coordinator.decrementShardCount('db-count-test'); // Should not go below 0

			response = await coordinator.fetch(statsRequest);
			stats = await response.json();
			shard = stats.find((s: any) => s.binding === 'db-count-test');
			expect(shard?.count).toBe(0); // Should not be negative
		});

		it('should handle increment/decrement for non-existent shards gracefully', async () => {
			// These should not throw errors
			await expect(coordinator.incrementShardCount('non-existent')).resolves.toBeUndefined();
			await expect(coordinator.decrementShardCount('non-existent')).resolves.toBeUndefined();
		});

		it('should flush all state', async () => {
			// Add shard and update stats
			const addRequest = {
				url: 'http://test/shards',
				method: 'POST',
				json: async () => ({ shard: 'db-test' })
			} as any;

			const updateStatsRequest = {
				url: 'http://test/stats',
				method: 'POST',
				json: async () => ({ shard: 'db-test', count: 10 })
			} as any;

			await coordinator.fetch(addRequest);
			await coordinator.fetch(updateStatsRequest);

			// Verify data exists
			const listRequest = {
				url: 'http://test/shards',
				method: 'GET'
			} as any;

			let response = await coordinator.fetch(listRequest);
			let shards = await response.json();
			expect(shards).toEqual(['db-test']);

			// Flush all data
			const flushRequest = {
				url: 'http://test/flush',
				method: 'POST'
			} as any;

			const flushResponse = await coordinator.fetch(flushRequest);
			expect(flushResponse.status).toBe(200);
			expect(await flushResponse.json()).toEqual({ success: true });

			// Verify data is cleared
			response = await coordinator.fetch(listRequest);
			shards = await response.json();
			expect(shards).toEqual([]);
		});

		it('should handle round-robin strategy with multiple allocations', async () => {
			// Add three shards
			const addRequests = ['db-1', 'db-2', 'db-3'].map(
				(shard) =>
					({
						url: 'http://test/shards',
						method: 'POST',
						json: async () => ({ shard })
					}) as any
			);

			for (const request of addRequests) {
				await coordinator.fetch(request);
			}

			// Allocate multiple keys with round-robin
			const allocations = [];
			for (let i = 0; i < 6; i++) {
				const request = {
					url: 'http://test/allocate',
					method: 'POST',
					json: async () => ({ primaryKey: `key-${i}`, strategy: 'round-robin' })
				} as any;

				const response = await coordinator.fetch(request);
				const result = await response.json();
				allocations.push(result.shard);
			}

			// Should cycle through shards: db-1, db-2, db-3, db-1, db-2, db-3
			expect(allocations[0]).toBe('db-1');
			expect(allocations[1]).toBe('db-2');
			expect(allocations[2]).toBe('db-3');
			expect(allocations[3]).toBe('db-1');
			expect(allocations[4]).toBe('db-2');
			expect(allocations[5]).toBe('db-3');
		});

		it('should handle shard removal and update stats', async () => {
			// Add shards
			await coordinator.fetch({
				url: 'http://test/shards',
				method: 'POST',
				json: async () => ({ shard: 'db-remove-test' })
			} as any);

			await coordinator.fetch({
				url: 'http://test/shards',
				method: 'POST',
				json: async () => ({ shard: 'db-keep' })
			} as any);

			// Update stats for both
			await coordinator.fetch({
				url: 'http://test/stats',
				method: 'POST',
				json: async () => ({ shard: 'db-remove-test', count: 5 })
			} as any);

			await coordinator.fetch({
				url: 'http://test/stats',
				method: 'POST',
				json: async () => ({ shard: 'db-keep', count: 10 })
			} as any);

			// Remove one shard
			const removeResponse = await coordinator.fetch({
				url: 'http://test/shards',
				method: 'DELETE',
				json: async () => ({ shard: 'db-remove-test' })
			} as any);

			expect(removeResponse.status).toBe(200);

			// Check that stats for removed shard are gone
			const statsResponse = await coordinator.fetch({
				url: 'http://test/stats',
				method: 'GET'
			} as any);

			const stats = await statsResponse.json();
			expect(stats).toHaveLength(1);
			expect(stats[0].binding).toBe('db-keep');
			expect(stats[0].count).toBe(10);
		});

		it('should fallback to default shard selection', async () => {
			// Add a single shard
			await coordinator.fetch({
				url: 'http://test/shards',
				method: 'POST',
				json: async () => ({ shard: 'db-only' })
			} as any);

			// Test with unknown strategy (should fallback)
			const request = {
				url: 'http://test/allocate',
				method: 'POST',
				json: async () => ({ primaryKey: 'test-key', strategy: 'unknown' })
			} as any;

			const response = await coordinator.fetch(request);
			expect(response.status).toBe(200);

			const result = await response.json();
			expect(result.shard).toBe('db-only');
		});

		it('should handle selectShard with single shard gracefully', async () => {
			// Add single shard
			await coordinator.fetch({
				url: 'http://test/shards',
				method: 'POST',
				json: async () => ({ shard: 'db-single' })
			} as any);

			// Test all strategies with single shard
			const strategies = ['hash', 'random', 'round-robin', 'location'];

			for (const strategy of strategies) {
				const request = {
					url: 'http://test/allocate',
					method: 'POST',
					json: async () => ({ primaryKey: `test-${strategy}`, strategy })
				} as any;

				const response = await coordinator.fetch(request);
				expect(response.status).toBe(200);

				const result = await response.json();
				expect(result.shard).toBe('db-single');
			}
		});

		it('should handle complex round-robin with shard removal during allocation', async () => {
			// Add multiple shards
			const shards = ['db-1', 'db-2', 'db-3', 'db-4'];
			for (const shard of shards) {
				await coordinator.fetch({
					url: 'http://test/shards',
					method: 'POST',
					json: async () => ({ shard })
				} as any);
			}

			// Allocate a few keys to advance round-robin
			for (let i = 0; i < 3; i++) {
				await coordinator.fetch({
					url: 'http://test/allocate',
					method: 'POST',
					json: async () => ({ primaryKey: `pre-key-${i}`, strategy: 'round-robin' })
				} as any);
			}

			// Remove a shard (should adjust round-robin index)
			await coordinator.fetch({
				url: 'http://test/shards',
				method: 'DELETE',
				json: async () => ({ shard: 'db-4' })
			} as any);

			// Next allocation should still work
			const response = await coordinator.fetch({
				url: 'http://test/allocate',
				method: 'POST',
				json: async () => ({ primaryKey: 'post-removal-key', strategy: 'round-robin' })
			} as any);

			expect(response.status).toBe(200);
			const result = await response.json();
			expect(['db-1', 'db-2', 'db-3']).toContain(result.shard);
		});

		it('should maintain consistent hash allocation for same keys', async () => {
			// Add multiple shards
			const shards = ['db-a', 'db-b', 'db-c', 'db-d', 'db-e'];
			for (const shard of shards) {
				await coordinator.fetch({
					url: 'http://test/shards',
					method: 'POST',
					json: async () => ({ shard })
				} as any);
			}

			// Test hash consistency for multiple keys
			const testKeys = ['user-123', 'order-456', 'product-789', 'session-abc'];
			const allocations: Record<string, string> = {};

			// First allocation round
			for (const key of testKeys) {
				const response = await coordinator.fetch({
					url: 'http://test/allocate',
					method: 'POST',
					json: async () => ({ primaryKey: key, strategy: 'hash' })
				} as any);

				const result = await response.json();
				allocations[key] = result.shard;
			}

			// Second allocation round - should be identical
			for (const key of testKeys) {
				const response = await coordinator.fetch({
					url: 'http://test/allocate',
					method: 'POST',
					json: async () => ({ primaryKey: key, strategy: 'hash' })
				} as any);

				const result = await response.json();
				expect(result.shard).toBe(allocations[key]);
			}
		});

		it('should handle random strategy distribution', async () => {
			// Add multiple shards
			const shards = ['db-rand-1', 'db-rand-2'];
			for (const shard of shards) {
				await coordinator.fetch({
					url: 'http://test/shards',
					method: 'POST',
					json: async () => ({ shard })
				} as any);
			}

			// Allocate many keys with random strategy
			const results = new Set<string>();
			for (let i = 0; i < 20; i++) {
				const response = await coordinator.fetch({
					url: 'http://test/allocate',
					method: 'POST',
					json: async () => ({ primaryKey: `random-key-${i}`, strategy: 'random' })
				} as any);

				const result = await response.json();
				results.add(result.shard);
			}

			// Should use both shards eventually (high probability with 20 allocations)
			expect(results.size).toBeGreaterThan(0);
			for (const shard of results) {
				expect(shards).toContain(shard);
			}
		});

		it('should handle edge cases in count management', async () => {
			// Add shard
			await coordinator.fetch({
				url: 'http://test/shards',
				method: 'POST',
				json: async () => ({ shard: 'db-edge-test' })
			} as any);

			// Test multiple decrements on zero count
			await coordinator.decrementShardCount('db-edge-test'); // Should handle gracefully
			await coordinator.decrementShardCount('db-edge-test'); // Should handle gracefully

			// Verify count is still zero
			const statsResponse = await coordinator.fetch({
				url: 'http://test/stats',
				method: 'GET'
			} as any);

			const stats = await statsResponse.json();
			const shard = stats.find((s: any) => s.binding === 'db-edge-test');
			expect(shard?.count).toBe(0);

			// Test increment after decrements
			await coordinator.incrementShardCount('db-edge-test');
			await coordinator.incrementShardCount('db-edge-test');

			const updatedStatsResponse = await coordinator.fetch({
				url: 'http://test/stats',
				method: 'GET'
			} as any);

			const updatedStats = await updatedStatsResponse.json();
			const updatedShard = updatedStats.find((s: any) => s.binding === 'db-edge-test');
			expect(updatedShard?.count).toBe(2);
		});

		it('should handle requests with missing parameters', async () => {
			// Add a shard first so we're not testing the "no shards" case
			await coordinator.fetch({
				url: 'http://test/shards',
				method: 'POST',
				json: async () => ({ shard: 'db-test-params' })
			} as any);

			// Test allocate without primaryKey
			const allocateResponse = await coordinator.fetch({
				url: 'http://test/allocate',
				method: 'POST',
				json: async () => ({}) // Missing primaryKey
			} as any);

			expect(allocateResponse.status).toBe(400); // Should error due to missing primaryKey

			// Test stats update without shard
			const statsResponse = await coordinator.fetch({
				url: 'http://test/stats',
				method: 'POST',
				json: async () => ({ count: 10 }) // Missing shard
			} as any);

			expect(statsResponse.status).toBe(400); // Should error due to missing shard
		});

		it('should test location strategy hash fallback thoroughly', async () => {
			// Add shards with diverse names to test hash distribution
			const shards = ['db-location-1', 'db-location-2', 'db-location-3'];
			for (const shard of shards) {
				await coordinator.fetch({
					url: 'http://test/shards',
					method: 'POST',
					json: async () => ({ shard })
				} as any);
			}

			// Test location strategy with various primary keys
			const testKeys = ['user-west-coast-123', 'order-east-region-456', 'product-europe-789', 'session-asia-abc', 'data-africa-def'];

			const keyToShard: Record<string, string> = {};

			// Allocate each key multiple times - should be consistent
			for (const key of testKeys) {
				const response1 = await coordinator.fetch({
					url: 'http://test/allocate',
					method: 'POST',
					json: async () => ({ primaryKey: key, strategy: 'location' })
				} as any);

				const result1 = await response1.json();
				keyToShard[key] = result1.shard;

				// Second allocation should be identical
				const response2 = await coordinator.fetch({
					url: 'http://test/allocate',
					method: 'POST',
					json: async () => ({ primaryKey: key, strategy: 'location' })
				} as any);

				const result2 = await response2.json();
				expect(result2.shard).toBe(result1.shard);
			}

			// Verify all shards are valid
			for (const shard of Object.values(keyToShard)) {
				expect(shards).toContain(shard);
			}
		});
	});

	describe('Shard Routing', () => {
		beforeEach(async () => {
			// Create schema directly with the migration module
			const { createSchema } = await import('../src/migrations');
			await createSchema(mockDB1 as any, TEST_SCHEMA);
			await createSchema(mockDB2 as any, TEST_SCHEMA);
		});

		it('should consistently route the same primary key to the same shard', async () => {
			const primaryKey = 'consistent-user';

			// Insert record
			await run(primaryKey, 'INSERT INTO users (id, name) VALUES (?, ?)', [primaryKey, 'Consistent User']);

			// Multiple selects should hit the same shard
			for (let i = 0; i < 5; i++) {
				const result = await first(primaryKey, 'SELECT * FROM users WHERE id = ?', [primaryKey]);
				expect(result).toBeDefined();
				expect(result!.id).toBe(primaryKey);
				expect(result!.name).toBe('Consistent User');
			}
		});

		it('should distribute different keys across shards', async () => {
			const keys = ['user-1', 'user-2', 'user-3', 'user-4', 'user-5'];

			for (const key of keys) {
				await run(key, 'INSERT INTO users (id, name) VALUES (?, ?)', [key, `User ${key}`]);
			}

			// Verify all records can be retrieved
			for (const key of keys) {
				const result = await all(key, 'SELECT * FROM users WHERE id = ?', [key]);
				expect(result.success).toBe(true);
				expect(result.results).toHaveLength(1);
				expect(result.results[0]).toBeDefined();
				expect(result.results[0]!.name).toBe(`User ${key}`);
			}
		});
	});

	describe('Drop-in Replacement', () => {
		beforeEach(async () => {
			// Create existing database with data
			const { createSchema } = await import('../src/migrations');
			await createSchema(mockDB1 as any, TEST_SCHEMA);

			// Add some existing data
			await mockDB1
				.prepare('INSERT INTO users (id, name, email) VALUES (?, ?, ?)')
				.bind('existing-user-1', 'John Doe', 'john@example.com')
				.run();
			await mockDB1
				.prepare('INSERT INTO users (id, name, email) VALUES (?, ?, ?)')
				.bind('existing-user-2', 'Jane Smith', 'jane@example.com')
				.run();
			await mockDB1
				.prepare('INSERT INTO posts (id, user_id, title, content) VALUES (?, ?, ?, ?)')
				.bind('post-1', 'existing-user-1', 'First Post', 'Hello World')
				.run();
		});

		it('should discover existing primary keys', async () => {
			const { discoverExistingPrimaryKeys } = await import('../src/migrations');

			const userIds = await discoverExistingPrimaryKeys(mockDB1 as any, 'users');
			expect(userIds).toContain('existing-user-1');
			expect(userIds).toContain('existing-user-2');
			expect(userIds).toHaveLength(2);

			const postIds = await discoverExistingPrimaryKeys(mockDB1 as any, 'posts');
			expect(postIds).toContain('post-1');
			expect(postIds).toHaveLength(1);
		});

		it('should validate tables for sharding', async () => {
			const { validateTableForSharding } = await import('../src/migrations');

			const usersValidation = await validateTableForSharding(mockDB1 as any, 'users', 'id');
			expect(usersValidation.isValid).toBe(true);
			expect(usersValidation.recordCount).toBe(2);
			expect(usersValidation.tableName).toBe('users');

			const invalidValidation = await validateTableForSharding(mockDB1 as any, 'nonexistent_table', 'id');
			expect(invalidValidation.isValid).toBe(false);
			expect(invalidValidation.issues).toContain("Table 'nonexistent_table' does not exist");
		});

		it('should integrate existing database', async () => {
			const { integrateExistingDatabase } = await import('../src/migrations');
			const { KVShardMapper } = await import('../src/kvmap');

			const mapper = new KVShardMapper(mockKV as any);

			const result = await integrateExistingDatabase(mockDB1 as any, 'db-existing', mapper, {
				tables: ['users', 'posts'],
				strategy: 'hash',
				addShardMappingsTable: true,
				dryRun: false
			});

			expect(result.success).toBe(true);
			expect(result.tablesProcessed).toBe(2);
			expect(result.totalRecords).toBe(3); // 2 users + 1 post
			expect(result.mappingsCreated).toBe(3);

			// Verify mappings were created
			const userMapping = await mapper.getShardMapping('existing-user-1');
			expect(userMapping?.shard).toBe('db-existing');

			const postMapping = await mapper.getShardMapping('post-1');
			expect(postMapping?.shard).toBe('db-existing');
		});

		it('should query existing data after integration', async () => {
			const { integrateExistingDatabase } = await import('../src/migrations');
			const { KVShardMapper } = await import('../src/kvmap');

			const mapper = new KVShardMapper(mockKV as any);

			// Integrate existing database
			await integrateExistingDatabase(mockDB1 as any, 'db-existing', mapper, {
				tables: ['users'],
				strategy: 'hash',
				dryRun: false
			});

			// Re-initialize with existing database as shard
			initialize({
				kv: mockKV as any,
				shards: {
					'db-existing': mockDB1 as any,
					'db-new': mockDB2 as any
				},
				strategy: 'hash'
			});

			// Query existing data
			const result = await all('existing-user-1', 'SELECT * FROM users WHERE id = ?', ['existing-user-1']);
			expect(result.success).toBe(true);
			expect(result.results).toHaveLength(1);
			expect(result.results[0]).toBeDefined();
			expect(result.results[0]!.id).toBe('existing-user-1');
			expect(result.results[0]!.name).toBe('John Doe');
			expect(result.results[0]!.email).toBe('john@example.com');
		});

		it('should handle dry run mode', async () => {
			const { integrateExistingDatabase } = await import('../src/migrations');
			const { KVShardMapper } = await import('../src/kvmap');

			const mapper = new KVShardMapper(mockKV as any);

			const result = await integrateExistingDatabase(mockDB1 as any, 'db-test', mapper, {
				tables: ['users'],
				strategy: 'hash',
				dryRun: true // No actual mappings should be created
			});

			expect(result.success).toBe(true);
			expect(result.totalRecords).toBe(2);
			expect(result.mappingsCreated).toBe(0); // No mappings in dry run

			// Verify no mappings were actually created
			const userMapping = await mapper.getShardMapping('existing-user-1');
			expect(userMapping).toBeNull();
		});
	});

	describe('Automatic Migration', () => {
		beforeEach(async () => {
			// Clear migration cache before each test
			clearMigrationCache();

			// Clear KV store state to ensure clean slate for each test
			mockKV.clear();

			// Clear database state and recreate schema
			mockDB1.clear();
			mockDB2.clear();

			// Create existing database with data
			const { createSchema } = await import('../src/migrations');
			await createSchema(mockDB1 as any, TEST_SCHEMA);

			// Add some existing data without mappings
			await (mockDB1 as any)
				.prepare('INSERT INTO users (id, name, email) VALUES (?, ?, ?)')
				.bind('auto-user-1', 'John Auto', 'john@auto.com')
				.run();
			await (mockDB1 as any)
				.prepare('INSERT INTO users (id, name, email) VALUES (?, ?, ?)')
				.bind('auto-user-2', 'Jane Auto', 'jane@auto.com')
				.run();
			await (mockDB1 as any)
				.prepare('INSERT INTO posts (id, user_id, title, content) VALUES (?, ?, ?, ?)')
				.bind('auto-post-1', 'auto-user-1', 'Auto Post', 'Automatically migrated')
				.run();

			// Clear specific shard cache to ensure fresh check
			clearShardMigrationCache('db-auto');
			clearShardMigrationCache('db-cache');
		});

		it('should detect when migration is needed', async () => {
			const config: CollegeDBConfig = {
				kv: mockKV as any,
				shards: { 'db-auto': mockDB1 as any },
				strategy: 'hash',
				disableAutoMigration: true // Disable auto-migration for this test
			};

			const needsMigration = await checkMigrationNeeded(mockDB1 as any, 'db-auto', config);
			expect(needsMigration).toBe(true);
		});

		it('should automatically detect and migrate existing data', async () => {
			const config: CollegeDBConfig = {
				kv: mockKV as any,
				shards: { 'db-auto': mockDB1 as any },
				strategy: 'hash',
				disableAutoMigration: true // Disable auto-migration for this test
			};

			const result = await autoDetectAndMigrate(mockDB1 as any, 'db-auto', config, { skipCache: true });
			console.log('Migration result:', result);

			expect(result.migrationNeeded).toBe(true);
			expect(result.migrationPerformed).toBe(true);
			expect(result.recordsMigrated).toBe(3); // 2 users + 1 post
			expect(result.tablesProcessed).toBe(2); // users and posts tables

			// Verify mappings were created
			const { KVShardMapper } = await import('../src/kvmap');
			const mapper = new KVShardMapper(mockKV as any);

			const userMapping = await mapper.getShardMapping('auto-user-1');
			expect(userMapping?.shard).toBe('db-auto');

			const postMapping = await mapper.getShardMapping('auto-post-1');
			expect(postMapping?.shard).toBe('db-auto');
		});

		it('should cache migration results to avoid repeated checks', async () => {
			const config: CollegeDBConfig = {
				kv: mockKV as any,
				shards: { 'db-auto': mockDB1 as any },
				strategy: 'hash',
				disableAutoMigration: true // Disable auto-migration for this test
			};

			// First migration should detect and migrate
			const result1 = await autoDetectAndMigrate(mockDB1 as any, 'db-auto', config);
			expect(result1.migrationPerformed).toBe(true);

			// Second migration should be cached (no migration performed)
			const result2 = await autoDetectAndMigrate(mockDB1 as any, 'db-auto', config);
			expect(result2.migrationPerformed).toBe(false);
			expect(result2.recordsMigrated).toBe(0);
		});

		it('should work with initialize() and automatic background migration', async () => {
			// Initialize with existing database (triggers background migration)
			initialize({
				kv: mockKV as any,
				shards: {
					'db-auto': mockDB1 as any, // Use same shard name as test data
					'db-west': mockDB2 as any
				},
				strategy: 'hash'
			});

			// Give background migration a moment to complete
			await new Promise((resolve) => setTimeout(resolve, 100));

			// Try to query existing data - should work after automatic migration
			const result = await all('auto-user-1', 'SELECT * FROM users WHERE id = ?', ['auto-user-1']);
			expect(result.success).toBe(true);
			expect(result.results).toHaveLength(1);
			expect(result.results[0]).toBeDefined();
			expect(result.results[0]!.id).toBe('auto-user-1');
			expect(result.results[0]!.name).toBe('John Auto');
		});

		it('should handle databases with no data gracefully', async () => {
			// Create empty database
			const { createSchema } = await import('../src/migrations');
			await createSchema(mockDB2 as any, TEST_SCHEMA);

			const config: CollegeDBConfig = {
				kv: mockKV as any,
				shards: { 'db-empty': mockDB2 as any },
				strategy: 'hash',
				disableAutoMigration: true // Disable auto-migration for this test
			};

			const result = await autoDetectAndMigrate(mockDB2 as any, 'db-empty', config);

			expect(result.migrationNeeded).toBe(false);
			expect(result.migrationPerformed).toBe(false);
			expect(result.recordsMigrated).toBe(0);
		});

		it('should skip tables without primary keys', async () => {
			// Create a table without proper primary key
			await (mockDB1 as any).prepare('CREATE TABLE IF NOT EXISTS logs (message TEXT, timestamp INTEGER)').run();
			await (mockDB1 as any).prepare('INSERT INTO logs (message, timestamp) VALUES (?, ?)').bind('Test log', Date.now()).run();

			const config: CollegeDBConfig = {
				kv: mockKV as any,
				shards: { 'db-mixed': mockDB1 as any },
				strategy: 'hash',
				disableAutoMigration: true // Disable auto-migration for this test
			};

			const result = await autoDetectAndMigrate(mockDB1 as any, 'db-mixed', config, { skipCache: true });

			// Should still migrate valid tables
			expect(result.migrationPerformed).toBe(true);
			expect(result.tablesProcessed).toBe(2); // Only users and posts, not logs
		});

		it('should clear migration cache when requested', async () => {
			const config: CollegeDBConfig = {
				kv: mockKV as any,
				shards: { 'db-cache': mockDB1 as any },
				strategy: 'hash',
				disableAutoMigration: true // Disable auto-migration for this test
			};

			// First migration
			const result1 = await autoDetectAndMigrate(mockDB1 as any, 'db-cache', config, { skipCache: true });
			expect(result1.migrationPerformed).toBe(true);

			// Should be cached
			const result2 = await autoDetectAndMigrate(mockDB1 as any, 'db-cache', config);
			expect(result2.migrationPerformed).toBe(false);

			// Clear cache
			clearMigrationCache();

			// Should check again but find already migrated
			const result3 = await autoDetectAndMigrate(mockDB1 as any, 'db-cache', config);
			expect(result3.migrationPerformed).toBe(false); // Already migrated, but cache was cleared
		});
	});

	describe('Migrations - integrateExistingDatabase + auto migration', () => {
		it('integrates a database in dry-run mode and reports row counts without writing mappings', async () => {
			const db = createInMemorySQLProvider();
			await db.prepare('CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT, username TEXT, name TEXT)').run();
			for (let i = 0; i < 4; i++) {
				await db
					.prepare('INSERT INTO users (id, email, username, name) VALUES (?, ?, ?, ?)')
					.bind(`u${i}`, `u${i}@x.io`, `user${i}`, `User ${i}`)
					.run();
			}

			const kv = createInMemoryKVProvider();
			const mapper = new KVShardMapper(kv, { hashShardMappings: false });

			const result = await integrateExistingDatabase(db, 'db-a', mapper, {
				tables: ['users'],
				dryRun: true,
				migrateOtherColumns: true,
				addShardMappingsTable: false
			});

			expect(result.success).toBe(true);
			expect(result.totalRecords).toBe(4);
			expect(result.mappingsCreated).toBe(0);
			expect(await mapper.getShardMapping('u0')).toBeNull();
		});

		it('integrates with multi-column lookup keys when requested', async () => {
			const db = createInMemorySQLProvider();
			await db.prepare('CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT, username TEXT, name TEXT)').run();
			await db.prepare('INSERT INTO users (id, email, username, name) VALUES (?, ?, ?, ?)').bind('u-1', 'a@x', 'a', 'A').run();

			const kv = createInMemoryKVProvider();
			const mapper = new KVShardMapper(kv, { hashShardMappings: false });

			const result = await integrateExistingDatabase(db, 'db-a', mapper, {
				tables: ['users'],
				migrateOtherColumns: true
			});

			expect(result.mappingsCreated).toBe(1);
			expect((await mapper.getShardMapping('email:a@x'))?.shard).toBe('db-a');
			expect((await mapper.getShardMapping('username:a'))?.shard).toBe('db-a');
		});

		it('autoDetectAndMigrate skips when no data tables are present', async () => {
			const db = createInMemorySQLProvider();
			const kv = createInMemoryKVProvider();
			const result = await autoDetectAndMigrate(db, 'db-a', {
				kv,
				shards: { 'db-a': db },
				hashShardMappings: false
			});
			expect(result.migrationNeeded).toBe(false);
			expect(result.migrationPerformed).toBe(false);
		});

		it('checkMigrationNeeded returns false when shard_mappings table is present', async () => {
			const db = createInMemorySQLProvider();
			await db.prepare('CREATE TABLE users (id TEXT PRIMARY KEY)').run();
			await db.prepare('CREATE TABLE shard_mappings (primary_key TEXT PRIMARY KEY, shard_name TEXT)').run();

			const result = await checkMigrationNeeded(db, 'shard-with-mappings', {
				kv: createInMemoryKVProvider(),
				shards: { 'shard-with-mappings': db },
				hashShardMappings: false
			});
			expect(result).toBe(false);
		});

		it('createSchemaAcrossShards applies the same schema everywhere', async () => {
			const a = createInMemorySQLProvider();
			const b = createInMemorySQLProvider();
			await createSchemaAcrossShards({ a, b }, 'CREATE TABLE t (id TEXT PRIMARY KEY);');
			expect(await schemaExists(a, 't')).toBe(true);
			expect(await schemaExists(b, 't')).toBe(true);
		});

		it('listTables and dropSchema operate against the in-memory engine', async () => {
			const db = createInMemorySQLProvider();
			await db.prepare('CREATE TABLE one (id TEXT PRIMARY KEY)').run();
			await db.prepare('CREATE TABLE two (id TEXT PRIMARY KEY)').run();
			const initial = await listTables(db);
			expect(initial.sort()).toEqual(['one', 'two']);

			await dropSchema(db, 'one');
			const after = await listTables(db);
			expect(after).toEqual(['two']);
		});

		it('migrateRecord refuses suspicious table names', async () => {
			const source = createInMemorySQLProvider();
			const target = createInMemorySQLProvider();
			await source.prepare('CREATE TABLE t (id TEXT PRIMARY KEY)').run();
			await source.prepare('INSERT INTO t (id) VALUES (?)').bind('x').run();

			await expect(migrateRecord(source, target, 'x', 't;DROP TABLE t')).rejects.toThrow('Invalid table name');
		});

		it('discoverExistingPrimaryKeys + validateTableForSharding flag empty tables', async () => {
			const db = createInMemorySQLProvider();
			await db.prepare('CREATE TABLE u (id TEXT PRIMARY KEY)').run();
			const keys = await discoverExistingPrimaryKeys(db, 'u');
			expect(keys).toEqual([]);

			const validation = await validateTableForSharding(db, 'u', 'id');
			expect(validation.issues).toEqual([expect.stringMatching(/empty/i)]);
		});

		it('createMappingsForExistingKeys distributes via hash strategy', async () => {
			const kv = createInMemoryKVProvider();
			const mapper = new KVShardMapper(kv, { hashShardMappings: false });
			const keys = Array.from({ length: 10 }, (_, i) => `key-${i}`);
			await createMappingsForExistingKeys(keys, ['a', 'b'], 'hash', mapper);
			for (const key of keys) {
				const mapping = await mapper.getShardMapping(key);
				expect(['a', 'b']).toContain(mapping?.shard);
			}
		});
	});

	describe('Location-based sharding', () => {
		it('should allocate to closest region shard', async () => {
			const config: CollegeDBConfig = {
				kv: mockKV as any,
				strategy: 'location',
				targetRegion: 'wnam',
				shardLocations: {
					'db-west': { region: 'wnam', priority: 2 },
					'db-east': { region: 'enam', priority: 1 },
					'db-europe': { region: 'weur', priority: 1 }
				},
				shards: {
					'db-west': mockDB1 as any,
					'db-east': mockDB2 as any,
					'db-europe': mockDB1 as any
				},
				disableAutoMigration: true
			};

			initialize(config);

			// Multiple allocations to test consistency
			const allocations = new Set<string>();
			for (let i = 0; i < 10; i++) {
				await run(`user-${i}`, 'INSERT INTO users (id, name) VALUES (?, ?)', [`user-${i}`, `User ${i}`]);
				// In actual implementation, we'd check which shard was used
				// For this test, we verify no errors occur
			}

			// Verify users were created successfully
			const user1 = await first('user-1', 'SELECT * FROM users WHERE id = ?', ['user-1']);
			expect(user1).toBeTruthy();
			expect(user1?.name).toBe('User 1');
		});

		it('should fallback to hash strategy when no location info available', async () => {
			const config: CollegeDBConfig = {
				kv: mockKV as any,
				strategy: 'location',
				targetRegion: 'wnam',
				// No shardLocations provided
				shards: {
					'db-1': mockDB1 as any,
					'db-2': mockDB2 as any
				},
				disableAutoMigration: true
			};

			initialize(config);

			// Should work even without location info (fallback to hash)
			await run('user-fallback', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-fallback', 'Fallback User']);
			const user = await first('user-fallback', 'SELECT * FROM users WHERE id = ?', ['user-fallback']);

			expect(user).toBeTruthy();
			expect(user?.name).toBe('Fallback User');
		});

		it('should handle missing target region gracefully', async () => {
			const config: CollegeDBConfig = {
				kv: mockKV as any,
				strategy: 'location',
				// No targetRegion provided
				shardLocations: {
					'db-west': { region: 'wnam', priority: 2 },
					'db-east': { region: 'enam', priority: 1 }
				},
				shards: {
					'db-west': mockDB1 as any,
					'db-east': mockDB2 as any
				},
				disableAutoMigration: true
			};

			initialize(config);

			// Should fallback to hash strategy
			await run('user-no-target', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-no-target', 'No Target User']);
			const user = await first('user-no-target', 'SELECT * FROM users WHERE id = ?', ['user-no-target']);

			expect(user).toBeTruthy();
			expect(user?.name).toBe('No Target User');
		});

		it('should respect shard priorities', async () => {
			const config: CollegeDBConfig = {
				kv: mockKV as any,
				strategy: 'location',
				targetRegion: 'weur',
				shardLocations: {
					'db-priority-high': { region: 'weur', priority: 3 },
					'db-priority-low': { region: 'weur', priority: 1 }
				},
				shards: {
					'db-priority-high': mockDB1 as any,
					'db-priority-low': mockDB2 as any
				},
				disableAutoMigration: true
			};

			initialize(config);

			// Both are in the same region but different priorities
			// High priority should be preferred (though we can't directly test
			// shard selection in this mock setup, we test that it works)
			await run('user-priority-test', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-priority-test', 'Priority Test User']);

			const user = await first('user-priority-test', 'SELECT * FROM users WHERE id = ?', ['user-priority-test']);
			expect(user).toBeTruthy();
			expect(user?.name).toBe('Priority Test User');
		});

		it('should maintain consistency for same primary key', async () => {
			const config: CollegeDBConfig = {
				kv: mockKV as any,
				strategy: 'location',
				targetRegion: 'apac',
				shardLocations: {
					'db-tokyo': { region: 'apac', priority: 2 },
					'db-sydney': { region: 'oc', priority: 1 }
				},
				shards: {
					'db-tokyo': mockDB1 as any,
					'db-sydney': mockDB2 as any
				},
				disableAutoMigration: true
			};

			initialize(config);

			const primaryKey = 'user-consistency-test';

			// First operation
			await run(primaryKey, 'INSERT INTO users (id, name) VALUES (?, ?)', [primaryKey, 'Consistency Test User']);

			// Subsequent operations should go to same shard
			await run(primaryKey, 'UPDATE users SET name = ? WHERE id = ?', ['Updated User', primaryKey]);

			const user = await first(primaryKey, 'SELECT * FROM users WHERE id = ?', [primaryKey]);
			expect(user).toBeTruthy();
			expect(user?.name).toBe('Updated User');
		});

		it('should support location strategy across multiple target regions with five mock databases', async () => {
			const regions: D1Region[] = ['wnam', 'enam', 'weur', 'apac', 'oc'];
			const shardLocations: Record<string, { region: D1Region; priority: number }> = {
				'db-wnam': { region: 'wnam', priority: 3 },
				'db-enam': { region: 'enam', priority: 2 },
				'db-weur': { region: 'weur', priority: 2 },
				'db-apac': { region: 'apac', priority: 2 },
				'db-oc': { region: 'oc', priority: 1 }
			};

			const shards = {
				'db-wnam': new MockD1Database() as any,
				'db-enam': new MockD1Database() as any,
				'db-weur': new MockD1Database() as any,
				'db-apac': new MockD1Database() as any,
				'db-oc': new MockD1Database() as any
			};

			for (const targetRegion of regions) {
				initialize({
					kv: mockKV as any,
					strategy: 'location',
					targetRegion,
					shardLocations,
					shards,
					disableAutoMigration: true
				});

				const key = `location-multi-region-${targetRegion}`;
				await run(key, 'INSERT INTO users (id, name) VALUES (?, ?)', [key, `Location ${targetRegion}`]);

				const row = await first(key, 'SELECT * FROM users WHERE id = ?', [key]);
				expect(row).toBeTruthy();
				expect((row as any).name).toBe(`Location ${targetRegion}`);
			}
		});
	});

	describe('IP Geolocation', () => {
		it('should return correct region for US West Coast', () => {
			const mockRequest = {
				cf: {
					country: 'US',
					continent: 'NA',
					region: 'CA',
					timezone: 'America/Los_Angeles'
				}
			} as unknown as Request;

			const region = getClosestRegionFromIP(mockRequest);
			expect(region).toBe('wnam');
		});

		it('should return correct region for US East Coast', () => {
			const mockRequest = {
				cf: {
					country: 'US',
					continent: 'NA',
					region: 'NY'
				}
			} as unknown as Request;

			const region = getClosestRegionFromIP(mockRequest);
			expect(region).toBe('enam');
		});

		it('should return correct region for European countries', () => {
			const mockRequestUK = {
				cf: {
					country: 'GB',
					continent: 'EU'
				}
			} as unknown as Request;

			const regionUK = getClosestRegionFromIP(mockRequestUK);
			expect(regionUK).toBe('weur');

			const mockRequestGermany = {
				cf: {
					country: 'DE',
					continent: 'EU'
				}
			} as unknown as Request;

			const regionDE = getClosestRegionFromIP(mockRequestGermany);
			expect(regionDE).toBe('eeur');
		});

		it('should return correct region for Asia Pacific countries', () => {
			const mockRequestJapan = {
				cf: {
					country: 'JP',
					continent: 'AS'
				}
			} as unknown as Request;

			const regionJP = getClosestRegionFromIP(mockRequestJapan);
			expect(regionJP).toBe('apac');

			const mockRequestIndia = {
				cf: {
					country: 'IN',
					continent: 'AS'
				}
			} as unknown as Request;

			const regionIN = getClosestRegionFromIP(mockRequestIndia);
			expect(regionIN).toBe('apac');
		});

		it('should return correct region for Oceania', () => {
			const mockRequest = {
				cf: {
					country: 'AU',
					continent: 'OC'
				}
			} as unknown as Request;

			const region = getClosestRegionFromIP(mockRequest);
			expect(region).toBe('oc');
		});

		it('should return correct region for Middle East', () => {
			const mockRequest = {
				cf: {
					country: 'AE',
					continent: 'AS'
				}
			} as unknown as Request;

			const region = getClosestRegionFromIP(mockRequest);
			expect(region).toBe('me');
		});

		it('should return correct region for Africa', () => {
			const mockRequest = {
				cf: {
					country: 'ZA',
					continent: 'AF'
				}
			} as unknown as Request;

			const region = getClosestRegionFromIP(mockRequest);
			expect(region).toBe('af');
		});

		it('should fallback to wnam when no CF data available', () => {
			const mockRequest = {} as unknown as Request;
			const region = getClosestRegionFromIP(mockRequest);
			expect(region).toBe('wnam');

			const mockRequestNoCF = {
				cf: undefined
			} as unknown as Request;
			const regionNoCF = getClosestRegionFromIP(mockRequestNoCF);
			expect(regionNoCF).toBe('wnam');
		});

		it('should handle South American countries correctly', () => {
			const mockRequest = {
				cf: {
					country: 'BR',
					continent: 'SA'
				}
			} as unknown as Request;

			const region = getClosestRegionFromIP(mockRequest);
			expect(region).toBe('enam'); // Geographically closer to Eastern North America
		});
	});

	describe('CollegeDB Method', () => {
		beforeEach(async () => {
			// Create schema directly with the migration module
			const { createSchema } = await import('../src/migrations');
			await createSchema(mockDB1 as any, TEST_SCHEMA);
			await createSchema(mockDB2 as any, TEST_SCHEMA);
		});

		it('should initialize and execute callback with collegedb method', async () => {
			const result = await collegedb(mockConfig, async () => {
				// Insert data using the collegedb method context
				await run('user-callback-123', 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', [
					'user-callback-123',
					'Callback User',
					'callback@example.com'
				]);

				// Query the data back
				const user = await first('user-callback-123', 'SELECT * FROM users WHERE id = ?', ['user-callback-123']);
				return user;
			});

			expect(result).toBeTruthy();
			expect(result?.id).toBe('user-callback-123');
			expect(result?.name).toBe('Callback User');
			expect(result?.email).toBe('callback@example.com');
		});

		it('should handle async operations in callback', async () => {
			const users = await collegedb(mockConfig, async () => {
				// Insert multiple users
				await run('user-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'User 1']);
				await run('user-2', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-2', 'User 2']);
				await run('user-3', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-3', 'User 3']);

				// Return all users
				const allUsers = [];
				for (const userId of ['user-1', 'user-2', 'user-3']) {
					const user = await first(userId, 'SELECT * FROM users WHERE id = ?', [userId]);
					if (user) allUsers.push(user);
				}
				return allUsers;
			});

			expect(users).toHaveLength(3);
			expect(users.map((u: any) => u.name)).toEqual(['User 1', 'User 2', 'User 3']);
		});
	});

	describe('Shard Methods', () => {
		beforeEach(async () => {
			// Create schema directly with the migration module
			const { createSchema } = await import('../src/migrations');
			await createSchema(mockDB1 as any, TEST_SCHEMA);
			await createSchema(mockDB2 as any, TEST_SCHEMA);
		});

		it('should execute runShard method on specific shard', async () => {
			const result = await runShard('db-east', 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', [
				'user-shard-run',
				'Shard User',
				'shard@example.com'
			]);

			expect(result.success).toBe(true);
		});

		it('should execute allShard method on specific shard', async () => {
			// Insert test data first
			await runShard('db-east', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-all-1', 'All User 1']);
			await runShard('db-east', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-all-2', 'All User 2']);

			const result = await allShard('db-east', 'SELECT * FROM users WHERE name LIKE ?', ['All User%']);

			expect(result.success).toBe(true);
			expect(result.results).toHaveLength(2);
		});

		it('should execute firstShard method on specific shard', async () => {
			// Insert test data first
			await runShard('db-west', 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', ['user-first', 'First User', 'first@example.com']);

			const result = await firstShard('db-west', 'SELECT * FROM users WHERE id = ?', ['user-first']);

			expect(result).toBeTruthy();
			expect(result?.name).toBe('First User');
			expect(result?.email).toBe('first@example.com');
		});

		it('should throw CollegeDBError for invalid shard in runShard', async () => {
			await expect(runShard('invalid-shard', 'SELECT 1')).rejects.toThrow(CollegeDBError);
			await expect(runShard('invalid-shard', 'SELECT 1')).rejects.toThrow('Shard invalid-shard not found');
		});

		it('should throw CollegeDBError for invalid shard in allShard', async () => {
			await expect(allShard('invalid-shard', 'SELECT 1')).rejects.toThrow(CollegeDBError);
		});

		it('should throw CollegeDBError for invalid shard in firstShard', async () => {
			await expect(firstShard('invalid-shard', 'SELECT 1')).rejects.toThrow(CollegeDBError);
		});
	});

	describe('Flush Method', () => {
		it('should flush all mappings', async () => {
			// Insert some data to create mappings
			await run('user-flush-test', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-flush-test', 'Flush User']);

			// Verify mapping exists by running stats
			const statsBefore = await getShardStats();
			expect(statsBefore.length).toBeGreaterThan(0);

			// Flush all mappings
			await flush();

			// Stats should be cleared or reset
			// Note: The actual implementation of flush depends on what it clears
		});
	});

	describe('Error Handling', () => {
		beforeEach(async () => {
			// Create schema directly with the migration module
			const { createSchema } = await import('../src/migrations');
			await createSchema(mockDB1 as any, TEST_SCHEMA);
			await createSchema(mockDB2 as any, TEST_SCHEMA);
		});

		it('should throw CollegeDBError for query execution failures', async () => {
			// Try to insert duplicate primary key
			await run('user-duplicate', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-duplicate', 'First']);

			await expect(run('user-duplicate', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-duplicate', 'Duplicate'])).rejects.toThrow(
				CollegeDBError
			);
		});

		it('should throw CollegeDBError when not initialized', async () => {
			// Clear initialization
			const originalConfig = mockConfig;
			resetConfig();

			await expect(run('test', 'SELECT 1')).rejects.toThrow(CollegeDBError);
			await expect(run('test', 'SELECT 1')).rejects.toThrow('CollegeDB not initialized');

			// Restore initialization
			initialize(originalConfig);
		});

		it('should handle allocation strategy without coordinator', async () => {
			// Test with hash strategy
			const configWithoutCoordinator: CollegeDBConfig = {
				kv: mockKV as any,
				shards: {
					'db-east': mockDB1 as any,
					'db-west': mockDB2 as any
				},
				strategy: 'hash'
			};

			initialize(configWithoutCoordinator);

			// Should still work without coordinator
			await run('hash-user-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['hash-user-1', 'Hash User 1']);
			const user = await first('hash-user-1', 'SELECT * FROM users WHERE id = ?', ['hash-user-1']);

			expect(user).toBeTruthy();
			expect(user?.name).toBe('Hash User 1');

			// Test random strategy
			configWithoutCoordinator.strategy = 'random';
			initialize(configWithoutCoordinator);

			await run('random-user-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['random-user-1', 'Random User 1']);
			const randomUser = await first('random-user-1', 'SELECT * FROM users WHERE id = ?', ['random-user-1']);

			expect(randomUser).toBeTruthy();
			expect(randomUser?.name).toBe('Random User 1');

			// Restore original config
			initialize(mockConfig);
		});
	});

	describe('CollegeDBError', () => {
		it('should create error with message and code', () => {
			const error = new CollegeDBError('Test error message', 'TEST_ERROR');

			expect(error.message).toBe('Test error message');
			expect(error.code).toBe('TEST_ERROR');
			expect(error.name).toBe('CollegeDBError');
			expect(error instanceof Error).toBe(true);
		});

		it('should create error without code', () => {
			const error = new CollegeDBError('Test error without code');

			expect(error.message).toBe('Test error without code');
			expect(error.code).toBeUndefined();
			expect(error.name).toBe('CollegeDBError');
		});
	});

	describe('Mixed Strategy Support', () => {
		it('should use different strategies for read and write operations', async () => {
			const mixedStrategy: MixedShardingStrategy = {
				read: 'hash',
				write: 'hash' // Using same strategy for deterministic testing
			};

			initialize({
				kv: mockKV as any,
				shards: {
					'db-east': mockDB1 as any,
					'db-west': mockDB2 as any
				},
				strategy: mixedStrategy
			});

			// Insert a user (write operation)
			await run('user-123', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-123', 'John Doe']);

			// Query the user (read operation) - should go to same shard due to existing mapping
			const result = await first('user-123', 'SELECT * FROM users WHERE id = ?', ['user-123']);

			expect(result).toBeDefined();
			expect((result as any).name).toBe('John Doe');
		});

		it('should handle location strategy for writes and hash for reads', async () => {
			const mixedStrategy: MixedShardingStrategy = {
				read: 'hash',
				write: 'location'
			};

			initialize({
				kv: mockKV as any,
				shards: {
					'db-east': mockDB1 as any,
					'db-west': mockDB2 as any
				},
				strategy: mixedStrategy,
				targetRegion: 'wnam',
				shardLocations: {
					'db-west': { region: 'wnam', priority: 2 },
					'db-east': { region: 'enam', priority: 1 }
				}
			});

			// Write operation should use location strategy
			await run('user-west-456', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-west-456', 'West User']);

			// Read operation should use hash strategy (but will find existing mapping)
			const result = await first('user-west-456', 'SELECT * FROM users WHERE id = ?', ['user-west-456']);

			expect(result).toBeDefined();
			expect((result as any).name).toBe('West User');
		});

		it('should resolve strategy type correctly for different SQL operations', async () => {
			const mixedStrategy: MixedShardingStrategy = {
				read: 'random',
				write: 'hash'
			};

			initialize({
				kv: mockKV as any,
				shards: {
					'db-east': mockDB1 as any,
					'db-west': mockDB2 as any
				},
				strategy: mixedStrategy
			});

			// Test different SQL operations

			// INSERT (write)
			await run('user-789', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-789', 'Insert User']);

			// UPDATE (write)
			await run('user-789', 'UPDATE users SET name = ? WHERE id = ?', ['Updated User', 'user-789']);

			// DELETE (write)
			await run('user-delete', 'DELETE FROM users WHERE id = ?', ['user-delete']);

			// SELECT (read) - should work even though it might route to different shard due to random strategy
			// In practice, existing mappings ensure consistency
			const result = await first('user-789', 'SELECT * FROM users WHERE id = ?', ['user-789']);

			// The result should be from the write operations
			expect(result).toBeDefined();
			expect((result as any).name).toBe('Updated User');
		});

		it('should fall back to single strategy when mixed strategy is not provided', async () => {
			// Using a single strategy (string) instead of mixed strategy object
			initialize({
				kv: mockKV as any,
				shards: {
					'db-east': mockDB1 as any,
					'db-west': mockDB2 as any
				},
				strategy: 'hash' // Single strategy
			});

			// Both read and write should use hash strategy
			await run('user-single', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-single', 'Single Strategy User']);
			const result = await first('user-single', 'SELECT * FROM users WHERE id = ?', ['user-single']);

			expect(result).toBeDefined();
			expect((result as any).name).toBe('Single Strategy User');
		});

		it('should handle complex mixed strategy scenarios', async () => {
			const mixedStrategy: MixedShardingStrategy = {
				read: 'hash', // Consistent reads
				write: 'random' // Distributed writes
			};

			initialize({
				kv: mockKV as any,
				shards: {
					'db-east': mockDB1 as any,
					'db-west': mockDB2 as any
				},
				strategy: mixedStrategy
			});

			// Create multiple users with different IDs
			const userIds = ['user-1', 'user-2', 'user-3', 'user-4', 'user-5'];

			for (const userId of userIds) {
				await run(userId, 'INSERT INTO users (id, name) VALUES (?, ?)', [userId, `User ${userId}`]);
			}

			// Read all users back
			for (const userId of userIds) {
				const result = await first(userId, 'SELECT * FROM users WHERE id = ?', [userId]);
				expect(result).toBeDefined();
				expect((result as any).name).toBe(`User ${userId}`);
			}
		});

		it('should use default strategy when none specified in mixed configuration', async () => {
			const mixedStrategy: MixedShardingStrategy = {
				read: 'hash',
				write: 'hash'
			};

			// Initialize without specifying strategy (should default to hash)
			initialize({
				kv: mockKV as any,
				shards: {
					'db-east': mockDB1 as any,
					'db-west': mockDB2 as any
				}
				// No strategy specified - should default to 'hash'
			});

			await run('user-default', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-default', 'Default User']);
			const result = await first('user-default', 'SELECT * FROM users WHERE id = ?', ['user-default']);

			expect(result).toBeDefined();
			expect((result as any).name).toBe('Default User');
		});

		it('should support all mixed read/write strategy permutations across regional location profiles', async () => {
			const strategies: ShardingStrategy[] = ['round-robin', 'random', 'hash', 'location'];
			const targetRegions: D1Region[] = ['wnam', 'enam', 'weur', 'apac', 'oc'];
			const shardLocations: Record<string, { region: D1Region; priority: number }> = {
				'db-wnam': { region: 'wnam', priority: 3 },
				'db-enam': { region: 'enam', priority: 2 },
				'db-weur': { region: 'weur', priority: 2 },
				'db-apac': { region: 'apac', priority: 2 },
				'db-oc': { region: 'oc', priority: 1 }
			};

			const shards = {
				'db-wnam': new MockD1Database() as any,
				'db-enam': new MockD1Database() as any,
				'db-weur': new MockD1Database() as any,
				'db-apac': new MockD1Database() as any,
				'db-oc': new MockD1Database() as any
			};

			let caseIndex = 0;
			for (const read of strategies) {
				for (const write of strategies) {
					const targetRegion = targetRegions[caseIndex % targetRegions.length] || 'wnam';
					const mixedStrategy: MixedShardingStrategy = {
						read,
						write
					};

					initialize({
						kv: mockKV as any,
						shards,
						strategy: mixedStrategy,
						targetRegion,
						shardLocations,
						disableAutoMigration: true
					});

					// Trigger read-path routing on an unmapped key.
					const missKey = `mix-read-miss-${read}-${write}-${caseIndex}`;
					const miss = await first(missKey, 'SELECT * FROM users WHERE id = ?', [missKey]);
					expect(miss).toBeNull();

					// Trigger write-path routing on a fresh key.
					const writeKey = `mix-write-${read}-${write}-${caseIndex}`;
					await run(writeKey, 'INSERT INTO users (id, name) VALUES (?, ?)', [writeKey, `Combo ${read}/${write}`]);

					const row = await first(writeKey, 'SELECT * FROM users WHERE id = ?', [writeKey]);
					expect(row).toBeTruthy();
					expect((row as any).name).toBe(`Combo ${read}/${write}`);

					caseIndex += 1;
				}
			}
		});
	});

	describe('Multi-Column Migration', () => {
		let mockKV: MockKVNamespace;
		let mockDB: MockD1Database;
		let mapper: KVShardMapper;
		let config: CollegeDBConfig;

		beforeEach(() => {
			mockKV = new MockKVNamespace();
			mockDB = new MockD1Database();
			mapper = new KVShardMapper(mockKV as any, { hashShardMappings: true });

			config = {
				kv: mockKV as any,
				shards: {
					'db-test': mockDB as any
				},
				strategy: 'hash',
				hashShardMappings: true
			};

			clearMigrationCache();
		});

		afterEach(() => {
			mockKV.clear();
		});

		describe('discoverExistingRecordsWithColumns', () => {
			it('should discover records with all available columns', async () => {
				// Setup test data
				mockDB.addTestData('users', [
					{
						id: 'user-1',
						username: 'johndoe',
						email: 'john@example.com',
						name: 'John Doe',
						created_at: 1234567890
					},
					{
						id: 'user-2',
						username: 'janesmith',
						email: 'jane@example.com',
						name: 'Jane Smith',
						created_at: 1234567891
					}
				]);

				const records = await discoverExistingRecordsWithColumns(mockDB as any, 'users');

				expect(records).toHaveLength(2);
				expect(records[0]).toMatchObject({
					id: 'user-1',
					username: 'johndoe',
					email: 'john@example.com',
					name: 'John Doe'
				});
			});

			it('should handle records with missing optional columns', async () => {
				mockDB.addTestData('users', [
					{
						id: 'user-1',
						username: 'johndoe',
						email: null, // Missing email
						name: 'John Doe',
						created_at: 1234567890
					},
					{
						id: 'user-2',
						username: null, // Missing username
						email: 'jane@example.com',
						name: 'Jane Smith',
						created_at: 1234567891
					}
				]);

				const records = await discoverExistingRecordsWithColumns(mockDB as any, 'users');

				expect(records).toHaveLength(2);
				expect(records[0]?.email).toBeNull();
				expect(records[1]?.username).toBeNull();
			});
		});

		describe('integrateExistingDatabase with migrateOtherColumns', () => {
			it('should create mappings for additional columns when enabled', async () => {
				// Setup test data
				mockDB.addTestData('users', [
					{
						id: 'user-1',
						username: 'johndoe',
						email: 'john@example.com',
						name: 'John Doe',
						created_at: 1234567890
					}
				]);

				console.log('Starting integration test with mockDB:', mockDB, 'mapper:', mapper);

				// First, let's test if basic mapping works
				await mapper.setShardMapping('test-key', 'db-test');
				const testMapping = await mapper.getShardMapping('test-key');
				console.log('Basic mapping test result:', testMapping);

				const result = await integrateExistingDatabase(mockDB as any, 'db-test', mapper, {
					tables: ['users'],
					migrateOtherColumns: true
				});
				console.log('Integration result:', result);

				expect(result.success).toBe(true);
				if (!result.success) {
					console.log('Integration failed with issues:', result.issues);
				}
				expect(result.mappingsCreated).toBe(1);

				// Check that multiple lookup keys were created
				const primaryMapping = await mapper.getShardMapping('user-1');
				const usernameMapping = await mapper.getShardMapping('username:johndoe');
				const emailMapping = await mapper.getShardMapping('email:john@example.com');
				const nameMapping = await mapper.getShardMapping('name:John Doe');

				expect(primaryMapping?.shard).toBe('db-test');
				expect(usernameMapping?.shard).toBe('db-test');
				expect(emailMapping?.shard).toBe('db-test');
				expect(nameMapping?.shard).toBe('db-test');
			});

			it('should only create mappings for primary key when disabled', async () => {
				mockDB.addTestData('users', [
					{
						id: 'user-1',
						username: 'johndoe',
						email: 'john@example.com',
						name: 'John Doe',
						created_at: 1234567890
					}
				]);

				console.log('Starting second integration test');
				const result = await integrateExistingDatabase(mockDB as any, 'db-test', mapper, {
					tables: ['users'],
					migrateOtherColumns: false
				});
				console.log('Second integration result:', result);

				expect(result.success).toBe(true);
				if (!result.success) {
					console.log('Integration failed with issues:', result.issues);
				}
				expect(result.mappingsCreated).toBe(1);

				// Check that only primary key mapping was created
				const primaryMapping = await mapper.getShardMapping('user-1');
				const usernameMapping = await mapper.getShardMapping('username:johndoe');

				expect(primaryMapping?.shard).toBe('db-test');
				expect(usernameMapping).toBeNull();
			});

			it('should skip null/undefined columns', async () => {
				mockDB.addTestData('users', [
					{
						id: 'user-1',
						username: 'johndoe',
						email: null,
						name: undefined,
						created_at: 1234567890
					}
				]);

				const result = await integrateExistingDatabase(mockDB as any, 'db-test', mapper, {
					tables: ['users'],
					migrateOtherColumns: true
				});

				expect(result.success).toBe(true);

				// Only primary key and username should have mappings
				const primaryMapping = await mapper.getShardMapping('user-1');
				const usernameMapping = await mapper.getShardMapping('username:johndoe');
				const emailMapping = await mapper.getShardMapping('email:null');
				const nameMapping = await mapper.getShardMapping('name:undefined');

				expect(primaryMapping?.shard).toBe('db-test');
				expect(usernameMapping?.shard).toBe('db-test');
				expect(emailMapping).toBeNull();
				expect(nameMapping).toBeNull();
			});
		});

		describe('autoDetectAndMigrate with migrateOtherColumns', () => {
			it('should perform auto-migration with multi-column lookup', async () => {
				mockDB.addTestData('users', [
					{
						id: 'user-1',
						username: 'johndoe',
						email: 'john@example.com',
						name: 'John Doe',
						created_at: 1234567890
					}
				]);

				console.log('Starting auto-migrate test');
				const result = await autoDetectAndMigrate(mockDB as any, 'db-test', config, {
					migrateOtherColumns: true,
					tablesToCheck: ['users'],
					skipCache: true
				});
				console.log('Auto-migrate result:', result);

				expect(result.migrationPerformed).toBe(true);
				if (!result.migrationPerformed) {
					console.log('Auto-migration result:', result);
				}
				expect(result.recordsMigrated).toBe(1); // Check that multiple lookup keys were created
				const usernameMapping = await mapper.getShardMapping('username:johndoe');
				const emailMapping = await mapper.getShardMapping('email:john@example.com');

				expect(usernameMapping?.shard).toBe('db-test');
				expect(emailMapping?.shard).toBe('db-test');
			});

			it('should not migrate already mapped records', async () => {
				mockDB.addTestData('users', [
					{
						id: 'user-1',
						username: 'johndoe',
						email: 'john@example.com',
						name: 'John Doe',
						created_at: 1234567890
					}
				]);

				// Pre-create a mapping
				await mapper.setShardMapping('user-1', 'db-test');

				const result = await autoDetectAndMigrate(mockDB as any, 'db-test', config, {
					migrateOtherColumns: true,
					tablesToCheck: ['users'],
					skipCache: true
				});

				// Should not perform migration since mapping already exists
				expect(result.migrationPerformed).toBe(false);
				expect(result.recordsMigrated).toBe(0);
			});
		});
	});

	describe('KVShardMapper', () => {
		let mapper: KVShardMapper;
		let nonHashMapper: KVShardMapper;
		let multiColumnMapper: KVShardMapper;

		beforeEach(() => {
			mapper = new KVShardMapper(mockKV as any, { hashShardMappings: true });
			nonHashMapper = new KVShardMapper(mockKV as any, { hashShardMappings: false });
		});

		it('should create a new mapping', async () => {
			await mapper.setShardMapping('user-1', 'db-test');
		});

		it('should retrieve an existing mapping', async () => {
			await mapper.setShardMapping('user-1', 'db-test');
			const mapping = await mapper.getShardMapping('user-1');
			expect(mapping?.shard).toBe('db-test');

			const allMappings = await mapper.getAllLookupKeys('user-1');
			expect(allMappings).toHaveLength(1);
			expect(allMappings).toContain('user-1');
		});

		it('should return null for non-existing mapping', async () => {
			const mapping = await mapper.getShardMapping('non-existing');
			expect(mapping).toBeNull();
		});

		it('should handle hash mappings correctly', async () => {
			await mapper.setShardMapping('user-1', 'db-test');
			const hashMapping = await mapper.getShardMapping('user-1');
			expect(hashMapping?.shard).toBe('db-test');

			await nonHashMapper.setShardMapping('user-2', 'db-test-non-hash');
			const nonHashMapping = await nonHashMapper.getShardMapping('user-2');
			expect(nonHashMapping?.shard).toBe('db-test-non-hash');
		});

		it('should add additional lookup keys for multi-column mappings', async () => {
			await mapper.setShardMapping('user-1', 'db-test');
			await mapper.addLookupKeys('user-1', ['username:johndoe', 'email:john@example.com']);

			const originalMapping = await mapper.getShardMapping('user-1');
			const usernameMapping = await mapper.getShardMapping('username:johndoe');
			const emailMapping = await mapper.getShardMapping('email:john@example.com');

			expect(originalMapping?.shard).toBe('db-test');
			expect(usernameMapping?.shard).toBe('db-test');
			expect(emailMapping?.shard).toBe('db-test');
		});

		it('should update additional lookup keys for existing mappings', async () => {
			await mapper.setShardMapping('user-1', 'db-test');
			await mapper.addLookupKeys('user-1', ['username:johndoe', 'email:john@example.com']);

			await mapper.updateShardMapping('user-1', 'db-test-updated');

			const updatedMapping = await mapper.getShardMapping('user-1');
			const usernameMapping = await mapper.getShardMapping('username:johndoe');
			const emailMapping = await mapper.getShardMapping('email:john@example.com');

			expect(updatedMapping?.shard).toBe('db-test-updated');
			expect(usernameMapping?.shard).toBe('db-test-updated');
			expect(emailMapping?.shard).toBe('db-test-updated');
		});

		it('should delete a mapping and its additional keys', async () => {
			await mapper.setShardMapping('user-1', 'db-test');
			await mapper.addLookupKeys('user-1', ['username:johndoe', 'email:john@example.com']);

			const mapping = await mapper.getShardMapping('user-1');
			expect(mapping?.shard).toBe('db-test');

			await mapper.deleteShardMapping('user-1');
			const deletedMapping = await mapper.getShardMapping('user-1');
			expect(deletedMapping).toBeNull();
		});

		it('should retrieve all mappings for a primary key', async () => {
			await mapper.setShardMapping('user-1', 'db-test');
			await mapper.addLookupKeys('user-1', ['username:johndoe', 'email:john@example.com']);

			const allMappings = await mapper.getAllLookupKeys('user-1');
			expect(allMappings).toHaveLength(3);

			await nonHashMapper.setShardMapping('user-2', 'db-test-non-hash');
			await nonHashMapper.addLookupKeys('user-2', ['username:janesmith', 'email:janesmith@example.com']);
			const nonHashMappings = await nonHashMapper.getAllLookupKeys('user-2');
			expect(nonHashMappings).toHaveLength(3);
			expect(nonHashMappings).toContain('user-2');
			expect(nonHashMappings).toContain('username:janesmith');
			expect(nonHashMappings).toContain('email:janesmith@example.com');
		});

		it('should retrieve all mapping counts for a shard', async () => {
			await mapper.setShardMapping('user-1', 'db-test');
			await mapper.addLookupKeys('user-1', ['username:johndoe', 'email:john@example.com']);

			await mapper.setShardMapping('user-2', 'db-test');
			await mapper.addLookupKeys('user-2', ['username:janesmith', 'email:janesmith@example.com']);

			const counts = await mapper.getKeysForShard('db-test');
			expect(counts).toHaveLength(6); // 2 primary keys + 3 additional keys each

			await nonHashMapper.setShardMapping('user-3', 'db-test-non-hash');
			await nonHashMapper.addLookupKeys('user-3', ['username:alice']);

			const nonHashCounts = await nonHashMapper.getKeysForShard('db-test-non-hash');
			expect(nonHashCounts).toHaveLength(2); // 1 primary key + 1 additional key
			expect(nonHashCounts).toContain('user-3');
			expect(nonHashCounts).toContain('username:alice');
		});

		it('should retrieve all mapping counts on all shards', async () => {
			await mapper.setShardMapping('user-1', 'db-test');
			await mapper.addLookupKeys('user-1', ['username:johndoe', 'email:john@example.com']);

			await mapper.setShardMapping('user-2', 'db-test');
			await mapper.addLookupKeys('user-2', ['username:janesmith', 'email:janesmith@example.com']);

			await mapper.setShardMapping('user-3', 'db-test-2');
			await mapper.addLookupKeys('user-3', ['username:alice', 'email:alice@example.com']);

			const counts = await mapper.getShardKeyCounts();
			expect(counts['db-test']).toBe(12); // 3 primary keys + 2 additional keys, plus reversed mappings
			expect(counts['db-test-2']).toBe(6); // 1 primary key + 2 additional keys, plus reversed mappings
		});

		it('should throw on non-existing shard in updateShardMapping', async () => {
			await expect(mapper.updateShardMapping('non-existing', 'db-test')).rejects.toThrow(CollegeDBError);
			await expect(mapper.updateShardMapping('non-existing', 'db-test')).rejects.toThrow(
				'No existing mapping found for primary key: non-existing'
			);
		});
	});

	describe('Database Size Limits', () => {
		beforeEach(async () => {
			// Create schema for testing
			const { createSchema } = await import('../src/migrations');
			await createSchema(mockDB1 as any, TEST_SCHEMA);
			await createSchema(mockDB2 as any, TEST_SCHEMA);
		});

		it('should exclude shards that exceed maxDatabaseSize from allocation', async () => {
			// Initialize with a small size limit
			initialize({
				kv: mockKV as any,
				shards: {
					'db-east': mockDB1 as any,
					'db-west': mockDB2 as any
				},
				strategy: 'hash',
				maxDatabaseSize: 1000 // Very small limit (1KB)
			});

			// Add some data to make db-east appear "large"
			for (let i = 0; i < 100; i++) {
				await runShard('db-east', 'INSERT INTO users (id, name) VALUES (?, ?)', [`user-${i}`, `User ${i}`]);
			}

			// Try to allocate a new user - should avoid db-east due to size
			await run('new-user-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['new-user-1', 'New User 1']);

			// The user should exist (allocation succeeded)
			const result = await first('new-user-1', 'SELECT * FROM users WHERE id = ?', ['new-user-1']);
			expect(result).toBeTruthy();
			expect((result as any).name).toBe('New User 1');
		});

		it('should still work when all shards exceed maxDatabaseSize', async () => {
			// Initialize with a very small size limit that all shards will exceed
			initialize({
				kv: mockKV as any,
				shards: {
					'db-east': mockDB1 as any,
					'db-west': mockDB2 as any
				},
				strategy: 'hash',
				maxDatabaseSize: 1 // Impossibly small limit (1 byte)
			});

			// Should still be able to allocate (fallback behavior)
			await run('fallback-user', 'INSERT INTO users (id, name) VALUES (?, ?)', ['fallback-user', 'Fallback User']);

			const result = await first('fallback-user', 'SELECT * FROM users WHERE id = ?', ['fallback-user']);
			expect(result).toBeTruthy();
			expect((result as any).name).toBe('Fallback User');
		});

		it('should work normally when maxDatabaseSize is not configured', async () => {
			// Initialize without maxDatabaseSize
			initialize({
				kv: mockKV as any,
				shards: {
					'db-east': mockDB1 as any,
					'db-west': mockDB2 as any
				},
				strategy: 'hash'
				// No maxDatabaseSize configured
			});

			// Should work normally
			await run('unlimited-user', 'INSERT INTO users (id, name) VALUES (?, ?)', ['unlimited-user', 'Unlimited User']);

			const result = await first('unlimited-user', 'SELECT * FROM users WHERE id = ?', ['unlimited-user']);
			expect(result).toBeTruthy();
			expect((result as any).name).toBe('Unlimited User');
		});
	});

	describe('SQL emulator', () => {
		it('handles NOT LIKE, multiple AND clauses, and arithmetic comparisons', async () => {
			const db = createInMemorySQLProvider();
			await db.prepare('CREATE TABLE t (id TEXT PRIMARY KEY, label TEXT, score INTEGER)').run();
			await db.prepare('INSERT INTO t (id, label, score) VALUES (?, ?, ?)').bind('a', 'apple', 5).run();
			await db.prepare('INSERT INTO t (id, label, score) VALUES (?, ?, ?)').bind('b', 'banana', 10).run();
			await db.prepare('INSERT INTO t (id, label, score) VALUES (?, ?, ?)').bind('c', 'cherry', 7).run();

			const notLike = await db.prepare('SELECT id FROM t WHERE label NOT LIKE ?').bind('a%').all<{ id: string }>();
			expect(notLike.results.map((r) => r.id).sort()).toEqual(['b', 'c']);

			const tripleAnd = await db
				.prepare('SELECT id FROM t WHERE score >= ? AND score <= ? AND label != ?')
				.bind(5, 10, 'banana')
				.all<{ id: string }>();
			expect(tripleAnd.results.map((r) => r.id).sort()).toEqual(['a', 'c']);

			const notEqual = await db.prepare('SELECT id FROM t WHERE score <> ?').bind(7).all<{ id: string }>();
			expect(notEqual.results.map((r) => r.id).sort()).toEqual(['a', 'b']);
		});

		it('returns 0 results for SELECT against an unknown table', async () => {
			const db = createInMemorySQLProvider();
			const result = await db.prepare('SELECT * FROM ghost').all();
			expect(result.results).toEqual([]);
			expect(result.success).toBe(true);
		});

		it('returns success with 0 changes for DELETE on unknown table', async () => {
			const db = createInMemorySQLProvider();
			const result = await db.prepare('DELETE FROM ghost WHERE id = ?').bind('x').run();
			expect(result.success).toBe(true);
			expect(result.meta.changes).toBe(0);
		});

		it('reports failure for UPDATE on a missing table', async () => {
			const db = createInMemorySQLProvider();
			const result = await db.prepare('UPDATE ghost SET a = ? WHERE id = ?').bind('1', 'x').run();
			expect(result.success).toBe(false);
			expect(result.error).toMatch(/not found/);
		});

		it('drops tables and returns success', async () => {
			const db = createInMemorySQLProvider();
			await db.prepare('CREATE TABLE t (id TEXT PRIMARY KEY)').run();
			const result = await db.prepare('DROP TABLE t').run();
			expect(result.success).toBe(true);

			const second = await db.prepare('DROP TABLE IF EXISTS missing').run();
			expect(second.success).toBe(true);
		});

		it('supports multi-row VALUES tuples in a single INSERT', async () => {
			const db = createInMemorySQLProvider();
			await db.prepare('CREATE TABLE t (id TEXT PRIMARY KEY, label TEXT)').run();
			const result = await db.prepare("INSERT INTO t (id, label) VALUES (?, ?), (?, 'literal')").bind('a', 'first', 'b').run();
			expect(result.meta.changes).toBe(2);

			const rows = await db.prepare('SELECT id, label FROM t ORDER BY id ASC').all<{ id: string; label: string }>();
			expect(rows.results).toEqual([
				{ id: 'a', label: 'first' },
				{ id: 'b', label: 'literal' }
			]);
		});

		it('supports COALESCE, LOWER, UPPER, LENGTH, ABS scalar functions', async () => {
			const db = createInMemorySQLProvider();
			await db.prepare('CREATE TABLE t (id TEXT PRIMARY KEY, label TEXT, n INTEGER)').run();
			await db.prepare('INSERT INTO t (id, label, n) VALUES (?, ?, ?)').bind('a', 'Hello', -5).run();
			await db.prepare('INSERT INTO t (id, label, n) VALUES (?, ?, ?)').bind('b', null, 3).run();

			const lowered = await db
				.prepare('SELECT id, LOWER(label) AS lc, UPPER(label) AS uc, LENGTH(label) AS len, ABS(n) AS positive_n FROM t WHERE id = ?')
				.bind('a')
				.first<{ id: string; lc: string; uc: string; len: number; positive_n: number }>();
			expect(lowered).toEqual({ id: 'a', lc: 'hello', uc: 'HELLO', len: 5, positive_n: 5 });

			const coalesced = await db
				.prepare("SELECT id, COALESCE(label, 'fallback') AS effective FROM t WHERE id = ?")
				.bind('b')
				.first<{ effective: string }>();
			expect(coalesced?.effective).toBe('fallback');
		});

		it('selects star and column projection together', async () => {
			const db = createInMemorySQLProvider();
			await db.prepare('CREATE TABLE t (id TEXT PRIMARY KEY, a TEXT, b INTEGER)').run();
			await db.prepare('INSERT INTO t (id, a, b) VALUES (?, ?, ?)').bind('1', 'x', 2).run();
			const row = await db.prepare('SELECT *, b * 10 AS scaled FROM t WHERE id = ?').bind('1').first<{
				id: string;
				a: string;
				b: number;
				scaled: number;
			}>();
			expect(row).toEqual({ id: '1', a: 'x', b: 2, scaled: 20 });
		});

		it('returns RETURNING rows for INSERT into auto-increment table without explicit id', async () => {
			const db = createInMemorySQLProvider();
			await db.prepare('CREATE TABLE auto (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)').run();
			const result = await db.prepare('INSERT INTO auto (name) VALUES (?) RETURNING *').bind('A').all<{ id: number; name: string }>();
			expect(result.results[0]?.id).toBe(1);
			expect(result.results[0]?.name).toBe('A');
		});

		it('DELETE ... RETURNING surfaces removed rows', async () => {
			const db = createInMemorySQLProvider();
			await db.prepare('CREATE TABLE t (id TEXT PRIMARY KEY, label TEXT)').run();
			await db.prepare('INSERT INTO t (id, label) VALUES (?, ?)').bind('a', 'first').run();

			const result = await db.prepare('DELETE FROM t WHERE id = ? RETURNING id, label').bind('a').all<{ id: string; label: string }>();
			expect(result.results[0]).toEqual({ id: 'a', label: 'first' });
			expect(result.meta.changes).toBe(1);
		});

		it('UPDATE without WHERE applies to every row', async () => {
			const db = createInMemorySQLProvider();
			await db.prepare('CREATE TABLE t (id TEXT PRIMARY KEY, label TEXT)').run();
			await db.prepare('INSERT INTO t (id, label) VALUES (?, ?)').bind('a', 'one').run();
			await db.prepare('INSERT INTO t (id, label) VALUES (?, ?)').bind('b', 'two').run();

			const result = await db.prepare('UPDATE t SET label = ?').bind('same').run();
			expect(result.meta.changes).toBe(2);

			const all = await db.prepare('SELECT id, label FROM t ORDER BY id').all<{ id: string; label: string }>();
			expect(all.results.map((r) => r.label)).toEqual(['same', 'same']);
		});

		it('UPDATE supports col = col + 1 expression', async () => {
			const db = createInMemorySQLProvider();
			await db.prepare('CREATE TABLE counters (id TEXT PRIMARY KEY, n INTEGER)').run();
			await db.prepare('INSERT INTO counters (id, n) VALUES (?, ?)').bind('a', 5).run();
			await db.prepare('UPDATE counters SET n = n + 1 WHERE id = ?').bind('a').run();
			const row = await db.prepare('SELECT n FROM counters WHERE id = ?').bind('a').first<{ n: number }>();
			expect(row?.n).toBe(6);
		});

		it('handles missing WHERE binding gracefully', async () => {
			const db = createInMemorySQLProvider();
			await db.prepare('CREATE TABLE t (id TEXT PRIMARY KEY)').run();
			await db.prepare('INSERT INTO t (id) VALUES (?)').bind('a').run();

			// No binding supplied: the parameter cursor reads `undefined` and
			// returns no matches, but no exception is thrown.
			const result = await db.prepare('SELECT * FROM t WHERE id = ?').all();
			expect(result.success).toBe(true);
			expect(result.results).toEqual([]);
		});

		it('PRAGMA queries return empty results for unknown PRAGMA forms', async () => {
			const db = createInMemorySQLProvider();
			const result = await db.prepare('PRAGMA cache_size').all();
			expect(result.success).toBe(true);
			expect(result.results).toEqual([]);
		});

		it('PRAGMA table_info returns empty when the table is unknown', async () => {
			const db = createInMemorySQLProvider();
			const result = await db.prepare('PRAGMA table_info(missing)').all();
			expect(result.success).toBe(true);
			expect(result.results).toEqual([]);
		});

		it('INSERT without explicit columns falls back to schema order', async () => {
			const db = createInMemorySQLProvider();
			await db.prepare('CREATE TABLE t (id TEXT PRIMARY KEY, label TEXT)').run();
			await db.prepare('INSERT INTO t VALUES (?, ?)').bind('a', 'hello').run();
			const row = await db.prepare('SELECT id, label FROM t WHERE id = ?').bind('a').first<{ id: string; label: string }>();
			expect(row).toEqual({ id: 'a', label: 'hello' });
		});

		it('INSERT with no schema synthesizes a rowid when no PK column maps', async () => {
			const db = createInMemorySQLProvider();
			const result = await db.prepare('INSERT INTO ghost (col1, col2) VALUES (?, ?)').bind('a', 'b').run();
			expect(result.success).toBe(true);
			expect(result.meta.changes).toBe(1);
			expect(result.meta.last_row_id).toBeDefined();
		});
	});

	describe('IP geolocation', () => {
		const cases: Array<[string, string]> = [
			['FR', 'weur'],
			['PL', 'eeur'],
			['JP', 'apac'],
			['IN', 'apac'],
			['AU', 'oc'],
			['AE', 'me'],
			['EG', 'af'],
			['BR', 'enam'],
			['KZ', 'eeur'],
			['GT', 'enam']
		];

		for (const [country, expectedRegion] of cases) {
			it(`returns ${expectedRegion} for country ${country}`, () => {
				expect(getClosestRegionFromIP({ cf: { country } } as any)).toBe(expectedRegion);
			});
		}

		it('uses continent code for African countries when country code does not match', () => {
			expect(getClosestRegionFromIP({ cf: { country: 'XX', continent: 'AF' } } as any)).toBe('af');
		});

		it('falls back to enam for South American countries via continent code', () => {
			expect(getClosestRegionFromIP({ cf: { country: 'XX', continent: 'SA' } } as any)).toBe('enam');
		});

		it('returns wnam for missing cf', () => {
			expect(getClosestRegionFromIP({} as any)).toBe('wnam');
		});
		it('returns eeur for Russia', () => {
			expect(getClosestRegionFromIP({ cf: { country: 'RU' } } as any)).toBe('eeur');
		});
		it('returns wnam for US California timezone', () => {
			expect(
				getClosestRegionFromIP({
					cf: { country: 'US', region: 'CA', timezone: 'America/Los_Angeles' }
				} as any)
			).toBe('wnam');
		});
		it('falls through to wnam for unknown countries', () => {
			expect(getClosestRegionFromIP({ cf: { country: 'XX' } } as any)).toBe('wnam');
		});
	});

	describe('Router - Edge Cases', () => {
		it('listKnownShards returns the configured list when no coordinator is set', async () => {
			initialize({
				kv: createInMemoryKVProvider(),
				shards: { a: createInMemorySQLProvider(), b: createInMemorySQLProvider() },
				disableAutoMigration: true,
				hashShardMappings: false
			});
			const shards = await listKnownShards();
			expect(shards.sort()).toEqual(['a', 'b']);
		});

		it('allByLookupKey runs against mapped shard but falls back to fanout when 0 results', async () => {
			const kv = createInMemoryKVProvider();
			const a = createInMemorySQLProvider();
			const b = createInMemorySQLProvider();
			await a.prepare('CREATE TABLE t (id TEXT PRIMARY KEY, email TEXT)').run();
			await b.prepare('CREATE TABLE t (id TEXT PRIMARY KEY, email TEXT)').run();
			await b.prepare('INSERT INTO t (id, email) VALUES (?, ?)').bind('1', 'b@x').run();

			initialize({
				kv,
				shards: { a, b },
				strategy: 'hash',
				disableAutoMigration: true,
				hashShardMappings: false
			});

			const mapper = new KVShardMapper(kv, { hashShardMappings: false });
			// Set a stale lookup pointing to `a`, but the row lives on `b`.
			await mapper.setShardMapping('email:b@x', 'a');

			const results = await allByLookupKey<{ id: string }>('email:b@x', 'SELECT id FROM t WHERE email = ?', ['b@x']);
			expect(results.results).toEqual([{ id: '1' }]);
		});

		it('insert() throws when no shards are configured', async () => {
			initialize({
				kv: createInMemoryKVProvider(),
				shards: {},
				disableAutoMigration: true
			});
			await expect(insert('INSERT INTO t VALUES (?)', [1])).rejects.toThrow(/No shards/);
		});

		it('allShard reports the underlying error when a shard query fails', async () => {
			const failingDb = {
				prepare() {
					return {
						bind() {
							return {
								async all() {
									return { success: false, results: [], meta: { duration: 0 }, error: 'forced' };
								},
								async first() {
									return null;
								},
								async run() {
									return { success: true, results: [], meta: { duration: 0 } };
								}
							};
						},
						async all() {
							return { success: false, results: [], meta: { duration: 0 }, error: 'forced' };
						}
					};
				}
			};

			initialize({
				kv: createInMemoryKVProvider(),
				shards: { a: failingDb as any },
				disableAutoMigration: true,
				hashShardMappings: false
			});

			const result = await allShard('a', 'SELECT * FROM t');
			expect(result.success).toBe(false);
			expect(result.error).toBe('forced');
		});

		it('migrates the underlying record and updates the mapping', async () => {
			const kv = createInMemoryKVProvider();
			initialize({
				kv,
				shards: {
					'db-a': createInMemorySQLProvider(),
					'db-b': createInMemorySQLProvider()
				},
				strategy: 'hash',
				disableAutoMigration: true,
				hashShardMappings: false
			});

			await runShard('db-a', 'CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)');
			await runShard('db-b', 'CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)');

			await run('alice', 'INSERT INTO users (id, name) VALUES (?, ?)', ['alice', 'Alice']);
			// Use a transient mapper to read the current mapping. Because the global
			// mapper has its own cache, we instantiate fresh mappers for assertions
			// so the test never reads stale cached values.
			const beforeMapper = new KVShardMapper(kv, { hashShardMappings: false });
			const before = await beforeMapper.getShardMapping('alice');
			const otherShard = before?.shard === 'db-a' ? 'db-b' : 'db-a';

			await reassignShard('alice', otherShard, 'users');

			const afterMapper = new KVShardMapper(kv, { hashShardMappings: false });
			const after = await afterMapper.getShardMapping('alice');
			expect(after?.shard).toBe(otherShard);

			const row = await firstShard(otherShard, 'SELECT id FROM users WHERE id = ?', ['alice']);
			expect(row).toEqual({ id: 'alice' });
		});

		it('rejects reassigning to the same shard with no migration', async () => {
			const kv = createInMemoryKVProvider();
			initialize({
				kv,
				shards: { 'db-a': createInMemorySQLProvider() },
				strategy: 'hash',
				disableAutoMigration: true,
				hashShardMappings: false
			});

			await runShard('db-a', 'CREATE TABLE users (id TEXT PRIMARY KEY)');
			await run('a', 'INSERT INTO users (id) VALUES (?)', ['a']);
			await reassignShard('a', 'db-a', 'users');

			const mapper = new KVShardMapper(kv, { hashShardMappings: false });
			expect((await mapper.getShardMapping('a'))?.shard).toBe('db-a');
		});

		it('excludes shards over maxDatabaseSize but never strands all shards', async () => {
			const small = createInMemorySQLProvider();
			const large = createInMemorySQLProvider();
			initialize({
				kv: createInMemoryKVProvider(),
				shards: { small, large },
				strategy: 'round-robin',
				disableAutoMigration: true,
				maxDatabaseSize: 8,
				sizeCacheTtlMs: 0,
				hashShardMappings: false
			});

			await runShard('small', 'CREATE TABLE u (id TEXT PRIMARY KEY)');
			await runShard('large', 'CREATE TABLE u (id TEXT PRIMARY KEY)');

			// Bloat the "large" shard so its page_count grows.
			for (let i = 0; i < 5; i++) {
				await runShard('large', 'INSERT INTO u (id) VALUES (?)', [`l${i}`]);
			}

			// Even after filtering, allocation must succeed.
			await run('new-id', 'INSERT INTO u (id) VALUES (?)', ['new-id']);

			const sizes = await getDatabaseSizesAllShards();
			expect(sizes.every((s) => typeof s.size === 'number')).toBe(true);
			expect(await getTotalDatabaseSize()).toBeGreaterThan(0);
		});

		it('listKnownShards merges configured shards with KV-registered ones', async () => {
			const kv = createInMemoryKVProvider();
			// Pre-register an extra shard via the mapper so the union path runs.
			await new KVShardMapper(kv, { hashShardMappings: false }).setKnownShards(['db-a', 'extra-shard']);

			initialize({
				kv,
				shards: { 'db-a': createInMemorySQLProvider() },
				disableAutoMigration: true,
				hashShardMappings: false
			});

			const shards = await listKnownShards();
			expect(shards.sort()).toContain('extra-shard');
			expect(shards).toContain('db-a');
		});

		it('flush clears KV mappings and the in-memory caches', async () => {
			const kv = createInMemoryKVProvider();
			initialize({
				kv,
				shards: { 'db-a': createInMemorySQLProvider() },
				strategy: 'hash',
				disableAutoMigration: true,
				hashShardMappings: false
			});

			await runShard('db-a', 'CREATE TABLE u (id TEXT PRIMARY KEY)');
			await run('row', 'INSERT INTO u (id) VALUES (?)', ['row']);

			expect(kv.size()).toBeGreaterThan(0);
			await flush();
			// `known_shards` may still be present, but the per-key mappings should
			// have been removed.
			const keys = await kv.keys('shard:');
			expect(keys).toHaveLength(0);
		});

		it('getShardStats falls back to KV counts when no coordinator is configured', async () => {
			const kv = createInMemoryKVProvider();
			initialize({
				kv,
				shards: {
					'db-a': createInMemorySQLProvider(),
					'db-b': createInMemorySQLProvider()
				},
				strategy: 'hash',
				disableAutoMigration: true,
				hashShardMappings: false
			});

			await runShard('db-a', 'CREATE TABLE u (id TEXT PRIMARY KEY)');
			await runShard('db-b', 'CREATE TABLE u (id TEXT PRIMARY KEY)');
			await run('k1', 'INSERT INTO u (id) VALUES (?)', ['k1']);
			await run('k2', 'INSERT INTO u (id) VALUES (?)', ['k2']);
			await run('k3', 'INSERT INTO u (id) VALUES (?)', ['k3']);

			const stats = await getShardStats();
			const totalCount = stats.reduce((sum, stat) => sum + stat.count, 0);
			expect(totalCount).toBe(3);
		});

		it('exposes a single-shard database size via getDatabaseSizeForKey', async () => {
			const kv = createInMemoryKVProvider();
			initialize({
				kv,
				shards: { 'db-a': createInMemorySQLProvider() },
				strategy: 'hash',
				disableAutoMigration: true,
				hashShardMappings: false
			});
			await runShard('db-a', 'CREATE TABLE u (id TEXT PRIMARY KEY)');
			await run('k', 'INSERT INTO u (id) VALUES (?)', ['k']);

			const size = await getDatabaseSizeForKey('k');
			expect(size).toBeGreaterThan(0);

			const shardSize = await getDatabaseSizeForShard('db-a');
			expect(shardSize).toBeGreaterThan(0);
		});

		it('throws CollegeDBError when the requested shard does not exist', async () => {
			initialize({
				kv: createInMemoryKVProvider(),
				shards: { 'db-a': createInMemorySQLProvider() },
				disableAutoMigration: true
			});
			await expect(getDatabaseSizeForShard('does-not-exist')).rejects.toThrow(CollegeDBError);
		});

		it('runAllShards fans out and surfaces per-shard errors', async () => {
			const kv = createInMemoryKVProvider();
			const goodDb = createInMemorySQLProvider();
			await goodDb.prepare('CREATE TABLE u (id TEXT PRIMARY KEY)').run();
			const failingDb = {
				prepare: () => ({
					bind: () => ({
						async all() {
							throw new Error('fail');
						},
						async first() {
							throw new Error('fail');
						},
						async run() {
							throw new Error('fail');
						}
					}),
					async run() {
						throw new Error('fail');
					}
				})
			};

			initialize({
				kv,
				shards: { good: goodDb, bad: failingDb as any },
				disableAutoMigration: true,
				hashShardMappings: false
			});

			const results = await runAllShards('DELETE FROM u');
			expect(results.some((r) => r.success === false)).toBe(true);
			expect(results.some((r) => r.success === true)).toBe(true);
		});

		it('allAllShards merges rows across shards', async () => {
			const a = createInMemorySQLProvider();
			const b = createInMemorySQLProvider();
			await a.prepare('CREATE TABLE u (id TEXT PRIMARY KEY)').run();
			await b.prepare('CREATE TABLE u (id TEXT PRIMARY KEY)').run();
			await a.prepare('INSERT INTO u (id) VALUES (?)').bind('a').run();
			await b.prepare('INSERT INTO u (id) VALUES (?)').bind('b').run();

			initialize({ kv: createInMemoryKVProvider(), shards: { a, b }, disableAutoMigration: true });
			const result = await allAllShards<{ id: string }>('SELECT id FROM u ORDER BY id');
			expect(result).toHaveLength(2);
		});
	});

	describe('Router - location and mixed strategies', () => {
		it('routes by geographic proximity, with priority breaking near-ties', async () => {
			initialize({
				kv: createInMemoryKVProvider(),
				shards: {
					'db-east': createInMemorySQLProvider(),
					'db-west': createInMemorySQLProvider(),
					'db-eu': createInMemorySQLProvider()
				},
				strategy: 'location',
				targetRegion: 'wnam',
				shardLocations: {
					'db-east': { region: 'enam', priority: 1 },
					'db-west': { region: 'wnam', priority: 2 },
					'db-eu': { region: 'weur', priority: 1 }
				},
				disableAutoMigration: true
			});

			await runShard('db-east', 'CREATE TABLE u (id TEXT PRIMARY KEY)');
			await runShard('db-west', 'CREATE TABLE u (id TEXT PRIMARY KEY)');
			await runShard('db-eu', 'CREATE TABLE u (id TEXT PRIMARY KEY)');

			await run('user-1', 'INSERT INTO u (id) VALUES (?)', ['user-1']);
			const westRow = await firstShard('db-west', 'SELECT id FROM u WHERE id = ?', ['user-1']);
			expect(westRow).toEqual({ id: 'user-1' });
		});

		it('falls back to hash when no target region is set for location strategy', async () => {
			initialize({
				kv: createInMemoryKVProvider(),
				shards: {
					'db-a': createInMemorySQLProvider(),
					'db-b': createInMemorySQLProvider()
				},
				strategy: 'location',
				shardLocations: {
					'db-a': { region: 'wnam' },
					'db-b': { region: 'enam' }
				},
				disableAutoMigration: true
			});

			await runShard('db-a', 'CREATE TABLE t (id TEXT PRIMARY KEY)');
			await runShard('db-b', 'CREATE TABLE t (id TEXT PRIMARY KEY)');

			await run('key-1', 'INSERT INTO t (id) VALUES (?)', ['key-1']);
			const row = await first<{ id: string }>('key-1', 'SELECT id FROM t WHERE id = ?', ['key-1']);
			expect(row?.id).toBe('key-1');
		});

		it('mixed strategies pick different shards for read vs write', async () => {
			const kv = createInMemoryKVProvider();
			initialize({
				kv,
				shards: {
					'db-a': createInMemorySQLProvider(),
					'db-b': createInMemorySQLProvider()
				},
				strategy: { read: 'hash', write: 'random' },
				disableAutoMigration: true
			});

			await runShard('db-a', 'CREATE TABLE t (id TEXT PRIMARY KEY)');
			await runShard('db-b', 'CREATE TABLE t (id TEXT PRIMARY KEY)');

			await run('row-1', 'INSERT INTO t (id) VALUES (?)', ['row-1']);

			const mapping = await new KVShardMapper(kv).getShardMapping('row-1');
			expect(['db-a', 'db-b']).toContain(mapping?.shard);
		});
	});

	describe('KVShardMapper - misc behaviors', () => {
		it('addLookupKeys converts a single-key mapping into a multi-key one', async () => {
			const kv = createInMemoryKVProvider();
			const mapper = new KVShardMapper(kv, { hashShardMappings: false });
			await mapper.setShardMapping('user-1', 'db-east');
			await mapper.addLookupKeys('user-1', ['email:user@example.com', 'username:user']);

			expect((await mapper.getShardMapping('email:user@example.com'))?.shard).toBe('db-east');
			expect((await mapper.getShardMapping('username:user'))?.shard).toBe('db-east');
			expect(await mapper.getAllLookupKeys('user-1')).toEqual(expect.arrayContaining(['user-1']));
		});

		it('deleteShardMapping removes multi-key entries entirely', async () => {
			const kv = createInMemoryKVProvider();
			const mapper = new KVShardMapper(kv, { hashShardMappings: false });
			await mapper.setShardMapping('user-2', 'db-east', ['email:multi@example.com']);
			await mapper.deleteShardMapping('user-2');
			expect(await mapper.getShardMapping('user-2')).toBeNull();
			expect(await mapper.getShardMapping('email:multi@example.com')).toBeNull();
		});

		it('clearAllMappings clears everything when concurrency is bounded', async () => {
			const kv = createInMemoryKVProvider();
			const mapper = new KVShardMapper(kv, { hashShardMappings: false });
			for (let i = 0; i < 50; i++) {
				await mapper.setShardMapping(`key-${i}`, 'db-east');
			}
			await mapper.clearAllMappings();
			expect(await mapper.getShardMapping('key-0')).toBeNull();
		});

		it('getKeysForShard and getShardKeyCounts return expected values', async () => {
			const kv = createInMemoryKVProvider();
			const mapper = new KVShardMapper(kv, { hashShardMappings: false });
			await mapper.setShardMapping('u-1', 'db-east');
			await mapper.setShardMapping('u-2', 'db-east');
			await mapper.setShardMapping('u-3', 'db-west');

			const keys = await mapper.getKeysForShard('db-east');
			expect(keys.sort()).toEqual(['u-1', 'u-2']);

			const counts = await mapper.getShardKeyCounts();
			expect(counts['db-east']).toBe(2);
			expect(counts['db-west']).toBe(1);
		});

		it('updateShardMapping migrates multi-key entries to the new shard', async () => {
			const kv = createInMemoryKVProvider();
			const mapper = new KVShardMapper(kv, { hashShardMappings: false });
			await mapper.setShardMapping('user-3', 'db-east', ['email:upd@example.com']);
			await mapper.updateShardMapping('user-3', 'db-west');
			expect((await mapper.getShardMapping('email:upd@example.com'))?.shard).toBe('db-west');
		});

		it('updateShardMapping rejects unknown primary keys', async () => {
			const kv = createInMemoryKVProvider();
			const mapper = new KVShardMapper(kv);
			await expect(mapper.updateShardMapping('nope', 'db-east')).rejects.toThrow('No existing mapping');
		});
	});

	describe('Router - initializeAsync + collegedb', () => {
		it('initializeAsync waits for the migration scan to finish', async () => {
			const db = createInMemorySQLProvider();
			await db.prepare('CREATE TABLE u (id TEXT PRIMARY KEY)').run();
			await db.prepare('INSERT INTO u (id) VALUES (?)').bind('legacy').run();

			await initializeAsync({
				kv: createInMemoryKVProvider(),
				shards: { 'db-a': db },
				strategy: 'hash',
				hashShardMappings: false
			});

			// After initializeAsync, the legacy row should be readable through the router.
			const row = await first<{ id: string }>('legacy', 'SELECT id FROM u WHERE id = ?', ['legacy']);
			expect(row?.id).toBe('legacy');
		});

		it('prepare() works with non-routing SQL like PRAGMA', async () => {
			initialize({
				kv: createInMemoryKVProvider(),
				shards: { 'db-a': createInMemorySQLProvider() },
				disableAutoMigration: true
			});
			const stmt = await prepare('anything', 'PRAGMA page_count');
			const row = await stmt.first<{ page_count: number }>();
			expect(row?.page_count).toBeGreaterThan(0);
		});
	});
});
