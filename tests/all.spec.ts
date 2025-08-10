import type { Request } from '@cloudflare/workers-types';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
	all,
	autoDetectAndMigrate,
	checkMigrationNeeded,
	clearMigrationCache,
	first,
	getClosestRegionFromIP,
	getShardStats,
	initialize,
	listKnownShards,
	reassignShard,
	run
} from '../src/index.js';
import type { CollegeDBConfig } from '../src/types.js';

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
		FOREIGN KEY (user_id) REFERE		it('should cache migration results to avoid repeated checks', async () => {
			const config: CollegeDBConfig = {
				kv: mockKV as any,
				shards: { 'db-cache': mockDB1 as any },
				strategy: 'hash'
			};

			// First migration should detect and migrate
			const result1 = await autoDetectAndMigrate(mockDB1 as any, 'db-cache', config, { skipCache: true });
			expect(result1.migrationPerformed).toBe(true);

			// Second migration should be cached (no migration performed)
			const result2 = await autoDetectAndMigrate(mockDB1 as any, 'db-cache', config);
			expect(result2.migrationPerformed).toBe(false);
			expect(result2.recordsMigrated).toBe(0);
		}););

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
			if (tableName) {
				if (!this.data.has(tableName)) this.data.set(tableName, []);
				const table = this.data.get(tableName)!;

				// Handle different table structures
				if (tableName === 'users') {
					const record = { id: args[0], name: args[1], email: args[2] };
					// Remove existing record with same id for REPLACE behavior
					const existingIndex = table.findIndex((r: any) => r.id === args[0]);
					if (existingIndex > -1) {
						table[existingIndex] = record;
					} else {
						table.push(record);
					}
				} else if (tableName === 'posts') {
					const record = { id: args[0], user_id: args[1], title: args[2], content: args[3] };
					const existingIndex = table.findIndex((r: any) => r.id === args[0]);
					if (existingIndex > -1) {
						table[existingIndex] = record;
					} else {
						table.push(record);
					}
				} else {
					// Generic handling for other tables
					const record: any = { id: args[0] };
					for (let i = 1; i < args.length; i++) {
						record[`field_${i}`] = args[i];
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
			strategy: 'round-robin'
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
			const { createSchema } = await import('../src/migrations.js');
			await expect(createSchema(mockDB1 as any, TEST_SCHEMA)).resolves.toBeUndefined();
		});
	});

	describe('Basic CRUD Operations', () => {
		beforeEach(async () => {
			// Create schema directly with the migration module
			const { createSchema } = await import('../src/migrations.js');
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

	describe('Shard Routing', () => {
		beforeEach(async () => {
			// Create schema directly with the migration module
			const { createSchema } = await import('../src/migrations.js');
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
			const { createSchema } = await import('../src/migrations.js');
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
			const { discoverExistingPrimaryKeys } = await import('../src/migrations.js');

			const userIds = await discoverExistingPrimaryKeys(mockDB1 as any, 'users');
			expect(userIds).toContain('existing-user-1');
			expect(userIds).toContain('existing-user-2');
			expect(userIds).toHaveLength(2);

			const postIds = await discoverExistingPrimaryKeys(mockDB1 as any, 'posts');
			expect(postIds).toContain('post-1');
			expect(postIds).toHaveLength(1);
		});

		it('should validate tables for sharding', async () => {
			const { validateTableForSharding } = await import('../src/migrations.js');

			const usersValidation = await validateTableForSharding(mockDB1 as any, 'users', 'id');
			expect(usersValidation.isValid).toBe(true);
			expect(usersValidation.recordCount).toBe(2);
			expect(usersValidation.tableName).toBe('users');

			const invalidValidation = await validateTableForSharding(mockDB1 as any, 'nonexistent_table', 'id');
			expect(invalidValidation.isValid).toBe(false);
			expect(invalidValidation.issues).toContain("Table 'nonexistent_table' does not exist");
		});

		it('should integrate existing database', async () => {
			const { integrateExistingDatabase } = await import('../src/migrations.js');
			const { KVShardMapper } = await import('../src/kvmap.js');

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
			const { integrateExistingDatabase } = await import('../src/migrations.js');
			const { KVShardMapper } = await import('../src/kvmap.js');

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
			const { integrateExistingDatabase } = await import('../src/migrations.js');
			const { KVShardMapper } = await import('../src/kvmap.js');

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

			// Create existing database with data
			const { createSchema } = await import('../src/migrations.js');
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
		});

		it('should detect when migration is needed', async () => {
			const config: CollegeDBConfig = {
				kv: mockKV as any,
				shards: { 'db-auto': mockDB1 as any },
				strategy: 'hash'
			};

			const needsMigration = await checkMigrationNeeded(mockDB1 as any, 'db-auto', config);
			expect(needsMigration).toBe(true);
		});

		it('should automatically detect and migrate existing data', async () => {
			const config: CollegeDBConfig = {
				kv: mockKV as any,
				shards: { 'db-auto': mockDB1 as any },
				strategy: 'hash'
			};

			const result = await autoDetectAndMigrate(mockDB1 as any, 'db-auto', config, { skipCache: true });

			expect(result.migrationNeeded).toBe(true);
			expect(result.migrationPerformed).toBe(true);
			expect(result.recordsMigrated).toBe(3); // 2 users + 1 post
			expect(result.tablesProcessed).toBe(2); // users and posts tables

			// Verify mappings were created
			const { KVShardMapper } = await import('../src/kvmap.js');
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
				strategy: 'hash'
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
					'db-auto': mockDB1 as any,
					'db-new': mockDB2 as any
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
			const { createSchema } = await import('../src/migrations.js');
			await createSchema(mockDB2 as any, TEST_SCHEMA);

			const config: CollegeDBConfig = {
				kv: mockKV as any,
				shards: { 'db-empty': mockDB2 as any },
				strategy: 'hash'
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
				strategy: 'hash'
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
				strategy: 'hash'
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
				}
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
				}
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
				}
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
				}
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
				}
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
});
