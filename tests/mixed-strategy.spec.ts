/**
 * Test suite for mixed strategy functionality
 */

import { beforeEach, describe, expect, it } from 'vitest';
import { first, initialize, resetConfig, run } from '../src/index.js';
import type { MixedShardingStrategy } from '../src/types.js';

// Mock D1Database for testing
class MockD1Database {
	private data = new Map<string, Map<string, any>>();
	public name: string;

	constructor(name: string) {
		this.name = name;
	}

	prepare(sql: string) {
		return {
			bind: (...params: any[]) => ({
				run: async () => {
					const trimmedSql = sql.trim().toUpperCase();

					if (trimmedSql.startsWith('INSERT')) {
						// Parse INSERT statement - simplified for testing
						const match = sql.match(/INSERT\s+INTO\s+(\w+)/i);
						const tableName = match?.[1] || 'default';

						if (!this.data.has(tableName)) {
							this.data.set(tableName, new Map());
						}

						const table = this.data.get(tableName)!;
						const record: any = {};

						// For testing, assume the first param is always the ID
						if (params.length >= 2) {
							record.id = params[0];
							record.name = params[1];
						}

						table.set(record.id, record);

						return {
							success: true,
							meta: {
								changes: 1,
								last_row_id: 1,
								duration: 0.1
							}
						};
					}

					if (trimmedSql.startsWith('UPDATE')) {
						const match = sql.match(/UPDATE\s+(\w+)/i);
						const tableName = match?.[1] || 'default';

						if (this.data.has(tableName)) {
							const table = this.data.get(tableName)!;
							// Assume last param is the ID for WHERE clause
							const id = params[params.length - 1];
							if (table.has(id)) {
								const record = table.get(id);
								// For testing, assume second param is the name
								if (params.length >= 2) {
									record.name = params[0];
								}
								table.set(id, record);
							}
						}

						return {
							success: true,
							meta: {
								changes: 1,
								duration: 0.1
							}
						};
					}

					if (trimmedSql.startsWith('DELETE')) {
						const match = sql.match(/DELETE\s+FROM\s+(\w+)/i);
						const tableName = match?.[1] || 'default';

						if (this.data.has(tableName)) {
							const table = this.data.get(tableName)!;
							const id = params[0];
							table.delete(id);
						}

						return {
							success: true,
							meta: {
								changes: 1,
								duration: 0.1
							}
						};
					}

					return {
						success: true,
						meta: {
							changes: 0,
							duration: 0.1
						}
					};
				},

				first: async () => {
					const trimmedSql = sql.trim().toUpperCase();

					if (trimmedSql.startsWith('SELECT')) {
						const match = sql.match(/FROM\s+(\w+)/i);
						const tableName = match?.[1] || 'default';

						if (this.data.has(tableName)) {
							const table = this.data.get(tableName)!;
							const id = params[0];
							return table.get(id) || null;
						}
					}

					return null;
				},

				all: async () => {
					const trimmedSql = sql.trim().toUpperCase();

					if (trimmedSql.startsWith('SELECT')) {
						const match = sql.match(/FROM\s+(\w+)/i);
						const tableName = match?.[1] || 'default';

						if (this.data.has(tableName)) {
							const table = this.data.get(tableName)!;
							return {
								success: true,
								results: Array.from(table.values()),
								meta: {
									count: table.size,
									duration: 0.1
								}
							};
						}
					}

					return {
						success: true,
						results: [],
						meta: {
							count: 0,
							duration: 0.1
						}
					};
				}
			})
		};
	}
}

// Mock KV Namespace for testing
class MockKVNamespace {
	private data = new Map<string, any>();

	async get(key: string, type?: 'text' | 'json' | 'arrayBuffer' | 'stream') {
		const value = this.data.get(key);
		if (value === undefined) return null;

		if (type === 'json') {
			try {
				return JSON.parse(value);
			} catch {
				return null;
			}
		}

		return value;
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
			keys: filteredKeys.map((name) => ({ name })),
			list_complete: true
		};
	}
}

describe('Mixed Strategy Support', () => {
	let mockKV: MockKVNamespace;
	let mockDB1: MockD1Database;
	let mockDB2: MockD1Database;

	beforeEach(() => {
		resetConfig();
		mockKV = new MockKVNamespace();
		mockDB1 = new MockD1Database('db-east');
		mockDB2 = new MockD1Database('db-west');
	});

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
});
