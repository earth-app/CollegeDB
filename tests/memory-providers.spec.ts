import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
	allAllShards,
	allShard,
	clearMigrationCache,
	clearShardMigrationCache,
	createInMemoryKVProvider,
	createInMemorySQLProvider,
	first,
	firstShard,
	initialize,
	resetConfig,
	run,
	runShard
} from '../src/index';
import type { CollegeDBConfig } from '../src/types';

describe('InMemory Providers - Basic Operations', () => {
	beforeEach(() => {
		resetConfig();
		clearMigrationCache();
		clearShardMigrationCache('shard-1');
		clearShardMigrationCache('shard-2');
		clearShardMigrationCache('shard-3');

		const config: CollegeDBConfig = {
			kv: createInMemoryKVProvider(),
			shards: {
				'shard-1': createInMemorySQLProvider(),
				'shard-2': createInMemorySQLProvider(),
				'shard-3': createInMemorySQLProvider()
			},
			strategy: 'hash'
		};

		initialize(config);
	});

	afterEach(() => {
		resetConfig();
		clearMigrationCache();
	});

	it('should create schema across all shards', async () => {
		const result = await runShard(
			'shard-1',
			'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)'
		);
		expect(result.success).toBe(true);
	});

	it('should insert and retrieve a user from the correct shard', async () => {
		// Create schema
		await runShard('shard-1', 'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)');
		await runShard('shard-2', 'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)');
		await runShard('shard-3', 'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)');

		// Insert a user (CollegeDB will route to correct shard based on hash of id)
		const insertResult = await run('user-1', 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', [
			'user-1',
			'Alice',
			'alice@example.com'
		]);
		expect(insertResult.success).toBe(true);

		// Retrieve the user
		const user = await first<{ id: string; name: string; email: string }>('user-1', 'SELECT id, name, email FROM users WHERE id = ?', [
			'user-1'
		]);
		expect(user).toBeDefined();
		expect(user?.name).toBe('Alice');
		expect(user?.email).toBe('alice@example.com');
	});

	it('should support multiple users across shards', async () => {
		// Create schema
		await runShard('shard-1', 'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)');
		await runShard('shard-2', 'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)');
		await runShard('shard-3', 'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)');

		// Insert multiple users
		for (let i = 1; i <= 5; i++) {
			await run(`user-${i}`, 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', [`user-${i}`, `User ${i}`, `user${i}@example.com`]);
		}

		// Retrieve each user
		for (let i = 1; i <= 5; i++) {
			const user = await first<{ id: string; name: string }>(`user-${i}`, 'SELECT id, name FROM users WHERE id = ?', [`user-${i}`]);
			expect(user).toBeDefined();
			expect(user?.id).toBe(`user-${i}`);
		}
	});

	it('should handle updates across shards', async () => {
		// Create schema
		await runShard('shard-1', 'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)');
		await runShard('shard-2', 'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)');
		await runShard('shard-3', 'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)');

		// Insert a user
		await run('user-1', 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', ['user-1', 'Alice', 'alice@example.com']);

		// Update the user
		await run('user-1', 'UPDATE users SET name = ? WHERE id = ?', ['Alice Updated', 'user-1']);

		// Verify the update
		const user = await first<{ name: string }>('user-1', 'SELECT name FROM users WHERE id = ?', ['user-1']);
		expect(user?.name).toBe('Alice Updated');
	});

	it('should handle deletes across shards', async () => {
		// Create schema
		await runShard('shard-1', 'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)');
		await runShard('shard-2', 'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)');
		await runShard('shard-3', 'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)');

		// Insert a user
		await run('user-1', 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', ['user-1', 'Alice', 'alice@example.com']);

		// Verify it exists
		let user = await first('user-1', 'SELECT id FROM users WHERE id = ?', ['user-1']);
		expect(user).toBeDefined();

		// Delete the user
		await run('user-1', 'DELETE FROM users WHERE id = ?', ['user-1']);

		// Verify it's deleted
		user = await first('user-1', 'SELECT id FROM users WHERE id = ?', ['user-1']);
		expect(user).toBeNull();
	});

	it('should count records from all shards', async () => {
		// Create schema
		await runShard('shard-1', 'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)');
		await runShard('shard-2', 'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)');
		await runShard('shard-3', 'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)');

		// Insert users
		for (let i = 1; i <= 3; i++) {
			await run(`user-${i}`, 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', [`user-${i}`, `User ${i}`, `user${i}@example.com`]);
		}

		// Count all users across all shards
		const results = await allAllShards('SELECT COUNT(*) as count FROM users');
		expect(results).toHaveLength(3);
		const totalCount = results.reduce((sum: number, r: any) => sum + (r.results[0]?.count || 0), 0);
		expect(totalCount).toBeGreaterThanOrEqual(3);
	});

	it('should support relationships (user-post)', async () => {
		// Create schema
		for (const shardKey of ['shard-1', 'shard-2', 'shard-3']) {
			await runShard(shardKey, 'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL)');
			await runShard(shardKey, 'CREATE TABLE IF NOT EXISTS posts (id TEXT PRIMARY KEY, user_id TEXT NOT NULL, title TEXT NOT NULL)');
		}

		// Create user
		await run('user-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Alice']);

		// Create post
		await run('post-1', 'INSERT INTO posts (id, user_id, title) VALUES (?, ?, ?)', ['post-1', 'user-1', 'My First Post']);

		// Retrieve post
		const post = await first<{ id: string; user_id: string; title: string }>(
			'post-1',
			'SELECT id, user_id, title FROM posts WHERE id = ?',
			['post-1']
		);
		expect(post).toBeDefined();
		expect(post?.user_id).toBe('user-1');
		expect(post?.title).toBe('My First Post');

		// Retrieve user
		const user = await first<{ id: string; name: string }>('user-1', 'SELECT id, name FROM users WHERE id = ?', ['user-1']);
		expect(user).toBeDefined();
		expect(user?.name).toBe('Alice');
	});
});

describe('InMemory Providers - Integration Testing Patterns', () => {
	beforeEach(() => {
		resetConfig();
		clearMigrationCache();
		clearShardMigrationCache('primary');
		clearShardMigrationCache('replica');
		clearShardMigrationCache('users-shard-1');
		clearShardMigrationCache('users-shard-2');
		clearShardMigrationCache('posts-shard-1');
		clearShardMigrationCache('posts-shard-2');
	});

	afterEach(() => {
		resetConfig();
		clearMigrationCache();
		clearShardMigrationCache('primary');
		clearShardMigrationCache('replica');
		clearShardMigrationCache('users-shard-1');
		clearShardMigrationCache('users-shard-2');
		clearShardMigrationCache('posts-shard-1');
		clearShardMigrationCache('posts-shard-2');
	});

	it('should support testing with different shard configurations', async () => {
		const config: CollegeDBConfig = {
			kv: createInMemoryKVProvider(),
			shards: {
				primary: createInMemorySQLProvider(),
				replica: createInMemorySQLProvider()
			},
			strategy: 'round-robin'
		};

		initialize(config);

		// Create schema
		for (const shardKey of ['primary', 'replica']) {
			await runShard(shardKey, 'CREATE TABLE IF NOT EXISTS data (id TEXT PRIMARY KEY, value TEXT)');
		}

		// Insert alternating between shards
		for (let i = 1; i <= 4; i++) {
			await run(`item-${i}`, 'INSERT INTO data (id, value) VALUES (?, ?)', [`item-${i}`, `value-${i}`]);
		}

		// Verify data in primary shard
		const primaryData = await allShard('primary', 'SELECT id FROM data');
		expect(primaryData).toBeDefined();
		expect(primaryData.success).toBe(true);
	});

	it('should support isolated testing per test case', async () => {
		const kv1 = createInMemoryKVProvider();
		const db1 = createInMemorySQLProvider();

		const config1: CollegeDBConfig = {
			kv: kv1,
			shards: { 'shard-1': db1 },
			strategy: 'hash'
		};

		initialize(config1);

		// Test 1: Insert and verify
		await runShard('shard-1', 'CREATE TABLE test1 (id TEXT PRIMARY KEY)');
		await runShard('shard-1', 'INSERT INTO test1 (id) VALUES (?)', ['record-1']);

		const record = await firstShard('shard-1', 'SELECT id FROM test1 WHERE id = ?', ['record-1']);
		expect(record).toBeDefined();

		// Reset for next test
		resetConfig();
		clearMigrationCache();
		clearShardMigrationCache('shard-1');

		const kv2 = createInMemoryKVProvider();
		const db2 = createInMemorySQLProvider();

		const config2: CollegeDBConfig = {
			kv: kv2,
			shards: { 'shard-1': db2 },
			strategy: 'hash'
		};

		initialize(config2);

		// Test 2: Fresh database, no prior data
		await runShard('shard-1', 'CREATE TABLE test2 (id TEXT PRIMARY KEY)');
		const record2 = await firstShard('shard-1', 'SELECT id FROM test2 WHERE id = ?', ['record-1']);
		expect(record2).toBeNull();
	});

	it('should support multiple independent test suites', async () => {
		// Suite 1: User-centric tests
		resetConfig();
		clearMigrationCache();
		clearShardMigrationCache('users-shard-1');
		clearShardMigrationCache('users-shard-2');

		const suite1Config: CollegeDBConfig = {
			kv: createInMemoryKVProvider(),
			shards: {
				'users-shard-1': createInMemorySQLProvider(),
				'users-shard-2': createInMemorySQLProvider()
			},
			strategy: 'hash'
		};

		initialize(suite1Config);
		await runShard('users-shard-1', 'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT)');
		await runShard('users-shard-2', 'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT)');
		await run('user-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Alice']);

		const user = await first('user-1', 'SELECT name FROM users WHERE id = ?', ['user-1']);
		expect(user?.name).toBe('Alice');

		// Suite 2: Post-centric tests (fresh setup)
		resetConfig();
		clearMigrationCache();
		clearShardMigrationCache('posts-shard-1');
		clearShardMigrationCache('posts-shard-2');

		const suite2Config: CollegeDBConfig = {
			kv: createInMemoryKVProvider(),
			shards: {
				'posts-shard-1': createInMemorySQLProvider(),
				'posts-shard-2': createInMemorySQLProvider()
			},
			strategy: 'hash'
		};

		initialize(suite2Config);
		await runShard('posts-shard-1', 'CREATE TABLE IF NOT EXISTS posts (id TEXT PRIMARY KEY, title TEXT)');
		await runShard('posts-shard-2', 'CREATE TABLE IF NOT EXISTS posts (id TEXT PRIMARY KEY, title TEXT)');
		await run('post-1', 'INSERT INTO posts (id, title) VALUES (?, ?)', ['post-1', 'My Post']);

		const post = await first('post-1', 'SELECT title FROM posts WHERE id = ?', ['post-1']);
		expect(post?.title).toBe('My Post');
	});
});
