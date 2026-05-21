import { sql as drizzleSql } from 'drizzle-orm';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
	allAllShards,
	allShard,
	clearMigrationCache,
	clearShardMigrationCache,
	createDrizzleSQLProvider,
	createInMemoryKVProvider,
	createInMemorySQLProvider,
	createNuxtHubKVProvider,
	first,
	firstShard,
	initialize,
	insert,
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

describe('InMemory Providers - Adapter Interop', () => {
	it('supports Drizzle ORM through the in-memory SQL provider', async () => {
		const database = createInMemorySQLProvider();
		const provider = createDrizzleSQLProvider(database, drizzleSql as any);

		await provider.prepare('CREATE TABLE IF NOT EXISTS tickets (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL)').run();

		const created = await provider
			.prepare('INSERT INTO tickets (title) VALUES (?) RETURNING id, title')
			.bind('First Ticket')
			.run<{ id: number; title: string }>();

		expect(created.success).toBe(true);
		expect(created.results[0]?.id).toBe(1);
		expect(created.results[0]?.title).toBe('First Ticket');
		expect(created.meta.last_row_id).toBe(1);

		const ticket = await provider
			.prepare('SELECT id, title FROM tickets WHERE title = ?')
			.bind('First Ticket')
			.first<{ id: number; title: string }>();

		expect(ticket).toEqual({ id: 1, title: 'First Ticket' });
	});

	it('supports NuxtHub KV through the in-memory KV provider', async () => {
		const storage = createInMemoryKVProvider();
		const kv = createNuxtHubKVProvider(storage);

		await storage.set('known_shards', ['db-east', 'db-west']);
		expect(await storage.keys('known_')).toEqual(['known_shards']);

		const knownShards = await kv.get<string[]>('known_shards', 'json');
		expect(knownShards).toEqual(['db-east', 'db-west']);

		await kv.put('shard:user:1', JSON.stringify({ shard: 'db-east' }));
		const listed = await kv.list({ prefix: 'shard:user:' });
		expect(listed.keys.map((key) => key.name)).toEqual(['shard:user:1']);

		await kv.delete('shard:user:1');
		expect(await storage.get('shard:user:1')).toBeNull();
	});
});

describe('InMemorySQLDatabase - INSERT variants', () => {
	it('honors INSERT OR REPLACE and surfaces UNIQUE constraint errors otherwise', async () => {
		const db = createInMemorySQLProvider();
		await db.prepare('CREATE TABLE k (id TEXT PRIMARY KEY, value TEXT)').run();
		await db.prepare('INSERT INTO k (id, value) VALUES (?, ?)').bind('1', 'first').run();

		const dup = await db.prepare('INSERT INTO k (id, value) VALUES (?, ?)').bind('1', 'second').run();
		expect(dup.success).toBe(false);
		expect(dup.error).toMatch(/UNIQUE constraint failed/);

		await db.prepare('INSERT OR REPLACE INTO k (id, value) VALUES (?, ?)').bind('1', 'replaced').run();
		const row = await db.prepare('SELECT value FROM k WHERE id = ?').bind('1').first<{ value: string }>();
		expect(row?.value).toBe('replaced');
	});

	it('honors INSERT OR IGNORE without failing', async () => {
		const db = createInMemorySQLProvider();
		await db.prepare('CREATE TABLE k (id TEXT PRIMARY KEY, value TEXT)').run();
		await db.prepare('INSERT INTO k (id, value) VALUES (?, ?)').bind('1', 'first').run();

		const second = await db.prepare('INSERT OR IGNORE INTO k (id, value) VALUES (?, ?)').bind('1', 'second').run();
		expect(second.success).toBe(true);
		expect(second.meta.changes).toBe(0);

		const row = await db.prepare('SELECT value FROM k WHERE id = ?').bind('1').first<{ value: string }>();
		expect(row?.value).toBe('first');
	});

	it('auto-assigns ids for AUTOINCREMENT columns and exposes RETURNING rows', async () => {
		const db = createInMemorySQLProvider();
		await db.prepare('CREATE TABLE auto (id INTEGER PRIMARY KEY AUTOINCREMENT, label TEXT)').run();

		const first = await db
			.prepare('INSERT INTO auto (label) VALUES (?) RETURNING id, label')
			.bind('hello')
			.all<{ id: number; label: string }>();
		expect(first.results[0]).toEqual({ id: 1, label: 'hello' });

		const second = await db.prepare('INSERT INTO auto (label) VALUES (?) RETURNING id').bind('world').all<{ id: number }>();
		expect(second.results[0]?.id).toBe(2);
	});

	it('applies inline DEFAULT literals when columns are omitted', async () => {
		const db = createInMemorySQLProvider();
		await db.prepare("CREATE TABLE defaults_tbl (id TEXT PRIMARY KEY, name TEXT DEFAULT 'guest', status INTEGER DEFAULT 1)").run();
		await db.prepare('INSERT INTO defaults_tbl (id) VALUES (?)').bind('a').run();
		const row = await db.prepare('SELECT name, status FROM defaults_tbl WHERE id = ?').bind('a').first<{ name: string; status: number }>();
		expect(row?.name).toBe('guest');
		expect(row?.status).toBe(1);
	});

	it('compound WHERE evaluates each clause (regression for first-clause-only bug)', async () => {
		const db = createInMemorySQLProvider();
		await db.prepare('CREATE TABLE tickets (id TEXT PRIMARY KEY, title TEXT, description TEXT, private INTEGER)').run();
		await db.prepare('INSERT INTO tickets (id, title, description, private) VALUES (?, ?, ?, ?)').bind('a', 'hello', 'x', 0).run();
		await db
			.prepare('INSERT INTO tickets (id, title, description, private) VALUES (?, ?, ?, ?)')
			.bind('b', 'world', 'hello world', 0)
			.run();
		await db
			.prepare('INSERT INTO tickets (id, title, description, private) VALUES (?, ?, ?, ?)')
			.bind('c', 'private', 'hello again', 1)
			.run();

		const result = await db
			.prepare('SELECT id FROM tickets WHERE (title LIKE ? OR description LIKE ?) AND private = 0')
			.bind('%hello%', '%hello%')
			.all<{ id: string }>();
		const ids = result.results.map((row) => row.id).sort();
		expect(ids).toEqual(['a', 'b']);
	});
});

describe('InMemorySQLDatabase - SELECT projection and predicates', () => {
	it('projects only the requested columns', async () => {
		const db = createInMemorySQLProvider();
		await db.prepare('CREATE TABLE u (id TEXT PRIMARY KEY, name TEXT, email TEXT)').run();
		await db.prepare('INSERT INTO u (id, name, email) VALUES (?, ?, ?)').bind('1', 'a', 'a@x').run();

		const row = await db.prepare('SELECT name FROM u WHERE id = ?').bind('1').first<{ name?: string; email?: string }>();
		expect(row).toEqual({ name: 'a' });
	});

	it('handles ORDER BY ASC/DESC with NULL placement and LIMIT/OFFSET', async () => {
		const db = createInMemorySQLProvider();
		await db.prepare('CREATE TABLE u (id TEXT PRIMARY KEY, age INTEGER)').run();
		await db.prepare('INSERT INTO u (id, age) VALUES (?, ?)').bind('a', 30).run();
		await db.prepare('INSERT INTO u (id, age) VALUES (?, ?)').bind('b', 20).run();
		await db.prepare('INSERT INTO u (id, age) VALUES (?, ?)').bind('c', 25).run();
		await db.prepare('INSERT INTO u (id, age) VALUES (?, ?)').bind('d', null).run();

		const asc = await db.prepare('SELECT id, age FROM u ORDER BY age ASC LIMIT 2').all<{ id: string; age: number | null }>();
		expect(asc.results.map((r) => r.id)).toEqual(['d', 'b']);

		const desc = await db.prepare('SELECT id, age FROM u ORDER BY age DESC LIMIT 2 OFFSET 1').all<{ id: string }>();
		expect(desc.results.map((r) => r.id)).toEqual(['c', 'b']);
	});

	it('supports IS NULL / IS NOT NULL predicates', async () => {
		const db = createInMemorySQLProvider();
		await db.prepare('CREATE TABLE u (id TEXT PRIMARY KEY, email TEXT)').run();
		await db.prepare('INSERT INTO u (id, email) VALUES (?, ?)').bind('a', null).run();
		await db.prepare('INSERT INTO u (id, email) VALUES (?, ?)').bind('b', 'b@x').run();

		const nullRows = await db.prepare('SELECT id FROM u WHERE email IS NULL').all<{ id: string }>();
		expect(nullRows.results.map((r) => r.id)).toEqual(['a']);

		const presentRows = await db.prepare('SELECT id FROM u WHERE email IS NOT NULL').all<{ id: string }>();
		expect(presentRows.results.map((r) => r.id)).toEqual(['b']);
	});

	it('supports numeric comparison operators', async () => {
		const db = createInMemorySQLProvider();
		await db.prepare('CREATE TABLE u (id TEXT PRIMARY KEY, age INTEGER)').run();
		await db.prepare('INSERT INTO u (id, age) VALUES (?, ?)').bind('a', 10).run();
		await db.prepare('INSERT INTO u (id, age) VALUES (?, ?)').bind('b', 20).run();
		await db.prepare('INSERT INTO u (id, age) VALUES (?, ?)').bind('c', 30).run();

		const greater = await db.prepare('SELECT id FROM u WHERE age > ?').bind(15).all<{ id: string }>();
		expect(greater.results.map((r) => r.id).sort()).toEqual(['b', 'c']);

		const between = await db.prepare('SELECT id FROM u WHERE age >= ? AND age <= ?').bind(15, 25).all<{ id: string }>();
		expect(between.results.map((r) => r.id)).toEqual(['b']);
	});

	it('computes COUNT(*), MIN, MAX, SUM, AVG aggregates', async () => {
		const db = createInMemorySQLProvider();
		await db.prepare('CREATE TABLE u (id TEXT PRIMARY KEY, age INTEGER)').run();
		await db.prepare('INSERT INTO u (id, age) VALUES (?, ?)').bind('a', 10).run();
		await db.prepare('INSERT INTO u (id, age) VALUES (?, ?)').bind('b', 20).run();
		await db.prepare('INSERT INTO u (id, age) VALUES (?, ?)').bind('c', 30).run();

		const result = await db
			.prepare('SELECT COUNT(*) AS c, MIN(age) AS min_age, MAX(age) AS max_age, SUM(age) AS total, AVG(age) AS avg_age FROM u')
			.first<{ c: number; min_age: number; max_age: number; total: number; avg_age: number }>();
		expect(result).toEqual({ c: 3, min_age: 10, max_age: 30, total: 60, avg_age: 20 });
	});

	it('honors WHERE filters when computing aggregates', async () => {
		const db = createInMemorySQLProvider();
		await db.prepare('CREATE TABLE u (id TEXT PRIMARY KEY, age INTEGER)').run();
		await db.prepare('INSERT INTO u (id, age) VALUES (?, ?)').bind('a', 10).run();
		await db.prepare('INSERT INTO u (id, age) VALUES (?, ?)').bind('b', 20).run();

		const result = await db.prepare('SELECT COUNT(*) AS c FROM u WHERE age > ?').bind(15).first<{ c: number }>();
		expect(result?.c).toBe(1);
	});

	it('supports COUNT(DISTINCT col)', async () => {
		const db = createInMemorySQLProvider();
		await db.prepare('CREATE TABLE u (id TEXT PRIMARY KEY, region TEXT)').run();
		await db.prepare('INSERT INTO u (id, region) VALUES (?, ?)').bind('a', 'us').run();
		await db.prepare('INSERT INTO u (id, region) VALUES (?, ?)').bind('b', 'us').run();
		await db.prepare('INSERT INTO u (id, region) VALUES (?, ?)').bind('c', 'eu').run();

		const result = await db.prepare('SELECT COUNT(DISTINCT region) AS regions FROM u').first<{ regions: number }>();
		expect(result?.regions).toBe(2);
	});

	it('supports LIKE pattern matching', async () => {
		const db = createInMemorySQLProvider();
		await db.prepare('CREATE TABLE u (id TEXT PRIMARY KEY, name TEXT)').run();
		await db.prepare('INSERT INTO u (id, name) VALUES (?, ?)').bind('1', 'Alice').run();
		await db.prepare('INSERT INTO u (id, name) VALUES (?, ?)').bind('2', 'Bob').run();
		await db.prepare('INSERT INTO u (id, name) VALUES (?, ?)').bind('3', 'Alicia').run();

		const matched = await db.prepare('SELECT id FROM u WHERE name LIKE ?').bind('Ali%').all<{ id: string }>();
		expect(matched.results.map((r) => r.id).sort()).toEqual(['1', '3']);
	});

	it('aggregate SELECT evaluates COALESCE(MAX(id), 0) + 1 across rows', async () => {
		const db = createInMemorySQLProvider();
		await db.prepare('CREATE TABLE tickets (id INTEGER PRIMARY KEY)').run();
		await db.prepare('INSERT INTO tickets (id) VALUES (?)').bind(1).run();
		await db.prepare('INSERT INTO tickets (id) VALUES (?)').bind(5).run();
		await db.prepare('INSERT INTO tickets (id) VALUES (?)').bind(3).run();

		const result = await db.prepare('SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM tickets').first<{ next_id: number }>();
		expect(result?.next_id).toBe(6);

		const empty = await db.prepare('SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM tickets WHERE id < ?').bind(0).first<{
			next_id: number;
		}>();
		expect(empty?.next_id).toBe(1);
	});

	it('aggregate arithmetic does not rely on new Function (Workers-isolate safe)', async () => {
		const original = globalThis.Function;
		let invocations = 0;
		const guarded = function (this: unknown) {
			invocations++;
			throw new Error('new Function is blocked in Workers isolates');
		} as unknown as { prototype: unknown };
		guarded.prototype = original.prototype;
		(globalThis as any).Function = guarded;

		try {
			const db = createInMemorySQLProvider();
			await db.prepare('CREATE TABLE t (id INTEGER PRIMARY KEY)').run();
			await db.prepare('INSERT INTO t (id) VALUES (?)').bind(2).run();
			await db.prepare('INSERT INTO t (id) VALUES (?)').bind(7).run();

			const row = await db.prepare('SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM t').first<{ next_id: number }>();
			expect(row?.next_id).toBe(8);
			expect(invocations).toBe(0);

			const empty = await db
				.prepare('SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM t WHERE id > ?')
				.bind(1000)
				.first<{ next_id: number }>();
			expect(empty?.next_id).toBe(1);
			expect(invocations).toBe(0);
		} finally {
			(globalThis as any).Function = original;
		}
	});
});

describe('InMemorySQLDatabase - UPDATE/DELETE advanced usage', () => {
	it('UPDATE applies to all rows matching a compound WHERE', async () => {
		const db = createInMemorySQLProvider();
		await db.prepare('CREATE TABLE u (id TEXT PRIMARY KEY, status TEXT, score INTEGER)').run();
		await db.prepare('INSERT INTO u (id, status, score) VALUES (?, ?, ?)').bind('a', 'new', 5).run();
		await db.prepare('INSERT INTO u (id, status, score) VALUES (?, ?, ?)').bind('b', 'new', 12).run();
		await db.prepare('INSERT INTO u (id, status, score) VALUES (?, ?, ?)').bind('c', 'done', 7).run();

		const update = await db.prepare('UPDATE u SET status = ? WHERE status = ? AND score >= ?').bind('active', 'new', 10).run();
		expect(update.meta.changes).toBe(1);

		const updated = await db.prepare('SELECT id, status FROM u WHERE status = ?').bind('active').all<{ id: string }>();
		expect(updated.results.map((r) => r.id)).toEqual(['b']);
	});

	it('DELETE removes rows matching a compound WHERE', async () => {
		const db = createInMemorySQLProvider();
		await db.prepare('CREATE TABLE u (id TEXT PRIMARY KEY, status TEXT, score INTEGER)').run();
		await db.prepare('INSERT INTO u (id, status, score) VALUES (?, ?, ?)').bind('a', 'new', 5).run();
		await db.prepare('INSERT INTO u (id, status, score) VALUES (?, ?, ?)').bind('b', 'new', 12).run();

		const result = await db.prepare('DELETE FROM u WHERE status = ? AND score > ?').bind('new', 10).run();
		expect(result.meta.changes).toBe(1);

		const remaining = await db.prepare('SELECT id FROM u').all<{ id: string }>();
		expect(remaining.results.map((r) => r.id)).toEqual(['a']);
	});

	it('DELETE without WHERE removes everything', async () => {
		const db = createInMemorySQLProvider();
		await db.prepare('CREATE TABLE u (id TEXT PRIMARY KEY)').run();
		await db.prepare('INSERT INTO u (id) VALUES (?)').bind('a').run();
		await db.prepare('INSERT INTO u (id) VALUES (?)').bind('b').run();

		const result = await db.prepare('DELETE FROM u').run();
		expect(result.meta.changes).toBe(2);

		const rows = await db.prepare('SELECT id FROM u').all();
		expect(rows.results).toHaveLength(0);
	});

	it('UPDATE returns RETURNING rows', async () => {
		const db = createInMemorySQLProvider();
		await db.prepare('CREATE TABLE u (id TEXT PRIMARY KEY, status TEXT)').run();
		await db.prepare('INSERT INTO u (id, status) VALUES (?, ?)').bind('a', 'new').run();
		const result = await db
			.prepare('UPDATE u SET status = ? WHERE id = ? RETURNING id, status')
			.bind('done', 'a')
			.all<{ id: string; status: string }>();
		expect(result.results[0]).toEqual({ id: 'a', status: 'done' });
	});

	it('UPDATE preserves columns whose names contain "w"', async () => {
		const db = createInMemorySQLProvider();
		await db.prepare('CREATE TABLE t (id TEXT PRIMARY KEY, wrapped_dek TEXT, nonce TEXT)').run();
		await db.prepare('INSERT INTO t (id, wrapped_dek, nonce) VALUES (?, ?, ?)').bind('a', 'old', 'n1').run();
		await db.prepare('UPDATE t SET wrapped_dek = ?, nonce = ? WHERE id = ?').bind('new', 'n2', 'a').run();

		const row = await db.prepare('SELECT wrapped_dek, nonce FROM t WHERE id = ?').bind('a').first<{
			wrapped_dek: string;
			nonce: string;
		}>();
		expect(row?.wrapped_dek).toBe('new');
		expect(row?.nonce).toBe('n2');
	});
});

describe('InMemorySQLDatabase - PRAGMA + sqlite_master', () => {
	it('returns page_count / page_size and table_info', async () => {
		const db = createInMemorySQLProvider();
		await db.prepare('CREATE TABLE u (id TEXT PRIMARY KEY, name TEXT NOT NULL)').run();
		await db.prepare('INSERT INTO u (id, name) VALUES (?, ?)').bind('1', 'A').run();

		const pageCount = await db.prepare('PRAGMA page_count').first<{ page_count: number }>();
		const pageSize = await db.prepare('PRAGMA page_size').first<{ page_size: number }>();
		expect(pageCount?.page_count).toBeGreaterThan(0);
		expect(pageSize?.page_size).toBeGreaterThan(0);

		const info = await db.prepare('PRAGMA table_info(u)').all<{ name: string; pk: number }>();
		const idColumn = info.results.find((row) => row.name === 'id');
		expect(idColumn?.pk).toBe(1);
	});

	it('lists tables via sqlite_master', async () => {
		const db = createInMemorySQLProvider();
		await db.prepare('CREATE TABLE one (id TEXT PRIMARY KEY)').run();
		await db.prepare('CREATE TABLE two (id TEXT PRIMARY KEY)').run();
		const result = await db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name").all<{ name: string }>();
		expect(result.results.map((row) => row.name).sort()).toEqual(['one', 'two']);
	});
});

describe('InMemoryKVStorage', () => {
	it('supports text + JSON get, TTL expiry, and prefix listing', async () => {
		const kv = createInMemoryKVProvider();
		await kv.put('hello', 'world');
		expect(await kv.get('hello')).toBe('world');

		await kv.put('json', JSON.stringify({ a: 1 }));
		expect(await kv.get<{ a: number }>('json', 'json')).toEqual({ a: 1 });

		await kv.put('temp', 'gone', { expirationTtl: 0.05 });
		await new Promise((resolve) => setTimeout(resolve, 70));
		expect(await kv.get('temp')).toBeNull();

		await kv.put('a:1', '1');
		await kv.put('a:2', '2');
		await kv.put('b:1', '3');
		const prefix = await kv.list({ prefix: 'a:' });
		expect(prefix.keys.map((k) => k.name).sort()).toEqual(['a:1', 'a:2']);
	});

	it('supports paginated list via cursor', async () => {
		const kv = createInMemoryKVProvider();
		for (let i = 0; i < 5; i++) {
			await kv.put(`item:${i}`, String(i));
		}

		const first = await kv.list({ prefix: 'item:', limit: 2 });
		expect(first.list_complete).toBe(false);
		expect(first.keys).toHaveLength(2);

		const second = await kv.list({ prefix: 'item:', limit: 2, cursor: first.cursor });
		expect(second.keys).toHaveLength(2);

		const third = await kv.list({ prefix: 'item:', limit: 2, cursor: second.cursor });
		expect(third.list_complete).toBe(true);
		expect(third.keys).toHaveLength(1);
	});

	it('exposes NuxtHub-style aliases (set/del/keys/getItem/setItem/removeItem)', async () => {
		const kv = createInMemoryKVProvider();
		await kv.set('o', { a: 1 });
		expect(await kv.get('o')).toBe(JSON.stringify({ a: 1 }));

		await kv.setItem('s', 'string');
		expect(await kv.getItem<string>('s')).toBe('string');

		expect(await kv.getKeys('')).toContain('o');

		await kv.del('o');
		await kv.removeItem('s');
		expect(kv.size()).toBe(0);
	});

	it('clear() drops everything', async () => {
		const kv = createInMemoryKVProvider();
		await kv.put('a', '1');
		await kv.put('b', '2');
		expect(kv.size()).toBe(2);
		kv.clear();
		expect(kv.size()).toBe(0);
	});
});

describe('InMemory Providers - Insert via Router and Aggregates', () => {
	beforeEach(() => {
		resetConfig();
		clearMigrationCache();
	});

	afterEach(() => {
		resetConfig();
		clearMigrationCache();
	});

	it('insert() returns generated ids and finds the row again via routed first()', async () => {
		const kv = createInMemoryKVProvider();
		initialize({
			kv,
			shards: {
				'db-a': createInMemorySQLProvider(),
				'db-b': createInMemorySQLProvider()
			},
			strategy: 'round-robin',
			disableAutoMigration: true,
			hashShardMappings: false
		});

		await runShard('db-a', 'CREATE TABLE auto_users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)');
		await runShard('db-b', 'CREATE TABLE auto_users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)');

		const result = await insert('INSERT INTO auto_users (name) VALUES (?)', ['Ada']);
		expect(typeof result.generatedId).toBe('number');

		const row = await first<{ id: number; name: string }>(String(result.generatedId), 'SELECT id, name FROM auto_users WHERE id = ?', [
			result.generatedId
		]);
		expect(row?.name).toBe('Ada');
	});

	it('all() with aggregates routes through the in-memory provider', async () => {
		const kv = createInMemoryKVProvider();
		initialize({
			kv,
			shards: { 'db-a': createInMemorySQLProvider() },
			strategy: 'hash',
			disableAutoMigration: true,
			hashShardMappings: false
		});

		await runShard('db-a', 'CREATE TABLE u (id TEXT PRIMARY KEY, score INTEGER)');
		await run('a', 'INSERT INTO u (id, score) VALUES (?, ?)', ['a', 10]);
		await run('b', 'INSERT INTO u (id, score) VALUES (?, ?)', ['b', 20]);

		const result = await allShard<{ total: number }>('db-a', 'SELECT SUM(score) AS total FROM u');
		expect(result.results[0]?.total).toBe(30);
	});
});
