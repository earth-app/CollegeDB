/**
 * @fileoverview Valkey / Redis KV integration example for CollegeDB.
 *
 * Valkey is API-compatible with Redis, so both `createValkeyKVProvider` and
 * `createRedisKVProvider` map onto the same `RedisLikeClient` contract.
 * Either of the official `valkey` or `redis` Node.js clients works. The
 * example below uses `redis` for clarity.
 *
 * The SQL side can be any provider — this example uses the in-memory
 * provider so the file runs without external SQL infrastructure, but the
 * pattern is identical for PostgreSQL/MySQL/SQLite.
 *
 * Quick start:
 *
 * ```bash
 * docker run -d --name valkey -p 6379:6379 valkey/valkey:8
 * bun examples/valkey-redis-integration.ts
 * ```
 *
 * @author CollegeDB Team
 * @since 1.2.2
 */

import { createClient as createRedisClient } from 'redis';
import {
	allAllShardsGlobal,
	countAllShards,
	createInMemorySQLProvider,
	createValkeyKVProvider,
	first,
	getShardStats,
	initialize,
	KVShardMapper,
	run,
	runShard,
	type CollegeDBConfig
} from '../src/index';

async function main() {
	const valkey = createRedisClient({ url: process.env.VALKEY_URL ?? process.env.REDIS_URL ?? 'redis://localhost:6379' });
	await valkey.connect();

	// `createValkeyKVProvider` shares its implementation with `createRedisKVProvider`,
	// so the same code works against Redis 7+ and Valkey 8.
	const config: CollegeDBConfig = {
		kv: createValkeyKVProvider(valkey as any),
		shards: {
			'db-east': createInMemorySQLProvider(),
			'db-west': createInMemorySQLProvider()
		},
		strategy: 'hash',
		disableAutoMigration: true
	};

	initialize(config);

	for (const shard of ['db-east', 'db-west']) {
		await runShard(shard, 'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)');
		await runShard(shard, 'CREATE TABLE IF NOT EXISTS posts (id TEXT PRIMARY KEY, user_id TEXT NOT NULL, title TEXT NOT NULL)');
	}

	await run('user-1', 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', ['user-1', 'Ada', 'ada@example.com']);

	// Multi-key lookup: a user can be addressed by email, username, or the
	// primary key. The Valkey KV store holds every mapping under the
	// `shard:` prefix and we expose a side-by-side hashed lookup so the
	// router can find the row by any of those identifiers.
	const mapper = new KVShardMapper(config.kv);
	await mapper.addLookupKeys('user-1', ['email:ada@example.com', 'username:ada']);

	// `first` resolves the primary key, while `firstByLookupKey` resolves the
	// secondary identifier and routes the SQL to the right shard.
	const direct = await first<{ name: string }>('user-1', 'SELECT name FROM users WHERE id = ?', ['user-1']);
	console.log('Direct lookup:', direct);

	// Insert a post on each user to demonstrate sharding.
	for (let i = 0; i < 4; i++) {
		const postId = `post-${i}`;
		await run(postId, 'INSERT INTO posts (id, user_id, title) VALUES (?, ?, ?)', [postId, 'user-1', `Post ${i}`]);
	}

	const recent = await allAllShardsGlobal<{ id: string; title: string }>('SELECT id, title FROM posts WHERE user_id = ?', ['user-1'], {
		sortBy: 'id',
		sortDirection: 'desc',
		limit: 10
	});
	console.log('Cross-shard recent posts:', recent.results);

	const counts = await countAllShards('posts');
	console.log('Cross-shard post count:', counts.total);

	console.log('Per-shard stats:', await getShardStats());

	await valkey.quit();
}

main().catch((error) => {
	console.error('Valkey integration example failed:', error);
	process.exit(1);
});
