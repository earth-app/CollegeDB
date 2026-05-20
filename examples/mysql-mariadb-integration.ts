/**
 * @fileoverview MySQL / MariaDB integration example for CollegeDB.
 *
 * Uses `mysql2/promise` as the SQL driver and Redis as the KV mapper. The
 * same provider also works for MariaDB connections — pass a MariaDB pool
 * built with `mariadb`'s `createPool().promise()` API.
 *
 * Quick start:
 *
 * ```bash
 * docker run -d --name mysql-east  -e MYSQL_ROOT_PASSWORD=root -p 3308:3306 mysql:8
 * docker run -d --name mariadb-west -e MARIADB_ROOT_PASSWORD=root -p 3309:3306 mariadb:12
 * docker run -d --name redis -p 6379:6379 redis:7
 *
 * mysql -h 127.0.0.1 -P 3308 -uroot -proot -e \
 *   "CREATE DATABASE app; USE app; CREATE TABLE users (id VARCHAR(64) PRIMARY KEY, name VARCHAR(255), email VARCHAR(255) UNIQUE)"
 * mysql -h 127.0.0.1 -P 3309 -uroot -proot -e \
 *   "CREATE DATABASE app; USE app; CREATE TABLE users (id VARCHAR(64) PRIMARY KEY, name VARCHAR(255), email VARCHAR(255) UNIQUE)"
 *
 * bun examples/mysql-mariadb-integration.ts
 * ```
 *
 * @author CollegeDB Team
 * @since 1.2.2
 */

import mysql from 'mysql2/promise';
import { createClient as createRedisClient } from 'redis';
import {
	allAllShardsGlobal,
	createMySQLProvider,
	createRedisKVProvider,
	first,
	initialize,
	insert,
	insertShard,
	run,
	type CollegeDBConfig
} from '../src/index';

async function main() {
	const redisClient = createRedisClient({ url: process.env.REDIS_URL ?? 'redis://localhost:6379' });
	await redisClient.connect();

	const mysqlPool = mysql.createPool({
		host: '127.0.0.1',
		port: Number(process.env.MYSQL_PORT ?? 3308),
		user: 'root',
		password: 'root',
		database: 'app',
		// mysql2 returns rows directly when `connectionLimit` is set on the
		// pool — the MySQL adapter knows how to read either shape.
		connectionLimit: 5
	});

	const mariadbPool = mysql.createPool({
		host: '127.0.0.1',
		port: Number(process.env.MARIADB_PORT ?? 3309),
		user: 'root',
		password: 'root',
		database: 'app',
		connectionLimit: 5
	});

	const config: CollegeDBConfig = {
		kv: createRedisKVProvider(redisClient as any),
		shards: {
			'mysql-east': createMySQLProvider(mysqlPool),
			'mariadb-west': createMySQLProvider(mariadbPool)
		},
		strategy: 'hash',
		disableAutoMigration: true
	};

	initialize(config);

	// Routed insert.
	await run('user-1', 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', ['user-1', 'Ada Lovelace', 'ada@example.com']);

	// MySQL/MariaDB auto-increment + AUTO_INCREMENT primary key. CollegeDB
	// captures `insertId` from the OK packet so routed reads work.
	await run(
		'audit-bootstrap',
		`CREATE TABLE IF NOT EXISTS audit_log (
			id BIGINT PRIMARY KEY AUTO_INCREMENT,
			actor VARCHAR(255) NOT NULL,
			action VARCHAR(255) NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`
	);
	const generatedAudit = await insert('INSERT INTO audit_log (actor, action) VALUES (?, ?)', ['user-1', 'login']);
	console.log('Audit id from insertId metadata:', generatedAudit.generatedId);

	// Pinned insert when you already know which shard you want.
	const pinned = await insertShard('mysql-east', 'INSERT INTO audit_log (actor, action) VALUES (?, ?)', ['system', 'startup']);
	console.log('Pinned audit id:', pinned.generatedId);

	const user = await first<{ id: string; email: string }>('user-1', 'SELECT id, email FROM users WHERE id = ?', ['user-1']);
	console.log('Routed user lookup:', user);

	// Cross-shard search with global ordering and pagination.
	const everyone = await allAllShardsGlobal<{ id: string; name: string }>('SELECT id, name FROM users WHERE name LIKE ?', ['%a%'], {
		sortBy: 'name',
		limit: 25
	});
	console.log('Cross-shard search returned', everyone.results.length, 'rows');

	await mysqlPool.end();
	await mariadbPool.end();
	await redisClient.quit();
}

main().catch((error) => {
	console.error('MySQL/MariaDB integration example failed:', error);
	process.exit(1);
});
