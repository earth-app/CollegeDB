/**
 * @fileoverview Sandbox example: In-memory provider benchmark
 *
 * This file demonstrates how to use CollegeDB's in-memory mock providers
 * for lightweight testing and local development without external dependencies.
 *
 * Run this with: bun sandbox/memory-example.ts
 */

import {
	all,
	clearMigrationCache,
	createInMemoryKVProvider,
	createInMemorySQLProvider,
	first,
	initialize,
	resetConfig,
	run,
	runShard
} from '../src/index';
import type { CollegeDBConfig } from '../src/types';

const BENCH_SCHEMA = `
	CREATE TABLE IF NOT EXISTS users (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT UNIQUE
	);

	CREATE TABLE IF NOT EXISTS posts (
		id TEXT PRIMARY KEY,
		user_id TEXT NOT NULL,
		title TEXT NOT NULL,
		content TEXT
	);
`;

interface BenchmarkResult {
	name: string;
	duration: number;
	opsPerSecond: number;
}

const results: BenchmarkResult[] = [];

function logSection(title: string): void {
	console.log(`\n${'='.repeat(60)}`);
	console.log(`${title}`);
	console.log(`${'='.repeat(60)}\n`);
}

function recordBenchmark(name: string, durationMs: number, operationCount: number): void {
	const opsPerSecond = (operationCount / durationMs) * 1000;
	results.push({ name, duration: durationMs, opsPerSecond });
	console.log(`✓ ${name}: ${durationMs.toFixed(2)}ms (${opsPerSecond.toFixed(2)} ops/sec)`);
}

async function benchmarkBasicCRUD(): Promise<void> {
	logSection('Benchmark: Basic CRUD Operations');

	resetConfig();
	clearMigrationCache();

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

	// Create schema
	for (const shardKey of ['shard-1', 'shard-2', 'shard-3']) {
		const result = await runShard(
			shardKey,
			`CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)`
		);
		if (!result.success) {
			console.error(`Failed to create schema on ${shardKey}`);
		}
	}

	const iterations = 100;
	const startTime = performance.now();

	for (let i = 0; i < iterations; i++) {
		const id = `user-${i}`;
		const email = `user${i}@example.com`;

		// Insert
		await run(id, 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', [id, `User ${i}`, email]);

		// Select
		const user = await first<{ id: string; name: string }>(id, 'SELECT id, name FROM users WHERE id = ?', [id]);

		// Update
		if (user) {
			await run(id, 'UPDATE users SET name = ? WHERE id = ?', [`User ${i} Updated`, id]);
		}

		// Delete
		await run(id, 'DELETE FROM users WHERE id = ?', [id]);
	}

	const duration = performance.now() - startTime;
	recordBenchmark('Basic CRUD (100 iterations)', duration, iterations * 4); // 4 ops per iteration
}

async function benchmarkMultiShardDistribution(): Promise<void> {
	logSection('Benchmark: Multi-Shard Data Distribution');

	resetConfig();
	clearMigrationCache();

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

	// Create schema across all shards
	for (const shardKey of ['shard-1', 'shard-2', 'shard-3']) {
		await runShard(shardKey, `CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL)`);
	}

	const insertCount = 50;
	const startTime = performance.now();

	for (let i = 0; i < insertCount; i++) {
		const id = `dist-user-${i}`;
		await run(id, 'INSERT INTO users (id, name) VALUES (?, ?)', [id, `Distributed User ${i}`]);
	}

	const duration = performance.now() - startTime;
	recordBenchmark(`Multi-shard distribution (${insertCount} inserts)`, duration, insertCount);

	// Verify distribution
	const shard1 = await runShard('shard-1', 'SELECT COUNT(*) as count FROM users');
	const shard2 = await runShard('shard-2', 'SELECT COUNT(*) as count FROM users');
	const shard3 = await runShard('shard-3', 'SELECT COUNT(*) as count FROM users');

	const count1 = shard1.results[0]?.count as number;
	const count2 = shard2.results[0]?.count as number;
	const count3 = shard3.results[0]?.count as number;

	console.log(`\nData distribution:`);
	console.log(`  Shard 1: ${count1} records`);
	console.log(`  Shard 2: ${count2} records`);
	console.log(`  Shard 3: ${count3} records`);
	console.log(`  Total: ${count1 + count2 + count3} records\n`);
}

async function benchmarkKVStorage(): Promise<void> {
	logSection('Benchmark: In-Memory KV Storage');

	const kv = createInMemoryKVProvider();

	const iterations = 200;
	const startTime = performance.now();

	for (let i = 0; i < iterations; i++) {
		const key = `kv-key-${i}`;
		const value = `kv-value-${i}`;

		// Put operation
		await kv.put(key, value);

		// Get operation
		await kv.get(key, 'text');
	}

	const duration = performance.now() - startTime;
	recordBenchmark(`KV storage operations (${iterations} iterations)`, duration, iterations * 2);
}

async function benchmarkRoundRobinStrategy(): Promise<void> {
	logSection('Benchmark: Different Sharding Strategies');

	// Test round-robin
	resetConfig();
	clearMigrationCache();

	const config: CollegeDBConfig = {
		kv: createInMemoryKVProvider(),
		shards: {
			'shard-1': createInMemorySQLProvider(),
			'shard-2': createInMemorySQLProvider(),
			'shard-3': createInMemorySQLProvider()
		},
		strategy: 'round-robin'
	};

	initialize(config);

	for (const shardKey of ['shard-1', 'shard-2', 'shard-3']) {
		await runShard(shardKey, `CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL)`);
	}

	const iterations = 60;
	const startTime = performance.now();

	for (let i = 0; i < iterations; i++) {
		const id = `rr-user-${i}`;
		await run(id, 'INSERT INTO users (id, name) VALUES (?, ?)', [id, `Round-Robin User ${i}`]);
	}

	const duration = performance.now() - startTime;
	recordBenchmark(`Round-robin strategy (${iterations} inserts)`, duration, iterations);

	// Check distribution
	const distributions = await Promise.all([
		runShard('shard-1', 'SELECT COUNT(*) as count FROM users'),
		runShard('shard-2', 'SELECT COUNT(*) as count FROM users'),
		runShard('shard-3', 'SELECT COUNT(*) as count FROM users')
	]);

	console.log(`\nRound-robin distribution:`);
	distributions.forEach((shard, idx) => {
		const count = shard.results[0]?.count as number;
		console.log(`  Shard ${idx + 1}: ${count} records`);
	});
}

async function benchmarkJoinQueries(): Promise<void> {
	logSection('Benchmark: JOIN Queries');

	resetConfig();
	clearMigrationCache();

	const config: CollegeDBConfig = {
		kv: createInMemoryKVProvider(),
		shards: {
			'shard-1': createInMemorySQLProvider(),
			'shard-2': createInMemorySQLProvider()
		},
		strategy: 'hash'
	};

	initialize(config);

	for (const shardKey of ['shard-1', 'shard-2']) {
		await runShard(shardKey, 'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL)');
		await runShard(shardKey, 'CREATE TABLE IF NOT EXISTS posts (id TEXT PRIMARY KEY, user_id TEXT NOT NULL, title TEXT NOT NULL)');
	}

	const userCount = 20;

	// Insert users and posts
	for (let i = 0; i < userCount; i++) {
		const userId = `user-${i}`;
		await run(userId, 'INSERT INTO users (id, name) VALUES (?, ?)', [userId, `User ${i}`]);

		for (let j = 0; j < 2; j++) {
			const postId = `user-${i}-post-${j}`;
			await run(postId, 'INSERT INTO posts (id, user_id, title) VALUES (?, ?, ?)', [postId, userId, `Post ${j}`]);
		}
	}

	const startTime = performance.now();

	// Run queries
	for (let i = 0; i < userCount; i++) {
		const userId = `user-${i}`;
		// Query posts for each user
		await all(userId, 'SELECT id, user_id, title FROM posts WHERE user_id = ?', [userId]);
	}

	const duration = performance.now() - startTime;
	recordBenchmark(`JOIN queries (${userCount} iterations)`, duration, userCount);
}

async function printSummary(): Promise<void> {
	logSection('Summary');

	console.log('Benchmark Results:');
	console.log(`${'Name'.padEnd(50)} | ${'Duration'.padEnd(12)} | ${'Ops/Sec'.padEnd(12)}`);
	console.log(`${'-'.repeat(50)}-+-${'-'.repeat(12)}-+-${'-'.repeat(12)}`);

	for (const result of results) {
		console.log(`${result.name.padEnd(50)} | ${result.duration.toFixed(2).padEnd(12)} | ${result.opsPerSecond.toFixed(2).padEnd(12)}`);
	}

	const totalDuration = results.reduce((sum, r) => sum + r.duration, 0);
	console.log(`\nTotal Benchmark Time: ${totalDuration.toFixed(2)}ms`);
}

async function main(): Promise<void> {
	console.log('\n🏠 CollegeDB In-Memory Provider Sandbox');
	console.log('Testing routing, sharding, and basic operations with zero external dependencies\n');

	try {
		await benchmarkBasicCRUD();
		await benchmarkMultiShardDistribution();
		await benchmarkKVStorage();
		await benchmarkRoundRobinStrategy();
		await benchmarkJoinQueries();

		await printSummary();

		console.log('\n✓ All benchmarks completed successfully!\n');
	} catch (error) {
		console.error('❌ Benchmark failed:', error);
		process.exit(1);
	}
}

main();
