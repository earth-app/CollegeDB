/**
 * @fileoverview Advanced usage example for CollegeDB
 *
 * This comprehensive example demonstrates advanced features and patterns for
 * production usage of CollegeDB. It includes sophisticated shard management,
 * monitoring, cross-shard operations, and administrative functions.
 *
 * Advanced features demonstrated:
 * - Comprehensive shard statistics and monitoring
 * - Cross-shard analytical queries
 * - Dynamic shard reassignment for load balancing
 * - Error handling and recovery strategies
 * - Administrative operations and maintenance
 * - Custom routing and query strategies
 * - Production-ready patterns and best practices
 *
 * Use cases covered:
 * - Load balancing and capacity management
 * - Data migration and shard rebalancing
 * - Multi-tenant data isolation
 * - Analytics across distributed data
 * - System monitoring and health checks
 *
 * @example Usage with different actions:
 * ```bash
 * # View shard statistics
 * curl "https://your-worker.workers.dev/?action=stats"
 *
 * # Perform cross-shard analytics
 * curl "https://your-worker.workers.dev/?action=analytics"
 *
 * # Demonstrate load balancing
 * curl "https://your-worker.workers.dev/?action=load-balance"
 * ```
 *
 * @author CollegeDB Team
 * @since 1.0.0
 */

import {
	createSchema,
	getShardStats,
	initialize,
	insert,
	listKnownShards,
	queryOnShard,
	reassignShard,
	selectByPrimaryKey
} from '../src/index.js';
import type { CollegeDBConfig, Env } from '../src/types.js';

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		const url = new URL(request.url);
		const action = url.searchParams.get('action') || 'demo';

		// Initialize CollegeDB with advanced configuration
		const config: CollegeDBConfig = {
			kv: env.KV,
			coordinator: env.ShardCoordinator,
			shards: {
				'db-east': env['db-east'],
				'db-west': env['db-west'],
				'db-central': env['db-central'] // Third shard for demonstration
			},
			strategy: 'hash' // Consistent hashing for better distribution
		};

		initialize(config);

		try {
			switch (action) {
				case 'setup':
					return await handleSetup(config);
				case 'load-test':
					return await handleLoadTest();
				case 'rebalance':
					return await handleRebalance();
				case 'stats':
					return await handleStats();
				case 'cross-shard-query':
					return await handleCrossShardQuery(config);
				case 'error-recovery':
					return await handleErrorRecovery();
				default:
					return await handleDemo();
			}
		} catch (error) {
			console.error('Advanced example error:', error);
			return new Response(
				JSON.stringify({
					error: 'Advanced example failed',
					message: error instanceof Error ? error.message : 'Unknown error'
				}),
				{
					status: 500,
					headers: { 'Content-Type': 'application/json' }
				}
			);
		}
	}
};

async function handleSetup(config: CollegeDBConfig): Promise<Response> {
	console.log('🚀 Setting up advanced CollegeDB example...');

	// Create schema on all shards
	for (const [shardName, db] of Object.entries(config.shards)) {
		console.log(`Creating schema on ${shardName}...`);
		await createSchema(db);
	}

	// Initialize coordinator with known shards
	if (config.coordinator) {
		const coordinatorId = config.coordinator.idFromName('default');
		const coordinator = config.coordinator.get(coordinatorId);

		for (const shardName of Object.keys(config.shards)) {
			await coordinator.fetch('http://coordinator/shards', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ shard: shardName })
			});
		}
	}

	return new Response(
		JSON.stringify({
			message: 'Setup completed successfully',
			shards: Object.keys(config.shards)
		}),
		{
			headers: { 'Content-Type': 'application/json' }
		}
	);
}

async function handleLoadTest(): Promise<Response> {
	console.log('📊 Running load test...');

	const userCount = 100;
	const startTime = Date.now();
	const results = [];

	// Insert users in parallel batches
	const batchSize = 10;
	for (let i = 0; i < userCount; i += batchSize) {
		const batch = [];
		for (let j = 0; j < batchSize && i + j < userCount; j++) {
			const userId = `load-test-user-${i + j}`;
			batch.push(
				insert(userId, 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', [
					userId,
					`Load Test User ${i + j}`,
					`user${i + j}@loadtest.com`
				])
			);
		}
		await Promise.all(batch);
		results.push(`Batch ${Math.floor(i / batchSize) + 1} completed`);
	}

	const insertTime = Date.now() - startTime;

	// Test read performance
	const readStartTime = Date.now();
	const readPromises = [];
	for (let i = 0; i < userCount; i += 5) {
		// Sample every 5th user
		const userId = `load-test-user-${i}`;
		readPromises.push(selectByPrimaryKey(userId, 'SELECT * FROM users WHERE id = ?', [userId]));
	}
	await Promise.all(readPromises);
	const readTime = Date.now() - readStartTime;

	return new Response(
		JSON.stringify({
			message: 'Load test completed',
			metrics: {
				usersInserted: userCount,
				insertTimeMs: insertTime,
				readSamples: readPromises.length,
				readTimeMs: readTime,
				insertsPerSecond: Math.round((userCount / insertTime) * 1000),
				readsPerSecond: Math.round((readPromises.length / readTime) * 1000)
			},
			batches: results
		}),
		{
			headers: { 'Content-Type': 'application/json' }
		}
	);
}

async function handleRebalance(): Promise<Response> {
	console.log('⚖️ Performing shard rebalancing...');

	const stats = await getShardStats();
	console.log('Current shard stats:', stats);

	// Find the most loaded shard
	const mostLoaded = stats.reduce((max, current) => (current.count > max.count ? current : max));

	// Find the least loaded shard
	const leastLoaded = stats.reduce((min, current) => (current.count < min.count ? current : min));

	if (mostLoaded.count - leastLoaded.count <= 1) {
		return new Response(
			JSON.stringify({
				message: 'Shards are already well balanced',
				stats
			}),
			{
				headers: { 'Content-Type': 'application/json' }
			}
		);
	}

	// Move some keys from most loaded to least loaded
	// const mapper = new KVShardMapper(config.kv); // Would need proper KV access
	const keysToMove = Math.floor((mostLoaded.count - leastLoaded.count) / 2);
	const movedKeys = [];

	// This is a simplified rebalancing - in practice, you'd need more sophisticated logic
	for (let i = 0; i < keysToMove; i++) {
		const keyToMove = `rebalance-key-${i}`;
		try {
			await reassignShard(keyToMove, leastLoaded.binding);
			movedKeys.push(keyToMove);
		} catch (error) {
			console.warn(`Failed to move key ${keyToMove}:`, error);
		}
	}

	const newStats = await getShardStats();

	return new Response(
		JSON.stringify({
			message: 'Rebalancing completed',
			keysMoved: movedKeys.length,
			from: mostLoaded.binding,
			to: leastLoaded.binding,
			oldStats: stats,
			newStats
		}),
		{
			headers: { 'Content-Type': 'application/json' }
		}
	);
}

async function handleStats(): Promise<Response> {
	console.log('📈 Gathering comprehensive statistics...');

	const [shards, stats] = await Promise.all([listKnownShards(), getShardStats()]);

	const totalKeys = stats.reduce((sum, stat) => sum + stat.count, 0);
	const averageKeys = totalKeys / stats.length;
	const loadImbalance = Math.max(...stats.map((s) => s.count)) - Math.min(...stats.map((s) => s.count));

	return new Response(
		JSON.stringify({
			message: 'Statistics gathered successfully',
			overview: {
				totalShards: shards.length,
				totalKeys,
				averageKeysPerShard: Math.round(averageKeys * 100) / 100,
				loadImbalance,
				isWellBalanced: loadImbalance <= 2
			},
			shards: stats,
			knownShards: shards
		}),
		{
			headers: { 'Content-Type': 'application/json' }
		}
	);
}

async function handleCrossShardQuery(config: CollegeDBConfig): Promise<Response> {
	console.log('🌐 Performing cross-shard queries...');

	const results = [];

	// Query each shard directly for aggregate operations
	for (const [shardName, _] of Object.entries(config.shards)) {
		try {
			const result = await queryOnShard(shardName, 'SELECT COUNT(*) as user_count FROM users', []);
			results.push({
				shard: shardName,
				userCount: result.results[0]?.user_count || 0
			});
		} catch (error) {
			console.warn(`Query failed on shard ${shardName}:`, error);
			results.push({
				shard: shardName,
				userCount: 0,
				error: error instanceof Error ? error.message : 'Unknown error'
			});
		}
	}

	const totalUsers = results.reduce((sum, r) => sum + (r.userCount || 0), 0);

	return new Response(
		JSON.stringify({
			message: 'Cross-shard query completed',
			totalUsers,
			shardBreakdown: results
		}),
		{
			headers: { 'Content-Type': 'application/json' }
		}
	);
}

async function handleErrorRecovery(): Promise<Response> {
	console.log('🔧 Testing error recovery scenarios...');

	const tests = [];

	// Test 1: Query non-existent user
	try {
		const result = await selectByPrimaryKey('non-existent-user', 'SELECT * FROM users WHERE id = ?', ['non-existent-user']);
		tests.push({
			test: 'Query non-existent user',
			passed: result.results.length === 0,
			details: 'Should return empty results'
		});
	} catch (error) {
		tests.push({
			test: 'Query non-existent user',
			passed: false,
			error: error instanceof Error ? error.message : 'Unknown error'
		});
	}

	// Test 2: Invalid shard reassignment
	try {
		await reassignShard('test-user', 'invalid-shard');
		tests.push({
			test: 'Invalid shard reassignment',
			passed: false,
			details: 'Should have thrown an error'
		});
	} catch (error) {
		tests.push({
			test: 'Invalid shard reassignment',
			passed: true,
			details: 'Correctly threw error: ' + (error instanceof Error ? error.message : 'Unknown error')
		});
	}

	// Test 3: Malformed SQL
	try {
		await selectByPrimaryKey('test-user', 'INVALID SQL STATEMENT', []);
		tests.push({
			test: 'Malformed SQL',
			passed: false,
			details: 'Should have thrown an error'
		});
	} catch (error) {
		tests.push({
			test: 'Malformed SQL',
			passed: true,
			details: 'Correctly handled SQL error'
		});
	}

	const passedTests = tests.filter((t) => t.passed).length;
	const totalTests = tests.length;

	return new Response(
		JSON.stringify({
			message: 'Error recovery tests completed',
			summary: `${passedTests}/${totalTests} tests passed`,
			tests
		}),
		{
			headers: { 'Content-Type': 'application/json' }
		}
	);
}

async function handleDemo(): Promise<Response> {
	console.log('🎯 Running advanced demo...');

	// Create some sample data with different patterns
	const demoUsers = [
		{ id: 'admin-001', name: 'System Admin', email: 'admin@company.com', role: 'admin' },
		{ id: 'user-001', name: 'John Doe', email: 'john@company.com', role: 'user' },
		{ id: 'user-002', name: 'Jane Smith', email: 'jane@company.com', role: 'user' },
		{ id: 'guest-001', name: 'Guest User', email: 'guest@company.com', role: 'guest' }
	];

	// Insert users and track which shard they go to
	const userPlacements = [];
	for (const user of demoUsers) {
		await insert(user.id, 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', [user.id, user.name, user.email]);

		// This would require access to the KV to determine actual shard
		userPlacements.push({
			userId: user.id,
			name: user.name,
			role: user.role
		});
	}

	const stats = await getShardStats();

	return new Response(
		JSON.stringify({
			message: 'Advanced demo completed successfully',
			demoUsers: userPlacements,
			shardDistribution: stats,
			features: [
				'Hash-based consistent routing',
				'Load balancing across shards',
				'Cross-shard statistics',
				'Error recovery handling',
				'Coordinator-based allocation'
			]
		}),
		{
			headers: { 'Content-Type': 'application/json' }
		}
	);
}
