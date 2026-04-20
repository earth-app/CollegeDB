/**
 * @fileoverview Example showing location-based sharding for geographic optimization
 *
 * This example demonstrates how to configure CollegeDB with location-aware
 * sharding to minimize latency by routing data to geographically closer shards.
 *
 * @example
 * ```bash
 * # Run this example
 * bun run examples/location-sharding.ts
 * ```
 */

import type { CollegeDBConfig, D1Region } from '../src/index';
import { collegedb, createRedisKVProvider, createSQLiteProvider, first, getShardStats, run } from '../src/index';

class InMemoryRedisLike {
	private readonly store = new Map<string, string>();

	async get(key: string): Promise<string | null> {
		return this.store.get(key) ?? null;
	}

	async set(key: string, value: string): Promise<void> {
		this.store.set(key, value);
	}

	async del(key: string): Promise<void> {
		this.store.delete(key);
	}

	async scan(_cursor: string, ...args: any[]): Promise<[string, string[]]> {
		const matchIndex = args.findIndex((arg) => arg === 'MATCH');
		const pattern = matchIndex >= 0 ? String(args[matchIndex + 1]) : '*';
		const prefix = pattern.endsWith('*') ? pattern.slice(0, -1) : pattern;
		const keys = Array.from(this.store.keys()).filter((key) => key.startsWith(prefix));
		return ['0', keys];
	}
}

class InMemorySQLiteLike {
	private readonly users = new Map<string, { id: string; name: string; region: string }>();

	prepare(sql: string) {
		return {
			run: (...bindings: any[]) => {
				if (sql.startsWith('CREATE TABLE')) {
					return { changes: 0 };
				}
				if (sql.startsWith('INSERT')) {
					this.users.set(String(bindings[0]), {
						id: String(bindings[0]),
						name: String(bindings[1]),
						region: String(bindings[2])
					});
					return { changes: 1, lastInsertRowid: 1 };
				}
				return { changes: 0 };
			},
			all: (...bindings: any[]) => {
				if (sql.startsWith('SELECT') && bindings.length > 0) {
					const user = this.users.get(String(bindings[0]));
					return user ? [user] : [];
				}
				return Array.from(this.users.values());
			},
			get: (...bindings: any[]) => {
				if (sql.startsWith('SELECT') && bindings.length > 0) {
					return this.users.get(String(bindings[0])) ?? null;
				}
				return null;
			}
		};
	}
}

function createInMemoryShard() {
	return createSQLiteProvider(new InMemorySQLiteLike());
}

// Example configuration for a global application
const config: CollegeDBConfig = {
	kv: createRedisKVProvider(new InMemoryRedisLike()),
	strategy: 'location',
	targetRegion: 'wnam', // Target Western North America
	shardLocations: {
		'db-west-us': { region: 'wnam', priority: 3 }, // Primary (San Francisco)
		'db-east-us': { region: 'enam', priority: 2 }, // Secondary (New York)
		'db-europe': { region: 'weur', priority: 1 }, // Tertiary (London)
		'db-asia': { region: 'apac', priority: 1 } // Tertiary (Tokyo)
	},
	shards: {
		'db-west-us': createInMemoryShard(),
		'db-east-us': createInMemoryShard(),
		'db-europe': createInMemoryShard(),
		'db-asia': createInMemoryShard()
	}
};

/**
 * Example: Global e-commerce application with geographic optimization
 */
async function locationShardingExample() {
	console.log('🌍 Location-Based Sharding Example');
	console.log('==================================');

	await collegedb(config, async () => {
		// Users will be allocated based on geographic proximity
		console.log('\n📍 Creating users with location-optimized sharding:');

		// These users will likely be assigned to db-west-us (closest to target region)
		await run('user-ca-123', 'INSERT INTO users (id, name, region) VALUES (?, ?, ?)', ['user-ca-123', 'California User', 'US-West']);

		await run('user-wa-456', 'INSERT INTO users (id, name, region) VALUES (?, ?, ?)', ['user-wa-456', 'Washington User', 'US-West']);

		// Query the users (routed to optimal shards automatically)
		const caUser = await first('user-ca-123', 'SELECT * FROM users WHERE id = ?', ['user-ca-123']);
		const waUser = await first('user-wa-456', 'SELECT * FROM users WHERE id = ?', ['user-wa-456']);

		console.log('✅ Users created and retrieved with optimal routing');
		console.log(`   CA User: ${caUser?.name}`);
		console.log(`   WA User: ${waUser?.name}`);

		// Check shard distribution
		const stats = await getShardStats();
		console.log('\n📊 Shard Distribution:');
		stats.forEach((shard) => {
			console.log(`   ${shard.binding}: ${shard.count} records`);
		});

		console.log('\n🚀 Benefits of Location Strategy:');
		console.log('   • 20-40ms latency reduction for target region');
		console.log('   • Intelligent fallback to nearby regions');
		console.log('   • Priority-based shard selection');
		console.log('   • Consistent allocation with geographic awareness');
	});
}

/**
 * Example: Multi-region deployment strategies
 */
async function multiRegionExample() {
	console.log('\n🌐 Multi-Region Deployment Strategies');
	console.log('===================================');

	// Example configurations for different target regions
	const regions: { region: D1Region; name: string }[] = [
		{ region: 'wnam', name: 'Western North America' },
		{ region: 'weur', name: 'Western Europe' },
		{ region: 'apac', name: 'Asia Pacific' }
	];

	for (const { region, name } of regions) {
		console.log(`\n📍 Optimal configuration for ${name} (${region}):`);

		// Show which shards would be preferred for each region
		const testConfig: CollegeDBConfig = {
			...config,
			targetRegion: region
		};

		console.log('   Preferred shard order:');

		// Mock the selection logic to show priority
		if (region === 'wnam') {
			console.log('   1. db-west-us (same region)');
			console.log('   2. db-east-us (nearby region)');
			console.log('   3. db-europe/db-asia (distant regions)');
		} else if (region === 'weur') {
			console.log('   1. db-europe (same region)');
			console.log('   2. db-east-us (nearby region)');
			console.log('   3. db-west-us/db-asia (distant regions)');
		} else if (region === 'apac') {
			console.log('   1. db-asia (same region)');
			console.log('   2. db-west-us (nearby region)');
			console.log('   3. db-europe/db-east-us (distant regions)');
		}
	}
}

/**
 * Example: Disaster recovery and fallback scenarios
 */
async function disasterRecoveryExample() {
	console.log('\n🚨 Disaster Recovery Example');
	console.log('============================');

	// Simulate primary region being unavailable
	const drConfig: CollegeDBConfig = {
		...config,
		shardLocations: {
			// db-west-us removed (simulating outage)
			'db-east-us': { region: 'enam', priority: 3 }, // Now primary
			'db-europe': { region: 'weur', priority: 2 }, // Secondary
			'db-asia': { region: 'apac', priority: 1 } // Tertiary
		},
		shards: {
			'db-east-us': createInMemoryShard(),
			'db-europe': createInMemoryShard(),
			'db-asia': createInMemoryShard()
		}
	};

	console.log('   Primary region (db-west-us) unavailable');
	console.log('   Automatic failover priority:');
	console.log('   1. db-east-us (next closest to wnam target)');
	console.log('   2. db-europe (good connectivity)');
	console.log('   3. db-asia (fallback)');
	console.log('   ✅ No application code changes required!');
}

// Run examples if this file is executed directly
if (import.meta.main) {
	await locationShardingExample();
	await multiRegionExample();
	await disasterRecoveryExample();
}

export { disasterRecoveryExample, locationShardingExample, multiRegionExample };
