/**
 * @fileoverview Automatic drop-in replacement example for CollegeDB
 *
 * This example demonstrates the new automatic migration capabilities of CollegeDB.
 * Simply configure your existing databases as shards, and CollegeDB will automatically
 * detect existing data and create the necessary mappings - no manual migration required!
 *
 * Features demonstrated:
 * - Zero-configuration automatic migration
 * - Seamless integration with existing databases
 * - Immediate access to existing data
 * - Transparent routing for new and existing records
 *
 * Perfect for migrating existing applications to CollegeDB with zero downtime.
 *
 * @example Deploy this as a Cloudflare Worker:
 * ```bash
 * # 1. Configure wrangler.toml with your existing D1 databases and KV namespace
 * # 2. Deploy with: wrangler deploy
 * # 3. Your existing data is immediately accessible!
 * ```
 *
 * @author CollegeDB Team
 * @since 1.0.0
 */

import { first, getShardStats, initialize, run } from '../src/index.js';
import type { Env } from '../src/types.js';

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		const url = new URL(request.url);

		try {
			if (url.pathname === '/demo') {
				return await handleAutomaticDemo(env);
			} else if (url.pathname === '/stats') {
				return await handleStats(env);
			} else {
				return new Response(
					`
# CollegeDB Automatic Drop-in Replacement Demo

Welcome to the automatic migration demo! CollegeDB now automatically detects and integrates existing databases.

## Endpoints:
- /demo  - Demonstrate automatic integration with existing data
- /stats - Show shard statistics after automatic migration

## How it works:
1. Configure existing databases as shards in initialize()
2. CollegeDB automatically detects existing data
3. Mappings are created in the background
4. Existing data becomes immediately queryable!

No migration scripts, no manual setup, no downtime! 🎉
				`,
					{
						headers: { 'Content-Type': 'text/plain' }
					}
				);
			}
		} catch (error) {
			return new Response(`Error: ${error}`, { status: 500 });
		}
	}
};

/**
 * Demonstrates automatic integration with existing databases
 */
async function handleAutomaticDemo(env: Env): Promise<Response> {
	console.log('🚀 Starting automatic drop-in replacement demo...');

	// Step 1: Initialize CollegeDB with existing databases
	// This is ALL you need to do - automatic migration happens here!
	initialize({
		kv: env.KV,
		coordinator: env.ShardCoordinator,
		shards: {
			'db-users': env.ExistingUserDB || env.DB_EAST, // Your existing database
			'db-orders': env.ExistingOrderDB || env.DB_WEST, // Another existing database
			'db-new': env.NewDB || env.DB_CENTRAL // Optional new shard
		},
		strategy: 'hash'
	});

	console.log('✅ CollegeDB initialized - automatic migration running in background');

	const results = [];

	// Step 2: Try to access existing data (assuming some exists)
	const sampleExistingIds = ['user-1', 'user-2', 'existing-user', 'test-user'];

	for (const userId of sampleExistingIds) {
		try {
			const result = await first(userId, 'SELECT * FROM users WHERE id = ?', [userId]);

			if (result) {
				results.push({
					type: 'existing_data',
					userId,
					found: true,
					data: result,
					message: 'Existing data automatically accessible!'
				});
			} else {
				results.push({
					type: 'existing_data',
					userId,
					found: false,
					message: 'No existing data found for this ID'
				});
			}
		} catch (error) {
			results.push({
				type: 'existing_data',
				userId,
				error: String(error)
			});
		}
	}

	// Step 3: Insert new data - gets automatically distributed
	const newUsers = [
		{ id: 'auto-user-1', name: 'Alice Auto', email: 'alice@auto.com' },
		{ id: 'auto-user-2', name: 'Bob Auto', email: 'bob@auto.com' }
	];

	for (const user of newUsers) {
		try {
			await run(user.id, 'INSERT OR REPLACE INTO users (id, name, email) VALUES (?, ?, ?)', [user.id, user.name, user.email]);

			// Immediately query it back
			const result = await first(user.id, 'SELECT * FROM users WHERE id = ?', [user.id]);

			results.push({
				type: 'new_data',
				userId: user.id,
				action: 'inserted_and_retrieved',
				data: result,
				message: 'New data automatically distributed and queryable!'
			});
		} catch (error) {
			results.push({
				type: 'new_data',
				userId: user.id,
				error: String(error)
			});
		}
	}

	// Step 4: Update existing data if any was found
	const existingUser = results.find((r) => r.type === 'existing_data' && r.found);
	if (existingUser && existingUser.data) {
		try {
			await run(existingUser.userId, 'UPDATE users SET name = ? WHERE id = ?', [
				`${existingUser.data.name} (Updated)`,
				existingUser.userId
			]);

			const updatedResult = await first(existingUser.userId, 'SELECT * FROM users WHERE id = ?', [existingUser.userId]);

			results.push({
				type: 'updated_data',
				userId: existingUser.userId,
				action: 'updated_existing',
				data: updatedResult,
				message: 'Existing data successfully updated through sharding!'
			});
		} catch (error) {
			results.push({
				type: 'updated_data',
				userId: existingUser.userId,
				error: String(error)
			});
		}
	}

	return new Response(
		JSON.stringify(
			{
				message: 'Automatic drop-in replacement demo completed!',
				summary: {
					total_operations: results.length,
					existing_data_found: results.filter((r) => r.type === 'existing_data' && r.found).length,
					new_data_created: results.filter((r) => r.type === 'new_data' && !r.error).length,
					updates_performed: results.filter((r) => r.type === 'updated_data' && !r.error).length
				},
				details: results,
				note: 'All operations automatically routed to correct shards without manual migration!'
			},
			null,
			2
		),
		{
			headers: { 'Content-Type': 'application/json' }
		}
	);
}

/**
 * Shows shard statistics after automatic migration
 */
async function handleStats(env: Env): Promise<Response> {
	// Initialize CollegeDB (automatic migration happens here if needed)
	initialize({
		kv: env.KV,
		coordinator: env.ShardCoordinator,
		shards: {
			'db-users': env.ExistingUserDB || env.DB_EAST,
			'db-orders': env.ExistingOrderDB || env.DB_WEST,
			'db-new': env.NewDB || env.DB_CENTRAL
		},
		strategy: 'hash'
	});

	try {
		const stats = await getShardStats();

		return new Response(
			JSON.stringify(
				{
					message: 'Shard statistics after automatic migration',
					stats,
					total_records: stats.reduce((sum, shard) => sum + shard.count, 0),
					note: 'These counts include automatically migrated existing data'
				},
				null,
				2
			),
			{
				headers: { 'Content-Type': 'application/json' }
			}
		);
	} catch (error) {
		return new Response(
			JSON.stringify({
				error: String(error),
				message: 'Failed to get shard statistics'
			}),
			{
				status: 500,
				headers: { 'Content-Type': 'application/json' }
			}
		);
	}
}
