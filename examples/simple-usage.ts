/**
 * @fileoverview Simple usage example for CollegeDB
 *
 * This example demonstrates the fundamental CRUD operations (Create, Read, Update, Delete)
 * using CollegeDB for distributed database access across multiple D1 instances. It shows
 * how to set up the library, perform basic operations, and handle routing automatically.
 *
 * Features demonstrated:
 * - System initialization with multiple shards
 * - Schema creation across shards
 * - Primary key-based routing
 * - Basic CRUD operations
 * - Error handling
 *
 * This is perfect for getting started with CollegeDB and understanding the basic concepts.
 *
 * @example Deploy this as a Cloudflare Worker:
 * ```bash
 * # 1. Configure wrangler.toml with your D1 databases and KV namespace
 * # 2. Deploy with: wrangler deploy
 * # 3. Test with: curl https://your-worker.your-subdomain.workers.dev/
 * ```
 *
 * @author CollegeDB Team
 * @since 1.0.0
 */

import { createSchema, deleteByPrimaryKey, initialize, insert, selectByPrimaryKey, updateByPrimaryKey } from '../src/index.js';
import type { Env } from '../src/types.js';

// Example schema for a simple user and posts system
const EXAMPLE_SCHEMA = `
	CREATE TABLE IF NOT EXISTS users (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT UNIQUE NOT NULL,
		created_at INTEGER DEFAULT (strftime('%s', 'now'))
	);

	CREATE TABLE IF NOT EXISTS posts (
		id TEXT PRIMARY KEY,
		user_id TEXT NOT NULL,
		title TEXT NOT NULL,
		content TEXT,
		created_at INTEGER DEFAULT (strftime('%s', 'now')),
		FOREIGN KEY (user_id) REFERENCES users(id)
	);

	CREATE INDEX IF NOT EXISTS idx_posts_user_id ON posts(user_id);
	CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at);
`;

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		// Initialize CollegeDB with your environment bindings
		initialize({
			kv: env.KV,
			coordinator: env.ShardCoordinator,
			shards: {
				'db-east': env['db-east'],
				'db-west': env['db-west']
			},
			strategy: 'round-robin'
		});

		try {
			// Ensure schema exists on all shards
			await createSchema(env['db-east'], EXAMPLE_SCHEMA);
			await createSchema(env['db-west'], EXAMPLE_SCHEMA);

			// Example 1: Insert a new user
			console.log('📝 Inserting user...');
			await insert('user-123', 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', ['user-123', 'Alice Johnson', 'alice@example.com']);

			// Example 2: Select the user
			console.log('🔍 Selecting user...');
			const userResult = await selectByPrimaryKey('user-123', 'SELECT * FROM users WHERE id = ?', ['user-123']);
			console.log('User found:', userResult.results[0]);

			// Example 3: Update the user
			console.log('✏️ Updating user...');
			await updateByPrimaryKey('user-123', 'UPDATE users SET name = ?, email = ? WHERE id = ?', [
				'Alice Smith',
				'alice.smith@example.com',
				'user-123'
			]);

			// Example 4: Verify the update
			const updatedResult = await selectByPrimaryKey('user-123', 'SELECT * FROM users WHERE id = ?', ['user-123']);
			console.log('Updated user:', updatedResult.results[0]);

			// Example 5: Insert multiple users to demonstrate sharding
			const users = [
				{ id: 'user-456', name: 'Bob Wilson', email: 'bob@example.com' },
				{ id: 'user-789', name: 'Carol Davis', email: 'carol@example.com' },
				{ id: 'user-101', name: 'David Brown', email: 'david@example.com' }
			];

			console.log('👥 Inserting multiple users...');
			for (const user of users) {
				await insert(user.id, 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', [user.id, user.name, user.email]);
			}

			// Example 6: Retrieve all users (demonstrating cross-shard queries)
			console.log('📋 Retrieving all users...');
			const allUsers = [];
			for (const user of users.concat([{ id: 'user-123', name: 'Alice Smith', email: 'alice.smith@example.com' }])) {
				const result = await selectByPrimaryKey(user.id, 'SELECT * FROM users WHERE id = ?', [user.id]);
				if (result.results.length > 0) {
					allUsers.push(result.results[0]);
				}
			}
			console.log('All users:', allUsers);

			// Example 7: Delete a user
			console.log('🗑️ Deleting user...');
			await deleteByPrimaryKey('user-789', 'DELETE FROM users WHERE id = ?', ['user-789']);

			// Example 8: Verify deletion
			const deletedResult = await selectByPrimaryKey('user-789', 'SELECT * FROM users WHERE id = ?', ['user-789']);
			console.log('User after deletion:', deletedResult.results.length === 0 ? 'Not found (deleted)' : 'Still exists');

			return new Response(
				JSON.stringify({
					message: 'Simple usage example completed successfully',
					usersCreated: users.length + 1,
					usersDeleted: 1,
					finalUserCount: allUsers.length - 1
				}),
				{
					headers: { 'Content-Type': 'application/json' }
				}
			);
		} catch (error) {
			console.error('Error in simple usage example:', error);
			return new Response(
				JSON.stringify({
					error: 'Example failed',
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

/**
 * Alternative example for use in a non-worker context
 */
export async function runSimpleExample(env: Env) {
	// Initialize CollegeDB
	initialize({
		kv: env.KV,
		coordinator: env.ShardCoordinator,
		shards: {
			'db-east': env['db-east'],
			'db-west': env['db-west']
		},
		strategy: 'hash' // Using hash strategy for consistent routing
	});

	// Create schema
	await createSchema(env['db-east'], EXAMPLE_SCHEMA);
	await createSchema(env['db-west'], EXAMPLE_SCHEMA);

	// Basic operations
	const userId = 'demo-user-' + Date.now();

	// Insert
	await insert(userId, 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', [userId, 'Demo User', 'demo@example.com']);

	// Select
	const result = await selectByPrimaryKey(userId, 'SELECT * FROM users WHERE id = ?', [userId]);
	console.log('Created user:', result.results[0]);

	// Update
	await updateByPrimaryKey(userId, 'UPDATE users SET name = ? WHERE id = ?', ['Updated Demo User', userId]);

	// Select again to verify update
	const updatedResult = await selectByPrimaryKey(userId, 'SELECT * FROM users WHERE id = ?', [userId]);
	console.log('Updated user:', updatedResult.results[0]);

	// Clean up
	await deleteByPrimaryKey(userId, 'DELETE FROM users WHERE id = ?', [userId]);
	console.log('User deleted successfully');
}
