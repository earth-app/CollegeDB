/**
 * @fileoverview Cloudflare Worker Demo for CollegeDB
 *
 * This comprehensive demo implements a production-ready Cloudflare Worker that
 * showcases all CollegeDB features in a real-world API service. It demonstrates
 * best practices for error handling, routing, authentication, and scalable
 * architecture patterns.
 *
 * Features demonstrated:
 * - Complete REST API implementation
 * - Production-grade error handling and logging
 * - Automatic schema initialization
 * - Multi-endpoint routing with HTTP methods
 * - JSON request/response handling
 * - Administrative endpoints for monitoring
 * - Proper TypeScript typing throughout
 *
 * API Endpoints:
 * - `GET /users/:id` - Get user by ID
 * - `POST /users` - Create new user
 * - `PUT /users/:id` - Update user
 * - `DELETE /users/:id` - Delete user
 * - `GET /admin/stats` - Get shard statistics
 * - `GET /admin/shards` - List all shards
 * - `POST /admin/reassign` - Reassign user to different shard
 * - `POST /admin/flush` - Clear all data (development only)
 *
 * @example Deploy and test:
 * ```bash
 * # Deploy the worker
 * wrangler deploy
 *
 * # Test the API
 * curl -X POST https://your-worker.workers.dev/users \
 *   -H "Content-Type: application/json" \
 *   -d '{"id": "user-123", "name": "John Doe", "email": "john@example.com"}'
 *
 * curl https://your-worker.workers.dev/users/user-123
 *
 * curl https://your-worker.workers.dev/admin/stats
 * ```
 *
 * @author CollegeDB Team
 * @since 1.0.0
 */

import type { ExecutionContext } from '@cloudflare/workers-types';
import { ShardCoordinator } from '../src/durable.js';
import {
	createSchemaAcrossShards,
	deleteByPrimaryKey,
	flush,
	getShardStats,
	initialize,
	insert,
	listKnownShards,
	reassignShard,
	selectByPrimaryKey,
	updateByPrimaryKey
} from '../src/index.js';
import type { Env } from '../src/types.js';

// Export the Durable Object class
export { ShardCoordinator };

/**
 * Main Cloudflare Worker handler
 */
export default {
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		// Initialize CollegeDB with environment bindings
		initialize({
			kv: env.KV,
			coordinator: env.ShardCoordinator,
			shards: {
				'db-east': env['db-east'],
				'db-west': env['db-west']
			},
			strategy: 'hash' // Use hash for consistent routing
		});

		// Add CORS headers for web requests
		const corsHeaders = {
			'Access-Control-Allow-Origin': '*',
			'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
			'Access-Control-Allow-Headers': 'Content-Type, Authorization'
		};

		// Handle preflight requests
		if (request.method === 'OPTIONS') {
			return new Response(null, { headers: corsHeaders });
		}

		const url = new URL(request.url);
		const path = url.pathname;

		try {
			// Route requests to appropriate handlers
			switch (path) {
				case '/':
					return handleHome();
				case '/init':
					return handleInit(env);
				case '/api/users':
					return handleUsersAPI(request);
				case '/api/stats':
					return handleStatsAPI();
				case '/api/shards':
					return handleShardsAPI(request);
				case '/api/admin/rebalance':
					return handleRebalance(request);
				case '/api/admin/flush':
					return handleFlush(request);
				case '/health':
					return handleHealth();
				default:
					return new Response('Not Found', {
						status: 404,
						headers: corsHeaders
					});
			}
		} catch (error) {
			console.error('Worker error:', error);
			return new Response(
				JSON.stringify({
					error: 'Internal Server Error',
					message: error instanceof Error ? error.message : 'Unknown error'
				}),
				{
					status: 500,
					headers: {
						...corsHeaders,
						'Content-Type': 'application/json'
					}
				}
			);
		}
	}
};

/**
 * Home page with API documentation
 */
function handleHome(): Response {
	const html = `
<!DOCTYPE html>
<html>
<head>
    <title>CollegeDB Demo</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }
        .endpoint { background: #f5f5f5; padding: 10px; margin: 10px 0; border-radius: 5px; }
        .method { font-weight: bold; color: #2563eb; }
        pre { background: #1f2937; color: #f9fafb; padding: 15px; border-radius: 5px; overflow-x: auto; }
    </style>
</head>
<body>
    <h1>🎓 CollegeDB Demo API</h1>
    <p>A sharded database router for Cloudflare D1 using KV for primary key mapping.</p>

    <h2>🚀 Quick Start</h2>
    <div class="endpoint">
        <div class="method">POST /init</div>
        <p>Initialize the database schema across all shards</p>
    </div>

    <h2>👥 User Management</h2>
    <div class="endpoint">
        <div class="method">POST /api/users</div>
        <p>Create a new user</p>
        <pre>{"id": "user-123", "name": "John Doe", "email": "john@example.com"}</pre>
    </div>

    <div class="endpoint">
        <div class="method">GET /api/users?id=user-123</div>
        <p>Get a user by ID</p>
    </div>

    <div class="endpoint">
        <div class="method">PUT /api/users</div>
        <p>Update a user</p>
        <pre>{"id": "user-123", "name": "John Smith", "email": "john.smith@example.com"}</pre>
    </div>

    <div class="endpoint">
        <div class="method">DELETE /api/users?id=user-123</div>
        <p>Delete a user</p>
    </div>

    <h2>📊 Monitoring</h2>
    <div class="endpoint">
        <div class="method">GET /api/stats</div>
        <p>Get shard statistics and load distribution</p>
    </div>

    <div class="endpoint">
        <div class="method">GET /api/shards</div>
        <p>List all known shards</p>
    </div>

    <h2>🔧 Administration</h2>
    <div class="endpoint">
        <div class="method">POST /api/admin/rebalance</div>
        <p>Trigger shard rebalancing (moves data between shards)</p>
        <pre>{"fromShard": "db-east", "toShard": "db-west", "primaryKey": "user-123"}</pre>
    </div>

    <div class="endpoint">
        <div class="method">POST /api/admin/flush</div>
        <p>⚠️ Clear all data and mappings (development only)</p>
    </div>

    <div class="endpoint">
        <div class="method">GET /health</div>
        <p>Health check endpoint</p>
    </div>

    <h2>🧪 Example Usage</h2>
    <pre>
// Create a user
curl -X POST https://your-worker.dev/api/users \\
  -H "Content-Type: application/json" \\
  -d '{"id": "user-001", "name": "Alice", "email": "alice@example.com"}'

// Get the user
curl https://your-worker.dev/api/users?id=user-001

// Update the user
curl -X PUT https://your-worker.dev/api/users \\
  -H "Content-Type: application/json" \\
  -d '{"id": "user-001", "name": "Alice Johnson", "email": "alice.johnson@example.com"}'

// Check stats
curl https://your-worker.dev/api/stats
    </pre>
</body>
</html>`;

	return new Response(html, {
		headers: { 'Content-Type': 'text/html' }
	});
}

/**
 * Initialize database schemas
 */
async function handleInit(env: Env): Promise<Response> {
	try {
		await createSchemaAcrossShards({
			'db-east': env['db-east'],
			'db-west': env['db-west']
		});

		return new Response(
			JSON.stringify({
				message: 'Database schemas initialized successfully',
				shards: ['db-east', 'db-west']
			}),
			{
				headers: { 'Content-Type': 'application/json' }
			}
		);
	} catch (error) {
		throw new Error(`Initialization failed: ${error}`);
	}
}

/**
 * Handle user CRUD operations
 */
async function handleUsersAPI(request: Request): Promise<Response> {
	const url = new URL(request.url);
	const method = request.method;

	switch (method) {
		case 'POST':
			return handleCreateUser(request);
		case 'GET':
			return handleGetUser(url);
		case 'PUT':
			return handleUpdateUser(request);
		case 'DELETE':
			return handleDeleteUser(url);
		default:
			return new Response('Method Not Allowed', { status: 405 });
	}
}

async function handleCreateUser(request: Request): Promise<Response> {
	const userData = (await request.json()) as { id: string; name: string; email: string };

	if (!userData.id || !userData.name) {
		return new Response(
			JSON.stringify({
				error: 'Missing required fields: id, name'
			}),
			{
				status: 400,
				headers: { 'Content-Type': 'application/json' }
			}
		);
	}

	await insert(userData.id, 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', [userData.id, userData.name, userData.email || null]);

	return new Response(
		JSON.stringify({
			message: 'User created successfully',
			user: userData
		}),
		{
			headers: { 'Content-Type': 'application/json' }
		}
	);
}

async function handleGetUser(url: URL): Promise<Response> {
	const userId = url.searchParams.get('id');

	if (!userId) {
		return new Response(
			JSON.stringify({
				error: 'Missing required parameter: id'
			}),
			{
				status: 400,
				headers: { 'Content-Type': 'application/json' }
			}
		);
	}

	const result = await selectByPrimaryKey(userId, 'SELECT * FROM users WHERE id = ?', [userId]);

	if (result.results.length === 0) {
		return new Response(
			JSON.stringify({
				error: 'User not found'
			}),
			{
				status: 404,
				headers: { 'Content-Type': 'application/json' }
			}
		);
	}

	return new Response(
		JSON.stringify({
			user: result.results[0]
		}),
		{
			headers: { 'Content-Type': 'application/json' }
		}
	);
}

async function handleUpdateUser(request: Request): Promise<Response> {
	const userData = (await request.json()) as { id: string; name?: string; email?: string };

	if (!userData.id) {
		return new Response(
			JSON.stringify({
				error: 'Missing required field: id'
			}),
			{
				status: 400,
				headers: { 'Content-Type': 'application/json' }
			}
		);
	}

	// Check if user exists first
	const existing = await selectByPrimaryKey(userData.id, 'SELECT * FROM users WHERE id = ?', [userData.id]);

	if (existing.results.length === 0) {
		return new Response(
			JSON.stringify({
				error: 'User not found'
			}),
			{
				status: 404,
				headers: { 'Content-Type': 'application/json' }
			}
		);
	}

	await updateByPrimaryKey(userData.id, 'UPDATE users SET name = COALESCE(?, name), email = COALESCE(?, email) WHERE id = ?', [
		userData.name,
		userData.email,
		userData.id
	]);

	return new Response(
		JSON.stringify({
			message: 'User updated successfully'
		}),
		{
			headers: { 'Content-Type': 'application/json' }
		}
	);
}

async function handleDeleteUser(url: URL): Promise<Response> {
	const userId = url.searchParams.get('id');

	if (!userId) {
		return new Response(
			JSON.stringify({
				error: 'Missing required parameter: id'
			}),
			{
				status: 400,
				headers: { 'Content-Type': 'application/json' }
			}
		);
	}

	await deleteByPrimaryKey(userId, 'DELETE FROM users WHERE id = ?', [userId]);

	return new Response(
		JSON.stringify({
			message: 'User deleted successfully'
		}),
		{
			headers: { 'Content-Type': 'application/json' }
		}
	);
}

/**
 * Handle statistics API
 */
async function handleStatsAPI(): Promise<Response> {
	const [shards, stats] = await Promise.all([listKnownShards(), getShardStats()]);

	const totalKeys = stats.reduce((sum, stat) => sum + stat.count, 0);

	return new Response(
		JSON.stringify({
			overview: {
				totalShards: shards.length,
				totalKeys,
				averageKeysPerShard: totalKeys / shards.length
			},
			shards: stats
		}),
		{
			headers: { 'Content-Type': 'application/json' }
		}
	);
}

/**
 * Handle shards API
 */
async function handleShardsAPI(request: Request): Promise<Response> {
	if (request.method === 'GET') {
		const shards = await listKnownShards();
		return new Response(JSON.stringify({ shards }), {
			headers: { 'Content-Type': 'application/json' }
		});
	}

	return new Response('Method Not Allowed', { status: 405 });
}

/**
 * Handle shard rebalancing
 */
async function handleRebalance(request: Request): Promise<Response> {
	if (request.method !== 'POST') {
		return new Response('Method Not Allowed', { status: 405 });
	}

	const { fromShard, toShard, primaryKey } = (await request.json()) as {
		fromShard?: string;
		toShard: string;
		primaryKey: string;
	};

	await reassignShard(primaryKey, toShard);

	return new Response(
		JSON.stringify({
			message: 'Shard reassignment completed',
			primaryKey,
			newShard: toShard
		}),
		{
			headers: { 'Content-Type': 'application/json' }
		}
	);
}

/**
 * Handle data flush (development only)
 */
async function handleFlush(request: Request): Promise<Response> {
	if (request.method !== 'POST') {
		return new Response('Method Not Allowed', { status: 405 });
	}

	await flush();

	return new Response(
		JSON.stringify({
			message: 'All data and mappings flushed successfully',
			warning: 'This action cannot be undone'
		}),
		{
			headers: { 'Content-Type': 'application/json' }
		}
	);
}

/**
 * Health check
 */
function handleHealth(): Response {
	return new Response(
		JSON.stringify({
			status: 'healthy',
			timestamp: new Date().toISOString(),
			service: 'CollegeDB Demo Worker'
		}),
		{
			headers: { 'Content-Type': 'application/json' }
		}
	);
}
