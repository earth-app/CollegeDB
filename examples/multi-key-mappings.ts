/**
 * @fileoverview Multi-Key Shard Mappings Example for CollegeDB
 *
 * This example demonstrates CollegeDB's multi-key mapping feature, which allows
 * you to query the same record using multiple unique identifiers (username, email,
 * ID, etc.) while maintaining data privacy through SHA-256 hashing.
 *
 * Features demonstrated:
 * - Creating mappings with multiple lookup keys
 * - Querying by different key types (username, email, ID)
 * - SHA-256 hashing for privacy and security
 * - Adding lookup keys to existing mappings
 * - Managing multi-key mappings
 *
 * Security features:
 * - Automatic SHA-256 hashing of sensitive keys (emails, usernames)
 * - Privacy-preserving KV storage (hashed keys only)
 * - Configurable hashing (can be disabled for development)
 *
 * @example Usage scenarios:
 * ```bash
 * # Create user with multiple lookup methods
 * curl "https://your-worker.workers.dev/?action=create-user&id=123&username=john&email=john@example.com"
 *
 * # Query by email
 * curl "https://your-worker.workers.dev/?action=get-user&email=john@example.com"
 *
 * # Query by username
 * curl "https://your-worker.workers.dev/?action=get-user&username=john"
 * ```
 *
 * @author CollegeDB Team
 * @since 1.0.3
 */

import { collegedb, first, run } from '../src/index.js';
import { KVShardMapper } from '../src/kvmap.js';
import type { CollegeDBConfig, Env } from '../src/types.js';

// Example schema with user table
const USER_SCHEMA = `
	CREATE TABLE IF NOT EXISTS users (
		id TEXT PRIMARY KEY,
		username TEXT UNIQUE NOT NULL,
		email TEXT UNIQUE NOT NULL,
		full_name TEXT NOT NULL,
		created_at INTEGER DEFAULT (strftime('%s', 'now')),
		last_login INTEGER,
		status TEXT DEFAULT 'active'
	);

	CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
	CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
	CREATE INDEX IF NOT EXISTS idx_users_status ON users(status);
`;

/**
 * User interface for type safety
 */
interface User {
	id: string;
	username: string;
	email: string;
	full_name: string;
	created_at: number;
	last_login?: number;
	status: string;
}

/**
 * Cloudflare Worker fetch handler demonstrating multi-key mappings
 */
export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		const url = new URL(request.url);
		const action = url.searchParams.get('action') || 'demo';

		try {
			// Initialize CollegeDB with hashing enabled (default)
			const config: CollegeDBConfig = {
				kv: env.KV,
				shards: {
					'db-users-east': env.DB_USERS_EAST,
					'db-users-west': env.DB_USERS_WEST,
					'db-users-central': env.DB_USERS_CENTRAL
				},
				strategy: 'hash',
				hashShardMappings: true // Enable SHA-256 hashing for privacy
			};

			const result = await collegedb(config, async () => {
				// Ensure tables exist on all shards
				await run('setup', USER_SCHEMA);

				switch (action) {
					case 'demo':
						return await runCompleteDemo(env);

					case 'create-user':
						return await createUserWithMultipleKeys(url.searchParams, env);

					case 'get-user':
						return await getUserByAnyKey(url.searchParams);

					case 'add-lookup-key':
						return await addLookupKeyDemo(url.searchParams, env);

					case 'security-demo':
						return await securityDemo(env);

					case 'management-demo':
						return await managementDemo(env);

					default:
						return { error: 'Unknown action' };
				}
			});

			return new Response(JSON.stringify(result, null, 2), {
				headers: { 'Content-Type': 'application/json' }
			});
		} catch (error: any) {
			return new Response(
				JSON.stringify({
					error: error.message,
					stack: error.stack
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
 * Complete demonstration of multi-key mapping features
 */
async function runCompleteDemo(env: Env) {
	const results: any = { steps: [] };

	// Step 1: Create user with multiple lookup keys
	results.steps.push('Creating user with multiple lookup keys...');

	const mapper = new KVShardMapper(env.KV, { hashShardMappings: true });
	const userId = 'user-' + Date.now();
	const username = 'demo_user_' + Date.now();
	const email = `demo${Date.now()}@example.com`;

	// Create the user record
	await run(userId, 'INSERT INTO users (id, username, email, full_name, status) VALUES (?, ?, ?, ?, ?)', [
		userId,
		username,
		email,
		'Demo User',
		'active'
	]);

	// Set up multi-key mapping
	await mapper.setShardMapping(userId, 'db-users-east', [`username:${username}`, `email:${email}`]);

	results.user_created = {
		id: userId,
		username,
		email,
		lookup_keys: [userId, `username:${username}`, `email:${email}`]
	};

	// Step 2: Query by different keys
	results.steps.push('Querying user by different keys...');

	const userById = await first<User>(userId, 'SELECT * FROM users WHERE id = ?', [userId]);
	const userByUsername = await first<User>(`username:${username}`, 'SELECT * FROM users WHERE username = ?', [username]);
	const userByEmail = await first<User>(`email:${email}`, 'SELECT * FROM users WHERE email = ?', [email]);

	results.query_results = {
		by_id: userById?.id === userId ? 'SUCCESS' : 'FAILED',
		by_username: userByUsername?.id === userId ? 'SUCCESS' : 'FAILED',
		by_email: userByEmail?.id === userId ? 'SUCCESS' : 'FAILED'
	};

	// Step 3: Add additional lookup key
	results.steps.push('Adding additional lookup key...');

	const socialHandle = `@demo${Date.now()}`;
	await mapper.addLookupKeys(userId, [`social:${socialHandle}`]);

	// Test the new lookup key
	const userBySocial = await first<User>(`social:${socialHandle}`, 'SELECT * FROM users WHERE id = ?', [userId]);

	results.additional_key = {
		social_handle: socialHandle,
		query_success: userBySocial?.id === userId ? 'SUCCESS' : 'FAILED'
	};

	// Step 4: Demonstrate key management
	results.steps.push('Getting all lookup keys for user...');

	const allKeys = await mapper.getAllLookupKeys(userId);
	results.all_lookup_keys = allKeys;

	return results;
}

/**
 * Create a user with multiple lookup keys from URL parameters
 */
async function createUserWithMultipleKeys(params: URLSearchParams, env: Env) {
	const id = params.get('id') || 'user-' + Date.now();
	const username = params.get('username') || 'user' + Date.now();
	const email = params.get('email') || `user${Date.now()}@example.com`;
	const fullName = params.get('name') || 'Test User';

	// Create the user record
	await run(id, 'INSERT INTO users (id, username, email, full_name, status) VALUES (?, ?, ?, ?, ?)', [
		id,
		username,
		email,
		fullName,
		'active'
	]);

	// Set up multi-key mapping
	const mapper = new KVShardMapper(env.KV, { hashShardMappings: true });
	await mapper.setShardMapping(id, 'db-users-east', [`username:${username}`, `email:${email}`]);

	return {
		success: true,
		user: { id, username, email, full_name: fullName },
		lookup_keys: [id, `username:${username}`, `email:${email}`],
		message: 'User created with multiple lookup keys'
	};
}

/**
 * Get user by any available key type
 */
async function getUserByAnyKey(params: URLSearchParams) {
	const id = params.get('id');
	const username = params.get('username');
	const email = params.get('email');
	const social = params.get('social');

	let lookupKey: string;
	let query: string;
	let queryParams: string[];

	if (id) {
		lookupKey = id;
		query = 'SELECT * FROM users WHERE id = ?';
		queryParams = [id];
	} else if (username) {
		lookupKey = `username:${username}`;
		query = 'SELECT * FROM users WHERE username = ?';
		queryParams = [username];
	} else if (email) {
		lookupKey = `email:${email}`;
		query = 'SELECT * FROM users WHERE email = ?';
		queryParams = [email];
	} else if (social) {
		lookupKey = `social:${social}`;
		query = 'SELECT * FROM users WHERE id = (SELECT id FROM users LIMIT 1)'; // This is a simplified example
		queryParams = [];
	} else {
		throw new Error('Provide at least one key: id, username, email, or social');
	}

	const user = await first<User>(lookupKey, query, queryParams);

	if (!user) {
		return {
			success: false,
			message: 'User not found',
			lookup_key: lookupKey
		};
	}

	return {
		success: true,
		user,
		lookup_key: lookupKey,
		message: `User found using ${lookupKey.split(':')[0] || 'id'}`
	};
}

/**
 * Demonstrate adding lookup keys to existing mappings
 */
async function addLookupKeyDemo(params: URLSearchParams, env: Env) {
	const existingKey = params.get('key');
	const newKey = params.get('new_key');

	if (!existingKey || !newKey) {
		throw new Error('Provide both "key" and "new_key" parameters');
	}

	const mapper = new KVShardMapper(env.KV, { hashShardMappings: true });

	// Add the new lookup key
	await mapper.addLookupKeys(existingKey, [newKey]);

	// Verify it works
	const originalMapping = await mapper.getShardMapping(existingKey);
	const newMapping = await mapper.getShardMapping(newKey);

	return {
		success: true,
		message: 'Lookup key added successfully',
		existing_key: existingKey,
		new_key: newKey,
		same_shard: originalMapping?.shard === newMapping?.shard,
		shard: originalMapping?.shard
	};
}

/**
 * Demonstrate security features and SHA-256 hashing
 */
async function securityDemo(env: Env) {
	const results: any = {};

	// Create mapper with hashing enabled
	const secureMapper = new KVShardMapper(env.KV, { hashShardMappings: true });

	// Create mapper with hashing disabled for comparison
	const plaintextMapper = new KVShardMapper(env.KV, { hashShardMappings: false });

	const sensitiveEmail = 'sensitive@example.com';
	const userId = 'secure-user-' + Date.now();

	// Set mapping with hashing
	await secureMapper.setShardMapping(userId, 'db-users-east', [`email:${sensitiveEmail}`]);

	// Set mapping without hashing (for demonstration only)
	await plaintextMapper.setShardMapping('plaintext-' + userId, 'db-users-west', [`email:plaintext-${sensitiveEmail}`]);

	results.security_demonstration = {
		hashed_storage: 'Email is hashed with SHA-256 in KV keys',
		plaintext_storage: 'Email is stored in plain text in KV keys',
		recommendation: 'Always use hashShardMappings: true in production',
		privacy_benefit: 'Sensitive data like emails are not visible in KV storage'
	};

	// Demonstrate that lookups still work with hashing
	const hashedResult = await secureMapper.getShardMapping(`email:${sensitiveEmail}`);
	const plaintextResult = await plaintextMapper.getShardMapping(`email:plaintext-${sensitiveEmail}`);

	results.lookup_verification = {
		hashed_lookup: hashedResult ? 'SUCCESS' : 'FAILED',
		plaintext_lookup: plaintextResult ? 'SUCCESS' : 'FAILED',
		message: 'Both approaches work for lookups, but hashing provides privacy'
	};

	return results;
}

/**
 * Demonstrate key management and administrative functions
 */
async function managementDemo(env: Env) {
	const mapper = new KVShardMapper(env.KV, { hashShardMappings: true });
	const results: any = {};

	// Create a test user for management demo
	const userId = 'mgmt-user-' + Date.now();
	const username = 'mgmt_user_' + Date.now();
	const email = `mgmt${Date.now()}@example.com`;

	await mapper.setShardMapping(userId, 'db-users-central', [
		`username:${username}`,
		`email:${email}`,
		`phone:555-0123`,
		`ssn:XXX-XX-XXXX` // Sensitive data that benefits from hashing
	]);

	// Get all lookup keys
	const allKeys = await mapper.getAllLookupKeys(userId);
	results.all_lookup_keys = allKeys;

	// Get shard statistics
	const shardCounts = await mapper.getShardKeyCounts();
	results.shard_statistics = shardCounts;

	// Demonstrate key updating (moves all associated keys)
	await mapper.updateShardMapping(userId, 'db-users-east');
	const updatedMapping = await mapper.getShardMapping(userId);

	results.shard_update = {
		previous_shard: 'db-users-central',
		new_shard: updatedMapping?.shard,
		message: 'All lookup keys now point to the new shard'
	};

	// Verify all keys still work after update
	const verificationResults: any = {};
	for (const key of allKeys.slice(0, 3)) {
		// Test first 3 keys
		const mapping = await mapper.getShardMapping(key);
		verificationResults[key] = {
			shard: mapping?.shard,
			works: mapping?.shard === updatedMapping?.shard
		};
	}

	results.verification_after_update = verificationResults;

	return results;
}
