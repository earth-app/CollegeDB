/**
 * @fileoverview Multi-column migration example for CollegeDB
 *
 * Demonstrates how to migrate existing databases with multiple lookup columns
 * including username, email, and name fields as additional lookup keys.
 *
 * @example
 * ```typescript
 * // Cloudflare Worker with multi-column migration
 * export default {
 *   async fetch(request, env) {
 *     return await runMultiColumnMigrationDemo(env);
 *   }
 * } satisfies ExportedHandler<Env>;
 * ```
 *
 * @author Gregory Mitchell
 * @since 1.0.4
 */

import type { D1Database, KVNamespace } from '@cloudflare/workers-types';
import { KVShardMapper } from '../src/kvmap.js';
import { autoDetectAndMigrate, discoverExistingRecordsWithColumns, integrateExistingDatabase } from '../src/migrations.js';
import type { CollegeDBConfig } from '../src/types.js';

interface Env {
	KV: KVNamespace;
	DB_EXISTING: D1Database;
	DB_EAST: D1Database;
	DB_WEST: D1Database;
}

/**
 * Demonstrates multi-column migration for existing databases
 */
export async function runMultiColumnMigrationDemo(env: Env): Promise<Response> {
	try {
		// Step 1: Setup existing database with user data
		await setupExistingDatabase(env.DB_EXISTING);

		// Step 2: Configure CollegeDB with multi-column migration enabled
		const config: CollegeDBConfig = {
			kv: env.KV,
			shards: {
				'db-east': env.DB_EAST,
				'db-west': env.DB_WEST
			},
			strategy: 'hash',
			hashShardMappings: true
		};

		const mapper = new KVShardMapper(env.KV, { hashShardMappings: true });

		// Step 3: Integrate existing database with multi-column lookup
		console.log('🔄 Integrating existing database with multi-column lookup...');

		const result = await integrateExistingDatabase(env.DB_EXISTING, 'db-existing', mapper, {
			tables: ['users'],
			migrateOtherColumns: true // Enable multi-column migration
		});

		console.log(`✅ Integration completed:`);
		console.log(`  - Tables processed: ${result.tablesProcessed}`);
		console.log(`  - Total records: ${result.totalRecords}`);
		console.log(`  - Mappings created: ${result.mappingsCreated}`);

		// Step 4: Demonstrate multi-key lookups
		await demonstrateMultiKeyLookups(mapper);

		// Step 5: Show automatic migration with multi-columns
		await demonstrateAutoMigration(env, config);

		return new Response(
			JSON.stringify({
				success: true,
				integration: result,
				message: 'Multi-column migration demo completed successfully'
			}),
			{
				headers: { 'Content-Type': 'application/json' }
			}
		);
	} catch (error) {
		console.error('❌ Multi-column migration demo failed:', error);
		return new Response(
			JSON.stringify({
				success: false,
				error: error instanceof Error ? error.message : 'Unknown error'
			}),
			{
				status: 500,
				headers: { 'Content-Type': 'application/json' }
			}
		);
	}
}

/**
 * Sets up an existing database with sample user data
 */
async function setupExistingDatabase(db: D1Database): Promise<void> {
	console.log('📊 Setting up existing database with sample data...');

	// Create users table with multiple lookup columns
	await db
		.prepare(
			`
		CREATE TABLE IF NOT EXISTS users (
			id TEXT PRIMARY KEY,
			username TEXT UNIQUE,
			email TEXT UNIQUE,
			name TEXT,
			created_at INTEGER
		)
	`
		)
		.run();

	// Insert sample users with various column combinations
	const users = [
		{
			id: 'user-1',
			username: 'johndoe',
			email: 'john.doe@example.com',
			name: 'John Doe',
			created_at: Date.now()
		},
		{
			id: 'user-2',
			username: 'janesmith',
			email: 'jane.smith@example.com',
			name: 'Jane Smith',
			created_at: Date.now()
		},
		{
			id: 'user-3',
			username: 'bobwilson',
			email: 'bob@company.com',
			name: 'Bob Wilson',
			created_at: Date.now()
		},
		{
			id: 'user-4',
			username: null, // Test with missing username
			email: 'alice@test.com',
			name: 'Alice Brown',
			created_at: Date.now()
		},
		{
			id: 'user-5',
			username: 'charlie123',
			email: null, // Test with missing email
			name: 'Charlie Davis',
			created_at: Date.now()
		}
	];

	for (const user of users) {
		await db
			.prepare(
				`
			INSERT OR REPLACE INTO users (id, username, email, name, created_at)
			VALUES (?, ?, ?, ?, ?)
		`
			)
			.bind(user.id, user.username, user.email, user.name, user.created_at)
			.run();
	}

	console.log(`✅ Created users table with ${users.length} sample records`);
}

/**
 * Demonstrates looking up users by different keys
 */
async function demonstrateMultiKeyLookups(mapper: KVShardMapper): Promise<void> {
	console.log('🔍 Demonstrating multi-key lookups...');

	// Test primary key lookup
	const byId = await mapper.getShardMapping('user-1');
	console.log(`  🔑 Lookup by ID (user-1): ${byId?.shard || 'Not found'}`);

	// Test username lookup
	const byUsername = await mapper.getShardMapping('username:johndoe');
	console.log(`  👤 Lookup by username (johndoe): ${byUsername?.shard || 'Not found'}`);

	// Test email lookup
	const byEmail = await mapper.getShardMapping('email:john.doe@example.com');
	console.log(`  📧 Lookup by email (john.doe@example.com): ${byEmail?.shard || 'Not found'}`);

	// Test name lookup
	const byName = await mapper.getShardMapping('name:John Doe');
	console.log(`  📛 Lookup by name (John Doe): ${byName?.shard || 'Not found'}`);

	// Show all lookup keys for a user
	const allKeys = await mapper.getAllLookupKeys('user-2');
	console.log(`  🔗 All lookup keys for user-2:`, allKeys);
}

/**
 * Demonstrates automatic migration with multi-column support
 */
async function demonstrateAutoMigration(env: Env, config: CollegeDBConfig): Promise<void> {
	console.log('🤖 Demonstrating automatic migration with multi-columns...');

	// Create another database with new data
	await env.DB_EAST.prepare(
		`
		CREATE TABLE IF NOT EXISTS posts (
			id TEXT PRIMARY KEY,
			username TEXT,  -- Will be used as lookup key
			email TEXT,     -- Will be used as lookup key
			title TEXT,
			content TEXT,
			created_at INTEGER
		)
	`
	).run();

	// Insert some posts with author information
	await env.DB_EAST.prepare(
		`
		INSERT OR REPLACE INTO posts (id, username, email, title, content, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`
	)
		.bind('post-1', 'johndoe', 'john.doe@example.com', 'My First Post', 'Hello world!', Date.now())
		.run();

	await env.DB_EAST.prepare(
		`
		INSERT OR REPLACE INTO posts (id, username, email, title, content, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`
	)
		.bind('post-2', 'janesmith', 'jane.smith@example.com', 'Welcome Post', 'Welcome to my blog!', Date.now())
		.run();

	// Trigger auto-migration with multi-column support
	const migrationResult = await autoDetectAndMigrate(env.DB_EAST, 'db-east', config, {
		migrateOtherColumns: true,
		tablesToCheck: ['posts']
	});

	console.log('✅ Auto-migration results:');
	console.log(`  - Migration needed: ${migrationResult.migrationNeeded}`);
	console.log(`  - Migration performed: ${migrationResult.migrationPerformed}`);
	console.log(`  - Records migrated: ${migrationResult.recordsMigrated}`);
	console.log(`  - Tables processed: ${migrationResult.tablesProcessed}`);

	if (migrationResult.issues.length > 0) {
		console.log('  - Issues:', migrationResult.issues);
	}

	// Test lookups for the auto-migrated posts
	const mapper = new KVShardMapper(config.kv, { hashShardMappings: config.hashShardMappings });

	const postByUsername = await mapper.getShardMapping('username:johndoe');
	console.log(`  🔍 Post lookup by username (johndoe): ${postByUsername?.shard || 'Not found'}`);
}

/**
 * Shows discovered records with available columns
 */
export async function showDiscoveredRecords(env: Env): Promise<Response> {
	try {
		// Setup the database first
		await setupExistingDatabase(env.DB_EXISTING);

		// Discover records with all available columns
		const records = await discoverExistingRecordsWithColumns(env.DB_EXISTING, 'users');

		console.log('🔍 Discovered records with columns:');
		records.forEach((record, index) => {
			console.log(`  Record ${index + 1}:`);
			console.log(`    ID: ${record.id}`);
			console.log(`    Username: ${record.username || 'N/A'}`);
			console.log(`    Email: ${record.email || 'N/A'}`);
			console.log(`    Name: ${record.name || 'N/A'}`);
		});

		return new Response(
			JSON.stringify({
				success: true,
				recordCount: records.length,
				records: records
			}),
			{
				headers: { 'Content-Type': 'application/json' }
			}
		);
	} catch (error) {
		return new Response(
			JSON.stringify({
				success: false,
				error: error instanceof Error ? error.message : 'Unknown error'
			}),
			{
				status: 500,
				headers: { 'Content-Type': 'application/json' }
			}
		);
	}
}
