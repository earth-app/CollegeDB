/**
 * @fileoverview Drop-in replacement example for CollegeDB
 *
 * This example demonstrates how to integrate CollegeDB with existing D1 databases
 * that already contain data. It shows the complete process of discovering existing
 * tables, validating schemas, creating shard mappings, and transitioning to
 * distributed operations.
 *
 * Features demonstrated:
 * - Existing database discovery and validation
 * - Automatic shard mapping creation for existing data
 * - Drop-in integration process
 * - Transition from single database to sharded system
 * - Validation and error handling
 *
 * Perfect for migrating existing applications to use CollegeDB without data loss.
 *
 * @example Deploy this as a Cloudflare Worker for migration:
 * ```bash
 * # 1. Configure wrangler.toml with your existing D1 databases and KV namespace
 * # 2. Deploy with: wrangler deploy
 * # 3. Run migration: curl https://your-worker.your-subdomain.workers.dev/migrate
 * ```
 *
 * @author CollegeDB Team
 * @since 1.0.0
 */

import {
	discoverExistingPrimaryKeys,
	initialize,
	integrateExistingDatabase,
	listTables,
	selectByPrimaryKey,
	validateTableForSharding
} from '../src/index.js';
import { KVShardMapper } from '../src/kvmap.js';
import type { Env } from '../src/types.js';

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		const url = new URL(request.url);

		try {
			if (url.pathname === '/migrate') {
				return await handleMigration(env);
			} else if (url.pathname === '/validate') {
				return await handleValidation(env);
			} else if (url.pathname === '/demo') {
				return await handleDemo(env);
			} else {
				return new Response(
					'CollegeDB Drop-in Replacement Demo\n\nEndpoints:\n/migrate - Perform database integration\n/validate - Validate existing databases\n/demo - Demo queries after integration',
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
 * Handles the migration process for existing databases
 */
async function handleMigration(env: Env): Promise<Response> {
	const results: any[] = [];

	// Initialize CollegeDB
	initialize({
		kv: env.KV,
		coordinator: env.ShardCoordinator,
		shards: {
			'db-existing-east': env['db-existing-east'], // Your existing database
			'db-existing-west': env['db-existing-west'], // Another existing database
			'db-new-central': env['db-new-central'] // Optional new shard
		},
		strategy: 'hash'
	});

	const mapper = new KVShardMapper(env.KV);

	// Integrate existing databases
	const databases = [
		{ db: env['db-existing-east'], name: 'db-existing-east' },
		{ db: env['db-existing-west'], name: 'db-existing-west' }
	];

	for (const { db, name } of databases) {
		console.log(`🔄 Integrating existing database: ${name}`);

		const result = await integrateExistingDatabase(db, name, mapper, {
			tables: ['users', 'posts', 'orders'], // Specify your existing tables
			primaryKeyColumn: 'id',
			strategy: 'hash',
			addShardMappingsTable: true,
			dryRun: false // Set to true for testing
		});

		results.push({
			database: name,
			...result
		});

		if (result.success) {
			console.log(`✅ Successfully integrated ${name}:`);
			console.log(`   - Tables processed: ${result.tablesProcessed}`);
			console.log(`   - Records mapped: ${result.totalRecords}`);
			console.log(`   - Mappings created: ${result.mappingsCreated}`);
		} else {
			console.log(`❌ Integration failed for ${name}:`);
			result.issues.forEach((issue: string) => console.log(`   - ${issue}`));
		}
	}

	return new Response(
		JSON.stringify(
			{
				message: 'Migration completed',
				results
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
 * Handles validation of existing databases before migration
 */
async function handleValidation(env: Env): Promise<Response> {
	const validations: any[] = [];

	const databases = [
		{ db: env['db-existing-east'], name: 'db-existing-east' },
		{ db: env['db-existing-west'], name: 'db-existing-west' }
	];

	for (const { db, name } of databases) {
		console.log(`🔍 Validating database: ${name}`);

		// List all tables
		const tables = await listTables(db);
		console.log(`   Found tables: ${tables.join(', ')}`);

		// Validate each table
		const tableValidations = [];
		for (const table of tables) {
			if (table === 'shard_mappings') continue; // Skip internal table

			const validation = await validateTableForSharding(db, table);
			tableValidations.push(validation);

			if (validation.isValid) {
				console.log(`   ✅ ${table}: ${validation.recordCount} records`);
			} else {
				console.log(`   ❌ ${table}: ${validation.issues.join(', ')}`);
			}
		}

		// Discover primary keys for valid tables
		const primaryKeyDiscovery = [];
		for (const validation of tableValidations) {
			if (validation.isValid) {
				const primaryKeys = await discoverExistingPrimaryKeys(db, validation.tableName);
				primaryKeyDiscovery.push({
					table: validation.tableName,
					primaryKeys: primaryKeys.slice(0, 5), // Show first 5 as sample
					totalCount: primaryKeys.length
				});
			}
		}

		validations.push({
			database: name,
			tables,
			tableValidations,
			primaryKeyDiscovery
		});
	}

	return new Response(
		JSON.stringify(
			{
				message: 'Validation completed',
				validations
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
 * Demonstrates querying after drop-in integration
 */
async function handleDemo(env: Env): Promise<Response> {
	// Initialize CollegeDB (assuming migration is complete)
	initialize({
		kv: env.KV,
		coordinator: env.ShardCoordinator,
		shards: {
			'db-existing-east': env['db-existing-east'],
			'db-existing-west': env['db-existing-west'],
			'db-new-central': env['db-new-central']
		},
		strategy: 'hash'
	});

	const results: any[] = [];

	// Example: Query existing users (these will be automatically routed to correct shards)
	const sampleUserIds = ['user-1', 'user-100', 'user-999'];

	for (const userId of sampleUserIds) {
		try {
			const result = await selectByPrimaryKey(userId, 'SELECT * FROM users WHERE id = ?', [userId]);

			results.push({
				userId,
				found: result.results.length > 0,
				data: result.results[0] || null,
				metadata: result.meta
			});
		} catch (error) {
			results.push({
				userId,
				found: false,
				error: String(error)
			});
		}
	}

	return new Response(
		JSON.stringify(
			{
				message: 'Demo queries completed',
				note: 'Queries automatically routed to correct shards based on existing data',
				results
			},
			null,
			2
		),
		{
			headers: { 'Content-Type': 'application/json' }
		}
	);
}
