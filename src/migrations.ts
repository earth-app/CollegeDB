/**
 * @fileoverview Database schema management and data migration utilities for CollegeDB
 *
 * This module provides utilities for managing database schemas across multiple D1 shards
 * and migrating data between shards. It includes default schema definitions, schema
 * validation, and data migration functions that ensure consistency across the distributed
 * database system.
 *
 * Key features:
 * - Default schema creation for typical use cases
 * - Schema validation and existence checking
 * - Data migration between D1 database instances
 * - Batch schema operations across multiple shards
 * - Table discovery and management utilities
 *
 * @example
 * ```typescript
 * import { createSchema, migrateRecord, schemaExists } from './migrations.js';
 *
 * // Create schema on a new shard
 * await createSchema(env.DB_EAST);
 *
 * // Check if schema exists
 * const hasSchema = await schemaExists(env.DB_WEST);
 *
 * // Migrate a user from one shard to another
 * await migrateRecord(env.DB_EAST, env.DB_WEST, 'user-123', 'users');
 * ```
 *
 * @author CollegeDB Team
 * @since 1.0.0
 */

import type { D1Database } from '@cloudflare/workers-types';
import type { CollegeDBConfig } from './types.js';

/**
 * Cache for migration status to avoid repeated checks
 * @private
 */
const migrationStatusCache = new Map<string, boolean>();

/**
 * Default schema for collegedb tables
 *
 * Provides a comprehensive default schema suitable for typical web applications.
 * Includes user management, content posting, and shard mapping tables with
 * appropriate indexes for performance.
 *
 * Tables included:
 * - `shard_mappings`: Stores primary key to shard assignments
 * - `users`: User accounts with email uniqueness constraints
 * - `posts`: User-generated content with foreign key relationships
 *
 * Indexes included:
 * - Email index for fast user lookups
 * - User ID index for efficient post queries
 * - Creation time index for chronological queries
 *
 * @constant
 * @example
 * ```sql
 * -- Users table example
 * INSERT INTO users (id, name, email) VALUES ('user-123', 'John Doe', 'john@example.com');
 *
 * -- Posts table example
 * INSERT INTO posts (id, user_id, title, content) VALUES
 *   ('post-456', 'user-123', 'Hello World', 'My first post!');
 * ```
 */
const DEFAULT_SCHEMA = `
CREATE TABLE IF NOT EXISTS shard_mappings (
  primary_key TEXT PRIMARY KEY,
  shard_name TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT UNIQUE,
  created_at INTEGER DEFAULT (strftime('%s', 'now')),
  updated_at INTEGER DEFAULT (strftime('%s', 'now'))
);

CREATE TABLE IF NOT EXISTS posts (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  title TEXT NOT NULL,
  content TEXT,
  created_at INTEGER DEFAULT (strftime('%s', 'now')),
  updated_at INTEGER DEFAULT (strftime('%s', 'now')),
  FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_posts_user_id ON posts(user_id);
CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at);
`;

/**
 * Creates the default schema in a D1 database
 *
 * Executes SQL statements to create the default table structure and indexes
 * in the specified D1 database. Supports custom schemas and handles SQL
 * statement parsing with comment filtering.
 *
 * The function:
 * 1. Splits the schema into individual SQL statements
 * 2. Filters out comments and empty statements
 * 3. Executes each statement using prepared statements
 * 4. Provides detailed error reporting on failures
 *
 * @public
 * @param d1 - The D1 database instance to create schema in
 * @param customSchema - Optional custom schema SQL to use instead of default
 * @returns Promise that resolves when all schema statements are executed
 * @throws {Error} If any schema statement fails with detailed error information
 * @example
 * ```typescript
 * // Create default schema
 * await createSchema(env.DB_EAST);
 *
 * // Create custom schema
 * const customSQL = `
 *   CREATE TABLE products (
 *     id TEXT PRIMARY KEY,
 *     name TEXT NOT NULL,
 *     price REAL
 *   );
 * `;
 * await createSchema(env.DB_PRODUCTS, customSQL);
 * ```
 */
export async function createSchema(d1: D1Database, customSchema?: string): Promise<void> {
	const schema = customSchema || DEFAULT_SCHEMA;
	const statements = schema
		.split(';')
		.map((stmt) => stmt.trim())
		.filter((stmt) => stmt.length > 0 && !stmt.startsWith('--')); // Filter out comments

	for (const statement of statements) {
		try {
			await (await d1.prepare(statement)).run();
		} catch (error) {
			console.error('Failed to execute schema statement:', statement, error);
			throw new Error(`Schema migration failed: ${error}`);
		}
	}
}

/**
 * Creates schema across multiple D1 databases (all shards)
 *
 * Applies the schema to all provided D1 database instances in parallel.
 * This is useful for initializing a complete sharded database system
 * where all shards need the same table structure.
 *
 * The function executes schema creation on all shards concurrently for
 * performance, but provides detailed error reporting that identifies
 * which specific shard failed if any errors occur.
 *
 * @public
 * @param shards - Record mapping shard names to D1 database instances
 * @param customSchema - Optional custom schema SQL to use instead of default
 * @returns Promise that resolves when schema is created on all shards
 * @throws {Error} If schema creation fails on any shard, with shard identification
 * @example
 * ```typescript
 * const shards = {
 *   'db-east': env.DB_EAST,
 *   'db-west': env.DB_WEST,
 *   'db-central': env.DB_CENTRAL
 * };
 *
 * try {
 *   await createSchemaAcrossShards(shards);
 *   console.log('Schema created on all shards successfully');
 * } catch (error) {
 *   console.error('Schema creation failed:', error.message);
 *   // Error will specify which shard failed
 * }
 * ```
 */
export async function createSchemaAcrossShards(shards: Record<string, D1Database>, customSchema?: string): Promise<void> {
	const promises = Object.entries(shards).map(([shardName, db]) => {
		return createSchema(db, customSchema).catch((error) => {
			throw new Error(`Failed to create schema on shard ${shardName}: ${error.message}`);
		});
	});

	await Promise.all(promises);
}

/**
 * Checks if the schema exists in a D1 database
 *
 * Performs a lightweight check to determine if the expected schema is present
 * in the database. Currently checks for the presence of the 'users' table as
 * an indicator that the full schema has been created.
 *
 * This is useful for:
 * - Conditional schema creation
 * - Health checks and monitoring
 * - Migration validation
 *
 * @public
 * @param d1 - The D1 database instance to check
 * @returns Promise resolving to true if schema tables exist, false otherwise
 * @example
 * ```typescript
 * const hasSchema = await schemaExists(env.DB_NEW_SHARD);
 * if (!hasSchema) {
 *   console.log('Creating schema on new shard...');
 *   await createSchema(env.DB_NEW_SHARD);
 * } else {
 *   console.log('Schema already exists, skipping creation');
 * }
 * ```
 */
export async function schemaExists(d1: D1Database): Promise<boolean> {
	try {
		const result = await (await d1.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")).first();
		return result !== null;
	} catch {
		return false;
	}
}

/**
 * Drops all tables in a D1 database (use with caution!)
 *
 * Removes all tables that are part of the default CollegeDB schema from
 * the specified database. This is a destructive operation that cannot be undone.
 *
 * Tables dropped:
 * - posts (with all user content)
 * - users (with all user accounts)
 * - shard_mappings (with all routing information)
 *
 * ⚠️ **DANGER**: This operation permanently deletes all data in the affected
 * tables. Only use during development, testing, or complete system resets.
 *
 * @public
 * @param d1 - The D1 database instance to drop tables from
 * @returns Promise that resolves when all tables are dropped
 * @example
 * ```typescript
 * // Only use in development/testing environments!
 * if (process.env.NODE_ENV === 'development') {
 *   await dropSchema(env.DB_TEST);
 *   console.log('Test database reset completed');
 * }
 * ```
 */
export async function dropSchema(d1: D1Database): Promise<void> {
	const tables = ['posts', 'users', 'shard_mappings'];

	for (const table of tables) {
		try {
			await (await d1.prepare(`DROP TABLE IF EXISTS ${table}`)).run();
		} catch (error) {
			console.error(`Failed to drop table ${table}:`, error);
		}
	}
}

/**
 * Gets the list of tables in a D1 database
 *
 * Queries the SQLite system catalog to retrieve all user-created tables
 * in the database. This is useful for schema inspection, validation,
 * and debugging purposes.
 *
 * @public
 * @param d1 - The D1 database instance to inspect
 * @returns Promise resolving to array of table names, sorted alphabetically
 * @throws Returns empty array if query fails or database is inaccessible
 * @example
 * ```typescript
 * const tables = await listTables(env.DB_EAST);
 * console.log('Available tables:', tables);
 * // Output: ['posts', 'shard_mappings', 'users']
 *
 * // Check for specific table
 * if (tables.includes('users')) {
 *   console.log('Users table exists');
 * }
 * ```
 */
export async function listTables(d1: D1Database): Promise<string[]> {
	try {
		const result = await (await d1.prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")).all();
		return result.results.map((row: any) => row.name as string);
	} catch {
		return [];
	}
}

/**
 * Migrates data from one shard to another
 *
 * Moves a single record from a source D1 database to a target D1 database.
 * This is typically used during shard rebalancing operations when data needs
 * to be redistributed across shards for load balancing.
 *
 * The migration process:
 * 1. Retrieves the complete record from the source database
 * 2. Ensures the target database has the required schema
 * 3. Inserts the record into the target database (using REPLACE for safety)
 * 4. Deletes the record from the source database
 *
 * The operation is atomic from the perspective of each database, but not
 * across databases. If the operation fails partway through, manual cleanup
 * may be required.
 *
 * @public
 * @param sourceDb - Source D1 database containing the record
 * @param targetDb - Target D1 database to receive the record
 * @param primaryKey - Primary key of the record to migrate
 * @param tableName - Name of the table containing the record (defaults to 'users')
 * @returns Promise that resolves when migration is complete
 * @throws {Error} If source record not found, schema creation fails, or database operations fail
 * @example
 * ```typescript
 * // Migrate a user from east to west shard
 * try {
 *   await migrateRecord(env.DB_EAST, env.DB_WEST, 'user-123', 'users');
 *   console.log('User migration completed successfully');
 * } catch (error) {
 *   console.error('Migration failed:', error.message);
 *   // May need manual cleanup depending on where it failed
 * }
 *
 * // Migrate a post between shards
 * await migrateRecord(sourceDb, targetDb, 'post-456', 'posts');
 * ```
 */
export async function migrateRecord(
	sourceDb: D1Database,
	targetDb: D1Database,
	primaryKey: string,
	tableName: string = 'users'
): Promise<void> {
	// Get the record from source
	const sourceRecord = await (await sourceDb.prepare(`SELECT * FROM ${tableName} WHERE id = ?`)).bind(primaryKey).first();

	if (!sourceRecord) {
		throw new Error(`Record with primary key ${primaryKey} not found in source database`);
	}

	// Create schema if it doesn't exist in target
	if (!(await schemaExists(targetDb))) {
		await createSchema(targetDb);
	}

	// Get column names
	const columns = Object.keys(sourceRecord);
	const placeholders = columns.map(() => '?').join(', ');
	const values = columns.map((col) => sourceRecord[col as keyof typeof sourceRecord]);

	// Insert into target database
	const insertSQL = `INSERT OR REPLACE INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`;
	await (await targetDb.prepare(insertSQL)).bind(...values).run();

	// Delete from source database
	await (await sourceDb.prepare(`DELETE FROM ${tableName} WHERE id = ?`)).bind(primaryKey).run();
}

/**
 * Discovers primary keys in an existing database table
 *
 * Scans an existing table to find all primary keys that need to be mapped to shards.
 * This is useful when integrating CollegeDB with an existing database that already
 * contains data. The function assumes the table has an 'id' column as the primary key.
 *
 * @public
 * @param d1 - The D1 database instance to scan
 * @param tableName - Name of the table to discover primary keys from
 * @param primaryKeyColumn - Name of the primary key column (defaults to 'id')
 * @returns Promise resolving to array of primary key values
 * @throws {Error} If table doesn't exist or database query fails
 * @example
 * ```typescript
 * // Discover all user IDs in an existing users table
 * const userIds = await discoverExistingPrimaryKeys(env.DB_EXISTING, 'users');
 * console.log(`Found ${userIds.length} existing users`);
 *
 * // Discover with custom primary key column
 * const orderIds = await discoverExistingPrimaryKeys(env.DB_ORDERS, 'orders', 'order_id');
 * ```
 */
export async function discoverExistingPrimaryKeys(d1: D1Database, tableName: string, primaryKeyColumn: string = 'id'): Promise<string[]> {
	try {
		const result = await (await d1.prepare(`SELECT ${primaryKeyColumn} FROM ${tableName}`)).all();
		return result.results.map((row: any) => String(row[primaryKeyColumn]));
	} catch (error) {
		throw new Error(`Failed to discover primary keys in table ${tableName}: ${error}`);
	}
}

/**
 * Creates shard mappings for existing primary keys
 *
 * Takes a list of existing primary keys and creates shard mappings for them using
 * the specified allocation strategy. This allows existing data to be integrated
 * into the CollegeDB sharding system without data migration.
 *
 * @public
 * @param primaryKeys - Array of primary key values to create mappings for
 * @param shardBindings - Array of available shard binding names
 * @param strategy - Allocation strategy to use ('hash', 'round-robin', or 'random')
 * @param mapper - KVShardMapper instance for storing mappings
 * @returns Promise that resolves when all mappings are created
 * @throws {Error} If mapping creation fails
 * @example
 * ```typescript
 * import { KVShardMapper } from './kvmap.js';
 *
 * const mapper = new KVShardMapper(env.KV);
 * const existingIds = await discoverExistingPrimaryKeys(env.DB_EXISTING, 'users');
 * const shards = ['db-east', 'db-west', 'db-central'];
 *
 * await createMappingsForExistingKeys(existingIds, shards, 'hash', mapper);
 * console.log('All existing users mapped to shards');
 * ```
 */
export async function createMappingsForExistingKeys(
	primaryKeys: string[],
	shardBindings: string[],
	strategy: 'hash' | 'round-robin' | 'random',
	mapper: any // KVShardMapper instance
): Promise<void> {
	const totalShards = shardBindings.length;

	for (let i = 0; i < primaryKeys.length; i++) {
		const primaryKey = primaryKeys[i]!;
		let selectedShard: string;

		switch (strategy) {
			case 'hash':
				let hash = 0;
				for (let j = 0; j < primaryKey.length; j++) {
					const char = primaryKey.charCodeAt(j);
					hash = (hash << 5) - hash + char;
					hash = hash & hash;
				}
				const hashIndex = Math.abs(hash) % totalShards;
				selectedShard = shardBindings[hashIndex]!;
				break;
			case 'random':
				selectedShard = shardBindings[Math.floor(Math.random() * totalShards)]!;
				break;
			default: // round-robin
				selectedShard = shardBindings[i % totalShards]!;
				break;
		}

		await mapper.setShardMapping(primaryKey, selectedShard);
	}
}

/**
 * Validates that a table has a suitable primary key for CollegeDB
 *
 * Checks if a table exists and has a primary key column that can be used
 * for sharding. Returns information about the table structure and primary key.
 *
 * @public
 * @param d1 - The D1 database instance to check
 * @param tableName - Name of the table to validate
 * @param primaryKeyColumn - Expected primary key column name (defaults to 'id')
 * @returns Promise resolving to validation result with table info
 * @throws {Error} If table doesn't exist or validation fails
 * @example
 * ```typescript
 * const validation = await validateTableForSharding(env.DB_EXISTING, 'users');
 * if (validation.isValid) {
 *   console.log(`Table ${validation.tableName} is ready for sharding`);
 *   console.log(`Primary key: ${validation.primaryKeyColumn}`);
 *   console.log(`Record count: ${validation.recordCount}`);
 * } else {
 *   console.error('Table validation failed:', validation.issues);
 * }
 * ```
 */
export async function validateTableForSharding(
	d1: D1Database,
	tableName: string,
	primaryKeyColumn: string = 'id'
): Promise<{
	isValid: boolean;
	tableName: string;
	primaryKeyColumn: string;
	recordCount: number;
	issues: string[];
}> {
	const issues: string[] = [];
	let recordCount = 0;

	try {
		// Check if table exists
		const tableCheck = await (
			await d1.prepare(`
			SELECT name FROM sqlite_master
			WHERE type='table' AND name=?
		`)
		)
			.bind(tableName)
			.first();

		if (!tableCheck) {
			issues.push(`Table '${tableName}' does not exist`);
			return {
				isValid: false,
				tableName,
				primaryKeyColumn,
				recordCount: 0,
				issues
			};
		}

		// Check if primary key column exists
		const columnCheck = await (await d1.prepare(`PRAGMA table_info(${tableName})`)).all();
		const hasIdColumn = columnCheck.results.some((col: any) => col.name === primaryKeyColumn && col.pk === 1);

		if (!hasIdColumn) {
			issues.push(`Primary key column '${primaryKeyColumn}' not found or not set as primary key`);
		}

		// Get record count
		const countResult = await (await d1.prepare(`SELECT COUNT(*) as count FROM ${tableName}`)).first();
		recordCount = (countResult as any)?.count || 0;

		if (recordCount === 0) {
			issues.push(`Table '${tableName}' is empty`);
		}
	} catch (error) {
		issues.push(`Database validation error: ${error}`);
	}

	return {
		isValid: issues.length === 0,
		tableName,
		primaryKeyColumn,
		recordCount,
		issues
	};
}

/**
 * Performs a complete drop-in integration of an existing database
 *
 * This is the main function for integrating CollegeDB with an existing database
 * that already contains data. It discovers tables, validates them, creates shard
 * mappings, and optionally adds the shard_mappings table if needed.
 *
 * @public
 * @param d1 - The existing D1 database to integrate
 * @param shardName - The shard binding name for this database
 * @param mapper - KVShardMapper instance for storing mappings
 * @param options - Configuration options for the integration
 * @returns Promise resolving to integration summary
 * @throws {Error} If integration fails
 * @example
 * ```typescript
 * import { KVShardMapper } from './kvmap.js';
 *
 * const mapper = new KVShardMapper(env.KV);
 * const result = await integrateExistingDatabase(env.DB_EXISTING, 'db-existing', mapper, {
 *   tables: ['users', 'posts'],
 *   strategy: 'hash',
 *   addShardMappingsTable: true
 * });
 *
 * console.log(`Integrated ${result.totalRecords} records from ${result.tablesProcessed} tables`);
 * ```
 */
export async function integrateExistingDatabase(
	d1: D1Database,
	shardName: string,
	mapper: any, // KVShardMapper instance
	options: {
		tables?: string[];
		primaryKeyColumn?: string;
		strategy?: 'hash' | 'round-robin' | 'random';
		addShardMappingsTable?: boolean;
		dryRun?: boolean;
	} = {}
): Promise<{
	success: boolean;
	shardName: string;
	tablesProcessed: number;
	totalRecords: number;
	mappingsCreated: number;
	issues: string[];
}> {
	const { tables, primaryKeyColumn = 'id', strategy = 'hash', addShardMappingsTable = true, dryRun = false } = options;

	const issues: string[] = [];
	let tablesProcessed = 0;
	let totalRecords = 0;
	let mappingsCreated = 0;

	try {
		// Discover tables if not specified
		const tablesToProcess = tables || (await listTables(d1));

		// Filter out the shard_mappings table if it already exists
		const dataTableNames = tablesToProcess.filter((table) => table !== 'shard_mappings');

		for (const tableName of dataTableNames) {
			try {
				// Validate table
				const validation = await validateTableForSharding(d1, tableName, primaryKeyColumn);

				if (!validation.isValid) {
					issues.push(`Table ${tableName}: ${validation.issues.join(', ')}`);
					continue;
				}

				// Discover existing primary keys
				const primaryKeys = await discoverExistingPrimaryKeys(d1, tableName, primaryKeyColumn);

				if (primaryKeys.length === 0) {
					issues.push(`Table ${tableName} has no records to process`);
					continue;
				}

				// Create shard mappings (all keys in this database go to this shard)
				if (!dryRun) {
					for (const primaryKey of primaryKeys) {
						await mapper.setShardMapping(primaryKey, shardName);
						mappingsCreated++;
					}
				}

				tablesProcessed++;
				totalRecords += primaryKeys.length;
			} catch (error) {
				issues.push(`Failed to process table ${tableName}: ${error}`);
			}
		}

		// Add shard_mappings table if requested and not in dry run
		if (addShardMappingsTable && !dryRun) {
			const hasMappingsTable = (await listTables(d1)).includes('shard_mappings');
			if (!hasMappingsTable) {
				await (
					await d1.prepare(`
					CREATE TABLE IF NOT EXISTS shard_mappings (
						primary_key TEXT PRIMARY KEY,
						shard_name TEXT NOT NULL,
						created_at INTEGER NOT NULL,
						updated_at INTEGER NOT NULL
					);
				`)
				).run();
			}
		}

		// Add this shard to known shards list
		if (!dryRun) {
			await mapper.addKnownShard(shardName);
		}
	} catch (error) {
		issues.push(`Integration failed: ${error}`);
	}

	return {
		success: issues.length === 0 || (issues.length > 0 && tablesProcessed > 0),
		shardName,
		tablesProcessed,
		totalRecords,
		mappingsCreated,
		issues
	};
}

/**
 * Automatically detects if a database needs migration and performs it
 *
 * This function is called automatically by CollegeDB operations to detect
 * existing databases that contain data but haven't been integrated into the
 * sharding system. It performs seamless migration without user intervention.
 *
 * The detection process:
 * 1. Checks if the database has data tables with primary keys
 * 2. Verifies if primary key mappings exist in KV
 * 3. If unmapped data is found, performs automatic integration
 * 4. Caches results to avoid repeated checks
 *
 * @public
 * @param d1 - The D1 database instance to check and potentially migrate
 * @param shardName - The shard binding name for this database
 * @param config - CollegeDB configuration containing KV and strategy
 * @param options - Optional migration configuration
 * @returns Promise resolving to migration result summary
 * @example
 * ```typescript
 * // Called automatically by CollegeDB operations
 * const result = await autoDetectAndMigrate(env.DB_EXISTING, 'db-existing', config);
 * if (result.migrationPerformed) {
 *   console.log(`Auto-migrated ${result.recordsMigrated} records`);
 * }
 * ```
 */
export async function autoDetectAndMigrate(
	d1: D1Database,
	shardName: string,
	config: CollegeDBConfig,
	options: {
		primaryKeyColumn?: string;
		tablesToCheck?: string[];
		skipCache?: boolean;
		maxRecordsToCheck?: number;
	} = {}
): Promise<{
	migrationNeeded: boolean;
	migrationPerformed: boolean;
	recordsMigrated: number;
	tablesProcessed: number;
	issues: string[];
}> {
	const { primaryKeyColumn = 'id', tablesToCheck, skipCache = false, maxRecordsToCheck = 1000 } = options;

	const cacheKey = `${shardName}_migration_check`;

	// Check cache to avoid repeated migration checks
	if (!skipCache && migrationStatusCache.has(cacheKey)) {
		return {
			migrationNeeded: false,
			migrationPerformed: false,
			recordsMigrated: 0,
			tablesProcessed: 0,
			issues: []
		};
	}

	const issues: string[] = [];
	let recordsMigrated = 0;
	let tablesProcessed = 0;
	let migrationNeeded = false;
	let migrationPerformed = false;

	try {
		const { KVShardMapper } = await import('./kvmap.js');
		const mapper = new KVShardMapper(config.kv);

		// Discover tables to check
		const allTables = await listTables(d1);
		const dataTableNames =
			tablesToCheck ||
			allTables.filter((table) => table !== 'shard_mappings' && !table.startsWith('sqlite_') && table !== 'sqlite_sequence');

		if (dataTableNames.length === 0) {
			// No data tables found, mark as migrated
			migrationStatusCache.set(cacheKey, true);
			return {
				migrationNeeded: false,
				migrationPerformed: false,
				recordsMigrated: 0,
				tablesProcessed: 0,
				issues: []
			};
		}

		// Check each table for unmapped data
		for (const tableName of dataTableNames) {
			try {
				// Quick validation
				const validation = await validateTableForSharding(d1, tableName, primaryKeyColumn);
				if (!validation.isValid || validation.recordCount === 0) {
					continue;
				}

				// Sample some primary keys to check if they're mapped
				const sampleSize = Math.min(maxRecordsToCheck, validation.recordCount);
				const sampleKeys = await (
					await d1.prepare(`
					SELECT ${primaryKeyColumn} FROM ${tableName}
					ORDER BY ${primaryKeyColumn}
					LIMIT ?
				`)
				)
					.bind(sampleSize)
					.all();

				let unmappedCount = 0;
				const keysToCheck = sampleKeys.results.slice(0, 10); // Check first 10 as sample

				for (const row of keysToCheck) {
					const primaryKey = String((row as any)[primaryKeyColumn]);
					const mapping = await mapper.getShardMapping(primaryKey);
					if (!mapping) {
						unmappedCount++;
						migrationNeeded = true;
					}
				}

				// If we found unmapped keys, migrate all keys in this table
				if (unmappedCount > 0) {
					console.log(`🔄 Auto-migrating table ${tableName} in shard ${shardName} (${validation.recordCount} records)`);

					const allPrimaryKeys = await discoverExistingPrimaryKeys(d1, tableName, primaryKeyColumn);

					// Create mappings for all unmapped keys
					let newMappings = 0;
					for (const primaryKey of allPrimaryKeys) {
						const existingMapping = await mapper.getShardMapping(primaryKey);
						if (!existingMapping) {
							await mapper.setShardMapping(primaryKey, shardName);
							newMappings++;
						}
					}

					recordsMigrated += newMappings;
					tablesProcessed++;
					migrationPerformed = true;

					console.log(`✅ Auto-migrated ${newMappings} records from table ${tableName}`);
				}
			} catch (error) {
				issues.push(`Auto-migration failed for table ${tableName}: ${error}`);
			}
		}

		// Add shard to known shards if migration was performed
		if (migrationPerformed) {
			await mapper.addKnownShard(shardName);

			// Add shard_mappings table if it doesn't exist
			const hasMappingsTable = allTables.includes('shard_mappings');
			if (!hasMappingsTable) {
				await (
					await d1.prepare(`
					CREATE TABLE IF NOT EXISTS shard_mappings (
						primary_key TEXT PRIMARY KEY,
						shard_name TEXT NOT NULL,
						created_at INTEGER NOT NULL,
						updated_at INTEGER NOT NULL
					);
				`)
				).run();
			}
		}

		// Cache the result to avoid repeated checks
		migrationStatusCache.set(cacheKey, true);

		if (migrationPerformed) {
			console.log(`🎉 Auto-migration completed for shard ${shardName}: ${recordsMigrated} records from ${tablesProcessed} tables`);
		}
	} catch (error) {
		issues.push(`Auto-migration error: ${error}`);
	}

	return {
		migrationNeeded,
		migrationPerformed,
		recordsMigrated,
		tablesProcessed,
		issues
	};
}

/**
 * Checks if a shard requires automatic migration
 *
 * Performs a lightweight check to determine if a database contains
 * existing data that hasn't been mapped to the sharding system.
 * This is used internally to trigger automatic migration.
 *
 * @public
 * @param d1 - The D1 database instance to check
 * @param shardName - The shard binding name
 * @param config - CollegeDB configuration
 * @returns Promise resolving to true if migration is needed
 * @example
 * ```typescript
 * const needsMigration = await checkMigrationNeeded(env.DB, 'db-main', config);
 * if (needsMigration) {
 *   console.log('Database contains unmapped data');
 * }
 * ```
 */
export async function checkMigrationNeeded(d1: D1Database, shardName: string, config: CollegeDBConfig): Promise<boolean> {
	const cacheKey = `${shardName}_migration_check`;

	// Check cache first
	if (migrationStatusCache.has(cacheKey)) {
		return false; // Already checked/migrated
	}

	try {
		const { KVShardMapper } = await import('./kvmap.js');
		const mapper = new KVShardMapper(config.kv);

		// Quick check: look for any table with data
		const tables = await listTables(d1);
		const dataTableNames = tables.filter(
			(table) => table !== 'shard_mappings' && !table.startsWith('sqlite_') && table !== 'sqlite_sequence'
		);

		for (const tableName of dataTableNames.slice(0, 3)) {
			// Check first 3 tables only
			try {
				// Check if table has records
				const countResult = await (await d1.prepare(`SELECT COUNT(*) as count FROM ${tableName} LIMIT 1`)).first();
				const recordCount = (countResult as any)?.count || 0;

				if (recordCount > 0) {
					// Sample one record to see if it's mapped
					const sampleRecord = await (await d1.prepare(`SELECT id FROM ${tableName} LIMIT 1`)).first();
					if (sampleRecord) {
						const primaryKey = String((sampleRecord as any).id);
						const mapping = await mapper.getShardMapping(primaryKey);
						if (!mapping) {
							return true; // Found unmapped data
						}
					}
				}
			} catch {
				// Skip tables that don't have 'id' column or have other issues
				continue;
			}
		}

		return false;
	} catch {
		return false; // Assume no migration needed if check fails
	}
}

/**
 * Clears the migration status cache
 *
 * Resets the internal cache used to track which databases have been
 * checked for migration. Useful for testing or forcing re-checks.
 *
 * @public
 * @example
 * ```typescript
 * // Force re-check of all databases
 * clearMigrationCache();
 * ```
 */
export function clearMigrationCache(): void {
	migrationStatusCache.clear();
}
