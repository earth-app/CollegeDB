/**
 * @fileoverview Database schema management and data migration utilities for CollegeDB
 *
 * This module provides utilities for managing database schemas across multiple shards
 * and migrating data between shards. It includes default schema definitions, schema
 * validation, and data migration functions that ensure consistency across the distributed
 * database system.
 *
 * Key features:
 * - Default schema creation for typical use cases
 * - Schema validation and existence checking
 * - Data migration between database instances
 * - Batch schema operations across multiple shards
 * - Table discovery and management utilities
 *
 * @example
 * ```typescript
 * import { createSchema, migrateRecord, schemaExists } from './migrations';
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
 * @author Gregory Mitchell
 * @since 1.0.0
 */

import { CollegeDBError } from './errors';
import type { KVShardMapper } from './kvmap';
import {
	HISTORY_INDEX,
	LEASE_TABLE,
	PROVEN_JOURNAL_DIALECTS,
	RETIRED_TABLE,
	buildCutoverStatements,
	buildFenceLock,
	buildInsertIgnore,
	buildJournalSchema,
	buildLeaseAcquire,
	buildLeaseFence,
	buildLeaseRelease,
	buildLeaseRenew,
	buildShardSidecarSchema,
	buildWriteLock,
	isProvenJournalDialect,
	quoteIdentifier,
	validateIdentifier,
	type BuiltQuery,
	type CutoverSpec,
	type IndexSpec,
	type LeaseFence,
	type TombstoneGuard
} from './query';
import type { CollegeDBConfig, QueryResult, SQLDatabase, SQLDialect, ShardingStrategy } from './types';

/**
 * Cache for migration status to avoid repeated checks
 * @private
 */
const migrationStatusCache = new Map<string, boolean>();

/**
 * Cache for table structure information to avoid repeated PRAGMA queries,
 * kept per provider so two databases that share a table name never share columns
 * @private
 */
const tableInfoCache = new WeakMap<SQLDatabase, Map<string, Array<{ name: string; type: string }>>>();

/**
 * Runs async work with a bounded concurrency limit.
 * @private
 */
async function mapWithConcurrency<T>(items: T[], concurrency: number, work: (item: T, index: number) => Promise<void>): Promise<void> {
	if (items.length === 0) {
		return;
	}

	const workerCount = Math.max(1, Math.min(concurrency, items.length));
	let index = 0;

	const workers = new Array(workerCount).fill(null).map(async () => {
		while (index < items.length) {
			const currentIndex = index++;
			const item = items[currentIndex];
			if (item === undefined) {
				continue;
			}

			await work(item, currentIndex);
		}
	});

	await Promise.all(workers);
}

/**
 * Selects a shard for a key using the requested strategy.
 * @private
 */
function selectShardForKey(
	primaryKey: string,
	index: number,
	shardBindings: string[],
	strategy: 'hash' | 'round-robin' | 'random'
): string {
	const totalShards = shardBindings.length;

	switch (strategy) {
		case 'hash': {
			let hash = 0;
			for (let i = 0; i < primaryKey.length; i++) {
				const char = primaryKey.charCodeAt(i);
				hash = (hash << 5) - hash + char;
				hash = hash & hash;
			}
			const hashIndex = Math.abs(hash) % totalShards;
			return shardBindings[hashIndex]!;
		}
		case 'random':
			return shardBindings[Math.floor(Math.random() * totalShards)]!;
		default:
			return shardBindings[index % totalShards]!;
	}
}

/**
 * Executes SQL statements to create the default table structure and indexes
 * in the specified database. Supports custom schemas and handles SQL
 * statement parsing with comment filtering.
 *
 * The function:
 * 1. Splits the schema into individual SQL statements
 * 2. Filters out comments and empty statements
 * 3. Executes each statement using prepared statements
 * 4. Provides detailed error reporting on failures
 *
 * @param db - The database instance to create schema in
 * @param schema - Schema SQL to use
 * @returns Promise that resolves when all schema statements are executed
 * @throws {Error} If any schema statement fails with detailed error information
 * @example
 * ```typescript
 * const sql = `
 *   CREATE TABLE products (
 *     id TEXT PRIMARY KEY,
 *     name TEXT NOT NULL,
 *     price REAL
 *   );
 * `;
 * await createSchema(env.DB_PRODUCTS, sql);
 * ```
 */
export async function createSchema(db: SQLDatabase, schema: string): Promise<void> {
	const statements = schema
		.split(';')
		.map((stmt) => stmt.trim())
		.filter((stmt) => stmt.length > 0 && !stmt.startsWith('--')); // Filter out comments

	for (const statement of statements) {
		try {
			await db.prepare(statement).run();
		} catch (error) {
			console.error('Failed to execute schema statement:', statement, error);
			throw new CollegeDBError(`Schema migration failed: ${error}`, 'SCHEMA_MIGRATION_FAILED');
		}
	}
}

/**
 * Applies the schema to all provided database instances in parallel.
 * This is useful for initializing a complete sharded database system
 * where all shards need the same table structure.
 *
 * The function executes schema creation on all shards concurrently for
 * performance, but provides detailed error reporting that identifies
 * which specific shard failed if any errors occur.
 *
 * A cluster that mixes backends cannot share one DDL string: `INTEGER PRIMARY
 * KEY AUTOINCREMENT` is a syntax error on Postgres, and `SERIAL` is one on
 * SQLite. Pass a function instead of a string to build the statement per shard
 * from its dialect.
 *
 * @param shards - Record mapping shard names to database instances
 * @param schema - Schema SQL, or a function returning it for a given shard
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
 *   await createSchemaAcrossShards(shards, userSchema);
 *   console.log('Schema created on all shards successfully');
 * } catch (error) {
 *   console.error('Schema creation failed:', error.message);
 *   // Error will specify which shard failed
 * }
 * ```
 * @example
 * ```typescript
 * // A cluster spanning D1 and Postgres needs the generated-key column spelled
 * // the way each backend spells it.
 * await createSchemaAcrossShards(shards, (dialect) =>
 * 	dialect === 'postgres' ? POSTGRES_SCHEMA : SQLITE_SCHEMA
 * );
 * ```
 */
export async function createSchemaAcrossShards(
	shards: Record<string, SQLDatabase>,
	schema: string | ((dialect: SQLDialect | undefined, binding: string) => string)
): Promise<void> {
	const promises = Object.entries(shards).map(([shardName, db]) => {
		const statement = typeof schema === 'function' ? schema(db.dialect, shardName) : schema;
		return createSchema(db, statement).catch((error) => {
			throw new CollegeDBError(`Failed to create schema on shard ${shardName}: ${error.message}`, 'SCHEMA_CREATION_FAILED');
		});
	});

	await Promise.all(promises);
}

/**
 * Performs a lightweight check to determine if the expected schema is present
 * in the database.
 *
 * @param db - The database instance to check
 * @param table - The name of the table to check
 * @returns Promise resolving to true if schema tables exist, false otherwise
 * @example
 * ```typescript
 * const hasSchema = await schemaExists(env.DB_NEW_SHARD, "users");
 * if (!hasSchema) {
 *     console.log('Creating schema on new shard...');
 *     await createSchema(env.DB_NEW_SHARD, usersSchema);
 * }
 * ```
 */
export async function schemaExists(db: SQLDatabase, table: string): Promise<boolean> {
	try {
		const result = await db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?").bind(table).first();
		return result !== null;
	} catch {
		return false;
	}
}

/**
 * Removes all tables that are part of the default CollegeDB schema from
 * the specified database. This is a destructive operation that cannot be undone.
 *
 * **DANGER**: This operation permanently deletes all data in the affected
 * tables. Only use during development, testing, or complete system resets.
 *
 * @param db - The database instance to drop tables from
 * @param tables - The table schemas to drop
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
export async function dropSchema(db: SQLDatabase, ...tables: string[]): Promise<void> {
	for (const table of tables) {
		try {
			await db.prepare(`DROP TABLE IF EXISTS ${table}`).run();
		} catch (error) {
			console.error(`Failed to drop table ${table}:`, error);
		}
	}
}

/**
 * Queries the SQLite system catalog to retrieve all user-created tables
 * in the database. This is useful for schema inspection, validation,
 * and debugging purposes.
 *
 * @param db - The database instance to inspect
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
export async function listTables(db: SQLDatabase): Promise<string[]> {
	try {
		const result = await db.prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").all();
		return result.results.map((row: any) => row.name as string);
	} catch {
		return [];
	}
}

/**
 * Moves a single record from a source database to a target database.
 * This is typically used during shard rebalancing operations when data needs
 * to be redistributed across shards for load balancing.
 *
 * The migration process:
 * 1. Retrieves the complete record from the source database
 * 2. Ensures the target database has the required schema
 * 3. Inserts the record into the target database
 * 4. Deletes the record from the source database
 *
 * A destination row that differs from the source stops the move with
 * `DESTINATION_CONFLICT` unless `onConflict` is `replace`. A destination row
 * equal to the source is a retried copy and the move continues.
 *
 * The operation is atomic from the perspective of each database, but not
 * across databases. If the operation fails partway through, manual cleanup
 * may be required. For a move that survives a crash, use `reassignShard` with a
 * `journalShard` configured.
 *
 * @param source - Source database containing the record
 * @param target - Target database to receive the record
 * @param primaryKey - Primary key of the record to migrate
 * @param tableName - Name of the table containing the record
 * @param options - Primary-key column (default `id`) and conflict policy (default `fail`)
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
 * await migrateRecord(source, target, 'post-456', 'posts');
 * ```
 */
export async function migrateRecord(
	source: SQLDatabase,
	target: SQLDatabase,
	primaryKey: string,
	tableName: string,
	options: MigrateRecordOptions = {}
): Promise<void> {
	// Validate the table name to avoid injection through callers that may pass
	// untrusted strings. Identifier rules match SQLite's unquoted identifier
	// grammar (alphanumeric + underscore, no leading digit).
	if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(tableName)) {
		throw new CollegeDBError(`Invalid table name for migration: ${tableName}`, 'INVALID_TABLE_NAME');
	}

	const pkColumn = options.pkColumn ?? 'id';
	const sourceRecord = await readRow(source, tableName, pkColumn, primaryKey);

	if (!sourceRecord) {
		throw new CollegeDBError(`Record with primary key ${primaryKey} not found in source database`, 'RECORD_NOT_FOUND');
	}

	// Previous versions called `createSchema(target, tableName)` here, passing
	// the bare table name as a SQL string. That branch silently failed because
	// `<tableName>` is not valid SQL, leaving target shards without the table
	// they were supposed to receive. Schema preparation now belongs to the
	// caller; if the table is missing on the target the INSERT below will
	// surface the real SQL error from the underlying engine.

	if (Object.keys(sourceRecord).length === 0) {
		throw new CollegeDBError(`Source record for ${primaryKey} has no columns`, 'EMPTY_SOURCE_RECORD');
	}

	const snapshot = encodeRow(sourceRecord);
	const existing = await readRow(target, tableName, pkColumn, primaryKey);

	if (existing && encodeRow(existing).payload !== snapshot.payload) {
		if (options.onConflict !== 'replace') {
			throw new CollegeDBError(
				`Destination already holds a different row for ${primaryKey} in ${tableName}; pass onConflict: 'replace' to overwrite it`,
				'DESTINATION_CONFLICT'
			);
		}
		await deleteRowByKey(target, tableName, pkColumn, snapshot);
	}

	if (!existing || encodeRow(existing).payload !== snapshot.payload) {
		await insertSnapshot(target, tableName, snapshot);
	}

	await deleteRowByKey(source, tableName, pkColumn, snapshot);
}

/**
 * Scans an existing table to find all primary keys that need to be mapped to shards.
 * This is useful when integrating CollegeDB with an existing database that already
 * contains data. The function assumes the table has an 'id' column as the primary key.
 *
 * @param db - The database instance to scan
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
export async function discoverExistingPrimaryKeys(db: SQLDatabase, tableName: string, primaryKeyColumn: string = 'id'): Promise<string[]> {
	try {
		const result = await db.prepare(`SELECT ${primaryKeyColumn} FROM ${tableName}`).all();
		return result.results.map((row: any) => String(row[primaryKeyColumn]));
	} catch (error) {
		throw new CollegeDBError(`Failed to discover primary keys in table ${tableName}: ${error}`, 'DISCOVERY_FAILED');
	}
}

/**
 * Discovers existing records with additional columns for multi-key mapping support.
 * Scans a table to find primary keys along with username, email, and name columns
 * when they exist, allowing these additional columns to be used as lookup keys.
 *
 * @param db - The database instance to scan
 * @param tableName - Name of the table to discover records from
 * @param primaryKeyColumn - Name of the primary key column (defaults to 'id')
 * @returns Promise resolving to array of record data with available columns
 * @throws {Error} If table doesn't exist or database query fails
 * @example
 * ```typescript
 * // Discover all user records with available lookup columns
 * const records = await discoverExistingRecordsWithColumns(env.DB_EXISTING, 'users');
 * console.log(`Found ${records.length} user records`);
 * records.forEach(record => {
 *   console.log(`ID: ${record.id}, Email: ${record.email || 'N/A'}`);
 * });
 * ```
 * @since 1.0.4
 */
export async function discoverExistingRecordsWithColumns(
	db: SQLDatabase,
	tableName: string,
	primaryKeyColumn: string = 'id'
): Promise<Array<{ [key: string]: any }>> {
	try {
		// Check cache first to avoid repeated PRAGMA queries
		const cacheKey = `${tableName}_columns`;
		let availableColumns: string[];
		let known = tableInfoCache.get(db);
		if (!known) {
			known = new Map();
			tableInfoCache.set(db, known);
		}

		if (known.has(cacheKey)) {
			availableColumns = known.get(cacheKey)!.map((col) => col.name);
		} else {
			// First, discover what columns exist in the table
			const columnInfo = await db.prepare(`PRAGMA table_info(${tableName})`).all();
			const columnData = (columnInfo.results as any[]).map((col) => ({ name: col.name as string, type: col.type as string }));

			// Cache the result
			known.set(cacheKey, columnData);
			availableColumns = columnData.map((col) => col.name);
		}

		// Build SELECT statement with available columns
		const columnsToSelect = [primaryKeyColumn];

		// Add optional columns if they exist
		if (availableColumns.includes('username')) {
			columnsToSelect.push('username');
		}
		if (availableColumns.includes('email')) {
			columnsToSelect.push('email');
		}
		if (availableColumns.includes('name')) {
			columnsToSelect.push('name');
		}

		const selectQuery = `SELECT ${columnsToSelect.join(', ')} FROM ${tableName}`;
		const result = await db.prepare(selectQuery).all();

		return result.results as Array<{ [key: string]: any }>;
	} catch (error) {
		throw new CollegeDBError(`Failed to discover records with columns in table ${tableName}: ${error}`, 'DISCOVERY_FAILED');
	}
}

/**
 * Takes a list of existing primary keys and creates shard mappings for them using
 * the specified allocation strategy. This allows existing data to be integrated
 * into the CollegeDB sharding system without data migration.
 *
 * @param primaryKeys - Array of primary key values to create mappings for
 * @param shardBindings - Array of available shard binding names
 * @param strategy - Allocation strategy to use ('hash', 'round-robin', or 'random')
 * @param mapper - KVShardMapper instance for storing mappings
 * @returns Promise that resolves when all mappings are created
 * @throws {Error} If mapping creation fails
 * @example
 * ```typescript
 * import { KVShardMapper } from './kvmap';
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
	mapper: KVShardMapper,
	options: { concurrency?: number } = {}
): Promise<void> {
	if (primaryKeys.length === 0 || shardBindings.length === 0) {
		return;
	}

	const concurrency = Math.max(1, options.concurrency ?? 25);
	const payload = primaryKeys.map((primaryKey, index) => ({
		primaryKey,
		shard: selectShardForKey(primaryKey, index, shardBindings, strategy)
	}));

	await mapper.setShardMappingsBatch(payload, { concurrency });
}

/**
 * Represents the result of validating a table for sharding.
 * Contains information about the table structure, primary key, record count,
 * and any issues encountered during validation.
 */
export type ValidationResult = {
	isValid: boolean;
	tableName: string;
	primaryKeyColumn: string;
	recordCount: number;
	issues: string[];
};

/**
 * Checks if a table exists and has a primary key column that can be used
 * for sharding. Returns information about the table structure and primary key.
 *
 * @param db - The database instance to check
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
export async function validateTableForSharding(db: SQLDatabase, tableName: string, primaryKeyColumn: string): Promise<ValidationResult> {
	const issues: string[] = [];
	let recordCount = 0;

	try {
		// Check if table exists
		const tableCheck = await db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`).bind(tableName).first();

		if (!tableCheck) {
			issues.push(`Table '${tableName}' does not exist`);
			return {
				isValid: false,
				tableName,
				primaryKeyColumn,
				recordCount: 0,
				issues
			} satisfies ValidationResult;
		}

		// Check if primary key column exists
		const columnCheck = await db.prepare(`PRAGMA table_info(${tableName})`).all();
		const hasIdColumn = columnCheck.results.some((col: any) => col.name === primaryKeyColumn && col.pk === 1);

		if (!hasIdColumn) {
			issues.push(`Primary key column '${primaryKeyColumn}' not found or not set as primary key`);
		}

		// Get record count
		const countResult = await db.prepare(`SELECT COUNT(*) as count FROM ${tableName}`).first();
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
	} satisfies ValidationResult;
}

/**
 * Configuration options for integrating an existing database with CollegeDB.
 * Allows customization of which tables to process, primary key column,
 * sharding strategy, and whether to add the shard_mappings table.
 */
export type IntegrationOptions = {
	tables?: string[];
	primaryKeyColumn?: string;
	/** @deprecated Ignored and has no effect. Will be removed in 2.0. */
	strategy?: ShardingStrategy;
	addShardMappingsTable?: boolean;
	dryRun?: boolean;
	migrateOtherColumns?: boolean;
	concurrency?: number;
};

/**
 * Represents the result of integrating an existing database with CollegeDB.
 * Contains information about the success of the integration, shard name,
 * number of tables processed, total records integrated, mappings created,
 * and any issues encountered during the process.
 */
export type IntegrationResult = {
	success: boolean;
	shardName: string;
	tablesProcessed: number;
	totalRecords: number;
	mappingsCreated: number;
	issues: string[];
};

/**
 * Performs a complete drop-in integration of an existing database.
 * This is the main function for integrating CollegeDB with an existing database
 * that already contains data. It discovers tables, validates them, creates shard
 * mappings, and optionally adds the shard_mappings table if needed.
 *
 * When `migrateOtherColumns` is enabled, the function will also create additional
 * lookup keys for username, email, and name columns if they exist in the table.
 * This allows these fields to be used as lookup keys in addition to the primary key.
 *
 * @param db - The existing database to integrate
 * @param shardName - The shard binding name for this database
 * @param mapper - KVShardMapper instance for storing mappings
 * @param options - Configuration options for the integration
 * @param options.migrateOtherColumns - When true, creates additional lookup keys for username, email, and name columns
 * @returns Promise resolving to integration summary
 * @throws {Error} If integration fails
 * @example
 * ```typescript
 * import { KVShardMapper } from './kvmap';
 *
 * const mapper = new KVShardMapper(env.KV);
 * const result = await integrateExistingDatabase(env.DB_EXISTING, 'db-existing', mapper, {
 *   tables: ['users', 'posts'],
 *   strategy: 'hash',
 *   addShardMappingsTable: true,
 *   migrateOtherColumns: true  // Creates additional lookup keys
 * });
 *
 * console.log(`Integrated ${result.totalRecords} records from ${result.tablesProcessed} tables`);
 * // Now users can be looked up by username:john, email:john@example.com, or name:John Doe
 * ```
 */
export async function integrateExistingDatabase(
	db: SQLDatabase,
	shardName: string,
	mapper: KVShardMapper,
	options: IntegrationOptions = {}
): Promise<IntegrationResult> {
	const {
		tables,
		primaryKeyColumn = 'id',
		addShardMappingsTable = true,
		dryRun = false,
		migrateOtherColumns = false,
		concurrency = 25
	} = options;

	const normalizedConcurrency = Math.max(1, concurrency);

	const issues: string[] = [];
	let tablesProcessed = 0;
	let totalRecords = 0;
	let mappingsCreated = 0;

	try {
		// Discover tables if not specified
		const tablesToProcess = tables || (await listTables(db));

		// Filter out the shard_mappings table if it already exists
		const dataTableNames = tablesToProcess.filter((table) => table !== 'shard_mappings' && !table.startsWith('collegedb_'));

		for (const tableName of dataTableNames) {
			try {
				// Validate table
				const validation = await validateTableForSharding(db, tableName, primaryKeyColumn);

				if (!validation.isValid) {
					issues.push(`Table ${tableName}: ${validation.issues.join(', ')}`);
					continue;
				}

				if (migrateOtherColumns) {
					// Use the new function to get records with additional columns
					const records = await discoverExistingRecordsWithColumns(db, tableName, primaryKeyColumn);
					if (records.length === 0) {
						issues.push(`Table ${tableName} has no records to process`);
						continue;
					}

					if (!dryRun) {
						const payload = records.map((record) => {
							const primaryKey = String(record[primaryKeyColumn]);
							const additionalKeys: string[] = [];

							if (record.username && typeof record.username === 'string') {
								additionalKeys.push(`username:${record.username}`);
							}
							if (record.email && typeof record.email === 'string') {
								additionalKeys.push(`email:${record.email}`);
							}
							if (record.name && typeof record.name === 'string') {
								additionalKeys.push(`name:${record.name}`);
							}

							return {
								primaryKey,
								shard: shardName,
								additionalKeys
							};
						});

						await mapper.setShardMappingsBatch(payload, { concurrency: normalizedConcurrency });
						mappingsCreated += payload.length;
					}

					totalRecords += records.length;
				} else {
					// Original behavior: only use primary keys
					const primaryKeys = await discoverExistingPrimaryKeys(db, tableName, primaryKeyColumn);
					if (primaryKeys.length === 0) {
						issues.push(`Table ${tableName} has no records to process`);
						continue;
					}

					if (!dryRun) {
						const payload = primaryKeys.map((primaryKey) => ({
							primaryKey,
							shard: shardName
						}));
						await mapper.setShardMappingsBatch(payload, { concurrency: normalizedConcurrency });
						mappingsCreated += payload.length;
					}

					totalRecords += primaryKeys.length;
				}

				tablesProcessed++;
			} catch (error) {
				issues.push(`Failed to process table ${tableName}: ${error}`);
			}
		}

		if (addShardMappingsTable && !dryRun) {
			const hasMappingsTable = (await listTables(db)).includes('shard_mappings');
			if (!hasMappingsTable) {
				await db
					.prepare(
						`
					CREATE TABLE IF NOT EXISTS shard_mappings (
						primary_key TEXT PRIMARY KEY,
						shard_name TEXT NOT NULL,
						created_at INTEGER NOT NULL,
						updated_at INTEGER NOT NULL
					);`.trim()
					)
					.run();
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
 * sharding system. It performs migration without user intervention.
 *
 * The detection process:
 * 1. Checks if the database has data tables with primary keys
 * 2. Verifies if primary key mappings exist in KV
 * 3. If unmapped data is found, performs automatic integration
 * 4. Caches results to avoid repeated checks
 *
 * When `migrateOtherColumns` is enabled, additional lookup keys will be created
 * for username, email, and name columns if they exist in the tables.
 *
 * @param db - The database instance to check and potentially migrate
 * @param shardName - The shard binding name for this database
 * @param config - CollegeDB configuration containing KV and strategy
 * @param options - Optional migration configuration
 * @param options.migrateOtherColumns - When true, creates additional lookup keys for username, email, and name columns
 * @param options.statusCache - Where checked shards are remembered; the module-level cache when omitted
 * @returns Promise resolving to migration result summary
 * @example
 * ```typescript
 * // Called automatically by CollegeDB operations
 * const result = await autoDetectAndMigrate(env.DB_EXISTING, 'db-existing', config, {
 *   migrateOtherColumns: true
 * });
 * if (result.migrationPerformed) {
 *   console.log(`Auto-migrated ${result.recordsMigrated} records`);
 * }
 * ```
 */
export async function autoDetectAndMigrate(
	db: SQLDatabase,
	shardName: string,
	config: CollegeDBConfig,
	options: {
		primaryKeyColumn?: string;
		tablesToCheck?: string[];
		skipCache?: boolean;
		maxRecordsToCheck?: number;
		migrateOtherColumns?: boolean;
		concurrency?: number;
		statusCache?: Map<string, boolean>;
	} = {}
): Promise<{
	migrationNeeded: boolean;
	migrationPerformed: boolean;
	recordsMigrated: number;
	tablesProcessed: number;
	issues: string[];
}> {
	const {
		primaryKeyColumn = 'id',
		tablesToCheck,
		skipCache = false,
		maxRecordsToCheck = 1000,
		migrateOtherColumns = false,
		concurrency = config.migrationConcurrency ?? 25,
		statusCache = migrationStatusCache
	} = options;

	const normalizedConcurrency = Math.max(1, concurrency);

	const cacheKey = `${shardName}_migration_check`;

	// Check cache to avoid repeated migration checks
	if (!skipCache && statusCache.has(cacheKey)) {
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
		const { KVShardMapper } = await import('./kvmap');
		const mapper = new KVShardMapper(config.kv, {
			hashShardMappings: config.hashShardMappings,
			mappingCacheTtlMs: config.mappingCacheTtlMs,
			knownShardsCacheTtlMs: config.knownShardsCacheTtlMs
		});

		// Discover tables to check
		const allTables = await listTables(db);
		const dataTableNames =
			tablesToCheck ||
			allTables.filter(
				(table) =>
					table !== 'shard_mappings' && !table.startsWith('sqlite_') && table !== 'sqlite_sequence' && !table.startsWith('collegedb_')
			);

		if (dataTableNames.length === 0) {
			// No data tables found, mark as migrated
			statusCache.set(cacheKey, true);
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
				const validation = await validateTableForSharding(db, tableName, primaryKeyColumn);
				if (!validation.isValid || validation.recordCount === 0) {
					continue;
				}

				// Sample some primary keys to check if they're mapped
				const sampleSize = Math.min(maxRecordsToCheck, validation.recordCount);
				const sampleKeys = await db
					.prepare(
						`
					SELECT ${primaryKeyColumn} FROM ${tableName}
					ORDER BY ${primaryKeyColumn}
					LIMIT ?`.trim()
					)
					.bind(sampleSize)
					.all();

				let unmappedCount = 0;
				const keysToCheck = sampleKeys.results.slice(0, 10); // Check first 10 as sample

				// OPTIMIZATION: Batch check multiple keys instead of one-by-one
				const keyCheckPromises = keysToCheck.map(async (row) => {
					const primaryKey = String((row as any)[primaryKeyColumn]);
					return {
						key: primaryKey,
						mapping: await mapper.getShardMapping(primaryKey)
					};
				});

				const keyResults = await Promise.all(keyCheckPromises);

				for (const result of keyResults) {
					if (!result.mapping) {
						unmappedCount++;
						migrationNeeded = true;
					}
				}

				if (unmappedCount > 0) {
					if (config.debug) console.log(`Auto-migrating table ${tableName} in shard ${shardName} (${validation.recordCount} records)`);

					if (migrateOtherColumns) {
						// Use multi-column discovery for migration with additional lookup keys
						const allRecords = await discoverExistingRecordsWithColumns(db, tableName, primaryKeyColumn);

						const payload: Array<{ primaryKey: string; shard: string; additionalKeys: string[] }> = [];
						await mapWithConcurrency(allRecords, normalizedConcurrency, async (record) => {
							const primaryKey = String(record[primaryKeyColumn]);
							const existingMapping = await mapper.getShardMapping(primaryKey);
							if (existingMapping) {
								return;
							}

							const additionalKeys: string[] = [];
							if (record.username && typeof record.username === 'string') {
								additionalKeys.push(`username:${record.username}`);
							}
							if (record.email && typeof record.email === 'string') {
								additionalKeys.push(`email:${record.email}`);
							}
							if (record.name && typeof record.name === 'string') {
								additionalKeys.push(`name:${record.name}`);
							}

							payload.push({
								primaryKey,
								shard: shardName,
								additionalKeys
							});
						});

						if (payload.length > 0) {
							await mapper.setShardMappingsBatch(payload, { concurrency: normalizedConcurrency });
						}

						recordsMigrated += payload.length;
					} else {
						// Original behavior: only use primary keys
						const allPrimaryKeys = await discoverExistingPrimaryKeys(db, tableName, primaryKeyColumn);

						const payload: Array<{ primaryKey: string; shard: string }> = [];
						await mapWithConcurrency(allPrimaryKeys, normalizedConcurrency, async (primaryKey) => {
							const existingMapping = await mapper.getShardMapping(primaryKey);
							if (!existingMapping) {
								payload.push({ primaryKey, shard: shardName });
							}
						});

						if (payload.length > 0) {
							await mapper.setShardMappingsBatch(payload, { concurrency: normalizedConcurrency });
						}

						recordsMigrated += payload.length;
					}

					tablesProcessed++;
					migrationPerformed = true;

					if (config.debug) console.log(`Auto-migrated ${recordsMigrated} records from table ${tableName}`);
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
				await db
					.prepare(
						`CREATE TABLE IF NOT EXISTS shard_mappings (
						primary_key TEXT PRIMARY KEY,
						shard_name TEXT NOT NULL,
						created_at INTEGER NOT NULL,
						updated_at INTEGER NOT NULL
					);
				`
					)
					.run();
			}
		}

		// Cache the result to avoid repeated checks
		statusCache.set(cacheKey, true);

		if (migrationPerformed && config.debug) {
			console.log(`Auto-migration completed for shard ${shardName}: ${recordsMigrated} records from ${tablesProcessed} tables`);
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
 * Performs a lightweight check to determine if a database contains
 * existing data that hasn't been mapped to the sharding system.
 * This is used internally to trigger automatic migration.
 *
 * @param db - The database instance to check
 * @param shardName - The shard binding name
 * @param config - CollegeDB configuration
 * @param statusCache - Where checked shards are remembered; the module-level cache when omitted
 * @returns Promise resolving to true if migration is needed
 * @example
 * ```typescript
 * const needsMigration = await checkMigrationNeeded(env.DB, 'db-main', config);
 * if (needsMigration) {
 *   console.log('Database contains unmapped data');
 * }
 * ```
 */
export async function checkMigrationNeeded(
	db: SQLDatabase,
	shardName: string,
	config: CollegeDBConfig,
	statusCache: Map<string, boolean> = migrationStatusCache
): Promise<boolean> {
	const cacheKey = `${shardName}_migration_check`;

	// Check cache first (but not during tests with skip cache)
	if (statusCache.has(cacheKey)) {
		return false; // Already checked/migrated
	}

	try {
		// Check if shard_mappings table exists as indicator of previous migration
		const tables = await listTables(db);
		const hasShardMappingsTable = tables.includes('shard_mappings');

		if (hasShardMappingsTable) {
			// If shard_mappings table exists, this database has been processed before
			statusCache.set(cacheKey, true);
			return false;
		}

		const { KVShardMapper } = await import('./kvmap');
		const mapper = new KVShardMapper(config.kv, {
			hashShardMappings: config.hashShardMappings,
			mappingCacheTtlMs: config.mappingCacheTtlMs,
			knownShardsCacheTtlMs: config.knownShardsCacheTtlMs
		});

		// Quick check: look for any table with data
		const dataTableNames = tables.filter(
			(table) =>
				table !== 'shard_mappings' && !table.startsWith('sqlite_') && table !== 'sqlite_sequence' && !table.startsWith('collegedb_')
		);

		for (const tableName of dataTableNames.slice(0, 3)) {
			// Check first 3 tables only
			try {
				// Check if table has records
				const countResult = await db.prepare(`SELECT COUNT(*) as count FROM ${tableName} LIMIT 1`).first();
				const recordCount = (countResult as any)?.count || 0;

				if (recordCount > 0) {
					// Sample one record to see if it's mapped
					const sampleRecord = await db.prepare(`SELECT id FROM ${tableName} LIMIT 1`).first();
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
 * @example
 * ```typescript
 * // Force re-check of all databases
 * clearMigrationCache();
 * ```
 */
export function clearMigrationCache(): void {
	migrationStatusCache.clear();
}

/**
 * Clears a specific migration cache entry
 *
 * Resets the cache for a specific shard, forcing re-check on next
 * migration detection call.
 *
 * @param shardName - The shard name to clear from cache
 * @example
 * ```typescript
 * // Force re-check of specific shard
 * clearShardMigrationCache('db-auto');
 * ```
 */
export function clearShardMigrationCache(shardName: string): void {
	const cacheKey = `${shardName}_migration_check`;
	migrationStatusCache.delete(cacheKey);
}

// #region typed row encoding

/**
 * One column value in the archive payload, tagged with its type so a blob, a
 * 64-bit integer, NULL and an empty string each restore as what they were.
 * @since 1.4.0
 */
export type TypedValue =
	| { t: 'null' }
	| { t: 'int'; v: number }
	| { t: 'bigint'; v: string }
	| { t: 'real'; v: number | string }
	| { t: 'text'; v: string }
	| { t: 'blob'; v: string }
	| { t: 'bool'; v: boolean };

/** A row as typed columns sorted by name, and the exact text that is hashed. @internal */
export interface EncodedRow {
	columns: Array<[string, TypedValue]>;
	payload: string;
}

function toBase64(bytes: Uint8Array): string {
	let binary = '';
	for (let i = 0; i < bytes.length; i += 0x8000) {
		binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
	}
	return btoa(binary);
}

function fromBase64(encoded: string): Uint8Array {
	const binary = atob(encoded);
	const bytes = new Uint8Array(binary.length);
	for (let i = 0; i < binary.length; i++) {
		bytes[i] = binary.charCodeAt(i);
	}
	return bytes;
}

/**
 * Encodes one driver value. Integers inside the safe range are `int` whether the
 * driver returned a number or a bigint, so two drivers agree on the same row.
 * @throws {CollegeDBError} For values with no portable encoding, such as objects
 * @private
 */
function encodeValue(value: unknown): TypedValue {
	if (value === null || value === undefined) {
		return { t: 'null' };
	}
	if (typeof value === 'boolean') {
		return { t: 'bool', v: value };
	}
	if (typeof value === 'bigint') {
		return value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)
			? { t: 'int', v: Number(value) }
			: { t: 'bigint', v: value.toString() };
	}
	if (typeof value === 'number') {
		if (Number.isSafeInteger(value) && !Object.is(value, -0)) {
			return { t: 'int', v: value };
		}
		return { t: 'real', v: Number.isFinite(value) && !Object.is(value, -0) ? value : String(value) };
	}
	if (typeof value === 'string') {
		return { t: 'text', v: value };
	}
	if (value instanceof Uint8Array) {
		return { t: 'blob', v: toBase64(value) };
	}
	if (value instanceof ArrayBuffer) {
		return { t: 'blob', v: toBase64(new Uint8Array(value)) };
	}

	throw new CollegeDBError(
		`Column value of type ${Object.prototype.toString.call(value)} cannot be archived`,
		'MIGRATION_UNSUPPORTED_VALUE'
	);
}

/**
 * Turns a typed value back into something a driver can bind.
 * @private
 */
function decodeValue(typed: TypedValue, dialect?: SQLDialect): unknown {
	switch (typed.t) {
		case 'null':
			return null;
		case 'bigint':
			return BigInt(typed.v);
		case 'real':
			return typeof typed.v === 'string' ? Number(typed.v) : typed.v;
		case 'blob':
			return fromBase64(typed.v);
		case 'bool':
			return dialect === undefined || dialect === 'sqlite' ? (typed.v ? 1 : 0) : typed.v;
		default:
			return typed.v;
	}
}

/**
 * Encodes a row into sorted typed columns and the canonical text that the
 * digest and the archive both use.
 * @internal
 */
export function encodeRow(row: Record<string, unknown>): EncodedRow {
	const columns = Object.keys(row)
		.sort()
		.map((name): [string, TypedValue] => [name, encodeValue(row[name])]);
	return { columns, payload: JSON.stringify({ v: 1, columns }) };
}

/** SHA-256 of a string as lowercase hex. @internal */
export async function sha256Hex(text: string): Promise<string> {
	const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
	return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, '0')).join('');
}

/** The decoded columns of a snapshot, ready to bind. @internal */
export function bindable(encoded: EncodedRow, dialect?: SQLDialect): Array<[string, unknown]> {
	return encoded.columns.map(([name, typed]) => [name, decodeValue(typed, dialect)]);
}

async function readRow(db: SQLDatabase, table: string, pkColumn: string, key: string): Promise<Record<string, unknown> | null> {
	const row = await db
		.prepare(`SELECT * FROM ${quoteIdentifier(table, db.dialect)} WHERE ${quoteIdentifier(pkColumn, db.dialect)} = ?`)
		.bind(key)
		.first<Record<string, unknown>>();
	return row ?? null;
}

function snapshotInsert(db: SQLDatabase, table: string, encoded: EncodedRow): BuiltQuery {
	const dialect = db.dialect;
	const columns = bindable(encoded, dialect);
	return {
		sql:
			`INSERT INTO ${quoteIdentifier(table, dialect)} (${columns.map(([name]) => quoteIdentifier(name, dialect)).join(', ')}) ` +
			`VALUES (${columns.map(() => '?').join(', ')})`,
		bindings: columns.map(([, value]) => value)
	};
}

async function insertSnapshot(db: SQLDatabase, table: string, encoded: EncodedRow): Promise<void> {
	await runOn(db, snapshotInsert(db, table, encoded));
}

async function deleteRowByKey(db: SQLDatabase, table: string, pkColumn: string, encoded: EncodedRow): Promise<void> {
	const pk = bindable(encoded, db.dialect).find(([name]) => name === pkColumn);
	if (!pk) {
		throw new CollegeDBError(`Row has no primary key column ${pkColumn}`, 'MIGRATION_PK_MISSING');
	}
	await db
		.prepare(`DELETE FROM ${quoteIdentifier(table, db.dialect)} WHERE ${quoteIdentifier(pkColumn, db.dialect)} = ?`)
		.bind(pk[1])
		.run();
}

// #endregion

// #region journal engine

/**
 * Options for {@link migrateRecord}.
 * @since 1.4.0
 */
export interface MigrateRecordOptions {
	/** Primary-key column of the table (default `id`) */
	pkColumn?: string;
	/**
	 * What to do when the destination already holds a row for the key. `fail`
	 * (the default) refuses unless that row is byte-equal to the source, which
	 * is a retried copy and proceeds. `replace` overwrites it.
	 */
	onConflict?: 'fail' | 'replace';
}

/**
 * How long the archived source row is kept before {@link purgeRetired} may
 * clear it. A number is milliseconds.
 * @since 1.4.0
 */
export type RetirePolicy = 'immediate' | { afterMs: number };

/**
 * Options for a journaled move.
 * @since 1.4.0
 */
export interface MoveRecordOptions extends MigrateRecordOptions {
	/**
	 * Column that changes on every write to the row. When set the cutover compares
	 * only the key and this column, which is required for rows with more columns
	 * than the bind-parameter limit allows.
	 */
	versionColumn?: string;
	/** Retention for the archived source row (default 7 days) */
	retire?: RetirePolicy;
	/** Bind-parameter limit of the smallest shard involved (default 100, the D1 limit) */
	maxBindParameters?: number;
}

/**
 * States of a journaled move, in the order a successful move visits them.
 * @since 1.4.0
 */
export type MigrationState = 'prepared' | 'copied' | 'verified' | 'cutover_pending' | 'committed' | 'published' | 'retired' | 'aborted';

/** What the engine needs from the router. @since 1.4.0 */
export interface MigrationEnv {
	/** Journal partition */
	scope: string;
	/** Binding name of the journal shard */
	journalBinding: string;
	/** Shard bindings by name; must include the journal shard */
	shards: Record<string, SQLDatabase>;
	/** Updates derived state (KV mapping, placement exception, caches) once the move is committed */
	publish: (info: PublishInfo) => Promise<void>;
	/** Topology epoch recorded in the history, when placement is computed */
	placementEpoch?: () => number | null;
	/** Clock, for tests */
	now?: () => number;
	/**
	 * The recovery lease generation this executor holds. Set by
	 * {@link recoverMigrations} on its own copy of the environment; a live move
	 * leaves it unset and writes unfenced.
	 * @internal
	 */
	fence?: LeaseFence;
}

/** A committed move, as handed to {@link MigrationEnv.publish}. @since 1.4.0 */
export interface PublishInfo {
	opId: string;
	table: string;
	key: string;
	source: string;
	destination: string;
	generation: number;
}

/** Result of one move. @since 1.4.0 */
export interface MoveResult {
	opId: string;
	/** `committed` once the destination owns the key and derived state is published */
	outcome: 'committed' | 'aborted';
	/** Why a move aborted */
	reason?: string;
	generation: number;
	source: string;
	destination: string;
}

/** Journal view of the latest operation on a key. @since 1.4.0 */
export interface MigrationStatus {
	opId: string;
	table: string;
	state: MigrationState;
	generation: number;
	source: string;
	destination: string;
	/** True while the key is claimed by an unfinished operation */
	active: boolean;
	updatedAt: number;
}

/** Outcome of {@link recoverMigrations}. @since 1.4.0 */
export interface RecoveryReport {
	/** Active claims examined */
	examined: number;
	/** Operations that reached `committed` or beyond */
	committed: Array<{ opId: string; key: string; destination: string }>;
	/** Operations that ended `aborted`, with the reason */
	aborted: Array<{ opId: string; key: string; reason: string }>;
	/** Claims with no journal history, released */
	released: string[];
	/** Operations left alone because they changed more recently than `olderThanMs`, or a claim too young to release */
	skipped: string[];
	/** Operations left as they were, with the error */
	failed: Array<{ opId: string; key: string; error: string; code?: string }>;
}

/** Outcome of {@link verifyMigrations}. @since 1.4.0 */
export interface VerifyReport {
	/** Operations still active in the journal */
	danglingOps: Array<{ opId: string; key: string; state: MigrationState }>;
	/** Live rows on a shard that also holds a tombstone for the key */
	ghosts: Array<{ shard: string; table: string; key: string; opId: string; equalsArchive: boolean }>;
	/** Tombstones whose operation the journal has no record of */
	strandedArchives: Array<{ shard: string; table: string; key: string; opId: string }>;
	/**
	 * Live rows on the destination of an aborted operation that equal a row the
	 * operation journaled, left by an executor that stalled past its lease
	 */
	strandedCopies: Array<{ shard: string; table: string; key: string; opId: string }>;
	/**
	 * Tombstones whose operation the journal ended as aborted, left by an executor
	 * that stalled past its lease. `restorable` is true when the archive still
	 * holds the row and nothing else owns the key
	 */
	staleCutovers: Array<{ shard: string; table: string; key: string; opId: string; restorable: boolean }>;
	/** Ghosts, stranded copies and stale cutovers repaired, and operations resumed, when `repair` was set */
	repaired: string[];
	/** True when nothing remains to report */
	clean: boolean;
}

/** Outcome of {@link purgeRetired}. @since 1.4.0 */
export interface PurgeReport {
	purged: Array<{ opId: string; key: string }>;
	refused: Array<{ opId: string; key: string; reason: 'RETENTION_NOT_ELAPSED' | 'NOT_VERIFIED' }>;
}

const DEFAULT_RETIRE_AFTER_MS = 7 * 24 * 60 * 60 * 1000;
const DEFAULT_MAX_BIND_PARAMETERS = 100;
const MAX_RECOPY = 3;

/** How long a claim with no journal history is left alone before recovery releases it. */
const ORPHAN_CLAIM_GRACE_MS = 60_000;

/** How long a recovery lease lasts without a renewal. Takeover after expiry bumps the generation. */
const RECOVERY_LEASE_MS = 60_000;

const HISTORY = 'collegedb_migration_history';
const ACTIVE = 'collegedb_migration_active';

const STATE_RANK: Record<MigrationState, number> = {
	prepared: 1,
	copied: 2,
	verified: 3,
	cutover_pending: 4,
	committed: 5,
	published: 6,
	retired: 7,
	aborted: 8
};

/** Called between protocol steps. Tests use it to inject writers. @internal */
export type MigrationStepHook = (step: string, info: { opId: string; table: string; key: string }) => Promise<void> | void;

let stepHook: MigrationStepHook | null = null;

/**
 * Installs a hook that runs after each protocol step. Pass `null` to remove it.
 * The hook is process-wide: it runs for the default instance and every client.
 * @internal
 */
export function setMigrationStepHook(hook: MigrationStepHook | null): void {
	stepHook = hook;
}

interface Header {
	opId: string;
	scope: string;
	table: string;
	key: string;
	source: string;
	destination: string;
	generation: number;
	options: Required<Pick<MoveRecordOptions, 'pkColumn' | 'onConflict' | 'maxBindParameters'>> &
		Pick<MoveRecordOptions, 'versionColumn'> & { retire: RetirePolicy };
}

interface OpRecord extends Header {
	state: MigrationState;
	attempt: number;
	digest: string | null;
	digests: Set<string>;
	retireAfter: number | null;
	retiredAt: number | null;
	updatedAt: number;
}

const tableMemo = new WeakMap<SQLDatabase, Promise<void>>();
const journalMemo = new WeakMap<SQLDatabase, Promise<void>>();

async function ensureOnce(
	memo: WeakMap<SQLDatabase, Promise<void>>,
	db: SQLDatabase,
	statements: string[],
	indexes: IndexSpec[] = []
): Promise<void> {
	let pending = memo.get(db);
	if (!pending) {
		pending = (async () => {
			for (const statement of statements) {
				await createCatalogStatement(db, statement);
			}
			for (const index of indexes) {
				await ensureIndex(db, index);
			}
		})();
		memo.set(db, pending);
		// a failed attempt is not remembered, so the next call retries
		pending.catch(() => memo.delete(db));
	}
	await pending;
}

/** Longest cause chain the walks below follow. @private */
const MAX_CAUSE_DEPTH = 8;

/**
 * The error, then the errors it wraps, outermost first. Stops at a cycle or
 * after {@link MAX_CAUSE_DEPTH} links.
 * @private
 */
export function errorChain(error: unknown): unknown[] {
	const chain: unknown[] = [];
	let link: unknown = error;
	while (link !== undefined && link !== null && chain.length < MAX_CAUSE_DEPTH && !chain.includes(link)) {
		chain.push(link);
		link = (link as { cause?: unknown }).cause;
	}
	return chain;
}

/** Postgres catalog constraints that two sessions creating one name at once collide on. @private */
const POSTGRES_CATALOG_CONSTRAINTS: ReadonlySet<unknown> = new Set(['pg_class_relname_nsp_index', 'pg_type_typname_nsp_index']);

/** A table or index that a `CREATE ... IF NOT EXISTS` statement names. @private */
interface CatalogObject {
	kind: 'table' | 'index';
	name: string;
}

/** The object a `CREATE TABLE IF NOT EXISTS` or `CREATE INDEX IF NOT EXISTS` statement names. @private */
function catalogObjectOf(statement: string): CatalogObject | undefined {
	const match = /^CREATE (TABLE|INDEX) IF NOT EXISTS (\w+)/.exec(statement);
	return match ? { kind: match[1] === 'TABLE' ? 'table' : 'index', name: match[2]! } : undefined;
}

/**
 * Whether a failed Postgres create lost a race to a concurrent creator, and the
 * object now exists. `IF NOT EXISTS` checks the name without a lock, so both
 * sessions can pass the check and the loser fails on a catalog index. Any other
 * failure, or an object that is still absent, is not a race.
 * @private
 */
async function lostCatalogRace(db: SQLDatabase, error: unknown, object: CatalogObject | undefined): Promise<boolean> {
	if (db.dialect !== 'postgres' || !object) {
		return false;
	}
	const raced = errorChain(error).some((link) => {
		const { code, constraint } = link as { code?: unknown; constraint?: unknown };
		return code === '42P07' || (code === '23505' && POSTGRES_CATALOG_CONSTRAINTS.has(constraint));
	});
	if (!raced) {
		return false;
	}
	const [catalog, column] = object.kind === 'table' ? ['pg_tables', 'tablename'] : ['pg_indexes', 'indexname'];
	const row = await db
		.prepare(`SELECT 1 AS present FROM ${catalog} WHERE schemaname = current_schema() AND ${column} = ?`)
		.bind(object.name)
		.first();
	return Boolean(row);
}

/**
 * Runs one statement of {@link buildShardSidecarSchema} or {@link buildJournalSchema}.
 * A lost catalog race counts as success. Any other failure is wrapped the way
 * {@link createSchema} wraps it.
 * @private
 */
async function createCatalogStatement(db: SQLDatabase, statement: string): Promise<void> {
	try {
		await db.prepare(statement).run();
	} catch (error) {
		if (await lostCatalogRace(db, error, catalogObjectOf(statement))) {
			return;
		}
		console.error('Failed to execute schema statement:', statement, error);
		throw new CollegeDBError(`Schema migration failed: ${error}`, 'SCHEMA_MIGRATION_FAILED');
	}
}

/** MySQL and MariaDB error number for a key name that already exists (`ER_DUP_KEYNAME`). @private */
const MYSQL_DUPLICATE_KEY_NAME = 1061;

function isDuplicateKeyName(error: unknown): boolean {
	for (const candidate of [error, (error as { cause?: unknown } | null)?.cause]) {
		const { errno, code } = (candidate ?? {}) as { errno?: unknown; code?: unknown };
		if (errno === MYSQL_DUPLICATE_KEY_NAME || code === 'ER_DUP_KEYNAME') {
			return true;
		}
	}
	return false;
}

/**
 * Creates an index unless the table already has one with that name, on any
 * dialect and any number of times.
 *
 * SQLite and Postgres run `CREATE INDEX IF NOT EXISTS`. MySQL and MariaDB report
 * the same `mysql` dialect and neither accepts that form on MySQL proper, so
 * they look the name up in `information_schema.statistics` for the current
 * database first. Two processes that both find it missing race to create it; the
 * loser gets `ER_DUP_KEYNAME` (1061), which counts as success. Postgres reports
 * the same race as a catalog unique violation, which counts as success once the
 * index exists. Any other error propagates.
 *
 * @param db - The shard that holds the table
 * @param index - Table, index name and columns
 * @since 1.4.0
 */
export async function ensureIndex(db: SQLDatabase, index: IndexSpec): Promise<void> {
	validateIdentifier(index.table);
	validateIdentifier(index.name);
	index.columns.forEach((column) => validateIdentifier(column));
	const on = `${index.name} ON ${index.table} (${index.columns.join(', ')})`;

	if (db.dialect !== 'mysql') {
		try {
			await db.prepare(`CREATE INDEX IF NOT EXISTS ${on}`).run();
		} catch (error) {
			if (!(await lostCatalogRace(db, error, { kind: 'index', name: index.name }))) {
				throw error;
			}
		}
		return;
	}

	const present = await db
		.prepare(
			'SELECT 1 AS present FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ? LIMIT 1'
		)
		.bind(index.table, index.name)
		.first();
	if (present) {
		return;
	}

	try {
		await db.prepare(`CREATE INDEX ${on}`).run();
	} catch (error) {
		if (!isDuplicateKeyName(error)) {
			throw error;
		}
	}
}

/**
 * Creates the tombstone and topology tables on a shard, once per provider.
 * @since 1.4.0
 */
export async function ensureShardTables(db: SQLDatabase): Promise<void> {
	await ensureOnce(tableMemo, db, buildShardSidecarSchema(db.dialect));
}

/**
 * Creates the sidecar tables on every shard and the journal tables on the
 * journal shard.
 * @since 1.4.0
 */
export async function ensureJournalSchema(shards: Record<string, SQLDatabase>, journalBinding: string): Promise<void> {
	const journal = shards[journalBinding];
	if (!journal) {
		throw new CollegeDBError(`Journal shard ${journalBinding} is not a configured shard`, 'INVALID_JOURNAL_SHARD');
	}

	await Promise.all(Object.values(shards).map((db) => (db && isProvenJournalDialect(db.dialect) ? ensureShardTables(db) : undefined)));
	await ensureOnce(journalMemo, journal, buildJournalSchema(journal.dialect), [HISTORY_INDEX]);
}

function shardDb(env: MigrationEnv, binding: string): SQLDatabase {
	const db = env.shards[binding];
	if (!db) {
		throw new CollegeDBError(`Shard ${binding} not found in configuration`, 'SHARD_NOT_FOUND');
	}
	return db;
}

function journalDb(env: MigrationEnv): SQLDatabase {
	return shardDb(env, env.journalBinding);
}

const nowOf = (env: MigrationEnv): number => env.now?.() ?? Date.now();

async function journalCall<T>(work: () => Promise<T>): Promise<T> {
	try {
		return await work();
	} catch (error) {
		if (error instanceof CollegeDBError) {
			throw error;
		}
		const wrapped = new CollegeDBError(
			`Migration journal unavailable: ${error instanceof Error ? error.message : String(error)}`,
			'JOURNAL_UNAVAILABLE'
		);
		Object.assign(wrapped, { cause: error });
		throw wrapped;
	}
}

const runOn = (db: SQLDatabase, query: { sql: string; bindings: any[] }) =>
	db
		.prepare(query.sql)
		.bind(...query.bindings)
		.run();

/**
 * Stops an executor whose recovery lease another holder took. A no-op for a live
 * move, which holds no lease.
 * @private
 */
async function assertLease(env: MigrationEnv): Promise<void> {
	const fence = env.fence;
	if (!fence) {
		return;
	}

	const journal = journalDb(env);
	const row = await journalCall(() =>
		journal.prepare(`SELECT generation FROM ${LEASE_TABLE} WHERE scope = ?`).bind(fence.scope).first<{ generation: number }>()
	);
	if (Number(row?.generation) !== fence.generation) {
		throw new CollegeDBError(
			`Recovery lease generation ${fence.generation} for scope ${fence.scope} was taken over (now ${row ? Number(row.generation) : 'gone'}). ` +
				'This executor made no further writes; the new holder owns the work.',
			'RECOVERY_FENCED'
		);
	}
}

/**
 * Runs one journal write. An executor that holds a lease carries the fence in the
 * statement itself, so a stale one matches no row. A zero or unreported row count
 * is ambiguous (a fenced miss, an ignored duplicate, a claim already gone), so the
 * lease is read to tell them apart.
 * @private
 */
async function journalWrite(env: MigrationEnv, query: BuiltQuery): Promise<void> {
	const journal = journalDb(env);
	const result = await journalCall(() => runOn(journal, query));
	if (env.fence && (result.meta?.changes ?? 0) < 1) {
		await assertLease(env);
	}
}

function assertProven(env: MigrationEnv, bindings: string[]): void {
	for (const binding of new Set([env.journalBinding, ...bindings])) {
		const dialect = shardDb(env, binding).dialect;
		if (!isProvenJournalDialect(dialect)) {
			throw new CollegeDBError(
				`Journal moves are not proven on the ${dialect} dialect (shard ${binding}). Only ${PROVEN_JOURNAL_DIALECTS.join(', ')} is supported.`,
				'MIGRATION_UNSUPPORTED_DIALECT'
			);
		}
	}
}

/**
 * Refuses a shard whose `runBatch` is missing or not declared atomic. The cutover
 * and the key lock rely on a failing group rolling back whole.
 * @private
 */
export function assertAtomicBatch(db: SQLDatabase, label: string, what: string): void {
	if (!db.runBatch) {
		throw new CollegeDBError(`${label} has no transactional runBatch, so ${what}`, 'MIGRATION_NO_BATCH');
	}
	if (db.atomicBatch !== true) {
		throw new CollegeDBError(
			`${label} does not declare atomicBatch, so ${what}. Use an adapter that declares it, or set atomicBatch: true on a custom provider once a failing-batch test shows runBatch rolls back.`,
			'MIGRATION_NO_ATOMIC_BATCH'
		);
	}
}

const CUTOVER_NEEDS_ATOMIC = 'the cutover cannot be made atomic';

async function hook(step: string, h: Header): Promise<void> {
	await stepHook?.(step, { opId: h.opId, table: h.table, key: h.key });
}

async function record(
	env: MigrationEnv,
	h: Header,
	state: MigrationState,
	extra: { attempt?: number; digest?: string; detail?: unknown } = {}
): Promise<void> {
	const journal = journalDb(env);
	const query = buildInsertIgnore(
		HISTORY,
		{
			op_id: h.opId,
			state,
			attempt: extra.attempt ?? 1,
			scope: h.scope,
			table_name: h.table,
			primary_key: h.key,
			source: h.source,
			destination: h.destination,
			generation: h.generation,
			placement_epoch: env.placementEpoch?.() ?? null,
			digest: extra.digest ?? null,
			detail: extra.detail === undefined ? null : JSON.stringify(extra.detail),
			at: nowOf(env)
		},
		journal.dialect,
		env.fence
	);
	await journalWrite(env, query);
}

async function claimKey(env: MigrationEnv, table: string, key: string, opId: string): Promise<void> {
	const journal = journalDb(env);
	await journalCall(async () => {
		await runOn(
			journal,
			buildInsertIgnore(
				ACTIVE,
				{ scope: env.scope, table_name: table, primary_key: key, op_id: opId, claimed_at: nowOf(env) },
				journal.dialect
			)
		);
		const holder = await journal
			.prepare(`SELECT op_id FROM ${ACTIVE} WHERE scope = ? AND table_name = ? AND primary_key = ?`)
			.bind(env.scope, table, key)
			.first<{ op_id: string }>();

		if (holder?.op_id !== opId) {
			throw new CollegeDBError(
				`Key ${key} in ${table} is already being moved${holder ? ` by operation ${holder.op_id}` : ''}; run recoverMigrations() or retry`,
				'MIGRATION_IN_PROGRESS'
			);
		}
	});
}

async function releaseClaim(env: MigrationEnv, h: Pick<Header, 'scope' | 'table' | 'key' | 'opId'>): Promise<void> {
	const journal = journalDb(env);
	const fence = env.fence ? buildLeaseFence(env.fence, journal.dialect) : { sql: '', bindings: [] };
	await journalWrite(env, {
		sql: `DELETE FROM ${ACTIVE} WHERE scope = ? AND table_name = ? AND primary_key = ? AND op_id = ?${fence.sql ? ` AND ${fence.sql}` : ''}`,
		bindings: [h.scope, h.table, h.key, h.opId, ...fence.bindings]
	});
}

function orderOf(state: MigrationState, attempt: number): number {
	const rank = STATE_RANK[state];
	return rank >= STATE_RANK.committed ? 1000 + rank : attempt * 10 + rank;
}

function resolveOptions(options: MoveRecordOptions = {}): Header['options'] {
	return {
		pkColumn: options.pkColumn ?? 'id',
		onConflict: options.onConflict ?? 'fail',
		versionColumn: options.versionColumn,
		retire: options.retire ?? { afterMs: DEFAULT_RETIRE_AFTER_MS },
		maxBindParameters: options.maxBindParameters ?? DEFAULT_MAX_BIND_PARAMETERS
	};
}

async function loadOp(env: MigrationEnv, opId: string): Promise<OpRecord | null> {
	const journal = journalDb(env);
	const rows = await journalCall(
		async () => (await journal.prepare(`SELECT * FROM ${HISTORY} WHERE op_id = ?`).bind(opId).all<Record<string, any>>()).results
	);
	return foldHistory(rows);
}

function foldHistory(rows: Array<Record<string, any>>): OpRecord | null {
	if (rows.length === 0) {
		return null;
	}

	let latest = rows[0]!;
	let latestDigest: Record<string, any> | null = null;
	const digests = new Set<string>();
	let options: Header['options'] = resolveOptions();
	let retireAfter: number | null = null;
	let retiredAt: number | null = null;

	for (const row of rows) {
		const order = orderOf(row.state, Number(row.attempt));
		if (order >= orderOf(latest.state, Number(latest.attempt))) {
			latest = row;
		}
		if (row.digest) {
			digests.add(row.digest);
			if (!latestDigest || order >= orderOf(latestDigest.state, Number(latestDigest.attempt))) {
				latestDigest = row;
			}
		}
		if (row.state === 'prepared' && row.detail && Number(row.attempt) === 1) {
			options = resolveOptions(JSON.parse(row.detail));
		}
		if (row.state === 'retired') {
			retiredAt = Number(row.at);
			retireAfter = row.detail ? Number(JSON.parse(row.detail).retireAfter) : null;
		}
	}

	return {
		opId: latest.op_id,
		scope: latest.scope,
		table: latest.table_name,
		key: latest.primary_key,
		source: latest.source,
		destination: latest.destination,
		generation: Number(latest.generation),
		options,
		state: latest.state,
		attempt: Number(latest.attempt),
		digest: latestDigest?.digest ?? null,
		digests,
		retireAfter,
		retiredAt,
		updatedAt: Number(latest.at)
	};
}

/**
 * Finds the tombstone a shard holds for a key, if any.
 *
 * Read paths use it to follow a moved key to its destination, and write paths
 * use it to tell a stale route from a genuinely missing row. The sidecar table
 * must exist; call {@link ensureShardTables} first.
 *
 * @param db - The shard to check
 * @param scope - Journal scope
 * @param key - Routing key
 * @param table - Restrict to one table, or omit to match any
 * @returns The newest tombstone, or `null`
 * @since 1.4.0
 */
export async function findTombstone(
	db: SQLDatabase,
	scope: string,
	key: string,
	table?: string
): Promise<{ opId: string; table: string; generation: number; movedTo: string } | null> {
	const row = await db
		.prepare(
			`SELECT op_id, table_name, generation, moved_to FROM ${RETIRED_TABLE} WHERE scope = ? AND primary_key = ?` +
				`${table === undefined ? '' : ' AND table_name = ?'} ORDER BY generation DESC LIMIT 1`
		)
		.bind(...(table === undefined ? [scope, key] : [scope, key, table]))
		.first<{ op_id: string; table_name: string; generation: number; moved_to: string }>();

	return row ? { opId: row.op_id, table: row.table_name, generation: Number(row.generation), movedTo: row.moved_to } : null;
}

/**
 * Runs one router-issued write on a shard, serialized against that key's
 * cutover.
 *
 * On Postgres, MySQL and MariaDB the key lock and the write run as one
 * `runBatch` group, so they share a connection and a transaction and the lock
 * lasts until commit. SQLite and D1 run the plain statement, because their
 * writers already serialize. A shard that needs the lock and cannot run a
 * transactional batch refuses, because an unserialized write could create a
 * row after the cutover removed it. A batch counts only when the adapter
 * declares {@link SQLDatabase.atomicBatch}.
 *
 * @param db - The shard the write is routed to
 * @param guard - Scope, table and key the write targets
 * @param query - The write, as built by the query builders
 * @returns The result of the write statement
 * @throws {CollegeDBError} `MIGRATION_NO_BATCH` when the shard has no `runBatch`, `MIGRATION_NO_ATOMIC_BATCH` when it does not declare `atomicBatch`
 * @since 1.4.0
 */
export async function runGuardedWrite<T = Record<string, unknown>>(
	db: SQLDatabase,
	guard: TombstoneGuard,
	query: BuiltQuery
): Promise<QueryResult<T>> {
	// the key lock, then the shared fence lock, so a fence raise waits for this write
	const lock = [...buildWriteLock(guard, db.dialect), ...buildFenceLock('shared', db.dialect)];
	if (lock.length === 0) {
		return await db
			.prepare(query.sql)
			.bind(...query.bindings)
			.run<T>();
	}

	assertAtomicBatch(db, `A ${db.dialect} shard`, 'a write cannot be serialized against a cutover');

	const results = await db.runBatch!<T>([...lock, query]);
	return results[lock.length]!;
}

async function journaledDigests(env: MigrationEnv, opId: string): Promise<Set<string>> {
	return (await loadOp(env, opId))?.digests ?? new Set();
}

async function nextGeneration(env: MigrationEnv, table: string, key: string): Promise<number> {
	const journal = journalDb(env);
	const row = await journalCall(() =>
		journal
			.prepare(`SELECT MAX(generation) AS top FROM ${HISTORY} WHERE scope = ? AND table_name = ? AND primary_key = ?`)
			.bind(env.scope, table, key)
			.first<{ top: number | null }>()
	);
	return Number(row?.top ?? 0) + 1;
}

/**
 * Ends an operation without moving the key. Returns `false`, and changes
 * nothing, when this operation's cutover already ran: the archive row is the
 * physical proof, and past it the destination copy is the live row.
 * @private
 */
async function abortOp(env: MigrationEnv, h: Header, reason: string, discardCopy: boolean): Promise<boolean> {
	const archived = await archiveRow(shardDb(env, h.source), env.scope, h.table, h.key);
	if (archived?.op_id === h.opId) {
		return false;
	}

	if (discardCopy) {
		await discardDestinationCopy(env, h);
	}
	await record(env, h, 'aborted', { detail: { reason } });
	await releaseClaim(env, h);
	return true;
}

/**
 * Removes the destination copy, but only when it equals a row this operation
 * journaled, so a row that was already there is never touched.
 * @private
 */
async function discardDestinationCopy(env: MigrationEnv, h: Header): Promise<void> {
	const destination = shardDb(env, h.destination);
	const row = await readRow(destination, h.table, h.options.pkColumn, h.key);
	if (!row) {
		return;
	}

	const encoded = encodeRow(row);
	if (!(await journaledDigests(env, h.opId)).has(await sha256Hex(encoded.payload))) {
		return;
	}

	const { delete: del } = buildCutoverStatements(cutoverSpec(env, h, encoded, 1, destination.dialect));
	await assertLease(env);
	await runOn(destination, del);
}

function cutoverSpec(env: MigrationEnv, h: Header, encoded: EncodedRow, archivedAt: number, dialect?: SQLDialect): CutoverSpec {
	return {
		table: h.table,
		pkColumn: h.options.pkColumn,
		columns: bindable(encoded, dialect),
		versionColumn: h.options.versionColumn,
		scope: env.scope,
		key: h.key,
		opId: h.opId,
		generation: h.generation,
		movedTo: h.destination,
		payload: encoded.payload,
		archivedAt,
		dialect
	};
}

type CopyOutcome = { snapshot: EncodedRow; digest: string } | { aborted: string } | { cutOver: string };

/**
 * One copy attempt: snapshot the source row, journal its digest, copy it to the
 * destination, read it back. Safe to run again at any point.
 * @private
 */
async function attemptCopy(env: MigrationEnv, h: Header, attempt: number, first: boolean): Promise<CopyOutcome> {
	const source = shardDb(env, h.source);
	const destination = shardDb(env, h.destination);
	const row = await readRow(source, h.table, h.options.pkColumn, h.key);

	if (!row) {
		// another driver of this operation may have already cut the key over
		const archived = first ? null : await archiveRow(source, env.scope, h.table, h.key);
		if (archived?.op_id === h.opId && archived.payload !== null) {
			return { cutOver: await sha256Hex(archived.payload) };
		}

		await abortOp(env, h, first ? 'RECORD_NOT_FOUND' : 'SOURCE_ROW_MISSING', !first);
		if (first) {
			throw new CollegeDBError(`Record with primary key ${h.key} not found in source database`, 'RECORD_NOT_FOUND');
		}
		return { aborted: 'SOURCE_ROW_MISSING' };
	}

	let encoded: EncodedRow;
	try {
		encoded = encodeRow(row);
	} catch (error) {
		await abortOp(env, h, 'UNSUPPORTED_VALUE', false);
		throw error;
	}

	if (first) {
		const probe = buildCutoverStatements(cutoverSpec(env, h, encoded, 0, source.dialect));
		const widest = Math.max(probe.archive.bindings.length, probe.delete.bindings.length);
		if (widest > h.options.maxBindParameters) {
			await abortOp(env, h, 'ROW_TOO_WIDE', false);
			throw new CollegeDBError(
				`Row ${h.key} in ${h.table} needs ${widest} bind parameters to move safely but the limit is ${h.options.maxBindParameters}. ` +
					'Pass versionColumn, a column that changes on every write, so the cutover compares only that column.',
				'MIGRATION_ROW_TOO_WIDE'
			);
		}
	}

	const digest = await sha256Hex(encoded.payload);
	await record(env, h, 'prepared', {
		attempt,
		digest,
		detail: first && attempt === 1 ? { ...h.options } : undefined
	});
	await hook('prepared', h);

	const existing = await readRow(destination, h.table, h.options.pkColumn, h.key);
	await assertLease(env);
	if (existing) {
		const existingEncoded = encodeRow(existing);
		if (existingEncoded.payload !== encoded.payload) {
			const ours = (await journaledDigests(env, h.opId)).has(await sha256Hex(existingEncoded.payload));
			if (!ours && h.options.onConflict !== 'replace') {
				await abortOp(env, h, 'DESTINATION_CONFLICT', false);
				throw new CollegeDBError(
					`Destination ${h.destination} already holds a different row for ${h.key} in ${h.table}; pass onConflict: 'replace' to overwrite it`,
					'DESTINATION_CONFLICT'
				);
			}
			await deleteRowByKey(destination, h.table, h.options.pkColumn, existingEncoded);
			await insertSnapshot(destination, h.table, encoded);
		}
	} else {
		await insertSnapshot(destination, h.table, encoded);
	}
	await record(env, h, 'copied', { attempt, digest });
	await hook('copied', h);

	const copied = await readRow(destination, h.table, h.options.pkColumn, h.key);
	if (!copied || encodeRow(copied).payload !== encoded.payload) {
		await abortOp(env, h, 'VERIFY_FAILED', true);
		throw new CollegeDBError(`Destination copy of ${h.key} did not match the source row`, 'MIGRATION_VERIFY_FAILED');
	}
	await record(env, h, 'verified', { attempt, digest });
	await hook('verified', h);

	return { snapshot: encoded, digest };
}

/**
 * The source cutover: an archive insert and a delete in one transaction, both
 * conditional on the snapshot. Returns whether this operation's archive row
 * exists afterward, which is the post-condition that matters.
 * @private
 */
async function runCutover(env: MigrationEnv, h: Header, attempt: number, encoded: EncodedRow, digest: string): Promise<boolean> {
	const source = shardDb(env, h.source);
	await record(env, h, 'cutover_pending', { attempt, digest });
	await hook('cutover_pending', h);

	assertAtomicBatch(source, `Shard ${h.source}`, CUTOVER_NEEDS_ATOMIC);

	const statements = buildCutoverStatements(cutoverSpec(env, h, encoded, nowOf(env), source.dialect));
	await assertLease(env);
	await source.runBatch!([
		...buildWriteLock({ scope: env.scope, table: h.table, key: h.key }, source.dialect),
		{ sql: statements.archive.sql, bindings: statements.archive.bindings },
		{ sql: statements.delete.sql, bindings: statements.delete.bindings }
	]);

	const archived = await archiveRow(source, env.scope, h.table, h.key);
	await hook('cas', h);
	return archived?.op_id === h.opId;
}

async function archiveRow(
	db: SQLDatabase,
	scope: string,
	table: string,
	key: string
): Promise<{ op_id: string; payload: string | null; generation: number } | null> {
	return await db
		.prepare(`SELECT op_id, payload, generation FROM ${RETIRED_TABLE} WHERE scope = ? AND table_name = ? AND primary_key = ?`)
		.bind(scope, table, key)
		.first();
}

/**
 * Everything after a successful cutover. Idempotent, so recovery can enter it
 * at `committed` or `published`.
 * @private
 */
async function finalize(env: MigrationEnv, h: Header, from: 'committed' | 'published', attempt: number, digest: string): Promise<void> {
	if (from === 'committed') {
		await record(env, h, 'committed', { attempt, digest });
		await hook('committed', h);

		const destination = shardDb(env, h.destination);
		await assertLease(env);
		await destination
			.prepare(`DELETE FROM ${RETIRED_TABLE} WHERE scope = ? AND table_name = ? AND primary_key = ? AND op_id <> ?`)
			.bind(env.scope, h.table, h.key, h.opId)
			.run();

		await assertLease(env);
		try {
			await env.publish({
				opId: h.opId,
				table: h.table,
				key: h.key,
				source: h.source,
				destination: h.destination,
				generation: h.generation
			});
		} catch (error) {
			const wrapped = new CollegeDBError(
				`Move ${h.opId} of ${h.key} is committed but publishing derived state failed: ${error instanceof Error ? error.message : String(error)}. ` +
					'Run recoverMigrations() to finish it.',
				'MIGRATION_PUBLISH_FAILED'
			);
			Object.assign(wrapped, { cause: error });
			throw wrapped;
		}

		await record(env, h, 'published', { attempt, digest });
		await hook('published', h);
	}

	const retireAfter = h.options.retire === 'immediate' ? nowOf(env) : nowOf(env) + h.options.retire.afterMs;
	await record(env, h, 'retired', { attempt, digest, detail: { retireAfter } });
	await releaseClaim(env, h);
	await hook('retired', h);
}

/**
 * Runs copy and cutover attempts until one commits or the budget is spent.
 * @private
 */
async function drive(env: MigrationEnv, h: Header, firstAttempt: number, first: boolean): Promise<MoveResult> {
	const result = (outcome: MoveResult['outcome'], reason?: string): MoveResult => ({
		opId: h.opId,
		outcome,
		reason,
		generation: h.generation,
		source: h.source,
		destination: h.destination
	});

	for (let i = 0; i <= MAX_RECOPY; i++) {
		const attempt = firstAttempt + i;
		const copy = await attemptCopy(env, h, attempt, first && i === 0);
		if ('aborted' in copy) {
			return result('aborted', copy.aborted);
		}
		if ('cutOver' in copy) {
			await finalize(env, h, 'committed', attempt, copy.cutOver);
			return result('committed');
		}

		if (await runCutover(env, h, attempt, copy.snapshot, copy.digest)) {
			await finalize(env, h, 'committed', attempt, copy.digest);
			return result('committed');
		}
	}

	if (!(await abortOp(env, h, 'CAS_MISS', true))) {
		await finalize(env, h, 'committed', firstAttempt + MAX_RECOPY, '');
		return result('committed');
	}
	return result('aborted', 'CAS_MISS');
}

/**
 * Moves one keyed record from one shard to another through the journal.
 *
 * Follows the protocol in `docs/migration-protocol.md`: claim, snapshot, copy,
 * verify, journal `cutover_pending`, then one source transaction that archives
 * and deletes the row only if it still equals the snapshot. A changed row makes
 * the cutover miss and the move recopies, up to three times, before it aborts
 * and leaves the source authoritative.
 *
 * @param env - Journal, shards and the publish hook
 * @param move - Table, routing key, source and destination bindings, and options
 * @returns The outcome. `aborted` means the source still owns the key.
 * @throws {CollegeDBError} `MIGRATION_UNSUPPORTED_DIALECT`, `MIGRATION_IN_PROGRESS`,
 * `MIGRATION_SOURCE_TOMBSTONED`, `RECORD_NOT_FOUND`, `DESTINATION_CONFLICT`,
 * `MIGRATION_ROW_TOO_WIDE`, `JOURNAL_UNAVAILABLE`, `MIGRATION_PUBLISH_FAILED`
 * @since 1.4.0
 */
export async function moveRecord(
	env: MigrationEnv,
	move: { table: string; key: string; source: string; destination: string; options?: MoveRecordOptions }
): Promise<MoveResult> {
	if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(move.table)) {
		throw new CollegeDBError(`Invalid table name for migration: ${move.table}`, 'INVALID_TABLE_NAME');
	}

	const options = resolveOptions(move.options);
	validateIdentifier(options.pkColumn);
	if (options.versionColumn) {
		validateIdentifier(options.versionColumn);
	}

	assertProven(env, [move.source, move.destination]);
	assertAtomicBatch(shardDb(env, move.source), `Shard ${move.source}`, CUTOVER_NEEDS_ATOMIC);
	await ensureJournalSchema(env.shards, env.journalBinding);

	const opId = crypto.randomUUID();
	await claimKey(env, move.table, move.key, opId);

	const generation = await nextGeneration(env, move.table, move.key);
	const h: Header = {
		opId,
		scope: env.scope,
		table: move.table,
		key: move.key,
		source: move.source,
		destination: move.destination,
		generation,
		options
	};

	const tombstone = await findTombstone(shardDb(env, move.source), env.scope, move.key, move.table);
	if (tombstone) {
		await releaseClaim(env, h);
		throw new CollegeDBError(
			`Shard ${move.source} holds a tombstone for ${move.key} from operation ${tombstone.opId}, so the key already left it. Run verifyMigrations().`,
			'MIGRATION_SOURCE_TOMBSTONED'
		);
	}

	return await drive(env, h, 1, true);
}

/**
 * Resumes one unfinished operation using the recovery rules for its state.
 * @private
 */
async function resume(env: MigrationEnv, op: OpRecord): Promise<MoveResult | null> {
	const source = shardDb(env, op.source);
	const destination = shardDb(env, op.destination);
	const done = (outcome: MoveResult['outcome'], reason?: string): MoveResult => ({
		opId: op.opId,
		outcome,
		reason,
		generation: op.generation,
		source: op.source,
		destination: op.destination
	});

	switch (op.state) {
		case 'prepared':
		case 'copied':
		case 'verified':
			return await drive(env, op, op.attempt + 1, false);

		case 'cutover_pending': {
			const sourceRow = await readRow(source, op.table, op.options.pkColumn, op.key);
			const archive = await archiveRow(source, op.scope, op.table, op.key);
			const ours = archive?.op_id === op.opId;

			if (!sourceRow && ours) {
				const copied = await readRow(destination, op.table, op.options.pkColumn, op.key);
				const matches =
					copied !== null &&
					op.digest !== null &&
					(await sha256Hex(encodeRow(copied).payload)) === op.digest &&
					archive?.payload !== null &&
					(await sha256Hex(archive!.payload!)) === op.digest;

				if (!matches) {
					throw new CollegeDBError(
						`Operation ${op.opId} cut over ${op.key} but the destination row does not match the archive`,
						'MIGRATION_AMBIGUOUS'
					);
				}
				await finalize(env, op, 'committed', op.attempt, op.digest!);
				return done('committed');
			}

			if (sourceRow && ours) {
				throw new CollegeDBError(`Operation ${op.opId} left both a live row and an archive for ${op.key}`, 'MIGRATION_AMBIGUOUS');
			}

			if (!sourceRow) {
				if (await abortOp(env, op, 'SOURCE_ROW_MISSING', true)) {
					return done('aborted', 'SOURCE_ROW_MISSING');
				}
				await finalize(env, op, 'committed', op.attempt, op.digest ?? '');
				return done('committed');
			}

			const encoded = encodeRow(sourceRow);
			if ((await sha256Hex(encoded.payload)) !== op.digest) {
				if (await abortOp(env, op, 'SOURCE_CHANGED', true)) {
					return done('aborted', 'SOURCE_CHANGED');
				}
				await finalize(env, op, 'committed', op.attempt, op.digest ?? '');
				return done('committed');
			}

			if (await runCutover(env, op, op.attempt, encoded, op.digest!)) {
				await finalize(env, op, 'committed', op.attempt, op.digest!);
				return done('committed');
			}
			return await drive(env, op, op.attempt + 1, false);
		}

		case 'committed':
			await finalize(env, op, 'committed', op.attempt, op.digest ?? '');
			return done('committed');

		case 'published':
			await finalize(env, op, 'published', op.attempt, op.digest ?? '');
			return done('committed');

		case 'retired':
			await releaseClaim(env, op);
			return done('committed');

		case 'aborted':
			await releaseClaim(env, op);
			return done('aborted', 'ABORTED');
	}
}

/**
 * Resumes every unfinished move recorded in the journal.
 *
 * Each operation is handled by its state, per the rules in
 * `docs/migration-protocol.md`. A failure on one operation is reported and the
 * rest continue. If the journal itself cannot be read, nothing proceeds and the
 * call throws.
 *
 * One recovery runs per scope at a time. It takes a lease row in the journal
 * first, with a compare-and-swap that bumps a generation, and every journal write
 * it makes after that carries the generation in the statement. A second call
 * while the lease is live throws `RECOVERY_IN_PROGRESS`, which a scheduler can
 * treat as success. A holder that crashed is taken over after the lease expires
 * (60 seconds without a renewal). An executor whose lease was taken over stops at
 * its next journal write or cross-shard step and throws `RECOVERY_FENCED`.
 *
 * A live move does not take the lease. Recovery and a live move can both drive one
 * operation: each step is safe to run twice, and an abort is refused once the
 * cutover has run. Pass `olderThanMs` to leave alone operations that changed more
 * recently, so a timer does not race a move that is still going.
 *
 * @param env - Journal, shards and the publish hook
 * @param options - `olderThanMs` skips operations touched more recently than that
 * @returns What happened to each active claim
 * @throws {CollegeDBError} `RECOVERY_IN_PROGRESS` when another recovery holds the lease; `RECOVERY_FENCED` when this one lost it mid-run; `JOURNAL_UNAVAILABLE` when the journal cannot be read
 * @since 1.4.0
 */
export async function recoverMigrations(env: MigrationEnv, options: { olderThanMs?: number } = {}): Promise<RecoveryReport> {
	await ensureJournalSchema(env.shards, env.journalBinding);
	const fence = await acquireLease(env);

	let fenced = false;
	try {
		return await recoverUnderLease({ ...env, fence }, options);
	} catch (error) {
		fenced = error instanceof CollegeDBError && error.code === 'RECOVERY_FENCED';
		throw error;
	} finally {
		// best effort: a failed release only leaves the lease to expire; a fenced executor writes nothing more
		if (!fenced) {
			const journal = journalDb(env);
			await runOn(journal, buildLeaseRelease(fence, journal.dialect)).catch(() => undefined);
		}
	}
}

/**
 * Takes the recovery lease of the scope, or throws `RECOVERY_IN_PROGRESS`. The
 * holder is a fresh id per call, so the read after the swap tells whether this
 * call won it.
 * @private
 */
async function acquireLease(env: MigrationEnv): Promise<LeaseFence> {
	const journal = journalDb(env);
	const holder = crypto.randomUUID();

	return await journalCall(async () => {
		await runOn(journal, buildLeaseAcquire(env.scope, holder, nowOf(env), RECOVERY_LEASE_MS, journal.dialect));
		const row = await journal
			.prepare(`SELECT holder, generation FROM ${LEASE_TABLE} WHERE scope = ?`)
			.bind(env.scope)
			.first<{ holder: string; generation: number }>();

		if (row?.holder !== holder) {
			throw new CollegeDBError(
				`Another recovery holds the lease for scope ${env.scope}; it will finish or its lease will expire, so retry later`,
				'RECOVERY_IN_PROGRESS'
			);
		}
		return { scope: env.scope, generation: Number(row.generation) };
	});
}

async function recoverUnderLease(env: MigrationEnv, options: { olderThanMs?: number }): Promise<RecoveryReport> {
	const journal = journalDb(env);
	const claims = await journalCall(
		async () =>
			(
				await journal
					.prepare(`SELECT op_id, table_name, primary_key, claimed_at FROM ${ACTIVE} WHERE scope = ?`)
					.bind(env.scope)
					.all<{ op_id: string; table_name: string; primary_key: string; claimed_at: number }>()
			).results
	);

	const report: RecoveryReport = { examined: claims.length, committed: [], aborted: [], released: [], skipped: [], failed: [] };
	const quietFor = options.olderThanMs ?? 0;

	for (const claim of claims) {
		await journalWrite(env, buildLeaseRenew(env.fence!, nowOf(env) + RECOVERY_LEASE_MS, journal.dialect));

		try {
			const op = await loadOp(env, claim.op_id);
			if (!op) {
				// a claim with no history is either a crash between claim and first record, or a move starting right now
				if (nowOf(env) - Number(claim.claimed_at) < ORPHAN_CLAIM_GRACE_MS) {
					report.skipped.push(claim.op_id);
					continue;
				}
				await releaseClaim(env, { opId: claim.op_id, scope: env.scope, table: claim.table_name, key: claim.primary_key });
				report.released.push(claim.op_id);
				continue;
			}

			if (nowOf(env) - op.updatedAt < quietFor) {
				report.skipped.push(claim.op_id);
				continue;
			}

			assertProven(env, [op.source, op.destination]);
			assertAtomicBatch(shardDb(env, op.source), `Shard ${op.source}`, CUTOVER_NEEDS_ATOMIC);
			const result = await resume(env, op);
			if (result?.outcome === 'committed') {
				report.committed.push({ opId: op.opId, key: op.key, destination: op.destination });
			} else if (result) {
				report.aborted.push({ opId: op.opId, key: op.key, reason: result.reason ?? 'ABORTED' });
			}
		} catch (error) {
			if (error instanceof CollegeDBError && error.code === 'RECOVERY_FENCED') {
				throw error;
			}
			// a stale step can fail on the new holder's work; that failure is not this executor's to report
			await assertLease(env);
			report.failed.push({
				opId: claim.op_id,
				key: claim.primary_key,
				error: error instanceof Error ? error.message : String(error),
				code: error instanceof CollegeDBError ? error.code : undefined
			});
		}
	}

	return report;
}

/**
 * Reads the journal's view of the newest operation on a key.
 *
 * @param env - Journal and shards
 * @param key - Routing key
 * @param table - Restrict to one table
 * @returns The newest operation, or `null` when the key was never moved
 * @since 1.4.0
 */
export async function getMigrationStatus(env: MigrationEnv, key: string, table?: string): Promise<MigrationStatus | null> {
	await ensureJournalSchema(env.shards, env.journalBinding);
	const journal = journalDb(env);
	const rows = await journalCall(
		async () =>
			(
				await journal
					.prepare(
						`SELECT * FROM ${HISTORY} WHERE scope = ? AND primary_key = ?${table === undefined ? '' : ' AND table_name = ?'} ORDER BY generation DESC`
					)
					.bind(...(table === undefined ? [env.scope, key] : [env.scope, key, table]))
					.all<Record<string, any>>()
			).results
	);

	if (rows.length === 0) {
		return null;
	}

	const newest = rows.filter((row) => Number(row.generation) === Number(rows[0]!.generation) && row.op_id === rows[0]!.op_id);
	const op = foldHistory(newest)!;
	const active = await journalCall(() =>
		journal
			.prepare(`SELECT op_id FROM ${ACTIVE} WHERE scope = ? AND table_name = ? AND primary_key = ? AND op_id = ?`)
			.bind(env.scope, op.table, key, op.opId)
			.first()
	);

	return {
		opId: op.opId,
		table: op.table,
		state: op.state,
		generation: op.generation,
		source: op.source,
		destination: op.destination,
		active: active !== null,
		updatedAt: op.updatedAt
	};
}

async function listTombstones(db: SQLDatabase, scope: string) {
	return (
		await db
			.prepare(`SELECT table_name, primary_key, pk_column, op_id, payload, purged_at FROM ${RETIRED_TABLE} WHERE scope = ?`)
			.bind(scope)
			.all<{
				table_name: string;
				primary_key: string;
				pk_column: string;
				op_id: string;
				payload: string | null;
				purged_at: number | null;
			}>()
	).results;
}

type Tombstone = Awaited<ReturnType<typeof listTombstones>>[number];

/**
 * Handles a tombstone whose operation the journal ended as aborted, which only an
 * executor that stalled past its lease leaves: its cutover ran after the new
 * holder aborted. The row is restorable when the archive still holds it, the
 * operation is the newest on the key, and neither shard holds a different row.
 * @private
 */
async function checkStaleCutover(
	env: MigrationEnv,
	report: VerifyReport,
	repair: boolean,
	shard: string,
	db: SQLDatabase,
	tomb: Tombstone,
	live: Record<string, unknown> | null,
	op: OpRecord
): Promise<void> {
	const archived = tomb.payload;
	const sourceFree = live === null || (archived !== null && encodeRow(live).payload === archived);

	let destinationFree = false;
	const destination = env.shards[op.destination];
	if (destination) {
		try {
			destinationFree = (await readRow(destination, tomb.table_name, tomb.pk_column, tomb.primary_key)) === null;
		} catch {
			destinationFree = false;
		}
	}

	const newest = (await nextGeneration(env, op.table, op.key)) - 1 === op.generation;
	const restorable = archived !== null && sourceFree && destinationFree && newest;
	const where = `${shard}:${tomb.table_name}:${tomb.primary_key}`;

	if (!repair || !restorable) {
		report.staleCutovers.push({ shard, table: tomb.table_name, key: tomb.primary_key, opId: tomb.op_id, restorable });
		return;
	}

	assertAtomicBatch(db, `Shard ${shard}`, 'the archive cannot be restored atomically');
	const restore: BuiltQuery[] = [
		...buildWriteLock({ scope: env.scope, table: tomb.table_name, key: tomb.primary_key }, db.dialect),
		{
			sql: `DELETE FROM ${RETIRED_TABLE} WHERE scope = ? AND table_name = ? AND primary_key = ? AND op_id = ?`,
			bindings: [env.scope, tomb.table_name, tomb.primary_key, tomb.op_id]
		}
	];
	if (live === null) {
		restore.push(snapshotInsert(db, tomb.table_name, { columns: JSON.parse(archived).columns, payload: archived }));
	}
	await db.runBatch!(restore);
	report.repaired.push(where);
}

/**
 * Finds live rows on the destination of an aborted operation that equal a row the
 * operation journaled. The operation must be the newest on its key, so no later
 * move can have made that destination the owner. With `repair` the row is
 * removed, which is the proof {@link discardDestinationCopy} uses.
 * @private
 */
async function checkStrandedCopies(env: MigrationEnv, report: VerifyReport, repair: boolean): Promise<void> {
	const journal = journalDb(env);
	const aborted = await journalCall(
		async () =>
			(
				await journal
					.prepare(`SELECT DISTINCT op_id FROM ${HISTORY} WHERE scope = ? AND state = 'aborted'`)
					.bind(env.scope)
					.all<{ op_id: string }>()
			).results
	);
	const handled = new Set([...report.ghosts.map((g) => `${g.shard}:${g.table}:${g.key}`), ...report.repaired]);

	for (const { op_id } of aborted) {
		const op = await loadOp(env, op_id);
		const destination = op ? env.shards[op.destination] : undefined;
		if (!op || op.state !== 'aborted' || !destination || !isProvenJournalDialect(destination.dialect)) {
			continue;
		}
		if ((await nextGeneration(env, op.table, op.key)) - 1 !== op.generation) {
			continue;
		}

		let live: Record<string, unknown> | null = null;
		try {
			live = await readRow(destination, op.table, op.options.pkColumn, op.key);
		} catch {
			continue;
		}
		if (!live) {
			continue;
		}

		const encoded = encodeRow(live);
		const where = `${op.destination}:${op.table}:${op.key}`;
		if (handled.has(where) || !op.digests.has(await sha256Hex(encoded.payload))) {
			continue;
		}

		if (repair) {
			await deleteRowByKey(destination, op.table, op.options.pkColumn, encoded);
			report.repaired.push(where);
		} else {
			report.strandedCopies.push({ shard: op.destination, table: op.table, key: op.key, opId: op.opId });
		}
	}
}

/**
 * Checks the journal against the shards without changing anything.
 *
 * Reports unfinished operations, live rows on a shard that also holds a
 * tombstone for the key (ghosts), and tombstones the journal does not know. It
 * also reports what an executor that stalled past its recovery lease can leave
 * after the new holder aborted a move: a copy on the destination, or an archive
 * and a deleted source row. With `repair`, unfinished operations are resumed, a
 * ghost is removed only when it equals the archived payload (any other ghost is
 * reported and kept), a stranded copy is removed when it equals a row the
 * operation journaled, and a stale cutover is undone by restoring the archived
 * row. The result of the pass is recorded, which is what {@link purgeRetired}
 * waits for. Repair runs {@link recoverMigrations}, so it takes the same lease.
 *
 * @param env - Journal and shards
 * @param options - `repair` to resume operations and fix what the report lists, where the journal proves it
 * @returns What was found and what was repaired
 * @throws {CollegeDBError} With `repair`: `RECOVERY_IN_PROGRESS` when another recovery holds the lease, `RECOVERY_FENCED` when this one lost it
 * @since 1.4.0
 */
export async function verifyMigrations(env: MigrationEnv, options: { repair?: boolean } = {}): Promise<VerifyReport> {
	await ensureJournalSchema(env.shards, env.journalBinding);
	const journal = journalDb(env);
	const report: VerifyReport = {
		danglingOps: [],
		ghosts: [],
		strandedArchives: [],
		strandedCopies: [],
		staleCutovers: [],
		repaired: [],
		clean: true
	};

	if (options.repair) {
		await recoverMigrations(env);
	}

	const claims = await journalCall(
		async () => (await journal.prepare(`SELECT op_id FROM ${ACTIVE} WHERE scope = ?`).bind(env.scope).all<{ op_id: string }>()).results
	);
	for (const claim of claims) {
		const op = await loadOp(env, claim.op_id);
		report.danglingOps.push({ opId: claim.op_id, key: op?.key ?? '', state: op?.state ?? 'prepared' });
	}

	for (const [shard, db] of Object.entries(env.shards)) {
		if (!db || !isProvenJournalDialect(db.dialect)) {
			continue;
		}
		await ensureShardTables(db);

		for (const tomb of await listTombstones(db, env.scope)) {
			const op = await loadOp(env, tomb.op_id);
			if (!op) {
				report.strandedArchives.push({ shard, table: tomb.table_name, key: tomb.primary_key, opId: tomb.op_id });
			}

			let live: Record<string, unknown> | null = null;
			try {
				live = await readRow(db, tomb.table_name, tomb.pk_column, tomb.primary_key);
			} catch {
				continue;
			}

			if (op?.state === 'aborted' && op.source === shard) {
				await checkStaleCutover(env, report, options.repair === true, shard, db, tomb, live, op);
				continue;
			}
			if (!live) {
				continue;
			}

			const encoded = encodeRow(live);
			const equalsArchive = tomb.payload !== null && encoded.payload === tomb.payload;
			if (options.repair && equalsArchive) {
				await deleteRowByKey(db, tomb.table_name, tomb.pk_column, encoded);
				report.repaired.push(`${shard}:${tomb.table_name}:${tomb.primary_key}`);
			} else {
				report.ghosts.push({ shard, table: tomb.table_name, key: tomb.primary_key, opId: tomb.op_id, equalsArchive });
			}
		}
	}

	await checkStrandedCopies(env, report, options.repair === true);

	report.clean =
		report.danglingOps.length === 0 &&
		report.ghosts.length === 0 &&
		report.strandedArchives.length === 0 &&
		report.strandedCopies.length === 0 &&
		report.staleCutovers.length === 0;

	await journalCall(async () => {
		await journal.prepare('DELETE FROM collegedb_migration_verification WHERE scope = ?').bind(env.scope).run();
		await runOn(
			journal,
			buildInsertIgnore(
				'collegedb_migration_verification',
				{ scope: env.scope, completed_at: nowOf(env), clean: report.clean ? 1 : 0 },
				journal.dialect
			)
		);
	});

	return report;
}

/**
 * Clears the archived payload of finished moves whose retention has passed.
 *
 * An operation is refused until its `retire_after` time has passed and a
 * {@link verifyMigrations} pass that found nothing wrong completed after the
 * move finished. The tombstone stays, so stale writers are still fenced.
 *
 * @param env - Journal and shards
 * @returns The operations purged and the ones refused, with the reason
 * @since 1.4.0
 */
export async function purgeRetired(env: MigrationEnv): Promise<PurgeReport> {
	await ensureJournalSchema(env.shards, env.journalBinding);
	const journal = journalDb(env);
	const report: PurgeReport = { purged: [], refused: [] };

	const ops = await journalCall(
		async () =>
			(
				await journal
					.prepare(`SELECT DISTINCT op_id FROM ${HISTORY} WHERE scope = ? AND state = 'retired'`)
					.bind(env.scope)
					.all<{ op_id: string }>()
			).results
	);
	const verification = await journalCall(() =>
		journal
			.prepare('SELECT completed_at, clean FROM collegedb_migration_verification WHERE scope = ?')
			.bind(env.scope)
			.first<{ completed_at: number; clean: number }>()
	);

	for (const { op_id } of ops) {
		const op = await loadOp(env, op_id);
		if (!op || op.state !== 'retired') {
			continue;
		}

		const source = shardDb(env, op.source);
		const archive = await archiveRow(source, op.scope, op.table, op.key);
		if (!archive || archive.op_id !== op.opId || archive.payload === null) {
			continue;
		}

		if (op.retireAfter === null || nowOf(env) < op.retireAfter) {
			report.refused.push({ opId: op.opId, key: op.key, reason: 'RETENTION_NOT_ELAPSED' });
		} else if (!verification || Number(verification.clean) !== 1 || Number(verification.completed_at) <= (op.retiredAt ?? 0)) {
			report.refused.push({ opId: op.opId, key: op.key, reason: 'NOT_VERIFIED' });
		} else {
			await source
				.prepare(
					`UPDATE ${RETIRED_TABLE} SET payload = NULL, purged_at = ? WHERE scope = ? AND table_name = ? AND primary_key = ? AND op_id = ?`
				)
				.bind(nowOf(env), op.scope, op.table, op.key, op.opId)
				.run();
			report.purged.push({ opId: op.opId, key: op.key });
		}
	}

	return report;
}

// #endregion
