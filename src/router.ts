/**
 * @fileoverview Main routing and query distribution logic for CollegeDB
 *
 * This module provides the core functionality for routing database queries to the
 * appropriate D1 shard based on primary key mappings. It handles shard selection,
 * database routing, and provides a unified API for CRUD operations across multiple
 * distributed D1 databases.
 *
 * Key responsibilities:
 * - Initialize and manage the global CollegeDB configuration
 * - Route queries to appropriate shards based on primary key mappings
 * - Implement shard allocation strategies (round-robin, random, hash-based)
 * - Provide unified CRUD operations across distributed shards
 * - Coordinate with Durable Objects for centralized shard management
 * - Handle shard rebalancing and data migration
 *
 * @example
 * ```typescript
 * import { initialize, insert, selectByPrimaryKey } from './router.js';
 *
 * // Initialize the system
 * initialize({
 *   kv: env.KV,
 *   coordinator: env.ShardCoordinator,
 *   shards: {
 *     'db-east': env.DB_EAST,
 *     'db-west': env.DB_WEST
 *   },
 *   strategy: 'hash'
 * });
 *
 * // Insert a record (automatically routed to appropriate shard)
 * await insert('user-123', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-123', 'John']);
 *
 * // Query the record (routed to same shard)
 * const result = await selectByPrimaryKey('user-123', 'SELECT * FROM users WHERE id = ?', ['user-123']);
 * ```
 *
 * @author CollegeDB Team
 * @since 1.0.0
 */

import type { D1Database } from '@cloudflare/workers-types';
import { KVShardMapper } from './kvmap.js';
import type { CollegeDBConfig, QueryResult, ShardStats } from './types.js';

/**
 * Global configuration for the collegedb instance
 *
 * Stores the system-wide configuration including KV namespace, available shards,
 * coordinator settings, and allocation strategy. Must be initialized before
 * any routing operations can be performed.
 *
 * @private
 */
let globalConfig: CollegeDBConfig | null = null;

/**
 * Initialize the collegedb with configuration
 *
 * Sets up the global configuration for the CollegeDB system. This must be called
 * before any other operations can be performed. The configuration includes KV
 * storage, available D1 shards, optional coordinator, and allocation strategy.
 *
 * **NEW**: Automatically detects and migrates existing databases without requiring
 * additional setup. If shards contain existing data with primary keys, CollegeDB
 * will automatically create the necessary mappings for seamless operation.
 *
 * @public
 * @param config - Configuration object containing all necessary bindings and settings
 * @throws {Error} If configuration is invalid or required bindings are missing
 * @example
 * ```typescript
 * // Basic setup with multiple shards - auto-migration happens automatically
 * initialize({
 *   kv: env.KV,
 *   shards: {
 *     'db-primary': env.DB_PRIMARY,     // Existing DB with data
 *     'db-secondary': env.DB_SECONDARY  // Another existing DB
 *   },
 *   strategy: 'round-robin'
 * });
 * // Existing data is now automatically accessible via CollegeDB!
 *
 * // Advanced setup with coordinator
 * initialize({
 *   kv: env.KV,
 *   coordinator: env.ShardCoordinator,
 *   shards: {
 *     'db-east': env.DB_EAST,
 *     'db-west': env.DB_WEST,
 *     'db-central': env.DB_CENTRAL
 *   },
 *   strategy: 'hash'
 * });
 * ```
 */
export function initialize(config: CollegeDBConfig): void {
	globalConfig = config;

	// Perform automatic migration detection in the background
	// This runs asynchronously to avoid blocking initialization
	if (config.shards && Object.keys(config.shards).length > 0) {
		performBackgroundAutoMigration(config).catch((error) => {
			console.warn('Background auto-migration failed:', error);
		});
	}
}

/**
 * Performs automatic migration detection for all shards in the background
 *
 * This function runs asynchronously after initialization to check all configured
 * shards for existing data that needs migration. It's designed to be non-blocking
 * and won't interfere with immediate database operations.
 *
 * @private
 * @param config - CollegeDB configuration
 */
async function performBackgroundAutoMigration(config: CollegeDBConfig): Promise<void> {
	try {
		const { autoDetectAndMigrate } = await import('./migrations.js');
		const shardNames = Object.keys(config.shards);

		console.log(`🔍 Checking ${shardNames.length} shards for existing data...`);

		// Check each shard for migration needs
		const migrationPromises = shardNames.map(async (shardName) => {
			const database = config.shards[shardName];
			if (!database) return null;

			try {
				const result = await autoDetectAndMigrate(database, shardName, config, {
					maxRecordsToCheck: 1000
				});

				return {
					shardName,
					...result
				};
			} catch (error) {
				console.warn(`Auto-migration failed for shard ${shardName}:`, error);
				return null;
			}
		});

		const results = await Promise.all(migrationPromises);
		const successfulMigrations = results.filter((r) => r?.migrationPerformed);

		if (successfulMigrations.length > 0) {
			const totalRecords = successfulMigrations.reduce((sum, r) => sum + (r?.recordsMigrated || 0), 0);
			console.log(`🎉 Auto-migration completed! Migrated ${totalRecords} records across ${successfulMigrations.length} shards`);
			successfulMigrations.forEach((result) => {
				if (result) {
					console.log(`   ✅ ${result.shardName}: ${result.recordsMigrated} records from ${result.tablesProcessed} tables`);
				}
			});
		} else {
			console.log('✅ All shards ready - no migration needed');
		}
	} catch (error) {
		console.warn('Background auto-migration setup failed:', error);
	}
}

/**
 * Gets the global configuration, throwing an error if not initialized
 *
 * Internal utility function that retrieves the global configuration and
 * ensures the system has been properly initialized before performing
 * any operations.
 *
 * @private
 * @returns The global CollegeDB configuration
 * @throws {Error} If initialize() has not been called yet
 */
function getConfig(): CollegeDBConfig {
	if (!globalConfig) {
		throw new Error('CollegeDB not initialized. Call initialize() first.');
	}
	return globalConfig;
}

/**
 * Gets or allocates a shard for a primary key
 *
 * This is the core routing function that determines which shard should handle
 * a given primary key. If a mapping already exists, it returns the existing
 * shard. If not, it allocates a new shard using the configured strategy.
 *
 * Allocation strategies:
 * - **round-robin**: Cycles through shards in order (with coordinator)
 * - **random**: Randomly selects from available shards
 * - **hash**: Uses consistent hashing for deterministic assignment
 *
 * The function prefers using the Durable Object coordinator when available
 * for centralized allocation decisions, falling back to local strategies
 * when the coordinator is unavailable.
 *
 * @private
 * @param primaryKey - The primary key to route
 * @returns Promise resolving to the shard binding name
 * @throws {Error} If no shards are configured or allocation fails
 * @example
 * ```typescript
 * // This function is called internally by CRUD operations
 * const shard = await getShardForKey('user-123');
 * console.log(`User 123 is assigned to: ${shard}`);
 * ```
 */
async function getShardForKey(primaryKey: string): Promise<string> {
	const config = getConfig();
	const mapper = new KVShardMapper(config.kv);

	// Check if mapping already exists
	const existingMapping = await mapper.getShardMapping(primaryKey);
	if (existingMapping) {
		return existingMapping.shard;
	}

	// Before allocating a new shard, check if any existing shards contain this key
	// and perform automatic migration if needed
	const availableShards = Object.keys(config.shards);
	if (availableShards.length === 0) {
		throw new Error('No shards configured');
	}

	// Check existing shards for unmapped data containing this primary key
	for (const shardName of availableShards) {
		const database = config.shards[shardName];
		if (!database) continue;

		try {
			// Quick check if this primary key exists in any table in this shard
			const { autoDetectAndMigrate } = await import('./migrations.js');
			const migrationResult = await autoDetectAndMigrate(database, shardName, config, {
				maxRecordsToCheck: 100 // Limit check for performance
			});

			if (migrationResult.migrationPerformed) {
				// Re-check mapping after migration
				const newMapping = await mapper.getShardMapping(primaryKey);
				if (newMapping) {
					return newMapping.shard;
				}
			}
		} catch (error) {
			// Don't fail the operation if auto-migration fails
			console.warn(`Auto-migration check failed for shard ${shardName}:`, error);
		}
	}

	// If no existing mapping found after auto-migration, allocate a new shard
	let selectedShard: string;

	// Use coordinator if available for allocation
	if (config.coordinator) {
		try {
			const coordinatorId = config.coordinator.idFromName('default');
			const coordinator = config.coordinator.get(coordinatorId);

			const response = await coordinator.fetch('http://coordinator/allocate', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					primaryKey,
					strategy: config.strategy || 'round-robin'
				})
			});

			if (response.ok) {
				const result = (await response.json()) as { shard: string };
				selectedShard = result.shard;
			} else {
				// Fallback to simple round-robin
				selectedShard = availableShards[Math.floor(Math.random() * availableShards.length)]!;
			}
		} catch (error) {
			console.warn('Coordinator allocation failed, falling back to random:', error);
			selectedShard = availableShards[Math.floor(Math.random() * availableShards.length)]!;
		}
	} else {
		// Simple allocation strategy without coordinator
		const strategy = config.strategy || 'round-robin';
		switch (strategy) {
			case 'hash':
				let hash = 0;
				for (let i = 0; i < primaryKey.length; i++) {
					const char = primaryKey.charCodeAt(i);
					hash = (hash << 5) - hash + char;
					hash = hash & hash;
				}
				const index = Math.abs(hash) % availableShards.length;
				selectedShard = availableShards[index]!;
				break;
			case 'random':
				selectedShard = availableShards[Math.floor(Math.random() * availableShards.length)]!;
				break;
			default: // round-robin
				selectedShard = availableShards[0]!; // Simplified without state
				break;
		}
	}

	// Store the mapping
	await mapper.setShardMapping(primaryKey, selectedShard);
	return selectedShard;
}

/**
 * Gets the D1 database instance for a primary key
 *
 * Resolves the primary key to its assigned shard and returns the corresponding
 * D1 database instance. This function handles the complete routing process
 * from primary key to database connection.
 *
 * @private
 * @param primaryKey - The primary key to route
 * @returns Promise resolving to the D1 database instance
 * @throws {Error} If shard routing fails or database instance not found
 */
async function getDatabase(primaryKey: string): Promise<D1Database> {
	const config = getConfig();
	const shard = await getShardForKey(primaryKey);
	const database = config.shards[shard];

	if (!database) {
		throw new Error(`Shard ${shard} not found in configuration`);
	}

	return database;
}

/**
 * Creates schema in a specific D1 database
 *
 * Creates the default CollegeDB schema in the specified D1 database instance.
 * This is a convenience wrapper around the migrations module's createSchema function.
 *
 * @public
 * @param d1 - The D1 database instance to create schema in
 * @returns Promise that resolves when schema creation is complete
 * @throws {Error} If schema creation fails
 * @example
 * ```typescript
 * // Create schema on a specific shard
 * await createSchema(env.DB_NEW_SHARD);
 * ```
 */
export async function createSchema(d1: D1Database): Promise<void> {
	const { createSchema: createSchemaImpl } = await import('./migrations.js');
	await createSchemaImpl(d1);
}

/**
 * Inserts a record using the primary key for routing
 *
 * Executes an INSERT statement on the appropriate shard based on the primary key.
 * The primary key is used to determine which shard should store the record,
 * ensuring consistent routing for future queries.
 *
 * @public
 * @param primaryKey - Primary key to route the query (should match the record's primary key)
 * @param sql - SQL INSERT statement with parameter placeholders
 * @param bindings - Parameter values to bind to the SQL statement
 * @returns Promise that resolves when insert is complete
 * @throws {Error} If insert fails or routing fails
 * @example
 * ```typescript
 * // Insert a new user
 * await insert('user-123',
 *   'INSERT INTO users (id, name, email) VALUES (?, ?, ?)',
 *   ['user-123', 'John Doe', 'john@example.com']
 * );
 *
 * // Insert a post linked to a user
 * await insert('post-456',
 *   'INSERT INTO posts (id, user_id, title, content) VALUES (?, ?, ?, ?)',
 *   ['post-456', 'user-123', 'Hello World', 'My first post!']
 * );
 * ```
 */
export async function insert(primaryKey: string, sql: string, bindings: any[] = []): Promise<void> {
	const db = await getDatabase(primaryKey);
	const result = await (await db.prepare(sql)).bind(...bindings).run();

	if (!result.success) {
		throw new Error(`Insert failed: ${result.error || 'Unknown error'}`);
	}
}

/**
 * Selects records using the primary key for routing
 *
 * Executes a SELECT query on the appropriate shard based on the primary key.
 * Returns structured results with metadata about the query execution.
 *
 * @public
 * @param primaryKey - Primary key to route the query
 * @param sql - SQL SELECT statement with parameter placeholders
 * @param bindings - Parameter values to bind to the SQL statement
 * @returns Promise resolving to structured query results with metadata
 * @throws {Error} If query fails or routing fails
 * @example
 * ```typescript
 * // Get a specific user
 * const userResult = await selectByPrimaryKey('user-123',
 *   'SELECT * FROM users WHERE id = ?',
 *   ['user-123']
 * );
 *
 * if (userResult.success && userResult.results.length > 0) {
 *   const user = userResult.results[0];
 *   console.log(`Found user: ${user.name}`);
 * }
 *
 * // Get user's posts
 * const postsResult = await selectByPrimaryKey('user-123',
 *   'SELECT * FROM posts WHERE user_id = ? ORDER BY created_at DESC',
 *   ['user-123']
 * );
 *
 * console.log(`User has ${postsResult.meta.count} posts`);
 * ```
 */
export async function selectByPrimaryKey(primaryKey: string, sql: string, bindings: any[] = []): Promise<QueryResult> {
	const db = await getDatabase(primaryKey);
	const result = await (await db.prepare(sql)).bind(...bindings).all();

	return {
		success: result.success,
		meta: {
			count: result.results.length,
			duration: 0 // D1 doesn't provide timing info
		},
		results: result.results,
		error: result.success ? undefined : 'Query failed'
	};
}

/**
 * Updates records using the primary key for routing
 *
 * Executes an UPDATE statement on the appropriate shard based on the primary key.
 * The primary key ensures the update is performed on the correct shard where
 * the record is stored.
 *
 * @public
 * @param primaryKey - Primary key to route the query
 * @param sql - SQL UPDATE statement with parameter placeholders
 * @param bindings - Parameter values to bind to the SQL statement
 * @returns Promise that resolves when update is complete
 * @throws {Error} If update fails or routing fails
 * @example
 * ```typescript
 * // Update user information
 * await updateByPrimaryKey('user-123',
 *   'UPDATE users SET name = ?, email = ? WHERE id = ?',
 *   ['John Smith', 'johnsmith@example.com', 'user-123']
 * );
 *
 * // Update post content
 * await updateByPrimaryKey('post-456',
 *   'UPDATE posts SET title = ?, content = ?, updated_at = strftime("%s", "now") WHERE id = ?',
 *   ['Updated Title', 'Updated content here', 'post-456']
 * );
 * ```
 */
export async function updateByPrimaryKey(primaryKey: string, sql: string, bindings: any[] = []): Promise<void> {
	const db = await getDatabase(primaryKey);
	const result = await (await db.prepare(sql)).bind(...bindings).run();

	if (!result.success) {
		throw new Error(`Update failed: ${result.error || 'Unknown error'}`);
	}
}

/**
 * Deletes records using the primary key for routing
 *
 * Executes a DELETE statement on the appropriate shard based on the primary key.
 * The primary key ensures the deletion is performed on the correct shard where
 * the record is stored.
 *
 * @public
 * @param primaryKey - Primary key to route the query
 * @param sql - SQL DELETE statement with parameter placeholders
 * @param bindings - Parameter values to bind to the SQL statement
 * @returns Promise that resolves when deletion is complete
 * @throws {Error} If delete fails or routing fails
 * @example
 * ```typescript
 * // Delete a specific user
 * await deleteByPrimaryKey('user-123',
 *   'DELETE FROM users WHERE id = ?',
 *   ['user-123']
 * );
 *
 * // Delete user's posts (cascade delete)
 * await deleteByPrimaryKey('user-123',
 *   'DELETE FROM posts WHERE user_id = ?',
 *   ['user-123']
 * );
 *
 * // Delete with conditions
 * await deleteByPrimaryKey('user-123',
 *   'DELETE FROM posts WHERE user_id = ? AND created_at < ?',
 *   ['user-123', Date.now() - 86400000] // Posts older than 1 day
 * );
 * ```
 */
export async function deleteByPrimaryKey(primaryKey: string, sql: string, bindings: any[] = []): Promise<void> {
	const db = await getDatabase(primaryKey);
	const result = await (await db.prepare(sql)).bind(...bindings).run();

	if (!result.success) {
		throw new Error(`Delete failed: ${result.error || 'Unknown error'}`);
	}
}

/**
 * Reassigns a primary key to a different shard
 *
 * Moves a primary key and its associated data from one shard to another. This
 * operation is useful for load balancing, shard maintenance, or geographic
 * redistribution of data.
 *
 * The reassignment process:
 * 1. Validates the target shard exists in configuration
 * 2. Checks that a mapping exists for the primary key
 * 3. If target shard differs from current, migrates the data
 * 4. Updates the KV mapping to point to the new shard
 *
 * ⚠️ **Note**: This operation involves data migration and should be used
 * carefully in production environments. Consider the impact on ongoing queries.
 *
 * @public
 * @param primaryKey - Primary key to reassign to a different shard
 * @param newBinding - New shard binding name where the data should be moved
 * @returns Promise that resolves when reassignment and migration are complete
 * @throws {Error} If target shard not found, mapping doesn't exist, or migration fails
 * @example
 * ```typescript
 * // Move a user from east to west coast for better latency
 * try {
 *   await reassignShard('user-california-123', 'db-west');
 *   console.log('User successfully moved to west coast shard');
 * } catch (error) {
 *   console.error('Reassignment failed:', error.message);
 * }
 *
 * // Load balancing: move high-activity user to dedicated shard
 * await reassignShard('user-enterprise-456', 'db-dedicated');
 * ```
 */
export async function reassignShard(primaryKey: string, newBinding: string): Promise<void> {
	const config = getConfig();

	if (!config.shards[newBinding]) {
		throw new Error(`Shard ${newBinding} not found in configuration`);
	}

	const mapper = new KVShardMapper(config.kv);
	const currentMapping = await mapper.getShardMapping(primaryKey);

	if (!currentMapping) {
		throw new Error(`No existing mapping found for primary key: ${primaryKey}`);
	}

	// Migrate data if different shard
	if (currentMapping.shard !== newBinding) {
		const { migrateRecord } = await import('./migrations.js');
		const sourceDb = config.shards[currentMapping.shard];
		const targetDb = config.shards[newBinding];

		if (!sourceDb || !targetDb) {
			throw new Error('Source or target shard not available');
		}

		await migrateRecord(sourceDb, targetDb, primaryKey);
	}

	// Update mapping
	await mapper.updateShardMapping(primaryKey, newBinding);
}

/**
 * Lists all known shards
 *
 * Returns an array of all shard binding names known to the system. First
 * attempts to get the list from the Durable Object coordinator for the most
 * up-to-date information, then falls back to the configured shards if the
 * coordinator is unavailable.
 *
 * @public
 * @returns Promise resolving to array of shard binding names
 * @example
 * ```typescript
 * const shards = await listKnownShards();
 * console.log('Available shards:', shards);
 * // Output: ['db-east', 'db-west', 'db-central']
 *
 * // Check if a specific shard is available
 * if (shards.includes('db-asia')) {
 *   console.log('Asia region shard is available');
 * }
 * ```
 */
export async function listKnownShards(): Promise<string[]> {
	const config = getConfig();

	// Try to get from coordinator first
	if (config.coordinator) {
		try {
			const coordinatorId = config.coordinator.idFromName('default');
			const coordinator = config.coordinator.get(coordinatorId);

			const response = await coordinator.fetch('http://coordinator/shards');
			if (response.ok) {
				return await response.json();
			}
		} catch (error) {
			console.warn('Failed to get shards from coordinator:', error);
		}
	}

	// Fallback to configured shards
	return Object.keys(config.shards);
}

/**
 * Gets statistics for all shards
 *
 * Returns usage statistics for all known shards, including key counts and
 * last updated timestamps. First attempts to get real-time statistics from
 * the Durable Object coordinator, then falls back to KV-based counting.
 *
 * This information is useful for:
 * - Load balancing decisions
 * - Monitoring shard utilization
 * - Capacity planning
 * - Performance analysis
 *
 * @public
 * @returns Promise resolving to array of shard statistics
 * @example
 * ```typescript
 * const stats = await getShardStats();
 * stats.forEach(shard => {
 *   console.log(`${shard.binding}: ${shard.count} keys`);
 *   if (shard.lastUpdated) {
 *     console.log(`  Last updated: ${new Date(shard.lastUpdated)}`);
 *   }
 * });
 *
 * // Find most loaded shard
 * const mostLoaded = stats.reduce((prev, current) =>
 *   (prev.count > current.count) ? prev : current
 * );
 * console.log(`Most loaded shard: ${mostLoaded.binding} (${mostLoaded.count} keys)`);
 * ```
 */
export async function getShardStats(): Promise<ShardStats[]> {
	const config = getConfig();

	// Try to get from coordinator first
	if (config.coordinator) {
		try {
			const coordinatorId = config.coordinator.idFromName('default');
			const coordinator = config.coordinator.get(coordinatorId);

			const response = await coordinator.fetch('http://coordinator/stats');
			if (response.ok) {
				return await response.json();
			}
		} catch (error) {
			console.warn('Failed to get stats from coordinator:', error);
		}
	}

	// Fallback to KV-based counting
	const mapper = new KVShardMapper(config.kv);
	const counts = await mapper.getShardKeyCounts();

	return Object.entries(config.shards).map(([binding, _]) => ({
		binding,
		count: counts[binding] || 0
	}));
}

/**
 * Executes a custom query on a specific shard
 *
 * Bypasses the normal routing logic to execute a query directly on a specified
 * shard. This is useful for administrative operations, cross-shard queries,
 * or when you need to query data that doesn't follow the primary key routing pattern.
 *
 * ⚠️ **Use with caution**: This function bypasses routing safeguards and should
 * be used only when you specifically need to target a particular shard.
 *
 * @public
 * @param shardBinding - The shard binding name to execute the query on
 * @param sql - SQL statement to execute
 * @param bindings - Parameter values to bind to the SQL statement
 * @returns Promise resolving to structured query results
 * @throws {Error} If shard not found or query fails
 * @example
 * ```typescript
 * // Administrative query: count all users across a specific shard
 * const eastCoastStats = await queryOnShard('db-east',
 *   'SELECT COUNT(*) as user_count FROM users'
 * );
 * console.log(`East coast users: ${eastCoastStats.results[0].user_count}`);
 *
 * // Cross-shard analytics: get recent posts from a specific region
 * const recentPosts = await queryOnShard('db-west',
 *   'SELECT id, title, created_at FROM posts WHERE created_at > ? ORDER BY created_at DESC LIMIT ?',
 *   [Date.now() - 86400000, 10] // Last 24 hours, limit 10
 * );
 *
 * // Schema inspection on specific shard
 * const tables = await queryOnShard('db-central',
 *   "SELECT name FROM sqlite_master WHERE type='table'"
 * );
 * ```
 */
export async function queryOnShard(shardBinding: string, sql: string, bindings: any[] = []): Promise<QueryResult> {
	const config = getConfig();
	const db = config.shards[shardBinding];

	if (!db) {
		throw new Error(`Shard ${shardBinding} not found`);
	}

	const result = await (await db.prepare(sql)).bind(...bindings).all();

	return {
		success: result.success,
		meta: {
			count: result.results.length,
			duration: 0
		},
		results: result.results,
		error: result.success ? undefined : 'Query failed'
	};
}

/**
 * Flushes all shard mappings (development only)
 *
 * Completely clears all primary key to shard mappings from both KV storage
 * and the Durable Object coordinator. This operation resets the entire
 * routing system to a clean state.
 *
 * ⚠️ **DANGER**: This operation is destructive and irreversible. After flushing,
 * all existing primary keys will be treated as new and may be assigned to
 * different shards than before, causing data routing issues.
 *
 * **Use only for**:
 * - Development and testing environments
 * - Complete system resets
 * - Emergency recovery scenarios
 *
 * @public
 * @returns Promise that resolves when all mappings are cleared
 * @example
 * ```typescript
 * // Only use in development!
 * if (process.env.NODE_ENV === 'development') {
 *   await flush();
 *   console.log('All shard mappings cleared for testing');
 *
 *   // Now all keys will be reassigned on next access
 *   await insert('user-123', 'INSERT INTO users (id, name) VALUES (?, ?)',
 *     ['user-123', 'Test User']);
 * }
 * ```
 */
export async function flush(): Promise<void> {
	const config = getConfig();
	const mapper = new KVShardMapper(config.kv);

	await mapper.clearAllMappings();

	// Also flush coordinator if available
	if (config.coordinator) {
		try {
			const coordinatorId = config.coordinator.idFromName('default');
			const coordinator = config.coordinator.get(coordinatorId);

			await coordinator.fetch('http://coordinator/flush', { method: 'POST' });
		} catch (error) {
			console.warn('Failed to flush coordinator:', error);
		}
	}
}
