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
 * import { initialize, insert, first, run } from 'collegedb';
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
 * await run('user-123', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-123', 'John']);
 *
 * // Query the record (routed to same shard)
 * const result = await first('user-123', 'SELECT * FROM users WHERE id = ?', ['user-123']);
 * ```
 *
 * @author CollegeDB Team
 * @since 1.0.0
 */

import type { Request } from '@cloudflare/workers-types';
import { CollegeDBError } from './errors';
import { KVShardMapper } from './kvmap';
import type {
	CollegeDBConfig,
	D1Region,
	OperationType,
	PreparedStatement,
	QueryResult,
	SQLDatabase,
	ShardLocation,
	ShardStats,
	ShardingStrategy
} from './types';

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
 * Shared mapper instance for the active configuration.
 *
 * Reusing a single mapper preserves in-memory caches and avoids repeated
 * constructor/setup overhead on each operation.
 *
 * @private
 */
let globalMapper: KVShardMapper | null = null;

/**
 * In-memory cache for per-shard size checks.
 * @private
 */
const shardSizeCache = new Map<string, { size: number; expiresAt: number }>();

/**
 * Gets the shared mapper for the active configuration.
 * @private
 */
function getMapper(config: CollegeDBConfig): KVShardMapper {
	if (!globalMapper) {
		globalMapper = new KVShardMapper(config.kv, {
			hashShardMappings: config.hashShardMappings,
			mappingCacheTtlMs: config.mappingCacheTtlMs,
			knownShardsCacheTtlMs: config.knownShardsCacheTtlMs
		});
	}

	return globalMapper;
}

/**
 * Sets up the global configuration for the CollegeDB system. This must be called
 * before any other operations can be performed. The configuration includes KV
 * storage, available D1 shards, optional coordinator, and allocation strategy.
 *
 * This will also automatically detect and migrate existing databases without requiring
 * additional setup. If shards contain existing data with primary keys, CollegeDB
 * will automatically create the necessary mappings for seamless operation.
 *
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
export function initialize(config: CollegeDBConfig) {
	globalConfig = config;
	globalMapper = new KVShardMapper(config.kv, {
		hashShardMappings: config.hashShardMappings,
		mappingCacheTtlMs: config.mappingCacheTtlMs,
		knownShardsCacheTtlMs: config.knownShardsCacheTtlMs
	});
	shardSizeCache.clear();

	// Background: sync KV known shards with configured shards
	try {
		const mapper = getMapper(config);
		Promise.resolve()
			.then(async () => {
				const existing = await mapper.getKnownShards();
				const merged = Array.from(new Set([...existing, ...Object.keys(config.shards)]));
				await mapper.setKnownShards(merged);
			})
			.catch(() => void 0);
	} catch {}

	if (config.shards && Object.keys(config.shards).length > 0 && !config.disableAutoMigration) {
		performAutoMigration(config).catch((error) => {
			console.warn('Background auto-migration failed:', error);
		});
	}
}

/**
 * Sets up the global configuration for the CollegeDB system asynchronously.
 * This must be called before any other operations can be performed. The
 * configuration includes KVstorage, available D1 shards, optional coordinator,
 * and allocation strategy.
 *
 * This will also automatically detect and migrate existing databases without requiring
 * additional setup. If shards contain existing data with primary keys, CollegeDB
 * will automatically create the necessary mappings for seamless operation.
 *
 * Compared to `initialize`, this method waits for the background check to finish.
 *
 * @param config - Configuration object containing all necessary bindings and settings
 * @throws {Error} If configuration is invalid or required bindings are missing
 * @example
 * ```typescript
 * // Basic setup with multiple shards - auto-migration happens automatically
 * initializeAsync({
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
 * initializeAsync({
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
export async function initializeAsync(config: CollegeDBConfig) {
	globalConfig = config;
	globalMapper = new KVShardMapper(config.kv, {
		hashShardMappings: config.hashShardMappings,
		mappingCacheTtlMs: config.mappingCacheTtlMs,
		knownShardsCacheTtlMs: config.knownShardsCacheTtlMs
	});
	shardSizeCache.clear();

	// Sync KV known shards with configured shards (awaited in async init)
	try {
		const mapper = getMapper(config);
		const existing = await mapper.getKnownShards();
		const merged = Array.from(new Set([...existing, ...Object.keys(config.shards)]));
		await mapper.setKnownShards(merged);
	} catch {}

	if (config.shards && Object.keys(config.shards).length > 0 && !config.disableAutoMigration)
		try {
			await performAutoMigration(config);
		} catch (error) {
			console.warn('Auto migration failed:', error);
		}
}

/**
 * Initializes the configuration and then performs a callback once the configuration
 * has finished initializing.
 *
 * @param config - CollegeDB Configuration
 * @param callback - The callback to perform after the initialization
 * @returns The result of the callback
 * @example
 * ```
 * import { collegedb, first } from 'collegedb'
 *
 * const result = collegedb({
 *   kv: env.KV,
 *   shards: {
 *     'db-primary': env.DB_PRIMARY,     // Existing DB with data
 *     'db-secondary': env.DB_SECONDARY  // Another existing DB
 *   },
 *   strategy: 'hash'
 * }, async () => {
 *     return await first('user-123', 'SELECT * FROM users WHERE id = ?', ['user-123']);
 * });
 * ```
 */
export async function collegedb<T>(config: CollegeDBConfig, callback: () => T) {
	await initializeAsync(config);
	return await callback();
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
async function performAutoMigration(config: CollegeDBConfig): Promise<void> {
	try {
		const { autoDetectAndMigrate } = await import('./migrations');
		const shardNames = Object.keys(config.shards);

		if (config.debug) {
			console.log(`🔍 Checking ${shardNames.length} shards for existing data...`);
		}

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

		if (config.debug) {
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
		}
	} catch (error) {
		console.warn('Background auto-migration setup failed:', error);
	}
}

/**
 * Resets the global configuration (for testing purposes only)
 *
 * @private
 * @internal
 */
export function resetConfig(): void {
	globalConfig = null;
	globalMapper = null;
	shardSizeCache.clear();
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
		throw new CollegeDBError('CollegeDB not initialized. Call initialize() first.', 'NOT_INITIALIZED');
	}
	return globalConfig;
}

/**
 * Determines the operation type from a SQL statement
 * @private
 * @param sql - The SQL statement to analyze
 * @returns The operation type ('read' for SELECT, 'write' for INSERT/UPDATE/DELETE)
 */
function getOperationType(sql: string): OperationType {
	const sql0 = sql.trim().toUpperCase();

	if (
		sql0.startsWith('SELECT') ||
		sql0.startsWith('VALUES') ||
		sql0.startsWith('TABLE') ||
		sql0.startsWith('PRAGMA') ||
		sql0.startsWith('EXPLAIN') ||
		sql0.startsWith('WITH') ||
		sql0.startsWith('SHOW')
	) {
		return 'read';
	}

	// All other operations (INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, etc.) are considered writes
	return 'write';
}

/**
 * Resolves the effective sharding strategy based on configuration and operation type
 * @private
 * @param config - CollegeDB configuration
 * @param type - The type of operation being performed
 * @returns The effective sharding strategy to use
 */
function resolveStrategy(config: CollegeDBConfig, type: OperationType): ShardingStrategy {
	const strategy = config.strategy || 'hash';

	if (typeof strategy === 'string') {
		return strategy;
	}

	// Fallbacks for partially specified mixed strategies
	const mixed = strategy as Partial<Record<OperationType, ShardingStrategy>>;
	return (mixed[type] || mixed.write || mixed.read || 'hash') as ShardingStrategy;
}

/**
 * Calculates the relative distance between two D1 regions for location-based sharding.
 * Lower values indicate closer regions with better expected latency.
 *
 * @private
 * @param from - Source region
 * @param to - Target region
 * @returns Relative distance score (lower is better)
 */
function calculateRegionDistance(from: D1Region, to: D1Region): number {
	// Same region = optimal
	if (from === to) return 0;

	// Define region coordinates (approximate)
	const regionCoords: Record<D1Region, { lat: number; lon: number }> = {
		wnam: { lat: 37.7749, lon: -122.4194 }, // San Francisco
		enam: { lat: 40.7128, lon: -74.006 }, // New York
		weur: { lat: 51.5074, lon: -0.1278 }, // London
		eeur: { lat: 52.52, lon: 13.405 }, // Berlin
		apac: { lat: 35.6762, lon: 139.6503 }, // Tokyo
		oc: { lat: -33.8688, lon: 151.2093 }, // Sydney
		me: { lat: 25.2048, lon: 55.2708 }, // Dubai
		af: { lat: -26.2041, lon: 28.0473 } // Johannesburg
	};

	const fromCoord = regionCoords[from];
	const toCoord = regionCoords[to];

	// Simple Euclidean distance calculation
	const latDiff = fromCoord.lat - toCoord.lat;
	const lonDiff = fromCoord.lon - toCoord.lon;
	return Math.sqrt(latDiff * latDiff + lonDiff * lonDiff);
}

/**
 * Determines the closest D1 region based on an IP address.
 * Uses IP geolocation to estimate the user's location and find the nearest D1 region.
 *
 * This function uses Cloudflare's CF object which provides geolocation data
 * in Cloudflare Workers environment. Falls back to 'wnam' if geolocation fails.
 *
 * @param request - The incoming Request object (contains CF geolocation data in Cloudflare Workers)
 * @returns The closest D1Region based on IP geolocation
 * @example
 * ```typescript
 * // In a Cloudflare Worker
 * export default {
 *   async fetch(request: Request, env: Env) {
 *     const userRegion = getClosestRegionFromIP(request);
 *
 *     initialize({
 *       kv: env.KV,
 *       strategy: 'location',
 *       targetRegion: userRegion, // Automatically optimized for user location
 *       shardLocations: { ... },
 *       shards: { ... }
 *     });
 *   }
 * };
 * ```
 */
export function getClosestRegionFromIP(request: Request): D1Region {
	const cf = request.cf;

	if (!cf || !cf.country) {
		return 'wnam';
	}

	const country = cf.country as string;
	const continent = cf.continent as string;

	// Western North America
	if (['US', 'CA', 'MX'].includes(country)) {
		// Further refine by region/state if available
		const region = (cf.region || cf.regionCode || '') as string;
		const timezone = (cf.timezone || '') as string;

		// West Coast indicators
		if (
			region.includes('CA') ||
			region.includes('WA') ||
			region.includes('OR') ||
			region.includes('NV') ||
			region.includes('AZ') ||
			region.includes('UT') ||
			timezone.includes('Pacific') ||
			timezone.includes('America/Los_Angeles')
		) {
			return 'wnam';
		}

		// East Coast and Central - default to Eastern North America
		return 'enam';
	}

	// Eastern North America (broader North America)
	if (['GL', 'PM', 'BM'].includes(country)) {
		return 'enam';
	}

	// Western Europe
	if (['GB', 'IE', 'FR', 'ES', 'PT', 'NL', 'BE', 'LU', 'CH', 'AT', 'IT'].includes(country)) {
		return 'weur';
	}

	// Eastern Europe
	if (
		[
			'DE',
			'PL',
			'CZ',
			'SK',
			'HU',
			'SI',
			'HR',
			'BA',
			'RS',
			'ME',
			'MK',
			'AL',
			'BG',
			'RO',
			'MD',
			'UA',
			'BY',
			'LT',
			'LV',
			'EE',
			'FI',
			'SE',
			'NO',
			'DK',
			'IS'
		].includes(country)
	) {
		return 'eeur';
	}

	// Russia - closer to Eastern Europe for most population centers
	if (country === 'RU') {
		return 'eeur';
	}

	// Asia Pacific
	if (['JP', 'KR', 'CN', 'HK', 'TW', 'MO', 'MN', 'KP'].includes(country)) {
		return 'apac';
	}

	// Southeast Asia and South Asia -> APAC
	if (
		['TH', 'VN', 'SG', 'MY', 'ID', 'PH', 'BN', 'KH', 'LA', 'MM', 'TL', 'IN', 'PK', 'BD', 'LK', 'NP', 'BT', 'MV', 'AF'].includes(country)
	) {
		return 'apac';
	}

	// Oceania
	if (['AU', 'NZ', 'PG', 'FJ', 'NC', 'VU', 'SB', 'WS', 'TO', 'KI', 'NR', 'PW', 'FM', 'MH', 'TV'].includes(country)) {
		return 'oc';
	}

	// Middle East
	if (['AE', 'SA', 'QA', 'KW', 'BH', 'OM', 'YE', 'IQ', 'IR', 'SY', 'LB', 'JO', 'IL', 'PS', 'TR', 'CY'].includes(country)) {
		return 'me';
	}

	// Africa
	if (continent === 'AF' || ['EG', 'LY', 'TN', 'DZ', 'MA', 'SD', 'SS', 'ET', 'ER', 'DJ', 'SO'].includes(country)) {
		return 'af';
	}

	// Central Asia -> closer to Eastern Europe
	if (['KZ', 'UZ', 'TM', 'TJ', 'KG'].includes(country)) {
		return 'eeur';
	}

	// South America -> geographically closer to Eastern North America
	if (continent === 'SA' || ['BR', 'AR', 'CL', 'PE', 'CO', 'VE', 'EC', 'BO', 'PY', 'UY', 'GY', 'SR', 'GF'].includes(country)) {
		return 'enam';
	}

	// Central America and Caribbean -> Eastern North America
	if (
		['GT', 'BZ', 'SV', 'HN', 'NI', 'CR', 'PA', 'CU', 'JM', 'HT', 'DO', 'PR', 'TT', 'BB', 'GD', 'VC', 'LC', 'DM', 'AG', 'KN'].includes(
			country
		)
	) {
		return 'enam';
	}

	// Default fallback - Western North America (major Cloudflare hub)
	return 'wnam';
}

function parseRegion(location: ShardLocation | D1Region): D1Region {
	if (typeof location === 'string') {
		return location;
	}

	return location.region || 'wnam';
}

/**
 * Gets the approximate size of a D1 database in bytes using an efficient SQL query.
 * Uses SQLite's page_count and page_size pragmas for accurate size calculation.
 *
 * @private
 * @param database - The SQL database instance to measure
 * @returns Promise resolving to the database size in bytes
 * @throws {CollegeDBError} If the size query fails
 */
async function getDatabaseSize(database: SQLDatabase): Promise<number> {
	try {
		// Get page count and page size efficiently
		const [pageCountResult, pageSizeResult] = await Promise.all([
			database.prepare('PRAGMA page_count').first<{ page_count: number }>(),
			database.prepare('PRAGMA page_size').first<{ page_size: number }>()
		]);

		if (!pageCountResult?.page_count || !pageSizeResult?.page_size) {
			throw new CollegeDBError('Failed to retrieve database size information', 'SIZE_QUERY_FAILED');
		}

		return pageCountResult.page_count * pageSizeResult.page_size;
	} catch (error) {
		throw new CollegeDBError(
			`Failed to get database size: ${error instanceof Error ? error.message : 'Unknown error'}`,
			'SIZE_QUERY_FAILED'
		);
	}
}

/**
 * Retrieves a shard size using a short-lived in-memory cache.
 * @private
 */
async function getDatabaseSizeForAllocation(shardName: string, config: CollegeDBConfig): Promise<number> {
	const cacheTtlMs = Math.max(0, config.sizeCacheTtlMs ?? 30_000);
	const cached = shardSizeCache.get(shardName);

	if (cached && cached.expiresAt >= Date.now()) {
		return cached.size;
	}

	const database = config.shards[shardName];
	if (!database) {
		throw new CollegeDBError(`Shard ${shardName} not found in configuration`, 'SHARD_NOT_FOUND');
	}

	const size = await getDatabaseSize(database);
	if (cacheTtlMs > 0) {
		shardSizeCache.set(shardName, {
			size,
			expiresAt: Date.now() + cacheTtlMs
		});
	}

	return size;
}

/**
 * Filters available shards to exclude those that exceed the configured maximum size.
 * This is used during shard allocation to prevent new data from being assigned to
 * shards that are approaching or exceeding their size limits.
 *
 * @private
 * @param availableShards - List of shard names to filter
 * @param config - CollegeDB configuration containing maxDatabaseSize setting
 * @returns Promise resolving to filtered list of shard names
 */
async function filterShardsBySize(availableShards: string[], config: CollegeDBConfig): Promise<string[]> {
	if (typeof config.maxDatabaseSize !== 'number' || !Number.isFinite(config.maxDatabaseSize) || config.maxDatabaseSize <= 0) {
		return availableShards;
	}

	const limit = config.maxDatabaseSize;

	const sizeChecks = await Promise.allSettled(
		availableShards.map(async (shardName) => {
			const size = await getDatabaseSizeForAllocation(shardName, config);
			return {
				shard: shardName,
				size,
				withinLimit: size < limit
			};
		})
	);

	const validShards = sizeChecks
		.filter(
			(result): result is PromiseFulfilledResult<{ shard: string; size: number; withinLimit: boolean }> =>
				result.status === 'fulfilled' && result.value.withinLimit
		)
		.map((result) => result.value.shard);

	// If all shards exceed the size limit, log warning and return all shards
	// to prevent complete failure (existing mappings should still work)
	if (validShards.length === 0) {
		if (config.debug) {
			console.warn('All shards exceed maxDatabaseSize limit. Allowing allocation to prevent failure.');
		}
		return availableShards;
	}

	if (config.debug && validShards.length < availableShards.length) {
		const excludedShards = availableShards.filter((shard) => !validShards.includes(shard));
		console.log(`Excluded ${excludedShards.length} shards due to size limits: ${excludedShards.join(', ')}`);
	}

	return validShards;
}

/**
 * Selects the optimal shard for location-based allocation strategy.
 * Prioritizes shards in the target region, then nearby regions by distance.
 *
 * @private
 * @param targetRegion - The preferred region for allocation
 * @param availableShards - List of available shard names
 * @param shardLocations - Geographic locations of each shard
 * @param primaryKey - The primary key being allocated (for consistent tiebreaking)
 * @returns Selected shard name
 */
function selectShardByLocation(
	targetRegion: D1Region,
	availableShards: string[],
	shardLocations: Record<string, ShardLocation | D1Region>,
	primaryKey: string
): string {
	// Filter shards that have location information
	const locatedShards = availableShards.filter((shard) => shardLocations[shard]);

	if (locatedShards.length === 0) {
		// Fallback to hash if no location info available
		let hash = 0;
		for (let i = 0; i < primaryKey.length; i++) {
			const char = primaryKey.charCodeAt(i);
			hash = (hash << 5) - hash + char;
			hash = hash & hash;
		}
		const index = Math.abs(hash) % availableShards.length;
		return availableShards[index]!;
	}

	// Calculate distances and priorities
	const shardScores = locatedShards.map((shard) => {
		const location = shardLocations[shard]!;
		const distance = calculateRegionDistance(targetRegion, parseRegion(location));

		const priority = typeof location === 'object' ? location.priority || 1 : 1;
		const score = distance - priority * 0.1;

		return { shard, score, distance, priority };
	});

	// Sort by score (lower is better)
	shardScores.sort((a, b) => a.score - b.score);

	const bestScore = shardScores[0]!.score;
	const bestShards = shardScores.filter((s) => Math.abs(s.score - bestScore) < 0.01);

	if (bestShards.length === 1) {
		return bestShards[0]!.shard;
	}

	// Consistent selection among best candidates
	let hash = 0;
	for (let i = 0; i < primaryKey.length; i++) {
		const char = primaryKey.charCodeAt(i);
		hash = (hash << 5) - hash + char;
		hash = hash & hash;
	}
	const index = Math.abs(hash) % bestShards.length;
	return bestShards[index]!.shard;
}

/**
 * Helper to select a shard locally based on the effective strategy when the coordinator
 * is unavailable or not configured. Provides sensible fallbacks to avoid hotspotting.
 * @private
 */
function selectShardByStrategy(
	effectiveStrategy: ShardingStrategy,
	primaryKey: string,
	availableShards: string[],
	config: CollegeDBConfig
): string {
	switch (effectiveStrategy) {
		case 'hash': {
			let hash = 0;
			for (let i = 0; i < primaryKey.length; i++) {
				const char = primaryKey.charCodeAt(i);
				hash = (hash << 5) - hash + char;
				hash = hash & hash;
			}
			const index = Math.abs(hash) % availableShards.length;
			return availableShards[index] || availableShards[0]!;
		}
		case 'location': {
			if (!config.targetRegion) {
				return selectShardByStrategy('hash', primaryKey, availableShards, config);
			}
			return selectShardByLocation(config.targetRegion, availableShards, config.shardLocations || {}, primaryKey);
		}
		case 'random': {
			return availableShards[Math.floor(Math.random() * availableShards.length)] || availableShards[0]!;
		}
		default: {
			return selectShardByStrategy('hash', primaryKey, availableShards, config);
		}
	}
}

/**
 * Gets or allocates a shard for a primary key with operation-specific strategy
 *
 * This is the core routing function that determines which shard should handle
 * a given primary key. If a mapping already exists, it returns the existing
 * shard. If not, it allocates a new shard using the configured strategy.
 *
 * Allocation strategies:
 * - **round-robin**: Cycles through shards in order (with coordinator)
 * - **random**: Randomly selects from available shards
 * - **hash**: Uses consistent hashing for deterministic assignment
 * - **location**: Selects shards based on geographic proximity to target region
 *
 * The function prefers using the Durable Object coordinator when available
 * for centralized allocation decisions, falling back to local strategies
 * when the coordinator is unavailable.
 *
 * @private
 * @param primaryKey - The primary key to route
 * @param operationType - The type of operation (read/write) for mixed strategy support
 * @returns Promise resolving to the shard binding name
 * @throws {Error} If no shards are configured or allocation fails
 * @example
 * ```typescript
 * // This function is called internally by CRUD operations
 * const readShard = await getShardForKey('user-123', 'read');
 * const writeShard = await getShardForKey('user-123', 'write');
 * console.log(`User 123 reads from: ${readShard}, writes to: ${writeShard}`);
 * ```
 */
async function getShardForKey(primaryKey: string, operationType: OperationType = 'write'): Promise<string> {
	const config = getConfig();
	const mapper = getMapper(config);

	// Check if mapping already exists
	const existingMapping = await mapper.getShardMapping(primaryKey);
	if (existingMapping) {
		return existingMapping.shard;
	}

	// Before allocating a new shard, check if any existing shards contain this key
	const availableShards = Object.keys(config.shards);
	if (availableShards.length === 0) {
		throw new CollegeDBError('No shards configured', 'NO_SHARDS');
	}

	// Filter shards by size limit if configured
	const eligibleShards = await filterShardsBySize(availableShards, config);

	// If no existing mapping found after auto-migration, allocate a new shard
	let selectedShard: string;
	const effectiveStrategy = resolveStrategy(config, operationType);

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
					strategy: effectiveStrategy, // Use resolved strategy instead of config.strategy
					operationType, // Pass operation type for coordinator awareness
					targetRegion: config.targetRegion,
					shardLocations: config.shardLocations,
					availableShards: eligibleShards // Pass filtered shards to coordinator
				})
			});

			if (response.ok) {
				const result = (await response.json()) as { shard: string };
				selectedShard = result.shard;
			} else {
				selectedShard = selectShardByStrategy(effectiveStrategy, primaryKey, eligibleShards, config);
			}
		} catch (error) {
			console.warn('Coordinator allocation failed, falling back to local strategy:', error);
			selectedShard = selectShardByStrategy(effectiveStrategy, primaryKey, eligibleShards, config);
		}
	} else {
		selectedShard = selectShardByStrategy(effectiveStrategy, primaryKey, eligibleShards, config);
	}

	// Store the mapping
	await mapper.setShardMapping(primaryKey, selectedShard);
	return selectedShard;
}

/**
 * Gets the D1 database instance for a primary key with operation-specific routing
 *
 * Resolves the primary key to its assigned shard and returns the corresponding
 * D1 database instance. This function handles the complete routing process
 * from primary key to database connection, with support for different strategies
 * based on operation type.
 *
 * @private
 * @param primaryKey - The primary key to route
 * @param operationType - The type of operation (read/write) for mixed strategy support
 * @returns Promise resolving to the D1 database instance
 * @throws {Error} If shard routing fails or database instance not found
 */
async function getDatabase(primaryKey: string, operationType: OperationType = 'write'): Promise<SQLDatabase> {
	const config = getConfig();
	const shard = await getShardForKey(primaryKey, operationType);
	const database = config.shards[shard];

	if (!database) {
		throw new CollegeDBError(`Shard ${shard} not found in configuration`, 'SHARD_NOT_FOUND');
	}

	return database;
}

/**
 * Creates the database schema in the specified D1 database
 *
 * @param d1 - The D1 database instance to create schema in
 * @param schema - The SQL schema definition to execute
 * @returns Promise that resolves when schema creation is complete
 * @throws {Error} If schema creation fails
 * @example
 * ```typescript
 * const userSchema = `
 *   CREATE TABLE users (
 *     id TEXT PRIMARY KEY,
 *     name TEXT NOT NULL,
 *     email TEXT UNIQUE
 *   );
 * `;
 * await createSchema(env.DB_NEW_SHARD, userSchema);
 * ```
 */
export async function createSchema(d1: SQLDatabase, schema: string): Promise<void> {
	const { createSchema: createSchemaImpl } = await import('./migrations');
	await createSchemaImpl(d1, schema);
}

/**
 * Prepares a SQL statement for execution with operation-aware routing.
 *
 * @param key - The primary key to route the query
 * @param sql - The SQL statement to prepare
 * @returns Promise that resolves to a prepared statement
 * @throws {Error} If preparation fails
 */
export async function prepare(key: string, sql: string): Promise<PreparedStatement> {
	const operationType = getOperationType(sql);
	const db = await getDatabase(key, operationType);
	const result = db.prepare(sql);
	return result;
}

/**
 * Executes a statement on the appropriate shard based on the primary key.
 * The primary key is used to determine which shard should store the record,
 * ensuring consistent routing for future queries.
 *
 * @template T - Type of the result records
 * @param key - Primary key to route the query (should match the record's primary key)
 * @param sql - SQL statement with parameter placeholders
 * @param bindings - Parameter values to bind to the SQL statement
 * @returns Promise that resolves when the statement is complete
 * @throws {Error} If statement fails or routing fails
 * @example
 * ```typescript
 * // Insert a new user
 * await run('user-123',
 *   'INSERT INTO users (id, name, email) VALUES (?, ?, ?)',
 *   ['user-123', 'John Doe', 'john@example.com']
 * );
 *
 * // Insert a post linked to a user
 * await run('post-456',
 *   'INSERT INTO posts (id, user_id, title, content) VALUES (?, ?, ?, ?)',
 *   ['post-456', 'user-123', 'Hello World', 'My first post!']
 * );
 * ```
 *
 * @example
 * ```typescript
 * // Update user information
 * await run('user-123',
 *   'UPDATE users SET name = ?, email = ? WHERE id = ?',
 *   ['John Smith', 'johnsmith@example.com', 'user-123']
 * );
 *
 * // Update post content
 * await run('post-456',
 *   'UPDATE posts SET title = ?, content = ?, updated_at = strftime("%s", "now") WHERE id = ?',
 *   ['Updated Title', 'Updated content here', 'post-456']
 * );
 * ```
 *
 * @example
 * ```typescript
 * // Delete a specific user
 * await run('user-123',
 *   'DELETE FROM users WHERE id = ?',
 *   ['user-123']
 * );
 *
 * // Delete user's posts (cascade delete)
 * await run('user-123',
 *   'DELETE FROM posts WHERE user_id = ?',
 *   ['user-123']
 * );
 *
 * // Delete with conditions
 * await run('user-123',
 *   'DELETE FROM posts WHERE user_id = ? AND created_at < ?',
 *   ['user-123', Date.now() - 86400000] // Posts older than 1 day
 * );
 * ```
 */
export async function run<T = Record<string, unknown>>(key: string, sql: string, bindings: any[] = []): Promise<QueryResult<T>> {
	const prepared = await prepare(key, sql);
	const result = await prepared.bind(...bindings).run<T>();

	if (!result.success) {
		throw new CollegeDBError(`Query failed: ${result.error || 'Unknown error'}`, 'QUERY_FAILED');
	}

	return result;
}

/**
 * Retrieves all records matching the query for a given primary key.
 *
 * This function is useful for fetching multiple records based on a primary key.
 * It automatically routes the query to the correct shard based on the provided
 * primary key, ensuring consistent data access.
 * @param key - Primary key to route the query
 * @param sql - The SQL statement to execute
 * @param bindings - Parameter values to bind to the SQL statement
 * @returns Promise that resolves to the result of the update operation
 * @throws {Error} If update fails or routing fails
 *
 * @example
 * ```typescript
 * type Post = {
 *  id: string;
 *  user_id: string;
 *  title: string;
 *  content: string;
 * };
 *
 *
 * // Get user's posts
 * const postsResult = await all<Post>('user-123',
 *   'SELECT * FROM posts WHERE user_id = ? ORDER BY created_at DESC',
 *   ['user-123']
 * );
 *
 * console.log(`User has ${postsResult.meta.count} posts`);
 * ```
 */
export async function all<T = Record<string, unknown>>(key: string, sql: string, bindings: any[] = []): Promise<QueryResult<T>> {
	const prepared = await prepare(key, sql);
	const result = await prepared.bind(...bindings).all<T>();

	if (!result.success) {
		throw new CollegeDBError(`Query failed: ${result.error || 'Unknown error'}`, 'QUERY_FAILED');
	}

	return result;
}

/**
 * Retrieves the first record matching the query for a given primary key.
 *
 * This function is useful for fetching a single record based on a primary key.
 * It automatically routes the query to the correct shard based on the provided
 * primary key, ensuring consistent data access.
 *
 * @template T - Type of the result record
 * @param key - Primary key to route the query
 * @param sql - SQL statement with parameter placeholders
 * @param bindings - Parameter values to bind to the SQL statement
 * @returns Promise that resolves to the first matching record, or null if not found
 * @throws {Error} If query fails or routing fails
 *
 * @example
 * ```typescript
 * type User = {
 *   id: string;
 *   name: string;
 *   email: string;
 * };
 * // Get a specific user
 * const userResult = await first<User>('user-123',
 *   'SELECT * FROM users WHERE id = ?',
 *   ['user-123']
 * );
 *
 * if (userResult) {
 *   console.log(`Found user: ${userResult.name}`);
 * }
 */
export async function first<T = Record<string, unknown>>(key: string, sql: string, bindings: any[] = []): Promise<T | null> {
	const prepared = await prepare(key, sql);
	const result = await prepared.bind(...bindings).first<T>();
	return result;
}

/**
 * Retrieves all records using a secondary lookup key when available.
 *
 * This helper attempts to resolve the lookup key through KV first. If a mapping
 * exists, the query executes on that shard directly. If the mapping is missing,
 * stale, or returns no rows, the helper safely falls back to fanout (`allAllShards`)
 * and returns merged results.
 *
 * @template T - Type of the result records
 * @param lookupKey - Secondary key such as `email:user@example.com` or `username:alice`
 * @param sql - SQL statement to execute
 * @param bindings - Parameter values to bind to the SQL statement
 * @param batchSize - Number of concurrent shard queries during fanout (default: 50)
 * @returns Promise resolving to merged query results
 * @since 1.1.4
 */
export async function allByLookupKey<T = Record<string, unknown>>(
	lookupKey: string,
	sql: string,
	bindings: any[] = [],
	batchSize: number = 50
): Promise<QueryResult<T>> {
	const config = getConfig();
	const mapper = getMapper(config);
	const mapping = await mapper.getShardMapping(lookupKey);

	if (mapping) {
		const mappedShardDb = config.shards[mapping.shard];
		if (mappedShardDb) {
			const mappedResult = await allShard<T>(mapping.shard, sql, bindings);
			if (mappedResult.success && mappedResult.results.length > 0) {
				return mappedResult;
			}
		}
	}

	const shardResults = await allAllShards<T>(sql, bindings, batchSize);
	return mergeAllShardQueryResults(shardResults);
}

/**
 * Retrieves the first record using a secondary lookup key when available.
 *
 * This helper avoids creating new primary-key mappings for secondary identifiers.
 * It first checks KV for a lookup-key mapping and queries that shard directly.
 * If no mapping exists (or the mapping is stale), it falls back to fanout
 * (`firstAllShards`) and returns the first non-null result.
 *
 * @template T - Type of the result record
 * @param lookupKey - Secondary key such as `email:user@example.com` or `username:alice`
 * @param sql - SQL statement to execute
 * @param bindings - Parameter values to bind to the SQL statement
 * @param batchSize - Number of concurrent shard queries during fanout (default: 50)
 * @returns Promise resolving to the first matching record, or null
 * @since 1.1.4
 */
export async function firstByLookupKey<T = Record<string, unknown>>(
	lookupKey: string,
	sql: string,
	bindings: any[] = [],
	batchSize: number = 50
): Promise<T | null> {
	const config = getConfig();
	const mapper = getMapper(config);
	const mapping = await mapper.getShardMapping(lookupKey);

	if (mapping) {
		const mappedShardDb = config.shards[mapping.shard];
		if (mappedShardDb) {
			const mappedFirst = await firstShard<T>(mapping.shard, sql, bindings);
			if (mappedFirst !== null) {
				return mappedFirst;
			}
		}
	}

	const fanoutResults = await firstAllShards<T>(sql, bindings, batchSize);
	return fanoutResults.find((row): row is T => row !== null) ?? null;
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
 * **Note**: This operation involves data migration and should be used
 * carefully in production environments. Consider the impact on ongoing queries.
 *
 * @param primaryKey - Primary key to reassign to a different shard
 * @param newBinding - New shard binding name where the data should be moved
 * @param tableName - Name of the table containing the record to migrate
 * @returns Promise that resolves when reassignment and migration are complete
 * @throws {Error} If target shard not found, mapping doesn't exist, or migration fails
 * @example
 * ```typescript
 * // Move a user from east to west coast for better latency
 * try {
 *   await reassignShard('user-california-123', 'db-west', 'users');
 *   console.log('User successfully moved to west coast shard');
 * } catch (error) {
 *   console.error('Reassignment failed:', error.message);
 * }
 *
 * // Load balancing: move high-activity user to dedicated shard
 * await reassignShard('user-enterprise-456', 'db-dedicated', 'users');
 * ```
 */
export async function reassignShard(primaryKey: string, newBinding: string, tableName: string): Promise<void> {
	const config = getConfig();

	if (!config.shards[newBinding]) {
		throw new CollegeDBError(`Shard ${newBinding} not found in configuration`, 'SHARD_NOT_FOUND');
	}

	const mapper = getMapper(config);
	const currentMapping = await mapper.getShardMapping(primaryKey);

	if (!currentMapping) {
		throw new CollegeDBError(`No existing mapping found for primary key: ${primaryKey}`, 'MAPPING_NOT_FOUND');
	}

	// Migrate data if different shard
	if (currentMapping.shard !== newBinding) {
		const { migrateRecord } = await import('./migrations');
		const sourceDb = config.shards[currentMapping.shard];
		const targetDb = config.shards[newBinding];

		if (!sourceDb || !targetDb) {
			throw new CollegeDBError('Source or target shard not available', 'SHARD_UNAVAILABLE');
		}

		await migrateRecord(sourceDb, targetDb, primaryKey, tableName);
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

	// Fallback: merge configured shards with KV-known shards
	try {
		const mapper = getMapper(config);
		const kvShards = await mapper.getKnownShards();
		const merged = new Set<string>([...Object.keys(config.shards), ...kvShards]);
		return Array.from(merged);
	} catch {
		// If KV lookup fails, just return configured shards
		return Object.keys(config.shards);
	}
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
	const mapper = getMapper(config);
	const counts = await mapper.getShardKeyCounts();

	// Merge shards from config and KV known shards
	let shardNames = Object.keys(config.shards);
	try {
		const kvKnown = await mapper.getKnownShards();
		shardNames = Array.from(new Set([...shardNames, ...kvKnown]));
	} catch {}

	return shardNames.map((binding) => ({
		binding,
		count: counts[binding] || 0
	}));
}

/**
 * Bypasses the normal routing logic to execute a query directly on a specified
 * shard. This is useful for administrative operations, cross-shard queries,
 * or when you need to query data that doesn't follow the primary key routing pattern.
 *
 * **Use with caution**: This function bypasses routing safeguards and should
 * be used only when you specifically need to target a particular shard.
 *
 * @param shardBinding - The shard binding name to execute the query on
 * @param sql - SQL statement to execute
 * @param bindings - Parameter values to bind to the SQL statement
 * @returns Promise resolving to the result of the query execution
 * @throws {Error} If shard not found or query fails
 * @example
 * ```typescript
 * // Administrative query: insert a new user directly into a specific shard
 * const result = await runShard('db-east',
 *   'INSERT INTO users (id, name, email) VALUES (?, ?, ?)',
 *   ['user-789', 'Alice', 'alice@example.com']
 * );
 * console.log(`Inserted user with ID: ${result.lastInsertId}`);
 * ```
 */
export async function runShard<T = Record<string, unknown>>(
	shardBinding: string,
	sql: string,
	bindings: any[] = []
): Promise<QueryResult<T>> {
	const config = getConfig();
	const db = config.shards[shardBinding];

	if (!db) {
		throw new CollegeDBError(`Shard ${shardBinding} not found`, 'SHARD_NOT_FOUND');
	}

	const result = await db
		.prepare(sql)
		.bind(...bindings)
		.run<T>();

	if (!result.success) {
		throw new CollegeDBError(`Query failed: ${result.error || 'Unknown error'}`, 'QUERY_FAILED');
	}

	return result;
}

/**
 * Bypasses the normal routing logic to execute a query directly on a specified
 * shard. This is useful for administrative operations, cross-shard queries,
 * or when you need to query data that doesn't follow the primary key routing pattern.
 *
 * **Use with caution**: This function bypasses routing safeguards and should
 * be used only when you specifically need to target a particular shard.
 *
 * @param shardBinding - The shard binding name to execute the query on
 * @param sql - SQL statement to execute
 * @param bindings - Parameter values to bind to the SQL statement
 * @returns Promise resolving to structured query results
 * @throws {Error} If shard not found or query fails
 * @example
 * ```typescript
 * // Administrative query: count all users across a specific shard
 * const eastCoastStats = await allShard('db-east',
 *   'SELECT COUNT(*) as user_count FROM users'
 * );
 * console.log(`East coast users: ${eastCoastStats.results[0].user_count}`);
 *
 * // Cross-shard analytics: get recent posts from a specific region
 * const recentPosts = await allShard('db-west',
 *   'SELECT id, title, created_at FROM posts WHERE created_at > ? ORDER BY created_at DESC LIMIT ?',
 *   [Date.now() - 86400000, 10] // Last 24 hours, limit 10
 * );
 *
 * // Schema inspection on specific shard
 * const tables = await allShard('db-central',
 *   "SELECT name FROM sqlite_master WHERE type='table'"
 * );
 * ```
 */
export async function allShard<T = Record<string, unknown>>(
	shardBinding: string,
	sql: string,
	bindings: any[] = []
): Promise<QueryResult<T>> {
	const config = getConfig();
	const db = config.shards[shardBinding];

	if (!db) {
		throw new CollegeDBError(`Shard ${shardBinding} not found`, 'SHARD_NOT_FOUND');
	}

	const result = await db
		.prepare(sql)
		.bind(...bindings)
		.all<T>();

	return result;
}

/**
 * Bypasses the normal routing logic to execute a query directly on a specified
 * shard. This is useful for administrative operations, cross-shard queries,
 * or when you need to query data that doesn't follow the primary key routing pattern.
 *
 * **Use with caution**: This function bypasses routing safeguards and should
 * be used only when you specifically need to target a particular shard.
 *
 * @param shardBinding - The shard binding name to execute the query on
 * @param sql - SQL statement to execute
 * @param bindings - Parameter values to bind to the SQL statement
 * @returns Promise resolving to the first matching record, or null if not found
 * @throws {Error} If shard not found or query fails
 * @example
 * ```typescript
 * // Administrative query: get a specific user from a shard
 * const user = await firstShard('db-east',
 *  'SELECT * FROM users WHERE id = ?',
 *   ['user-123']);
 * if (user) {
 *   console.log(`Found user: ${user.name}`);
 * } else {
 *   console.log('User not found in east shard');
 * }
 * ```
 */
export async function firstShard<T = Record<string, unknown>>(shardBinding: string, sql: string, bindings: any[] = []): Promise<T | null> {
	const config = getConfig();
	const db = config.shards[shardBinding];

	if (!db) {
		throw new CollegeDBError(`Shard ${shardBinding} not found`, 'SHARD_NOT_FOUND');
	}

	const result = await db
		.prepare(sql)
		.bind(...bindings)
		.first<T>();

	return result;
}

/**
 * Executes a query on all shards and returns the results from each shard.
 *
 * This function is useful for scenarios where you need to aggregate data
 * from multiple shards, such as running analytics or cross-shard queries.
 * It executes the same SQL statement on each shard and collects the results.
 * @param sql - The SQL statement to execute on each shard
 * @param bindings - Parameter values to bind to the SQL statement
 * @param batchSize - Number of concurrent queries to run at once (default: 50)
 * @returns Promise resolving to an array of results from each shard
 * @since 1.0.4
 */
export async function runAllShards<T = Record<string, unknown>>(
	sql: string,
	bindings: any[] = [],
	batchSize: number = 50
): Promise<QueryResult<T>[]> {
	const config = getConfig();
	const tasks: Array<() => Promise<QueryResult<T>>> = [];

	for (const [binding, db] of Object.entries(config.shards)) {
		if (!binding || !db) {
			console.error(`Shard ${binding ?? '<null>'} not found, skipping`);
			continue;
		}

		tasks.push(() =>
			db
				.prepare(sql)
				.bind(...bindings)
				.run<T>()
				.catch((error) => {
					console.error(`Error executing query on shard ${binding}:`, error);
					return {
						success: false,
						results: [],
						error: error instanceof Error ? error.message : String(error),
						meta: { duration: 0 }
					} satisfies QueryResult<T>;
				})
		);
	}

	const out: QueryResult<T>[] = [];
	for (let i = 0; i < tasks.length; i += batchSize) {
		const batch = tasks.slice(i, i + batchSize).map((fn) => fn());
		out.push(...(await Promise.all(batch)));
	}

	return out;
}

/**
 * Executes a query on all shards and returns all matching records from each shard.
 *
 * This function is useful for scenarios where you need to retrieve all records
 * matching a query across multiple shards, such as aggregating data or running
 * cross-shard analytics.
 * @param sql - The SQL statement to execute on each shard
 * @param bindings - Parameter values to bind to the SQL statement
 * @param batchSize - Number of concurrent queries to run at once (default: 50)
 * @returns Promise resolving to an array of results from each shard
 * @since 1.0.4
 */
export async function allAllShards<T = Record<string, unknown>>(
	sql: string,
	bindings: any[] = [],
	batchSize: number = 50
): Promise<QueryResult<T>[]> {
	const config = getConfig();
	const tasks: Array<() => Promise<QueryResult<T>>> = [];

	for (const [binding, db] of Object.entries(config.shards)) {
		if (!binding || !db) {
			console.error(`Shard ${binding ?? '<null>'} not found, skipping`);
			continue;
		}

		tasks.push(() =>
			db
				.prepare(sql)
				.bind(...bindings)
				.all<T>()
				.catch((error) => {
					console.error(`Error executing query on shard ${binding}:`, error);
					return {
						success: false,
						results: [],
						error: error instanceof Error ? error.message : String(error),
						meta: { duration: 0 }
					} satisfies QueryResult<T>;
				})
		);
	}

	const out: QueryResult<T>[] = [];
	for (let i = 0; i < tasks.length; i += batchSize) {
		const batch = tasks.slice(i, i + batchSize).map((fn) => fn());
		out.push(...(await Promise.all(batch)));
	}

	return out;
}

/**
 * Options for global all-shards merge/sort/pagination.
 * @since 1.1.4
 */
export interface GlobalAllShardsOptions<T = Record<string, unknown>> {
	/** Number of concurrent shard queries to run at once (default: 50). */
	batchSize?: number;
	/** Number of rows to skip after global merge/sort (default: 0). */
	offset?: number;
	/** Maximum rows to return after global merge/sort. */
	limit?: number;
	/** Field name or selector used for global sorting. */
	sortBy?: keyof T | ((row: T) => unknown);
	/** Sort direction for `sortBy` (default: `asc`). */
	sortDirection?: 'asc' | 'desc';
	/** Optional custom comparator; takes precedence over `sortBy`. */
	comparator?: (left: T, right: T) => number;
	/** Optional global row filter applied before sort/paginate. */
	filter?: (row: T) => boolean;
}

function normalizeBatchSize(batchSize: number | undefined, defaultValue: number = 50): number {
	if (!Number.isFinite(batchSize ?? defaultValue)) {
		return defaultValue;
	}

	return Math.max(1, Math.floor(batchSize ?? defaultValue));
}

function normalizeOffset(offset: number | undefined): number {
	if (!Number.isFinite(offset ?? 0)) {
		return 0;
	}

	return Math.max(0, Math.floor(offset ?? 0));
}

function normalizeLimit(limit: number | undefined): number | undefined {
	if (limit === undefined) {
		return undefined;
	}

	if (!Number.isFinite(limit)) {
		return undefined;
	}

	return Math.max(0, Math.floor(limit));
}

function getRowSortValue<T>(row: T, sortBy: GlobalAllShardsOptions<T>['sortBy']): unknown {
	if (typeof sortBy === 'function') {
		return sortBy(row);
	}

	if (!sortBy || typeof row !== 'object' || row === null) {
		return undefined;
	}

	return (row as Record<string, unknown>)[String(sortBy)];
}

function compareUnknown(left: unknown, right: unknown): number {
	if (left === right) return 0;
	if (left === null || left === undefined) return 1;
	if (right === null || right === undefined) return -1;

	if (typeof left === 'number' && typeof right === 'number') {
		return left - right;
	}

	if (typeof left === 'bigint' && typeof right === 'bigint') {
		return left < right ? -1 : 1;
	}

	if (left instanceof Date && right instanceof Date) {
		return left.getTime() - right.getTime();
	}

	if (typeof left === 'boolean' && typeof right === 'boolean') {
		return Number(left) - Number(right);
	}

	return String(left).localeCompare(String(right), undefined, { numeric: true, sensitivity: 'base' });
}

function mergeAllShardQueryResults<T = Record<string, unknown>>(shardResults: QueryResult<T>[]): QueryResult<T> {
	const allResults = shardResults.flatMap((result) => result.results || []);
	const failures = shardResults.filter((result) => !result.success);
	const totalDuration = shardResults.reduce((sum, result) => sum + (result.meta?.duration || 0), 0);

	if (failures.length === 0) {
		return {
			success: true,
			results: allResults,
			meta: { duration: totalDuration }
		};
	}

	const errorMessage = failures
		.map((failure) => failure.error || 'Unknown shard query error')
		.filter(Boolean)
		.join('; ');

	return {
		success: false,
		results: allResults,
		error: errorMessage || 'One or more shard queries failed',
		meta: { duration: totalDuration }
	};
}

/**
 * Executes a query on all shards and applies global merge/sort/pagination in-library.
 *
 * Unlike `allAllShards`, this helper returns a single merged `QueryResult` and can
 * sort/paginate across the full combined result set after fanout.
 *
 * @template T - Type of the result records
 * @param sql - SQL statement to execute on each shard
 * @param bindings - Parameter values to bind to the SQL statement
 * @param options - Global merge/sort/pagination options
 * @returns Promise resolving to one globally-processed query result
 * @since 1.1.4
 */
export async function allAllShardsGlobal<T = Record<string, unknown>>(
	sql: string,
	bindings: any[] = [],
	options: GlobalAllShardsOptions<T> = {}
): Promise<QueryResult<T>> {
	const batchSize = normalizeBatchSize(options.batchSize);
	const offset = normalizeOffset(options.offset);
	const limit = normalizeLimit(options.limit);

	const merged = mergeAllShardQueryResults(await allAllShards<T>(sql, bindings, batchSize));
	let rows = merged.results;

	if (options.filter) {
		rows = rows.filter((row) => options.filter?.(row));
	}

	if (options.comparator) {
		rows = [...rows].sort(options.comparator);
	} else if (options.sortBy) {
		const direction = options.sortDirection === 'desc' ? -1 : 1;
		rows = [...rows].sort((left, right) => {
			const leftValue = getRowSortValue(left, options.sortBy);
			const rightValue = getRowSortValue(right, options.sortBy);
			return compareUnknown(leftValue, rightValue) * direction;
		});
	}

	const end = limit === undefined ? undefined : offset + limit;
	const pagedRows = rows.slice(offset, end);

	return {
		...merged,
		results: pagedRows
	};
}

/**
 * Executes a query on all shards and returns the first matching record from each shard.
 *
 * This function is useful for scenarios where you need to retrieve a single record
 * from each shard, such as fetching the latest entry or a specific item that may
 * exist on multiple shards.
 * @param sql - The SQL statement to execute
 * @param bindings - Parameter values to bind to the SQL statement
 * @param batchSize - Number of concurrent queries to run at once (default: 50)
 * @returns Promise resolving to an array of first matching records from each shard
 * @since 1.0.4
 */
export async function firstAllShards<T = Record<string, unknown>>(
	sql: string,
	bindings: any[] = [],
	batchSize: number = 50
): Promise<(T | null)[]> {
	const config = getConfig();
	const tasks: Array<() => Promise<T | null>> = [];

	for (const [binding, db] of Object.entries(config.shards)) {
		if (!binding || !db) {
			console.error(`Shard ${binding ?? '<null>'} not found, skipping`);
			continue;
		}

		tasks.push(() =>
			db
				.prepare(sql)
				.bind(...bindings)
				.first<T>()
				.catch((error) => {
					console.error(`Error executing query on shard ${binding}:`, error);
					return null;
				})
		);
	}

	const out: (T | null)[] = [];
	for (let i = 0; i < tasks.length; i += batchSize) {
		const batch = tasks.slice(i, i + batchSize).map((fn) => fn());
		out.push(...(await Promise.all(batch)));
	}

	return out;
}

/**
 * Executes a query on all shards with global merge/sort/pagination and returns
 * the first row after global processing.
 *
 * @template T - Type of the result record
 * @param sql - SQL statement to execute on each shard
 * @param bindings - Parameter values to bind to the SQL statement
 * @param options - Global merge/sort/pagination options (batchSize, sort, offset)
 * @returns Promise resolving to the first globally-processed row, or null
 * @since 1.1.4
 */
export async function firstAllShardsGlobal<T = Record<string, unknown>>(
	sql: string,
	bindings: any[] = [],
	options: Omit<GlobalAllShardsOptions<T>, 'limit'> = {}
): Promise<T | null> {
	const merged = await allAllShardsGlobal<T>(sql, bindings, {
		...options,
		limit: 1
	});

	return merged.results[0] ?? null;
}

/**
 * Flushes all shard mappings (development only)
 *
 * Completely clears all primary key to shard mappings from both KV storage
 * and the Durable Object coordinator. This operation resets the entire
 * routing system to a clean state.
 *
 * **DANGER**: This operation is destructive and irreversible. After flushing,
 * all existing primary keys will be treated as new and may be assigned to
 * different shards than before, causing data routing issues.
 *
 * **Use only for**:
 * - Development and testing environments
 * - Complete system resets
 * - Emergency recovery scenarios
 *
 * @returns Promise that resolves when all mappings are cleared
 * @example
 * ```typescript
 * // Only use in development!
 * if (process.env.NODE_ENV === 'development') {
 *   await flush();
 *   console.log('All shard mappings cleared for testing');
 *
 *   // Now all keys will be reassigned on next access
 *   await run('user-123', 'INSERT INTO users (id, name) VALUES (?, ?)',
 *     ['user-123', 'Test User']);
 * }
 * ```
 */
export async function flush(): Promise<void> {
	const config = getConfig();
	const mapper = getMapper(config);

	await mapper.clearAllMappings();
	shardSizeCache.clear();

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

/**
 * Gets the size of a specific D1 database in bytes.
 * Uses efficient SQLite pragma queries to determine database size.
 *
 * @param shardBinding - The shard binding name to check the size of
 * @returns Promise resolving to the database size in bytes
 * @throws {CollegeDBError} If shard not found or size query fails
 * @example
 * ```typescript
 * // Get size of a specific shard
 * const sizeInBytes = await getDatabaseSizeForShard('db-east');
 * console.log(`Database size: ${Math.round(sizeInBytes / 1024 / 1024)} MB`);
 * ```
 */
export async function getDatabaseSizeForShard(shardBinding: string): Promise<number> {
	const config = getConfig();
	const database = config.shards[shardBinding];

	if (!database) {
		throw new CollegeDBError(`Shard ${shardBinding} not found`, 'SHARD_NOT_FOUND');
	}

	return await getDatabaseSize(database);
}

const SQL_IDENTIFIER_PART_REGEX = /^[A-Za-z_][A-Za-z0-9_]*$/;

function quoteIdentifier(identifier: string): string {
	const trimmed = identifier.trim();
	if (!trimmed) {
		throw new CollegeDBError('Identifier cannot be empty', 'INVALID_IDENTIFIER');
	}

	const parts = trimmed.split('.').map((part) => part.trim());
	if (parts.some((part) => !part || !SQL_IDENTIFIER_PART_REGEX.test(part))) {
		throw new CollegeDBError(`Invalid SQL identifier: ${identifier}`, 'INVALID_IDENTIFIER');
	}

	return parts.map((part) => `"${part}"`).join('.');
}

function normalizeIndexNameSegment(value: string): string {
	return value
		.toLowerCase()
		.replace(/[^a-z0-9_]+/g, '_')
		.replace(/_+/g, '_')
		.replace(/^_+|_+$/g, '');
}

/**
 * Column specification for index creation helpers.
 * @since 1.1.4
 */
export interface IndexColumnDefinition {
	/** Column name to include in the index. */
	name: string;
	/** Optional sort direction for this column. */
	order?: 'ASC' | 'DESC';
	/** Optional collation (e.g., NOCASE). */
	collate?: string;
}

/**
 * Options for index creation helpers.
 * @since 1.1.4
 */
export interface CreateIndexOptions {
	/** Explicit index name. When omitted, a deterministic name is generated. */
	indexName?: string;
	/** Create a unique index. */
	unique?: boolean;
	/** Include `IF NOT EXISTS` in generated SQL. @default true */
	ifNotExists?: boolean;
	/** Optional partial-index predicate (trusted SQL only). */
	where?: string;
	/** Number of concurrent shard operations for all-shard variants. @default 50 */
	batchSize?: number;
}

function normalizeIndexColumns(columns: string | string[] | IndexColumnDefinition[]): IndexColumnDefinition[] {
	if (typeof columns === 'string') {
		return [{ name: columns }];
	}

	if (!Array.isArray(columns) || columns.length === 0) {
		throw new CollegeDBError('At least one index column is required', 'INVALID_INDEX_COLUMNS');
	}

	return columns.map((column) => {
		if (typeof column === 'string') {
			return { name: column };
		}

		if (!column?.name) {
			throw new CollegeDBError('Index column name is required', 'INVALID_INDEX_COLUMNS');
		}

		return {
			name: column.name,
			order: column.order,
			collate: column.collate
		};
	});
}

function buildCreateIndexSQL(
	table: string,
	columns: string | string[] | IndexColumnDefinition[],
	options: CreateIndexOptions = {}
): string {
	const normalizedColumns = normalizeIndexColumns(columns);
	const quotedTable = quoteIdentifier(table);
	const generatedIndexName = options.indexName
		? options.indexName
		: ['idx', normalizeIndexNameSegment(table), ...normalizedColumns.map((column) => normalizeIndexNameSegment(column.name))]
				.filter(Boolean)
				.join('_')
				.slice(0, 120);
	const quotedIndexName = quoteIdentifier(generatedIndexName || 'idx_auto');

	const columnClauses = normalizedColumns
		.map((column) => {
			const quotedColumn = quoteIdentifier(column.name);
			const order = column.order ? ` ${column.order}` : '';
			const collate = column.collate ? ` COLLATE ${quoteIdentifier(column.collate).replace(/"/g, '')}` : '';
			return `${quotedColumn}${collate}${order}`;
		})
		.join(', ');

	const ifNotExistsClause = options.ifNotExists === false ? '' : ' IF NOT EXISTS';
	const uniqueClause = options.unique ? 'UNIQUE ' : '';
	const whereClause = options.where?.trim() ? ` WHERE ${options.where.trim()}` : '';

	return `CREATE ${uniqueClause}INDEX${ifNotExistsClause} ${quotedIndexName} ON ${quotedTable} (${columnClauses})${whereClause}`;
}

/**
 * Creates an index on the shard resolved by the provided key.
 *
 * @param key - Primary key used for shard routing
 * @param table - Target table name
 * @param columns - One or more columns to index
 * @param options - Index creation options
 * @returns Query result for the DDL statement
 * @since 1.1.4
 */
export async function index<T = Record<string, unknown>>(
	key: string,
	table: string,
	columns: string | string[] | IndexColumnDefinition[],
	options: Omit<CreateIndexOptions, 'batchSize'> = {}
): Promise<QueryResult<T>> {
	const sql = buildCreateIndexSQL(table, columns, options);
	return run<T>(key, sql);
}

/**
 * Creates an index directly on a specific shard.
 *
 * @param shardBinding - Shard binding name
 * @param table - Target table name
 * @param columns - One or more columns to index
 * @param options - Index creation options
 * @returns Query result for the DDL statement
 * @since 1.1.4
 */
export async function indexShard<T = Record<string, unknown>>(
	shardBinding: string,
	table: string,
	columns: string | string[] | IndexColumnDefinition[],
	options: Omit<CreateIndexOptions, 'batchSize'> = {}
): Promise<QueryResult<T>> {
	const sql = buildCreateIndexSQL(table, columns, options);
	return runShard<T>(shardBinding, sql);
}

/**
 * Creates an index across all configured shards.
 *
 * @param table - Target table name
 * @param columns - One or more columns to index
 * @param options - Index creation options, including `batchSize`
 * @returns Per-shard query results
 * @since 1.1.4
 */
export async function indexAllShards<T = Record<string, unknown>>(
	table: string,
	columns: string | string[] | IndexColumnDefinition[],
	options: CreateIndexOptions = {}
): Promise<QueryResult<T>[]> {
	const sql = buildCreateIndexSQL(table, columns, options);
	return runAllShards<T>(sql, [], normalizeBatchSize(options.batchSize));
}

/**
 * Explain helpers options.
 * @since 1.1.4
 */
export interface ExplainOptions {
	/** Explain mode. @default query-plan */
	mode?: 'query-plan' | 'raw' | 'analyze';
	/** Number of concurrent shard operations for all-shard variants. @default 50 */
	batchSize?: number;
}

function buildExplainSQL(sql: string, mode: ExplainOptions['mode'] = 'query-plan'): string {
	switch (mode) {
		case 'raw':
			return `EXPLAIN ${sql}`;
		case 'analyze':
			return `EXPLAIN ANALYZE ${sql}`;
		case 'query-plan':
		default:
			return `EXPLAIN QUERY PLAN ${sql}`;
	}
}

/**
 * Executes an explain query on the shard resolved by key.
 *
 * @param key - Primary key used for shard routing
 * @param sql - SQL statement to inspect
 * @param bindings - Parameter values for the SQL statement
 * @param options - Explain mode options
 * @returns Explain rows as a QueryResult
 * @since 1.1.4
 */
export async function explain<T = Record<string, unknown>>(
	key: string,
	sql: string,
	bindings: any[] = [],
	options: Omit<ExplainOptions, 'batchSize'> = {}
): Promise<QueryResult<T>> {
	return all<T>(key, buildExplainSQL(sql, options.mode), bindings);
}

/**
 * Executes an explain query on a specific shard.
 *
 * @param shardBinding - Shard binding name
 * @param sql - SQL statement to inspect
 * @param bindings - Parameter values for the SQL statement
 * @param options - Explain mode options
 * @returns Explain rows as a QueryResult
 * @since 1.1.4
 */
export async function explainShard<T = Record<string, unknown>>(
	shardBinding: string,
	sql: string,
	bindings: any[] = [],
	options: Omit<ExplainOptions, 'batchSize'> = {}
): Promise<QueryResult<T>> {
	return allShard<T>(shardBinding, buildExplainSQL(sql, options.mode), bindings);
}

/**
 * Executes an explain query across all shards.
 *
 * @param sql - SQL statement to inspect
 * @param bindings - Parameter values for the SQL statement
 * @param options - Explain options, including `batchSize`
 * @returns Per-shard explain query results
 * @since 1.1.4
 */
export async function explainAllShards<T = Record<string, unknown>>(
	sql: string,
	bindings: any[] = [],
	options: ExplainOptions = {}
): Promise<QueryResult<T>[]> {
	return allAllShards<T>(buildExplainSQL(sql, options.mode), bindings, normalizeBatchSize(options.batchSize));
}

/**
 * Table row-count result for a shard.
 * @since 1.1.4
 */
export interface ShardTableCount {
	shard: string;
	count: number | null;
	success: boolean;
	error?: string;
}

/**
 * Counts rows for a table on the shard resolved by key.
 *
 * @param key - Primary key used for shard routing
 * @param table - Table name to count
 * @returns Row count for that routed shard
 * @since 1.1.4
 */
export async function count(key: string, table: string): Promise<number> {
	const quotedTable = quoteIdentifier(table);
	const row = await first<{ row_count?: number | string }>(key, `SELECT COUNT(*) AS row_count FROM ${quotedTable}`);
	if (!row || row.row_count === undefined || row.row_count === null) {
		return 0;
	}

	return Number(row.row_count) || 0;
}

/**
 * Counts rows for a table on a specific shard.
 *
 * @param shardBinding - Shard binding name
 * @param table - Table name to count
 * @returns Row count for the shard
 * @since 1.1.4
 */
export async function countShard(shardBinding: string, table: string): Promise<number> {
	const quotedTable = quoteIdentifier(table);
	const row = await firstShard<{ row_count?: number | string }>(shardBinding, `SELECT COUNT(*) AS row_count FROM ${quotedTable}`);
	if (!row || row.row_count === undefined || row.row_count === null) {
		return 0;
	}

	return Number(row.row_count) || 0;
}

/**
 * Counts rows for a table across all shards.
 *
 * @param table - Table name to count
 * @param batchSize - Number of concurrent shard queries (default: 50)
 * @returns Per-shard counts and global total
 * @since 1.1.4
 */
export async function countAllShards(table: string, batchSize: number = 50): Promise<{ total: number; shards: ShardTableCount[] }> {
	const config = getConfig();
	const normalizedBatchSize = normalizeBatchSize(batchSize);
	const quotedTable = quoteIdentifier(table);
	const sql = `SELECT COUNT(*) AS row_count FROM ${quotedTable}`;
	const tasks: Array<() => Promise<ShardTableCount>> = [];

	for (const [binding, db] of Object.entries(config.shards)) {
		if (!binding || !db) {
			continue;
		}

		tasks.push(async () => {
			try {
				const row = await db.prepare(sql).first<{ row_count?: number | string }>();
				const parsed = Number(row?.row_count ?? 0);
				return {
					shard: binding,
					count: Number.isFinite(parsed) ? parsed : 0,
					success: true
				};
			} catch (error) {
				return {
					shard: binding,
					count: null,
					success: false,
					error: error instanceof Error ? error.message : String(error)
				};
			}
		});
	}

	const shards: ShardTableCount[] = [];
	for (let i = 0; i < tasks.length; i += normalizedBatchSize) {
		const batch = tasks.slice(i, i + normalizedBatchSize).map((task) => task());
		shards.push(...(await Promise.all(batch)));
	}

	const total = shards.reduce((sum, shard) => sum + (shard.count ?? 0), 0);
	return { total, shards };
}

/**
 * Size information for a shard.
 * @since 1.1.4
 */
export interface ShardSizeResult {
	shard: string;
	size: number | null;
	success: boolean;
	error?: string;
}

/**
 * Gets the size in bytes for the shard resolved by key.
 *
 * @param key - Primary key used for shard routing
 * @returns Database size in bytes
 * @since 1.1.4
 */
export async function getDatabaseSizeForKey(key: string): Promise<number> {
	const config = getConfig();
	const shardBinding = await getShardForKey(key, 'read');
	const database = config.shards[shardBinding];

	if (!database) {
		throw new CollegeDBError(`Shard ${shardBinding} not found in configuration`, 'SHARD_NOT_FOUND');
	}

	return getDatabaseSize(database);
}

/**
 * Gets database sizes for all shards.
 *
 * @param batchSize - Number of concurrent shard queries (default: 50)
 * @returns Per-shard size results with success/error status
 * @since 1.1.4
 */
export async function getDatabaseSizesAllShards(batchSize: number = 50): Promise<ShardSizeResult[]> {
	const config = getConfig();
	const normalizedBatchSize = normalizeBatchSize(batchSize);
	const tasks: Array<() => Promise<ShardSizeResult>> = [];

	for (const [binding, db] of Object.entries(config.shards)) {
		if (!binding || !db) {
			continue;
		}

		tasks.push(async () => {
			try {
				return {
					shard: binding,
					size: await getDatabaseSize(db),
					success: true
				};
			} catch (error) {
				return {
					shard: binding,
					size: null,
					success: false,
					error: error instanceof Error ? error.message : String(error)
				};
			}
		});
	}

	const results: ShardSizeResult[] = [];
	for (let i = 0; i < tasks.length; i += normalizedBatchSize) {
		const batch = tasks.slice(i, i + normalizedBatchSize).map((task) => task());
		results.push(...(await Promise.all(batch)));
	}

	return results;
}

/**
 * Gets the combined size in bytes across all shards.
 *
 * Failed shard size checks are excluded from the sum.
 *
 * @param batchSize - Number of concurrent shard queries (default: 50)
 * @returns Total size in bytes across successfully measured shards
 * @since 1.1.4
 */
export async function getTotalDatabaseSize(batchSize: number = 50): Promise<number> {
	const sizes = await getDatabaseSizesAllShards(batchSize);
	return sizes.reduce((sum, result) => sum + (result.size ?? 0), 0);
}
