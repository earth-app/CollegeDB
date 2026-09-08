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
 * @author Gregory Mitchell
 * @since 1.0.0
 */

import type { Request } from '@cloudflare/workers-types';
import { CollegeDBError } from './errors';
import { KVShardMapper } from './kvmap';
import { createSchemaAcrossShards } from './migrations';
import {
	addPlacementException,
	candidateShards,
	createManifest,
	hrwShard,
	loadPlacementExceptions,
	loadPlacementManifest,
	resetPlacementState,
	savePlacementManifest,
	withCurrentTopology,
	type PlacementManifest
} from './placement';
import { planQuery, unroutableError } from './planner';
import { createWorkersKVProvider, isKVStorage, toProvider, type DrizzleSqlTagLike } from './providers';
import {
	buildDelete,
	buildInsert,
	buildUpdate,
	buildUpsert,
	validateIdentifier,
	type BuildInsertOptions,
	type BuildUpsertOptions,
	type ColumnValues
} from './query';
import { instrumentKV, instrumentSQL, phaseEnd, phaseStart, setPhaseObserver } from './telemetry';
import type {
	CollegeDBConfig,
	D1Region,
	KVStorage,
	OperationType,
	PreparedStatement,
	QueryResult,
	SQLDatabase,
	SQLDialect,
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

let generatedInsertRoundRobinIndex = 0;

/**
 * Cached placement decision for the active configuration. Cleared whenever the
 * configuration changes, so a reconfigured process never resolves against
 * another deployment's topology.
 *
 * @private
 */
let placementDecision: { mode: 'computed' | 'kv'; manifest: PlacementManifest | null } | null = null;

/**
 * Shard sets whose known-shard registration has already been done in this
 * process, so calling {@link initialize} per request costs nothing after the
 * first call.
 *
 * @private
 */
const syncedShardSets = new Set<string>();

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
 * will automatically create the necessary mappings so existing rows stay reachable.
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
	const active = applyConfig(config);
	const fingerprint = shardSetFingerprint(active);

	// The documented Workers pattern calls initialize on every request, so this
	// bookkeeping runs per request unless it is memoized. Registering the same
	// shard set again is a KV read plus a KV write that changes nothing, and it
	// is now awaited through waitUntil rather than silently cancelled, so the
	// cost is real. Skip it once the set has been registered.
	if (syncedShardSets.has(fingerprint)) {
		return;
	}
	syncedShardSets.add(fingerprint);

	// Background: sync KV known shards with configured shards
	try {
		const mapper = getMapper(active);
		track(
			active,
			Promise.resolve()
				.then(async () => {
					const existing = await mapper.getKnownShards();
					const merged = Array.from(new Set([...existing, ...Object.keys(active.shards)]));
					await mapper.setKnownShards(merged);
				})
				.catch(() => {
					// Let a later initialize retry rather than leaving the set
					// recorded as synced when it never was.
					syncedShardSets.delete(fingerprint);
				})
		);
	} catch {
		syncedShardSets.delete(fingerprint);
	}

	if (active.shards && Object.keys(active.shards).length > 0 && !active.disableAutoMigration) {
		track(
			active,
			performAutoMigration(active).catch((error) => {
				console.warn('Background auto-migration failed:', error);
			})
		);
	}
}

/**
 * Identifies a configuration by the shard set its bookkeeping depends on.
 * @private
 */
function shardSetFingerprint(config: CollegeDBConfig): string {
	return Object.keys(config.shards).sort().join('\u0000');
}

/**
 * Installs a configuration as the active one, wrapping the providers with
 * timing instrumentation when `onPhase` is set.
 *
 * The instrumented providers replace the originals on the stored config rather
 * than at each call site, so the read-through cache and the lookup helpers are
 * measured too. When `onPhase` is unset no wrapper is created.
 *
 * @private
 * @returns The configuration that was installed
 */
function applyConfig(config: CollegeDBConfig): CollegeDBConfig {
	setPhaseObserver(config.onPhase ?? null);

	let active = config;
	if (config.onPhase) {
		const shards: Record<string, SQLDatabase> = {};
		for (const [binding, database] of Object.entries(config.shards)) {
			shards[binding] = database ? instrumentSQL(database, binding) : database;
		}
		active = { ...config, kv: instrumentKV(config.kv), shards };
	}

	// Reconfiguring with the same bindings and options keeps the existing mapper,
	// so its mapping and hash caches survive. Rebuilding it every time made
	// `mappingCacheTtlMs` dead on Workers, where the documented pattern calls
	// initialize once per request: every routed read paid a KV round trip no
	// matter how recently the same key had been resolved.
	//
	// The mapper's caches are keyed by primary key and do not depend on the
	// allocation strategy, so they survive a strategy change. Placement state
	// does depend on it, and is rebuilt whenever the strategy or the shard set
	// moves.
	const previous = globalConfig;
	const reuseMapper = globalMapper !== null && previous !== null && isSameMapperConfig(previous, active);
	const reusePlacement = reuseMapper && previous !== null && isSamePlacementConfig(previous, active);

	globalConfig = active;

	if (!reuseMapper) {
		globalMapper = new KVShardMapper(active.kv, {
			hashShardMappings: active.hashShardMappings,
			mappingCacheTtlMs: active.mappingCacheTtlMs,
			knownShardsCacheTtlMs: active.knownShardsCacheTtlMs,
			legacyMultiKeyLookup: active.legacyMultiKeyLookup
		});
	}

	if (!reusePlacement) {
		shardSizeCache.clear();
		generatedInsertRoundRobinIndex = 0;
		resetPlacementState();
		placementDecision = null;
	}

	return active;
}

/**
 * Whether two configurations can share one {@link KVShardMapper}.
 *
 * Compares the KV store and the shard providers by identity rather than by
 * name, so a test that builds fresh in-memory providers always gets a fresh
 * mapper while a Worker re-initializing with the same bindings keeps its caches.
 *
 * @private
 */
function isSameMapperConfig(left: CollegeDBConfig, right: CollegeDBConfig): boolean {
	if (
		left.kv !== right.kv ||
		left.hashShardMappings !== right.hashShardMappings ||
		left.mappingCacheTtlMs !== right.mappingCacheTtlMs ||
		left.knownShardsCacheTtlMs !== right.knownShardsCacheTtlMs ||
		left.legacyMultiKeyLookup !== right.legacyMultiKeyLookup
	) {
		return false;
	}

	const leftBindings = Object.keys(left.shards);
	const rightBindings = Object.keys(right.shards);
	if (leftBindings.length !== rightBindings.length) {
		return false;
	}

	return leftBindings.every((binding) => left.shards[binding] === right.shards[binding]);
}

/**
 * Whether two configurations resolve placement identically.
 *
 * Placement depends on the strategy and the target region as well as the shard
 * set, so a cached decision cannot outlive a change to any of them.
 *
 * @private
 */
function isSamePlacementConfig(left: CollegeDBConfig, right: CollegeDBConfig): boolean {
	return (
		left.placement === right.placement &&
		left.targetRegion === right.targetRegion &&
		left.maxDatabaseSize === right.maxDatabaseSize &&
		JSON.stringify(left.strategy ?? null) === JSON.stringify(right.strategy ?? null) &&
		JSON.stringify(left.shardLocations ?? null) === JSON.stringify(right.shardLocations ?? null)
	);
}

/**
 * Hands a background promise to the host so it outlives the current request.
 *
 * On Workers, work not attached to a request is cancelled when that request
 * ends, which silently abandoned the known-shard sync and the auto-migration
 * that {@link initialize} starts. Passing `ctx.waitUntil` as `config.waitUntil`
 * keeps them alive; without it the behavior is unchanged.
 *
 * @private
 */
function track(config: CollegeDBConfig, promise: Promise<unknown>): void {
	if (config.waitUntil) {
		try {
			config.waitUntil(promise);
			return;
		} catch (error) {
			console.warn('waitUntil rejected the background task:', error);
		}
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
 * will automatically create the necessary mappings so existing rows stay reachable.
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
	const active = applyConfig(config);

	// Sync KV known shards with configured shards (awaited in async init)
	try {
		const mapper = getMapper(active);
		const existing = await mapper.getKnownShards();
		const merged = Array.from(new Set([...existing, ...Object.keys(active.shards)]));
		await mapper.setKnownShards(merged);
	} catch {}

	if (active.shards && Object.keys(active.shards).length > 0 && !active.disableAutoMigration)
		try {
			await performAutoMigration(active);
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
	generatedInsertRoundRobinIndex = 0;
	ensuredSchemaFingerprints.clear();
	resetPlacementState();
	placementDecision = null;
	syncedShardSets.clear();
	setPhaseObserver(null);
}

/**
 * Reports whether CollegeDB has been initialized in the current context.
 *
 * Lets callers drop the ad-hoc `let initialized = false` guard they otherwise
 * keep alongside a wrapper around {@link initialize}.
 *
 * @returns `true` once {@link initialize}/{@link initializeAsync}/{@link initializeFromEnv} has run
 * @since 1.2.4
 * @example
 * ```typescript
 * if (!isInitialized()) {
 *   initializeFromEnv(env);
 * }
 * ```
 */
export function isInitialized(): boolean {
	return globalConfig !== null;
}

/**
 * Returns the active configuration, or `null` when not initialized.
 *
 * Intended for CollegeDB's own KV-layer helpers (`cached`, `setLookup`, ...)
 * that need the configured KV store without throwing.
 *
 * @internal
 */
export function getActiveConfig(): CollegeDBConfig | null {
	return globalConfig;
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

	// A statement can open with WITH and still be a write: SQLite and PostgreSQL
	// both accept `WITH x AS (...) INSERT/UPDATE/DELETE ...`. Treating those as
	// reads picked the read strategy to allocate a shard for a write.
	if (sql0.startsWith('WITH')) {
		return /\b(INSERT|UPDATE|DELETE|REPLACE|MERGE|UPSERT)\b/.test(sql0) ? 'write' : 'read';
	}

	if (
		sql0.startsWith('SELECT') ||
		sql0.startsWith('VALUES') ||
		sql0.startsWith('TABLE') ||
		sql0.startsWith('PRAGMA') ||
		sql0.startsWith('EXPLAIN') ||
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
 * Gets the approximate size of a shard's database in bytes.
 *
 * @private
 * @param database - The SQL database instance to measure
 * @returns Promise resolving to the database size in bytes
 * @throws {CollegeDBError} If the size query fails
 */
/**
 * Sizing statements per backend family, tried in order until one answers.
 *
 * Sizing used to be two SQLite pragmas for every backend, which throws on
 * PostgreSQL and MySQL. `filterShardsBySize` swallowed the rejection through
 * `Promise.allSettled`, so `maxDatabaseSize` silently did nothing outside
 * SQLite and D1, and the public sizing helpers threw outright.
 *
 * The SQLite entry is a single statement using the pragma table-valued
 * functions; the two-pragma form follows it for backends that expose `PRAGMA`
 * as a statement but not as a function.
 * @private
 */
interface SizeQuery {
	/** Backend family this statement set targets */
	family: string;
	/** Statements to run, with the result column each one must produce */
	steps: Array<{ sql: string; column: string }>;
}

const SIZE_QUERIES: SizeQuery[] = [
	{
		family: 'sqlite-pragma',
		steps: [
			{ sql: 'PRAGMA page_count', column: 'page_count' },
			{ sql: 'PRAGMA page_size', column: 'page_size' }
		]
	},
	{
		family: 'sqlite',
		steps: [
			{
				sql: 'SELECT (SELECT * FROM pragma_page_count()) * (SELECT * FROM pragma_page_size()) AS collegedb_size_bytes',
				column: 'collegedb_size_bytes'
			}
		]
	},
	{
		family: 'postgres',
		steps: [{ sql: 'SELECT pg_database_size(current_database()) AS collegedb_size_bytes', column: 'collegedb_size_bytes' }]
	},
	{
		family: 'mysql',
		steps: [
			{
				sql: 'SELECT COALESCE(SUM(data_length + index_length), 0) AS collegedb_size_bytes FROM information_schema.tables WHERE table_schema = DATABASE()',
				column: 'collegedb_size_bytes'
			}
		]
	}
];

/**
 * Which sizing statement worked for a given provider, so the probe runs once.
 * @private
 */
const sizeQueryByDatabase = new WeakMap<SQLDatabase, SizeQuery>();

/**
 * Reads a named numeric column out of a result row.
 *
 * The column has to be named rather than positional. Some providers answer a
 * statement they do not understand with unrelated rows instead of raising, and
 * accepting the first number in such a row would report a confident wrong size
 * and silently mis-filter shards under `maxDatabaseSize`.
 * @private
 */
function namedNumeric(row: Record<string, unknown> | null, column: string): number | undefined {
	if (!row || !(column in row)) {
		return undefined;
	}

	const value = row[column];
	const numeric = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : NaN;
	return Number.isFinite(numeric) ? numeric : undefined;
}

/**
 * Runs one candidate sizing statement set and multiplies its results.
 * @private
 */
async function runSizeQuery(database: SQLDatabase, query: SizeQuery): Promise<number | undefined> {
	let product = 1;

	for (const step of query.steps) {
		const row = await database.prepare(step.sql).first<Record<string, unknown>>();
		const value = namedNumeric(row, step.column);
		if (value === undefined) {
			return undefined;
		}
		product *= value;
	}

	return product;
}

async function getDatabaseSize(database: SQLDatabase): Promise<number> {
	const known = sizeQueryByDatabase.get(database);
	if (known) {
		try {
			const size = await runSizeQuery(database, known);
			if (size !== undefined) {
				return size;
			}
		} catch {
			// the provider stopped answering the statement that used to work
		}
		sizeQueryByDatabase.delete(database);
	}

	const failures: string[] = [];

	for (const candidate of SIZE_QUERIES) {
		try {
			const size = await runSizeQuery(database, candidate);
			if (size !== undefined) {
				sizeQueryByDatabase.set(database, candidate);
				return size;
			}
			failures.push(`${candidate.family}: no ${candidate.steps.map((step) => step.column).join('/')} column`);
		} catch (error) {
			failures.push(`${candidate.family}: ${error instanceof Error ? error.message : 'unknown error'}`);
		}
	}

	throw new CollegeDBError(`Failed to get database size. Tried ${failures.join('; ')}`, 'SIZE_QUERY_FAILED');
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
		return hrwShard(primaryKey, availableShards);
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
	return hrwShard(
		primaryKey,
		bestShards.map((candidate) => candidate.shard)
	);
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
	const started = phaseStart();
	try {
		switch (effectiveStrategy) {
			case 'hash': {
				return hrwShard(primaryKey, availableShards);
			}
			case 'location': {
				if (!config.targetRegion) {
					return hrwShard(primaryKey, availableShards);
				}
				return selectShardByLocation(config.targetRegion, availableShards, config.shardLocations || {}, primaryKey);
			}
			case 'random': {
				return availableShards[Math.floor(Math.random() * availableShards.length)] || availableShards[0]!;
			}
			case 'round-robin': {
				const shard = availableShards[generatedInsertRoundRobinIndex % availableShards.length]!;
				generatedInsertRoundRobinIndex = (generatedInsertRoundRobinIndex + 1) % availableShards.length;
				return shard;
			}
			default: {
				return hrwShard(primaryKey, availableShards);
			}
		}
	} finally {
		phaseEnd('shard.select', started, effectiveStrategy);
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
/**
 * Decides whether placement is computed or read from KV, once per configuration.
 *
 * Computed placement is opt-in through `placement: 'computed'`. It is not the
 * default because it does not store mappings, and three shipped capabilities
 * read the keyspace back out of those mappings: `getShardStats` key counts,
 * `KVShardMapper.getKeysForShard`, and the migration helpers that enumerate
 * mapped keys. Under computed placement the assignment is implied by the hash
 * over a keyspace nobody enumerates, so those cannot answer. That is a
 * capability trade, not a latency one, and it belongs to the caller.
 *
 * Only the `hash` strategy can be computed. `round-robin` and `random` are not
 * functions of the key, and `location` depends on the requesting region rather
 * than the key, so any of them forces the KV path.
 *
 * @private
 */
async function resolvePlacement(config: CollegeDBConfig): Promise<{ mode: 'computed' | 'kv'; manifest: PlacementManifest | null }> {
	if (placementDecision) {
		return placementDecision;
	}

	const shards = Object.keys(config.shards);
	const strategyIsHash = resolveStrategy(config, 'read') === 'hash' && resolveStrategy(config, 'write') === 'hash';

	if (config.placement !== 'computed' || !strategyIsHash || shards.length === 0) {
		placementDecision = { mode: 'kv', manifest: null };
		return placementDecision;
	}

	try {
		const stored = await loadPlacementManifest(config.kv);
		const base = stored ?? createManifest(shards, 'hrw');
		const refreshed = withCurrentTopology(base, shards, 'hrw');

		if (!stored || refreshed !== base) {
			await savePlacementManifest(config.kv, refreshed);
		}

		placementDecision = { mode: 'computed', manifest: refreshed };
		return placementDecision;
	} catch (error) {
		if (config.debug) {
			console.warn('Placement manifest unavailable, falling back to KV mappings:', error);
		}
		placementDecision = { mode: 'kv', manifest: null };
		return placementDecision;
	}
}

/**
 * Every shard a key could be on, in resolution order.
 *
 * In computed mode the first entry is where a write goes and where a read looks
 * first, and any remaining entries are the epoch walk for keys placed under an
 * earlier topology. In KV mode the list is whatever the mapping says, or empty
 * when there is no mapping yet.
 *
 * @private
 */
async function resolveCandidates(primaryKey: string, operationType: OperationType = 'write'): Promise<string[]> {
	const config = getConfig();
	const decision = await resolvePlacement(config);

	if (decision.mode !== 'computed' || !decision.manifest) {
		return [await getShardForKey(primaryKey, operationType)];
	}

	const mapper = getMapper(config);
	const hashedKey = await mapper.hashKey(primaryKey);
	const exceptions = await loadPlacementExceptions(config.kv, decision.manifest.exceptionsVersion);

	if (exceptions.has(hashedKey)) {
		return [await getShardForKey(primaryKey, operationType)];
	}

	const candidates = candidateShards(primaryKey, decision.manifest, Object.keys(config.shards));
	return candidates.length > 0 ? candidates : [await getShardForKey(primaryKey, operationType)];
}

/**
 * Records that a key lives somewhere its placement function would not compute,
 * so later operations resolve it through KV instead of walking epochs.
 *
 * @private
 */
async function memoizePlacementException(primaryKey: string, shard: string): Promise<void> {
	const config = getConfig();
	const decision = await resolvePlacement(config);
	if (decision.mode !== 'computed' || !decision.manifest) {
		return;
	}

	const mapper = getMapper(config);

	try {
		await mapper.setShardMapping(primaryKey, shard);
		const updated = await addPlacementException(config.kv, decision.manifest, await mapper.hashKey(primaryKey));
		placementDecision = { mode: 'computed', manifest: updated };
	} catch (error) {
		if (config.debug) {
			console.warn(`Failed to record placement exception for ${primaryKey}:`, error);
		}
	}
}

async function getShardForKey(primaryKey: string, operationType: OperationType = 'write'): Promise<string> {
	const config = getConfig();
	const mapper = getMapper(config);

	// Check if mapping already exists
	const existingMapping = await mapper.getShardMapping(primaryKey);
	if (existingMapping) {
		return existingMapping.shard;
	}

	const selectedShard = await allocateShardForKey(config, primaryKey, operationType);

	// A read that finds no mapping does not need to leave one behind. Recording
	// it costs a KV write and pins a key that may have no row at all, which is
	// what a lookup for something that does not exist looks like.
	if (operationType === 'write' || config.allocateOnRead === true) {
		await mapper.setShardMapping(primaryKey, selectedShard);
	}

	return selectedShard;
}

/**
 * Picks the shard a not-yet-mapped key should live on, without recording it.
 *
 * Split out of {@link getShardForKey} so {@link batch} can allocate many keys and
 * then persist their mappings in one write, rather than one write per key,
 * while both paths keep the same coordinator, strategy and size-filter behavior.
 *
 * @private
 */
async function allocateShardForKey(config: CollegeDBConfig, primaryKey: string, operationType: OperationType): Promise<string> {
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
		const started = phaseStart();
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
		} finally {
			phaseEnd('coordinator.fetch', started, 'allocate');
		}
	} else {
		selectedShard = selectShardByStrategy(effectiveStrategy, primaryKey, eligibleShards, config);
	}

	return selectedShard;
}

/**
 * Gets the database instance for a primary key with operation-specific routing
 *
 * Resolves the primary key to its assigned shard and returns the corresponding
 * database instance. This function handles the complete routing process
 * from primary key to database connection, with support for different strategies
 * based on operation type.
 *
 * @private
 * @param primaryKey - The primary key to route
 * @param operationType - The type of operation (read/write) for mixed strategy support
 * @returns Promise resolving to the database instance
 * @throws {Error} If shard routing fails or database instance not found
 */
async function getDatabase(primaryKey: string, operationType: OperationType = 'write'): Promise<SQLDatabase> {
	const config = getConfig();
	const decision = placementDecision ?? (await resolvePlacement(config));
	const shard =
		decision.mode === 'computed' && decision.manifest
			? (await resolveCandidates(primaryKey, operationType))[0]!
			: await getShardForKey(primaryKey, operationType);
	const database = config.shards[shard];

	if (!database) {
		throw new CollegeDBError(`Shard ${shard} not found in configuration`, 'SHARD_NOT_FOUND');
	}

	return database;
}

/**
 * Creates the database schema in the specified shard
 *
 * @param db - The database instance to create schema in
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
export async function createSchema(db: SQLDatabase, schema: string): Promise<void> {
	const { createSchema: createSchemaImpl } = await import('./migrations');
	await createSchemaImpl(db, schema);
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
 * Runs a read against each shard a key could be on, newest placement first,
 * and returns the first non-empty answer.
 *
 * With one candidate this is a single query, which is the steady state. More
 * than one only happens after a shard is added or removed, or while a
 * deployment still holds keys placed by an earlier algorithm: the row is where
 * an older epoch put it, so it is found there and then recorded as an exception
 * so the walk never repeats for that key.
 *
 * @private
 */
async function readAcrossCandidates<T>(
	key: string,
	sql: string,
	bindings: any[],
	execute: (database: SQLDatabase) => Promise<T>,
	isEmpty: (value: T) => boolean
): Promise<T> {
	const config = getConfig();
	const operationType = getOperationType(sql);

	// KV placement resolves to exactly one shard, which is the steady state for
	// every deployment that has not opted into computed placement. Taking it
	// here skips the candidate array, the epoch walk and the empty-result
	// bookkeeping below, none of which can do anything with one candidate.
	const decision = placementDecision ?? (await resolvePlacement(config));
	if (decision.mode !== 'computed' || !decision.manifest) {
		const binding = await getShardForKey(key, operationType);
		const database = config.shards[binding];
		if (!database) {
			throw new CollegeDBError(`Shard ${binding} not found in configuration`, 'SHARD_NOT_FOUND');
		}

		return await execute(database);
	}

	const candidates = await resolveCandidates(key, operationType);

	let firstResult: T | undefined;

	for (let i = 0; i < candidates.length; i++) {
		const binding = candidates[i]!;
		const database = config.shards[binding];
		if (!database) continue;

		const result = await execute(database);

		if (!isEmpty(result)) {
			if (i > 0) {
				await memoizePlacementException(key, binding);
			}
			return result;
		}

		if (firstResult === undefined) {
			firstResult = result;
		}
	}

	if (firstResult !== undefined) {
		return firstResult;
	}

	const fallback = config.shards[candidates[0] ?? ''];
	if (!fallback) {
		throw new CollegeDBError(`Shard ${candidates[0] ?? '<none>'} not found in configuration`, 'SHARD_NOT_FOUND');
	}

	return await execute(fallback);
}

/**
 * Executes a statement on the appropriate shard based on the primary key.
 * The primary key is used to determine which shard should store the record,
 * ensuring consistent routing for future queries.
 *
 * Use this helper when your application already knows the routing key before
 * issuing the write. For database-generated primary keys, use {@link insert}
 * so the returned generated id can be captured and reused for follow-up reads.
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
 * Result returned by {@link insert} and {@link insertShard}.
 *
 * The helper keeps the normal query payload but also exposes the generated
 * primary key when the backend returns one through `RETURNING` rows or
 * provider metadata.
 *
 * @since 1.1.4
 */
export interface InsertResult<T = Record<string, unknown>> extends QueryResult<T> {
	/** Generated primary key returned by the database or driver. */
	generatedId: number | string;
}

function extractGeneratedId<T = Record<string, unknown>>(result: QueryResult<T>, idColumn?: string): number | string | undefined {
	const firstRow = result.results[0] as Record<string, unknown> | undefined;
	if (firstRow && typeof firstRow === 'object') {
		// An explicitly named column wins outright, and its absence is an error
		// rather than a reason to start guessing.
		if (idColumn) {
			const value = firstRow[idColumn];
			return value === undefined || value === null ? undefined : (value as number | string);
		}

		// Prefer explicit RETURNING rows over provider metadata when available.
		for (const key of ['id', 'ID', 'Id', 'rowid', 'ROWID', 'RowId', 'last_row_id', 'lastInsertId', 'insertId']) {
			const value = firstRow[key];
			if (value !== undefined && value !== null) {
				return value as number | string;
			}
		}

		for (const [key, value] of Object.entries(firstRow)) {
			const lowerKey = key.toLowerCase();
			if ((lowerKey === 'id' || lowerKey === 'rowid') && (typeof value === 'number' || typeof value === 'string')) {
				return value as number | string;
			}
		}

		// Deliberately no "first scalar column" fallback, and deliberately no
		// fall-through to `meta.last_row_id` either. The statement returned a row
		// and none of its columns is an id, so the caller named columns that do
		// not include the primary key. `last_row_id` there is the backend's
		// internal rowid, which for a TEXT primary key is a different value than
		// the key, and routing on it puts the row somewhere no reader looks.
		return undefined;
	}

	const metaId = result.meta.last_row_id;
	if (metaId !== undefined && metaId !== null) {
		return metaId;
	}

	return undefined;
}

function createInsertAllocatorKey(): string {
	return `insert:${Date.now()}:${Math.random().toString(36).slice(2)}`;
}

async function allocateInsertShard(): Promise<string> {
	const config = getConfig();
	const availableShards = Object.keys(config.shards);

	if (availableShards.length === 0) {
		throw new CollegeDBError('No shards configured', 'NO_SHARDS');
	}

	const eligibleShards = await filterShardsBySize(availableShards, config);
	if (eligibleShards.length === 0) {
		throw new CollegeDBError('No shards available for insert', 'NO_SHARDS');
	}

	const effectiveStrategy = resolveStrategy(config, 'write');
	const allocatorKey = createInsertAllocatorKey();

	if (config.coordinator) {
		try {
			const coordinatorId = config.coordinator.idFromName('default');
			const coordinator = config.coordinator.get(coordinatorId);

			const response = await coordinator.fetch('http://coordinator/allocate', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					primaryKey: allocatorKey,
					strategy: effectiveStrategy,
					operationType: 'write',
					targetRegion: config.targetRegion,
					shardLocations: config.shardLocations,
					availableShards: eligibleShards
				})
			});

			if (response.ok) {
				const result = (await response.json()) as { shard: string };
				return result.shard;
			}
		} catch (error) {
			console.warn('Coordinator allocation for insert failed, falling back to local strategy:', error);
		}
	}

	return selectShardByStrategy(effectiveStrategy, allocatorKey, eligibleShards, config);
}

async function executeInsertOnShard<T = Record<string, unknown>>(
	shardBinding: string,
	sql: string,
	bindings: any[] = [],
	options: IdColumnOptions = {}
): Promise<InsertResult<T>> {
	const config = getConfig();
	if (!config.shards[shardBinding]) {
		throw new CollegeDBError(`Shard ${shardBinding} not found`, 'SHARD_NOT_FOUND');
	}

	const returning = /\breturning\b/i.test(sql);
	const result = returning ? await allShard<T>(shardBinding, sql, bindings) : await runShard<T>(shardBinding, sql, bindings);
	const generatedId = extractGeneratedId(result, options.idColumn);

	if (generatedId === undefined) {
		throw new CollegeDBError(
			options.idColumn
				? `Insert did not return a value for the id column "${options.idColumn}"`
				: 'Insert did not return a generated primary key. Pass { idColumn } when the primary key is not named id or rowid.',
			'GENERATED_KEY_UNAVAILABLE'
		);
	}

	const mapper = getMapper(config);
	const idKey = String(generatedId);

	// Each shard mints its own sequence, so two shards both hand out 1, then 2,
	// and so on. Storing the second mapping would overwrite the first and leave
	// the earlier row on a shard nothing routes to. Refusing here turns silent
	// unreachability into an error at the point the collision happens.
	//
	// One shard has no second sequence to collide with, so the lookup is skipped
	// there rather than spending a KV read per insert to rule out a collision
	// that cannot occur.
	const existing = Object.keys(config.shards).length > 1 ? await mapper.getShardMapping(idKey) : null;
	if (existing && existing.shard !== shardBinding) {
		throw new CollegeDBError(
			`Generated id ${idKey} is already mapped to shard ${existing.shard}, but this insert ran on ${shardBinding}. ` +
				'Per-shard AUTOINCREMENT and SERIAL sequences repeat across shards, so a database-generated id is not unique ' +
				'cluster-wide. Use nextId() to allocate a cluster-unique id and pass it explicitly, or confine the table to one ' +
				'shard with insertShard().',
			'GENERATED_KEY_COLLISION'
		);
	}

	await mapper.setShardMapping(idKey, shardBinding);

	return {
		...result,
		generatedId
	};
}

/**
 * Executes an insert on an automatically selected shard and returns the generated primary key.
 *
 * This is the default helper for generated-key tables. CollegeDB picks a shard
 * using the configured allocation strategy, then stores the generated primary
 * key -> shard mapping so routed reads can find the row later.
 *
 * **A database-generated id is only unique within its own shard.** Every shard
 * runs its own `AUTOINCREMENT` or `SERIAL` sequence, so spreading a
 * generated-key table across shards eventually mints the same id twice. When
 * that happens this throws `GENERATED_KEY_COLLISION` rather than overwriting the
 * first mapping and stranding its row. For a generated-key table that spans
 * shards, allocate the id with {@link nextId} and pass it explicitly; to keep
 * using the database's own sequence, confine the table to one shard with
 * {@link insertShard}.
 *
 * @template T - Type of returned rows when the insert uses `RETURNING`
 * @param sql - The INSERT statement to execute
 * @param bindings - Parameter values to bind to the statement
 * @returns Promise resolving to the write result plus the generated id
 * @throws {CollegeDBError} If the insert succeeds but no generated id can be determined
 * @since 1.1.4
 * @example
 * ```typescript
 * const created = await insert(
 *   'INSERT INTO auto_users (name, email) VALUES (?, ?)',
 *   ['Ada', 'ada@example.com']
 * );
 *
 * const row = await first(String(created.generatedId), 'SELECT * FROM auto_users WHERE id = ?', [created.generatedId]);
 * ```
 */
export async function insert<T = Record<string, unknown>>(
	sql: string,
	bindings: any[] = [],
	options: IdColumnOptions = {}
): Promise<InsertResult<T>> {
	const shardBinding = await allocateInsertShard();
	return await executeInsertOnShard<T>(shardBinding, sql, bindings, options);
}

/**
 * Executes an insert directly on a named shard and returns the generated primary key.
 *
 * Use this helper when you already know the shard you want to target.
 * The helper still captures the generated id and stores the mapping so routed
 * reads can find the new row later.
 *
 * @template T - Type of returned rows when the insert uses `RETURNING`
 * @param shardBinding - The shard binding to execute the insert on
 * @param sql - The INSERT statement to execute
 * @param bindings - Parameter values to bind to the statement
 * @returns Promise resolving to the write result plus the generated id
 * @throws {CollegeDBError} If the insert succeeds but no generated id can be determined
 * @since 1.1.4
 * @example
 * ```typescript
 * const created = await insertShard('db-east',
 *   'INSERT INTO auto_users (name, email, created_at) VALUES (?, ?, ?)',
 *   ['Ada', 'ada@example.com', Date.now()]
 * );
 *
 * console.log(created.generatedId);
 * ```
 * @example
 * ```typescript
 * const created = await insertShard('db-east',
 *   'INSERT INTO auto_users (name, email) VALUES (?, ?) RETURNING id',
 *   ['Ada', 'ada@example.com']
 * );
 * ```
 */
export async function insertShard<T = Record<string, unknown>>(
	shardBinding: string,
	sql: string,
	bindings: any[] = [],
	options: IdColumnOptions = {}
): Promise<InsertResult<T>> {
	return await executeInsertOnShard<T>(shardBinding, sql, bindings, options);
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
	const result = await readAcrossCandidates<QueryResult<T>>(
		key,
		sql,
		bindings,
		(database) =>
			database
				.prepare(sql)
				.bind(...bindings)
				.all<T>(),
		(value) => value.success && value.results.length === 0
	);

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
	return await readAcrossCandidates<T | null>(
		key,
		sql,
		bindings,
		(database) =>
			database
				.prepare(sql)
				.bind(...bindings)
				.first<T>(),
		(value) => value === null
	);
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
 * Drops cached shard mappings so the next read resolves them from KV again.
 *
 * CollegeDB caches a key's shard in memory for `mappingCacheTtlMs`, which makes
 * that TTL a staleness window: a mapping changed elsewhere, by another isolate
 * or by a {@link KVShardMapper} built directly, is not visible here until the
 * entry expires. {@link reassignShard} already clears what it changes, so this
 * is for changes made outside CollegeDB's own routing.
 *
 * Re-running {@link initialize} is not a substitute. It reuses the existing
 * mapper when the bindings and options are unchanged, precisely so the cache
 * survives the per-request initialization the Workers examples use.
 *
 * @param key - Logical key to drop, or omit to drop every cached mapping
 * @throws {CollegeDBError} If CollegeDB is not initialized
 * @since 1.4.0
 * @example
 * ```typescript
 * const mapper = new KVShardMapper(env.KV, { hashShardMappings: true });
 * await mapper.updateShardMapping('user-123', 'db-west');
 *
 * invalidateMappingCache('user-123'); // this process now reads the new shard
 * ```
 */
export async function invalidateMappingCache(key?: string): Promise<void> {
	const mapper = getMapper(getConfig());

	if (key === undefined) {
		mapper.clearMappingCache();
		return;
	}

	await mapper.invalidateCachedMapping(key);
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
	const decision = await resolvePlacement(config);

	// Under computed placement there is no stored mapping to read, so the current
	// shard comes from the placement function instead of being an error.
	let currentShard = currentMapping?.shard;
	if (!currentShard && decision.mode === 'computed' && decision.manifest) {
		currentShard = candidateShards(primaryKey, decision.manifest, Object.keys(config.shards))[0];
	}

	if (!currentShard) {
		throw new CollegeDBError(`No existing mapping found for primary key: ${primaryKey}`, 'MAPPING_NOT_FOUND');
	}

	// Migrate data if different shard
	if (currentShard !== newBinding) {
		const { migrateRecord } = await import('./migrations');
		const sourceDb = config.shards[currentShard];
		const targetDb = config.shards[newBinding];

		if (!sourceDb || !targetDb) {
			throw new CollegeDBError('Source or target shard not available', 'SHARD_UNAVAILABLE');
		}

		await migrateRecord(sourceDb, targetDb, primaryKey, tableName);
	}

	// Update mapping. A key moved off the shard its placement function computes
	// has to be recorded as an exception, or the next read would look at the
	// computed shard and find nothing there.
	if (currentMapping) {
		await mapper.updateShardMapping(primaryKey, newBinding);
	} else {
		await mapper.setShardMapping(primaryKey, newBinding);
	}

	if (decision.mode === 'computed' && decision.manifest) {
		placementDecision = {
			mode: 'computed',
			manifest: await addPlacementException(config.kv, decision.manifest, await mapper.hashKey(primaryKey))
		};
	}

	// Drop any coordinator-recorded allocation so a later first touch of this key
	// is not handed the shard it has just been moved off.
	await forgetCoordinatorAllocation(config, primaryKey);

	await mapper.invalidateCachedMapping(primaryKey);
}

/**
 * Asks the coordinator to forget a recorded allocation, ignoring failures.
 *
 * Best effort on purpose: the KV mapping is authoritative, so a coordinator
 * that is unreachable must not fail a reassignment.
 *
 * @private
 */
async function forgetCoordinatorAllocation(config: CollegeDBConfig, primaryKey: string): Promise<void> {
	if (!config.coordinator) {
		return;
	}

	try {
		const coordinatorId = config.coordinator.idFromName('default');
		const coordinator = config.coordinator.get(coordinatorId);
		await coordinator.fetch('http://coordinator/forget', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ primaryKey })
		});
	} catch (error) {
		if (config.debug) {
			console.warn(`Coordinator did not forget the allocation for ${primaryKey}:`, error);
		}
	}
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
 * Runs an all-shards query whose SQL is built per shard.
 *
 * `allAllShards` takes one statement for every shard, which cannot be right
 * when the shards are different backends: a generated statement has to quote
 * identifiers the way its own target expects. This builds the statement once
 * per shard from that shard's dialect.
 *
 * @private
 */
async function allAllShardsBuilt<T = Record<string, unknown>>(
	build: (dialect: SQLDialect | undefined) => string,
	bindings: any[] = [],
	batchSize: number = 50
): Promise<QueryResult<T>[]> {
	const config = getConfig();
	const tasks: Array<() => Promise<QueryResult<T>>> = [];

	// One statement per distinct dialect rather than one per shard. A cluster
	// that does not mix vendors builds it once.
	const byDialect = new Map<SQLDialect | undefined, string>();
	const statementFor = (dialect: SQLDialect | undefined): string => {
		let sql = byDialect.get(dialect);
		if (sql === undefined) {
			sql = build(dialect);
			byDialect.set(dialect, sql);
		}
		return sql;
	};

	for (const [binding, db] of Object.entries(config.shards)) {
		if (!binding || !db) {
			console.error(`Shard ${binding ?? '<null>'} not found, skipping`);
			continue;
		}

		tasks.push(() =>
			db
				.prepare(statementFor(db.dialect))
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
		out.push(...(await Promise.all(tasks.slice(i, i + batchSize).map((fn) => fn()))));
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
	/**
	 * When `true`, record the total number of rows that matched (after `filter`,
	 * before `offset`/`limit`) on `meta.total`. Useful for paginated UIs.
	 * @since 1.2.4
	 */
	includeTotal?: boolean;
}

/**
 * Returns a function that quotes `table` for a given shard.
 *
 * MySQL and MariaDB reject ANSI double quotes, so a cross-shard helper has to
 * quote per shard rather than once. Almost no cluster actually mixes vendors,
 * so the quoted form is resolved once here and the per-shard branch only runs
 * for the clusters that need it.
 *
 * @private
 */
function tableQuoter(config: CollegeDBConfig, table: string): (db: SQLDatabase) => string {
	let dialect: SQLDialect | undefined;
	let seen = false;

	for (const db of Object.values(config.shards)) {
		if (!db) continue;
		if (!seen) {
			dialect = db.dialect;
			seen = true;
		} else if (db.dialect !== dialect) {
			return (shard) => quoteIdentifier(table, shard.dialect);
		}
	}

	const quoted = quoteIdentifier(table, dialect);
	return () => quoted;
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
/**
 * Bounds each shard's result set when the global page can be satisfied from the
 * top `offset + limit` rows of every shard.
 *
 * Without this, a global page pulls every matching row from every shard into
 * one isolate and then throws almost all of them away, which on Workers runs
 * into the 128 MB isolate ceiling long before the query is slow. With it, each
 * shard returns at most as many rows as the page could possibly need.
 *
 * Deliberately conservative. The rewrite only applies when the caller sorts by
 * a column name that SQL can order by, is not filtering in JavaScript, is not
 * asking for a total, and the statement carries no `LIMIT`, `OFFSET`, or set
 * operator of its own. Anything else keeps the original statement, because a
 * JavaScript `filter` or `comparator` can promote a row this rewrite would have
 * discarded, and `includeTotal` has to count rows the page does not contain.
 *
 * @private
 */
function pushDownGlobalLimit<T>(
	sql: string,
	options: GlobalAllShardsOptions<T>,
	offset: number,
	limit: number | undefined,
	dialect?: SQLDialect
): string {
	if (limit === undefined || options.filter || options.comparator || options.includeTotal) {
		return sql;
	}

	if (typeof options.sortBy !== 'string' && options.sortBy !== undefined) {
		return sql;
	}

	const upper = sql.toUpperCase();
	if (/\bLIMIT\b|\bOFFSET\b|\bUNION\b|\bINTERSECT\b|\bEXCEPT\b/.test(upper)) {
		return sql;
	}

	if (!upper.trimStart().startsWith('SELECT')) {
		return sql;
	}

	// Each shard only has to surrender enough rows to fill the page, since the
	// merge takes the globally best `offset + limit` and no shard can contribute
	// more than that.
	const perShard = offset + limit;
	const trimmed = sql.replace(/;\s*$/, '');

	if (typeof options.sortBy === 'string') {
		const direction = options.sortDirection === 'desc' ? 'DESC' : 'ASC';
		const alreadyOrdered = /\bORDER\s+BY\b/.test(upper);
		if (!alreadyOrdered) {
			return `${trimmed} ORDER BY ${quoteIdentifier(options.sortBy, dialect)} ${direction} LIMIT ${perShard}`;
		}
	}

	return `${trimmed} LIMIT ${perShard}`;
}

export async function allAllShardsGlobal<T = Record<string, unknown>>(
	sql: string,
	bindings: any[] = [],
	options: GlobalAllShardsOptions<T> = {}
): Promise<QueryResult<T>> {
	const batchSize = normalizeBatchSize(options.batchSize);
	const offset = normalizeOffset(options.offset);
	const limit = normalizeLimit(options.limit);

	const merged = mergeAllShardQueryResults(
		await allAllShardsBuilt<T>((dialect) => pushDownGlobalLimit(sql, options, offset, limit, dialect), bindings, batchSize)
	);
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

	const total = rows.length;
	const end = limit === undefined ? undefined : offset + limit;
	const pagedRows = rows.slice(offset, end);

	return {
		...merged,
		results: pagedRows,
		meta: options.includeTotal ? { ...merged.meta, total } : merged.meta
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
	// The known-shard registration is memoized per process, so a flush has to
	// let the next initialize re-register rather than assume it already did.
	syncedShardSets.clear();

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
 * Gets the size of a specific shard's database in bytes.
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

function quoteIdentifier(identifier: string, dialect?: SQLDialect): string {
	const quote = dialect === 'mysql' ? '`' : '"';
	return validateIdentifier(identifier)
		.map((part) => `${quote}${part}${quote}`)
		.join('.');
}

/**
 * Dialect of a named shard, or `undefined` when the provider does not say.
 *
 * Statements CollegeDB generates itself have to quote identifiers the way the
 * target backend expects: MySQL and MariaDB reject ANSI double quotes unless
 * `ANSI_QUOTES` is set, so a generated `SELECT MAX("id") FROM "t"` fails there
 * outright.
 *
 * @private
 */
function dialectOf(binding: string | undefined): SQLDialect | undefined {
	if (!binding) {
		return undefined;
	}
	return globalConfig?.shards[binding]?.dialect;
}

/**
 * Dialect of the shard a key routes to.
 * @private
 */
async function dialectForKey(key: string, operationType: OperationType = 'write'): Promise<SQLDialect | undefined> {
	const candidates = await resolveCandidates(key, operationType);
	return dialectOf(candidates[0]);
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
	options: CreateIndexOptions = {},
	dialect?: SQLDialect
): string {
	const normalizedColumns = normalizeIndexColumns(columns);
	const quotedTable = quoteIdentifier(table, dialect);
	const generatedIndexName = options.indexName
		? options.indexName
		: ['idx', normalizeIndexNameSegment(table), ...normalizedColumns.map((column) => normalizeIndexNameSegment(column.name))]
				.filter(Boolean)
				.join('_')
				.slice(0, 120);
	const quotedIndexName = quoteIdentifier(generatedIndexName || 'idx_auto', dialect);

	const columnClauses = normalizedColumns
		.map((column) => {
			const quotedColumn = quoteIdentifier(column.name, dialect);
			const order = column.order ? ` ${column.order}` : '';
			// A collation is a bare name, not a quoted identifier.
			const collate = column.collate ? ` COLLATE ${validateIdentifier(column.collate).join('.')}` : '';
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
	const sql = buildCreateIndexSQL(table, columns, options, await dialectForKey(key));
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
	const sql = buildCreateIndexSQL(table, columns, options, dialectOf(shardBinding));
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
	const config = getConfig();
	const batchSize = normalizeBatchSize(options.batchSize);
	const tasks = Object.entries(config.shards)
		.filter(([binding, db]) => binding && db)
		.map(([binding, db]) => async () => {
			const sql = buildCreateIndexSQL(table, columns, options, db.dialect);
			return await runShard<T>(binding, sql);
		});

	const out: QueryResult<T>[] = [];
	for (let i = 0; i < tasks.length; i += batchSize) {
		out.push(...(await Promise.all(tasks.slice(i, i + batchSize).map((fn) => fn()))));
	}
	return out;
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
	const quotedTable = quoteIdentifier(table, await dialectForKey(key, 'read'));
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
	const quotedTable = quoteIdentifier(table, dialectOf(shardBinding));
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
	const tasks: Array<() => Promise<ShardTableCount>> = [];
	const quoter = tableQuoter(config, table);

	for (const [binding, db] of Object.entries(config.shards)) {
		if (!binding || !db) {
			continue;
		}

		const probeSql = `SELECT COUNT(*) AS row_count FROM ${quoter(db)}`;

		tasks.push(async () => {
			try {
				const row = await db.prepare(probeSql).first<{ row_count?: number | string }>();
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

/**
 * Reads the first matching row via key routing, falling back to a global
 * all-shards scan when the routed read misses.
 *
 * The routed read stays the fast path. The fallback exists for the window
 * where a primary-key -> shard mapping has not been created yet (a brand-new
 * key, an eventually-consistent KV, or a row written directly to a shard), so
 * a single lookup still resolves without the caller wiring their own fanout.
 *
 * @template T - Type of the result record
 * @param key - Primary key used for routing the fast-path read
 * @param sql - SQL statement to execute
 * @param bindings - Parameter values to bind to the SQL statement
 * @returns The first matching row, or `null` when neither path finds one
 * @since 1.2.4
 * @example
 * ```typescript
 * const user = await firstResilient<User>('user-123', 'SELECT * FROM users WHERE id = ?', ['user-123']);
 * ```
 */
export async function firstResilient<T = Record<string, unknown>>(key: string, sql: string, bindings: any[] = []): Promise<T | null> {
	const routed = await first<T>(key, sql, bindings);
	if (routed) {
		return routed;
	}

	return (await firstAllShardsGlobal<T>(sql, bindings)) ?? null;
}

/**
 * Executes a statement, taking the routing key from the statement itself.
 *
 * This is {@link run} without the duplicated key argument. The planner reads
 * the primary key out of the statement, so
 * `query('INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Ada'])`
 * routes exactly as `run('user-1', ...)` would.
 *
 * Recognized shapes are `INSERT INTO t (cols) VALUES (...)` including multi-row
 * inserts, and `UPDATE`/`DELETE`/`SELECT` whose entire `WHERE` clause is
 * `key = ?` or `key IN (?, ?, ...)`. The key column comes from `keyColumns` in
 * the configuration and defaults to `id`.
 *
 * A statement whose key cannot be proven is not guessed at. By default it
 * throws and names the explicit-key alternative; set `onUnroutable: 'fanout'`
 * to run it on every shard instead.
 *
 * A statement that resolves to several keys is grouped by shard and executed
 * with one round trip per shard, so `WHERE id IN (...)` spanning three shards is
 * three statements rather than one per id.
 *
 * @template T - Type of the result records
 * @param sql - Statement text using `?` placeholders
 * @param bindings - Positional bindings for the statement
 * @returns The result, merged across shards when the statement routes to several
 * @throws {CollegeDBError} If the routing key cannot be determined and `onUnroutable` is `throw`
 * @since 1.4.0
 * @example
 * ```typescript
 * await query('INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Ada']);
 * await query('UPDATE users SET name = ? WHERE id = ?', ['Ada L.', 'user-1']);
 * await query('DELETE FROM users WHERE id IN (?, ?)', ['user-1', 'user-2']);
 * ```
 */
export async function query<T = Record<string, unknown>>(sql: string, bindings: any[] = []): Promise<QueryResult<T>> {
	const config = getConfig();
	const plan = planQuery(sql, bindings, { keyColumns: config.keyColumns });

	if (!plan) {
		if ((config.onUnroutable ?? 'throw') === 'throw') {
			throw unroutableError(sql);
		}
		return mergeAllShardQueryResults(await allAllShards<T>(sql, bindings));
	}

	if (plan.keys.length === 1) {
		return plan.readOnly ? await all<T>(plan.keys[0]!, sql, bindings) : await run<T>(plan.keys[0]!, sql, bindings);
	}

	const grouped = await batch<T>(plan.keys.map((key) => ({ key, sql, bindings })));
	return mergeAllShardQueryResults(grouped.flatMap((group) => group.results));
}

/**
 * Reads the first matching row, taking the routing key from the statement.
 *
 * @template T - Type of the result record
 * @param sql - Statement text using `?` placeholders
 * @param bindings - Positional bindings for the statement
 * @returns The first matching row, or `null`
 * @throws {CollegeDBError} If the routing key cannot be determined and `onUnroutable` is `throw`
 * @since 1.4.0
 * @example
 * ```typescript
 * const user = await queryFirst<User>('SELECT * FROM users WHERE id = ?', ['user-1']);
 * ```
 */
export async function queryFirst<T = Record<string, unknown>>(sql: string, bindings: any[] = []): Promise<T | null> {
	const config = getConfig();
	const plan = planQuery(sql, bindings, { keyColumns: config.keyColumns });

	if (!plan) {
		if ((config.onUnroutable ?? 'throw') === 'throw') {
			throw unroutableError(sql);
		}
		const rows = await firstAllShards<T>(sql, bindings);
		return rows.find((row): row is T => row !== null) ?? null;
	}

	if (plan.keys.length === 1) {
		return await first<T>(plan.keys[0]!, sql, bindings);
	}

	for (const key of plan.keys) {
		const row = await first<T>(key, sql, bindings);
		if (row !== null) {
			return row;
		}
	}

	return null;
}

/**
 * Reads every matching row, taking the routing key from the statement.
 *
 * @template T - Type of the result records
 * @param sql - Statement text using `?` placeholders
 * @param bindings - Positional bindings for the statement
 * @returns Rows from every shard the statement routes to
 * @throws {CollegeDBError} If the routing key cannot be determined and `onUnroutable` is `throw`
 * @since 1.4.0
 * @example
 * ```typescript
 * const { results } = await queryAll<User>('SELECT * FROM users WHERE id IN (?, ?)', ['user-1', 'user-2']);
 * ```
 */
export async function queryAll<T = Record<string, unknown>>(sql: string, bindings: any[] = []): Promise<QueryResult<T>> {
	const config = getConfig();
	const plan = planQuery(sql, bindings, { keyColumns: config.keyColumns });

	if (!plan) {
		if ((config.onUnroutable ?? 'throw') === 'throw') {
			throw unroutableError(sql);
		}
		return mergeAllShardQueryResults(await allAllShards<T>(sql, bindings));
	}

	if (plan.keys.length === 1) {
		return await all<T>(plan.keys[0]!, sql, bindings);
	}

	const perKey = await Promise.all(plan.keys.map((key) => all<T>(key, sql, bindings)));
	return mergeAllShardQueryResults(perKey);
}

/**
 * One routed statement in a {@link batch}.
 * @since 1.4.0
 */
export interface BatchEntry {
	/** Primary key used to choose the shard */
	key: string;
	/** SQL text using `?` placeholders */
	sql: string;
	/** Positional bindings for the statement */
	bindings?: any[];
}

/**
 * Result of a routed {@link batch}, grouped by the shard that ran it.
 *
 * There is no combined success flag on purpose: a batch spanning three shards is
 * three independent transactions, so "did it work" is a per-shard question.
 * @since 1.4.0
 */
export interface BatchShardResult<T = Record<string, unknown>> {
	/** Shard the statements ran against */
	shard: string;
	/** Indices into the original `entries` array, in execution order */
	indices: number[];
	/** Result per statement, in the same order as `indices` */
	results: QueryResult<T>[];
	/** Set when the whole group failed before producing per-statement results */
	error?: string;
}

/**
 * Executes many routed statements with one round trip per shard.
 *
 * Every statement is resolved to a shard, grouped with the others that landed on
 * the same shard, and the groups are executed concurrently. A shard whose
 * provider implements {@link SQLDatabase.runBatch} runs its group in a single
 * call; the rest fall back to sequential statements, which is what a loop of
 * {@link run} did before.
 *
 * On D1 this is the difference between one HTTP round trip per statement and one
 * per shard, and Cloudflare caps a Worker invocation at 1000 D1 queries on the
 * paid plan and 50 on the free plan, so a few hundred single-row writes is not
 * merely slow there but impossible.
 *
 * **There is no cross-shard atomicity.** Statements sharing a shard share that
 * shard's transaction and execute in submission order. Statements on different
 * shards do not, so a batch can leave one shard updated and another not. When
 * that matters, key the whole unit of work to one shard.
 *
 * @template T - Type of returned rows
 * @param entries - Routed statements to execute
 * @returns One entry per shard that ran statements
 * @throws {CollegeDBError} If CollegeDB is not initialized
 * @since 1.4.0
 * @example
 * ```typescript
 * const results = await batch([
 * 	{ key: 'user-1', sql: 'INSERT INTO users (id, name) VALUES (?, ?)', bindings: ['user-1', 'Ada'] },
 * 	{ key: 'user-2', sql: 'INSERT INTO users (id, name) VALUES (?, ?)', bindings: ['user-2', 'Grace'] }
 * ]);
 *
 * for (const group of results) {
 * 	console.log(`${group.shard} ran ${group.results.length} statements`);
 * }
 * ```
 */
/**
 * Resolves every entry in a batch to a shard, in submission order.
 *
 * Routing a batch one key at a time costs a KV read per key and a KV write per
 * new key, which for a few hundred statements is more round trips than the
 * statements themselves. The mappings that exist are read together, and the
 * mappings that have to be created are written together.
 *
 * Allocation itself stays per key: a coordinator has to see each one, and the
 * strategies that are not functions of the key depend on call order.
 *
 * @private
 */
async function resolveBatchShards(config: CollegeDBConfig, entries: BatchEntry[]): Promise<string[]> {
	const decision = placementDecision ?? (await resolvePlacement(config));

	// Computed placement reads no mappings at all, and the epoch walk is per key.
	if (decision.mode === 'computed' && decision.manifest) {
		return await Promise.all(entries.map((entry) => getShardForKey(entry.key, getOperationType(entry.sql))));
	}

	const mapper = getMapper(config);
	const known = await mapper.getShardMappings(entries.map((entry) => entry.key));

	const shards: string[] = new Array(entries.length);
	const created: Array<{ primaryKey: string; shard: string }> = [];
	const allocated = new Map<string, string>();

	for (let i = 0; i < entries.length; i++) {
		const entry = entries[i]!;
		const mapping = known.get(entry.key);

		if (mapping) {
			shards[i] = mapping.shard;
			continue;
		}

		// A key can appear more than once in one batch; it must not be allocated
		// twice or the second decision would overwrite the first.
		const already = allocated.get(entry.key);
		if (already !== undefined) {
			shards[i] = already;
			continue;
		}

		const operationType = getOperationType(entry.sql);
		const shard = await allocateShardForKey(config, entry.key, operationType);
		shards[i] = shard;
		allocated.set(entry.key, shard);

		if (operationType === 'write' || config.allocateOnRead === true) {
			created.push({ primaryKey: entry.key, shard });
		}
	}

	if (created.length > 0) {
		await mapper.setShardMappings(created);
	}

	return shards;
}

export async function batch<T = Record<string, unknown>>(entries: BatchEntry[]): Promise<BatchShardResult<T>[]> {
	const config = getConfig();

	if (entries.length === 0) {
		return [];
	}

	const groups = new Map<string, number[]>();
	const shards = await resolveBatchShards(config, entries);

	shards.forEach((shard, index) => {
		const existing = groups.get(shard);
		if (existing) {
			existing.push(index);
		} else {
			groups.set(shard, [index]);
		}
	});

	return await Promise.all(
		[...groups.entries()].map(async ([shard, indices]): Promise<BatchShardResult<T>> => {
			const database = config.shards[shard];
			if (!database) {
				return { shard, indices, results: [], error: `Shard ${shard} not found in configuration` };
			}

			const statements = indices.map((index) => {
				const entry = entries[index]!;
				return { sql: entry.sql, bindings: entry.bindings ?? [] };
			});

			try {
				if (database.runBatch) {
					return { shard, indices, results: await database.runBatch<T>(statements) };
				}

				const results: QueryResult<T>[] = [];
				for (const statement of statements) {
					results.push(
						await database
							.prepare(statement.sql)
							.bind(...statement.bindings)
							.run<T>()
					);
				}
				return { shard, indices, results };
			} catch (error) {
				return { shard, indices, results: [], error: error instanceof Error ? error.message : String(error) };
			}
		})
	);
}

/**
 * Options for {@link paginate}.
 * @since 1.2.4
 */
export interface PaginateOptions<T = Record<string, unknown>> extends Omit<GlobalAllShardsOptions<T>, 'offset' | 'includeTotal'> {
	/** 1-based page number (default: 1) */
	page?: number;
}

/**
 * A page of rows plus the metadata needed to render pagination controls.
 * @since 1.2.4
 */
export interface PaginatedResult<T = Record<string, unknown>> {
	/** Rows for the requested page */
	results: T[];
	/** Total rows across all shards that matched (after `filter`, before paging) */
	total: number;
	/** The 1-based page number that was returned */
	page: number;
	/** The page size that was applied */
	limit: number;
	/** Total number of pages for `total`/`limit` */
	pages: number;
}

/**
 * Runs a query across all shards and returns a single page plus the total
 * match count.
 *
 * `allAllShardsGlobal` already merges, filters, sorts, and slices across shards
 * but discards the pre-slice count. `paginate` keeps that count so list
 * endpoints can return `{ results, total, page, limit, pages }` for a UI in one
 * call instead of issuing a second COUNT query.
 *
 * @template T - Type of the result records
 * @param sql - SQL statement to execute on each shard
 * @param bindings - Parameter values to bind to the SQL statement
 * @param options - Sort/filter options plus `page` and `limit`
 * @returns The requested page and pagination metadata
 * @since 1.2.4
 * @example
 * ```typescript
 * const { results, total, pages } = await paginate<User>(
 *   'SELECT * FROM users WHERE username LIKE ?',
 *   ['%ada%'],
 *   { page: 2, limit: 25, sortBy: 'created_at', sortDirection: 'desc' }
 * );
 * ```
 */
export async function paginate<T = Record<string, unknown>>(
	sql: string,
	bindings: any[] = [],
	options: PaginateOptions<T> = {}
): Promise<PaginatedResult<T>> {
	const limit = normalizeLimit(options.limit) ?? 20;
	const page = Math.max(1, Math.floor(options.page ?? 1));
	const offset = (page - 1) * limit;

	const merged = await allAllShardsGlobal<T>(sql, bindings, {
		...options,
		offset,
		limit,
		includeTotal: true
	});

	const total = Number(merged.meta.total ?? merged.results.length);
	const pages = limit > 0 ? Math.ceil(total / limit) : 0;

	return {
		results: merged.results,
		total: Number.isFinite(total) ? total : merged.results.length,
		page,
		limit,
		pages
	};
}

/**
 * Outcome of a {@link rebalance} pass.
 * @since 1.4.0
 */
export interface RebalanceResult {
	/** Keys examined */
	examined: number;
	/** Keys whose stored shard already matched their computed shard */
	agreed: number;
	/** Keys moved onto their computed shard */
	moved: number;
	/** Keys that could not be moved, with the reason */
	failed: Array<{ key: string; error: string }>;
}

/**
 * Moves stored mappings onto the shard the placement function computes.
 *
 * This is the migration for a deployment whose keys were placed before 1.4.0.
 * Once it reports `moved: 0` with no failures, the stored mappings agree with
 * the placement function and `placement: 'computed'` can drop the KV read
 * without changing where any key resolves.
 *
 * It is also the measurement that decides whether computed placement is worth
 * enabling: a keyspace that still disagrees after a pass is a keyspace whose
 * exceptions are the mapping.
 *
 * @param table - Table whose rows move with their mapping
 * @param options - Concurrency and a dry-run switch
 * @returns Counts of agreed, moved, and failed keys
 * @throws {CollegeDBError} If CollegeDB is not initialized
 * @since 1.4.0
 * @example
 * ```typescript
 * const result = await rebalance('users', { dryRun: true });
 * console.log(`${result.agreed}/${result.examined} keys already agree`);
 * ```
 */
export async function rebalance(table: string, options: { concurrency?: number; dryRun?: boolean } = {}): Promise<RebalanceResult> {
	const config = getConfig();
	const mapper = getMapper(config);
	const shards = Object.keys(config.shards);

	if (shards.length === 0) {
		throw new CollegeDBError('No shards configured', 'NO_SHARDS');
	}

	const counts = await mapper.getShardKeyCounts();
	const keysByShard = await Promise.all(Object.keys(counts).map(async (shard) => await mapper.getKeysForShard(shard)));
	const keys = keysByShard.flat();

	const result: RebalanceResult = { examined: keys.length, agreed: 0, moved: 0, failed: [] };
	const concurrency = Math.max(1, options.concurrency ?? config.migrationConcurrency ?? 25);

	let cursor = 0;
	const workers = new Array(Math.min(concurrency, keys.length || 1)).fill(null).map(async () => {
		while (cursor < keys.length) {
			const key = keys[cursor++];
			if (key === undefined) continue;

			try {
				const mapping = await mapper.getShardMapping(key);
				const target = hrwShard(key, shards);

				if (!mapping || mapping.shard === target) {
					result.agreed++;
					continue;
				}

				if (!options.dryRun) {
					await reassignShard(key, target, table);
				}
				result.moved++;
			} catch (error) {
				result.failed.push({ key, error: error instanceof Error ? error.message : String(error) });
			}
		}
	});

	await Promise.all(workers);

	return result;
}

/**
 * Options for {@link nextId}.
 * @since 1.2.4
 */
export interface NextIdOptions {
	/** Primary-key column to scan for the current maximum (default: `id`) */
	column?: string;
	/** Lower bound applied to the returned id (never returns below `min`) */
	min?: number;
}

/**
 * Computes the greatest numeric value of `column` across every shard for a table.
 * @private
 */
async function maxColumnAcrossShards(table: string, column: string): Promise<number> {
	const results = await allAllShardsBuilt<{ max_value: number | string | null }>(
		(dialect) => `SELECT MAX(${quoteIdentifier(column, dialect)}) AS max_value FROM ${quoteIdentifier(table, dialect)}`
	);
	const rows = results.map((result) => result.results[0] ?? null);

	let max = 0;
	for (const row of rows) {
		const value = Number(row?.max_value ?? 0);
		if (Number.isFinite(value) && value > max) {
			max = value;
		}
	}

	return max;
}

/**
 * Generates the next monotonic integer id for a sharded table.
 *
 * This replaces the common but broken `SELECT COALESCE(MAX(id), 0) + 1` on a
 * single shard: because rows for a generated id land on the shard the *new* id
 * hashes to, a per-shard MAX never sees rows on the other shards and hands out
 * colliding ids. `nextId` reads `MAX(column)` across *all* shards, then:
 *
 * - If a coordinator is configured, it calls the Durable Object's atomic
 *   sequence (seeded by that cross-shard max), which is race-free across
 *   concurrent callers and isolates. **Recommended for production writes.**
 * - Otherwise it returns `max + 1`. This is cross-shard-correct but not
 *   concurrency-safe on its own; pair it with a unique constraint or a
 *   coordinator when multiple writers race.
 *
 * @param table - Table whose id sequence is being advanced
 * @param options - Column override and lower bound
 * @returns The next id to use for an insert
 * @throws {CollegeDBError} If CollegeDB is not initialized
 * @since 1.2.4
 * @example
 * ```typescript
 * const id = await nextId('tickets');
 * await insertInto(String(id), 'tickets', { id, title, created_at: nowSeconds });
 * ```
 */
export async function nextId(table: string, options: NextIdOptions = {}): Promise<number> {
	const config = getConfig();
	const column = options.column ?? 'id';
	const min = Math.floor(options.min ?? 0);

	if (config.coordinator) {
		// Ask the sequence first. Once it is seeded it is already ahead of every
		// row in every shard, so computing the cross-shard MAX to seed it again
		// would spend one query per shard on every id.
		const seeded = await requestSequence(config, table, min, false);
		if (seeded !== undefined) {
			return seeded;
		}

		try {
			const base = await maxColumnAcrossShards(table, column);
			const floor = Math.max(base + 1, min);
			const value = await requestSequence(config, table, floor, true);
			if (value !== undefined) {
				return value;
			}
			return floor;
		} catch (error) {
			console.warn('Coordinator sequence allocation failed, falling back to cross-shard MAX:', error);
		}
	}

	const base = await maxColumnAcrossShards(table, column);
	return Math.max(base + 1, min);
}

/**
 * Requests the next value of a coordinator sequence.
 *
 * `seed` distinguishes the two calls this makes. The first asks for a value
 * without a floor and is answered only if the sequence already exists, which is
 * the steady state and costs no shard queries. If it does not exist the
 * coordinator says so, and the second call supplies the cross-shard maximum as
 * the floor.
 *
 * @private
 * @returns The allocated value, or `undefined` when the sequence needs seeding
 */
async function requestSequence(config: CollegeDBConfig, table: string, min: number, seed: boolean): Promise<number | undefined> {
	if (!config.coordinator) {
		return undefined;
	}

	const started = phaseStart();
	try {
		const coordinatorId = config.coordinator.idFromName('default');
		const coordinator = config.coordinator.get(coordinatorId);
		const response = await coordinator.fetch('http://coordinator/sequence', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(seed ? { name: table, min } : { name: table, requireExisting: true })
		});

		if (!response.ok) {
			return undefined;
		}

		const result = (await response.json()) as { value?: number; needsSeed?: boolean };
		if (result.needsSeed) {
			return undefined;
		}

		return typeof result.value === 'number' && Number.isFinite(result.value) ? result.value : undefined;
	} catch (error) {
		if (config.debug) {
			console.warn('Coordinator sequence request failed:', error);
		}
		return undefined;
	} finally {
		phaseEnd('coordinator.fetch', started, 'sequence');
	}
}

/**
 * Options for {@link ensureSchema}.
 * @since 1.2.4
 */
export interface EnsureSchemaOptions {
	/**
	 * Skip re-running when the same schema has already been applied to the same
	 * set of shards in this process. DDL is idempotent (`IF NOT EXISTS`), so
	 * this is a latency optimization, not a correctness gate.
	 */
	once?: boolean;
	/**
	 * KV key holding an applied-schema version marker. When the stored value
	 * equals `version`, schema creation is skipped entirely; otherwise the
	 * schema runs and the marker is written. Persists across processes.
	 */
	versionKey?: string;
	/** Version value paired with `versionKey` (default: `'1'`) */
	version?: string;
}

/** In-process record of schema fingerprints already applied (for `once`). */
const ensuredSchemaFingerprints = new Set<string>();

/**
 * Creates schema across every configured shard, idempotently.
 *
 * Wraps {@link createSchemaAcrossShards} with the two guards consumers keep
 * rebuilding: an in-process `once` flag and a KV-backed `versionKey` gate. Pass
 * either a single schema string (statements separated by `;`) or an array of
 * statements.
 *
 * @param schema - Schema SQL string, or an array of statements
 * @param options - Idempotency guards (`once`, `versionKey`/`version`)
 * @returns Promise that resolves when schema is ensured on all shards
 * @throws {CollegeDBError} If CollegeDB is not initialized or a statement fails
 * @since 1.2.4
 * @example
 * ```typescript
 * await ensureSchema(
 *   [
 *     'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL)',
 *     'CREATE INDEX IF NOT EXISTS idx_users_name ON users (name)'
 *   ],
 *   { versionKey: 'schema:version', version: '3' }
 * );
 * ```
 */
export async function ensureSchema(schema: string | string[], options: EnsureSchemaOptions = {}): Promise<void> {
	const config = getConfig();
	const schemaSql = Array.isArray(schema) ? schema.join(';\n') : schema;

	if (options.versionKey) {
		const version = options.version ?? '1';
		try {
			const existing = await config.kv.get(options.versionKey, 'text');
			if (existing === version) {
				return;
			}
		} catch {
			// fall through and (re)apply schema when the version marker is unreadable
		}

		await createSchemaAcrossShards(config.shards, schemaSql);

		try {
			await config.kv.put(options.versionKey, version);
		} catch (error) {
			console.warn('Failed to persist schema version marker:', error);
		}
		return;
	}

	if (options.once) {
		const fingerprint = `${Object.keys(config.shards).sort().join(',')}::${schemaSql}`;
		if (ensuredSchemaFingerprints.has(fingerprint)) {
			return;
		}
		await createSchemaAcrossShards(config.shards, schemaSql);
		ensuredSchemaFingerprints.add(fingerprint);
		return;
	}

	await createSchemaAcrossShards(config.shards, schemaSql);
}

/**
 * Options for {@link initializeFromEnv}.
 * @since 1.2.4
 */
export interface InitializeFromEnvOptions extends Partial<Omit<CollegeDBConfig, 'kv' | 'shards'>> {
	/** Explicit KV store; when omitted, `env.KV` is detected and wrapped */
	kv?: KVStorage;
	/** Drizzle `sql` tag, forwarded to {@link toProvider} for Drizzle bindings */
	sql?: DrizzleSqlTagLike;
	/** Binding-name prefixes that identify shards (default: `['DB_', 'DB-', 'db-']`) */
	shardPrefixes?: string[];
	/** Binding names to never treat as a shard */
	reserved?: string[];
	/** Explicit primary binding; defaults to `env.DB` when present */
	primary?: unknown;
	/** Shard name to register the primary binding under (default: `'db-primary'`) */
	primaryName?: string;
}

const DEFAULT_SHARD_PREFIXES = ['DB_', 'DB-', 'db-'];
const DEFAULT_RESERVED_BINDINGS = ['KV', 'CACHE', 'EMAIL', 'ShardCoordinator', 'HYPERDRIVE', 'ASSETS', 'DB'];

/**
 * Initializes CollegeDB by discovering shard bindings from a Workers `env`.
 *
 * On Cloudflare, D1 bindings live on `env` under conventional names
 * (`DB_EAST`, `DB_WEST`, ...). This scans `env` for those, resolves each with
 * {@link toProvider} (D1 / Drizzle / SQLite), wires the KV store (raw Workers
 * KV is auto-wrapped via {@link createWorkersKVProvider}), and calls
 * {@link initialize}. It removes the ~40-line hand-rolled wiring most Worker
 * apps otherwise write.
 *
 * Shard names are the binding name lowercased with `_` replaced by `-`
 * (`DB_EAST` -> `db-east`).
 *
 * @param env - The Worker environment bindings
 * @param options - KV/coordinator overrides, discovery prefixes, strategy, etc.
 * @returns The list of shard names that were registered
 * @throws {CollegeDBError} If no KV binding or no shard bindings can be resolved
 * @since 1.2.4
 * @example
 * ```typescript
 * import { sql } from 'drizzle-orm';
 *
 * export default {
 *   async fetch(request, env) {
 *     if (!isInitialized()) {
 *       initializeFromEnv(env, { sql, strategy: { read: 'location', write: 'hash' } });
 *     }
 *     // ... routed queries
 *   }
 * };
 * ```
 */
export function initializeFromEnv(env: Record<string, unknown>, options: InitializeFromEnvOptions = {}): string[] {
	const { kv: kvOption, sql, shardPrefixes, reserved, primary, primaryName, ...configRest } = options;

	const prefixes = shardPrefixes ?? DEFAULT_SHARD_PREFIXES;
	const reservedNames = new Set(reserved ?? DEFAULT_RESERVED_BINDINGS);

	let kv = kvOption;
	if (!kv) {
		const envKv = env?.KV;
		if (envKv) {
			kv = isKVStorage(envKv) ? envKv : createWorkersKVProvider(envKv as any);
		}
	}
	if (!kv) {
		throw new CollegeDBError('No KV binding found; pass options.kv or set env.KV', 'NO_KV');
	}

	const shards: Record<string, SQLDatabase> = {};

	const primaryBinding = primary ?? env?.DB;
	if (primaryBinding) {
		const provider = toProvider(primaryBinding, { sql });
		if (provider) {
			shards[primaryName ?? 'db-primary'] = provider;
		}
	}

	for (const key of Object.keys(env ?? {})) {
		if (!key || reservedNames.has(key)) {
			continue;
		}
		if (!prefixes.some((prefix) => key.startsWith(prefix))) {
			continue;
		}

		const binding = env[key];
		if (!binding) {
			continue;
		}

		const provider = toProvider(binding, { sql });
		if (!provider) {
			console.warn(`Binding ${key} is not a recognized SQL provider; skipping`);
			continue;
		}

		shards[key.toLowerCase().replace(/_/g, '-')] = provider;
	}

	if (Object.keys(shards).length === 0) {
		throw new CollegeDBError('No shard bindings found in env', 'NO_SHARDS');
	}

	const config: CollegeDBConfig = { ...configRest, kv, shards };
	if (!config.coordinator && env?.ShardCoordinator) {
		config.coordinator = env.ShardCoordinator as CollegeDBConfig['coordinator'];
	}

	initialize(config);
	return Object.keys(shards);
}

/**
 * Options shared by the object CRUD helpers that support a `RETURNING` clause.
 */
export interface CrudReturningOptions {
	/** Append a `RETURNING` clause; `true` returns `*`, or pass explicit columns */
	returning?: boolean | string | string[];
}

/**
 * Options for the id-scoped convenience helpers ({@link patch}, {@link deleteById}).
 */
export interface IdColumnOptions extends CrudReturningOptions {
	/** Primary-key column name (default: `id`) */
	idColumn?: string;
}

/**
 * Inserts a row built from a plain object, routed to `key`'s shard.
 *
 * @template T - Type of returned rows when `RETURNING` is used
 * @param key - Primary key used for shard routing
 * @param table - Target table name
 * @param values - Column to value map for the new row
 * @param options - Insert modifiers (`orReplace`/`orIgnore`, `returning`)
 * @returns The write result
 * @since 1.2.4
 * @example
 * ```typescript
 * await insertInto('user-123', 'users', {
 *   id: 'user-123',
 *   username: 'ada',
 *   created_at: Math.floor(Date.now() / 1000)
 * });
 * ```
 */
export async function insertInto<T = Record<string, unknown>>(
	key: string,
	table: string,
	values: ColumnValues,
	options: BuildInsertOptions = {}
): Promise<QueryResult<T>> {
	const { sql, bindings } = buildInsert(table, values, { ...options, dialect: await dialectForKey(key) });
	return await run<T>(key, sql, bindings);
}

/**
 * Inserts a row, then reads it back and returns it.
 *
 * Folds the ubiquitous "insert then immediately SELECT the row" pattern into a
 * single call. The row is re-read on the same shard using the routing `key`,
 * matched by `idColumn` (defaulting to the value at `values[idColumn]`, else
 * `key`).
 *
 * @template T - Type of the returned row
 * @param key - Primary key used for shard routing and re-read
 * @param table - Target table name
 * @param values - Column to value map for the new row
 * @param options - Insert modifiers plus the `idColumn` used to re-read (default `id`)
 * @returns The created row, or `null` if it could not be read back
 * @since 1.2.4
 * @example
 * ```typescript
 * const created = await insertReturning('user-123', 'users', { id: 'user-123', username: 'ada' });
 * ```
 */
export async function insertReturning<T = Record<string, unknown>>(
	key: string,
	table: string,
	values: ColumnValues,
	options: BuildInsertOptions & { idColumn?: string } = {}
): Promise<T | null> {
	const idColumn = options.idColumn ?? 'id';
	await insertInto(key, table, values, options);

	const idValue = values[idColumn] ?? key;
	const dialect = await dialectForKey(key, 'read');
	return await first<T>(key, `SELECT * FROM ${quoteIdentifier(table, dialect)} WHERE ${quoteIdentifier(idColumn, dialect)} = ?`, [idValue]);
}

/**
 * Updates rows built from a changes object, scoped by a WHERE map.
 *
 * @template T - Type of returned rows when `RETURNING` is used
 * @param key - Primary key used for shard routing
 * @param table - Target table name
 * @param values - Column to value map of changes to apply
 * @param where - Column to value equality conditions (required; empty throws)
 * @param options - Update modifiers (`returning`)
 * @returns The write result
 * @since 1.2.4
 * @example
 * ```typescript
 * await updateRow('user-123', 'users', { username: 'ada2' }, { id: 'user-123' });
 * ```
 */
export async function updateRow<T = Record<string, unknown>>(
	key: string,
	table: string,
	values: ColumnValues,
	where: ColumnValues,
	options: CrudReturningOptions = {}
): Promise<QueryResult<T>> {
	const { sql, bindings } = buildUpdate(table, values, where, { ...options, dialect: await dialectForKey(key) });
	return await run<T>(key, sql, bindings);
}

/**
 * Convenience wrapper over {@link updateRow} that scopes the update to a single
 * id (`WHERE idColumn = id`).
 *
 * @template T - Type of returned rows when `RETURNING` is used
 * @param key - Primary key used for shard routing
 * @param table - Target table name
 * @param id - Primary-key value to match
 * @param values - Column to value map of changes to apply
 * @param options - `idColumn` (default `id`) and `returning`
 * @returns The write result
 * @since 1.2.4
 * @example
 * ```typescript
 * await patch('42', 'tickets', 42, { status: 'closed', priority: 'high' });
 * ```
 */
export async function patch<T = Record<string, unknown>>(
	key: string,
	table: string,
	id: string | number,
	values: ColumnValues,
	options: IdColumnOptions = {}
): Promise<QueryResult<T>> {
	const idColumn = options.idColumn ?? 'id';
	return await updateRow<T>(key, table, values, { [idColumn]: id }, { returning: options.returning });
}

/**
 * Deletes rows scoped by a WHERE map.
 *
 * @template T - Type of returned rows
 * @param key - Primary key used for shard routing
 * @param table - Target table name
 * @param where - Column to value equality conditions (required; empty throws)
 * @returns The write result
 * @since 1.2.4
 * @example
 * ```typescript
 * await deleteRow('user-123', 'sessions', { user_id: 'user-123' });
 * ```
 */
export async function deleteRow<T = Record<string, unknown>>(key: string, table: string, where: ColumnValues): Promise<QueryResult<T>> {
	const { sql, bindings } = buildDelete(table, where, { dialect: await dialectForKey(key) });
	return await run<T>(key, sql, bindings);
}

/**
 * Convenience wrapper over {@link deleteRow} that deletes a single id
 * (`WHERE idColumn = id`).
 *
 * @template T - Type of returned rows
 * @param key - Primary key used for shard routing
 * @param table - Target table name
 * @param id - Primary-key value to match
 * @param options - `idColumn` (default `id`)
 * @returns The write result
 * @since 1.2.4
 * @example
 * ```typescript
 * await deleteById('user-123', 'users', 'user-123');
 * ```
 */
export async function deleteById<T = Record<string, unknown>>(
	key: string,
	table: string,
	id: string | number,
	options: { idColumn?: string } = {}
): Promise<QueryResult<T>> {
	const idColumn = options.idColumn ?? 'id';
	return await deleteRow<T>(key, table, { [idColumn]: id });
}

/**
 * Inserts a row, or updates the conflicting columns when a unique/primary key
 * already exists (`INSERT ... ON CONFLICT ... DO UPDATE`).
 *
 * @template T - Type of returned rows when `RETURNING` is used
 * @param key - Primary key used for shard routing
 * @param table - Target table name
 * @param values - Column to value map for the row
 * @param conflictColumns - Column(s) forming the conflict target
 * @param options - Upsert modifiers (`update` subset, `returning`)
 * @returns The write result
 * @since 1.2.4
 * @example
 * ```typescript
 * await upsert('settings:theme', 'settings', { key: 'theme', value: 'dark' }, 'key');
 * ```
 */
export async function upsert<T = Record<string, unknown>>(
	key: string,
	table: string,
	values: ColumnValues,
	conflictColumns: string | string[],
	options: BuildUpsertOptions = {}
): Promise<QueryResult<T>> {
	const { sql, bindings } = buildUpsert(table, values, conflictColumns, { ...options, dialect: await dialectForKey(key) });
	return await run<T>(key, sql, bindings);
}

const DEFAULT_LOOKUP_NAMESPACE = 'collegedb:lookup:';

/**
 * Options for the lookup helpers.
 */
export interface LookupOptions {
	/** KV store override; defaults to the store passed to {@link initialize} */
	kv?: KVStorage;
	/** Key namespace prefix (default: `collegedb:lookup:`) */
	namespace?: string;
}

/**
 * Resolves the KV store to use, preferring an explicit override.
 * @private
 */
function resolveKV(explicit?: KVStorage): KVStorage {
	const kv = explicit ?? getActiveConfig()?.kv;
	if (!kv) {
		throw new CollegeDBError('CollegeDB not initialized. Call initialize() first or pass options.kv.', 'NOT_INITIALIZED');
	}
	return kv;
}

/**
 * Builds the namespaced KV key for a lookup entry.
 * @private
 */
function lookupKey(key: string, namespace?: string): string {
	return `${namespace ?? DEFAULT_LOOKUP_NAMESPACE}${key}`;
}

/**
 * Stores (or overwrites) a secondary-index value.
 *
 * @param key - Secondary identifier (e.g. an email hash or slug)
 * @param value - Value to associate (e.g. a primary id)
 * @param options - Namespace and KV override
 * @throws {CollegeDBError} If no KV store is available
 * @since 1.2.4
 * @example
 * ```typescript
 * await setLookup(emailHash, String(customerId));
 * ```
 */
export async function setLookup(key: string, value: string, options: LookupOptions = {}): Promise<void> {
	const kv = resolveKV(options.kv);
	await kv.put(lookupKey(key, options.namespace), value);
}

/**
 * Reads a secondary-index value.
 *
 * @param key - Secondary identifier
 * @param options - Namespace and KV override
 * @returns The stored value, or `null` if absent
 * @throws {CollegeDBError} If no KV store is available
 * @since 1.2.4
 * @example
 * ```typescript
 * const customerId = await getLookup(emailHash);
 * ```
 */
export async function getLookup(key: string, options: LookupOptions = {}): Promise<string | null> {
	const kv = resolveKV(options.kv);
	return await kv.get(lookupKey(key, options.namespace), 'text');
}

/**
 * Deletes a secondary-index value.
 *
 * @param key - Secondary identifier
 * @param options - Namespace and KV override
 * @throws {CollegeDBError} If no KV store is available
 * @since 1.2.4
 * @example
 * ```typescript
 * await deleteLookup(oldEmailHash);
 * ```
 */
export async function deleteLookup(key: string, options: LookupOptions = {}): Promise<void> {
	const kv = resolveKV(options.kv);
	await kv.delete(lookupKey(key, options.namespace));
}
