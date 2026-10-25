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
import { asyncContextAvailable, runInScope } from './async-context';
import { cached as cacheValue, invalidate as invalidateCache, type CacheOptions } from './cache';
import { CollegeDBError } from './errors';
import { withKVTimeout } from './kv-timeout';
import { KVShardMapper } from './kvmap';
import {
	createSchemaAcrossShards,
	ensureJournalSchema,
	ensureShardTables,
	errorChain,
	findTombstone,
	getMigrationStatus as getJournalStatus,
	moveRecord,
	purgeRetired as purgeJournal,
	recoverMigrations as recoverJournal,
	runGuardedWrite,
	verifyMigrations as verifyJournal,
	type MigrationEnv,
	type MigrationStatus,
	type MoveRecordOptions,
	type PublishInfo,
	type PurgeReport,
	type RecoveryReport,
	type VerifyReport
} from './migrations';
import {
	addPlacementException,
	candidateShards,
	hrwShard,
	isPlacementBackingOff,
	isPlacementManifestFresh,
	loadPlacementExceptions,
	loadPlacementManifest,
	placementUnverified,
	publishPlacementEpoch,
	republishPlacementExceptions,
	resetPlacementState,
	savePlacementManifest,
	shardForEpoch,
	type PlacementManifest
} from './placement';
import { planQuery, unroutableError } from './planner';
import { createWorkersKVProvider, isKVStorage, toProvider, type DrizzleSqlTagLike } from './providers';
import {
	RETIRED_TABLE,
	buildDelete,
	buildGuardedInsert,
	buildGuardedUpsert,
	buildInsert,
	buildUpdate,
	buildUpsert,
	isProvenJournalDialect,
	validateIdentifier,
	type BuildInsertOptions,
	type BuildUpsertOptions,
	type BuiltQuery,
	type ColumnValues,
	type TombstoneGuard
} from './query';
import { reconcileAllocationsIn, type ReconcileOptions, type ReconcileReport } from './reconcile';
import { createRouterState, currentState, type RouterState } from './state';
import { instrumentKV, instrumentSQL, phaseEnd, phaseStart } from './telemetry';
import { readFence, runPrune, type PruneOptions, type PruneReport } from './topology';
import type {
	CollegeDBConfig,
	D1Region,
	KVStorage,
	OperationType,
	PreparedStatement,
	QueryResult,
	ReadConsistency,
	ReadOptions,
	SQLDatabase,
	SQLDialect,
	ShardLocation,
	ShardStats,
	ShardingStrategy
} from './types';

/** @private */
function clearPlacementDecision(s: RouterState): void {
	s.placementDecision = null;
	s.placementFlight = null;
	s.placementReadOnly = false;
	s.placementToken++;
}

/**
 * Gets the shared mapper for the active configuration.
 * @private
 */
function getMapper(s: RouterState, config: CollegeDBConfig): KVShardMapper {
	if (!s.mapper) {
		s.mapper = new KVShardMapper(config.kv, {
			hashShardMappings: config.hashShardMappings,
			mappingCacheTtlMs: config.mappingCacheTtlMs,
			knownShardsCacheTtlMs: config.knownShardsCacheTtlMs
		});
	}

	return s.mapper;
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
	return initializeIn(currentState(), config);
}

function initializeIn(s: RouterState, config: CollegeDBConfig) {
	validateJournalConfig(config);
	const active = applyConfig(s, config);
	const fingerprint = shardSetFingerprint(active);

	// The documented Workers pattern calls initialize on every request, so this
	// bookkeeping runs per request unless it is memoized. Registering the same
	// shard set again is a KV read plus a KV write that changes nothing, and it
	// is now awaited through waitUntil rather than silently cancelled, so the
	// cost is real. Skip it once the set has been registered.
	if (s.syncedShardSets.has(fingerprint)) {
		return;
	}
	s.syncedShardSets.add(fingerprint);

	// Background: sync KV known shards with configured shards
	try {
		const mapper = getMapper(s, active);
		track(
			active,
			Promise.resolve()
				.then(async () => {
					const existing = await mapper.getKnownShards();
					const merged = Array.from(new Set([...existing, ...Object.keys(active.shards)]));
					// an unchanged list is not written back
					if (merged.length !== existing.length) {
						await mapper.setKnownShards(merged);
					}
				})
				.catch(() => {
					// Let a later initialize retry rather than leaving the set
					// recorded as synced when it never was.
					s.syncedShardSets.delete(fingerprint);
				})
		);
	} catch {
		s.syncedShardSets.delete(fingerprint);
	}

	if (active.journalShard !== undefined) {
		track(
			active,
			ensureJournalSchema(active.shards, active.journalShard).catch((error) => {
				console.warn('Creating the migration journal tables failed; they are created again on first use:', error);
			})
		);
	}

	if (active.shards && Object.keys(active.shards).length > 0 && !active.disableAutoMigration) {
		track(
			active,
			performAutoMigration(s, active).catch((error) => {
				console.warn('Background auto-migration failed:', error);
			})
		);
	}
}

/**
 * Rejects a `journalShard` that is not one of the configured shards.
 * @private
 */
function validateJournalConfig(config: CollegeDBConfig): void {
	if (config.journalShard !== undefined && !config.shards[config.journalShard]) {
		throw new CollegeDBError(`journalShard ${config.journalShard} is not a configured shard`, 'INVALID_JOURNAL_SHARD');
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
function applyConfig(s: RouterState, config: CollegeDBConfig): CollegeDBConfig {
	s.sink.observer = config.onPhase ?? null;

	let active = config;
	if (config.kvTimeoutMs) {
		active = { ...config, kv: withKVTimeout(config.kv, config.kvTimeoutMs) };
	}
	if (config.onPhase) {
		const shards: Record<string, SQLDatabase> = {};
		for (const [binding, database] of Object.entries(config.shards)) {
			shards[binding] = database ? instrumentSQL(database, binding, s.sink) : database;
		}
		active = { ...active, kv: instrumentKV(active.kv, s.sink), shards };
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
	const previous = s.config;
	const reuseMapper = s.mapper !== null && previous !== null && isSameMapperConfig(previous, active);
	const reusePlacement = reuseMapper && previous !== null && isSamePlacementConfig(previous, active);

	s.config = active;

	if (!reuseMapper) {
		s.mapper = new KVShardMapper(active.kv, {
			hashShardMappings: active.hashShardMappings,
			mappingCacheTtlMs: active.mappingCacheTtlMs,
			knownShardsCacheTtlMs: active.knownShardsCacheTtlMs,
			legacyMultiKeyLookup: active.legacyMultiKeyLookup
		});
	}

	if (!reusePlacement) {
		s.shardSizeCache.clear();
		s.unmeasuredLogged.clear();
		s.roundRobinIndex = 0;
		resetPlacementState(s.placement);
		clearPlacementDecision(s);
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
		JSON.stringify(left.shardState ?? null) === JSON.stringify(right.shardState ?? null) &&
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
export function initializeAsync(config: CollegeDBConfig) {
	return initializeAsyncIn(currentState(), config);
}

async function initializeAsyncIn(s: RouterState, config: CollegeDBConfig) {
	validateJournalConfig(config);
	const active = applyConfig(s, config);

	if (active.journalShard !== undefined) {
		await ensureJournalSchema(active.shards, active.journalShard);
	}

	// Sync KV known shards with configured shards (awaited in async init)
	try {
		const mapper = getMapper(s, active);
		const existing = await mapper.getKnownShards();
		const merged = Array.from(new Set([...existing, ...Object.keys(active.shards)]));
		await mapper.setKnownShards(merged);
	} catch {}

	if (active.shards && Object.keys(active.shards).length > 0 && !active.disableAutoMigration)
		try {
			await performAutoMigration(s, active);
		} catch (error) {
			console.warn('Auto migration failed:', error);
		}
}

/**
 * Initializes the configuration and then performs a callback once the configuration
 * has finished initializing.
 *
 * The callback runs against a state of its own, so every module-level call it
 * makes, before or after an `await`, uses `config` even when another
 * `collegedb` call with a different configuration overlaps it. The module-level
 * configuration is left as it was. A later call with the same bindings and
 * options reuses that state, so per-request callers keep their caches.
 *
 * That isolation needs `AsyncLocalStorage`. On a runtime without it (Workers
 * without `nodejs_compat`), a callback runs against the module-level state like
 * `initialize` does. A different configuration started while another callback is
 * running then throws `CONCURRENT_CONFIG`, and the same configuration may overlap.
 * {@link createClient} needs no async context and is the only isolation-safe API
 * there.
 *
 * @param config - CollegeDB Configuration
 * @param callback - The callback to perform after the initialization
 * @returns The result of the callback
 * @throws {CollegeDBError} `CONCURRENT_CONFIG` when there is no async context and a callback with a different configuration is running
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
export async function collegedb<T>(config: CollegeDBConfig, callback: () => T): Promise<Awaited<T>> {
	if (!asyncContextAvailable()) {
		return await runOnDefaultState(config, callback);
	}

	const scoped = scopedStateFor(config);
	return await runInScope(scoped, async () => {
		await initializeAsyncIn(scoped, config);
		return await callback();
	});
}

/** Most scoped states kept per KV store. @private */
const MAX_SCOPED_STATES = 8;

/**
 * States made for `collegedb` callbacks, by KV store, each with the configuration
 * it was made for.
 * @private
 */
const scopedStates = new WeakMap<KVStorage, Array<{ config: CollegeDBConfig; state: RouterState }>>();

/**
 * The state a `collegedb` callback with `config` runs against. A configuration
 * with the same bindings and options gets the state an earlier call made, so a
 * Worker that calls `collegedb` on every request keeps its mapping and manifest
 * caches warm like one that calls `initialize`. Any other configuration gets a
 * state of its own.
 * @private
 */
function scopedStateFor(config: CollegeDBConfig): RouterState {
	const kept = scopedStates.get(config.kv) ?? [];
	const found = kept.find((entry) => isSameMapperConfig(entry.config, config) && isSamePlacementConfig(entry.config, config));
	if (found) {
		return found.state;
	}

	const state = createRouterState();
	kept.push({ config, state });
	if (kept.length > MAX_SCOPED_STATES) {
		kept.shift();
	}
	scopedStates.set(config.kv, kept);
	return state;
}

/**
 * The configuration of the `collegedb` callbacks running on the default state,
 * which is all there is without an async context.
 * @private
 */
let unscopedClaim: { config: CollegeDBConfig; running: number } | null = null;

/**
 * Runs a `collegedb` callback on the default state, refusing a different
 * configuration while another callback is still running.
 * @private
 */
async function runOnDefaultState<T>(config: CollegeDBConfig, callback: () => T): Promise<Awaited<T>> {
	if (unscopedClaim && !(isSameMapperConfig(unscopedClaim.config, config) && isSamePlacementConfig(unscopedClaim.config, config))) {
		throw new CollegeDBError(
			'collegedb() cannot start this callback: another collegedb() callback with a different configuration is still running, and this runtime has no AsyncLocalStorage to keep them apart. Wait for it to finish, or use createClient(config), which keeps each configuration separate.',
			'CONCURRENT_CONFIG'
		);
	}

	const claim = (unscopedClaim ??= { config, running: 0 });
	claim.running++;

	try {
		await initializeAsync(config);
		return await callback();
	} finally {
		if (--claim.running === 0 && unscopedClaim === claim) {
			unscopedClaim = null;
		}
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
async function performAutoMigration(s: RouterState, config: CollegeDBConfig): Promise<void> {
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
					maxRecordsToCheck: 1000,
					statusCache: s.migrationStatus
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
	resetConfigIn(currentState());
}

function resetConfigIn(s: RouterState): void {
	s.config = null;
	s.mapper = null;
	s.shardSizeCache.clear();
	s.unmeasuredLogged.clear();
	s.roundRobinIndex = 0;
	s.ensuredSchemaFingerprints.clear();
	resetPlacementState(s.placement);
	clearPlacementDecision(s);
	s.syncedShardSets.clear();
	s.migrationStatus?.clear();
	s.sink.observer = null;
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
	return isInitializedIn(currentState());
}

function isInitializedIn(s: RouterState): boolean {
	return s.config !== null;
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
	return getActiveConfigIn(currentState());
}

function getActiveConfigIn(s: RouterState): CollegeDBConfig | null {
	return s.config;
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
function getConfig(s: RouterState): CollegeDBConfig {
	if (!s.config) {
		throw new CollegeDBError('CollegeDB not initialized. Call initialize() first.', 'NOT_INITIALIZED');
	}
	return s.config;
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
async function getDatabaseSizeForAllocation(s: RouterState, shardName: string, config: CollegeDBConfig): Promise<number> {
	const cacheTtlMs = Math.max(0, config.sizeCacheTtlMs ?? 30_000);
	const cached = s.shardSizeCache.get(shardName);

	if (cached && cached.expiresAt >= Date.now()) {
		return cached.size;
	}

	const database = config.shards[shardName];
	if (!database) {
		throw new CollegeDBError(`Shard ${shardName} not found in configuration`, 'SHARD_NOT_FOUND');
	}

	const size = await getDatabaseSize(database);
	if (cacheTtlMs > 0) {
		s.shardSizeCache.set(shardName, {
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
 * @returns Promise resolving to filtered list of shard names, which keeps every shard whose size cannot be read
 */
async function filterShardsBySize(s: RouterState, availableShards: string[], config: CollegeDBConfig): Promise<string[]> {
	if (typeof config.maxDatabaseSize !== 'number' || !Number.isFinite(config.maxDatabaseSize) || config.maxDatabaseSize <= 0) {
		return availableShards;
	}

	const limit = config.maxDatabaseSize;

	const sizeChecks = await Promise.allSettled(
		availableShards.map(async (shardName) => {
			const size = await getDatabaseSizeForAllocation(s, shardName, config);
			return {
				shard: shardName,
				size,
				withinLimit: size < limit
			};
		})
	);

	// a size that cannot be read is unknown, and the limit applies only where it can be measured
	const validShards = availableShards.filter((shard, index) => {
		const check = sizeChecks[index]!;
		if (check.status === 'fulfilled') {
			return check.value.withinLimit;
		}

		if (config.debug && !s.unmeasuredLogged.has(shard)) {
			s.unmeasuredLogged.add(shard);
			console.warn(`Could not read the size of ${shard}, so maxDatabaseSize does not apply to it:`, check.reason);
		}
		return true;
	});

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
 * Keeps the shards that `shardState` and the `eligible` predicate allow to take
 * new keys. A predicate that throws, rejects or returns anything but `true`
 * leaves its shard out, and a throw also fails the call: a shard whose
 * eligibility is unknown never counts as eligible.
 *
 * @private
 * @throws {CollegeDBError} `ELIGIBILITY_FAILED` when the predicate throws or rejects
 */
async function filterShardsByState(
	shards: string[],
	config: CollegeDBConfig,
	operation: 'allocate' | 'publish',
	key?: string
): Promise<string[]> {
	const open = shards.filter((shard) => {
		const state = config.shardState?.[shard];
		return state?.writable !== false && (state?.drain ?? 'active') === 'active';
	});

	const predicate = config.eligible;
	if (!predicate) {
		return open;
	}

	const verdicts = await Promise.all(
		open.map(async (shard) => {
			const state = config.shardState?.[shard] ?? {};

			try {
				return (await predicate(shard, { key, operation, tags: state.tags ?? {}, state })) === true;
			} catch (error) {
				throw new CollegeDBError(
					`The eligible predicate failed for ${shard} (${error instanceof Error ? error.message : String(error)}). Nothing was ${operation === 'publish' ? 'published' : 'allocated'}; retry the operation.`,
					'ELIGIBILITY_FAILED'
				);
			}
		})
	);

	return open.filter((_, index) => verdicts[index]);
}

/**
 * The shards a new key may go to: `shardState`, then the predicate, then the
 * `maxDatabaseSize` limit, which keeps its fallback of ignoring itself when every
 * remaining shard is over it. A shard `shardState` or the predicate turned away
 * is never a fallback.
 *
 * @private
 * @throws {CollegeDBError} `NO_ELIGIBLE_SHARD` when `shardState` and the predicate leave nothing
 */
async function selectEligibleShards(
	s: RouterState,
	config: CollegeDBConfig,
	operation: 'allocate' | 'publish',
	key?: string,
	withSize: boolean = true
): Promise<string[]> {
	const allowed = await filterShardsByState(Object.keys(config.shards), config, operation, key);

	if (allowed.length === 0) {
		throw new CollegeDBError(
			'No shard is eligible for new keys: every configured shard is read-only, draining, retired or turned down by the eligible predicate.',
			'NO_ELIGIBLE_SHARD'
		);
	}

	return withSize ? await filterShardsBySize(s, allowed, config) : allowed;
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
	s: RouterState,
	effectiveStrategy: ShardingStrategy,
	primaryKey: string,
	availableShards: string[],
	config: CollegeDBConfig
): string {
	const started = phaseStart(s.sink);
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
				const shard = availableShards[s.roundRobinIndex % availableShards.length]!;
				s.roundRobinIndex = (s.roundRobinIndex + 1) % availableShards.length;
				return shard;
			}
			default: {
				return hrwShard(primaryKey, availableShards);
			}
		}
	} finally {
		phaseEnd('shard.select', started, effectiveStrategy, s.sink);
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
 * Under computed placement the manifest is re-read once its TTL passes, and a
 * newer one another client published is adopted without publishing anything. A
 * read tolerates a failed re-read and keeps the manifest it holds; a write needs
 * one verified within its TTL. With nothing held, a failed read throws
 * `PLACEMENT_UNVERIFIED` rather than assuming a topology.
 *
 * With `journalShard` set the manifest comes from the journal, which is strongly
 * consistent, and KV holds a derived copy. Callers that find the cache expired
 * share one refresh, and for a second after a refresh fails a read keeps the stale
 * manifest without trying again, so an outage does not make every read wait out
 * the timeout. Writes always try.
 *
 * When a manifest exists and no shard is eligible, nothing is published: the
 * epochs held keep serving reads and every write is refused with
 * `NO_ELIGIBLE_SHARD` until a refresh finds an eligible shard.
 *
 * @private
 */
async function resolvePlacement(
	s: RouterState,
	config: CollegeDBConfig,
	forWrite: boolean = false
): Promise<{ mode: 'computed' | 'kv'; manifest: PlacementManifest | null }> {
	const ready = heldDecision(s, forWrite);
	if (ready) {
		return ready;
	}

	const held = s.placementDecision;

	try {
		const flight =
			s.placementFlight ??
			refreshPlacement(s, config).finally(() => {
				s.placementFlight = null;
			});
		s.placementFlight = flight;
		return await flight;
	} catch (error) {
		if (held && !forWrite && error instanceof CollegeDBError && error.code === 'PLACEMENT_UNVERIFIED') {
			return held;
		}

		throw error;
	}
}

/**
 * The decision {@link resolvePlacement} returns without reading anything, or
 * `null` when a refresh is due. Hot paths take it before awaiting, so a held
 * decision costs no promise.
 * @private
 */
function heldDecision(s: RouterState, forWrite: boolean): { mode: 'computed' | 'kv'; manifest: PlacementManifest | null } | null {
	const held = s.placementDecision;
	if (held && (held.mode === 'kv' || isPlacementManifestFresh(s.placement))) {
		return held;
	}

	return held && !forWrite && isPlacementBackingOff(s.placement) ? held : null;
}

/**
 * Errors from publishing the manifest that are not a verification failure.
 * @private
 */
const PUBLISH_ERRORS = new Set(['TOO_MANY_EPOCHS', 'PLACEMENT_CONFLICT', 'MANIFEST_CORRUPT']);

/**
 * The slow path of {@link resolvePlacement}: decides the mode, reads the manifest
 * and, on a cold start, publishes the current topology.
 * @private
 */
async function refreshPlacement(
	s: RouterState,
	config: CollegeDBConfig
): Promise<{ mode: 'computed' | 'kv'; manifest: PlacementManifest | null }> {
	const token = s.placementToken;
	const held = s.placementDecision;
	const shards = Object.keys(config.shards);
	const remember = <T extends { mode: 'computed' | 'kv'; manifest: PlacementManifest | null }>(decision: T): T => {
		if (token === s.placementToken) {
			s.placementDecision = decision;
		}
		return decision;
	};

	if (!held) {
		const strategyIsHash = resolveStrategy(config, 'read') === 'hash' && resolveStrategy(config, 'write') === 'hash';

		if (config.placement !== 'computed' || !strategyIsHash || shards.length === 0) {
			return remember({ mode: 'kv', manifest: null });
		}
	}

	const journal = placementJournal(config);
	const stored = await loadPlacementManifest(config.kv, undefined, undefined, journal, s.placement);

	const base = held?.manifest ? advancedManifest(held.manifest, stored) : stored;
	if (held?.manifest && base && !s.placementReadOnly) {
		return remember({ mode: 'computed', manifest: base });
	}

	// the newest epoch names the eligible shards, so a draining one stops owning new keys
	let eligible: string[];
	try {
		eligible = await selectEligibleShards(s, config, 'publish');
	} catch (error) {
		// nothing to publish: the epochs that exist keep serving reads and writes are refused
		if (base && error instanceof CollegeDBError && error.code === 'NO_ELIGIBLE_SHARD') {
			if (token === s.placementToken) {
				s.placementReadOnly = true;
			}
			return remember({ mode: 'computed', manifest: base });
		}

		throw error;
	}

	try {
		const published = (await publishPlacementEpoch(config.kv, journal, base, eligible, s.placement)).manifest;
		if (token === s.placementToken) {
			s.placementReadOnly = false;
		}
		return remember({ mode: 'computed', manifest: published });
	} catch (error) {
		if (error instanceof CollegeDBError && PUBLISH_ERRORS.has(error.code ?? '')) {
			throw error;
		}

		throw placementUnverified('manifest', error, journal ? 'the journal' : 'KV');
	}
}

/**
 * Evaluates eligibility again and publishes a new placement snapshot when the
 * eligible set differs from the newest epoch's shard set.
 *
 * Under computed placement the eligible set is part of the topology: it is
 * computed when a snapshot is published, never inside the hash. A cold start
 * publishes one, and this publishes another on demand, for example from a health
 * job after the `eligible` predicate started answering differently. The predicate
 * runs with `operation: 'publish'`. A changed set adds an epoch and keeps every
 * older one, so keys placed under an earlier epoch keep resolving; shards that
 * dropped out stay readable.
 *
 * With a journal the swap is a compare-and-swap, so of two callers that find the
 * same change, one publishes and the other reports `published: false`. Without
 * one, both may report `true`.
 *
 * Under kv placement there is no snapshot: it returns the shards eligible now,
 * `published: false` and `epoch: 0`.
 *
 * @returns The eligible shards, whether this call added an epoch, and the newest epoch number
 * @throws {CollegeDBError} `NO_ELIGIBLE_SHARD`, `ELIGIBILITY_FAILED`, `TOO_MANY_EPOCHS` when the manifest is full (the manifest is left as it was), `PLACEMENT_UNVERIFIED` when KV or the journal fails, `PLACEMENT_CONFLICT`
 * @since 1.4.0
 * @example
 * ```typescript
 * const { eligible, published, epoch } = await publishEligibility();
 * ```
 */
export function publishEligibility(): Promise<{ eligible: string[]; published: boolean; epoch: number }> {
	return publishEligibilityIn(currentState());
}

async function publishEligibilityIn(s: RouterState): Promise<{ eligible: string[]; published: boolean; epoch: number }> {
	const config = getConfig(s);
	const eligible = await selectEligibleShards(s, config, 'publish');
	const strategyIsHash = resolveStrategy(config, 'read') === 'hash' && resolveStrategy(config, 'write') === 'hash';

	if (config.placement !== 'computed' || !strategyIsHash) {
		return { eligible, published: false, epoch: 0 };
	}

	const token = s.placementToken;
	const held = s.placementDecision?.manifest ?? null;
	const journal = placementJournal(config);
	const loaded = await loadPlacementManifest(config.kv, undefined, { fresh: true }, journal, s.placement);
	const base = held && loaded ? advancedManifest(held, loaded) : (loaded ?? held);

	let result: { manifest: PlacementManifest; published: boolean };
	try {
		result = await publishPlacementEpoch(config.kv, journal, base, eligible, s.placement);
	} catch (error) {
		if (error instanceof CollegeDBError && PUBLISH_ERRORS.has(error.code ?? '')) {
			throw error;
		}

		throw placementUnverified('manifest', error, journal ? 'the journal' : 'KV');
	}

	if (token === s.placementToken) {
		s.placementDecision = { mode: 'computed', manifest: result.manifest };
		s.placementReadOnly = false;
	}

	return { eligible, published: result.published, epoch: result.manifest.epochs[0]!.n };
}

/**
 * Reads the manifest again, bypassing the cache, and adopts it when it is newer.
 * A writer calls it after a topology fence turned its write away.
 * @private
 */
async function reloadPlacement(s: RouterState, config: CollegeDBConfig): Promise<void> {
	const held = s.placementDecision;
	if (held?.mode !== 'computed' || !held.manifest) {
		return;
	}

	const stored = await loadPlacementManifest(config.kv, undefined, { fresh: true }, placementJournal(config), s.placement);
	s.placementDecision = { mode: 'computed', manifest: advancedManifest(held.manifest, stored) };
}

/**
 * The manifest that is further along: the stored one when it has a newer epoch,
 * or the same epoch with at least as many exceptions, else the one already held.
 * @private
 */
function advancedManifest(held: PlacementManifest, stored: PlacementManifest | null): PlacementManifest {
	if (!stored) {
		return held;
	}

	const heldEpoch = held.epochs[0]!.n;
	const storedEpoch = stored.epochs[0]!.n;
	return storedEpoch > heldEpoch || (storedEpoch === heldEpoch && stored.exceptionsVersion >= held.exceptionsVersion) ? stored : held;
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
async function resolveCandidates(
	s: RouterState,
	primaryKey: string,
	operationType: OperationType = 'write',
	resolved?: { mode: 'computed' | 'kv'; manifest: PlacementManifest | null }
): Promise<string[]> {
	const config = getConfig(s);
	const forWrite = operationType === 'write';
	const decision = resolved ?? heldDecision(s, forWrite) ?? (await resolvePlacement(s, config, forWrite));

	if (decision.mode !== 'computed' || !decision.manifest) {
		return [await getShardForKey(s, primaryKey, operationType)];
	}

	if (forWrite && s.placementReadOnly) {
		throw new CollegeDBError(
			'No shard is eligible for writes: every configured shard is read-only, draining, retired or turned down by the eligible predicate. Reads still work.',
			'NO_ELIGIBLE_SHARD'
		);
	}

	const mapper = getMapper(s, config);
	const hashedKey = await mapper.hashKey(primaryKey);
	const exceptions = await loadPlacementExceptions(
		config.kv,
		decision.manifest.exceptionsVersion,
		undefined,
		{ staleOk: !forWrite },
		s.placement
	);

	if (exceptions.has(hashedKey)) {
		return [await getShardForKey(s, primaryKey, operationType)];
	}

	const candidates = candidateShards(primaryKey, decision.manifest, Object.keys(config.shards));
	return candidates.length > 0 ? candidates : [await getShardForKey(s, primaryKey, operationType)];
}

const WRITE_TABLE =
	/^\s*(?:(?:insert|replace)(?:\s+or\s+\w+|\s+ignore)?\s+into|update(?:\s+or\s+\w+)?|delete\s+from)\s+((?:["`]?[\w$]+["`]?\.)*["`]?[\w$]+["`]?)/i;

/**
 * The table an INSERT, REPLACE, UPDATE or DELETE statement writes to: `undefined`
 * for any other statement, `null` for one of those that does not parse.
 * @private
 */
function statementTable(sql: string): string | null | undefined {
	if (!/^\s*(?:insert|replace|update|delete)\b/i.test(sql)) {
		return undefined;
	}

	const match = WRITE_TABLE.exec(sql);
	return match ? match[1]!.replace(/["`]/g, '') : null;
}

/**
 * Picks the shard a write goes to among the candidates of a computed placement.
 *
 * One candidate is the steady state and costs nothing. Several mean the key
 * computes a different owner than an older epoch did, and the row may still sit
 * on the older one, where a write to the newest owner would match nothing or
 * create a second row. The candidates are probed by primary key, newest first,
 * and the write follows the row, which is then recorded as an exception so the
 * probe is not repeated. A key no shard holds goes to the newest owner. A probe that
 * fails (table or key column missing) cannot prove the key absent, so it refuses.
 *
 * @private
 * @throws {CollegeDBError} `WRITE_UNRESOLVED` when no shard holds the key and a probe failed or the table is unknown
 */
async function locateWriteShard(
	s: RouterState,
	config: CollegeDBConfig,
	key: string,
	table: string | null | undefined,
	candidates: string[]
): Promise<string> {
	if (candidates.length <= 1 || table === undefined) {
		return candidates[0]!;
	}

	const unresolved = (detail: string) =>
		new CollegeDBError(
			`Could not tell which shard holds ${key}${table ? ` in ${table}` : ''} (${detail}). After a topology change a write first finds the row, so the table must exist on every shard and its key column must be named in keyColumns when it is not id. The write was not applied.`,
			'WRITE_UNRESOLVED'
		);

	if (table === null) {
		throw unresolved('the statement names no table CollegeDB can read');
	}

	const column = config.keyColumns?.[table] ?? 'id';
	let failure: string | undefined;

	for (let i = 0; i < candidates.length; i++) {
		const binding = candidates[i]!;
		const database = config.shards[binding]!;

		try {
			const row = await database
				.prepare(
					`SELECT 1 AS found FROM ${quoteIdentifier(table, database.dialect)} WHERE ${quoteIdentifier(column, database.dialect)} = ? LIMIT 1`
				)
				.bind(key)
				.first();

			if (row) {
				if (i > 0) {
					await memoizePlacementException(s, key, binding);
				}
				return binding;
			}
		} catch (error) {
			failure ??= `${binding}: ${error instanceof Error ? error.message : String(error)}`;
		}
	}

	if (failure) {
		throw unresolved(failure);
	}

	return candidates[0]!;
}

/**
 * The shard a write for `key` goes to under computed placement, found before the
 * write runs. `table` is what the statement writes to, as {@link statementTable} reads it.
 * @private
 */
async function resolveWriteShard(s: RouterState, config: CollegeDBConfig, key: string, table: string | null | undefined): Promise<string> {
	return await locateWriteShard(s, config, key, table, await resolveCandidates(s, key, 'write'));
}

/**
 * Records that a key lives somewhere its placement function would not compute,
 * so later operations resolve it through KV instead of walking epochs.
 *
 * @private
 */
async function memoizePlacementException(s: RouterState, primaryKey: string, shard: string): Promise<void> {
	const config = getConfig(s);
	const decision = s.placementDecision;
	if (decision?.mode !== 'computed' || !decision.manifest) {
		return;
	}

	const mapper = getMapper(s, config);

	try {
		await mapper.setShardMapping(primaryKey, shard);
		if (config.journalShard !== undefined) {
			await ensureJournalSchema(config.shards, config.journalShard);
		}

		// saved from the newest manifest KV has, so a stale copy cannot overwrite a newer epoch
		const journal = placementJournal(config);
		const base = advancedManifest(
			decision.manifest,
			await loadPlacementManifest(config.kv, undefined, { fresh: true }, journal, s.placement)
		);
		const updated = await addPlacementException(config.kv, base, await mapper.hashKey(primaryKey), journal, s.placement);
		s.placementDecision = { mode: 'computed', manifest: updated };
	} catch (error) {
		if (config.debug) {
			console.warn(`Failed to record placement exception for ${primaryKey}:`, error);
		}
	}
}

async function getShardForKey(s: RouterState, primaryKey: string, operationType: OperationType = 'write'): Promise<string> {
	const config = getConfig(s);
	const mapper = getMapper(s, config);

	// Check if mapping already exists
	const existingMapping = await mapper.getShardMapping(primaryKey);
	if (existingMapping) {
		return existingMapping.shard;
	}

	const selectedShard = await allocateShardForKey(s, config, primaryKey, operationType);

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
async function allocateShardForKey(
	s: RouterState,
	config: CollegeDBConfig,
	primaryKey: string,
	operationType: OperationType
): Promise<string> {
	// Before allocating a new shard, check if any existing shards contain this key
	const availableShards = Object.keys(config.shards);
	if (availableShards.length === 0) {
		throw new CollegeDBError('No shards configured', 'NO_SHARDS');
	}

	// a read that records no mapping picks no owner, so only a persisted choice is held to eligibility
	const persists = operationType === 'write' || config.allocateOnRead === true;
	const eligibleShards = persists
		? await selectEligibleShards(s, config, 'allocate', primaryKey)
		: await filterShardsBySize(s, availableShards, config);

	// If no existing mapping found after auto-migration, allocate a new shard
	let selectedShard: string;
	const effectiveStrategy = resolveStrategy(config, operationType);

	// Use coordinator if available for allocation
	if (config.coordinator) {
		const started = phaseStart(s.sink);
		try {
			const coordinatorId = config.coordinator.idFromName(config.coordinatorScope ?? 'default');
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
				selectedShard = selectShardByStrategy(s, effectiveStrategy, primaryKey, eligibleShards, config);
			}
		} catch (error) {
			console.warn('Coordinator allocation failed, falling back to local strategy:', error);
			selectedShard = selectShardByStrategy(s, effectiveStrategy, primaryKey, eligibleShards, config);
		} finally {
			phaseEnd('coordinator.fetch', started, 'allocate', s.sink);
		}
	} else {
		selectedShard = selectShardByStrategy(s, effectiveStrategy, primaryKey, eligibleShards, config);
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
async function getDatabase(s: RouterState, primaryKey: string, operationType: OperationType = 'write', sql?: string): Promise<SQLDatabase> {
	const config = getConfig(s);
	const decision = heldDecision(s, operationType === 'write') ?? (await resolvePlacement(s, config, operationType === 'write'));
	let shard: string;

	if (decision.mode === 'computed' && decision.manifest) {
		const candidates = await resolveCandidates(s, primaryKey, operationType, decision);

		// the statement is parsed only when there is more than one owner to tell apart
		shard =
			operationType === 'write' && sql !== undefined && candidates.length > 1
				? await locateWriteShard(s, config, primaryKey, statementTable(sql), candidates)
				: candidates[0]!;
	} else {
		shard = await getShardForKey(s, primaryKey, operationType);
	}

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
export function prepare(key: string, sql: string): Promise<PreparedStatement> {
	return prepareIn(currentState(), key, sql);
}

async function prepareIn(s: RouterState, key: string, sql: string): Promise<PreparedStatement> {
	const operationType = getOperationType(sql);
	const db = await getDatabase(s, key, operationType, sql);
	const result = db.prepare(sql);
	return result;
}

/**
 * Where the migration journal lives for this configuration, or `null` when
 * `journalShard` is not set. Builder writes and reads only pay for the
 * tombstone check when it is set.
 * @private
 */
function explicitJournal(config: CollegeDBConfig): { binding: string; db: SQLDatabase; scope: string } | null {
	if (config.journalShard === undefined) {
		return null;
	}

	const db = config.shards[config.journalShard];
	if (!db) {
		throw new CollegeDBError(`journalShard ${config.journalShard} is not a configured shard`, 'INVALID_JOURNAL_SHARD');
	}

	return { binding: config.journalShard, db, scope: config.journalScope ?? 'default' };
}

/**
 * The journal a move needs: the configured shard, or the only shard of a
 * single-shard config.
 * @throws {CollegeDBError} `JOURNAL_NOT_CONFIGURED` with several shards and no `journalShard`
 * @private
 */
function requireJournal(config: CollegeDBConfig): { binding: string; db: SQLDatabase; scope: string } {
	const explicit = explicitJournal(config);
	if (explicit) {
		return explicit;
	}

	const bindings = Object.keys(config.shards);
	const only = bindings.length === 1 ? config.shards[bindings[0]!] : undefined;
	if (!only) {
		throw new CollegeDBError(
			'Moving a key between shards needs a journal. Set journalShard in the configuration to the binding that should hold it.',
			'JOURNAL_NOT_CONFIGURED'
		);
	}

	return { binding: bindings[0]!, db: only, scope: config.journalScope ?? 'default' };
}

/**
 * Follows a tombstone from a shard that no longer holds the key to the shard it
 * moved to, so a read in the cutover window still finds the row. Returns
 * `undefined` when the key has no tombstone here.
 *
 * A failed lookup, a move chain longer than {@link MAX_TOMBSTONE_HOPS}, a cycle
 * and a move to an unknown shard all throw, unless `bestEffort` is set. Then
 * they return `undefined`, which reads as not found.
 *
 * @private
 */
async function followTombstone<T>(
	config: CollegeDBConfig,
	binding: string,
	key: string,
	execute: (database: SQLDatabase) => Promise<T>,
	isEmpty: (value: T) => boolean,
	bestEffort: boolean
): Promise<T | undefined> {
	const journal = explicitJournal(config);
	if (!journal) {
		return undefined;
	}

	const visited = [binding];
	let current = binding;
	for (let hop = 0; ; hop++) {
		const database = config.shards[current];
		if (!database || !isProvenJournalDialect(database.dialect)) {
			return undefined;
		}

		let moved: string | undefined;
		try {
			moved = await tombstoneTarget(database, journal.scope, key);
		} catch (error) {
			if (bestEffort) {
				return undefined;
			}
			throw lookupUnavailable(key, current, error);
		}

		if (moved === undefined) {
			return undefined;
		}

		if (!config.shards[moved]) {
			if (bestEffort) {
				return undefined;
			}
			throw new CollegeDBError(
				`Tombstone for ${key} on ${current} points to ${moved}, which is not a configured shard. ` +
					'Check the shard configuration; retrying will not help.',
				'MIGRATION_LOOKUP_UNAVAILABLE'
			);
		}

		if (visited.includes(moved) || hop === MAX_TOMBSTONE_HOPS) {
			if (bestEffort) {
				return undefined;
			}
			const path = visited.join(' -> ');
			const reason = visited.includes(moved) ? `loops back to ${moved}` : `is longer than ${MAX_TOMBSTONE_HOPS} hops (next ${moved})`;
			throw new CollegeDBError(`Move chain for ${key} ${reason}: ${path}. Retrying will not help.`, 'MIGRATION_CHAIN_EXCEEDED');
		}

		visited.push(moved);
		const result = await execute(config.shards[moved]!);
		if (!isEmpty(result)) {
			return result;
		}
		current = moved;
	}
}

/** Longest chain of tombstones a read follows. @private */
const MAX_TOMBSTONE_HOPS = 4;

/**
 * Where a shard's tombstone for a key points, or `undefined` when it holds none.
 *
 * Only a missing sidecar table counts as no tombstone, because no move can have
 * written one there. The read does not create the table, so a read-only shard
 * still answers. Every other failure propagates.
 *
 * @private
 */
async function tombstoneTarget(database: SQLDatabase, scope: string, key: string): Promise<string | undefined> {
	try {
		return (await findTombstone(database, scope, key))?.movedTo;
	} catch (error) {
		if (isMissingSidecarTable(error)) {
			return undefined;
		}
		throw error;
	}
}

/** MySQL and MariaDB error number for a missing table. @private */
const MYSQL_NO_SUCH_TABLE = 1146;

/**
 * Whether a lookup failed because the shard has no sidecar table. A missing-table
 * code counts on any link of the cause chain, because Drizzle wraps the driver
 * error. A message counts only on a link with no cause: a wrapper's message
 * echoes the query and its parameters, and a key can hold the phrase. The table
 * name must appear in the message, so an unrelated missing object does not match.
 * @private
 */
function isMissingSidecarTable(error: unknown): boolean {
	return errorChain(error).some((link) => {
		const { code, errno, cause } = link as { code?: unknown; errno?: unknown; cause?: unknown };
		if (code === '42P01' || code === 'ER_NO_SUCH_TABLE' || errno === MYSQL_NO_SUCH_TABLE) {
			return true;
		}
		if (cause) {
			return false;
		}
		const message = link instanceof Error ? link.message : String(link);
		return message.includes(RETIRED_TABLE) && /no such table|does not exist|doesn't exist/i.test(message);
	});
}

/**
 * The error for a tombstone lookup that failed for a reason other than a missing
 * table. The read may be retried, or called with `bestEffort` to read it as not
 * found.
 * @private
 */
function lookupUnavailable(key: string, binding: string, cause: unknown): CollegeDBError {
	const reason = cause instanceof Error ? cause.message : String(cause);
	const error = new CollegeDBError(
		`Could not check whether ${key} moved off ${binding}, because the tombstone lookup failed (${reason}). ` +
			'Retry the read, or pass bestEffort to treat it as not found.',
		'MIGRATION_LOOKUP_UNAVAILABLE'
	);
	return Object.assign(error, { cause });
}

/**
 * What a read runs on: the shard, or a read session opened on it.
 * @private
 */
interface ReadTarget {
	prepare(sql: string): PreparedStatement;
	getBookmark?(): string | null;
}

/**
 * The read targets of one logical operation, one per shard and reused by every
 * query the operation makes on that shard.
 * @private
 */
interface ReadSessions {
	on(database: SQLDatabase): ReadTarget;
	/** Copy of `result` carrying the shard's latest bookmark, or `result` when it has none */
	stamp<R extends QueryResult<any>>(database: SQLDatabase, result: R): R;
	/** Latest bookmark of every session opened so far, keyed by database id */
	bookmarks(): Record<string, string> | undefined;
}

type InternalReadOptions = ReadOptions & { sessions?: ReadSessions };

const PLAIN_SESSIONS: ReadSessions = { on: (database) => database, stamp: (_database, result) => result, bookmarks: () => undefined };

/**
 * Turns a consistency request into the object reads run on, per shard.
 *
 * With no request the shard itself is returned, so the default call path stays
 * the one it always was. Anything the shard cannot honor throws here, before a
 * query runs.
 *
 * @private
 */
function readSessions(config: CollegeDBConfig, consistency: ReadConsistency | undefined): ReadSessions {
	if (!consistency) {
		return PLAIN_SESSIONS;
	}

	const mode = consistency.mode;
	if (mode !== 'primary' && mode !== 'session' && mode !== 'eventual') {
		throw new CollegeDBError(`Unknown read consistency mode: ${String(mode)}`, 'INVALID_CONSISTENCY');
	}

	// keyed by database so a caller holding only the provider (the tombstone walk) finds its session
	const opened = new Map<SQLDatabase, ReadTarget>();
	const bindingOf = (database: SQLDatabase): string =>
		Object.keys(config.shards).find((binding) => config.shards[binding] === database) ?? '<unknown>';
	const idOf = (database: SQLDatabase): string | undefined => config.shardState?.[bindingOf(database)]?.id;

	return {
		on(database) {
			const existing = opened.get(database);
			if (existing) {
				return existing;
			}

			const binding = bindingOf(database);
			const sessionCapable = typeof database.withSession === 'function' && database.consistency?.session !== false;
			const supported = mode === 'session' ? sessionCapable : database.consistency?.[mode] !== false;
			if (!supported) {
				throw new CollegeDBError(`Shard ${binding} cannot serve a ${mode} read`, 'CONSISTENCY_UNSUPPORTED');
			}

			let constraint = mode === 'primary' ? 'first-primary' : 'first-unconstrained';
			if (consistency.mode === 'session' && consistency.bookmarks && Object.keys(consistency.bookmarks).length > 0) {
				const id = idOf(database);
				if (!id) {
					throw new CollegeDBError(
						`Shard ${binding} has no database id in shardState, so a bookmark cannot be matched to it`,
						'BOOKMARK_IDENTITY_UNVERIFIED'
					);
				}
				const bookmark = consistency.bookmarks[id];
				if (typeof bookmark === 'string' && bookmark) {
					constraint = bookmark;
				}
			}

			const target: ReadTarget = sessionCapable ? database.withSession!(constraint) : database;
			opened.set(database, target);
			return target;
		},
		stamp(database, result) {
			const bookmark = opened.get(database)?.getBookmark?.();
			return bookmark ? { ...result, meta: { ...result.meta, bookmark } } : result;
		},
		bookmarks() {
			let out: Record<string, string> | undefined;
			for (const [database, target] of opened) {
				const id = idOf(database);
				const bookmark = target.getBookmark?.();
				if (id && bookmark) {
					(out ??= {})[id] = bookmark;
				}
			}
			return out;
		}
	};
}

/**
 * Sessions for one operation: the ones its caller already opened, or new ones.
 * @private
 */
function sessionsFor(s: RouterState, options: ReadOptions | undefined): ReadSessions {
	if (!options) {
		return PLAIN_SESSIONS;
	}

	return (options as InternalReadOptions).sessions ?? readSessions(getConfig(s), options.consistency);
}

/**
 * Options for the inner calls of an operation, so they share its sessions.
 * @private
 */
function sharing(s: RouterState, options: ReadOptions | undefined): ReadOptions {
	if (!options?.consistency || (options as InternalReadOptions).sessions) {
		return options ?? {};
	}

	return {
		consistency: options.consistency,
		bestEffort: options.bestEffort,
		sessions: readSessions(getConfig(s), options.consistency)
	} as ReadOptions;
}

/**
 * Stamps a read's result with its session bookmark. Plain sessions have none, so
 * the statement's own promise is returned and the default path adds no hop.
 * @private
 */
function stamped<R extends QueryResult<any>>(sessions: ReadSessions, database: SQLDatabase, pending: Promise<R>): Promise<R> {
	return sessions === PLAIN_SESSIONS ? pending : pending.then((result) => sessions.stamp(database, result));
}

/**
 * Adds the bookmarks of every database a merged read touched to its meta.
 * @private
 */
function withBookmarks<R extends QueryResult<any>>(result: R, sessions: ReadSessions): R {
	const bookmarks = sessions.bookmarks();
	return bookmarks ? { ...result, meta: { ...result.meta, bookmarks } } : result;
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
	s: RouterState,
	key: string,
	sql: string,
	execute: (database: SQLDatabase) => Promise<T>,
	isEmpty: (value: T) => boolean,
	options: ReadOptions | undefined
): Promise<T> {
	const config = getConfig(s);
	const operationType = getOperationType(sql);

	// KV placement resolves to exactly one shard, which is the steady state for
	// every deployment that has not opted into computed placement. Taking it
	// here skips the candidate array, the epoch walk and the empty-result
	// bookkeeping below, none of which can do anything with one candidate.
	const decision = heldDecision(s, operationType === 'write') ?? (await resolvePlacement(s, config, operationType === 'write'));
	if (decision.mode !== 'computed' || !decision.manifest) {
		const binding = await getShardForKey(s, key, operationType);
		const database = config.shards[binding];
		if (!database) {
			throw new CollegeDBError(`Shard ${binding} not found in configuration`, 'SHARD_NOT_FOUND');
		}

		const result = await execute(database);
		// no journal means no tombstones, so the async walk is skipped
		return isEmpty(result) && config.journalShard !== undefined
			? ((await followTombstone(config, binding, key, execute, isEmpty, options?.bestEffort === true)) ?? result)
			: result;
	}

	let candidates = await resolveCandidates(s, key, operationType, decision);

	// a write statement passed through here must not run on a shard that does not hold the row
	if (operationType === 'write' && candidates.length > 1) {
		candidates = [await locateWriteShard(s, config, key, statementTable(sql), candidates)];
	}

	let firstResult: T | undefined;

	for (let i = 0; i < candidates.length; i++) {
		const binding = candidates[i]!;
		const database = config.shards[binding];
		if (!database) continue;

		const result = await execute(database);

		if (!isEmpty(result)) {
			if (i > 0) {
				await memoizePlacementException(s, key, binding);
			}
			return result;
		}

		if (firstResult === undefined) {
			firstResult = result;
		}
	}

	if (firstResult !== undefined) {
		return config.journalShard === undefined
			? firstResult
			: ((await followTombstone(config, candidates[0] ?? '', key, execute, isEmpty, options?.bestEffort === true)) ?? firstResult);
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
export function run<T = Record<string, unknown>>(key: string, sql: string, bindings: any[] = []): Promise<QueryResult<T>> {
	return runIn<T>(currentState(), key, sql, bindings);
}

async function runIn<T = Record<string, unknown>>(s: RouterState, key: string, sql: string, bindings: any[] = []): Promise<QueryResult<T>> {
	const prepared = await prepareIn(s, key, sql);
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

async function allocateInsertShard(s: RouterState): Promise<string> {
	const config = getConfig(s);
	const availableShards = Object.keys(config.shards);

	if (availableShards.length === 0) {
		throw new CollegeDBError('No shards configured', 'NO_SHARDS');
	}

	const eligibleShards = await selectEligibleShards(s, config, 'allocate');
	if (eligibleShards.length === 0) {
		throw new CollegeDBError('No shards available for insert', 'NO_SHARDS');
	}

	const effectiveStrategy = resolveStrategy(config, 'write');
	const allocatorKey = createInsertAllocatorKey();

	if (config.coordinator) {
		try {
			const coordinatorId = config.coordinator.idFromName(config.coordinatorScope ?? 'default');
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

	return selectShardByStrategy(s, effectiveStrategy, allocatorKey, eligibleShards, config);
}

async function executeInsertOnShard<T = Record<string, unknown>>(
	s: RouterState,
	shardBinding: string,
	sql: string,
	bindings: any[] = [],
	options: InsertOptions = {}
): Promise<InsertResult<T>> {
	const config = getConfig(s);
	if (!config.shards[shardBinding]) {
		throw new CollegeDBError(`Shard ${shardBinding} not found`, 'SHARD_NOT_FOUND');
	}

	// validated before the INSERT so a bad name cannot leave a row behind
	const dialect = dialectOf(s, shardBinding);
	const idColumn = options.idColumn ?? 'id';
	if (options.table) {
		quoteIdentifier(options.table, dialect);
		quoteIdentifier(idColumn, dialect);
	}

	const returning = /\breturning\b/i.test(sql);
	const result = returning ? await allShardIn<T>(s, shardBinding, sql, bindings) : await runShardIn<T>(s, shardBinding, sql, bindings);
	const generatedId = extractGeneratedId(result, options.idColumn);

	if (generatedId === undefined) {
		throw new CollegeDBError(
			options.idColumn
				? `Insert did not return a value for the id column "${options.idColumn}"`
				: 'Insert did not return a generated primary key. Pass { idColumn } when the primary key is not named id or rowid.',
			'GENERATED_KEY_UNAVAILABLE'
		);
	}

	const mapper = getMapper(s, config);
	const idKey = String(generatedId);

	// Each shard mints its own sequence, so two shards both hand out 1, then 2,
	// and so on. Storing the second mapping would overwrite the first and leave
	// the earlier row on a shard nothing routes to. The INSERT has already run by
	// the time the id is known, so a refusal also has to remove that row, or it
	// stays on the shard with no mapping. That needs the table, hence options.table.
	//
	// One shard has no second sequence to collide with, so the lookup is skipped
	// there rather than spending a KV read per insert to rule out a collision
	// that cannot occur.
	const existing = Object.keys(config.shards).length > 1 ? await mapper.getShardMapping(idKey) : null;
	if (existing && existing.shard !== shardBinding) {
		const reason =
			`Generated id ${idKey} is already mapped to shard ${existing.shard}, but this insert ran on ${shardBinding}. ` +
			'Per-shard AUTOINCREMENT and SERIAL sequences repeat across shards, so a database-generated id is not unique ' +
			'cluster-wide. Use nextId() to allocate a cluster-unique id and pass it explicitly, or confine the table to one ' +
			'shard with insertShard().';
		const orphan = `The row is still on shard ${shardBinding} with id ${idKey}`;

		if (!options.table) {
			throw new CollegeDBError(`${reason} ${orphan}; delete it, or pass { table } so the insert removes it.`, 'GENERATED_KEY_COLLISION');
		}

		try {
			const removal = buildDelete(options.table, { [idColumn]: generatedId }, { dialect });
			const deleted = await runShardIn(s, shardBinding, removal.sql, removal.bindings);
			if (deleted.meta.changes === 0) {
				throw new Error(`DELETE on ${options.table} matched no row where ${idColumn} = ${idKey}`);
			}
		} catch (cause) {
			const failure = new CollegeDBError(`${reason} ${orphan}, and removing it from ${options.table} failed.`, 'GENERATED_KEY_COLLISION');
			failure.cause = cause;
			throw failure;
		}

		throw new CollegeDBError(`${reason} The row this insert wrote on ${shardBinding} was removed.`, 'GENERATED_KEY_COLLISION');
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
 * **The refused row.** The INSERT has already run when the collision shows up.
 * CollegeDB takes only a raw SQL string here and does not parse it, so it does
 * not know which table to clean. Pass `options.table` and it deletes the row it
 * just wrote (matched on `idColumn`, default `id`) before it throws. Without
 * `table` the row stays on the shard and removing it is the caller's job; the
 * error message names the shard and id. If the delete itself fails, the error
 * still has the code `GENERATED_KEY_COLLISION`, names the shard and id of the
 * leftover row, and carries the delete failure as `cause`.
 *
 * @template T - Type of returned rows when the insert uses `RETURNING`
 * @param sql - The INSERT statement to execute
 * @param bindings - Parameter values to bind to the statement
 * @param options - `idColumn` names the generated key column, `table` enables cleanup of a refused row
 * @returns Promise resolving to the write result plus the generated id
 * @throws {CollegeDBError} If the insert succeeds but no generated id can be determined
 * @throws {CollegeDBError} `GENERATED_KEY_COLLISION` when another shard already owns the generated id
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
export function insert<T = Record<string, unknown>>(
	sql: string,
	bindings: any[] = [],
	options: InsertOptions = {}
): Promise<InsertResult<T>> {
	return insertIn<T>(currentState(), sql, bindings, options);
}

async function insertIn<T = Record<string, unknown>>(
	s: RouterState,
	sql: string,
	bindings: any[] = [],
	options: InsertOptions = {}
): Promise<InsertResult<T>> {
	const shardBinding = await allocateInsertShard(s);
	return await executeInsertOnShard<T>(s, shardBinding, sql, bindings, options);
}

/**
 * Executes an insert directly on a named shard and returns the generated primary key.
 *
 * Use this helper when you already know the shard you want to target.
 * The helper still captures the generated id and stores the mapping so routed
 * reads can find the new row later.
 *
 * Throws `GENERATED_KEY_COLLISION` when another shard already owns the
 * generated id. Cleanup of the row the refused INSERT wrote works as in
 * {@link insert}: pass `options.table` to have it deleted, otherwise it is the
 * caller's to remove.
 *
 * @template T - Type of returned rows when the insert uses `RETURNING`
 * @param shardBinding - The shard binding to execute the insert on
 * @param sql - The INSERT statement to execute
 * @param bindings - Parameter values to bind to the statement
 * @param options - `idColumn` names the generated key column, `table` enables cleanup of a refused row
 * @returns Promise resolving to the write result plus the generated id
 * @throws {CollegeDBError} If the insert succeeds but no generated id can be determined
 * @throws {CollegeDBError} `GENERATED_KEY_COLLISION` when another shard already owns the generated id
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
export function insertShard<T = Record<string, unknown>>(
	shardBinding: string,
	sql: string,
	bindings: any[] = [],
	options: InsertOptions = {}
): Promise<InsertResult<T>> {
	return insertShardIn<T>(currentState(), shardBinding, sql, bindings, options);
}

async function insertShardIn<T = Record<string, unknown>>(
	s: RouterState,
	shardBinding: string,
	sql: string,
	bindings: any[] = [],
	options: InsertOptions = {}
): Promise<InsertResult<T>> {
	return await executeInsertOnShard<T>(s, shardBinding, sql, bindings, options);
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
 * @throws {CollegeDBError} `MIGRATION_LOOKUP_UNAVAILABLE` when a tombstone lookup fails or names an unknown shard;
 * `MIGRATION_CHAIN_EXCEEDED` on a move cycle or a chain past four hops. Both return not found with `bestEffort`.
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
 *
 * @param options - Read guarantee for this call; the shard's bookmark comes back on `meta.bookmark`
 */
export function all<T = Record<string, unknown>>(
	key: string,
	sql: string,
	bindings: any[] = [],
	options?: ReadOptions
): Promise<QueryResult<T>> {
	return allIn<T>(currentState(), key, sql, bindings, options);
}

async function allIn<T = Record<string, unknown>>(
	s: RouterState,
	key: string,
	sql: string,
	bindings: any[] = [],
	options?: ReadOptions
): Promise<QueryResult<T>> {
	const sessions = sessionsFor(s, options);
	const result = await readAcrossCandidates<QueryResult<T>>(
		s,
		key,
		sql,
		(database) =>
			stamped(
				sessions,
				database,
				sessions
					.on(database)
					.prepare(sql)
					.bind(...bindings)
					.all<T>()
			),
		(value) => value.success && value.results.length === 0,
		options
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
 * @throws {CollegeDBError} `MIGRATION_LOOKUP_UNAVAILABLE` when a tombstone lookup fails or names an unknown shard;
 * `MIGRATION_CHAIN_EXCEEDED` on a move cycle or a chain past four hops. Both return not found with `bestEffort`.
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
 * ```
 *
 * @param options - Read guarantee for this call. A bare row has no meta, so use {@link all} to get a bookmark back
 */
export function first<T = Record<string, unknown>>(
	key: string,
	sql: string,
	bindings: any[] = [],
	options?: ReadOptions
): Promise<T | null> {
	return firstIn<T>(currentState(), key, sql, bindings, options);
}

async function firstIn<T = Record<string, unknown>>(
	s: RouterState,
	key: string,
	sql: string,
	bindings: any[] = [],
	options?: ReadOptions
): Promise<T | null> {
	const sessions = sessionsFor(s, options);
	return await readAcrossCandidates<T | null>(
		s,
		key,
		sql,
		(database) =>
			sessions
				.on(database)
				.prepare(sql)
				.bind(...bindings)
				.first<T>(),
		(value) => value === null,
		options
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
 * @param options - Read guarantee for this call. The mapped shard and the fanout share one session per shard
 * @returns Promise resolving to merged query results
 * @since 1.1.4
 */
export function allByLookupKey<T = Record<string, unknown>>(
	lookupKey: string,
	sql: string,
	bindings: any[] = [],
	batchSize: number = 50,
	options?: ReadOptions
): Promise<QueryResult<T>> {
	return allByLookupKeyIn<T>(currentState(), lookupKey, sql, bindings, batchSize, options);
}

async function allByLookupKeyIn<T = Record<string, unknown>>(
	s: RouterState,
	lookupKey: string,
	sql: string,
	bindings: any[] = [],
	batchSize: number = 50,
	options?: ReadOptions
): Promise<QueryResult<T>> {
	const config = getConfig(s);
	const mapper = getMapper(s, config);
	const mapping = await mapper.getShardMapping(lookupKey);
	const shared = sharing(s, options);

	if (mapping) {
		const mappedShardDb = config.shards[mapping.shard];
		if (mappedShardDb) {
			const mappedResult = await allShardIn<T>(s, mapping.shard, sql, bindings, shared);
			if (mappedResult.success && mappedResult.results.length > 0) {
				return mappedResult;
			}
		}
	}

	const shardResults = await allAllShardsIn<T>(s, sql, bindings, batchSize, shared);
	return withBookmarks(mergeAllShardQueryResults(shardResults), sessionsFor(s, shared));
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
 * @param options - Read guarantee for this call. The mapped shard and the fanout share one session per shard
 * @returns Promise resolving to the first matching record, or null
 * @since 1.1.4
 */
export function firstByLookupKey<T = Record<string, unknown>>(
	lookupKey: string,
	sql: string,
	bindings: any[] = [],
	batchSize: number = 50,
	options?: ReadOptions
): Promise<T | null> {
	return firstByLookupKeyIn<T>(currentState(), lookupKey, sql, bindings, batchSize, options);
}

async function firstByLookupKeyIn<T = Record<string, unknown>>(
	s: RouterState,
	lookupKey: string,
	sql: string,
	bindings: any[] = [],
	batchSize: number = 50,
	options?: ReadOptions
): Promise<T | null> {
	const config = getConfig(s);
	const mapper = getMapper(s, config);
	const mapping = await mapper.getShardMapping(lookupKey);
	const shared = sharing(s, options);

	if (mapping) {
		const mappedShardDb = config.shards[mapping.shard];
		if (mappedShardDb) {
			const mappedFirst = await firstShardIn<T>(s, mapping.shard, sql, bindings, shared);
			if (mappedFirst !== null) {
				return mappedFirst;
			}
		}
	}

	const fanoutResults = await firstAllShardsIn<T>(s, sql, bindings, batchSize, shared);
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
 * @since 1.3.0
 * @example
 * ```typescript
 * const mapper = new KVShardMapper(env.KV, { hashShardMappings: true });
 * await mapper.updateShardMapping('user-123', 'db-west');
 *
 * invalidateMappingCache('user-123'); // this process now reads the new shard
 * ```
 */
export function invalidateMappingCache(key?: string): Promise<void> {
	return invalidateMappingCacheIn(currentState(), key);
}

async function invalidateMappingCacheIn(s: RouterState, key?: string): Promise<void> {
	const mapper = getMapper(s, getConfig(s));

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
 * 3. If the target shard differs from the current one, moves the row through the
 *    migration journal (see `docs/migration-protocol.md`)
 * 4. Updates the KV mapping, and the placement exception set when the target is
 *    not the shard placement computes
 *
 * A move needs a journal: set `journalShard` in the configuration. With several
 * shards and none set it throws `JOURNAL_NOT_CONFIGURED`. A crash at any step is
 * finished or rolled back by {@link recoverMigrations}. Only one table row moves;
 * related rows in other tables are the caller's to move.
 *
 * @param primaryKey - Primary key to reassign to a different shard
 * @param newBinding - New shard binding name where the data should be moved
 * @param tableName - Name of the table containing the record to migrate
 * @param options - Primary-key column, conflict policy, version column and retention
 * @returns Promise that resolves when reassignment and migration are complete
 * @throws {CollegeDBError} `SHARD_NOT_FOUND`, `MAPPING_NOT_FOUND`, `JOURNAL_NOT_CONFIGURED`,
 * `MIGRATION_IN_PROGRESS`, `MIGRATION_ABORTED`, `MIGRATION_PUBLISH_FAILED`, or the errors of the move itself
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
export function reassignShard(primaryKey: string, newBinding: string, tableName: string, options: MoveRecordOptions = {}): Promise<void> {
	return reassignShardIn(currentState(), primaryKey, newBinding, tableName, options);
}

async function reassignShardIn(
	s: RouterState,
	primaryKey: string,
	newBinding: string,
	tableName: string,
	options: MoveRecordOptions = {}
): Promise<void> {
	const config = getConfig(s);

	if (!config.shards[newBinding]) {
		throw new CollegeDBError(`Shard ${newBinding} not found in configuration`, 'SHARD_NOT_FOUND');
	}

	const mapper = getMapper(s, config);
	const currentMapping = await mapper.getShardMapping(primaryKey);
	const decision = await resolvePlacement(s, config, true);

	// Under computed placement there is no stored mapping to read, so the current
	// shard comes from the placement function instead of being an error.
	let currentShard = currentMapping?.shard;
	if (!currentShard && decision.mode === 'computed' && decision.manifest) {
		currentShard = candidateShards(primaryKey, decision.manifest, Object.keys(config.shards))[0];
	}

	if (!currentShard) {
		throw new CollegeDBError(`No existing mapping found for primary key: ${primaryKey}`, 'MAPPING_NOT_FOUND');
	}

	if (currentShard === newBinding) {
		await publishMove(s, config, explicitJournal(config), { key: primaryKey, destination: newBinding });
		return;
	}

	const journal = requireJournal(config);
	const result = await moveRecord(migrationEnv(s, config, journal), {
		table: tableName,
		key: primaryKey,
		source: currentShard,
		destination: newBinding,
		options: { pkColumn: config.keyColumns?.[tableName], ...options }
	});

	if (result.outcome === 'aborted') {
		throw new CollegeDBError(
			`Move of ${primaryKey} to ${newBinding} aborted (${result.reason}); ${currentShard} still owns the key`,
			'MIGRATION_ABORTED'
		);
	}
}

/**
 * Builds the engine's view of the active configuration.
 * @private
 */
function migrationEnv(s: RouterState, config: CollegeDBConfig, journal: { binding: string; db: SQLDatabase; scope: string }): MigrationEnv {
	return {
		scope: journal.scope,
		journalBinding: journal.binding,
		shards: config.shards,
		placementEpoch: () => s.placementDecision?.manifest?.epochs[0]?.n ?? null,
		publish: (info) => publishMove(s, config, journal, info)
	};
}

/**
 * Updates the state derived from a committed move: the KV mapping, the placement
 * exception, the coordinator ledger and the mapping cache. Safe to run again.
 *
 * A key moved off the shard its placement function computes is recorded as an
 * exception, or the next read would look at the computed shard and find nothing.
 * A key moved onto its computed shard needs no exception.
 * @private
 */
async function publishMove(
	s: RouterState,
	config: CollegeDBConfig,
	journal: { binding: string; db: SQLDatabase; scope: string } | null,
	info: Pick<PublishInfo, 'key' | 'destination'>
): Promise<void> {
	const mapper = getMapper(s, config);
	const decision = await resolvePlacement(s, config, true);

	if (await mapper.getShardMapping(info.key)) {
		await mapper.updateShardMapping(info.key, info.destination);
	} else {
		await mapper.setShardMapping(info.key, info.destination);
	}

	if (decision.mode === 'computed' && decision.manifest) {
		const owner = candidateShards(info.key, decision.manifest, Object.keys(config.shards))[0];
		if (owner !== info.destination) {
			if (journal) {
				await ensureJournalSchema(config.shards, journal.binding);
			}

			// saved from the newest manifest, so a stale copy cannot overwrite a newer epoch
			const authority = journal ? { db: journal.db, scope: journal.scope } : undefined;
			const base = advancedManifest(
				decision.manifest,
				await loadPlacementManifest(config.kv, undefined, { fresh: true }, authority, s.placement)
			);
			s.placementDecision = {
				mode: 'computed',
				manifest: await addPlacementException(config.kv, base, await mapper.hashKey(info.key), authority, s.placement)
			};
		}
	}

	// Drop any coordinator-recorded allocation so a later first touch of this key
	// is not handed the shard it has just been moved off.
	await forgetCoordinatorAllocation(config, info.key);

	await mapper.invalidateCachedMapping(info.key);
}

/**
 * Rebuilds the KV copy of the exception set from the journal, so a copy that
 * another isolate overwrote with an older snapshot is repaired. Best effort.
 * @private
 */
async function republishExceptions(s: RouterState, config: CollegeDBConfig): Promise<void> {
	const journal = explicitJournal(config);
	const decision = s.placementDecision;
	if (!journal || decision?.mode !== 'computed' || !decision.manifest) {
		return;
	}

	try {
		s.placementDecision = {
			mode: 'computed',
			manifest: await republishPlacementExceptions(config.kv, decision.manifest, { db: journal.db, scope: journal.scope }, s.placement)
		};
	} catch (error) {
		if (config.debug) {
			console.warn('Could not rebuild the placement exception set:', error);
		}
	}
}

/**
 * Resumes every unfinished move recorded in the journal.
 *
 * A crash can stop a move at any step. This reads each active claim, decides what
 * the move already did from the shards and the journal, and finishes it or aborts
 * it, per the rules in `docs/migration-protocol.md`. A move that cannot be
 * resolved is reported in `failed` and left for an operator. Run it on a timer, or
 * after a deploy that may have interrupted a move. One recovery runs per journal
 * scope at a time, on any number of isolates: it takes a lease in the journal, and a
 * second call while the lease is live throws `RECOVERY_IN_PROGRESS`, which a timer
 * can treat as success.
 *
 * @param options - `olderThanMs` leaves alone moves that changed more recently than that
 * @returns What happened to each active claim
 * @throws {CollegeDBError} `JOURNAL_NOT_CONFIGURED` with several shards and no `journalShard`; `RECOVERY_IN_PROGRESS` when another recovery holds the lease; `RECOVERY_FENCED` when this one lost the lease mid-run; `JOURNAL_UNAVAILABLE` when the journal cannot be read
 * @since 1.4.0
 * @example
 * ```typescript
 * const report = await recoverMigrations();
 * if (report.failed.length > 0) {
 *   console.error('moves needing attention', report.failed);
 * }
 * ```
 */
export function recoverMigrations(options: { olderThanMs?: number } = {}): Promise<RecoveryReport> {
	return recoverMigrationsIn(currentState(), options);
}

async function recoverMigrationsIn(s: RouterState, options: { olderThanMs?: number } = {}): Promise<RecoveryReport> {
	const config = getConfig(s);
	const report = await recoverJournal(migrationEnv(s, config, requireJournal(config)), options);
	await republishExceptions(s, config);
	return report;
}

/**
 * Checks the journal against the shards. Observational unless `repair` is set.
 *
 * Reports unfinished moves, live rows left on a shard the key moved off, and
 * tombstones the journal does not know. It also reports `strandedCopies`, live
 * destination rows of an aborted move that equal a row the move journaled, and
 * `staleCutovers`, tombstones whose move the journal ended as aborted. Both come
 * from a recovery executor that stalled past its lease, and both count against
 * `clean`. With `repair: true` it resumes unfinished moves, removes a ghost row
 * only when it equals the archived copy, removes a stranded copy under the same
 * proof, and restores the archived row of a stale cutover when nothing else owns
 * the key. The pass is recorded; {@link purgeRetired} waits for a clean one.
 *
 * @param options - `repair` to act on what it finds
 * @returns The findings
 * @throws {CollegeDBError} `JOURNAL_NOT_CONFIGURED` with several shards and no `journalShard`
 * @throws {CollegeDBError} With `repair`: `RECOVERY_IN_PROGRESS` when another recovery holds the lease; `RECOVERY_FENCED` when this one lost it mid-run
 * @throws {CollegeDBError} With `repair`: `MIGRATION_NO_ATOMIC_BATCH` when a stale cutover is restored on a shard that does not declare `atomicBatch`
 * @since 1.4.0
 */
export function verifyMigrations(options: { repair?: boolean } = {}): Promise<VerifyReport> {
	return verifyMigrationsIn(currentState(), options);
}

async function verifyMigrationsIn(s: RouterState, options: { repair?: boolean } = {}): Promise<VerifyReport> {
	const config = getConfig(s);
	const report = await verifyJournal(migrationEnv(s, config, requireJournal(config)), options);
	if (options.repair) {
		await republishExceptions(s, config);
	}
	return report;
}

/**
 * Reports where the journal says a key stands.
 *
 * @param key - Routing key
 * @param table - Restrict to one table
 * @returns The newest move of the key, or `null` when it was never moved
 * @throws {CollegeDBError} `JOURNAL_NOT_CONFIGURED` with several shards and no `journalShard`
 * @since 1.4.0
 */
export function getMigrationStatus(key: string, table?: string): Promise<MigrationStatus | null> {
	return getMigrationStatusIn(currentState(), key, table);
}

async function getMigrationStatusIn(s: RouterState, key: string, table?: string): Promise<MigrationStatus | null> {
	const config = getConfig(s);
	return await getJournalStatus(migrationEnv(s, config, requireJournal(config)), key, table);
}

/**
 * Clears the archived copy of moved rows once it is safe to.
 *
 * A move is refused until its retention window has passed and a clean
 * {@link verifyMigrations} pass finished after it. The tombstone stays, so a stale
 * writer is still turned away.
 *
 * @returns The moves purged and the ones refused, with the reason
 * @throws {CollegeDBError} `JOURNAL_NOT_CONFIGURED` with several shards and no `journalShard`
 * @since 1.4.0
 */
export function purgeRetired(): Promise<PurgeReport> {
	return purgeRetiredIn(currentState());
}

async function purgeRetiredIn(s: RouterState): Promise<PurgeReport> {
	const config = getConfig(s);
	return await purgeJournal(migrationEnv(s, config, requireJournal(config)));
}

/**
 * Drops the placement epochs no row depends on.
 *
 * An epoch is removed only when a complete ownership census proves it. The fence
 * goes up on every shard first, so a writer on an older topology generation can no
 * longer create a row that only a discarded epoch would find. Then every named
 * table on every shard is read in keyset pages, and each row is attributed to the
 * newest epoch whose owner is the shard it sits on. A row pinned by a placement
 * exception that has a mapping to its shard depends on no epoch. The census runs
 * twice, and the pruned manifest is published with a compare-and-swap only when
 * both passes agree. The newest epoch is never removed. A zero-move
 * {@link rebalance} is not proof and is not accepted in its place.
 *
 * The fence covers builder writes ({@link insertInto}, {@link upsert} and the
 * helpers on them). Raw SQL issued outside CollegeDB, and raw `run()` statements,
 * bypass it; stop those writers before pruning.
 *
 * @param options - Tables to scan, key columns, `dryRun`, `pageSize` and `concurrency`
 * @returns The epochs removed and retained, the rows depending on each, and the rows read
 * @throws {CollegeDBError} `JOURNAL_NOT_CONFIGURED` without `journalShard`; `PRUNE_REFUSED` when the proof is incomplete
 * @since 1.4.0
 * @example
 * ```typescript
 * const preview = await pruneEpochs({ tables: ['users', 'posts'], dryRun: true });
 * if (preview.dependents[1] === 0) {
 *   await pruneEpochs({ tables: ['users', 'posts'] });
 * }
 * ```
 */
export function pruneEpochs(options: PruneOptions): Promise<PruneReport> {
	return pruneEpochsIn(currentState(), options);
}

async function pruneEpochsIn(s: RouterState, options: PruneOptions): Promise<PruneReport> {
	const config = getConfig(s);
	const journal = explicitJournal(config);
	if (!journal) {
		throw new CollegeDBError(
			'Pruning epochs needs a journal. Set journalShard in the configuration to the binding that should hold it.',
			'JOURNAL_NOT_CONFIGURED'
		);
	}

	await ensureJournalSchema(config.shards, journal.binding);

	const mapper = getMapper(s, config);
	let pinned: Set<string>;
	try {
		const rows = await journal.db
			.prepare('SELECT hashed_key FROM collegedb_placement_exceptions WHERE scope = ?')
			.bind(journal.scope)
			.all<{ hashed_key: string }>();
		const kvSet = await loadPlacementExceptions(config.kv, 0, 0, { fresh: true }, s.placement);
		pinned = new Set([...kvSet, ...rows.results.map((row) => row.hashed_key)]);
	} catch (error) {
		throw new CollegeDBError(
			`pruneEpochs refused: the exception set could not be read (${error instanceof Error ? error.message : String(error)})`,
			'PRUNE_REFUSED'
		);
	}

	const { report, published } = await runPrune(
		{
			journal: { db: journal.db, scope: journal.scope },
			shards: config.shards,
			keyColumns: config.keyColumns,
			epochOwner: shardForEpoch,
			isPinned: async (key, shard) => pinned.has(await mapper.hashKey(key)) && (await mapper.getShardMapping(key))?.shard === shard
		},
		options
	);

	if (published) {
		s.placementDecision = { mode: 'computed', manifest: published };
		try {
			await savePlacementManifest(config.kv, published, undefined, s.placement);
		} catch (error) {
			if (config.debug) {
				console.warn('Could not refresh the KV copy of the pruned manifest:', error);
			}
		}
	}

	return report;
}

/**
 * The journal handle placement exceptions are written through, if configured.
 * @private
 */
function placementJournal(config: CollegeDBConfig): { db: SQLDatabase; scope: string } | undefined {
	const journal = explicitJournal(config);
	return journal ? { db: journal.db, scope: journal.scope } : undefined;
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
		const coordinatorId = config.coordinator.idFromName(config.coordinatorScope ?? 'default');
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
export function listKnownShards(): Promise<string[]> {
	return listKnownShardsIn(currentState());
}

async function listKnownShardsIn(s: RouterState): Promise<string[]> {
	const config = getConfig(s);

	// Try to get from coordinator first
	if (config.coordinator) {
		try {
			const coordinatorId = config.coordinator.idFromName(config.coordinatorScope ?? 'default');
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
		const mapper = getMapper(s, config);
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
export function getShardStats(): Promise<ShardStats[]> {
	return getShardStatsIn(currentState());
}

async function getShardStatsIn(s: RouterState): Promise<ShardStats[]> {
	const config = getConfig(s);

	// Try to get from coordinator first
	if (config.coordinator) {
		try {
			const coordinatorId = config.coordinator.idFromName(config.coordinatorScope ?? 'default');
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
	const mapper = getMapper(s, config);
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
export function runShard<T = Record<string, unknown>>(shardBinding: string, sql: string, bindings: any[] = []): Promise<QueryResult<T>> {
	return runShardIn<T>(currentState(), shardBinding, sql, bindings);
}

async function runShardIn<T = Record<string, unknown>>(
	s: RouterState,
	shardBinding: string,
	sql: string,
	bindings: any[] = []
): Promise<QueryResult<T>> {
	const config = getConfig(s);
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
 * @param options - Read guarantee for this call; the shard's bookmark comes back on `meta.bookmark`
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
export function allShard<T = Record<string, unknown>>(
	shardBinding: string,
	sql: string,
	bindings: any[] = [],
	options?: ReadOptions
): Promise<QueryResult<T>> {
	return allShardIn<T>(currentState(), shardBinding, sql, bindings, options);
}

async function allShardIn<T = Record<string, unknown>>(
	s: RouterState,
	shardBinding: string,
	sql: string,
	bindings: any[] = [],
	options?: ReadOptions
): Promise<QueryResult<T>> {
	const config = getConfig(s);
	const db = config.shards[shardBinding];

	if (!db) {
		throw new CollegeDBError(`Shard ${shardBinding} not found`, 'SHARD_NOT_FOUND');
	}

	if (!options) {
		return await db
			.prepare(sql)
			.bind(...bindings)
			.all<T>();
	}

	return await allShardOnSession<T>(sessionsFor(s, options), db, sql, bindings);
}

async function allShardOnSession<T>(sessions: ReadSessions, db: SQLDatabase, sql: string, bindings: any[]): Promise<QueryResult<T>> {
	const result = await sessions
		.on(db)
		.prepare(sql)
		.bind(...bindings)
		.all<T>();

	return sessions.stamp(db, result);
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
 * @param options - Read guarantee for this call
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
export function firstShard<T = Record<string, unknown>>(
	shardBinding: string,
	sql: string,
	bindings: any[] = [],
	options?: ReadOptions
): Promise<T | null> {
	return firstShardIn<T>(currentState(), shardBinding, sql, bindings, options);
}

async function firstShardIn<T = Record<string, unknown>>(
	s: RouterState,
	shardBinding: string,
	sql: string,
	bindings: any[] = [],
	options?: ReadOptions
): Promise<T | null> {
	const config = getConfig(s);
	const db = config.shards[shardBinding];

	if (!db) {
		throw new CollegeDBError(`Shard ${shardBinding} not found`, 'SHARD_NOT_FOUND');
	}

	const result = await sessionsFor(s, options)
		.on(db)
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
export function runAllShards<T = Record<string, unknown>>(
	sql: string,
	bindings: any[] = [],
	batchSize: number = 50
): Promise<QueryResult<T>[]> {
	return runAllShardsIn<T>(currentState(), sql, bindings, batchSize);
}

async function runAllShardsIn<T = Record<string, unknown>>(
	s: RouterState,
	sql: string,
	bindings: any[] = [],
	batchSize: number = 50
): Promise<QueryResult<T>[]> {
	const config = getConfig(s);
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
 * @param options - Read guarantee for this call. Each result carries its shard's bookmark on `meta.bookmark`, in `config.shards` order
 * @returns Promise resolving to an array of results from each shard
 * @since 1.0.4
 */
export function allAllShards<T = Record<string, unknown>>(
	sql: string,
	bindings: any[] = [],
	batchSize: number = 50,
	options?: ReadOptions
): Promise<QueryResult<T>[]> {
	return allAllShardsIn<T>(currentState(), sql, bindings, batchSize, options);
}

async function allAllShardsIn<T = Record<string, unknown>>(
	s: RouterState,
	sql: string,
	bindings: any[] = [],
	batchSize: number = 50,
	options?: ReadOptions
): Promise<QueryResult<T>[]> {
	const config = getConfig(s);
	const sessions = sessionsFor(s, options);
	const tasks: Array<() => Promise<QueryResult<T>>> = [];

	for (const [binding, db] of Object.entries(config.shards)) {
		if (!binding || !db) {
			console.error(`Shard ${binding ?? '<null>'} not found, skipping`);
			continue;
		}

		if (sessions === PLAIN_SESSIONS) {
			tasks.push(() =>
				db
					.prepare(sql)
					.bind(...bindings)
					.all<T>()
					.catch((error) => failedShard<T>(binding, error))
			);
			continue;
		}

		// opened outside the catch below: a refused consistency must throw, not become a failed shard
		const target = sessions.on(db);

		tasks.push(() =>
			stamped(
				sessions,
				db,
				target
					.prepare(sql)
					.bind(...bindings)
					.all<T>()
			).catch((error) => failedShard<T>(binding, error))
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
 * The result a fan-out reports for a shard whose query threw.
 * @private
 */
function failedShard<T>(binding: string, error: unknown): QueryResult<T> {
	console.error(`Error executing query on shard ${binding}:`, error);
	return {
		success: false,
		results: [],
		error: error instanceof Error ? error.message : String(error),
		meta: { duration: 0 }
	} satisfies QueryResult<T>;
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
	s: RouterState,
	build: (dialect: SQLDialect | undefined) => string,
	bindings: any[] = [],
	batchSize: number = 50,
	options?: ReadOptions
): Promise<QueryResult<T>[]> {
	const config = getConfig(s);
	const sessions = sessionsFor(s, options);
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

		const target = sessions.on(db);

		tasks.push(() =>
			stamped(
				sessions,
				db,
				target
					.prepare(statementFor(db.dialect))
					.bind(...bindings)
					.all<T>()
			).catch((error) => {
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
	/**
	 * Read guarantee for every shard query. Each shard gets one session; the merged
	 * `meta.bookmarks` holds one bookmark per database, not one snapshot token.
	 * @since 1.4.0
	 */
	consistency?: ReadConsistency;
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

export function allAllShardsGlobal<T = Record<string, unknown>>(
	sql: string,
	bindings: any[] = [],
	options: GlobalAllShardsOptions<T> = {}
): Promise<QueryResult<T>> {
	return allAllShardsGlobalIn<T>(currentState(), sql, bindings, options);
}

async function allAllShardsGlobalIn<T = Record<string, unknown>>(
	s: RouterState,
	sql: string,
	bindings: any[] = [],
	options: GlobalAllShardsOptions<T> = {}
): Promise<QueryResult<T>> {
	const batchSize = normalizeBatchSize(options.batchSize);
	const offset = normalizeOffset(options.offset);
	const limit = normalizeLimit(options.limit);

	const shared = sharing(s, options);
	const merged = withBookmarks(
		mergeAllShardQueryResults(
			await allAllShardsBuilt<T>(s, (dialect) => pushDownGlobalLimit(sql, options, offset, limit, dialect), bindings, batchSize, shared)
		),
		sessionsFor(s, shared)
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
 * @param options - Read guarantee for this call
 * @returns Promise resolving to an array of first matching records from each shard
 * @since 1.0.4
 */
export function firstAllShards<T = Record<string, unknown>>(
	sql: string,
	bindings: any[] = [],
	batchSize: number = 50,
	options?: ReadOptions
): Promise<(T | null)[]> {
	return firstAllShardsIn<T>(currentState(), sql, bindings, batchSize, options);
}

async function firstAllShardsIn<T = Record<string, unknown>>(
	s: RouterState,
	sql: string,
	bindings: any[] = [],
	batchSize: number = 50,
	options?: ReadOptions
): Promise<(T | null)[]> {
	const config = getConfig(s);
	const sessions = sessionsFor(s, options);
	const tasks: Array<() => Promise<T | null>> = [];

	for (const [binding, db] of Object.entries(config.shards)) {
		if (!binding || !db) {
			console.error(`Shard ${binding ?? '<null>'} not found, skipping`);
			continue;
		}

		const target = sessions.on(db);

		tasks.push(() =>
			target
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
export function firstAllShardsGlobal<T = Record<string, unknown>>(
	sql: string,
	bindings: any[] = [],
	options: Omit<GlobalAllShardsOptions<T>, 'limit'> = {}
): Promise<T | null> {
	return firstAllShardsGlobalIn<T>(currentState(), sql, bindings, options);
}

async function firstAllShardsGlobalIn<T = Record<string, unknown>>(
	s: RouterState,
	sql: string,
	bindings: any[] = [],
	options: Omit<GlobalAllShardsOptions<T>, 'limit'> = {}
): Promise<T | null> {
	const merged = await allAllShardsGlobalIn<T>(s, sql, bindings, {
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
export function flush(): Promise<void> {
	return flushIn(currentState());
}

async function flushIn(s: RouterState): Promise<void> {
	const config = getConfig(s);
	const mapper = getMapper(s, config);

	await mapper.clearAllMappings();
	s.shardSizeCache.clear();
	// The known-shard registration is memoized per process, so a flush has to
	// let the next initialize re-register rather than assume it already did.
	s.syncedShardSets.clear();

	// Also flush coordinator if available
	if (config.coordinator) {
		try {
			const coordinatorId = config.coordinator.idFromName(config.coordinatorScope ?? 'default');
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
export function getDatabaseSizeForShard(shardBinding: string): Promise<number> {
	return getDatabaseSizeForShardIn(currentState(), shardBinding);
}

async function getDatabaseSizeForShardIn(s: RouterState, shardBinding: string): Promise<number> {
	const config = getConfig(s);
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
function dialectOf(s: RouterState, binding: string | undefined): SQLDialect | undefined {
	if (!binding) {
		return undefined;
	}
	return s.config?.shards[binding]?.dialect;
}

/**
 * Dialect of the shard a key routes to.
 * @private
 */
async function dialectForKey(s: RouterState, key: string, operationType: OperationType = 'write'): Promise<SQLDialect | undefined> {
	const candidates = await resolveCandidates(s, key, operationType);
	return dialectOf(s, candidates[0]);
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
export function index<T = Record<string, unknown>>(
	key: string,
	table: string,
	columns: string | string[] | IndexColumnDefinition[],
	options: Omit<CreateIndexOptions, 'batchSize'> = {}
): Promise<QueryResult<T>> {
	return indexIn<T>(currentState(), key, table, columns, options);
}

async function indexIn<T = Record<string, unknown>>(
	s: RouterState,
	key: string,
	table: string,
	columns: string | string[] | IndexColumnDefinition[],
	options: Omit<CreateIndexOptions, 'batchSize'> = {}
): Promise<QueryResult<T>> {
	const sql = buildCreateIndexSQL(table, columns, options, await dialectForKey(s, key));
	return runIn<T>(s, key, sql);
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
export function indexShard<T = Record<string, unknown>>(
	shardBinding: string,
	table: string,
	columns: string | string[] | IndexColumnDefinition[],
	options: Omit<CreateIndexOptions, 'batchSize'> = {}
): Promise<QueryResult<T>> {
	return indexShardIn<T>(currentState(), shardBinding, table, columns, options);
}

async function indexShardIn<T = Record<string, unknown>>(
	s: RouterState,
	shardBinding: string,
	table: string,
	columns: string | string[] | IndexColumnDefinition[],
	options: Omit<CreateIndexOptions, 'batchSize'> = {}
): Promise<QueryResult<T>> {
	const sql = buildCreateIndexSQL(table, columns, options, dialectOf(s, shardBinding));
	return runShardIn<T>(s, shardBinding, sql);
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
export function indexAllShards<T = Record<string, unknown>>(
	table: string,
	columns: string | string[] | IndexColumnDefinition[],
	options: CreateIndexOptions = {}
): Promise<QueryResult<T>[]> {
	return indexAllShardsIn<T>(currentState(), table, columns, options);
}

async function indexAllShardsIn<T = Record<string, unknown>>(
	s: RouterState,
	table: string,
	columns: string | string[] | IndexColumnDefinition[],
	options: CreateIndexOptions = {}
): Promise<QueryResult<T>[]> {
	const config = getConfig(s);
	const batchSize = normalizeBatchSize(options.batchSize);
	const tasks = Object.entries(config.shards)
		.filter(([binding, db]) => binding && db)
		.map(([binding, db]) => async () => {
			const sql = buildCreateIndexSQL(table, columns, options, db.dialect);
			return await runShardIn<T>(s, binding, sql);
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
export function explain<T = Record<string, unknown>>(
	key: string,
	sql: string,
	bindings: any[] = [],
	options: Omit<ExplainOptions, 'batchSize'> = {}
): Promise<QueryResult<T>> {
	return explainIn<T>(currentState(), key, sql, bindings, options);
}

async function explainIn<T = Record<string, unknown>>(
	s: RouterState,
	key: string,
	sql: string,
	bindings: any[] = [],
	options: Omit<ExplainOptions, 'batchSize'> = {}
): Promise<QueryResult<T>> {
	return allIn<T>(s, key, buildExplainSQL(sql, options.mode), bindings);
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
export function explainShard<T = Record<string, unknown>>(
	shardBinding: string,
	sql: string,
	bindings: any[] = [],
	options: Omit<ExplainOptions, 'batchSize'> = {}
): Promise<QueryResult<T>> {
	return explainShardIn<T>(currentState(), shardBinding, sql, bindings, options);
}

async function explainShardIn<T = Record<string, unknown>>(
	s: RouterState,
	shardBinding: string,
	sql: string,
	bindings: any[] = [],
	options: Omit<ExplainOptions, 'batchSize'> = {}
): Promise<QueryResult<T>> {
	return allShardIn<T>(s, shardBinding, buildExplainSQL(sql, options.mode), bindings);
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
export function explainAllShards<T = Record<string, unknown>>(
	sql: string,
	bindings: any[] = [],
	options: ExplainOptions = {}
): Promise<QueryResult<T>[]> {
	return explainAllShardsIn<T>(currentState(), sql, bindings, options);
}

async function explainAllShardsIn<T = Record<string, unknown>>(
	s: RouterState,
	sql: string,
	bindings: any[] = [],
	options: ExplainOptions = {}
): Promise<QueryResult<T>[]> {
	return allAllShardsIn<T>(s, buildExplainSQL(sql, options.mode), bindings, normalizeBatchSize(options.batchSize));
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
 * @param options - Read guarantee for this call
 * @returns Row count for that routed shard
 * @since 1.1.4
 */
export function count(key: string, table: string, options?: ReadOptions): Promise<number> {
	return countIn(currentState(), key, table, options);
}

async function countIn(s: RouterState, key: string, table: string, options?: ReadOptions): Promise<number> {
	const quotedTable = quoteIdentifier(table, await dialectForKey(s, key, 'read'));
	const row = await firstIn<{ row_count?: number | string }>(s, key, `SELECT COUNT(*) AS row_count FROM ${quotedTable}`, [], options);
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
 * @param options - Read guarantee for this call
 * @returns Row count for the shard
 * @since 1.1.4
 */
export function countShard(shardBinding: string, table: string, options?: ReadOptions): Promise<number> {
	return countShardIn(currentState(), shardBinding, table, options);
}

async function countShardIn(s: RouterState, shardBinding: string, table: string, options?: ReadOptions): Promise<number> {
	const quotedTable = quoteIdentifier(table, dialectOf(s, shardBinding));
	const row = await firstShardIn<{ row_count?: number | string }>(
		s,
		shardBinding,
		`SELECT COUNT(*) AS row_count FROM ${quotedTable}`,
		[],
		options
	);
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
 * @param options - Read guarantee for this call; per-database bookmarks come back as `bookmarks`
 * @returns Per-shard counts and global total
 * @since 1.1.4
 */
export function countAllShards(
	table: string,
	batchSize: number = 50,
	options?: ReadOptions
): Promise<{ total: number; shards: ShardTableCount[]; bookmarks?: Record<string, string> }> {
	return countAllShardsIn(currentState(), table, batchSize, options);
}

async function countAllShardsIn(
	s: RouterState,
	table: string,
	batchSize: number = 50,
	options?: ReadOptions
): Promise<{ total: number; shards: ShardTableCount[]; bookmarks?: Record<string, string> }> {
	const config = getConfig(s);
	const normalizedBatchSize = normalizeBatchSize(batchSize);
	const tasks: Array<() => Promise<ShardTableCount>> = [];
	const quoter = tableQuoter(config, table);
	const sessions = sessionsFor(s, options);

	for (const [binding, db] of Object.entries(config.shards)) {
		if (!binding || !db) {
			continue;
		}

		const probeSql = `SELECT COUNT(*) AS row_count FROM ${quoter(db)}`;
		// opened outside the try below: a refused consistency must throw, not become a failed shard
		const target = sessions.on(db);

		tasks.push(async () => {
			try {
				const row = await target.prepare(probeSql).first<{ row_count?: number | string }>();
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
	const bookmarks = sessions.bookmarks();
	return bookmarks ? { total, shards, bookmarks } : { total, shards };
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
export function getDatabaseSizeForKey(key: string): Promise<number> {
	return getDatabaseSizeForKeyIn(currentState(), key);
}

async function getDatabaseSizeForKeyIn(s: RouterState, key: string): Promise<number> {
	const config = getConfig(s);
	const shardBinding = await getShardForKey(s, key, 'read');
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
export function getDatabaseSizesAllShards(batchSize: number = 50): Promise<ShardSizeResult[]> {
	return getDatabaseSizesAllShardsIn(currentState(), batchSize);
}

async function getDatabaseSizesAllShardsIn(s: RouterState, batchSize: number = 50): Promise<ShardSizeResult[]> {
	const config = getConfig(s);
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
export function getTotalDatabaseSize(batchSize: number = 50): Promise<number> {
	return getTotalDatabaseSizeIn(currentState(), batchSize);
}

async function getTotalDatabaseSizeIn(s: RouterState, batchSize: number = 50): Promise<number> {
	const sizes = await getDatabaseSizesAllShardsIn(s, batchSize);
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
 * @param options - Read guarantee for this call. The routed read and the fallback share one session per shard
 * @returns The first matching row, or `null` when neither path finds one
 * @since 1.2.4
 * @example
 * ```typescript
 * const user = await firstResilient<User>('user-123', 'SELECT * FROM users WHERE id = ?', ['user-123']);
 * ```
 */
export function firstResilient<T = Record<string, unknown>>(
	key: string,
	sql: string,
	bindings: any[] = [],
	options?: ReadOptions
): Promise<T | null> {
	return firstResilientIn<T>(currentState(), key, sql, bindings, options);
}

async function firstResilientIn<T = Record<string, unknown>>(
	s: RouterState,
	key: string,
	sql: string,
	bindings: any[] = [],
	options?: ReadOptions
): Promise<T | null> {
	const shared = sharing(s, options);
	const routed = await firstIn<T>(s, key, sql, bindings, shared);
	if (routed) {
		return routed;
	}

	return (await firstAllShardsGlobalIn<T>(s, sql, bindings, shared)) ?? null;
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
 * @since 1.3.0
 * @example
 * ```typescript
 * await query('INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Ada']);
 * await query('UPDATE users SET name = ? WHERE id = ?', ['Ada L.', 'user-1']);
 * await query('DELETE FROM users WHERE id IN (?, ?)', ['user-1', 'user-2']);
 * ```
 */
export function query<T = Record<string, unknown>>(sql: string, bindings: any[] = []): Promise<QueryResult<T>> {
	return queryIn<T>(currentState(), sql, bindings);
}

async function queryIn<T = Record<string, unknown>>(s: RouterState, sql: string, bindings: any[] = []): Promise<QueryResult<T>> {
	const config = getConfig(s);
	const plan = planQuery(sql, bindings, { keyColumns: config.keyColumns });

	if (!plan) {
		if ((config.onUnroutable ?? 'throw') === 'throw') {
			throw unroutableError(sql);
		}
		return mergeAllShardQueryResults(await allAllShardsIn<T>(s, sql, bindings));
	}

	if (plan.keys.length === 1) {
		return plan.readOnly ? await allIn<T>(s, plan.keys[0]!, sql, bindings) : await runIn<T>(s, plan.keys[0]!, sql, bindings);
	}

	const grouped = await batchIn<T>(
		s,
		plan.keys.map((key) => ({ key, sql, bindings }))
	);
	return mergeAllShardQueryResults(grouped.flatMap((group) => group.results));
}

/**
 * Reads the first matching row, taking the routing key from the statement.
 *
 * @template T - Type of the result record
 * @param sql - Statement text using `?` placeholders
 * @param bindings - Positional bindings for the statement
 * @param options - Read guarantee for this call
 * @returns The first matching row, or `null`
 * @throws {CollegeDBError} If the routing key cannot be determined and `onUnroutable` is `throw`
 * @since 1.3.0
 * @example
 * ```typescript
 * const user = await queryFirst<User>('SELECT * FROM users WHERE id = ?', ['user-1']);
 * ```
 */
export function queryFirst<T = Record<string, unknown>>(sql: string, bindings: any[] = [], options?: ReadOptions): Promise<T | null> {
	return queryFirstIn<T>(currentState(), sql, bindings, options);
}

async function queryFirstIn<T = Record<string, unknown>>(
	s: RouterState,
	sql: string,
	bindings: any[] = [],
	options?: ReadOptions
): Promise<T | null> {
	const config = getConfig(s);
	const plan = planQuery(sql, bindings, { keyColumns: config.keyColumns });

	if (!plan) {
		if ((config.onUnroutable ?? 'throw') === 'throw') {
			throw unroutableError(sql);
		}
		const rows = await firstAllShardsIn<T>(s, sql, bindings, 50, options);
		return rows.find((row): row is T => row !== null) ?? null;
	}

	if (plan.keys.length === 1) {
		return await firstIn<T>(s, plan.keys[0]!, sql, bindings, options);
	}

	const shared = sharing(s, options);
	for (const key of plan.keys) {
		const row = await firstIn<T>(s, key, sql, bindings, shared);
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
 * @param options - Read guarantee for this call. A statement spanning shards gets one session per shard and `meta.bookmarks`
 * @returns Rows from every shard the statement routes to
 * @throws {CollegeDBError} If the routing key cannot be determined and `onUnroutable` is `throw`
 * @since 1.3.0
 * @example
 * ```typescript
 * const { results } = await queryAll<User>('SELECT * FROM users WHERE id IN (?, ?)', ['user-1', 'user-2']);
 * ```
 */
export function queryAll<T = Record<string, unknown>>(sql: string, bindings: any[] = [], options?: ReadOptions): Promise<QueryResult<T>> {
	return queryAllIn<T>(currentState(), sql, bindings, options);
}

async function queryAllIn<T = Record<string, unknown>>(
	s: RouterState,
	sql: string,
	bindings: any[] = [],
	options?: ReadOptions
): Promise<QueryResult<T>> {
	const config = getConfig(s);
	const plan = planQuery(sql, bindings, { keyColumns: config.keyColumns });
	const shared = sharing(s, options);

	if (!plan) {
		if ((config.onUnroutable ?? 'throw') === 'throw') {
			throw unroutableError(sql);
		}
		return withBookmarks(mergeAllShardQueryResults(await allAllShardsIn<T>(s, sql, bindings, 50, shared)), sessionsFor(s, shared));
	}

	if (plan.keys.length === 1) {
		return await allIn<T>(s, plan.keys[0]!, sql, bindings, shared);
	}

	const perKey = await Promise.all(plan.keys.map((key) => allIn<T>(s, key, sql, bindings, shared)));
	return withBookmarks(mergeAllShardQueryResults(perKey), sessionsFor(s, shared));
}

/**
 * One routed statement in a {@link batch}.
 * @since 1.3.0
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
 * @since 1.3.0
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
 * @since 1.3.0
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
async function resolveBatchShards(s: RouterState, config: CollegeDBConfig, entries: BatchEntry[]): Promise<string[]> {
	// a held decision serves reads and writes alike, so the statements are classified only when a refresh may be due
	const decision =
		heldDecision(s, true) ??
		(await resolvePlacement(
			s,
			config,
			entries.some((entry) => getOperationType(entry.sql) === 'write')
		));

	// Computed placement reads no mappings at all, and a write finds the shard that holds its row.
	if (decision.mode === 'computed' && decision.manifest) {
		const located = new Map<string, string>();
		const shards: string[] = [];

		for (const entry of entries) {
			const operationType = getOperationType(entry.sql);
			if (operationType !== 'write') {
				shards.push(await getShardForKey(s, entry.key, operationType));
				continue;
			}

			const table = statementTable(entry.sql);
			const id = `${entry.key}\u0000${table}`;
			if (!located.has(id)) {
				located.set(id, await resolveWriteShard(s, config, entry.key, table));
			}
			shards.push(located.get(id)!);
		}

		return shards;
	}

	const mapper = getMapper(s, config);
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
		const shard = await allocateShardForKey(s, config, entry.key, operationType);
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

export function batch<T = Record<string, unknown>>(entries: BatchEntry[]): Promise<BatchShardResult<T>[]> {
	return batchIn<T>(currentState(), entries);
}

async function batchIn<T = Record<string, unknown>>(s: RouterState, entries: BatchEntry[]): Promise<BatchShardResult<T>[]> {
	const config = getConfig(s);

	if (entries.length === 0) {
		return [];
	}

	const groups = new Map<string, number[]>();
	const shards = await resolveBatchShards(s, config, entries);

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
	/**
	 * Latest bookmark per database, keyed by database id, when the call used a
	 * read session. These are separate sessions: two shards, or a later page, are
	 * never one snapshot.
	 * @since 1.4.0
	 */
	bookmarks?: Record<string, string>;
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
 * With `consistency` set, each shard runs its query in one session. The
 * sessions are separate: the page is not a snapshot across shards, and two
 * calls for two pages are not one snapshot either.
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
export function paginate<T = Record<string, unknown>>(
	sql: string,
	bindings: any[] = [],
	options: PaginateOptions<T> = {}
): Promise<PaginatedResult<T>> {
	return paginateIn<T>(currentState(), sql, bindings, options);
}

async function paginateIn<T = Record<string, unknown>>(
	s: RouterState,
	sql: string,
	bindings: any[] = [],
	options: PaginateOptions<T> = {}
): Promise<PaginatedResult<T>> {
	const limit = normalizeLimit(options.limit) ?? 20;
	const page = Math.max(1, Math.floor(options.page ?? 1));
	const offset = (page - 1) * limit;

	const merged = await allAllShardsGlobalIn<T>(s, sql, bindings, {
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
		pages,
		...(merged.meta.bookmarks ? { bookmarks: merged.meta.bookmarks } : {})
	};
}

/**
 * Outcome of a {@link rebalance} pass.
 * @since 1.3.0
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
 * @since 1.3.0
 * @example
 * ```typescript
 * const result = await rebalance('users', { dryRun: true });
 * console.log(`${result.agreed}/${result.examined} keys already agree`);
 * ```
 */
export function rebalance(table: string, options: { concurrency?: number; dryRun?: boolean } = {}): Promise<RebalanceResult> {
	return rebalanceIn(currentState(), table, options);
}

async function rebalanceIn(
	s: RouterState,
	table: string,
	options: { concurrency?: number; dryRun?: boolean } = {}
): Promise<RebalanceResult> {
	const config = getConfig(s);
	const mapper = getMapper(s, config);
	const shards = Object.keys(config.shards);

	if (shards.length === 0) {
		throw new CollegeDBError('No shards configured', 'NO_SHARDS');
	}

	if (!options.dryRun) {
		requireJournal(config);
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
				// a draining shard is not a target (the size limit never applied here)
				const target = hrwShard(key, await selectEligibleShards(s, config, 'allocate', key, false));

				if (!mapping || mapping.shard === target) {
					result.agreed++;
					continue;
				}

				if (!options.dryRun) {
					await reassignShardIn(s, key, target, table);
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
async function maxColumnAcrossShards(s: RouterState, table: string, column: string): Promise<number> {
	const results = await allAllShardsBuilt<{ max_value: number | string | null }>(
		s,
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
export function nextId(table: string, options: NextIdOptions = {}): Promise<number> {
	return nextIdIn(currentState(), table, options);
}

async function nextIdIn(s: RouterState, table: string, options: NextIdOptions = {}): Promise<number> {
	const config = getConfig(s);
	const column = options.column ?? 'id';
	const min = Math.floor(options.min ?? 0);

	if (config.coordinator) {
		// Ask the sequence first. Once it is seeded it is already ahead of every
		// row in every shard, so computing the cross-shard MAX to seed it again
		// would spend one query per shard on every id.
		const seeded = await requestSequence(s, config, table, min, false);
		if (seeded !== undefined) {
			return seeded;
		}

		try {
			const base = await maxColumnAcrossShards(s, table, column);
			const floor = Math.max(base + 1, min);
			const value = await requestSequence(s, config, table, floor, true);
			if (value !== undefined) {
				return value;
			}
			return floor;
		} catch (error) {
			console.warn('Coordinator sequence allocation failed, falling back to cross-shard MAX:', error);
		}
	}

	const base = await maxColumnAcrossShards(s, table, column);
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
async function requestSequence(
	s: RouterState,
	config: CollegeDBConfig,
	table: string,
	min: number,
	seed: boolean
): Promise<number | undefined> {
	if (!config.coordinator) {
		return undefined;
	}

	const started = phaseStart(s.sink);
	try {
		const coordinatorId = config.coordinator.idFromName(config.coordinatorScope ?? 'default');
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
		phaseEnd('coordinator.fetch', started, 'sequence', s.sink);
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
export function ensureSchema(schema: string | string[], options: EnsureSchemaOptions = {}): Promise<void> {
	return ensureSchemaIn(currentState(), schema, options);
}

async function ensureSchemaIn(s: RouterState, schema: string | string[], options: EnsureSchemaOptions = {}): Promise<void> {
	const config = getConfig(s);
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
		if (s.ensuredSchemaFingerprints.has(fingerprint)) {
			return;
		}
		await createSchemaAcrossShards(config.shards, schemaSql);
		s.ensuredSchemaFingerprints.add(fingerprint);
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
	return initializeFromEnvIn(currentState(), env, options);
}

function initializeFromEnvIn(s: RouterState, env: Record<string, unknown>, options: InitializeFromEnvOptions = {}): string[] {
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

	initializeIn(s, config);
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
 * Options for {@link insert} and {@link insertShard}.
 * @since 1.4.0
 */
export interface InsertOptions extends IdColumnOptions {
	/**
	 * Table the INSERT writes to. When set and the generated id collides with a
	 * mapping on another shard, the call deletes the row it just wrote (matched on
	 * `idColumn`, default `id`) before it throws. It is validated before the
	 * INSERT runs. Without it, the refused row stays on the shard and the error
	 * names its shard and id.
	 */
	table?: string;
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
export function insertInto<T = Record<string, unknown>>(
	key: string,
	table: string,
	values: ColumnValues,
	options: BuildInsertOptions = {}
): Promise<QueryResult<T>> {
	return insertIntoIn<T>(currentState(), key, table, values, options);
}

async function insertIntoIn<T = Record<string, unknown>>(
	s: RouterState,
	key: string,
	table: string,
	values: ColumnValues,
	options: BuildInsertOptions = {}
): Promise<QueryResult<T>> {
	return await builderWrite<T>(s, key, table, 'insert', (dialect, guard) =>
		guard ? buildGuardedInsert(table, values, guard, { ...options, dialect }) : buildInsert(table, values, { ...options, dialect })
	);
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
export function insertReturning<T = Record<string, unknown>>(
	key: string,
	table: string,
	values: ColumnValues,
	options: BuildInsertOptions & { idColumn?: string } = {}
): Promise<T | null> {
	return insertReturningIn<T>(currentState(), key, table, values, options);
}

async function insertReturningIn<T = Record<string, unknown>>(
	s: RouterState,
	key: string,
	table: string,
	values: ColumnValues,
	options: BuildInsertOptions & { idColumn?: string } = {}
): Promise<T | null> {
	const idColumn = options.idColumn ?? 'id';
	await insertIntoIn(s, key, table, values, options);

	const idValue = values[idColumn] ?? key;
	const dialect = await dialectForKey(s, key, 'read');
	return await firstIn<T>(s, key, `SELECT * FROM ${quoteIdentifier(table, dialect)} WHERE ${quoteIdentifier(idColumn, dialect)} = ?`, [
		idValue
	]);
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
export function updateRow<T = Record<string, unknown>>(
	key: string,
	table: string,
	values: ColumnValues,
	where: ColumnValues,
	options: CrudReturningOptions = {}
): Promise<QueryResult<T>> {
	return updateRowIn<T>(currentState(), key, table, values, where, options);
}

async function updateRowIn<T = Record<string, unknown>>(
	s: RouterState,
	key: string,
	table: string,
	values: ColumnValues,
	where: ColumnValues,
	options: CrudReturningOptions = {}
): Promise<QueryResult<T>> {
	return await builderWrite<T>(s, key, table, 'mutate', (dialect) => buildUpdate(table, values, where, { ...options, dialect }));
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
export function patch<T = Record<string, unknown>>(
	key: string,
	table: string,
	id: string | number,
	values: ColumnValues,
	options: IdColumnOptions = {}
): Promise<QueryResult<T>> {
	return patchIn<T>(currentState(), key, table, id, values, options);
}

async function patchIn<T = Record<string, unknown>>(
	s: RouterState,
	key: string,
	table: string,
	id: string | number,
	values: ColumnValues,
	options: IdColumnOptions = {}
): Promise<QueryResult<T>> {
	const idColumn = options.idColumn ?? 'id';
	return await updateRowIn<T>(s, key, table, values, { [idColumn]: id }, { returning: options.returning });
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
export function deleteRow<T = Record<string, unknown>>(key: string, table: string, where: ColumnValues): Promise<QueryResult<T>> {
	return deleteRowIn<T>(currentState(), key, table, where);
}

async function deleteRowIn<T = Record<string, unknown>>(
	s: RouterState,
	key: string,
	table: string,
	where: ColumnValues
): Promise<QueryResult<T>> {
	return await builderWrite<T>(s, key, table, 'mutate', (dialect) => buildDelete(table, where, { dialect }));
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
export function deleteById<T = Record<string, unknown>>(
	key: string,
	table: string,
	id: string | number,
	options: { idColumn?: string } = {}
): Promise<QueryResult<T>> {
	return deleteByIdIn<T>(currentState(), key, table, id, options);
}

async function deleteByIdIn<T = Record<string, unknown>>(
	s: RouterState,
	key: string,
	table: string,
	id: string | number,
	options: { idColumn?: string } = {}
): Promise<QueryResult<T>> {
	const idColumn = options.idColumn ?? 'id';
	return await deleteRowIn<T>(s, key, table, { [idColumn]: id });
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
export function upsert<T = Record<string, unknown>>(
	key: string,
	table: string,
	values: ColumnValues,
	conflictColumns: string | string[],
	options: BuildUpsertOptions = {}
): Promise<QueryResult<T>> {
	return upsertIn<T>(currentState(), key, table, values, conflictColumns, options);
}

async function upsertIn<T = Record<string, unknown>>(
	s: RouterState,
	key: string,
	table: string,
	values: ColumnValues,
	conflictColumns: string | string[],
	options: BuildUpsertOptions = {}
): Promise<QueryResult<T>> {
	return await builderWrite<T>(s, key, table, 'upsert', (dialect, guard) =>
		guard
			? buildGuardedUpsert(table, values, conflictColumns, guard, { ...options, dialect })
			: buildUpsert(table, values, conflictColumns, { ...options, dialect })
	);
}

/**
 * Generation of a writer that routes through KV mappings. Mapped keys do not
 * depend on an epoch, so the topology fence never turns such a writer away.
 * @private
 */
const MAPPED_WRITER_GENERATION = Number.MAX_SAFE_INTEGER;

/**
 * Runs a builder-issued write on the shard that owns the key.
 *
 * Without `journalShard` this is the plain routed statement. With it, inserts
 * and upserts carry the tombstone guard and the writer's topology generation, and
 * a write that changes no row is explained before it is allowed to count as done.
 * A tombstone means the route was stale, so the key is resolved again once and the
 * write retried on a different shard, or refused with `MIGRATION_CUTOVER_PENDING`.
 * A raised fence means the writer's manifest is older than a pruning client's, so
 * the manifest is reloaded and the write retried once, then refused with
 * `STALE_TOPOLOGY`. Anything else is a genuine conflict and is returned.
 * Shards on a dialect the journal is not proven on keep the plain statement.
 *
 * @private
 */
async function builderWrite<T>(
	s: RouterState,
	key: string,
	table: string,
	kind: 'insert' | 'upsert' | 'mutate',
	build: (dialect: SQLDialect | undefined, guard: TombstoneGuard | null) => BuiltQuery
): Promise<QueryResult<T>> {
	const config = getConfig(s);
	const journal = explicitJournal(config);

	if (!journal) {
		const { sql, bindings } = build(await dialectForKey(s, key), null);
		return await runIn<T>(s, key, sql, bindings);
	}

	let rerouted = false;
	let reloaded = false;

	for (;;) {
		const target = await resolveWriteTarget(s, config, key, table);

		if (!isProvenJournalDialect(target.db.dialect)) {
			const { sql, bindings } = build(target.db.dialect, null);
			return await runIn<T>(s, key, sql, bindings);
		}

		await ensureShardTables(target.db);
		const identity = { scope: journal.scope, table, key, generation: target.generation };
		const guard = kind === 'mutate' ? null : identity;
		const result = await runGuardedWrite<T>(target.db, identity, build(target.db.dialect, guard));

		if (!result.success) {
			throw new CollegeDBError(`Query failed: ${result.error || 'Unknown error'}`, 'QUERY_FAILED');
		}

		const changes = result.meta.changes;
		if (typeof changes === 'number' && changes > 0) {
			return result;
		}

		// zero rows, or a driver that does not say: only a tombstone explains it
		if (!(await findTombstone(target.db, journal.scope, key, table))) {
			if (guard && (await readFence(target.db)) > target.generation) {
				if (!reloaded) {
					reloaded = true;
					await reloadPlacement(s, config);
					continue;
				}

				throw new CollegeDBError(
					`Key ${key} in ${table}: ${target.binding} only accepts writers on a newer topology than generation ${target.generation}, even after reloading the manifest. The write was not applied; retry it.`,
					'STALE_TOPOLOGY'
				);
			}

			return result;
		}

		if (!rerouted) {
			rerouted = true;
			await getMapper(s, config).invalidateCachedMapping(key);
			if ((await resolveWriteTarget(s, config, key, table)).binding !== target.binding) {
				continue;
			}
		}

		throw new CollegeDBError(
			`Key ${key} in ${table} moved off ${target.binding} and its new owner is not published yet. The write was not applied; retry it, or run recoverMigrations() if it keeps failing.`,
			'MIGRATION_CUTOVER_PENDING'
		);
	}
}

/**
 * Resolves the shard a write for `key` goes to, with its binding name.
 * @private
 */
async function resolveWriteTarget(
	s: RouterState,
	config: CollegeDBConfig,
	key: string,
	table: string
): Promise<{ binding: string; db: SQLDatabase; generation: number }> {
	const decision = await resolvePlacement(s, config, true);
	const binding =
		decision.mode === 'computed' && decision.manifest
			? await resolveWriteShard(s, config, key, table)
			: await getShardForKey(s, key, 'write');
	const db = config.shards[binding];

	if (!db) {
		throw new CollegeDBError(`Shard ${binding} not found in configuration`, 'SHARD_NOT_FOUND');
	}

	// read after the candidates, which may have adopted a newer manifest
	const manifest = s.placementDecision?.manifest;
	return { binding, db, generation: manifest ? manifest.epochs[0]!.n : MAPPED_WRITER_GENERATION };
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
function resolveKV(s: RouterState, explicit?: KVStorage): KVStorage {
	const kv = explicit ?? getActiveConfigIn(s)?.kv;
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
export function setLookup(key: string, value: string, options: LookupOptions = {}): Promise<void> {
	return setLookupIn(currentState(), key, value, options);
}

async function setLookupIn(s: RouterState, key: string, value: string, options: LookupOptions = {}): Promise<void> {
	const kv = resolveKV(s, options.kv);
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
export function getLookup(key: string, options: LookupOptions = {}): Promise<string | null> {
	return getLookupIn(currentState(), key, options);
}

async function getLookupIn(s: RouterState, key: string, options: LookupOptions = {}): Promise<string | null> {
	const kv = resolveKV(s, options.kv);
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
export function deleteLookup(key: string, options: LookupOptions = {}): Promise<void> {
	return deleteLookupIn(currentState(), key, options);
}

async function deleteLookupIn(s: RouterState, key: string, options: LookupOptions = {}): Promise<void> {
	const kv = resolveKV(s, options.kv);
	await kv.delete(lookupKey(key, options.namespace));
}

/**
 * Creates a client: the router API bound to a configuration of its own.
 *
 * A client behaves as if it ran in a separate process. It shares no mapper,
 * mapping cache, placement manifest or epoch, shard size cache, schema record,
 * migration cache or phase observer with the module-level API, a scoped
 * `collegedb(config, callback)` or any other client, even when they are
 * given the same providers. Every method works the same as the function of the
 * same name, and `cached`, `invalidate` and `reconcileAllocations` use the
 * client's KV store, placement cache and coordinator.
 *
 * Nothing about a client needs `AsyncLocalStorage`: state travels with the
 * object, so it is the isolation-safe choice on runtimes without an async context,
 * and the one to use when several configurations serve requests in one isolate.
 * The sync part of the configuration is applied before it returns, like
 * {@link initialize}. Call `initializeAsync` on the client to wait for the
 * background checks.
 *
 * @param config - Configuration the client routes with
 * @returns The client
 * @throws {CollegeDBError} `INVALID_JOURNAL_SHARD` when `journalShard` is not a configured shard
 * @since 1.4.0
 * @example
 * ```typescript
 * const tenantA = createClient({ kv: env.KV_A, shards: { 'db-a1': env.DB_A1, 'db-a2': env.DB_A2 } });
 * const tenantB = createClient({ kv: env.KV_B, shards: { 'db-b1': env.DB_B1 } });
 *
 * await Promise.all([tenantA.run('user-1', 'INSERT INTO users (id) VALUES (?)', ['user-1']), tenantB.first('user-9', 'SELECT * FROM users WHERE id = ?', ['user-9'])]);
 * ```
 */
export function createClient(config: CollegeDBConfig) {
	const s = createRouterState();
	initializeIn(s, config);

	return {
		/** Same as {@link initialize}, on this client. */
		initialize(config: CollegeDBConfig) {
			return initializeIn(s, config);
		},

		/** Same as `resetConfig`, on this client. */
		resetConfig(): void {
			resetConfigIn(s);
		},

		/** Same as {@link isInitialized}, on this client. */
		isInitialized(): boolean {
			return isInitializedIn(s);
		},

		/** Same as {@link getActiveConfig}, on this client. */
		getActiveConfig(): CollegeDBConfig | null {
			return getActiveConfigIn(s);
		},

		/** Same as {@link publishEligibility}, on this client. */
		publishEligibility(): Promise<{ eligible: string[]; published: boolean; epoch: number }> {
			return publishEligibilityIn(s);
		},

		/** Same as {@link pruneEpochs}, on this client. */
		pruneEpochs(options: PruneOptions): Promise<PruneReport> {
			return pruneEpochsIn(s, options);
		},

		/** Same as {@link flush}, on this client. */
		flush(): Promise<void> {
			return flushIn(s);
		},

		/** Same as {@link ensureSchema}, on this client. */
		ensureSchema(schema: string | string[], options: EnsureSchemaOptions = {}): Promise<void> {
			return ensureSchemaIn(s, schema, options);
		},

		/** Same as {@link initializeAsync}, on this client. */
		initializeAsync(config: CollegeDBConfig) {
			return initializeAsyncIn(s, config);
		},

		/** Same as {@link prepare}, on this client. */
		prepare(key: string, sql: string): Promise<PreparedStatement> {
			return prepareIn(s, key, sql);
		},

		/** Same as {@link run}, on this client. */
		run<T = Record<string, unknown>>(key: string, sql: string, bindings: any[] = []): Promise<QueryResult<T>> {
			return runIn<T>(s, key, sql, bindings);
		},

		/** Same as {@link insert}, on this client. */
		insert<T = Record<string, unknown>>(sql: string, bindings: any[] = [], options: InsertOptions = {}): Promise<InsertResult<T>> {
			return insertIn<T>(s, sql, bindings, options);
		},

		/** Same as {@link insertShard}, on this client. */
		insertShard<T = Record<string, unknown>>(
			shardBinding: string,
			sql: string,
			bindings: any[] = [],
			options: InsertOptions = {}
		): Promise<InsertResult<T>> {
			return insertShardIn<T>(s, shardBinding, sql, bindings, options);
		},

		/** Same as {@link all}, on this client. */
		all<T = Record<string, unknown>>(key: string, sql: string, bindings: any[] = [], options?: ReadOptions): Promise<QueryResult<T>> {
			return allIn<T>(s, key, sql, bindings, options);
		},

		/** Same as {@link first}, on this client. */
		first<T = Record<string, unknown>>(key: string, sql: string, bindings: any[] = [], options?: ReadOptions): Promise<T | null> {
			return firstIn<T>(s, key, sql, bindings, options);
		},

		/** Same as {@link allByLookupKey}, on this client. */
		allByLookupKey<T = Record<string, unknown>>(
			lookupKey: string,
			sql: string,
			bindings: any[] = [],
			batchSize: number = 50,
			options?: ReadOptions
		): Promise<QueryResult<T>> {
			return allByLookupKeyIn<T>(s, lookupKey, sql, bindings, batchSize, options);
		},

		/** Same as {@link firstByLookupKey}, on this client. */
		firstByLookupKey<T = Record<string, unknown>>(
			lookupKey: string,
			sql: string,
			bindings: any[] = [],
			batchSize: number = 50,
			options?: ReadOptions
		): Promise<T | null> {
			return firstByLookupKeyIn<T>(s, lookupKey, sql, bindings, batchSize, options);
		},

		/** Same as {@link invalidateMappingCache}, on this client. */
		invalidateMappingCache(key?: string): Promise<void> {
			return invalidateMappingCacheIn(s, key);
		},

		/** Same as {@link reassignShard}, on this client. */
		reassignShard(primaryKey: string, newBinding: string, tableName: string, options: MoveRecordOptions = {}): Promise<void> {
			return reassignShardIn(s, primaryKey, newBinding, tableName, options);
		},

		/** Same as {@link recoverMigrations}, on this client. */
		recoverMigrations(options: { olderThanMs?: number } = {}): Promise<RecoveryReport> {
			return recoverMigrationsIn(s, options);
		},

		/** Same as {@link verifyMigrations}, on this client. */
		verifyMigrations(options: { repair?: boolean } = {}): Promise<VerifyReport> {
			return verifyMigrationsIn(s, options);
		},

		/** Same as {@link getMigrationStatus}, on this client. */
		getMigrationStatus(key: string, table?: string): Promise<MigrationStatus | null> {
			return getMigrationStatusIn(s, key, table);
		},

		/** Same as {@link purgeRetired}, on this client. */
		purgeRetired(): Promise<PurgeReport> {
			return purgeRetiredIn(s);
		},

		/** Same as {@link listKnownShards}, on this client. */
		listKnownShards(): Promise<string[]> {
			return listKnownShardsIn(s);
		},

		/** Same as {@link getShardStats}, on this client. */
		getShardStats(): Promise<ShardStats[]> {
			return getShardStatsIn(s);
		},

		/** Same as {@link runShard}, on this client. */
		runShard<T = Record<string, unknown>>(shardBinding: string, sql: string, bindings: any[] = []): Promise<QueryResult<T>> {
			return runShardIn<T>(s, shardBinding, sql, bindings);
		},

		/** Same as {@link allShard}, on this client. */
		allShard<T = Record<string, unknown>>(
			shardBinding: string,
			sql: string,
			bindings: any[] = [],
			options?: ReadOptions
		): Promise<QueryResult<T>> {
			return allShardIn<T>(s, shardBinding, sql, bindings, options);
		},

		/** Same as {@link firstShard}, on this client. */
		firstShard<T = Record<string, unknown>>(
			shardBinding: string,
			sql: string,
			bindings: any[] = [],
			options?: ReadOptions
		): Promise<T | null> {
			return firstShardIn<T>(s, shardBinding, sql, bindings, options);
		},

		/** Same as {@link runAllShards}, on this client. */
		runAllShards<T = Record<string, unknown>>(sql: string, bindings: any[] = [], batchSize: number = 50): Promise<QueryResult<T>[]> {
			return runAllShardsIn<T>(s, sql, bindings, batchSize);
		},

		/** Same as {@link allAllShards}, on this client. */
		allAllShards<T = Record<string, unknown>>(
			sql: string,
			bindings: any[] = [],
			batchSize: number = 50,
			options?: ReadOptions
		): Promise<QueryResult<T>[]> {
			return allAllShardsIn<T>(s, sql, bindings, batchSize, options);
		},

		/** Same as {@link allAllShardsGlobal}, on this client. */
		allAllShardsGlobal<T = Record<string, unknown>>(
			sql: string,
			bindings: any[] = [],
			options: GlobalAllShardsOptions<T> = {}
		): Promise<QueryResult<T>> {
			return allAllShardsGlobalIn<T>(s, sql, bindings, options);
		},

		/** Same as {@link firstAllShards}, on this client. */
		firstAllShards<T = Record<string, unknown>>(
			sql: string,
			bindings: any[] = [],
			batchSize: number = 50,
			options?: ReadOptions
		): Promise<(T | null)[]> {
			return firstAllShardsIn<T>(s, sql, bindings, batchSize, options);
		},

		/** Same as {@link firstAllShardsGlobal}, on this client. */
		firstAllShardsGlobal<T = Record<string, unknown>>(
			sql: string,
			bindings: any[] = [],
			options: Omit<GlobalAllShardsOptions<T>, 'limit'> = {}
		): Promise<T | null> {
			return firstAllShardsGlobalIn<T>(s, sql, bindings, options);
		},

		/** Same as {@link getDatabaseSizeForShard}, on this client. */
		getDatabaseSizeForShard(shardBinding: string): Promise<number> {
			return getDatabaseSizeForShardIn(s, shardBinding);
		},

		/** Same as {@link index}, on this client. */
		index<T = Record<string, unknown>>(
			key: string,
			table: string,
			columns: string | string[] | IndexColumnDefinition[],
			options: Omit<CreateIndexOptions, 'batchSize'> = {}
		): Promise<QueryResult<T>> {
			return indexIn<T>(s, key, table, columns, options);
		},

		/** Same as {@link indexShard}, on this client. */
		indexShard<T = Record<string, unknown>>(
			shardBinding: string,
			table: string,
			columns: string | string[] | IndexColumnDefinition[],
			options: Omit<CreateIndexOptions, 'batchSize'> = {}
		): Promise<QueryResult<T>> {
			return indexShardIn<T>(s, shardBinding, table, columns, options);
		},

		/** Same as {@link indexAllShards}, on this client. */
		indexAllShards<T = Record<string, unknown>>(
			table: string,
			columns: string | string[] | IndexColumnDefinition[],
			options: CreateIndexOptions = {}
		): Promise<QueryResult<T>[]> {
			return indexAllShardsIn<T>(s, table, columns, options);
		},

		/** Same as {@link explain}, on this client. */
		explain<T = Record<string, unknown>>(
			key: string,
			sql: string,
			bindings: any[] = [],
			options: Omit<ExplainOptions, 'batchSize'> = {}
		): Promise<QueryResult<T>> {
			return explainIn<T>(s, key, sql, bindings, options);
		},

		/** Same as {@link explainShard}, on this client. */
		explainShard<T = Record<string, unknown>>(
			shardBinding: string,
			sql: string,
			bindings: any[] = [],
			options: Omit<ExplainOptions, 'batchSize'> = {}
		): Promise<QueryResult<T>> {
			return explainShardIn<T>(s, shardBinding, sql, bindings, options);
		},

		/** Same as {@link explainAllShards}, on this client. */
		explainAllShards<T = Record<string, unknown>>(
			sql: string,
			bindings: any[] = [],
			options: ExplainOptions = {}
		): Promise<QueryResult<T>[]> {
			return explainAllShardsIn<T>(s, sql, bindings, options);
		},

		/** Same as {@link count}, on this client. */
		count(key: string, table: string, options?: ReadOptions): Promise<number> {
			return countIn(s, key, table, options);
		},

		/** Same as {@link countShard}, on this client. */
		countShard(shardBinding: string, table: string, options?: ReadOptions): Promise<number> {
			return countShardIn(s, shardBinding, table, options);
		},

		/** Same as {@link countAllShards}, on this client. */
		countAllShards(
			table: string,
			batchSize: number = 50,
			options?: ReadOptions
		): Promise<{ total: number; shards: ShardTableCount[]; bookmarks?: Record<string, string> }> {
			return countAllShardsIn(s, table, batchSize, options);
		},

		/** Same as {@link getDatabaseSizeForKey}, on this client. */
		getDatabaseSizeForKey(key: string): Promise<number> {
			return getDatabaseSizeForKeyIn(s, key);
		},

		/** Same as {@link getDatabaseSizesAllShards}, on this client. */
		getDatabaseSizesAllShards(batchSize: number = 50): Promise<ShardSizeResult[]> {
			return getDatabaseSizesAllShardsIn(s, batchSize);
		},

		/** Same as {@link getTotalDatabaseSize}, on this client. */
		getTotalDatabaseSize(batchSize: number = 50): Promise<number> {
			return getTotalDatabaseSizeIn(s, batchSize);
		},

		/** Same as {@link firstResilient}, on this client. */
		firstResilient<T = Record<string, unknown>>(key: string, sql: string, bindings: any[] = [], options?: ReadOptions): Promise<T | null> {
			return firstResilientIn<T>(s, key, sql, bindings, options);
		},

		/** Same as {@link query}, on this client. */
		query<T = Record<string, unknown>>(sql: string, bindings: any[] = []): Promise<QueryResult<T>> {
			return queryIn<T>(s, sql, bindings);
		},

		/** Same as {@link queryFirst}, on this client. */
		queryFirst<T = Record<string, unknown>>(sql: string, bindings: any[] = [], options?: ReadOptions): Promise<T | null> {
			return queryFirstIn<T>(s, sql, bindings, options);
		},

		/** Same as {@link queryAll}, on this client. */
		queryAll<T = Record<string, unknown>>(sql: string, bindings: any[] = [], options?: ReadOptions): Promise<QueryResult<T>> {
			return queryAllIn<T>(s, sql, bindings, options);
		},

		/** Same as {@link batch}, on this client. */
		batch<T = Record<string, unknown>>(entries: BatchEntry[]): Promise<BatchShardResult<T>[]> {
			return batchIn<T>(s, entries);
		},

		/** Same as {@link paginate}, on this client. */
		paginate<T = Record<string, unknown>>(
			sql: string,
			bindings: any[] = [],
			options: PaginateOptions<T> = {}
		): Promise<PaginatedResult<T>> {
			return paginateIn<T>(s, sql, bindings, options);
		},

		/** Same as {@link rebalance}, on this client. */
		rebalance(table: string, options: { concurrency?: number; dryRun?: boolean } = {}): Promise<RebalanceResult> {
			return rebalanceIn(s, table, options);
		},

		/** Same as {@link nextId}, on this client. */
		nextId(table: string, options: NextIdOptions = {}): Promise<number> {
			return nextIdIn(s, table, options);
		},

		/** Same as {@link initializeFromEnv}, on this client. */
		initializeFromEnv(env: Record<string, unknown>, options: InitializeFromEnvOptions = {}): string[] {
			return initializeFromEnvIn(s, env, options);
		},

		/** Same as {@link insertReturning}, on this client. */
		insertReturning<T = Record<string, unknown>>(
			key: string,
			table: string,
			values: ColumnValues,
			options: BuildInsertOptions & { idColumn?: string } = {}
		): Promise<T | null> {
			return insertReturningIn<T>(s, key, table, values, options);
		},

		/** Same as {@link setLookup}, on this client. */
		setLookup(key: string, value: string, options: LookupOptions = {}): Promise<void> {
			return setLookupIn(s, key, value, options);
		},

		/** Same as {@link getLookup}, on this client. */
		getLookup(key: string, options: LookupOptions = {}): Promise<string | null> {
			return getLookupIn(s, key, options);
		},

		/** Same as {@link deleteLookup}, on this client. */
		deleteLookup(key: string, options: LookupOptions = {}): Promise<void> {
			return deleteLookupIn(s, key, options);
		},

		/** Same as {@link insertInto}, on this client. */
		insertInto<T = Record<string, unknown>>(
			key: string,
			table: string,
			values: ColumnValues,
			options: BuildInsertOptions = {}
		): Promise<QueryResult<T>> {
			return insertIntoIn<T>(s, key, table, values, options);
		},

		/** Same as {@link updateRow}, on this client. */
		updateRow<T = Record<string, unknown>>(
			key: string,
			table: string,
			values: ColumnValues,
			where: ColumnValues,
			options: CrudReturningOptions = {}
		): Promise<QueryResult<T>> {
			return updateRowIn<T>(s, key, table, values, where, options);
		},

		/** Same as {@link patch}, on this client. */
		patch<T = Record<string, unknown>>(
			key: string,
			table: string,
			id: string | number,
			values: ColumnValues,
			options: IdColumnOptions = {}
		): Promise<QueryResult<T>> {
			return patchIn<T>(s, key, table, id, values, options);
		},

		/** Same as {@link deleteRow}, on this client. */
		deleteRow<T = Record<string, unknown>>(key: string, table: string, where: ColumnValues): Promise<QueryResult<T>> {
			return deleteRowIn<T>(s, key, table, where);
		},

		/** Same as {@link deleteById}, on this client. */
		deleteById<T = Record<string, unknown>>(
			key: string,
			table: string,
			id: string | number,
			options: { idColumn?: string } = {}
		): Promise<QueryResult<T>> {
			return deleteByIdIn<T>(s, key, table, id, options);
		},

		/** Same as {@link upsert}, on this client. */
		upsert<T = Record<string, unknown>>(
			key: string,
			table: string,
			values: ColumnValues,
			conflictColumns: string | string[],
			options: BuildUpsertOptions = {}
		): Promise<QueryResult<T>> {
			return upsertIn<T>(s, key, table, values, conflictColumns, options);
		},

		/** Same as {@link getClosestRegionFromIP}. */
		getClosestRegionFromIP,

		/** Same as {@link createSchema}. */
		createSchema,

		/** Same as `cached`, reading and writing this client's KV store unless `options.kv` is set. */
		async cached<T>(key: string, fetcher: () => Promise<T>, options: CacheOptions = {}): Promise<T> {
			return await cacheValue<T>(key, fetcher, { ...options, kv: options.kv ?? getConfig(s).kv });
		},

		/** Same as `invalidate`, on this client's KV store unless `options.kv` is set. */
		async invalidate(prefix: string, options: { kv?: KVStorage; concurrency?: number } = {}): Promise<number> {
			return await invalidateCache(prefix, { ...options, kv: options.kv ?? getConfig(s).kv });
		},

		/** Same as `reconcileAllocations`, on this client's coordinator and placement cache. */
		async reconcileAllocations(options: ReconcileOptions = {}): Promise<ReconcileReport> {
			return await reconcileAllocationsIn(s.config, s.placement, options);
		}
	};
}

/**
 * The type of the object {@link createClient} returns.
 * @since 1.4.0
 */
export type CollegeDBClient = ReturnType<typeof createClient>;
