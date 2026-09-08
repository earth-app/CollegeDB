/**
 * @fileoverview TypeScript type definitions for CollegeDB
 *
 * This module contains all the TypeScript interfaces and types used throughout
 * the CollegeDB library. These types provide compile-time safety, provider-
 * agnostic storage abstractions, and a better developer experience with IDE
 * autocompletion and error checking.
 *
 * The types are organized into several categories:
 * - Environment and configuration types
 * - Query result and metadata types
 * - Shard management and statistics types
 * - Strategy and coordination types
 *
 * @example
 * ```typescript
 * import type { CollegeDBConfig, QueryResult, ShardStats } from './types';
 *
 * const config: CollegeDBConfig = {
 *   kv: env.KV,
 *   shards: { 'db-east': env.DB_EAST },
 *   strategy: 'hash'
 * };
 * ```
 *
 * @author Gregory Mitchell
 * @since 1.0.0
 */

import type { DurableObjectNamespace } from '@cloudflare/workers-types';
import type { PhaseObserver } from './telemetry';

/**
 * Result item returned by a key-value store list operation.
 */
export interface KVListKey {
	/** Full key name */
	name: string;
	/** Optional absolute expiration timestamp */
	expiration?: number;
	/** Optional backend-specific metadata */
	metadata?: unknown;
}

/**
 * Result payload returned by key-value store list operations.
 */
export interface KVListResult {
	/** The keys that matched the provided list filter */
	keys: KVListKey[];
	/** Cursor for paginated list operations (backend-specific) */
	cursor?: string;
	/** Whether the list is complete (backend-specific) */
	list_complete?: boolean;
}

/**
 * Provider-agnostic key-value storage contract.
 *
 * This interface is implemented by Cloudflare KV, Redis/Valkey adapters,
 * and any custom KV backend supported by CollegeDB.
 */
export interface KVStorage {
	/**
	 * Retrieves a value by key. When `type` is `json`, the value should be parsed.
	 */
	get<T = unknown>(key: string, type: 'json'): Promise<T | null>;
	get(key: string, type?: 'text'): Promise<string | null>;
	/**
	 * Stores a value by key.
	 */
	put(key: string, value: string): Promise<void>;
	/**
	 * Deletes a key.
	 */
	delete(key: string): Promise<void>;
	/**
	 * Lists keys, optionally filtered by prefix.
	 */
	list(options?: { prefix?: string; cursor?: string; limit?: number }): Promise<KVListResult>;
	/**
	 * Reads several keys in one round trip, returning results positionally.
	 *
	 * Optional. Backends with a native multi-get (Redis/Valkey `MGET`) implement
	 * it; CollegeDB falls back to concurrent single reads when it is absent, so
	 * callers never need to check for it.
	 * @since 1.4.0
	 */
	getMany?<T = unknown>(keys: string[], type: 'json'): Promise<(T | null)[]>;
	getMany?(keys: string[], type?: 'text'): Promise<(string | null)[]>;
	/**
	 * Writes several key/value pairs in one round trip.
	 *
	 * Optional, with the same fallback contract as {@link KVStorage.getMany}.
	 * A multi-key shard mapping otherwise costs one round trip per lookup key.
	 * @since 1.4.0
	 */
	putMany?(entries: Array<{ key: string; value: string }>): Promise<void>;
	/**
	 * Deletes several keys in one round trip.
	 *
	 * Optional, with the same fallback contract as {@link KVStorage.getMany}.
	 * @since 1.4.0
	 */
	deleteMany?(keys: string[]): Promise<void>;
}

/**
 * Metadata for SQL query execution results.
 */
export interface QueryResultMeta {
	/** Query duration in milliseconds */
	duration: number;
	/** Number of changed rows (when available) */
	changes?: number;
	/** Last inserted row id (when available) */
	last_row_id?: number | string;
	/** Additional provider-specific metadata */
	[key: string]: unknown;
}

/**
 * Provider-agnostic query result payload.
 */
export interface QueryResult<T = Record<string, unknown>> {
	/** Whether the statement executed successfully */
	success: boolean;
	/** Returned rows for the statement */
	results: T[];
	/** Execution metadata */
	meta: QueryResultMeta;
	/** Optional backend-specific error detail */
	error?: string;
}

/**
 * Provider-agnostic prepared statement contract.
 */
export interface PreparedStatement {
	/** Binds positional parameters */
	bind(...bindings: any[]): PreparedStatement;
	/** Executes a write-oriented statement */
	run<T = Record<string, unknown>>(): Promise<QueryResult<T>>;
	/** Executes a query and returns all matching rows */
	all<T = Record<string, unknown>>(): Promise<QueryResult<T>>;
	/** Executes a query and returns the first row */
	first<T = Record<string, unknown>>(): Promise<T | null>;
}

/**
 * SQL dialect of a shard, used to quote identifiers the way that backend
 * expects.
 *
 * MySQL and MariaDB reject double-quoted identifiers unless `ANSI_QUOTES` is in
 * their `sql_mode`, so a statement built with ANSI quoting fails outright there.
 * Adapters report their dialect so the builders can pick the right character.
 * @since 1.4.0
 */
export type SQLDialect = 'sqlite' | 'postgres' | 'mysql';

/**
 * A statement plus its bindings, as accepted by {@link SQLDatabase.runBatch}.
 * @since 1.4.0
 */
export interface BatchStatement {
	/** SQL text using `?` placeholders */
	sql: string;
	/** Positional bindings for the statement */
	bindings?: any[];
}

/**
 * Provider-agnostic SQL database contract.
 */
export interface SQLDatabase {
	/** Creates a prepared statement */
	prepare(sql: string): PreparedStatement;
	/**
	 * Which SQL dialect this shard speaks, so generated statements quote
	 * identifiers correctly.
	 *
	 * Optional. The adapter factories set it; a raw binding passed straight to
	 * {@link initialize} leaves it unset, which is read as ANSI double quoting
	 * and is correct for D1 and SQLite.
	 * @since 1.4.0
	 */
	dialect?: SQLDialect;
	/**
	 * Executes several statements against this shard in one round trip.
	 *
	 * Optional. The provider adapters implement it over D1's native `batch`,
	 * Drizzle's `db.batch`, or a single driver transaction. CollegeDB falls back
	 * to sequential `prepare().run()` calls when a provider does not implement
	 * it, so callers never need to check for it.
	 *
	 * Named `runBatch` rather than `batch` on purpose: a raw `D1Database` is
	 * structurally assignable to this contract, and D1 already has a `batch`
	 * that takes prepared statements rather than SQL text. Reusing the name
	 * would make `env.DB` stop satisfying `SQLDatabase`.
	 *
	 * Statements execute in order and share one transaction per shard. There is
	 * no cross-shard atomicity: a routed batch spanning three shards is three
	 * independent transactions.
	 * @since 1.4.0
	 */
	runBatch?<T = Record<string, unknown>>(statements: BatchStatement[]): Promise<QueryResult<T>[]>;
}

/**
 * Available Cloudflare D1 regions for geographic optimization
 */
export type D1Region =
	| 'wnam' // Western North America (US West Coast)
	| 'enam' // Eastern North America (US East Coast)
	| 'weur' // Western Europe
	| 'eeur' // Eastern Europe
	| 'apac' // Asia Pacific
	| 'oc' // Oceania
	| 'me' // Middle East
	| 'af'; // Africa

/**
 * Shard location configuration for geographic optimization
 */
export interface ShardLocation {
	/** The D1 region where this shard is located */
	region: D1Region;
	/** Optional priority weight for this shard (higher = preferred) */
	priority?: number;
}

/**
 * Sharding strategy options for CollegeDB
 * - `round-robin`: Distributes keys evenly across available shards.
 * - `random`: Selects a random shard for each key.
 * - `hash`: Uses a hash function to determine the shard based on the primary key.
 * - `location`: Selects shards based on geographic proximity to reduce latency.
 */
export type ShardingStrategy = 'round-robin' | 'random' | 'hash' | 'location';

/**
 * Mixed sharding strategy configuration for different operation types
 * @since 1.0.2
 */
export interface MixedShardingStrategy {
	/** Strategy for read operations (SELECT) */
	read: ShardingStrategy;
	/** Strategy for write operations (INSERT, UPDATE, DELETE) */
	write: ShardingStrategy;
}

/**
 * Database operation types for strategy selection
 * @since 1.0.2
 */
export type OperationType = 'read' | 'write';

/**
 * Environment bindings for the Cloudflare Worker
 */
export interface Env {
	/** Key-value namespace for storing primary key to shard mappings */
	KV: KVStorage;
	/** Durable Object binding for shard coordination */
	ShardCoordinator: DurableObjectNamespace;
	/** Optional Hyperdrive binding for external SQL connectivity */
	HYPERDRIVE?: { connectionString: string; localConnectionString?: string };
	/** Shard bindings - dynamic based on Wrangler configuration */
	[key: string]: any;
}

/**
 * Configuration for the collegedb sharded database
 */
export interface CollegeDBConfig {
	/** Key-value provider for storing shard mappings */
	kv: KVStorage;
	/** Shard coordinator Durable Object */
	coordinator?: DurableObjectNamespace;
	/** Available SQL shard providers */
	shards: Record<string, SQLDatabase>;
	/** Default shard allocation strategy (can be single strategy or mixed strategy object) */
	strategy?: ShardingStrategy | MixedShardingStrategy;
	/** Target region for location-based sharding */
	targetRegion?: D1Region;
	/** Geographic locations of each shard (required for location strategy) */
	shardLocations?: Record<string, ShardLocation | D1Region>;
	/**
	 * Disable automatic migration detection and background migration (useful for testing)
	 * @since 1.0.2
	 */
	disableAutoMigration?: boolean;
	/**
	 * Whether to hash shard mapping keys with SHA-256 for security and privacy.
	 * When enabled, primary keys are hashed before storing in KV, protecting
	 * sensitive data like emails from being visible in KV keys.
	 * @default true
	 * @since 1.0.3
	 */
	hashShardMappings?: boolean;
	/**
	 * Enable debug logging for development and troubleshooting
	 * @default false
	 * @since 1.0.6
	 */
	debug?: boolean;
	/**
	 * Maximum database size in bytes. When set, shards that exceed this size are
	 * excluded from new allocations (existing mappings remain intact).
	 *
	 * When omitted, size-based filtering is disabled to avoid extra sizing queries.
	 * This significantly reduces routing latency for write-heavy workloads.
	 * @since 1.0.8
	 */
	maxDatabaseSize?: number;
	/**
	 * In-memory TTL for primary key to shard mapping cache.
	 * @default 30000
	 * @since 1.1.0
	 */
	mappingCacheTtlMs?: number;
	/**
	 * In-memory TTL for known shard list cache.
	 * @default 10000
	 * @since 1.1.0
	 */
	knownShardsCacheTtlMs?: number;
	/**
	 * In-memory TTL for shard size checks when `maxDatabaseSize` is enabled.
	 * @default 30000
	 * @since 1.1.0
	 */
	sizeCacheTtlMs?: number;
	/**
	 * Concurrency limit for migration mapping operations.
	 * @default 25
	 * @since 1.1.0
	 */
	migrationConcurrency?: number;
	/**
	 * How a primary key is resolved to a shard.
	 *
	 * - `computed` - derive the shard from the key with rendezvous hashing and
	 *   consult KV only for keys that were explicitly reassigned or placed by an
	 *   older algorithm. Removes one KV read per operation and one KV write per
	 *   new key.
	 * - `kv` - read the mapping from KV, allocating and recording on a miss.
	 *
	 * Only meaningful for the `hash` strategy. `round-robin` and `random` are not
	 * functions of the key, and `location` depends on the requesting region
	 * rather than the key, so all three force `kv`.
	 *
	 * Defaults to `computed` for the `hash` strategy on a deployment CollegeDB
	 * has not seen before, and to `kv` when existing mappings are detected, so
	 * an upgrade never starts computing placements for keys another algorithm
	 * placed. Call `rebalance()` to migrate such a deployment.
	 * @since 1.4.0
	 */
	placement?: 'computed' | 'kv';
	/**
	 * Whether a read that finds no mapping should record one.
	 *
	 * When `false` a read resolves a shard and returns it without writing to KV,
	 * so looking up a key that has no row costs no KV write and leaves no
	 * mapping behind. Writes always record their mapping.
	 * @default false
	 * @since 1.4.0
	 */
	allocateOnRead?: boolean;
	/**
	 * Whether a mapping miss should also probe the legacy multi-key record.
	 *
	 * Every writer since 1.0.3 stores a single-key record for each lookup key
	 * alongside the multi-key record, so this second read cannot succeed for
	 * data this version wrote and doubles the KV cost of every true miss. Enable
	 * it only while migrating mappings written before 1.0.3.
	 * @default false
	 * @since 1.4.0
	 */
	legacyMultiKeyLookup?: boolean;
	/**
	 * Primary-key column per table, used by the query planner to recover the
	 * routing key from a statement. Tables absent from the map use `id`.
	 * @since 1.4.0
	 */
	keyColumns?: Record<string, string>;
	/**
	 * What the query planner does with a statement whose routing key it cannot
	 * prove.
	 *
	 * - `throw` - reject the call and point the caller at the explicit-key API.
	 * - `fanout` - run the statement on every shard.
	 *
	 * `throw` is the default because a mis-routed write lands a row where no
	 * reader will look, while a thrown error is visible immediately.
	 * @default 'throw'
	 * @since 1.4.0
	 */
	onUnroutable?: 'throw' | 'fanout';
	/**
	 * Per-phase timing observer. When set, CollegeDB reports the cost of hashing,
	 * each KV round trip, shard selection, coordinator calls, and SQL execution.
	 * Unset, the instrumentation allocates nothing.
	 * @since 1.4.0
	 */
	onPhase?: PhaseObserver;
	/**
	 * Extends the lifetime of CollegeDB's background work past the current
	 * request. Pass `ctx.waitUntil` from a Worker: without it, the background
	 * known-shard sync and auto-migration started by {@link initialize} are
	 * cancelled when the request that triggered them ends.
	 * @since 1.4.0
	 */
	waitUntil?: (promise: Promise<unknown>) => void;
}

/**
 * Shard statistics for monitoring and load balancing
 */
export interface ShardStats {
	/** Shard binding name */
	binding: string;
	/** Number of primary keys assigned to this shard */
	count: number;
	/** Last updated timestamp */
	lastUpdated?: number;
}

/**
 * Shard allocation strategy interface
 */
export interface ShardStrategy {
	/** Select a shard for a new primary key */
	selectShard(primaryKey: string, availableShards: string[]): string;
}

/**
 * Primary key to shard mapping stored in KV
 */
export interface ShardMapping {
	/** Shard binding name */
	shard: string;
	/** Timestamp when mapping was created */
	createdAt: number;
	/** Timestamp when mapping was last updated */
	updatedAt: number;
	/** Original unhashed primary key (only stored when hashing is disabled) */
	originalKey?: string;
}

/**
 * Multi-key shard mapping for lookup by various unique identifiers
 * @since 1.0.3
 */
export interface MultiKeyShardMapping {
	/** Shard binding name */
	shard: string;
	/** Timestamp when mapping was created */
	createdAt: number;
	/** Timestamp when mapping was last updated */
	updatedAt: number;
	/** All keys that resolve to this shard mapping (for reverse lookups) */
	keys: string[];
}

/**
 * Durable Object state for shard coordination
 */
export interface ShardCoordinatorState {
	/** List of known shard bindings */
	knownShards: string[];
	/** Statistics for each shard */
	shardStats: Record<string, ShardStats>;
	/**
	 * Current allocation strategy
	 * `round-robin` - distributes keys evenly across shards
	 * `random` - selects a random shard for each key
	 * `hash` - uses a hash function to determine shard based on primary key (default)
	 * `location` - selects shards based on geographic proximity to reduce latency
	 * Can also be a mixed strategy object with separate read/write strategies
	 */
	strategy: ShardingStrategy | MixedShardingStrategy;
	/** Round-robin counter for allocation */
	roundRobinIndex: number;
	/** Target region for location-based allocation */
	targetRegion?: D1Region;
	/** Geographic locations of each shard */
	shardLocations?: Record<string, ShardLocation>;
	/**
	 * Monotonic named sequence counters used for cross-shard id generation.
	 * Keyed by sequence name (typically a table name).
	 * @since 1.2.4
	 */
	sequences?: Record<string, number>;
}
