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
 * Provider-agnostic SQL database contract.
 */
export interface SQLDatabase {
	/** Creates a prepared statement */
	prepare(sql: string): PreparedStatement;
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
	/** D1 database bindings - dynamic based on Wrangler configuration */
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
}

/**
 * Shard statistics for monitoring and load balancing
 */
export interface ShardStats {
	/** D1 binding name */
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
	/** D1 binding name */
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
	/** D1 binding name */
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
	/** List of known D1 bindings */
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
}
