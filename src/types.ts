/**
 * @fileoverview TypeScript type definitions for CollegeDB
 *
 * This module contains all the TypeScript interfaces and types used throughout
 * the CollegeDB library. These types provide compile-time safety and enable
 * better developer experience with IDE autocompletion and error checking.
 *
 * The types are organized into several categories:
 * - Environment and configuration types
 * - Query result and metadata types
 * - Shard management and statistics types
 * - Strategy and coordination types
 *
 * @example
 * ```typescript
 * import type { CollegeDBConfig, QueryResult, ShardStats } from './types.js';
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

import type { D1Database, DurableObjectNamespace, KVNamespace } from '@cloudflare/workers-types';

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
	/** Cloudflare KV namespace for storing primary key to shard mappings */
	KV: KVNamespace;
	/** Durable Object binding for shard coordination */
	ShardCoordinator: DurableObjectNamespace;
	/** D1 database bindings - dynamic based on wrangler.toml */
	[key: string]: any;
}

/**
 * Configuration for the collegedb sharded database
 */
export interface CollegeDBConfig {
	/** KV namespace for storing mappings */
	kv: KVNamespace;
	/** Shard coordinator Durable Object */
	coordinator?: DurableObjectNamespace;
	/** Available D1 database bindings */
	shards: Record<string, D1Database>;
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
	 * Maximum database size in bytes. Shards that exceed this size will be excluded
	 * from new allocations (existing mappings remain intact).
	 * @default 10683731148 // 9.95 GB, close to D1's 10 GB limit
	 * @since 1.0.8
	 */
	maxDatabaseSize?: number;
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
