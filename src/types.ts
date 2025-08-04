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
 * Sharding strategy options for CollegeDB
 * - `round-robin`: Distributes keys evenly across available shards.
 * - `random`: Selects a random shard for each key.
 * - `hash`: Uses a hash function to determine the shard based on the primary key.
 */
export type ShardingStrategy = 'round-robin' | 'random' | 'hash';

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
	/** Default shard allocation strategy */
	strategy?: ShardingStrategy;
}

/**
 * Query result from D1 database operations
 */
export interface QueryResult {
	/** Query execution success status */
	success: boolean;
	/** Query execution metadata */
	meta: {
		/** Number of rows returned */
		count: number;
		/** Time taken for query execution */
		duration: number;
		/** Last inserted row ID (for INSERT operations) */
		last_row_id?: number;
		/** Number of rows changed (for UPDATE/DELETE operations) */
		changes?: number;
	};
	/** Query results data */
	results: any[];
	/** Any errors that occurred */
	error?: string;
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
	 */
	strategy: ShardingStrategy;
	/** Round-robin counter for allocation */
	roundRobinIndex: number;
}
