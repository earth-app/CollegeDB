/**
 * @fileoverview KV-based shard mapping implementation for CollegeDB
 *
 * This module provides the KVShardMapper class that uses Cloudflare KV storage
 * to maintain mappings between primary keys and their assigned D1 database shards.
 * It handles the persistence and retrieval of shard assignments, enabling the
 * database router to consistently route queries to the correct shard.
 *
 * Key features:
 * - Primary key to shard mapping storage and retrieval
 * - Shard discovery and management
 * - Key counting and statistics for load balancing
 * - Batch operations for efficient KV usage
 *
 * @example
 * ```typescript
 * const mapper = new KVShardMapper(env.KV);
 *
 * // Set a mapping
 * await mapper.setShardMapping('user-123', 'db-east');
 *
 * // Get a mapping
 * const mapping = await mapper.getShardMapping('user-123');
 * console.log(mapping?.shard); // 'db-east'
 *
 * // Get statistics
 * const counts = await mapper.getShardKeyCounts();
 * console.log(counts); // { 'db-east': 42, 'db-west': 38 }
 * ```
 *
 * @author Gregory Mitchell
 * @since 1.0.0
 */

import type { KVNamespace } from '@cloudflare/workers-types';
import type { ShardMapping } from './types.js';

/**
 * KV key prefix for shard mappings
 *
 * All primary key to shard mappings are stored with this prefix to namespace
 * them from other data in the KV store and enable efficient prefix-based queries.
 *
 * @constant
 * @private
 */
const SHARD_MAPPING_PREFIX = 'shard:';

/**
 * KV key for storing the list of known shards
 *
 * A single key that stores the JSON array of all known shard binding names.
 * This provides a quick way to discover available shards without scanning
 * all mapping entries.
 *
 * @constant
 * @private
 */
const KNOWN_SHARDS_KEY = 'known_shards';

/**
 * The KVShardMapper class provides a persistent storage layer for mapping
 * primary keys to their assigned D1 database shards. It uses Cloudflare KV
 * for global, eventually consistent storage with low latency reads.
 *
 * Features:
 * - CRUD operations for shard mappings
 * - Atomic updates with timestamp tracking
 * - Efficient bulk operations and statistics
 * - Prefix-based key organization for performance
 *
 * @example
 * ```typescript
 * const mapper = new KVShardMapper(env.KV);
 *
 * // Create a new mapping
 * await mapper.setShardMapping('order-456', 'db-central');
 *
 * // Update an existing mapping
 * await mapper.updateShardMapping('order-456', 'db-west');
 *
 * // Query mapping
 * const mapping = await mapper.getShardMapping('order-456');
 * if (mapping) {
 *   console.log(`Order 456 is on ${mapping.shard}`);
 * }
 * ```
 */
export class KVShardMapper {
	/**
	 * Cloudflare KV namespace for storing mappings
	 * @readonly
	 */
	constructor(private kv: KVNamespace) {}

	/**
	 * Retrieves the shard assignment for a given primary key from KV storage.
	 * Returns null if no mapping exists, indicating the key has not been
	 * assigned to any shard yet.
	 * @param primaryKey - The primary key to look up
	 * @returns Promise resolving to the shard mapping or null if not found
	 * @throws {Error} If KV read operation fails
	 * @example
	 * ```typescript
	 * const mapping = await mapper.getShardMapping('user-789');
	 * if (mapping) {
	 *   console.log(`User 789 is on shard: ${mapping.shard}`);
	 *   console.log(`Created: ${new Date(mapping.createdAt)}`);
	 * } else {
	 *   console.log('User 789 not yet assigned to any shard');
	 * }
	 * ```
	 */
	async getShardMapping(primaryKey: string): Promise<ShardMapping | null> {
		const key = `${SHARD_MAPPING_PREFIX}${primaryKey}`;
		const value = await this.kv.get<ShardMapping>(key, 'json');
		return value;
	}

	/**
	 * Creates a new shard assignment for a primary key. This is typically used
	 * when a new primary key is first encountered and needs to be assigned to
	 * a shard. Sets both created and updated timestamps to the current time.
	 *
	 * **Note**: This will overwrite any existing mapping for the same key.
	 * Use updateShardMapping() if you want to preserve creation timestamp.
	 * @param primaryKey - The primary key to map
	 * @param shard - The shard binding name to assign
	 * @returns Promise that resolves when the mapping is stored
	 * @throws {Error} If KV write operation fails
	 * @example
	 * ```typescript
	 * // Assign a new user to the west coast shard
	 * await mapper.setShardMapping('user-california-123', 'db-west');
	 * ```
	 */
	async setShardMapping(primaryKey: string, shard: string): Promise<void> {
		const key = `${SHARD_MAPPING_PREFIX}${primaryKey}`;
		const mapping = {
			shard,
			createdAt: Date.now(),
			updatedAt: Date.now()
		} satisfies ShardMapping;

		await this.kv.put(key, JSON.stringify(mapping));
	}

	/**
	 * Changes the shard assignment for a primary key that already has a mapping.
	 * Preserves the original creation timestamp while updating the modified
	 * timestamp. Throws an error if no existing mapping is found.
	 *
	 * This is typically used during shard rebalancing or data migration operations.
	 * @param primaryKey - The primary key to update
	 * @param newShard - The new shard binding name to assign
	 * @returns Promise that resolves when the mapping is updated
	 * @throws {Error} If no existing mapping found or KV operation fails
	 * @example
	 * ```typescript
	 * try {
	 *   // Move user to a different shard for rebalancing
	 *   await mapper.updateShardMapping('user-456', 'db-central');
	 *   console.log('User successfully moved to central shard');
	 * } catch (error) {
	 *   console.error('Failed to update mapping:', error.message);
	 * }
	 * ```
	 */
	async updateShardMapping(primaryKey: string, newShard: string): Promise<void> {
		const existing = await this.getShardMapping(primaryKey);
		if (!existing) {
			throw new Error(`No existing mapping found for primary key: ${primaryKey}`);
		}

		const key = `${SHARD_MAPPING_PREFIX}${primaryKey}`;
		const mapping = {
			...existing,
			shard: newShard,
			updatedAt: Date.now()
		} satisfies ShardMapping;

		await this.kv.put(key, JSON.stringify(mapping));
	}

	/**
	 * Completely removes the shard assignment for a primary key from KV storage.
	 * This is typically used when data is being permanently deleted or when
	 * cleaning up orphaned mappings.
	 *
	 * **WARNING**: After deletion, the primary key will be treated as new
	 * and may be assigned to a different shard on next access.
	 *
	 * @param primaryKey - The primary key mapping to remove
	 * @returns Promise that resolves when the mapping is deleted
	 * @throws {Error} If KV delete operation fails
	 * @example
	 * ```typescript
	 * // Remove mapping for deleted user
	 * await mapper.deleteShardMapping('user-deleted-789');
	 * console.log('Mapping removed for deleted user');
	 * ```
	 */
	async deleteShardMapping(primaryKey: string): Promise<void> {
		const key = `${SHARD_MAPPING_PREFIX}${primaryKey}`;
		await this.kv.delete(key);
	}

	/**
	 * Retrieves the list of all shard binding names that have been registered
	 * with the system. This is maintained separately from the individual mappings
	 * for efficient shard discovery.
	 *
	 * @returns Promise resolving to array of shard binding names
	 * @throws {Error} If KV read operation fails
	 * @example
	 * ```typescript
	 * const shards = await mapper.getKnownShards();
	 * console.log('Available shards:', shards);
	 * // Output: ['db-east', 'db-west', 'db-central']
	 * ```
	 */
	async getKnownShards(): Promise<string[]> {
		const shards = await this.kv.get<string[]>(KNOWN_SHARDS_KEY, 'json');
		return shards || [];
	}

	/**
	 * Replaces the entire list of known shards with a new list. This is typically
	 * used during system initialization or when shards are added/removed in bulk.
	 *
	 * @param shards - Array of shard binding names to store
	 * @returns Promise that resolves when the list is updated
	 * @throws {Error} If KV write operation fails
	 * @example
	 * ```typescript
	 * // Update shard list after adding new regions
	 * await mapper.setKnownShards(['db-east', 'db-west', 'db-central', 'db-asia']);
	 * ```
	 */
	async setKnownShards(shards: string[]): Promise<void> {
		if (!shards || shards.length === 0) return;
		await this.kv.put(KNOWN_SHARDS_KEY, JSON.stringify(shards));
	}

	/**
	 * Appends a new shard to the list of known shards if it's not already present.
	 * This operation is idempotent - adding the same shard multiple times has no effect.
	 *
	 * @param shard - The shard binding name to add
	 * @returns Promise that resolves when the shard is added
	 * @throws {Error} If KV operations fail
	 * @example
	 * ```typescript
	 * // Register a new shard when it comes online
	 * await mapper.addKnownShard('db-europe');
	 * console.log('European shard registered');
	 * ```
	 */
	async addKnownShard(shard: string): Promise<void> {
		if (!shard) return;

		const knownShards = await this.getKnownShards();
		if (!knownShards.includes(shard)) {
			knownShards.push(shard);
			await this.setKnownShards(knownShards);
		}
	}

	/**
	 * Scans all shard mappings to find primary keys assigned to the specified shard.
	 * This operation requires reading all mappings and can be expensive for large
	 * datasets. Consider caching results or using getShardKeyCounts() for statistics.
	 *
	 * @param shard - The shard binding name to search for
	 * @returns Promise resolving to array of primary keys assigned to the shard
	 * @throws {Error} If KV operations fail
	 * @example
	 * ```typescript
	 * // Find all users on the east coast shard
	 * const eastCoastUsers = await mapper.getKeysForShard('db-east');
	 * console.log(`East coast has ${eastCoastUsers.length} users`);
	 * ```
	 */
	async getKeysForShard(shard: string): Promise<string[]> {
		const keys: string[] = [];
		const list = await this.kv.list({ prefix: SHARD_MAPPING_PREFIX });

		for (const key of list.keys) {
			const mapping = await this.getShardMapping(key.name.replace(SHARD_MAPPING_PREFIX, ''));
			if (mapping?.shard === shard) {
				keys.push(key.name.replace(SHARD_MAPPING_PREFIX, ''));
			}
		}

		return keys;
	}

	/**
	 * Scans all shard mappings to count how many primary keys are assigned to
	 * each shard. Returns a mapping of shard names to their key counts. This
	 * is useful for load balancing and monitoring shard utilization.
	 *
	 * **Performance Note**: This operation scans all mappings and can be
	 * expensive for large datasets. Consider implementing caching for frequently
	 * accessed statistics.
	 *
	 * @returns Promise resolving to object mapping shard names to key counts
	 * @throws {Error} If KV operations fail
	 * @example
	 * ```typescript
	 * const counts = await mapper.getShardKeyCounts();
	 * console.log('Shard utilization:', counts);
	 * // Output: { 'db-east': 1247, 'db-west': 982, 'db-central': 1156 }
	 *
	 * // Find the least loaded shard
	 * const leastLoaded = Object.entries(counts)
	 *   .sort(([,a], [,b]) => a - b)[0][0];
	 * console.log('Least loaded shard:', leastLoaded);
	 * ```
	 */
	async getShardKeyCounts(): Promise<Record<string, number>> {
		const counts: Record<string, number> = {};
		const list = await this.kv.list({ prefix: SHARD_MAPPING_PREFIX });

		for (const key of list.keys) {
			const mapping = await this.getShardMapping(key.name.replace(SHARD_MAPPING_PREFIX, ''));
			if (mapping) {
				counts[mapping.shard] = (counts[mapping.shard] || 0) + 1;
			}
		}

		return counts;
	}

	/**
	 * Deletes ALL shard mappings from KV storage. This is a destructive operation
	 * that removes all primary key assignments. After this operation, all keys
	 * will be treated as new and may be assigned to different shards.
	 *
	 * **DANGER**: This operation is irreversible and will cause data routing
	 * issues if used in production. Only use during development, testing, or
	 * complete system resets.
	 *
	 * @returns Promise that resolves when all mappings are deleted
	 * @throws {Error} If KV operations fail
	 * @example
	 * ```typescript
	 * // Only use in development/testing!
	 * if (process.env.NODE_ENV === 'development') {
	 *   await mapper.clearAllMappings();
	 *   console.log('All mappings cleared for testing');
	 * }
	 * ```
	 */
	async clearAllMappings(): Promise<void> {
		const list = await this.kv.list({ prefix: SHARD_MAPPING_PREFIX });
		const promises = list.keys.map((key) => this.kv.delete(key.name));
		await Promise.all(promises);
	}
}
