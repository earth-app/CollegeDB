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
 * - Multiple lookup keys for the same shard mapping (e.g., username, email, id)
 * - SHA-256 hashing of keys for security and privacy (enabled by default)
 * - Shard discovery and management
 * - Key counting and statistics for load balancing
 * - Batch operations for efficient KV usage
 *
 * @example
 * ```typescript
 * const mapper = new KVShardMapper(env.KV, { hashShardMappings: true });
 *
 * // Set a mapping with multiple lookup keys
 * await mapper.setShardMapping('user-123', 'db-east', ['username:john', 'email:john@example.com', 'id:123']);
 *
 * // Get a mapping by any of the keys
 * const mapping = await mapper.getShardMapping('email:john@example.com');
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
import { CollegeDBError } from './errors.js';
import type { CollegeDBConfig, MultiKeyShardMapping, ShardMapping } from './types.js';

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
 * KV key prefix for multi-key shard mappings
 *
 * Multi-key mappings store the primary mapping data and list of all associated keys.
 *
 * @constant
 * @private
 * @since 1.0.3
 */
const MULTI_KEY_MAPPING_PREFIX = 'multikey:';

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
 * - Multiple lookup keys for the same shard (username, email, id, etc.)
 * - SHA-256 hashing of keys for security and privacy
 * - Atomic updates with timestamp tracking
 * - Efficient bulk operations and statistics
 * - Prefix-based key organization for performance
 *
 * @example
 * ```typescript
 * const mapper = new KVShardMapper(env.KV, { hashShardMappings: true });
 *
 * // Create a new mapping with multiple lookup keys
 * await mapper.setShardMapping('primary-key-123', 'db-central', ['username:john', 'email:john@example.com']);
 *
 * // Update an existing mapping
 * await mapper.updateShardMapping('username:john', 'db-west');
 *
 * // Query mapping by any key
 * const mapping = await mapper.getShardMapping('email:john@example.com');
 * if (mapping) {
 *   console.log(`User is on ${mapping.shard}`);
 * }
 * ```
 */
export class KVShardMapper {
	/**
	 * Cloudflare KV namespace for storing mappings
	 * @readonly
	 */
	private readonly kv: KVNamespace;

	/**
	 * Whether to hash mapping keys with SHA-256
	 * @readonly
	 */
	private readonly hashKeys: boolean;

	/**
	 * Creates a new KVShardMapper instance
	 * @param kv - Cloudflare KV namespace
	 * @param config - Configuration options including hashing preference
	 */
	constructor(kv: KVNamespace, config: Partial<Pick<CollegeDBConfig, 'hashShardMappings'>> = {}) {
		this.kv = kv;
		this.hashKeys = config.hashShardMappings ?? true; // Default to true for security
	}

	/**
	 * Hashes a key using SHA-256 if hashing is enabled
	 * @private
	 * @param key - The key to hash
	 * @returns The hashed key or original key if hashing is disabled
	 */
	private async hashKey(key: string): Promise<string> {
		if (!this.hashKeys) {
			return key;
		}

		const encoder = new TextEncoder();
		const data = encoder.encode(key);
		const hashBuffer = await crypto.subtle.digest('SHA-256', data);
		const hashArray = new Uint8Array(hashBuffer);
		const hashHex = Array.from(hashArray)
			.map((b) => b.toString(16).padStart(2, '0'))
			.join('');
		return hashHex;
	}

	/**
	 * Retrieves the shard assignment for a given primary key from KV storage.
	 * Returns null if no mapping exists, indicating the key has not been
	 * assigned to any shard yet. Supports both single-key and multi-key lookups.
	 * @param primaryKey - The primary key to look up (will be hashed if hashing is enabled)
	 * @returns Promise resolving to the shard mapping or null if not found
	 * @throws {Error} If KV read operation fails
	 * @example
	 * ```typescript
	 * const mapping = await mapper.getShardMapping('email:user@example.com');
	 * if (mapping) {
	 *   console.log(`User is on shard: ${mapping.shard}`);
	 *   console.log(`Created: ${new Date(mapping.createdAt)}`);
	 * } else {
	 *   console.log('User not yet assigned to any shard');
	 * }
	 * ```
	 */
	async getShardMapping(primaryKey: string): Promise<ShardMapping | null> {
		const hashedKey = await this.hashKey(primaryKey);
		const key = `${SHARD_MAPPING_PREFIX}${hashedKey}`;

		// Try single-key mapping first
		const singleMapping = await this.kv.get<ShardMapping>(key, 'json');
		if (singleMapping) {
			return singleMapping;
		}

		// Try multi-key mapping lookup
		const multiKeyMapping = await this.kv.get<MultiKeyShardMapping>(`${MULTI_KEY_MAPPING_PREFIX}${hashedKey}`, 'json');
		if (multiKeyMapping) {
			// Convert multi-key mapping to single mapping format for compatibility
			return {
				shard: multiKeyMapping.shard,
				createdAt: multiKeyMapping.createdAt,
				updatedAt: multiKeyMapping.updatedAt,
				originalKey: this.hashKeys ? undefined : primaryKey
			};
		}

		return null;
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
	 * @param additionalKeys - Optional array of additional lookup keys for the same mapping
	 * @returns Promise that resolves when the mapping is stored
	 * @throws {Error} If KV write operation fails
	 * @example
	 * ```typescript
	 * // Assign a new user to the west coast shard with multiple lookup keys
	 * await mapper.setShardMapping('user-123', 'db-west', ['username:john', 'email:john@example.com']);
	 * ```
	 */
	async setShardMapping(primaryKey: string, shard: string, additionalKeys: string[] = []): Promise<void> {
		const allKeys = [primaryKey, ...additionalKeys];
		const timestamp = Date.now();

		if (allKeys.length === 1) {
			// Single key mapping - use the original format for backward compatibility
			const hashedKey = await this.hashKey(primaryKey);
			const key = `${SHARD_MAPPING_PREFIX}${hashedKey}`;
			const mapping: ShardMapping = {
				shard,
				createdAt: timestamp,
				updatedAt: timestamp,
				originalKey: this.hashKeys ? undefined : primaryKey
			};

			await this.kv.put(key, JSON.stringify(mapping));
		} else {
			// Multi-key mapping - store the primary mapping and create lookup entries
			const primaryHashedKey = await this.hashKey(primaryKey);
			const primaryMappingKey = `${MULTI_KEY_MAPPING_PREFIX}${primaryHashedKey}`;

			const multiKeyMapping: MultiKeyShardMapping = {
				shard,
				createdAt: timestamp,
				updatedAt: timestamp,
				keys: this.hashKeys ? [] : allKeys // Only store original keys if hashing is disabled
			};

			// Store the primary multi-key mapping
			await this.kv.put(primaryMappingKey, JSON.stringify(multiKeyMapping));

			// Create lookup entries for all keys pointing to the primary mapping
			const lookupPromises = allKeys.map(async (lookupKey) => {
				const hashedLookupKey = await this.hashKey(lookupKey);
				const lookupMappingKey = `${SHARD_MAPPING_PREFIX}${hashedLookupKey}`;
				const lookupMapping: ShardMapping = {
					shard,
					createdAt: timestamp,
					updatedAt: timestamp,
					originalKey: this.hashKeys ? undefined : lookupKey
				};
				return this.kv.put(lookupMappingKey, JSON.stringify(lookupMapping));
			});

			await Promise.all(lookupPromises);
		}
	}

	/**
	 * Changes the shard assignment for a primary key that already has a mapping.
	 * Preserves the original creation timestamp while updating the modified
	 * timestamp. Throws an error if no existing mapping is found.
	 *
	 * This is typically used during shard rebalancing or data migration operations.
	 * Works with both single-key and multi-key mappings.
	 * @param primaryKey - The primary key to update
	 * @param newShard - The new shard binding name to assign
	 * @returns Promise that resolves when the mapping is updated
	 * @throws {Error} If no existing mapping found or KV operation fails
	 * @example
	 * ```typescript
	 * try {
	 *   // Move user to a different shard for rebalancing
	 *   await mapper.updateShardMapping('email:user@example.com', 'db-central');
	 *   console.log('User successfully moved to central shard');
	 * } catch (error) {
	 *   console.error('Failed to update mapping:', error.message);
	 * }
	 * ```
	 */
	async updateShardMapping(primaryKey: string, newShard: string): Promise<void> {
		const existing = await this.getShardMapping(primaryKey);
		if (!existing) {
			throw new CollegeDBError(`No existing mapping found for primary key: ${primaryKey}`, 'MAPPING_NOT_FOUND');
		}

		const hashedKey = await this.hashKey(primaryKey);
		const singleMappingKey = `${SHARD_MAPPING_PREFIX}${hashedKey}`;
		const multiKeyMappingKey = `${MULTI_KEY_MAPPING_PREFIX}${hashedKey}`;

		// Check if this is a multi-key mapping
		const multiKeyMapping = await this.kv.get<MultiKeyShardMapping>(multiKeyMappingKey, 'json');

		if (multiKeyMapping) {
			// Update multi-key mapping
			const updatedMultiKeyMapping: MultiKeyShardMapping = {
				...multiKeyMapping,
				shard: newShard,
				updatedAt: Date.now()
			};
			await this.kv.put(multiKeyMappingKey, JSON.stringify(updatedMultiKeyMapping));

			// Update all lookup entries
			const lookupPromises = multiKeyMapping.keys.map(async (lookupKey) => {
				const hashedLookupKey = await this.hashKey(lookupKey);
				const lookupMappingKey = `${SHARD_MAPPING_PREFIX}${hashedLookupKey}`;
				const lookupMapping: ShardMapping = {
					...existing,
					shard: newShard,
					updatedAt: Date.now()
				};
				return this.kv.put(lookupMappingKey, JSON.stringify(lookupMapping));
			});

			await Promise.all(lookupPromises);
		} else {
			// Update single-key mapping
			const updatedMapping: ShardMapping = {
				...existing,
				shard: newShard,
				updatedAt: Date.now()
			};
			await this.kv.put(singleMappingKey, JSON.stringify(updatedMapping));
		}
	}

	/**
	 * Completely removes the shard assignment for a primary key from KV storage.
	 * This is typically used when data is being permanently deleted or when
	 * cleaning up orphaned mappings. Handles both single-key and multi-key mappings.
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
	 * await mapper.deleteShardMapping('email:deleted@example.com');
	 * console.log('Mapping removed for deleted user');
	 * ```
	 */
	async deleteShardMapping(primaryKey: string): Promise<void> {
		const hashedKey = await this.hashKey(primaryKey);
		const singleMappingKey = `${SHARD_MAPPING_PREFIX}${hashedKey}`;
		const multiKeyMappingKey = `${MULTI_KEY_MAPPING_PREFIX}${hashedKey}`;

		// Check if this is a multi-key mapping
		const multiKeyMapping = await this.kv.get<MultiKeyShardMapping>(multiKeyMappingKey, 'json');

		if (multiKeyMapping) {
			// Delete multi-key mapping
			await this.kv.delete(multiKeyMappingKey);

			// Delete all lookup entries
			const deletePromises = multiKeyMapping.keys.map(async (lookupKey) => {
				const hashedLookupKey = await this.hashKey(lookupKey);
				const lookupMappingKey = `${SHARD_MAPPING_PREFIX}${hashedLookupKey}`;
				return this.kv.delete(lookupMappingKey);
			});

			await Promise.all(deletePromises);
		} else {
			// Delete single-key mapping
			await this.kv.delete(singleMappingKey);
		}
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

		// Scan single-key mappings
		const singleKeyList = await this.kv.list({ prefix: SHARD_MAPPING_PREFIX });
		for (const kvKey of singleKeyList.keys) {
			const mapping = await this.kv.get<ShardMapping>(kvKey.name, 'json');
			if (mapping?.shard === shard) {
				const originalKey = kvKey.name.replace(SHARD_MAPPING_PREFIX, '');
				// If hashing is enabled, we can't recover the original key
				if (mapping.originalKey) {
					keys.push(mapping.originalKey);
				} else if (!this.hashKeys) {
					keys.push(originalKey);
				}
			}
		}

		// Scan multi-key mappings
		const multiKeyList = await this.kv.list({ prefix: MULTI_KEY_MAPPING_PREFIX });
		for (const kvKey of multiKeyList.keys) {
			const mapping = await this.kv.get<MultiKeyShardMapping>(kvKey.name, 'json');
			if (mapping?.shard === shard) {
				// Add all keys from this multi-key mapping
				keys.push(...mapping.keys);
			}
		}

		return [...new Set(keys)]; // Remove duplicates
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

		// Count single-key mappings
		const singleKeyList = await this.kv.list({ prefix: SHARD_MAPPING_PREFIX });
		for (const kvKey of singleKeyList.keys) {
			const mapping = await this.kv.get<ShardMapping>(kvKey.name, 'json');
			if (mapping) {
				counts[mapping.shard] = (counts[mapping.shard] || 0) + 1;
			}
		}

		// Count multi-key mappings
		const multiKeyList = await this.kv.list({ prefix: MULTI_KEY_MAPPING_PREFIX });
		for (const kvKey of multiKeyList.keys) {
			const mapping = await this.kv.get<MultiKeyShardMapping>(kvKey.name, 'json');
			if (mapping) {
				// Each multi-key mapping represents multiple keys, so count them all
				counts[mapping.shard] = (counts[mapping.shard] || 0) + mapping.keys.length;
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
		// Clear single-key mappings
		const singleKeyList = await this.kv.list({ prefix: SHARD_MAPPING_PREFIX });
		const singleKeyPromises = singleKeyList.keys.map((key) => this.kv.delete(key.name));

		// Clear multi-key mappings
		const multiKeyList = await this.kv.list({ prefix: MULTI_KEY_MAPPING_PREFIX });
		const multiKeyPromises = multiKeyList.keys.map((key) => this.kv.delete(key.name));

		await Promise.all([...singleKeyPromises, ...multiKeyPromises]);
	}

	/**
	 * Adds additional lookup keys to an existing shard mapping. This allows you to
	 * query the same shard mapping using multiple identifiers (e.g., username, email, id).
	 *
	 * @param primaryKey - An existing key in the mapping
	 * @param additionalKeys - New keys to add for lookup
	 * @returns Promise that resolves when the additional keys are added
	 * @throws {Error} If no existing mapping found or KV operations fail
	 * @example
	 * ```typescript
	 * // Add email lookup to an existing user mapping
	 * await mapper.addLookupKeys('user-123', ['email:user@example.com']);
	 * ```
	 * @since 1.0.3
	 */
	async addLookupKeys(primaryKey: string, additionalKeys: string[]): Promise<void> {
		const existing = await this.getShardMapping(primaryKey);
		if (!existing) {
			throw new CollegeDBError(`No existing mapping found for primary key: ${primaryKey}`, 'MAPPING_NOT_FOUND');
		}

		// Create a new multi-key mapping or add to existing one
		const hashedPrimaryKey = await this.hashKey(primaryKey);
		const multiKeyMappingKey = `${MULTI_KEY_MAPPING_PREFIX}${hashedPrimaryKey}`;
		let multiKeyMapping = await this.kv.get<MultiKeyShardMapping>(multiKeyMappingKey, 'json');

		const allKeys = [primaryKey, ...additionalKeys];
		const timestamp = Date.now();

		if (!multiKeyMapping) {
			// Convert single-key to multi-key mapping
			multiKeyMapping = {
				shard: existing.shard,
				createdAt: existing.createdAt,
				updatedAt: timestamp,
				keys: this.hashKeys ? [] : allKeys
			};
		} else {
			// Add to existing multi-key mapping
			multiKeyMapping = {
				...multiKeyMapping,
				updatedAt: timestamp,
				keys: this.hashKeys ? [] : [...new Set([...multiKeyMapping.keys, ...allKeys])]
			};
		}

		// Store the updated multi-key mapping
		await this.kv.put(multiKeyMappingKey, JSON.stringify(multiKeyMapping));

		// Create lookup entries for the new additional keys
		const lookupPromises = additionalKeys.map(async (lookupKey) => {
			const hashedLookupKey = await this.hashKey(lookupKey);
			const lookupMappingKey = `${SHARD_MAPPING_PREFIX}${hashedLookupKey}`;
			const lookupMapping: ShardMapping = {
				shard: existing.shard,
				createdAt: existing.createdAt,
				updatedAt: timestamp,
				originalKey: this.hashKeys ? undefined : lookupKey
			};
			return this.kv.put(lookupMappingKey, JSON.stringify(lookupMapping));
		});

		await Promise.all(lookupPromises);
	}

	/**
	 * Gets all lookup keys associated with a shard mapping. This is useful for
	 * understanding what keys resolve to the same shard.
	 *
	 * @param primaryKey - Any key in the mapping
	 * @returns Promise resolving to array of all keys in the mapping
	 * @throws {Error} If no existing mapping found
	 * @example
	 * ```typescript
	 * const allKeys = await mapper.getAllLookupKeys('email:user@example.com');
	 * console.log(allKeys); // ['user-123', 'username:john', 'email:user@example.com']
	 * ```
	 * @since 1.0.3
	 */
	async getAllLookupKeys(primaryKey: string): Promise<string[]> {
		const hashedKey = await this.hashKey(primaryKey);
		const multiKeyMappingKey = `${MULTI_KEY_MAPPING_PREFIX}${hashedKey}`;

		// Check if this is a multi-key mapping
		const multiKeyMapping = await this.kv.get<MultiKeyShardMapping>(multiKeyMappingKey, 'json');
		if (multiKeyMapping) {
			return multiKeyMapping.keys;
		}

		// If not a multi-key mapping, check if single-key mapping exists
		const singleMapping = await this.getShardMapping(primaryKey);
		if (singleMapping) {
			return singleMapping.originalKey ? [singleMapping.originalKey] : [primaryKey];
		}

		throw new CollegeDBError(`No mapping found for key: ${primaryKey}`, 'MAPPING_NOT_FOUND');
	}
}
