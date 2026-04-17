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

import { CollegeDBError } from './errors';
import type { CollegeDBConfig, KVStorage, MultiKeyShardMapping, ShardMapping } from './types';

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
 * Default TTL for in-memory mapping cache entries.
 * @private
 */
const DEFAULT_MAPPING_CACHE_TTL_MS = 30_000;

/**
 * Default TTL for in-memory known shards cache entries.
 * @private
 */
const DEFAULT_KNOWN_SHARDS_CACHE_TTL_MS = 10_000;

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
	 * KV provider for storing mappings
	 * @readonly
	 */
	private readonly kv: KVStorage;

	/**
	 * Whether to hash mapping keys with SHA-256
	 * @readonly
	 */
	private readonly hashKeys: boolean;

	/**
	 * Cache for hashed keys to avoid repeated crypto operations
	 * @private
	 */
	private readonly hashCache = new Map<string, string>();

	/**
	 * In-memory mapping cache to reduce repeated KV reads.
	 * @private
	 */
	private readonly mappingCache = new Map<string, { mapping: ShardMapping | null; expiresAt: number }>();

	/**
	 * In-memory known shards cache.
	 * @private
	 */
	private readonly knownShardsCache = {
		shards: null as string[] | null,
		expiresAt: 0
	};

	/**
	 * Mapping cache TTL in milliseconds.
	 * @private
	 */
	private readonly mappingCacheTtlMs: number;

	/**
	 * Known shards cache TTL in milliseconds.
	 * @private
	 */
	private readonly knownShardsCacheTtlMs: number;

	/**
	 * Creates a new KVShardMapper instance
	 * @param kv - KV storage provider
	 * @param config - Configuration options including hashing preference
	 */
	constructor(
		kv: KVStorage,
		config: Partial<Pick<CollegeDBConfig, 'hashShardMappings' | 'mappingCacheTtlMs' | 'knownShardsCacheTtlMs'>> = {}
	) {
		this.kv = kv;
		this.hashKeys = config.hashShardMappings ?? true; // Default to true for security
		this.mappingCacheTtlMs = config.mappingCacheTtlMs ?? DEFAULT_MAPPING_CACHE_TTL_MS;
		this.knownShardsCacheTtlMs = config.knownShardsCacheTtlMs ?? DEFAULT_KNOWN_SHARDS_CACHE_TTL_MS;
	}

	/**
	 * Reads a mapping from the in-memory cache.
	 * @private
	 */
	private getCachedMapping(hashedKey: string): ShardMapping | null | undefined {
		const cached = this.mappingCache.get(hashedKey);
		if (!cached) {
			return undefined;
		}

		if (cached.expiresAt < Date.now()) {
			this.mappingCache.delete(hashedKey);
			return undefined;
		}

		return cached.mapping;
	}

	/**
	 * Writes a mapping to the in-memory cache.
	 * @private
	 */
	private setCachedMapping(hashedKey: string, mapping: ShardMapping | null): void {
		if (this.mappingCache.size > 50_000) {
			const firstKey = this.mappingCache.keys().next().value;
			if (firstKey) {
				this.mappingCache.delete(firstKey);
			}
		}

		this.mappingCache.set(hashedKey, {
			mapping,
			expiresAt: Date.now() + this.mappingCacheTtlMs
		});
	}

	/**
	 * Updates mapping cache entries for the provided logical keys.
	 * @private
	 */
	private async cacheMappingForKeys(keys: string[], mapping: ShardMapping | null): Promise<void> {
		const hashedKeys = await Promise.all(keys.map((k) => this.hashKey(k)));
		for (const hashedKey of hashedKeys) {
			this.setCachedMapping(hashedKey, mapping);
		}
	}

	/**
	 * Retrieves known shards from cache when available.
	 * @private
	 */
	private getCachedKnownShards(): string[] | null {
		if (this.knownShardsCache.shards && this.knownShardsCache.expiresAt >= Date.now()) {
			return [...this.knownShardsCache.shards];
		}

		return null;
	}

	/**
	 * Stores known shards in the cache.
	 * @private
	 */
	private setCachedKnownShards(shards: string[]): void {
		this.knownShardsCache.shards = [...shards];
		this.knownShardsCache.expiresAt = Date.now() + this.knownShardsCacheTtlMs;
	}

	/**
	 * Hashes a key using SHA-256 if hashing is enabled
	 * @param key - The key to hash
	 * @returns The hashed key or original key if hashing is disabled
	 */
	async hashKey(key: string): Promise<string> {
		if (!this.hashKeys) {
			return key;
		}

		// Check cache first to avoid repeated crypto operations
		const cached = this.hashCache.get(key);
		if (cached) {
			return cached;
		}

		const encoder = new TextEncoder();
		const data = encoder.encode(key);
		const hashBuffer = await crypto.subtle.digest('SHA-256', data);
		const hashArray = new Uint8Array(hashBuffer);
		const hashHex = Array.from(hashArray)
			.map((b) => b.toString(16).padStart(2, '0'))
			.join('');

		// Cache the result (limit cache size to prevent memory issues)
		if (this.hashCache.size < 10000) {
			this.hashCache.set(key, hashHex);
		}

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
		const cached = this.getCachedMapping(hashedKey);
		if (cached !== undefined) {
			return cached;
		}

		const key = `${SHARD_MAPPING_PREFIX}${hashedKey}`;

		// Try single-key mapping first
		const singleMapping = await this.kv.get<ShardMapping>(key, 'json');
		if (singleMapping) {
			this.setCachedMapping(hashedKey, singleMapping);
			return singleMapping;
		}

		// Try multi-key mapping lookup
		const multiKeyMapping = await this.kv.get<MultiKeyShardMapping>(`${MULTI_KEY_MAPPING_PREFIX}${hashedKey}`, 'json');
		if (multiKeyMapping) {
			const resolved: ShardMapping = {
				shard: multiKeyMapping.shard,
				createdAt: multiKeyMapping.createdAt,
				updatedAt: multiKeyMapping.updatedAt,
				originalKey: this.hashKeys ? undefined : primaryKey
			};

			this.setCachedMapping(hashedKey, resolved);

			// If hash-based key storage is enabled, cache sibling lookup keys too.
			if (this.hashKeys) {
				for (const siblingHashedKey of multiKeyMapping.keys) {
					this.setCachedMapping(siblingHashedKey, resolved);
				}
			}

			return resolved;
		}

		this.setCachedMapping(hashedKey, null);
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
		const mapping: ShardMapping = {
			shard,
			createdAt: timestamp,
			updatedAt: timestamp,
			originalKey: this.hashKeys ? undefined : primaryKey
		};

		if (allKeys.length === 1) {
			// Single key mapping - use the original format for backward compatibility
			const hashedKey = await this.hashKey(primaryKey);
			const key = `${SHARD_MAPPING_PREFIX}${hashedKey}`;

			await this.kv.put(key, JSON.stringify(mapping));
			this.setCachedMapping(hashedKey, mapping);
		} else {
			// Multi-key mapping - store the primary mapping and create lookup entries
			const primaryHashedKey = await this.hashKey(primaryKey);
			const primaryMappingKey = `${MULTI_KEY_MAPPING_PREFIX}${primaryHashedKey}`;

			// Store hashed keys when hashing is enabled to avoid leaking originals while enabling updates
			const storedKeys = this.hashKeys ? await Promise.all(allKeys.map((k) => this.hashKey(k))) : allKeys;

			const multiKeyMapping: MultiKeyShardMapping = {
				shard,
				createdAt: timestamp,
				updatedAt: timestamp,
				keys: storedKeys
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

			// Cache all lookup keys to avoid immediate read-after-write KV hits.
			await this.cacheMappingForKeys(allKeys, mapping);
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
			const timestamp = Date.now();
			const updatedMultiKeyMapping: MultiKeyShardMapping = {
				...multiKeyMapping,
				shard: newShard,
				updatedAt: timestamp
			};
			await this.kv.put(multiKeyMappingKey, JSON.stringify(updatedMultiKeyMapping));

			// Update all lookup entries. Keys are stored hashed when hashing is enabled.
			const keysToUpdate =
				multiKeyMapping.keys.length > 0 ? (this.hashKeys ? multiKeyMapping.keys : multiKeyMapping.keys) : [await this.hashKey(primaryKey)]; // Fallback for legacy empty key lists

			const lookupPromises = keysToUpdate.map(async (keyId) => {
				const lookupMappingKey = `${SHARD_MAPPING_PREFIX}${keyId}`;
				const lookupMapping: ShardMapping = {
					...existing,
					shard: newShard,
					updatedAt: timestamp
				};
				return this.kv.put(lookupMappingKey, JSON.stringify(lookupMapping));
			});

			await Promise.all(lookupPromises);

			const updatedMapping: ShardMapping = {
				...existing,
				shard: newShard,
				updatedAt: timestamp
			};

			if (this.hashKeys) {
				for (const keyId of keysToUpdate) {
					this.setCachedMapping(keyId, updatedMapping);
				}
			}

			this.setCachedMapping(hashedKey, updatedMapping);
		} else {
			// Update single-key mapping
			const updatedMapping: ShardMapping = {
				...existing,
				shard: newShard,
				updatedAt: Date.now()
			};
			await this.kv.put(singleMappingKey, JSON.stringify(updatedMapping));
			this.setCachedMapping(hashedKey, updatedMapping);
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

			// Delete all lookup entries. Keys are stored hashed when hashing is enabled.
			const keysToDelete =
				multiKeyMapping.keys.length > 0 ? (this.hashKeys ? multiKeyMapping.keys : multiKeyMapping.keys) : [await this.hashKey(primaryKey)]; // Fallback for legacy empty key lists

			const deletePromises = keysToDelete.map(async (keyId) => {
				const lookupMappingKey = `${SHARD_MAPPING_PREFIX}${keyId}`;
				return this.kv.delete(lookupMappingKey);
			});

			await Promise.all(deletePromises);

			if (this.hashKeys) {
				for (const keyId of keysToDelete) {
					this.setCachedMapping(keyId, null);
				}
			}
			this.setCachedMapping(hashedKey, null);
		} else {
			// Delete single-key mapping
			await this.kv.delete(singleMappingKey);
			this.setCachedMapping(hashedKey, null);
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
		const cached = this.getCachedKnownShards();
		if (cached) {
			return cached;
		}

		const shards = await this.kv.get<string[]>(KNOWN_SHARDS_KEY, 'json');
		const normalized = shards || [];
		this.setCachedKnownShards(normalized);
		return normalized;
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

		const unique = [...new Set(shards.filter(Boolean))];
		if (unique.length === 0) {
			return;
		}

		await this.kv.put(KNOWN_SHARDS_KEY, JSON.stringify(unique));
		this.setCachedKnownShards(unique);
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
		this.mappingCache.clear();
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
			const storedAllKeys = this.hashKeys ? await Promise.all(allKeys.map((k) => this.hashKey(k))) : allKeys;
			multiKeyMapping = {
				shard: existing.shard,
				createdAt: existing.createdAt,
				updatedAt: timestamp,
				keys: storedAllKeys
			};
		} else {
			// Add to existing multi-key mapping
			const storedAllKeys = this.hashKeys ? await Promise.all(allKeys.map((k) => this.hashKey(k))) : allKeys;
			multiKeyMapping = {
				...multiKeyMapping,
				updatedAt: timestamp,
				keys: [...new Set([...multiKeyMapping.keys, ...storedAllKeys])]
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

		// Refresh cache for primary and added keys.
		const refreshedMapping: ShardMapping = {
			shard: existing.shard,
			createdAt: existing.createdAt,
			updatedAt: timestamp,
			originalKey: existing.originalKey
		};
		await this.cacheMappingForKeys([primaryKey, ...additionalKeys], refreshedMapping);
	}

	/**
	 * Sets multiple shard mappings concurrently with a configurable concurrency limit.
	 *
	 * This helper is used by migration workflows to significantly reduce total
	 * mapping time while avoiding unbounded concurrency spikes.
	 *
	 * @param mappings - The mappings to create
	 * @param options - Batch execution options
	 * @param options.concurrency - Maximum concurrent set operations (default: 25)
	 * @returns Promise that resolves when all mappings are written
	 * @since 1.1.0
	 */
	async setShardMappingsBatch(
		mappings: Array<{ primaryKey: string; shard: string; additionalKeys?: string[] }>,
		options: { concurrency?: number } = {}
	): Promise<void> {
		if (mappings.length === 0) {
			return;
		}

		const concurrency = Math.max(1, options.concurrency ?? 25);
		let index = 0;

		const workers = new Array(Math.min(concurrency, mappings.length)).fill(null).map(async () => {
			while (index < mappings.length) {
				const currentIndex = index++;
				const item = mappings[currentIndex];
				if (!item) {
					continue;
				}

				await this.setShardMapping(item.primaryKey, item.shard, item.additionalKeys || []);
			}
		});

		await Promise.all(workers);
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
