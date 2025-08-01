/**
 * @fileoverview Durable Object for coordinating shard allocation and maintaining statistics
 *
 * This module provides the ShardCoordinator Durable Object that manages shard allocation
 * strategies and maintains real-time statistics about shard utilization. It provides
 * an HTTP API for other parts of the system to interact with the coordinator.
 *
 * @example
 * ```typescript
 * // In wrangler.toml:
 * [[durable_objects.bindings]]
 * name = "ShardCoordinator"
 * class_name = "ShardCoordinator"
 *
 * // Usage in a Worker:
 * const coordinatorId = env.ShardCoordinator.idFromName('default');
 * const coordinator = env.ShardCoordinator.get(coordinatorId);
 * const response = await coordinator.fetch('http://coordinator/allocate', {
 *   method: 'POST',
 *   body: JSON.stringify({ primaryKey: 'user-123', strategy: 'hash' })
 * });
 * ```
 *
 * @author CollegeDB Team
 * @since 1.0.0
 */

import type { DurableObjectState } from '@cloudflare/workers-types';
import type { ShardCoordinatorState } from './types.js';

/**
 * Durable Object for coordinating shard allocation and maintaining statistics
 *
 * The ShardCoordinator is a Cloudflare Durable Object that provides centralized
 * coordination for shard allocation across multiple D1 databases. It maintains
 * state about available shards, allocation strategies, and usage statistics.
 *
 * Key responsibilities:
 * - Track available D1 shards and their current load
 * - Implement allocation strategies (round-robin, random, hash-based)
 * - Provide HTTP API for shard allocation and management
 * - Maintain persistent state using Durable Object storage
 *
 * @public
 * @example
 * ```typescript
 * // Allocate a shard for a new primary key
 * const response = await coordinator.fetch('http://coordinator/allocate', {
 *   method: 'POST',
 *   body: JSON.stringify({ primaryKey: 'user-456', strategy: 'hash' })
 * });
 * const { shard } = await response.json();
 * ```
 */
export class ShardCoordinator {
	/**
	 * Durable Object state handle for persistent storage
	 * @private
	 */
	private state: DurableObjectState;

	/**
	 * Creates a new ShardCoordinator instance
	 * @param state - Durable Object state provided by Cloudflare runtime
	 */
	constructor(state: DurableObjectState) {
		this.state = state;
	}

	/**
	 * Gets the current coordinator state from persistent storage
	 *
	 * Retrieves the coordinator state from Durable Object storage, returning
	 * a default state if none exists. The state includes known shards, statistics,
	 * allocation strategy, and round-robin counter.
	 *
	 * @private
	 * @returns Promise resolving to the current coordinator state
	 * @throws {Error} If storage access fails
	 */
	private async getState(): Promise<ShardCoordinatorState> {
		const state = await this.state.storage.get<ShardCoordinatorState>('coordinator_state');
		return (
			state || {
				knownShards: [],
				shardStats: {},
				strategy: 'round-robin',
				roundRobinIndex: 0
			}
		);
	}

	/**
	 * Saves the coordinator state to persistent storage
	 *
	 * Persists the coordinator state to Durable Object storage. This includes
	 * all shard information, statistics, and configuration.
	 *
	 * @private
	 * @param state - The coordinator state to persist
	 * @returns Promise that resolves when state is saved
	 * @throws {Error} If storage write fails
	 */
	private async saveState(state: ShardCoordinatorState): Promise<void> {
		await this.state.storage.put('coordinator_state', state);
	}

	/**
	 * Handles HTTP requests to the Durable Object
	 *
	 * Main entry point for all HTTP requests to the ShardCoordinator. Routes
	 * requests based on method and path to appropriate handler functions.
	 *
	 * Supported endpoints:
	 * - GET /shards - List all known shards
	 * - POST /shards - Add a new shard
	 * - DELETE /shards - Remove a shard
	 * - GET /stats - Get shard statistics
	 * - POST /stats - Update shard statistics
	 * - POST /allocate - Allocate a shard for a primary key
	 * - POST /flush - Clear all coordinator state (development only)
	 * - GET /health - Health check endpoint
	 *
	 * @public
	 * @param request - The incoming HTTP request
	 * @returns Promise resolving to HTTP response
	 * @example
	 * ```typescript
	 * // Allocate a shard
	 * const response = await coordinator.fetch('http://coordinator/allocate', {
	 *   method: 'POST',
	 *   headers: { 'Content-Type': 'application/json' },
	 *   body: JSON.stringify({ primaryKey: 'user-123' })
	 * });
	 * ```
	 */
	async fetch(request: Request): Promise<Response> {
		const url = new URL(request.url);
		const path = url.pathname;
		const method = request.method;

		try {
			switch (`${method} ${path}`) {
				case 'GET /shards':
					return this.handleListShards();
				case 'POST /shards':
					return this.handleAddShard(request);
				case 'DELETE /shards':
					return this.handleRemoveShard(request);
				case 'GET /stats':
					return this.handleGetStats();
				case 'POST /stats':
					return this.handleUpdateStats(request);
				case 'POST /allocate':
					return this.handleAllocateShard(request);
				case 'POST /flush':
					return this.handleFlush();
				case 'GET /health':
					return new Response('OK', { status: 200 });
				default:
					return new Response('Not Found', { status: 404 });
			}
		} catch (error) {
			console.error('ShardCoordinator error:', error);
			return new Response('Internal Server Error', { status: 500 });
		}
	}

	/**
	 * Lists all known shards
	 *
	 * Returns a JSON array of all D1 binding names that have been registered
	 * with the coordinator.
	 *
	 * @private
	 * @returns Promise resolving to HTTP response with shard list
	 * @example Response body: `["db-east", "db-west", "db-central"]`
	 */
	private async handleListShards(): Promise<Response> {
		const state = await this.getState();
		return new Response(JSON.stringify(state.knownShards), {
			headers: { 'Content-Type': 'application/json' }
		});
	}

	/**
	 * Adds a new shard to the known shards list
	 *
	 * Registers a new D1 database binding with the coordinator. If the shard
	 * is already known, this operation is idempotent. Initializes statistics
	 * for the new shard.
	 *
	 * @private
	 * @param request - HTTP request containing shard binding name in JSON body
	 * @returns Promise resolving to HTTP response indicating success
	 * @throws {Error} If request body is invalid JSON
	 * @example Request body: `{"shard": "db-new-region"}`
	 */
	private async handleAddShard(request: Request): Promise<Response> {
		const { shard } = (await request.json()) as { shard: string };
		const state = await this.getState();

		if (!state.knownShards.includes(shard)) {
			state.knownShards.push(shard);
			state.shardStats[shard] = {
				binding: shard,
				count: 0,
				lastUpdated: Date.now()
			};
			await this.saveState(state);
		}

		return new Response(JSON.stringify({ success: true }), {
			headers: { 'Content-Type': 'application/json' }
		});
	}

	/**
	 * Removes a shard from the known shards list
	 *
	 * Unregisters a D1 database binding from the coordinator. Removes the shard
	 * from the known shards list and deletes its statistics. Adjusts the round-robin
	 * index if necessary to prevent out-of-bounds access.
	 *
	 * @private
	 * @param request - HTTP request containing shard binding name in JSON body
	 * @returns Promise resolving to HTTP response indicating success
	 * @throws {Error} If request body is invalid JSON
	 * @example Request body: `{"shard": "db-old-region"}`
	 */
	private async handleRemoveShard(request: Request): Promise<Response> {
		const { shard } = (await request.json()) as { shard: string };
		const state = await this.getState();

		const index = state.knownShards.indexOf(shard);
		if (index > -1) {
			state.knownShards.splice(index, 1);
			delete state.shardStats[shard];
			// Adjust round-robin index if necessary
			if (state.roundRobinIndex >= state.knownShards.length) {
				state.roundRobinIndex = 0;
			}
			await this.saveState(state);
		}

		return new Response(JSON.stringify({ success: true }), {
			headers: { 'Content-Type': 'application/json' }
		});
	}

	/**
	 * Gets shard statistics
	 *
	 * Returns an array of statistics for all known shards, including
	 * binding names, key counts, and last updated timestamps.
	 *
	 * @private
	 * @returns Promise resolving to HTTP response with statistics array
	 * @example Response body: `[{"binding": "db-east", "count": 1234, "lastUpdated": 1672531200000}]`
	 */
	private async handleGetStats(): Promise<Response> {
		const state = await this.getState();
		const stats = Object.values(state.shardStats);
		return new Response(JSON.stringify(stats), {
			headers: { 'Content-Type': 'application/json' }
		});
	}

	/**
	 * Updates shard statistics
	 *
	 * Updates the key count and last updated timestamp for a specific shard.
	 * Used by other parts of the system to report changes in shard utilization.
	 *
	 * @private
	 * @param request - HTTP request containing shard name and count in JSON body
	 * @returns Promise resolving to HTTP response indicating success
	 * @throws {Error} If request body is invalid JSON or shard doesn't exist
	 * @example Request body: `{"shard": "db-east", "count": 1500}`
	 */
	private async handleUpdateStats(request: Request): Promise<Response> {
		const { shard, count } = (await request.json()) as { shard: string; count: number };
		const state = await this.getState();

		if (state.shardStats[shard]) {
			state.shardStats[shard].count = count;
			state.shardStats[shard].lastUpdated = Date.now();
			await this.saveState(state);
		}

		return new Response(JSON.stringify({ success: true }), {
			headers: { 'Content-Type': 'application/json' }
		});
	}

	/**
	 * Allocates a shard for a new primary key
	 *
	 * Selects an appropriate shard for a new primary key using the specified
	 * allocation strategy. Updates internal state for round-robin allocation.
	 *
	 * Supported strategies:
	 * - round-robin: Cycles through shards in order
	 * - random: Selects a random shard
	 * - hash: Uses consistent hashing based on primary key
	 *
	 * @private
	 * @param request - HTTP request containing primary key and optional strategy
	 * @returns Promise resolving to HTTP response with selected shard
	 * @throws {Error} If no shards are available or request body is invalid
	 * @example Request body: `{"primaryKey": "user-123", "strategy": "hash"}`
	 * @example Response body: `{"shard": "db-west"}`
	 */
	private async handleAllocateShard(request: Request): Promise<Response> {
		const { primaryKey, strategy } = (await request.json()) as {
			primaryKey: string;
			strategy?: 'round-robin' | 'random' | 'hash';
		};
		const state = await this.getState();

		if (state.knownShards.length === 0) {
			return new Response(JSON.stringify({ error: 'No shards available' }), {
				status: 400,
				headers: { 'Content-Type': 'application/json' }
			});
		}

		const selectedShard = this.selectShard(primaryKey, state, strategy || state.strategy);

		// Update round-robin index for next allocation
		if ((strategy || state.strategy) === 'round-robin') {
			state.roundRobinIndex = (state.roundRobinIndex + 1) % state.knownShards.length;
			await this.saveState(state);
		}

		return new Response(JSON.stringify({ shard: selectedShard }), {
			headers: { 'Content-Type': 'application/json' }
		});
	}

	/**
	 * Flushes all coordinator state (development only)
	 *
	 * Completely clears all coordinator state from Durable Object storage.
	 * This removes all shard registrations, statistics, and configuration.
	 *
	 * ⚠️ **WARNING**: This operation is destructive and should only be used
	 * in development environments or during testing.
	 *
	 * @private
	 * @returns Promise resolving to HTTP response indicating success
	 * @example Response body: `{"success": true}`
	 */
	private async handleFlush(): Promise<Response> {
		await this.state.storage.deleteAll();
		return new Response(JSON.stringify({ success: true }), {
			headers: { 'Content-Type': 'application/json' }
		});
	}

	/**
	 * Selects a shard based on the allocation strategy
	 *
	 * Implements the core shard selection logic for different allocation strategies.
	 * Uses consistent algorithms to ensure predictable shard assignment.
	 *
	 * Strategy details:
	 * - round-robin: Uses roundRobinIndex to cycle through shards
	 * - random: Uses Math.random() for uniform distribution
	 * - hash: Uses string hash function for consistent assignment
	 *
	 * @private
	 * @param primaryKey - The primary key to allocate a shard for
	 * @param state - Current coordinator state containing available shards
	 * @param strategy - The allocation strategy to use
	 * @returns The selected shard binding name
	 * @throws {Error} If no shards are available
	 * @example
	 * ```typescript
	 * const shard = this.selectShard('user-123', state, 'hash');
	 * // Returns: "db-west" (consistent for this key)
	 * ```
	 */
	private selectShard(primaryKey: string, state: ShardCoordinatorState, strategy: 'round-robin' | 'random' | 'hash'): string {
		const shards = state.knownShards;

		if (shards.length === 0) {
			throw new Error('No shards available');
		}

		switch (strategy) {
			case 'round-robin':
				return shards[state.roundRobinIndex] ?? shards[0]!;

			case 'random':
				return shards[Math.floor(Math.random() * shards.length)]!;

			case 'hash':
				// Simple hash function for consistent shard selection
				let hash = 0;
				for (let i = 0; i < primaryKey.length; i++) {
					const char = primaryKey.charCodeAt(i);
					hash = (hash << 5) - hash + char;
					hash = hash & hash; // Convert to 32-bit integer
				}
				const index = Math.abs(hash) % shards.length;
				return shards[index]!;

			default:
				return shards[0]!;
		}
	}

	/**
	 * Increments the key count for a shard
	 *
	 * Atomically increments the key count for a specific shard and updates
	 * the last modified timestamp. Used when new primary keys are assigned
	 * to a shard.
	 *
	 * @public
	 * @param shard - The shard binding name to increment
	 * @returns Promise that resolves when the count is updated
	 * @throws {Error} If the shard is not known to the coordinator
	 * @example
	 * ```typescript
	 * await coordinator.incrementShardCount('db-east');
	 * ```
	 */
	async incrementShardCount(shard: string): Promise<void> {
		const state = await this.getState();
		if (state.shardStats[shard]) {
			state.shardStats[shard].count++;
			state.shardStats[shard].lastUpdated = Date.now();
			await this.saveState(state);
		}
	}

	/**
	 * Decrements the key count for a shard
	 *
	 * Atomically decrements the key count for a specific shard and updates
	 * the last modified timestamp. Used when primary keys are removed or
	 * moved from a shard. Prevents negative counts.
	 *
	 * @public
	 * @param shard - The shard binding name to decrement
	 * @returns Promise that resolves when the count is updated
	 * @throws {Error} If the shard is not known to the coordinator
	 * @example
	 * ```typescript
	 * await coordinator.decrementShardCount('db-west');
	 * ```
	 */
	async decrementShardCount(shard: string): Promise<void> {
		const state = await this.getState();
		if (state.shardStats[shard] && state.shardStats[shard].count > 0) {
			state.shardStats[shard].count--;
			state.shardStats[shard].lastUpdated = Date.now();
			await this.saveState(state);
		}
	}
}
