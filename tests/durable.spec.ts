import { beforeEach, describe, expect, it } from 'vitest';
import { ShardCoordinator } from '../src/durable';

/**
 * Mock Durable Object Storage for testing
 */
class MockDurableObjectStorage {
	private data = new Map<string, any>();

	async get<T = unknown>(key: string): Promise<T | undefined> {
		return this.data.get(key);
	}

	async put<T = unknown>(key: string, value: T): Promise<void> {
		this.data.set(key, value);
	}

	async delete(key: string): Promise<boolean> {
		return this.data.delete(key);
	}

	async deleteAll(): Promise<void> {
		this.data.clear();
	}

	async list(options?: { prefix?: string }): Promise<Map<string, any>> {
		if (!options?.prefix) return new Map(this.data);

		const filtered = new Map();
		for (const [key, value] of this.data.entries()) {
			if (key.startsWith(options.prefix)) {
				filtered.set(key, value);
			}
		}
		return filtered;
	}
}

/**
 * Mock Durable Object State for testing
 */
class MockDurableObjectState {
	storage: MockDurableObjectStorage;

	constructor() {
		this.storage = new MockDurableObjectStorage();
	}
}

describe('ShardCoordinator', () => {
	let coordinator: ShardCoordinator;
	let mockState: MockDurableObjectState;

	beforeEach(() => {
		mockState = new MockDurableObjectState();
		coordinator = new ShardCoordinator(mockState as any);
	});

	describe('Shard allocation strategies', () => {
		it('should allocate shards using round-robin strategy', async () => {
			// Add some shards first
			await coordinator.fetch(
				new Request('http://test/shards', {
					method: 'POST',
					body: JSON.stringify({ shard: 'db-east' })
				})
			);

			await coordinator.fetch(
				new Request('http://test/shards', {
					method: 'POST',
					body: JSON.stringify({ shard: 'db-west' })
				})
			);

			// Test round-robin allocation
			const allocation1 = await coordinator.fetch(
				new Request('http://test/allocate', {
					method: 'POST',
					body: JSON.stringify({ primaryKey: 'user-1', strategy: 'round-robin' })
				})
			);
			const result1 = (await allocation1.json()) as { shard: string };

			const allocation2 = await coordinator.fetch(
				new Request('http://test/allocate', {
					method: 'POST',
					body: JSON.stringify({ primaryKey: 'user-2', strategy: 'round-robin' })
				})
			);
			const result2 = (await allocation2.json()) as { shard: string };

			// Should allocate to different shards with round-robin
			expect(result1.shard).not.toBe(result2.shard);
		});

		it('should allocate shards consistently using hash strategy', async () => {
			// Add some shards
			await coordinator.fetch(
				new Request('http://test/shards', {
					method: 'POST',
					body: JSON.stringify({ shard: 'db-east' })
				})
			);

			await coordinator.fetch(
				new Request('http://test/shards', {
					method: 'POST',
					body: JSON.stringify({ shard: 'db-west' })
				})
			);

			// Test hash allocation (should be consistent)
			const hashAllocation1 = await coordinator.fetch(
				new Request('http://test/allocate', {
					method: 'POST',
					body: JSON.stringify({ primaryKey: 'consistent-key', strategy: 'hash' })
				})
			);
			const hashResult1 = (await hashAllocation1.json()) as { shard: string };

			const hashAllocation2 = await coordinator.fetch(
				new Request('http://test/allocate', {
					method: 'POST',
					body: JSON.stringify({ primaryKey: 'consistent-key', strategy: 'hash' })
				})
			);
			const hashResult2 = (await hashAllocation2.json()) as { shard: string };

			// Hash should be consistent for same key
			expect(hashResult1.shard).toBe(hashResult2.shard);
		});
	});

	describe('Shard statistics management', () => {
		it('should manage shard statistics correctly', async () => {
			// Clear state
			await coordinator.fetch(new Request('http://test/flush', { method: 'POST' }));

			// Add shard
			await coordinator.fetch(
				new Request('http://test/shards', {
					method: 'POST',
					body: JSON.stringify({ shard: 'db-stats-test' })
				})
			);

			// Update stats
			await coordinator.fetch(
				new Request('http://test/stats', {
					method: 'POST',
					body: JSON.stringify({ shard: 'db-stats-test', count: 42 })
				})
			);

			// Get stats
			const statsResponse = await coordinator.fetch(new Request('http://test/stats', { method: 'GET' }));
			const stats = (await statsResponse.json()) as Array<{ binding: string; count: number }>;

			expect(stats).toHaveLength(1);
			expect(stats[0]?.binding).toBe('db-stats-test');
			expect(stats[0]?.count).toBe(42);
		});
	});

	describe('Error handling', () => {
		it('should return 400 when no shards are available', async () => {
			// Clear state
			await coordinator.fetch(new Request('http://test/flush', { method: 'POST' }));

			// Try to allocate with no shards
			const emptyAllocation = await coordinator.fetch(
				new Request('http://test/allocate', {
					method: 'POST',
					body: JSON.stringify({ primaryKey: 'test-key' })
				})
			);

			expect(emptyAllocation.status).toBe(400);
		});

		it('should return 404 for invalid endpoints', async () => {
			// Test invalid endpoint
			const invalidEndpoint = await coordinator.fetch(new Request('http://test/invalid', { method: 'GET' }));
			expect(invalidEndpoint.status).toBe(404);
		});
	});

	describe('Count management', () => {
		it('should increment and decrement shard counts correctly', async () => {
			// Add a shard
			await coordinator.fetch(
				new Request('http://test/shards', {
					method: 'POST',
					body: JSON.stringify({ shard: 'db-count-test' })
				})
			);

			// Increment count
			await coordinator.incrementShardCount('db-count-test');
			await coordinator.incrementShardCount('db-count-test');

			// Check stats
			let statsResponse = await coordinator.fetch(new Request('http://test/stats', { method: 'GET' }));
			let stats = (await statsResponse.json()) as Array<{ binding: string; count: number }>;

			const shard = stats.find((s) => s.binding === 'db-count-test');
			expect(shard?.count).toBe(2);

			// Decrement count
			await coordinator.decrementShardCount('db-count-test');

			statsResponse = await coordinator.fetch(new Request('http://test/stats', { method: 'GET' }));
			stats = (await statsResponse.json()) as Array<{ binding: string; count: number }>;

			const updatedShard = stats.find((s) => s.binding === 'db-count-test');
			expect(updatedShard?.count).toBe(1);
		});
	});
});
