import { beforeEach, describe, expect, it } from 'vitest';
import { ShardCoordinator } from '../src/durable';

/**
 * Mock Durable Object Storage for testing
 */
class MockDurableObjectStorage {
	data = new Map<string, any>();

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

async function asJson<T>(response: Response): Promise<T> {
	return JSON.parse(await response.text()) as T;
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

	describe('Advanced usage', () => {
		it('rejects invalid add/remove/update requests with 400 responses', async () => {
			const addRes = await coordinator.fetch(
				new Request('http://coordinator/shards', {
					method: 'POST',
					body: JSON.stringify({})
				}) as any
			);
			expect(addRes.status).toBe(400);

			const removeRes = await coordinator.fetch(
				new Request('http://coordinator/shards', {
					method: 'DELETE',
					body: JSON.stringify({})
				}) as any
			);
			expect(removeRes.status).toBe(400);

			const statsRes = await coordinator.fetch(
				new Request('http://coordinator/stats', {
					method: 'POST',
					body: JSON.stringify({ shard: 'a' })
				}) as any
			);
			expect(statsRes.status).toBe(400);
		});

		it('add -> stats -> allocate (round-robin) -> remove flow', async () => {
			const add = async (shard: string) =>
				coordinator.fetch(new Request('http://coordinator/shards', { method: 'POST', body: JSON.stringify({ shard }) }) as any);

			await add('db-a');
			await add('db-b');
			await add('db-c');

			const listed = await asJson<string[]>(await coordinator.fetch(new Request('http://coordinator/shards') as any));
			expect(listed.sort()).toEqual(['db-a', 'db-b', 'db-c']);

			// round-robin allocate three times and ensure each shard is touched
			const firstAlloc = await asJson<{ shard: string }>(
				await coordinator.fetch(
					new Request('http://coordinator/allocate', {
						method: 'POST',
						body: JSON.stringify({ primaryKey: 'k1' })
					}) as any
				)
			);
			expect(['db-a', 'db-b', 'db-c']).toContain(firstAlloc.shard);

			const updateStats = await coordinator.fetch(
				new Request('http://coordinator/stats', {
					method: 'POST',
					body: JSON.stringify({ shard: 'db-a', count: 100 })
				}) as any
			);
			expect((await asJson<{ success: boolean }>(updateStats)).success).toBe(true);

			const stats = await asJson<Array<{ binding: string; count: number }>>(
				await coordinator.fetch(new Request('http://coordinator/stats') as any)
			);
			expect(stats.find((s) => s.binding === 'db-a')?.count).toBe(100);

			const remove = await coordinator.fetch(
				new Request('http://coordinator/shards', {
					method: 'DELETE',
					body: JSON.stringify({ shard: 'db-b' })
				}) as any
			);
			expect((await asJson<{ success: boolean }>(remove)).success).toBe(true);

			const updatedList = await asJson<string[]>(await coordinator.fetch(new Request('http://coordinator/shards') as any));
			expect(updatedList.sort()).toEqual(['db-a', 'db-c']);
		});

		it('allocates with hash strategy override on the request body', async () => {
			await coordinator.fetch(new Request('http://coordinator/shards', { method: 'POST', body: JSON.stringify({ shard: 'east' }) }) as any);
			await coordinator.fetch(new Request('http://coordinator/shards', { method: 'POST', body: JSON.stringify({ shard: 'west' }) }) as any);

			const first = await asJson<{ shard: string }>(
				await coordinator.fetch(
					new Request('http://coordinator/allocate', {
						method: 'POST',
						body: JSON.stringify({ primaryKey: 'consistent-key', strategy: 'hash' })
					}) as any
				)
			);
			const second = await asJson<{ shard: string }>(
				await coordinator.fetch(
					new Request('http://coordinator/allocate', {
						method: 'POST',
						body: JSON.stringify({ primaryKey: 'consistent-key', strategy: 'hash' })
					}) as any
				)
			);
			expect(first.shard).toBe(second.shard);
		});

		it('allocates with random strategy', async () => {
			await coordinator.fetch(new Request('http://coordinator/shards', { method: 'POST', body: JSON.stringify({ shard: 'a' }) }) as any);
			await coordinator.fetch(new Request('http://coordinator/shards', { method: 'POST', body: JSON.stringify({ shard: 'b' }) }) as any);

			const result = await asJson<{ shard: string }>(
				await coordinator.fetch(
					new Request('http://coordinator/allocate', {
						method: 'POST',
						body: JSON.stringify({ primaryKey: 'rand-key', strategy: 'random' })
					}) as any
				)
			);
			expect(['a', 'b']).toContain(result.shard);
		});

		it('falls back to hash when location strategy has no shardLocations', async () => {
			// Seed the storage that the coordinator was already constructed with —
			// instantiating a fresh `MockDurableObjectStorage` here would not be
			// wired up to `coordinator`.
			await mockState.storage.put('coordinator_state', {
				knownShards: ['a', 'b'],
				shardStats: { a: { binding: 'a', count: 0 }, b: { binding: 'b', count: 0 } },
				strategy: { read: 'location', write: 'location' },
				roundRobinIndex: 0,
				targetRegion: 'wnam'
			});

			const result = await asJson<{ shard: string }>(
				await coordinator.fetch(
					new Request('http://coordinator/allocate', {
						method: 'POST',
						body: JSON.stringify({ primaryKey: 'loc-key' })
					}) as any
				)
			);
			expect(['a', 'b']).toContain(result.shard);
		});

		it('uses provided shardLocations + priorities for location allocation', async () => {
			await mockState.storage.put('coordinator_state', {
				knownShards: ['east', 'west'],
				shardStats: { east: { binding: 'east', count: 0 }, west: { binding: 'west', count: 0 } },
				strategy: 'location',
				roundRobinIndex: 0,
				targetRegion: 'wnam',
				shardLocations: {
					east: { region: 'enam', priority: 1 },
					west: { region: 'wnam', priority: 2 }
				}
			});

			const result = await asJson<{ shard: string }>(
				await coordinator.fetch(
					new Request('http://coordinator/allocate', {
						method: 'POST',
						body: JSON.stringify({ primaryKey: 'west-side', strategy: 'location', availableShards: ['east', 'west'] })
					}) as any
				)
			);
			expect(result.shard).toBe('west');
		});

		it('selectShard returns 400 when no shards are configured', async () => {
			const result = await coordinator.fetch(
				new Request('http://coordinator/allocate', {
					method: 'POST',
					body: JSON.stringify({ primaryKey: 'no-shards' })
				}) as any
			);
			expect(result.status).toBe(400);
		});

		it('flush wipes state', async () => {
			// Use the coordinator's own storage so the flush endpoint actually
			// observes the writes that happened above.
			await coordinator.fetch(new Request('http://coordinator/shards', { method: 'POST', body: JSON.stringify({ shard: 'a' }) }) as any);
			expect(mockState.storage.data.size).toBeGreaterThan(0);
			await coordinator.fetch(new Request('http://coordinator/flush', { method: 'POST' }) as any);
			expect(mockState.storage.data.size).toBe(0);
		});

		it('responds with 404 for unknown endpoints and 200 for health', async () => {
			expect((await coordinator.fetch(new Request('http://coordinator/health') as any)).status).toBe(200);
			expect((await coordinator.fetch(new Request('http://coordinator/nope') as any)).status).toBe(404);
		});
	});
});
