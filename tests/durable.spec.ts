import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ShardCoordinator } from '../src/durable';
import { KVShardMapper } from '../src/kvmap';
import { candidateShards, createManifest, resetPlacementState, savePlacementManifest } from '../src/placement';
import { createInMemoryKVProvider, createInMemorySQLProvider } from '../src/providers-memory';
import { reconcileAllocations } from '../src/reconcile';
import { initialize, listKnownShards, resetConfig } from '../src/router';
import type { CollegeDBConfig } from '../src/types';
import { MockDurableObjectState } from './helpers/durable-storage';

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

	describe('Sequence allocation', () => {
		async function sequence(body: unknown): Promise<Response> {
			return await coordinator.fetch(
				new Request('http://coordinator/sequence', {
					method: 'POST',
					headers: { 'Content-Type': 'application/json' },
					body: JSON.stringify(body)
				}) as any
			);
		}

		it('increments a named sequence monotonically', async () => {
			const first = (await (await sequence({ name: 'tickets' })).json()) as { value: number };
			const second = (await (await sequence({ name: 'tickets' })).json()) as { value: number };
			expect(first.value).toBe(1);
			expect(second.value).toBe(2);
		});

		it('keeps separate counters per name', async () => {
			await sequence({ name: 'tickets' });
			const customers = (await (await sequence({ name: 'customers' })).json()) as { value: number };
			expect(customers.value).toBe(1);
		});

		it('respects the min floor and stays monotonic afterward', async () => {
			const seeded = (await (await sequence({ name: 'labels', min: 50 })).json()) as { value: number };
			expect(seeded.value).toBe(50);
			const next = (await (await sequence({ name: 'labels' })).json()) as { value: number };
			expect(next.value).toBe(51);
		});

		it('ignores a min lower than the current counter', async () => {
			await sequence({ name: 'orders', min: 10 });
			const next = (await (await sequence({ name: 'orders', min: 5 })).json()) as { value: number };
			expect(next.value).toBe(11);
		});

		it('rejects a missing name', async () => {
			const result = await sequence({});
			expect(result.status).toBe(400);
		});
	});
});

describe('ShardCoordinator on ctx.storage.sql', () => {
	type Ctx = { state: MockDurableObjectState; coordinator: ShardCoordinator };

	const call = (coordinator: ShardCoordinator, method: string, path: string, body?: unknown) =>
		coordinator.fetch(
			new Request(`http://coordinator${path}`, { method, body: body === undefined ? undefined : JSON.stringify(body) }) as any
		);
	const json = async <T>(response: Response) => (await response.json()) as T;
	const metaValue = (state: MockDurableObjectState, key: string) =>
		state.storage.sql.exec('SELECT value FROM meta WHERE key = ?', key).toArray()[0]?.value as string | undefined;
	const sqlAllocations = (state: MockDurableObjectState) =>
		new Map(
			state.storage.sql
				.exec('SELECT primary_key, shard FROM allocations ORDER BY primary_key')
				.toArray()
				.map((row) => [row.primary_key as string, row.shard as string])
		);
	const legacyAllocations = (state: MockDurableObjectState) =>
		new Map([...state.storage.data].filter(([key]) => key.startsWith('alloc:')).map(([key, value]) => [key.slice(6), value as string]));
	const allocate = async (coordinator: ShardCoordinator, primaryKey: string, strategy = 'round-robin') =>
		json<{ shard: string; reused?: boolean }>(await call(coordinator, 'POST', '/allocate', { primaryKey, strategy }));

	async function open(state = new MockDurableObjectState()): Promise<Ctx> {
		const coordinator = new ShardCoordinator(state as any);
		await call(coordinator, 'GET', '/health');
		return { state, coordinator };
	}

	async function finishUpgrade({ state, coordinator }: Ctx) {
		for (let i = 0; i < 20 && metaValue(state, 'phase') !== 'dual'; i++) await call(coordinator, 'GET', '/shards');
		expect(metaValue(state, 'phase')).toBe('dual');
	}

	async function warm(): Promise<Ctx> {
		const ctx = await open();
		for (const shard of ['a', 'b', 'c']) await call(ctx.coordinator, 'POST', '/shards', { shard });
		await allocate(ctx.coordinator, 'seed-1');
		await allocate(ctx.coordinator, 'seed-2');
		await finishUpgrade(ctx);
		return ctx;
	}

	async function seedLegacy(state: MockDurableObjectState, count: number) {
		await state.storage.put('coordinator_state', {
			knownShards: ['a', 'b'],
			shardStats: { a: { binding: 'a', count: 3, lastUpdated: 1 }, b: { binding: 'b', count: 4, lastUpdated: 2 } },
			strategy: 'hash',
			roundRobinIndex: 1,
			sequences: { tickets: 41 }
		});
		for (let i = 0; i < count; i++) await state.storage.put(`alloc:key-${String(i).padStart(3, '0')}`, i % 2 ? 'a' : 'b');
	}

	function expectRepresentationsAgree(state: MockDurableObjectState) {
		const seq = Number(metaValue(state, 'seq'));
		const legacy = state.storage.data.get('coordinator_state') as any;
		const shards = state.storage.sql
			.exec('SELECT name FROM shards ORDER BY position')
			.toArray()
			.map((row) => row.name);
		expect(state.storage.data.get('gen')).toBe(seq);
		expect(legacy.seq).toBe(seq);
		expect(legacy.knownShards).toEqual(shards);
		expect(legacy.roundRobinIndex).toBe(Number(metaValue(state, 'round_robin_index')));
		expect(legacyAllocations(state)).toEqual(sqlAllocations(state));
	}

	beforeEach(() => {
		vi.spyOn(console, 'error').mockImplementation(() => {});
	});

	afterEach(() => {
		vi.restoreAllMocks();
		resetConfig();
	});

	it('creates the sql tables and an upgrade phase', async () => {
		const { state } = await open();
		const names = state.storage.sql
			.exec("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
			.toArray()
			.map((row) => row.name);
		expect(names).toEqual(['allocations', 'meta', 'sequences', 'shards', 'stats']);
		expect(metaValue(state, 'phase')).toBe('legacy');
	});

	it('dual-writes allocations, shards and sequences to the legacy keys', async () => {
		const { state, coordinator } = await warm();
		await call(coordinator, 'POST', '/sequence', { name: 'tickets' });
		await call(coordinator, 'POST', '/sequence', { name: 'tickets' });

		expectRepresentationsAgree(state);
		const legacy = state.storage.data.get('coordinator_state') as any;
		expect(legacy.knownShards).toEqual(['a', 'b', 'c']);
		expect(legacy.sequences).toEqual({ tickets: 2 });
		expect(sqlAllocations(state).get('seed-1')).toBe(state.storage.data.get('alloc:seed-1'));
		expect(sqlAllocations(state).size).toBe(2);
	});

	describe('staged upgrade', () => {
		it('copies legacy allocations in pages behind a durable cursor', async () => {
			const state = new MockDurableObjectState();
			await seedLegacy(state, 450);
			const { coordinator } = await open(state);
			expect(metaValue(state, 'phase')).toBe('legacy');

			expect(await json(await call(coordinator, 'GET', '/shards'))).toEqual(['a', 'b']);
			expect(metaValue(state, 'phase')).toBe('copying');
			expect(sqlAllocations(state).size).toBe(0);

			await call(coordinator, 'GET', '/shards');
			expect(metaValue(state, 'cursor')).toBe('alloc:key-199');
			expect(sqlAllocations(state).size).toBe(200);

			// key-440 sits past the cursor, so the lookup falls through to the legacy key
			expect(await allocate(coordinator, 'key-440')).toEqual({ shard: 'b', reused: true });
			expect(metaValue(state, 'cursor')).toBe('alloc:key-399');
			expect(sqlAllocations(state).has('key-440')).toBe(false);

			await call(coordinator, 'GET', '/shards');
			expect(metaValue(state, 'phase')).toBe('dual');
			expect(sqlAllocations(state)).toEqual(legacyAllocations(state));
			expect(sqlAllocations(state).size).toBe(450);
		});

		it('keeps statistics, strategy and sequences across the upgrade', async () => {
			const state = new MockDurableObjectState();
			await seedLegacy(state, 3);
			const ctx = await open(state);
			await finishUpgrade(ctx);

			const stats = await json<Array<{ binding: string; count: number }>>(await call(ctx.coordinator, 'GET', '/stats'));
			expect(stats.map((s) => [s.binding, s.count])).toEqual([
				['a', 3],
				['b', 4]
			]);
			expect(await json(await call(ctx.coordinator, 'POST', '/sequence', { name: 'tickets' }))).toEqual({ value: 42 });
			expect(JSON.parse(metaValue(state, 'config')!).strategy).toBe('hash');
		});

		it('resumes from the cursor after a crash inside a page', async () => {
			const state = new MockDurableObjectState();
			await seedLegacy(state, 450);
			const { coordinator } = await open(state);
			await call(coordinator, 'GET', '/shards');
			await call(coordinator, 'GET', '/shards');
			expect(metaValue(state, 'cursor')).toBe('alloc:key-199');

			state.storage.armFault(50);
			expect((await call(coordinator, 'GET', '/shards')).status).toBe(500);
			state.storage.disarmFault();
			expect(metaValue(state, 'cursor')).toBe('alloc:key-199');
			expect(sqlAllocations(state).size).toBe(200);

			const ctx = await open(state);
			await finishUpgrade(ctx);
			expect(sqlAllocations(state)).toEqual(legacyAllocations(state));
			expect(sqlAllocations(state).size).toBe(450);
		});

		it('survives a crash at every write of the upgrade', async () => {
			let completed = false;
			for (let n = 1; n <= 80 && !completed; n++) {
				const state = new MockDurableObjectState();
				await seedLegacy(state, 5);
				const { coordinator } = await open(state);
				state.storage.armFault(n);
				for (let i = 0; i < 4; i++) await call(coordinator, 'GET', '/shards');
				completed = !state.storage.faultTriggered;
				state.storage.disarmFault();

				const ctx = await open(state);
				await finishUpgrade(ctx);
				expect(sqlAllocations(state)).toEqual(legacyAllocations(state));
				expect(sqlAllocations(state).size).toBe(5);
				expect(await json(await call(ctx.coordinator, 'GET', '/shards'))).toEqual(['a', 'b']);
				expect(await json(await call(ctx.coordinator, 'POST', '/sequence', { name: 'tickets' }))).toEqual({ value: 42 });
			}
			expect(completed).toBe(true);
		});

		it('reads from sql once the cutover flag has flipped, even after a restart', async () => {
			const state = new MockDurableObjectState();
			await seedLegacy(state, 5);
			const ctx = await open(state);
			await finishUpgrade(ctx);

			const reopened = await open(state);
			const get = vi.spyOn(state.storage, 'get');
			expect(await allocate(reopened.coordinator, 'key-001')).toEqual({ shard: 'a', reused: true });
			expect(get.mock.calls.map(([key]) => key)).not.toContain('alloc:key-001');
		});
	});

	describe('crash recovery', () => {
		it('converges both representations after a crash at every write of an allocation', async () => {
			let completed = false;
			let crashedAfterSqlCommit = 0;
			let crashedBeforeSqlCommit = 0;

			for (let n = 1; n <= 40 && !completed; n++) {
				const { state, coordinator } = await warm();
				state.storage.armFault(n);
				const response = await call(coordinator, 'POST', '/allocate', { primaryKey: 'crash-key', strategy: 'round-robin' });
				completed = !state.storage.faultTriggered;
				state.storage.disarmFault();

				const reopened = await open(state);
				expectRepresentationsAgree(state);

				const row = sqlAllocations(state).get('crash-key');
				if (response.status === 200) expect(row).toBe((await json<{ shard: string }>(response)).shard);
				else if (row) crashedAfterSqlCommit++;
				else crashedBeforeSqlCommit++;

				const retry = await allocate(reopened.coordinator, 'crash-key');
				if (row) expect(retry.shard).toBe(row);
				expect(sqlAllocations(state).get('crash-key')).toBe(retry.shard);
				expectRepresentationsAgree(state);
			}

			expect(completed).toBe(true);
			expect(crashedAfterSqlCommit).toBeGreaterThan(0);
			expect(crashedBeforeSqlCommit).toBeGreaterThan(0);
		});

		it('converges after a crash at every write of a sequence value', async () => {
			let completed = false;
			for (let n = 1; n <= 40 && !completed; n++) {
				const { state, coordinator } = await warm();
				await call(coordinator, 'POST', '/sequence', { name: 'tickets' });
				state.storage.armFault(n);
				const response = await call(coordinator, 'POST', '/sequence', { name: 'tickets' });
				completed = !state.storage.faultTriggered;
				state.storage.disarmFault();

				const reopened = await open(state);
				expectRepresentationsAgree(state);
				const next = await json<{ value: number }>(await call(reopened.coordinator, 'POST', '/sequence', { name: 'tickets' }));
				if (response.status === 200) expect(next.value).toBe(3);
				else expect(next.value).toBeGreaterThanOrEqual(2);
				expectRepresentationsAgree(state);
			}
			expect(completed).toBe(true);
		});

		it('replays a failed legacy write on the next request of the same instance', async () => {
			const { state, coordinator } = await warm();
			state.storage.armFault(5);
			expect((await call(coordinator, 'POST', '/allocate', { primaryKey: 'half', strategy: 'hash' })).status).toBe(500);
			state.storage.disarmFault();
			expect(sqlAllocations(state).has('half')).toBe(true);
			expect(state.storage.data.has('alloc:half')).toBe(true);
			expect(state.storage.data.get('gen')).not.toBe(Number(metaValue(state, 'seq')));

			await call(coordinator, 'GET', '/shards');
			expectRepresentationsAgree(state);
		});

		it('discards a newer legacy write whose generation marker never landed', async () => {
			const { state } = await warm();
			const sqlSeq = Number(metaValue(state, 'seq'));
			const legacy = state.storage.data.get('coordinator_state') as any;
			state.storage.data.set('coordinator_state', { ...legacy, seq: 999, knownShards: ['bogus'] });

			const reopened = await open(state);
			expect(await json(await call(reopened.coordinator, 'GET', '/shards'))).toEqual(['a', 'b', 'c']);
			expect(Number(metaValue(state, 'seq'))).toBe(sqlSeq);
			expectRepresentationsAgree(state);
		});

		it('adopts a legacy generation that is newer and complete', async () => {
			const { state } = await warm();
			const sqlSeq = Number(metaValue(state, 'seq'));
			const legacy = state.storage.data.get('coordinator_state') as any;
			state.storage.data.set('coordinator_state', { ...legacy, seq: sqlSeq + 5, knownShards: ['x', 'y'] });
			state.storage.data.set('gen', sqlSeq + 5);

			const reopened = await open(state);
			expect(await json(await call(reopened.coordinator, 'GET', '/shards'))).toEqual(['x', 'y']);
			expect(Number(metaValue(state, 'seq'))).toBe(sqlSeq + 5);
		});
	});

	describe('legacy window', () => {
		it('leaves the legacy keys readable by the previous version until retired', async () => {
			const { state, coordinator } = await warm();
			await allocate(coordinator, 'late-key');
			await call(coordinator, 'POST', '/stats', { shard: 'a', count: 9 });

			const legacy = state.storage.data.get('coordinator_state') as any;
			expect(legacy.knownShards).toEqual(['a', 'b', 'c']);
			expect(legacy.shardStats.a.count).toBe(9);
			expect(state.storage.data.get('alloc:late-key')).toBe(sqlAllocations(state).get('late-key'));
			expect(state.storage.data.get('alloc:seed-2')).toBe(sqlAllocations(state).get('seed-2'));
		});

		it('retires the legacy keys on request and stops writing them', async () => {
			const { state, coordinator } = await warm();
			expect(await json(await call(coordinator, 'POST', '/retire-legacy'))).toEqual({ success: true });
			expect(metaValue(state, 'phase')).toBe('sql-only');
			expect(state.storage.data.size).toBe(0);

			await allocate(coordinator, 'after-retire');
			await call(coordinator, 'POST', '/sequence', { name: 'tickets' });
			expect(state.storage.data.size).toBe(0);

			const reopened = await open(state);
			expect(state.storage.data.size).toBe(0);
			expect(await allocate(reopened.coordinator, 'after-retire')).toMatchObject({ reused: true });
		});

		it('finishes a pending copy before it retires', async () => {
			const state = new MockDurableObjectState();
			await seedLegacy(state, 450);
			const { coordinator } = await open(state);

			await coordinator.retireLegacy();
			expect(metaValue(state, 'phase')).toBe('sql-only');
			expect(sqlAllocations(state).size).toBe(450);
			expect(state.storage.data.size).toBe(0);
		});
	});

	describe('allocation ledger', () => {
		it('serves one shard to concurrent allocations of the same key', async () => {
			const { state, coordinator } = await warm();
			const before = Number(metaValue(state, 'round_robin_index'));

			const results = await Promise.all(Array.from({ length: 6 }, () => allocate(coordinator, 'contended')));
			expect(new Set(results.map((r) => r.shard)).size).toBe(1);
			expect(results.filter((r) => r.reused).length).toBe(5);
			expect(Number(metaValue(state, 'round_robin_index'))).toBe((before + 1) % 3);
			expect(sqlAllocations(state).size).toBe(3);
		});

		it('gives concurrent allocations of different keys different round-robin shards', async () => {
			const { coordinator } = await warm();
			const results = await Promise.all(['x1', 'x2', 'x3'].map((key) => allocate(coordinator, key)));
			expect(new Set(results.map((r) => r.shard)).size).toBe(3);
		});

		it('pages through allocations in key order', async () => {
			const { coordinator } = await warm();
			for (let i = 0; i < 5; i++) await allocate(coordinator, `p-${i}`);
			type Page = { allocations: Array<{ primaryKey: string; shard: string; createdAt: number }>; next: string | null };
			const page = async (query: string) => json<Page>(await call(coordinator, 'GET', `/allocations${query}`));

			const first = await page('?limit=3');
			expect(first.allocations.map((a) => a.primaryKey)).toEqual(['p-0', 'p-1', 'p-2']);
			expect(first.next).toBe('p-2');
			expect(first.allocations[0]!.createdAt).toBeTypeOf('number');

			const second = await page(`?after=${first.next}&limit=3`);
			expect(second.allocations.map((a) => a.primaryKey)).toEqual(['p-3', 'p-4', 'seed-1']);

			const third = await page(`?after=${second.next}&limit=3`);
			expect(third.allocations.map((a) => a.primaryKey)).toEqual(['seed-2']);
			expect(third.next).toBeNull();
		});

		it('completes a pending upgrade before it lists allocations', async () => {
			const state = new MockDurableObjectState();
			await seedLegacy(state, 450);
			const { coordinator } = await open(state);

			const listed = await json<{ allocations: unknown[] }>(await call(coordinator, 'GET', '/allocations?limit=1000'));
			expect(listed.allocations).toHaveLength(450);
			expect(metaValue(state, 'phase')).toBe('dual');
		});

		it('prunes only the keys the caller names, in both representations', async () => {
			const { state, coordinator } = await warm();

			const response = await call(coordinator, 'POST', '/allocations/prune', { primaryKeys: ['seed-1', 'missing'] });
			expect(await json(response)).toEqual({ pruned: 1 });
			expect(sqlAllocations(state).has('seed-1')).toBe(false);
			expect(sqlAllocations(state).has('seed-2')).toBe(true);
			expect(state.storage.data.has('alloc:seed-1')).toBe(false);
			expectRepresentationsAgree(state);

			const again = await allocate(coordinator, 'seed-1');
			expect(again.reused).toBeUndefined();
			expect(sqlAllocations(state).has('seed-1')).toBe(true);
		});

		it('rejects a malformed or oversized prune request', async () => {
			const { coordinator } = await warm();
			expect((await call(coordinator, 'POST', '/allocations/prune', { primaryKeys: 'seed-1' })).status).toBe(400);
			expect((await call(coordinator, 'POST', '/allocations/prune', {})).status).toBe(400);
			const tooMany = Array.from({ length: 129 }, (_, i) => `k-${i}`);
			expect((await call(coordinator, 'POST', '/allocations/prune', { primaryKeys: tooMany })).status).toBe(400);
		});

		it('forgets a key in both representations', async () => {
			const { state, coordinator } = await warm();
			await call(coordinator, 'POST', '/forget', { primaryKey: 'seed-1' });
			expect(sqlAllocations(state).has('seed-1')).toBe(false);
			expect(state.storage.data.has('alloc:seed-1')).toBe(false);
			expectRepresentationsAgree(state);
		});
	});

	it('recreates the tables and restarts the upgrade after a flush', async () => {
		const { state, coordinator } = await warm();
		await call(coordinator, 'POST', '/flush');
		expect(state.storage.data.size).toBe(0);
		expect(metaValue(state, 'phase')).toBe('legacy');

		await call(coordinator, 'POST', '/shards', { shard: 'fresh' });
		expect(await json(await call(coordinator, 'GET', '/shards'))).toEqual(['fresh']);
	});

	describe('coordinatorScope', () => {
		function namespace(coordinators: Map<string, ShardCoordinator>, ids: string[]) {
			return {
				idFromName: (name: string) => {
					ids.push(name);
					return name;
				},
				get: (id: string) => ({ fetch: (url: string, init?: RequestInit) => coordinators.get(id)!.fetch(new Request(url, init) as any) })
			} as any;
		}

		it('reaches a separate coordinator per scope and keeps default when unset', async () => {
			const coordinators = new Map<string, ShardCoordinator>();
			for (const [name, shard] of [
				['tenant-a', 'only-a'],
				['tenant-b', 'only-b'],
				['default', 'only-default']
			] as const) {
				const coordinator = new ShardCoordinator(new MockDurableObjectState() as any);
				await call(coordinator, 'POST', '/shards', { shard });
				coordinators.set(name, coordinator);
			}

			const ids: string[] = [];
			const base = { kv: createInMemoryKVProvider(), shards: { s1: createInMemorySQLProvider() }, disableAutoMigration: true };
			const coordinator = namespace(coordinators, ids);

			initialize({ ...base, coordinator, coordinatorScope: 'tenant-a' });
			expect(await listKnownShards()).toEqual(['only-a']);
			initialize({ ...base, coordinator, coordinatorScope: 'tenant-b' });
			expect(await listKnownShards()).toEqual(['only-b']);
			initialize({ ...base, coordinator });
			expect(await listKnownShards()).toEqual(['only-default']);
			expect(ids).toEqual(['tenant-a', 'tenant-b', 'default']);
		});
	});
});

describe('reconcileAllocations', () => {
	type Call = { method: string; url: string; body?: string };

	let state: MockDurableObjectState;
	let coordinator: ShardCoordinator;
	let kv: ReturnType<typeof createInMemoryKVProvider>;
	let mapper: KVShardMapper;
	let shards: CollegeDBConfig['shards'];
	let calls: Call[];

	const callsTo = (path: string) => calls.filter((call) => new URL(call.url).pathname === path);

	function namespace(onCall: () => void = () => {}) {
		return {
			idFromName: (name: string) => name,
			get: () => ({
				fetch: (url: string, init?: RequestInit) => {
					calls.push({ method: init?.method ?? 'GET', url, body: init?.body as string | undefined });
					onCall();
					return coordinator.fetch(new Request(url, init) as any);
				}
			})
		} as any;
	}

	// initialize() syncs known shards in the background; let that write land before any spy is set.
	async function setup(options: Partial<CollegeDBConfig> = {}, onCall?: () => void) {
		initialize({ kv, shards, coordinator: namespace(onCall), disableAutoMigration: true, ...options });
		await new Promise((resolve) => setTimeout(resolve, 0));
	}

	function ledger(rows: Array<[string, string]>) {
		for (const [primaryKey, shard] of rows) {
			state.storage.sql.exec('INSERT INTO allocations (primary_key, shard, created_at) VALUES (?, ?, ?)', primaryKey, shard, 1);
		}
	}

	function ledgerShards() {
		return new Map(
			state.storage.sql
				.exec('SELECT primary_key, shard FROM allocations')
				.toArray()
				.map((row) => [row.primary_key as string, row.shard as string])
		);
	}

	const other = (shard: string) => (shard === 'a' ? 'b' : 'a');

	beforeEach(async () => {
		state = new MockDurableObjectState();
		coordinator = new ShardCoordinator(state as any);
		await coordinator.fetch(new Request('http://coordinator/health') as any);
		kv = createInMemoryKVProvider();
		mapper = new KVShardMapper(kv);
		shards = { a: createInMemorySQLProvider(), b: createInMemorySQLProvider(), c: createInMemorySQLProvider() };
		calls = [];
	});

	afterEach(() => {
		resetConfig();
		resetPlacementState();
	});

	it('observational run changes nothing (KV and coordinator untouched)', async () => {
		ledger([
			['k1', 'a'],
			['k2', 'a'],
			['k3', 'b']
		]);
		await mapper.setShardMapping('k1', 'a');
		await mapper.setShardMapping('k2', 'b');
		const before = ledgerShards();
		await setup();
		const put = vi.spyOn(kv, 'put');
		const remove = vi.spyOn(kv, 'delete');

		await reconcileAllocations();

		expect(put).not.toHaveBeenCalled();
		expect(remove).not.toHaveBeenCalled();
		expect(callsTo('/allocations/prune')).toHaveLength(0);
		expect(ledgerShards()).toEqual(before);
		expect(await mapper.getShardMapping('k3')).toBeNull();
	});

	it('counts agree, missing and disagree correctly across pages', async () => {
		ledger([
			['k1', 'a'],
			['k2', 'a'],
			['k3', 'b'],
			['k4', 'c'],
			['k5', 'c']
		]);
		await mapper.setShardMapping('k1', 'a');
		await mapper.setShardMapping('k2', 'b');
		await mapper.setShardMapping('k4', 'c');
		await setup();

		const report = await reconcileAllocations({ limit: 2 });

		expect(report).toEqual({
			examined: 5,
			agreed: 2,
			missingMapping: ['k3', 'k5'],
			missingMappingCount: 2,
			disagreeing: [{ key: 'k2', ledgerShard: 'a', recordShard: 'b' }],
			disagreeingCount: 1,
			repaired: 0,
			pruned: 0
		});
		const afters = callsTo('/allocations').map((call) => new URL(call.url).searchParams.get('after'));
		expect(afters).toEqual(['', 'k2', 'k4']);
	});

	it('repair writes only missing mappings and never touches a disagreeing row', async () => {
		ledger([
			['k1', 'a'],
			['k2', 'a'],
			['k3', 'b'],
			['k4', 'c'],
			['k5', 'c']
		]);
		await mapper.setShardMapping('k1', 'a');
		await mapper.setShardMapping('k2', 'b');
		await mapper.setShardMapping('k4', 'c');
		await setup();
		const put = vi.spyOn(kv, 'put');

		const report = await reconcileAllocations({ limit: 2, repair: true });

		expect(report.repaired).toBe(2);
		expect(put).toHaveBeenCalledTimes(2);
		expect(await mapper.getShardMapping('k3')).toMatchObject({ shard: 'b' });
		expect(await mapper.getShardMapping('k5')).toMatchObject({ shard: 'c' });
		expect(await mapper.getShardMapping('k2')).toMatchObject({ shard: 'b' });
		expect(report.disagreeing).toEqual([{ key: 'k2', ledgerShard: 'a', recordShard: 'b' }]);
	});

	it('pruneVerified prunes only agreed keys and batches at 128', async () => {
		const verified = Array.from({ length: 130 }, (_, i) => `p${String(i).padStart(3, '0')}`);
		ledger([...verified.map((key): [string, string] => [key, 'a']), ['x', 'a'], ['y', 'a']]);
		for (const key of verified) await mapper.setShardMapping(key, 'a');
		await mapper.setShardMapping('x', 'b');
		await setup();

		const report = await reconcileAllocations({ limit: 1000, pruneVerified: true });

		expect(report).toMatchObject({ examined: 132, agreed: 130, pruned: 130, missingMappingCount: 1, disagreeingCount: 1 });
		const batches = callsTo('/allocations/prune').map((call) => JSON.parse(call.body!).primaryKeys.length);
		expect(batches).toEqual([128, 2]);
		expect([...ledgerShards().keys()].sort()).toEqual(['x', 'y']);
	});

	it('computed placement: a ledger row equal to the computed owner is agreed', async () => {
		const manifest = createManifest(['a', 'b', 'c'], 'hrw');
		await savePlacementManifest(kv, manifest);
		const owner = (key: string) => candidateShards(key, manifest, ['a', 'b', 'c'])[0]!;
		ledger([
			['p1', owner('p1')],
			['p2', other(owner('p2'))]
		]);
		await setup({ placement: 'computed' });

		const report = await reconcileAllocations({ pruneVerified: true });

		expect(report).toMatchObject({
			examined: 2,
			agreed: 1,
			missingMapping: [],
			disagreeing: [{ key: 'p2', ledgerShard: other(owner('p2')), recordShard: owner('p2') }],
			pruned: 0
		});
		expect(callsTo('/allocations/prune')).toHaveLength(0);
	});

	it('throws COORDINATOR_NOT_CONFIGURED when no coordinator is configured', async () => {
		initialize({ kv, shards, disableAutoMigration: true });

		await expect(reconcileAllocations()).rejects.toMatchObject({ code: 'COORDINATOR_NOT_CONFIGURED' });
	});

	it('caps report key lists at 1000 entries and keeps exact counts', async () => {
		const missing = Array.from({ length: 1005 }, (_, i) => `m${String(i).padStart(4, '0')}`);
		const disagree = Array.from({ length: 1005 }, (_, i) => `d${String(i).padStart(4, '0')}`);
		ledger([...missing.map((key): [string, string] => [key, 'a']), ...disagree.map((key): [string, string] => [key, 'a'])]);
		for (const key of disagree) await mapper.setShardMapping(key, 'b');
		await setup();

		const report = await reconcileAllocations({ limit: 1000 });

		expect(report.examined).toBe(2010);
		expect(report.missingMapping).toHaveLength(1000);
		expect(report.missingMapping[0]).toBe('m0000');
		expect(report.missingMappingCount).toBe(1005);
		expect(report.disagreeing).toHaveLength(1000);
		expect(report.disagreeingCount).toBe(1005);
	});

	it('an aborted signal stops between pages', async () => {
		ledger([
			['k1', 'a'],
			['k2', 'a'],
			['k3', 'a'],
			['k4', 'a'],
			['k5', 'a']
		]);
		const controller = new AbortController();
		await setup({}, () => controller.abort());

		await expect(reconcileAllocations({ limit: 2, signal: controller.signal })).rejects.toMatchObject({ name: 'AbortError' });
		expect(callsTo('/allocations')).toHaveLength(1);
	});
});
