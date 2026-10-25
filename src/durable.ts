/**
 * @fileoverview Durable Object for coordinating shard allocation and maintaining statistics
 *
 * This module provides the ShardCoordinator Durable Object that manages shard allocation
 * strategies and maintains real-time statistics about shard utilization. It provides
 * an HTTP API for other parts of the system to interact with the coordinator.
 *
 * @example
 * ```typescript
 * // In wrangler.jsonc:
 * // {
 * //   "durable_objects": {
 * //     "bindings": [
 * //       { "name": "ShardCoordinator", "class_name": "ShardCoordinator" }
 * //     ]
 * //   }
 * // }
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
 * @author Gregory Mitchell
 * @since 1.0.0
 */

import type { DurableObjectState } from '@cloudflare/workers-types';
import { CollegeDBError } from './errors';
import { hrwShard } from './placement';
import type { D1Region, MixedShardingStrategy, OperationType, ShardCoordinatorState, ShardingStrategy } from './types';

/**
 * Legacy storage key prefix for the per-key allocation ledger.
 *
 * Recording each decision is what makes allocation idempotent across concurrent
 * callers, since a Durable Object serializes its own requests. The ledger lives in
 * the `allocations` table; these keys are the copy older versions read.
 * @private
 */
const ALLOCATION_LEDGER_PREFIX = 'alloc:';

/** @private */
const LEGACY_STATE_KEY = 'coordinator_state';

/** @private */
const LEGACY_GENERATION_KEY = 'gen';

/** @private */
const UPGRADE_PAGE_SIZE = 200;

/** Durable Object storage rejects batch deletes and puts above 128 keys. @private */
const MAX_KEYS_PER_WRITE = 128;

/** @private */
const SCHEMA = [
	'CREATE TABLE IF NOT EXISTS shards (name TEXT PRIMARY KEY, position INTEGER NOT NULL)',
	'CREATE TABLE IF NOT EXISTS stats (binding TEXT PRIMARY KEY, count INTEGER NOT NULL, last_updated INTEGER)',
	'CREATE TABLE IF NOT EXISTS allocations (primary_key TEXT PRIMARY KEY, shard TEXT NOT NULL, created_at INTEGER NOT NULL)',
	'CREATE TABLE IF NOT EXISTS sequences (name TEXT PRIMARY KEY, value INTEGER NOT NULL)',
	'CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)'
];

/**
 * Where the coordinator is in the move from legacy storage keys to SQL.
 *
 * `legacy`: nothing copied yet. `copying`: state copied, allocations copied in pages behind
 * a cursor, reads of uncopied keys fall through to the legacy keys. `dual`: SQL serves every
 * read and writes still land on the legacy keys. `sql-only`: legacy keys retired.
 * @private
 */
type UpgradePhase = 'legacy' | 'copying' | 'dual' | 'sql-only';

/** The ledger change of one commit, kept in `meta` so a half-finished legacy write can be replayed. @private */
interface LedgerOp {
	put?: Array<[string, string]>;
	del?: string[];
}

/** Legacy copy of the coordinator state, stamped with the generation that wrote it. @private */
type LegacyState = ShardCoordinatorState & { seq?: number };

/**
 * Durable Object for coordinating shard allocation and maintaining statistics
 *
 * The ShardCoordinator is a Cloudflare Durable Object that provides centralized
 * coordination for shard allocation across multiple database instances. It maintains
 * state about available shards, allocation strategies, and usage statistics.
 *
 * Key responsibilities:
 * - Track available shards and their current load
 * - Implement allocation strategies (round-robin, random, hash-based)
 * - Provide HTTP API for shard allocation and management
 * - Maintain persistent state in the object's SQLite database (`ctx.storage.sql`)
 *
 * State from earlier versions lives under legacy storage keys. The first requests copy it
 * into SQL in pages, and until {@link ShardCoordinator.retireLegacy} runs every write also
 * lands on the legacy keys, so the previous version can still read the object. That window is
 * read-only for a downgrade: writes the previous version makes to the legacy keys are not
 * copied back when the object upgrades again, and the new version overwrites them.
 *
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
	 * Resolves once the tables exist and legacy recovery has run
	 * @private
	 */
	private ready: Promise<void>;

	/**
	 * Tail of the request queue; a request finishes before the next one starts
	 * @private
	 */
	private queue: Promise<unknown> = Promise.resolve();

	/**
	 * Set when a legacy write failed after SQL committed, so the next request replays it
	 * @private
	 */
	private legacyBehind = false;

	/**
	 * Creates a new ShardCoordinator instance
	 * @param state - Durable Object state provided by Cloudflare runtime
	 */
	constructor(state: DurableObjectState) {
		this.state = state;
		this.ready = state.blockConcurrencyWhile(() => this.initialize());
	}

	/**
	 * Creates the tables and reconciles the legacy copy with SQL before any request runs
	 * @private
	 */
	private async initialize(): Promise<void> {
		this.createSchema();
		await this.recoverLegacy();
	}

	/**
	 * Creates the tables if they are missing and seeds the upgrade phase
	 * @private
	 */
	private createSchema(): void {
		for (const statement of SCHEMA) this.state.storage.sql.exec(statement);
		this.state.storage.sql.exec("INSERT OR IGNORE INTO meta (key, value) VALUES ('phase', 'legacy')");
	}

	/**
	 * Runs one request body after earlier ones finish and after one upgrade step
	 *
	 * Handlers await between reading and writing, so the queue keeps two requests
	 * for the same key from both seeing it unallocated.
	 *
	 * @private
	 * @param task - The request body
	 * @returns Promise resolving to the body's result
	 */
	private exclusive<T>(task: () => Promise<T>): Promise<T> {
		const run = this.queue.then(async () => {
			await this.ready;
			if (this.legacyBehind) {
				await this.recoverLegacy();
				this.legacyBehind = false;
			}
			await this.advanceUpgrade();
			return task();
		});
		this.queue = run.catch(() => {});
		return run;
	}

	/**
	 * Reads one value from the `meta` table
	 * @private
	 */
	private meta(key: string): string | undefined {
		return this.state.storage.sql.exec<{ value: string }>('SELECT value FROM meta WHERE key = ?', key).toArray()[0]?.value;
	}

	/**
	 * Writes one value to the `meta` table
	 * @private
	 */
	private setMeta(key: string, value: string | number): void {
		this.state.storage.sql.exec('INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)', key, String(value));
	}

	/**
	 * Current upgrade phase
	 * @private
	 */
	private phase(): UpgradePhase {
		return (this.meta('phase') ?? 'legacy') as UpgradePhase;
	}

	/**
	 * Generation marker of the SQL representation, bumped by every commit
	 * @private
	 */
	private seq(): number {
		return Number(this.meta('seq') ?? 0);
	}

	/**
	 * Reads the coordinator state from the SQL tables
	 *
	 * The cursors are turned into arrays here so no result stays open across an await.
	 *
	 * @private
	 * @returns The current coordinator state, empty when nothing was stored
	 */
	private readSqlState(): ShardCoordinatorState {
		const sql = this.state.storage.sql;
		const knownShards = sql
			.exec<{ name: string }>('SELECT name FROM shards ORDER BY position')
			.toArray()
			.map((row) => row.name);

		const shardStats: ShardCoordinatorState['shardStats'] = {};
		for (const row of sql
			.exec<{ binding: string; count: number; last_updated: number | null }>('SELECT * FROM stats ORDER BY rowid')
			.toArray()) {
			shardStats[row.binding] = {
				binding: row.binding,
				count: row.count,
				...(row.last_updated !== null && { lastUpdated: row.last_updated })
			};
		}

		const sequences: Record<string, number> = {};
		for (const row of sql.exec<{ name: string; value: number }>('SELECT * FROM sequences ORDER BY rowid').toArray()) {
			sequences[row.name] = row.value;
		}

		const config = JSON.parse(this.meta('config') ?? '{}') as Pick<ShardCoordinatorState, 'strategy' | 'targetRegion' | 'shardLocations'>;
		return {
			knownShards,
			shardStats,
			sequences,
			strategy: config.strategy ?? 'round-robin',
			roundRobinIndex: Number(this.meta('round_robin_index') ?? 0),
			...(config.targetRegion && { targetRegion: config.targetRegion }),
			...(config.shardLocations && { shardLocations: config.shardLocations })
		};
	}

	/**
	 * Replaces the state tables with the given state; call inside a transaction
	 * @private
	 * @param state - The state to store
	 */
	private writeSqlState(state: ShardCoordinatorState): void {
		const sql = this.state.storage.sql;
		sql.exec('DELETE FROM shards');
		sql.exec('DELETE FROM stats');
		sql.exec('DELETE FROM sequences');
		state.knownShards.forEach((name, position) => sql.exec('INSERT INTO shards (name, position) VALUES (?, ?)', name, position));
		for (const [binding, stat] of Object.entries(state.shardStats)) {
			sql.exec('INSERT INTO stats (binding, count, last_updated) VALUES (?, ?, ?)', binding, stat.count, stat.lastUpdated ?? null);
		}
		for (const [name, value] of Object.entries(state.sequences ?? {})) {
			sql.exec('INSERT INTO sequences (name, value) VALUES (?, ?)', name, value);
		}
		this.setMeta('round_robin_index', state.roundRobinIndex);
		this.setMeta(
			'config',
			JSON.stringify({ strategy: state.strategy, targetRegion: state.targetRegion, shardLocations: state.shardLocations })
		);
	}

	/**
	 * Applies allocation changes to the `allocations` table; call inside a transaction
	 * @private
	 * @param op - Rows to record and keys to drop
	 * @returns How many rows the deletes removed
	 */
	private applyLedger(op: LedgerOp): number {
		const sql = this.state.storage.sql;
		let deleted = 0;
		for (const [key, shard] of op.put ?? []) {
			sql.exec(
				'INSERT INTO allocations (primary_key, shard, created_at) VALUES (?, ?, ?) ON CONFLICT(primary_key) DO UPDATE SET shard = excluded.shard',
				key,
				shard,
				Date.now()
			);
		}
		for (const key of op.del ?? []) {
			deleted += sql.exec('SELECT 1 FROM allocations WHERE primary_key = ?', key).toArray().length;
			sql.exec('DELETE FROM allocations WHERE primary_key = ?', key);
		}
		return deleted;
	}

	/**
	 * Commits one change: SQL first with the generation marker last in the same
	 * transaction, then the legacy copy with its own marker last
	 *
	 * A crash between the two leaves SQL ahead of the legacy marker, which
	 * {@link recoverLegacy} repairs before the next request runs.
	 *
	 * @private
	 * @param change - New coordinator state and or allocation changes
	 * @returns How many allocation rows the change deleted
	 */
	private async commit(change: { state?: ShardCoordinatorState; ledger?: LedgerOp }): Promise<number> {
		const seq = this.seq() + 1;
		let deleted = 0;
		this.state.storage.transactionSync(() => {
			if (change.state) this.writeSqlState(change.state);
			if (change.ledger) deleted = this.applyLedger(change.ledger);
			this.setMeta('last_op', JSON.stringify(change.ledger ?? {}));
			this.setMeta('seq', seq);
		});

		if (this.phase() !== 'sql-only') {
			try {
				await this.writeLegacy(seq, change.ledger ?? {});
			} catch (error) {
				this.legacyBehind = true;
				throw error;
			}
		}
		return deleted;
	}

	/**
	 * Writes the legacy copy of one commit: allocation keys, the state, then the generation key
	 * @private
	 * @param seq - Generation of the commit
	 * @param op - Allocation changes of the commit
	 */
	private async writeLegacy(seq: number, op: LedgerOp): Promise<void> {
		const storage = this.state.storage;
		if (op.put?.length) {
			await storage.put(Object.fromEntries(op.put.map(([key, shard]) => [`${ALLOCATION_LEDGER_PREFIX}${key}`, shard])));
		}
		if (op.del?.length) {
			await storage.delete(op.del.map((key) => `${ALLOCATION_LEDGER_PREFIX}${key}`));
		}
		await storage.put(LEGACY_STATE_KEY, { ...this.readSqlState(), seq } satisfies LegacyState);
		await storage.put(LEGACY_GENERATION_KEY, seq);
	}

	/**
	 * Picks the representation with the highest completed generation and makes the other match
	 *
	 * The legacy copy is complete when its `gen` key equals the stamp on its state. SQL is
	 * the default winner. Legacy wins only when it is complete and strictly newer; a legacy
	 * state stamped past its own `gen` is an interrupted write and is overwritten from SQL.
	 *
	 * @private
	 */
	private async recoverLegacy(): Promise<void> {
		const phase = this.phase();
		if (phase === 'legacy' || phase === 'sql-only') return;

		const storage = this.state.storage;
		const generation = (await storage.get<number>(LEGACY_GENERATION_KEY)) ?? 0;
		const legacy = await storage.get<LegacyState>(LEGACY_STATE_KEY);
		const complete = (legacy?.seq ?? 0) === generation;
		const sqlSeq = this.seq();

		if (complete && generation > sqlSeq && legacy) {
			storage.transactionSync(() => {
				this.writeSqlState(legacy);
				this.state.storage.sql.exec('DELETE FROM allocations');
				this.state.storage.sql.exec("DELETE FROM meta WHERE key = 'cursor'");
				this.setMeta('seq', generation);
				this.setMeta('phase', 'copying');
			});
		} else if (!complete || generation !== sqlSeq) {
			await this.writeLegacy(sqlSeq, JSON.parse(this.meta('last_op') ?? '{}') as LedgerOp);
		}
	}

	/**
	 * Runs one step of the legacy to SQL copy
	 *
	 * The first step copies the state; each later step copies one page of the allocation
	 * ledger and moves the cursor in the same transaction, and the last page flips the
	 * phase to `dual`. Allocations made during the copy are dual-written, so the copy
	 * only fills in keys SQL has not seen.
	 *
	 * @private
	 */
	private async advanceUpgrade(): Promise<void> {
		const storage = this.state.storage;
		const phase = this.phase();

		if (phase === 'legacy') {
			const legacy = await storage.get<LegacyState>(LEGACY_STATE_KEY);
			storage.transactionSync(() => {
				this.writeSqlState(legacy ?? { knownShards: [], shardStats: {}, strategy: 'round-robin', roundRobinIndex: 0 });
				this.setMeta('phase', 'copying');
			});
		} else if (phase === 'copying') {
			const cursor = this.meta('cursor');
			const page = await storage.list<string>({
				prefix: ALLOCATION_LEDGER_PREFIX,
				limit: UPGRADE_PAGE_SIZE,
				...(cursor && { startAfter: cursor })
			});
			storage.transactionSync(() => {
				for (const [key, shard] of page) {
					if (typeof shard !== 'string') continue;
					storage.sql.exec(
						'INSERT INTO allocations (primary_key, shard, created_at) VALUES (?, ?, ?) ON CONFLICT(primary_key) DO UPDATE SET shard = excluded.shard',
						key.slice(ALLOCATION_LEDGER_PREFIX.length),
						shard,
						Date.now()
					);
				}
				if (page.size < UPGRADE_PAGE_SIZE) {
					storage.sql.exec("DELETE FROM meta WHERE key = 'cursor'");
					this.setMeta('phase', 'dual');
				} else {
					this.setMeta('cursor', [...page.keys()].pop()!);
				}
			});
		}
	}

	/**
	 * Runs the remaining copy steps, for routes that need every allocation in SQL
	 * @private
	 */
	private async completeUpgrade(): Promise<void> {
		while (this.phase() === 'legacy' || this.phase() === 'copying') await this.advanceUpgrade();
	}

	/**
	 * Looks up the recorded shard for a key, falling through to the legacy key while the copy runs
	 * @private
	 * @param primaryKey - The key to look up
	 * @returns The recorded shard, or undefined when none is recorded
	 */
	private async readAllocation(primaryKey: string): Promise<string | undefined> {
		const row = this.state.storage.sql
			.exec<{ shard: string }>('SELECT shard FROM allocations WHERE primary_key = ?', primaryKey)
			.toArray()[0];
		if (row) return row.shard;
		if (this.phase() !== 'copying') return undefined;
		return await this.state.storage.get<string>(`${ALLOCATION_LEDGER_PREFIX}${primaryKey}`);
	}

	/**
	 * Stops writing the legacy keys and deletes them
	 *
	 * Finishes any pending copy first. The phase flips to `sql-only` before the keys go, so an
	 * interrupted run leaves unused keys behind and calling it again finishes the cleanup.
	 * Afterwards the previous coordinator version cannot read this object; downgrading past
	 * this call is unsupported.
	 *
	 * @returns Promise that resolves when the legacy keys are gone
	 * @since 1.4.0
	 */
	async retireLegacy(): Promise<void> {
		await this.exclusive(async () => {
			await this.completeUpgrade();
			this.setMeta('phase', 'sql-only');
			const storage = this.state.storage;
			await storage.delete([LEGACY_STATE_KEY, LEGACY_GENERATION_KEY]);
			for (;;) {
				const page = await storage.list({ prefix: ALLOCATION_LEDGER_PREFIX, limit: MAX_KEYS_PER_WRITE });
				if (page.size === 0) break;
				await storage.delete([...page.keys()]);
			}
		});
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
	 * - POST /forget - Drop the recorded allocation for a primary key
	 * - POST /sequence - Take the next value of a named sequence
	 * - GET /allocations - Page through recorded allocations (`?after=<key>&limit=<n>`, since 1.4.0)
	 * - POST /allocations/prune - Drop the named allocations, at most 128 keys (since 1.4.0)
	 * - POST /retire-legacy - Delete the legacy storage keys (since 1.4.0)
	 * - POST /flush - Clear all coordinator state (development only)
	 * - GET /health - Health check endpoint
	 *
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
			await this.ready;
			switch (`${method} ${path}`) {
				case 'GET /shards':
					return await this.handleListShards();
				case 'POST /shards':
					return await this.handleAddShard(request);
				case 'DELETE /shards':
					return await this.handleRemoveShard(request);
				case 'GET /stats':
					return await this.handleGetStats();
				case 'POST /stats':
					return await this.handleUpdateStats(request);
				case 'POST /allocate':
					return await this.handleAllocateShard(request);
				case 'POST /forget':
					return await this.handleForgetAllocation(request);
				case 'POST /sequence':
					return await this.handleSequence(request);
				case 'GET /allocations':
					return await this.handleListAllocations(url);
				case 'POST /allocations/prune':
					return await this.handlePruneAllocations(request);
				case 'POST /retire-legacy':
					await this.retireLegacy();
					return new Response(JSON.stringify({ success: true }), { headers: { 'Content-Type': 'application/json' } });
				case 'POST /flush':
					return await this.handleFlush();
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
	 * Retrieves all known shards as a JSON array of all D1 binding names that have been registered
	 * with the coordinator.
	 * @private
	 * @returns Promise resolving to HTTP response with shard list
	 * @example Response body: `["db-east", "db-west", "db-central"]`
	 */
	private async handleListShards(): Promise<Response> {
		return this.exclusive(async () => {
			return new Response(JSON.stringify(this.readSqlState().knownShards), {
				headers: { 'Content-Type': 'application/json' }
			});
		});
	}

	/**
	 * Registers a new shard binding with the coordinator. If the shard
	 * is already known, this operation is idempotent. Initializes statistics
	 * for the new shard.
	 * @private
	 * @param request - HTTP request containing shard binding name in JSON body
	 * @returns Promise resolving to HTTP response indicating success
	 * @throws {Error} If request body is invalid JSON
	 * @example Request body: `{"shard": "db-new-region"}`
	 */
	private async handleAddShard(request: Request): Promise<Response> {
		const { shard } = (await request.json()) as { shard: string };

		// Validate required parameters
		if (!shard || typeof shard !== 'string') {
			return new Response(JSON.stringify({ error: 'Missing or invalid shard parameter' }), {
				status: 400,
				headers: { 'Content-Type': 'application/json' }
			});
		}

		return this.exclusive(async () => {
			const state = this.readSqlState();

			if (!state.knownShards.includes(shard)) {
				state.knownShards.push(shard);
				state.shardStats[shard] = {
					binding: shard,
					count: 0,
					lastUpdated: Date.now()
				};
				await this.commit({ state });
			}

			return new Response(JSON.stringify({ success: true }), {
				headers: { 'Content-Type': 'application/json' }
			});
		});
	}

	/**
	 * Unregisters a shard binding from the coordinator. Removes the shard
	 * from the known shards list and deletes its statistics. Adjusts the round-robin
	 * index if necessary to prevent out-of-bounds access.
	 * @private
	 * @param request - HTTP request containing shard binding name in JSON body
	 * @returns Promise resolving to HTTP response indicating success
	 * @throws {Error} If request body is invalid JSON
	 * @example Request body: `{"shard": "db-old-region"}`
	 */
	private async handleRemoveShard(request: Request): Promise<Response> {
		const { shard } = (await request.json()) as { shard: string };

		// Validate required parameters
		if (!shard || typeof shard !== 'string') {
			return new Response(JSON.stringify({ error: 'Missing or invalid shard parameter' }), {
				status: 400,
				headers: { 'Content-Type': 'application/json' }
			});
		}

		return this.exclusive(async () => {
			const state = this.readSqlState();

			const index = state.knownShards.indexOf(shard);
			if (index > -1) {
				state.knownShards.splice(index, 1);
				delete state.shardStats[shard];
				// Adjust round-robin index if necessary
				if (state.roundRobinIndex >= state.knownShards.length) {
					state.roundRobinIndex = 0;
				}
				await this.commit({ state });
			}

			return new Response(JSON.stringify({ success: true }), {
				headers: { 'Content-Type': 'application/json' }
			});
		});
	}

	/**
	 * Returns an array of statistics for all known shards, including
	 * binding names, key counts, and last updated timestamps.
	 * @private
	 * @returns Promise resolving to HTTP response with statistics array
	 * @example Response body: `[{"binding": "db-east", "count": 1234, "lastUpdated": 1672531200000}]`
	 */
	private async handleGetStats(): Promise<Response> {
		return this.exclusive(async () => {
			const stats = Object.values(this.readSqlState().shardStats);
			return new Response(JSON.stringify(stats), {
				headers: { 'Content-Type': 'application/json' }
			});
		});
	}

	/**
	 * Updates the key count and last updated timestamp for a specific shard.
	 * Used by other parts of the system to report changes in shard utilization.
	 * @private
	 * @param request - HTTP request containing shard name and count in JSON body
	 * @returns Promise resolving to HTTP response indicating success
	 * @throws {Error} If request body is invalid JSON or shard doesn't exist
	 * @example Request body: `{"shard": "db-east", "count": 1500}`
	 */
	private async handleUpdateStats(request: Request): Promise<Response> {
		const { shard, count } = (await request.json()) as { shard: string; count: number };

		// Validate required parameters
		if (!shard || typeof shard !== 'string') {
			return new Response(JSON.stringify({ error: 'Missing or invalid shard parameter' }), {
				status: 400,
				headers: { 'Content-Type': 'application/json' }
			});
		}

		if (count === undefined || typeof count !== 'number') {
			return new Response(JSON.stringify({ error: 'Missing or invalid count parameter' }), {
				status: 400,
				headers: { 'Content-Type': 'application/json' }
			});
		}

		return this.exclusive(async () => {
			const state = this.readSqlState();

			if (state.shardStats[shard]) {
				state.shardStats[shard].count = count;
				state.shardStats[shard].lastUpdated = Date.now();
				await this.commit({ state });
			}

			return new Response(JSON.stringify({ success: true }), {
				headers: { 'Content-Type': 'application/json' }
			});
		});
	}

	/**
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
		const { primaryKey, strategy, operationType, availableShards } = (await request.json()) as {
			primaryKey: string;
			strategy?: ShardingStrategy;
			operationType?: OperationType;
			availableShards?: string[];
		};

		if (!primaryKey || typeof primaryKey !== 'string') {
			return new Response(JSON.stringify({ error: 'Missing or invalid primaryKey parameter' }), {
				status: 400,
				headers: { 'Content-Type': 'application/json' }
			});
		}

		return this.exclusive(async () => {
			const state = this.readSqlState();
			const eligibleShards = availableShards || state.knownShards;

			if (eligibleShards.length === 0) {
				return new Response(JSON.stringify({ error: 'No shards available' }), {
					status: 400,
					headers: { 'Content-Type': 'application/json' }
				});
			}

			// Recording the decision makes allocation idempotent per key. Without it,
			// two concurrent first-touches of the same key under round-robin or random
			// receive different shards, both write the mapping to KV, and the row
			// written by the loser of that race sits on a shard the surviving mapping
			// does not name. The ledger is advisory: the KV mapping stays the record.
			const recorded = await this.readAllocation(primaryKey);
			if (typeof recorded === 'string' && eligibleShards.includes(recorded)) {
				return new Response(JSON.stringify({ shard: recorded, reused: true }), {
					headers: { 'Content-Type': 'application/json' }
				});
			}

			const effectiveStrategy = this.resolveStrategy(state.strategy, strategy, operationType || 'write');
			const selectedShard = this.selectShard(primaryKey, state, effectiveStrategy, eligibleShards);

			const ledger: LedgerOp = { put: [[primaryKey, selectedShard]] };
			if (effectiveStrategy === 'round-robin') {
				state.roundRobinIndex = (state.roundRobinIndex + 1) % eligibleShards.length;
				await this.commit({ state, ledger });
			} else {
				await this.commit({ ledger });
			}

			return new Response(JSON.stringify({ shard: selectedShard }), {
				headers: { 'Content-Type': 'application/json' }
			});
		});
	}

	/**
	 * Forgets the recorded allocation for a primary key.
	 *
	 * Called when a key is reassigned or its mapping deleted, so a later
	 * first-touch allocates afresh instead of being handed the stale decision.
	 *
	 * @param primaryKey - The key whose recorded allocation should be dropped
	 * @since 1.3.0
	 */
	async forgetAllocation(primaryKey: string): Promise<void> {
		await this.exclusive(async () => {
			if ((await this.readAllocation(primaryKey)) === undefined) return;
			await this.commit({ ledger: { del: [primaryKey] } });
		});
	}

	/**
	 * Drops a recorded allocation so the key is allocated afresh next time.
	 *
	 * @private
	 * @param request - HTTP request containing the primary key
	 * @returns Promise resolving to HTTP response
	 * @example Request body: `{"primaryKey": "user-123"}`
	 */
	private async handleForgetAllocation(request: Request): Promise<Response> {
		const { primaryKey } = (await request.json()) as { primaryKey?: string };

		if (!primaryKey || typeof primaryKey !== 'string') {
			return new Response(JSON.stringify({ error: 'Missing or invalid primaryKey parameter' }), {
				status: 400,
				headers: { 'Content-Type': 'application/json' }
			});
		}

		await this.forgetAllocation(primaryKey);

		return new Response(JSON.stringify({ success: true }), {
			headers: { 'Content-Type': 'application/json' }
		});
	}

	/**
	 * Atomically returns the next value for a named monotonic sequence.
	 *
	 * Because a Durable Object serializes all requests to a single instance,
	 * the read-increment-write cycle here is race-free across concurrent
	 * callers and isolates. Callers may pass a `min` floor (typically derived
	 * from the current cross-shard `MAX(id)`) so a freshly created coordinator
	 * still respects rows that already exist in the shards; the returned value
	 * is `max(storedCounter + 1, min)`.
	 *
	 * @private
	 * @param request - HTTP request containing the sequence name and optional floor
	 * @returns Promise resolving to HTTP response with the allocated value
	 * @example Request body: `{"name": "tickets", "min": 42}`
	 * @example Response body: `{"value": 42}`
	 */
	private async handleSequence(request: Request): Promise<Response> {
		const { name, min, requireExisting } = (await request.json()) as { name: string; min?: number; requireExisting?: boolean };

		if (!name || typeof name !== 'string') {
			return new Response(JSON.stringify({ error: 'Missing or invalid name parameter' }), {
				status: 400,
				headers: { 'Content-Type': 'application/json' }
			});
		}

		return this.exclusive(async () => {
			const state = this.readSqlState();
			const sequences = state.sequences ?? {};

			// A caller that has not paid for a cross-shard maximum asks with
			// `requireExisting` so it only gets an answer when the sequence is already
			// past every existing row. Answering with 1 here would hand out an id that
			// collides with rows the shards already hold.
			if (requireExisting && sequences[name] === undefined) {
				return new Response(JSON.stringify({ needsSeed: true }), {
					headers: { 'Content-Type': 'application/json' }
				});
			}

			const current = sequences[name] ?? 0;
			const floor = typeof min === 'number' && Number.isFinite(min) ? Math.floor(min) : 0;
			const next = Math.max(current + 1, floor);

			sequences[name] = next;
			state.sequences = sequences;
			await this.commit({ state });

			return new Response(JSON.stringify({ value: next }), {
				headers: { 'Content-Type': 'application/json' }
			});
		});
	}

	/**
	 * Lists recorded allocations in key order, one page at a time.
	 *
	 * Finishes any pending legacy copy first so the listing is complete. The `next`
	 * field is the last key of a full page, or null on the final page.
	 *
	 * @private
	 * @param url - Request URL carrying the optional `after` and `limit` query parameters
	 * @returns Promise resolving to HTTP response with the page
	 * @example Response body: `{"allocations": [{"primaryKey": "user-1", "shard": "db-east", "createdAt": 1672531200000}], "next": null}`
	 */
	private async handleListAllocations(url: URL): Promise<Response> {
		const after = url.searchParams.get('after') ?? '';
		const requested = Number.parseInt(url.searchParams.get('limit') ?? '', 10);
		const limit = Number.isFinite(requested) ? Math.min(Math.max(requested, 1), 1000) : 100;

		return this.exclusive(async () => {
			await this.completeUpgrade();
			const rows = this.state.storage.sql
				.exec<{
					primary_key: string;
					shard: string;
					created_at: number;
				}>('SELECT primary_key, shard, created_at FROM allocations WHERE primary_key > ? ORDER BY primary_key LIMIT ?', after, limit + 1)
				.toArray();
			const page = rows.slice(0, limit);
			const body = {
				allocations: page.map((row) => ({ primaryKey: row.primary_key, shard: row.shard, createdAt: row.created_at })),
				next: rows.length > limit ? page[page.length - 1]!.primary_key : null
			};
			return new Response(JSON.stringify(body), { headers: { 'Content-Type': 'application/json' } });
		});
	}

	/**
	 * Drops the named allocations. The caller has checked each key against its mapping,
	 * and nothing here prunes on its own.
	 *
	 * @private
	 * @param request - HTTP request containing up to 128 primary keys
	 * @returns Promise resolving to HTTP response with the number of rows removed
	 * @example Request body: `{"primaryKeys": ["user-1", "user-2"]}`
	 * @example Response body: `{"pruned": 2}`
	 */
	private async handlePruneAllocations(request: Request): Promise<Response> {
		const { primaryKeys } = (await request.json()) as { primaryKeys?: unknown };

		if (!Array.isArray(primaryKeys) || primaryKeys.length > MAX_KEYS_PER_WRITE || primaryKeys.some((key) => typeof key !== 'string')) {
			return new Response(JSON.stringify({ error: `primaryKeys must be an array of at most ${MAX_KEYS_PER_WRITE} strings` }), {
				status: 400,
				headers: { 'Content-Type': 'application/json' }
			});
		}

		return this.exclusive(async () => {
			await this.completeUpgrade();
			const pruned = primaryKeys.length === 0 ? 0 : await this.commit({ ledger: { del: primaryKeys as string[] } });
			return new Response(JSON.stringify({ pruned }), { headers: { 'Content-Type': 'application/json' } });
		});
	}

	/**
	 * Completely clears all coordinator state from Durable Object storage.
	 * This removes all shard registrations, statistics, and configuration.
	 *
	 * **WARNING**: This operation is destructive and should only be used
	 * in development environments or during testing.
	 *
	 * @private
	 * @returns Promise resolving to HTTP response indicating success
	 * @example Response body: `{"success": true}`
	 */
	private async handleFlush(): Promise<Response> {
		return this.exclusive(async () => {
			await this.state.storage.deleteAll();
			// deleteAll may or may not drop the tables, so create them and empty them either way
			this.createSchema();
			for (const table of ['shards', 'stats', 'allocations', 'sequences', 'meta']) {
				this.state.storage.sql.exec(`DELETE FROM ${table}`);
			}
			this.createSchema();
			return new Response(JSON.stringify({ success: true }), {
				headers: { 'Content-Type': 'application/json' }
			});
		});
	}

	/**
	 * Resolves the effective sharding strategy for a given operation type
	 *
	 * @private
	 * @param configStrategy - The strategy from state configuration
	 * @param requestStrategy - Optional strategy override from request
	 * @param operationType - The type of operation (read/write)
	 * @returns The effective sharding strategy to use
	 */
	private resolveStrategy(
		configStrategy: ShardingStrategy | MixedShardingStrategy,
		requestStrategy?: ShardingStrategy,
		operationType: OperationType = 'write'
	): ShardingStrategy {
		// Request strategy overrides everything
		if (requestStrategy) {
			return requestStrategy;
		}

		// If config strategy is a string, use it for all operations
		if (typeof configStrategy === 'string') {
			return configStrategy;
		}

		// If config strategy is a mixed strategy object, use the appropriate strategy for the operation type
		return configStrategy[operationType];
	}

	/**
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
	 * @param eligibleShards - Optional filtered list of shards to choose from
	 * @returns The selected shard binding name
	 * @throws {CollegeDBError} If no shards are available
	 * @example
	 * ```typescript
	 * const shard = this.selectShard('user-123', state, 'hash');
	 * // Returns: "db-west" (consistent for this key)
	 * ```
	 */
	private selectShard(primaryKey: string, state: ShardCoordinatorState, strategy: ShardingStrategy, eligibleShards?: string[]): string {
		const shards = eligibleShards || state.knownShards;

		if (shards.length === 0) {
			throw new CollegeDBError('No shards available', 'NO_SHARDS');
		}

		switch (strategy) {
			case 'round-robin':
				return shards[state.roundRobinIndex % shards.length] ?? shards[0]!;
			case 'random':
				return shards[Math.floor(Math.random() * shards.length)]!;
			case 'hash': {
				return hrwShard(primaryKey, shards);
			}
			case 'location': {
				// If location config missing, fallback to hash
				const region = state.targetRegion;
				const locations = state.shardLocations || {};
				const located = shards.filter((s) => locations[s]);
				if (!region || located.length === 0) {
					return hrwShard(primaryKey, shards);
				}

				// Simple location scoring similar to router.ts
				const coords: Record<D1Region, { lat: number; lon: number }> = {
					// Western North America - San Francisco, CA
					wnam: { lat: 37.7749, lon: -122.4194 },
					// Eastern North America - Newark, NJ
					enam: { lat: 40.7357, lon: -74.1724 },
					// Western Europe - London, UK
					weur: { lat: 51.5074, lon: -0.1278 },
					// Eastern Europe - Warsaw, Poland
					eeur: { lat: 52.2297, lon: 21.0122 },
					// Asia Pacific - Tokyo, Japan
					apac: { lat: 35.6762, lon: 139.6503 },
					// Oceania - Sydney, Australia
					oc: { lat: -33.8688, lon: 151.2093 },
					// Middle East - Dubai, UAE
					me: { lat: 25.2048, lon: 55.2708 },
					// Africa - Johannesburg, South Africa
					af: { lat: -26.2041, lon: 28.0473 }
				};

				const hasCoordKey = (obj: typeof coords, key: D1Region) => key in obj;
				const getKey = (k: D1Region): keyof typeof coords => (hasCoordKey(coords, k) ? (k as keyof typeof coords) : 'wnam');
				const dist = (from: D1Region, to: D1Region) => {
					const a = coords[getKey(from)];
					const b = coords[getKey(to)];
					const lat = a.lat - b.lat;
					const lon = a.lon - b.lon;
					return Math.sqrt(lat * lat + lon * lon);
				};

				const scores = located.map((shard) => {
					const meta = locations[shard]!;
					const distance = dist(region, meta.region);
					const priority = meta.priority || 1;
					return { shard, score: distance - priority * 0.1 };
				});

				scores.sort((a, b) => a.score - b.score);
				const bestScore = scores[0]!.score;
				const best = scores.filter((s) => Math.abs(s.score - bestScore) < 0.01);
				if (best.length === 1) return best[0]!.shard;

				// Tie-breaker by consistent hash
				return hrwShard(
					primaryKey,
					best.map((candidate) => candidate.shard)
				);
			}

			default:
				return hrwShard(primaryKey, shards);
		}
	}

	/**
	 * Atomically increments the key count for a specific shard and updates
	 * the last modified timestamp. Used when new primary keys are assigned
	 * to a shard.
	 * @param shard - The shard binding name to increment
	 * @returns Promise that resolves when the count is updated
	 * @throws {Error} If the shard is not known to the coordinator
	 * @example
	 * ```typescript
	 * await coordinator.incrementShardCount('db-east');
	 * ```
	 */
	async incrementShardCount(shard: string): Promise<void> {
		await this.exclusive(async () => {
			const state = this.readSqlState();
			if (state.shardStats[shard]) {
				state.shardStats[shard].count++;
				state.shardStats[shard].lastUpdated = Date.now();
				await this.commit({ state });
			}
		});
	}

	/**
	 * Atomically decrements the key count for a specific shard and updates
	 * the last modified timestamp. Used when primary keys are removed or
	 * moved from a shard. Prevents negative counts.
	 * @param shard - The shard binding name to decrement
	 * @returns Promise that resolves when the count is updated
	 * @throws {CollegeDBError} If the shard is not known to the coordinator
	 * @example
	 * ```typescript
	 * await coordinator.decrementShardCount('db-west');
	 * ```
	 */
	async decrementShardCount(shard: string): Promise<void> {
		await this.exclusive(async () => {
			const state = this.readSqlState();
			if (state.shardStats[shard] && state.shardStats[shard].count > 0) {
				state.shardStats[shard].count--;
				state.shardStats[shard].lastUpdated = Date.now();
				await this.commit({ state });
			}
		});
	}
}
