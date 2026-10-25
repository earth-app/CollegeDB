import type { DatabaseSync } from 'node:sqlite';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
	CollegeDBError,
	KVShardMapper,
	countAllShards,
	createInMemoryKVProvider,
	deleteRow,
	first,
	getMigrationStatus,
	hrwShard,
	initialize,
	initializeAsync,
	insertInto,
	invalidateMappingCache,
	migrateRecord,
	purgeRetired,
	reassignShard,
	rebalance,
	recoverMigrations,
	resetConfig,
	updateRow,
	upsert,
	verifyMigrations
} from '../src/index';
import { ensureIndex, ensureJournalSchema, runGuardedWrite, setMigrationStepHook } from '../src/migrations';
import { addPlacementException, createManifest, resetPlacementState } from '../src/placement';
import { HISTORY_INDEX, buildJournalSchema } from '../src/query';
import { raiseFence, readFence } from '../src/topology';
import type { BatchStatement, KVStorage, SQLDatabase, SQLDialect } from '../src/types';
import { restoreClock, setClock } from './helpers/clock';
import {
	createNodeSqliteClient,
	createNodeSqliteProvider,
	openNodeSqlite,
	runCutoverRace,
	type RaceKind
} from './helpers/concurrent-sqlite';
import { FaultPlan, createStepWriters } from './helpers/fault-plan';

const ITEMS = 'CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, qty INTEGER, note TEXT, data BLOB)';
const SOURCE = 'db-a';
const TARGET = 'db-b';
const JOURNAL = 'db-j';

interface World {
	kv: KVStorage;
	raw: Record<string, DatabaseSync>;
	shards: Record<string, SQLDatabase>;
	plan: FaultPlan;
	failPuts: { on: boolean };
}

function makeKV(failPuts: { on: boolean }): KVStorage {
	const kv = createInMemoryKVProvider();
	return {
		get: ((key: string, type?: 'text' | 'json') => kv.get(key, type as 'json')) as KVStorage['get'],
		put: async (key: string, value: string) => {
			if (failPuts.on) {
				throw new Error('kv is down');
			}
			await kv.put(key, value);
		},
		delete: (key: string) => kv.delete(key),
		list: (options) => kv.list(options)
	};
}

function makeWorld(names: string[] = [SOURCE, TARGET, JOURNAL], plans: Record<string, FaultPlan> = {}): World {
	const plan = new FaultPlan();
	const failPuts = { on: false };
	const raw: Record<string, DatabaseSync> = {};
	const shards: Record<string, SQLDatabase> = {};
	for (const name of names) {
		raw[name] = openNodeSqlite();
		raw[name].exec(ITEMS);
		shards[name] = (plans[name] ?? plan).wrapSQL(createNodeSqliteProvider(raw[name]), name);
	}
	return { kv: plan.wrapKV(makeKV(failPuts)), raw, shards, plan, failPuts };
}

function configOf(world: World, extra: Record<string, unknown> = {}) {
	return {
		kv: world.kv,
		shards: world.shards,
		strategy: 'hash' as const,
		journalShard: JOURNAL,
		disableAutoMigration: true,
		hashShardMappings: false,
		...extra
	};
}

/** Starts a fresh process over the world's databases. */
async function start(world: World, extra: Record<string, unknown> = {}): Promise<void> {
	resetConfig();
	resetPlacementState();
	await initializeAsync(configOf(world, extra));
	world.plan.reset();
}

async function mappingOf(kv: KVStorage, key: string): Promise<string | undefined> {
	return (await new KVShardMapper(kv, { hashShardMappings: false }).getShardMapping(key))?.shard;
}

function rowsOn(world: World, shard: string, key: string): Array<Record<string, unknown>> {
	return world.raw[shard]!.prepare('SELECT * FROM items WHERE id = ?').all(key) as Array<Record<string, unknown>>;
}

function tombstones(world: World, shard: string, key: string): Array<Record<string, any>> {
	return world.raw[shard]!.prepare('SELECT * FROM collegedb_retired WHERE primary_key = ?').all(key) as Array<Record<string, any>>;
}

function activeClaims(world: World): number {
	return Number((world.raw[JOURNAL]!.prepare('SELECT COUNT(*) AS n FROM collegedb_migration_active').get() as { n: number }).n);
}

function statesOf(world: World, key: string): string[] {
	const rows = world.raw[JOURNAL]!.prepare('SELECT state, attempt FROM collegedb_migration_history WHERE primary_key = ?').all(
		key
	) as Array<{
		state: string;
		attempt: number;
	}>;
	return rows.map((row) => `${row.state}:${row.attempt}`);
}

/** Puts a row on a shard and maps the key there, bypassing the router. */
async function seed(world: World, key: string, row: Record<string, unknown> = {}, shard = SOURCE): Promise<void> {
	const values = { id: key, name: `name-${key}`, qty: 1, note: null, data: null, ...row };
	world.raw[shard]!.prepare('INSERT INTO items (id, name, qty, note, data) VALUES (?, ?, ?, ?, ?)').run(
		values.id as string,
		values.name as string,
		values.qty as number,
		values.note as string | null,
		values.data as Uint8Array | null
	);
	await new KVShardMapper(world.kv, { hashShardMappings: false }).setShardMapping(key, shard);
}

/** Moves the clock past the time recovery leaves a claim with no history alone. */
function pastOrphanGrace(): void {
	setClock(Date.now() + 61_000);
}

async function codeOf(work: Promise<unknown>): Promise<string | undefined> {
	try {
		await work;
		return undefined;
	} catch (error) {
		return error instanceof CollegeDBError ? error.code : `raw:${error instanceof Error ? error.message : String(error)}`;
	}
}

afterEach(() => {
	setMigrationStepHook(null);
	restoreClock();
	resetConfig();
});

// #region audit findings

describe('Audit findings D3, D4, D5 and D11', () => {
	let world: World;

	beforeEach(() => {
		resetConfig();
		resetPlacementState();
		world = makeWorld();
	});

	it('D3: a move interrupted before the mapping update leaves the key readable', async () => {
		await start(world);
		await seed(world, 'k1', { name: 'one' });

		world.failPuts.on = true;
		await expect(reassignShard('k1', TARGET, 'items')).rejects.toThrow();
		world.failPuts.on = false;
		await invalidateMappingCache();

		expect(await first<{ name: string }>('k1', 'SELECT * FROM items WHERE id = ?', ['k1'])).toMatchObject({ name: 'one' });
	});

	it('D4: concurrent exception records lose nothing', async () => {
		const kv = createInMemoryKVProvider();
		const manifest = createManifest(['a', 'b'], 'hrw');

		await Promise.all(Array.from({ length: 12 }, (_, i) => addPlacementException(kv, manifest, `hashed-${i}`)));

		const stored = (await kv.get<string[]>('collegedb:placement:exceptions', 'json')) ?? [];
		expect(new Set(stored).size).toBe(12);
	});

	it('D4: concurrent exception records through the journal lose nothing in the table or the KV copy', async () => {
		await start(world);
		const kv = createInMemoryKVProvider();
		const manifest = createManifest(['a', 'b'], 'hrw');
		const journal = { db: world.shards[JOURNAL]!, scope: 'default' };

		await Promise.all(Array.from({ length: 12 }, (_, i) => addPlacementException(kv, manifest, `hashed-${i}`, journal)));

		const rows = world.raw[JOURNAL]!.prepare('SELECT hashed_key FROM collegedb_placement_exceptions').all();
		const stored = (await kv.get<string[]>('collegedb:placement:exceptions', 'json')) ?? [];
		expect(rows).toHaveLength(12);
		expect(new Set(stored).size).toBe(12);
	});

	it('D4: a move onto the computed owner records no exception', async () => {
		await start(world, { placement: 'computed' });

		const owner = hrwShard('k2', [SOURCE, TARGET, JOURNAL]);
		const other = owner === SOURCE ? TARGET : SOURCE;
		await seed(world, 'k2', { name: 'two' }, other);

		await reassignShard('k2', owner, 'items');

		const exceptions = (await world.kv.get<string[]>('collegedb:placement:exceptions', 'json')) ?? [];
		expect(exceptions).not.toContain('k2');
		expect(rowsOn(world, owner, 'k2')).toHaveLength(1);
		expect(rowsOn(world, other, 'k2')).toHaveLength(0);
	});

	it('D4: a move off the computed owner records the exception through the journal', async () => {
		await start(world, { placement: 'computed' });

		const owner = hrwShard('k2', [SOURCE, TARGET, JOURNAL]);
		const target = [SOURCE, TARGET].find((name) => name !== owner)!;
		await seed(world, 'k2', { name: 'two' }, owner);

		await reassignShard('k2', target, 'items');

		expect(world.raw[JOURNAL]!.prepare('SELECT hashed_key FROM collegedb_placement_exceptions').all()).toEqual([{ hashed_key: 'k2' }]);
		expect(await first<{ name: string }>('k2', 'SELECT * FROM items WHERE id = ?', ['k2'])).toMatchObject({ name: 'two' });
	});

	it('D5: migrateRecord honors a primary key column other than id', async () => {
		const source = createNodeSqliteProvider(openNodeSqlite());
		const target = createNodeSqliteProvider(openNodeSqlite());
		for (const db of [source, target]) {
			await db.prepare('CREATE TABLE orders (order_id TEXT PRIMARY KEY, total INTEGER)').run();
		}
		await source.prepare('INSERT INTO orders (order_id, total) VALUES (?, ?)').bind('o1', 5).run();

		await migrateRecord(source, target, 'o1', 'orders', { pkColumn: 'order_id' });

		expect(await target.prepare('SELECT total FROM orders WHERE order_id = ?').bind('o1').first()).toMatchObject({ total: 5 });
		expect(await source.prepare('SELECT * FROM orders WHERE order_id = ?').bind('o1').first()).toBeNull();
	});

	it('D5: migrateRecord does not overwrite a different destination row by default', async () => {
		const source = createNodeSqliteProvider(openNodeSqlite());
		const target = createNodeSqliteProvider(openNodeSqlite());
		for (const db of [source, target]) {
			await db.prepare(ITEMS).run();
		}
		await source.prepare('INSERT INTO items (id, name) VALUES (?, ?)').bind('x', 'from source').run();
		await target.prepare('INSERT INTO items (id, name) VALUES (?, ?)').bind('x', 'already here').run();

		await expect(migrateRecord(source, target, 'x', 'items')).rejects.toMatchObject({ code: 'DESTINATION_CONFLICT' });
		expect(await target.prepare('SELECT name FROM items WHERE id = ?').bind('x').first()).toMatchObject({ name: 'already here' });
		expect(await source.prepare('SELECT name FROM items WHERE id = ?').bind('x').first()).toMatchObject({ name: 'from source' });
	});

	it('D5: migrateRecord replaces a different destination row on request and accepts an equal one', async () => {
		const source = createNodeSqliteProvider(openNodeSqlite());
		const target = createNodeSqliteProvider(openNodeSqlite());
		for (const db of [source, target]) {
			await db.prepare(ITEMS).run();
		}
		await source.prepare('INSERT INTO items (id, name) VALUES (?, ?)').bind('x', 'from source').run();
		await target.prepare('INSERT INTO items (id, name) VALUES (?, ?)').bind('x', 'already here').run();
		await migrateRecord(source, target, 'x', 'items', { onConflict: 'replace' });
		expect(await target.prepare('SELECT name FROM items WHERE id = ?').bind('x').first()).toMatchObject({ name: 'from source' });

		await source.prepare('INSERT INTO items (id, name) VALUES (?, ?)').bind('y', 'same').run();
		await target.prepare('INSERT INTO items (id, name) VALUES (?, ?)').bind('y', 'same').run();
		await migrateRecord(source, target, 'y', 'items');
		expect(await source.prepare('SELECT * FROM items WHERE id = ?').bind('y').first()).toBeNull();
	});

	it('D11: a stale writer cannot recreate a row on the shard a key left', async () => {
		await start(world);
		await seed(world, 'k3', { name: 'three' });
		await reassignShard('k3', TARGET, 'items');

		// a caller whose KV view still says the old shard
		await new KVShardMapper(world.kv, { hashShardMappings: false }).updateShardMapping('k3', SOURCE);
		await invalidateMappingCache('k3');

		expect(await codeOf(insertInto('k3', 'items', { id: 'k3', name: 'ghost' }))).toBe('MIGRATION_CUTOVER_PENDING');
		expect(rowsOn(world, SOURCE, 'k3')).toHaveLength(0);
	});
});

// #endregion

// #region configuration

describe('Journal configuration', () => {
	beforeEach(() => {
		resetConfig();
		resetPlacementState();
	});

	it('refuses moves, rebalance and recovery on several shards without a journalShard', async () => {
		const world = makeWorld();
		await start(world, { journalShard: undefined });
		await seed(world, 'k', {});

		expect(await codeOf(reassignShard('k', TARGET, 'items'))).toBe('JOURNAL_NOT_CONFIGURED');
		expect(await codeOf(rebalance('items'))).toBe('JOURNAL_NOT_CONFIGURED');
		expect(await codeOf(recoverMigrations())).toBe('JOURNAL_NOT_CONFIGURED');
		expect(rowsOn(world, SOURCE, 'k')).toHaveLength(1);
	});

	it('lets a dry-run rebalance report without a journal', async () => {
		const world = makeWorld();
		await start(world, { journalShard: undefined });
		await expect(rebalance('items', { dryRun: true })).resolves.toMatchObject({ moved: 0 });
	});

	it('defaults the journal to the only shard of a single-shard config', async () => {
		const world = makeWorld([SOURCE]);
		await start(world, { journalShard: undefined });

		await expect(recoverMigrations()).resolves.toMatchObject({ examined: 0 });
		expect(await getMigrationStatus('nothing')).toBeNull();
	});

	it('rejects a journalShard that is not a configured shard', () => {
		const world = makeWorld();
		expect(() => initialize(configOf(world, { journalShard: 'db-missing' }))).toThrow(/journalShard db-missing/);
		expect(() => initialize(configOf(world, { journalShard: 'db-missing' }))).toThrowError(
			expect.objectContaining({ code: 'INVALID_JOURNAL_SHARD' })
		);
	});

	it('refuses a move that involves a dialect the journal is not proven on', async () => {
		const world = makeWorld();
		world.shards[TARGET] = { ...world.shards[TARGET]!, dialect: 'oracle' as SQLDialect };
		await start(world);
		await seed(world, 'k', {});

		expect(await codeOf(reassignShard('k', TARGET, 'items'))).toBe('MIGRATION_UNSUPPORTED_DIALECT');
		expect(rowsOn(world, SOURCE, 'k')).toHaveLength(1);
		expect(activeClaims(world)).toBe(0);
	});

	it('refuses a move whose source cannot run a transactional batch', async () => {
		const world = makeWorld();
		const { runBatch: _drop, ...noBatch } = world.shards[SOURCE]!;
		world.shards[SOURCE] = noBatch;
		await start(world);
		await seed(world, 'k', {});

		expect(await codeOf(reassignShard('k', TARGET, 'items'))).toBe('MIGRATION_NO_BATCH');
		expect(rowsOn(world, SOURCE, 'k')).toHaveLength(1);
	});

	it('refuses a move whose source runs a batch it does not declare atomic, before anything is claimed or copied', async () => {
		const world = makeWorld();
		const { atomicBatch: _drop, ...undeclared } = world.shards[SOURCE]!;
		world.shards[SOURCE] = undeclared;
		await start(world);
		await seed(world, 'k', {});

		expect(await codeOf(reassignShard('k', TARGET, 'items'))).toBe('MIGRATION_NO_ATOMIC_BATCH');
		expect(rowsOn(world, SOURCE, 'k')).toHaveLength(1);
		expect(rowsOn(world, TARGET, 'k')).toHaveLength(0);
		expect(activeClaims(world)).toBe(0);
	});

	it('does not ask the destination or the journal for an atomic batch', async () => {
		const world = makeWorld();
		for (const name of [TARGET, JOURNAL]) {
			const { atomicBatch: _drop, runBatch: _batch, ...plain } = world.shards[name]!;
			world.shards[name] = plain;
		}
		await start(world);
		await seed(world, 'k', {});

		await reassignShard('k', TARGET, 'items');
		expect(rowsOn(world, TARGET, 'k')).toHaveLength(1);
	});

	it('names the shard and the missing declaration in the message', async () => {
		const world = makeWorld();
		world.shards[SOURCE] = { ...world.shards[SOURCE]!, atomicBatch: false };
		await start(world);
		await seed(world, 'k', {});

		const error = await reassignShard('k', TARGET, 'items').catch((caught) => caught);
		expect(error).toBeInstanceOf(CollegeDBError);
		expect(error.code).toBe('MIGRATION_NO_ATOMIC_BATCH');
		expect(error.message).toContain(SOURCE);
		expect(error.message).toContain('atomicBatch');
	});

	it('reports an unresumable operation from recoverMigrations when its source lost the declaration', async () => {
		const world = makeWorld();
		await seed(world, 'k', {});
		setMigrationStepHook((step) => {
			if (step === 'copied') {
				throw new Error('crash after the copy');
			}
		});
		await start(world);
		await reassignShard('k', TARGET, 'items').catch(() => undefined);
		setMigrationStepHook(null);

		const { atomicBatch: _drop, ...undeclared } = world.shards[SOURCE]!;
		world.shards[SOURCE] = undeclared;
		await start(world);
		const report = await recoverMigrations();

		expect(report.failed.map((entry) => entry.code)).toEqual(['MIGRATION_NO_ATOMIC_BATCH']);
		expect(rowsOn(world, SOURCE, 'k')).toHaveLength(1);
	});
});

// #endregion

// #region per-dialect key lock

const LOCK_SQL = /pg_advisory_xact_lock|SET TRANSACTION ISOLATION|collegedb_lock_stripes/;

/** A SQLite shard that claims another dialect, logs every batch and skips the lock statements SQLite cannot run */
function claimingDialect(db: SQLDatabase, dialect: SQLDialect, log: string[][]): SQLDatabase {
	return {
		...db,
		dialect,
		runBatch: async (statements: BatchStatement[]) => {
			log.push(statements.map((statement) => statement.sql));
			const locks = statements.filter((statement) => LOCK_SQL.test(statement.sql));
			const results = await db.runBatch!(statements.filter((statement) => !LOCK_SQL.test(statement.sql)));
			return [...locks.map(() => results[0]!), ...results];
		}
	} as SQLDatabase;
}

describe('The per-key lock', () => {
	const guard = { scope: 'default', table: 'items', key: 'k' };
	const update = { sql: 'UPDATE items SET name = ? WHERE id = ?', bindings: ['changed', 'k'] };

	beforeEach(() => {
		resetConfig();
		resetPlacementState();
	});

	it('runs the Postgres lock and the write as one batch and returns the write result', async () => {
		const world = makeWorld();
		await seed(world, 'k', {});
		const log: string[][] = [];
		const db = claimingDialect(world.shards[SOURCE]!, 'postgres', log);

		const result = await runGuardedWrite(db, guard, update);

		expect(result.meta.changes).toBe(1);
		expect(log).toHaveLength(1);
		expect(log[0]).toEqual([
			'SET TRANSACTION ISOLATION LEVEL READ COMMITTED',
			'SELECT pg_advisory_xact_lock(CAST(? AS bigint))',
			'SELECT pg_advisory_xact_lock_shared(CAST(? AS bigint))',
			update.sql
		]);
		expect(rowsOn(world, SOURCE, 'k')[0]!.name).toBe('changed');
	});

	it('runs the MySQL stripe lock ahead of the write in the same batch', async () => {
		const world = makeWorld();
		await seed(world, 'k', {});
		const log: string[][] = [];

		const result = await runGuardedWrite(claimingDialect(world.shards[SOURCE]!, 'mysql', log), guard, update);

		expect(result.meta.changes).toBe(1);
		expect(log[0]).toEqual([
			'SELECT stripe FROM collegedb_lock_stripes WHERE stripe = ? FOR UPDATE',
			'SELECT stripe FROM collegedb_lock_stripes WHERE stripe = ? LOCK IN SHARE MODE',
			update.sql
		]);
	});

	it('raises the fence on a locking dialect inside one batch that takes the exclusive fence lock first', async () => {
		for (const [dialect, first] of [
			['postgres', 'SET TRANSACTION ISOLATION LEVEL READ COMMITTED'],
			['mysql', 'SELECT stripe FROM collegedb_lock_stripes WHERE stripe = ? FOR UPDATE']
		] as const) {
			const log: string[][] = [];
			const db = {
				dialect,
				atomicBatch: true,
				prepare: () => {
					throw new Error('the raise must run as one batch');
				},
				runBatch: async (statements: BatchStatement[]) => {
					log.push(statements.map((statement) => statement.sql));
					return statements.map(() => ({ success: true, results: [], meta: { duration: 0 } }));
				}
			} as unknown as SQLDatabase;

			await raiseFence(db, 3);

			expect(log).toHaveLength(1);
			expect(log[0]![0]).toBe(first);
			expect(log[0]!.some((sql) => /pg_advisory_xact_lock\(|FOR UPDATE/.test(sql))).toBe(true);
			expect(log[0]!.some((sql) => /_shared|SHARE MODE/.test(sql))).toBe(false);
			expect(log[0]!.slice(-2).map((sql) => sql.split(' ')[0])).toEqual(['INSERT', 'UPDATE']);
		}
	});

	it('raises the fence with plain statements on SQLite', async () => {
		const world = makeWorld();
		const spy = vi.spyOn(world.shards[SOURCE]!, 'runBatch');
		await world.shards[SOURCE]!.prepare('CREATE TABLE IF NOT EXISTS collegedb_topology (min_generation INTEGER NOT NULL)').run();

		await raiseFence(world.shards[SOURCE]!, 2);
		await raiseFence(world.shards[SOURCE]!, 1);

		expect(spy).not.toHaveBeenCalled();
		expect(await readFence(world.shards[SOURCE]!)).toBe(2);
	});

	it('refuses to raise the fence on a locking dialect without an atomic batch, and raises nothing', async () => {
		const world = makeWorld();
		const { atomicBatch: _drop, ...undeclared } = world.shards[SOURCE]!;
		const { runBatch: _batch, ...noBatch } = undeclared;
		await world.shards[SOURCE]!.prepare('CREATE TABLE IF NOT EXISTS collegedb_topology (min_generation INTEGER NOT NULL)').run();

		for (const [shard, code] of [
			[undeclared, 'MIGRATION_NO_ATOMIC_BATCH'],
			[noBatch, 'MIGRATION_NO_BATCH']
		] as const) {
			expect(await codeOf(raiseFence({ ...shard, dialect: 'postgres' } as SQLDatabase, 5))).toBe(code);
		}
		expect(await readFence(world.shards[SOURCE]!)).toBe(0);
	});

	it('runs the plain statement on SQLite, with or without a batch', async () => {
		const world = makeWorld();
		await seed(world, 'k', {});
		const { runBatch: _drop, ...noBatch } = world.shards[SOURCE]!;
		const spy = vi.spyOn(world.shards[SOURCE]!, 'runBatch');

		expect((await runGuardedWrite(world.shards[SOURCE]!, guard, update)).meta.changes).toBe(1);
		expect((await runGuardedWrite(noBatch, guard, { ...update, bindings: ['again', 'k'] })).meta.changes).toBe(1);
		expect(spy).not.toHaveBeenCalled();
		expect(rowsOn(world, SOURCE, 'k')[0]!.name).toBe('again');
	});

	it('refuses a write on a locking dialect whose shard cannot run a batch', async () => {
		const world = makeWorld();
		await seed(world, 'k', {});
		const { runBatch: _drop, ...noBatch } = world.shards[SOURCE]!;

		for (const dialect of ['postgres', 'mysql'] as const) {
			expect(await codeOf(runGuardedWrite({ ...noBatch, dialect } as SQLDatabase, guard, update))).toBe('MIGRATION_NO_BATCH');
		}
		expect(rowsOn(world, SOURCE, 'k')[0]!.name).toBe('name-k');
	});

	it('refuses a write on a locking dialect whose batch is not declared atomic, and runs nothing', async () => {
		const world = makeWorld();
		await seed(world, 'k', {});
		const { atomicBatch: _drop, ...undeclared } = world.shards[SOURCE]!;
		const log: string[][] = [];

		for (const dialect of ['postgres', 'mysql'] as const) {
			const shard = claimingDialect(undeclared as SQLDatabase, dialect, log);
			const error = await runGuardedWrite(shard, guard, update).catch((caught) => caught);
			expect(error).toBeInstanceOf(CollegeDBError);
			expect(error.code).toBe('MIGRATION_NO_ATOMIC_BATCH');
		}
		expect(log).toEqual([]);
		expect(rowsOn(world, SOURCE, 'k')[0]!.name).toBe('name-k');
	});

	it('does not ask SQLite for an atomic batch on a guarded write, which is one statement', async () => {
		const world = makeWorld();
		await seed(world, 'k', {});
		const { atomicBatch: _drop, ...undeclared } = world.shards[SOURCE]!;

		expect((await runGuardedWrite(undeclared as SQLDatabase, guard, update)).meta.changes).toBe(1);
	});

	it('takes the lock first in the cutover transaction and still moves the row', async () => {
		const world = makeWorld();
		const log: string[][] = [];
		world.shards[SOURCE] = claimingDialect(world.shards[SOURCE]!, 'postgres', log);
		await start(world);
		await seed(world, 'k', { name: 'Ada' });

		await reassignShard('k', TARGET, 'items');

		const cutover = log.find((batch) => batch.some((sql) => sql.includes('collegedb_retired')))!;
		expect(cutover.slice(0, 2)).toEqual([
			'SET TRANSACTION ISOLATION LEVEL READ COMMITTED',
			'SELECT pg_advisory_xact_lock(CAST(? AS bigint))'
		]);
		expect(cutover).toHaveLength(4);
		expect(rowsOn(world, TARGET, 'k')[0]!.name).toBe('Ada');
		expect(rowsOn(world, SOURCE, 'k')).toHaveLength(0);
		expect(tombstones(world, SOURCE, 'k')).toHaveLength(1);
	});
});

// #endregion

// #region the move

describe('A journaled move', () => {
	let world: World;

	beforeEach(async () => {
		resetConfig();
		resetPlacementState();
		world = makeWorld();
		await start(world);
	});

	it('walks every state, archives the old row, and leaves the destination as the owner', async () => {
		await seed(world, 'k', { name: 'Ada', qty: 7 });

		await reassignShard('k', TARGET, 'items');

		expect(rowsOn(world, SOURCE, 'k')).toHaveLength(0);
		expect(rowsOn(world, TARGET, 'k')).toMatchObject([{ id: 'k', name: 'Ada', qty: 7 }]);
		expect(tombstones(world, SOURCE, 'k')).toMatchObject([{ moved_to: TARGET, generation: 1, pk_column: 'id' }]);
		expect(tombstones(world, TARGET, 'k')).toHaveLength(0);
		expect(statesOf(world, 'k').sort()).toEqual(
			['prepared:1', 'copied:1', 'verified:1', 'cutover_pending:1', 'committed:1', 'published:1', 'retired:1'].sort()
		);
		expect(activeClaims(world)).toBe(0);
		expect(await mappingOf(world.kv, 'k')).toBe(TARGET);
		expect(await getMigrationStatus('k')).toMatchObject({
			state: 'retired',
			generation: 1,
			source: SOURCE,
			destination: TARGET,
			active: false
		});
		// the archive lives in the sidecar table, so scatter and gather never see two copies
		expect((await countAllShards('items')).total).toBe(1);
		expect(await first<{ name: string }>('k', 'SELECT * FROM items WHERE id = ?', ['k'])).toMatchObject({ name: 'Ada' });
	});

	it('records the retention deadline, seven days by default and now for immediate', async () => {
		setClock(1_000_000);
		await seed(world, 'a', {});
		await seed(world, 'b', {});

		await reassignShard('a', TARGET, 'items');
		await reassignShard('b', TARGET, 'items', { retire: 'immediate' });

		const detail = (key: string) =>
			JSON.parse(
				(
					world.raw[JOURNAL]!.prepare("SELECT detail FROM collegedb_migration_history WHERE primary_key = ? AND state = 'retired'").get(
						key
					) as {
						detail: string;
					}
				).detail
			);
		expect(detail('a').retireAfter).toBe(1_000_000 + 7 * 24 * 60 * 60 * 1000);
		expect(detail('b').retireAfter).toBe(1_000_000);
	});

	it('moves a key back onto a shard that holds its tombstone', async () => {
		await seed(world, 'k', { name: 'Ada' });
		await reassignShard('k', TARGET, 'items');
		await reassignShard('k', SOURCE, 'items');

		expect(rowsOn(world, SOURCE, 'k')).toMatchObject([{ name: 'Ada' }]);
		expect(rowsOn(world, TARGET, 'k')).toHaveLength(0);
		expect(tombstones(world, SOURCE, 'k')).toHaveLength(0);
		expect(tombstones(world, TARGET, 'k')).toMatchObject([{ moved_to: SOURCE, generation: 2 }]);
		expect(await getMigrationStatus('k')).toMatchObject({ generation: 2, destination: SOURCE, state: 'retired' });

		// the old owner is fenced again, the new one accepts writes
		expect(await updateRow('k', 'items', { name: 'Grace' }, { id: 'k' })).toMatchObject({ success: true });
		expect(rowsOn(world, SOURCE, 'k')).toMatchObject([{ name: 'Grace' }]);
	});

	it('lets exactly one of two concurrent moves of one key win', async () => {
		await seed(world, 'k', {});

		const results = await Promise.allSettled([reassignShard('k', TARGET, 'items'), reassignShard('k', TARGET, 'items')]);

		expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1);
		const rejected = results.find((result): result is PromiseRejectedResult => result.status === 'rejected')!;
		expect(['MIGRATION_IN_PROGRESS', 'MIGRATION_SOURCE_TOMBSTONED']).toContain((rejected.reason as CollegeDBError).code);
		expect(rowsOn(world, TARGET, 'k')).toHaveLength(1);
		expect(rowsOn(world, SOURCE, 'k')).toHaveLength(0);
		expect(activeClaims(world)).toBe(0);
	});

	it('lets exactly one of two concurrent moves to different shards win', async () => {
		const three = makeWorld([SOURCE, TARGET, JOURNAL, 'db-c']);
		await start(three);
		await seed(three, 'k', {});

		const results = await Promise.allSettled([reassignShard('k', TARGET, 'items'), reassignShard('k', 'db-c', 'items')]);

		expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1);
		const holders = [SOURCE, TARGET, 'db-c'].filter((shard) => rowsOn(three, shard, 'k').length === 1);
		expect(holders).toHaveLength(1);
		expect(holders[0]).not.toBe(SOURCE);
		expect(await mappingOf(three.kv, 'k')).toBe(holders[0]);
	});

	it('refuses a second move of a key whose move is unfinished', async () => {
		await seed(world, 'k', {});
		world.plan.arm(1, 'before');
		await start(world);
		world.plan.reset();

		setMigrationStepHook(async (step) => {
			if (step === 'copied') {
				expect(await codeOf(reassignShard('k', TARGET, 'items'))).toBe('MIGRATION_IN_PROGRESS');
			}
		});
		await reassignShard('k', TARGET, 'items');
		expect(rowsOn(world, TARGET, 'k')).toHaveLength(1);
	});

	it('refuses a row with a value that has no portable encoding before copying anything', async () => {
		await seed(world, 'odd', {});
		const real = world.shards[SOURCE]!;
		world.shards[SOURCE] = {
			...real,
			prepare: (sql: string) => {
				const statement = real.prepare(sql);
				if (!sql.startsWith('SELECT * FROM "items"')) {
					return statement;
				}
				return {
					...statement,
					bind: (...bindings: unknown[]) => {
						const bound = statement.bind(...bindings);
						return { ...bound, first: async () => ({ ...(await bound.first<object>()), extra: new Date(0) }) };
					}
				} as ReturnType<SQLDatabase['prepare']>;
			}
		};
		await start(world);

		expect(await codeOf(reassignShard('odd', TARGET, 'items'))).toBe('MIGRATION_UNSUPPORTED_VALUE');
		expect(rowsOn(world, TARGET, 'odd')).toHaveLength(0);
		expect(activeClaims(world)).toBe(0);
	});

	it('aborts with RECORD_NOT_FOUND when the source has no row', async () => {
		await new KVShardMapper(world.kv, { hashShardMappings: false }).setShardMapping('ghost', SOURCE);

		expect(await codeOf(reassignShard('ghost', TARGET, 'items'))).toBe('RECORD_NOT_FOUND');
		expect(activeClaims(world)).toBe(0);
		expect(statesOf(world, 'ghost').sort()).toEqual(['aborted:1']);
	});

	it('fails a different destination row by default and leaves both rows alone', async () => {
		await seed(world, 'k', { name: 'source' });
		world.raw[TARGET]!.prepare('INSERT INTO items (id, name) VALUES (?, ?)').run('k', 'foreign');

		expect(await codeOf(reassignShard('k', TARGET, 'items'))).toBe('DESTINATION_CONFLICT');
		expect(rowsOn(world, TARGET, 'k')).toMatchObject([{ name: 'foreign' }]);
		expect(rowsOn(world, SOURCE, 'k')).toMatchObject([{ name: 'source' }]);
		expect(activeClaims(world)).toBe(0);
	});

	it('overwrites a different destination row with onConflict replace and accepts an equal one', async () => {
		await seed(world, 'k', { name: 'source' });
		world.raw[TARGET]!.prepare('INSERT INTO items (id, name) VALUES (?, ?)').run('k', 'foreign');
		await reassignShard('k', TARGET, 'items', { onConflict: 'replace' });
		expect(rowsOn(world, TARGET, 'k')).toMatchObject([{ name: 'source' }]);

		await seed(world, 'same', { name: 'same', qty: 3 });
		world.raw[TARGET]!.prepare('INSERT INTO items (id, name, qty) VALUES (?, ?, ?)').run('same', 'same', 3);
		await reassignShard('same', TARGET, 'items');
		expect(rowsOn(world, SOURCE, 'same')).toHaveLength(0);
	});

	it('refuses a row too wide for the bind limit before copying, and moves it with a version column', async () => {
		const columns = Array.from({ length: 110 }, (_, i) => `c${i}`);
		for (const shard of [SOURCE, TARGET]) {
			world.raw[shard]!.exec(`CREATE TABLE wide (id TEXT PRIMARY KEY, version INTEGER, ${columns.map((c) => `${c} INTEGER`).join(', ')})`);
		}
		world.raw[SOURCE]!.prepare(
			`INSERT INTO wide (id, version, ${columns.join(', ')}) VALUES (${new Array(columns.length + 2).fill('?').join(', ')})`
		).run('w', 1, ...columns.map((_, i) => i));
		await new KVShardMapper(world.kv, { hashShardMappings: false }).setShardMapping('w', SOURCE);

		expect(await codeOf(reassignShard('w', TARGET, 'wide'))).toBe('MIGRATION_ROW_TOO_WIDE');
		expect(world.raw[TARGET]!.prepare('SELECT * FROM wide').all()).toHaveLength(0);
		expect(activeClaims(world)).toBe(0);

		await reassignShard('w', TARGET, 'wide', { versionColumn: 'version' });
		expect(world.raw[TARGET]!.prepare('SELECT * FROM wide').all()).toHaveLength(1);
		expect(world.raw[SOURCE]!.prepare('SELECT * FROM wide').all()).toHaveLength(0);
	});

	it('moves a table whose primary key column is not id', async () => {
		for (const shard of [SOURCE, TARGET]) {
			world.raw[shard]!.exec('CREATE TABLE orders (order_id TEXT PRIMARY KEY, total INTEGER)');
		}
		world.raw[SOURCE]!.prepare('INSERT INTO orders (order_id, total) VALUES (?, ?)').run('o1', 9);
		await new KVShardMapper(world.kv, { hashShardMappings: false }).setShardMapping('o1', SOURCE);

		await reassignShard('o1', TARGET, 'orders', { pkColumn: 'order_id' });
		expect(world.raw[TARGET]!.prepare('SELECT * FROM orders').all()).toEqual([{ order_id: 'o1', total: 9 }]);
	});
});

// #endregion

// #region row fidelity

describe('Source row fidelity', () => {
	let world: World;

	beforeEach(async () => {
		resetConfig();
		resetPlacementState();
		world = makeWorld();
		await start(world);
	});

	it('moves blobs, 64-bit integers, NULL, an empty string and unicode without change', async () => {
		const data = new Uint8Array([0, 1, 2, 253, 254, 255]);
		await seed(world, 'rich', { name: 'é中😀 \u0000end', qty: 9223372036854775807n as unknown as number, note: null, data });
		await seed(world, 'empty', { name: '', qty: -9223372036854775807n as unknown as number, note: '' });

		await reassignShard('rich', TARGET, 'items');
		await reassignShard('empty', TARGET, 'items');

		const client = createNodeSqliteClient(world.raw[TARGET]!);
		const rich = client.prepare!('SELECT * FROM items WHERE id = ?').get!('rich') as Record<string, unknown>;
		expect(rich.name).toBe('é中😀 \u0000end');
		expect(rich.qty).toBe(9223372036854775807n);
		expect(rich.note).toBeNull();
		expect(Array.from(rich.data as Uint8Array)).toEqual(Array.from(data));

		const empty = client.prepare!('SELECT * FROM items WHERE id = ?').get!('empty') as Record<string, unknown>;
		expect(empty.name).toBe('');
		expect(empty.note).toBe('');
		expect(empty.qty).toBe(-9223372036854775807n);
	});

	it('archives a typed payload, not a bare JSON dump', async () => {
		await seed(world, 'rich', {
			name: 'n',
			qty: 9223372036854775807n as unknown as number,
			note: null,
			data: new Uint8Array([0, 1, 2, 255])
		});
		await reassignShard('rich', TARGET, 'items');

		const payload = JSON.parse(tombstones(world, SOURCE, 'rich')[0]!.payload as string);
		const columns = Object.fromEntries(payload.columns);
		expect(columns.qty).toEqual({ t: 'bigint', v: '9223372036854775807' });
		expect(columns.data).toEqual({ t: 'blob', v: 'AAEC/w==' });
		expect(columns.note).toEqual({ t: 'null' });
		expect(columns.name).toEqual({ t: 'text', v: 'n' });
		expect(columns.id).toEqual({ t: 'text', v: 'rich' });
	});

	it('treats a changed NULL against an empty string as a change, so the cutover misses and recopies', async () => {
		await seed(world, 'k', { note: null });

		let changed = false;
		setMigrationStepHook(async (step) => {
			if (step === 'verified' && !changed) {
				changed = true;
				await updateRow('k', 'items', { note: '' }, { id: 'k' });
			}
		});
		await reassignShard('k', TARGET, 'items');

		expect(rowsOn(world, TARGET, 'k')).toMatchObject([{ note: '' }]);
		expect(statesOf(world, 'k')).toContain('prepared:2');
	});

	it('does not reject an unchanged row that holds NULLs and blobs', async () => {
		await seed(world, 'k', { note: null, data: new Uint8Array([9, 9]) });
		await reassignShard('k', TARGET, 'items');

		expect(statesOf(world, 'k')).not.toContain('prepared:2');
		expect(rowsOn(world, TARGET, 'k')).toHaveLength(1);
	});
});

// #endregion

// #region writers and readers around the cutover

describe('Writers and readers around a move', () => {
	let world: World;

	beforeEach(async () => {
		resetConfig();
		resetPlacementState();
		world = makeWorld();
		await start(world);
	});

	/** Crashes right after the cutover transaction commits. */
	async function crashAfterCutover(key = 'k'): Promise<void> {
		await seed(world, key, { name: 'Ada' });
		const probe = makeWorld();
		await start(probe);
		await seed(probe, key, { name: 'Ada' });
		await reassignShard(key, TARGET, 'items');
		const at = probe.plan.log.find((entry) => entry.includes(`${SOURCE} batch of 2`))!;
		const n = Number(at.split(' ')[0]);

		await start(world);
		world.plan.arm(n, 'after');
		await expect(reassignShard(key, TARGET, 'items')).rejects.toBeDefined();
		world.plan.revive();
		await start(world);
	}

	it('survives a crash right after the source cutover commits and before the journal advances', async () => {
		await crashAfterCutover();

		// the cutover committed: row gone from the source, archived there, copied to the destination
		expect(rowsOn(world, SOURCE, 'k')).toHaveLength(0);
		expect(tombstones(world, SOURCE, 'k')).toHaveLength(1);
		expect(rowsOn(world, TARGET, 'k')).toHaveLength(1);
		expect(statesOf(world, 'k')).toContain('cutover_pending:1');
		expect(statesOf(world, 'k')).not.toContain('committed:1');
		expect(await mappingOf(world.kv, 'k')).toBe(SOURCE);

		const report = await recoverMigrations();

		expect(report.committed).toHaveLength(1);
		expect(report.failed).toEqual([]);
		expect(await mappingOf(world.kv, 'k')).toBe(TARGET);
		expect(await getMigrationStatus('k')).toMatchObject({ state: 'retired', active: false });
		expect(activeClaims(world)).toBe(0);
		expect(rowsOn(world, TARGET, 'k')).toMatchObject([{ name: 'Ada' }]);
	});

	it('gives a writer that sees zero rows in the cutover window MIGRATION_CUTOVER_PENDING, never success', async () => {
		await crashAfterCutover();

		expect(await codeOf(updateRow('k', 'items', { name: 'W' }, { id: 'k' }))).toBe('MIGRATION_CUTOVER_PENDING');
		expect(await codeOf(deleteRow('k', 'items', { id: 'k' }))).toBe('MIGRATION_CUTOVER_PENDING');
		expect(await codeOf(upsert('k', 'items', { id: 'k', name: 'U' }, 'id'))).toBe('MIGRATION_CUTOVER_PENDING');
		expect(await codeOf(insertInto('k', 'items', { id: 'k', name: 'I' }))).toBe('MIGRATION_CUTOVER_PENDING');
		expect(rowsOn(world, SOURCE, 'k')).toHaveLength(0);
		expect(rowsOn(world, TARGET, 'k')).toMatchObject([{ name: 'Ada' }]);
	});

	it('reads a key in the cutover window from the destination by following the tombstone', async () => {
		await crashAfterCutover();

		expect(await first<{ name: string }>('k', 'SELECT * FROM items WHERE id = ?', ['k'])).toMatchObject({ name: 'Ada' });
	});

	it('accepts the write again once recovery has published the move', async () => {
		await crashAfterCutover();
		await recoverMigrations();

		await updateRow('k', 'items', { name: 'W' }, { id: 'k' });
		expect(rowsOn(world, TARGET, 'k')).toMatchObject([{ name: 'W' }]);
	});

	it('retries a stale route once on the new owner when the mapping has moved on', async () => {
		await seed(world, 'k', { name: 'Ada' });
		await reassignShard('k', TARGET, 'items');

		// this process cached the old shard; KV already knows the new one
		await new KVShardMapper(world.kv, { hashShardMappings: false }).setShardMapping('k', SOURCE);
		await invalidateMappingCache('k');
		await first('k', 'SELECT * FROM items WHERE id = ?', ['k']);
		await new KVShardMapper(world.kv, { hashShardMappings: false }).setShardMapping('k', TARGET);

		await updateRow('k', 'items', { name: 'W' }, { id: 'k' });

		expect(rowsOn(world, TARGET, 'k')).toMatchObject([{ name: 'W' }]);
		expect(rowsOn(world, SOURCE, 'k')).toHaveLength(0);
	});

	it('returns a plain zero-row result for a key that never had a row', async () => {
		await new KVShardMapper(world.kv, { hashShardMappings: false }).setShardMapping('none', SOURCE);

		const updated = await updateRow('none', 'items', { name: 'W' }, { id: 'none' });
		const removed = await deleteRow('none', 'items', { id: 'none' });

		expect(updated.meta.changes).toBe(0);
		expect(removed.meta.changes).toBe(0);
	});

	it('returns an ignored duplicate insert as the genuine conflict it is', async () => {
		await seed(world, 'dup', { name: 'first' });

		const result = await insertInto('dup', 'items', { id: 'dup', name: 'second' }, { orIgnore: true });

		expect(result.meta.changes).toBe(0);
		expect(rowsOn(world, SOURCE, 'dup')).toMatchObject([{ name: 'first' }]);
	});

	it('carries the tombstone guard on builder inserts and upserts, and applies them to a live shard', async () => {
		await new KVShardMapper(world.kv, { hashShardMappings: false }).setShardMapping('fresh', SOURCE);

		await insertInto('fresh', 'items', { id: 'fresh', name: 'new' });
		await upsert('fresh', 'items', { id: 'fresh', name: 'changed' }, 'id');

		expect(rowsOn(world, SOURCE, 'fresh')).toMatchObject([{ name: 'changed' }]);
	});
});

// #endregion

// #region writers injected at every step

const STEPS = ['prepared', 'copied', 'verified', 'cutover_pending', 'cas', 'committed', 'published', 'retired'];
const WRITERS = ['update', 'delete', 'upsert', 'insert'] as const;

describe('A writer injected at every step of the move', () => {
	let world: World;

	beforeEach(async () => {
		resetConfig();
		resetPlacementState();
		world = makeWorld();
		await start(world);
	});

	for (const step of STEPS) {
		for (const kind of WRITERS) {
			it(`keeps every acknowledged ${kind} when it lands after ${step}`, async () => {
				await seed(world, 'k', { name: 'orig' });

				const attempt = () =>
					kind === 'update'
						? updateRow('k', 'items', { name: 'W' }, { id: 'k' })
						: kind === 'delete'
							? deleteRow('k', 'items', { id: 'k' })
							: kind === 'upsert'
								? upsert('k', 'items', { id: 'k', name: 'U' }, 'id')
								: insertInto('k', 'items', { id: 'k', name: 'I' });

				let acknowledged = false;
				const { hook } = createStepWriters({
					[step]: async () => {
						try {
							const result = await attempt();
							acknowledged = (result.meta.changes ?? 0) > 0;
						} catch {
							acknowledged = false;
						}
					}
				});
				setMigrationStepHook(hook);

				const outcome = await codeOf(reassignShard('k', TARGET, 'items'));
				setMigrationStepHook(null);
				await recoverMigrations();

				// a refused write is retried once the move is over
				if (!acknowledged && kind !== 'insert') {
					const retry = await attempt();
					acknowledged = (retry.meta.changes ?? 0) > 0;
				}

				const holders = [SOURCE, TARGET].filter((shard) => rowsOn(world, shard, 'k').length > 0);
				const expected = kind === 'update' ? 'W' : kind === 'upsert' ? 'U' : 'orig';

				if (kind === 'delete') {
					expect(holders).toEqual([]);
				} else {
					expect(holders).toHaveLength(1);
					expect(rowsOn(world, holders[0]!, 'k')).toMatchObject([{ name: expected }]);
					expect(await first<{ name: string }>('k', 'SELECT * FROM items WHERE id = ?', ['k'])).toMatchObject({ name: expected });
					expect(tombstones(world, holders[0]!, 'k')).toHaveLength(0);
				}

				if (kind === 'insert') {
					expect(acknowledged).toBe(false);
				}
				expect(['ok', undefined, 'MIGRATION_ABORTED']).toContain(outcome);
				expect(activeClaims(world)).toBe(0);
			});
		}
	}
});

// #endregion

// #region compare and swap misses

describe('A cutover that misses', () => {
	let world: World;

	beforeEach(async () => {
		resetConfig();
		resetPlacementState();
		world = makeWorld();
		await start(world);
	});

	it('recopies a row a writer changed between the copy and the cutover, and the write survives', async () => {
		await seed(world, 'k', { name: 'orig', qty: 1 });

		let changed = false;
		setMigrationStepHook(async (step) => {
			if (step === 'cutover_pending' && !changed) {
				changed = true;
				await updateRow('k', 'items', { qty: 2 }, { id: 'k' });
			}
		});
		await reassignShard('k', TARGET, 'items');

		expect(rowsOn(world, TARGET, 'k')).toMatchObject([{ qty: 2 }]);
		expect(rowsOn(world, SOURCE, 'k')).toHaveLength(0);
		expect(statesOf(world, 'k')).toContain('cutover_pending:2');
	});

	it('aborts after three recopies and leaves the source authoritative with no archive', async () => {
		await seed(world, 'k', { qty: 0 });

		let counter = 0;
		setMigrationStepHook(async (step) => {
			if (step === 'cutover_pending') {
				counter++;
				await updateRow('k', 'items', { qty: counter }, { id: 'k' });
			}
		});

		expect(await codeOf(reassignShard('k', TARGET, 'items'))).toBe('MIGRATION_ABORTED');
		setMigrationStepHook(null);

		expect(rowsOn(world, SOURCE, 'k')).toMatchObject([{ qty: counter }]);
		expect(rowsOn(world, TARGET, 'k')).toHaveLength(0);
		expect(tombstones(world, SOURCE, 'k')).toHaveLength(0);
		expect(await mappingOf(world.kv, 'k')).toBe(SOURCE);
		expect(await getMigrationStatus('k')).toMatchObject({ state: 'aborted', active: false });
		expect(activeClaims(world)).toBe(0);
	});

	it('aborts when an acknowledged delete removed the row, and keeps no copy of it', async () => {
		await seed(world, 'k', {});

		setMigrationStepHook(async (step) => {
			if (step === 'verified') {
				await deleteRow('k', 'items', { id: 'k' });
			}
		});

		expect(await codeOf(reassignShard('k', TARGET, 'items'))).toBe('MIGRATION_ABORTED');
		expect(rowsOn(world, SOURCE, 'k')).toHaveLength(0);
		expect(rowsOn(world, TARGET, 'k')).toHaveLength(0);
		expect(tombstones(world, SOURCE, 'k')).toHaveLength(0);
		expect(tombstones(world, TARGET, 'k')).toHaveLength(0);
	});
});

// #endregion

// #region faults at every operation

describe('Fault injection at every operation of a move', () => {
	async function cleanRunOperations(): Promise<number> {
		const world = makeWorld();
		await start(world);
		await seed(world, 'k', { name: 'Ada', qty: 5 });
		world.plan.reset();
		await reassignShard('k', TARGET, 'items');
		return world.plan.count;
	}

	async function converge(world: World): Promise<void> {
		expect(await mappingOf(world.kv, 'k')).toBeDefined();
		const holders = [SOURCE, TARGET].filter((shard) => rowsOn(world, shard, 'k').length > 0);
		expect(holders).toHaveLength(1);
		const owner = holders[0]!;

		expect(rowsOn(world, owner, 'k')).toMatchObject([{ id: 'k', name: 'Ada', qty: 5 }]);
		expect(tombstones(world, owner, 'k')).toHaveLength(0);
		expect(await mappingOf(world.kv, 'k')).toBe(owner);
		expect(activeClaims(world)).toBe(0);
		expect(await first<{ name: string }>('k', 'SELECT * FROM items WHERE id = ?', ['k'])).toMatchObject({ name: 'Ada' });
		await expect(recoverMigrations()).resolves.toMatchObject({ examined: 0 });
		await expect(verifyMigrations()).resolves.toMatchObject({ clean: true });
	}

	it('keeps the key readable at the crash, then converges to one owner after recovery', async () => {
		const total = await cleanRunOperations();
		expect(total).toBeGreaterThan(20);
		if (process.env.MIGRATION_RACE_REPORT) {
			console.log(`fault sweep: ${total} operations in a clean move, ${total * 2} crash points`);
		}

		const failures: string[] = [];
		for (let n = 1; n <= total; n++) {
			for (const mode of ['before', 'after'] as const) {
				const world = makeWorld();
				await start(world);
				await seed(world, 'k', { name: 'Ada', qty: 5 });
				world.plan.arm(n, mode);

				const crashed = await reassignShard('k', TARGET, 'items').then(
					() => false,
					() => true
				);
				const where = `n=${n} ${mode}: ${world.plan.log[world.plan.log.length - 1]}`;
				world.plan.revive();
				pastOrphanGrace();

				try {
					expect(crashed || n > total).toBe(true);

					// a restarted process reads the key before anyone runs recovery
					await start(world);
					expect(await first<{ name: string }>('k', 'SELECT * FROM items WHERE id = ?', ['k'])).toMatchObject({ name: 'Ada' });

					const report = await recoverMigrations();
					expect(report.failed).toEqual([]);
					await converge(world);
				} catch (error) {
					failures.push(`${where}: ${error instanceof Error ? error.message : String(error)}`);
				}
			}
		}

		expect(failures).toEqual([]);
	});

	it('keeps the key readable and writable-or-refused at every crash, with writers racing recovery', async () => {
		const total = await cleanRunOperations();
		const failures: string[] = [];

		for (let n = 1; n <= total; n += 3) {
			const world = makeWorld();
			await start(world);
			await seed(world, 'k', { name: 'Ada', qty: 5 });
			world.plan.arm(n, 'after');
			await reassignShard('k', TARGET, 'items').catch(() => undefined);
			world.plan.revive();
			pastOrphanGrace();
			await start(world);

			const code = await codeOf(updateRow('k', 'items', { qty: 6 }, { id: 'k' }));
			if (code !== undefined && code !== 'MIGRATION_CUTOVER_PENDING') {
				failures.push(`n=${n}: unexpected ${code}`);
				continue;
			}
			await recoverMigrations();
			if (code === 'MIGRATION_CUTOVER_PENDING') {
				await updateRow('k', 'items', { qty: 6 }, { id: 'k' });
			}

			const holders = [SOURCE, TARGET].filter((shard) => rowsOn(world, shard, 'k').length > 0);
			if (holders.length !== 1 || (rowsOn(world, holders[0]!, 'k')[0] as { qty: number }).qty !== 6) {
				failures.push(
					`n=${n}: holders ${holders.join(',')} qty ${(rowsOn(world, holders[0] ?? SOURCE, 'k')[0] as { qty: number } | undefined)?.qty}`
				);
			}
		}

		expect(failures).toEqual([]);
	});
});

// #endregion

// #region unavailable dependencies

describe('Unavailable journal and KV', () => {
	it('moves nothing when the journal is unavailable, and refuses recovery until it returns', async () => {
		const journalPlan = new FaultPlan();
		const world = makeWorld([SOURCE, TARGET, JOURNAL], { [JOURNAL]: journalPlan });
		await start(world);
		await seed(world, 'k', {});

		journalPlan.arm(1, 'before');
		expect(await codeOf(reassignShard('k', TARGET, 'items'))).toBe('JOURNAL_UNAVAILABLE');
		expect(await codeOf(recoverMigrations())).toBe('JOURNAL_UNAVAILABLE');
		expect(rowsOn(world, SOURCE, 'k')).toHaveLength(1);
		expect(rowsOn(world, TARGET, 'k')).toHaveLength(0);

		journalPlan.revive();
		await expect(recoverMigrations()).resolves.toMatchObject({ failed: [] });
		await reassignShard('k', TARGET, 'items');
		expect(rowsOn(world, TARGET, 'k')).toHaveLength(1);
	});

	it('finishes a committed move when KV was down during publish', async () => {
		const world = makeWorld();
		await start(world);
		await seed(world, 'k', { name: 'Ada' });

		setMigrationStepHook((step) => {
			if (step === 'committed') {
				world.failPuts.on = true;
			}
		});
		expect(await codeOf(reassignShard('k', TARGET, 'items'))).toBe('MIGRATION_PUBLISH_FAILED');
		setMigrationStepHook(null);

		expect(await getMigrationStatus('k')).toMatchObject({ state: 'committed', active: true });
		expect(await first<{ name: string }>('k', 'SELECT * FROM items WHERE id = ?', ['k'])).toMatchObject({ name: 'Ada' });
		expect(await codeOf(updateRow('k', 'items', { name: 'W' }, { id: 'k' }))).toBe('MIGRATION_CUTOVER_PENDING');

		world.failPuts.on = false;
		await expect(recoverMigrations()).resolves.toMatchObject({ failed: [] });

		expect(await getMigrationStatus('k')).toMatchObject({ state: 'retired', active: false });
		expect(await mappingOf(world.kv, 'k')).toBe(TARGET);
		await updateRow('k', 'items', { name: 'W' }, { id: 'k' });
		expect(rowsOn(world, TARGET, 'k')).toMatchObject([{ name: 'W' }]);
	});

	it('reports an operation it cannot resolve instead of guessing, and changes nothing', async () => {
		const world = makeWorld();
		await start(world);
		await seed(world, 'k', { name: 'Ada' });

		const probe = makeWorld();
		await start(probe);
		await seed(probe, 'k', { name: 'Ada' });
		await reassignShard('k', TARGET, 'items');
		const n = Number(probe.plan.log.find((entry) => entry.includes(`${SOURCE} batch of 2`))!.split(' ')[0]);

		await start(world);
		world.plan.arm(n, 'after');
		await reassignShard('k', TARGET, 'items').catch(() => undefined);
		world.plan.revive();
		await start(world);

		// something else rewrote the destination after the cutover
		world.raw[TARGET]!.prepare('UPDATE items SET name = ? WHERE id = ?').run('tampered', 'k');
		const report = await recoverMigrations();

		expect(report.failed).toMatchObject([{ code: 'MIGRATION_AMBIGUOUS' }]);
		expect(rowsOn(world, TARGET, 'k')).toMatchObject([{ name: 'tampered' }]);
		expect(await getMigrationStatus('k')).toMatchObject({ state: 'cutover_pending', active: true });
	});

	it('releases a claim that has no history once it is old, and leaves a young one alone', async () => {
		const world = makeWorld();
		await start(world);
		const claim = (key: string, claimedAt: number) =>
			world.raw[JOURNAL]!.prepare(
				'INSERT INTO collegedb_migration_active (scope, table_name, primary_key, op_id, claimed_at) VALUES (?, ?, ?, ?, ?)'
			).run('default', 'items', key, `op-${key}`, claimedAt);
		claim('lost', 0);
		claim('starting', Date.now());

		const report = await recoverMigrations();

		expect(report.released).toEqual(['op-lost']);
		expect(report.skipped).toEqual(['op-starting']);
		expect(activeClaims(world)).toBe(1);
	});

	it('leaves alone a move that changed more recently than olderThanMs', async () => {
		const world = makeWorld();
		await start(world);
		await seed(world, 'k', {});
		let seen: Awaited<ReturnType<typeof recoverMigrations>> | undefined;
		setMigrationStepHook(async (step) => {
			if (step === 'verified') {
				seen = await recoverMigrations({ olderThanMs: 60_000 });
			}
		});

		await reassignShard('k', TARGET, 'items');

		expect(seen?.skipped).toHaveLength(1);
		expect(seen?.committed).toEqual([]);
		expect(await getMigrationStatus('k')).toMatchObject({ state: 'retired' });
	});
});

describe('Recovery running while a move is still going', () => {
	let world: World;

	beforeEach(async () => {
		resetConfig();
		resetPlacementState();
		world = makeWorld();
		await start(world);
		await seed(world, 'k', { name: 'Ada' });
	});

	for (const step of ['prepared', 'copied', 'verified', 'cutover_pending', 'cas', 'committed', 'published']) {
		it(`converges to one owner when recovery runs inside the move after ${step}`, async () => {
			let ran = false;
			setMigrationStepHook(async (at) => {
				if (at === step && !ran) {
					ran = true;
					await recoverMigrations();
				}
			});

			const outcome = await codeOf(reassignShard('k', TARGET, 'items'));
			setMigrationStepHook(null);
			await recoverMigrations();

			const holders = [SOURCE, TARGET].filter((shard) => rowsOn(world, shard, 'k').length > 0);
			expect(holders).toHaveLength(1);
			expect(rowsOn(world, holders[0]!, 'k')).toMatchObject([{ name: 'Ada' }]);
			expect(tombstones(world, holders[0]!, 'k')).toHaveLength(0);
			expect(await mappingOf(world.kv, 'k')).toBe(holders[0]);
			expect(activeClaims(world)).toBe(0);
			expect(['MIGRATION_ABORTED', 'RECORD_NOT_FOUND', undefined, 'raw:UNIQUE constraint failed: items.id']).toContain(outcome);
			await expect(verifyMigrations()).resolves.toMatchObject({ clean: true });
		});
	}
});

// #endregion

// #region recovery lease

describe('Recovery lease', () => {
	const HISTORY_INSERT = /db-j run INSERT OR IGNORE INTO "collegedb_migration_histo/;
	let world: World;

	beforeEach(async () => {
		resetConfig();
		resetPlacementState();
		world = makeWorld();
		await start(world);
		await seed(world, 'k', { name: 'Ada' });
	});

	/** Stops a live move right after the given step, as a crash would. */
	async function leaveMoveAt(step: 'prepared' | 'cutover_pending' | 'committed'): Promise<void> {
		setMigrationStepHook((at) => {
			if (at === step) {
				throw new Error(`crash after ${step}`);
			}
		});
		await reassignShard('k', TARGET, 'items').catch(() => undefined);
		setMigrationStepHook(null);
		expect(await getMigrationStatus('k')).toMatchObject({ state: step, active: true });
	}

	function leaseRows(): Array<{ scope: string; holder: string; generation: number; expires_at: number }> {
		return world.raw[JOURNAL]!.prepare('SELECT * FROM collegedb_recovery_lease').all() as any;
	}

	function historyRows(): unknown[] {
		return world.raw[JOURNAL]!.prepare(
			'SELECT op_id, state, attempt FROM collegedb_migration_history ORDER BY op_id, state, attempt'
		).all();
	}

	function jumpClock(ms: number): void {
		setClock(Date.now() + ms);
	}

	async function expectOneOwner(): Promise<void> {
		const holders = [SOURCE, TARGET].filter((shard) => rowsOn(world, shard, 'k').length > 0);
		expect(holders).toHaveLength(1);
		expect(rowsOn(world, holders[0]!, 'k')).toMatchObject([{ name: 'Ada' }]);
		expect(tombstones(world, holders[0]!, 'k')).toHaveLength(0);
		expect(await mappingOf(world.kv, 'k')).toBe(holders[0]);
		expect(activeClaims(world)).toBe(0);
	}

	for (const step of ['prepared', 'cutover_pending', 'committed'] as const) {
		it(`lets one of two simultaneous recoveries run a move left at ${step}, and the other is refused`, async () => {
			await leaveMoveAt(step);

			const results = await Promise.allSettled([recoverMigrations(), recoverMigrations()]);
			const ran = results.filter(
				(result): result is PromiseFulfilledResult<Awaited<ReturnType<typeof recoverMigrations>>> => result.status === 'fulfilled'
			);
			const refused = results.filter((result): result is PromiseRejectedResult => result.status === 'rejected');

			expect(ran).toHaveLength(1);
			expect(refused).toHaveLength(1);
			expect(refused[0]!.reason).toBeInstanceOf(CollegeDBError);
			expect(refused[0]!.reason.code).toBe('RECOVERY_IN_PROGRESS');
			expect(ran[0]!.value.failed).toEqual([]);
			expect(ran[0]!.value.committed).toHaveLength(1);
			await expectOneOwner();
		});
	}

	it('stops a stale executor at its next journal write once another holder took the lease', async () => {
		await leaveMoveAt('prepared');
		const pause = world.plan.pauseOn(HISTORY_INSERT);
		const stale = recoverMigrations().then(
			() => undefined,
			(error: unknown) => error
		);
		await pause.reached;
		expect(leaseRows()).toMatchObject([{ generation: 1 }]);

		// the lease runs out, a second holder takes it, and the row it was about to copy is gone
		jumpClock(61_000);
		world.raw[SOURCE]!.prepare('DELETE FROM items WHERE id = ?').run('k');
		const report = await recoverMigrations();
		expect(report.aborted).toMatchObject([{ reason: 'SOURCE_ROW_MISSING' }]);
		expect(leaseRows()).toMatchObject([{ generation: 2 }]);

		const history = historyRows();
		const mark = world.plan.log.length;
		pause.resume();
		const error = (await stale) as CollegeDBError;

		expect(error).toBeInstanceOf(CollegeDBError);
		expect(error.code).toBe('RECOVERY_FENCED');
		expect(historyRows()).toEqual(history);
		expect(rowsOn(world, TARGET, 'k')).toHaveLength(0);
		expect(leaseRows()).toMatchObject([{ generation: 2 }]);

		// the held statement is the only write the stale executor issues, and it matched no row
		const tail = world.plan.log.slice(mark);
		expect(tail.filter((entry) => /db-[ab] (run|batch)/.test(entry))).toEqual([]);
		expect(tail.filter((entry) => /db-j (run|batch)/.test(entry))).toHaveLength(1);
	});

	it('stops a stale executor before a destination write with reads only', async () => {
		await leaveMoveAt('prepared');
		const pause = world.plan.pauseOn(/db-b first/);
		const stale = recoverMigrations().then(
			() => undefined,
			(error: unknown) => error
		);
		await pause.reached;

		jumpClock(61_000);
		world.raw[SOURCE]!.prepare('DELETE FROM items WHERE id = ?').run('k');
		await recoverMigrations();
		expect(leaseRows()).toMatchObject([{ generation: 2 }]);

		const history = historyRows();
		const mark = world.plan.log.length;
		pause.resume();
		const error = (await stale) as CollegeDBError;

		expect(error.code).toBe('RECOVERY_FENCED');
		expect(rowsOn(world, TARGET, 'k')).toHaveLength(0);
		expect(historyRows()).toEqual(history);
		const tail = world.plan.log.slice(mark);
		expect(tail.length).toBeGreaterThan(0);
		expect(tail.filter((entry) => /db-[abj] (run|batch)/.test(entry))).toEqual([]);
	});

	it('refuses a second recovery while a crashed one still holds the lease, and takes over after expiry', async () => {
		await leaveMoveAt('prepared');
		world.plan.crashOn(HISTORY_INSERT);
		await recoverMigrations().catch(() => undefined);
		world.plan.revive();

		expect(leaseRows()).toMatchObject([{ generation: 1 }]);
		expect(leaseRows()[0]!.expires_at).toBeGreaterThan(Date.now());
		expect(await codeOf(recoverMigrations())).toBe('RECOVERY_IN_PROGRESS');
		expect(activeClaims(world)).toBe(1);

		jumpClock(61_000);
		const report = await recoverMigrations();

		expect(report.failed).toEqual([]);
		expect(report.committed).toHaveLength(1);
		expect(leaseRows()).toMatchObject([{ generation: 2, expires_at: 0 }]);
		await expectOneOwner();
	});

	it('holds the lease while it runs and keeps the generation rising across acquire, release and acquire', async () => {
		await leaveMoveAt('prepared');
		let held: ReturnType<typeof leaseRows> = [];
		setMigrationStepHook((at) => {
			if (at === 'copied') {
				held = leaseRows();
			}
		});

		await recoverMigrations();

		expect(held).toHaveLength(1);
		expect(held[0]).toMatchObject({ scope: 'default', generation: 1 });
		expect(held[0]!.expires_at).toBeGreaterThan(Date.now());
		expect(leaseRows()).toMatchObject([{ generation: 1, expires_at: 0 }]);

		await recoverMigrations();
		expect(leaseRows()).toMatchObject([{ generation: 2, expires_at: 0 }]);

		await recoverMigrations();
		expect(leaseRows()).toMatchObject([{ generation: 3, expires_at: 0 }]);
	});

	it('creates the lease table and the history index idempotently, twice over one database', async () => {
		const raw = openNodeSqlite();
		await ensureJournalSchema({ j: createNodeSqliteProvider(raw) }, 'j');
		await ensureJournalSchema({ j: createNodeSqliteProvider(raw) }, 'j');

		const columns = raw.prepare('PRAGMA table_info(collegedb_recovery_lease)').all() as Array<{ name: string }>;
		const indexes = raw.prepare('PRAGMA index_list(collegedb_migration_history)').all() as Array<{ name: string }>;
		expect(columns.map((column) => column.name)).toEqual(['scope', 'holder', 'generation', 'expires_at']);
		expect(indexes.map((index) => index.name)).toContain('collegedb_history_by_key');
	});

	it('adds the history index to a journal table that an earlier build created without it', async () => {
		const raw = openNodeSqlite();
		for (const statement of buildJournalSchema('sqlite')) {
			raw.exec(statement);
		}
		const indexNames = () =>
			(raw.prepare('PRAGMA index_list(collegedb_migration_history)').all() as Array<{ name: string }>).map((i) => i.name);
		expect(indexNames()).not.toContain('collegedb_history_by_key');

		await ensureJournalSchema({ j: createNodeSqliteProvider(raw) }, 'j');

		expect(indexNames()).toContain('collegedb_history_by_key');
	});

	it('takes the lease for verifyMigrations repair, and not for a plain verify or a live move', async () => {
		await recoverMigrations();
		world.raw[JOURNAL]!.prepare('UPDATE collegedb_recovery_lease SET holder = ?, generation = ?, expires_at = ?').run(
			'other',
			7,
			Date.now() + 60_000
		);

		expect(await codeOf(verifyMigrations({ repair: true }))).toBe('RECOVERY_IN_PROGRESS');
		await expect(verifyMigrations()).resolves.toMatchObject({ clean: true });
		await reassignShard('k', TARGET, 'items');
		expect(rowsOn(world, TARGET, 'k')).toHaveLength(1);

		expect(leaseRows()).toMatchObject([{ holder: 'other', generation: 7 }]);
		await expectOneOwner();
	});

	describe('Recovery lease window', () => {
		const DESTINATION_INSERT = /db-b run INSERT INTO "items"/;
		const SOURCE_CUTOVER = /db-a batch of/;

		/**
		 * Holds executor A right after its lease check, before the shard statement, runs
		 * `meanwhile` (the second holder), then lets A's statement through.
		 */
		async function stallAfterCheck(pauseAt: RegExp, meanwhile: () => Promise<void>) {
			const pause = world.plan.pauseOn(pauseAt);
			const stale = recoverMigrations().then(
				(report) => ({ report }),
				(error: unknown) => ({ error })
			);
			await pause.reached;
			expect(leaseRows()).toMatchObject([{ generation: 1 }]);

			jumpClock(61_000);
			await meanwhile();
			expect(leaseRows()).toMatchObject([{ generation: 2 }]);

			const before = historyRows();
			const mark = world.plan.log.length;
			pause.resume();
			const outcome = await stale;
			return { outcome, before, tail: world.plan.log.slice(mark) };
		}

		function expectFenced(outcome: Awaited<ReturnType<typeof stallAfterCheck>>['outcome']): void {
			expect('report' in outcome ? `returned ${JSON.stringify(outcome.report)}` : undefined).toBeUndefined();
			const error = (outcome as { error: unknown }).error as CollegeDBError;
			expect(error).toBeInstanceOf(CollegeDBError);
			expect(error.code).toBe('RECOVERY_FENCED');
		}

		const total = async () => (await countAllShards('items')).total;

		it('fences a copy that lands after the new holder finished the move, and leaves nothing behind', async () => {
			await leaveMoveAt('prepared');

			const { outcome, before, tail } = await stallAfterCheck(DESTINATION_INSERT, async () => {
				expect((await recoverMigrations()).committed).toHaveLength(1);
			});

			// the held statement ran, hit the new holder's row, and nothing else of the stale executor's landed
			expect(tail.filter((entry) => DESTINATION_INSERT.test(entry))).toHaveLength(1);
			expectFenced(outcome);
			expect(historyRows()).toEqual(before);
			expect(await getMigrationStatus('k')).toMatchObject({ state: 'retired', active: false, destination: TARGET });
			expect(rowsOn(world, SOURCE, 'k')).toHaveLength(0);
			expect(rowsOn(world, TARGET, 'k')).toMatchObject([{ name: 'Ada' }]);
			expect(tombstones(world, SOURCE, 'k')).toHaveLength(1);
			expect(tombstones(world, TARGET, 'k')).toHaveLength(0);
			expect(await mappingOf(world.kv, 'k')).toBe(TARGET);
			expect(activeClaims(world)).toBe(0);
			expect(await total()).toBe(1);
			expect(await first<{ name: string }>('k', 'SELECT * FROM items WHERE id = ?', ['k'])).toMatchObject({ name: 'Ada' });
			await expect(verifyMigrations()).resolves.toMatchObject({ clean: true, strandedCopies: [], staleCutovers: [] });
		});

		const aborts = {
			'the source row was deleted': async () => {
				world.raw[SOURCE]!.prepare('DELETE FROM items WHERE id = ?').run('k');
				expect((await recoverMigrations()).aborted).toMatchObject([{ reason: 'SOURCE_ROW_MISSING' }]);
				return { source: 0, total: 1 };
			},
			'the source row kept changing until the cutover gave up': async () => {
				let writes = 0;
				setMigrationStepHook(async (step) => {
					if (step === 'cutover_pending') {
						await updateRow('k', 'items', { qty: 100 + ++writes }, { id: 'k' });
					}
				});
				const report = await recoverMigrations();
				setMigrationStepHook(null);
				expect(report.aborted).toMatchObject([{ reason: 'CAS_MISS' }]);
				return { source: 1, total: 2 };
			}
		};

		for (const [why, abort] of Object.entries(aborts)) {
			it(`finds and removes the destination copy a stale executor lands after the new holder aborted, because ${why}`, async () => {
				await leaveMoveAt('prepared');

				let expected!: Awaited<ReturnType<(typeof aborts)[keyof typeof aborts]>>;
				const { outcome, before } = await stallAfterCheck(DESTINATION_INSERT, async () => {
					expected = await abort();
				});

				expectFenced(outcome);
				expect(historyRows()).toEqual(before);
				expect(await getMigrationStatus('k')).toMatchObject({ state: 'aborted', active: false });
				expect(rowsOn(world, SOURCE, 'k')).toHaveLength(expected.source);
				expect(await mappingOf(world.kv, 'k')).toBe(SOURCE);
				expect(activeClaims(world)).toBe(0);

				// the stale copy is live on the destination, which a count sees
				expect(rowsOn(world, TARGET, 'k')).toMatchObject([{ name: 'Ada' }]);
				expect(await total()).toBe(expected.total);

				const report = await verifyMigrations();
				expect(report).toMatchObject({
					clean: false,
					strandedCopies: [{ shard: TARGET, table: 'items', key: 'k' }],
					staleCutovers: [],
					ghosts: []
				});
				expect(rowsOn(world, TARGET, 'k')).toHaveLength(1);

				const repaired = await verifyMigrations({ repair: true });
				expect(repaired.repaired).toEqual([`${TARGET}:items:k`]);
				expect(rowsOn(world, TARGET, 'k')).toHaveLength(0);
				expect(rowsOn(world, SOURCE, 'k')).toHaveLength(expected.source);
				expect(tombstones(world, SOURCE, 'k')).toHaveLength(0);
				expect(await total()).toBe(expected.source);
				await expect(verifyMigrations()).resolves.toMatchObject({ clean: true });
			});
		}

		it('fences a cutover that lands after the new holder committed, and it matches no row', async () => {
			await leaveMoveAt('cutover_pending');

			const { outcome, before, tail } = await stallAfterCheck(SOURCE_CUTOVER, async () => {
				expect((await recoverMigrations()).committed).toHaveLength(1);
			});

			expect(tail.filter((entry) => SOURCE_CUTOVER.test(entry))).toHaveLength(1);
			expectFenced(outcome);
			expect(historyRows()).toEqual(before);
			expect(await getMigrationStatus('k')).toMatchObject({ state: 'retired', active: false, destination: TARGET });
			expect(rowsOn(world, SOURCE, 'k')).toHaveLength(0);
			expect(rowsOn(world, TARGET, 'k')).toMatchObject([{ name: 'Ada' }]);
			expect(tombstones(world, SOURCE, 'k')).toHaveLength(1);
			expect(await mappingOf(world.kv, 'k')).toBe(TARGET);
			expect(activeClaims(world)).toBe(0);
			expect(await total()).toBe(1);
			expect(await first<{ name: string }>('k', 'SELECT * FROM items WHERE id = ?', ['k'])).toMatchObject({ name: 'Ada' });
			await expect(verifyMigrations()).resolves.toMatchObject({ clean: true, strandedCopies: [], staleCutovers: [] });
		});

		/** The source row changes and returns to the snapshot while A holds its cutover, and B aborts in between. */
		async function staleCutover() {
			await leaveMoveAt('cutover_pending');
			const put = (name: string) => world.raw[SOURCE]!.prepare('UPDATE items SET name = ? WHERE id = ?').run(name, 'k');

			return await stallAfterCheck(SOURCE_CUTOVER, async () => {
				put('Grace');
				expect((await recoverMigrations()).aborted).toMatchObject([{ reason: 'SOURCE_CHANGED' }]);
				put('Ada');
			});
		}

		it('finds and restores the row a stale cutover archived after the source changed and returned to the snapshot', async () => {
			const { outcome, before, tail } = await staleCutover();

			// the held cutover found a row equal to its snapshot, archived it and deleted it
			expect(tail.filter((entry) => SOURCE_CUTOVER.test(entry))).toHaveLength(1);
			expectFenced(outcome);
			expect(historyRows()).toEqual(before);
			expect(await getMigrationStatus('k')).toMatchObject({ state: 'aborted', active: false });
			expect(rowsOn(world, SOURCE, 'k')).toHaveLength(0);
			expect(rowsOn(world, TARGET, 'k')).toHaveLength(0);
			expect(tombstones(world, SOURCE, 'k')).toHaveLength(1);
			expect(await mappingOf(world.kv, 'k')).toBe(SOURCE);
			expect(await total()).toBe(0);
			expect(await first('k', 'SELECT * FROM items WHERE id = ?', ['k'])).toBeNull();

			const report = await verifyMigrations();
			expect(report).toMatchObject({
				clean: false,
				strandedCopies: [],
				staleCutovers: [{ shard: SOURCE, table: 'items', key: 'k', restorable: true }]
			});
			expect(rowsOn(world, SOURCE, 'k')).toHaveLength(0);

			const repaired = await verifyMigrations({ repair: true });
			expect(repaired.repaired).toEqual([`${SOURCE}:items:k`]);
			expect(rowsOn(world, SOURCE, 'k')).toMatchObject([{ id: 'k', name: 'Ada', qty: 1 }]);
			expect(rowsOn(world, TARGET, 'k')).toHaveLength(0);
			expect(tombstones(world, SOURCE, 'k')).toHaveLength(0);
			expect(await first<{ name: string }>('k', 'SELECT * FROM items WHERE id = ?', ['k'])).toMatchObject({ name: 'Ada' });
			expect(await total()).toBe(1);
			await expect(verifyMigrations()).resolves.toMatchObject({ clean: true });
		});

		it('reports a stale cutover and leaves it alone when the destination holds a row or the archive is gone', async () => {
			await staleCutover();
			world.raw[TARGET]!.prepare("INSERT INTO items (id, name) VALUES ('k', 'elsewhere')").run();

			const report = await verifyMigrations({ repair: true });
			expect(report).toMatchObject({ clean: false, repaired: [], staleCutovers: [{ key: 'k', restorable: false }] });
			expect(rowsOn(world, SOURCE, 'k')).toHaveLength(0);
			expect(tombstones(world, SOURCE, 'k')).toHaveLength(1);
			expect(rowsOn(world, TARGET, 'k')).toMatchObject([{ name: 'elsewhere' }]);

			world.raw[TARGET]!.prepare("DELETE FROM items WHERE id = 'k'").run();
			world.raw[SOURCE]!.prepare('UPDATE collegedb_retired SET payload = NULL, purged_at = 1').run();
			await expect(verifyMigrations({ repair: true })).resolves.toMatchObject({ repaired: [], staleCutovers: [{ restorable: false }] });
			expect(tombstones(world, SOURCE, 'k')).toHaveLength(1);
		});

		it('leaves a destination row alone when it differs from every row the aborted move journaled', async () => {
			await leaveMoveAt('prepared');
			world.raw[SOURCE]!.prepare('DELETE FROM items WHERE id = ?').run('k');
			expect((await recoverMigrations()).aborted).toMatchObject([{ reason: 'SOURCE_ROW_MISSING' }]);
			world.raw[TARGET]!.prepare("INSERT INTO items (id, name) VALUES ('k', 'not the snapshot')").run();

			await expect(verifyMigrations({ repair: true })).resolves.toMatchObject({ clean: true, repaired: [], strandedCopies: [] });
			expect(rowsOn(world, TARGET, 'k')).toMatchObject([{ name: 'not the snapshot' }]);
		});

		it('leaves the destination row of a later committed move alone, though it equals what an aborted move journaled', async () => {
			await leaveMoveAt('prepared');
			world.raw[SOURCE]!.prepare('DELETE FROM items WHERE id = ?').run('k');
			expect((await recoverMigrations()).aborted).toHaveLength(1);

			world.raw[SOURCE]!.prepare("INSERT INTO items (id, name, qty) VALUES ('k', 'Ada', 1)").run();
			await reassignShard('k', TARGET, 'items');
			expect(await getMigrationStatus('k')).toMatchObject({ state: 'retired', generation: 2 });

			await expect(verifyMigrations({ repair: true })).resolves.toMatchObject({ clean: true, repaired: [], strandedCopies: [] });
			expect(rowsOn(world, TARGET, 'k')).toMatchObject([{ name: 'Ada' }]);
		});

		it('residual: a stale copy brings back a row deleted from the destination after the new holder committed, and verify cannot tell', async () => {
			await leaveMoveAt('prepared');
			const { outcome, before } = await stallAfterCheck(DESTINATION_INSERT, async () => {
				await recoverMigrations();
				expect(await deleteRow('k', 'items', { id: 'k' })).toMatchObject({ success: true });
			});

			expectFenced(outcome);
			expect(historyRows()).toEqual(before);
			expect(rowsOn(world, TARGET, 'k')).toMatchObject([{ name: 'Ada' }]);
			expect(await total()).toBe(1);
			await expect(verifyMigrations()).resolves.toMatchObject({ clean: true });
		});
	});
});

// #endregion

// #region retention, verification and rollback

describe('Retention, verification and purge', () => {
	let world: World;

	beforeEach(async () => {
		resetConfig();
		resetPlacementState();
		world = makeWorld();
		await start(world);
		setClock(10_000);
		await seed(world, 'k', { name: 'Ada' });
		await reassignShard('k', TARGET, 'items', { retire: { afterMs: 1000 } });
	});

	it('refuses to purge before the retention window passes', async () => {
		setClock(10_500);
		await verifyMigrations();

		const report = await purgeRetired();

		expect(report.purged).toEqual([]);
		expect(report.refused).toMatchObject([{ key: 'k', reason: 'RETENTION_NOT_ELAPSED' }]);
		expect(tombstones(world, SOURCE, 'k')[0]!.payload).not.toBeNull();
	});

	it('refuses to purge until a verification pass finished after the move', async () => {
		setClock(20_000);

		expect((await purgeRetired()).refused).toMatchObject([{ reason: 'NOT_VERIFIED' }]);
	});

	it('purges the archive after a clean verification, and keeps the tombstone fence', async () => {
		setClock(20_000);
		expect((await verifyMigrations()).clean).toBe(true);

		const report = await purgeRetired();

		expect(report.purged).toMatchObject([{ key: 'k' }]);
		expect(tombstones(world, SOURCE, 'k')).toMatchObject([{ payload: null, purged_at: 20_000 }]);
		expect(await purgeRetired()).toEqual({ purged: [], refused: [] });

		await new KVShardMapper(world.kv, { hashShardMappings: false }).setShardMapping('k', SOURCE);
		await invalidateMappingCache('k');
		expect(await codeOf(insertInto('k', 'items', { id: 'k', name: 'ghost' }))).toBe('MIGRATION_CUTOVER_PENDING');
	});

	it('reports a ghost without touching it, and removes one equal to the archive only on repair', async () => {
		setClock(20_000);
		world.raw[SOURCE]!.prepare('INSERT INTO items (id, name, qty) VALUES (?, ?, ?)').run('k', 'Ada', 1);

		const observed = await verifyMigrations();
		expect(observed.clean).toBe(false);
		expect(observed.ghosts).toMatchObject([{ shard: SOURCE, key: 'k', equalsArchive: true }]);
		expect(rowsOn(world, SOURCE, 'k')).toHaveLength(1);
		expect((await purgeRetired()).refused).toMatchObject([{ reason: 'NOT_VERIFIED' }]);

		const repaired = await verifyMigrations({ repair: true });
		expect(repaired.repaired).toEqual([`${SOURCE}:items:k`]);
		expect(rowsOn(world, SOURCE, 'k')).toHaveLength(0);
		expect((await verifyMigrations()).clean).toBe(true);
	});

	it('keeps a ghost that differs from the archive and reports it', async () => {
		setClock(20_000);
		world.raw[SOURCE]!.prepare('INSERT INTO items (id, name) VALUES (?, ?)').run('k', 'newer, unrelated');

		const report = await verifyMigrations({ repair: true });

		expect(report.ghosts).toMatchObject([{ equalsArchive: false }]);
		expect(rowsOn(world, SOURCE, 'k')).toHaveLength(1);
	});

	it('rolls back as a new move, never as an undo', async () => {
		await reassignShard('k', SOURCE, 'items');

		expect(rowsOn(world, SOURCE, 'k')).toMatchObject([{ name: 'Ada' }]);
		expect(await getMigrationStatus('k')).toMatchObject({ generation: 2, source: TARGET, destination: SOURCE });
		expect(statesOf(world, 'k').filter((entry) => entry.startsWith('retired'))).toHaveLength(2);
	});
});

// #endregion

// #region catalog races on create
// a create matching `when` loses a race; the winner's object is visible only when `winnerCommits`
function fakePostgres(race?: { when: RegExp; error: unknown; winnerCommits: boolean }) {
	const log: string[] = [];
	const objects = new Set<string>();
	const database = {
		dialect: 'postgres',
		prepare(sql: string) {
			const args: unknown[] = [];
			const statement = {
				bind(...bindings: unknown[]) {
					args.push(...bindings);
					return statement;
				},
				async run() {
					log.push(sql);
					const name = /IF NOT EXISTS (\w+)/.exec(sql)?.[1];
					if (name && !objects.has(name) && race?.when.test(sql)) {
						if (race.winnerCommits) {
							objects.add(name);
						}
						throw race.error;
					}
					if (name) {
						objects.add(name);
					}
					return { success: true, results: [], meta: { duration: 0 } };
				},
				async first() {
					log.push(sql);
					return objects.has(String(args[0])) ? { present: 1 } : null;
				}
			};
			return statement;
		}
	} as unknown as SQLDatabase;
	return { database, log, objects };
}

function pgError(code: string, constraint?: string): Error {
	return Object.assign(new Error(`postgres ${code}`), { code, constraint });
}

describe('Concurrent creates on Postgres', () => {
	const INDEX_RACE = /^CREATE INDEX IF NOT EXISTS collegedb_history_by_key/;
	const TABLE_RACE = /^CREATE TABLE IF NOT EXISTS collegedb_migration_active/;
	const CATALOG_CLASH = pgError('23505', 'pg_class_relname_nsp_index');

	it.each([
		['a unique violation on the catalog name, object committed by the winner', CATALOG_CLASH, true, true],
		['duplicate_table, object committed by the winner', pgError('42P07'), true, true],
		[
			'a unique violation wrapped by Drizzle, object committed by the winner',
			Object.assign(new Error('Failed query'), { cause: CATALOG_CLASH }),
			true,
			true
		],
		['a unique violation on the catalog name, object absent after the check', CATALOG_CLASH, false, false],
		['a unique violation on another constraint, object committed by the winner', pgError('23505', 'collegedb_history_pkey'), true, false],
		['permission denied, object committed by the winner', pgError('42501'), true, false]
	])('ensureIndex: %s', async (_label, error, winnerCommits, swallowed) => {
		const { database, log } = fakePostgres({ when: INDEX_RACE, error, winnerCommits });

		const work = ensureIndex(database, HISTORY_INDEX);
		if (swallowed) {
			await expect(work).resolves.toBeUndefined();
			expect(log.some((sql) => /FROM pg_indexes/.test(sql))).toBe(true);
		} else {
			await expect(work).rejects.toBe(error);
		}
	});

	it('ensureJournalSchema creates every journal table when a concurrent creator wins the race', async () => {
		const { database, objects } = fakePostgres({
			when: TABLE_RACE,
			error: pgError('23505', 'pg_type_typname_nsp_index'),
			winnerCommits: true
		});

		await ensureJournalSchema({ j: database }, 'j');

		expect([...objects]).toEqual(
			expect.arrayContaining(['collegedb_migration_active', 'collegedb_migration_history', 'collegedb_history_by_key'])
		);
	});

	it('ensureJournalSchema fails with SCHEMA_MIGRATION_FAILED when the raced table is absent after the check', async () => {
		const { database } = fakePostgres({
			when: TABLE_RACE,
			error: pgError('23505', 'pg_type_typname_nsp_index'),
			winnerCommits: false
		});

		expect(await codeOf(ensureJournalSchema({ j: database }, 'j'))).toBe('SCHEMA_MIGRATION_FAILED');
	});

	it('ensureJournalSchema fails with SCHEMA_MIGRATION_FAILED for another error on a journal table', async () => {
		const { database } = fakePostgres({ when: TABLE_RACE, error: pgError('42501'), winnerCommits: true });

		expect(await codeOf(ensureJournalSchema({ j: database }, 'j'))).toBe('SCHEMA_MIGRATION_FAILED');
	});
});

// #endregion

// #region rebalance

describe('Rebalance on the journal', () => {
	it('moves every disagreeing key through the journal and loses none', async () => {
		resetConfig();
		resetPlacementState();
		const world = makeWorld([SOURCE, TARGET, JOURNAL]);
		await start(world);

		const names = [SOURCE, TARGET, JOURNAL];
		const keys = Array.from({ length: 12 }, (_, i) => `r-${i}`);
		for (const key of keys) {
			const wrong = names.find((name) => name !== hrwShard(key, names))!;
			await seed(world, key, { name: key }, wrong);
		}

		const result = await rebalance('items', { concurrency: 4 });

		expect(result.failed).toEqual([]);
		expect(result.moved).toBe(keys.length);
		for (const key of keys) {
			const owner = hrwShard(key, names);
			expect(rowsOn(world, owner, key)).toHaveLength(1);
			expect(names.filter((name) => rowsOn(world, name, key).length > 0)).toEqual([owner]);
			expect(await first<{ name: string }>(key, 'SELECT * FROM items WHERE id = ?', [key])).toMatchObject({ name: key });
		}
		expect(activeClaims(world)).toBe(0);
		await expect(verifyMigrations()).resolves.toMatchObject({ clean: true });
	});
});

// #endregion

// #region real concurrency

const RACE_ITERATIONS = Number(process.env.MIGRATION_RACE_ITERATIONS ?? 6);
const RACE_SEED = process.env.MIGRATION_RACE_SEED === undefined ? undefined : Number(process.env.MIGRATION_RACE_SEED);

describe('The cutover racing a writer in separate threads on one WAL file', () => {
	for (const kind of ['update', 'delete', 'insert', 'upsert'] as RaceKind[]) {
		it(`holds the write-ordering invariant for ${kind}`, async () => {
			const report = await runCutoverRace({ kind, iterations: RACE_ITERATIONS, seed: RACE_SEED });

			if (process.env.MIGRATION_RACE_REPORT) {
				console.log(`race ${JSON.stringify({ ...report, violations: report.violations.length })}`);
			}
			expect(report.violations).toEqual([]);
			expect(report.hits + report.misses).toBe(RACE_ITERATIONS);
			if (RACE_ITERATIONS >= 200 && kind !== 'insert') {
				expect(report.hits).toBeGreaterThan(0);
				expect(report.misses).toBeGreaterThan(0);
			}
		}, 600_000);
	}

	it('control arm: notices a cutover that compares only the key', async () => {
		const report = await runCutoverRace({ kind: 'update', iterations: 60, control: 'unconditional-cutover' });

		expect(report.violations.length).toBeGreaterThan(0);
		expect(report.violations.join('\n')).toMatch(/acknowledged update is not on the owner|archive payload differs/);
	});

	it('control arm: notices an insert that skips the tombstone guard', async () => {
		const report = await runCutoverRace({ kind: 'insert', iterations: 60, control: 'unguarded-insert' });

		expect(report.violations.length).toBeGreaterThan(0);
		expect(report.violations.join('\n')).toMatch(/ghost|insert over an existing key/);
	});
});

// #endregion
