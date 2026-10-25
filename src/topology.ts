/**
 * @fileoverview The journal copy of the placement manifest, the topology fence
 * on every shard, and the census behind `pruneEpochs`.
 *
 * KV is eventually consistent and has no compare-and-set. With a journal shard
 * configured, the manifest lives in a SQL table and every write to it is a
 * compare-and-swap on a revision number, so two clients cannot overwrite each
 * other and a lagging KV cannot make a cold isolate publish epoch 1 over the real
 * topology. The KV copy becomes a derived one.
 *
 * Each data shard also carries `collegedb_topology(min_generation)`. A guarded
 * insert or upsert binds the writer's manifest generation and matches no row when
 * the shard's `min_generation` is above it, so raising the fence on every shard
 * stops a writer on an older generation before a census counts what depends on an
 * epoch.
 *
 * @author Gregory Mitchell
 * @since 1.4.0
 */

import { CollegeDBError } from './errors';
import { assertAtomicBatch } from './migrations';
import type { PlacementEpoch, PlacementJournal, PlacementManifest } from './placement';
import { TOPOLOGY_TABLE, buildFenceLock, buildInsertIgnore, isProvenJournalDialect, quoteIdentifier, type BuiltQuery } from './query';
import type { QueryResult, SQLDatabase } from './types';

/** Table on the journal shard that holds the placement manifest. @since 1.4.0 */
export const MANIFEST_TABLE = 'collegedb_manifest';

/** Attempts a manifest write makes before it reports `PLACEMENT_CONFLICT`. @private */
const MAX_CAS_ATTEMPTS = 5;

/**
 * The manifest row as stored.
 * @since 1.4.0
 */
export interface JournaledManifest {
	manifest: PlacementManifest;
	/** Counts writes; the compare-and-swap matches on it */
	revision: number;
	/** The newest epoch number, which is the generation a writer carries */
	generation: number;
}

/**
 * The statements that create the manifest table for one dialect.
 * @since 1.4.0
 */
export function buildManifestSchema(dialect?: SQLDatabase['dialect']): string[] {
	const key = dialect === 'mysql' ? 'VARCHAR(190)' : 'TEXT';
	const long = dialect === 'mysql' ? 'LONGTEXT' : 'TEXT';
	const int = dialect === 'sqlite' || dialect === undefined ? 'INTEGER' : 'BIGINT';

	return [
		`CREATE TABLE IF NOT EXISTS ${MANIFEST_TABLE} (scope ${key} NOT NULL, revision ${int} NOT NULL, generation ${int} NOT NULL, ` +
			`body ${long} NOT NULL, PRIMARY KEY (scope))`
	];
}

const manifestTables = new WeakMap<SQLDatabase, Promise<void>>();

/**
 * Creates the manifest table on the journal shard, once per provider.
 * @since 1.4.0
 */
export async function ensureManifestTable(db: SQLDatabase): Promise<void> {
	let pending = manifestTables.get(db);
	if (!pending) {
		pending = (async () => {
			for (const statement of buildManifestSchema(db.dialect)) {
				await db.prepare(statement).run();
			}
		})();
		manifestTables.set(db, pending);
		// a failed attempt is not remembered, so the next call retries
		pending.catch(() => manifestTables.delete(db));
	}
	await pending;
}

function isManifest(value: unknown): value is PlacementManifest {
	const candidate = value as PlacementManifest | null;
	return !!candidate && candidate.version === 1 && Array.isArray(candidate.epochs) && candidate.epochs.length > 0;
}

/**
 * Reads the manifest from the journal.
 *
 * @param journal - The journal shard and scope
 * @returns The stored manifest with its revision, or `null` when none was written
 * @throws {CollegeDBError} `MANIFEST_CORRUPT` when the stored body is not a manifest
 * @since 1.4.0
 */
export async function loadJournalManifest(journal: PlacementJournal): Promise<JournaledManifest | null> {
	await ensureManifestTable(journal.db);

	const row = await journal.db
		.prepare(`SELECT revision, generation, body FROM ${MANIFEST_TABLE} WHERE scope = ?`)
		.bind(journal.scope)
		.first<{ revision: number | bigint; generation: number | bigint; body: string }>();

	if (!row) {
		return null;
	}

	let body: unknown;
	try {
		body = JSON.parse(row.body);
	} catch {
		body = null;
	}

	if (!isManifest(body)) {
		throw new CollegeDBError(`The manifest stored for journal scope ${journal.scope} is not readable`, 'MANIFEST_CORRUPT');
	}

	return { manifest: body, revision: Number(row.revision), generation: Number(row.generation) };
}

/**
 * What {@link commitJournalManifest} left stored.
 * @since 1.4.0
 */
export interface CommitResult extends JournaledManifest {
	/** Whether this call wrote a new revision */
	changed: boolean;
}

/**
 * Whether a write changed exactly one row. A driver that reports no change count
 * is settled by reading the row back.
 * @private
 */
async function wonWrite(journal: PlacementJournal, result: QueryResult, expect: { revision: number; body: string }): Promise<boolean> {
	if (typeof result.meta.changes === 'number') {
		return result.meta.changes === 1;
	}

	const row = await journal.db
		.prepare(`SELECT revision, body FROM ${MANIFEST_TABLE} WHERE scope = ?`)
		.bind(journal.scope)
		.first<{ revision: number | bigint; body: string }>();
	return !!row && Number(row.revision) === expect.revision && row.body === expect.body;
}

/**
 * Changes the manifest with a compare-and-swap, retrying on a lost race.
 *
 * `mutate` receives the stored manifest (or `null` before the first write) and
 * returns the one to store. Returning the same object it was given writes nothing.
 * The first write is an insert that ignores an existing row, so it cannot clobber
 * one. A lost swap reloads and runs `mutate` again on the winner's manifest, so
 * both changes survive. After {@link MAX_CAS_ATTEMPTS} lost swaps it throws.
 *
 * @param journal - The journal shard and scope
 * @param mutate - Computes the next manifest from the stored one; may throw
 * @returns What is stored after the call
 * @throws {CollegeDBError} `PLACEMENT_CONFLICT` when every attempt lost the swap
 * @since 1.4.0
 */
export async function commitJournalManifest(
	journal: PlacementJournal,
	mutate: (current: PlacementManifest | null) => PlacementManifest
): Promise<CommitResult> {
	for (let attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
		const current = await loadJournalManifest(journal);
		const next = mutate(current?.manifest ?? null);

		if (current && next === current.manifest) {
			return { ...current, changed: false };
		}

		const body = JSON.stringify(next);
		const generation = next.epochs[0]!.n;
		const revision = (current?.revision ?? 0) + 1;

		let result: QueryResult;
		if (current) {
			result = await journal.db
				.prepare(`UPDATE ${MANIFEST_TABLE} SET revision = revision + 1, generation = ?, body = ? WHERE scope = ? AND revision = ?`)
				.bind(generation, body, journal.scope, current.revision)
				.run();
		} else {
			const insert = buildInsertIgnore(MANIFEST_TABLE, { scope: journal.scope, revision, generation, body }, journal.db.dialect);
			result = await journal.db
				.prepare(insert.sql)
				.bind(...insert.bindings)
				.run();
		}

		if (await wonWrite(journal, result, { revision, body })) {
			return { manifest: next, revision, generation, changed: true };
		}
	}

	throw new CollegeDBError(
		`Could not publish the placement manifest: ${MAX_CAS_ATTEMPTS} attempts lost the swap to other writers. Retry the operation.`,
		'PLACEMENT_CONFLICT'
	);
}

/**
 * The statements that raise a shard's fence to `generation`: one inserts the row
 * when the table is empty, the other lifts any lower row. Both only move the
 * value up, so two raisers racing leave the higher one.
 * @since 1.4.0
 */
export function buildFenceRaise(generation: number, dialect?: SQLDatabase['dialect']): BuiltQuery[] {
	const table = quoteIdentifier(TOPOLOGY_TABLE, dialect);
	const column = quoteIdentifier('min_generation', dialect);
	const dual = dialect === 'mysql' ? ' FROM DUAL' : '';
	// Postgres types a bare parameter in a select list as text
	const value = dialect === 'postgres' ? 'CAST(? AS BIGINT)' : '?';

	return [
		{ sql: `INSERT INTO ${table} (${column}) SELECT ${value}${dual} WHERE NOT EXISTS (SELECT 1 FROM ${table})`, bindings: [generation] },
		{ sql: `UPDATE ${table} SET ${column} = ? WHERE ${column} < ?`, bindings: [generation, generation] }
	];
}

/**
 * Raises the fence of one shard. The sidecar tables must exist.
 * @since 1.4.0
 */
export async function raiseFence(db: SQLDatabase, generation: number): Promise<void> {
	const raise = buildFenceRaise(generation, db.dialect);
	const lock = buildFenceLock('exclusive', db.dialect);

	if (lock.length === 0) {
		for (const statement of raise) {
			await db
				.prepare(statement.sql)
				.bind(...statement.bindings)
				.run();
		}
		return;
	}

	assertAtomicBatch(db, `A ${db.dialect} shard`, 'the fence raise cannot wait for writers already in flight');
	// postgres wants the isolation statement ahead of any query in the transaction
	const isolation = db.dialect === 'postgres' ? [{ sql: 'SET TRANSACTION ISOLATION LEVEL READ COMMITTED', bindings: [] }] : [];
	await db.runBatch!([...isolation, ...lock, ...raise]);
}

/**
 * Reads a shard's fence, which is 0 until something raises it.
 * @since 1.4.0
 */
export async function readFence(db: SQLDatabase): Promise<number> {
	const table = quoteIdentifier(TOPOLOGY_TABLE, db.dialect);
	const column = quoteIdentifier('min_generation', db.dialect);
	const row = await db.prepare(`SELECT COALESCE(MAX(${column}), 0) AS fence FROM ${table}`).first<{ fence: number | bigint | null }>();
	return Number(row?.fence ?? 0);
}

/**
 * Options for `pruneEpochs`.
 * @since 1.4.0
 */
export interface PruneOptions {
	/** Every table that holds keyed rows. Required: a census of fewer tables proves nothing */
	tables: string[];
	/** Primary-key column per table; defaults to `keyColumns[table]`, then `id` */
	pkColumns?: Record<string, string>;
	/** Report the dependents per epoch and change nothing, not even the fence */
	dryRun?: boolean;
	/** Rows per census page (default 500) */
	pageSize?: number;
	/** Shard and table pairs scanned at once (default 4) */
	concurrency?: number;
}

/**
 * What `pruneEpochs` found and did.
 * @since 1.4.0
 */
export interface PruneReport {
	/** Epoch numbers dropped from the manifest, newest first */
	removed: number[];
	/** Epoch numbers still in the manifest, newest first */
	retained: number[];
	/** Rows that depend on each epoch: the newest epoch whose owner is the shard the row sits on */
	dependents: Record<number, number>;
	/** Rows one census pass read */
	scanned: number;
}

/**
 * What the census engine needs from the router.
 * @since 1.4.0
 */
export interface PruneEnv {
	journal: PlacementJournal;
	shards: Record<string, SQLDatabase>;
	keyColumns?: Record<string, string>;
	/** The shard an epoch computes for a key */
	epochOwner: (key: string, epoch: PlacementEpoch) => string | null;
	/** Whether the key has an exception and a mapping that points at `shard` */
	isPinned: (key: string, shard: string) => Promise<boolean>;
}

/**
 * Called between the steps of a prune. Tests use it to inject writers.
 * @internal
 * @since 1.4.0
 */
export type PruneStepHook = (step: 'fenced' | 'census-1' | 'census-2') => Promise<void> | void;

let pruneHook: PruneStepHook | null = null;

/**
 * Installs a hook that runs after each prune step. Pass `null` to remove it.
 * The hook is process-wide: it runs for the default instance and every client.
 * @internal
 * @since 1.4.0
 */
export function setPruneStepHook(hook: PruneStepHook | null): void {
	pruneHook = hook;
}

function refused(reason: string): CollegeDBError {
	return new CollegeDBError(`pruneEpochs refused: ${reason}`, 'PRUNE_REFUSED');
}

function describeError(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

async function runPool<T>(items: T[], concurrency: number, work: (item: T) => Promise<void>): Promise<void> {
	let next = 0;
	const lanes = Array.from({ length: Math.max(1, Math.min(concurrency, items.length)) }, async () => {
		while (next < items.length) {
			await work(items[next++]!);
		}
	});
	await Promise.all(lanes);
}

interface Census {
	dependents: Record<number, number>;
	scanned: number;
}

/**
 * Reads every named table on every shard in keyset pages and attributes each row
 * to the newest epoch whose owner is the shard it sits on. A row on a shard no
 * epoch picks, or one pinned by an exception, depends on no epoch.
 * @private
 */
async function census(
	env: PruneEnv,
	manifest: PlacementManifest,
	tables: Array<{ table: string; pk: string }>,
	pageSize: number,
	concurrency: number
): Promise<Census> {
	const dependents: Record<number, number> = Object.fromEntries(manifest.epochs.map((epoch) => [epoch.n, 0]));
	const newest = manifest.epochs[0]!.n;
	let scanned = 0;

	const jobs = Object.keys(env.shards).flatMap((shard) => tables.map(({ table, pk }) => ({ shard, table, pk })));

	await runPool(jobs, concurrency, async ({ shard, table, pk }) => {
		const db = env.shards[shard]!;
		const quotedTable = quoteIdentifier(table, db.dialect);
		const column = quoteIdentifier(pk, db.dialect);
		let last: unknown;

		for (;;) {
			let rows: Array<Record<string, unknown>>;
			try {
				const statement =
					last === undefined
						? db.prepare(`SELECT ${column} AS collegedb_key FROM ${quotedTable} ORDER BY ${column} LIMIT ?`).bind(pageSize)
						: db
								.prepare(`SELECT ${column} AS collegedb_key FROM ${quotedTable} WHERE ${column} > ? ORDER BY ${column} LIMIT ?`)
								.bind(last, pageSize);
				rows = (await statement.all<Record<string, unknown>>()).results;
			} catch (error) {
				throw refused(`could not scan ${table} on ${shard} (${describeError(error)})`);
			}

			for (const row of rows) {
				const key = String(row.collegedb_key);
				scanned++;

				const owner = manifest.epochs.find((epoch) => env.epochOwner(key, epoch) === shard);
				if (!owner) {
					continue;
				}

				if (owner.n !== newest && (await env.isPinned(key, shard))) {
					continue;
				}

				dependents[owner.n]!++;
			}

			if (rows.length < pageSize) {
				return;
			}
			last = rows[rows.length - 1]!.collegedb_key;
		}
	});

	return { dependents, scanned };
}

/**
 * Removes the epochs a census proves nothing depends on.
 *
 * The order is the safety argument. The fence goes up on every shard first, so a
 * writer on an older generation matches no row from then on. The census runs after
 * the fence, so it cannot miss a row such a writer was about to create. It runs a
 * second time, and the manifest is published with a compare-and-swap only when both
 * passes agree and no newer epoch appeared. The newest epoch is never removed.
 *
 * @param env - Shards, journal and the ownership functions
 * @param options - Tables to scan and paging
 * @returns The report, and the manifest that was published (`null` when nothing was)
 * @throws {CollegeDBError} `PRUNE_REFUSED` with the reason when the proof is incomplete
 * @since 1.4.0
 */
export async function runPrune(
	env: PruneEnv,
	options: PruneOptions
): Promise<{ report: PruneReport; published: PlacementManifest | null }> {
	const names = [...new Set(options.tables ?? [])];
	if (names.length === 0) {
		throw refused(
			'tables is empty; name every table that holds keyed rows, because an epoch is only removable when none of them depends on it'
		);
	}

	const pageSize = options.pageSize ?? 500;
	if (!Number.isInteger(pageSize) || pageSize < 1) {
		throw refused(`pageSize must be a positive integer, got ${pageSize}`);
	}
	const concurrency = options.concurrency ?? 4;

	const tables = names.map((table) => ({
		table,
		pk: options.pkColumns?.[table] ?? env.keyColumns?.[table] ?? 'id'
	}));
	for (const { table, pk } of tables) {
		quoteIdentifier(table);
		quoteIdentifier(pk);
	}

	let loaded: JournaledManifest | null;
	try {
		loaded = await loadJournalManifest(env.journal);
	} catch (error) {
		throw refused(`the journal manifest could not be read (${describeError(error)})`);
	}
	if (!loaded) {
		throw refused('the journal holds no placement manifest');
	}

	const manifest = loaded.manifest;
	const generation = manifest.epochs[0]!.n;
	const everyEpoch = manifest.epochs.map((epoch) => epoch.n);

	const bindings = Object.keys(env.shards);
	const unproven = bindings.find((binding) => !isProvenJournalDialect(env.shards[binding]!.dialect));
	if (unproven) {
		throw refused(`shard ${unproven} speaks a dialect the topology fence is not proven on, so a writer there could not be fenced`);
	}

	if (manifest.epochs.length === 1) {
		return { report: { removed: [], retained: everyEpoch, dependents: { [generation]: 0 }, scanned: 0 }, published: null };
	}

	if (!options.dryRun) {
		await runPool(bindings, concurrency, async (binding) => {
			try {
				await raiseFence(env.shards[binding]!, generation);
			} catch (error) {
				throw refused(`could not raise the fence on ${binding} (${describeError(error)})`);
			}
		});
		await pruneHook?.('fenced');
	}

	const first = await census(env, manifest, tables, pageSize, concurrency);
	await pruneHook?.('census-1');

	const removable = manifest.epochs.slice(1).filter((epoch) => first.dependents[epoch.n] === 0);

	if (options.dryRun || removable.length === 0) {
		return {
			report: { removed: [], retained: everyEpoch, dependents: first.dependents, scanned: first.scanned },
			published: null
		};
	}

	const second = await census(env, manifest, tables, pageSize, concurrency);
	await pruneHook?.('census-2');

	if (second.scanned !== first.scanned || JSON.stringify(second.dependents) !== JSON.stringify(first.dependents)) {
		throw refused('the census changed between its two passes, so a writer was active; run it again');
	}

	const drop = new Set(removable.map((epoch) => epoch.n));
	const committed = await commitJournalManifest(env.journal, (current) => {
		if (!current || current.epochs[0]!.n !== generation) {
			throw refused('a newer epoch was published during the census');
		}

		const epochs = current.epochs.filter((epoch) => !drop.has(epoch.n));
		return epochs.length === current.epochs.length ? current : { ...current, epochs };
	});

	return {
		report: {
			removed: removable.map((epoch) => epoch.n),
			retained: committed.manifest.epochs.map((epoch) => epoch.n),
			dependents: first.dependents,
			scanned: first.scanned
		},
		published: committed.manifest
	};
}
