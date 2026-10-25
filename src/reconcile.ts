import { CollegeDBError } from './errors';
import { KVShardMapper } from './kvmap';
import { candidateShards, loadPlacementManifest, withCurrentTopology, type PlacementCache } from './placement';
import { quoteIdentifier, validateIdentifier } from './query';
import { currentState } from './state';
import type { CollegeDBConfig } from './types';

const DEFAULT_PAGE_SIZE = 100;
const PRUNE_BATCH_SIZE = 128;
const REPORT_KEY_CAP = 1000;
const PROBE_CONCURRENCY = 8;

type CoordinatorStub = ReturnType<NonNullable<CollegeDBConfig['coordinator']>['get']>;

type VerifyTables = Array<{ table: string; pkColumn?: string }>;

// verified: a listed table has the key. missing: none does. unchecked: no answer either way.
type Probe = 'verified' | 'missing' | 'unchecked';

interface LedgerRow {
	primaryKey: string;
	shard: string;
}

interface LedgerPage {
	allocations: LedgerRow[];
	next: string | null;
}

interface RoutedRow {
	primaryKey: string;
	ledgerShard: string;
	mapped: boolean;
	recordShard: string | undefined;
}

interface ProbedRow extends RoutedRow {
	probe: Probe;
}

/**
 * Options for {@link reconcileAllocations}. Every write is off by default.
 *
 * @since 1.4.0
 */
export interface ReconcileOptions {
	/** Ledger rows per coordinator page. Defaults to 100. */
	limit?: number;
	/**
	 * Write a KV mapping from the ledger shard for keys that have none, but only when
	 * the row exists on that shard. Requires `verifyRecords`.
	 */
	repair?: boolean;
	/**
	 * Remove ledger rows that agree with routing and have an explicit KV mapping. With
	 * `verifyRecords`, the row must also exist on the routed shard.
	 */
	pruneVerified?: boolean;
	/**
	 * Probe the routed shard for each key's row in the listed tables. Without this, no
	 * shard is queried. `pkColumn` defaults to `id`.
	 */
	verifyRecords?: { tables: Array<{ table: string; pkColumn?: string }> };
	/** Checked between pages. An aborted signal rejects the run. */
	signal?: AbortSignal;
}

/**
 * Result of {@link reconcileAllocations}. Key lists hold at most 1000 entries; the counts are exact.
 *
 * @since 1.4.0
 */
export interface ReconcileReport {
	/** Ledger rows read. */
	examined: number;
	/**
	 * Rows whose ledger shard matches the shard routing points at (the KV mapping, or the
	 * computed owner when there is no mapping). This compares metadata only.
	 */
	routingAgreed: number;
	/** Rows with a matching primary key in a listed table on the routed shard. Zero without `verifyRecords`. */
	recordVerified: number;
	/** Keys with no matching row in any listed table on the routed shard. */
	recordMissing: string[];
	/** Total keys in `recordMissing`, including those past the list cap. */
	recordMissingCount: number;
	/**
	 * Rows the run could not check. Without `verifyRecords` this is every examined row. With
	 * it, this counts probes that threw, shards that are not configured, and runs with no tables.
	 */
	recordUnchecked: number;
	/** Keys with no KV mapping and no computed owner. */
	missingMapping: string[];
	/** Total keys in `missingMapping`, including those past the list cap. */
	missingMappingCount: number;
	/** Keys whose ledger shard differs from their mapping or computed owner. */
	disagreeing: Array<{ key: string; ledgerShard: string; recordShard: string }>;
	/** Total keys in `disagreeing`, including those past the list cap. */
	disagreeingCount: number;
	/** Mappings written by `repair`. */
	repaired: number;
	/** Ledger rows removed by `pruneVerified`. */
	pruned: number;
}

/**
 * Compares the coordinator's allocation ledger with routing metadata: the KV mapping, or
 * the computed owner when a key has no mapping. Observational by default and off the read path.
 *
 * With `verifyRecords`, each row is also probed for its primary key on the routed shard
 * (the ledger shard when a key has no mapping). A row that exists does not prove it is the
 * authoritative copy. A shard holding a tombstone can keep a retained row, and the journal
 * is the authority for moved keys. Disagreements are reported and never resolved.
 *
 * `repair` writes a KV mapping for keys that have none, and it needs `verifyRecords`.
 * `pruneVerified` removes ledger rows that agree with routing and have an explicit mapping.
 *
 * @param options - Page size, write flags, record probes and an abort signal
 * @returns Promise resolving to the reconciliation report
 * @throws {CollegeDBError} `COORDINATOR_NOT_CONFIGURED` when no coordinator is configured
 * @throws {CollegeDBError} `RECONCILE_VERIFY_REQUIRED` when `repair` is set without `verifyRecords`
 * @throws {CollegeDBError} `INVALID_IDENTIFIER` when a listed table or key column is not a bare identifier
 * @since 1.4.0
 * @example
 * ```typescript
 * const report = await reconcileAllocations({
 * 	pruneVerified: true,
 * 	verifyRecords: { tables: [{ table: 'users' }] }
 * });
 * console.log(report.routingAgreed, report.recordMissingCount);
 * ```
 */
export function reconcileAllocations(options: ReconcileOptions = {}): Promise<ReconcileReport> {
	const state = currentState();
	return reconcileAllocationsIn(state.config, state.placement, options);
}

/**
 * {@link reconcileAllocations} against an explicit configuration and placement
 * cache, so a client reads its own manifest.
 * @internal
 * @since 1.4.0
 */
export async function reconcileAllocationsIn(
	config: CollegeDBConfig | null,
	placement: PlacementCache,
	options: ReconcileOptions = {}
): Promise<ReconcileReport> {
	if (!config?.coordinator) {
		throw new CollegeDBError('reconcileAllocations needs a coordinator in the config', 'COORDINATOR_NOT_CONFIGURED');
	}

	const { limit = DEFAULT_PAGE_SIZE, repair = false, pruneVerified = false, verifyRecords, signal } = options;
	if (repair && !verifyRecords) {
		throw new CollegeDBError('reconcileAllocations repair needs verifyRecords to confirm each row exists', 'RECONCILE_VERIFY_REQUIRED');
	}
	for (const { table, pkColumn = 'id' } of verifyRecords?.tables ?? []) {
		validateIdentifier(table);
		validateIdentifier(pkColumn);
	}

	const coordinator = config.coordinator.get(config.coordinator.idFromName(config.coordinatorScope ?? 'default'));
	const mapper = new KVShardMapper(config.kv, {
		hashShardMappings: config.hashShardMappings,
		mappingCacheTtlMs: config.mappingCacheTtlMs,
		knownShardsCacheTtlMs: config.knownShardsCacheTtlMs
	});
	const shards = Object.keys(config.shards);
	const stored =
		config.placement === 'computed' ? await loadPlacementManifest(config.kv, undefined, undefined, undefined, placement) : null;
	const manifest = stored ? withCurrentTopology(stored, shards, 'hrw') : null;

	const report: ReconcileReport = {
		examined: 0,
		routingAgreed: 0,
		recordVerified: 0,
		recordMissing: [],
		recordMissingCount: 0,
		recordUnchecked: 0,
		missingMapping: [],
		missingMappingCount: 0,
		disagreeing: [],
		disagreeingCount: 0,
		repaired: 0,
		pruned: 0
	};

	let after = '';
	for (;;) {
		if (signal?.aborted) throw signal.reason;

		const page = await readPage(coordinator, after, limit);
		const routed: RoutedRow[] = [];
		for (const { primaryKey, shard: ledgerShard } of page.allocations) {
			const mapping = await mapper.getShardMapping(primaryKey);
			routed.push({
				primaryKey,
				ledgerShard,
				mapped: mapping !== null,
				recordShard: mapping?.shard ?? (manifest ? candidateShards(primaryKey, manifest, shards)[0] : undefined)
			});
		}

		const probed: ProbedRow[] = verifyRecords
			? await probeRows(config, routed, verifyRecords.tables)
			: routed.map((row) => ({ ...row, probe: 'unchecked' }));
		const verified: string[] = [];

		for (const row of probed) {
			report.examined++;

			if (row.probe === 'verified') {
				report.recordVerified++;
			} else if (row.probe === 'missing') {
				report.recordMissingCount++;
				if (report.recordMissing.length < REPORT_KEY_CAP) report.recordMissing.push(row.primaryKey);
			} else {
				report.recordUnchecked++;
			}

			if (row.recordShard === undefined) {
				report.missingMappingCount++;
				if (report.missingMapping.length < REPORT_KEY_CAP) report.missingMapping.push(row.primaryKey);

				// verified means the probe reached the ledger shard, which is configured
				if (repair && row.probe === 'verified') {
					await mapper.setShardMapping(row.primaryKey, row.ledgerShard);
					report.repaired++;
				}
			} else if (row.recordShard === row.ledgerShard) {
				report.routingAgreed++;
				if (row.mapped && (!verifyRecords || row.probe === 'verified')) verified.push(row.primaryKey);
			} else {
				report.disagreeingCount++;
				if (report.disagreeing.length < REPORT_KEY_CAP) {
					report.disagreeing.push({ key: row.primaryKey, ledgerShard: row.ledgerShard, recordShard: row.recordShard });
				}
			}
		}

		if (pruneVerified && verified.length > 0) {
			report.pruned += await prune(coordinator, verified);
		}

		if (page.next === null) return report;
		after = page.next;
	}
}

async function readPage(coordinator: CoordinatorStub, after: string, limit: number): Promise<LedgerPage> {
	const response = await coordinator.fetch(`http://coordinator/allocations?after=${encodeURIComponent(after)}&limit=${limit}`);
	if (!response.ok) {
		throw new CollegeDBError(`Coordinator returned ${response.status} while listing allocations`, 'COORDINATOR_ERROR');
	}
	return (await response.json()) as LedgerPage;
}

async function prune(coordinator: CoordinatorStub, primaryKeys: string[]): Promise<number> {
	let pruned = 0;
	for (let i = 0; i < primaryKeys.length; i += PRUNE_BATCH_SIZE) {
		const response = await coordinator.fetch('http://coordinator/allocations/prune', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ primaryKeys: primaryKeys.slice(i, i + PRUNE_BATCH_SIZE) })
		});
		if (!response.ok) {
			throw new CollegeDBError(`Coordinator returned ${response.status} while pruning allocations`, 'COORDINATOR_ERROR');
		}
		pruned += ((await response.json()) as { pruned: number }).pruned;
	}
	return pruned;
}

async function probeRows(config: CollegeDBConfig, rows: RoutedRow[], tables: VerifyTables): Promise<ProbedRow[]> {
	const probed: ProbedRow[] = [];
	for (let i = 0; i < rows.length; i += PROBE_CONCURRENCY) {
		const chunk = rows.slice(i, i + PROBE_CONCURRENCY);
		probed.push(
			...(await Promise.all(
				chunk.map(async (row) => ({
					...row,
					probe: await probeRecord(config, row.recordShard ?? row.ledgerShard, row.primaryKey, tables)
				}))
			))
		);
	}
	return probed;
}

async function probeRecord(config: CollegeDBConfig, shard: string, primaryKey: string, tables: VerifyTables): Promise<Probe> {
	const db = config.shards[shard];
	if (!db || tables.length === 0) return 'unchecked';

	let unchecked = false;
	for (const { table, pkColumn = 'id' } of tables) {
		try {
			const row = await db
				.prepare(`SELECT 1 FROM ${quoteIdentifier(table, db.dialect)} WHERE ${quoteIdentifier(pkColumn, db.dialect)} = ? LIMIT 1`)
				.bind(primaryKey)
				.first();
			if (row) return 'verified';
		} catch {
			unchecked = true;
		}
	}
	return unchecked ? 'unchecked' : 'missing';
}
