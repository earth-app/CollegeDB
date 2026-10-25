import { CollegeDBError } from './errors';
import { KVShardMapper } from './kvmap';
import { candidateShards, loadPlacementManifest, withCurrentTopology, type PlacementCache } from './placement';
import { currentState } from './state';
import type { CollegeDBConfig } from './types';

const DEFAULT_PAGE_SIZE = 100;
const PRUNE_BATCH_SIZE = 128;
const REPORT_KEY_CAP = 1000;

type CoordinatorStub = ReturnType<NonNullable<CollegeDBConfig['coordinator']>['get']>;

interface LedgerRow {
	primaryKey: string;
	shard: string;
}

interface LedgerPage {
	allocations: LedgerRow[];
	next: string | null;
}

/**
 * Options for {@link reconcileAllocations}. Every write is off by default.
 *
 * @since 1.4.0
 */
export interface ReconcileOptions {
	/** Ledger rows per coordinator page. Defaults to 100. */
	limit?: number;
	/** Write a KV mapping from the ledger for keys that have no record. */
	repair?: boolean;
	/** Remove ledger rows whose shard matches their KV mapping. */
	pruneVerified?: boolean;
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
	/** Rows whose ledger shard is where the key lives. */
	agreed: number;
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
 * Compares the coordinator's allocation ledger with the shard each key lives on.
 *
 * The ledger is advisory: the KV mapping, or the computed placement when there is
 * no mapping, is the record. Observational by default. `repair` writes a mapping for
 * keys missing one, using the ledger shard when it is configured. `pruneVerified`
 * removes ledger rows that match their KV mapping (computed-only matches stay).
 * Disagreements are reported and never resolved.
 *
 * @param options - Page size, write flags and an abort signal
 * @returns Promise resolving to the reconciliation report
 * @throws {CollegeDBError} `COORDINATOR_NOT_CONFIGURED` when no coordinator is configured
 * @since 1.4.0
 * @example
 * ```typescript
 * const report = await reconcileAllocations({ pruneVerified: true });
 * console.log(report.agreed, report.disagreeingCount);
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

	const { limit = DEFAULT_PAGE_SIZE, repair = false, pruneVerified = false, signal } = options;
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
		agreed: 0,
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
		const verified: string[] = [];

		for (const { primaryKey, shard: ledgerShard } of page.allocations) {
			report.examined++;

			const mapping = await mapper.getShardMapping(primaryKey);
			const recordShard = mapping?.shard ?? (manifest ? candidateShards(primaryKey, manifest, shards)[0] : undefined);

			if (recordShard === undefined) {
				report.missingMappingCount++;
				if (report.missingMapping.length < REPORT_KEY_CAP) report.missingMapping.push(primaryKey);

				// an unconfigured ledger shard would point the mapping at nothing
				if (repair && config.shards[ledgerShard]) {
					await mapper.setShardMapping(primaryKey, ledgerShard);
					report.repaired++;
				}
			} else if (recordShard === ledgerShard) {
				report.agreed++;
				if (mapping) verified.push(primaryKey);
			} else {
				report.disagreeingCount++;
				if (report.disagreeing.length < REPORT_KEY_CAP) {
					report.disagreeing.push({ key: primaryKey, ledgerShard, recordShard });
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
