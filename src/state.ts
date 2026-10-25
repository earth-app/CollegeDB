import { currentScope } from './async-context';
import type { KVShardMapper } from './kvmap';
import { createPlacementCache, defaultPlacementCache, type PlacementCache, type PlacementManifest } from './placement';
import { createPhaseSink, defaultPhaseSink, type PhaseSink } from './telemetry';
import type { CollegeDBConfig } from './types';

/**
 * How a configuration places keys: from KV mappings, or computed from a manifest.
 * @private
 */
export interface PlacementDecision {
	mode: 'computed' | 'kv';
	manifest: PlacementManifest | null;
}

/**
 * Everything the router keeps per configuration. The module-level API runs
 * against one default instance of this; `createClient` and a scoped
 * `collegedb(config, callback)` each make their own, so nothing here is shared
 * between them.
 *
 * @private
 */
export interface RouterState {
	/** The active configuration, with the providers instrumentation wrapped */
	config: CollegeDBConfig | null;
	/** Mapper for the active configuration, kept so its caches survive re-initialization */
	mapper: KVShardMapper | null;
	/** Manifest and exception caches */
	placement: PlacementCache;
	/** Where placement resolves keys from, cleared whenever the configuration changes */
	placementDecision: PlacementDecision | null;
	/** The manifest refresh in flight, shared by every caller that finds the decision expired */
	placementFlight: Promise<PlacementDecision> | null;
	/** Bumped whenever the decision is cleared, so a refresh started earlier cannot store its result */
	placementToken: number;
	/** Set while no shard is eligible: the held epochs serve reads and writes are refused */
	placementReadOnly: boolean;
	/** Shard sizes read for `maxDatabaseSize`, by shard name */
	shardSizeCache: Map<string, { size: number; expiresAt: number }>;
	/** Shards whose unreadable size was already reported to the debug log */
	unmeasuredLogged: Set<string>;
	/** Next shard for the `round-robin` strategy */
	roundRobinIndex: number;
	/** Shard sets whose known-shard registration already ran */
	syncedShardSets: Set<string>;
	/** Schemas `ensureSchema({ once })` already applied */
	ensuredSchemaFingerprints: Set<string>;
	/** Shards the auto-migration check already passed; the migrations module's own cache when `undefined` */
	migrationStatus: Map<string, boolean> | undefined;
	/** Where this configuration's phase spans go */
	sink: PhaseSink;
}

/**
 * Creates a state that shares nothing with any other.
 * @private
 */
export function createRouterState(): RouterState {
	return {
		config: null,
		mapper: null,
		placement: createPlacementCache(),
		placementDecision: null,
		placementFlight: null,
		placementToken: 0,
		placementReadOnly: false,
		shardSizeCache: new Map(),
		unmeasuredLogged: new Set(),
		roundRobinIndex: 0,
		syncedShardSets: new Set(),
		ensuredSchemaFingerprints: new Set(),
		migrationStatus: new Map(),
		sink: createPhaseSink()
	};
}

/** The state behind the module-level API, using the module-level placement cache, migration cache and phase observer. @private */
export const defaultState: RouterState = {
	...createRouterState(),
	placement: defaultPlacementCache,
	migrationStatus: undefined,
	sink: defaultPhaseSink
};

/**
 * The state the caller runs against: the scope `collegedb(config, callback)`
 * opened, or the default instance.
 * @private
 */
export function currentState(): RouterState {
	return currentScope<RouterState>() ?? defaultState;
}
