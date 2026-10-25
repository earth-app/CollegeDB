/**
 * @fileoverview Shard placement as a function of the primary key.
 *
 * The `hash` strategy has always derived the shard from the key, yet every
 * routed operation still read the mapping back out of KV and every new key
 * still wrote one. This module makes the derivation authoritative so both round
 * trips disappear, and carries the machinery that keeps that safe: a versioned
 * manifest of shard sets, an epoch walk for keys placed under an earlier
 * topology or an earlier algorithm, and an exception set for keys deliberately
 * moved off their computed shard by `reassignShard`.
 *
 * Placement uses rendezvous hashing (highest random weight). The scheme it
 * replaces, `hash(key) % shards.length`, reassigns almost every key when the
 * shard count changes: Thaler and Ravishankar give the disruption coefficient
 * as `(m-1)/m` (*Using Name-Based Mappings to Increase Hit Rates*, IEEE/ACM
 * Transactions on Networking 6(1), 1998, section IV.A). Rendezvous hashing
 * moves only the keys that must move, and its result does not depend on the
 * order the shards were declared in.
 *
 * Everything here is synchronous and free of I/O except
 * {@link loadPlacementManifest} and {@link savePlacementManifest}, which take a
 * KV store as an argument rather than reading global configuration.
 *
 * @author Gregory Mitchell
 * @since 1.3.0
 */

import { CollegeDBError } from './errors';
import { buildInsertIgnore, isProvenJournalDialect } from './query';
import { commitJournalManifest, loadJournalManifest, type JournaledManifest } from './topology';
import type { KVStorage, SQLDatabase } from './types';

/** KV key holding the placement manifest. @private */
const MANIFEST_KEY = 'collegedb:placement:manifest';

/** KV key holding the set of keys placed off their computed shard. @private */
const EXCEPTIONS_KEY = 'collegedb:placement:exceptions';

/** How long a loaded manifest is reused before being re-read. @private */
const DEFAULT_MANIFEST_TTL_MS = 10_000;

/**
 * How the shard for a key was derived during one epoch.
 *
 * - `hrw` - rendezvous hashing, the current scheme
 * - `modulo` - `hash(key) % shards.length` over `shards` in the recorded order,
 *   the scheme used before 1.4.0
 * @since 1.3.0
 */
export type PlacementAlgorithm = 'hrw' | 'modulo';

/**
 * One generation of the shard topology.
 *
 * An epoch records the algorithm as well as the shard set, so a change of
 * algorithm is walked back exactly like a change of topology.
 * @since 1.3.0
 */
export interface PlacementEpoch {
	/** Monotonic generation number; higher is newer */
	n: number;
	/** How placement was computed during this epoch */
	algorithm: PlacementAlgorithm;
	/** Shard bindings that existed during this epoch, in declaration order */
	shards: string[];
}

/**
 * The manifest: every epoch CollegeDB still needs to be able to resolve.
 * @since 1.3.0
 */
export interface PlacementManifest {
	/** Manifest schema version */
	version: 1;
	/** Epochs, newest first */
	epochs: PlacementEpoch[];
	/** Bumped whenever the exception set changes, so isolates can notice cheaply */
	exceptionsVersion: number;
}

/**
 * Most epochs a manifest may hold, current one included.
 *
 * A key physically placed under an epoch needs that epoch to be found, so an
 * epoch is never dropped to make room. Creating one past this ceiling throws
 * `TOO_MANY_EPOCHS`. Each epoch costs one extra query on a read that misses
 * everywhere, and a key that resolves through an old epoch is memoized as an
 * exception on first hit and never walks again.
 * @since 1.3.0
 */
export const MAX_RETAINED_EPOCHS = 8;

type CachedManifest = { manifest: PlacementManifest; expiresAt: number };
type CachedExceptions = { keys: Set<string>; version: number; expiresAt: number };

/**
 * The manifest and exception set one configuration holds, and when a read of
 * them last failed. The default instance uses {@link defaultPlacementCache}; a
 * client makes its own, so two clients never serve each other's topology.
 * @since 1.4.0
 */
export interface PlacementCache {
	/** The loaded manifest and when it expires */
	manifest: CachedManifest | null;
	/** The loaded exception set, its version and when it expires */
	exceptions: CachedExceptions | null;
	/** When a manifest or exception read last failed, or 0 */
	failedAt: number;
}

/** Creates an empty cache. @since 1.4.0 */
export function createPlacementCache(): PlacementCache {
	return { manifest: null, exceptions: null, failedAt: 0 };
}

/** The cache every placement function uses when it is not handed one. @since 1.4.0 */
export const defaultPlacementCache: PlacementCache = createPlacementCache();

/** How long stale-allowed reads serve cached state after a failure instead of retrying. @private */
const FAILURE_BACKOFF_MS = 1000;

/**
 * Clears the cached manifest and exception set.
 *
 * Called on `initialize` and `resetConfig` so a reconfigured process never
 * resolves against another deployment's topology.
 * @param cache - The cache to clear; the default instance's when omitted
 * @since 1.3.0
 */
export function resetPlacementState(cache: PlacementCache = defaultPlacementCache): void {
	cache.manifest = null;
	cache.exceptions = null;
	cache.failedAt = 0;
}

/**
 * Whether a manifest or exception read failed within the last second. While it
 * did, a stale-allowed read serves what it holds instead of waiting out another
 * timeout. Writes ignore it.
 * @since 1.4.0
 */
export function isPlacementBackingOff(cache: PlacementCache = defaultPlacementCache): boolean {
	return cache.failedAt > 0 && Date.now() - cache.failedAt < FAILURE_BACKOFF_MS;
}

/**
 * The cached manifest and exception set of one isolate.
 * @internal
 * @since 1.4.0
 */
export interface PlacementCacheSnapshot {
	manifest: CachedManifest | null;
	exceptions: CachedExceptions | null;
}

/**
 * Installs `next` as the placement cache (empty when omitted) and returns the
 * one it replaces. Tests use it to play two isolates over one KV store.
 * @internal
 * @since 1.4.0
 */
export function swapPlacementCache(next?: PlacementCacheSnapshot, cache: PlacementCache = defaultPlacementCache): PlacementCacheSnapshot {
	const previous = { manifest: cache.manifest, exceptions: cache.exceptions };
	cache.manifest = next?.manifest ?? null;
	cache.exceptions = next?.exceptions ?? null;
	return previous;
}

/**
 * Whether the cached manifest was loaded or written within its TTL.
 * @since 1.4.0
 */
export function isPlacementManifestFresh(cache: PlacementCache = defaultPlacementCache): boolean {
	return cache.manifest !== null && cache.manifest.expiresAt >= Date.now();
}

/**
 * Options for {@link loadPlacementManifest} and {@link loadPlacementExceptions}.
 * @since 1.4.0
 */
export interface PlacementLoadOptions {
	/** Return a cached copy past its TTL when KV fails, instead of throwing */
	staleOk?: boolean;
	/** Read KV even when a copy is within its TTL */
	fresh?: boolean;
}

/**
 * Builds the retryable error for placement state that KV or the journal could
 * not confirm.
 * @since 1.4.0
 */
export function placementUnverified(what: string, cause: unknown, source: 'KV' | 'the journal' = 'KV'): CollegeDBError {
	const reason = cause instanceof Error ? cause.message : String(cause);
	return new CollegeDBError(
		`Could not verify the placement ${what} because ${source} failed (${reason}). Nothing was routed; retry the operation.`,
		'PLACEMENT_UNVERIFIED'
	);
}

/**
 * The journal that holds the manifest, or `undefined` when there is none or its
 * dialect is not one the journal is proven on (the KV copy stays authoritative).
 * @private
 */
function manifestJournal(journal: PlacementJournal | undefined): PlacementJournal | undefined {
	return journal && isProvenJournalDialect(journal.db.dialect) ? journal : undefined;
}

/**
 * Refreshes the KV copy of a manifest the journal holds and the local cache. The
 * journal is the record, so a failed KV write does not fail the caller.
 * @private
 */
async function deriveManifestCopy(
	kv: KVStorage,
	manifest: PlacementManifest,
	writeKV: boolean = true,
	cache: PlacementCache = defaultPlacementCache
): Promise<void> {
	if (writeKV) {
		try {
			await kv.put(MANIFEST_KEY, JSON.stringify(manifest));
		} catch {
			// the next publish writes it again
		}
	}
	cache.manifest = { manifest, expiresAt: Date.now() + DEFAULT_MANIFEST_TTL_MS };
}

/**
 * 32-bit string hash: FNV-1a followed by the MurmurHash3 finalizer.
 *
 * FNV-1a alone leaves low-bit structure that shows up as uneven shard weights;
 * the finalizer avalanches it. `Math.imul` keeps the multiply in 32-bit space.
 * @private
 */
function hash32(value: string): number {
	let h = 0x811c9dc5;

	for (let i = 0; i < value.length; i++) {
		h ^= value.charCodeAt(i);
		h = Math.imul(h, 0x01000193);
	}

	h ^= h >>> 16;
	h = Math.imul(h, 0x85ebca6b);
	h ^= h >>> 13;
	h = Math.imul(h, 0xc2b2ae35);
	h ^= h >>> 16;

	return h >>> 0;
}

/**
 * The legacy 32-bit key hash, preserved bit-for-bit so the `modulo` epoch
 * resolves keys the way the version that placed them did.
 * @private
 */
function legacyHash(key: string): number {
	let hash = 0;

	for (let i = 0; i < key.length; i++) {
		const char = key.charCodeAt(i);
		hash = (hash << 5) - hash + char;
		hash = hash & hash;
	}

	return hash;
}

/**
 * Selects a shard for `key` using rendezvous hashing.
 *
 * Each candidate is scored with a hash of the shard name combined with the key,
 * and the highest score wins, with the shard name breaking exact ties. Adding
 * or removing a shard only moves the keys whose winner changed, and the result
 * is independent of the order `shards` is given in.
 *
 * @param key - Primary key to place
 * @param shards - Candidate shard bindings (at least one)
 * @returns The winning shard binding
 * @throws {Error} If `shards` is empty
 * @since 1.3.0
 * @example
 * ```typescript
 * hrwShard('user-123', ['db-east', 'db-west']); // => 'db-east'
 * ```
 */
export function hrwShard(key: string, shards: string[]): string {
	if (shards.length === 0) {
		throw new Error('hrwShard requires at least one shard');
	}

	let bestShard = shards[0]!;
	let bestWeight = hash32(`${bestShard}:${key}`);

	for (let i = 1; i < shards.length; i++) {
		const shard = shards[i]!;
		const weight = hash32(`${shard}:${key}`);

		if (weight > bestWeight || (weight === bestWeight && shard < bestShard)) {
			bestShard = shard;
			bestWeight = weight;
		}
	}

	return bestShard;
}

/**
 * Selects a shard the way CollegeDB did before 1.4.0.
 *
 * Retained only so the `modulo` epoch can resolve keys placed by an earlier
 * version. Do not use for new placement: it is sensitive to the order `shards`
 * is declared in, and changing the shard count relocates almost every key.
 *
 * @param key - Primary key to place
 * @param shards - Candidate shard bindings, in the order the old config had them
 * @returns The shard binding the pre-1.4.0 algorithm would have chosen
 * @throws {Error} If `shards` is empty
 * @since 1.3.0
 */
export function legacyModuloShard(key: string, shards: string[]): string {
	if (shards.length === 0) {
		throw new Error('legacyModuloShard requires at least one shard');
	}

	const index = Math.abs(legacyHash(key)) % shards.length;
	return shards[index] ?? shards[0]!;
}

/**
 * Applies one epoch's algorithm to a key.
 *
 * @param key - Primary key to place
 * @param epoch - The epoch to resolve against
 * @returns The shard binding, or `null` when the epoch recorded no shards
 * @since 1.3.0
 */
export function shardForEpoch(key: string, epoch: PlacementEpoch): string | null {
	if (epoch.shards.length === 0) {
		return null;
	}

	return epoch.algorithm === 'hrw' ? hrwShard(key, epoch.shards) : legacyModuloShard(key, epoch.shards);
}

/**
 * Every shard a key could be on, newest epoch first, without repeats.
 *
 * The first entry is where a write goes and where a read looks first. The rest
 * are the epoch walk: the shards an earlier topology or an earlier algorithm
 * would have chosen, tried only when the row is not where it should be.
 *
 * Shards absent from `available` are dropped, since a query cannot be sent to a
 * binding this process does not hold.
 *
 * @param key - Primary key to place
 * @param manifest - The placement manifest
 * @param available - Shard bindings currently configured
 * @returns Candidate shard bindings in resolution order
 * @since 1.3.0
 */
export function candidateShards(key: string, manifest: PlacementManifest, available: string[]): string[] {
	const configured = new Set(available);
	const out: string[] = [];

	for (const epoch of manifest.epochs) {
		const shard = shardForEpoch(key, epoch);
		if (shard && configured.has(shard) && !out.includes(shard)) {
			out.push(shard);
		}
	}

	return out;
}

/**
 * Builds the manifest a deployment starts life with.
 *
 * `algorithm` is the caller's decision, and it is the whole upgrade story:
 * a deployment CollegeDB has never seen starts at `hrw` and computes placements
 * immediately, while a deployment that already has mappings starts at `modulo`
 * so the keys an earlier version placed still resolve.
 *
 * @param shards - Currently configured shard bindings
 * @param algorithm - Algorithm for the first epoch
 * @returns A single-epoch manifest
 * @since 1.3.0
 */
export function createManifest(shards: string[], algorithm: PlacementAlgorithm): PlacementManifest {
	return {
		version: 1,
		epochs: [{ n: 1, algorithm, shards: [...shards] }],
		exceptionsVersion: 0
	};
}

/**
 * Returns a manifest whose newest epoch matches `shards` under `hrw`.
 *
 * When the newest epoch already describes exactly this shard set with this
 * algorithm the manifest is returned unchanged, so this is safe to call on
 * every initialization. Otherwise a new epoch is prepended and every earlier
 * epoch is kept, because a key may still sit where only that epoch would look.
 *
 * @param manifest - The current manifest
 * @param shards - Currently configured shard bindings
 * @param algorithm - Algorithm for the new epoch
 * @returns The manifest to store, or the original when nothing changed
 * @throws {CollegeDBError} `TOO_MANY_EPOCHS` when the manifest already holds `MAX_RETAINED_EPOCHS` epochs
 * @since 1.3.0
 */
export function withCurrentTopology(
	manifest: PlacementManifest,
	shards: string[],
	algorithm: PlacementAlgorithm = 'hrw'
): PlacementManifest {
	const newest = manifest.epochs[0];
	const sorted = [...shards].sort();

	if (newest && newest.algorithm === algorithm && sameShardSet(newest.shards, sorted)) {
		return manifest;
	}

	if (manifest.epochs.length >= MAX_RETAINED_EPOCHS) {
		throw new CollegeDBError(
			`The placement manifest already holds the maximum of ${MAX_RETAINED_EPOCHS} epochs. ` +
				'Dropping the oldest would strand keys placed under it. Converge first: move keys to their current owners, ' +
				'then prune epochs after a verified migration.',
			'TOO_MANY_EPOCHS'
		);
	}

	const epoch: PlacementEpoch = {
		n: (newest?.n ?? 0) + 1,
		algorithm,
		shards: [...shards]
	};

	return {
		version: 1,
		epochs: [epoch, ...manifest.epochs],
		exceptionsVersion: manifest.exceptionsVersion
	};
}

/**
 * Compares a shard list against an already-sorted list, ignoring order.
 * @private
 */
function sameShardSet(left: string[], sortedRight: string[]): boolean {
	if (left.length !== sortedRight.length) {
		return false;
	}

	const sortedLeft = [...left].sort();
	for (let i = 0; i < sortedLeft.length; i++) {
		if (sortedLeft[i] !== sortedRight[i]) {
			return false;
		}
	}

	return true;
}

/**
 * Reads the placement manifest, reusing a recently loaded copy.
 *
 * One read per isolate per `ttlMs` replaces one read per operation, which is
 * the saving computed placement exists to capture.
 *
 * A store with `getMany` returns the manifest and the exception set in one
 * round trip, and the exception set is cached under the manifest's version so
 * the first {@link loadPlacementExceptions} call needs no read.
 *
 * A failed read throws `PLACEMENT_UNVERIFIED`; it is never reported as a missing
 * manifest. With `staleOk`, a copy past its TTL is returned instead.
 *
 * With a `journal`, the journal copy is read first and KV is not consulted when
 * it has one. KV is eventually consistent and can return nothing for a manifest
 * that exists, so only a journal with no manifest yet falls through to the KV
 * copy, which seeds the first journal write.
 *
 * @param kv - KV store holding the manifest
 * @param ttlMs - How long a loaded manifest may be reused
 * @param options - Stale and cache behavior
 * @param journal - Journal shard and scope, when one is configured
 * @param cache - The cache to read and fill; the default instance's when omitted
 * @returns The stored manifest, or `null` when this deployment has none
 * @throws {CollegeDBError} `PLACEMENT_UNVERIFIED` when KV or the journal fails and no stale copy is allowed or held
 * @since 1.3.0
 */
export async function loadPlacementManifest(
	kv: KVStorage,
	ttlMs: number = DEFAULT_MANIFEST_TTL_MS,
	options: PlacementLoadOptions = {},
	journal?: PlacementJournal,
	cache: PlacementCache = defaultPlacementCache
): Promise<PlacementManifest | null> {
	if (!options.fresh && cache.manifest && cache.manifest.expiresAt >= Date.now()) {
		return cache.manifest.manifest;
	}

	const authority = manifestJournal(journal);
	if (authority) {
		let row: JournaledManifest | null;

		try {
			row = await loadJournalManifest(authority);
		} catch (error) {
			cache.failedAt = Date.now();
			if (options.staleOk && cache.manifest) {
				return cache.manifest.manifest;
			}

			throw error instanceof CollegeDBError ? error : placementUnverified('manifest', error, 'the journal');
		}

		if (row) {
			if (ttlMs > 0) {
				cache.manifest = { manifest: row.manifest, expiresAt: Date.now() + ttlMs };
			}

			return row.manifest;
		}
	}

	let stored: PlacementManifest | null | undefined;

	try {
		if (kv.getMany && ttlMs > 0) {
			const [manifest, exceptions] = await kv.getMany<unknown>([MANIFEST_KEY, EXCEPTIONS_KEY], 'json');
			stored = manifest as PlacementManifest | null;

			if (isManifest(stored)) {
				const keys = new Set(Array.isArray(exceptions) ? (exceptions as string[]) : []);
				cache.exceptions = { keys, version: stored.exceptionsVersion, expiresAt: Date.now() + ttlMs };
			}
		} else {
			stored = await kv.get<PlacementManifest>(MANIFEST_KEY, 'json');
		}
	} catch (error) {
		cache.failedAt = Date.now();
		if (options.staleOk && cache.manifest) {
			return cache.manifest.manifest;
		}

		throw placementUnverified('manifest', error);
	}

	if (!isManifest(stored)) {
		return null;
	}

	if (ttlMs > 0) {
		cache.manifest = { manifest: stored, expiresAt: Date.now() + ttlMs };
	}

	return stored;
}

/** @private */
function isManifest(value: PlacementManifest | null | undefined): value is PlacementManifest {
	return !!value && value.version === 1 && Array.isArray(value.epochs) && value.epochs.length > 0;
}

/**
 * Writes the placement manifest and refreshes the local copy.
 *
 * @param kv - KV store to write to
 * @param manifest - Manifest to store
 * @param ttlMs - How long the written manifest may be reused locally
 * @param cache - The cache to refresh; the default instance's when omitted
 * @since 1.3.0
 */
export async function savePlacementManifest(
	kv: KVStorage,
	manifest: PlacementManifest,
	ttlMs: number = DEFAULT_MANIFEST_TTL_MS,
	cache: PlacementCache = defaultPlacementCache
): Promise<void> {
	await kv.put(MANIFEST_KEY, JSON.stringify(manifest));
	cache.manifest = ttlMs > 0 ? { manifest, expiresAt: Date.now() + ttlMs } : null;
}

/**
 * Publishes the manifest whose newest epoch matches `shards`, and returns it.
 *
 * `stored` is what {@link loadPlacementManifest} returned. Without a journal, a
 * manifest that already fits the shard set is returned as it is, and any other is
 * written to KV. With a journal, the change goes in through a compare-and-swap
 * that runs again on whatever another client published first, so concurrent
 * publishers each add their epoch and neither overwrites the other. It also seeds
 * a journal that has no manifest from `stored`. KV then holds a derived copy,
 * written only when the manifest changed.
 *
 * @param kv - KV store for the manifest copy
 * @param journal - Journal shard and scope, when one is configured
 * @param stored - The manifest as loaded, or `null` when there is none
 * @param shards - Currently configured shard bindings
 * @param cache - The cache to refresh; the default instance's when omitted
 * @returns The manifest now in force
 * @throws {CollegeDBError} `TOO_MANY_EPOCHS`, or `PLACEMENT_CONFLICT` when every swap lost to another writer
 * @since 1.4.0
 */
export async function publishPlacementTopology(
	kv: KVStorage,
	journal: PlacementJournal | undefined,
	stored: PlacementManifest | null,
	shards: string[],
	cache: PlacementCache = defaultPlacementCache
): Promise<PlacementManifest> {
	return (await publishPlacementEpoch(kv, journal, stored, shards, cache)).manifest;
}

/**
 * Like {@link publishPlacementTopology}, and also says whether this call wrote a
 * manifest. With a journal that is exact: a caller whose swap found another client
 * had already published the same shard set reports `false`. Without one, two
 * clients that publish at once can both report `true`, since a KV write cannot
 * tell.
 *
 * @param kv - KV store for the manifest copy
 * @param journal - Journal shard and scope, when one is configured
 * @param stored - The manifest as loaded, or `null` when there is none
 * @param shards - Shard bindings the newest epoch should name
 * @param cache - The cache to refresh; the default instance's when omitted
 * @returns The manifest now in force and whether this call changed it
 * @throws {CollegeDBError} `TOO_MANY_EPOCHS`, or `PLACEMENT_CONFLICT` when every swap lost to another writer
 * @since 1.4.0
 */
export async function publishPlacementEpoch(
	kv: KVStorage,
	journal: PlacementJournal | undefined,
	stored: PlacementManifest | null,
	shards: string[],
	cache: PlacementCache = defaultPlacementCache
): Promise<{ manifest: PlacementManifest; published: boolean }> {
	const base = stored ?? createManifest(shards, 'hrw');
	const authority = manifestJournal(journal);

	if (authority) {
		// a manifest read from KV is seeded into a journal that has none, so the next lagging read cannot overwrite it
		const committed = await commitJournalManifest(authority, (current) => withCurrentTopology(current ?? base, shards, 'hrw'));
		await deriveManifestCopy(kv, committed.manifest, committed.changed, cache);
		// seeding a journal from the KV copy writes without adding an epoch
		const added = committed.manifest.epochs[0]!.n > (stored?.epochs[0]?.n ?? 0);
		return { manifest: committed.manifest, published: committed.changed && added };
	}

	const refreshed = withCurrentTopology(base, shards, 'hrw');

	if (stored && refreshed === base) {
		return { manifest: stored, published: false };
	}

	await savePlacementManifest(kv, refreshed, undefined, cache);
	return { manifest: refreshed, published: true };
}

/**
 * Reads the set of keys placed off their computed shard.
 *
 * Stored as one JSON array rather than a probabilistic structure: the set only
 * grows through explicit reassignment and lazy absorption of pre-1.4.0 keys, so
 * it stays small, and an exact set cannot produce the false positives that
 * would turn into extra KV reads.
 *
 * @param kv - KV store holding the exception set
 * @param version - Current `exceptionsVersion` from the manifest; a change invalidates the local copy
 * @param ttlMs - How long a loaded set may be reused
 * @param options - With `staleOk`, a cached set past its TTL is returned when KV fails, but only one cached under this `version`
 * @param cache - The cache to read and fill; the default instance's when omitted
 * @returns Hashed keys that must be resolved through KV
 * @throws {CollegeDBError} `PLACEMENT_UNVERIFIED` when KV fails and no usable stale copy is allowed or held
 * @since 1.3.0
 */
export async function loadPlacementExceptions(
	kv: KVStorage,
	version: number,
	ttlMs: number = DEFAULT_MANIFEST_TTL_MS,
	options: PlacementLoadOptions = {},
	cache: PlacementCache = defaultPlacementCache
): Promise<Set<string>> {
	const held = cache.exceptions;
	if (!options.fresh && held && held.version === version && held.expiresAt >= Date.now()) {
		return held.keys;
	}

	let stored: string[] | null;

	if (options.staleOk && !options.fresh && held && held.version === version && isPlacementBackingOff(cache)) {
		return held.keys;
	}

	try {
		stored = await kv.get<string[]>(EXCEPTIONS_KEY, 'json');
	} catch (error) {
		cache.failedAt = Date.now();
		// read again: another call may have replaced the set while this read was out
		const current = cache.exceptions;
		if (options.staleOk && current && current.version === version) {
			return current.keys;
		}

		throw placementUnverified('exception set', error);
	}

	const keys = new Set(Array.isArray(stored) ? stored : []);

	if (ttlMs > 0) {
		cache.exceptions = { keys, version, expiresAt: Date.now() + ttlMs };
	}

	return keys;
}

/**
 * Adds a hashed key to the exception set and bumps the manifest version.
 *
 * The version bump is what makes a reassignment visible to other isolates: they
 * refresh the manifest on their own schedule, see a new `exceptionsVersion`, and
 * discard their cached set.
 *
 * Calls for one KV store run one at a time within a process, so concurrent calls
 * lose nothing. With a `journal`, the key is first written to a journal table by
 * an insert that ignores duplicates, and the KV set is rebuilt from that table.
 * The table is the durable record and the KV set is a derived copy. The version
 * bump is a compare-and-swap on the journal's own manifest, so a caller holding an
 * older manifest cannot roll an epoch back.
 *
 * @param kv - KV store holding the exception set and manifest
 * @param manifest - Current manifest, returned updated
 * @param hashedKey - The hashed key to record
 * @param journal - Journal shard and scope, when one is configured
 * @param cache - The cache to refresh; the default instance's when omitted
 * @returns The manifest with an incremented `exceptionsVersion`
 * @since 1.3.0
 */
export async function addPlacementException(
	kv: KVStorage,
	manifest: PlacementManifest,
	hashedKey: string,
	journal?: PlacementJournal,
	cache: PlacementCache = defaultPlacementCache
): Promise<PlacementManifest> {
	return await inOrder(kv, async () => {
		if (journal) {
			const insert = buildInsertIgnore(
				EXCEPTIONS_TABLE,
				{ scope: journal.scope, hashed_key: hashedKey, added_at: Date.now() },
				journal.db.dialect
			);
			await journal.db
				.prepare(insert.sql)
				.bind(...insert.bindings)
				.run();
			return await publishJournaledExceptions(kv, manifest, journal, cache);
		}

		const existing = await loadPlacementExceptions(kv, manifest.exceptionsVersion, 0, {}, cache);
		if (existing.has(hashedKey)) {
			return manifest;
		}

		existing.add(hashedKey);
		await kv.put(EXCEPTIONS_KEY, JSON.stringify([...existing]));

		const updated: PlacementManifest = {
			...manifest,
			exceptionsVersion: manifest.exceptionsVersion + 1
		};

		await savePlacementManifest(kv, updated, undefined, cache);
		cache.exceptions = { keys: existing, version: updated.exceptionsVersion, expiresAt: Date.now() + DEFAULT_MANIFEST_TTL_MS };

		return updated;
	});
}

/**
 * Where durable placement exceptions live when a journal is configured.
 * @since 1.4.0
 */
export interface PlacementJournal {
	/** The journal shard */
	db: SQLDatabase;
	/** Journal scope */
	scope: string;
}

/** Table on the journal shard that holds the durable exception set. @private */
const EXCEPTIONS_TABLE = 'collegedb_placement_exceptions';

const exceptionQueues = new WeakMap<KVStorage, Promise<unknown>>();

/**
 * Runs exception writes for one KV store one at a time within this process, so
 * each rebuild of the KV copy starts after the previous one finished and the
 * last rebuild sees every row. KV has no compare-and-set, so another isolate can
 * still overwrite the copy with an older snapshot; rebuilding from the journal
 * repairs that.
 * @private
 */
function inOrder<T>(kv: KVStorage, work: () => Promise<T>): Promise<T> {
	const next = (exceptionQueues.get(kv) ?? Promise.resolve()).catch(() => undefined).then(work);
	exceptionQueues.set(kv, next);
	return next;
}

/**
 * Rebuilds the KV exception set from the journal table, keeping any key the KV
 * copy already has, and bumps the manifest version.
 * @private
 */
async function publishJournaledExceptions(
	kv: KVStorage,
	manifest: PlacementManifest,
	journal: PlacementJournal,
	cache: PlacementCache
): Promise<PlacementManifest> {
	const rows = await journal.db
		.prepare(`SELECT hashed_key FROM ${EXCEPTIONS_TABLE} WHERE scope = ?`)
		.bind(journal.scope)
		.all<{ hashed_key: string }>();
	const stored = await kv.get<string[]>(EXCEPTIONS_KEY, 'json');
	const keys = new Set([...(Array.isArray(stored) ? stored : []), ...rows.results.map((row) => row.hashed_key)]);

	await kv.put(EXCEPTIONS_KEY, JSON.stringify([...keys]));

	let updated: PlacementManifest;
	const authority = manifestJournal(journal);

	if (authority) {
		// bumped from the journal's copy, so a caller holding an older manifest cannot roll an epoch back
		const committed = await commitJournalManifest(authority, (current) => {
			const base = current ?? manifest;
			return { ...base, exceptionsVersion: Math.max(base.exceptionsVersion + 1, keys.size) };
		});
		updated = committed.manifest;
		await deriveManifestCopy(kv, updated, true, cache);
	} else {
		updated = { ...manifest, exceptionsVersion: Math.max(manifest.exceptionsVersion + 1, keys.size) };
		await savePlacementManifest(kv, updated, undefined, cache);
	}

	cache.exceptions = { keys, version: updated.exceptionsVersion, expiresAt: Date.now() + DEFAULT_MANIFEST_TTL_MS };

	return updated;
}

/**
 * Rebuilds the KV exception set from the journal. Recovery and verification call
 * it to repair a copy that another isolate overwrote with an older snapshot.
 *
 * @param kv - KV store holding the exception set and manifest
 * @param manifest - Current manifest, returned updated
 * @param journal - The journal shard and scope
 * @param cache - The cache to refresh; the default instance's when omitted
 * @returns The manifest with an incremented `exceptionsVersion`
 * @since 1.4.0
 */
export async function republishPlacementExceptions(
	kv: KVStorage,
	manifest: PlacementManifest,
	journal: PlacementJournal,
	cache: PlacementCache = defaultPlacementCache
): Promise<PlacementManifest> {
	return await inOrder(kv, () => publishJournaledExceptions(kv, manifest, journal, cache));
}
