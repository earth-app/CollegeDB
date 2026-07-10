/**
 * @fileoverview Read-through cache helpers over CollegeDB's configured KV store.
 *
 * `cached` collapses the "check KV, parse, on miss compute and store" block
 * that consumers paste around nearly every hot read. `invalidate` clears a
 * key prefix after a write. Both default to the KV store passed to
 * {@link initialize}, so callers do not thread a KV handle through their code.
 *
 * Time-to-live is enforced in-band (an expiry timestamp is stored alongside the
 * value) rather than relying on a backend TTL, because CollegeDB's provider-
 * agnostic {@link KVStorage} contract does not expose native expiration. This
 * keeps behavior identical across Cloudflare KV, Redis/Valkey, NuxtHub, and the
 * in-memory test provider.
 *
 * @author Gregory Mitchell
 * @since 1.2.4
 */

import { CollegeDBError } from './errors';
import { getActiveConfig } from './router';
import type { KVStorage } from './types';

/**
 * Stored cache payload: the value plus an optional epoch-ms expiry.
 * @private
 */
interface CacheEnvelope<T> {
	/** Cached value */
	v: T;
	/** Absolute expiry in epoch milliseconds (omitted when no TTL) */
	e?: number;
}

/**
 * Options for {@link cached} and {@link invalidate}.
 */
export interface CacheOptions {
	/** Time-to-live in seconds; omit or set `<= 0` to cache without expiry */
	ttl?: number;
	/** KV store override; defaults to the store passed to {@link initialize} */
	kv?: KVStorage;
}

/**
 * Resolves the KV store to use, preferring an explicit override.
 * @private
 */
function resolveKV(explicit?: KVStorage): KVStorage {
	const kv = explicit ?? getActiveConfig()?.kv;
	if (!kv) {
		throw new CollegeDBError('CollegeDB not initialized. Call initialize() first or pass options.kv.', 'NOT_INITIALIZED');
	}
	return kv;
}

/**
 * Returns a cached value for `key`, computing and storing it on a miss.
 *
 * On a hit the stored value is returned (unless it has expired, in which case
 * the entry is deleted and treated as a miss). A corrupt entry self-heals: it
 * is deleted and the value is recomputed.
 *
 * @template T - Type of the cached value
 * @param key - Cache key
 * @param fetcher - Async producer invoked on a miss
 * @param options - TTL and KV override
 * @returns The cached or freshly computed value
 * @throws {CollegeDBError} If no KV store is available
 * @since 1.2.4
 * @example
 * ```typescript
 * const user = await cached(`user:${id}`, () => loadUser(id), { ttl: 3600 });
 * ```
 */
export async function cached<T>(key: string, fetcher: () => Promise<T>, options: CacheOptions = {}): Promise<T> {
	const kv = resolveKV(options.kv);

	const raw = await kv.get(key, 'text');
	if (typeof raw === 'string') {
		try {
			const envelope = JSON.parse(raw) as CacheEnvelope<T>;
			if (envelope && typeof envelope === 'object' && 'v' in envelope) {
				if (!envelope.e || envelope.e > Date.now()) {
					return envelope.v;
				}
				await kv.delete(key);
			}
		} catch {
			// corrupt entry; drop it and recompute
			await kv.delete(key);
		}
	}

	const data = await fetcher();
	const envelope: CacheEnvelope<T> = { v: data };
	if (options.ttl && options.ttl > 0) {
		envelope.e = Date.now() + options.ttl * 1000;
	}
	await kv.put(key, JSON.stringify(envelope));
	return data;
}

/**
 * Deletes every cache key that starts with `prefix`.
 *
 * @param prefix - Key prefix to clear (e.g. `user:` or `tickets:list:`)
 * @param options - KV override
 * @returns The number of keys deleted
 * @throws {CollegeDBError} If no KV store is available
 * @since 1.2.4
 * @example
 * ```typescript
 * await invalidate(`tickets:list:`); // after a ticket write
 * ```
 */
export async function invalidate(prefix: string, options: { kv?: KVStorage } = {}): Promise<number> {
	const kv = resolveKV(options.kv);

	let deleted = 0;
	let cursor: string | undefined;
	do {
		const result = await kv.list({ prefix, cursor });
		for (const entry of result.keys) {
			await kv.delete(entry.name);
			deleted++;
		}
		cursor = result.list_complete ? undefined : result.cursor;
	} while (cursor);

	return deleted;
}
