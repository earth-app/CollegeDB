/**
 * @fileoverview Per-call timeout for KVStorage backends
 *
 * @since 1.4.0
 */

import { CollegeDBError } from './errors';
import type { KVStorage } from './types';

const wrappers = new WeakMap<KVStorage, { timeoutMs: number; wrapped: KVStorage }>();

/**
 * Wraps a KV store so each call rejects with `KV_TIMEOUT` when it runs past `timeoutMs`.
 *
 * The inner call is not cancelled, because Workers KV cannot abort a request. A timed-out
 * write can still land. A timeout rejects; it never resolves with `null` or an empty value.
 * Optional bulk methods exist on the wrapper only when the inner store has them.
 *
 * The wrapper is cached per store and limit, so re-initializing with the same store keeps
 * the same identity and the mapper caches keyed on it survive.
 *
 * @param kv - Store to wrap
 * @param timeoutMs - Per-call limit in milliseconds; a non-positive or non-finite value returns `kv` unchanged
 * @returns The wrapped store
 * @since 1.4.0
 */
export function withKVTimeout(kv: KVStorage, timeoutMs: number): KVStorage {
	if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) return kv;

	const cached = wrappers.get(kv);
	if (cached?.timeoutMs === timeoutMs) return cached.wrapped;

	const limit = <T>(method: string, call: Promise<T>): Promise<T> => {
		let timer: ReturnType<typeof setTimeout>;
		const timeout = new Promise<never>((_, reject) => {
			timer = setTimeout(() => reject(new CollegeDBError(`KV ${method} timed out after ${timeoutMs} ms`, 'KV_TIMEOUT')), timeoutMs);
		});
		return Promise.race([call, timeout]).finally(() => clearTimeout(timer));
	};

	const wrapped: KVStorage = {
		get: (async (key: string, type?: 'json' | 'text') => limit('get', kv.get(key, type as 'text'))) as KVStorage['get'],
		put: async (key, value) => limit('put', kv.put(key, value)),
		delete: async (key) => limit('delete', kv.delete(key)),
		list: async (options) => limit('list', kv.list(options))
	};

	if (kv.getMany) {
		wrapped.getMany = (async (keys: string[], type?: 'json' | 'text') =>
			limit('getMany', kv.getMany!(keys, type as 'text'))) as KVStorage['getMany'];
	}
	if (kv.putMany) {
		wrapped.putMany = async (entries) => limit('putMany', kv.putMany!(entries));
	}
	if (kv.deleteMany) {
		wrapped.deleteMany = async (keys) => limit('deleteMany', kv.deleteMany!(keys));
	}

	wrappers.set(kv, { timeoutMs, wrapped });
	return wrapped;
}
