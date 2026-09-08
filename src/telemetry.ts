/**
 * @fileoverview Per-phase timing hook for routed operations.
 *
 * CollegeDB spends a routed operation across several distinct costs: hashing
 * the key, reading and writing the KV mapping, selecting a shard, and executing
 * SQL. Reported end to end they are indistinguishable, which makes it
 * impossible to say whether a change helped. This module exposes the seam that
 * separates them.
 *
 * The observer is opt-in through {@link CollegeDBConfig.onPhase}. When it is
 * unset the instrumentation is two function calls that return immediately and
 * allocate nothing, so instrumented code paths carry no measurable cost in
 * production.
 *
 * Timings come from `performance.now()` where available, which is a
 * monotonic high-resolution clock in Node, Bun, and workerd. `Date.now()` is
 * the fallback and only has millisecond resolution; every routed phase is
 * faster than that, which is why it is not the default.
 *
 * @author Gregory Mitchell
 * @since 1.4.0
 */

import type { BatchStatement, KVListResult, KVStorage, PreparedStatement, QueryResult, SQLDatabase } from './types';

/**
 * A distinct cost inside a routed operation.
 *
 * - `hash` - deriving the KV key for a primary key (SHA-256, memoized)
 * - `kv.get` / `kv.put` / `kv.delete` / `kv.list` - KV round trips
 * - `shard.select` - running the allocation strategy for an unmapped key
 * - `coordinator.fetch` - a Durable Object round trip
 * - `sql.prepare` / `sql.exec` - statement construction and execution
 * @since 1.4.0
 */
export type PhaseName =
	'hash' | 'kv.get' | 'kv.put' | 'kv.delete' | 'kv.list' | 'shard.select' | 'coordinator.fetch' | 'sql.prepare' | 'sql.exec';

/**
 * One measured phase.
 * @since 1.4.0
 */
export interface PhaseSpan {
	/** Which cost this span measured */
	phase: PhaseName;
	/** Elapsed wall time in milliseconds */
	durationMs: number;
	/** Optional discriminator, such as the KV key prefix or the shard binding */
	detail?: string;
}

/**
 * Receives every measured phase. Must not throw; exceptions are swallowed so a
 * broken observer cannot fail a query.
 * @since 1.4.0
 */
export type PhaseObserver = (span: PhaseSpan) => void;

let observer: PhaseObserver | null = null;

const clock: () => number =
	typeof performance !== 'undefined' && typeof performance.now === 'function' ? () => performance.now() : () => Date.now();

/**
 * Installs the phase observer, or clears it when passed `null`.
 *
 * Called by {@link initialize} from `config.onPhase`; exported so tests and
 * benchmark harnesses can attach a collector without reinitializing.
 *
 * @param next - Observer to install, or `null` to disable instrumentation
 * @since 1.4.0
 */
export function setPhaseObserver(next: PhaseObserver | null): void {
	observer = next ?? null;
}

/**
 * Reports whether instrumentation is currently active.
 *
 * Use this to skip building an expensive `detail` string that would only be
 * read by an observer.
 *
 * @returns `true` when a phase observer is installed
 * @since 1.4.0
 */
export function isPhaseObserverActive(): boolean {
	return observer !== null;
}

/**
 * Starts a phase measurement.
 *
 * Returns `undefined` when no observer is installed, which {@link phaseEnd}
 * treats as "do nothing". Reading the clock is skipped entirely in that case.
 *
 * @returns An opaque start timestamp, or `undefined` when instrumentation is off
 * @since 1.4.0
 * @example
 * ```typescript
 * const started = phaseStart();
 * const mapping = await kv.get(key, 'json');
 * phaseEnd('kv.get', started);
 * ```
 */
export function phaseStart(): number | undefined {
	return observer === null ? undefined : clock();
}

/**
 * Closes a phase measurement opened by {@link phaseStart} and reports it.
 *
 * A no-op when instrumentation is off or `started` is `undefined`, so call
 * sites need no conditional of their own.
 *
 * @param phase - Which cost this span measured
 * @param started - The value returned by {@link phaseStart}
 * @param detail - Optional discriminator recorded on the span
 * @since 1.4.0
 */
export function phaseEnd(phase: PhaseName, started: number | undefined, detail?: string): void {
	if (observer === null || started === undefined) {
		return;
	}

	const span: PhaseSpan =
		detail === undefined ? { phase, durationMs: clock() - started } : { phase, durationMs: clock() - started, detail };

	try {
		observer(span);
	} catch {
		// a broken observer must never fail the query it was measuring
	}
}

/**
 * Wraps a KV store so every round trip reports a phase span.
 *
 * Applied once by {@link initialize} when `config.onPhase` is set, rather than
 * at each call site, so it also covers the KV traffic from the read-through
 * cache and the secondary-index helpers. When instrumentation is off no wrapper
 * is created at all and the original store is used directly.
 *
 * @param kv - The store to wrap
 * @returns A store with identical behavior that reports timings
 * @since 1.4.0
 */
export function instrumentKV(kv: KVStorage): KVStorage {
	const wrapped: KVStorage = {
		async get<T = unknown>(key: string, type?: 'text' | 'json'): Promise<any> {
			const started = phaseStart();
			try {
				return await (type === 'json' ? kv.get<T>(key, 'json') : kv.get(key, type));
			} finally {
				phaseEnd('kv.get', started, keyPrefix(key));
			}
		},
		async put(key: string, value: string): Promise<void> {
			const started = phaseStart();
			try {
				await kv.put(key, value);
			} finally {
				phaseEnd('kv.put', started, keyPrefix(key));
			}
		},
		async delete(key: string): Promise<void> {
			const started = phaseStart();
			try {
				await kv.delete(key);
			} finally {
				phaseEnd('kv.delete', started, keyPrefix(key));
			}
		},
		async list(options?: { prefix?: string; cursor?: string; limit?: number }): Promise<KVListResult> {
			const started = phaseStart();
			try {
				return await kv.list(options);
			} finally {
				phaseEnd('kv.list', started, options?.prefix);
			}
		}
	};

	if (kv.getMany) {
		wrapped.getMany = async <T = unknown>(keys: string[], type?: 'text' | 'json'): Promise<any> => {
			const started = phaseStart();
			try {
				return await (type === 'json' ? kv.getMany!<T>(keys, 'json') : kv.getMany!(keys, type));
			} finally {
				phaseEnd('kv.get', started, `many:${keys.length}`);
			}
		};
	}

	if (kv.putMany) {
		wrapped.putMany = async (entries) => {
			const started = phaseStart();
			try {
				await kv.putMany!(entries);
			} finally {
				phaseEnd('kv.put', started, `many:${entries.length}`);
			}
		};
	}

	if (kv.deleteMany) {
		wrapped.deleteMany = async (keys) => {
			const started = phaseStart();
			try {
				await kv.deleteMany!(keys);
			} finally {
				phaseEnd('kv.delete', started, `many:${keys.length}`);
			}
		};
	}

	return wrapped;
}

/**
 * Wraps a SQL provider so statement construction and execution report phase
 * spans, tagged with the shard binding.
 *
 * Applied once per shard by {@link initialize} when `config.onPhase` is set.
 *
 * @param db - The provider to wrap
 * @param binding - Shard binding name, recorded as the span detail
 * @returns A provider with identical behavior that reports timings
 * @since 1.4.0
 */
export function instrumentSQL(db: SQLDatabase, binding: string): SQLDatabase {
	const wrapped: SQLDatabase = {
		prepare(sql: string): PreparedStatement {
			const prepareStarted = phaseStart();
			const inner = db.prepare(sql);
			phaseEnd('sql.prepare', prepareStarted, binding);
			return instrumentStatement(inner, binding);
		}
	};

	if (db.runBatch) {
		wrapped.runBatch = async <T = Record<string, unknown>>(statements: BatchStatement[]): Promise<QueryResult<T>[]> => {
			const started = phaseStart();
			try {
				return await db.runBatch!<T>(statements);
			} finally {
				phaseEnd('sql.exec', started, `${binding}:batch:${statements.length}`);
			}
		};
	}

	return wrapped;
}

/**
 * Wraps a prepared statement so each terminal call reports a `sql.exec` span.
 * @private
 */
function instrumentStatement(statement: PreparedStatement, binding: string): PreparedStatement {
	return {
		bind(...bindings: any[]): PreparedStatement {
			return instrumentStatement(statement.bind(...bindings), binding);
		},
		async run<T = Record<string, unknown>>(): Promise<QueryResult<T>> {
			const started = phaseStart();
			try {
				return await statement.run<T>();
			} finally {
				phaseEnd('sql.exec', started, binding);
			}
		},
		async all<T = Record<string, unknown>>(): Promise<QueryResult<T>> {
			const started = phaseStart();
			try {
				return await statement.all<T>();
			} finally {
				phaseEnd('sql.exec', started, binding);
			}
		},
		async first<T = Record<string, unknown>>(): Promise<T | null> {
			const started = phaseStart();
			try {
				return await statement.first<T>();
			} finally {
				phaseEnd('sql.exec', started, binding);
			}
		}
	};
}

/**
 * Reduces a KV key to its `collegedb:<kind>:` prefix so spans group by kind
 * instead of by individual key.
 * @private
 */
function keyPrefix(key: string): string {
	const parts = key.split(':');
	return parts.length > 2 ? `${parts[0]}:${parts[1]}` : key;
}

/**
 * Aggregated statistics for one phase.
 * @since 1.4.0
 */
export interface PhaseStats {
	/** Which cost these statistics describe */
	phase: PhaseName;
	/** Number of spans recorded */
	count: number;
	/** Sum of all span durations in milliseconds */
	totalMs: number;
	/** Mean span duration in milliseconds */
	avgMs: number;
	/** Shortest span in milliseconds */
	minMs: number;
	/** Longest span in milliseconds */
	maxMs: number;
	/** 50th percentile span duration in milliseconds */
	p50Ms: number;
	/** 95th percentile span duration in milliseconds */
	p95Ms: number;
}

/**
 * Collects spans and summarizes them per phase.
 *
 * This is the counterpart to {@link setPhaseObserver} for callers that want a
 * table rather than a stream: attach {@link PhaseCollector.observer}, run a
 * workload, then read {@link PhaseCollector.stats}.
 *
 * @since 1.4.0
 * @example
 * ```typescript
 * const collector = new PhaseCollector();
 * setPhaseObserver(collector.observer);
 * await run('user-1', 'INSERT INTO users (id) VALUES (?)', ['user-1']);
 * setPhaseObserver(null);
 * console.table(collector.stats());
 * ```
 */
export class PhaseCollector {
	private readonly samples = new Map<PhaseName, number[]>();

	/** Observer to hand to {@link setPhaseObserver}. Bound, so it can be passed directly. */
	readonly observer: PhaseObserver = (span) => {
		const existing = this.samples.get(span.phase);
		if (existing) {
			existing.push(span.durationMs);
		} else {
			this.samples.set(span.phase, [span.durationMs]);
		}
	};

	/** Discards every recorded span. */
	reset(): void {
		this.samples.clear();
	}

	/** Total number of spans recorded across every phase. */
	get size(): number {
		let total = 0;
		for (const values of this.samples.values()) {
			total += values.length;
		}
		return total;
	}

	/**
	 * Summarizes the recorded spans, ordered by total time descending.
	 *
	 * @returns One {@link PhaseStats} entry per phase that recorded at least one span
	 */
	stats(): PhaseStats[] {
		const out: PhaseStats[] = [];

		for (const [phase, values] of this.samples) {
			if (values.length === 0) continue;

			const sorted = [...values].sort((left, right) => left - right);
			const totalMs = sorted.reduce((sum, value) => sum + value, 0);

			out.push({
				phase,
				count: sorted.length,
				totalMs,
				avgMs: totalMs / sorted.length,
				minMs: sorted[0]!,
				maxMs: sorted[sorted.length - 1]!,
				p50Ms: percentile(sorted, 0.5),
				p95Ms: percentile(sorted, 0.95)
			});
		}

		return out.sort((left, right) => right.totalMs - left.totalMs);
	}
}

/**
 * Reads a percentile from an ascending-sorted sample array.
 * @private
 */
function percentile(sorted: number[], ratio: number): number {
	if (sorted.length === 0) {
		return 0;
	}

	const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * ratio) - 1));
	return sorted[index]!;
}
