/** The part of `AsyncLocalStorage` used here, declared locally so the build needs no Node types. */
interface ScopeStorage {
	getStore(): unknown;
	run<R>(store: unknown, work: () => R): R;
}

/**
 * Finds `AsyncLocalStorage` without a static import, which would stop the
 * module from loading on Workers without `nodejs_compat`. Runtimes that lack
 * `process.getBuiltinModule` count as having no async context.
 */
function detect(): ScopeStorage | null {
	try {
		const host = globalThis as {
			process?: { getBuiltinModule?: (id: string) => { AsyncLocalStorage?: new () => ScopeStorage } | undefined };
		};
		const Storage = host.process?.getBuiltinModule?.('node:async_hooks')?.AsyncLocalStorage;
		return Storage ? new Storage() : null;
	} catch {
		return null;
	}
}

const detected = detect();
let storage = detected;

// no scope can exist before the first run, so the lookup is skipped until then
let started = false;

/**
 * Whether `collegedb(config, callback)` can scope its callback to its own state.
 * @since 1.4.0
 */
export function asyncContextAvailable(): boolean {
	return storage !== null;
}

/**
 * The value of the innermost scope the caller runs in, or `undefined` outside one.
 * @private
 */
export function currentScope<S>(): S | undefined {
	return started && storage ? (storage.getStore() as S | undefined) : undefined;
}

/**
 * Runs `work` with `value` as the scope, for its whole async lifetime.
 * @private
 */
export function runInScope<S, R>(value: S, work: () => R): R {
	started = true;
	return storage!.run(value, work);
}

/**
 * Turns the async context off, or back on where the runtime has one, so a test
 * can take the path a runtime without it takes.
 *
 * @internal
 * @since 1.4.0
 */
export function setAsyncContextEnabled(enabled: boolean): void {
	storage = enabled ? detected : null;
}
