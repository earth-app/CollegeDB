import type { KVListResult, KVStorage, PreparedStatement, QueryResult, SQLDatabase } from '../../src/types';

/** Thrown by a {@link FaultPlan} in place of a real failure. */
export class InjectedFault extends Error {
	constructor(message: string) {
		super(message);
		this.name = 'InjectedFault';
	}
}

/**
 * Throws at the nth externally visible SQL or KV operation, and then plays dead.
 *
 * `before` fails the operation without running it. `after` runs it and then
 * fails, which is a crash that lands after a commit but before the caller hears
 * about it. Once a fault fires every later operation also throws until
 * {@link FaultPlan.revive}, because a crashed process runs no `finally` block
 * that writes journal state. A batch counts as one operation.
 */
export class FaultPlan {
	/** Operations seen since the last {@link FaultPlan.reset} */
	count = 0;
	/** Labels of the operations seen, for failure messages */
	readonly log: string[] = [];
	private failAt: number | null = null;
	private failMode: 'before' | 'after' = 'before';
	private dead = false;
	private pauses: Array<{ match: RegExp; reached: () => void; gate: Promise<void> }> = [];
	private crashes: RegExp[] = [];

	/**
	 * Holds the next operation whose label matches, before it runs, until `resume`
	 * is called. `reached` settles once the operation is held. The pause fires once.
	 */
	pauseOn(match: RegExp): { reached: Promise<void>; resume: () => void } {
		let reachedFn!: () => void;
		let resume!: () => void;
		const reached = new Promise<void>((done) => (reachedFn = done));
		const gate = new Promise<void>((done) => (resume = done));
		this.pauses.push({ match, reached: reachedFn, gate });
		return { reached, resume };
	}

	/** Kills the process at the next operation whose label matches, without running it. */
	crashOn(match: RegExp): void {
		this.crashes.push(match);
	}

	/** Fails the nth operation from now. */
	arm(n: number, mode: 'before' | 'after' = 'before'): void {
		this.count = 0;
		this.log.length = 0;
		this.failAt = n;
		this.failMode = mode;
		this.dead = false;
	}

	/** Counts operations without failing any. */
	reset(): void {
		this.count = 0;
		this.log.length = 0;
		this.failAt = null;
		this.dead = false;
	}

	/** Brings the process back after a fault. */
	revive(): void {
		this.failAt = null;
		this.dead = false;
	}

	get crashed(): boolean {
		return this.dead;
	}

	async guard<T>(label: string, work: () => Promise<T>): Promise<T> {
		if (this.dead) {
			throw new InjectedFault(`process is down (${label})`);
		}

		const pauseAt = this.pauses.findIndex((pause) => pause.match.test(label));
		if (pauseAt >= 0) {
			const [pause] = this.pauses.splice(pauseAt, 1);
			pause!.reached();
			await pause!.gate;
		}

		const crashAt = this.crashes.findIndex((match) => match.test(label));
		if (crashAt >= 0) {
			this.crashes.splice(crashAt, 1);
			this.dead = true;
			throw new InjectedFault(`crash before: ${label}`);
		}

		const n = ++this.count;
		this.log.push(`${n} ${label}`);

		if (this.failAt === n) {
			this.dead = true;
			if (this.failMode === 'before') {
				throw new InjectedFault(`fault before operation ${n}: ${label}`);
			}

			this.dead = false;
			await work();
			this.dead = true;
			throw new InjectedFault(`fault after operation ${n}: ${label}`);
		}

		return await work();
	}

	/** Wraps a SQL shard. */
	wrapSQL(db: SQLDatabase, name: string): SQLDatabase {
		const plan = this;
		const wrapStatement = (statement: PreparedStatement, sql: string): PreparedStatement => ({
			bind: (...bindings: any[]) => wrapStatement(statement.bind(...bindings), sql),
			run: <T>() => plan.guard(`${name} run ${sql.slice(0, 48)}`, () => statement.run<T>()),
			all: <T>() => plan.guard(`${name} all ${sql.slice(0, 48)}`, () => statement.all<T>()),
			first: <T>() => plan.guard(`${name} first ${sql.slice(0, 48)}`, () => statement.first<T>())
		});

		const wrapped: SQLDatabase = {
			dialect: db.dialect,
			atomicBatch: db.atomicBatch,
			prepare: (sql: string) => wrapStatement(db.prepare(sql), sql)
		};

		if (db.runBatch) {
			wrapped.runBatch = <T>(statements: Parameters<NonNullable<SQLDatabase['runBatch']>>[0]): Promise<QueryResult<T>[]> =>
				plan.guard(`${name} batch of ${statements.length}`, () => db.runBatch!<T>(statements));
		}

		return wrapped;
	}

	/** Wraps a KV store. */
	wrapKV(kv: KVStorage, name = 'kv'): KVStorage {
		const plan = this;
		return {
			get: ((key: string, type?: 'text' | 'json') =>
				plan.guard(`${name} get ${key}`, () => kv.get(key, type as 'json'))) as KVStorage['get'],
			put: (key: string, value: string) => plan.guard(`${name} put ${key}`, () => kv.put(key, value)),
			delete: (key: string) => plan.guard(`${name} delete ${key}`, () => kv.delete(key)),
			list: (options?: { prefix?: string; cursor?: string; limit?: number }): Promise<KVListResult> =>
				plan.guard(`${name} list`, () => kv.list(options))
		};
	}
}

/**
 * Builds a step hook that runs one writer per protocol step, once each, so a
 * test can place a router-issued write at every phase of a move.
 *
 * @param writers - Step name to the writer that runs after that step
 * @returns The hook and the list of steps it fired, in order
 */
export function createStepWriters(writers: Record<string, () => Promise<void>>): {
	hook: (step: string) => Promise<void>;
	fired: string[];
} {
	const fired: string[] = [];
	const hook = async (step: string): Promise<void> => {
		const writer = writers[step];
		if (writer && !fired.includes(step)) {
			fired.push(step);
			await writer();
		}
	};

	return { hook, fired };
}
