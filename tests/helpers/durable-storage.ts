import { DatabaseSync } from 'node:sqlite';

/** Thrown by the fault hook; stands in for the isolate dying at that write. */
export class FaultInjected extends Error {
	constructor(public readonly write: number) {
		super(`fault injected at write ${write}`);
	}
}

const WRITE_STATEMENT = /^\s*(insert|update|delete|replace|create|drop|alter)\b/i;
class MockSqlCursor {
	rowsWritten: number;

	constructor(
		private readonly rows: Record<string, unknown>[],
		rowsWritten = 0
	) {
		this.rowsWritten = rowsWritten;
	}

	toArray(): Record<string, unknown>[] {
		return this.rows;
	}

	one(): Record<string, unknown> {
		if (this.rows.length !== 1) throw new Error(`expected exactly one row, got ${this.rows.length}`);
		return this.rows[0]!;
	}

	[Symbol.iterator](): Iterator<Record<string, unknown>> {
		return this.rows[Symbol.iterator]();
	}
}

export class MockDurableObjectStorage {
	data = new Map<string, unknown>();
	writes = 0;
	faultAt: number | null = null;
	faultTriggered = false;
	db = new DatabaseSync(':memory:');

	sql = {
		exec: (query: string, ...bindings: (string | number | null)[]): MockSqlCursor => {
			const statement = this.db.prepare(query);
			if (WRITE_STATEMENT.test(query)) {
				this.countWrite();
				const result = statement.run(...bindings);
				return new MockSqlCursor([], Number(result.changes));
			}
			return new MockSqlCursor(statement.all(...bindings).map((row) => ({ ...row })));
		}
	};

	/** Throws at the nth write counted from now (sql writes and storage put, delete and deleteAll). */
	armFault(nth: number): void {
		this.writes = 0;
		this.faultAt = nth;
		this.faultTriggered = false;
	}

	disarmFault(): void {
		this.faultAt = null;
	}

	private countWrite(): void {
		this.writes++;
		if (this.faultAt !== null && this.writes === this.faultAt) {
			this.faultTriggered = true;
			throw new FaultInjected(this.writes);
		}
	}

	transactionSync<T>(callback: () => T): T {
		this.db.exec('BEGIN');
		try {
			const result = callback();
			this.db.exec('COMMIT');
			return result;
		} catch (error) {
			this.db.exec('ROLLBACK');
			throw error;
		}
	}

	async get<T = unknown>(key: string): Promise<T | undefined> {
		return structuredClone(this.data.get(key)) as T | undefined;
	}

	async put(keyOrEntries: string | Record<string, unknown>, value?: unknown): Promise<void> {
		this.countWrite();
		if (typeof keyOrEntries === 'string') {
			this.data.set(keyOrEntries, structuredClone(value));
			return;
		}
		for (const [key, entry] of Object.entries(keyOrEntries)) this.data.set(key, structuredClone(entry));
	}

	async delete(keyOrKeys: string | string[]): Promise<boolean | number> {
		this.countWrite();
		if (typeof keyOrKeys === 'string') return this.data.delete(keyOrKeys);
		return keyOrKeys.filter((key) => this.data.delete(key)).length;
	}

	async deleteAll(): Promise<void> {
		this.countWrite();
		this.data.clear();
		this.db.close();
		this.db = new DatabaseSync(':memory:');
	}

	async list<T = unknown>(options?: { prefix?: string; startAfter?: string; limit?: number }): Promise<Map<string, T>> {
		const keys = [...this.data.keys()]
			.filter((key) => (options?.prefix ? key.startsWith(options.prefix) : true))
			.filter((key) => (options?.startAfter !== undefined ? key > options.startAfter : true))
			.sort();
		const page = options?.limit === undefined ? keys : keys.slice(0, options.limit);
		return new Map(page.map((key) => [key, structuredClone(this.data.get(key)) as T]));
	}
}

/** Test double for DurableObjectState with the legacy key-value API and a node:sqlite backed sql surface. */
export class MockDurableObjectState {
	storage = new MockDurableObjectStorage();

	blockConcurrencyWhile<T>(callback: () => Promise<T>): Promise<T> {
		return callback();
	}
}
