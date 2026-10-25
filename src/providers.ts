/**
 * @fileoverview Adapters that map each supported backend onto CollegeDB's
 * storage contracts.
 *
 * Every adapter produces either a {@link KVStorage} or a {@link SQLDatabase},
 * so the router never learns which backend it is talking to.
 *
 * Supported KV backends:
 * - Cloudflare KV (native shape)
 * - Redis
 * - Valkey
 * - NuxtHub KV / Unstorage-compatible clients
 *
 * Supported SQL backends:
 * - Cloudflare D1 (native shape)
 * - PostgreSQL compatible clients
 * - MySQL / MariaDB compatible clients
 * - SQLite clients
 * - Drizzle ORM database instances
 * - Hyperdrive-backed PostgreSQL / MySQL clients
 *
 * @author Gregory Mitchell
 * @since 1.1.0
 */

import { CollegeDBError } from './errors';
import type {
	BatchStatement,
	ConsistencyCapabilities,
	KVListResult,
	KVStorage,
	PreparedStatement,
	QueryResult,
	QueryResultMeta,
	SQLDatabase,
	SQLDialect
} from './types';

const DEFAULT_REDIS_SCAN_COUNT = 500;

// one connection or pool with no replica routing: primary reads, no sessions
const SINGLE_NODE_CONSISTENCY: ConsistencyCapabilities = Object.freeze({ primary: true, session: false, eventual: true });

/**
 * Minimal Redis/Valkey client contract used by the KV adapter.
 */
export interface RedisLikeClient {
	get(key: string): Promise<string | null>;
	set(key: string, value: string): unknown | Promise<unknown>;
	del(key: string): unknown | Promise<unknown>;
	scan(cursor: string, ...args: any[]): RedisScanResult | Promise<RedisScanResult>;
	/** Multi-get, present on node-redis and ioredis. Used when available. @since 1.3.0 */
	mGet?(keys: string[]): Promise<(string | null)[]>;
	/** ioredis spelling of multi-get. @since 1.3.0 */
	mget?(keys: string[]): Promise<(string | null)[]>;
	/** Multi-set, present on node-redis. @since 1.3.0 */
	mSet?(entries: Array<[string, string]> | Record<string, string>): unknown | Promise<unknown>;
	/** ioredis spelling of multi-set. @since 1.3.0 */
	mset?(entries: Record<string, string>): unknown | Promise<unknown>;
	/** Variadic delete, used to clear many keys in one round trip. @since 1.3.0 */
	unlink?(...keys: string[]): unknown | Promise<unknown>;
}

/**
 * Redis/Valkey SCAN response in ioredis tuple form.
 */
export type RedisScanTupleResult = [string, string[]];

/**
 * Redis/Valkey SCAN response in node-redis object form.
 */
export interface RedisScanObjectResult {
	cursor: string | number;
	keys: string[];
}

/**
 * Accepted SCAN response formats.
 */
export type RedisScanResult = RedisScanTupleResult | RedisScanObjectResult;

/**
 * PostgreSQL result shape used by adapters.
 */
export interface PostgresQueryResult<T = Record<string, unknown>> {
	rows: T[];
	rowCount?: number | null;
	command?: string;
	[key: string]: unknown;
}

/**
 * Minimal PostgreSQL client contract used by the SQL adapter.
 */
export interface PostgresClientLike {
	query<T = Record<string, unknown>>(sql: string, bindings?: any[]): Promise<PostgresQueryResult<T>>;
}

/**
 * Optional lifecycle methods used by Hyperdrive helpers.
 */
export interface PostgresLifecycleClientLike extends PostgresClientLike {
	connect?: () => void | Promise<void>;
	end?: () => void | Promise<void>;
	release?: () => void;
}

/**
 * MySQL/MariaDB OK packet shape.
 */
export interface MySQLOkPacket {
	affectedRows?: number;
	insertId?: number;
	warningStatus?: number;
	[key: string]: unknown;
}

/**
 * Minimal MySQL/MariaDB client contract used by adapters.
 */
export interface MySQLClientLike {
	execute?: (sql: string, bindings?: any[]) => any | Promise<any>;
	query?: (sql: string, bindings?: any[]) => any | Promise<any>;
}

/**
 * Optional lifecycle methods used by Hyperdrive helpers.
 */
export interface MySQLLifecycleClientLike extends MySQLClientLike {
	end?: () => void | Promise<void>;
	close?: () => void | Promise<void>;
	destroy?: () => void;
}

/**
 * Statement contract for SQLite adapters.
 */
export interface SQLiteStatementLike {
	run?: (...bindings: any[]) => unknown | Promise<unknown>;
	all?: (...bindings: any[]) => unknown[] | Promise<unknown[]>;
	get?: (...bindings: any[]) => unknown | Promise<unknown>;
}

/**
 * Minimal SQLite client contract used by adapters.
 */
export interface SQLiteClientLike {
	prepare?: (sql: string) => SQLiteStatementLike;
	execute?: (sql: string, bindings?: any[]) => any | Promise<any>;
}

/**
 * A connection leased for the duration of one batch, and the hook that returns
 * it to wherever it came from.
 *
 * @template C - Client contract the leased connection satisfies
 * @since 1.3.0
 */
export interface LeasedConnection<C> {
	/** The dedicated connection; every statement in the batch runs through it */
	client: C;
	/** Returns the connection to its pool. Called once, including on failure. */
	release: () => void | Promise<void>;
}

/**
 * Controls whether an adapter may run a {@link SQLDatabase.runBatch} group inside
 * one transaction, and on which connection.
 *
 * A transaction is only correct when every statement in it, `BEGIN` and `COMMIT`
 * included, travels down the same connection. A pool does not guarantee that,
 * and a pool cannot be told apart from a single connection by inspection: `pg`'s
 * `Client` and `Pool` both expose `connect()`, and any of them can be wrapped in
 * something that exposes only `query()`. So the adapter never guesses. It
 * batches when it recognizes a pool it knows how to lease from, when you hand it
 * a {@link SQLBatchOptions.lease} of your own, or when you state that the handle
 * is a single connection. Otherwise statements run one at a time, exactly as
 * before.
 *
 * A single connection runs one transaction at a time. While a batch is open,
 * every other statement on that connection waits until it commits or rolls
 * back, and two batches run one after the other, so one slow batch delays every
 * other request that shares the connection. Use a pool when requests need to
 * run concurrently: a pool leases one connection per batch and never waits.
 *
 * @since 1.3.0
 */
export interface SQLBatchOptions<C = unknown> {
	/**
	 * Declares that the handle is one dedicated connection, so `BEGIN`/`COMMIT`
	 * can be issued straight through it. Set this for a `pg.Client`, a `mysql2`
	 * connection, or any SQLite handle. Do **not** set it for a pool.
	 *
	 * The adapter then holds every plain statement back while a batch is open, so
	 * none can run inside the batch's transaction. The wait is the cost: one slow
	 * batch delays every other statement on the connection. A pool avoids it.
	 *
	 * Combined with {@link SQLBatchOptions.lease}, the lease still supplies the
	 * batch's connection, and the wait applies to plain statements all the same.
	 */
	singleConnection?: boolean;
	/**
	 * Leases a dedicated connection for one batch. Takes precedence over
	 * {@link SQLBatchOptions.singleConnection} and over pool autodetection, and is
	 * the escape hatch for a handle CollegeDB does not recognize.
	 *
	 * On its own, a lease adds no wait for plain statements, because the adapter
	 * cannot tell a pool from one shared connection. If the lease hands out one
	 * shared connection, also set `singleConnection: true`, or serialize the
	 * statements yourself.
	 */
	lease?: () => Promise<LeasedConnection<C>>;
}

/**
 * Hyperdrive binding shape used by helper factories.
 */
export interface HyperdriveBindingLike {
	connectionString: string;
	localConnectionString?: string;
}

/**
 * Factory for creating PostgreSQL clients from a Hyperdrive connection string.
 */
export type HyperdrivePostgresClientFactory = (connectionString: string) => PostgresLifecycleClientLike;

/**
 * Factory for creating MySQL clients from a Hyperdrive connection string.
 */
export type HyperdriveMySQLClientFactory = (connectionString: string) => MySQLLifecycleClientLike;

/**
 * Minimal SQL chunk contract produced by Drizzle's `sql` helper.
 */
export interface DrizzleSqlChunkLike {
	append(chunk: DrizzleSqlChunkLike): void;
}

/**
 * Minimal Drizzle `sql` tag contract used to build parameterized raw statements.
 */
export interface DrizzleSqlTagLike {
	(strings: TemplateStringsArray, ...params: any[]): DrizzleSqlChunkLike;
	raw(sql: string): DrizzleSqlChunkLike;
	empty?: () => DrizzleSqlChunkLike;
}

/**
 * Minimal Drizzle database contract used by the SQL adapter.
 */
export interface DrizzleClientLike {
	execute?: (query: any) => any | Promise<any>;
	run?: (query: any) => any | Promise<any>;
	all?: (query: any) => any | Promise<any>;
	get?: (query: any) => any | Promise<any>;
	/**
	 * Drizzle's own transaction API, present on the node-postgres, mysql2 and
	 * SQLite drivers. Where it exists, Drizzle pins the connection and issues the
	 * transaction control itself, which is the guarantee a batch needs.
	 * @since 1.3.0
	 */
	transaction?: <T>(callback: (tx: DrizzleClientLike) => Promise<T>) => Promise<T>;
}

/**
 * NuxtHub/Unstorage-like KV contract used by the adapter.
 */
export interface NuxtHubKVLike {
	get?<T = any>(key: string): T | null | undefined | Promise<T | null | undefined>;
	set?(key: string, value: any, options?: { ttl?: number }): any | Promise<any>;
	del?(key: string): any | Promise<any>;
	keys?(prefix?: string): string[] | readonly string[] | Promise<string[] | readonly string[]>;
	getItem?<T = any>(key: string): T | null | undefined | Promise<T | null | undefined>;
	setItem?(key: string, value: any): any | Promise<any>;
	removeItem?(key: string): any | Promise<any>;
	getKeys?(prefix?: string): string[] | readonly string[] | Promise<string[] | readonly string[]>;
}

/**
 * Creates a Redis-backed KV provider adapter.
 *
 * The adapter supports both node-redis and ioredis scan formats.
 *
 * @param client - Redis/Valkey client
 * @param options - Adapter tuning options
 * @returns KVStorage-compatible adapter
 */
export function createRedisKVProvider(client: RedisLikeClient, options: { scanCount?: number } = {}): KVStorage {
	const scanCount = options.scanCount ?? DEFAULT_REDIS_SCAN_COUNT;

	const parse = <T>(raw: string | null, key: string, type: 'text' | 'json'): T | string | null => {
		if (raw === null) {
			return null;
		}
		if (type !== 'json') {
			return raw;
		}
		try {
			return JSON.parse(raw) as T;
		} catch (error) {
			throw new CollegeDBError(
				`Failed to parse JSON from Redis for key ${key}: ${error instanceof Error ? error.message : String(error)}`,
				'KV_JSON_PARSE_FAILED'
			);
		}
	};

	const multiGet = client.mGet ?? client.mget;

	const provider: KVStorage = {
		async get<T = unknown>(key: string, type: 'text' | 'json' = 'text'): Promise<T | string | null> {
			const raw = await client.get(key);
			if (raw === null) {
				return null;
			}

			if (type !== 'json') {
				return raw;
			}

			try {
				return JSON.parse(raw) as T;
			} catch (error) {
				throw new CollegeDBError(
					`Failed to parse JSON from Redis for key ${key}: ${error instanceof Error ? error.message : String(error)}`,
					'KV_JSON_PARSE_FAILED'
				);
			}
		},

		async put(key: string, value: string): Promise<void> {
			await client.set(key, value);
		},

		async delete(key: string): Promise<void> {
			await client.del(key);
		},

		async list(options?: { prefix?: string; cursor?: string; limit?: number }): Promise<KVListResult> {
			const prefix = options?.prefix ?? '';
			const pattern = `${prefix}*`;
			const limit = options?.limit;
			const keys: string[] = [];

			// SCAN hands back a whole batch at a time, so honouring an exact `limit`
			// means stopping partway through one. The cursor therefore carries both
			// the SCAN cursor that produced the current batch and how many of that
			// batch's matching keys have already been returned. Truncating without
			// recording the offset dropped the rest of the batch, and the plain SCAN
			// cursor then resumed past it, so a paginating caller lost keys.
			let { scanCursor, skip } = decodeScanCursor(options?.cursor);

			while (true) {
				const batchCursor = scanCursor;
				const scanResult = await executeRedisScan(client, batchCursor, pattern, scanCount);
				scanCursor = scanResult.cursor;

				let consumed = 0;
				for (const key of scanResult.keys) {
					if (prefix && !key.startsWith(prefix)) {
						continue;
					}

					consumed++;
					if (consumed <= skip) {
						continue;
					}

					keys.push(key);

					if (limit && keys.length >= limit) {
						const exhausted = consumed === scanResult.keys.filter((k) => !prefix || k.startsWith(prefix)).length;

						// Resuming inside this batch means re-issuing the cursor that
						// produced it and skipping what has been handed out.
						if (!exhausted) {
							return {
								keys: keys.map((name) => ({ name })),
								cursor: encodeScanCursor(batchCursor, consumed),
								list_complete: false
							};
						}

						return {
							keys: keys.map((name) => ({ name })),
							cursor: scanCursor === '0' ? undefined : encodeScanCursor(scanCursor, 0),
							list_complete: scanCursor === '0'
						};
					}
				}

				skip = 0;

				if (scanCursor === '0') {
					return {
						keys: keys.map((name) => ({ name })),
						cursor: undefined,
						list_complete: true
					};
				}
			}
		}
	};

	if (multiGet) {
		provider.getMany = async <T = unknown>(keys: string[], type: 'text' | 'json' = 'text'): Promise<any> => {
			if (keys.length === 0) {
				return [];
			}

			const raw = await multiGet.call(client, keys);
			return keys.map((key, index) => parse<T>(raw[index] ?? null, key, type));
		};
	}

	if (client.mSet || client.mset) {
		provider.putMany = async (entries) => {
			if (entries.length === 0) {
				return;
			}

			if (client.mSet) {
				await client.mSet(entries.map((entry) => [entry.key, entry.value] as [string, string]));
				return;
			}

			const record: Record<string, string> = {};
			for (const entry of entries) {
				record[entry.key] = entry.value;
			}
			await client.mset!(record);
		};
	}

	if (client.unlink) {
		provider.deleteMany = async (keys) => {
			if (keys.length > 0) {
				await client.unlink!(...keys);
			}
		};
	}

	return provider;
}

/**
 * Splits a `list` cursor into the SCAN cursor and the in-batch offset.
 *
 * Plain numeric cursors from an earlier version are read as a SCAN cursor with
 * no offset, so an in-flight pagination keeps working across an upgrade.
 * @private
 */
function decodeScanCursor(cursor: string | undefined): { scanCursor: string; skip: number } {
	if (!cursor) {
		return { scanCursor: '0', skip: 0 };
	}

	const separator = cursor.lastIndexOf('|');
	if (separator === -1) {
		return { scanCursor: cursor, skip: 0 };
	}

	const skip = Number.parseInt(cursor.slice(separator + 1), 10);
	return {
		scanCursor: cursor.slice(0, separator) || '0',
		skip: Number.isFinite(skip) && skip > 0 ? skip : 0
	};
}

/**
 * Builds a `list` cursor from a SCAN cursor and an in-batch offset.
 * @private
 */
function encodeScanCursor(scanCursor: string, skip: number): string {
	return `${scanCursor}|${skip}`;
}

/**
 * Creates a Valkey-backed KV provider adapter.
 *
 * Valkey and Redis have compatible command surfaces for this adapter.
 *
 * @param client - Valkey client
 * @param options - Adapter tuning options
 * @returns KVStorage-compatible adapter
 */
export function createValkeyKVProvider(client: RedisLikeClient, options: { scanCount?: number } = {}): KVStorage {
	return createRedisKVProvider(client, options);
}

/**
 * Creates a PostgreSQL adapter implementing CollegeDB's SQL contract.
 *
 * Supports `pg` Client, Pool, and compatible implementations.
 * SQL using `?` placeholders is automatically rewritten to `$1..$n`.
 *
 * For Drizzle-backed PostgreSQL clients, pass the Drizzle DB instance as
 * `client` and the Drizzle `sql` helper as the second argument.
 *
 * @param client - PostgreSQL client/pool
 * @param options - Batch lease options, see {@link SQLBatchOptions}
 * @returns SQLDatabase-compatible adapter
 */
export function createPostgreSQLProvider(client: PostgresClientLike, options?: SQLBatchOptions<PostgresClientLike>): SQLDatabase;
export function createPostgreSQLProvider(client: DrizzleClientLike, sqlTag: DrizzleSqlTagLike): SQLDatabase;
export function createPostgreSQLProvider(
	client: PostgresClientLike | DrizzleClientLike,
	sqlTagOrOptions?: DrizzleSqlTagLike | SQLBatchOptions<PostgresClientLike>
): SQLDatabase {
	if (typeof sqlTagOrOptions === 'function') {
		return createDrizzleSQLProvider(client as DrizzleClientLike, sqlTagOrOptions, 'postgres');
	}

	const batch = resolveLease<PostgresClientLike>(client, sqlTagOrOptions);
	const lease = batch?.lease;
	const provider: SQLDatabase = {
		dialect: 'postgres',
		consistency: SINGLE_NODE_CONSISTENCY,
		prepare(sql: string): PreparedStatement {
			return new PostgresPreparedStatement(client as PostgresClientLike, sql, [], batch?.gate);
		}
	};

	if (lease) {
		provider.atomicBatch = true;
		provider.runBatch = async <T = Record<string, unknown>>(statements: BatchStatement[]): Promise<QueryResult<T>[]> => {
			if (statements.length === 0) {
				return [];
			}

			return (await runBatchInTransaction(lease, statements, (leased, statement) =>
				new PostgresPreparedStatement(leased, statement.sql, statement.bindings ?? []).run<T>()
			)) as QueryResult<T>[];
		};
	}

	return provider;
}

/**
 * Creates a MySQL/MariaDB adapter implementing CollegeDB's SQL contract.
 *
 * Supports mysql2/promise clients, pools, and compatible wrappers.
 *
 * For Drizzle-backed MySQL/MariaDB clients, pass the Drizzle DB instance as
 * `client` and the Drizzle `sql` helper as the second argument.
 *
 * @param client - MySQL/MariaDB client or pool
 * @param options - Batch lease options, see {@link SQLBatchOptions}
 * @returns SQLDatabase-compatible adapter
 */
export function createMySQLProvider(client: MySQLClientLike, options?: SQLBatchOptions<MySQLClientLike>): SQLDatabase;
export function createMySQLProvider(client: DrizzleClientLike, sqlTag: DrizzleSqlTagLike): SQLDatabase;
export function createMySQLProvider(
	client: MySQLClientLike | DrizzleClientLike,
	sqlTagOrOptions?: DrizzleSqlTagLike | SQLBatchOptions<MySQLClientLike>
): SQLDatabase {
	if (typeof sqlTagOrOptions === 'function') {
		return createDrizzleSQLProvider(client as DrizzleClientLike, sqlTagOrOptions, 'mysql');
	}

	const batch = resolveLease<MySQLClientLike>(client, sqlTagOrOptions);
	const lease = batch?.lease;
	const provider: SQLDatabase = {
		dialect: 'mysql',
		consistency: SINGLE_NODE_CONSISTENCY,
		prepare(sql: string): PreparedStatement {
			return new MySQLPreparedStatement(client as MySQLClientLike, sql, [], batch?.gate);
		}
	};

	if (lease) {
		provider.atomicBatch = true;
		provider.runBatch = async <T = Record<string, unknown>>(statements: BatchStatement[]): Promise<QueryResult<T>[]> => {
			if (statements.length === 0) {
				return [];
			}

			return (await runBatchInTransaction(
				lease,
				statements,
				(leased, statement) => new MySQLPreparedStatement(leased, statement.sql, statement.bindings ?? []).run<T>(),
				// `mysql2`'s execute() speaks the prepared-statement protocol, which
				// answers BEGIN with "This command is not supported in the prepared
				// statement protocol yet". query() is the text protocol and takes it.
				async (leased, sql) => (leased.query ? await leased.query(sql) : await leased.execute!(sql))
			)) as QueryResult<T>[];
		};
	}

	return provider;
}

/**
 * Creates a SQLite adapter implementing CollegeDB's SQL contract.
 *
 * Supports both execute-style clients and prepare/run/get/all statement clients.
 *
 * For Drizzle-backed SQLite/D1 clients, pass the Drizzle DB instance as
 * `client` and the Drizzle `sql` helper as the second argument.
 *
 * @param client - SQLite client
 * @param options - Batch lease options, see {@link SQLBatchOptions}
 * @returns SQLDatabase-compatible adapter
 */
export function createSQLiteProvider(client: SQLiteClientLike, options?: SQLBatchOptions<SQLiteClientLike>): SQLDatabase;
export function createSQLiteProvider(client: DrizzleClientLike, sqlTag: DrizzleSqlTagLike): SQLDatabase;
export function createSQLiteProvider(
	client: SQLiteClientLike | DrizzleClientLike,
	sqlTagOrOptions?: DrizzleSqlTagLike | SQLBatchOptions<SQLiteClientLike>
): SQLDatabase {
	if (typeof sqlTagOrOptions === 'function') {
		return createDrizzleSQLProvider(client as DrizzleClientLike, sqlTagOrOptions, 'sqlite');
	}

	// A SQLite handle is a file, not a pool, so there is nothing to lease and a
	// transaction is always on the one connection. It still has to be asked for,
	// because the handle could be a wrapper that serializes elsewhere.
	const batch = resolveLease<SQLiteClientLike>(client, sqlTagOrOptions);
	const lease = batch?.lease;
	const provider: SQLDatabase = {
		dialect: 'sqlite',
		consistency: SINGLE_NODE_CONSISTENCY,
		prepare(sql: string): PreparedStatement {
			return new SQLitePreparedStatement(client as SQLiteClientLike, sql, [], batch?.gate);
		}
	};

	if (lease) {
		provider.atomicBatch = true;
		provider.runBatch = async <T = Record<string, unknown>>(statements: BatchStatement[]): Promise<QueryResult<T>[]> => {
			if (statements.length === 0) {
				return [];
			}

			return (await runBatchInTransaction(lease, statements, (leased, statement) =>
				new SQLitePreparedStatement(leased, statement.sql, statement.bindings ?? []).run<T>()
			)) as QueryResult<T>[];
		};
	}

	return provider;
}

/**
 * Creates a Drizzle-backed SQL adapter implementing CollegeDB's SQL contract.
 *
 * This adapter enables using Drizzle database instances (including NuxtHub's
 * `db` from `@nuxthub/db` or `hub:db`) as shard providers while CollegeDB keeps
 * query routing and key->shard mapping responsibilities.
 *
 * @param client - Drizzle database instance
 * @param sqlTag - Drizzle `sql` helper from `drizzle-orm`
 * @returns SQLDatabase-compatible adapter
 */
export function createDrizzleSQLProvider(client: DrizzleClientLike, sqlTag: DrizzleSqlTagLike, dialect?: SQLDialect): SQLDatabase {
	const provider: SQLDatabase = {
		dialect,
		consistency: SINGLE_NODE_CONSISTENCY,
		prepare(sql: string): PreparedStatement {
			return new DrizzlePreparedStatement(client, sqlTag, sql, [], dialect);
		}
	};

	// Drizzle owns the connection, so its own transaction is the only sound way
	// to bracket a group here: there is nothing to lease and no way to tell
	// whether the driver underneath is pooled.
	//
	// Except on a driver that has a native batch, which is Drizzle's marker for
	// a backend without interactive transactions. D1 is the case that matters:
	// `db.transaction()` exists there and resolves against a mock, but a real D1
	// answers the `begin` it emits with `Failed query: begin`. Those keep the
	// sequential path; the raw D1 binding gets its native batch elsewhere.
	const hasNativeBatch = typeof (client as { batch?: unknown }).batch === 'function';

	if (typeof client.transaction === 'function' && !hasNativeBatch) {
		// bun:sqlite and better-sqlite3 transactions are synchronous: drizzle commits as
		// soon as the callback returns, so an async callback commits before its
		// statements run. The callback stays synchronous until a driver returns a promise.
		const transaction = client.transaction.bind(client) as unknown as (callback: (tx: DrizzleClientLike) => unknown) => unknown;

		provider.runBatch = async <T = Record<string, unknown>>(statements: BatchStatement[]): Promise<QueryResult<T>[]> => {
			if (statements.length === 0) {
				return [];
			}

			return (await transaction((tx) => {
				const results: QueryResult<T>[] = [];
				const runFrom = (start: number): QueryResult<T>[] | Promise<QueryResult<T>[]> => {
					for (let index = start; index < statements.length; index++) {
						const statement = statements[index]!;
						const startedAt = nowMs();
						const raw = dispatchDrizzleRun(tx, buildDrizzleQuery(sqlTag, statement.sql, statement.bindings ?? []));

						if (typeof (raw as PromiseLike<unknown> | null)?.then === 'function') {
							return Promise.resolve(raw as PromiseLike<unknown>).then((value) => {
								results.push(toDrizzleRunResult<T>(value, startedAt, dialect));
								return runFrom(index + 1);
							});
						}
						results.push(toDrizzleRunResult<T>(raw, startedAt, dialect));
					}
					return results;
				};

				return runFrom(0);
			})) as QueryResult<T>[];
		};
		provider.atomicBatch = true;
	}

	return provider;
}

/**
 * Creates a NuxtHub KV adapter implementing CollegeDB's KV contract.
 *
 * Supports both `@nuxthub/kv` (`get/set/del/keys`) and unstorage-like
 * implementations (`getItem/setItem/removeItem/getKeys`).
 *
 * @param client - NuxtHub KV/Unstorage-like client
 * @returns KVStorage-compatible adapter
 */
export function createNuxtHubKVProvider(client: NuxtHubKVLike): KVStorage {
	return {
		async get<T = unknown>(key: string, type: 'text' | 'json' = 'text'): Promise<T | string | null> {
			const raw = await getNuxtHubKVValue(client, key);
			if (raw === null || raw === undefined) {
				return null;
			}

			if (type === 'json') {
				if (typeof raw === 'string') {
					try {
						return JSON.parse(raw) as T;
					} catch (error) {
						throw new CollegeDBError(
							`Failed to parse JSON from NuxtHub KV for key ${key}: ${error instanceof Error ? error.message : String(error)}`,
							'KV_JSON_PARSE_FAILED'
						);
					}
				}

				return raw as T;
			}

			return typeof raw === 'string' ? raw : JSON.stringify(raw);
		},

		async put(key: string, value: string): Promise<void> {
			await setNuxtHubKVValue(client, key, value);
		},

		async delete(key: string): Promise<void> {
			await deleteNuxtHubKVValue(client, key);
		},

		async list(options?: { prefix?: string; cursor?: string; limit?: number }): Promise<KVListResult> {
			const prefix = options?.prefix ?? '';
			const allKeys = await listNuxtHubKVKeys(client, prefix);

			// The underlying client has no cursor of its own, so paginate over the
			// returned key list by offset. Reporting `list_complete: true` while
			// silently truncating at `limit` made a paginated caller lose keys.
			const start = decodeOffsetCursor(options?.cursor);
			const limit = typeof options?.limit === 'number' && options.limit > 0 ? options.limit : undefined;
			const end = limit === undefined ? allKeys.length : Math.min(allKeys.length, start + limit);
			const page = allKeys.slice(start, end);
			const complete = end >= allKeys.length;

			return {
				keys: page.map((name) => ({ name })),
				list_complete: complete,
				cursor: complete ? undefined : String(end)
			};
		}
	};
}

/**
 * Reads an offset-style list cursor, treating anything unparseable as the start.
 * @private
 */
function decodeOffsetCursor(cursor: string | undefined): number {
	if (!cursor) {
		return 0;
	}

	const parsed = Number.parseInt(cursor, 10);
	return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
}

/**
 * Options controlling how a Hyperdrive-backed provider manages its client.
 * @since 1.3.0
 */
export interface HyperdriveProviderOptions {
	/**
	 * Returns an object identifying the current request, typically the `Request`
	 * itself. The provider keeps one client per distinct token in a `WeakMap`, so
	 * a new request transparently gets a new client and the old one is released
	 * without an explicit {@link HyperdriveProvider.dispose} call.
	 *
	 * Use this when a single CollegeDB configuration is shared across requests,
	 * which is what every module-scope `initialize` does.
	 */
	scope?: () => object | undefined;
}

/**
 * A Hyperdrive-backed provider plus the hook that releases its connection.
 * @since 1.3.0
 */
export interface HyperdriveProvider extends SQLDatabase {
	/**
	 * Closes the client held for the current request.
	 *
	 * Call it once per request, typically as `ctx.waitUntil(provider.dispose())`.
	 * Unnecessary when `scope` is configured.
	 */
	dispose(): Promise<void>;
}

/**
 * Orders the statements of one connection around its batches.
 *
 * A single connection shares itself among every operation that uses it, so a
 * plain statement sent while a batch is open would run inside that transaction
 * and be committed or rolled back with it. A batch takes the gate with `enter`,
 * and a plain statement waits at `idle` until every batch queued so far has
 * finished. Plain statements check `busy` first, so an idle gate costs no
 * promise and no extra tick.
 * @private
 */
class BatchGate {
	private tail: Promise<void> = Promise.resolve();
	private open = 0;

	/** True from the moment a batch asks to enter until it releases, so a plain statement can skip the wait */
	get busy(): boolean {
		return this.open > 0;
	}

	idle(): Promise<void> {
		return this.tail;
	}

	async enter(): Promise<() => void> {
		const previous = this.tail;
		let release!: () => void;
		this.tail = new Promise<void>((resolve) => {
			release = resolve;
		});
		this.open++;
		await previous;

		let released = false;
		return () => {
			if (!released) {
				released = true;
				this.open--;
				release();
			}
		};
	}
}

const batchGates = new WeakMap<object, BatchGate>();

function gateOf(connection: object): BatchGate {
	let gate = batchGates.get(connection);
	if (!gate) {
		gate = new BatchGate();
		batchGates.set(connection, gate);
	}
	return gate;
}

/**
 * Tracks one client per request scope without keeping the scope object alive.
 * @private
 */
function createScopedClientCache<C>(factory: () => C, close: (client: C) => Promise<void>, scope?: () => object | undefined) {
	const byScope = new WeakMap<object, C>();
	let unscoped: C | null = null;

	return {
		acquire(): C {
			const token = scope?.();
			if (token) {
				const existing = byScope.get(token);
				if (existing) {
					return existing;
				}
				const created = factory();
				byScope.set(token, created);
				return created;
			}

			unscoped ??= factory();
			return unscoped;
		},
		async dispose(): Promise<void> {
			const token = scope?.();
			if (token) {
				const existing = byScope.get(token);
				if (existing) {
					byScope.delete(token);
					await close(existing);
				}
				return;
			}

			if (unscoped) {
				const client = unscoped;
				unscoped = null;
				await close(client);
			}
		}
	};
}

/**
 * Creates a PostgreSQL adapter wired to a Hyperdrive binding.
 *
 * The client is created once per request and reused for every statement in that
 * request, which is the pattern Cloudflare documents: Hyperdrive pools the
 * underlying connection, so creating a client is cheap, but each `connect()`
 * still acquires a pooled connection and the pooler runs in transaction mode.
 * Opening one per statement therefore acquires and releases a pooled connection
 * per statement and forfeits named prepared statements.
 *
 * Caching a client in module scope is not the alternative: Workers reject I/O
 * created by one request and used by another. Either pass `scope` so the
 * provider can key its client on the current request, or call
 * {@link HyperdriveProvider.dispose} at the end of each request.
 *
 * The factory must return one connection, such as a `pg` `Client`. The provider
 * has a {@link SQLDatabase.runBatch} that runs its group in one transaction on
 * that connection and declares {@link SQLDatabase.atomicBatch}. A batch runs
 * alone: a plain statement from the same request waits until it commits, so it
 * cannot slip into the open transaction.
 *
 * @param hyperdrive - Hyperdrive binding
 * @param clientFactory - Client factory (e.g. `connectionString => new Client({ connectionString })`)
 * @param options - Request-scope hook
 * @returns SQLDatabase-compatible adapter with a `dispose` hook
 * @since 1.0.0
 * @example
 * ```typescript
 * const shard = createHyperdrivePostgresProvider(env.HYPERDRIVE, (connectionString) => new Client({ connectionString }), {
 * 	scope: () => currentRequest
 * });
 * ```
 */
export function createHyperdrivePostgresProvider(
	hyperdrive: HyperdriveBindingLike,
	clientFactory: HyperdrivePostgresClientFactory,
	options: HyperdriveProviderOptions = {}
): HyperdriveProvider {
	const cache = createScopedClientCache<{ client: ReturnType<HyperdrivePostgresClientFactory>; connected: Promise<void> | null }>(
		() => ({ client: clientFactory(hyperdrive.connectionString), connected: null }),
		async (entry) => {
			if (typeof entry.client.release === 'function') {
				entry.client.release();
			} else if (typeof entry.client.end === 'function') {
				await entry.client.end();
			}
		},
		options.scope
	);

	const connect = async (entry: ReturnType<typeof cache.acquire>) => {
		if (typeof entry.client.connect === 'function') {
			entry.connected ??= Promise.resolve(entry.client.connect()).then(() => undefined);
			await entry.connected;
		}
	};

	const delegatedClient: PostgresClientLike = {
		query: async <T = Record<string, unknown>>(sql: string, bindings: any[] = []) => {
			const entry = cache.acquire();
			await gateOf(entry).idle();
			await connect(entry);

			return await entry.client.query<T>(sql, bindings);
		}
	};

	// the request's client is one connection, so a batch holds the gate and talks to it directly
	const lease = async (): Promise<LeasedConnection<PostgresClientLike>> => {
		const entry = cache.acquire();
		const release = await gateOf(entry).enter();
		try {
			await connect(entry);
		} catch (error) {
			release();
			throw error;
		}
		return { client: { query: (sql, bindings) => entry.client.query(sql, bindings) }, release };
	};

	const provider = createPostgreSQLProvider(delegatedClient, { lease }) as HyperdriveProvider;
	provider.dispose = () => cache.dispose();
	return provider;
}

/**
 * Creates a MySQL/MariaDB adapter wired to a Hyperdrive binding.
 *
 * The client is created once per request and reused for every statement in that
 * request. See {@link createHyperdrivePostgresProvider} for why per-statement
 * clients are the wrong unit and why a module-scoped client is not the fix.
 *
 * The factory must return one connection, such as a `mysql2` connection, with
 * `query` for transaction control. Batches work as they do for
 * {@link createHyperdrivePostgresProvider}.
 *
 * @param hyperdrive - Hyperdrive binding
 * @param clientFactory - Client factory (e.g. `connectionString => mysql.createConnection(connectionString)`)
 * @param options - Request-scope hook
 * @returns SQLDatabase-compatible adapter with a `dispose` hook
 * @since 1.0.0
 */
export function createHyperdriveMySQLProvider(
	hyperdrive: HyperdriveBindingLike,
	clientFactory: HyperdriveMySQLClientFactory,
	options: HyperdriveProviderOptions = {}
): HyperdriveProvider {
	const cache = createScopedClientCache<ReturnType<HyperdriveMySQLClientFactory>>(
		() => clientFactory(hyperdrive.connectionString),
		async (client) => {
			if (typeof client.end === 'function') {
				await client.end();
			} else if (typeof client.close === 'function') {
				await client.close();
			} else if (typeof client.destroy === 'function') {
				client.destroy();
			}
		},
		options.scope
	);

	const delegatedClient: MySQLClientLike = {
		execute: async (sql: string, bindings: any[] = []) => {
			const client = cache.acquire();
			await gateOf(client).idle();

			if (typeof client.execute === 'function') {
				return await client.execute(sql, bindings);
			}
			if (typeof client.query === 'function') {
				return await client.query(sql, bindings);
			}

			throw new CollegeDBError('Hyperdrive MySQL client is missing execute/query methods', 'MYSQL_CLIENT_INVALID');
		}
	};

	// the request's client is one connection, so a batch holds the gate and uses it directly
	const lease = async (): Promise<LeasedConnection<MySQLClientLike>> => {
		const client = cache.acquire();
		return { client, release: await gateOf(client).enter() };
	};

	const provider = createMySQLProvider(delegatedClient, { lease }) as HyperdriveProvider;
	provider.dispose = () => cache.dispose();
	return provider;
}

/**
 * Returns `true` when a value looks like an SQL database provider.
 */
export function isSQLDatabase(value: unknown): value is SQLDatabase {
	if (!value || typeof value !== 'object') {
		return false;
	}

	return typeof (value as SQLDatabase).prepare === 'function';
}

/**
 * Returns `true` when a value looks like a KV storage provider.
 */
export function isKVStorage(value: unknown): value is KVStorage {
	if (!value || typeof value !== 'object') {
		return false;
	}

	const kv = value as KVStorage;
	return typeof kv.get === 'function' && typeof kv.put === 'function' && typeof kv.delete === 'function' && typeof kv.list === 'function';
}

/**
 * Options for {@link toProvider}.
 */
export interface ToProviderOptions {
	/**
	 * Drizzle `sql` tag from `drizzle-orm`. Required to wrap a raw Drizzle
	 * client (one exposing `run`/`all`/`get`/`execute`) into a shard provider.
	 */
	sql?: DrizzleSqlTagLike;
}

/**
 * Minimal raw Cloudflare Workers `KVNamespace` shape consumed by
 * {@link createWorkersKVProvider}. Declared structurally so the library keeps
 * zero runtime dependency on `@cloudflare/workers-types`.
 */
export interface WorkersKVNamespaceLike {
	get(key: string, type?: 'text'): Promise<string | null>;
	put(key: string, value: string, options?: { expirationTtl?: number; expiration?: number; metadata?: unknown }): Promise<void>;
	delete(key: string): Promise<void>;
	list(options?: {
		prefix?: string;
		cursor?: string;
		limit?: number;
	}): Promise<{ keys: Array<{ name: string; expiration?: number; metadata?: unknown }>; list_complete?: boolean; cursor?: string }>;
}

/**
 * Bulk-read form of the namespace, used by `getMany`. Native `KVNamespace`
 * bindings have it; a hand-written namespace must too before `getMany` is safe.
 * Kept off {@link WorkersKVNamespaceLike} so existing namespace shapes still
 * compile.
 * @private
 */
interface WorkersKVBulkGetLike {
	get(keys: string[], options: { type: 'text'; cacheTtl?: number }): Promise<Map<string, string | null>>;
}

/**
 * Detects the right CollegeDB shard provider for an arbitrary binding.
 *
 * This removes the per-app "is this a D1 binding, a Drizzle client, or a raw
 * SQLite client?" wiring that every consumer otherwise rewrites. Resolution
 * order:
 *
 * 1. A raw Drizzle client (`run`/`all`/`get`/`execute`) is wrapped with
 *    {@link createDrizzleSQLProvider} when `options.sql` is supplied.
 * 2. A value already implementing the {@link SQLDatabase} contract (a D1
 *    binding, or a provider from any `create*Provider`) is returned as-is.
 * 3. A client exposing `prepare(sql)` is wrapped with
 *    {@link createSQLiteProvider}.
 * 4. Anything else returns `null` (unrecognized).
 *
 * @param binding - Candidate database binding
 * @param options - Resolution options (Drizzle `sql` tag)
 * @returns A shard provider, or `null` when the binding is not recognized
 * @since 1.2.4
 * @example
 * ```typescript
 * import { sql } from 'drizzle-orm';
 * const provider = toProvider(env.DB_EAST, { sql });
 * if (!provider) throw new Error('Unrecognized database binding');
 * ```
 */
export function toProvider(binding: unknown, options: ToProviderOptions = {}): SQLDatabase | null {
	if (!binding || typeof binding !== 'object') {
		return null;
	}

	const candidate = binding as DrizzleClientLike & { prepare?: unknown };
	const looksDrizzle =
		typeof candidate.run === 'function' ||
		typeof candidate.all === 'function' ||
		typeof candidate.get === 'function' ||
		typeof candidate.execute === 'function';

	if (looksDrizzle && options.sql) {
		return createDrizzleSQLProvider(candidate, options.sql);
	}

	if (isSQLDatabase(binding)) {
		return withNativeBatch(binding);
	}

	if (typeof candidate.prepare === 'function') {
		return withNativeBatch(createSQLiteProvider(binding as SQLiteClientLike), binding);
	}

	return null;
}

/**
 * Shape of a D1-style `batch`, which takes prepared statements rather than SQL.
 * @private
 */
interface NativeBatchCapable {
	prepare(sql: string): { bind(...bindings: any[]): unknown };
	batch<T = unknown>(
		statements: unknown[]
	): Promise<Array<{ success?: boolean; results?: T[]; meta?: Record<string, unknown>; error?: string }>>;
}

/**
 * Adds {@link SQLDatabase.runBatch} when the binding has a native batch.
 *
 * Only D1 and the Drizzle drivers built on it are wired up here. The `pg`,
 * `mysql2` and SQLite adapters are left without `runBatch` on purpose: their
 * statements already travel over one connection, so grouping saves little, and
 * wrapping them in `BEGIN`/`COMMIT` through a pool would send the transaction
 * control statements down a different connection than the statements they are
 * meant to bracket. CollegeDB falls back to sequential execution for those,
 * which is what they did before.
 *
 * @private
 */
/**
 * Resolves how a batch should get a connection, or `null` when it must not.
 *
 * Autodetection is deliberately one-directional: it recognizes the two pool
 * shapes it knows how to lease from and says nothing about anything else. A
 * handle it does not recognize is left un-batched rather than assumed to be a
 * single connection, because assuming wrong sends `BEGIN` down a different
 * connection than the statements it is supposed to bracket.
 *
 * `gate` is set only when the batch runs on one shared connection the caller
 * declared with `singleConnection`. Plain statements wait on it; a pool leases a
 * connection per batch and needs none.
 *
 * @private
 */
function resolveLease<C>(client: unknown, options: SQLBatchOptions<C> | undefined): BatchConnection<C> | null {
	if (options?.lease) {
		return options.singleConnection ? gatedLease(options.lease) : { lease: options.lease };
	}

	const candidate = client as {
		getConnection?: () => Promise<{ release?: () => void; destroy?: () => void }>;
		connect?: () => Promise<{ release?: () => void }>;
		totalCount?: number;
		idleCount?: number;
	};

	// `mysql2` pools, which hand back a connection carrying its own release.
	if (typeof candidate.getConnection === 'function') {
		return {
			lease: async () => {
				const connection = await candidate.getConnection!();
				return {
					client: connection as C,
					release: () => connection.release?.()
				};
			}
		};
	}

	// `pg` pools. `Client` also has `connect`, so the pool-only counters are what
	// separate them; `Client.connect()` resolves to nothing and would leave the
	// batch with no connection to run on.
	if (typeof candidate.connect === 'function' && typeof candidate.totalCount === 'number' && typeof candidate.idleCount === 'number') {
		return {
			lease: async () => {
				const connection = await candidate.connect!();
				return {
					client: connection as C,
					release: () => connection.release?.()
				};
			}
		};
	}

	if (options?.singleConnection) {
		return gatedLease(async () => ({ client: client as C, release: () => {} }));
	}

	return null;
}

/**
 * How a batch gets its connection, and the gate that holds plain statements back
 * while that connection is inside a transaction (set only for a shared one).
 * @private
 */
interface BatchConnection<C> {
	lease: () => Promise<LeasedConnection<C>>;
	gate?: BatchGate;
}

/**
 * Wraps a lease of one shared connection so a batch holds the gate for as long
 * as it holds the connection, including when the lease itself throws.
 * @private
 */
function gatedLease<C>(inner: () => Promise<LeasedConnection<C>>): BatchConnection<C> {
	const gate = new BatchGate();

	return {
		gate,
		lease: async () => {
			const open = await gate.enter();
			try {
				const leased = await inner();
				return {
					client: leased.client,
					release: async () => {
						try {
							await leased.release();
						} finally {
							open();
						}
					}
				};
			} catch (error) {
				open();
				throw error;
			}
		}
	};
}

/**
 * Runs a group of statements inside one transaction on one connection.
 *
 * `BEGIN` and `COMMIT` are issued through the same handle as the statements,
 * which is the whole point of the lease. A failure rolls back and rethrows, so a
 * partially applied group is never reported as success.
 *
 * @private
 */
async function runBatchInTransaction<C>(
	lease: () => Promise<LeasedConnection<C>>,
	statements: BatchStatement[],
	execute: (client: C, statement: BatchStatement) => Promise<QueryResult<any>>,
	control: (client: C, sql: string) => Promise<unknown> = (client, sql) => execute(client, { sql })
): Promise<QueryResult<any>[]> {
	const { client, release } = await lease();

	try {
		await control(client, 'BEGIN');

		try {
			const results: QueryResult<any>[] = [];
			for (const statement of statements) {
				results.push(await execute(client, statement));
			}
			await control(client, 'COMMIT');
			return results;
		} catch (error) {
			// A rollback that itself fails must not replace the original error,
			// which is the one that explains what went wrong.
			await Promise.resolve(control(client, 'ROLLBACK')).catch(() => undefined);
			throw error;
		}
	} finally {
		await release();
	}
}

function withNativeBatch(provider: SQLDatabase, rawBinding: unknown = provider): SQLDatabase {
	const candidate = rawBinding as Partial<NativeBatchCapable>;
	if (typeof candidate.batch !== 'function' || typeof candidate.prepare !== 'function' || provider.runBatch) {
		return provider;
	}

	const native = candidate as NativeBatchCapable;
	const session = (rawBinding as Partial<SQLDatabase>).withSession;

	// this wrapper replaces the binding, so it has to pass sessions through or D1 loses them
	return {
		dialect: provider.dialect,
		// local workerd d1 rolled a failing group back (production d1 is unproven)
		atomicBatch: true,
		...(provider.consistency ? { consistency: provider.consistency } : {}),
		...(typeof session === 'function' ? { withSession: (constraint?: string) => session.call(rawBinding, constraint) } : {}),
		prepare: (sql: string) => provider.prepare(sql),
		async runBatch<T = Record<string, unknown>>(statements: BatchStatement[]): Promise<QueryResult<T>[]> {
			if (statements.length === 0) {
				return [];
			}

			const prepared = statements.map((statement) => {
				const stmt = native.prepare(statement.sql);
				return statement.bindings && statement.bindings.length > 0 ? stmt.bind(...statement.bindings) : stmt;
			});

			const results = await native.batch<T>(prepared);

			return results.map((result) => ({
				success: result.success ?? true,
				results: (result.results ?? []) as T[],
				meta: { duration: 0, ...(result.meta ?? {}) },
				...(result.error ? { error: result.error } : {})
			}));
		}
	};
}

/**
 * Default `cacheTtl` for Workers KV reads, in seconds.
 *
 * Cloudflare's guidance is to raise this above the 60 second default, and shard
 * mappings are close to write-once: a key keeps its shard until something calls
 * `reassignShard`. CollegeDB already tolerates a stale mapping for
 * `mappingCacheTtlMs` in process, so a longer edge TTL does not widen the
 * window in kind, only in duration.
 * @private
 */
const DEFAULT_WORKERS_KV_CACHE_TTL = 3600;

/**
 * Largest key count one Workers KV bulk `get` accepts.
 * @private
 */
const WORKERS_KV_BULK_GET_LIMIT = 100;

/**
 * Bulk `get` requests in flight at once for one `getMany`.
 * @private
 */
const WORKERS_KV_BULK_GET_CONCURRENCY = 4;

/**
 * Default concurrent `put` calls for `putMany`.
 * @private
 */
const DEFAULT_WORKERS_KV_PUT_CONCURRENCY = 16;

/**
 * Options for {@link createWorkersKVProvider}.
 * @since 1.3.0
 */
export interface WorkersKVProviderOptions {
	/**
	 * Edge cache lifetime for reads, in seconds. Set `0` to send no `cacheTtl`
	 * and use Cloudflare's own default.
	 * @default 3600
	 */
	cacheTtl?: number;
	/**
	 * Concurrent `put` calls used by `putMany`.
	 * @default 16
	 * @since 1.4.0
	 */
	putConcurrency?: number;
}

/**
 * Wraps a raw Cloudflare Workers `KVNamespace` in CollegeDB's
 * {@link KVStorage} contract.
 *
 * Use this when your KV binding is the native Workers `KVNamespace` (from
 * `env.KV`) rather than a NuxtHub/Unstorage-style client (see
 * {@link createNuxtHubKVProvider} for that). The adapter maps `get`/`put`/
 * `delete`/`list` directly and preserves the paginated `list` cursor. `getMany`
 * uses native bulk reads; `putMany` is a bounded set of single `put` calls.
 *
 * @param kv - Raw Workers KV namespace
 * @returns KVStorage-compatible adapter
 * @since 1.2.4
 * @example
 * ```typescript
 * initialize({
 *   kv: createWorkersKVProvider(env.KV),
 *   shards: { 'db-east': env.DB_EAST },
 *   strategy: 'hash'
 * });
 * ```
 */
export function createWorkersKVProvider(kv: WorkersKVNamespaceLike, options: WorkersKVProviderOptions = {}): KVStorage {
	const cacheTtl = options.cacheTtl ?? DEFAULT_WORKERS_KV_CACHE_TTL;
	const getOptions = cacheTtl > 0 ? { type: 'text' as const, cacheTtl } : { type: 'text' as const };
	const putConcurrency = Math.max(1, options.putConcurrency ?? DEFAULT_WORKERS_KV_PUT_CONCURRENCY);
	const bulk = kv as WorkersKVNamespaceLike & WorkersKVBulkGetLike;

	return {
		async get<T = unknown>(key: string, type: 'text' | 'json' = 'text'): Promise<T | string | null> {
			return decodeWorkersKVValue<T>(await kv.get(key, getOptions as any), key, type);
		},

		/**
		 * Reads keys with Workers KV bulk gets of up to 100 keys each, up to four
		 * requests in flight. Results follow the order of `keys`; a missing key is `null`.
		 * @since 1.4.0
		 */
		async getMany<T = unknown>(keys: string[], type: 'text' | 'json' = 'text'): Promise<any> {
			const values = new Array<string | null>(keys.length);
			const chunkCount = Math.ceil(keys.length / WORKERS_KV_BULK_GET_LIMIT);
			const starts = Array.from({ length: chunkCount }, (_, i) => i * WORKERS_KV_BULK_GET_LIMIT);

			await runBounded(starts, WORKERS_KV_BULK_GET_CONCURRENCY, async (start) => {
				const chunk = keys.slice(start, start + WORKERS_KV_BULK_GET_LIMIT);
				const found = await bulk.get(chunk, getOptions);
				chunk.forEach((key, offset) => {
					values[start + offset] = found.get(key) ?? null;
				});
			});

			return keys.map((key, index) => decodeWorkersKVValue<T>(values[index] ?? null, key, type));
		},

		async put(key: string, value: string): Promise<void> {
			await kv.put(key, value);
		},

		/**
		 * Writes each entry with its own `put`, up to `putConcurrency` at a time.
		 *
		 * Not atomic. A failed `put` rejects with `KV_PUT_MANY_FAILED` and names the
		 * key. Writes already in flight finish, no new write starts after the failure,
		 * and entries written before it stay written.
		 * @since 1.4.0
		 */
		async putMany(entries: Array<{ key: string; value: string }>): Promise<void> {
			await runBounded(entries, putConcurrency, async ({ key, value }) => {
				try {
					await kv.put(key, value);
				} catch (error) {
					throw new CollegeDBError(
						`Workers KV putMany failed at key ${key}: ${error instanceof Error ? error.message : String(error)}`,
						'KV_PUT_MANY_FAILED'
					);
				}
			});
		},

		async delete(key: string): Promise<void> {
			await kv.delete(key);
		},

		async list(options?: { prefix?: string; cursor?: string; limit?: number }): Promise<KVListResult> {
			const result = await kv.list(options);
			return {
				keys: result.keys.map((item) => ({ name: item.name, expiration: item.expiration, metadata: item.metadata })),
				list_complete: result.list_complete ?? true,
				cursor: result.cursor
			};
		}
	};
}

/**
 * Decodes a raw Workers KV value: `null` for a missing key, parsed JSON for `json`.
 * @private
 */
function decodeWorkersKVValue<T>(raw: string | null | undefined, key: string, type: 'text' | 'json'): T | string | null {
	if (raw === null || raw === undefined) {
		return null;
	}

	if (type === 'json') {
		try {
			return JSON.parse(raw) as T;
		} catch (error) {
			throw new CollegeDBError(
				`Failed to parse JSON from Workers KV for key ${key}: ${error instanceof Error ? error.message : String(error)}`,
				'KV_JSON_PARSE_FAILED'
			);
		}
	}

	return raw;
}

/**
 * Runs `task` over `items` with at most `concurrency` in flight. After a failure
 * no new task starts; the first error is rethrown once in-flight tasks settle.
 * @private
 */
async function runBounded<T>(items: readonly T[], concurrency: number, task: (item: T) => Promise<void>): Promise<void> {
	// object, not let: TS narrows a let written only in closures to undefined
	const state: { next: number; failure?: { error: unknown } } = { next: 0 };

	const worker = async () => {
		while (!state.failure && state.next < items.length) {
			const item = items[state.next++] as T;
			try {
				await task(item);
			} catch (error) {
				if (!state.failure) {
					state.failure = { error };
				}
			}
		}
	};

	await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, worker));
	if (state.failure) {
		throw state.failure.error;
	}
}

class PostgresPreparedStatement implements PreparedStatement {
	private readonly client: PostgresClientLike;
	private readonly sql: string;
	private readonly bindings: any[];
	private readonly gate: BatchGate | undefined;

	constructor(client: PostgresClientLike, sql: string, bindings: any[] = [], gate?: BatchGate) {
		this.client = client;
		this.sql = sql;
		this.bindings = bindings;
		this.gate = gate;
	}

	bind(...bindings: any[]): PreparedStatement {
		return new PostgresPreparedStatement(this.client, this.sql, bindings, this.gate);
	}

	async run<T = Record<string, unknown>>(): Promise<QueryResult<T>> {
		if (this.gate?.busy) await this.gate.idle();
		const startedAt = nowMs();
		const sql = rewriteQuestionPlaceholders(this.sql);
		const result = await this.client.query<T>(sql, this.bindings);
		return {
			success: true,
			results: result.rows ?? [],
			meta: createMeta(startedAt, {
				changes: typeof result.rowCount === 'number' ? result.rowCount : undefined,
				command: result.command
			})
		};
	}

	async all<T = Record<string, unknown>>(): Promise<QueryResult<T>> {
		if (this.gate?.busy) await this.gate.idle();
		const startedAt = nowMs();
		const sql = rewriteQuestionPlaceholders(this.sql);
		const result = await this.client.query<T>(sql, this.bindings);
		return {
			success: true,
			results: result.rows ?? [],
			meta: createMeta(startedAt, {
				changes: typeof result.rowCount === 'number' ? result.rowCount : undefined,
				command: result.command
			})
		};
	}

	async first<T = Record<string, unknown>>(): Promise<T | null> {
		if (this.gate?.busy) await this.gate.idle();
		const sql = rewriteQuestionPlaceholders(this.sql);
		const result = await this.client.query<T>(sql, this.bindings);
		return result.rows?.[0] ?? null;
	}
}

class MySQLPreparedStatement implements PreparedStatement {
	private readonly client: MySQLClientLike;
	private readonly sql: string;
	private readonly bindings: any[];
	private readonly gate: BatchGate | undefined;

	constructor(client: MySQLClientLike, sql: string, bindings: any[] = [], gate?: BatchGate) {
		this.client = client;
		this.sql = sql;
		this.bindings = bindings;
		this.gate = gate;
	}

	bind(...bindings: any[]): PreparedStatement {
		return new MySQLPreparedStatement(this.client, this.sql, bindings, this.gate);
	}

	async run<T = Record<string, unknown>>(): Promise<QueryResult<T>> {
		if (this.gate?.busy) await this.gate.idle();
		const startedAt = nowMs();
		const rows = await executeMySQL(this.client, this.sql, this.bindings);

		if (Array.isArray(rows)) {
			return {
				success: true,
				results: rows as T[],
				meta: createMeta(startedAt)
			};
		}

		const packet = rows as MySQLOkPacket;
		return {
			success: true,
			results: [],
			meta: createMeta(startedAt, {
				changes: packet.affectedRows,
				last_row_id: packet.insertId,
				warningStatus: packet.warningStatus
			})
		};
	}

	async all<T = Record<string, unknown>>(): Promise<QueryResult<T>> {
		if (this.gate?.busy) await this.gate.idle();
		const startedAt = nowMs();
		const rows = await executeMySQL(this.client, this.sql, this.bindings);
		return {
			success: true,
			results: Array.isArray(rows) ? (rows as T[]) : [],
			meta: createMeta(startedAt, {
				changes: !Array.isArray(rows) ? (rows as MySQLOkPacket).affectedRows : undefined
			})
		};
	}

	async first<T = Record<string, unknown>>(): Promise<T | null> {
		if (this.gate?.busy) await this.gate.idle();
		const rows = await executeMySQL(this.client, this.sql, this.bindings);
		if (!Array.isArray(rows) || rows.length === 0) {
			return null;
		}

		return rows[0] as T;
	}
}

class SQLitePreparedStatement implements PreparedStatement {
	private readonly client: SQLiteClientLike;
	private readonly sql: string;
	private readonly bindings: any[];
	private readonly gate: BatchGate | undefined;

	constructor(client: SQLiteClientLike, sql: string, bindings: any[] = [], gate?: BatchGate) {
		this.client = client;
		this.sql = sql;
		this.bindings = bindings;
		this.gate = gate;
	}

	bind(...bindings: any[]): PreparedStatement {
		return new SQLitePreparedStatement(this.client, this.sql, bindings, this.gate);
	}

	async run<T = Record<string, unknown>>(): Promise<QueryResult<T>> {
		if (this.gate?.busy) await this.gate.idle();
		const startedAt = nowMs();

		if (typeof this.client.execute === 'function') {
			const result = await this.client.execute(this.sql, this.bindings);
			const rows = extractRowsFromSQLiteExecute<T>(result);
			return {
				success: true,
				results: rows,
				meta: createMeta(startedAt)
			};
		}

		const statement = this.client.prepare?.(this.sql);
		if (!statement || typeof statement.run !== 'function') {
			throw new CollegeDBError('SQLite client must expose execute() or prepare().run()', 'SQLITE_CLIENT_INVALID');
		}

		const runResult = await statement.run(...this.bindings);
		const runMeta = (runResult ?? {}) as Record<string, unknown>;
		return {
			success: true,
			results: [],
			meta: createMeta(startedAt, {
				changes: toMaybeNumber(runMeta.changes),
				last_row_id: (runMeta.lastInsertRowid ?? runMeta.lastID) as number | string | undefined
			})
		};
	}

	async all<T = Record<string, unknown>>(): Promise<QueryResult<T>> {
		if (this.gate?.busy) await this.gate.idle();
		const startedAt = nowMs();

		if (typeof this.client.execute === 'function') {
			const result = await this.client.execute(this.sql, this.bindings);
			return {
				success: true,
				results: extractRowsFromSQLiteExecute<T>(result),
				meta: createMeta(startedAt)
			};
		}

		const statement = this.client.prepare?.(this.sql);
		if (!statement || typeof statement.all !== 'function') {
			throw new CollegeDBError('SQLite client must expose execute() or prepare().all()', 'SQLITE_CLIENT_INVALID');
		}

		const rows = await statement.all(...this.bindings);
		return {
			success: true,
			results: (Array.isArray(rows) ? rows : []) as T[],
			meta: createMeta(startedAt)
		};
	}

	async first<T = Record<string, unknown>>(): Promise<T | null> {
		if (this.gate?.busy) await this.gate.idle();
		if (typeof this.client.execute === 'function') {
			const result = await this.client.execute(this.sql, this.bindings);
			const rows = extractRowsFromSQLiteExecute<T>(result);
			return rows[0] ?? null;
		}

		const statement = this.client.prepare?.(this.sql);
		if (!statement) {
			throw new CollegeDBError('SQLite client must expose execute() or prepare().get()', 'SQLITE_CLIENT_INVALID');
		}

		if (typeof statement.get === 'function') {
			const row = await statement.get(...this.bindings);
			return row === undefined || row === null ? null : (row as T);
		}

		if (typeof statement.all === 'function') {
			const rows = await statement.all(...this.bindings);
			if (!Array.isArray(rows) || rows.length === 0) {
				return null;
			}

			const firstRow = rows[0];
			return firstRow === undefined || firstRow === null ? null : (firstRow as T);
		}

		throw new CollegeDBError('SQLite prepare() result must expose get() or all()', 'SQLITE_CLIENT_INVALID');
	}
}

class DrizzlePreparedStatement implements PreparedStatement {
	private readonly client: DrizzleClientLike;
	private readonly sqlTag: DrizzleSqlTagLike;
	private readonly sqlText: string;
	private readonly bindings: any[];
	private readonly dialect: SQLDialect | undefined;

	constructor(client: DrizzleClientLike, sqlTag: DrizzleSqlTagLike, sqlText: string, bindings: any[] = [], dialect?: SQLDialect) {
		this.client = client;
		this.sqlTag = sqlTag;
		this.sqlText = sqlText;
		this.bindings = bindings;
		this.dialect = dialect;
	}

	bind(...bindings: any[]): PreparedStatement {
		return new DrizzlePreparedStatement(this.client, this.sqlTag, this.sqlText, bindings, this.dialect);
	}

	async run<T = Record<string, unknown>>(): Promise<QueryResult<T>> {
		const startedAt = nowMs();
		const query = buildDrizzleQuery(this.sqlTag, this.sqlText, this.bindings);
		return toDrizzleRunResult<T>(await executeDrizzleRun(this.client, query), startedAt, this.dialect);
	}

	async all<T = Record<string, unknown>>(): Promise<QueryResult<T>> {
		const startedAt = nowMs();
		const query = buildDrizzleQuery(this.sqlTag, this.sqlText, this.bindings);
		const result = await executeDrizzleAll(this.client, query);

		return {
			success: true,
			results: extractRowsFromDrizzleExecute<T>(result, this.dialect),
			meta: createMeta(startedAt, extractMetaFromDrizzleExecute(result, this.dialect))
		};
	}

	async first<T = Record<string, unknown>>(): Promise<T | null> {
		const query = buildDrizzleQuery(this.sqlTag, this.sqlText, this.bindings);
		const result = await executeDrizzleFirst(this.client, query);
		const rows = extractRowsFromDrizzleExecute<T>(result, this.dialect);
		if (rows.length > 0) {
			return rows[0] ?? null;
		}

		if (result && typeof result === 'object' && 'row' in result) {
			const row = (result as Record<string, unknown>).row;
			return row === undefined || row === null ? null : (row as T);
		}

		if (
			result &&
			typeof result === 'object' &&
			!Array.isArray(result) &&
			!('rows' in result) &&
			!('results' in result) &&
			!('data' in result)
		) {
			return result as T;
		}

		return null;
	}
}

async function executeRedisScan(
	client: RedisLikeClient,
	cursor: string,
	pattern: string,
	scanCount: number
): Promise<{ cursor: string; keys: string[] }> {
	try {
		const objectResult = await client.scan(cursor, { MATCH: pattern, COUNT: scanCount });
		return normalizeRedisScanResult(objectResult);
	} catch {
		const tupleResult = await client.scan(cursor, 'MATCH', pattern, 'COUNT', String(scanCount));
		return normalizeRedisScanResult(tupleResult);
	}
}

function normalizeRedisScanResult(result: RedisScanResult): { cursor: string; keys: string[] } {
	if (Array.isArray(result)) {
		return {
			cursor: String(result[0] ?? '0'),
			keys: Array.isArray(result[1]) ? result[1] : []
		};
	}

	return {
		cursor: String(result.cursor ?? '0'),
		keys: Array.isArray(result.keys) ? result.keys : []
	};
}

async function executeMySQL(client: MySQLClientLike, sql: string, bindings: any[]): Promise<unknown> {
	if (typeof client.execute === 'function') {
		const result = await client.execute(sql, bindings);
		if (Array.isArray(result)) {
			return result[0];
		}
		return result;
	}

	if (typeof client.query === 'function') {
		const result = await client.query(sql, bindings);
		if (Array.isArray(result)) {
			return result[0];
		}
		return result;
	}

	throw new CollegeDBError('MySQL client must expose execute() or query()', 'MYSQL_CLIENT_INVALID');
}

/** Calls the client's run-style method and returns what it returns, a promise or not */
function dispatchDrizzleRun(client: DrizzleClientLike, query: DrizzleSqlChunkLike): unknown {
	if (typeof client.run === 'function') {
		return client.run(query);
	}

	if (typeof client.execute === 'function') {
		return client.execute(query);
	}

	if (typeof client.all === 'function') {
		return client.all(query);
	}

	throw new CollegeDBError('Drizzle client must expose run(), execute(), or all()', 'DRIZZLE_CLIENT_INVALID');
}

async function executeDrizzleRun(client: DrizzleClientLike, query: DrizzleSqlChunkLike): Promise<unknown> {
	return await dispatchDrizzleRun(client, query);
}

function toDrizzleRunResult<T>(result: unknown, startedAt: number, dialect?: SQLDialect): QueryResult<T> {
	return {
		success: true,
		results: extractRowsFromDrizzleExecute<T>(result, dialect),
		meta: createMeta(startedAt, extractMetaFromDrizzleExecute(result, dialect))
	};
}

async function executeDrizzleAll(client: DrizzleClientLike, query: DrizzleSqlChunkLike): Promise<unknown> {
	if (typeof client.all === 'function') {
		return await client.all(query);
	}

	if (typeof client.execute === 'function') {
		return await client.execute(query);
	}

	if (typeof client.run === 'function') {
		return await client.run(query);
	}

	throw new CollegeDBError('Drizzle client must expose all(), execute(), or run()', 'DRIZZLE_CLIENT_INVALID');
}

async function executeDrizzleFirst(client: DrizzleClientLike, query: DrizzleSqlChunkLike): Promise<unknown> {
	// drizzle get() drops blob columns on some drivers (libsql); reuse the normalized all() path and take rows[0]
	return await executeDrizzleAll(client, query);
}

function buildDrizzleQuery(sqlTag: DrizzleSqlTagLike, sqlText: string, bindings: any[]): DrizzleSqlChunkLike {
	const segments = splitQuestionPlaceholders(sqlText);
	const placeholderCount = segments.length - 1;

	if (placeholderCount !== bindings.length) {
		throw new CollegeDBError(
			`Drizzle binding mismatch: expected ${placeholderCount} bindings, received ${bindings.length}`,
			'DRIZZLE_BINDINGS_MISMATCH'
		);
	}

	if (placeholderCount === 0) {
		return sqlTag.raw(sqlText);
	}

	const statement = typeof sqlTag.empty === 'function' ? sqlTag.empty() : sqlTag.raw('');

	for (let i = 0; i < segments.length; i++) {
		const segment = segments[i];
		if (segment) {
			statement.append(sqlTag.raw(segment));
		}

		if (i < placeholderCount) {
			statement.append(sqlTag`${bindings[i]}`);
		}
	}

	return statement;
}

function splitQuestionPlaceholders(sql: string): string[] {
	const segments: string[] = [];
	let segmentStart = 0;

	let inSingleQuote = false;
	let inDoubleQuote = false;
	let inLineComment = false;
	let inBlockComment = false;

	for (let i = 0; i < sql.length; i++) {
		const char = sql[i]!;
		const next = i + 1 < sql.length ? sql[i + 1] : '';

		if (inLineComment) {
			if (char === '\n') {
				inLineComment = false;
			}
			continue;
		}

		if (inBlockComment) {
			if (char === '*' && next === '/') {
				i++;
				inBlockComment = false;
			}
			continue;
		}

		if (!inSingleQuote && !inDoubleQuote) {
			if (char === '-' && next === '-') {
				i++;
				inLineComment = true;
				continue;
			}

			if (char === '/' && next === '*') {
				i++;
				inBlockComment = true;
				continue;
			}
		}

		if (char === "'" && !inDoubleQuote) {
			if (inSingleQuote && next === "'") {
				i++;
				continue;
			}
			inSingleQuote = !inSingleQuote;
			continue;
		}

		if (char === '"' && !inSingleQuote) {
			if (inDoubleQuote && next === '"') {
				i++;
				continue;
			}
			inDoubleQuote = !inDoubleQuote;
			continue;
		}

		if (char === '?' && !inSingleQuote && !inDoubleQuote) {
			segments.push(sql.slice(segmentStart, i));
			segmentStart = i + 1;
		}
	}

	segments.push(sql.slice(segmentStart));
	return segments;
}

// mysql2 resolves [rows, fields] for a read and [ResultSetHeader, undefined] for a write
function isMySQLTuple(result: unknown[]): boolean {
	if (result.length !== 2) {
		return false;
	}

	const [head, fields] = result;
	if (head === null || typeof head !== 'object') {
		return false;
	}

	if (Array.isArray(head)) {
		return Array.isArray(fields) && fields.every((field) => field !== null && typeof field === 'object' && !Array.isArray(field));
	}

	return fields === undefined || Array.isArray(fields);
}

// sqlite and postgres drivers never return a pair, and array rows of blobs would read as one
function readsMySQLTuple(dialect: SQLDialect | undefined): boolean {
	return dialect === undefined || dialect === 'mysql';
}

function extractRowsFromDrizzleExecute<T>(result: unknown, dialect?: SQLDialect): T[] {
	if (Array.isArray(result)) {
		if (readsMySQLTuple(dialect) && isMySQLTuple(result)) {
			return (Array.isArray(result[0]) ? result[0] : []) as T[];
		}

		return result as T[];
	}

	if (result && typeof result === 'object') {
		const objectValue = result as Record<string, unknown>;
		if (Array.isArray(objectValue.rows)) {
			return objectValue.rows as T[];
		}

		if (Array.isArray(objectValue.results)) {
			return objectValue.results as T[];
		}

		if (Array.isArray(objectValue.data)) {
			return objectValue.data as T[];
		}
	}

	return [];
}

function extractMetaFromDrizzleExecute(result: unknown, dialect?: SQLDialect): Record<string, unknown> {
	if (!result) {
		return {};
	}

	let objectResult: Record<string, unknown> | undefined;

	if (Array.isArray(result)) {
		if (readsMySQLTuple(dialect) && isMySQLTuple(result) && !Array.isArray(result[0])) {
			objectResult = result[0] as Record<string, unknown>;
		}
	} else if (typeof result === 'object') {
		objectResult = result as Record<string, unknown>;
	}

	if (!objectResult) {
		return {};
	}

	const meta: Record<string, unknown> = {};

	const changes = toMaybeNumber(objectResult.rowCount) ?? toMaybeNumber(objectResult.changes) ?? toMaybeNumber(objectResult.affectedRows);
	if (changes !== undefined) {
		meta.changes = changes;
	}

	const lastRowId =
		objectResult.lastInsertRowid ??
		objectResult.lastInsertId ??
		objectResult.lastID ??
		objectResult.lastRowID ??
		objectResult.insertId ??
		objectResult.insertID;
	if (typeof lastRowId === 'number' || typeof lastRowId === 'string') {
		meta.last_row_id = lastRowId;
	}

	if (objectResult.meta && typeof objectResult.meta === 'object') {
		Object.assign(meta, objectResult.meta as Record<string, unknown>);
	}

	return meta;
}

async function getNuxtHubKVValue<T = unknown>(client: NuxtHubKVLike, key: string): Promise<T | null | undefined> {
	if (typeof client.get === 'function') {
		return await client.get<T>(key);
	}

	if (typeof client.getItem === 'function') {
		return await client.getItem<T>(key);
	}

	throw new CollegeDBError('NuxtHub KV client must expose get() or getItem()', 'NUXTHUB_KV_CLIENT_INVALID');
}

async function setNuxtHubKVValue(client: NuxtHubKVLike, key: string, value: string): Promise<void> {
	if (typeof client.set === 'function') {
		await client.set(key, value);
		return;
	}

	if (typeof client.setItem === 'function') {
		await client.setItem(key, value);
		return;
	}

	throw new CollegeDBError('NuxtHub KV client must expose set() or setItem()', 'NUXTHUB_KV_CLIENT_INVALID');
}

async function deleteNuxtHubKVValue(client: NuxtHubKVLike, key: string): Promise<void> {
	if (typeof client.del === 'function') {
		await client.del(key);
		return;
	}

	if (typeof client.removeItem === 'function') {
		await client.removeItem(key);
		return;
	}

	throw new CollegeDBError('NuxtHub KV client must expose del() or removeItem()', 'NUXTHUB_KV_CLIENT_INVALID');
}

async function listNuxtHubKVKeys(client: NuxtHubKVLike, prefix: string): Promise<string[]> {
	let keys: readonly string[];

	if (typeof client.keys === 'function') {
		keys = await client.keys(prefix);
	} else if (typeof client.getKeys === 'function') {
		keys = await client.getKeys(prefix);
	} else {
		throw new CollegeDBError('NuxtHub KV client must expose keys() or getKeys()', 'NUXTHUB_KV_CLIENT_INVALID');
	}

	if (!Array.isArray(keys)) {
		return [];
	}

	if (!prefix) {
		return [...keys];
	}

	return keys.filter((key) => key.startsWith(prefix));
}

function extractRowsFromSQLiteExecute<T>(result: unknown): T[] {
	if (Array.isArray(result)) {
		return result as T[];
	}

	if (result && typeof result === 'object') {
		const objectResult = result as Record<string, unknown>;

		if (Array.isArray(objectResult.rows)) {
			return objectResult.rows as T[];
		}
		if (Array.isArray(objectResult.results)) {
			return objectResult.results as T[];
		}
	}

	return [];
}

/**
 * Reads the monotonic clock used for `meta.duration`.
 *
 * `Date.now()` only has millisecond resolution and is coarsened further inside
 * workerd, which reported every sub-millisecond statement as 0 or 1.
 * @private
 */
function nowMs(): number {
	return typeof performance !== 'undefined' && typeof performance.now === 'function' ? performance.now() : Date.now();
}

function createMeta(startedAt: number, extra: Record<string, unknown> = {}): QueryResultMeta {
	return {
		duration: nowMs() - startedAt,
		...extra
	};
}

function toMaybeNumber(value: unknown): number | undefined {
	if (typeof value === 'number' && Number.isFinite(value)) {
		return value;
	}
	return undefined;
}

const postgresPlaceholderCache = new Map<string, string>();

function rewriteQuestionPlaceholders(sql: string): string {
	const cached = postgresPlaceholderCache.get(sql);
	if (cached) {
		return cached;
	}

	let output = '';
	let placeholderIndex = 0;
	let inSingleQuote = false;
	let inDoubleQuote = false;
	let inLineComment = false;
	let inBlockComment = false;

	for (let i = 0; i < sql.length; i++) {
		const char = sql[i]!;
		const next = i + 1 < sql.length ? sql[i + 1] : '';

		if (inLineComment) {
			output += char;
			if (char === '\n') {
				inLineComment = false;
			}
			continue;
		}

		if (inBlockComment) {
			output += char;
			if (char === '*' && next === '/') {
				output += '/';
				i++;
				inBlockComment = false;
			}
			continue;
		}

		if (!inSingleQuote && !inDoubleQuote) {
			if (char === '-' && next === '-') {
				output += '--';
				i++;
				inLineComment = true;
				continue;
			}
			if (char === '/' && next === '*') {
				output += '/*';
				i++;
				inBlockComment = true;
				continue;
			}
		}

		if (char === "'" && !inDoubleQuote) {
			inSingleQuote = !inSingleQuote;
			output += char;
			continue;
		}

		if (char === '"' && !inSingleQuote) {
			inDoubleQuote = !inDoubleQuote;
			output += char;
			continue;
		}

		if (char === '?' && !inSingleQuote && !inDoubleQuote) {
			placeholderIndex++;
			output += `$${placeholderIndex}`;
			continue;
		}

		output += char;
	}

	postgresPlaceholderCache.set(sql, output);
	if (postgresPlaceholderCache.size > 5000) {
		const firstKey = postgresPlaceholderCache.keys().next().value;
		if (firstKey) {
			postgresPlaceholderCache.delete(firstKey);
		}
	}

	return output;
}
