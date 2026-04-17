/**
 * @fileoverview Provider adapters for non-Cloudflare backends.
 *
 * This module defines maintainable adapter factories that allow CollegeDB to run
 * on multiple storage backends while preserving Cloudflare compatibility.
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
 * @author CollegeDB Team
 * @since 1.1.0
 */

import { CollegeDBError } from './errors';
import type { KVListResult, KVStorage, PreparedStatement, QueryResult, QueryResultMeta, SQLDatabase } from './types';

const DEFAULT_REDIS_SCAN_COUNT = 500;

/**
 * Minimal Redis/Valkey client contract used by the KV adapter.
 */
export interface RedisLikeClient {
	get(key: string): Promise<string | null>;
	set(key: string, value: string): Promise<unknown>;
	del(key: string): Promise<unknown>;
	scan(cursor: string, ...args: any[]): Promise<RedisScanResult>;
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
	connect?: () => Promise<void>;
	end?: () => Promise<void>;
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
	execute?: (sql: string, bindings?: any[]) => Promise<[unknown, unknown]>;
	query?: (sql: string, bindings?: any[]) => Promise<[unknown, unknown]>;
}

/**
 * Optional lifecycle methods used by Hyperdrive helpers.
 */
export interface MySQLLifecycleClientLike extends MySQLClientLike {
	end?: () => Promise<void>;
	close?: () => Promise<void>;
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
	execute?: (sql: string, bindings?: any[]) => Promise<unknown>;
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
	execute?: (query: unknown) => Promise<unknown>;
	run?: (query: unknown) => Promise<unknown>;
	all?: (query: unknown) => Promise<unknown>;
	get?: (query: unknown) => Promise<unknown>;
}

/**
 * NuxtHub/Unstorage-like KV contract used by the adapter.
 */
export interface NuxtHubKVLike {
	get?<T = unknown>(key: string): Promise<T | null | undefined>;
	set?(key: string, value: unknown, options?: { ttl?: number }): Promise<unknown>;
	del?(key: string): Promise<unknown>;
	keys?(prefix?: string): Promise<string[]>;
	getItem?<T = unknown>(key: string): Promise<T | null | undefined>;
	setItem?(key: string, value: unknown): Promise<unknown>;
	removeItem?(key: string): Promise<unknown>;
	getKeys?(prefix?: string): Promise<string[]>;
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

	return {
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
			let cursor = options?.cursor ?? '0';
			const limit = options?.limit;
			const keys: string[] = [];

			do {
				const scanResult = await executeRedisScan(client, cursor, pattern, scanCount);
				cursor = scanResult.cursor;

				for (const key of scanResult.keys) {
					if (!prefix || key.startsWith(prefix)) {
						keys.push(key);
					}
					if (limit && keys.length >= limit) {
						break;
					}
				}

				if (limit && keys.length >= limit) {
					break;
				}
			} while (cursor !== '0');

			return {
				keys: keys.map((name) => ({ name })),
				cursor,
				list_complete: cursor === '0'
			};
		}
	};
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
 * @param sqlTag - Optional Drizzle `sql` helper for Drizzle interop
 * @returns SQLDatabase-compatible adapter
 */
export function createPostgreSQLProvider(client: PostgresClientLike): SQLDatabase;
export function createPostgreSQLProvider(client: DrizzleClientLike, sqlTag: DrizzleSqlTagLike): SQLDatabase;
export function createPostgreSQLProvider(client: PostgresClientLike | DrizzleClientLike, sqlTag?: DrizzleSqlTagLike): SQLDatabase {
	if (sqlTag) {
		return createDrizzleSQLProvider(client as DrizzleClientLike, sqlTag);
	}

	return {
		prepare(sql: string): PreparedStatement {
			return new PostgresPreparedStatement(client as PostgresClientLike, sql);
		}
	};
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
 * @param sqlTag - Optional Drizzle `sql` helper for Drizzle interop
 * @returns SQLDatabase-compatible adapter
 */
export function createMySQLProvider(client: MySQLClientLike): SQLDatabase;
export function createMySQLProvider(client: DrizzleClientLike, sqlTag: DrizzleSqlTagLike): SQLDatabase;
export function createMySQLProvider(client: MySQLClientLike | DrizzleClientLike, sqlTag?: DrizzleSqlTagLike): SQLDatabase {
	if (sqlTag) {
		return createDrizzleSQLProvider(client as DrizzleClientLike, sqlTag);
	}

	return {
		prepare(sql: string): PreparedStatement {
			return new MySQLPreparedStatement(client as MySQLClientLike, sql);
		}
	};
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
 * @param sqlTag - Optional Drizzle `sql` helper for Drizzle interop
 * @returns SQLDatabase-compatible adapter
 */
export function createSQLiteProvider(client: SQLiteClientLike): SQLDatabase;
export function createSQLiteProvider(client: DrizzleClientLike, sqlTag: DrizzleSqlTagLike): SQLDatabase;
export function createSQLiteProvider(client: SQLiteClientLike | DrizzleClientLike, sqlTag?: DrizzleSqlTagLike): SQLDatabase {
	if (sqlTag) {
		return createDrizzleSQLProvider(client as DrizzleClientLike, sqlTag);
	}

	return {
		prepare(sql: string): PreparedStatement {
			return new SQLitePreparedStatement(client as SQLiteClientLike, sql);
		}
	};
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
export function createDrizzleSQLProvider(client: DrizzleClientLike, sqlTag: DrizzleSqlTagLike): SQLDatabase {
	return {
		prepare(sql: string): PreparedStatement {
			return new DrizzlePreparedStatement(client, sqlTag, sql);
		}
	};
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
			const limitedKeys = typeof options?.limit === 'number' ? allKeys.slice(0, options.limit) : allKeys;

			return {
				keys: limitedKeys.map((name) => ({ name })),
				list_complete: true
			};
		}
	};
}

/**
 * Creates a PostgreSQL adapter wired to a Hyperdrive binding.
 *
 * The returned provider creates a transient client for each statement execution.
 * Hyperdrive handles connection pooling at the edge, so this pattern remains fast
 * and scalable in Workers.
 *
 * @param hyperdrive - Hyperdrive binding
 * @param clientFactory - Client factory (e.g. `connectionString => new Client({ connectionString })`)
 * @returns SQLDatabase-compatible adapter
 */
export function createHyperdrivePostgresProvider(
	hyperdrive: HyperdriveBindingLike,
	clientFactory: HyperdrivePostgresClientFactory
): SQLDatabase {
	const delegatedClient: PostgresClientLike = {
		query: async <T = Record<string, unknown>>(sql: string, bindings: any[] = []) => {
			const client = clientFactory(hyperdrive.connectionString);
			if (typeof client.connect === 'function') {
				await client.connect();
			}

			try {
				return await client.query<T>(sql, bindings);
			} finally {
				if (typeof client.release === 'function') {
					client.release();
				} else if (typeof client.end === 'function') {
					await client.end();
				}
			}
		}
	};

	return createPostgreSQLProvider(delegatedClient);
}

/**
 * Creates a MySQL/MariaDB adapter wired to a Hyperdrive binding.
 *
 * The returned provider creates a transient client for each statement execution.
 * Hyperdrive handles connection pooling under the hood.
 *
 * @param hyperdrive - Hyperdrive binding
 * @param clientFactory - Client factory (e.g. `connectionString => mysql.createConnection(connectionString)`)
 * @returns SQLDatabase-compatible adapter
 */
export function createHyperdriveMySQLProvider(hyperdrive: HyperdriveBindingLike, clientFactory: HyperdriveMySQLClientFactory): SQLDatabase {
	const delegatedClient: MySQLClientLike = {
		execute: async (sql: string, bindings: any[] = []) => {
			const client = clientFactory(hyperdrive.connectionString);
			try {
				if (typeof client.execute === 'function') {
					return await client.execute(sql, bindings);
				}
				if (typeof client.query === 'function') {
					return await client.query(sql, bindings);
				}

				throw new CollegeDBError('Hyperdrive MySQL client is missing execute/query methods', 'MYSQL_CLIENT_INVALID');
			} finally {
				if (typeof client.end === 'function') {
					await client.end();
				} else if (typeof client.close === 'function') {
					await client.close();
				} else if (typeof client.destroy === 'function') {
					client.destroy();
				}
			}
		}
	};

	return createMySQLProvider(delegatedClient);
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

class PostgresPreparedStatement implements PreparedStatement {
	private readonly client: PostgresClientLike;
	private readonly sql: string;
	private readonly bindings: any[];

	constructor(client: PostgresClientLike, sql: string, bindings: any[] = []) {
		this.client = client;
		this.sql = sql;
		this.bindings = bindings;
	}

	bind(...bindings: any[]): PreparedStatement {
		return new PostgresPreparedStatement(this.client, this.sql, bindings);
	}

	async run<T = Record<string, unknown>>(): Promise<QueryResult<T>> {
		const startedAt = Date.now();
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
		const startedAt = Date.now();
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
		const sql = rewriteQuestionPlaceholders(this.sql);
		const result = await this.client.query<T>(sql, this.bindings);
		return result.rows?.[0] ?? null;
	}
}

class MySQLPreparedStatement implements PreparedStatement {
	private readonly client: MySQLClientLike;
	private readonly sql: string;
	private readonly bindings: any[];

	constructor(client: MySQLClientLike, sql: string, bindings: any[] = []) {
		this.client = client;
		this.sql = sql;
		this.bindings = bindings;
	}

	bind(...bindings: any[]): PreparedStatement {
		return new MySQLPreparedStatement(this.client, this.sql, bindings);
	}

	async run<T = Record<string, unknown>>(): Promise<QueryResult<T>> {
		const startedAt = Date.now();
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
		const startedAt = Date.now();
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

	constructor(client: SQLiteClientLike, sql: string, bindings: any[] = []) {
		this.client = client;
		this.sql = sql;
		this.bindings = bindings;
	}

	bind(...bindings: any[]): PreparedStatement {
		return new SQLitePreparedStatement(this.client, this.sql, bindings);
	}

	async run<T = Record<string, unknown>>(): Promise<QueryResult<T>> {
		const startedAt = Date.now();

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
		const startedAt = Date.now();

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

	constructor(client: DrizzleClientLike, sqlTag: DrizzleSqlTagLike, sqlText: string, bindings: any[] = []) {
		this.client = client;
		this.sqlTag = sqlTag;
		this.sqlText = sqlText;
		this.bindings = bindings;
	}

	bind(...bindings: any[]): PreparedStatement {
		return new DrizzlePreparedStatement(this.client, this.sqlTag, this.sqlText, bindings);
	}

	async run<T = Record<string, unknown>>(): Promise<QueryResult<T>> {
		const startedAt = Date.now();
		const query = buildDrizzleQuery(this.sqlTag, this.sqlText, this.bindings);
		const result = await executeDrizzleRun(this.client, query);

		return {
			success: true,
			results: extractRowsFromDrizzleExecute<T>(result),
			meta: createMeta(startedAt, extractMetaFromDrizzleExecute(result))
		};
	}

	async all<T = Record<string, unknown>>(): Promise<QueryResult<T>> {
		const startedAt = Date.now();
		const query = buildDrizzleQuery(this.sqlTag, this.sqlText, this.bindings);
		const result = await executeDrizzleAll(this.client, query);

		return {
			success: true,
			results: extractRowsFromDrizzleExecute<T>(result),
			meta: createMeta(startedAt, extractMetaFromDrizzleExecute(result))
		};
	}

	async first<T = Record<string, unknown>>(): Promise<T | null> {
		const query = buildDrizzleQuery(this.sqlTag, this.sqlText, this.bindings);
		const result = await executeDrizzleFirst(this.client, query);
		const rows = extractRowsFromDrizzleExecute<T>(result);
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
		const [rows] = await client.execute(sql, bindings);
		return rows;
	}

	if (typeof client.query === 'function') {
		const [rows] = await client.query(sql, bindings);
		return rows;
	}

	throw new CollegeDBError('MySQL client must expose execute() or query()', 'MYSQL_CLIENT_INVALID');
}

async function executeDrizzleRun(client: DrizzleClientLike, query: DrizzleSqlChunkLike): Promise<unknown> {
	if (typeof client.run === 'function') {
		return await client.run(query);
	}

	if (typeof client.execute === 'function') {
		return await client.execute(query);
	}

	if (typeof client.all === 'function') {
		return await client.all(query);
	}

	throw new CollegeDBError('Drizzle client must expose run(), execute(), or all()', 'DRIZZLE_CLIENT_INVALID');
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
	if (typeof client.get === 'function') {
		return await client.get(query);
	}

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

function extractRowsFromDrizzleExecute<T>(result: unknown): T[] {
	if (Array.isArray(result)) {
		if (result.length === 2 && Array.isArray(result[0])) {
			return result[0] as T[];
		}

		if (
			result.length === 2 &&
			result[0] &&
			typeof result[0] === 'object' &&
			!Array.isArray(result[0]) &&
			(Array.isArray(result[1]) || result[1] === null || result[1] === undefined)
		) {
			return [];
		}

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

		if (Array.isArray(objectResult.data)) {
			return objectResult.data as T[];
		}
	}

	return [];
}

function extractMetaFromDrizzleExecute(result: unknown): Record<string, unknown> {
	if (!result) {
		return {};
	}

	let objectResult: Record<string, unknown> | undefined;

	if (Array.isArray(result) && result.length === 2 && result[0] && typeof result[0] === 'object' && !Array.isArray(result[0])) {
		objectResult = result[0] as Record<string, unknown>;
	} else if (typeof result === 'object' && !Array.isArray(result)) {
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

	const lastRowId = objectResult.lastInsertRowid ?? objectResult.lastInsertId ?? objectResult.insertId;
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
	let keys: string[];

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
		return keys;
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

function createMeta(startedAt: number, extra: Record<string, unknown> = {}): QueryResultMeta {
	return {
		duration: Date.now() - startedAt,
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
