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
 *
 * Supported SQL backends:
 * - Cloudflare D1 (native shape)
 * - PostgreSQL compatible clients
 * - MySQL / MariaDB compatible clients
 * - SQLite clients
 * - Hyperdrive-backed PostgreSQL / MySQL clients
 *
 * @author CollegeDB Team
 * @since 1.1.0
 */

import { CollegeDBError } from './errors.js';
import type { KVListResult, KVStorage, PreparedStatement, QueryResult, QueryResultMeta, SQLDatabase } from './types.js';

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
 * @param client - PostgreSQL client/pool
 * @returns SQLDatabase-compatible adapter
 */
export function createPostgreSQLProvider(client: PostgresClientLike): SQLDatabase {
	return {
		prepare(sql: string): PreparedStatement {
			return new PostgresPreparedStatement(client, sql);
		}
	};
}

/**
 * Creates a MySQL/MariaDB adapter implementing CollegeDB's SQL contract.
 *
 * Supports mysql2/promise clients, pools, and compatible wrappers.
 *
 * @param client - MySQL/MariaDB client or pool
 * @returns SQLDatabase-compatible adapter
 */
export function createMySQLProvider(client: MySQLClientLike): SQLDatabase {
	return {
		prepare(sql: string): PreparedStatement {
			return new MySQLPreparedStatement(client, sql);
		}
	};
}

/**
 * Creates a SQLite adapter implementing CollegeDB's SQL contract.
 *
 * Supports both execute-style clients and prepare/run/get/all statement clients.
 *
 * @param client - SQLite client
 * @returns SQLDatabase-compatible adapter
 */
export function createSQLiteProvider(client: SQLiteClientLike): SQLDatabase {
	return {
		prepare(sql: string): PreparedStatement {
			return new SQLitePreparedStatement(client, sql);
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
