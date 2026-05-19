/**
 * @fileoverview In-memory mock providers for testing and development.
 *
 * This module provides lightweight, zero-dependency in-memory implementations of
 * CollegeDB's KVStorage and SQLDatabase interfaces, perfect for:
 *
 * - Unit testing without external dependencies
 * - Integration testing with multiple shard combinations
 * - Local development and rapid iteration
 * - Sandboxed playtesting of routing logic
 *
 * Both providers are suitable for Cloudflare Workers, Node.js, and Deno environments.
 *
 * @author CollegeDB Team
 * @since 1.2.0
 */

import { CollegeDBError } from './errors';
import type { KVListResult, KVStorage, PreparedStatement, QueryResult, SQLDatabase } from './types';

/**
 * Column metadata extracted from CREATE TABLE statements.
 */
interface ColumnInfo {
	name: string;
	type: string;
	isPrimaryKey: boolean;
	isAutoIncrement: boolean;
}

type InMemoryDrizzleQuery = {
	toQuery?: (config: Record<string, unknown>) => { sql?: string; params?: unknown[] };
	sql?: string;
	params?: unknown[];
	text?: string;
};

const IN_MEMORY_DRIZZLE_QUERY_CONFIG = {
	casing: {
		getColumnCasing(column: { name?: string }): string {
			return column?.name ?? '';
		}
	},
	escapeName(name: string): string {
		return name;
	},
	escapeParam(): string {
		return '?';
	},
	escapeString(str: string): string {
		return str.replace(/'/g, "''");
	}
} as const;

function normalizeSqlIdentifier(identifier: string): string {
	return (
		identifier
			.trim()
			.replace(/^[`"\[]+|[`"\]]+$/g, '')
			.split('.')
			.pop()
			?.trim() ?? identifier.trim()
	);
}

function escapeRegExp(value: string): string {
	return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Simple in-memory SQL query result parser for common patterns.
 * Not a full SQL parser - handles the most common CRUD operations.
 */
class SimpleQueryParser {
	/**
	 * Parse a SQL string into tokens for basic analysis.
	 */
	static tokenize(sql: string): string[] {
		return sql
			.trim()
			.split(/\s+/)
			.map((t) => t.toLowerCase());
	}

	/**
	 * Extract table name from SQL statement.
	 */
	static extractTableName(sql: string): string | null {
		const tokens = this.tokenize(sql);

		// Handle CREATE TABLE [IF NOT EXISTS] table_name
		if (tokens[0] === 'create' && tokens[1] === 'table') {
			let idx = 2;
			if (tokens[idx] === 'if') {
				idx += 3; // Skip 'if not exists'
			}
			return tokens[idx] ?? null;
		}

		// Handle DROP TABLE [IF EXISTS] table_name
		if (tokens[0] === 'drop' && tokens[1] === 'table') {
			let idx = 2;
			if (tokens[idx] === 'if') {
				idx += 2; // Skip 'if exists'
			}
			return tokens[idx] ?? null;
		}

		// Handle SELECT, INSERT, UPDATE, DELETE
		const fromIdx = tokens.indexOf('from');
		const intoIdx = tokens.indexOf('into');
		const updateIdx = tokens.indexOf('update');

		if (fromIdx >= 0 && fromIdx + 1 < tokens.length) {
			return normalizeSqlIdentifier(tokens[fromIdx + 1] ?? '');
		}
		if (intoIdx >= 0 && intoIdx + 1 < tokens.length) {
			return normalizeSqlIdentifier(tokens[intoIdx + 1] ?? '');
		}
		if (updateIdx >= 0 && updateIdx + 1 < tokens.length) {
			return normalizeSqlIdentifier(tokens[updateIdx + 1] ?? '');
		}
		return null;
	}

	static extractInsertColumns(sql: string): string[] | null {
		const match = sql.match(/insert\s+into\s+[^()]+\(([^)]+)\)/i);
		if (!match || !match[1]) {
			return null;
		}

		return match[1]
			.split(',')
			.map((column) => normalizeSqlIdentifier(column))
			.filter((column) => column.length > 0);
	}

	static extractWhereClause(sql: string): { column: string; operator: '=' | 'like' } | null {
		const match = sql.match(/where\s+([`"\w.]+)\s*(=|like)\s*\?/i);
		if (!match || !match[1] || !match[2]) {
			return null;
		}

		const operator = match[2].toLowerCase() === 'like' ? 'like' : '=';
		return {
			column: normalizeSqlIdentifier(match[1]),
			operator
		};
	}

	static extractOrderByClause(sql: string): { column: string; direction: 'asc' | 'desc' } | null {
		const match = sql.match(/order\s+by\s+([`"\w.]+)(?:\s+(asc|desc))?/i);
		if (!match || !match[1]) {
			return null;
		}

		return {
			column: normalizeSqlIdentifier(match[1]),
			direction: match[2]?.toLowerCase() === 'desc' ? 'desc' : 'asc'
		};
	}

	static matchesWhereClause(record: Record<string, any>, clause: { column: string; operator: '=' | 'like' }, value: unknown): boolean {
		const fieldValue = record[clause.column];

		if (clause.operator === 'like') {
			const pattern = String(value ?? '');
			const regex = new RegExp(`^${escapeRegExp(pattern).replace(/%/g, '.*').replace(/_/g, '.')}$`, 'i');
			return regex.test(String(fieldValue ?? ''));
		}

		return String(fieldValue ?? '') === String(value ?? '');
	}

	/**
	 * Check if SQL is a SELECT statement.
	 */
	static isSelect(sql: string): boolean {
		return this.tokenize(sql)[0] === 'select';
	}

	/**
	 * Check if SQL is an INSERT statement.
	 */
	static isInsert(sql: string): boolean {
		return this.tokenize(sql)[0] === 'insert';
	}

	/**
	 * Check if SQL is a CREATE TABLE statement.
	 */
	static isCreateTable(sql: string): boolean {
		const tokens = this.tokenize(sql);
		return tokens[0] === 'create' && tokens[1] === 'table';
	}

	/**
	 * Check if SQL is a DROP TABLE statement.
	 */
	static isDropTable(sql: string): boolean {
		const tokens = this.tokenize(sql);
		return tokens[0] === 'drop' && tokens[1] === 'table';
	}

	/**
	 * Check if SQL is a DELETE statement.
	 */
	static isDelete(sql: string): boolean {
		return this.tokenize(sql)[0] === 'delete';
	}

	/**
	 * Check if SQL is an UPDATE statement.
	 */
	static isUpdate(sql: string): boolean {
		return this.tokenize(sql)[0] === 'update';
	}

	/**
	 * Check if SQL has a WHERE clause with "id = ?".
	 */
	static hasIdWhereClause(sql: string): boolean {
		return sql.includes('WHERE') && sql.includes('id');
	}

	/**
	 * Check for AUTOINCREMENT or AUTO_INCREMENT pattern.
	 */
	static hasAutoIncrement(sql: string): boolean {
		return sql.includes('AUTOINCREMENT') || sql.includes('AUTO_INCREMENT') || sql.includes('GENERATED BY DEFAULT AS IDENTITY');
	}

	/**
	 * Check for RETURNING clause.
	 */
	static hasReturning(sql: string): boolean {
		return sql.includes('RETURNING');
	}
}

function normalizeInMemoryDrizzleQuery(query: string | InMemoryDrizzleQuery, bindings: any[] = []): { sql: string; bindings: any[] } {
	if (typeof query === 'string') {
		return { sql: query, bindings };
	}

	if (!query || typeof query !== 'object') {
		throw new CollegeDBError('Unsupported query input', 'INVALID_QUERY_INPUT');
	}

	if (typeof query.toQuery === 'function') {
		const built = query.toQuery(IN_MEMORY_DRIZZLE_QUERY_CONFIG as Record<string, unknown>);
		if (built && typeof built.sql === 'string') {
			return {
				sql: built.sql,
				bindings: Array.isArray(built.params) ? [...built.params] : bindings
			};
		}
	}

	if (typeof query.sql === 'string') {
		return {
			sql: query.sql,
			bindings: Array.isArray(query.params) ? [...query.params] : bindings
		};
	}

	if (typeof query.text === 'string') {
		return {
			sql: query.text,
			bindings: Array.isArray(query.params) ? [...query.params] : bindings
		};
	}

	throw new CollegeDBError('Unsupported Drizzle-style query object', 'INVALID_QUERY_INPUT');
}

/**
 * In-memory implementation of SQLDatabase for testing.
 *
 * Supports basic CRUD operations without external dependencies.
 * Suitable for unit tests, integration tests, and development.
 */
export class InMemorySQLDatabase implements SQLDatabase {
	private tables = new Map<string, Map<string, Record<string, any>>>();
	private schemas = new Map<string, Map<string, ColumnInfo>>();
	private autoIncrementCounters = new Map<string, number>();
	private lastInsertRowId: number | string | null = null;

	prepare(sql: string): PreparedStatement {
		return new InMemoryPreparedStatement(this, sql);
	}

	async execute(query: string | InMemoryDrizzleQuery, bindings: any[] = []): Promise<QueryResult<Record<string, any>>> {
		return await this.executeClientQuery(query, bindings);
	}

	async run(query: string | InMemoryDrizzleQuery, bindings: any[] = []): Promise<QueryResult<Record<string, any>>> {
		return await this.executeClientQuery(query, bindings);
	}

	async all(query: string | InMemoryDrizzleQuery, bindings: any[] = []): Promise<QueryResult<Record<string, any>>> {
		return await this.executeClientQuery(query, bindings);
	}

	async get(query: string | InMemoryDrizzleQuery, bindings: any[] = []): Promise<QueryResult<Record<string, any>>> {
		return await this.executeClientQuery(query, bindings);
	}

	private async executeClientQuery(query: string | InMemoryDrizzleQuery, bindings: any[]): Promise<QueryResult<Record<string, any>>> {
		const normalized = normalizeInMemoryDrizzleQuery(query, bindings);
		return await this.executeStatement(normalized.sql, normalized.bindings);
	}

	/**
	 * Internal method to execute a statement.
	 */
	async executeStatement(sql: string, bindings: any[]): Promise<QueryResult<Record<string, any>>> {
		const startTime = Date.now();

		try {
			if (SimpleQueryParser.isCreateTable(sql)) {
				return await this.handleCreateTable(sql, bindings);
			}

			if (SimpleQueryParser.isDropTable(sql)) {
				return await this.handleDropTable(sql, bindings);
			}

			if (SimpleQueryParser.isInsert(sql)) {
				return await this.handleInsert(sql, bindings);
			}

			if (SimpleQueryParser.isUpdate(sql)) {
				return await this.handleUpdate(sql, bindings);
			}

			if (SimpleQueryParser.isDelete(sql)) {
				return await this.handleDelete(sql, bindings);
			}

			if (SimpleQueryParser.isSelect(sql)) {
				return await this.handleSelect(sql, bindings);
			}

			// For other statements (PRAGMA, etc), return success
			return {
				success: true,
				results: [],
				meta: { duration: Date.now() - startTime }
			};
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error);
			return {
				success: false,
				results: [],
				meta: { duration: Date.now() - startTime },
				error: message
			};
		}
	}

	/**
	 * Internal method to execute a query.
	 */
	async executeQuery(sql: string, bindings: any[]): Promise<QueryResult<Record<string, any>>> {
		const startTime = Date.now();

		try {
			if (SimpleQueryParser.isSelect(sql)) {
				return await this.handleSelect(sql, bindings);
			}

			// Non-SELECT queries should use executeStatement
			return {
				success: false,
				results: [],
				meta: { duration: Date.now() - startTime },
				error: 'Use executeStatement for non-SELECT queries'
			};
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error);
			return {
				success: false,
				results: [],
				meta: { duration: Date.now() - startTime },
				error: message
			};
		}
	}

	private async handleCreateTable(sql: string, bindings: any[]): Promise<QueryResult> {
		const startTime = Date.now();
		const tableName = SimpleQueryParser.extractTableName(sql);

		if (!tableName) {
			return {
				success: false,
				results: [],
				meta: { duration: Date.now() - startTime },
				error: 'Could not extract table name'
			};
		}

		// Parse schema from CREATE TABLE statement
		const schema = new Map<string, any>();

		// Extract the content between parentheses
		const parenMatch = sql.match(/\((.*)\)/s);
		if (!parenMatch || !parenMatch[1]) {
			this.tables.set(tableName, new Map());
			this.schemas.set(tableName, schema);
			return {
				success: true,
				results: [],
				meta: { duration: Date.now() - startTime }
			};
		}

		const tableDefinition = parenMatch[1];
		const columnPattern = /(\w+)\s+([A-Z_]+)(\s+[^,)]*)?/gi;
		let match;

		while ((match = columnPattern.exec(tableDefinition)) !== null) {
			const colName = match[1] || '';
			const colType = match[2] || '';
			const modifiers = match[3] || '';

			schema.set(colName, {
				name: colName,
				type: colType,
				isPrimaryKey: modifiers.includes('PRIMARY KEY'),
				isAutoIncrement:
					modifiers.includes('AUTOINCREMENT') ||
					modifiers.includes('AUTO_INCREMENT') ||
					modifiers.includes('GENERATED BY DEFAULT AS IDENTITY')
			});
		}

		this.tables.set(tableName, new Map());
		this.schemas.set(tableName, schema);

		if (SimpleQueryParser.hasAutoIncrement(sql)) {
			this.autoIncrementCounters.set(tableName, 0);
		}

		return {
			success: true,
			results: [],
			meta: { duration: Date.now() - startTime }
		};
	}

	private async handleDropTable(sql: string, bindings: any[]): Promise<QueryResult> {
		const startTime = Date.now();
		const tableName = SimpleQueryParser.extractTableName(sql);

		if (!tableName) {
			return {
				success: false,
				results: [],
				meta: { duration: Date.now() - startTime },
				error: 'Could not extract table name'
			};
		}

		this.tables.delete(tableName);
		this.schemas.delete(tableName);
		this.autoIncrementCounters.delete(tableName);

		return {
			success: true,
			results: [],
			meta: { duration: Date.now() - startTime }
		};
	}

	private async handleInsert(sql: string, bindings: any[]): Promise<QueryResult> {
		const startTime = Date.now();
		const tableName = SimpleQueryParser.extractTableName(sql);

		if (!tableName) {
			return {
				success: false,
				results: [],
				meta: { duration: Date.now() - startTime },
				error: 'Could not extract table name'
			};
		}

		if (!this.tables.has(tableName)) {
			this.tables.set(tableName, new Map());
		}

		const table = this.tables.get(tableName)!;
		const schema = this.schemas.get(tableName);
		const schemaEntries = schema ? Array.from(schema.values()) : [];
		const primaryKeyInfo = schemaEntries.find((column) => column.isPrimaryKey);
		const autoIncrementInfo = schemaEntries.find((column) => column.isAutoIncrement);
		const insertColumns = SimpleQueryParser.extractInsertColumns(sql);
		const record: Record<string, any> = {};

		// Build record from the explicit INSERT column order when available.
		// Otherwise fall back to schema order for simple legacy statements.
		if (insertColumns && insertColumns.length > 0) {
			insertColumns.forEach((columnName, index) => {
				if (index < bindings.length) {
					record[columnName] = bindings[index];
				}
			});
		} else if (schema) {
			let bindingIdx = 0;
			for (const [colName] of schema) {
				if (bindingIdx < bindings.length) {
					record[colName] = bindings[bindingIdx];
					bindingIdx++;
				}
			}
		} else {
			for (let i = 0; i < bindings.length; i++) {
				if (i === 0) {
					record.id = bindings[i];
				} else {
					record[`column_${i}`] = bindings[i];
				}
			}
		}

		const primaryKeyColumn = primaryKeyInfo?.name ?? 'id';
		let primaryKeyValue = record[primaryKeyColumn];

		if ((primaryKeyValue === undefined || primaryKeyValue === null || primaryKeyValue === '') && autoIncrementInfo) {
			const nextValue = (this.autoIncrementCounters.get(tableName) ?? 0) + 1;
			this.autoIncrementCounters.set(tableName, nextValue);
			record[autoIncrementInfo.name] = nextValue;
			if (primaryKeyColumn !== autoIncrementInfo.name) {
				record[primaryKeyColumn] = nextValue;
			}
			primaryKeyValue = nextValue;
		}

		if (primaryKeyValue === undefined || primaryKeyValue === null || primaryKeyValue === '') {
			return {
				success: false,
				results: [],
				meta: { duration: Date.now() - startTime },
				error: 'No primary key value provided'
			};
		}

		if (autoIncrementInfo) {
			const numericPrimaryKey = typeof primaryKeyValue === 'number' ? primaryKeyValue : Number(primaryKeyValue);
			if (Number.isFinite(numericPrimaryKey)) {
				this.autoIncrementCounters.set(tableName, Math.max(this.autoIncrementCounters.get(tableName) ?? 0, numericPrimaryKey));
			}
		}

		table.set(String(primaryKeyValue), record);
		this.lastInsertRowId = primaryKeyValue;

		const hasReturning = SimpleQueryParser.hasReturning(sql);
		const results = hasReturning ? [record] : [];

		return {
			success: true,
			results,
			meta: {
				duration: Date.now() - startTime,
				last_row_id: primaryKeyValue,
				changes: 1
			}
		};
	}

	private async handleUpdate(sql: string, bindings: any[]): Promise<QueryResult> {
		const startTime = Date.now();
		const tableName = SimpleQueryParser.extractTableName(sql);

		if (!tableName) {
			return {
				success: false,
				results: [],
				meta: { duration: Date.now() - startTime },
				error: 'Could not extract table name'
			};
		}

		if (!this.tables.has(tableName)) {
			return {
				success: false,
				results: [],
				meta: { duration: Date.now() - startTime },
				error: `Table ${tableName} not found`
			};
		}

		const table = this.tables.get(tableName)!;
		const whereClause = SimpleQueryParser.extractWhereClause(sql);
		if (!whereClause || bindings.length === 0) {
			return {
				success: true,
				results: [],
				meta: { duration: Date.now() - startTime, changes: 0 }
			};
		}

		const whereValue = bindings[bindings.length - 1];
		const records = Array.from(table.entries()).filter(([, record]) =>
			SimpleQueryParser.matchesWhereClause(record, whereClause, whereValue)
		);

		if (records.length === 0) {
			return {
				success: true,
				results: [],
				meta: { duration: Date.now() - startTime, changes: 0 }
			};
		}

		// Apply updates to each matched record.
		// Assume bindings before the last one are the updated values in SET order.
		for (const [, record] of records) {
			for (let i = 0; i < bindings.length - 1; i++) {
				const colName = this.extractSetColumnName(sql, i);
				if (colName) {
					record[colName] = bindings[i];
				}
			}
		}

		return {
			success: true,
			results: [],
			meta: { duration: Date.now() - startTime, changes: records.length }
		};
	}

	private extractSetColumnName(sql: string, setIndex: number): string | null {
		// Extract column names from SET clause
		const setMatch = sql.match(/SET\s+([^W]+?)(?:WHERE|$)/i);
		if (!setMatch || !setMatch[1]) return null;

		const setPart = setMatch[1];
		const columns = setPart.split(',').map((part) => {
			const match = part.match(/(\w+)\s*=/);
			return match && match[1] ? match[1] : null;
		});

		return columns[setIndex] ?? null;
	}

	private async handleDelete(sql: string, bindings: any[]): Promise<QueryResult> {
		const startTime = Date.now();
		const tableName = SimpleQueryParser.extractTableName(sql);

		if (!tableName) {
			return {
				success: false,
				results: [],
				meta: { duration: Date.now() - startTime },
				error: 'Could not extract table name'
			};
		}

		if (!this.tables.has(tableName)) {
			return {
				success: true,
				results: [],
				meta: { duration: Date.now() - startTime, changes: 0 }
			};
		}

		const table = this.tables.get(tableName)!;

		const whereClause = SimpleQueryParser.extractWhereClause(sql);
		if (!whereClause) {
			// DELETE without a WHERE clause - delete all
			const size = table.size;
			table.clear();
			return {
				success: true,
				results: [],
				meta: { duration: Date.now() - startTime, changes: size }
			};
		}

		const whereValue = bindings[0];
		const matchingKeys = Array.from(table.entries())
			.filter(([, record]) => SimpleQueryParser.matchesWhereClause(record, whereClause, whereValue))
			.map(([key]) => key);

		for (const key of matchingKeys) {
			table.delete(key);
		}

		return {
			success: true,
			results: [],
			meta: { duration: Date.now() - startTime, changes: matchingKeys.length }
		};
	}

	private async handleSelect(sql: string, bindings: any[]): Promise<QueryResult> {
		const startTime = Date.now();
		const tableName = SimpleQueryParser.extractTableName(sql);

		if (!tableName) {
			return {
				success: false,
				results: [],
				meta: { duration: Date.now() - startTime },
				error: 'Could not extract table name'
			};
		}

		if (!this.tables.has(tableName)) {
			return {
				success: true,
				results: [],
				meta: { duration: Date.now() - startTime }
			};
		}

		const table = this.tables.get(tableName)!;
		let results = Array.from(table.values());

		const whereClause = SimpleQueryParser.extractWhereClause(sql);
		if (whereClause && bindings.length > 0) {
			results = results.filter((record) => SimpleQueryParser.matchesWhereClause(record, whereClause, bindings[0]));
		}

		const orderByClause = SimpleQueryParser.extractOrderByClause(sql);
		if (orderByClause) {
			results = [...results].sort((left, right) => {
				const leftValue = left[orderByClause.column];
				const rightValue = right[orderByClause.column];

				if (leftValue === rightValue) {
					return 0;
				}

				if (leftValue === undefined || leftValue === null) {
					return orderByClause.direction === 'desc' ? 1 : -1;
				}

				if (rightValue === undefined || rightValue === null) {
					return orderByClause.direction === 'desc' ? -1 : 1;
				}

				const comparison = String(leftValue).localeCompare(String(rightValue));
				return orderByClause.direction === 'desc' ? -comparison : comparison;
			});
		}

		// Handle COUNT queries after filters are applied.
		if (sql.includes('COUNT(*)')) {
			const aliasMatch = sql.match(/COUNT\(\*\)\s+as\s+(\w+)/i);
			const countKey = aliasMatch?.[1] ?? 'COUNT(*)';
			const result: Record<string, number> = {};
			result[countKey] = results.length;
			return {
				success: true,
				results: [result],
				meta: { duration: Date.now() - startTime }
			};
		}

		return {
			success: true,
			results,
			meta: { duration: Date.now() - startTime }
		};
	}
}

/**
 * Prepared statement wrapper for in-memory SQL database.
 */
class InMemoryPreparedStatement implements PreparedStatement {
	private bindings: any[] = [];

	constructor(
		private database: InMemorySQLDatabase,
		private sql: string
	) {}

	bind(...bindings: any[]): PreparedStatement {
		this.bindings = bindings;
		return this;
	}

	async run<T = Record<string, unknown>>(): Promise<QueryResult<T>> {
		return (await this.database.executeStatement(this.sql, this.bindings)) as QueryResult<T>;
	}

	async all<T = Record<string, unknown>>(): Promise<QueryResult<T>> {
		return (await this.database.executeQuery(this.sql, this.bindings)) as QueryResult<T>;
	}

	async first<T = Record<string, unknown>>(): Promise<T | null> {
		const result = await this.database.executeQuery(this.sql, this.bindings);
		return (result.results[0] as T) || null;
	}
}

/**
 * In-memory implementation of KVStorage for testing.
 *
 * Simple key-value store with support for prefix listing and cursor-based pagination.
 * Suitable for unit tests, integration tests, and development.
 */
export class InMemoryKVStorage implements KVStorage {
	private store = new Map<string, string>();
	private expirations = new Map<string, number>();

	private normalizeValue(value: unknown): string {
		return typeof value === 'string' ? value : (JSON.stringify(value) ?? String(value));
	}

	async get<T = unknown>(key: string, type: 'json'): Promise<T | null>;
	async get(key: string, type?: 'text'): Promise<string | null>;
	async get<T = unknown>(key: string, type: 'text' | 'json' = 'text'): Promise<any> {
		// Check expiration
		const expiration = this.expirations.get(key);
		if (expiration && expiration < Date.now()) {
			this.store.delete(key);
			this.expirations.delete(key);
			return null;
		}

		const value = this.store.get(key);
		if (value === undefined) {
			return null;
		}

		if (type === 'json') {
			try {
				return JSON.parse(value) as T;
			} catch (error) {
				throw new CollegeDBError(
					`Failed to parse JSON from KV for key ${key}: ${error instanceof Error ? error.message : String(error)}`,
					'KV_JSON_PARSE_FAILED'
				);
			}
		}

		return value;
	}

	async put(key: string, value: string, options?: { expirationTtl?: number }): Promise<void> {
		this.store.set(key, value);

		if (options?.expirationTtl) {
			this.expirations.set(key, Date.now() + options.expirationTtl * 1000);
		} else {
			this.expirations.delete(key);
		}
	}

	async set(key: string, value: unknown, options?: { ttl?: number }): Promise<void> {
		this.store.set(key, this.normalizeValue(value));

		if (options?.ttl) {
			this.expirations.set(key, Date.now() + options.ttl * 1000);
		} else {
			this.expirations.delete(key);
		}
	}

	async del(key: string): Promise<void> {
		await this.delete(key);
	}

	async keys(prefix: string = ''): Promise<string[]> {
		const listed = await this.list({ prefix });
		return listed.keys.map((key) => key.name);
	}

	async getItem<T = unknown>(key: string): Promise<T | null> {
		const value = await this.get(key, 'text');
		return (value as T | null) ?? null;
	}

	async setItem(key: string, value: unknown): Promise<void> {
		await this.set(key, value);
	}

	async removeItem(key: string): Promise<void> {
		await this.delete(key);
	}

	async getKeys(prefix: string = ''): Promise<string[]> {
		return await this.keys(prefix);
	}

	async delete(key: string): Promise<void> {
		this.store.delete(key);
		this.expirations.delete(key);
	}

	async list(options?: { prefix?: string; cursor?: string; limit?: number }): Promise<KVListResult> {
		const prefix = options?.prefix ?? '';
		const limit = options?.limit ?? 1000;
		let cursor = options?.cursor ? parseInt(options.cursor, 10) : 0;

		// Clean up expired keys first
		const now = Date.now();
		for (const [key, expiration] of this.expirations) {
			if (expiration < now) {
				this.store.delete(key);
				this.expirations.delete(key);
			}
		}

		// Filter keys by prefix
		const allKeys = Array.from(this.store.keys()).filter((k) => k.startsWith(prefix));

		// Apply cursor-based pagination
		const keys = allKeys.slice(cursor, cursor + limit).map((name) => ({ name }));
		const nextCursor = cursor + limit;
		const listComplete = nextCursor >= allKeys.length;

		return {
			keys,
			cursor: listComplete ? undefined : String(nextCursor),
			list_complete: listComplete
		};
	}

	/**
	 * Clear all data from the store.
	 */
	clear(): void {
		this.store.clear();
		this.expirations.clear();
	}

	/**
	 * Get the current size of the store.
	 */
	size(): number {
		return this.store.size;
	}
}

/**
 * Factory function to create an in-memory SQL database provider.
 *
 * @returns InMemorySQLDatabase instance
 *
 * @example
 * ```typescript
 * import { createInMemorySQLProvider } from '@earth-app/collegedb';
 *
 * const db = createInMemorySQLProvider();
 * initialize({
 *   kv: new InMemoryKVStorage(),
 *   shards: {
 *     'shard-1': db,
 *     'shard-2': db
 *   }
 * });
 * ```
 */
export function createInMemorySQLProvider(): InMemorySQLDatabase {
	return new InMemorySQLDatabase();
}

/**
 * Factory function to create an in-memory KV storage provider.
 *
 * @returns InMemoryKVStorage instance
 *
 * @example
 * ```typescript
 * import { createInMemoryKVProvider } from '@earth-app/collegedb';
 *
 * const kv = createInMemoryKVProvider();
 * initialize({
 *   kv,
 *   shards: { / * ... * / }
 * });
 * ```
 */
export function createInMemoryKVProvider(): InMemoryKVStorage {
	return new InMemoryKVStorage();
}
