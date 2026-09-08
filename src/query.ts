/**
 * @fileoverview Deterministic SQL builders for object-shaped CRUD statements.
 *
 * These helpers turn plain JavaScript objects into parameterized SQL strings
 * plus a positional bindings array. They exist to remove the hand-aligned
 * `(col, col, ...) VALUES (?, ?, ...)` boilerplate that every CollegeDB
 * consumer otherwise rewrites per table, and the class of bugs that comes with
 * it (columns and bindings drifting out of order).
 *
 * The module is intentionally dependency-free and side-effect-free: it never
 * touches the router, the global config, or any shard. Callers pair these
 * builders with the routed helpers in `crud.ts` (or call `run`/`first`
 * directly) to execute the generated statement. Because the builders are pure,
 * they are trivially unit-testable in isolation.
 *
 * All column and table names are validated and double-quoted, so object keys
 * can never inject SQL. Values always flow through positional `?` bindings.
 *
 * @author Gregory Mitchell
 * @since 1.2.4
 */

import { CollegeDBError } from './errors';
import type { SQLDialect } from './types';

/**
 * A built statement: the parameterized SQL text and its positional bindings.
 */
export interface BuiltQuery {
	/** Parameterized SQL text using `?` placeholders */
	sql: string;
	/** Positional bindings in the same order as the `?` placeholders */
	bindings: any[];
}

/**
 * Column-name to value map used by the object builders. `undefined` values are
 * skipped (treated as "not provided"); `null` is written as a real SQL NULL.
 */
export type ColumnValues = Record<string, unknown>;

/**
 * Options accepted by {@link buildInsert}.
 */
export interface BuildInsertOptions {
	/** Dialect of the target shard, used to quote identifiers @since 1.4.0 */
	dialect?: SQLDialect;
	/** Emit `INSERT OR REPLACE` (SQLite) instead of a plain `INSERT` */
	orReplace?: boolean;
	/** Emit `INSERT OR IGNORE` (SQLite) instead of a plain `INSERT` */
	orIgnore?: boolean;
	/** Append a `RETURNING` clause; `true` returns `*`, or pass explicit columns */
	returning?: boolean | string | string[];
}

/**
 * Options accepted by {@link buildUpsert}.
 */
export interface BuildUpsertOptions {
	/** Dialect of the target shard, used to quote identifiers @since 1.4.0 */
	dialect?: SQLDialect;
	/**
	 * Columns to overwrite on conflict. Defaults to every inserted column that
	 * is not part of `conflictColumns`.
	 */
	update?: string[];
	/** Append a `RETURNING` clause; `true` returns `*`, or pass explicit columns */
	returning?: boolean | string | string[];
}

const SQL_IDENTIFIER_PART_REGEX = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * Memoized {@link quoteIdentifier} results, keyed by identifier and dialect.
 *
 * Every generated statement quotes at least a table name, and the cross-shard
 * helpers quote the same one once per shard on every call. The set of table and
 * column names an application uses is small and fixed, so this converts the
 * validation and the string building into a map lookup after first use.
 * @private
 */
const quotedIdentifiers = new Map<string, string>();

/** Bound on the quote cache, since a caller can pass generated names. @private */
const QUOTE_CACHE_LIMIT = 1000;

/**
 * Validates and quotes a SQL identifier (optionally schema-qualified).
 *
 * Rejects anything that is not a bare identifier, so object keys and table names
 * can never smuggle SQL into the generated statement. The quote character
 * follows `dialect`: backticks for MySQL and MariaDB, which reject ANSI double
 * quotes unless `ANSI_QUOTES` is set, and double quotes everywhere else.
 *
 * @param identifier - Table or column name (may be `schema.name`)
 * @param dialect - Dialect of the target shard; omitted means ANSI double quotes
 * @returns The quoted identifier
 * @throws {CollegeDBError} If the identifier is empty or not a bare SQL identifier
 * @since 1.2.4
 */
export function quoteIdentifier(identifier: string, dialect?: SQLDialect): string {
	const cacheKey = dialect === undefined ? identifier : `${identifier}:${dialect}`;
	const cached = quotedIdentifiers.get(cacheKey);
	if (cached !== undefined) {
		return cached;
	}

	const quote = dialect === 'mysql' ? '`' : '"';
	const quoted = validateIdentifier(identifier)
		.map((part) => `${quote}${part}${quote}`)
		.join('.');

	// An invalid identifier throws above and is never cached, so a rejection
	// stays a rejection on every later call.
	if (quotedIdentifiers.size >= QUOTE_CACHE_LIMIT) {
		quotedIdentifiers.clear();
	}
	quotedIdentifiers.set(cacheKey, quoted);

	return quoted;
}

/**
 * Validates a SQL identifier and returns its parts unquoted.
 *
 * The validation is what makes an object key safe to interpolate: anything that
 * is not a bare identifier is rejected outright, so no quoting style can be
 * escaped. Use this where a bare name is required, such as a `COLLATE` clause.
 *
 * @param identifier - Table or column name (may be `schema.name`)
 * @returns The validated identifier parts
 * @throws {CollegeDBError} If the identifier is empty or not a bare SQL identifier
 * @since 1.4.0
 */
export function validateIdentifier(identifier: string): string[] {
	const trimmed = identifier.trim();
	if (!trimmed) {
		throw new CollegeDBError('Identifier cannot be empty', 'INVALID_IDENTIFIER');
	}

	const parts = trimmed.split('.').map((part) => part.trim());
	if (parts.some((part) => !part || !SQL_IDENTIFIER_PART_REGEX.test(part))) {
		throw new CollegeDBError(`Invalid SQL identifier: ${identifier}`, 'INVALID_IDENTIFIER');
	}

	return parts;
}

/**
 * Drops `undefined` entries and returns `[column, value]` pairs, preserving
 * insertion order. `null` is preserved so callers can write explicit NULLs.
 * @private
 */
function definedEntries(values: ColumnValues): Array<[string, unknown]> {
	return Object.entries(values).filter(([, value]) => value !== undefined);
}

/**
 * Builds a `RETURNING` clause fragment (including the leading space) from the
 * `returning` option. Returns an empty string when returning is falsy.
 * @private
 */
function buildReturningClause(returning: boolean | string | string[] | undefined, dialect?: SQLDialect): string {
	if (!returning) {
		return '';
	}

	if (returning === true) {
		return ' RETURNING *';
	}

	const columns = (Array.isArray(returning) ? returning : [returning]).map((column) => quoteIdentifier(column, dialect));
	return ` RETURNING ${columns.join(', ')}`;
}

/**
 * Builds a compound `WHERE` clause from an object of equality conditions.
 *
 * Each key becomes `"col" = ?` (its value is appended to `bindings`), except
 * for `null` values which become `"col" IS NULL` and consume no binding. All
 * conditions are joined with `AND`.
 *
 * @param where - Column to value conditions (must contain at least one entry)
 * @returns The `WHERE`-body clause (without the leading `WHERE` keyword) and its bindings
 * @throws {CollegeDBError} If `where` has no usable conditions
 * @private
 */
function buildWhereClause(where: ColumnValues, dialect?: SQLDialect): BuiltQuery {
	const entries = definedEntries(where);
	if (entries.length === 0) {
		throw new CollegeDBError('A WHERE condition is required', 'EMPTY_WHERE');
	}

	const bindings: any[] = [];
	const clauses = entries.map(([column, value]) => {
		const quoted = quoteIdentifier(column, dialect);
		if (value === null) {
			return `${quoted} IS NULL`;
		}
		bindings.push(value);
		return `${quoted} = ?`;
	});

	return { sql: clauses.join(' AND '), bindings };
}

/**
 * Builds an `INSERT` statement from a column to value object.
 *
 * Column order follows object key order. Values flow through positional
 * bindings, so key names are quoted-and-validated while values are never
 * interpolated. `undefined` values are skipped; `null` inserts a SQL NULL.
 *
 * @param table - Target table name
 * @param values - Column to value map for the new row
 * @param options - Insert modifiers (`OR REPLACE`/`OR IGNORE`, `RETURNING`)
 * @returns The parameterized SQL and its bindings
 * @throws {CollegeDBError} If no columns are provided or an identifier is invalid
 * @example
 * ```typescript
 * const { sql, bindings } = buildInsert('users', { id: 'u1', name: 'Ada', age: 36 });
 * // sql: INSERT INTO "users" ("id", "name", "age") VALUES (?, ?, ?)
 * // bindings: ['u1', 'Ada', 36]
 * ```
 */
export function buildInsert(table: string, values: ColumnValues, options: BuildInsertOptions = {}): BuiltQuery {
	const entries = definedEntries(values);
	if (entries.length === 0) {
		throw new CollegeDBError('At least one column value is required for INSERT', 'EMPTY_INSERT');
	}

	const quotedTable = quoteIdentifier(table, options.dialect);
	const columns = entries.map(([column]) => quoteIdentifier(column, options.dialect));
	const placeholders = entries.map(() => '?');
	const bindings = entries.map(([, value]) => value);

	const prefix = options.orReplace ? 'INSERT OR REPLACE' : options.orIgnore ? 'INSERT OR IGNORE' : 'INSERT';
	const returning = buildReturningClause(options.returning, options.dialect);

	return {
		sql: `${prefix} INTO ${quotedTable} (${columns.join(', ')}) VALUES (${placeholders.join(', ')})${returning}`,
		bindings
	};
}

/**
 * Builds an `UPDATE` statement from a column to value object and a WHERE map.
 *
 * The `WHERE` map is required; an empty condition throws rather than emit an
 * unfiltered `UPDATE` that would rewrite every row. `null` in the values map
 * sets a column to SQL NULL; `null` in the where map matches with `IS NULL`.
 *
 * @param table - Target table name
 * @param values - Column to value map of changes to apply (at least one)
 * @param where - Column to value equality conditions (at least one)
 * @param options - Update modifiers (`RETURNING`)
 * @returns The parameterized SQL and its bindings (SET bindings first, then WHERE)
 * @throws {CollegeDBError} If no changes or no WHERE conditions are provided
 * @example
 * ```typescript
 * const { sql, bindings } = buildUpdate('users', { name: 'Ada' }, { id: 'u1' });
 * // sql: UPDATE "users" SET "name" = ? WHERE "id" = ?
 * // bindings: ['Ada', 'u1']
 * ```
 */
export function buildUpdate(
	table: string,
	values: ColumnValues,
	where: ColumnValues,
	options: { returning?: boolean | string | string[]; dialect?: SQLDialect } = {}
): BuiltQuery {
	const entries = definedEntries(values);
	if (entries.length === 0) {
		throw new CollegeDBError('At least one column value is required for UPDATE', 'EMPTY_UPDATE');
	}

	const quotedTable = quoteIdentifier(table, options.dialect);
	const setClauses = entries.map(([column]) => `${quoteIdentifier(column, options.dialect)} = ?`);
	const setBindings = entries.map(([, value]) => value);
	const whereClause = buildWhereClause(where, options.dialect);
	const returning = buildReturningClause(options.returning, options.dialect);

	return {
		sql: `UPDATE ${quotedTable} SET ${setClauses.join(', ')} WHERE ${whereClause.sql}${returning}`,
		bindings: [...setBindings, ...whereClause.bindings]
	};
}

/**
 * Builds a `DELETE` statement scoped by a WHERE map.
 *
 * The `WHERE` map is required; an empty condition throws rather than emit an
 * unfiltered `DELETE` that would clear the table.
 *
 * @param table - Target table name
 * @param where - Column to value equality conditions (at least one)
 * @returns The parameterized SQL and its bindings
 * @throws {CollegeDBError} If no WHERE conditions are provided
 * @example
 * ```typescript
 * const { sql, bindings } = buildDelete('users', { id: 'u1' });
 * // sql: DELETE FROM "users" WHERE "id" = ?
 * // bindings: ['u1']
 * ```
 */
export function buildDelete(table: string, where: ColumnValues, options: { dialect?: SQLDialect } = {}): BuiltQuery {
	const quotedTable = quoteIdentifier(table, options.dialect);
	const whereClause = buildWhereClause(where, options.dialect);
	return {
		sql: `DELETE FROM ${quotedTable} WHERE ${whereClause.sql}`,
		bindings: whereClause.bindings
	};
}

/**
 * Builds an "upsert" using the SQLite/PostgreSQL `ON CONFLICT ... DO UPDATE`
 * form. On conflict for `conflictColumns`, the listed `update` columns (or all
 * non-conflict columns by default) are overwritten with the incoming values
 * via the `excluded` pseudo-row.
 *
 * @param table - Target table name
 * @param values - Column to value map for the row to insert
 * @param conflictColumns - Column(s) that trigger the conflict resolution
 * @param options - Upsert modifiers (`update` subset, `RETURNING`)
 * @returns The parameterized SQL and its bindings
 * @throws {CollegeDBError} If no columns, no conflict columns, or an identifier is invalid
 * @example
 * ```typescript
 * const { sql, bindings } = buildUpsert('kv', { k: 'a', v: '1' }, 'k');
 * // sql: INSERT INTO "kv" ("k", "v") VALUES (?, ?) ON CONFLICT ("k") DO UPDATE SET "v" = excluded."v"
 * // bindings: ['a', '1']
 * ```
 */
export function buildUpsert(
	table: string,
	values: ColumnValues,
	conflictColumns: string | string[],
	options: BuildUpsertOptions = {}
): BuiltQuery {
	const conflicts = (Array.isArray(conflictColumns) ? conflictColumns : [conflictColumns]).filter((column) => column.trim().length > 0);
	if (conflicts.length === 0) {
		throw new CollegeDBError('At least one conflict column is required for upsert', 'EMPTY_CONFLICT');
	}

	const insert = buildInsert(table, values, { dialect: options.dialect });
	const conflictSet = new Set(conflicts.map((column) => column.trim()));

	const updateColumns = (options.update ?? definedEntries(values).map(([column]) => column)).filter((column) => !conflictSet.has(column));

	const conflictClause = conflicts.map((column) => quoteIdentifier(column, options.dialect)).join(', ');
	const returning = buildReturningClause(options.returning, options.dialect);

	if (updateColumns.length === 0) {
		// Nothing to overwrite; degrade to DO NOTHING so the insert stays idempotent.
		return {
			sql: `${insert.sql} ON CONFLICT (${conflictClause}) DO NOTHING${returning}`,
			bindings: insert.bindings
		};
	}

	const assignments = updateColumns.map((column) => {
		const quoted = quoteIdentifier(column, options.dialect);
		return `${quoted} = excluded.${quoted}`;
	});

	return {
		sql: `${insert.sql} ON CONFLICT (${conflictClause}) DO UPDATE SET ${assignments.join(', ')}${returning}`,
		bindings: insert.bindings
	};
}
