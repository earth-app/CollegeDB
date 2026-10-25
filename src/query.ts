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
	/** Dialect of the target shard, used to quote identifiers @since 1.3.0 */
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
	/** Dialect of the target shard, used to quote identifiers @since 1.3.0 */
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
 * @since 1.3.0
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
 * Builds the conflict clause of an upsert, from the leading space to the end.
 * @private
 */
function buildUpsertTail(values: ColumnValues, conflictColumns: string | string[], options: BuildUpsertOptions): string {
	const conflicts = (Array.isArray(conflictColumns) ? conflictColumns : [conflictColumns]).filter((column) => column.trim().length > 0);
	if (conflicts.length === 0) {
		throw new CollegeDBError('At least one conflict column is required for upsert', 'EMPTY_CONFLICT');
	}

	const conflictSet = new Set(conflicts.map((column) => column.trim()));
	const updateColumns = (options.update ?? definedEntries(values).map(([column]) => column)).filter((column) => !conflictSet.has(column));

	// MySQL names no conflict target, but the columns are still validated and quoted
	const conflictClause = conflicts.map((column) => quoteIdentifier(column, options.dialect)).join(', ');
	const returning = buildReturningClause(options.returning, options.dialect);

	if (options.dialect === 'mysql') {
		// VALUES() is the one form MySQL and MariaDB both accept; the row alias is MySQL only
		const assignments =
			updateColumns.length === 0
				? [`${quoteIdentifier(conflicts[0]!, options.dialect)} = ${quoteIdentifier(conflicts[0]!, options.dialect)}`]
				: updateColumns.map(
						(column) => `${quoteIdentifier(column, options.dialect)} = VALUES(${quoteIdentifier(column, options.dialect)})`
					);
		return ` ON DUPLICATE KEY UPDATE ${assignments.join(', ')}${returning}`;
	}

	if (updateColumns.length === 0) {
		// Nothing to overwrite; degrade to DO NOTHING so the insert stays idempotent.
		return ` ON CONFLICT (${conflictClause}) DO NOTHING${returning}`;
	}

	const assignments = updateColumns.map((column) => {
		const quoted = quoteIdentifier(column, options.dialect);
		return `${quoted} = excluded.${quoted}`;
	});

	return ` ON CONFLICT (${conflictClause}) DO UPDATE SET ${assignments.join(', ')}${returning}`;
}

/**
 * Builds an "upsert". SQLite and PostgreSQL get `ON CONFLICT ... DO UPDATE`:
 * on conflict for `conflictColumns`, the listed `update` columns (or all
 * non-conflict columns by default) are overwritten with the incoming values
 * via the `excluded` pseudo-row. MySQL and MariaDB get
 * `ON DUPLICATE KEY UPDATE col = VALUES(col)`, which reacts to a conflict on any
 * unique key of the table, so `conflictColumns` should name the primary key or
 * the unique key you mean. MySQL and MariaDB report 2 affected rows when the
 * update changed the row, 1 for an insert, and 0 when nothing changed.
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
	const tail = buildUpsertTail(values, conflictColumns, options);
	const insert = buildInsert(table, values, { dialect: options.dialect });

	return { sql: `${insert.sql}${tail}`, bindings: insert.bindings };
}

/**
 * Dialects the migration journal protocol has been proven on.
 *
 * SQLite includes D1 and a binding with no declared dialect. MySQL covers
 * MariaDB, which shares the dialect value and passed the same race. Each has a
 * concurrency test with a control arm behind it; a journal move on any other
 * dialect is refused. Postgres and MySQL writes are only safe when router
 * builder writes run through `runGuardedWrite`, which takes the key lock.
 * @since 1.4.0
 */
export const PROVEN_JOURNAL_DIALECTS: readonly SQLDialect[] = ['sqlite', 'postgres', 'mysql'];

/**
 * Whether a shard's dialect is covered by {@link PROVEN_JOURNAL_DIALECTS}.
 * An unset dialect is read as SQLite, matching how identifiers are quoted.
 * @since 1.4.0
 */
export function isProvenJournalDialect(dialect?: SQLDialect): boolean {
	return PROVEN_JOURNAL_DIALECTS.includes(dialect ?? 'sqlite');
}

/** Name of the shard-local tombstone and archive table. @since 1.4.0 */
export const RETIRED_TABLE = 'collegedb_retired';

/** Name of the shard-local topology fence table. @since 1.4.0 */
export const TOPOLOGY_TABLE = 'collegedb_topology';

/**
 * Null-safe equality of a quoted column against one bound value: SQLite `IS`,
 * Postgres `IS NOT DISTINCT FROM`, MySQL `<=>`.
 * @since 1.4.0
 */
export function nullSafeEquals(quotedColumn: string, dialect?: SQLDialect): string {
	if (dialect === 'postgres') {
		return `${quotedColumn} IS NOT DISTINCT FROM ?`;
	}
	return dialect === 'mysql' ? `${quotedColumn} <=> ?` : `${quotedColumn} IS ?`;
}

/** Name of the journal table that serializes recovery. @since 1.4.0 */
export const LEASE_TABLE = 'collegedb_recovery_lease';

/**
 * The lease generation a journal write must still match to land.
 * @since 1.4.0
 */
export interface LeaseFence {
	/** Journal scope of the lease row */
	scope: string;
	/** Generation the writer acquired */
	generation: number;
}

/**
 * The predicate of a fenced journal write: true while the lease row of the scope
 * still holds the writer's generation. A write that carries it matches no row once
 * another holder has taken the lease.
 * @since 1.4.0
 */
export function buildLeaseFence(fence: LeaseFence, dialect?: SQLDialect): BuiltQuery {
	const cols = (name: string) => quoteIdentifier(name, dialect);
	return {
		sql: `(SELECT ${cols('generation')} FROM ${cols(LEASE_TABLE)} WHERE ${cols('scope')} = ?) = ?`,
		bindings: [fence.scope, fence.generation]
	};
}

/**
 * Builds an insert that ignores a duplicate key instead of failing. With a
 * `fence` it becomes an `INSERT ... SELECT ... WHERE <lease still held>`, so the
 * row is written only while the lease generation matches.
 * @since 1.4.0
 */
export function buildInsertIgnore(table: string, values: ColumnValues, dialect?: SQLDialect, fence?: LeaseFence): BuiltQuery {
	const entries = definedEntries(values);
	const columns = entries.map(([column]) => quoteIdentifier(column, dialect)).join(', ');
	const placeholders = entries.map(() => '?').join(', ');
	const bindings = entries.map(([, value]) => value);
	const prefix = dialect === 'postgres' ? 'INSERT' : dialect === 'mysql' ? 'INSERT IGNORE' : 'INSERT OR IGNORE';
	const tail = dialect === 'postgres' ? ' ON CONFLICT DO NOTHING' : '';
	const into = `INTO ${quoteIdentifier(table, dialect)} (${columns})`;

	if (!fence) {
		return { sql: `${prefix} ${into} VALUES (${placeholders})${tail}`, bindings };
	}

	const predicate = buildLeaseFence(fence, dialect);
	return {
		sql: `${prefix} ${into} SELECT ${placeholders}${dialect === 'mysql' ? ' FROM DUAL' : ''} WHERE ${predicate.sql}${tail}`,
		bindings: [...bindings, ...predicate.bindings]
	};
}

/**
 * Builds the compare-and-swap that takes the recovery lease of a scope: insert
 * when no row exists, or take over when the current one has expired. The
 * generation rises by one on every takeover. The statement changes no row while
 * a live lease exists, but drivers differ on whether they report that, so the
 * caller reads the row back and compares the holder.
 * @since 1.4.0
 */
export function buildLeaseAcquire(scope: string, holder: string, now: number, ttlMs: number, dialect?: SQLDialect): BuiltQuery {
	const cols = (name: string) => quoteIdentifier(name, dialect);
	const table = cols(LEASE_TABLE);
	const insert = `INSERT INTO ${table} (${cols('scope')}, ${cols('holder')}, ${cols('generation')}, ${cols('expires_at')}) VALUES (?, ?, 1, ?)`;
	const expiresAt = now + ttlMs;

	if (dialect === 'mysql') {
		// assignments run left to right, so expires_at goes last: the others still read the old value
		const expired = `${cols('expires_at')} <= ?`;
		return {
			sql:
				`${insert} ON DUPLICATE KEY UPDATE ` +
				`${cols('holder')} = IF(${expired}, VALUES(${cols('holder')}), ${cols('holder')}), ` +
				`${cols('generation')} = IF(${expired}, ${cols('generation')} + 1, ${cols('generation')}), ` +
				`${cols('expires_at')} = IF(${expired}, VALUES(${cols('expires_at')}), ${cols('expires_at')})`,
			bindings: [scope, holder, expiresAt, now, now, now]
		};
	}

	return {
		sql:
			`${insert} ON CONFLICT (${cols('scope')}) DO UPDATE SET ${cols('holder')} = EXCLUDED.${cols('holder')}, ` +
			`${cols('generation')} = ${table}.${cols('generation')} + 1, ${cols('expires_at')} = EXCLUDED.${cols('expires_at')} ` +
			`WHERE ${table}.${cols('expires_at')} <= ?`,
		bindings: [scope, holder, expiresAt, now]
	};
}

/**
 * Builds the extension of a held lease, conditional on the holder's generation.
 * @since 1.4.0
 */
export function buildLeaseRenew(fence: LeaseFence, expiresAt: number, dialect?: SQLDialect): BuiltQuery {
	const cols = (name: string) => quoteIdentifier(name, dialect);
	return {
		sql: `UPDATE ${cols(LEASE_TABLE)} SET ${cols('expires_at')} = ? WHERE ${cols('scope')} = ? AND ${cols('generation')} = ?`,
		bindings: [expiresAt, fence.scope, fence.generation]
	};
}

/**
 * Builds the release of a held lease. The row stays with `expires_at` at zero,
 * so the generation keeps rising across holders.
 * @since 1.4.0
 */
export function buildLeaseRelease(fence: LeaseFence, dialect?: SQLDialect): BuiltQuery {
	return buildLeaseRenew(fence, 0, dialect);
}

/**
 * Identifies the tombstone a guarded write must not run past.
 * @since 1.4.0
 */
export interface TombstoneGuard {
	/** Journal scope */
	scope: string;
	/** Table the write targets, as the journal recorded it */
	table: string;
	/** Routing key of the record */
	key: string;
	/** Writer's topology generation; the fence compares it with `min_generation` */
	generation?: number;
}

/** Name of the lock-stripe table MySQL and MariaDB writers take row locks on. @since 1.4.0 */
export const LOCK_STRIPES_TABLE = 'collegedb_lock_stripes';

/** Number of key stripes in {@link LOCK_STRIPES_TABLE}. @since 1.4.0 */
export const LOCK_STRIPES = 256;

/** Row of {@link LOCK_STRIPES_TABLE} that the fence lock uses, beside the key stripes. @since 1.4.0 */
export const FENCE_STRIPE = LOCK_STRIPES;

/** Advisory lock id of the fence on Postgres. @private */
const FENCE_LOCK_ID = '4172601337920511817';

/**
 * 64-bit FNV-1a over scope, table and key.
 * @private
 */
function keyLockHash(guard: TombstoneGuard): bigint {
	let hash = 0xcbf29ce484222325n;
	for (const byte of new TextEncoder().encode(`${guard.scope}\0${guard.table}\0${guard.key}`)) {
		hash = BigInt.asUintN(64, (hash ^ BigInt(byte)) * 0x100000001b3n);
	}
	return hash;
}

/**
 * Builds the statements that serialize one key's cutover and writes. They are
 * empty on a dialect that needs none.
 *
 * Statement snapshots alone leave a window on Postgres and MySQL: a guarded
 * insert can read "no tombstone" before the cutover commits and insert after
 * the row is deleted. Both sides run these statements first, inside their own
 * transaction, so the second one waits for the first to commit and then reads
 * the committed state. Postgres takes `pg_advisory_xact_lock` on a hash of the
 * key, after pinning the transaction to READ COMMITTED: under REPEATABLE READ
 * the snapshot is taken at the lock statement, before the wait, and the
 * concurrency test shows ghost rows and lost acknowledged writes there. MySQL
 * and MariaDB lock one of {@link LOCK_STRIPES} rows with `SELECT ... FOR
 * UPDATE`; the row lock lasts until commit, where a named lock would have to be
 * released by hand after it. SQLite and D1 serialize writers and need nothing.
 *
 * @param guard - Scope, table and key of the record
 * @param dialect - Dialect of the shard the transaction runs on
 * @returns The statements to run first in the transaction, in order
 * @since 1.4.0
 */
export function buildWriteLock(guard: TombstoneGuard, dialect?: SQLDialect): BuiltQuery[] {
	if (dialect === 'postgres') {
		return [
			{ sql: 'SET TRANSACTION ISOLATION LEVEL READ COMMITTED', bindings: [] },
			{ sql: 'SELECT pg_advisory_xact_lock(CAST(? AS bigint))', bindings: [BigInt.asIntN(64, keyLockHash(guard)).toString()] }
		];
	}
	if (dialect === 'mysql') {
		return [
			{
				sql: `SELECT stripe FROM ${LOCK_STRIPES_TABLE} WHERE stripe = ? FOR UPDATE`,
				bindings: [Number(keyLockHash(guard) % BigInt(LOCK_STRIPES))]
			}
		];
	}
	return [];
}

/**
 * Builds the lock that orders a fence raise against in-flight writers on
 * Postgres, MySQL and MariaDB.
 *
 * A guarded insert checks the fence in its own statement, so a writer that read
 * the old fence can still commit after the raise and the census that follows it,
 * and the census then misses its row. Writers take this lock in `shared` mode
 * inside their transaction, and the raise takes it `exclusive` inside its own.
 * The raise waits for writers already in flight and holds back the ones that
 * come after, so a writer either commits before the raise or reads the new fence.
 * Postgres uses an advisory lock; MySQL and MariaDB lock {@link FENCE_STRIPE}
 * with `LOCK IN SHARE MODE` or `FOR UPDATE`, the one form both engines accept.
 * SQLite and D1 serialize writers and need nothing.
 *
 * @param mode - `shared` for a writer, `exclusive` for the raise
 * @param dialect - Dialect of the shard the transaction runs on
 * @returns The statements to run in the transaction after any isolation statement
 * @since 1.4.0
 */
export function buildFenceLock(mode: 'shared' | 'exclusive', dialect?: SQLDialect): BuiltQuery[] {
	if (dialect === 'postgres') {
		return [{ sql: `SELECT pg_advisory_xact_lock${mode === 'shared' ? '_shared' : ''}(CAST(? AS bigint))`, bindings: [FENCE_LOCK_ID] }];
	}
	if (dialect === 'mysql') {
		return [
			{
				sql: `SELECT stripe FROM ${LOCK_STRIPES_TABLE} WHERE stripe = ? ${mode === 'shared' ? 'LOCK IN SHARE MODE' : 'FOR UPDATE'}`,
				bindings: [FENCE_STRIPE]
			}
		];
	}
	return [];
}

/**
 * The shared tail of a guarded insert: a `WHERE` that fails when this shard
 * holds a tombstone for the key or when the topology fence is above the
 * writer's generation.
 * @private
 */
function buildTombstoneGuard(guard: TombstoneGuard, dialect?: SQLDialect): BuiltQuery {
	const retired = quoteIdentifier(RETIRED_TABLE, dialect);
	const topology = quoteIdentifier(TOPOLOGY_TABLE, dialect);
	const cols = (name: string) => quoteIdentifier(name, dialect);

	return {
		sql:
			`NOT EXISTS (SELECT 1 FROM ${retired} WHERE ${cols('scope')} = ? AND ${cols('table_name')} = ? AND ${cols('primary_key')} = ?) ` +
			`AND (SELECT COALESCE(MAX(${cols('min_generation')}), 0) FROM ${topology}) <= ?`,
		bindings: [guard.scope, guard.table, guard.key, guard.generation ?? 0]
	};
}

/**
 * Builds an `INSERT ... SELECT ... WHERE NOT EXISTS (tombstone)` so the tombstone
 * check and the write are one statement and cannot race a cutover.
 *
 * Zero rows inserted means a tombstone, a raised fence, or (with `orIgnore`) a
 * genuine conflict. MySQL needs `FROM DUAL` for a `SELECT` with a `WHERE`.
 *
 * @param table - Target table
 * @param values - Column to value map for the new row
 * @param guard - The tombstone and fence the write must clear
 * @param options - Insert modifiers
 * @returns The parameterized SQL and its bindings
 * @since 1.4.0
 */
export function buildGuardedInsert(
	table: string,
	values: ColumnValues,
	guard: TombstoneGuard,
	options: BuildInsertOptions = {}
): BuiltQuery {
	const entries = definedEntries(values);
	if (entries.length === 0) {
		throw new CollegeDBError('At least one column value is required for INSERT', 'EMPTY_INSERT');
	}

	const columns = entries.map(([column]) => quoteIdentifier(column, options.dialect));
	const placeholders = entries.map(() => '?').join(', ');
	const prefix = options.orReplace ? 'INSERT OR REPLACE' : options.orIgnore ? 'INSERT OR IGNORE' : 'INSERT';
	const fence = buildTombstoneGuard(guard, options.dialect);
	const dual = options.dialect === 'mysql' ? ' FROM DUAL' : '';

	return {
		sql:
			`${prefix} INTO ${quoteIdentifier(table, options.dialect)} (${columns.join(', ')}) ` +
			`SELECT ${placeholders}${dual} WHERE ${fence.sql}${buildReturningClause(options.returning, options.dialect)}`,
		bindings: [...entries.map(([, value]) => value), ...fence.bindings]
	};
}

/**
 * Builds the guarded form of {@link buildUpsert}. The select ends in a `WHERE`,
 * which SQLite requires before `ON CONFLICT` so the parser does not read `ON`
 * as a join constraint.
 *
 * @param table - Target table
 * @param values - Column to value map for the row
 * @param conflictColumns - Column(s) forming the conflict target
 * @param guard - The tombstone and fence the write must clear
 * @param options - Upsert modifiers
 * @returns The parameterized SQL and its bindings
 * @since 1.4.0
 */
export function buildGuardedUpsert(
	table: string,
	values: ColumnValues,
	conflictColumns: string | string[],
	guard: TombstoneGuard,
	options: BuildUpsertOptions = {}
): BuiltQuery {
	const tail = buildUpsertTail(values, conflictColumns, options);
	const insert = buildGuardedInsert(table, values, guard, { dialect: options.dialect });

	return { sql: `${insert.sql}${tail}`, bindings: insert.bindings };
}

/**
 * The statements that make a shard able to hold tombstones and archives.
 * @since 1.4.0
 */
export function buildShardSidecarSchema(dialect?: SQLDialect): string[] {
	const key = dialect === 'mysql' ? 'VARCHAR(190)' : 'TEXT';
	const long = dialect === 'mysql' ? 'LONGTEXT' : 'TEXT';
	const int = dialect === 'sqlite' || dialect === undefined ? 'INTEGER' : 'BIGINT';

	const statements = [
		`CREATE TABLE IF NOT EXISTS ${RETIRED_TABLE} (scope ${key} NOT NULL, table_name ${key} NOT NULL, primary_key ${key} NOT NULL, ` +
			`pk_column ${key} NOT NULL, op_id ${key} NOT NULL, generation ${int} NOT NULL, moved_to ${key} NOT NULL, payload ${long}, ` +
			`archived_at ${int} NOT NULL, purged_at ${int}, PRIMARY KEY (scope, table_name, primary_key))`,
		`CREATE TABLE IF NOT EXISTS ${TOPOLOGY_TABLE} (min_generation ${int} NOT NULL)`
	];

	if (dialect === 'mysql') {
		const stripes = Array.from({ length: LOCK_STRIPES + 1 }, (_, stripe) => `(${stripe})`).join(', ');
		statements.push(
			`CREATE TABLE IF NOT EXISTS ${LOCK_STRIPES_TABLE} (stripe INT NOT NULL, PRIMARY KEY (stripe))`,
			`INSERT IGNORE INTO ${LOCK_STRIPES_TABLE} (stripe) VALUES ${stripes}`
		);
	}

	return statements;
}

/** Name of the index that serves the history lookup by key. @since 1.4.0 */
export const HISTORY_KEY_INDEX = 'collegedb_history_by_key';

/**
 * One index a table needs. {@link ensureIndex} creates it, because MySQL has no
 * `CREATE INDEX IF NOT EXISTS`.
 * @since 1.4.0
 */
export interface IndexSpec {
	table: string;
	name: string;
	columns: string[];
}

/** The index {@link ensureJournalSchema} creates on the history table. @since 1.4.0 */
export const HISTORY_INDEX: IndexSpec = {
	table: 'collegedb_migration_history',
	name: HISTORY_KEY_INDEX,
	columns: ['scope', 'primary_key', 'table_name']
};

/**
 * The statements for the journal shard: the active claim table, the append-only
 * history, the durable placement exception set, the verification record and the
 * recovery lease. The history key index is not here; {@link ensureIndex} adds it
 * to a new and an existing table alike.
 * @since 1.4.0
 */
export function buildJournalSchema(dialect?: SQLDialect): string[] {
	const key = dialect === 'mysql' ? 'VARCHAR(190)' : 'TEXT';
	const long = dialect === 'mysql' ? 'LONGTEXT' : 'TEXT';
	const int = dialect === 'sqlite' || dialect === undefined ? 'INTEGER' : 'BIGINT';

	return [
		`CREATE TABLE IF NOT EXISTS collegedb_migration_active (scope ${key} NOT NULL, table_name ${key} NOT NULL, ` +
			`primary_key ${key} NOT NULL, op_id ${key} NOT NULL, claimed_at ${int} NOT NULL, PRIMARY KEY (scope, table_name, primary_key))`,
		`CREATE TABLE IF NOT EXISTS collegedb_migration_history (op_id ${key} NOT NULL, state ${key} NOT NULL, attempt ${int} NOT NULL, ` +
			`scope ${key} NOT NULL, table_name ${key} NOT NULL, primary_key ${key} NOT NULL, source ${key} NOT NULL, ` +
			`destination ${key} NOT NULL, generation ${int} NOT NULL, placement_epoch ${int}, digest ${key}, detail ${long}, ` +
			`at ${int} NOT NULL, PRIMARY KEY (op_id, state, attempt))`,
		`CREATE TABLE IF NOT EXISTS collegedb_placement_exceptions (scope ${key} NOT NULL, hashed_key ${key} NOT NULL, ` +
			`added_at ${int} NOT NULL, PRIMARY KEY (scope, hashed_key))`,
		`CREATE TABLE IF NOT EXISTS collegedb_migration_verification (scope ${key} NOT NULL, completed_at ${int} NOT NULL, ` +
			`clean ${int} NOT NULL, PRIMARY KEY (scope))`,
		`CREATE TABLE IF NOT EXISTS ${LEASE_TABLE} (scope ${key} NOT NULL, holder ${key} NOT NULL, generation ${int} NOT NULL, ` +
			`expires_at ${int} NOT NULL, PRIMARY KEY (scope))`
	];
}

/**
 * Inputs for {@link buildCutoverStatements}.
 * @since 1.4.0
 */
export interface CutoverSpec {
	/** Table holding the record */
	table: string;
	/** Primary-key column */
	pkColumn: string;
	/** Every column of the snapshot with the value to compare, in any order */
	columns: Array<[string, unknown]>;
	/** When set, only this column (plus the key) is compared */
	versionColumn?: string;
	/** Journal scope */
	scope: string;
	/** Routing key */
	key: string;
	/** Operation id the archive row carries */
	opId: string;
	/** Generation the archive row carries */
	generation: number;
	/** Binding the key moves to */
	movedTo: string;
	/** Archive payload, exactly the text that was hashed for the digest */
	payload: string;
	/** Archive timestamp in milliseconds */
	archivedAt: number;
	/** Dialect of the source shard */
	dialect?: SQLDialect;
}

/**
 * Builds the two cutover statements: an archive insert and a delete, both
 * conditional on one snapshot-equality predicate. Run together in one
 * transaction, a changed row makes both match nothing, so a miss writes no
 * archive row and no tombstone.
 *
 * @param spec - What to move and what the row must still look like
 * @returns The archive insert and the delete
 * @since 1.4.0
 */
export function buildCutoverStatements(spec: CutoverSpec): { archive: BuiltQuery; delete: BuiltQuery } {
	const { dialect } = spec;
	const pk = spec.columns.find(([name]) => name === spec.pkColumn);
	if (!pk) {
		throw new CollegeDBError(`Snapshot has no primary key column ${spec.pkColumn}`, 'MIGRATION_PK_MISSING');
	}

	const compared = spec.versionColumn
		? spec.columns.filter(([name]) => name === spec.versionColumn)
		: spec.columns.filter(([name]) => name !== spec.pkColumn);
	if (spec.versionColumn && compared.length === 0) {
		throw new CollegeDBError(`Snapshot has no version column ${spec.versionColumn}`, 'MIGRATION_VERSION_COLUMN_MISSING');
	}

	const predicate = [
		`${quoteIdentifier(spec.pkColumn, dialect)} = ?`,
		...compared.map(([name]) => nullSafeEquals(quoteIdentifier(name, dialect), dialect))
	];
	const predicateBindings = [pk[1], ...compared.map(([, value]) => value)];
	const table = quoteIdentifier(spec.table, dialect);
	const archiveColumns = ['scope', 'table_name', 'primary_key', 'pk_column', 'op_id', 'generation', 'moved_to', 'payload', 'archived_at'];
	const dual = dialect === 'mysql' ? ' FROM DUAL' : '';

	return {
		archive: {
			sql:
				`INSERT INTO ${quoteIdentifier(RETIRED_TABLE, dialect)} (${archiveColumns.map((c) => quoteIdentifier(c, dialect)).join(', ')}) ` +
				`SELECT ${archiveColumns.map(() => '?').join(', ')}${dual} WHERE EXISTS (SELECT 1 FROM ${table} WHERE ${predicate.join(' AND ')})`,
			bindings: [
				spec.scope,
				spec.table,
				spec.key,
				spec.pkColumn,
				spec.opId,
				spec.generation,
				spec.movedTo,
				spec.payload,
				spec.archivedAt,
				...predicateBindings
			]
		},
		delete: {
			sql: `DELETE FROM ${table} WHERE ${predicate.join(' AND ')}`,
			bindings: predicateBindings
		}
	};
}
