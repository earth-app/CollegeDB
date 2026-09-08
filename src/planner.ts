/**
 * @fileoverview Recovers the routing key from a statement.
 *
 * The routed API asks for the primary key as its own argument even when the
 * statement already contains it, so `INSERT INTO users (id, ...) VALUES (?, ...)`
 * makes the caller name the key twice. This module reads it back out of the SQL
 * so `query()` can route a statement the way `run()` routes an explicit key.
 *
 * It is a tokenizer over a fixed set of statement shapes, not a SQL parser.
 * Anything it cannot prove is reported as unroutable, and the caller decides
 * whether that throws or fans out. Guessing is not an option it has: a
 * misrouted write puts a row on a shard no reader will look at, and nothing
 * downstream would notice.
 *
 * Portability comes free. CollegeDB's own contract is `?` placeholders, and the
 * PostgreSQL adapter rewrites them to `$n` further down, so the planner sees
 * the same text regardless of which backend runs it and never touches a
 * provider.
 *
 * @author Gregory Mitchell
 * @since 1.4.0
 */

import { CollegeDBError } from './errors';

/**
 * What a statement was found to be, and which keys it routes to.
 * @since 1.4.0
 */
export interface QueryPlan {
	/** Statement kind that was recognized */
	kind: 'insert' | 'update' | 'delete' | 'select';
	/** Table the statement targets */
	table: string;
	/** Key column that was matched */
	keyColumn: string;
	/**
	 * Routing keys, in statement order. More than one means the statement fans
	 * out across shards: an `IN (...)` predicate or a multi-row `VALUES`.
	 */
	keys: string[];
	/** `true` when the statement reads and can be answered by a single shard per key */
	readOnly: boolean;
}

/**
 * Options for {@link planQuery}.
 * @since 1.4.0
 */
export interface PlanQueryOptions {
	/** Primary-key column per table. Tables not listed use `defaultKeyColumn`. */
	keyColumns?: Record<string, string>;
	/** Key column for tables absent from `keyColumns` (default: `id`) */
	defaultKeyColumn?: string;
}

/** Cache of parsed statement shapes, keyed by SQL text. @private */
const planCache = new Map<string, ParsedStatement | null>();

/** Bound on the parse cache, since a caller can generate SQL text. @private */
const PLAN_CACHE_LIMIT = 2000;

/**
 * A statement shape, independent of the bindings it will be run with.
 * @private
 */
interface ParsedStatement {
	kind: QueryPlan['kind'];
	table: string;
	/** Column names in an INSERT's column list, in order */
	insertColumns?: string[];
	/**
	 * For a multi-row INSERT, the placeholder ordinal of the key column in each
	 * row. For the other kinds, the placeholder ordinals matched by the key
	 * predicate.
	 */
	placeholderIndices?: number[];
	/** Literal key values written directly into the statement rather than bound */
	literals?: string[];
	/** Column the predicate or column list matched */
	keyColumn?: string;
}

/**
 * Strips string literals, comments, and backtick/bracket quoting so the shape
 * matchers never see a keyword or a column name that is really data.
 *
 * Replaced spans keep their length, so placeholder counting stays aligned with
 * the original text.
 * @private
 */
function scrub(sql: string): string {
	let out = '';
	let i = 0;

	while (i < sql.length) {
		const char = sql[i]!;

		if (char === '-' && sql[i + 1] === '-') {
			const end = sql.indexOf('\n', i);
			const stop = end === -1 ? sql.length : end;
			out += ' '.repeat(stop - i);
			i = stop;
			continue;
		}

		if (char === '/' && sql[i + 1] === '*') {
			const end = sql.indexOf('*/', i + 2);
			const stop = end === -1 ? sql.length : end + 2;
			out += ' '.repeat(stop - i);
			i = stop;
			continue;
		}

		if (char === "'" || char === '"' || char === '`') {
			const quote = char;
			let j = i + 1;
			while (j < sql.length) {
				if (sql[j] === quote) {
					if (sql[j + 1] === quote) {
						j += 2;
						continue;
					}
					break;
				}
				j++;
			}
			const stop = Math.min(sql.length, j + 1);

			// Double quotes and backticks delimit identifiers, so keep their
			// contents; single quotes delimit data, so blank it out.
			if (quote === "'") {
				out += ' '.repeat(stop - i);
			} else {
				out += ' ' + sql.slice(i + 1, stop - 1) + ' ';
			}

			i = stop;
			continue;
		}

		out += char;
		i++;
	}

	return out;
}

/**
 * Counts `?` placeholders in a span of scrubbed SQL.
 * @private
 */
function countPlaceholders(sql: string, from: number, to: number): number {
	let count = 0;
	for (let i = from; i < to && i < sql.length; i++) {
		if (sql[i] === '?') count++;
	}
	return count;
}

/** Matches a bare or dotted identifier, optionally double-quoted. @private */
const IDENTIFIER = '[A-Za-z_][A-Za-z0-9_]*';

/**
 * Finds the matching close parenthesis for the open parenthesis at `start`.
 * @private
 */
function matchParen(sql: string, start: number): number {
	let depth = 0;
	for (let i = start; i < sql.length; i++) {
		if (sql[i] === '(') depth++;
		else if (sql[i] === ')') {
			depth--;
			if (depth === 0) return i;
		}
	}
	return -1;
}

/**
 * Splits a comma-separated list at depth zero.
 * @private
 */
function splitTopLevel(text: string): string[] {
	const parts: string[] = [];
	let depth = 0;
	let current = '';

	for (const char of text) {
		if (char === '(') depth++;
		if (char === ')') depth--;
		if (char === ',' && depth === 0) {
			parts.push(current);
			current = '';
			continue;
		}
		current += char;
	}

	if (current.trim().length > 0) {
		parts.push(current);
	}

	return parts;
}

/** Removes surrounding quotes from an identifier. @private */
function unquote(value: string): string {
	return value
		.trim()
		.replace(/^["`\[]/, '')
		.replace(/["`\]]$/, '');
}

/**
 * Recognizes the statement shape, without reference to bindings.
 * @private
 */
function parseStatement(sql: string): ParsedStatement | null {
	const cached = planCache.get(sql);
	if (cached !== undefined) {
		return cached;
	}

	const parsed = parseStatementUncached(sql);

	if (planCache.size >= PLAN_CACHE_LIMIT) {
		const oldest = planCache.keys().next().value;
		if (oldest !== undefined) {
			planCache.delete(oldest);
		}
	}
	planCache.set(sql, parsed);

	return parsed;
}

/** @private */
function parseStatementUncached(sql: string): ParsedStatement | null {
	const scrubbed = scrub(sql);
	const trimmed = scrubbed.trim();

	// A leading CTE is not handled: the routing predicate could live in any of
	// its branches, and picking one would be a guess.
	if (/^WITH\b/i.test(trimmed)) {
		return null;
	}

	const insert = new RegExp(`^\\s*INSERT\\s+(?:OR\\s+\\w+\\s+)?INTO\\s+("?${IDENTIFIER}"?)\\s*\\(`, 'i').exec(scrubbed);
	if (insert) {
		return parseInsert(scrubbed, insert);
	}

	const update = new RegExp(`^\\s*UPDATE\\s+("?${IDENTIFIER}"?)\\s+SET\\b`, 'i').exec(scrubbed);
	if (update) {
		return { kind: 'update', table: unquote(update[1]!), ...parseWherePredicate(scrubbed) };
	}

	const del = new RegExp(`^\\s*DELETE\\s+FROM\\s+("?${IDENTIFIER}"?)`, 'i').exec(scrubbed);
	if (del) {
		return { kind: 'delete', table: unquote(del[1]!), ...parseWherePredicate(scrubbed) };
	}

	const select = new RegExp(`^\\s*SELECT\\b[\\s\\S]*?\\bFROM\\s+("?${IDENTIFIER}"?)`, 'i').exec(scrubbed);
	if (select) {
		// Joins and subqueries put more than one table in scope, so a bare column
		// name in the predicate no longer identifies a table unambiguously.
		if (/\bJOIN\b/i.test(scrubbed) || /\(\s*SELECT\b/i.test(scrubbed)) {
			return null;
		}
		return { kind: 'select', table: unquote(select[1]!), ...parseWherePredicate(scrubbed) };
	}

	return null;
}

/**
 * Reads an INSERT's column list and the placeholder ordinal each row uses.
 * @private
 */
function parseInsert(scrubbed: string, match: RegExpExecArray): ParsedStatement | null {
	const table = unquote(match[1]!);
	const open = scrubbed.indexOf('(', match.index + match[0].length - 1);
	const close = matchParen(scrubbed, open);
	if (open === -1 || close === -1) {
		return null;
	}

	const columns = splitTopLevel(scrubbed.slice(open + 1, close)).map(unquote);

	const valuesMatch = /\bVALUES\s*/i.exec(scrubbed.slice(close));
	if (!valuesMatch) {
		return null;
	}

	// Row groups after VALUES, each contributing one routing key.
	const rows: Array<{ start: number; end: number }> = [];
	let cursor = close + valuesMatch.index + valuesMatch[0].length;

	while (cursor < scrubbed.length) {
		const rowOpen = scrubbed.indexOf('(', cursor);
		if (rowOpen === -1) break;
		const rowClose = matchParen(scrubbed, rowOpen);
		if (rowClose === -1) break;

		rows.push({ start: rowOpen, end: rowClose });

		const between = scrubbed.slice(rowClose + 1);
		const next = /^\s*,/.exec(between);
		if (!next) break;
		cursor = rowClose + 1 + next[0].length;
	}

	if (rows.length === 0) {
		return null;
	}

	return {
		kind: 'insert',
		table,
		insertColumns: columns,
		placeholderIndices: rows.map((row) => countPlaceholders(scrubbed, 0, row.start)),
		literals: rows.map((row) => scrubbed.slice(row.start + 1, row.end))
	};
}

/**
 * Reads an equality or `IN` predicate from a WHERE clause.
 *
 * Only the whole clause being a single predicate on one column counts. An
 * `AND`/`OR` chain is rejected rather than having one conjunct picked out of it,
 * because the others may narrow the statement to a different row.
 * @private
 */
function parseWherePredicate(scrubbed: string): Pick<ParsedStatement, 'placeholderIndices' | 'literals' | 'keyColumn'> {
	const whereMatch = /\bWHERE\b/i.exec(scrubbed);
	if (!whereMatch) {
		return {};
	}

	const whereStart = whereMatch.index + whereMatch[0].length;
	const tail = /\b(GROUP\s+BY|ORDER\s+BY|LIMIT|RETURNING|HAVING|WINDOW)\b/i.exec(scrubbed.slice(whereStart));
	const whereEnd = tail ? whereStart + tail.index : scrubbed.length;
	const clause = scrubbed.slice(whereStart, whereEnd);

	if (/\b(AND|OR)\b/i.test(clause)) {
		return {};
	}

	const equality = new RegExp(`^\\s*("?${IDENTIFIER}"?)\\s*=\\s*\\?\\s*$`, 'i').exec(clause);
	if (equality) {
		return {
			keyColumn: unquote(equality[1]!),
			placeholderIndices: [countPlaceholders(scrubbed, 0, whereStart + clause.indexOf('?'))]
		};
	}

	const inMatch = new RegExp(`^\\s*("?${IDENTIFIER}"?)\\s+IN\\s*\\(([^)]*)\\)\\s*$`, 'i').exec(clause);
	if (inMatch) {
		const items = splitTopLevel(inMatch[2]!).map((item) => item.trim());
		if (items.length === 0 || !items.every((item) => item === '?')) {
			return {};
		}

		const listStart = whereStart + clause.indexOf('(');
		const before = countPlaceholders(scrubbed, 0, listStart);
		return {
			keyColumn: unquote(inMatch[1]!),
			placeholderIndices: items.map((_, offset) => before + offset)
		};
	}

	return {};
}

/**
 * Reads the routing key or keys out of a statement and its bindings.
 *
 * @param sql - Statement text using `?` placeholders
 * @param bindings - Positional bindings for the statement
 * @param options - Key-column configuration
 * @returns The plan, or `null` when the routing key cannot be proven
 * @since 1.4.0
 * @example
 * ```typescript
 * planQuery('SELECT * FROM users WHERE id = ?', ['user-1']);
 * // => { kind: 'select', table: 'users', keyColumn: 'id', keys: ['user-1'], readOnly: true }
 *
 * planQuery('SELECT * FROM users WHERE email = ?', ['a@b.c']);
 * // => null, because email is not the key column
 * ```
 */
export function planQuery(sql: string, bindings: any[] = [], options: PlanQueryOptions = {}): QueryPlan | null {
	const parsed = parseStatement(sql);
	if (!parsed) {
		return null;
	}

	const defaultColumn = options.defaultKeyColumn ?? 'id';
	const keyColumn = options.keyColumns?.[parsed.table] ?? defaultColumn;

	if (parsed.kind === 'insert') {
		const columnIndex = parsed.insertColumns?.findIndex((column) => column.toLowerCase() === keyColumn.toLowerCase()) ?? -1;
		if (columnIndex === -1 || !parsed.placeholderIndices) {
			return null;
		}

		const keys: string[] = [];
		for (let row = 0; row < parsed.placeholderIndices.length; row++) {
			const rowValues = splitTopLevel(parsed.literals?.[row] ?? '').map((value) => value.trim());
			const valueExpression = rowValues[columnIndex];
			if (valueExpression === undefined) {
				return null;
			}

			if (valueExpression === '?') {
				// The binding index is the placeholders before this row plus the
				// placeholders among this row's earlier columns.
				const placeholdersBeforeRow = parsed.placeholderIndices[row]!;
				const withinRow = rowValues.slice(0, columnIndex).filter((value) => value === '?').length;
				const key = bindings[placeholdersBeforeRow + withinRow];
				if (key === undefined || key === null) {
					return null;
				}
				keys.push(String(key));
				continue;
			}

			// A literal that is not a placeholder is only usable when it is a plain
			// number; a quoted string was blanked out by the scrubber and an
			// expression cannot be evaluated here.
			if (/^-?\d+(\.\d+)?$/.test(valueExpression)) {
				keys.push(valueExpression);
				continue;
			}

			return null;
		}

		return { kind: 'insert', table: parsed.table, keyColumn, keys, readOnly: false };
	}

	if (!parsed.keyColumn || parsed.keyColumn.toLowerCase() !== keyColumn.toLowerCase() || !parsed.placeholderIndices) {
		return null;
	}

	const keys: string[] = [];
	for (const index of parsed.placeholderIndices) {
		const value = bindings[index];
		if (value === undefined || value === null) {
			return null;
		}
		keys.push(String(value));
	}

	if (keys.length === 0) {
		return null;
	}

	return {
		kind: parsed.kind,
		table: parsed.table,
		keyColumn,
		keys,
		readOnly: parsed.kind === 'select'
	};
}

/**
 * Raised when a statement's routing key cannot be proven and the configuration
 * asks for that to be an error rather than a fanout.
 *
 * @param sql - The statement that could not be planned
 * @returns The error to throw
 * @since 1.4.0
 */
export function unroutableError(sql: string): CollegeDBError {
	const preview = sql.length > 120 ? `${sql.slice(0, 117)}...` : sql;
	return new CollegeDBError(
		`Could not determine a routing key for: ${preview}\n` +
			'Pass the key explicitly with run/first/all, declare the table in `keyColumns`, ' +
			'or set `onUnroutable: "fanout"` to query every shard.',
		'UNROUTABLE_QUERY'
	);
}

/**
 * Clears the parsed-statement cache. Intended for tests.
 * @internal
 */
export function resetPlanCache(): void {
	planCache.clear();
}
