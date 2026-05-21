/**
 * @fileoverview In-memory mock providers for testing and development.
 *
 * Zero-dependency, lightweight implementations of the {@link KVStorage} and
 * {@link SQLDatabase} contracts. Intended for unit tests, integration tests,
 * local development, and sandboxed routing experiments. The SQL implementation
 * emulates a useful subset of SQLite syntax — enough to drive CollegeDB's
 * router and most ORM-generated CRUD statements without spinning up a real
 * database.
 *
 * The SQL emulator supports:
 *
 * - `CREATE TABLE [IF NOT EXISTS]` with simple column definitions and inline
 *   PRIMARY KEY / AUTOINCREMENT / DEFAULT clauses
 * - `INSERT [OR REPLACE|IGNORE|...] INTO` with explicit column lists and
 *   `RETURNING` rows
 * - `UPDATE ... SET col = ?, col = expr WHERE ...` with compound WHERE
 *   (AND, OR, parens, =, !=, <, >, <=, >=, LIKE, IS NULL, IS NOT NULL)
 * - `DELETE FROM ... WHERE ...` with the same compound WHERE support
 * - `SELECT` projections including `*`, plain columns, aggregate calls
 *   (`COUNT(*)`, `COUNT(col)`, `MAX`, `MIN`, `SUM`, `AVG`), `COALESCE`,
 *   simple arithmetic on aggregate expressions, `AS` aliases, `ORDER BY`,
 *   `LIMIT`, and `OFFSET`
 * - Common metadata queries (`PRAGMA page_count`, `PRAGMA page_size`,
 *   `PRAGMA table_info`, `SELECT ... FROM sqlite_master`)
 *
 * The emulator deliberately does not implement transactions, indexes,
 * triggers, full SQL expression evaluation, or cross-table joins. Production
 * workloads should always run against real D1/Postgres/MySQL/SQLite shards
 * via the adapters in `providers.ts`.
 *
 * @author CollegeDB Team
 * @since 1.2.0
 */

import { CollegeDBError } from './errors';
import type { KVListResult, KVStorage, PreparedStatement, QueryResult, SQLDatabase } from './types';

/**
 * Column metadata captured when a CREATE TABLE statement is parsed.
 * @private
 */
interface ColumnInfo {
	name: string;
	type: string;
	isPrimaryKey: boolean;
	isAutoIncrement: boolean;
	defaultValue?: unknown;
	notNull?: boolean;
}

/**
 * Drizzle-style query payloads passed to {@link InMemorySQLDatabase}.
 * @private
 */
type InMemoryDrizzleQuery = {
	toQuery?: (config: Record<string, unknown>) => { sql?: string; params?: unknown[] };
	sql?: string;
	params?: unknown[];
	text?: string;
};

/**
 * Format config requested by `drizzle-orm` when building parameterized SQL.
 * Used purely to coax Drizzle into emitting `?` placeholders.
 * @private
 */
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

const COMPARISON_OPERATORS = ['<=', '>=', '!=', '<>', '=', '<', '>'] as const;
type ComparisonOperator = (typeof COMPARISON_OPERATORS)[number];

const AGGREGATE_FUNCTIONS = new Set(['COUNT', 'MAX', 'MIN', 'SUM', 'AVG']);
const SUPPORTED_SCALARS = new Set(['COALESCE', 'IFNULL', 'LOWER', 'UPPER', 'LENGTH', 'ABS']);

/**
 * Strips back-ticks, double quotes, square brackets, and schema prefixes from an SQL identifier.
 * @private
 */
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

/**
 * Escapes a literal value before injection into a `RegExp` constructor.
 * @private
 */
function escapeRegExp(value: string): string {
	return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Tries to coerce a value to a finite number.
 * @private
 */
function toFiniteNumber(value: unknown): number | undefined {
	if (typeof value === 'number' && Number.isFinite(value)) {
		return value;
	}
	if (typeof value === 'bigint') {
		return Number(value);
	}
	if (typeof value === 'string') {
		const trimmed = value.trim();
		if (trimmed === '') {
			return undefined;
		}
		const parsed = Number(trimmed);
		return Number.isFinite(parsed) ? parsed : undefined;
	}
	return undefined;
}

/**
 * Splits a comma-separated SQL fragment while respecting parenthesis depth.
 * Used to break apart SELECT column lists and SET assignments without
 * tripping over function calls like `COALESCE(a, 0)`.
 * @private
 */
function splitTopLevel(fragment: string, separator: string = ','): string[] {
	const parts: string[] = [];
	let depth = 0;
	let buffer = '';
	let inSingleQuote = false;
	let inDoubleQuote = false;

	for (let i = 0; i < fragment.length; i++) {
		const char = fragment[i]!;

		if (char === "'" && !inDoubleQuote) {
			inSingleQuote = !inSingleQuote;
			buffer += char;
			continue;
		}
		if (char === '"' && !inSingleQuote) {
			inDoubleQuote = !inDoubleQuote;
			buffer += char;
			continue;
		}

		if (!inSingleQuote && !inDoubleQuote) {
			if (char === '(') {
				depth++;
			} else if (char === ')') {
				depth = Math.max(0, depth - 1);
			} else if (depth === 0 && char === separator) {
				parts.push(buffer.trim());
				buffer = '';
				continue;
			}
		}

		buffer += char;
	}

	const tail = buffer.trim();
	if (tail.length > 0) {
		parts.push(tail);
	}
	return parts;
}

/**
 * Finds the matching closing parenthesis index for the `(` at `openIndex`.
 * @private
 */
function findMatchingParen(text: string, openIndex: number): number {
	let depth = 0;
	let inSingleQuote = false;
	let inDoubleQuote = false;

	for (let i = openIndex; i < text.length; i++) {
		const char = text[i]!;
		if (char === "'" && !inDoubleQuote) {
			inSingleQuote = !inSingleQuote;
			continue;
		}
		if (char === '"' && !inSingleQuote) {
			inDoubleQuote = !inDoubleQuote;
			continue;
		}
		if (inSingleQuote || inDoubleQuote) {
			continue;
		}
		if (char === '(') depth++;
		else if (char === ')') {
			depth--;
			if (depth === 0) return i;
		}
	}
	return -1;
}

/**
 * Parses a JS literal from a SQL expression — strings, numbers, NULL, TRUE/FALSE.
 * @private
 */
function parseLiteral(expression: string): { value: unknown; consumed: boolean } {
	const text = expression.trim();
	if (text.length === 0) {
		return { value: null, consumed: false };
	}

	if (text === '?') {
		return { value: null, consumed: false };
	}

	if (/^null$/i.test(text)) {
		return { value: null, consumed: true };
	}

	if (/^true$/i.test(text)) {
		return { value: 1, consumed: true };
	}

	if (/^false$/i.test(text)) {
		return { value: 0, consumed: true };
	}

	if ((text.startsWith("'") && text.endsWith("'")) || (text.startsWith('"') && text.endsWith('"'))) {
		const inner = text.slice(1, -1).replace(/''/g, "'").replace(/""/g, '"');
		return { value: inner, consumed: true };
	}

	const numeric = Number(text);
	if (Number.isFinite(numeric) && /^-?\d+(\.\d+)?$/.test(text)) {
		return { value: numeric, consumed: true };
	}

	return { value: undefined, consumed: false };
}

/**
 * Compound WHERE node — either a logical join or a single comparison/IS NULL leaf.
 * @private
 */
type WhereNode =
	| { type: 'and' | 'or'; children: WhereNode[] }
	| {
			type: 'compare';
			column: string;
			operator: ComparisonOperator | 'LIKE' | 'NOT LIKE';
			value: { kind: 'literal'; value: unknown } | { kind: 'param' };
	  }
	| {
			type: 'null-check';
			column: string;
			operator: 'IS NULL' | 'IS NOT NULL';
	  }
	| { type: 'always-true' };

/**
 * Pulls the trailing `WHERE ...` substring out of an SQL statement, returning
 * everything from the WHERE keyword to either the next clause (`ORDER BY`,
 * `LIMIT`, `OFFSET`, `GROUP BY`, `RETURNING`, `;`) or the end of the string.
 * Quoted strings and parenthesized expressions are respected.
 * @private
 */
function extractWhereSegment(sql: string): { whereClause: string | null; afterWhere: string } {
	const upper = sql.toUpperCase();
	const whereIdx = findKeyword(upper, sql, 'WHERE');
	if (whereIdx === -1) {
		// Even without a WHERE clause we want the post-FROM tail so
		// extractLimitOffset / extractOrderBy can locate clauses like
		// `ORDER BY age DESC LIMIT 5`.
		const fromIdx = findKeyword(upper, sql, 'FROM');
		const tail = fromIdx === -1 ? '' : sql.slice(fromIdx + 'FROM'.length);
		return { whereClause: null, afterWhere: tail };
	}

	const startOfWhere = whereIdx + 'WHERE'.length;
	const terminators = ['ORDER BY', 'GROUP BY', 'HAVING', 'LIMIT', 'OFFSET', 'RETURNING', ';'];

	let endIdx = sql.length;
	for (const terminator of terminators) {
		const idx = findKeyword(upper, sql, terminator, startOfWhere);
		if (idx !== -1 && idx < endIdx) {
			endIdx = idx;
		}
	}

	return {
		whereClause: sql.slice(startOfWhere, endIdx).trim(),
		afterWhere: sql.slice(endIdx)
	};
}

/**
 * Finds a keyword in `sql` (case-insensitive) by matching against `upper`,
 * ignoring positions inside quoted strings. Returns `-1` if not present.
 * @private
 */
function findKeyword(upper: string, original: string, keyword: string, fromIndex: number = 0): number {
	let i = fromIndex;
	let inSingle = false;
	let inDouble = false;

	while (i <= upper.length - keyword.length) {
		const char = original[i]!;
		if (char === "'" && !inDouble) {
			inSingle = !inSingle;
			i++;
			continue;
		}
		if (char === '"' && !inSingle) {
			inDouble = !inDouble;
			i++;
			continue;
		}
		if (!inSingle && !inDouble) {
			if (upper.startsWith(keyword, i)) {
				const before = i === 0 ? ' ' : upper[i - 1]!;
				const after = upper[i + keyword.length] ?? ' ';
				const startBoundary = /[A-Z0-9_]/.test(before) === false;
				const endBoundary = /[A-Z0-9_]/.test(after) === false;
				if (startBoundary && endBoundary) {
					return i;
				}
			}
		}
		i++;
	}
	return -1;
}

/**
 * Pulls out the next LIMIT and OFFSET integer literals from `afterWhere`, if present.
 * @private
 */
function extractLimitOffset(afterWhere: string): { limit?: number; offset?: number } {
	const upper = afterWhere.toUpperCase();
	const result: { limit?: number; offset?: number } = {};

	const limitIdx = findKeyword(upper, afterWhere, 'LIMIT');
	if (limitIdx !== -1) {
		const tail = afterWhere.slice(limitIdx + 'LIMIT'.length).trim();
		const match = tail.match(/^(\d+)(?:\s*,\s*(\d+))?/);
		if (match) {
			const first = Number(match[1]);
			const second = match[2] !== undefined ? Number(match[2]) : undefined;
			if (second !== undefined) {
				result.offset = first;
				result.limit = second;
			} else {
				result.limit = first;
			}
		}
	}

	const offsetIdx = findKeyword(upper, afterWhere, 'OFFSET');
	if (offsetIdx !== -1) {
		const tail = afterWhere.slice(offsetIdx + 'OFFSET'.length).trim();
		const match = tail.match(/^(\d+)/);
		if (match) {
			result.offset = Number(match[1]);
		}
	}

	return result;
}

/**
 * Splits the body of a SET clause into `(column, expression)` pairs while
 * respecting parenthesis depth and quoted strings. Replaces the buggy
 * `/SET\s+([^W]+?)WHERE/i` extractor — the original regex's `[^W]` accidentally
 * also excluded lowercase `w` under the `/i` flag, dropping any column whose
 * name contained `w` (for example `wrapped_dek`).
 * @private
 */
function parseSetAssignments(setBody: string): Array<{ column: string; expression: string }> {
	const assignments: Array<{ column: string; expression: string }> = [];
	for (const segment of splitTopLevel(setBody, ',')) {
		const eqIdx = segment.indexOf('=');
		if (eqIdx === -1) continue;
		const column = normalizeSqlIdentifier(segment.slice(0, eqIdx));
		const expression = segment.slice(eqIdx + 1).trim();
		if (column.length === 0) continue;
		assignments.push({ column, expression });
	}
	return assignments;
}

/**
 * Tokenizes a WHERE expression into an array of strings.
 * @private
 */
function tokenizeWhere(expression: string): string[] {
	const tokens: string[] = [];
	let i = 0;
	while (i < expression.length) {
		const char = expression[i]!;

		if (/\s/.test(char)) {
			i++;
			continue;
		}

		if (char === '(' || char === ')') {
			tokens.push(char);
			i++;
			continue;
		}

		if (char === '?') {
			tokens.push('?');
			i++;
			continue;
		}

		if (char === "'" || char === '"') {
			const quote = char;
			let end = i + 1;
			while (end < expression.length) {
				if (expression[end] === quote) {
					if (expression[end + 1] === quote) {
						end += 2;
						continue;
					}
					end++;
					break;
				}
				end++;
			}
			tokens.push(expression.slice(i, end));
			i = end;
			continue;
		}

		let matchedOperator: string | null = null;
		for (const op of COMPARISON_OPERATORS) {
			if (expression.startsWith(op, i)) {
				matchedOperator = op;
				break;
			}
		}
		if (matchedOperator) {
			tokens.push(matchedOperator);
			i += matchedOperator.length;
			continue;
		}

		// Identifier or word token (operator keywords like AND/OR/LIKE/IS/NULL/NOT).
		let end = i;
		while (end < expression.length && /[A-Za-z0-9_.$]/.test(expression[end]!)) {
			end++;
		}
		if (end === i) {
			tokens.push(char);
			i++;
		} else {
			tokens.push(expression.slice(i, end));
			i = end;
		}
	}
	return tokens;
}

/**
 * Tiny recursive-descent parser that converts a tokenized WHERE clause into a
 * tree of `WhereNode`s. Supports AND/OR with parenthesized groups and the
 * comparison/null-check operators enumerated above.
 * @private
 */
class WhereParser {
	private position = 0;

	constructor(private readonly tokens: string[]) {}

	parse(): WhereNode {
		if (this.tokens.length === 0) {
			return { type: 'always-true' };
		}
		return this.parseOr();
	}

	private peek(): string | undefined {
		return this.tokens[this.position];
	}

	private take(): string | undefined {
		return this.tokens[this.position++];
	}

	private isKeyword(token: string | undefined, keyword: string): boolean {
		return typeof token === 'string' && token.toUpperCase() === keyword;
	}

	private parseOr(): WhereNode {
		let left = this.parseAnd();
		while (this.isKeyword(this.peek(), 'OR')) {
			this.take();
			const right = this.parseAnd();
			left = left.type === 'or' ? { type: 'or', children: [...left.children, right] } : { type: 'or', children: [left, right] };
		}
		return left;
	}

	private parseAnd(): WhereNode {
		let left = this.parseTerm();
		while (this.isKeyword(this.peek(), 'AND')) {
			this.take();
			const right = this.parseTerm();
			left = left.type === 'and' ? { type: 'and', children: [...left.children, right] } : { type: 'and', children: [left, right] };
		}
		return left;
	}

	private parseTerm(): WhereNode {
		const next = this.peek();
		if (next === '(') {
			this.take();
			const expression = this.parseOr();
			if (this.peek() === ')') {
				this.take();
			}
			return expression;
		}
		return this.parseComparison();
	}

	private parseComparison(): WhereNode {
		const columnToken = this.take();
		if (columnToken === undefined) {
			return { type: 'always-true' };
		}

		const column = normalizeSqlIdentifier(columnToken);
		const operatorToken = this.take();
		if (operatorToken === undefined) {
			return { type: 'always-true' };
		}

		const operatorUpper = operatorToken.toUpperCase();
		if (operatorUpper === 'IS') {
			const maybeNot = this.peek();
			if (this.isKeyword(maybeNot, 'NOT')) {
				this.take();
				const nullToken = this.take();
				if (this.isKeyword(nullToken, 'NULL')) {
					return { type: 'null-check', column, operator: 'IS NOT NULL' };
				}
			}
			const nullToken = this.take();
			if (this.isKeyword(nullToken, 'NULL')) {
				return { type: 'null-check', column, operator: 'IS NULL' };
			}
			return { type: 'always-true' };
		}

		let comparisonOperator: ComparisonOperator | 'LIKE' | 'NOT LIKE' | null = null;
		if (operatorUpper === 'LIKE') {
			comparisonOperator = 'LIKE';
		} else if (operatorUpper === 'NOT') {
			const maybeLike = this.take();
			if (this.isKeyword(maybeLike, 'LIKE')) {
				comparisonOperator = 'NOT LIKE';
			} else {
				return { type: 'always-true' };
			}
		} else if (COMPARISON_OPERATORS.includes(operatorUpper as ComparisonOperator)) {
			comparisonOperator = operatorUpper as ComparisonOperator;
		}

		if (!comparisonOperator) {
			return { type: 'always-true' };
		}

		const valueToken = this.take();
		if (valueToken === '?') {
			return { type: 'compare', column, operator: comparisonOperator, value: { kind: 'param' } };
		}
		const literal = parseLiteral(valueToken ?? '');
		return {
			type: 'compare',
			column,
			operator: comparisonOperator,
			value: { kind: 'literal', value: literal.consumed ? literal.value : (valueToken ?? null) }
		};
	}
}

/**
 * Walks a {@link WhereNode} tree against a record, consuming `?` bindings in
 * left-to-right order from `bindings[paramCursor]`.
 * @private
 */
function evaluateWhere(node: WhereNode, record: Record<string, unknown>, bindings: unknown[], paramCursor: { value: number }): boolean {
	switch (node.type) {
		case 'always-true':
			return true;
		case 'and':
			return node.children.every((child) => evaluateWhere(child, record, bindings, paramCursor));
		case 'or': {
			// All `?` parameters in an OR expression still consume bindings even
			// when short-circuited, otherwise downstream clauses pick up the wrong
			// binding indices.
			let matched = false;
			for (const child of node.children) {
				if (evaluateWhere(child, record, bindings, paramCursor)) {
					matched = true;
				}
			}
			return matched;
		}
		case 'null-check': {
			const recordValue = record[node.column];
			const isNull = recordValue === null || recordValue === undefined;
			return node.operator === 'IS NULL' ? isNull : !isNull;
		}
		case 'compare': {
			let value: unknown;
			if (node.value.kind === 'param') {
				value = bindings[paramCursor.value++];
			} else {
				value = node.value.value;
			}
			const recordValue = record[node.column];

			if (node.operator === 'LIKE' || node.operator === 'NOT LIKE') {
				const pattern = String(value ?? '');
				const regex = new RegExp(`^${escapeRegExp(pattern).replace(/%/g, '.*').replace(/_/g, '.')}$`, 'i');
				const matches = regex.test(String(recordValue ?? ''));
				return node.operator === 'LIKE' ? matches : !matches;
			}

			const leftNumber = toFiniteNumber(recordValue);
			const rightNumber = toFiniteNumber(value);

			let comparison: number;
			if (leftNumber !== undefined && rightNumber !== undefined) {
				comparison = leftNumber === rightNumber ? 0 : leftNumber < rightNumber ? -1 : 1;
			} else if (recordValue === null || recordValue === undefined) {
				if (value === null || value === undefined) {
					comparison = 0;
				} else {
					comparison = -1;
				}
			} else if (value === null || value === undefined) {
				comparison = 1;
			} else {
				const leftStr = String(recordValue);
				const rightStr = String(value);
				comparison = leftStr === rightStr ? 0 : leftStr < rightStr ? -1 : 1;
			}

			switch (node.operator) {
				case '=':
					return comparison === 0;
				case '!=':
				case '<>':
					return comparison !== 0;
				case '<':
					return comparison < 0;
				case '<=':
					return comparison <= 0;
				case '>':
					return comparison > 0;
				case '>=':
					return comparison >= 0;
				default:
					return false;
			}
		}
	}
}

/**
 * Reports the number of `?` placeholders that appear in `text`, respecting
 * quoted strings.
 * @private
 */
function countQuestionPlaceholders(text: string): number {
	let count = 0;
	let inSingle = false;
	let inDouble = false;
	for (let i = 0; i < text.length; i++) {
		const char = text[i]!;
		if (char === "'" && !inDouble) {
			inSingle = !inSingle;
			continue;
		}
		if (char === '"' && !inSingle) {
			inDouble = !inDouble;
			continue;
		}
		if (!inSingle && !inDouble && char === '?') {
			count++;
		}
	}
	return count;
}

/**
 * Lightweight detector for the SQL statement kind. Used to pick the matching
 * handler in {@link InMemorySQLDatabase.executeStatement}.
 * @private
 */
function detectStatementType(
	sql: string
): 'select' | 'insert' | 'update' | 'delete' | 'create-table' | 'drop-table' | 'pragma' | 'sqlite-master-select' | 'other' {
	const trimmed = sql.trim().toUpperCase();
	if (trimmed.startsWith('SELECT') || trimmed.startsWith('WITH')) {
		if (trimmed.includes('SQLITE_MASTER')) {
			return 'sqlite-master-select';
		}
		return 'select';
	}
	if (trimmed.startsWith('INSERT')) return 'insert';
	if (trimmed.startsWith('UPDATE')) return 'update';
	if (trimmed.startsWith('DELETE')) return 'delete';
	if (trimmed.startsWith('CREATE TABLE')) return 'create-table';
	if (trimmed.startsWith('DROP TABLE')) return 'drop-table';
	if (trimmed.startsWith('PRAGMA')) return 'pragma';
	return 'other';
}

/**
 * Extracts a table name when one is referenced by an `INSERT INTO`, `UPDATE`,
 * `DELETE FROM`, `SELECT ... FROM`, or `CREATE TABLE` clause.
 * @private
 */
function extractTableName(sql: string): string | null {
	const upper = sql.toUpperCase();

	// CREATE TABLE [IF NOT EXISTS] <name>
	const createMatch = sql.match(/create\s+table\s+(?:if\s+not\s+exists\s+)?([`"\[]?[\w.]+[`"\]]?)/i);
	if (createMatch) {
		return normalizeSqlIdentifier(createMatch[1] ?? '');
	}

	// DROP TABLE [IF EXISTS] <name>
	const dropMatch = sql.match(/drop\s+table\s+(?:if\s+exists\s+)?([`"\[]?[\w.]+[`"\]]?)/i);
	if (dropMatch) {
		return normalizeSqlIdentifier(dropMatch[1] ?? '');
	}

	// INSERT [OR REPLACE|IGNORE|...] INTO <name>
	const insertMatch = sql.match(/insert\s+(?:or\s+\w+\s+)?into\s+([`"\[]?[\w.]+[`"\]]?)/i);
	if (insertMatch) {
		return normalizeSqlIdentifier(insertMatch[1] ?? '');
	}

	// UPDATE [OR ...] <name>
	const updateMatch = sql.match(/update\s+(?:or\s+\w+\s+)?([`"\[]?[\w.]+[`"\]]?)\s+set/i);
	if (updateMatch) {
		return normalizeSqlIdentifier(updateMatch[1] ?? '');
	}

	// DELETE FROM <name>
	const deleteMatch = sql.match(/delete\s+from\s+([`"\[]?[\w.]+[`"\]]?)/i);
	if (deleteMatch) {
		return normalizeSqlIdentifier(deleteMatch[1] ?? '');
	}

	// SELECT ... FROM <name>
	const fromIdx = findKeyword(upper, sql, 'FROM');
	if (fromIdx !== -1) {
		const after = sql.slice(fromIdx + 'FROM'.length).trim();
		const match = after.match(/^([`"\[]?[\w.]+[`"\]]?)/);
		if (match) {
			return normalizeSqlIdentifier(match[1] ?? '');
		}
	}

	return null;
}

/**
 * Normalizes Drizzle-style query payloads into plain `(sql, bindings)`.
 * @private
 */
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
 * Internal projection descriptor produced by {@link parseSelectColumns}.
 * @private
 */
interface SelectColumn {
	expression: string;
	alias: string;
	kind: 'star' | 'column' | 'aggregate' | 'scalar' | 'expression';
}

/**
 * Splits the body of a SELECT projection into descriptive fragments. Each
 * fragment carries the original expression text, the resolved output alias,
 * and a coarse "kind" so {@link evaluateSelectColumn} can decide whether to
 * materialize an aggregate or simply project a column.
 * @private
 */
function parseSelectColumns(selectBody: string): SelectColumn[] {
	const fragments = splitTopLevel(selectBody, ',');
	return fragments.map((fragment) => {
		const aliasMatch = fragment.match(/\s+as\s+([`"\[]?[\w.]+[`"\]]?)\s*$/i);
		const expression = aliasMatch ? fragment.slice(0, aliasMatch.index).trim() : fragment.trim();
		const alias = aliasMatch ? normalizeSqlIdentifier(aliasMatch[1] ?? '') : deriveDefaultAlias(expression);

		if (expression === '*') {
			return { expression, alias: '*', kind: 'star' };
		}

		const upperExpression = expression.toUpperCase();
		const funcMatch = expression.match(/^([A-Za-z_][A-Za-z0-9_]*)\s*\(/);
		if (funcMatch) {
			const fnName = (funcMatch[1] ?? '').toUpperCase();
			if (AGGREGATE_FUNCTIONS.has(fnName) || /\b(?:COUNT|MAX|MIN|SUM|AVG)\s*\(/i.test(upperExpression)) {
				return { expression, alias, kind: 'aggregate' };
			}
			if (SUPPORTED_SCALARS.has(fnName)) {
				return { expression, alias, kind: 'scalar' };
			}
		}

		if (/[+\-*/()]/.test(expression) || /\bCOUNT\s*\(/i.test(expression) || /\bMAX\s*\(/i.test(expression)) {
			return { expression, alias, kind: 'expression' };
		}

		return { expression, alias, kind: 'column' };
	});
}

/**
 * Picks a deterministic alias for a SELECT expression when the caller didn't
 * explicitly assign one with `AS`.
 * @private
 */
function deriveDefaultAlias(expression: string): string {
	const trimmed = expression.trim();
	if (trimmed === '*') return '*';
	const match = trimmed.match(/^([`"\[]?[\w.]+[`"\]]?)$/);
	if (match) {
		return normalizeSqlIdentifier(trimmed);
	}
	return trimmed;
}

/**
 * Replaces the next sequence of `?` placeholders inside an expression with the
 * corresponding bindings so the resulting text can be evaluated by
 * {@link evaluateScalarExpression} as a literal expression. Returns the
 * updated expression and the new binding cursor index.
 * @private
 */
function substituteBindings(expression: string, bindings: unknown[], cursor: number): { expression: string; cursor: number } {
	let output = '';
	let inSingle = false;
	let inDouble = false;
	let i = 0;
	let localCursor = cursor;
	while (i < expression.length) {
		const char = expression[i]!;
		if (char === "'" && !inDouble) {
			inSingle = !inSingle;
			output += char;
			i++;
			continue;
		}
		if (char === '"' && !inSingle) {
			inDouble = !inDouble;
			output += char;
			i++;
			continue;
		}
		if (!inSingle && !inDouble && char === '?') {
			const value = bindings[localCursor++];
			output += literalToSql(value);
			i++;
			continue;
		}
		output += char;
		i++;
	}
	return { expression: output, cursor: localCursor };
}

/**
 * Encodes a JS value as a SQL literal suitable for in-place substitution.
 * @private
 */
function literalToSql(value: unknown): string {
	if (value === null || value === undefined) return 'NULL';
	if (typeof value === 'number' || typeof value === 'bigint') return String(value);
	if (typeof value === 'boolean') return value ? '1' : '0';
	const stringValue = String(value).replace(/'/g, "''");
	return `'${stringValue}'`;
}

/**
 * Evaluates a single SELECT expression against the rows that survived
 * filtering. Aggregates are computed across `rows`; non-aggregate expressions
 * are evaluated against the first row (or returned as `null` when empty).
 * @private
 */
function evaluateSelectColumn(column: SelectColumn, rows: Record<string, unknown>[], bindings: unknown[], cursor: number): unknown {
	if (column.kind === 'star') {
		return undefined;
	}

	if (column.kind === 'column') {
		const first = rows[0];
		return first ? first[column.expression] : null;
	}

	const substituted = substituteBindings(column.expression, bindings, cursor).expression;

	if (column.kind === 'aggregate' || /\b(?:COUNT|MAX|MIN|SUM|AVG)\s*\(/i.test(substituted)) {
		return evaluateAggregateExpression(substituted, rows);
	}

	if (column.kind === 'scalar') {
		const first = rows[0] ?? {};
		return evaluateScalarExpression(substituted, first);
	}

	const first = rows[0] ?? {};
	return evaluateScalarExpression(substituted, first);
}

/**
 * Replaces every aggregate call inside `expression` with its evaluated value
 * (computed across `rows`), then defers any remaining expression evaluation —
 * which at this point should only involve numbers and operators — to
 * {@link evaluateScalarExpression}.
 * @private
 */
function evaluateAggregateExpression(expression: string, rows: Record<string, unknown>[]): unknown {
	let working = expression;
	const aggregatePattern = /(COUNT|MAX|MIN|SUM|AVG)\s*\(/i;
	let safety = 0;

	while (aggregatePattern.test(working) && safety < 25) {
		safety++;
		const match = working.match(aggregatePattern);
		if (!match || match.index === undefined) break;
		const openParenIndex = working.indexOf('(', match.index);
		if (openParenIndex === -1) break;
		const closeParenIndex = findMatchingParen(working, openParenIndex);
		if (closeParenIndex === -1) break;

		const fnName = (match[1] ?? '').toUpperCase();
		const inner = working.slice(openParenIndex + 1, closeParenIndex).trim();
		const value = computeAggregate(fnName, inner, rows);
		const replacement = literalToSql(value);
		working = working.slice(0, match.index) + replacement + working.slice(closeParenIndex + 1);
	}

	return evaluateScalarExpression(working, rows[0] ?? {});
}

/**
 * Computes a single aggregate value (COUNT/MAX/MIN/SUM/AVG) for the provided rows.
 * @private
 */
function computeAggregate(fnName: string, inner: string, rows: Record<string, unknown>[]): unknown {
	if (fnName === 'COUNT') {
		if (inner === '*') {
			return rows.length;
		}
		const isDistinct = /^distinct\s+/i.test(inner);
		const column = normalizeSqlIdentifier(inner.replace(/^distinct\s+/i, ''));
		const values = rows.map((row) => row[column]).filter((value) => value !== null && value !== undefined);
		if (isDistinct) {
			return new Set(values.map((value) => String(value))).size;
		}
		return values.length;
	}

	const column = normalizeSqlIdentifier(inner);
	const numericValues = rows.map((row) => toFiniteNumber(row[column])).filter((value): value is number => value !== undefined);

	if (numericValues.length === 0) {
		return null;
	}

	switch (fnName) {
		case 'MAX':
			return Math.max(...numericValues);
		case 'MIN':
			return Math.min(...numericValues);
		case 'SUM':
			return numericValues.reduce((sum, value) => sum + value, 0);
		case 'AVG':
			return numericValues.reduce((sum, value) => sum + value, 0) / numericValues.length;
		default:
			return null;
	}
}

/**
 * Tiny expression evaluator for the residual scalar forms left after
 * aggregates have been substituted. Supports COALESCE/IFNULL, LOWER/UPPER,
 * LENGTH, ABS, and arithmetic / comparison via {@link safelyEvaluate}.
 * @private
 */
function evaluateScalarExpression(expression: string, record: Record<string, unknown>): unknown {
	const trimmed = expression.trim();
	if (trimmed.length === 0) return null;

	const literal = parseLiteral(trimmed);
	if (literal.consumed) {
		return literal.value;
	}

	const scalarMatch = trimmed.match(/^([A-Za-z_][A-Za-z0-9_]*)\s*\(/);
	if (scalarMatch) {
		const fnName = (scalarMatch[1] ?? '').toUpperCase();
		const openParenIndex = trimmed.indexOf('(', scalarMatch.index);
		const closeParenIndex = findMatchingParen(trimmed, openParenIndex);
		if (closeParenIndex !== -1) {
			const inner = trimmed.slice(openParenIndex + 1, closeParenIndex);
			const args = splitTopLevel(inner, ',').map((arg) => evaluateScalarExpression(arg, record));
			const suffix = trimmed.slice(closeParenIndex + 1).trim();
			const value = applyScalarFunction(fnName, args);
			if (suffix.length === 0) {
				return value;
			}
			return safelyEvaluate(`${literalToSql(value)} ${suffix}`, record);
		}
	}

	const identifierMatch = trimmed.match(/^([`"\[]?[\w.]+[`"\]]?)$/);
	if (identifierMatch) {
		return record[normalizeSqlIdentifier(trimmed)] ?? null;
	}

	return safelyEvaluate(trimmed, record);
}

/**
 * Applies a non-aggregate scalar function to its evaluated argument list.
 * @private
 */
function applyScalarFunction(fnName: string, args: unknown[]): unknown {
	switch (fnName) {
		case 'COALESCE':
		case 'IFNULL': {
			for (const arg of args) {
				if (arg !== null && arg !== undefined) return arg;
			}
			return null;
		}
		case 'LOWER':
			return args[0] === null || args[0] === undefined ? null : String(args[0]).toLowerCase();
		case 'UPPER':
			return args[0] === null || args[0] === undefined ? null : String(args[0]).toUpperCase();
		case 'LENGTH':
			return args[0] === null || args[0] === undefined ? null : String(args[0]).length;
		case 'ABS': {
			const numeric = toFiniteNumber(args[0]);
			return numeric === undefined ? null : Math.abs(numeric);
		}
		default:
			return null;
	}
}

/**
 * Computes a constrained subset of scalar expressions — numeric arithmetic
 * over column references and constants. Anything outside that subset is
 * returned verbatim so the caller can decide whether the result is useful.
 *
 * The arithmetic path uses a token-based recursive-descent evaluator rather
 * than `new Function(...)` so the in-memory provider remains usable inside
 * Cloudflare Workers isolates, which disallow dynamic code evaluation.
 * @private
 */
function safelyEvaluate(expression: string, record: Record<string, unknown>): unknown {
	const trimmed = expression.trim();
	if (trimmed.length === 0) return null;

	const literal = parseLiteral(trimmed);
	if (literal.consumed) return literal.value;

	const numeric = evaluateNumericExpression(trimmed, record);
	return numeric === undefined ? trimmed : numeric;
}

/**
 * Numeric expression token used by the in-memory arithmetic evaluator.
 * @private
 */
type NumericToken =
	| { type: 'number'; value: number }
	| { type: 'identifier'; name: string }
	| { type: 'null' }
	| { type: 'lparen' }
	| { type: 'rparen' }
	| { type: 'plus' }
	| { type: 'minus' }
	| { type: 'star' }
	| { type: 'slash' };

/**
 * Tokenizes a numeric expression like `max_id + 1` or `(5 + 3) * 2`. Returns
 * `undefined` if any character falls outside the supported arithmetic subset
 * so the caller can bail out without attempting a parse.
 * @private
 */
function tokenizeNumericExpression(expression: string): NumericToken[] | undefined {
	const tokens: NumericToken[] = [];
	let i = 0;
	while (i < expression.length) {
		const char = expression[i]!;
		if (/\s/.test(char)) {
			i++;
			continue;
		}
		if (char === '(') {
			tokens.push({ type: 'lparen' });
			i++;
			continue;
		}
		if (char === ')') {
			tokens.push({ type: 'rparen' });
			i++;
			continue;
		}
		if (char === '+') {
			tokens.push({ type: 'plus' });
			i++;
			continue;
		}
		if (char === '-') {
			tokens.push({ type: 'minus' });
			i++;
			continue;
		}
		if (char === '*') {
			tokens.push({ type: 'star' });
			i++;
			continue;
		}
		if (char === '/') {
			tokens.push({ type: 'slash' });
			i++;
			continue;
		}
		if (/[0-9.]/.test(char)) {
			let end = i;
			while (end < expression.length && /[0-9.]/.test(expression[end]!)) end++;
			const value = Number(expression.slice(i, end));
			if (!Number.isFinite(value)) return undefined;
			tokens.push({ type: 'number', value });
			i = end;
			continue;
		}
		if (/[A-Za-z_`"\[]/.test(char)) {
			let end = i;
			while (end < expression.length && /[A-Za-z0-9_`"\[\].]/.test(expression[end]!)) end++;
			const name = expression.slice(i, end);
			if (/^null$/i.test(name)) {
				tokens.push({ type: 'null' });
			} else {
				tokens.push({ type: 'identifier', name });
			}
			i = end;
			continue;
		}
		return undefined;
	}
	return tokens;
}

/**
 * Cursor state passed through the recursive-descent numeric parser.
 * @private
 */
interface NumericParser {
	tokens: NumericToken[];
	position: number;
}

/**
 * Evaluates `<literal-or-column> (+|-|*|/) <literal-or-column>` style
 * expressions, with parentheses and unary +/-. Returns `undefined` when the
 * expression contains anything outside that subset — including string column
 * values, comparison operators, or unsupported syntax — so the caller can
 * fall back to returning the raw expression.
 * @private
 */
function evaluateNumericExpression(expression: string, record: Record<string, unknown>): number | undefined {
	const tokens = tokenizeNumericExpression(expression);
	if (!tokens || tokens.length === 0) return undefined;

	const parser: NumericParser = { tokens, position: 0 };
	const result = parseAdditive(parser, record);
	if (result === undefined) return undefined;
	if (parser.position !== tokens.length) return undefined;
	return Number.isFinite(result) ? result : undefined;
}

function parseAdditive(parser: NumericParser, record: Record<string, unknown>): number | undefined {
	let left = parseMultiplicative(parser, record);
	if (left === undefined) return undefined;
	while (parser.position < parser.tokens.length) {
		const next = parser.tokens[parser.position]!;
		if (next.type !== 'plus' && next.type !== 'minus') break;
		parser.position++;
		const right = parseMultiplicative(parser, record);
		if (right === undefined) return undefined;
		left = next.type === 'plus' ? left + right : left - right;
	}
	return left;
}

function parseMultiplicative(parser: NumericParser, record: Record<string, unknown>): number | undefined {
	let left = parseUnary(parser, record);
	if (left === undefined) return undefined;
	while (parser.position < parser.tokens.length) {
		const next = parser.tokens[parser.position]!;
		if (next.type !== 'star' && next.type !== 'slash') break;
		parser.position++;
		const right = parseUnary(parser, record);
		if (right === undefined) return undefined;
		if (next.type === 'slash' && right === 0) return undefined;
		left = next.type === 'star' ? left * right : left / right;
	}
	return left;
}

function parseUnary(parser: NumericParser, record: Record<string, unknown>): number | undefined {
	const next = parser.tokens[parser.position];
	if (!next) return undefined;
	if (next.type === 'plus') {
		parser.position++;
		return parseUnary(parser, record);
	}
	if (next.type === 'minus') {
		parser.position++;
		const value = parseUnary(parser, record);
		return value === undefined ? undefined : -value;
	}
	return parsePrimary(parser, record);
}

function parsePrimary(parser: NumericParser, record: Record<string, unknown>): number | undefined {
	const token = parser.tokens[parser.position++];
	if (!token) return undefined;
	if (token.type === 'number') return token.value;
	if (token.type === 'null') return 0;
	if (token.type === 'identifier') {
		const value = record[normalizeSqlIdentifier(token.name)];
		if (value === null || value === undefined) return 0;
		return toFiniteNumber(value);
	}
	if (token.type === 'lparen') {
		const inner = parseAdditive(parser, record);
		if (inner === undefined) return undefined;
		const close = parser.tokens[parser.position++];
		if (!close || close.type !== 'rparen') return undefined;
		return inner;
	}
	return undefined;
}

/**
 * In-memory SQL backend used by tests and local sandboxes.
 *
 * Implements the {@link SQLDatabase} contract with a deliberately limited
 * subset of SQLite-flavored SQL. See the file-level docstring for the exact
 * features supported.
 */
export class InMemorySQLDatabase implements SQLDatabase {
	private tables = new Map<string, Map<string, Record<string, unknown>>>();
	private schemas = new Map<string, Map<string, ColumnInfo>>();
	private autoIncrementCounters = new Map<string, number>();
	private lastInsertRowId: number | string | null = null;

	prepare(sql: string): PreparedStatement {
		return new InMemoryPreparedStatement(this, sql);
	}

	async execute(query: string | InMemoryDrizzleQuery, bindings: any[] = []): Promise<QueryResult<Record<string, unknown>>> {
		return await this.executeClientQuery(query, bindings);
	}

	async run(query: string | InMemoryDrizzleQuery, bindings: any[] = []): Promise<QueryResult<Record<string, unknown>>> {
		return await this.executeClientQuery(query, bindings);
	}

	async all(query: string | InMemoryDrizzleQuery, bindings: any[] = []): Promise<QueryResult<Record<string, unknown>>> {
		return await this.executeClientQuery(query, bindings);
	}

	async get(query: string | InMemoryDrizzleQuery, bindings: any[] = []): Promise<QueryResult<Record<string, unknown>>> {
		return await this.executeClientQuery(query, bindings);
	}

	/**
	 * Routes a Drizzle-style query payload through the SQL emulator.
	 * @private
	 */
	private async executeClientQuery(query: string | InMemoryDrizzleQuery, bindings: any[]): Promise<QueryResult<Record<string, unknown>>> {
		const normalized = normalizeInMemoryDrizzleQuery(query, bindings);
		return await this.executeStatement(normalized.sql, normalized.bindings);
	}

	/**
	 * Dispatches to the correct handler based on the SQL statement kind.
	 * Public so the prepared-statement wrapper can target it directly.
	 */
	async executeStatement(sql: string, bindings: any[]): Promise<QueryResult<Record<string, unknown>>> {
		const startTime = Date.now();
		try {
			switch (detectStatementType(sql)) {
				case 'create-table':
					return await this.handleCreateTable(sql);
				case 'drop-table':
					return await this.handleDropTable(sql);
				case 'insert':
					return await this.handleInsert(sql, bindings);
				case 'update':
					return await this.handleUpdate(sql, bindings);
				case 'delete':
					return await this.handleDelete(sql, bindings);
				case 'select':
					return await this.handleSelect(sql, bindings);
				case 'sqlite-master-select':
					return await this.handleSqliteMasterSelect(sql, bindings);
				case 'pragma':
					return await this.handlePragma(sql);
				case 'other':
				default:
					// No-op success for unsupported statements (CREATE INDEX, ATTACH, etc.).
					return {
						success: true,
						results: [],
						meta: { duration: Date.now() - startTime }
					};
			}
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
	 * Convenience wrapper kept for backwards compatibility. Routes SELECT
	 * statements through the emulator and rejects other statement kinds.
	 */
	async executeQuery(sql: string, bindings: any[]): Promise<QueryResult<Record<string, unknown>>> {
		const startTime = Date.now();
		const kind = detectStatementType(sql);
		if (kind === 'select' || kind === 'sqlite-master-select' || kind === 'pragma') {
			return await this.executeStatement(sql, bindings);
		}
		return {
			success: false,
			results: [],
			meta: { duration: Date.now() - startTime },
			error: 'Use executeStatement for non-SELECT queries'
		};
	}

	private async handleCreateTable(sql: string): Promise<QueryResult> {
		const startTime = Date.now();
		const tableName = extractTableName(sql);

		if (!tableName) {
			return {
				success: false,
				results: [],
				meta: { duration: Date.now() - startTime },
				error: 'Could not extract table name'
			};
		}

		const schema = new Map<string, ColumnInfo>();
		const openParen = sql.indexOf('(');
		const closeParen = openParen === -1 ? -1 : findMatchingParen(sql, openParen);

		if (openParen !== -1 && closeParen !== -1) {
			const definitionBody = sql.slice(openParen + 1, closeParen);
			for (const rawColumn of splitTopLevel(definitionBody, ',')) {
				const tokens = rawColumn.trim().split(/\s+/);
				if (tokens.length === 0) continue;
				const firstUpper = tokens[0]?.toUpperCase() ?? '';
				// Skip table-level constraints such as PRIMARY KEY (...), FOREIGN KEY, CHECK, UNIQUE.
				if (['PRIMARY', 'FOREIGN', 'UNIQUE', 'CHECK', 'CONSTRAINT'].includes(firstUpper)) {
					continue;
				}
				const colName = normalizeSqlIdentifier(tokens[0] ?? '');
				if (colName.length === 0) continue;
				const colType = (tokens[1] ?? '').toUpperCase();
				const modifiers = tokens.slice(2).join(' ').toUpperCase();

				schema.set(colName, {
					name: colName,
					type: colType,
					isPrimaryKey: modifiers.includes('PRIMARY KEY'),
					isAutoIncrement:
						modifiers.includes('AUTOINCREMENT') ||
						modifiers.includes('AUTO_INCREMENT') ||
						modifiers.includes('GENERATED BY DEFAULT AS IDENTITY'),
					defaultValue: extractDefaultValue(tokens.slice(2).join(' ')),
					notNull: modifiers.includes('NOT NULL')
				});
			}
		}

		this.tables.set(tableName, new Map());
		this.schemas.set(tableName, schema);

		for (const column of schema.values()) {
			if (column.isAutoIncrement) {
				this.autoIncrementCounters.set(tableName, this.autoIncrementCounters.get(tableName) ?? 0);
				break;
			}
		}

		return {
			success: true,
			results: [],
			meta: { duration: Date.now() - startTime }
		};
	}

	private async handleDropTable(sql: string): Promise<QueryResult> {
		const startTime = Date.now();
		const tableName = extractTableName(sql);

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
		const tableName = extractTableName(sql);

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

		const explicitColumns = extractInsertColumns(sql);
		const isOrReplace = /\binsert\s+or\s+replace\b/i.test(sql);
		const isOrIgnore = /\binsert\s+or\s+(?:ignore|abort|fail|rollback)\b/i.test(sql);

		const valuesSegments = extractInsertValues(sql);
		const insertedRows: Record<string, unknown>[] = [];

		// When values appear as a VALUES (...) list, evaluate each tuple. When the
		// caller omits VALUES (e.g. INSERT INTO ... SELECT ...) we fall back to
		// the binding-order behavior used by older versions of the emulator.
		const tuples = valuesSegments.length > 0 ? valuesSegments : [null];
		let bindingCursor = 0;

		for (const tuple of tuples) {
			const record: Record<string, unknown> = {};

			if (explicitColumns && explicitColumns.length > 0) {
				const tupleExpressions = tuple ? splitTopLevel(tuple, ',').map((expression) => expression.trim()) : explicitColumns.map(() => '?');

				explicitColumns.forEach((columnName, index) => {
					const expr = tupleExpressions[index];
					if (expr === undefined) return;
					if (expr === '?') {
						record[columnName] = bindings[bindingCursor++];
					} else {
						const literal = parseLiteral(expr);
						record[columnName] = literal.consumed ? literal.value : expr;
					}
				});
			} else if (schema) {
				// Without an explicit column list, fall back to schema order — the
				// historical default. Bindings are consumed in declaration order.
				for (const column of schema.keys()) {
					if (bindingCursor < bindings.length) {
						record[column] = bindings[bindingCursor++];
					}
				}
			} else {
				for (const value of bindings.slice(bindingCursor)) {
					record[Object.keys(record).length === 0 ? 'id' : `column_${Object.keys(record).length}`] = value;
					bindingCursor++;
				}
			}

			// Apply column defaults for omitted columns.
			if (schema) {
				for (const column of schema.values()) {
					if (record[column.name] === undefined && column.defaultValue !== undefined) {
						record[column.name] = column.defaultValue;
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
				// No primary key declared. Fall back to synthesized rowids so
				// downstream operations still find the inserted row.
				const nextValue = (this.autoIncrementCounters.get(tableName) ?? 0) + 1;
				this.autoIncrementCounters.set(tableName, nextValue);
				primaryKeyValue = nextValue;
				record[primaryKeyColumn] = nextValue;
			}

			if (autoIncrementInfo) {
				const numericPrimaryKey = typeof primaryKeyValue === 'number' ? primaryKeyValue : Number(primaryKeyValue);
				if (Number.isFinite(numericPrimaryKey)) {
					this.autoIncrementCounters.set(tableName, Math.max(this.autoIncrementCounters.get(tableName) ?? 0, numericPrimaryKey));
				}
			}

			const key = String(primaryKeyValue);
			const existed = table.has(key);

			if (existed && !isOrReplace) {
				if (isOrIgnore) {
					continue;
				}
				return {
					success: false,
					results: [],
					meta: { duration: Date.now() - startTime },
					error: `UNIQUE constraint failed: ${tableName}.${primaryKeyColumn}`
				};
			}

			table.set(key, record);
			this.lastInsertRowId =
				typeof primaryKeyValue === 'number' || typeof primaryKeyValue === 'string' ? primaryKeyValue : String(primaryKeyValue);
			insertedRows.push(record);
		}

		const hasReturning = /\breturning\b/i.test(sql);
		const returningColumns = hasReturning ? extractReturningColumns(sql) : null;
		const results = hasReturning ? insertedRows.map((row) => projectReturningRow(row, returningColumns ?? ['*'])) : [];

		return {
			success: true,
			results,
			meta: {
				duration: Date.now() - startTime,
				last_row_id: this.lastInsertRowId ?? undefined,
				changes: insertedRows.length
			}
		};
	}

	private async handleUpdate(sql: string, bindings: any[]): Promise<QueryResult> {
		const startTime = Date.now();
		const tableName = extractTableName(sql);

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

		const setBody = extractSetBody(sql);
		if (!setBody) {
			return {
				success: false,
				results: [],
				meta: { duration: Date.now() - startTime },
				error: 'UPDATE missing SET clause'
			};
		}

		const assignments = parseSetAssignments(setBody);

		const table = this.tables.get(tableName)!;
		const records = Array.from(table.values());

		// Bindings are consumed first by the SET expressions (in order), then by
		// the WHERE expression. Aggregate this layout once so we can re-evaluate
		// WHERE per row without altering the binding cursor for later iterations.
		const setPlaceholderCount = countQuestionPlaceholders(setBody);
		const whereBindings = bindings.slice(setPlaceholderCount);
		const setBindings = bindings.slice(0, setPlaceholderCount);

		const { whereClause } = extractWhereSegment(sql);
		const whereNode = whereClause ? new WhereParser(tokenizeWhere(whereClause)).parse() : ({ type: 'always-true' } as WhereNode);

		const matched: Record<string, unknown>[] = [];
		for (const record of records) {
			const cursor = { value: 0 };
			if (evaluateWhere(whereNode, record, whereBindings, cursor)) {
				matched.push(record);
			}
		}

		let changes = 0;
		for (const record of matched) {
			const cursor = { value: 0 };
			for (const { column, expression } of assignments) {
				const placeholderCount = countQuestionPlaceholders(expression);
				const slice = setBindings.slice(cursor.value, cursor.value + placeholderCount);
				cursor.value += placeholderCount;
				record[column] = computeAssignmentValue(expression, slice, record);
			}
			changes++;
		}

		const hasReturning = /\breturning\b/i.test(sql);
		const returningColumns = hasReturning ? extractReturningColumns(sql) : null;
		const results = hasReturning ? matched.map((row) => projectReturningRow(row, returningColumns ?? ['*'])) : [];

		return {
			success: true,
			results,
			meta: { duration: Date.now() - startTime, changes }
		};
	}

	private async handleDelete(sql: string, bindings: any[]): Promise<QueryResult> {
		const startTime = Date.now();
		const tableName = extractTableName(sql);

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

		const { whereClause } = extractWhereSegment(sql);
		if (!whereClause) {
			const removed = table.size;
			table.clear();
			return {
				success: true,
				results: [],
				meta: { duration: Date.now() - startTime, changes: removed }
			};
		}

		const whereNode = new WhereParser(tokenizeWhere(whereClause)).parse();
		const removedRows: Record<string, unknown>[] = [];
		for (const [key, record] of Array.from(table.entries())) {
			const cursor = { value: 0 };
			if (evaluateWhere(whereNode, record, bindings, cursor)) {
				table.delete(key);
				removedRows.push(record);
			}
		}

		const hasReturning = /\breturning\b/i.test(sql);
		const returningColumns = hasReturning ? extractReturningColumns(sql) : null;
		const results = hasReturning ? removedRows.map((row) => projectReturningRow(row, returningColumns ?? ['*'])) : [];

		return {
			success: true,
			results,
			meta: { duration: Date.now() - startTime, changes: removedRows.length }
		};
	}

	private async handleSelect(sql: string, bindings: any[]): Promise<QueryResult> {
		const startTime = Date.now();
		const tableName = extractTableName(sql);

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
		const allRows = Array.from(table.values());

		// Filter
		const { whereClause, afterWhere } = extractWhereSegment(sql);
		const whereNode = whereClause ? new WhereParser(tokenizeWhere(whereClause)).parse() : ({ type: 'always-true' } as WhereNode);
		const selectBody = extractSelectBody(sql);
		const selectColumns = parseSelectColumns(selectBody);

		// Bindings are split between SELECT projection (uncommon), WHERE, and
		// LIMIT/OFFSET. The current emulator does not support placeholders in
		// SELECT or LIMIT/OFFSET, so every binding feeds the WHERE evaluator.
		const filtered: Record<string, unknown>[] = [];
		for (const row of allRows) {
			const cursor = { value: 0 };
			if (evaluateWhere(whereNode, row, bindings, cursor)) {
				filtered.push(row);
			}
		}

		// ORDER BY
		const orderBy = extractOrderBy(afterWhere || sql);
		let orderedRows = filtered;
		if (orderBy) {
			orderedRows = [...filtered].sort((left, right) => compareRowsByOrder(left, right, orderBy));
		}

		const isAggregateSelect = selectColumns.some((col) => col.kind === 'aggregate');
		let projectedRows: Record<string, unknown>[];
		if (isAggregateSelect) {
			const aggregateRow: Record<string, unknown> = {};
			for (const column of selectColumns) {
				aggregateRow[column.alias] = evaluateSelectColumn(column, orderedRows, bindings, 0);
			}
			projectedRows = [aggregateRow];
		} else if (selectColumns.length === 1 && selectColumns[0]?.kind === 'star') {
			projectedRows = orderedRows.map((row) => ({ ...row }));
		} else {
			projectedRows = orderedRows.map((row) => {
				const projected: Record<string, unknown> = {};
				for (const column of selectColumns) {
					if (column.kind === 'star') {
						Object.assign(projected, row);
					} else if (column.kind === 'column') {
						projected[column.alias] = row[column.expression] ?? null;
					} else {
						projected[column.alias] = evaluateSelectColumn(column, [row], bindings, 0);
					}
				}
				return projected;
			});
		}

		const { limit, offset } = extractLimitOffset(afterWhere);
		const startIndex = offset ?? 0;
		const endIndex = limit === undefined ? projectedRows.length : startIndex + limit;
		const paginatedRows = projectedRows.slice(startIndex, endIndex);

		return {
			success: true,
			results: paginatedRows,
			meta: { duration: Date.now() - startTime }
		};
	}

	private async handleSqliteMasterSelect(sql: string, bindings: any[]): Promise<QueryResult> {
		const startTime = Date.now();
		const rows: Record<string, unknown>[] = [];
		for (const tableName of this.tables.keys()) {
			rows.push({ name: tableName, type: 'table', tbl_name: tableName, sql: `CREATE TABLE ${tableName} (...)` });
		}

		const { whereClause, afterWhere } = extractWhereSegment(sql);
		const whereNode = whereClause ? new WhereParser(tokenizeWhere(whereClause)).parse() : ({ type: 'always-true' } as WhereNode);

		const filtered = rows.filter((row) => {
			const cursor = { value: 0 };
			return evaluateWhere(whereNode, row, bindings, cursor);
		});

		const selectBody = extractSelectBody(sql);
		const projection = parseSelectColumns(selectBody);
		const projected = filtered.map((row) => {
			if (projection.length === 1 && projection[0]?.kind === 'star') return { ...row };
			const out: Record<string, unknown> = {};
			for (const column of projection) {
				if (column.kind === 'star') {
					Object.assign(out, row);
				} else if (column.kind === 'column') {
					out[column.alias] = row[column.expression] ?? null;
				} else {
					out[column.alias] = evaluateSelectColumn(column, [row], bindings, 0);
				}
			}
			return out;
		});

		const { limit, offset } = extractLimitOffset(afterWhere);
		const start = offset ?? 0;
		const end = limit === undefined ? projected.length : start + limit;

		return {
			success: true,
			results: projected.slice(start, end),
			meta: { duration: Date.now() - startTime }
		};
	}

	private async handlePragma(sql: string): Promise<QueryResult> {
		const startTime = Date.now();
		const lower = sql.trim().toLowerCase();

		if (lower.startsWith('pragma page_count')) {
			// Approximate "page count" using a stable hash of the total row count
			// so size-based shard allocation still has a meaningful signal.
			let totalRows = 0;
			for (const table of this.tables.values()) {
				totalRows += table.size;
			}
			return {
				success: true,
				results: [{ page_count: Math.max(1, totalRows) }],
				meta: { duration: Date.now() - startTime }
			};
		}

		if (lower.startsWith('pragma page_size')) {
			return {
				success: true,
				results: [{ page_size: 4096 }],
				meta: { duration: Date.now() - startTime }
			};
		}

		if (lower.startsWith('pragma table_info')) {
			const match = lower.match(/pragma\s+table_info\s*\(\s*([`"\[]?[\w.]+[`"\]]?)\s*\)/);
			if (match) {
				const tableName = normalizeSqlIdentifier(match[1] ?? '');
				const schema = this.schemas.get(tableName);
				if (schema) {
					const rows = Array.from(schema.values()).map((column, index) => ({
						cid: index,
						name: column.name,
						type: column.type,
						notnull: column.notNull ? 1 : 0,
						dflt_value: column.defaultValue ?? null,
						pk: column.isPrimaryKey ? 1 : 0
					}));
					return {
						success: true,
						results: rows,
						meta: { duration: Date.now() - startTime }
					};
				}
			}
		}

		return {
			success: true,
			results: [],
			meta: { duration: Date.now() - startTime }
		};
	}
}

/**
 * Helper used by `UPDATE ... SET col = <expr>` to compute the assigned value
 * for each row. Pure column references and the special form `col = col + 1`
 * are evaluated against the live row so updates can reference their previous
 * column values.
 * @private
 */
function computeAssignmentValue(expression: string, slice: unknown[], row: Record<string, unknown>): unknown {
	if (expression === '?') {
		return slice[0];
	}

	const substituted = substituteBindings(expression, slice, 0).expression;
	return evaluateScalarExpression(substituted, row);
}

/**
 * Returns the body of a SELECT projection — the substring between `SELECT`
 * (or a leading distinct/all keyword) and the matching `FROM`.
 * @private
 */
function extractSelectBody(sql: string): string {
	const upper = sql.toUpperCase();
	const selectIdx = findKeyword(upper, sql, 'SELECT');
	if (selectIdx === -1) return '*';
	const fromIdx = findKeyword(upper, sql, 'FROM', selectIdx + 'SELECT'.length);
	if (fromIdx === -1) return sql.slice(selectIdx + 'SELECT'.length).trim();
	return sql
		.slice(selectIdx + 'SELECT'.length, fromIdx)
		.trim()
		.replace(/^distinct\s+/i, '');
}

/**
 * Returns the body of a `SET` clause from an UPDATE statement.
 * @private
 */
function extractSetBody(sql: string): string | null {
	const upper = sql.toUpperCase();
	const setIdx = findKeyword(upper, sql, 'SET');
	if (setIdx === -1) return null;

	const startOfSet = setIdx + 'SET'.length;
	const terminators = ['WHERE', 'RETURNING', 'ORDER BY', 'LIMIT'];
	let endIdx = sql.length;
	for (const terminator of terminators) {
		const idx = findKeyword(upper, sql, terminator, startOfSet);
		if (idx !== -1 && idx < endIdx) {
			endIdx = idx;
		}
	}
	return sql.slice(startOfSet, endIdx).trim();
}

/**
 * Returns the body of an ORDER BY clause.
 * @private
 */
interface OrderByClause {
	column: string;
	direction: 'asc' | 'desc';
}

function extractOrderBy(sql: string): OrderByClause | null {
	const upper = sql.toUpperCase();
	const orderIdx = findKeyword(upper, sql, 'ORDER BY');
	if (orderIdx === -1) return null;

	const tail = sql.slice(orderIdx + 'ORDER BY'.length).trim();
	const match = tail.match(/^([`"\[]?[\w.]+[`"\]]?)(?:\s+(asc|desc))?/i);
	if (!match) return null;
	return {
		column: normalizeSqlIdentifier(match[1] ?? ''),
		direction: (match[2] ?? 'asc').toLowerCase() === 'desc' ? 'desc' : 'asc'
	};
}

function compareRowsByOrder(left: Record<string, unknown>, right: Record<string, unknown>, order: OrderByClause): number {
	const leftValue = left[order.column];
	const rightValue = right[order.column];
	if (leftValue === rightValue) return 0;
	if (leftValue === undefined || leftValue === null) {
		return order.direction === 'desc' ? 1 : -1;
	}
	if (rightValue === undefined || rightValue === null) {
		return order.direction === 'desc' ? -1 : 1;
	}
	const leftNumber = toFiniteNumber(leftValue);
	const rightNumber = toFiniteNumber(rightValue);
	let comparison: number;
	if (leftNumber !== undefined && rightNumber !== undefined) {
		comparison = leftNumber === rightNumber ? 0 : leftNumber < rightNumber ? -1 : 1;
	} else {
		comparison = String(leftValue).localeCompare(String(rightValue));
	}
	return order.direction === 'desc' ? -comparison : comparison;
}

/**
 * Extracts each `VALUES (...)` tuple body from an INSERT statement.
 * @private
 */
function extractInsertValues(sql: string): string[] {
	const upper = sql.toUpperCase();
	const valuesIdx = findKeyword(upper, sql, 'VALUES');
	if (valuesIdx === -1) return [];

	const tuples: string[] = [];
	let i = valuesIdx + 'VALUES'.length;
	while (i < sql.length) {
		while (i < sql.length && /[\s,]/.test(sql[i]!)) i++;
		if (sql[i] !== '(') break;
		const close = findMatchingParen(sql, i);
		if (close === -1) break;
		tuples.push(sql.slice(i + 1, close).trim());
		i = close + 1;
	}
	return tuples;
}

/**
 * Extracts the explicit column list from an INSERT statement, when present.
 * Distinguishes between `INSERT INTO t (a, b) VALUES (?, ?)` (column list) and
 * `INSERT INTO t VALUES (?, ?)` (no column list — first parenthesized group is
 * the VALUES tuple) by checking that the matched paren group appears before
 * any `VALUES` / `SELECT` keyword.
 * @private
 */
function extractInsertColumns(sql: string): string[] | null {
	const match = sql.match(/insert\s+(?:or\s+\w+\s+)?into\s+[^()]+\(([^)]+)\)/i);
	if (!match || !match[1] || match.index === undefined) return null;

	const beforeMatch = sql.slice(0, match.index + match[0].length);
	const upper = beforeMatch.toUpperCase();
	if (upper.includes('VALUES') || upper.includes('SELECT')) {
		// The parenthesized group falls inside (or after) the VALUES clause,
		// not before it — that means the statement omits the column list.
		return null;
	}

	return match[1]
		.split(',')
		.map((column) => normalizeSqlIdentifier(column))
		.filter((column) => column.length > 0);
}

/**
 * Pulls the column list out of a RETURNING clause, if any.
 * @private
 */
function extractReturningColumns(sql: string): string[] | null {
	const match = sql.match(/\breturning\b\s+([^;]+)/i);
	if (!match || !match[1]) return null;
	return splitTopLevel(match[1], ',').map((entry) => entry.trim());
}

function projectReturningRow(row: Record<string, unknown>, columns: string[]): Record<string, unknown> {
	if (columns.length === 1 && columns[0] === '*') {
		return { ...row };
	}
	const projection = parseSelectColumns(columns.join(', '));
	const output: Record<string, unknown> = {};
	for (const column of projection) {
		if (column.kind === 'star') {
			Object.assign(output, row);
		} else if (column.kind === 'column') {
			output[column.alias] = row[column.expression] ?? null;
		} else {
			output[column.alias] = evaluateSelectColumn(column, [row], [], 0);
		}
	}
	return output;
}

/**
 * Recognizes `DEFAULT <literal>` modifiers in column definitions and
 * normalizes the literal value.
 * @private
 */
function extractDefaultValue(modifiers: string): unknown {
	const match = modifiers.match(/default\s+('([^']*)'|"([^"]*)"|[\w.-]+)/i);
	if (!match) return undefined;
	const raw = match[1] ?? '';
	const literal = parseLiteral(raw);
	return literal.consumed ? literal.value : raw;
}

/**
 * Prepared statement wrapper that delegates execution back to the in-memory
 * database. The wrapper picks the right method on the underlying database
 * based on the statement kind, so callers can use the same `.run()` /
 * `.first()` / `.all()` API regardless of the SQL kind.
 * @private
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
		// For non-SELECT statements `executeQuery` previously returned a failed
		// result. Route through `executeStatement` so RETURNING rows are still
		// surfaced via `.all()`.
		return (await this.database.executeStatement(this.sql, this.bindings)) as QueryResult<T>;
	}

	async first<T = Record<string, unknown>>(): Promise<T | null> {
		const result = await this.database.executeStatement(this.sql, this.bindings);
		return (result.results[0] as T) ?? null;
	}
}

/**
 * In-memory implementation of {@link KVStorage}.
 *
 * Implements every method on the contract plus a few NuxtHub-flavored aliases
 * (`set`, `del`, `keys`, `getItem`, `setItem`, `removeItem`, `getKeys`) so the
 * same instance can be wrapped by `createNuxtHubKVProvider` without any
 * additional adapter glue.
 */
export class InMemoryKVStorage implements KVStorage {
	private store = new Map<string, string>();
	private expirations = new Map<string, number>();

	/**
	 * Removes any keys whose TTL has expired. Cheap to call as part of read
	 * paths because the store stays small in test workloads.
	 * @private
	 */
	private pruneExpired(): void {
		if (this.expirations.size === 0) return;
		const now = Date.now();
		for (const [key, expiresAt] of this.expirations) {
			if (expiresAt < now) {
				this.store.delete(key);
				this.expirations.delete(key);
			}
		}
	}

	private normalizeValue(value: unknown): string {
		return typeof value === 'string' ? value : (JSON.stringify(value) ?? String(value));
	}

	async get<T = unknown>(key: string, type: 'json'): Promise<T | null>;
	async get(key: string, type?: 'text'): Promise<string | null>;
	async get<T = unknown>(key: string, type: 'text' | 'json' = 'text'): Promise<any> {
		const expiration = this.expirations.get(key);
		if (expiration !== undefined && expiration < Date.now()) {
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
		this.pruneExpired();

		const prefix = options?.prefix ?? '';
		const limit = options?.limit ?? 1000;
		const parsedCursor = options?.cursor ? Number.parseInt(options.cursor, 10) : 0;
		const cursor = Number.isFinite(parsedCursor) && parsedCursor >= 0 ? parsedCursor : 0;

		const allKeys: string[] = [];
		for (const key of this.store.keys()) {
			if (key.startsWith(prefix)) {
				allKeys.push(key);
			}
		}

		const slice = allKeys.slice(cursor, cursor + limit).map((name) => ({ name }));
		const nextCursor = cursor + limit;
		const listComplete = nextCursor >= allKeys.length;

		return {
			keys: slice,
			cursor: listComplete ? undefined : String(nextCursor),
			list_complete: listComplete
		};
	}

	/**
	 * Drops every entry from the store. Test utility only — production
	 * implementations should not expose this affordance.
	 */
	clear(): void {
		this.store.clear();
		this.expirations.clear();
	}

	/**
	 * Returns the number of non-expired entries currently in the store. Useful
	 * for asserting on cleanup behavior in tests.
	 */
	size(): number {
		this.pruneExpired();
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
 *   shards: { /* ... *\/ }
 * });
 * ```
 */
export function createInMemoryKVProvider(): InMemoryKVStorage {
	return new InMemoryKVStorage();
}
