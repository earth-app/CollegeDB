import { beforeEach, describe, expect, it } from 'vitest';
import { planQuery, resetPlanCache, unroutableError } from '../src/planner';

describe('Query planner', () => {
	beforeEach(() => {
		resetPlanCache();
	});

	describe('recognized shapes', () => {
		it('reads the key from an INSERT column list', () => {
			const plan = planQuery('INSERT INTO users (id, name, email) VALUES (?, ?, ?)', ['user-1', 'Ada', 'ada@example.com']);
			expect(plan).toEqual({ kind: 'insert', table: 'users', keyColumn: 'id', keys: ['user-1'], readOnly: false });
		});

		it('reads the key when it is not the first column', () => {
			const plan = planQuery('INSERT INTO users (name, id, email) VALUES (?, ?, ?)', ['Ada', 'user-1', 'ada@example.com']);
			expect(plan?.keys).toEqual(['user-1']);
		});

		it('handles INSERT OR REPLACE and OR IGNORE', () => {
			expect(planQuery('INSERT OR REPLACE INTO users (id) VALUES (?)', ['user-1'])?.keys).toEqual(['user-1']);
			expect(planQuery('INSERT OR IGNORE INTO users (id) VALUES (?)', ['user-1'])?.keys).toEqual(['user-1']);
		});

		it('reads one key per row of a multi-row INSERT', () => {
			const plan = planQuery('INSERT INTO users (id, name) VALUES (?, ?), (?, ?), (?, ?)', [
				'user-1',
				'Ada',
				'user-2',
				'Grace',
				'user-3',
				'Alan'
			]);
			expect(plan?.keys).toEqual(['user-1', 'user-2', 'user-3']);
		});

		it('reads a numeric literal key written into the statement', () => {
			expect(planQuery('INSERT INTO users (id, name) VALUES (7, ?)', ['Ada'])?.keys).toEqual(['7']);
		});

		it('reads the key from UPDATE, DELETE, and SELECT', () => {
			expect(planQuery('UPDATE users SET name = ? WHERE id = ?', ['Ada', 'user-1'])).toMatchObject({
				kind: 'update',
				keys: ['user-1'],
				readOnly: false
			});
			expect(planQuery('DELETE FROM users WHERE id = ?', ['user-1'])).toMatchObject({ kind: 'delete', keys: ['user-1'] });
			expect(planQuery('SELECT * FROM users WHERE id = ?', ['user-1'])).toMatchObject({ kind: 'select', readOnly: true });
		});

		it('reads every key from an IN predicate', () => {
			const plan = planQuery('SELECT * FROM users WHERE id IN (?, ?, ?)', ['user-1', 'user-2', 'user-3']);
			expect(plan?.keys).toEqual(['user-1', 'user-2', 'user-3']);
		});

		it('tolerates quoted identifiers, extra whitespace, and trailing clauses', () => {
			expect(planQuery('SELECT * FROM "users" WHERE "id" = ? LIMIT 1', ['user-1'])?.keys).toEqual(['user-1']);
			expect(planQuery('select   *\n  from users\n  where id = ?\n order by name', ['user-1'])?.keys).toEqual(['user-1']);
			expect(planQuery('UPDATE users SET name = ? WHERE id = ? RETURNING *', ['Ada', 'user-1'])?.keys).toEqual(['user-1']);
		});

		it('uses the configured key column per table', () => {
			const options = { keyColumns: { tickets: 'ticket_id' } };
			expect(planQuery('SELECT * FROM tickets WHERE ticket_id = ?', ['t-1'], options)?.keys).toEqual(['t-1']);
			expect(planQuery('SELECT * FROM tickets WHERE id = ?', ['t-1'], options)).toBeNull();
			expect(planQuery('INSERT INTO tickets (ticket_id, title) VALUES (?, ?)', ['t-1', 'Broken'], options)?.keys).toEqual(['t-1']);
		});

		it('honours a non-default global key column', () => {
			expect(planQuery('SELECT * FROM users WHERE uuid = ?', ['u-1'], { defaultKeyColumn: 'uuid' })?.keys).toEqual(['u-1']);
		});
	});

	describe('refuses to guess', () => {
		it('returns null when the predicate is not on the key column', () => {
			expect(planQuery('SELECT * FROM users WHERE email = ?', ['ada@example.com'])).toBeNull();
		});

		it('returns null when the key column is only inside a string literal', () => {
			expect(planQuery('SELECT * FROM users WHERE email = ?', ["id = 'user-1'"])).toBeNull();
			expect(planQuery('SELECT * FROM logs WHERE message = ?', ['WHERE id = ?'])).toBeNull();
		});

		it('returns null when the key predicate is commented out', () => {
			expect(planQuery('SELECT * FROM users -- WHERE id = ?\nWHERE email = ?', ['ada@example.com'])).toBeNull();
			expect(planQuery('SELECT * FROM users /* WHERE id = ? */ WHERE email = ?', ['ada@example.com'])).toBeNull();
		});

		it('returns null for a compound WHERE clause, because another conjunct may narrow it', () => {
			expect(planQuery('SELECT * FROM users WHERE id = ? AND active = ?', ['user-1', 1])).toBeNull();
			expect(planQuery('DELETE FROM posts WHERE id = ? OR user_id = ?', ['p-1', 'user-1'])).toBeNull();
		});

		it('returns null for joins and subqueries, where a bare column is ambiguous', () => {
			expect(planQuery('SELECT * FROM users JOIN posts ON posts.user_id = users.id WHERE id = ?', ['user-1'])).toBeNull();
			expect(planQuery('SELECT * FROM users WHERE id = ?', ['user-1'])).not.toBeNull();
			expect(planQuery('SELECT * FROM users WHERE id IN (SELECT user_id FROM posts)', [])).toBeNull();
		});

		it('returns null for a CTE, whose routing predicate could be in any branch', () => {
			expect(planQuery('WITH recent AS (SELECT * FROM posts) SELECT * FROM recent WHERE id = ?', ['p-1'])).toBeNull();
			expect(planQuery('WITH x AS (SELECT 1) INSERT INTO users (id) VALUES (?)', ['user-1'])).toBeNull();
		});

		it('returns null when the key predicate is not fully parameterized', () => {
			expect(planQuery('SELECT * FROM users WHERE id IN (?, ?, 3)', ['user-1', 'user-2'])).toBeNull();
			expect(planQuery('SELECT * FROM users WHERE id > ?', ['user-1'])).toBeNull();
			expect(planQuery('SELECT * FROM users WHERE id LIKE ?', ['user-%'])).toBeNull();
		});

		it('returns null when the binding for the key is missing or null', () => {
			expect(planQuery('SELECT * FROM users WHERE id = ?', [])).toBeNull();
			expect(planQuery('SELECT * FROM users WHERE id = ?', [null])).toBeNull();
			expect(planQuery('INSERT INTO users (id, name) VALUES (?, ?)', [null, 'Ada'])).toBeNull();
		});

		it('returns null when the INSERT has no column list to match against', () => {
			expect(planQuery('INSERT INTO users VALUES (?, ?)', ['user-1', 'Ada'])).toBeNull();
		});

		it('returns null for statements with no routing concept', () => {
			expect(planQuery('CREATE TABLE users (id TEXT PRIMARY KEY)', [])).toBeNull();
			expect(planQuery('PRAGMA page_count', [])).toBeNull();
			expect(planQuery('SELECT COUNT(*) FROM users', [])).toBeNull();
			expect(planQuery('', [])).toBeNull();
		});

		it('returns null when a quoted column merely looks like the key column', () => {
			// The identifier is `id x`, not `id`, so it must not match.
			expect(planQuery('SELECT * FROM users WHERE "id x" = ?', ['user-1'])).toBeNull();
		});
	});

	describe('caching', () => {
		it('reuses the parsed shape while still reading fresh bindings', () => {
			const sql = 'SELECT * FROM users WHERE id = ?';
			expect(planQuery(sql, ['user-1'])?.keys).toEqual(['user-1']);
			expect(planQuery(sql, ['user-2'])?.keys).toEqual(['user-2']);
		});

		it('stays bounded when fed many distinct statements', () => {
			for (let i = 0; i < 2500; i++) {
				planQuery(`SELECT c${i} FROM users WHERE id = ?`, ['user-1']);
			}
			expect(planQuery('SELECT * FROM users WHERE id = ?', ['user-9'])?.keys).toEqual(['user-9']);
		});
	});

	it('names the explicit-key alternative when it cannot route', () => {
		const error = unroutableError('SELECT * FROM users WHERE email = ?');
		expect(error.code).toBe('UNROUTABLE_QUERY');
		expect(error.message).toContain('run/first/all');
		expect(error.message).toContain('keyColumns');
	});
});
