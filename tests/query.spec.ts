import { describe, expect, it } from 'vitest';
import { buildDelete, buildInsert, buildUpdate, buildUpsert, quoteIdentifier, validateIdentifier } from '../src/index';

describe('SQL builders', () => {
	describe('buildInsert', () => {
		it('builds a parameterized insert preserving key order', () => {
			const { sql, bindings } = buildInsert('users', { id: 'u1', name: 'Ada', age: 36 });
			expect(sql).toBe('INSERT INTO "users" ("id", "name", "age") VALUES (?, ?, ?)');
			expect(bindings).toEqual(['u1', 'Ada', 36]);
		});

		it('skips undefined values but keeps explicit null', () => {
			const { sql, bindings } = buildInsert('users', { id: 'u1', nickname: null, missing: undefined });
			expect(sql).toBe('INSERT INTO "users" ("id", "nickname") VALUES (?, ?)');
			expect(bindings).toEqual(['u1', null]);
		});

		it('supports OR REPLACE, OR IGNORE, and RETURNING', () => {
			expect(buildInsert('t', { id: 1 }, { orReplace: true }).sql).toBe('INSERT OR REPLACE INTO "t" ("id") VALUES (?)');
			expect(buildInsert('t', { id: 1 }, { orIgnore: true }).sql).toBe('INSERT OR IGNORE INTO "t" ("id") VALUES (?)');
			expect(buildInsert('t', { id: 1 }, { returning: true }).sql).toBe('INSERT INTO "t" ("id") VALUES (?) RETURNING *');
			expect(buildInsert('t', { id: 1 }, { returning: ['id', 'name'] }).sql).toBe(
				'INSERT INTO "t" ("id") VALUES (?) RETURNING "id", "name"'
			);
		});

		it('throws on empty values', () => {
			expect(() => buildInsert('t', {})).toThrow('At least one column value is required');
		});

		it('rejects unsafe identifiers', () => {
			expect(() => buildInsert('users; DROP TABLE users', { id: 1 })).toThrow('Invalid SQL identifier');
			expect(() => buildInsert('users', { 'id = 1); DROP': 1 })).toThrow('Invalid SQL identifier');
		});
	});

	describe('buildUpdate', () => {
		it('builds SET then WHERE bindings in order', () => {
			const { sql, bindings } = buildUpdate('users', { name: 'Ada', age: 37 }, { id: 'u1' });
			expect(sql).toBe('UPDATE "users" SET "name" = ?, "age" = ? WHERE "id" = ?');
			expect(bindings).toEqual(['Ada', 37, 'u1']);
		});

		it('uses IS NULL for null where conditions without consuming a binding', () => {
			const { sql, bindings } = buildUpdate('users', { active: 0 }, { deleted_at: null });
			expect(sql).toBe('UPDATE "users" SET "active" = ? WHERE "deleted_at" IS NULL');
			expect(bindings).toEqual([0]);
		});

		it('refuses an update with no changes or no where', () => {
			expect(() => buildUpdate('users', {}, { id: 'u1' })).toThrow('At least one column value is required');
			expect(() => buildUpdate('users', { name: 'x' }, {})).toThrow('A WHERE condition is required');
		});
	});

	describe('buildDelete', () => {
		it('builds a scoped delete', () => {
			const { sql, bindings } = buildDelete('sessions', { user_id: 'u1' });
			expect(sql).toBe('DELETE FROM "sessions" WHERE "user_id" = ?');
			expect(bindings).toEqual(['u1']);
		});

		it('refuses an unfiltered delete', () => {
			expect(() => buildDelete('sessions', {})).toThrow('A WHERE condition is required');
		});
	});

	describe('buildUpsert', () => {
		it('overwrites non-conflict columns from excluded by default', () => {
			const { sql, bindings } = buildUpsert('kv', { k: 'a', v: '1' }, 'k');
			expect(sql).toBe('INSERT INTO "kv" ("k", "v") VALUES (?, ?) ON CONFLICT ("k") DO UPDATE SET "v" = excluded."v"');
			expect(bindings).toEqual(['a', '1']);
		});

		it('supports composite conflict targets and an explicit update subset', () => {
			const { sql } = buildUpsert('stats', { day: 1, region: 'x', hits: 5, note: 'n' }, ['day', 'region'], { update: ['hits'] });
			expect(sql).toBe(
				'INSERT INTO "stats" ("day", "region", "hits", "note") VALUES (?, ?, ?, ?) ON CONFLICT ("day", "region") DO UPDATE SET "hits" = excluded."hits"'
			);
		});

		it('degrades to DO NOTHING when there is nothing to overwrite', () => {
			const { sql } = buildUpsert('kv', { k: 'a' }, 'k');
			expect(sql).toBe('INSERT INTO "kv" ("k") VALUES (?) ON CONFLICT ("k") DO NOTHING');
		});

		it('throws when no conflict column is provided', () => {
			expect(() => buildUpsert('kv', { k: 'a' }, [])).toThrow('At least one conflict column is required');
		});
	});

	describe('quoteIdentifier', () => {
		it('quotes valid identifiers and schema-qualified names', () => {
			expect(quoteIdentifier('users')).toBe('"users"');
			expect(quoteIdentifier('main.users')).toBe('"main"."users"');
		});

		it('rejects empty and unsafe identifiers', () => {
			expect(() => quoteIdentifier('')).toThrow('Identifier cannot be empty');
			expect(() => quoteIdentifier('a b')).toThrow('Invalid SQL identifier');
		});

		it('keeps rejecting an invalid identifier after a valid one is cached', () => {
			// The result is memoized, so the branch that throws must not be able to
			// leave anything behind that a later call could read as an answer.
			expect(quoteIdentifier('users')).toBe('"users"');

			for (let attempt = 0; attempt < 3; attempt++) {
				expect(() => quoteIdentifier('users; DROP TABLE users')).toThrow('Invalid SQL identifier');
				expect(() => quoteIdentifier('')).toThrow('Identifier cannot be empty');
			}

			expect(quoteIdentifier('users')).toBe('"users"');
		});

		it('caches per dialect, so the same name quotes differently per backend', () => {
			expect(quoteIdentifier('accounts', 'postgres')).toBe('"accounts"');
			expect(quoteIdentifier('accounts', 'mysql')).toBe('`accounts`');
			expect(quoteIdentifier('accounts', 'postgres')).toBe('"accounts"');
			expect(quoteIdentifier('accounts')).toBe('"accounts"');
		});

		it('survives more distinct identifiers than the cache holds', () => {
			for (let i = 0; i < 1200; i++) {
				expect(quoteIdentifier(`col_${i}`)).toBe(`"col_${i}"`);
			}

			expect(quoteIdentifier('col_0')).toBe('"col_0"');
			expect(quoteIdentifier('col_1199')).toBe('"col_1199"');
		});
	});
});

describe('Dialect-aware identifier quoting', () => {
	it('uses backticks for MySQL and double quotes elsewhere', () => {
		expect(quoteIdentifier('users')).toBe('"users"');
		expect(quoteIdentifier('users', 'sqlite')).toBe('"users"');
		expect(quoteIdentifier('users', 'postgres')).toBe('"users"');

		// MySQL and MariaDB reject ANSI double quotes unless ANSI_QUOTES is set, so
		// a statement built with them fails outright there.
		expect(quoteIdentifier('users', 'mysql')).toBe('`users`');
		expect(quoteIdentifier('app.users', 'mysql')).toBe('`app`.`users`');
	});

	it('validates without quoting, for places that need a bare name', () => {
		expect(validateIdentifier('users')).toEqual(['users']);
		expect(validateIdentifier('app.users')).toEqual(['app', 'users']);
		expect(() => validateIdentifier('users; DROP TABLE t')).toThrow(/Invalid SQL identifier/);
		expect(() => validateIdentifier('  ')).toThrow(/cannot be empty/);
	});

	it('builds MySQL-safe statements from every builder', () => {
		const dialect = 'mysql' as const;

		expect(buildInsert('users', { id: 'u1', name: 'Ada' }, { dialect }).sql).toBe('INSERT INTO `users` (`id`, `name`) VALUES (?, ?)');
		expect(buildUpdate('users', { name: 'Ada' }, { id: 'u1' }, { dialect }).sql).toBe('UPDATE `users` SET `name` = ? WHERE `id` = ?');
		expect(buildDelete('users', { id: 'u1' }, { dialect }).sql).toBe('DELETE FROM `users` WHERE `id` = ?');
		expect(buildUpsert('kv', { k: 'a', v: '1' }, 'k', { dialect }).sql).toBe(
			'INSERT INTO `kv` (`k`, `v`) VALUES (?, ?) ON CONFLICT (`k`) DO UPDATE SET `v` = excluded.`v`'
		);
		expect(buildInsert('users', { id: 'u1' }, { dialect, returning: ['id'] }).sql).toContain('RETURNING `id`');
	});

	it('leaves the default output unchanged when no dialect is given', () => {
		expect(buildInsert('users', { id: 'u1' }).sql).toBe('INSERT INTO "users" ("id") VALUES (?)');
		expect(buildDelete('users', { id: 'u1' }).sql).toBe('DELETE FROM "users" WHERE "id" = ?');
	});
});
