import { describe, expect, it } from 'vitest';
import {
	PROVEN_JOURNAL_DIALECTS,
	buildDelete,
	buildInsert,
	buildUpdate,
	buildUpsert,
	quoteIdentifier,
	validateIdentifier
} from '../src/index';
import {
	FENCE_STRIPE,
	LOCK_STRIPES,
	buildCutoverStatements,
	buildFenceLock,
	buildGuardedInsert,
	buildGuardedUpsert,
	buildInsertIgnore,
	buildJournalSchema,
	buildLeaseAcquire,
	buildLeaseFence,
	buildLeaseRelease,
	buildLeaseRenew,
	buildShardSidecarSchema,
	buildWriteLock,
	isProvenJournalDialect,
	nullSafeEquals
} from '../src/query';

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

		describe('on MySQL and MariaDB', () => {
			const dialect = 'mysql' as const;

			it('uses ON DUPLICATE KEY UPDATE with VALUES(), the form both engines accept', () => {
				const { sql, bindings } = buildUpsert('kv', { k: 'a', v: '1', n: 2 }, 'k', { dialect });

				expect(sql).toBe('INSERT INTO `kv` (`k`, `v`, `n`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `v` = VALUES(`v`), `n` = VALUES(`n`)');
				expect(sql).not.toContain('ON CONFLICT');
				expect(sql).not.toContain(' AS ');
				expect(bindings).toEqual(['a', '1', 2]);
			});

			it('overwrites only the listed update columns and never a conflict column', () => {
				const { sql } = buildUpsert('stats', { day: 1, region: 'x', hits: 5, note: 'n' }, ['day', 'region'], {
					dialect,
					update: ['hits', 'day']
				});

				expect(sql).toBe(
					'INSERT INTO `stats` (`day`, `region`, `hits`, `note`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `hits` = VALUES(`hits`)'
				);
			});

			it('degrades to a no-op assignment on the first conflict column when there is nothing to overwrite', () => {
				expect(buildUpsert('kv', { k: 'a' }, 'k', { dialect }).sql).toBe(
					'INSERT INTO `kv` (`k`) VALUES (?) ON DUPLICATE KEY UPDATE `k` = `k`'
				);
			});

			it('keeps the conflict column validation', () => {
				expect(() => buildUpsert('kv', { k: 'a' }, [], { dialect })).toThrow('At least one conflict column is required');
				expect(() => buildUpsert('kv', { k: 'a' }, ' ', { dialect })).toThrow('At least one conflict column is required');
				expect(() => buildUpsert('kv', { k: 'a' }, 'k; DROP TABLE kv', { dialect })).toThrow('Invalid SQL identifier');
				expect(() => buildUpsert('kv', { k: 'a', v: 'b' }, ['k', 'v x'], { dialect })).toThrow('Invalid SQL identifier');
			});

			it('appends RETURNING after the assignments when asked', () => {
				expect(buildUpsert('kv', { k: 'a', v: '1' }, 'k', { dialect, returning: ['k'] }).sql).toBe(
					'INSERT INTO `kv` (`k`, `v`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `v` = VALUES(`v`) RETURNING `k`'
				);
			});

			it('leaves the other dialects on ON CONFLICT', () => {
				for (const other of ['postgres', 'sqlite', undefined] as const) {
					expect(buildUpsert('kv', { k: 'a', v: '1' }, 'k', { dialect: other }).sql).toContain(
						'ON CONFLICT ("k") DO UPDATE SET "v" = excluded."v"'
					);
				}
			});
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
			'INSERT INTO `kv` (`k`, `v`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `v` = VALUES(`v`)'
		);
		expect(buildInsert('users', { id: 'u1' }, { dialect, returning: ['id'] }).sql).toContain('RETURNING `id`');
	});

	it('leaves the default output unchanged when no dialect is given', () => {
		expect(buildInsert('users', { id: 'u1' }).sql).toBe('INSERT INTO "users" ("id") VALUES (?)');
		expect(buildDelete('users', { id: 'u1' }).sql).toBe('DELETE FROM "users" WHERE "id" = ?');
	});
});

describe('Journal statement builders', () => {
	const guard = { scope: 'default', table: 'items', key: 'k1' };

	it('builds a tombstone-guarded insert as one INSERT ... SELECT', () => {
		const { sql, bindings } = buildGuardedInsert('items', { id: 'k1', name: 'Ada' }, guard);

		expect(sql).toBe(
			'INSERT INTO "items" ("id", "name") SELECT ?, ? WHERE NOT EXISTS (SELECT 1 FROM "collegedb_retired" WHERE "scope" = ? AND "table_name" = ? AND "primary_key" = ?) ' +
				'AND (SELECT COALESCE(MAX("min_generation"), 0) FROM "collegedb_topology") <= ?'
		);
		expect(bindings).toEqual(['k1', 'Ada', 'default', 'items', 'k1', 0]);
	});

	it('carries the writer generation and the insert modifiers', () => {
		const built = buildGuardedInsert('items', { id: 'k1' }, { ...guard, generation: 4 }, { orIgnore: true, returning: ['id'] });

		expect(built.sql.startsWith('INSERT OR IGNORE INTO "items" ("id") SELECT ? WHERE')).toBe(true);
		expect(built.sql.endsWith('RETURNING "id"')).toBe(true);
		expect(built.bindings.at(-1)).toBe(4);
	});

	it('needs FROM DUAL on MySQL and quotes with backticks there', () => {
		const { sql } = buildGuardedInsert('items', { id: 'k1' }, guard, { dialect: 'mysql' });

		expect(sql).toContain('SELECT ? FROM DUAL WHERE NOT EXISTS (SELECT 1 FROM `collegedb_retired`');
		expect(sql).not.toContain('"');
	});

	it('puts the conflict clause after the WHERE of a guarded upsert', () => {
		const { sql, bindings } = buildGuardedUpsert('items', { id: 'k1', name: 'Ada' }, 'id', guard);

		expect(sql).toMatch(/WHERE NOT EXISTS .* <= \? ON CONFLICT \("id"\) DO UPDATE SET "name" = excluded\."name"$/);
		expect(bindings).toEqual(['k1', 'Ada', 'default', 'items', 'k1', 0]);
	});

	it('degrades a guarded upsert with nothing to update to DO NOTHING', () => {
		expect(buildGuardedUpsert('items', { id: 'k1' }, 'id', guard).sql).toMatch(/ON CONFLICT \("id"\) DO NOTHING$/);
	});

	it('guards a MySQL upsert and ends it with ON DUPLICATE KEY UPDATE', () => {
		const { sql, bindings } = buildGuardedUpsert('items', { id: 'k1', name: 'Ada' }, 'id', guard, { dialect: 'mysql' });

		expect(sql).toMatch(
			/^INSERT INTO `items` \(`id`, `name`\) SELECT \?, \? FROM DUAL WHERE NOT EXISTS .* <= \? ON DUPLICATE KEY UPDATE `name` = VALUES\(`name`\)$/
		);
		expect(sql).not.toContain('ON CONFLICT');
		expect(bindings).toEqual(['k1', 'Ada', 'default', 'items', 'k1', 0]);
		expect(buildGuardedUpsert('items', { id: 'k1' }, 'id', guard, { dialect: 'mysql' }).sql).toMatch(
			/ON DUPLICATE KEY UPDATE `id` = `id`$/
		);
	});

	it('refuses an empty guarded insert', () => {
		expect(() => buildGuardedInsert('items', {}, guard)).toThrow('At least one column value is required');
	});

	it('spells null-safe equality per dialect', () => {
		expect(nullSafeEquals('"note"', 'sqlite')).toBe('"note" IS ?');
		expect(nullSafeEquals('"note"')).toBe('"note" IS ?');
		expect(nullSafeEquals('"note"', 'postgres')).toBe('"note" IS NOT DISTINCT FROM ?');
		expect(nullSafeEquals('`note`', 'mysql')).toBe('`note` <=> ?');
	});

	describe('buildWriteLock', () => {
		it('pins Postgres to READ COMMITTED and takes an advisory lock on the key hash', () => {
			const [isolation, lock] = buildWriteLock(guard, 'postgres');

			expect(isolation).toEqual({ sql: 'SET TRANSACTION ISOLATION LEVEL READ COMMITTED', bindings: [] });
			expect(lock!.sql).toBe('SELECT pg_advisory_xact_lock(CAST(? AS bigint))');
			expect(lock!.bindings).toHaveLength(1);
			expect(BigInt(lock!.bindings[0] as string)).toBeGreaterThanOrEqual(-(2n ** 63n));
			expect(BigInt(lock!.bindings[0] as string)).toBeLessThan(2n ** 63n);
		});

		it('hashes the same key to the same lock and different keys apart', () => {
			const lockOf = (key: string, table = 'items') => buildWriteLock({ ...guard, key, table }, 'postgres')[1]!.bindings[0];

			expect(lockOf('k1')).toBe(lockOf('k1'));
			expect(lockOf('k1')).not.toBe(lockOf('k2'));
			expect(lockOf('k1')).not.toBe(lockOf('k1', 'other'));
			expect(buildWriteLock({ ...guard, scope: 'other' }, 'postgres')[1]!.bindings).not.toEqual(
				buildWriteLock(guard, 'postgres')[1]!.bindings
			);
		});

		it('locks one stripe row with a locking read on MySQL and MariaDB', () => {
			const locks = buildWriteLock(guard, 'mysql');

			expect(locks).toHaveLength(1);
			expect(locks[0]!.sql).toBe('SELECT stripe FROM collegedb_lock_stripes WHERE stripe = ? FOR UPDATE');
			const stripe = locks[0]!.bindings[0] as number;
			expect(Number.isInteger(stripe) && stripe >= 0 && stripe < LOCK_STRIPES).toBe(true);
			expect(buildWriteLock(guard, 'mysql')[0]!.bindings[0]).toBe(stripe);

			const stripes = new Set(Array.from({ length: 200 }, (_, i) => buildWriteLock({ ...guard, key: `k${i}` }, 'mysql')[0]!.bindings[0]));
			expect(stripes.size).toBeGreaterThan(100);
		});

		it('needs no lock on SQLite and D1', () => {
			expect(buildWriteLock(guard, 'sqlite')).toEqual([]);
			expect(buildWriteLock(guard)).toEqual([]);
		});

		it('builds the fence lock: shared for writers, exclusive for the raise, nothing on SQLite', () => {
			const [shared] = buildFenceLock('shared', 'postgres');
			const [exclusive] = buildFenceLock('exclusive', 'postgres');

			expect(shared!.sql).toBe('SELECT pg_advisory_xact_lock_shared(CAST(? AS bigint))');
			expect(exclusive!.sql).toBe('SELECT pg_advisory_xact_lock(CAST(? AS bigint))');
			expect(shared!.bindings).toEqual(exclusive!.bindings);
			expect(buildFenceLock('shared', 'mysql')).toEqual([
				{ sql: 'SELECT stripe FROM collegedb_lock_stripes WHERE stripe = ? LOCK IN SHARE MODE', bindings: [FENCE_STRIPE] }
			]);
			expect(buildFenceLock('exclusive', 'mysql')[0]!.sql).toBe('SELECT stripe FROM collegedb_lock_stripes WHERE stripe = ? FOR UPDATE');
			expect(buildFenceLock('shared', 'sqlite')).toEqual([]);
			expect(buildFenceLock('exclusive')).toEqual([]);
		});

		it('keeps the fence lock off every key stripe and off every key hash', () => {
			expect(FENCE_STRIPE).toBe(LOCK_STRIPES);
			for (let i = 0; i < 500; i++) {
				expect(buildWriteLock({ ...guard, key: `k${i}` }, 'mysql')[0]!.bindings[0]).toBeLessThan(FENCE_STRIPE);
				expect(buildWriteLock({ ...guard, key: `k${i}` }, 'postgres')[1]!.bindings[0]).not.toBe(
					buildFenceLock('shared', 'postgres')[0]!.bindings[0]
				);
			}
		});

		it('creates the stripe table only on MySQL', () => {
			const mysql = buildShardSidecarSchema('mysql');

			expect(mysql).toContain('CREATE TABLE IF NOT EXISTS collegedb_lock_stripes (stripe INT NOT NULL, PRIMARY KEY (stripe))');
			const fill = mysql.find((statement) => statement.startsWith('INSERT IGNORE INTO collegedb_lock_stripes'))!;
			// one row per stripe plus the row the fence lock uses
			expect(fill.match(/\(\d+\)/g)).toHaveLength(LOCK_STRIPES + 1);
			expect(fill).toContain(`(${FENCE_STRIPE})`);
			expect(buildShardSidecarSchema('postgres').join('\n')).not.toContain('collegedb_lock_stripes');
			expect(buildShardSidecarSchema('sqlite').join('\n')).not.toContain('collegedb_lock_stripes');
		});
	});

	it('spells insert-or-ignore per dialect', () => {
		expect(buildInsertIgnore('t', { a: 1 }, 'sqlite').sql).toBe('INSERT OR IGNORE INTO "t" ("a") VALUES (?)');
		expect(buildInsertIgnore('t', { a: 1 }, 'mysql').sql).toBe('INSERT IGNORE INTO `t` (`a`) VALUES (?)');
		expect(buildInsertIgnore('t', { a: 1 }, 'postgres').sql).toBe('INSERT INTO "t" ("a") VALUES (?) ON CONFLICT DO NOTHING');
	});

	describe('recovery lease statements', () => {
		const fence = { scope: 'default', generation: 3 };

		it('takes the lease with one upsert whose update only applies to an expired row', () => {
			const sqlite = buildLeaseAcquire('default', 'h1', 1000, 60_000, 'sqlite');

			expect(sqlite.sql).toBe(
				'INSERT INTO "collegedb_recovery_lease" ("scope", "holder", "generation", "expires_at") VALUES (?, ?, 1, ?) ' +
					'ON CONFLICT ("scope") DO UPDATE SET "holder" = EXCLUDED."holder", ' +
					'"generation" = "collegedb_recovery_lease"."generation" + 1, "expires_at" = EXCLUDED."expires_at" ' +
					'WHERE "collegedb_recovery_lease"."expires_at" <= ?'
			);
			expect(sqlite.bindings).toEqual(['default', 'h1', 61_000, 1000]);
			expect(buildLeaseAcquire('default', 'h1', 1000, 60_000, 'postgres')).toEqual(sqlite);
		});

		it('spells the takeover with IF() on MySQL and MariaDB, which have no conditional upsert', () => {
			const { sql, bindings } = buildLeaseAcquire('default', 'h1', 1000, 60_000, 'mysql');

			expect(sql).toContain('ON DUPLICATE KEY UPDATE `holder` = IF(`expires_at` <= ?, VALUES(`holder`), `holder`)');
			expect(sql).toContain('`generation` = IF(`expires_at` <= ?, `generation` + 1, `generation`)');
			// expires_at is assigned last, so the first two assignments still read its old value
			expect(sql.endsWith('`expires_at` = IF(`expires_at` <= ?, VALUES(`expires_at`), `expires_at`)')).toBe(true);
			expect(bindings).toEqual(['default', 'h1', 61_000, 1000, 1000, 1000]);
		});

		it('renews and releases only the generation the caller holds, and release zeroes the expiry', () => {
			expect(buildLeaseRenew(fence, 9000, 'sqlite')).toEqual({
				sql: 'UPDATE "collegedb_recovery_lease" SET "expires_at" = ? WHERE "scope" = ? AND "generation" = ?',
				bindings: [9000, 'default', 3]
			});
			expect(buildLeaseRelease(fence, 'mysql')).toEqual({
				sql: 'UPDATE `collegedb_recovery_lease` SET `expires_at` = ? WHERE `scope` = ? AND `generation` = ?',
				bindings: [0, 'default', 3]
			});
		});

		it('fences a journal insert in the statement itself, per dialect', () => {
			expect(buildLeaseFence(fence).sql).toBe('(SELECT "generation" FROM "collegedb_recovery_lease" WHERE "scope" = ?) = ?');

			const sqlite = buildInsertIgnore('t', { a: 1 }, 'sqlite', fence);
			expect(sqlite.sql).toBe(
				'INSERT OR IGNORE INTO "t" ("a") SELECT ? WHERE (SELECT "generation" FROM "collegedb_recovery_lease" WHERE "scope" = ?) = ?'
			);
			expect(sqlite.bindings).toEqual([1, 'default', 3]);

			const mysql = buildInsertIgnore('t', { a: 1 }, 'mysql', fence);
			expect(mysql.sql.startsWith('INSERT IGNORE INTO `t` (`a`) SELECT ? FROM DUAL WHERE (SELECT')).toBe(true);

			const postgres = buildInsertIgnore('t', { a: 1 }, 'postgres', fence);
			expect(postgres.sql.endsWith('= ? ON CONFLICT DO NOTHING')).toBe(true);
		});

		it('defines the lease table and the history key index on every dialect', () => {
			for (const dialect of ['sqlite', 'postgres', 'mysql'] as const) {
				const schema = buildJournalSchema(dialect).join('\n');
				expect(schema).toContain('CREATE TABLE IF NOT EXISTS collegedb_recovery_lease');
				expect(schema).toContain('PRIMARY KEY (scope))');
			}

			// MySQL has no CREATE INDEX IF NOT EXISTS, so the index is part of the table
			expect(buildJournalSchema('sqlite')).toContain(
				'CREATE INDEX IF NOT EXISTS collegedb_history_by_key ON collegedb_migration_history (scope, primary_key, table_name)'
			);
			expect(buildJournalSchema('postgres').join('\n')).toContain('CREATE INDEX IF NOT EXISTS collegedb_history_by_key');
			expect(buildJournalSchema('mysql').join('\n')).not.toContain('CREATE INDEX');
			expect(buildJournalSchema('mysql').join('\n')).toContain(', INDEX collegedb_history_by_key (scope, primary_key, table_name))');
		});
	});

	describe('buildCutoverStatements', () => {
		const spec = {
			table: 'items',
			pkColumn: 'id',
			columns: [
				['id', 'k1'],
				['name', null],
				['data', new Uint8Array([1, 2])]
			] as Array<[string, unknown]>,
			scope: 'default',
			key: 'k1',
			opId: 'op-1',
			generation: 3,
			movedTo: 'db-b',
			payload: '{"v":1}',
			archivedAt: 99
		};

		it('conditions the archive insert and the delete on the same predicate', () => {
			const { archive, delete: del } = buildCutoverStatements(spec);
			const predicate = '"id" = ? AND "name" IS ? AND "data" IS ?';

			expect(del.sql).toBe(`DELETE FROM "items" WHERE ${predicate}`);
			expect(archive.sql).toContain(`WHERE EXISTS (SELECT 1 FROM "items" WHERE ${predicate})`);
			expect(del.bindings).toEqual(['k1', null, new Uint8Array([1, 2])]);
			expect(archive.bindings.slice(-3)).toEqual(del.bindings);
			expect(archive.bindings.slice(0, 9)).toEqual(['default', 'items', 'k1', 'id', 'op-1', 3, 'db-b', '{"v":1}', 99]);
		});

		it('compares only the key and the version column when one is given', () => {
			const { delete: del } = buildCutoverStatements({ ...spec, columns: [...spec.columns, ['version', 7]], versionColumn: 'version' });

			expect(del.sql).toBe('DELETE FROM "items" WHERE "id" = ? AND "version" IS ?');
			expect(del.bindings).toEqual(['k1', 7]);
		});

		it('uses the dialect spelling for equality, quoting and the select', () => {
			const { archive, delete: del } = buildCutoverStatements({ ...spec, dialect: 'mysql' });

			expect(del.sql).toBe('DELETE FROM `items` WHERE `id` = ? AND `name` <=> ? AND `data` <=> ?');
			expect(archive.sql).toContain('FROM DUAL WHERE EXISTS');
			expect(buildCutoverStatements({ ...spec, dialect: 'postgres' }).delete.sql).toContain('"name" IS NOT DISTINCT FROM ?');
		});

		it('counts bind parameters so a wide row can be refused', () => {
			const wide = Array.from({ length: 120 }, (_, i) => [`c${i}`, i] as [string, unknown]);
			const { archive } = buildCutoverStatements({ ...spec, columns: [['id', 'k1'], ...wide] });

			expect(archive.bindings.length).toBe(9 + 1 + 120);
		});

		it('fails when the snapshot lacks the key or the version column', () => {
			expect(() => buildCutoverStatements({ ...spec, columns: [['name', 'x']] })).toThrow('no primary key column');
			expect(() => buildCutoverStatements({ ...spec, versionColumn: 'version' })).toThrow('no version column');
		});
	});

	it('defines the journal and sidecar tables with portable types', () => {
		const sqlite = [...buildShardSidecarSchema('sqlite'), ...buildJournalSchema('sqlite')].join('\n');
		const mysql = [...buildShardSidecarSchema('mysql'), ...buildJournalSchema('mysql')].join('\n');

		expect(sqlite).toContain('CREATE TABLE IF NOT EXISTS collegedb_retired');
		expect(sqlite).toContain('PRIMARY KEY (scope, table_name, primary_key)');
		expect(sqlite).not.toMatch(/WHERE|AUTOINCREMENT|UNIQUE INDEX/);
		expect(mysql).toContain('VARCHAR(190)');
		expect(mysql).toContain('LONGTEXT');
		expect(mysql).not.toContain('TEXT NOT NULL');
		expect(buildJournalSchema('postgres').join('\n')).toContain('BIGINT');
	});

	it('proves the journal on SQLite, Postgres and MySQL, which includes MariaDB', () => {
		expect(PROVEN_JOURNAL_DIALECTS).toEqual(['sqlite', 'postgres', 'mysql']);
		expect(isProvenJournalDialect()).toBe(true);
		expect(isProvenJournalDialect('sqlite')).toBe(true);
		expect(isProvenJournalDialect('postgres')).toBe(true);
		expect(isProvenJournalDialect('mysql')).toBe(true);
		expect(isProvenJournalDialect('oracle' as 'mysql')).toBe(false);
	});
});
