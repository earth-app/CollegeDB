/**
 * @fileoverview Vitest example: zero-dependency unit + integration tests with the in-memory providers.
 *
 * Demonstrates how to build a complete test harness around CollegeDB without
 * spinning up a real database or KV namespace. Save this file as
 * `tests/users.spec.ts` (or similar) and run it with `vitest`.
 *
 * What it covers:
 * - Per-test isolation via `resetConfig()` and fresh provider instances
 * - Hash routing across three in-memory shards
 * - RETURNING-based AUTOINCREMENT inserts
 * - Compound WHERE assertions with `LIKE` + `AND`
 * - Cross-shard aggregates with `countAllShards` / `allAllShardsGlobal`
 * - Lookup-key mappings for email-based authentication queries
 *
 * The same patterns work against the real adapters in production — only the
 * provider factories change.
 *
 * @author CollegeDB Team
 * @since 1.2.2
 */

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
	allAllShardsGlobal,
	countAllShards,
	createInMemoryKVProvider,
	createInMemorySQLProvider,
	first,
	firstByLookupKey,
	initialize,
	insert,
	KVShardMapper,
	resetConfig,
	run,
	runShard
} from '../src/index';

// Each test gets its own KVStorage so lookup-key mappings don't leak.
let kv: ReturnType<typeof createInMemoryKVProvider>;

beforeEach(async () => {
	resetConfig();
	kv = createInMemoryKVProvider();

	initialize({
		kv,
		shards: {
			'db-east': createInMemorySQLProvider(),
			'db-west': createInMemorySQLProvider(),
			'db-central': createInMemorySQLProvider()
		},
		strategy: 'hash',
		hashShardMappings: false,
		disableAutoMigration: true
	});

	for (const shard of ['db-east', 'db-west', 'db-central']) {
		await runShard(
			shard,
			`CREATE TABLE IF NOT EXISTS users (
				id TEXT PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT UNIQUE,
				role TEXT,
				created_at INTEGER
			)`
		);
		await runShard(
			shard,
			`CREATE TABLE IF NOT EXISTS tickets (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				user_id TEXT NOT NULL,
				subject TEXT NOT NULL,
				severity TEXT,
				created_at INTEGER
			)`
		);
	}
});

afterEach(() => {
	resetConfig();
});

describe('users CRUD', () => {
	it('inserts and reads back a routed user', async () => {
		await run('user-1', 'INSERT INTO users (id, name, email, role, created_at) VALUES (?, ?, ?, ?, ?)', [
			'user-1',
			'Alice',
			'alice@example.com',
			'admin',
			Date.now()
		]);

		const user = await first<{ id: string; email: string }>('user-1', 'SELECT id, email FROM users WHERE id = ?', ['user-1']);

		expect(user).toEqual({ id: 'user-1', email: 'alice@example.com' });
	});

	it('finds a user via email through a lookup-key mapping', async () => {
		await run('user-1', 'INSERT INTO users (id, name, email, role, created_at) VALUES (?, ?, ?, ?, ?)', [
			'user-1',
			'Alice',
			'alice@example.com',
			'admin',
			Date.now()
		]);

		const mapper = new KVShardMapper(kv, { hashShardMappings: false });
		await mapper.addLookupKeys('user-1', ['email:alice@example.com']);

		const found = await firstByLookupKey<{ id: string }>('email:alice@example.com', 'SELECT id FROM users WHERE email = ?', [
			'alice@example.com'
		]);

		expect(found?.id).toBe('user-1');
	});
});

describe('tickets with auto-increment + aggregates', () => {
	it('creates a ticket with auto-generated id and finds it again', async () => {
		const created = await insert('INSERT INTO tickets (user_id, subject, severity, created_at) VALUES (?, ?, ?, ?)', [
			'user-1',
			'Cannot log in',
			'high',
			Date.now()
		]);

		expect(typeof created.generatedId).toBe('number');

		const ticket = await first<{ subject: string }>(String(created.generatedId), 'SELECT subject FROM tickets WHERE id = ?', [
			created.generatedId
		]);
		expect(ticket?.subject).toBe('Cannot log in');
	});

	it('returns the next available ticket id with COALESCE(MAX(id), 0) + 1', async () => {
		for (const subject of ['login', 'sync', 'billing']) {
			await insert('INSERT INTO tickets (user_id, subject, severity, created_at) VALUES (?, ?, ?, ?)', [
				'user-1',
				subject,
				'low',
				Date.now()
			]);
		}

		// Cross-shard fanout: each shard reports its own MAX, then we take the
		// global MAX. Real apps should prefer AUTOINCREMENT; this exercises the
		// aggregate emulator.
		const result = await allAllShardsGlobal<{ next_id: number }>('SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM tickets', [], {
			sortBy: 'next_id',
			sortDirection: 'desc',
			limit: 1
		});
		expect(result.results[0]?.next_id).toBeGreaterThan(0);
	});

	it('filters with compound WHERE (LIKE + AND severity = ?)', async () => {
		await insert('INSERT INTO tickets (user_id, subject, severity, created_at) VALUES (?, ?, ?, ?)', [
			'user-1',
			'Critical login error',
			'high',
			Date.now()
		]);
		await insert('INSERT INTO tickets (user_id, subject, severity, created_at) VALUES (?, ?, ?, ?)', [
			'user-2',
			'Minor login warning',
			'low',
			Date.now()
		]);

		const rows = await allAllShardsGlobal<{ subject: string }>(
			'SELECT subject FROM tickets WHERE subject LIKE ? AND severity = ?',
			['%login%', 'high'],
			{ sortBy: 'subject' }
		);

		expect(rows.results.map((row: { subject: string }) => row.subject)).toEqual(['Critical login error']);
	});

	it('reports counts across shards', async () => {
		for (let i = 0; i < 6; i++) {
			await insert('INSERT INTO tickets (user_id, subject, severity, created_at) VALUES (?, ?, ?, ?)', [
				`user-${i % 3}`,
				`Issue ${i}`,
				'medium',
				Date.now()
			]);
		}

		const aggregate = await countAllShards('tickets');
		expect(aggregate.total).toBe(6);
	});
});
