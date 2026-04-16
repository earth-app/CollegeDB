/**
 * @fileoverview Local sandbox showing non-Cloudflare providers with CollegeDB.
 *
 * This example runs entirely in-memory and demonstrates how to plug Redis/Valkey
 * style KV and SQLite-style SQL providers into the routing layer.
 *
 * Run with:
 * bun examples/provider-sandbox.ts
 */

import { createRedisKVProvider, createSQLiteProvider } from '../src/providers.js';
import { first, initialize, run } from '../src/router.js';

class InMemoryRedisLike {
	private readonly store = new Map<string, string>();

	async get(key: string): Promise<string | null> {
		return this.store.get(key) ?? null;
	}

	async set(key: string, value: string): Promise<void> {
		this.store.set(key, value);
	}

	async del(key: string): Promise<void> {
		this.store.delete(key);
	}

	async scan(_cursor: string, ...args: any[]): Promise<[string, string[]]> {
		const matchIndex = args.findIndex((arg) => arg === 'MATCH');
		const pattern = matchIndex >= 0 ? String(args[matchIndex + 1]) : '*';
		const prefix = pattern.endsWith('*') ? pattern.slice(0, -1) : pattern;
		const keys = Array.from(this.store.keys()).filter((key) => key.startsWith(prefix));
		return ['0', keys];
	}
}

class InMemorySQLiteLike {
	private readonly users = new Map<string, { id: string; name: string }>();

	prepare(sql: string) {
		return {
			run: (...bindings: any[]) => {
				if (sql.startsWith('CREATE TABLE')) {
					return { changes: 0 };
				}
				if (sql.startsWith('INSERT')) {
					this.users.set(String(bindings[0]), { id: String(bindings[0]), name: String(bindings[1]) });
					return { changes: 1, lastInsertRowid: 1 };
				}
				return { changes: 0 };
			},
			all: (...bindings: any[]) => {
				if (sql.startsWith('SELECT') && bindings.length > 0) {
					const user = this.users.get(String(bindings[0]));
					return user ? [user] : [];
				}
				return Array.from(this.users.values());
			},
			get: (...bindings: any[]) => {
				if (sql.startsWith('SELECT') && bindings.length > 0) {
					return this.users.get(String(bindings[0])) ?? null;
				}
				return null;
			}
		};
	}
}

async function main() {
	const kv = createRedisKVProvider(new InMemoryRedisLike());
	const sqliteShardA = createSQLiteProvider(new InMemorySQLiteLike());
	const sqliteShardB = createSQLiteProvider(new InMemorySQLiteLike());

	initialize({
		kv,
		shards: {
			'sqlite-a': sqliteShardA,
			'sqlite-b': sqliteShardB
		},
		strategy: 'hash',
		disableAutoMigration: true
	});

	await run('user-sandbox-1', 'CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL)');
	await run('user-sandbox-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-sandbox-1', 'Sandbox User']);

	const user = await first<{ id: string; name: string }>('user-sandbox-1', 'SELECT * FROM users WHERE id = ?', ['user-sandbox-1']);
	console.log('Sandbox lookup:', user);
}

main().catch((error) => {
	console.error(error);
	process.exit(1);
});
