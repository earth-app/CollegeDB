import type { D1Database, KVNamespace } from '@cloudflare/workers-types';
import { sql as drizzleSql } from 'drizzle-orm';
import { drizzle as drizzleD1 } from 'drizzle-orm/d1';
import { createNuxtHubKVProvider, createSQLiteProvider, first, initialize, run } from '../src/index';

let initialized = false;

function createNuxtHubKVCompatClient(kv: KVNamespace) {
	return {
		async get<T = unknown>(key: string): Promise<T | null> {
			return (await kv.get(key, 'text')) as T | null;
		},
		async set(key: string, value: unknown, options?: { ttl?: number }): Promise<void> {
			const serialized = typeof value === 'string' ? value : JSON.stringify(value);
			await kv.put(key, serialized, options?.ttl ? { expirationTtl: options.ttl } : undefined);
		},
		async del(key: string): Promise<void> {
			await kv.delete(key);
		},
		async keys(prefix: string = ''): Promise<string[]> {
			const allKeys: string[] = [];
			let cursor: string | undefined;

			do {
				const result = await kv.list({ prefix, cursor, limit: 1000 });
				for (const item of result.keys) {
					allKeys.push(item.name);
				}
				cursor = result.list_complete ? undefined : result.cursor;
			} while (cursor);

			return allKeys;
		}
	};
}

function createDrizzleD1Provider(db: D1Database) {
	return createSQLiteProvider(drizzleD1(db), drizzleSql);
}

export function ensureCollegeDB(env: { KV: KVNamespace; DB_PRIMARY: D1Database; DB_SECONDARY: D1Database }): void {
	if (initialized) {
		return;
	}

	initialize({
		kv: createNuxtHubKVProvider(createNuxtHubKVCompatClient(env.KV)),
		shards: {
			'db-primary': createDrizzleD1Provider(env.DB_PRIMARY),
			'db-secondary': createDrizzleD1Provider(env.DB_SECONDARY)
		},
		strategy: 'hash',
		disableAutoMigration: true
	});

	initialized = true;
}

export async function upsertPostDraft(
	env: { KV: KVNamespace; DB_PRIMARY: D1Database; DB_SECONDARY: D1Database },
	slug: string,
	title: string
) {
	ensureCollegeDB(env);

	const key = `post:${slug}`;
	await run(key, 'INSERT OR REPLACE INTO blog_posts (id, slug, title) VALUES (?, ?, ?)', [key, slug, title]);

	return await first<{ id: string; slug: string; title: string }>(key, 'SELECT id, slug, title FROM blog_posts WHERE slug = ? LIMIT 1', [
		slug
	]);
}
