import type { D1Database, KVNamespace } from '@cloudflare/workers-types';
import { sql as drizzleSql } from 'drizzle-orm';
import { drizzle as drizzleD1 } from 'drizzle-orm/d1';
import { drizzle as drizzleMySQL } from 'drizzle-orm/mysql2';
import { drizzle as drizzlePg } from 'drizzle-orm/node-postgres';
import type { Pool as MySQLPool } from 'mysql2/promise';
import type { Pool as PostgresPool } from 'pg';
import {
	createMySQLProvider,
	createNuxtHubKVProvider,
	createPostgreSQLProvider,
	createSQLiteProvider,
	initialize,
	run,
	type CollegeDBConfig
} from '../src/index';

function createNuxtHubKVCompatClient(kv: KVNamespace) {
	return {
		async get<T = unknown>(key: string): Promise<T | null> {
			return (await kv.get(key, 'text')) as T | null;
		},
		async set(key: string, value: unknown): Promise<void> {
			const serialized = typeof value === 'string' ? value : JSON.stringify(value);
			await kv.put(key, serialized);
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

export interface MultiVendorEnv {
	KV: KVNamespace;
	DB_CF: D1Database;
	PG_POOL: PostgresPool;
	MYSQL_POOL: MySQLPool;
}

export function initializeMultiVendorCollegeDB(env: MultiVendorEnv): CollegeDBConfig {
	const config: CollegeDBConfig = {
		kv: createNuxtHubKVProvider(createNuxtHubKVCompatClient(env.KV)),
		shards: {
			'db-cf': createSQLiteProvider(drizzleD1(env.DB_CF), drizzleSql),
			'db-pg': createPostgreSQLProvider(drizzlePg(env.PG_POOL), drizzleSql),
			'db-mysql': createMySQLProvider(drizzleMySQL(env.MYSQL_POOL), drizzleSql)
		},
		strategy: 'hash',
		disableAutoMigration: true
	};

	initialize(config);
	return config;
}

export async function createTenantUser(env: MultiVendorEnv, tenantId: string, userId: string, name: string): Promise<void> {
	initializeMultiVendorCollegeDB(env);
	const routedKey = `${tenantId}:user:${userId}`;
	await run(routedKey, 'INSERT INTO users (id, tenant_id, name) VALUES (?, ?, ?)', [routedKey, tenantId, name]);
}
