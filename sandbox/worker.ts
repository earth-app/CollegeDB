import type { D1Database, DurableObjectNamespace, KVNamespace } from '@cloudflare/workers-types';
import { sql as drizzleSql } from 'drizzle-orm';
import { drizzle as drizzleD1 } from 'drizzle-orm/d1';
import {
	all,
	allAllShards,
	allShard,
	createMappingsForExistingKeys,
	createNuxtHubKVProvider,
	createSchemaAcrossShards,
	createSQLiteProvider,
	first,
	flush,
	getShardStats,
	initialize,
	listKnownShards,
	run,
	runAllShards,
	runShard,
	ShardCoordinator
} from '../src/index';
import { KVShardMapper } from '../src/kvmap';
import type { CollegeDBConfig } from '../src/types';

export { ShardCoordinator };

interface Env {
	KV: KVNamespace;
	ShardCoordinator: DurableObjectNamespace;
	'db-east': D1Database;
	'db-west': D1Database;
	'db-central': D1Database;
}

type SandboxProfile = 'native' | 'drizzle' | 'nuxthub';

const BENCH_SCHEMA = `
	CREATE TABLE IF NOT EXISTS users (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT UNIQUE,
		created_at INTEGER
	);

	CREATE TABLE IF NOT EXISTS posts (
		id TEXT PRIMARY KEY,
		user_id TEXT NOT NULL,
		title TEXT NOT NULL,
		content TEXT,
		created_at INTEGER
	);
`;

const INSERT_USER_SQL = 'INSERT INTO users (id, name, email, created_at) VALUES (?, ?, ?, ?)';
const SELECT_USER_SQL = 'SELECT id, name, email, created_at FROM users WHERE id = ?';
const UPDATE_USER_SQL = 'UPDATE users SET name = ?, email = ? WHERE id = ?';
const DELETE_USER_SQL = 'DELETE FROM users WHERE id = ?';
const INSERT_POST_SQL = 'INSERT INTO posts (id, user_id, title, content, created_at) VALUES (?, ?, ?, ?, ?)';

function json(data: unknown, status = 200): Response {
	return new Response(JSON.stringify(data), {
		status,
		headers: { 'Content-Type': 'application/json' }
	});
}

function createNuxtHubKVCompatClient(kv: KVNamespace): {
	get: <T = unknown>(key: string) => Promise<T | null>;
	set: (key: string, value: unknown) => Promise<void>;
	del: (key: string) => Promise<void>;
	keys: (prefix?: string) => Promise<string[]>;
} {
	return {
		get: async <T = unknown>(key: string) => (await kv.get(key, 'text')) as T | null,
		set: async (key: string, value: unknown) => {
			const serialized = typeof value === 'string' ? value : JSON.stringify(value);
			await kv.put(key, serialized);
		},
		del: async (key: string) => {
			await kv.delete(key);
		},
		keys: async (prefix: string = '') => {
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

function createDrizzleD1CompatProvider(db: D1Database) {
	const drizzleDb = drizzleD1(db) as any;
	return createSQLiteProvider(
		{
			run: async (query: unknown) => await drizzleDb.run(query as any),
			all: async (query: unknown) => await drizzleDb.all(query as any),
			get: async (query: unknown) => await drizzleDb.get(query as any),
			execute: async (query: unknown) => await drizzleDb.run(query as any)
		},
		drizzleSql as any
	);
}

function normalizeProfile(raw: string | null): SandboxProfile {
	if (raw === 'drizzle' || raw === 'nuxthub') {
		return raw;
	}
	return 'native';
}

function buildConfig(env: Env, profile: SandboxProfile): CollegeDBConfig {
	const useDrizzle = profile === 'drizzle' || profile === 'nuxthub';
	const shardA = useDrizzle ? createDrizzleD1CompatProvider(env['db-east']) : (env['db-east'] as any);
	const shardB = useDrizzle ? createDrizzleD1CompatProvider(env['db-west']) : (env['db-west'] as any);
	const shardC = useDrizzle ? createDrizzleD1CompatProvider(env['db-central']) : (env['db-central'] as any);

	const kv = profile === 'nuxthub' ? createNuxtHubKVProvider(createNuxtHubKVCompatClient(env.KV)) : (env.KV as any);

	return {
		kv,
		coordinator: env.ShardCoordinator as any,
		shards: {
			'db-east': shardA,
			'db-west': shardB,
			'db-central': shardC
		},
		strategy: 'hash',
		disableAutoMigration: true,
		hashShardMappings: true,
		mappingCacheTtlMs: 60_000,
		knownShardsCacheTtlMs: 10_000,
		sizeCacheTtlMs: 10_000,
		migrationConcurrency: 25
	};
}

async function parseJson(request: Request): Promise<Record<string, unknown>> {
	try {
		const value = await request.json();
		if (!value || typeof value !== 'object') {
			return {};
		}
		return value as Record<string, unknown>;
	} catch {
		return {};
	}
}

function toBoundedInt(value: unknown, fallback: number, min: number, max: number): number {
	const parsed = Number.parseInt(String(value ?? ''), 10);
	if (!Number.isFinite(parsed)) {
		return fallback;
	}
	return Math.max(min, Math.min(max, parsed));
}

function extractCount(row: Record<string, unknown>): number {
	const keys = ['count', 'COUNT(*)', 'count(*)', 'COUNT'];
	for (const key of keys) {
		const value = row[key];
		if (value !== undefined && value !== null) {
			const parsed = Number(value);
			if (Number.isFinite(parsed)) {
				return parsed;
			}
		}
	}
	return 0;
}

async function resetData(): Promise<void> {
	await runAllShards('DELETE FROM posts');
	await runAllShards('DELETE FROM users');
	await flush();
}

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		const url = new URL(request.url);
		const { pathname, searchParams } = url;
		const profile = normalizeProfile(searchParams.get('profile'));

		const config = buildConfig(env, profile);
		initialize(config);

		try {
			if (pathname === '/health') {
				return new Response('OK', { status: 200 });
			}

			if (pathname === '/init' && request.method === 'POST') {
				await createSchemaAcrossShards(config.shards, BENCH_SCHEMA);
				await resetData();
				return json({
					success: true,
					message: 'Sandbox schema initialized',
					shards: Object.keys(config.shards)
				});
			}

			if (pathname === '/api/users' && request.method === 'POST') {
				const body = await parseJson(request);
				const id = String(body.id ?? '');
				const name = String(body.name ?? 'Unknown');
				const email = String(body.email ?? `${id}@sandbox.local`);
				if (!id) {
					return json({ error: 'id is required' }, 400);
				}

				await run(id, INSERT_USER_SQL, [id, name, email, Date.now()]);
				return json({ success: true, id });
			}

			if (pathname === '/api/users' && request.method === 'GET') {
				const id = searchParams.get('id');
				if (!id) {
					return json({ error: 'id query parameter is required' }, 400);
				}

				const user = await first<Record<string, unknown>>(id, SELECT_USER_SQL, [id]);
				if (!user) {
					return json({ error: 'Not found' }, 404);
				}

				return json({ success: true, user });
			}

			if (pathname === '/api/users' && request.method === 'PUT') {
				const body = await parseJson(request);
				const id = String(body.id ?? '');
				const name = String(body.name ?? 'Unknown');
				const email = String(body.email ?? `${id}@sandbox.local`);
				if (!id) {
					return json({ error: 'id is required' }, 400);
				}

				await run(id, UPDATE_USER_SQL, [name, email, id]);
				return json({ success: true, id });
			}

			if (pathname === '/api/users' && request.method === 'DELETE') {
				const id = searchParams.get('id');
				if (!id) {
					return json({ error: 'id query parameter is required' }, 400);
				}

				await run(id, DELETE_USER_SQL, [id]);
				return json({ success: true, id });
			}

			if (pathname === '/api/stats' && request.method === 'GET') {
				const shards = await getShardStats();
				const totalKeys = shards.reduce((sum, shard) => sum + (shard.count || 0), 0);
				return json({
					success: true,
					overview: {
						totalKeys,
						shardCount: shards.length
					},
					shards
				});
			}

			if (pathname === '/api/shards' && request.method === 'GET') {
				const shards = await listKnownShards();
				return json({ success: true, shards });
			}

			if (pathname === '/api/benchmark/reset' && request.method === 'POST') {
				await resetData();
				return json({ success: true });
			}

			if (pathname === '/api/benchmark/seed-users' && request.method === 'POST') {
				const body = await parseJson(request);
				const prefix = String(body.prefix ?? `seed-${Date.now()}`);
				const records = toBoundedInt(body.records, 50, 1, 1000);
				for (let i = 0; i < records; i++) {
					const id = `${prefix}-${i}`;
					await run(id, INSERT_USER_SQL, [id, `Seed ${i}`, `${id}@seed.local`, Date.now()]);
				}
				return json({ success: true, inserted: records });
			}

			if (pathname === '/api/benchmark/migration' && request.method === 'POST') {
				const body = await parseJson(request);
				const prefix = String(body.prefix ?? `migration-${Date.now()}`);
				const records = toBoundedInt(body.records, 20, 1, 500);
				const keys: string[] = [];
				for (let i = 0; i < records; i++) {
					const id = `${prefix}-${i}`;
					keys.push(id);
					await runShard('db-east', INSERT_USER_SQL, [id, `Legacy ${i}`, `${id}@legacy.local`, Date.now()]);
				}

				const mapper = new KVShardMapper(config.kv, {
					hashShardMappings: config.hashShardMappings,
					mappingCacheTtlMs: config.mappingCacheTtlMs,
					knownShardsCacheTtlMs: config.knownShardsCacheTtlMs
				});
				await createMappingsForExistingKeys(keys, ['db-east'], 'hash', mapper, { concurrency: Math.min(25, keys.length) });
				return json({ success: true, mapped: keys.length });
			}

			if (pathname === '/api/benchmark/indexing' && request.method === 'POST') {
				const body = await parseJson(request);
				const userId = String(body.userId ?? `indexed-user-${Date.now()}`);
				const posts = toBoundedInt(body.posts, 80, 1, 1000);
				await run(userId, INSERT_USER_SQL, [userId, 'Indexed User', `${userId}@index.local`, Date.now()]);
				for (let i = 0; i < posts; i++) {
					await run(userId, INSERT_POST_SQL, [`idx-${userId}-${i}`, userId, `Post ${i}`, 'index body', Date.now()]);
				}
				await runShard('db-east', 'CREATE INDEX IF NOT EXISTS idx_posts_user_id_bench ON posts (user_id)');

				const result = await all<{ id: string }>(userId, 'SELECT id FROM posts WHERE user_id = ?', [userId]);
				return json({ success: true, rows: result.results.length });
			}

			if (pathname === '/api/benchmark/metadata' && request.method === 'GET') {
				const result = await allShard<Record<string, unknown>>(
					'db-east',
					"SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name LIMIT 25"
				);
				return json({ success: result.success, rows: result.results.length });
			}

			if (pathname === '/api/benchmark/pragma' && request.method === 'GET') {
				const result = await allShard<Record<string, unknown>>('db-east', 'SELECT 1 AS info_ok');
				const firstRow = result.results[0] ?? null;
				return json({ success: result.success, value: firstRow });
			}

			if (pathname === '/api/benchmark/counting' && request.method === 'GET') {
				const shardResults = await allAllShards<Record<string, unknown>>('SELECT COUNT(*) AS count FROM users');
				const total = shardResults.reduce((sum, shard) => {
					return sum + shard.results.reduce((innerSum, row) => innerSum + extractCount(row), 0);
				}, 0);
				return json({ success: true, total });
			}

			if (pathname === '/api/benchmark/fanout' && request.method === 'GET') {
				const shardResults = await allAllShards<Record<string, unknown>>('SELECT COUNT(*) AS count FROM users');
				const total = shardResults.reduce((sum, shard) => {
					return sum + shard.results.reduce((innerSum, row) => innerSum + extractCount(row), 0);
				}, 0);
				return json({ success: true, total, shards: shardResults.length });
			}

			if (pathname === '/api/benchmark/reassignment' && request.method === 'POST') {
				const body = await parseJson(request);
				const id = String(body.id ?? `reassign-${Date.now()}`);
				await run(id, INSERT_USER_SQL, [id, 'Reassigned User', `${id}@reassign.local`, Date.now()]);

				const mapper = new KVShardMapper(config.kv, {
					hashShardMappings: config.hashShardMappings,
					mappingCacheTtlMs: config.mappingCacheTtlMs,
					knownShardsCacheTtlMs: config.knownShardsCacheTtlMs
				});
				const mapping = await mapper.getShardMapping(id);
				const shards = Object.keys(config.shards);
				const fallbackShard = shards[0];
				if (!fallbackShard) {
					return json({ error: 'No shard available for reassignment' }, 400);
				}
				const sourceShard = mapping?.shard ?? fallbackShard;
				const reassignedTo = shards.find((shard) => shard !== sourceShard) ?? fallbackShard;
				if (!reassignedTo) {
					return json({ error: 'No shard available for reassignment' }, 400);
				}

				const sourceRows = await allShard<Record<string, unknown>>(sourceShard, SELECT_USER_SQL, [id]);
				const sourceRecord = sourceRows.results[0];
				if (!sourceRecord) {
					return json({ error: 'Source record not found for reassignment' }, 500);
				}

				await runShard(reassignedTo, INSERT_USER_SQL, [
					id,
					String(sourceRecord.name ?? 'Reassigned User'),
					String(sourceRecord.email ?? `${id}@reassign.local`),
					Number(sourceRecord.created_at ?? Date.now())
				]);
				await runShard(sourceShard, DELETE_USER_SQL, [id]);
				await mapper.updateShardMapping(id, reassignedTo);

				const movedRows = await allShard<Record<string, unknown>>(reassignedTo, SELECT_USER_SQL, [id]);
				if (movedRows.results.length === 0) {
					return json({ error: 'Reassigned record not found' }, 500);
				}

				return json({ success: true, reassignedTo });
			}

			return new Response('Not Found', { status: 404 });
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error);
			return json({ error: message }, 500);
		}
	}
};
