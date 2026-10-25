# Cloudflare

This page covers running CollegeDB on Cloudflare Workers: D1 databases as shards, a Workers KV namespace for shard mappings, and the optional coordinator. It is for developers deploying a Worker and for operators who run one. The library runs on other platforms too, and the [providers](providers.md) page lists the backends. The coordinator has its own page, [Coordinator](coordinator.md).

## Bindings

A Worker needs one D1 binding per shard and one KV binding for shard mappings. The coordinator binding is optional. The binding name is the shard name. The `db-east` binding is the shard `db-east` in the `shards` config, and the Worker reads it as `env['db-east']`.

| Binding            | Type                 | Required         | Role                             |
| ------------------ | -------------------- | ---------------- | -------------------------------- |
| `db-*`             | D1 database          | Yes, one or more | A data shard                     |
| `KV`               | Workers KV namespace | Yes              | Key to shard mappings            |
| `ShardCoordinator` | Durable Object       | No               | Shared allocation and statistics |

Upgrading a coordinator from 1.3.x is covered in [Coordinator](coordinator.md).

## Create D1 Databases

Each shard is a separate D1 database. Create one for each shard, then copy each database ID into the `database_id` field of its binding.

```bash
wrangler d1 create collegedb-east
wrangler d1 create collegedb-west
wrangler d1 create collegedb-central
```

The shards start empty. Create the same tables on every shard before the first request. [Getting started](getting-started.md) covers the schema calls.

## Create the KV Namespace

```bash
wrangler kv namespace create "KV"
```

Copy the namespace ID into the `id` field of the `KV` binding. Pass the binding through `createWorkersKVProvider`, which adapts it to the `KVStorage` interface the router uses. [Providers](providers.md) covers the adapter options, including the edge cache lifetime.

The adapter serves `getMany` with bulk reads of up to 100 keys each, with four requests in flight. `putMany` writes one entry per `put` call, 16 at a time by default. The `putConcurrency` option changes that count. `putMany` is not atomic. A failed write rejects with `KV_PUT_MANY_FAILED` and names the key, and the entries written before it stay written. A namespace object you write yourself must also implement the bulk form of `get(keys, options)`, because `getMany` calls it.

## Configure wrangler.jsonc

This config sets up two shards and the KV namespace. Add one `d1_databases` entry for each further shard. Each `binding` must match a key in the `shards` config.

```jsonc
{
	"$schema": "./node_modules/wrangler/config-schema.json",
	"name": "collegedb-app",
	"main": "src/index.ts",
	"compatibility_date": "2026-04-15",
	"d1_databases": [
		{
			"binding": "db-east",
			"database_name": "collegedb-east",
			"database_id": "your-east-database-id"
		},
		{
			"binding": "db-west",
			"database_name": "collegedb-west",
			"database_id": "your-west-database-id"
		}
	],
	"kv_namespaces": [
		{
			"binding": "KV",
			"id": "your-kv-namespace-id",
			"preview_id": "your-kv-preview-id"
		}
	]
}
```

### Compatibility Flags

Add `"compatibility_flags": ["nodejs_compat"]` only when you use the legacy `collegedb(config, callback)` form and run calls with different configs at the same time. The library loads without the flag, and `createClient(config)` needs nothing extra.

Without the flag, `collegedb` cannot keep two overlapping calls apart. A call that starts while a different configuration is still running throws `CONCURRENT_CONFIG`. With the flag, overlapping calls each keep their own configuration.

<details>
<summary>Full wrangler.jsonc with three shards, the coordinator and a production environment</summary>

```jsonc
{
	"$schema": "./node_modules/wrangler/config-schema.json",
	"name": "collegedb-app",
	"main": "src/index.ts",
	"compatibility_date": "2026-04-15",
	"compatibility_flags": ["nodejs_compat"],
	"d1_databases": [
		{
			"binding": "db-east",
			"database_name": "collegedb-east",
			"database_id": "your-east-database-id"
		},
		{
			"binding": "db-west",
			"database_name": "collegedb-west",
			"database_id": "your-west-database-id"
		},
		{
			"binding": "db-central",
			"database_name": "collegedb-central",
			"database_id": "your-central-database-id"
		}
	],
	"kv_namespaces": [
		{
			"binding": "KV",
			"id": "your-kv-namespace-id",
			"preview_id": "your-kv-preview-id"
		}
	],
	"durable_objects": {
		"bindings": [
			{
				"name": "ShardCoordinator",
				"class_name": "ShardCoordinator"
			}
		]
	},
	"migrations": [
		{
			"tag": "v1",
			"new_sqlite_classes": ["ShardCoordinator"]
		}
	],
	"env": {
		"production": {
			"d1_databases": [
				{
					"binding": "db-east",
					"database_name": "collegedb-prod-east",
					"database_id": "your-prod-east-id"
				},
				{
					"binding": "db-west",
					"database_name": "collegedb-prod-west",
					"database_id": "your-prod-west-id"
				}
			],
			"kv_namespaces": [
				{
					"binding": "KV",
					"id": "your-prod-kv-namespace-id"
				}
			],
			"durable_objects": {
				"bindings": [
					{
						"name": "ShardCoordinator",
						"class_name": "ShardCoordinator"
					}
				]
			}
		}
	}
}
```

</details>

## Worker Entry Point

The Worker builds the config from its bindings. The `coordinator` field is optional. When it is set, every new key is allocated through the coordinator, whatever the strategy. Export the `ShardCoordinator` class from the entry module when the binding is configured, so the runtime can resolve it.

```ts
import { collegedb, createWorkersKVProvider, first, ShardCoordinator } from '@earth-app/collegedb';

export { ShardCoordinator };

interface Env {
	KV: KVNamespace;
	ShardCoordinator: DurableObjectNamespace;
	'db-east': D1Database;
	'db-west': D1Database;
}

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		return await collegedb(
			{
				kv: createWorkersKVProvider(env.KV),
				coordinator: env.ShardCoordinator,
				strategy: 'hash',
				shards: {
					'db-east': env['db-east'],
					'db-west': env['db-west']
				}
			},
			async () => {
				const userId = new URL(request.url).searchParams.get('id');
				if (!userId) {
					return new Response('Missing user ID', { status: 400 });
				}

				const user = await first(userId, 'SELECT * FROM users WHERE id = ?', [userId]);
				return Response.json(user);
			}
		);
	}
};
```

## Deploy

```bash
wrangler deploy
wrangler deploy --env production
```

The second command uses the `production` block of the full config.

## Operations

### Shard Statistics

`getShardStats()` returns one entry per shard with its `binding` and `count`. `listKnownShards()` returns the shard names.

```ts
import { getShardStats, listKnownShards } from '@earth-app/collegedb';

const stats = await getShardStats();
// [{ binding: 'db-east', count: 1542 }, { binding: 'db-west', count: 1458 }]

const shards = await listKnownShards();
// ['db-east', 'db-west']
```

The source of the counts depends on the coordinator. With a coordinator configured and reachable, `getShardStats()` returns the coordinator's counters, and those change only when your code calls `incrementShardCount`, `decrementShardCount` or `POST /stats`. The router does not update them on writes. Without a coordinator, or when the coordinator call fails, the counts come from the KV mappings. [Coordinator](coordinator.md) lists the methods and endpoints.

### Moving Keys

`reassignShard(primaryKey, newBinding, tableName)` moves one key's row to another shard. [Shard moves](shard-moves.md) covers the move states, recovery and the journal.

### Health Checks

This recipe is for D1 shards on Workers. It runs a `SELECT 1` on each known shard, checks KV with a write and a read, and asks the coordinator for its health. Use the same `coordinatorScope` as the router. The sketch uses the default scope.

```ts
import { listKnownShards } from '@earth-app/collegedb';

type Status = 'healthy' | 'unhealthy';

async function checkHealth(env: Env): Promise<Record<string, Status>> {
	const checks: Record<string, Status> = {};

	try {
		await env.KV.put('health-check', 'ok', { expirationTtl: 60 });
		checks.kv = (await env.KV.get('health-check')) === 'ok' ? 'healthy' : 'unhealthy';
	} catch {
		checks.kv = 'unhealthy';
	}

	const databases = env as unknown as Record<string, D1Database>;
	await Promise.all(
		(await listKnownShards()).map(async (name) => {
			try {
				await databases[name]!.prepare('SELECT 1').first();
				checks[name] = 'healthy';
			} catch {
				checks[name] = 'unhealthy';
			}
		})
	);

	try {
		const coordinator = env.ShardCoordinator.get(env.ShardCoordinator.idFromName('default'));
		const response = await coordinator.fetch('http://coordinator/health');
		checks.coordinator = response.ok ? 'healthy' : 'unhealthy';
	} catch {
		checks.coordinator = 'unhealthy';
	}

	return checks;
}
```

Run the same function from a Cron Trigger by adding a `scheduled` handler next to `fetch`. The check returns the results, and what happens to them is up to you.

```ts
export default {
	async scheduled(_event: ScheduledEvent, env: Env): Promise<void> {
		console.log(JSON.stringify(await checkHealth(env)));
	}
};
```

### KV Timeouts

`kvTimeoutMs` limits how long one KV read or write may run. A call that runs longer rejects with `KV_TIMEOUT`. It never resolves as "not found". The underlying KV call is not cancelled. The option has no default, and the value below is only an example.

```ts
collegedb(
	{
		kv: createWorkersKVProvider(env.KV),
		kvTimeoutMs: 2000,
		shards: { 'db-east': env['db-east'], 'db-west': env['db-west'] },
		strategy: 'hash'
	},
	callback
);
```
