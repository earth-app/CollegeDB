# Providers

This page lists the adapters that connect CollegeDB to SQL databases and key-value stores. It covers what each adapter takes, which SQL dialect it reports, whether it declares an atomic batch, and which adapters can be the source of a shard move. It is for developers choosing a backend or wiring an existing database client into CollegeDB.

A provider adapts one client to the `SQLDatabase` or `KVStorage` interface that the router uses. Native bindings and custom providers both work, as long as they match the exported interfaces. [Getting Started](getting-started.md) shows a first setup with the common adapters.

## Provider Adapters

Pass a Drizzle `sql` tag as the second argument to `createPostgreSQLProvider`, `createMySQLProvider` or `createSQLiteProvider` to run statements through Drizzle. Without the tag, the adapter takes the native client.

| Adapter                                       | Takes                                                                                           | Dialect               | Declares atomicBatch                                          | Journal source                                                    |
| --------------------------------------------- | ----------------------------------------------------------------------------------------------- | --------------------- | ------------------------------------------------------------- | ----------------------------------------------------------------- |
| `createPostgreSQLProvider`                    | A `pg` pool, or a `pg` client with `singleConnection` or `lease`                                | `postgres`            | Yes for a pool. A client needs `singleConnection` or `lease`. | Yes                                                               |
| `createMySQLProvider`                         | A `mysql2` pool, or a connection with `singleConnection` or `lease`. MariaDB uses this adapter. | `mysql`               | Same rule as PostgreSQL                                       | Yes, for MySQL and MariaDB                                        |
| `createSQLiteProvider`                        | A SQLite handle, with `singleConnection: true` or `lease`                                       | `sqlite`              | Only with `singleConnection: true` or `lease`                 | Yes                                                               |
| `toProvider` on a D1 binding                  | A D1 binding                                                                                    | `sqlite`              | Yes                                                           | Yes, with the production caveat below                             |
| Drizzle over Postgres or MySQL                | A Drizzle client and its `sql` tag                                                              | `postgres` or `mysql` | Yes, when the client has a transaction                        | Yes                                                               |
| Drizzle over `bun:sqlite` or `better-sqlite3` | A Drizzle client and its `sql` tag                                                              | `sqlite`              | Yes                                                           | Yes over `bun:sqlite`. `better-sqlite3` is not in the test suite. |
| Drizzle over D1 or libsql over HTTP           | A Drizzle client and its `sql` tag                                                              | `sqlite`              | No. Batches run one statement at a time.                      | No. A move from this shard is refused.                            |
| `createHyperdrivePostgresProvider`            | A Hyperdrive binding and a `pg` client factory                                                  | `postgres`            | Yes                                                           | Yes                                                               |
| `createHyperdriveMySQLProvider`               | A Hyperdrive binding and a `mysql2` client factory                                              | `mysql`               | Yes                                                           | Yes                                                               |
| `createDrizzleSQLProvider`                    | A Drizzle client, its `sql` tag and an optional dialect                                         | As passed             | Depends on the client                                         | Depends on the client                                             |

`createDrizzleSQLProvider` is a compatibility helper. The three Drizzle-aware factories above call it internally. The dialect of a Drizzle client is set by the factory you call, not detected from the driver.

## Batch and Journal Support

`atomicBatch` means the adapter runs a batch inside one transaction. Without it, the statements of a batch run one at a time. A shard move needs its source to declare `atomicBatch` and to report a dialect that the journal supports. The supported dialects are `sqlite`, `postgres` and `mysql`. Builder writes on PostgreSQL, MySQL and MariaDB also need `atomicBatch` when `journalShard` is set. The full rules are in [Migration Protocol](migration-protocol.md).

Pools are detected from their methods. A `mysql2` pool has `getConnection`, and a `pg` pool has `connect` with `totalCount` and `idleCount`. A pool leases one connection per batch, so the batch does not wait on other statements. Set `singleConnection: true` only on a dedicated connection. A single shared connection with that flag holds other statements back while a batch is open, so one slow batch delays every request that uses the connection. Use `lease` for a handle that CollegeDB does not recognize.

```ts
import { createPostgreSQLProvider } from '@earth-app/collegedb';
import { Client } from 'pg';

const client = new Client({ connectionString: process.env.POSTGRES_URL });
await client.connect();

const shard = createPostgreSQLProvider(client, { singleConnection: true });
```

The `pg` client above is one dedicated connection, so `singleConnection` is set. A pool would not set it.

## PostgreSQL With Redis Mappings

The example stores shard mappings in Redis and puts rows on one PostgreSQL pool. Connect Redis before `createClient` runs, since the client starts reading the mapping store in the background as soon as it is created.

```ts
import { createClient as createRedisClient } from 'redis';
import { Pool } from 'pg';
import { createClient, createPostgreSQLProvider, createRedisKVProvider, type CollegeDBConfig } from '@earth-app/collegedb';

const redis = createRedisClient({ url: process.env.REDIS_URL });
const pool = new Pool({ connectionString: process.env.POSTGRES_URL });

const config: CollegeDBConfig = {
	kv: createRedisKVProvider(redis),
	shards: {
		'pg-east': createPostgreSQLProvider(pool)
	},
	strategy: 'hash'
};

await redis.connect();
const client = createClient(config);

await client.run('user-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Taylor']);
```

## Runtime Examples

Each example below pairs one set of adapters with the same routing calls used above.

<details>
<summary>NuxtHub with D1 through Drizzle</summary>

The shard is the D1 database behind the NuxtHub Drizzle `db`. Drizzle over D1 cannot be a shard move source, as the table above notes.

```ts
import { db } from '@nuxthub/db';
import { kv } from '@nuxthub/kv';
import { sql } from 'drizzle-orm';
import { createNuxtHubKVProvider, createSQLiteProvider, initialize, run, first } from '@earth-app/collegedb';

initialize({
	kv: createNuxtHubKVProvider(kv),
	shards: {
		'db-primary': createSQLiteProvider(db, sql)
	},
	strategy: 'hash'
});

await run('draft:home', 'INSERT OR REPLACE INTO drafts (id, content) VALUES (?, ?)', ['draft:home', '# Home']);

const draft = await first<{ id: string; content: string }>('draft:home', 'SELECT id, content FROM drafts WHERE id = ?', ['draft:home']);
```

</details>

<details>
<summary>Cloudflare D1 bindings with Workers KV</summary>

`toProvider` detects a raw D1 binding and wraps it. It returns `null` for a value it does not recognize, so check the result before adding it to `shards`. [One-Call Setup](getting-started.md) makes the same call for each binding in `env`.

```ts
import { createClient, createWorkersKVProvider, toProvider } from '@earth-app/collegedb';

const dbEast = toProvider(env.DB_EAST);
if (!dbEast) throw new Error('DB_EAST is not a supported binding');

const client = createClient({
	kv: createWorkersKVProvider(env.KV),
	shards: { 'db-east': dbEast },
	strategy: 'hash'
});
```

</details>

## Multi-Vendor Shards

A shard map can mix vendors. Each shard gets the adapter for its own client. The example routes rows across a D1 database, a PostgreSQL server and a MySQL server.

```ts
import { sql } from 'drizzle-orm';
import { drizzle as drizzlePg } from 'drizzle-orm/postgres-js';
import { drizzle as drizzleMySQL } from 'drizzle-orm/mysql2';
import { drizzle as drizzleD1 } from 'drizzle-orm/d1';
import { kv } from '@nuxthub/kv';
import postgres from 'postgres';
import mysql from 'mysql2/promise';
import {
	createMySQLProvider,
	createNuxtHubKVProvider,
	createPostgreSQLProvider,
	createSQLiteProvider,
	initialize,
	run
} from '@earth-app/collegedb';

const pgClient = postgres(process.env.POSTGRES_URL!);
const mysqlPool = mysql.createPool(process.env.MYSQL_URL!);

function setup(env: { DB_CF: D1Database }) {
	initialize({
		kv: createNuxtHubKVProvider(kv),
		shards: {
			'db-cf': createSQLiteProvider(drizzleD1(env.DB_CF), sql),
			'db-pg': createPostgreSQLProvider(drizzlePg(pgClient), sql),
			'db-mysql': createMySQLProvider(drizzleMySQL(mysqlPool), sql)
		},
		strategy: 'hash'
	});
}

export default defineEventHandler(async (event) => {
	setup(event.context.cloudflare.env);

	await run('tenant:acme:user:1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['tenant:acme:user:1', 'Ada']);
});
```

Drizzle over D1 is not a journal source, so a shard move cannot start from `db-cf`. The PostgreSQL and MySQL shards can be move sources when their clients declare `atomicBatch`, as the Drizzle rows above describe.

## Keep Drizzle Schema and Migrations

CollegeDB does not replace a Drizzle schema or the NuxtHub migration workflow. Generate and apply migrations with the tools you already use, then route runtime reads and writes through CollegeDB:

```bash
npx nuxt db generate
npx nuxt db migrate
```

Each shard needs the tables in its schema. Run the migration against every shard, or create the tables with `ensureSchema` from the client. A complete setup that does not use Cloudflare is in [`examples/provider-sandbox.ts`](https://github.com/earth-app/CollegeDB/blob/master/examples/provider-sandbox.ts).

## Keep NuxtHub Cache and KV Semantics

The mapping store and the application cache can share one NuxtHub KV namespace. CollegeDB stores mappings under the `shard:` and `multikey:` prefixes, and it hashes the primary key in each mapping key by default. Application cache keys in the same store do not collide with them.

```ts
import { kv } from '@nuxthub/kv';
import { first } from '@earth-app/collegedb';

const cacheKey = `nuxtpress:post:${slug}`;
const cached = await kv.get(cacheKey);
if (cached) return cached;

const post = await first(cacheKey, 'SELECT * FROM blog_posts WHERE slug = ? LIMIT 1', [slug]);
await kv.set(cacheKey, post, { ttl: 3600 });
```

The first argument to `first` is the routing key. This example routes by the cache key, so the same slug always reaches the same shard.

## Mapping Stores

A mapping store is a `KVStorage`. Any mapping store works with any SQL adapter.

| Store                | Adapter                                |
| -------------------- | -------------------------------------- |
| Workers KV           | `createWorkersKVProvider(kv, options)` |
| NuxtHub KV           | `createNuxtHubKVProvider(kv)`          |
| Redis                | `createRedisKVProvider(client)`        |
| Valkey               | `createValkeyKVProvider(client)`       |
| In-memory, for tests | `createInMemoryKVProvider()`           |

`isKVStorage` checks whether a value already matches the `KVStorage` interface, so a custom store that matches it needs no adapter. `initializeFromEnv` wraps a raw Workers KV namespace for you. Elsewhere, pass the namespace through `createWorkersKVProvider`.

## Workers KV Edge Caching

Shard mappings are close to write-once, so the Workers KV adapter asks for a one-hour edge cache by default (`cacheTtl`, 3600 seconds, in `src/providers.ts`). A reassigned mapping can be served from an edge cache for up to `cacheTtl` seconds after the write. Lower the value when reassignments must reach every edge sooner. Pass `0` to send no `cacheTtl` and use the platform's own default. The in-process mapping cache adds its own window, set by `mappingCacheTtlMs` (30000 milliseconds by default).

```ts
import { createWorkersKVProvider, initialize } from '@earth-app/collegedb';

initialize({
	kv: createWorkersKVProvider(env.KV, { cacheTtl: 300 }),
	shards: { 'db-east': env.DB_EAST }
});
```

## Hyperdrive Connection Lifetime

The Hyperdrive adapters create one client per request and reuse it for every statement in that request. A client cached in module scope throws on Workers, so either give the provider a way to identify the current request, or dispose of it at the end of the request.

Scoped, for one configuration shared across requests:

```ts
import { Client } from 'pg';
import { createHyperdrivePostgresProvider } from '@earth-app/collegedb';

let currentRequest: Request | undefined;

const shard = createHyperdrivePostgresProvider(env.HYPERDRIVE, (connectionString) => new Client({ connectionString }), {
	scope: () => currentRequest
});

export default {
	async fetch(request: Request, env: Env) {
		currentRequest = request; // a new request gets a new client
		// ... handle the request
	}
};
```

Explicit, when the provider is built per request:

```ts
import { Client } from 'pg';
import { createHyperdrivePostgresProvider } from '@earth-app/collegedb';

export default {
	async fetch(request: Request, env: Env, ctx: ExecutionContext) {
		const shard = createHyperdrivePostgresProvider(env.HYPERDRIVE, (cs) => new Client({ connectionString: cs }));
		ctx.waitUntil(shard.dispose());
		// ... handle the request
	}
};
```

The Hyperdrive adapters take PostgreSQL and MySQL clients only. D1 needs no connection pooling from Workers, so use a D1 binding instead.
