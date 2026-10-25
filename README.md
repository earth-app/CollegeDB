# CollegeDB

Horizontal sharding router for SQL databases

[![TypeScript](https://img.shields.io/badge/TypeScript-5.8+-blue.svg)](https://www.typescriptlang.org/)
[![GitHub Issues](https://img.shields.io/github/issues/earth-app/CollegeDB)](https://github.com/earth-app/CollegeDB/issues)
[![GitHub License](https://img.shields.io/github/license/earth-app/CollegeDB)](LICENSE)
[![NPM Version](https://img.shields.io/npm/v/%40earth-app%2Fcollegedb)](https://www.npmjs.com/package/@earth-app/collegedb)

CollegeDB splits one logical table across many databases. It routes each key to the shard that owns it, either through a mapping store or by computing the shard from the key. It works over Cloudflare D1, PostgreSQL, MySQL, MariaDB and SQLite, directly or through Drizzle ORM, and runs on Cloudflare Workers, Node and Bun.

## Supported Backends

Any shard database works with any mapping store. A deployment can also mix shard databases from different vendors.

| Role           | Backend                        | Adapter                                                                |
| -------------- | ------------------------------ | ---------------------------------------------------------------------- |
| Shard database | Cloudflare D1                  | A D1 binding, or `toProvider`                                          |
| Shard database | PostgreSQL                     | `createPostgreSQLProvider`                                             |
| Shard database | MySQL and MariaDB              | `createMySQLProvider`                                                  |
| Shard database | SQLite                         | `createSQLiteProvider`                                                 |
| Shard database | Drizzle ORM over the above     | The matching adapter, with the Drizzle `sql` tag                       |
| Shard database | Hyperdrive (PostgreSQL, MySQL) | `createHyperdrivePostgresProvider` and `createHyperdriveMySQLProvider` |
| Shard database | In-memory, for tests           | `createInMemorySQLProvider`                                            |
| Mapping store  | Workers KV                     | `createWorkersKVProvider`                                              |
| Mapping store  | Redis                          | `createRedisKVProvider`                                                |
| Mapping store  | Valkey                         | `createValkeyKVProvider`                                               |
| Mapping store  | NuxtHub KV                     | `createNuxtHubKVProvider`                                              |
| Mapping store  | In-memory, for tests           | `createInMemoryKVProvider`                                             |
| Coordinator    | Durable Object                 | `ShardCoordinator`, optional and available on Cloudflare Workers only  |

[Providers](docs/providers.md) lists what each adapter takes and which ones support atomic batches and shard moves.

## Install

```bash
bun add @earth-app/collegedb
# or
npm install @earth-app/collegedb
```

CollegeDB has no runtime dependencies. Install the client for your backend, such as `pg`, `mysql2`, `redis` or `drizzle-orm`. TypeScript 5.8 or later is the supported compiler. The declaration files import types from `@cloudflare/workers-types`, so add it as a dev dependency in a TypeScript project.

## Quick Start

`createClient` takes the mapping store, the shards and a strategy. `run` and `first` take the routing key before the SQL, so the insert and the read for `user-1` reach the same shard. The example uses the in-memory providers so it runs without a database. Each snippet after it swaps in a real backend and leaves the calls unchanged.

```ts
import { createClient, createInMemoryKVProvider, createInMemorySQLProvider } from '@earth-app/collegedb';

const client = createClient({
	kv: createInMemoryKVProvider(),
	shards: {
		'shard-a': createInMemorySQLProvider(),
		'shard-b': createInMemorySQLProvider()
	},
	strategy: 'hash'
});

await client.ensureSchema('CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT NOT NULL)');

await client.run('user-1', 'INSERT INTO users (id, name) VALUES (?, ?)', ['user-1', 'Ada']);

const user = await client.first<{ id: string; name: string }>('user-1', 'SELECT id, name FROM users WHERE id = ?', ['user-1']);
console.log(user);
```

Cloudflare Workers with two D1 databases and a Workers KV namespace:

```ts
import type { D1Database, KVNamespace } from '@cloudflare/workers-types';
import { createClient, createWorkersKVProvider, type CollegeDBClient } from '@earth-app/collegedb';

interface Env {
	KV: KVNamespace;
	DB_EAST: D1Database;
	DB_WEST: D1Database;
}

let client: CollegeDBClient | undefined;

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		client ??= createClient({
			kv: createWorkersKVProvider(env.KV),
			shards: { 'db-east': env.DB_EAST, 'db-west': env.DB_WEST },
			strategy: 'hash'
		});

		const id = new URL(request.url).searchParams.get('id') ?? '';
		const user = await client.first(id, 'SELECT id, name FROM users WHERE id = ?', [id]);
		return Response.json(user);
	}
};
```

Node with two PostgreSQL pools and Redis:

```ts
import { createClient as createRedisClient } from 'redis';
import { Pool } from 'pg';
import { createClient, createPostgreSQLProvider, createRedisKVProvider } from '@earth-app/collegedb';

const redis = createRedisClient({ url: process.env.REDIS_URL });
await redis.connect();

const client = createClient({
	kv: createRedisKVProvider(redis),
	shards: {
		'pg-a': createPostgreSQLProvider(new Pool({ connectionString: process.env.PG_A_URL })),
		'pg-b': createPostgreSQLProvider(new Pool({ connectionString: process.env.PG_B_URL }))
	},
	strategy: 'hash'
});
```

[Getting Started](docs/getting-started.md) covers a first setup in full, and [Providers](docs/providers.md) has an example for each backend.

## How It Works

```text
 application
     |  run(key, sql, bindings)
     v
 +-----------+   known key?   +-----------------------------+
 |  router   |--------------->|  mapping store              |
 |           |<---------------|  Workers KV, Redis, Valkey, |
 +-----------+   shard name   |  NuxtHub KV or in-memory    |
   |     |                    +-----------------------------+
   |     | new key
   |     +--> strategy: hash, round-robin, random or location
   |          (optional coordinator allocates instead)
   v
 +---------+    +---------+    +---------+
 | shard a |    | shard b |    | shard c |   D1, PostgreSQL, MySQL, ...
 +---------+    +---------+    +---------+
```

Every keyed call carries a routing key. The router looks the key up in the mapping store, which holds the shard name for each key written so far. A key with no mapping gets a shard from the configured strategy, and a write records the result. With `placement: 'computed'`, the `hash` strategy derives the shard from the key, and the mapping store holds only keys that were moved. The optional coordinator is a Durable Object that allocates new keys for every strategy, which makes round-robin exact across Workers isolates. [Architecture](docs/architecture.md) describes the read and write paths.

## Features

- **Sharding strategies:** `hash`, `round-robin`, `random` and `location`, with separate strategies for reads and writes. [Sharding](docs/sharding.md)
- **Computed placement:** rendezvous hashing finds a key's shard without a mapping lookup, and topology epochs keep existing keys resolvable when shards change. [Sharding](docs/sharding.md)
- **Crash-safe shard moves:** `reassignShard`, `rebalance` and `recoverMigrations` move rows through a journal, and a recovery lease lets one recovery run at a time. [Shard Moves](docs/shard-moves.md)
- **Read consistency:** a per-call `consistency` option selects `primary`, `session` or `eventual` reads. [Reads and Writes](docs/reads-and-writes.md)
- **Shard eligibility:** `shardState` and an `eligible` predicate keep draining, read-only or out-of-jurisdiction shards out of new allocations. [Sharding](docs/sharding.md)
- **Instance clients:** `createClient` gives each configuration its own state, so tenants and tests do not share caches. [Getting Started](docs/getting-started.md)
- **Batched and transactional writes:** `batch()` groups statements by shard and sends one round trip per shard, inside one transaction where the adapter supports it. [Reads and Writes](docs/reads-and-writes.md)
- **Routing without a key argument:** `query`, `queryFirst` and `queryAll` take the routing key from the statement. [Reads and Writes](docs/reads-and-writes.md)
- **Generated ids:** `insert`, `insertShard` and `nextId` handle keys that the database assigns. [Generated Ids](docs/generated-ids.md)
- **Drop-in integration:** existing databases join as shards, and integration records a mapping for each existing key. [Drop-in](docs/drop-in.md)
- **In-memory providers:** emulated shards and a mapping store for unit tests and local development. [Testing](docs/testing.md)

## Sharding Strategies

A strategy picks the shard for a new key. After that, reads and writes follow the recorded mapping. `strategy` also takes an object with a `read` and a `write` strategy.

| Strategy      | New key goes to                                      |
| ------------- | ---------------------------------------------------- |
| `hash`        | The eligible shard with the highest rendezvous score |
| `round-robin` | The next eligible shard in order                     |
| `random`      | A random eligible shard                              |
| `location`    | The eligible shard nearest `targetRegion`            |

`hash` is the default and the only strategy that supports computed placement. `round-robin` splits keys exactly only through a coordinator. [Sharding](docs/sharding.md) covers how to choose one.

## Documentation

The API reference is generated from the source at [earth-app.github.io/CollegeDB](https://earth-app.github.io/CollegeDB).

| Page                                             | Covers                                                                      |
| ------------------------------------------------ | --------------------------------------------------------------------------- |
| [Getting Started](docs/getting-started.md)       | A first setup, instance clients, one-call setup on Workers and CRUD helpers |
| [Providers](docs/providers.md)                   | Adapters, mapping stores, batch and journal support, Hyperdrive             |
| [Configuration](docs/configuration.md)           | Every `CollegeDBConfig` field, its default and example configurations       |
| [Sharding](docs/sharding.md)                     | Strategies, computed placement, eligibility, size limits and lookup keys    |
| [Reads and Writes](docs/reads-and-writes.md)     | Read helpers, consistency, routing without a key and batched writes         |
| [Generated Ids](docs/generated-ids.md)           | Database-assigned keys and how rows with them are routed                    |
| [Shard Moves](docs/shard-moves.md)               | `reassignShard`, `rebalance`, recovery, topology epochs and reconciliation  |
| [Migration Protocol](docs/migration-protocol.md) | The contract for moving one keyed record between shards                     |
| [Drop-in](docs/drop-in.md)                       | Adding CollegeDB in front of databases that already hold data               |
| [Query Guidance](docs/query-guidance.md)         | Cross-shard pagination, indexes, query plans and SQL habits for fanout      |
| [Architecture](docs/architecture.md)             | Components, the read and write paths and the coordinator                    |
| [Coordinator](docs/coordinator.md)               | The `ShardCoordinator` Durable Object, its endpoints and its failure modes  |
| [Cloudflare](docs/cloudflare.md)                 | Bindings, `wrangler.jsonc`, the Worker entry point and operations           |
| [Testing](docs/testing.md)                       | In-memory providers, unit and integration tests and the memory sandbox      |
| [Benchmarks](docs/benchmarks.md)                 | The sandbox benchmark runner and measuring your own deployment              |
| [API Reference](docs/api-reference.md)           | Every exported function, grouped by purpose, and every error code           |

## Upgrading

A `ShardCoordinator` Durable Object written by 1.3.x copies its state into its SQLite storage on its first requests, and [Coordinator](docs/coordinator.md) describes the dual-write window and `POST /retire-legacy`. Since 1.4.0, `reassignShard`, `recoverMigrations` and a `rebalance` that is not a dry run need `journalShard` on a multi-shard configuration, as [Shard Moves](docs/shard-moves.md) explains.

## Contributing

Fork the repository, create a branch, and open a pull request against `master`. CI runs Prettier, the TypeScript check and the tests. `bun run prettier:check`, `bun run typecheck` and `bun run test` run the same checks locally.

## License

CollegeDB is released under the MIT License. See the [LICENSE](LICENSE) file for details.

## Links

- [npm package](https://www.npmjs.com/package/@earth-app/collegedb)
- [API reference](https://earth-app.github.io/CollegeDB)
- [Cloudflare D1](https://developers.cloudflare.com/d1/), [Workers KV](https://developers.cloudflare.com/kv/), [Durable Objects](https://developers.cloudflare.com/durable-objects/) and [Hyperdrive](https://developers.cloudflare.com/hyperdrive/)
- [Drizzle ORM](https://orm.drizzle.team/)

## Support

- [Documentation](https://earth-app.github.io/CollegeDB)
- [Report an issue](https://github.com/earth-app/CollegeDB/issues)
- [Discussions](https://github.com/earth-app/CollegeDB/discussions)
