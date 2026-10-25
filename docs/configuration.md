# Configuration

This page lists every field of `CollegeDBConfig`, the strategy and region types, and example configurations. Read it when you write the object you pass to `initialize`, `collegedb` or `createClient`. Function signatures are in [API reference](api-reference.md).

## Config Fields

`kv` and `shards` are required. Every other field is optional, and the default applies when the field is omitted. A field marked "none" has no default and changes nothing when unset.

| Field                   | Default              | Purpose                                                                                                                                                                             |
| ----------------------- | -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `kv`                    | required             | Key-value store for shard mappings. See [Providers](providers.md).                                                                                                                  |
| `shards`                | required             | Map of shard binding name to `SQLDatabase`.                                                                                                                                         |
| `strategy`              | `'hash'`             | Strategy for new keys: one `ShardingStrategy`, or a `MixedShardingStrategy` with separate `read` and `write` values. A partial object falls back to the other side, then to `hash`. |
| `coordinator`           | none                 | `DurableObjectNamespace` for allocation. Used by every strategy when set. Setup is in [Cloudflare](cloudflare.md).                                                                  |
| `coordinatorScope`      | `'default'`          | Name passed to `idFromName` for the coordinator object. A new scope starts from an empty allocation ledger.                                                                         |
| `targetRegion`          | none                 | `D1Region` that the `location` strategy aims at. Without it, `location` falls back to `hash`.                                                                                       |
| `shardLocations`        | none                 | Region for each shard, as a `D1Region` or a `{ region, priority? }` object. `location` only picks shards that have an entry. See [Sharding](sharding.md).                           |
| `placement`             | `'kv'`               | `'computed'` resolves `hash` keys from the key itself and skips the KV read. It is opt-in and works only with `hash`. The other strategies always use `kv`.                         |
| `allocateOnRead`        | `false`              | Records a mapping when a read finds no row. Writes always record a mapping.                                                                                                         |
| `legacyMultiKeyLookup`  | `false`              | Probes the pre-1.0.3 multi-key record on a miss. Costs a second KV read per miss. Enable it only while migrating older mappings.                                                    |
| `keyColumns`            | `id` for every table | Primary-key column per table. The planner uses it to find the routing key, and topology probes use it to find a row.                                                                |
| `onUnroutable`          | `'throw'`            | `throw` rejects a statement whose routing key cannot be proven. `fanout` runs it on every shard.                                                                                    |
| `hashShardMappings`     | `true`               | Hashes primary keys with SHA-256 before they are written to KV. With it on, a mapping's `originalKey` is not stored.                                                                |
| `mappingCacheTtlMs`     | `30000`              | In-memory TTL, in milliseconds, for the key-to-shard cache.                                                                                                                         |
| `knownShardsCacheTtlMs` | `10000`              | In-memory TTL, in milliseconds, for the list of known shards.                                                                                                                       |
| `sizeCacheTtlMs`        | `30000`              | In-memory TTL, in milliseconds, for shard size checks. Applies when `maxDatabaseSize` is set.                                                                                       |
| `maxDatabaseSize`       | none                 | Size limit in bytes. A shard at or above the limit takes no new keys. A shard whose size cannot be read is not excluded. See [Sharding](sharding.md).                               |
| `migrationConcurrency`  | `25`                 | Concurrent operations for migration mapping work and for `rebalance`.                                                                                                               |
| `disableAutoMigration`  | `false`              | Skips the background migration detection that `initialize` starts.                                                                                                                  |
| `journalShard`          | none                 | Binding that holds the migration journal. Required for moves on a multi-shard configuration. A single-shard configuration uses its only shard. See [Shard moves](shard-moves.md).   |
| `journalScope`          | `'default'`          | Partition of the journal, for deployments that share one journal database.                                                                                                          |
| `kvTimeoutMs`           | none                 | Per-call time limit for KV reads and writes. A call past the limit rejects with `KV_TIMEOUT`. The underlying call is not cancelled.                                                 |
| `shardState`            | none                 | Known facts per shard binding: `id`, `writable`, `drain`, and `tags`. See [Sharding](sharding.md).                                                                                  |
| `eligible`              | none                 | Predicate `(shard, context) => boolean or Promise<boolean>` that decides whether a shard takes new keys. A throw fails the operation with `ELIGIBILITY_FAILED`.                     |
| `onPhase`               | none                 | `(span: PhaseSpan) => void` observer for per-phase timings. Nothing is allocated when it is unset.                                                                                  |
| `waitUntil`             | none                 | Passes the Workers `ctx.waitUntil`. It keeps the background work started by `initialize` alive after the request ends.                                                              |
| `debug`                 | `false`              | Enables debug logging: size exclusions, shards whose size cannot be read, and auto-migration progress.                                                                              |

`shardState` takes the following fields. `writable` defaults to `true`, and `drain` defaults to `'active'`. Only a writable shard with `drain` set to `'active'` takes new keys. A shard that is not writable, or is `draining` or `retired`, keeps its existing keys readable.

| Field      | Type                                  | Purpose                                                                               |
| ---------- | ------------------------------------- | ------------------------------------------------------------------------------------- |
| `id`       | `string`                              | Physical database id. For D1, the database UUID. Session bookmarks are keyed by it.   |
| `writable` | `boolean`                             | `false` keeps the shard out of new allocations.                                       |
| `drain`    | `'active' or 'draining' or 'retired'` | Where the shard is in being emptied. `retired` says its data should already be moved. |
| `tags`     | `Record<string, string>`              | Labels for the `eligible` predicate, such as a jurisdiction or a schema version.      |

## Strategy Types

The strategy types are declared in `src/types.ts`.

```typescript
// one strategy for every operation
type ShardingStrategy = 'round-robin' | 'random' | 'hash' | 'location';

// separate strategies for reads and writes
interface MixedShardingStrategy {
	read: ShardingStrategy;
	write: ShardingStrategy;
}

type OperationType = 'read' | 'write';
```

`hash` is the default. Under `kv` placement, a key's shard is recorded in KV, so a later change to the strategy does not move existing keys. See [Sharding](sharding.md) for what each strategy does with a new key.

## Regions and Shard Locations

`D1Region` names the region of a shard. The `location` strategy compares these codes against `targetRegion`.

| Code   | Region                                |
| ------ | ------------------------------------- |
| `wnam` | Western North America (US West Coast) |
| `enam` | Eastern North America (US East Coast) |
| `weur` | Western Europe                        |
| `eeur` | Eastern Europe                        |
| `apac` | Asia Pacific                          |
| `oc`   | Oceania                               |
| `me`   | Middle East                           |
| `af`   | Africa                                |

## Example Configurations

A single strategy for every operation:

```typescript
const singleStrategyConfig: CollegeDBConfig = {
	kv: env.KV,
	strategy: 'hash',
	shards: { 'db-east': env.DB_EAST, 'db-west': env.DB_WEST }
};
```

Separate strategies for reads and writes, with location data for the `location` side:

```typescript
const mixedStrategyConfig: CollegeDBConfig = {
	kv: env.KV,
	strategy: {
		read: 'hash',
		write: 'location'
	},
	targetRegion: 'wnam',
	shardLocations: {
		'db-west': { region: 'wnam', priority: 2 },
		'db-east': { region: 'enam', priority: 1 }
	},
	shards: { 'db-west': env.DB_WEST, 'db-east': env.DB_EAST }
};
```

The `CollegeDBConfig` type comes from `@earth-app/collegedb`. Import it with `import type { CollegeDBConfig } from '@earth-app/collegedb';`.

## Types

The types below are exported from the package root. The option and result types used by the functions in [API reference](api-reference.md) are exported too; their fields are documented in the source files that declare them.

| Type                      | Purpose                                                                                                       |
| ------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `CollegeDBConfig`         | The configuration object described in [Config Fields](#config-fields).                                        |
| `Env`                     | Workers bindings: `KV`, `ShardCoordinator`, an optional `HYPERDRIVE` binding, and shard bindings by name.     |
| `ShardingStrategy`        | One strategy: `'round-robin'`, `'random'`, `'hash'` or `'location'`.                                          |
| `MixedShardingStrategy`   | `{ read, write }` pair of `ShardingStrategy` values.                                                          |
| `OperationType`           | `'read'` or `'write'`. Selects the strategy in a mixed configuration.                                         |
| `D1Region`                | Region code, listed in [Regions and Shard Locations](#regions-and-shard-locations).                           |
| `ShardLocation`           | `{ region, priority? }`. Higher `priority` values are preferred.                                              |
| `ShardState`              | Per-binding facts used by `shardState`.                                                                       |
| `EligibilityContext`      | Argument to the `eligible` predicate: `key`, `operation`, `tags` and `state`.                                 |
| `ReadConsistency`         | `{ mode: 'primary' }`, `{ mode: 'session', bookmarks? }` or `{ mode: 'eventual' }`.                           |
| `ReadOptions`             | Options for read functions: `consistency`, and `bestEffort` for move lookups.                                 |
| `ConsistencyCapabilities` | Which read modes a shard can honor: `primary`, `session`, `eventual`.                                         |
| `KVStorage`               | The key-value contract. Adapters in [Providers](providers.md) implement it.                                   |
| `KVListResult`            | Result of a KV list call: `keys`, and an optional `cursor` and `list_complete`.                               |
| `SQLDatabase`             | The SQL contract a shard implements.                                                                          |
| `SQLDialect`              | `'sqlite'`, `'postgres'` or `'mysql'`.                                                                        |
| `SQLSession`              | A read session with `prepare` and `getBookmark`.                                                              |
| `PreparedStatement`       | `bind`, `run`, `all` and `first`, as the SQL contract defines them.                                           |
| `BatchStatement`          | A SQL string with its bindings, for `runBatch`.                                                               |
| `QueryResult`             | Result of a statement: `success`, `results`, `meta`, and an optional `error`.                                 |
| `QueryResultMeta`         | `duration`, and when available `changes` and `last_row_id`. Read consistency adds `bookmark` and `bookmarks`. |
| `ShardStats`              | `binding`, `count` and an optional `lastUpdated`, as returned by `getShardStats`.                             |
| `ShardMapping`            | Stored key-to-shard record: `shard`, `createdAt`, `updatedAt` and an optional `originalKey`.                  |
| `ShardCoordinatorState`   | Durable Object state: known shards, shard statistics, strategy, round-robin counter and sequence counters.    |
| `CollegeDBClient`         | The object returned by `createClient`.                                                                        |

## Configuration Templates

Each template is a complete configuration. Open the one that matches your strategy and copy it.

<details>
<summary>Hash strategy</summary>

```typescript
{
	kv: env.KV,
	strategy: 'hash',
	shards: { 'db-1': env.DB_1, 'db-2': env.DB_2 }
}
```

</details>

<details>
<summary>Location strategy</summary>

```typescript
{
	kv: env.KV,
	strategy: 'location',
	targetRegion: 'wnam',
	shardLocations: {
		'db-west': { region: 'wnam', priority: 2 },
		'db-east': { region: 'enam', priority: 1 }
	},
	shards: { 'db-west': env.DB_WEST, 'db-east': env.DB_EAST }
}
```

</details>

<details>
<summary>Round-robin strategy with a coordinator</summary>

```typescript
{
	kv: env.KV,
	coordinator: env.ShardCoordinator,
	strategy: 'round-robin',
	shards: { 'db-1': env.DB_1, 'db-2': env.DB_2, 'db-3': env.DB_3 }
}
```

</details>

<details>
<summary>Mixed strategy: hash reads, location writes</summary>

```typescript
{
	kv: env.KV,
	strategy: {
		read: 'hash',
		write: 'location'
	},
	targetRegion: 'wnam',
	shardLocations: {
		'db-west': { region: 'wnam', priority: 2 },
		'db-east': { region: 'enam', priority: 1 }
	},
	shards: { 'db-west': env.DB_WEST, 'db-east': env.DB_EAST }
}
```

</details>

## Strategy Selection Guide

Pick the strategy from what a new key must do. The [Sharding](sharding.md) page covers the trade-offs in detail.

| Strategy      | New key goes to                                      | Coordinator                                                                                       |
| ------------- | ---------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `hash`        | The eligible shard with the highest rendezvous score | Optional. Used when set.                                                                          |
| `round-robin` | The next eligible shard in order                     | Optional. Without one, each isolate keeps its own counter, so the split is only even per isolate. |
| `random`      | A random eligible shard                              | Optional. Used when set.                                                                          |
| `location`    | The eligible shard nearest `targetRegion`            | Optional. Used when set. Needs `targetRegion`, or it falls back to `hash`.                        |

For a mixed strategy, see the mixed-strategy example in [Sharding](sharding.md).

## Database Size

Two fields control size checks: `maxDatabaseSize` sets the limit, and `sizeCacheTtlMs` sets how long a measured size is reused. The size checks and the behavior when every shard is over the limit are described in [Sharding](sharding.md).

The size functions, such as `getDatabaseSizeForShard`, are listed in [API reference](api-reference.md).
