# API Reference

This page lists the functions CollegeDB exports from `src/index.ts`, grouped by purpose, and every error code the library throws. Use it when you need a signature or want to know what a code means. Configuration fields and types are in [Configuration](configuration.md). Setup, examples and the first call are in [Getting started](getting-started.md).

Optional parameters are marked with `?`. Parameters with a default show it as `= value`. Generic type parameters are omitted from the tables.

## Client and Setup

| Function                           | Parameters                                          | Purpose                                                                                                                                                    |
| ---------------------------------- | --------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `collegedb(config, callback)`      | `CollegeDBConfig, () => T`                          | Runs a callback with the configuration active. Without `AsyncLocalStorage`, overlapping callbacks with different configurations throw `CONCURRENT_CONFIG`. |
| `initialize(config)`               | `CollegeDBConfig`                                   | Sets the global configuration and starts background work (known-shard sync and auto-migration) unless `disableAutoMigration` is set.                       |
| `initializeAsync(config)`          | `CollegeDBConfig`                                   | Asynchronous form of `initialize`.                                                                                                                         |
| `initializeFromEnv(env, options?)` | `Record<string, unknown>, InitializeFromEnvOptions` | Builds the configuration from a Workers `env`. Detects `env.KV` and the shard bindings by prefix. Returns the shard binding names.                         |
| `createClient(config)`             | `CollegeDBConfig`                                   | Returns a client with its own state and the same operations as the module-level functions.                                                                 |
| `getActiveConfig()`                | none                                                | Returns the active configuration, or `null` before initialization.                                                                                         |
| `isInitialized()`                  | none                                                | Reports whether the current context is initialized.                                                                                                        |
| `resetConfig()`                    | none                                                | Clears the global configuration. For tests.                                                                                                                |
| `createSchema(db, schema)`         | `SQLDatabase, string`                               | Runs schema SQL on one shard.                                                                                                                              |
| `ensureSchema(schema, options?)`   | `string or string[], EnsureSchemaOptions`           | Creates the schema on every configured shard. Safe to call again.                                                                                          |

## Reads and Writes

Each function here resolves the shard from a primary key or from the statement itself. See [Reads and writes](reads-and-writes.md) for consistency modes and the planner rules.

| Function                                                | Parameters                                                             | Purpose                                                                                                                                                        |
| ------------------------------------------------------- | ---------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `prepare(key, sql)`                                     | `string, string`                                                       | Prepares a statement on the shard that owns `key`.                                                                                                             |
| `run(key, sql, bindings = [])`                          | `string, string, any[]`                                                | Executes one statement on the shard that owns `key`.                                                                                                           |
| `first(key, sql, bindings = [], options?)`              | `string, string, any[], ReadOptions`                                   | Returns the first row from the shard that owns `key`.                                                                                                          |
| `all(key, sql, bindings = [], options?)`                | `string, string, any[], ReadOptions`                                   | Returns every row from the shard that owns `key`.                                                                                                              |
| `firstResilient(key, sql, bindings = [], options?)`     | `string, string, any[], ReadOptions`                                   | Reads the first row by key. On a miss, falls back to a scan of every shard.                                                                                    |
| `query(sql, bindings = [])`                             | `string, any[]`                                                        | Executes a statement and takes the routing key from the statement. Throws `UNROUTABLE_QUERY` when the key cannot be proven, unless `onUnroutable` is `fanout`. |
| `queryFirst(sql, bindings = [], options?)`              | `string, any[], ReadOptions`                                           | Like `query`, returns the first matching row or `null`.                                                                                                        |
| `queryAll(sql, bindings = [], options?)`                | `string, any[], ReadOptions`                                           | Like `query`, returns every matching row.                                                                                                                      |
| `insert(sql, bindings = [], options?)`                  | `string, any[], InsertOptions`                                         | Inserts on an automatically chosen shard and returns the generated primary key.                                                                                |
| `insertShard(shard, sql, bindings = [], options?)`      | `string, string, any[], InsertOptions`                                 | Same as `insert`, on the named shard.                                                                                                                          |
| `insertInto(key, table, values, options?)`              | `string, string, ColumnValues, BuildInsertOptions`                     | Inserts a row built from an object, on the shard that owns `key`.                                                                                              |
| `insertReturning(key, table, values, options?)`         | `string, string, ColumnValues, BuildInsertOptions`                     | Inserts a row, reads it back and returns it.                                                                                                                   |
| `updateRow(key, table, values, where, options?)`        | `string, string, ColumnValues, ColumnValues, CrudReturningOptions`     | Updates the rows matching a WHERE map.                                                                                                                         |
| `patch(key, table, id, values, options?)`               | `string, string, string or number, ColumnValues, IdColumnOptions`      | Updates one row by id. Wraps `updateRow`.                                                                                                                      |
| `deleteRow(key, table, where)`                          | `string, string, ColumnValues`                                         | Deletes the rows matching a WHERE map.                                                                                                                         |
| `deleteById(key, table, id, options?)`                  | `string, string, string or number, IdColumnOptions`                    | Deletes one row by id. Wraps `deleteRow`.                                                                                                                      |
| `upsert(key, table, values, conflictColumns, options?)` | `string, string, ColumnValues, string or string[], BuildUpsertOptions` | Inserts a row, or updates the conflicting columns when the key already exists.                                                                                 |
| `batch(entries)`                                        | `BatchEntry[]`                                                         | Resolves each entry to a shard and returns the results in submission order.                                                                                    |
| `nextId(table, options?)`                               | `string, NextIdOptions`                                                | Returns the next monotonic integer id for a table. See [Generated ids](generated-ids.md).                                                                      |
| `count(key, table, options?)`                           | `string, string, ReadOptions`                                          | Counts rows on the shard that owns `key`.                                                                                                                      |
| `explain(key, sql, bindings = [], options?)`            | `string, string, any[], ExplainOptions`                                | Runs an explain on the shard that owns `key`.                                                                                                                  |
| `paginate(sql, bindings = [], options?)`                | `string, any[], PaginateOptions`                                       | Runs a query across all shards. Returns one page and the total match count.                                                                                    |

## Shard-Targeted and All-Shard Queries

These functions skip key routing. Use them for administration or for reads that span shards. See [Query guidance](query-guidance.md).

| Function                                                       | Parameters                                                 | Purpose                                                                    |
| -------------------------------------------------------------- | ---------------------------------------------------------- | -------------------------------------------------------------------------- |
| `runShard(shard, sql, bindings = [])`                          | `string, string, any[]`                                    | Runs a statement on a named shard.                                         |
| `allShard(shard, sql, bindings = [], options?)`                | `string, string, any[], ReadOptions`                       | Returns every row from a named shard.                                      |
| `firstShard(shard, sql, bindings = [], options?)`              | `string, string, any[], ReadOptions`                       | Returns the first row from a named shard.                                  |
| `countShard(shard, table, options?)`                           | `string, string, ReadOptions`                              | Counts rows for a table on a named shard.                                  |
| `runAllShards(sql, bindings = [], batchSize = 50)`             | `string, any[], number`                                    | Runs a statement on every shard and returns each result.                   |
| `allAllShards(sql, bindings = [], batchSize = 50, options?)`   | `string, any[], number, ReadOptions`                       | Returns every row from every shard. SQL pagination applies per shard.      |
| `firstAllShards(sql, bindings = [], batchSize = 50, options?)` | `string, any[], number, ReadOptions`                       | Returns the first matching row from each shard.                            |
| `allAllShardsGlobal(sql, bindings = [], options = {})`         | `string, any[], GlobalAllShardsOptions`                    | Runs on every shard, then merges, sorts and paginates the combined result. |
| `firstAllShardsGlobal(sql, bindings = [], options = {})`       | `string, any[], Omit<GlobalAllShardsOptions, 'limit'>`     | Like `allAllShardsGlobal`, returns the first row after the global step.    |
| `countAllShards(table, batchSize = 50, options?)`              | `string, number, ReadOptions`                              | Counts rows per shard and the global total.                                |
| `explainShard(shard, sql, bindings = [], options?)`            | `string, string, any[], Omit<ExplainOptions, 'batchSize'>` | Runs an explain on a named shard.                                          |
| `explainAllShards(sql, bindings = [], options = {})`           | `string, any[], ExplainOptions`                            | Runs an explain on every shard.                                            |

## Indexes

| Function                                       | Parameters                                                              | Purpose                                        |
| ---------------------------------------------- | ----------------------------------------------------------------------- | ---------------------------------------------- |
| `index(key, table, columns, options?)`         | `string, string, string or IndexColumnDefinition[], CreateIndexOptions` | Creates an index on the shard that owns `key`. |
| `indexShard(shard, table, columns, options?)`  | `string, string, string or IndexColumnDefinition[], CreateIndexOptions` | Creates an index on a named shard.             |
| `indexAllShards(table, columns, options = {})` | `string, string or IndexColumnDefinition[], CreateIndexOptions`         | Creates an index on every shard.               |

## Lookup Keys

A lookup key maps a secondary value to a primary key. The `*ByLookupKey` functions read the mapping and fall back to a scan of every shard.

| Function                                                                    | Parameters                                   | Purpose                                                                 |
| --------------------------------------------------------------------------- | -------------------------------------------- | ----------------------------------------------------------------------- |
| `setLookup(key, value, options = {})`                                       | `string, string, LookupOptions`              | Stores or overwrites a secondary value.                                 |
| `getLookup(key, options = {})`                                              | `string, LookupOptions`                      | Reads a secondary value, or `null`.                                     |
| `deleteLookup(key, options = {})`                                           | `string, LookupOptions`                      | Deletes a secondary value.                                              |
| `firstByLookupKey(lookupKey, sql, bindings = [], batchSize = 50, options?)` | `string, string, any[], number, ReadOptions` | Returns the first row found through a lookup key, with fanout fallback. |
| `allByLookupKey(lookupKey, sql, bindings = [], batchSize = 50, options?)`   | `string, string, any[], number, ReadOptions` | Returns every row found through a lookup key, with fanout fallback.     |
| `invalidateMappingCache(key?)`                                              | `string?`                                    | Drops cached mappings so the next read loads them from KV.              |

## Moves and Recovery

Moves need a journal once the configuration has more than one shard. Set `journalShard`. See [Shard moves](shard-moves.md) for the protocol.

| Function                                                         | Parameters                                  | Purpose                                                                                     |
| ---------------------------------------------------------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `reassignShard(primaryKey, newBinding, tableName, options = {})` | `string, string, string, MoveRecordOptions` | Moves one key and its row to another shard.                                                 |
| `rebalance(table, options = {})`                                 | `string, { concurrency?, dryRun? }`         | Moves stored mappings onto the shard the placement function computes.                       |
| `recoverMigrations(options = {})`                                | `{ olderThanMs? }`                          | Finishes the unfinished moves recorded in the journal. Returns a `RecoveryReport`.          |
| `verifyMigrations(options = {})`                                 | `{ repair? }`                               | Checks the journal against the shards. Returns a `VerifyReport`.                            |
| `getMigrationStatus(key, table?)`                                | `string, string?`                           | Returns what the journal records for a key, or `null`.                                      |
| `purgeRetired()`                                                 | none                                        | Clears archived copies of moved rows once they are safe to remove. Returns a `PurgeReport`. |

## Placement, Topology and Eligibility

| Function                                      | Parameters                        | Purpose                                                                                                                               |
| --------------------------------------------- | --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `listKnownShards()`                           | none                              | Lists the shard binding names known to the system.                                                                                    |
| `getShardStats()`                             | none                              | Returns key counts and timestamps for each known shard.                                                                               |
| `publishEligibility()`                        | none                              | Re-evaluates eligibility. Publishes a new placement snapshot when the eligible set changed. Returns `{ eligible, published, epoch }`. |
| `pruneEpochs(options)`                        | `PruneOptions`                    | Drops placement epochs that no row depends on. Returns a `PruneReport`.                                                               |
| `reconcileAllocations(options = {})`          | `ReconcileOptions`                | Compares the coordinator's allocation ledger with the routing metadata. Needs a `coordinator`. See [Cloudflare](cloudflare.md).       |
| `planQuery(sql, bindings = [], options = {})` | `string, any[], PlanQueryOptions` | Returns the routing key or keys that a statement names.                                                                               |
| `unroutableError(sql)`                        | `string`                          | Builds the `UNROUTABLE_QUERY` error that the planner raises.                                                                          |

The placement primitives are pure functions over a manifest. They take no configuration. See [Sharding](sharding.md) for computed placement.

| Function                                                   | Parameters                                        | Purpose                                                                   |
| ---------------------------------------------------------- | ------------------------------------------------- | ------------------------------------------------------------------------- |
| `hrwShard(key, shards)`                                    | `string, string[]`                                | Picks a shard by rendezvous hashing.                                      |
| `legacyModuloShard(key, shards)`                           | `string, string[]`                                | Modulo placement, kept for keys placed by the earlier algorithm.          |
| `createManifest(shards, algorithm)`                        | `string[], PlacementAlgorithm`                    | Builds the first placement manifest for a deployment.                     |
| `withCurrentTopology(manifest, shards, algorithm = 'hrw')` | `PlacementManifest, string[], PlacementAlgorithm` | Returns a manifest whose newest epoch matches `shards`.                   |
| `candidateShards(key, manifest, available)`                | `string, PlacementManifest, string[]`             | Lists every shard a key could be on, newest epoch first, without repeats. |
| `shardForEpoch(key, epoch)`                                | `string, PlacementEpoch`                          | Applies one epoch's algorithm to a key.                                   |

## Sizes

These report the size of the database behind each shard. The `maxDatabaseSize` limit and its size checks are described in [Sharding](sharding.md).

| Function                                    | Parameters | Purpose                                            |
| ------------------------------------------- | ---------- | -------------------------------------------------- |
| `getDatabaseSizeForKey(key)`                | `string`   | Size in bytes of the shard that owns `key`.        |
| `getDatabaseSizeForShard(shard)`            | `string`   | Size in bytes of a named shard.                    |
| `getDatabaseSizesAllShards(batchSize = 50)` | `number`   | Size of every shard, as `ShardSizeResult` entries. |
| `getTotalDatabaseSize(batchSize = 50)`      | `number`   | Combined size in bytes across all shards.          |

## SQL Builders and Identifiers

These build SQL strings and bindings without running them. They throw on invalid input, so they can be called before any database is touched.

| Function                                                    | Parameters                                                     | Purpose                                                             |
| ----------------------------------------------------------- | -------------------------------------------------------------- | ------------------------------------------------------------------- |
| `buildInsert(table, values, options = {})`                  | `string, ColumnValues, BuildInsertOptions`                     | Builds an `INSERT` statement from a column-to-value object.         |
| `buildUpdate(table, values, where, options = {})`           | `string, ColumnValues, ColumnValues, { returning?, dialect? }` | Builds an `UPDATE` statement from a changes object and a WHERE map. |
| `buildDelete(table, where, options = {})`                   | `string, ColumnValues, { dialect? }`                           | Builds a `DELETE` statement from a WHERE map.                       |
| `buildUpsert(table, values, conflictColumns, options = {})` | `string, ColumnValues, string or string[], BuildUpsertOptions` | Builds an upsert statement for the conflict columns.                |
| `quoteIdentifier(identifier, dialect?)`                     | `string, SQLDialect?`                                          | Validates and quotes a SQL identifier, optionally schema-qualified. |
| `validateIdentifier(identifier)`                            | `string`                                                       | Validates a SQL identifier and returns its parts without quotes.    |

`PROVEN_JOURNAL_DIALECTS` is the list of dialects the move journal is proven on: `sqlite`, `postgres` and `mysql`.

## Cache and KV Helpers

| Function                             | Parameters                               | Purpose                                                                                                                        |
| ------------------------------------ | ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `cached(key, fetcher, options = {})` | `string, () => Promise<T>, CacheOptions` | Returns the cached value for `key`, or computes and stores it on a miss. `fetchTimeoutMs` bounds the fetch.                    |
| `invalidate(prefix, options = {})`   | `string, { kv?, concurrency? }`          | Deletes every cache key that starts with `prefix`.                                                                             |
| `withKVTimeout(kv, timeoutMs)`       | `KVStorage, number`                      | Wraps a KV store so each call rejects with `KV_TIMEOUT` after `timeoutMs`. `kvTimeoutMs` applies this to the configured store. |
| `KVShardMapper`                      | class                                    | Stores key-to-shard mappings in KV. Methods include `updateShardMapping` and `getKeysForShard`.                                |
| `flush()`                            | none                                     | Clears every shard mapping from KV and the coordinator. Development only.                                                      |

## Providers

Provider factories adapt a driver or client to CollegeDB's contracts. See [Providers](providers.md) for when each one applies.

| Function                                                                    | Parameters                                                                          | Purpose                                                        |
| --------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| `createPostgreSQLProvider(client, sqlTag?)`                                 | `PostgresClientLike, SQLBatchOptions` or `DrizzleClientLike, DrizzleSqlTagLike`     | Adapts a PostgreSQL client, or a Drizzle PostgreSQL database.  |
| `createMySQLProvider(client, sqlTag?)`                                      | `MySQLClientLike, SQLBatchOptions` or `DrizzleClientLike, DrizzleSqlTagLike`        | Adapts a MySQL or MariaDB client, or a Drizzle MySQL database. |
| `createSQLiteProvider(client, sqlTag?)`                                     | `SQLiteClientLike, SQLBatchOptions` or `DrizzleClientLike, DrizzleSqlTagLike`       | Adapts a SQLite or D1 client, or a Drizzle SQLite database.    |
| `createDrizzleSQLProvider(client, sqlTag, dialect?)`                        | `DrizzleClientLike, DrizzleSqlTagLike, SQLDialect`                                  | Generic Drizzle adapter.                                       |
| `createHyperdrivePostgresProvider(hyperdrive, clientFactory, options = {})` | `HyperdriveBindingLike, HyperdrivePostgresClientFactory, HyperdriveProviderOptions` | Adapts PostgreSQL through a Hyperdrive binding.                |
| `createHyperdriveMySQLProvider(hyperdrive, clientFactory, options = {})`    | `HyperdriveBindingLike, HyperdriveMySQLClientFactory, HyperdriveProviderOptions`    | Adapts MySQL or MariaDB through a Hyperdrive binding.          |
| `createWorkersKVProvider(kv, options = {})`                                 | `WorkersKVNamespaceLike, WorkersKVProviderOptions`                                  | Wraps a Workers KV namespace in the `KVStorage` contract.      |
| `createRedisKVProvider(client, options = {})`                               | `RedisLikeClient, { scanCount? }`                                                   | Adapts a Redis client to `KVStorage`.                          |
| `createValkeyKVProvider(client, options = {})`                              | `RedisLikeClient, { scanCount? }`                                                   | Adapts a Valkey client to `KVStorage`.                         |
| `createNuxtHubKVProvider(client)`                                           | `NuxtHubKVLike`                                                                     | Adapts a NuxtHub or Unstorage KV client to `KVStorage`.        |
| `toProvider(binding, options = {})`                                         | `unknown, ToProviderOptions`                                                        | Detects the provider for an arbitrary binding.                 |
| `isKVStorage(value)`                                                        | `unknown`                                                                           | Returns `true` when a value satisfies `KVStorage`.             |
| `isSQLDatabase(value)`                                                      | `unknown`                                                                           | Returns `true` when a value satisfies `SQLDatabase`.           |
| `createInMemoryKVProvider()`                                                | none                                                                                | Creates an in-memory `KVStorage`, for tests and local runs.    |
| `createInMemorySQLProvider()`                                               | none                                                                                | Creates an in-memory `SQLDatabase`, for tests and local runs.  |

`InMemoryKVStorage` and `InMemorySQLDatabase` are the classes behind those two factories.

## Drop-in Functions

These bring an existing database into CollegeDB without moving its rows. See [Drop-in](drop-in.md) for the workflow.

| Function                                                                                    | Parameters                                                                                 | Purpose                                                                                       |
| ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------- |
| `autoDetectAndMigrate(db, shard, config, options?)`                                         | `SQLDatabase, string, CollegeDBConfig, options`                                            | Detects a database that holds unmapped data, then migrates it.                                |
| `checkMigrationNeeded(db, shard, config, statusCache?)`                                     | `SQLDatabase, string, CollegeDBConfig, Map`                                                | Checks whether a shard holds data that is not mapped yet.                                     |
| `integrateExistingDatabase(db, shard, mapper, options = {})`                                | `SQLDatabase, string, KVShardMapper, IntegrationOptions`                                   | Runs a full drop-in integration of an existing database.                                      |
| `createMappingsForExistingKeys(primaryKeys, shardBindings, strategy, mapper, options = {})` | `string[], string[], 'hash' or 'round-robin' or 'random', KVShardMapper, { concurrency? }` | Writes mappings for keys that already exist.                                                  |
| `validateTableForSharding(db, table, primaryKeyColumn)`                                     | `SQLDatabase, string, string`                                                              | Checks that a table exists and has a usable primary key column.                               |
| `discoverExistingPrimaryKeys(db, table, primaryKeyColumn = 'id')`                           | `SQLDatabase, string, string`                                                              | Lists every primary key in a table.                                                           |
| `discoverExistingRecordsWithColumns(db, table, primaryKeyColumn = 'id')`                    | `SQLDatabase, string, string`                                                              | Lists the existing records with their extra columns, for multi-key mapping.                   |
| `migrateRecord(source, target, primaryKey, table, options = {})`                            | `SQLDatabase, SQLDatabase, string, string, MigrateRecordOptions`                           | Copies one row to another database. `onConflict` defaults to `fail`.                          |
| `createSchemaAcrossShards(shards, schema)`                                                  | `Record<string, SQLDatabase>, string or function`                                          | Applies a schema to every database in parallel.                                               |
| `dropSchema(db, ...tables)`                                                                 | `SQLDatabase, string[]`                                                                    | Runs `DROP TABLE IF EXISTS` for each named table. Destructive. Errors are logged, not thrown. |
| `listTables(db)`                                                                            | `SQLDatabase`                                                                              | Lists the user tables in a database.                                                          |
| `schemaExists(db, table)`                                                                   | `SQLDatabase, string`                                                                      | Checks whether the expected schema is present.                                                |
| `clearMigrationCache()`                                                                     | none                                                                                       | Clears the cache of shards already checked for migration.                                     |
| `clearShardMigrationCache(shard)`                                                           | `string`                                                                                   | Clears the cache entry for one shard.                                                         |

`IntegrationOptions.strategy` is `@deprecated`. It has no effect and will be removed in 2.0.

## Telemetry

| Function or class         | Parameters              | Purpose                                                                        |
| ------------------------- | ----------------------- | ------------------------------------------------------------------------------ |
| `setPhaseObserver(next)`  | `PhaseObserver or null` | Installs the phase observer on the default instance, or clears it with `null`. |
| `isPhaseObserverActive()` | none                    | Reports whether phase timing is active on the default instance.                |
| `PhaseCollector`          | class                   | Collects phase spans and summarizes them per phase.                            |

The `onPhase` configuration field sends the same spans to one configuration. See [Configuration](configuration.md).

## Errors

`CollegeDBError` extends `Error` and adds an optional `code` string. Check `error instanceof CollegeDBError` before reading `code`.

```typescript
import { CollegeDBError, run } from '@earth-app/collegedb';

try {
	await run('invalid-key', 'SELECT * FROM users WHERE id = ?', ['invalid-key']);
} catch (error) {
	if (error instanceof CollegeDBError) {
		console.error(`CollegeDB error (${error.code}): ${error.message}`);
	}
}
```

The Retry column uses three values:

- **Yes**: the message says to retry, or the failure is transient.
- **No**: the message says a retry will not help, or the input or configuration has to change first.
- **Not stated**: the message names no retry, and the outcome depends on the underlying failure.

### Setup and Input

| Code                               | Thrown when                                                                                                                     | Retry                                                                          |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| `NOT_INITIALIZED`                  | A routed helper runs before `initialize`, or a cache helper has no KV store.                                                    | No. Call `initialize` first.                                                   |
| `NO_KV`                            | `initializeFromEnv` finds no KV binding and none was passed.                                                                    | No. Pass `kv` or set `env.KV`.                                                 |
| `NO_SHARDS`                        | The configuration has no shards. An insert also throws it when no shard is eligible.                                            | No for an empty configuration. An insert can succeed once eligibility changes. |
| `CONCURRENT_CONFIG`                | `collegedb` runs a callback with a different configuration while another is active, and the runtime has no `AsyncLocalStorage`. | Not stated. The message says to wait, or to use `createClient`.                |
| `INVALID_CONSISTENCY`              | A read names a `consistency` mode that does not exist.                                                                          | No.                                                                            |
| `CONSISTENCY_UNSUPPORTED`          | A shard cannot serve the requested `consistency` mode.                                                                          | No. Choose a mode the shard supports.                                          |
| `BOOKMARK_IDENTITY_UNVERIFIED`     | A session read passes bookmarks for a binding that has no `id` in `shardState`.                                                 | No until `shardState` names the database id.                                   |
| `INVALID_IDENTIFIER`               | A table or column name is not a bare SQL identifier.                                                                            | No.                                                                            |
| `EMPTY_WHERE`                      | A WHERE map has no usable condition for an `UPDATE` or `DELETE` builder.                                                        | No.                                                                            |
| `EMPTY_INSERT`                     | An insert builder receives no column values.                                                                                    | No.                                                                            |
| `EMPTY_UPDATE`                     | `buildUpdate` receives no column values.                                                                                        | No.                                                                            |
| `EMPTY_CONFLICT`                   | `buildUpsert` receives no conflict column.                                                                                      | No.                                                                            |
| `INVALID_INDEX_COLUMNS`            | An index call has no columns, or a column with no name.                                                                         | No.                                                                            |
| `INVALID_TABLE_NAME`               | A migration function receives a table name that is not valid.                                                                   | No.                                                                            |
| `INVALID_QUERY_INPUT`              | The in-memory SQL backend receives a query input it does not support.                                                           | No.                                                                            |
| `MIGRATION_PK_MISSING`             | A row or snapshot lacks its primary key column.                                                                                 | No.                                                                            |
| `MIGRATION_VERSION_COLUMN_MISSING` | A snapshot lacks its version column.                                                                                            | No.                                                                            |
| `EMPTY_SOURCE_RECORD`              | A source record has no columns.                                                                                                 | No.                                                                            |
| `QUERY_FAILED`                     | The backend reports the statement as unsuccessful.                                                                              | Not stated. The message includes the backend error.                            |
| `SIZE_QUERY_FAILED`                | No size statement worked on the backend.                                                                                        | Not stated.                                                                    |

### Client Adapters and Schema

| Code                        | Thrown when                                                                                               | Retry       |
| --------------------------- | --------------------------------------------------------------------------------------------------------- | ----------- |
| `SQLITE_CLIENT_INVALID`     | A SQLite client lacks the methods the adapter needs (`execute`, or `prepare` with `run`, `all` or `get`). | No.         |
| `MYSQL_CLIENT_INVALID`      | A MySQL client lacks `execute` or `query`.                                                                | No.         |
| `DRIZZLE_CLIENT_INVALID`    | A Drizzle client lacks `run`, `execute` or `all`.                                                         | No.         |
| `NUXTHUB_KV_CLIENT_INVALID` | A NuxtHub KV client lacks a get, set, delete or key-listing method.                                       | No.         |
| `DRIZZLE_BINDINGS_MISMATCH` | A Drizzle statement has a different placeholder count than its bindings.                                  | No.         |
| `SCHEMA_CREATION_FAILED`    | Creating the schema on one shard fails.                                                                   | Not stated. |
| `SCHEMA_MIGRATION_FAILED`   | A schema migration fails.                                                                                 | Not stated. |
| `DISCOVERY_FAILED`          | Scanning a table for primary keys or records fails.                                                       | Not stated. |
| `MIGRATION_VERIFY_FAILED`   | A destination copy does not match its source row.                                                         | Not stated. |

### KV and Cache

| Code                   | Thrown when                                                                                                 | Retry                                             |
| ---------------------- | ----------------------------------------------------------------------------------------------------------- | ------------------------------------------------- |
| `KV_TIMEOUT`           | A KV call runs past `kvTimeoutMs`. Never resolves as "not found".                                           | Not stated. The underlying call is not cancelled. |
| `KV_JSON_PARSE_FAILED` | A stored KV value is not valid JSON.                                                                        | Not stated. A retry reads the same value.         |
| `KV_PUT_MANY_FAILED`   | A Workers KV `putMany` write fails. The message names the key.                                              | Not stated.                                       |
| `CACHE_FETCH_TIMEOUT`  | A `cached` fetch runs past `fetchTimeoutMs`. The fetch is not cancelled, and a late result is still stored. | Not stated.                                       |

### Keys and Mappings

| Code                        | Thrown when                                                                                                | Retry                                                                  |
| --------------------------- | ---------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| `SHARD_NOT_FOUND`           | A mapping or call names a binding that is not in `shards`.                                                 | No. Fix the configuration.                                             |
| `MAPPING_NOT_FOUND`         | `reassignShard`, `updateShardMapping`, `addLookupKeys` or `getAllLookupKeys` finds no mapping for the key. | No. The key has no mapping yet.                                        |
| `GENERATED_KEY_UNAVAILABLE` | An insert returns no id. Pass `idColumn` when the primary key is not named `id` or `rowid`.                | Not stated.                                                            |
| `GENERATED_KEY_COLLISION`   | A generated id is already mapped to another shard. The row this insert wrote is removed.                   | Not stated. The message says to delete the orphan row or pass `table`. |
| `UNROUTABLE_QUERY`          | The planner cannot prove a routing key and `onUnroutable` is `throw`.                                      | No. Pass the key, declare the table in `keyColumns`, or set `fanout`.  |
| `WRITE_UNRESOLVED`          | After a topology change, the probe that finds the row cannot run. The write is not applied.                | Not stated. The message says the table must exist on every shard.      |

### Coordinator

| Code                         | Thrown when                                                                            | Retry                    |
| ---------------------------- | -------------------------------------------------------------------------------------- | ------------------------ |
| `COORDINATOR_NOT_CONFIGURED` | `reconcileAllocations` runs without a `coordinator`.                                   | No. Add `coordinator`.   |
| `COORDINATOR_ERROR`          | The coordinator returns a non-OK response while reconcile lists or prunes allocations. | Not stated.              |
| `RECONCILE_VERIFY_REQUIRED`  | `reconcileAllocations` runs in repair mode without `verifyRecords`.                    | No. Add `verifyRecords`. |

### Moves and Journal

| Code                            | Thrown when                                                                                                                                                        | Retry                                                                                 |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------- |
| `JOURNAL_NOT_CONFIGURED`        | A move, `pruneEpochs`, `recoverMigrations`, `verifyMigrations`, `getMigrationStatus` or `purgeRetired` runs on a multi-shard configuration without `journalShard`. | No. Set `journalShard`.                                                               |
| `INVALID_JOURNAL_SHARD`         | `journalShard` names a binding that is not configured.                                                                                                             | No. Fix the configuration.                                                            |
| `JOURNAL_UNAVAILABLE`           | A read or write to the migration journal fails. The cause is attached.                                                                                             | Not stated.                                                                           |
| `MIGRATION_UNSUPPORTED_DIALECT` | A journaled move involves a shard on a dialect outside `PROVEN_JOURNAL_DIALECTS`.                                                                                  | No.                                                                                   |
| `MIGRATION_NO_BATCH`            | A move or guarded write needs a batch, and the shard has no `runBatch`.                                                                                            | No. Use a provider that implements `runBatch`.                                        |
| `MIGRATION_NO_ATOMIC_BATCH`     | A shard's `runBatch` does not declare `atomicBatch`.                                                                                                               | No. Use an adapter that declares it, or set `atomicBatch` after a failing-batch test. |
| `MIGRATION_IN_PROGRESS`         | Another move holds the claim on this key.                                                                                                                          | Yes. The message says to run `recoverMigrations()` or retry.                          |
| `MIGRATION_CUTOVER_PENDING`     | The key has moved off its shard, and the new owner is not published yet. The write is not applied.                                                                 | Yes. The message says to retry.                                                       |
| `MIGRATION_LOOKUP_UNAVAILABLE`  | The tombstone lookup failed (Yes), or the tombstone names a shard that is not configured (No).                                                                     | Yes for a failed lookup. No for an unknown shard.                                     |
| `MIGRATION_CHAIN_EXCEEDED`      | The move chain for a key loops back, or runs past four tombstone hops.                                                                                             | No. The message says a retry will not help.                                           |
| `MIGRATION_ABORTED`             | `reassignShard` stops with a reason. The key stays on its current shard.                                                                                           | Not stated.                                                                           |
| `MIGRATION_SOURCE_TOMBSTONED`   | The source shard holds a tombstone for the key from another move.                                                                                                  | No until `verifyMigrations()` has run.                                                |
| `MIGRATION_ROW_TOO_WIDE`        | The row needs more bind parameters than `maxBindParameters`, and no `versionColumn` was given.                                                                     | No. Pass `versionColumn`.                                                             |
| `MIGRATION_UNSUPPORTED_VALUE`   | A column value has a type that cannot be archived.                                                                                                                 | No.                                                                                   |
| `DESTINATION_CONFLICT`          | The destination holds a different row, and `onConflict` is not `replace`.                                                                                          | No. Pass `onConflict: 'replace'` to overwrite.                                        |
| `RECORD_NOT_FOUND`              | `migrateRecord` or a move finds no source row.                                                                                                                     | No.                                                                                   |
| `MIGRATION_PUBLISH_FAILED`      | The move is committed, but publishing derived state failed.                                                                                                        | Yes. Run `recoverMigrations()` to finish it.                                          |
| `MIGRATION_AMBIGUOUS`           | Recovery finds a cut-over row that does not match its archive.                                                                                                     | Not stated. Resolve the mismatch before retrying.                                     |

### Recovery

| Code                   | Thrown when                                                      | Retry                                   |
| ---------------------- | ---------------------------------------------------------------- | --------------------------------------- |
| `RECOVERY_IN_PROGRESS` | Another recovery holds the lease for this scope.                 | Yes. The message says to retry later.   |
| `RECOVERY_FENCED`      | This recovery lost its lease mid-run and made no further writes. | No. The new lease holder owns the work. |

### Placement and Topology

| Code                   | Thrown when                                                                                          | Retry                                             |
| ---------------------- | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------- |
| `PLACEMENT_UNVERIFIED` | The placement manifest cannot be confirmed because KV or the journal failed. Nothing is routed.      | Yes. The message says to retry.                   |
| `PLACEMENT_CONFLICT`   | Five manifest compare-and-swap attempts in a row lost to other writers.                              | Yes. The message says to retry.                   |
| `TOO_MANY_EPOCHS`      | The manifest already holds 8 epochs.                                                                 | No. Move keys to their owners, then prune epochs. |
| `STALE_TOPOLOGY`       | A write from an older manifest generation is refused, even after a reload. The write is not applied. | Yes. The message says to retry.                   |
| `PRUNE_REFUSED`        | `pruneEpochs` cannot prove that no row depends on an epoch. The message gives the reason.            | Not stated.                                       |
| `MANIFEST_CORRUPT`     | The stored placement manifest is not readable.                                                       | Not stated.                                       |

### Eligibility

| Code                 | Thrown when                                                                                                                                           | Retry                                         |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------- |
| `NO_ELIGIBLE_SHARD`  | No shard accepts new keys. Each one is read-only, draining, retired, or rejected by `eligible`. Thrown on allocation and on writes; reads still work. | Not stated. Eligibility must change.          |
| `ELIGIBILITY_FAILED` | The `eligible` predicate throws or rejects. A shard whose eligibility is unknown never counts as eligible.                                            | Yes. The message says to retry the operation. |

Several migration functions also fill a report with a reason code (for example `CAS_MISS`, `SOURCE_CHANGED`, `SOURCE_ROW_MISSING`, `VERIFY_FAILED`, `RETENTION_NOT_ELAPSED`). Those values are report fields, not thrown errors, so they are not listed here.

## Coordinator

The `ShardCoordinator` Durable Object and its HTTP API are described in [Cloudflare](cloudflare.md).
