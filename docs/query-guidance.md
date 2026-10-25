# Query Guidance

This page covers how queries behave when they run across shards: per-shard `LIMIT`, global merge and sort, and the memory that a global page holds. It also covers the helpers for indexes, query plans and sizes, the SQL habits that keep routed and fanout queries fast, and the KV cache and secondary-index helpers. It is for developers who write list pages, search, and admin queries against a sharded table.

## Cross-Shard Pagination Behavior

`allAllShards()` and `firstAllShards()` run the exact SQL on each shard independently. SQL `LIMIT` and `OFFSET` therefore apply per shard, not to the merged result.

```ts
// with two shards, this can return up to 20 total rows (10 per shard)
const perShard = await allAllShards('SELECT * FROM posts ORDER BY created_at DESC LIMIT 10');
```

For a global merge, sort and page, use `allAllShardsGlobal()` or `firstAllShardsGlobal()`. They take `sortBy`, `sortDirection`, `offset`, `limit`, `filter`, `comparator`, `includeTotal` and `batchSize`. The offset and limit apply once, after the merge. `batchSize` sets how many shard queries run at once, with a default of 50.

```ts
import { allAllShardsGlobal, firstAllShardsGlobal } from '@earth-app/collegedb';

const page = await allAllShardsGlobal<{ id: string; created_at: number }>('SELECT id, created_at FROM posts', [], {
	sortBy: 'created_at',
	sortDirection: 'desc',
	offset: 20,
	limit: 10
});

const newest = await firstAllShardsGlobal<{ id: string; created_at: number }>('SELECT id, created_at FROM posts', [], {
	sortBy: 'created_at',
	sortDirection: 'desc'
});
```

## Memory Ceiling

A global page merges and sorts in one isolate, so it holds every matching row from every shard at once. The router reduces this when the call allows it. The rewrite applies when all of these hold:

- `sortBy` names a column.
- The call uses no `filter`, `comparator` or `includeTotal`.
- The statement has no `LIMIT`, `OFFSET`, `UNION`, `INTERSECT` or `EXCEPT`.

When they hold, CollegeDB adds an `ORDER BY` on the sort column when the statement has none, and a `LIMIT` of `offset + limit` to each shard query. Each shard then returns only the rows the page can use. A JavaScript `filter` or `comparator` can promote a row that the bound would have discarded, and `includeTotal` must count rows the page does not contain, so either one turns the rewrite off.

`paginate()` sets `includeTotal` internally. The rewrite never applies to it, and every matching row from every shard is fetched into the isolate. Use `allAllShardsGlobal()` with `offset` and `limit` when the page does not need a total.

Workers caps an isolate at 128 MB. For a large result set, filter in SQL rather than in a `filter` callback. To page a large table, count the rows with `count()` and read keyed ranges, rather than sorting every row globally.

## Operational Helpers

Some helpers run across every shard, for schema, plan and size checks:

```ts
import { countAllShards, explainAllShards, getDatabaseSizesAllShards, indexAllShards } from '@earth-app/collegedb';

// create the same index on every shard; IF NOT EXISTS is on by default
await indexAllShards('posts', [{ name: 'user_id' }, { name: 'created_at', order: 'DESC' }], {
	ifNotExists: true
});

// query plan for the statement on every shard
const plans = await explainAllShards('SELECT * FROM posts WHERE user_id = ? ORDER BY created_at DESC LIMIT 20', ['user-123']);

// row count across all shards, with a count per shard
const counts = await countAllShards('posts');

// size measurement per shard
const sizes = await getDatabaseSizesAllShards();
```

Use them in this order:

- `indexAllShards()` keeps the index set the same on every shard.
- `explainAllShards()` shows the plan before an index is added or a query shape changes.
- `countAllShards()` and `getDatabaseSizesAllShards()` feed dashboards and rebalancing thresholds.

## Creating Targeted Indexes

Index the columns that a query filters or sorts on, in the order the query uses them. The statements below work on SQLite, D1 and PostgreSQL. `indexAllShards()` creates the same index on every shard from one call.

```sql
CREATE INDEX IF NOT EXISTS idx_posts_user_id_created_at ON posts(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
```

## Inspecting Query Plans and Statistics

The statements below use SQLite syntax, which D1 also accepts:

```sql
EXPLAIN QUERY PLAN SELECT * FROM posts WHERE user_id = ? ORDER BY created_at DESC LIMIT 20;
PRAGMA optimize;
ANALYZE;
```

`explainAllShards()` prefixes the statement with one of three forms, chosen by `mode`. The default, `query-plan`, prefixes `EXPLAIN QUERY PLAN`, which SQLite and D1 accept. `raw` prefixes `EXPLAIN`, and `analyze` prefixes `EXPLAIN ANALYZE`. On PostgreSQL or MySQL shards, pass `mode: 'raw'` or `mode: 'analyze'`.

## Use Bound Parameters

Bind user input as parameters. Do not put it into the SQL string.

```ts
import { allAllShards, first } from '@earth-app/collegedb';

// safe: parameterized query, routed on the key
await first('user-123', 'SELECT * FROM users WHERE id = ? AND email = ?', ['user-123', email]);

// safe: parameterized query, fanout across shards because email is not the key
await allAllShards('SELECT * FROM users WHERE email = ?', [email]);

// avoid string interpolation with user input
// bad: `... WHERE email = '${email}'`
```

## Design Search for Scale

- Index exact-match and prefix search fields.
- Use bounded result sets (`LIMIT`) with a stable sort. Break ties with a unique column, so pages do not repeat or skip rows.
- For a global search page, run a lightweight per-shard statement and merge it with `allAllShardsGlobal()`.

## Pagination Guidance

- Routed single-key reads (`first()`, `all()`) paginate in SQL on one shard, so the page is predictable.
- Fanout helpers (`allAllShards()`, `firstAllShards()`) paginate per shard. Each shard applies its own `LIMIT`.
- User-facing global pages use `allAllShardsGlobal()`, or `paginate()` when the total count is needed, so that `offset` and `limit` apply once after the merge. Read the memory ceiling above before choosing `paginate()` for a large table.

## KV Cache and Secondary-Index Lookups

`cached()` wraps a read with the configured KV store. The expiry is stored with the value, so the TTL works on any KV backend. `invalidate()` deletes every key under a prefix and returns the number of keys it deleted.

`setLookup()`, `getLookup()` and `deleteLookup()` maintain a secondary index, such as an email hash to a customer id. Their keys live under the `collegedb:lookup:` namespace by default. They are separate from the lookup keys that `firstByLookupKey()` uses for shard routing.

```ts
import { cached, deleteLookup, getLookup, invalidate, setLookup } from '@earth-app/collegedb';

const user = await cached(
	`user:${id}`,
	async () => {
		// load the user row from your database and return it
		return userRow;
	},
	{ ttl: 3600 }
);
await invalidate('tickets:list:'); // after a write

await setLookup(emailHash, String(customerId));
const found = await getLookup(emailHash);
await deleteLookup(oldEmailHash);
```

`ttl` is in seconds. Omit it, or set it to zero or less, to cache without expiry. A corrupt entry is deleted and recomputed.

Concurrent `cached()` calls for the same key in the same isolate share one fetch, so a miss under load runs the fetcher once. Pass `fetchTimeoutMs` to bound how long callers wait. Callers still waiting when the limit passes reject with `CACHE_FETCH_TIMEOUT`. The fetch is not cancelled, and if it resolves later, its value is still stored.
