# CollegeDB

> Cloudflare D1 Horizontal Sharding Router

[![TypeScript](https://img.shields.io/badge/TypeScript-5.0+-blue.svg)](https://www.typescriptlang.org/)
[![GitHub Issues](https://img.shields.io/github/issues/earth-app/CollegeDB)](https://github.com/earth-app/CollegeDB/issues)
[![Cloudflare Workers](https://img.shields.io/badge/cloudflare-workers-orange.svg)](https://workers.cloudflare.com/)
[![GitHub License](https://img.shields.io/github/license/earth-app/CollegeDB)](LICENSE)
![NPM Version](https://img.shields.io/npm/v/%40earth-app%2Fcollegedb)

A TypeScript library for **true horizontal scaling** of SQLite-style databases on Cloudflare using D1 and KV. CollegeDB distributes your data across multiple D1 databases, with each table's records split by primary key across different database instances.

CollegeDB implements **data distribution** where a single logical table is physically stored across multiple D1 databases:

```txt
env.db-east (Shard 1)
┌────────────────────────────────────────────┐
│ table users: [user-1, user-3, user-5, ...] │
│ table posts: [post-2, post-7, post-9, ...] │
└────────────────────────────────────────────┘

env.db-west (Shard 2)
┌────────────────────────────────────────────┐
│ table users: [user-2, user-4, user-6, ...] │
│ table posts: [post-1, post-3, post-8, ...] │
└────────────────────────────────────────────┘

env.db-central (Shard 3)
┌────────────────────────────────────────────┐
│ table users: [user-7, user-8, user-9, ...] │
│ table posts: [post-4, post-5, post-6, ...] │
└────────────────────────────────────────────┘
```

This allows you to:

- **Break through D1's single database limits** by spreading data across many databases
- **Improve query performance** by reducing data per database instance
- **Scale geographically** by placing shards in different regions
- **Increase write throughput** by parallelizing across multiple database instances

## 📈 Overview

CollegeDB provides a sharding layer on top of Cloudflare D1 databases, enabling you to:

- **Scale horizontally** by distributing table data across multiple D1 instances
- **Route queries automatically** based on primary key mappings
- **Maintain consistency** with KV-based shard mapping
- **Optimize for geography** with location-aware shard allocation
- **Monitor and rebalance** shard distribution
- **Handle migrations** between shards seamlessly

## 📦 Features

- **🔀 Automatic Query Routing**: Primary key → shard mapping using Cloudflare KV
- **🎯 Multiple Allocation Strategies**: Round-robin, random, or hash-based distribution
- **📊 Shard Coordination**: Durable Objects for allocation and statistics
- **🛠 Migration Support**: Move data between shards with zero downtime
- **🔄 Automatic Drop-in Replacement**: Zero-config integration with existing databases
- **🤖 Smart Migration Detection**: Automatically discovers and maps existing data
- **⚡ High Performance**: Optimized for Cloudflare Workers runtime
- **🔧 TypeScript First**: Full type safety and excellent DX

## Installation

```bash
bun add collegedb
# or
npm install collegedb
```

## Basic Usage

```typescript
import { collegedb, createSchema, run, first } from 'collegedb';

// Initialize with your Cloudflare bindings (existing databases work automatically!)
collegedb(
	{
		kv: env.KV,
		coordinator: env.ShardCoordinator,
		shards: {
			'db-east': env['db-east'], // Can be existing DB with data
			'db-west': env['db-west'] // Can be existing DB with data
		},
		strategy: 'hash'
	},
	async () => {
		// Create schema on new shards only (existing shards auto-detected)
		await createSchema(env['db-new-shard']);

		// Insert data (automatically routed to appropriate shard)
		await run('user-123', 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', ['user-123', 'Johnson', 'alice@example.com']);

		// Query data (automatically routed to correct shard, works with existing data!)
		const result = await first<User>('existing-user-456', 'SELECT * FROM users WHERE id = ?', ['existing-user-456']);

		console.log(result); // User data from existing database
	}
);
```

### Geographic Distribution Example

```typescript
import { collegedb, first, run } from 'collegedb';

// Optimize for North American users with geographic sharding
collegedb(
	{
		kv: env.KV,
		strategy: 'location',
		targetRegion: 'wnam', // Western North America
		shardLocations: {
			'db-west': { region: 'wnam', priority: 2 }, // SF - Preferred for target region
			'db-east': { region: 'enam', priority: 1 }, // NYC - Secondary
			'db-europe': { region: 'weur', priority: 0.5 } // London - Fallback
		},
		shards: {
			'db-west': env.DB_WEST,
			'db-east': env.DB_EAST,
			'db-europe': env.DB_EUROPE
		}
	},
	async () => {
		// New users will be allocated to db-west (closest to target region)
		await run('user-west-123', 'INSERT INTO users (id, name, location) VALUES (?, ?, ?)', [
			'user-west-123',
			'West Coast User',
			'California'
		]);

		// Queries are routed to the correct geographic shard
		const user = await first<User>('user-west-123', 'SELECT * FROM users WHERE id = ?', ['user-west-123']);
		console.log(`User found in optimal shard: ${user?.name}`);
	}
);
```

## Drop-in Replacement for Existing Databases

CollegeDB supports **seamless, automatic integration** with existing D1 databases that already contain data. Simply add your existing databases as shards in the configuration. CollegeDB will automatically detect existing data and create the necessary shard mappings **without requiring any manual migration steps**.

### Requirements for Drop-in Replacement

1. **Primary Keys**: All tables must have a primary key column (typically named `id`)
2. **Schema Compatibility**: Tables should use standard SQLite data types
3. **Access Permissions**: CollegeDB needs read/write access to existing databases
4. **KV Namespace**: A Cloudflare KV namespace for storing shard mappings

```typescript
import { collegedb, first, run } from 'collegedb';

// Add your existing databases as shards - that's it!
collegedb(
	{
		kv: env.KV,
		shards: {
			'db-users': env.ExistingUserDB, // Your existing database with users
			'db-orders': env.ExistingOrderDB, // Your existing database with orders
			'db-new': env.NewDB // Optional new shard for growth
		},
		strategy: 'hash'
	},
	async () => {
		// Existing data works immediately!
		const existingUser = await first('user-from-old-db', 'SELECT * FROM users WHERE id = ?', ['user-from-old-db']);

		// New data gets distributed automatically
		await run('new-user-123', 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', ['new-user-123', 'New User', 'new@example.com']);
	}
);
```

**That's it!** No migration scripts, no manual mapping creation, no downtime. Your existing data is immediately accessible through CollegeDB's sharding system.

### Manual Validation (Optional)

You can manually validate databases before integration if needed:

```typescript
import { validateTableForSharding, listTables } from 'collegedb';

// Check database structure
const tables = await listTables(env.ExistingDB);
console.log('Found tables:', tables);

// Validate each table
for (const table of tables) {
	const validation = await validateTableForSharding(env.ExistingDB, table);
	if (validation.isValid) {
		console.log(`✅ ${table}: ${validation.recordCount} records ready`);
	} else {
		console.log(`❌ ${table}: ${validation.issues.join(', ')}`);
	}
}
```

### Manual Data Discovery (Optional)

If you want to inspect existing data before automatic migration:

```typescript
import { discoverExistingPrimaryKeys } from 'collegedb';

// Discover all user IDs in existing users table
const userIds = await discoverExistingPrimaryKeys(env.ExistingDB, 'users');
console.log(`Found ${userIds.length} existing users`);

// Custom primary key column
const orderIds = await discoverExistingPrimaryKeys(env.ExistingDB, 'orders', 'order_id');
```

### Manual Integration (Optional)

For complete control over the integration process:

```typescript
import { integrateExistingDatabase, KVShardMapper } from 'collegedb';

const mapper = new KVShardMapper(env.KV);

// Integrate your existing database
const result = await integrateExistingDatabase(
	env.ExistingDB, // Your existing D1 database
	'db-primary', // Shard name for this database
	mapper, // KV mapper instance
	{
		tables: ['users', 'posts', 'orders'], // Tables to integrate
		primaryKeyColumn: 'id', // Primary key column name
		strategy: 'hash', // Allocation strategy for future records
		addShardMappingsTable: true, // Add CollegeDB metadata table
		dryRun: false // Set true for testing
	}
);

if (result.success) {
	console.log(`✅ Integrated ${result.totalRecords} records from ${result.tablesProcessed} tables`);
} else {
	console.error('Integration issues:', result.issues);
}
```

After integration, initialize CollegeDB with your existing databases as shards:

```typescript
import { initialize, first } from 'collegedb';

// Include existing databases as shards
initialize({
	kv: env.KV,
	coordinator: env.ShardCoordinator,
	shards: {
		'db-primary': env.ExistingDB, // Your integrated existing database
		'db-secondary': env.AnotherExistingDB, // Another existing database
		'db-new': env.NewDB // Optional new shard for growth
	},
	strategy: 'hash'
});

// Existing data is now automatically routed!
const user = await first('existing-user-123', 'SELECT * FROM users WHERE id = ?', ['existing-user-123']);
```

### Complete Drop-in Example

The simplest possible integration - just add your existing databases:

```typescript
import { initialize, first, run } from 'collegedb';

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		// Step 1: Initialize with existing databases (automatic migration happens here!)
		initialize({
			kv: env.KV,
			shards: {
				'db-users': env.ExistingUserDB, // Your existing database with users
				'db-orders': env.ExistingOrderDB, // Your existing database with orders
				'db-new': env.NewDB // New shard for future growth
			},
			strategy: 'hash'
		});

		// Step 2: Use existing data immediately - no migration needed!
		// Supports typed queries, inserts, updates, deletes, etc.
		const existingUser = await first<User>('user-from-old-db', 'SELECT * FROM users WHERE id = ?', ['user-from-old-db']);

		// Step 3: New data gets distributed automatically
		await run('new-user-123', 'INSERT INTO users (id, name, email) VALUES (?, ?, ?)', ['new-user-123', 'New User', 'new@example.com']);

		return new Response(
			JSON.stringify({
				existingUser: existingUser.results[0],
				message: 'Automatic drop-in replacement successful!'
			})
		);
	}
};
```

### Manual Integration Example

If your tables use different primary key column names:

```typescript
// For tables with custom primary key columns
const productIds = await discoverExistingPrimaryKeys(env.ProductDB, 'products', 'product_id');
const sessionIds = await discoverExistingPrimaryKeys(env.SessionDB, 'sessions', 'session_key');
```

Integrate only specific tables from existing databases:

```typescript
const result = await integrateExistingDatabase(env.ExistingDB, 'db-legacy', mapper, {
	tables: ['users', 'orders'] // Only integrate these tables
	// Skip 'temp_logs', 'cache_data', etc.
});
```

Test integration without making changes:

```typescript
const testResult = await integrateExistingDatabase(env.ExistingDB, 'db-test', mapper, {
	dryRun: true // No actual mappings created
});

console.log(`Would process ${testResult.totalRecords} records from ${testResult.tablesProcessed} tables`);
```

### Performance Impact

- **One-time Setup**: Migration detection runs once per shard
- **Minimal Overhead**: Only scans table metadata and sample records
- **Cached Results**: Subsequent operations have no migration overhead
- **Async Processing**: Doesn't block application startup or queries

```typescript
// Simple rollback - clear all mappings
import { KVShardMapper } from 'collegedb';
const mapper = new KVShardMapper(env.KV);
await mapper.clearAllMappings(); // Returns to pre-migration state

// Or clear cache to force re-detection
import { clearMigrationCache } from 'collegedb';
clearMigrationCache(); // Forces fresh migration check
```

## Troubleshooting

### Tables without Primary Keys

```typescript
// Error: Primary key column 'id' not found
// Solution: Add primary key to existing table
await db.prepare(`ALTER TABLE legacy_table ADD COLUMN id TEXT PRIMARY KEY`).run();
```

### Large Database Integration

```typescript
// For very large databases, integrate in batches
const allTables = await listTables(env.LargeDB);
const batchSize = 2;

for (let i = 0; i < allTables.length; i += batchSize) {
	const batch = allTables.slice(i, i + batchSize);
	await integrateExistingDatabase(env.LargeDB, 'db-large', mapper, {
		tables: batch
	});
}
```

### Mixed Primary Key Types

```typescript
// Handle different primary key column names per table
const customIntegration = {
	users: 'user_id',
	orders: 'order_number',
	products: 'sku'
};

for (const [table, pkColumn] of Object.entries(customIntegration)) {
	const keys = await discoverExistingPrimaryKeys(env.DB, table, pkColumn);
	await createMappingsForExistingKeys(keys, ['db-shard1'], 'hash', mapper);
}
```

## 📚 API Reference

| Function                       | Description                                  | Parameters               |
| ------------------------------ | -------------------------------------------- | ------------------------ |
| `collegedb(config, callback)`  | Initialize CollegeDB, then run a callback    | `CollegeDBConfig, ()=>T` |
| `initialize(config)`           | Initialize CollegeDB with configuration      | `CollegeDBConfig`        |
| `createSchema(d1)`             | Create database schema on a D1 instance      | `D1Database`             |
| `prepare(key, sql)`            | Prepare a SQL statement for execution        | `string, string`         |
| `run(key, sql, bindings)`      | Execute a SQL query with primary key routing | `string, string, any[]`  |
| `first(key, sql, bindings)`    | Execute a SQL query and return first result  | `string, string, any[]`  |
| `all(key, sql, bindings)`      | Execute a SQL query and return all results   | `string, string, any[]`  |
| `reassignShard(key, newShard)` | Move primary key to different shard          | `string, string`         |
| `listKnownShards()`            | Get list of available shards                 | `void`                   |
| `getShardStats()`              | Get statistics for all shards                | `void`                   |

### Drop-in Replacement Functions

| Function                                  | Description                                             | Parameters                     |
| ----------------------------------------- | ------------------------------------------------------- | ------------------------------ |
| `autoDetectAndMigrate(d1, shard, config)` | **NEW**: Automatically detect and migrate existing data | `D1Database, string, config`   |
| `checkMigrationNeeded(d1, shard, config)` | **NEW**: Check if database needs migration              | `D1Database, string, config`   |
| `validateTableForSharding(d1, table)`     | Check if table is suitable for sharding                 | `D1Database, string`           |
| `discoverExistingPrimaryKeys(d1, table)`  | Find all primary keys in existing table                 | `D1Database, string`           |
| `integrateExistingDatabase(d1, shard)`    | Complete drop-in integration of existing DB             | `D1Database, string, mapper`   |
| `createMappingsForExistingKeys(keys)`     | Create shard mappings for existing keys                 | `string[], string[], strategy` |
| `listTables(d1)`                          | Get list of tables in database                          | `D1Database`                   |
| `clearMigrationCache()`                   | Clear automatic migration cache                         | `void`                         |

## 🏗 Architecture

```txt
┌─────────────────────────────────────────────────────────────┐
│                    Cloudflare Worker                        │
├─────────────────────────────────────────────────────────────┤
│                     CollegeDB Router                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │     KV      │  │  Durable    │  │   Query Router      │  │
│  │  Mappings   │  │  Objects    │  │                     │  │
│  │             │  │ (Optional)  │  │                     │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   D1 East   │  │  D1 West    │  │    D1 Central       │  │
│  │   Shard     │  │   Shard     │  │     Shard           │  │
│  │             │  │             │  │   (Optional)        │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Query Received**: Application sends query with primary key
2. **Shard Resolution**: CollegeDB checks KV for existing mapping or allocates new shard
3. **Query Execution**: SQL executed on appropriate D1 database
4. **Response**: Results returned to application

### Shard Allocation Strategies

- **Hash**: Consistent hashing for deterministic shard selection
- **Round-Robin**: Evenly distribute new keys across shards
- **Random**: Random shard selection for load balancing
- **Location**: Geographic proximity-based allocation for optimal latency

## 🌐 Cloudflare Setup

### 1. Create D1 Databases

```bash
# Create multiple D1 databases for sharding
wrangler d1 create collegedb-east
wrangler d1 create collegedb-west
wrangler d1 create collegedb-central
```

### 2. Create KV Namespace

```bash
# Create KV namespace for shard mappings
wrangler kv namespace create "KV"
```

### 3. Configure wrangler.toml

```toml
[[d1_databases]]
binding = "db-east"
database_name = "collegedb-east"
database_id = "your-database-id"

[[d1_databases]]
binding = "db-west"
database_name = "collegedb-west"
database_id = "your-database-id"

[[kv_namespaces]]
binding = "KV"
id = "your-kv-namespace-id"

[[durable_objects.bindings]]
name = "ShardCoordinator"
class_name = "ShardCoordinator"
```

### 4. Deploy

```bash
# Deploy to Cloudflare Workers
wrangler deploy

# Deploy with environment
wrangler deploy --env production
```

## 📊 Monitoring and Maintenance

### Shard Statistics

```typescript
import { getShardStats, listKnownShards } from 'collegedb';

// Get detailed statistics
const stats = await getShardStats();
console.log(stats);
// [
//   { binding: 'db-east', count: 1542 },
//   { binding: 'db-west', count: 1458 }
// ]

// List available shards
const shards = await listKnownShards();
console.log(shards); // ['db-east', 'db-west']
```

### Shard Rebalancing

```typescript
import { reassignShard } from 'collegedb';

// Move a primary key to a different shard
await reassignShard('user-123', 'db-west');
```

### Health Monitoring

Monitor your CollegeDB deployment by tracking:

- **Shard distribution balance**
- **Query latency per shard**
- **Error rates and failed queries**
- **KV operation metrics**

## � Performance Analysis

### Scaling Performance Comparison

CollegeDB provides significant performance improvements through horizontal scaling. Here are mathematical estimates comparing single D1 database vs CollegeDB with different shard counts:

#### Query Performance (SELECT operations)

| Configuration           | Query Latency\* | Concurrent Queries      | Throughput Gain |
| ----------------------- | --------------- | ----------------------- | --------------- |
| Single D1               | ~50-80ms        | Limited by D1 limits    | 1x (baseline)   |
| CollegeDB (10 shards)   | ~55-85ms        | 10x parallel capacity   | ~8-9x           |
| CollegeDB (100 shards)  | ~60-90ms        | 100x parallel capacity  | ~75-80x         |
| CollegeDB (1000 shards) | ~65-95ms        | 1000x parallel capacity | ~650-700x       |

\*Includes KV lookup overhead (~5-15ms)

#### Write Performance (INSERT/UPDATE operations)

| Configuration           | Write Latency\* | Concurrent Writes  | Throughput Gain |
| ----------------------- | --------------- | ------------------ | --------------- |
| Single D1               | ~80-120ms       | ~50 writes/sec     | 1x (baseline)   |
| CollegeDB (10 shards)   | ~90-135ms       | ~450 writes/sec    | ~9x             |
| CollegeDB (100 shards)  | ~95-145ms       | ~4,200 writes/sec  | ~84x            |
| CollegeDB (1000 shards) | ~105-160ms      | ~35,000 writes/sec | ~700x           |

\*Includes KV mapping creation/update overhead (~10-25ms)

### Strategy-Specific Performance

#### Hash Strategy

- **Best for**: Consistent performance, even data distribution
- **Latency**: Lowest overhead (no coordinator calls)
- **Throughput**: Optimal for high-volume scenarios

| Shards | Avg Latency | Distribution Quality | Coordinator Dependency |
| ------ | ----------- | -------------------- | ---------------------- |
| 10     | +5ms        | Excellent            | None                   |
| 100    | +5ms        | Excellent            | None                   |
| 1000   | +5ms        | Excellent            | None                   |

#### Round-Robin Strategy

- **Best for**: Guaranteed even distribution
- **Latency**: Requires coordinator communication
- **Throughput**: Good, limited by coordinator

| Shards | Avg Latency | Distribution Quality | Coordinator Dependency |
| ------ | ----------- | -------------------- | ---------------------- |
| 10     | +15ms       | Perfect              | High                   |
| 100    | +20ms       | Perfect              | High                   |
| 1000   | +25ms       | Perfect              | High                   |

#### Random Strategy

- **Best for**: Simple setup, good distribution over time
- **Latency**: Low overhead
- **Throughput**: Good for medium-scale deployments

| Shards | Avg Latency | Distribution Quality | Coordinator Dependency |
| ------ | ----------- | -------------------- | ---------------------- |
| 10     | +3ms        | Good                 | None                   |
| 100    | +3ms        | Good                 | None                   |
| 1000   | +3ms        | Fair                 | None                   |

#### Location Strategy

- **Best for**: Geographic optimization, reduced latency
- **Latency**: Optimized by region proximity
- **Throughput**: Regional performance benefits

| Shards | Avg Latency | Geographic Benefit   | Coordinator Dependency |
| ------ | ----------- | -------------------- | ---------------------- |
| 10     | +8ms        | Excellent (-20-40ms) | Optional               |
| 100    | +10ms       | Excellent (-20-40ms) | Optional               |
| 1000   | +12ms       | Excellent (-20-40ms) | Optional               |

### Real-World Scaling Benefits

#### Database Size Limits

- **Single D1**: Limited to D1's database size constraints
- **CollegeDB**: Virtually unlimited through horizontal distribution
- **Data per shard**: Scales inversely with shard count (1000 shards = 1/1000 data per shard)

#### Geographic Distribution

```typescript
// Location-aware sharding reduces latency by 20-40ms
initialize({
  kv: env.KV,
  strategy: 'location',
  targetRegion: 'wnam', // Western North America
  shardLocations: {
    'db-west': { region: 'wnam', priority: 2 },    // Preferred
    'db-east': { region: 'enam', priority: 1 },    // Secondary
    'db-europe': { region: 'weur', priority: 0.5 } // Fallback
  },
  shards: { ... }
});
```

#### Fault Tolerance

- **Single D1**: Single point of failure
- **CollegeDB**: Distributed failure isolation (failure of 1 shard affects only 1/N of data)

### Cost-Performance Analysis

| Shards | D1 Costs\*\* | Performance Gain | Cost per Performance Unit |
| ------ | ------------ | ---------------- | ------------------------- |
| 1      | 1x           | 1x               | 1.00x                     |
| 10     | 1.2x         | ~9x              | 0.13x                     |
| 100    | 2.5x         | ~80x             | 0.03x                     |
| 1000   | 15x          | ~700x            | 0.02x                     |

\*\*Estimated based on D1's pricing model including KV overhead

### When to Use CollegeDB

✅ **Recommended for:**

- High-traffic applications (>1000 QPS)
- Large datasets approaching D1 limits
- Geographic distribution requirements
- Applications needing >50 concurrent operations
- Systems requiring fault tolerance

❌ **Not recommended for:**

- Small applications (<100 QPS)
- Simple CRUD operations with minimal scale
- Applications without geographic spread
- Cost-sensitive deployments at small scale

## �🔧 Advanced Configuration

### Custom Allocation Strategy

```typescript
initialize({
	kv: env.KV,
	shards: { 'db-east': env['db-east'], 'db-west': env['db-west'] },
	strategy: 'hash' // Shard selection based on primary key hash
});
```

### Environment-Specific Setup

```typescript
const config = {
	kv: env.KV,
	shards: env.NODE_ENV === 'production' ? { 'db-prod-1': env['db-prod-1'], 'db-prod-2': env['db-prod-2'] } : { 'db-dev': env['db-dev'] },
	strategy: 'round-robin' // Shard selection is evenly distributed, regardless of size
};

initialize(config);
```

## 🚀 Quick Reference

### Strategy Selection Guide

| Strategy      | Use Case                                 | Latency          | Distribution | Coordinator Required |
| ------------- | ---------------------------------------- | ---------------- | ------------ | -------------------- |
| `hash`        | High-volume apps, consistent performance | Lowest           | Excellent    | No                   |
| `round-robin` | Guaranteed even distribution             | Medium           | Perfect      | Yes                  |
| `random`      | Simple setup, good enough distribution   | Low              | Good         | No                   |
| `location`    | Geographic optimization, reduced latency | Region-optimized | Good         | No                   |

### Configuration Templates

**Hash Strategy (Recommended for most apps):**

```typescript
{
  kv: env.KV,
  strategy: 'hash',
  shards: { 'db-1': env.DB_1, 'db-2': env.DB_2 }
}
```

**Location Strategy (Geographic optimization):**

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

**Round-Robin Strategy (Even distribution):**

```typescript
{
  kv: env.KV,
  coordinator: env.ShardCoordinator,
  strategy: 'round-robin',
  shards: { 'db-1': env.DB_1, 'db-2': env.DB_2, 'db-3': env.DB_3 }
}
```

### Region Codes Reference

| Code   | Region                | Typical Location |
| ------ | --------------------- | ---------------- |
| `wnam` | Western North America | San Francisco    |
| `enam` | Eastern North America | New York         |
| `weur` | Western Europe        | London           |
| `eeur` | Eastern Europe        | Berlin           |
| `apac` | Asia Pacific          | Tokyo            |
| `oc`   | Oceania               | Sydney           |
| `me`   | Middle East           | Dubai            |
| `af`   | Africa                | Johannesburg     |

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Commit changes: `git commit -m 'Add amazing feature'`
4. Push to branch: `git push origin feature/amazing-feature`
5. Submit a pull request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Links

- [Cloudflare D1 Documentation](https://developers.cloudflare.com/d1/)
- [Cloudflare KV Documentation](https://developers.cloudflare.com/kv/)
- [Cloudflare Workers Documentation](https://developers.cloudflare.com/workers/)
- [Durable Objects Documentation](https://developers.cloudflare.com/durable-objects/)

## 🆘 Support

- 📖 [Documentation](https://earth-app.github.io/CollegeDB)
- 🐛 [Report Issues](https://github.com/earth-app/CollegeDB/issues)
- 💬 [Discussions](https://github.com/earth-app/CollegeDB/discussions)
