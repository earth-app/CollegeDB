# CollegeDB

_Cloudflare D1 Horizontal Sharding Router_

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
- **🎯 Multiple Allocation Strategies**: Round-robin, random, hash-based, and location-aware distribution
- **🔄 Mixed Strategy Support**: Different strategies for reads vs writes (e.g., location for writes, hash for reads)
- **📊 Shard Coordination**: Durable Objects for allocation and statistics
- **🛠 Migration Support**: Move data between shards with zero downtime
- **🔄 Automatic Drop-in Replacement**: Zero-config integration with existing databases
- **🤖 Smart Migration Detection**: Automatically discovers and maps existing data
- **⚡ High Performance**: Optimized for Cloudflare Workers runtime
- **🔧 TypeScript First**: Full type safety and excellent DX

## Installation

```bash
bun add @earth-app/collegedb
# or
npm install @earth-app/collegedb
```

## Basic Usage

```typescript
import { collegedb, createSchema, run, first } from '@earth-app/collegedb';

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
import { collegedb, first, run } from '@earth-app/collegedb';

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

### Mixed Strategy Example

```typescript
import { collegedb, first, run, type MixedShardingStrategy } from '@earth-app/collegedb';

// Use location strategy for writes (optimal data placement) and hash for reads (optimal performance)
const mixedStrategy: MixedShardingStrategy = {
	write: 'location', // New data goes to geographically optimal shards
	read: 'hash' // Reads use consistent hashing for best performance
};

collegedb(
	{
		kv: env.KV,
		strategy: mixedStrategy,
		targetRegion: 'wnam', // Western North America for writes
		shardLocations: {
			'db-west': { region: 'wnam', priority: 2 },
			'db-east': { region: 'enam', priority: 1 },
			'db-central': { region: 'enam', priority: 1 }
		},
		shards: {
			'db-west': env.DB_WEST,
			'db-east': env.DB_EAST,
			'db-central': env.DB_CENTRAL
		}
	},
	async () => {
		// Write operations use location strategy - new users placed optimally
		await run('user-california-456', 'INSERT INTO users (id, name, location) VALUES (?, ?, ?)', [
			'user-california-456',
			'California User',
			'Los Angeles'
		]);

		// Read operations use hash strategy - consistent and fast routing
		const user = await first<User>('user-california-456', 'SELECT * FROM users WHERE id = ?', ['user-california-456']);

		// Different operations can route to different shards based on strategy
		// This optimizes both data placement (writes) and query performance (reads)
		console.log(`User: ${user?.name}, Location: ${user?.location}`);
	}
);
```

This approach provides:

- **Optimal data placement**: New records are written to geographically optimal shards using `location` strategy
- **Optimal read performance**: Queries use `hash` strategy for consistent, high-performance routing
- **Flexibility**: Each operation type can use the most appropriate routing strategy

## Multi-Key Shard Mappings

CollegeDB supports **multiple lookup keys** for the same record, allowing you to query by username, email, ID, or any unique identifier. Keys are automatically hashed with SHA-256 for security and privacy.

```typescript
import { collegedb, first, run, KVShardMapper } from '@earth-app/collegedb';

collegedb(
	{
		kv: env.KV,
		shards: { 'db-east': env.DB_EAST, 'db-west': env.DB_WEST },
		hashShardMappings: true, // Default: enabled for security
		strategy: 'hash'
	},
	async () => {
		// Create a user with multiple lookup keys
		const mapper = new KVShardMapper(env.KV, { hashShardMappings: true });

		await mapper.setShardMapping('user-123', 'db-east', ['username:john_doe', 'email:john@example.com', 'id:123']);

		// Now you can query by ANY of these keys
		const byId = await first('user-123', 'SELECT * FROM users WHERE id = ?', ['user-123']);
		const byUsername = await first('username:john_doe', 'SELECT * FROM users WHERE username = ?', ['john_doe']);
		const byEmail = await first('email:john@example.com', 'SELECT * FROM users WHERE email = ?', ['john@example.com']);

		// All queries route to the same shard (db-east)
		console.log('All queries find the same user:', byId?.name);
	}
);
```

### Adding Lookup Keys to Existing Mappings

s

```typescript
const mapper = new KVShardMapper(env.KV);

// User initially created with just ID
await mapper.setShardMapping('user-456', 'db-west');

// Later, add additional lookup methods
await mapper.addLookupKeys('user-456', ['email:jane@example.com', 'username:jane']);

// Now works with any key
const user = await first('email:jane@example.com', 'SELECT * FROM users WHERE email = ?', ['jane@example.com']);
```

### Security and Privacy

**SHA-256 Hashing (Enabled by Default)**: Sensitive data like emails are hashed before being stored as KV keys, protecting user privacy:

```typescript
// With hashShardMappings: true (default)
// KV stores: "shard:a1b2c3d4..." instead of "shard:email:user@example.com"

const config = {
	kv: env.KV,
	shards: {
		/* ... */
	},
	hashShardMappings: true, // Hashes keys with SHA-256
	strategy: 'hash'
};
```

**⚠️ Performance Trade-off**: When hashing is enabled, operations like `getKeysForShard()` cannot return original key names, only hashed versions. For full key recovery, disable hashing:

```typescript
const config = {
	hashShardMappings: false // Disables hashing - keys stored in plain text
};
```

### Multi-Key Management

```typescript
const mapper = new KVShardMapper(env.KV);

// Get all lookup keys for a mapping
const allKeys = await mapper.getAllLookupKeys('email:user@example.com');
console.log(allKeys); // ['user-123', 'username:john', 'email:user@example.com']

// Update shard assignment (updates all keys)
await mapper.updateShardMapping('username:john', 'db-central');

// Delete mapping (removes all associated keys)
await mapper.deleteShardMapping('user-123');
```

## Drop-in Replacement for Existing Databases

CollegeDB supports **seamless, automatic integration** with existing D1 databases that already contain data. Simply add your existing databases as shards in the configuration. CollegeDB will automatically detect existing data and create the necessary shard mappings **without requiring any manual migration steps**.

### Requirements for Drop-in Replacement

1. **Primary Keys**: All tables must have a primary key column (typically named `id`)
2. **Schema Compatibility**: Tables should use standard SQLite data types
3. **Access Permissions**: CollegeDB needs read/write access to existing databases
4. **KV Namespace**: A Cloudflare KV namespace for storing shard mappings

```typescript
import { collegedb, first, run } from '@earth-app/collegedb';

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
import { validateTableForSharding, listTables } from '@earth-app/collegedb';

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
import { discoverExistingPrimaryKeys } from '@earth-app/collegedb';

// Discover all user IDs in existing users table
const userIds = await discoverExistingPrimaryKeys(env.ExistingDB, 'users');
console.log(`Found ${userIds.length} existing users`);

// Custom primary key column
const orderIds = await discoverExistingPrimaryKeys(env.ExistingDB, 'orders', 'order_id');
```

### Manual Integration (Optional)

For complete control over the integration process:

```typescript
import { integrateExistingDatabase, KVShardMapper } from '@earth-app/collegedb';

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
import { initialize, first } from '@earth-app/collegedb';

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
import { initialize, first, run } from '@earth-app/collegedb';

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
import { KVShardMapper } from '@earth-app/collegedb';
const mapper = new KVShardMapper(env.KV);
await mapper.clearAllMappings(); // Returns to pre-migration state

// Or clear cache to force re-detection
import { clearMigrationCache } from '@earth-app/collegedb';
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

| Function                                   | Description                                                      | Parameters                 |
| ------------------------------------------ | ---------------------------------------------------------------- | -------------------------- |
| `collegedb(config, callback)`              | Initialize CollegeDB, then run a callback                        | `CollegeDBConfig, () => T` |
| `initialize(config)`                       | Initialize CollegeDB with configuration                          | `CollegeDBConfig`          |
| `createSchema(d1)`                         | Create database schema on a D1 instance                          | `D1Database`               |
| `prepare(key, sql)`                        | Prepare a SQL statement for execution                            | `string, string`           |
| `run(key, sql, bindings)`                  | Execute a SQL query with primary key routing                     | `string, string, any[]`    |
| `first(key, sql, bindings)`                | Execute a SQL query and return first result                      | `string, string, any[]`    |
| `all(key, sql, bindings)`                  | Execute a SQL query and return all results                       | `string, string, any[]`    |
| `runShard(shard, sql, bindings)`           | Execute a query directly on a specific shard                     | `string, string, any[]`    |
| `allShard(shard, sql, bindings)`           | Execute a query on specific shard, return all results            | `string, string, any[]`    |
| `firstShard(shard, sql, bindings)`         | Execute a query on specific shard, return first result           | `string, string, any[]`    |
| `runAllShards(sql, bindings, batchSize)`   | Execute query on all shards                                      | `string, any[], number`    |
| `allAllShards(sql, bindings, batchSize)`   | Execute query on all shards, return all results from all shards  | `string, any[], number`    |
| `firstAllShards(sql, bindings, batchSize)` | Execute query on all shards, return first result from all shards | `string, any[], number`    |
| `reassignShard(key, newShard)`             | Move primary key to different shard                              | `string, string`           |
| `listKnownShards()`                        | Get list of available shards                                     | `void`                     |
| `getShardStats()`                          | Get statistics for all shards                                    | `void`                     |
| `getDatabaseSizeForShard(shard)`           | Get size of a specific shard in bytes                            | `string`                   |
| `flush()`                                  | Clear all shard mappings (development only)                      | `void`                     |

### Drop-in Replacement Functions

| Function                                  | Description                                    | Parameters                     |
| ----------------------------------------- | ---------------------------------------------- | ------------------------------ |
| `autoDetectAndMigrate(d1, shard, config)` | Automatically detect and migrate existing data | `D1Database, string, config`   |
| `checkMigrationNeeded(d1, shard, config)` | Check if database needs migration              | `D1Database, string, config`   |
| `validateTableForSharding(d1, table)`     | Check if table is suitable for sharding        | `D1Database, string`           |
| `discoverExistingPrimaryKeys(d1, table)`  | Find all primary keys in existing table        | `D1Database, string`           |
| `integrateExistingDatabase(d1, shard)`    | Complete drop-in integration of existing DB    | `D1Database, string, mapper`   |
| `createMappingsForExistingKeys(keys)`     | Create shard mappings for existing keys        | `string[], string[], strategy` |
| `listTables(d1)`                          | Get list of tables in database                 | `D1Database`                   |
| `clearMigrationCache()`                   | Clear automatic migration cache                | `void`                         |

### Error Handling

| Class            | Description                                 | Usage                                 |
| ---------------- | ------------------------------------------- | ------------------------------------- |
| `CollegeDBError` | Custom error class for CollegeDB operations | `throw new CollegeDBError(msg, code)` |

The `CollegeDBError` class extends the native `Error` class and includes an optional error code for better error categorization:

```typescript
try {
	await run('invalid-key', 'SELECT * FROM users WHERE id = ?', ['invalid-key']);
} catch (error) {
	if (error instanceof CollegeDBError) {
		console.error(`CollegeDB Error (${error.code}): ${error.message}`);
	}
}
```

### ShardCoordinator (Durable Object) API

The `ShardCoordinator` is an optional Durable Object that provides centralized shard allocation and statistics management. All endpoints return JSON responses.

#### HTTP API Endpoints

| Endpoint    | Method | Description                        | Request Body                                     | Response                               |
| ----------- | ------ | ---------------------------------- | ------------------------------------------------ | -------------------------------------- |
| `/shards`   | GET    | List all registered shards         | None                                             | `["db-east", "db-west"]`               |
| `/shards`   | POST   | Register a new shard               | `{"shard": "db-new"}`                            | `{"success": true}`                    |
| `/shards`   | DELETE | Unregister a shard                 | `{"shard": "db-old"}`                            | `{"success": true}`                    |
| `/stats`    | GET    | Get shard statistics               | None                                             | `[{"binding":"db-east","count":1542}]` |
| `/stats`    | POST   | Update shard statistics            | `{"shard": "db-east", "count": 1600}`            | `{"success": true}`                    |
| `/allocate` | POST   | Allocate shard for primary key     | `{"primaryKey": "user-123"}`                     | `{"shard": "db-west"}`                 |
| `/allocate` | POST   | Allocate with specific strategy    | `{"primaryKey": "user-123", "strategy": "hash"}` | `{"shard": "db-west"}`                 |
| `/flush`    | POST   | Clear all state (development only) | None                                             | `{"success": true}`                    |
| `/health`   | GET    | Health check                       | None                                             | `"OK"`                                 |

#### Programmatic Methods

| Method                        | Description                   | Parameters           | Returns             |
| ----------------------------- | ----------------------------- | -------------------- | ------------------- |
| `new ShardCoordinator(state)` | Create coordinator instance   | `DurableObjectState` | `ShardCoordinator`  |
| `fetch(request)`              | Handle HTTP requests          | `Request`            | `Promise<Response>` |
| `incrementShardCount(shard)`  | Increment key count for shard | `string`             | `Promise<void>`     |
| `decrementShardCount(shard)`  | Decrement key count for shard | `string`             | `Promise<void>`     |

#### Usage Example

```typescript
import { ShardCoordinator } from '@earth-app/collegedb';

// Export for Cloudflare Workers runtime
export { ShardCoordinator };

// Use in your worker
export default {
	async fetch(request: Request, env: Env) {
		const coordinatorId = env.ShardCoordinator.idFromName('default');
		const coordinator = env.ShardCoordinator.get(coordinatorId);

		// Allocate shard for user
		const response = await coordinator.fetch('http://coordinator/allocate', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ primaryKey: 'user-123', strategy: 'hash' })
		});

		const { shard } = await response.json();
		// Use allocated shard for database operations...
	}
};
```

### Configuration Types

#### CollegeDBConfig

The main configuration interface supports both single strategies and mixed strategies:

```typescript
interface CollegeDBConfig {
	kv: KVNamespace;
	coordinator?: DurableObjectNamespace;
	shards: Record<string, D1Database>;
	strategy?: ShardingStrategy | MixedShardingStrategy;
	targetRegion?: D1Region;
	shardLocations?: Record<string, ShardLocation>;
	disableAutoMigration?: boolean; // Default: false
	hashShardMappings?: boolean; // Default: true
	maxDatabaseSize?: number; // Default: undefined (no limit)
}
```

When `hashShardMappings` is enabled (default), original keys cannot be recovered during shard operations like `getKeysForShard()`. This is intentional for privacy but means you'll get fewer results from such operations. For full key recovery, set `hashShardMappings: false`, but be aware this may expose sensitive data in KV keys.

#### Strategy Types

```typescript
// Single strategy for all operations
type ShardingStrategy = 'round-robin' | 'random' | 'hash' | 'location';

// Mixed strategy for different operation types
interface MixedShardingStrategy {
	read: ShardingStrategy; // Strategy for SELECT operations
	write: ShardingStrategy; // Strategy for INSERT/UPDATE/DELETE operations
}

// Operation types for internal routing
type OperationType = 'read' | 'write';
```

#### Example Configurations

```typescript
// Single strategy configuration (traditional)
const singleStrategyConfig: CollegeDBConfig = {
	kv: env.KV,
	strategy: 'hash', // All operations use hash strategy
	shards: {
		/* ... */
	}
};

// Mixed strategy configuration (new feature)
const mixedStrategyConfig: CollegeDBConfig = {
	kv: env.KV,
	strategy: {
		read: 'hash', // Fast, consistent reads
		write: 'location' // Optimal data placement
	},
	targetRegion: 'wnam',
	shardLocations: {
		/* ... */
	},
	shards: {
		/* ... */
	}
};
```

### Database Size Management

CollegeDB supports automatic size-based shard exclusion to prevent individual shards from becoming too large. This feature helps maintain optimal performance and prevents hitting D1 storage limits.

#### Configuration

```typescript
const config: CollegeDBConfig = {
	kv: env.KV,
	shards: {
		'db-east': env.DB_EAST,
		'db-west': env.DB_WEST,
		'db-central': env.DB_CENTRAL
	},
	strategy: 'hash',
	maxDatabaseSize: 500 * 1024 * 1024 // 500 MB limit per shard
};
```

#### How It Works

When `maxDatabaseSize` is configured:

1. **Allocation Phase**: Before allocating new records, CollegeDB checks each shard's size using efficient SQLite pragmas
2. **Size Filtering**: Shards exceeding the limit are excluded from new allocations
3. **Fallback Protection**: If all shards exceed the limit, allocation continues to prevent complete failure
4. **Existing Records**: Records already mapped to oversized shards remain accessible

#### Size Check Implementation

The size check uses SQLite's `PRAGMA page_count` and `PRAGMA page_size` for accurate, low-overhead size calculation:

```sql
-- Efficient size calculation (used internally)
PRAGMA page_count;  -- Returns number of database pages
PRAGMA page_size;   -- Returns size of each page in bytes
-- Total size = page_count × page_size
```

#### Usage Examples

```typescript
// Conservative limit for high-performance scenarios
const performanceConfig: CollegeDBConfig = {
	// ... other config
	maxDatabaseSize: 100 * 1024 * 1024, // 100 MB per shard
	strategy: 'round-robin' // Ensures even distribution
};

// Standard production limit
const productionConfig: CollegeDBConfig = {
	// ... other config
	maxDatabaseSize: 1024 * 1024 * 1024, // 1 GB per shard
	strategy: 'hash' // Consistent allocation
};

// Check individual shard sizes
import { getDatabaseSizeForShard } from '@earth-app/collegedb';

const eastSize = await getDatabaseSizeForShard('db-east');
console.log(`East shard: ${Math.round(eastSize / 1024 / 1024)} MB`);
```

#### Debug Logging

Enable debug logging to monitor size-based exclusions:

```typescript
const config: CollegeDBConfig = {
	// ... other config
	maxDatabaseSize: 500 * 1024 * 1024,
	debug: true // Logs when shards are excluded due to size
};

// Console output example:
// "Excluded 2 shards due to size limits: db-east, db-central"
```

#### Performance Impact

- **Size Check Frequency**: Only performed during new allocations (not on reads)
- **Query Efficiency**: Uses fast SQLite pragmas (microsecond execution time)
- **Parallel Execution**: Size checks run concurrently across all shards
- **Caching**: No caching implemented to ensure accurate real-time limits

### Types

CollegeDB exports TypeScript types for better development experience and type safety:

| Type                    | Description                    | Example                                             |
| ----------------------- | ------------------------------ | --------------------------------------------------- |
| `CollegeDBConfig`       | Main configuration object      | `{ kv, shards, strategy }`                          |
| `ShardingStrategy`      | Single strategy options        | `'hash' \| 'location' \| 'round-robin' \| 'random'` |
| `MixedShardingStrategy` | Mixed strategy configuration   | `{ read: 'hash', write: 'location' }`               |
| `OperationType`         | Database operation types       | `'read' \| 'write'`                                 |
| `D1Region`              | Cloudflare D1 regions          | `'wnam' \| 'enam' \| 'weur' \| ...`                 |
| `ShardLocation`         | Geographic shard configuration | `{ region: 'wnam', priority: 2 }`                   |
| `ShardStats`            | Shard usage statistics         | `{ binding: 'db-east', count: 1542 }`               |

#### Mixed Strategy Configuration

```typescript
import type { MixedShardingStrategy, CollegeDBConfig } from '@earth-app/collegedb';

// Type-safe mixed strategy configuration
const mixedStrategy: MixedShardingStrategy = {
	read: 'hash', // Fast, deterministic reads
	write: 'location' // Geographically optimized writes
};

const config: CollegeDBConfig = {
	kv: env.KV,
	strategy: mixedStrategy, // Type-checked
	targetRegion: 'wnam',
	shardLocations: {
		'db-west': { region: 'wnam', priority: 2 },
		'db-east': { region: 'enam', priority: 1 }
	},
	shards: {
		'db-west': env.DB_WEST,
		'db-east': env.DB_EAST
	}
};
```

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

#### Without ShardCoordinator (Hash/Random/Location strategies)

1. **Query Received**: Application sends query with primary key
2. **Shard Resolution**: CollegeDB checks KV for existing mapping or calculates shard using strategy
3. **Direct Allocation**: For new keys, shard selected using hash/random/location algorithm
4. **Query Execution**: SQL executed on appropriate D1 database
5. **Response**: Results returned to application

#### With ShardCoordinator (Round-Robin strategy)

1. **Query Received**: Application sends query with primary key
2. **Shard Resolution**: CollegeDB checks KV for existing mapping
3. **Coordinator Allocation**: For new keys, coordinator allocates shard using round-robin
4. **State Update**: Coordinator updates round-robin index and shard statistics
5. **Query Execution**: SQL executed on appropriate D1 database
6. **Response**: Results returned to application

#### ShardCoordinator Internal Flow

```txt
┌─────────────────────────────────────────────────────────────┐
│              ShardCoordinator (Durable Object)             │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────────────────────┐  │
│  │  HTTP API       │  │       Persistent Storage        │  │
│  │  - /allocate    │  │  - knownShards: string[]        │  │
│  │  - /shards      │  │  - shardStats: ShardStats{}     │  │
│  │  - /stats       │  │  - strategy: ShardingStrategy   │  │
│  │  - /health      │  │  - roundRobinIndex: number      │  │
│  └─────────────────┘  └─────────────────────────────────┘  │
│                                  │                         │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           Allocation Algorithms                     │   │
│  │  - Round-Robin: state.roundRobinIndex               │   │
│  │  - Hash: consistent hash(primaryKey)                │   │
│  │  - Random: Math.random() * shards.length            │   │
│  │  - Location: region proximity + priority            │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

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

#### Complete wrangler.toml with ShardCoordinator

```toml
name = "collegedb-app"
main = "src/index.ts"
compatibility_date = "2024-08-10"

# D1 Database bindings
[[d1_databases]]
binding = "db-east"
database_name = "collegedb-east"
database_id = "your-east-database-id"

[[d1_databases]]
binding = "db-west"
database_name = "collegedb-west"
database_id = "your-west-database-id"

[[d1_databases]]
binding = "db-central"
database_name = "collegedb-central"
database_id = "your-central-database-id"

# KV namespace for shard mappings
[[kv_namespaces]]
binding = "KV"
id = "your-kv-namespace-id"
preview_id = "your-kv-preview-id" # For local development

# Durable Object for shard coordination
[[durable_objects.bindings]]
name = "ShardCoordinator"
class_name = "ShardCoordinator"

# Environment-specific configurations
[env.production]
[[env.production.d1_databases]]
binding = "db-east"
database_name = "collegedb-prod-east"
database_id = "your-prod-east-id"

[[env.production.d1_databases]]
binding = "db-west"
database_name = "collegedb-prod-west"
database_id = "your-prod-west-id"

[[env.production.kv_namespaces]]
binding = "KV"
id = "your-prod-kv-namespace-id"

[[env.production.durable_objects.bindings]]
name = "ShardCoordinator"
class_name = "ShardCoordinator"
```

### 3.1. Worker Script Setup (Required for ShardCoordinator)

Create your main worker file with ShardCoordinator export:

```typescript
// src/index.ts
import { collegedb, ShardCoordinator, first, run } from '@earth-app/collegedb';

// IMPORTANT: Export ShardCoordinator for Cloudflare Workers runtime
export { ShardCoordinator };

interface Env {
	KV: KVNamespace;
	ShardCoordinator: DurableObjectNamespace;
	'db-east': D1Database;
	'db-west': D1Database;
	'db-central': D1Database;
}

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		return await collegedb(
			{
				kv: env.KV,
				coordinator: env.ShardCoordinator, // Optional: only needed for round-robin
				strategy: 'hash', // or 'round-robin', 'random', 'location'
				shards: {
					'db-east': env['db-east'],
					'db-west': env['db-west'],
					'db-central': env['db-central']
				}
			},
			async () => {
				// Your application logic here
				const url = new URL(request.url);

				if (url.pathname === '/user') {
					const userId = url.searchParams.get('id');
					if (!userId) {
						return new Response('Missing user ID', { status: 400 });
					}

					const user = await first(userId, 'SELECT * FROM users WHERE id = ?', [userId]);
					return Response.json(user);
				}

				return new Response('CollegeDB API', { status: 200 });
			}
		);
	}
};
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

#### Using CollegeDB Functions

```typescript
import { getShardStats, listKnownShards } from '@earth-app/collegedb';

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

#### Using ShardCoordinator Directly

```typescript
// Get coordinator instance
const coordinatorId = env.ShardCoordinator.idFromName('default');
const coordinator = env.ShardCoordinator.get(coordinatorId);

// Get real-time shard statistics
const statsResponse = await coordinator.fetch('http://coordinator/stats');
const detailedStats = await statsResponse.json();
console.log(detailedStats);
/* Returns:
[
  {
    "binding": "db-east",
    "count": 1542,
    "lastUpdated": 1672531200000
  },
  {
    "binding": "db-west",
    "count": 1458,
    "lastUpdated": 1672531205000
  }
]
*/

// List registered shards
const shardsResponse = await coordinator.fetch('http://coordinator/shards');
const allShards = await shardsResponse.json();
console.log(allShards); // ['db-east', 'db-west', 'db-central']
```

#### Advanced Monitoring Dashboard

```typescript
async function createMonitoringDashboard(env: Env) {
	const coordinatorId = env.ShardCoordinator.idFromName('default');
	const coordinator = env.ShardCoordinator.get(coordinatorId);

	// Get comprehensive metrics
	const [shardsResponse, statsResponse, healthResponse] = await Promise.all([
		coordinator.fetch('http://coordinator/shards'),
		coordinator.fetch('http://coordinator/stats'),
		coordinator.fetch('http://coordinator/health')
	]);

	const shards = await shardsResponse.json();
	const stats = await statsResponse.json();
	const isHealthy = healthResponse.ok;

	// Calculate distribution metrics
	const totalKeys = stats.reduce((sum: number, shard: any) => sum + shard.count, 0);
	const avgKeysPerShard = totalKeys / stats.length;
	const maxKeys = Math.max(...stats.map((s: any) => s.count));
	const minKeys = Math.min(...stats.map((s: any) => s.count));
	const distributionRatio = maxKeys / (minKeys || 1);

	// Check for stale statistics (>5 minutes)
	const now = Date.now();
	const staleThreshold = 5 * 60 * 1000; // 5 minutes
	const staleShards = stats.filter((shard: any) => now - shard.lastUpdated > staleThreshold);

	return {
		healthy: isHealthy,
		totalShards: shards.length,
		totalKeys,
		avgKeysPerShard: Math.round(avgKeysPerShard),
		distributionRatio: Math.round(distributionRatio * 100) / 100,
		isBalanced: distributionRatio < 1.5, // Less than 50% difference
		staleShards: staleShards.length,
		shardDetails: stats.map((shard: any) => ({
			...shard,
			loadPercentage: Math.round((shard.count / totalKeys) * 100),
			isStale: now - shard.lastUpdated > staleThreshold
		}))
	};
}

// Usage in monitoring endpoint
export default {
	async fetch(request: Request, env: Env) {
		if (new URL(request.url).pathname === '/monitor') {
			const dashboard = await createMonitoringDashboard(env);
			return Response.json(dashboard);
		}
		// ... rest of your app
	}
};
```

### Shard Rebalancing

```typescript
import { reassignShard } from '@earth-app/collegedb';

// Move a primary key to a different shard
await reassignShard('user-123', 'db-west');
```

### Health Monitoring

Monitor your CollegeDB deployment by tracking:

- **Shard distribution balance**
- **Query latency per shard**
- **Error rates and failed queries**
- **KV operation metrics**
- **ShardCoordinator health and availability**

#### Automated Health Checks

```typescript
async function performHealthChecks(env: Env): Promise<HealthReport> {
	const results: HealthReport = {
		overall: 'healthy',
		timestamp: new Date().toISOString(),
		checks: {}
	};

	// 1. Test KV availability
	try {
		await env.KV.put('health-check', 'ok', { expirationTtl: 60 });
		const kvTest = await env.KV.get('health-check');
		results.checks.kv = kvTest === 'ok' ? 'healthy' : 'degraded';
	} catch (error) {
		results.checks.kv = 'unhealthy';
		results.overall = 'unhealthy';
	}

	// 2. Test ShardCoordinator availability
	if (env.ShardCoordinator) {
		try {
			const coordinatorId = env.ShardCoordinator.idFromName('default');
			const coordinator = env.ShardCoordinator.get(coordinatorId);
			const healthResponse = await coordinator.fetch('http://coordinator/health');
			results.checks.coordinator = healthResponse.ok ? 'healthy' : 'unhealthy';

			if (!healthResponse.ok) {
				results.overall = 'degraded';
			}
		} catch (error) {
			results.checks.coordinator = 'unhealthy';
			results.overall = 'degraded'; // Can fallback to hash allocation
		}
	}

	// 3. Test each D1 shard
	const shardTests = Object.entries(env)
		.filter(([key]) => key.startsWith('db-'))
		.map(async ([shardName, db]: [string, any]) => {
			try {
				// Simple query to test connectivity
				await db.prepare('SELECT 1 as test').first();
				results.checks[shardName] = 'healthy';
			} catch (error) {
				results.checks[shardName] = 'unhealthy';
				results.overall = 'unhealthy';
			}
		});

	await Promise.all(shardTests);

	// 4. Check shard distribution balance
	if (results.checks.coordinator === 'healthy') {
		try {
			const coordinatorId = env.ShardCoordinator.idFromName('default');
			const coordinator = env.ShardCoordinator.get(coordinatorId);
			const statsResponse = await coordinator.fetch('http://coordinator/stats');
			const stats = await statsResponse.json();

			const totalKeys = stats.reduce((sum: number, shard: any) => sum + shard.count, 0);
			if (totalKeys > 0) {
				const avgKeys = totalKeys / stats.length;
				const maxKeys = Math.max(...stats.map((s: any) => s.count));
				const distributionRatio = maxKeys / avgKeys;

				results.checks.distribution = distributionRatio < 2 ? 'healthy' : 'degraded';
				results.distributionRatio = distributionRatio;

				if (distributionRatio >= 3 && results.overall === 'healthy') {
					results.overall = 'degraded';
				}
			}
		} catch (error) {
			results.checks.distribution = 'unknown';
		}
	}

	return results;
}

interface HealthReport {
	overall: 'healthy' | 'degraded' | 'unhealthy';
	timestamp: string;
	checks: Record<string, 'healthy' | 'degraded' | 'unhealthy' | 'unknown'>;
	distributionRatio?: number;
}

// Health endpoint example
export default {
	async fetch(request: Request, env: Env) {
		if (new URL(request.url).pathname === '/health') {
			const health = await performHealthChecks(env);
			const statusCode = health.overall === 'healthy' ? 200 : health.overall === 'degraded' ? 206 : 503;
			return Response.json(health, { status: statusCode });
		}
		// ... rest of your app
	}
};
```

#### Alerting and Monitoring Integration

```typescript
// Integration with external monitoring services
async function sendAlert(severity: 'warning' | 'critical', message: string, env: Env) {
	// Example: Slack webhook
	if (env.SLACK_WEBHOOK_URL) {
		await fetch(env.SLACK_WEBHOOK_URL, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({
				text: `🚨 CollegeDB ${severity.toUpperCase()}: ${message}`,
				username: 'CollegeDB Monitor'
			})
		});
	}

	// Example: Custom webhook
	if (env.MONITORING_WEBHOOK_URL) {
		await fetch(env.MONITORING_WEBHOOK_URL, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({
				service: 'collegedb',
				severity,
				message,
				timestamp: new Date().toISOString()
			})
		});
	}
}

// Scheduled monitoring (using Cron Triggers)
export default {
	async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
		const health = await performHealthChecks(env);

		if (health.overall === 'unhealthy') {
			await sendAlert('critical', `System unhealthy: ${JSON.stringify(health.checks)}`, env);
		} else if (health.overall === 'degraded') {
			await sendAlert('warning', `System degraded: ${JSON.stringify(health.checks)}`, env);
		}

		// Check for severe shard imbalance
		if (health.distributionRatio && health.distributionRatio > 5) {
			await sendAlert('warning', `Severe shard imbalance detected: ${health.distributionRatio}x difference`, env);
		}
	}
};
```

## ⚙️ Performance Analysis

### Scaling Performance Comparison

CollegeDB provides significant performance improvements through horizontal scaling. Here are mathematical estimates comparing single D1 database vs CollegeDB with different shard counts:

#### Query Performance

_SELECT, VALUES, TABLE, PRAGMA, ..._

| Configuration           | Query Latency\* | Concurrent Queries      | Throughput Gain |
| ----------------------- | --------------- | ----------------------- | --------------- |
| Single D1               | ~50-80ms        | Limited by D1 limits    | 1x (baseline)   |
| CollegeDB (10 shards)   | ~55-85ms        | 10x parallel capacity   | ~8-9x           |
| CollegeDB (100 shards)  | ~60-90ms        | 100x parallel capacity  | ~75-80x         |
| CollegeDB (1000 shards) | ~65-95ms        | 1000x parallel capacity | ~650-700x       |

\*Includes KV lookup overhead (~5-15ms) and SHA-256 hashing overhead (~1-3ms when `hashShardMappings: true`)

#### Write Performance

_INSERT, UPDATE, DELETE, ..._

| Configuration           | Write Latency\* | Concurrent Writes  | Throughput Gain |
| ----------------------- | --------------- | ------------------ | --------------- |
| Single D1               | ~80-120ms       | ~50 writes/sec     | 1x (baseline)   |
| CollegeDB (10 shards)   | ~90-135ms       | ~450 writes/sec    | ~9x             |
| CollegeDB (100 shards)  | ~95-145ms       | ~4,200 writes/sec  | ~84x            |
| CollegeDB (1000 shards) | ~105-160ms      | ~35,000 writes/sec | ~700x           |

\*Includes KV mapping creation/update overhead (~10-25ms) and SHA-256 hashing overhead (~1-3ms when `hashShardMappings: true`)

### Strategy-Specific Performance

#### Hash Strategy

- **Best for**: Consistent performance, even data distribution
- **Latency**: Lowest overhead (no coordinator calls, ~1-3ms SHA-256 hashing when enabled)
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

#### Mixed Strategy

- **Best for**: Optimizing both read and write performance independently
- **Latency**: Best of both strategies combined
- **Throughput**: Optimal for workloads with different read/write patterns

**High-Performance Mix**: `{ read: 'hash', write: 'location' }`

| Operation | Strategy | Latency Impact           | Throughput Benefit | Geographic Benefit   |
| --------- | -------- | ------------------------ | ------------------ | -------------------- |
| Reads     | Hash     | +5ms                     | Excellent          | None                 |
| Writes    | Location | +8ms (-20-40ms regional) | Good               | Excellent (-20-40ms) |

**Balanced Mix**: `{ read: 'location', write: 'hash' }`

| Operation | Strategy | Latency Impact           | Throughput Benefit | Geographic Benefit   |
| --------- | -------- | ------------------------ | ------------------ | -------------------- |
| Reads     | Location | +8ms (-20-40ms regional) | Good               | Excellent (-20-40ms) |
| Writes    | Hash     | +5ms                     | Excellent          | None                 |

**Enterprise Mix**: `{ read: 'hash', write: 'round-robin' }`

| Operation | Strategy    | Latency Impact | Distribution Quality | Coordinator Dependency |
| --------- | ----------- | -------------- | -------------------- | ---------------------- |
| Reads     | Hash        | +5ms           | Excellent            | None                   |
| Writes    | Round-Robin | +15-25ms       | Perfect              | High                   |

##### By Shard Count

**Hash + Location Mix** (`{ read: 'hash', write: 'location' }`)

| Shards | Read Latency | Write Latency          | Combined Benefit      | Best Use Case    |
| ------ | ------------ | ---------------------- | --------------------- | ---------------- |
| 10     | +5ms         | +8ms (-30ms regional)  | ~22ms net improvement | Global apps      |
| 100    | +5ms         | +10ms (-30ms regional) | ~20ms net improvement | Enterprise scale |
| 1000   | +5ms         | +12ms (-30ms regional) | ~18ms net improvement | Massive scale    |

**Location + Hash Mix** (`{ read: 'location', write: 'hash' }`)

| Shards | Read Latency           | Write Latency | Combined Benefit      | Best Use Case         |
| ------ | ---------------------- | ------------- | --------------------- | --------------------- |
| 10     | +8ms (-30ms regional)  | +5ms          | ~17ms net improvement | Read-heavy regional   |
| 100    | +10ms (-30ms regional) | +5ms          | ~15ms net improvement | Analytics workloads   |
| 1000   | +12ms (-30ms regional) | +5ms          | ~13ms net improvement | Large-scale reporting |

**Hash + Round-Robin Mix** (`{ read: 'hash', write: 'round-robin' }`)

| Shards | Read Latency | Write Latency | Distribution Quality            | Best Use Case      |
| ------ | ------------ | ------------- | ------------------------------- | ------------------ |
| 10     | +5ms         | +15ms         | Perfect writes, Excellent reads | Balanced workloads |
| 100    | +5ms         | +20ms         | Perfect writes, Excellent reads | Large databases    |
| 1000   | +5ms         | +25ms         | Perfect writes, Excellent reads | Enterprise scale   |

### Mixed Strategy Scenarios & Recommendations

#### Large Database Scenarios (>10M records)

**Scenario**: Massive datasets requiring optimal query performance and balanced growth

```typescript
// Recommended: Hash reads + Round-Robin writes
{
  strategy: { read: 'hash', write: 'round-robin' },
  coordinator: env.ShardCoordinator // Required for round-robin
}
```

**Performance Profile**:

- Read latency: +5ms (fastest possible routing)
- Write latency: +15-25ms (coordinator overhead)
- Data distribution: Perfect balance over time
- **Ideal for**: Analytics platforms, data warehouses, reporting systems

#### Vast Geographic Spread Scenarios

**Scenario**: Global applications with users across multiple continents

```typescript
// Recommended: Hash reads + Location writes
{
  strategy: { read: 'hash', write: 'location' },
  targetRegion: getClosestRegionFromIP(request), // Dynamic region targeting
  shardLocations: {
    'db-americas': { region: 'wnam', priority: 2 },
    'db-europe': { region: 'weur', priority: 2 },
    'db-asia': { region: 'apac', priority: 2 }
  }
}
```

**Performance Profile**:

- Read latency: +5ms (consistent global performance)
- Write latency: +8ms baseline (-20-40ms regional benefit)
- **Net improvement**: 15-35ms for geographically distributed users
- **Ideal for**: Social networks, e-commerce, content platforms

#### High-Volume Write Scenarios

**Scenario**: Applications with heavy write loads (IoT, logging, real-time data)

```typescript
// Recommended: Location reads + Hash writes
{
  strategy: { read: 'location', write: 'hash' },
  targetRegion: 'wnam',
  shardLocations: {
    'db-west': { region: 'wnam', priority: 3 },
    'db-central': { region: 'enam', priority: 2 },
    'db-east': { region: 'enam', priority: 1 }
  }
}
```

**Performance Profile**:

- Read latency: +8ms baseline (-20-40ms regional benefit)
- Write latency: +5ms (fastest write routing)
- Write throughput: Maximum possible for hash strategy
- **Ideal for**: IoT data collection, real-time analytics, logging systems

#### Multi-Tenant SaaS Scenarios

**Scenario**: SaaS applications with predictable performance requirements

```typescript
// Recommended: Hash reads + Hash writes (consistent performance)
{
  strategy: { read: 'hash', write: 'hash' }
  // No coordinator needed, predictable routing for both operations
}
```

**Performance Profile**:

- Read latency: +5ms (most predictable)
- Write latency: +5ms (most predictable)
- Tenant isolation: Natural sharding by tenant ID
- **Ideal for**: B2B SaaS, multi-tenant platforms, predictable workloads

#### Read-Heavy Analytics Scenarios

**Scenario**: Analytics and reporting workloads with occasional writes

```typescript
// Recommended: Random reads + Location writes
{
  strategy: { read: 'random', write: 'location' },
  targetRegion: 'wnam',
  shardLocations: { /* geographic configuration */ }
}
```

**Performance Profile**:

- Read latency: +3ms (lowest overhead, good load balancing)
- Write latency: +8ms baseline (-20-40ms regional benefit)
- Read load distribution: Excellent across all shards
- **Ideal for**: Business intelligence, data analysis, reporting dashboards

### Mixed Strategy Performance Comparison

#### By Database Size

| Database Size                 | Best Mixed Strategy                        | Read Performance      | Write Performance    | Overall Benefit       |
| ----------------------------- | ------------------------------------------ | --------------------- | -------------------- | --------------------- |
| **Small (1K-100K records)**   | `{read: 'hash', write: 'hash'}`            | Excellent             | Excellent            | Consistent, simple    |
| **Medium (100K-1M records)**  | `{read: 'hash', write: 'location'}`        | Excellent             | Good + Regional      | 15-35ms improvement   |
| **Large (1M-10M records)**    | `{read: 'hash', write: 'round-robin'}`     | Excellent             | Perfect distribution | Optimal scaling       |
| **Very Large (10M+ records)** | `{read: 'location', write: 'round-robin'}` | Regional optimization | Perfect distribution | Best for global scale |

#### By Geographic Distribution

| Geographic Spread | Best Mixed Strategy                     | Latency Improvement     | Use Case                        |
| ----------------- | --------------------------------------- | ----------------------- | ------------------------------- |
| **Single Region** | `{read: 'hash', write: 'hash'}`         | +5ms both operations    | Simple, fast                    |
| **Multi-Region**  | `{read: 'hash', write: 'location'}`     | 15-35ms net improvement | Global apps                     |
| **Global**        | `{read: 'location', write: 'location'}` | 20-40ms both operations | Maximum geographic optimization |

#### By Workload Pattern

| Workload Type   | Read/Write Ratio | Best Mixed Strategy                        | Primary Benefit                 |
| --------------- | ---------------- | ------------------------------------------ | ------------------------------- |
| **Read-Heavy**  | 90% reads        | `{read: 'random', write: 'location'}`      | Load-balanced queries           |
| **Write-Heavy** | 70% writes       | `{read: 'location', write: 'hash'}`        | Fast write processing           |
| **Balanced**    | 50/50            | `{read: 'hash', write: 'hash'}`            | Consistent performance          |
| **Analytics**   | 95% reads        | `{read: 'location', write: 'round-robin'}` | Regional + perfect distribution |

### SHA-256 Hashing Performance Impact

CollegeDB uses SHA-256 hashing by default (`hashShardMappings: true`) to protect sensitive data in KV keys. This adds a small but measurable performance overhead:

#### Hashing Performance Characteristics

| Operation Type     | SHA-256 Overhead | Total Latency Impact | Security Benefit             |
| ------------------ | ---------------- | -------------------- | ---------------------------- |
| **Query (Read)**   | ~1-2ms           | 2-4% increase        | Keys hashed in KV storage    |
| **Insert (Write)** | ~2-3ms           | 2-3% increase        | Multi-key mappings protected |
| **Update Mapping** | ~1-3ms           | 1-2% increase        | Existing keys remain secure  |

#### Performance by Key Length

| Key Type                 | Example                        | Hash Time    | Recommendation          |
| ------------------------ | ------------------------------ | ------------ | ----------------------- |
| **Short keys**           | `user-123`                     | ~0.5-1ms     | Minimal impact          |
| **Medium keys**          | `email:user@example.com`       | ~1-2ms       | Good balance            |
| **Long keys**            | `session:very-long-token-here` | ~2-3ms       | Consider key shortening |
| **Multi-key operations** | 3+ lookup keys                 | ~3-5ms total | Benefits outweigh cost  |

#### Hashing vs No-Hashing Trade-offs

```typescript
// With hashing (default - recommended for production)
const secureConfig = {
	hashShardMappings: true // Default
	// + Privacy: Sensitive data not visible in KV
	// + Security: Keys cannot be enumerated
	// - Performance: +1-3ms per operation
	// - Debugging: Original keys not recoverable
};

// Without hashing (development/debugging only)
const developmentConfig = {
	hashShardMappings: false
	// + Performance: No hashing overhead
	// + Debugging: Original keys visible in KV
	// - Privacy: Sensitive data exposed in KV keys
	// - Security: Keys can be enumerated
};
```

#### Optimization Recommendations

1. **Keep keys reasonably short** - Hash time scales with key length
2. **Use hashing in production** - Security benefits outweigh minimal performance cost
3. **Disable hashing for development** - When debugging shard distribution
4. **Monitor hash performance** - Track operation latencies in high-volume scenarios

**Bottom Line**: SHA-256 hashing adds 1-3ms overhead but provides essential privacy and security benefits. The performance impact is minimal compared to network latency and D1 query time.

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

✅ **Mixed Strategy specifically recommended for:**

- **Global applications** needing both fast queries and optimal data placement
- **Large-scale databases** requiring different optimization for reads vs writes
- **Multi-workload systems** with distinct read/write patterns
- **Geographic optimization** while maintaining query performance
- **Enterprise applications** needing fine-tuned performance control

❌ **Not recommended for:**

- Small applications (<100 QPS)
- Simple CRUD operations with minimal scale
- Applications without geographic spread
- Cost-sensitive deployments at small scale
- **Single-strategy applications** where reads and writes have identical performance needs

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

### Durable Objects Integration (ShardCoordinator)

CollegeDB includes an optional **ShardCoordinator** Durable Object that provides centralized shard allocation and statistics management. This is particularly useful for round-robin allocation strategies and monitoring shard utilization across your application.

#### Durable Object Setup

First, configure the Durable Object in your `wrangler.toml`:

```toml
[[durable_objects.bindings]]
name = "ShardCoordinator"
class_name = "ShardCoordinator"

# Export the ShardCoordinator class
[durable_objects.bindings.script_name]
# If using modules format
[[durable_objects.bindings]]
name = "ShardCoordinator"
class_name = "ShardCoordinator"
```

#### Basic Usage with ShardCoordinator

```typescript
import { collegedb, ShardCoordinator } from '@earth-app/collegedb';

// Export the Durable Object class for Cloudflare Workers
export { ShardCoordinator };

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		// Initialize CollegeDB with coordinator support
		await collegedb(
			{
				kv: env.KV,
				coordinator: env.ShardCoordinator, // Add coordinator binding
				strategy: 'round-robin',
				shards: {
					'db-east': env.DB_EAST,
					'db-west': env.DB_WEST,
					'db-central': env.DB_CENTRAL
				}
			},
			async () => {
				// Your application logic here
				const user = await first('user-123', 'SELECT * FROM users WHERE id = ?', ['user-123']);
				return Response.json(user);
			}
		);
	}
};
```

#### ShardCoordinator HTTP API

The ShardCoordinator exposes a comprehensive HTTP API for managing shards and allocation:

##### Shard Management

```typescript
// Get coordinator instance
const coordinatorId = env.ShardCoordinator.idFromName('default');
const coordinator = env.ShardCoordinator.get(coordinatorId);

// List all registered shards
const shardsResponse = await coordinator.fetch('http://coordinator/shards');
const shards = await shardsResponse.json();
// Returns: ["db-east", "db-west", "db-central"]

// Register a new shard
await coordinator.fetch('http://coordinator/shards', {
	method: 'POST',
	headers: { 'Content-Type': 'application/json' },
	body: JSON.stringify({ shard: 'db-new-region' })
});

// Remove a shard
await coordinator.fetch('http://coordinator/shards', {
	method: 'DELETE',
	headers: { 'Content-Type': 'application/json' },
	body: JSON.stringify({ shard: 'db-old-region' })
});
```

##### Statistics and Monitoring

```typescript
// Get shard statistics
const statsResponse = await coordinator.fetch('http://coordinator/stats');
const stats = await statsResponse.json();
/* Returns:
[
  {
    "binding": "db-east",
    "count": 1542,
    "lastUpdated": 1672531200000
  },
  {
    "binding": "db-west",
    "count": 1458,
    "lastUpdated": 1672531205000
  }
]
*/

// Update shard statistics manually
await coordinator.fetch('http://coordinator/stats', {
	method: 'POST',
	headers: { 'Content-Type': 'application/json' },
	body: JSON.stringify({
		shard: 'db-east',
		count: 1600
	})
});
```

##### Shard Allocation

```typescript
// Allocate a shard for a primary key
const allocationResponse = await coordinator.fetch('http://coordinator/allocate', {
	method: 'POST',
	headers: { 'Content-Type': 'application/json' },
	body: JSON.stringify({
		primaryKey: 'user-123',
		strategy: 'round-robin' // Optional, uses coordinator default if not specified
	})
});

const { shard } = await allocationResponse.json();
// Returns: { "shard": "db-west" }

// Hash-based allocation (consistent for same key)
const hashAllocation = await coordinator.fetch('http://coordinator/allocate', {
	method: 'POST',
	headers: { 'Content-Type': 'application/json' },
	body: JSON.stringify({
		primaryKey: 'user-456',
		strategy: 'hash'
	})
});
```

##### Health Check and Development

```typescript
// Health check endpoint
const healthResponse = await coordinator.fetch('http://coordinator/health');
// Returns: "OK" with 200 status

// Clear all coordinator state (DEVELOPMENT ONLY!)
await coordinator.fetch('http://coordinator/flush', {
	method: 'POST'
});
// WARNING: This removes all shard registrations and statistics
```

#### Programmatic Shard Management

The ShardCoordinator also provides methods for direct programmatic access:

```typescript
// Get coordinator instance
const coordinatorId = env.ShardCoordinator.idFromName('default');
const coordinator = env.ShardCoordinator.get(coordinatorId);

// Increment shard count (when adding new keys)
await coordinator.incrementShardCount('db-east');

// Decrement shard count (when removing keys)
await coordinator.decrementShardCount('db-west');
```

#### Advanced Monitoring Setup

Set up comprehensive monitoring of your shard distribution:

```typescript
async function monitorShardHealth(env: Env) {
	const coordinatorId = env.ShardCoordinator.idFromName('default');
	const coordinator = env.ShardCoordinator.get(coordinatorId);

	// Get current statistics
	const statsResponse = await coordinator.fetch('http://coordinator/stats');
	const stats = await statsResponse.json();

	// Calculate distribution balance
	const totalKeys = stats.reduce((sum: number, shard: any) => sum + shard.count, 0);
	const avgKeysPerShard = totalKeys / stats.length;

	// Check for imbalanced shards (>20% deviation from average)
	const imbalancedShards = stats.filter((shard: any) => {
		const deviation = Math.abs(shard.count - avgKeysPerShard) / avgKeysPerShard;
		return deviation > 0.2;
	});

	if (imbalancedShards.length > 0) {
		console.warn('Shard imbalance detected:', imbalancedShards);
		// Trigger rebalancing logic or alerts
	}

	// Check for stale statistics (>1 hour old)
	const now = Date.now();
	const staleShards = stats.filter((shard: any) => {
		return now - shard.lastUpdated > 3600000; // 1 hour in ms
	});

	if (staleShards.length > 0) {
		console.warn('Stale shard statistics detected:', staleShards);
	}

	return {
		totalKeys,
		avgKeysPerShard,
		balance: imbalancedShards.length === 0,
		freshStats: staleShards.length === 0,
		shards: stats
	};
}
```

#### Error Handling with ShardCoordinator

When using the ShardCoordinator, ensure you handle potential errors gracefully:

```typescript
try {
	const coordinator = env.ShardCoordinator.get(coordinatorId);
	const response = await coordinator.fetch('http://coordinator/allocate', {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify({ primaryKey: 'user-123' })
	});

	if (!response.ok) {
		const error = await response.json();
		throw new Error(`ShardCoordinator error: ${error.error}`);
	}

	const { shard } = await response.json();
	return shard;
} catch (error) {
	console.error('Failed to allocate shard:', error);
	// Fallback to hash-based allocation without coordinator
	return hashFunction('user-123', availableShards);
}
```

#### Performance Considerations

- **Coordinator Latency**: Round-robin allocation adds ~10-20ms latency due to coordinator communication
- **Scalability**: Single coordinator instance can handle thousands of allocations per second
- **Fault Tolerance**: Design fallback allocation strategies when coordinator is unavailable
- **Caching**: Consider caching allocation results for frequently accessed keys

```typescript
// Fallback allocation when coordinator is unavailable
function fallbackAllocation(primaryKey: string, shards: string[]): string {
	// Use hash-based allocation as fallback
	const hash = simpleHash(primaryKey);
	return shards[hash % shards.length];
}

async function allocateWithFallback(coordinator: DurableObjectNamespace, primaryKey: string, shards: string[]): Promise<string> {
	try {
		const coordinatorId = coordinator.idFromName('default');
		const instance = coordinator.get(coordinatorId);

		const response = await instance.fetch('http://coordinator/allocate', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ primaryKey })
		});

		if (response.ok) {
			const { shard } = await response.json();
			return shard;
		}
	} catch (error) {
		console.warn('Coordinator unavailable, using fallback allocation:', error);
	}

	// Fallback to hash-based allocation
	return fallbackAllocation(primaryKey, shards);
}
```

## 🚀 Quick Reference

### Strategy Selection Guide

| Strategy      | Use Case                                 | Latency            | Distribution | Coordinator Required |
| ------------- | ---------------------------------------- | ------------------ | ------------ | -------------------- |
| `hash`        | High-volume apps, consistent performance | Lowest             | Excellent    | No                   |
| `round-robin` | Guaranteed even distribution             | Medium             | Perfect      | Yes                  |
| `random`      | Simple setup, good enough distribution   | Low                | Good         | No                   |
| `location`    | Geographic optimization, reduced latency | Region-optimized   | Good         | No                   |
| `mixed`       | Optimized read/write performance         | Strategy-dependent | Variable     | Strategy-dependent   |

#### Mixed Strategy Recommendations by Scenario

| Scenario                           | Recommended Mix                        | Read Strategy | Write Strategy | Benefits                                       |
| ---------------------------------- | -------------------------------------- | ------------- | -------------- | ---------------------------------------------- |
| **Large Databases (>10M records)** | `{read: 'hash', write: 'round-robin'}` | Hash          | Round-Robin    | Fastest reads, even data distribution          |
| **Global Applications**            | `{read: 'hash', write: 'location'}`    | Hash          | Location       | Fast queries, optimal geographic placement     |
| **High Write Volume**              | `{read: 'location', write: 'hash'}`    | Location      | Hash           | Regional read optimization, fast write routing |
| **Analytics Workloads**            | `{read: 'random', write: 'location'}`  | Random        | Location       | Load-balanced queries, optimal data placement  |
| **Multi-Tenant SaaS**              | `{read: 'hash', write: 'hash'}`        | Hash          | Hash           | Consistent performance, predictable routing    |

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

**Mixed Strategy (Global applications):**

```typescript
{
  kv: env.KV,
  strategy: {
    read: 'hash',      // Fast, consistent reads
    write: 'location'  // Optimal geographic placement
  },
  targetRegion: 'wnam',
  shardLocations: {
    'db-west': { region: 'wnam', priority: 2 },
    'db-east': { region: 'enam', priority: 1 }
  },
  shards: { 'db-west': env.DB_WEST, 'db-east': env.DB_EAST }
}
```

**Mixed Strategy (Large databases):**

```typescript
{
  kv: env.KV,
  coordinator: env.ShardCoordinator,
  strategy: {
    read: 'hash',         // Fastest possible reads
    write: 'round-robin'  // Perfect distribution
  },
  shards: { 'db-1': env.DB_1, 'db-2': env.DB_2, 'db-3': env.DB_3 }
}
```

**Mixed Strategy (High-performance consistent):**

```typescript
{
  kv: env.KV,
  strategy: {
    read: 'hash',   // Predictable read performance
    write: 'hash'   // Predictable write performance
  },
  shards: { 'db-1': env.DB_1, 'db-2': env.DB_2 }
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
