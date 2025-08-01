/**
 * CollegeDB - Cloudflare D1 Sharding Router
 *
 * A TypeScript library for horizontal scaling of SQLite-style databases on Cloudflare
 * using D1 and KV. Routes queries to the correct D1 database instance using primary
 * key mappings stored in Cloudflare KV.
 *
 * @author CollegeDB Team
 * @version 1.0.0
 */

// Export main API functions
export {
	createSchema,
	deleteByPrimaryKey,
	flush,
	getShardStats,
	initialize,
	insert,
	listKnownShards,
	queryOnShard,
	reassignShard,
	selectByPrimaryKey,
	updateByPrimaryKey
} from './router.js';

// Export utility classes
export { ShardCoordinator } from './durable.js';
export { KVShardMapper } from './kvmap.js';

// Export migration functions
export {
	autoDetectAndMigrate,
	checkMigrationNeeded,
	clearMigrationCache,
	createMappingsForExistingKeys,
	createSchemaAcrossShards,
	discoverExistingPrimaryKeys,
	dropSchema,
	integrateExistingDatabase,
	listTables,
	migrateRecord,
	schemaExists,
	validateTableForSharding
} from './migrations.js';

// Export types
export type { CollegeDBConfig, Env, QueryResult, ShardCoordinatorState, ShardMapping, ShardStats, ShardStrategy } from './types.js';
