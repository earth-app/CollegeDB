/**
 * CollegeDB - Cloudflare D1 Sharding Router
 *
 * A TypeScript library for horizontal scaling of SQLite-style databases on Cloudflare
 * using D1 and KV. Routes queries to the correct D1 database instance using primary
 * key mappings stored in Cloudflare KV.
 *
 * @author Gregory Mitchell
 * @license MIT
 */

// Export main API functions
export {
	all,
	allShard,
	collegedb,
	createSchema,
	first,
	firstShard,
	flush,
	getClosestRegionFromIP,
	getShardStats,
	initialize,
	initializeAsync,
	listKnownShards,
	prepare,
	reassignShard,
	run,
	runShard
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
	validateTableForSharding,
	type IntegrationOptions,
	type IntegrationResult,
	type ValidationResult
} from './migrations.js';

// Export types
export type {
	CollegeDBConfig,
	D1Region,
	Env,
	ShardCoordinatorState,
	ShardLocation,
	ShardMapping,
	ShardStats,
	ShardingStrategy
} from './types.js';
