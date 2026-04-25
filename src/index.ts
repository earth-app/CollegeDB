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
	allAllShards,
	allAllShardsGlobal,
	allByLookupKey,
	allShard,
	collegedb,
	count,
	countAllShards,
	countShard,
	createSchema,
	explain,
	explainAllShards,
	explainShard,
	first,
	firstAllShards,
	firstAllShardsGlobal,
	firstByLookupKey,
	firstShard,
	flush,
	getClosestRegionFromIP,
	getDatabaseSizeForKey,
	getDatabaseSizeForShard,
	getDatabaseSizesAllShards,
	getShardStats,
	getTotalDatabaseSize,
	index,
	indexAllShards,
	indexShard,
	initialize,
	initializeAsync,
	listKnownShards,
	prepare,
	reassignShard,
	resetConfig,
	run,
	runAllShards,
	runShard
} from './router';

export type {
	CreateIndexOptions,
	ExplainOptions,
	GlobalAllShardsOptions,
	IndexColumnDefinition,
	ShardSizeResult,
	ShardTableCount
} from './router';

// Export utility classes
export { ShardCoordinator } from './durable';
export { CollegeDBError } from './errors';
export { KVShardMapper } from './kvmap';

// Export provider adapters
export {
	createDrizzleSQLProvider,
	createHyperdriveMySQLProvider,
	createHyperdrivePostgresProvider,
	createMySQLProvider,
	createNuxtHubKVProvider,
	createPostgreSQLProvider,
	createRedisKVProvider,
	createSQLiteProvider,
	createValkeyKVProvider,
	isKVStorage,
	isSQLDatabase,
	type DrizzleClientLike,
	type DrizzleSqlChunkLike,
	type DrizzleSqlTagLike,
	type HyperdriveBindingLike,
	type HyperdriveMySQLClientFactory,
	type HyperdrivePostgresClientFactory,
	type MySQLClientLike,
	type NuxtHubKVLike,
	type PostgresClientLike,
	type RedisLikeClient,
	type SQLiteClientLike
} from './providers';

// Export migration functions
export {
	autoDetectAndMigrate,
	checkMigrationNeeded,
	clearMigrationCache,
	clearShardMigrationCache,
	createMappingsForExistingKeys,
	createSchemaAcrossShards,
	discoverExistingPrimaryKeys,
	discoverExistingRecordsWithColumns,
	dropSchema,
	integrateExistingDatabase,
	listTables,
	migrateRecord,
	schemaExists,
	validateTableForSharding,
	type IntegrationOptions,
	type IntegrationResult,
	type ValidationResult
} from './migrations';

// Export types
export type {
	CollegeDBConfig,
	D1Region,
	Env,
	KVListResult,
	KVStorage,
	MixedShardingStrategy,
	OperationType,
	PreparedStatement,
	QueryResult,
	QueryResultMeta,
	SQLDatabase,
	ShardCoordinatorState,
	ShardLocation,
	ShardMapping,
	ShardStats,
	ShardingStrategy
} from './types';
