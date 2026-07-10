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
	deleteById,
	deleteLookup,
	deleteRow,
	ensureSchema,
	explain,
	explainAllShards,
	explainShard,
	first,
	firstAllShards,
	firstAllShardsGlobal,
	firstByLookupKey,
	firstResilient,
	firstShard,
	flush,
	getActiveConfig,
	getClosestRegionFromIP,
	getDatabaseSizeForKey,
	getDatabaseSizeForShard,
	getDatabaseSizesAllShards,
	getLookup,
	getShardStats,
	getTotalDatabaseSize,
	index,
	indexAllShards,
	indexShard,
	initialize,
	initializeAsync,
	initializeFromEnv,
	insert,
	insertInto,
	insertReturning,
	insertShard,
	isInitialized,
	listKnownShards,
	nextId,
	paginate,
	patch,
	prepare,
	reassignShard,
	resetConfig,
	run,
	runAllShards,
	runShard,
	setLookup,
	updateRow,
	upsert
} from './router';

export type {
	CreateIndexOptions,
	CrudReturningOptions,
	EnsureSchemaOptions,
	ExplainOptions,
	GlobalAllShardsOptions,
	IdColumnOptions,
	IndexColumnDefinition,
	InitializeFromEnvOptions,
	InsertResult,
	LookupOptions,
	NextIdOptions,
	PaginateOptions,
	PaginatedResult,
	ShardSizeResult,
	ShardTableCount
} from './router';

// Export deterministic SQL builders
export { buildDelete, buildInsert, buildUpdate, buildUpsert, quoteIdentifier } from './query';
export type { BuildInsertOptions, BuildUpsertOptions, BuiltQuery, ColumnValues } from './query';

// Export KV read-through cache helpers
export { cached, invalidate } from './cache';
export type { CacheOptions } from './cache';

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
	createWorkersKVProvider,
	isKVStorage,
	isSQLDatabase,
	toProvider,
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
	type SQLiteClientLike,
	type ToProviderOptions,
	type WorkersKVNamespaceLike
} from './providers';

// Export in-memory mock providers for testing
export { InMemoryKVStorage, InMemorySQLDatabase, createInMemoryKVProvider, createInMemorySQLProvider } from './providers-memory';

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
