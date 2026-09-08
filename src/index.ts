/**
 * CollegeDB - Universal Database Horizontal Sharding Router
 *
 * A TypeScript library for horizontal scaling of SQL databases. Routes each
 * query to the shard that owns its primary key, either by computing the shard
 * from the key or by reading a mapping from a key-value store.
 *
 * SQL backends: Cloudflare D1, PostgreSQL, MySQL, MariaDB, SQLite, and any
 * Drizzle ORM instance over them. Mapping backends: Cloudflare Workers KV,
 * Redis, Valkey, and NuxtHub KV. Runs on Cloudflare Workers and on Node or Bun.
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
	batch,
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
	invalidateMappingCache,
	isInitialized,
	listKnownShards,
	nextId,
	paginate,
	patch,
	prepare,
	query,
	queryAll,
	queryFirst,
	reassignShard,
	rebalance,
	resetConfig,
	run,
	runAllShards,
	runShard,
	setLookup,
	updateRow,
	upsert
} from './router';

export type {
	BatchEntry,
	BatchShardResult,
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
	RebalanceResult,
	ShardSizeResult,
	ShardTableCount
} from './router';

// Export the routing-key planner
export { planQuery, unroutableError } from './planner';
export type { PlanQueryOptions, QueryPlan } from './planner';

// Export computed-placement primitives
export {
	candidateShards,
	createManifest,
	hrwShard,
	legacyModuloShard,
	shardForEpoch,
	withCurrentTopology,
	type PlacementAlgorithm,
	type PlacementEpoch,
	type PlacementManifest
} from './placement';

// Export per-phase timing instrumentation
export { PhaseCollector, isPhaseObserverActive, setPhaseObserver } from './telemetry';
export type { PhaseName, PhaseObserver, PhaseSpan, PhaseStats } from './telemetry';

// Export deterministic SQL builders
export { buildDelete, buildInsert, buildUpdate, buildUpsert, quoteIdentifier, validateIdentifier } from './query';
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
	type HyperdriveProvider,
	type HyperdriveProviderOptions,
	type LeasedConnection,
	type MySQLClientLike,
	type NuxtHubKVLike,
	type PostgresClientLike,
	type RedisLikeClient,
	type SQLBatchOptions,
	type SQLiteClientLike,
	type ToProviderOptions,
	type WorkersKVNamespaceLike,
	type WorkersKVProviderOptions
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
	BatchStatement,
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
	SQLDialect,
	ShardCoordinatorState,
	ShardLocation,
	ShardMapping,
	ShardStats,
	ShardingStrategy
} from './types';
