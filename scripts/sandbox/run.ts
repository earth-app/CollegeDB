#!/usr/bin/env bun

/// <reference types="bun-types" />

import { Database } from 'bun:sqlite';
import { sql as drizzleSql } from 'drizzle-orm';
import { drizzle as drizzleBunSQLite } from 'drizzle-orm/bun-sqlite';
import { drizzle as drizzleMySQL } from 'drizzle-orm/mysql2';
import { drizzle as drizzlePostgres } from 'drizzle-orm/node-postgres';
import mysql from 'mysql2/promise';
import { mkdir, rm } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { Client as PostgresClient, Pool as PostgresPool } from 'pg';
import { createClient as createRedisClient } from 'redis';
import {
	CollegeDBError,
	all,
	allAllShards,
	allShard,
	batch,
	createHyperdriveMySQLProvider,
	createHyperdrivePostgresProvider,
	createMappingsForExistingKeys,
	createMySQLProvider,
	createPostgreSQLProvider,
	createRedisKVProvider,
	createSQLiteProvider,
	createSchemaAcrossShards,
	createValkeyKVProvider,
	deleteRow,
	first,
	flush,
	getShardStats,
	hrwShard,
	initialize,
	initializeAsync,
	insertInto,
	insertShard,
	invalidateMappingCache,
	nextId,
	paginate,
	pruneEpochs,
	reassignShard,
	reconcileAllocations,
	resetConfig,
	run,
	runAllShards,
	runShard,
	updateRow,
	upsert
} from '../../src/index';
import { KVShardMapper } from '../../src/kvmap';
import type { MigrationEnv } from '../../src/migrations';
import {
	ensureIndex,
	ensureJournalSchema,
	findTombstone,
	getMigrationStatus,
	moveRecord,
	recoverMigrations,
	runGuardedWrite,
	setMigrationStepHook
} from '../../src/migrations';
import { createManifest, resetPlacementState } from '../../src/placement';
import {
	HISTORY_INDEX,
	LEASE_TABLE,
	buildGuardedInsert,
	buildInsert,
	buildJournalSchema,
	buildShardSidecarSchema,
	quoteIdentifier
} from '../../src/query';
import { PhaseCollector } from '../../src/telemetry';
import { buildManifestSchema, commitJournalManifest, loadJournalManifest, raiseFence, readFence } from '../../src/topology';
import type {
	BatchStatement,
	CollegeDBConfig,
	D1Region,
	KVStorage,
	MixedShardingStrategy,
	PreparedStatement,
	SQLDatabase,
	ShardLocation,
	ShardingStrategy
} from '../../src/types';

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = resolve(__dirname, '..', '..');
const RESULTS_DIR = resolve(PROJECT_ROOT, 'sandbox', 'results');
const TMP_DIR = resolve(PROJECT_ROOT, 'sandbox', 'tmp');
const COMPOSE_SCRIPT = resolve(PROJECT_ROOT, 'scripts', 'sandbox', 'compose.sh');
const WRANGLER_CONFIG = resolve(PROJECT_ROOT, 'sandbox', 'wrangler.jsonc');
const WRANGLER_NO_COMPAT_CONFIG = resolve(PROJECT_ROOT, 'sandbox', 'wrangler.no-compat.jsonc');
const WRANGLER_BIN = resolve(PROJECT_ROOT, 'node_modules', '.bin', 'wrangler');
const NO_COMPAT_URL = 'http://127.0.0.1:8788';
/** Overlapping pairs per arm of the async context probe */
const ALS_PROBE_ITERATIONS = 200;

const ALL_DATABASES = ['postgres', 'mysql', 'mariadb', 'sqlite'] as const;
const ALL_KV = ['redis', 'valkey'] as const;
// `nuxthub` used to sit here, but it built the same Drizzle SQL provider as
// `drizzle` and differed only in the KV adapter, so its column measured noise
// (the observed delta swung from -4.40 ms to +5.11 ms on the same adapter).
// KV-adapter behaviour is covered by the KVStorage conformance spec instead.
// `per-statement-connection` is the old `hyperdrive` profile under the name of
// what it actually measures: a fresh driver connection per statement against a
// local server, with no Hyperdrive involved.
const ALL_ADAPTER_PROFILES = ['native', 'drizzle', 'per-statement-connection', 'hyperdrive'] as const;
const DEFAULT_ADAPTER_PROFILES = ['native', 'drizzle'] as const;
const SCENARIO_NAMES = [
	'basic_crud',
	'advanced_usage',
	'migration_mapping',
	'bulk_crud',
	'auto_increment',
	'indexing',
	'metadata_fetch',
	'pragma_or_info',
	'counting',
	'shard_fanout',
	'reassignment',
	'mapping_miss',
	'next_id',
	'paginate',
	'batch_write',
	'cold_mapping_cache',
	'concurrent_load',
	'journal_move',
	'stale_route_write',
	'kv_bulk_read',
	'batch_rollback',
	'topology_cas',
	'missing_sidecar_lookup',
	'reconcile_record_probe',
	'recovery_lease_takeover',
	'history_index_ensure',
	'als_overlap',
	'als_no_compat'
] as const;
const SHARDING_STRATEGIES = ['round-robin', 'random', 'hash', 'location'] as const;
const STRATEGY_BENCHMARK_TARGET_REGIONS = ['wnam', 'enam', 'weur', 'apac', 'oc'] as const;
const STRATEGY_BENCHMARK_SHARDS = [
	{ binding: 'db-wnam', location: { region: 'wnam', priority: 3 } },
	{ binding: 'db-enam', location: { region: 'enam', priority: 2 } },
	{ binding: 'db-weur', location: { region: 'weur', priority: 2 } },
	{ binding: 'db-apac', location: { region: 'apac', priority: 2 } },
	{ binding: 'db-oc', location: { region: 'oc', priority: 1 } }
] as const;
const CLOUDFLARE_STRATEGY_SHARD_LOCATIONS: Record<string, ShardLocation> = {
	'db-east': { region: 'enam', priority: 2 },
	'db-west': { region: 'wnam', priority: 2 },
	'db-central': { region: 'weur', priority: 1 }
};

type DatabaseFlavor = (typeof ALL_DATABASES)[number];
type KVFlavor = (typeof ALL_KV)[number];
type AdapterProfile = (typeof ALL_ADAPTER_PROFILES)[number];
type ScenarioName = (typeof SCENARIO_NAMES)[number];
type Status = 'passed' | 'failed' | 'skipped' | 'partial_passed';

interface CLIOptions {
	db: DatabaseFlavor | 'all';
	kv: KVFlavor | 'all';
	profile: AdapterProfile | 'all';
	iterations: number;
	bulkSize: number;
	strategyStatements: number;
	includeCloudflare: boolean;
	cloudflareOnly: boolean;
	/** Runs only these scenarios and skips the strategy matrix; null runs everything */
	only: ScenarioName[] | null;
	/** Iterations per write kind for stale_route_write */
	raceIterations: number;
}

interface Combo {
	db: DatabaseFlavor;
	kv: KVFlavor;
	id: string;
}

interface ScenarioStats {
	name: ScenarioName;
	status: Status;
	iterations: number;
	samplesMs: number[];
	avgMs?: number;
	p50Ms?: number;
	p95Ms?: number;
	minMs?: number;
	maxMs?: number;
	error?: string;
	notes?: string;
}

interface PhaseAttribution {
	phase: string;
	count: number;
	totalMs: number;
	avgMs: number;
	p95Ms: number;
	share: number;
}

interface ComboResult {
	id: string;
	baseId: string;
	profile: AdapterProfile;
	db: DatabaseFlavor | 'cloudflare';
	kv: KVFlavor | 'cloudflare-kv';
	status: Status;
	scenarios: Record<ScenarioName, ScenarioStats>;
	strategyBenchmark?: StrategyBenchmarkResult;
	overallAvgMs?: number;
	perOpAvgMs?: number;
	phaseAttribution?: PhaseAttribution[];
	durationMs: number;
	error?: string;
}

function formatStatus(status: Status): string {
	return status === 'partial_passed' ? 'PARTIAL_PASSED' : status.toUpperCase();
}

interface StrategyMatrixColumn {
	key: string;
	label: string;
	strategy: ShardingStrategy | MixedShardingStrategy;
}

interface StrategyBenchmarkCell {
	status: Status;
	statements: number;
	samplesMs: number[];
	avgMs?: number;
	p50Ms?: number;
	p95Ms?: number;
	minMs?: number;
	maxMs?: number;
	/**
	 * Keys landed per shard. This is the axis strategies actually differ on: the
	 * latency spread across all 16 columns of the old matrix was 5.2% with
	 * near-uniform wins, because without a coordinator every strategy is a few
	 * microseconds of local computation and only decides where a new key goes.
	 */
	keysPerShard?: Record<string, number>;
	/**
	 * Fraction of keys on the busiest shard. Even distribution over N shards puts
	 * this at 1/N; 1.0 means every key landed on one shard. Preferred over a
	 * busiest-to-quietest ratio, which is infinite as soon as one shard is empty
	 * and so cannot rank anything.
	 */
	maxShare?: number;
	/** Chi-square against a uniform distribution over the shards. */
	chiSquare?: number;
	error?: string;
}

interface StrategyBenchmarkRow {
	targetRegion: D1Region;
	columns: Record<string, StrategyBenchmarkCell>;
}

interface StrategyBenchmarkResult {
	statementsPerOperation: number;
	writesPerStrategy: number;
	readsPerStrategy: number;
	shardCount: number;
	shardLocations: Record<string, ShardLocation>;
	databaseSizesBytes?: Record<string, number | undefined>;
	columns: StrategyMatrixColumn[];
	rows: StrategyBenchmarkRow[];
	error?: string;
}

interface StrategyAggregate {
	label: string;
	avgMs: number;
	p95Ms: number;
}

interface RowMetricCell {
	text: string;
	metric?: number;
}

interface SQLRuntime {
	shards: Record<string, SQLDatabase>;
	close: () => Promise<void>;
	/** Opens the same database as a login that can only read the listed tables; Postgres, MySQL and MariaDB only */
	restricted?: (binding: string, readableTables: string[]) => Promise<SQLDatabase>;
}

interface KVRuntime {
	kv: KVStorage;
	close: () => Promise<void>;
}

interface CommandResult {
	code: number;
	stdout: string;
	stderr: string;
}

interface IterationPlan {
	basic: number;
	advanced: number;
	migration: number;
	bulk: number;
	autoIncrement: number;
	indexing: number;
	metadata: number;
	pragma: number;
	counting: number;
	fanout: number;
	reassignment: number;
}

const BENCH_SCHEMA = `
	CREATE TABLE IF NOT EXISTS users (
		id VARCHAR(191) PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		email VARCHAR(255) UNIQUE,
		created_at BIGINT
	);

	CREATE TABLE IF NOT EXISTS posts (
		id VARCHAR(191) PRIMARY KEY,
		user_id VARCHAR(191) NOT NULL,
		title VARCHAR(255) NOT NULL,
		content TEXT,
		created_at BIGINT
	);
`;

function autoIncrementSchemaFor(dbFlavor: DatabaseFlavor): string {
	switch (dbFlavor) {
		case 'postgres':
			return `
	CREATE TABLE IF NOT EXISTS auto_users (
		id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		email VARCHAR(255) UNIQUE,
		created_at BIGINT
	);`;
		case 'mysql':
		case 'mariadb':
			return `
	CREATE TABLE IF NOT EXISTS auto_users (
		id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		email VARCHAR(255) UNIQUE,
		created_at BIGINT
	);`;
		case 'sqlite':
			return `
	CREATE TABLE IF NOT EXISTS auto_users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		email TEXT UNIQUE,
		created_at BIGINT
	);`;
	}
}

function autoIncrementInsertSqlFor(dbFlavor: DatabaseFlavor): string {
	// PostgreSQL and SQLite both support RETURNING clause (SQLite 3.35.0+)
	return dbFlavor === 'postgres' || dbFlavor === 'sqlite'
		? 'INSERT INTO auto_users (name, email, created_at) VALUES (?, ?, ?) RETURNING id'
		: 'INSERT INTO auto_users (name, email, created_at) VALUES (?, ?, ?)';
}

const AUTO_USER_SELECT_SQL = 'SELECT id, name, email, created_at FROM auto_users WHERE id = ?';

const INSERT_USER_SQL = 'INSERT INTO users (id, name, email, created_at) VALUES (?, ?, ?, ?)';
const SELECT_USER_SQL = 'SELECT id, name, email, created_at FROM users WHERE id = ?';
const UPDATE_USER_SQL = 'UPDATE users SET name = ? WHERE id = ?';
const DELETE_USER_SQL = 'DELETE FROM users WHERE id = ?';
const INSERT_POST_SQL = 'INSERT INTO posts (id, user_id, title, content, created_at) VALUES (?, ?, ?, ?, ?)';

const SCENARIO_CATALOG: Record<
	ScenarioName,
	{ title: string; details: string; workload: (plan: IterationPlan, bulkSize: number) => string }
> = {
	basic_crud: {
		title: 'Basic CRUD round-trip',
		details: 'Insert, read, update, and delete a user via routed queries.',
		workload: (plan) => `${plan.basic} iterations; 4 routed SQL ops per iteration`
	},
	advanced_usage: {
		title: 'Advanced lookup workflow',
		details: 'Writes user+post, adds lookup aliases, then validates join and alias-based lookup.',
		workload: (plan) => `${plan.advanced} iterations; ~5 routed SQL ops + KV lookup-key updates per iteration`
	},
	migration_mapping: {
		title: 'Migration-style mapping creation',
		details: 'Inserts legacy records on a fixed shard, then builds shard mappings in batch and validates routing.',
		workload: (plan, bulkSize) =>
			`${plan.migration} iterations; ${Math.max(8, Math.floor(bulkSize / 8))} legacy records mapped per iteration`
	},
	bulk_crud: {
		title: 'Bulk CRUD pressure',
		details: 'Performs bulk inserts, half updates, and full delete sweep, then validates shard-wide totals.',
		workload: (plan, bulkSize) =>
			`${plan.bulk} iterations; ${bulkSize} inserts + ${Math.floor(bulkSize / 2)} updates + ${bulkSize} deletes per iteration`
	},
	auto_increment: {
		title: 'Auto-generated primary keys',
		details:
			'Docker lanes pin a generated-id table to one shard with insertShard, capture the returned key and read it back through the router. Cloudflare lanes allocate ids with nextId, insert them explicitly across shards and read them back, then hand one id to two shards and expect GENERATED_KEY_COLLISION with and without the table option.',
		workload: (plan) => `${plan.autoIncrement} iterations; insert + generated-id readback per iteration (Cloudflare: plus one refusal arm)`
	},
	indexing: {
		title: 'Indexed query scan',
		details: 'Creates an index on posts(user_id) and repeatedly queries the indexed path.',
		workload: (plan) => `${plan.indexing} iterations after warmup dataset build`
	},
	metadata_fetch: {
		title: 'Metadata inspection',
		details: 'Reads table metadata/introspection rows from one shard.',
		workload: (plan) => `${plan.metadata} iterations; 1 metadata query per iteration`
	},
	pragma_or_info: {
		title: 'PRAGMA / server info',
		details: 'Runs provider-specific PRAGMA/info query to sample low-level metadata latency.',
		workload: (plan) => `${plan.pragma} iterations; 1 pragma/info query per iteration`
	},
	counting: {
		title: 'Cross-shard counting',
		details: 'Counts users across all shards to measure fanout aggregation overhead.',
		workload: (plan) => `${plan.counting} iterations; all-shard count aggregation per iteration`
	},
	shard_fanout: {
		title: 'Shard fanout query',
		details: 'Runs query fanout to all shards and aggregates shard-level responses.',
		workload: (plan) => `${plan.fanout} iterations; 1 all-shards query per iteration`
	},
	reassignment: {
		title: 'Shard reassignment flow',
		details: 'Creates a record, reassigns it to another shard, and verifies routed reads still succeed.',
		workload: (plan) => `${plan.reassignment} iterations; insert + reassignment + verification per iteration`
	},
	mapping_miss: {
		title: 'Lookup of a key with no row',
		details: 'Reads a key that was never written, which is the most common shape a public API sees and used to be the most expensive path.',
		workload: (plan) => `${plan.basic} iterations; 1 routed read of an unmapped key per iteration`
	},
	next_id: {
		title: 'Cross-shard id allocation',
		details: 'Allocates a cluster-unique id with nextId, which is the supported way to key a generated-id table that spans shards.',
		workload: (plan) => `${plan.basic} iterations; 1 id allocation per iteration`
	},
	paginate: {
		title: 'Global paginated read',
		details: 'Reads one sorted page across all shards, exercising the per-shard limit pushdown.',
		workload: (plan) => `${plan.counting} iterations; 1 global page per iteration after a warmup dataset`
	},
	batch_write: {
		title: 'Shard-grouped batch write',
		details: 'Writes a batch of routed statements with one round trip per shard rather than one per statement.',
		workload: (plan, bulkSize) => `${plan.bulk} iterations; ${bulkSize} routed statements grouped per shard`
	},
	cold_mapping_cache: {
		title: 'Cold mapping cache read',
		details: 'Reads an existing key with the in-memory mapping cache disabled, so every read pays the KV lookup.',
		workload: (plan) => `${plan.basic} iterations; 1 routed read with no mapping cache per iteration`
	},
	concurrent_load: {
		title: 'Concurrent routed reads',
		details: 'Issues routed reads concurrently rather than one at a time, which every other scenario does.',
		workload: (plan) => `${plan.basic} iterations; 16 concurrent routed reads per iteration`
	},
	journal_move: {
		title: 'Journaled key move',
		details:
			'Moves a key with the crash-safe journal, then runs tombstone-checked writes on the destination and a stale-route write the tombstone must refuse.',
		workload: (plan) => `${plan.reassignment} iterations; move + guarded update, upsert and insert + stale-route refusals per iteration`
	},
	stale_route_write: {
		title: 'Stale-route writes racing the cutover',
		details:
			'Races update, delete, insert and upsert writers against the source cutover on separate connections, and asserts every acknowledged write is on the one owner and no ghost row appears. A control arm with the key lock removed must report violations.',
		workload: () => '--race-iterations per write kind; protocol arm plus control arm; Postgres, MySQL and MariaDB only'
	},
	kv_bulk_read: {
		title: 'Bulk KV read',
		details: 'Reads 100 keys (a tenth of them absent) in one getMany call and checks order and nulls against 100 single gets.',
		workload: (plan) => `${plan.basic} iterations; 1 getMany over 100 keys per iteration`
	},
	batch_rollback: {
		title: 'Failing batch rollback',
		details: 'Runs a batch whose third statement repeats a primary key and checks that the two statements before it were rolled back.',
		workload: (plan) => `${plan.reassignment} iterations; 1 failing 3-statement batch per iteration`
	},
	topology_cas: {
		title: 'Topology tables on the real engine',
		details:
			'Creates the manifest, sidecar and journal tables twice, races manifest publishers on the compare-and-swap, races a stale-generation writer against the fence raise and the census, and runs pruneEpochs (dry run, then real) on three shards. Control arms (a swap with no revision check, a writer with no fence) must report violations.',
		workload: () => '--race-iterations per race kind; protocol arm plus control arm'
	},
	missing_sidecar_lookup: {
		title: 'Missing sidecar table on a journaled read',
		details:
			'Reads a key that has no row from a shard that holds no sidecar tables, and checks the read answers not found, issues no DDL and leaves the tables absent. Records the driver error of the missing table. Then breaks the lookup for a real reason (a revoked privilege on Postgres, MySQL and MariaDB, a renamed column everywhere) and checks the read throws MIGRATION_LOOKUP_UNAVAILABLE, or answers not found with bestEffort.',
		workload: () => 'a few reads per arm; missing-table, followed-tombstone, renamed-column and revoked-privilege arms'
	},
	reconcile_record_probe: {
		title: 'Reconcile record probes on live shards',
		details:
			'Runs reconcileAllocations against a fake ledger and live shards: a row that exists, a row that is missing, a mapping that repair may write, a routed shard that refuses the probe, and a table that does not exist. Checks recordVerified, recordMissing and recordUnchecked, that repair and prune act only on verified rows, and that a run without verifyRecords issues no SQL. Control arms that read a failed probe as missing, or every probe as a hit, must trip the same checks.',
		workload: () => '9 reconcile runs per engine and adapter; 6 ledger keys over 3 shards'
	},
	recovery_lease_takeover: {
		title: 'Recovery lease expiry and takeover',
		details:
			'Leaves a move at cutover_pending or copied, holds recovery A at a protocol step, expires its lease on an injected clock, lets recovery B take over and finish, then releases A and checks it throws RECOVERY_FENCED and writes nothing. Pairs and quads of recoveries started together must give one winner and RECOVERY_IN_PROGRESS for the rest. Control arms (no generation condition on journal writes, no generation read before a shard step, an acquire with no expiry condition) must each report violations.',
		workload: () => 'about a quarter of --race-iterations per arm; three protocol arms plus three control arms'
	},
	history_index_ensure: {
		title: 'History index on a journal made without it',
		details:
			'Creates the journal tables with the old DDL, which holds no history index, then runs ensureJournalSchema and checks the engine catalog holds the index on the key columns. Runs ensureIndex again, then four at once over a dropped index in several rounds, and checks every call succeeds and one index remains. A plain CREATE INDEX run twice must fail, which shows the repeat above is not vacuous.',
		workload: () => '2 sequential ensures and about a quarter of --race-iterations rounds of 4 concurrent ensures'
	},
	als_overlap: {
		title: 'Overlapping collegedb calls on workerd',
		details:
			'Overlaps collegedb callbacks with different shard sets on local workerd with nodejs_compat, awaiting KV, D1, a Durable Object and a timer, and checks each callback sees its own configuration and writes only to its own shards. createClient runs the same overlap. A control arm that calls initialize twice must show cross-talk.',
		workload: () => `${ALS_PROBE_ITERATIONS} overlapping pairs per arm; scoped, createClient and control`
	},
	als_no_compat: {
		title: 'Overlapping collegedb calls without nodejs_compat',
		details:
			'Runs a second Worker without nodejs_compat. The library must load with no async context, a callback with a different configuration must be refused with CONCURRENT_CONFIG while another runs, the same configuration may overlap, and createClient must keep two clusters apart. A control arm that calls initialize twice must show cross-talk.',
		workload: () =>
			`${ALS_PROBE_ITERATIONS} iterations of the refusal arm; ${ALS_PROBE_ITERATIONS} overlapping pairs for createClient and control`
	}
};

function getStrategyBenchmarkShardBindings(): string[] {
	return STRATEGY_BENCHMARK_SHARDS.map((entry) => entry.binding);
}

function getStrategyBenchmarkShardLocations(): Record<string, ShardLocation> {
	const entries = STRATEGY_BENCHMARK_SHARDS.map((entry) => [entry.binding, entry.location] as const);
	return Object.fromEntries(entries);
}

function buildStrategyBenchmarkColumns(): StrategyMatrixColumn[] {
	const columns: StrategyMatrixColumn[] = [];

	for (const strategy of SHARDING_STRATEGIES) {
		columns.push({
			key: `single:${strategy}`,
			label: `single:${strategy}`,
			strategy
		});
	}

	for (const read of SHARDING_STRATEGIES) {
		for (const write of SHARDING_STRATEGIES) {
			if (read === write) {
				continue;
			}

			columns.push({
				key: `mixed:r=${read},w=${write}`,
				label: `mixed:r=${read},w=${write}`,
				strategy: { read, write }
			});
		}
	}

	return columns;
}

const STRATEGY_BENCHMARK_COLUMNS = buildStrategyBenchmarkColumns();

async function main(): Promise<void> {
	const options = parseArgs(Bun.argv.slice(2));
	const combos = options.cloudflareOnly ? [] : buildCombos(options.db, options.kv);

	await mkdir(RESULTS_DIR, { recursive: true });
	await mkdir(TMP_DIR, { recursive: true });

	if (!options.cloudflareOnly) {
		await ensureDockerComposeReady();
	}

	// Every run already isolates itself with run-scoped database names that are
	// created and dropped per run, so tearing the containers down between runs
	// only bought a cold boot of each server. MySQL and MariaDB reinitialise
	// their data directory on every one of those.
	const requiredServices = [...new Set(combos.flatMap((combo) => composeServicesForCombo(combo)))];
	let composeStarted = false;

	if (requiredServices.length > 0) {
		const composeBegan = performance.now();
		await composeDown();
		await composeUp(requiredServices);
		composeStarted = true;
		console.log(`[Sandbox] Brought up ${requiredServices.join(', ')} in ${formatMs(performance.now() - composeBegan)}`);
	}

	const comboResults: ComboResult[] = [];

	for (const combo of combos) {
		const profiles = profilesForCombo(combo.db, options.profile);
		for (const profile of profiles) {
			const startedAt = new Date();
			const wallStarted = performance.now();
			console.log(`\n[Sandbox] [${startedAt.toISOString()}] START ${combo.id} (${profile})`);

			const result = await benchmarkCombo(combo, profile, options);
			comboResults.push(result);

			const completedAt = new Date();
			const wallDurationMs = performance.now() - wallStarted;
			console.log(
				`[Sandbox] [${completedAt.toISOString()}] COMPLETE ${combo.id} (${profile}) => ${result.status.toUpperCase()} (benchmark=${formatMs(result.durationMs)}, wall=${formatMs(wallDurationMs)})`
			);
		}
	}

	const cloudflareResults: ComboResult[] = [];
	if (options.includeCloudflare || options.cloudflareOnly) {
		const cloudflareProfiles = profilesForCloudflare(options.profile);

		// The hyperdrive profile binds env.HYPERDRIVE to the compose Postgres via
		// `localConnectionString`, so that container has to be running even for a
		// Cloudflare-only run.
		if (cloudflareProfiles.includes('hyperdrive') && !composeStarted) {
			await composeDown();
			await composeUp(['postgres']);
			composeStarted = true;
		}
		if (cloudflareProfiles.includes('hyperdrive')) {
			await waitForPostgres();
		}

		for (const profile of cloudflareProfiles) {
			const startedAt = new Date();
			const wallStarted = performance.now();
			console.log(`\n[Sandbox] [${startedAt.toISOString()}] START cloudflare (${profile})`);

			const result = await benchmarkCloudflare(options, profile);
			cloudflareResults.push(result);

			const completedAt = new Date();
			const wallDurationMs = performance.now() - wallStarted;
			console.log(
				`[Sandbox] [${completedAt.toISOString()}] COMPLETE cloudflare (${profile}) => ${result.status.toUpperCase()} (benchmark=${formatMs(result.durationMs)}, wall=${formatMs(wallDurationMs)})`
			);
		}
	}

	if (composeStarted) {
		await composeDown();
	}

	const markdown = buildMarkdownReport(options, comboResults, cloudflareResults);
	const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
	const scope = options.cloudflareOnly
		? 'cloudflare'
		: `${options.db}-${options.kv}-${options.profile}${options.includeCloudflare ? '-plus-cloudflare' : ''}`;
	const outPath = join(RESULTS_DIR, `sandbox-latency-${scope}-${timestamp}.md`);
	const latestPath = join(RESULTS_DIR, 'latest.md');

	await Bun.write(outPath, markdown);

	// `latest.md` is pasted into the CI job summary, so a single-combo run must
	// not overwrite it and present itself as the matrix.
	const isFullMatrix = options.db === 'all' && options.kv === 'all' && options.profile === 'all' && !options.cloudflareOnly;
	if (isFullMatrix) {
		await Bun.write(latestPath, markdown);
	}

	console.log(`\n[Sandbox] Markdown report written: ${outPath}`);
	console.log(isFullMatrix ? `[Sandbox] Latest report updated: ${latestPath}\n` : `[Sandbox] Partial run; ${latestPath} left untouched\n`);
	printMarkdownAnsi(markdown);

	// Calculate pass rate and determine if we should exit with code 1
	const allResults = [...comboResults, ...cloudflareResults];
	const failedCount = allResults.filter((r) => r.status === 'failed').length;
	const passRate = allResults.length > 0 ? (allResults.length - failedCount) / allResults.length : 1;
	const PASS_THRESHOLD = 0.66; // 66% pass rate threshold for acceptable results

	if (passRate < PASS_THRESHOLD) {
		// Less than 66% passed - this is a hard failure
		process.exitCode = 1;
	}
	// 66% or more passed - exit with code 0 even if some failed (PARTIAL_PASSED state handled by status field in markdown)
}

function parseArgs(args: string[]): CLIOptions {
	const defaults: CLIOptions = {
		db: 'all',
		kv: 'all',
		profile: 'all',
		iterations: 20,
		bulkSize: 160,
		strategyStatements: 100,
		includeCloudflare: false,
		cloudflareOnly: false,
		only: null,
		raceIterations: 60
	};

	for (const arg of args) {
		if (arg.startsWith('--db=')) {
			const raw = arg.slice('--db='.length).trim().toLowerCase();
			defaults.db = normalizeDb(raw);
		} else if (arg.startsWith('--kv=')) {
			const raw = arg.slice('--kv='.length).trim().toLowerCase();
			if (raw === 'all' || raw === 'redis' || raw === 'valkey') {
				defaults.kv = raw;
			} else {
				throw new Error(`Unsupported --kv value: ${raw}`);
			}
		} else if (arg.startsWith('--profile=')) {
			const raw = arg.slice('--profile='.length).trim().toLowerCase();
			defaults.profile = normalizeProfile(raw);
		} else if (arg.startsWith('--iterations=')) {
			defaults.iterations = Math.max(1, Number.parseInt(arg.slice('--iterations='.length), 10) || defaults.iterations);
		} else if (arg.startsWith('--bulk-size=')) {
			defaults.bulkSize = Math.max(10, Number.parseInt(arg.slice('--bulk-size='.length), 10) || defaults.bulkSize);
		} else if (arg.startsWith('--strategy-statements=')) {
			defaults.strategyStatements = Math.max(
				10,
				Number.parseInt(arg.slice('--strategy-statements='.length), 10) || defaults.strategyStatements
			);
		} else if (arg.startsWith('--only=')) {
			const names = arg
				.slice('--only='.length)
				.split(',')
				.map((name) => name.trim())
				.filter(Boolean);
			const unknown = names.filter((name) => !(SCENARIO_NAMES as readonly string[]).includes(name));
			if (unknown.length > 0) {
				throw new Error(`Unsupported --only scenario: ${unknown.join(', ')}`);
			}
			defaults.only = names as ScenarioName[];
		} else if (arg.startsWith('--race-iterations=')) {
			defaults.raceIterations = Math.max(2, Number.parseInt(arg.slice('--race-iterations='.length), 10) || defaults.raceIterations);
		} else if (arg === '--include-cloudflare') {
			defaults.includeCloudflare = true;
		} else if (arg === '--cloudflare-only') {
			defaults.cloudflareOnly = true;
		}
	}

	if (defaults.cloudflareOnly) {
		defaults.includeCloudflare = true;
	}

	return defaults;
}

function normalizeDb(raw: string): DatabaseFlavor | 'all' {
	if (raw === 'all') {
		return 'all';
	}
	if (raw === 'postgresql') {
		return 'postgres';
	}
	if ((ALL_DATABASES as readonly string[]).includes(raw)) {
		return raw as DatabaseFlavor;
	}
	throw new Error(`Unsupported --db value: ${raw}`);
}

function normalizeProfile(raw: string): AdapterProfile | 'all' {
	if (raw === 'all') {
		return 'all';
	}
	if ((ALL_ADAPTER_PROFILES as readonly string[]).includes(raw)) {
		return raw as AdapterProfile;
	}
	throw new Error(`Unsupported --profile value: ${raw}`);
}

function profilesForCombo(dbFlavor: DatabaseFlavor, profileFilter: AdapterProfile | 'all'): AdapterProfile[] {
	// `hyperdrive` is deliberately absent: without a Workers runtime there is no
	// Hyperdrive binding to test, which is what the old profile of that name got
	// wrong.
	const supported: AdapterProfile[] = dbFlavor === 'sqlite' ? ['native', 'drizzle'] : ['native', 'drizzle', 'per-statement-connection'];

	if (profileFilter === 'all') {
		// `per-statement-connection` accounted for 40.6% of the full matrix while
		// measuring a pattern no supported deployment uses, so it is opt-in.
		return supported.filter((profile) => (DEFAULT_ADAPTER_PROFILES as readonly AdapterProfile[]).includes(profile));
	}

	return supported.includes(profileFilter) ? [profileFilter] : [];
}

function profilesForCloudflare(profileFilter: AdapterProfile | 'all'): AdapterProfile[] {
	// `hyperdrive` only exists here, because it is the sole place a real
	// `env.HYPERDRIVE` binding exists: `wrangler dev` supplies one from the
	// compose Postgres through `localConnectionString`. It is also the only
	// environment in the matrix where one cluster spans two backends, since the
	// Hyperdrive shard is Postgres and the other two are D1.
	const supported: AdapterProfile[] = ['native', 'drizzle', 'hyperdrive'];
	if (profileFilter === 'all') {
		return supported;
	}
	return supported.includes(profileFilter) ? [profileFilter] : [];
}

function buildCombos(dbFilter: DatabaseFlavor | 'all', kvFilter: KVFlavor | 'all'): Combo[] {
	const dbs = dbFilter === 'all' ? [...ALL_DATABASES] : [dbFilter];
	const kvs = kvFilter === 'all' ? [...ALL_KV] : [kvFilter];
	const combos: Combo[] = [];

	for (const db of dbs) {
		for (const kv of kvs) {
			combos.push({ db, kv, id: `${db}+${kv}` });
		}
	}

	return combos;
}

async function ensureDockerComposeReady(): Promise<void> {
	const check = await runCommand(['docker', 'compose', 'version'], PROJECT_ROOT, true);
	if (check.code !== 0) {
		throw new Error(`docker compose is required.\n${check.stderr || check.stdout}`);
	}
}

async function benchmarkCombo(combo: Combo, profile: AdapterProfile, options: CLIOptions): Promise<ComboResult> {
	const started = performance.now();
	const scenarios = createSkippedScenarioMap('Not run');
	let kvRuntime: KVRuntime | null = null;
	let sqlRuntime: SQLRuntime | null = null;
	let strategyBenchmark: StrategyBenchmarkResult | undefined;
	let phaseAttribution: PhaseAttribution[] | undefined;
	let initialized = false;
	const resultId = `${combo.id}/${profile}`;

	try {
		const runId = createRunId(resultId);
		kvRuntime = await createKVRuntime(combo.kv, profile);
		sqlRuntime = await createSQLRuntime(combo.db, runId, profile);

		const config: CollegeDBConfig = {
			kv: kvRuntime.kv,
			shards: sqlRuntime.shards,
			strategy: 'hash',
			disableAutoMigration: true,
			hashShardMappings: true,
			mappingCacheTtlMs: 60_000,
			knownShardsCacheTtlMs: 10_000,
			sizeCacheTtlMs: 10_000,
			migrationConcurrency: 25
		};

		initialize(config);
		initialized = true;
		await createSchemaAcrossShards(config.shards, `${BENCH_SCHEMA}\n${autoIncrementSchemaFor(combo.db)}`);
		await resetBenchData();

		const plan = buildIterationPlan(options.iterations);
		const selected = (name: ScenarioName) => !options.only || options.only.includes(name);

		if (!options.only) {
			scenarios.basic_crud = await scenarioBasicCrud(plan.basic);

			await resetBenchData();
			scenarios.advanced_usage = await scenarioAdvancedUsage(plan.advanced, config);

			await resetBenchData();
			scenarios.migration_mapping = await scenarioMigrationMapping(plan.migration, config, options.bulkSize);

			await resetBenchData();
			scenarios.bulk_crud = await scenarioBulkCrud(plan.bulk, options.bulkSize);

			await resetBenchData();
			scenarios.auto_increment = await scenarioAutoIncrement(plan.autoIncrement, combo.db);

			await resetBenchData();
			scenarios.indexing = await scenarioIndexing(plan.indexing);

			await resetBenchData();
			scenarios.metadata_fetch = await scenarioMetadataFetch(plan.metadata, combo.db);

			scenarios.pragma_or_info = await scenarioPragmaOrInfo(plan.pragma, combo.db);

			await resetBenchData();
			scenarios.counting = await scenarioCounting(plan.counting, options.bulkSize);

			await resetBenchData();
			scenarios.shard_fanout = await scenarioShardFanout(plan.fanout, options.bulkSize);

			await resetBenchData();
			scenarios.reassignment = await scenarioReassignment(plan.reassignment, config);

			await resetBenchData();
			scenarios.mapping_miss = await scenarioMappingMiss(plan.basic);

			await resetBenchData();
			scenarios.next_id = await scenarioNextId(plan.basic);

			await resetBenchData();
			scenarios.paginate = await scenarioPaginate(plan.counting, options.bulkSize);

			await resetBenchData();
			scenarios.batch_write = await scenarioBatchWrite(plan.bulk, options.bulkSize);

			await resetBenchData();
			scenarios.cold_mapping_cache = await scenarioColdMappingCache(plan.basic, config);

			await resetBenchData();
			scenarios.concurrent_load = await scenarioConcurrentLoad(plan.basic);
		}

		if (selected('journal_move')) {
			await resetBenchData();
			scenarios.journal_move = await scenarioJournalMove(combo.db, profile, kvRuntime.kv, plan.reassignment);
			initialize(config);
		}

		if (selected('stale_route_write')) {
			await resetBenchData();
			scenarios.stale_route_write = await scenarioStaleRouteWrite(combo.db, profile, kvRuntime.kv, options.raceIterations);
			initialize(config);
		}

		if (selected('kv_bulk_read')) {
			scenarios.kv_bulk_read = await scenarioKvBulkRead(kvRuntime.kv, combo.kv, plan.basic);
		}

		if (selected('batch_rollback')) {
			scenarios.batch_rollback = await scenarioBatchRollback(sqlRuntime.shards, plan.reassignment);
		}

		if (selected('topology_cas')) {
			await resetBenchData();
			scenarios.topology_cas = await scenarioTopologyCas(combo.db, profile, kvRuntime.kv, options.raceIterations);
			initialize(config);
		}

		if (selected('missing_sidecar_lookup')) {
			scenarios.missing_sidecar_lookup = await scenarioMissingSidecarLookup(combo.db, profile, kvRuntime.kv);
			initialize(config);
		}

		if (selected('reconcile_record_probe')) {
			scenarios.reconcile_record_probe = await scenarioReconcileRecordProbe(combo.db, profile, kvRuntime.kv);
			initialize(config);
		}

		if (selected('recovery_lease_takeover')) {
			scenarios.recovery_lease_takeover = await scenarioRecoveryLeaseTakeover(combo.db, profile, options.raceIterations);
			initialize(config);
		}

		if (selected('history_index_ensure')) {
			scenarios.history_index_ensure = await scenarioHistoryIndexEnsure(combo.db, profile, options.raceIterations);
		}

		scenarios.als_overlap = createSkippedScenario('als_overlap', 'needs workerd; the cloudflare lane runs it');
		scenarios.als_no_compat = createSkippedScenario('als_no_compat', 'needs workerd; the cloudflare lane runs it');

		if (!options.only) {
			await resetBenchData();
			phaseAttribution = await measurePhaseAttribution(config, Math.max(5, Math.floor(options.iterations / 2)));

			await resetBenchData();
			strategyBenchmark = await benchmarkStrategyBulkMatrix(combo.db, profile, options, kvRuntime.kv);
		}

		const totalScenarios = Object.values(scenarios).length;
		const failedScenarios = Object.values(scenarios).filter((scenario) => scenario.status === 'failed').length;
		const scenarioPassRate = totalScenarios > 0 ? (totalScenarios - failedScenarios) / totalScenarios : 1;
		const status: Status =
			failedScenarios > 0 && scenarioPassRate >= 0.66
				? 'partial_passed'
				: Object.values(scenarios).some((scenario) => scenario.status === 'failed') || strategyBenchmarkHasFailures(strategyBenchmark)
					? 'failed'
					: 'passed';
		return {
			id: resultId,
			baseId: combo.id,
			profile,
			db: combo.db,
			kv: combo.kv,
			status,
			scenarios,
			strategyBenchmark,
			overallAvgMs: computeOverallAverage(scenarios),
			perOpAvgMs: computePerOpAverage(scenarios, options.bulkSize),
			phaseAttribution,
			durationMs: performance.now() - started
		};
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		return {
			id: resultId,
			baseId: combo.id,
			profile,
			db: combo.db,
			kv: combo.kv,
			status: 'failed',
			scenarios,
			strategyBenchmark,
			durationMs: performance.now() - started,
			error: message
		};
	} finally {
		if (initialized) {
			await safely(async () => flush());
		}
		resetConfig();

		if (kvRuntime) {
			await safely(async () => kvRuntime?.close());
		}

		if (sqlRuntime) {
			await safely(async () => sqlRuntime?.close());
		}
	}
}

async function benchmarkCloudflare(options: CLIOptions, profile: AdapterProfile): Promise<ComboResult> {
	const scenarios = createSkippedScenarioMap('Not run');
	const started = performance.now();
	const resultId = `cloudflare/${profile}`;
	let strategyBenchmark: StrategyBenchmarkResult | undefined;

	const wranglerArgs = [
		WRANGLER_BIN,
		'dev',
		'--config',
		WRANGLER_CONFIG,
		'--port',
		'8787',
		'--ip',
		'127.0.0.1',
		'--local',
		'--log-level',
		'error'
	];

	const proc = Bun.spawn(wranglerArgs, {
		cwd: PROJECT_ROOT,
		stdout: 'pipe',
		stderr: 'pipe'
	});

	const stdoutPromise = new Response(proc.stdout).text();
	const stderrPromise = new Response(proc.stderr).text();

	try {
		await waitForHttp(cloudflareUrl('/health', profile), 90_000);
		await assertOk(await fetch(cloudflareUrl('/init', profile), { method: 'POST' }), 'POST /init');

		const plan = buildIterationPlan(Math.max(8, options.iterations));
		const selected = (name: ScenarioName) => !options.only || options.only.includes(name);

		if (!options.only) {
			await resetCloudflareBenchmarkData(profile);
			scenarios.basic_crud = await scenarioCloudflareBasicCrud(Math.max(6, Math.floor(plan.basic / 2)), profile);

			await resetCloudflareBenchmarkData(profile);
			scenarios.advanced_usage = await scenarioCloudflareAdvanced(Math.max(5, Math.floor(plan.advanced / 2)), profile);

			await resetCloudflareBenchmarkData(profile);
			scenarios.migration_mapping = await scenarioCloudflareMigration(
				Math.max(4, Math.floor(plan.migration / 2)),
				Math.max(12, Math.floor(options.bulkSize / 6)),
				profile
			);

			await resetCloudflareBenchmarkData(profile);
			scenarios.bulk_crud = await scenarioCloudflareBulk(
				Math.max(3, Math.floor(plan.bulk / 2)),
				Math.max(20, Math.floor(options.bulkSize / 4)),
				profile
			);

			await resetCloudflareBenchmarkData(profile);
			scenarios.auto_increment = await scenarioCloudflareAutoIncrement(Math.max(4, Math.floor(plan.autoIncrement / 2)), profile);

			await resetCloudflareBenchmarkData(profile);
			scenarios.indexing = await scenarioCloudflareIndexing(
				Math.max(4, Math.floor(plan.indexing / 2)),
				Math.max(60, Math.floor(options.bulkSize * 0.75)),
				profile
			);

			scenarios.metadata_fetch = await scenarioCloudflareMetadata(Math.max(6, Math.floor(plan.metadata / 2)), profile);
			scenarios.pragma_or_info = await scenarioCloudflarePragma(Math.max(6, Math.floor(plan.pragma / 2)), profile);

			await resetCloudflareBenchmarkData(profile);
			scenarios.counting = await scenarioCloudflareCounting(
				Math.max(6, Math.floor(plan.counting / 2)),
				Math.max(30, Math.floor(options.bulkSize / 2)),
				profile
			);

			scenarios.shard_fanout = await scenarioCloudflareFanout(Math.max(6, Math.floor(plan.fanout / 2)), profile);

			// The only place the batch API runs against D1, which is the backend it
			// exists for: D1 caps a Worker invocation at 1000 queries on the paid plan
			// and 50 on the free one.
			await resetCloudflareBenchmarkData(profile);
			scenarios.batch_write = await scenarioCloudflareBatchWrite(
				Math.max(3, Math.floor(plan.bulk / 2)),
				Math.max(40, Math.floor(options.bulkSize / 2)),
				profile
			);

			await resetCloudflareBenchmarkData(profile);
			scenarios.reassignment = await scenarioCloudflareReassignment(Math.max(4, Math.floor(plan.reassignment / 2)), profile);
		}

		if (selected('journal_move')) {
			scenarios.journal_move = await scenarioCloudflareJournalMove(Math.max(4, Math.floor(plan.reassignment / 2)), profile);
		}
		if (selected('kv_bulk_read')) {
			scenarios.kv_bulk_read = await scenarioCloudflareKvBulkRead(profile);
		}
		if (selected('batch_rollback')) {
			scenarios.batch_rollback = await scenarioCloudflareBatchRollback(profile);
		}
		if (selected('als_overlap')) {
			scenarios.als_overlap = await scenarioCloudflareAlsOverlap(profile);
		}
		if (selected('als_no_compat')) {
			scenarios.als_no_compat = await scenarioCloudflareAlsNoCompat(profile);
		}
		if (selected('stale_route_write')) {
			scenarios.stale_route_write = createSkippedScenario(
				'stale_route_write',
				'local D1 is one SQLite file behind workerd, so there is no second connection to race; the Docker lanes and the worker-thread race cover it'
			);
		}
		if (selected('topology_cas')) {
			scenarios.topology_cas = createSkippedScenario(
				'topology_cas',
				'local D1 is one SQLite file behind workerd, so the swap and the fence have no second connection to race; the Docker lanes cover them'
			);
		}
		for (const name of ['missing_sidecar_lookup', 'reconcile_record_probe', 'recovery_lease_takeover', 'history_index_ensure'] as const) {
			if (selected(name)) {
				scenarios[name] = createSkippedScenario(
					name,
					'local D1 has no second connection, no privilege to revoke and no engine error codes to record; the Docker lanes cover it'
				);
			}
		}

		if (!options.only) {
			strategyBenchmark = await benchmarkCloudflareStrategyBulkMatrix(options, profile);
		}

		const totalScenarios = Object.values(scenarios).length;
		const failedScenarios = Object.values(scenarios).filter((scenario) => scenario.status === 'failed').length;
		const scenarioPassRate = totalScenarios > 0 ? (totalScenarios - failedScenarios) / totalScenarios : 1;
		const status: Status =
			failedScenarios > 0 && scenarioPassRate >= 0.66
				? 'partial_passed'
				: Object.values(scenarios).some((scenario) => scenario.status === 'failed') || strategyBenchmarkHasFailures(strategyBenchmark)
					? 'failed'
					: 'passed';
		return {
			id: resultId,
			baseId: 'cloudflare',
			profile,
			db: 'cloudflare',
			kv: 'cloudflare-kv',
			status,
			scenarios,
			strategyBenchmark,
			overallAvgMs: computeOverallAverage(scenarios),
			perOpAvgMs: computePerOpAverage(scenarios, options.bulkSize),
			durationMs: performance.now() - started
		};
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);

		// The dev server has to be stopped before its pipes are read. Awaiting them
		// while it is still running waits for an EOF that never arrives, so every
		// failure in this lane hung with no diagnostic instead of reporting the
		// error that caused it.
		await terminateProcess(proc);
		const stderr = await readCapturedOutput(stderrPromise);
		const stdout = await readCapturedOutput(stdoutPromise);
		return {
			id: resultId,
			baseId: 'cloudflare',
			profile,
			db: 'cloudflare',
			kv: 'cloudflare-kv',
			status: 'failed',
			scenarios,
			strategyBenchmark,
			durationMs: performance.now() - started,
			error: `${message}\n${trimForReport(stderr || stdout, 2000)}`.trim()
		};
	} finally {
		await terminateProcess(proc);
	}
}

function createFailedCloudflareStrategyBenchmarkResult(statements: number, error: string): StrategyBenchmarkResult {
	const rows: StrategyBenchmarkRow[] = STRATEGY_BENCHMARK_TARGET_REGIONS.map((targetRegion) => {
		const failedColumns = Object.fromEntries(
			STRATEGY_BENCHMARK_COLUMNS.map((column) => [
				column.key,
				{
					status: 'failed' as Status,
					statements: statements * 2,
					samplesMs: [],
					error
				} satisfies StrategyBenchmarkCell
			])
		) as Record<string, StrategyBenchmarkCell>;

		return {
			targetRegion,
			columns: failedColumns
		};
	});

	return {
		statementsPerOperation: statements,
		writesPerStrategy: statements,
		readsPerStrategy: statements,
		shardCount: Object.keys(CLOUDFLARE_STRATEGY_SHARD_LOCATIONS).length,
		shardLocations: CLOUDFLARE_STRATEGY_SHARD_LOCATIONS,
		databaseSizesBytes: undefined,
		columns: STRATEGY_BENCHMARK_COLUMNS,
		rows,
		error
	};
}

async function benchmarkCloudflareStrategyBulkMatrix(options: CLIOptions, profile: AdapterProfile): Promise<StrategyBenchmarkResult> {
	const statements = Math.max(10, Math.floor(options.strategyStatements / 2));

	try {
		const rows: StrategyBenchmarkRow[] = [];

		for (const targetRegion of STRATEGY_BENCHMARK_TARGET_REGIONS) {
			const columns: Record<string, StrategyBenchmarkCell> = {};

			for (const column of STRATEGY_BENCHMARK_COLUMNS) {
				columns[column.key] = await runCloudflareStrategyBenchmarkCell(column, targetRegion, statements, profile);
			}

			rows.push({
				targetRegion,
				columns
			});
		}

		const databaseSizesBytes = await collectCloudflareStrategyBenchmarkDatabaseSizes(profile);

		return {
			statementsPerOperation: statements,
			writesPerStrategy: statements,
			readsPerStrategy: statements,
			shardCount: Object.keys(CLOUDFLARE_STRATEGY_SHARD_LOCATIONS).length,
			shardLocations: CLOUDFLARE_STRATEGY_SHARD_LOCATIONS,
			databaseSizesBytes,
			columns: STRATEGY_BENCHMARK_COLUMNS,
			rows
		};
	} catch (error) {
		return createFailedCloudflareStrategyBenchmarkResult(statements, error instanceof Error ? error.message : String(error));
	}
}

async function runCloudflareStrategyBenchmarkCell(
	column: StrategyMatrixColumn,
	targetRegion: D1Region,
	statementsPerOperation: number,
	profile: AdapterProfile
): Promise<StrategyBenchmarkCell> {
	try {
		const response = await postCloudflareBenchmark(
			'/api/benchmark/strategy-bulk-rw',
			{
				strategy: column.strategy,
				targetRegion,
				writes: statementsPerOperation,
				reads: statementsPerOperation,
				prefix: `${column.key.replace(/[^a-z0-9]+/gi, '_')}_${targetRegion}`
			},
			profile
		);

		const body = (await response.json()) as {
			success?: boolean;
			samplesMs?: number[];
			statements?: number;
			error?: string;
		};

		const samplesMs = Array.isArray(body.samplesMs)
			? body.samplesMs.filter((sample): sample is number => typeof sample === 'number' && Number.isFinite(sample))
			: [];

		if (!body.success) {
			return {
				status: 'failed',
				statements: typeof body.statements === 'number' ? body.statements : samplesMs.length,
				samplesMs,
				...calculateLatencyStats(samplesMs),
				error: body.error ?? 'Cloudflare strategy benchmark endpoint reported failure'
			};
		}

		return {
			status: 'passed',
			statements: typeof body.statements === 'number' ? body.statements : samplesMs.length,
			samplesMs,
			...calculateLatencyStats(samplesMs)
		};
	} catch (error) {
		return {
			status: 'failed',
			statements: 0,
			samplesMs: [],
			...calculateLatencyStats([]),
			error: error instanceof Error ? error.message : String(error)
		};
	}
}

async function collectCloudflareStrategyBenchmarkDatabaseSizes(
	profile: AdapterProfile
): Promise<Record<string, number | undefined> | undefined> {
	try {
		const response = await fetch(cloudflareUrl('/api/benchmark/database-sizes', profile));
		await assertOk(response, 'GET /api/benchmark/database-sizes');
		const body = (await response.json()) as {
			success?: boolean;
			sizes?: Record<string, unknown>;
		};

		if (!body.success || !body.sizes || typeof body.sizes !== 'object') {
			return undefined;
		}

		const out: Record<string, number | undefined> = {};
		for (const [binding, value] of Object.entries(body.sizes)) {
			const numeric = Number(value);
			out[binding] = Number.isFinite(numeric) ? numeric : undefined;
		}

		return out;
	} catch {
		return undefined;
	}
}

function strategyBenchmarkHasFailures(strategyBenchmark?: StrategyBenchmarkResult): boolean {
	if (!strategyBenchmark) {
		return false;
	}

	if (strategyBenchmark.error) {
		return true;
	}

	for (const row of strategyBenchmark.rows) {
		for (const column of strategyBenchmark.columns) {
			const cell = row.columns[column.key];
			if (!cell || cell.status === 'failed') {
				return true;
			}
		}
	}

	return false;
}

function createFailedStrategyBenchmarkResult(statements: number, error: string): StrategyBenchmarkResult {
	const rows: StrategyBenchmarkRow[] = STRATEGY_BENCHMARK_TARGET_REGIONS.map((targetRegion) => {
		const failedColumns = Object.fromEntries(
			STRATEGY_BENCHMARK_COLUMNS.map((column) => [
				column.key,
				{
					status: 'failed' as Status,
					statements: statements * 2,
					samplesMs: [],
					error
				} satisfies StrategyBenchmarkCell
			])
		) as Record<string, StrategyBenchmarkCell>;

		return {
			targetRegion,
			columns: failedColumns
		};
	});

	return {
		statementsPerOperation: statements,
		writesPerStrategy: statements,
		readsPerStrategy: statements,
		shardCount: STRATEGY_BENCHMARK_SHARDS.length,
		shardLocations: getStrategyBenchmarkShardLocations(),
		columns: STRATEGY_BENCHMARK_COLUMNS,
		rows,
		error
	};
}

async function benchmarkStrategyBulkMatrix(
	dbFlavor: DatabaseFlavor,
	profile: AdapterProfile,
	options: CLIOptions,
	kv: KVStorage
): Promise<StrategyBenchmarkResult> {
	let sqlRuntime: SQLRuntime | null = null;
	const statements = options.strategyStatements;
	const shardBindings = getStrategyBenchmarkShardBindings();
	const shardLocations = getStrategyBenchmarkShardLocations();

	try {
		sqlRuntime = await createSQLRuntime(dbFlavor, createRunId(`strategy_${dbFlavor}_${profile}`), profile, shardBindings);

		const baseConfig: Omit<CollegeDBConfig, 'strategy' | 'targetRegion'> = {
			kv,
			shards: sqlRuntime.shards,
			shardLocations,
			disableAutoMigration: true,
			hashShardMappings: true,
			mappingCacheTtlMs: 60_000,
			knownShardsCacheTtlMs: 10_000,
			sizeCacheTtlMs: 10_000,
			migrationConcurrency: 25
		};

		initialize({ ...baseConfig, strategy: 'hash', targetRegion: 'wnam' });
		await createSchemaAcrossShards(sqlRuntime.shards, BENCH_SCHEMA);

		const rows: StrategyBenchmarkRow[] = [];

		for (const targetRegion of STRATEGY_BENCHMARK_TARGET_REGIONS) {
			const columns: Record<string, StrategyBenchmarkCell> = {};

			for (const column of STRATEGY_BENCHMARK_COLUMNS) {
				initialize({
					...baseConfig,
					strategy: column.strategy,
					targetRegion
				});

				columns[column.key] = await runStrategyBenchmarkCell(column, targetRegion, statements);
			}

			rows.push({
				targetRegion,
				columns
			});
		}

		const databaseSizesBytes = await collectStrategyBenchmarkDatabaseSizes(dbFlavor, shardBindings);

		return {
			statementsPerOperation: statements,
			writesPerStrategy: statements,
			readsPerStrategy: statements,
			shardCount: shardBindings.length,
			shardLocations,
			databaseSizesBytes,
			columns: STRATEGY_BENCHMARK_COLUMNS,
			rows
		};
	} catch (error) {
		return createFailedStrategyBenchmarkResult(statements, error instanceof Error ? error.message : String(error));
	} finally {
		await safely(async () => flush());
		resetConfig();

		if (sqlRuntime) {
			await safely(async () => sqlRuntime?.close());
		}
	}
}

async function runStrategyBenchmarkCell(
	column: StrategyMatrixColumn,
	targetRegion: D1Region,
	statementsPerOperation: number
): Promise<StrategyBenchmarkCell> {
	const samplesMs: number[] = [];
	const existingReadCount = Math.floor(statementsPerOperation / 2);
	const missReadCount = statementsPerOperation - existingReadCount;

	try {
		await resetBenchData();

		const prefix = `${column.key.replace(/[^a-z0-9]+/gi, '_')}_${targetRegion}_${Date.now()}_${Math.floor(Math.random() * 10_000)}`;
		const writeKeys = new Array(statementsPerOperation).fill(null).map((_, i) => `${prefix}_write_${i}`);

		for (let i = 0; i < writeKeys.length; i++) {
			const key = writeKeys[i]!;
			const sample = await measureLatencySample(async () => {
				await run(key, INSERT_USER_SQL, [key, `Strategy Write ${i}`, `${key}@strategy.local`, Date.now()]);
			});
			samplesMs.push(sample);
		}

		for (let i = 0; i < existingReadCount; i++) {
			const key = writeKeys[i]!;
			const sample = await measureLatencySample(async () => {
				const row = await first<Record<string, unknown>>(key, SELECT_USER_SQL, [key]);
				if (!row) {
					throw new Error(`Missing row for existing-key read: ${key}`);
				}
			});
			samplesMs.push(sample);
		}

		for (let i = 0; i < missReadCount; i++) {
			const key = `${prefix}_readmiss_${i}`;
			const sample = await measureLatencySample(async () => {
				await first<Record<string, unknown>>(key, SELECT_USER_SQL, [key]);
			});
			samplesMs.push(sample);
		}

		return {
			status: 'passed',
			statements: samplesMs.length,
			samplesMs,
			...calculateLatencyStats(samplesMs),
			...(await measureKeyDistribution(getStrategyBenchmarkShardBindings()))
		};
	} catch (error) {
		return {
			status: 'failed',
			statements: samplesMs.length,
			samplesMs,
			...calculateLatencyStats(samplesMs),
			error: error instanceof Error ? error.message : String(error)
		};
	}
}

/**
 * Counts how the just-written keys landed across the shards.
 *
 * Distribution is what separates the strategies; latency does not. Sizes cannot
 * substitute either, because SQLite page granularity reported all five shards at
 * exactly 64.00 KB for a hundred rows.
 */
async function measureKeyDistribution(
	expectedShards: string[]
): Promise<Pick<StrategyBenchmarkCell, 'keysPerShard' | 'maxShare' | 'chiSquare'>> {
	try {
		const stats = await getShardStats();

		// getShardStats merges the configured shards with every shard the KV store
		// still remembers, so earlier scenarios leave extra bindings in the list.
		// Counting those inflates the chi-square baseline: five shards holding
		// twenty keys each scored 40.0 instead of 0 because two stale bindings were
		// being scored as empty.
		const expected = new Set(expectedShards);
		const keysPerShard: Record<string, number> = Object.fromEntries(expectedShards.map((shard) => [shard, 0]));
		for (const stat of stats) {
			if (expected.has(stat.binding)) {
				keysPerShard[stat.binding] = stat.count;
			}
		}

		const counts = Object.values(keysPerShard);
		const total = counts.reduce((sum, count) => sum + count, 0);
		if (counts.length === 0 || total === 0) {
			return { keysPerShard };
		}

		const ideal = total / counts.length;
		const busiest = Math.max(...counts);

		return {
			keysPerShard,
			maxShare: busiest / total,
			chiSquare: counts.reduce((sum, observed) => sum + (observed - ideal) ** 2 / ideal, 0)
		};
	} catch {
		return {};
	}
}

async function collectStrategyBenchmarkDatabaseSizes(
	dbFlavor: DatabaseFlavor,
	shardBindings: string[]
): Promise<Record<string, number | undefined>> {
	const out: Record<string, number | undefined> = {};

	for (const shardBinding of shardBindings) {
		out[shardBinding] = await getShardDatabaseSizeAfterBenchmark(dbFlavor, shardBinding);
	}

	return out;
}

async function getShardDatabaseSizeAfterBenchmark(dbFlavor: DatabaseFlavor, shardBinding: string): Promise<number | undefined> {
	try {
		switch (dbFlavor) {
			case 'postgres': {
				const result = await allShard<Record<string, unknown>>(shardBinding, 'SELECT pg_database_size(current_database()) AS size_bytes');
				return extractFirstNumericFromRows(result.results);
			}
			case 'mysql':
			case 'mariadb': {
				const result = await allShard<Record<string, unknown>>(
					shardBinding,
					'SELECT COALESCE(SUM(data_length + index_length), 0) AS size_bytes FROM information_schema.tables WHERE table_schema = DATABASE()'
				);
				return extractFirstNumericFromRows(result.results);
			}
			case 'sqlite': {
				const pageCountResult = await allShard<Record<string, unknown>>(shardBinding, 'PRAGMA page_count');
				const pageSizeResult = await allShard<Record<string, unknown>>(shardBinding, 'PRAGMA page_size');
				const pageCount = extractFirstNumericFromRows(pageCountResult.results);
				const pageSize = extractFirstNumericFromRows(pageSizeResult.results);
				if (typeof pageCount === 'number' && typeof pageSize === 'number') {
					return pageCount * pageSize;
				}
				return undefined;
			}
		}
	} catch {
		return undefined;
	}
}

function extractFirstNumericFromRows(rows: Record<string, unknown>[]): number | undefined {
	const firstRow = rows[0];
	if (!firstRow) {
		return undefined;
	}

	for (const value of Object.values(firstRow)) {
		const numeric = Number(value);
		if (Number.isFinite(numeric)) {
			return numeric;
		}
	}

	return undefined;
}

async function scenarioBasicCrud(iterations: number): Promise<ScenarioStats> {
	return measureScenario('basic_crud', iterations, async (i) => {
		const id = `basic-${Date.now()}-${i}-${Math.floor(Math.random() * 100_000)}`;
		const email = `${id}@bench.local`;
		await run(id, INSERT_USER_SQL, [id, `Basic ${i}`, email, Date.now()]);
		const found = await first<Record<string, unknown>>(id, SELECT_USER_SQL, [id]);
		if (!found) {
			throw new Error('Insert/select roundtrip failed');
		}
		await run(id, UPDATE_USER_SQL, [`Basic ${i} Updated`, id]);
		await run(id, DELETE_USER_SQL, [id]);
	});
}

async function scenarioAdvancedUsage(iterations: number, config: CollegeDBConfig): Promise<ScenarioStats> {
	const mapper = new KVShardMapper(config.kv, {
		hashShardMappings: config.hashShardMappings,
		mappingCacheTtlMs: config.mappingCacheTtlMs,
		knownShardsCacheTtlMs: config.knownShardsCacheTtlMs
	});

	return measureScenario('advanced_usage', iterations, async (i) => {
		const id = `advanced-${Date.now()}-${i}-${Math.floor(Math.random() * 100_000)}`;
		const email = `${id}@advanced.local`;
		await run(id, INSERT_USER_SQL, [id, `Advanced ${i}`, email, Date.now()]);
		await run(id, INSERT_POST_SQL, [`post-${id}`, id, `Post ${i}`, 'advanced scenario post', Date.now()]);

		await mapper.addLookupKeys(id, [`email:${email}`, `username:${id}`]);

		const joined = await all<{ id: string; title: string }>(
			id,
			'SELECT u.id, p.title FROM users u LEFT JOIN posts p ON p.user_id = u.id WHERE u.id = ?',
			[id]
		);
		if (joined.results.length === 0) {
			throw new Error('Join query returned no rows');
		}

		const byEmail = await first<Record<string, unknown>>(`email:${email}`, 'SELECT * FROM users WHERE email = ?', [email]);
		if (!byEmail) {
			throw new Error('Lookup by additional key failed');
		}
	});
}

async function scenarioMigrationMapping(iterations: number, config: CollegeDBConfig, bulkSize: number): Promise<ScenarioStats> {
	const mapper = new KVShardMapper(config.kv, {
		hashShardMappings: config.hashShardMappings,
		mappingCacheTtlMs: config.mappingCacheTtlMs,
		knownShardsCacheTtlMs: config.knownShardsCacheTtlMs
	});

	const recordsPerIteration = Math.max(8, Math.floor(bulkSize / 8));

	return measureScenario('migration_mapping', iterations, async (i) => {
		const prefix = `legacy-${Date.now()}-${i}`;
		const keys: string[] = [];
		for (let j = 0; j < recordsPerIteration; j++) {
			const id = `${prefix}-${j}`;
			keys.push(id);
			await runShard('shard-a', INSERT_USER_SQL, [id, `Legacy ${j}`, `${id}@legacy.local`, Date.now()]);
		}

		await createMappingsForExistingKeys(keys, ['shard-a'], 'hash', mapper, {
			concurrency: Math.min(25, keys.length)
		});

		const firstKey = keys[0];
		if (!firstKey) {
			throw new Error('No legacy keys generated');
		}

		const mapping = await mapper.getShardMapping(firstKey);
		if (!mapping || mapping.shard !== 'shard-a') {
			throw new Error('Expected migration mapping to shard-a');
		}

		const migrated = await first(firstKey, SELECT_USER_SQL, [firstKey]);
		if (!migrated) {
			throw new Error('Mapped record lookup failed after migration mapping');
		}
	});
}

async function scenarioBulkCrud(iterations: number, bulkSize: number): Promise<ScenarioStats> {
	return measureScenario('bulk_crud', iterations, async (i) => {
		const ids = new Array(bulkSize).fill(null).map((_, idx) => `bulk-${Date.now()}-${i}-${idx}`);

		await runInBatches(ids, 20, async (id, idx) => {
			await run(id, INSERT_USER_SQL, [id, `Bulk ${idx}`, `${id}@bulk.local`, Date.now()]);
		});

		const totalAfterInsert = await totalUserCount();
		if (totalAfterInsert < bulkSize) {
			throw new Error(`Expected at least ${bulkSize} users after insert, found ${totalAfterInsert}`);
		}

		const updates = ids.slice(0, Math.floor(ids.length / 2));
		await runInBatches(updates, 20, async (id, idx) => {
			await run(id, UPDATE_USER_SQL, [`Bulk Updated ${idx}`, id]);
		});

		await runInBatches(ids, 20, async (id) => {
			await run(id, DELETE_USER_SQL, [id]);
		});

		const totalAfterDelete = await totalUserCount();
		if (totalAfterDelete !== 0) {
			throw new Error(`Expected 0 users after delete, found ${totalAfterDelete}`);
		}
	});
}

async function scenarioAutoIncrement(iterations: number, dbFlavor: DatabaseFlavor): Promise<ScenarioStats> {
	const insertSql = autoIncrementInsertSqlFor(dbFlavor);

	// Every shard runs its own AUTOINCREMENT/SERIAL sequence, so `insert()`
	// across shards eventually mints the same id twice and CollegeDB now refuses
	// the second one. Pinning the generated-id table to one shard is the
	// supported way to keep using the database's own sequence; `nextId()` is the
	// other, and `next_id` covers it.
	const generatedIdShard = 'shard-a';

	return measureScenario('auto_increment', iterations, async (i) => {
		const routingKey = `auto-${Date.now()}-${i}-${Math.floor(Math.random() * 100_000)}`;
		const email = `${routingKey}@auto.local`;
		const result = await insertShard<{ id: number | string }>(generatedIdShard, insertSql, [`Auto ${i}`, email, Date.now()]);
		const generatedId = result.generatedId;

		const row = await first<Record<string, unknown>>(String(generatedId), AUTO_USER_SELECT_SQL, [generatedId]);
		if (!row) {
			throw new Error('Generated-id insert could not be read back');
		}

		const returnedId =
			typeof row === 'number' || typeof row === 'string'
				? row
				: Array.isArray(row)
					? row[0]
					: (row.id ?? row.rowid ?? row.last_row_id ?? row.lastInsertId ?? row.insertId);
		if (returnedId !== generatedId && String(returnedId) !== String(generatedId)) {
			throw new Error(
				`Generated-id readback returned a different id (generated=${String(generatedId)}, returned=${String(returnedId)}, row=${JSON.stringify(row)})`
			);
		}
	});
}

async function scenarioIndexing(iterations: number): Promise<ScenarioStats> {
	const userId = `index-user-${Date.now()}`;
	const indexName = `idx_posts_user_id_${Date.now()}_${Math.floor(Math.random() * 10_000)}`;

	await run(userId, INSERT_USER_SQL, [userId, 'Indexed User', `${userId}@idx.local`, Date.now()]);
	for (let i = 0; i < 120; i++) {
		await run(userId, INSERT_POST_SQL, [`idx-post-${i}-${Date.now()}`, userId, `Indexed Post ${i}`, 'indexed content', Date.now()]);
	}

	await runShard('shard-a', `CREATE INDEX ${indexName} ON posts (user_id)`);

	return measureScenario('indexing', iterations, async () => {
		const posts = await all(userId, 'SELECT id, user_id, title FROM posts WHERE user_id = ?', [userId]);
		if (posts.results.length === 0) {
			throw new Error('Indexed query returned no rows');
		}
	});
}

async function scenarioMetadataFetch(iterations: number, dbFlavor: DatabaseFlavor): Promise<ScenarioStats> {
	const sql = metadataQueryFor(dbFlavor);
	return measureScenario('metadata_fetch', iterations, async () => {
		const result = await allShard<Record<string, unknown>>('shard-a', sql);
		if (!result.success) {
			throw new Error('Metadata query failed');
		}
	});
}

async function scenarioPragmaOrInfo(iterations: number, dbFlavor: DatabaseFlavor): Promise<ScenarioStats> {
	const sql = pragmaOrInfoQueryFor(dbFlavor);
	return measureScenario('pragma_or_info', iterations, async () => {
		const result = await allShard<Record<string, unknown>>('shard-a', sql);
		if (!result.success) {
			throw new Error('PRAGMA/info query failed');
		}
	});
}

async function scenarioCounting(iterations: number, bulkSize: number): Promise<ScenarioStats> {
	const ids = new Array(Math.max(40, Math.floor(bulkSize / 2))).fill(null).map((_, i) => `count-${Date.now()}-${i}`);
	await runInBatches(ids, 20, async (id, i) => {
		await run(id, INSERT_USER_SQL, [id, `Count ${i}`, `${id}@count.local`, Date.now()]);
	});

	return measureScenario('counting', iterations, async () => {
		const total = await totalUserCount();
		if (total <= 0) {
			throw new Error('Expected total user count to be > 0');
		}
	});
}

async function scenarioShardFanout(iterations: number, bulkSize: number): Promise<ScenarioStats> {
	const ids = new Array(Math.max(20, Math.floor(bulkSize / 2))).fill(null).map((_, i) => `fanout-${Date.now()}-${i}`);
	await runInBatches(ids, 20, async (id, i) => {
		await run(id, INSERT_USER_SQL, [id, `Fanout ${i}`, `${id}@fanout.local`, Date.now()]);
	});

	return measureScenario('shard_fanout', iterations, async () => {
		const shardResults = await allAllShards<Record<string, unknown>>('SELECT COUNT(*) AS count FROM users');
		const total = shardResults.reduce((sum, shard) => {
			return sum + shard.results.reduce((innerSum, row) => innerSum + extractCount(row), 0);
		}, 0);

		if (total <= 0) {
			throw new Error('Expected fanout query total to be > 0');
		}
	});
}

/**
 * Measures where a routed operation's time actually goes.
 *
 * The rest of the harness reports end-to-end scenario latency, which cannot say
 * whether a change to the KV path helped. This attaches the router's phase
 * observer and runs the four routed shapes, so KV reads, KV writes, hashing,
 * shard selection, and SQL execution are reported separately.
 */
async function measurePhaseAttribution(config: CollegeDBConfig, iterations: number): Promise<PhaseAttribution[]> {
	const collector = new PhaseCollector();

	initialize({ ...config, onPhase: collector.observer, mappingCacheTtlMs: 0 });

	try {
		await resetBenchData();
		collector.reset();

		for (let i = 0; i < iterations; i++) {
			const id = `attr-${Date.now()}-${i}`;

			await run(id, INSERT_USER_SQL, [id, `Attr ${i}`, `${id}@attr.local`, Date.now()]);
			await first<Record<string, unknown>>(id, SELECT_USER_SQL, [id]);
			await run(id, UPDATE_USER_SQL, [`Attr ${i} updated`, id]);
			await first<Record<string, unknown>>(`${id}-absent`, SELECT_USER_SQL, [`${id}-absent`]);
		}

		const stats = collector.stats();
		const total = stats.reduce((sum, entry) => sum + entry.totalMs, 0);

		return stats.map((entry) => ({
			phase: entry.phase,
			count: entry.count,
			totalMs: entry.totalMs,
			avgMs: entry.avgMs,
			p95Ms: entry.p95Ms,
			share: total > 0 ? entry.totalMs / total : 0
		}));
	} finally {
		initialize(config);
	}
}

async function scenarioMappingMiss(iterations: number): Promise<ScenarioStats> {
	return measureScenario('mapping_miss', iterations, async (i) => {
		const key = `absent-${Date.now()}-${i}-${Math.floor(Math.random() * 100_000)}`;
		const row = await first<Record<string, unknown>>(key, SELECT_USER_SQL, [key]);
		if (row) {
			throw new Error(`Key ${key} should not exist`);
		}
	});
}

async function scenarioNextId(iterations: number): Promise<ScenarioStats> {
	await runAllShards('CREATE TABLE IF NOT EXISTS seq_items (id INTEGER PRIMARY KEY, title VARCHAR(191))');

	return measureScenario('next_id', iterations, async () => {
		const id = await nextId('seq_items');
		if (!Number.isFinite(id) || id <= 0) {
			throw new Error(`nextId returned ${String(id)}`);
		}
	});
}

async function scenarioPaginate(iterations: number, seedCount: number): Promise<ScenarioStats> {
	const prefix = `page-${Date.now()}`;
	for (let i = 0; i < seedCount; i++) {
		const id = `${prefix}-${String(i).padStart(4, '0')}`;
		await run(id, INSERT_USER_SQL, [id, `Page ${i}`, `${id}@page.local`, Date.now()]);
	}

	return measureScenario('paginate', iterations, async (i) => {
		const page = await paginate<Record<string, unknown>>('SELECT id, name FROM users', [], {
			page: (i % 3) + 1,
			limit: 10,
			sortBy: 'id'
		});
		if (page.results.length === 0) {
			throw new Error('Paginated read returned no rows');
		}
	});
}

async function scenarioBatchWrite(iterations: number, bulkSize: number): Promise<ScenarioStats> {
	return measureScenario('batch_write', iterations, async (i) => {
		const entries = new Array(bulkSize).fill(null).map((_, index) => {
			const id = `batch-${Date.now()}-${i}-${index}`;
			return {
				key: id,
				sql: INSERT_USER_SQL,
				bindings: [id, `Batch ${index}`, `${id}@batch.local`, Date.now()]
			};
		});

		const groups = await batch(entries);
		const failed = groups.filter((group) => group.error);
		if (failed.length > 0) {
			throw new Error(`Batch failed on ${failed.map((group) => group.shard).join(', ')}: ${failed[0]?.error}`);
		}
	});
}

async function scenarioColdMappingCache(iterations: number, config: CollegeDBConfig): Promise<ScenarioStats> {
	const seeded: string[] = [];
	for (let i = 0; i < iterations; i++) {
		const id = `cold-${Date.now()}-${i}`;
		await run(id, INSERT_USER_SQL, [id, `Cold ${i}`, `${id}@cold.local`, Date.now()]);
		seeded.push(id);
	}

	// Re-initialising with no mapping cache is what makes every read pay the KV
	// lookup, which is the difference a warm isolate hides.
	initialize({ ...config, mappingCacheTtlMs: 0 });

	const stats = await measureScenario('cold_mapping_cache', iterations, async (i) => {
		// Wrapped, because the warmup pass runs with an index past the measured
		// range and this is the one scenario that indexes a fixed set of keys.
		const key = seeded[i % seeded.length]!;
		const row = await first<Record<string, unknown>>(key, SELECT_USER_SQL, [key]);
		if (!row) {
			throw new Error(`Missing seeded row ${key}`);
		}
	});

	initialize(config);
	return stats;
}

async function scenarioConcurrentLoad(iterations: number, concurrency: number = 16): Promise<ScenarioStats> {
	const seeded: string[] = [];
	for (let i = 0; i < concurrency; i++) {
		const id = `conc-${Date.now()}-${i}`;
		await run(id, INSERT_USER_SQL, [id, `Conc ${i}`, `${id}@conc.local`, Date.now()]);
		seeded.push(id);
	}

	return measureScenario('concurrent_load', iterations, async () => {
		const rows = await Promise.all(seeded.map((key) => first<Record<string, unknown>>(key, SELECT_USER_SQL, [key])));
		if (rows.some((row) => !row)) {
			throw new Error('Concurrent routed read missed a seeded row');
		}
	});
}

async function scenarioReassignment(iterations: number, config: CollegeDBConfig): Promise<ScenarioStats> {
	const mapper = new KVShardMapper(config.kv, {
		hashShardMappings: config.hashShardMappings,
		mappingCacheTtlMs: config.mappingCacheTtlMs,
		knownShardsCacheTtlMs: config.knownShardsCacheTtlMs
	});

	return measureScenario('reassignment', iterations, async (i) => {
		const id = `reassign-${Date.now()}-${i}-${Math.floor(Math.random() * 100_000)}`;
		await run(id, INSERT_USER_SQL, [id, `Reassign ${i}`, `${id}@reassign.local`, Date.now()]);

		const mapping = await mapper.getShardMapping(id);
		if (!mapping) {
			throw new Error('Expected mapping before reassignment benchmark');
		}

		const sourceShard = mapping.shard;
		const targetShard = sourceShard === 'shard-a' ? 'shard-b' : 'shard-a';
		const sourceRows = await allShard<Record<string, unknown>>(sourceShard, SELECT_USER_SQL, [id]);
		const sourceRecord = sourceRows.results[0];
		if (!sourceRecord) {
			throw new Error('Expected source record before reassignment migration');
		}

		await runShard(targetShard, INSERT_USER_SQL, [
			id,
			String(sourceRecord.name ?? `Reassign ${i}`),
			String(sourceRecord.email ?? `${id}@reassign.local`),
			Number(sourceRecord.created_at ?? Date.now())
		]);
		await runShard(sourceShard, DELETE_USER_SQL, [id]);
		await mapper.updateShardMapping(id, targetShard);

		const targetRows = await allShard<Record<string, unknown>>(targetShard, SELECT_USER_SQL, [id]);
		if (targetRows.results.length === 0) {
			throw new Error('Expected reassigned record to exist on target shard');
		}

		// The mapping was changed through a separate mapper, so this process has
		// to be told to drop what it cached. Re-running initialize is no longer
		// the way: it reuses the mapper when the config is unchanged.
		await invalidateMappingCache(id);

		const moved = await first(id, SELECT_USER_SQL, [id]);
		if (!moved) {
			throw new Error('Expected reassigned record to be routable after reassignment');
		}
	});
}

// #region journal scenarios

const RACE_TABLE = 'race_items';
const RACE_SCHEMA = 'CREATE TABLE IF NOT EXISTS race_items (id VARCHAR(191) PRIMARY KEY, name VARCHAR(255), qty BIGINT)';
const LOCK_STATEMENT = /pg_advisory_xact_lock|collegedb_lock_stripes|SET TRANSACTION ISOLATION/;
const RACE_WINDOWS = ['none', 'cutover', 'writer'] as const;
const RACE_SEED = Number(process.env.COLLEGEDB_RACE_SEED ?? 20261024);

type RaceKind = 'update' | 'delete' | 'insert' | 'upsert';
type RaceWindow = (typeof RACE_WINDOWS)[number];

interface RaceMode {
	/** Which transaction is held open for a few milliseconds before it commits */
	held: RaceWindow;
	/** Strips the key lock from every batch, which is the protocol broken on purpose */
	lockless: boolean;
}

interface JournalHarness {
	close: () => Promise<void>;
}

interface RaceTally {
	iterations: number;
	moved: number;
	aborted: number;
	casMisses: number;
	acked: number;
	fenced: number;
	duplicates: number;
	zeroRows: number;
	faults: number;
	violations: string[];
	orders: Record<string, number>;
}

function check(condition: unknown, message: string): asserts condition {
	if (!condition) {
		throw new Error(message);
	}
}

function mulberry32(seed: number): () => number {
	let state = seed >>> 0;
	return () => {
		state = (state + 0x6d2b79f5) >>> 0;
		let t = Math.imul(state ^ (state >>> 15), state | 1);
		t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
}

async function pause(ms: number): Promise<void> {
	const end = performance.now() + ms;
	while (performance.now() < end) {
		await new Promise((done) => setImmediate(done));
	}
}

/** Delegates to the real shard and rewrites the batches that matter to a race */
function wrapRaceShard(inner: SQLDatabase, mode: RaceMode, dbFlavor: DatabaseFlavor): SQLDatabase {
	const wrapped = Object.create(inner) as SQLDatabase;
	if (!inner.runBatch) {
		return wrapped;
	}

	const sleepSql = dbFlavor === 'postgres' ? 'SELECT pg_sleep(0.004)' : 'SELECT SLEEP(0.004)';
	wrapped.runBatch = (async (statements: BatchStatement[]) => {
		const kept = mode.lockless ? statements.filter((statement) => !LOCK_STATEMENT.test(statement.sql)) : statements;
		const isCutover = kept.some((statement) => /^INSERT INTO\s+["`]?collegedb_retired/i.test(statement.sql));
		// a fence raise is neither a writer nor a cutover, and holding it would only delay the commit the race looks for
		const isRaise = kept.some((statement) => /^UPDATE\s+["`]?collegedb_topology/i.test(statement.sql));
		const held = (mode.held === 'cutover' && isCutover) || (mode.held === 'writer' && !isCutover && !isRaise);
		const results = await inner.runBatch!(held ? [...kept, { sql: sleepSql }] : kept);
		const own = results.slice(0, kept.length);
		return [...Array.from({ length: statements.length - kept.length }, () => own[0]!), ...own];
	}) as SQLDatabase['runBatch'];

	return wrapped;
}

async function openJournalHarness(
	dbFlavor: DatabaseFlavor,
	profile: AdapterProfile,
	kv: KVStorage,
	mode: RaceMode
): Promise<JournalHarness> {
	const runtime = await createSQLRuntime(dbFlavor, createRunId(`journal_${dbFlavor}_${profile}`), profile, [
		'shard-a',
		'shard-b',
		'shard-j'
	]);
	const shards = Object.fromEntries(Object.entries(runtime.shards).map(([name, db]) => [name, wrapRaceShard(db, mode, dbFlavor)]));

	await initializeAsync({
		kv,
		shards,
		strategy: 'hash',
		journalShard: 'shard-j',
		disableAutoMigration: true,
		hashShardMappings: false,
		mappingCacheTtlMs: 60_000
	});
	await createSchemaAcrossShards(shards, RACE_SCHEMA);

	return {
		close: async () => {
			resetConfig();
			await runtime.close();
		}
	};
}

function raceMapper(kv: KVStorage): KVShardMapper {
	return new KVShardMapper(kv, { hashShardMappings: false });
}

async function seedRaceRow(mapper: KVShardMapper, key: string): Promise<void> {
	await runShard('shard-a', `INSERT INTO ${RACE_TABLE} (id, name, qty) VALUES (?, ?, ?)`, [key, 'orig', 1]);
	await mapper.setShardMapping(key, 'shard-a');
	await invalidateMappingCache(key);
}

async function raceRows(binding: string, key: string): Promise<Array<{ id: string; name: string; qty: unknown }>> {
	const result = await allShard<{ id: string; name: string; qty: unknown }>(
		binding,
		`SELECT id, name, qty FROM ${RACE_TABLE} WHERE id = ?`,
		[key]
	);
	return result.results;
}

async function tombstoneCount(binding: string, key: string): Promise<number> {
	const result = await allShard(binding, 'SELECT op_id FROM collegedb_retired WHERE primary_key = ?', [key]);
	return result.results.length;
}

async function errorCode(work: Promise<unknown>): Promise<string> {
	try {
		await work;
		return 'none';
	} catch (error) {
		return error instanceof CollegeDBError ? String(error.code) : `other:${error instanceof Error ? error.message : String(error)}`;
	}
}

async function scenarioJournalMove(
	dbFlavor: DatabaseFlavor,
	profile: AdapterProfile,
	kv: KVStorage,
	iterations: number
): Promise<ScenarioStats> {
	let harness: JournalHarness | null = null;

	try {
		harness = await openJournalHarness(dbFlavor, profile, kv, { held: 'none', lockless: false });
		const mapper = raceMapper(kv);
		const prefix = `jm_${Date.now()}`;
		const mysqlFamily = dbFlavor === 'mysql' || dbFlavor === 'mariadb';

		return await measureScenario('journal_move', iterations, async (i) => {
			const key = `${prefix}_${i}`;
			await seedRaceRow(mapper, key);
			await reassignShard(key, 'shard-b', RACE_TABLE);

			const onDestination = await raceRows('shard-b', key);
			check(onDestination.length === 1 && onDestination[0]!.name === 'orig', 'moved row is not on the destination');
			check((await raceRows('shard-a', key)).length === 0, 'source still holds the row');
			check((await tombstoneCount('shard-a', key)) === 1, 'source has no tombstone');

			const updated = await updateRow(key, RACE_TABLE, { name: 'after' }, { id: key });
			check(updated.meta.changes === 1, `update after the move changed ${updated.meta.changes} rows`);

			// MySQL and MariaDB count an upsert that changes an existing row as 2
			const upserted = await upsert(key, RACE_TABLE, { id: key, name: 'upserted', qty: 2 }, 'id');
			check(upserted.meta.changes === (mysqlFamily ? 2 : 1), `upsert over the moved row changed ${upserted.meta.changes} rows`);
			const upsertedNew = `${key}_upsert`;
			const upsertInserted = await upsert(upsertedNew, RACE_TABLE, { id: upsertedNew, name: 'fresh', qty: 4 }, 'id');
			check(upsertInserted.meta.changes === 1, `upsert of a new key changed ${upsertInserted.meta.changes} rows`);

			const fresh = `${key}_new`;
			const inserted = await insertInto(fresh, RACE_TABLE, { id: fresh, name: 'new', qty: 9 });
			check(inserted.meta.changes === 1, `guarded insert changed ${inserted.meta.changes} rows`);
			const freshRow = await first<{ qty: unknown }>(fresh, `SELECT qty FROM ${RACE_TABLE} WHERE id = ?`, [fresh]);
			check(Number(freshRow?.qty) === 9, 'guarded insert did not keep the 64-bit column value');

			const readBack = await first<{ name: string }>(key, `SELECT name FROM ${RACE_TABLE} WHERE id = ?`, [key]);
			check(readBack?.name === 'upserted', `read back ${readBack?.name}`);

			// a caller still routing to the old shard must be refused
			await mapper.setShardMapping(key, 'shard-a');
			await invalidateMappingCache(key);
			const staleUpdate = await errorCode(updateRow(key, RACE_TABLE, { name: 'stale' }, { id: key }));
			const staleInsert = await errorCode(insertInto(key, RACE_TABLE, { id: key, name: 'ghost', qty: 1 }));
			check(staleUpdate === 'MIGRATION_CUTOVER_PENDING', `stale update: ${staleUpdate}`);
			check(staleInsert === 'MIGRATION_CUTOVER_PENDING', `stale insert: ${staleInsert}`);
			check((await raceRows('shard-a', key)).length === 0, 'a stale write left a ghost on the source');

			await mapper.setShardMapping(key, 'shard-b');
			await invalidateMappingCache(key);
		});
	} catch (error) {
		return {
			name: 'journal_move',
			status: 'failed',
			iterations: 0,
			samplesMs: [],
			error: error instanceof Error ? error.message : String(error)
		};
	} finally {
		if (harness) {
			await safely(async () => harness?.close());
		}
	}
}

async function runRaceWriter(kind: RaceKind, key: string): Promise<{ acked: boolean; changes?: number; code?: string; message?: string }> {
	try {
		const result =
			kind === 'update'
				? await updateRow(key, RACE_TABLE, { name: 'W' }, { id: key })
				: kind === 'delete'
					? await deleteRow(key, RACE_TABLE, { id: key })
					: kind === 'insert'
						? await insertInto(key, RACE_TABLE, { id: key, name: 'I', qty: 5 })
						: await upsert(key, RACE_TABLE, { id: key, name: 'U', qty: 3 }, 'id');
		const changes = result.meta.changes;
		return { acked: typeof changes === 'number' ? changes > 0 : true, changes };
	} catch (error) {
		return {
			acked: false,
			code: error instanceof CollegeDBError ? error.code : undefined,
			// drizzle keeps the driver message in the cause
			message: error instanceof Error ? `${error.message} ${(error.cause as Error | undefined)?.message ?? ''}` : String(error)
		};
	}
}

async function raceOnce(kind: RaceKind, key: string, mapper: KVShardMapper, random: () => number, tally: RaceTally): Promise<void> {
	await seedRaceRow(mapper, key);

	const pick = random();
	const launchStep = pick < 0.35 ? 'verified' : pick < 0.9 ? 'cutover_pending' : 'cas';
	const delayMs = random() ** 2 * 7;
	tally.orders[`${launchStep}+${delayMs < 2 ? 'early' : delayMs < 5 ? 'mid' : 'late'}`] =
		(tally.orders[`${launchStep}+${delayMs < 2 ? 'early' : delayMs < 5 ? 'mid' : 'late'}`] ?? 0) + 1;

	let writer: Promise<Awaited<ReturnType<typeof runRaceWriter>>> | null = null;
	setMigrationStepHook((step, info) => {
		if (step === launchStep && info.key === key && !writer) {
			writer = (async () => {
				await pause(delayMs);
				return await runRaceWriter(kind, key);
			})();
		}
	});

	let moveError: unknown = null;
	try {
		await reassignShard(key, 'shard-b', RACE_TABLE);
	} catch (error) {
		moveError = error;
	} finally {
		setMigrationStepHook(null);
	}

	const outcome = await (writer ?? runRaceWriter(kind, key));

	const [rowsA, rowsB, tombA] = await Promise.all([raceRows('shard-a', key), raceRows('shard-b', key), tombstoneCount('shard-a', key)]);
	const attempts = await allShard<{ attempts: unknown }>(
		'shard-j',
		'SELECT MAX(attempt) AS attempts FROM collegedb_migration_history WHERE primary_key = ?',
		[key]
	);
	if (Number(attempts.results[0]?.attempts ?? 1) > 1) {
		tally.casMisses++;
	}

	const fail = (message: string) => tally.violations.push(`${kind}/${launchStep}/${delayMs.toFixed(1)}ms/${key}: ${message}`);
	const moved = moveError === null;
	if (moved) {
		tally.moved++;
	} else if (moveError instanceof CollegeDBError && moveError.code === 'MIGRATION_ABORTED') {
		tally.aborted++;
	} else {
		fail(`move failed: ${moveError instanceof Error ? moveError.message : String(moveError)}`);
	}

	if (outcome.acked) {
		tally.acked++;
	} else if (outcome.code === 'MIGRATION_CUTOVER_PENDING') {
		tally.fenced++;
	} else if (outcome.code === undefined && outcome.message === undefined) {
		tally.zeroRows++;
	} else if (kind === 'insert' && /duplicate|unique|1062|23505/i.test(outcome.message ?? '')) {
		tally.duplicates++;
	} else {
		tally.faults++;
		fail(`unexpected writer error: ${outcome.code ?? ''} ${outcome.message}`);
	}

	const owner = rowsB[0] ?? rowsA[0];
	if (tombA > 0 && rowsA.length > 0) {
		fail('ghost: a live row sits beside the tombstone');
	}
	if (rowsA.length > 0 && rowsB.length > 0) {
		fail('two live copies');
	}
	if (moved && (tombA !== 1 || rowsA.length > 0)) {
		fail(`moved but source holds ${rowsA.length} rows and ${tombA} tombstones`);
	}
	if (!moved && tombA > 0) {
		fail('move did not complete but a tombstone exists');
	}
	if (!owner && !(kind === 'delete' && outcome.acked)) {
		fail('row lost without an acknowledged delete');
	}
	if (outcome.changes === 0 && tombA === 0 && owner) {
		fail('writer changed zero rows with no tombstone while the key exists');
	}
	if (kind === 'update' && outcome.acked && owner?.name !== 'W') {
		fail(`acknowledged update is not on the owner (${owner?.name})`);
	}
	if (kind === 'upsert' && outcome.acked && owner?.name !== 'U') {
		fail(`acknowledged upsert is not on the owner (${owner?.name})`);
	}
	if (kind === 'delete' && outcome.acked && owner) {
		fail('acknowledged delete left a row');
	}
	if (kind === 'insert' && outcome.acked) {
		fail('insert over an existing key was acknowledged');
	}
}

async function runRaceArm(
	dbFlavor: DatabaseFlavor,
	profile: AdapterProfile,
	kv: KVStorage,
	arm: 'protocol' | 'control',
	kinds: RaceKind[],
	perKind: number,
	isolation: string | undefined
): Promise<Record<RaceKind, RaceTally>> {
	const mode: RaceMode = { held: 'none', lockless: arm === 'control' };
	const previousIsolation = sandboxIsolation;
	sandboxIsolation = isolation ?? previousIsolation;
	let harness: JournalHarness;
	try {
		harness = await openJournalHarness(dbFlavor, profile, kv, mode);
	} finally {
		sandboxIsolation = previousIsolation;
	}

	try {
		const mapper = raceMapper(kv);
		const random = mulberry32(RACE_SEED + (arm === 'control' ? 1 : 0));
		const prefix = `race_${arm}_${Date.now()}`;
		const tallies = {} as Record<RaceKind, RaceTally>;

		for (const kind of kinds) {
			const tally: RaceTally = {
				iterations: perKind,
				moved: 0,
				aborted: 0,
				casMisses: 0,
				acked: 0,
				fenced: 0,
				duplicates: 0,
				zeroRows: 0,
				faults: 0,
				violations: [],
				orders: {}
			};
			tallies[kind] = tally;

			for (let i = 0; i < perKind; i++) {
				mode.held = RACE_WINDOWS[i % RACE_WINDOWS.length]!;
				await raceOnce(kind, `${prefix}_${kind}_${i}`, mapper, random, tally);
			}
		}

		return tallies;
	} finally {
		await safely(async () => harness.close());
	}
}

function describeTally(tally: RaceTally): string {
	return `moved=${tally.moved} aborted=${tally.aborted} casMisses=${tally.casMisses} acked=${tally.acked} fenced=${tally.fenced} dup=${tally.duplicates} zero=${tally.zeroRows} faults=${tally.faults} violations=${tally.violations.length}`;
}

interface RaceArm {
	label: string;
	arm: 'protocol' | 'control';
	/** Session isolation for the arm; undefined keeps the server default */
	isolation?: string;
	/** A control that must report violations for the run to prove anything */
	required?: boolean;
}

/**
 * MySQL and MariaDB default to REPEATABLE READ, where the `INSERT ... SELECT`
 * guard reads current rows and the race does not show without the lock. Under
 * READ COMMITTED it does, so that is the isolation the control must break at.
 */
function raceArmsFor(dbFlavor: DatabaseFlavor): RaceArm[] {
	if (dbFlavor === 'postgres') {
		return [
			{ label: 'protocol', arm: 'protocol' },
			{ label: 'control', arm: 'control', required: true }
		];
	}

	return [
		{ label: 'protocol@default', arm: 'protocol' },
		{ label: 'protocol@read-committed', arm: 'protocol', isolation: 'READ COMMITTED' },
		{ label: 'control@read-committed', arm: 'control', isolation: 'READ COMMITTED', required: true },
		{ label: 'control@default', arm: 'control' }
	];
}

async function scenarioStaleRouteWrite(
	dbFlavor: DatabaseFlavor,
	profile: AdapterProfile,
	kv: KVStorage,
	perKind: number
): Promise<ScenarioStats> {
	if (dbFlavor === 'sqlite') {
		return createSkippedScenario(
			'stale_route_write',
			'one connection per file means async writers interleave inside the cutover transaction; the worker-thread race in tests/migration-journal.spec.ts covers SQLite'
		);
	}

	if (profile === 'per-statement-connection') {
		return createSkippedScenario(
			'stale_route_write',
			'a Hyperdrive client is one connection and its gate runs a batch alone, so there is no second connection to race'
		);
	}

	const kinds: RaceKind[] = ['update', 'delete', 'insert', 'upsert'];
	const controlPerKind = Math.max(30, Math.ceil(perKind / 2));
	const started = performance.now();

	try {
		const problems: string[] = [];
		const lines: string[] = [];

		for (const spec of raceArmsFor(dbFlavor)) {
			const n = spec.arm === 'protocol' ? perKind : controlPerKind;
			const tallies = await runRaceArm(dbFlavor, profile, kv, spec.arm, kinds, n, spec.isolation);

			for (const kind of kinds) {
				const tally = tallies[kind];
				lines.push(`${spec.label} ${kind} n=${n}: ${describeTally(tally)}`);
				console.log(`[Sandbox] stale_route_write ${dbFlavor}/${profile} ${spec.label} ${kind} n=${n}: ${describeTally(tally)}`);
				if (spec.arm === 'protocol') {
					console.log(`[Sandbox]   orders ${JSON.stringify(tally.orders)}`);
					if (tally.violations.length > 0) {
						problems.push(`${spec.label} ${kind}: ${tally.violations.length} violations, first: ${tally.violations[0]}`);
					}
				} else if (tally.violations.length > 0) {
					console.log(`[Sandbox]   first violation: ${tally.violations[0]}`);
				} else if (spec.required) {
					problems.push(`${spec.label} ${kind}: the control arm reported no violation, so the checks are not shown able to see one`);
				}
			}
		}

		const elapsed = performance.now() - started;
		return {
			name: 'stale_route_write',
			status: problems.length === 0 ? 'passed' : 'failed',
			iterations: perKind * kinds.length,
			samplesMs: [elapsed],
			...calculateLatencyStats([elapsed]),
			notes: lines.join(' | '),
			error: problems.length === 0 ? undefined : problems.join('; ')
		};
	} catch (error) {
		return {
			name: 'stale_route_write',
			status: 'failed',
			iterations: 0,
			samplesMs: [],
			error: error instanceof Error ? error.message : String(error)
		};
	}
}

// #endregion

// #region topology scenario

const TOPO_TABLE = 'topo_items';
const TOPO_SCHEMA = `CREATE TABLE IF NOT EXISTS ${TOPO_TABLE} (id VARCHAR(191) PRIMARY KEY, name VARCHAR(255))`;
const PLACEMENT_KV_KEYS = ['collegedb:placement:manifest', 'collegedb:placement:exceptions'];
const FENCE_ORDERS = ['writer-first', 'fence-first', 'simultaneous', 'simultaneous'] as const;

interface TopologyTally {
	iterations: number;
	landed: number;
	rejected: number;
	faults: number;
	violations: string[];
	orders: Record<string, number>;
}

const newTally = (iterations: number): TopologyTally => ({ iterations, landed: 0, rejected: 0, faults: 0, violations: [], orders: {} });

const tallyLine = (tally: TopologyTally) =>
	`landed=${tally.landed} rejected=${tally.rejected} faults=${tally.faults} violations=${tally.violations.length}`;

async function runStatements(db: SQLDatabase, statements: string[]): Promise<void> {
	for (const statement of statements) {
		await db.prepare(statement).run();
	}
}

/** Creates every topology table twice on one shard; the second pass proves the DDL is idempotent */
async function createTopologyTables(db: SQLDatabase, journal: boolean): Promise<void> {
	for (let pass = 0; pass < 2; pass++) {
		await runStatements(db, buildShardSidecarSchema(db.dialect));
		if (journal) {
			await runStatements(db, buildJournalSchema(db.dialect));
			await runStatements(db, buildManifestSchema(db.dialect));
		}
		await db.prepare(TOPO_SCHEMA).run();
	}
}

/** Publishes the next epoch; every call adds one, so the epoch count counts the writes that survived */
const nextEpoch = (current: ReturnType<typeof createManifest> | null) => {
	const base = current ?? createManifest(['shard-a', 'shard-b'], 'hrw');
	return { ...base, epochs: [{ n: base.epochs[0]!.n + 1, algorithm: 'hrw' as const, shards: ['shard-a', 'shard-b'] }, ...base.epochs] };
};

async function casRace(db: SQLDatabase, arm: 'protocol' | 'control', n: number, random: () => number): Promise<TopologyTally> {
	const tally = newTally(n);
	const prefix = `cas_${arm}_${Date.now()}`;

	for (let i = 0; i < n; i++) {
		const journal = { db, scope: `${prefix}_${i}` };
		const racers = i % 2 === 0 ? 2 : 4;
		const fail = (message: string) => tally.violations.push(`${arm}/${i}/${racers} racers: ${message}`);

		try {
			let revisions: number[] = [];

			if (arm === 'protocol') {
				const results = await Promise.all(
					Array.from({ length: racers }, async () => {
						await pause(random() * 2);
						return await commitJournalManifest(journal, nextEpoch);
					})
				);
				revisions = results.map((result) => result.revision).sort((a, b) => a - b);
				if (results.some((result) => !result.changed)) {
					fail('a publisher returned without writing');
				}
			} else {
				// the same read-modify-write with the revision check removed
				await commitJournalManifest(journal, (current) => current ?? createManifest(['shard-a', 'shard-b'], 'hrw'));
				await Promise.all(
					Array.from({ length: racers }, async () => {
						const loaded = (await loadJournalManifest(journal))!;
						await pause(random() * 3);
						const next = nextEpoch(loaded.manifest);
						await db
							.prepare('UPDATE collegedb_manifest SET revision = revision + 1, generation = ?, body = ? WHERE scope = ?')
							.bind(next.epochs[0]!.n, JSON.stringify(next), journal.scope)
							.run();
					})
				);
			}

			const stored = (await loadJournalManifest(journal))!;
			const numbers = stored.manifest.epochs.map((epoch) => epoch.n);
			const expected = Array.from({ length: racers + 1 }, (_, k) => racers + 1 - k);

			if (JSON.stringify(numbers) !== JSON.stringify(expected)) {
				fail(`stored epochs ${numbers.join(',')}, expected ${expected.join(',')}`);
			} else if (stored.revision !== (arm === 'protocol' ? racers : racers + 1) || stored.generation !== racers + 1) {
				fail(`stored revision ${stored.revision} and generation ${stored.generation} after ${racers} publishes`);
			} else if (arm === 'protocol' && JSON.stringify(revisions) !== JSON.stringify(Array.from({ length: racers }, (_, k) => k + 1))) {
				fail(`publishers saw revisions ${revisions.join(',')}, not one winner per revision`);
			} else {
				tally.landed++;
			}
		} catch (error) {
			tally.faults++;
			fail(`error ${error instanceof CollegeDBError ? error.code : ''} ${error instanceof Error ? error.message : String(error)}`);
		}
	}

	return tally;
}

/**
 * A writer on an older generation against the fence raise and the census that
 * follows it. A row that exists although the census read none for its key is a
 * violation: `pruneEpochs` would count that key as absent.
 */
async function fenceRace(
	shard: SQLDatabase,
	arm: 'protocol' | 'control',
	n: number,
	random: () => number,
	mode: RaceMode,
	startAt: number
): Promise<TopologyTally> {
	const tally = newTally(n);
	const prefix = `fence_${arm}_${Date.now()}`;

	for (let i = 0; i < n; i++) {
		const key = `${prefix}_${i}`;
		const generation = startAt + i;
		const order = FENCE_ORDERS[Math.floor(random() * FENCE_ORDERS.length)]!;
		tally.orders[order] = (tally.orders[order] ?? 0) + 1;
		mode.held = i % 3 === 2 ? 'writer' : 'none';

		const guard = { scope: 'default', table: TOPO_TABLE, key, generation };
		const values = { id: key, name: 'W' };
		const query =
			arm === 'control'
				? buildInsert(TOPO_TABLE, values, { dialect: shard.dialect })
				: buildGuardedInsert(TOPO_TABLE, values, guard, { dialect: shard.dialect });

		const write = async () => {
			try {
				const result =
					arm === 'control'
						? await shard
								.prepare(query.sql)
								.bind(...query.bindings)
								.run()
						: await runGuardedWrite(shard, guard, query);
				return { changes: result.meta.changes as number | undefined };
			} catch (error) {
				return { error: error instanceof Error ? error.message : String(error) };
			}
		};

		let seen = -1;
		const fence = async () => {
			await raiseFence(shard, generation + 1);
			const row = await shard.prepare(`SELECT COUNT(*) AS n FROM ${TOPO_TABLE} WHERE id = ?`).bind(key).first<{ n: unknown }>();
			seen = Number(row?.n);
		};

		let outcome: { changes?: number; error?: string };
		if (order === 'writer-first') {
			outcome = await write();
			await fence();
		} else if (order === 'fence-first') {
			await fence();
			outcome = await write();
		} else {
			await pause(0);
			const delay = random() * 4;
			[outcome] = await Promise.all([write(), pause(delay).then(fence)]);
		}

		if (outcome.error !== undefined) {
			tally.faults++;
			tally.violations.push(`${arm}/${order}/${key}: writer error ${outcome.error}`);
			continue;
		}

		const exists =
			Number((await shard.prepare(`SELECT COUNT(*) AS n FROM ${TOPO_TABLE} WHERE id = ?`).bind(key).first<{ n: unknown }>())?.n) > 0;
		const acked = (outcome.changes ?? 0) > 0;
		if (acked) {
			tally.landed++;
		} else {
			tally.rejected++;
		}
		if (exists && seen === 0) {
			tally.violations.push(`${arm}/${order}/${key}: row landed after the census read none`);
		}
		if (acked !== exists) {
			tally.violations.push(
				`${arm}/${order}/${key}: writer reported ${outcome.changes} changes but the row ${exists ? 'exists' : 'is missing'}`
			);
		}
	}

	return tally;
}

interface FenceArm {
	label: string;
	/** `lockless` runs the protocol statements with every lock statement stripped, which is the lock broken on purpose */
	arm: 'protocol' | 'lockless' | 'control';
	isolation?: string;
	/** A control that must report violations for the run to prove anything */
	required?: boolean;
}

/** As in the stale-route race, MySQL and MariaDB only break without the lock under READ COMMITTED */
function fenceArmsFor(dbFlavor: DatabaseFlavor): FenceArm[] {
	if (dbFlavor === 'sqlite') {
		return [
			{ label: 'protocol', arm: 'protocol' },
			{ label: 'control (no fence)', arm: 'control', required: true }
		];
	}

	if (dbFlavor === 'postgres') {
		return [
			{ label: 'protocol', arm: 'protocol' },
			{ label: 'control (no fence lock)', arm: 'lockless', required: true },
			{ label: 'control (no fence)', arm: 'control', required: true }
		];
	}

	return [
		{ label: 'protocol@default', arm: 'protocol' },
		{ label: 'protocol@read-committed', arm: 'protocol', isolation: 'READ COMMITTED' },
		{ label: 'control (no fence lock)@read-committed', arm: 'lockless', isolation: 'READ COMMITTED', required: true },
		{ label: 'control (no fence)@default', arm: 'control', required: true }
	];
}

/** Runs one fence arm on a fresh one-shard database, so a raised fence never leaks into another arm */
async function fenceArm(
	dbFlavor: DatabaseFlavor,
	profile: AdapterProfile,
	spec: FenceArm,
	n: number,
	random: () => number
): Promise<TopologyTally> {
	const previousIsolation = sandboxIsolation;
	sandboxIsolation = spec.isolation ?? previousIsolation;
	let runtime: SQLRuntime;
	try {
		runtime = await createSQLRuntime(dbFlavor, createRunId(`fence_${dbFlavor}_${profile}`), profile, ['shard-f']);
	} finally {
		sandboxIsolation = previousIsolation;
	}

	try {
		const mode: RaceMode = { held: 'none', lockless: spec.arm === 'lockless' };
		const inner = runtime.shards['shard-f']!;
		await createTopologyTables(inner, false);
		const shard = dbFlavor === 'sqlite' ? inner : wrapRaceShard(inner, mode, dbFlavor);
		return await fenceRace(shard, spec.arm === 'control' ? 'control' : 'protocol', n, random, mode, 1);
	} finally {
		await safely(async () => runtime.close());
	}
}

async function scenarioTopologyCas(
	dbFlavor: DatabaseFlavor,
	profile: AdapterProfile,
	kv: KVStorage,
	perKind: number
): Promise<ScenarioStats> {
	const started = performance.now();
	// shard-f takes the fence races, whose raised fence would turn away the prune check's writers
	const names = ['shard-a', 'shard-b', 'shard-j'];
	const lines: string[] = [];
	const problems: string[] = [];
	let runtime: SQLRuntime | null = null;

	const record = (label: string, tally: TopologyTally, required: 'zero' | 'some' | 'none') => {
		const line = `${label} n=${tally.iterations}: ${tallyLine(tally)}`;
		lines.push(line);
		console.log(`[Sandbox] topology_cas ${dbFlavor}/${profile} ${line}`);
		if (tally.violations.length > 0) {
			console.log(`[Sandbox]   first violation: ${tally.violations[0]}`);
		}
		if (required === 'zero' && tally.violations.length > 0) {
			problems.push(`${label}: ${tally.violations.length} violations, first: ${tally.violations[0]}`);
		} else if (required === 'some' && tally.violations.length === 0) {
			problems.push(`${label}: the control arm reported no violation, so the check is not shown able to see one`);
		}
	};

	try {
		runtime = await createSQLRuntime(dbFlavor, createRunId(`topology_${dbFlavor}_${profile}`), profile, names);
		const raw = runtime.shards;
		const journalDb = raw['shard-j']!;
		for (const [name, db] of Object.entries(raw)) {
			await createTopologyTables(db, name === 'shard-j');
		}
		lines.push('ddl created twice on every shard');

		const random = mulberry32(RACE_SEED + 7);
		record('cas protocol', await casRace(journalDb, 'protocol', perKind, random), 'zero');
		record('cas control (no revision check)', await casRace(journalDb, 'control', Math.max(30, Math.ceil(perKind / 2)), random), 'some');

		const controlN = Math.max(30, Math.ceil(perKind / 2));
		for (const spec of fenceArmsFor(dbFlavor)) {
			const n = spec.arm === 'protocol' ? perKind : controlN;
			// a single-connection Hyperdrive client has nothing to race, so its controls cannot fire
			const mustFire = spec.required && profile !== 'per-statement-connection';
			record(
				`fence ${spec.label}`,
				await fenceArm(dbFlavor, profile, spec, n, random),
				spec.arm === 'protocol' ? 'zero' : mustFire ? 'some' : 'none'
			);
		}

		lines.push(await pruneEndToEnd(raw, kv));
	} catch (error) {
		problems.push(error instanceof Error ? error.message : String(error));
	} finally {
		resetConfig();
		resetPlacementState();
		await safely(async () => void (await Promise.all(PLACEMENT_KV_KEYS.map((key) => kv.delete(key)))));
		if (runtime) {
			await safely(async () => runtime?.close());
		}
	}

	const elapsed = performance.now() - started;
	return {
		name: 'topology_cas',
		status: problems.length === 0 ? 'passed' : 'failed',
		iterations: perKind * 2,
		samplesMs: [elapsed],
		...calculateLatencyStats([elapsed]),
		notes: lines.join(' | '),
		error: problems.length === 0 ? undefined : problems.join('; ')
	};
}

/** pruneEpochs on a real engine: dry run, a real run that must keep a needed epoch, then a real removal */
async function pruneEndToEnd(shards: Record<string, SQLDatabase>, kv: KVStorage): Promise<string> {
	const two = ['shard-a', 'shard-b'];
	const three = ['shard-a', 'shard-b', 'shard-j'];
	const boot = async (names: string[]) => {
		resetConfig();
		resetPlacementState();
		await initializeAsync({
			kv,
			shards: Object.fromEntries(names.map((name) => [name, shards[name]!])),
			strategy: 'hash',
			placement: 'computed',
			journalShard: 'shard-a',
			disableAutoMigration: true,
			hashShardMappings: false
		});
		await first('warm', `SELECT * FROM ${TOPO_TABLE} WHERE id = ?`, ['warm']);
	};
	const fences = async () => Promise.all(three.map((name) => readFence(shards[name]!)));

	await Promise.all(PLACEMENT_KV_KEYS.map((key) => kv.delete(key)));
	await boot(two);
	const keys = Array.from({ length: 40 }, (_, i) => `prune_${Date.now()}_${i}`);
	for (const key of keys) {
		await insertInto(key, TOPO_TABLE, { id: key, name: key });
	}

	await boot(three);
	const relocated = keys.filter((key) => hrwShard(key, three) !== hrwShard(key, two));
	check(relocated.length > 0 && relocated.length < keys.length, `${relocated.length} of ${keys.length} keys relocated`);

	const dry = await pruneEpochs({ tables: [TOPO_TABLE], dryRun: true });
	check(dry.removed.length === 0 && dry.retained.join() === '2,1', `dry run removed ${dry.removed} and kept ${dry.retained}`);
	check(
		dry.dependents[1] === relocated.length && dry.scanned === keys.length,
		`dry run counted ${JSON.stringify(dry.dependents)} over ${dry.scanned} rows`
	);
	check(
		(await fences()).every((fence) => fence === 0),
		'a dry run raised a fence'
	);

	const kept = await pruneEpochs({ tables: [TOPO_TABLE], pageSize: 7 });
	check(kept.removed.length === 0 && kept.retained.join() === '2,1', `a needed epoch was removed: ${kept.removed}`);
	check(
		(await fences()).every((fence) => fence === 2),
		`fences after the real run: ${await fences()}`
	);

	for (const key of keys) {
		const row = await first<{ name: string }>(key, `SELECT name FROM ${TOPO_TABLE} WHERE id = ?`, [key]);
		check(row?.name === key, `${key} did not resolve before the prune`);
	}
	const done = await pruneEpochs({ tables: [TOPO_TABLE], pageSize: 7 });
	check(done.removed.join() === '1' && done.retained.join() === '2' && done.dependents[1] === 0, `prune report ${JSON.stringify(done)}`);

	const stored = await loadJournalManifest({ db: shards['shard-a']!, scope: 'default' });
	check(stored?.manifest.epochs.map((epoch) => epoch.n).join() === '2', `stored epochs ${stored?.manifest.epochs.map((epoch) => epoch.n)}`);

	invalidateMappingCache();
	for (const key of keys) {
		const row = await first<{ name: string }>(key, `SELECT name FROM ${TOPO_TABLE} WHERE id = ?`, [key]);
		check(row?.name === key, `${key} did not resolve after the prune`);
	}

	const stale = buildGuardedInsert(
		TOPO_TABLE,
		{ id: 'stale', name: 'S' },
		{ scope: 'default', table: TOPO_TABLE, key: 'stale', generation: 1 },
		{ dialect: shards['shard-a']!.dialect }
	);
	const staleResult = await runGuardedWrite(
		shards['shard-a']!,
		{ scope: 'default', table: TOPO_TABLE, key: 'stale', generation: 1 },
		stale
	);
	check(staleResult.meta.changes === 0, `a generation 1 writer changed ${staleResult.meta.changes} rows after the fence`);

	return `prune: ${keys.length} rows, ${relocated.length} relocated, dry run counted ${relocated.length} on epoch 1, epoch 1 removed, every key resolved before and after, stale writer fenced`;
}

// #endregion

// #region live engine scenarios

/** What a statement tap does besides logging; every field is a test seam that breaks one protection on purpose */
interface Tap {
	/** Every statement sent, as `shard sql` */
	log: string[];
	/** What a `SELECT 1 FROM` probe returns: its real answer, a hit for every key, or an empty answer for every failure */
	probe: 'real' | 'always' | 'swallow';
	/** Rewrites a statement before it is sent; `drop` counts the trailing bindings the rewrite removes */
	rewrite?: (sql: string) => { sql: string; drop: number } | null;
	/** Answers the lease generation read with this value instead of asking the journal */
	fakeLease?: number;
}

const LEASE_CHECK = /^SELECT\s+generation\s+FROM\s+collegedb_recovery_lease\s+WHERE\s+scope\s*=\s*\?$/i;
const FENCE_PREDICATE =
	/\(SELECT\s+["`]?generation["`]?\s+FROM\s+["`]?collegedb_recovery_lease["`]?\s+WHERE\s+["`]?scope["`]?\s*=\s*\?\)\s*=\s*\?/;
const CRASH_MESSAGE = 'sandbox crash';

const isWriteEntry = (entry: string): boolean => !/^\S+\s+SELECT\s/i.test(entry);

/** Wraps a shard so every statement is logged and the Tap seams apply; the shard itself is untouched */
function tapShard(inner: SQLDatabase, name: string, tap: Tap): SQLDatabase {
	const wrapped = Object.create(inner) as SQLDatabase;

	const statement = (sql: string, bound: any[] | null): PreparedStatement => {
		const rewritten = tap.rewrite?.(sql) ?? null;
		const text = rewritten?.sql ?? sql;
		const open = (): PreparedStatement => {
			const prepared = inner.prepare(text);
			return bound === null ? prepared : prepared.bind(...(rewritten ? bound.slice(0, bound.length - rewritten.drop) : bound));
		};
		const isProbe = /^SELECT 1 FROM/i.test(sql);

		return {
			bind: (...bindings: any[]) => statement(sql, bindings),
			run: (() => {
				tap.log.push(`${name} ${text}`);
				return open().run();
			}) as PreparedStatement['run'],
			all: (() => {
				tap.log.push(`${name} ${text}`);
				return open().all();
			}) as PreparedStatement['all'],
			first: (async () => {
				tap.log.push(`${name} ${text}`);
				if (isProbe && tap.probe === 'always') {
					return { one: 1 };
				}
				if (tap.fakeLease !== undefined && LEASE_CHECK.test(sql)) {
					return { generation: tap.fakeLease };
				}
				try {
					return await open().first();
				} catch (error) {
					if (isProbe && tap.probe === 'swallow') {
						return null;
					}
					throw error;
				}
			}) as PreparedStatement['first']
		};
	};

	wrapped.prepare = (sql: string) => statement(sql, null);
	if (inner.runBatch) {
		wrapped.runBatch = (async (statements: BatchStatement[]) => {
			for (const entry of statements) {
				tap.log.push(`${name} ${entry.sql}`);
			}
			return await inner.runBatch!(statements);
		}) as SQLDatabase['runBatch'];
	}

	return wrapped;
}

function tapAll(shards: Record<string, SQLDatabase>, tap: Tap): Record<string, SQLDatabase> {
	return Object.fromEntries(Object.entries(shards).map(([name, db]) => [name, tapShard(db, name, tap)]));
}

type Settled<T> = { ok: true; value: T } | { ok: false; error: unknown };

async function settle<T>(work: Promise<T>): Promise<Settled<T>> {
	try {
		return { ok: true, value: await work };
	} catch (error) {
		return { ok: false, error };
	}
}

/** The driver's own code, number, SQLSTATE and message for an error and its causes, which is what a classifier has to match */
function describeDriverError(error: unknown): string {
	const parts: string[] = [];
	let current: unknown = error;
	for (let depth = 0; depth < 3 && current && typeof current === 'object'; depth++) {
		const e = current as { name?: string; code?: unknown; errno?: unknown; sqlState?: unknown; message?: string; cause?: unknown };
		const fields = [
			e.name,
			e.code !== undefined && `code=${e.code}`,
			e.errno !== undefined && `errno=${e.errno}`,
			e.sqlState && `sqlState=${e.sqlState}`
		];
		parts.push(
			`${depth === 0 ? '' : 'cause '}{${fields.filter(Boolean).join(' ')}: ${String(e.message).replace(/\s+/g, ' ').slice(0, 100)}}`
		);
		current = e.cause;
	}
	return parts.join(' ') || String(error);
}

function describeOutcome(outcome: Settled<unknown>): string {
	if (outcome.ok) {
		return 'no error';
	}
	if (outcome.error instanceof CollegeDBError) {
		return `${outcome.error.code}: ${outcome.error.message.slice(0, 120)} cause ${describeDriverError((outcome.error as { cause?: unknown }).cause)}`;
	}
	return describeDriverError(outcome.error);
}

async function tableExists(db: SQLDatabase, table: string): Promise<boolean> {
	const sql =
		db.dialect === 'postgres'
			? "SELECT COUNT(*) AS n FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ?"
			: db.dialect === 'mysql'
				? 'SELECT COUNT(*) AS n FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?'
				: "SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'table' AND name = ?";
	const row = await db.prepare(sql).bind(table).first<{ n: unknown }>();
	return Number(row?.n) > 0;
}

function liveResult(name: ScenarioName, started: number, iterations: number, lines: string[], problems: string[]): ScenarioStats {
	const elapsed = performance.now() - started;
	return {
		name,
		status: problems.length === 0 ? 'passed' : 'failed',
		iterations,
		samplesMs: [elapsed],
		...calculateLatencyStats([elapsed]),
		notes: lines.join(' | '),
		// a failed row shows only its error, so the recorded driver errors travel with it
		error: problems.length === 0 ? undefined : `${problems.join('; ')} [${lines.join(' | ')}]`
	};
}

const insertRaceRow = (db: SQLDatabase, key: string, name = 'orig') =>
	db.prepare(`INSERT INTO ${RACE_TABLE} (id, name, qty) VALUES (?, ?, ?)`).bind(key, name, 1).run();

// #region missing sidecar scenario

/**
 * A shard that never held the sidecar tables must read as "no tombstone", a lookup that fails for any
 * other reason must not, and the read must never create a table.
 */
async function scenarioMissingSidecarLookup(dbFlavor: DatabaseFlavor, profile: AdapterProfile, kv: KVStorage): Promise<ScenarioStats> {
	const started = performance.now();
	const lines: string[] = [];
	const problems: string[] = [];
	const expect = (condition: unknown, message: string) => {
		if (!condition) {
			problems.push(message);
		}
	};
	const mysqlFamily = dbFlavor === 'mysql' || dbFlavor === 'mariadb';
	const select = `SELECT name FROM ${RACE_TABLE} WHERE id = ?`;
	const pending: Promise<unknown>[] = [];
	let runtime: SQLRuntime | null = null;
	let reads = 0;

	try {
		runtime = await createSQLRuntime(dbFlavor, createRunId(`sidecar_${dbFlavor}_${profile}`), profile, ['shard-a', 'shard-b', 'shard-j']);
		const raw = runtime.shards;
		const tap: Tap = { log: [], probe: 'real' };
		const shards = tapAll(raw, tap);
		const configFor = (used: Record<string, SQLDatabase>): CollegeDBConfig => ({
			kv,
			shards: used,
			strategy: 'hash',
			journalShard: 'shard-j',
			disableAutoMigration: true,
			hashShardMappings: false,
			mappingCacheTtlMs: 0,
			waitUntil: (promise) => void pending.push(promise)
		});

		await createSchemaAcrossShards(raw, RACE_SCHEMA);
		await initializeAsync(configFor(shards));
		await Promise.allSettled(pending);

		const mapper = raceMapper(kv);
		const a = raw['shard-a']!;
		const sidecar = ['collegedb_retired', 'collegedb_topology', ...(mysqlFamily ? ['collegedb_lock_stripes'] : [])];
		const routedTo = async (key: string, binding: string) => {
			await mapper.setShardMapping(key, binding);
			await invalidateMappingCache(key);
		};
		const absent = async () => (await Promise.all(sidecar.map((table) => tableExists(a, table)))).every((exists) => !exists);

		// arm 1: the shard has never held the sidecar tables
		for (const table of sidecar) {
			await a.prepare(`DROP TABLE IF EXISTS ${table}`).run();
		}
		expect(await absent(), 'the sidecar tables could not be removed from shard-a');

		const key = `sc_${Date.now()}`;
		const lookup = await settle(findTombstone(a, 'default', key));
		expect(!lookup.ok, 'a tombstone lookup on a shard with no sidecar table did not fail');
		lines.push(`missing table error: ${lookup.ok ? 'none' : describeDriverError(lookup.error)}`);

		await routedTo(key, 'shard-a');
		tap.log.length = 0;
		for (let i = 0; i < 3; i++) {
			const row = await settle(first(key, select, [key]));
			const rows = await settle(all(key, select, [key]));
			const best = await settle(first(key, select, [key], { bestEffort: true }));
			reads += 3;
			expect(row.ok && row.value === null, `first on a shard with no sidecar table: ${describeOutcome(row)}`);
			expect(rows.ok && rows.value.results.length === 0, `all on a shard with no sidecar table: ${describeOutcome(rows)}`);
			expect(best.ok && best.value === null, `first with bestEffort on a shard with no sidecar table: ${describeOutcome(best)}`);
		}
		const onA = tap.log.filter((entry) => entry.startsWith('shard-a '));
		expect(
			onA.filter((entry) => entry.includes('FROM collegedb_retired')).length === 9,
			`expected 9 tombstone lookups on shard-a, saw ${onA.filter((entry) => entry.includes('FROM collegedb_retired')).length}`
		);
		expect(!onA.some((entry) => /^\S+\s+(CREATE|DROP|ALTER|INSERT|UPDATE|DELETE)\b/i.test(entry)), 'a read sent DDL or a write to shard-a');
		expect(await absent(), 'a read created a sidecar table on shard-a');
		lines.push('absent read: 9 reads answered not found, 9 lookups, no DDL, tables still absent');

		// control: the catalog check sees a table when one exists
		await runStatements(a, buildShardSidecarSchema(a.dialect));
		expect(await tableExists(a, 'collegedb_retired'), 'the catalog check missed a table that exists');

		// control: with the tables present the same lookup finds a real tombstone
		const moved = `${key}_moved`;
		await seedRaceRow(mapper, moved);
		await reassignShard(moved, 'shard-b', RACE_TABLE);
		await routedTo(moved, 'shard-a');
		const followed = await settle(first<{ name: string }>(moved, select, [moved]));
		reads++;
		expect(followed.ok && followed.value?.name === 'orig', `a moved key was not followed to its destination: ${describeOutcome(followed)}`);
		await routedTo(moved, 'shard-b');

		// arm 2: the lookup fails for a real reason that is not a missing table
		const failing = async (label: string, code: string) => {
			const target = `sc_${label}_${Date.now()}`;
			await routedTo(target, 'shard-a');
			const thrown = await settle(first(target, select, [target]));
			const quiet = await settle(first(target, select, [target], { bestEffort: true }));
			const quietAll = await settle(all(target, select, [target], { bestEffort: true }));
			reads += 3;
			const cause = !thrown.ok && thrown.error instanceof CollegeDBError ? (thrown.error as { cause?: unknown }).cause : undefined;
			expect(
				!thrown.ok && thrown.error instanceof CollegeDBError && thrown.error.code === code,
				`${label}: first threw ${describeOutcome(thrown)}`
			);
			expect(quiet.ok && quiet.value === null, `${label}: bestEffort first: ${describeOutcome(quiet)}`);
			expect(quietAll.ok, `${label}: all: ${describeOutcome(quietAll)}`);
			lines.push(`${label}: ${describeDriverError(cause)}`);
		};

		await a.prepare('ALTER TABLE collegedb_retired RENAME COLUMN moved_to TO moved_to_x').run();
		try {
			await failing('renamed column', 'MIGRATION_LOOKUP_UNAVAILABLE');
		} finally {
			await a.prepare('ALTER TABLE collegedb_retired RENAME COLUMN moved_to_x TO moved_to').run();
		}
		const restored = `sc_restored_${Date.now()}`;
		await routedTo(restored, 'shard-a');
		const afterRestore = await settle(first(restored, select, [restored]));
		reads++;
		expect(afterRestore.ok && afterRestore.value === null, `after the column was restored: ${describeOutcome(afterRestore)}`);

		if (runtime.restricted) {
			const reader = await runtime.restricted('shard-a', [RACE_TABLE]);
			const warn = console.warn;
			console.warn = () => undefined;
			try {
				initialize(configFor({ ...shards, 'shard-a': tapShard(reader, 'shard-a', tap) }));
				await Promise.allSettled(pending);
			} finally {
				console.warn = warn;
			}
			await failing('revoked privilege', 'MIGRATION_LOOKUP_UNAVAILABLE');
		} else {
			lines.push('revoked privilege: not available on SQLite');
		}
	} catch (error) {
		problems.push(error instanceof Error ? `${error.message}\n${error.stack}` : String(error));
	} finally {
		resetConfig();
		resetPlacementState();
		await safely(async () => void (await Promise.all(PLACEMENT_KV_KEYS.map((entry) => kv.delete(entry)))));
		if (runtime) {
			await safely(async () => runtime?.close());
		}
	}

	return liveResult('missing_sidecar_lookup', started, reads, lines, problems);
}

// #endregion

// #region reconcile scenario

interface ReconcileExpectation {
	examined: number;
	routingAgreed: number;
	recordVerified: number;
	recordMissing: string[];
	recordUnchecked: number;
	missingMapping: string[];
	disagreeing: string[];
	repaired: number;
	pruned: number;
}

/** A coordinator ledger in memory, shaped like the Durable Object's allocations routes */
function fakeCoordinator(ledger: Map<string, string>): NonNullable<CollegeDBConfig['coordinator']> {
	const stub = {
		fetch: async (url: string, init?: { body?: string }) => {
			const target = new URL(url);
			if (target.pathname === '/allocations/prune') {
				const { primaryKeys } = JSON.parse(init?.body ?? '{}') as { primaryKeys: string[] };
				return Response.json({ pruned: primaryKeys.filter((key) => ledger.delete(key)).length });
			}
			const after = target.searchParams.get('after') ?? '';
			const limit = Number(target.searchParams.get('limit') ?? 100);
			const keys = [...ledger.keys()].filter((key) => key > after).sort();
			const page = keys.slice(0, limit);
			return Response.json({
				allocations: page.map((primaryKey) => ({ primaryKey, shard: ledger.get(primaryKey) })),
				next: keys.length > limit ? page[page.length - 1] : null
			});
		}
	};
	return { idFromName: (name: string) => name, get: () => stub } as unknown as NonNullable<CollegeDBConfig['coordinator']>;
}

/** Compares a report with what the live rows say it must be; the controls feed it a distorted shard layer and must fail it */
function reconcileProblems(
	label: string,
	report: Awaited<ReturnType<typeof reconcileAllocations>>,
	expected: ReconcileExpectation
): string[] {
	const actual: ReconcileExpectation = {
		examined: report.examined,
		routingAgreed: report.routingAgreed,
		recordVerified: report.recordVerified,
		recordMissing: report.recordMissing,
		recordUnchecked: report.recordUnchecked,
		missingMapping: report.missingMapping,
		disagreeing: report.disagreeing.map((entry) => entry.key),
		repaired: report.repaired,
		pruned: report.pruned
	};
	const found: string[] = [];
	for (const field of Object.keys(expected) as Array<keyof ReconcileExpectation>) {
		if (JSON.stringify(actual[field]) !== JSON.stringify(expected[field])) {
			found.push(`${label}: ${field} was ${JSON.stringify(actual[field])}, expected ${JSON.stringify(expected[field])}`);
		}
	}
	if (report.recordMissingCount !== report.recordMissing.length) {
		found.push(`${label}: recordMissingCount ${report.recordMissingCount} against ${report.recordMissing.length} listed`);
	}
	return found;
}

/**
 * reconcileAllocations with verifyRecords against live rows: a hit, a miss, a probe the shard refuses
 * and a table that does not exist.
 */
async function scenarioReconcileRecordProbe(dbFlavor: DatabaseFlavor, profile: AdapterProfile, kv: KVStorage): Promise<ScenarioStats> {
	const started = performance.now();
	const lines: string[] = [];
	const problems: string[] = [];
	const pending: Promise<unknown>[] = [];
	let runtime: SQLRuntime | null = null;
	let runs = 0;

	try {
		runtime = await createSQLRuntime(dbFlavor, createRunId(`reconcile_${dbFlavor}_${profile}`), profile, ['shard-a', 'shard-b', 'shard-c']);
		const raw = runtime.shards;
		await createSchemaAcrossShards(raw, RACE_SCHEMA);

		// shard-c refuses every probe: a login with no privilege on the table, or on SQLite a view that overflows when read
		// (a renamed column would not do, since SQLite reads a double-quoted unknown column as a string literal)
		let refusing = raw['shard-c']!;
		if (runtime.restricted) {
			refusing = await runtime.restricted('shard-c', []);
		} else {
			await refusing.prepare(`DROP TABLE ${RACE_TABLE}`).run();
			await refusing.prepare(`CREATE VIEW ${RACE_TABLE} (id, name, qty) AS SELECT abs(-9223372036854775807 - 1), 'x', 1`).run();
		}
		const refusal = await settle(refusing.prepare(`SELECT 1 FROM ${RACE_TABLE} WHERE id = ? LIMIT 1`).bind('x').first());
		lines.push(`refused probe on shard-c: ${refusal.ok ? 'none' : describeDriverError(refusal.error)}`);
		if (refusal.ok) {
			problems.push('shard-c did not refuse the probe');
		}
		const missingTable = await settle(
			raw['shard-a']!.prepare(`SELECT 1 FROM ${quoteIdentifier('no_such_table_ff2', raw['shard-a']!.dialect)} WHERE id = ? LIMIT 1`)
				.bind('x')
				.first()
		);
		lines.push(`unconfigured table probe: ${missingTable.ok ? 'none' : describeDriverError(missingTable.error)}`);

		const tap: Tap = { log: [], probe: 'real' };
		const shards = tapAll({ ...raw, 'shard-c': refusing }, tap);
		const ledger = new Map<string, string>();
		const prefix = `rp_${Date.now()}`;
		const [k1, k2, k3, k4, k5, k6] = [1, 2, 3, 4, 5, 6].map((n) => `${prefix}_${n}`) as [string, string, string, string, string, string];
		const mapper = raceMapper(kv);

		await insertRaceRow(raw['shard-a']!, k1);
		await insertRaceRow(raw['shard-a']!, k3);
		await insertRaceRow(raw['shard-b']!, k5);
		for (const [key, shard] of [
			[k1, 'shard-a'],
			[k2, 'shard-a'],
			[k3, 'shard-a'],
			[k4, 'shard-a'],
			[k5, 'shard-a'],
			[k6, 'shard-c']
		] as const) {
			ledger.set(key, shard);
		}
		for (const [key, shard] of [
			[k1, 'shard-a'],
			[k2, 'shard-a'],
			[k5, 'shard-b'],
			[k6, 'shard-c']
		] as const) {
			await mapper.setShardMapping(key, shard);
		}

		initialize({
			kv,
			shards,
			strategy: 'hash',
			hashShardMappings: false,
			mappingCacheTtlMs: 0,
			disableAutoMigration: true,
			coordinator: fakeCoordinator(ledger),
			waitUntil: (promise) => void pending.push(promise)
		});
		await Promise.allSettled(pending);

		const verify = { tables: [{ table: RACE_TABLE }] };
		const probesSent = () => tap.log.filter((entry) => /^\S+\s+SELECT 1 FROM/i.test(entry)).length;
		const reconcile = async (options: Parameters<typeof reconcileAllocations>[0]) => {
			tap.log.length = 0;
			runs++;
			return await reconcileAllocations(options);
		};
		const reader = new KVShardMapper(kv, { hashShardMappings: false, mappingCacheTtlMs: 0 });
		const mapped = async (key: string) => (await reader.getShardMapping(key))?.shard ?? null;
		const addProblems = (found: string[]) => void problems.push(...found);
		const expectation = (overrides: Partial<ReconcileExpectation>): ReconcileExpectation => ({
			examined: 6,
			routingAgreed: 3,
			recordVerified: 3,
			recordMissing: [k2, k4],
			recordUnchecked: 1,
			missingMapping: [k3, k4],
			disagreeing: [k5],
			repaired: 0,
			pruned: 0,
			...overrides
		});

		// no verifyRecords: nothing is queried and nothing counts as checked
		const observed = await reconcile({ limit: 4 });
		addProblems(
			reconcileProblems('without verifyRecords', observed, expectation({ recordVerified: 0, recordMissing: [], recordUnchecked: 6 }))
		);
		if (tap.log.length !== 0) {
			problems.push(`a run without verifyRecords sent ${tap.log.length} statements to the shards, first: ${tap.log[0]}`);
		}

		// verifyRecords, observational
		const first1 = await reconcile({ limit: 4, verifyRecords: verify });
		addProblems(reconcileProblems('verifyRecords', first1, expectation({})));
		if (probesSent() !== 6) {
			problems.push(`verifyRecords sent ${probesSent()} probes for 6 rows`);
		}
		if (ledger.size !== 6 || (await mapped(k3)) !== null || (await mapped(k4)) !== null) {
			problems.push('an observational run changed the ledger or wrote a mapping');
		}
		const paged = await reconcile({ limit: 1, verifyRecords: verify });
		if (JSON.stringify(paged) !== JSON.stringify(first1)) {
			problems.push(`a page size of 1 changed the report: ${JSON.stringify(paged)}`);
		}

		// controls: a failed probe read as missing, and every probe read as a hit, must trip the same checks
		for (const mode of ['swallow', 'always'] as const) {
			tap.probe = mode;
			const distorted = reconcileProblems(`control ${mode}`, await reconcile({ limit: 4, verifyRecords: verify }), expectation({}));
			tap.probe = 'real';
			lines.push(`control ${mode}: ${distorted.length} checks tripped`);
			if (distorted.length === 0) {
				problems.push(`control ${mode} tripped no check, so the checks are not shown able to see it`);
			}
		}

		// repair writes a mapping only where the row exists on the ledger shard
		const repaired = await reconcile({ limit: 4, repair: true, verifyRecords: verify });
		addProblems(reconcileProblems('repair', repaired, expectation({ repaired: 1 })));
		if ((await mapped(k3)) !== 'shard-a' || (await mapped(k4)) !== null || ledger.size !== 6) {
			problems.push(`repair left k3 at ${await mapped(k3)} and k4 at ${await mapped(k4)}`);
		}

		// prune removes only the rows routing agrees on, that have a mapping, and whose row exists
		const pruned = await reconcile({ limit: 4, pruneVerified: true, verifyRecords: verify });
		addProblems(reconcileProblems('prune', pruned, expectation({ routingAgreed: 4, missingMapping: [k4], pruned: 2 })));
		if ([...ledger.keys()].sort().join() !== [k2, k4, k5, k6].sort().join()) {
			problems.push(`prune left ledger keys ${[...ledger.keys()].sort().join()}`);
		}

		// a table that does not exist: the engine's missing-table error is unchecked, never missing
		const left = { examined: 4, routingAgreed: 2, missingMapping: [k4], disagreeing: [k5], repaired: 0, pruned: 0 };
		const mixed = await reconcile({
			limit: 4,
			repair: true,
			pruneVerified: true,
			verifyRecords: { tables: [{ table: RACE_TABLE }, { table: 'no_such_table_ff2' }] }
		});
		addProblems(
			reconcileProblems('listed table plus a missing one', mixed, { ...left, recordVerified: 1, recordMissing: [], recordUnchecked: 3 })
		);
		const absentOnly = await reconcile({
			limit: 4,
			repair: true,
			pruneVerified: true,
			verifyRecords: { tables: [{ table: 'no_such_table_ff2' }] }
		});
		addProblems(
			reconcileProblems('only a missing table', absentOnly, { ...left, recordVerified: 0, recordMissing: [], recordUnchecked: 4 })
		);
		if (ledger.size !== 4 || (await mapped(k4)) !== null) {
			problems.push('a run whose probes all failed repaired or pruned something');
		}
	} catch (error) {
		problems.push(error instanceof Error ? `${error.message}\n${error.stack}` : String(error));
	} finally {
		resetConfig();
		resetPlacementState();
		await safely(async () => void (await Promise.all(PLACEMENT_KV_KEYS.map((entry) => kv.delete(entry)))));
		if (runtime) {
			await safely(async () => runtime?.close());
		}
	}

	return liveResult('reconcile_record_probe', started, runs, lines, problems);
}

// #endregion

// #region recovery lease scenario

interface LeaseWorld {
	/** The shards as the engine sees them, with no tap */
	raw: Record<string, SQLDatabase>;
	shards: Record<string, SQLDatabase>;
	tap: Tap;
	clock: { now: number };
	dialect: SQLDatabase['dialect'];
	close: () => Promise<void>;
}

interface LeaseArm {
	label: string;
	kind: 'takeover' | 'race';
	/** The state the move is left in, which is also the step recovery A is held at */
	leftAt?: 'cutover_pending' | 'copied';
	/** The protection this arm breaks on purpose */
	control?: 'no-fence' | 'no-check' | 'no-expiry';
	iterations: number;
}

async function openLeaseWorld(dbFlavor: DatabaseFlavor, profile: AdapterProfile): Promise<LeaseWorld> {
	const runtime = await createSQLRuntime(dbFlavor, createRunId(`lease_${dbFlavor}_${profile}`), profile, ['shard-a', 'shard-b', 'shard-j']);
	const tap: Tap = { log: [], probe: 'real' };
	const shards = tapAll(runtime.shards, tap);

	try {
		await createSchemaAcrossShards(runtime.shards, RACE_SCHEMA);
		await ensureJournalSchema(shards, 'shard-j');
	} catch (error) {
		await safely(async () => runtime.close());
		throw error;
	}

	return {
		raw: runtime.shards,
		shards,
		tap,
		clock: { now: Date.now() },
		dialect: runtime.shards['shard-j']!.dialect,
		close: runtime.close
	};
}

const leaseEnv = (world: LeaseWorld, scope: string): MigrationEnv => ({
	scope,
	journalBinding: 'shard-j',
	shards: world.shards,
	publish: async () => undefined,
	now: () => world.clock.now
});

/** Drops the generation condition from every fenced journal write */
const stripFence = (sql: string) => (FENCE_PREDICATE.test(sql) ? { sql: sql.replace(FENCE_PREDICATE, '1 = 1'), drop: 2 } : null);

/** Drops the expiry condition from the lease acquire, so every caller takes the lease */
const stripAcquire = (sql: string) => {
	if (!sql.includes(LEASE_TABLE)) {
		return null;
	}
	if (/ON DUPLICATE KEY UPDATE/i.test(sql)) {
		return { sql: sql.replace(/IF\([`"]?expires_at[`"]? <= \?, ([^,]+), [^)]+\)/g, '$1'), drop: 3 };
	}
	if (/DO UPDATE SET/i.test(sql)) {
		return { sql: sql.replace(/ WHERE \S*expires_at\S* <= \?$/, ''), drop: 1 };
	}
	return null;
};

/** Installs the process-wide step hook for one key: crash once at a step, hold the first arrival at a step, or slow each arrival */
function makeGate(key: string) {
	let arrive: () => void = () => undefined;
	let release: () => void = () => undefined;
	const arrived = new Promise<void>((resolveArrival) => (arrive = resolveArrival));
	const released = new Promise<void>((resolveRelease) => (release = resolveRelease));
	const gate = { crashAt: '', holdAt: '', delayAt: '', held: false, arrived, release: () => release() };

	setMigrationStepHook(async (step, info) => {
		if (info.key !== key) {
			return;
		}
		if (step === gate.crashAt) {
			gate.crashAt = '';
			throw new Error(CRASH_MESSAGE);
		}
		if (step === gate.delayAt) {
			await pause(6);
		}
		if (step === gate.holdAt && !gate.held) {
			gate.held = true;
			arrive();
			await released;
		}
	});

	return gate;
}

async function leaseRow(world: LeaseWorld, scope: string) {
	return await world.raw['shard-j']!.prepare(`SELECT holder, generation, expires_at FROM ${LEASE_TABLE} WHERE scope = ?`)
		.bind(scope)
		.first<{ holder: string; generation: unknown; expires_at: unknown }>();
}

/** Everything a recovery could still write for one move: its journal rows, the lease, and the key on both shards */
async function leaseSnapshot(world: LeaseWorld, scope: string, key: string): Promise<string> {
	const rows = async (binding: string, sql: string, ...bindings: unknown[]) =>
		(
			await world.raw[binding]!.prepare(sql)
				.bind(...bindings)
				.all()
		).results;

	return JSON.stringify([
		await rows(
			'shard-j',
			'SELECT op_id, state, attempt, generation, digest FROM collegedb_migration_history WHERE scope = ? ORDER BY op_id, state, attempt',
			scope
		),
		await rows('shard-j', 'SELECT op_id FROM collegedb_migration_active WHERE scope = ? AND primary_key = ?', scope, key),
		await rows('shard-j', `SELECT holder, generation, expires_at FROM ${LEASE_TABLE} WHERE scope = ?`, scope),
		await rows('shard-a', `SELECT id, name, qty FROM ${RACE_TABLE} WHERE id = ?`, key),
		await rows('shard-b', `SELECT id, name, qty FROM ${RACE_TABLE} WHERE id = ?`, key),
		await rows('shard-a', 'SELECT op_id, generation FROM collegedb_retired WHERE scope = ? AND primary_key = ?', scope, key),
		await rows('shard-b', 'SELECT op_id, generation FROM collegedb_retired WHERE scope = ? AND primary_key = ?', scope, key)
	]);
}

/** Leaves a move at the named step by crashing the executor there, and returns what went wrong if it did not stop */
async function leaveMoveAt(
	world: LeaseWorld,
	gate: ReturnType<typeof makeGate>,
	env: MigrationEnv,
	key: string,
	step: string
): Promise<string | null> {
	await insertRaceRow(world.raw['shard-a']!, key);
	gate.crashAt = step;
	const crashed = await settle(moveRecord(env, { table: RACE_TABLE, key, source: 'shard-a', destination: 'shard-b' }));
	if (crashed.ok || !(crashed.error instanceof Error) || crashed.error.message !== CRASH_MESSAGE) {
		return `the move did not stop at ${step}: ${describeOutcome(crashed)}`;
	}
	const status = await getMigrationStatus(env, key, RACE_TABLE);
	if (status?.state === step && status.active) {
		return null;
	}
	const history = await world.raw['shard-j']!.prepare(`SELECT state, attempt FROM collegedb_migration_history WHERE primary_key = ?`)
		.bind(key)
		.all();
	const tail = world.tap.log.slice(-5).map((entry) => entry.slice(0, 80));
	return `the journal shows ${status?.state} (active ${status?.active}) after a crash at ${step}; history ${JSON.stringify(history.results)}; last statements ${JSON.stringify(tail)}`;
}

/** Recovery A is held mid-run, its lease expires, B takes over and finishes, then A is released */
async function leaseTakeover(world: LeaseWorld, arm: LeaseArm, i: number, prefix: string): Promise<string[]> {
	const problems: string[] = [];
	const key = `${prefix}_${i}`;
	const fail = (message: string) => void problems.push(`${arm.label}/${i}: ${message}`);
	const env = leaseEnv(world, key);
	const gate = makeGate(key);
	const step = arm.leftAt!;
	let recoveryA: Promise<Settled<Awaited<ReturnType<typeof recoverMigrations>>>> | null = null;

	world.tap.log.length = 0;
	world.tap.rewrite = undefined;
	world.tap.fakeLease = undefined;

	try {
		const left = await leaveMoveAt(world, gate, env, key, step);
		if (left) {
			fail(left);
			return problems;
		}

		gate.holdAt = step;
		const startedAt = world.clock.now;
		recoveryA = settle(recoverMigrations(env));
		const reached = await Promise.race([gate.arrived.then(() => true), recoveryA.then(() => false)]);
		if (!reached) {
			fail('recovery A finished without reaching the hold');
			return problems;
		}

		let row = await leaseRow(world, key);
		if (Number(row?.generation) !== 1 || Number(row?.expires_at) !== startedAt + 60_000) {
			fail(`lease row while A runs: generation ${row?.generation}, expires_at ${row?.expires_at}, expected 1 and ${startedAt + 60_000}`);
		}
		for (const offset of [0, 59_999]) {
			world.clock.now = startedAt + offset;
			const refused = await settle(recoverMigrations(env));
			if (refused.ok || !(refused.error instanceof CollegeDBError) || refused.error.code !== 'RECOVERY_IN_PROGRESS') {
				fail(`a recovery ${offset} ms into a live lease: ${describeOutcome(refused)}`);
			}
		}

		world.clock.now = startedAt + 60_000;
		const takeover = await settle(recoverMigrations(env));
		if (!takeover.ok || takeover.value.committed.length !== 1 || takeover.value.failed.length !== 0) {
			fail(`recovery B at expiry: ${takeover.ok ? JSON.stringify(takeover.value) : describeOutcome(takeover)}`);
		}
		row = await leaseRow(world, key);
		if (Number(row?.generation) !== 2) {
			fail(`lease generation after the takeover is ${row?.generation}, expected 2`);
		}
		const status = await getMigrationStatus(env, key, RACE_TABLE);
		const [onA, onB] = await Promise.all(
			['shard-a', 'shard-b'].map(
				async (binding) => (await world.raw[binding]!.prepare(`SELECT id FROM ${RACE_TABLE} WHERE id = ?`).bind(key).all()).results.length
			)
		);
		if (status?.active !== false || onA !== 0 || onB !== 1) {
			fail(`after B: state ${status?.state}, active ${status?.active}, ${onA} rows on the source, ${onB} on the destination`);
		}

		const before = await leaseSnapshot(world, key, key);
		const mark = world.tap.log.length;
		if (arm.control === 'no-fence') {
			world.tap.rewrite = stripFence;
		} else if (arm.control === 'no-check') {
			world.tap.fakeLease = 1;
		}
		gate.release();
		const resumed = await recoveryA;
		recoveryA = null;

		const sent = world.tap.log.slice(mark).filter(isWriteEntry);
		const shardWrites = sent.filter((entry) => !entry.startsWith('shard-j '));
		const journalWrites = sent.filter((entry) => entry.startsWith('shard-j '));
		const fenced = !resumed.ok && resumed.error instanceof CollegeDBError && resumed.error.code === 'RECOVERY_FENCED';
		const allowed = step === 'copied' ? 1 : 0;

		if (!fenced) {
			fail(`recovery A ended with ${describeOutcome(resumed)}, expected RECOVERY_FENCED`);
		}
		if ((await leaseSnapshot(world, key, key)) !== before) {
			fail('the journal, the lease or a shard changed after the takeover');
		}
		if (shardWrites.length > 0) {
			fail(`recovery A sent ${shardWrites.length} writes to a shard after the takeover, first: ${shardWrites[0]!.slice(0, 90)}`);
		}
		if (journalWrites.length > allowed || journalWrites.some((entry) => !entry.includes(LEASE_TABLE))) {
			fail(
				`recovery A sent ${journalWrites.length} journal writes after the takeover (${allowed} fenced write allowed), first: ${journalWrites[0]?.slice(0, 90)}`
			);
		}
	} finally {
		gate.release();
		setMigrationStepHook(null);
		world.tap.rewrite = undefined;
		world.tap.fakeLease = undefined;
		if (recoveryA) {
			await recoveryA;
		}
	}

	return problems;
}

/** Two or four recoveries start together on one unfinished move */
async function leaseRace(world: LeaseWorld, arm: LeaseArm, i: number, prefix: string): Promise<string[]> {
	const problems: string[] = [];
	const key = `${prefix}_${i}`;
	const racers = i % 2 === 0 ? 2 : 4;
	const fail = (message: string) => void problems.push(`${arm.label}/${i}/${racers} racers: ${message}`);
	const env = leaseEnv(world, key);
	const gate = makeGate(key);

	world.tap.log.length = 0;
	world.tap.rewrite = arm.control === 'no-expiry' ? stripAcquire : undefined;

	try {
		const left = await leaveMoveAt(world, gate, env, key, 'cutover_pending');
		if (left) {
			fail(left);
			return problems;
		}

		// holding each arrival keeps the winner inside its recovery while the others try to take the lease
		gate.delayAt = 'cutover_pending';
		const outcomes = await Promise.all(Array.from({ length: racers }, () => settle(recoverMigrations(env))));
		const won = outcomes.filter((outcome) => outcome.ok);
		const refused = outcomes.filter(
			(outcome) => !outcome.ok && outcome.error instanceof CollegeDBError && outcome.error.code === 'RECOVERY_IN_PROGRESS'
		);
		const other = outcomes.filter((outcome) => !outcome.ok && !refused.includes(outcome));
		if (won.length !== 1 || refused.length !== racers - 1 || other.length > 0) {
			fail(
				`${won.length} ran, ${refused.length} refused with RECOVERY_IN_PROGRESS, ${other.length} other: ${other.map(describeOutcome).join(', ')}`
			);
		}
		const report = won.find((outcome) => outcome.ok);
		if (report?.ok && (report.value.committed.length !== 1 || report.value.failed.length !== 0)) {
			fail(`the winner reported ${JSON.stringify(report.value)}`);
		}

		const row = await leaseRow(world, key);
		if (Number(row?.generation) !== 1 || Number(row?.expires_at) !== 0) {
			fail(`lease row after the race: generation ${row?.generation}, expires_at ${row?.expires_at}, expected 1 and 0`);
		}
		const status = await getMigrationStatus(env, key, RACE_TABLE);
		const [onA, onB, tombstones] = await Promise.all([
			world.raw['shard-a']!.prepare(`SELECT id FROM ${RACE_TABLE} WHERE id = ?`)
				.bind(key)
				.all()
				.then((result) => result.results.length),
			world.raw['shard-b']!.prepare(`SELECT id FROM ${RACE_TABLE} WHERE id = ?`)
				.bind(key)
				.all()
				.then((result) => result.results.length),
			world.raw['shard-a']!.prepare('SELECT op_id FROM collegedb_retired WHERE scope = ? AND primary_key = ?')
				.bind(key, key)
				.all()
				.then((result) => result.results.length)
		]);
		if (status?.active !== false || onA !== 0 || onB !== 1 || tombstones !== 1) {
			fail(
				`move ended in ${status?.state} (active ${status?.active}): ${onA} source rows, ${onB} destination rows, ${tombstones} tombstones`
			);
		}
	} finally {
		setMigrationStepHook(null);
		world.tap.rewrite = undefined;
	}

	return problems;
}

/**
 * Lease expiry and takeover on a live engine, and the same protections broken on purpose. A control
 * arm that reports no violation would mean its check cannot see the defect it stands for.
 */
async function scenarioRecoveryLeaseTakeover(dbFlavor: DatabaseFlavor, profile: AdapterProfile, perKind: number): Promise<ScenarioStats> {
	const started = performance.now();
	const lines: string[] = [];
	const problems: string[] = [];
	const n = Math.max(8, Math.ceil(perKind / 4));
	const nc = Math.max(4, Math.ceil(n / 2));
	const arms: LeaseArm[] = [
		{ label: 'takeover at cutover_pending', kind: 'takeover', leftAt: 'cutover_pending', iterations: n },
		{ label: 'takeover at copied', kind: 'takeover', leftAt: 'copied', iterations: n },
		{ label: 'concurrent recoveries', kind: 'race', iterations: n },
		{ label: 'control (no generation on journal writes)', kind: 'takeover', leftAt: 'copied', control: 'no-fence', iterations: nc },
		{
			label: 'control (no generation read before a shard step)',
			kind: 'takeover',
			leftAt: 'cutover_pending',
			control: 'no-check',
			iterations: nc
		},
		{ label: 'control (acquire with no expiry condition)', kind: 'race', control: 'no-expiry', iterations: nc }
	];
	let world: LeaseWorld | null = null;
	let total = 0;

	try {
		world = await openLeaseWorld(dbFlavor, profile);
		const prefix = `ls_${Date.now()}`;

		for (const [index, arm] of arms.entries()) {
			const found: string[][] = [];
			for (let i = 0; i < arm.iterations; i++) {
				found.push(await (arm.kind === 'takeover' ? leaseTakeover : leaseRace)(world, arm, i, `${prefix}_${index}`));
				total++;
			}

			const flagged = found.filter((entry) => entry.length > 0);
			const line = `${arm.label} n=${arm.iterations}: ${flagged.length} iterations with violations`;
			lines.push(line);
			console.log(`[Sandbox] recovery_lease_takeover ${dbFlavor}/${profile} ${line}`);
			if (flagged.length > 0) {
				console.log(`[Sandbox]   first violation: ${flagged[0]![0]}`);
			}
			if (!arm.control && flagged.length > 0) {
				problems.push(`${arm.label}: ${flagged.length} iterations, first: ${flagged[0]![0]}`);
			} else if (arm.control && flagged.length === 0) {
				problems.push(`${arm.label}: the control reported no violation, so the checks are not shown able to see it`);
			}
		}
	} catch (error) {
		problems.push(error instanceof Error ? `${error.message}\n${error.stack}` : String(error));
	} finally {
		setMigrationStepHook(null);
		if (world) {
			await safely(async () => world?.close());
		}
	}

	return liveResult('recovery_lease_takeover', started, total, lines, problems);
}

// #endregion

// #region history index scenario

/** The columns of the history key index in order, read from the engine catalog; empty when the index does not exist */
async function historyIndexColumns(db: SQLDatabase): Promise<string[]> {
	const { table, name } = HISTORY_INDEX;
	if (db.dialect === 'postgres') {
		const row = await db
			.prepare("SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' AND tablename = ? AND indexname = ?")
			.bind(table, name)
			.first<{ indexdef: string }>();
		return row ? (/\(([^)]*)\)\s*$/.exec(row.indexdef)?.[1] ?? '').split(',').map((column) => column.trim()) : [];
	}

	const sql =
		db.dialect === 'mysql'
			? 'SELECT column_name AS col FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ? ORDER BY seq_in_index'
			: 'SELECT name AS col FROM pragma_index_info(?) ORDER BY seqno';
	const rows = await db
		.prepare(sql)
		.bind(...(db.dialect === 'mysql' ? [table, name] : [name]))
		.all<{ col: string }>();
	return rows.results.map((row) => row.col);
}

/** How many indexes the history table has besides its primary key, so a duplicate under another name shows up */
async function historyExtraIndexes(db: SQLDatabase): Promise<number> {
	const { table } = HISTORY_INDEX;
	const sql =
		db.dialect === 'postgres'
			? "SELECT COUNT(*) AS n FROM pg_indexes WHERE schemaname = 'public' AND tablename = ? AND indexname NOT LIKE '%_pkey'"
			: db.dialect === 'mysql'
				? "SELECT COUNT(DISTINCT index_name) AS n FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name <> 'PRIMARY'"
				: "SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND name NOT LIKE 'sqlite_autoindex%'";
	return Number((await db.prepare(sql).bind(table).first<{ n: unknown }>())?.n);
}

/**
 * A journal table created before the index existed gets it from the ensure step, once and without error,
 * however often and however concurrently the step runs.
 */
async function scenarioHistoryIndexEnsure(dbFlavor: DatabaseFlavor, profile: AdapterProfile, perKind: number): Promise<ScenarioStats> {
	const started = performance.now();
	const lines: string[] = [];
	const problems: string[] = [];
	const rounds = Math.max(8, Math.ceil(perKind / 4));
	const wanted = HISTORY_INDEX.columns.join(',');
	let runtime: SQLRuntime | null = null;
	let ensures = 0;

	try {
		runtime = await createSQLRuntime(dbFlavor, createRunId(`index_${dbFlavor}_${profile}`), profile, ['shard-j']);
		const tap: Tap = { log: [], probe: 'real' };
		const db = tapShard(runtime.shards['shard-j']!, 'shard-j', tap);
		const state = async () => ({ columns: (await historyIndexColumns(db)).join(','), extra: await historyExtraIndexes(db) });
		const expectState = (label: string, actual: { columns: string; extra: number }, columns: string, extra: number) => {
			if (actual.columns !== columns || actual.extra !== extra) {
				problems.push(`${label}: index columns [${actual.columns}] and ${actual.extra} extra indexes, expected [${columns}] and ${extra}`);
			}
		};

		// the old DDL: the journal tables with no history index
		await runStatements(db, buildJournalSchema(db.dialect));
		expectState('journal tables made by the old DDL', await state(), '', 0);

		// the ensure step on a fresh provider adds it
		await ensureJournalSchema({ 'shard-j': db }, 'shard-j');
		ensures++;
		expectState('after ensureJournalSchema', await state(), wanted, 1);

		// again, sequentially: no error, no second index, and on MySQL and MariaDB no second CREATE
		tap.log.length = 0;
		for (let i = 0; i < 2; i++) {
			const repeat = await settle(ensureIndex(db, HISTORY_INDEX));
			ensures++;
			if (!repeat.ok) {
				problems.push(`ensureIndex on an existing index: ${describeOutcome(repeat)}`);
			}
		}
		expectState('after two more ensures', await state(), wanted, 1);
		const creates = tap.log.filter((entry) => /\bCREATE INDEX\b/i.test(entry)).length;
		if (db.dialect === 'mysql' && creates !== 0) {
			problems.push(`ensureIndex sent ${creates} CREATE INDEX statements for an index that exists`);
		}

		// four at once over a dropped index, several rounds
		let contested = 0;
		for (let round = 0; round < rounds; round++) {
			await db
				.prepare(db.dialect === 'mysql' ? `DROP INDEX ${HISTORY_INDEX.name} ON ${HISTORY_INDEX.table}` : `DROP INDEX ${HISTORY_INDEX.name}`)
				.run();
			const dropped = await state();
			if (dropped.columns !== '') {
				problems.push(`round ${round}: the index could not be dropped`);
				break;
			}

			tap.log.length = 0;
			const outcomes = await Promise.all(Array.from({ length: 4 }, () => settle(ensureIndex(db, HISTORY_INDEX))));
			ensures += 4;
			const failed = outcomes.filter((outcome) => !outcome.ok);
			if (failed.length > 0) {
				problems.push(`round ${round}: ${failed.length} of 4 concurrent ensures failed: ${describeOutcome(failed[0]!)}`);
			}
			expectState(`round ${round}`, await state(), wanted, 1);
			if (tap.log.filter((entry) => /\bCREATE INDEX\b/i.test(entry)).length > 1) {
				contested++;
			}
		}
		lines.push(`${rounds} rounds of 4 concurrent ensures, ${contested} sent more than one CREATE INDEX`);

		// control: a plain CREATE INDEX over the existing index fails, so a clean repeat above means something
		const columns = HISTORY_INDEX.columns.join(', ');
		const naive = await settle(db.prepare(`CREATE INDEX ${HISTORY_INDEX.name} ON ${HISTORY_INDEX.table} (${columns})`).run());
		lines.push(`plain CREATE INDEX over an existing one: ${naive.ok ? 'no error' : describeDriverError(naive.error)}`);
		if (naive.ok) {
			problems.push('a plain CREATE INDEX over an existing index did not fail, so the repeat checks cannot tell');
		}
		expectState('after the plain repeat', await state(), wanted, 1);
	} catch (error) {
		problems.push(error instanceof Error ? `${error.message}\n${error.stack}` : String(error));
	} finally {
		if (runtime) {
			await safely(async () => runtime?.close());
		}
	}

	return liveResult('history_index_ensure', started, ensures, lines, problems);
}

// #endregion

// #endregion

// #region kv and batch scenarios

const BULK_READ_KEYS = 100;

async function scenarioKvBulkRead(kv: KVStorage, kvFlavor: KVFlavor, iterations: number): Promise<ScenarioStats> {
	const getMany = kv.getMany?.bind(kv);
	if (!getMany || !kv.putMany) {
		return createSkippedScenario('kv_bulk_read', `${kvFlavor} adapter has no getMany`);
	}

	const prefix = `kvbulk:${Date.now()}:`;
	const keys = Array.from({ length: BULK_READ_KEYS }, (_, i) => `${prefix}${i}`);
	const present = keys.filter((_, i) => i % 10 !== 0);

	try {
		await kv.putMany(present.map((key) => ({ key, value: JSON.stringify({ key }) })));

		const verify = async () => {
			const text = await getMany(keys, 'text');
			check(text.length === BULK_READ_KEYS, `getMany returned ${text.length} values`);
			for (const [i, value] of text.entries()) {
				check(value === (i % 10 === 0 ? null : JSON.stringify({ key: keys[i] })), `getMany value ${i} is ${value}`);
			}
			const json = await getMany<{ key: string }>(keys, 'json');
			check(json[1]?.key === keys[1] && json[0] === null, 'getMany json mode lost order or nulls');
		};

		const singles = await Promise.all(keys.map((key) => kv.get(key, 'text')));
		check(
			singles.every((value, i) => value === (i % 10 === 0 ? null : JSON.stringify({ key: keys[i] }))),
			'single gets disagree with the seed'
		);

		const stats = await measureScenario('kv_bulk_read', iterations, async () => {
			await verify();
		});

		const timedSingles: number[] = [];
		for (let i = 0; i < 5; i++) {
			timedSingles.push(await measureLatencySample(async () => void (await Promise.all(keys.map((key) => kv.get(key, 'text'))))));
		}
		const singlesP50 = [...timedSingles].sort((a, b) => a - b)[2]!;
		return { ...stats, notes: `getMany p50 ${formatMs(stats.p50Ms)} against ${formatMs(singlesP50)} for ${BULK_READ_KEYS} parallel gets` };
	} catch (error) {
		return {
			name: 'kv_bulk_read',
			status: 'failed',
			iterations: 0,
			samplesMs: [],
			error: error instanceof Error ? error.message : String(error)
		};
	} finally {
		await safely(async () => {
			for (const key of present) {
				await kv.delete(key);
			}
		});
	}
}

async function scenarioBatchRollback(shards: Record<string, SQLDatabase>, iterations: number): Promise<ScenarioStats> {
	const db = shards['shard-a']!;
	if (!db.runBatch) {
		return createSkippedScenario('batch_rollback', 'this adapter runs a batch one statement at a time, with no transaction to roll back');
	}

	const insert = 'INSERT INTO probe_rollback (id, v) VALUES (?, ?)';
	await db.prepare('CREATE TABLE IF NOT EXISTS probe_rollback (id VARCHAR(191) PRIMARY KEY, v BIGINT)').run();

	try {
		const stats = await measureScenario('batch_rollback', iterations, async (i) => {
			await db.prepare('DELETE FROM probe_rollback').run();

			// the third statement repeats the first key, so it fails after two good ones
			let threw = false;
			try {
				await db.runBatch!([
					{ sql: insert, bindings: [`a${i}`, 1] },
					{ sql: insert, bindings: [`b${i}`, 2] },
					{ sql: insert, bindings: [`a${i}`, 3] }
				]);
			} catch {
				threw = true;
			}
			check(threw, 'a batch with a duplicate key did not fail');

			const left = Number((await db.prepare('SELECT COUNT(*) AS n FROM probe_rollback').first<{ n: unknown }>())?.n);
			// the declaration and the proof must agree, so an adapter cannot claim or hide the capability
			check(left === 0 || db.atomicBatch !== true, `atomicBatch is declared but ${left} rows survived a failed batch`);
			check(left !== 0 || db.atomicBatch === true, 'a failed batch rolled back whole but the adapter does not declare atomicBatch');
		});

		return { ...stats, notes: `atomicBatch ${db.atomicBatch === true ? 'declared and proven' : 'not declared'}` };
	} finally {
		await safely(async () => void (await db.prepare('DROP TABLE IF EXISTS probe_rollback').run()));
	}
}

// #endregion

function cloudflareUrl(path: string, profile: AdapterProfile): string {
	const separator = path.includes('?') ? '&' : '?';
	return `http://127.0.0.1:8787${path}${separator}profile=${encodeURIComponent(profile)}`;
}

async function scenarioCloudflareBasicCrud(iterations: number, profile: AdapterProfile): Promise<ScenarioStats> {
	return measureScenario('basic_crud', iterations, async (i) => {
		const id = `cf-user-${Date.now()}-${i}`;
		const payload = {
			id,
			name: `Cloudflare ${i}`,
			email: `${id}@cloudflare.local`
		};

		await assertOk(
			await fetch(cloudflareUrl('/api/users', profile), {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify(payload)
			}),
			'POST /api/users'
		);

		await assertOk(await fetch(cloudflareUrl(`/api/users?id=${encodeURIComponent(id)}`, profile)), 'GET /api/users');

		await assertOk(
			await fetch(cloudflareUrl('/api/users', profile), {
				method: 'PUT',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					id,
					name: `Cloudflare ${i} Updated`,
					email: `${id}@cloudflare.local`
				})
			}),
			'PUT /api/users'
		);

		await assertOk(
			await fetch(cloudflareUrl(`/api/users?id=${encodeURIComponent(id)}`, profile), { method: 'DELETE' }),
			'DELETE /api/users'
		);
	});
}

async function scenarioCloudflareAdvanced(iterations: number, profile: AdapterProfile): Promise<ScenarioStats> {
	return measureScenario('advanced_usage', iterations, async () => {
		await assertOk(await fetch(cloudflareUrl('/api/stats', profile)), 'GET /api/stats');
		await assertOk(await fetch(cloudflareUrl('/api/shards', profile)), 'GET /api/shards');
	});
}

async function scenarioCloudflareBulk(iterations: number, bulkSize: number, profile: AdapterProfile): Promise<ScenarioStats> {
	return measureScenario('bulk_crud', iterations, async (i) => {
		const ids = new Array(bulkSize).fill(null).map((_, idx) => `cf-bulk-${Date.now()}-${i}-${idx}`);
		for (const id of ids) {
			await assertOk(
				await fetch(cloudflareUrl('/api/users', profile), {
					method: 'POST',
					headers: { 'Content-Type': 'application/json' },
					body: JSON.stringify({ id, name: `Bulk ${id}`, email: `${id}@bulk.cloudflare.local` })
				}),
				'POST /api/users (bulk)'
			);
		}
		await assertOk(await fetch(cloudflareUrl('/api/stats', profile)), 'GET /api/stats');
	});
}

async function scenarioCloudflareAutoIncrement(iterations: number, profile: AdapterProfile): Promise<ScenarioStats> {
	const seen = new Set<number>();

	const stats = await measureScenario('auto_increment', iterations, async (i) => {
		const response = await postCloudflareBenchmark(
			'/api/benchmark/auto-increment',
			{
				prefix: `cf-auto-${Date.now()}-${i}`,
				records: 1
			},
			profile
		);

		const body = (await response.json()) as { success?: boolean; inserted?: number; generatedIds?: number[] };
		if (!body?.success || typeof body.inserted !== 'number' || body.inserted <= 0) {
			throw new Error('Cloudflare auto-increment endpoint did not return an inserted count');
		}

		if (!Array.isArray(body.generatedIds) || body.generatedIds.length !== body.inserted) {
			throw new Error('Cloudflare auto-increment endpoint did not return generated ids');
		}

		for (const id of body.generatedIds) {
			check(!seen.has(id), `nextId handed out id ${id} twice`);
			seen.add(id);
		}
	});
	if (stats.status !== 'passed') {
		return stats;
	}

	// two shards given the same id stand in for two per-shard sequences colliding
	try {
		const response = await postCloudflareBenchmark(
			'/api/benchmark/auto-increment-collision',
			{ prefix: `cf-collision-${Date.now()}` },
			profile
		);
		const arm = (await response.json()) as {
			withoutTable: { code: string | null; orphanRows: number };
			withTable: { code: string | null; orphanRows: number };
			routedName: string | null;
		};

		check(arm.withoutTable.code === 'GENERATED_KEY_COLLISION', `second shard without { table } got code ${arm.withoutTable.code}`);
		check(
			arm.withoutTable.orphanRows === 1,
			`without { table } the refused row should stay on the shard, found ${arm.withoutTable.orphanRows}`
		);
		check(arm.withTable.code === 'GENERATED_KEY_COLLISION', `second shard with { table } got code ${arm.withTable.code}`);
		check(arm.withTable.orphanRows === 0, `with { table } the refused row should be deleted, found ${arm.withTable.orphanRows}`);
		check(arm.routedName === 'first', `the first mapping no longer routes to the first row (read ${arm.routedName})`);

		return {
			...stats,
			notes: `${seen.size} ids from nextId read back through routed reads; a repeated id on a second shard was refused with GENERATED_KEY_COLLISION, left its row without { table } and removed it with { table }, and the first mapping still routes`
		};
	} catch (error) {
		return { ...stats, status: 'failed', error: error instanceof Error ? error.message : String(error) };
	}
}

async function scenarioCloudflareMigration(
	iterations: number,
	recordsPerIteration: number,
	profile: AdapterProfile
): Promise<ScenarioStats> {
	return measureScenario('migration_mapping', iterations, async (i) => {
		const response = await postCloudflareBenchmark(
			'/api/benchmark/migration',
			{
				prefix: `cf-migrate-${Date.now()}-${i}`,
				records: recordsPerIteration
			},
			profile
		);
		const body = (await response.json()) as { success?: boolean; mapped?: number };
		if (!body?.success || typeof body.mapped !== 'number' || body.mapped <= 0) {
			throw new Error('Cloudflare migration endpoint did not return mapped record count');
		}
	});
}

async function scenarioCloudflareIndexing(iterations: number, postsPerIteration: number, profile: AdapterProfile): Promise<ScenarioStats> {
	return measureScenario('indexing', iterations, async (i) => {
		const response = await postCloudflareBenchmark(
			'/api/benchmark/indexing',
			{
				userId: `cf-index-${Date.now()}-${i}`,
				posts: postsPerIteration
			},
			profile
		);
		const body = (await response.json()) as { success?: boolean; rows?: number };
		if (!body?.success || typeof body.rows !== 'number' || body.rows <= 0) {
			throw new Error('Cloudflare indexing endpoint returned no rows');
		}
	});
}

async function scenarioCloudflareMetadata(iterations: number, profile: AdapterProfile): Promise<ScenarioStats> {
	return measureScenario('metadata_fetch', iterations, async () => {
		const response = await fetch(cloudflareUrl('/api/benchmark/metadata', profile));
		await assertOk(response, 'GET /api/benchmark/metadata');
		const body = (await response.json()) as { success?: boolean; rows?: number };
		if (!body?.success || typeof body.rows !== 'number' || body.rows <= 0) {
			throw new Error('Cloudflare metadata endpoint returned no rows');
		}
	});
}

async function scenarioCloudflarePragma(iterations: number, profile: AdapterProfile): Promise<ScenarioStats> {
	return measureScenario('pragma_or_info', iterations, async () => {
		const response = await fetch(cloudflareUrl('/api/benchmark/pragma', profile));
		await assertOk(response, 'GET /api/benchmark/pragma');
		const body = (await response.json()) as { success?: boolean; value?: unknown };
		if (!body?.success || body.value === undefined || body.value === null) {
			throw new Error('Cloudflare pragma/info endpoint did not return a value');
		}
	});
}

async function scenarioCloudflareCounting(iterations: number, seedCount: number, profile: AdapterProfile): Promise<ScenarioStats> {
	const seedResponse = await postCloudflareBenchmark(
		'/api/benchmark/seed-users',
		{
			prefix: `cf-count-seed-${Date.now()}`,
			records: seedCount
		},
		profile
	);
	const seedBody = (await seedResponse.json()) as { success?: boolean; inserted?: number };
	if (!seedBody?.success || typeof seedBody.inserted !== 'number' || seedBody.inserted <= 0) {
		throw new Error('Cloudflare seed-users endpoint did not insert records for counting benchmark');
	}

	return measureScenario('counting', iterations, async () => {
		const response = await fetch(cloudflareUrl('/api/benchmark/counting', profile));
		await assertOk(response, 'GET /api/benchmark/counting');
		const body = (await response.json()) as { success?: boolean; total?: number };
		if (!body?.success || typeof body.total !== 'number' || body.total <= 0) {
			throw new Error('Cloudflare counting endpoint returned invalid total');
		}
	});
}

async function scenarioCloudflareFanout(iterations: number, profile: AdapterProfile): Promise<ScenarioStats> {
	return measureScenario('shard_fanout', iterations, async () => {
		const response = await fetch(cloudflareUrl('/api/benchmark/fanout', profile));
		await assertOk(response, 'GET /api/benchmark/fanout');
		const body = (await response.json()) as { success?: boolean; total?: number };
		if (!body?.success || typeof body.total !== 'number') {
			throw new Error('Cloudflare fanout endpoint returned invalid payload');
		}
	});
}

async function scenarioCloudflareBatchWrite(iterations: number, records: number, profile: AdapterProfile): Promise<ScenarioStats> {
	return measureScenario('batch_write', iterations, async (i) => {
		const response = await postCloudflareBenchmark(
			'/api/benchmark/batch-write',
			{ prefix: `cf-batch-${Date.now()}-${i}`, records },
			profile
		);
		const body = (await response.json()) as { success?: boolean; statements?: number };
		if (!body?.success || body.statements !== records) {
			throw new Error(`Cloudflare batch endpoint wrote ${body?.statements ?? 0} of ${records} statements`);
		}
	});
}

async function scenarioCloudflareReassignment(iterations: number, profile: AdapterProfile): Promise<ScenarioStats> {
	return measureScenario('reassignment', iterations, async (i) => {
		const response = await postCloudflareBenchmark(
			'/api/benchmark/reassignment',
			{
				id: `cf-reassign-${Date.now()}-${i}`
			},
			profile
		);
		const body = (await response.json()) as { success?: boolean; reassignedTo?: string };
		if (!body?.success || !body.reassignedTo) {
			throw new Error('Cloudflare reassignment endpoint did not report target shard');
		}
	});
}

/** Cloudflare lane results are local workerd D1 and KV, which prove correctness and say nothing about production D1 or KV */
async function scenarioCloudflareJournalMove(iterations: number, profile: AdapterProfile): Promise<ScenarioStats> {
	if (profile !== 'native' && profile !== 'hyperdrive') {
		return createSkippedScenario('journal_move', `the ${profile} profile has no transactional batch on every shard`);
	}

	return measureScenario('journal_move', iterations, async (i) => {
		const response = await fetch(cloudflareUrl('/api/benchmark/journal-move', profile), {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ id: `cf-journal-${Date.now()}-${i}` }),
			signal: AbortSignal.timeout(120_000)
		});
		const body = (await response.json()) as { success?: boolean; refusals?: string[]; error?: string };
		if (!response.ok || !body.success) {
			throw new Error(`${profile} journal move on local workerd failed: ${body.error ?? JSON.stringify(body)}`);
		}
	});
}

async function scenarioCloudflareKvBulkRead(profile: AdapterProfile): Promise<ScenarioStats> {
	return measureScenario('kv_bulk_read', 3, async (i) => {
		const response = await postCloudflareBenchmark('/api/benchmark/kv-bulk-read', { prefix: `cf-kv-${Date.now()}-${i}:` }, profile);
		const body = (await response.json()) as { success?: boolean; single?: boolean; chunked?: boolean };
		if (!body.success) {
			throw new Error(`Workers KV getMany returned a wrong value (100 keys: ${body.single}, 250 keys: ${body.chunked})`);
		}
	});
}

async function scenarioCloudflareBatchRollback(profile: AdapterProfile): Promise<ScenarioStats> {
	return measureScenario('batch_rollback', 3, async () => {
		const response = await postCloudflareBenchmark('/api/probe/failing-batch', {}, profile);
		const body = (await response.json()) as {
			success?: boolean;
			raw?: { threw: boolean; rowsAfter: number };
			adapter?: { threw: boolean; rowsAfter: number };
		};
		if (!body.success) {
			throw new Error(`local workerd D1 failing batch: ${JSON.stringify(body)}`);
		}
	});
}

interface AlsProbeReport {
	asyncContextAvailable: boolean;
	hasGetBuiltinModule: string;
	callbacks?: number;
	checkpoints?: number;
	crossTalk?: number;
	rejected?: number;
	configMismatches?: number;
	misplacedRows?: number;
	missingRows?: number;
	rejectedOk?: number;
	rejectedWrong?: number;
	sameConfigOk?: number;
	sameConfigFailed?: number;
	afterOk?: number;
	afterFailed?: number;
	refusedWrites?: number;
	firstError?: string | null;
}

async function postAlsProbe(baseUrl: string, mode: string): Promise<AlsProbeReport> {
	const response = await fetch(`${baseUrl}/api/probe/als`, {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify({ mode, iterations: ALS_PROBE_ITERATIONS }),
		// a hung dev server fails the scenario instead of the whole lane
		signal: AbortSignal.timeout(240_000)
	});
	await assertOk(response, `POST /api/probe/als (${mode})`);
	return (await response.json()) as AlsProbeReport;
}

function alsArmSummary(label: string, report: AlsProbeReport): string {
	return `${label}: ${report.callbacks} callbacks, ${report.checkpoints ?? 0} config reads, ${report.crossTalk} cross-talk`;
}

/** Local workerd with nodejs_compat; says nothing about production Workers */
async function scenarioCloudflareAlsOverlap(profile: AdapterProfile): Promise<ScenarioStats> {
	if (profile !== 'native') {
		return createSkippedScenario(
			'als_overlap',
			'the probe builds its own configurations from the D1 bindings, so the native profile covers it'
		);
	}

	try {
		const base = 'http://127.0.0.1:8787';
		const scoped = await postAlsProbe(base, 'scoped');
		const client = await postAlsProbe(base, 'client');
		const control = await postAlsProbe(base, 'unscoped');

		check(scoped.hasGetBuiltinModule === 'function', `process.getBuiltinModule is ${scoped.hasGetBuiltinModule} under nodejs_compat`);
		check(scoped.asyncContextAvailable, 'asyncContextAvailable() is false under nodejs_compat');
		check(scoped.crossTalk === 0, `collegedb callbacks crossed over: ${JSON.stringify(scoped)}`);
		check(client.crossTalk === 0, `createClient clusters crossed over: ${JSON.stringify(client)}`);
		check(
			(control.crossTalk ?? 0) > 0,
			'the control arm (initialize twice) showed no cross-talk, so this probe cannot detect the defect it checks for'
		);

		return {
			name: 'als_overlap',
			status: 'passed',
			iterations: ALS_PROBE_ITERATIONS,
			samplesMs: [],
			notes: `asyncContextAvailable=true; ${alsArmSummary('scoped', scoped)}; ${alsArmSummary('createClient', client)}; control (initialize twice) ${control.crossTalk} cross-talk in ${control.callbacks} callbacks`
		};
	} catch (error) {
		return {
			name: 'als_overlap',
			status: 'failed',
			iterations: ALS_PROBE_ITERATIONS,
			samplesMs: [],
			error: error instanceof Error ? error.message : String(error)
		};
	}
}

/**
 * Runs the same probe on a second Worker built without nodejs_compat. It has
 * its own entry because the sandbox Worker imports `pg`, which cannot bundle
 * without the flag, and its own state directory so two dev servers never share
 * a SQLite file.
 */
async function scenarioCloudflareAlsNoCompat(profile: AdapterProfile): Promise<ScenarioStats> {
	if (profile !== 'native') {
		return createSkippedScenario(
			'als_no_compat',
			'the probe builds its own configurations from the D1 bindings, so the native profile covers it'
		);
	}

	const proc = Bun.spawn(
		[
			WRANGLER_BIN,
			'dev',
			'--config',
			WRANGLER_NO_COMPAT_CONFIG,
			'--port',
			'8788',
			'--inspector-port',
			'9330',
			'--ip',
			'127.0.0.1',
			'--local',
			'--persist-to',
			resolve(TMP_DIR, 'als-no-compat-state'),
			'--log-level',
			'error'
		],
		{ cwd: PROJECT_ROOT, stdout: 'pipe', stderr: 'pipe' }
	);
	const stdoutPromise = new Response(proc.stdout).text();
	const stderrPromise = new Response(proc.stderr).text();

	try {
		await waitForHttp(`${NO_COMPAT_URL}/health`, 90_000);

		const refusal = await postAlsProbe(NO_COMPAT_URL, 'reject');
		const client = await postAlsProbe(NO_COMPAT_URL, 'client');
		const control = await postAlsProbe(NO_COMPAT_URL, 'unscoped');
		const n = ALS_PROBE_ITERATIONS;

		check(!refusal.asyncContextAvailable, 'asyncContextAvailable() is true without nodejs_compat');
		check(
			refusal.refusedWrites === 0 && refusal.misplacedRows === 0 && refusal.missingRows === 0,
			`refusal arm wrote rows wrongly: ${JSON.stringify(refusal)}`
		);
		check(
			refusal.rejectedOk === n && refusal.rejectedWrong === 0,
			`a different configuration was not refused with CONCURRENT_CONFIG ${refusal.rejectedWrong} of ${n} times: ${refusal.firstError}`
		);
		check(
			refusal.sameConfigOk === n && refusal.sameConfigFailed === 0,
			`the same configuration did not overlap cleanly: ${refusal.firstError}`
		);
		check(
			refusal.afterOk === n && refusal.afterFailed === 0,
			`the claim did not clear after the callbacks finished: ${refusal.firstError}`
		);
		check(client.crossTalk === 0, `createClient clusters crossed over without nodejs_compat: ${JSON.stringify(client)}`);
		check(
			(control.crossTalk ?? 0) > 0,
			'the control arm (initialize twice) showed no cross-talk, so this probe cannot detect the defect it checks for'
		);

		return {
			name: 'als_no_compat',
			status: 'passed',
			iterations: n,
			samplesMs: [],
			notes: `asyncContextAvailable=false; ${n} different-config overlaps refused with CONCURRENT_CONFIG, ${refusal.sameConfigOk} same-config overlaps ran, ${refusal.afterOk} callbacks ran after the claim cleared; ${alsArmSummary('createClient', client)}; control (initialize twice) ${control.crossTalk} cross-talk in ${control.callbacks} callbacks`
		};
	} catch (error) {
		await terminateProcess(proc);
		const stderr = await readCapturedOutput(stderrPromise);
		const stdout = await readCapturedOutput(stdoutPromise);
		return {
			name: 'als_no_compat',
			status: 'failed',
			iterations: ALS_PROBE_ITERATIONS,
			samplesMs: [],
			error: `${error instanceof Error ? error.message : String(error)} ${trimForReport(stderr || stdout, 1500)}`.trim()
		};
	} finally {
		await terminateProcess(proc);
	}
}

async function resetCloudflareBenchmarkData(profile: AdapterProfile): Promise<void> {
	await assertOk(await fetch(cloudflareUrl('/api/benchmark/reset', profile), { method: 'POST' }), 'POST /api/benchmark/reset');
}

async function postCloudflareBenchmark(path: string, payload: Record<string, unknown>, profile: AdapterProfile): Promise<Response> {
	const response = await fetch(cloudflareUrl(path, profile), {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify(payload),
		// a hung dev server fails the scenario instead of the whole lane
		signal: AbortSignal.timeout(120_000)
	});
	await assertOk(response, `POST ${path}`);
	return response;
}

/**
 * Host ports for the compose services.
 *
 * Overridable because these are published on the host, and a machine that
 * already runs a Redis or a Postgres on the default port cannot otherwise start
 * the sandbox at all. The compose file reads the same variables, so both sides
 * move together.
 */
const PORTS = {
	postgres: Number(process.env.COLLEGEDB_POSTGRES_PORT ?? 5432),
	mysql: Number(process.env.COLLEGEDB_MYSQL_PORT ?? 3306),
	mariadb: Number(process.env.COLLEGEDB_MARIADB_PORT ?? 3307),
	redis: Number(process.env.COLLEGEDB_REDIS_PORT ?? 6379),
	valkey: Number(process.env.COLLEGEDB_VALKEY_PORT ?? 6380)
} as const;

/** Session isolation level for the Postgres and MySQL pools, for example `READ COMMITTED`; unset keeps the server default */
let sandboxIsolation = /^[A-Z ]+$/.test(process.env.COLLEGEDB_SANDBOX_ISOLATION ?? '')
	? process.env.COLLEGEDB_SANDBOX_ISOLATION
	: undefined;

async function createKVRuntime(kvFlavor: KVFlavor, profile: AdapterProfile): Promise<KVRuntime> {
	const url = kvFlavor === 'valkey' ? `redis://127.0.0.1:${PORTS.valkey}` : `redis://127.0.0.1:${PORTS.redis}`;
	await waitForRedis(url);

	const client: any = createRedisClient({
		url,
		socket: {
			connectTimeout: 10_000
		}
	});
	client.on('error', () => {
		// Keep benchmark output focused; connection retries are handled elsewhere.
	});
	await client.connect();

	const kv = kvFlavor === 'redis' ? createRedisKVProvider(client) : createValkeyKVProvider(client);
	return {
		kv,
		close: async () => {
			try {
				await client.quit();
			} catch {
				try {
					client.disconnect();
				} catch {
					// ignore close errors
				}
			}
		}
	};
}

async function createSQLRuntime(
	dbFlavor: DatabaseFlavor,
	runId: string,
	profile: AdapterProfile,
	shardBindings: string[] = ['shard-a', 'shard-b']
): Promise<SQLRuntime> {
	switch (dbFlavor) {
		case 'postgres':
			return createPostgresRuntime(runId, profile, shardBindings);
		case 'mysql':
			return createMySQLRuntime(runId, PORTS.mysql, profile, shardBindings);
		case 'mariadb':
			return createMySQLRuntime(runId, PORTS.mariadb, profile, shardBindings);
		case 'sqlite':
			return createSQLiteRuntime(runId, profile, shardBindings);
	}
}

/** A Hyperdrive provider keeps its client open until disposed, and a dropped database kills that connection under it */
async function disposeHyperdriveShards(shards: Record<string, SQLDatabase>): Promise<void> {
	for (const shard of Object.values(shards)) {
		await (shard as { dispose?: () => Promise<void> }).dispose?.();
	}
}

async function createPostgresRuntime(runId: string, profile: AdapterProfile, shardBindings: string[]): Promise<SQLRuntime> {
	await waitForPostgres();

	const dbNamesByBinding = new Map<string, string>();
	for (const binding of shardBindings) {
		dbNamesByBinding.set(binding, sanitizeDatabaseName(`collegedb_${binding}_${runId}`));
	}

	const admin = new PostgresClient({
		host: '127.0.0.1',
		port: PORTS.postgres,
		user: 'collegedb',
		password: 'collegedb',
		database: 'postgres'
	});
	await admin.connect();
	for (const dbName of dbNamesByBinding.values()) {
		await dropPostgresDatabase(admin, dbName);
	}
	for (const dbName of dbNamesByBinding.values()) {
		await admin.query(`CREATE DATABASE "${escapePgIdentifier(dbName)}"`);
	}
	await admin.end();

	const poolsByBinding = new Map<string, PostgresPool>();
	for (const [binding, dbName] of dbNamesByBinding) {
		const pool = new PostgresPool({
			host: '127.0.0.1',
			port: PORTS.postgres,
			user: 'collegedb',
			password: 'collegedb',
			database: dbName,
			max: 10
		});
		const isolation = sandboxIsolation;
		if (isolation) {
			pool.on('connect', (client) => void client.query(`SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ${isolation}`));
		}
		await pool.query('SELECT 1');
		poolsByBinding.set(binding, pool);
	}

	const createPostgresProviderForProfile = (pool: PostgresPool, connectionString: string, currentProfile: AdapterProfile): SQLDatabase => {
		switch (currentProfile) {
			// 'hyperdrive' never reaches here; it only runs in the Cloudflare lane.
			default:
			case 'native':
				return createPostgreSQLProvider(pool);
			case 'drizzle': {
				const drizzleDb = drizzlePostgres(pool);
				return createPostgreSQLProvider(drizzleDb, drizzleSql);
			}
			case 'per-statement-connection':
				return createHyperdrivePostgresProvider({ connectionString }, (conn) => {
					const client = new PostgresClient({ connectionString: conn });
					return {
						connect: async () => {
							await client.connect();
						},
						query: async <T = Record<string, unknown>>(sql: string, bindings: any[] = []) => {
							const result = await client.query(sql, bindings);
							return {
								rows: result.rows as T[],
								rowCount: result.rowCount,
								command: result.command
							};
						},
						end: async () => {
							await client.end();
						}
					};
				});
		}
	};

	const shards: Record<string, SQLDatabase> = {};
	for (const [binding, pool] of poolsByBinding) {
		const dbName = dbNamesByBinding.get(binding);
		if (!dbName) {
			continue;
		}
		const connectionString = `postgres://collegedb:collegedb@127.0.0.1:${PORTS.postgres}/${dbName}`;
		shards[binding] = createPostgresProviderForProfile(pool, connectionString, profile);
	}

	const adminOn = async (database: string): Promise<PostgresClient> => {
		const client = new PostgresClient({ host: '127.0.0.1', port: PORTS.postgres, user: 'collegedb', password: 'collegedb', database });
		await client.connect();
		return client;
	};
	const restrictedLogins: Array<{ role: string; database: string; pool: PostgresPool; provider: SQLDatabase }> = [];

	return {
		shards,
		restricted: async (binding, readableTables) => {
			const database = dbNamesByBinding.get(binding)!;
			const role = `cdb_ro_${Math.floor(Math.random() * 1e9)}`;
			const admin = await adminOn(database);
			try {
				await admin.query(`CREATE ROLE "${role}" LOGIN PASSWORD 'ro'`);
				await admin.query(`GRANT CONNECT ON DATABASE "${escapePgIdentifier(database)}" TO "${role}"`);
				await admin.query(`GRANT USAGE ON SCHEMA public TO "${role}"`);
				for (const table of readableTables) {
					await admin.query(`GRANT SELECT ON "${escapePgIdentifier(table)}" TO "${role}"`);
				}
			} finally {
				await admin.end();
			}

			const pool = new PostgresPool({ host: '127.0.0.1', port: PORTS.postgres, user: role, password: 'ro', database, max: 4 });
			await pool.query('SELECT 1');
			const provider = createPostgresProviderForProfile(pool, `postgres://${role}:ro@127.0.0.1:${PORTS.postgres}/${database}`, profile);
			restrictedLogins.push({ role, database, pool, provider });
			return provider;
		},
		close: async () => {
			await disposeHyperdriveShards(Object.fromEntries(restrictedLogins.map((login, i) => [String(i), login.provider])));
			for (const login of restrictedLogins) {
				await login.pool.end();
				const admin = await adminOn(login.database);
				await admin.query(`DROP OWNED BY "${login.role}"`);
				await admin.end();
				const cluster = await adminOn('postgres');
				await cluster.query(`DROP ROLE IF EXISTS "${login.role}"`);
				await cluster.end();
			}

			await disposeHyperdriveShards(shards);
			for (const pool of poolsByBinding.values()) {
				await pool.end();
			}

			const closeAdmin = new PostgresClient({
				host: '127.0.0.1',
				port: PORTS.postgres,
				user: 'collegedb',
				password: 'collegedb',
				database: 'postgres'
			});
			await closeAdmin.connect();
			for (const dbName of dbNamesByBinding.values()) {
				await dropPostgresDatabase(closeAdmin, dbName);
			}
			await closeAdmin.end();
		}
	};
}

async function createMySQLRuntime(runId: string, port: number, profile: AdapterProfile, shardBindings: string[]): Promise<SQLRuntime> {
	await waitForMySQL(port);

	const dbNamesByBinding = new Map<string, string>();
	for (const binding of shardBindings) {
		dbNamesByBinding.set(binding, sanitizeDatabaseName(`collegedb_${binding}_${runId}`));
	}

	const admin = await mysql.createConnection({
		host: '127.0.0.1',
		port,
		user: 'root',
		password: 'root'
	});
	for (const dbName of dbNamesByBinding.values()) {
		await admin.execute(`DROP DATABASE IF EXISTS \`${dbName}\``);
	}
	for (const dbName of dbNamesByBinding.values()) {
		await admin.execute(`CREATE DATABASE \`${dbName}\``);
	}
	await admin.end();

	const poolsByBinding = new Map<string, mysql.Pool>();
	for (const [binding, dbName] of dbNamesByBinding) {
		const pool = mysql.createPool({
			host: '127.0.0.1',
			port,
			user: 'root',
			password: 'root',
			database: dbName,
			connectionLimit: 10
		});
		const isolation = sandboxIsolation;
		if (isolation) {
			pool.on('connection', (connection) => void connection.query(`SET SESSION TRANSACTION ISOLATION LEVEL ${isolation}`));
		}
		await pool.query('SELECT 1');
		poolsByBinding.set(binding, pool);
	}

	const createMySQLProviderForProfile = (pool: mysql.Pool, connectionString: string, currentProfile: AdapterProfile): SQLDatabase => {
		switch (currentProfile) {
			// 'hyperdrive' never reaches here; it only runs in the Cloudflare lane.
			default:
			case 'native':
				return createMySQLProvider(pool);
			case 'drizzle': {
				const drizzleDb = drizzleMySQL(pool);
				return createMySQLProvider(drizzleDb, drizzleSql);
			}
			case 'per-statement-connection':
				return createHyperdriveMySQLProvider({ connectionString }, (conn) => {
					// one connection per client, opened on first use, which is what a Hyperdrive client is
					let connection: Promise<mysql.Connection> | null = null;
					const open = () => (connection ??= mysql.createConnection(conn));
					return {
						execute: async (sql: string, bindings: any[] = []) => await (await open()).execute(sql, bindings),
						query: async (sql: string, bindings: any[] = []) => await (await open()).query(sql, bindings),
						end: async () => {
							if (connection) {
								await (await connection).end();
							}
						}
					};
				});
		}
	};

	const shards: Record<string, SQLDatabase> = {};
	for (const [binding, pool] of poolsByBinding) {
		const dbName = dbNamesByBinding.get(binding);
		if (!dbName) {
			continue;
		}
		const connectionString = `mysql://root:root@127.0.0.1:${port}/${dbName}`;
		shards[binding] = createMySQLProviderForProfile(pool, connectionString, profile);
	}

	const rootConnection = () => mysql.createConnection({ host: '127.0.0.1', port, user: 'root', password: 'root' });
	const restrictedLogins: Array<{ user: string; pool: mysql.Pool; provider: SQLDatabase }> = [];

	return {
		shards,
		restricted: async (binding, readableTables) => {
			const dbName = dbNamesByBinding.get(binding)!;
			const user = `cdb_ro_${Math.floor(Math.random() * 1e9)}`;
			const admin = await rootConnection();
			try {
				await admin.execute(`CREATE USER '${user}'@'%' IDENTIFIED BY 'ro'`);
				// a login needs a privilege somewhere in the database to connect to it, so it gets a table of its own
				await admin.execute(`CREATE TABLE IF NOT EXISTS \`${dbName}\`.cdb_ro_anchor (id INT)`);
				for (const table of ['cdb_ro_anchor', ...readableTables]) {
					await admin.execute(`GRANT SELECT ON \`${dbName}\`.\`${table}\` TO '${user}'@'%'`);
				}
			} finally {
				await admin.end();
			}

			const pool = mysql.createPool({ host: '127.0.0.1', port, user, password: 'ro', database: dbName, connectionLimit: 4 });
			await pool.query('SELECT 1');
			const provider = createMySQLProviderForProfile(pool, `mysql://${user}:ro@127.0.0.1:${port}/${dbName}`, profile);
			restrictedLogins.push({ user, pool, provider });
			return provider;
		},
		close: async () => {
			await disposeHyperdriveShards(Object.fromEntries(restrictedLogins.map((login, i) => [String(i), login.provider])));
			for (const login of restrictedLogins) {
				await login.pool.end();
				const admin = await rootConnection();
				await admin.execute(`DROP USER IF EXISTS '${login.user}'@'%'`);
				await admin.end();
			}

			await disposeHyperdriveShards(shards);
			for (const pool of poolsByBinding.values()) {
				await pool.end();
			}

			const closeAdmin = await mysql.createConnection({
				host: '127.0.0.1',
				port,
				user: 'root',
				password: 'root'
			});
			for (const dbName of dbNamesByBinding.values()) {
				await closeAdmin.execute(`DROP DATABASE IF EXISTS \`${dbName}\``);
			}
			await closeAdmin.end();
		}
	};
}

async function createSQLiteRuntime(runId: string, profile: AdapterProfile, shardBindings: string[]): Promise<SQLRuntime> {
	const shardFiles = shardBindings.map((binding, index) => ({
		binding,
		file: resolve(TMP_DIR, `${runId}-${index}-${sanitizeDatabaseName(binding)}.sqlite`)
	}));

	const dbByBinding = new Map<string, Database>();
	for (const shardFile of shardFiles) {
		dbByBinding.set(shardFile.binding, new Database(shardFile.file, { create: true }));
	}

	const useDrizzle = profile === 'drizzle';
	const shards: Record<string, SQLDatabase> = {};

	for (const binding of shardBindings) {
		const db = dbByBinding.get(binding);
		if (!db) {
			continue;
		}

		// A SQLite handle is one connection by definition, which is what lets a
		// batch group run inside a single transaction.
		shards[binding] = useDrizzle
			? createSQLiteProvider(drizzleBunSQLite({ client: db }), drizzleSql)
			: createSQLiteProvider(db, { singleConnection: true });
	}

	return {
		shards,
		close: async () => {
			for (const db of dbByBinding.values()) {
				db.close();
			}
			for (const shardFile of shardFiles) {
				await rm(shardFile.file, { force: true });
			}
		}
	};
}

function buildIterationPlan(baseIterations: number): IterationPlan {
	return {
		basic: baseIterations,
		advanced: Math.max(6, Math.floor(baseIterations * 0.75)),
		migration: Math.max(4, Math.floor(baseIterations * 0.5)),
		bulk: Math.max(3, Math.floor(baseIterations * 0.35)),
		autoIncrement: Math.max(4, Math.floor(baseIterations * 0.45)),
		indexing: Math.max(6, Math.floor(baseIterations * 0.75)),
		// Sampled harder than the rest: one iteration of these costs well under a
		// millisecond, so 14 samples put the reported mean within range of a single
		// scheduler hiccup while 30 samples cost 15 ms in total.
		metadata: Math.max(20, Math.floor(baseIterations * 1.5)),
		pragma: Math.max(20, Math.floor(baseIterations * 1.5)),
		counting: Math.max(20, Math.floor(baseIterations * 1.5)),
		fanout: Math.max(20, Math.floor(baseIterations * 1.5)),
		reassignment: Math.max(4, Math.floor(baseIterations * 0.5))
	};
}

function metadataQueryFor(dbFlavor: DatabaseFlavor): string {
	switch (dbFlavor) {
		case 'postgres':
			return "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name LIMIT 25";
		case 'mysql':
		case 'mariadb':
			return 'SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() ORDER BY table_name LIMIT 25';
		case 'sqlite':
			return "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name LIMIT 25";
	}
}

function pragmaOrInfoQueryFor(dbFlavor: DatabaseFlavor): string {
	switch (dbFlavor) {
		case 'postgres':
			return 'SELECT current_database() AS current_database, version() AS version';
		case 'mysql':
		case 'mariadb':
			return 'SELECT DATABASE() AS current_database, VERSION() AS version';
		case 'sqlite':
			return 'PRAGMA page_count';
	}
}

async function measureLatencySample(task: () => Promise<void>): Promise<number> {
	const started = performance.now();
	await task();
	return performance.now() - started;
}

async function runInBatches<T>(items: T[], batchSize: number, task: (item: T, index: number) => Promise<void>): Promise<void> {
	for (let i = 0; i < items.length; i += batchSize) {
		const batch = items.slice(i, i + batchSize);
		await Promise.all(batch.map((item, index) => task(item, i + index)));
	}
}

async function totalUserCount(): Promise<number> {
	const shardResults = await allAllShards<Record<string, unknown>>('SELECT COUNT(*) AS count FROM users');
	let total = 0;

	for (const shardResult of shardResults) {
		for (const row of shardResult.results) {
			total += extractCount(row);
		}
	}

	return total;
}

function extractCount(row: Record<string, unknown>): number {
	const keys = ['count', 'COUNT(*)', 'count(*)', 'COUNT'];
	for (const key of keys) {
		const value = row[key];
		if (value !== undefined && value !== null) {
			const parsed = Number(value);
			if (Number.isFinite(parsed)) {
				return parsed;
			}
		}
	}
	return 0;
}

async function resetBenchData(): Promise<void> {
	await runAllShards('DELETE FROM posts');
	await runAllShards('DELETE FROM users');
	await flush();
}

async function measureScenario(
	name: ScenarioName,
	iterations: number,
	workload: (iteration: number) => Promise<void>
): Promise<ScenarioStats> {
	const samplesMs: number[] = [];

	try {
		// One discarded pass first. Without it the first sample carries JIT
		// compilation, the driver's first prepare and a cold connection, and on the
		// sub-millisecond scenarios that single sample moved the reported mean by
		// double digits: metadata_fetch reported avg=0.63 ms against p50=0.47 ms
		// off one 2.45 ms first sample in a set of 14.
		// Indexed past the measured range so a scenario that derives primary keys
		// from the iteration number cannot collide with its own warmup.
		await workload(iterations);

		for (let i = 0; i < iterations; i++) {
			const started = performance.now();
			await workload(i);
			samplesMs.push(performance.now() - started);
		}

		return {
			name,
			status: 'passed',
			iterations,
			samplesMs,
			...calculateLatencyStats(samplesMs)
		};
	} catch (error) {
		return {
			name,
			status: 'failed',
			iterations,
			samplesMs,
			...calculateLatencyStats(samplesMs),
			error: error instanceof Error ? error.message : String(error)
		};
	}
}

function calculateLatencyStats(samplesMs: number[]): Pick<ScenarioStats, 'avgMs' | 'p50Ms' | 'p95Ms' | 'minMs' | 'maxMs'> {
	if (samplesMs.length === 0) {
		return {};
	}

	const sorted = [...samplesMs].sort((a, b) => a - b);
	const avgMs = sorted.reduce((sum, value) => sum + value, 0) / sorted.length;
	const p50Ms = percentile(sorted, 0.5);
	const p95Ms = percentile(sorted, 0.95);
	const minMs = sorted[0];
	const maxMs = sorted[sorted.length - 1];

	return {
		avgMs,
		p50Ms,
		p95Ms,
		minMs,
		maxMs
	};
}

function percentile(sorted: number[], ratio: number): number {
	const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * ratio) - 1));
	return sorted[index] ?? sorted[sorted.length - 1] ?? 0;
}

function composeServicesForCombo(combo: Combo): string[] {
	const services = new Set<string>();
	if (combo.kv === 'redis') {
		services.add('redis');
	}
	if (combo.kv === 'valkey') {
		services.add('valkey');
	}
	switch (combo.db) {
		case 'postgres':
			services.add('postgres');
			break;
		case 'mysql':
			services.add('mysql');
			break;
		case 'mariadb':
			services.add('mariadb');
			break;
		case 'sqlite':
			break;
	}

	return [...services];
}

async function composeUp(services: string[]): Promise<void> {
	if (services.length === 0) {
		return;
	}
	await runCommand(['bash', COMPOSE_SCRIPT, 'up', ...services]);
}

async function composeDown(): Promise<void> {
	await runCommand(['bash', COMPOSE_SCRIPT, 'down'], PROJECT_ROOT, true);
}

async function runCommand(args: string[], cwd: string = PROJECT_ROOT, allowFailure: boolean = false): Promise<CommandResult> {
	const proc = Bun.spawn(args, {
		cwd,
		stdout: 'pipe',
		stderr: 'pipe'
	});

	const [stdout, stderr, code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited]);
	if (code !== 0 && !allowFailure) {
		throw new Error(`Command failed (${args.join(' ')}):\n${stderr || stdout}`);
	}

	return {
		code,
		stdout,
		stderr
	};
}

async function terminateProcess(proc: Bun.Subprocess): Promise<void> {
	if (proc.exitCode !== null) {
		await proc.exited;
		return;
	}

	proc.kill();
	const result = await Promise.race([proc.exited.then(() => 'exited' as const), sleep(3000).then(() => 'timeout' as const)]);
	if (result === 'timeout') {
		proc.kill(9);
		await proc.exited;
	}
}

/**
 * Reads output captured from a subprocess, giving up rather than blocking.
 *
 * A backstop for the case the pipe stays open anyway, because a stuck read here
 * costs the whole run and the output is only ever used to explain a failure.
 */
async function readCapturedOutput(captured: Promise<string>, timeoutMs: number = 5_000): Promise<string> {
	let timer: ReturnType<typeof setTimeout> | undefined;

	try {
		return await Promise.race([
			captured,
			new Promise<string>((resolve) => {
				timer = setTimeout(() => resolve('<output unavailable: stream did not close>'), timeoutMs);
			})
		]);
	} finally {
		if (timer) {
			clearTimeout(timer);
		}
	}
}

async function retry<T>(task: () => Promise<T>, attempts: number, delayMs: number, label: string): Promise<T> {
	let lastError: unknown;

	for (let i = 0; i < attempts; i++) {
		try {
			return await task();
		} catch (error) {
			lastError = error;
			if (i < attempts - 1) {
				await sleep(delayMs);
			}
		}
	}

	throw new Error(`${label} did not become ready: ${lastError instanceof Error ? lastError.message : String(lastError)}`);
}

async function waitForRedis(url: string): Promise<void> {
	await retry(
		async () => {
			const client: any = createRedisClient({ url });
			client.on('error', () => {
				// wait loop handles retries
			});
			await client.connect();
			await client.ping();
			await client.quit();
		},
		45,
		1000,
		`Redis/Valkey ${url}`
	);
}

async function waitForPostgres(): Promise<void> {
	await retry(
		async () => {
			const client = new PostgresClient({
				host: '127.0.0.1',
				port: PORTS.postgres,
				user: 'collegedb',
				password: 'collegedb',
				database: 'postgres'
			});
			await client.connect();
			await client.query('SELECT 1');
			await client.end();
		},
		60,
		1000,
		`PostgreSQL on ${PORTS.postgres}`
	);
}

async function waitForMySQL(port: number): Promise<void> {
	await retry(
		async () => {
			const conn = await mysql.createConnection({
				host: '127.0.0.1',
				port,
				user: 'root',
				password: 'root'
			});
			await conn.query('SELECT 1');
			await conn.end();
		},
		120,
		1000,
		`MySQL-compatible server on ${port}`
	);
}

async function waitForHttp(url: string, timeoutMs: number, perAttemptMs: number = 5_000): Promise<void> {
	const started = Date.now();
	let lastError: unknown;
	while (Date.now() - started < timeoutMs) {
		try {
			// Each attempt needs its own deadline. Without one, a server that
			// accepts the connection and never answers leaves this fetch pending
			// forever, so the outer deadline is never reached and the lane hangs
			// instead of failing. A failed Worker build does exactly that.
			const response = await fetch(url, { signal: AbortSignal.timeout(perAttemptMs) });
			if (response.ok) {
				return;
			}
			lastError = new Error(`HTTP ${response.status}`);
		} catch (error) {
			lastError = error;
		}
		await sleep(500);
	}

	throw new Error(`Timed out waiting for ${url}: ${lastError instanceof Error ? lastError.message : String(lastError)}`);
}

async function assertOk(response: Response, label: string): Promise<void> {
	if (response.ok) {
		return;
	}
	const body = await response.text();
	throw new Error(`${label} failed with ${response.status}: ${trimForReport(body, 500)}`);
}

function trimForReport(input: string, maxLength: number): string {
	const singleLine = input.replace(/\s+/g, ' ').trim();
	if (singleLine.length <= maxLength) {
		return singleLine;
	}
	return `${singleLine.slice(0, maxLength)}...`;
}

async function dropPostgresDatabase(admin: PostgresClient, dbName: string): Promise<void> {
	await admin.query('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1', [dbName]);
	await admin.query(`DROP DATABASE IF EXISTS "${escapePgIdentifier(dbName)}"`);
}

function escapePgIdentifier(identifier: string): string {
	return identifier.replaceAll('"', '""');
}

function sanitizeDatabaseName(name: string): string {
	return name
		.toLowerCase()
		.replace(/[^a-z0-9_]/g, '_')
		.slice(0, 63);
}

function createRunId(prefix: string): string {
	const safePrefix = prefix.replace(/[^a-z0-9_+]/gi, '_').replaceAll('+', '_');
	return `${safePrefix}_${Date.now()}_${Math.floor(Math.random() * 10_000)}`;
}

function createSkippedScenarioMap(notes: string): Record<ScenarioName, ScenarioStats> {
	const entries = SCENARIO_NAMES.map((name) => [name, createSkippedScenario(name, notes)] as const);
	return Object.fromEntries(entries) as Record<ScenarioName, ScenarioStats>;
}

function createSkippedScenario(name: ScenarioName, notes: string): ScenarioStats {
	return {
		name,
		status: 'skipped',
		iterations: 0,
		samplesMs: [],
		notes
	};
}

/**
 * Routed operations each scenario performs per iteration, used to normalize its
 * average into a per-operation figure.
 *
 * `Overall Avg` is an unweighted mean of the scenario averages, so `bulk_crud`
 * at 400 routed operations per iteration dominates `basic_crud` at 4 and the
 * headline number ranks backends by bulk throughput while calling itself
 * latency. Per-op normalization is comparable across scenarios.
 */
const SCENARIO_OPS_PER_ITERATION: Record<ScenarioName, (bulkSize: number) => number> = {
	basic_crud: () => 4,
	mapping_miss: () => 1,
	next_id: () => 1,
	paginate: () => 1,
	batch_write: (bulkSize) => bulkSize,
	cold_mapping_cache: () => 1,
	concurrent_load: () => 16,
	advanced_usage: () => 5,
	migration_mapping: () => 20,
	bulk_crud: (bulkSize) => bulkSize * 2 + Math.floor(bulkSize / 2),
	auto_increment: () => 2,
	indexing: () => 1,
	metadata_fetch: () => 1,
	pragma_or_info: () => 1,
	counting: () => 1,
	shard_fanout: () => 1,
	reassignment: () => 3,
	journal_move: () => 6,
	stale_route_write: () => 2,
	kv_bulk_read: () => 100,
	batch_rollback: () => 3,
	topology_cas: () => 2,
	missing_sidecar_lookup: () => 6,
	reconcile_record_probe: () => 6,
	recovery_lease_takeover: () => 8,
	history_index_ensure: () => 4,
	als_overlap: () => 1,
	als_no_compat: () => 1
};

/** Scenarios that check correctness; their latency stays out of the averages */
const CORRECTNESS_SCENARIOS: ReadonlySet<ScenarioName> = new Set<ScenarioName>([
	'journal_move',
	'stale_route_write',
	'batch_rollback',
	'topology_cas',
	'missing_sidecar_lookup',
	'reconcile_record_probe',
	'recovery_lease_takeover',
	'history_index_ensure',
	'als_overlap',
	'als_no_compat'
]);

/**
 * Mean latency per routed operation across every scenario that passed.
 */
function computePerOpAverage(scenarios: Record<ScenarioName, ScenarioStats>, bulkSize: number): number | undefined {
	const values: number[] = [];

	for (const name of SCENARIO_NAMES) {
		const scenario = scenarios[name];
		if (CORRECTNESS_SCENARIOS.has(name) || scenario.status !== 'passed' || scenario.avgMs === undefined) {
			continue;
		}

		const ops = Math.max(1, SCENARIO_OPS_PER_ITERATION[name](bulkSize));
		values.push(scenario.avgMs / ops);
	}

	if (values.length === 0) {
		return undefined;
	}

	return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function computeOverallAverage(scenarios: Record<ScenarioName, ScenarioStats>): number | undefined {
	const passing = Object.values(scenarios).filter(
		(scenario) => !CORRECTNESS_SCENARIOS.has(scenario.name) && scenario.status === 'passed' && typeof scenario.avgMs === 'number'
	);
	if (passing.length === 0) {
		return undefined;
	}
	const total = passing.reduce((sum, scenario) => sum + (scenario.avgMs ?? 0), 0);
	return total / passing.length;
}

function formatMs(value: number | undefined): string {
	if (value === undefined || Number.isNaN(value)) {
		return 'n/a';
	}
	return `${value.toFixed(2)} ms`;
}

function formatBytes(value: number | undefined): string {
	if (value === undefined || Number.isNaN(value)) {
		return 'n/a';
	}

	const units = ['B', 'KB', 'MB', 'GB', 'TB'] as const;
	let size = value;
	let unitIndex = 0;

	while (size >= 1024 && unitIndex < units.length - 1) {
		size /= 1024;
		unitIndex += 1;
	}

	return `${size.toFixed(2)} ${units[unitIndex]}`;
}

function emphasizeRowExtremes(cells: RowMetricCell[]): string[] {
	const out = cells.map((cell) => cell.text);
	const numeric = cells
		.map((cell, index) => ({ index, metric: cell.metric }))
		.filter((entry): entry is { index: number; metric: number } => typeof entry.metric === 'number' && Number.isFinite(entry.metric));

	if (numeric.length === 0) {
		return out;
	}

	let fastest = numeric[0]!;
	let slowest = numeric[0]!;

	for (const entry of numeric.slice(1)) {
		if (entry.metric < fastest.metric) {
			fastest = entry;
		}
		if (entry.metric > slowest.metric) {
			slowest = entry;
		}
	}

	out[fastest.index] = `**${out[fastest.index]}**`;
	if (slowest.index !== fastest.index) {
		out[slowest.index] = `*${out[slowest.index]}*`;
	}

	return out;
}

function strategyCellMetric(cell: StrategyBenchmarkCell | undefined): number | undefined {
	if (!cell || cell.status !== 'passed') {
		return undefined;
	}
	return cell.avgMs;
}

function strategyAggregateExtremes(strategyBenchmark?: StrategyBenchmarkResult): { best?: StrategyAggregate; worst?: StrategyAggregate } {
	if (!strategyBenchmark) {
		return {};
	}

	const aggregates: StrategyAggregate[] = [];

	for (const column of strategyBenchmark.columns) {
		const passing = strategyBenchmark.rows
			.map((row) => row.columns[column.key])
			.filter((cell): cell is StrategyBenchmarkCell => !!cell && cell.status === 'passed' && typeof cell.avgMs === 'number');

		if (passing.length === 0) {
			continue;
		}

		const avgMs = passing.reduce((sum, cell) => sum + (cell.avgMs ?? 0), 0) / passing.length;
		const p95Ms =
			passing.reduce((sum, cell) => sum + (typeof cell.p95Ms === 'number' ? cell.p95Ms : (cell.avgMs ?? 0)), 0) / passing.length;

		aggregates.push({
			label: column.label,
			avgMs,
			p95Ms
		});
	}

	if (aggregates.length === 0) {
		return {};
	}

	let best = aggregates[0]!;
	let worst = aggregates[0]!;

	for (const aggregate of aggregates.slice(1)) {
		if (aggregate.avgMs < best.avgMs) {
			best = aggregate;
		}
		if (aggregate.avgMs > worst.avgMs) {
			worst = aggregate;
		}
	}

	return { best, worst };
}

function formatStrategyAggregate(summary: StrategyAggregate | undefined): string {
	if (!summary) {
		return 'N/A';
	}
	return `${summary.label} (${formatMs(summary.avgMs)} / ${formatMs(summary.p95Ms)})`;
}

function scenarioCell(scenario: ScenarioStats): string {
	if (scenario.status === 'failed') {
		return 'FAILED';
	}
	if (scenario.status === 'skipped') {
		return 'N/A';
	}
	return `${formatMs(scenario.avgMs)} / ${formatMs(scenario.p95Ms)}`;
}

function strategyBenchmarkCell(cell: StrategyBenchmarkCell | undefined): string {
	if (!cell) {
		return 'N/A';
	}
	if (cell.status === 'failed') {
		return 'FAILED';
	}
	if (cell.status === 'skipped') {
		return 'N/A';
	}

	if (cell.maxShare === undefined || cell.chiSquare === undefined) {
		return 'n/a';
	}

	return `${(cell.maxShare * 100).toFixed(0)}% / ${cell.chiSquare.toFixed(1)}`;
}

/**
 * Chi-square against uniform, used to rank strategy columns. Lower is more even,
 * and unlike a busiest-to-quietest ratio it stays finite when a shard is empty.
 */
function strategyDistributionMetric(cell: StrategyBenchmarkCell | undefined): number | undefined {
	if (!cell || cell.status !== 'passed' || cell.chiSquare === undefined || !Number.isFinite(cell.chiSquare)) {
		return undefined;
	}
	return cell.chiSquare;
}

function scenarioMetric(scenario: ScenarioStats): number | undefined {
	if (scenario.status !== 'passed') {
		return undefined;
	}
	return scenario.avgMs;
}

function countScenarioStates(scenarios: Record<ScenarioName, ScenarioStats>): { passed: number; failed: number; skipped: number } {
	let passed = 0;
	let failed = 0;
	let skipped = 0;

	for (const scenario of Object.values(scenarios)) {
		if (scenario.status === 'passed') {
			passed++;
		} else if (scenario.status === 'failed') {
			failed++;
		} else {
			skipped++;
		}
	}

	return { passed, failed, skipped };
}

function scenarioLabel(name: ScenarioName): string {
	return SCENARIO_CATALOG[name]?.title ?? name;
}

function buildMarkdownReport(options: CLIOptions, comboResults: ComboResult[], cloudflareResults: ComboResult[]): string {
	const generatedAt = new Date().toISOString();
	const lines: string[] = [];
	const catalogPlan = buildIterationPlan(options.iterations);
	const groupedByBase = new Map<string, Map<AdapterProfile, ComboResult>>();
	const strategyResults = comboResults.filter((result) => result.strategyBenchmark);
	const cloudflareStrategyResults = cloudflareResults.filter((result) => result.strategyBenchmark);

	for (const result of comboResults) {
		const byProfile = groupedByBase.get(result.baseId) ?? new Map<AdapterProfile, ComboResult>();
		byProfile.set(result.profile, result);
		groupedByBase.set(result.baseId, byProfile);
	}

	const groupedCloudflare = new Map<AdapterProfile, ComboResult>();
	for (const result of cloudflareResults) {
		groupedCloudflare.set(result.profile, result);
	}

	const profileCell = (result: ComboResult | undefined): RowMetricCell => {
		if (!result) {
			return { text: 'N/A' };
		}
		if (result.status === 'failed') {
			return { text: 'FAILED' };
		}
		return {
			text: formatMs(result.overallAvgMs),
			metric: result.overallAvgMs
		};
	};

	lines.push('# CollegeDB Sandbox Benchmark Report');
	lines.push('');
	lines.push(`Generated: ${generatedAt}`);
	lines.push('');
	lines.push('## Run Configuration');
	lines.push('');
	lines.push(`- Database filter: ${options.db}`);
	lines.push(`- KV filter: ${options.kv}`);
	lines.push(`- Profile filter: ${options.profile}`);
	lines.push(`- Base iterations: ${options.iterations}`);
	lines.push(`- Bulk size: ${options.bulkSize}`);
	lines.push(`- Strategy benchmark statements per operation: ${options.strategyStatements}`);
	lines.push(`- Included Cloudflare run: ${options.includeCloudflare || options.cloudflareOnly ? 'yes' : 'no'}`);
	lines.push('');
	lines.push('## How To Read This Report');
	lines.push('');
	lines.push(
		'- `Status` is `PASSED` only when every scenario in that environment passed, `PARTIAL_PASSED` when at least 66% passed, and `FAILED` below that threshold.'
	);
	lines.push('- Matrix latency cells are `average / p95` in milliseconds.');
	lines.push(
		'- `Per-Op Avg` divides each scenario by the routed operations it performs, so scenarios are comparable; `Overall Avg` is the unweighted scenario mean and is dominated by `bulk_crud`.'
	);
	lines.push(
		'- Strategy cells report key distribution (`imbalance / chi-square`), not latency: without a coordinator every strategy is local computation and only decides where a new key lands.'
	);
	lines.push('- Fastest latency in each matrix row is bold; slowest latency is italicized.');
	lines.push(
		`- Strategy matrix cells execute ${options.strategyStatements} writes + ${options.strategyStatements} reads per strategy, per target-region profile.`
	);
	lines.push('- `N/A` indicates an intentionally skipped scenario for that environment.');
	lines.push('- Use `Detailed Scenario Statistics` for full per-scenario latency distribution and errors.');
	lines.push('');
	lines.push('## Benchmark Catalog');
	lines.push('');
	lines.push('| Scenario Key | Scenario | What Happens | Workload Per Run |');
	lines.push('| --- | --- | --- | --- |');
	for (const name of SCENARIO_NAMES) {
		const item = SCENARIO_CATALOG[name];
		lines.push(`| ${name} | ${item.title} | ${item.details} | ${item.workload(catalogPlan, options.bulkSize)} |`);
	}
	lines.push('');

	if (comboResults.length > 0) {
		lines.push('## Matrix: SQL x KV (Overall)');
		lines.push('');
		// The old Best/Worst Strategy columns ranked a 5.2% latency spread whose wins
		// were near-uniform across all 16 columns, so they reported noise as a
		// verdict. Strategy comparison lives in the distribution matrix now.
		lines.push('| Combination | Profile | Status | Passed | Failed | Skipped | Per-Op Avg | Overall Avg | Duration |');
		lines.push('| --- | --- | --- | --- | --- | --- | --- | --- | --- |');

		for (const result of comboResults) {
			const states = countScenarioStates(result.scenarios);
			lines.push(
				`| ${result.baseId} | ${result.profile} | ${formatStatus(result.status)} | ${states.passed} | ${states.failed} | ${states.skipped} | ${formatMs(result.perOpAvgMs)} | ${formatMs(result.overallAvgMs)} | ${formatMs(result.durationMs)} |`
			);
		}
		lines.push('');

		const attributed = comboResults.filter((result) => result.phaseAttribution && result.phaseAttribution.length > 0);
		if (attributed.length > 0) {
			lines.push('## Matrix: Where a Routed Operation Spends Its Time');
			lines.push('');
			lines.push('- Measured with the router phase observer, with the in-memory mapping cache disabled so every read pays its KV lookup.');
			lines.push('- `Share` is the fraction of total measured time in that phase. `Avg` is per occurrence, not per operation.');
			lines.push('- Workload per environment: insert, routed read, update, and a read of a key with no row.');
			lines.push('');
			lines.push('| Combination | Profile | Phase | Count | Avg | p95 | Total | Share |');
			lines.push('| --- | --- | --- | --- | --- | --- | --- | --- |');

			for (const result of attributed) {
				for (const phase of result.phaseAttribution ?? []) {
					lines.push(
						`| ${result.baseId} | ${result.profile} | \`${phase.phase}\` | ${phase.count} | ${formatMs(phase.avgMs)} | ${formatMs(phase.p95Ms)} | ${formatMs(phase.totalMs)} | ${(phase.share * 100).toFixed(1)}% |`
					);
				}
			}
			lines.push('');
		}

		lines.push('## Matrix: Adapter Profiles (Overall Avg)');
		lines.push('');
		lines.push('| Combination | native | drizzle | per-statement-connection |');
		lines.push('| --- | --- | --- | --- |');
		for (const [baseId, byProfile] of groupedByBase) {
			const profileCells = [
				profileCell(byProfile.get('native')),
				profileCell(byProfile.get('drizzle')),
				profileCell(byProfile.get('per-statement-connection'))
			];
			const formattedProfileCells = emphasizeRowExtremes(profileCells);

			lines.push(`| ${baseId} | ${formattedProfileCells[0]} | ${formattedProfileCells[1]} | ${formattedProfileCells[2]} |`);
		}
		lines.push('');

		lines.push('## Matrix: Core Scenario Latency (avg/p95)');
		lines.push('');
		lines.push('| Combination | Profile | Basic CRUD | Advanced | Migration | Bulk CRUD | Indexing | Overall Avg |');
		lines.push('| --- | --- | --- | --- | --- | --- | --- | --- |');

		for (const result of comboResults) {
			const rowCells = [
				{ text: scenarioCell(result.scenarios.basic_crud), metric: scenarioMetric(result.scenarios.basic_crud) },
				{ text: scenarioCell(result.scenarios.advanced_usage), metric: scenarioMetric(result.scenarios.advanced_usage) },
				{ text: scenarioCell(result.scenarios.migration_mapping), metric: scenarioMetric(result.scenarios.migration_mapping) },
				{ text: scenarioCell(result.scenarios.bulk_crud), metric: scenarioMetric(result.scenarios.bulk_crud) },
				{ text: scenarioCell(result.scenarios.indexing), metric: scenarioMetric(result.scenarios.indexing) },
				{ text: formatMs(result.overallAvgMs), metric: result.overallAvgMs }
			];
			const formattedCells = emphasizeRowExtremes(rowCells);

			lines.push(
				`| ${result.baseId} | ${result.profile} | ${formattedCells[0]} | ${formattedCells[1]} | ${formattedCells[2]} | ${formattedCells[3]} | ${formattedCells[4]} | ${formattedCells[5]} |`
			);
		}
		lines.push('');

		lines.push('## Matrix: Introspection and Routing Latency (avg/p95)');
		lines.push('');
		lines.push('| Combination | Profile | Metadata | Pragma/Info | Counting | Fanout | Reassignment |');
		lines.push('| --- | --- | --- | --- | --- | --- | --- |');

		for (const result of comboResults) {
			const rowCells = [
				{ text: scenarioCell(result.scenarios.metadata_fetch), metric: scenarioMetric(result.scenarios.metadata_fetch) },
				{ text: scenarioCell(result.scenarios.pragma_or_info), metric: scenarioMetric(result.scenarios.pragma_or_info) },
				{ text: scenarioCell(result.scenarios.counting), metric: scenarioMetric(result.scenarios.counting) },
				{ text: scenarioCell(result.scenarios.shard_fanout), metric: scenarioMetric(result.scenarios.shard_fanout) },
				{ text: scenarioCell(result.scenarios.reassignment), metric: scenarioMetric(result.scenarios.reassignment) }
			];
			const formattedCells = emphasizeRowExtremes(rowCells);

			lines.push(
				`| ${result.baseId} | ${result.profile} | ${formattedCells[0]} | ${formattedCells[1]} | ${formattedCells[2]} | ${formattedCells[3]} | ${formattedCells[4]} |`
			);
		}
		lines.push('');

		if (strategyResults.length > 0) {
			const firstStrategy = strategyResults.find((result) => result.strategyBenchmark)?.strategyBenchmark;
			if (firstStrategy) {
				const shardLocationSummary = Object.entries(firstStrategy.shardLocations)
					.map(([binding, location]) => `${binding}:${location.region}`)
					.join(', ');

				lines.push('## Matrix: Strategy Key Distribution (imbalance / chi-square)');
				lines.push('');
				lines.push(
					`- Workload per strategy cell: ${firstStrategy.writesPerStrategy} writes + ${firstStrategy.readsPerStrategy} reads on ${firstStrategy.shardCount} mock databases.`
				);
				lines.push(
					`- Cells are \`share of keys on the busiest shard\` / \`chi-square against uniform\`. Even distribution over ${firstStrategy.shardCount} shards puts the share at ${(100 / firstStrategy.shardCount).toFixed(0)}%; lower chi-square is more even.`
				);
				lines.push(
					'- Distribution is measured rather than latency: without a coordinator every strategy is a few microseconds of local computation and only decides where a new key lands.'
				);
				lines.push('- Strategy columns include every single strategy and mixed read/write permutations where read and write differ.');
				lines.push(`- Mock shard locations (Cloudflare D1-style regions): ${shardLocationSummary}`);
				lines.push('');

				for (const targetRegion of STRATEGY_BENCHMARK_TARGET_REGIONS) {
					lines.push(`### Target Region: ${targetRegion}`);
					lines.push('');
					const headers = ['Combination', 'Profile', ...firstStrategy.columns.map((column) => column.label)];
					lines.push(`| ${headers.join(' | ')} |`);
					lines.push(`| ${headers.map(() => '---').join(' | ')} |`);

					for (const result of strategyResults) {
						const benchmark = result.strategyBenchmark;
						if (!benchmark) {
							continue;
						}

						const row = benchmark.rows.find((entry) => entry.targetRegion === targetRegion);
						const cells = benchmark.columns.map((column) => ({
							text: strategyBenchmarkCell(row?.columns[column.key]),
							metric: strategyDistributionMetric(row?.columns[column.key])
						}));
						const formattedCells = emphasizeRowExtremes(cells);
						lines.push(`| ${result.baseId} | ${result.profile} | ${formattedCells.join(' | ')} |`);
					}

					lines.push('');
				}

				lines.push('## Matrix: Strategy Benchmark Database Sizes');
				lines.push('');
				lines.push('| Combination | Profile | db-wnam | db-enam | db-weur | db-apac | db-oc |');
				lines.push('| --- | --- | --- | --- | --- | --- | --- |');
				for (const result of strategyResults) {
					const sizes = result.strategyBenchmark?.databaseSizesBytes;
					lines.push(
						`| ${result.baseId} | ${result.profile} | ${formatBytes(sizes?.['db-wnam'])} | ${formatBytes(sizes?.['db-enam'])} | ${formatBytes(sizes?.['db-weur'])} | ${formatBytes(sizes?.['db-apac'])} | ${formatBytes(sizes?.['db-oc'])} |`
					);
				}
				lines.push('');
			}
		}
	}

	if (cloudflareResults.length > 0) {
		lines.push('## Cloudflare Worker (`wrangler dev --local`)');
		lines.push('');
		lines.push(
			'| Environment | Profile | Status | Basic CRUD | Advanced | Migration | Bulk CRUD | Indexing | Metadata | Pragma | Counting | Fanout | Reassignment | Overall Avg |'
		);
		lines.push('| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |');
		for (const result of cloudflareResults) {
			const rowCells = [
				{ text: scenarioCell(result.scenarios.basic_crud), metric: scenarioMetric(result.scenarios.basic_crud) },
				{ text: scenarioCell(result.scenarios.advanced_usage), metric: scenarioMetric(result.scenarios.advanced_usage) },
				{ text: scenarioCell(result.scenarios.migration_mapping), metric: scenarioMetric(result.scenarios.migration_mapping) },
				{ text: scenarioCell(result.scenarios.bulk_crud), metric: scenarioMetric(result.scenarios.bulk_crud) },
				{ text: scenarioCell(result.scenarios.indexing), metric: scenarioMetric(result.scenarios.indexing) },
				{ text: scenarioCell(result.scenarios.metadata_fetch), metric: scenarioMetric(result.scenarios.metadata_fetch) },
				{ text: scenarioCell(result.scenarios.pragma_or_info), metric: scenarioMetric(result.scenarios.pragma_or_info) },
				{ text: scenarioCell(result.scenarios.counting), metric: scenarioMetric(result.scenarios.counting) },
				{ text: scenarioCell(result.scenarios.shard_fanout), metric: scenarioMetric(result.scenarios.shard_fanout) },
				{ text: scenarioCell(result.scenarios.reassignment), metric: scenarioMetric(result.scenarios.reassignment) },
				{ text: formatMs(result.overallAvgMs), metric: result.overallAvgMs }
			];
			const formattedCells = emphasizeRowExtremes(rowCells);

			lines.push(
				`| cloudflare | ${result.profile} | ${result.status.toUpperCase()} | ${formattedCells[0]} | ${formattedCells[1]} | ${formattedCells[2]} | ${formattedCells[3]} | ${formattedCells[4]} | ${formattedCells[5]} | ${formattedCells[6]} | ${formattedCells[7]} | ${formattedCells[8]} | ${formattedCells[9]} | ${formattedCells[10]} |`
			);
		}
		lines.push('');
		lines.push('## Matrix: Cloudflare Adapter Profiles (Overall Avg)');
		lines.push('');
		lines.push('| Environment | native | drizzle | hyperdrive |');
		lines.push('| --- | --- | --- | --- |');
		const cloudflareProfileCells = [
			profileCell(groupedCloudflare.get('native')),
			profileCell(groupedCloudflare.get('drizzle')),
			profileCell(groupedCloudflare.get('hyperdrive'))
		];
		const formattedCloudflareProfiles = emphasizeRowExtremes(cloudflareProfileCells);
		lines.push(
			`| cloudflare | ${formattedCloudflareProfiles[0]} | ${formattedCloudflareProfiles[1]} | ${formattedCloudflareProfiles[2]} |`
		);
		lines.push('');

		if (cloudflareStrategyResults.length > 0) {
			const firstCloudflareStrategy = cloudflareStrategyResults.find((result) => result.strategyBenchmark)?.strategyBenchmark;
			if (firstCloudflareStrategy) {
				const shardLocationSummary = Object.entries(firstCloudflareStrategy.shardLocations)
					.map(([binding, location]) => `${binding}:${location.region}`)
					.join(', ');
				const cloudflareShardBindings = Object.keys(firstCloudflareStrategy.shardLocations);

				lines.push('## Matrix: Cloudflare Bulk Read/Write Strategy Mix (avg/p95)');
				lines.push('');
				lines.push(
					`- Workload per strategy cell: ${firstCloudflareStrategy.writesPerStrategy} writes + ${firstCloudflareStrategy.readsPerStrategy} reads on ${firstCloudflareStrategy.shardCount} Cloudflare D1 shard bindings.`
				);
				lines.push('- Strategy columns include every single strategy and mixed read/write permutations where read and write differ.');
				lines.push(`- Cloudflare D1 shard locations: ${shardLocationSummary}`);
				lines.push('');

				for (const targetRegion of STRATEGY_BENCHMARK_TARGET_REGIONS) {
					lines.push(`### Cloudflare Target Region: ${targetRegion}`);
					lines.push('');
					const headers = ['Environment', 'Profile', ...firstCloudflareStrategy.columns.map((column) => column.label)];
					lines.push(`| ${headers.join(' | ')} |`);
					lines.push(`| ${headers.map(() => '---').join(' | ')} |`);

					for (const result of cloudflareStrategyResults) {
						const benchmark = result.strategyBenchmark;
						if (!benchmark) {
							continue;
						}

						const row = benchmark.rows.find((entry) => entry.targetRegion === targetRegion);
						const cells = benchmark.columns.map((column) => ({
							text: strategyBenchmarkCell(row?.columns[column.key]),
							metric: strategyDistributionMetric(row?.columns[column.key])
						}));
						const formattedCells = emphasizeRowExtremes(cells);
						lines.push(`| cloudflare | ${result.profile} | ${formattedCells.join(' | ')} |`);
					}

					lines.push('');
				}

				lines.push('## Matrix: Cloudflare Strategy Benchmark Database Sizes');
				lines.push('');
				lines.push(`| Environment | Profile | ${cloudflareShardBindings.join(' | ')} |`);
				lines.push(`| ${['---', '---', ...cloudflareShardBindings.map(() => '---')].join(' | ')} |`);
				for (const result of cloudflareStrategyResults) {
					const sizes = result.strategyBenchmark?.databaseSizesBytes;
					const sizeCells = cloudflareShardBindings.map((binding) => formatBytes(sizes?.[binding]));
					lines.push(`| cloudflare | ${result.profile} | ${sizeCells.join(' | ')} |`);
				}
				lines.push('');
			}
		}
	}

	lines.push('## Detailed Scenario Statistics');
	lines.push('');

	for (const result of comboResults) {
		lines.push(`### ${result.id}`);
		lines.push('');
		lines.push(`- Status: ${result.status.toUpperCase()}`);
		lines.push(`- Duration: ${formatMs(result.durationMs)}`);
		if (result.error) {
			lines.push(`- Error: ${result.error}`);
		}
		for (const scenarioName of SCENARIO_NAMES) {
			const scenario = result.scenarios[scenarioName];
			const label = scenarioLabel(scenarioName);
			if (scenario.status === 'passed') {
				lines.push(
					`- ${scenario.name} (${label}): avg=${formatMs(scenario.avgMs)}, p50=${formatMs(scenario.p50Ms)}, p95=${formatMs(scenario.p95Ms)}, min=${formatMs(scenario.minMs)}, max=${formatMs(scenario.maxMs)}, n=${scenario.iterations}${scenario.notes ? `; ${scenario.notes}` : ''}`
				);
			} else if (scenario.status === 'failed') {
				lines.push(`- ${scenario.name} (${label}): FAILED (${scenario.error ?? 'unknown error'})`);
			} else {
				lines.push(`- ${scenario.name} (${label}): SKIPPED (${scenario.notes ?? 'not applicable'})`);
			}
		}

		if (result.strategyBenchmark) {
			const benchmark = result.strategyBenchmark;
			lines.push(
				`- strategy_bulk_rw_matrix: writes=${benchmark.writesPerStrategy}, reads=${benchmark.readsPerStrategy}, shards=${benchmark.shardCount}, columns=${benchmark.columns.length}`
			);
			if (benchmark.databaseSizesBytes) {
				lines.push(
					`- strategy_bulk_rw_matrix sizes: db-wnam=${formatBytes(benchmark.databaseSizesBytes['db-wnam'])}, db-enam=${formatBytes(benchmark.databaseSizesBytes['db-enam'])}, db-weur=${formatBytes(benchmark.databaseSizesBytes['db-weur'])}, db-apac=${formatBytes(benchmark.databaseSizesBytes['db-apac'])}, db-oc=${formatBytes(benchmark.databaseSizesBytes['db-oc'])}`
				);
			}
			if (benchmark.error) {
				lines.push(`- strategy_bulk_rw_matrix error: ${benchmark.error}`);
			}

			for (const row of benchmark.rows) {
				let passed = 0;
				const failedColumns: string[] = [];

				for (const column of benchmark.columns) {
					const cell = row.columns[column.key];
					if (cell?.status === 'passed') {
						passed += 1;
					} else {
						failedColumns.push(column.label);
					}
				}

				const failed = benchmark.columns.length - passed;
				if (failed > 0) {
					const preview = failedColumns.slice(0, 4).join(', ');
					lines.push(
						`- strategy_bulk_rw_matrix[${row.targetRegion}]: passed=${passed}, failed=${failed} (${preview}${failedColumns.length > 4 ? ', ...' : ''})`
					);
				} else {
					lines.push(`- strategy_bulk_rw_matrix[${row.targetRegion}]: passed=${passed}, failed=${failed}`);
				}
			}
		}

		lines.push('');
	}

	for (const cloudflareResult of cloudflareResults) {
		lines.push(`### cloudflare/${cloudflareResult.profile}`);
		lines.push('');
		lines.push(`- Status: ${cloudflareResult.status.toUpperCase()}`);
		lines.push(`- Duration: ${formatMs(cloudflareResult.durationMs)}`);
		if (cloudflareResult.error) {
			lines.push(`- Error: ${cloudflareResult.error}`);
		}
		for (const scenarioName of SCENARIO_NAMES) {
			const scenario = cloudflareResult.scenarios[scenarioName];
			const label = scenarioLabel(scenarioName);
			if (scenario.status === 'passed') {
				lines.push(
					`- ${scenario.name} (${label}): avg=${formatMs(scenario.avgMs)}, p50=${formatMs(scenario.p50Ms)}, p95=${formatMs(scenario.p95Ms)}, min=${formatMs(scenario.minMs)}, max=${formatMs(scenario.maxMs)}, n=${scenario.iterations}${scenario.notes ? `; ${scenario.notes}` : ''}`
				);
			} else if (scenario.status === 'failed') {
				lines.push(`- ${scenario.name} (${label}): FAILED (${scenario.error ?? 'unknown error'})`);
			} else {
				lines.push(`- ${scenario.name} (${label}): SKIPPED (${scenario.notes ?? 'not applicable'})`);
			}
		}

		if (cloudflareResult.strategyBenchmark) {
			const benchmark = cloudflareResult.strategyBenchmark;
			lines.push(
				`- strategy_bulk_rw_matrix: writes=${benchmark.writesPerStrategy}, reads=${benchmark.readsPerStrategy}, shards=${benchmark.shardCount}, columns=${benchmark.columns.length}`
			);
			if (benchmark.databaseSizesBytes) {
				const sizeSummary = Object.keys(benchmark.shardLocations)
					.map((binding) => `${binding}=${formatBytes(benchmark.databaseSizesBytes?.[binding])}`)
					.join(', ');
				lines.push(`- strategy_bulk_rw_matrix sizes: ${sizeSummary}`);
			}
			if (benchmark.error) {
				lines.push(`- strategy_bulk_rw_matrix error: ${benchmark.error}`);
			}

			for (const row of benchmark.rows) {
				let passed = 0;
				const failedColumns: string[] = [];

				for (const column of benchmark.columns) {
					const cell = row.columns[column.key];
					if (cell?.status === 'passed') {
						passed += 1;
					} else {
						failedColumns.push(column.label);
					}
				}

				const failed = benchmark.columns.length - passed;
				if (failed > 0) {
					const preview = failedColumns.slice(0, 4).join(', ');
					lines.push(
						`- strategy_bulk_rw_matrix[${row.targetRegion}]: passed=${passed}, failed=${failed} (${preview}${failedColumns.length > 4 ? ', ...' : ''})`
					);
				} else {
					lines.push(`- strategy_bulk_rw_matrix[${row.targetRegion}]: passed=${passed}, failed=${failed}`);
				}
			}
		}
		lines.push('');
	}

	lines.push('## Notes');
	lines.push('');
	lines.push('- Metric format in matrix cells: average latency / p95 latency.');
	lines.push('- Measurements are end-to-end and include routing, KV mapping operations, and SQL execution.');
	lines.push('- Strategy matrix runs all ShardingStrategy values plus all read/write mixed permutations across five regional shard mocks.');
	lines.push('- Location strategy is evaluated across target regions: wnam, enam, weur, apac, and oc.');
	lines.push('- Cloudflare benchmark uses sandbox Worker endpoints via `wrangler dev --local`.');
	lines.push('- Cloudflare iterations are intentionally scaled down from the local matrix to keep dev-server runs stable.');
	lines.push('');

	return lines.join('\n');
}

function printMarkdownAnsi(markdown: string): void {
	const ansi = Bun.markdown.render(
		markdown,
		{
			heading: (children: string, meta?: { level?: number }) => {
				const level = meta?.level ?? 1;
				const color = level === 1 ? '\x1b[1;36m' : '\x1b[1;34m';
				return `${color}${children}\x1b[0m\n`;
			},
			paragraph: (children: string) => `${children}\n`,
			strong: (children: string) => `\x1b[1m${children}\x1b[22m`,
			emphasis: (children: string) => `\x1b[3m${children}\x1b[23m`,
			codespan: (children: string) => `\x1b[38;5;81m${children}\x1b[39m`,
			link: (children: string, meta?: { href?: string }) => `${children}${meta?.href ? ` (${meta.href})` : ''}`,
			listItem: (children: string, meta?: { ordered?: boolean; index?: number; start?: number }) => {
				if (meta?.ordered) {
					const marker = (meta.start ?? 1) + (meta.index ?? 0);
					return `${marker}. ${children.trim()}\n`;
				}
				return `- ${children.trim()}\n`;
			},
			list: (children: string) => `${children}`,
			table: (children: string) => `\n${children}\n`,
			tr: (children: string) => `${children}\n`,
			th: (children: string) => `${children.trim()} | `,
			td: (children: string) => `${children.trim()} | `,
			code: (children: string, meta?: { language?: string }) => {
				const header = meta?.language ? `[${meta.language}]\n` : '';
				return `\x1b[38;5;246m${header}${children}\x1b[39m\n`;
			}
		},
		{ tables: true, strikethrough: true, tasklists: true }
	);

	console.log(ansi);
}

async function safely(task: () => Promise<void>): Promise<void> {
	try {
		await task();
	} catch {
		// ignore cleanup failures
	}
}

main().catch((error) => {
	console.error('[Sandbox] Fatal error:', error instanceof Error ? error.message : String(error));
	process.exit(1);
});
