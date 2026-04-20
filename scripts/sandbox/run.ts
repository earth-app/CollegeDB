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
	all,
	allAllShards,
	allShard,
	createHyperdriveMySQLProvider,
	createHyperdrivePostgresProvider,
	createMappingsForExistingKeys,
	createMySQLProvider,
	createNuxtHubKVProvider,
	createPostgreSQLProvider,
	createRedisKVProvider,
	createSQLiteProvider,
	createSchemaAcrossShards,
	createValkeyKVProvider,
	first,
	flush,
	initialize,
	resetConfig,
	run,
	runAllShards,
	runShard
} from '../../src/index';
import { KVShardMapper } from '../../src/kvmap';
import type {
	CollegeDBConfig,
	D1Region,
	KVStorage,
	MixedShardingStrategy,
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
const WRANGLER_BIN = resolve(PROJECT_ROOT, 'node_modules', '.bin', 'wrangler');

const ALL_DATABASES = ['postgres', 'mysql', 'mariadb', 'sqlite'] as const;
const ALL_KV = ['redis', 'valkey'] as const;
const ALL_ADAPTER_PROFILES = ['native', 'drizzle', 'hyperdrive', 'nuxthub'] as const;
const SCENARIO_NAMES = [
	'basic_crud',
	'advanced_usage',
	'migration_mapping',
	'bulk_crud',
	'indexing',
	'metadata_fetch',
	'pragma_or_info',
	'counting',
	'shard_fanout',
	'reassignment'
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
type Status = 'passed' | 'failed' | 'skipped';

interface CLIOptions {
	db: DatabaseFlavor | 'all';
	kv: KVFlavor | 'all';
	profile: AdapterProfile | 'all';
	iterations: number;
	bulkSize: number;
	strategyStatements: number;
	includeCloudflare: boolean;
	cloudflareOnly: boolean;
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
	durationMs: number;
	error?: string;
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
		for (const profile of profilesForCloudflare(options.profile)) {
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

	const markdown = buildMarkdownReport(options, comboResults, cloudflareResults);
	const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
	const scope = options.cloudflareOnly
		? 'cloudflare'
		: `${options.db}-${options.kv}-${options.profile}${options.includeCloudflare ? '-plus-cloudflare' : ''}`;
	const outPath = join(RESULTS_DIR, `sandbox-latency-${scope}-${timestamp}.md`);
	const latestPath = join(RESULTS_DIR, 'latest.md');

	await Bun.write(outPath, markdown);
	await Bun.write(latestPath, markdown);

	console.log(`\n[Sandbox] Markdown report written: ${outPath}`);
	console.log(`[Sandbox] Latest report updated: ${latestPath}\n`);
	printMarkdownAnsi(markdown);

	const hasFailures = [...comboResults, ...cloudflareResults].some((r) => r.status === 'failed');
	if (hasFailures) {
		process.exitCode = 1;
	}
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
		cloudflareOnly: false
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
	const supported =
		dbFlavor === 'sqlite' ? (['native', 'drizzle', 'nuxthub'] as const) : (['native', 'drizzle', 'hyperdrive', 'nuxthub'] as const);

	if (profileFilter === 'all') {
		return [...supported];
	}

	return (supported as readonly AdapterProfile[]).includes(profileFilter) ? [profileFilter] : [];
}

function profilesForCloudflare(profileFilter: AdapterProfile | 'all'): AdapterProfile[] {
	const supported = ['native', 'drizzle', 'nuxthub'] as const;
	if (profileFilter === 'all') {
		return [...supported];
	}
	return (supported as readonly AdapterProfile[]).includes(profileFilter) ? [profileFilter] : [];
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
	const services = composeServicesForCombo(combo);
	let kvRuntime: KVRuntime | null = null;
	let sqlRuntime: SQLRuntime | null = null;
	let strategyBenchmark: StrategyBenchmarkResult | undefined;
	let initialized = false;
	const resultId = `${combo.id}/${profile}`;

	try {
		await composeDown();
		if (services.length > 0) {
			await composeUp(services);
		}

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
		await createSchemaAcrossShards(config.shards, BENCH_SCHEMA);
		await resetBenchData();

		const plan = buildIterationPlan(options.iterations);
		scenarios.basic_crud = await scenarioBasicCrud(plan.basic);

		await resetBenchData();
		scenarios.advanced_usage = await scenarioAdvancedUsage(plan.advanced, config);

		await resetBenchData();
		scenarios.migration_mapping = await scenarioMigrationMapping(plan.migration, config, options.bulkSize);

		await resetBenchData();
		scenarios.bulk_crud = await scenarioBulkCrud(plan.bulk, options.bulkSize);

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

		strategyBenchmark = await benchmarkStrategyBulkMatrix(combo.db, profile, options, kvRuntime.kv);

		const status: Status =
			Object.values(scenarios).some((scenario) => scenario.status === 'failed') || strategyBenchmarkHasFailures(strategyBenchmark)
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

		await composeDown();
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

		await resetCloudflareBenchmarkData(profile);
		scenarios.reassignment = await scenarioCloudflareReassignment(Math.max(4, Math.floor(plan.reassignment / 2)), profile);

		strategyBenchmark = await benchmarkCloudflareStrategyBulkMatrix(options, profile);

		const status: Status =
			Object.values(scenarios).some((scenario) => scenario.status === 'failed') || strategyBenchmarkHasFailures(strategyBenchmark)
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
			durationMs: performance.now() - started
		};
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		const stderr = await stderrPromise;
		const stdout = await stdoutPromise;
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
			...calculateLatencyStats(samplesMs)
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

		initialize(config);

		const moved = await first(id, SELECT_USER_SQL, [id]);
		if (!moved) {
			throw new Error('Expected reassigned record to be routable after reassignment');
		}
	});
}

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

async function resetCloudflareBenchmarkData(profile: AdapterProfile): Promise<void> {
	await assertOk(await fetch(cloudflareUrl('/api/benchmark/reset', profile), { method: 'POST' }), 'POST /api/benchmark/reset');
}

async function postCloudflareBenchmark(path: string, payload: Record<string, unknown>, profile: AdapterProfile): Promise<Response> {
	const response = await fetch(cloudflareUrl(path, profile), {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify(payload)
	});
	await assertOk(response, `POST ${path}`);
	return response;
}

async function createKVRuntime(kvFlavor: KVFlavor, profile: AdapterProfile): Promise<KVRuntime> {
	const url = kvFlavor === 'valkey' ? 'redis://127.0.0.1:6380' : 'redis://127.0.0.1:6379';
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

	const kv =
		profile === 'nuxthub'
			? createNuxtHubKVProvider(createNuxtHubKVCompatClient(client))
			: kvFlavor === 'redis'
				? createRedisKVProvider(client)
				: createValkeyKVProvider(client);
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

function createNuxtHubKVCompatClient(client: any): {
	get: <T = unknown>(key: string) => Promise<T | null>;
	set: (key: string, value: unknown) => Promise<void>;
	del: (key: string) => Promise<void>;
	keys: (prefix?: string) => Promise<string[]>;
} {
	return {
		get: async <T = unknown>(key: string) => {
			const value = await client.get(key);
			return (value === null ? null : String(value)) as T | null;
		},
		set: async (key: string, value: unknown) => {
			const serialized = typeof value === 'string' ? value : JSON.stringify(value);
			await client.set(key, serialized);
		},
		del: async (key: string) => {
			await client.del(key);
		},
		keys: async (prefix: string = '') => {
			const pattern = `${prefix}*`;
			let cursor = '0';
			const found: string[] = [];

			do {
				const scanResult = await scanRedisKeys(client, cursor, pattern);
				cursor = scanResult.cursor;
				for (const key of scanResult.keys) {
					if (!prefix || key.startsWith(prefix)) {
						found.push(key);
					}
				}
			} while (cursor !== '0');

			return found;
		}
	};
}

async function scanRedisKeys(client: any, cursor: string, pattern: string): Promise<{ cursor: string; keys: string[] }> {
	try {
		const objectResult = await client.scan(cursor, { MATCH: pattern, COUNT: 500 });
		if (Array.isArray(objectResult)) {
			return {
				cursor: String(objectResult[0] ?? '0'),
				keys: Array.isArray(objectResult[1]) ? objectResult[1] : []
			};
		}

		return {
			cursor: String(objectResult?.cursor ?? '0'),
			keys: Array.isArray(objectResult?.keys) ? objectResult.keys : []
		};
	} catch {
		const tupleResult = await client.scan(cursor, 'MATCH', pattern, 'COUNT', '500');
		return {
			cursor: String(tupleResult?.[0] ?? '0'),
			keys: Array.isArray(tupleResult?.[1]) ? tupleResult[1] : []
		};
	}
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
			return createMySQLRuntime(runId, 3306, profile, shardBindings);
		case 'mariadb':
			return createMySQLRuntime(runId, 3307, profile, shardBindings);
		case 'sqlite':
			return createSQLiteRuntime(runId, profile, shardBindings);
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
		port: 5432,
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
			port: 5432,
			user: 'collegedb',
			password: 'collegedb',
			database: dbName,
			max: 10
		});
		await pool.query('SELECT 1');
		poolsByBinding.set(binding, pool);
	}

	const createPostgresProviderForProfile = (pool: PostgresPool, connectionString: string, currentProfile: AdapterProfile): SQLDatabase => {
		switch (currentProfile) {
			case 'native':
				return createPostgreSQLProvider(pool);
			case 'drizzle':
			case 'nuxthub': {
				const drizzleDb = drizzlePostgres(pool);
				return createPostgreSQLProvider(drizzleDb, drizzleSql);
			}
			case 'hyperdrive':
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
		const connectionString = `postgres://collegedb:collegedb@127.0.0.1:5432/${dbName}`;
		shards[binding] = createPostgresProviderForProfile(pool, connectionString, profile);
	}

	return {
		shards,
		close: async () => {
			for (const pool of poolsByBinding.values()) {
				await pool.end();
			}

			const closeAdmin = new PostgresClient({
				host: '127.0.0.1',
				port: 5432,
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
		await pool.query('SELECT 1');
		poolsByBinding.set(binding, pool);
	}

	const createMySQLProviderForProfile = (pool: mysql.Pool, connectionString: string, currentProfile: AdapterProfile): SQLDatabase => {
		switch (currentProfile) {
			case 'native':
				return createMySQLProvider(pool);
			case 'drizzle':
			case 'nuxthub': {
				const drizzleDb = drizzleMySQL(pool);
				return createMySQLProvider(drizzleDb, drizzleSql);
			}
			case 'hyperdrive':
				return createHyperdriveMySQLProvider({ connectionString }, (conn) => ({
					execute: async (sql: string, bindings: any[] = []) => {
						const connection = await mysql.createConnection(conn);
						try {
							return await connection.execute(sql, bindings);
						} finally {
							await connection.end();
						}
					}
				}));
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

	return {
		shards,
		close: async () => {
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

	const useDrizzle = profile === 'drizzle' || profile === 'nuxthub';
	const shards: Record<string, SQLDatabase> = {};

	for (const binding of shardBindings) {
		const db = dbByBinding.get(binding);
		if (!db) {
			continue;
		}

		shards[binding] = useDrizzle ? createSQLiteProvider(drizzleBunSQLite({ client: db }), drizzleSql) : createSQLiteProvider(db);
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
		indexing: Math.max(6, Math.floor(baseIterations * 0.75)),
		metadata: Math.max(6, Math.floor(baseIterations * 0.7)),
		pragma: Math.max(6, Math.floor(baseIterations * 0.7)),
		counting: Math.max(6, Math.floor(baseIterations * 0.7)),
		fanout: Math.max(6, Math.floor(baseIterations * 0.7)),
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
				port: 5432,
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
		'PostgreSQL on 5432'
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

async function waitForHttp(url: string, timeoutMs: number): Promise<void> {
	const started = Date.now();
	let lastError: unknown;
	while (Date.now() - started < timeoutMs) {
		try {
			const response = await fetch(url);
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

function computeOverallAverage(scenarios: Record<ScenarioName, ScenarioStats>): number | undefined {
	const passing = Object.values(scenarios).filter((scenario) => scenario.status === 'passed' && typeof scenario.avgMs === 'number');
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
	return `${formatMs(cell.avgMs)} / ${formatMs(cell.p95Ms)}`;
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
	lines.push('- `Status` is `PASSED` only when every scenario in that environment passed.');
	lines.push('- Matrix latency cells are `average / p95` in milliseconds.');
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
		lines.push('| Combination | Profile | Status | Passed | Failed | Skipped | Overall Avg | Duration | Best Strategy | Worst Strategy |');
		lines.push('| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |');

		for (const result of comboResults) {
			const states = countScenarioStates(result.scenarios);
			const strategyExtremes = strategyAggregateExtremes(result.strategyBenchmark);
			const formattedStrategyCells = emphasizeRowExtremes([
				{ text: formatStrategyAggregate(strategyExtremes.best), metric: strategyExtremes.best?.avgMs },
				{ text: formatStrategyAggregate(strategyExtremes.worst), metric: strategyExtremes.worst?.avgMs }
			]);

			lines.push(
				`| ${result.baseId} | ${result.profile} | ${result.status.toUpperCase()} | ${states.passed} | ${states.failed} | ${states.skipped} | ${formatMs(result.overallAvgMs)} | ${formatMs(result.durationMs)} | ${formattedStrategyCells[0]} | ${formattedStrategyCells[1]} |`
			);
		}
		lines.push('');

		lines.push('## Matrix: Adapter Profiles (Overall Avg)');
		lines.push('');
		lines.push('| Combination | native | drizzle | hyperdrive | nuxthub |');
		lines.push('| --- | --- | --- | --- | --- |');
		for (const [baseId, byProfile] of groupedByBase) {
			const profileCells = [
				profileCell(byProfile.get('native')),
				profileCell(byProfile.get('drizzle')),
				profileCell(byProfile.get('hyperdrive')),
				profileCell(byProfile.get('nuxthub'))
			];
			const formattedProfileCells = emphasizeRowExtremes(profileCells);

			lines.push(
				`| ${baseId} | ${formattedProfileCells[0]} | ${formattedProfileCells[1]} | ${formattedProfileCells[2]} | ${formattedProfileCells[3]} |`
			);
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

				lines.push('## Matrix: Bulk Read/Write Strategy Mix (avg/p95)');
				lines.push('');
				lines.push(
					`- Workload per strategy cell: ${firstStrategy.writesPerStrategy} writes + ${firstStrategy.readsPerStrategy} reads on ${firstStrategy.shardCount} mock databases.`
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
							metric: strategyCellMetric(row?.columns[column.key])
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
		lines.push('| Environment | native | drizzle | nuxthub |');
		lines.push('| --- | --- | --- | --- |');
		const cloudflareProfileCells = [
			profileCell(groupedCloudflare.get('native')),
			profileCell(groupedCloudflare.get('drizzle')),
			profileCell(groupedCloudflare.get('nuxthub'))
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
							metric: strategyCellMetric(row?.columns[column.key])
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
					`- ${scenario.name} (${label}): avg=${formatMs(scenario.avgMs)}, p50=${formatMs(scenario.p50Ms)}, p95=${formatMs(scenario.p95Ms)}, min=${formatMs(scenario.minMs)}, max=${formatMs(scenario.maxMs)}, n=${scenario.iterations}`
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
					`- ${scenario.name} (${label}): avg=${formatMs(scenario.avgMs)}, p50=${formatMs(scenario.p50Ms)}, p95=${formatMs(scenario.p95Ms)}, min=${formatMs(scenario.minMs)}, max=${formatMs(scenario.maxMs)}, n=${scenario.iterations}`
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
