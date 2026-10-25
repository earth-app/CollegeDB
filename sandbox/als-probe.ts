import type { D1Database, DurableObjectNamespace, ExecutionContext, KVNamespace } from '@cloudflare/workers-types';
import { asyncContextAvailable } from '../src/async-context';
import { collegedb, CollegeDBError, createClient, first, getActiveConfig, initialize, run } from '../src/index';
import type { CollegeDBConfig } from '../src/types';

export interface AlsEnv {
	KV: KVNamespace;
	ShardCoordinator: DurableObjectNamespace;
	'db-east': D1Database;
	'db-west': D1Database;
	'db-central': D1Database;
}

type Env = AlsEnv;

type D1Binding = 'db-east' | 'db-west' | 'db-central';
type AlsTag = 'a' | 'b';
export type AlsMode = 'scoped' | 'unscoped' | 'client' | 'reject';

const ALS_SCHEMA = 'CREATE TABLE IF NOT EXISTS als_rows (id TEXT PRIMARY KEY, writer TEXT)';
const ALS_INSERT_SQL = 'INSERT INTO als_rows (id, writer) VALUES (?, ?)';
const ALS_SELECT_SQL = 'SELECT id, writer FROM als_rows WHERE id = ?';
const ALS_BINDINGS: D1Binding[] = ['db-east', 'db-west', 'db-central'];
const ALS_SIDES: Record<AlsTag, { bindings: D1Binding[]; names: string[] }> = {
	a: { bindings: ['db-east', 'db-central'], names: ['als-a1', 'als-a2'] },
	b: { bindings: ['db-west'], names: ['als-b1'] }
};
// callbacks per wave; each wave overlaps this many pairs of different configurations
const ALS_WAVE = 10;

interface AlsExpected {
	id: string;
	tag: AlsTag;
	done: boolean;
}

interface AlsApi {
	run: (key: string, sql: string, bindings?: any[]) => Promise<unknown>;
	first: (key: string, sql: string, bindings?: any[]) => Promise<unknown>;
	getActiveConfig: () => CollegeDBConfig | null;
}

interface AlsTrace {
	tag: AlsTag;
	checkpoints: string[][];
	readBack: boolean;
}

function alsId(label: string, nonce: string, i: number, k: number): string {
	return `${label}:${nonce}:${i}:${k}`;
}

function alsConfig(env: Env, ctx: ExecutionContext, tag: AlsTag, nonce: string): CollegeDBConfig {
	const side = ALS_SIDES[tag];
	return {
		kv: env.KV,
		coordinator: env.ShardCoordinator,
		coordinatorScope: `als-${tag}-${nonce}`,
		shards: Object.fromEntries(side.names.map((name, index) => [name, env[side.bindings[index]!]])),
		strategy: 'round-robin',
		disableAutoMigration: true,
		waitUntil: (promise) => ctx.waitUntil(promise)
	};
}

async function alsPrepare(env: Env): Promise<void> {
	for (const binding of ALS_BINDINGS) {
		await env[binding].exec(ALS_SCHEMA);
		await env[binding].exec('DELETE FROM als_rows');
	}
}

async function alsCleanup(env: Env): Promise<void> {
	for (const binding of ALS_BINDINGS) {
		await env[binding].exec('DROP TABLE IF EXISTS als_rows');
	}
}

/** One callback: reads its configuration before and after each kind of await, and writes two rows. */
async function alsBody(env: Env, tag: AlsTag, label: string, nonce: string, i: number, api: AlsApi): Promise<AlsTrace> {
	const checkpoints: string[][] = [];
	const look = () => void checkpoints.push(Object.keys(api.getActiveConfig()?.shards ?? {}));
	const idOne = alsId(label, nonce, i, 0);
	const idTwo = alsId(label, nonce, i, 1);

	look();
	await env.KV.get(`als-probe:${label}`);
	look();
	await api.run(idOne, ALS_INSERT_SQL, [idOne, label]);
	look();
	await env.ShardCoordinator.get(env.ShardCoordinator.idFromName(`als-await-${label}-${nonce}`)).fetch('http://coordinator/stats');
	look();
	await new Promise<void>((resolve) => setTimeout(resolve, 1));
	look();
	await api.run(idTwo, ALS_INSERT_SQL, [idTwo, label]);
	look();
	const row = await api.first(idTwo, ALS_SELECT_SQL, [idTwo]);
	look();

	return { tag, checkpoints, readBack: row !== null && row !== undefined };
}

/** Which D1 binding holds each row id, read straight from the bindings. */
async function alsRowLocations(env: Env, nonce: string): Promise<Map<string, D1Binding[]>> {
	const found = new Map<string, D1Binding[]>();
	for (const binding of ALS_BINDINGS) {
		const { results } = await env[binding].prepare('SELECT id FROM als_rows WHERE id LIKE ?').bind(`%:${nonce}:%`).all<{ id: string }>();
		for (const { id } of results) {
			found.set(id, [...(found.get(id) ?? []), binding]);
		}
	}
	return found;
}

function alsAudit(found: Map<string, D1Binding[]>, expected: AlsExpected[]): { misplaced: number; missing: number } {
	let misplaced = 0;
	let missing = 0;
	for (const { id, tag, done } of expected) {
		const on = found.get(id) ?? [];
		if (on.some((binding) => !ALS_SIDES[tag].bindings.includes(binding))) {
			misplaced++;
		} else if (on.length === 0 && done) {
			missing++;
		}
	}
	return { misplaced, missing };
}

/** Pairs of overlapping callbacks with different configurations, through the path `mode` names. */
export async function alsOverlapProbe(env: Env, ctx: ExecutionContext, mode: Exclude<AlsMode, 'reject'>, iterations: number) {
	const nonce = crypto.randomUUID().slice(0, 8);
	await alsPrepare(env);

	const moduleApi: AlsApi = { run, first, getActiveConfig };
	const start = (tag: AlsTag, i: number): Promise<AlsTrace> => {
		const config = alsConfig(env, ctx, tag, nonce);
		if (mode === 'scoped') {
			return (async () => await collegedb(config, () => alsBody(env, tag, tag, nonce, i, moduleApi)))();
		}
		if (mode === 'client') {
			return (async () => alsBody(env, tag, tag, nonce, i, createClient(config)))();
		}
		// the control: what collegedb did before it was scoped
		return (async () => {
			initialize(config);
			return alsBody(env, tag, tag, nonce, i, moduleApi);
		})();
	};

	const traces: Array<PromiseSettledResult<AlsTrace>> = [];
	const tags: AlsTag[] = [];
	const indexes: number[] = [];
	for (let from = 0; from < iterations; from += ALS_WAVE) {
		const wave: Array<Promise<AlsTrace>> = [];
		for (let i = from; i < Math.min(iterations, from + ALS_WAVE); i++) {
			for (const tag of ['a', 'b'] as const) {
				wave.push(start(tag, i));
				tags.push(tag);
				indexes.push(i);
			}
		}
		traces.push(...(await Promise.allSettled(wave)));
	}

	let rejected = 0;
	let checkpoints = 0;
	let configMismatches = 0;
	let readBackMisses = 0;
	let firstError: string | null = null;
	const expected: AlsExpected[] = [];
	traces.forEach((outcome, n) => {
		const tag = tags[n]!;
		const done = outcome.status === 'fulfilled';
		for (const k of [0, 1]) {
			expected.push({ id: alsId(tag, nonce, indexes[n]!, k), tag, done });
		}
		if (outcome.status === 'rejected') {
			rejected++;
			firstError ??= outcome.reason instanceof Error ? outcome.reason.message : String(outcome.reason);
			return;
		}
		const own = JSON.stringify(ALS_SIDES[tag].names);
		for (const seen of outcome.value.checkpoints) {
			checkpoints++;
			if (JSON.stringify(seen) !== own) {
				configMismatches++;
			}
		}
		if (!outcome.value.readBack) {
			readBackMisses++;
		}
	});

	const audit = alsAudit(await alsRowLocations(env, nonce), expected);
	await alsCleanup(env);

	return {
		mode,
		asyncContextAvailable: asyncContextAvailable(),
		hasGetBuiltinModule: typeof (globalThis as { process?: { getBuiltinModule?: unknown } }).process?.getBuiltinModule,
		iterations,
		callbacks: traces.length,
		rejected,
		checkpoints,
		configMismatches,
		readBackMisses,
		misplacedRows: audit.misplaced,
		missingRows: audit.missing,
		crossTalk: rejected + configMismatches + readBackMisses + audit.misplaced + audit.missing,
		firstError
	};
}

/** Without an async context: a different configuration must be refused while another callback runs, the same one must overlap, and the claim must clear. */
export async function alsRejectProbe(env: Env, ctx: ExecutionContext, iterations: number) {
	const nonce = crypto.randomUUID().slice(0, 8);
	await alsPrepare(env);

	const write = async (label: string, i: number) => {
		const id = alsId(label, nonce, i, 0);
		await env.KV.get(`als-probe:${label}`);
		await run(id, ALS_INSERT_SQL, [id, label]);
	};

	let rejectedOk = 0;
	let rejectedWrong = 0;
	let sameConfigOk = 0;
	let sameConfigFailed = 0;
	let afterOk = 0;
	let afterFailed = 0;
	let firstError: string | null = null;
	const expected: AlsExpected[] = [];

	for (let i = 0; i < iterations; i++) {
		let release!: () => void;
		const gate = new Promise<void>((resolve) => (release = resolve));

		const holder = collegedb(alsConfig(env, ctx, 'a', nonce), async () => {
			await gate;
			await write('a', i);
		});
		const different = collegedb(alsConfig(env, ctx, 'b', nonce), () => write('b', i)).then(
			() => 'resolved',
			(error) => (error instanceof CollegeDBError ? String(error.code) : 'other')
		);
		const same = collegedb(alsConfig(env, ctx, 'a', nonce), async () => {
			await gate;
			await write('a2', i);
		});

		const refusal = await different;
		if (refusal === 'CONCURRENT_CONFIG') {
			rejectedOk++;
		} else {
			rejectedWrong++;
			firstError ??= `a different configuration overlapped a running callback and got ${refusal}`;
		}
		release();

		const [heldOutcome, sameOutcome] = await Promise.allSettled([holder, same]);
		if (heldOutcome.status === 'rejected') {
			sameConfigFailed++;
			firstError ??= heldOutcome.reason instanceof Error ? heldOutcome.reason.message : String(heldOutcome.reason);
		}
		if (sameOutcome.status === 'fulfilled') {
			sameConfigOk++;
		} else {
			sameConfigFailed++;
			firstError ??= sameOutcome.reason instanceof Error ? sameOutcome.reason.message : String(sameOutcome.reason);
		}

		const after = await collegedb(alsConfig(env, ctx, 'b', nonce), () => write('bafter', i)).then(
			() => true,
			(error) => {
				firstError ??= error instanceof Error ? error.message : String(error);
				return false;
			}
		);
		after ? afterOk++ : afterFailed++;

		expected.push(
			{ id: alsId('a', nonce, i, 0), tag: 'a', done: heldOutcome.status === 'fulfilled' },
			{ id: alsId('a2', nonce, i, 0), tag: 'a', done: sameOutcome.status === 'fulfilled' },
			{ id: alsId('b', nonce, i, 0), tag: 'b', done: false },
			{ id: alsId('bafter', nonce, i, 0), tag: 'b', done: after }
		);
	}

	const found = await alsRowLocations(env, nonce);
	const audit = alsAudit(found, expected);
	const refusedWrites = [...found.keys()].filter((id) => id.startsWith('b:')).length;
	await alsCleanup(env);

	return {
		mode: 'reject' as const,
		asyncContextAvailable: asyncContextAvailable(),
		hasGetBuiltinModule: typeof (globalThis as { process?: { getBuiltinModule?: unknown } }).process?.getBuiltinModule,
		iterations,
		rejectedOk,
		rejectedWrong,
		sameConfigOk,
		sameConfigFailed,
		afterOk,
		afterFailed,
		refusedWrites,
		misplacedRows: audit.misplaced,
		missingRows: audit.missing,
		firstError
	};
}
