/**
 * @fileoverview Cold-start cost of the library: minified bundle size and module import time.
 *
 * Builds the entry point into a fresh temp directory, then imports the bundle in
 * {@link RUNS} fresh bun processes and reports the median import time. Set
 * `COLLEGEDB_SRC` to measure another checkout, as in `micro.ts`.
 *
 * ```
 * bun run scripts/bench/cold-start.ts
 * COLLEGEDB_SRC=/tmp/baseline/src/index.ts bun run scripts/bench/cold-start.ts --json
 * ```
 *
 * @author Gregory Mitchell
 * @since 1.3.0
 */

import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { gzipSync } from 'node:zlib';

const SRC = process.env.COLLEGEDB_SRC ?? new URL('../../src/index.ts', import.meta.url).pathname;
const RUNS = 15;
const JSON_OUT = process.argv.includes('--json');

const CHILD = `const started = performance.now();
await import(process.argv[2]);
console.log(performance.now() - started);
`;

const dir = mkdtempSync(join(tmpdir(), 'collegedb-cold-start-'));

try {
	const build = await Bun.build({
		entrypoints: [SRC],
		outdir: dir,
		target: 'node',
		format: 'esm',
		minify: true
	});

	if (!build.success) {
		throw new Error(build.logs.map(String).join('\n'));
	}

	const built = build.outputs[0]!.path;
	const bytes = readFileSync(built);
	const gzipBytes = gzipSync(bytes).length;

	const child = join(dir, 'child.mjs');
	writeFileSync(child, CHILD);
	const url = pathToFileURL(built).href;

	const samples: number[] = [];
	for (let run = 0; run < RUNS; run++) {
		const proc = Bun.spawnSync([process.execPath, child, url], { stdout: 'pipe', stderr: 'pipe' });
		if (proc.exitCode !== 0) {
			throw new Error(`import failed (exit ${proc.exitCode}): ${proc.stderr.toString()}`);
		}

		const ms = Number(proc.stdout.toString().trim());
		if (!Number.isFinite(ms)) {
			throw new Error(`unexpected child output: ${proc.stdout.toString()}`);
		}
		samples.push(ms);
	}

	samples.sort((left, right) => left - right);
	const medianMs = samples[Math.floor(samples.length / 2)]!;

	if (JSON_OUT) {
		console.log(
			JSON.stringify({ src: SRC, rawBytes: bytes.length, gzipBytes, importMs: { runs: RUNS, median: medianMs, samples } }, null, 2)
		);
	} else {
		console.log(`source: ${SRC}`);
		console.log(`raw bytes:   ${bytes.length}`);
		console.log(`gzip bytes:  ${gzipBytes}`);
		console.log(
			`import ms:   ${medianMs.toFixed(2)} median of ${RUNS} fresh processes ` +
				`(min ${samples[0]!.toFixed(2)}, max ${samples[samples.length - 1]!.toFixed(2)})`
		);
	}
} finally {
	rmSync(dir, { recursive: true, force: true });
}
