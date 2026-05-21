# CLAUDE.md — CollegeDB (earth-app/CollegeDB)

This repository implements CollegeDB: a Cloudflare-oriented D1 sharding router and tools for running, testing, and benchmarking multi-shard SQLite-style databases (D1 / Drizzle / other SQL backends) with KV-backed primary-key mappings and a Durable Object shard coordinator.

Purpose of this file

- Give Claude (and new contributors) an immediate orientation to the repo and development workflows.
- Capture the commands and CI behaviors you want automated or respected.
- Provide safe workflows Claude should follow before making changes.

**Project Snapshot**

- Primary language: TypeScript (ESM), targeted for Node and Cloudflare Workers runtime.
- Build system: Bun + TypeScript; tests use Vitest.
- Documentation: TypeDoc-generated HTML under `typedoc/` (script: `docs:build`).
- Packaging: distributed as an npm package (`dist/`), version in `package.json`.

**High-level architecture**

- `src/` contains library code that provides the API surface (`index.ts` exports).
  - `router.ts` (main router implementation, exported API surface in `src/index.ts`).
  - `durable.ts` implements `ShardCoordinator` — a Cloudflare Durable Object used to coordinate shard allocation and stats.
  - `providers.ts` contains adapters for multiple backend types (D1, Postgres, MySQL, SQLite, Redis, Valkey, Drizzle, etc.).
  - `providers-memory.ts` includes in-memory mock providers used in tests.
  - `kvmap.ts` holds KV mapping utilities.
  - `migrations.ts` provides migration helpers for schema creation and migration across shards.
  - `errors.ts`, `types.ts` contain error classes and shared types.

**Sandbox environment**

- `sandbox/worker.ts` is a runnable Cloudflare-style sandbox harness exposing convenience HTTP endpoints for:
  - Schema initialization (`POST /init`), seeding (`/api/benchmark/seed-users`), CRUD endpoints for `users` and benchmark/migration helpers.
  - The sandbox supports multiple profiles (`native`, `drizzle`, `nuxthub`) selecting provider compatibility layers.
- Sandbox scripts and Docker compose exist under `sandbox/` — used by CI sandbox benchmarks and local benchmarking.
- Commands to exercise the sandbox locally (from repo root):
  - `bun run test:memory` — in-memory quick run
  - `bun run test:sandbox` — run sandbox benchmarks (CI also runs this)
  - For specific profiles: `bun run test:sandbox:drizzle`, `bun run test:sandbox:nuxthub`, etc.

**Tests**

- Tests are under `tests/` (Vitest). Key patterns:
  - `tests/all.spec.ts` contains integration-style tests using `MockD1Database`, `MockKVNamespace`, and `MockDurableObjectNamespace` to simulate runtimes.
  - `tests/*.spec.ts` exercise `migrations`, `providers`, `durable` logic and router behaviors.
- Common test commands:
  - `bun test` (runs tests once)
  - `bun run test:coverage` (collect coverage)
  - `bun run test:memory` (sandbox/memory quick-run harness)

**Documentation generation**

- Docs are generated with TypeDoc: `bun run docs:build` runs `typedoc src --out ./typedoc`.
- CI build job runs the same command and uploads `typedoc/` as a build artifact.
- Deploy docs: `.github/workflows/docs.yml` calls `bash typedoc.sh ${GITHUB_SHA::7}` — `typedoc.sh` performs publish of `typedoc/` (Pages or other hosting); treat `typedoc.sh` as the canonical publish script.

**Continuous Integration (see `.github/workflows`)**

- `build.yml` (Build and Test):
  - Triggers: push & PR on `main`, `master`, `develop`.
  - Jobs: `format` (Prettier), `test` (tsc + tests), `build` (build + docs:build + upload typedoc artifact).
- `coverage.yml` (Coverage & Sandbox):
  - Triggers: push & pull_request.
  - Jobs: `coverage` (run tests, upload to Codecov; also produces coverage artifacts) and `sandbox` (runs sandbox benchmarks; requires Docker + Docker Compose; uploads `sandbox/results/` artifacts).
  - Notes: `sandbox` job verifies Docker availability and runs `bun run test:sandbox`. It collects `sandbox/results/latest.md` into the job summary.
- `docs.yml` (Deploy Documentation):
  - Triggers: push to `master` and manual dispatch.
  - Runs `bun run docs:build` then `typedoc.sh` to deploy.
- `release.yml` (Manual release):
  - Manual trigger (workflow_dispatch). Verifies tests, builds, publishes to npm and GitHub Packages, and creates a GitHub release using a changelog builder.
  - Publishing uses `NPM_TOKEN` / `GITHUB_TOKEN` secrets — Claude must never write or expose secrets.

**What Claude must do before changing code**

- Never publish releases or change CI secrets; these are privileged operations. If asked, create a PR and note required secret values for maintainers to set.
- Before modifying code that affects runtime behavior (anything under `src/`):
  1. Run the test suite locally: `bun test` and `bun run test:coverage` for coverage-sensitive changes.
  2. For changes touching migrations or schema, run sandbox harnesses: `bun run test:memory` and/or `bun run test:sandbox` (Docker required for some sandbox profiles).
  3. If the change affects public API (exports from `src/index.ts`), update `CHANGELOG.md` / release notes and ensure TypeScript types compile: `bunx tsc --noEmit`.
  4. Provide a concise test plan in PR description and include reproducer steps (commands to run locally). If the change modifies durability or migration logic, include migration safety checks and a rollback plan.
- For performance / architecture changes:
  - Produce a short design summary before code changes.
  - Add targeted unit and integration tests replicating the performance scenario.

**Standard workflow for Claude when asked to implement changes**

- Use the explore-plan-code-commit workflow: explore repository → propose a focused plan (short bullets) → implement minimal change with tests → run tests locally → open a PR with description and test evidence.
- If a requested change is large or architectural, ask clarifying questions and generate a design doc before any code edits.

**Common commands (copy-paste friendly)**

- Install deps: `bun install --frozen-lockfile`
- Type check: `bunx tsc --noEmit`
- Run tests: `bun test` or `bun run test:coverage`
- Run sandbox memory harness: `bun run test:memory`
- Run sandbox benchmarks (requires Docker for some profiles): `bun run test:sandbox`
- Build library & types: `bun run build`
- Build docs: `bun run docs:build`
- Format: `bun run prettier:check` / `bun run prettier`

**File-level notes and cautions**

- `src/durable.ts` (ShardCoordinator): centralized coordination logic — changes may affect live shard allocations. Add tests and review Durable Object storage usage.
- `src/providers.ts`: many adapter contracts; avoid breaking public provider signatures without a major version bump.
- `migrations.ts`: destructive operations (dropSchema, flush) exist — mark such operations with clear warnings in any PR touching them.
- `sandbox/worker.ts`: convenient for local testing and benchmarks; changes here affect CI benchmarks and should be validated by running `bun run test:sandbox`.
- `typedoc.sh`: deployment script for docs; it is invoked by `docs.yml`. Do not hard-code secrets in this script.

**Testing & Quality expectations**

- New code should come with unit tests or integration tests as appropriate to the change's scope.
- Keep changes small and iterative: prefer many tiny PRs with focused scope over large, invasive commits.
- Run `bunx tsc --noEmit` to ensure type-safety; CI runs this as part of `test` job.
- Prettier formatting is enforced in CI (`build.yml` format job). Use `bun run prettier` to autofix.

**Documentation & maintainability**

- TypeDoc is the canonical code documentation generator. Update JSDoc comments where public APIs change.
- `typedoc/` is built in CI and shipped as an artifact; the `docs.yml` workflow deploys these artifacts to the project's pages via `typedoc.sh`.

**Security & secrets**

- Never add secrets or credentials to `CLAUDE.md` or commit them to the repo.
- Publishing, Codecov tokens and npm tokens are stored in GitHub secrets. If a change requires new secrets, mention this in the PR and do not attempt to set secrets.

**Recommended small improvements for maintainers** (suggestions Claude may propose on PRs)

- Add a `CONTRIBUTING.md` that codifies the local sandbox run steps (Docker prerequisites) and release checklist.
- Add a short `docs/` index that links to `typedoc/` artifacts and sandbox benchmark reports in `sandbox/results/` for easier discovery.
- Add a `scripts/verify-ci.sh` helper that runs the CI job sequence locally (format → type-check → tests → build docs) for maintainers.

**Quick contact / context hints**

- Primary author: Gregory Mitchell (see `src/index.ts` header comment).
- Keep messages concise in PRs: what you changed, why, how to test, and risk/rollout notes.

**Workflow guidance for Claude**

- When asked to fix or modify code: always (a) run tests and type-check locally, (b) propose the exact commands you will run in the PR description, (c) avoid pushing release or secret changes — create a PR and request human approval for publishing.
- Use subagents (or a separate conversation) for long, multi-phase tasks like performance reviews vs. code changes so context doesn't bleed between phases.
