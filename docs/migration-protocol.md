Key migration protocol
======================

This is the contract for moving one keyed record from one shard to another. The implementation is in `src/migrations.ts`; the tests that hold it to this document are `tests/migration-journal.spec.ts`, plus the real-concurrency harness in `tests/helpers/concurrent-sqlite.ts`.

Anything marked UNPROVEN has no passing test behind it. It is written down so nobody reads it as a guarantee.

What the protocol promises
--------------------------

An interrupted move converges to one authoritative owner. Every update or delete that a router-issued builder call acknowledged is on that owner afterward. A router-issued write from a stale caller cannot create a row on a shard the key has left. A write that cannot tell who owns the key is refused with the retryable `MIGRATION_CUTOVER_PENDING`; it is never acknowledged.

What it does not promise
------------------------

There is no atomicity across shards. A mutation on shard A is never atomic with a journal write on shard B, and nothing here pretends otherwise. Durable journal state is the recovery mechanism.

The protocol does not cover raw SQL issued outside CollegeDB, and it does not cover raw `run()` or `all()` statements, which the planner routes but does not rewrite. Those can still write to a shard the key has left.

The journal moves one table row at a time. Moving an organisation's related rows from several tables is an application procedure built from several moves. The key mapping is per key, not per table, so a one-table move strands the key's rows in other tables unless the caller moves them too.

The journal is crash recovery. It is not a backup, and rollback is a new explicit move.

Configuration
-------------

`journalShard` names the shard binding that holds the journal tables. `journalScope` (default `default`) partitions journals that share one database. With more than one shard and no `journalShard`, `reassignShard`, `rebalance` and `recoverMigrations` throw `JOURNAL_NOT_CONFIGURED`. A single-shard config uses its only shard. `initialize` rejects a `journalShard` that is not a configured binding (`INVALID_JOURNAL_SHARD`).

`PROVEN_JOURNAL_DIALECTS` is `sqlite`, `postgres` and `mysql`. `sqlite` includes D1 and a raw binding with no `dialect`. `mysql` covers MariaDB, which shares the dialect value, so both engines had to pass before it joined the list. A move where the source, destination or journal shard speaks any other dialect throws `MIGRATION_UNSUPPORTED_DIALECT`, and builder writes on such a shard keep their unguarded statement. Each proven dialect has a concurrency test with a control arm behind it; see Per-dialect serialization. A move also needs its source shard to declare `atomicBatch`, and a builder write on Postgres, MySQL or MariaDB needs the same; a shard that does not declare it is refused with `MIGRATION_NO_ATOMIC_BATCH`. See Atomicity requirements.

Tables
------

On the journal shard:

- `collegedb_migration_active(scope, table_name, primary_key, op_id, claimed_at)`, primary key `(scope, table_name, primary_key)`. A move claims its key by insert. A second claim fails, so two moves of one key cannot run. The row is deleted at a terminal state.
- `collegedb_migration_history(op_id, state, attempt, scope, table_name, primary_key, source, destination, generation, placement_epoch, digest, detail, at)`, primary key `(op_id, state, attempt)`. Append only. The current state of an operation is the highest state it has reached.
- `collegedb_placement_exceptions(scope, hashed_key, added_at)`. The durable record of keys placed off their computed shard. The KV set is a derived copy.
- `collegedb_migration_verification(scope, completed_at, clean)`. The last completed verification pass.

On every data shard:

- `collegedb_retired(scope, table_name, primary_key, pk_column, op_id, generation, moved_to, payload, archived_at, purged_at)`, primary key `(scope, table_name, primary_key)`. One row per key that left this shard. It is the archive of the old row and the tombstone that fences stale writers.
- `collegedb_topology(min_generation)`. The fence. A guarded insert or upsert binds the writer's manifest generation and matches no row while `min_generation` is above it. `pruneEpochs` raises it on every shard before it counts rows. The router tells a raised fence from a tombstone and from an ignored conflict, reloads the manifest once, and then throws `STALE_TOPOLOGY`.
- `collegedb_lock_stripes(stripe)`, on MySQL and MariaDB only. 256 rows for the key locks plus one row, `FENCE_STRIPE`, for the fence lock.

On the journal shard, when `journalShard` is set and the dialect is proven, `collegedb_manifest(scope, revision, generation, body)` holds the placement manifest. Every write to it is a compare-and-swap on `revision`, a lost swap reloads and applies its change again, and five lost swaps throw `PLACEMENT_CONFLICT`. KV holds a derived copy. `pruneEpochs` refuses with `PRUNE_REFUSED` when its proof is incomplete.

The schema uses no partial indexes and no autoincrement, so one definition works on every dialect. Key columns are `VARCHAR(190)` on MySQL so the three-column primary keys fit its index limit. `InMemorySQLDatabase` cannot host these tables or run the cutover; use a real SQL engine for anything that sets `journalShard`.

States
------

An operation moves through `prepared`, `copied`, `verified`, `cutover_pending`, `committed`, `published`, `retired`. `aborted` is the other terminal state. Each state is a journal row written before the work that follows it, and every step can be run again.

A snapshot is the typed encoding of the source row at one moment, plus its SHA-256 digest. A move may take several snapshots. Each attempt is numbered, and the digest of an attempt is journaled in `prepared` before the destination is written, so the journal always knows every row the destination could hold on this operation's behalf.

Steps
-----

1. Check dialects, ensure the sidecar tables exist on the source and destination, and claim the key in `collegedb_migration_active`. A claim held by another operation fails with `MIGRATION_IN_PROGRESS`. A tombstone on the source from another operation means a row exists where the key already left; the move refuses with `MIGRATION_SOURCE_TOMBSTONED`.
2. Read the source row. A missing row ends the operation (`RECORD_NOT_FOUND` when it was the first read). Encode the snapshot. If the row is too wide for the bind-parameter limit (default 100, the D1 limit) and the caller gave no `versionColumn`, refuse with `MIGRATION_ROW_TOO_WIDE` before anything is copied.
3. Journal `prepared` with the digest. Copy the row to the destination. A destination row equal to the snapshot is a retried copy and is kept. A different destination row that this operation journaled is replaced. Any other different row is a conflict: with `onConflict: 'fail'` (the default) the move aborts with `DESTINATION_CONFLICT`; with `'replace'` the row is overwritten. Journal `copied`.
4. Read the destination row back and compare digests. Journal `verified`.
5. Journal `cutover_pending` with the digest. Nothing after this point may assume the source is unchanged.
6. Cutover. One source-shard transaction through `runBatch`, led on Postgres, MySQL and MariaDB by the key lock statements, with two statements, both conditional on the same predicate: insert the archive row into `collegedb_retired`, then delete the source row. The predicate is the primary key plus a null-safe, byte-exact equality of every column to the snapshot (SQLite `IS`, Postgres `IS NOT DISTINCT FROM`, MySQL `<=>`). With a `versionColumn` the predicate is the key plus that column. Because both statements use one predicate inside one transaction, a mismatch writes no archive row and no tombstone. The outcome is read back, not inferred from the batch: the archive row for this operation exists, or it does not.
7. On a miss, take a new snapshot and go back to step 3, up to three recopies. After that, or when the source row is gone, abort. Abort deletes the destination copy only where it still equals a digest this operation journaled. The source stays authoritative, and `reassignShard` throws `MIGRATION_ABORTED` with the reason. The discarded copy is not archived, because after an acknowledged delete a retained copy would defeat the delete.
8. On a hit, journal `committed` with the new generation (one more than the highest generation journaled for the key). Delete tombstones for this key on the destination that belong to other operations, since the key is now home there. Publish the derived state: KV mapping, placement exception, coordinator allocation, caches. Journal `published`.
9. Journal `retired` with a `retire_after` time, then delete the active claim.

Write-ordering invariant
------------------------

Every router-issued write either commits before the successful source cutover and is reflected in the snapshot comparison, or runs after the source is fenced and cannot create a stale source row.

The first case: a write that changes the row before the cutover statement makes the predicate fail, the cutover misses, and the acknowledged write stays on the source, which is still the owner. The second case: after the cutover, the row is gone and the tombstone exists in the same transaction, so a builder write finds either a missing row or the tombstone.

Builder writes (`insertInto`, `insertReturning`, `upsert`, `updateRow`, `patch`, `deleteRow`, `deleteById`) follow these rules when `journalShard` is set and the shard dialect is proven:

- Insert and upsert run as one guarded statement: `INSERT ... SELECT ... WHERE NOT EXISTS (SELECT 1 FROM collegedb_retired WHERE ...) AND (SELECT COALESCE(MAX(min_generation), 0) FROM collegedb_topology) <= ?`. SQLite upserts need the select to end in a `WHERE` before `ON CONFLICT`. MySQL needs `FROM DUAL`.
- When a write changes zero rows, or the driver cannot report a change count, the caller checks the shard-local tombstone. No tombstone means the zero was real (a missing row, or an ignored conflict) and the result is returned. A tombstone means the caller routed on a stale mapping. It drops the cached mapping, resolves again, and retries once on a different shard if there is one. Otherwise it throws `MIGRATION_CUTOVER_PENDING`.
- A write is never acknowledged on an unverified zero-row result.

Reads
-----

Reads need no journal. When a routed read finds nothing on a shard and the shard holds a tombstone for the key, the read follows `moved_to`, up to four hops. The destination holds a verified identical copy by construction, so this is correct at every state from `cutover_pending` on. A shard with no tombstone, the common case, costs one extra local lookup on a miss and nothing on a hit.

This replaces the idea that reads of keys with an unfinished operation resolve through the journal. Following the tombstone is available when the journal is not, and it cannot return a row the cutover did not already copy. If the tombstone lookup itself fails, the read returns what it found.

Recovery
--------

`recoverMigrations()` lists every active claim and resumes each one by state. If the journal cannot be read, nothing proceeds and the call throws.

| State                                                                            | Rule                                                                                                                                                                                                |
| -------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `prepared`, `copied`, `verified`                                                 | Nothing irreversible happened. Start again at step 2 with a fresh snapshot.                                                                                                                         |
| `cutover_pending`, source row absent, archive row for this operation present     | The cutover ran. Read the destination row and compare it with the archive payload. Equal: go to step 8. Different or missing: report `MIGRATION_AMBIGUOUS` and leave the operation for an operator. |
| `cutover_pending`, source row absent, no archive row                             | An acknowledged delete removed the row first. Abort and remove the destination copy.                                                                                                                |
| `cutover_pending`, source row present and its digest equals the journaled digest | The cutover never ran. Run step 6 again.                                                                                                                                                            |
| `cutover_pending`, source row present and changed                                | A writer got in. Abort and remove the destination copy. The source stays authoritative.                                                                                                             |
| `committed`                                                                      | Run step 8 again from the tombstone cleanup.                                                                                                                                                        |
| `published`                                                                      | Run step 9.                                                                                                                                                                                         |
| `retired`, `aborted`                                                             | Only the claim is left. Delete it.                                                                                                                                                                  |

Every rule is covered by a crash test that injects a failure before or after the statement in question.

A claim with no journal history, left by a crash between the claim and the first record, is released only after 60 seconds, so recovery does not release the claim of a move that is starting. `recoverMigrations({ olderThanMs })` also leaves alone any operation that changed more recently than that.

Recovery and a live move may drive the same operation at once. Every step is safe to run twice, and an abort is refused once the operation's archive row exists, which is the physical proof that the cutover ran. Tests run recovery inside a live move after each step. Two recoveries at once are UNPROVEN: they can race on the destination insert and on which snapshot digest gets journaled for an attempt. Run one.

Nothing runs recovery on its own. A crash after the cutover leaves the key readable but refuses its writes with `MIGRATION_CUTOVER_PENDING` until `recoverMigrations()` runs, so schedule it, and run it after any deploy that may have interrupted a move. A claim left by a crash also blocks a second move of that key with `MIGRATION_IN_PROGRESS`.

`rebalance` needs a journal for the same reason. A dry run only reports and does not.

Retention
---------

The default policy is `retire: { afterMs: 7 days }`. `'immediate'` is opt-in and sets the deadline to now. It does not purge anything by itself.

`verifyMigrations()` is observational. It reports unfinished operations, ghost rows (a live row on a shard that also holds a tombstone for the key), and tombstones with no journal record. `verifyMigrations({ repair: true })` runs recovery and removes a ghost only when it equals the archived payload; any other ghost is reported and kept. A completed pass is recorded in `collegedb_migration_verification`.

`purgeRetired()` clears the archive payload of a retired operation. It refuses an operation until its `retire_after` has passed and a verification pass completed after the move finished cleanly. Refusals come back with a reason. The tombstone row stays after a purge so a stale writer is still fenced, which costs one small row per moved key. Restoring an old row is a new explicit move from the archive, never an automatic undo. A key that returns to a shard replaces that shard's older tombstone, and with it the older archive.

Placement exceptions
--------------------

A move records a placement exception only when the destination differs from the key's computed owner. The exception goes into `collegedb_placement_exceptions` with an insert that ignores duplicates, so concurrent adds cannot lose each other. The KV set is rebuilt from that table. Rebuilds in one process run one at a time, and each re-reads the table, so the last rebuild sees every row. Another isolate can still overwrite the KV copy with an older snapshot; `recoverMigrations()` and `verifyMigrations({ repair: true })` rebuild it. Callers without a journal keep the old KV read-modify-write, now serialized within the process.

Topology fence
--------------

Each data shard holds the fence in `collegedb_topology`. A builder write binds the generation of the manifest it routed with, and a guarded insert or upsert matches no row while the fence is above that generation. `pruneEpochs` raises the fence on every shard before its census. The raise runs in one transaction that holds the fence lock exclusively.

A zero-row write then has three possible causes, and the router tells them apart. A raised fence means the manifest is stale: the router reloads it once and throws `STALE_TOPOLOGY` if the write still cannot land. A tombstone follows the builder write rules under Write-ordering invariant. An ignored conflict is a real zero-row result.

`PRUNE_REFUSED` means the prune's proof is incomplete. `PLACEMENT_CONFLICT` means the manifest compare-and-swap lost five times in a row.

Atomicity requirements
----------------------

The cutover depends on `runBatch` rolling back the whole group when any statement fails. An adapter says so with `atomicBatch: true` on its `SQLDatabase`, and sets it only for a `runBatch` whose rollback a test has shown. The move refuses a source shard that does not declare it before it claims or copies anything (`MIGRATION_NO_ATOMIC_BATCH`; `MIGRATION_NO_BATCH` when there is no `runBatch` at all), `recoverMigrations` reports such an operation as failed, and a builder write on Postgres, MySQL or MariaDB refuses the same way. The declaration is about rollback only. It does not make a single connection safe against unrelated statements from the same process, see below.

The sandbox `batch_rollback` scenario runs a batch whose third statement repeats a primary key and checks that the two statements before it left no row. It also checks the declaration against the result: an adapter that declares `atomicBatch` must roll back, and one that rolls back must declare it. These declare it and pass:

- Postgres, MySQL, MariaDB and SQLite (`singleConnection: true`) through the native adapters, and through the Drizzle adapters on Postgres, MySQL, MariaDB and over `bun:sqlite`.
- The Hyperdrive Postgres and MySQL providers, on Docker Postgres, MySQL and MariaDB and through the local Hyperdrive binding in the Cloudflare lane.
- A D1 binding through `toProvider`, on local workerd. The D1 docs contradict themselves on failed-batch rollback, and local workerd D1 is SQLite on workerd, not Cloudflare's production D1. Rollback on production D1 stays UNPROVEN, and the declaration there rests on the local run.

Drizzle over a synchronous driver (`bun:sqlite`, `better-sqlite3`) used to fail this check. Its transaction commits as soon as the callback returns, so an async callback committed before its statements ran and a failed batch left its early rows. The adapter now runs the group inside a callback that stays synchronous until a driver hands back a promise, so a synchronous driver gets one real transaction and an asynchronous one is awaited as before. Drizzle over D1 and libsql over HTTP, which have a native `batch`, still run a batch statement by statement and declare nothing, because their `transaction()` is refused by the backend. A journal move whose source is such a shard refuses.

The Hyperdrive providers treat the client of a request as one connection. A batch takes a gate on it and runs alone, and a plain statement from the same request waits until the batch commits, so nothing slips into the open transaction. The factory must return one connection (a `pg` `Client`, a `mysql2` connection).

The plain Postgres, MySQL and SQLite adapters declared through `singleConnection: true` hold their other statements behind an open batch, through the same gate. A caller `lease` gates only when `singleConnection: true` is also set. Pooled adapters lease a connection per batch and are not gated. The gate is checked with order-logging fakes for `pg.Client` and `mysql2`; the SQLite path ran on a real handle (see UNPROVEN).

A driver must hand back 64-bit integers without loss. The tests use a `node:sqlite` adapter that returns an integer outside the safe range as a `bigint`. A driver that returns it as a rounded number cannot be told apart from a real column value, so the move would compare rounded values. UNPROVEN beyond that adapter.

SQLite equality treats `1` and `1.0` as equal in a column with no declared type. A column with a declared type normalizes both. UNPROVEN for untyped columns.

Per-dialect serialization
-------------------------

SQLite and D1 serialize writers. One statement is atomic, so the guarded insert and the cutover cannot interleave, and statement atomicity is enough. The worker-thread harness checks this on a shared WAL file.

Postgres, MySQL and MariaDB do not serialize writers. A guarded insert can read "no tombstone" before the cutover commits and insert after the row is deleted. An update can also race the cutover's two statements: the archive insert sees the old row, the delete then misses the updated row, and the shard ends with a tombstone beside a live row that holds an acknowledged write. So the cutover and every router builder write run lock statements first, in the same transaction on the same connection, and the second of two contenders waits for the first to commit and then reads committed state. `runCutover` takes the lock for the move, and `runGuardedWrite` takes it for builder writes. The router has to call `runGuardedWrite` for every builder write on a shard whose dialect needs it; a write that skips it is unserialized. Update and delete take the key lock on Postgres, MySQL and MariaDB as well as insert and upsert.

The lock is built from `(scope, table, key)` hashed with 64-bit FNV-1a.

- Postgres runs `SET TRANSACTION ISOLATION LEVEL READ COMMITTED`, then `pg_advisory_xact_lock` on the hash. The isolation statement is required. In a session whose default is REPEATABLE READ or SERIALIZABLE the snapshot is taken at the lock statement, before the wait, and the race run showed ghost rows, inserts acknowledged over an existing key and serialization errors with the lock in place. With the statement, the same sessions passed.
- MySQL and MariaDB run `SELECT ... FOR UPDATE` on one of 256 rows in `collegedb_lock_stripes`, chosen by the hash. A row lock lasts until commit. A named lock would have to be released by hand after the commit, which a `runBatch` group cannot do. Two keys on one stripe wait for each other, which costs concurrency and cannot deadlock, because each transaction takes one stripe lock and takes it first. At the default REPEATABLE READ the `INSERT ... SELECT` guard reads current rows and the race did not show without the lock. Under READ COMMITTED it did. The lock holds the protocol at both.
- A shard that needs the lock and has no `runBatch` refuses builder writes with `MIGRATION_NO_BATCH`, and one whose `runBatch` is not declared atomic refuses with `MIGRATION_NO_ATOMIC_BATCH`. The Hyperdrive providers have a `runBatch` now, so their shards take the lock like any other.
- The builder `upsert` on MySQL and MariaDB is `INSERT ... ON DUPLICATE KEY UPDATE col = VALUES(col)`. The row-alias form is MySQL only, and `VALUES()` ran on both engines. MySQL reacts to a conflict on any unique key of the table, so name the key you mean in `conflictColumns`. The affected-row count is 1 for an insert, 2 when the update changed the row and 0 when it changed nothing, and the router reads any positive count as acknowledged.

The fence needs a lock of its own. A guarded insert checks the fence inside its own statement, so on Postgres, MySQL and MariaDB a writer that read the old fence could still commit after `pruneEpochs` raised the fence and ran its census, and the census then missed the row. A sandbox race showed it on Postgres: 26 of 150 stale-generation writers landed a row after the census read none, with the key lock in place. Every builder write now takes a shared fence lock after its key lock, and the fence raise runs in one transaction that takes the same lock exclusively. The raise waits for writers already in flight and holds back the ones that follow, so a writer either commits before the raise or reads the new fence. Postgres uses `pg_advisory_xact_lock_shared` and `pg_advisory_xact_lock` on one constant id. MySQL and MariaDB lock the `FENCE_STRIPE` row with `LOCK IN SHARE MODE` and `FOR UPDATE`, which both engines accept and which holds under READ COMMITTED. A fence raise on a shard without an atomic `runBatch` refuses, so `pruneEpochs` refuses the prune.

- Postgres infers untyped parameters in an `INSERT ... SELECT` list from the target column. The guarded insert needs no casts: BIGINT, BOOLEAN, BYTEA, JSONB, NUMERIC, UUID, TIMESTAMPTZ, REAL and NULL all inserted on Postgres 18.

What the tests establish
------------------------

The table lists what the suite runs. Anything not listed is UNPROVEN.

| Claim                                                                                                                                                                | Where it is checked                                          |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ |
| Fault at every operation of a move, before and after the operation, recovers to one owner with no lost acknowledged write                                            | `tests/migration-journal.spec.ts` fault sweep                |
| Crash right after the source cutover commits, before the journal advances                                                                                            | same file, named case                                        |
| Zero-row write in the cutover window gets `MIGRATION_CUTOVER_PENDING`                                                                                                | same file                                                    |
| Two concurrent moves of one key, exactly one wins                                                                                                                    | same file                                                    |
| Move back onto a shard that holds a tombstone                                                                                                                        | same file                                                    |
| Journal unavailable, KV unavailable during publish                                                                                                                   | same file                                                    |
| Update, delete, insert and upsert racing the cutover in separate threads on one WAL file                                                                             | same file, through `tests/helpers/concurrent-sqlite.ts`      |
| Blob, 64-bit integer, NULL against empty string, unicode survive the archive and the cutover predicate                                                               | same file                                                    |
| Concurrent `addPlacementException` loses nothing                                                                                                                     | same file                                                    |
| Builder writes honor the tombstone                                                                                                                                   | `tests/crud.spec.ts`                                         |
| Statement shapes per dialect, the key lock statements, the lock hash and the MySQL stripe table                                                                      | `tests/query.spec.ts`                                        |
| The cutover and `runGuardedWrite` run the lock first in one batch; a locking dialect with no `runBatch` refuses                                                      | `tests/migration-journal.spec.ts`, The per-key lock          |
| Update, delete, insert and upsert racing the cutover on Postgres, MySQL and MariaDB, with control arms                                                               | sandbox scenario `stale_route_write`, not in vitest          |
| A journaled move on each engine, then tombstone-checked writes and refused stale-route writes                                                                        | sandbox scenario `journal_move`                              |
| A failing batch rolls back the whole group, and `atomicBatch` agrees with the result                                                                                 | sandbox scenario `batch_rollback`                            |
| `atomicBatch` on every adapter that holds a transaction, a refused move and refused guarded write without it, the Hyperdrive gate, Drizzle over a synchronous driver | `tests/providers.spec.ts`, `tests/migration-journal.spec.ts` |
| MySQL and MariaDB upsert statements, the fence lock statements and the fence raise batch                                                                             | `tests/query.spec.ts`, `tests/migration-journal.spec.ts`     |
| Manifest compare-and-swap, the fence race with a control arm, `pruneEpochs` dry run then real, and the DDL twice, on Postgres, MySQL, MariaDB and SQLite             | sandbox scenario `topology_cas`                              |
| Upsert on MySQL and MariaDB over a moved key and racing the cutover                                                                                                  | sandbox scenarios `journal_move`, `stale_route_write`        |
| The single-connection gate on SQLite, `pg.Client` and `mysql2`: plain statements wait behind an open batch and batches run one at a time                             | `tests/providers.spec.ts`, `Single-connection batch gate`    |

What the runs measured
----------------------

The fault sweep injects a failure before and after each of the 22 operations a clean move makes, 44 crash points, and checks each one for a readable key before recovery and one owner after it. The race harness has two control arms, a cutover that compares only the key and an insert without the tombstone guard, and both must report violations or the suite fails. A soak of 1,000 iterations for each of the four write kinds over three seeds found no violation.

The sandbox scenario `stale_route_write` races update, delete, insert and upsert writers against the cutover on separate pool connections. Each iteration launches the writer at a random protocol step with a random delay of up to 7 ms, and holds the cutover or the writer open for 4 ms before its commit in two of every three iterations, so the vulnerable window is hit often. A soak of 500 iterations per write kind on Postgres, MySQL and MariaDB found no violation in the protocol arm, at the default isolation and, on MySQL and MariaDB, under READ COMMITTED as well. Upsert is in that soak on MySQL and MariaDB since it gained its `ON DUPLICATE KEY UPDATE` form: 500 iterations each at both isolations, no violation. On Postgres, 100 iterations per kind under REPEATABLE READ and under SERIALIZABLE session defaults also found none, once the transaction pins READ COMMITTED.

The control arm strips the lock statements and ran 250 iterations per kind. It reported 164, 44, 280 and 416 violations on Postgres (update, delete, insert, upsert), 220, 56 and 372 on MySQL under READ COMMITTED, and 148, 32 and 67 on MariaDB under READ COMMITTED. MySQL and MariaDB at the default REPEATABLE READ reported none, which is why their control runs under READ COMMITTED; the harness fails a run whose required control stays silent. In the soak that added the fence lock, the control upsert reported 540 violations on MySQL and 213 on MariaDB under READ COMMITTED.

The sandbox scenario `topology_cas` runs on Postgres, MySQL, MariaDB and SQLite, through the native adapters and through Drizzle, at 500 iterations per race kind in the protocol arms and 250 in the control arms. It creates the manifest, sidecar and journal tables twice on every shard, so the DDL is idempotent. Two or four clients publish the next epoch through `commitJournalManifest` on a fresh scope each iteration, and each round must end with consecutive epochs, one revision per publisher and no publisher returning without a write. A stale-generation writer races the fence raise and the census that follows it, in three orders (writer first, fence first, together) with the writer held open for 4 ms in one iteration of three, and a row that exists although the census read none counts as a violation. Last, `pruneEpochs` runs on three shards with 40 rows, as a dry run that must change nothing, as a real run that must keep an epoch rows still depend on, and as a real run after those keys are read, which removes the old epoch with every key still resolving and a generation 1 writer turned away.

| Engine and adapter                                      | CAS protocol violations | CAS control violations (250) | Fence protocol violations | Fence controls (250 each)                                     |
| ------------------------------------------------------- | ----------------------- | ---------------------------- | ------------------------- | ------------------------------------------------------------- |
| Postgres, native and Drizzle                            | 0 of 500                | 250                          | 0 of 500                  | no fence lock 39 and 36, no fence 55 and 55                   |
| MySQL, native and Drizzle, default and READ COMMITTED   | 0 of 500                | 250                          | 0 of 500 at each          | no fence lock at READ COMMITTED 37 and 35, no fence 76 and 76 |
| MariaDB, native and Drizzle, default and READ COMMITTED | 0 of 500                | 250                          | 0 of 500 at each          | no fence lock at READ COMMITTED 35 and 36, no fence 76 and 76 |
| SQLite, native and Drizzle                              | 0 of 500                | 250                          | 0 of 500                  | no fence 67 and 67 (the lock does not apply)                  |

The first run of the fence race on Postgres, before the fence lock existed, found 26 violations in 150 protocol iterations. That is the defect the lock fixes. The Hyperdrive providers ran the same scenario, `journal_move` and `batch_rollback` at 60 iterations with one connection per client and no violation; their stale-route race is skipped, since a gate runs a batch alone and the writers never race.

Values with no portable encoding, such as a JSON object column returned as an object or a `Date`, make a move refuse with `MIGRATION_UNSUPPORTED_VALUE` before anything is copied. Those are UNPROVEN on the real engines, as are these:

- Column types beyond `VARCHAR` and `BIGINT` in the race on Postgres and MySQL, including JSON, bytea and timestamps.
- A `BIGINT` above 2^53 on MySQL, where `mysql2` returns a rounded number unless `supportBigNumbers` is set.
- A MySQL or MariaDB session under SERIALIZABLE, and a second writer racing the cutover on any engine; the harness races one.
- KV convergence across isolates. Local workerd KV is not eventually consistent, so no probe built on it can show convergence.
- Rollback of a failing batch on production D1, as above.
- A native-batch Drizzle driver (D1, libsql over HTTP) as a journal source. It declares nothing and refuses. A batch through `db.batch` with `db.run` statements might work and has not been tried.
- The `pg.Client` and `mysql2` single-connection gate on a real Postgres or MySQL server. Only order-logging fakes have run it.
- The manifest swap and the fence under a second writer on the Cloudflare lane. Local D1 is one SQLite file, so `topology_cas` is skipped there.
- A MySQL or MariaDB upsert whose table has a second unique key. `ON DUPLICATE KEY UPDATE` reacts to any of them.
- The fallback that settles a write on a driver that reports no change count, by reading the row back.
- First journal adoption. When KV returns no manifest on the first cold start after `journalShard` is set, the isolate publishes epoch 1 into an empty journal.
- Loss of KV mappings for keys moved off their computed owner. Nothing here repairs them.
- `isPinned`, used by `pruneEpochs`, may read a mapping from the mapper's cache window. Run a prune from a fresh process.
- The placement probe ran on `node:sqlite` and the in-memory provider only. It has not run on Postgres, MySQL, MariaDB, D1 or workerd.
- Writes keyed on a column other than the table's key column after a topology change. The probe checks the key column only.
- Concurrent writers to one key during a topology change. The probe and the write are not atomic.
- Manifest and KV refresh across isolates with real KV and D1.
