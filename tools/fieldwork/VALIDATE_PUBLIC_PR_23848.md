# Validate public DuckDB PR #23848 without competing upstream work

## TL;DR

Public DuckDB issue #23788 and draft PR #23848 describe the same secondary-ART persisted wrong-result mechanism independently reproduced in Linux Fieldwork.

This controlled-fork validation compares exact public fix head `f47004f5d2098d5a4ed32e082a1703ccdbf28359` with its parent. It changes no DuckDB product source and prepares no competing upstream patch.

The lifecycle is split into four process phases:

1. replay a pending WAL and perform one context-free shutdown without touching the indexed table;
2. repeat that context-free shutdown again, still without binding the index;
3. query the index with a live client context and execute the only explicit SQL `CHECKPOINT`;
4. inspect the final file read-only in another fresh process.

The parent must persist a corrupt index during the first shutdown. The candidate must preserve the WAL through both unbound shutdowns, return correct indexed results after binding, checkpoint successfully with an active context, remove the WAL, and remain correct read-only.

## Explain like I'm five

The broken version files an old index card while new entries are still waiting in an inbox.

The draft fix refuses to file the stale card when nobody is present to process the inbox. A second unattended close must refuse again, not quietly lose the inbox. Later, a real reader processes the inbox and files the corrected card.

## Why care

A guard that merely skips one dangerous checkpoint could still:

- lose the WAL on a later shutdown;
- crash or invalidate the database during close;
- create a recurring WAL that can never heal;
- return a wrong indexed answer before the later checkpoint;
- appear correct only because the original writer closes cleanly;
- produce a file that later read-only clients still observe incorrectly.

The candidate needs both repeatable fail-closed safety and a complete recovery path.

## Public overlap and authority

Public source:

- issue `duckdb/duckdb#23788`;
- draft PR `duckdb/duckdb#23848`;
- exact validation head `f47004f5d2098d5a4ed32e082a1703ccdbf28359`;
- parent resolved at execution as `head^`.

The draft fix changes checkpoint behavior when:

- checkpoint has no client context;
- an index remains unbound after WAL replay;
- buffered index operations are pending.

It throws before stale `IndexStorageInfo` is reused. Shutdown uses `TRY_CHECKPOINT`, catches the exception, logs it, and still cleans up, so the intended effect is WAL preservation rather than a fatal close error.

This work is independent validation only. It does not authorize comments, reviews, issues, patches, or other upstream contact.

## Exact engine identity

Each row builds one exact public source commit and strictly resolves:

```text
build/release/src/libduckdb.so
```

The resolved target must be a regular file inside the release tree. Its bytes are copied into two runtime files with:

- equal SHA-256;
- different device/inode identities;
- different loaded `duckdb_open_ext` addresses.

## Phase A: first context-free shutdown

The writer engine:

1. sets a very large WAL autocheckpoint threshold;
2. creates table `t`;
3. creates secondary ART index `secondary_i`;
4. inserts rows `1..N`;
5. remains open with a pending WAL.

The second engine opens read-write, executes only `SELECT 1`, and closes. That query deliberately does not bind the indexed table.

The process records database/WAL state, prints the receipt, and exits through `os._exit()` so the original writer cannot heal either row.

Expected parent:

```text
WAL before shutdown: present
WAL after shutdown: absent
```

Expected candidate:

```text
WAL before shutdown: present
WAL after shutdown: present
```

## Phase A2: repeat shutdown without binding

A fresh process again opens read-write, executes only `SELECT 1`, and closes.

Expected parent:

```text
WAL before repeat: absent
WAL after repeat: absent
```

Expected candidate:

```text
WAL before repeat: present
WAL after repeat: present
```

This proves the draft guard remains fail-closed across repeated context-free closes until a client actually binds the index.

## Phase B: bind and explicit checkpoint

A fresh process:

1. forces the small index-scan threshold;
2. queries first and terminal indexed values;
3. retains the enabled plan;
4. verifies full row count and ordered values;
5. disables the optimizer and verifies sequential-scan controls;
6. executes the only SQL `CHECKPOINT` in the lifecycle;
7. records WAL state before bind, after checkpoint, and after close.

Expected parent:

- no WAL arrives;
- indexed counts are `0` while sequential counts are `1`;
- explicit checkpoint completes but preserves the already-corrupt index;
- WAL remains absent.

Expected candidate:

- preserved WAL arrives;
- the first indexed query binds the unbound index and applies buffered replay operations;
- indexed and sequential counts are `1`;
- active-context checkpoint removes the WAL;
- close completes with no WAL.

## Phase C: read-only persistence proof

A fourth process opens read-only.

Both rows must preserve:

- full count `N`;
- ordered values `1..N`;
- correct optimizer-disabled equality counts;
- no WAL before or after read-only inspection.

Expected parent indexed counts remain `0`. Expected candidate indexed counts remain `1`.

## Fail-closed classification

`classify_lifecycle()` accepts only:

- `parent-corrupt-shutdown-checkpoint`;
- `candidate-preserves-wal-and-heals-after-bind`.

Common controls must pass first:

- full rows and order;
- sequential-scan counts;
- explicit checkpoint execution;
- WAL removal by active-context checkpoint;
- no WAL during final read-only inspection.

The parent shape requires WAL loss on both shutdown observations. The candidate shape requires WAL preservation through both unbound closes. Mixed or partially correct combinations fail.

Unit controls cover both positive shapes, failed sequential control, ambiguous indexed state, and candidate WAL loss on the repeated close.

## Evidence retained

Each hosted row retains:

- requested revision and exact source SHA;
- exact public fix head;
- library resolution and hashes;
- first-shutdown JSON/stderr;
- repeated-shutdown JSON/stderr;
- bind/checkpoint JSON/stderr;
- read-only JSON/stderr;
- final classification;
- database and any surviving WAL.

## Evidence boundary

This validates one exact draft head and parent under the two-engine, secondary-ART, pending-WAL lifecycle.

It does not cover every index type or mutation, deletes/updates/composite keys, larger datasets, concurrency variants, every detach path, logging failure, or performance/WAL-retention scale. It does not prove the draft will merge unchanged or authorize upstream contact.

If the public PR head moves, the workflow fails before building.

## Disposition

`INDEPENDENT PARENT/EXACT VALIDATION` until both hosted rows pass and artifacts prove the two complete lifecycle classifications.

No product patch, upstream merge recommendation, or external contact is included.
