# Validate public DuckDB PR #23848 without competing upstream work

## TL;DR

Public DuckDB issue #23788 and draft PR #23848 identify the same secondary-ART persisted wrong-result mechanism independently reproduced in Linux Fieldwork.

This controlled-fork validation compares the exact public fix head with its parent. It does not modify DuckDB product source or prepare a competing patch.

The lifecycle is split into three process phases:

1. replay a pending WAL in a second engine and close it without touching the indexed table;
2. in a fresh process, query the index with a client context and execute the only explicit SQL `CHECKPOINT`;
3. inspect the resulting file read-only in another fresh process.

The parent must persist a corrupt index during context-free shutdown. The candidate must preserve the WAL, return correct indexed results after binding, checkpoint successfully with an active context, remove the WAL, and remain correct read-only.

## Explain like I'm five

The broken version files an old index card while new entries are still waiting in an inbox.

The draft fix refuses to file the stale card when nobody is present to process the inbox. It leaves the inbox intact. Later, a real reader processes the inbox, updates the card, and files the correct version.

The test checks each step separately so “refused to corrupt” is not mistaken for “finished saving correctly.”

## Why care

A guard that merely skips one dangerous checkpoint could still:

- lose the WAL;
- crash or invalidate the database during shutdown;
- leave an endless WAL that can never checkpoint;
- return a wrong indexed answer before the later checkpoint;
- succeed only because the original writer closes cleanly and heals the file;
- produce a file that later read-only clients still observe incorrectly.

The candidate needs both fail-closed safety and a complete recovery path.

## Public overlap and authority

Public source:

- issue `duckdb/duckdb#23788`;
- draft PR `duckdb/duckdb#23848`;
- exact validation head `f47004f5d2098d5a4ed32e082a1703ccdbf28359`;
- parent resolved at execution as `head^`.

The PR changes checkpoint behavior when:

- checkpoint has no client context;
- an index remains unbound after WAL replay;
- buffered index operations are still pending.

It throws before stale `IndexStorageInfo` is reused. Shutdown uses `TRY_CHECKPOINT`, catches the exception, logs it, and still cleans up, so the intended effect is to preserve the WAL rather than surface a fatal close error.

This repository work is independent validation only. It does not authorize comments, reviews, issues, patches, or other upstream contact.

## Exact engine identity

Each row builds one exact public source commit and resolves:

```text
build/release/src/libduckdb.so
```

through the strict resolver introduced by the source-matrix repair.

The resolved target must be a regular file inside the release tree. Its bytes are copied into two runtime files that must have:

- equal SHA-256;
- different device/inode identities;
- different loaded `duckdb_open_ext` addresses.

This establishes two independently loaded engine images from one exact source build.

## Phase A: context-free shutdown

The writer engine:

1. sets a very large WAL autocheckpoint threshold;
2. creates table `t`;
3. creates secondary ART index `secondary_i`;
4. inserts rows `1..N`;
5. remains open with a pending WAL.

The second engine:

1. opens read-write, replaying the WAL;
2. executes only `SELECT 1`, which does not bind the indexed table;
3. closes, invoking context-free shutdown checkpoint.

The process records database and WAL state before and after that close, prints the receipt, and exits through `os._exit()` so the original writer cannot cleanly close and heal either row.

Expected parent:

```text
WAL present before shutdown: yes
WAL present after shutdown: no
```

Expected candidate:

```text
WAL present before shutdown: yes
WAL present after shutdown: yes
```

## Phase B: bind and explicit checkpoint

A fresh process opens the file read-write with the same exact source build.

It:

1. forces the small index-scan threshold;
2. runs indexed equality queries for the first and terminal values;
3. retains the enabled plan;
4. verifies full row count and ordered values;
5. disables the optimizer and verifies sequential-scan controls;
6. executes SQL `CHECKPOINT` in that live client context;
7. records WAL state before binding, immediately after checkpoint, and after close.

Expected parent:

- no WAL arrives from phase A;
- indexed counts are `0` while sequential counts are `1`;
- explicit checkpoint completes but preserves the already-corrupt index;
- WAL absent after checkpoint.

Expected candidate:

- preserved WAL arrives from phase A;
- the first indexed query binds the unbound index and applies buffered replay operations;
- indexed and sequential counts are `1`;
- explicit checkpoint completes;
- WAL absent after checkpoint and close.

## Phase C: read-only persistence proof

A third process opens the resulting database read-only.

Both rows must preserve:

- full count `N`;
- ordered values `1..N`;
- correct optimizer-disabled equality counts;
- no WAL before or after read-only inspection.

Expected parent:

- optimizer-enabled first and terminal indexed counts remain `0`.

Expected candidate:

- optimizer-enabled first and terminal indexed counts remain `1`.

This proves candidate correctness survives beyond the binding/checkpoint process.

## Fail-closed classification

`classify_lifecycle()` accepts only two complete shapes:

- `parent-corrupt-shutdown-checkpoint`;
- `candidate-preserves-wal-and-heals-after-bind`.

Common controls must all pass first:

- full rows and order;
- sequential-scan counts;
- explicit checkpoint execution;
- WAL removal by the active-context checkpoint;
- no WAL during final read-only inspection.

Ambiguous or partially correct combinations fail instead of being rounded toward either conclusion.

Unit controls cover both expected shapes, a failed sequential control, and an ambiguous mixed result.

## Evidence retained

Each hosted row retains:

- requested revision and exact source SHA;
- exact public fix head;
- library resolution metadata and hashes;
- phase A JSON/stderr;
- phase B JSON/stderr;
- phase C JSON/stderr;
- final classification;
- database and any surviving WAL.

## Evidence boundary

This validates one exact draft head and parent under the two-engine, secondary-ART, pending-WAL lifecycle.

It does not prove:

- every index type or index mutation;
- updates, deletes, composite keys, larger datasets, or concurrent transaction variants;
- behavior if logging itself fails;
- every detach or attached-database checkpoint path;
- absence of performance or WAL-retention regressions at scale;
- that the public draft will merge unchanged;
- authority to contact upstream.

If the public PR head moves, the workflow fails before building rather than silently validating a different candidate.

## Disposition

`INDEPENDENT PARENT/EXACT VALIDATION` until both hosted rows pass and artifacts prove the two distinct lifecycle classifications.

No product patch, merge recommendation to upstream, or external contact is included.
