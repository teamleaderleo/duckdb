# Secondary ART source-matrix packaging and persistence triggers

## TL;DR

The predecessor source-identity matrix established a persisted secondary-index false negative on an exact current-source build, but it used the word *checkpoint* for a lifecycle that never executed SQL `CHECKPOINT`.

It also left three historical rows unexecuted because the build produced `build/release/src/libduckdb.so` as a symlink to a versioned regular file, while the workflow searched only regular files named exactly `libduckdb.so`.

This successor repairs those two evidence boundaries without changing DuckDB product source:

1. resolve and dereference the exact release-library path;
2. rerun only the three packaging-blocked historical rows;
3. compare four isolated current-source triggers:
   - read-write open/close only;
   - `SELECT 1`;
   - indexed read;
   - explicit SQL `CHECKPOINT`.

## Explain like I'm five

The old test looked for a book only if the shelf label itself was a physical book. Some releases use a shelf label that points to the real versioned book, so the test said the book was missing even though the build succeeded.

The old test also called several door-opening and reading steps *checkpointing* without ever pressing the actual `CHECKPOINT` button.

The repair follows the exact shelf label to the real book and labels every button separately.

## Why care

A wrong-result bisect is useful only when every row actually ran. A persistence explanation is useful only when the named operation really happened.

Without these repairs:

- v1.4.0 and both ARTBuilder rows look like build failures even though compilation completed;
- an open/recovery/close transition can be misreported as explicit checkpoint serialization;
- a fresh read-write inspector can accidentally become the operation that changes the file.

## Predecessor evidence

Controlled fork PR #11 exact head:

```text
edabe5173da0ca01a052131f5b7125766e179d50
```

Workflow run:

```text
30627971958
```

Successful current-source row `2c9e51aa33dd07e928edae66304430aeb038edd7` established:

- one source build copied to two byte-identical libraries;
- different inodes and different `duckdb_open_ext` addresses;
- persisted optimizer-enabled count `0` for a row that exists;
- optimizer-disabled sequential-scan count `1`;
- full count and ordered table rows intact;
- index scan selected in the wrong-result path.

The source probe's second engine performed repeated read-write `open → query → close` cycles. It did **not** execute SQL `CHECKPOINT`.

The Node::Free pair did not separate:

- parent `582bf19845d91b0c1bf0fa65641617f84237643c` — affected;
- exact `65db3ee757413a6ad09504595e65983da134a80d` — affected.

The first bad source change is earlier.

## Packaging failure owner

The three blocked rows completed their builds and produced a versioned shared library plus symlinks such as:

```text
build/release/src/libduckdb.so -> libduckdb.so.1.4
build/release/src/libduckdb.so.1.4 -> libduckdb.so.1.4.0
build/release/src/libduckdb.so.1.4.0
```

The predecessor used:

```text
find build/release -type f -name libduckdb.so
```

`-type f` follows neither the expected symlink name nor its target for selection, so it returned zero.

The repair uses exactly:

```text
build/release/src/libduckdb.so
```

It requires that path to exist, resolves it strictly, requires a regular target inside `build/release`, records the symlink and target metadata, and copies the target bytes into two distinct runtime files.

Controls cover:

- versioned symlink target;
- regular unversioned file;
- missing path;
- dangling link;
- directory target;
- target escaping the release tree;
- copied bytes and distinct inode.

## Trigger ownership matrix

Each trigger starts with a fresh database and one writer engine that:

1. creates table `t`;
2. creates secondary index `secondary_i`;
3. inserts rows `1..N`;
4. remains open with a pending WAL.

A second, byte-identical but independently loaded engine performs exactly one named trigger:

| Trigger | Exact operation |
| --- | --- |
| `open-close` | read-write open and close, no query |
| `select-one` | `SELECT 1` |
| `indexed-read` | `SELECT count(*) FROM t WHERE a = 1` |
| `explicit-checkpoint` | SQL `CHECKPOINT` |

The trigger process records WAL and database state before and after the named operation, then exits abruptly without closing the original writer.

## No hidden inspector trigger

A read-write inspector could itself recover or checkpoint the WAL and become an unrecorded fifth trigger.

The supervisor therefore follows this rule:

- when the named trigger has already removed the WAL, inspect the standalone file through a fresh **read-only** engine;
- when the WAL remains, record the row as `unsettled` and leave `wrong_result=null`;
- never open an unsettled row read-write merely to obtain an answer.

For settled rows, the fresh inspector requires:

- full row count unchanged;
- ordered values intact;
- optimizer-disabled filtered counts equal `1`;
- optimizer-enabled counts and plans retained;
- exact source identity.

The summary records both:

- first trigger in test order that settles the WAL;
- first settled trigger in test order that persists the wrong result.

## Historical rerun rows

Only the rows blocked by the symlink packaging error are rerun:

- `v1.4.0`, expected affected status from the release boundary;
- parent of ARTBuilder commit `22e6d1e3751829d2d029636b75ff931710ca7cbc`;
- exact ARTBuilder commit.

The workflow does not repeat the already-green v1.3.2, Node::Free pair, or predecessor current-source row.

## Evidence boundary

This successor can identify the first losing operation among the four tested lifecycle triggers and can recover the three missing historical rows.

It does not yet:

- identify the exact source line or serialization structure;
- prove that the first losing trigger is the only trigger that can lose;
- cover process crashes at every instruction boundary;
- cover concurrent writers, larger row counts, composite indexes, deletes, updates, or different checkpoint thresholds;
- select or implement a product patch;
- contact upstream.

Dynamic `public/main` is resolved to an exact commit and retained by each run. The conclusion belongs to that recorded source SHA, not to the moving branch name.

## Disposition

`EXECUTE SOURCE PACKAGING REPAIR AND TRIGGER DISCRIMINATOR`.

A green workflow supports a causal narrowing and historical matrix completion, not a product landing decision.
