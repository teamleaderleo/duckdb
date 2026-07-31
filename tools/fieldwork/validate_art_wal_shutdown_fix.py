#!/usr/bin/env python3
"""Validate buffered secondary-ART WAL replay across shutdown and later binding."""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
from dataclasses import asdict
from typing import Any, Sequence

try:
    from tools.fieldwork.secondary_art_source_identity_probe import (
        Engine,
        remove_database,
    )
except ModuleNotFoundError:
    from secondary_art_source_identity_probe import Engine, remove_database


def file_state(path: pathlib.Path) -> dict[str, object]:
    try:
        metadata = path.stat()
    except FileNotFoundError:
        return {"present": False, "size": None}
    return {"present": True, "size": metadata.st_size}


def require_engine_pair(writer: Engine, shutdown: Engine) -> tuple[object, object]:
    writer_identity = writer.identity()
    shutdown_identity = shutdown.identity()
    if writer_identity.sha256 != shutdown_identity.sha256:
        raise RuntimeError("writer and shutdown library bytes differ")
    if (
        writer_identity.device == shutdown_identity.device
        and writer_identity.inode == shutdown_identity.inode
    ):
        raise RuntimeError("writer and shutdown libraries share one inode")
    if writer_identity.open_address == shutdown_identity.open_address:
        raise RuntimeError(
            "dynamic loader reused one duckdb_open_ext address; two engine images were not established"
        )
    return writer_identity, shutdown_identity


def phase_shutdown(args: argparse.Namespace) -> int:
    database = pathlib.Path(args.database).resolve()
    wal = pathlib.Path(f"{database}.wal")
    remove_database(database)

    writer = Engine(pathlib.Path(args.writer_library))
    shutdown = Engine(pathlib.Path(args.shutdown_library))
    writer_identity, shutdown_identity = require_engine_pair(writer, shutdown)

    writer_connection = writer.connect(database)
    writer_connection.execute("PRAGMA wal_autocheckpoint='1TB'")
    writer_connection.execute("CREATE TABLE t(a INTEGER)")
    writer_connection.execute("CREATE INDEX secondary_i ON t(a)")
    writer_connection.execute(
        f"INSERT INTO t SELECT range AS a FROM range(1, {args.row_count + 1})"
    )

    database_before = file_state(database)
    wal_before = file_state(wal)
    if wal_before["present"] is not True:
        raise RuntimeError("expected a pending WAL while original writer remains open")

    # The query deliberately does not touch the indexed table. Opening replays
    # the WAL; closing has no client context and exercises shutdown checkpoint.
    with shutdown.connect(database) as connection:
        scalar = connection.scalar("SELECT 1")
    if scalar != 1:
        raise RuntimeError(f"shutdown-engine SELECT 1 returned {scalar}")

    record = {
        "schema_version": 1,
        "phase": "context-free-shutdown",
        "source_sha": args.source_sha,
        "database": str(database),
        "row_count": args.row_count,
        "writer_library": asdict(writer_identity),
        "shutdown_library": asdict(shutdown_identity),
        "writer_intentionally_left_open": True,
        "shutdown_query": "SELECT 1",
        "shutdown_query_scalar": scalar,
        "database_before_shutdown": database_before,
        "wal_before_shutdown": wal_before,
        "database_after_shutdown": file_state(database),
        "wal_after_shutdown": file_state(wal),
    }
    print(json.dumps(record, sort_keys=True), flush=True)

    # A clean writer close can heal the parent corruption and would erase the
    # candidate's preserved WAL. Leave the exact post-shutdown state on disk.
    os._exit(0)


def phase_repeat_shutdown(args: argparse.Namespace) -> int:
    database = pathlib.Path(args.database).resolve(strict=True)
    wal = pathlib.Path(f"{database}.wal")
    engine = Engine(pathlib.Path(args.library))
    identity = engine.identity()
    database_before = file_state(database)
    wal_before = file_state(wal)

    # This process also avoids the indexed table. For the candidate, replayed
    # operations must remain buffered and the second context-free close must
    # refuse stale serialization again, preserving the WAL for a later bind.
    with engine.connect(database) as connection:
        scalar = connection.scalar("SELECT 1")
    if scalar != 1:
        raise RuntimeError(f"repeat-shutdown SELECT 1 returned {scalar}")

    record = {
        "schema_version": 1,
        "phase": "repeat-context-free-shutdown",
        "source_sha": args.source_sha,
        "database": str(database),
        "row_count": args.row_count,
        "library": asdict(identity),
        "shutdown_query": "SELECT 1",
        "shutdown_query_scalar": scalar,
        "database_before_repeat_shutdown": database_before,
        "wal_before_repeat_shutdown": wal_before,
        "database_after_repeat_shutdown": file_state(database),
        "wal_after_repeat_shutdown": file_state(wal),
    }
    print(json.dumps(record, sort_keys=True))
    return 0


def phase_bind_checkpoint(args: argparse.Namespace) -> int:
    database = pathlib.Path(args.database).resolve(strict=True)
    wal = pathlib.Path(f"{database}.wal")
    engine = Engine(pathlib.Path(args.library))
    identity = engine.identity()
    wal_before = file_state(wal)

    with engine.connect(database) as connection:
        connection.execute("SET index_scan_max_count = 1")
        enabled_count = connection.scalar("SELECT count(*) FROM t WHERE a = 1")
        enabled_terminal = connection.scalar(
            f"SELECT count(*) FROM t WHERE a = {args.row_count}"
        )
        enabled_plan = connection.text_matrix(
            "EXPLAIN ANALYZE SELECT count(*) FROM t WHERE a = 1"
        )
        full_count = connection.scalar("SELECT count(*) FROM t")
        first_values = connection.integer_column(
            "SELECT a FROM t ORDER BY a LIMIT 10"
        )

        connection.execute("PRAGMA disable_optimizer")
        sequential_count = connection.scalar(
            "SELECT count(*) FROM t WHERE a = 1"
        )
        sequential_terminal = connection.scalar(
            f"SELECT count(*) FROM t WHERE a = {args.row_count}"
        )
        sequential_plan = connection.text_matrix(
            "EXPLAIN ANALYZE SELECT count(*) FROM t WHERE a = 1"
        )

        # This is the only explicit SQL checkpoint in the lifecycle. The first
        # indexed query above gives WAL replay a client context in which to bind
        # the unbound index and apply buffered operations.
        connection.execute("CHECKPOINT")
        wal_after_checkpoint_while_open = file_state(wal)

    record = {
        "schema_version": 1,
        "phase": "bind-and-explicit-checkpoint",
        "source_sha": args.source_sha,
        "database": str(database),
        "row_count": args.row_count,
        "library": asdict(identity),
        "wal_before_bind": wal_before,
        "enabled_filtered_count": enabled_count,
        "enabled_terminal_count": enabled_terminal,
        "enabled_plan": enabled_plan,
        "sequential_filtered_count": sequential_count,
        "sequential_terminal_count": sequential_terminal,
        "sequential_plan": sequential_plan,
        "full_count": full_count,
        "first_values": first_values,
        "explicit_checkpoint_executed": True,
        "wal_after_explicit_checkpoint_while_open": wal_after_checkpoint_while_open,
        "wal_after_connection_close": file_state(wal),
        "database_after_connection_close": file_state(database),
    }
    print(json.dumps(record, sort_keys=True))
    return 0


def phase_inspect(args: argparse.Namespace) -> int:
    database = pathlib.Path(args.database).resolve(strict=True)
    wal = pathlib.Path(f"{database}.wal")
    engine = Engine(pathlib.Path(args.library))
    identity = engine.identity()

    with engine.connect(database, "READ_ONLY") as connection:
        connection.execute("SET index_scan_max_count = 1")
        enabled_count = connection.scalar("SELECT count(*) FROM t WHERE a = 1")
        enabled_terminal = connection.scalar(
            f"SELECT count(*) FROM t WHERE a = {args.row_count}"
        )
        enabled_plan = connection.text_matrix(
            "EXPLAIN ANALYZE SELECT count(*) FROM t WHERE a = 1"
        )
        full_count = connection.scalar("SELECT count(*) FROM t")
        first_values = connection.integer_column(
            "SELECT a FROM t ORDER BY a LIMIT 10"
        )
        connection.execute("PRAGMA disable_optimizer")
        sequential_count = connection.scalar(
            "SELECT count(*) FROM t WHERE a = 1"
        )
        sequential_terminal = connection.scalar(
            f"SELECT count(*) FROM t WHERE a = {args.row_count}"
        )

    record = {
        "schema_version": 1,
        "phase": "read-only-inspection",
        "source_sha": args.source_sha,
        "database": str(database),
        "row_count": args.row_count,
        "library": asdict(identity),
        "wal_before_read_only_open": file_state(wal),
        "enabled_filtered_count": enabled_count,
        "enabled_terminal_count": enabled_terminal,
        "enabled_plan": enabled_plan,
        "sequential_filtered_count": sequential_count,
        "sequential_terminal_count": sequential_terminal,
        "full_count": full_count,
        "first_values": first_values,
        "wal_after_read_only_close": file_state(wal),
    }
    print(json.dumps(record, sort_keys=True))
    return 0


def classify_lifecycle(
    label: str,
    shutdown: dict[str, Any],
    repeat: dict[str, Any],
    bind: dict[str, Any],
    inspection: dict[str, Any],
    row_count: int,
) -> dict[str, object]:
    expected_values = list(range(1, min(row_count, 10) + 1))
    common = (
        bind.get("full_count") == row_count
        and inspection.get("full_count") == row_count
        and bind.get("first_values") == expected_values
        and inspection.get("first_values") == expected_values
        and bind.get("sequential_filtered_count") == 1
        and bind.get("sequential_terminal_count") == 1
        and inspection.get("sequential_filtered_count") == 1
        and inspection.get("sequential_terminal_count") == 1
        and bind.get("explicit_checkpoint_executed") is True
        and bind.get("wal_after_explicit_checkpoint_while_open", {}).get("present")
        is False
        and bind.get("wal_after_connection_close", {}).get("present") is False
        and inspection.get("wal_before_read_only_open", {}).get("present") is False
        and inspection.get("wal_after_read_only_close", {}).get("present") is False
    )
    if not common:
        raise ValueError(f"{label}: common lifecycle controls failed")

    parent_shape = (
        shutdown.get("wal_before_shutdown", {}).get("present") is True
        and shutdown.get("wal_after_shutdown", {}).get("present") is False
        and repeat.get("wal_before_repeat_shutdown", {}).get("present") is False
        and repeat.get("wal_after_repeat_shutdown", {}).get("present") is False
        and bind.get("wal_before_bind", {}).get("present") is False
        and bind.get("enabled_filtered_count") == 0
        and bind.get("enabled_terminal_count") == 0
        and inspection.get("enabled_filtered_count") == 0
        and inspection.get("enabled_terminal_count") == 0
    )
    candidate_shape = (
        shutdown.get("wal_before_shutdown", {}).get("present") is True
        and shutdown.get("wal_after_shutdown", {}).get("present") is True
        and repeat.get("wal_before_repeat_shutdown", {}).get("present") is True
        and repeat.get("wal_after_repeat_shutdown", {}).get("present") is True
        and bind.get("wal_before_bind", {}).get("present") is True
        and bind.get("enabled_filtered_count") == 1
        and bind.get("enabled_terminal_count") == 1
        and inspection.get("enabled_filtered_count") == 1
        and inspection.get("enabled_terminal_count") == 1
    )
    if parent_shape == candidate_shape:
        raise ValueError(
            f"{label}: lifecycle is neither uniquely parent-corrupt nor candidate-preserved"
        )

    return {
        "schema_version": 1,
        "label": label,
        "classification": (
            "parent-corrupt-shutdown-checkpoint"
            if parent_shape
            else "candidate-preserves-wal-and-heals-after-bind"
        ),
        "wal_preserved_after_context_free_shutdown": shutdown[
            "wal_after_shutdown"
        ]["present"],
        "wal_preserved_after_repeat_shutdown": repeat[
            "wal_after_repeat_shutdown"
        ]["present"],
        "indexed_count_before_explicit_checkpoint": bind[
            "enabled_filtered_count"
        ],
        "read_only_indexed_count_after_checkpoint": inspection[
            "enabled_filtered_count"
        ],
        "sequential_controls_correct": True,
        "full_rows_and_order_correct": True,
        "explicit_checkpoint_removed_wal": True,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    shutdown = subparsers.add_parser("shutdown")
    shutdown.add_argument("writer_library")
    shutdown.add_argument("shutdown_library")
    shutdown.add_argument("database")
    shutdown.add_argument("row_count", type=int)
    shutdown.add_argument("source_sha")
    shutdown.set_defaults(function=phase_shutdown)

    repeat = subparsers.add_parser("repeat-shutdown")
    repeat.add_argument("library")
    repeat.add_argument("database")
    repeat.add_argument("row_count", type=int)
    repeat.add_argument("source_sha")
    repeat.set_defaults(function=phase_repeat_shutdown)

    bind = subparsers.add_parser("bind-checkpoint")
    bind.add_argument("library")
    bind.add_argument("database")
    bind.add_argument("row_count", type=int)
    bind.add_argument("source_sha")
    bind.set_defaults(function=phase_bind_checkpoint)

    inspect = subparsers.add_parser("inspect")
    inspect.add_argument("library")
    inspect.add_argument("database")
    inspect.add_argument("row_count", type=int)
    inspect.add_argument("source_sha")
    inspect.set_defaults(function=phase_inspect)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.row_count < 2:
        raise SystemExit("row count must be at least 2")
    try:
        return int(args.function(args))
    except Exception as error:
        print(f"ART WAL shutdown validation failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
