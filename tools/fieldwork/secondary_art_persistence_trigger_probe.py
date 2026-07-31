#!/usr/bin/env python3
"""Create one WAL-backed secondary-ART database and apply one named trigger."""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
from dataclasses import asdict
from typing import Sequence

try:
    from tools.fieldwork.secondary_art_source_identity_probe import (
        Engine,
        remove_database,
    )
except ModuleNotFoundError:
    from secondary_art_source_identity_probe import Engine, remove_database


TRIGGERS = (
    "open-close",
    "select-one",
    "indexed-read",
    "explicit-checkpoint",
)


def file_state(path: pathlib.Path) -> dict[str, object]:
    try:
        metadata = path.stat()
    except FileNotFoundError:
        return {"present": False, "size": None}
    return {"present": True, "size": metadata.st_size}


def validate_engine_pair(writer: Engine, trigger_engine: Engine) -> tuple[object, object]:
    writer_identity = writer.identity()
    trigger_identity = trigger_engine.identity()
    if writer_identity.sha256 != trigger_identity.sha256:
        raise RuntimeError("writer and trigger library bytes differ")
    if (
        writer_identity.device == trigger_identity.device
        and writer_identity.inode == trigger_identity.inode
    ):
        raise RuntimeError("writer and trigger libraries share one inode")
    if writer_identity.open_address == trigger_identity.open_address:
        raise RuntimeError(
            "dynamic loader reused one duckdb_open_ext address; two engine images were not established"
        )
    return writer_identity, trigger_identity


def apply_trigger(engine: Engine, database: pathlib.Path, trigger: str) -> dict[str, object]:
    observation: dict[str, object] = {"trigger": trigger, "sql": None, "scalar": None}
    with engine.connect(database) as connection:
        if trigger == "open-close":
            pass
        elif trigger == "select-one":
            sql = "SELECT 1"
            observation["sql"] = sql
            observation["scalar"] = connection.scalar(sql)
        elif trigger == "indexed-read":
            sql = "SELECT count(*) FROM t WHERE a = 1"
            observation["sql"] = sql
            observation["scalar"] = connection.scalar(sql)
        elif trigger == "explicit-checkpoint":
            sql = "CHECKPOINT"
            observation["sql"] = sql
            connection.execute(sql)
        else:
            raise RuntimeError(f"unknown trigger: {trigger}")
    return observation


def create_trigger_state(args: argparse.Namespace) -> int:
    database = pathlib.Path(args.database).resolve()
    remove_database(database)
    writer = Engine(pathlib.Path(args.writer_library))
    trigger_engine = Engine(pathlib.Path(args.trigger_library))
    writer_identity, trigger_identity = validate_engine_pair(writer, trigger_engine)

    writer_connection = writer.connect(database)
    writer_connection.execute("CREATE TABLE t(a INTEGER)")
    writer_connection.execute("CREATE INDEX secondary_i ON t(a)")
    writer_connection.execute(
        f"INSERT INTO t SELECT range AS a FROM range(1, {args.row_count + 1})"
    )

    wal_path = pathlib.Path(f"{database}.wal")
    wal_before = file_state(wal_path)
    database_before = file_state(database)
    if wal_before["present"] is not True:
        raise RuntimeError("expected a pending WAL while the writer remains open")

    observation = apply_trigger(trigger_engine, database, args.trigger)

    record = {
        "schema_version": 1,
        "phase": "trigger",
        "source_sha": args.source_sha,
        "database": str(database),
        "row_count": args.row_count,
        "trigger": args.trigger,
        "trigger_observation": observation,
        "writer_library": asdict(writer_identity),
        "trigger_library": asdict(trigger_identity),
        "database_before_trigger": database_before,
        "database_after_trigger": file_state(database),
        "wal_before_trigger": wal_before,
        "wal_after_trigger": file_state(wal_path),
        "writer_intentionally_left_open": True,
    }
    print(json.dumps(record, sort_keys=True), flush=True)

    # Preserve exactly the state produced by the named second-engine trigger.
    # Closing the original writer could perform additional persistence work.
    os._exit(0)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("writer_library")
    parser.add_argument("trigger_library")
    parser.add_argument("database")
    parser.add_argument("row_count", type=int)
    parser.add_argument("source_sha")
    parser.add_argument("trigger", choices=TRIGGERS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.row_count < 2:
        raise SystemExit("row count must be at least 2")
    try:
        return create_trigger_state(args)
    except Exception as error:
        print(f"persistence-trigger probe failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
