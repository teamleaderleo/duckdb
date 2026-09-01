#!/usr/bin/env python3
"""Probe secondary ART checkpoint persistence with two same-source engines."""

from __future__ import annotations

import argparse
import ctypes as c
import hashlib
import json
import os
import pathlib
import sys
from dataclasses import asdict, dataclass
from typing import Sequence


RESULT_WORDS = 256


@dataclass(frozen=True)
class LibraryIdentity:
    path: str
    sha256: str
    device: int
    inode: int
    open_address: int


class QueryResult:
    def __init__(self, engine: "Engine", connection: c.c_void_p, sql: str):
        self.engine = engine
        self.storage = (c.c_uint64 * RESULT_WORDS)()
        self.pointer = c.cast(c.byref(self.storage), c.c_void_p)
        status = engine.library.duckdb_query(
            connection,
            sql.encode("utf-8"),
            self.pointer,
        )
        if status != 0:
            engine.library.duckdb_destroy_result(self.pointer)
            raise RuntimeError(f"duckdb_query failed: {sql}")
        self.closed = False

    def close(self) -> None:
        if not self.closed:
            self.engine.library.duckdb_destroy_result(self.pointer)
            self.closed = True

    def __enter__(self) -> "QueryResult":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    @property
    def row_count(self) -> int:
        return int(self.engine.library.duckdb_row_count(self.pointer))

    @property
    def column_count(self) -> int:
        return int(self.engine.library.duckdb_column_count(self.pointer))

    def int64(self, row: int = 0, column: int = 0) -> int:
        if row >= self.row_count or column >= self.column_count:
            raise RuntimeError(
                f"result cell out of range: row={row}, column={column}, "
                f"shape={self.row_count}x{self.column_count}"
            )
        return int(
            self.engine.library.duckdb_value_int64(
                self.pointer,
                c.c_uint64(column),
                c.c_uint64(row),
            )
        )

    def string(self, row: int, column: int) -> str:
        if row >= self.row_count or column >= self.column_count:
            raise RuntimeError(
                f"result cell out of range: row={row}, column={column}, "
                f"shape={self.row_count}x{self.column_count}"
            )
        value = self.engine.library.duckdb_value_varchar(
            self.pointer,
            c.c_uint64(column),
            c.c_uint64(row),
        )
        if not value:
            return ""
        try:
            return c.string_at(value).decode("utf-8", errors="replace")
        finally:
            self.engine.library.duckdb_free(value)


class Connection:
    def __init__(self, engine: "Engine", database: pathlib.Path, mode: str):
        self.engine = engine
        self.database_handle = c.c_void_p()
        self.connection_handle = c.c_void_p()
        config = c.c_void_p()
        error = c.c_char_p()

        if engine.library.duckdb_create_config(c.byref(config)) != 0:
            raise RuntimeError("duckdb_create_config failed")
        try:
            if (
                engine.library.duckdb_set_config(
                    config,
                    b"access_mode",
                    mode.encode("ascii"),
                )
                != 0
            ):
                raise RuntimeError("duckdb_set_config failed")
            if (
                engine.library.duckdb_open_ext(
                    str(database).encode("utf-8"),
                    c.byref(self.database_handle),
                    config,
                    c.byref(error),
                )
                != 0
            ):
                message = (
                    error.value.decode("utf-8", errors="replace")
                    if error.value
                    else "unknown error"
                )
                raise RuntimeError(f"duckdb_open_ext failed: {message}")
        finally:
            engine.library.duckdb_destroy_config(c.byref(config))

        if (
            engine.library.duckdb_connect(
                self.database_handle,
                c.byref(self.connection_handle),
            )
            != 0
        ):
            engine.library.duckdb_close(c.byref(self.database_handle))
            raise RuntimeError("duckdb_connect failed")
        self.closed = False

    def close(self) -> None:
        if self.closed:
            return
        self.engine.library.duckdb_disconnect(c.byref(self.connection_handle))
        self.engine.library.duckdb_close(c.byref(self.database_handle))
        self.closed = True

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def execute(self, sql: str) -> None:
        with QueryResult(self.engine, self.connection_handle, sql):
            pass

    def scalar(self, sql: str) -> int:
        with QueryResult(self.engine, self.connection_handle, sql) as result:
            return result.int64()

    def integer_column(self, sql: str) -> list[int]:
        with QueryResult(self.engine, self.connection_handle, sql) as result:
            if result.column_count != 1:
                raise RuntimeError(
                    f"expected one result column, got {result.column_count}: {sql}"
                )
            return [result.int64(row, 0) for row in range(result.row_count)]

    def text_matrix(self, sql: str) -> list[list[str]]:
        with QueryResult(self.engine, self.connection_handle, sql) as result:
            return [
                [result.string(row, column) for column in range(result.column_count)]
                for row in range(result.row_count)
            ]


class Engine:
    def __init__(self, library_path: pathlib.Path):
        self.path = library_path.resolve(strict=True)
        self.library = c.CDLL(str(self.path), mode=c.RTLD_LOCAL)
        signatures = (
            ("duckdb_create_config", [c.POINTER(c.c_void_p)], c.c_int),
            ("duckdb_set_config", [c.c_void_p, c.c_char_p, c.c_char_p], c.c_int),
            (
                "duckdb_open_ext",
                [
                    c.c_char_p,
                    c.POINTER(c.c_void_p),
                    c.c_void_p,
                    c.POINTER(c.c_char_p),
                ],
                c.c_int,
            ),
            ("duckdb_connect", [c.c_void_p, c.POINTER(c.c_void_p)], c.c_int),
            ("duckdb_query", [c.c_void_p, c.c_char_p, c.c_void_p], c.c_int),
            ("duckdb_row_count", [c.c_void_p], c.c_uint64),
            ("duckdb_column_count", [c.c_void_p], c.c_uint64),
            (
                "duckdb_value_int64",
                [c.c_void_p, c.c_uint64, c.c_uint64],
                c.c_int64,
            ),
            (
                "duckdb_value_varchar",
                [c.c_void_p, c.c_uint64, c.c_uint64],
                c.c_void_p,
            ),
            ("duckdb_destroy_result", [c.c_void_p], None),
            ("duckdb_disconnect", [c.POINTER(c.c_void_p)], None),
            ("duckdb_close", [c.POINTER(c.c_void_p)], None),
            ("duckdb_destroy_config", [c.POINTER(c.c_void_p)], None),
            ("duckdb_free", [c.c_void_p], None),
        )
        for name, argtypes, restype in signatures:
            function = getattr(self.library, name)
            function.argtypes = argtypes
            function.restype = restype

    def connect(self, database: pathlib.Path, mode: str = "READ_WRITE") -> Connection:
        return Connection(self, database, mode)

    def scalar_once(
        self,
        database: pathlib.Path,
        sql: str,
        mode: str = "READ_WRITE",
    ) -> int:
        with self.connect(database, mode) as connection:
            return connection.scalar(sql)

    def identity(self) -> LibraryIdentity:
        metadata = self.path.stat()
        digest = hashlib.sha256(self.path.read_bytes()).hexdigest()
        address = c.cast(self.library.duckdb_open_ext, c.c_void_p).value
        if address is None:
            raise RuntimeError("duckdb_open_ext function address is unavailable")
        return LibraryIdentity(
            path=str(self.path),
            sha256=digest,
            device=metadata.st_dev,
            inode=metadata.st_ino,
            open_address=int(address),
        )


def remove_database(database: pathlib.Path) -> None:
    for candidate in (database, pathlib.Path(f"{database}.wal")):
        try:
            candidate.unlink()
        except FileNotFoundError:
            pass


def create_corruptible_state(args: argparse.Namespace) -> int:
    database = pathlib.Path(args.database).resolve()
    remove_database(database)
    writer = Engine(pathlib.Path(args.writer_library))
    checkpointer = Engine(pathlib.Path(args.checkpointer_library))
    writer_identity = writer.identity()
    checkpointer_identity = checkpointer.identity()

    if writer_identity.sha256 != checkpointer_identity.sha256:
        raise RuntimeError("writer and checkpointer library bytes differ")
    if (
        writer_identity.device == checkpointer_identity.device
        and writer_identity.inode == checkpointer_identity.inode
    ):
        raise RuntimeError("writer and checkpointer libraries share one inode")
    if writer_identity.open_address == checkpointer_identity.open_address:
        raise RuntimeError(
            "dynamic loader reused one duckdb_open_ext address; two engine images were not established"
        )

    writer_connection = writer.connect(database)
    writer_connection.execute("CREATE TABLE t(a INTEGER)")
    writer_connection.execute("CREATE INDEX secondary_i ON t(a)")
    writer_connection.execute(
        f"INSERT INTO t SELECT range AS a FROM range(1, {args.row_count + 1})"
    )
    wal_path = pathlib.Path(f"{database}.wal")
    if not wal_path.exists():
        raise RuntimeError("expected a pending WAL while the writer remains open")

    checkpointer.scalar_once(database, "SELECT 1")
    filtered_count = checkpointer.scalar_once(
        database,
        "SELECT count(*) FROM t WHERE a = 1",
    )
    terminal_count = checkpointer.scalar_once(
        database,
        f"SELECT count(*) FROM t WHERE a = {args.row_count}",
    )
    full_count = checkpointer.scalar_once(database, "SELECT count(*) FROM t")
    wrong_result = (
        filtered_count != 1
        or terminal_count != 1
        or full_count != args.row_count
    )

    record = {
        "schema_version": 1,
        "phase": "create",
        "source_sha": args.source_sha,
        "database": str(database),
        "row_count": args.row_count,
        "wal_present_before_checkpoint": True,
        "writer_library": asdict(writer_identity),
        "checkpointer_library": asdict(checkpointer_identity),
        "filtered_count": filtered_count,
        "terminal_filtered_count": terminal_count,
        "full_count": full_count,
        "wrong_result": wrong_result,
    }
    print(json.dumps(record, sort_keys=True), flush=True)

    # Preserve the checkpointed file before the original writer can heal it.
    os._exit(0 if wrong_result else 1)


def inspect_persisted_state(args: argparse.Namespace) -> int:
    database = pathlib.Path(args.database).resolve(strict=True)
    engine = Engine(pathlib.Path(args.library))
    identity = engine.identity()
    with engine.connect(database, "READ_ONLY") as connection:
        enabled_count = connection.scalar(
            "SELECT count(*) FROM t WHERE a = 1"
        )
        terminal_count = connection.scalar(
            f"SELECT count(*) FROM t WHERE a = {args.row_count}"
        )
        full_count = connection.scalar("SELECT count(*) FROM t")
        first_values = connection.integer_column(
            "SELECT a FROM t ORDER BY a LIMIT 10"
        )
        enabled_plan = connection.text_matrix(
            "EXPLAIN ANALYZE SELECT count(*) FROM t WHERE a = 1"
        )
        connection.execute("PRAGMA disable_optimizer")
        disabled_count = connection.scalar(
            "SELECT count(*) FROM t WHERE a = 1"
        )
        disabled_terminal_count = connection.scalar(
            f"SELECT count(*) FROM t WHERE a = {args.row_count}"
        )
        disabled_plan = connection.text_matrix(
            "EXPLAIN ANALYZE SELECT count(*) FROM t WHERE a = 1"
        )

    record = {
        "schema_version": 1,
        "phase": "inspect",
        "source_sha": args.source_sha,
        "database": str(database),
        "row_count": args.row_count,
        "library": asdict(identity),
        "enabled_filtered_count": enabled_count,
        "enabled_terminal_count": terminal_count,
        "disabled_filtered_count": disabled_count,
        "disabled_terminal_count": disabled_terminal_count,
        "full_count": full_count,
        "first_values": first_values,
        "enabled_plan": enabled_plan,
        "disabled_plan": disabled_plan,
    }
    print(json.dumps(record, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    create = subparsers.add_parser("create")
    create.add_argument("writer_library")
    create.add_argument("checkpointer_library")
    create.add_argument("database")
    create.add_argument("row_count", type=int)
    create.add_argument("source_sha")
    create.set_defaults(function=create_corruptible_state)

    inspect = subparsers.add_parser("inspect")
    inspect.add_argument("library")
    inspect.add_argument("database")
    inspect.add_argument("row_count", type=int)
    inspect.add_argument("source_sha")
    inspect.set_defaults(function=inspect_persisted_state)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.row_count < 2:
        raise SystemExit("row count must be at least 2")
    try:
        return int(args.function(args))
    except Exception as error:
        print(f"source-identity probe failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
