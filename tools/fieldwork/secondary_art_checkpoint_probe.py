#!/usr/bin/env python3
from __future__ import annotations

import ctypes as c
import json
import os
import pathlib
import sys

import duckdb


LIBRARY = pathlib.Path(sys.argv[1]).resolve()
DATABASE = pathlib.Path(sys.argv[2]).resolve()
for candidate in (DATABASE, pathlib.Path(f"{DATABASE}.wal")):
    try:
        candidate.unlink()
    except FileNotFoundError:
        pass

lib = c.CDLL(str(LIBRARY))
for name, argtypes, restype in [
    ("duckdb_create_config", [c.POINTER(c.c_void_p)], c.c_int),
    ("duckdb_set_config", [c.c_void_p, c.c_char_p, c.c_char_p], c.c_int),
    (
        "duckdb_open_ext",
        [c.c_char_p, c.POINTER(c.c_void_p), c.c_void_p, c.POINTER(c.c_char_p)],
        c.c_int,
    ),
    ("duckdb_connect", [c.c_void_p, c.POINTER(c.c_void_p)], c.c_int),
    ("duckdb_query", [c.c_void_p, c.c_char_p, c.c_void_p], c.c_int),
    ("duckdb_value_int64", [c.c_void_p, c.c_uint64, c.c_uint64], c.c_int64),
    ("duckdb_destroy_result", [c.c_void_p], None),
    ("duckdb_disconnect", [c.POINTER(c.c_void_p)], None),
    ("duckdb_close", [c.POINTER(c.c_void_p)], None),
    ("duckdb_destroy_config", [c.POINTER(c.c_void_p)], None),
]:
    function = getattr(lib, name)
    function.argtypes = argtypes
    function.restype = restype


def scalar(sql: str, mode: str = "READ_WRITE") -> int:
    config = c.c_void_p()
    if lib.duckdb_create_config(c.byref(config)) != 0:
        raise RuntimeError("duckdb_create_config failed")
    try:
        if lib.duckdb_set_config(config, b"access_mode", mode.encode()) != 0:
            raise RuntimeError("duckdb_set_config failed")
        database = c.c_void_p()
        error = c.c_char_p()
        if (
            lib.duckdb_open_ext(
                str(DATABASE).encode(), c.byref(database), config, c.byref(error)
            )
            != 0
        ):
            message = error.value.decode(errors="replace") if error.value else "unknown"
            raise RuntimeError(f"duckdb_open_ext failed: {message}")
    finally:
        lib.duckdb_destroy_config(c.byref(config))

    connection = c.c_void_p()
    if lib.duckdb_connect(database, c.byref(connection)) != 0:
        lib.duckdb_close(c.byref(database))
        raise RuntimeError("duckdb_connect failed")
    result = (c.c_byte * 512)()
    try:
        if lib.duckdb_query(connection, sql.encode(), c.byref(result)) != 0:
            raise RuntimeError(f"duckdb_query failed: {sql}")
        return int(lib.duckdb_value_int64(c.byref(result), 0, 0))
    finally:
        lib.duckdb_destroy_result(c.byref(result))
        lib.duckdb_disconnect(c.byref(connection))
        lib.duckdb_close(c.byref(database))


writer = duckdb.connect(str(DATABASE))
writer.execute("CREATE TABLE t(a INTEGER)")
writer.execute("CREATE INDEX secondary_i ON t(a)")
writer.execute("INSERT INTO t VALUES (1), (2)")
wal_present = pathlib.Path(f"{DATABASE}.wal").exists()
if not wal_present:
    raise RuntimeError("expected pending WAL while writer connection remains open")

# A second independently loaded engine opens read-write and checkpoints the WAL.
scalar("SELECT 1")
indexed_count = scalar("SELECT count(*) FROM t WHERE a = 1")
full_count = scalar("SELECT count(*) FROM t")
corrupt = indexed_count != 1 or full_count != 2

record = {
    "python_duckdb_version": duckdb.__version__,
    "library": str(LIBRARY),
    "database": str(DATABASE),
    "wal_present_before_checkpoint": wal_present,
    "indexed_count": indexed_count,
    "full_count": full_count,
    "corrupt": corrupt,
}
print(json.dumps(record, sort_keys=True), flush=True)

# Deliberately bypass Python/C++ cleanup so the original writer cannot heal the
# persisted index with its own correct in-memory state during interpreter exit.
os._exit(0 if corrupt else 1)
