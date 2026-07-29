#!/usr/bin/env python3
"""Exact-threshold crash probe for Fieldwork issue 55.

This probe corrects the V2 crash control. It kills the COPY worker immediately
when the observed destination reaches at least 1 MiB, rather than waiting for a
fixed observation window. Generated data only.
"""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import duckdb

VERSION = "1.5.5"
FORMATS = ("csv", "parquet")
ROWS = 100_000_000
THRESHOLD = 1_048_576
POLL_SECONDS = 0.002
TIMEOUT_SECONDS = 20.0
RETRY_ROWS = 1_000


def quote(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def options(fmt: str) -> str:
    if fmt == "csv":
        return "FORMAT CSV, HEADER true"
    if fmt == "parquet":
        return "FORMAT PARQUET, COMPRESSION ZSTD"
    raise ValueError(fmt)


def copy_sql(path: Path, fmt: str, rows: int) -> str:
    return (
        "COPY (SELECT i::BIGINT AS i, "
        "md5(i::VARCHAR) || md5((i + 1)::VARCHAR) AS payload "
        f"FROM range({rows}) t(i)) TO '{quote(path)}' ({options(fmt)})"
    )


def expected_sum(rows: int) -> int:
    return rows * (rows - 1) // 2


def inspect(path: Path, fmt: str) -> dict[str, Any]:
    result: dict[str, Any] = {"exists": path.exists()}
    if not path.exists():
        return result
    result["size_bytes"] = path.stat().st_size
    relation = (
        f"read_csv_auto('{quote(path)}', header=true)"
        if fmt == "csv"
        else f"read_parquet('{quote(path)}')"
    )
    connection = duckdb.connect()
    try:
        count, checksum, minimum, maximum = connection.execute(
            f"SELECT count(*), sum(i), min(i), max(i) FROM {relation}"
        ).fetchone()
        result.update(
            readable=True,
            row_count=count,
            checksum=checksum,
            minimum=minimum,
            maximum=maximum,
        )
    except Exception as exc:
        result.update(readable=False, error_type=type(exc).__name__, error=str(exc))
    finally:
        connection.close()
    return result


def worker(path: Path, fmt: str) -> int:
    connection = duckdb.connect()
    connection.execute("SET threads=1")
    try:
        connection.execute(copy_sql(path, fmt, ROWS))
        return 0
    finally:
        connection.close()


def run_case(root: Path, fmt: str) -> dict[str, Any]:
    path = root / f"threshold-crash.{fmt}"
    process = subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), "--worker", str(path), fmt],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    started = time.monotonic()
    first_visible: float | None = None
    first_nonzero: float | None = None
    observed_size = 0
    threshold_at: float | None = None
    samples = 0

    while time.monotonic() - started < TIMEOUT_SECONDS:
        samples += 1
        if path.exists():
            now = time.monotonic() - started
            if first_visible is None:
                first_visible = now
            try:
                observed_size = path.stat().st_size
            except FileNotFoundError:
                observed_size = 0
            if observed_size > 0 and first_nonzero is None:
                first_nonzero = now
            if observed_size >= THRESHOLD:
                threshold_at = now
                break
        if process.poll() is not None:
            break
        time.sleep(POLL_SECONDS)

    running_at_trigger = process.poll() is None
    killed = threshold_at is not None and running_at_trigger
    if killed:
        process.kill()
    stdout, stderr = process.communicate(timeout=20.0)
    killed_at = time.monotonic() - started
    residue = inspect(path, fmt)

    retry_connection = duckdb.connect()
    try:
        retry_connection.execute(copy_sql(path, fmt, RETRY_ROWS))
        retry = {"succeeded": True, "after": inspect(path, fmt)}
    except Exception as exc:
        retry = {"succeeded": False, "error_type": type(exc).__name__, "error": str(exc)}
    finally:
        retry_connection.close()

    return {
        "format": fmt,
        "rows_requested": ROWS,
        "threshold_bytes": THRESHOLD,
        "poll_seconds": POLL_SECONDS,
        "first_visible_seconds": first_visible,
        "first_nonzero_seconds": first_nonzero,
        "threshold_at_seconds": threshold_at,
        "kill_completed_seconds": killed_at,
        "observed_size_at_trigger_bytes": observed_size,
        "samples": samples,
        "running_at_trigger": running_at_trigger,
        "killed": killed,
        "returncode": process.returncode,
        "stdout": stdout.strip(),
        "stderr": stderr.strip(),
        "residue": residue,
        "default_retry": retry,
    }


def evaluate(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []

    def add(name: str, passed: bool, detail: Any) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    for case in cases:
        fmt = case["format"]
        residue = case["residue"]
        retry = case["default_retry"]
        retry_after = retry.get("after", {})
        add(f"{fmt}.threshold_reached", case["threshold_at_seconds"] is not None, case)
        add(f"{fmt}.running_at_trigger", case["running_at_trigger"], case)
        add(f"{fmt}.killed", case["killed"] and case["returncode"] == -9, case)
        add(f"{fmt}.bounded_overshoot", case["observed_size_at_trigger_bytes"] < 16 * THRESHOLD, case)
        add(f"{fmt}.final_path_remains", residue.get("exists") is True, residue)
        if fmt == "csv":
            partial = residue.get("readable") is True and 0 < residue.get("row_count", 0) < ROWS
            add("csv.partial_readable", partial, residue)
        else:
            add("parquet.incomplete_rejected", residue.get("readable") is False, residue)
        add(f"{fmt}.retry_succeeds", retry.get("succeeded") is True, retry)
        add(f"{fmt}.retry_count", retry_after.get("row_count") == RETRY_ROWS, retry_after)
        add(f"{fmt}.retry_checksum", retry_after.get("checksum") == expected_sum(RETRY_ROWS), retry_after)
    return checks


def run(output: Path) -> int:
    if duckdb.__version__ != VERSION:
        raise RuntimeError(f"expected DuckDB {VERSION}, got {duckdb.__version__}")
    with tempfile.TemporaryDirectory(prefix="fieldwork-duckdb-issue55-threshold-") as directory:
        root = Path(directory)
        cases = [run_case(root, fmt) for fmt in FORMATS]
    checks = evaluate(cases)
    document = {
        "probe": "fieldwork-issue55-exact-crash-threshold",
        "duckdb_version": duckdb.__version__,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "cases": cases,
        "checks": checks,
        "passed": sum(check["passed"] for check in checks),
        "failed": sum(not check["passed"] for check in checks),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(output), "passed": document["passed"], "failed": document["failed"]}))
    return 0 if document["failed"] == 0 else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("issue55-crash-threshold.json"))
    parser.add_argument("--worker", nargs=2, metavar=("PATH", "FORMAT"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.worker:
        path, fmt = args.worker
        return worker(Path(path), fmt)
    return run(args.output)


if __name__ == "__main__":
    raise SystemExit(main())
