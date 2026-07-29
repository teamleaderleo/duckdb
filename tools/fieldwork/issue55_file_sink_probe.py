#!/usr/bin/env python3
"""Deterministic DuckDB file-publication probe for Fieldwork issue 55.

The probe distinguishes four moments for CSV and Parquet COPY output:
1. normal completion,
2. manual interruption after the destination becomes visible,
3. abrupt process death after bytes reach the destination,
4. low-memory failure during a spill-heavy source query.

It records what a filesystem observer can see while COPY is active and what a
fresh DuckDB connection sees afterward. Generated data only.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import duckdb

EXPECTED_DUCKDB_VERSION = "1.5.5"
FORMATS = ("csv", "parquet")
SUCCESS_ROWS = 500_000
INTERRUPT_ROWS = 50_000_000
CRASH_ROWS = 100_000_000
OOM_ROWS = 2_000_000
CRASH_THRESHOLD_BYTES = 1_048_576
POLL_SECONDS = 0.01


def sql_path(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def copy_options(fmt: str) -> str:
    if fmt == "csv":
        return "FORMAT CSV, HEADER true"
    if fmt == "parquet":
        return "FORMAT PARQUET, COMPRESSION ZSTD"
    raise ValueError(f"unsupported format: {fmt}")


def copy_sql(path: Path, fmt: str, rows: int, *, ordered: bool = False) -> str:
    source = (
        "SELECT i::BIGINT AS i, "
        "md5(i::VARCHAR) || md5((i + 1)::VARCHAR) AS payload "
        f"FROM range({rows}) AS t(i)"
    )
    if ordered:
        source += " ORDER BY hash(i)"
    return f"COPY ({source}) TO '{sql_path(path)}' ({copy_options(fmt)})"


def expected_sum(rows: int) -> int:
    return rows * (rows - 1) // 2


def observe_path(path: Path, process: subprocess.Popen[str], timeout: float) -> dict[str, Any]:
    started = time.monotonic()
    first_visible: float | None = None
    first_nonzero: float | None = None
    max_size = 0
    samples = 0
    while time.monotonic() - started < timeout:
        samples += 1
        exists = path.exists()
        now = time.monotonic()
        if exists and first_visible is None:
            first_visible = now - started
        if exists:
            try:
                size = path.stat().st_size
            except FileNotFoundError:
                size = 0
            max_size = max(max_size, size)
            if size > 0 and first_nonzero is None:
                first_nonzero = now - started
        if process.poll() is not None:
            break
        time.sleep(POLL_SECONDS)
    return {
        "first_visible_seconds": first_visible,
        "first_nonzero_seconds": first_nonzero,
        "max_observed_size_bytes": max_size,
        "samples": samples,
        "process_running_at_observer_end": process.poll() is None,
    }


def read_summary(path: Path, fmt: str) -> dict[str, Any]:
    result: dict[str, Any] = {"exists": path.exists()}
    if not path.exists():
        return result
    result["size_bytes"] = path.stat().st_size
    relation = (
        f"read_csv_auto('{sql_path(path)}', header=true)"
        if fmt == "csv"
        else f"read_parquet('{sql_path(path)}')"
    )
    connection = duckdb.connect()
    try:
        count, checksum, minimum, maximum = connection.execute(
            f"SELECT count(*), sum(i), min(i), max(i) FROM {relation}"
        ).fetchone()
        result.update(
            {
                "readable": True,
                "row_count": count,
                "checksum": checksum,
                "minimum": minimum,
                "maximum": maximum,
            }
        )
    except Exception as exc:  # exact typed message is evidence
        result.update(
            {
                "readable": False,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
        )
    finally:
        connection.close()
    return result


def worker_copy(path: Path, fmt: str, rows: int) -> int:
    connection = duckdb.connect()
    connection.execute("SET threads=1")
    try:
        connection.execute(copy_sql(path, fmt, rows))
        print(json.dumps({"ok": True, "rows": rows}), flush=True)
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {"ok": False, "error_type": type(exc).__name__, "error": str(exc)}
            ),
            flush=True,
        )
        return 2
    finally:
        connection.close()


def launch_worker(path: Path, fmt: str, rows: int) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--worker-copy",
            str(path),
            fmt,
            str(rows),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def success_case(root: Path, fmt: str) -> dict[str, Any]:
    path = root / f"success.{fmt}"
    process = launch_worker(path, fmt, SUCCESS_ROWS)
    observation = observe_path(path, process, timeout=60.0)
    stdout, stderr = process.communicate(timeout=60.0)
    summary = read_summary(path, fmt)
    return {
        "case": "success",
        "format": fmt,
        "rows_requested": SUCCESS_ROWS,
        "returncode": process.returncode,
        "stdout": stdout.strip(),
        "stderr": stderr.strip(),
        "observer": observation,
        "visible_before_process_exit": observation["first_visible_seconds"] is not None
        and observation["process_running_at_observer_end"],
        "postcondition": summary,
    }


def interrupt_case(root: Path, fmt: str) -> dict[str, Any]:
    path = root / f"interrupt.{fmt}"
    connection = duckdb.connect()
    connection.execute("SET threads=1")
    outcome: dict[str, Any] = {}

    def run_copy() -> None:
        try:
            connection.execute(copy_sql(path, fmt, INTERRUPT_ROWS))
            outcome["completed"] = True
        except Exception as exc:
            outcome.update(
                {
                    "completed": False,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )

    thread = threading.Thread(target=run_copy, name=f"copy-interrupt-{fmt}")
    started = time.monotonic()
    thread.start()
    first_visible: float | None = None
    max_size = 0
    interrupt_reason = "visibility-threshold"
    deadline = started + 20.0
    while thread.is_alive() and time.monotonic() < deadline:
        if path.exists():
            if first_visible is None:
                first_visible = time.monotonic() - started
            try:
                max_size = max(max_size, path.stat().st_size)
            except FileNotFoundError:
                pass
            if max_size >= 262_144:
                break
        time.sleep(POLL_SECONDS)
    else:
        interrupt_reason = "deadline-or-query-finished"

    interrupt_at = time.monotonic() - started
    connection.interrupt()
    thread.join(timeout=30.0)
    if thread.is_alive():
        outcome["join_timeout"] = True

    time.sleep(0.2)
    postcondition = read_summary(path, fmt)
    try:
        reusable_value = connection.execute("SELECT 7").fetchone()[0]
        reusable = reusable_value == 7
        reuse_error = None
    except Exception as exc:
        reusable = False
        reuse_error = f"{type(exc).__name__}: {exc}"
    connection.close()

    return {
        "case": "interrupt",
        "format": fmt,
        "rows_requested": INTERRUPT_ROWS,
        "first_visible_seconds": first_visible,
        "max_size_before_interrupt_bytes": max_size,
        "interrupt_at_seconds": interrupt_at,
        "interrupt_reason": interrupt_reason,
        "thread_finished": not thread.is_alive(),
        "outcome": outcome,
        "connection_reusable": reusable,
        "connection_reuse_error": reuse_error,
        "postcondition": postcondition,
    }


def crash_case(root: Path, fmt: str) -> dict[str, Any]:
    path = root / f"crash.{fmt}"
    process = launch_worker(path, fmt, CRASH_ROWS)
    started = time.monotonic()
    first_visible: float | None = None
    max_size = 0
    threshold_reached = False
    while time.monotonic() - started < 30.0:
        if path.exists():
            if first_visible is None:
                first_visible = time.monotonic() - started
            try:
                max_size = max(max_size, path.stat().st_size)
            except FileNotFoundError:
                pass
            if max_size >= CRASH_THRESHOLD_BYTES:
                threshold_reached = True
                break
        if process.poll() is not None:
            break
        time.sleep(POLL_SECONDS)

    killed = process.poll() is None
    if killed:
        process.kill()
    stdout, stderr = process.communicate(timeout=30.0)
    postcondition = read_summary(path, fmt)

    retry_connection = duckdb.connect()
    try:
        retry_connection.execute(copy_sql(path, fmt, 1_000))
        default_retry = {"succeeded": True}
    except Exception as exc:
        default_retry = {
            "succeeded": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
    finally:
        retry_connection.close()

    if path.exists():
        path.unlink()
    cleanup_retry_connection = duckdb.connect()
    try:
        cleanup_retry_connection.execute(copy_sql(path, fmt, 1_000))
        cleanup_retry = read_summary(path, fmt)
        cleanup_retry["succeeded"] = True
    except Exception as exc:
        cleanup_retry = {
            "succeeded": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
    finally:
        cleanup_retry_connection.close()

    return {
        "case": "crash",
        "format": fmt,
        "rows_requested": CRASH_ROWS,
        "threshold_bytes": CRASH_THRESHOLD_BYTES,
        "threshold_reached": threshold_reached,
        "first_visible_seconds": first_visible,
        "max_size_before_kill_bytes": max_size,
        "killed": killed,
        "returncode": process.returncode,
        "stdout": stdout.strip(),
        "stderr": stderr.strip(),
        "postcondition_before_cleanup": postcondition,
        "default_retry": default_retry,
        "cleanup_then_retry": cleanup_retry,
    }


def low_memory_case(root: Path, fmt: str) -> dict[str, Any]:
    path = root / f"oom.{fmt}"
    temp_dir = root / f"oom-{fmt}-tmp"
    temp_dir.mkdir()
    connection = duckdb.connect()
    connection.execute("SET threads=1")
    connection.execute("SET preserve_insertion_order=false")
    connection.execute("SET memory_limit='16MB'")
    connection.execute(f"SET temp_directory='{sql_path(temp_dir)}'")
    outcome: dict[str, Any] = {}

    def run_copy() -> None:
        try:
            connection.execute(copy_sql(path, fmt, OOM_ROWS, ordered=True))
            outcome["completed"] = True
        except Exception as exc:
            outcome.update(
                {
                    "completed": False,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )

    thread = threading.Thread(target=run_copy, name=f"copy-oom-{fmt}")
    thread.start()
    first_visible: float | None = None
    max_output_size = 0
    max_temp_bytes = 0
    while thread.is_alive():
        if path.exists():
            if first_visible is None:
                first_visible = time.monotonic()
            try:
                max_output_size = max(max_output_size, path.stat().st_size)
            except FileNotFoundError:
                pass
        temp_bytes = 0
        for entry in temp_dir.rglob("*"):
            if entry.is_file():
                try:
                    temp_bytes += entry.stat().st_size
                except FileNotFoundError:
                    pass
        max_temp_bytes = max(max_temp_bytes, temp_bytes)
        time.sleep(POLL_SECONDS)
    thread.join()
    time.sleep(0.2)

    postcondition = read_summary(path, fmt)
    temp_entries = sorted(str(entry.relative_to(temp_dir)) for entry in temp_dir.rglob("*"))
    try:
        reusable = connection.execute("SELECT 9").fetchone()[0] == 9
        reuse_error = None
    except Exception as exc:
        reusable = False
        reuse_error = f"{type(exc).__name__}: {exc}"
    connection.close()

    return {
        "case": "low_memory",
        "format": fmt,
        "rows_requested": OOM_ROWS,
        "memory_limit": "16MB",
        "first_visible_monotonic": first_visible,
        "max_output_size_bytes": max_output_size,
        "max_temp_bytes": max_temp_bytes,
        "outcome": outcome,
        "connection_reusable": reusable,
        "connection_reuse_error": reuse_error,
        "postcondition": postcondition,
        "temp_entries_after_failure": temp_entries,
    }


def evaluate(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []

    def add(name: str, passed: bool, detail: Any) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    by_key = {(case["case"], case["format"]): case for case in cases}
    for fmt in FORMATS:
        success = by_key[("success", fmt)]
        post = success["postcondition"]
        add(f"{fmt}.success.process", success["returncode"] == 0, success["returncode"])
        add(f"{fmt}.success.readable", post.get("readable") is True, post)
        add(f"{fmt}.success.row_count", post.get("row_count") == SUCCESS_ROWS, post.get("row_count"))
        add(
            f"{fmt}.success.checksum",
            post.get("checksum") == expected_sum(SUCCESS_ROWS),
            post.get("checksum"),
        )

        interrupted = by_key[("interrupt", fmt)]
        add(
            f"{fmt}.interrupt.raised",
            interrupted["outcome"].get("completed") is False,
            interrupted["outcome"],
        )
        add(
            f"{fmt}.interrupt.final_absent",
            interrupted["postcondition"].get("exists") is False,
            interrupted["postcondition"],
        )
        add(
            f"{fmt}.interrupt.connection_reusable",
            interrupted["connection_reusable"],
            interrupted["connection_reuse_error"],
        )

        crashed = by_key[("crash", fmt)]
        add(f"{fmt}.crash.threshold", crashed["threshold_reached"], crashed["max_size_before_kill_bytes"])
        add(f"{fmt}.crash.killed", crashed["killed"], crashed["returncode"])
        add(
            f"{fmt}.crash.final_remains",
            crashed["postcondition_before_cleanup"].get("exists") is True,
            crashed["postcondition_before_cleanup"],
        )
        add(
            f"{fmt}.crash.default_retry_blocked",
            crashed["default_retry"].get("succeeded") is False,
            crashed["default_retry"],
        )
        cleanup_retry = crashed["cleanup_then_retry"]
        add(
            f"{fmt}.crash.cleanup_retry",
            cleanup_retry.get("succeeded") is True and cleanup_retry.get("row_count") == 1_000,
            cleanup_retry,
        )

        oom = by_key[("low_memory", fmt)]
        add(
            f"{fmt}.oom.raised",
            oom["outcome"].get("completed") is False,
            oom["outcome"],
        )
        add(
            f"{fmt}.oom.final_absent",
            oom["postcondition"].get("exists") is False,
            oom["postcondition"],
        )
        add(
            f"{fmt}.oom.connection_reusable",
            oom["connection_reusable"],
            oom["connection_reuse_error"],
        )
        add(
            f"{fmt}.oom.temp_clean",
            not oom["temp_entries_after_failure"],
            oom["temp_entries_after_failure"],
        )

    return checks


def run_probe(output: Path) -> int:
    if duckdb.__version__ != EXPECTED_DUCKDB_VERSION:
        raise RuntimeError(
            f"expected DuckDB {EXPECTED_DUCKDB_VERSION}, got {duckdb.__version__}"
        )

    work_root = Path(tempfile.mkdtemp(prefix="fieldwork-duckdb-issue55-"))
    try:
        cases: list[dict[str, Any]] = []
        for fmt in FORMATS:
            cases.append(success_case(work_root, fmt))
            cases.append(interrupt_case(work_root, fmt))
            cases.append(crash_case(work_root, fmt))
            cases.append(low_memory_case(work_root, fmt))

        checks = evaluate(cases)
        result = {
            "probe": "fieldwork-issue55-file-sink-publication",
            "duckdb_version": duckdb.__version__,
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "pid": os.getpid(),
            "constants": {
                "success_rows": SUCCESS_ROWS,
                "interrupt_rows": INTERRUPT_ROWS,
                "crash_rows": CRASH_ROWS,
                "oom_rows": OOM_ROWS,
                "crash_threshold_bytes": CRASH_THRESHOLD_BYTES,
            },
            "cases": cases,
            "checks": checks,
            "passed": sum(1 for check in checks if check["passed"]),
            "failed": sum(1 for check in checks if not check["passed"]),
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps({"output": str(output), "passed": result["passed"], "failed": result["failed"]}))
        return 0 if result["failed"] == 0 else 1
    finally:
        shutil.rmtree(work_root, ignore_errors=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("issue55-results.json"))
    parser.add_argument("--worker-copy", nargs=3, metavar=("PATH", "FORMAT", "ROWS"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.worker_copy:
        path, fmt, rows = args.worker_copy
        return worker_copy(Path(path), fmt, int(rows))
    return run_probe(args.output)


if __name__ == "__main__":
    raise SystemExit(main())
