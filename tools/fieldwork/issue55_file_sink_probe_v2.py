#!/usr/bin/env python3
"""Second deterministic publication probe for Fieldwork issue 55.

V1 intentionally remains in the branch because it recorded a failed hypothesis:
a default COPY retry was expected to reject crash residue, but DuckDB 1.5.5
replaced the residue. V2 records that behavior and fixes observer timestamps.
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

VERSION = "1.5.5"
FORMATS = ("csv", "parquet")
SUCCESS_ROWS = 500_000
INTERRUPT_ROWS = 50_000_000
CRASH_ROWS = 100_000_000
OOM_ROWS = 2_000_000
INTERRUPT_THRESHOLD = 262_144
CRASH_THRESHOLD = 1_048_576
POLL = 0.01


def quoted(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def options(fmt: str) -> str:
    if fmt == "csv":
        return "FORMAT CSV, HEADER true"
    if fmt == "parquet":
        return "FORMAT PARQUET, COMPRESSION ZSTD"
    raise ValueError(fmt)


def query(path: Path, fmt: str, rows: int, ordered: bool = False) -> str:
    source = (
        "SELECT i::BIGINT AS i, "
        "md5(i::VARCHAR) || md5((i + 1)::VARCHAR) AS payload "
        f"FROM range({rows}) t(i)"
    )
    if ordered:
        source += " ORDER BY hash(i)"
    return f"COPY ({source}) TO '{quoted(path)}' ({options(fmt)})"


def checksum(rows: int) -> int:
    return rows * (rows - 1) // 2


def inspect(path: Path, fmt: str) -> dict[str, Any]:
    result: dict[str, Any] = {"exists": path.exists()}
    if not path.exists():
        return result
    result["size_bytes"] = path.stat().st_size
    relation = (
        f"read_csv_auto('{quoted(path)}', header=true)"
        if fmt == "csv"
        else f"read_parquet('{quoted(path)}')"
    )
    con = duckdb.connect()
    try:
        count, total, minimum, maximum = con.execute(
            f"SELECT count(*), sum(i), min(i), max(i) FROM {relation}"
        ).fetchone()
        result.update(
            readable=True,
            row_count=count,
            checksum=total,
            minimum=minimum,
            maximum=maximum,
        )
    except Exception as exc:
        result.update(readable=False, error_type=type(exc).__name__, error=str(exc))
    finally:
        con.close()
    return result


def worker(path: Path, fmt: str, rows: int) -> int:
    con = duckdb.connect()
    con.execute("SET threads=1")
    try:
        con.execute(query(path, fmt, rows))
        print(json.dumps({"ok": True, "rows": rows}), flush=True)
        return 0
    except Exception as exc:
        print(
            json.dumps({"ok": False, "error_type": type(exc).__name__, "error": str(exc)}),
            flush=True,
        )
        return 2
    finally:
        con.close()


def spawn(path: Path, fmt: str, rows: int) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), "--worker", str(path), fmt, str(rows)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def observe_process(path: Path, process: subprocess.Popen[str], timeout: float) -> dict[str, Any]:
    start = time.monotonic()
    first_visible: float | None = None
    first_nonzero: float | None = None
    visible_while_running = False
    nonzero_while_running = False
    max_size = 0
    samples = 0
    while time.monotonic() - start < timeout:
        samples += 1
        running = process.poll() is None
        exists = path.exists()
        now = time.monotonic() - start
        if exists:
            if first_visible is None:
                first_visible = now
            if running:
                visible_while_running = True
            try:
                size = path.stat().st_size
            except FileNotFoundError:
                size = 0
            max_size = max(max_size, size)
            if size > 0:
                if first_nonzero is None:
                    first_nonzero = now
                if running:
                    nonzero_while_running = True
        if not running:
            break
        time.sleep(POLL)
    return {
        "first_visible_seconds": first_visible,
        "first_nonzero_seconds": first_nonzero,
        "visible_while_running": visible_while_running,
        "nonzero_while_running": nonzero_while_running,
        "max_observed_size_bytes": max_size,
        "samples": samples,
    }


def normal(root: Path, fmt: str) -> dict[str, Any]:
    path = root / f"normal.{fmt}"
    process = spawn(path, fmt, SUCCESS_ROWS)
    observer = observe_process(path, process, 60.0)
    stdout, stderr = process.communicate(timeout=60.0)
    return {
        "case": "normal",
        "format": fmt,
        "returncode": process.returncode,
        "stdout": stdout.strip(),
        "stderr": stderr.strip(),
        "observer": observer,
        "after": inspect(path, fmt),
    }


def interrupted(root: Path, fmt: str) -> dict[str, Any]:
    path = root / f"interrupt.{fmt}"
    con = duckdb.connect()
    con.execute("SET threads=1")
    outcome: dict[str, Any] = {}

    def execute() -> None:
        try:
            con.execute(query(path, fmt, INTERRUPT_ROWS))
            outcome["completed"] = True
        except Exception as exc:
            outcome.update(completed=False, error_type=type(exc).__name__, error=str(exc))

    thread = threading.Thread(target=execute, name=f"issue55-interrupt-{fmt}")
    start = time.monotonic()
    thread.start()
    first_visible: float | None = None
    max_size = 0
    while thread.is_alive() and time.monotonic() - start < 20.0:
        if path.exists():
            if first_visible is None:
                first_visible = time.monotonic() - start
            try:
                max_size = max(max_size, path.stat().st_size)
            except FileNotFoundError:
                pass
            if max_size >= INTERRUPT_THRESHOLD:
                break
        time.sleep(POLL)
    con.interrupt()
    interrupted_at = time.monotonic() - start
    thread.join(timeout=30.0)
    time.sleep(0.2)
    after = inspect(path, fmt)
    try:
        reusable = con.execute("SELECT 7").fetchone()[0] == 7
        reuse_error = None
    except Exception as exc:
        reusable = False
        reuse_error = f"{type(exc).__name__}: {exc}"
    con.close()
    return {
        "case": "interrupt",
        "format": fmt,
        "first_visible_seconds": first_visible,
        "max_size_before_interrupt_bytes": max_size,
        "interrupted_at_seconds": interrupted_at,
        "threshold_reached": max_size >= INTERRUPT_THRESHOLD,
        "thread_finished": not thread.is_alive(),
        "outcome": outcome,
        "connection_reusable": reusable,
        "connection_reuse_error": reuse_error,
        "after": after,
    }


def crashed(root: Path, fmt: str) -> dict[str, Any]:
    path = root / f"crash.{fmt}"
    process = spawn(path, fmt, CRASH_ROWS)
    observer = observe_process(path, process, 30.0)
    threshold_reached = observer["max_observed_size_bytes"] >= CRASH_THRESHOLD
    killed = process.poll() is None
    if killed:
        process.kill()
    stdout, stderr = process.communicate(timeout=30.0)
    residue = inspect(path, fmt)

    con = duckdb.connect()
    try:
        con.execute(query(path, fmt, 1_000))
        retry = {"succeeded": True, "after": inspect(path, fmt)}
    except Exception as exc:
        retry = {"succeeded": False, "error_type": type(exc).__name__, "error": str(exc)}
    finally:
        con.close()

    return {
        "case": "crash",
        "format": fmt,
        "threshold_bytes": CRASH_THRESHOLD,
        "threshold_reached": threshold_reached,
        "killed": killed,
        "returncode": process.returncode,
        "stdout": stdout.strip(),
        "stderr": stderr.strip(),
        "observer": observer,
        "residue_before_retry": residue,
        "default_retry": retry,
    }


def low_memory(root: Path, fmt: str) -> dict[str, Any]:
    path = root / f"oom.{fmt}"
    temp_dir = root / f"oom-{fmt}-tmp"
    temp_dir.mkdir()
    con = duckdb.connect()
    con.execute("SET threads=1")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='16MB'")
    con.execute(f"SET temp_directory='{quoted(temp_dir)}'")
    outcome: dict[str, Any] = {}

    def execute() -> None:
        try:
            con.execute(query(path, fmt, OOM_ROWS, ordered=True))
            outcome["completed"] = True
        except Exception as exc:
            outcome.update(completed=False, error_type=type(exc).__name__, error=str(exc))

    thread = threading.Thread(target=execute, name=f"issue55-oom-{fmt}")
    start = time.monotonic()
    thread.start()
    first_visible: float | None = None
    visible_while_running = False
    max_output = 0
    max_temp = 0
    while thread.is_alive():
        if path.exists():
            if first_visible is None:
                first_visible = time.monotonic() - start
            visible_while_running = True
            try:
                max_output = max(max_output, path.stat().st_size)
            except FileNotFoundError:
                pass
        temp_bytes = 0
        for entry in temp_dir.rglob("*"):
            if entry.is_file():
                try:
                    temp_bytes += entry.stat().st_size
                except FileNotFoundError:
                    pass
        max_temp = max(max_temp, temp_bytes)
        time.sleep(POLL)
    thread.join()
    time.sleep(0.2)
    after = inspect(path, fmt)
    residue = sorted(str(entry.relative_to(temp_dir)) for entry in temp_dir.rglob("*"))
    try:
        reusable = con.execute("SELECT 9").fetchone()[0] == 9
        reuse_error = None
    except Exception as exc:
        reusable = False
        reuse_error = f"{type(exc).__name__}: {exc}"
    con.close()
    return {
        "case": "low_memory",
        "format": fmt,
        "memory_limit": "16MB",
        "first_visible_seconds": first_visible,
        "visible_while_running": visible_while_running,
        "max_output_size_bytes": max_output,
        "max_temp_bytes": max_temp,
        "outcome": outcome,
        "connection_reusable": reusable,
        "connection_reuse_error": reuse_error,
        "after": after,
        "temp_entries_after_failure": residue,
    }


def checks(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    indexed = {(case["case"], case["format"]): case for case in cases}

    def add(name: str, passed: bool, detail: Any) -> None:
        result.append({"name": name, "passed": bool(passed), "detail": detail})

    for fmt in FORMATS:
        case = indexed[("normal", fmt)]
        after = case["after"]
        add(f"{fmt}.normal.returncode", case["returncode"] == 0, case["returncode"])
        add(f"{fmt}.normal.visible_while_running", case["observer"]["visible_while_running"], case["observer"])
        add(f"{fmt}.normal.nonzero_while_running", case["observer"]["nonzero_while_running"], case["observer"])
        add(f"{fmt}.normal.readable", after.get("readable") is True, after)
        add(f"{fmt}.normal.count", after.get("row_count") == SUCCESS_ROWS, after.get("row_count"))
        add(f"{fmt}.normal.checksum", after.get("checksum") == checksum(SUCCESS_ROWS), after.get("checksum"))

        case = indexed[("interrupt", fmt)]
        add(f"{fmt}.interrupt.threshold", case["threshold_reached"], case["max_size_before_interrupt_bytes"])
        add(f"{fmt}.interrupt.raised", case["outcome"].get("completed") is False, case["outcome"])
        add(f"{fmt}.interrupt.final_absent", case["after"].get("exists") is False, case["after"])
        add(f"{fmt}.interrupt.reusable", case["connection_reusable"], case["connection_reuse_error"])

        case = indexed[("crash", fmt)]
        residue = case["residue_before_retry"]
        retry = case["default_retry"]
        add(f"{fmt}.crash.threshold", case["threshold_reached"], case["observer"])
        add(f"{fmt}.crash.killed", case["killed"], case["returncode"])
        add(f"{fmt}.crash.final_remains", residue.get("exists") is True, residue)
        if fmt == "csv":
            partial = residue.get("readable") is True and 0 < residue.get("row_count", 0) < CRASH_ROWS
            add("csv.crash.partial_readable", partial, residue)
        else:
            add("parquet.crash.incomplete_rejected", residue.get("readable") is False, residue)
        retry_after = retry.get("after", {})
        add(f"{fmt}.crash.default_retry_succeeds", retry.get("succeeded") is True, retry)
        add(f"{fmt}.crash.default_retry_count", retry_after.get("row_count") == 1_000, retry_after)
        add(f"{fmt}.crash.default_retry_checksum", retry_after.get("checksum") == checksum(1_000), retry_after)

        case = indexed[("low_memory", fmt)]
        add(f"{fmt}.oom.raised", case["outcome"].get("completed") is False, case["outcome"])
        add(f"{fmt}.oom.final_absent", case["after"].get("exists") is False, case["after"])
        add(f"{fmt}.oom.reusable", case["connection_reusable"], case["connection_reuse_error"])
        add(f"{fmt}.oom.temp_clean", not case["temp_entries_after_failure"], case["temp_entries_after_failure"])

    return result


def run(output: Path) -> int:
    if duckdb.__version__ != VERSION:
        raise RuntimeError(f"expected DuckDB {VERSION}, got {duckdb.__version__}")
    root = Path(tempfile.mkdtemp(prefix="fieldwork-duckdb-issue55-v2-"))
    try:
        cases: list[dict[str, Any]] = []
        for fmt in FORMATS:
            cases.extend([normal(root, fmt), interrupted(root, fmt), crashed(root, fmt), low_memory(root, fmt)])
        evaluated = checks(cases)
        document = {
            "probe": "fieldwork-issue55-file-sink-publication-v2",
            "duckdb_version": duckdb.__version__,
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "pid": os.getpid(),
            "constants": {
                "success_rows": SUCCESS_ROWS,
                "interrupt_rows": INTERRUPT_ROWS,
                "crash_rows": CRASH_ROWS,
                "oom_rows": OOM_ROWS,
                "interrupt_threshold_bytes": INTERRUPT_THRESHOLD,
                "crash_threshold_bytes": CRASH_THRESHOLD,
            },
            "cases": cases,
            "checks": evaluated,
            "passed": sum(check["passed"] for check in evaluated),
            "failed": sum(not check["passed"] for check in evaluated),
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps({"output": str(output), "passed": document["passed"], "failed": document["failed"]}))
        return 0 if document["failed"] == 0 else 1
    finally:
        shutil.rmtree(root, ignore_errors=True)


def args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("issue55-results-v2.json"))
    parser.add_argument("--worker", nargs=3, metavar=("PATH", "FORMAT", "ROWS"))
    return parser.parse_args()


def main() -> int:
    parsed = args()
    if parsed.worker:
        path, fmt, rows = parsed.worker
        return worker(Path(path), fmt, int(rows))
    return run(parsed.output)


if __name__ == "__main__":
    raise SystemExit(main())
