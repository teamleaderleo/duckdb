#!/usr/bin/env python3
"""DuckDB temporary-file publication probe for Fieldwork issue 55.

The binder resolves USE_TMP_FILE differently for a fresh path and an existing
path. This probe tests the consumer-visible consequences for CSV and Parquet:

* explicit USE_TMP_FILE on a fresh destination;
* automatic temporary-file replacement when the destination already exists;
* interruption cleanup;
* abrupt process death and retry residue.

Generated deterministic data only.
"""

from __future__ import annotations

import argparse
import json
import platform
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
SEED_ROWS = 1_000
RETRY_ROWS = 2_000
SUCCESS_ROWS = 500_000
LONG_ROWS = 100_000_000
THRESHOLD = 1_048_576
INTERRUPT_THRESHOLD = 262_144
POLL_SECONDS = 0.002
TIMEOUT_SECONDS = 20.0


def quote(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def format_options(fmt: str) -> list[str]:
    if fmt == "csv":
        return ["FORMAT CSV", "HEADER true"]
    if fmt == "parquet":
        return ["FORMAT PARQUET", "COMPRESSION ZSTD"]
    raise ValueError(fmt)


def copy_sql(path: Path, fmt: str, rows: int, *, explicit_tmp: bool | None = None) -> str:
    options = format_options(fmt)
    if explicit_tmp is not None:
        options.append(f"USE_TMP_FILE {'true' if explicit_tmp else 'false'}")
    source = (
        "SELECT i::BIGINT AS i, "
        "md5(i::VARCHAR) || md5((i + 1)::VARCHAR) AS payload "
        f"FROM range({rows}) t(i)"
    )
    return f"COPY ({source}) TO '{quote(path)}' ({', '.join(options)})"


def expected_sum(rows: int) -> int:
    return rows * (rows - 1) // 2


def tmp_path(final_path: Path) -> Path:
    return final_path.with_name("tmp_" + final_path.name)


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


def execute_copy(path: Path, fmt: str, rows: int, *, explicit_tmp: bool | None = None) -> None:
    connection = duckdb.connect()
    connection.execute("SET threads=1")
    try:
        connection.execute(copy_sql(path, fmt, rows, explicit_tmp=explicit_tmp))
    finally:
        connection.close()


def worker(path: Path, fmt: str, rows: int, tmp_mode: str, marker: Path | None) -> int:
    explicit_tmp: bool | None
    if tmp_mode == "true":
        explicit_tmp = True
    elif tmp_mode == "false":
        explicit_tmp = False
    elif tmp_mode == "auto":
        explicit_tmp = None
    else:
        raise ValueError(tmp_mode)
    execute_copy(path, fmt, rows, explicit_tmp=explicit_tmp)
    if marker is not None:
        marker.write_text("statement-complete\n", encoding="utf-8")
    return 0


def spawn(path: Path, fmt: str, rows: int, tmp_mode: str, marker: Path | None = None) -> subprocess.Popen[str]:
    marker_arg = str(marker) if marker is not None else "-"
    return subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--worker",
            str(path),
            fmt,
            str(rows),
            tmp_mode,
            marker_arg,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def observe_until_threshold(
    process: subprocess.Popen[str],
    watched_path: Path,
    threshold: int,
    *,
    final_path: Path | None = None,
    marker: Path | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    first_watched_visible: float | None = None
    first_watched_nonzero: float | None = None
    first_final_visible: float | None = None
    final_visible_before_marker = False
    observed_size = 0
    samples = 0
    threshold_at: float | None = None

    while time.monotonic() - started < TIMEOUT_SECONDS:
        samples += 1
        elapsed = time.monotonic() - started
        marker_exists = marker.exists() if marker is not None else False
        if final_path is not None and final_path.exists():
            if first_final_visible is None:
                first_final_visible = elapsed
            if marker is not None and not marker_exists:
                final_visible_before_marker = True
        if watched_path.exists():
            if first_watched_visible is None:
                first_watched_visible = elapsed
            try:
                observed_size = watched_path.stat().st_size
            except FileNotFoundError:
                observed_size = 0
            if observed_size > 0 and first_watched_nonzero is None:
                first_watched_nonzero = elapsed
            if observed_size >= threshold:
                threshold_at = elapsed
                break
        if process.poll() is not None:
            break
        time.sleep(POLL_SECONDS)

    return {
        "first_watched_visible_seconds": first_watched_visible,
        "first_watched_nonzero_seconds": first_watched_nonzero,
        "first_final_visible_seconds": first_final_visible,
        "final_visible_before_marker": final_visible_before_marker,
        "observed_size_bytes": observed_size,
        "threshold_bytes": threshold,
        "threshold_at_seconds": threshold_at,
        "samples": samples,
        "process_running": process.poll() is None,
    }


def fresh_explicit_success(root: Path, fmt: str) -> dict[str, Any]:
    final = root / f"fresh-success.{fmt}"
    temporary = tmp_path(final)
    marker = root / f"fresh-success-{fmt}.complete"
    process = spawn(final, fmt, SUCCESS_ROWS, "true", marker)
    started = time.monotonic()
    first_tmp_visible: float | None = None
    first_tmp_nonzero: float | None = None
    first_final_visible: float | None = None
    final_visible_before_marker = False
    max_tmp_size = 0
    samples = 0

    while process.poll() is None and time.monotonic() - started < TIMEOUT_SECONDS:
        samples += 1
        elapsed = time.monotonic() - started
        marker_exists = marker.exists()
        if final.exists():
            if first_final_visible is None:
                first_final_visible = elapsed
            if not marker_exists:
                final_visible_before_marker = True
        if temporary.exists():
            if first_tmp_visible is None:
                first_tmp_visible = elapsed
            try:
                size = temporary.stat().st_size
            except FileNotFoundError:
                size = 0
            max_tmp_size = max(max_tmp_size, size)
            if size > 0 and first_tmp_nonzero is None:
                first_tmp_nonzero = elapsed
        time.sleep(POLL_SECONDS)

    stdout, stderr = process.communicate(timeout=20.0)
    return {
        "case": "fresh_explicit_success",
        "format": fmt,
        "returncode": process.returncode,
        "stdout": stdout.strip(),
        "stderr": stderr.strip(),
        "marker_exists": marker.exists(),
        "first_tmp_visible_seconds": first_tmp_visible,
        "first_tmp_nonzero_seconds": first_tmp_nonzero,
        "first_final_visible_seconds": first_final_visible,
        "final_visible_before_marker": final_visible_before_marker,
        "max_tmp_size_bytes": max_tmp_size,
        "samples": samples,
        "final_after": inspect(final, fmt),
        "tmp_after": inspect(temporary, fmt),
    }


def fresh_explicit_crash(root: Path, fmt: str) -> dict[str, Any]:
    final = root / f"fresh-crash.{fmt}"
    temporary = tmp_path(final)
    process = spawn(final, fmt, LONG_ROWS, "true")
    observation = observe_until_threshold(process, temporary, THRESHOLD, final_path=final)
    killed = observation["threshold_at_seconds"] is not None and process.poll() is None
    if killed:
        process.kill()
    stdout, stderr = process.communicate(timeout=20.0)
    final_after_kill = inspect(final, fmt)
    tmp_after_kill = inspect(temporary, fmt)

    explicit_retry_connection = duckdb.connect()
    try:
        explicit_retry_connection.execute(copy_sql(final, fmt, RETRY_ROWS, explicit_tmp=True))
        explicit_retry = {"succeeded": True, "final": inspect(final, fmt), "tmp": inspect(temporary, fmt)}
    except Exception as exc:
        explicit_retry = {"succeeded": False, "error_type": type(exc).__name__, "error": str(exc)}
    finally:
        explicit_retry_connection.close()

    if temporary.exists():
        temporary.unlink()
    if final.exists():
        final.unlink()
    default_retry_connection = duckdb.connect()
    try:
        default_retry_connection.execute(copy_sql(final, fmt, RETRY_ROWS))
        default_retry = {"succeeded": True, "final": inspect(final, fmt), "tmp": inspect(temporary, fmt)}
    except Exception as exc:
        default_retry = {"succeeded": False, "error_type": type(exc).__name__, "error": str(exc)}
    finally:
        default_retry_connection.close()

    return {
        "case": "fresh_explicit_crash",
        "format": fmt,
        "observation": observation,
        "killed": killed,
        "returncode": process.returncode,
        "stdout": stdout.strip(),
        "stderr": stderr.strip(),
        "final_after_kill": final_after_kill,
        "tmp_after_kill": tmp_after_kill,
        "explicit_retry_with_residue": explicit_retry,
        "cleanup_then_default_retry": default_retry,
    }


def existing_default_interrupt(root: Path, fmt: str) -> dict[str, Any]:
    final = root / f"existing-interrupt.{fmt}"
    temporary = tmp_path(final)
    execute_copy(final, fmt, SEED_ROWS)
    seed = inspect(final, fmt)

    connection = duckdb.connect()
    connection.execute("SET threads=1")
    outcome: dict[str, Any] = {}

    def run() -> None:
        try:
            connection.execute(copy_sql(final, fmt, LONG_ROWS))
            outcome["completed"] = True
        except Exception as exc:
            outcome.update(completed=False, error_type=type(exc).__name__, error=str(exc))

    thread = threading.Thread(target=run, name=f"issue55-existing-interrupt-{fmt}")
    started = time.monotonic()
    thread.start()
    first_tmp_visible: float | None = None
    max_tmp_size = 0
    while thread.is_alive() and time.monotonic() - started < TIMEOUT_SECONDS:
        if temporary.exists():
            if first_tmp_visible is None:
                first_tmp_visible = time.monotonic() - started
            try:
                max_tmp_size = max(max_tmp_size, temporary.stat().st_size)
            except FileNotFoundError:
                pass
            if max_tmp_size >= INTERRUPT_THRESHOLD:
                break
        time.sleep(POLL_SECONDS)
    connection.interrupt()
    interrupted_at = time.monotonic() - started
    thread.join(timeout=20.0)
    time.sleep(0.1)
    final_after = inspect(final, fmt)
    tmp_after = inspect(temporary, fmt)
    try:
        reusable = connection.execute("SELECT 11").fetchone()[0] == 11
        reuse_error = None
    except Exception as exc:
        reusable = False
        reuse_error = f"{type(exc).__name__}: {exc}"
    connection.close()

    return {
        "case": "existing_default_interrupt",
        "format": fmt,
        "seed": seed,
        "first_tmp_visible_seconds": first_tmp_visible,
        "max_tmp_size_before_interrupt_bytes": max_tmp_size,
        "interrupted_at_seconds": interrupted_at,
        "threshold_reached": max_tmp_size >= INTERRUPT_THRESHOLD,
        "thread_finished": not thread.is_alive(),
        "outcome": outcome,
        "connection_reusable": reusable,
        "connection_reuse_error": reuse_error,
        "final_after": final_after,
        "tmp_after": tmp_after,
    }


def existing_default_crash(root: Path, fmt: str) -> dict[str, Any]:
    final = root / f"existing-crash.{fmt}"
    temporary = tmp_path(final)
    execute_copy(final, fmt, SEED_ROWS)
    seed = inspect(final, fmt)
    process = spawn(final, fmt, LONG_ROWS, "auto")
    observation = observe_until_threshold(process, temporary, THRESHOLD, final_path=final)
    killed = observation["threshold_at_seconds"] is not None and process.poll() is None
    if killed:
        process.kill()
    stdout, stderr = process.communicate(timeout=20.0)
    final_after_kill = inspect(final, fmt)
    tmp_after_kill = inspect(temporary, fmt)

    retry_connection = duckdb.connect()
    try:
        retry_connection.execute(copy_sql(final, fmt, RETRY_ROWS))
        retry = {"succeeded": True, "final": inspect(final, fmt), "tmp": inspect(temporary, fmt)}
    except Exception as exc:
        retry = {"succeeded": False, "error_type": type(exc).__name__, "error": str(exc)}
    finally:
        retry_connection.close()

    return {
        "case": "existing_default_crash",
        "format": fmt,
        "seed": seed,
        "observation": observation,
        "killed": killed,
        "returncode": process.returncode,
        "stdout": stdout.strip(),
        "stderr": stderr.strip(),
        "final_after_kill": final_after_kill,
        "tmp_after_kill": tmp_after_kill,
        "default_retry_with_tmp_residue": retry,
    }


def same_snapshot(left: dict[str, Any], right: dict[str, Any], rows: int) -> bool:
    return (
        left.get("readable") is True
        and right.get("readable") is True
        and left.get("row_count") == rows
        and right.get("row_count") == rows
        and left.get("checksum") == expected_sum(rows)
        and right.get("checksum") == expected_sum(rows)
    )


def evaluate(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []

    def add(name: str, passed: bool, detail: Any) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    indexed = {(case["case"], case["format"]): case for case in cases}
    for fmt in FORMATS:
        case = indexed[("fresh_explicit_success", fmt)]
        add(f"{fmt}.fresh_success.process", case["returncode"] == 0, case)
        add(f"{fmt}.fresh_success.tmp_observed", case["first_tmp_visible_seconds"] is not None, case)
        add(f"{fmt}.fresh_success.tmp_nonzero", case["first_tmp_nonzero_seconds"] is not None, case)
        add(f"{fmt}.fresh_success.final_not_early", not case["final_visible_before_marker"], case)
        add(f"{fmt}.fresh_success.final_exact", same_snapshot(case["final_after"], case["final_after"], SUCCESS_ROWS), case["final_after"])
        add(f"{fmt}.fresh_success.tmp_absent", case["tmp_after"].get("exists") is False, case["tmp_after"])

        case = indexed[("fresh_explicit_crash", fmt)]
        add(f"{fmt}.fresh_crash.threshold", case["observation"]["threshold_at_seconds"] is not None, case)
        add(f"{fmt}.fresh_crash.killed", case["killed"] and case["returncode"] == -9, case)
        add(f"{fmt}.fresh_crash.final_absent", case["final_after_kill"].get("exists") is False, case["final_after_kill"])
        add(f"{fmt}.fresh_crash.tmp_remains", case["tmp_after_kill"].get("exists") is True, case["tmp_after_kill"])
        add(f"{fmt}.fresh_crash.explicit_retry_result_recorded", "succeeded" in case["explicit_retry_with_residue"], case["explicit_retry_with_residue"])
        retry = case["cleanup_then_default_retry"]
        add(f"{fmt}.fresh_crash.cleanup_retry", retry.get("succeeded") is True and retry.get("final", {}).get("row_count") == RETRY_ROWS, retry)

        case = indexed[("existing_default_interrupt", fmt)]
        add(f"{fmt}.existing_interrupt.threshold", case["threshold_reached"], case)
        add(f"{fmt}.existing_interrupt.raised", case["outcome"].get("completed") is False, case["outcome"])
        add(f"{fmt}.existing_interrupt.final_preserved", same_snapshot(case["seed"], case["final_after"], SEED_ROWS), case)
        add(f"{fmt}.existing_interrupt.tmp_clean", case["tmp_after"].get("exists") is False, case["tmp_after"])
        add(f"{fmt}.existing_interrupt.reusable", case["connection_reusable"], case["connection_reuse_error"])

        case = indexed[("existing_default_crash", fmt)]
        add(f"{fmt}.existing_crash.threshold", case["observation"]["threshold_at_seconds"] is not None, case)
        add(f"{fmt}.existing_crash.killed", case["killed"] and case["returncode"] == -9, case)
        add(f"{fmt}.existing_crash.final_preserved", same_snapshot(case["seed"], case["final_after_kill"], SEED_ROWS), case)
        add(f"{fmt}.existing_crash.tmp_remains", case["tmp_after_kill"].get("exists") is True, case["tmp_after_kill"])
        retry = case["default_retry_with_tmp_residue"]
        add(f"{fmt}.existing_crash.retry_recorded", "succeeded" in retry, retry)
        if retry.get("succeeded"):
            add(f"{fmt}.existing_crash.retry_exact", retry.get("final", {}).get("row_count") == RETRY_ROWS and retry.get("final", {}).get("checksum") == expected_sum(RETRY_ROWS), retry)

    return checks


def run(output: Path) -> int:
    if duckdb.__version__ != VERSION:
        raise RuntimeError(f"expected DuckDB {VERSION}, got {duckdb.__version__}")
    with tempfile.TemporaryDirectory(prefix="fieldwork-duckdb-issue55-tmp-") as directory:
        root = Path(directory)
        cases: list[dict[str, Any]] = []
        for fmt in FORMATS:
            cases.extend(
                [
                    fresh_explicit_success(root, fmt),
                    fresh_explicit_crash(root, fmt),
                    existing_default_interrupt(root, fmt),
                    existing_default_crash(root, fmt),
                ]
            )
    checks = evaluate(cases)
    document = {
        "probe": "fieldwork-issue55-tmp-publication",
        "duckdb_version": duckdb.__version__,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "constants": {
            "seed_rows": SEED_ROWS,
            "retry_rows": RETRY_ROWS,
            "success_rows": SUCCESS_ROWS,
            "long_rows": LONG_ROWS,
            "threshold_bytes": THRESHOLD,
            "interrupt_threshold_bytes": INTERRUPT_THRESHOLD,
        },
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
    parser.add_argument("--output", type=Path, default=Path("issue55-tmp-publication.json"))
    parser.add_argument("--worker", nargs=5, metavar=("PATH", "FORMAT", "ROWS", "TMP_MODE", "MARKER"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.worker:
        path, fmt, rows, tmp_mode, marker_arg = args.worker
        marker = None if marker_arg == "-" else Path(marker_arg)
        return worker(Path(path), fmt, int(rows), tmp_mode, marker)
    return run(args.output)


if __name__ == "__main__":
    raise SystemExit(main())
