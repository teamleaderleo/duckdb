#!/usr/bin/env python3
"""Trace S3 request ordering around DuckDB max_execution_time failure."""

from __future__ import annotations

import argparse
import json
import os
import platform
import threading
import time
from pathlib import Path
from typing import Any

import duckdb

import issue103_s3_interrupt_trace as trace
import issue103_s3_interrupt_trace_v2 as hardened

base = trace.base
FORMATS = trace.FORMATS
ROWS = trace.ROWS
TIMEOUT_MS = 5_000
POLL_SECONDS = 0.05
WORKER_WAIT_SECONDS = 30.0


def finish_worker(
    connection: duckdb.DuckDBPyConnection,
    worker: threading.Thread,
    fmt: str,
    key: str,
) -> tuple[bool, bool]:
    """Wait for the deadline; use manual interrupt only as bounded emergency cleanup."""
    worker.join(timeout=WORKER_WAIT_SECONDS)
    if not worker.is_alive():
        return True, False

    trace.trace_event(
        "timeout_worker_still_running",
        format=fmt,
        key=key,
        after_seconds=WORKER_WAIT_SECONDS,
    )
    connection.interrupt()
    worker.join(timeout=10)
    return not worker.is_alive(), True


def run_case(client, fmt: str) -> dict[str, Any]:
    key = f"timeout/{fmt}/result.{fmt}"
    connection = duckdb.connect()
    trace.configure(connection)
    connection.execute(f"SET max_execution_time={TIMEOUT_MS}")
    outcome: dict[str, Any] = {}

    def execute() -> None:
        try:
            connection.execute(trace.copy_sql(key, fmt))
            outcome["completed"] = True
        except Exception as exc:
            outcome.update(
                completed=False,
                error_type=type(exc).__name__,
                error=str(exc),
            )
        finally:
            trace.trace_event(
                "timeout_query_thread_finished",
                format=fmt,
                key=key,
                outcome=dict(outcome),
            )

    worker = threading.Thread(
        target=execute,
        name=f"issue103-timeout-copy-{fmt}",
        daemon=True,
    )
    trace.trace_event(
        "timeout_case_start",
        format=fmt,
        key=key,
        timeout_ms=TIMEOUT_MS,
    )
    worker.start()

    first_upload: float | None = None
    first_part: float | None = None
    max_part_count = 0
    max_part_bytes = 0
    started = time.monotonic()
    while worker.is_alive() and time.monotonic() - started < WORKER_WAIT_SECONDS:
        state = base.snapshot(client, key)
        elapsed = time.monotonic() - started
        if state["uploads"] and first_upload is None:
            first_upload = elapsed
        if state["part_count"] and first_part is None:
            first_part = elapsed
        max_part_count = max(max_part_count, state["part_count"])
        max_part_bytes = max(max_part_bytes, state["part_bytes"])
        time.sleep(POLL_SECONDS)

    thread_finished, manual_cleanup_interrupt = finish_worker(connection, worker, fmt, key)
    time.sleep(0.5)

    after = base.snapshot(client, key)
    reader = (
        base.read_remote(key, fmt)
        if after["head"].get("exists")
        else {"readable": False, "absent": True}
    )

    if thread_finished:
        try:
            reusable = connection.execute("SELECT 103").fetchone()[0] == 103
            reuse_error = None
        except Exception as exc:
            reusable = False
            reuse_error = f"{type(exc).__name__}: {exc}"
        finally:
            connection.close()
    else:
        reusable = False
        reuse_error = "deadline and cleanup interrupt failed to stop worker"

    events = trace.request_events_for_key(key)
    finish_events = [
        event
        for event in trace.TRACE
        if event.get("kind") == "timeout_query_thread_finished"
        and event.get("key") == key
    ]
    finish_seconds = finish_events[-1]["seconds"] if finish_events else None
    successful_completions = [
        event
        for event in events
        if event.get("kind") == "request_end"
        and event.get("request_class") == "complete_multipart"
        and event.get("status", 500) < 300
    ]
    destructive_requests = [
        event
        for event in events
        if event.get("kind") == "request_end"
        and event.get("request_class") in {"delete_object", "abort_multipart"}
    ]

    result = {
        "format": fmt,
        "key": key,
        "timeout_ms": TIMEOUT_MS,
        "first_upload_seconds": first_upload,
        "first_part_seconds": first_part,
        "max_part_count": max_part_count,
        "max_part_bytes": max_part_bytes,
        "threshold_reached": max_part_count > 0,
        "outcome": outcome,
        "thread_finished": thread_finished,
        "manual_cleanup_interrupt": manual_cleanup_interrupt,
        "connection_reusable": reusable,
        "connection_reuse_error": reuse_error,
        "after": after,
        "reader": reader,
        "request_events": events,
        "finish_seconds": finish_seconds,
        "successful_completions": successful_completions,
        "completion_before_error_return": bool(
            finish_seconds is not None
            and any(event["seconds"] <= finish_seconds for event in successful_completions)
        ),
        "destructive_requests": destructive_requests,
    }

    if thread_finished:
        result["cleanup"] = {
            "aborted_uploads": base.abort_all_uploads(client),
            "deleted_objects": base.delete_all_objects(client),
        }
        result["cleanup"]["state_after"] = {
            "uploads": base.list_uploads(client),
            "objects": base.list_objects(client),
        }
    else:
        result["cleanup"] = {
            "skipped": True,
            "state_after": {
                "uploads": base.list_uploads(client),
                "objects": base.list_objects(client),
            },
        }
    return result


def build_checks(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []

    def add(name: str, passed: bool, detail: Any) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    for case in cases:
        fmt = case["format"]
        outcome = case["outcome"]
        add(f"{fmt}.part_observed_before_timeout", case["threshold_reached"], case)
        add(f"{fmt}.deadline_stopped_worker", case["thread_finished"], case)
        add(
            f"{fmt}.no_manual_interrupt_needed",
            not case["manual_cleanup_interrupt"],
            case,
        )
        add(
            f"{fmt}.deadline_interrupt_exception",
            outcome.get("completed") is False
            and outcome.get("error_type") == "InterruptException"
            and "maximum execution time" in outcome.get("error", "").lower(),
            outcome,
        )
        add(f"{fmt}.connection_reusable", case["connection_reusable"], case)
        add(f"{fmt}.final_object_exists", case["after"]["head"].get("exists") is True, case)
        add(
            f"{fmt}.multipart_completed_before_error_return",
            case["completion_before_error_return"],
            case["request_events"],
        )
        add(
            f"{fmt}.no_delete_or_abort",
            not case["destructive_requests"],
            case["destructive_requests"],
        )
        if fmt == "csv":
            add(
                "csv.timeout_reader_accepts_exact_prefix",
                trace.csv_prefix_exact(case["reader"]),
                case["reader"],
            )
        else:
            add(
                "parquet.timeout_reader_rejects_incomplete_file",
                case["reader"].get("readable") is False
                and "magic bytes" in case["reader"].get("error", "").lower(),
                case["reader"],
            )
        add(
            f"{fmt}.cleanup_empty",
            not case["cleanup"]["state_after"]["uploads"]
            and not case["cleanup"]["state_after"]["objects"],
            case["cleanup"],
        )
    return checks


def main(output: Path) -> int:
    with trace.TRACE_LOCK:
        trace.TRACE.clear()
        trace.REQUEST_COUNTER = 0
        trace.TRACE_START = time.monotonic()

    client = base.s3_client()
    hardened.ensure_bucket(client)
    base.abort_all_uploads(client)
    base.delete_all_objects(client)

    preparation = duckdb.connect()
    trace.configure(preparation, install=True)
    httpfs = base.extension_info(preparation)
    preparation.close()

    server, proxy_thread = trace.start_proxy()
    cases: list[dict[str, Any]] = []
    try:
        for fmt in FORMATS:
            case = run_case(client, fmt)
            cases.append(case)
            if not case["thread_finished"]:
                break
    finally:
        server.shutdown()
        server.server_close()
        proxy_thread.join(timeout=5)

    checks = build_checks(cases)
    result = {
        "metadata": {
            "probe": "issue103_s3_timeout_trace",
            "duckdb_version": duckdb.__version__,
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "httpfs": httpfs,
            "rows_requested": ROWS,
            "timeout_ms": TIMEOUT_MS,
            "proxy": f"http://{trace.PROXY_HOST}:{trace.PROXY_PORT}",
            "backend": f"http://{trace.BACKEND_HOST}:{trace.BACKEND_PORT}",
            "minio_release": os.environ.get("ISSUE96_MINIO_RELEASE"),
            "minio_sha256": os.environ.get("ISSUE96_MINIO_SHA256"),
        },
        "cases": cases,
        "trace": trace.TRACE,
        "checks": checks,
        "summary": {
            "passed": sum(1 for check in checks if check["passed"]),
            "failed": sum(1 for check in checks if not check["passed"]),
        },
    }
    output.write_text(json.dumps(result, indent=2, sort_keys=True, default=str) + "\n")
    print(json.dumps({"output": str(output), **result["summary"]}), flush=True)
    return 0 if result["summary"]["failed"] == 0 else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("issue103-s3-timeout-trace.json"),
    )
    raise SystemExit(main(parser.parse_args().output))
