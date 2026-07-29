#!/usr/bin/env python3
"""Hardened request-order trace for interrupted DuckDB S3 COPY (Fieldwork issue 103)."""

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
from botocore.exceptions import ClientError

import issue103_s3_interrupt_trace as trace

base = trace.base
ROWS = trace.ROWS
FORMATS = trace.FORMATS
POLL_SECONDS = trace.POLL_SECONDS
WAIT_SECONDS = trace.WAIT_SECONDS


def ensure_bucket(client) -> None:
    """Create the owned bucket, suppressing only the expected already-exists responses."""
    try:
        client.create_bucket(Bucket=base.BUCKET)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"} and status != 409:
            raise


def finish_worker(
    connection: duckdb.DuckDBPyConnection,
    worker: threading.Thread,
    fmt: str,
    key: str,
) -> bool:
    """Bound worker shutdown and report its actual state before connection reuse."""
    worker.join(timeout=30)
    if not worker.is_alive():
        trace.trace_event("query_thread_finished", format=fmt, key=key)
        return True

    trace.trace_event("query_thread_still_running", format=fmt, key=key, after_seconds=30)
    connection.interrupt()
    worker.join(timeout=10)
    finished = not worker.is_alive()
    trace.trace_event(
        "query_thread_cleanup_result",
        format=fmt,
        key=key,
        finished=finished,
        second_join_seconds=10,
    )
    return finished


def run_case(client, fmt: str) -> dict[str, Any]:
    key = f"trace-v2/{fmt}/result.{fmt}"
    connection = duckdb.connect()
    trace.configure(connection)
    outcome: dict[str, Any] = {}

    def execute() -> None:
        try:
            connection.execute(trace.copy_sql(key, fmt))
            outcome["completed"] = True
        except Exception as exc:  # evidence records the exact binding exception type and text
            outcome.update(
                completed=False,
                error_type=type(exc).__name__,
                error=str(exc),
            )

    worker = threading.Thread(
        target=execute,
        name=f"issue103-v2-copy-{fmt}",
        daemon=True,
    )
    trace.trace_event("case_start", format=fmt, key=key)
    worker.start()

    first_upload: float | None = None
    first_part: float | None = None
    observed_part_count = 0
    observed_part_bytes = 0
    started = time.monotonic()
    while time.monotonic() - started < WAIT_SECONDS and worker.is_alive():
        state = base.snapshot(client, key)
        if state["uploads"] and first_upload is None:
            first_upload = time.monotonic() - started
        if state["part_count"]:
            observed_part_count = state["part_count"]
            observed_part_bytes = state["part_bytes"]
            first_part = time.monotonic() - started
            break
        time.sleep(POLL_SECONDS)

    trace.trace_event(
        "interrupt_called",
        format=fmt,
        key=key,
        observed_part_count=observed_part_count,
        observed_part_bytes=observed_part_bytes,
    )
    connection.interrupt()
    thread_finished = finish_worker(connection, worker, fmt, key)
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
        reuse_error = "query worker remained alive; connection reuse deliberately skipped"

    events = trace.request_events_for_key(key)
    interrupt_seconds = next(
        event["seconds"]
        for event in trace.TRACE
        if event["kind"] == "interrupt_called" and event.get("key") == key
    )
    completions = [
        event
        for event in events
        if event.get("kind") == "request_end"
        and event.get("request_class") == "complete_multipart"
        and event.get("status", 500) < 300
    ]
    heads_after_interrupt = [
        event
        for event in events
        if event.get("kind") == "request_end"
        and event.get("request_class") == "head_object"
        and event["seconds"] >= interrupt_seconds
    ]
    destructive_requests_after_interrupt = [
        event
        for event in events
        if event.get("kind") == "request_end"
        and event.get("request_class") in {"delete_object", "abort_multipart"}
        and event["seconds"] >= interrupt_seconds
    ]

    result = {
        "format": fmt,
        "key": key,
        "threshold_reached": observed_part_count > 0,
        "first_upload_seconds": first_upload,
        "first_part_seconds": first_part,
        "observed_part_count": observed_part_count,
        "observed_part_bytes": observed_part_bytes,
        "outcome": outcome,
        "thread_finished": thread_finished,
        "connection_reusable": reusable,
        "connection_reuse_error": reuse_error,
        "after": after,
        "reader": reader,
        "request_events": events,
        "completion_after_interrupt": any(
            event["seconds"] >= interrupt_seconds for event in completions
        ),
        "heads_after_interrupt": heads_after_interrupt,
        "destructive_requests_after_interrupt": destructive_requests_after_interrupt,
    }

    if thread_finished:
        trace.trace_event("cleanup_start", format=fmt, key=key)
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
            "reason": "worker still owns the DuckDB connection",
            "state_after": {
                "uploads": base.list_uploads(client),
                "objects": base.list_objects(client),
            },
        }
    return result


def build_checks(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    checks = trace.build_checks(cases)
    for case in cases:
        checks.append(
            {
                "name": f"{case['format']}.thread_finished",
                "passed": case["thread_finished"],
                "detail": {
                    "thread_finished": case["thread_finished"],
                    "outcome": case["outcome"],
                },
            }
        )
    return checks


def main(output: Path) -> int:
    with trace.TRACE_LOCK:
        trace.TRACE.clear()
        trace.REQUEST_COUNTER = 0
        trace.TRACE_START = time.monotonic()

    client = base.s3_client()
    ensure_bucket(client)
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
            "probe_revision": 2,
            "duckdb_version": duckdb.__version__,
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "httpfs": httpfs,
            "rows_requested": ROWS,
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
        default=Path("issue103-s3-interrupt-trace-v2.json"),
    )
    raise SystemExit(main(parser.parse_args().output))
