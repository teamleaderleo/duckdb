#!/usr/bin/env python3
"""Trace S3 request ordering around interrupted DuckDB COPY for Fieldwork issue 103."""

from __future__ import annotations

import argparse
import http.client
import json
import os
import platform
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import duckdb

import issue96_remote_publication_probe as base

PROXY_HOST = "127.0.0.1"
PROXY_PORT = 9002
BACKEND_HOST = "127.0.0.1"
BACKEND_PORT = 9000
ROWS = 50_000_000
FORMATS = ("csv", "parquet")
POLL_SECONDS = 0.02
WAIT_SECONDS = 45.0

TRACE_LOCK = threading.Lock()
TRACE_START = time.monotonic()
TRACE: list[dict[str, Any]] = []
REQUEST_COUNTER = 0

HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


def trace_event(kind: str, **fields: Any) -> None:
    with TRACE_LOCK:
        TRACE.append(
            {
                "kind": kind,
                "seconds": time.monotonic() - TRACE_START,
                **fields,
            }
        )


def classify_request(method: str, path: str) -> str:
    parsed = urllib.parse.urlsplit(path)
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    if method == "POST" and "uploads" in query:
        return "create_multipart"
    if method == "PUT" and "partNumber" in query and "uploadId" in query:
        return "upload_part"
    if method == "POST" and "uploadId" in query:
        return "complete_multipart"
    if method == "DELETE" and "uploadId" in query:
        return "abort_multipart"
    if method == "DELETE":
        return "delete_object"
    if method == "HEAD":
        return "head_object"
    if method == "GET":
        return "get_object_or_list"
    return "other"


class S3TraceProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        return

    def _forward(self) -> None:
        global REQUEST_COUNTER
        with TRACE_LOCK:
            REQUEST_COUNTER += 1
            request_id = REQUEST_COUNTER

        content_length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(content_length) if content_length else None
        request_class = classify_request(self.command, self.path)
        trace_event(
            "request_start",
            request_id=request_id,
            method=self.command,
            path=self.path,
            request_class=request_class,
            content_length=content_length,
        )

        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() not in HOP_BY_HOP
        }
        connection = http.client.HTTPConnection(BACKEND_HOST, BACKEND_PORT, timeout=120)
        try:
            connection.request(self.command, self.path, body=body, headers=headers)
            response = connection.getresponse()
            response_body = response.read()
            self.send_response(response.status, response.reason)
            for key, value in response.getheaders():
                lower = key.lower()
                if lower in HOP_BY_HOP or lower == "content-length":
                    continue
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(response_body)))
            self.end_headers()
            if self.command != "HEAD" and response_body:
                self.wfile.write(response_body)
            self.wfile.flush()
            trace_event(
                "request_end",
                request_id=request_id,
                method=self.command,
                path=self.path,
                request_class=request_class,
                status=response.status,
                response_bytes=len(response_body),
            )
        except Exception as exc:
            trace_event(
                "proxy_error",
                request_id=request_id,
                method=self.command,
                path=self.path,
                request_class=request_class,
                error=f"{type(exc).__name__}: {exc}",
            )
            try:
                self.send_error(502, str(exc))
            except Exception:
                pass
        finally:
            connection.close()

    do_GET = _forward
    do_HEAD = _forward
    do_POST = _forward
    do_PUT = _forward
    do_DELETE = _forward


def start_proxy() -> tuple[ThreadingHTTPServer, threading.Thread]:
    server = ThreadingHTTPServer((PROXY_HOST, PROXY_PORT), S3TraceProxyHandler)
    thread = threading.Thread(target=server.serve_forever, name="issue103-s3-trace-proxy", daemon=True)
    thread.start()
    return server, thread


def configure(connection: duckdb.DuckDBPyConnection, install: bool = False) -> None:
    if install:
        connection.execute("INSTALL httpfs")
    connection.execute("LOAD httpfs")
    connection.execute("SET threads=1")
    connection.execute(f"SET s3_region='{base.REGION}'")
    connection.execute(f"SET s3_access_key_id='{base.ACCESS_KEY}'")
    connection.execute(f"SET s3_secret_access_key='{base.SECRET_KEY}'")
    connection.execute(f"SET s3_endpoint='{PROXY_HOST}:{PROXY_PORT}'")
    connection.execute("SET s3_url_style='path'")
    connection.execute("SET s3_use_ssl=false")
    connection.execute("SET s3_uploader_max_filesize='100MB'")
    connection.execute("SET s3_uploader_max_parts_per_file=20")
    connection.execute("SET s3_uploader_thread_limit=2")


def copy_sql(key: str, fmt: str) -> str:
    options = "FORMAT CSV, HEADER true" if fmt == "csv" else "FORMAT PARQUET, COMPRESSION ZSTD"
    return (
        "COPY (SELECT i::BIGINT AS i, "
        "md5(i::VARCHAR) || md5((i + 1)::VARCHAR) AS payload "
        f"FROM range({ROWS}) t(i)) TO 's3://{base.BUCKET}/{key}' ({options})"
    )


def request_events_for_key(key: str) -> list[dict[str, Any]]:
    marker = f"/{base.BUCKET}/{key}"
    with TRACE_LOCK:
        return [event for event in TRACE if marker in str(event.get("path", ""))]


def run_case(client, fmt: str) -> dict[str, Any]:
    key = f"trace/{fmt}/result.{fmt}"
    connection = duckdb.connect()
    configure(connection)
    outcome: dict[str, Any] = {}

    def execute() -> None:
        try:
            connection.execute(copy_sql(key, fmt))
            outcome["completed"] = True
        except Exception as exc:
            outcome.update(
                completed=False,
                error_type=type(exc).__name__,
                error=str(exc),
            )

    thread = threading.Thread(target=execute, name=f"issue103-copy-{fmt}")
    trace_event("case_start", format=fmt, key=key)
    thread.start()

    first_upload: float | None = None
    first_part: float | None = None
    observed_part_count = 0
    observed_part_bytes = 0
    started = time.monotonic()
    while time.monotonic() - started < WAIT_SECONDS and thread.is_alive():
        state = base.snapshot(client, key)
        if state["uploads"] and first_upload is None:
            first_upload = time.monotonic() - started
        if state["part_count"]:
            observed_part_count = state["part_count"]
            observed_part_bytes = state["part_bytes"]
            first_part = time.monotonic() - started
            break
        time.sleep(POLL_SECONDS)

    trace_event(
        "interrupt_called",
        format=fmt,
        key=key,
        observed_part_count=observed_part_count,
        observed_part_bytes=observed_part_bytes,
    )
    connection.interrupt()
    thread.join(timeout=30)
    trace_event("query_thread_finished", format=fmt, key=key, outcome=outcome)
    time.sleep(0.5)

    after = base.snapshot(client, key)
    reader = base.read_remote(key, fmt) if after["head"].get("exists") else {"readable": False, "absent": True}
    try:
        reusable = connection.execute("SELECT 103").fetchone()[0] == 103
        reuse_error = None
    except Exception as exc:
        reusable = False
        reuse_error = f"{type(exc).__name__}: {exc}"
    connection.close()

    events = request_events_for_key(key)
    interrupt_seconds = next(
        event["seconds"]
        for event in TRACE
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
    deletes_after_interrupt = [
        event
        for event in events
        if event.get("kind") == "request_end"
        and event.get("request_class") in {"delete_object", "abort_multipart"}
        and event["seconds"] >= interrupt_seconds
    ]
    completion_after_interrupt = any(event["seconds"] >= interrupt_seconds for event in completions)

    result = {
        "format": fmt,
        "key": key,
        "threshold_reached": observed_part_count > 0,
        "first_upload_seconds": first_upload,
        "first_part_seconds": first_part,
        "observed_part_count": observed_part_count,
        "observed_part_bytes": observed_part_bytes,
        "outcome": outcome,
        "thread_finished": not thread.is_alive(),
        "connection_reusable": reusable,
        "connection_reuse_error": reuse_error,
        "after": after,
        "reader": reader,
        "request_events": events,
        "completion_after_interrupt": completion_after_interrupt,
        "heads_after_interrupt": heads_after_interrupt,
        "deletes_after_interrupt": deletes_after_interrupt,
    }

    trace_event("cleanup_start", format=fmt, key=key)
    result["cleanup"] = {
        "aborted_uploads": base.abort_all_uploads(client),
        "deleted_objects": base.delete_all_objects(client),
    }
    result["cleanup"]["state_after"] = {
        "uploads": base.list_uploads(client),
        "objects": base.list_objects(client),
    }
    return result


def csv_prefix_exact(reader: dict[str, Any]) -> bool:
    count = reader.get("row_count")
    return (
        reader.get("readable") is True
        and isinstance(count, int)
        and 0 < count < ROWS
        and reader.get("minimum") == 0
        and reader.get("maximum") == count - 1
        and reader.get("checksum") == base.expected_checksum(count)
    )


def build_checks(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []

    def add(name: str, passed: bool, detail: Any) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    for case in cases:
        fmt = case["format"]
        add(f"{fmt}.part_observed", case["threshold_reached"], case)
        add(
            f"{fmt}.interrupt_exception",
            case["outcome"].get("completed") is False
            and case["outcome"].get("error_type") == "InterruptException",
            case["outcome"],
        )
        add(f"{fmt}.connection_reusable", case["connection_reusable"], case)
        add(f"{fmt}.final_object_exists", case["after"]["head"].get("exists") is True, case["after"])
        add(f"{fmt}.multipart_completed_after_interrupt", case["completion_after_interrupt"], case["request_events"])
        add(f"{fmt}.no_abort_request", not any(e.get("request_class") == "abort_multipart" for e in case["request_events"]), case["request_events"])
        if fmt == "csv":
            add("csv.reader_accepts_exact_prefix", csv_prefix_exact(case["reader"]), case["reader"])
        else:
            add(
                "parquet.reader_rejects_incomplete_file",
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
    client = base.s3_client()
    try:
        client.create_bucket(Bucket=base.BUCKET)
    except Exception:
        pass
    base.abort_all_uploads(client)
    base.delete_all_objects(client)

    preparation = duckdb.connect()
    configure(preparation, install=True)
    httpfs = base.extension_info(preparation)
    preparation.close()

    server, thread = start_proxy()
    try:
        cases = [run_case(client, fmt) for fmt in FORMATS]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    checks = build_checks(cases)
    result = {
        "metadata": {
            "duckdb_version": duckdb.__version__,
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "httpfs": httpfs,
            "rows_requested": ROWS,
            "proxy": f"http://{PROXY_HOST}:{PROXY_PORT}",
            "backend": f"http://{BACKEND_HOST}:{BACKEND_PORT}",
            "minio_release": os.environ.get("ISSUE96_MINIO_RELEASE"),
            "minio_sha256": os.environ.get("ISSUE96_MINIO_SHA256"),
        },
        "cases": cases,
        "trace": TRACE,
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
    parser.add_argument("--output", type=Path, default=Path("issue103-s3-interrupt-trace.json"))
    args = parser.parse_args()
    raise SystemExit(main(args.output))
