#!/usr/bin/env python3
"""Deterministic DuckDB S3-compatible publication probe for Fieldwork issue 96."""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable

import boto3
import duckdb
from botocore.client import Config
from botocore.exceptions import ClientError

DUCKDB_VERSION = "1.5.5"
BUCKET = "fieldwork-issue96"
ENDPOINT_URL = os.environ.get("ISSUE96_S3_ENDPOINT", "http://127.0.0.1:9000")
DUCKDB_ENDPOINT = ENDPOINT_URL.removeprefix("http://").removeprefix("https://")
ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "fieldwork96")
SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "fieldwork96-secret")
REGION = "us-east-1"
FORMATS = ("csv", "parquet")
SUCCESS_ROWS = 400_000
FAIL_ROWS = 50_000_000
RETRY_ROWS = 2_000
POLL_SECONDS = 0.05
PART_WAIT_SECONDS = 45.0
PROCESS_WAIT_SECONDS = 90.0


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 1, "mode": "standard"},
        ),
    )


def configure_duckdb(connection: duckdb.DuckDBPyConnection, install: bool = False) -> None:
    if install:
        connection.execute("INSTALL httpfs")
    connection.execute("LOAD httpfs")
    connection.execute("SET threads=1")
    connection.execute(f"SET s3_region='{REGION}'")
    connection.execute(f"SET s3_access_key_id='{ACCESS_KEY}'")
    connection.execute(f"SET s3_secret_access_key='{SECRET_KEY}'")
    connection.execute(f"SET s3_endpoint='{DUCKDB_ENDPOINT}'")
    connection.execute("SET s3_url_style='path'")
    connection.execute("SET s3_use_ssl=false")
    # Force the minimum legal S3 part size (5 MiB) while keeping each test file
    # well below the declared maximum.
    connection.execute("SET s3_uploader_max_filesize='100MB'")
    connection.execute("SET s3_uploader_max_parts_per_file=20")
    connection.execute("SET s3_uploader_thread_limit=2")


def format_options(fmt: str, use_tmp_file: bool = False) -> str:
    if fmt == "csv":
        options = ["FORMAT CSV", "HEADER true"]
    elif fmt == "parquet":
        options = ["FORMAT PARQUET", "COMPRESSION ZSTD"]
    else:
        raise ValueError(fmt)
    if use_tmp_file:
        options.append("USE_TMP_FILE true")
    return ", ".join(options)


def copy_sql(key: str, fmt: str, rows: int, use_tmp_file: bool = False) -> str:
    payload = "md5(i::VARCHAR) || md5((i + 1)::VARCHAR)"
    return (
        "COPY (SELECT i::BIGINT AS i, "
        f"{payload} AS payload FROM range({rows}) t(i)) "
        f"TO 's3://{BUCKET}/{key}' ({format_options(fmt, use_tmp_file)})"
    )


def expected_checksum(rows: int) -> int:
    return rows * (rows - 1) // 2


def extension_info(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    cursor = connection.execute(
        "SELECT * FROM duckdb_extensions() WHERE extension_name='httpfs'"
    )
    names = [description[0] for description in cursor.description]
    row = cursor.fetchone()
    return dict(zip(names, row)) if row else {}


def object_head(client, key: str) -> dict[str, Any]:
    try:
        result = client.head_object(Bucket=BUCKET, Key=key)
        return {
            "exists": True,
            "content_length": result.get("ContentLength"),
            "etag": result.get("ETag"),
            "last_modified": result.get("LastModified").isoformat()
            if result.get("LastModified")
            else None,
        }
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
            return {"exists": False}
        return {"exists": False, "error": f"{type(exc).__name__}: {exc}"}


def list_objects(client, prefix: str = "") -> list[dict[str, Any]]:
    response = client.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    return [
        {
            "key": item["Key"],
            "size": item["Size"],
            "etag": item.get("ETag"),
            "last_modified": item["LastModified"].isoformat(),
        }
        for item in response.get("Contents", [])
    ]


def list_uploads(client, prefix: str = "") -> list[dict[str, Any]]:
    response = client.list_multipart_uploads(Bucket=BUCKET, Prefix=prefix)
    uploads: list[dict[str, Any]] = []
    for item in response.get("Uploads", []) or []:
        record: dict[str, Any] = {
            "key": item["Key"],
            "upload_id": item["UploadId"],
            "initiated": item.get("Initiated").isoformat() if item.get("Initiated") else None,
        }
        try:
            parts_response = client.list_parts(
                Bucket=BUCKET, Key=item["Key"], UploadId=item["UploadId"]
            )
            record["parts"] = [
                {
                    "part_number": part["PartNumber"],
                    "size": part["Size"],
                    "etag": part.get("ETag"),
                }
                for part in parts_response.get("Parts", [])
            ]
        except ClientError as exc:
            record["parts_error"] = f"{type(exc).__name__}: {exc}"
            record["parts"] = []
        uploads.append(record)
    return uploads


def snapshot(client, key: str, scan_all: bool = False) -> dict[str, Any]:
    prefix = "" if scan_all else key
    uploads = list_uploads(client, prefix)
    objects = list_objects(client, prefix)
    return {
        "head": object_head(client, key),
        "uploads": uploads,
        "objects": objects,
        "part_count": sum(len(upload.get("parts", [])) for upload in uploads),
        "part_bytes": sum(
            part.get("size", 0)
            for upload in uploads
            for part in upload.get("parts", [])
        ),
    }


def observe_until(
    client,
    key: str,
    alive: Callable[[], bool],
    *,
    stop_after_part: bool,
    timeout: float,
    scan_all: bool = False,
) -> dict[str, Any]:
    started = time.monotonic()
    first_upload: float | None = None
    first_part: float | None = None
    first_object: float | None = None
    object_while_upload_active = False
    max_part_count = 0
    max_part_bytes = 0
    seen_upload_keys: set[str] = set()
    seen_object_keys: set[str] = set()
    samples = 0
    last_state: dict[str, Any] = {}
    while time.monotonic() - started < timeout:
        samples += 1
        now = time.monotonic() - started
        last_state = snapshot(client, key, scan_all=scan_all)
        uploads = last_state["uploads"]
        objects = last_state["objects"]
        head = last_state["head"]
        seen_upload_keys.update(upload["key"] for upload in uploads)
        seen_object_keys.update(item["key"] for item in objects)
        if uploads and first_upload is None:
            first_upload = now
        if last_state["part_count"] and first_part is None:
            first_part = now
        if head.get("exists") and first_object is None:
            first_object = now
        if head.get("exists") and uploads:
            object_while_upload_active = True
        max_part_count = max(max_part_count, last_state["part_count"])
        max_part_bytes = max(max_part_bytes, last_state["part_bytes"])
        if stop_after_part and last_state["part_count"] > 0:
            break
        if not alive():
            break
        time.sleep(POLL_SECONDS)
    return {
        "elapsed_seconds": time.monotonic() - started,
        "samples": samples,
        "first_upload_seconds": first_upload,
        "first_part_seconds": first_part,
        "first_object_seconds": first_object,
        "object_while_upload_active": object_while_upload_active,
        "max_part_count": max_part_count,
        "max_part_bytes": max_part_bytes,
        "seen_upload_keys": sorted(seen_upload_keys),
        "seen_object_keys": sorted(seen_object_keys),
        "last_state": last_state,
    }


def read_remote(key: str, fmt: str) -> dict[str, Any]:
    connection = duckdb.connect()
    configure_duckdb(connection)
    relation = (
        f"read_csv_auto('s3://{BUCKET}/{key}', header=true)"
        if fmt == "csv"
        else f"read_parquet('s3://{BUCKET}/{key}')"
    )
    try:
        count, total, minimum, maximum = connection.execute(
            f"SELECT count(*), sum(i), min(i), max(i) FROM {relation}"
        ).fetchone()
        return {
            "readable": True,
            "row_count": count,
            "checksum": total,
            "minimum": minimum,
            "maximum": maximum,
        }
    except Exception as exc:
        return {
            "readable": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
    finally:
        connection.close()


def worker(key: str, fmt: str, rows: int, use_tmp_file: bool) -> int:
    connection = duckdb.connect()
    configure_duckdb(connection)
    try:
        connection.execute(copy_sql(key, fmt, rows, use_tmp_file))
        print(json.dumps({"ok": True, "key": key, "rows": rows}), flush=True)
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "key": key,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            ),
            flush=True,
        )
        return 2
    finally:
        connection.close()


def spawn_worker(key: str, fmt: str, rows: int, use_tmp_file: bool = False):
    return subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--worker",
            key,
            fmt,
            str(rows),
            "1" if use_tmp_file else "0",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def normal_case(client, fmt: str, use_tmp_file: bool = False) -> dict[str, Any]:
    label = "explicit_tmp" if use_tmp_file else "normal"
    key = f"{label}/{fmt}/result.{fmt}"
    process = spawn_worker(key, fmt, SUCCESS_ROWS, use_tmp_file)
    observation = observe_until(
        client,
        key,
        lambda: process.poll() is None,
        stop_after_part=False,
        timeout=PROCESS_WAIT_SECONDS,
        scan_all=use_tmp_file,
    )
    stdout, stderr = process.communicate(timeout=30)
    return {
        "case": label,
        "format": fmt,
        "key": key,
        "returncode": process.returncode,
        "stdout": stdout.strip(),
        "stderr": stderr.strip(),
        "observation": observation,
        "final_state": snapshot(client, key, scan_all=use_tmp_file),
        "reader": read_remote(key, fmt),
    }


def interrupt_case(client, fmt: str) -> dict[str, Any]:
    key = f"interrupt/{fmt}/result.{fmt}"
    connection = duckdb.connect()
    configure_duckdb(connection)
    outcome: dict[str, Any] = {}

    def execute() -> None:
        try:
            connection.execute(copy_sql(key, fmt, FAIL_ROWS))
            outcome["completed"] = True
        except Exception as exc:
            outcome.update(
                completed=False,
                error_type=type(exc).__name__,
                error=str(exc),
            )

    thread = threading.Thread(target=execute, name=f"issue96-interrupt-{fmt}")
    thread.start()
    observation = observe_until(
        client,
        key,
        thread.is_alive,
        stop_after_part=True,
        timeout=PART_WAIT_SECONDS,
    )
    connection.interrupt()
    interrupt_at = observation["elapsed_seconds"]
    thread.join(timeout=30)
    time.sleep(0.5)
    try:
        reusable = connection.execute("SELECT 7").fetchone()[0] == 7
        reuse_error = None
    except Exception as exc:
        reusable = False
        reuse_error = f"{type(exc).__name__}: {exc}"
    connection.close()
    return {
        "case": "interrupt",
        "format": fmt,
        "key": key,
        "interrupt_at_seconds": interrupt_at,
        "threshold_reached": observation["max_part_count"] > 0,
        "thread_finished": not thread.is_alive(),
        "outcome": outcome,
        "connection_reusable": reusable,
        "connection_reuse_error": reuse_error,
        "observation": observation,
        "after": snapshot(client, key),
    }


def crash_case(client, fmt: str) -> dict[str, Any]:
    key = f"crash/{fmt}/result.{fmt}"
    process = spawn_worker(key, fmt, FAIL_ROWS)
    observation = observe_until(
        client,
        key,
        lambda: process.poll() is None,
        stop_after_part=True,
        timeout=PART_WAIT_SECONDS,
    )
    killed = process.poll() is None
    if killed:
        process.kill()
    stdout, stderr = process.communicate(timeout=30)
    time.sleep(0.5)
    return {
        "case": "crash",
        "format": fmt,
        "key": key,
        "threshold_reached": observation["max_part_count"] > 0,
        "killed": killed,
        "returncode": process.returncode,
        "stdout": stdout.strip(),
        "stderr": stderr.strip(),
        "observation": observation,
        "after": snapshot(client, key),
    }


def retry_case(client, fmt: str, key: str, previous_upload_ids: list[str]) -> dict[str, Any]:
    connection = duckdb.connect()
    configure_duckdb(connection)
    try:
        connection.execute(copy_sql(key, fmt, RETRY_ROWS))
        succeeded = True
        error = None
    except Exception as exc:
        succeeded = False
        error = f"{type(exc).__name__}: {exc}"
    finally:
        connection.close()
    after = snapshot(client, key)
    remaining_ids = [upload["upload_id"] for upload in after["uploads"]]
    return {
        "case": "retry",
        "format": fmt,
        "key": key,
        "succeeded": succeeded,
        "error": error,
        "previous_upload_ids": previous_upload_ids,
        "remaining_upload_ids": remaining_ids,
        "previous_uploads_still_present": sorted(set(previous_upload_ids) & set(remaining_ids)),
        "after": after,
        "reader": read_remote(key, fmt),
    }


def upload_ids(state: dict[str, Any]) -> list[str]:
    return [upload["upload_id"] for upload in state.get("uploads", [])]


def abort_all_uploads(client) -> list[dict[str, str]]:
    aborted: list[dict[str, str]] = []
    for upload in list_uploads(client):
        client.abort_multipart_upload(
            Bucket=BUCKET, Key=upload["key"], UploadId=upload["upload_id"]
        )
        aborted.append({"key": upload["key"], "upload_id": upload["upload_id"]})
    return aborted


def delete_all_objects(client) -> list[str]:
    deleted: list[str] = []
    for item in list_objects(client):
        client.delete_object(Bucket=BUCKET, Key=item["key"])
        deleted.append(item["key"])
    return deleted


def build_checks(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []

    def add(name: str, passed: bool, detail: Any) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    for fmt in FORMATS:
        normal = next(c for c in cases if c["case"] == "normal" and c["format"] == fmt)
        reader = normal["reader"]
        add(f"{fmt}.normal.returncode", normal["returncode"] == 0, normal["returncode"])
        add(
            f"{fmt}.normal.multipart_observed",
            normal["observation"]["max_part_count"] > 0,
            normal["observation"],
        )
        add(
            f"{fmt}.normal.no_object_during_active_upload",
            not normal["observation"]["object_while_upload_active"],
            normal["observation"],
        )
        add(f"{fmt}.normal.readable", reader.get("readable") is True, reader)
        add(f"{fmt}.normal.count", reader.get("row_count") == SUCCESS_ROWS, reader)
        add(
            f"{fmt}.normal.checksum",
            reader.get("checksum") == expected_checksum(SUCCESS_ROWS),
            reader,
        )

        interrupted = next(c for c in cases if c["case"] == "interrupt" and c["format"] == fmt)
        add(f"{fmt}.interrupt.part_observed", interrupted["threshold_reached"], interrupted)
        add(
            f"{fmt}.interrupt.raised",
            interrupted["outcome"].get("completed") is False,
            interrupted["outcome"],
        )
        add(
            f"{fmt}.interrupt.final_absent",
            interrupted["after"]["head"].get("exists") is False,
            interrupted["after"],
        )
        add(
            f"{fmt}.interrupt.multipart_remains",
            len(interrupted["after"]["uploads"]) > 0,
            interrupted["after"],
        )
        add(f"{fmt}.interrupt.connection_reusable", interrupted["connection_reusable"], interrupted)

        interrupt_retry = next(
            c
            for c in cases
            if c["case"] == "retry" and c["format"] == fmt and c["key"].startswith("interrupt/")
        )
        add(f"{fmt}.interrupt_retry.succeeded", interrupt_retry["succeeded"], interrupt_retry)
        add(
            f"{fmt}.interrupt_retry.exact",
            interrupt_retry["reader"].get("row_count") == RETRY_ROWS
            and interrupt_retry["reader"].get("checksum") == expected_checksum(RETRY_ROWS),
            interrupt_retry["reader"],
        )
        add(
            f"{fmt}.interrupt_retry.old_upload_remains",
            len(interrupt_retry["previous_uploads_still_present"]) > 0,
            interrupt_retry,
        )

        crashed = next(c for c in cases if c["case"] == "crash" and c["format"] == fmt)
        add(f"{fmt}.crash.part_observed", crashed["threshold_reached"], crashed)
        add(f"{fmt}.crash.killed", crashed["killed"] and crashed["returncode"] != 0, crashed)
        add(
            f"{fmt}.crash.final_absent",
            crashed["after"]["head"].get("exists") is False,
            crashed["after"],
        )
        add(
            f"{fmt}.crash.multipart_remains",
            len(crashed["after"]["uploads"]) > 0,
            crashed["after"],
        )

        crash_retry = next(
            c
            for c in cases
            if c["case"] == "retry" and c["format"] == fmt and c["key"].startswith("crash/")
        )
        add(f"{fmt}.crash_retry.succeeded", crash_retry["succeeded"], crash_retry)
        add(
            f"{fmt}.crash_retry.exact",
            crash_retry["reader"].get("row_count") == RETRY_ROWS
            and crash_retry["reader"].get("checksum") == expected_checksum(RETRY_ROWS),
            crash_retry["reader"],
        )
        add(
            f"{fmt}.crash_retry.old_upload_remains",
            len(crash_retry["previous_uploads_still_present"]) > 0,
            crash_retry,
        )

        tmp_case = next(c for c in cases if c["case"] == "explicit_tmp" and c["format"] == fmt)
        tmp_reader = tmp_case["reader"]
        tmp_seen = [
            key
            for key in tmp_case["observation"]["seen_upload_keys"]
            + tmp_case["observation"]["seen_object_keys"]
            if "/tmp_" in key or key.startswith("tmp_")
        ]
        add(f"{fmt}.remote_tmp_option.accepted", tmp_case["returncode"] == 0, tmp_case)
        add(f"{fmt}.remote_tmp_option.no_tmp_key", not tmp_seen, tmp_seen)
        add(
            f"{fmt}.remote_tmp_option.multipart_direct",
            tmp_case["observation"]["max_part_count"] > 0,
            tmp_case["observation"],
        )
        add(
            f"{fmt}.remote_tmp_option.exact",
            tmp_reader.get("row_count") == SUCCESS_ROWS
            and tmp_reader.get("checksum") == expected_checksum(SUCCESS_ROWS),
            tmp_reader,
        )

    return checks


def main(output: Path) -> int:
    client = s3_client()
    try:
        client.create_bucket(Bucket=BUCKET)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            raise

    abort_all_uploads(client)
    delete_all_objects(client)

    preparation = duckdb.connect()
    configure_duckdb(preparation, install=True)
    httpfs = extension_info(preparation)
    preparation.close()

    cases: list[dict[str, Any]] = []
    for fmt in FORMATS:
        cases.append(normal_case(client, fmt))

        interrupted = interrupt_case(client, fmt)
        cases.append(interrupted)
        cases.append(retry_case(client, fmt, interrupted["key"], upload_ids(interrupted["after"])))

        crashed = crash_case(client, fmt)
        cases.append(crashed)
        cases.append(retry_case(client, fmt, crashed["key"], upload_ids(crashed["after"])))

        cases.append(normal_case(client, fmt, use_tmp_file=True))

    checks = build_checks(cases)
    state_before_cleanup = snapshot(client, "", scan_all=True)
    cleanup = {
        "aborted_uploads": abort_all_uploads(client),
        "deleted_objects": delete_all_objects(client),
        "state_after": snapshot(client, "", scan_all=True),
    }
    result = {
        "metadata": {
            "duckdb_version": duckdb.__version__,
            "expected_duckdb_version": DUCKDB_VERSION,
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "endpoint": ENDPOINT_URL,
            "bucket": BUCKET,
            "httpfs": httpfs,
            "minio_release": os.environ.get("ISSUE96_MINIO_RELEASE"),
            "minio_sha256": os.environ.get("ISSUE96_MINIO_SHA256"),
            "rows": {
                "success": SUCCESS_ROWS,
                "failure": FAIL_ROWS,
                "retry": RETRY_ROWS,
            },
            "s3_uploader": {
                "max_filesize": "100MB",
                "max_parts_per_file": 20,
                "thread_limit": 2,
            },
        },
        "cases": cases,
        "checks": checks,
        "summary": {
            "passed": sum(1 for check in checks if check["passed"]),
            "failed": sum(1 for check in checks if not check["passed"]),
        },
        "state_before_cleanup": state_before_cleanup,
        "cleanup": cleanup,
    }
    output.write_text(json.dumps(result, indent=2, sort_keys=True, default=str) + "\n")
    print(json.dumps({"output": str(output), **result["summary"]}), flush=True)
    return 0 if result["summary"]["failed"] == 0 else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("issue96-results.json"))
    parser.add_argument("--worker", nargs=4, metavar=("KEY", "FORMAT", "ROWS", "USE_TMP"))
    args = parser.parse_args()
    if args.worker:
        worker_key, worker_format, worker_rows, worker_tmp = args.worker
        raise SystemExit(worker(worker_key, worker_format, int(worker_rows), worker_tmp == "1"))
    raise SystemExit(main(args.output))
