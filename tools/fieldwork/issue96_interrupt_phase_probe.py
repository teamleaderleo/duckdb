#!/usr/bin/env python3
"""Focused remote-interrupt phase probe for Fieldwork issue 96.

The broad matrix showed that an interrupt raised to the caller while a completed
remote object was left behind. This probe compares the default buffering path
with an early-flush COPY option and reads any published object before cleanup.
"""

from __future__ import annotations

import argparse
import json
import platform
import threading
import time
from pathlib import Path
from typing import Any

import duckdb

import issue96_remote_publication_probe as base

ROWS = 50_000_000
REPEATS = 2
EARLY_BATCH_SIZE = "6MB"


def phase_copy_sql(key: str, fmt: str, early_flush: bool) -> str:
    payload = "md5(i::VARCHAR) || md5((i + 1)::VARCHAR)"
    options = ["FORMAT CSV", "HEADER true"] if fmt == "csv" else ["FORMAT PARQUET", "COMPRESSION ZSTD"]
    if early_flush:
        options.append(f"BATCH_SIZE_BYTES '{EARLY_BATCH_SIZE}'")
    return (
        "COPY (SELECT i::BIGINT AS i, "
        f"{payload} AS payload FROM range({ROWS}) t(i)) "
        f"TO 's3://{base.BUCKET}/{key}' ({', '.join(options)})"
    )


def query_progress(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    try:
        value = connection.query_progress()
        return {"available": True, "value": value}
    except Exception as exc:
        return {"available": False, "error": f"{type(exc).__name__}: {exc}"}


def classify(after: dict[str, Any]) -> str:
    if after["head"].get("exists"):
        return "completed_object"
    if after["uploads"]:
        return "incomplete_multipart"
    return "clean_absence"


def run_case(client, fmt: str, mode: str, repeat: int) -> dict[str, Any]:
    early_flush = mode == "early_flush"
    key = f"phase/{mode}/{fmt}/repeat-{repeat}.{fmt}"
    connection = duckdb.connect()
    base.configure_duckdb(connection)
    outcome: dict[str, Any] = {}

    def execute() -> None:
        try:
            connection.execute(phase_copy_sql(key, fmt, early_flush))
            outcome["completed"] = True
        except Exception as exc:
            outcome.update(
                completed=False,
                error_type=type(exc).__name__,
                error=str(exc),
            )

    thread = threading.Thread(target=execute, name=f"issue96-{mode}-{fmt}-{repeat}")
    thread.start()
    observation = base.observe_until(
        client,
        key,
        thread.is_alive,
        stop_after_part=True,
        timeout=base.PART_WAIT_SECONDS,
    )
    progress_at_trigger = query_progress(connection)
    connection.interrupt()
    interrupt_at = observation["elapsed_seconds"]
    thread.join(timeout=30)
    time.sleep(0.5)
    after = base.snapshot(client, key)
    reader = base.read_remote(key, fmt) if after["head"].get("exists") else {"readable": False, "absent": True}
    try:
        reusable = connection.execute("SELECT 96").fetchone()[0] == 96
        reuse_error = None
    except Exception as exc:
        reusable = False
        reuse_error = f"{type(exc).__name__}: {exc}"
    connection.close()

    result = {
        "case": "interrupt_phase",
        "format": fmt,
        "mode": mode,
        "repeat": repeat,
        "key": key,
        "batch_size_bytes": EARLY_BATCH_SIZE if early_flush else None,
        "threshold_reached": observation["max_part_count"] > 0,
        "interrupt_at_seconds": interrupt_at,
        "progress_at_trigger": progress_at_trigger,
        "thread_finished": not thread.is_alive(),
        "outcome": outcome,
        "connection_reusable": reusable,
        "connection_reuse_error": reuse_error,
        "observation": observation,
        "after": after,
        "classification": classify(after),
        "reader": reader,
    }

    result["cleanup"] = {
        "aborted": base.abort_all_uploads(client),
        "deleted": base.delete_all_objects(client),
        "state_after": base.snapshot(client, "", scan_all=True),
    }
    return result


def prefix_is_exact(reader: dict[str, Any]) -> bool:
    if reader.get("readable") is not True:
        return False
    count = reader.get("row_count")
    if not isinstance(count, int) or not (0 < count < ROWS):
        return False
    return (
        reader.get("minimum") == 0
        and reader.get("maximum") == count - 1
        and reader.get("checksum") == base.expected_checksum(count)
    )


def checks(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []

    def add(name: str, passed: bool, detail: Any) -> None:
        result.append({"name": name, "passed": bool(passed), "detail": detail})

    for case in cases:
        stem = f"{case['format']}.{case['mode']}.repeat{case['repeat']}"
        add(f"{stem}.part_observed", case["threshold_reached"], case["observation"])
        add(
            f"{stem}.interrupt_raised",
            case["outcome"].get("completed") is False
            and case["outcome"].get("error_type") == "InterruptException",
            case["outcome"],
        )
        add(f"{stem}.thread_finished", case["thread_finished"], case)
        add(f"{stem}.connection_reusable", case["connection_reusable"], case)
        add(
            f"{stem}.residue_is_bounded",
            case["classification"] in {"completed_object", "incomplete_multipart", "clean_absence"},
            case["classification"],
        )
        if case["classification"] == "completed_object":
            add(f"{stem}.published_object_is_exact_prefix", prefix_is_exact(case["reader"]), case["reader"])
        else:
            add(
                f"{stem}.no_published_object",
                case["after"]["head"].get("exists") is False,
                case["after"],
            )
        add(
            f"{stem}.cleanup_empty",
            not case["cleanup"]["state_after"]["uploads"]
            and not case["cleanup"]["state_after"]["objects"],
            case["cleanup"],
        )

    for fmt in base.FORMATS:
        for mode in ("default", "early_flush"):
            matching = [case for case in cases if case["format"] == fmt and case["mode"] == mode]
            classes = [case["classification"] for case in matching]
            add(f"{fmt}.{mode}.classification_repeatable", len(set(classes)) == 1, classes)

    return result


def main(output: Path) -> int:
    client = base.s3_client()
    try:
        client.create_bucket(Bucket=base.BUCKET)
    except Exception:
        pass
    base.abort_all_uploads(client)
    base.delete_all_objects(client)

    preparation = duckdb.connect()
    base.configure_duckdb(preparation, install=True)
    httpfs = base.extension_info(preparation)
    preparation.close()

    cases: list[dict[str, Any]] = []
    for fmt in base.FORMATS:
        for mode in ("default", "early_flush"):
            for repeat in range(1, REPEATS + 1):
                cases.append(run_case(client, fmt, mode, repeat))

    all_checks = checks(cases)
    result = {
        "metadata": {
            "duckdb_version": duckdb.__version__,
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "httpfs": httpfs,
            "rows_requested": ROWS,
            "repeats": REPEATS,
            "early_batch_size": EARLY_BATCH_SIZE,
            "minio_release": __import__("os").environ.get("ISSUE96_MINIO_RELEASE"),
            "minio_sha256": __import__("os").environ.get("ISSUE96_MINIO_SHA256"),
        },
        "cases": cases,
        "checks": all_checks,
        "summary": {
            "passed": sum(1 for check in all_checks if check["passed"]),
            "failed": sum(1 for check in all_checks if not check["passed"]),
        },
    }
    output.write_text(json.dumps(result, indent=2, sort_keys=True, default=str) + "\n")
    print(json.dumps({"output": str(output), **result["summary"]}), flush=True)
    return 0 if result["summary"]["failed"] == 0 else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("issue96-interrupt-phase.json"))
    args = parser.parse_args()
    raise SystemExit(main(args.output))
