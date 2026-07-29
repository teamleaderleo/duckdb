#!/usr/bin/env python3
"""Corrected temporary-file publication matrix for Fieldwork issue 55.

The first temporary-file probe treated the final path appearing before a
post-execute marker as a failure. Source review and the raw result show that
DuckDB closes the writer and moves the temporary file during statement
finalization, immediately before the Python call returns. V2 checks the file
at first appearance instead of equating API return with publication.
"""

from __future__ import annotations

import argparse
import json
import platform
import tempfile
import time
from pathlib import Path
from typing import Any

import duckdb

import issue55_tmp_publication_probe as base


def fresh_visibility(root: Path, fmt: str) -> dict[str, Any]:
    final = root / f"fresh-visibility.{fmt}"
    temporary = base.tmp_path(final)
    marker = root / f"fresh-visibility-{fmt}.complete"
    process = base.spawn(final, fmt, base.SUCCESS_ROWS, "true", marker)
    started = time.monotonic()
    first_tmp_visible: float | None = None
    first_tmp_nonzero: float | None = None
    first_final_visible: float | None = None
    first_final_inspection: dict[str, Any] | None = None
    marker_at_first_final: bool | None = None
    tmp_at_first_final: dict[str, Any] | None = None
    max_tmp_size = 0
    samples = 0

    while process.poll() is None and time.monotonic() - started < base.TIMEOUT_SECONDS:
        samples += 1
        elapsed = time.monotonic() - started
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
        if final.exists() and first_final_visible is None:
            first_final_visible = elapsed
            marker_at_first_final = marker.exists()
            first_final_inspection = base.inspect(final, fmt)
            tmp_at_first_final = base.inspect(temporary, fmt)
        time.sleep(base.POLL_SECONDS)

    stdout, stderr = process.communicate(timeout=20.0)
    if first_final_inspection is None and final.exists():
        first_final_visible = time.monotonic() - started
        marker_at_first_final = marker.exists()
        first_final_inspection = base.inspect(final, fmt)
        tmp_at_first_final = base.inspect(temporary, fmt)

    return {
        "case": "fresh_tmp_visibility",
        "format": fmt,
        "returncode": process.returncode,
        "stdout": stdout.strip(),
        "stderr": stderr.strip(),
        "marker_exists_after": marker.exists(),
        "first_tmp_visible_seconds": first_tmp_visible,
        "first_tmp_nonzero_seconds": first_tmp_nonzero,
        "first_final_visible_seconds": first_final_visible,
        "marker_existed_at_first_final": marker_at_first_final,
        "max_tmp_size_bytes": max_tmp_size,
        "samples": samples,
        "first_final_inspection": first_final_inspection,
        "tmp_at_first_final": tmp_at_first_final,
        "final_after": base.inspect(final, fmt),
        "tmp_after": base.inspect(temporary, fmt),
    }


def exact(snapshot: dict[str, Any] | None, rows: int) -> bool:
    if snapshot is None:
        return False
    return (
        snapshot.get("readable") is True
        and snapshot.get("row_count") == rows
        and snapshot.get("checksum") == base.expected_sum(rows)
        and snapshot.get("minimum") == 0
        and snapshot.get("maximum") == rows - 1
    )


def evaluate(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    indexed = {(case["case"], case["format"]): case for case in cases}

    def add(name: str, passed: bool, detail: Any) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    for fmt in base.FORMATS:
        case = indexed[("fresh_tmp_visibility", fmt)]
        add(f"{fmt}.fresh.process", case["returncode"] == 0, case)
        add(f"{fmt}.fresh.tmp_visible", case["first_tmp_visible_seconds"] is not None, case)
        add(f"{fmt}.fresh.tmp_nonzero", case["first_tmp_nonzero_seconds"] is not None, case)
        add(f"{fmt}.fresh.final_visible", case["first_final_visible_seconds"] is not None, case)
        add(f"{fmt}.fresh.final_exact_at_first_sighting", exact(case["first_final_inspection"], base.SUCCESS_ROWS), case)
        add(f"{fmt}.fresh.tmp_absent_at_first_final", case["tmp_at_first_final"].get("exists") is False, case)
        add(f"{fmt}.fresh.final_exact_after", exact(case["final_after"], base.SUCCESS_ROWS), case["final_after"])
        add(f"{fmt}.fresh.tmp_absent_after", case["tmp_after"].get("exists") is False, case["tmp_after"])

        case = indexed[("fresh_explicit_crash", fmt)]
        add(f"{fmt}.fresh_crash.threshold", case["observation"]["threshold_at_seconds"] is not None, case)
        add(f"{fmt}.fresh_crash.killed", case["killed"] and case["returncode"] == -9, case)
        add(f"{fmt}.fresh_crash.final_absent", case["final_after_kill"].get("exists") is False, case["final_after_kill"])
        add(f"{fmt}.fresh_crash.tmp_remains", case["tmp_after_kill"].get("exists") is True, case["tmp_after_kill"])
        if fmt == "csv":
            partial = case["tmp_after_kill"].get("readable") is True and case["tmp_after_kill"].get("row_count", 0) > 0
            add("csv.fresh_crash.tmp_partial_readable", partial, case["tmp_after_kill"])
        else:
            add("parquet.fresh_crash.tmp_incomplete_rejected", case["tmp_after_kill"].get("readable") is False, case["tmp_after_kill"])
        explicit_retry = case["explicit_retry_with_residue"]
        add(f"{fmt}.fresh_crash.explicit_retry", explicit_retry.get("succeeded") is True, explicit_retry)
        add(f"{fmt}.fresh_crash.explicit_retry_exact", exact(explicit_retry.get("final"), base.RETRY_ROWS), explicit_retry)
        add(f"{fmt}.fresh_crash.explicit_retry_tmp_clean", explicit_retry.get("tmp", {}).get("exists") is False, explicit_retry)

        case = indexed[("existing_default_interrupt", fmt)]
        add(f"{fmt}.existing_interrupt.threshold", case["threshold_reached"], case)
        add(f"{fmt}.existing_interrupt.raised", case["outcome"].get("completed") is False, case["outcome"])
        add(f"{fmt}.existing_interrupt.seed_exact", exact(case["seed"], base.SEED_ROWS), case["seed"])
        add(f"{fmt}.existing_interrupt.final_preserved", exact(case["final_after"], base.SEED_ROWS), case["final_after"])
        add(f"{fmt}.existing_interrupt.tmp_clean", case["tmp_after"].get("exists") is False, case["tmp_after"])
        add(f"{fmt}.existing_interrupt.connection_reusable", case["connection_reusable"], case["connection_reuse_error"])

        case = indexed[("existing_default_crash", fmt)]
        add(f"{fmt}.existing_crash.threshold", case["observation"]["threshold_at_seconds"] is not None, case)
        add(f"{fmt}.existing_crash.killed", case["killed"] and case["returncode"] == -9, case)
        add(f"{fmt}.existing_crash.final_preserved", exact(case["final_after_kill"], base.SEED_ROWS), case["final_after_kill"])
        add(f"{fmt}.existing_crash.tmp_remains", case["tmp_after_kill"].get("exists") is True, case["tmp_after_kill"])
        retry = case["default_retry_with_tmp_residue"]
        add(f"{fmt}.existing_crash.retry", retry.get("succeeded") is True, retry)
        add(f"{fmt}.existing_crash.retry_exact", exact(retry.get("final"), base.RETRY_ROWS), retry)
        add(f"{fmt}.existing_crash.retry_tmp_clean", retry.get("tmp", {}).get("exists") is False, retry)

    return checks


def run(output: Path) -> int:
    if duckdb.__version__ != base.VERSION:
        raise RuntimeError(f"expected DuckDB {base.VERSION}, got {duckdb.__version__}")
    with tempfile.TemporaryDirectory(prefix="fieldwork-duckdb-issue55-tmp-v2-") as directory:
        root = Path(directory)
        cases: list[dict[str, Any]] = []
        for fmt in base.FORMATS:
            cases.extend(
                [
                    fresh_visibility(root, fmt),
                    base.fresh_explicit_crash(root, fmt),
                    base.existing_default_interrupt(root, fmt),
                    base.existing_default_crash(root, fmt),
                ]
            )
    checks = evaluate(cases)
    document = {
        "probe": "fieldwork-issue55-tmp-publication-v2",
        "duckdb_version": duckdb.__version__,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "constants": {
            "seed_rows": base.SEED_ROWS,
            "retry_rows": base.RETRY_ROWS,
            "success_rows": base.SUCCESS_ROWS,
            "long_rows": base.LONG_ROWS,
            "threshold_bytes": base.THRESHOLD,
            "interrupt_threshold_bytes": base.INTERRUPT_THRESHOLD,
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
    parser.add_argument("--output", type=Path, default=Path("issue55-tmp-publication-v2.json"))
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(run(parse_args().output))
