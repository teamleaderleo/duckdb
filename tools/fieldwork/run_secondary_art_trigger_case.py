#!/usr/bin/env python3
"""Run one named trigger, then inspect only if it already settled the WAL."""

from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
import sys
from typing import Any, Sequence


ROOT = pathlib.Path(__file__).resolve().parents[2]
TRIGGER_PROBE = ROOT / "tools/fieldwork/secondary_art_persistence_trigger_probe.py"
SOURCE_PROBE = ROOT / "tools/fieldwork/secondary_art_source_identity_probe.py"


class TriggerCaseError(RuntimeError):
    pass


def parse_single_json(stdout: str, label: str) -> dict[str, Any]:
    lines = [line for line in stdout.splitlines() if line.strip()]
    if len(lines) != 1:
        raise TriggerCaseError(
            f"{label} must emit exactly one nonempty JSON line, observed {len(lines)}"
        )
    try:
        payload = json.loads(lines[0])
    except json.JSONDecodeError as error:
        raise TriggerCaseError(f"{label} emitted invalid JSON: {error}") from error
    if type(payload) is not dict:
        raise TriggerCaseError(f"{label} JSON must be an object")
    return payload


def run_command(command: list[str], label: str) -> tuple[dict[str, Any], str, str]:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=120,
    )
    if completed.returncode != 0:
        raise TriggerCaseError(
            f"{label} failed with status {completed.returncode}: {completed.stderr}"
        )
    return parse_single_json(completed.stdout, label), completed.stdout, completed.stderr


def require(condition: bool, message: str) -> None:
    if not condition:
        raise TriggerCaseError(message)


def run_case(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = pathlib.Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    database = pathlib.Path(args.database).resolve()

    trigger, trigger_stdout, trigger_stderr = run_command(
        [
            sys.executable,
            str(TRIGGER_PROBE),
            args.writer_library,
            args.trigger_library,
            str(database),
            str(args.row_count),
            args.source_sha,
            args.trigger,
        ],
        "trigger probe",
    )
    (output_dir / "trigger.stdout").write_text(trigger_stdout, encoding="utf-8")
    (output_dir / "trigger.stderr").write_text(trigger_stderr, encoding="utf-8")
    (output_dir / "trigger.json").write_text(
        json.dumps(trigger, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    require(trigger.get("schema_version") == 1, "invalid trigger schema")
    require(trigger.get("phase") == "trigger", "invalid trigger phase")
    require(trigger.get("trigger") == args.trigger, "trigger identity mismatch")
    require(trigger.get("source_sha") == args.source_sha, "trigger source mismatch")
    require(trigger.get("row_count") == args.row_count, "trigger row-count mismatch")
    require(
        trigger.get("wal_before_trigger", {}).get("present") is True,
        "pre-trigger WAL was not retained",
    )

    wal_after = trigger.get("wal_after_trigger")
    require(type(wal_after) is dict, "post-trigger WAL state is missing")
    settled = wal_after.get("present") is False
    inspection: dict[str, Any] | None = None
    wrong_result: bool | None = None

    if settled:
        inspection, inspection_stdout, inspection_stderr = run_command(
            [
                sys.executable,
                str(SOURCE_PROBE),
                "inspect",
                args.trigger_library,
                str(database),
                str(args.row_count),
                args.source_sha,
            ],
            "read-only inspection",
        )
        (output_dir / "inspection.stdout").write_text(
            inspection_stdout, encoding="utf-8"
        )
        (output_dir / "inspection.stderr").write_text(
            inspection_stderr, encoding="utf-8"
        )
        (output_dir / "inspection.json").write_text(
            json.dumps(inspection, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        require(inspection.get("schema_version") == 1, "invalid inspection schema")
        require(inspection.get("phase") == "inspect", "invalid inspection phase")
        require(
            inspection.get("source_sha") == args.source_sha,
            "inspection source mismatch",
        )
        require(
            inspection.get("row_count") == args.row_count,
            "inspection row-count mismatch",
        )
        require(
            inspection.get("full_count") == args.row_count,
            "full table row count changed",
        )
        require(
            inspection.get("first_values")
            == list(range(1, min(args.row_count, 10) + 1)),
            "ordered table values changed",
        )
        require(
            inspection.get("disabled_filtered_count") == 1
            and inspection.get("disabled_terminal_count") == 1,
            "sequential-scan controls failed",
        )
        wrong_result = bool(
            inspection.get("enabled_filtered_count") != 1
            or inspection.get("enabled_terminal_count") != 1
            or inspection.get("full_count") != args.row_count
        )
    else:
        (output_dir / "inspection-skipped.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "phase": "inspection-skipped",
                    "reason": "named trigger left the WAL pending; a read-write inspector would be an additional trigger",
                    "wal_after_trigger": wal_after,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    writer = trigger.get("writer_library")
    trigger_library = trigger.get("trigger_library")
    require(type(writer) is dict and type(trigger_library) is dict, "library identity missing")
    require(writer.get("sha256") == trigger_library.get("sha256"), "library bytes differ")
    require(
        (writer.get("device"), writer.get("inode"))
        != (trigger_library.get("device"), trigger_library.get("inode")),
        "library inodes are not distinct",
    )
    require(
        writer.get("open_address") != trigger_library.get("open_address"),
        "engine function addresses are not distinct",
    )

    return {
        "schema_version": 1,
        "source_sha": args.source_sha,
        "row_count": args.row_count,
        "trigger": args.trigger,
        "settled_without_writer_close": settled,
        "wrong_result": wrong_result,
        "trigger_record": trigger,
        "inspection_record": inspection,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("writer_library")
    parser.add_argument("trigger_library")
    parser.add_argument("database")
    parser.add_argument("row_count", type=int)
    parser.add_argument("source_sha")
    parser.add_argument(
        "trigger",
        choices=("open-close", "select-one", "indexed-read", "explicit-checkpoint"),
    )
    parser.add_argument("output_dir")
    parser.add_argument("--output", type=pathlib.Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.row_count < 2:
        raise SystemExit("row count must be at least 2")
    try:
        receipt = run_case(args)
    except (OSError, subprocess.SubprocessError, TriggerCaseError) as error:
        print(f"secondary ART trigger case failed: {error}", file=sys.stderr)
        return 2
    rendered = json.dumps(receipt, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
