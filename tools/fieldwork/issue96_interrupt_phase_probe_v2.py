#!/usr/bin/env python3
"""Corrected focused remote-interrupt phase entrypoint for Fieldwork issue 96.

The first focused probe completed its first case and then reused the V1
bucket-wide snapshot helper during cleanup. V2 patches only that empty-key
helper before loading the unchanged phase workload and acceptance checks.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import issue96_remote_publication_probe as base


_original_snapshot = base.snapshot


def corrected_snapshot(client, key: str, scan_all: bool = False) -> dict[str, Any]:
    if key:
        return _original_snapshot(client, key, scan_all=scan_all)
    uploads = base.list_uploads(client, "")
    objects = base.list_objects(client, "")
    return {
        "head": {"exists": False, "scope": "bucket-wide snapshot"},
        "uploads": uploads,
        "objects": objects,
        "part_count": sum(len(upload.get("parts", [])) for upload in uploads),
        "part_bytes": sum(
            part.get("size", 0)
            for upload in uploads
            for part in upload.get("parts", [])
        ),
    }


base.snapshot = corrected_snapshot

import issue96_interrupt_phase_probe as phase  # noqa: E402


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("issue96-interrupt-phase-v2.json"))
    args = parser.parse_args()
    raise SystemExit(phase.main(args.output))
