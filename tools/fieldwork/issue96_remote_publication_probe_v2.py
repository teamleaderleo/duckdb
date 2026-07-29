#!/usr/bin/env python3
"""Corrected entrypoint for the Fieldwork issue 96 remote publication probe.

V1 retained the full matrix but attempted HEAD with an empty object key while
capturing the bucket-wide pre-cleanup state. V2 replaces only that helper and
keeps the original workload and acceptance checks unchanged.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import issue96_remote_publication_probe as probe


_original_snapshot = probe.snapshot


def corrected_snapshot(client, key: str, scan_all: bool = False) -> dict[str, Any]:
    if key:
        return _original_snapshot(client, key, scan_all=scan_all)
    uploads = probe.list_uploads(client, "")
    objects = probe.list_objects(client, "")
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


probe.snapshot = corrected_snapshot


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("issue96-results-v2.json"))
    args = parser.parse_args()
    raise SystemExit(probe.main(args.output))
