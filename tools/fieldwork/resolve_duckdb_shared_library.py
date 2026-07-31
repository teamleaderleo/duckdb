#!/usr/bin/env python3
"""Resolve DuckDB's release shared-library symlink without broad filesystem search."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import shutil
import stat
import sys
from typing import Sequence


class LibraryResolutionError(ValueError):
    """Raised when the expected build output cannot be resolved safely."""


def _inside(path: pathlib.Path, root: pathlib.Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def resolve_library(source_dir: pathlib.Path) -> dict[str, object]:
    source = source_dir.resolve(strict=True)
    release_root = (source / "build/release").resolve(strict=True)
    link = release_root / "src/libduckdb.so"

    if not os.path.lexists(link):
        raise LibraryResolutionError(f"expected shared-library path is absent: {link}")

    is_symlink = link.is_symlink()
    link_target = os.readlink(link) if is_symlink else None
    try:
        resolved = link.resolve(strict=True)
    except FileNotFoundError as error:
        raise LibraryResolutionError(f"shared-library link is dangling: {link}") from error

    if not resolved.is_file():
        raise LibraryResolutionError(
            f"resolved shared library is not a regular file: {resolved}"
        )
    if not _inside(resolved, release_root):
        raise LibraryResolutionError(
            f"resolved shared library escaped release tree: {resolved}"
        )

    metadata = resolved.stat()
    return {
        "schema_version": 1,
        "source_dir": str(source),
        "release_root": str(release_root),
        "link_path": str(link),
        "link_is_symlink": is_symlink,
        "link_target": link_target,
        "resolved_path": str(resolved),
        "resolved_size": metadata.st_size,
        "resolved_mode": stat.filemode(metadata.st_mode),
        "resolved_sha256": hashlib.sha256(resolved.read_bytes()).hexdigest(),
    }


def copy_library(receipt: dict[str, object], destination: pathlib.Path) -> None:
    resolved = pathlib.Path(str(receipt["resolved_path"]))
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(resolved, destination)
    shutil.copymode(resolved, destination)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("source_dir", type=pathlib.Path)
    parser.add_argument("--copy-to", action="append", default=[], type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        receipt = resolve_library(args.source_dir)
        for destination in args.copy_to:
            copy_library(receipt, destination)
    except (OSError, LibraryResolutionError) as error:
        print(f"DuckDB shared-library resolution failed: {error}", file=sys.stderr)
        return 2

    rendered = json.dumps(receipt, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
