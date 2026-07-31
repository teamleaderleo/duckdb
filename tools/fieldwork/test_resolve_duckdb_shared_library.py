from __future__ import annotations

import os
import pathlib
import tempfile
import unittest

from tools.fieldwork.resolve_duckdb_shared_library import (
    LibraryResolutionError,
    copy_library,
    resolve_library,
)


class ResolveDuckdbSharedLibraryTest(unittest.TestCase):
    def make_tree(self, root: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
        source = root / "source"
        library_dir = source / "build/release/src"
        library_dir.mkdir(parents=True)
        target = library_dir / "libduckdb.so.1.4.0"
        target.write_bytes(b"duckdb-shared-library\n")
        os.chmod(target, 0o755)
        return source, target

    def test_versioned_target_and_symlink_are_resolved_and_copied(self) -> None:
        with tempfile.TemporaryDirectory(prefix="duckdb-library-resolution-") as temporary:
            root = pathlib.Path(temporary)
            source, target = self.make_tree(root)
            link = target.parent / "libduckdb.so"
            link.symlink_to(target.name)

            receipt = resolve_library(source)
            copied = root / "runtime/libduckdb.so"
            copy_library(receipt, copied)

            self.assertIs(receipt["link_is_symlink"], True)
            self.assertEqual(receipt["link_target"], target.name)
            self.assertEqual(pathlib.Path(str(receipt["resolved_path"])), target)
            self.assertEqual(copied.read_bytes(), target.read_bytes())
            self.assertFalse(copied.is_symlink())
            self.assertNotEqual(copied.stat().st_ino, target.stat().st_ino)

    def test_regular_unversioned_library_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory(prefix="duckdb-library-regular-") as temporary:
            root = pathlib.Path(temporary)
            source, target = self.make_tree(root)
            link = target.parent / "libduckdb.so"
            link.write_bytes(target.read_bytes())

            receipt = resolve_library(source)
            self.assertIs(receipt["link_is_symlink"], False)
            self.assertIsNone(receipt["link_target"])
            self.assertEqual(pathlib.Path(str(receipt["resolved_path"])), link)

    def test_missing_and_dangling_paths_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="duckdb-library-missing-") as temporary:
            root = pathlib.Path(temporary)
            source, target = self.make_tree(root)
            with self.assertRaisesRegex(LibraryResolutionError, "absent"):
                resolve_library(source)

            link = target.parent / "libduckdb.so"
            link.symlink_to("missing-version")
            with self.assertRaisesRegex(LibraryResolutionError, "dangling"):
                resolve_library(source)

    def test_directory_and_escape_targets_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="duckdb-library-unsafe-") as temporary:
            root = pathlib.Path(temporary)
            source, target = self.make_tree(root)
            link = target.parent / "libduckdb.so"
            link.symlink_to(".")
            with self.assertRaisesRegex(LibraryResolutionError, "not a regular file"):
                resolve_library(source)

            link.unlink()
            outside = root / "outside-libduckdb.so"
            outside.write_bytes(b"outside\n")
            link.symlink_to(outside)
            with self.assertRaisesRegex(LibraryResolutionError, "escaped"):
                resolve_library(source)


if __name__ == "__main__":
    unittest.main()
