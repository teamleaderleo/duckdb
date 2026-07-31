#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    target = Path(path)
    text = target.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one generated-source anchor, found {count}")
    target.write_text(text.replace(old, new, 1))
    print(f"hardened {path}")


replace_once(
    "src/function/table/arrow/arrow_type_info.cpp",
    '''\t\tif (type_id < 0) {
\t\t\tthrow InvalidInputException("Arrow union type ID out of range: %d", static_cast<int>(type_id));
\t\t}
\t\ttype_id_to_child_idx[NumericCast<idx_t>(type_id)] = child_idx;
''',
    '''\t\tif (type_id < 0) {
\t\t\tthrow InvalidInputException("Arrow union type ID out of range: %d", static_cast<int>(type_id));
\t\t}
\t\tauto &mapped_child_idx = type_id_to_child_idx[NumericCast<idx_t>(type_id)];
\t\tif (mapped_child_idx != DConstants::INVALID_INDEX) {
\t\t\tthrow InvalidInputException("Arrow union type ID %d is duplicated", static_cast<int>(type_id));
\t\t}
\t\tmapped_child_idx = child_idx;
''',
)

print("FIELDWORK_262_HARDENING=duplicate-type-id-rejection")
