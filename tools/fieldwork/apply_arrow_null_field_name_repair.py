#!/usr/bin/env python3
"""Apply the focused Arrow C Data null optional field-name repair."""

from pathlib import Path

SOURCE = Path("src/function/table/arrow.cpp")

OLD = "\t\tauto name = string(schema.name);\n"
NEW = "\t\tauto name = schema.name ? string(schema.name) : string();\n"


def main() -> None:
    source = SOURCE.read_text()
    count = source.count(OLD)
    if count != 1:
        raise RuntimeError(f"expected one Arrow field-name anchor, found {count}")
    SOURCE.write_text(source.replace(OLD, NEW, 1))
    print("FIELDWORK_ARROW_NULL_NAME=normalize-null-before-fallback")


if __name__ == "__main__":
    main()
