#!/usr/bin/env python3
"""Apply the focused C API Arrow root child-count validation repair."""

from pathlib import Path

SOURCE = Path("src/main/capi/arrow-c.cpp")

INCLUDE_OLD = '''#include "duckdb/main/capi/capi_internal.hpp"\n'''
INCLUDE_NEW = '''#include "duckdb/main/capi/capi_internal.hpp"\n#include "fmt/format.h"\n'''

OLD = """\tauto arrow_table = reinterpret_cast<duckdb::ArrowTableSchema *>(converted_schema);
\tauto conn = reinterpret_cast<Connection *>(connection);
\tauto &types = arrow_table->GetTypes();

\tauto dchunk = duckdb::make_uniq<duckdb::DataChunk>();
"""

NEW = """\tauto arrow_table = reinterpret_cast<duckdb::ArrowTableSchema *>(converted_schema);
\tauto conn = reinterpret_cast<Connection *>(connection);
\tauto &types = arrow_table->GetTypes();
\t*out_chunk = nullptr;

\tconst auto expected_children = duckdb::NumericCast<int64_t>(types.size());
\tif (arrow_array->n_children != expected_children) {
\t\tauto message = duckdb_fmt::format("Arrow array child count mismatch: expected {}, got {}", expected_children,
\t\t                                  arrow_array->n_children);
\t\treturn duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, message.c_str());
\t}

\tauto dchunk = duckdb::make_uniq<duckdb::DataChunk>();
"""


def main() -> None:
    source = SOURCE.read_text()

    include_count = source.count(INCLUDE_OLD)
    if include_count != 1:
        raise RuntimeError(f"expected one C API include anchor, found {include_count}")
    source = source.replace(INCLUDE_OLD, INCLUDE_NEW, 1)

    count = source.count(OLD)
    if count != 1:
        raise RuntimeError(f"expected one child-count validation anchor, found {count}")
    source = source.replace(OLD, NEW, 1)

    SOURCE.write_text(source)
    print("FIELDWORK_CAPI_ARROW_CHILD_COUNT=validate-before-transfer")
    print("FIELDWORK_CAPI_ARROW_CHILD_COUNT_FMT=explicit-include")


if __name__ == "__main__":
    main()
