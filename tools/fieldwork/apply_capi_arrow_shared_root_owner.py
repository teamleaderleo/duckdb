#!/usr/bin/env python3
"""Apply the focused C API Arrow shared-root ownership repair.

This private Fieldwork carrier modifies exactly src/main/capi/arrow-c.cpp.
"""

from pathlib import Path

SOURCE = Path("src/main/capi/arrow-c.cpp")

OLD = """\tauto &arrow_types = arrow_table->GetColumns();
\tdchunk->SetChildCardinality(duckdb::NumericCast<idx_t>(arrow_array->length));
\tfor (idx_t i = 0; i < dchunk->ColumnCount(); i++) {
\t\tauto &parent_array = *arrow_array;
\t\tauto &array = parent_array.children[i];
\t\tauto arrow_type = arrow_types.at(i);
\t\tauto array_physical_type = arrow_type->GetPhysicalType();
\t\tauto array_state = duckdb::make_uniq<duckdb::ArrowArrayScanState>(*conn->context);
\t\t// We need to make sure that our chunk will hold the ownership
\t\tarray_state->owned_data = duckdb::make_shared_ptr<duckdb::ArrowArrayWrapper>();
\t\tarray_state->owned_data->arrow_array = *arrow_array;
\t\t// We set it to nullptr to effectively transfer the ownership
\t\tarrow_array->release = nullptr;
\t\ttry {
"""

NEW = """\tauto &arrow_types = arrow_table->GetColumns();
\tdchunk->SetChildCardinality(duckdb::NumericCast<idx_t>(arrow_array->length));

\t// Transfer the Arrow root exactly once. Every output vector that aliases any
\t// child buffer must retain this same owner, independent of column order.
\tauto root_owner = duckdb::make_shared_ptr<duckdb::ArrowArrayWrapper>();
\troot_owner->arrow_array = *arrow_array;
\tarrow_array->release = nullptr;

\tfor (idx_t i = 0; i < dchunk->ColumnCount(); i++) {
\t\tauto &parent_array = root_owner->arrow_array;
\t\tauto &array = parent_array.children[i];
\t\tauto arrow_type = arrow_types.at(i);
\t\tauto array_physical_type = arrow_type->GetPhysicalType();
\t\tauto array_state = duckdb::make_uniq<duckdb::ArrowArrayScanState>(*conn->context);
\t\tarray_state->owned_data = root_owner;
\t\ttry {
"""


def main() -> None:
    source = SOURCE.read_text()
    count = source.count(OLD)
    if count != 1:
        raise RuntimeError(f"expected one shared-root replacement anchor, found {count}")
    SOURCE.write_text(source.replace(OLD, NEW, 1))
    print("FIELDWORK_CAPI_ARROW_SHARED_ROOT=one-owner-per-root")


if __name__ == "__main__":
    main()
