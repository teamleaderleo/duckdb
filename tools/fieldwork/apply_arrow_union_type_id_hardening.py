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

replace_once(
    "src/function/table/arrow_conversion.cpp",
    '''\t\tauto &validity_mask = FlatVector::ValidityMutable(vector);
\t\tauto &union_info = arrow_type.GetTypeInfo<ArrowUnionInfo>();
\t\tduckdb::vector<Vector> children;
''',
    '''\t\tauto &validity_mask = FlatVector::ValidityMutable(vector);
\t\tauto &union_info = arrow_type.GetTypeInfo<ArrowUnionInfo>();
\t\tif (array.n_children < 0 || NumericCast<idx_t>(array.n_children) != union_info.ChildCount() ||
\t\t    (array.n_children > 0 && !array.children)) {
\t\t\tthrow InvalidInputException("Arrow union array child count must match schema child count");
\t\t}
\t\tduckdb::vector<Vector> children;
''',
)

replace_once(
    "src/function/table/arrow_conversion.cpp",
    '''\t\tduckdb::vector<Vector> children;
\t\tfor (idx_t child_idx = 0; child_idx < NumericCast<idx_t>(array.n_children); child_idx++) {
\t\t\tVector child(members[child_idx].second, size);
\t\t\tauto &child_array = *array.children[child_idx];
\t\t\tauto &child_state = array_state.GetChild(child_idx);
\t\t\tauto &child_type = union_info.GetChild(child_idx);

\t\t\tArrowToDuckDBConversion::SetValidityMask(child, child_array, chunk_offset, size,
\t\t\t                                         NumericCast<int64_t>(parent_offset), nested_offset);
\t\t\tauto array_physical_type = child_type.GetPhysicalType();

\t\t\tswitch (array_physical_type) {
\t\t\tcase ArrowArrayPhysicalType::DICTIONARY_ENCODED:
\t\t\t\tArrowToDuckDBConversion::ColumnArrowToDuckDBDictionary(child, child_array, chunk_offset, child_state,
\t\t\t\t                                                       size, child_type);
\t\t\t\tbreak;
\t\t\tcase ArrowArrayPhysicalType::RUN_END_ENCODED:
\t\t\t\tArrowToDuckDBConversion::ColumnArrowToDuckDBRunEndEncoded(child, child_array, chunk_offset, child_state,
\t\t\t\t                                                          size, child_type);
\t\t\t\tbreak;
\t\t\tcase ArrowArrayPhysicalType::DEFAULT:
\t\t\t\tArrowToDuckDBConversion::ColumnArrowToDuckDB(child, child_array, chunk_offset, child_state, size,
\t\t\t\t                                             child_type, nested_offset, &validity_mask, false);
\t\t\t\tbreak;
\t\t\tdefault:
\t\t\t\tthrow NotImplementedException("ArrowArrayPhysicalType not recognized");
\t\t\t}

\t\t\tchildren.push_back(std::move(child));
\t\t}
''',
    '''\t\tduckdb::vector<Vector> children;
\t\tconst auto union_child_parent_offset = NumericCast<uint64_t>(array.offset) + parent_offset;
\t\tconst auto union_child_nested_offset =
\t\t    nested_offset == -1 ? nested_offset : NumericCast<int64_t>(array.offset) + nested_offset;
\t\tfor (idx_t child_idx = 0; child_idx < NumericCast<idx_t>(array.n_children); child_idx++) {
\t\t\tVector child(members[child_idx].second, size);
\t\t\tauto &child_array = *array.children[child_idx];
\t\t\tauto &child_state = array_state.GetChild(child_idx);
\t\t\tauto &child_type = union_info.GetChild(child_idx);

\t\t\tArrowToDuckDBConversion::SetValidityMask(child, child_array, chunk_offset, size,
\t\t\t                                         NumericCast<int64_t>(union_child_parent_offset),
\t\t\t                                         union_child_nested_offset);
\t\t\tauto array_physical_type = child_type.GetPhysicalType();

\t\t\tswitch (array_physical_type) {
\t\t\tcase ArrowArrayPhysicalType::DICTIONARY_ENCODED:
\t\t\t\tArrowToDuckDBConversion::ColumnArrowToDuckDBDictionary(
\t\t\t\t    child, child_array, chunk_offset, child_state, size, child_type, union_child_nested_offset,
\t\t\t\t    &validity_mask, union_child_parent_offset);
\t\t\t\tbreak;
\t\t\tcase ArrowArrayPhysicalType::RUN_END_ENCODED:
\t\t\t\tArrowToDuckDBConversion::ColumnArrowToDuckDBRunEndEncoded(
\t\t\t\t    child, child_array, chunk_offset, child_state, size, child_type, union_child_nested_offset,
\t\t\t\t    &validity_mask, union_child_parent_offset);
\t\t\t\tbreak;
\t\t\tcase ArrowArrayPhysicalType::DEFAULT:
\t\t\t\tArrowToDuckDBConversion::ColumnArrowToDuckDB(
\t\t\t\t    child, child_array, chunk_offset, child_state, size, child_type, union_child_nested_offset,
\t\t\t\t    &validity_mask, union_child_parent_offset, false);
\t\t\t\tbreak;
\t\t\tdefault:
\t\t\t\tthrow NotImplementedException("ArrowArrayPhysicalType not recognized");
\t\t\t}

\t\t\tchildren.push_back(std::move(child));
\t\t}
''',
)

print("FIELDWORK_262_HARDENING=nonnegative-type-id-duplicate-rejection")
print("FIELDWORK_262_REPAIR=union-child-offset-and-count-validation")
