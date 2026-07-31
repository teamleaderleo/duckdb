#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    target = Path(path)
    text = target.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one source anchor, found {count}")
    target.write_text(text.replace(old, new, 1))
    print(f"updated {path}")


replace_once(
    "src/include/duckdb/function/table/arrow/enum/arrow_type_info_type.hpp",
    "enum class ArrowTypeInfoType : uint8_t { LIST, STRUCT, DATE_TIME, STRING, ARRAY, DECIMAL };",
    "enum class ArrowTypeInfoType : uint8_t { LIST, STRUCT, DATE_TIME, STRING, ARRAY, DECIMAL, UNION };",
)

replace_once(
    "src/common/enum_util.cpp",
    '''\t\t{ static_cast<uint32_t>(ArrowTypeInfoType::ARRAY), "ARRAY" },
\t\t{ static_cast<uint32_t>(ArrowTypeInfoType::DECIMAL), "DECIMAL" }
\t};
\treturn values;
}

template<>
const char* EnumUtil::ToChars<ArrowTypeInfoType>(ArrowTypeInfoType value) {
\treturn StringUtil::EnumToString(GetArrowTypeInfoTypeValues(), 6, "ArrowTypeInfoType", static_cast<uint32_t>(value));
}

template<>
ArrowTypeInfoType EnumUtil::FromString<ArrowTypeInfoType>(const char *value) {
\treturn static_cast<ArrowTypeInfoType>(StringUtil::StringToEnum(GetArrowTypeInfoTypeValues(), 6, "ArrowTypeInfoType", value));
}''',
    '''\t\t{ static_cast<uint32_t>(ArrowTypeInfoType::ARRAY), "ARRAY" },
\t\t{ static_cast<uint32_t>(ArrowTypeInfoType::DECIMAL), "DECIMAL" },
\t\t{ static_cast<uint32_t>(ArrowTypeInfoType::UNION), "UNION" }
\t};
\treturn values;
}

template<>
const char* EnumUtil::ToChars<ArrowTypeInfoType>(ArrowTypeInfoType value) {
\treturn StringUtil::EnumToString(GetArrowTypeInfoTypeValues(), 7, "ArrowTypeInfoType", static_cast<uint32_t>(value));
}

template<>
ArrowTypeInfoType EnumUtil::FromString<ArrowTypeInfoType>(const char *value) {
\treturn static_cast<ArrowTypeInfoType>(StringUtil::StringToEnum(GetArrowTypeInfoTypeValues(), 7, "ArrowTypeInfoType", value));
}''',
)

replace_once(
    "src/include/duckdb/function/table/arrow/arrow_type_info.hpp",
    '''struct ArrowArrayInfo : public ArrowTypeInfo {
public:
\tstatic constexpr const ArrowTypeInfoType TYPE = ArrowTypeInfoType::ARRAY;''',
    '''struct ArrowUnionInfo : public ArrowTypeInfo {
public:
\tstatic constexpr const ArrowTypeInfoType TYPE = ArrowTypeInfoType::UNION;

public:
\texplicit ArrowUnionInfo(vector<shared_ptr<ArrowType>> children, vector<int8_t> type_ids);
\t~ArrowUnionInfo() override;

public:
\tidx_t ChildCount() const;
\tconst ArrowType &GetChild(idx_t index) const;
\tconst vector<shared_ptr<ArrowType>> &GetChildren() const;
\tidx_t TypeIdToChildIndex(int8_t type_id) const;

private:
\tvector<shared_ptr<ArrowType>> children;
\tvector<idx_t> type_id_to_child_idx;
};

struct ArrowArrayInfo : public ArrowTypeInfo {
public:
\tstatic constexpr const ArrowTypeInfoType TYPE = ArrowTypeInfoType::ARRAY;''',
)

replace_once(
    "src/function/table/arrow/arrow_type_info.cpp",
    '''//===--------------------------------------------------------------------===//
// ArrowArrayInfo
//===--------------------------------------------------------------------===//''',
    '''//===--------------------------------------------------------------------===//
// ArrowUnionInfo
//===--------------------------------------------------------------------===//

ArrowUnionInfo::ArrowUnionInfo(vector<shared_ptr<ArrowType>> children, vector<int8_t> type_ids)
    : ArrowTypeInfo(ArrowTypeInfoType::UNION), children(std::move(children)),
      type_id_to_child_idx(128, DConstants::INVALID_INDEX) {
\tif (this->children.size() != type_ids.size()) {
\t\tthrow InvalidInputException("Arrow union type ID count must match child count");
\t}
\tfor (idx_t child_idx = 0; child_idx < type_ids.size(); child_idx++) {
\t\tauto type_id = type_ids[child_idx];
\t\tif (type_id < 0) {
\t\t\tthrow InvalidInputException("Arrow union type ID out of range: %d", static_cast<int>(type_id));
\t\t}
\t\ttype_id_to_child_idx[NumericCast<idx_t>(type_id)] = child_idx;
\t}
}

ArrowUnionInfo::~ArrowUnionInfo() {
}

idx_t ArrowUnionInfo::ChildCount() const {
\treturn children.size();
}

const ArrowType &ArrowUnionInfo::GetChild(idx_t index) const {
\tD_ASSERT(index < children.size());
\treturn *children[index];
}

const vector<shared_ptr<ArrowType>> &ArrowUnionInfo::GetChildren() const {
\treturn children;
}

idx_t ArrowUnionInfo::TypeIdToChildIndex(int8_t type_id) const {
\tif (type_id < 0) {
\t\tthrow InvalidInputException("Arrow union type ID out of range: %d", static_cast<int>(type_id));
\t}
\tauto child_idx = type_id_to_child_idx[NumericCast<idx_t>(type_id)];
\tif (child_idx == DConstants::INVALID_INDEX) {
\t\tthrow InvalidInputException("Arrow union type ID %d does not map to a child", static_cast<int>(type_id));
\t}
\treturn child_idx;
}

//===--------------------------------------------------------------------===//
// ArrowArrayInfo
//===--------------------------------------------------------------------===//''',
)

replace_once(
    "src/function/table/arrow/arrow_duck_schema.cpp",
    '''\t} else if (format[0] == '+' && format[1] == 'u') {
\t\tif (format[2] != 's') {
\t\t\tthrow NotImplementedException("Unsupported Internal Arrow Type: \\\"%c\\\" Union", format[2]);
\t\t}
\t\tD_ASSERT(format[3] == ':');

\t\tstd::string prefix = "+us:";
\t\t// TODO: what are these type ids actually for?
\t\tauto type_ids = StringUtil::Split(format.substr(prefix.size()), ',');

\t\tchild_list_t<LogicalType> members;
\t\tvector<shared_ptr<ArrowType>> children;
\t\tif (schema.n_children == 0) {
\t\t\tthrow InvalidInputException("Attempted to convert a UNION with no fields to DuckDB which is not supported");
\t\t}
\t\tfor (idx_t type_idx = 0; type_idx < static_cast<idx_t>(schema.n_children); type_idx++) {
\t\t\tauto type = schema.children[type_idx];

\t\t\tchildren.emplace_back(GetArrowLogicalType(context, *type));
\t\t\tmembers.emplace_back(type->name, children.back()->GetDuckType());
\t\t}

\t\tauto type_info = make_uniq<ArrowStructInfo>(std::move(children));
\t\tauto union_type = make_uniq<ArrowType>(LogicalType::UNION(members), std::move(type_info));
\t\treturn union_type;''',
    '''\t} else if (format.size() >= 2 && format[0] == '+' && format[1] == 'u') {
\t\tif (format.size() < 4 || format[2] != 's') {
\t\t\tthrow NotImplementedException("Unsupported Internal Arrow Union Type: \\\"%s\\\"", format);
\t\t}
\t\tif (format[3] != ':') {
\t\t\tthrow InvalidInputException("Invalid Arrow sparse union format string: \\\"%s\\\"", format);
\t\t}

\t\tauto type_id_strings = StringUtil::Split(format.substr(4), ',');
\t\tif (schema.n_children == 0) {
\t\t\tthrow InvalidInputException("Attempted to convert a UNION with no fields to DuckDB which is not supported");
\t\t}
\t\tif (type_id_strings.size() != NumericCast<idx_t>(schema.n_children)) {
\t\t\tthrow InvalidInputException("Arrow union type ID count must match child count");
\t\t}

\t\tvector<int8_t> type_ids;
\t\ttype_ids.reserve(type_id_strings.size());
\t\tfor (const auto &type_id_string : type_id_strings) {
\t\t\tsize_t parsed_length = 0;
\t\t\tint parsed_type_id;
\t\t\ttry {
\t\t\t\tparsed_type_id = std::stoi(type_id_string, &parsed_length);
\t\t\t} catch (const std::exception &) {
\t\t\t\tthrow InvalidInputException("Invalid Arrow union type ID: \\\"%s\\\"", type_id_string);
\t\t\t}
\t\t\tif (parsed_length != type_id_string.size() || parsed_type_id < 0 || parsed_type_id > 127) {
\t\t\t\tthrow InvalidInputException("Arrow union type ID out of range: %s", type_id_string);
\t\t\t}
\t\t\ttype_ids.push_back(NumericCast<int8_t>(parsed_type_id));
\t\t}

\t\tchild_list_t<LogicalType> members;
\t\tvector<shared_ptr<ArrowType>> children;
\t\tfor (idx_t type_idx = 0; type_idx < static_cast<idx_t>(schema.n_children); type_idx++) {
\t\t\tauto type = schema.children[type_idx];
\t\t\tchildren.emplace_back(GetArrowLogicalType(context, *type));
\t\t\tmembers.emplace_back(type->name, children.back()->GetDuckType());
\t\t}

\t\tauto type_info = make_uniq<ArrowUnionInfo>(std::move(children), std::move(type_ids));
\t\tauto union_type = make_uniq<ArrowType>(LogicalType::UNION(members), std::move(type_info));
\t\treturn union_type;''',
)

replace_once(
    "src/function/table/arrow/arrow_duck_schema.cpp",
    '''\tcase LogicalTypeId::UNION: {
\t\tauto &union_info = type_info->Cast<ArrowStructInfo>();''',
    '''\tcase LogicalTypeId::UNION: {
\t\tauto &union_info = type_info->Cast<ArrowUnionInfo>();''',
)

replace_once(
    "src/function/table/arrow.cpp",
    '''\tcase LogicalTypeId::STRUCT:
\tcase LogicalTypeId::TUPLE:
\tcase LogicalTypeId::UNION: {
\t\tconst auto &struct_info = type.GetTypeInfo<ArrowStructInfo>();
\t\tfor (idx_t i = 0; i < struct_info.ChildCount(); i++) {
\t\t\tif (HasViewType(struct_info.GetChild(i))) {
\t\t\t\treturn true;
\t\t\t}
\t\t}
\t\treturn false;
\t}''',
    '''\tcase LogicalTypeId::STRUCT:
\tcase LogicalTypeId::TUPLE: {
\t\tconst auto &struct_info = type.GetTypeInfo<ArrowStructInfo>();
\t\tfor (idx_t i = 0; i < struct_info.ChildCount(); i++) {
\t\t\tif (HasViewType(struct_info.GetChild(i))) {
\t\t\t\treturn true;
\t\t\t}
\t\t}
\t\treturn false;
\t}
\tcase LogicalTypeId::UNION: {
\t\tconst auto &union_info = type.GetTypeInfo<ArrowUnionInfo>();
\t\tfor (idx_t i = 0; i < union_info.ChildCount(); i++) {
\t\t\tif (HasViewType(union_info.GetChild(i))) {
\t\t\t\treturn true;
\t\t\t}
\t\t}
\t\treturn false;
\t}''',
)

replace_once(
    "src/function/table/arrow_conversion.cpp",
    '''\t\tauto &validity_mask = FlatVector::ValidityMutable(vector);
\t\tauto &union_info = arrow_type.GetTypeInfo<ArrowStructInfo>();''',
    '''\t\tauto &validity_mask = FlatVector::ValidityMutable(vector);
\t\tauto &union_info = arrow_type.GetTypeInfo<ArrowUnionInfo>();''',
)

replace_once(
    "src/function/table/arrow_conversion.cpp",
    '''\t\tfor (idx_t row_idx = 0; row_idx < size; row_idx++) {
\t\t\tauto tag = NumericCast<uint8_t>(type_ids[row_idx]);

\t\t\tauto out_of_range = tag >= array.n_children;
\t\t\tif (out_of_range) {
\t\t\t\tthrow InvalidInputException("Arrow union tag out of range: %d", tag);
\t\t\t}

\t\t\tconst Value &value = children[tag].GetValue(row_idx);
\t\t\tvector.SetValue(row_idx, value.IsNull() ? Value() : Value::UNION(members, tag, value));
\t\t}''',
    '''\t\tfor (idx_t row_idx = 0; row_idx < size; row_idx++) {
\t\t\tauto child_idx = union_info.TypeIdToChildIndex(type_ids[row_idx]);
\t\t\tconst Value &value = children[child_idx].GetValue(row_idx);
\t\t\tvector.SetValue(row_idx,
\t\t\t                value.IsNull() ? Value() : Value::UNION(members, NumericCast<uint8_t>(child_idx), value));
\t\t}''',
)

print("FIELDWORK_262_PATCH=arrow-union-type-id-mapping")
