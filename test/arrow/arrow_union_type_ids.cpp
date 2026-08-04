#include "catch.hpp"

#include "arrow/arrow_test_helper.hpp"

using namespace duckdb;

static void NoOpSchemaRelease(ArrowSchema *schema) {
	schema->release = nullptr;
}

static void NoOpArrayRelease(ArrowArray *array) {
	array->release = nullptr;
}

struct SingleBatchStreamState {
	ArrowSchema schema;
	ArrowArray batch;
	bool consumed = false;
};

static int SingleBatchGetSchema(ArrowArrayStream *stream, ArrowSchema *out) {
	auto &state = *reinterpret_cast<SingleBatchStreamState *>(stream->private_data);
	*out = state.schema;
	return 0;
}

static int SingleBatchGetNext(ArrowArrayStream *stream, ArrowArray *out) {
	auto &state = *reinterpret_cast<SingleBatchStreamState *>(stream->private_data);
	if (state.consumed) {
		*out = {};
		return 0;
	}
	*out = state.batch;
	state.consumed = true;
	return 0;
}

static const char *SingleBatchGetLastError(ArrowArrayStream *) {
	return nullptr;
}

static void SingleBatchRelease(ArrowArrayStream *stream) {
	stream->release = nullptr;
}

static vector<Value> ScanSparseIntUnion(const char *format, const vector<int8_t> &physical_type_ids,
                                        idx_t union_offset = 0, const char *expected_error = nullptr,
                                        bool wrap_in_fixed_size_list = false, int64_t array_child_count = 3) {
	constexpr idx_t N_ROWS = 3;
	const auto physical_count = N_ROWS + union_offset;
	REQUIRE(physical_type_ids.size() == physical_count);
	const char *child_names[3] = {"zero", "one", "two"};

	ArrowSchema child_schemas[3] = {};
	for (idx_t child_idx = 0; child_idx < 3; child_idx++) {
		child_schemas[child_idx].format = "i";
		child_schemas[child_idx].name = child_names[child_idx];
		child_schemas[child_idx].flags = ARROW_FLAG_NULLABLE;
		child_schemas[child_idx].release = NoOpSchemaRelease;
	}

	ArrowSchema *union_child_ptrs[3] = {&child_schemas[0], &child_schemas[1], &child_schemas[2]};
	ArrowSchema union_schema = {};
	union_schema.format = format;
	union_schema.name = "mapped_union";
	union_schema.n_children = 3;
	union_schema.children = union_child_ptrs;
	union_schema.release = NoOpSchemaRelease;

	ArrowSchema *fixed_size_list_child_ptrs[1] = {&union_schema};
	ArrowSchema fixed_size_list_schema = {};
	fixed_size_list_schema.format = "+w:1";
	fixed_size_list_schema.name = "wrapped_union";
	fixed_size_list_schema.n_children = 1;
	fixed_size_list_schema.children = fixed_size_list_child_ptrs;
	fixed_size_list_schema.release = NoOpSchemaRelease;

	ArrowSchema *root_child_ptrs[1] = {wrap_in_fixed_size_list ? &fixed_size_list_schema : &union_schema};
	ArrowSchema root_schema = {};
	root_schema.format = "+s";
	root_schema.name = "root";
	root_schema.n_children = 1;
	root_schema.children = root_child_ptrs;
	root_schema.release = NoOpSchemaRelease;

	vector<vector<int32_t>> child_values(3, vector<int32_t>(physical_count));
	for (idx_t child_idx = 0; child_idx < 3; child_idx++) {
		for (idx_t physical_idx = 0; physical_idx < physical_count; physical_idx++) {
			const auto logical_idx = physical_idx < union_offset ? 0 : physical_idx - union_offset;
			child_values[child_idx][physical_idx] = NumericCast<int32_t>((child_idx + 1) * 10 + logical_idx);
		}
	}

	const void *child_buffers[3][2] = {};
	ArrowArray child_arrays[3] = {};
	for (idx_t child_idx = 0; child_idx < 3; child_idx++) {
		child_buffers[child_idx][1] = child_values[child_idx].data();
		child_arrays[child_idx].length = NumericCast<int64_t>(physical_count);
		child_arrays[child_idx].n_buffers = 2;
		child_arrays[child_idx].buffers = child_buffers[child_idx];
		child_arrays[child_idx].release = NoOpArrayRelease;
	}

	const void *union_buffers[1] = {physical_type_ids.data()};
	ArrowArray *union_child_array_ptrs[3] = {&child_arrays[0], &child_arrays[1], &child_arrays[2]};
	ArrowArray union_array = {};
	union_array.length = N_ROWS;
	union_array.offset = NumericCast<int64_t>(union_offset);
	union_array.n_buffers = 1;
	union_array.buffers = union_buffers;
	union_array.n_children = array_child_count;
	union_array.children = union_child_array_ptrs;
	union_array.release = NoOpArrayRelease;

	const void *fixed_size_list_buffers[1] = {nullptr};
	ArrowArray *fixed_size_list_child_array_ptrs[1] = {&union_array};
	ArrowArray fixed_size_list_array = {};
	fixed_size_list_array.length = N_ROWS;
	fixed_size_list_array.n_buffers = 1;
	fixed_size_list_array.buffers = fixed_size_list_buffers;
	fixed_size_list_array.n_children = 1;
	fixed_size_list_array.children = fixed_size_list_child_array_ptrs;
	fixed_size_list_array.release = NoOpArrayRelease;

	const void *root_buffers[1] = {nullptr};
	ArrowArray *root_child_array_ptrs[1] = {wrap_in_fixed_size_list ? &fixed_size_list_array : &union_array};
	ArrowArray root_array = {};
	root_array.length = N_ROWS;
	root_array.n_buffers = 1;
	root_array.buffers = root_buffers;
	root_array.n_children = 1;
	root_array.children = root_child_array_ptrs;
	root_array.release = NoOpArrayRelease;

	SingleBatchStreamState stream_state {root_schema, root_array};
	ArrowArrayStream stream = {};
	stream.get_schema = SingleBatchGetSchema;
	stream.get_next = SingleBatchGetNext;
	stream.get_last_error = SingleBatchGetLastError;
	stream.release = SingleBatchRelease;
	stream.private_data = &stream_state;

	DuckDB db(nullptr);
	Connection connection(db);
	auto params = ArrowTestHelper::ConstructArrowScan(stream);
	unique_ptr<QueryResult> result;
	try {
		result = connection.TableFunction("arrow_scan", params)->Execute();
	} catch (const std::exception &exception) {
		REQUIRE(expected_error);
		REQUIRE(string(exception.what()).find(expected_error) != string::npos);
		return {};
	}
	REQUIRE(result);

	if (expected_error) {
		REQUIRE(result->HasError());
		REQUIRE(result->GetError().find(expected_error) != string::npos);
		return {};
	}

	REQUIRE_FALSE(result->HasError());
	vector<Value> values;
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		REQUIRE(chunk->ColumnCount() == 1);
		for (idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
			auto value = chunk->data[0].GetValue(row_idx);
			if (wrap_in_fixed_size_list) {
				const auto &array_children = ArrayValue::GetChildren(value);
				REQUIRE(array_children.size() == 1);
				values.push_back(array_children[0]);
			} else {
				values.push_back(std::move(value));
			}
		}
	}
	REQUIRE_FALSE(result->HasError());
	REQUIRE(values.size() == N_ROWS);
	return values;
}

static void RequireMappedValues(const vector<Value> &values) {
	REQUIRE(UnionValue::GetTag(values[0]) == 0);
	REQUIRE(UnionValue::GetValue(values[0]) == Value::INTEGER(10));
	REQUIRE(UnionValue::GetTag(values[1]) == 1);
	REQUIRE(UnionValue::GetValue(values[1]) == Value::INTEGER(21));
	REQUIRE(UnionValue::GetTag(values[2]) == 2);
	REQUIRE(UnionValue::GetValue(values[2]) == Value::INTEGER(32));
}

TEST_CASE("Arrow sparse union maps identity type IDs", "[arrow][fieldwork]") {
	auto values = ScanSparseIntUnion("+us:0,1,2", {0, 1, 2});
	RequireMappedValues(values);
}

TEST_CASE("Arrow sparse union maps non-sequential type IDs", "[arrow][fieldwork]") {
	auto values = ScanSparseIntUnion("+us:5,7,9", {5, 7, 9});
	RequireMappedValues(values);
}

TEST_CASE("Arrow sparse union maps reordered in-range type IDs", "[arrow][fieldwork]") {
	auto values = ScanSparseIntUnion("+us:2,1,0", {2, 1, 0});
	RequireMappedValues(values);
}

TEST_CASE("Arrow sparse union maps upper-bound type IDs", "[arrow][fieldwork]") {
	auto values = ScanSparseIntUnion("+us:0,64,127", {0, 64, 127});
	RequireMappedValues(values);
}

TEST_CASE("Arrow sparse union mapping honors a nonzero parent offset", "[arrow][fieldwork]") {
	auto values = ScanSparseIntUnion("+us:5,7,9", {99, 5, 7, 9}, 1);
	RequireMappedValues(values);
}

TEST_CASE("Arrow sparse union mapping honors its offset when nested in a fixed-size list", "[arrow][fieldwork]") {
	auto values = ScanSparseIntUnion("+us:5,7,9", {99, 5, 7, 9}, 1, nullptr, true);
	RequireMappedValues(values);
}

TEST_CASE("Arrow sparse union rejects duplicate schema type IDs", "[arrow][fieldwork]") {
	ScanSparseIntUnion("+us:5,5,9", {5, 5, 9}, 0, "Arrow union type ID 5 is duplicated");
}

TEST_CASE("Arrow sparse union rejects a negative schema type ID", "[arrow][fieldwork]") {
	ScanSparseIntUnion("+us:-1,0,127", {-1, 0, 127}, 0, "Arrow union type ID out of range: -1");
}

TEST_CASE("Arrow sparse union rejects a schema type-ID count mismatch", "[arrow][fieldwork]") {
	ScanSparseIntUnion("+us:5,7", {5, 7, 9}, 0, "Arrow union type ID count must match child count");
}

TEST_CASE("Arrow sparse union rejects an array child-count mismatch", "[arrow][fieldwork]") {
	ScanSparseIntUnion("+us:5,7,9", {5, 7, 9}, 0,
	                   "Arrow union array child count must match schema child count", false, 2);
}

TEST_CASE("Arrow sparse union rejects an unmapped runtime type ID", "[arrow][fieldwork]") {
	ScanSparseIntUnion("+us:5,7,9", {5, 8, 9}, 0, "Arrow union type ID 8 does not map to a child");
}

TEST_CASE("Arrow sparse union rejects a negative runtime type ID", "[arrow][fieldwork]") {
	ScanSparseIntUnion("+us:0,64,127", {0, -1, 127}, 0, "Arrow union type ID out of range: -1");
}
