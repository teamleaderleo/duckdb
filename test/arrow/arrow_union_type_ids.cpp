#include "catch.hpp"

#include "arrow/arrow_test_helper.hpp"
#include "duckdb/common/adbc/single_batch_array_stream.hpp"

using namespace duckdb;

static void NoOpSchemaRelease(ArrowSchema *schema) {
	schema->release = nullptr;
}

static void NoOpArrayRelease(ArrowArray *array) {
	array->release = nullptr;
}

static vector<Value> ScanSparseIntUnion(const char *format, const int8_t (&type_ids)[3]) {
	constexpr idx_t N_ROWS = 3;
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

	ArrowSchema *root_child_ptrs[1] = {&union_schema};
	ArrowSchema root_schema = {};
	root_schema.format = "+s";
	root_schema.name = "root";
	root_schema.n_children = 1;
	root_schema.children = root_child_ptrs;
	root_schema.release = NoOpSchemaRelease;

	int32_t child_values[3][N_ROWS] = {
	    {10, 11, 12},
	    {20, 21, 22},
	    {30, 31, 32},
	};
	const void *child_buffers[3][2] = {};
	ArrowArray child_arrays[3] = {};
	for (idx_t child_idx = 0; child_idx < 3; child_idx++) {
		child_buffers[child_idx][1] = child_values[child_idx];
		child_arrays[child_idx].length = N_ROWS;
		child_arrays[child_idx].n_buffers = 2;
		child_arrays[child_idx].buffers = child_buffers[child_idx];
		child_arrays[child_idx].release = NoOpArrayRelease;
	}

	const void *union_buffers[1] = {type_ids};
	ArrowArray *union_child_array_ptrs[3] = {&child_arrays[0], &child_arrays[1], &child_arrays[2]};
	ArrowArray union_array = {};
	union_array.length = N_ROWS;
	union_array.n_buffers = 1;
	union_array.buffers = union_buffers;
	union_array.n_children = 3;
	union_array.children = union_child_array_ptrs;
	union_array.release = NoOpArrayRelease;

	const void *root_buffers[1] = {nullptr};
	ArrowArray *root_child_array_ptrs[1] = {&union_array};
	ArrowArray root_array = {};
	root_array.length = N_ROWS;
	root_array.n_buffers = 1;
	root_array.buffers = root_buffers;
	root_array.n_children = 1;
	root_array.children = root_child_array_ptrs;
	root_array.release = NoOpArrayRelease;

	ArrowArrayStream stream = {};
	AdbcError adbc_error = {};
	auto status = duckdb_adbc::BatchToArrayStream(&root_array, &root_schema, &stream, &adbc_error);
	REQUIRE(status == ADBC_STATUS_OK);

	DuckDB db(nullptr);
	Connection connection(db);
	auto params = ArrowTestHelper::ConstructArrowScan(stream);
	auto result = ArrowTestHelper::ScanArrowObject(connection, params);
	REQUIRE(result);
	REQUIRE_FALSE(result->HasError());

	vector<Value> values;
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		REQUIRE(chunk->ColumnCount() == 1);
		for (idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
			values.push_back(chunk->data[0].GetValue(row_idx));
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
	const int8_t type_ids[3] = {0, 1, 2};
	auto values = ScanSparseIntUnion("+us:0,1,2", type_ids);
	RequireMappedValues(values);
}

TEST_CASE("Arrow sparse union maps non-sequential type IDs", "[arrow][fieldwork]") {
	const int8_t type_ids[3] = {5, 7, 9};
	auto values = ScanSparseIntUnion("+us:5,7,9", type_ids);
	RequireMappedValues(values);
}

TEST_CASE("Arrow sparse union maps reordered in-range type IDs", "[arrow][fieldwork]") {
	const int8_t type_ids[3] = {2, 1, 0};
	auto values = ScanSparseIntUnion("+us:2,1,0", type_ids);
	RequireMappedValues(values);
}
