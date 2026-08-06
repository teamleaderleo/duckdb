#include "capi_tester.hpp"
#include "duckdb/common/arrow/arrow.hpp"

#include <array>
#include <string>

using namespace duckdb;

namespace {

struct ReleaseCounter {
	idx_t count = 0;
};

void ReleaseSchemaNoOp(ArrowSchema *schema) {
	schema->release = nullptr;
}

void ReleaseArrayNoOp(ArrowArray *array) {
	array->release = nullptr;
}

void ReleaseRoot(ArrowArray *array) {
	auto counter = static_cast<ReleaseCounter *>(array->private_data);
	counter->count++;
	array->release = nullptr;
}

} // namespace

TEST_CASE("C API Arrow validates runtime root child count before ownership transfer", "[capi][arrow]") {
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));

	ArrowSchema first_schema {};
	first_schema.format = "i";
	first_schema.name = "first";
	first_schema.release = ReleaseSchemaNoOp;

	ArrowSchema second_schema {};
	second_schema.format = "i";
	second_schema.name = "second";
	second_schema.release = ReleaseSchemaNoOp;

	ArrowSchema *schema_children[2] = {&first_schema, &second_schema};
	ArrowSchema root_schema {};
	root_schema.format = "+s";
	root_schema.name = "root";
	root_schema.n_children = 2;
	root_schema.children = schema_children;
	root_schema.release = ReleaseSchemaNoOp;

	duckdb_arrow_converted_schema converted_schema = nullptr;
	auto error = duckdb_schema_from_arrow(tester.connection, &root_schema, &converted_schema);
	REQUIRE(error == nullptr);
	REQUIRE(converted_schema != nullptr);

	std::array<int32_t, 2> first_values = {11, 12};
	std::array<int32_t, 2> second_values = {21, 22};

	const void *first_buffers[2] = {nullptr, first_values.data()};
	ArrowArray first_array {};
	first_array.length = NumericCast<int64_t>(first_values.size());
	first_array.n_buffers = 2;
	first_array.buffers = first_buffers;
	first_array.release = ReleaseArrayNoOp;

	const void *second_buffers[2] = {nullptr, second_values.data()};
	ArrowArray second_array {};
	second_array.length = NumericCast<int64_t>(second_values.size());
	second_array.n_buffers = 2;
	second_array.buffers = second_buffers;
	second_array.release = ReleaseArrayNoOp;

	// Keep two valid entries allocated so a pre-fix implementation can read both
	// deterministically despite declaring only one runtime child.
	ArrowArray *array_children[2] = {&first_array, &second_array};
	const void *root_buffers[1] = {nullptr};
	ReleaseCounter release_counter;

	ArrowArray root_array {};
	root_array.length = NumericCast<int64_t>(first_values.size());
	root_array.n_buffers = 1;
	root_array.buffers = root_buffers;
	root_array.n_children = 1;
	root_array.children = array_children;
	root_array.release = ReleaseRoot;
	root_array.private_data = &release_counter;

	duckdb_data_chunk output_chunk = reinterpret_cast<duckdb_data_chunk>(uintptr_t(1));
	error = duckdb_data_chunk_from_arrow(tester.connection, &root_array, converted_schema, &output_chunk);

	REQUIRE(error != nullptr);
	CHECK(duckdb_error_data_error_type(error) == DUCKDB_ERROR_INVALID_INPUT);
	CHECK(std::string(duckdb_error_data_message(error)) == "Arrow array child count mismatch: expected 2, got 1");
	CHECK(output_chunk == nullptr);
	CHECK(root_array.release == ReleaseRoot);
	CHECK(release_counter.count == 0);

	duckdb_destroy_error_data(&error);
	root_array.release(&root_array);
	CHECK(release_counter.count == 1);

	duckdb_destroy_arrow_converted_schema(&converted_schema);
	if (root_schema.release) {
		root_schema.release(&root_schema);
	}
}
