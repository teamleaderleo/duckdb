#include "capi_tester.hpp"
#include "duckdb/common/arrow/arrow.hpp"

#include <array>

using namespace duckdb;

namespace {

struct RootReleaseState {
	idx_t release_count = 0;
	int32_t *poison_data = nullptr;
	idx_t poison_count = 0;
};

void ReleaseSchemaNoOp(ArrowSchema *schema) {
	schema->release = nullptr;
}

void ReleaseArrayNoOp(ArrowArray *array) {
	array->release = nullptr;
}

void ReleaseRootAndPoisonSecondColumn(ArrowArray *array) {
	auto state = static_cast<RootReleaseState *>(array->private_data);
	state->release_count++;
	for (idx_t i = 0; i < state->poison_count; i++) {
		state->poison_data[i] = -9999;
	}
	array->release = nullptr;
}

} // namespace

TEST_CASE("C API Arrow multi-column conversion retains root ownership", "[capi][arrow]") {
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

	const std::array<int32_t, 3> expected_first = {11, 12, 13};
	const std::array<int32_t, 3> expected_second = {21, 22, 23};
	std::array<int32_t, 3> first_values = expected_first;
	std::array<int32_t, 3> second_values = expected_second;

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

	ArrowArray *array_children[2] = {&first_array, &second_array};
	const void *root_buffers[1] = {nullptr};
	RootReleaseState release_state;
	release_state.poison_data = second_values.data();
	release_state.poison_count = second_values.size();

	ArrowArray root_array {};
	root_array.length = NumericCast<int64_t>(first_values.size());
	root_array.n_buffers = 1;
	root_array.buffers = root_buffers;
	root_array.n_children = 2;
	root_array.children = array_children;
	root_array.release = ReleaseRootAndPoisonSecondColumn;
	root_array.private_data = &release_state;

	duckdb_data_chunk output_chunk = nullptr;
	error = duckdb_data_chunk_from_arrow(tester.connection, &root_array, converted_schema, &output_chunk);
	REQUIRE(error == nullptr);
	REQUIRE(output_chunk != nullptr);

	const auto releases_before_chunk_destroy = release_state.release_count;
	const auto output_size = duckdb_data_chunk_get_size(output_chunk);
	REQUIRE(output_size == first_values.size());

	auto first_vector = duckdb_data_chunk_get_vector(output_chunk, 0);
	auto second_vector = duckdb_data_chunk_get_vector(output_chunk, 1);
	auto first_output = static_cast<int32_t *>(duckdb_vector_get_data(first_vector));
	auto second_output = static_cast<int32_t *>(duckdb_vector_get_data(second_vector));

	std::array<int32_t, 3> copied_first = {first_output[0], first_output[1], first_output[2]};
	std::array<int32_t, 3> copied_second = {second_output[0], second_output[1], second_output[2]};

	duckdb_destroy_data_chunk(&output_chunk);
	const auto releases_after_chunk_destroy = release_state.release_count;
	duckdb_destroy_arrow_converted_schema(&converted_schema);
	if (root_schema.release) {
		root_schema.release(&root_schema);
	}

	INFO("root release count before chunk destroy=" << releases_before_chunk_destroy);
	INFO("root release count after chunk destroy=" << releases_after_chunk_destroy);
	INFO("second output=" << copied_second[0] << "," << copied_second[1] << "," << copied_second[2]);

	CHECK(copied_first == expected_first);
	CHECK(copied_second == expected_second);
	CHECK(releases_before_chunk_destroy == 0);
	CHECK(releases_after_chunk_destroy == 1);
}
