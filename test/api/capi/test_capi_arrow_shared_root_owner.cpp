#include "capi_tester.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

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

TEST_CASE("C API Arrow shared root retains projected later column", "[capi][arrow]") {
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

	const std::array<int32_t, 3> expected_second = {21, 22, 23};
	std::array<int32_t, 3> first_values = {11, 12, 13};
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
	REQUIRE(root_array.release == nullptr);
	REQUIRE(release_state.release_count == 0);

	idx_t releases_after_source_destroy;
	{
		auto internal_chunk = reinterpret_cast<DataChunk *>(output_chunk);
		Vector surviving_second = Vector::Ref(internal_chunk->data[1]);

		duckdb_destroy_data_chunk(&output_chunk);
		releases_after_source_destroy = release_state.release_count;

		const auto values = FlatVector::GetData<int32_t>(surviving_second);
		std::array<int32_t, 3> surviving_values = {values[0], values[1], values[2]};

		INFO("root release count after source chunk destroy=" << releases_after_source_destroy);
		INFO("surviving second output=" << surviving_values[0] << "," << surviving_values[1] << ","
		                                << surviving_values[2]);
		CHECK(releases_after_source_destroy == 0);
		CHECK(surviving_values == expected_second);
	}

	const auto releases_after_survivor_destroy = release_state.release_count;
	INFO("root release count after surviving projection destroy=" << releases_after_survivor_destroy);
	CHECK(releases_after_survivor_destroy == 1);

	duckdb_destroy_arrow_converted_schema(&converted_schema);
	if (root_schema.release) {
		root_schema.release(&root_schema);
	}
}
