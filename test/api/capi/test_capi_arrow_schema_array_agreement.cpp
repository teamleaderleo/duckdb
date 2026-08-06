#include "capi_tester.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

#include <array>

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

TEST_CASE("C API Arrow rejects runtime child-count disagreement", "[capi][arrow]") {
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

	// The allocation contains two valid child pointers so the current implementation can
	// read both without crashing, but the ArrowArray contract declares only one child.
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

	duckdb_data_chunk output_chunk = nullptr;
	error = duckdb_data_chunk_from_arrow(tester.connection, &root_array, converted_schema, &output_chunk);

	const bool accepted = error == nullptr;
	idx_t output_columns = 0;
	std::array<int32_t, 2> second_output = {-1, -1};
	if (accepted) {
		auto internal_chunk = reinterpret_cast<DataChunk *>(output_chunk);
		output_columns = internal_chunk->ColumnCount();
		if (output_columns > 1) {
			auto values = FlatVector::GetData<int32_t>(internal_chunk->data[1]);
			second_output = {values[0], values[1]};
		}
	}

	INFO("declared runtime child count=1 accepted=" << accepted << " output columns=" << output_columns
	                                                << " second output=" << second_output[0] << ","
	                                                << second_output[1]);
	CHECK(error != nullptr);

	if (error) {
		duckdb_destroy_error_data(&error);
	}
	if (output_chunk) {
		duckdb_destroy_data_chunk(&output_chunk);
	}
	if (root_array.release) {
		root_array.release(&root_array);
	}
	CHECK(release_counter.count == 1);

	duckdb_destroy_arrow_converted_schema(&converted_schema);
	if (root_schema.release) {
		root_schema.release(&root_schema);
	}
}
