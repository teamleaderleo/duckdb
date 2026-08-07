#include "capi_tester.hpp"
#include "duckdb/common/arrow/arrow.hpp"

using namespace duckdb;

namespace {

struct ArrowOwnershipProbe {
	idx_t root_array_releases = 0;
	idx_t root_schema_releases = 0;
};

void ReleaseChildArray(ArrowArray *array) {
	array->release = nullptr;
}

void ReleaseRootArray(ArrowArray *array) {
	auto probe = static_cast<ArrowOwnershipProbe *>(array->private_data);
	probe->root_array_releases++;
	for (int64_t i = 0; i < array->n_children; i++) {
		if (array->children[i]->release) {
			array->children[i]->release(array->children[i]);
		}
	}
	array->release = nullptr;
}

void ReleaseChildSchema(ArrowSchema *schema) {
	schema->release = nullptr;
}

void ReleaseRootSchema(ArrowSchema *schema) {
	auto probe = static_cast<ArrowOwnershipProbe *>(schema->private_data);
	probe->root_schema_releases++;
	for (int64_t i = 0; i < schema->n_children; i++) {
		if (schema->children[i]->release) {
			schema->children[i]->release(schema->children[i]);
		}
	}
	schema->release = nullptr;
}

} // namespace

TEST_CASE("C API Arrow chunk retains multi-column root ownership", "[capi][arrow]") {
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));

	ArrowOwnershipProbe probe;

	ArrowSchema left_schema {};
	left_schema.format = "i";
	left_schema.name = "left_value";
	left_schema.release = ReleaseChildSchema;

	ArrowSchema right_schema {};
	right_schema.format = "i";
	right_schema.name = "right_value";
	right_schema.release = ReleaseChildSchema;

	ArrowSchema *schema_children[] = {&left_schema, &right_schema};
	ArrowSchema root_schema {};
	root_schema.format = "+s";
	root_schema.n_children = 2;
	root_schema.children = schema_children;
	root_schema.release = ReleaseRootSchema;
	root_schema.private_data = &probe;

	int32_t left_value = 11;
	int32_t right_value = 22;
	const void *left_buffers[] = {nullptr, &left_value};
	const void *right_buffers[] = {nullptr, &right_value};

	ArrowArray left_array {};
	left_array.length = 1;
	left_array.n_buffers = 2;
	left_array.buffers = left_buffers;
	left_array.release = ReleaseChildArray;

	ArrowArray right_array {};
	right_array.length = 1;
	right_array.n_buffers = 2;
	right_array.buffers = right_buffers;
	right_array.release = ReleaseChildArray;

	ArrowArray *array_children[] = {&left_array, &right_array};
	const void *root_buffers[] = {nullptr};
	ArrowArray root_array {};
	root_array.length = 1;
	root_array.n_buffers = 1;
	root_array.buffers = root_buffers;
	root_array.n_children = 2;
	root_array.children = array_children;
	root_array.release = ReleaseRootArray;
	root_array.private_data = &probe;

	duckdb_arrow_converted_schema converted_schema = nullptr;
	auto err = duckdb_schema_from_arrow(tester.connection, &root_schema, &converted_schema);
	REQUIRE(err == nullptr);
	REQUIRE(converted_schema != nullptr);

	duckdb_data_chunk out_chunk = nullptr;
	err = duckdb_data_chunk_from_arrow(tester.connection, &root_array, converted_schema, &out_chunk);
	REQUIRE(err == nullptr);
	REQUIRE(out_chunk != nullptr);

	// Ownership is transferred from the caller immediately, but the producer's
	// release callback must stay deferred while the returned zero-copy chunk is alive.
	CHECK(root_array.release == nullptr);
	CHECK(probe.root_array_releases == 0);

	auto left_vector = duckdb_data_chunk_get_vector(out_chunk, 0);
	auto right_vector = duckdb_data_chunk_get_vector(out_chunk, 1);
	REQUIRE(left_vector != nullptr);
	REQUIRE(right_vector != nullptr);
	CHECK(static_cast<int32_t *>(duckdb_vector_get_data(left_vector))[0] == 11);
	CHECK(static_cast<int32_t *>(duckdb_vector_get_data(right_vector))[0] == 22);

	duckdb_destroy_data_chunk(&out_chunk);
	CHECK(probe.root_array_releases == 1);

	duckdb_destroy_arrow_converted_schema(&converted_schema);
	if (root_schema.release) {
		root_schema.release(&root_schema);
	}
	CHECK(probe.root_schema_releases == 1);
}
