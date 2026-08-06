#include "capi_tester.hpp"
#include "duckdb/common/arrow/arrow.hpp"

using namespace duckdb;

namespace {

void ReleaseSchemaNoOp(ArrowSchema *schema) {
	schema->release = nullptr;
}

bool ConvertSingleIntSchema(duckdb_connection connection, const char *name) {
	ArrowSchema child {};
	child.format = "i";
	child.name = name;
	child.release = ReleaseSchemaNoOp;

	ArrowSchema *children[1] = {&child};
	ArrowSchema root {};
	root.format = "+s";
	root.name = "root";
	root.n_children = 1;
	root.children = children;
	root.release = ReleaseSchemaNoOp;

	duckdb_arrow_converted_schema converted = nullptr;
	auto error = duckdb_schema_from_arrow(connection, &root, &converted);
	const bool accepted = error == nullptr;
	if (error) {
		duckdb_destroy_error_data(&error);
	}
	if (converted) {
		duckdb_destroy_arrow_converted_schema(&converted);
	}
	if (root.release) {
		root.release(&root);
	}
	return accepted;
}

} // namespace

TEST_CASE("C API Arrow accepts a null optional field name", "[capi][arrow]") {
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));

	const bool empty_name_accepted = ConvertSingleIntSchema(tester.connection, "");
	const bool null_name_accepted = ConvertSingleIntSchema(tester.connection, nullptr);

	INFO("empty field name accepted=" << empty_name_accepted << " null field name accepted=" << null_name_accepted);
	CHECK(empty_name_accepted);
	CHECK(null_name_accepted);
}
