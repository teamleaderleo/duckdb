#include "duckdb/common/hive_partitioning.hpp"
#include "catch.hpp"

using namespace duckdb;

TEST_CASE("Hive partition parsing stops at URI metadata boundaries", "[hive_partitioning]") {
	auto query = HivePartitioning::Parse("https://host/p=real/file.parquet?redirect=/q=fake/file");
	REQUIRE(query.size() == 1);
	REQUIRE(query.at("p") == "real");
	REQUIRE(query.find("q") == query.end());

	auto fragment = HivePartitioning::Parse("https://host/p=real/file.parquet#redirect=/q=fake/file");
	REQUIRE(fragment.size() == 1);
	REQUIRE(fragment.at("p") == "real");
	REQUIRE(fragment.find("q") == fragment.end());

	auto custom_scheme = HivePartitioning::Parse("s3+test://bucket/p=real/file.parquet?redirect=/q=fake/file");
	REQUIRE(custom_scheme.size() == 1);
	REQUIRE(custom_scheme.at("p") == "real");
	REQUIRE(custom_scheme.find("q") == custom_scheme.end());
}

TEST_CASE("Hive partition parsing preserves local path characters", "[hive_partitioning]") {
	auto question_mark = HivePartitioning::Parse("/tmp/literal?segment/p=local/file.parquet");
	REQUIRE(question_mark.size() == 1);
	REQUIRE(question_mark.at("p") == "local");

	auto fragment = HivePartitioning::Parse("/tmp/literal#segment/p=hash/file.parquet");
	REQUIRE(fragment.size() == 1);
	REQUIRE(fragment.at("p") == "hash");

	auto windows_drive = HivePartitioning::Parse("C://p=drive/file.parquet");
	REQUIRE(windows_drive.size() == 1);
	REQUIRE(windows_drive.at("p") == "drive");

	auto embedded_scheme_text = HivePartitioning::Parse("relative/http://p=local/file.parquet");
	REQUIRE(embedded_scheme_text.size() == 1);
	REQUIRE(embedded_scheme_text.at("p") == "local");
}
