from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old not in text:
        raise RuntimeError(f"{label} insertion point changed")
    return text.replace(old, new, 1)


source_path = Path("src/common/hive_partitioning.cpp")
source = source_path.read_text()

if "static bool HasURIScheme(const string &filename)" not in source:
    marker = '''bool HivePartitioning::IsNull(const string &input) {
\treturn StringUtil::CIEquals(input, "NULL") || input == "__HIVE_DEFAULT_PARTITION__";
}

'''
    helper = '''static bool HasURIScheme(const string &filename) {
\tauto scheme_end = filename.find("://");
\tif (scheme_end == string::npos || scheme_end <= 1) {
\t\t// A one-character prefix can be a Windows drive letter.
\t\treturn false;
\t}
\tif (!((filename[0] >= 'a' && filename[0] <= 'z') || (filename[0] >= 'A' && filename[0] <= 'Z'))) {
\t\treturn false;
\t}
\tfor (idx_t i = 1; i < scheme_end; i++) {
\t\tauto c = filename[i];
\t\tif (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '+' || c == '-' ||
\t\t      c == '.')) {
\t\t\treturn false;
\t\t}
\t}
\treturn true;
}

'''
    source = replace_once(source, marker, marker + helper, "IsNull")

    state_marker = '''\tbool candidate_partition = true;
\tstd::map<string, string> result;
'''
    state_replacement = '''\tbool candidate_partition = true;
\tconst bool uri_path = HasURIScheme(filename);
\tstd::map<string, string> result;
'''
    source = replace_once(source, state_marker, state_replacement, "Parse state")

    loop_marker = '''\tfor (idx_t c = 0; c < filename.size(); c++) {
\t\tif (filename[c] == '?' || filename[c] == '\\n') {
'''
    loop_replacement = '''\tfor (idx_t c = 0; c < filename.size(); c++) {
\t\tif (uri_path && (filename[c] == '?' || filename[c] == '#')) {
\t\t\t// URI query and fragment text cannot contain path partitions
\t\t\tbreak;
\t\t}
\t\tif (filename[c] == '?' || filename[c] == '\\n') {
'''
    source = replace_once(source, loop_marker, loop_replacement, "Parse loop")
    source_path.write_text(source)

cmake_path = Path("test/common/CMakeLists.txt")
cmake = cmake_path.read_text()
if "test_hive_partitioning.cpp" not in cmake:
    marker = "  test_external_file_cache.cpp\n"
    cmake = replace_once(cmake, marker, marker + "  test_hive_partitioning.cpp\n", "test/common CMake")
    cmake_path.write_text(cmake)
