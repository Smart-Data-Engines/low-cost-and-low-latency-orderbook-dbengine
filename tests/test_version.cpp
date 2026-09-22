// The version, and the three places it used to live independently.
//
// Before #90 the version a running node showed was a hardcoded `v0.1.0` — in
// `tools/ob_tcp_server.cpp`'s startup line and, as #148 found, in the welcome banner every client
// reads and in the CLI's greeting — and nothing checked any of them against `project(... VERSION)`
// in `CMakeLists.txt` or against `pyproject.toml`. Copies of one fact that could drift in silence,
// and the failure mode is an operator told the wrong build is running, which is worse than being
// told nothing.
//
// The binary now takes it from the build system through a compile definition, so those are gone.
// The Python package still carries its own, because a wheel's metadata cannot be a C++ macro, and
// this test is what keeps the two in step.
#include "orderbook/version.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <istream>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace {

std::string read_source(const char* relative) {
    const std::filesystem::path path = std::filesystem::path(OB_SOURCE_DIR) / relative;
    std::ifstream in(path);
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

std::string first_match(const std::string& text, const std::regex& pattern) {
    std::smatch m;
    return std::regex_search(text, m, pattern) ? m[1].str() : std::string{};
}

/// Every line of `in` that carries `version` inside a string literal, numbered, comments aside.
///
/// One function for the tree and for the rule's own cases below, so a scan that stops seeing a
/// literal fails on the cases instead of passing on a tree it no longer reads properly.
std::vector<std::string> literals_carrying(std::istream& in, const std::string& version) {
    const std::string escaped = std::regex_replace(version, std::regex("\\."), "\\.");
    // The version anywhere inside one literal, as a whole number: `v10.1.0` is not `0.1.0`.
    const std::regex literal("\"[^\"\\n]*\\bv?" + escaped + "\\b[^\"\\n]*\"");
    std::vector<std::string> found;
    std::string line;
    for (std::size_t line_no = 1; std::getline(in, line); ++line_no) {
        // std::regex is slow, and a line without the digits cannot match.
        if (line.find(version) == std::string::npos) continue;
        // A comment may quote the version it is about; only code is a copy of it.
        const std::size_t comment = line.find("//");
        const std::size_t quote = line.find('"');
        const bool commented = comment != std::string::npos &&
                               (quote == std::string::npos || comment < quote);
        if (!commented && std::regex_search(line, literal)) {
            found.push_back(std::to_string(line_no) + ": " + line);
        }
    }
    return found;
}

}  // namespace

TEST(VersionStatic, TheBinaryReportsTheBuildSystemsVersion) {
    const std::string cmake = read_source("CMakeLists.txt");
    ASSERT_FALSE(cmake.empty()) << "cannot read CMakeLists.txt";

    const std::string declared =
        first_match(cmake, std::regex(R"(project\([^)]*VERSION\s+([0-9]+\.[0-9]+\.[0-9]+))"));
    ASSERT_FALSE(declared.empty()) << "no VERSION in project(); the regex or the file has changed";

    EXPECT_EQ(std::string(ob::version()), declared)
        << "the compiled version and `project(... VERSION)` disagree, which means the compile "
           "definition is not reaching this translation unit";
}

TEST(VersionStatic, ThePythonPackageAgreesWithTheBuildSystem) {
    const std::string cmake = read_source("CMakeLists.txt");
    const std::string pyproject = read_source("pyproject.toml");
    ASSERT_FALSE(pyproject.empty()) << "cannot read pyproject.toml";

    const std::string declared =
        first_match(cmake, std::regex(R"(project\([^)]*VERSION\s+([0-9]+\.[0-9]+\.[0-9]+))"));
    const std::string python =
        // A custom delimiter, because the pattern contains `")` and the default raw-string
        // terminator would end the literal in the middle of the regex. Anchored on a newline
        // rather than with `^`, so no locale- or flag-dependent multiline behaviour is involved.
        first_match(pyproject, std::regex(R"re(\nversion\s*=\s*"([0-9]+\.[0-9]+\.[0-9]+)")re"));
    ASSERT_FALSE(python.empty()) << "no version in pyproject.toml";

    EXPECT_EQ(python, declared)
        << "pyproject.toml says " << python << " and CMake says " << declared
        << ". `pip install orderbook-dbengine` would report a version the binary does not, and a "
           "bug report citing one of them would point at the wrong build. Bump both.";
}

TEST(VersionStatic, TheVersionIsNotRetypedInSources) {
    // A literal that agrees today is a literal that drifts at the first bump. The one in
    // `tools/ob_tcp_server.cpp` was the whole of #90's first half.
    //
    // Two holes closed by #148. The pattern matched a string literal made of the version and
    // nothing else, so the welcome banner — "OK ob_tcp_server v<version>\n\n", the first bytes
    // every client reads — carried a copy for as long as this test existed and passed it. And the
    // files were four names written here, so `tools/ob_cli.cpp` printing its own was never looked
    // at. Now: the version anywhere inside a literal, in every C++ file under the directories
    // binaries are built from, listed from the tree rather than by hand.
    const std::string version{ob::version()};

    // The rule's own cases, through the function the tree goes through: the banner #148 found,
    // the CLI's greeting, a comment quoting the version, the form that is right, and a longer
    // number on each side of it. Exactly the first two carry the version as a literal.
    std::istringstream cases(
        "    s->send_response(\"OK ob_tcp_server v" + version + "\\n\\n\");\n"
        "    std::cout << \"orderbook-dbengine CLI v" + version + "\\n\";\n"
        "    // #90 missed \"OK ob_tcp_server v" + version + "\" here\n"
        "    send(\"OK ob_tcp_server v\" + std::string(version()));\n"
        "    send(\"v1" + version + "\");\n"
        "    send(\"v" + version + "1\");\n");
    const std::vector<std::string> case_hits = literals_carrying(cases, version);
    ASSERT_EQ(case_hits.size(), 2u)
        << "the scan reported " << case_hits.size() << " of the six cases, and exactly the first "
        << "two carry the version as a literal; a rule that misreads its own cases cannot be "
        << "trusted with the tree";
    EXPECT_EQ(case_hits[0].rfind("1: ", 0), 0u) << case_hits[0];
    EXPECT_EQ(case_hits[1].rfind("2: ", 0), 0u) << case_hits[1];

    const std::filesystem::path root(OB_SOURCE_DIR);
    std::set<std::string> read;
    for (const char* dir : {"src", "tools", "include", "benchmarks"}) {
        for (const auto& entry : std::filesystem::recursive_directory_iterator(root / dir)) {
            const std::string extension = entry.path().extension().string();
            if (!entry.is_regular_file() || (extension != ".cpp" && extension != ".hpp")) continue;
            const std::string relative = entry.path().lexically_relative(root).generic_string();
            read.insert(relative);
            std::ifstream in(entry.path());
            for (const std::string& hit : literals_carrying(in, version)) {
                ADD_FAILURE() << relative << ":" << hit
                              << "\ncarries the version as a literal. It comes from "
                                 "`ob::version()`, which comes from the build system; a second "
                                 "copy is a copy that drifts.";
            }
        }
    }
    // The places copies were found in (#90, #148) must be among the files read, so a walk that
    // lost a directory says so rather than passing a tree it never opened.
    for (const char* known : {"tools/ob_tcp_server.cpp", "tools/ob_cli.cpp", "src/tcp_server.cpp",
                              "include/orderbook/version.hpp"}) {
        EXPECT_TRUE(read.count(known))
            << known << " was not read; " << read.size() << " files were";
    }
}
