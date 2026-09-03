// The version, and the three places it used to live independently.
//
// Before #90 the only version a running node could show was a hardcoded `v0.1.0` in
// `tools/ob_tcp_server.cpp`, and nothing checked it against `project(... VERSION)` in
// `CMakeLists.txt` or against `pyproject.toml`. Three copies of one fact, two of which could drift
// in silence — and the failure mode is an operator told the wrong build is running, which is worse
// than being told nothing.
//
// The binary now takes it from the build system through a compile definition, so that copy is gone.
// The Python package still carries its own, because a wheel's metadata cannot be a C++ macro, and
// this test is what keeps the two in step.
#include "orderbook/version.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <regex>
#include <string>

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
    const std::string version{ob::version()};
    const std::regex literal("\"v?" + std::regex_replace(version, std::regex("\\."), "\\.") + "\"");

    for (const char* relative : {"tools/ob_tcp_server.cpp", "src/tcp_server.cpp",
                                 "src/response_formatter.cpp", "src/metrics.cpp"}) {
        const std::string source = read_source(relative);
        ASSERT_FALSE(source.empty()) << relative;
        EXPECT_FALSE(std::regex_search(source, literal))
            << relative << " contains the version as a literal. It comes from `ob::version()`, "
            << "which comes from the build system; a second copy is a copy that drifts.";
    }
}
