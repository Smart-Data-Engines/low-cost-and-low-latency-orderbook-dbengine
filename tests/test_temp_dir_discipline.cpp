// A test's temporary directories and files are its process's own (#285).
//
// A test that names its data directory the same way in every run - a counter that starts at 0 in
// every process, `std::rand()` without a seed, a fixed name - is given the directory a crashed run
// of the same binary left behind, and opens that run's data as its own. On 19 November 2026 a
// mutant of #284 crashed `test_pending_row_names`, whose directory was `/tmp/pending_row_names_0`
// in every run, and the next run stored 3860 rows of a pair where it had written 1930: the crashed
// run's WAL was replayed beside its own. Fifty-seven places in forty-three of the tests' files named
// a path that way.
//
// **What this claims**: every path a test builds under the temporary directory - a
// `temp_directory_path()` followed by `/` - goes through `test_temp_dirs.hpp`, or names its process
// by `getpid()`, or is a `mkdtemp` template (`XXXXXX`) in the same statement. **What it does not
// cover**: a path spelled out as `/tmp/...` rather than built from `temp_directory_path()` - the
// tests that do so write `mkdtemp` templates - and whether a test removes what it made.

#include "source_scan.hpp"
#include "test_temp_dirs.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <unistd.h>

namespace fs = std::filesystem;
namespace scan = ob::source_scan;

namespace {

/// The lines of `src` that build a path under the temporary directory that is not this process's
/// own: the statement holding `temp_directory_path()` and a `/` after it has no `getpid` in its
/// code and no `XXXXXX` in its literals.
std::vector<size_t> unowned_temp_paths(const std::string& src) {
    const scan::Views v = scan::blank(src);
    std::vector<size_t> out;
    for (const size_t open : scan::calls_of(v.bare, "temp_directory_path")) {
        const std::string args = scan::call_args(v.bare, open);
        size_t k = open + args.size();
        while (k < v.bare.size() && (v.bare[k] == ' ' || v.bare[k] == '\t' || v.bare[k] == '\n')) ++k;
        if (k >= v.bare.size() || v.bare[k] != '/') continue;  // the directory itself, not a path under it
        const size_t from = v.bare.find_last_of(";{}", open);
        const size_t start = from == std::string::npos ? 0 : from + 1;
        size_t end = v.bare.find(';', open);
        if (end == std::string::npos) end = v.bare.size();
        const std::string code = v.code.substr(start, end - start);
        const std::string bare = v.bare.substr(start, end - start);
        if (bare.find("getpid") != std::string::npos || code.find("XXXXXX") != std::string::npos) continue;
        out.push_back(scan::line_of(src, open));
    }
    return out;
}

/// The tests' own sources, but the header that is the rule.
std::vector<fs::path> test_sources() {
    std::vector<fs::path> out;
    for (const auto& e : fs::directory_iterator(fs::path(OB_SOURCE_DIR) / "tests")) {
        const auto ext = e.path().extension();
        if (!e.is_regular_file() || (ext != ".cpp" && ext != ".hpp")) continue;
        if (e.path().filename() == "test_temp_dirs.hpp") continue;
        out.push_back(e.path());
    }
    return out;
}

/// The name own_temp_name() gives `prefix` next in this process, without taking it: asked of the helper and
/// its number put back, so that a planted directory is the one the helper would take by construction rather
/// than by a copy of its naming here (a control of the mutation table found the copy: with another separator
/// the helper passed a planted directory it never reached).
fs::path next_name(const std::string& prefix, const std::string& suffix = {}) {
    const uint64_t n = ob::test::g_temp_name_counter.load();
    fs::path p = ob::test::own_temp_name(prefix, suffix);
    ob::test::g_temp_name_counter.store(n);
    return p;
}

}  // namespace

// ── The rule's own cases ─────────────────────────────────────────────────────────────────────────

TEST(TempDirRule, APathNamedTheSameInEveryRunIsNotTheProcesss) {
    EXPECT_EQ(unowned_temp_paths("auto p = fs::temp_directory_path() /\n"
                                 "         (prefix + std::to_string(g_dir_counter.fetch_add(1)));\n"),
              (std::vector<size_t>{1}))
        << "a counter starts at 0 in every process";
    EXPECT_EQ(unowned_temp_paths("auto p = fs::temp_directory_path() / (prefix + std::to_string(std::rand()));\n"),
              (std::vector<size_t>{1}))
        << "std::rand() without a seed gives every process the same numbers";
    EXPECT_EQ(unowned_temp_paths("int x;\nconst auto dir = fs::temp_directory_path() / \"ob_tls_dir\";\n"),
              (std::vector<size_t>{2}))
        << "a fixed name";
    EXPECT_EQ(unowned_temp_paths("const auto dir = fs::temp_directory_path()\n                 / \"ob_tls_dir\";\n"),
              (std::vector<size_t>{1}))
        << "the path's '/' on the next line";
}

TEST(TempDirRule, AProcessIdAMkdtempTemplateOrTheDirectoryItselfIsTheProcesss) {
    EXPECT_TRUE(unowned_temp_paths("auto p = fs::temp_directory_path() /\n"
                                   "         (\"ob_x_\" + std::to_string(::getpid()) + \"_\" + n);\n")
                    .empty());
    EXPECT_TRUE(unowned_temp_paths("std::string t = (fs::temp_directory_path() / (prefix + \"_XXXXXX\")).string();\n")
                    .empty());
    EXPECT_TRUE(unowned_temp_paths("refusal(std::filesystem::temp_directory_path().string(), mine);\n").empty())
        << "the temporary directory itself is no path under it";
    EXPECT_TRUE(unowned_temp_paths("// fs::temp_directory_path() / \"x\" in a comment\n").empty());
    EXPECT_TRUE(unowned_temp_paths("auto p = ob::test::own_temp_dir(prefix);\n").empty());
}

// ── The tree ─────────────────────────────────────────────────────────────────────────────────────

TEST(TempDirDiscipline, EveryPathATestBuildsUnderTheTemporaryDirectoryIsItsProcesss) {
    const std::vector<fs::path> sources = test_sources();
    ASSERT_GT(sources.size(), 100u) << "the tests directory was not found under " << OB_SOURCE_DIR;
    size_t built = 0;
    for (const fs::path& p : sources) {
        const std::string src = scan::read_file(p);
        built += scan::calls_of(scan::blank(src).bare, "temp_directory_path").size();
        for (const size_t line : unowned_temp_paths(src)) {
            ADD_FAILURE() << scan::rel(p) << ":" << line
                          << " builds a path under the temporary directory that a crashed run of the same "
                             "binary would leave for the next: use ob::test::own_temp_dir() or own_temp_file() "
                             "(tests/test_temp_dirs.hpp)";
        }
    }
    EXPECT_GT(built, 20u) << "the scan found almost no temporary paths; it is reading the wrong files";
}

// ── The helper ───────────────────────────────────────────────────────────────────────────────────

TEST(OwnTempDir, IsANewEmptyDirectoryNamedByTheProcess) {
    const fs::path a = ob::test::own_temp_dir("ob_own_temp_dir_");
    const fs::path b = ob::test::own_temp_dir("ob_own_temp_dir_");
    EXPECT_NE(a, b);
    for (const fs::path& d : {a, b}) {
        EXPECT_TRUE(fs::is_directory(d));
        EXPECT_TRUE(fs::is_empty(d));
        EXPECT_EQ(d.parent_path(), fs::temp_directory_path());
        EXPECT_NE(d.filename().string().find(std::to_string(::getpid())), std::string::npos) << d;
        fs::remove_all(d);
    }
}

TEST(OwnTempDir, ADirectoryLeftUnderItsNamePassesOverIt) {
    // What a crashed run of a process with this process's id would have left: the directory the
    // next call would take, with data in it.
    const fs::path left = next_name("ob_own_temp_left_");
    fs::create_directories(left);
    { std::ofstream(left / "wal_000001.log") << "a crashed run's rows"; }
    const fs::path mine = ob::test::own_temp_dir("ob_own_temp_left_");
    EXPECT_NE(mine, left);
    EXPECT_TRUE(fs::is_empty(mine)) << "a test opened the crashed run's data as its own";
    EXPECT_TRUE(fs::exists(left / "wal_000001.log")) << "what the crashed run left is not this run's to remove";
    fs::remove_all(mine);
    fs::remove_all(left);
}

TEST(OwnTempFile, AFileLeftUnderItsNameIsRemovedAndNothingIsCreated) {
    const fs::path left = next_name("ob_own_temp_file_", ".conf");
    { std::ofstream(left) << "a crashed run's secret"; }
    fs::permissions(left, fs::perms::none);  // a test that chmods its file and crashes before restoring it
    const fs::path mine = ob::test::own_temp_file("ob_own_temp_file_", ".conf");
    EXPECT_EQ(mine, left);
    EXPECT_FALSE(fs::exists(mine)) << "the next writer would meet the crashed run's file";
    const fs::path fresh = ob::test::own_temp_file("ob_own_temp_file_", ".conf");
    EXPECT_NE(fresh, mine);
    EXPECT_FALSE(fs::exists(fresh));
}

TEST(OwnTempDir, IsUnderTMPDIRWhenItIsSet) {
    // The mutation tables give each run a TMPDIR of their own; the helper has to honour it.
    const fs::path base = ob::test::own_temp_dir("ob_own_temp_base_");
    const char* before = std::getenv("TMPDIR");
    const std::string saved = before ? before : "";
    ASSERT_EQ(::setenv("TMPDIR", base.c_str(), 1), 0);
    const fs::path d = ob::test::own_temp_dir("ob_own_temp_under_");
    if (before) ::setenv("TMPDIR", saved.c_str(), 1); else ::unsetenv("TMPDIR");
    EXPECT_EQ(d.parent_path(), base);
    fs::remove_all(base);
}
