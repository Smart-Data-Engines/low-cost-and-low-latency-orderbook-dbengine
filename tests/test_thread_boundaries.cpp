// Every thread body goes through an exception boundary (#112).
//
// A joinable `std::thread` whose function lets an exception escape ends the **process** through
// `std::terminate`, not the thread. Measured before this test existed: one `ENOSPC` on the flush
// thread's 24-byte checkpoint aborted a node that was answering its clients correctly, killed an
// *idle* node 0.5 s into idleness with nobody connected, and did it again on each of three
// restarts while the disk stayed full. Sixteen of seventeen `std::thread` constructions had no
// boundary at all.
//
// **What this claims, exactly**: every `std::thread` construction in `src/` that takes a callable
// has `run_thread_body` somewhere inside that construction's parentheses. One grep-able property,
// chosen over "there is a `try` before the loop in the entry function" because the second needs a
// parser and this needs a paren match - and because the one place that had put a `try` inside its
// body still left the statements before it and the mutex after it outside the guard.
//
// **What it therefore does not cover**, named rather than left to be discovered: it does not check
// that the boundary is the *outermost* thing in the body, that a loop inside survives its own
// failures (that is a per-iteration boundary, a judgement per loop, recorded in #112), or that a
// thread started outside `src/` - in a test, or in `tools/` - has one. A `std::thread` handed a
// `std::function` filled in elsewhere would also pass, because the body is not here to read.
#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace {

struct Construction {
    int         line;
    std::string span;   // from `std::thread(` to its matching `)`
};

/// Every `std::thread(...)` in `text` that is handed a callable.
///
/// Two exclusions, both of which this function got wrong first:
///
/// - `std::thread victim;` and `std::thread stale = std::move(worker_);` construct nothing from a
///   callable. The first has no parenthesis at all; the second is a move.
/// - **`std::move` may appear inside the callable** and must not be read as a move-construction.
///   `async_snapshot.cpp` writes `std::thread([this, token, produce = std::move(produce)]() {`,
///   and an earlier version of this rule filed the only guarded construction in the tree as a
///   move because it searched the whole line for `std::move`. So the check is whether a move
///   begins *immediately* after the parenthesis.
std::vector<Construction> constructions(const std::string& text) {
    std::vector<Construction> out;
    const std::string needle = "std::thread";
    size_t at = 0;
    while ((at = text.find(needle, at)) != std::string::npos) {
        size_t i = at + needle.size();
        while (i < text.size() && std::isspace(static_cast<unsigned char>(text[i]))) ++i;
        if (i >= text.size() || text[i] != '(') { at += needle.size(); continue; }

        size_t arg = i + 1;
        while (arg < text.size() && std::isspace(static_cast<unsigned char>(text[arg]))) ++arg;
        if (text.compare(arg, 10, "std::move(") == 0) { at += needle.size(); continue; }

        int depth = 1;
        size_t j = i + 1;
        for (; j < text.size() && depth > 0; ++j) {
            if (text[j] == '(') ++depth;
            else if (text[j] == ')') --depth;
        }
        const int line = 1 + static_cast<int>(std::count(text.begin(), text.begin() + static_cast<long>(at), '\n'));
        out.push_back({line, text.substr(at, j - at)});
        at = j;
    }
    return out;
}

bool guarded(const Construction& c) {
    return c.span.find("run_thread_body") != std::string::npos;
}

std::string read_file(const std::filesystem::path& p) {
    std::ifstream in(p, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

} // namespace

// ── The rule's own cases, each one a mistake it made before it shipped ───────
TEST(ThreadBoundariesRule, ADeclarationIsNotAConstruction) {
    EXPECT_TRUE(constructions("std::thread victim;").empty());
    EXPECT_TRUE(constructions("std::thread         watch_thread;").empty());
}

TEST(ThreadBoundariesRule, AMoveIsNotAConstruction) {
    EXPECT_TRUE(constructions("std::thread stale = std::move(worker_);").empty());
    EXPECT_TRUE(constructions("worker_ = std::thread(std::move(other));").empty());
}

TEST(ThreadBoundariesRule, TheTypeNameAloneIsNotAConstruction) {
    EXPECT_TRUE(constructions("std::hash<std::thread::id>{}(std::this_thread::get_id())").empty());
}

TEST(ThreadBoundariesRule, MoveInsideACaptureListIsStillAConstruction) {
    // The case that made this rule report the tree's only guarded thread as a move.
    const auto found = constructions(
        "worker_ = std::thread([this, token, produce = std::move(produce)]() mutable {\n"
        "    run_thread_body(\"snapshot\", \"worker\", [&] { work(); });\n"
        "});\n");
    ASSERT_EQ(found.size(), 1u);
    EXPECT_TRUE(guarded(found[0]));
}

TEST(ThreadBoundariesRule, ItSeesAnUnguardedConstructionAndAGuardedOneDifferently) {
    const auto bare = constructions("t_ = std::thread([this] { loop(); });");
    ASSERT_EQ(bare.size(), 1u);
    EXPECT_FALSE(guarded(bare[0])) << "the rule cannot tell the two apart, so it proves nothing";

    const auto wrapped = constructions(
        "t_ = std::thread([this] { run_thread_body(\"c\", \"loop\", [this] { loop(); }); });");
    ASSERT_EQ(wrapped.size(), 1u);
    EXPECT_TRUE(guarded(wrapped[0]));
}

TEST(ThreadBoundariesRule, AConstructionSpanningLinesIsFoundWhole) {
    const auto found = constructions(
        "impl_->watch_thread = std::thread([endpoint, prefix]() {\n"
        "    run_thread_body(\"coordinator\", \"watch\", [&] {\n"
        "        watch(endpoint, prefix);\n"
        "    });\n"
        "});\n");
    ASSERT_EQ(found.size(), 1u);
    EXPECT_TRUE(guarded(found[0])) << "the span stopped short of the boundary on the third line";
}

// ── The tree ─────────────────────────────────────────────────────────────────
TEST(ThreadBoundaries, EveryThreadBodyGoesThroughTheBoundary) {
    std::vector<std::filesystem::path> files;
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(std::filesystem::path(OB_SOURCE_DIR) / "src")) {
        if (entry.is_regular_file() && entry.path().extension() == ".cpp") files.push_back(entry.path());
    }
    std::sort(files.begin(), files.end());
    ASSERT_FALSE(files.empty()) << "no sources were read, so this test measured nothing";

    std::vector<std::string> unguarded;
    int total = 0;
    for (const auto& file : files) {
        const std::string text = read_file(file);
        for (const auto& c : constructions(text)) {
            ++total;
            if (!guarded(c)) {
                unguarded.push_back(
                    std::filesystem::relative(file, std::filesystem::path(OB_SOURCE_DIR)).string()
                    + ":" + std::to_string(c.line));
            }
        }
    }

    // The count is asserted as well as the verdict: a rule that stopped finding constructions
    // would otherwise report a clean tree, which is the failure mode this file exists to prevent.
    EXPECT_GE(total, 15) << "far fewer thread constructions than this tree has had; the rule has "
                            "probably stopped matching";

    std::string report;
    for (const auto& site : unguarded) report += "\n  " + site;
    EXPECT_TRUE(unguarded.empty())
        << unguarded.size() << " of " << total << " thread constructions let an exception reach "
        << "the runtime, which ends the process rather than the thread:" << report;
}
