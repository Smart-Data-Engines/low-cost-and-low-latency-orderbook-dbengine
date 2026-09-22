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
// **The second rule, added when #112's last loop was closed and completed by #131.** The outer
// boundary stops the process dying and leaves a subsystem that ends on its first exception, so
// every loop in `src/` where stopping is not survivable guards **one iteration at a time** - four
// closed one at a time in #112, one that had done so since before it, and the other seven together
// through `LoopGuard`. The set is named here, and naming it is the point: a fourteenth such loop
// has to join a list or explain itself, where thirteen scattered `try`s say nothing about the one
// nobody wrote.
//
// **What this file therefore does not cover**, named rather than left to be discovered: it does not
// check that the boundary is the *outermost* thing in the body, that the per-iteration `try` covers
// the whole iteration rather than part of it, or that a thread started outside `src/` - in a test,
// or in `tools/` - has one. A `std::thread` handed a `std::function` filled in elsewhere would also
// pass, because the body is not here to read.
#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <regex>
#include <string>
#include <utility>
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

// ── Every loop in src/, classified ───────────────────────────────────────────
//
// The first version of this was a hand-written list of the four loops #112 names, and a mutation
// deleting one row **survived**: the rule covered less and stayed green. That is the third time
// inside this one item that a list written by hand turned out not to be evidence about the code -
// the count of thread entry points was wrong twice the same way, one then eleven then seventeen -
// so the set is derived from the tree and the list only says what each member *is*.
//
// Both directions, for the same reason the metrics checker runs both: a loop in neither list fails
// here, and a list entry naming a function the tree no longer has fails too.
namespace {

struct LoopFn {
    std::string file;
    std::string name;   // Class::function
    std::string body;
    bool        has_loop_statement;
    bool        guarded;
};

/// Every `void Class::something_loop()` definition in `src/`, plus `AntiEntropyManager::loop`.
///
/// Sliced from the signature to the next line that is a lone `}` in column zero, which is this
/// tree's layout for a free-standing definition. A neighbour's `try` therefore cannot satisfy a
/// row: `replication.cpp` holds two functions called `run_loop`.
std::vector<LoopFn> loop_functions() {
    std::vector<std::filesystem::path> files;
    for (const auto& entry :
         std::filesystem::directory_iterator(std::filesystem::path(OB_SOURCE_DIR) / "src")) {
        if (entry.is_regular_file() && entry.path().extension() == ".cpp") files.push_back(entry.path());
    }
    std::sort(files.begin(), files.end());

    std::vector<LoopFn> out;
    const std::regex sig(R"(^void ([A-Za-z_]+::[a-z_]*loop)\(\) \{$)");
    for (const auto& file : files) {
        const std::string text = read_file(file);
        std::size_t at = 0;
        while (at < text.size()) {
            const std::size_t eol = text.find('\n', at);
            const std::string line = text.substr(at, (eol == std::string::npos ? text.size() : eol) - at);
            std::smatch m;
            if (std::regex_match(line, m, sig)) {
                const std::size_t end = text.find("\n}\n", at);
                const std::string body = text.substr(at, (end == std::string::npos ? text.size() : end) - at);
                const std::size_t loop_at = std::min(body.find("while ("), body.find("for (;;)"));
                const bool has_loop = loop_at != std::string::npos;
                out.push_back(LoopFn{file.filename().string(), m[1].str(), body, has_loop,
                                     has_loop && body.find("try {", loop_at) != std::string::npos});
            }
            if (eol == std::string::npos) break;
            at = eol + 1;
        }
    }
    return out;
}

}  // namespace

TEST(ThreadBoundaries, EveryLoopInTheTreeIsEitherGuardedPerIterationOrRecorded) {
    // Guarded: a `try` inside the loop, so one iteration is what a failure costs. What each one's
    // death would cost is written beside it, because that is the judgement being reviewed.
    const std::vector<std::pair<std::string, std::string>> guarded = {
        {"Engine::flush_loop",
         "rows stay in memory and the WAL grows without bound while clients are answered OK"},
        {"FailoverManager::monitor_loop",
         "the node never learns about a role change (#82's shape), and a half-finished promotion "
         "leaves it a replica of itself (#130)"},
        {"MultiMasterManager::io_loop",
         "the node is out of the mesh while PING, MM_PEERS and the peer's connected row all still "
         "look healthy"},
        {"ReplicationManager::run_loop",
         "no replica is accepted, no catch-up advances and no heartbeat goes out"},
        {"PeerRegistry::lease_loop",
         "the lease expires, the mesh registration is gone from etcd and nothing puts it back "
         "(#132)"},
        {"ReplicationClient::run_loop",
         "this replica stops following its primary; guarded since before #112, around the connect "
         "and replay it retries"},
        {"PeerRegistry::watch_loop",
         "no new or moved peer is ever learned again, and this is the loop that runs the topology "
         "callback, which in the mesh dials peers (#131)"},
        {"MultiMasterManager::reconnect_loop",
         "a dropped mesh link is never re-dialled; #95's and #97's work all lives here (#131)"},
        {"AntiEntropyManager::loop",
         "reconciliation stops, so divergence between masters is never repaired - the mechanism "
         "#57 exists for (#131)"},
        {"ShardRouter::watch_loop",
         "the shard map goes stale and this client keeps routing by it (#131)"},
        {"ShardCoordinator::watch_loop",
         "shard ownership is never re-read (#131)"},
        {"MetricsServer::run_loop",
         "/metrics stops answering: monitoring goes dark while the engine is fine, which is the "
         "same problem as the others from the other side (#131)"},
        {"OrderbookPool::health_check_loop",
         "a client pool stops noticing dead connections; the only one outside the server (#131)"},
        {"Reactor::run_loop",
         "every client this reactor serves stops being answered - on the reactor that accepts, no "
         "new client is admitted either - while the process, its other reactors and /metrics all "
         "look healthy. Guarded per event rather than per pass, because client descriptors are "
         "edge-triggered and an event abandoned with its batch is not delivered again. Note what "
         "this row's check cannot see: the loop also holds the `try` that wraps a TLS handshake, "
         "which satisfies this scan on its own, so the reactor's boundary has a static test of "
         "its own in test_tcp_server.cpp"},
    };

    // Empty since #131, and it stays in the test rather than being deleted: this is where the next
    // loop whose death is not survivable gets recorded if it is not guarded on the day it is
    // written, and an empty list is a claim the forward direction below still checks - every loop
    // in the tree has to appear in one of the two.
    const std::vector<std::pair<std::string, std::string>> recorded = {};

    const std::vector<LoopFn> found = loop_functions();
    ASSERT_GE(found.size(), guarded.size() + recorded.size())
        << "found " << found.size() << " loop functions in src/, fewer than the "
        << (guarded.size() + recorded.size()) << " already classified - the scan has stopped "
        << "matching and would report a clean tree";

    auto named_in = [](const std::vector<std::pair<std::string, std::string>>& list,
                       const std::string& name) {
        return std::any_of(list.begin(), list.end(),
                           [&](const auto& row) { return row.first == name; });
    };

    // Forward: every loop the tree has is classified, and classified correctly.
    for (const auto& fn : found) {
        if (!fn.has_loop_statement) {
            // A function whose name ends in `loop` and which contains none is a notifier, not a
            // loop. Exactly one of those exists and it is named, so a real loop cannot hide here
            // by having its `while` rewritten into something this scan does not know.
            EXPECT_EQ(fn.name, "MultiMasterManager::wake_io_loop")
                << fn.file << ": " << fn.name << " has no loop statement this scan recognises";
            continue;
        }
        const bool g = named_in(guarded, fn.name);
        const bool r = named_in(recorded, fn.name);
        EXPECT_TRUE(g || r) << fn.file << ": " << fn.name << " is a loop this test has never "
                            << "heard of. Classify it: either it guards one iteration at a time, "
                            << "or its death is recorded on the roadmap (#131)";
        if (g) {
            EXPECT_TRUE(fn.guarded)
                << fn.file << ": " << fn.name << " is listed as guarding one iteration at a time "
                << "and has no `try` inside its loop. The outer thread boundary keeps the process "
                << "alive and lets this thread end (#112)";
        }
        if (r) {
            EXPECT_FALSE(fn.guarded)
                << fn.file << ": " << fn.name << " has a per-iteration boundary now. Move it to "
                << "the guarded list with what its death would cost, and close its line in #131";
        }
    }

    // Backward: a row naming a function the tree no longer has is a row that stopped checking.
    for (const auto& list : {guarded, recorded}) {
        for (const auto& row : list) {
            EXPECT_TRUE(std::any_of(found.begin(), found.end(),
                                    [&](const LoopFn& fn) { return fn.name == row.first; }))
                << row.first << " is classified here and is not in src/ any more, so this row is "
                << "asserting about nothing. Fix the row rather than leaving it";
        }
    }
}
