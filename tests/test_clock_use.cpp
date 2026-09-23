// A clock reading becomes a number in this engine only when the number is a moment on the wall
// clock, and the list of such places is the tree rather than this file (#163).
//
// The TTL sweep compared segment times - event times, nanoseconds since 1970 - with a reading of
// `steady_clock`, which counts from this machine's boot. On a machine up for less than the
// retention the subtraction wrapped to a cutoff past every timestamp and the first sweep deleted
// every segment: measured, a restart with `--ttl-hours 24` on a machine up 21.5 hours took 200 rows
// and both their segment directories to none. On a machine up for longer nothing ever expired.
// Sweeping the tree for the same shape found three more - an anti-entropy run's time, a conflict's
// time documented as "wall clock", a snapshot manifest's creation time - none of which decided
// anything, and each of which would have the day something read it.
//
// `time_since_epoch()` is the one way to turn a clock reading into a count, and a count carries
// nothing of the clock it came from, which is how the two got compared. So it may be called on a
// `system_clock::now()` reading and nowhere else, except at the sites listed below, each with the
// reason its number is an interval and never a moment - checked both ways, so a site that
// disappears takes its entry with it. `wall_clock_ns()` (`include/orderbook/wall_clock.hpp`) is
// the ordinary way to get the time and calls neither; a monotonic reading kept as a
// `std::chrono::steady_clock::time_point` cannot be compared with a count at all.
//
// The scan reads code with comments and literals blanked (`source_scan.hpp`), and the last test
// gives it a snippet in which it must flag four sites and pass four.

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <map>
#include <string>
#include <vector>

#include "source_scan.hpp"

namespace {

using namespace ob::source_scan;

/// Files in which a count is taken from something other than `system_clock::now()`, how many
/// times, and why that count is not a moment.
struct Allowed {
    const char* file;
    size_t      sites;
    const char* why;
};

constexpr Allowed kAllowed[] = {
    {"src/logger.cpp", 1,
     "the milliseconds of a log line's time, from the system_clock::now() read two lines above"},
    {"src/multi_master.cpp", 1,
     "now_ms(), the clock of the version-vector grace window: only differences of it are used, and "
     "it is monotonic so that a stepped wall clock does not move the window"},
};

/// The callee of the call expression ending at the ')' at `close`, e.g. `std::chrono::system_clock::now`
/// for `std::chrono::system_clock::now()`; empty when the text before `close` is not a call.
std::string callee_of(const std::string& text, size_t close) {
    int depth = 0;
    size_t k = close + 1;
    while (k > 0) {
        --k;
        if (text[k] == ')') ++depth;
        if (text[k] == '(' && --depth == 0) break;
    }
    if (depth != 0) return {};
    size_t end = k;
    while (end > 0 && text[end - 1] == ' ') --end;
    size_t start = end;
    while (start > 0 && (ident(text[start - 1]) || text[start - 1] == ':')) --start;
    return text.substr(start, end - start);
}

struct Site {
    size_t line;         // 1-based
    std::string receiver;
};

/// Every `time_since_epoch()` called on a clock reading in one source: those on a
/// `system_clock::now()` reading counted, the others listed.
struct Scan {
    std::vector<Site> other;
    size_t wall{0};
};

Scan scan(const std::string& src) {
    const Views v = blank(src);
    Scan out;
    for (const size_t paren : calls_of(v.bare, "time_since_epoch")) {
        size_t name_end = paren;
        while (name_end > 0 && v.bare[name_end - 1] == ' ') --name_end;
        size_t dot = name_end - std::string("time_since_epoch").size();
        while (dot > 0 && v.bare[dot - 1] == ' ') --dot;
        if (dot == 0 || v.bare[dot - 1] != '.') continue;   // a declaration, not a call on a reading
        size_t before = dot - 1;
        while (before > 0 && v.bare[before - 1] == ' ') --before;
        std::string receiver;
        if (before > 0 && v.bare[before - 1] == ')') {
            receiver = callee_of(v.bare, before - 1) + "()";
        } else {
            size_t start = before;
            while (start > 0 && ident(v.bare[start - 1])) --start;
            receiver = v.bare.substr(start, before - start);
        }
        // `system_clock::now()`, qualified or not - and not a clock of another name ending in it.
        const std::string wall_call = "system_clock::now()";
        const bool wall =
            receiver == wall_call ||
            (receiver.size() > wall_call.size() + 2 &&
             receiver.compare(receiver.size() - wall_call.size() - 2, std::string::npos,
                              "::" + wall_call) == 0);
        if (wall) {
            ++out.wall;
        } else {
            out.other.push_back({line_of(v.bare, paren), receiver});
        }
    }
    return out;
}

}  // namespace

TEST(ClockUse, EveryCountTakenFromAClockIsTheWallClockOrAnIntervalSaysWhy) {
    std::map<std::string, std::vector<Site>> found;
    size_t wall_sites = 0;
    for (const auto& path : engine_sources()) {
        const Scan s = scan(read_file(path));
        if (!s.other.empty()) found[rel(path)] = s.other;
        wall_sites += s.wall;
    }
    // A scan that reads nothing finds nothing, so it has to find the wall-clock readings every
    // build has: the arrival stamp in the server and the checkpoint's time in the engine.
    ASSERT_GE(wall_sites, 2u) << "the scan found " << wall_sites << " system_clock reading(s) turned "
                              << "into a count, so it is not reading what it should";

    std::string unexplained;
    for (const auto& [file, sites] : found) {
        const auto allowed = std::find_if(std::begin(kAllowed), std::end(kAllowed),
                                          [&](const Allowed& a) { return file == a.file; });
        if (allowed != std::end(kAllowed) && allowed->sites == sites.size()) continue;
        for (const Site& s : sites) {
            unexplained += "\n  " + file + ":" + std::to_string(s.line) + " (on " + s.receiver + ")";
        }
    }
    EXPECT_TRUE(unexplained.empty())
        << "a clock reading turned into a count that is not the wall clock's. If it is a moment, "
        << "take it from wall_clock_ns(); if it is an interval, keep the reading a time_point, or "
        << "list the site in kAllowed with the reason its number is never read as a time:"
        << unexplained;

    std::string stale;
    for (const Allowed& a : kAllowed) {
        const auto it = found.find(a.file);
        const size_t now = it == found.end() ? 0 : it->second.size();
        if (now != a.sites) {
            stale += "\n  " + std::string(a.file) + ": listed with " + std::to_string(a.sites) +
                     " site(s), the tree has " + std::to_string(now);
        }
    }
    EXPECT_TRUE(stale.empty())
        << "an entry in kAllowed no longer matches the tree - a reason kept for a site that moved or "
        << "went is a reason for something else:" << stale;
}

TEST(ClockUse, TheScanFlagsEveryReadingThatIsNotTheWallClockAndNothingThatOnlyMentionsOne) {
    const std::string snippet =
        "auto a = std::chrono::system_clock::now().time_since_epoch();\n"             // 1: wall
        "auto b = std::chrono::steady_clock::now().time_since_epoch();\n"             // 2: flagged
        "auto c = duration_cast<milliseconds>(now.time_since_epoch());\n"             // 3: flagged
        "auto d = system_clock::now() .time_since_epoch();\n"                         // 4: wall
        "// steady_clock::now().time_since_epoch() in a comment\n"                    // 5: prose
        "const char* e = \"steady_clock::now().time_since_epoch()\";\n"               // 6: a string
        "auto f = std::chrono::high_resolution_clock::now().time_since_epoch();\n"   // 7: flagged
        "auto g = my_system_clock::now().time_since_epoch();\n";                     // 8: flagged
    std::vector<size_t> flagged;
    const Scan s = scan(snippet);
    for (const Site& site : s.other) flagged.push_back(site.line);
    EXPECT_EQ(s.wall, 2u);
    EXPECT_EQ(flagged, (std::vector<size_t>{2, 3, 7, 8}));
}
