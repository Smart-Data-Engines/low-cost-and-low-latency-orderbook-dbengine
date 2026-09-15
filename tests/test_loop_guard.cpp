// One iteration of a loop that must not end on its first exception — roadmap #131.
//
// `LoopGuard` is the mechanism for the seven loops #112 left uncovered, and it is one mechanism
// rather than seven copies for `LogEpisode`'s reason: two copies of "have I already said this"
// drift apart, and then the two log shapes an operator greps for disagree.
//
// **What this file pins, and why each half needs pinning.** The counter, because an increment that
// goes nowhere is #117's whole subject. The episode, because loud-once is the difference between a
// line an operator reads and a line an operator filters. The null registry, because two of the
// seven run in somebody else's process and have no registry to write to — a guard that assumed one
// would crash exactly where the library is least welcome to. And the *shape* of the recovery: `ok()`
// is unreachable from an iteration that threw, which is what the mesh io loop paid for with a log
// alternating ERROR / "handled again" for three failing records.
//
// What it does not pin: the text of any line. That is deliberate — the mesh version of a test like
// this anchored on a log phrase, so rewording the message failed the test and killed that item's
// mutation control (#128). The guard's own arguments name the loop; the wording is prose.

#include "orderbook/loop_guard.hpp"

#include <gtest/gtest.h>

#include <stdexcept>

namespace {

constexpr const char* kCounter = "ob_loop_errors_total";

}  // namespace

TEST(LoopGuard, ItCountsEveryIterationThatThrew) {
    ob::MetricsRegistry registry;
    ob::LoopGuard guard{"test", "a probe pass", &registry};

    EXPECT_EQ(registry.counter_value(kCounter), 0u);
    guard.caught(std::runtime_error("one"));
    guard.caught(std::runtime_error("two"));
    guard.caught(std::runtime_error("three"));

    // Every iteration, not every episode. Three records a full disk refused is three facts; the
    // *log* is what goes quiet after the first, and the counter is the thing to alarm on.
    EXPECT_EQ(registry.counter_value(kCounter), 3u);
}

TEST(LoopGuard, TheEpisodeCountsConsecutiveFailuresAndTheRecoveryClearsIt) {
    ob::MetricsRegistry registry;
    ob::LoopGuard guard{"test", "a probe pass", &registry};

    EXPECT_EQ(guard.consecutive(), 0u);
    guard.caught(std::runtime_error("one"));
    EXPECT_EQ(guard.consecutive(), 1u);
    guard.caught(std::runtime_error("two"));
    EXPECT_EQ(guard.consecutive(), 2u);

    guard.ok();
    EXPECT_EQ(guard.consecutive(), 0u) << "the episode did not close, so the next failure would "
                                          "be reported as a continuation of one that ended";

    // And it opens again rather than staying closed: a condition that comes back is a new episode
    // and gets its own loud line.
    guard.caught(std::runtime_error("again"));
    EXPECT_EQ(guard.consecutive(), 1u);
}

TEST(LoopGuard, AnIterationThatSucceededTwiceRunningDoesNotKeepAnnouncingIt) {
    ob::MetricsRegistry registry;
    ob::LoopGuard guard{"test", "a probe pass", &registry};

    // The observable half of "the recovery line fires once": `ok()` on a guard with no open
    // episode leaves nothing to report, and an idle loop calls it on every iteration. Without
    // this, a healthy loop polling every 100 ms would announce its own health ten times a second.
    guard.ok();
    guard.ok();
    EXPECT_EQ(guard.consecutive(), 0u);
    EXPECT_EQ(registry.counter_value(kCounter), 0u)
        << "a successful iteration incremented the failure counter";
}

TEST(LoopGuard, ANullRegistryIsALoopInSomebodyElsesProcess) {
    // `ShardRouter` and `OrderbookPool` are client-side: the process that owns a metrics registry
    // is the server, and there is nothing for them to write to. The guard takes a null pointer
    // rather than growing a second code path, and the log is the whole report there.
    ob::LoopGuard guard{"test", "a probe pass", nullptr};

    guard.caught(std::runtime_error("no registry here"));
    EXPECT_EQ(guard.consecutive(), 1u) << "the episode has to work without a registry, or the two "
                                          "client-side loops get no loud-once behaviour at all";
    guard.ok();
    EXPECT_EQ(guard.consecutive(), 0u);
}

TEST(LoopGuard, TheCounterItFeedsIsRegistered) {
    // An increment on an unregistered name is dropped in silence — that was #77, five dead gauges
    // serving a flat zero while the engine worked. `scripts/check_metrics.py` holds this in both
    // directions for the whole engine; this asserts it for the one name this header writes, so the
    // header can be read on its own.
    ob::MetricsRegistry registry;
    ob::LoopGuard guard{"test", "a probe pass", &registry};
    guard.caught(std::runtime_error("boom"));

    const std::string text = registry.serialize();
    EXPECT_NE(text.find(kCounter), std::string::npos)
        << "the counter LoopGuard writes is not in the registry's output, so it was dropped";
}
