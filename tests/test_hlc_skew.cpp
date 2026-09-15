// Clock skew against the hybrid logical clock: a peer whose clock is ahead of ours, a peer whose
// clock is behind, the drift boundary, and the one property this class exists to provide — that
// the clock never goes backwards.
//
// Feature: fault-injection, stage D of roadmap #54.
//
// Why these tests are here rather than in test_hlc.cpp: that file proves the algorithm against
// timestamps a well-behaved cluster produces. This one presents the timestamps a broken cluster
// produces, which is a different question and the one #54 asks. Two defects came out of writing
// it, #119 and #120, and the assertions below are on the fixed behaviour.
//
// No clock injection. The half of the input that comes off the wire is already an argument —
// `tick_receive(remote)` — and it is the half a peer controls, so the skew that matters is
// expressible without making the wall clock a parameter of production code.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <ctime>
#include <thread>
#include <vector>

#include "orderbook/conflict_resolver.hpp"
#include "orderbook/hlc.hpp"

namespace {

constexpr uint64_t kSecond = 1'000'000'000ULL;


/// How many local ticks it takes for the 16-bit logical counter to come back to where it started.
/// Spelled as the arithmetic rather than as 65536 so the reason is visible at the call site.
constexpr long kLogicalPeriod = static_cast<long>(UINT16_MAX) + 1;

} // namespace

// ── The property the whole class exists for ──────────────────────────────────

// The control, and it has to come first: a clock nobody has skewed must not regress over the same
// number of ticks the skewed test uses. Without it, a green run of the test below cannot be told
// apart from a probe that is not looking.
TEST(HLCSkew, AnUnskewedClockDoesNotRegressOverTheLogicalPeriod) {
    ob::HybridLogicalClock hlc(1);
    ob::HLCTimestamp prev = hlc.tick_local();
    long regressions = 0;
    for (long i = 0; i < 3 * kLogicalPeriod; ++i) {
        ob::HLCTimestamp now = hlc.tick_local();
        if (now < prev) ++regressions;
        prev = now;
    }
    EXPECT_EQ(regressions, 0) << "an unskewed clock regressed, so the test below proves nothing";
}

// #119. With the physical component pinned above the wall clock, every local tick increments the
// logical counter, and the counter is 16 bits on the wire. Before the fix this wrapped to zero on
// tick 65 533 of a 200 000-tick run and the timestamp went **backwards** — measured, three
// regressions per 200 000 ticks, exactly one per period.
//
// Pinning is not hypothetical: a remote timestamp ahead of our clock does it (#120), and at this
// engine's published native ingestion rate a pinned window of 100 ms is some 135 000 ticks.
TEST(HLCSkew, TheClockDoesNotRegressWhenTheLogicalCounterOverflows) {
    ob::HybridLogicalClock hlc(1);

    // Pin the physical component an hour ahead, which is what a peer with a wrong clock does.
    hlc.tick_receive(ob::HLCTimestamp{ob::wall_clock_ns() + 3600 * kSecond, 0, 2});

    ob::HLCTimestamp prev = hlc.tick_local();
    const uint64_t pinned_physical = prev.physical_ns;
    long regressions = 0;
    for (long i = 0; i < 3 * kLogicalPeriod; ++i) {
        ob::HLCTimestamp now = hlc.tick_local();
        if (now < prev) ++regressions;
        prev = now;
    }

    EXPECT_EQ(regressions, 0);
    // And the carry went where it was supposed to go: three periods of ticks cost three
    // nanoseconds of physical time, not a reset counter.
    EXPECT_GE(prev.physical_ns, pinned_physical)
        << "the physical component must absorb the overflow, never be left behind by it";
}

// The same statement one layer down, so a failure says which half broke: crossing the period
// boundary must produce a strictly greater timestamp, not an equal one. Saturating the counter
// would keep the test above green and silently stop breaking ties.
TEST(HLCSkew, CrossingTheLogicalPeriodProducesAStrictlyGreaterTimestamp) {
    ob::HybridLogicalClock hlc(1);
    hlc.tick_receive(ob::HLCTimestamp{ob::wall_clock_ns() + 3600 * kSecond, 0, 2});

    ob::HLCTimestamp prev = hlc.tick_local();
    for (long i = 0; i < kLogicalPeriod + 8; ++i) {
        ob::HLCTimestamp now = hlc.tick_local();
        ASSERT_LT(prev, now) << "tick " << i << " of the period was not strictly greater";
        prev = now;
    }
}

// ── A peer whose clock is behind ours ────────────────────────────────────────

// The control on the other side. A ceiling on how far a remote may move us forward must not turn
// into a clock that follows a peer backwards: that would break the same property from the other
// direction, and it is the mistake a clamp written in the wrong place makes.
TEST(HLCSkew, ARemoteTimestampAnHourBehindDoesNotMoveTheClockBack) {
    ob::HybridLogicalClock hlc(1);
    const ob::HLCTimestamp before = hlc.tick_local();

    const ob::HLCTimestamp after =
        hlc.tick_receive(ob::HLCTimestamp{ob::wall_clock_ns() - 3600 * kSecond, 0, 2});

    EXPECT_GE(after, before);
    EXPECT_GE(after.physical_ns, before.physical_ns);
}

// A peer far behind must not drag the *logical* counter either: its logical belongs to a physical
// nanosecond we are nowhere near, so treating it as a tie-break would import a number with no
// meaning here.
TEST(HLCSkew, ARemoteTimestampBehindUsDoesNotImportItsLogicalCounter) {
    ob::HybridLogicalClock hlc(1);
    hlc.tick_local();

    const ob::HLCTimestamp after =
        hlc.tick_receive(ob::HLCTimestamp{ob::wall_clock_ns() - 3600 * kSecond, 60000, 2});

    EXPECT_LT(after.logical, 60000)
        << "a logical counter from a physical nanosecond we are not in was adopted";
}

// ── The bound itself, with no clock, no socket and no peer (#121) ────────────

// `remote_clock_is_plausible` is the whole policy, so it is tested as arithmetic. Five cases, and
// the two at the boundary are the ones that matter: a rule tested only far from its edge is a rule
// whose edge nobody has read.
TEST(HLCSkew, TheBoundAcceptsUpToItAndRefusesPastIt) {
    const uint64_t now = 1'700'000'000'000'000'000ULL;   // a fixed "now"; no clock is read here

    EXPECT_TRUE(ob::remote_clock_is_plausible(now, now)) << "a peer agreeing with us is refused";
    EXPECT_TRUE(ob::remote_clock_is_plausible(now + ob::MM_MAX_CLOCK_SKEW_NS, now))
        << "the bound is exclusive, so a peer exactly at it is refused - the boundary has to belong "
           "to one side and the accepting side is the one that keeps a working cluster working";
    EXPECT_FALSE(ob::remote_clock_is_plausible(now + ob::MM_MAX_CLOCK_SKEW_NS + 1, now))
        << "one nanosecond past the bound is accepted, so the bound is not a bound";
}

// Behind us needs no rule and must not get one: `tick_receive` takes a maximum, so a peer behind is
// ignored by arithmetic. A node with a dead RTC comes up in 1970 and is harmless; refusing it would
// cost its data for nothing.
TEST(HLCSkew, APeerBehindUsIsAlwaysPlausibleHoweverFarBehind) {
    const uint64_t now = 1'700'000'000'000'000'000ULL;
    EXPECT_TRUE(ob::remote_clock_is_plausible(now - 3600 * kSecond, now));
    EXPECT_TRUE(ob::remote_clock_is_plausible(0, now)) << "the epoch itself is refused";
}

// The tail this bound exists to make unreachable. `resolve_logical` carries a logical overflow into
// the physical component and cannot carry past UINT64_MAX; at that value the next tick returns
// logical 0 after 65535, so the clock goes **backwards** by the whole counter and then oscillates
// there. The comment in that function used to claim saturating "keeps this function's promise" — it
// does not. Making the state unreachable is the fix, and this is the assertion that it is.
TEST(HLCSkew, AnAbsurdRemoteValueIsRefusedSoTheCarryCanNeverSaturate) {
    const uint64_t now = 1'700'000'000'000'000'000ULL;
    EXPECT_FALSE(ob::remote_clock_is_plausible(UINT64_MAX, now));
    EXPECT_FALSE(ob::remote_clock_is_plausible(now + 365ULL * 24 * 3600 * kSecond, now))
        << "a year ahead is accepted, which is the class of wrong clock - a hand-set date, a dead "
           "RTC - that the bound is drawn to exclude";
}

// ── A peer whose clock is ahead: what the engine does, and what it says about it ──

// The clock itself is **still uncapped, and that is the decision rather than the absence of one**
// (#121). `tick_receive` takes `max({now, last_, remote})` with no ceiling, so a value an hour
// ahead handed straight to this class moves it an hour ahead and nothing moves it back — which is
// exactly what this test requires, because the bound lives one layer out.
//
// Why there and not here. A clock that silently refused part of what it was told would break the
// one invariant it exists for: if we accept a record we must stamp our later writes above it, or a
// causally later write can lose an LWW conflict to the record it followed. So the layer that can
// say no is the layer that can also decline the **record** — and, since a record refused while the
// link stays up is a silent hole, the layer that can decline the *peer*. That is
// `MultiMasterManager::drop_peer_if_clock_is_implausible()`, and `tests/test_mm_wire_clock.cpp`
// holds both halves of it: an hour is refused, a minute is absorbed.
//
// With #119 fixed, what absorption costs is being wrong about real time rather than being
// incorrect: every node that receives such a record adopts the same value, timestamps stay
// monotonic, and LWW still converges. The bound is not there to make the clock a time; it is there
// because the mesh's clock is the **maximum** of its members' clocks and nothing bounded the
// maximum.
TEST(HLCSkew, ARemoteTimestampAheadOfUsIsAdoptedAndKept) {
    ob::HybridLogicalClock hlc(1);
    const uint64_t ahead = ob::wall_clock_ns() + 3600 * kSecond;

    hlc.tick_receive(ob::HLCTimestamp{ahead, 0, 2});
    EXPECT_GE(hlc.current().physical_ns, ahead);

    // Not a transient: the next twenty local ticks still carry it, with the wall clock far below.
    for (int i = 0; i < 20; ++i) hlc.tick_local();
    EXPECT_GE(hlc.current().physical_ns, ahead);
    EXPECT_GT(hlc.current().physical_ns, ob::wall_clock_ns());
}

// The observation half. `max_drift_ns()` is the distance between the HLC's physical component and
// the wall clock, and it is what feeds ob_mm_hlc_drift_ns.
TEST(HLCSkew, DriftIsObservedOnBothSidesOfTheOneSecondBoundary) {
    // Below the boundary: a peer half a second ahead is ordinary skew and must still be measured,
    // because a gauge that only moves once something is already wrong cannot show it approaching.
    {
        ob::HybridLogicalClock hlc(1);
        hlc.tick_receive(ob::HLCTimestamp{ob::wall_clock_ns() + kSecond / 2, 0, 2});
        const int64_t drift = hlc.max_drift_ns();
        EXPECT_GT(drift, 0);
        EXPECT_LT(drift, static_cast<int64_t>(kSecond));
    }
    // Over it.
    {
        ob::HybridLogicalClock hlc(1);
        hlc.tick_receive(ob::HLCTimestamp{ob::wall_clock_ns() + 10 * kSecond, 0, 2});
        EXPECT_GT(hlc.max_drift_ns(), static_cast<int64_t>(kSecond));
    }
}

// #120. The drift warning used to be emitted on every tick, so a clock that is off produced one
// WARN per **write** — measured at 200 002 lines for 200 000 ticks, 22.9 MB from one probe run.
// This is #95's shape (a permanent condition logged at loop frequency) at write frequency, and
// #116's fix applied at a different site: say it once per episode, and count every occurrence,
// because the count is the half that can be alerted on and the log is the half a human reads.
//
// The count is asserted here rather than the log: C++ tests have no log sink to read, and the
// integration battery is where the engine's log shape is checked. A counter is also the better
// contract — it is what ob_mm_hlc_drift_episodes_total publishes.
TEST(HLCSkew, EveryDriftExcursionIsCountedNotJustTheFirst) {
    ob::HybridLogicalClock hlc(1);
    EXPECT_EQ(hlc.drift_excursions(), 0u);

    hlc.tick_receive(ob::HLCTimestamp{ob::wall_clock_ns() + 10 * kSecond, 0, 2});
    const uint64_t after_first = hlc.drift_excursions();
    EXPECT_GT(after_first, 0u);

    // Further local ticks are still over the boundary, and each one is an occurrence: the episode
    // is what the log collapses, never what the counter collapses.
    for (int i = 0; i < 50; ++i) hlc.tick_local();
    EXPECT_GT(hlc.drift_excursions(), after_first);
}

// And the counter must stay still when the clock is healthy, or it says nothing when it moves.
TEST(HLCSkew, AHealthyClockCountsNoDriftExcursions) {
    ob::HybridLogicalClock hlc(1);
    for (int i = 0; i < 1000; ++i) hlc.tick_local();
    EXPECT_EQ(hlc.drift_excursions(), 0u);
}

// A peer within the boundary is not an excursion either — the threshold has to be a threshold in
// both directions, or the counter degenerates into "a remote record arrived".
TEST(HLCSkew, SkewInsideTheBoundaryIsNotCountedAsAnExcursion) {
    ob::HybridLogicalClock hlc(1);
    hlc.tick_receive(ob::HLCTimestamp{ob::wall_clock_ns() + kSecond / 2, 0, 2});
    EXPECT_EQ(hlc.drift_excursions(), 0u);
}

// The statement #120 is actually about, in the only form a C++ test can make it: many events,
// two lines. `drift_excursions()` counts occurrences, `drift_episodes()` counts the loud lines,
// and the ratio between them is the fix. Asserting the log itself needs a sink these tests do not
// have, which is why the pair exists rather than a log matcher.
TEST(HLCSkew, AnExcursionThatLastsAThousandTicksIsOneLine) {
    ob::HybridLogicalClock hlc(1);
    hlc.tick_receive(ob::HLCTimestamp{ob::wall_clock_ns() + 10 * kSecond, 0, 2});
    for (int i = 0; i < 1000; ++i) hlc.tick_local();

    EXPECT_GT(hlc.drift_excursions(), 1000u)
        << "every tick over the boundary must be counted";
    EXPECT_EQ(hlc.drift_episodes(), 1u)
        << "the excursion never ended, so it must have produced exactly one loud line";
}

// And the episode has to be able to close, or "one line per episode" is indistinguishable from
// "one line, ever" — the failure mode of an episode whose end() is never reached.
//
// The drift is put *just* over the boundary rather than an hour over it, so the wall clock catches
// up in milliseconds instead of in the hour the poisoning case would take. It is the same close
// path either way: a tick whose drift is back inside.
TEST(HLCSkew, AnExcursionThatClearsAndReturnsIsTwoLines) {
    using namespace std::chrono_literals;
    constexpr uint64_t kJustOver = 2'000'000ULL;   // 2 ms past the boundary

    ob::HybridLogicalClock hlc(1);
    hlc.tick_receive(ob::HLCTimestamp{
        ob::wall_clock_ns() + static_cast<uint64_t>(ob::HybridLogicalClock::DRIFT_WARN_NS) + kJustOver,
        0, 2});
    ASSERT_EQ(hlc.drift_episodes(), 1u);

    std::this_thread::sleep_for(8ms);
    hlc.tick_local();
    EXPECT_EQ(hlc.drift_episodes(), 1u) << "closing an episode must not write another loud line";

    hlc.tick_receive(ob::HLCTimestamp{
        ob::wall_clock_ns() + static_cast<uint64_t>(ob::HybridLogicalClock::DRIFT_WARN_NS) + kJustOver,
        0, 2});
    EXPECT_EQ(hlc.drift_episodes(), 2u)
        << "a second excursion after the first cleared is a second line, not a suppressed one";
}

// ── D2: two nodes, opposite drift, the same conflict ────────────────────────
//
// This is the property the stage exists for. Divergence here is not a wrong number in a log: it
// is two nodes permanently disagreeing about the content of a row, with no mechanism that would
// ever notice. Anti-entropy compares what each side *holds*, so two sides each holding a
// different winner look consistent to it.

namespace {

/// One node's view: its own clock, and the resolver that decides for it.
struct NodeView {
    ob::HybridLogicalClock clock;
    ob::ConflictResolver   resolver{};

    explicit NodeView(uint16_t id) : clock(id) {}
};

ob::ConflictKey key() { return ob::ConflictKey{"BTC-USD", "BINANCE", 0, 5000000}; }

} // namespace

TEST(HLCSkew, TwoNodesWithOppositeDriftAgreeOnTheWinnerOfTheSameConflict) {
    // Node 1's clock runs ahead of real time, node 2's behind it. Both directions on purpose:
    // a rule that only looked at one side would pass a test that only skewed one side.
    NodeView one(1);
    NodeView two(2);
    one.clock.tick_receive(ob::HLCTimestamp{ob::wall_clock_ns() + 30 * kSecond, 0, 9});

    // Each node stamps its own write with its own clock. These are the two records that will meet.
    const ob::HLCTimestamp from_one = one.clock.tick_local();
    const ob::HLCTimestamp from_two = two.clock.tick_local();

    // Node 1 applied its own write first, then node 2's arrives.
    one.resolver.update_hlc(key(), from_one, 1);
    const ob::ConflictResolution on_one = one.resolver.resolve(key(), from_two, 2);

    // Node 2 applied its own write first, then node 1's arrives.
    two.resolver.update_hlc(key(), from_two, 2);
    const ob::ConflictResolution on_two = two.resolver.resolve(key(), from_one, 1);

    // The two nodes are answering mirrored questions, so agreement means opposite verdicts about
    // "the remote": exactly one of them must end up holding node 1's write. Stating it as the
    // winner rather than as the verdict is what makes the assertion readable — and a test that
    // compared the two enum values directly would demand they be *equal*, which would be the
    // definition of divergence.
    const bool one_keeps_its_own = (on_one == ob::ConflictResolution::REJECT_REMOTE);
    const bool two_takes_ones    = (on_two == ob::ConflictResolution::APPLY_REMOTE);
    EXPECT_EQ(one_keeps_its_own, two_takes_ones)
        << "the two nodes disagree about which write wins, which is permanent divergence";

    // And the winner is the node whose clock is ahead. That is LWW working as designed rather
    // than a defect — worth asserting so that a change to the tie-break has to be deliberate.
    EXPECT_TRUE(one_keeps_its_own)
        << "the write stamped by the clock that is ahead did not win";
}

TEST(HLCSkew, AgreementSurvivesTheClocksMergingInOppositeOrders) {
    // The same pair, but each node has already merged the other's timestamp — which is what
    // `apply_remote_delta` does on the way in, and it happens in a different order on each side.
    // If merging could change a verdict, the two nodes would diverge on the *third* write rather
    // than the second, which is harder to see and no less permanent.
    NodeView one(1);
    NodeView two(2);
    one.clock.tick_receive(ob::HLCTimestamp{ob::wall_clock_ns() + 30 * kSecond, 0, 9});

    const ob::HLCTimestamp from_one = one.clock.tick_local();
    const ob::HLCTimestamp from_two = two.clock.tick_local();

    one.clock.tick_receive(from_two);
    two.clock.tick_receive(from_one);

    const ob::HLCTimestamp next_one = one.clock.tick_local();
    const ob::HLCTimestamp next_two = two.clock.tick_local();

    one.resolver.update_hlc(key(), next_one, 1);
    two.resolver.update_hlc(key(), next_two, 2);

    const ob::ConflictResolution on_one = one.resolver.resolve(key(), next_two, 2);
    const ob::ConflictResolution on_two = two.resolver.resolve(key(), next_one, 1);

    const bool one_keeps_its_own = (on_one == ob::ConflictResolution::REJECT_REMOTE);
    const bool two_takes_ones    = (on_two == ob::ConflictResolution::APPLY_REMOTE);
    EXPECT_EQ(one_keeps_its_own, two_takes_ones)
        << "after merging each other's clocks the two nodes chose different winners";
}

// The control for both of the above: with no skew at all the same pair still has to reach one
// answer, decided by the node id. Without it, a resolver that always said REJECT_REMOTE would
// pass every assertion above.
TEST(HLCSkew, TwoNodesWithNoSkewStillAgreeAndTheTieBreakIsTheNodeId) {
    NodeView one(1);
    NodeView two(2);

    // Same physical nanosecond on both sides, which is the case the node id exists for.
    const uint64_t shared = ob::wall_clock_ns();
    const ob::HLCTimestamp from_one{shared, 0, 1};
    const ob::HLCTimestamp from_two{shared, 0, 2};

    one.resolver.update_hlc(key(), from_one, 1);
    two.resolver.update_hlc(key(), from_two, 2);

    const ob::ConflictResolution on_one = one.resolver.resolve(key(), from_two, 2);
    const ob::ConflictResolution on_two = two.resolver.resolve(key(), from_one, 1);

    const bool one_keeps_its_own = (on_one == ob::ConflictResolution::REJECT_REMOTE);
    const bool two_takes_ones    = (on_two == ob::ConflictResolution::APPLY_REMOTE);
    EXPECT_EQ(one_keeps_its_own, two_takes_ones);
    EXPECT_FALSE(one_keeps_its_own)
        << "with physical and logical equal the higher node id must win, on both sides";
}
