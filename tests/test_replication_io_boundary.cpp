// What one exception costs the replication io loop — roadmap #112, the last of its four loops.
//
// The outer thread boundary (`run_thread_body`) stops an escaping exception from ending the
// process. It does not stop it from ending the *thread*, and a `ReplicationManager` whose run loop
// is gone stops accepting replicas, stops advancing every catch-up and stops sending heartbeats
// while the node answers clients normally — the "guarantee absent in production" shape this item
// measured twice, on the mesh loop and on the failover monitor.
//
// Two boundaries rather than one, and the two halves of this file are the two reasons.
//
// **Per event**, because every replica registration is `EPOLLIN | EPOLLOUT | EPOLLET`: an event the
// loop abandons is not re-delivered, so taking the pass down would strand whatever the other
// descriptors of a batch of 32 had ready. **Per pass**, because this loop — unlike the mesh one —
// does real work outside the dispatch, and an exception from the snapshot poll, the replica gauges,
// a catch-up cursor or the heartbeat would step straight over a per-event boundary.
//
// And the pacing, which is the part worth measuring rather than reading: the boundary *creates* a
// hazard that did not exist while an exception took the thread with it. `wait_ms` outlives one
// pass, so a pass that throws before reaching the recompute keeps the previous value — zero,
// whenever a catch-up had queue space. A throwing pass at a zero timeout is a spin at the cost of
// a core. `replication_wait_ms()` is the whole answer and this file pins both its cases and the
// rule that nothing assigns `wait_ms` any other way.

#include "orderbook/replication.hpp"

#include <gtest/gtest.h>

#include <fstream>
#include <regex>
#include <sstream>
#include <string>

namespace {

std::string read_file(const std::string& path) {
    std::ifstream in(path);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

/// The body of `ReplicationManager::run_loop()`, from its definition to the next definition.
///
/// Sliced rather than searched over the whole file because every assertion below is about *this*
/// loop, and `ReplicationClient::run_loop()` in the same file already has a boundary of its own —
/// a check that accepted either one would pass on the wrong function.
std::string run_loop_body(const std::string& src) {
    const std::size_t from = src.find("void ReplicationManager::run_loop() {");
    if (from == std::string::npos) return {};
    const std::size_t to = src.find("\nvoid ReplicationManager::accept_replica()", from);
    if (to == std::string::npos) return {};
    return src.substr(from, to - from);
}

}  // namespace

TEST(ReplicationWait, ZeroOnlyWhileACatchupCanProgress) {
    // The two cases, exhaustively: the argument is a bool, so this is the whole function.
    EXPECT_EQ(ob::replication_wait_ms(true), 0);
    EXPECT_EQ(ob::replication_wait_ms(false), ob::kReplicationIdleWaitMs);

    // The floor is not a threshold chosen for the failure path. It is what this loop already waits
    // when it has nothing in hand, which is why a failing pass can be paced like an idle one
    // without anybody picking a number: 100 ms, ten passes a second.
    EXPECT_EQ(ob::kReplicationIdleWaitMs, 100);

    // And it must be positive, because that is the property the spin turns on. A zero floor would
    // satisfy every assertion above about "the idle wait" and reintroduce the busy loop.
    EXPECT_GT(ob::kReplicationIdleWaitMs, 0);
}

TEST(ReplicationIoBoundary, NothingPacesTheRunLoopExceptThatFunction) {
    const std::string src = read_file(std::string(OB_SOURCE_DIR) + "/src/replication.cpp");
    ASSERT_FALSE(src.empty()) << "could not read src/replication.cpp; the check would pass by "
                                 "finding nothing";
    const std::string body = run_loop_body(src);
    ASSERT_FALSE(body.empty()) << "could not find ReplicationManager::run_loop() in "
                                  "src/replication.cpp - the anchors this test slices on have "
                                  "moved, so it is asserting about nothing";

    // Every assignment to `wait_ms`, wherever it is, has to come from the one function. A literal
    // here is how the failure path and the idle path come to disagree, and the failure path is the
    // one no test drives.
    const std::regex assign(R"(wait_ms\s*=\s*([^;]+);)");
    int found = 0;
    for (auto it = std::sregex_iterator(body.begin(), body.end(), assign);
         it != std::sregex_iterator(); ++it) {
        ++found;
        const std::string rhs = (*it)[1].str();
        EXPECT_NE(rhs.find("replication_wait_ms("), std::string::npos)
            << "run_loop() assigns wait_ms = " << rhs << ", not through replication_wait_ms(). "
            << "Two places deciding one timeout is how the throwing path keeps a zero it inherited "
            << "from the pass before it (#112)";
    }

    // Three: the declaration, the recompute at the end of a pass, and the floor in the catch. A
    // count rather than "at least one", because a rule that stopped matching would report a clean
    // tree - and losing the one in the catch is exactly the regression this test exists for.
    EXPECT_EQ(found, 3) << "expected three assignments to wait_ms in run_loop(); found " << found;
}

TEST(ReplicationIoBoundary, BothBoundariesAreThereAndTheEventOneIsInsideTheDispatch) {
    const std::string src = read_file(std::string(OB_SOURCE_DIR) + "/src/replication.cpp");
    ASSERT_FALSE(src.empty());
    const std::string body = run_loop_body(src);
    ASSERT_FALSE(body.empty());

    // Anchored on the code rather than on a log phrase. The mesh version of this test was anchored
    // on the branch's message, so rewording the message failed the test - and that killed the
    // control in that item's mutation table (#128).
    const std::size_t dispatch = body.find("for (int i = 0; i < nfds; ++i) {");
    ASSERT_NE(dispatch, std::string::npos) << "the event dispatch loop has moved";

    const std::size_t tail = body.find("// Advance every catch-up that has room");
    ASSERT_NE(tail, std::string::npos) << "the per-pass tail anchor has moved";
    ASSERT_LT(dispatch, tail);

    // A `try` between the dispatch's first line and the per-pass tail: that is the per-event
    // boundary, and being *inside* the loop is the whole point of it under EPOLLET.
    const std::size_t event_try = body.find("try {", dispatch);
    EXPECT_NE(event_try, std::string::npos);
    EXPECT_LT(event_try, tail) << "there is no try inside the event dispatch loop. A boundary "
                                  "around the pass instead would abandon the rest of a batch of "
                                  "32 edge-triggered events, which are not re-delivered (#112)";

    // And one before it, which is the pass's own.
    const std::size_t pass_try = body.find("try {");
    EXPECT_NE(pass_try, std::string::npos);
    EXPECT_LT(pass_try, dispatch) << "the pass has no boundary of its own, so an exception from "
                                     "the snapshot poll, the gauges, a catch-up cursor or the "
                                     "heartbeat would step over the per-event one";

    // Both report through the same place. Two copies of "have I already said this" are how one of
    // them learns to count the episode and the other does not.
    const std::regex reports(R"(note_io_error\()");
    const auto begin = std::sregex_iterator(body.begin(), body.end(), reports);
    EXPECT_EQ(std::distance(begin, std::sregex_iterator()), 2)
        << "expected exactly two note_io_error() call sites in run_loop(), one per boundary";

    // The recovery line must not be reachable from a pass that threw - the mistake the mesh
    // boundary made one commit earlier, where `end()` ran after the loop that opened the episode
    // and the log alternated ERROR / "handled again" for three failing records.
    EXPECT_NE(body.find("!threw_this_pass"), std::string::npos)
        << "the episode's closing line is not gated on a pass that did not throw";
}
