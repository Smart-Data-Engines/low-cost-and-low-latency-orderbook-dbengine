// What the io_uring transport publishes, checked by reading it (#117).
//
// This file exists because of a gap with a precise shape. `OB_USE_IO_URING` is off by default;
// since #108 a CI job **builds** `src/io_uring_server.cpp` in Release and checks the symbol is in
// the binary, and that job's own message says compiling is all it does. Nothing runs it. So a
// number that transport publishes can be wrong for ever without a red test anywhere, which is how
// `ob_iouring_sqe_submitted` came to be fed the *completion* count — making it equal to
// `ob_iouring_cqe_processed` by construction, so an operator comparing the two to find a backlog
// compared a number with itself.
//
// Two kinds of check are possible here and both are used. The arithmetic was moved into
// `metrics.hpp` so the ordinary suite executes it (`queue_utilization_percent`,
// tests/test_metrics_registry.cpp). What cannot be moved — which expression is handed to which
// counter — is asserted against the source text below. That is weaker than running it and is the
// strongest thing available; saying so is the point.

#include <gtest/gtest.h>

#include <fstream>
#include <iterator>
#include <string>

namespace {

std::string io_uring_source() {
    std::ifstream in(std::string(OB_SOURCE_DIR) + "/src/io_uring_server.cpp");
    if (!in) return {};
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

/// What is passed *after* the metric name in a registry call whose text begins at `needle`,
/// up to the closing `;`.
///
/// The arguments and not the whole statement, because the statement contains the callee's name
/// and `increment_counter` contains the word `count`. The first version of the test below asked
/// whether the statement mentioned `count` and matched that — the same shape as a denylist that
/// finds `rds` inside `records`, committed here in the check written to catch it.
std::string arguments_after_metric_name(const std::string& src, const std::string& needle) {
    const auto at = src.find(needle);
    if (at == std::string::npos) return {};
    const auto end = src.find(';', at);
    if (end == std::string::npos) return {};
    const auto comma = src.find(',', at);
    if (comma == std::string::npos || comma > end) return {};   // no second argument at all
    return src.substr(comma + 1, end - comma - 1);
}

} // namespace

TEST(IoUringInstrumentation, TheSubmitResultIsCapturedRatherThanDiscarded) {
    const std::string src = io_uring_source();
    ASSERT_FALSE(src.empty()) << "cannot read src/io_uring_server.cpp";

    // A bare `io_uring_submit(ring_);` throws away two things: how many entries were submitted,
    // and whether any were. A negative return means the prepared entries stayed in the ring, so
    // the transport stops making progress and says nothing — #113's shape (a discarded result
    // that only matters when it is not zero) in a file no test runs.
    EXPECT_EQ(src.find("\n        io_uring_submit(ring_);"), std::string::npos)
        << "io_uring_submit's result is discarded, so a failed submit is silent";
    EXPECT_NE(src.find("= io_uring_submit(ring_)"), std::string::npos)
        << "nothing captures the number of entries submitted";
}

TEST(IoUringInstrumentation, TheTwoQueueCountersAreNotFedTheSameNumber) {
    const std::string src = io_uring_source();
    ASSERT_FALSE(src.empty());

    const std::string cqe =
        arguments_after_metric_name(src, "increment_counter(\"ob_iouring_cqe_processed\"");
    const std::string sqe =
        arguments_after_metric_name(src, "increment_counter(\"ob_iouring_sqe_submitted\"");
    ASSERT_FALSE(cqe.empty()) << "ob_iouring_cqe_processed is published with no argument, so it "
                                 "counts one per loop pass";
    ASSERT_FALSE(sqe.empty()) << "ob_iouring_sqe_submitted is published with no argument";

    EXPECT_NE(cqe.find("count"), std::string::npos)
        << "the completion counter should be fed the completion count:\n" << cqe;
    EXPECT_NE(sqe.find("submitted"), std::string::npos)
        << "the submission counter is not fed the submit result, so it carries no information "
           "the completion counter does not already carry:\n" << sqe;
    EXPECT_EQ(sqe.find("count"), std::string::npos)
        << "the submission counter is fed the completion count again:\n" << sqe;
}

TEST(IoUringInstrumentation, UtilizationIsSampledBeforeSubmittingNotAfter) {
    const std::string src = io_uring_source();
    ASSERT_FALSE(src.empty());

    const auto sample = src.find("set_gauge(\"ob_iouring_sq_utilization\"");
    const auto submit = src.find("= io_uring_submit(ring_)");
    ASSERT_NE(sample, std::string::npos) << "ob_iouring_sq_utilization is not published";
    ASSERT_NE(submit, std::string::npos);

    // Submitting empties the submission queue, so a sample taken afterwards reads zero whatever
    // the handlers queued — a gauge that is always zero and always looks healthy.
    EXPECT_LT(sample, submit)
        << "the submission queue is sampled after it has been emptied, so the gauge reads zero";
}

TEST(IoUringInstrumentation, OverflowIsCountedPerEpisodeRatherThanPerLoopPass) {
    const std::string src = io_uring_source();
    ASSERT_FALSE(src.empty());

    const auto overflow = src.find("io_uring_cq_has_overflow(ring_)");
    ASSERT_NE(overflow, std::string::npos) << "overflow is not observed at all";

    // `io_uring_cq_has_overflow()` is a flag the kernel leaves raised, not an event. A counter fed
    // from it on every pass counts this loop's speed, and the log would repeat once per pass —
    // #120's defect, which cost 200 002 lines for 200 000 ticks in the clock.
    const std::string window = src.substr(overflow, 400);
    EXPECT_NE(window.find("cq_overflow_episode_.begin()"), std::string::npos)
        << "overflow is not edge-detected, so the counter counts loop passes:\n" << window;
    EXPECT_NE(src.find("cq_overflow_episode_.end()"), std::string::npos)
        << "the episode never ends, so one overflow silences every later one";
}
