#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>

namespace ob {

/// Which of the server's client loops are serving events, now or a moment ago (#249). Each loop
/// writes a flag as a pass over its events begins and, as it ends, the time it stopped - on a cache
/// line of its own, so that no two loops write one line - and a scan that could divide among the
/// scan pool's threads reads them all to learn whether a CPU is left over.
class LoopLoad {
public:
    using Clock = std::chrono::steady_clock;

    explicit LoopLoad(size_t loops) : flags_(std::make_unique<Flag[]>(loops)), loops_(loops) {}

    /// A pass over loop `loop`'s events begins.
    void serving(size_t loop) { flags_[loop].serving.store(true, std::memory_order_relaxed); }

    /// The loop waits for events again. The clock is read only at the end of a pass, so a loop that
    /// spins between events with nothing to serve reads nothing.
    void stopped(size_t loop) {
        if (flags_[loop].serving.load(std::memory_order_relaxed)) stopped_at(loop, Clock::now());
    }
    /// The pass ended at `at`. The time is stored before the flag is cleared, and released with it,
    /// so a reader that sees the loop not serving sees when it stopped.
    void stopped_at(size_t loop, Clock::time_point at) {
        Flag& f = flags_[loop];
        f.stopped_at.store(at, std::memory_order_relaxed);
        f.serving.store(false, std::memory_order_release);
    }

    /// The loops serving at `now`, or that stopped serving less than `recent` before it, each flag and
    /// time read once: a loop between two of a connection's requests counts as the busy loop it is.
    size_t serving_within(Clock::time_point now, std::chrono::nanoseconds recent) const {
        size_t n = 0;
        for (size_t i = 0; i < loops_; ++i) {
            const Flag& f = flags_[i];
            // Compared as `stopped_at > now - recent`, not `now - stopped_at < recent`: a loop
            // that never served stopped at time_point::min(), whose distance from now overflows.
            const bool busy = f.serving.load(std::memory_order_acquire) ||
                              f.stopped_at.load(std::memory_order_relaxed) > now - recent;
            n += busy ? 1 : 0;
        }
        return n;
    }

    size_t loops() const { return loops_; }

private:
    struct alignas(64) Flag {
        std::atomic<bool> serving{false};
        /// Never, until it first stops. Not the clock's epoch: steady_clock's is the boot, and on a
        /// machine up less than the window a loop that never served counted as serving a moment
        /// ago - as a CI runner a few minutes old showed with a test's window of an hour.
        std::atomic<Clock::time_point> stopped_at{Clock::time_point::min()};
    };
    std::unique_ptr<Flag[]> flags_;
    size_t loops_;
};

}  // namespace ob
