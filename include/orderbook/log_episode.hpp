#pragma once

#include <cstdint>

namespace ob {

/// One condition, logged once when it starts and once when it ends, however long it lasts.
///
/// Written for #116, where a node that could not reach its coordinator wrote the same two WARN
/// lines every second for the length of the outage — 2.17 and 2.23 lines per second per node,
/// measured — and extracted here for #120, where the hybrid logical clock did the same thing once
/// per **write** rather than once per second. A permanent condition retried at loop frequency and
/// logged at loop frequency is #95's shape; this is the answer to it.
///
/// It lives in its own header because the second user is what makes a pattern into a mechanism.
/// Two copies of "have I already said this" drift apart — one of them learns to report the
/// duration, the other does not — and then the two log shapes an operator greps for disagree.
///
/// Not thread-safe by itself, deliberately: every user so far already holds a lock over the
/// decision that consults it, and an atomic here would buy nothing while suggesting it is safe to
/// call from two threads, which the `begin()`/`end()` pairing is not.
struct LogEpisode {
    /// Note the condition holding on this tick. True only on the tick that opens an episode,
    /// which is the tick that should log loudly.
    bool begin() {
        ++ticks_;
        return ticks_ == 1;
    }

    /// Note the condition absent. Returns how many ticks the episode lasted, or 0 if none was
    /// open — so `if (const uint64_t n = e.end())` is both the test and the number to report.
    uint64_t end() {
        const uint64_t held = ticks_;
        ticks_ = 0;
        return held;
    }

    /// How long the open episode has been running, 0 when none is. For a caller that wants to
    /// report progress without ending the episode.
    uint64_t ticks() const { return ticks_; }

private:
    uint64_t ticks_{0};
};

} // namespace ob
