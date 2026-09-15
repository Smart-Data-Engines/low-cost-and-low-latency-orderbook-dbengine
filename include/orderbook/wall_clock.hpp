#pragma once

// ── The wall clock, once ──────────────────────────────────────────────────────
//
// Nanoseconds since the Unix epoch, from `CLOCK_REALTIME`.
//
// **This file exists because there were four copies.** `HybridLogicalClock` had it as a private
// static, `src/failover.cpp` had a free one of the same name, and two test files had their own —
// written two different ways, `clock_gettime(CLOCK_REALTIME)` in two and
// `std::chrono::system_clock::now()` in the other two. On Linux those agree, because `system_clock`
// *is* `CLOCK_REALTIME`, so this was never a defect. It was the shape that produces one: #118 had
// two fields named after the same quantity and neither held it, and four modules once each grew
// their own path to the server binary (pitfall 77). #121 needed the wall clock in a third
// production place, and a fifth copy is the wrong direction.
//
// Its own header rather than a corner of `hlc.hpp`, because `src/failover.cpp` reads the wall clock
// and has nothing to do with hybrid logical clocks — and a dependency added for a two-line helper
// is how a header comes to mean something other than its name.
//
// `inline` rather than a translation unit of its own: it is two lines, it is called once per clock
// tick, and a link-time dependency for that is a cost with no buyer.
//
// `Engine::stamp_for()` keeps its own expression, deliberately. It answers a different question —
// *whose* time a row carries, the client's or ours — and #105 gave that one function of its own for
// the same reason this one exists.

#include <cstdint>
#include <ctime>

namespace ob {

/// Nanoseconds since the Unix epoch. Never fails in a way a caller can act on: `clock_gettime`
/// with `CLOCK_REALTIME` fails only for an invalid clock id, which is a compile-time constant here.
inline uint64_t wall_clock_ns() {
    struct timespec ts{};
    ::clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL +
           static_cast<uint64_t>(ts.tv_nsec);
}

} // namespace ob
