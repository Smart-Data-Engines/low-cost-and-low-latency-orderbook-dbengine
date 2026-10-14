#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>

namespace ob {

/// How much WAL may wait for a sync before a writer waits for one (#190, step 9).
///
/// A writer waits for room when the pending queue is full, and room comes back when the flush tick
/// has synced the WAL holding the queue's rows and drained them. A full queue is a million rows -
/// about 34 MB of WAL at ten levels a record - and before anything has measured the device, the
/// writers fill it at full speed: on the m9g.xlarge with the volume capped at 7 MB/s the sync that
/// followed took 4.9 s, and at 5 MB/s writes were refused at the five-second deadline
/// (evidence/2026-10-14-write-ceiling-slower/). Admission (`AdmissionController`) paces writes from
/// the first slow tick on, and cannot take back what was written before it.
///
/// So the WAL waiting for a sync is bounded by the time its sync would take: a budget in bytes,
/// set from the syncs themselves. It starts small, because before the first sync nothing is known;
/// a quick sync that used at least half the budget doubles it, up to a ceiling above the queue's
/// own; a slow one sets it to what the device writes in `target` at the rate that sync measured.
/// **A sync that used less than half does not raise it**: light traffic says nothing about what the
/// device takes, and a budget grown in a quiet hour would hand the next burst the whole queue - the
/// rule TCP's congestion window learned as RFC 7661.
///
/// Thread-safe: `on_sync()` from whoever completed a sync, `bytes()` from a writer deciding whether
/// to wait - one relaxed load.
class SyncBudget {
public:
    using Clock = std::chrono::steady_clock;

    struct Config {
        /// Before the first sync: 0.8 s of a 5 MB/s device.
        uint64_t initial_bytes{4ull << 20};
        /// A bad measurement cannot bring it lower, so that it cannot stop writes.
        uint64_t floor_bytes{1ull << 20};
        /// Above a full pending queue in every record shape (~34 MB at ten levels a record, ~62 MB
        /// at one), so on a device that keeps up the queue's own ceiling is the one that binds.
        uint64_t ceiling_bytes{256ull << 20};
        /// What a sync of the whole budget should take: far below the five seconds after which a
        /// write that finds no room is refused, so that a rate measured two or three times too high
        /// still does not reach them.
        Clock::duration target{std::chrono::seconds(1)};
    };

    /// What a sync changed, for the engine to say.
    enum class Change { None, Lowered, Raised };

    explicit SyncBudget(Config config);

    /// After a sync that newly covered `bytes` in `took`. A failed sync is not reported.
    Change on_sync(uint64_t bytes, Clock::duration took);

    /// The budget in force.
    uint64_t bytes() const { return bytes_.load(std::memory_order_relaxed); }

private:
    const Config config_;
    std::atomic<uint64_t> bytes_;
    /// Serialises `on_sync()`: the flush tick, FLUSH and a snapshot's cut each complete syncs.
    std::mutex mtx_;
};

} // namespace ob
