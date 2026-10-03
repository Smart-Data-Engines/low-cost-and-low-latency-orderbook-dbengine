#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>

namespace ob {

/// Writes admitted at the rate the device takes (#190, step 5).
///
/// A flush tick that took much longer than its interval is the device saying how many rows it
/// takes in how long: the tick synced the WAL covering them, drained and sealed them, and the next
/// tick could not start before it ended. From such a tick on, a batch of writes waits - after it is
/// written, outside the engine's lock - for the time its rows take at that rate, so that an ingest
/// faster than the device becomes slower writes, rather than writers stopped at a full pending
/// queue and refused after five seconds. Measured on EC2 with the volume's writes capped at 30 MB/s:
/// the node refused writes in every round (evidence/2026-10-03-write-ceiling-ec2/).
///
/// **The queue's occupancy is deliberately not the signal.** The prototype that admitted from it
/// (`f8c04ad`) removed those refusals and halved the throughput of a device that keeps up: at
/// 7.7 M levels/s the queue passes half between two ticks of a fast device. A slow tick is the
/// device being behind; a full queue can be the ingest being fast.
///
/// The rate is a control loop, AIMD-shaped. Admission begins at half what the first slow tick
/// measured (#190, step 7): that tick's rate counts the WAL's share of the device and not the
/// segments it sealed, whose sync runs behind it, so on the m9g.xlarge it measured 1.5-2 times what
/// the device then sustained, and the queue filled again before the next tick drained it - the
/// worst batch of every overload. A later slow tick sets the rate to what it measured, if that is
/// lower; a tick that fits its interval raises it by a step, because the device had room, so from
/// half the rate climbs to what the device takes rather than the queue finding it from above; and
/// admission ends only once it has stopped delaying anyone - a whole run of ticks in which no batch
/// waited - since ending it while writers are still being paced would let them refill the queue and
/// cost the stall it exists to remove.
///
/// Thread-safe: `admit()` from any writer, `on_tick()` from the flush thread. When admission is
/// off - every deployment whose device keeps up - `admit()` is one relaxed load.
class AdmissionController {
public:
    using Clock = std::chrono::steady_clock;

    struct Config {
        /// The flush interval; a tick is slow when it took `slow_factor` times this or more.
        Clock::duration interval{std::chrono::milliseconds(100)};
        double slow_factor{2.0};
        /// What one batch may be made to wait, far below the five seconds after which a write that
        /// finds no room is refused.
        Clock::duration max_delay{std::chrono::milliseconds(500)};
        /// Credit a writer below the rate builds up, so that such a writer is not delayed at all -
        /// without it every batch would wait at least its own rows' time, and "nobody waited", the
        /// signal that ends admission, could never be seen.
        Clock::duration burst{std::chrono::milliseconds(100)};
        /// The rate's step up after a tick that was not slow.
        double raise{1.1};
        /// What admission begins at, as a share of the rate the first slow tick measured.
        double begin_at{0.5};
        /// Ticks in a row with no batch delayed after which admission ends.
        uint32_t idle_ticks_to_end{10};
        /// Rows a second the rate never goes below, so that one bad measurement cannot stop writes.
        double floor_rows_per_s{1000.0};
    };

    /// What a tick changed, for the engine to say once.
    enum class Change { None, Began, Ended };

    explicit AdmissionController(Config config);

    /// The defaults, measured against a flush tick of `interval`.
    static Config for_interval(Clock::duration interval) {
        Config c;
        c.interval = interval;
        c.burst = interval;
        return c;
    }

    /// `--write-admission on|off`. Turning it off ends admission at once: the next batch waits for
    /// nothing.
    void set_enabled(bool enabled);
    bool enabled() const { return enabled_.load(std::memory_order_relaxed); }

    /// After a flush tick that took `rows` from the pending queue in `took`.
    Change on_tick(uint64_t rows, Clock::duration took);

    /// How long a batch of `rows` just written waits before its writer goes on. Zero when admission
    /// is off.
    Clock::duration admit(uint64_t rows, Clock::time_point now);
    Clock::duration admit(uint64_t rows) { return admit(rows, Clock::now()); }

    /// Rows a second admission takes writes at; 0 while it is off.
    double rate() const;
    bool active() const { return active_.load(std::memory_order_relaxed); }

    /// Since admission last began: batches delayed and the time they waited in all.
    uint64_t delayed_batches() const;
    Clock::duration delayed_total() const;

    /// The rate the slow tick that began it measured, and that tick's own numbers, for the log.
    struct Measurement {
        uint64_t rows{0};
        Clock::duration took{};
        double rows_per_s{0.0};
    };
    Measurement last_slow_tick() const;

private:
    const Config config_;
    std::atomic<bool> enabled_{true};
    std::atomic<bool> active_{false};

    mutable std::mutex mtx_;
    double rate_{0.0};
    Clock::time_point next_free_{};
    uint32_t idle_ticks_{0};
    uint64_t delayed_since_tick_{0};
    uint64_t delayed_batches_{0};
    Clock::duration delayed_total_{};
    Measurement last_slow_{};
};

} // namespace ob
