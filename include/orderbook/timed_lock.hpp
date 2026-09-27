#pragma once

// How long a thread waited for a mutex and then held it, said when either is long enough to hold a
// writer up (#186).
//
// Every client write takes the engine's lock. A section that keeps it for 600 ms is a write that is
// answered 600 ms late - measured on a mesh node of 4 000 symbols, now and then, with the WAL and
// segment syncs on and off alike - and nothing said which section it was. The sections that take
// the lock while writers wait name themselves here, and a hold or a wait of kSlowLockMs or more is
// logged with the name.

#include "orderbook/logger.hpp"

#include <chrono>
#include <mutex>

namespace ob {

/// A hold or a wait of this long is one a writer notices: the median write takes 0.05 ms.
inline constexpr double kSlowLockMs = 20.0;
/// A flush tick this long has let the pending queue fill at the write ceiling (#186).
inline constexpr double kSlowTickMs = 1000.0;

class TimedLock {
public:
    using Clock = std::chrono::steady_clock;

    /// Acquires `m`; `what` names the section, for the line a slow wait or hold is said with.
    TimedLock(std::mutex& m, const char* what)
        : what_(what), asked_(Clock::now()), lock_(m), held_(Clock::now()) {}
    ~TimedLock() { unlock(); }
    TimedLock(const TimedLock&) = delete;
    TimedLock& operator=(const TimedLock&) = delete;

    /// The lock itself, for a condition variable. Time it spends released inside a wait counts as
    /// held here, so a section that waits on one names the wait in `what`.
    std::unique_lock<std::mutex>& lock() { return lock_; }

    /// Release now rather than at the end of the scope, and say so if it was slow.
    void unlock() {
        if (reported_) return;
        reported_ = true;
        const auto released = Clock::now();
        if (lock_.owns_lock()) lock_.unlock();
        const double waited = std::chrono::duration<double, std::milli>(held_ - asked_).count();
        const double held   = std::chrono::duration<double, std::milli>(released - held_).count();
        if (waited >= kSlowLockMs || held >= kSlowLockMs) {
            OB_LOG_WARN("engine", "%s: waited %.1f ms for the engine lock and held it %.1f ms",
                        what_, waited, held);
        }
    }

private:
    const char*                  what_;
    Clock::time_point            asked_;
    std::unique_lock<std::mutex> lock_;
    Clock::time_point            held_;
    bool                         reported_{false};
};

}  // namespace ob
