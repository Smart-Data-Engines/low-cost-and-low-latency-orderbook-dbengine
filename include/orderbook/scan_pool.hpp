#pragma once

#include "orderbook/metrics.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace ob {

/// When a scan pool thread enters the kernel between the steps of its work (#248) - the rule alone,
/// with no clock and no syscall in it, so that it can be held to what it says.
///
/// On a kernel that preempts lazily (`CONFIG_PREEMPT_LAZY`) a client loop woken onto the CPU a pool
/// thread holds runs when that thread next enters the kernel, or at the next tick: a pool thread
/// under `SCHED_IDLE` gives way at once only on a kernel that preempts eagerly. So what a writer
/// beside a query on the pool loses follows how long a pool thread works between entries. Measured
/// on an m8a.xlarge (kernel 7.0, `HZ` 1000) beside `price_all` of 1.9 M rows
/// (`evidence/2026-10-24-scan-pool-writer/` in the workspace, runs 4 and 5): the writer's floor 80%
/// of its rate with no pool at an entry every 2 048 rows (17-18 us), 90-95% every 512 (4.4 us),
/// 94.6% every 2 us and 98.7% every microsecond. An entry costs 46-216 ns, so a thread nobody waits
/// for enters only every `kQuietNs`.
class ScanPace {
public:
    /// The interval while an entry in the last `kWindowNs` gave the CPU to another thread.
    static constexpr uint64_t kBusyNs = 1'000;
    /// The interval otherwise.
    static constexpr uint64_t kQuietNs = 16'000;
    static constexpr uint64_t kWindowNs = 1'000'000;
    /// An entry that took this long gave the CPU away: one that did not takes 46-216 ns, and a loop
    /// that ran in it works for microseconds.
    static constexpr uint64_t kGaveUpNs = 3'000;

    /// Whether a thread whose clock reads `now_ns` should enter the kernel now.
    bool due(uint64_t now_ns) const {
        const uint64_t interval =
            gave_up_ && now_ns - gave_up_at_ns_ < kWindowNs ? kBusyNs : kQuietNs;
        return now_ns - entered_at_ns_ >= interval;
    }
    /// An entry, from `before_ns` to `after_ns`; true if it gave the CPU away.
    bool entered(uint64_t before_ns, uint64_t after_ns) {
        entered_at_ns_ = after_ns;
        if (after_ns - before_ns < kGaveUpNs) return false;
        gave_up_ = true;
        gave_up_at_ns_ = after_ns;
        return true;
    }

private:
    uint64_t entered_at_ns_{0};
    uint64_t gave_up_at_ns_{0};
    bool gave_up_{false};
};

/// Threads the candidates of a scan are read on (OB-12): sized by the profile, and shared by every
/// query of the process.
///
/// A query hands it one task per candidate and takes back, in the candidates' order, what each made.
/// A task no thread has taken yet the query runs itself (`run_or_wait()`), so a query never waits
/// on the queue - only on a task a thread is already running, and a task waits on nothing. That is
/// the whole argument that a pool shared by every reactor's queries cannot deadlock, and it needs
/// one thing from the work: that it never waits for another task.
class ScanPool {
public:
    /// One candidate's work: queued, then taken by exactly one of a thread and its query, then done.
    class Task {
    public:
        explicit Task(std::function<void()> work) : work_(std::move(work)) {}
        Task(const Task&) = delete;
        Task& operator=(const Task&) = delete;
        bool done() const { return state_.load(std::memory_order_acquire) == kDone; }

    private:
        friend class ScanPool;
        static constexpr uint8_t kQueued = 0;
        static constexpr uint8_t kTaken  = 1;
        static constexpr uint8_t kDone   = 2;
        /// Whether this caller is the one that takes it: exactly one caller is.
        bool take();
        /// The work, run by whoever took it; what it threw kept for its query; then done, and every
        /// waiter woken.
        void run();
        /// Done without running: a task its query no longer needs.
        void drop();
        void wait_done() const;

        std::function<void()> work_;
        std::exception_ptr error_;
        std::atomic<uint8_t> state_{kQueued};
    };

    /// `threads` of 0 is a pool that runs every task on the thread that waits for it.
    ScanPool(unsigned threads, MetricsRegistry* registry);
    ~ScanPool();
    ScanPool(const ScanPool&) = delete;
    ScanPool& operator=(const ScanPool&) = delete;

    unsigned threads() const { return thread_count_; }

    /// The rows a pool thread hands a part at a time, a step of a batch: 0.6 us of the buckets and
    /// 1.1 us of `price_all` on the m8a.xlarge, so that `pace()` between them can keep to `kBusyNs`.
    static constexpr size_t kStepRows = 128;

    /// Called by a pool thread between the steps of its work: enters the kernel when `ScanPace`
    /// says, so a loop woken onto its CPU runs (#248). The pace is the thread's own, across its parts.
    static void pace();

    /// The entries `pace()` made on the pool's threads, and those of them that gave the CPU away,
    /// counted as each task ends.
    uint64_t entries() const { return entries_.load(std::memory_order_relaxed); }
    uint64_t gave_up() const { return gave_up_.load(std::memory_order_relaxed); }

    /// Queue `work` for the pool's threads. With none, nothing is queued, and run_or_wait() runs it.
    std::shared_ptr<Task> submit(std::function<void()> work);

    /// The task done: run here if no thread has taken it, waited for if one has. Rethrows what its
    /// work threw.
    void run_or_wait(Task& task);

    /// The task finished or never started: dropped if no thread has taken it, waited for if one
    /// has. What its work threw is not rethrown - a query that drops its tasks is already leaving.
    void drop_or_wait(Task& task);

    /// A part its query read itself without handing it out - one past what a LIMIT let out ahead -
    /// counted with the parts queries ran.
    void ran_here();

    /// Whether this scan may divide: one scan at a time does, and a scan that finds the pool divided
    /// already reads on its own thread, so queries that run at once are not all queued behind one
    /// another's parts. `release()` gives it back.
    bool claim();
    void release();

    /// Stop the threads and join them. A task still queued stays with whoever waits for it, and
    /// run_or_wait() runs a task nobody took, so a stop strands no query.
    void stop();

private:
    void worker_loop();

    MetricsRegistry* registry_;
    unsigned thread_count_;
    std::mutex mtx_;
    std::condition_variable cv_;
    std::deque<std::shared_ptr<Task>> queue_;
    bool stopping_{false};
    std::vector<std::thread> threads_;
    std::atomic<bool> claimed_{false};
    std::atomic<uint64_t> pooled_{0};
    std::atomic<uint64_t> by_queries_{0};
    std::atomic<uint64_t> entries_{0};
    std::atomic<uint64_t> gave_up_{0};
};

}  // namespace ob
