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
    std::atomic<uint64_t> pooled_{0};
    std::atomic<uint64_t> by_queries_{0};
};

}  // namespace ob
