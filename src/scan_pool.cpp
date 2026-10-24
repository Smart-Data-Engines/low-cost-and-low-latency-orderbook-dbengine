#include "orderbook/scan_pool.hpp"

#include "orderbook/logger.hpp"
#include "orderbook/loop_guard.hpp"
#include "orderbook/thread_boundary.hpp"

#include <pthread.h>
#include <sched.h>

#include <chrono>
#include <cstring>

#include <string>
#include <utility>

namespace ob {

namespace {

/// What `pace()` did on this thread since its last task ended, added to the pool's counts then.
thread_local uint64_t t_entries = 0;
thread_local uint64_t t_gave_up = 0;

}  // namespace

void ScanPool::pace() {
    thread_local ScanPace pace;
    const auto now = ScanPace::Clock::now();
    if (!pace.due(now)) return;
    ::sched_yield();
    ++t_entries;
    if (pace.entered(now, ScanPace::Clock::now())) ++t_gave_up;
}

bool ScanPool::Task::take() {
    uint8_t queued = kQueued;
    return state_.compare_exchange_strong(queued, kTaken, std::memory_order_acq_rel,
                                          std::memory_order_acquire);
}

void ScanPool::Task::run() {
    try {
        work_();
    } catch (...) {
        error_ = std::current_exception();
    }
    // What the work captured goes now rather than with the last pointer to the task, which a
    // thread's queue can hold a moment longer than the query that made it.
    work_ = nullptr;
    state_.store(kDone, std::memory_order_release);
    state_.notify_all();
}

void ScanPool::Task::drop() {
    work_ = nullptr;
    state_.store(kDone, std::memory_order_release);
    state_.notify_all();
}

void ScanPool::Task::wait_done() const {
    for (uint8_t s = state_.load(std::memory_order_acquire); s != kDone;
         s = state_.load(std::memory_order_acquire)) {
        state_.wait(s, std::memory_order_acquire);
    }
}

ScanPool::ScanPool(unsigned threads, MetricsRegistry* registry)
    : registry_(registry), thread_count_(threads) {
    if (registry_ != nullptr) registry_->set_gauge("ob_scan_threads", static_cast<int64_t>(threads));
    threads_.reserve(threads);
    for (unsigned i = 0; i < threads; ++i) {
        threads_.emplace_back([this] {
            // Below the client loops (#246): the pool reads on the CPU the loops leave, and enters
            // the kernel between the steps of its work (`pace()`, #248). Neither half was enough
            // alone, measured on the x86 host beside a writer: at the normal policy, at nice 19 and
            // under SCHED_IDLE the writer's p99 was 0.4-1.3 ms, a loop woken onto a CPU a pool
            // thread held waiting for the scheduler's tick. Lowering a thread's own policy needs no
            // privilege.
            sched_param idle{};
            idle.sched_priority = 0;
            if (const int err = ::pthread_setschedparam(::pthread_self(), SCHED_IDLE, &idle); err != 0) {
                OB_LOG_WARN("scan_pool", "a scan pool thread could not take SCHED_IDLE: %s; it reads "
                                         "at the client loops' priority",
                            std::strerror(err));
            }
            run_thread_body("scan_pool", "a scan pool thread", [this] { worker_loop(); });
        });
        const std::string name = "ob-scan-" + std::to_string(i);
        pthread_setname_np(threads_.back().native_handle(), name.c_str());
    }
    OB_LOG_INFO("scan_pool", "scan pool: %u thread(s)%s", threads,
                threads == 0 ? "; every part of a scan runs on its query's thread"
                             : ", under SCHED_IDLE below the client loops");
}

ScanPool::~ScanPool() { stop(); }

std::shared_ptr<ScanPool::Task> ScanPool::submit(std::function<void()> work) {
    auto task = std::make_shared<Task>(std::move(work));
    if (thread_count_ == 0) return task;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (stopping_) return task;   // left to run_or_wait(), which runs a task nobody took
        queue_.push_back(task);
    }
    cv_.notify_one();
    return task;
}

void ScanPool::run_or_wait(Task& task) {
    if (task.take()) {
        by_queries_.fetch_add(1, std::memory_order_relaxed);
        if (registry_ != nullptr) registry_->increment_counter("ob_scan_parts_inline_total");
        task.run();
    } else {
        task.wait_done();
    }
    if (task.error_) std::rethrow_exception(task.error_);
}

bool ScanPool::claim() {
    bool free = false;
    return claimed_.compare_exchange_strong(free, true, std::memory_order_acq_rel);
}

void ScanPool::release() { claimed_.store(false, std::memory_order_release); }

void ScanPool::ran_here() {
    by_queries_.fetch_add(1, std::memory_order_relaxed);
    if (registry_ != nullptr) registry_->increment_counter("ob_scan_parts_inline_total");
}

void ScanPool::drop_or_wait(Task& task) {
    if (task.take()) {
        task.drop();
        return;
    }
    task.wait_done();
}

void ScanPool::stop() {
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (stopping_) return;
        stopping_ = true;
    }
    cv_.notify_all();
    for (auto& t : threads_) {
        if (t.joinable()) t.join();
    }
    size_t left = 0;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        left = queue_.size();
        queue_.clear();
    }
    OB_LOG_INFO("scan_pool", "scan pool stopped: %llu part(s) run on its threads, %llu by their "
                             "queries; %zu queued and left to their queries; its threads entered "
                             "the kernel %llu time(s) between steps, %llu of them giving their CPU "
                             "to another thread",
                static_cast<unsigned long long>(pooled_.load(std::memory_order_relaxed)),
                static_cast<unsigned long long>(by_queries_.load(std::memory_order_relaxed)), left,
                static_cast<unsigned long long>(entries_.load(std::memory_order_relaxed)),
                static_cast<unsigned long long>(gave_up_.load(std::memory_order_relaxed)));
}

void ScanPool::worker_loop() {
    LoopGuard guard{"scan_pool", "a scan pool pass", registry_};
    for (;;) {
        std::shared_ptr<Task> task;
        {
            std::unique_lock<std::mutex> lock(mtx_);
            cv_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
            if (stopping_) return;
            task = std::move(queue_.front());
            queue_.pop_front();
        }
        try {
            // A task its query has taken already, to run itself or to drop, is passed over.
            if (task->take()) {
                pooled_.fetch_add(1, std::memory_order_relaxed);
                if (registry_ != nullptr) registry_->increment_counter("ob_scan_parts_pooled_total");
                task->run();
                entries_.fetch_add(std::exchange(t_entries, 0), std::memory_order_relaxed);
                gave_up_.fetch_add(std::exchange(t_gave_up, 0), std::memory_order_relaxed);
            }
            guard.ok();
        } catch (const std::exception& e) {
            guard.caught(e);
        }
    }
}

}  // namespace ob
