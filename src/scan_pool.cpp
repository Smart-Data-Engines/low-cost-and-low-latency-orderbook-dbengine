#include "orderbook/scan_pool.hpp"

#include "orderbook/logger.hpp"
#include "orderbook/loop_guard.hpp"
#include "orderbook/thread_boundary.hpp"

#include <pthread.h>

#include <string>

namespace ob {

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
            run_thread_body("scan_pool", "a scan pool thread", [this] { worker_loop(); });
        });
        const std::string name = "ob-scan-" + std::to_string(i);
        pthread_setname_np(threads_.back().native_handle(), name.c_str());
    }
    OB_LOG_INFO("scan_pool", "scan pool: %u thread(s)%s", threads,
                threads == 0 ? "; every part of a scan runs on its query's thread" : "");
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

void ScanPool::ran_here() {
    by_queries_.fetch_add(1, std::memory_order_relaxed);
    if (registry_ != nullptr) registry_->increment_counter("ob_scan_parts_inline_total");
}

bool ScanPool::run_here_if_untaken(Task& task) {
    if (!task.take()) return false;
    by_queries_.fetch_add(1, std::memory_order_relaxed);
    if (registry_ != nullptr) registry_->increment_counter("ob_scan_parts_inline_total");
    task.run();
    return true;
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
                             "queries; %zu queued and left to their queries",
                static_cast<unsigned long long>(pooled_.load(std::memory_order_relaxed)),
                static_cast<unsigned long long>(by_queries_.load(std::memory_order_relaxed)), left);
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
            }
            guard.ok();
        } catch (const std::exception& e) {
            guard.caught(e);
        }
    }
}

}  // namespace ob
