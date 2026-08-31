#include "orderbook/async_snapshot.hpp"

#include "orderbook/logger.hpp"

#include <chrono>
#include <exception>
#include <system_error>
#include <utility>

namespace ob {

AsyncSnapshotBuilder::AsyncSnapshotBuilder(Notify notify) : notify_(std::move(notify)) {}

AsyncSnapshotBuilder::~AsyncSnapshotBuilder() { shutdown(); }

bool AsyncSnapshotBuilder::start(uint64_t token, Producer produce) {
    std::unique_lock<std::mutex> lock(mtx_);

    if (running_ || done_) {
        OB_LOG_DEBUG("async_snapshot", "Refusing token %llu: %s",
                     static_cast<unsigned long long>(token),
                     running_ ? "a snapshot is already being created"
                              : "a finished snapshot has not been collected");
        return false;
    }
    if (!produce) {
        OB_LOG_ERROR("async_snapshot", "Refusing token %llu: no producer",
                     static_cast<unsigned long long>(token));
        return false;
    }

    // Unreachable: every exit path moves the thread object out before clearing the flags, so a
    // joinable one here would mean those flags lied. It is checked anyway because assigning over a
    // joinable std::thread calls std::terminate, and losing one snapshot beats losing the process.
    //
    // This refuses rather than carrying on, so nothing has to be assumed about the state across the
    // gap where the lock is released — and it repairs the object on the way out, so the next request
    // is not wedged for ever.
    if (worker_.joinable()) {
        std::thread stale = std::move(worker_);
        lock.unlock();
        stale.join();
        OB_LOG_ERROR("async_snapshot",
                     "Refusing token %llu: a previous worker had not been joined; it has been now",
                     static_cast<unsigned long long>(token));
        return false;
    }

    running_ = true;
    done_    = false;
    result_  = Result{};

    try {
        worker_ = std::thread([this, token, produce = std::move(produce)]() mutable {
            const auto t0 = std::chrono::steady_clock::now();

            Result r;
            r.token = token;
            try {
                r.snap = produce();
                r.ok   = true;
            } catch (const std::exception& e) {
                r.error = e.what();
                OB_LOG_ERROR("async_snapshot", "Creating snapshot for token %llu threw: %s",
                             static_cast<unsigned long long>(token), e.what());
            } catch (...) {
                r.error = "unknown exception";
                OB_LOG_ERROR("async_snapshot",
                             "Creating snapshot for token %llu threw a non-standard exception",
                             static_cast<unsigned long long>(token));
            }

            const double ms = std::chrono::duration<double, std::milli>(
                                  std::chrono::steady_clock::now() - t0).count();

            {
                std::lock_guard<std::mutex> pub(mtx_);
                result_  = std::move(r);
                running_ = false;
                done_    = true;
            }

            // Publish first, notify second. The other order loses the wake-up: the owner can look,
            // find nothing, and go back to waiting — and no second notification is coming.
            OB_LOG_INFO("async_snapshot", "Worker finished token %llu in %.1f ms",
                        static_cast<unsigned long long>(token), ms);
            if (notify_) notify_();
        });
    } catch (const std::system_error& e) {
        running_ = false;
        OB_LOG_ERROR("async_snapshot", "Cannot start a worker for token %llu: %s",
                     static_cast<unsigned long long>(token), e.what());
        return false;
    }

    OB_LOG_INFO("async_snapshot", "Creating a snapshot on a worker thread (token %llu)",
                static_cast<unsigned long long>(token));
    return true;
}

bool AsyncSnapshotBuilder::busy() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return running_ || done_;
}

std::optional<AsyncSnapshotBuilder::Result> AsyncSnapshotBuilder::take_result() {
    std::thread finished;
    Result      out;

    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (!done_) return std::nullopt;

        finished = std::move(worker_);
        out      = std::move(result_);
        result_  = Result{};
        done_    = false;
    }

    // Joined with the mutex released. Holding it here would be safe on this path alone — `done_`
    // means the worker has already published and let go — but it would also mean a Notify that
    // touched this object could hang the process, and that is not a property to leave lying about.
    // The join itself waits only for the worker to return.
    if (finished.joinable()) finished.join();
    return out;
}

void AsyncSnapshotBuilder::shutdown() {
    std::thread victim;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        victim = std::move(worker_);
    }

    // The mutex must be released before this join, and this is the path that proves it: unlike
    // take_result(), shutdown() joins a worker that may not have published yet — and publishing is
    // the worker taking that very mutex. Holding it here deadlocked the process, with the owner in
    // join() and the worker one line from the end (found by gdb, since the hang printed nothing).
    if (victim.joinable()) {
        OB_LOG_DEBUG("async_snapshot", "Waiting for the snapshot worker before shutting down");
        victim.join();
    }

    std::lock_guard<std::mutex> lock(mtx_);
    if (done_) {
        OB_LOG_DEBUG("async_snapshot", "Dropping a finished snapshot (token %llu): shutting down",
                     static_cast<unsigned long long>(result_.token));
    }

    result_  = Result{};
    running_ = false;
    done_    = false;
}

}  // namespace ob
