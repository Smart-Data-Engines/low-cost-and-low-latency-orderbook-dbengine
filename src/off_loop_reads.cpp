// Large reads answered off the client loop (#260).

#include "orderbook/off_loop_reads.hpp"
#include "orderbook/logger.hpp"

#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>

#include <pthread.h>

namespace ob {

OffLoopMailbox::OffLoopMailbox() {
    event_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (event_fd_ < 0) {
        throw std::runtime_error(std::string("eventfd() failed for the off-loop mailbox: ") +
                                 std::strerror(errno));
    }
}

OffLoopMailbox::~OffLoopMailbox() {
    if (event_fd_ >= 0) ::close(event_fd_);
}

void OffLoopMailbox::post(OffLoopAnswer answer) {
    {
        std::lock_guard<std::mutex> lock(mtx_);
        answers_.push_back(std::move(answer));
    }
    const uint64_t one = 1;
    const ssize_t written = ::write(event_fd_, &one, sizeof(one));
    (void)written;   // a full counter is a loop already woken
}

std::vector<OffLoopAnswer> OffLoopMailbox::take() {
    uint64_t drained = 0;
    const ssize_t got = ::read(event_fd_, &drained, sizeof(drained));
    (void)got;   // EAGAIN: nothing posted since the last take
    std::lock_guard<std::mutex> lock(mtx_);
    std::vector<OffLoopAnswer> out;
    out.swap(answers_);
    return out;
}

OffLoopPool::OffLoopPool(unsigned threads) {
    threads_.reserve(threads);
    for (unsigned i = 0; i < threads; ++i) {
        threads_.emplace_back([this, i] { run(i); });
    }
    OB_LOG_INFO("off_loop", "%u thread(s) answer large reads off the client loops: a read whose scan "
                "the index puts at %llu rows or more (#260)", threads,
                static_cast<unsigned long long>(kOffLoopReadRows));
}

OffLoopPool::~OffLoopPool() {
    size_t dropped = 0;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        stopping_ = true;
        dropped = queue_.size();
        queue_.clear();
    }
    cv_.notify_all();
    for (auto& t : threads_) {
        if (t.joinable()) t.join();
    }
    OB_LOG_INFO("off_loop", "stopped: %zu queued read(s) dropped, the running ones finished", dropped);
}

void OffLoopPool::submit(std::function<void()> read) {
    size_t waiting = 0;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        queue_.push_back(std::move(read));
        waiting = queue_.size();
    }
    cv_.notify_one();
    OB_LOG_DEBUG("off_loop", "a large read handed off, %zu waiting for a thread", waiting);
}

size_t OffLoopPool::queued() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return queue_.size();
}

void OffLoopPool::run(unsigned index) {
    const std::string name = "ob-read-" + std::to_string(index);
    pthread_setname_np(pthread_self(), name.c_str());
    for (;;) {
        std::function<void()> read;
        {
            std::unique_lock<std::mutex> lock(mtx_);
            cv_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
            if (queue_.empty()) return;   // stopping, and nothing left
            read = std::move(queue_.front());
            queue_.pop_front();
        }
        try {
            read();
        } catch (const std::exception& e) {
            // The read answers its own failures; one that escapes is a defect, logged, and the
            // thread goes on serving the others.
            OB_LOG_ERROR("off_loop", "%s: a read threw past its own answer: %s", name.c_str(), e.what());
        }
    }
}

}  // namespace ob
