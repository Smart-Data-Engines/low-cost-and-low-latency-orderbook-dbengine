// TLS handshakes of the client port, off the client loops (#265).

#include "orderbook/tls_handshake_pool.hpp"

#include "orderbook/logger.hpp"
#include "orderbook/metrics.hpp"
#include "orderbook/thread_boundary.hpp"
#include "orderbook/tls.hpp"

#include <openssl/err.h>
#include <openssl/ssl.h>

#include <pthread.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>

namespace ob {

namespace {

double ms_since(std::chrono::steady_clock::time_point started) {
    return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - started).count();
}

}  // namespace

// ── TlsHandshakeWorker ───────────────────────────────────────────────────────

TlsHandshakeWorker::TlsHandshakeWorker(unsigned index, TlsHandshakePool& pool)
    : index_(index), pool_(pool) {
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        throw std::runtime_error(std::string("epoll_create1() failed for a TLS handshake thread: ") +
                                 std::strerror(errno));
    }
    wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd_ < 0) {
        const int err = errno;
        ::close(epoll_fd_);
        throw std::runtime_error(std::string("eventfd() failed for a TLS handshake thread: ") +
                                 std::strerror(err));
    }
    epoll_event ev{};
    ev.events  = EPOLLIN;
    ev.data.fd = wake_fd_;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &ev) < 0) {
        const int err = errno;
        ::close(wake_fd_);
        ::close(epoll_fd_);
        throw std::runtime_error(std::string("epoll_ctl() failed for a TLS handshake thread: ") +
                                 std::strerror(err));
    }
}

TlsHandshakeWorker::~TlsHandshakeWorker() {
    // After the thread has ended, whichever way: what it still held is closed here, not leaked, and
    // the accepting loop's count of sessions gets each place back.
    size_t closed = 0;
    for (auto& [fd, h] : active_) {
        ::close(fd);
        if (pool_.registry_) pool_.registry_->increment_gauge("ob_tls_handshakes_in_flight", -1);
        pool_.give_back_();
        ++closed;
    }
    active_.clear();
    std::vector<Incoming> left;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        left.swap(incoming_);
    }
    for (const Incoming& in : left) {
        ::close(in.fd);
        pool_.give_back_();
        ++closed;
    }
    if (closed > 0) {
        OB_LOG_INFO("tls", "ob-tls-%u stopped: %zu handshake(s) under way closed", index_, closed);
    }
    if (wake_fd_ >= 0) ::close(wake_fd_);
    if (epoll_fd_ >= 0) ::close(epoll_fd_);
}

void TlsHandshakeWorker::submit(int fd, uint64_t conn_id, size_t loop) {
    {
        std::lock_guard<std::mutex> lock(mtx_);
        incoming_.push_back(Incoming{fd, conn_id, loop});
    }
    const uint64_t one = 1;
    const ssize_t written = ::write(wake_fd_, &one, sizeof(one));
    (void)written;   // a full counter is a thread already woken
    OB_LOG_DEBUG("tls", "fd=%d conn_id=%llu handed to ob-tls-%u for its handshake, then client loop %zu",
                 fd, static_cast<unsigned long long>(conn_id), index_, loop);
}

void TlsHandshakeWorker::stop() {
    stopping_.store(true, std::memory_order_release);
    const uint64_t one = 1;
    const ssize_t written = ::write(wake_fd_, &one, sizeof(one));
    (void)written;
}

void TlsHandshakeWorker::run_loop() {
    epoll_event events[64];
    while (!stopping_.load(std::memory_order_acquire)) {
        // Until the nearest deadline, which the oldest handshake still has, or an event.
        int wait_ms = -1;
        if (!deadlines_.empty()) {
            const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadlines_.front().at - std::chrono::steady_clock::now());
            wait_ms = left.count() <= 0 ? 0 : static_cast<int>(std::min<long long>(left.count() + 1, 60'000));
        }
        const int n = ::epoll_wait(epoll_fd_, events, 64, wait_ms);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error(std::string("epoll_wait() failed on a TLS handshake thread: ") +
                                     std::strerror(errno));
        }
        for (int i = 0; i < n; ++i) {
            const int fd = events[i].data.fd;
            // One connection's failure costs that connection: it is closed, and the thread goes on
            // driving the others. Ended, the thread would leave every connection dealt to it waiting
            // for a handshake nobody drives.
            try {
                if (fd == wake_fd_) {
                    take_incoming();
                } else {
                    drive(fd);
                }
            } catch (const std::exception& e) {
                OB_LOG_ERROR("tls", "ob-tls-%u: %s on fd=%d", index_, e.what(), fd);
                if (fd != wake_fd_) drop(fd, "ob_tls_handshake_failures_total", "failed on an exception");
            }
        }
        try {
            expire(std::chrono::steady_clock::now());
        } catch (const std::exception& e) {
            OB_LOG_ERROR("tls", "ob-tls-%u: %s while closing the handshakes past their time", index_, e.what());
        }
    }
}

void TlsHandshakeWorker::take_incoming() {
    uint64_t drained = 0;
    const ssize_t got = ::read(wake_fd_, &drained, sizeof(drained));
    (void)got;   // EAGAIN: another pass took the wake-up; the queue decides
    std::vector<Incoming> taken;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        taken.swap(incoming_);
    }
    for (const Incoming& in : taken) start(in);
}

void TlsHandshakeWorker::start(const Incoming& in) {
    std::shared_ptr<ssl_st> ssl;
    try {
        // current() when the handshake starts: a certificate replaced by SIGHUP (#238) is the one
        // every handshake started after it presents, as on the loop.
        ssl = pool_.surface_->current()->wrap(in.fd, /*server_side=*/true);
    } catch (const std::exception& e) {
        OB_LOG_WARN("tls", "cannot start a handshake on fd=%d conn_id=%llu: %s", in.fd,
                    static_cast<unsigned long long>(in.conn_id), e.what());
        ::close(in.fd);
        if (pool_.registry_) pool_.registry_->increment_counter("ob_tls_handshake_failures_total");
        pool_.give_back_();
        return;
    }
    epoll_event ev{};
    ev.events  = EPOLLIN | EPOLLOUT | EPOLLET;
    ev.data.fd = in.fd;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, in.fd, &ev) < 0) {
        OB_LOG_WARN("tls", "ob-tls-%u cannot watch fd=%d conn_id=%llu: %s", index_, in.fd,
                    static_cast<unsigned long long>(in.conn_id), std::strerror(errno));
        ::close(in.fd);
        if (pool_.registry_) pool_.registry_->increment_counter("ob_tls_handshake_failures_total");
        pool_.give_back_();
        return;
    }
    const auto now = std::chrono::steady_clock::now();
    active_[in.fd] = Handshake{in.fd, in.conn_id, in.loop, std::move(ssl), now};
    deadlines_.push_back(Deadline{now + pool_.timeout_, in.fd, in.conn_id});
    if (pool_.registry_) pool_.registry_->increment_gauge("ob_tls_handshakes_in_flight", 1);
    OB_LOG_DEBUG("tls", "handshake started on ob-tls-%u: fd=%d conn_id=%llu", index_, in.fd,
                 static_cast<unsigned long long>(in.conn_id));
    drive(in.fd);   // the ClientHello may be in the socket already
}

void TlsHandshakeWorker::drive(int fd) {
    const auto it = active_.find(fd);
    if (it == active_.end()) return;   // ended on an earlier event of this pass
    Handshake& h = it->second;
    ERR_clear_error();
    const int rc = SSL_accept(h.ssl.get());
    if (rc == 1) {
        complete(fd);
        return;
    }
    const int err = SSL_get_error(h.ssl.get(), rc);
    if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) return;   // the socket's next edge
    const std::string why = std::string("failed: ") + tls_error_name(err) + ": " + tls_last_error();
    drop(fd, "ob_tls_handshake_failures_total", why.c_str());
}

void TlsHandshakeWorker::complete(int fd) {
    auto node = active_.extract(fd);
    Handshake& h = node.mapped();
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);   // the loop's from here
    const double ms = ms_since(h.started);
    if (pool_.registry_) {
        pool_.registry_->increment_counter("ob_tls_handshakes_total");
        pool_.registry_->increment_gauge("ob_tls_handshakes_in_flight", -1);
        pool_.registry_->observe_histogram("ob_tls_handshake_seconds", ms / 1000.0);
    }
    OB_LOG_INFO("tls", "handshake complete: fd=%d conn_id=%llu version=%s cipher=%s, on ob-tls-%u in %.1f ms",
                fd, static_cast<unsigned long long>(h.conn_id), SSL_get_version(h.ssl.get()),
                SSL_get_cipher(h.ssl.get()), index_, ms);
    try {
        pool_.deliver_(h.loop, TlsHandshakePool::Done{fd, h.conn_id, std::move(h.ssl)});
    } catch (const std::exception& e) {
        // Not in the loop's queue, so nobody else will close it: out of the handshakes already, the
        // connection is closed here and its place given back.
        OB_LOG_ERROR("tls", "fd=%d conn_id=%llu could not be given to client loop %zu: %s; closed", fd,
                     static_cast<unsigned long long>(h.conn_id), h.loop, e.what());
        ::close(fd);
        if (pool_.registry_) pool_.registry_->increment_counter("ob_tls_handshake_failures_total");
        pool_.give_back_();
    }
}

void TlsHandshakeWorker::drop(int fd, const char* metric, const char* why) {
    const auto it = active_.find(fd);
    if (it == active_.end()) return;
    const uint64_t conn_id = it->second.conn_id;
    const double   ms      = ms_since(it->second.started);
    active_.erase(it);   // the SSL with it; it holds the descriptor without owning it
    ::close(fd);          // and out of the epoll with it
    if (pool_.registry_) {
        pool_.registry_->increment_counter(metric);
        pool_.registry_->increment_gauge("ob_tls_handshakes_in_flight", -1);
    }
    OB_LOG_WARN("tls", "handshake %s: fd=%d conn_id=%llu after %.1f ms on ob-tls-%u; closed", why, fd,
                static_cast<unsigned long long>(conn_id), ms, index_);
    pool_.give_back_();
}

void TlsHandshakeWorker::expire(std::chrono::steady_clock::time_point now) {
    while (!deadlines_.empty() && deadlines_.front().at <= now) {
        const Deadline d = deadlines_.front();
        deadlines_.pop_front();
        const auto it = active_.find(d.fd);
        // Done already, or the descriptor is another connection's now.
        if (it == active_.end() || it->second.conn_id != d.conn_id) continue;
        drop(d.fd, "ob_tls_handshake_timeouts_total", "timed out");
    }
}

// ── TlsHandshakePool ─────────────────────────────────────────────────────────

TlsHandshakePool::TlsHandshakePool(unsigned threads, std::chrono::milliseconds timeout,
                                   std::shared_ptr<TlsSurface> surface, MetricsRegistry* registry,
                                   Deliver deliver, GiveBack give_back)
    : timeout_(timeout),
      surface_(std::move(surface)),
      registry_(registry),
      deliver_(std::move(deliver)),
      give_back_(std::move(give_back)) {
    if (threads == 0) throw std::invalid_argument("a TLS handshake pool needs a thread");
    workers_.reserve(threads);
    threads_.reserve(threads);
    for (unsigned i = 0; i < threads; ++i) {
        workers_.push_back(std::make_unique<TlsHandshakeWorker>(i, *this));
    }
    try {
        for (unsigned i = 0; i < threads; ++i) {
            TlsHandshakeWorker* w = workers_[i].get();
            threads_.push_back(std::thread([w] {
                run_thread_body("tls", "a TLS handshake thread", [&] { w->run_loop(); });
            }));
            // Named so `top -H` says which thread is which; "ob-tls-63" is within the kernel's fifteen.
            const std::string name = "ob-tls-" + std::to_string(i);
            pthread_setname_np(threads_.back().native_handle(), name.c_str());
        }
    } catch (const std::exception& e) {
        // A thread the system would not start: those started are stopped and joined, which a
        // destructor that never runs for a half-built pool would not do, and the server is told why.
        OB_LOG_ERROR("tls", "cannot start TLS handshake thread %zu of %u: %s", threads_.size(), threads,
                     e.what());
        for (auto& w : workers_) w->stop();
        for (auto& t : threads_) t.join();
        throw;
    }
    OB_LOG_INFO("tls", "%u thread(s) drive the client port's TLS handshakes, off the client loops, "
                "%lld ms at most each (#265)", threads, static_cast<long long>(timeout.count()));
}

TlsHandshakePool::~TlsHandshakePool() {
    for (auto& w : workers_) w->stop();
    for (auto& t : threads_) {
        if (t.joinable()) t.join();
    }
    workers_.clear();   // each closes what it still held
    OB_LOG_INFO("tls", "TLS handshake threads stopped");
}

void TlsHandshakePool::submit(int fd, uint64_t conn_id, size_t loop) {
    const size_t i = next_.fetch_add(1, std::memory_order_relaxed) % workers_.size();
    workers_[i]->submit(fd, conn_id, loop);
}

}  // namespace ob
