#pragma once
// TLS handshakes of the client port, off the client loops (#265).
//
// A client loop drove each of its TLS sessions' handshakes itself, between the events of every
// other session it serves: two processes opening connections back to back had about 2 200 RSA-2048
// handshakes a second out of a loop on an m8a.xlarge and 1 300 on an m9g.xlarge, and raised the
// median PING of a session on the same loop 31-36 times. Here the accepting loop hands a TLS
// connection to a thread of this pool instead, which drives SSL_accept() on the connection's events
// and gives the loop it was dealt to a session whose handshake is done - or closes it, when the
// handshake fails or runs out of time.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

struct ssl_st;

namespace ob {

class MetricsRegistry;
class TlsSurface;
class TlsHandshakePool;

/// One thread of the pool: an epoll of its own, a queue of connections the accepting loop submitted,
/// and the handshakes under way with their deadlines.
class TlsHandshakeWorker {
public:
    TlsHandshakeWorker(unsigned index, TlsHandshakePool& pool);
    ~TlsHandshakeWorker();   // closes every handshake still here, each place in --max-sessions given back

    TlsHandshakeWorker(const TlsHandshakeWorker&)            = delete;
    TlsHandshakeWorker& operator=(const TlsHandshakeWorker&) = delete;

    /// From the accepting loop: a connection whose handshake this thread is to drive.
    void submit(int fd, uint64_t conn_id, size_t loop);
    /// The loop ends at its next pass.
    void stop();
    /// The thread's body.
    void run_loop();

private:
    struct Incoming {
        int      fd;
        uint64_t conn_id;
        size_t   loop;
    };
    struct Handshake {
        int                                   fd;
        uint64_t                              conn_id;
        size_t                                loop;
        std::shared_ptr<ssl_st>               ssl;
        std::chrono::steady_clock::time_point started;
    };
    struct Deadline {
        std::chrono::steady_clock::time_point at;
        int                                   fd;
        uint64_t                              conn_id;
    };

    void take_incoming();
    void start(const Incoming& in);
    void drive(int fd);
    void complete(int fd);
    /// Closes a handshake that will not complete, with the counter `metric` and `why` in the log.
    void drop(int fd, const char* metric, const char* why);
    void expire(std::chrono::steady_clock::time_point now);

    const unsigned    index_;
    TlsHandshakePool& pool_;
    int               epoll_fd_{-1};
    int               wake_fd_{-1};
    std::atomic<bool> stopping_{false};

    std::mutex            mtx_;
    std::vector<Incoming> incoming_;   ///< under mtx_

    // The thread's own: only run_loop() and the destructor, after the thread has ended, touch these.
    std::unordered_map<int, Handshake> active_;
    /// In the order handshakes started, which is the order of their deadlines: every handshake has the
    /// same time. An entry outlives a handshake that ended; its conn_id says whether it still applies.
    std::deque<Deadline> deadlines_;
};

/// TLS handshakes of the client port, off the client loops (#265): `threads` threads, connections dealt
/// to them in turn. A handshake not done within `timeout` is closed.
class TlsHandshakePool {
public:
    /// A handshake done: the connection, and the SSL that is now the session's.
    struct Done {
        int                     fd;
        uint64_t                conn_id;
        std::shared_ptr<ssl_st> ssl;
    };
    /// Gives a session whose handshake is done to the client loop it was dealt to.
    using Deliver = std::function<void(size_t loop, Done done)>;
    /// Gives back the place in --max-sessions the accepting loop took, for a connection closed here.
    using GiveBack = std::function<void()>;

    TlsHandshakePool(unsigned threads, std::chrono::milliseconds timeout,
                     std::shared_ptr<TlsSurface> surface, MetricsRegistry* registry, Deliver deliver,
                     GiveBack give_back);
    /// Stops and joins the threads; every handshake under way is closed and its place given back.
    ~TlsHandshakePool();

    TlsHandshakePool(const TlsHandshakePool&)            = delete;
    TlsHandshakePool& operator=(const TlsHandshakePool&) = delete;

    /// An accepted connection of the client port, from the accepting loop: its handshake is driven on
    /// a thread of the pool, and the session is given to `loop` once it is done.
    void submit(int fd, uint64_t conn_id, size_t loop);

private:
    friend class TlsHandshakeWorker;

    const std::chrono::milliseconds                  timeout_;
    const std::shared_ptr<TlsSurface>                surface_;
    MetricsRegistry* const                           registry_;
    const Deliver                                    deliver_;
    const GiveBack                                   give_back_;
    std::vector<std::unique_ptr<TlsHandshakeWorker>> workers_;
    std::vector<std::thread>                         threads_;
    std::atomic<size_t>                              next_{0};
};

}  // namespace ob
