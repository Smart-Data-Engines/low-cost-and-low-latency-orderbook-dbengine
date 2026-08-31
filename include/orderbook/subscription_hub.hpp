#pragma once

#include "orderbook/engine.hpp"
#include "orderbook/session.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

namespace ob {

// ── SubscriptionHub ───────────────────────────────────────────────────────────
//
// The piece that lets a subscription reach a socket without the write path touching one.
//
// `QueryEngine::notify_subscribers()` runs on whichever thread owns the write path: the server's
// epoll loop for a client's INSERT, `MultiMasterManager::io_loop` for a peer's delta. `Session` is
// not thread-safe and arming EPOLLOUT belongs to the epoll loop, so a notification arriving from
// io_loop may not write to a session. It enqueues here and wakes the loop through one eventfd; the
// loop calls drain() and moves bytes into sessions.
//
// Both paths go through the queue, including the client one that *is* already on the epoll thread.
// Letting that path write directly would give one buffer two writers, one of which is correct only
// by coincidence — the shape #79 removed from `handle_snapshot_request()`.
//
// Not part of TcpServer, because `TcpServer::run()` is already a thousand-line loop, and not part of
// QueryEngine, which knows nothing about sessions or epoll.

/// One subscriber's queued output.
struct SubscriberQueue {
    uint64_t    subscription_id{0};
    int         fd{-1};

    /// Connection identity, not the descriptor.
    ///
    /// Descriptor numbers are reused, so a subscription pinned to `fd` after a disconnect and a
    /// reconnect would push rows **to a different client**. Not hypothetical on a server that closes
    /// sessions on errors. Same reasoning as `PeerConnection::conn_id` in the multi-master path.
    uint64_t    conn_id{0};

    /// The id `QueryEngine` gave this subscription, for cancelling it.
    ///
    /// A second id, and the reason is a race rather than taste: the callback needs to know which
    /// queue to append to, and the engine's id is only known after `subscribe()` returns. A callback
    /// capturing a slot to be filled afterwards can fire in the window between the two, because the
    /// write path is another thread. So the client-visible id is the hub's, allocated first.
    uint64_t    engine_sub_id{0};

    /// Formatted, complete lines awaiting the epoll loop. Never a partial row.
    std::string pending;

    /// Rows delivered to the session, for the counter.
    uint64_t    pushed{0};

    /// Set when `pending` passed the ceiling. The queue is **not** cleared: the client has already
    /// read part of it, and taking the rest back leaves their parser looking at truncated input
    /// rather than at a disconnect. That is #69, where clearing a peer's queue corrupted the framing.
    bool        overflowed{false};
};

class SubscriptionHub {
public:
    SubscriptionHub(size_t max_queue_bytes, int max_per_session);
    ~SubscriptionHub();

    SubscriptionHub(const SubscriptionHub&) = delete;
    SubscriptionHub& operator=(const SubscriptionHub&) = delete;

    /// The eventfd for the epoll loop to watch. -1 if it could not be created.
    int wakeup_fd() const { return wakeup_fd_; }

    /// Register a subscription. Called on the epoll thread, when SUBSCRIBE arrives.
    ///
    /// Returns the id, or 0 with `*error` set. Refusals: the query does not parse, or this
    /// connection is already at `max_per_session`.
    uint64_t add(Engine& engine, int fd, uint64_t conn_id, const std::string& sql,
                 std::string* error);

    /// Cancel one subscription. Returns 1 if it existed, 0 otherwise.
    int remove(Engine& engine, uint64_t id);

    /// Cancel every subscription of one connection. Returns how many. Called from close_session()
    /// **before** the session is removed: the other order leaves a window in which a notification
    /// lands in a queue whose session is already gone.
    int remove_connection(Engine& engine, int fd, uint64_t conn_id);

    /// Move queued bytes into sessions. Called only on the epoll thread.
    ///
    /// Returns the descriptors whose session must be closed — a queue past the ceiling, or a session
    /// that refused the write. Closing is the caller's job because it owns the epoll set and the
    /// session map.
    std::vector<int> drain(SessionManager& sessions,
                           const std::function<void(int)>& arm_epollout);

    /// Live subscriptions, for the gauge and for tests.
    size_t active() const;

    /// Bytes queued across all subscribers, for the gauge. The operator-facing view of a consumer
    /// that has stopped reading.
    size_t queued_bytes() const;

    uint64_t rows_pushed() const { return rows_pushed_.load(std::memory_order_relaxed); }
    uint64_t overflow_disconnects() const {
        return overflow_disconnects_.load(std::memory_order_relaxed);
    }
    uint64_t refused() const { return refused_.load(std::memory_order_relaxed); }

private:
    /// Called from whichever thread the write path owns. Never touches a Session.
    void enqueue(uint64_t subscription_id, const QueryResult& row);

    /// Wake the epoll loop, at most once per pending batch.
    ///
    /// A 1000-level MINSERT enqueues a thousand rows. One eventfd write per row is a thousand
    /// syscalls on a path whose budget is measured in microseconds, so the first row into an
    /// un-woken hub writes and the rest do not. The loop clears the flag **after** reading the
    /// eventfd and **before** draining: the other order loses the wake-up for a row that arrives
    /// between the two, which is the publish-then-notify ordering from #79 pointed the same way.
    void wake();

    const size_t max_queue_bytes_;
    const int    max_per_session_;

    mutable std::mutex mtx_;
    std::vector<SubscriberQueue> queues_;

    uint64_t next_id_{1};

    int wakeup_fd_{-1};
    std::atomic<bool> wake_pending_{false};

    std::atomic<uint64_t> rows_pushed_{0};
    std::atomic<uint64_t> overflow_disconnects_{0};
    std::atomic<uint64_t> refused_{0};
};

} // namespace ob
