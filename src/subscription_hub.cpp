// SubscriptionHub: bounded per-subscriber queues, one eventfd, and the epoll loop as the only
// thread that touches a Session.

#include "orderbook/subscription_hub.hpp"

#include "orderbook/logger.hpp"
#include "orderbook/response_formatter.hpp"

#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <errno.h>
#include <iterator>
#include <string_view>

namespace ob {

SubscriptionHub::SubscriptionHub(size_t max_queue_bytes, int max_per_session)
    : max_queue_bytes_(max_queue_bytes), max_per_session_(max_per_session) {
    wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK);
    if (wakeup_fd_ < 0) {
        OB_LOG_ERROR("subscriptions", "eventfd failed: %s", std::strerror(errno));
    } else {
        OB_LOG_INFO("subscriptions",
                    "Hub ready: wakeup_fd=%d max_queue_bytes=%zu max_per_session=%d",
                    wakeup_fd_, max_queue_bytes_, max_per_session_);
    }
}

SubscriptionHub::~SubscriptionHub() {
    if (wakeup_fd_ >= 0) {
        ::close(wakeup_fd_);
        wakeup_fd_ = -1;
    }
}

void SubscriptionHub::wake() {
    const int fd = wakeup_fd_;
    if (fd < 0) return;
    // At most one write per pending batch. See the header: a 1000-level MINSERT would otherwise be a
    // thousand syscalls on the write path.
    bool expected = false;
    if (!wake_pending_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) return;
    const uint64_t one = 1;
    const ssize_t wr = ::write(fd, &one, sizeof(one));
    (void)wr;   // a full eventfd counter still means the loop will wake
}

uint64_t SubscriptionHub::add(Engine& engine, int fd, uint64_t conn_id, const std::string& sql,
                              std::string* error) {
    uint64_t hub_id = 0;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        const int already = static_cast<int>(std::count_if(
            queues_.begin(), queues_.end(),
            [&](const auto& entry) { return entry.second.fd == fd && entry.second.conn_id == conn_id; }));
        if (already >= max_per_session_) {
            refused_.fetch_add(1, std::memory_order_relaxed);
            if (error) {
                *error = "subscription limit reached for this session (" +
                         std::to_string(max_per_session_) + ")";
            }
            OB_LOG_WARN("subscriptions",
                        "Refused subscription on fd=%d conn=%llu: already at the limit of %d",
                        fd, static_cast<unsigned long long>(conn_id), max_per_session_);
            return 0;
        }
        hub_id = next_id_++;
        queues_.emplace(hub_id, SubscriberQueue{hub_id, fd, conn_id, /*engine_sub_id*/ 0, "",
                                                /*pending_rows*/ 0, /*pushed*/ 0, false});
    }

    // The id the callback needs is allocated above, before registering, and deliberately so. The
    // engine's own id is only known after `subscribe()` returns, and a callback capturing a slot to
    // be filled in afterwards can fire in the window between the two - the write path is another
    // thread. So the client-visible id is ours, and the engine's is recorded for cancellation.
    const uint64_t engine_id = engine.subscribe_batch(sql, [this, hub_id](const SubscriptionBatch& b) {
        enqueue_batch(hub_id, b);
    });

    if (engine_id == 0) {
        std::lock_guard<std::mutex> lock(mtx_);
        queues_.erase(hub_id);
        refused_.fetch_add(1, std::memory_order_relaxed);
        if (error) *error = "query does not parse as a subscription";
        OB_LOG_WARN("subscriptions", "Refused subscription on fd=%d: query does not parse: %s",
                    fd, sql.c_str());
        return 0;
    }

    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (auto it = queues_.find(hub_id); it != queues_.end()) it->second.engine_sub_id = engine_id;
    }
    OB_LOG_INFO("subscriptions",
                "Session fd=%d conn=%llu subscribed as %llu (engine id %llu): %s",
                fd, static_cast<unsigned long long>(conn_id),
                static_cast<unsigned long long>(hub_id),
                static_cast<unsigned long long>(engine_id), sql.c_str());
    return hub_id;
}

int SubscriptionHub::remove(Engine& engine, uint64_t id) {
    uint64_t engine_id = 0;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = queues_.find(id);
        if (it == queues_.end()) return 0;
        engine_id = it->second.engine_sub_id;
        queues_.erase(it);
    }
    // Outside the lock: `unsubscribe()` takes the engine's own lock, and holding two locks in an
    // order nothing else establishes is how a cycle gets built (#80).
    if (engine_id != 0) engine.unsubscribe(engine_id);
    OB_LOG_INFO("subscriptions", "Subscription %llu cancelled",
                static_cast<unsigned long long>(id));
    return 1;
}

int SubscriptionHub::remove_connection(Engine& engine, int fd, uint64_t conn_id) {
    std::vector<uint64_t> engine_ids;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        std::erase_if(queues_, [&](const auto& entry) {
            const SubscriberQueue& q = entry.second;
            if (q.fd != fd || q.conn_id != conn_id) return false;
            engine_ids.push_back(q.engine_sub_id);
            return true;
        });
    }
    for (uint64_t engine_id : engine_ids) {
        if (engine_id != 0) engine.unsubscribe(engine_id);
    }
    if (!engine_ids.empty()) {
        OB_LOG_INFO("subscriptions", "Session fd=%d conn=%llu left; %zu subscription(s) cancelled",
                    fd, static_cast<unsigned long long>(conn_id), engine_ids.size());
    }
    return static_cast<int>(engine_ids.size());
}

void SubscriptionHub::enqueue_batch(uint64_t subscription_id, const SubscriptionBatch& batch) {
    // The rows' text first, outside the lock: in the notification's scratch, made by the first
    // subscription a row reaches and read by the rest, so a row pushed to a thousand subscriptions is
    // formatted once (#263). The scratch is the notifying thread's alone.
    NotifyScratch& scratch = batch.scratch;
    if (scratch.offset.size() != batch.rows.size()) {
        scratch.offset.assign(batch.rows.size(), NotifyScratch::kNone);
        scratch.length.assign(batch.rows.size(), 0);
    }
    for (const uint32_t i : batch.allowed) {
        if (scratch.offset[i] != NotifyScratch::kNone) continue;
        char row[kPushRowRoom];
        const char* end = format_push_row(row, std::end(row), batch.rows[i]);
        // Bounded by the buffer as well as by the formatter: GCC 14 at -O3 cannot see the bound
        // through a pointer another unit returns, and warns of an unbounded copy (-Wrestrict).
        const size_t length = std::min(static_cast<size_t>(end - row), sizeof(row));
        scratch.offset[i] = static_cast<uint32_t>(scratch.bytes.size());
        scratch.length[i] = static_cast<uint32_t>(length);
        scratch.bytes.append(row, length);
    }
    char head[kPushHeadRoom];
    const size_t head_len = std::min(
        static_cast<size_t>(format_push_head(head, std::end(head), subscription_id) - head), sizeof(head));

    bool should_wake = false;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = queues_.find(subscription_id);
        // Gone between the notification starting and this line. Expected, not an error: a cancelled
        // subscription may still receive one write, which `QueryEngine::unsubscribe()` documents.
        if (it == queues_.end()) return;
        SubscriberQueue& q = it->second;
        if (q.overflowed) return;   // already condemned; nothing to add to

        for (const uint32_t i : batch.allowed) {
            const std::string_view text(scratch.bytes.data() + scratch.offset[i], scratch.length[i]);
            if (q.pending.size() + head_len + text.size() > max_queue_bytes_) {
                // Marked, not cleared, and the rest of the write is not added. See
                // SubscriberQueue::overflowed.
                q.overflowed = true;
                OB_LOG_ERROR("subscriptions",
                             "Subscription %llu overflowed at %zu bytes of %zu on fd=%d; the session "
                             "will be closed. The queue is left intact: taking back bytes the client "
                             "has partly read truncates their input instead of disconnecting them.",
                             static_cast<unsigned long long>(subscription_id), q.pending.size(),
                             max_queue_bytes_, q.fd);
                should_wake = true;   // the loop has to notice and close
                break;
            }
            if (q.pending.empty()) should_wake = true;
            q.pending.append(head, head_len);
            q.pending.append(text);
            ++q.pending_rows;
        }
    }
    if (should_wake) wake();
}

std::vector<int> SubscriptionHub::drain(SessionManager& sessions,
                                        const std::function<void(int)>& arm_epollout) {
    // Nothing woke the hub since the last drain: no queue has a row or an overflow, because the
    // first row into an empty queue and every overflow set the flag before the eventfd is written,
    // and the drain that moved the last bytes cleared it before moving them. A loop that polls -
    // `boost`, and the rings of #52 - comes here on every pass, and this is what keeps that pass to
    // one load rather than a read() and a lock (measured: the read alone was 6% of a polling loop's
    // cycles on the Graviton). A row whose wake() lands after this load is delivered on the next
    // pass.
    if (!wake_pending_.load(std::memory_order_acquire)) return {};

    // Drain the eventfd and clear the flag *before* moving any bytes. The other order loses the
    // wake-up for a row enqueued between the two.
    if (wakeup_fd_ >= 0) {
        uint64_t counter = 0;
        const ssize_t rd = ::read(wakeup_fd_, &counter, sizeof(counter));
        (void)rd;   // EAGAIN is normal: a plain epoll timeout also lands here
    }
    wake_pending_.store(false, std::memory_order_release);

    struct Ready {
        int         fd;
        std::string bytes;
        uint64_t    rows;
        bool        condemned;
    };
    std::vector<Ready> ready;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        for (auto& [id, q] : queues_) {
            if (q.overflowed) {
                ready.push_back(Ready{q.fd, "", 0, true});
                continue;
            }
            if (q.pending.empty()) continue;
            const uint64_t rows  = q.pending_rows;
            const size_t   taken = q.pending.size();
            ready.push_back(Ready{q.fd, std::move(q.pending), rows, false});
            q.pending.clear();
            // The bytes moved out took their room with them: room for as much again, made here on the
            // subscriber's loop, so the writes before the next drain append into it rather than grow
            // a string from nothing, a reallocation at a time, on the writer's thread (#263). Up to
            // kPendingRoom, so that one burst does not leave every subscription holding its size.
            q.pending.reserve(std::min(taken, kPendingRoom));
            q.pending_rows = 0;
            q.pushed += rows;
        }
    }

    std::vector<int> to_close;
    for (auto& item : ready) {
        if (item.condemned) {
            overflow_disconnects_.fetch_add(1, std::memory_order_relaxed);
            to_close.push_back(item.fd);
            continue;
        }
        Session* session = sessions.get_session(item.fd);
        if (session == nullptr) {
            // The session went away between the enqueue and here. The bytes are dropped, which is
            // correct: there is nobody to send them to, and the subscription is about to be
            // cancelled by close_session() if it has not been already.
            continue;
        }
        if (!session->send_response(item.bytes)) {
            OB_LOG_WARN("subscriptions", "Push to fd=%d failed; closing", item.fd);
            to_close.push_back(item.fd);
            continue;
        }
        rows_pushed_.fetch_add(item.rows, std::memory_order_relaxed);
        if (session->has_pending_output()) arm_epollout(item.fd);
    }
    return to_close;
}

size_t SubscriptionHub::active() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return queues_.size();
}

size_t SubscriptionHub::queued_bytes() const {
    std::lock_guard<std::mutex> lock(mtx_);
    size_t total = 0;
    for (const auto& [id, q] : queues_) total += q.pending.size();
    return total;
}

} // namespace ob
