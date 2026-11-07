#pragma once

// Large reads answered off the client loop (#260).
//
// A client loop runs a `SELECT` to its last byte before it serves another session: a million rows
// held the sessions beside it 11-23 ms on the m9g.xlarge, and a refused LZ4 answer up to 288 ms. A read
// whose scan the index puts past a threshold is handed to these threads instead, with what its session
// needs to have it answered - its descriptor and connection id, and the ceiling its answer is measured
// against - and the loop goes on. The answer comes back through the loop's mailbox, which the loop
// reads once a pass.

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace ob {

/// A large read's answer, or a FLUSH's (#261), on its way back to the loop that took it.
struct OffLoopAnswer {
    int         fd{-1};
    uint64_t    conn_id{0};
    std::string answer;            ///< what the loop queues: text, a refusal, or an LZ4 frame
    bool        framed{false};     ///< an LZ4 frame already (#257)
    uint64_t    text_bytes{0};     ///< the text a frame holds, for the session's counters
    bool        counted{false};    ///< a query that ran, which the session's counters take
    bool        flush{false};      ///< a FLUSH's answer, not a read's (#261)
};

/// Where answers come back to one client loop: a list under a mutex and an eventfd of its own in the
/// loop's epoll set. Shared by the loop and every read it handed out, so that a read finishing after
/// its loop has gone writes to a mailbox nobody reads, never to a descriptor closed under it.
class OffLoopMailbox {
public:
    OffLoopMailbox();
    ~OffLoopMailbox();
    OffLoopMailbox(const OffLoopMailbox&) = delete;
    OffLoopMailbox& operator=(const OffLoopMailbox&) = delete;

    /// The eventfd the loop adds to its epoll set, edge-triggered: each post is an event, and a
    /// counter take() left unread wakes nobody again.
    int event_fd() const { return event_fd_; }
    /// An answer in, and the loop woken.
    void post(OffLoopAnswer answer);
    /// Every answer in so far, taken; the eventfd read down. With none posted, nothing: one load,
    /// and no system call - the loop asks every pass, its spins included.
    std::vector<OffLoopAnswer> take();

private:
    std::mutex                 mtx_;
    std::vector<OffLoopAnswer> answers_;
    /// The answers in `answers_`, moved under `mtx_` and read without it as take()'s first question.
    std::atomic<size_t>        posted_{0};
    int                        event_fd_{-1};
};

/// The threads large reads run on (#260): FIFO, each read to its end. One read a session at a time,
/// so `--max-sessions` bounds the queue.
class OffLoopPool {
public:
    explicit OffLoopPool(unsigned threads);
    /// The reads queued are dropped and the ones running finish; then the threads are joined. Built
    /// after the engine and destroyed before it.
    ~OffLoopPool();
    OffLoopPool(const OffLoopPool&) = delete;
    OffLoopPool& operator=(const OffLoopPool&) = delete;

    void submit(std::function<void()> read);
    unsigned threads() const { return static_cast<unsigned>(threads_.size()); }
    /// Reads waiting for a thread, for the log and a test.
    size_t queued() const;

private:
    void run(unsigned index);

    mutable std::mutex                mtx_;
    std::condition_variable           cv_;
    std::deque<std::function<void()>> queue_;
    bool                              stopping_{false};
    std::vector<std::thread>          threads_;
};

/// The fewest rows the index puts in a read's scan for it to be answered off the loop (#260): about
/// 4.5 MB of `SELECT *` text and a few milliseconds of formatting, below which handing it to a thread
/// would cost more than it saves the sessions beside it.
inline constexpr uint64_t kOffLoopReadRows = 100'000;

}  // namespace ob
