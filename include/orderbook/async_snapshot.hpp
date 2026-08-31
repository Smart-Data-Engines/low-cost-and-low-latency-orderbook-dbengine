#pragma once

// ── AsyncSnapshotBuilder — one snapshot at a time, on a thread of its own ─────
//
// Creating a snapshot is a flush of the whole store plus a CRC32C pass over every columnar file, so
// the work grows with the store. Until #79 it ran on whichever thread asked for it, and both
// askers are io loops: `MultiMasterManager::io_loop()`, which also carries live deltas, catch-up
// and peer handshakes, and `ReplicationManager::run_loop()`. Measured at 4.1 ms for 2.37 MB after
// the first half of #79 — which puts a gigabyte at about 1.7 seconds of a loop that answers
// nothing while it waits.
//
// This class moves that work to a short-lived worker and hands the result back through a
// notification whose only job is to wake the owner's loop. The owner then collects the result from
// its own thread, so every field it touches still has exactly one owner at any moment. That
// property is the point: the bug class that cross-thread state invites is the one ThreadSanitizer
// found twice in this repo already (#37, #80).

#include "orderbook/snapshot.hpp"

#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

namespace ob {

/// start(), take_result() and shutdown() are meant to be called from one thread — the owner's loop,
/// and its stop() path after that loop has been joined. They are individually safe, but two
/// collectors racing to take the same result is not a scenario this class is built for, because it
/// is not one either owner has.
class AsyncSnapshotBuilder {
public:
    struct Result {
        /// Echoes the token given to start(), so a caller can tell whose result this is.
        uint64_t                  token{0};
        bool                      ok{false};
        /// Set when !ok — what the producer threw.
        std::string               error;
        /// Valid when ok.
        SnapshotWithSequenceState snap;
    };

    /// Runs on the worker thread. Whatever it throws becomes `Result::error`.
    using Producer = std::function<SnapshotWithSequenceState()>;

    /// Runs on the worker thread, after the result has been published, and must do nothing but
    /// wake the owner's loop.
    ///
    /// One constraint: it must not block, because it delays the worker's exit and therefore every
    /// join. Writing to an eventfd satisfies it.
    ///
    /// It may call back into this object. That is only true because no method here holds the
    /// internal mutex across a join — the first version of shutdown() did, and deadlocked against
    /// a worker that needed the same mutex to publish its result.
    using Notify = std::function<void()>;

    explicit AsyncSnapshotBuilder(Notify notify);

    /// Joins the worker. A snapshot in flight is waited for, not cancelled — see shutdown().
    ~AsyncSnapshotBuilder();

    AsyncSnapshotBuilder(const AsyncSnapshotBuilder&)            = delete;
    AsyncSnapshotBuilder& operator=(const AsyncSnapshotBuilder&) = delete;

    /// Starts creating a snapshot. Returns false when one is already in flight **or** when a
    /// finished result has not been collected yet — the caller refuses the request rather than
    /// queueing it, because two concurrent flush-and-checksum passes would double the cost this
    /// class exists to avoid.
    bool start(uint64_t token, Producer produce);

    /// True from start() until take_result() has handed the result over.
    [[nodiscard]] bool busy() const;

    /// Collects a finished result, or nothing while the worker is still running.
    ///
    /// Joins the worker before returning, so the caller acts on the result with no worker alive to
    /// race against. The join does not wait for the snapshot: the worker publishes first and the
    /// only thing it has left to do is return.
    std::optional<Result> take_result();

    /// Joins the worker and drops any result. Idempotent.
    ///
    /// There is no cancellation. The producer is a flush plus a checksum pass, and abandoning
    /// either half-way is worse than waiting for work whose result is about to be thrown away.
    void shutdown();

private:
    Notify             notify_;
    mutable std::mutex mtx_;
    std::thread        worker_;
    bool               running_{false};   // worker started and not yet finished
    bool               done_{false};      // result_ is filled and uncollected
    Result             result_;
};

}  // namespace ob
