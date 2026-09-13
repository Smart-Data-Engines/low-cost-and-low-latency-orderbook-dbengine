#pragma once

// ── AntiEntropyManager — reconciliation on a timer ───────────────────────────
//
// Eventual consistency between multi-master nodes, by periodically exchanging version vectors
// with every connected peer. Receiving a peer's vector already makes a node stream what that
// peer lacks (see MultiMasterManager::start_catchup_to_peer), so reconciliation needs no
// protocol of its own: sending our vector is the repair.
//
// What this replaced: comparing the local WAL position with peer positions published in etcd,
// and describing gaps as WAL file indices and byte offsets. Two independent WALs have no common
// scale — that model is what roadmap #61 measured as data loss — and detect_gaps() returned an
// empty list unconditionally while the metrics reported runs, which read as "checked, nothing to
// repair". The scheduler was never even constructed (#68).
//
// The work is injected as a function rather than a reference to MultiMasterManager, which owns
// this object: a reference back would be a cycle, and a function makes a run testable with no
// cluster, no etcd and no ports.

#include "orderbook/hlc.hpp"
#include "orderbook/peer_registry.hpp"
#include "orderbook/version_vector.hpp"

#include <functional>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace ob {

class Engine;  // forward — full integration comes in task 12

// ── Anti-entropy configuration ────────────────────────────────────────────────

struct AntiEntropyConfig {
    uint32_t interval_seconds{30};              // --anti-entropy-interval-seconds
};

// ── Result of a single anti-entropy run ───────────────────────────────────────

struct AntiEntropyResult {
    uint64_t run_id{0};
    uint64_t timestamp_ns{0};
    size_t   peers_checked{0};
    size_t   gaps_detected{0};   ///< pairs where the two sides disagree, both directions
    size_t   we_lack{0};         ///< of those, the ones where this node is behind
    size_t   gaps_closed{0};     ///< gaps this node was behind on last run and is not now
    size_t   vectors_sent{0};
    bool     reconciler_missing{false};  ///< no work was possible, which is not "nothing to do"
};

// ── What one reconciliation pass found and did ────────────────────────────────
//
// Gaps are (symbol, origin, sequence range), not WAL offsets: a sequence number minted by an
// origin means the same thing on every node that received it, and a byte offset does not.

/// The largest number of records any single peer is known to be missing, ignoring peers whose
/// position is unknown.
///
/// The mesh's honest lag, and the reason it is in **records** rather than bytes: sequence numbers
/// are per-origin and origin-stamped, which is the mechanism #61's fix introduced, so they are
/// comparable across nodes. Byte offsets are not — each node's WAL holds its own client writes as
/// well as what it replicated — and #118 is the item where subtracting them produced this node's
/// own WAL size.
///
/// Max across peers rather than a sum, because the question an operator asks is "is anybody
/// behind", and a sum lets two peers each missing one record read the same as one peer missing
/// two. Peers listed in `unknown` contribute nothing at all: `compare_vectors()` reports a peer
/// that has said nothing as lacking everything, on purpose, so counting it would make silence the
/// largest lag in the mesh.
///
/// A free function on the report's own vectors so that the ordinary suite can state these
/// properties without a mesh — the same answer #117 gave for arithmetic whose caller is hard to
/// reach.
uint64_t max_records_behind(const std::vector<VectorGap>& peer_lacks,
                            const std::vector<uint16_t>& unknown);

struct ReconcileReport {
    size_t peers_contacted{0};
    size_t vectors_sent{0};
    std::vector<VectorGap> we_lack;
    std::vector<VectorGap> peer_lacks;

    /// Peers that have not said what they hold, by node id.
    ///
    /// Carried as ids rather than as a count because the records lag has to **exclude** them and
    /// a count cannot say which gaps to skip. `compare_vectors()` treats a peer that has said
    /// nothing as holding nothing — deliberately, since that is the safe direction for the repair
    /// — so its `peer_lacks` is everything this node holds. Counting that as lag would report a
    /// peer which has merely not spoken yet as the furthest behind in the mesh (#118).
    std::vector<uint16_t> peers_position_unknown;
};

/// Performs one reconciliation pass and reports what it saw. Supplied by MultiMasterManager.
using ReconcileFn = std::function<ReconcileReport()>;

// ── AntiEntropyManager ────────────────────────────────────────────────────────

class AntiEntropyManager {
public:
    explicit AntiEntropyManager(AntiEntropyConfig config, Engine& engine,
                                PeerRegistry& registry);
    ~AntiEntropyManager();

    AntiEntropyManager(const AntiEntropyManager&) = delete;
    AntiEntropyManager& operator=(const AntiEntropyManager&) = delete;

    /// Start the periodic anti-entropy loop.
    void start();

    /// Stop the anti-entropy loop (wakes the sleeping thread immediately).
    void stop();

    /// Trigger an immediate anti-entropy run (for testing/diagnostics).
    AntiEntropyResult run_now();

    /// Get total number of runs completed.
    uint64_t total_runs() const { return total_runs_.load(std::memory_order_relaxed); }

    /// Get total number of repairs performed.
    uint64_t total_repairs() const { return total_repairs_.load(std::memory_order_relaxed); }

    /// Get the result of the last run.
    AntiEntropyResult last_result() const;

    /// Install the function that performs a pass. Without it, a run does nothing and says so.
    void set_reconciler(ReconcileFn fn);

private:
    AntiEntropyConfig config_;
    Engine& engine_;
    PeerRegistry& registry_;

    std::thread thread_;
    std::atomic<bool> running_{false};
    std::atomic<uint64_t> total_runs_{0};
    std::atomic<uint64_t> total_repairs_{0};

    mutable std::mutex result_mtx_;
    AntiEntropyResult last_result_;

    // Condition variable for clean shutdown (instead of plain sleep).
    std::mutex loop_mtx_;
    std::condition_variable loop_cv_;

    /// Periodic loop: sleeps for interval_seconds, then calls execute_run().
    /// Uses condition_variable so stop() can wake it immediately.
    void loop();

    /// Execute a single anti-entropy run.
    AntiEntropyResult execute_run();

    /// Gaps this node was behind on during the previous run, as "key|origin" keys.
    ///
    /// Closure is measured, not assumed: a repair counts when the difference is gone on the next
    /// pass, not when a request was sent. A metric that counts requests measures diligence.
    std::set<std::string> previous_we_lack_;

    mutable std::mutex reconciler_mtx_;
    ReconcileFn reconciler_;
};

} // namespace ob
