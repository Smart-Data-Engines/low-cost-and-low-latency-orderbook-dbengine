#pragma once

// ── AntiEntropyManager — periodic gap detection and repair ───────────────────
//
// Ensures eventual consistency between multi-master nodes by periodically
// comparing local WAL position with peer-published positions from etcd.
// Detected gaps are repaired via WAL catch-up or, when WAL is truncated,
// via full snapshot synchronization.
//
// Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6

#include "orderbook/hlc.hpp"
#include "orderbook/peer_registry.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace ob {

class Engine;  // forward — full integration comes in task 12

// ── Anti-entropy configuration ────────────────────────────────────────────────

struct AntiEntropyConfig {
    uint32_t interval_seconds{30};              // --anti-entropy-interval-seconds
    size_t   max_repair_bytes{64ULL << 20};     // max bytes to repair per run (64MB)
};

// ── Result of a single anti-entropy run ───────────────────────────────────────

struct AntiEntropyResult {
    uint64_t run_id{0};
    uint64_t timestamp_ns{0};
    size_t   peers_checked{0};
    size_t   gaps_detected{0};
    size_t   gaps_repaired{0};
    size_t   bytes_transferred{0};
    bool     snapshot_triggered{false};
};

// ── Gap information ───────────────────────────────────────────────────────────

struct GapInfo {
    uint16_t peer_node_id{0};
    uint32_t from_file{0};
    size_t   from_offset{0};
    uint32_t to_file{0};
    size_t   to_offset{0};
};

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

    /// Compare local WAL position with peer-published positions.
    /// Returns a GapInfo for each peer that is ahead of the local node.
    std::vector<GapInfo> detect_gaps(const std::vector<PeerInfo>& peers);

    /// Request missing WAL records from a peer (placeholder — full networking in MultiMasterManager).
    bool repair_gap(const GapInfo& gap);

    /// Trigger full snapshot synchronization when WAL repair is not possible.
    bool trigger_snapshot_repair(uint16_t peer_node_id);
};

} // namespace ob
