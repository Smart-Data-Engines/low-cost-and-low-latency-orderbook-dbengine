// ── AntiEntropyManager implementation ─────────────────────────────────────────
//
// Periodic gap detection and repair for multi-master eventual consistency.
// Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6

#include "orderbook/anti_entropy.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/logger.hpp"

#include <chrono>

namespace ob {

// ── Constructor / Destructor ──────────────────────────────────────────────────

AntiEntropyManager::AntiEntropyManager(AntiEntropyConfig config,
                                       Engine& engine,
                                       PeerRegistry& registry)
    : config_(config), engine_(engine), registry_(registry) {
    OB_LOG_DEBUG("anti_entropy",
                 "AntiEntropyManager created: interval=%u max_repair_bytes=%zu",
                 config_.interval_seconds, config_.max_repair_bytes);
}

AntiEntropyManager::~AntiEntropyManager() {
    stop();
}

// ── start ─────────────────────────────────────────────────────────────────────

void AntiEntropyManager::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) {
        OB_LOG_WARN("anti_entropy", "AntiEntropyManager already running");
        return;
    }

    OB_LOG_INFO("anti_entropy",
                "Starting anti-entropy loop: interval=%u seconds",
                config_.interval_seconds);

    thread_ = std::thread([this] { loop(); });
}

// ── stop ──────────────────────────────────────────────────────────────────────

void AntiEntropyManager::stop() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false)) {
        return;  // already stopped or never started
    }

    OB_LOG_INFO("anti_entropy", "Stopping anti-entropy loop");

    // Wake the sleeping thread immediately via condition variable.
    {
        std::lock_guard<std::mutex> lock(loop_mtx_);
        loop_cv_.notify_one();
    }

    if (thread_.joinable()) {
        thread_.join();
    }

    OB_LOG_INFO("anti_entropy", "Anti-entropy loop stopped");
}

// ── run_now ───────────────────────────────────────────────────────────────────

AntiEntropyResult AntiEntropyManager::run_now() {
    OB_LOG_INFO("anti_entropy", "Manual anti-entropy run triggered");
    return execute_run();
}

// ── last_result ───────────────────────────────────────────────────────────────

AntiEntropyResult AntiEntropyManager::last_result() const {
    std::lock_guard<std::mutex> lock(result_mtx_);
    return last_result_;
}

// ── loop ──────────────────────────────────────────────────────────────────────

void AntiEntropyManager::loop() {
    OB_LOG_DEBUG("anti_entropy", "Anti-entropy loop thread started");

    while (running_.load(std::memory_order_relaxed)) {
        // Sleep for interval_seconds, but wake immediately on stop().
        {
            std::unique_lock<std::mutex> lock(loop_mtx_);
            loop_cv_.wait_for(lock,
                              std::chrono::seconds(config_.interval_seconds),
                              [this] { return !running_.load(std::memory_order_relaxed); });
        }

        if (!running_.load(std::memory_order_relaxed)) {
            break;
        }

        execute_run();
    }

    OB_LOG_DEBUG("anti_entropy", "Anti-entropy loop thread exiting");
}

// ── execute_run ───────────────────────────────────────────────────────────────

AntiEntropyResult AntiEntropyManager::execute_run() {
    const uint64_t run_id = total_runs_.load(std::memory_order_relaxed) + 1;
    const auto now_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());

    AntiEntropyResult result{};
    result.run_id = run_id;
    result.timestamp_ns = now_ns;

    // Fetch current peer list from registry.
    auto peers = registry_.get_peers();
    result.peers_checked = peers.size();

    if (peers.empty()) {
        OB_LOG_DEBUG("anti_entropy", "Run #%lu: no peers to check",
                     static_cast<unsigned long>(run_id));
        total_runs_.fetch_add(1, std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> lock(result_mtx_);
            last_result_ = result;
        }
        return result;
    }

    // Detect gaps between local WAL and peer positions.
    auto gaps = detect_gaps(peers);
    result.gaps_detected = gaps.size();

    // Attempt to repair each gap.
    size_t bytes_transferred = 0;
    for (const auto& gap : gaps) {
        if (bytes_transferred >= config_.max_repair_bytes) {
            OB_LOG_WARN("anti_entropy",
                        "Run #%lu: max_repair_bytes reached (%zu), "
                        "deferring remaining gaps",
                        static_cast<unsigned long>(run_id),
                        config_.max_repair_bytes);
            break;
        }

        // If the gap spans different WAL files, the WAL may have been
        // truncated — fall back to snapshot repair.
        if (gap.from_file != gap.to_file) {
            OB_LOG_WARN("anti_entropy",
                        "WAL truncated for peer %u, triggering snapshot repair",
                        static_cast<unsigned>(gap.peer_node_id));
            if (trigger_snapshot_repair(gap.peer_node_id)) {
                result.snapshot_triggered = true;
                result.gaps_repaired++;
                total_repairs_.fetch_add(1, std::memory_order_relaxed);
            }
        } else {
            if (repair_gap(gap)) {
                result.gaps_repaired++;
                total_repairs_.fetch_add(1, std::memory_order_relaxed);
                // Estimate bytes transferred from gap size.
                if (gap.to_offset > gap.from_offset) {
                    bytes_transferred += (gap.to_offset - gap.from_offset);
                }
            }
        }
    }

    result.bytes_transferred = bytes_transferred;

    OB_LOG_INFO("anti_entropy",
                "Run #%lu: checked %zu peers, %zu gaps detected, %zu repaired",
                static_cast<unsigned long>(run_id),
                result.peers_checked,
                result.gaps_detected,
                result.gaps_repaired);

    total_runs_.fetch_add(1, std::memory_order_relaxed);

    // Update Prometheus metrics.
    engine_.registry().increment_counter("ob_mm_anti_entropy_runs_total");
    if (result.gaps_repaired > 0) {
        engine_.registry().increment_counter("ob_mm_anti_entropy_repairs_total",
                                             result.gaps_repaired);
    }

    {
        std::lock_guard<std::mutex> lock(result_mtx_);
        last_result_ = result;
    }

    return result;
}

// ── detect_gaps ───────────────────────────────────────────────────────────────

std::vector<GapInfo>
AntiEntropyManager::detect_gaps(const std::vector<PeerInfo>& peers) {
    std::vector<GapInfo> gaps;

    // Placeholder: In the full implementation, we would compare the local
    // WAL position (from Engine) with each peer's published position.
    // For now, we log and return an empty list — the actual WAL position
    // comparison requires Engine integration (task 12).

    for (const auto& peer : peers) {
        OB_LOG_DEBUG("anti_entropy",
                     "Checking peer %u: wal_file=%u wal_offset=%zu",
                     static_cast<unsigned>(peer.node_id),
                     peer.wal_file_index,
                     peer.wal_byte_offset);

        // TODO(task-12): Compare with engine_.wal().current_file_index()
        // and engine_.wal().current_offset() to detect actual gaps.
        // For now, no gaps are detected until Engine integration is complete.
    }

    return gaps;
}

// ── repair_gap ────────────────────────────────────────────────────────────────

bool AntiEntropyManager::repair_gap(const GapInfo& gap) {
    // Placeholder: In the full implementation, this would send a CATCHUP
    // request to the peer and replay the missing WAL records.
    // Full networking integration comes with MultiMasterManager.

    OB_LOG_WARN("anti_entropy",
                "Gap detected: peer=%u from={%u,%zu} to={%u,%zu}",
                static_cast<unsigned>(gap.peer_node_id),
                gap.from_file, gap.from_offset,
                gap.to_file, gap.to_offset);

    OB_LOG_INFO("anti_entropy",
                "Repair gap for peer %u: placeholder — "
                "full networking in MultiMasterManager",
                static_cast<unsigned>(gap.peer_node_id));

    // Return false since we cannot actually repair yet.
    return false;
}

// ── trigger_snapshot_repair ───────────────────────────────────────────────────

bool AntiEntropyManager::trigger_snapshot_repair(uint16_t peer_node_id) {
    // Placeholder: In the full implementation, this would initiate a full
    // snapshot transfer from the peer when WAL-based repair is not possible.

    OB_LOG_WARN("anti_entropy",
                "WAL truncated for peer %u, triggering snapshot repair",
                static_cast<unsigned>(peer_node_id));

    OB_LOG_INFO("anti_entropy",
                "Snapshot repair for peer %u: placeholder — "
                "full networking in MultiMasterManager",
                static_cast<unsigned>(peer_node_id));

    // Return false since we cannot actually perform snapshot repair yet.
    return false;
}

} // namespace ob
