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
    OB_LOG_DEBUG("anti_entropy", "AntiEntropyManager created: interval=%us",
                 config_.interval_seconds);
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

void AntiEntropyManager::set_reconciler(ReconcileFn fn) {
    std::lock_guard<std::mutex> lock(reconciler_mtx_);
    reconciler_ = std::move(fn);
    OB_LOG_INFO("anti_entropy", "Reconciler installed");
}

AntiEntropyResult AntiEntropyManager::execute_run() {
    const uint64_t run_id = total_runs_.load(std::memory_order_relaxed) + 1;
    const auto now_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());

    AntiEntropyResult result{};
    result.run_id       = run_id;
    result.timestamp_ns = now_ns;

    ReconcileFn fn;
    {
        std::lock_guard<std::mutex> lock(reconciler_mtx_);
        fn = reconciler_;
    }

    if (!fn) {
        // No work is possible, and that is a different statement from "nothing to repair". The
        // previous version of this class returned an empty gap list unconditionally and let the
        // run counter tick, which read as a clean bill of health for months.
        result.reconciler_missing = true;
        OB_LOG_WARN("anti_entropy",
                    "Run #%llu: no reconciler installed, so nothing was checked",
                    static_cast<unsigned long long>(run_id));
        total_runs_.fetch_add(1, std::memory_order_relaxed);
        engine_.registry().increment_counter("ob_mm_anti_entropy_runs_total");
        {
            std::lock_guard<std::mutex> lock(result_mtx_);
            last_result_ = result;
        }
        return result;
    }

    const ReconcileReport report = fn();
    result.peers_checked = report.peers_contacted;
    result.vectors_sent  = report.vectors_sent;
    result.gaps_detected = report.we_lack.size() + report.peer_lacks.size();
    result.we_lack       = report.we_lack.size();

    // Closure, measured against the previous pass. A gap we were behind on and are not behind on
    // any more was repaired; one that is still there was not, however many vectors we sent.
    std::set<std::string> now_we_lack;
    for (const auto& gap : report.we_lack) {
        now_we_lack.insert(gap.key + "|" + std::to_string(gap.origin) + "|" +
                           std::to_string(gap.peer_node_id));
    }
    for (const auto& previous : previous_we_lack_) {
        if (now_we_lack.count(previous) == 0) ++result.gaps_closed;
    }
    previous_we_lack_ = std::move(now_we_lack);

    total_runs_.fetch_add(1, std::memory_order_relaxed);
    total_repairs_.fetch_add(result.gaps_closed, std::memory_order_relaxed);

    engine_.registry().increment_counter("ob_mm_anti_entropy_runs_total");
    if (result.gaps_closed > 0) {
        engine_.registry().increment_counter("ob_mm_anti_entropy_repairs_total",
                                             static_cast<int64_t>(result.gaps_closed));
    }
    engine_.registry().set_gauge("ob_mm_reconcile_gaps_detected",
                                 static_cast<int64_t>(result.gaps_detected));
    engine_.registry().set_gauge("ob_mm_reconcile_we_lack",
                                 static_cast<int64_t>(result.we_lack));

    OB_LOG_INFO("anti_entropy",
                "Run #%llu: peers=%zu vectors_sent=%zu gaps=%zu (we_lack=%zu) closed_since_last=%zu",
                static_cast<unsigned long long>(run_id), result.peers_checked,
                result.vectors_sent, result.gaps_detected, result.we_lack, result.gaps_closed);

    // The individual gaps at DEBUG: an operator chasing divergence wants the symbol and the
    // range, not just a count.
    for (const auto& gap : report.we_lack) {
        OB_LOG_DEBUG("anti_entropy", "Behind peer %u on %s origin=%u: missing %llu..%llu",
                     gap.peer_node_id, gap.key.c_str(), gap.origin,
                     static_cast<unsigned long long>(gap.from_seq),
                     static_cast<unsigned long long>(gap.to_seq));
    }
    for (const auto& gap : report.peer_lacks) {
        OB_LOG_DEBUG("anti_entropy", "Peer %u behind us on %s origin=%u: missing %llu..%llu",
                     gap.peer_node_id, gap.key.c_str(), gap.origin,
                     static_cast<unsigned long long>(gap.from_seq),
                     static_cast<unsigned long long>(gap.to_seq));
    }

    {
        std::lock_guard<std::mutex> lock(result_mtx_);
        last_result_ = result;
    }
    return result;
}
} // namespace ob
