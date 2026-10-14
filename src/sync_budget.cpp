// #190, step 9: how much WAL may wait for a sync. See include/orderbook/sync_budget.hpp.
#include "orderbook/sync_budget.hpp"

#include <algorithm>

namespace ob {

namespace {

using Seconds = std::chrono::duration<double>;

}  // namespace

SyncBudget::SyncBudget(Config config)
    : config_(config),
      bytes_(std::clamp(config.initial_bytes, config.floor_bytes, config.ceiling_bytes)) {}

SyncBudget::Change SyncBudget::on_sync(uint64_t bytes, Clock::duration took) {
    if (bytes == 0) return Change::None;   // nothing written back, nothing learned
    std::lock_guard<std::mutex> lock(mtx_);
    const uint64_t budget = bytes_.load(std::memory_order_relaxed);
    // Whether the sync was of enough to say anything about what the device takes: at least half
    // the budget. Less, and only a lower budget is believed - a sync that was slow for its size is
    // the device being busy whatever it was asked for.
    const bool used = bytes >= budget / 2;
    const uint64_t doubled = std::min(config_.ceiling_bytes, budget * 2);

    uint64_t next = budget;
    if (Seconds(took) <= Seconds(config_.target) / 4) {
        if (used) next = doubled;
    } else {
        // What the device writes in `target` at the rate this sync measured. The same ×2 bound on
        // the way up as a quick sync has, so that the two rules meet where the sync took half the
        // target, and one sync cannot take the budget to the ceiling.
        const double rate = static_cast<double>(bytes) / Seconds(took).count();
        const double wanted = rate * Seconds(config_.target).count();
        const uint64_t measured = static_cast<uint64_t>(std::clamp(
            wanted, static_cast<double>(config_.floor_bytes), static_cast<double>(config_.ceiling_bytes)));
        if (measured < budget) next = measured;
        else if (used) next = std::min(measured, doubled);
    }

    if (next == budget) return Change::None;
    bytes_.store(next, std::memory_order_relaxed);
    return next < budget ? Change::Lowered : Change::Raised;
}

} // namespace ob
