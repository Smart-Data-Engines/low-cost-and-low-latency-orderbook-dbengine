#include "orderbook/failover.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

namespace ob {

// ── Construction / destruction ───────────────────────────────────────────────

FailoverManager::FailoverManager(FailoverConfig config,
                                 RoleTransitionHandler& handler)
    : config_(std::move(config))
    , handler_(handler)
{}

FailoverManager::~FailoverManager() {
    stop();
}

// ── start() ─────────────────────────────────────────────────────────────────

void FailoverManager::start() {
    if (running_.load()) return;

    // Create and connect the coordinator client.
    coordinator_ = std::make_unique<CoordinatorClient>(config_.coordinator);
    if (!coordinator_->connect()) {
        std::fprintf(stderr, "[failover] WARNING: cannot connect to coordinator, "
                     "operating in standalone mode\n");
        role_.store(NodeRole::STANDALONE);
        return;
    }

    // Determine initial role from cluster state.
    auto state = coordinator_->get_cluster_state();
    if (state.has_value()) {
        reconcile_epoch(*state);

        if (state->leader_node_id.empty()) {
            // No leader — attempt promotion if failover is enabled.
            if (config_.failover_enabled) {
                attempt_promotion();
            }
        } else if (state->leader_node_id == config_.coordinator.node_id) {
            // We are the leader.
            role_.store(NodeRole::PRIMARY);
            {
                std::lock_guard<std::mutex> lk(mtx_);
                primary_address_ = config_.replication_address;
            }
        } else {
            // Someone else is the leader — we are a replica.
            role_.store(NodeRole::REPLICA);
            {
                std::lock_guard<std::mutex> lk(mtx_);
                primary_address_ = state->leader_address;
            }
        }
    } else if (config_.failover_enabled) {
        // Could not read cluster state — try to become primary.
        attempt_promotion();
    }

    // Start the background monitor thread.
    running_.store(true);
    monitor_thread_ = std::thread([this] { monitor_loop(); });
}

// ── stop() ──────────────────────────────────────────────────────────────────

void FailoverManager::stop() {
    if (!running_.exchange(false)) return;

    // If we are PRIMARY, revoke our lease so replicas can detect expiry quickly.
    if (role_.load() == NodeRole::PRIMARY && coordinator_ && lease_id_ != 0) {
        coordinator_->revoke_lease(lease_id_);
    }

    // Join the monitor thread.
    if (monitor_thread_.joinable()) {
        monitor_thread_.join();
    }

    // Disconnect from coordinator.
    if (coordinator_) {
        coordinator_->stop_watch();
        coordinator_->disconnect();
    }
}

// ── Accessors ───────────────────────────────────────────────────────────────

NodeRole FailoverManager::role() const {
    return role_.load(std::memory_order_acquire);
}

EpochValue FailoverManager::epoch() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return epoch_;
}

std::string FailoverManager::primary_address() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return primary_address_;
}

int64_t FailoverManager::lease_ttl_remaining() const {
    if (role_.load() != NodeRole::PRIMARY) return 0;

    std::lock_guard<std::mutex> lk(mtx_);
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
        now - last_lease_refresh_);
    int64_t remaining = config_.coordinator.lease_ttl_seconds - elapsed.count();
    return remaining > 0 ? remaining : 0;
}


// ── initiate_graceful_failover() ────────────────────────────────────────────

bool FailoverManager::initiate_graceful_failover(
        const std::string& /*target_node_id*/) {
    if (role_.load() != NodeRole::PRIMARY) return false;
    if (!coordinator_ || lease_id_ == 0) return false;

    // Revoke our lease — replicas will detect the expiry and compete.
    bool revoked = coordinator_->revoke_lease(lease_id_);
    if (!revoked) return false;

    {
        std::lock_guard<std::mutex> lk(mtx_);
        lease_id_ = 0;
    }

    role_.store(NodeRole::REPLICA);

    // Discover the new primary from the coordinator.
    auto state = coordinator_->get_cluster_state();
    if (state.has_value() && !state->leader_address.empty()) {
        std::lock_guard<std::mutex> lk(mtx_);
        primary_address_ = state->leader_address;
        handler_.demote_to_replica(state->leader_address);
    }

    return true;
}

// ── monitor_loop() ──────────────────────────────────────────────────────────

void FailoverManager::monitor_loop() {
    while (running_.load(std::memory_order_acquire)) {
        NodeRole current = role_.load();

        if (current == NodeRole::PRIMARY) {
            // Refresh lease every TTL/3 seconds.
            auto now = std::chrono::steady_clock::now();
            auto since_refresh = std::chrono::duration_cast<std::chrono::seconds>(
                now - last_lease_refresh_);
            int64_t refresh_interval = config_.coordinator.lease_ttl_seconds / 3;
            if (refresh_interval < 1) refresh_interval = 1;

            if (since_refresh.count() >= refresh_interval) {
                if (coordinator_ && lease_id_ != 0) {
                    bool ok = coordinator_->refresh_lease(lease_id_);
                    if (ok) {
                        std::lock_guard<std::mutex> lk(mtx_);
                        last_lease_refresh_ = std::chrono::steady_clock::now();
                    } else {
                        handle_primary_lease_lost();
                    }
                }
            }
        } else if (current == NodeRole::REPLICA) {
            // Poll cluster state every 2 seconds to detect leader changes.
            if (coordinator_) {
                auto state = coordinator_->get_cluster_state();
                if (state.has_value()) {
                    reconcile_epoch(*state);

                    if (state->leader_node_id.empty()) {
                        // Leader gone — attempt promotion.
                        handle_lease_expiry();
                    } else {
                        // Update known primary address.
                        std::lock_guard<std::mutex> lk(mtx_);
                        primary_address_ = state->leader_address;
                    }
                }
            }
        }

        // Sleep 1 second between iterations.
        for (int i = 0; i < 10 && running_.load(std::memory_order_relaxed); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
}

// ── handle_lease_expiry() ───────────────────────────────────────────────────

void FailoverManager::handle_lease_expiry() {
    if (!config_.failover_enabled) return;
    attempt_promotion();
}

// ── attempt_promotion() ─────────────────────────────────────────────────────

void FailoverManager::attempt_promotion() {
    if (!coordinator_) return;

    // 1. Publish our WAL position for election comparison.
    auto [wal_file, wal_offset] = handler_.get_wal_position();
    coordinator_->publish_wal_position(wal_file, wal_offset);

    // 2. Wait 500ms for other replicas to publish their positions.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // 3. Get all published positions.
    auto positions = coordinator_->get_published_positions();
    if (positions.empty()) return;

    // 4. Find the winner: highest WAL position, tie-break by lowest node_id.
    std::sort(positions.begin(), positions.end(),
              [](const PublishedPosition& a, const PublishedPosition& b) {
                  if (a.wal_file_index != b.wal_file_index)
                      return a.wal_file_index > b.wal_file_index;
                  if (a.wal_byte_offset != b.wal_byte_offset)
                      return a.wal_byte_offset > b.wal_byte_offset;
                  return a.node_id < b.node_id;
              });

    const auto& winner = positions.front();
    if (winner.node_id != config_.coordinator.node_id) {
        // We are not the winner — remain replica.
        return;
    }

    // 5. We are the winner — grant lease and try to acquire leadership.
    int64_t new_lease = coordinator_->grant_lease();
    if (new_lease == 0) return;

    EpochValue current = handler_.get_current_epoch();
    EpochValue new_epoch = current.incremented();

    bool acquired = coordinator_->try_acquire_leadership(
        new_lease, new_epoch, config_.replication_address);
    if (!acquired) {
        coordinator_->revoke_lease(new_lease);
        return;
    }

    // 6. Leadership acquired — update state and promote.
    {
        std::lock_guard<std::mutex> lk(mtx_);
        lease_id_ = new_lease;
        epoch_ = new_epoch;
        primary_address_ = config_.replication_address;
        last_lease_refresh_ = std::chrono::steady_clock::now();
    }

    role_.store(NodeRole::PRIMARY);
    handler_.promote_to_primary(new_epoch);

    std::fprintf(stderr, "[failover] promoted to PRIMARY, epoch=%lu\n",
                 static_cast<unsigned long>(new_epoch.term));
}

// ── handle_primary_lease_lost() ─────────────────────────────────────────────

void FailoverManager::handle_primary_lease_lost() {
    std::fprintf(stderr, "[failover] lease lost, demoting to REPLICA\n");

    role_.store(NodeRole::REPLICA);
    {
        std::lock_guard<std::mutex> lk(mtx_);
        lease_id_ = 0;
    }

    // Discover the new primary from the coordinator.
    if (coordinator_) {
        auto state = coordinator_->get_cluster_state();
        if (state.has_value() && !state->leader_address.empty()) {
            std::lock_guard<std::mutex> lk(mtx_);
            primary_address_ = state->leader_address;
            handler_.demote_to_replica(state->leader_address);
        }
    }
}

// ── reconcile_epoch() ───────────────────────────────────────────────────────

void FailoverManager::reconcile_epoch(const ClusterState& state) {
    EpochValue local_epoch;
    {
        std::lock_guard<std::mutex> lk(mtx_);
        local_epoch = epoch_;
    }

    if (state.epoch > local_epoch) {
        std::lock_guard<std::mutex> lk(mtx_);
        epoch_ = state.epoch;

        // If the coordinator epoch is more than 1 ahead, we need to re-bootstrap.
        if (state.epoch.term > local_epoch.term + 1 && !state.leader_address.empty()) {
            handler_.truncate_and_rebootstrap(state.epoch, state.leader_address);
        }
    }
}

// ── elect_winner() ──────────────────────────────────────────────────────────

const PublishedPosition* elect_winner(const std::vector<PublishedPosition>& positions) {
    if (positions.empty()) return nullptr;

    const PublishedPosition* best = &positions[0];
    for (size_t i = 1; i < positions.size(); ++i) {
        const auto& p = positions[i];
        if (p.wal_file_index > best->wal_file_index) {
            best = &p;
        } else if (p.wal_file_index == best->wal_file_index) {
            if (p.wal_byte_offset > best->wal_byte_offset) {
                best = &p;
            } else if (p.wal_byte_offset == best->wal_byte_offset) {
                if (p.node_id < best->node_id) {
                    best = &p;
                }
            }
        }
    }
    return best;
}

} // namespace ob
