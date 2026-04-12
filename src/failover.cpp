#include "orderbook/failover.hpp"
#include "orderbook/logger.hpp"

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
        OB_LOG_WARN("failover", "cannot connect to coordinator, operating in standalone mode");
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
            // Call demote_to_replica() to start ReplicationClient with the
            // primary address discovered from etcd. This is critical for HA:
            // a node that restarts while another node is PRIMARY must
            // automatically connect and start replicating.
            role_.store(NodeRole::REPLICA);
            {
                std::lock_guard<std::mutex> lk(mtx_);
                primary_address_ = state->leader_address;
            }
            OB_LOG_INFO("failover", "starting as REPLICA, primary=%s (from etcd)",
                        state->leader_address.c_str());
            handler_.demote_to_replica(state->leader_address);
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
    OB_LOG_INFO("failover", "stop() called on %s, running=%d role=%d lease=%ld",
                config_.coordinator.node_id.c_str(),
                running_.load(), static_cast<int>(role_.load()),
                static_cast<long>(lease_id_.load(std::memory_order_acquire)));
    if (!running_.exchange(false)) {
        OB_LOG_INFO("failover", "stop() early return — was not running");
        return;
    }

    // If we are PRIMARY, revoke our lease immediately using a separate
    // coordinator connection. The monitor thread might be blocking on an
    // HTTP call, so we can't wait for it before revoking.
    int64_t lid = lease_id_.load(std::memory_order_acquire);
    if (role_.load(std::memory_order_acquire) == NodeRole::PRIMARY && lid != 0) {
        // Create a temporary coordinator client for the revoke call.
        OB_LOG_INFO("failover", "revoking lease %ld via separate connection...",
                    static_cast<long>(lid));
        CoordinatorClient revoke_client(config_.coordinator);
        if (revoke_client.connect()) {
            OB_LOG_INFO("failover", "connected to coordinator for revoke");
            bool revoked = revoke_client.revoke_lease(lid);
            if (revoked) {
                OB_LOG_INFO("failover", "lease revoked successfully, lease_id=%ld",
                            static_cast<long>(lid));
            } else {
                OB_LOG_WARN("failover", "failed to revoke lease, lease_id=%ld",
                            static_cast<long>(lid));
            }
            revoke_client.disconnect();
        } else {
            OB_LOG_WARN("failover", "could not connect to coordinator for revoke");
        }
    }

    // Now join the monitor thread (it will exit within ~100ms + HTTP timeout).
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
    if (!coordinator_ || lease_id_.load() == 0) return false;

    // Revoke our lease — replicas will detect the expiry and compete.
    bool revoked = coordinator_->revoke_lease(lease_id_.load());
    if (!revoked) return false;

    {
        std::lock_guard<std::mutex> lk(mtx_);
        lease_id_.store(0);
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
                if (coordinator_ && lease_id_.load() != 0) {
                    int64_t lid = lease_id_.load();
                    bool ok = coordinator_->refresh_lease(lid);
                    if (ok) {
                        std::lock_guard<std::mutex> lk(mtx_);
                        last_lease_refresh_ = std::chrono::steady_clock::now();
                    } else {
                        OB_LOG_WARN("failover", "refresh_lease failed for lease=%ld, demoting",
                                    static_cast<long>(lid));
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
                } else {
                    // Could not read cluster state (key missing = leader gone).
                    // Attempt promotion if failover is enabled.
                    if (config_.failover_enabled) {
                        handle_lease_expiry();
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

    // Grant a new lease and try to acquire leadership via CAS.
    // If the leader key doesn't exist, CAS succeeds and we become primary.
    // If it exists (another node promoted first), CAS fails and we stay replica.
    int64_t new_lease = coordinator_->grant_lease();
    if (new_lease == 0) return;

    EpochValue local_epoch = handler_.get_current_epoch();
    EpochValue fm_epoch;
    {
        std::lock_guard<std::mutex> lk(mtx_);
        fm_epoch = epoch_;
    }
    // Use the higher of local engine epoch and the epoch we know from etcd.
    EpochValue current = (fm_epoch.term > local_epoch.term) ? fm_epoch : local_epoch;
    EpochValue new_epoch = current.incremented();

    bool acquired = coordinator_->try_acquire_leadership(
        new_lease, new_epoch, config_.replication_address);
    if (!acquired) {
        coordinator_->revoke_lease(new_lease);
        return;
    }

    // 6. Leadership acquired — update state and promote.
    // Store lease_id_ with release semantics BEFORE role_ so that
    // stop() sees the lease_id when it checks role_ == PRIMARY.
    lease_id_.store(new_lease, std::memory_order_release);
    OB_LOG_INFO("failover", "lease_id_ set to %ld", static_cast<long>(new_lease));
    {
        std::lock_guard<std::mutex> lk(mtx_);
        epoch_ = new_epoch;
        primary_address_ = config_.replication_address;
        last_lease_refresh_ = std::chrono::steady_clock::now();
    }

    role_.store(NodeRole::PRIMARY, std::memory_order_release);
    handler_.promote_to_primary(new_epoch);

    OB_LOG_INFO("failover", "promoted to PRIMARY, epoch=%lu",
                static_cast<unsigned long>(new_epoch.term));
}

// ── handle_primary_lease_lost() ─────────────────────────────────────────────

void FailoverManager::handle_primary_lease_lost() {
    OB_LOG_WARN("failover", "lease lost, demoting to REPLICA");

    role_.store(NodeRole::REPLICA);
    {
        std::lock_guard<std::mutex> lk(mtx_);
        lease_id_.store(0);
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
