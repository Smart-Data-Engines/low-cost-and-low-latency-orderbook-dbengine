#include "orderbook/failover.hpp"
#include "orderbook/logger.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

namespace ob {

namespace {

/// Wall-clock nanoseconds, for the handover deadline.
///
/// Deliberately wall clock rather than steady clock: the deadline is written to
/// etcd and read by other nodes, so it has to mean something across processes.
/// Clock skew only widens or narrows the preference window; promotion still goes
/// through a CAS, so it cannot cause two primaries.
uint64_t wall_clock_ns() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

} // namespace


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

void FailoverManager::set_role(NodeRole role) {
    NodeRole old = role_.exchange(role, std::memory_order_acq_rel);
    if (old != role) {
        OB_LOG_INFO("failover", "Role changed externally: %d -> %d",
                    static_cast<int>(old), static_cast<int>(role));
    }
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

FailoverManager::HandoverResult FailoverManager::initiate_graceful_failover(
        const std::string& target_node_id) {
    if (role_.load() != NodeRole::PRIMARY) {
        return HandoverResult::NOT_PRIMARY;
    }
    if (!coordinator_ || lease_id_.load() == 0) {
        return HandoverResult::NOT_CONFIGURED;
    }

    const std::string& self_id = config_.coordinator.node_id;

    // 1. Validate the target before touching anything. Handing the role to a
    //    node that does not exist would drop the cluster into an election for
    //    no reason, and the operator would see OK for an operation that did the
    //    opposite of what they asked.
    if (target_node_id.empty() || target_node_id == self_id) {
        OB_LOG_WARN("failover",
                    "Graceful failover rejected: invalid target '%s' (self=%s)",
                    target_node_id.c_str(), self_id.c_str());
        return HandoverResult::INVALID_TARGET;
    }

    {
        const auto positions = coordinator_->get_published_positions();
        const bool known = std::any_of(
            positions.begin(), positions.end(),
            [&](const PublishedPosition& p) { return p.node_id == target_node_id; });
        if (!known) {
            OB_LOG_WARN("failover",
                        "Graceful failover rejected: unknown target %s "
                        "(%zu nodes known to coordinator)",
                        target_node_id.c_str(), positions.size());
            return HandoverResult::UNKNOWN_TARGET;
        }
    }

    // 2. Announce the intent BEFORE revoking the lease. The other order would
    //    leave a window where the leader key is gone and nothing says who should
    //    take it, which is exactly the race this fix removes.
    HandoverIntent intent;
    intent.target_node_id = target_node_id;
    intent.from_node_id   = self_id;
    intent.deadline_ns    = wall_clock_ns() +
        static_cast<uint64_t>(config_.handover_grace_seconds) * 1'000'000'000ULL;

    if (!coordinator_->publish_handover_intent(intent)) {
        OB_LOG_ERROR("failover",
                     "Graceful failover aborted: cannot publish intent, "
                     "staying primary (target=%s)",
                     target_node_id.c_str());
        return HandoverResult::COORDINATOR_ERROR;
    }

    OB_LOG_INFO("failover",
                "Graceful failover: handing role to %s (grace=%lds cooldown=%lds)",
                target_node_id.c_str(),
                static_cast<long>(config_.handover_grace_seconds),
                static_cast<long>(config_.handover_cooldown_seconds));

    // 3. Block ourselves from standing for election BEFORE revoking, so our own
    //    monitor_loop() cannot slip into attempt_promotion() between the
    //    revocation and the block.
    {
        std::lock_guard<std::mutex> lk(mtx_);
        election_blocked_until_ = std::chrono::steady_clock::now() +
            std::chrono::seconds(config_.handover_cooldown_seconds);
    }

    // 4. Revoke the lease; the leader key is held under it, so it disappears and
    //    the target sees an empty leader with an intent naming it.
    const int64_t lease = lease_id_.load();
    if (!coordinator_->revoke_lease(lease)) {
        // We are still primary as far as etcd is concerned. Undo the block and
        // clear the intent so the cluster is not left in a half-handed-over
        // state.
        {
            std::lock_guard<std::mutex> lk(mtx_);
            election_blocked_until_ = {};
        }
        coordinator_->clear_handover_intent();
        OB_LOG_ERROR("failover",
                     "Graceful failover aborted: lease revoke failed, "
                     "staying primary (lease=%ld)", static_cast<long>(lease));
        return HandoverResult::COORDINATOR_ERROR;
    }

    lease_id_.store(0);
    role_.store(NodeRole::REPLICA);

    OB_LOG_INFO("failover",
                "Graceful failover: lease %ld revoked, now REPLICA, waiting for %s",
                static_cast<long>(lease), target_node_id.c_str());

    // 5. If the target was quick, adopt it as our primary right away. Otherwise
    //    monitor_loop() will pick it up on its next pass.
    // The lease is gone, so this node is not primary any more whatever the target does next. Tell
    // the engine now: it owns the ROLE answer and the read-only flag, and this call used to happen
    // only if the target had already promoted by this instant — which it has not, because it first
    // has to notice the empty leader key. So a node that had just handed the role away kept
    // answering ROLE with PRIMARY and kept accepting writes. The empty-address case is handled:
    // Engine::demote_to_replica() only starts a replication client when it can parse host:port.
    auto state = coordinator_->get_cluster_state();
    const std::string new_primary =
        (state.has_value() && !state->leader_address.empty()) ? state->leader_address
                                                             : std::string{};
    {
        std::lock_guard<std::mutex> lk(mtx_);
        primary_address_          = new_primary;
        adopted_primary_address_  = new_primary;
    }
    handler_.demote_to_replica(new_primary);

    OB_LOG_INFO("failover", "Graceful failover: demoted locally, primary=%s",
                new_primary.empty() ? "(not elected yet)" : new_primary.c_str());

    return HandoverResult::OK;
}

/// Whether this node should stand aside because a graceful handover named
/// someone else.
///
/// Returns true only while an intent is live and points at another node. An
/// absent, unparsable or expired intent means ordinary election, which is what
/// keeps an unreachable target from deadlocking the cluster: the deadline passes
/// and everyone competes again.
bool FailoverManager::should_defer_to_handover_target() {
    if (!coordinator_) return false;

    const auto intent = coordinator_->get_handover_intent();
    if (!intent.has_value()) return false;
    if (!intent->is_active(wall_clock_ns())) {
        OB_LOG_DEBUG("failover",
                     "Handover intent for %s has expired, resuming normal election",
                     intent->target_node_id.c_str());
        return false;
    }

    if (intent->target_node_id == config_.coordinator.node_id) {
        OB_LOG_INFO("failover",
                    "Handover intent targets us (from=%s), promoting",
                    intent->from_node_id.c_str());
        return false;
    }

    OB_LOG_DEBUG("failover",
                 "Deferring election: handover intent targets %s (from=%s)",
                 intent->target_node_id.c_str(), intent->from_node_id.c_str());
    return true;
}

// ── monitor_loop() ──────────────────────────────────────────────────────────

void FailoverManager::publish_position_if_due() {
    if (!coordinator_) return;

    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lk(mtx_);
        if (last_position_publish_ != std::chrono::steady_clock::time_point{} &&
            now - last_position_publish_ < std::chrono::seconds(1)) {
            return;
        }
        last_position_publish_ = now;
    }

    const auto [file_index, byte_offset] = handler_.get_wal_position();
    if (!coordinator_->publish_wal_position(file_index, byte_offset)) {
        OB_LOG_WARN("failover", "publish_wal_position failed: file=%u offset=%zu",
                    file_index, byte_offset);
        return;
    }
    OB_LOG_DEBUG("failover", "Published WAL position: file=%u offset=%zu",
                 file_index, byte_offset);
}

void FailoverManager::monitor_loop() {
    while (running_.load(std::memory_order_acquire)) {
        NodeRole current = role_.load();

        // Both roles publish: a handover target is a replica, and FAILOVER <target> validates the
        // name against the published positions. Before this call existed, that list was empty on
        // every real cluster and every graceful handover was refused (#60).
        if (current != NodeRole::MULTI_MASTER) {
            publish_position_if_due();
        }

        // Multi-master nodes do not participate in primary/replica election.
        if (current == NodeRole::MULTI_MASTER) {
            OB_LOG_DEBUG("failover", "Node role: MULTI_MASTER — skipping election");
            // Sleep and continue — no lease management needed.
            for (int i = 0; i < 10 && running_.load(std::memory_order_relaxed); ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            continue;
        }

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

                // There is no leader when the key is absent, which
                // get_cluster_state() reports as nullopt, and also when it is
                // present with an empty node id. Both cases must go through the
                // same path: the first is what a lease revocation produces, so
                // handling the handover only in the second would leave the
                // common case unprotected.
                const bool leader_present =
                    state.has_value() && !state->leader_node_id.empty();

                if (state.has_value()) {
                    reconcile_epoch(*state);
                }

                if (leader_present) {
                    // Update known primary address.
                    std::lock_guard<std::mutex> lk(mtx_);
                    primary_address_ = state->leader_address;
                } else if (config_.failover_enabled) {
                    // Before competing, check whether this is a graceful
                    // handover with a named successor: if so, only that node
                    // should campaign, so the role goes where the operator sent
                    // it rather than to whoever is quickest.
                    if (!should_defer_to_handover_target()) {
                        handle_lease_expiry();
                    }
                    // Otherwise: not our turn, re-check on the next pass.
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

    // A node that has just handed the role away must not win the election it
    // announced. Without this, the outgoing primary races the intended successor
    // and wins roughly half the time, because it already has a warm connection
    // to the coordinator.
    {
        std::lock_guard<std::mutex> lk(mtx_);
        const auto now = std::chrono::steady_clock::now();
        if (now < election_blocked_until_) {
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                election_blocked_until_ - now);
            OB_LOG_DEBUG("failover",
                         "attempt_promotion skipped: handover cooldown, %ld ms remaining",
                         static_cast<long>(remaining.count()));
            return;
        }
    }

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

    // The handover, if there was one, is complete. Clearing is best-effort: the
    // intent expires on its own, and every reader checks the deadline anyway.
    if (coordinator_->clear_handover_intent()) {
        OB_LOG_DEBUG("failover", "Handover intent cleared after promotion");
    }
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
