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
        role_.store(NodeRole::STANDALONE);
        if (config_.coordinator.endpoints.empty()) {
            // Genuinely single-node: nothing to poll, so do not start a thread that would retry a
            // connection to nowhere once a second.
            OB_LOG_INFO("failover", "no coordinator endpoints configured, running standalone");
            return;
        }
        // Endpoints were configured, so this is an outage, not a deployment choice. Start the
        // monitor thread anyway and let it retry: returning here is what left a node that booted
        // during an etcd restart permanently outside its own cluster (#73).
        OB_LOG_WARN("failover", "cannot reach the coordinator at startup — starting STANDALONE and "
                                "retrying, this node will join once it answers");
        running_.store(true);
        monitor_thread_ = std::thread([this] { monitor_loop(); });
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

    // Drop the position lease too, so a node that was stopped on purpose stops being a target for
    // another node's election deference immediately rather than after the TTL. The thread is joined
    // by now, so the client is ours to use.
    const int64_t plid = position_lease_id_.exchange(0, std::memory_order_acq_rel);
    if (plid != 0 && coordinator_) {
        if (coordinator_->revoke_lease(plid)) {
            OB_LOG_INFO("failover", "position lease %ld revoked on shutdown",
                        static_cast<long>(plid));
        } else {
            OB_LOG_WARN("failover",
                        "could not revoke position lease %ld — this node's position stays visible "
                        "for up to the lease TTL", static_cast<long>(plid));
        }
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

int64_t FailoverManager::lease_wait_ms() const {
    if (config_.election_lease_wait_ms > 0) return config_.election_lease_wait_ms;
    // Derived rather than duplicated: the wait has to match the bound the primary steps down
    // within, and two numbers that must agree should not both be written down.
    return config_.coordinator.lease_ttl_seconds * 1000;
}

void FailoverManager::note_leader_absent() {
    std::lock_guard<std::mutex> lk(mtx_);
    if (leader_absent_since_.has_value()) return;      // only the first sighting counts
    leader_absent_since_ = std::chrono::steady_clock::now();
    OB_LOG_INFO("failover",
                "leader key is absent — waiting %lld ms before standing for election, so the "
                "previous holder has certainly stepped down (#82)",
                static_cast<long long>(lease_wait_ms()));
}

void FailoverManager::note_leader_present() {
    std::lock_guard<std::mutex> lk(mtx_);
    if (!leader_absent_since_.has_value()) return;
    OB_LOG_INFO("failover", "leader key is back — cancelling the election wait");
    leader_absent_since_.reset();
}

bool election_wait_elapsed(std::optional<std::chrono::steady_clock::time_point> absent_since,
                           std::chrono::steady_clock::time_point now,
                           int64_t wait_ms) {
    if (!absent_since.has_value()) {
        // Nothing has been observed absent. This is what keeps a failed read from starting a
        // campaign: "I could not find out" is not "there is no leader".
        return false;
    }
    // A wait of zero is no wait. The configuration cannot produce this by accident — 0 there means
    // "derive from the lease TTL" and a negative value is rejected at startup — so reaching it means
    // a caller asked for it deliberately, which the tests do.
    if (wait_ms <= 0) return true;
    const auto waited = std::chrono::duration_cast<std::chrono::milliseconds>(now - *absent_since);
    return waited.count() >= wait_ms;
}

bool FailoverManager::leader_absence_settled() const {
    std::optional<std::chrono::steady_clock::time_point> since;
    {
        std::lock_guard<std::mutex> lk(mtx_);
        since = leader_absent_since_;
    }
    return election_wait_elapsed(since, std::chrono::steady_clock::now(), lease_wait_ms());
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
    {
        // A fresh election starts with a fresh window, or the next one inherits a deferral clock
        // from this handover and promotes sooner than it should.
        std::lock_guard<std::mutex> lk(mtx_);
        deferring_since_ = {};
    }

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

    const int64_t lease = ensure_position_lease();
    const auto [file_index, byte_offset] = handler_.get_wal_position();
    if (!coordinator_->publish_wal_position(file_index, byte_offset, lease)) {
        OB_LOG_WARN("failover", "publish_wal_position failed: file=%u offset=%zu lease=%ld",
                    file_index, byte_offset, static_cast<long>(lease));
        return;
    }
    OB_LOG_DEBUG("failover", "Published WAL position: file=%u offset=%zu lease=%ld",
                 file_index, byte_offset, static_cast<long>(lease));
}

// ── ensure_position_lease() ─────────────────────────────────────────────────

int64_t FailoverManager::ensure_position_lease() {
    if (!coordinator_) return 0;

    int64_t lease = position_lease_id_.load(std::memory_order_acquire);
    if (lease != 0) {
        if (coordinator_->refresh_lease(lease)) return lease;
        // etcd does not know this lease any more: it restarted, or this process was stalled past
        // the TTL. Publishing under a dead lease writes a key that is deleted the moment it lands,
        // so take a new lease rather than retrying the old one for ever.
        OB_LOG_WARN("failover",
                    "position lease %ld is gone, taking a new one so this node stays visible to "
                    "an election", static_cast<long>(lease));
        position_lease_id_.store(0, std::memory_order_release);
    }

    lease = coordinator_->grant_lease();
    if (lease == 0) {
        OB_LOG_WARN("failover",
                    "could not grant a lease for the published position — publishing without one, "
                    "so this position will outlive this node and other nodes may defer to it after "
                    "it dies");
        return 0;
    }
    position_lease_id_.store(lease, std::memory_order_release);
    OB_LOG_INFO("failover", "position lease %ld granted, ttl=%lds",
                static_cast<long>(lease),
                static_cast<long>(config_.coordinator.lease_ttl_seconds));
    return lease;
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

            // A lease that is alive is not proof that the role still belongs to us: the key can be
            // gone while the lease lives, and before #74 a keepalive could not fail at all, so this
            // is the second and independent guard. Demote on the first sighting of a leader key that
            // is not ours — a spurious demotion costs seconds of unavailability, two primaries cost
            // divergent data.
            //
            // Three answers, not two. Until #82 this asked get_cluster_state(), whose std::nullopt
            // meant "not connected" or "empty response" or "the key is not there" or "the body would
            // not parse" — so an absent key could not be acted on without also stepping down on
            // every transient etcd error. That is why the holder used to learn only from a failed
            // refresh, up to lease_ttl/3 later, while a candidate could take the vacated key at once.
            if (coordinator_) {
                ClusterState state;
                const auto verdict = coordinator_->read_leader(state);

                if (verdict == CoordinatorClient::LeaderRead::Present) {
                    reconcile_epoch(state);
                    const std::string& holder = state.leader_node_id;
                    if (holder != config_.coordinator.node_id) {
                        OB_LOG_WARN("failover",
                                    "we hold the PRIMARY role but the leader key says '%s' — "
                                    "stepping down",
                                    holder.empty() ? "(empty)" : holder.c_str());
                        handle_primary_lease_lost();
                        for (int i = 0; i < 10 && running_.load(std::memory_order_relaxed); ++i) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(100));
                        }
                        continue;
                    }
                    // The one moment at which ownership is established rather than assumed.
                    {
                        std::lock_guard<std::mutex> lk(mtx_);
                        last_ownership_confirmed_ = std::chrono::steady_clock::now();
                    }
                } else if (verdict == CoordinatorClient::LeaderRead::Absent) {
                    OB_LOG_WARN("failover",
                                "we hold the PRIMARY role but the leader key is gone — stepping "
                                "down. Its lease expired or was revoked, so the role is not ours "
                                "and a candidate may already be taking it");
                    handle_primary_lease_lost();
                    for (int i = 0; i < 10 && running_.load(std::memory_order_relaxed); ++i) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    }
                    continue;
                }
                // Unavailable: no information. Leave the role alone — but not for ever; the clock
                // rule below is what bounds that.

                // A holder that has not been able to confirm ownership for a whole TTL is holding a
                // claim it cannot support: whatever the reason, its lease has had time to expire.
                // Never fires on a healthy node, which confirms every second — ten times more often
                // than this threshold at the default TTL.
                const auto ttl = std::chrono::seconds(config_.coordinator.lease_ttl_seconds);
                std::chrono::steady_clock::time_point confirmed;
                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    confirmed = last_ownership_confirmed_;
                }
                if (confirmed.time_since_epoch().count() != 0 &&
                    std::chrono::steady_clock::now() - confirmed >= ttl) {
                    OB_LOG_WARN("failover",
                                "could not confirm the leader key names us for %lld s (TTL %lld s) "
                                "— stepping down rather than holding a claim we cannot support",
                                static_cast<long long>(
                                    std::chrono::duration_cast<std::chrono::seconds>(
                                        std::chrono::steady_clock::now() - confirmed).count()),
                                static_cast<long long>(config_.coordinator.lease_ttl_seconds));
                    handle_primary_lease_lost();
                    for (int i = 0; i < 10 && running_.load(std::memory_order_relaxed); ++i) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    }
                    continue;
                }
            }

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
                ClusterState state;
                const auto verdict = coordinator_->read_leader(state);

                // A key that is present with an empty node id means the same thing as an absent
                // one — nobody holds the role — and both must go through the same path: the first
                // is what a lease revocation produces, so handling only the second would leave the
                // common case unprotected.
                const bool leader_present =
                    verdict == CoordinatorClient::LeaderRead::Present &&
                    !state.leader_node_id.empty();

                if (verdict == CoordinatorClient::LeaderRead::Present) {
                    reconcile_epoch(state);
                }

                if (leader_present) {
                    note_leader_present();
                    std::lock_guard<std::mutex> lk(mtx_);
                    primary_address_ = state.leader_address;
                } else if (verdict == CoordinatorClient::LeaderRead::Unavailable) {
                    // No information. Standing for election here is what this branch used to do,
                    // because get_cluster_state() reported an unreachable coordinator and a vacant
                    // key with the same std::nullopt. Campaigning because we could not read is not
                    // the same as campaigning because there is no leader.
                    OB_LOG_DEBUG("failover",
                                 "cluster state unreadable — staying REPLICA rather than "
                                 "campaigning on a read that failed");
                } else if (config_.failover_enabled) {
                    note_leader_absent();

                    // Wait out the previous holder's step-down bound before competing (#82).
                    // Without this, the vacated key can be claimed while the old holder still
                    // believes it is primary — and both accept writes while both believe it.
                    if (!leader_absence_settled()) {
                        OB_LOG_DEBUG("failover",
                                     "leader key absent but the election wait has not elapsed — "
                                     "not campaigning yet");
                    } else if (!should_defer_to_handover_target()) {
                        // A graceful handover with a named successor: only that node should
                        // campaign, so the role goes where the operator sent it rather than to
                        // whoever is quickest.
                        handle_lease_expiry();
                    }
                    // Otherwise: not our turn, re-check on the next pass.
                }
            }
        } else if (current == NodeRole::STANDALONE) {
            // No dead states. Before #73 this role matched neither branch above, so a node that
            // lost the startup CAS, or booted while the coordinator was briefly unreachable, sat
            // here for the rest of its life: no lease, no leader poll, no campaign, no replication
            // — and no takeover when the primary died.
            ++standalone_polls_;
            if (coordinator_ && !coordinator_->is_connected()) {
                if (coordinator_->connect()) {
                    OB_LOG_INFO("failover", "coordinator reachable again after %llu attempts, "
                                            "rejoining the cluster",
                                static_cast<unsigned long long>(standalone_polls_));
                } else if (standalone_polls_ % 30 == 1) {
                    // Once per 30 s, not once per second: an unreachable coordinator is a
                    // condition, not an event.
                    OB_LOG_WARN("failover", "coordinator still unreachable after %llu attempts, "
                                            "this node holds no cluster role",
                                static_cast<unsigned long long>(standalone_polls_));
                }
            }
            if (coordinator_ && coordinator_->is_connected() && !adopt_leader_if_present() &&
                config_.failover_enabled) {
                // The same wait as the REPLICA branch, but only once we know a leader has existed
                // (#82). A cold start has no previous holder to wait for, and making every cluster
                // spend a lease TTL before it has a primary would be a real cost for no safety.
                //
                // "A leader has existed" is read from the epoch, which is persisted: a node that
                // was ever part of this cluster comes back with a non-zero one. The residual hole is
                // a brand-new node that has never seen this cluster's epoch and reconnects during
                // the vacancy — narrower than the window this closes, and named here rather than
                // left to be discovered.
                EpochValue known_epoch = handler_.get_current_epoch();
                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    if (epoch_.term > known_epoch.term) known_epoch = epoch_;
                }

                if (known_epoch.term > 0) {
                    note_leader_absent();
                    if (!leader_absence_settled()) {
                        OB_LOG_DEBUG("failover",
                                     "no leader published, but epoch %llu says one existed — "
                                     "waiting out the election delay before standing",
                                     static_cast<unsigned long long>(known_epoch.term));
                        // Fall through to the sleep at the bottom of the loop.
                        for (int i = 0; i < 10 && running_.load(std::memory_order_relaxed); ++i) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(100));
                        }
                        continue;
                    }
                }

                OB_LOG_INFO("failover", "no leader published and no role held — standing for "
                                        "election");
                attempt_promotion();
            }
        }

        // Sleep 1 second between iterations.
        for (int i = 0; i < 10 && running_.load(std::memory_order_relaxed); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
}

// ── adopt_leader_if_present() ───────────────────────────────────────────────

bool FailoverManager::adopt_leader_if_present() {
    if (!coordinator_) return false;

    auto state = coordinator_->get_cluster_state();
    const bool leader_present = state.has_value() && !state->leader_node_id.empty();
    if (!leader_present) return false;
    if (state->leader_node_id == config_.coordinator.node_id) {
        // The key names us. Whoever calls this is mid-transition; leave the role alone.
        return false;
    }

    reconcile_epoch(*state);
    {
        std::lock_guard<std::mutex> lk(mtx_);
        primary_address_ = state->leader_address;
    }
    role_.store(NodeRole::REPLICA, std::memory_order_release);
    OB_LOG_INFO("failover", "following %s at %s — this node is a REPLICA",
                state->leader_node_id.c_str(), state->leader_address.c_str());
    handler_.demote_to_replica(state->leader_address);
    return true;
}

// ── handle_lease_expiry() ───────────────────────────────────────────────────

void FailoverManager::handle_lease_expiry() {
    if (!config_.failover_enabled) return;
    attempt_promotion();
}

// ── attempt_promotion() ─────────────────────────────────────────────────────

ElectionDecision decide_election(const std::vector<PublishedPosition>& positions,
                                 const std::string& self_node_id,
                                 std::chrono::milliseconds deferred_for,
                                 std::chrono::milliseconds window) {
    if (positions.empty()) {
        // Nothing published: behave as before #70 and race for the key. Also the path for a cluster
        // whose nodes predate position publishing.
        return ElectionDecision::PromoteNow;
    }

    const PublishedPosition* best = elect_winner(positions);
    if (!best || best->node_id == self_node_id) {
        // We are the most advanced, or the comparison could not name anyone. Nobody to wait for.
        return ElectionDecision::PromoteNow;
    }

    // Someone published a further position. Wait for them — but not for ever: positions are written
    // without a lease, so a node that died leaves its position behind, and deferring to a dead node
    // indefinitely would leave the cluster with no primary at all.
    if (deferred_for < window) return ElectionDecision::Defer;
    return ElectionDecision::PromoteAfterWindow;
}

bool FailoverManager::should_promote_now() {
    if (!coordinator_) return true;

    const auto positions = coordinator_->get_published_positions();
    const auto now = std::chrono::steady_clock::now();

    std::chrono::milliseconds deferred_for{0};
    {
        std::lock_guard<std::mutex> lk(mtx_);
        if (deferring_since_ != std::chrono::steady_clock::time_point{}) {
            deferred_for = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - deferring_since_);
        }
    }

    const auto decision = decide_election(
        positions, config_.coordinator.node_id, deferred_for,
        std::chrono::milliseconds(config_.election_deference_ms));

    switch (decision) {
    case ElectionDecision::PromoteNow: {
        std::lock_guard<std::mutex> lk(mtx_);
        deferring_since_ = {};
        return true;
    }
    case ElectionDecision::Defer: {
        bool first = false;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            if (deferring_since_ == std::chrono::steady_clock::time_point{}) {
                deferring_since_ = now;
                first = true;
            }
        }
        if (first) {
            const PublishedPosition* best = elect_winner(positions);
            deferrals_.fetch_add(1, std::memory_order_relaxed);
            OB_LOG_INFO("failover",
                        "Deferring election to %s (file=%u offset=%zu), window=%lldms — it holds "
                        "more of the log than we do",
                        best ? best->node_id.c_str() : "?",
                        best ? best->wal_file_index : 0u,
                        best ? best->wal_byte_offset : size_t{0},
                        static_cast<long long>(config_.election_deference_ms));
        }
        return false;
    }
    case ElectionDecision::PromoteAfterWindow: {
        const PublishedPosition* best = elect_winner(positions);
        OB_LOG_WARN("failover",
                    "Deference window expired after %lldms and %s never promoted — its position may "
                    "be stale, since positions carry no lease. Promoting anyway",
                    static_cast<long long>(deferred_for.count()),
                    best ? best->node_id.c_str() : "?");
        std::lock_guard<std::mutex> lk(mtx_);
        deferring_since_ = {};
        return true;
    }
    }
    return true;
}

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

    // Prefer the replica that lost the least. Until #70 this was a pure CAS race, so the role went
    // to whoever polled first — elect_winner() existed, with unit tests, and had no callers.
    // Checked before granting a lease: no point taking one out only to stand down.
    if (!should_promote_now()) return;

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
        // Losing the CAS means someone else is primary — so follow them. Returning here without
        // touching the role is what left a node stuck at STANDALONE for the rest of its life (#73).
        OB_LOG_INFO("failover", "lost the leadership CAS on node %s — following the winner",
                    config_.coordinator.node_id.c_str());
        if (!adopt_leader_if_present()) {
            OB_LOG_WARN("failover", "lost the CAS but no leader is published yet — will retry on "
                                    "the next pass rather than idle at STANDALONE");
        }
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
        // A create-only CAS that succeeded is a confirmation: at this instant the leader key names
        // us. Without seeding it here, the clock rule in monitor_loop() would have nothing to
        // measure from until the first successful read, so a node that promoted and then lost etcd
        // would hold the role indefinitely.
        last_ownership_confirmed_ = last_lease_refresh_;
        leader_absent_since_.reset();
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

    // Where the new primary is, if anybody has said yet. Optional — and that is the whole point of
    // this shape.
    //
    // This used to call handler_.demote_to_replica() *only* when it could read a leader key with a
    // non-empty address. In the case that matters most it cannot: a revoked or expired lease deletes
    // the key, so there is no address to find, and the demotion never reached the Engine at all.
    // The FailoverManager's own role_ went to REPLICA while the Engine kept node_role_ == PRIMARY,
    // read_only_flag_ unset, and ROLE answering "PRIMARY <epoch>" — so the node went on accepting
    // writes for as long as it took to stand for election again, and indefinitely if it never did.
    // That is the larger half of #82, and it is pitfall 28 in the very path pitfall 28 is about:
    // when a role moves, every component that answers questions about it has to be told.
    //
    // Not knowing where to point the replication client is a reason to start no client. It is not a
    // reason to keep claiming a role.
    std::string new_primary;
    if (coordinator_) {
        ClusterState state;
        if (coordinator_->read_leader(state) == CoordinatorClient::LeaderRead::Present) {
            new_primary = state.leader_address;
        }
    }

    {
        std::lock_guard<std::mutex> lk(mtx_);
        primary_address_ = new_primary;
    }

    if (new_primary.empty()) {
        OB_LOG_WARN("failover",
                    "no new primary is published yet — demoting anyway and starting no replication "
                    "client; this node must stop answering as PRIMARY either way");
    }
    handler_.demote_to_replica(new_primary);
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
