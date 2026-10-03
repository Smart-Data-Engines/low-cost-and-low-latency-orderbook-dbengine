#pragma once

// ── FailoverManager — role transitions and monitoring ────────────────────────
//
// Orchestrates automatic failover using an external coordinator (etcd).
// Runs a background thread that monitors the coordinator lease and triggers
// promotion/demotion as needed.  The Engine implements RoleTransitionHandler
// to perform the actual state changes.

#include "orderbook/log_episode.hpp"
#include "orderbook/metrics.hpp"
#include "orderbook/coordinator.hpp"
#include "orderbook/epoch.hpp"
#include "orderbook/stream_position.hpp"

#include <atomic>
#include <chrono>
#include <optional>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace ob {

class Engine;  // forward

// ── Node role ────────────────────────────────────────────────────────────────

enum class NodeRole : uint8_t {
    STANDALONE   = 0,
    PRIMARY      = 1,
    REPLICA      = 2,
    MULTI_MASTER = 3,
};

// ── Failover configuration ──────────────────────────────────────────────────

struct FailoverConfig {
    CoordinatorConfig coordinator;
    bool              failover_enabled{true};
    std::string       replication_address;  // host:port for replication
    uint16_t          replication_port{0};

    /// How long the named successor gets to take over during a graceful
    /// failover, before the cluster falls back to an ordinary election.
    /// Shorter than the lease TTL, so a handover completes faster than a
    /// failure would be detected.
    int64_t handover_grace_seconds{5};

    /// How long the outgoing primary refrains from standing for election after
    /// giving up the role. Must be >= handover_grace_seconds, otherwise the
    /// node could win the very race it announced. Longer than the lease TTL, so
    /// it does not come back before the new primary has settled.
    int64_t handover_cooldown_seconds{15};

    /// How long a candidate defers to a replica that published a further WAL position, before
    /// promoting anyway (--election-deference-ms).
    ///
    /// Bounded on purpose. Deferring without a deadline would leave the cluster with no primary at
    /// all if the node it waits for never comes back, which is worse than the defect this preference
    /// fixes. The default clears the 2-second cluster-state poll, so a live better candidate has
    /// time to take the role before this window runs out.
    ///
    /// The original reason for the bound was that positions were written without a lease, so a dead
    /// node left its position behind for ever. #72 gave them per-node leases, so a dead node drops
    /// off the list on its own and this window is now a backstop rather than the main defence.
    int64_t election_deference_ms{3000};

    /// How long a candidate waits, after first seeing the leader key absent, before campaigning.
    /// `0` means "derive it from `lease_ttl_seconds`", which is the intended use — two numbers that
    /// have to agree should not both be written down.
    ///
    /// This is what closes the window in #82. The previous holder steps down no later than
    /// `lease_ttl` after it last confirmed ownership, and a candidate cannot know when that was, so
    /// it assumes the worst: that the confirmation happened just before the key vanished. Waiting a
    /// full TTL from *observing* the absence covers that.
    ///
    /// The cost is failover latency, and it is paid every time rather than only in the unlucky
    /// case — roughly `lease_ttl` on top of what failover took before. Setting this to a smaller
    /// value narrows the safety margin in exact proportion; setting it to a value below the
    /// primary's own step-down bound reopens the window.
    int64_t election_lease_wait_ms{0};
};

// ── Callback interface for Engine to implement role transitions ──────────────

/// The Engine implements this interface so that FailoverManager can trigger
/// role changes without depending on the full Engine class.
struct RoleTransitionHandler {
    virtual ~RoleTransitionHandler() = default;

    /// Called when this node should become primary.
    /// Must: stop ReplicationClient, increment epoch, write Epoch_Record,
    ///       start ReplicationManager, disable read-only.
    virtual void promote_to_primary(const EpochValue& new_epoch) = 0;

    /// Called when this node should become replica.
    /// Must: close writes first, then - when there is a primary to follow - stop the
    /// ReplicationManager and start a ReplicationClient. With nobody to follow, the stream this
    /// node serves stays up (#204).
    virtual void demote_to_replica(const std::string& new_primary_address) = 0;

    /// Called by a graceful handover before it says anything to the coordinator: close writes,
    /// become a replica following nobody, and keep serving the replication stream, so the successor
    /// can take the rest of this node's log before it stands (#204). Returns where that stream ends,
    /// or nullopt when this node serves none.
    ///
    /// The default demotes, which is everything a handler without a stream can do.
    virtual std::optional<StreamPosition> step_down_for_handover() {
        demote_to_replica({});
        return std::nullopt;
    }

    /// How far this node is into the stream it replicates, and whose stream that is; nullopt when
    /// it follows none. What a handover's successor compares with the announced end.
    /// Not const: the engine answers it under the lock that replaces its replication client.
    virtual std::optional<StreamPosition> replicated_position() { return std::nullopt; }

    /// Called to get current WAL position for election comparison.
    virtual std::pair<uint32_t, size_t> get_wal_position() const = 0;

    /// Called to get current epoch.
    virtual EpochValue get_current_epoch() const = 0;

    /// Called to truncate stale WAL records and re-bootstrap from new primary.
    virtual void truncate_and_rebootstrap(const EpochValue& new_epoch,
                                          const std::string& primary_address) = 0;
};

// ── FailoverManager ─────────────────────────────────────────────────────────

class FailoverManager {
public:
    /// `registry` is not decoration: once `monitor_loop()` survives a throwing tick, the thread
    /// staying alive means every outward signal keeps looking healthy, and a counter is the only
    /// thing an operator can alarm on. Passed here rather than reached through
    /// `RoleTransitionHandler` because incrementing a counter is not a role transition, and not
    /// defaulted because a default would let every test leave the counter unfed - which is #117
    /// exactly, and #117 is why this counter exists (#112).
    explicit FailoverManager(FailoverConfig config, RoleTransitionHandler& handler,
                             MetricsRegistry& registry);
    ~FailoverManager();

    FailoverManager(const FailoverManager&) = delete;
    FailoverManager& operator=(const FailoverManager&) = delete;

    /// Start the failover manager (connect to coordinator, begin monitoring).
    void start();

    /// Stop the failover manager.
    void stop();

    /// Get current node role.
    NodeRole role() const;

    /// Set node role externally (used by Engine to set MULTI_MASTER).
    void set_role(NodeRole role);

    /// Get current epoch.
    EpochValue epoch() const;

    /// How many times this node stood down for a replica with a further WAL position. Zero means
    /// there was never a better-placed candidate, not that the preference is switched off.
    uint64_t deferrals() const { return deferrals_.load(std::memory_order_relaxed); }

    /// Outcome of an attempted graceful failover.
    ///
    /// Distinguishing the causes matters to the operator: "unknown target"
    /// usually means a typo in a node id, while "coordinator error" means the
    /// node is still primary and the handover never started.
    enum class HandoverResult {
        OK,                 ///< stepped down and said so; the lease revoked, or left to expire
        NOT_PRIMARY,        ///< this node is not the primary
        NOT_CONFIGURED,     ///< no coordinator, or no lease held
        INVALID_TARGET,     ///< target empty, or naming this node itself
        UNKNOWN_TARGET,     ///< target not known to the coordinator
        COORDINATOR_ERROR,  ///< could not publish the intent, or revoke and withdraw it; still
                            ///< primary, or primary again on the next tick
    };

    /// Hand the primary role to a named node.
    ///
    /// Publishes a handover intent, blocks itself from standing for election for
    /// handover_cooldown_seconds, steps down - writes closed, the replication stream left up -
    /// publishes the intent again with the term it stepped down from and where its stream ends,
    /// and only then revokes its lease (#204). Only works if we are PRIMARY.
    ///
    /// A rejected handover is not a partial one: before the step-down nothing has changed, and a
    /// revoke that fails after it is undone when the intent can be withdrawn. When it cannot, the
    /// node stays a REPLICA and the role moves once its lease expires - and this answers OK.
    HandoverResult initiate_graceful_failover(const std::string& target_node_id);

    /// Test seam: called on the handover's thread after the step-down and the second intent, before
    /// the lease is revoked - the window in which the leader key names a node that takes no writes.
    void hold_before_revoke_for_test(std::function<void()> hook);

    /// Get the current primary address (from coordinator).
    std::string primary_address() const;

    /// Get coordinator lease TTL remaining in seconds (for STATUS).
    int64_t lease_ttl_remaining() const;

private:
    FailoverConfig          config_;
    RoleTransitionHandler&  handler_;
    std::unique_ptr<CoordinatorClient> coordinator_;

    std::atomic<NodeRole>   role_{NodeRole::STANDALONE};
    mutable std::mutex      mtx_;
    EpochValue              epoch_;
    std::atomic<int64_t>    lease_id_{0};

    /// True only while `initiate_graceful_failover()` is between revoking its own lease and
    /// recording the new role.
    ///
    /// Without it the monitor loop treats a handover as a fault. The handover revokes the outgoing
    /// primary's **own** lease, so the leader key legitimately disappears while `role_` still says
    /// PRIMARY for a few instructions — and a pass landing there sees "we hold the role and the key
    /// is gone", which is #82's unconditional demotion doing exactly what it was added for. The node
    /// then demotes twice, and prints three warnings during a healthy planned operation. Until #88
    /// the second demotion also aborted the process.
    ///
    /// Set through a scope guard rather than by hand, and that is the load-bearing part: a flag
    /// which suppresses a safety check must be impossible to leave set, and this function has seven
    /// return paths — one of which keeps the role when the revoke fails. If the handover dies after
    /// revoking, the guard clears on unwind and the next pass demotes, so the net is still there.
    std::atomic<bool>       handing_over_{false};
    std::string             primary_address_;

    /// The primary this node's replication client was started towards, empty when it follows
    /// nobody - a primary, or a node that demoted before anyone was elected (#201). Written at
    /// every `demote_to_replica()` and promotion, under `mtx_`, through `note_following()`; read by
    /// the REPLICA branch of `monitor_tick()`, which follows the leader in the coordinator when it
    /// is not this one. `primary_address_` cannot stand in for it: that is what `ROLE` answers,
    /// recorded from the coordinator on every tick whether or not anything follows it - which is how
    /// a node that replicated nothing named its successor.
    std::string             following_;

    /// The term this node last handed the role over at, under `mtx_`; 0 when it never has (#204).
    ///
    /// A handover now steps down **before** it revokes the lease, so for a moment the leader key
    /// still names this node while it is a REPLICA - which is exactly what the REPLICA branch's
    /// #130 arm reads as a promotion it won and has to finish. That arm does not finish one at a
    /// term this node stepped down from: the handover's second intent says it takes no writes at
    /// that term, and a successor may already be standing on the strength of it. Recorded before
    /// the step-down, because this node's own monitor thread can read the key in between. Never
    /// reset on the way up: terms only increase, so the first real promotion is past it.
    uint64_t                stepped_down_term_{0};

    /// The REPLICA branch's #204 arm declining, once per episode (it repeats every tick until the
    /// lease goes).
    LogEpisode              handed_key_episode_{};
    /// A handover's successor waiting for the rest of the outgoing stream, once per episode.
    LogEpisode              awaiting_stream_episode_{};
    /// See `hold_before_revoke_for_test()`. Set before the handover starts, read on its thread.
    std::function<void()>   before_revoke_hook_for_test_;
    std::chrono::steady_clock::time_point last_lease_refresh_;

    /// When this node last *confirmed* that the leader key names it.
    ///
    /// Not the same thing as the last successful lease refresh: a live lease does not prove the role
    /// still belongs to us, which is what #74 was about. This is the only moment at which ownership
    /// is established rather than assumed, and the clock rule in monitor_loop() measures from it.
    std::chrono::steady_clock::time_point last_ownership_confirmed_;

    /// When this node first saw the leader key absent, or the epoch of nothing if it is present.
    ///
    /// A candidate waits `election_lease_wait_ms` from here before campaigning, so that the previous
    /// holder has certainly stepped down (#82). An `Unavailable` read neither sets nor clears it: it
    /// says nothing about the key.
    std::optional<std::chrono::steady_clock::time_point> leader_absent_since_;

    /// Record that the leader key was seen absent. Idempotent: only the first sighting counts, so
    /// the wait is measured from when the key went away rather than from the latest poll.
    void note_leader_absent();

    /// Record that the leader key is present, cancelling any wait in progress.
    void note_leader_present();

    /// Whether the wait since the first sighting of an absent key has elapsed.
    ///
    /// False when no absence has been recorded at all, which is what stops a node from campaigning
    /// on the strength of a read that failed.
    bool leader_absence_settled() const;

    /// The configured wait, or the value derived from the lease TTL when it is 0.
    int64_t lease_wait_ms() const;

    /// Until when this node declines to stand for election, after handing the
    /// role away. steady_clock, because this measures elapsed time locally and
    /// must not be affected by wall-clock adjustments.
    std::chrono::steady_clock::time_point election_blocked_until_{};

    std::thread             monitor_thread_;
    std::chrono::steady_clock::time_point last_position_publish_{};
    /// When this node started deferring to a better-placed replica; zero when it is not deferring.
    std::chrono::steady_clock::time_point deferring_since_{};
    std::atomic<uint64_t>   deferrals_{0};
    std::atomic<bool>       running_{false};
    /// Lease that keeps this node's published WAL position alive. Separate from the leadership
    /// lease on purpose: a position says "this node is here and holds this much log", which is true
    /// of a replica as much as of a primary, and must not disappear when a role changes hands.
    std::atomic<int64_t>    position_lease_id_{0};
    /// How many times the monitor loop has looked at a STANDALONE role. Touched only from the
    /// monitor thread, so it needs no lock; it exists to keep the retry log from repeating every
    /// second while a coordinator stays unreachable.
    uint64_t                standalone_polls_{0};

    /// A condition that repeats once per monitor tick, logged once per episode instead.
    ///
    /// The idea was already in this file, applied to exactly one of the places that needed it:
    /// `standalone_polls_` above was written for this and guards the STANDALONE branch alone.
    /// Pitfall 172's shape - a rule applied by hand is applied once too few. Measured before this
    /// existed (#116): a node whose coordinator was unreachable wrote about **2.2 lines a second
    /// for the whole outage**, and 60 of the 65 lines in a 30-second window were two sentences
    /// repeated once a second.
    ///
    /// Touched only from the monitor thread, like the counter above, so no lock.
    /// The coordinator would not grant a lease for the published position (#116).
    LogEpisode              lease_grant_episode_{};
    /// The published position could not be written at all (#116).
    LogEpisode              publish_episode_{};
    /// This node is declining to campaign because it cannot read the leader key (#115). Logged at
    /// INFO when the episode opens, because it is the answer to "etcd is down and nothing failed
    /// over - is that the engine deciding, or the engine broken?", and that question is asked of
    /// the default log level.
    LogEpisode              campaign_refusal_episode_{};

    void monitor_loop();

    /// One pass of monitor_loop(), so a throw costs a pass rather than the thread (#112).
    void monitor_tick();

    /// One second between iterations of monitor_loop(), in ten interruptible pieces.
    void nap_between_iterations();

    MetricsRegistry& registry_;

    /// One condition, one pair of log lines: a monitor tick that threw.
    ///
    /// Measured before the boundary existed (#112, #54's injector): an `ENOSPC` on the `EPOCH`
    /// record a promotion writes ended this thread, and the node then reported
    /// `REPLICA <its own replication port>` for the forty seconds observed, answering `PING`. A
    /// full disk throws on every tick, so the log says it once - #95's shape.
    LogEpisode monitor_errors_;

    /// Should this node take the role now, or is a better-placed replica expected to?
    ///
    /// Returns true when the published positions name this node as the most advanced, when there
    /// are no positions at all (the pre-#70 behaviour: a CAS race), or when the deference window
    /// has expired. Returns false while deferring.
    bool should_promote_now();

    /// Follow whoever holds the leader key, if anyone does. Returns true when this node is now a
    /// REPLICA of a live leader.
    ///
    /// Must be called without `mtx_` held: it calls `reconcile_epoch()` and the role-transition
    /// handler, both of which take locks of their own.
    ///
    /// **Adoption happens once per leader change** (roadmap #104): this is reached from the
    /// STANDALONE branch and after a lost CAS, and it stores `REPLICA` into `role_` before
    /// returning, so the next pass of `monitor_loop()` takes the REPLICA branch - which restarts
    /// replication only for a leader that is not the one in `following_`. A replica watching an
    /// unchanged leader therefore restarts nothing. That branch restarted nothing in any case until
    /// #201, and a primary that demoted before anyone was elected, which follows nobody, never
    /// followed the node elected after it. (There used to be an `adopted_primary_address_` field
    /// assigned at one site and read at none; `following_` is read by that branch, and
    /// `FieldUsage.NoMemberIsWrittenAndNeverRead` is what keeps that so.)
    ///
    /// Exists because losing a race is not a role. `attempt_promotion()` used to return on a lost
    /// CAS without touching `role_`, which left a node at STANDALONE — a state `monitor_loop()`
    /// had no branch for, so the node never campaigned, never replicated, and never took over
    /// (roadmap #73).
    bool adopt_leader_if_present();

    /// Record what this node's replication client now follows (#201); see `following_`.
    void note_following(const std::string& address);

    /// Whether this node is currently holding a leader key it won and could not act on — the
    /// state #130 is about. Loud once and then quiet, because the condition repeats at the tick
    /// rate for as long as the storage that refused the epoch record goes on refusing it.
    ///
    /// Touched only by the monitor thread.
    LogEpisode stalled_promotion_;

    /// Publish this node's WAL position to the coordinator, at most once per second.
    ///
    /// Nothing did this before: `publish_wal_position()` was called from tests and from one
    /// connectivity check, so `get_published_positions()` was always empty on a real cluster and
    /// `FAILOVER <target>` answered ERR unknown_target every time (roadmap #60). The positions are
    /// also what `elect_winner()` was written to compare, though nothing calls that yet.
    void publish_position_if_due();

    /// Grant, or keep alive, the lease that keeps this node's published position present.
    ///
    /// Returns the lease id, or 0 when no lease could be had — in which case the caller publishes
    /// without one and the position outlives the node, which is what happened before #72.
    ///
    /// The re-grant path only works because #74 made `refresh_lease()` capable of failing: before
    /// that, a keepalive for a lease etcd had forgotten answered 200 and this function would have
    /// gone on publishing under a lease that no longer existed.
    int64_t ensure_position_lease();
    void handle_lease_expiry();

    /// Why this node is standing. A handover's successor that holds the whole outgoing stream does
    /// not defer to a further-published position (#70): that position belongs to the node that
    /// handed over, which takes no writes and cannot stand, and nobody has more of its log than
    /// it streamed.
    enum class PromotionBasis { Election, Handover };
    void attempt_promotion(PromotionBasis basis = PromotionBasis::Election);
    void handle_primary_lease_lost();

    /// The highest term this node knows a leader at: the coordinator's, as reconciled, or the
    /// engine's, raised by the stream it replicates. What a handover's statement is checked against.
    uint64_t known_leader_term() const;

    /// The REPLICA branch with the leader key vacant: wait, defer, or stand (#82, #204).
    void act_on_vacant_leader();
    void reconcile_epoch(const ClusterState& state);
};

// ── Election helper (exposed for testing) ────────────────────────────────────

/// Given a set of published positions, return the election winner:
/// highest WAL position (file_index, byte_offset), tie-break by lowest node_id.
/// Returns nullptr if positions is empty.
const PublishedPosition* elect_winner(const std::vector<PublishedPosition>& positions);

// ── Election decision ─────────────────────────────────────────────────────────

/// What a candidate should do about promotion right now.
enum class ElectionDecision {
    PromoteNow,          ///< nobody better is published, or we are the best
    Defer,               ///< a further position exists and the window has not run out
    PromoteAfterWindow,  ///< a further position exists but its owner never promoted
};

/// Decide, from the published positions alone, whether this node should take the role.
///
/// Pure on purpose: the cases worth testing — we are best, someone else is, the window expiring,
/// nothing published at all — then need no etcd, no cluster and no clock. The caller owns the
/// bookkeeping (when deferral started, logging, metrics), which is the part that needs a running
/// node and is not where the mistakes live.
/// Whether a candidate has waited long enough since the leader key first went absent (#82).
///
/// Pure for the same reason `decide_election()` is: the cases worth testing — never saw it absent,
/// saw it a moment ago, waited exactly the window, waited longer, a wait of zero — need no etcd and
/// no cluster. The caller owns the bookkeeping of *when* absence was first seen, which is where a
/// running node is required and is not where the mistakes live.
///
/// `absent_since` empty means no absence has been observed, and the answer is false. That is what
/// stops a node from campaigning on the strength of a read that failed: "I could not find out" must
/// not be recorded as "the key is gone".
bool election_wait_elapsed(std::optional<std::chrono::steady_clock::time_point> absent_since,
                           std::chrono::steady_clock::time_point now,
                           int64_t wait_ms);

ElectionDecision decide_election(const std::vector<PublishedPosition>& positions,
                                 const std::string& self_node_id,
                                 std::chrono::milliseconds deferred_for,
                                 std::chrono::milliseconds window);

// ── Absent leader key ─────────────────────────────────────────────────────────

/// What a node that believes it is PRIMARY should do when the leader key reads as absent.
enum class AbsentKeyAction {
    StepDown,            ///< the lease is genuinely gone; #82's unconditional demotion applies
    HandoverInFlight,    ///< we revoked it ourselves and are a moment from recording the new role
    RoleAlreadyChanged,  ///< the role moved on while we were asking etcd about the key
};

/// Decide what an absent leader key means, given the role *now* and whether a handover is in flight.
///
/// Pure for the same reason `decide_election()` is, and here the reason is sharper: the situation
/// this rules out is a race that reproduces about **one run in three** (measured, #89), so a test
/// that waits for it reads as flaky and gets re-run rather than read. As a function, all six
/// combinations are one assertion each.
///
/// `handing_over` is checked before the role, because during a handover the role may still say
/// PRIMARY — the revoke happens first — so the more specific answer would otherwise be lost to the
/// more general one.
///
/// Why two conditions rather than one: `monitor_loop()` reads the role at the top of an iteration
/// and the key later in the same one, with a network round trip between them. A handover *in
/// flight* is the flag's case; a handover that started and finished inside that gap has already
/// cleared the flag, and only the role says so.
AbsentKeyAction decide_on_absent_key(NodeRole role_now, bool handing_over);

// ── Vacant leader key ─────────────────────────────────────────────────────────

/// What a REPLICA does while the leader key is vacant.
enum class VacantLeaderAction {
    Wait,              ///< the election wait (#82) has not elapsed, and nothing lets this node skip it
    Defer,             ///< a live handover names another node
    AwaitStream,       ///< a handover names this node, which lacks part of the outgoing stream
    Stand,             ///< an ordinary election: the wait has elapsed
    StandForHandover,  ///< a handover names this node, its sender stepped down at the term this
                       ///< node knows, and this node holds all of its stream (#204)
};

/// Decide, from the intent and what this node knows, whether it stands now.
///
/// Pure for the reason `decide_on_absent_key()` is: the case that matters - a successor standing
/// without the election wait - is safe only under a conjunction (the intent live, naming us, its
/// sender's statement at the term we know, our position at the end of its stream), and each
/// conjunct is one assertion here rather than a cluster arranged to break it.
///
/// `known_term` is the highest term this node knows a leader at. A statement at another term says
/// nothing about the leader that term had - an intent that outlived its handover, say. Without a
/// statement, or once the wait has elapsed, this is the election it was before #204.
VacantLeaderAction decide_on_vacant_leader(const std::optional<HandoverIntent>& intent,
                                           const std::string& self_node_id, uint64_t now_ns,
                                           uint64_t known_term,
                                           const std::optional<StreamPosition>& replicated,
                                           bool wait_elapsed);

} // namespace ob
