#include "orderbook/metrics.hpp"
#include "orderbook/version.hpp"
#include "orderbook/logger.hpp"

#include <cmath>
#include <cstdio>
#include <sstream>

namespace ob {

// ── Construction: pre-register all metrics ────────────────────────────────────

static std::unique_ptr<CounterEntry> make_counter(std::string name, std::string help) {
    auto e = std::make_unique<CounterEntry>();
    e->name = std::move(name);
    e->help = std::move(help);
    return e;
}

static std::unique_ptr<GaugeEntry> make_gauge(std::string name, std::string help) {
    auto e = std::make_unique<GaugeEntry>();
    e->name = std::move(name);
    e->help = std::move(help);
    return e;
}

static std::unique_ptr<HistogramEntry> make_histogram(std::string name, std::string help) {
    auto e = std::make_unique<HistogramEntry>();
    e->name = std::move(name);
    e->help = std::move(help);
    return e;
}

MetricsRegistry::MetricsRegistry() {
    // Counters
    counters_.push_back(make_counter("ob_total_inserts",         "Total number of insert operations"));
    counters_.push_back(make_counter("ob_total_queries",         "Total number of query operations"));
    counters_.push_back(make_counter("ob_total_flushes",         "Total number of flush operations"));
    // Streaming subscriptions (#45).
    counters_.push_back(make_counter("ob_subscription_rows_pushed_total",
                                     "Rows delivered to subscribers over the wire"));
    counters_.push_back(make_counter("ob_subscription_overflow_disconnects_total",
                                     "Sessions closed because a subscriber queue passed its "
                                     "ceiling. The only way an operator learns that a consumer "
                                     "cannot keep up."));
    counters_.push_back(make_counter("ob_subscription_refused_total",
                                     "Subscriptions refused: unparseable query or per-session "
                                     "limit reached"));
    // Wire authentication (#30). Unlabelled by identity, and that is a decision rather than a
    // limitation of the registry: per-identity attribution belongs to #31, where an identity gains
    // permissions and therefore meaning. While every authenticated identity may run every command,
    // a counter per identity answers a question nothing can be done about. What must never happen
    // is a label fed by the identity a peer *claims* before authenticating - that is an unbounded
    // label set an attacker controls.
    counters_.push_back(make_counter("ob_auth_challenges_total",
                                     "Challenges issued to client sessions"));
    counters_.push_back(make_counter("ob_auth_success_total",
                                     "Client sessions that authenticated successfully"));
    counters_.push_back(make_counter("ob_auth_failures_total",
                                     "Failed client authentication attempts. Each one also closed "
                                     "a session, so a rising number means someone is trying."));
    counters_.push_back(make_counter("ob_refused_commands_total",
                                     "Command lines the parser refused: an unknown word, or a known "
                                     "command carrying a token its grammar has no place for. A "
                                     "client that is working does not produce these, so any rate "
                                     "at all is a client sending something it thinks is being "
                                     "stored (#107). Logged once per connection, counted every "
                                     "time - the count is the alertable half."));
    counters_.push_back(make_counter("ob_wal_records_written",
                                     "WAL records appended, of every type - so checkpoints, "
                                     "epochs, version vectors and gaps are in it as well as "
                                     "client writes. That is what makes it worth having beside "
                                     "ob_inserts_total instead of being a duplicate of it: the "
                                     "difference is the bookkeeping this WAL does on its own "
                                     "behalf (#117)"));
    counters_.push_back(make_counter("ob_repl_records_replayed",
                                     "Replication records this node has applied as a replica. "
                                     "Accumulates across role changes, although the object that "
                                     "counts them is rebuilt by each one, so a failover does not "
                                     "reset it (#117)"));
    // A flush tick that threw. Registered in the same change that writes it, because an
    // unregistered counter is discarded and reports a flat zero for ever - and this is the
    // number that tells an operator a full disk is stopping flushes while the node still
    // answers clients (#112).
    counters_.push_back(make_counter("ob_flush_errors_total",
                                     "Flush ticks that ended in an exception and were retried"));
    // A session dropped for holding more unparsed input than a command can legitimately need
    // (#143). Registered in the same change that writes it. It is the only external sign of this
    // refusal, and it distinguishes the two ways a client can reach it: bytes with no newline at
    // all, and a `MINSERT` that announced levels and then sent oversized payload lines. Neither
    // was bounded before, and neither could be seen by the per-line length check.
    counters_.push_back(make_counter("ob_sessions_unparsed_overflow_total",
                                     "Sessions closed for unparsed input above the ceiling"));
    // Failed `fsync` calls on the WAL (#113). Separate from ob_flush_errors_total because the
    // two ask for different actions: a full disk is freed, a disk reporting EIO is replaced.
    counters_.push_back(make_counter("ob_wal_fsync_errors_total",
                                     "fsync calls on the WAL that failed"));
    // Flushes whose segment sync failed (#160). Registered in the same change that writes it. Its
    // consequence is not a lost row but a checkpoint not written: the segments stay merged and
    // readable, WAL retention stops advancing, and a power cut in that state replays from the last
    // flush that synced. A number that climbs here is a disk that accepts writes and refuses to
    // make them durable, which is the one state in which the WAL grows without anything failing.
    counters_.push_back(make_counter("ob_segment_sync_errors_total",
                                     "Flushes whose segment syncfs() failed, so no checkpoint claimed them"));
    // Segments removed at startup because no surviving checkpoint vouched for them, their rows
    // rebuilt from the WAL (#160). Registered in the same change that writes it. Nonzero after a
    // crash that cut a flush short, which is expected; nonzero after a clean restart is not.
    counters_.push_back(make_counter("ob_segments_rebuilt_from_wal_total",
                                     "Segments removed at startup and rebuilt from the WAL"));
    // A writer that ran out of room in the pending queue, and one whose wait for room ran out
    // (#137). The pair matters: waits without refusals is backpressure working, and refusals
    // mean the flush itself is not making progress.
    // How often the flush loop ran. The interval says how often it *should*, and the two
    // disagreeing is the observable half of #137's request mechanism: a flag the loop never
    // clears turns a bounded wait into a loop that flushes as fast as it can, which costs a core
    // and reports nothing.
    counters_.push_back(make_counter("ob_flush_ticks_total",
                                     "flush loop iterations that ran a tick"));
    counters_.push_back(make_counter("ob_writer_backpressure_waits_total",
                                     "writes that waited for room in the pending queue"));
    counters_.push_back(make_counter("ob_writer_backpressure_refusals_total",
                                     "writes refused because the pending queue never freed room"));
    // Mesh events whose handling threw and which the io loop abandoned (#112). Registered in the
    // same change that writes it: measured, an ENOSPC on a peer's delta used to end that thread
    // outright, and every outside signal - PING, MM_PEERS, the peer's `connected` row - stayed
    // healthy while the node received nothing. This is the number that contradicts them.
    counters_.push_back(make_counter("ob_mm_io_errors_total",
                                     "Mesh events abandoned because handling them threw"));
    // Monitor ticks that threw and were retried (#112). The counter exists because the boundary
    // makes the thread survive: measured without it, an ENOSPC on the EPOCH record a promotion
    // writes ended that thread and left the node reporting REPLICA <its own replication port> for
    // ever, answering PING the whole time. Nothing outward changes when a tick fails, so this is
    // the only thing to alarm on.
    counters_.push_back(make_counter("ob_monitor_errors_total",
                                     "Failover monitor ticks that ended in an exception"));
    // Replication io loop failures, per event abandoned and per pass abandoned (#112). Registered
    // in the same change that writes it, and honest about what it is: no path in this tree is
    // known to reach it - unlike the three counters above, each of which was measured firing - so
    // it is a ratchet. What makes it worth registering anyway is that the boundary under it turns
    // the first such exception from "this node stops serving replicas" into one line and one
    // increment, and an increment nobody registered is discarded in silence (#77).
    counters_.push_back(make_counter("ob_repl_io_errors_total",
                                     "Replication io loop events or passes abandoned because "
                                     "they threw"));
    // Lease refreshes that threw (#112). A ratchet like the one above and for the same reason -
    // `CoordinatorClient` contains no `throw` and `refresh_lease()` answers failure with `false`
    // - but the thing behind it is the least recoverable of the four loops: the lease this thread
    // holds open is what keeps this node in the mesh registry, and since #132 the repair for a
    // lost registration lives in **this same loop**. Measured before that fix: revoke the lease
    // and the key never comes back while the node answers PING. A thread that ends now takes the
    // repair with it, which is the same condition reached a different way.
    counters_.push_back(make_counter("ob_peer_lease_errors_total",
                                     "Peer registry lease refreshes that ended in an exception"));
    // The other seven loops, together (#131). One counter rather than seven, and that is a
    // decision: each of the four above asks an operator for a different thing - free the disk,
    // replace the device, look at why a promotion stopped, look at why a lease will not refresh -
    // and three of them were measured firing. None of the seven has a known trigger, so all seven
    // ask for the same thing: read the line, it names the loop. Seven registered counters nothing
    // can reach would be seven flat zeros dressed as coverage (#117). Two of the seven cannot feed
    // it at all - the shard router and the client pool run in somebody else's process and have no
    // registry - and `LoopGuard` takes a null registry rather than pretending otherwise.
    counters_.push_back(make_counter("ob_loop_errors_total",
                                     "Iterations abandoned because they threw, across the loops "
                                     "guarded by LoopGuard"));

    // Gauges
    gauges_.push_back(make_gauge("ob_active_sessions", "Number of active TCP sessions"));
    gauges_.push_back(make_gauge("ob_session_pending_bytes",
                                 "Response bytes queued across sessions (a slow client shows up here)"));
    // Streaming subscriptions (#45). Registered in the same task that writes them, because
    // set_gauge() on an unregistered name is dropped in silence — that was #77, five dead gauges
    // serving a flat zero while the engine worked — and scripts/check_metrics.py fails CI for a
    // written-but-unregistered name.
    gauges_.push_back(make_gauge("ob_subscriptions_active",
                                 "Live streaming subscriptions"));
    gauges_.push_back(make_gauge("ob_subscription_queued_bytes",
                                 "Push bytes queued across subscribers (a consumer that stopped "
                                 "reading shows up here before it is disconnected)"));
    gauges_.push_back(make_gauge("ob_pending_rows",    "Number of rows pending flush"));
    gauges_.push_back(make_gauge("ob_wal_file_index",  "Current WAL file index"));
    gauges_.push_back(make_gauge("ob_segment_count",   "Number of columnar segments"));
    gauges_.push_back(make_gauge("ob_segment_merge_refused",
                                 "Segments refused as already indexed (a flush race; should stay 0)"));
    gauges_.push_back(make_gauge("ob_symbol_count",    "Number of tracked symbols"));
    gauges_.push_back(make_gauge("ob_current_epoch",   "Current failover epoch"));
    // TLS on the node links (#30 part three, series D). Registered in the same change that writes
    // them: `set_gauge()` on an unregistered name is dropped in silence, which is how five gauges
    // served a flat zero while the engine worked (#77), and `scripts/check_metrics.py` fails CI for
    // a written-but-unregistered name.
    gauges_.push_back(make_gauge("ob_replication_lag_bytes",
                                 "Bytes the furthest-behind replica has yet to acknowledge, "
                                 "counted across WAL files. Unlike the mesh, a replica streams "
                                 "*this* node's WAL and acknowledges into it, so bytes are a "
                                 "quantity two sides can compare - which is the distinction #118 "
                                 "measured. Read it beside ob_replicas_lag_unknown"));
    gauges_.push_back(make_gauge("ob_replicas_lag_unknown",
                                 "Connected replicas whose lag cannot be measured because a WAL "
                                 "file between their position and ours is gone. Worse than a "
                                 "large lag rather than merely unmeasured: retention keeps files "
                                 "back to the slowest connected replica, so a missing one says "
                                 "that replica can no longer catch up from this log and needs a "
                                 "snapshot"));
    gauges_.push_back(make_gauge("ob_replicas_connected",
                                 "Replicas currently connected to this primary. Exported next to "
                                 "the verified count because the guarantee is the comparison, and "
                                 "a number an operator has to read off STATUS cannot be alerted "
                                 "on."));
    gauges_.push_back(make_gauge("ob_replicas_tls_verified",
                                 "Connected replicas that presented a certificate this node "
                                 "verified. Equal to ob_replicas_connected on a link running with "
                                 "--tls-replication; a gap is a replica talking plaintext."));

    // Histograms
    histograms_.push_back(make_histogram("ob_insert_latency_seconds", "Insert operation latency in seconds"));
    histograms_.push_back(make_histogram("ob_flush_latency_seconds",  "Flush operation latency in seconds"));
    histograms_.push_back(make_histogram("ob_query_latency_seconds",  "Query operation latency in seconds"));

    // Multi-master metrics
    gauges_.push_back(make_gauge("ob_mm_peers_connected",          "Number of connected multi-master peers"));
    gauges_.push_back(make_gauge("ob_mm_peers_tls_verified",
                                 "Connected peers that presented a certificate this node verified. "
                                 "Equal to ob_mm_peers_connected on a mesh running with "
                                 "--tls-multi-master, and zero without it."));
    counters_.push_back(make_counter("ob_mm_conflicts_total",      "Total number of multi-master conflicts resolved"));
    // Written by Engine::apply_remote_delta() since receive-side dedup existed, and never
    // registered — so /metrics reported a flat zero for the one number that says whether
    // over-delivery is being handled. The registry logs an ERROR for every such write; that log
    // line is how this was found.
    counters_.push_back(make_counter("ob_mm_duplicates_dropped",   "Remote records refused because this node had already applied them"));
    // Separate from ob_mm_duplicates_dropped rather than shared with it: two links
    // over-deliver for different reasons, and one counter cannot say which one is doing it.
    counters_.push_back(make_counter("ob_replication_duplicates_dropped",
                                     "Replicated records refused because this node had already applied them"));
    counters_.push_back(make_counter("ob_sequence_gaps_detected",  "Gaps detected in an origin's sequence numbering"));
    // `ob_mm_replication_lag_bytes` used to be registered here and was never written, which is
    // how #117 found it. It is **removed** rather than fed, because #118 measured that the mesh
    // has no byte position to compute it from: a peer's `confirmed_offset` is an offset into that
    // peer's own WAL, recorded once at handshake. The two gauges below are the replacement, and
    // they are a different question with a different unit - which is exactly why the old name
    // could not be reused.
    gauges_.push_back(make_gauge("ob_mm_replication_lag_records",
                                 "Records the furthest-behind mesh peer is known to be missing, "
                                 "from the per-origin sequence vectors - the one position two "
                                 "nodes can compare. Peers that have not stated what they hold "
                                 "are excluded and counted in ob_mm_peers_position_unknown, "
                                 "because a peer that has said nothing is reported by the "
                                 "comparison as holding nothing. Recomputed once per "
                                 "anti-entropy pass, so it is as stale as "
                                 "--anti-entropy-interval-seconds allows"));
    gauges_.push_back(make_gauge("ob_mm_peers_position_unknown",
                                 "Connected peers that have not said what they hold. Read it "
                                 "beside ob_mm_replication_lag_records: zero lag with a nonzero "
                                 "count here means 'we do not know', which is a different answer "
                                 "from 'converged' and would otherwise share its number"));
    counters_.push_back(make_counter("ob_mm_anti_entropy_runs_total",    "Total number of anti-entropy runs"));
    counters_.push_back(make_counter("ob_mm_anti_entropy_repairs_total", "Total number of anti-entropy repairs"));
    gauges_.push_back(make_gauge("ob_mm_reconcile_gaps_detected",
                                 "Symbol/origin pairs where this node and a peer disagree, both directions"));
    gauges_.push_back(make_gauge("ob_mm_peer_send_buf_bytes",
                                 "Queued output for the most recently written peer, in bytes"));
    counters_.push_back(make_counter("ob_mm_peer_dropped_slow_total",
                                     "Peers dropped for not draining their queued output"));
    gauges_.push_back(make_gauge("ob_mm_reconcile_we_lack",
                                 "Of those, the pairs where this node is the one behind"));
    counters_.push_back(make_counter("ob_mm_backpressure_snapshot_total",
                                     "Times a peer fell back to snapshot sync under backpressure"));
    // Snapshot bootstrap over the multi-master protocol (#76).
    counters_.push_back(make_counter("ob_mm_snapshot_requested_total",
                                     "Snapshots this node asked a peer for"));
    counters_.push_back(make_counter("ob_mm_snapshot_sent_total",
                                     "Snapshots streamed to a peer in full"));
    counters_.push_back(make_counter("ob_mm_snapshot_received_total",
                                     "Snapshots received, verified and installed"));
    counters_.push_back(make_counter("ob_mm_snapshot_refused_total",
                                     "Snapshot requests refused because one was already in flight"));
    counters_.push_back(make_counter("ob_mm_snapshot_failed_total",
                                     "Snapshot transfers abandoned, in either direction"));
    counters_.push_back(make_counter("ob_mm_snapshot_bytes_sent_total",
                                     "Bytes of snapshot chunk payload sent"));
    counters_.push_back(make_counter("ob_mm_snapshot_bytes_received_total",
                                     "Bytes of snapshot chunk payload received"));
    counters_.push_back(make_counter("ob_mm_records_dropped_bootstrapping_total",
                                     "Remote records dropped, unrecorded, while installing a snapshot"));
    counters_.push_back(make_counter("ob_mm_snapshot_discarded_total",
                                     "Snapshots created and then thrown away because the peer that "
                                     "asked for one had gone (#79)"));
    gauges_.push_back(make_gauge("ob_mm_snapshot_create_ms",
                                 "Milliseconds the last snapshot creation took"));
    gauges_.push_back(make_gauge("ob_mm_snapshot_prepare_ms",
                                 "Milliseconds from a snapshot request to its result being "
                                 "collected by the io loop (#79)"));
    gauges_.push_back(make_gauge("ob_mm_hlc_drift_ns",             "Maximum HLC drift in nanoseconds"));
    // How often the clock has been over the drift boundary, beside how far it went. Registered in
    // the same change that writes it, because a registration nobody feeds reads as zero and that
    // is the one value an operator cannot tell from good news (#117). The pair matters: a single
    // ten-second excursion and a clock that is permanently an hour out give the same peak, and
    // ob_mm_hlc_drift_ns never comes down because nothing lowers the HLC's physical component.
    // Peers refused for an implausible clock (#121). One per record refused, not one per peer, so
    // a node whose clock is wrong and which keeps writing is visible as a rate - while the log
    // line is one per episode. The pair is the same shape the drift metrics already have: this
    // counts occurrences, `ob_mm_hlc_drift_ns` says how far.
    counters_.push_back(make_counter("ob_mm_peer_dropped_clock_total",
                                     "Remote records refused, and their peer dropped, because "
                                     "the peer's clock was too far ahead of ours"));
    counters_.push_back(make_counter("ob_mm_hlc_drift_excursions_total",
                                     "Ticks that found the HLC more than a second ahead of the "
                                     "wall clock. The log says this twice per excursion, at its "
                                     "edges (#120); this counts every occurrence, so it is the "
                                     "alertable half"));
}

// ── Lookup helpers ────────────────────────────────────────────────────────────

CounterEntry* MetricsRegistry::find_counter(std::string_view name) {
    for (auto& c : counters_) {
        if (c->name == name) return c.get();
    }
    return nullptr;
}

const CounterEntry* MetricsRegistry::find_counter(std::string_view name) const {
    for (auto& c : counters_) {
        if (c->name == name) return c.get();
    }
    return nullptr;
}

GaugeEntry* MetricsRegistry::find_gauge(std::string_view name) {
    for (auto& g : gauges_) {
        if (g->name == name) return g.get();
    }
    return nullptr;
}

const GaugeEntry* MetricsRegistry::find_gauge(std::string_view name) const {
    for (auto& g : gauges_) {
        if (g->name == name) return g.get();
    }
    return nullptr;
}

HistogramEntry* MetricsRegistry::find_histogram(std::string_view name) {
    for (auto& h : histograms_) {
        if (h->name == name) return h.get();
    }
    return nullptr;
}

const HistogramEntry* MetricsRegistry::find_histogram(std::string_view name) const {
    for (auto& h : histograms_) {
        if (h->name == name) return h.get();
    }
    return nullptr;
}

// ── Counter operations ────────────────────────────────────────────────────────

void MetricsRegistry::increment_counter(std::string_view name, uint64_t delta) {
    if (auto* c = find_counter(name)) {
        c->value.fetch_add(delta, std::memory_order_relaxed);
        return;
    }
    report_unknown_metric("counter", name);
}

uint64_t MetricsRegistry::counter_value(std::string_view name) const {
    if (auto* c = find_counter(name)) {
        return c->value.load(std::memory_order_relaxed);
    }
    return 0;
}

void MetricsRegistry::report_unknown_metric(std::string_view kind,
                                            std::string_view name) const {
    unknown_metric_writes_.fetch_add(1, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lock(unknown_names_mtx_);
        if (!unknown_names_reported_.insert(std::string(name)).second) {
            return;  // already reported; callers can run ten times a second
        }
    }
    OB_LOG_ERROR("metrics",
                 "Write to unregistered %s '%.*s': the value is discarded and "
                 "/metrics will report a flat zero. Check the name against the "
                 "registrations in MetricsRegistry::MetricsRegistry()",
                 std::string(kind).c_str(),
                 static_cast<int>(name.size()), name.data());
}

// ── Gauge operations ──────────────────────────────────────────────────────────

void MetricsRegistry::set_gauge(std::string_view name, int64_t value) {
    if (auto* g = find_gauge(name)) {
        g->value.store(value, std::memory_order_relaxed);
        return;
    }
    report_unknown_metric("gauge", name);
}

void MetricsRegistry::increment_gauge(std::string_view name, int64_t delta) {
    if (auto* g = find_gauge(name)) {
        g->value.fetch_add(delta, std::memory_order_relaxed);
        return;
    }
    report_unknown_metric("gauge", name);
}

int64_t MetricsRegistry::gauge_value(std::string_view name) const {
    if (auto* g = find_gauge(name)) {
        return g->value.load(std::memory_order_relaxed);
    }
    return 0;
}

// ── Histogram operations ──────────────────────────────────────────────────────

void MetricsRegistry::observe_histogram(std::string_view name, double seconds, uint64_t count) {
    auto* h = find_histogram(name);
    if (!h) {
        report_unknown_metric("histogram", name);
        return;
    }
    if (count == 0) return;

    auto& d = h->data;

    // Increment matching buckets (cumulative: all buckets >= observation)
    for (size_t i = 0; i < kNumBuckets; ++i) {
        if (seconds <= kLatencyBuckets[i]) {
            d.buckets[i].fetch_add(count, std::memory_order_relaxed);
        }
    }
    // +Inf bucket always incremented
    d.buckets[kNumBuckets].fetch_add(count, std::memory_order_relaxed);

    d.count.fetch_add(count, std::memory_order_relaxed);

    // Convert seconds to nanoseconds for integer precision
    auto ns = static_cast<int64_t>(seconds * 1e9);
    d.sum_ns.fetch_add(ns * static_cast<int64_t>(count), std::memory_order_relaxed);
}

// ── Direct histogram access ───────────────────────────────────────────────────

const HistogramData* MetricsRegistry::histogram_data(std::string_view name) const {
    if (auto* h = find_histogram(name)) {
        return &h->data;
    }
    return nullptr;
}

// ── Serialization ─────────────────────────────────────────────────────────────

void MetricsRegistry::set_node_role(std::string_view role) {
    std::lock_guard<std::mutex> lock(serialize_mtx_);
    node_role_ = std::string(role);
}

std::string MetricsRegistry::serialize() const {
    std::lock_guard<std::mutex> lock(serialize_mtx_);

    std::ostringstream out;

    const auto& role = node_role_;

    // Which build is answering, as a labelled gauge fixed at 1 — the conventional shape for build
    // information in Prometheus, and the third place #90 made the version askable. A monitoring
    // system that scrapes a fleet can now tell a node running an old binary from one running the
    // new one, which was not a question this engine could answer at all.
    out << "# HELP ob_build_info Build information for this node; the value is always 1\n";
    out << "# TYPE ob_build_info gauge\n";
    out << "ob_build_info{version=\"" << version() << "\",node_role=\"" << role << "\"} 1\n";

    // Counters
    for (auto& c : counters_) {
        out << "# HELP " << c->name << " " << c->help << "\n";
        out << "# TYPE " << c->name << " counter\n";
        out << c->name << "{node_role=\"" << role << "\"} "
            << c->value.load(std::memory_order_relaxed) << "\n";
    }

    // Gauges
    for (auto& g : gauges_) {
        out << "# HELP " << g->name << " " << g->help << "\n";
        out << "# TYPE " << g->name << " gauge\n";
        out << g->name << "{node_role=\"" << role << "\"} "
            << g->value.load(std::memory_order_relaxed) << "\n";
    }

    // Histograms
    for (auto& h : histograms_) {
        auto& d = h->data;

        out << "# HELP " << h->name << " " << h->help << "\n";
        out << "# TYPE " << h->name << " histogram\n";

        // Bucket lines
        for (size_t i = 0; i < kNumBuckets; ++i) {
            char le_buf[32]{};
            std::snprintf(le_buf, sizeof(le_buf), "%.6g", kLatencyBuckets[i]);
            out << h->name << "_bucket{node_role=\"" << role
                << "\",le=\"" << le_buf << "\"} "
                << d.buckets[i].load(std::memory_order_relaxed) << "\n";
        }
        // +Inf bucket
        out << h->name << "_bucket{node_role=\"" << role
            << "\",le=\"+Inf\"} "
            << d.buckets[kNumBuckets].load(std::memory_order_relaxed) << "\n";

        // Sum (convert nanoseconds back to seconds with high precision)
        double sum_sec = static_cast<double>(d.sum_ns.load(std::memory_order_relaxed)) / 1e9;
        char sum_buf[64]{};
        std::snprintf(sum_buf, sizeof(sum_buf), "%.9g", sum_sec);
        out << h->name << "_sum{node_role=\"" << role << "\"} " << sum_buf << "\n";

        // Count
        out << h->name << "_count{node_role=\"" << role << "\"} "
            << d.count.load(std::memory_order_relaxed) << "\n";
    }

    return out.str();
}

} // namespace ob
