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
    counters_.push_back(make_counter("ob_wal_records_written",   "Total number of WAL records written"));
    counters_.push_back(make_counter("ob_repl_records_replayed", "Total number of replication records replayed"));

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

    // Histograms
    histograms_.push_back(make_histogram("ob_insert_latency_seconds", "Insert operation latency in seconds"));
    histograms_.push_back(make_histogram("ob_flush_latency_seconds",  "Flush operation latency in seconds"));
    histograms_.push_back(make_histogram("ob_query_latency_seconds",  "Query operation latency in seconds"));

    // Multi-master metrics
    gauges_.push_back(make_gauge("ob_mm_peers_connected",          "Number of connected multi-master peers"));
    counters_.push_back(make_counter("ob_mm_conflicts_total",      "Total number of multi-master conflicts resolved"));
    // Written by Engine::apply_remote_delta() since receive-side dedup existed, and never
    // registered — so /metrics reported a flat zero for the one number that says whether
    // over-delivery is being handled. The registry logs an ERROR for every such write; that log
    // line is how this was found.
    counters_.push_back(make_counter("ob_mm_duplicates_dropped",   "Remote records refused because this node had already applied them"));
    counters_.push_back(make_counter("ob_sequence_gaps_detected",  "Gaps detected in an origin's sequence numbering"));
    gauges_.push_back(make_gauge("ob_mm_replication_lag_bytes",    "Replication lag in bytes (max across peers)"));
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

#ifdef OB_USE_IO_URING
    // io_uring metrics
    gauges_.push_back(make_gauge("ob_iouring_sq_utilization", "Submission Queue utilization percentage (0-100)"));
    counters_.push_back(make_counter("ob_iouring_cq_overflows", "Number of Completion Queue overflows"));
    counters_.push_back(make_counter("ob_iouring_sqe_submitted", "Total number of SQEs submitted"));
    counters_.push_back(make_counter("ob_iouring_cqe_processed", "Total number of CQEs processed"));
#endif
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

void MetricsRegistry::observe_histogram(std::string_view name, double seconds) {
    auto* h = find_histogram(name);
    if (!h) {
        report_unknown_metric("histogram", name);
        return;
    }

    auto& d = h->data;

    // Increment matching buckets (cumulative: all buckets >= observation)
    for (size_t i = 0; i < kNumBuckets; ++i) {
        if (seconds <= kLatencyBuckets[i]) {
            d.buckets[i].fetch_add(1, std::memory_order_relaxed);
        }
    }
    // +Inf bucket always incremented
    d.buckets[kNumBuckets].fetch_add(1, std::memory_order_relaxed);

    d.count.fetch_add(1, std::memory_order_relaxed);

    // Convert seconds to nanoseconds for integer precision
    auto ns = static_cast<int64_t>(seconds * 1e9);
    d.sum_ns.fetch_add(ns, std::memory_order_relaxed);
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
