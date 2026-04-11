#include "orderbook/metrics.hpp"

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
    counters_.push_back(make_counter("ob_wal_records_written",   "Total number of WAL records written"));
    counters_.push_back(make_counter("ob_repl_records_replayed", "Total number of replication records replayed"));

    // Gauges
    gauges_.push_back(make_gauge("ob_active_sessions", "Number of active TCP sessions"));
    gauges_.push_back(make_gauge("ob_pending_rows",    "Number of rows pending flush"));
    gauges_.push_back(make_gauge("ob_wal_file_index",  "Current WAL file index"));
    gauges_.push_back(make_gauge("ob_segment_count",   "Number of columnar segments"));
    gauges_.push_back(make_gauge("ob_symbol_count",    "Number of tracked symbols"));
    gauges_.push_back(make_gauge("ob_current_epoch",   "Current failover epoch"));

    // Histograms
    histograms_.push_back(make_histogram("ob_insert_latency_seconds", "Insert operation latency in seconds"));
    histograms_.push_back(make_histogram("ob_flush_latency_seconds",  "Flush operation latency in seconds"));
    histograms_.push_back(make_histogram("ob_query_latency_seconds",  "Query operation latency in seconds"));
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
    }
}

uint64_t MetricsRegistry::counter_value(std::string_view name) const {
    if (auto* c = find_counter(name)) {
        return c->value.load(std::memory_order_relaxed);
    }
    return 0;
}

// ── Gauge operations ──────────────────────────────────────────────────────────

void MetricsRegistry::set_gauge(std::string_view name, int64_t value) {
    if (auto* g = find_gauge(name)) {
        g->value.store(value, std::memory_order_relaxed);
    }
}

void MetricsRegistry::increment_gauge(std::string_view name, int64_t delta) {
    if (auto* g = find_gauge(name)) {
        g->value.fetch_add(delta, std::memory_order_relaxed);
    }
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
    if (!h) return;

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
