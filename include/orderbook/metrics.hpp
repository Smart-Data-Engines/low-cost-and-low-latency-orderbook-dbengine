#pragma once

#include <atomic>
#include <array>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace ob {

// ── Histogram bucket boundaries (compile-time) ───────────────────────────────
inline constexpr double kLatencyBuckets[] = {
    0.00001, 0.00005, 0.0001, 0.0005, 0.001,
    0.005,   0.01,    0.05,   0.1,    0.5,
    1.0
};
inline constexpr size_t kNumBuckets = sizeof(kLatencyBuckets) / sizeof(double);

// ── HistogramData ─────────────────────────────────────────────────────────────
// Lock-free histogram storage. Each bucket counts observations <= boundary.
// The last bucket (index kNumBuckets) is the +Inf bucket.
struct HistogramData {
    std::atomic<uint64_t> buckets[kNumBuckets + 1]{}; // +1 for +Inf
    std::atomic<uint64_t> count{};
    std::atomic<int64_t>  sum_ns{};  // sum in nanoseconds for precision
};

// ── Entry types ───────────────────────────────────────────────────────────────
struct CounterEntry {
    std::string            name;
    std::string            help;
    std::atomic<uint64_t>  value{};
};

struct GaugeEntry {
    std::string           name;
    std::string           help;
    std::atomic<int64_t>  value{};
};

struct HistogramEntry {
    std::string   name;
    std::string   help;
    HistogramData data;
};

// ── MetricsRegistry ──────────────────────────────────────────────────────────
// Central registry for Prometheus-style metrics.
// Hot-path operations (increment_counter, set_gauge, observe_histogram) are
// lock-free using std::atomic.  Only serialize() acquires a mutex.
class MetricsRegistry {
public:
    MetricsRegistry();

    // ── Counter operations (monotonic) ────────────────────────────────────────
    void     increment_counter(std::string_view name, uint64_t delta = 1);
    uint64_t counter_value(std::string_view name) const;

    // ── Gauge operations (can increase/decrease) ──────────────────────────────
    void    set_gauge(std::string_view name, int64_t value);
    void    increment_gauge(std::string_view name, int64_t delta = 1);
    int64_t gauge_value(std::string_view name) const;

    // ── Histogram operations ──────────────────────────────────────────────────
    void observe_histogram(std::string_view name, double seconds);

    // ── Serialization ─────────────────────────────────────────────────────────
    /// Produce Prometheus exposition text with # HELP, # TYPE lines and
    /// node_role labels on every metric line.
    std::string serialize() const;

    // ── Label for node role ───────────────────────────────────────────────────
    void set_node_role(std::string_view role);

    // ── Direct histogram access (for testing) ─────────────────────────────────
    const HistogramData* histogram_data(std::string_view name) const;

private:
    // Pre-registered metrics (fixed at construction, no dynamic allocation).
    // unique_ptr because atomic members are non-movable.
    std::vector<std::unique_ptr<CounterEntry>>   counters_;
    std::vector<std::unique_ptr<GaugeEntry>>     gauges_;
    std::vector<std::unique_ptr<HistogramEntry>> histograms_;
    std::string                                  node_role_{"standalone"};
    mutable std::mutex                           serialize_mtx_;

    // Lookup helpers — linear scan over small fixed-size vectors
    CounterEntry*         find_counter(std::string_view name);
    const CounterEntry*   find_counter(std::string_view name) const;
    GaugeEntry*           find_gauge(std::string_view name);
    const GaugeEntry*     find_gauge(std::string_view name) const;
    HistogramEntry*       find_histogram(std::string_view name);
    const HistogramEntry* find_histogram(std::string_view name) const;
};

} // namespace ob
