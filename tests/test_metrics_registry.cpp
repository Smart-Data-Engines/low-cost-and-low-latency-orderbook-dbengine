// Tests for MetricsRegistry: property-based tests (Properties 1, 2, 3, 9) and unit tests.
// Feature: observability

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <cmath>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

#include "orderbook/metrics.hpp"

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Parse a numeric value from a Prometheus metric line like:
///   ob_total_inserts{node_role="standalone"} 42
static double parse_metric_value(const std::string& output,
                                 const std::string& metric_prefix) {
    std::istringstream stream(output);
    std::string line;
    while (std::getline(stream, line)) {
        if (line.empty() || line[0] == '#') continue;
        if (line.rfind(metric_prefix, 0) == 0) {
            // Find last space — value follows it
            auto pos = line.rfind(' ');
            if (pos != std::string::npos) {
                return std::stod(line.substr(pos + 1));
            }
        }
    }
    return -1.0; // sentinel: not found
}

// ═════════════════════════════════════════════════════════════════════════════
// Property 1: Serialize round-trip (Prometheus exposition text)
// Feature: observability, Property 1: Serializacja metryk — round-trip Prometheus
// Validates: Requirements 1.3, 1.4, 1.8, 2.3, 7.1, 7.2, 7.4, 7.5
// ═════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(MetricsRegistryProperty, prop_serialize_round_trip, ()) {
    ob::MetricsRegistry registry;

    // Generate random counter increments
    const auto counter_delta = *rc::gen::inRange<uint64_t>(0, 100000);
    registry.increment_counter("ob_total_inserts", counter_delta);

    const auto query_delta = *rc::gen::inRange<uint64_t>(0, 100000);
    registry.increment_counter("ob_total_queries", query_delta);

    // Generate random gauge values
    const auto gauge_val = *rc::gen::inRange<int64_t>(-10000, 10000);
    registry.set_gauge("ob_active_sessions", gauge_val);

    // Generate random histogram observations
    const auto num_obs = *rc::gen::inRange<int>(0, 50);
    for (int i = 0; i < num_obs; ++i) {
        double val = *rc::gen::inRange<int>(0, 1000000) / 1e6;
        registry.observe_histogram("ob_insert_latency_seconds", val);
    }

    std::string output = registry.serialize();

    // 1) Every pre-registered metric must have # HELP and # TYPE lines
    std::vector<std::string> expected_counters = {
        "ob_total_inserts", "ob_total_queries", "ob_total_flushes",
        "ob_wal_records_written", "ob_repl_records_replayed"
    };
    for (const auto& name : expected_counters) {
        RC_ASSERT(output.find("# HELP " + name) != std::string::npos);
        RC_ASSERT(output.find("# TYPE " + name + " counter") != std::string::npos);
    }

    std::vector<std::string> expected_gauges = {
        "ob_active_sessions", "ob_pending_rows", "ob_wal_file_index",
        "ob_segment_count", "ob_symbol_count", "ob_current_epoch"
    };
    for (const auto& name : expected_gauges) {
        RC_ASSERT(output.find("# HELP " + name) != std::string::npos);
        RC_ASSERT(output.find("# TYPE " + name + " gauge") != std::string::npos);
    }

    std::vector<std::string> expected_histograms = {
        "ob_insert_latency_seconds", "ob_flush_latency_seconds",
        "ob_query_latency_seconds"
    };
    for (const auto& name : expected_histograms) {
        RC_ASSERT(output.find("# HELP " + name) != std::string::npos);
        RC_ASSERT(output.find("# TYPE " + name + " histogram") != std::string::npos);
    }

    // 2) Metric names match ob_[a-z_]+ pattern
    std::regex name_pattern(R"(^(ob_[a-z_]+))");
    std::istringstream stream(output);
    std::string line;
    while (std::getline(stream, line)) {
        if (line.empty() || line[0] == '#') continue;
        std::smatch match;
        RC_ASSERT(std::regex_search(line, match, name_pattern));
    }

    // 3) Counter values match what we set
    auto inserts_val = parse_metric_value(output, "ob_total_inserts{");
    RC_ASSERT(static_cast<uint64_t>(inserts_val) == counter_delta);

    auto queries_val = parse_metric_value(output, "ob_total_queries{");
    RC_ASSERT(static_cast<uint64_t>(queries_val) == query_delta);

    // 4) Gauge value matches
    auto sessions_val = parse_metric_value(output, "ob_active_sessions{");
    RC_ASSERT(static_cast<int64_t>(sessions_val) == gauge_val);
}


// ═════════════════════════════════════════════════════════════════════════════
// Property 2: Histogram — structural invariants
// Feature: observability, Property 2: Histogram — structural invariants
// Validates: Requirements 1.5, 2.4, 7.3
// ═════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(MetricsRegistryProperty, prop_histogram_invariants, ()) {
    ob::MetricsRegistry registry;

    // Generate random observation count and values
    const auto num_obs = *rc::gen::inRange<int>(1, 200);
    double expected_sum = 0.0;

    for (int i = 0; i < num_obs; ++i) {
        // Generate non-negative doubles via integer division
        double val = *rc::gen::inRange<int>(0, 2000000) / 1e6;
        registry.observe_histogram("ob_insert_latency_seconds", val);
        expected_sum += val;
    }

    const ob::HistogramData* data = registry.histogram_data("ob_insert_latency_seconds");
    RC_ASSERT(data != nullptr);

    // 1) Bucket monotonicity: bucket[i] <= bucket[i+1]
    for (size_t i = 0; i < ob::kNumBuckets; ++i) {
        uint64_t curr = data->buckets[i].load(std::memory_order_relaxed);
        uint64_t next = data->buckets[i + 1].load(std::memory_order_relaxed);
        RC_ASSERT(curr <= next);
    }

    // 2) +Inf bucket == count
    uint64_t inf_bucket = data->buckets[ob::kNumBuckets].load(std::memory_order_relaxed);
    uint64_t count = data->count.load(std::memory_order_relaxed);
    RC_ASSERT(inf_bucket == count);

    // 3) count == number of observations
    RC_ASSERT(count == static_cast<uint64_t>(num_obs));

    // 4) sum correctness (with floating-point tolerance)
    double actual_sum = static_cast<double>(data->sum_ns.load(std::memory_order_relaxed)) / 1e9;
    // Tolerance: each observation can have up to ~0.5ns rounding error from double→int64
    double tolerance = num_obs * 1e-9 + expected_sum * 1e-6;
    RC_ASSERT(std::fabs(actual_sum - expected_sum) < tolerance + 1e-12);
}

// ═════════════════════════════════════════════════════════════════════════════
// Property 3: Label node_role present on every metric
// Feature: observability, Property 3: Label node_role present on every metric
// Validates: Requirements 1.6
// ═════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(MetricsRegistryProperty, prop_node_role_label, ()) {
    ob::MetricsRegistry registry;

    // Pick a random role from the valid set
    const auto role_idx = *rc::gen::inRange<int>(0, 3);
    const char* roles[] = {"standalone", "primary", "replica"};
    std::string chosen_role = roles[role_idx];
    registry.set_node_role(chosen_role);

    // Add some random data to make output non-trivial
    registry.increment_counter("ob_total_inserts", *rc::gen::inRange<uint64_t>(0, 1000));
    registry.set_gauge("ob_active_sessions", *rc::gen::inRange<int64_t>(0, 100));
    double obs_val = *rc::gen::inRange<int>(0, 1000000) / 1e6;
    registry.observe_histogram("ob_insert_latency_seconds", obs_val);

    std::string output = registry.serialize();
    std::string expected_label = "node_role=\"" + chosen_role + "\"";

    // Every non-comment, non-empty line must contain the node_role label
    std::istringstream stream(output);
    std::string line;
    while (std::getline(stream, line)) {
        if (line.empty() || line[0] == '#') continue;
        RC_ASSERT(line.find(expected_label) != std::string::npos);
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Property 9: Floating-point precision in serialization
// Feature: observability, Property 9: Floating-point precision in serialization
// Validates: Requirements 2.6
// ═════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(MetricsRegistryProperty, prop_float_precision, ()) {
    ob::MetricsRegistry registry;

    // Generate a random double in [1e-9, 1e6] via log-uniform distribution
    // Use integer exponent + mantissa for better coverage
    const auto exp_val = *rc::gen::inRange<int>(-9, 7); // 10^-9 to 10^6
    const auto mantissa = *rc::gen::inRange<int>(100, 1000); // 1.00 to 9.99
    double original = (mantissa / 100.0) * std::pow(10.0, exp_val);

    // Clamp to valid range
    if (original < 1e-9) original = 1e-9;
    if (original > 1e6) original = 1e6;

    // Observe the value in a histogram — it goes through sum_ns (int64 nanoseconds)
    registry.observe_histogram("ob_insert_latency_seconds", original);

    std::string output = registry.serialize();

    // Parse the _sum line back
    double parsed = parse_metric_value(output, "ob_insert_latency_seconds_sum{");
    RC_ASSERT(parsed >= 0.0);

    // Verify precision: the implementation stores sum in nanoseconds (int64),
    // so there is a quantization floor of 1 nanosecond = 1e-9 seconds.
    // For values >> 1e-9, relative error < 1e-6 holds.
    // For values near 1e-9, the absolute error is bounded by 1 nanosecond.
    // Combined check: |parsed - original| < max(|original| * 1e-6, 1e-9)
    double abs_error = std::fabs(parsed - original);
    double allowed = std::max(std::fabs(original) * 1e-6, 1e-9);
    RC_ASSERT(abs_error < allowed + 1e-15); // +epsilon for floating-point comparison
}

// ═════════════════════════════════════════════════════════════════════════════
// Unit Tests: MetricsRegistry
// Feature: observability
// ═════════════════════════════════════════════════════════════════════════════

TEST(MetricsRegistryUnit, CounterIncrementAndRead) {
    ob::MetricsRegistry registry;

    EXPECT_EQ(registry.counter_value("ob_total_inserts"), 0u);

    registry.increment_counter("ob_total_inserts", 5);
    EXPECT_EQ(registry.counter_value("ob_total_inserts"), 5u);

    registry.increment_counter("ob_total_inserts", 3);
    EXPECT_EQ(registry.counter_value("ob_total_inserts"), 8u);

    // Default delta = 1
    registry.increment_counter("ob_total_inserts");
    EXPECT_EQ(registry.counter_value("ob_total_inserts"), 9u);

    // Unknown counter returns 0
    EXPECT_EQ(registry.counter_value("nonexistent"), 0u);
}

TEST(MetricsRegistryUnit, GaugeSetIncrementAndRead) {
    ob::MetricsRegistry registry;

    EXPECT_EQ(registry.gauge_value("ob_active_sessions"), 0);

    registry.set_gauge("ob_active_sessions", 42);
    EXPECT_EQ(registry.gauge_value("ob_active_sessions"), 42);

    registry.increment_gauge("ob_active_sessions", 3);
    EXPECT_EQ(registry.gauge_value("ob_active_sessions"), 45);

    // Negative increment (decrement)
    registry.increment_gauge("ob_active_sessions", -10);
    EXPECT_EQ(registry.gauge_value("ob_active_sessions"), 35);

    // Set overwrites
    registry.set_gauge("ob_active_sessions", -5);
    EXPECT_EQ(registry.gauge_value("ob_active_sessions"), -5);

    // Unknown gauge returns 0
    EXPECT_EQ(registry.gauge_value("nonexistent"), 0);
}

TEST(MetricsRegistryUnit, HistogramObserveAndBucketDistribution) {
    ob::MetricsRegistry registry;

    // Observe values that fall into different buckets
    // kLatencyBuckets: 0.00001, 0.00005, 0.0001, 0.0005, 0.001,
    //                  0.005,   0.01,    0.05,   0.1,    0.5, 1.0

    registry.observe_histogram("ob_insert_latency_seconds", 0.000005);  // < 0.00001
    registry.observe_histogram("ob_insert_latency_seconds", 0.0003);    // < 0.0005
    registry.observe_histogram("ob_insert_latency_seconds", 0.5);       // <= 0.5
    registry.observe_histogram("ob_insert_latency_seconds", 2.0);       // > 1.0, only +Inf

    const ob::HistogramData* data = registry.histogram_data("ob_insert_latency_seconds");
    ASSERT_NE(data, nullptr);

    // count should be 4
    EXPECT_EQ(data->count.load(), 4u);

    // +Inf bucket should equal count
    EXPECT_EQ(data->buckets[ob::kNumBuckets].load(), 4u);

    // First bucket (le=0.00001): 0.000005 fits → 1
    EXPECT_EQ(data->buckets[0].load(), 1u);

    // Bucket le=0.0005 (index 3): 0.000005 + 0.0003 fit → 2
    EXPECT_EQ(data->buckets[3].load(), 2u);

    // Bucket le=0.5 (index 9): 0.000005 + 0.0003 + 0.5 fit → 3
    EXPECT_EQ(data->buckets[9].load(), 3u);

    // Bucket le=1.0 (index 10): same 3 (2.0 doesn't fit)
    EXPECT_EQ(data->buckets[10].load(), 3u);

    // Verify monotonicity
    for (size_t i = 0; i < ob::kNumBuckets; ++i) {
        EXPECT_LE(data->buckets[i].load(), data->buckets[i + 1].load());
    }
}

TEST(MetricsRegistryUnit, SerializeFormat) {
    ob::MetricsRegistry registry;

    registry.increment_counter("ob_total_inserts", 42);
    registry.set_gauge("ob_pending_rows", 100);
    registry.observe_histogram("ob_query_latency_seconds", 0.001);
    registry.set_node_role("primary");

    std::string output = registry.serialize();

    // Check counter format
    EXPECT_NE(output.find("# HELP ob_total_inserts"), std::string::npos);
    EXPECT_NE(output.find("# TYPE ob_total_inserts counter"), std::string::npos);
    EXPECT_NE(output.find("ob_total_inserts{node_role=\"primary\"} 42"), std::string::npos);

    // Check gauge format
    EXPECT_NE(output.find("# HELP ob_pending_rows"), std::string::npos);
    EXPECT_NE(output.find("# TYPE ob_pending_rows gauge"), std::string::npos);
    EXPECT_NE(output.find("ob_pending_rows{node_role=\"primary\"} 100"), std::string::npos);

    // Check histogram format
    EXPECT_NE(output.find("# HELP ob_query_latency_seconds"), std::string::npos);
    EXPECT_NE(output.find("# TYPE ob_query_latency_seconds histogram"), std::string::npos);
    EXPECT_NE(output.find("ob_query_latency_seconds_bucket{node_role=\"primary\",le=\"+Inf\"} 1"),
              std::string::npos);
    EXPECT_NE(output.find("ob_query_latency_seconds_count{node_role=\"primary\"} 1"),
              std::string::npos);

    // _sum line should exist and contain a numeric value
    EXPECT_NE(output.find("ob_query_latency_seconds_sum{node_role=\"primary\"}"),
              std::string::npos);
}
