// ob_integration_test.cpp — C++ integration test binary for orderbook-dbengine.
// Invoked by Python test_cpp_client.py via subprocess.
//
// Usage: ob_integration_test --host <host> --port <port> --test <test_name>
//   test_name: "ping", "insert_query", "minsert"
//
// Exit code 0 = success, 1 = failure.
// Prints JSON result to stdout: {"test":"...","status":"pass"/"fail","message":"..."}

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

#include "orderbook/client.hpp"

// ── Helpers ──────────────────────────────────────────────────────────────────

static void print_result(const std::string& test, const std::string& status,
                         const std::string& message) {
    // Minimal JSON — no external deps.
    std::cout << "{\"test\":\"" << test
              << "\",\"status\":\"" << status
              << "\",\"message\":\"" << message
              << "\"}" << std::endl;
}

// ── Test: ping ───────────────────────────────────────────────────────────────

static int run_ping(const std::string& host, uint16_t port) {
    ob::ClientConfig cfg;
    cfg.host = host;
    cfg.port = port;

    ob::OrderbookClient client(cfg);
    auto conn = client.connect();
    if (!conn) {
        print_result("ping", "fail", "connect failed: " + conn.error_message());
        return 1;
    }

    auto res = client.ping();
    if (!res || !res.value()) {
        print_result("ping", "fail", "ping failed");
        return 1;
    }

    print_result("ping", "pass", "ping ok");
    return 0;
}

// ── Test: insert_query ───────────────────────────────────────────────────────

static int run_insert_query(const std::string& host, uint16_t port) {
    ob::ClientConfig cfg;
    cfg.host = host;
    cfg.port = port;

    ob::OrderbookClient client(cfg);
    auto conn = client.connect();
    if (!conn) {
        print_result("insert_query", "fail",
                     "connect failed: " + conn.error_message());
        return 1;
    }

    // Insert one level
    auto ins = client.insert("CPP-IQ", "TEST-EX", ob::Side::BID, 10000, 50, 1);
    if (!ins) {
        print_result("insert_query", "fail",
                     "insert failed: " + ins.error_message());
        return 1;
    }

    auto fl = client.flush();
    if (!fl) {
        print_result("insert_query", "fail",
                     "flush failed: " + fl.error_message());
        return 1;
    }

    auto qr = client.query("SELECT * FROM 'CPP-IQ'.'TEST-EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999");
    if (!qr) {
        print_result("insert_query", "fail",
                     "query failed: " + qr.error_message());
        return 1;
    }

    size_t row_count = qr.value().rows.size();
    if (row_count < 1) {
        print_result("insert_query", "fail",
                     "expected >=1 rows, got " + std::to_string(row_count));
        return 1;
    }

    print_result("insert_query", "pass",
                 "rows=" + std::to_string(row_count));
    return 0;
}

// ── Test: minsert ────────────────────────────────────────────────────────────

static int run_minsert(const std::string& host, uint16_t port) {
    ob::ClientConfig cfg;
    cfg.host = host;
    cfg.port = port;

    ob::OrderbookClient client(cfg);
    auto conn = client.connect();
    if (!conn) {
        print_result("minsert", "fail",
                     "connect failed: " + conn.error_message());
        return 1;
    }

    // Build 100 levels
    constexpr size_t N = 100;
    ob::Level levels[N];
    for (size_t i = 0; i < N; ++i) {
        levels[i].price = static_cast<int64_t>(10000 + i);
        levels[i].qty   = 10;
        levels[i].count = 1;
    }

    auto mi = client.minsert("CPP-MI", "TEST-EX", ob::Side::BID, levels, N);
    if (!mi) {
        print_result("minsert", "fail",
                     "minsert failed: " + mi.error_message());
        return 1;
    }

    auto fl = client.flush();
    if (!fl) {
        print_result("minsert", "fail",
                     "flush failed: " + fl.error_message());
        return 1;
    }

    auto qr = client.query("SELECT * FROM 'CPP-MI'.'TEST-EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999");
    if (!qr) {
        print_result("minsert", "fail",
                     "query failed: " + qr.error_message());
        return 1;
    }

    size_t row_count = qr.value().rows.size();
    if (row_count < N) {
        print_result("minsert", "fail",
                     "expected >=" + std::to_string(N) + " rows, got " +
                     std::to_string(row_count));
        return 1;
    }

    print_result("minsert", "pass",
                 "rows=" + std::to_string(row_count));
    return 0;
}

// ── CLI argument parsing ─────────────────────────────────────────────────────

static void usage(const char* prog) {
    std::cerr << "Usage: " << prog
              << " --host <host> --port <port> --test <ping|insert_query|minsert>\n";
}

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    uint16_t port = 9090;
    std::string test_name;

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--host") == 0 && i + 1 < argc) {
            host = argv[++i];
        } else if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            port = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else if (std::strcmp(argv[i], "--test") == 0 && i + 1 < argc) {
            test_name = argv[++i];
        } else {
            usage(argv[0]);
            return 1;
        }
    }

    if (test_name.empty()) {
        usage(argv[0]);
        return 1;
    }

    if (test_name == "ping")         return run_ping(host, port);
    if (test_name == "insert_query") return run_insert_query(host, port);
    if (test_name == "minsert")      return run_minsert(host, port);

    std::cerr << "Unknown test: " << test_name << "\n";
    usage(argv[0]);
    return 1;
}
