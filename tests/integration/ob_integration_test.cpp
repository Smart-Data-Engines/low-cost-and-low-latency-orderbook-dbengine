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

// ── Test: query_agg ──────────────────────────────────────────────────────────
// Exercises the aggregate response over a real socket: values, scale factors, and
// the row API refusing a shape it cannot represent. The unit tests for this parser
// feed it hand-written strings; only this one proves the server and the client
// agree on the bytes.

static int run_query_agg(const std::string& host, uint16_t port) {
    ob::ClientConfig cfg;
    cfg.host = host;
    cfg.port = port;

    ob::OrderbookClient client(cfg);
    auto conn = client.connect();
    if (!conn) {
        print_result("query_agg", "fail", "connect failed: " + conn.error_message());
        return 1;
    }

    // Aggregates read the live book, so no flush is needed — and that is part of
    // what this checks.
    auto bid = client.insert("CPP-AGG", "TEST-EX", ob::Side::BID, 100000, 50, 1);
    auto ask = client.insert("CPP-AGG", "TEST-EX", ob::Side::ASK, 101000, 30, 1);
    if (!bid || !ask) {
        print_result("query_agg", "fail", "insert failed");
        return 1;
    }

    const std::string sql =
        "SELECT SPREAD(*), MID_PRICE(*) FROM 'CPP-AGG'.'TEST-EX'";

    auto agg = client.query_agg(sql);
    if (!agg) {
        print_result("query_agg", "fail",
                     "query_agg failed: " + agg.error_message());
        return 1;
    }

    const auto& entries = agg.value();
    if (entries.size() != 2) {
        print_result("query_agg", "fail",
                     "expected 2 aggregates, got " + std::to_string(entries.size()));
        return 1;
    }

    // spread = 101000 - 100000, raw units.
    if (entries[0].name != "SPREAD(*)" || entries[0].value != 1000 ||
        entries[0].scale != 1 || entries[0].empty) {
        print_result("query_agg", "fail",
                     "spread wrong: name=" + entries[0].name +
                     " value=" + std::to_string(entries[0].value) +
                     " scale=" + std::to_string(entries[0].scale));
        return 1;
    }

    // mid price = 100500, scaled by 10^6.
    if (entries[1].name != "MID_PRICE(*)" || entries[1].scale != 1000000 ||
        entries[1].value != 100500LL * 1000000LL) {
        print_result("query_agg", "fail",
                     "mid price wrong: value=" + std::to_string(entries[1].value) +
                     " scale=" + std::to_string(entries[1].scale));
        return 1;
    }
    if (entries[1].real() < 100499.9 || entries[1].real() > 100500.1) {
        print_result("query_agg", "fail", "real() did not apply the scale");
        return 1;
    }

    // The row API must refuse this response by name rather than misparse it.
    auto as_rows = client.query(sql);
    if (as_rows) {
        print_result("query_agg", "fail",
                     "query() accepted an aggregate response and returned rows");
        return 1;
    }
    if (as_rows.error_message().find("query_agg") == std::string::npos) {
        print_result("query_agg", "fail",
                     "query() refused it without naming query_agg: " +
                     as_rows.error_message());
        return 1;
    }

    print_result("query_agg", "pass",
                 "spread=1000 mid=100500 scale=1000000");
    return 0;
}

// ── CLI argument parsing ─────────────────────────────────────────────────────

static void usage(const char* prog) {
    std::cerr << "Usage: " << prog
              << " --host <host> --port <port>"
              << " --test <ping|insert_query|minsert|query_agg>\n";
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
    if (test_name == "query_agg")    return run_query_agg(host, port);

    std::cerr << "Unknown test: " << test_name << "\n";
    usage(argv[0]);
    return 1;
}
