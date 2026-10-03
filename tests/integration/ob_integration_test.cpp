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
#include <fstream>
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

/// Credentials from the environment, so the Python harness can point this binary at an
/// authenticated node without a new flag on every subcommand.
///
/// Environment rather than argv on purpose: an argument is visible in `/proc/<pid>/cmdline` to
/// every process on the machine, which is the same reason the server has no flag carrying a secret.
static void apply_auth_from_env(ob::ClientConfig& cfg) {
    if (const char* id = std::getenv("OB_AUTH_IDENTITY")) cfg.auth_identity = id;
    if (const char* sec = std::getenv("OB_AUTH_SECRET"))  cfg.auth_secret = sec;

    // TLS the same way, so every subcommand covers it without four more flags. A CA path is not a
    // secret, but keeping one mechanism means the harness has one thing to set.
    if (std::getenv("OB_TLS") != nullptr) cfg.tls = true;
    if (const char* ca = std::getenv("OB_TLS_CA_FILE")) cfg.tls_ca_file = ca;
    if (const char* v = std::getenv("OB_TLS_VERIFY")) {
        cfg.tls_verify = !(std::strcmp(v, "0") == 0);
    }
}

// ── Test: ping ───────────────────────────────────────────────────────────────

static int run_ping(const std::string& host, uint16_t port) {
    ob::ClientConfig cfg;
    cfg.host = host;
    cfg.port = port;
    apply_auth_from_env(cfg);

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
    apply_auth_from_env(cfg);

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
    apply_auth_from_env(cfg);

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
    apply_auth_from_env(cfg);

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

// ── Test: query_buckets (#44) ────────────────────────────────────────────────
// A time-bucket answer over a real socket: the header's scales, the values, NULL for a bucket with
// nothing to weigh by, and the row API refusing a shape it cannot represent.

static int run_query_buckets(const std::string& host, uint16_t port) {
    ob::ClientConfig cfg;
    cfg.host = host;
    cfg.port = port;
    apply_auth_from_env(cfg);

    ob::OrderbookClient client(cfg);
    auto conn = client.connect();
    if (!conn) {
        print_result("query_buckets", "fail", "connect failed: " + conn.error_message());
        return 1;
    }
    // Two rows in the first second of a whole minute, one at quantity zero in the next second.
    constexpr uint64_t kMinute = 29'834'000ULL * 60ULL * 1'000'000'000ULL;
    const bool written =
        client.insert("CPP-BKT", "TEST-EX", ob::Side::BID, 100, 1, 1, kMinute + 1) &&
        client.insert("CPP-BKT", "TEST-EX", ob::Side::BID, 200, 3, 1, kMinute + 2) &&
        client.insert("CPP-BKT", "TEST-EX", ob::Side::BID, 300, 0, 1, kMinute + 1'000'000'001ULL) &&
        client.flush();
    if (!written) {
        print_result("query_buckets", "fail", "insert or flush failed");
        return 1;
    }
    const std::string sql =
        "SELECT COUNT(*), VWAP(price) FROM 'CPP-BKT'.'TEST-EX' GROUP BY TIME_BUCKET(1s)";
    auto buckets = client.query_buckets(sql);
    if (!buckets) {
        print_result("query_buckets", "fail", "query_buckets failed: " + buckets.error_message());
        return 1;
    }
    const auto& b = buckets.value();
    if (b.size() != 2 || b[0].start_ns != kMinute || b[1].start_ns != kMinute + 1'000'000'000ULL) {
        print_result("query_buckets", "fail", "expected buckets at the minute and a second after it");
        return 1;
    }
    // (100x1 + 200x3) / 4 = 175, scaled by 10^6; the second bucket weighs by nothing.
    if (b[0].values.size() != 2 || b[0].values[0].name != "COUNT(*)" || b[0].values[0].value != 2 ||
        b[0].values[1].name != "VWAP(price)" || b[0].values[1].scale != 1'000'000 ||
        b[0].values[1].value != 175'000'000 || b[0].values[1].empty) {
        print_result("query_buckets", "fail", "first bucket wrong: VWAP=" +
                     std::to_string(b[0].values.size() > 1 ? b[0].values[1].value : -1));
        return 1;
    }
    if (b[1].values.size() != 2 || b[1].values[0].value != 1 || !b[1].values[1].empty) {
        print_result("query_buckets", "fail", "second bucket's VWAP is not NULL");
        return 1;
    }
    auto as_rows = client.query(sql);
    if (as_rows || as_rows.error_message().find("query_buckets") == std::string::npos) {
        print_result("query_buckets", "fail",
                     "query() did not refuse a time-bucket answer by name: " +
                     (as_rows ? std::string("it returned rows") : as_rows.error_message()));
        return 1;
    }

    // A series of the book (#44 step 2): bid 100 and ask 104 at the minute, the bid 102 at 1.5 s.
    // Every bucket of the range is answered - [the minute, 1.5 s] - the mid at MID_PRICE's scale,
    // the bid's close at its own, and the spread's TWAP over the second bucket's window: 4 for
    // 0.5 s, 2 for its last nanosecond, truncated.
    const bool book =
        client.insert("CPP-SER", "TEST-EX", ob::Side::BID, 100, 1, 1, kMinute) &&
        client.insert("CPP-SER", "TEST-EX", ob::Side::ASK, 104, 1, 1, kMinute) &&
        client.insert("CPP-SER", "TEST-EX", ob::Side::BID, 102, 1, 1, kMinute + 1'500'000'000ULL) &&
        client.flush();
    if (!book) {
        print_result("query_buckets", "fail", "the series' insert or flush failed");
        return 1;
    }
    auto series = client.query_buckets(
        "SELECT OPEN(mid), CLOSE(bid), TWAP(spread) FROM 'CPP-SER'.'TEST-EX' GROUP BY TIME_BUCKET(1s)");
    if (!series) {
        print_result("query_buckets", "fail", "the series failed: " + series.error_message());
        return 1;
    }
    const auto& s = series.value();
    const auto is = [](const auto& v, const char* name, int64_t scale, int64_t value) {
        return v.name == name && v.scale == scale && v.value == value && !v.empty;
    };
    if (s.size() != 2 || s[0].values.size() != 3 || s[1].values.size() != 3 ||
        !is(s[0].values[0], "OPEN(mid)", 1'000'000, 102'000'000) ||
        !is(s[0].values[1], "CLOSE(bid)", 1, 100) ||
        !is(s[0].values[2], "TWAP(spread)", 1'000'000, 4'000'000) ||
        !is(s[1].values[0], "OPEN(mid)", 1'000'000, 102'000'000) ||
        !is(s[1].values[1], "CLOSE(bid)", 1, 102) ||
        !is(s[1].values[2], "TWAP(spread)", 1'000'000, 3'999'999)) {
        print_result("query_buckets", "fail", "the series' buckets are wrong: " +
                     std::to_string(s.size()) + " bucket(s)");
        return 1;
    }
    print_result("query_buckets", "pass", "2 buckets, VWAP=175000000 and NULL; a series of 2 buckets");
    return 0;
}

// ── shard_pool (#175): a pool that finds its shards in etcd ──────────────────

/// One `ask` row of each symbol, through a pool built from coordinator endpoints alone: the shard
/// map it routes by is the one the shards wrote to etcd.
static int run_shard_pool(const std::string& coordinator, const std::string& symbols) {
    ob::PoolConfig pc;
    pc.coordinator_endpoints = {coordinator};
    pc.health_check_interval_sec = 0.5;
    ob::OrderbookPool pool(pc);
    size_t written = 0;
    size_t start = 0;
    while (start <= symbols.size()) {
        const size_t comma = symbols.find(',', start);
        const std::string sym = symbols.substr(start, comma == std::string::npos ? std::string::npos
                                                                                 : comma - start);
        if (!sym.empty()) {
            auto res = pool.insert(sym, "EX", ob::Side::ASK, 300, 3, 1, 1'700'000'000'000'000'002ULL);
            if (!res) {
                print_result("shard_pool", "fail", sym + ": " + res.error_message());
                pool.close();
                return 1;
            }
            ++written;
        }
        if (comma == std::string::npos) break;
        start = comma + 1;
    }
    pool.close();
    print_result("shard_pool", "pass", std::to_string(written) + " written");
    return 0;
}

// ── shard_writer (#196): a pool writing one symbol while it moves ────────────

/// Writes of one symbol through a pool that finds its shards in etcd, one `bid` level each at its
/// own event time, until `until` exists; the event time of every write acknowledged goes to `out`,
/// one a line. A write the pool could not place is a failure: the migration a test runs beside it is
/// to cost writers nothing but latency.
static int run_shard_writer(const std::string& coordinator, const std::string& symbol,
                            const std::string& until, const std::string& out_path) {
    ob::PoolConfig pc;
    pc.coordinator_endpoints = {coordinator};
    pc.health_check_interval_sec = 0.5;
    ob::OrderbookPool pool(pc);
    std::ofstream out(out_path);
    constexpr uint64_t kFirst = 1'760'000'200'000'000'000ULL;
    uint64_t written = 0;
    uint64_t failed = 0;
    std::string first_failure;
    for (uint64_t i = 0; !std::ifstream(until).good(); ++i) {
        const uint64_t ts = kFirst + i * 1000;
        auto res = pool.insert(symbol, "EX", ob::Side::BID, static_cast<int64_t>(30'000 + i), 1, 1, ts);
        if (res) {
            out << ts << "\n";
            ++written;
        } else {
            if (failed++ == 0) first_failure = std::to_string(ts) + ": " + res.error_message();
        }
    }
    out.flush();
    pool.close();
    if (failed > 0) {
        print_result("shard_writer", "fail", std::to_string(failed) + " failed, the first " + first_failure);
        return 1;
    }
    print_result("shard_writer", "pass", std::to_string(written) + " written");
    return 0;
}

// ── CLI argument parsing ─────────────────────────────────────────────────────

static void usage(const char* prog) {
    std::cerr << "Usage: " << prog
              << " --host <host> --port <port>"
              << " --test <ping|insert_query|minsert|query_agg>\n"
              << "       " << prog << " --test shard_pool --coordinator <URL> --symbols <A,B,...>\n"
              << "       " << prog << " --test shard_writer --coordinator <URL> --symbols <A>"
              << " --until <FILE> --out <FILE>\n";
}

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    uint16_t port = 9090;
    std::string test_name;
    std::string coordinator;
    std::string symbols;
    std::string until;
    std::string out;

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--host") == 0 && i + 1 < argc) {
            host = argv[++i];
        } else if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            port = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else if (std::strcmp(argv[i], "--test") == 0 && i + 1 < argc) {
            test_name = argv[++i];
        } else if (std::strcmp(argv[i], "--coordinator") == 0 && i + 1 < argc) {
            coordinator = argv[++i];
        } else if (std::strcmp(argv[i], "--symbols") == 0 && i + 1 < argc) {
            symbols = argv[++i];
        } else if (std::strcmp(argv[i], "--until") == 0 && i + 1 < argc) {
            until = argv[++i];
        } else if (std::strcmp(argv[i], "--out") == 0 && i + 1 < argc) {
            out = argv[++i];
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
    if (test_name == "query_buckets") return run_query_buckets(host, port);
    if (test_name == "shard_pool")   return run_shard_pool(coordinator, symbols);
    if (test_name == "shard_writer") return run_shard_writer(coordinator, symbols, until, out);

    std::cerr << "Unknown test: " << test_name << "\n";
    usage(argv[0]);
    return 1;
}
