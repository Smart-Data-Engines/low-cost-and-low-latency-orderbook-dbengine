// tools/ob_cli.cpp — Interactive CLI for the orderbook-dbengine.
//
// Usage:
//   ./ob_cli [data_dir]
//
// Commands:
//   insert <symbol> <exchange> <bid|ask> <price> <qty> [count]
//   bulk   <symbol> <exchange> <bid|ask> <n_levels> <base_price> <base_qty>
//   query  <SQL>
//   flush          — force-flush pending data to columnar store
//   status
//   help
//   quit

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <exception>
#include <iostream>
#include <sstream>
#include <optional>
#include <cctype>
#include <string>
#include <vector>

#include "orderbook/data_model.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/query_engine.hpp"
#include "orderbook/types.hpp"

namespace {

uint64_t now_ns() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

uint64_t g_seq = 0;
uint64_t g_inserts = 0;
uint64_t g_queries = 0;

void print_help() {
    std::cout << R"(
orderbook-dbengine interactive CLI
───────────────────────────────────
Commands:

  insert <symbol> <exchange> <bid|ask> <price> <qty> [count]
      Insert a single price level.
      Example: insert BTC-USD BINANCE bid 6500000 1500
      (prices are in smallest sub-units, e.g. cents or satoshis)

  bulk <symbol> <exchange> <bid|ask> <n_levels> <base_price> <base_qty>
      Insert N levels at once (prices step by 100 per level).
      Example: bulk BTC-USD BINANCE bid 10 6500000 100

  query <SQL>
      Run a SQL query. Data must be flushed first (see 'flush').
      Syntax: SELECT <cols> FROM '<symbol>'.'<exchange>' WHERE timestamp BETWEEN <start> AND <end>
      Example: query SELECT price, quantity FROM 'BTC-USD'.'BINANCE' WHERE timestamp BETWEEN 0 AND 9999999999999999999

  flush
      Force-flush all pending data to the columnar store so queries can see it.

  load <csv_file>
      Import rows from a CSV file. Expected columns (with header):
        symbol,exchange,side,price,qty,count,timestamp_ns
      'side' is 'bid' or 'ask'. 'count' and 'timestamp_ns' are optional.
      Example: load /tmp/orderbook_data.csv

  generate <symbol> <exchange> <n_rows>
      Generate N synthetic orderbook rows (bid+ask) for quick testing.
      Example: generate BTC-USD BINANCE 1000

  status
      Show engine stats.

  help
      Show this message.

  quit / exit
      Shut down and exit.
)";
}

/// `bid` or `ask`, in any case, and nothing else (#110).
///
/// Every caller of this used to be
/// `(side_str == "ask" || side_str == "ASK") ? SIDE_ASK : SIDE_BID`, written three times - which
/// makes **every** word that is not exactly `ask` or `ASK` a bid. `Ask`, `asks`, `sell`, `sideways`
/// and a typo all stored the opposite side of the book from the one that was typed, and the
/// confirmation line echoed the typed word back as though it had been understood. The wire refuses
/// that same word; the tool a human types into was the one that guessed.
std::optional<uint8_t> parse_side(const std::string& word) {
    std::string lowered;
    lowered.reserve(word.size());
    for (const char c : word) lowered += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (lowered == "bid") return ob::SIDE_BID;
    if (lowered == "ask") return ob::SIDE_ASK;
    return std::nullopt;
}

/// The canonical spelling, for a confirmation line that must not repeat the caller's word.
const char* side_name(uint8_t side) { return side == ob::SIDE_ASK ? "ask" : "bid"; }

/// Refuse a line carrying a token the command's grammar has no place for (#110, #107's rule).
///
/// `istringstream >>` reads the fields it knows and leaves the rest, so
/// `insert A EX bid 100 5 1 1700000000000000000` stored a row and discarded the timestamp - which is
/// what the wire protocol did until #107, and the embedded API here has no event-time argument at
/// all because it goes through `DeltaUpdate` rather than through a command line.
bool nothing_follows(std::istringstream& args, const char* usage) {
    std::string extra;
    if (args >> extra) {
        std::cerr << "unexpected token '" << extra << "'\n" << "Usage: " << usage << "\n";
        return false;
    }
    return true;
}

void cmd_insert(ob::Engine& engine, std::istringstream& args) {
    std::string symbol, exchange, side_str;
    int64_t price = 0;
    uint64_t qty = 0;
    uint32_t cnt = 1;

    args >> symbol >> exchange >> side_str >> price >> qty;
    if (args.fail()) {
        std::cerr << "Usage: insert <symbol> <exchange> <bid|ask> <price> <qty> [count]\n";
        return;
    }
    args >> cnt; // optional
    static constexpr const char* kUsage =
        "insert <symbol> <exchange> <bid|ask> <price> <qty> [count]";
    if (!nothing_follows(args, kUsage)) return;

    const auto parsed_side = parse_side(side_str);
    if (!parsed_side) {
        std::cerr << "'" << side_str << "' is not a side; it must be bid or ask\n"
                  << "Usage: " << kUsage << "\n";
        return;
    }
    const uint8_t side = *parsed_side;
    ++g_seq;

    ob::DeltaUpdate du{};
    std::strncpy(du.symbol,   symbol.c_str(),   sizeof(du.symbol)   - 1);
    std::strncpy(du.exchange, exchange.c_str(), sizeof(du.exchange) - 1);
    du.sequence_number = g_seq;
    du.timestamp_ns    = now_ns();
    du.side            = side;
    du.n_levels        = 1;

    ob::Level lev{};
    lev.price = price;
    lev.qty   = qty;
    lev.cnt   = cnt;

    ob::ob_status_t rc = engine.apply_delta(du, &lev);
    if (rc == ob::OB_OK) {
        ++g_inserts;
        // `side_name(side)` rather than `side_str`: a confirmation that repeats the caller's word
        // confirms the typing rather than the storing, and that is the line a human reads to check
        // that the tool understood.
        std::cout << "OK  seq=" << g_seq
                  << "  " << side_name(side) << " " << symbol << "@" << exchange
                  << "  price=" << price << " qty=" << qty << "\n";
    } else {
        std::cerr << "ERROR: apply_delta returned " << rc << "\n";
    }
}

void cmd_bulk(ob::Engine& engine, std::istringstream& args) {
    std::string symbol, exchange, side_str;
    uint32_t n_levels = 0;
    int64_t base_price = 0;
    uint64_t base_qty = 0;

    args >> symbol >> exchange >> side_str >> n_levels >> base_price >> base_qty;
    if (args.fail() || n_levels == 0 || n_levels > 1000) {
        std::cerr << "Usage: bulk <symbol> <exchange> <bid|ask> <n_levels 1-1000> <base_price> <base_qty>\n";
        (void)engine;
        return;
    }

    static constexpr const char* kUsage =
        "bulk <symbol> <exchange> <bid|ask> <n_levels 1-1000> <base_price> <base_qty>";
    if (!nothing_follows(args, kUsage)) return;

    const auto parsed_side = parse_side(side_str);
    if (!parsed_side) {
        std::cerr << "'" << side_str << "' is not a side; it must be bid or ask\n"
                  << "Usage: " << kUsage << "\n";
        return;
    }
    const uint8_t side = *parsed_side;
    int64_t step = (side == ob::SIDE_BID) ? -100 : 100;

    std::vector<ob::Level> levels(n_levels);
    for (uint32_t i = 0; i < n_levels; ++i) {
        levels[i].price = base_price + static_cast<int64_t>(i) * step;
        levels[i].qty   = base_qty + i * 10;
        levels[i].cnt   = 1;
    }

    ++g_seq;
    ob::DeltaUpdate du{};
    std::strncpy(du.symbol,   symbol.c_str(),   sizeof(du.symbol)   - 1);
    std::strncpy(du.exchange, exchange.c_str(), sizeof(du.exchange) - 1);
    du.sequence_number = g_seq;
    du.timestamp_ns    = now_ns();
    du.side            = side;
    du.n_levels        = static_cast<uint16_t>(n_levels);

    ob::ob_status_t rc = engine.apply_delta(du, levels.data());
    if (rc == ob::OB_OK) {
        ++g_inserts;
        std::cout << "OK  seq=" << g_seq
                  << "  " << n_levels << " " << side_name(side) << " levels for "
                  << symbol << "@" << exchange
                  << "  base_price=" << base_price << "\n";
    } else {
        std::cerr << "ERROR: apply_delta returned " << rc << "\n";
    }
}

void cmd_load(ob::Engine& engine, std::istringstream& args) {
    std::string path;
    args >> path;
    if (path.empty()) {
        std::cerr << "Usage: load <csv_file>\n";
        return;
    }

    std::ifstream file(path);
    if (!file.is_open()) {
        std::cerr << "ERROR: cannot open " << path << "\n";
        return;
    }

    // Read and skip header line
    std::string header;
    if (!std::getline(file, header)) {
        std::cerr << "ERROR: empty file\n";
        return;
    }

    int loaded = 0;
    int errors = 0;
    std::string csv_line;
    while (std::getline(file, csv_line)) {
        if (csv_line.empty()) continue;

        // Parse CSV: symbol,exchange,side,price,qty[,count[,timestamp_ns]]
        std::istringstream row(csv_line);
        std::string symbol, exchange, side_str, price_s, qty_s, cnt_s, ts_s;

        std::getline(row, symbol, ',');
        std::getline(row, exchange, ',');
        std::getline(row, side_str, ',');
        std::getline(row, price_s, ',');
        std::getline(row, qty_s, ',');
        std::getline(row, cnt_s, ',');
        std::getline(row, ts_s, ',');

        if (symbol.empty() || exchange.empty() || price_s.empty() || qty_s.empty()) {
            ++errors;
            continue;
        }

        int64_t  price = 0;
        uint64_t qty   = 0;
        uint32_t cnt   = 1;
        uint64_t ts    = now_ns();
        try {
            price = std::stoll(price_s);
            qty   = std::stoull(qty_s);
            if (!cnt_s.empty()) cnt = static_cast<uint32_t>(std::stoul(cnt_s));
            if (!ts_s.empty())  ts  = std::stoull(ts_s);
        } catch (...) {
            ++errors;
            continue;
        }
        const auto parsed_side = parse_side(side_str);
        if (!parsed_side) {
            // Counted as an error rather than defaulted: a CSV row whose side is a typo used to
            // load as a bid, and a loader that silently halves the book is worse than one that
            // says which rows it refused.
            ++errors;
            continue;
        }
        const uint8_t side = *parsed_side;

        ++g_seq;
        ob::DeltaUpdate du{};
        std::strncpy(du.symbol,   symbol.c_str(),   sizeof(du.symbol)   - 1);
        std::strncpy(du.exchange, exchange.c_str(), sizeof(du.exchange) - 1);
        du.sequence_number = g_seq;
        du.timestamp_ns    = ts;
        du.side            = side;
        du.n_levels        = 1;

        ob::Level lev{};
        lev.price = price;
        lev.qty   = qty;
        lev.cnt   = cnt;

        ob::ob_status_t rc = engine.apply_delta(du, &lev);
        if (rc == ob::OB_OK) {
            ++loaded;
            ++g_inserts;
        } else {
            ++errors;
        }
    }

    std::cout << "  Loaded " << loaded << " rows";
    if (errors > 0) std::cout << " (" << errors << " errors)";
    std::cout << " from " << path << "\n";
    std::cout << "  Run 'flush' to make them queryable.\n";
}

void cmd_generate(ob::Engine& engine, std::istringstream& args) {
    std::string symbol, exchange;
    int n_rows = 0;
    args >> symbol >> exchange >> n_rows;
    if (args.fail() || n_rows <= 0) {
        std::cerr << "Usage: generate <symbol> <exchange> <n_rows>\n";
        (void)engine;
        return;
    }

    auto t0 = std::chrono::steady_clock::now();
    int64_t base_bid = 6'500'000;  // $65,000.00 in cents
    int64_t base_ask = 6'510'000;  // $65,100.00 in cents
    uint64_t base_ts = now_ns();

    for (int i = 0; i < n_rows; ++i) {
        ++g_seq;
        bool is_bid = (i % 2 == 0);
        int64_t price = is_bid
            ? base_bid - (i % 100) * 100
            : base_ask + (i % 100) * 100;
        uint64_t qty = 100 + static_cast<uint64_t>(i % 500);

        ob::DeltaUpdate du{};
        std::strncpy(du.symbol,   symbol.c_str(),   sizeof(du.symbol)   - 1);
        std::strncpy(du.exchange, exchange.c_str(), sizeof(du.exchange) - 1);
        du.sequence_number = g_seq;
        du.timestamp_ns    = base_ts + static_cast<uint64_t>(i) * 1000ULL;
        du.side            = is_bid ? ob::SIDE_BID : ob::SIDE_ASK;
        du.n_levels        = 1;

        ob::Level lev{};
        lev.price = price;
        lev.qty   = qty;
        lev.cnt   = 1;

        engine.apply_delta(du, &lev);
        ++g_inserts;
    }

    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    std::cout << "  Generated " << n_rows << " rows for "
              << symbol << "@" << exchange
              << " in " << ms << " ms"
              << " (" << static_cast<int>(n_rows / (ms / 1000.0)) << " rows/sec)\n";
    std::cout << "  Run 'flush' to make them queryable.\n";
}


void cmd_query(ob::Engine& engine, const std::string& sql) {
    auto t0 = std::chrono::steady_clock::now();

    // Results are collected before anything is printed, so the table header can
    // match what actually came back. Printing the orderbook header first meant an
    // aggregate query was rendered as one row of zeros under price/qty/level
    // columns, and a failed query printed an empty table.
    std::vector<ob::QueryResult> results;
    std::string err = engine.execute(sql, [&](const ob::QueryResult& r) {
        results.push_back(r);
    });

    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    if (!err.empty()) {
        std::cerr << "  ERROR: " << err << "\n";
        ++g_queries;
        return;
    }

    // Aggregate response: one value per requested expression, with its scale.
    if (!results.empty() && !results.front().agg_values.empty()) {
        const auto& values = results.front().agg_values;
        std::cout << "  aggregate            | value                |      scale | in units\n";
        std::cout << "  ─────────────────────┼──────────────────────┼────────────┼──────────────\n";
        for (const auto& v : values) {
            if (v.empty) {
                // NULL, not 0: there was nothing to aggregate. A zero here would
                // read as a real measurement.
                std::printf("  %-20s | %20s | %10ld | %s\n",
                            v.name.c_str(), "NULL", v.scale, "-");
            } else {
                std::printf("  %-20s | %20ld | %10ld | %.6g\n",
                            v.name.c_str(), v.value, v.scale,
                            static_cast<double>(v.value) / static_cast<double>(v.scale));
            }
        }
        std::cout << "  ── " << values.size() << " aggregate(s) in " << ms << " ms\n";
        ++g_queries;
        return;
    }

    // seq last, like the wire format: a number the reader can check for holes, and a 0 that says
    // the row predates sequence numbering rather than that it is the first one.
    std::cout << "  ts_ns               | side | level | price        | qty          | orders |      seq\n";
    std::cout << "  ────────────────────┼──────┼───────┼──────────────┼──────────────┼────────┼─────────\n";
    for (const auto& r : results) {
        std::printf("  %-20lu | %-4s | %5u | %12ld | %12lu | %6u | %8lu\n",
                    r.timestamp_ns,
                    r.side == 0 ? "bid" : "ask",
                    r.level,
                    r.price,
                    r.quantity,
                    r.order_count,
                    r.sequence_number);
    }
    std::cout << "  ── " << results.size() << " row(s) in " << ms << " ms\n";
    ++g_queries;
}

void cmd_status() {
    std::cout << "  sequence: " << g_seq
              << "  inserts: " << g_inserts
              << "  queries: " << g_queries << "\n";
}

} // namespace

static int run_cli(int argc, char* argv[]) {
    std::string data_dir = "/tmp/ob_cli_data";
    if (argc > 1) {
        // `ob_cli --data-dir /tmp/x` used to open a store in a directory **named** `--data-dir`,
        // because the whole of argv handling was `argv[1]`. That is #36's `--prot 5599` with a
        // filesystem attached, and the usage line below is what the reader gets instead (#110).
        const std::string first = argv[1];
        if (!first.empty() && first[0] == '-') {
            if (first == "-h" || first == "--help") {
                std::cout << "Usage: ob_cli [data_dir]\n"
                          << "  data_dir defaults to /tmp/ob_cli_data. This tool embeds the engine "
                             "rather than\n  speaking the wire protocol, so it takes no flags.\n";
                return 0;
            }
            std::cerr << "unknown argument '" << first << "'\n"
                      << "Usage: ob_cli [data_dir]   (this tool takes no flags)\n";
            return 2;
        }
        data_dir = first;
    }

    std::filesystem::create_directories(data_dir);
    std::cout << "orderbook-dbengine CLI v0.1.0\n";
    std::cout << "Data directory: " << data_dir << "\n";
    std::cout << "Type 'help' for commands.\n\n";

    // Use a short flush interval (10ms) so data appears quickly in queries.
    ob::Engine engine(data_dir, /*flush_interval_ns=*/10'000'000ULL);
    engine.open();

    std::string line;
    while (true) {
        std::cout << "ob> " << std::flush;
        if (!std::getline(std::cin, line)) break;

        auto pos = line.find_first_not_of(" \t");
        if (pos == std::string::npos) continue;
        line = line.substr(pos);
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string cmd;
        iss >> cmd;

        if (cmd == "quit" || cmd == "exit") {
            break;
        } else if (cmd == "help") {
            print_help();
        } else if (cmd == "status") {
            cmd_status();
        } else if (cmd == "insert") {
            cmd_insert(engine, iss);
        } else if (cmd == "bulk") {
            cmd_bulk(engine, iss);
        } else if (cmd == "flush") {
            std::cout << "  Flushing...\n";
            engine.close();
            engine.open();
            std::cout << "  Done. Data is now queryable.\n";
        } else if (cmd == "load") {
            cmd_load(engine, iss);
        } else if (cmd == "generate") {
            cmd_generate(engine, iss);
        } else if (cmd == "query") {
            auto sql_start = line.find_first_not_of(" \t", 5);
            if (sql_start == std::string::npos) {
                std::cerr << "Usage: query <SQL>\n";
            } else {
                cmd_query(engine, line.substr(sql_start));
            }
        } else {
            std::cerr << "Unknown command: " << cmd << "  (type 'help')\n";
        }
    }

    std::cout << "Shutting down engine...\n";
    engine.close();
    std::cout << "Done.\n";
    return 0;
}

int main(int argc, char* argv[]) {
    // A refusal to start is a refusal, not a crash - the same contract as `ob_tcp_server` (#102),
    // and here for the same reason it was worth closing there: a fix that exists at one of two entry
    // points is not a fix, it is the shape that has cost this repository three separate defects.
    //
    // `Engine::open()` throws when it cannot open the WAL, which a data directory it may not write
    // is enough to produce. Nothing supervises this tool, so the exit code matters less than it does
    // for the server; what matters is that both entry points answer the question the same way, and a
    // static test over `tools/` now requires it of any that gets added.
    try {
        return run_cli(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    } catch (...) {
        std::cerr << "Error: startup failed with an unknown exception\n";
        return 1;
    }
}
