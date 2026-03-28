// Tests for batch-insert: property-based tests (Properties 1–2) and unit tests.
// Feature: batch-insert

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "orderbook/command_parser.hpp"
#include "orderbook/data_model.hpp"

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

/// Build a valid MINSERT wire string from components.
static std::string build_minsert_wire(
    const std::string& symbol,
    const std::string& exchange,
    uint8_t side,
    const std::vector<ob::MinsertArgs::Level>& levels)
{
    std::string out = "MINSERT ";
    out += symbol;
    out += ' ';
    out += exchange;
    out += ' ';
    out += (side == 0) ? "bid" : "ask";
    out += ' ';
    out += std::to_string(levels.size());
    out += '\n';
    for (const auto& lvl : levels) {
        out += std::to_string(lvl.price);
        out += ' ';
        out += std::to_string(lvl.qty);
        out += ' ';
        out += std::to_string(lvl.count);
        out += '\n';
    }
    return out;
}

/// RapidCheck generator for an alphanumeric string of length [1, max_len].
static rc::Gen<std::string> genAlphaNum(int min_len, int max_len) {
    return rc::gen::mapcat(
        rc::gen::inRange(min_len, max_len + 1),
        [](int len) {
            return rc::gen::container<std::string>(
                len,
                rc::gen::oneOf(
                    rc::gen::inRange('a', static_cast<char>('z' + 1)),
                    rc::gen::inRange('A', static_cast<char>('Z' + 1)),
                    rc::gen::inRange('0', static_cast<char>('9' + 1))
                )
            );
        }
    );
}


// ═══════════════════════════════════════════════════════════════════════════════
// Property 1: MINSERT parse/format round-trip
// Feature: batch-insert, Property 1: MINSERT parse/format round-trip
// For any valid MINSERT wire string (random symbol, exchange, side, n_levels
// in [1, 50], random price/qty/count per level), parsing to Command, then
// formatting back to wire and re-parsing should yield an equivalent Command.
// Validates: Requirements 1.1, 1.2, 1.6, 7.1, 7.2
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(BatchInsertProperty, prop_minsert_parse_format_roundtrip, ()) {
    // Generate random symbol (1-15 alphanum chars)
    auto symbol = *genAlphaNum(1, 15);
    // Generate random exchange (1-15 alphanum chars)
    auto exchange = *genAlphaNum(1, 15);
    // Random side: 0 (bid) or 1 (ask)
    auto side = *rc::gen::inRange<uint8_t>(0, 2);
    // Random n_levels in [1, 50] (keep small for speed)
    auto n_levels = *rc::gen::inRange<uint16_t>(1, 51);

    // Generate random levels
    std::vector<ob::MinsertArgs::Level> levels(n_levels);
    for (uint16_t i = 0; i < n_levels; ++i) {
        levels[i].price = *rc::gen::inRange<int64_t>(-1000000, 1000001);
        levels[i].qty   = *rc::gen::inRange<uint64_t>(1, 1000001);
        levels[i].count = *rc::gen::inRange<uint32_t>(1, 10001);
    }

    // Build wire string
    std::string wire = build_minsert_wire(symbol, exchange, side, levels);

    // First parse
    auto cmd1 = ob::parse_minsert(wire);
    RC_ASSERT(cmd1.type == ob::CommandType::MINSERT);

    // Format back to wire
    std::string wire2 = ob::format_command(cmd1);

    // Second parse
    auto cmd2 = ob::parse_minsert(wire2);
    RC_ASSERT(cmd2.type == ob::CommandType::MINSERT);

    // Compare structures
    RC_ASSERT(cmd1.minsert_args.symbol == cmd2.minsert_args.symbol);
    RC_ASSERT(cmd1.minsert_args.exchange == cmd2.minsert_args.exchange);
    RC_ASSERT(cmd1.minsert_args.side == cmd2.minsert_args.side);
    RC_ASSERT(cmd1.minsert_args.n_levels == cmd2.minsert_args.n_levels);
    RC_ASSERT(cmd1.minsert_args.levels.size() == cmd2.minsert_args.levels.size());

    for (size_t i = 0; i < cmd1.minsert_args.levels.size(); ++i) {
        RC_ASSERT(cmd1.minsert_args.levels[i].price == cmd2.minsert_args.levels[i].price);
        RC_ASSERT(cmd1.minsert_args.levels[i].qty   == cmd2.minsert_args.levels[i].qty);
        RC_ASSERT(cmd1.minsert_args.levels[i].count == cmd2.minsert_args.levels[i].count);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 2: Invalid payload line rejects entire batch
// Feature: batch-insert, Property 2: Invalid payload line rejects entire batch
// For any valid MINSERT wire string, if one random payload line is replaced
// with garbage text, parse_minsert should return UNKNOWN.
// Validates: Requirements 1.5
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(BatchInsertProperty, prop_invalid_payload_rejects_batch, ()) {
    // Generate a valid MINSERT
    auto symbol = *genAlphaNum(1, 15);
    auto exchange = *genAlphaNum(1, 15);
    auto side = *rc::gen::inRange<uint8_t>(0, 2);
    auto n_levels = *rc::gen::inRange<uint16_t>(1, 51);

    std::vector<ob::MinsertArgs::Level> levels(n_levels);
    for (uint16_t i = 0; i < n_levels; ++i) {
        levels[i].price = *rc::gen::inRange<int64_t>(-1000000, 1000001);
        levels[i].qty   = *rc::gen::inRange<uint64_t>(1, 1000001);
        levels[i].count = *rc::gen::inRange<uint32_t>(1, 10001);
    }

    std::string wire = build_minsert_wire(symbol, exchange, side, levels);

    // Split into lines
    std::vector<std::string> lines;
    {
        size_t pos = 0;
        while (pos < wire.size()) {
            size_t nl = wire.find('\n', pos);
            if (nl == std::string::npos) {
                lines.push_back(wire.substr(pos));
                break;
            }
            lines.push_back(wire.substr(pos, nl - pos));
            pos = nl + 1;
        }
    }
    // Remove any trailing empty line
    while (!lines.empty() && lines.back().empty()) {
        lines.pop_back();
    }

    // Pick a random payload line index (1..n_levels) to corrupt
    auto corrupt_idx = *rc::gen::inRange<size_t>(1, static_cast<size_t>(n_levels) + 1);
    RC_PRE(corrupt_idx < lines.size());

    // Generate garbage text (non-numeric)
    auto garbage = *genAlphaNum(3, 20);
    // Ensure garbage is not parseable as numbers by prepending letters
    lines[corrupt_idx] = "abc " + garbage + " xyz";

    // Reassemble wire string
    std::string corrupted;
    for (size_t i = 0; i < lines.size(); ++i) {
        corrupted += lines[i];
        corrupted += '\n';
    }

    auto cmd = ob::parse_minsert(corrupted);
    RC_ASSERT(cmd.type == ob::CommandType::UNKNOWN);
}


// ═══════════════════════════════════════════════════════════════════════════════
// Unit tests for MINSERT parsing
// ═══════════════════════════════════════════════════════════════════════════════

// ── MinsertParseValid: 3 levels, verify all fields ───────────────────────────
TEST(BatchInsertUnit, MinsertParseValid) {
    std::string wire =
        "MINSERT AAPL NASDAQ bid 3\n"
        "10050 100 3\n"
        "10051 200 5\n"
        "10052 150 2\n";

    auto cmd = ob::parse_minsert(wire);
    ASSERT_EQ(cmd.type, ob::CommandType::MINSERT);

    const auto& a = cmd.minsert_args;
    EXPECT_EQ(a.symbol, "AAPL");
    EXPECT_EQ(a.exchange, "NASDAQ");
    EXPECT_EQ(a.side, 0u);
    EXPECT_EQ(a.n_levels, 3u);
    ASSERT_EQ(a.levels.size(), 3u);

    EXPECT_EQ(a.levels[0].price, 10050);
    EXPECT_EQ(a.levels[0].qty, 100u);
    EXPECT_EQ(a.levels[0].count, 3u);

    EXPECT_EQ(a.levels[1].price, 10051);
    EXPECT_EQ(a.levels[1].qty, 200u);
    EXPECT_EQ(a.levels[1].count, 5u);

    EXPECT_EQ(a.levels[2].price, 10052);
    EXPECT_EQ(a.levels[2].qty, 150u);
    EXPECT_EQ(a.levels[2].count, 2u);
}

// ── MinsertParseNLevelsZero: n_levels=0 → UNKNOWN ───────────────────────────
TEST(BatchInsertUnit, MinsertParseNLevelsZero) {
    std::string wire = "MINSERT AAPL NASDAQ bid 0\n";
    auto cmd = ob::parse_minsert(wire);
    EXPECT_EQ(cmd.type, ob::CommandType::UNKNOWN);
}

// ── MinsertParseNLevelsExceedsMax: n_levels=1001 → UNKNOWN ──────────────────
TEST(BatchInsertUnit, MinsertParseNLevelsExceedsMax) {
    // Build header with n_levels=1001 and provide 1001 payload lines
    std::string wire = "MINSERT AAPL NASDAQ bid 1001\n";
    for (int i = 0; i < 1001; ++i) {
        wire += std::to_string(10000 + i) + " 100 1\n";
    }
    auto cmd = ob::parse_minsert(wire);
    EXPECT_EQ(cmd.type, ob::CommandType::UNKNOWN);
}

// ── MinsertParseNLevelsMax: n_levels=1000 → accepted ────────────────────────
TEST(BatchInsertUnit, MinsertParseNLevelsMax) {
    std::string wire = "MINSERT AAPL NASDAQ ask 1000\n";
    for (int i = 0; i < 1000; ++i) {
        wire += std::to_string(10000 + i) + " " + std::to_string(100 + i) + " 1\n";
    }
    auto cmd = ob::parse_minsert(wire);
    ASSERT_EQ(cmd.type, ob::CommandType::MINSERT);
    EXPECT_EQ(cmd.minsert_args.n_levels, 1000u);
    EXPECT_EQ(cmd.minsert_args.levels.size(), 1000u);
    EXPECT_EQ(cmd.minsert_args.side, 1u);

    // Spot-check first and last level
    EXPECT_EQ(cmd.minsert_args.levels[0].price, 10000);
    EXPECT_EQ(cmd.minsert_args.levels[0].qty, 100u);
    EXPECT_EQ(cmd.minsert_args.levels[999].price, 10999);
    EXPECT_EQ(cmd.minsert_args.levels[999].qty, 1099u);
}

// ── MinsertParseBadSide: side="sell" → UNKNOWN ──────────────────────────────
TEST(BatchInsertUnit, MinsertParseBadSide) {
    std::string wire =
        "MINSERT AAPL NASDAQ sell 2\n"
        "10050 100 3\n"
        "10051 200 5\n";
    auto cmd = ob::parse_minsert(wire);
    EXPECT_EQ(cmd.type, ob::CommandType::UNKNOWN);
}

// ── MinsertParseMissingPayloadLine: header says 5 but only 3 lines ──────────
TEST(BatchInsertUnit, MinsertParseMissingPayloadLine) {
    std::string wire =
        "MINSERT AAPL NASDAQ bid 5\n"
        "10050 100 3\n"
        "10051 200 5\n"
        "10052 150 2\n";
    auto cmd = ob::parse_minsert(wire);
    EXPECT_EQ(cmd.type, ob::CommandType::UNKNOWN);
}

// ── MinsertParseOptionalCount: payload line without count → default 1 ───────
TEST(BatchInsertUnit, MinsertParseOptionalCount) {
    std::string wire =
        "MINSERT AAPL NASDAQ bid 2\n"
        "10050 100\n"
        "10051 200 5\n";
    auto cmd = ob::parse_minsert(wire);
    ASSERT_EQ(cmd.type, ob::CommandType::MINSERT);
    EXPECT_EQ(cmd.minsert_args.levels[0].count, 1u);  // default
    EXPECT_EQ(cmd.minsert_args.levels[1].count, 5u);
}

// ── MinsertFormatRoundTrip: format → parse → compare ────────────────────────
TEST(BatchInsertUnit, MinsertFormatRoundTrip) {
    // Build a Command manually
    ob::Command original{};
    original.type = ob::CommandType::MINSERT;
    original.minsert_args.symbol = "BTCUSD";
    original.minsert_args.exchange = "BINANCE";
    original.minsert_args.side = 1; // ask
    original.minsert_args.n_levels = 3;
    original.minsert_args.levels = {
        {50000, 10, 2},
        {50001, 20, 4},
        {50002, 30, 6},
    };

    // Format to wire
    std::string wire = ob::format_command(original);

    // Parse back
    auto parsed = ob::parse_minsert(wire);
    ASSERT_EQ(parsed.type, ob::CommandType::MINSERT);

    const auto& a = parsed.minsert_args;
    EXPECT_EQ(a.symbol, "BTCUSD");
    EXPECT_EQ(a.exchange, "BINANCE");
    EXPECT_EQ(a.side, 1u);
    EXPECT_EQ(a.n_levels, 3u);
    ASSERT_EQ(a.levels.size(), 3u);

    EXPECT_EQ(a.levels[0].price, 50000);
    EXPECT_EQ(a.levels[0].qty, 10u);
    EXPECT_EQ(a.levels[0].count, 2u);

    EXPECT_EQ(a.levels[1].price, 50001);
    EXPECT_EQ(a.levels[1].qty, 20u);
    EXPECT_EQ(a.levels[1].count, 4u);

    EXPECT_EQ(a.levels[2].price, 50002);
    EXPECT_EQ(a.levels[2].qty, 30u);
    EXPECT_EQ(a.levels[2].count, 6u);
}


// ═══════════════════════════════════════════════════════════════════════════════
// Session buffering tests — requires Session header
// ═══════════════════════════════════════════════════════════════════════════════

#include "orderbook/session.hpp"

// Helper: build a valid INSERT wire string
static std::string build_insert_wire(
    const std::string& symbol,
    const std::string& exchange,
    uint8_t side,
    int64_t price,
    uint64_t qty,
    uint32_t count)
{
    std::string out = "INSERT ";
    out += symbol;
    out += ' ';
    out += exchange;
    out += ' ';
    out += (side == 0) ? "bid" : "ask";
    out += ' ';
    out += std::to_string(price);
    out += ' ';
    out += std::to_string(qty);
    out += ' ';
    out += std::to_string(count);
    out += '\n';
    return out;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 3: Session multi-line buffering delivers complete MINSERT
// Feature: batch-insert, Property 3: Session multi-line buffering
// For any valid MINSERT wire string, split into random byte chunks (1 byte to
// full), feeding all chunks to Session::feed() should yield exactly 1 element
// containing header + n_levels payload lines.
// Validates: Requirements 2.1, 2.2
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(BatchInsertProperty, prop_session_multiline_buffering, ()) {
    auto symbol = *genAlphaNum(1, 10);
    auto exchange = *genAlphaNum(1, 10);
    auto side = *rc::gen::inRange<uint8_t>(0, 2);
    auto n_levels = *rc::gen::inRange<uint16_t>(1, 21);

    std::vector<ob::MinsertArgs::Level> levels(n_levels);
    for (uint16_t i = 0; i < n_levels; ++i) {
        levels[i].price = *rc::gen::inRange<int64_t>(1, 100001);
        levels[i].qty   = *rc::gen::inRange<uint64_t>(1, 100001);
        levels[i].count = *rc::gen::inRange<uint32_t>(1, 1001);
    }

    std::string wire = build_minsert_wire(symbol, exchange, side, levels);

    // Generate random chunk sizes to split the wire
    std::vector<size_t> chunk_sizes;
    size_t remaining = wire.size();
    while (remaining > 0) {
        auto chunk = *rc::gen::inRange<size_t>(1, remaining + 1);
        chunk_sizes.push_back(chunk);
        remaining -= chunk;
    }

    ob::Session session(-1);  // dummy fd
    std::vector<std::string> all_results;
    size_t offset = 0;
    for (auto cs : chunk_sizes) {
        auto results = session.feed(wire.data() + offset, cs);
        for (auto& r : results) {
            all_results.push_back(std::move(r));
        }
        offset += cs;
    }

    // Should have exactly 1 element
    RC_ASSERT(all_results.size() == 1u);

    // The element should be parseable as a valid MINSERT
    auto cmd = ob::parse_minsert(all_results[0]);
    RC_ASSERT(cmd.type == ob::CommandType::MINSERT);
    RC_ASSERT(cmd.minsert_args.n_levels == n_levels);
    RC_ASSERT(cmd.minsert_args.levels.size() == static_cast<size_t>(n_levels));
    RC_ASSERT(cmd.minsert_args.symbol == symbol);
    RC_ASSERT(cmd.minsert_args.exchange == exchange);
    RC_ASSERT(cmd.minsert_args.side == side);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 4: Mixed INSERT and MINSERT commands parse independently
// Feature: batch-insert, Property 4: Mixed INSERT and MINSERT
// For any random sequence of 3-10 INSERT and MINSERT commands as a continuous
// byte stream, Session::feed() should return the correct number of elements,
// each parseable.
// Validates: Requirements 2.3, 6.3
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(BatchInsertProperty, prop_mixed_insert_minsert, ()) {
    auto cmd_count = *rc::gen::inRange(3, 11);

    std::string stream;
    int expected_count = cmd_count;

    for (int i = 0; i < cmd_count; ++i) {
        bool use_minsert = *rc::gen::inRange(0, 2) == 1;
        auto sym = *genAlphaNum(1, 8);
        auto exch = *genAlphaNum(1, 8);
        auto side_val = *rc::gen::inRange<uint8_t>(0, 2);

        if (use_minsert) {
            auto nl = *rc::gen::inRange<uint16_t>(1, 6);
            std::vector<ob::MinsertArgs::Level> lvls(nl);
            for (uint16_t j = 0; j < nl; ++j) {
                lvls[j].price = *rc::gen::inRange<int64_t>(1, 100001);
                lvls[j].qty   = *rc::gen::inRange<uint64_t>(1, 100001);
                lvls[j].count = *rc::gen::inRange<uint32_t>(1, 1001);
            }
            stream += build_minsert_wire(sym, exch, side_val, lvls);
        } else {
            auto price = *rc::gen::inRange<int64_t>(1, 100001);
            auto qty   = *rc::gen::inRange<uint64_t>(1, 100001);
            auto cnt   = *rc::gen::inRange<uint32_t>(1, 1001);
            stream += build_insert_wire(sym, exch, side_val, price, qty, cnt);
        }
    }

    ob::Session session(-1);
    auto results = session.feed(stream.data(), stream.size());

    RC_ASSERT(static_cast<int>(results.size()) == expected_count);

    // Each element should be parseable
    for (const auto& elem : results) {
        if (elem.find('\n') != std::string::npos) {
            // Multi-line → MINSERT
            auto cmd = ob::parse_minsert(elem);
            RC_ASSERT(cmd.type == ob::CommandType::MINSERT);
        } else {
            // Single line → INSERT
            auto cmd = ob::parse_command(elem);
            RC_ASSERT(cmd.type == ob::CommandType::INSERT);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Unit tests for Session MINSERT buffering (Task 3.5)
// ═══════════════════════════════════════════════════════════════════════════════

// ── SessionMinsertOneShot: entire MINSERT in one feed() → 1 element ─────────
TEST(BatchInsertSession, SessionMinsertOneShot) {
    std::string wire =
        "MINSERT AAPL NASDAQ bid 3\n"
        "10050 100 3\n"
        "10051 200 5\n"
        "10052 150 2\n";

    ob::Session session(-1);
    auto results = session.feed(wire.data(), wire.size());

    ASSERT_EQ(results.size(), 1u);

    auto cmd = ob::parse_minsert(results[0]);
    ASSERT_EQ(cmd.type, ob::CommandType::MINSERT);
    EXPECT_EQ(cmd.minsert_args.n_levels, 3u);
    EXPECT_EQ(cmd.minsert_args.symbol, "AAPL");
}

// ── SessionMinsertByteByByte: MINSERT fed byte by byte → 1 element ──────────
TEST(BatchInsertSession, SessionMinsertByteByByte) {
    std::string wire =
        "MINSERT AAPL NASDAQ bid 3\n"
        "10050 100 3\n"
        "10051 200 5\n"
        "10052 150 2\n";

    ob::Session session(-1);
    std::vector<std::string> all_results;

    for (size_t i = 0; i < wire.size(); ++i) {
        auto results = session.feed(wire.data() + i, 1);
        for (auto& r : results) {
            all_results.push_back(std::move(r));
        }
    }

    ASSERT_EQ(all_results.size(), 1u);

    auto cmd = ob::parse_minsert(all_results[0]);
    ASSERT_EQ(cmd.type, ob::CommandType::MINSERT);
    EXPECT_EQ(cmd.minsert_args.n_levels, 3u);
}

// ── SessionMinsertIncomplete: header + 2 of 5 lines, no more data → 0 ──────
TEST(BatchInsertSession, SessionMinsertIncomplete) {
    std::string wire =
        "MINSERT AAPL NASDAQ bid 5\n"
        "10050 100 3\n"
        "10051 200 5\n";

    ob::Session session(-1);
    auto results = session.feed(wire.data(), wire.size());

    // Only 2 of 5 payload lines received — should not deliver anything
    EXPECT_EQ(results.size(), 0u);
}

// ── SessionMinsertThenInsert: MINSERT + INSERT in one stream → 2 elements ───
TEST(BatchInsertSession, SessionMinsertThenInsert) {
    std::string wire =
        "MINSERT AAPL NASDAQ bid 2\n"
        "10050 100 3\n"
        "10051 200 5\n"
        "INSERT BTCUSD BINANCE ask 50000 10 1\n";

    ob::Session session(-1);
    auto results = session.feed(wire.data(), wire.size());

    ASSERT_EQ(results.size(), 2u);

    // First element: MINSERT
    auto cmd1 = ob::parse_minsert(results[0]);
    ASSERT_EQ(cmd1.type, ob::CommandType::MINSERT);
    EXPECT_EQ(cmd1.minsert_args.n_levels, 2u);

    // Second element: INSERT
    auto cmd2 = ob::parse_command(results[1]);
    ASSERT_EQ(cmd2.type, ob::CommandType::INSERT);
    EXPECT_EQ(cmd2.insert_args.symbol, "BTCUSD");
}

// ── SessionInsertThenMinsert: INSERT + MINSERT in one stream → 2 elements ───
TEST(BatchInsertSession, SessionInsertThenMinsert) {
    std::string wire =
        "INSERT BTCUSD BINANCE ask 50000 10 1\n"
        "MINSERT AAPL NASDAQ bid 2\n"
        "10050 100 3\n"
        "10051 200 5\n";

    ob::Session session(-1);
    auto results = session.feed(wire.data(), wire.size());

    ASSERT_EQ(results.size(), 2u);

    // First element: INSERT
    auto cmd1 = ob::parse_command(results[0]);
    ASSERT_EQ(cmd1.type, ob::CommandType::INSERT);
    EXPECT_EQ(cmd1.insert_args.symbol, "BTCUSD");

    // Second element: MINSERT
    auto cmd2 = ob::parse_minsert(results[1]);
    ASSERT_EQ(cmd2.type, ob::CommandType::MINSERT);
    EXPECT_EQ(cmd2.minsert_args.n_levels, 2u);
    EXPECT_EQ(cmd2.minsert_args.symbol, "AAPL");
}


// ═══════════════════════════════════════════════════════════════════════════════
// Engine-based tests — requires Engine, TempDir, EngineGuard
// ═══════════════════════════════════════════════════════════════════════════════

#include "orderbook/engine.hpp"
#include "orderbook/tcp_server.hpp"
#include "orderbook/types.hpp"

#include <atomic>
#include <cstring>
#include <filesystem>

namespace fs = std::filesystem;

namespace {

static std::atomic<uint64_t> g_batch_dir_counter{0};

std::string make_batch_temp_dir(const std::string& prefix) {
    auto tmp = fs::temp_directory_path() /
               (prefix + std::to_string(g_batch_dir_counter.fetch_add(1, std::memory_order_relaxed)));
    fs::create_directories(tmp);
    return tmp.string();
}

struct BatchTempDir {
    std::string path;
    explicit BatchTempDir(const std::string& prefix)
        : path(make_batch_temp_dir(prefix)) {}
    ~BatchTempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    BatchTempDir(const BatchTempDir&) = delete;
    BatchTempDir& operator=(const BatchTempDir&) = delete;
};

struct BatchEngineGuard {
    ob::Engine engine;
    std::string dir;

    explicit BatchEngineGuard(const std::string& d,
                              uint64_t flush_ns = 1'000'000'000ULL)
        : engine(d, flush_ns, ob::FsyncPolicy::NONE), dir(d) {
        engine.open();
    }
    ~BatchEngineGuard() {
        engine.close();
    }
    BatchEngineGuard(const BatchEngineGuard&) = delete;
    BatchEngineGuard& operator=(const BatchEngineGuard&) = delete;
};

/// Count rows returned by a SELECT query on the given symbol.exchange.
int batch_query_row_count(ob::Engine& engine, const char* symbol, const char* exchange) {
    int count = 0;
    std::string sql = std::string("SELECT * FROM '") + symbol + "'.'" + exchange +
                      "' WHERE timestamp BETWEEN 0 AND 9999999999999999999";
    std::string err = engine.execute(sql, [&](const ob::QueryResult&) { ++count; });
    return count;
}

} // anonymous namespace

// ═══════════════════════════════════════════════════════════════════════════════
// Property 5: MINSERT produces single apply_delta call with correct n_levels
// Feature: batch-insert, Property 5: MINSERT produces single apply_delta
// Generate valid MINSERT with random n_levels, execute via a real Engine.
// Verify the data is queryable (correct number of rows after flush).
// Validates: Requirements 3.1, 3.2
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(BatchInsertProperty, prop_minsert_single_apply_delta, ()) {
    auto n_levels = *rc::gen::inRange<uint16_t>(1, 51);
    auto symbol = *genAlphaNum(1, 10);
    auto exchange = *genAlphaNum(1, 10);
    auto side_val = *rc::gen::inRange<uint8_t>(0, 2);

    // Build levels
    std::vector<ob::MinsertArgs::Level> mlevels(n_levels);
    for (uint16_t i = 0; i < n_levels; ++i) {
        mlevels[i].price = *rc::gen::inRange<int64_t>(1, 100001);
        mlevels[i].qty   = *rc::gen::inRange<uint64_t>(1, 100001);
        mlevels[i].count = *rc::gen::inRange<uint32_t>(1, 1001);
    }

    // Build wire and parse
    std::string wire = build_minsert_wire(symbol, exchange, side_val, mlevels);
    auto cmd = ob::parse_minsert(wire);
    RC_ASSERT(cmd.type == ob::CommandType::MINSERT);

    // Execute via real Engine
    BatchTempDir tmp("batch_prop5_");
    BatchEngineGuard eg(tmp.path);
    ob::Session session(-1);
    ob::ServerStats stats;

    std::string response = ob::execute_command(cmd, eg.engine, session, stats, false);
    RC_ASSERT(response == "OK\n\n");

    // Flush and verify data is queryable
    eg.engine.flush_incremental();
    int row_count = batch_query_row_count(eg.engine, symbol.c_str(), exchange.c_str());
    RC_ASSERT(row_count == static_cast<int>(n_levels));
}

// ═══════════════════════════════════════════════════════════════════════════════
// Unit tests for execute_command MINSERT (Task 5.3)
// ═══════════════════════════════════════════════════════════════════════════════

// ── ExecuteMinsertValid: valid MINSERT → "OK\n\n", data queryable after flush
TEST(BatchInsertExecute, ExecuteMinsertValid) {
    std::string wire =
        "MINSERT AAPL NASDAQ bid 3\n"
        "10050 100 3\n"
        "10051 200 5\n"
        "10052 150 2\n";

    auto cmd = ob::parse_minsert(wire);
    ASSERT_EQ(cmd.type, ob::CommandType::MINSERT);

    BatchTempDir tmp("batch_exec_valid_");
    BatchEngineGuard eg(tmp.path);
    ob::Session session(-1);
    ob::ServerStats stats;

    std::string response = ob::execute_command(cmd, eg.engine, session, stats, false);
    EXPECT_EQ(response, "OK\n\n");

    // Flush and verify data
    eg.engine.flush_incremental();
    int row_count = batch_query_row_count(eg.engine, "AAPL", "NASDAQ");
    EXPECT_EQ(row_count, 3);
}

// ── ExecuteMinsertReadOnly: read_only=true → "ERR read-only replica"
TEST(BatchInsertExecute, ExecuteMinsertReadOnly) {
    std::string wire =
        "MINSERT AAPL NASDAQ bid 2\n"
        "10050 100 3\n"
        "10051 200 5\n";

    auto cmd = ob::parse_minsert(wire);
    ASSERT_EQ(cmd.type, ob::CommandType::MINSERT);

    BatchTempDir tmp("batch_exec_ro_");
    BatchEngineGuard eg(tmp.path);
    ob::Session session(-1);
    ob::ServerStats stats;

    std::string response = ob::execute_command(cmd, eg.engine, session, stats, /*read_only=*/true);
    EXPECT_NE(response.find("ERR read-only replica"), std::string::npos);
}
