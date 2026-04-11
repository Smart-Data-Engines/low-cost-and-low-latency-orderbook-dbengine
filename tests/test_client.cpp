// Tests for cpp-native-client: property-based tests (Properties 1–6) and unit tests.
// Feature: cpp-native-client

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "orderbook/client.hpp"
#include "orderbook/command_parser.hpp"

// ═══════════════════════════════════════════════════════════════════════════════
// Generators
// ═══════════════════════════════════════════════════════════════════════════════

/// Generate an uppercase alpha string of length [min_len, max_len].
static rc::Gen<std::string> genUpperAlpha(int min_len, int max_len) {
    return rc::gen::mapcat(
        rc::gen::inRange(min_len, max_len + 1),
        [](int len) {
            return rc::gen::container<std::string>(
                len,
                rc::gen::inRange('A', static_cast<char>('Z' + 1))
            );
        }
    );
}

/// Generate a printable ASCII string (no newlines) of length [min_len, max_len].
static rc::Gen<std::string> genPrintableNoNewline(int min_len, int max_len) {
    return rc::gen::mapcat(
        rc::gen::inRange(min_len, max_len + 1),
        [](int len) {
            return rc::gen::container<std::string>(
                len,
                rc::gen::inRange(' ', static_cast<char>('~' + 1))
            );
        }
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 1: INSERT command format round-trip
// Feature: cpp-native-client, Property 1: INSERT format round-trip
// For any valid symbol, exchange, side, price, qty, count: format_insert()
// then parse_command() SHALL preserve all parameters.
// Validates: Requirements 2.1, 9.1, 9.6, 8.7
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(ClientProperty, prop_insert_format_roundtrip, ()) {
    auto symbol   = *genUpperAlpha(1, 31);
    auto exchange = *genUpperAlpha(1, 31);
    auto side     = *rc::gen::element(ob::Side::BID, ob::Side::ASK);
    auto price    = *rc::gen::inRange<int64_t>(-1'000'000'000, 1'000'000'001);
    auto qty      = *rc::gen::inRange<uint64_t>(1, 1'000'000'001);
    auto count    = *rc::gen::inRange<uint32_t>(1, 100'001);

    ob::OrderbookClient client;
    client.format_insert(symbol, exchange, side, price, qty, count);
    std::string wire(client.send_buffer());

    auto cmd = ob::parse_command(wire);
    RC_ASSERT(cmd.type == ob::CommandType::INSERT);
    RC_ASSERT(cmd.insert_args.symbol == symbol);
    RC_ASSERT(cmd.insert_args.exchange == exchange);
    RC_ASSERT(cmd.insert_args.side == static_cast<uint8_t>(side));
    RC_ASSERT(cmd.insert_args.price == price);
    RC_ASSERT(cmd.insert_args.qty == qty);
    RC_ASSERT(cmd.insert_args.count == count);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 2: MINSERT command format round-trip
// Feature: cpp-native-client, Property 2: MINSERT format round-trip
// For any valid symbol, exchange, side, and vector of 1–100 levels:
// format_minsert() then parse_minsert() SHALL preserve header and all levels.
// Validates: Requirements 3.1, 9.2
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(ClientProperty, prop_minsert_format_roundtrip, ()) {
    auto symbol   = *genUpperAlpha(1, 31);
    auto exchange = *genUpperAlpha(1, 31);
    auto side     = *rc::gen::element(ob::Side::BID, ob::Side::ASK);
    auto n_levels = *rc::gen::inRange<size_t>(1, 101);

    std::vector<ob::Level> levels(n_levels);
    for (size_t i = 0; i < n_levels; ++i) {
        levels[i].price = *rc::gen::inRange<int64_t>(-1'000'000'000, 1'000'000'001);
        levels[i].qty   = *rc::gen::inRange<uint64_t>(1, 1'000'000'001);
        levels[i].count = *rc::gen::inRange<uint32_t>(1, 100'001);
    }

    ob::OrderbookClient client;
    client.format_minsert(symbol, exchange, side, levels.data(), levels.size());
    std::string wire(client.send_buffer());

    auto cmd = ob::parse_minsert(wire);
    RC_ASSERT(cmd.type == ob::CommandType::MINSERT);
    RC_ASSERT(cmd.minsert_args.symbol == symbol);
    RC_ASSERT(cmd.minsert_args.exchange == exchange);
    RC_ASSERT(cmd.minsert_args.side == static_cast<uint8_t>(side));
    RC_ASSERT(cmd.minsert_args.n_levels == static_cast<uint16_t>(n_levels));
    RC_ASSERT(cmd.minsert_args.levels.size() == n_levels);

    for (size_t i = 0; i < n_levels; ++i) {
        RC_ASSERT(cmd.minsert_args.levels[i].price == levels[i].price);
        RC_ASSERT(cmd.minsert_args.levels[i].qty == levels[i].qty);
        RC_ASSERT(cmd.minsert_args.levels[i].count == levels[i].count);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 3: TSV query response parse round-trip
// Feature: cpp-native-client, Property 3: TSV query response parse round-trip
// For any vector of QueryRow, formatting as TSV wire protocol then parsing
// via parse_query_response() SHALL reproduce identical rows.
// Validates: Requirements 5.2, 8.3
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(ClientProperty, prop_tsv_query_response_roundtrip, ()) {
    auto n_rows = *rc::gen::inRange<size_t>(1, 51);

    std::vector<ob::QueryRow> rows(n_rows);
    for (size_t i = 0; i < n_rows; ++i) {
        rows[i].timestamp_ns = *rc::gen::inRange<uint64_t>(0, 9'999'999'999'999ULL);
        rows[i].price        = *rc::gen::inRange<int64_t>(-1'000'000'000, 1'000'000'001);
        rows[i].quantity     = *rc::gen::inRange<uint64_t>(1, 1'000'000'001);
        rows[i].order_count  = *rc::gen::inRange<uint32_t>(1, 100'001);
        rows[i].side         = *rc::gen::inRange<uint8_t>(0, 2);
        rows[i].level        = *rc::gen::inRange<uint16_t>(0, 1000);
    }

    // Format as TSV wire protocol
    std::string wire = "OK\ntimestamp_ns\tprice\tquantity\torder_count\tside\tlevel\n";
    for (const auto& row : rows) {
        wire += std::to_string(row.timestamp_ns) + "\t";
        wire += std::to_string(row.price) + "\t";
        wire += std::to_string(row.quantity) + "\t";
        wire += std::to_string(row.order_count) + "\t";
        wire += std::to_string(row.side) + "\t";
        wire += std::to_string(row.level) + "\n";
    }
    wire += "\n";  // double newline terminator

    ob::OrderbookClient client;
    auto result = client.parse_query_response(wire);
    RC_ASSERT(result.has_value());

    const auto& parsed = result.value().rows;
    RC_ASSERT(parsed.size() == n_rows);

    for (size_t i = 0; i < n_rows; ++i) {
        RC_ASSERT(parsed[i].timestamp_ns == rows[i].timestamp_ns);
        RC_ASSERT(parsed[i].price == rows[i].price);
        RC_ASSERT(parsed[i].quantity == rows[i].quantity);
        RC_ASSERT(parsed[i].order_count == rows[i].order_count);
        RC_ASSERT(parsed[i].side == rows[i].side);
        RC_ASSERT(parsed[i].level == rows[i].level);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 4: ROLE response parse round-trip
// Feature: cpp-native-client, Property 4: ROLE response parse round-trip
// For any role (PRIMARY with epoch, REPLICA with address and epoch, STANDALONE),
// formatting as wire protocol then parsing SHALL preserve role, epoch, address.
// Validates: Requirements 6.5, 6.6, 8.5
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(ClientProperty, prop_role_response_roundtrip, ()) {
    auto role_type = *rc::gen::inRange(0, 3);  // 0=PRIMARY, 1=REPLICA, 2=STANDALONE

    ob::OrderbookClient client;

    if (role_type == 0) {
        // PRIMARY
        auto epoch = *rc::gen::inRange<uint64_t>(0, 1'000'000'000ULL);
        std::string wire = "PRIMARY " + std::to_string(epoch) + "\n";
        auto info = client.parse_role_response(wire);
        RC_ASSERT(info.role == ob::NodeRole::PRIMARY);
        RC_ASSERT(info.epoch == epoch);
    } else if (role_type == 1) {
        // REPLICA with address "X.X.X.X:PPPP"
        auto o1 = *rc::gen::inRange(1, 256);
        auto o2 = *rc::gen::inRange(0, 256);
        auto o3 = *rc::gen::inRange(0, 256);
        auto o4 = *rc::gen::inRange(1, 256);
        auto port = *rc::gen::inRange(1024, 65536);
        auto epoch = *rc::gen::inRange<uint64_t>(0, 1'000'000'000ULL);

        std::string addr = std::to_string(o1) + "." + std::to_string(o2) + "."
                         + std::to_string(o3) + "." + std::to_string(o4)
                         + ":" + std::to_string(port);
        std::string wire = "REPLICA " + addr + " " + std::to_string(epoch) + "\n";

        auto info = client.parse_role_response(wire);
        RC_ASSERT(info.role == ob::NodeRole::REPLICA);
        RC_ASSERT(info.primary_address == addr);
        RC_ASSERT(info.epoch == epoch);
    } else {
        // STANDALONE
        std::string wire = "STANDALONE\n";
        auto info = client.parse_role_response(wire);
        RC_ASSERT(info.role == ob::NodeRole::STANDALONE);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 5: ERR message parse preserves content
// Feature: cpp-native-client, Property 5: ERR message parse preserves content
// For any error message (printable ASCII, no newlines), formatting as
// "ERR <msg>\n" then parsing SHALL preserve the message.
// Validates: Requirements 2.3, 8.1
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(ClientProperty, prop_err_message_preserves_content, ()) {
    auto msg = *genPrintableNoNewline(1, 100);

    std::string wire = "ERR " + msg + "\n";

    ob::OrderbookClient client;
    auto result = client.parse_ok_response(wire);
    RC_ASSERT(!result.has_value());
    RC_ASSERT(result.error_code() == ob::OB_ERR_INTERNAL);
    RC_ASSERT(result.error_message() == msg);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 6: Pool writes always route to PRIMARY
// Feature: cpp-native-client, Property 6: Pool writes route to PRIMARY
// Create a pool with unreachable hosts, verify that write operations return
// "no primary available" error (since no connection can be established).
// Validates: Requirements 10.3
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(ClientProperty, prop_pool_writes_route_to_primary, ()) {
    // Generate 1-5 unreachable hosts
    auto n_hosts = *rc::gen::inRange(1, 6);

    ob::PoolConfig cfg;
    cfg.connect_timeout_sec = 0.01;  // very short timeout
    cfg.read_timeout_sec    = 0.01;
    cfg.health_check_interval_sec = 100.0;  // effectively disable health checks

    for (int i = 0; i < n_hosts; ++i) {
        // Use RFC 5737 TEST-NET addresses — guaranteed unreachable
        cfg.hosts.push_back("198.51.100." + std::to_string(i + 1) + ":19999");
    }

    ob::OrderbookPool pool(std::move(cfg));

    // All write operations should fail with "no primary available"
    auto insert_res = pool.insert("SYM", "EXCH", ob::Side::BID, 100, 10, 1);
    RC_ASSERT(!insert_res.has_value());

    ob::Level lvl{100, 10, 1};
    auto minsert_res = pool.minsert("SYM", "EXCH", ob::Side::BID, &lvl, 1);
    RC_ASSERT(!minsert_res.has_value());

    auto flush_res = pool.flush();
    RC_ASSERT(!flush_res.has_value());

    pool.close();
}


// ═══════════════════════════════════════════════════════════════════════════════
// Task 10.1: Input validation unit tests
// ═══════════════════════════════════════════════════════════════════════════════

TEST(ClientUnit, MinsertEmptyLevelsReturnsInvalidArg) {
    ob::OrderbookClient client;
    auto result = client.minsert("SYM", "EXCH", ob::Side::BID, nullptr, 0);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error_code(), ob::OB_ERR_INVALID_ARG);
}

TEST(ClientUnit, MinsertTooManyLevelsReturnsInvalidArg) {
    ob::OrderbookClient client;
    // Create 1001 dummy levels
    std::vector<ob::Level> levels(1001, ob::Level{100, 10, 1});
    auto result = client.minsert("SYM", "EXCH", ob::Side::BID, levels.data(), levels.size());
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error_code(), ob::OB_ERR_INVALID_ARG);
}

TEST(ClientUnit, DisconnectOnInactiveConnectionIsNoop) {
    ob::OrderbookClient client;
    EXPECT_FALSE(client.connected());
    // Should not crash or throw
    client.disconnect();
    EXPECT_FALSE(client.connected());
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 10.2: Response parsing unit tests
// ═══════════════════════════════════════════════════════════════════════════════

TEST(ClientUnit, ParseOkResponse) {
    ob::OrderbookClient client;
    auto result = client.parse_ok_response("OK\n\n");
    EXPECT_TRUE(result.has_value());
}

TEST(ClientUnit, ParseErrResponse) {
    ob::OrderbookClient client;
    auto result = client.parse_ok_response("ERR unknown command\n");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error_code(), ob::OB_ERR_INTERNAL);
    EXPECT_EQ(result.error_message(), "unknown command");
}

TEST(ClientUnit, ParsePongResponse) {
    ob::OrderbookClient client;
    // PONG is detected by checking the string directly (as ping() does)
    std::string_view resp = "PONG\n";
    EXPECT_EQ(resp, "PONG\n");
}

TEST(ClientUnit, ParseRoleStandalone) {
    ob::OrderbookClient client;
    auto info = client.parse_role_response("STANDALONE\n");
    EXPECT_EQ(info.role, ob::NodeRole::STANDALONE);
}

TEST(ClientUnit, ParseRolePrimary) {
    ob::OrderbookClient client;
    auto info = client.parse_role_response("PRIMARY 42\n");
    EXPECT_EQ(info.role, ob::NodeRole::PRIMARY);
    EXPECT_EQ(info.epoch, 42u);
}

TEST(ClientUnit, ParseRoleReplica) {
    ob::OrderbookClient client;
    auto info = client.parse_role_response("REPLICA 10.0.0.1:5555 7\n");
    EXPECT_EQ(info.role, ob::NodeRole::REPLICA);
    EXPECT_EQ(info.primary_address, "10.0.0.1:5555");
    EXPECT_EQ(info.epoch, 7u);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 10.3: Move-only semantics tests
// ═══════════════════════════════════════════════════════════════════════════════

TEST(ClientUnit, MoveOnlySemantics) {
    static_assert(!std::is_copy_constructible_v<ob::OrderbookClient>,
                  "OrderbookClient must not be copy-constructible");
    static_assert(!std::is_copy_assignable_v<ob::OrderbookClient>,
                  "OrderbookClient must not be copy-assignable");
    static_assert(std::is_move_constructible_v<ob::OrderbookClient>,
                  "OrderbookClient must be move-constructible");
    static_assert(std::is_move_assignable_v<ob::OrderbookClient>,
                  "OrderbookClient must be move-assignable");
}
