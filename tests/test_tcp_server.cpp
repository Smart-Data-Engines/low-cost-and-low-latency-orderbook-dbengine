// Feature: tcp-server — Property-based + Unit tests for TCP server components
// Tests cover: CommandParser, ResponseFormatter, Session, SessionManager,
//              execute_command, and CLI argument parsing.

#include <chrono>
#include <fstream>
#include <regex>
#include <sstream>
#include <iterator>
#include "orderbook/command_parser.hpp"
#include "orderbook/response_formatter.hpp"
#include "orderbook/session.hpp"
#include "orderbook/tcp_server.hpp"
#include "orderbook/capabilities.hpp"

#include <cctype>
#include "orderbook/engine.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

namespace fs = std::filesystem;

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

static std::string make_temp_dir(const std::string& prefix) {
    auto tmp = fs::temp_directory_path() / (prefix + std::to_string(std::rand()));
    fs::create_directories(tmp);
    return tmp.string();
}

// ═══════════════════════════════════════════════════════════════════════════════
// RapidCheck generators
// ═══════════════════════════════════════════════════════════════════════════════

namespace rc {

// Generator for alphanumeric + hyphen strings (non-empty, no spaces/newlines)
static Gen<std::string> genSymbol() {
    return gen::nonEmpty(
        gen::container<std::string>(
            gen::oneOf(
                gen::inRange('a', 'z'),
                gen::inRange('A', 'Z'),
                gen::inRange('0', '9'),
                gen::just('-')
            )
        )
    );
}

/// A Command carrying nothing but its type.
static ob::Command simple_command(ob::CommandType t) {
    ob::Command cmd{};
    cmd.type = t;
    return cmd;
}

// Generator for alphanumeric strings (non-empty, no spaces/newlines/hyphens)
static Gen<std::string> genExchange() {
    return gen::nonEmpty(
        gen::container<std::string>(
            gen::oneOf(
                gen::inRange('a', 'z'),
                gen::inRange('A', 'Z'),
                gen::inRange('0', '9')
            )
        )
    );
}

// Generator for valid InsertArgs
static Gen<ob::InsertArgs> genInsertArgs() {
    return gen::build<ob::InsertArgs>(
        gen::set(&ob::InsertArgs::symbol,   genSymbol()),
        gen::set(&ob::InsertArgs::exchange, genExchange()),
        gen::set(&ob::InsertArgs::side,     gen::map(gen::inRange(0, 2), [](int v) { return static_cast<uint8_t>(v); })),
        gen::set(&ob::InsertArgs::price,    gen::inRange<int64_t>(-1000000000LL, 1000000000LL)),
        gen::set(&ob::InsertArgs::qty,      gen::inRange<uint64_t>(1, 1000000000ULL)),
        gen::set(&ob::InsertArgs::count,    gen::inRange<uint32_t>(1, 100000))
    );
}

// Generator for valid Command objects (all types)
static Gen<ob::Command> genCommand() {
    return gen::oneOf(
        // SELECT
        gen::map(genSymbol(), [](std::string sym) {
            ob::Command cmd{};
            cmd.type = ob::CommandType::SELECT;
            // Build a valid SELECT SQL — no embedded newlines
            cmd.raw_sql = "SELECT * FROM '" + sym + "'.'BINANCE' WHERE timestamp BETWEEN 0 AND 999";
            return cmd;
        }),
        // INSERT
        gen::map(genInsertArgs(), [](ob::InsertArgs args) {
            ob::Command cmd{};
            cmd.type = ob::CommandType::INSERT;
            cmd.insert_args = std::move(args);
            return cmd;
        }),
        // Simple commands.
        //
        // Built by setting the type on a value-initialised Command rather than by positional
        // aggregate initialisation. The positional form listed every field, so each new field in
        // Command broke five lines of this generator with a -Wmissing-field-initializers error and
        // no relation to what was being tested - which is how #30 first touched this file.
        gen::just(simple_command(ob::CommandType::FLUSH)),
        gen::just(simple_command(ob::CommandType::PING)),
        gen::just(simple_command(ob::CommandType::STATUS)),
        gen::just(simple_command(ob::CommandType::ROLE)),
        gen::just(simple_command(ob::CommandType::QUIT))
    );
}

// Generator for QueryResult rows
static Gen<ob::QueryResult> genQueryResult() {
    return gen::build<ob::QueryResult>(
        gen::set(&ob::QueryResult::timestamp_ns,    gen::inRange<uint64_t>(0, 9999999999ULL)),
        gen::set(&ob::QueryResult::sequence_number, gen::inRange<uint64_t>(0, 100000ULL)),
        gen::set(&ob::QueryResult::price,           gen::inRange<int64_t>(0, 1000000000LL)),
        gen::set(&ob::QueryResult::quantity,         gen::inRange<uint64_t>(1, 1000000ULL)),
        gen::set(&ob::QueryResult::order_count,     gen::inRange<uint32_t>(1, 10000)),
        gen::set(&ob::QueryResult::side,            gen::map(gen::inRange(0, 2), [](int v) { return static_cast<uint8_t>(v); })),
        gen::set(&ob::QueryResult::level,           gen::inRange<uint16_t>(0, 100))
    );
}

// Generator for non-empty error message strings (no newlines)
static Gen<std::string> genErrorMessage() {
    return gen::nonEmpty(
        gen::container<std::string>(
            gen::suchThat(gen::inRange<char>(32, 126), [](char c) {
                return c != '\n' && c != '\r';
            })
        )
    );
}

} // namespace rc

// ═══════════════════════════════════════════════════════════════════════════════
// Task 2.2 — Property 1: Command round-trip
// Feature: tcp-server, Property 1: Command round-trip
// Validates: Requirements 8.1, 8.2, 8.3
// ═══════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(CommandParser, RoundTrip, ()) {
    auto cmd = *rc::genCommand();

    // UNKNOWN commands produce empty format string — skip
    if (cmd.type == ob::CommandType::UNKNOWN) return;

    std::string wire = ob::format_command(cmd);
    ob::Command parsed = ob::parse_command(wire);

    RC_ASSERT(parsed.type == cmd.type);

    if (cmd.type == ob::CommandType::SELECT) {
        RC_ASSERT(parsed.raw_sql == cmd.raw_sql);
    } else if (cmd.type == ob::CommandType::INSERT) {
        RC_ASSERT(parsed.insert_args.symbol   == cmd.insert_args.symbol);
        RC_ASSERT(parsed.insert_args.exchange == cmd.insert_args.exchange);
        RC_ASSERT(parsed.insert_args.side     == cmd.insert_args.side);
        RC_ASSERT(parsed.insert_args.price    == cmd.insert_args.price);
        RC_ASSERT(parsed.insert_args.qty      == cmd.insert_args.qty);
        RC_ASSERT(parsed.insert_args.count    == cmd.insert_args.count);
    }
}


// ═══════════════════════════════════════════════════════════════════════════════
// Task 2.3 — Property 5: Newline-delimited wire format
// Feature: tcp-server, Property 5: Newline-delimited wire format
// Validates: Requirements 3.1
// ═══════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(CommandParser, NewlineDelimitedFormat, ()) {
    auto cmd = *rc::genCommand();

    if (cmd.type == ob::CommandType::UNKNOWN) return;

    std::string wire = ob::format_command(cmd);

    // Must end with exactly one '\n'
    RC_ASSERT(!wire.empty());
    RC_ASSERT(wire.back() == '\n');

    // No embedded '\n' before the terminator
    auto body = std::string_view(wire).substr(0, wire.size() - 1);
    RC_ASSERT(body.find('\n') == std::string_view::npos);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 2.4 — Property 6: Valid INSERT parsing
// Feature: tcp-server, Property 6: Valid INSERT arguments produce OK
// Validates: Requirements 5.1
// ═══════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(CommandParser, ValidInsertParsing, ()) {
    auto args = *rc::genInsertArgs();

    // Build INSERT string manually
    std::string line = "INSERT " + args.symbol + " " + args.exchange + " "
                     + (args.side == 0 ? "bid" : "ask") + " "
                     + std::to_string(args.price) + " "
                     + std::to_string(args.qty) + " "
                     + std::to_string(args.count);

    ob::Command parsed = ob::parse_command(line);

    RC_ASSERT(parsed.type == ob::CommandType::INSERT);
    RC_ASSERT(parsed.insert_args.symbol   == args.symbol);
    RC_ASSERT(parsed.insert_args.exchange == args.exchange);
    RC_ASSERT(parsed.insert_args.side     == args.side);
    RC_ASSERT(parsed.insert_args.price    == args.price);
    RC_ASSERT(parsed.insert_args.qty      == args.qty);
    RC_ASSERT(parsed.insert_args.count    == args.count);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 2.5 — Property 7: Invalid INSERT rejection
// Feature: tcp-server, Property 7: Invalid INSERT arguments produce parse failure
// Validates: Requirements 5.2
// ═══════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(CommandParser, InvalidInsertRejection, ()) {
    // Generate one of several invalid INSERT variants
    int variant = *rc::gen::inRange(0, 3);

    std::string line;
    switch (variant) {
    case 0: {
        // Missing fields: only 1-4 tokens after INSERT
        int n_tokens = *rc::gen::inRange(0, 4);
        line = "INSERT";
        auto sym = *rc::genSymbol();
        auto exch = *rc::genExchange();
        if (n_tokens >= 1) line += " " + sym;
        if (n_tokens >= 2) line += " " + exch;
        if (n_tokens >= 3) line += " bid";
        if (n_tokens >= 4) line += " 100";
        // Still missing qty (need at least 5 tokens after INSERT = 6 total)
        break;
    }
    case 1: {
        // Invalid side (not bid/ask)
        auto sym = *rc::genSymbol();
        auto exch = *rc::genExchange();
        line = "INSERT " + sym + " " + exch + " sell 100 200";
        break;
    }
    case 2: {
        // Non-numeric price
        auto sym = *rc::genSymbol();
        auto exch = *rc::genExchange();
        line = "INSERT " + sym + " " + exch + " bid abc 200";
        break;
    }
    default:
        // Empty INSERT
        line = "INSERT";
        break;
    }

    ob::Command parsed = ob::parse_command(line);
    RC_ASSERT(parsed.type == ob::CommandType::UNKNOWN);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 3.2 — Property 2: Response round-trip
// Feature: tcp-server, Property 2: Response round-trip
// Validates: Requirements 8.4
// ═══════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(ResponseFormatter, RoundTrip, ()) {
    bool is_error = *rc::gen::element(true, false);

    if (is_error) {
        // Error variant
        auto msg = *rc::genErrorMessage();
        std::string wire = ob::format_error(msg);
        ob::ParsedResponse parsed = ob::parse_response(wire);

        RC_ASSERT(parsed.is_error);
        RC_ASSERT(parsed.error_message == msg);
    } else {
        // Success variant with TSV data
        auto rows = *rc::gen::nonEmpty(
            rc::gen::container<std::vector<ob::QueryResult>>(rc::genQueryResult())
        );

        std::string wire = ob::format_query_response(rows, ob::all_query_columns());
        ob::ParsedResponse parsed = ob::parse_response(wire);

        RC_ASSERT(!parsed.is_error);
        // Header should have 7 columns: sequence_number was appended by #65
        RC_ASSERT(parsed.header_columns.size() == 7u);
        RC_ASSERT(parsed.header_columns[6] == "sequence_number");
        // Row count should match
        RC_ASSERT(parsed.rows.size() == rows.size());

        // Verify each row's values match
        for (size_t i = 0; i < rows.size(); ++i) {
            RC_ASSERT(parsed.rows[i].size() == size_t(7));
            RC_ASSERT(parsed.rows[i][0] == std::to_string(rows[i].timestamp_ns));
            RC_ASSERT(parsed.rows[i][1] == std::to_string(rows[i].price));
            RC_ASSERT(parsed.rows[i][2] == std::to_string(rows[i].quantity));
            RC_ASSERT(parsed.rows[i][3] == std::to_string(rows[i].order_count));
            RC_ASSERT(parsed.rows[i][4] == std::to_string(rows[i].side));
            RC_ASSERT(parsed.rows[i][5] == std::to_string(rows[i].level));
            RC_ASSERT(parsed.rows[i][6] == std::to_string(rows[i].sequence_number));
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 3.3 — Property 3: Success response format invariant
// Feature: tcp-server, Property 3: Success response format invariant
// Validates: Requirements 3.3, 3.5
// ═══════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(ResponseFormatter, SuccessFormatInvariant, ()) {
    auto rows = *rc::gen::nonEmpty(
        rc::gen::container<std::vector<ob::QueryResult>>(rc::genQueryResult())
    );

    std::string wire = ob::format_query_response(rows, ob::all_query_columns());

    // (a) Starts with "OK\n"
    RC_ASSERT(wire.size() >= size_t(3));
    RC_ASSERT(wire.substr(0, 3) == "OK\n");

    // (d) Ends with "\n\n" (empty line terminator)
    RC_ASSERT(wire.size() >= size_t(2));
    RC_ASSERT(wire.substr(wire.size() - 2) == "\n\n");

    // Split into lines
    std::vector<std::string> lines;
    size_t pos = 0;
    while (pos < wire.size()) {
        auto nl = wire.find('\n', pos);
        if (nl == std::string::npos) {
            lines.emplace_back(wire.substr(pos));
            break;
        }
        lines.emplace_back(wire.substr(pos, nl - pos));
        pos = nl + 1;
    }

    // lines[0] = "OK", lines[1] = header, lines[2..N+1] = data, lines[N+2] = ""
    RC_ASSERT(lines.size() >= size_t(3)); // OK + header + at least empty terminator
    RC_ASSERT(lines[0] == "OK");

    // (b) Header line has tab-separated column names
    const auto& header = lines[1];
    RC_ASSERT(header.find('\t') != std::string::npos);

    // (c) One data row per result
    // Data rows are lines[2] through lines[2 + rows.size() - 1]
    for (size_t i = 0; i < rows.size(); ++i) {
        RC_ASSERT(size_t(2) + i < lines.size());
        RC_ASSERT(!lines[2 + i].empty()); // data rows are non-empty
    }

    // Last line should be empty (terminator)
    RC_ASSERT(lines.back().empty());
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 3.4 — Property 4: Error response format invariant
// Feature: tcp-server, Property 4: Error response format invariant
// Validates: Requirements 3.4
// ═══════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(ResponseFormatter, ErrorFormatInvariant, ()) {
    auto msg = *rc::genErrorMessage();

    std::string wire = ob::format_error(msg);

    // Must match "ERR <msg>\n" exactly
    std::string expected = "ERR " + msg + "\n";
    RC_ASSERT(wire == expected);

    // No trailing empty line (no double \n)
    RC_ASSERT(wire.size() < size_t(2) || wire.substr(wire.size() - 2) != "\n\n");
}


// ═══════════════════════════════════════════════════════════════════════════════
// Task 7.2 — Property 8: CLI argument parsing
// Feature: tcp-server, Property 8: CLI argument parsing
// Validates: Requirements 1.3
// ═══════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(CliArgs, Parsing, ()) {
    auto port = *rc::gen::inRange<uint16_t>(1, 65535);
    // data_dir: non-empty, no spaces, printable ASCII
    auto data_dir = *rc::gen::nonEmpty(
        rc::gen::container<std::string>(
            rc::gen::suchThat(rc::gen::inRange<char>(33, 126), [](char c) {
                return c != ' ' && c != '\t';
            })
        )
    );

    std::string port_str = std::to_string(port);

    // Build argv
    const char* prog = "ob_tcp_server";
    const char* arg1 = "--port";
    const char* arg3 = "--data-dir";

    // We need mutable char* for argv
    std::vector<char*> argv_vec;
    std::string prog_s(prog);
    std::string arg1_s(arg1);
    std::string arg3_s(arg3);

    argv_vec.push_back(prog_s.data());
    argv_vec.push_back(arg1_s.data());
    argv_vec.push_back(port_str.data());
    argv_vec.push_back(arg3_s.data());
    argv_vec.push_back(data_dir.data());

    ob::ServerConfig config = ob::parse_cli_args(
        static_cast<int>(argv_vec.size()), argv_vec.data());

    RC_ASSERT(config.port == port);
    RC_ASSERT(config.data_dir == data_dir);
}

// Test defaults when args are omitted
TEST(CliArgs, DefaultValues) {
    const char* prog = "ob_tcp_server";
    char* argv[] = { const_cast<char*>(prog) };

    ob::ServerConfig config = ob::parse_cli_args(1, argv);

    EXPECT_EQ(config.port, 9090);
    EXPECT_EQ(config.data_dir, "/tmp/ob_data");
    EXPECT_EQ(config.max_sessions, 64);
}

// Test partial args (only port)
TEST(CliArgs, PartialArgsPort) {
    std::string port_str = "8080";
    char* argv[] = {
        const_cast<char*>("ob_tcp_server"),
        const_cast<char*>("--port"),
        port_str.data()
    };

    ob::ServerConfig config = ob::parse_cli_args(3, argv);

    EXPECT_EQ(config.port, 8080);
    EXPECT_EQ(config.data_dir, "/tmp/ob_data"); // default
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 5.2 — Session and SessionManager unit tests
// Validates: Requirements 2.2, 2.4
// ═══════════════════════════════════════════════════════════════════════════════

// Helper: create a socketpair for testing Session
static std::pair<int, int> make_socketpair() {
    int fds[2];
    int rc = ::socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    if (rc != 0) {
        ADD_FAILURE() << "socketpair() failed";
        return {-1, -1};
    }
    return {fds[0], fds[1]};
}

// Test: feed() with partial lines — data without '\n' stays in buffer
TEST(Session, FeedPartialLine) {
    auto [fd_server, fd_client] = make_socketpair();
    ob::Session session(fd_server);

    // Send data without newline — should return no complete lines
    const char* data1 = "PING";
    auto lines1 = session.feed(data1, 4);
    EXPECT_TRUE(lines1.empty());

    // Now send the newline — should return the complete line
    const char* data2 = "\n";
    auto lines2 = session.feed(data2, 1);
    ASSERT_EQ(lines2.size(), 1u);
    EXPECT_EQ(lines2[0], "PING");

    ::close(fd_server);
    ::close(fd_client);
}

// Test: feed() with multiple lines in one buffer
TEST(Session, FeedMultipleLines) {
    auto [fd_server, fd_client] = make_socketpair();
    ob::Session session(fd_server);

    const char* data = "line1\nline2\n";
    auto lines = session.feed(data, std::strlen(data));
    ASSERT_EQ(lines.size(), 2u);
    EXPECT_EQ(lines[0], "line1");
    EXPECT_EQ(lines[1], "line2");

    ::close(fd_server);
    ::close(fd_client);
}

// Test: feed() with fragmented data across multiple calls
TEST(Session, FeedFragmented) {
    auto [fd_server, fd_client] = make_socketpair();
    ob::Session session(fd_server);

    // First fragment: partial first line + complete second line + partial third
    const char* data1 = "SEL";
    auto lines1 = session.feed(data1, 3);
    EXPECT_TRUE(lines1.empty());

    const char* data2 = "ECT *\nPING\nSTA";
    auto lines2 = session.feed(data2, std::strlen(data2));
    ASSERT_EQ(lines2.size(), 2u);
    EXPECT_EQ(lines2[0], "SELECT *");
    EXPECT_EQ(lines2[1], "PING");

    const char* data3 = "TUS\n";
    auto lines3 = session.feed(data3, std::strlen(data3));
    ASSERT_EQ(lines3.size(), 1u);
    EXPECT_EQ(lines3[0], "STATUS");

    ::close(fd_server);
    ::close(fd_client);
}

// Test: SessionManager max_sessions limit
TEST(SessionManager, MaxSessionsLimit) {
    // Use socketpairs so we have valid fds that SessionManager can close
    ob::SessionManager mgr(64);
    std::vector<std::pair<int, int>> pairs;

    // Add 64 sessions — all should succeed
    for (int i = 0; i < 64; ++i) {
        auto [fd_a, fd_b] = make_socketpair();
        ASSERT_TRUE(mgr.add_session(fd_a))
            << "Failed to add session " << i;
        pairs.push_back({fd_a, fd_b});
    }

    EXPECT_EQ(mgr.active_count(), 64);

    // 65th should fail
    auto [fd_extra_a, fd_extra_b] = make_socketpair();
    EXPECT_FALSE(mgr.add_session(fd_extra_a));

    // Clean up
    mgr.close_all();
    ::close(fd_extra_a);
    ::close(fd_extra_b);
    // close the "b" side of each pair
    for (auto& [a, b] : pairs) {
        ::close(b);
    }
}

// Test: SessionManager remove_session and active_count
TEST(SessionManager, RemoveAndCount) {
    ob::SessionManager mgr(10);

    auto [fd1_a, fd1_b] = make_socketpair();
    auto [fd2_a, fd2_b] = make_socketpair();
    auto [fd3_a, fd3_b] = make_socketpair();

    EXPECT_TRUE(mgr.add_session(fd1_a));
    EXPECT_TRUE(mgr.add_session(fd2_a));
    EXPECT_TRUE(mgr.add_session(fd3_a));
    EXPECT_EQ(mgr.active_count(), 3);

    mgr.remove_session(fd2_a);
    EXPECT_EQ(mgr.active_count(), 2);
    EXPECT_EQ(mgr.get_session(fd2_a), nullptr);

    // fd1 and fd3 should still be accessible
    EXPECT_NE(mgr.get_session(fd1_a), nullptr);
    EXPECT_NE(mgr.get_session(fd3_a), nullptr);

    mgr.close_all();
    ::close(fd1_b);
    ::close(fd2_b);
    ::close(fd3_b);
}


// ═══════════════════════════════════════════════════════════════════════════════
// Task 6.2 — execute_command unit tests
// Validates: Requirements 4.1, 5.1, 5.2, 6.1, 6.2
// ═══════════════════════════════════════════════════════════════════════════════

class ExecuteCommandTest : public ::testing::Test {
protected:
    std::string temp_dir_;
    std::unique_ptr<ob::Engine> engine_;
    ob::ServerStats stats_;
    int fd_server_ = -1;
    int fd_client_ = -1;

    void SetUp() override {
        temp_dir_ = make_temp_dir("exec_cmd_test_");
        engine_ = std::make_unique<ob::Engine>(temp_dir_);
        engine_->open();

        int fds[2];
        ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
        fd_server_ = fds[0];
        fd_client_ = fds[1];
    }

    void TearDown() override {
        engine_->close();
        if (fd_server_ >= 0) ::close(fd_server_);
        if (fd_client_ >= 0) ::close(fd_client_);
        fs::remove_all(temp_dir_);
    }
};

// Test PING → returns "PONG\n"
TEST_F(ExecuteCommandTest, PingReturnsPong) {
    ob::Session session(fd_server_);
    ob::Command cmd{};
    cmd.type = ob::CommandType::PING;

    std::string response = ob::execute_command(cmd, *engine_, session, stats_);
    EXPECT_EQ(response, "PONG\n");
}

// Test STATUS → returns "OK\n..." with stats
TEST_F(ExecuteCommandTest, StatusReturnsStats) {
    ob::Session session(fd_server_);
    ob::Command cmd{};
    cmd.type = ob::CommandType::STATUS;

    std::string response = ob::execute_command(cmd, *engine_, session, stats_);

    // Should start with "OK\n"
    ASSERT_GE(response.size(), 3u);
    EXPECT_EQ(response.substr(0, 3), "OK\n");

    // Should contain the stats header
    EXPECT_NE(response.find("sessions"), std::string::npos);
    EXPECT_NE(response.find("queries"), std::string::npos);
    EXPECT_NE(response.find("inserts"), std::string::npos);
}

// STATUS answers the engine's figures from a snapshot of its own (#151). Formatting from the shared
// ServerStats instead - which STATUS no longer writes, because two client loops answering STATUS at
// once would race on its vector - answers zeros for every engine figure, and nothing in this suite
// looked at one until the mutation that did it survived.
TEST_F(ExecuteCommandTest, StatusAnswersTheEnginesFiguresNotTheSharedDefaults) {
    ob::Session session(fd_server_);
    ob::Command insert{};
    insert.type = ob::CommandType::INSERT;
    insert.insert_args.symbol   = "SNAP";
    insert.insert_args.exchange = "EX";
    insert.insert_args.side     = 0;
    insert.insert_args.price    = 100;
    insert.insert_args.qty      = 5;
    insert.insert_args.count    = 1;
    ASSERT_EQ(ob::execute_command(insert, *engine_, session, stats_), "OK\n\n");

    ob::Command status{};
    status.type = ob::CommandType::STATUS;
    const std::string answer = ob::execute_command(status, *engine_, session, stats_);

    // "OK", the header, then the values under it.
    const auto header_end = answer.find('\n', answer.find('\n') + 1);
    ASSERT_NE(header_end, std::string::npos) << answer;
    const auto values_end = answer.find('\n', header_end + 1);
    ASSERT_NE(values_end, std::string::npos) << answer;
    std::vector<std::string> values;
    std::istringstream row(answer.substr(header_end + 1, values_end - header_end - 1));
    for (std::string field; std::getline(row, field, '\t');) values.push_back(field);
    ASSERT_EQ(values.size(), 7u) << "sessions queries inserts pending_rows wal_file segments "
                                    "symbols, got: " << answer;
    EXPECT_EQ(values[6], "1") << "one symbol written and STATUS counts " << values[6]
                              << ", which is what an answer from the shared defaults says";
    EXPECT_NE(values[3], "0") << "a row pending and STATUS says none";
}

// Test UNKNOWN command → returns "ERR unknown command\n"
TEST_F(ExecuteCommandTest, UnknownCommandReturnsError) {
    ob::Session session(fd_server_);
    ob::Command cmd{};
    cmd.type = ob::CommandType::UNKNOWN;

    std::string response = ob::execute_command(cmd, *engine_, session, stats_);
    EXPECT_EQ(response, "ERR unknown command\n");
}

// Test INSERT with valid args → returns "OK\n\n"
TEST_F(ExecuteCommandTest, InsertValidArgs) {
    ob::Session session(fd_server_);
    ob::Command cmd{};
    cmd.type = ob::CommandType::INSERT;
    cmd.insert_args.symbol   = "BTC-USD";
    cmd.insert_args.exchange = "BINANCE";
    cmd.insert_args.side     = 0; // bid
    cmd.insert_args.price    = 6500000;
    cmd.insert_args.qty      = 1500;
    cmd.insert_args.count    = 1;

    std::string response = ob::execute_command(cmd, *engine_, session, stats_);
    EXPECT_EQ(response, "OK\n\n");
    EXPECT_EQ(session.inserts_executed(), 1u);
    EXPECT_EQ(stats_.total_inserts.load(), 1u);
}

// Test FLUSH → returns "OK\n\n"
TEST_F(ExecuteCommandTest, FlushReturnsOk) {
    ob::Session session(fd_server_);
    ob::Command cmd{};
    cmd.type = ob::CommandType::FLUSH;

    std::string response = ob::execute_command(cmd, *engine_, session, stats_);
    EXPECT_EQ(response, "OK\n\n");
}

// Test QUIT → returns empty string (signals session close)
TEST_F(ExecuteCommandTest, QuitReturnsEmpty) {
    ob::Session session(fd_server_);
    ob::Command cmd{};
    cmd.type = ob::CommandType::QUIT;

    std::string response = ob::execute_command(cmd, *engine_, session, stats_);
    EXPECT_TRUE(response.empty());
}


// ═══════════════════════════════════════════════════════════════════════════════
// Task 5.1 — Read-only mode unit tests
// Validates: Requirements 3.1, 3.2
// ═══════════════════════════════════════════════════════════════════════════════

class ReadOnlyCommandTest : public ExecuteCommandTest {};

// Test: INSERT rejected in read-only mode
TEST_F(ReadOnlyCommandTest, InsertRejected) {
    ob::Session session(fd_server_);
    ob::Command cmd{};
    cmd.type = ob::CommandType::INSERT;
    cmd.insert_args.symbol   = "BTC-USD";
    cmd.insert_args.exchange = "BINANCE";
    cmd.insert_args.side     = 0;
    cmd.insert_args.price    = 6500000;
    cmd.insert_args.qty      = 1500;
    cmd.insert_args.count    = 1;

    std::string response = ob::execute_command(cmd, *engine_, session, stats_, /*read_only=*/true);
    EXPECT_EQ(response, "ERR read-only replica\n");
    // Insert counter should NOT be incremented
    EXPECT_EQ(session.inserts_executed(), 0u);
    EXPECT_EQ(stats_.total_inserts.load(), 0u);
}

// Test: FLUSH rejected in read-only mode
TEST_F(ReadOnlyCommandTest, FlushRejected) {
    ob::Session session(fd_server_);
    ob::Command cmd{};
    cmd.type = ob::CommandType::FLUSH;

    std::string response = ob::execute_command(cmd, *engine_, session, stats_, /*read_only=*/true);
    EXPECT_EQ(response, "ERR read-only replica\n");
}

// Test: SELECT still works in read-only mode (Requirement 3.1)
TEST_F(ReadOnlyCommandTest, SelectAllowed) {
    ob::Session session(fd_server_);
    ob::Command cmd{};
    cmd.type = ob::CommandType::SELECT;
    cmd.raw_sql = "SELECT * FROM 'BTC-USD'.'BINANCE' WHERE timestamp BETWEEN 0 AND 999";

    std::string response = ob::execute_command(cmd, *engine_, session, stats_, /*read_only=*/true);
    // Should NOT be rejected with read-only error — the query is allowed
    EXPECT_EQ(response.find("read-only replica"), std::string::npos);
}

// Test: PING still works in read-only mode
TEST_F(ReadOnlyCommandTest, PingAllowed) {
    ob::Session session(fd_server_);
    ob::Command cmd{};
    cmd.type = ob::CommandType::PING;

    std::string response = ob::execute_command(cmd, *engine_, session, stats_, /*read_only=*/true);
    EXPECT_EQ(response, "PONG\n");
}

// Test: STATUS still works in read-only mode
TEST_F(ReadOnlyCommandTest, StatusAllowed) {
    ob::Session session(fd_server_);
    ob::Command cmd{};
    cmd.type = ob::CommandType::STATUS;

    std::string response = ob::execute_command(cmd, *engine_, session, stats_, /*read_only=*/true);
    ASSERT_GE(response.size(), 3u);
    EXPECT_EQ(response.substr(0, 3), "OK\n");
}

// Test: --read-only CLI flag parsed correctly
TEST(CliArgs, ReadOnlyFlag) {
    char* argv[] = {
        const_cast<char*>("ob_tcp_server"),
        const_cast<char*>("--read-only"),
        const_cast<char*>("--port"),
        const_cast<char*>("8080")
    };

    ob::ServerConfig config = ob::parse_cli_args(4, argv);
    EXPECT_TRUE(config.read_only);
    EXPECT_EQ(config.port, 8080);
}

// Test: read_only defaults to false
TEST(CliArgs, ReadOnlyDefaultFalse) {
    char* argv[] = { const_cast<char*>("ob_tcp_server") };

    ob::ServerConfig config = ob::parse_cli_args(1, argv);
    EXPECT_FALSE(config.read_only);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Aggregate responses (aggregations-over-wire)
//
// format_query_response() has one fixed row header and never read agg_values, so
// every aggregate query answered a network client with OK and a row of zeros. These
// tests cover the shape that replaced it, including the two things the row format
// had no room for: an empty result and a scale factor.
// ═══════════════════════════════════════════════════════════════════════════════

TEST(ResponseFormatterAgg, SingleAggregateCarriesValueAndScale) {
    std::vector<ob::AggValue> values = {
        {"SPREAD(*)", 1000, false, 1},
    };

    std::string wire = ob::format_agg_response(values);
    ob::ParsedResponse parsed = ob::parse_response(wire);

    ASSERT_FALSE(parsed.is_error) << wire;
    ASSERT_EQ(parsed.header_columns.size(), 3u);
    EXPECT_EQ(parsed.header_columns[0], "name");
    EXPECT_EQ(parsed.header_columns[1], "value");
    EXPECT_EQ(parsed.header_columns[2], "scale");

    ASSERT_EQ(parsed.rows.size(), 1u);
    ASSERT_EQ(parsed.rows[0].size(), 3u);
    EXPECT_EQ(parsed.rows[0][0], "SPREAD(*)");
    EXPECT_EQ(parsed.rows[0][1], "1000");
    EXPECT_EQ(parsed.rows[0][2], "1");
}

TEST(ResponseFormatterAgg, SeveralAggregatesKeepTheirOrder) {
    std::vector<ob::AggValue> values = {
        {"SPREAD(*)",    1000,            false, 1},
        {"MID_PRICE(*)", 100500000000LL,  false, 1000000},
        {"IMBALANCE(10)", 200000000LL,    false, 1000000000},
    };

    ob::ParsedResponse parsed = ob::parse_response(ob::format_agg_response(values));

    ASSERT_EQ(parsed.rows.size(), 3u);
    EXPECT_EQ(parsed.rows[0][0], "SPREAD(*)");
    EXPECT_EQ(parsed.rows[1][0], "MID_PRICE(*)");
    EXPECT_EQ(parsed.rows[2][0], "IMBALANCE(10)");
    // The scale is the whole point: 100500000000 with scale 1000000 is 100500.
    EXPECT_EQ(parsed.rows[1][1], "100500000000");
    EXPECT_EQ(parsed.rows[1][2], "1000000");
}

TEST(ResponseFormatterAgg, EmptyResultIsNullNotZero) {
    std::vector<ob::AggValue> values = {
        {"SPREAD(*)", 0, true, 1},
    };

    ob::ParsedResponse parsed = ob::parse_response(ob::format_agg_response(values));

    ASSERT_EQ(parsed.rows.size(), 1u);
    EXPECT_EQ(parsed.rows[0][1], "NULL")
        << "a spread with no ask side is absent, not zero, and a trading client "
           "cannot tell those apart if it is serialised as 0";
    EXPECT_EQ(parsed.rows[0][2], "1") << "the scale is still meaningful for a NULL";
}

TEST(ResponseFormatterAgg, ResponseFollowsTheSameFramingAsARowResponse) {
    std::vector<ob::AggValue> values = {{"SPREAD(*)", 1000, false, 1}};
    std::string wire = ob::format_agg_response(values);

    EXPECT_EQ(wire.rfind("OK\n", 0), 0u) << "must start with OK";
    EXPECT_GE(wire.size(), 2u);
    EXPECT_EQ(wire.substr(wire.size() - 2), "\n\n")
        << "must end with the blank-line terminator every client waits for";
}

TEST(ResponseFormatterAgg, NoAggregatesStillProducesAWellFormedResponse) {
    std::string wire = ob::format_agg_response({});

    ob::ParsedResponse parsed = ob::parse_response(wire);
    EXPECT_FALSE(parsed.is_error);
    EXPECT_TRUE(parsed.rows.empty());
}

// ═══════════════════════════════════════════════════════════════════════════════
// Session output buffering (large-response-write-path)
//
// Client sockets are non-blocking, so a response bigger than the socket send buffer
// makes write() return EAGAIN. The old send_response() treated that as a failure and
// the server closed the session — truncating every response above a couple of
// megabytes, with nothing in the log. EAGAIN has to mean "come back later".
// ═══════════════════════════════════════════════════════════════════════════════

namespace {

/// A non-blocking socketpair with a deliberately tiny send buffer, so EAGAIN is
/// reached with a small payload instead of megabytes.
std::pair<int, int> make_tiny_nonblocking_socketpair() {
    int fds[2];
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
        ADD_FAILURE() << "socketpair() failed";
        return {-1, -1};
    }
    int small = 4096;
    ::setsockopt(fds[0], SOL_SOCKET, SO_SNDBUF, &small, sizeof(small));
    ::setsockopt(fds[1], SOL_SOCKET, SO_RCVBUF, &small, sizeof(small));
    int flags = ::fcntl(fds[0], F_GETFL, 0);
    ::fcntl(fds[0], F_SETFL, flags | O_NONBLOCK);
    return {fds[0], fds[1]};
}

} // namespace

TEST(SessionOutput, FullSocketBufferIsNotAnError) {
    auto [fd_server, fd_client] = make_tiny_nonblocking_socketpair();
    ASSERT_GE(fd_server, 0);
    ob::Session session(fd_server);

    // Far more than the 4KB buffers configured above.
    const std::string big(512 * 1024, 'x');

    EXPECT_TRUE(session.send_response(big))
        << "a full socket buffer was reported as a failure, which is what closed "
           "the session mid-response";
    EXPECT_TRUE(session.has_pending_output())
        << "the unsent remainder must stay queued for EPOLLOUT";
    EXPECT_GT(session.pending_output_bytes(), 0u);

    ::close(fd_client);
    ::close(fd_server);
}

TEST(SessionOutput, PendingOutputDrainsAsTheReaderConsumes) {
    auto [fd_server, fd_client] = make_tiny_nonblocking_socketpair();
    ASSERT_GE(fd_server, 0);
    ob::Session session(fd_server);

    const std::string big(256 * 1024, 'y');
    ASSERT_TRUE(session.send_response(big));
    ASSERT_TRUE(session.has_pending_output());

    // Drain from the other end, flushing after each read, exactly as the epoll loop
    // does when EPOLLOUT fires.
    std::vector<char> buf(8192);
    size_t received = 0;
    for (int guard = 0; guard < 10000 && received < big.size(); ++guard) {
        ssize_t n = ::read(fd_client, buf.data(), buf.size());
        if (n > 0) {
            received += static_cast<size_t>(n);
            ASSERT_TRUE(session.flush_output());
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            ASSERT_TRUE(session.flush_output());
            continue;
        }
        break;
    }

    EXPECT_EQ(received, big.size()) << "not everything arrived";
    EXPECT_FALSE(session.has_pending_output()) << "queue should be empty now";

    ::close(fd_client);
    ::close(fd_server);
}

TEST(SessionOutput, SmallResponseLeavesNothingQueued) {
    auto [fd_server, fd_client] = make_tiny_nonblocking_socketpair();
    ASSERT_GE(fd_server, 0);
    ob::Session session(fd_server);

    EXPECT_TRUE(session.send_response("PONG\n"));
    EXPECT_FALSE(session.has_pending_output())
        << "a five-byte response must not need EPOLLOUT";
    EXPECT_EQ(session.pending_output_bytes(), 0u);

    ::close(fd_client);
    ::close(fd_server);
}

TEST(SessionOutput, BufferCapIsEnforced) {
    auto [fd_server, fd_client] = make_tiny_nonblocking_socketpair();
    ASSERT_GE(fd_server, 0);
    ob::Session session(fd_server);

    // Nobody reads, so everything past the socket buffer accumulates. Past the cap
    // the session must be refused rather than growing the server's memory for a
    // client that is not consuming.
    const std::string chunk(8 * 1024 * 1024, 'z');
    bool refused = false;
    for (int i = 0; i < 16; ++i) {
        if (!session.send_response(chunk)) {
            refused = true;
            break;
        }
    }

    EXPECT_TRUE(refused)
        << "queued output grew past the cap without being refused; a client that "
           "never reads would take the server's memory with it";

    ::close(fd_client);
    ::close(fd_server);
}

TEST(SessionOutput, WriteToAClosedPeerIsAnError) {
    auto [fd_server, fd_client] = make_tiny_nonblocking_socketpair();
    ASSERT_GE(fd_server, 0);
    ob::Session session(fd_server);
    ::close(fd_client);

    // EPIPE is a genuine failure and must still be reported as one — the fix must
    // not turn every write error into "come back later".
    bool failed = false;
    for (int i = 0; i < 200; ++i) {
        if (!session.send_response(std::string(64 * 1024, 'q'))) {
            failed = true;
            break;
        }
    }
    EXPECT_TRUE(failed) << "writing to a closed peer was reported as success";

    ::close(fd_server);
}

TEST(SessionOutput, CloseAfterFlushIsRecorded) {
    auto [fd_server, fd_client] = make_tiny_nonblocking_socketpair();
    ASSERT_GE(fd_server, 0);
    ob::Session session(fd_server);

    EXPECT_FALSE(session.close_requested());
    session.request_close_after_flush();
    EXPECT_TRUE(session.close_requested())
        << "QUIT arriving while a response drains must be remembered, not acted on "
           "immediately";

    ::close(fd_client);
    ::close(fd_server);
}

TEST(ResponseFormatterStatus, ReplicaCountIsReportedEvenWhenZero) {
    // A monitoring consumer must be able to read zero. Emitting the field only when
    // replicas exist makes "no replicas" indistinguishable from "field missing",
    // which is the difference between a healthy standalone node and a broken parser.
    ob::ServerStats stats;
    stats.replicas.clear();

    const std::string wire = ob::format_status(stats);

    EXPECT_NE(wire.find("replicas: 0"), std::string::npos)
        << "STATUS omitted the replica count for a node with no replicas:\n" << wire;
    EXPECT_EQ(wire.find("replica["), std::string::npos)
        << "per-replica detail lines should not appear when there are none";
}

// ── SUBSCRIBE / UNSUBSCRIBE on the wire (streaming-subscriptions, task 4.1) ───────────────────────

TEST(CommandParserSubscribe, TheWholeLineIsHandedToTheQueryEngine) {
    // Not decomposed here: `QueryEngine::parse()` already accepts the full grammar, and
    // re-implementing a subset in the command parser would make two languages with one name.
    const std::string line =
        "SUBSCRIBE price FROM 'AAPL'.'NYSE' WHERE price BETWEEN 10000 AND 20000";
    const ob::Command parsed = ob::parse_command(line);
    EXPECT_EQ(parsed.type, ob::CommandType::SUBSCRIBE);
    EXPECT_EQ(parsed.subscribe_sql, line)
        << "the filter clause was dropped, so a client's WHERE would be silently ignored and they "
           "would receive every row for the symbol";
}

TEST(CommandParserSubscribe, UnsubscribeWithAnIdCarriesIt) {
    const ob::Command parsed = ob::parse_command("UNSUBSCRIBE 42");
    EXPECT_EQ(parsed.type, ob::CommandType::UNSUBSCRIBE);
    EXPECT_EQ(parsed.unsubscribe_id, 42u);
}

TEST(CommandParserSubscribe, UnsubscribeWithNoIdMeansEveryOneOfThisSession) {
    const ob::Command parsed = ob::parse_command("UNSUBSCRIBE");
    EXPECT_EQ(parsed.type, ob::CommandType::UNSUBSCRIBE);
    EXPECT_EQ(parsed.unsubscribe_id, 0u) << "zero is the sentinel for 'all of them'";
}

TEST(CommandParserSubscribe, AMalformedIdIsUnknownRatherThanCancelEverything) {
    // Widening a typo into "all of them" is the shape of the argument-parser defect in #36, where a
    // flag that did not parse started the server anyway. Here it would silently cancel a
    // subscription the client still wanted.
    for (const char* bad : {"UNSUBSCRIBE abc", "UNSUBSCRIBE 12x", "UNSUBSCRIBE -1"}) {
        const ob::Command parsed = ob::parse_command(bad);
        EXPECT_EQ(parsed.type, ob::CommandType::UNKNOWN) << bad;
        EXPECT_EQ(parsed.unsubscribe_id, 0u) << bad;
    }
}

TEST(CommandParserSubscribe, BothRoundTripThroughFormatCommand) {
    for (const std::string& line : {std::string("SUBSCRIBE * FROM 'AAPL'.'NYSE'"),
                                    std::string("UNSUBSCRIBE 7"),
                                    std::string("UNSUBSCRIBE")}) {
        const ob::Command parsed = ob::parse_command(line);
        ASSERT_NE(parsed.type, ob::CommandType::UNKNOWN) << line;
        EXPECT_EQ(ob::format_command(parsed), line + "\n") << line;
    }
}

TEST(ResponseFormatterPush, APushedRowIsTheSameSevenColumnsAsASelectRow) {
    ob::QueryResult row{};
    row.timestamp_ns    = 1'756'640'400'000'000'000ULL;
    row.sequence_number = 91823;
    row.price           = 7'845'812;
    row.quantity        = 1500;
    row.order_count     = 3;
    row.side            = 0;
    row.level           = 0;

    // Byte-exact, because a client parses this with one branch on the prefix and then the same code
    // it uses for SELECT. A column order that drifts from #65's is a client reading price as
    // quantity, with nothing failing.
    EXPECT_EQ(ob::format_push(4, row),
              "PUSH 4\t1756640400000000000\t7845812\t1500\t3\t0\t0\t91823\n");
}

// ── A bounded drain on shutdown (#106) ───────────────────────────────────────
//
// Measured before the bound existed, i3-7100U, Release: `SIGTERM` with nothing connected exits in
// **0.11 s**; with one **idle** client attached the process was **still running after 60 s**. Not a
// hang - the listener closes at once and the loop then waits for every session to end, exiting
// 0.00 s after the last client disconnects. An idle client never disconnects, and a long-lived
// client is the normal case for a database, so a supervisor reaches its own timeout and sends
// `SIGKILL`: the flush and checkpoint this path exists for are exactly what then does not run.

TEST(BoundedDrain, NothingConnectedStopsImmediatelyWhateverTheClock) {
    const auto started = std::chrono::steady_clock::now();
    // An hour past the deadline, and it does not matter: no sessions is the cleanest possible exit
    // and must not be reported as a deadline, or the log would claim sessions were cut.
    EXPECT_EQ(ob::drain_verdict(started, 0, 10'000, started + std::chrono::hours(1)),
              ob::DrainVerdict::AllSessionsClosed);
    EXPECT_EQ(ob::drain_verdict(started, 0, 0, started),
              ob::DrainVerdict::AllSessionsClosed);
}

TEST(BoundedDrain, AnOpenSessionWaitsUntilTheDeadlineAndThenIsCut) {
    const auto started = std::chrono::steady_clock::now();
    EXPECT_EQ(ob::drain_verdict(started, 1, 10'000, started + std::chrono::milliseconds(9'999)),
              ob::DrainVerdict::KeepWaiting);
    // The boundary is inclusive, because the alternative is a bound that never fires when the loop
    // happens to poll exactly on it.
    EXPECT_EQ(ob::drain_verdict(started, 1, 10'000, started + std::chrono::milliseconds(10'000)),
              ob::DrainVerdict::DeadlineReached);
    EXPECT_EQ(ob::drain_verdict(started, 3, 10'000, started + std::chrono::seconds(30)),
              ob::DrainVerdict::DeadlineReached);
}

TEST(BoundedDrain, ZeroMeansWaitForEverAndHasToBeAskedFor) {
    // The old behaviour, kept and made explicit. A default of 0 is what #106 is about, so this test
    // is also the statement that the *default* is not this: `ServerConfig` says 10 s.
    const auto started = std::chrono::steady_clock::now();
    EXPECT_EQ(ob::drain_verdict(started, 1, 0, started + std::chrono::hours(24)),
              ob::DrainVerdict::KeepWaiting);
    EXPECT_EQ(ob::ServerConfig{}.drain_timeout_ms, 10'000u)
        << "the default is what a supervisor meets, so it cannot be unbounded";
}

TEST(BoundedDrain, TheLoopDoesNotDecideForItself) {
    // The reason this is static: the io_uring transport asked the same question in **two** places
    // and the epoll loop in one, and no CI job built the io_uring file - so a bound written three
    // times could not even be compiled on one of the two sides. That transport is gone (#147); the
    // rule stays, because this repository has paid for "the fix exists and is used at one of two
    // sites" in #91, #101 and #102, and a second loop is exactly what the next stage adds.
    const auto read = [](const char* rel) {
        std::ifstream in(std::string(OB_SOURCE_DIR) + "/" + rel);
        return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    };

    for (const char* rel : {"src/tcp_server.cpp"}) {
        const std::string source = read(rel);
        ASSERT_FALSE(source.empty()) << rel << " could not be read, so this test checks nothing";

        // Positive half: the file consults the shared decision.
        EXPECT_NE(source.find("drain_verdict("), std::string::npos)
            << rel << " does not go through drain_verdict(), so its drain has no bound";

        // Negative half: nobody pairs the flag with the session count on their own again.
        std::size_t line_no = 0;
        for (std::size_t at = 0; at < source.size();) {
            const std::size_t end = source.find('\n', at);
            const std::string line = source.substr(at, (end == std::string::npos ? source.size()
                                                                                 : end) - at);
            at = (end == std::string::npos) ? source.size() : end + 1;
            ++line_no;
            if (line.find("//") != std::string::npos &&
                line.find("//") < line.find("draining_")) {
                continue;                      // prose about the rule is not the rule
            }
            const bool pairs = line.find("draining_") != std::string::npos &&
                               line.find("active_sessions") != std::string::npos;
            EXPECT_FALSE(pairs)
                << rel << ":" << line_no << " decides the drain itself: " << line
                << "\nThe bound lives in drain_verdict(); a second copy is a second thing to "
                   "forget, and one of the two transports is not built by any CI job";
        }
    }
}

// ── The io wait: eco and boost (#144) ─────────────────────────────────────────
//
// `boost` buys the kernel wake-up and nothing else. Measured on an m9g.xlarge over loopback, four
// interleaved rounds of 20,000 `PING` round trips: p50 7963 -> 6342 ns, p99 8276 -> 6720, and the
// **minimum unchanged** at ~5.9 µs - so what leaves is 1.6 µs of the 8.0 at both percentiles,
// which is what says wake-up rather than tail. Server CPU +59%.
//
// The decision is a pure function for the same reason `drain_verdict()` is: the alternative is a
// clock gate, and a test whose threshold is a duration fails on legitimate variation and teaches
// its reader to re-run it.

TEST(IoWait, ZeroSpinAlwaysBlocksAndThatIsTheDefault) {
    const auto now = std::chrono::steady_clock::now();
    // The shipped default, and byte for byte the behaviour before the mode existed: elapsed time
    // cannot matter, because there is no window to be inside.
    EXPECT_EQ(ob::io_wait_ms(now, 0, 100, now), 100);
    EXPECT_EQ(ob::io_wait_ms(now, 0, 100, now + std::chrono::hours(1)), 100);
    EXPECT_EQ(ob::io_wait_ms(now, 0, 100, now - std::chrono::hours(1)), 100);

    EXPECT_EQ(ob::ServerConfig{}.io_spin_us, 0u)
        << "spinning costs up to a core, so it has to be asked for";
    EXPECT_EQ(ob::ServerConfig{}.profile, "eco")
        << "the default profile is the one that changes nothing";
}

TEST(IoWait, InsideTheWindowThePollDoesNotBlock) {
    const auto last = std::chrono::steady_clock::now();
    EXPECT_EQ(ob::io_wait_ms(last, 50, 100, last), 0);
    EXPECT_EQ(ob::io_wait_ms(last, 50, 100, last + std::chrono::microseconds(49)), 0);
}

TEST(IoWait, TheWindowEndsAndTheLoopGoesBackToBlocking) {
    const auto last = std::chrono::steady_clock::now();
    // The boundary blocks: `since == spin_us` is the first instant the window is over, and the
    // alternative is a window one tick longer than the number an operator gave.
    EXPECT_EQ(ob::io_wait_ms(last, 50, 100, last + std::chrono::microseconds(50)), 100);
    EXPECT_EQ(ob::io_wait_ms(last, 50, 100, last + std::chrono::seconds(1)), 100);
    // This is what bounds the cost: an idle node spins for one window after its last event and
    // then stops, so the public cost is "up to one core while traffic flows" rather than a core.
    EXPECT_EQ(ob::io_wait_ms(last, ob::kBoostSpinUs, 100, last + std::chrono::seconds(30)), 100);
}

TEST(IoWait, AClockThatWentBackwardsPollsRatherThanSleeps) {
    // `steady_clock` is monotonic, so this is unreachable rather than expected - which is exactly
    // why it needs an answer written down: the signed subtraction would otherwise be cast to an
    // enormous unsigned value and read as "the window is long over", blocking a node that has just
    // had an event. Polling one pass costs a pass.
    const auto last = std::chrono::steady_clock::now();
    EXPECT_EQ(ob::io_wait_ms(last, 50, 100, last - std::chrono::microseconds(1)), 0);
}

TEST(IoWait, TheBoostWindowIsBoundedFromBothSidesRatherThanPinned) {
    // Deliberately not `EXPECT_EQ(kBoostSpinUs, 50)`: pinning the number fails on any legitimate
    // retuning and teaches a reader that this test is noise (the lesson #121 wrote down). What the
    // bounds say instead is that the two ends are different modes - zero would make `boost`
    // identical to `eco`, and a window past a millisecond is a node that mostly spins, which is a
    // different promise from the one `docs/cli.md` prints.
    EXPECT_GT(ob::kBoostSpinUs, 0u);
    EXPECT_LE(ob::kBoostSpinUs, 1000u);
}

TEST(IoWait, TheLoopTakesItsTimeoutFromTheDecisionRatherThanALiteral) {
    // Static for the same reason as `NeitherTransportDecidesForItself` above: replacing `wait_ms`
    // with `100` reinstates the blocking behaviour and leaves every test above green, because they
    // measure the function and not its use. A behavioural test for this would be a clock gate.
    const auto read = [](const char* rel) {
        std::ifstream in(std::string(OB_SOURCE_DIR) + "/" + rel);
        return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    };
    const std::string source = read("src/tcp_server.cpp");
    ASSERT_FALSE(source.empty()) << "cannot read src/tcp_server.cpp, so this test checks nothing";

    // Every epoll_wait in this file must take the computed wait. One call site today; the check is
    // over all of them so a second loop cannot quietly hard-code its own.
    std::size_t calls = 0;
    for (std::size_t at = source.find("epoll_wait("); at != std::string::npos;
         at = source.find("epoll_wait(", at + 1)) {
        // A mention in a comment is not a call. The reactor's own docstring says "a fatal
        // `epoll_wait()` error", and the first version of this loop failed on it - use against
        // mention again, in a check whose own comment above warns about exactly that.
        const std::size_t line_start = source.rfind('\n', at) + 1;
        if (source.substr(line_start, at - line_start).find("//") != std::string::npos) continue;
        ++calls;
        const std::size_t end = source.find(')', at);
        ASSERT_NE(end, std::string::npos);
        const std::string args = source.substr(at, end - at);
        EXPECT_NE(args.find("wait_ms"), std::string::npos)
            << "epoll_wait with a hard-coded timeout: " << args
            << "\nThe wait is a decision since the io-spin profile; a literal here is the mode "
               "silently switched off";
    }
    EXPECT_GT(calls, 0u) << "no epoll_wait found, so this test asserted nothing";

    // And `wait_ms` itself must come from the decision, at every assignment. The first version of
    // this test asked only whether `io_wait_ms(` appears in the file, and a mutation replacing the
    // computation with a literal **survived** it: the file still contains the function's own
    // definition, so the check was satisfied by a mention rather than by a use. Same shape as the
    // check that was satisfied by a neighbouring DEBUG line, and the assignment is the thing the
    // mode actually turns on. Idiom borrowed from `ReplicationIoBoundary` (#112), which learnt it
    // for the same reason.
    // The leading non-identifier character matters: this file also has `election_lease_wait_ms`
    // and the parser's own `wait_ms` keys, and a bare `wait_ms` matches the tail of each. Pitfall
    // 252's shape - `rds` inside `records` - met again in the check written to close a substring
    // hole, so the first run of this version reported three assignments and named two flags.
    const std::regex assign(R"(([^A-Za-z0-9_])wait_ms\s*=\s*([^;]+);)");
    std::size_t assignments = 0;
    for (auto it = std::sregex_iterator(source.begin(), source.end(), assign);
         it != std::sregex_iterator(); ++it) {
        ++assignments;
        const std::string rhs = (*it)[2].str();
        EXPECT_NE(rhs.find("io_wait_ms("), std::string::npos)
            << "wait_ms = " << rhs << ", not through io_wait_ms(). Two places deciding one timeout "
               "is how --io-spin-us comes to do nothing while every unit test stays green";
    }
    // One: the declaration inside the loop. A count rather than "at least one", because a rule
    // that stopped matching would report a clean tree.
    EXPECT_EQ(assignments, 1u)
        << "expected one assignment to wait_ms in src/tcp_server.cpp; found " << assignments;
}

// ── Event time on the wire (#105) ─────────────────────────────────────────────
//
// Measured before this existed, by #39 part two: a dataset's own span selected **0 of 400 rows**
// through this protocol while the same load into ClickHouse and TimescaleDB selected 400. The
// embedded path had honoured `timestamp_ns` since the first version, so the value was accepted by
// the client, dropped at the wire, and the row stored with its arrival time — which is a write
// nobody can find by the time they were looking for.

namespace {

/// A moment far enough from now that arrival time cannot be mistaken for it.
constexpr uint64_t kEventTime = 1700000000000000000ULL;   // 2023-11-14

/// Rows a symbol has inside a two-nanosecond window around `kEventTime`.
///
/// The window is that narrow deliberately: a wide one would pass for a row stamped on arrival, and
/// "the row is somewhere in the store" is the claim that was already true before this feature.
size_t rows_at_event_time(ob::Engine& engine, ob::Session& session, ob::ServerStats& stats,
                          const std::string& symbol) {
    ob::Command flush{};
    flush.type = ob::CommandType::FLUSH;
    ob::execute_command(flush, engine, session, stats);

    ob::Command select{};
    select.type    = ob::CommandType::SELECT;
    select.raw_sql = "SELECT * FROM '" + symbol + "'.'EX' WHERE timestamp BETWEEN " +
                     std::to_string(kEventTime - 1) + " AND " + std::to_string(kEventTime + 1);
    const std::string wire = ob::execute_command(select, engine, session, stats);

    // Data rows are the lines that begin with a timestamp; the header names columns and the
    // response ends with a blank line.
    size_t rows = 0;
    size_t pos = 0;
    while (pos < wire.size()) {
        const size_t nl = wire.find('\n', pos);
        const std::string line = wire.substr(pos, nl == std::string::npos ? nl : nl - pos);
        if (!line.empty() && std::isdigit(static_cast<unsigned char>(line[0]))) ++rows;
        if (nl == std::string::npos) break;
        pos = nl + 1;
    }
    return rows;
}

} // namespace

TEST_F(ExecuteCommandTest, AnInsertKeepsTheEventTimeItWasGiven) {
    ob::Session session(fd_server_);
    const auto cmd = ob::parse_command("INSERT EVT-GIVEN EX bid 100 5 1 " +
                                       std::to_string(kEventTime));
    ASSERT_EQ(cmd.type, ob::CommandType::INSERT) << cmd.error;
    ASSERT_TRUE(cmd.insert_args.timestamp_ns.has_value());
    EXPECT_EQ(*cmd.insert_args.timestamp_ns, kEventTime);

    EXPECT_EQ(ob::execute_command(cmd, *engine_, session, stats_).substr(0, 2), "OK");
    EXPECT_EQ(rows_at_event_time(*engine_, session, stats_, "EVT-GIVEN"), 1u)
        << "the row is not inside the span its sender named, so the time was discarded - which is "
        << "the defect #105 is about, and it answered OK while doing it";
}

TEST_F(ExecuteCommandTest, AnInsertWithoutOneIsStampedOnArrival) {
    // The control, and it is what makes the test above mean something: a server that stored every
    // row at `kEventTime` regardless would pass that one.
    ob::Session session(fd_server_);
    const auto cmd = ob::parse_command("INSERT EVT-ARRIVAL EX bid 100 5 1");
    ASSERT_EQ(cmd.type, ob::CommandType::INSERT) << cmd.error;
    EXPECT_FALSE(cmd.insert_args.timestamp_ns.has_value())
        << "an absent field became a value, so \"stamp it on arrival\" is no longer expressible";

    EXPECT_EQ(ob::execute_command(cmd, *engine_, session, stats_).substr(0, 2), "OK");
    EXPECT_EQ(rows_at_event_time(*engine_, session, stats_, "EVT-ARRIVAL"), 0u)
        << "a row nobody gave a time to landed in 2023";
}

TEST_F(ExecuteCommandTest, ABatchCarriesOneEventTimeForEveryLevel) {
    // One time per batch rather than one per level, because a batch is one book update at one
    // instant - which is what `DeltaUpdate` already models.
    ob::Session session(fd_server_);
    const auto cmd = ob::parse_minsert("MINSERT EVT-BATCH EX bid 3 " + std::to_string(kEventTime) +
                                       "\n100\t5\t1\n101\t6\t1\n102\t7\t1\n");
    ASSERT_EQ(cmd.type, ob::CommandType::MINSERT) << cmd.error;
    ASSERT_TRUE(cmd.minsert_args.timestamp_ns.has_value());

    EXPECT_EQ(ob::execute_command(cmd, *engine_, session, stats_).substr(0, 2), "OK");
    EXPECT_EQ(rows_at_event_time(*engine_, session, stats_, "EVT-BATCH"), 3u);
}

TEST_F(ExecuteCommandTest, AnEventTimeOfZeroIsRefusedRatherThanTreatedAsAbsent) {
    // Zero is how "unassigned" is spelled everywhere else here - `DeltaUpdate::sequence_number`
    // uses it for exactly that - so accepting it would make "I have no time for this row" and
    // "stamp it on arrival" the same request, in the one place where telling them apart is the
    // whole feature.
    const auto zero = ob::parse_command("INSERT EVT-ZERO EX bid 100 5 1 0");
    EXPECT_EQ(zero.type, ob::CommandType::UNKNOWN);
    EXPECT_NE(zero.error.find("event time 0"), std::string::npos) << zero.error;

    const auto garbage = ob::parse_command("INSERT EVT-BAD EX bid 100 5 1 yesterday");
    EXPECT_EQ(garbage.type, ob::CommandType::UNKNOWN);
    EXPECT_NE(garbage.error.find("'yesterday'"), std::string::npos)
        << "the refusal does not name the token: " << garbage.error;

    const auto trailing = ob::parse_command("INSERT EVT-EXTRA EX bid 100 5 1 " +
                                            std::to_string(kEventTime) + " extra");
    EXPECT_EQ(trailing.type, ob::CommandType::UNKNOWN);
    EXPECT_NE(trailing.error.find("'extra'"), std::string::npos) << trailing.error;
}

TEST_F(ExecuteCommandTest, TheEventTimeSurvivesARoundTripThroughFormatCommand) {
    // And an absent one stays absent: a formatter that filled the field in would turn "stamp it on
    // arrival" into a fixed instant the first time anything replayed a command.
    const auto with_time = ob::parse_command("INSERT EVT-RT EX bid 100 5 1 " +
                                             std::to_string(kEventTime));
    const auto again = ob::parse_command(ob::format_command(with_time));
    ASSERT_EQ(again.type, ob::CommandType::INSERT) << again.error;
    ASSERT_TRUE(again.insert_args.timestamp_ns.has_value());
    EXPECT_EQ(*again.insert_args.timestamp_ns, kEventTime);

    const auto without = ob::parse_command("INSERT EVT-RT2 EX bid 100 5 1");
    const auto without_again = ob::parse_command(ob::format_command(without));
    ASSERT_EQ(without_again.type, ob::CommandType::INSERT);
    EXPECT_FALSE(without_again.insert_args.timestamp_ns.has_value());
}

TEST_F(ExecuteCommandTest, StatusNamesWhatThisBuildCanDo) {
    // Sending the field is not a test for it: measured, a server without #107 answers `OK` to a
    // trailing token and stores the row without it. So the question has to be separate, and the
    // answer has to be in a place a client reads before its first write.
    ob::Session session(fd_server_);
    ob::Command cmd{};
    cmd.type = ob::CommandType::STATUS;
    const std::string wire = ob::execute_command(cmd, *engine_, session, stats_);

    ASSERT_NE(wire.find("capabilities: "), std::string::npos)
        << "STATUS says nothing about what this build can do:\n" << wire;
    for (const auto& name : ob::kCapabilities) {
        EXPECT_NE(wire.find(std::string(name)), std::string::npos)
            << "the list in capabilities.hpp names " << name << " and STATUS does not:\n" << wire;
    }
}

// ── One send per read, not one per response (#146) ───────────────────────────

namespace {
/// Where the loop over one read's commands begins in the read path - what comes before it is set-up.
std::size_t header_pos_of(const std::string& path) {
    const std::size_t at = path.find("for (const auto& line : lines)");
    return at == std::string::npos ? path.size() : at;
}
} // namespace

TEST(ReadLoopStatic, EveryCommandFromOneReadIsAnsweredWithOneSend) {
    // Static, because nothing behavioural can see it: the bytes a client reads are the same bytes in
    // the same order whether the loop sends once per read or once per response - which is exactly
    // what `tests/integration/test_pipelined_answers.py` holds - so a change back to one send per
    // response passes every test that reads the wire. What it costs is measured, not observed: 32.4%
    // of the io thread on an m9g.xlarge, one `send()` and one segment per response.
    const auto read = [](const char* rel) {
        std::ifstream in(std::string(OB_SOURCE_DIR) + "/" + rel);
        return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    };
    const std::string source = read("src/tcp_server.cpp");
    ASSERT_FALSE(source.empty()) << "cannot read src/tcp_server.cpp, so this test checks nothing";

    // The read path runs from the line the session hands back to the label the loop jumps to.
    const std::size_t from = source.find("auto lines = session->feed(buf, got);");
    ASSERT_NE(from, std::string::npos) << "the read path moved; this test would check nothing";
    const std::size_t to = source.find("next_event:;", from);
    ASSERT_NE(to, std::string::npos);
    const std::string path = source.substr(from, to - from);

    // The loop over the commands of this read, found by its header and closed by brace matching
    // rather than by the next `}` - its body has braces of its own.
    const std::size_t header = path.find("for (const auto& line : lines)");
    ASSERT_NE(header, std::string::npos) << "no loop over the commands of a read";
    const std::size_t open = path.find('{', header);
    ASSERT_NE(open, std::string::npos);
    std::size_t close = open;
    int depth = 0;
    for (std::size_t i = open; i < path.size(); ++i) {
        if (path[i] == '{') ++depth;
        if (path[i] == '}' && --depth == 0) { close = i; break; }
    }
    ASSERT_GT(close, open) << "unbalanced braces in the read loop";
    const std::string body = path.substr(open, close - open);
    const std::string after = path.substr(close);

    // `queue_answer`, not `queue_response`: since #152 the loop queues through the call that tells
    // an answer too large to ever send from a client that stopped reading, and the one
    // `queue_response` left in the loop is the error that replaces such an answer - which would
    // satisfy a search for `queue_response(` while the path every other answer takes was gone.
    //
    // Since #155 that call is in one lambda, `enqueue`, which the loop and the held writes both
    // answer through - so the lambda is read too, and held to the same rule as the loop: a send or
    // a flush inside it is a send per response, whoever calls it.
    const auto lambda_body = [&](const std::string& header) {
        const std::size_t at = path.find(header);
        if (at == std::string::npos || at > header_pos_of(path)) return std::string();
        const std::size_t lopen = path.find('{', at);
        int d = 0;
        for (std::size_t i = lopen; i < path.size(); ++i) {
            if (path[i] == '{') ++d;
            if (path[i] == '}' && --d == 0) return path.substr(lopen, i - lopen);
        }
        return std::string();
    };
    const std::string enqueue = lambda_body("const auto enqueue = [&](const std::string& response)");
    ASSERT_FALSE(enqueue.empty()) << "the read path no longer queues its answers through enqueue()";
    EXPECT_NE(enqueue.find("= session->queue_answer(response);"), std::string::npos)
        << "the commands of a read are no longer answered into the session's buffer";
    EXPECT_NE(body.find("enqueue(response)"), std::string::npos)
        << "the loop over the commands of a read does not queue their answers through enqueue()";
    const std::string held = lambda_body("const auto apply_held_writes = [&]()");
    ASSERT_FALSE(held.empty()) << "the read path no longer applies the writes it held (#155)";
    EXPECT_NE(held.find("enqueue(answer)"), std::string::npos)
        << "the answers to the held writes are not queued through enqueue()";
    for (const std::string* part : {&body, &enqueue, &held}) {
        EXPECT_EQ(part->find("send_response("), std::string::npos)
            << "a response is sent from inside the handling of one read's commands: one send per "
               "response again, which is what #146 measured at 32.4% of the io thread";
        EXPECT_EQ(part->find("flush_output("), std::string::npos)
            << "the session is flushed from inside the handling of one read's commands";
    }

    // And exactly one flush of what the loop queued. A count rather than "at least one", because a
    // second flush after the loop is a second send per read and a rule that stopped matching would
    // report a clean tree.
    std::size_t flushes = 0;
    for (std::size_t at = after.find("flush_output("); at != std::string::npos;
         at = after.find("flush_output(", at + 1)) {
        ++flushes;
    }
    EXPECT_EQ(flushes, 1u) << "expected one flush of the queued answers after the loop, found "
                           << flushes;
}

TEST(ReadLoopStatic, TheWritesOfAReadAreHeldAndAnsweredBeforeAnythingElse) {
    // What makes a batch of writes invisible to a client (#155): every write the gate lets through
    // is held rather than executed, the held writes are applied and answered before any other
    // command is executed or refused, and whatever is still held when the read ends is applied
    // then. The behavioural half is `test_pipelined_answers.py` - the same bytes pipelined as one
    // command at a time - but that passes a loop that never holds anything, which is the shape
    // this stage exists to replace: 64 acquisitions of the engine's lock per read.
    const auto read = [](const char* rel) {
        std::ifstream in(std::string(OB_SOURCE_DIR) + "/" + rel);
        return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    };
    const std::string source = read("src/tcp_server.cpp");
    ASSERT_FALSE(source.empty()) << "cannot read src/tcp_server.cpp, so this test checks nothing";
    const std::size_t from = source.find("auto lines = session->feed(buf, got);");
    ASSERT_NE(from, std::string::npos) << "the read path moved; this test would check nothing";
    const std::size_t to = source.find("next_event:;", from);
    ASSERT_NE(to, std::string::npos);
    const std::string path = source.substr(from, to - from);

    const std::size_t header = path.find("for (const auto& line : lines)");
    ASSERT_NE(header, std::string::npos);
    const std::size_t open = path.find('{', header);
    std::size_t close = open;
    int depth = 0;
    for (std::size_t i = open; i < path.size(); ++i) {
        if (path[i] == '{') ++depth;
        if (path[i] == '}' && --depth == 0) { close = i; break; }
    }
    ASSERT_GT(close, open);
    const std::string body = path.substr(open, close - open);

    // Emptied at the start of the read - before the function that applies them, whose own `clear()`
    // runs only after an `execute_writes()` that returned. What the first one is for is the path
    // where that did not return: an exception out of it leaves the writes held, and the reactor's
    // next read may be another session's. Mutation row 14 of #155 removed the first and this rule
    // found the second - inside the lambda - and passed, so it now asks for one ahead of the lambda.
    const std::size_t lambda = path.find("const auto apply_held_writes");
    ASSERT_NE(lambda, std::string::npos);
    const std::size_t cleared = path.find("pending_writes_.clear();");
    ASSERT_NE(cleared, std::string::npos) << "the held writes are not emptied at the start of a read";
    EXPECT_LT(cleared, lambda)
        << "the held writes are emptied only inside the function that applies them, which an "
           "exception out of execute_writes() skips";
    EXPECT_LT(cleared, header);

    // Held, and nothing else done with it.
    const std::size_t held = body.find("if (deferrable_write(cmd, *session, secrets_.client_store()))");
    ASSERT_NE(held, std::string::npos) << "the loop does not hold the writes of a read";
    const std::size_t pushed = body.find("pending_writes_.push_back(std::move(cmd));", held);
    ASSERT_NE(pushed, std::string::npos);
    EXPECT_NE(body.find("continue;", pushed), std::string::npos);

    // Applied before a command that is not a write is executed - and before the one refusal the
    // loop makes itself, a line too long.
    const std::size_t executed = body.find("execute_command(");
    ASSERT_NE(executed, std::string::npos);
    const std::size_t applied_before = body.rfind("apply_held_writes()", executed);
    ASSERT_NE(applied_before, std::string::npos)
        << "a command is executed with the writes before it still held, so its answer overtakes "
           "theirs and it does not see them";
    EXPECT_GT(applied_before, pushed);
    const std::size_t too_long = body.find("format_error(\"line too long\")");
    ASSERT_NE(too_long, std::string::npos);
    EXPECT_NE(body.rfind("apply_held_writes()", too_long), std::string::npos)
        << "a line too long is refused ahead of the writes before it";

    // And applied when the read ends, before the one flush that sends it all.
    const std::string after = path.substr(close);
    const std::size_t last = after.find("apply_held_writes()");
    ASSERT_NE(last, std::string::npos) << "writes still held when the read ends are never applied";
    EXPECT_LT(last, after.find("flush_output("));
}

TEST(ReactorStatic, EveryClientEventIsServedInsideTheBoundary) {
    // The generic loop scan in test_thread_boundaries.cpp accepts any `try {` after the loop
    // statement, and this loop holds one that has nothing to do with surviving an iteration: the
    // TLS handshake started at accept. So the scan alone passes with the boundary deleted, and this
    // test asks the specific question - is the handling of each event inside a `try` whose `catch`
    // counts the failure and closes the session it happened on?
    const auto read = [](const char* rel) {
        std::ifstream in(std::string(OB_SOURCE_DIR) + "/" + rel);
        return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    };
    const std::string source = read("src/tcp_server.cpp");
    ASSERT_FALSE(source.empty()) << "cannot read src/tcp_server.cpp, so this test checks nothing";

    const std::size_t fn = source.find("\nvoid Reactor::run_loop() {\n");
    ASSERT_NE(fn, std::string::npos) << "Reactor::run_loop() moved; this test would check nothing";
    const std::size_t fn_end = source.find("\n}\n", fn);
    ASSERT_NE(fn_end, std::string::npos);
    const std::string loop = source.substr(fn, fn_end - fn);

    // The loop over one pass's events, and the first statements of its body.
    const std::size_t header = loop.find("for (int i = 0; i < nfds; ++i) {");
    ASSERT_NE(header, std::string::npos) << "no loop over the events of a pass";
    const std::size_t open = loop.find('{', header);
    std::size_t close = open;
    int depth = 0;
    for (std::size_t i = open; i < loop.size(); ++i) {
        if (loop[i] == '{') ++depth;
        if (loop[i] == '}' && --depth == 0) { close = i; break; }
    }
    ASSERT_GT(close, open) << "unbalanced braces in the event loop";
    const std::string body = loop.substr(open + 1, close - open - 1);

    const std::size_t fd_line = body.find("int fd = events_[i].data.fd;");
    ASSERT_NE(fd_line, std::string::npos);
    const std::size_t try_at =
        body.find_first_not_of(" \n", fd_line + std::strlen("int fd = events_[i].data.fd;"));
    ASSERT_NE(try_at, std::string::npos);
    EXPECT_EQ(body.compare(try_at, 5, "try {"), 0)
        << "the handling of an event does not begin with `try {`, so an exception serving one client "
           "leaves the reactor and every other client on it";

    // The catch that closes that try, at the event loop's own depth.
    const std::size_t catch_at = body.rfind("} catch (const std::exception& e) {");
    ASSERT_NE(catch_at, std::string::npos) << "no catch at the end of the event handling";
    const std::string handler = body.substr(catch_at);
    EXPECT_NE(handler.find("event_guard_.caught(e)"), std::string::npos)
        << "the boundary does not count the failure, so an operator has nothing to alarm on";
    EXPECT_NE(handler.find("close_session(fd,"), std::string::npos)
        << "the boundary does not close the session whose event threw, so it is served again in "
           "a state nothing can vouch for";

    // And the recovery line waits for a pass in which nothing threw (pitfall 291).
    const std::string after = loop.substr(close);
    EXPECT_NE(after.find("if (nfds > 0 && !threw_this_pass) event_guard_.ok();"), std::string::npos)
        << "the recovery line is not gated on a pass that had events and none of which threw";
}

namespace {

/// Every `remove_session(X)` in `source` followed, in the rest of its block, by `::close(X)`: the
/// statements after the call up to the first `return`, `continue;` or `break;`, or the `}` that
/// closes the block the call is in. Each entry is the call and the close, for the message.
std::vector<std::string> closes_after_remove(const std::string& source) {
    std::vector<std::string> found;
    const std::string call = "remove_session(";
    for (std::size_t at = source.find(call); at != std::string::npos;
         at = source.find(call, at + 1)) {
        const std::size_t open = at + call.size();
        const std::size_t shut = source.find(')', open);
        if (shut == std::string::npos) break;
        const std::string arg = source.substr(open, shut - open);
        // The declaration and the definition name a parameter type; a call names a descriptor.
        if (arg.empty() || arg.find(' ') != std::string::npos) continue;
        int depth = 0;
        std::size_t end = shut;
        for (; end < source.size(); ++end) {
            const char c = source[end];
            if (c == '{') ++depth;
            if (c == '}' && --depth < 0) break;
            if (depth == 0 && (source.compare(end, 6, "return") == 0 ||
                               source.compare(end, 9, "continue;") == 0 ||
                               source.compare(end, 6, "break;") == 0)) {
                break;
            }
        }
        const std::string tail = source.substr(shut, end - shut);
        if (tail.find("::close(" + arg + ")") != std::string::npos) {
            found.push_back("remove_session(" + arg + ") then ::close(" + arg + ")");
        }
    }
    return found;
}

}  // namespace

TEST(SessionsStatic, NoDescriptorIsClosedAgainAfterItsSessionIsRemoved) {
    // `SessionManager::remove_session()` closes the descriptor. The accept path's TLS failure
    // branch removed the session and then closed the number itself (#150): a second close of a
    // number the kernel is free to have handed, in between, to a file another thread opened -
    // #128's class, which this server's WAL, flush and replication threads make more than
    // theoretical. The branch moved into `Reactor::adopt()` with the multi-reactor stage and closes
    // once now; this holds every other place that removes a session to the same rule.

    // The rule's own cases: the shape #150 was, and the shape that is right.
    EXPECT_EQ(closes_after_remove("    sessions_.remove_session(fd);\n"
                                  "    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);\n"
                                  "    ::close(fd);\n"
                                  "    continue;\n")
                  .size(),
              1u);
    EXPECT_TRUE(closes_after_remove("    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);\n"
                                    "    sessions_.remove_session(fd);\n"
                                    "    return;\n"
                                    "}\n"
                                    "void other(int fd) { ::close(fd); }\n")
                    .empty())
        << "a close in a different function is not a close after this removal";

    std::ifstream in(std::string(OB_SOURCE_DIR) + "/src/tcp_server.cpp");
    const std::string source((std::istreambuf_iterator<char>(in)),
                             std::istreambuf_iterator<char>());
    ASSERT_NE(source.find("remove_session("), std::string::npos)
        << "src/tcp_server.cpp removes no session, so this rule reads nothing";
    for (const std::string& hit : closes_after_remove(source)) {
        ADD_FAILURE() << "src/tcp_server.cpp: " << hit
                      << " - remove_session() has already closed it, and the number may belong to "
                         "someone else by now";
    }
}

// ── Commands that run alone across reactors ────────────────────────────────────

TEST(AdminSerialisation, ExactlyFailoverAndMigrateRunAlone) {
    // Iterating the enumeration rather than a list, like the authentication gate: a new command
    // classified as serialised fails here, and a new command classified at all is forced by
    // -Wswitch in the classifier.
    std::vector<int> alone;
    for (int i = 0; i <= static_cast<int>(ob::CommandType::UNKNOWN); ++i) {
        if (ob::serialised_across_reactors(static_cast<ob::CommandType>(i))) alone.push_back(i);
    }
    EXPECT_EQ(alone, (std::vector<int>{static_cast<int>(ob::CommandType::FAILOVER),
                                       static_cast<int>(ob::CommandType::MIGRATE)}))
        << "FAILOVER and MIGRATE were written for one caller at a time; anything added to that set "
           "is a decision, and so is anything taken out of it";
}

TEST(AdminSerialisation, TheClassifierHasNoDefaultAndTheLoopConsultsIt) {
    std::ifstream in(std::string(OB_SOURCE_DIR) + "/src/tcp_server.cpp");
    ASSERT_TRUE(in) << "cannot read src/tcp_server.cpp";
    const std::string src((std::istreambuf_iterator<char>(in)),
                          std::istreambuf_iterator<char>());

    const auto begin = src.find("bool serialised_across_reactors(CommandType t) {");
    ASSERT_NE(begin, std::string::npos) << "classifier not found - did it get renamed?";
    const auto end = src.find("\n}\n", begin);
    ASSERT_NE(end, std::string::npos);
    EXPECT_EQ(src.substr(begin, end - begin).find("default:"), std::string::npos)
        << "a default label turns off the exhaustiveness check this classifier relies on";

    // A classifier nothing consults is a comment. The reactor's dispatch must take the shared
    // mutex for what it names, before the command runs.
    const auto loop = src.find("\nvoid Reactor::run_loop() {\n");
    ASSERT_NE(loop, std::string::npos);
    const auto call = src.find("= execute_command(", loop);
    ASSERT_NE(call, std::string::npos) << "the reactor does not dispatch commands any more?";
    const std::string before = src.substr(loop, call - loop);
    const auto consult = before.rfind("if (serialised_across_reactors(cmd.type)) alone.lock();");
    ASSERT_NE(consult, std::string::npos)
        << "the reactor runs every command without asking whether it must run alone";
    EXPECT_EQ(before.find('\n', before.find('\n', consult) + 1), std::string::npos)
        << "the lock is taken more than a line before the command it guards";
}

// ── An answer too large to ever send (#152) ────────────────────────────────────

TEST(AnswerCeiling, AnAnswerLargerThanTheCapIsRefusedAloneAndNothingIsQueued) {
    int sv[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    {
        ob::Session s(sv[0], 1);
        const std::string too_large(ob::Session::max_queued_bytes() + 1, 'x');
        EXPECT_EQ(s.queue_answer(too_large), ob::Session::Queued::TooLargeAlone);
        EXPECT_EQ(s.pending_output_bytes(), 0u) << "a refused answer left bytes queued";
        // The control: the largest answer that fits is queued, because the refusal is about one
        // answer being larger than the cap, not about being large.
        const std::string at_cap(ob::Session::max_queued_bytes(), 'y');
        EXPECT_EQ(s.queue_answer(at_cap), ob::Session::Queued::Yes);
        EXPECT_EQ(s.pending_output_bytes(), ob::Session::max_queued_bytes());
        // And the other refusal is still the other one: one byte more behind a full buffer is a
        // client that is not reading.
        EXPECT_EQ(s.queue_answer("z"), ob::Session::Queued::CapExceeded);
    }
    // A Session does not own its descriptor - SessionManager::remove_session() closes it.
    ::close(sv[0]);
    ::close(sv[1]);
}

TEST(AnswerCeiling, TheLoopAnswersAnErrorInsteadOfClosing) {
    // Static, because the behavioural half needs an answer above 64 MB, which is a store of about
    // two and a half million rows - measured on the m9g.xlarge at 76 MB for the query that found
    // this. The unit test above holds the classification; this holds that the loop acts on it.
    std::ifstream in(std::string(OB_SOURCE_DIR) + "/src/tcp_server.cpp");
    const std::string src((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    ASSERT_FALSE(src.empty());
    const auto at = src.find("if (queued == Session::Queued::TooLargeAlone) {");
    ASSERT_NE(at, std::string::npos) << "the read loop does not tell an answer too large to send "
                                        "from a client that stopped reading";
    // The branch, by brace matching. Since #155 it is the first half of `enqueue`, which the loop
    // and the held writes both answer through, and it returns rather than running into an
    // `else if` - so a slice to the next `} else if` would now run to the end of the file.
    const auto open = src.find('{', at);
    ASSERT_NE(open, std::string::npos);
    std::size_t close = open;
    int depth = 0;
    for (std::size_t i = open; i < src.size(); ++i) {
        if (src[i] == '{') ++depth;
        if (src[i] == '}' && --depth == 0) { close = i; break; }
    }
    ASSERT_GT(close, open) << "unbalanced braces in the too-large branch";
    const std::string branch = src.substr(at, close - at + 1);
    EXPECT_NE(branch.find("format_error("), std::string::npos)
        << "an answer too large to send is not replaced by an error";
    EXPECT_EQ(branch.find("close_session("), std::string::npos)
        << "an answer too large to send closes the session of a client that did nothing wrong";
    // The error is what gets queued, and the session ends only if even the error cannot be. The
    // first version of this rule stopped at the two lines above, and a branch that built the error,
    // dropped it and set `queue_refused` satisfied both - the session closed exactly as before
    // (#151's mutation row 4). Now the branch answers with whether the error fit, and its caller
    // closes the session on no - so it may return that and nothing else.
    EXPECT_NE(branch.find("return session->queue_response(refusal);"), std::string::npos)
        << "the error replacing the answer is not queued";
    EXPECT_EQ(branch.find("return false;"), std::string::npos)
        << "the branch gives up on the session before trying to queue the error";
    EXPECT_EQ(branch.find("queue_refused"), std::string::npos)
        << "the branch decides the session's end itself rather than by whether the error fit";
}
