// Feature: tcp-server — Property-based + Unit tests for TCP server components
// Tests cover: CommandParser, ResponseFormatter, Session, SessionManager,
//              execute_command, and CLI argument parsing.

#include "orderbook/command_parser.hpp"
#include "orderbook/response_formatter.hpp"
#include "orderbook/session.hpp"
#include "orderbook/tcp_server.hpp"
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
        // Simple commands
        gen::just(ob::Command{ob::CommandType::FLUSH, {}, {}, {}}),
        gen::just(ob::Command{ob::CommandType::PING, {}, {}, {}}),
        gen::just(ob::Command{ob::CommandType::STATUS, {}, {}, {}}),
        gen::just(ob::Command{ob::CommandType::ROLE, {}, {}, {}}),
        gen::just(ob::Command{ob::CommandType::QUIT, {}, {}, {}})
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

        std::string wire = ob::format_query_response(rows);
        ob::ParsedResponse parsed = ob::parse_response(wire);

        RC_ASSERT(!parsed.is_error);
        // Header should have 6 columns
        RC_ASSERT(parsed.header_columns.size() == 6u);
        // Row count should match
        RC_ASSERT(parsed.rows.size() == rows.size());

        // Verify each row's values match
        for (size_t i = 0; i < rows.size(); ++i) {
            RC_ASSERT(parsed.rows[i].size() == size_t(6));
            RC_ASSERT(parsed.rows[i][0] == std::to_string(rows[i].timestamp_ns));
            RC_ASSERT(parsed.rows[i][1] == std::to_string(rows[i].price));
            RC_ASSERT(parsed.rows[i][2] == std::to_string(rows[i].quantity));
            RC_ASSERT(parsed.rows[i][3] == std::to_string(rows[i].order_count));
            RC_ASSERT(parsed.rows[i][4] == std::to_string(rows[i].side));
            RC_ASSERT(parsed.rows[i][5] == std::to_string(rows[i].level));
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

    std::string wire = ob::format_query_response(rows);

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
    EXPECT_EQ(config.worker_threads, 4);
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
