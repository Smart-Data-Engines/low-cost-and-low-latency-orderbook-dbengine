// Tests for query session compression negotiation (compression-and-ttl, Task 4.4)
// Tests cover: COMPRESS LZ4 command parsing, execute_command negotiation,
//              mid-stream rejection, and default uncompressed operation.

#include "orderbook/command_parser.hpp"
#include "orderbook/compression.hpp"
#include "orderbook/response_formatter.hpp"
#include "orderbook/session.hpp"
#include "orderbook/tcp_server.hpp"
#include "orderbook/engine.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <string>

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
// Test fixture with Engine
// ═══════════════════════════════════════════════════════════════════════════════

class SessionCompressTest : public ::testing::Test {
protected:
    std::string temp_dir_;
    std::unique_ptr<ob::Engine> engine_;
    ob::ServerStats stats_;
    int fd_server_ = -1;
    int fd_client_ = -1;

    void SetUp() override {
        temp_dir_ = make_temp_dir("session_compress_test_");
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

// ═══════════════════════════════════════════════════════════════════════════════
// Command parsing tests
// ═══════════════════════════════════════════════════════════════════════════════

TEST(CompressCommandParsing, ParseCompressLZ4) {
    ob::Command cmd = ob::parse_command("COMPRESS LZ4");
    EXPECT_EQ(cmd.type, ob::CommandType::COMPRESS);
}

TEST(CompressCommandParsing, ParseCompressLZ4CaseInsensitive) {
    ob::Command cmd = ob::parse_command("compress lz4");
    EXPECT_EQ(cmd.type, ob::CommandType::COMPRESS);
}

TEST(CompressCommandParsing, ParseCompressWithoutAlgo) {
    ob::Command cmd = ob::parse_command("COMPRESS");
    EXPECT_EQ(cmd.type, ob::CommandType::UNKNOWN);
}

TEST(CompressCommandParsing, ParseCompressUnsupportedAlgo) {
    ob::Command cmd = ob::parse_command("COMPRESS ZSTD");
    EXPECT_EQ(cmd.type, ob::CommandType::UNKNOWN);
}

TEST(CompressCommandParsing, FormatCompressCommand) {
    ob::Command cmd{};
    cmd.type = ob::CommandType::COMPRESS;
    std::string wire = ob::format_command(cmd);
    EXPECT_EQ(wire, "COMPRESS LZ4\n");
}

// ═══════════════════════════════════════════════════════════════════════════════
// SessionCompressNegotiation: send COMPRESS LZ4, verify OK COMPRESS LZ4
// Validates: Requirement 3.1
// ═══════════════════════════════════════════════════════════════════════════════

TEST_F(SessionCompressTest, SessionCompressNegotiation) {
    ob::Session session(fd_server_);

    ob::Command cmd = ob::parse_command("COMPRESS LZ4");
    ASSERT_EQ(cmd.type, ob::CommandType::COMPRESS);

    std::string response = ob::execute_command(cmd, *engine_, session, stats_);
    EXPECT_EQ(response, "OK COMPRESS LZ4\n\n");
    // execute_command no longer sets compressed — the caller (epoll/io_uring loop) does it
    // after sending the plain-text response. Simulate that here:
    session.set_compressed(true);
    EXPECT_TRUE(session.is_compressed());
}

// ═══════════════════════════════════════════════════════════════════════════════
// SessionCompressDefault: connect without compression, verify uncompressed
// Validates: Requirement 3.4
// ═══════════════════════════════════════════════════════════════════════════════

TEST_F(SessionCompressTest, SessionCompressDefault) {
    ob::Session session(fd_server_);

    // Session should default to uncompressed
    EXPECT_FALSE(session.is_compressed());

    // Execute a PING — should work normally
    ob::Command cmd{};
    cmd.type = ob::CommandType::PING;
    std::string response = ob::execute_command(cmd, *engine_, session, stats_);
    EXPECT_EQ(response, "PONG\n");

    // Still not compressed
    EXPECT_FALSE(session.is_compressed());
}

// ═══════════════════════════════════════════════════════════════════════════════
// SessionCompressMidStream: send COMPRESS LZ4 after another command, verify error
// Validates: Requirement 3.1 (must be first command)
// ═══════════════════════════════════════════════════════════════════════════════

TEST_F(SessionCompressTest, SessionCompressMidStream) {
    ob::Session session(fd_server_);

    // Execute a PING first
    ob::Command ping_cmd{};
    ping_cmd.type = ob::CommandType::PING;
    std::string ping_response = ob::execute_command(ping_cmd, *engine_, session, stats_);
    EXPECT_EQ(ping_response, "PONG\n");

    // Now try COMPRESS LZ4 — should fail
    ob::Command compress_cmd = ob::parse_command("COMPRESS LZ4");
    ASSERT_EQ(compress_cmd.type, ob::CommandType::COMPRESS);

    std::string response = ob::execute_command(compress_cmd, *engine_, session, stats_);
    EXPECT_EQ(response, "ERR compress_must_be_first\n");
    EXPECT_FALSE(session.is_compressed());
}

// ═══════════════════════════════════════════════════════════════════════════════
// SessionCompressMidStreamAfterInsert: COMPRESS after INSERT also fails
// ═══════════════════════════════════════════════════════════════════════════════

TEST_F(SessionCompressTest, SessionCompressMidStreamAfterInsert) {
    ob::Session session(fd_server_);

    // Execute an INSERT first
    ob::Command insert_cmd{};
    insert_cmd.type = ob::CommandType::INSERT;
    insert_cmd.insert_args.symbol   = "BTC-USD";
    insert_cmd.insert_args.exchange = "BINANCE";
    insert_cmd.insert_args.side     = 0;
    insert_cmd.insert_args.price    = 6500000;
    insert_cmd.insert_args.qty      = 1500;
    insert_cmd.insert_args.count    = 1;

    ob::execute_command(insert_cmd, *engine_, session, stats_);

    // Now try COMPRESS LZ4 — should fail
    ob::Command compress_cmd = ob::parse_command("COMPRESS LZ4");
    std::string response = ob::execute_command(compress_cmd, *engine_, session, stats_);
    EXPECT_EQ(response, "ERR compress_must_be_first\n");
    EXPECT_FALSE(session.is_compressed());
}

// ═══════════════════════════════════════════════════════════════════════════════
// SessionCommandCounterTracking: verify command counter increments correctly
// ═══════════════════════════════════════════════════════════════════════════════

TEST_F(SessionCompressTest, SessionCommandCounterTracking) {
    ob::Session session(fd_server_);

    EXPECT_EQ(session.commands_executed(), 0u);

    // PING increments counter
    ob::Command ping_cmd{};
    ping_cmd.type = ob::CommandType::PING;
    ob::execute_command(ping_cmd, *engine_, session, stats_);
    EXPECT_EQ(session.commands_executed(), 1u);

    // STATUS increments counter
    ob::Command status_cmd{};
    status_cmd.type = ob::CommandType::STATUS;
    ob::execute_command(status_cmd, *engine_, session, stats_);
    EXPECT_EQ(session.commands_executed(), 2u);
}

// ═══════════════════════════════════════════════════════════════════════════════
// CompressMetricsStatus: verify STATUS includes compress_bytes_in/out fields
// Validates: Requirement 4.1
// ═══════════════════════════════════════════════════════════════════════════════

TEST_F(SessionCompressTest, CompressMetricsStatus) {
    ob::Session session(fd_server_);

    // Negotiate compression
    ob::Command compress_cmd = ob::parse_command("COMPRESS LZ4");
    std::string compress_resp = ob::execute_command(compress_cmd, *engine_, session, stats_);
    EXPECT_EQ(compress_resp, "OK COMPRESS LZ4\n\n");
    // execute_command no longer sets compressed — simulate the epoll loop behavior:
    session.set_compressed(true);
    EXPECT_TRUE(session.is_compressed());

    // Send a compressed command through feed() to generate metrics
    // Build a compressed PING command
    std::string ping_text = "PING\n";
    auto compressed_ping = ob::lz4_compress(ping_text.data(), ping_text.size());
    uint32_t frame_len = static_cast<uint32_t>(compressed_ping.size());
    std::string framed;
    framed.push_back(static_cast<char>((frame_len >> 24) & 0xFF));
    framed.push_back(static_cast<char>((frame_len >> 16) & 0xFF));
    framed.push_back(static_cast<char>((frame_len >> 8) & 0xFF));
    framed.push_back(static_cast<char>(frame_len & 0xFF));
    framed.append(reinterpret_cast<const char*>(compressed_ping.data()),
                  compressed_ping.size());

    auto lines = session.feed(framed.data(), framed.size());
    ASSERT_EQ(lines.size(), 1u);
    EXPECT_EQ(lines[0], "PING");

    // Also send a compressed response to generate outgoing metrics
    session.send_response("PONG\n");

    // Verify per-session compression counters are non-zero
    EXPECT_GT(session.compress_bytes_in(), 0u);
    EXPECT_GT(session.compress_bytes_out(), 0u);

    // Now execute STATUS and verify the response includes compression metrics
    ob::Command status_cmd{};
    status_cmd.type = ob::CommandType::STATUS;
    std::string status_resp = ob::execute_command(status_cmd, *engine_, session, stats_);

    EXPECT_NE(status_resp.find("compress_bytes_in:"), std::string::npos)
        << "STATUS response should contain compress_bytes_in field";
    EXPECT_NE(status_resp.find("compress_bytes_out:"), std::string::npos)
        << "STATUS response should contain compress_bytes_out field";

    // Verify the values are non-zero in the output
    EXPECT_EQ(status_resp.find("compress_bytes_in: 0"), std::string::npos)
        << "compress_bytes_in should be non-zero after compressed data transfer";
    EXPECT_EQ(status_resp.find("compress_bytes_out: 0"), std::string::npos)
        << "compress_bytes_out should be non-zero after compressed data transfer";
}

// ═══════════════════════════════════════════════════════════════════════════════
// CompressMetricsZeroWhenUncompressed: verify no compression metrics when not compressed
// ═══════════════════════════════════════════════════════════════════════════════

TEST_F(SessionCompressTest, CompressMetricsZeroWhenUncompressed) {
    ob::Session session(fd_server_);

    // Without compression, metrics should be zero
    EXPECT_EQ(session.compress_bytes_in(), 0u);
    EXPECT_EQ(session.compress_bytes_out(), 0u);

    // Execute STATUS — should NOT contain compression metrics lines
    ob::Command status_cmd{};
    status_cmd.type = ob::CommandType::STATUS;
    std::string status_resp = ob::execute_command(status_cmd, *engine_, session, stats_);

    // When both are 0, format_status omits the compression section
    EXPECT_EQ(status_resp.find("compress_bytes_in:"), std::string::npos)
        << "STATUS should not contain compress_bytes_in when no compression is active";
}
