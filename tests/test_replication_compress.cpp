// Tests for replication compression handshake (Task 3.5)
// Feature: compression-and-ttl
// Requirements: 2.2, 2.4

#include <gtest/gtest.h>

#include "orderbook/replication.hpp"
#include "orderbook/wal.hpp"

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

static std::atomic<uint64_t> dir_counter{0};

static std::filesystem::path make_temp_dir(const std::string& suffix) {
    auto base = std::filesystem::temp_directory_path() /
                ("ob_repl_compress_" + suffix + "_" +
                 std::to_string(dir_counter.fetch_add(1, std::memory_order_relaxed)));
    std::filesystem::create_directories(base);
    return base;
}

struct TempDir {
    std::filesystem::path path;
    explicit TempDir(const std::string& suffix = "")
        : path(make_temp_dir(suffix)) {}
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
    std::string str() const { return path.string(); }
};

static std::atomic<uint16_t> next_port{21876};

static uint16_t alloc_port() {
    return next_port.fetch_add(1, std::memory_order_relaxed);
}

static int connect_to_localhost(uint16_t port, int timeout_ms = 3000) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return -1;
    }

    struct timeval tv{};
    tv.tv_sec  = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    return fd;
}

static std::string recv_line(int fd, int timeout_ms = 3000) {
    struct timeval tv{};
    tv.tv_sec  = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    std::string result;
    char ch;
    while (true) {
        ssize_t n = ::recv(fd, &ch, 1, 0);
        if (n <= 0) break;
        if (ch == '\n') break;
        result += ch;
    }
    return result;
}

} // anonymous namespace

// ── ReplCompressHandshake: verify COMPRESS LZ4 is sent when config_.compress is true ──
// Validates: Requirement 2.2
TEST(ReplCompress, ReplCompressHandshake) {
    TempDir tmp("compress_hs");
    ob::WALWriter wal(tmp.str());
    uint16_t port = alloc_port();

    ob::ReplicationConfig cfg;
    cfg.port = port;
    cfg.max_replicas = 4;
    cfg.compress = true;

    ob::ReplicationManager mgr(cfg, wal);
    mgr.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    int fd = connect_to_localhost(port);
    ASSERT_GE(fd, 0) << "Should connect to replication port";

    // The primary should send COMPRESS LZ4 as the first line.
    std::string line = recv_line(fd, 3000);
    EXPECT_EQ(line, "COMPRESS LZ4")
        << "Primary with compress=true should send COMPRESS LZ4 directive, got: " << line;

    ::close(fd);
    mgr.stop();
}

// ── ReplNoCompressDefault: verify no directive when config_.compress is false ──
// Validates: Requirement 2.4
TEST(ReplCompress, ReplNoCompressDefault) {
    TempDir tmp("no_compress");
    ob::WALWriter wal(tmp.str());
    uint16_t port = alloc_port();

    ob::ReplicationConfig cfg;
    cfg.port = port;
    cfg.max_replicas = 4;
    cfg.compress = false;

    ob::ReplicationManager mgr(cfg, wal);
    mgr.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    int fd = connect_to_localhost(port, 2000);
    ASSERT_GE(fd, 0) << "Should connect to replication port";

    // Send REPLICATE handshake — the primary should NOT have sent COMPRESS LZ4.
    const char* handshake = "REPLICATE 0 0\n";
    ::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL);

    // Wait for heartbeat (which comes after ~5s). If we receive a HEARTBEAT
    // as the first line, it means no COMPRESS LZ4 was sent.
    // Use a shorter approach: try to recv with a short timeout.
    // With compress=false, the primary sends nothing until heartbeat or WAL data.
    // Set a short recv timeout and verify we get nothing (EAGAIN).
    struct timeval tv{};
    tv.tv_sec  = 1;
    tv.tv_usec = 0;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    char buf[64];
    ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
    // n should be -1 with EAGAIN (no data sent by primary in 1 second)
    // or n > 0 if the primary sent something (which it shouldn't before heartbeat).
    if (n > 0) {
        std::string received(buf, static_cast<size_t>(n));
        EXPECT_TRUE(received.find("COMPRESS") == std::string::npos)
            << "Primary with compress=false should NOT send COMPRESS directive, got: " << received;
    }
    // If n <= 0, that's expected — no data sent.

    ::close(fd);
    mgr.stop();
}
