#include <gtest/gtest.h>
#include "orderbook/replication.hpp"

TEST(ReplicationSmoke, ConfigDefaults) {
    ob::ReplicationConfig config;
    EXPECT_EQ(config.port, 0);
    EXPECT_EQ(config.max_replicas, 4);

    ob::ReplicationClientConfig client_config;
    EXPECT_EQ(client_config.primary_port, 0);
    EXPECT_TRUE(client_config.primary_host.empty());
}

// ── Replication protocol integration tests (Task 7.1) ─────────────────────────
// Tests: REPLICATE handshake, ACK message, HEARTBEAT
// Requirements: 4.2, 4.3, 4.4

#include "orderbook/wal.hpp"
#include "orderbook/data_model.hpp"

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

// Unique temp directory helper (same pattern as test_wal.cpp).
static std::filesystem::path make_repl_temp_dir(const std::string& suffix = "") {
    static std::atomic<uint64_t> counter{0};
    auto base = std::filesystem::temp_directory_path() /
                ("ob_repl_test_" + suffix + "_" +
                 std::to_string(counter.fetch_add(1, std::memory_order_relaxed)));
    std::filesystem::create_directories(base);
    return base;
}

struct ReplTempDir {
    std::filesystem::path path;
    explicit ReplTempDir(const std::string& suffix = "")
        : path(make_repl_temp_dir(suffix)) {}
    ~ReplTempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
    std::string str() const { return path.string(); }
};

// Connect to localhost:port. Returns fd or -1 on failure.
static int connect_to_localhost(uint16_t port, int timeout_ms = 2000) {
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

    // Set recv timeout so tests don't hang.
    struct timeval tv{};
    tv.tv_sec  = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    return fd;
}

// Read a newline-terminated line from fd. Returns the line (without \n), or "" on timeout/error.
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

// Use a base port that's unlikely to conflict. Each test fixture picks a unique port.
static std::atomic<uint16_t> next_port{19876};

static uint16_t alloc_port() {
    return next_port.fetch_add(1, std::memory_order_relaxed);
}

} // anonymous namespace

// ── Test fixture ──────────────────────────────────────────────────────────────

class ReplicationProtocolTest : public ::testing::Test {
protected:
    void SetUp() override {
        tmp_ = std::make_unique<ReplTempDir>("proto");
        wal_ = std::make_unique<ob::WALWriter>(tmp_->str());
        port_ = alloc_port();
    }

    void TearDown() override {
        wal_.reset();
        tmp_.reset();
    }

    // Start a ReplicationManager and wait for it to be ready.
    std::unique_ptr<ob::ReplicationManager> start_manager() {
        ob::ReplicationConfig cfg;
        cfg.port = port_;
        cfg.max_replicas = 4;
        auto mgr = std::make_unique<ob::ReplicationManager>(cfg, *wal_);
        mgr->start();
        // Give the epoll thread time to start and bind.
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        return mgr;
    }

    uint16_t port_{0};
    std::unique_ptr<ReplTempDir> tmp_;
    std::unique_ptr<ob::WALWriter> wal_;
};

// ── Test 1: ReplicationManager starts and accepts connections ─────────────────
// Validates: Requirement 4.1 (dedicated TCP port)
TEST_F(ReplicationProtocolTest, ManagerAcceptsConnection) {
    auto mgr = start_manager();

    int fd = connect_to_localhost(port_);
    ASSERT_GE(fd, 0) << "Should connect to replication port";

    // Give the manager time to accept.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto states = mgr->replica_states();
    EXPECT_EQ(states.size(), 1u) << "One replica should be registered";

    ::close(fd);
    mgr->stop();
}

// ── Test 2: REPLICATE handshake is accepted ───────────────────────────────────
// Validates: Requirement 4.2 (REPLICATE handshake)
TEST_F(ReplicationProtocolTest, ReplicateHandshakeAccepted) {
    auto mgr = start_manager();

    int fd = connect_to_localhost(port_);
    ASSERT_GE(fd, 0);

    // Send REPLICATE handshake.
    const char* handshake = "REPLICATE 0 0\n";
    ssize_t sent = ::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL);
    EXPECT_GT(sent, 0);

    // Give the manager time to process.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Connection should still be open — verify by checking replica_states.
    auto states = mgr->replica_states();
    EXPECT_EQ(states.size(), 1u);
    EXPECT_EQ(states[0].confirmed_file, 0u);
    EXPECT_EQ(states[0].confirmed_offset, 0u);

    ::close(fd);
    mgr->stop();
}

// ── Test 3: REPLICATE handshake with non-zero offset ──────────────────────────
// Validates: Requirement 4.2 (REPLICATE with offset)
TEST_F(ReplicationProtocolTest, ReplicateHandshakeWithOffset) {
    auto mgr = start_manager();

    int fd = connect_to_localhost(port_);
    ASSERT_GE(fd, 0);

    // Send REPLICATE with a specific offset.
    const char* handshake = "REPLICATE 2 4096\n";
    ssize_t sent = ::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL);
    EXPECT_GT(sent, 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    auto states = mgr->replica_states();
    EXPECT_EQ(states.size(), 1u);
    EXPECT_EQ(states[0].confirmed_file, 2u);
    EXPECT_EQ(states[0].confirmed_offset, 4096u);

    ::close(fd);
    mgr->stop();
}

// ── Test 4: ACK message updates replica state ─────────────────────────────────
// Validates: Requirement 4.4 (ACK message)
TEST_F(ReplicationProtocolTest, AckUpdatesReplicaState) {
    auto mgr = start_manager();

    int fd = connect_to_localhost(port_);
    ASSERT_GE(fd, 0);

    // First send handshake.
    const char* handshake = "REPLICATE 0 0\n";
    ::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL);
    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    // Now send ACK with updated offset.
    const char* ack = "ACK 1 1024\n";
    ssize_t sent = ::send(fd, ack, std::strlen(ack), MSG_NOSIGNAL);
    EXPECT_GT(sent, 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    auto states = mgr->replica_states();
    ASSERT_EQ(states.size(), 1u);
    EXPECT_EQ(states[0].confirmed_file, 1u);
    EXPECT_EQ(states[0].confirmed_offset, 1024u);

    ::close(fd);
    mgr->stop();
}

// ── Test 5: Multiple ACKs update state progressively ──────────────────────────
// Validates: Requirement 4.4 (ACK updates confirmed offset)
TEST_F(ReplicationProtocolTest, MultipleAcksUpdateState) {
    auto mgr = start_manager();

    int fd = connect_to_localhost(port_);
    ASSERT_GE(fd, 0);

    const char* handshake = "REPLICATE 0 0\n";
    ::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL);
    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    // Send first ACK.
    const char* ack1 = "ACK 0 512\n";
    ::send(fd, ack1, std::strlen(ack1), MSG_NOSIGNAL);
    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    auto states = mgr->replica_states();
    ASSERT_EQ(states.size(), 1u);
    EXPECT_EQ(states[0].confirmed_file, 0u);
    EXPECT_EQ(states[0].confirmed_offset, 512u);

    // Send second ACK with higher offset.
    const char* ack2 = "ACK 1 2048\n";
    ::send(fd, ack2, std::strlen(ack2), MSG_NOSIGNAL);
    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    states = mgr->replica_states();
    ASSERT_EQ(states.size(), 1u);
    EXPECT_EQ(states[0].confirmed_file, 1u);
    EXPECT_EQ(states[0].confirmed_offset, 2048u);

    ::close(fd);
    mgr->stop();
}

// ── Test 6: HEARTBEAT is sent after idle period ───────────────────────────────
// Validates: Requirement 4.5 (HEARTBEAT every 5 seconds)
TEST_F(ReplicationProtocolTest, HeartbeatSentAfterIdle) {
    auto mgr = start_manager();

    int fd = connect_to_localhost(port_, 8000);
    ASSERT_GE(fd, 0);

    // Send handshake so we're a registered replica.
    const char* handshake = "REPLICATE 0 0\n";
    ::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL);

    // Wait for heartbeat (sent every 5 seconds). Use a generous timeout.
    // The epoll loop checks every 100ms and sends heartbeat after 5s idle.
    std::string line = recv_line(fd, 7000);
    EXPECT_EQ(line, "HEARTBEAT") << "Should receive HEARTBEAT after idle period";

    ::close(fd);
    mgr->stop();
}

// ── Test 7: Replica disconnect is handled gracefully ──────────────────────────
// Validates: Requirement 1.3 (disconnect handling)
TEST_F(ReplicationProtocolTest, ReplicaDisconnectHandled) {
    auto mgr = start_manager();

    int fd = connect_to_localhost(port_);
    ASSERT_GE(fd, 0);

    const char* handshake = "REPLICATE 0 0\n";
    ::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL);
    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    EXPECT_EQ(mgr->replica_states().size(), 1u);

    // Disconnect.
    ::close(fd);

    // Give the manager time to detect the disconnect (next epoll cycle or heartbeat).
    // The manager detects disconnect on the next read or write attempt.
    // Force detection by waiting for a heartbeat cycle.
    std::this_thread::sleep_for(std::chrono::milliseconds(6000));

    EXPECT_EQ(mgr->replica_states().size(), 0u)
        << "Disconnected replica should be removed";

    mgr->stop();
}

// ── Task 7.2: Unit tests for ReplicationManager ───────────────────────────────
// Tests: broadcast to multiple replicas, disconnect handling, max replicas
// Requirements: 1.2, 1.3, 4.5

// ── Test 8: Broadcast WAL record to multiple replicas ─────────────────────────
// Validates: Requirement 1.2 (send WAL record to all connected replicas)
TEST_F(ReplicationProtocolTest, BroadcastToMultipleReplicas) {
    auto mgr = start_manager();

    // Connect two replicas.
    int fd1 = connect_to_localhost(port_);
    int fd2 = connect_to_localhost(port_);
    ASSERT_GE(fd1, 0);
    ASSERT_GE(fd2, 0);

    // Send handshake from both.
    const char* handshake = "REPLICATE 0 0\n";
    ::send(fd1, handshake, std::strlen(handshake), MSG_NOSIGNAL);
    ::send(fd2, handshake, std::strlen(handshake), MSG_NOSIGNAL);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    EXPECT_EQ(mgr->replica_states().size(), 2u);

    // Broadcast a WAL record.
    ob::WALRecord hdr{};
    hdr.sequence_number = 1;
    hdr.timestamp_ns    = 1000;
    hdr.checksum        = 0x12345678;
    hdr.payload_len     = 4;
    hdr.record_type     = ob::WAL_RECORD_DELTA;
    hdr._pad            = 0;
    uint8_t payload[] = {0xDE, 0xAD, 0xBE, 0xEF};
    mgr->broadcast(hdr, payload, 4);

    // Both replicas should receive the WAL header line.
    std::string line1 = recv_line(fd1, 3000);
    std::string line2 = recv_line(fd2, 3000);

    EXPECT_TRUE(line1.rfind("WAL ", 0) == 0)
        << "Replica 1 should receive WAL header, got: " << line1;
    EXPECT_TRUE(line2.rfind("WAL ", 0) == 0)
        << "Replica 2 should receive WAL header, got: " << line2;

    ::close(fd1);
    ::close(fd2);
    mgr->stop();
}

// ── Test 9: Broadcast removes disconnected replica ────────────────────────────
// Validates: Requirement 1.3 (disconnect handling during broadcast)
TEST_F(ReplicationProtocolTest, BroadcastRemovesDisconnectedReplica) {
    auto mgr = start_manager();

    // Connect two replicas.
    int fd1 = connect_to_localhost(port_);
    int fd2 = connect_to_localhost(port_);
    ASSERT_GE(fd1, 0);
    ASSERT_GE(fd2, 0);

    // Send handshake from both.
    const char* handshake = "REPLICATE 0 0\n";
    ::send(fd1, handshake, std::strlen(handshake), MSG_NOSIGNAL);
    ::send(fd2, handshake, std::strlen(handshake), MSG_NOSIGNAL);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    EXPECT_EQ(mgr->replica_states().size(), 2u);

    // Disconnect replica 1.
    ::close(fd1);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Broadcast a WAL record — this should detect the dead fd1 and remove it.
    ob::WALRecord hdr{};
    hdr.sequence_number = 1;
    hdr.timestamp_ns    = 1000;
    hdr.checksum        = 0x12345678;
    hdr.payload_len     = 4;
    hdr.record_type     = ob::WAL_RECORD_DELTA;
    hdr._pad            = 0;
    uint8_t payload[] = {0xDE, 0xAD, 0xBE, 0xEF};
    mgr->broadcast(hdr, payload, 4);

    // The surviving replica should receive the WAL message.
    std::string line2 = recv_line(fd2, 3000);
    EXPECT_TRUE(line2.rfind("WAL ", 0) == 0)
        << "Surviving replica should receive WAL header, got: " << line2;

    // After broadcast, only 1 replica should remain.
    // The disconnected one may be removed during broadcast or on next epoll cycle.
    // Give a moment for cleanup.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    auto states = mgr->replica_states();
    EXPECT_LE(states.size(), 1u)
        << "Disconnected replica should be removed after broadcast";

    ::close(fd2);
    mgr->stop();
}

// ── Test 10: Max replicas enforced ────────────────────────────────────────────
// Validates: Requirement 1.4 (max_replicas limit)
TEST_F(ReplicationProtocolTest, MaxReplicasEnforced) {
    // Create a manager with max_replicas=2.
    ob::ReplicationConfig cfg;
    cfg.port = port_;
    cfg.max_replicas = 2;
    auto mgr = std::make_unique<ob::ReplicationManager>(cfg, *wal_);
    mgr->start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Connect 2 replicas — both should succeed.
    int fd1 = connect_to_localhost(port_);
    int fd2 = connect_to_localhost(port_);
    ASSERT_GE(fd1, 0);
    ASSERT_GE(fd2, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    EXPECT_EQ(mgr->replica_states().size(), 2u);

    // Connect a 3rd replica — should be rejected.
    int fd3 = connect_to_localhost(port_);
    ASSERT_GE(fd3, 0) << "TCP connect should succeed (rejection happens after accept)";

    // The 3rd connection should receive "ERR max_replicas_reached" and be closed.
    std::string err_line = recv_line(fd3, 3000);
    EXPECT_EQ(err_line, "ERR max_replicas_reached")
        << "3rd replica should receive max_replicas_reached error, got: " << err_line;

    // Still only 2 replicas registered.
    EXPECT_EQ(mgr->replica_states().size(), 2u);

    ::close(fd1);
    ::close(fd2);
    ::close(fd3);
    mgr->stop();
}

// ── Task 7.3: Unit tests for ReplicationClient ────────────────────────────────
// Tests: Receive and replay WAL record, CRC verification, ACK sending
// Requirements: 2.1, 2.2, 2.3, 2.4

#include "orderbook/engine.hpp"

namespace {

// ── Portable CRC32C (same lookup table as replication.cpp / engine.cpp) ───────
static constexpr uint32_t TEST_CRC32C_POLY = 0x82F63B78u;
static uint32_t test_crc32c_table[256];
static bool     test_crc32c_table_init = false;

static void test_init_crc32c_table() {
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            crc = (crc >> 1) ^ ((crc & 1u) ? TEST_CRC32C_POLY : 0u);
        }
        test_crc32c_table[i] = crc;
    }
    test_crc32c_table_init = true;
}

static uint32_t test_crc32c(const void* data, size_t len) {
    if (!test_crc32c_table_init) test_init_crc32c_table();
    const auto* p = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; ++i) {
        crc = (crc >> 8) ^ test_crc32c_table[(crc ^ p[i]) & 0xFFu];
    }
    return crc ^ 0xFFFFFFFFu;
}

// ── Mock primary server helper ────────────────────────────────────────────────
// Creates a listening TCP socket on a given port. Returns listen_fd or -1.
static int create_mock_primary(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port);

    if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return -1;
    }
    if (::listen(fd, 4) < 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

// Accept a connection with a timeout. Returns client_fd or -1.
static int accept_with_timeout(int listen_fd, int timeout_ms = 5000) {
    struct timeval tv{};
    tv.tv_sec  = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;

    fd_set fds;
    FD_ZERO(&fds);
    FD_SET(listen_fd, &fds);

    int ret = ::select(listen_fd + 1, &fds, nullptr, nullptr, &tv);
    if (ret <= 0) return -1;

    struct sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    int client_fd = ::accept(listen_fd,
                             reinterpret_cast<struct sockaddr*>(&client_addr),
                             &client_len);
    if (client_fd >= 0) {
        // Set recv timeout on the accepted socket.
        ::setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }
    return client_fd;
}

// Build a valid WAL wire message: "WAL <file_index> <byte_offset> <total_len>\n<WALRecord><payload>"
// Returns the complete message bytes.
static std::vector<uint8_t> build_wal_message(uint32_t file_index, size_t byte_offset,
                                               const ob::WALRecord& hdr,
                                               const void* payload, size_t payload_len) {
    const size_t total_len = sizeof(ob::WALRecord) + payload_len;
    char line[128];
    int line_len = std::snprintf(line, sizeof(line), "WAL %u %zu %zu\n",
                                  file_index, byte_offset, total_len);

    std::vector<uint8_t> msg(static_cast<size_t>(line_len) + total_len);
    std::memcpy(msg.data(), line, static_cast<size_t>(line_len));
    std::memcpy(msg.data() + line_len, &hdr, sizeof(ob::WALRecord));
    if (payload_len > 0) {
        std::memcpy(msg.data() + line_len + sizeof(ob::WALRecord), payload, payload_len);
    }
    return msg;
}

// Build a DeltaUpdate + Level payload and compute its CRC32C.
// Returns {payload_bytes, crc32c}.
struct PayloadWithCrc {
    std::vector<uint8_t> data;
    uint32_t crc;
};

static PayloadWithCrc build_delta_payload(const char* symbol, const char* exchange,
                                           uint64_t seq, uint64_t ts_ns,
                                           uint8_t side, int64_t price, uint64_t qty) {
    ob::DeltaUpdate delta{};
    // Zero-init symbol and exchange arrays explicitly (value-init handles the rest).
    std::memset(delta.symbol, 0, sizeof(delta.symbol));
    std::memset(delta.exchange, 0, sizeof(delta.exchange));
    std::strncpy(delta.symbol, symbol, sizeof(delta.symbol) - 1);
    std::strncpy(delta.exchange, exchange, sizeof(delta.exchange) - 1);
    delta.sequence_number = seq;
    delta.timestamp_ns    = ts_ns;
    delta.side            = side;
    delta.n_levels        = 1;

    ob::Level lvl{};
    lvl.price = price;
    lvl.qty   = qty;
    lvl.cnt   = 1;
    lvl._pad  = 0;

    const size_t payload_len = sizeof(ob::DeltaUpdate) + sizeof(ob::Level);
    std::vector<uint8_t> payload(payload_len);
    std::memcpy(payload.data(), &delta, sizeof(ob::DeltaUpdate));
    std::memcpy(payload.data() + sizeof(ob::DeltaUpdate), &lvl, sizeof(ob::Level));

    uint32_t crc = test_crc32c(payload.data(), payload_len);
    return {std::move(payload), crc};
}

} // anonymous namespace

// ── Test fixture for ReplicationClient tests ──────────────────────────────────

class ReplicationClientTest : public ::testing::Test {
protected:
    void SetUp() override {
        tmp_ = std::make_unique<ReplTempDir>("client");
        port_ = alloc_port();
    }

    void TearDown() override {
        tmp_.reset();
    }

    uint16_t port_{0};
    std::unique_ptr<ReplTempDir> tmp_;
};

// ── Test 11: Client connects and sends REPLICATE handshake ────────────────────
// Validates: Requirement 4.2 (REPLICATE handshake from replica)
TEST_F(ReplicationClientTest, ClientConnectsAndSendsHandshake) {
    // 1. Start a mock primary TCP server.
    int listen_fd = create_mock_primary(port_);
    ASSERT_GE(listen_fd, 0) << "Mock primary should bind successfully";

    // 2. Create an Engine in the temp directory and open it.
    ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();

    // 3. Create a ReplicationClient pointing to the mock primary.
    ob::ReplicationClientConfig cfg;
    cfg.primary_host = "127.0.0.1";
    cfg.primary_port = port_;
    cfg.state_file   = tmp_->str() + "/repl_state.txt";

    ob::ReplicationClient client(cfg, engine);
    client.start();

    // 4. Accept the connection from the client.
    int client_fd = accept_with_timeout(listen_fd, 5000);
    ASSERT_GE(client_fd, 0) << "Client should connect to mock primary";

    // 5. Read the REPLICATE handshake.
    std::string handshake = recv_line(client_fd, 3000);
    EXPECT_EQ(handshake, "REPLICATE 0 0")
        << "Client should send REPLICATE 0 0 handshake, got: " << handshake;

    // Cleanup.
    client.stop();
    ::close(client_fd);
    ::close(listen_fd);
    engine.close();
}

// ── Test 12: Client receives and replays a WAL record ─────────────────────────
// Validates: Requirements 2.1 (replay), 2.2 (CRC verification), 2.4 (ACK)
TEST_F(ReplicationClientTest, ClientReceivesAndReplaysWalRecord) {
    // 1. Start mock primary.
    int listen_fd = create_mock_primary(port_);
    ASSERT_GE(listen_fd, 0);

    // 2. Create and open Engine.
    ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();

    // 3. Create and start ReplicationClient.
    ob::ReplicationClientConfig cfg;
    cfg.primary_host = "127.0.0.1";
    cfg.primary_port = port_;
    cfg.state_file   = tmp_->str() + "/repl_state.txt";

    ob::ReplicationClient client(cfg, engine);
    client.start();

    // 4. Accept connection and read handshake.
    int client_fd = accept_with_timeout(listen_fd, 5000);
    ASSERT_GE(client_fd, 0);
    std::string handshake = recv_line(client_fd, 3000);
    EXPECT_TRUE(handshake.rfind("REPLICATE", 0) == 0);

    // 5. Build a valid WAL record with correct CRC32C.
    auto [payload, crc] = build_delta_payload("BTCUSD", "BINANCE", 1, 1000000, 0, 50000, 100);

    ob::WALRecord hdr{};
    hdr.sequence_number = 1;
    hdr.timestamp_ns    = 1000000;
    hdr.checksum        = crc;
    hdr.payload_len     = static_cast<uint16_t>(payload.size());
    hdr.record_type     = ob::WAL_RECORD_DELTA;
    hdr._pad            = 0;

    auto msg = build_wal_message(0, 0, hdr, payload.data(), payload.size());

    // 6. Send the WAL record to the client.
    ssize_t sent = ::send(client_fd, msg.data(), msg.size(), MSG_NOSIGNAL);
    EXPECT_EQ(sent, static_cast<ssize_t>(msg.size()));

    // 7. Wait for the client to process and send ACK.
    std::string ack = recv_line(client_fd, 5000);
    EXPECT_TRUE(ack.rfind("ACK ", 0) == 0)
        << "Client should send ACK after replaying, got: " << ack;

    // 8. Verify client state shows records_replayed > 0.
    // Give a moment for state to update.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    auto state = client.state();
    EXPECT_GE(state.records_replayed, 1u)
        << "Client should have replayed at least 1 record";

    // Cleanup.
    client.stop();
    ::close(client_fd);
    ::close(listen_fd);
    engine.close();
}

// ── Test 13: Client rejects WAL record with bad CRC ──────────────────────────
// Validates: Requirements 2.2 (CRC verification), 2.3 (disconnect on mismatch)
TEST_F(ReplicationClientTest, ClientRejectsBadCrc) {
    // 1. Start mock primary.
    int listen_fd = create_mock_primary(port_);
    ASSERT_GE(listen_fd, 0);

    // 2. Create and open Engine.
    ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();

    // 3. Create and start ReplicationClient.
    ob::ReplicationClientConfig cfg;
    cfg.primary_host = "127.0.0.1";
    cfg.primary_port = port_;
    cfg.state_file   = tmp_->str() + "/repl_state.txt";

    ob::ReplicationClient client(cfg, engine);
    client.start();

    // 4. Accept connection and read handshake.
    int client_fd = accept_with_timeout(listen_fd, 5000);
    ASSERT_GE(client_fd, 0);
    std::string handshake = recv_line(client_fd, 3000);
    EXPECT_TRUE(handshake.rfind("REPLICATE", 0) == 0);

    // 5. Build a WAL record with INCORRECT CRC32C.
    auto [payload, correct_crc] = build_delta_payload("ETHUSD", "KRAKEN", 1, 2000000, 1, 3000, 50);

    ob::WALRecord hdr{};
    hdr.sequence_number = 1;
    hdr.timestamp_ns    = 2000000;
    hdr.checksum        = correct_crc ^ 0xDEADBEEF; // Corrupt the CRC
    hdr.payload_len     = static_cast<uint16_t>(payload.size());
    hdr.record_type     = ob::WAL_RECORD_DELTA;
    hdr._pad            = 0;

    auto msg = build_wal_message(0, 0, hdr, payload.data(), payload.size());

    // 6. Send the bad WAL record.
    ssize_t sent = ::send(client_fd, msg.data(), msg.size(), MSG_NOSIGNAL);
    EXPECT_EQ(sent, static_cast<ssize_t>(msg.size()));

    // 7. The client should disconnect (CRC mismatch → disconnect per Requirement 2.3).
    //    Wait for the client to process and disconnect. The client's run_loop will
    //    close the fd and attempt to reconnect. We detect this by:
    //    a) No ACK received (recv times out or returns 0)
    //    b) Client state shows records_replayed == 0
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto state = client.state();
    EXPECT_EQ(state.records_replayed, 0u)
        << "Client should NOT have replayed a record with bad CRC";

    // The client will try to reconnect (run_loop backoff). We can verify
    // by accepting the reconnection attempt.
    int reconnect_fd = accept_with_timeout(listen_fd, 8000);
    EXPECT_GE(reconnect_fd, 0)
        << "Client should attempt to reconnect after CRC-induced disconnect";

    // Cleanup.
    client.stop();
    if (reconnect_fd >= 0) ::close(reconnect_fd);
    ::close(client_fd);
    ::close(listen_fd);
    engine.close();
}

// ── Task 7.5: Integration test — primary-replica full cycle ───────────────────
// Validates: Requirements 1.2, 2.1, 3.1

TEST(ReplicationIntegration, PrimaryReplicaFullCycle) {
    // 1. Allocate a unique replication port and two separate temp directories.
    const uint16_t repl_port = alloc_port();
    ReplTempDir primary_dir("primary");
    ReplTempDir replica_dir("replica");

    // 2. Create primary Engine with replication enabled.
    ob::Engine primary(primary_dir.str(), 100'000'000ULL, ob::FsyncPolicy::NONE,
                       {repl_port, 4}, {});
    primary.open();

    // Give the primary's ReplicationManager time to bind and start listening.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 3. Create replica Engine pointing to the primary's replication port.
    ob::Engine replica(replica_dir.str(), 100'000'000ULL, ob::FsyncPolicy::NONE,
                       {},
                       {"127.0.0.1", repl_port, replica_dir.str() + "/repl_state.txt"});
    replica.open();

    // Give the replica time to connect and complete the REPLICATE handshake.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // 4. Insert data into the primary.
    ob::DeltaUpdate delta{};
    std::memset(delta.symbol, 0, sizeof(delta.symbol));
    std::memset(delta.exchange, 0, sizeof(delta.exchange));
    std::strncpy(delta.symbol, "BTCUSD", sizeof(delta.symbol) - 1);
    std::strncpy(delta.exchange, "BINANCE", sizeof(delta.exchange) - 1);
    delta.sequence_number = 1;
    delta.timestamp_ns    = 1'000'000'000ULL;
    delta.side            = ob::SIDE_BID;
    delta.n_levels        = 1;

    ob::Level lvl{};
    lvl.price = 50000;
    lvl.qty   = 100;
    lvl.cnt   = 1;
    lvl._pad  = 0;

    ob::ob_status_t status = primary.apply_delta(delta, &lvl);
    EXPECT_EQ(status, ob::OB_OK);

    // 5. Wait for replication to propagate (the primary broadcasts the WAL record,
    //    the replica receives, verifies CRC, replays via apply_delta, and sends ACK).
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // 6. Verify the replica's stats show it is a replica with replayed records.
    auto es = replica.stats();
    EXPECT_TRUE(es.is_replica) << "Replica engine should report is_replica=true";
    EXPECT_GT(es.repl_records_replayed, 0u)
        << "Replica should have replayed at least 1 record";

    // 7. Clean up: close both engines.
    replica.close();
    primary.close();
}

// ── Task 7.6: Unit test for WAL truncation safety with replicas ───────────────
// Validates: Requirement 6.3 (WAL truncation respects replica confirmed offsets)
//
// The Engine::flush_loop() computes safe_truncate as:
//   safe_truncate = min(wal_.current_file_index(), min(r.confirmed_file for all replicas))
// This test verifies that ReplicationManager::replica_states() correctly reports
// each replica's confirmed_file, which flush_loop() uses to block premature truncation.

TEST_F(ReplicationProtocolTest, WalTruncationRespectsReplicaConfirmedOffset) {
    auto mgr = start_manager();

    // 1. Connect a mock replica and send REPLICATE 0 0 (replica is at file 0).
    int fd = connect_to_localhost(port_);
    ASSERT_GE(fd, 0);

    const char* handshake = "REPLICATE 0 0\n";
    ::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 2. Verify replica_states() reports confirmed_file=0.
    //    This means flush_loop() would compute safe_truncate = min(current, 0) = 0,
    //    so truncate_before(0) removes nothing — WAL file 0 is protected.
    {
        auto states = mgr->replica_states();
        ASSERT_EQ(states.size(), 1u);
        EXPECT_EQ(states[0].confirmed_file, 0u)
            << "Replica at file 0 should block truncation of file 0";
        EXPECT_EQ(states[0].confirmed_offset, 0u);
    }

    // 3. Simulate the safe_truncate computation from flush_loop().
    //    With current_file_index (e.g. 3) and replica at file 0,
    //    safe_truncate should be 0 — no files truncated.
    {
        const uint32_t simulated_current_file = 3;
        uint32_t safe_truncate = simulated_current_file;
        for (const auto& r : mgr->replica_states()) {
            safe_truncate = std::min(safe_truncate, r.confirmed_file);
        }
        EXPECT_EQ(safe_truncate, 0u)
            << "safe_truncate should be 0 when replica is at file 0";
    }

    // 4. Replica sends ACK advancing past file 0 (now confirmed at file 2).
    const char* ack = "ACK 2 4096\n";
    ::send(fd, ack, std::strlen(ack), MSG_NOSIGNAL);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 5. Verify replica_states() now reports confirmed_file=2.
    //    flush_loop() would compute safe_truncate = min(current, 2) = 2 (if current >= 2),
    //    so truncate_before(2) can now remove files 0 and 1.
    {
        auto states = mgr->replica_states();
        ASSERT_EQ(states.size(), 1u);
        EXPECT_EQ(states[0].confirmed_file, 2u)
            << "After ACK 2, replica should be at file 2";
        EXPECT_EQ(states[0].confirmed_offset, 4096u);
    }

    // 6. Re-simulate safe_truncate: with replica at file 2 and current=3,
    //    safe_truncate = min(3, 2) = 2 — files before 2 can be truncated.
    {
        const uint32_t simulated_current_file = 3;
        uint32_t safe_truncate = simulated_current_file;
        for (const auto& r : mgr->replica_states()) {
            safe_truncate = std::min(safe_truncate, r.confirmed_file);
        }
        EXPECT_EQ(safe_truncate, 2u)
            << "safe_truncate should be 2 after replica confirms past file 1";
    }

    ::close(fd);
    mgr->stop();
}

// ── Test: Multiple replicas — truncation blocked by slowest replica ───────────
// Validates: Requirement 6.3 (ALL replicas must confirm past truncation point)
TEST_F(ReplicationProtocolTest, WalTruncationBlockedBySlowestReplica) {
    auto mgr = start_manager();

    // Connect two replicas.
    int fd1 = connect_to_localhost(port_);
    int fd2 = connect_to_localhost(port_);
    ASSERT_GE(fd1, 0);
    ASSERT_GE(fd2, 0);

    // Replica 1 starts at file 0, replica 2 starts at file 0.
    const char* hs = "REPLICATE 0 0\n";
    ::send(fd1, hs, std::strlen(hs), MSG_NOSIGNAL);
    ::send(fd2, hs, std::strlen(hs), MSG_NOSIGNAL);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Advance replica 1 to file 3 (fast replica).
    const char* ack1 = "ACK 3 8192\n";
    ::send(fd1, ack1, std::strlen(ack1), MSG_NOSIGNAL);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Replica 2 stays at file 0 (slow replica).
    // Compute safe_truncate: min(current=5, min(3, 0)) = 0.
    {
        const uint32_t simulated_current_file = 5;
        uint32_t safe_truncate = simulated_current_file;
        for (const auto& r : mgr->replica_states()) {
            safe_truncate = std::min(safe_truncate, r.confirmed_file);
        }
        EXPECT_EQ(safe_truncate, 0u)
            << "Slow replica at file 0 should block all truncation";
    }

    // Now advance the slow replica to file 2.
    const char* ack2 = "ACK 2 1024\n";
    ::send(fd2, ack2, std::strlen(ack2), MSG_NOSIGNAL);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Compute safe_truncate: min(current=5, min(3, 2)) = 2.
    {
        const uint32_t simulated_current_file = 5;
        uint32_t safe_truncate = simulated_current_file;
        for (const auto& r : mgr->replica_states()) {
            safe_truncate = std::min(safe_truncate, r.confirmed_file);
        }
        EXPECT_EQ(safe_truncate, 2u)
            << "safe_truncate should equal the slowest replica's confirmed_file";
    }

    ::close(fd1);
    ::close(fd2);
    mgr->stop();
}
