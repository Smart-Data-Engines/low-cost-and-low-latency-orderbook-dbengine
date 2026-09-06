// Tests for replication compression handshake (Task 3.5)
// Feature: compression-and-ttl
// Requirements: 2.2, 2.4

#include <gtest/gtest.h>

#include "orderbook/replication.hpp"
#include "orderbook/wal.hpp"
#include "orderbook/compression.hpp"
#include "orderbook/crc32c.hpp"
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

// ── ReplCompressHandshake: verify COMPRESS LZ4 is sent after catchup when config_.compress is true ──
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

    // Send REPLICATE handshake — COMPRESS LZ4 is sent AFTER catchup.
    const char* handshake = "REPLICATE 0 0 0\n";
    ::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL);

    // After catchup (empty WAL), the primary should send COMPRESS LZ4.
    std::string line = recv_line(fd, 3000);
    EXPECT_EQ(line, "COMPRESS LZ4")
        << "Primary with compress=true should send COMPRESS LZ4 after catchup, got: " << line;

    ::close(fd);
    mgr.stop();
}


// ── The seam between plain catch-up and compressed live streaming ─────────────
// Validates: Requirement 2.4, roadmap #93

TEST(ReplCompress, TheDirectiveMarksTheSeamOfAnUnfinishedCatchup) {
    // The directive is a switch in how every following byte is framed, so where it sits in the
    // stream is the whole of its meaning. The synchronous catch-up made that trivial - it was sent
    // after a pass nothing could interrupt. A cursor stops and resumes, and live records arrive
    // mid-stream already compressed, so a directive in the wrong place hands the replica LZ4 frames
    // to read as plain lines.
    TempDir tmp("compress_seam");
    ob::WALWriter wal(tmp.str());
    uint16_t port = alloc_port();

    constexpr int    kRecords   = 1000;
    constexpr size_t kLevels    = 1000;
    constexpr uint64_t kMarkerSeq = 999999;

    std::vector<ob::Level> lv(kLevels);
    for (int i = 0; i < kRecords; ++i) {
        ob::DeltaUpdate delta{};
        std::strncpy(delta.symbol, "BTCUSD", sizeof(delta.symbol) - 1);
        std::strncpy(delta.exchange, "BINANCE", sizeof(delta.exchange) - 1);
        delta.sequence_number = static_cast<uint64_t>(i) + 1;
        delta.timestamp_ns    = 1'000'000'000ULL + static_cast<uint64_t>(i);
        delta.side            = ob::SIDE_BID;
        delta.n_levels        = static_cast<uint16_t>(kLevels);
        for (size_t l = 0; l < kLevels; ++l) {
            lv[l].price = static_cast<int64_t>(50000 + i * 1000 + static_cast<int>(l));
            lv[l].qty   = 100;
            lv[l].cnt   = 1;
            lv[l]._pad  = 0;
        }
        wal.append(delta, lv.data());
    }
    wal.flush();

    ob::ReplicationConfig cfg;
    cfg.port         = port;
    cfg.max_replicas = 4;
    cfg.compress     = true;

    ob::ReplicationManager mgr(cfg, wal);
    mgr.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    int fd = connect_to_localhost(port);
    ASSERT_GE(fd, 0);

    const char* handshake = "REPLICATE 0 0 0\n";
    ASSERT_GT(::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL), 0);

    // Nothing is read for this window, so the cursor is still streaming when the live write lands.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    ob::Level one{};
    one.price = 1; one.qty = 1; one.cnt = 1;
    ob::DeltaUpdate marker{};
    std::strncpy(marker.symbol, "BTCUSD", sizeof(marker.symbol) - 1);
    std::strncpy(marker.exchange, "BINANCE", sizeof(marker.exchange) - 1);
    marker.sequence_number = kMarkerSeq;
    marker.timestamp_ns    = 2'000'000'000ULL;
    marker.side            = ob::SIDE_BID;
    marker.n_levels        = 1;
    std::vector<uint8_t> payload(sizeof(ob::DeltaUpdate) + sizeof(ob::Level));
    std::memcpy(payload.data(), &marker, sizeof(marker));
    std::memcpy(payload.data() + sizeof(marker), &one, sizeof(one));
    ob::WALRecord hdr{};
    hdr.sequence_number = kMarkerSeq;
    hdr.timestamp_ns    = marker.timestamp_ns;
    hdr.payload_len     = static_cast<uint16_t>(payload.size());
    hdr.checksum        = ob::crc32c(payload.data(), payload.size());
    hdr.record_type     = ob::WAL_RECORD_DELTA;
    mgr.broadcast(hdr, payload.data(), payload.size());

    // Walk the stream: plain framed records, then the directive, then LZ4 frames.
    struct timeval tv{};
    tv.tv_sec = 10;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    std::string in;
    char rbuf[65536];
    const auto pull = [&]() {
        const ssize_t n = ::recv(fd, rbuf, sizeof(rbuf), 0);
        if (n <= 0) return false;
        in.append(rbuf, static_cast<size_t>(n));
        return true;
    };

    size_t plain_records = 0;
    bool   seam_seen     = false;
    while (!seam_seen) {
        const size_t nl = in.find('\n');
        if (nl == std::string::npos) { if (!pull()) break; continue; }
        const std::string line = in.substr(0, nl);
        if (line == "COMPRESS LZ4") {
            seam_seen = true;
            in.erase(0, nl + 1);
            break;
        }
        if (line.rfind("HEARTBEAT", 0) == 0) { in.erase(0, nl + 1); continue; }
        unsigned file = 0, epoch_lo = 0;
        size_t offset = 0, total_len = 0;
        if (std::sscanf(line.c_str(), "WAL %u %zu %zu %u", &file, &offset, &total_len,
                        &epoch_lo) < 3) {
            ADD_FAILURE() << "the plain half of the stream carried a line that is not a record "
                             "and not the directive: '" << line << "'. An LZ4 frame read as text "
                             "looks exactly like this";
            break;
        }
        if (in.size() < nl + 1 + total_len) { if (!pull()) break; continue; }
        in.erase(0, nl + 1 + total_len);
        ++plain_records;
    }

    EXPECT_TRUE(seam_seen) << "the directive never arrived, so the replica would read the live "
                              "records that follow it as plain text";
    EXPECT_EQ(plain_records, static_cast<size_t>(kRecords))
        << "the catch-up delivered " << plain_records << " of " << kRecords
        << " records before the seam; a record on the wrong side of it is framed the wrong way";

    // Exactly one frame after the seam, and it is the live record that waited.
    while (in.size() < 4) { if (!pull()) break; }
    ASSERT_GE(in.size(), 4u) << "nothing followed the directive";
    const uint32_t frame_len = (static_cast<uint32_t>(static_cast<uint8_t>(in[0])) << 24) |
                               (static_cast<uint32_t>(static_cast<uint8_t>(in[1])) << 16) |
                               (static_cast<uint32_t>(static_cast<uint8_t>(in[2])) << 8) |
                                static_cast<uint32_t>(static_cast<uint8_t>(in[3]));
    while (in.size() < 4 + frame_len) { if (!pull()) break; }
    ASSERT_GE(in.size(), 4u + frame_len) << "the frame after the directive is short";

    const auto plain = ob::lz4_decompress(in.data() + 4, frame_len);
    const std::string frame(reinterpret_cast<const char*>(plain.data()), plain.size());
    const size_t nl = frame.find('\n');
    ASSERT_NE(nl, std::string::npos) << "the decompressed frame carries no record line";
    uint64_t seq = 0;
    ASSERT_GE(plain.size(), nl + 1 + sizeof(uint64_t));
    std::memcpy(&seq, plain.data() + nl + 1, sizeof(seq));
    EXPECT_EQ(seq, kMarkerSeq) << "the first frame after the seam is not the record that waited";

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
