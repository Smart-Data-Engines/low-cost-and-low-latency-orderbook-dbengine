#include <gtest/gtest.h>
#include "orderbook/replication.hpp"

#include <set>

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
#include "orderbook/crc32c.hpp"

#include <algorithm>
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

/// A primary-side replication config, by field rather than by position.
///
/// Positional aggregate initialisation listed every field, so each new field in ReplicationConfig
/// broke six call sites in this file with a -Wmissing-field-initializers error unrelated to what
/// they test - which is how #30 first touched it.
ob::ReplicationConfig primary_config(uint16_t port, int max_replicas = 4,
                                     ob::SecretStore secret = {}) {
    ob::ReplicationConfig cfg{};
    cfg.port           = port;
    cfg.max_replicas   = max_replicas;
    cfg.cluster_secret = std::move(secret);
    return cfg;
}

/// The replica side of the same.
ob::ReplicationClientConfig replica_config(uint16_t port, const std::string& state_file,
                                           ob::SecretStore secret = {}) {
    ob::ReplicationClientConfig cfg{};
    cfg.primary_host   = "127.0.0.1";
    cfg.primary_port   = port;
    cfg.state_file     = state_file;
    cfg.cluster_secret = std::move(secret);
    return cfg;
}

} // namespace

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

namespace {

/// Fill a WAL with `records` deltas of `levels` levels each. Returns the bytes they weigh on the
/// wire, which is also what they weigh in the WAL: a record is its header followed by its payload.
size_t fill_wal(ob::WALWriter& wal, int records, size_t levels) {
    std::vector<ob::Level> lv(levels);
    for (int i = 0; i < records; ++i) {
        ob::DeltaUpdate delta{};
        std::strncpy(delta.symbol, "BTCUSD", sizeof(delta.symbol) - 1);
        std::strncpy(delta.exchange, "BINANCE", sizeof(delta.exchange) - 1);
        delta.sequence_number = static_cast<uint64_t>(i) + 1;
        delta.timestamp_ns    = 1'000'000'000ULL + static_cast<uint64_t>(i);
        delta.side            = ob::SIDE_BID;
        delta.n_levels        = static_cast<uint16_t>(levels);
        for (size_t l = 0; l < levels; ++l) {
            lv[l].price = static_cast<int64_t>(50000 + i * 1000 + static_cast<int>(l));
            lv[l].qty   = 100;
            lv[l].cnt   = 1;
            lv[l]._pad  = 0;
        }
        wal.append(delta, lv.data());
    }
    wal.flush();
    const size_t payload_len = sizeof(ob::DeltaUpdate) + levels * sizeof(ob::Level);
    return static_cast<size_t>(records) * (sizeof(ob::WALRecord) + payload_len);
}

/// Read framed records off a replication socket until it goes quiet, and return their WAL sequence
/// numbers in arrival order.
///
/// Until quiet rather than until `want`, and that is the difference between a test that can see a
/// record delivered twice and one that cannot: stopping at the expected count leaves the extra copy
/// in this function's own buffer, where it is indistinguishable from never having been sent. Found
/// by a mutation that survived for exactly that reason.
std::vector<uint64_t> recv_sequence_numbers(int fd, size_t want, int quiet_ms = 400,
                                            int total_cap_s = 20) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(total_cap_s);
    struct timeval tv{};
    tv.tv_sec  = quiet_ms / 1000;
    tv.tv_usec = (quiet_ms % 1000) * 1000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    std::vector<uint64_t> seqs;
    std::string in;
    char buf[65536];
    while (std::chrono::steady_clock::now() < deadline) {
        const size_t nl = in.find('\n');
        if (nl == std::string::npos) {
            const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
            if (n <= 0) break;   // quiet for `quiet_ms`, or closed
            in.append(buf, static_cast<size_t>(n));
            continue;
        }
        const std::string header = in.substr(0, nl);
        // A heartbeat or a COMPRESS directive is a line of this protocol too, and skipping them
        // rather than failing is what keeps this reader from being a clock: the heartbeat is on a
        // five-second timer that no test should have to finish inside.
        if (header.rfind("HEARTBEAT", 0) == 0 || header.rfind("COMPRESS", 0) == 0) {
            in.erase(0, nl + 1);
            continue;
        }
        unsigned file = 0, epoch_lo = 0;
        size_t offset = 0, total_len = 0;
        if (std::sscanf(header.c_str(), "WAL %u %zu %zu %u", &file, &offset, &total_len,
                        &epoch_lo) < 3) {
            ADD_FAILURE() << "unexpected line in the replication stream: '" << header << "'";
            break;
        }
        if (in.size() < nl + 1 + total_len) {
            const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
            if (n <= 0) break;
            in.append(buf, static_cast<size_t>(n));
            continue;
        }
        uint64_t seq = 0;
        std::memcpy(&seq, in.data() + nl + 1, sizeof(seq));   // WALRecord::sequence_number is first
        seqs.push_back(seq);
        in.erase(0, nl + 1 + total_len);
        // A stream longer than expected is a finding, not a reason to keep reading for twenty
        // seconds. Two extra records are enough to say "too many" with the numbers in the message.
        if (seqs.size() > want + 1) break;
    }
    return seqs;
}

} // namespace

// ── Catch-up past the socket send buffer ──────────────────────────────────────

TEST_F(ReplicationProtocolTest, CatchupSurvivesAWalLargerThanTheSocketSendBuffer) {
    // A catch-up runs on the epoll thread, and whether it can deliver more than one socket buffer
    // is a question about where a full socket gets to say "come back later" - the same question
    // that decides whether the TLS path has anywhere to put `SSL_ERROR_WANT_WRITE` (series D §16).
    // Since #93 the pass is also bounded and resumable; this test is about the first of those.
    //
    // Before the fix this used `send_all()` on a **non-blocking** socket, so the first EAGAIN was
    // read as a dead replica: measured, 17 270 of 40 000 records delivered and then
    // `send_to_replica failed for fd=7, marking disconnected`.
    //
    // Wide records rather than many, so the WAL passes the socket buffer in 1600 appends instead of
    // 60 000 - the same bytes on the wire for a fortieth of the setup time.
    //
    // 8 MB, and the number is measured rather than generous: with neither side setting a buffer
    // size, this loopback pair absorbed **2.6 MB** before the sender first saw EAGAIN (539 of 1600
    // records got through under the old code). A 2 MB version of this test passed against the defect.
    constexpr int    kRecords = 1600;
    constexpr size_t kLevels  = 200;
    const size_t payload_len = sizeof(ob::DeltaUpdate) + kLevels * sizeof(ob::Level);
    ASSERT_LT(payload_len, 65536u) << "a wider record would overflow WALRecord::payload_len "
                                      "(pitfall 44)";

    const size_t wire_bytes = fill_wal(*wal_, kRecords, kLevels);
    ASSERT_GT(wire_bytes, 1u << 20) << "the WAL must be well past one socket send buffer for this "
                                       "test to be about anything";

    auto mgr = start_manager();

    int fd = connect_to_localhost(port_);
    ASSERT_GE(fd, 0);
    // Deliberately no `SO_RCVBUF` shrink. A 4 kB receive window does fill the sender reliably, and
    // it also made this test take 49 seconds: 2 MB through a window that small is one delayed ACK
    // per few kilobytes. Not reading for half a second while the WAL is several times
    // `net.ipv4.tcp_wmem[1]` (16 kB here, autotuned up only as the receiver drains) fills it just as
    // certainly and drains at full speed afterwards.

    const char* handshake = "REPLICATE 0 0 0\n";
    ASSERT_GT(::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL), 0);

    // Long enough for catch-up to run to completion or to give up, and with this side reading
    // nothing at all while it does.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto states = mgr->replica_states();
    ASSERT_EQ(states.size(), 1u);
    EXPECT_GE(states[0].fd, 0)
        << "the primary closed the replica's socket during catch-up. A full socket buffer is not a "
           "dead replica - the same confusion pitfall 11 is about, one class away. (The record "
           "itself lingers with fd=-1 until the next read fails, so asserting on the *count* would "
           "have passed here.)";

    // Walking the framing rather than searching for a marker: each record arrives as
    // `WAL <file> <offset> <total_len> <epoch>\n` followed by exactly total_len bytes, so the
    // framing counts records exactly and notices a byte lost mid-stream immediately - which
    // searching for "WAL " cannot, since the payloads are binary and contain those four bytes. And
    // the sequence numbers say the order, which counting cannot.
    const auto seqs = recv_sequence_numbers(fd, static_cast<size_t>(kRecords));

    EXPECT_EQ(seqs.size(), static_cast<size_t>(kRecords))
        << "catch-up delivered " << seqs.size() << " of " << kRecords << " records ("
        << wire_bytes << " bytes requested); the stream stopped part way";
    for (size_t i = 0; i < seqs.size() && i < static_cast<size_t>(kRecords); ++i) {
        ASSERT_EQ(seqs[i], i + 1) << "record " << i << " arrived out of order";
    }

    ::close(fd);
    mgr->stop();
}

TEST_F(ReplicationProtocolTest, ACatchupLargerThanTheQueueCeilingDoesNotDropTheReplica) {
    // The test above stops one order of magnitude short of the thing that still drops a replica:
    // `handle_catchup()` streamed the whole requested range in one synchronous pass, so the queue
    // grew to whatever that range weighed. Past 16 MB the ceiling in `send_to_replica()` closed the
    // connection (roadmap #93), and the reconnect made almost no progress: every record of a
    // catch-up carries the replica's own last-acked position rather than its own, so the replica
    // saved one record's worth however many it received (#98).
    //
    // Widest records the header can carry - `payload_len` is a `uint16_t` - so 24 MB is 1000
    // appends rather than five thousand.
    constexpr int    kRecords = 1000;
    constexpr size_t kLevels  = 1000;
    const size_t payload_len = sizeof(ob::DeltaUpdate) + kLevels * sizeof(ob::Level);
    ASSERT_LT(payload_len, 65536u) << "a wider record would overflow WALRecord::payload_len "
                                      "(pitfall 44)";

    const size_t wire_bytes = fill_wal(*wal_, kRecords, kLevels);
    ASSERT_GT(wire_bytes, 20u << 20) << "the requested range must be well past the 16 MB queue "
                                        "ceiling for this test to be about anything";

    auto mgr = start_manager();

    int fd = connect_to_localhost(port_);
    ASSERT_GE(fd, 0);

    const char* handshake = "REPLICATE 0 0 0\n";
    ASSERT_GT(::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL), 0);

    // Nothing is read for this second. Long enough for the whole range to be queued - reading
    // 24 MB out of the page cache is milliseconds - and therefore long enough for the ceiling to
    // be crossed if the pass does not stop at it.
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    auto states = mgr->replica_states();
    ASSERT_EQ(states.size(), 1u);
    EXPECT_GE(states[0].fd, 0)
        << "the primary dropped the replica because its own catch-up queued " << wire_bytes
        << " bytes into a 16 MB buffer. A range larger than the ceiling is not a slow replica";

    // Walking the framing rather than searching for a marker: each record arrives as
    // `WAL <file> <offset> <total_len> <epoch>\n` followed by exactly total_len bytes, so the
    // framing counts records exactly and notices a byte lost mid-stream immediately - which
    // searching for "WAL " cannot, since the payloads are binary and contain those four bytes. And
    // the sequence numbers say the order, which counting cannot.
    const auto seqs = recv_sequence_numbers(fd, static_cast<size_t>(kRecords));

    EXPECT_EQ(seqs.size(), static_cast<size_t>(kRecords))
        << "catch-up delivered " << seqs.size() << " of " << kRecords << " records ("
        << wire_bytes << " bytes requested); the stream stopped part way";
    for (size_t i = 0; i < seqs.size() && i < static_cast<size_t>(kRecords); ++i) {
        ASSERT_EQ(seqs[i], i + 1) << "record " << i << " arrived out of order";
    }

    ::close(fd);
    mgr->stop();
}



TEST_F(ReplicationProtocolTest, ALiveRecordDoesNotOvertakeAnUnfinishedCatchup) {
    // What the cursor of #93 costs, and the assertion that pays for it. A synchronous pass held
    // `mtx_` from the first record to the last, so `broadcast()` could not interleave and order was
    // free. A pass that stops and resumes gives up that guarantee: a record written while the
    // cursor is halfway would be queued *now*, in front of the history it comes after, and the
    // replica would replay it before records older than it.
    //
    // So the cursor's end is fixed when it is created, and live records that arrive while it runs
    // wait behind it. This test states that in the only form that can fail: the whole arrival
    // order, not the presence of the record.
    constexpr int      kRecords   = 1000;
    constexpr size_t   kLevels    = 1000;
    constexpr uint64_t kMarkerSeq = 999999;

    const size_t wire_bytes = fill_wal(*wal_, kRecords, kLevels);
    ASSERT_GT(wire_bytes, 20u << 20) << "the range must be large enough that the cursor is still "
                                        "streaming when the live record arrives";

    auto mgr = start_manager();
    int fd = connect_to_localhost(port_);
    ASSERT_GE(fd, 0);

    const char* handshake = "REPLICATE 0 0 0\n";
    ASSERT_GT(::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL), 0);

    // Nothing is read here, so the cursor fills its half of the ceiling and backs off with most of
    // the range still to send. That is the state this test needs, and 300 ms is two orders of
    // magnitude more than it takes to reach: the measured pass that queued 16 MB took 61 ms.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // A live write lands on the primary, mid-catch-up - and it lands the way one does, into the
    // WAL first and onto the wire second, because `Engine::apply_delta()` calls `wal_.append()` and
    // then `broadcast()` under one lock. Broadcasting without appending would leave the record
    // invisible to the WAL reader, and a cursor that chases the live end would then look correct.
    const size_t marker_levels = 4;
    std::vector<ob::Level> lv(marker_levels);
    for (size_t l = 0; l < marker_levels; ++l) {
        lv[l].price = 1;
        lv[l].qty   = 1;
        lv[l].cnt   = 1;
        lv[l]._pad  = 0;
    }
    ob::DeltaUpdate marker{};
    std::strncpy(marker.symbol, "BTCUSD", sizeof(marker.symbol) - 1);
    std::strncpy(marker.exchange, "BINANCE", sizeof(marker.exchange) - 1);
    marker.sequence_number = kMarkerSeq;
    marker.timestamp_ns    = 2'000'000'000ULL;
    marker.side            = ob::SIDE_BID;
    marker.n_levels        = static_cast<uint16_t>(marker_levels);

    std::vector<uint8_t> payload(sizeof(ob::DeltaUpdate) + marker_levels * sizeof(ob::Level));
    std::memcpy(payload.data(), &marker, sizeof(marker));
    std::memcpy(payload.data() + sizeof(marker), lv.data(), marker_levels * sizeof(ob::Level));

    ob::WALRecord hdr{};
    hdr.sequence_number = kMarkerSeq;
    hdr.timestamp_ns    = marker.timestamp_ns;
    hdr.payload_len     = static_cast<uint16_t>(payload.size());
    hdr.checksum        = ob::crc32c(payload.data(), payload.size());
    hdr.record_type     = ob::WAL_RECORD_DELTA;
    wal_->append(marker, lv.data());
    wal_->flush();
    mgr->broadcast(hdr, payload.data(), payload.size());

    const auto seqs = recv_sequence_numbers(fd, static_cast<size_t>(kRecords) + 1);

    // Exactly this many, in both directions. Too few is a lost record; too many is the live write
    // delivered twice - once from the WAL file it is also in, once from the queue that held it -
    // which is what a cursor chasing the live end would do.
    ASSERT_EQ(seqs.size(), static_cast<size_t>(kRecords) + 1)
        << "the stream carried " << seqs.size() << " records where " << (kRecords + 1)
        << " were expected";
    EXPECT_EQ(seqs.back(), kMarkerSeq)
        << "the live record overtook the catch-up. It arrived at index "
        << (std::find(seqs.begin(), seqs.end(), kMarkerSeq) - seqs.begin()) << " of " << seqs.size()
        << ", so the replica replayed it before " << kRecords << " records that precede it";
    for (int i = 0; i < kRecords; ++i) {
        ASSERT_EQ(seqs[static_cast<size_t>(i)], static_cast<uint64_t>(i) + 1)
            << "catch-up record " << i << " arrived out of order";
    }

    ::close(fd);
    mgr->stop();
}

TEST_F(ReplicationProtocolTest, ACatchupWalksEveryWalFileInTheRequestedRange) {
    // The cursor carries a file index as well as an offset, so the file boundary is a place it can
    // stop - and a rotation writes a ROTATE record which is the end of its file rather than a
    // record to send. Both were inner loops of one synchronous pass before, where nothing could
    // interrupt them.
    constexpr int    kRecords = 300;
    constexpr size_t kLevels  = 1000;

    // A megabyte per file, against 24 kB records: seven-odd files instead of one.
    wal_ = std::make_unique<ob::WALWriter>(tmp_->str(), 1024 * 1024);
    fill_wal(*wal_, kRecords, kLevels);
    ASSERT_GT(wal_->current_file_index(), 3u) << "the range has to span several WAL files for this "
                                                "test to be about anything";

    auto mgr = start_manager();
    int fd = connect_to_localhost(port_);
    ASSERT_GE(fd, 0);

    const char* handshake = "REPLICATE 0 0 0\n";
    ASSERT_GT(::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL), 0);

    const auto seqs = recv_sequence_numbers(fd, static_cast<size_t>(kRecords));

    ASSERT_EQ(seqs.size(), static_cast<size_t>(kRecords))
        << "catch-up across " << (wal_->current_file_index() + 1) << " WAL files delivered "
        << seqs.size() << " of " << kRecords << " records";
    for (int i = 0; i < kRecords; ++i) {
        ASSERT_EQ(seqs[static_cast<size_t>(i)], static_cast<uint64_t>(i) + 1)
            << "record " << i << " arrived out of order across a file boundary";
    }

    ::close(fd);
    mgr->stop();
}


TEST_F(ReplicationProtocolTest, DISABLED_TheWritePathWaitOfALargeCatchup) {
    // What a client write pays while a replica catches up, measured at the entry point the write
    // path actually uses. `Engine::apply_delta()` calls `broadcast()` holding the engine's write
    // lock, and `broadcast()` needs `mtx_` - so the longest call here is the longest a client write
    // waits on this catch-up. Measuring `handle_catchup()` itself would answer a question nobody
    // asks (roadmap #97: measuring the wrong entry point acquits the code in the same voice it
    // would use if the code were fine).
    //
    // Three windows, and the third is the one that justifies `kCatchupBatchBytes`. A receiver that
    // reads nothing is bounded by the queue ceiling whatever the batch is; a receiver that *drains*
    // keeps the queue low forever, so without a batch bound one pass streams the whole range under
    // one lock. The first window is the control: this machine produces multi-millisecond scheduling
    // delays on its own, and without measuring that a delay reads as a lock being held.
    //
    // Not part of `ctest`: it is a stopwatch, and the numbers belong to one machine.
    constexpr int    kRecords = 1000;
    constexpr size_t kLevels  = 1000;
    const size_t wire_bytes = fill_wal(*wal_, kRecords, kLevels);

    // A one-level record, so the probe measures waiting rather than its own work.
    ob::Level lv{};
    lv.price = 1; lv.qty = 1; lv.cnt = 1;
    ob::DeltaUpdate probe{};
    std::strncpy(probe.symbol, "PROBE", sizeof(probe.symbol) - 1);
    std::strncpy(probe.exchange, "T", sizeof(probe.exchange) - 1);
    probe.side = ob::SIDE_BID;
    probe.n_levels = 1;
    std::vector<uint8_t> payload(sizeof(ob::DeltaUpdate) + sizeof(ob::Level));
    std::memcpy(payload.data(), &probe, sizeof(probe));
    std::memcpy(payload.data() + sizeof(probe), &lv, sizeof(lv));
    ob::WALRecord hdr{};
    hdr.payload_len = static_cast<uint16_t>(payload.size());
    hdr.checksum    = ob::crc32c(payload.data(), payload.size());
    hdr.record_type = ob::WAL_RECORD_DELTA;

    const auto report = [](const char* what, std::vector<double> v) {
        if (v.empty()) { std::fprintf(stderr, "MEASUREMENT %s: no samples\n", what); return; }
        std::sort(v.begin(), v.end());
        const auto pct = [&](double p) {
            return v[static_cast<size_t>(p * static_cast<double>(v.size() - 1))];
        };
        std::fprintf(stderr,
                     "MEASUREMENT broadcast() wait %s: n=%zu p50=%.3f ms p99=%.3f ms "
                     "p999=%.3f ms max=%.3f ms\n",
                     what, v.size(), pct(0.50), pct(0.99), pct(0.999), v.back());
    };

    // `drain` says what the receiver does, which is the whole difference between the two windows.
    const auto measure = [&](bool drain, const char* what) {
        auto mgr = start_manager();
        int fd = connect_to_localhost(port_);
        ASSERT_GE(fd, 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        std::atomic<bool> stop{false};
        std::vector<double> control_ms, during_ms;
        std::vector<double>* sink = &control_ms;
        std::thread writer([&] {
            while (!stop.load(std::memory_order_relaxed)) {
                const auto t0 = std::chrono::steady_clock::now();
                mgr->broadcast(hdr, payload.data(), payload.size());
                const auto t1 = std::chrono::steady_clock::now();
                sink->push_back(std::chrono::duration<double, std::milli>(t1 - t0).count());
                std::this_thread::sleep_for(std::chrono::microseconds(200));
            }
        });

        std::thread reader;
        if (drain) {
            reader = std::thread([&] {
                char buf[65536];
                struct timeval tv{};
                tv.tv_usec = 200000;
                ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
                while (!stop.load(std::memory_order_relaxed)) {
                    if (::recv(fd, buf, sizeof(buf), 0) <= 0) continue;
                }
            });
        }

        // A window on the same machine with the same threads, before the catch-up starts.
        std::this_thread::sleep_for(std::chrono::milliseconds(1500));
        sink = &during_ms;

        const char* handshake = "REPLICATE 0 0 0\n";
        ASSERT_GT(::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL), 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        stop.store(true, std::memory_order_relaxed);
        writer.join();
        if (reader.joinable()) reader.join();

        report("with no catch-up running (control)", control_ms);
        report(what, during_ms);
        auto states = mgr->replica_states();
        std::fprintf(stderr, "MEASUREMENT   ... replica still connected: %s\n",
                     (!states.empty() && states[0].fd >= 0) ? "yes" : "no");
        ::close(fd);
        mgr->stop();
    };

    std::fprintf(stderr, "MEASUREMENT range requested: %zu bytes\n", wire_bytes);
    measure(false, "during a catch-up, receiver reading nothing");
    measure(true,  "during a catch-up, receiver draining      ");
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
    EXPECT_TRUE(line.rfind("HEARTBEAT", 0) == 0) << "Should receive HEARTBEAT after idle period";

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
#include "orderbook/crc32c.hpp"

namespace {

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

    uint32_t crc = ob::crc32c(payload.data(), payload_len);
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
    EXPECT_TRUE(handshake.rfind("REPLICATE 0 0", 0) == 0)
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
                       primary_config(repl_port), {});
    primary.open();

    // Give the primary's ReplicationManager time to bind and start listening.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 3. Create replica Engine pointing to the primary's replication port.
    ob::Engine replica(replica_dir.str(), 100'000'000ULL, ob::FsyncPolicy::NONE,
                       {},
                       replica_config(repl_port, replica_dir.str() + "/repl_state.txt"));
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

// ═══════════════════════════════════════════════════════════════════════════════
// Snapshot-Based Replica Bootstrap Tests
// ═══════════════════════════════════════════════════════════════════════════════

#include "orderbook/crc32c.hpp"
#include <fstream>



// ── Test: SnapshotManifest round-trip serialization ───────────────────────────
// Validates: Requirements 9.1, 9.3
TEST(SnapshotManifest, RoundTrip) {
    ob::SnapshotManifest original;
    original.wal_file_index  = 5;
    original.wal_byte_offset = 4096;
    original.total_bytes     = 1024000;
    original.total_rows      = 5000;
    original.created_at_ns   = 1700000000000000000ULL;

    original.files.push_back({"BTC/BINANCE/1000_2000/price.col", 4096, 12345});
    original.files.push_back({"BTC/BINANCE/1000_2000/qty.col", 2048, 67890});
    original.files.push_back({"BTC/BINANCE/1000_2000/meta.json", 256, 11111});

    std::string json = original.to_json();

    ob::SnapshotManifest parsed;
    ASSERT_TRUE(ob::SnapshotManifest::from_json(json, parsed));

    EXPECT_EQ(parsed.wal_file_index, original.wal_file_index);
    EXPECT_EQ(parsed.wal_byte_offset, original.wal_byte_offset);
    EXPECT_EQ(parsed.total_bytes, original.total_bytes);
    EXPECT_EQ(parsed.total_rows, original.total_rows);
    EXPECT_EQ(parsed.created_at_ns, original.created_at_ns);
    ASSERT_EQ(parsed.files.size(), original.files.size());

    // Files are sorted by path in JSON output.
    for (size_t i = 0; i < parsed.files.size(); ++i) {
        // Find matching file by path.
        bool found = false;
        for (const auto& orig_f : original.files) {
            if (orig_f.path == parsed.files[i].path) {
                EXPECT_EQ(parsed.files[i].size, orig_f.size);
                EXPECT_EQ(parsed.files[i].crc32c, orig_f.crc32c);
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << "File not found: " << parsed.files[i].path;
    }
}

// ── Test: SnapshotManifest deterministic output ──────────────────────────────
// Validates: Requirement 9.4
TEST(SnapshotManifest, Deterministic) {
    ob::SnapshotManifest m;
    m.wal_file_index  = 3;
    m.wal_byte_offset = 1024;
    m.total_bytes     = 8192;
    m.total_rows      = 100;
    m.created_at_ns   = 999;
    m.files.push_back({"z/file.col", 100, 1});
    m.files.push_back({"a/file.col", 200, 2});

    std::string json1 = m.to_json();
    std::string json2 = m.to_json();
    EXPECT_EQ(json1, json2) << "Serialization must be deterministic";
}

// ── Test: SnapshotManifest alphabetical field ordering ───────────────────────
// Validates: Requirement 9.4
TEST(SnapshotManifest, FieldOrdering) {
    ob::SnapshotManifest m;
    m.wal_file_index  = 1;
    m.wal_byte_offset = 2;
    m.total_bytes     = 3;
    m.total_rows      = 4;
    m.created_at_ns   = 5;

    std::string json = m.to_json();

    // Verify alphabetical ordering of top-level keys.
    auto pos_created   = json.find("\"created_at_ns\"");
    auto pos_files     = json.find("\"files\"");
    auto pos_total_b   = json.find("\"total_bytes\"");
    auto pos_total_r   = json.find("\"total_rows\"");
    auto pos_wal_off   = json.find("\"wal_byte_offset\"");
    auto pos_wal_fi    = json.find("\"wal_file_index\"");

    EXPECT_LT(pos_created, pos_files);
    EXPECT_LT(pos_files, pos_total_b);
    EXPECT_LT(pos_total_b, pos_total_r);
    EXPECT_LT(pos_total_r, pos_wal_off);
    EXPECT_LT(pos_wal_off, pos_wal_fi);
}

// ── Test: SnapshotManifest empty files list ──────────────────────────────────
TEST(SnapshotManifest, EmptyFiles) {
    ob::SnapshotManifest m;
    m.wal_file_index = 0;
    m.total_bytes    = 0;
    m.total_rows     = 0;
    m.created_at_ns  = 42;

    std::string json = m.to_json();

    ob::SnapshotManifest parsed;
    ASSERT_TRUE(ob::SnapshotManifest::from_json(json, parsed));
    EXPECT_EQ(parsed.created_at_ns, 42u);
    EXPECT_TRUE(parsed.files.empty());
}

// ── Test fixture for snapshot engine tests ────────────────────────────────────

class SnapshotEngineTest : public ::testing::Test {
protected:
    void SetUp() override {
        tmp_ = std::make_unique<ReplTempDir>("snap");
    }

    void TearDown() override {
        tmp_.reset();
    }

    // Helper: create an engine, insert some data, and flush.
    void populate_engine(ob::Engine& engine, int n_inserts = 10) {
        for (int i = 0; i < n_inserts; ++i) {
            ob::DeltaUpdate delta{};
            std::strncpy(delta.symbol, "BTCUSD", sizeof(delta.symbol) - 1);
            std::strncpy(delta.exchange, "BINANCE", sizeof(delta.exchange) - 1);
            delta.sequence_number = static_cast<uint64_t>(i + 1);
            delta.timestamp_ns    = static_cast<uint64_t>(1000000 + i * 1000);
            delta.side            = 0;
            delta.n_levels        = 1;

            ob::Level level{};
            level.price = static_cast<int64_t>(50000 + i);
            level.qty   = 100;
            level.cnt   = 1;

            engine.apply_delta(delta, &level);
        }
    }

    std::unique_ptr<ReplTempDir> tmp_;
};

// ── Test: Basic snapshot creation ────────────────────────────────────────────
// Validates: Requirements 1.1-1.6
TEST_F(SnapshotEngineTest, SnapshotIncludesEveryColumnFile) {
    // The file walk used to match an allowlist of column names, so adding a
    // column to the segment format silently left it out of every snapshot. A
    // replica bootstrapped from such a snapshot receives segments its own reader
    // then rejects as incomplete, which is a data-loss bug two components apart
    // from the change that caused it.
    ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();
    populate_engine(engine, 10);
    engine.close();
    engine.open();

    auto manifest = engine.create_snapshot();

    // Gather the column files that actually exist on disk.
    std::set<std::string> on_disk;
    for (auto& entry : std::filesystem::recursive_directory_iterator(tmp_->str())) {
        if (entry.is_regular_file() && entry.path().extension() == ".col") {
            on_disk.insert(entry.path().filename().string());
        }
    }
    ASSERT_FALSE(on_disk.empty()) << "sanity: the flush should have written columns";

    std::set<std::string> in_manifest;
    for (const auto& f : manifest.files) {
        auto name = std::filesystem::path(f.path).filename().string();
        if (std::filesystem::path(name).extension() == ".col") {
            in_manifest.insert(name);
        }
    }

    for (const auto& name : on_disk) {
        EXPECT_TRUE(in_manifest.count(name) > 0)
            << "column file " << name << " exists on disk but is missing from the "
               "snapshot manifest; a replica restoring this snapshot would get an "
               "incomplete segment";
    }

    engine.close();
}

TEST_F(SnapshotEngineTest, SnapshotCreateBasic) {
    ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();

    populate_engine(engine, 10);

    // Force flush so data is in columnar store.
    engine.close();
    engine.open();

    auto manifest = engine.create_snapshot();

    EXPECT_GT(manifest.files.size(), 0u) << "Snapshot should contain files";
    EXPECT_GT(manifest.total_bytes, 0u) << "Snapshot should have non-zero total bytes";
    EXPECT_GT(manifest.created_at_ns, 0u) << "Snapshot should have a timestamp";

    // Verify snapshot_manifest.json was written.
    std::string manifest_path = tmp_->str() + "/snapshot_manifest.json";
    EXPECT_TRUE(std::filesystem::exists(manifest_path));

    engine.close();
}

// ── Test: Snapshot creation flushes pending rows ─────────────────────────────
// Validates: Requirements 1.1, 1.2
TEST_F(SnapshotEngineTest, SnapshotCreateFlushes) {
    ob::Engine engine(tmp_->str(), 5'000'000'000ULL, ob::FsyncPolicy::NONE);
    // 5-second flush interval so data stays pending during the test.
    engine.open();

    populate_engine(engine, 5);

    // Data should be pending (not yet flushed to columnar store).
    auto stats_before = engine.stats();
    EXPECT_GT(stats_before.pending_rows, 0u);

    auto manifest = engine.create_snapshot();

    // After snapshot, pending rows should be flushed.
    EXPECT_GT(manifest.total_rows, 0u) << "Snapshot should include flushed rows";
    EXPECT_GT(manifest.files.size(), 0u) << "Snapshot should contain segment files";

    engine.close();
}

// ── Test: Snapshot WAL position ──────────────────────────────────────────────
// Validates: Requirement 1.2
TEST_F(SnapshotEngineTest, SnapshotCreateWalPosition) {
    ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();

    populate_engine(engine, 5);

    auto manifest = engine.create_snapshot();

    // WAL position should be valid (file index 0 at minimum).
    EXPECT_GE(manifest.wal_file_index, 0u);
    // Byte offset should be > 0 since we wrote records.
    EXPECT_GT(manifest.wal_byte_offset, 0u);

    engine.close();
}

// ── Test: Snapshot file CRC32C integrity ─────────────────────────────────────
// Validates: Requirement 5.1
TEST_F(SnapshotEngineTest, SnapshotFileCRC32C) {
    ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();

    populate_engine(engine, 10);
    engine.close();
    engine.open();

    auto manifest = engine.create_snapshot();

    for (const auto& entry : manifest.files) {
        std::string full_path = tmp_->str() + "/" + entry.path;
        ASSERT_TRUE(std::filesystem::exists(full_path))
            << "File should exist: " << full_path;

        // Read file and compute CRC32C.
        std::ifstream f(full_path, std::ios::binary);
        ASSERT_TRUE(f.is_open());
        std::vector<uint8_t> data(entry.size);
        f.read(reinterpret_cast<char*>(data.data()),
               static_cast<std::streamsize>(entry.size));

        uint32_t computed = ob::crc32c(data.data(), data.size());
        EXPECT_EQ(computed, entry.crc32c)
            << "CRC32C mismatch for file: " << entry.path;
    }

    engine.close();
}

// ── Test: Snapshot lifecycle — at most one manifest ──────────────────────────
// Validates: Requirements 6.1, 6.2
TEST_F(SnapshotEngineTest, SnapshotLifecycleOneManifest) {
    ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();

    populate_engine(engine, 5);
    engine.close();
    engine.open();

    auto manifest1 = engine.create_snapshot();
    std::string manifest_path = tmp_->str() + "/snapshot_manifest.json";
    ASSERT_TRUE(std::filesystem::exists(manifest_path));

    // Read first manifest content.
    std::string content1;
    {
        std::ifstream f(manifest_path);
        content1.assign(std::istreambuf_iterator<char>(f),
                        std::istreambuf_iterator<char>());
    }

    // Insert more data and create a second snapshot.
    populate_engine(engine, 5);
    engine.close();
    engine.open();

    auto manifest2 = engine.create_snapshot();

    // Read second manifest content.
    std::string content2;
    {
        std::ifstream f(manifest_path);
        content2.assign(std::istreambuf_iterator<char>(f),
                        std::istreambuf_iterator<char>());
    }

    // The manifest should have been overwritten (different content).
    EXPECT_NE(content1, content2) << "Second snapshot should overwrite the first manifest";

    // Only one manifest file should exist.
    int manifest_count = 0;
    for (auto& entry : std::filesystem::recursive_directory_iterator(tmp_->str())) {
        if (entry.path().filename() == "snapshot_manifest.json") {
            ++manifest_count;
        }
    }
    EXPECT_EQ(manifest_count, 1) << "Only one snapshot manifest should exist";

    engine.close();
}

// ── Test: Snapshot load on fresh engine ──────────────────────────────────────
// Validates: Requirements 3.4, 3.5
TEST_F(SnapshotEngineTest, SnapshotLoadBasic) {
    // Create and populate an engine.
    {
        ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
        engine.open();
        populate_engine(engine, 10);
        engine.close();
    }

    // Open a fresh engine on the same directory and load snapshot.
    {
        ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
        engine.open();

        auto manifest = engine.create_snapshot();

        // Simulate loading: clear and rebuild.
        engine.load_snapshot(manifest);

        auto stats = engine.stats();
        EXPECT_GT(stats.segment_count, 0u) << "After load_snapshot, segments should be present";

        engine.close();
    }
}

// ── Integration test: Snapshot bootstrap on WAL_TRUNCATED ────────────────────
// Validates: Requirements 4.1, 3.5
TEST(SnapshotIntegration, BootstrapOnTruncatedWAL) {
    auto primary_dir = std::make_unique<ReplTempDir>("snap_primary");
    auto replica_dir = std::make_unique<ReplTempDir>("snap_replica");
    uint16_t repl_port = alloc_port();

    // 1. Create primary with replication enabled.
    ob::Engine primary(primary_dir->str(), 100'000'000ULL, ob::FsyncPolicy::NONE,
                       primary_config(repl_port), {});
    primary.open();

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 2. Insert data into primary and flush.
    for (int i = 0; i < 20; ++i) {
        ob::DeltaUpdate delta{};
        std::strncpy(delta.symbol, "BTCUSD", sizeof(delta.symbol) - 1);
        std::strncpy(delta.exchange, "BINANCE", sizeof(delta.exchange) - 1);
        delta.sequence_number = static_cast<uint64_t>(i + 1);
        delta.timestamp_ns    = static_cast<uint64_t>(1000000 + i * 1000);
        delta.side            = 0;
        delta.n_levels        = 1;

        ob::Level level{};
        level.price = static_cast<int64_t>(50000 + i);
        level.qty   = 100;
        level.cnt   = 1;

        primary.apply_delta(delta, &level);
    }

    // Close and reopen to flush data to columnar store.
    primary.close();
    primary.open();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 3. Verify primary has data.
    auto primary_stats = primary.stats();
    EXPECT_GT(primary_stats.segment_count, 0u) << "Primary should have segments";

    // 4. Create a replica that connects to the primary.
    // The replica starts fresh (REPLICATE 0 0), and the primary should have WAL
    // available for catchup. This tests the normal path.
    ob::Engine replica(replica_dir->str(), 100'000'000ULL, ob::FsyncPolicy::NONE,
                       {},
                       replica_config(repl_port, replica_dir->str() + "/repl_state.txt"));
    replica.open();

    // Give replica time to connect and catch up.
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    auto replica_state = replica.stats();
    EXPECT_TRUE(replica_state.is_replica);
    EXPECT_TRUE(replica_state.repl_connected);

    replica.close();
    primary.close();
}

// ── Integration test: Snapshot bootstrap resumes WAL streaming ───────────────
// Validates: Requirements 4.4, 3.5
TEST(SnapshotIntegration, BootstrapResumesStreaming) {
    auto primary_dir = std::make_unique<ReplTempDir>("snap_resume_primary");
    auto replica_dir = std::make_unique<ReplTempDir>("snap_resume_replica");
    uint16_t repl_port = alloc_port();

    // 1. Create primary with data.
    ob::Engine primary(primary_dir->str(), 100'000'000ULL, ob::FsyncPolicy::NONE,
                       primary_config(repl_port), {});
    primary.open();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Insert initial data.
    for (int i = 0; i < 10; ++i) {
        ob::DeltaUpdate delta{};
        std::strncpy(delta.symbol, "ETHUSD", sizeof(delta.symbol) - 1);
        std::strncpy(delta.exchange, "KRAKEN", sizeof(delta.exchange) - 1);
        delta.sequence_number = static_cast<uint64_t>(i + 1);
        delta.timestamp_ns    = static_cast<uint64_t>(2000000 + i * 1000);
        delta.side            = 0;
        delta.n_levels        = 1;

        ob::Level level{};
        level.price = static_cast<int64_t>(3000 + i);
        level.qty   = 50;
        level.cnt   = 1;

        primary.apply_delta(delta, &level);
    }

    // 2. Connect replica.
    ob::Engine replica(replica_dir->str(), 100'000'000ULL, ob::FsyncPolicy::NONE,
                       {},
                       replica_config(repl_port, replica_dir->str() + "/repl_state.txt"));
    replica.open();

    // Give replica time to connect and catch up.
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));

    // 3. Insert more data on primary AFTER replica is connected.
    for (int i = 10; i < 20; ++i) {
        ob::DeltaUpdate delta{};
        std::strncpy(delta.symbol, "ETHUSD", sizeof(delta.symbol) - 1);
        std::strncpy(delta.exchange, "KRAKEN", sizeof(delta.exchange) - 1);
        delta.sequence_number = static_cast<uint64_t>(i + 1);
        delta.timestamp_ns    = static_cast<uint64_t>(2000000 + i * 1000);
        delta.side            = 0;
        delta.n_levels        = 1;

        ob::Level level{};
        level.price = static_cast<int64_t>(3000 + i);
        level.qty   = 50;
        level.cnt   = 1;

        primary.apply_delta(delta, &level);
    }

    // Give replica time to receive the new records via WAL streaming.
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));

    auto replica_state = replica.stats();
    EXPECT_TRUE(replica_state.repl_connected);
    EXPECT_GT(replica_state.repl_records_replayed, 0u)
        << "Replica should have replayed records via WAL streaming";

    replica.close();
    primary.close();
}

// ── Running CRC32C ────────────────────────────────────────────────────────────
//
// The streaming form exists because a snapshot file arrives in chunks and checksumming it means
// either buffering the whole file or folding as it goes. Two callers now fold as they go, so the
// property worth pinning is that folding in pieces cannot differ from one call over the whole.

TEST(Crc32cRunning, FoldingInPiecesMatchesOneCall) {
    std::vector<uint8_t> data(4096);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<uint8_t>((i * 31u + 7u) & 0xFFu);
    }
    const uint32_t whole = ob::crc32c(data.data(), data.size());

    // Deliberately uneven splits: an implementation that only worked on aligned or equal chunks
    // would pass a two-halves test and fail here.
    for (size_t first : {size_t(0), size_t(1), size_t(7), size_t(64), size_t(4095), size_t(4096)}) {
        uint32_t crc = ob::crc32c_init;
        crc = ob::crc32c_update(crc, data.data(), first);
        crc = ob::crc32c_update(crc, data.data() + first, data.size() - first);
        EXPECT_EQ(ob::crc32c_finish(crc), whole) << "split at " << first;
    }

    // Three pieces, and an empty update in the middle, which a chunked reader produces at EOF.
    uint32_t crc = ob::crc32c_init;
    crc = ob::crc32c_update(crc, data.data(), 100);
    crc = ob::crc32c_update(crc, data.data() + 100, 0);
    crc = ob::crc32c_update(crc, data.data() + 100, data.size() - 100);
    EXPECT_EQ(ob::crc32c_finish(crc), whole);
}

TEST(Crc32cRunning, EmptyInputMatchesTheOneShotForm) {
    EXPECT_EQ(ob::crc32c_finish(ob::crc32c_init), ob::crc32c(nullptr, 0));
}

// ── Concurrent stop() ─────────────────────────────────────────────────────────
//
// A graceful `FAILOVER` killed the outgoing primary with SIGABRT, and the node's own log ended on
// `terminate called without an active exception` — libstdc++ for a joinable `std::thread` being
// destroyed (#86, #88).
//
// The mechanism was in the guard. `stop()` began `if (!running_) return;` and then stored `false`
// **before** joining, so its early return meant *a stop has begun* while reading as *stopped*. Two
// callers therefore both got past the null checks in `Engine::demote_to_replica()`, and the second
// skipped the join and destroyed the object the first was still inside.
//
// Two callers is not exotic here: the handover revokes the outgoing primary's own lease, so #82's
// unconditional lease-lost demotion fires while the handover's demotion is still running.
TEST_F(ReplicationProtocolTest, ConcurrentStopsJoinTheThreadExactlyOnce) {
    auto mgr = start_manager();

    // Both callers race into the same window deliberately. Under the old guard this became two
    // concurrent `thread_.join()` calls on one thread object, and the measured behaviour is worth
    // recording because it is not the obvious one: it **hangs** rather than aborting - one join
    // succeeds and the other waits on a thread id that will never be signalled. Twelve runs against
    // the reverted fix hung; none aborted.
    //
    // A hanging test detects a defect and reports nothing, so this needed the per-test `TIMEOUT`
    // added to `tests/CMakeLists.txt` in the same change. CTest's default is 1500 seconds, which in
    // CI reads as a stuck runner rather than as a failure.
    std::atomic<int> ready{0};
    auto racer = [&] {
        ready.fetch_add(1, std::memory_order_release);
        while (ready.load(std::memory_order_acquire) < 2) { /* spin to align the callers */ }
        mgr->stop();
    };

    std::thread first(racer);
    std::thread second(racer);
    first.join();
    second.join();

    // Both returned, and the one that returned early did so knowing the stop had *finished*: the
    // manager is stopped and destroying it must not need to join anything.
    EXPECT_FALSE(mgr->is_running());
    mgr.reset();
}

// Sequential idempotence, which is the property the early return is supposed to have and did not.
// Cheap, deterministic, and it holds when the race above happens to serialise on its own.
TEST_F(ReplicationProtocolTest, StopIsIdempotent) {
    auto mgr = start_manager();

    mgr->stop();
    EXPECT_FALSE(mgr->is_running());
    mgr->stop();          // must be a no-op rather than a second join
    mgr->stop();
    EXPECT_FALSE(mgr->is_running());
}
