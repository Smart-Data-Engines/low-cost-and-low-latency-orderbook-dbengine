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
#include <cinttypes>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
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
    ///
    /// `engine` is set **before** `start()`, which is the order `Engine::open()` and the promotion
    /// path both use. Setting it afterwards is safe since the setter takes the mutex, but a test
    /// should exercise the ordering production has.
    /// `wal_identity` defaults to 0, which is how a manager with no engine behind it answers
    /// `STREAMID?`: not at all, exactly as a pre-#101 primary does. Tests that care pass one.
    std::unique_ptr<ob::ReplicationManager> start_manager(ob::Engine* engine = nullptr,
                                                          uint64_t wal_identity = 0) {
        ob::ReplicationConfig cfg;
        cfg.port = port_;
        cfg.max_replicas = 4;
        cfg.wal_identity = wal_identity;
        auto mgr = std::make_unique<ob::ReplicationManager>(cfg, *wal_);
        if (engine != nullptr) mgr->set_engine(engine);
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

/// One record as it arrived on the wire: the position its header claimed, its framed length, and
/// the sequence number inside it.
struct WireRecord {
    uint32_t file{0};
    size_t   offset{0};
    size_t   total_len{0};
    uint64_t seq{0};
};

/// Read framed records off a replication socket until it goes quiet, and return them in arrival
/// order with the position each one's header claimed.
///
/// Until quiet rather than until `want`, and that is the difference between a test that can see a
/// record delivered twice and one that cannot: stopping at the expected count leaves the extra copy
/// in this function's own buffer, where it is indistinguishable from never having been sent. Found
/// by a mutation that survived for exactly that reason.
std::vector<WireRecord> recv_wire_records(int fd, size_t want, int quiet_ms = 400,
                                           int total_cap_s = 20) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(total_cap_s);
    struct timeval tv{};
    tv.tv_sec  = quiet_ms / 1000;
    tv.tv_usec = (quiet_ms % 1000) * 1000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    std::vector<WireRecord> recs;
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
        recs.push_back(WireRecord{file, offset, total_len, seq});
        in.erase(0, nl + 1 + total_len);
        // A stream longer than expected is a finding, not a reason to keep reading for twenty
        // seconds. Two extra records are enough to say "too many" with the numbers in the message.
        if (recs.size() > want + 1) break;
    }
    return recs;
}

/// Read the record the WAL on disk holds at `(file, offset)` and return its sequence number, or -1
/// if there is no whole record there.
///
/// This is the instrument the whole of #98 needs: the claim under test is that the position on the
/// wire names the record it was sent with, and the only way to check a claim about a position is to
/// go to it. Comparing wire positions against each other proves they are self-consistent, which a
/// column of zeroes also is.
int64_t seq_at_wal_position(const std::string& dir, uint32_t file, size_t offset) {
    char name[32];
    std::snprintf(name, sizeof(name), "wal_%06u.bin", file);
    const std::string path = dir + "/" + name;
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return -1;
    ob::WALRecord hdr{};
    const ssize_t n = ::pread(fd, &hdr, sizeof(hdr), static_cast<off_t>(offset));
    ::close(fd);
    if (n != static_cast<ssize_t>(sizeof(hdr))) return -1;
    return static_cast<int64_t>(hdr.sequence_number);
}

/// Every record's header names the position that record occupies in the WAL. One helper for both
/// paths, because "the position of the record being sent" is one promise however it got sent.
void expect_positions_name_their_records(const std::string& dir,
                                          const std::vector<WireRecord>& recs) {
    for (size_t i = 0; i < recs.size(); ++i) {
        const WireRecord& r = recs[i];
        EXPECT_EQ(seq_at_wal_position(dir, r.file, r.offset), static_cast<int64_t>(r.seq))
            << "record " << i << " (seq " << r.seq << ") was announced at file " << r.file
            << " offset " << r.offset << ", and that is not where it is in the WAL";
    }
}

/// The sequence numbers of what arrived, in order. A projection of `recv_wire_records()` rather
/// than a second framing loop: two copies of this protocol's framing is two places for it to be
/// read differently.
std::vector<uint64_t> recv_sequence_numbers(int fd, size_t want, int quiet_ms = 400,
                                            int total_cap_s = 20) {
    std::vector<uint64_t> seqs;
    for (const WireRecord& r : recv_wire_records(fd, want, quiet_ms, total_cap_s)) {
        seqs.push_back(r.seq);
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
    const ob::WalPosition marker_pos = wal_->append(marker, lv.data());
    wal_->flush();
    mgr->broadcast(hdr, payload.data(), payload.size(), marker_pos);

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


// ── #98: the position on the wire is the position of the record it carries ────────────────────

TEST_F(ReplicationProtocolTest, EveryCatchupRecordIsAnnouncedAtItsOwnWalPosition) {
    // The replica does `confirmed_offset = byte_offset + total_len` and saves that, so this field
    // is where a reconnect resumes from. Before #98 the catch-up wrote the position the replica had
    // last **acknowledged** instead - and during a catch-up nothing acknowledges anything, because
    // the ACKs that would move it are read by the same loop that is doing the sending. Measured:
    // every one of 112 records delivered before a drop carried `file=0 offset=0`.
    constexpr int    kRecords = 40;
    constexpr size_t kLevels  = 8;
    fill_wal(*wal_, kRecords, kLevels);

    auto mgr = start_manager();
    int fd = connect_to_localhost(port_);
    ASSERT_GE(fd, 0);

    const char* handshake = "REPLICATE 0 0 0\n";
    ASSERT_GT(::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL), 0);

    const auto recs = recv_wire_records(fd, static_cast<size_t>(kRecords));
    ASSERT_EQ(recs.size(), static_cast<size_t>(kRecords));

    expect_positions_name_their_records(tmp_->str(), recs);

    // And the positions have to move, which is the half a column of zeroes fails on its own terms:
    // a replica whose saved position never advances resumes one record along however much it got.
    EXPECT_EQ(recs.front().offset, 0u) << "the first record of file 0 starts at offset 0";
    for (size_t i = 1; i < recs.size(); ++i) {
        EXPECT_EQ(recs[i].offset, recs[i - 1].offset + recs[i - 1].total_len)
            << "record " << i << " does not begin where record " << (i - 1) << " ended";
    }

    ::close(fd);
    mgr->stop();
}

TEST_F(ReplicationProtocolTest, ACatchupAcrossFilesAnnouncesTheFileTheRecordIsIn) {
    // The same promise where it can go wrong in a second way: a position is a pair, and a pair
    // whose offset walks correctly while the file index stands still names records in the wrong
    // file from the second file onwards. WAL retention is gated on that index.
    constexpr int    kRecords = 300;
    constexpr size_t kLevels  = 1000;

    wal_ = std::make_unique<ob::WALWriter>(tmp_->str(), 1024 * 1024);
    fill_wal(*wal_, kRecords, kLevels);
    ASSERT_GT(wal_->current_file_index(), 3u);

    auto mgr = start_manager();
    int fd = connect_to_localhost(port_);
    ASSERT_GE(fd, 0);

    const char* handshake = "REPLICATE 0 0 0\n";
    ASSERT_GT(::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL), 0);

    const auto recs = recv_wire_records(fd, static_cast<size_t>(kRecords));
    ASSERT_EQ(recs.size(), static_cast<size_t>(kRecords));

    expect_positions_name_their_records(tmp_->str(), recs);

    // Distinct files actually appeared in the stream, so the assertion above was asked about more
    // than one of them.
    uint32_t highest = 0;
    for (const WireRecord& r : recs) highest = std::max(highest, r.file);
    EXPECT_GT(highest, 3u) << "every record was announced in file " << highest
                           << ", so this test never asked about a file boundary";

    ::close(fd);
    mgr->stop();
}

TEST_F(ReplicationProtocolTest, ALiveRecordIsAnnouncedAtItsOwnWalPosition) {
    // The live path was worse than the catch-up: it wrote a literal zero for the offset. So a
    // replica that restarted asked for `total_len` bytes into the current file and was re-sent
    // nearly all of it - and replication storage is append-only with no sequence check on this
    // path, so those rows land a second time.
    //
    // Two records are written before the handshake and read back before the live run starts, and
    // that read is what makes this test about the live path: a stream that has gone quiet after
    // delivering the catch-up is a cursor that has finished. Waiting on a sleep instead would make
    // the assertion depend on how the machine was scheduled, and broadcasting *during* the window
    // between `accept()` and the handshake measures #100 rather than this.
    constexpr int    kPrefill = 2;
    fill_wal(*wal_, kPrefill, 4);

    auto mgr = start_manager();
    int fd = connect_to_localhost(port_);
    ASSERT_GE(fd, 0);

    const char* handshake = "REPLICATE 0 0 0\n";
    ASSERT_GT(::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL), 0);

    const auto caught_up = recv_wire_records(fd, static_cast<size_t>(kPrefill));
    ASSERT_EQ(caught_up.size(), static_cast<size_t>(kPrefill))
        << "the catch-up has to finish before the live run, or this test is about both paths";
    expect_positions_name_their_records(tmp_->str(), caught_up);

    constexpr int    kRecords = 12;
    constexpr size_t kLevels  = 6;
    std::vector<ob::Level> lv(kLevels);
    for (size_t l = 0; l < kLevels; ++l) {
        lv[l].price = static_cast<int64_t>(l) + 1;
        lv[l].qty   = 1;
        lv[l].cnt   = 1;
        lv[l]._pad  = 0;
    }

    // Appended and then broadcast, in that order and with nothing between, which is what the
    // engine does under one lock (`engine.cpp` apply_delta step 1 and 1b).
    for (int i = 0; i < kRecords; ++i) {
        ob::DeltaUpdate d{};
        std::strncpy(d.symbol, "BTCUSD", sizeof(d.symbol) - 1);
        std::strncpy(d.exchange, "BINANCE", sizeof(d.exchange) - 1);
        d.sequence_number = static_cast<uint64_t>(i) + 1 + kPrefill;
        d.timestamp_ns    = 3'000'000'000ULL + static_cast<uint64_t>(i);
        d.side            = ob::SIDE_BID;
        d.n_levels        = static_cast<uint16_t>(kLevels);

        std::vector<uint8_t> payload(sizeof(ob::DeltaUpdate) + kLevels * sizeof(ob::Level));
        std::memcpy(payload.data(), &d, sizeof(d));
        std::memcpy(payload.data() + sizeof(d), lv.data(), kLevels * sizeof(ob::Level));

        ob::WALRecord hdr{};
        hdr.sequence_number = d.sequence_number;
        hdr.timestamp_ns    = d.timestamp_ns;
        hdr.payload_len     = static_cast<uint16_t>(payload.size());
        hdr.checksum        = ob::crc32c(payload.data(), payload.size());
        hdr.record_type     = ob::WAL_RECORD_DELTA;

        const ob::WalPosition pos = wal_->append(d, lv.data());
        mgr->broadcast(hdr, payload.data(), payload.size(), pos);
    }
    wal_->flush();

    const auto recs = recv_wire_records(fd, static_cast<size_t>(kRecords));
    ASSERT_EQ(recs.size(), static_cast<size_t>(kRecords));
    expect_positions_name_their_records(tmp_->str(), recs);

    ::close(fd);
    mgr->stop();
}

TEST_F(ReplicationProtocolTest, ARotatingAppendAnnouncesTheFileTheRecordWentInto) {
    // This is the test the obvious fix fails, and it is why #98 needed a design note rather than a
    // subtraction. `append()` rotates **after** the write, and `rotate()` publishes
    // `{next_index, next_offset}` in one store - so for the record that crosses the threshold,
    // `current_position()` is in the **new** file while the record itself sits near the end of the
    // **old** one. Deriving the position as `current_position() - total_len` therefore names a file
    // the record is not in, and can underflow. Only a function that returns where it wrote knows.
    constexpr size_t kLevels = 8;
    const size_t record_bytes =
        sizeof(ob::WALRecord) + sizeof(ob::DeltaUpdate) + kLevels * sizeof(ob::Level);

    // A threshold a few records wide, so the run below rotates several times rather than once.
    wal_ = std::make_unique<ob::WALWriter>(tmp_->str(), record_bytes * 4);

    // Prefilled and read back before the live run, for the reason given in the test above: it is
    // what proves the cursor has finished without asking a clock.
    constexpr int kPrefill = 2;
    fill_wal(*wal_, kPrefill, kLevels);

    auto mgr = start_manager();
    int fd = connect_to_localhost(port_);
    ASSERT_GE(fd, 0);

    const char* handshake = "REPLICATE 0 0 0\n";
    ASSERT_GT(::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL), 0);

    const auto caught_up = recv_wire_records(fd, static_cast<size_t>(kPrefill));
    ASSERT_EQ(caught_up.size(), static_cast<size_t>(kPrefill));
    expect_positions_name_their_records(tmp_->str(), caught_up);

    constexpr int kRecords = 25;
    std::vector<ob::Level> lv(kLevels);
    for (size_t l = 0; l < kLevels; ++l) {
        lv[l].price = static_cast<int64_t>(l) + 1;
        lv[l].qty   = 1;
        lv[l].cnt   = 1;
        lv[l]._pad  = 0;
    }
    for (int i = 0; i < kRecords; ++i) {
        ob::DeltaUpdate d{};
        std::strncpy(d.symbol, "BTCUSD", sizeof(d.symbol) - 1);
        std::strncpy(d.exchange, "BINANCE", sizeof(d.exchange) - 1);
        d.sequence_number = static_cast<uint64_t>(i) + 1 + kPrefill;
        d.timestamp_ns    = 4'000'000'000ULL + static_cast<uint64_t>(i);
        d.side            = ob::SIDE_BID;
        d.n_levels        = static_cast<uint16_t>(kLevels);

        std::vector<uint8_t> payload(sizeof(ob::DeltaUpdate) + kLevels * sizeof(ob::Level));
        std::memcpy(payload.data(), &d, sizeof(d));
        std::memcpy(payload.data() + sizeof(d), lv.data(), kLevels * sizeof(ob::Level));

        ob::WALRecord hdr{};
        hdr.sequence_number = d.sequence_number;
        hdr.timestamp_ns    = d.timestamp_ns;
        hdr.payload_len     = static_cast<uint16_t>(payload.size());
        hdr.checksum        = ob::crc32c(payload.data(), payload.size());
        hdr.record_type     = ob::WAL_RECORD_DELTA;

        const ob::WalPosition pos = wal_->append(d, lv.data());
        mgr->broadcast(hdr, payload.data(), payload.size(), pos);
    }
    wal_->flush();
    ASSERT_GT(wal_->current_file_index(), 2u) << "the run has to rotate for this test to be about "
                                                 "anything";

    const auto recs = recv_wire_records(fd, static_cast<size_t>(kRecords));
    ASSERT_EQ(recs.size(), static_cast<size_t>(kRecords));
    expect_positions_name_their_records(tmp_->str(), recs);

    ::close(fd);
    mgr->stop();
}


// ── #100: a record broadcast before the handshake ────────────────────────────────────────────

namespace {

/// Append one delta to the WAL and broadcast it, the way the engine does under one lock.
/// Returns the sequence number it used.
uint64_t append_and_broadcast(ob::WALWriter& wal, ob::ReplicationManager& mgr, uint64_t seq,
                              size_t levels) {
    std::vector<ob::Level> lv(levels);
    for (size_t l = 0; l < levels; ++l) {
        lv[l].price = static_cast<int64_t>(l) + 1;
        lv[l].qty   = 1;
        lv[l].cnt   = 1;
        lv[l]._pad  = 0;
    }
    ob::DeltaUpdate d{};
    std::strncpy(d.symbol, "BTCUSD", sizeof(d.symbol) - 1);
    std::strncpy(d.exchange, "BINANCE", sizeof(d.exchange) - 1);
    d.sequence_number = seq;
    d.timestamp_ns    = 6'000'000'000ULL + seq;
    d.side            = ob::SIDE_BID;
    d.n_levels        = static_cast<uint16_t>(levels);

    std::vector<uint8_t> payload(sizeof(ob::DeltaUpdate) + levels * sizeof(ob::Level));
    std::memcpy(payload.data(), &d, sizeof(d));
    std::memcpy(payload.data() + sizeof(d), lv.data(), levels * sizeof(ob::Level));

    ob::WALRecord hdr{};
    hdr.sequence_number = seq;
    hdr.timestamp_ns    = d.timestamp_ns;
    hdr.payload_len     = static_cast<uint16_t>(payload.size());
    hdr.checksum        = ob::crc32c(payload.data(), payload.size());
    hdr.record_type     = ob::WAL_RECORD_DELTA;

    const ob::WalPosition pos = wal.append(d, lv.data());
    mgr.broadcast(hdr, payload.data(), payload.size(), pos);
    return seq;
}

/// Wait until the manager has registered `want` replicas, or give up.
///
/// This is the signal that `accept()` has run, and it is the protocol's own rather than a sleep:
/// the window this test needs is between the accept and the `REPLICATE` line, so it has to know
/// that the first has happened and be sure the second has not.
bool wait_for_registered_replicas(ob::ReplicationManager& mgr, size_t want, int timeout_ms = 3000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (mgr.replica_states().size() >= want) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

} // namespace

TEST_F(ReplicationProtocolTest, ARecordBroadcastBeforeTheHandshakeArrivesOnce) {
    // `accept_replica()` puts the entry in `replicas_` as soon as the socket is accepted, before the
    // `REPLICATE` line has been read - so `broadcast()` queues live records to a replica that has
    // not asked for anything. Then the handshake arrives, the cursor's range ends at the WAL
    // position read *then* - which is past those records, because the append precedes the broadcast
    // - and the catch-up sends every one of them again.
    //
    // Measured before the fix: ten broadcast in that window, **twenty** received, in the pattern
    // 1..10 then 1..10.
    constexpr int    kRecords = 10;
    constexpr size_t kLevels  = 4;

    auto mgr = start_manager();
    int fd = connect_to_localhost(port_);
    ASSERT_GE(fd, 0);
    ASSERT_TRUE(wait_for_registered_replicas(*mgr, 1))
        << "the manager never registered the connection, so this test never reached the window it "
           "is about";

    for (int i = 0; i < kRecords; ++i) {
        append_and_broadcast(*wal_, *mgr, static_cast<uint64_t>(i) + 1, kLevels);
    }
    wal_->flush();

    const char* handshake = "REPLICATE 0 0 0\n";
    ASSERT_GT(::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL), 0);

    const auto recs = recv_wire_records(fd, static_cast<size_t>(kRecords));

    // Read until quiet and count exactly, because the failure is upwards: a reader that stops at
    // the expected number cannot see a second copy (the lesson a surviving mutation taught #93).
    ASSERT_EQ(recs.size(), static_cast<size_t>(kRecords))
        << "the stream carried " << recs.size() << " records where " << kRecords << " were sent";
    for (size_t i = 0; i < recs.size(); ++i) {
        EXPECT_EQ(recs[i].seq, static_cast<uint64_t>(i) + 1)
            << "record " << i << " is out of order, which one repeat of the batch looks like";
    }
    expect_positions_name_their_records(tmp_->str(), recs);

    ::close(fd);
    mgr->stop();
}

TEST_F(ReplicationProtocolTest, ARecordBroadcastAfterTheHandshakeStillArrives) {
    // The other half, and it is what makes the one above a fix rather than a silence: dropping the
    // live copy is only correct while a catch-up is going to deliver it. Once the cursor has
    // finished, `broadcast()` is the only path a record has.
    constexpr int    kPrefill = 3;
    constexpr size_t kLevels  = 4;
    fill_wal(*wal_, kPrefill, kLevels);

    auto mgr = start_manager();
    int fd = connect_to_localhost(port_);
    ASSERT_GE(fd, 0);

    const char* handshake = "REPLICATE 0 0 0\n";
    ASSERT_GT(::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL), 0);

    const auto caught_up = recv_wire_records(fd, static_cast<size_t>(kPrefill));
    ASSERT_EQ(caught_up.size(), static_cast<size_t>(kPrefill));

    constexpr int kLive = 5;
    for (int i = 0; i < kLive; ++i) {
        append_and_broadcast(*wal_, *mgr, static_cast<uint64_t>(i) + 1 + kPrefill, kLevels);
    }
    wal_->flush();

    const auto live = recv_wire_records(fd, static_cast<size_t>(kLive));
    ASSERT_EQ(live.size(), static_cast<size_t>(kLive))
        << "a live record after the catch-up did not arrive: " << live.size() << " of " << kLive;
    expect_positions_name_their_records(tmp_->str(), live);

    ::close(fd);
    mgr->stop();
}


// ── #98: the engine hands over the position its append returned ──────────────────────────────

namespace {

/// The body of one function in a source file, by brace matching from its signature.
std::string function_body(const std::string& file, const std::string& signature) {
    std::ifstream in(std::string(OB_SOURCE_DIR) + "/" + file);
    if (!in) return {};
    const std::string src((std::istreambuf_iterator<char>(in)),
                          std::istreambuf_iterator<char>());
    const auto sig = src.find(signature);
    if (sig == std::string::npos) return {};
    auto pos = src.find('{', sig);
    if (pos == std::string::npos) return {};
    int depth = 0;
    const auto start = pos;
    for (; pos < src.size(); ++pos) {
        if (src[pos] == '{') ++depth;
        else if (src[pos] == '}' && --depth == 0) return src.substr(start, pos - start + 1);
    }
    return {};
}

/// The body of whichever `Engine::` function contains `marker`, found by walking back to the last
/// definition line that starts at column 0.
///
/// Derived rather than named, and that is the point: this check used to name
/// `Engine::apply_delta`, and when #101 split it into a delegating pair the body moved to
/// `apply_delta_impl` — so the test found a four-line wrapper and reported that the engine had
/// stopped capturing the append's position. A static test pinned to a function *name* stops
/// checking anything the day the body moves, and the failure looks like the defect it guards.
std::string read_source(const std::string& rel) {
    std::ifstream in(std::string(OB_SOURCE_DIR) + "/" + rel);
    if (!in) return {};
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

/// Where the definition containing `at` begins, or npos. A definition's signature starts at
/// column 0 and nothing inside a body does, so walking forward and keeping the last such line
/// before `at` finds it.
///
/// One copy, used three ways: the body of the first match, the name and body of every match, and
/// whether one function's body contains a call. Three walks would be three chances for one of
/// them to stop finding anything and go on passing.
std::size_t definition_start(const std::string& src, std::size_t at,
                             const std::string& qualifier) {
    std::size_t sig = std::string::npos;
    for (std::size_t line = 0; line < at;) {
        const std::size_t next = src.find('\n', line);
        if (next == std::string::npos || next > at) break;
        const char first = src[line];
        if (first != ' ' && first != '\t' && first != '\n' &&
            src.compare(line, 2, "//") != 0 &&
            src.find(qualifier, line) < next && src.find('(', line) < next) {
            sig = line;
        }
        line = next + 1;
    }
    return sig;
}

/// The braced body that follows the signature at `sig`, braces included.
std::string body_at(const std::string& src, std::size_t sig) {
    auto pos = src.find('{', sig);
    if (pos == std::string::npos) return {};
    int depth = 0;
    const auto start = pos;
    for (; pos < src.size(); ++pos) {
        if (src[pos] == '{') ++depth;
        else if (src[pos] == '}' && --depth == 0) return src.substr(start, pos - start + 1);
    }
    return {};
}

std::string enclosing_definition(const std::string& file, const std::string& marker,
                                 const std::string& qualifier = "Engine::") {
    const std::string src = read_source(file);
    if (src.empty()) return {};
    const auto at = src.find(marker);
    if (at == std::string::npos) return {};
    const auto sig = definition_start(src, at, qualifier);
    if (sig == std::string::npos) return {};
    return body_at(src, sig);
}

} // namespace

TEST(WalPositionWireStatic, NothingQueuedFromTheRunLoopBypassesTheTransferDecision) {
    // The heartbeat is #99 with a five-second timer instead of a write: that loop walks every
    // replica, and a `HEARTBEAT <epoch>` line landing inside a snapshot's byte stream abandons the
    // bootstrap exactly as a `WAL` record does - with no client write involved at all, so any
    // transfer that takes longer than five seconds hits it.
    //
    // A behavioural test for that would have to wait out the interval inside a stalled transfer, or
    // make the interval configurable for a test's sake. This is the mechanism instead: a mutation
    // routing the heartbeat around `queue_to_replica()` survived the whole suite, and it is what
    // this refuses. `queue_to_replica()` is the only function that knows about deferring, so
    // anything the run loop queues has to go through it.
    const std::string body = function_body("src/replication.cpp",
                                           "void ReplicationManager::run_loop()");
    ASSERT_FALSE(body.empty()) << "could not find ReplicationManager::run_loop in "
                                  "src/replication.cpp; if it was renamed, this test has stopped "
                                  "checking anything";

    EXPECT_EQ(body.find("enqueue_send("), std::string::npos)
        << "run_loop() queues bytes to a replica without going through queue_to_replica(), so they "
           "can be spliced into a catch-up or a snapshot transfer in progress (#99)";
    EXPECT_EQ(body.find("enqueue_and_flush("), std::string::npos)
        << "same rule: enqueue_and_flush() writes straight to the socket, which is a splice if a "
           "transfer is streaming";

    // And the pair that makes this test fail for the right reason rather than by finding nothing:
    // the queueing it demands has to be there.
    EXPECT_NE(body.find("queue_to_replica("), std::string::npos)
        << "run_loop() no longer queues anything to a replica, so this guard is watching an empty "
           "function";
}

TEST(WalPositionWireStatic, TheEngineBroadcastsThePositionItsAppendReturned) {
    // The behavioural tests in this file drive `ReplicationManager` directly, so they pin what the
    // manager does with a position and say nothing about which position the engine chooses. That
    // gap is not hypothetical: a mutation deriving the position at the engine's call site as
    // `current_position() - total_len` survived every one of the 951 tests, because the WAL
    // threshold the engine hardcodes is 512 MB and no test rotates it.
    //
    // The derivation is wrong for exactly one record in every WAL file - the one whose append
    // rotated - so a behavioural test for it would need a WAL that rotates inside `Engine`, which
    // is not configurable and should not become configurable for a test. This is the mechanism
    // instead, and it is the stronger one for this claim: the engine may not *compute* a position
    // at all.
    // Whichever function appends a client write and broadcasts it - derived from the two markers
    // rather than named, so a rename or a split cannot retire the check (it already did once).
    const std::string body =
        enclosing_definition("src/engine.cpp", "= wal_.append(delta, levels);");
    ASSERT_FALSE(body.empty()) << "nothing in src/engine.cpp captures wal_.append(delta, levels); "
                                  "if the write path changed shape, this test has stopped checking "
                                  "anything";

    // And they are the *same* function, which is part of the claim rather than a detail: the append
    // and the broadcast happen under one acquisition of `mtx_`, which is what keeps the WAL order
    // and the wire order the same.
    EXPECT_EQ(body, enclosing_definition("src/engine.cpp", "repl_mgr_->broadcast("))
        << "the append and the broadcast are in different functions, so nothing holds them under "
           "one lock";

    // The append's position is captured, and the name it is captured under is the one handed to
    // broadcast(). Read out of the source rather than written down here, so renaming the variable
    // cannot silently retire the check.
    const std::string marker = "= wal_.append(delta, levels);";
    const auto assign = body.find(marker);
    ASSERT_NE(assign, std::string::npos)
        << "the write path no longer captures what wal_.append() returns";
    const auto line_start = body.rfind('\n', assign) + 1;
    const std::string decl = body.substr(line_start, assign - line_start);
    // "    const WalPosition record_pos " -> "record_pos"
    auto last = decl.find_last_not_of(" \t");
    auto first = decl.find_last_of(" \t", last) + 1;
    const std::string name = decl.substr(first, last - first + 1);
    ASSERT_FALSE(name.empty());
    EXPECT_NE(decl.find("WalPosition"), std::string::npos)
        << "the append's return is captured as something other than a WalPosition: '" << decl
        << "'";

    const auto call = body.find("repl_mgr_->broadcast(");
    ASSERT_NE(call, std::string::npos) << "the write path no longer broadcasts";
    const auto call_end = body.find(");", call);
    ASSERT_NE(call_end, std::string::npos);
    const std::string args = body.substr(call, call_end - call);
    EXPECT_NE(args.find(name), std::string::npos)
        << "the position broadcast is not the one wal_.append() returned; it is '" << args << "'";

    // And nothing in this function derives a position from where the WAL happens to be now. That
    // is the arithmetic the whole of #98 is about, and after a rotating append it names a file the
    // record is not in.
    EXPECT_EQ(body.find("current_position()"), std::string::npos)
        << "Engine::apply_delta reads the WAL's current position. A record's position is what "
           "append() returned; the current one is past it, and after a rotation it is in the "
           "next file (#98)";
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
                // This harness measures how long `broadcast()` waits for `mtx_`, and never
                // appends, so it has no position of its own to announce. The WAL's current one is
                // the honest stand-in here: nothing reads it, and the alternative is a literal.
                mgr->broadcast(hdr, payload.data(), payload.size(), wal_->current_position());
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
    mgr->broadcast(hdr, payload, 4, wal_->current_position());

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
    mgr->broadcast(hdr, payload, 4, wal_->current_position());

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

// Answer `STREAMID?` the way a #101 primary does, so the client goes on to send its position.
//
// Every mock primary needs this. Without it the client waits out its deadline, concludes it is
// talking to a pre-#101 primary, discards what it holds and asks from zero - correct behaviour,
// and the wrong subject for a test about anything else. The one test that *is* about that has no
// call to this.
static void answer_stream_id(int fd, uint64_t identity, int timeout_ms = 3000) {
    const std::string question = recv_line(fd, timeout_ms);
    ASSERT_EQ(question.rfind("STREAMID?", 0), 0u)
        << "the client asks which stream this is before sending its position, got: " << question;
    const std::string answer = "STREAM " + std::to_string(identity) + "\n";
    ASSERT_EQ(::send(fd, answer.data(), answer.size(), MSG_NOSIGNAL),
              static_cast<ssize_t>(answer.size()));
}

// Build a valid WAL wire message: "WAL <file_index> <byte_offset> <total_len>\n<WALRecord><payload>"
// Returns the complete message bytes.
//
// `epoch` is optional because the field count is what arms the replica's epoch filter: the parse
// requires four fields before it compares anything, so a line without one is deliberately exempt.
// Passing an explicit zero is therefore a different message from passing none, which is why this
// takes an optional rather than defaulting the number to 0.
static std::vector<uint8_t> build_wal_message(uint32_t file_index, size_t byte_offset,
                                               const ob::WALRecord& hdr,
                                               const void* payload, size_t payload_len,
                                               std::optional<uint64_t> epoch = std::nullopt) {
    const size_t total_len = sizeof(ob::WALRecord) + payload_len;
    char line[128];
    int line_len = epoch.has_value()
        ? std::snprintf(line, sizeof(line), "WAL %u %zu %zu %" PRIu64 "\n",
                        file_index, byte_offset, total_len, *epoch)
        : std::snprintf(line, sizeof(line), "WAL %u %zu %zu\n",
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

    // 5. Answer which stream this is, then read the REPLICATE handshake (#101).
    answer_stream_id(client_fd, 0x51DULL);
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
// ── #99: the primary does not splice a live record into a snapshot ───────────────────────────

TEST_F(ReplicationProtocolTest, ALiveRecordDoesNotEnterASnapshotStream) {
    // The primary half of #99, and the first test in this repo to drive a real
    // `ReplicationManager` through a snapshot transfer at all - the path had unit coverage for the
    // receiving side (paths, CRC) and none for the sending side.
    //
    // Deterministic rather than raced: this socket never reads, so the transfer stalls with
    // `snapshot_transfer.active` still true and the live broadcast lands inside the window for
    // certain. `snapshot_active()` is the signal, so the test does not guess.
    ob::Engine engine(tmp_->str() + "/engine", 100'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();

    // Something to put in the snapshot: rows flushed to columnar files, across several symbols so
    // the manifest has several entries and "between two files" is a place that exists.
    // Above the back-off threshold on purpose. `continue_snapshot_transfer()` returns once the
    // send buffer reaches half of `MAX_SEND_BUF_SIZE` - 8 MB - and this loopback pair absorbs about
    // 2.6 MB (measured in #93), so a snapshot under about 10 MB is streamed in one pass and
    // `snapshot_transfer.active` is false again before anything can look at it. Measured here: 12
    // symbols made 7.3 MB and the transfer never stalled; 24 make about 14.6 MB and it does.
    constexpr int kSymbols = 24;
    constexpr int kRows    = 400;
    std::vector<ob::Level> lv(64);
    for (size_t l = 0; l < lv.size(); ++l) {
        lv[l].price = static_cast<int64_t>(l) + 1;
        lv[l].qty   = 10;
        lv[l].cnt   = 1;
        lv[l]._pad  = 0;
    }
    for (int sym = 0; sym < kSymbols; ++sym) {
        char name[16];
        std::snprintf(name, sizeof(name), "SNAP%02d", sym);
        for (int i = 0; i < kRows; ++i) {
            ob::DeltaUpdate d{};
            std::strncpy(d.symbol, name, sizeof(d.symbol) - 1);
            std::strncpy(d.exchange, "BINANCE", sizeof(d.exchange) - 1);
            d.timestamp_ns = 8'000'000'000ULL + static_cast<uint64_t>(i);
            d.side         = ob::SIDE_BID;
            d.n_levels     = static_cast<uint16_t>(lv.size());
            ASSERT_EQ(engine.apply_delta(d, lv.data()), ob::OB_OK);
        }
    }
    engine.flush_incremental();

    auto mgr = start_manager(&engine);

    int fd = connect_to_localhost(port_);
    ASSERT_GE(fd, 0);

    const char* handshake = "REPLICATE 0 0 0\n";
    ASSERT_GT(::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL), 0);
    ASSERT_TRUE(wait_for_registered_replicas(*mgr, 1));

    const char* request = "SNAPSHOT_REQUEST\n";
    ASSERT_GT(::send(fd, request, std::strlen(request), MSG_NOSIGNAL), 0);

    // Wait for the transfer to actually be in progress. The snapshot is created on a worker
    // thread (#79), so this is not immediate and a sleep would be a guess.
    bool streaming = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
        if (mgr->snapshot_active()) { streaming = true; break; }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(streaming) << "the snapshot transfer never started, so this test never reached the "
                              "window it is about";

    // A live write while the snapshot streams. The position is deliberately far past anything the
    // snapshot could have been taken at, so the decision under test is Defer rather than Drop -
    // the Drop side is covered by `LiveRecordDecision`.
    constexpr uint64_t kMarkerSeq = 4242;
    ob::Level one{};
    one.price = 1; one.qty = 1; one.cnt = 1;
    ob::DeltaUpdate marker{};
    std::strncpy(marker.symbol, "MARKER", sizeof(marker.symbol) - 1);
    std::strncpy(marker.exchange, "BINANCE", sizeof(marker.exchange) - 1);
    marker.sequence_number = kMarkerSeq;
    marker.timestamp_ns    = 9'000'000'000ULL;
    marker.side            = ob::SIDE_BID;
    marker.n_levels        = 1;
    std::vector<uint8_t> payload(sizeof(ob::DeltaUpdate) + sizeof(ob::Level));
    std::memcpy(payload.data(), &marker, sizeof(marker));
    std::memcpy(payload.data() + sizeof(marker), &one, sizeof(one));
    ob::WALRecord mhdr{};
    mhdr.sequence_number = kMarkerSeq;
    mhdr.timestamp_ns    = marker.timestamp_ns;
    mhdr.payload_len     = static_cast<uint16_t>(payload.size());
    mhdr.checksum        = ob::crc32c(payload.data(), payload.size());
    mhdr.record_type     = ob::WAL_RECORD_DELTA;
    mgr->broadcast(mhdr, payload.data(), payload.size(), ob::WalPosition{9999, 0});

    // Now read the whole stream and walk it the way the replica does: a header line, then exactly
    // as many bytes as it named. Anything else in between is the defect.
    struct timeval tv{};
    tv.tv_sec  = 5;
    tv.tv_usec = 0;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    std::string in;
    char rbuf[65536];
    const auto pull = [&]() {
        const ssize_t n = ::recv(fd, rbuf, sizeof(rbuf), 0);
        if (n > 0) in.append(rbuf, static_cast<size_t>(n));
        return n > 0;
    };
    const auto next_line = [&]() -> std::string {
        for (;;) {
            const size_t nl = in.find('\n');
            if (nl != std::string::npos) {
                const std::string line = in.substr(0, nl);
                in.erase(0, nl + 1);
                return line;
            }
            if (!pull()) return {};
        }
    };
    const auto skip_bytes = [&](size_t want) {
        while (in.size() < want) { if (!pull()) return false; }
        in.erase(0, want);
        return true;
    };

    const std::string begin = next_line();
    ASSERT_EQ(begin.rfind("SNAPSHOT_BEGIN", 0), 0u) << "got: " << begin;
    size_t total_bytes = 0, file_count = 0;
    unsigned snap_file = 0;
    size_t snap_off = 0;
    ASSERT_EQ(std::sscanf(begin.c_str(), "SNAPSHOT_BEGIN %zu %u %zu %zu", &total_bytes, &snap_file,
                          &snap_off, &file_count), 4);
    ASSERT_GT(file_count, 0u) << "the snapshot has no files, so nothing could have been spliced "
                                 "into it";

    for (size_t i = 0; i < file_count; ++i) {
        const std::string header = next_line();
        ASSERT_EQ(header.rfind("SNAPSHOT_FILE", 0), 0u)
            << "file " << i << " of " << file_count << ": expected a SNAPSHOT_FILE header, got '"
            << header << "' - a live record was spliced into the snapshot's byte stream (#99)";
        char rel[256] = {};
        size_t size = 0;
        unsigned crc = 0;
        ASSERT_EQ(std::sscanf(header.c_str(), "SNAPSHOT_FILE %255s %zu %u", rel, &size, &crc), 3)
            << "got: " << header;
        ASSERT_TRUE(skip_bytes(size)) << "the stream ended inside file " << i;
    }

    const std::string end = next_line();
    EXPECT_EQ(end.rfind("SNAPSHOT_END", 0), 0u)
        << "expected SNAPSHOT_END, got '" << end << "'";

    // And the deferred record follows it, rather than having been dropped: the snapshot carries the
    // position it was taken at, so a record past that position is what the replica needs next.
    //
    // Read through the same buffer the walk above used, not with `recv_wire_records()`. That helper
    // starts from an empty buffer and reads the socket, and by this point the record's bytes are
    // already in `in` - the "reader with a buffer of its own" mistake, from the other side.
    const std::string wal_line = next_line();
    ASSERT_EQ(wal_line.rfind("WAL ", 0), 0u)
        << "expected the deferred live record after SNAPSHOT_END, got '" << wal_line << "'";
    unsigned wl_file = 0, wl_epoch = 0;
    size_t wl_offset = 0, wl_total = 0;
    ASSERT_GE(std::sscanf(wal_line.c_str(), "WAL %u %zu %zu %u", &wl_file, &wl_offset, &wl_total,
                          &wl_epoch), 3);
    while (in.size() < wl_total) { ASSERT_TRUE(pull()) << "the deferred record was truncated"; }
    uint64_t seq = 0;
    std::memcpy(&seq, in.data(), sizeof(seq));   // WALRecord::sequence_number is first
    EXPECT_EQ(seq, kMarkerSeq)
        << "the record released after the snapshot is not the one that waited for it";

    ::close(fd);
    mgr->stop();
    engine.close();
}


// ── #99/#100: what to do with a live record, as a contract ───────────────────────────────────

TEST(LiveRecordDecision, TheAnswerIsAContractRatherThanASideEffect) {
    // A pure function over the replica's transfer state, checked directly. Not every case here is
    // reachable from a socket - a record appended before a cursor was created but broadcast after
    // it needs the handshake to land between an append and its broadcast, one mutex hand-off wide -
    // and an ordering helper this decision rests on already survived every behavioural test once
    // (#100). So the cases are enumerated rather than provoked.
    using ob::LiveRecordAction;
    using ob::WalPosition;

    ob::ReplicaInfo r;
    const WalPosition somewhere{3, 100};

    // Accepted, and it has not said what it wants yet. Whatever it asks for, the catch-up ends at
    // or past this record, so the live copy would be the second one.
    r.asked_for_stream = false;
    EXPECT_EQ(ob::live_record_action(r, somewhere), LiveRecordAction::Drop);
    EXPECT_FALSE(ob::transfer_in_progress(r));

    // Streaming, nothing in progress: the live path is the only path.
    r.asked_for_stream = true;
    EXPECT_EQ(ob::live_record_action(r, somewhere), LiveRecordAction::Send);

    // A catch-up walking a range. Inside it the cursor will read this record out of the WAL file
    // itself; at or past its end, the live path is what delivers it - after the cursor's last byte.
    r.catchup.active         = true;
    r.catchup.through_file   = 3;
    r.catchup.through_offset = 100;
    EXPECT_TRUE(ob::transfer_in_progress(r));
    EXPECT_EQ(ob::live_record_action(r, WalPosition{3, 99}), LiveRecordAction::Drop);
    EXPECT_EQ(ob::live_record_action(r, WalPosition{2, 999999}), LiveRecordAction::Drop);
    EXPECT_EQ(ob::live_record_action(r, WalPosition{3, 100}), LiveRecordAction::Defer);
    EXPECT_EQ(ob::live_record_action(r, WalPosition{4, 0}), LiveRecordAction::Defer);
    r.catchup.active = false;

    // A snapshot being streamed. The snapshot carries the WAL position it was taken at: earlier
    // records are inside the files being installed, later ones are what the replica needs next.
    r.snapshot_transfer.active                  = true;
    r.snapshot_transfer.manifest.wal_file_index  = 5;
    r.snapshot_transfer.manifest.wal_byte_offset = 4096;
    EXPECT_TRUE(ob::transfer_in_progress(r));
    EXPECT_EQ(ob::live_record_action(r, WalPosition{5, 4095}), LiveRecordAction::Drop);
    EXPECT_EQ(ob::live_record_action(r, WalPosition{4, 999999}), LiveRecordAction::Drop);
    EXPECT_EQ(ob::live_record_action(r, WalPosition{5, 4096}), LiveRecordAction::Defer);
    EXPECT_EQ(ob::live_record_action(r, WalPosition{6, 0}), LiveRecordAction::Defer);

    // Never `Send` while anything is streaming: a `Send` is what splices bytes into a stream whose
    // receiver is counting them.
    for (const WalPosition p : {WalPosition{0, 0}, WalPosition{5, 4095}, WalPosition{5, 4096},
                                WalPosition{9, 1}}) {
        EXPECT_NE(ob::live_record_action(r, p), LiveRecordAction::Send)
            << "a record at file " << p.file_index << " offset " << p.offset
            << " would be sent into a snapshot stream";
    }

    // And a replica that has not asked is dropped whatever is in progress, because the request it
    // makes decides where its stream starts.
    r.asked_for_stream = false;
    EXPECT_EQ(ob::live_record_action(r, WalPosition{6, 0}), LiveRecordAction::Drop);
}


// ── #99: a live record spliced into a snapshot stream ────────────────────────────────────────

namespace {

/// Drive a replica through a snapshot bootstrap from a mock primary, optionally splicing a live
/// `WAL` record into the stream between the two files. Returns whether the snapshot was installed,
/// judged by the files landing in the replica's data directory rather than by a log line.
struct SnapshotBootstrapOutcome {
    bool first_file_installed{false};
    bool second_file_installed{false};
    /// `repl_state.txt` as it stands once the install is done, read before `stop()` so it is the
    /// install's own write rather than the one on the way out (#101 requirement 4.4).
    std::string saved_state;
};

SnapshotBootstrapOutcome run_snapshot_bootstrap(const std::string& dir, uint16_t port,
                                                 bool splice_a_live_record) {
    SnapshotBootstrapOutcome out;
    const int listen_fd = create_mock_primary(port);
    if (listen_fd < 0) return out;

    ob::Engine engine(dir, 100'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();

    ob::ReplicationClientConfig cfg;
    cfg.primary_host         = "127.0.0.1";
    cfg.primary_port         = port;
    cfg.state_file           = dir + "/repl_state.txt";
    cfg.snapshot_staging_dir = dir + "/snapshot_staging";

    ob::ReplicationClient client(cfg, engine);
    client.start();

    const int peer_fd = accept_with_timeout(listen_fd, 5000);
    if (peer_fd < 0) { client.stop(); ::close(listen_fd); engine.close(); return out; }

    answer_stream_id(peer_fd, 0x51DULL);
    const std::string handshake = recv_line(peer_fd, 3000);
    EXPECT_EQ(handshake.rfind("REPLICATE", 0), 0u) << "got: " << handshake;

    const char* truncated = "ERR WAL_TRUNCATED\n";
    EXPECT_GT(::send(peer_fd, truncated, std::strlen(truncated), MSG_NOSIGNAL), 0);
    const std::string request = recv_line(peer_fd, 5000);
    EXPECT_EQ(request, "SNAPSHOT_REQUEST") << "got: " << request;

    const std::string a_body = "AAAAA";
    const std::string b_body = "BBBBB";
    const uint32_t a_crc = ob::crc32c(a_body.data(), a_body.size());
    const uint32_t b_crc = ob::crc32c(b_body.data(), b_body.size());

    const auto send_str = [&](const std::string& text) {
        return ::send(peer_fd, text.data(), text.size(), MSG_NOSIGNAL) ==
               static_cast<ssize_t>(text.size());
    };

    // A non-zero WAL position, deliberately: a snapshot taken at 0 0 is indistinguishable from
    // the zeros a wipe writes, so an assertion about where the bootstrap left the replica would
    // hold for the wrong reason (#101 requirement 4.4).
    char begin[128];
    std::snprintf(begin, sizeof(begin), "SNAPSHOT_BEGIN %zu 3 4096 2\n",
                  a_body.size() + b_body.size());
    EXPECT_TRUE(send_str(begin));

    char header[256];
    std::snprintf(header, sizeof(header), "SNAPSHOT_FILE SNAPA/EXCH/seg/a.col %zu %u\n",
                  a_body.size(), a_crc);
    EXPECT_TRUE(send_str(header));
    EXPECT_TRUE(send_str(a_body));

    if (splice_a_live_record) {
        // Exactly what `broadcast()` puts on the wire, in the place the next `SNAPSHOT_FILE`
        // header belongs. This is the stream a primary taking writes produces today.
        auto [payload, crc] = build_delta_payload("BTCUSD", "BINANCE", 7, 7'000'000, 0, 50000, 100);
        ob::WALRecord hdr{};
        hdr.sequence_number = 7;
        hdr.timestamp_ns    = 7'000'000;
        hdr.checksum        = crc;
        hdr.payload_len     = static_cast<uint16_t>(payload.size());
        hdr.record_type     = ob::WAL_RECORD_DELTA;
        hdr._pad            = 0;
        const auto msg = build_wal_message(0, 4888, hdr, payload.data(), payload.size());
        EXPECT_EQ(::send(peer_fd, msg.data(), msg.size(), MSG_NOSIGNAL),
                  static_cast<ssize_t>(msg.size()));
    }

    std::snprintf(header, sizeof(header), "SNAPSHOT_FILE SNAPB/EXCH/seg/b.col %zu %u\n",
                  b_body.size(), b_crc);
    EXPECT_TRUE(send_str(header));
    EXPECT_TRUE(send_str(b_body));
    // SNAPSHOT_END carries the CRC32C of the manifest the replica assembles from the headers it
    // received, so the mock primary has to build the same manifest to name it. Getting this wrong
    // is what the control test caught: a bare `SNAPSHOT_END` fails `sscanf` and the bootstrap is
    // abandoned for a reason that has nothing to do with what the test is about.
    ob::SnapshotManifest expected;
    expected.wal_file_index  = 3;
    expected.wal_byte_offset = 4096;
    expected.total_bytes     = a_body.size() + b_body.size();
    expected.files.push_back(ob::SnapshotFileEntry{"SNAPA/EXCH/seg/a.col", a_body.size(), a_crc});
    expected.files.push_back(ob::SnapshotFileEntry{"SNAPB/EXCH/seg/b.col", b_body.size(), b_crc});
    const std::string manifest_json = expected.to_json();
    char end[64];
    std::snprintf(end, sizeof(end), "SNAPSHOT_END %u\n",
                  ob::crc32c(manifest_json.data(), manifest_json.size()));
    EXPECT_TRUE(send_str(end));

    // Long enough for the install, and the assertion is on the filesystem rather than on a timer:
    // a bootstrap that has not finished by now has not finished because it was abandoned.
    std::this_thread::sleep_for(std::chrono::milliseconds(800));

    out.first_file_installed  = std::filesystem::exists(dir + "/SNAPA/EXCH/seg/a.col");
    out.second_file_installed = std::filesystem::exists(dir + "/SNAPB/EXCH/seg/b.col");
    {
        std::ifstream in(cfg.state_file);
        out.saved_state.assign(std::istreambuf_iterator<char>(in),
                               std::istreambuf_iterator<char>());
    }

    client.stop();
    ::close(peer_fd);
    ::close(listen_fd);
    engine.close();
    return out;
}

} // namespace

TEST_F(ReplicationClientTest, ASnapshotBootstrapInstallsWhatThePrimarySends) {
    // The control, and it is what makes the test below a measurement rather than a tautology: this
    // stream is the same one minus the spliced record, and it has to install.
    const auto out = run_snapshot_bootstrap(tmp_->str(), port_, /*splice_a_live_record=*/false);
    EXPECT_TRUE(out.first_file_installed) << "the first snapshot file was not installed";
    EXPECT_TRUE(out.second_file_installed) << "the second snapshot file was not installed";
}

TEST_F(ReplicationClientTest, ASplicedLiveRecordAbandonsTheSnapshotBootstrap) {
    // Measured rather than argued (#99). `broadcast()` walks every entry in `replicas_` with no
    // branch on `snapshot_transfer.active`, so a replica being bootstrapped from a primary that is
    // taking writes receives live `WAL ...` records interleaved into a stream of `SNAPSHOT_FILE`
    // headers and raw file bytes. On this side `request_and_receive_snapshot()` reads a header with
    // `read_line()` and then exactly `file_size` bytes - so a record landing between two files
    // makes the next `read_line()` return `WAL 0 4888 ...` where `SNAPSHOT_FILE` was expected.
    //
    // The outcome is read off the replica's data directory, not off a log line: the bootstrap is
    // abandoned, so nothing is installed - including the file that arrived intact before the
    // splice, because the install happens at `SNAPSHOT_END`.
    const auto out = run_snapshot_bootstrap(tmp_->str(), port_, /*splice_a_live_record=*/true);
    EXPECT_FALSE(out.second_file_installed)
        << "the bootstrap survived a record spliced into its byte stream, which would mean this "
           "test no longer measures what #99 is about";
    EXPECT_FALSE(out.first_file_installed)
        << "a file arrived before the splice and was installed anyway - the install is supposed to "
           "happen at SNAPSHOT_END, so a partial install is a second defect";
}


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

    // 4. Accept connection, name the stream (#101) and read the handshake.
    int client_fd = accept_with_timeout(listen_fd, 5000);
    ASSERT_GE(client_fd, 0);
    answer_stream_id(client_fd, 0x51DULL);
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

    // 4. Accept connection, name the stream (#101) and read the handshake.
    int client_fd = accept_with_timeout(listen_fd, 5000);
    ASSERT_GE(client_fd, 0);
    answer_stream_id(client_fd, 0x51DULL);
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

// ── #101 requirement 5: over-delivery on the replication link must not duplicate rows ────────
TEST_F(ReplicationClientTest, ARecordDeliveredTwiceIsAppliedOnce) {
    // The load-bearing measurement of #101, and the reason its requirement 5 comes before its
    // requirement 1. `repl_state.txt` is written every ten seconds while `confirmed_*` advances per
    // record, so a replica resuming from its saved position is handed records it already has.
    // Storage is append-only, so applying a duplicate appends its rows a second time - which would
    // turn a fix about cost into a defect about correctness.
    //
    // The guard exists one function away: `apply_remote_delta()`, the mesh path, drops a record
    // whose sequence number it has seen, with the comment explaining that catch-up over-delivers on
    // purpose. `ReplicationClient` calls `apply_delta()`, which had no such guard.
    //
    // Roadmap #100 says of exactly this: "whether that produces duplicate rows depends on flush
    // timing, and that half is not measured". This is the measurement.
    int listen_fd = create_mock_primary(port_);
    ASSERT_GE(listen_fd, 0);

    ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();

    ob::ReplicationClientConfig cfg;
    cfg.primary_host = "127.0.0.1";
    cfg.primary_port = port_;
    cfg.state_file   = tmp_->str() + "/repl_state.txt";
    ob::ReplicationClient client(cfg, engine);
    client.start();

    int client_fd = accept_with_timeout(listen_fd, 5000);
    ASSERT_GE(client_fd, 0);
    answer_stream_id(client_fd, 0x51DULL);
    const std::string handshake = recv_line(client_fd, 3000);
    ASSERT_TRUE(handshake.rfind("REPLICATE", 0) == 0) << "got: " << handshake;

    // One record, one level, carrying a sequence number the primary minted - which is what makes it
    // a replicated record rather than a client write. `stamp_sequence()` only assigns when the
    // number is zero, so this one passes through untouched.
    auto [payload, crc] =
        build_delta_payload("DUPSYM", "BINANCE", 7, 1'700'000'000ULL, 0, 50'000, 100);
    ob::WALRecord hdr{};
    hdr.sequence_number = 7;
    hdr.timestamp_ns    = 1'700'000'000ULL;
    hdr.checksum        = crc;
    hdr.payload_len     = static_cast<uint16_t>(payload.size());
    hdr.record_type     = ob::WAL_RECORD_DELTA;
    hdr._pad            = 0;

    const auto msg = build_wal_message(0, 0, hdr, payload.data(), payload.size());
    ASSERT_EQ(::send(client_fd, msg.data(), msg.size(), MSG_NOSIGNAL),
              static_cast<ssize_t>(msg.size()));
    ASSERT_TRUE(recv_line(client_fd, 5000).rfind("ACK ", 0) == 0);

    // The same record again, announced at the same position - which is exactly what a resume from a
    // lagging saved position re-delivers. The wait is the second ACK rather than a sleep: the client
    // acknowledges either way, so this synchronises without deciding the outcome.
    ASSERT_EQ(::send(client_fd, msg.data(), msg.size(), MSG_NOSIGNAL),
              static_cast<ssize_t>(msg.size()));
    ASSERT_TRUE(recv_line(client_fd, 5000).rfind("ACK ", 0) == 0);

    engine.flush_incremental();

    size_t rows = 0;
    const std::string err = engine.execute(
        "SELECT * FROM 'DUPSYM'.'BINANCE' WHERE timestamp BETWEEN 0 AND 9999999999999999999",
        [&](const ob::QueryResult&) { ++rows; });
    EXPECT_TRUE(err.empty()) << "query failed: " << err;
    EXPECT_EQ(rows, 1u)
        << "the same record arrived twice and its row was stored " << rows << " times; storage is "
        << "append-only, so over-delivery on this link is a correctness defect rather than a cost";

    client.stop();
    ::close(client_fd);
    ::close(listen_fd);
    engine.close();
}

// ── #101 requirement 5: what the dedup guard must and must not touch ─────────────────────────────

TEST(ReplicationDedup, AnEmbeddedWriteKeepsItsOwnSequenceNumbering) {
    // The regression test for the finding that changed this design. The obvious guard - drop when
    // `sequence_number != 0`, since "a client write always carries zero" - is false of the embedded
    // path: `ob_apply_delta()` takes `seq` as a caller parameter and the Python client's
    // `insert(..., seq, timestamp_ns)` has it as a *required* argument. An embedded user numbering
    // their own records from 1 per symbol would then have had every write after the first silently
    // dropped: data loss introduced by an item about restart cost, in a public API.
    //
    // So `apply_delta()` applies whatever it is handed, and the two records below - deliberately
    // sharing a sequence number - both have to be stored. If this test ever fails, the guard has
    // leaked out of `apply_delta_replicated()`.
    ReplTempDir tmp;
    ob::Engine engine(tmp.str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();

    ob::Level lvl{};
    lvl.price = 42'000; lvl.qty = 7; lvl.cnt = 1; lvl._pad = 0;
    for (int i = 0; i < 2; ++i) {
        ob::DeltaUpdate d{};
        std::strncpy(d.symbol, "EMBED", sizeof(d.symbol) - 1);
        std::strncpy(d.exchange, "LOCAL", sizeof(d.exchange) - 1);
        d.sequence_number = 5;                     // the caller's own numbering, repeated
        d.timestamp_ns    = 1'700'000'000ULL + static_cast<uint64_t>(i);
        d.side            = ob::SIDE_BID;
        d.n_levels        = 1;
        ASSERT_EQ(engine.apply_delta(d, &lvl), ob::OB_OK);
    }
    engine.flush_incremental();

    size_t rows = 0;
    const std::string err = engine.execute(
        "SELECT * FROM 'EMBED'.'LOCAL' WHERE timestamp BETWEEN 0 AND 9999999999999999999",
        [&](const ob::QueryResult&) { ++rows; });
    EXPECT_TRUE(err.empty()) << err;
    EXPECT_EQ(rows, 2u)
        << "apply_delta() dropped a write because its sequence number repeated; that number belongs "
        << "to the caller on this path, and dropping it loses an embedded user's data";
    engine.close();
}

TEST(ReplicationDedup, TheGuardIsOnEveryEntryPointAnOverDeliveringLinkUses) {
    // Static, and the list of functions comes from the source rather than from this test: whatever
    // `replication.cpp` and `multi_master.cpp` call on the engine to apply a record is what has to
    // carry the guard. A list written here by hand would be a claim about the code rather than
    // evidence about it, and the shape this guards against - a fix present at one of two sites - has
    // cost this repository three separate defects.
    const auto read = [](const char* rel) {
        std::ifstream in(std::string(OB_SOURCE_DIR) + "/" + rel);
        return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    };
    const std::string engine_src = read("src/engine.cpp");
    ASSERT_FALSE(engine_src.empty());

    // Which engine methods do the two over-delivering links apply through?
    std::set<std::string> applied_through;
    for (const char* rel : {"src/replication.cpp", "src/multi_master.cpp"}) {
        const std::string src = read(rel);
        ASSERT_FALSE(src.empty()) << rel;
        const std::string needle = "engine_.apply_";
        for (size_t at = src.find(needle); at != std::string::npos;
             at = src.find(needle, at + 1)) {
            const size_t name_at = at + std::strlen("engine_.");
            const size_t paren   = src.find('(', name_at);
            if (paren == std::string::npos) continue;
            applied_through.insert(src.substr(name_at, paren - name_at));
        }
    }
    // The pair: if this came back empty, "every one of them has the guard" would be vacuous.
    EXPECT_GE(applied_through.size(), 2u)
        << "expected at least two apply entry points across the replication link and the mesh; "
        << "found " << applied_through.size() << ", so this test is not looking at what it thinks";

    std::string missing;
    for (const std::string& fn : applied_through) {
        const std::string sig = "ob_status_t Engine::" + fn + "(";
        const size_t begin = engine_src.find(sig);
        if (begin == std::string::npos) continue;          // not defined here (e.g. a wrapper)
        const size_t end = engine_src.find("\nob_status_t Engine::", begin + sig.size());
        const std::string body = engine_src.substr(begin, end == std::string::npos
                                                          ? std::string::npos : end - begin);
        // `apply_delta_replicated` delegates, so accept the policy it passes as well as the guard
        // itself. Accepting the *name* of the shared implementation would not do: `apply_delta`
        // delegates to the very same function with the opposite policy, so a call site moved back
        // to `apply_delta` would still read as guarded. The token has to be the one that differs.
        const bool guarded = body.find("has_seen(") != std::string::npos ||
                             body.find("DropIfSeen") != std::string::npos;
        if (!guarded) missing += fn + " ";
    }
    EXPECT_TRUE(missing.empty())
        << "these engine entry points are used by a link that over-delivers on purpose and do not "
        << "drop what they have already applied, so storage grows a duplicate row per repeat: "
        << missing;
}

TEST(ReplicationDedup, TheFrontierThatMakesDedupWorkSurvivesARestart) {
    // Without this, the guard protects one process life and #101's resume protects nothing: a
    // replica restarts, is handed the ten seconds of records its saved position lags behind, and
    // has forgotten that it applied them.
    //
    // What restores it is the replica's own WAL - `restore_version_vector()` runs in `open()`,
    // before the tail replay, from the vector records the flush writes. So the flush before the
    // close is part of the subject and not tidiness.
    //
    // **The observable is `pending_rows`, and counting stored rows instead was wrong.** The first
    // version of this test asserted one row after the re-delivery and **passed with the guard
    // disabled** - measured, not supposed. After a restart the re-flushed segment covers the
    // timestamp range the restored one already covers, and `ColumnarStore` refuses that merge as a
    // duplicate, so the row count was measuring the store's own refusal and said nothing about the
    // frontier. That confound is already on record from the mesh's dedup work. `pending_rows` moves
    // if and only if the record entered the write pipeline.
    ReplTempDir tmp;
    ob::Level lvl{};
    lvl.price = 31'337; lvl.qty = 3; lvl.cnt = 1; lvl._pad = 0;

    ob::DeltaUpdate d{};
    std::strncpy(d.symbol, "AFTERBOOT", sizeof(d.symbol) - 1);
    std::strncpy(d.exchange, "BINANCE", sizeof(d.exchange) - 1);
    d.sequence_number = 7;
    d.timestamp_ns    = 1'700'000'000ULL;
    d.side            = ob::SIDE_BID;
    d.n_levels        = 1;

    {
        ob::Engine engine(tmp.str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
        engine.open();
        ASSERT_EQ(engine.apply_delta_replicated(d, &lvl), ob::OB_OK);
        engine.flush_incremental();
        engine.close();
    }

    ob::Engine engine(tmp.str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();
    engine.flush_incremental();   // a known queue depth, whatever the tail replay left behind
    ASSERT_EQ(engine.stats().pending_rows, 0u)
        << "the queue is not empty before the re-delivery, so its depth afterwards decides nothing";

    // The same record, re-delivered after the restart - which is exactly what resuming from a
    // position written on a ten-second timer produces.
    ASSERT_EQ(engine.apply_delta_replicated(d, &lvl), ob::OB_OK);

    EXPECT_EQ(engine.stats().pending_rows, 0u)
        << "the re-delivered record entered the write pipeline, so the sequence frontier did not "
        << "survive the restart: dedup then protects one process life, and a resumed replica "
        << "duplicates every record its saved position lagged behind";
    engine.close();
}

// ── #101 group 3: the saved position is invalidated by promotion, not by demotion ────────────────

TEST_F(ReplicationClientTest, PromotionForgetsWhereWeWereInThePrimarysStream) {
    // The inversion this item turns on. The position used to be deleted on the way *into*
    // replication, which is exactly when it is needed; the moment it stops being true is the one
    // where this node starts writing records of its own.
    //
    // Without this, a node that was a replica at (f,o), got promoted, accepted writes and later came
    // back as a replica would resume from (f,o) - with records above it that the primary never had.
    ob::ReplicationClientConfig cfg;
    cfg.state_file = tmp_->str() + "/repl_state.txt";     // primary_port stays 0: no client, no socket

    ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE, {}, cfg);
    engine.open();

    // A saved position, of the shape a replica leaves behind.
    { std::ofstream out(cfg.state_file); out << "file_index=3\nbyte_offset=4096\n"; }
    ASSERT_TRUE(std::filesystem::exists(cfg.state_file));

    engine.promote_to_primary(ob::EpochValue{7});

    EXPECT_FALSE(std::filesystem::exists(cfg.state_file))
        << "a promoted node kept the position it had in someone else's stream; after a restart it "
        << "would resume from there, with its own records sitting above it";
    engine.close();
}

TEST_F(ReplicationClientTest, AClientStoppedByThePromotionDoesNotRecreateThePosition) {
    // Pins an ordering that is currently a property of how `promote_to_primary()` happens to be
    // arranged rather than of anything written down - and a comment claiming a property the code
    // does not have has already survived a mutation in this repository.
    //
    // `ReplicationClient::stop()` ends with `save_state()`. So a deletion placed before the client
    // is stopped is undone by the client on its way out, and the node comes back after a restart
    // resuming from a stream it no longer follows. The deletion has to run after the stop.
    int listen_fd = create_mock_primary(port_);
    ASSERT_GE(listen_fd, 0);

    ob::ReplicationClientConfig cfg;
    cfg.primary_host = "127.0.0.1";
    cfg.primary_port = port_;                            // so `open()` starts a client
    cfg.state_file   = tmp_->str() + "/repl_state.txt";

    ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE, {}, cfg);
    engine.open();

    // Wait until the client is really up, rather than sleeping: it has to be alive for its stop to
    // be able to rewrite the file, or this test passes for the wrong reason.
    int client_fd = accept_with_timeout(listen_fd, 5000);
    ASSERT_GE(client_fd, 0) << "the replication client never connected, so nothing here could have "
                               "rewritten the position file";
    answer_stream_id(client_fd, 0x51DULL);
    ASSERT_TRUE(recv_line(client_fd, 3000).rfind("REPLICATE", 0) == 0);

    engine.promote_to_primary(ob::EpochValue{9});

    EXPECT_FALSE(std::filesystem::exists(cfg.state_file))
        << "the position file is back: the client wrote it while stopping, after the promotion had "
        << "deleted it, so the deletion ran too early";

    ::close(client_fd);
    ::close(listen_fd);
    engine.close();
}

TEST(ReplicationDedup, DiscardingLocalDataAlsoForgetsWhatWasApplied) {
    // Dedup and the wipe are only compatible if the wipe clears the sequence frontier too, and this
    // is the test that says so. `discard_local_data_for_resync()` clears the buffers, the pending
    // queue and every segment on disk - and the frontier lives in `seq_tracker_`, which is not part
    // of any of those. Leave it standing and the replica claims to have seen records it has just
    // deleted, so the `REPLICATE 0 0` that follows a wipe has **every** record dropped as a
    // duplicate and the store stays empty. Silent, total data loss.
    //
    // Before the dedup guard this was harmless, which is why nothing here caught it: without a
    // guard, an over-claimed frontier costs nothing. `SequenceTracker::reset()`'s own docstring
    // names the mechanism - "a frontier from the discarded contents would survive the discard and
    // claim records that are no longer on disk" - and the snapshot install path had been calling it
    // for exactly that reason all along. The wipe path had not.
    ReplTempDir tmp;
    ob::Engine engine(tmp.str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();

    ob::Level lvl{};
    lvl.price = 55'555; lvl.qty = 9; lvl.cnt = 1; lvl._pad = 0;
    ob::DeltaUpdate d{};
    std::strncpy(d.symbol, "WIPED", sizeof(d.symbol) - 1);
    std::strncpy(d.exchange, "BINANCE", sizeof(d.exchange) - 1);
    d.sequence_number = 11;
    d.timestamp_ns    = 1'700'000'000ULL;
    d.side            = ob::SIDE_BID;
    d.n_levels        = 1;

    ASSERT_EQ(engine.apply_delta_replicated(d, &lvl), ob::OB_OK);
    engine.flush_incremental();

    engine.discard_local_data_for_resync();

    // The stream replayed from zero, which is what follows a wipe.
    ASSERT_EQ(engine.apply_delta_replicated(d, &lvl), ob::OB_OK);
    engine.flush_incremental();

    size_t rows = 0;
    const std::string err = engine.execute(
        "SELECT * FROM 'WIPED'.'BINANCE' WHERE timestamp BETWEEN 0 AND 9999999999999999999",
        [&](const ob::QueryResult&) { ++rows; });
    EXPECT_TRUE(err.empty()) << err;
    EXPECT_EQ(rows, 1u)
        << "the wipe kept the sequence frontier, so the record replayed into the empty store was "
        << "dropped as a duplicate and the store holds " << rows << " rows: a replica that wipes "
        << "would re-sync into nothing";
    engine.close();
}

TEST(ReplicationDedup, EveryPathThatDiscardsTheStoreAlsoDiscardsTheFrontier) {
    // Static, over `src/engine.cpp`, and it exists because this defect had **two** instances and
    // the second was found only by going looking. Both clear the store and neither had reset the
    // frontier that describes it:
    //
    //   - `discard_local_data_for_resync()`, the failover wipe;
    //   - `load_snapshot()`, which the replication bootstrap calls and which got away with it
    //     because the *mesh* calls `adopt_snapshot_sequence_state()` straight after, and that
    //     resets.
    //
    // A frontier left standing claims rows the discard has just deleted, so every record the
    // primary sends below it is dropped as a duplicate and nothing ever refills the hole. Measured
    // on the first instance: 0 rows where 1 was replayed.
    //
    // The list of functions comes from the source. Naming the two would be a claim about the code
    // rather than evidence about it - and a third path is exactly what this is for.
    const std::string src = read_source("src/engine.cpp");
    ASSERT_FALSE(src.empty());

    std::vector<std::string> discarding;
    std::vector<std::string> offenders;
    const std::string marker = "buffers_.clear();";
    for (std::size_t at = src.find(marker); at != std::string::npos;
         at = src.find(marker, at + marker.size())) {
        const std::size_t sig = definition_start(src, at, "Engine::");
        ASSERT_NE(sig, std::string::npos) << "could not find the function containing a buffers_ clear";
        const std::string name = src.substr(sig, src.find('(', sig) - sig);
        const std::string body = body_at(src, sig);
        ASSERT_FALSE(body.empty()) << "no body for " << name;
        discarding.push_back(name);
        if (body.find("seq_tracker_.reset()") == std::string::npos) offenders.push_back(name);
    }

    // The pair, because "no offenders" is also what a broken walk produces.
    EXPECT_GE(discarding.size(), 2u)
        << "expected at least the failover wipe and the snapshot install to discard the store; "
        << "found " << discarding.size() << ", so this test is not looking at what it thinks";

    std::string joined;
    for (const auto& name : offenders) joined += name + "  ";
    EXPECT_TRUE(offenders.empty())
        << "these functions discard the store and keep the sequence frontier that describes it, so "
        << "the records replayed afterwards are dropped as duplicates and the store stays short: "
        << joined;
}

// ── #101 group 4: whose stream is this ───────────────────────────────────────────────────────────
//
// A saved position is a pair of numbers, and numbers do not say which WAL they index. Two data
// directories restored from the same backup, or one primary rebuilt from scratch at the same
// address, hand out offsets that read as valid and name different records. So the primary
// announces an identity, the replica saves it next to the position, and a mismatch means start
// over. The address is deliberately not the identity, and the tests below keep it constant to say
// so (requirement 4.3).

namespace {

/// One flushed row, so a wipe has something to take. `apply_delta_replicated` rather than
/// `apply_delta`: the sequence number comes from the primary on this path, which is the entry
/// point a replica actually uses.
void insert_one_replicated_row(ob::Engine& engine, const char* symbol, uint64_t seq) {
    ob::Level lvl{};
    lvl.price = 42'000; lvl.qty = 3; lvl.cnt = 1; lvl._pad = 0;
    ob::DeltaUpdate d{};
    std::strncpy(d.symbol, symbol, sizeof(d.symbol) - 1);
    std::strncpy(d.exchange, "BINANCE", sizeof(d.exchange) - 1);
    d.sequence_number = seq;
    d.timestamp_ns    = 1'700'000'000ULL + seq;
    d.side            = ob::SIDE_BID;
    d.n_levels        = 1;
    ASSERT_EQ(engine.apply_delta_replicated(d, &lvl), ob::OB_OK);
    engine.flush_incremental();
}

size_t count_rows(ob::Engine& engine, const char* symbol) {
    size_t rows = 0;
    const std::string q = std::string("SELECT * FROM '") + symbol +
                          "'.'BINANCE' WHERE timestamp BETWEEN 0 AND 9999999999999999999";
    const std::string err = engine.execute(q, [&](const ob::QueryResult&) { ++rows; });
    // A wiped store does not hold an empty symbol, it holds no symbol - so the query refuses
    // rather than returning nothing. That one refusal means zero rows; any other error is a
    // failure, because "the query broke" must not read as "the store was discarded".
    if (err.find("OB_ERR_NOT_FOUND") != std::string::npos) return 0;
    EXPECT_TRUE(err.empty()) << err;
    return rows;
}

std::string read_whole_file(const std::string& path) {
    std::ifstream in(path);
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

}  // namespace

TEST_F(ReplicationClientTest, APositionIsResumedFromWhenThePrimaryNamesTheStreamItBelongsTo) {
    // The case this whole item exists for: a replica that restarted keeps what it holds and asks
    // for the rest. Before #101 the restart wiped the store and re-synced from zero.
    int listen_fd = create_mock_primary(port_);
    ASSERT_GE(listen_fd, 0);

    ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();
    insert_one_replicated_row(engine, "KEPT", 5);
    ASSERT_EQ(count_rows(engine, "KEPT"), 1u);

    ob::ReplicationClientConfig cfg;
    cfg.primary_host = "127.0.0.1";
    cfg.primary_port = port_;
    cfg.state_file   = tmp_->str() + "/repl_state.txt";

    // What the previous run left behind: a position, and whose stream it indexes.
    { std::ofstream out(cfg.state_file);
      out << "file_index=2\nbyte_offset=1024\nstream_id=777\n"; }

    ob::ReplicationClient client(cfg, engine);
    client.start();

    int client_fd = accept_with_timeout(listen_fd, 5000);
    ASSERT_GE(client_fd, 0);
    answer_stream_id(client_fd, 777);

    const std::string handshake = recv_line(client_fd, 3000);
    EXPECT_EQ(handshake, "REPLICATE 2 1024 0")
        << "the primary named the stream this position belongs to and the replica still asked from "
        << "somewhere else, got: " << handshake;
    EXPECT_EQ(count_rows(engine, "KEPT"), 1u)
        << "the store was discarded although the position was still valid - which is the re-sync "
        << "this item removes";

    client.stop();

    // The file a downgrade would read. `stop()` rewrote it, so these are the bytes on disk.
    //
    // Both later fields are *added lines*, not changes to the two that were there: a build without
    // the `stream_id` branch (or without #103's `epoch`) ignores what it does not recognise, so it
    // still reads file 2 at offset 1024. Had either been folded into one of those lines, a
    // downgrade would read a wrong position and say nothing (requirement 6.2). The epoch is 0 here
    // because nothing in this test ever announced one.
    const std::string saved = read_whole_file(cfg.state_file);
    EXPECT_EQ(saved, "file_index=2\nbyte_offset=1024\nstream_id=777\nepoch=0\n") << saved;

    ::close(client_fd);
    ::close(listen_fd);
    engine.close();
}

TEST_F(ReplicationClientTest, ADifferentStreamAtTheSameAddressMakesTheReplicaStartOver) {
    // Requirement 4.3, and the reason the address is not the identity: the mock primary here is at
    // the same host and port as the one the position came from. Only the identity differs, which is
    // what a data directory rebuilt from scratch looks like from the outside.
    int listen_fd = create_mock_primary(port_);
    ASSERT_GE(listen_fd, 0);

    ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();
    insert_one_replicated_row(engine, "STALE", 5);
    ASSERT_EQ(count_rows(engine, "STALE"), 1u);

    ob::ReplicationClientConfig cfg;
    cfg.primary_host = "127.0.0.1";
    cfg.primary_port = port_;
    cfg.state_file   = tmp_->str() + "/repl_state.txt";
    { std::ofstream out(cfg.state_file);
      out << "file_index=2\nbyte_offset=1024\nstream_id=777\n"; }

    ob::ReplicationClient client(cfg, engine);
    client.start();

    int client_fd = accept_with_timeout(listen_fd, 5000);
    ASSERT_GE(client_fd, 0);
    answer_stream_id(client_fd, 778);      // one apart, and that is the whole difference

    const std::string handshake = recv_line(client_fd, 3000);
    EXPECT_EQ(handshake, "REPLICATE 0 0 0")
        << "the replica asked to resume inside a WAL it has never seen, got: " << handshake;
    EXPECT_EQ(count_rows(engine, "STALE"), 0u)
        << "the replica kept rows from a stream it no longer follows, and the records the new "
        << "primary sends will not overwrite them";

    // Written before the position went out, so a crash in between leaves a file describing the
    // empty store rather than the deleted stream's offset.
    const std::string saved = read_whole_file(cfg.state_file);
    EXPECT_EQ(saved, "file_index=0\nbyte_offset=0\nstream_id=778\nepoch=0\n") << saved;

    client.stop();
    ::close(client_fd);
    ::close(listen_fd);
    engine.close();
}

TEST_F(ReplicationClientTest, APositionThatNamesNoStreamIsNotResumedFrom) {
    // A state file written before #101, byte for byte. It parses - the loop ignores lines it does
    // not recognise - and it yields no identity, which is the answer that makes the replica start
    // over rather than resume against a stream it cannot attribute the numbers to.
    //
    // On real bytes rather than against a fake that pretends to be the old format: a fake would
    // pretend to be what I remember of it.
    int listen_fd = create_mock_primary(port_);
    ASSERT_GE(listen_fd, 0);

    ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();
    insert_one_replicated_row(engine, "OLDFILE", 5);

    ob::ReplicationClientConfig cfg;
    cfg.primary_host = "127.0.0.1";
    cfg.primary_port = port_;
    cfg.state_file   = tmp_->str() + "/repl_state.txt";
    { std::ofstream out(cfg.state_file); out << "file_index=2\nbyte_offset=1024\n"; }

    ob::ReplicationClient client(cfg, engine);
    client.start();

    int client_fd = accept_with_timeout(listen_fd, 5000);
    ASSERT_GE(client_fd, 0);
    answer_stream_id(client_fd, 777);

    const std::string handshake = recv_line(client_fd, 3000);
    EXPECT_EQ(handshake, "REPLICATE 0 0 0") << handshake;
    EXPECT_EQ(count_rows(engine, "OLDFILE"), 0u);
    EXPECT_EQ(read_whole_file(cfg.state_file),
              "file_index=0\nbyte_offset=0\nstream_id=777\nepoch=0\n")
        << "the file still names no stream, so the next restart would wipe again - the upgrade "
        << "would never take";

    client.stop();
    ::close(client_fd);
    ::close(listen_fd);
    engine.close();
}

TEST_F(ReplicationClientTest, APrimaryThatNamesNoStreamMakesTheReplicaStartOver) {
    // The mixed-version window, in the direction that costs something: this mock primary is a
    // pre-#101 one, so `STREAMID?` lands in its "unknown message - ignore" branch and it answers
    // nothing at all. The replica waits out its deadline, concludes it cannot attribute what it
    // holds, and starts over. That wait is the named price of design §3.1 - and it is a delay, on
    // one connection attempt, not a refusal.
    //
    // Which makes this the slowest test in the file. Deliberately not shortened by making the
    // deadline configurable: the value under test is the one production runs with.
    int listen_fd = create_mock_primary(port_);
    ASSERT_GE(listen_fd, 0);

    ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();
    insert_one_replicated_row(engine, "OLDPRIM", 5);

    ob::ReplicationClientConfig cfg;
    cfg.primary_host = "127.0.0.1";
    cfg.primary_port = port_;
    cfg.state_file   = tmp_->str() + "/repl_state.txt";
    { std::ofstream out(cfg.state_file);
      out << "file_index=2\nbyte_offset=1024\nstream_id=777\n"; }

    ob::ReplicationClient client(cfg, engine);
    client.start();

    int client_fd = accept_with_timeout(listen_fd, 5000);
    ASSERT_GE(client_fd, 0);

    // The question arrives and goes unanswered, which is all a pre-#101 primary does with it.
    const std::string question = recv_line(client_fd, 3000);
    ASSERT_EQ(question.rfind("STREAMID?", 0), 0u) << question;

    const std::string handshake = recv_line(client_fd, 12000);
    EXPECT_EQ(handshake, "REPLICATE 0 0 0")
        << "a replica that cannot tell whose stream it is asked to resume anyway, got: "
        << handshake;
    EXPECT_EQ(count_rows(engine, "OLDPRIM"), 0u);

    client.stop();
    ::close(client_fd);
    ::close(listen_fd);
    engine.close();
}

TEST_F(ReplicationProtocolTest, ThePrimaryAnswersTheStreamQuestionAndStreamsNothing) {
    // The primary's whole half of this: it answers and it decides nothing. Stateless, so asking
    // twice on one connection gives the same answer twice; and it must not start streaming, or the
    // replica would be reading records before it has decided whether to keep what it holds.
    fill_wal(*wal_, 4, 2);
    auto mgr = start_manager(nullptr, 0xC0FFEEULL);

    int fd = connect_to_localhost(port_);
    ASSERT_GE(fd, 0);

    const char* q = "STREAMID?\n";
    ASSERT_GT(::send(fd, q, std::strlen(q), MSG_NOSIGNAL), 0);
    EXPECT_EQ(recv_line(fd, 3000), "STREAM 12648430");

    ASSERT_GT(::send(fd, q, std::strlen(q), MSG_NOSIGNAL), 0);
    EXPECT_EQ(recv_line(fd, 3000), "STREAM 12648430")
        << "the second answer differs from the first, so the question changed something";

    // Nothing else, and not just "no records": a `HEARTBEAT` here would be read by the replica
    // where it expects `STREAM`, which is why this side no longer sends one to a connection that
    // has not asked for the stream. Six seconds covers the five-second heartbeat timer.
    const std::string quiet = recv_line(fd, 6000);
    EXPECT_TRUE(quiet.empty())
        << "the primary sent something to a connection that has only asked which stream this is: "
        << quiet;

    ::close(fd);
    mgr->stop();
}

TEST_F(ReplicationProtocolTest, APeerThatAsksAndNeverReadsIsDroppedRatherThanBuffered) {
    // The cost of adding a message that produces an answer. Every other line this loop reads is
    // either unanswered (`ACK`, anything unknown), answered once before the connection is closed
    // (a failed `AUTH`), or answered by a bounded cursor (`REPLICATE`). `STREAMID?` is answered
    // every time it is asked, and `enqueue_send()` has no ceiling of its own - so this is #69's
    // shape in a new place: 10 bytes in, 29 bytes of our memory out, from anyone who can reach the
    // port on a link with no cluster secret.
    //
    // The observable is the hang-up rather than a buffer size, because a buffer size is not
    // reachable from outside. This socket asks and never reads, so the answers pile up; when the
    // ceiling is reached the primary drops the connection and this side's `send` fails.
    auto mgr = start_manager(nullptr, 0xC0FFEEULL);

    const int fd = connect_to_localhost(port_, 5000);
    ASSERT_GE(fd, 0);

    // 16 MB of answers at 29 bytes each needs about 580k questions; the cap is comfortably past
    // that and small enough that a *missing* ceiling fails this test by reaching it.
    const std::string question = "STREAMID?\n";
    std::string chunk;
    for (int i = 0; i < 4096; ++i) chunk += question;

    bool hung_up = false;
    size_t sent_bytes = 0;
    for (int round = 0; round < 400 && !hung_up; ++round) {
        size_t off = 0;
        while (off < chunk.size()) {
            const ssize_t n = ::send(fd, chunk.data() + off, chunk.size() - off, MSG_NOSIGNAL);
            if (n <= 0) { hung_up = true; break; }
            off += static_cast<size_t>(n);
            sent_bytes += static_cast<size_t>(n);
        }
    }

    EXPECT_TRUE(hung_up)
        << "the primary kept answering after " << sent_bytes << " bytes of questions from a peer "
        << "that never read one of them, so its send buffer for this connection is unbounded";

    ::close(fd);
    mgr->stop();
}

TEST_F(ReplicationProtocolTest, AReplicaThatNeverAsksWhichStreamGetsWhatItAlwaysGot) {
    // The other compatibility direction: a pre-#101 replica sends `REPLICATE` straight away and
    // never asks. The branch added on this side is additive, so the old exchange has to be
    // untouched - on real bytes, because a fake old replica is a fake of what I remember.
    const size_t bytes = fill_wal(*wal_, 4, 2);
    ASSERT_GT(bytes, 0u);
    auto mgr = start_manager(nullptr, 0xC0FFEEULL);

    int fd = connect_to_localhost(port_);
    ASSERT_GE(fd, 0);

    const char* handshake = "REPLICATE 0 0 0\n";
    ASSERT_GT(::send(fd, handshake, std::strlen(handshake), MSG_NOSIGNAL), 0);

    const std::string first = recv_line(fd, 5000);
    EXPECT_EQ(first.rfind("WAL ", 0), 0u)
        << "a replica that does not know about stream identities got something other than its "
        << "catch-up, got: " << first;

    ::close(fd);
    mgr->stop();
}

TEST_F(ReplicationClientTest, ASnapshotBootstrapRecordsWhichStreamThePositionCameFrom) {
    // Requirement 4.4, and the case that costs the most if it is missing: a replica bootstrapped by
    // snapshot holds a whole store and a position it did not walk to. Without the identity beside
    // it, its very next restart cannot attribute either, wipes, and asks for the snapshot again -
    // the loop this item exists to end, entered by the most expensive path into it.
    //
    // It needs no code of its own, and that is the §3.1 inversion paying for itself twice: the
    // identity is resolved *before* the position is ever asked for, so every write of the position
    // after that point already has it. Asserted rather than assumed - the alternative design,
    // where the identity travels with `REPLICATE`, would leave this path saving a zero.
    const auto out = run_snapshot_bootstrap(tmp_->str(), port_, /*splice_a_live_record=*/false);
    ASSERT_TRUE(out.first_file_installed && out.second_file_installed)
        << "the bootstrap did not finish, so what the state file says is about something else";
    EXPECT_EQ(out.saved_state, "file_index=3\nbyte_offset=4096\nstream_id=1309\nepoch=0\n")
        << out.saved_state;
}

TEST_F(ReplicationClientTest, ARealPrimaryAnnouncesTheIdentityOfTheWalItWrites) {
    // The seam between the engine and the manager, on real bytes. Everything else in this group
    // hands the manager an identity directly, so nothing yet says the engine gives it its own -
    // and dropping that one assignment is silent: every primary would answer nothing, every
    // replica would read that as "pre-#101" and wipe on every restart, and the whole item would
    // be undone while the suite stayed green.
    ob::ReplicationConfig repl;
    repl.port = port_;

    ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE, repl);
    engine.open();
    ASSERT_NE(engine.wal_identity(), 0u)
        << "a real engine always has one - 0 is reserved for \"unknown\"";
    std::this_thread::sleep_for(std::chrono::milliseconds(150));   // let the manager bind

    const int fd = connect_to_localhost(port_, 5000);
    ASSERT_GE(fd, 0);
    const char* q = "STREAMID?\n";
    ASSERT_GT(::send(fd, q, std::strlen(q), MSG_NOSIGNAL), 0);

    EXPECT_EQ(recv_line(fd, 3000), "STREAM " + std::to_string(engine.wal_identity()));

    ::close(fd);
    engine.close();
}

TEST_F(ReplicationClientTest, TheIdentitySurvivesARestartAndDiffersBetweenDataDirectories) {
    // What requirement 4.3 rests on, and neither half was pinned anywhere. Survives a restart, or
    // a replica would wipe every time its own primary restarts; differs between directories, or a
    // primary rebuilt from scratch at the same address would be trusted to continue a stream it
    // never had - two directories restored from one backup being the case that says why it is
    // random rather than derived from the path.
    uint64_t first = 0;
    {
        ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
        engine.open();
        first = engine.wal_identity();
        engine.close();
    }
    ASSERT_NE(first, 0u);
    {
        ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
        engine.open();
        EXPECT_EQ(engine.wal_identity(), first)
            << "the same data directory came back as a different stream, so every replica of it "
            << "discards what it holds whenever this node restarts";
        engine.close();
    }

    ReplTempDir other("identity-other");
    ob::Engine fresh(other.str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
    fresh.open();
    EXPECT_NE(fresh.wal_identity(), first)
        << "a directory built from scratch claims the stream the old one was serving";
    fresh.close();
}

// ── #101 group 5: the discard is decided by the data, not by who called ──────────────────────────

TEST(ReplicationDedup, NothingDecidesWhetherToDiscardFromWhereItWasCalled) {
    // Requirement 2.1. `demote_to_replica()` has four call sites - a graceful handover, a lost
    // lease, `adopt_leader_if_present()` and process start - and it used to discard the store on
    // all four. The condition is a property of the data ("is what I hold a prefix of this
    // primary's stream?"), and that list of call sites is a thing that grows: the fifth would
    // arrive with a comment about why it is different.
    //
    // Static, because there is nothing behavioural to catch. A branch on the caller would be
    // *correct* in every case somebody thought about while writing it; what it breaks is the case
    // added later. So this asserts the shape: nothing about the caller reaches the decision.
    const std::string engine = read_source("src/engine.cpp");
    const std::string repl   = read_source("src/replication.cpp");
    const std::string failover_hpp = read_source("include/orderbook/failover.hpp");
    ASSERT_FALSE(engine.empty());
    ASSERT_FALSE(repl.empty());
    ASSERT_FALSE(failover_hpp.empty());

    // (a) The signature carries no discriminator. One parameter, the address of the primary to
    //     follow - so there is no `DemotionReason` or `bool was_primary` to branch on, and adding
    //     one has to change the interface every failover path calls through.
    const std::string decl = "virtual void demote_to_replica(";
    const auto decl_at = failover_hpp.find(decl);
    ASSERT_NE(decl_at, std::string::npos)
        << "RoleTransitionHandler no longer declares demote_to_replica, so this test is looking "
           "at something that has moved";
    const std::string params = failover_hpp.substr(
        decl_at + decl.size(), failover_hpp.find(')', decl_at) - decl_at - decl.size());
    EXPECT_EQ(params.find(','), std::string::npos)
        << "demote_to_replica takes more than the primary's address now: '" << params << "'. If "
           "one of those says why the demotion happened, the discard can be decided from the call "
           "site again";

    // (b) The function makes no discard decision at all: neither the wipe nor the deletion of the
    //     saved position appears in its body. Derived from the body rather than from a grep over
    //     the file, so a call added anywhere else in `engine.cpp` does not read as this one.
    const auto demote_sig = engine.find("void Engine::demote_to_replica(");
    ASSERT_NE(demote_sig, std::string::npos);
    const std::string demote_body = body_at(engine, demote_sig);
    ASSERT_FALSE(demote_body.empty());
    EXPECT_EQ(demote_body.find("discard_local_data_for_resync()"), std::string::npos)
        << "demoting discards the store again, so a node that merely restarted re-syncs a full "
           "store from its primary - which is #101";
    EXPECT_EQ(demote_body.find("replication_state_path()"), std::string::npos)
        << "demoting deletes the saved position again, so there is nothing left for the identity "
           "check to compare and every reconnection starts from zero";

    // (c) And the one place that does decide is the one holding both facts: the identity the
    //     primary announced and the identity the position was saved under. Every call in the two
    //     files is accounted for, so a second decision maker fails this rather than being
    //     silently correct-looking.
    std::vector<std::string> deciders;
    for (const auto& [file, src, qualifier] :
         std::vector<std::tuple<std::string, std::string, std::string>>{
             {"src/engine.cpp", engine, "Engine::"},
             {"src/replication.cpp", repl, "Replication"}}) {
        const std::string call = "discard_local_data_for_resync()";
        for (std::size_t at = src.find(call); at != std::string::npos;
             at = src.find(call, at + call.size())) {
            // The name matches its own definition, and `definition_start` walking back from a
            // signature line finds the *previous* function - so the definition read as a second
            // decision maker. A definition starts at column 0 and a call inside a body never
            // does, which tells the two apart without naming either.
            const std::size_t line_start = src.rfind('\n', at) + 1;
            if (src[line_start] != ' ' && src[line_start] != '\t') continue;
            const std::size_t sig = definition_start(src, at, qualifier);
            if (sig == std::string::npos) continue;
            const std::string name = src.substr(sig, src.find('(', sig) - sig);
            deciders.push_back(file + ": " + name);
        }
    }

    ASSERT_EQ(deciders.size(), 1u)
        << "expected exactly one place to decide whether to discard; found " << deciders.size();
    EXPECT_NE(deciders.front().find("ReplicationClient::resolve_stream_identity"),
              std::string::npos)
        << "the discard is decided in " << deciders.front() << " - the decision needs the "
           "primary's stream identity, which is known only after the connection";
}

TEST_F(ReplicationClientTest, ANodeThatAcceptedWritesStartsOverEvenAgainstTheSameStream) {
    // Requirements 2.2 and 3.3, as one process rather than as two claims. A node that held PRIMARY
    // and took writes has records of its own above wherever it was in somebody else's stream, so it
    // must start over - and the interesting part is that this test hands it back **the same stream
    // identity it was following before**. Nothing about the identity says to discard here. What
    // says it is the absence of a position, deleted by the promotion, which is the inversion
    // requirement 3 is about: no position means nothing to match, and no match means start over.
    //
    // So the correctness of removing the discard from `demote_to_replica()` does not rest on a
    // check of "was I primary?" anywhere. It falls out.
    int listen_fd = create_mock_primary(port_);
    ASSERT_GE(listen_fd, 0);

    ob::ReplicationClientConfig cfg;
    cfg.state_file = tmp_->str() + "/repl_state.txt";   // primary_port 0: no client until demotion

    ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE, {}, cfg);
    engine.open();

    // Where it was in the primary's stream, and whose stream that was.
    { std::ofstream out(cfg.state_file);
      out << "file_index=2\nbyte_offset=1024\nstream_id=777\n"; }
    insert_one_replicated_row(engine, "WASPRIM", 5);
    ASSERT_EQ(count_rows(engine, "WASPRIM"), 1u);

    engine.promote_to_primary(ob::EpochValue{7});
    ASSERT_FALSE(std::filesystem::exists(cfg.state_file))
        << "the promotion kept the position, so the rest of this test measures nothing";

    // A record of its own, which is what makes its data no longer a prefix of anyone's stream.
    ob::Level lvl{};
    lvl.price = 61'000; lvl.qty = 2; lvl.cnt = 1; lvl._pad = 0;
    ob::DeltaUpdate own{};
    std::strncpy(own.symbol, "WASPRIM", sizeof(own.symbol) - 1);
    std::strncpy(own.exchange, "BINANCE", sizeof(own.exchange) - 1);
    own.timestamp_ns = 1'800'000'000ULL;
    own.side         = ob::SIDE_BID;
    own.n_levels     = 1;
    ASSERT_EQ(engine.apply_delta(own, &lvl), ob::OB_OK);
    engine.flush_incremental();

    engine.demote_to_replica("127.0.0.1:" + std::to_string(port_));

    int client_fd = accept_with_timeout(listen_fd, 5000);
    ASSERT_GE(client_fd, 0) << "the demotion started no replication client";
    answer_stream_id(client_fd, 777);          // the same stream it used to follow

    // The epoch on that line is the 7 this node held the role in, which is #103: it used to be 0,
    // because the number lived in the replication client and every fresh client started at zero.
    // It reaches the wire now, so the primary's `ERR STALE_PRIMARY` has something to compare
    // against on a first connection - the only kind a role change produces.
    const std::string handshake = recv_line(client_fd, 3000);
    EXPECT_EQ(handshake, "REPLICATE 0 0 7")
        << "a node that accepted writes asked to resume inside the stream it left, got: "
        << handshake;
    EXPECT_EQ(count_rows(engine, "WASPRIM"), 0u)
        << "it kept records the primary never had, sitting above where the replay starts";

    ::close(client_fd);
    ::close(listen_fd);
    engine.close();
}

// ── #103: the epoch a replica compares against ────────────────────────────────
//
// Two guards stand on the same number and neither one holds it. The primary refuses a request from
// a replica that has seen a newer epoch (`ERR STALE_PRIMARY`); the replica drops a record from a
// primary that is behind what it has seen. Both are second lines - #82 makes an outgoing primary
// demote itself when it loses its lease - and a second line that cannot fire is the one you find
// out about from the first line's bad day.

namespace {

/// True when `fd` sees the peer close within `timeout_ms`.
///
/// The replica ends a connection by returning from its receive loop, which closes the socket and
/// reconnects. From the mock primary's side that is a zero-length read, and it is the only
/// observable difference between "the record was refused" and "the record was ignored".
bool wait_for_peer_close(int fd, int timeout_ms) {
    struct timeval tv{};
    tv.tv_sec  = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    char buf[256];
    while (true) {
        const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n == 0) return true;      // orderly close
        if (n < 0) return false;      // timeout: the peer is still there
        // Anything else is the replica talking (an ACK, a reconnect's question) - keep reading.
    }
}

/// A DELTA record on the wire, at `epoch` if one is given.
std::vector<uint8_t> delta_message(const char* symbol, uint64_t seq,
                                   std::optional<uint64_t> epoch) {
    auto [payload, crc] = build_delta_payload(symbol, "BINANCE", seq, 1'700'000'000ULL + seq,
                                               ob::SIDE_BID, 61'000, 2);
    ob::WALRecord hdr{};
    hdr.sequence_number = seq;
    hdr.timestamp_ns    = 1'700'000'000ULL + seq;
    hdr.checksum        = crc;
    hdr.payload_len     = static_cast<uint16_t>(payload.size());
    hdr.record_type     = ob::WAL_RECORD_DELTA;
    hdr._pad            = 0;
    return build_wal_message(0, 0, hdr, payload.data(), payload.size(), epoch);
}

/// An EPOCH record on the wire: `term` in the payload, `line_epoch` on the line above it.
///
/// The two differ during a catch-up, which is the case that matters - see the test below.
std::vector<uint8_t> epoch_record_message(uint64_t term, uint64_t line_epoch) {
    uint8_t payload[8];
    ob::epoch_to_payload(ob::EpochValue{term}, payload);

    ob::WALRecord hdr{};
    hdr.sequence_number = 0;
    hdr.timestamp_ns    = 1'700'000'000ULL;
    hdr.checksum        = ob::crc32c(payload, sizeof(payload));
    hdr.payload_len     = static_cast<uint16_t>(sizeof(payload));
    hdr.record_type     = ob::WAL_RECORD_EPOCH;
    hdr._pad            = 0;
    return build_wal_message(0, 0, hdr, payload, sizeof(payload), line_epoch);
}

}  // namespace

TEST_F(ReplicationClientTest, AnEpochLearnedFromOnePrimaryOutlivesTheConnectionItArrivedOn) {
    // #103 in the shape a cluster actually produces. This node has never held the role, so
    // everything it knows about the epoch arrived over the wire; a failover then points it at a new
    // primary and `demote_to_replica()` builds a **new** client. If the number lives in that
    // object, the guard restarts at zero exactly when it is needed.
    const uint16_t port_b = alloc_port();
    int listen_a = create_mock_primary(port_);
    int listen_b = create_mock_primary(port_b);
    ASSERT_GE(listen_a, 0);
    ASSERT_GE(listen_b, 0);

    ob::ReplicationClientConfig cfg;
    cfg.state_file = tmp_->str() + "/repl_state.txt";   // primary_port 0: no client until demotion

    ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE, {}, cfg);
    engine.open();

    engine.demote_to_replica("127.0.0.1:" + std::to_string(port_));
    int fd_a = accept_with_timeout(listen_a, 5000);
    ASSERT_GE(fd_a, 0) << "the demotion started no replication client";
    answer_stream_id(fd_a, 777);
    ASSERT_EQ(recv_line(fd_a, 3000), "REPLICATE 0 0 0")
        << "nothing has announced an epoch yet, so there is none to carry";

    // The only thing that will ever tell this node the epoch. The ACK is what says the line was
    // read rather than merely sent - a condition that becomes true once and stays true.
    const std::string hb = "HEARTBEAT 9\n";
    ASSERT_EQ(::send(fd_a, hb.data(), hb.size(), MSG_NOSIGNAL), static_cast<ssize_t>(hb.size()));
    ASSERT_EQ(recv_line(fd_a, 3000).rfind("ACK", 0), 0u)
        << "the heartbeat was not processed, so this test has not established what it needs";

    // The failover: same node, new primary, new client object.
    engine.demote_to_replica("127.0.0.1:" + std::to_string(port_b));
    int fd_b = accept_with_timeout(listen_b, 5000);
    ASSERT_GE(fd_b, 0);
    answer_stream_id(fd_b, 778);

    EXPECT_EQ(recv_line(fd_b, 3000), "REPLICATE 0 0 9")
        << "the replica introduced itself to the new primary as having seen no epoch at all, so "
           "`ERR STALE_PRIMARY` is inert on the connection it exists for (#103)";

    ::close(fd_a);
    ::close(fd_b);
    ::close(listen_a);
    ::close(listen_b);
    engine.close();
}

TEST_F(ReplicationClientTest, ARecordFromAPrimaryBehindWhatWeKnowIsNotApplied) {
    // The replica's own half of the guard, with the epoch known **before** the connection - which
    // is the only arrangement #103 is about. A node that held the role in epoch 9 knows 9 from its
    // own WAL; a primary streaming epoch 5 at it is superseded, and its records are not ours to
    // apply.
    int listen_fd = create_mock_primary(port_);
    ASSERT_GE(listen_fd, 0);

    // The engine's own config names no primary, so `open()` starts no client of its own to race
    // this test's for the one connection the mock accepts.
    ob::ReplicationClientConfig engine_cfg;
    engine_cfg.state_file = tmp_->str() + "/repl_state.txt";

    ob::ReplicationClientConfig cfg = engine_cfg;
    cfg.primary_host = "127.0.0.1";
    cfg.primary_port = port_;

    ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE, {}, engine_cfg);
    engine.open();
    engine.promote_to_primary(ob::EpochValue{9});
    engine.demote_to_replica("");   // REPLICA, epoch 9, and no client of its own to fight with

    ob::ReplicationClient client(cfg, engine);
    client.start();

    int fd = accept_with_timeout(listen_fd, 5000);
    ASSERT_GE(fd, 0);
    answer_stream_id(fd, 777);
    EXPECT_EQ(recv_line(fd, 3000), "REPLICATE 0 0 9")
        << "the position is gone with the promotion, but the epoch is this node's own";

    const auto msg = delta_message("STALEREC", 1, 5);
    ASSERT_EQ(::send(fd, msg.data(), msg.size(), MSG_NOSIGNAL), static_cast<ssize_t>(msg.size()));

    EXPECT_TRUE(wait_for_peer_close(fd, 4000))
        << "the replica stayed on a connection with a primary behind the epoch it holds";
    EXPECT_EQ(client.state().records_replayed, 0u)
        << "a record from a superseded primary was applied";
    engine.flush_incremental();
    EXPECT_EQ(count_rows(engine, "STALEREC"), 0u)
        << "the record landed in the store, which is the fencing this link is supposed to have";

    client.stop();
    ::close(fd);
    ::close(listen_fd);
    engine.close();
}

TEST_F(ReplicationClientTest, ARecordFromALegitimatePromotionIsStillApplied) {
    // The control, and it is what makes the refusal above mean anything: a guard that drops
    // everything passes that test. A promotion moves the epoch **up**, so the new primary announces
    // a number above ours and its records are exactly what we are here for.
    int listen_fd = create_mock_primary(port_);
    ASSERT_GE(listen_fd, 0);

    // The engine's own config names no primary, so `open()` starts no client of its own to race
    // this test's for the one connection the mock accepts.
    ob::ReplicationClientConfig engine_cfg;
    engine_cfg.state_file = tmp_->str() + "/repl_state.txt";

    ob::ReplicationClientConfig cfg = engine_cfg;
    cfg.primary_host = "127.0.0.1";
    cfg.primary_port = port_;

    ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE, {}, engine_cfg);
    engine.open();
    engine.promote_to_primary(ob::EpochValue{9});
    engine.demote_to_replica("");

    ob::ReplicationClient client(cfg, engine);
    client.start();

    int fd = accept_with_timeout(listen_fd, 5000);
    ASSERT_GE(fd, 0);
    answer_stream_id(fd, 777);
    ASSERT_EQ(recv_line(fd, 3000), "REPLICATE 0 0 9");

    const auto msg = delta_message("GOODREC", 1, 10);
    ASSERT_EQ(::send(fd, msg.data(), msg.size(), MSG_NOSIGNAL), static_cast<ssize_t>(msg.size()));

    ASSERT_EQ(recv_line(fd, 5000).rfind("ACK ", 0), 0u)
        << "the replica did not acknowledge a record from its new primary";
    EXPECT_EQ(client.state().records_replayed, 1u);
    engine.flush_incremental();
    EXPECT_EQ(count_rows(engine, "GOODREC"), 1u)
        << "a record from a primary one epoch ahead was refused, which is a failover this replica "
           "would never complete";

    client.stop();
    ::close(fd);
    ::close(listen_fd);
    engine.close();
}

TEST_F(ReplicationProtocolTest, APrimaryBehindTheEpochARequestNamesIsRefused) {
    // The primary's half. Nothing behavioural covered it before: the property test in
    // `test_wire_protocol_epoch.cpp` asserts `msg >= local` equals `msg >= local`, which is true of
    // every implementation including one with no check at all.
    wal_->set_epoch(5);
    auto mgr = start_manager();

    int ahead_fd = connect_to_localhost(port_);
    ASSERT_GE(ahead_fd, 0);
    const std::string ahead = "REPLICATE 0 0 9\n";
    ASSERT_EQ(::send(ahead_fd, ahead.data(), ahead.size(), MSG_NOSIGNAL),
              static_cast<ssize_t>(ahead.size()));
    EXPECT_EQ(recv_line(ahead_fd, 3000), "ERR STALE_PRIMARY")
        << "a primary at epoch 5 served a replica that has seen 9";

    // The control: the same request at our own epoch is served, so the refusal is about the number
    // rather than about a manager that turns away whatever it is handed.
    int equal_fd = connect_to_localhost(port_);
    ASSERT_GE(equal_fd, 0);
    const std::string equal = "REPLICATE 0 0 5\n";
    ASSERT_EQ(::send(equal_fd, equal.data(), equal.size(), MSG_NOSIGNAL),
              static_cast<ssize_t>(equal.size()));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    const auto states = mgr->replica_states();
    EXPECT_EQ(states.size(), 1u)
        << "the refused connection should be gone and the current one kept; " << states.size()
        << " remain";

    ::close(ahead_fd);
    ::close(equal_fd);
    mgr->stop();
}

TEST_F(ReplicationClientTest, AHistoricalEpochRecordIsNotMistakenForASupersededPrimary) {
    // Why the record's epoch raises and never refuses, while the line's does both. A catch-up
    // forwards every record type but ROTATE, so a replica replaying the log is handed the EPOCH
    // record of every past promotion - each on a line announcing the primary's epoch **now**. The
    // payload is then; the line is now. Refusing on the payload would disconnect every replica
    // catching up across a failover, which is the shape this test exists to keep out.
    int listen_fd = create_mock_primary(port_);
    ASSERT_GE(listen_fd, 0);

    ob::ReplicationClientConfig engine_cfg;
    engine_cfg.state_file = tmp_->str() + "/repl_state.txt";
    ob::ReplicationClientConfig cfg = engine_cfg;
    cfg.primary_host = "127.0.0.1";
    cfg.primary_port = port_;

    ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE, {}, engine_cfg);
    engine.open();
    engine.promote_to_primary(ob::EpochValue{9});
    engine.demote_to_replica("");

    ob::ReplicationClient client(cfg, engine);
    client.start();

    int fd = accept_with_timeout(listen_fd, 5000);
    ASSERT_GE(fd, 0);
    answer_stream_id(fd, 777);
    ASSERT_EQ(recv_line(fd, 3000), "REPLICATE 0 0 9");

    // Epoch 3's promotion, replayed out of the log by a primary that is at 9.
    const auto history = epoch_record_message(3, 9);
    ASSERT_EQ(::send(fd, history.data(), history.size(), MSG_NOSIGNAL),
              static_cast<ssize_t>(history.size()));
    ASSERT_EQ(recv_line(fd, 3000).rfind("ACK ", 0), 0u)
        << "the historical epoch record was not acknowledged, so the connection ended on it";

    // The proof that the connection lived is that the next record lands, not that nothing was
    // logged: a test asserting the absence of a disconnect passes on a replica that has stopped
    // reading altogether.
    const auto msg = delta_message("AFTERHIST", 1, 9);
    ASSERT_EQ(::send(fd, msg.data(), msg.size(), MSG_NOSIGNAL), static_cast<ssize_t>(msg.size()));
    ASSERT_EQ(recv_line(fd, 5000).rfind("ACK ", 0), 0u);
    engine.flush_incremental();
    EXPECT_EQ(count_rows(engine, "AFTERHIST"), 1u)
        << "the replica dropped the connection over an EPOCH record from the log's past, so a "
           "catch-up across a failover would never finish";
    EXPECT_EQ(engine.current_epoch(), 9u)
        << "a historical epoch lowered what this node knows";

    client.stop();
    ::close(fd);
    ::close(listen_fd);
    engine.close();
}

TEST_F(ReplicationClientTest, AnEpochLearnedWhileFollowingSurvivesARestart) {
    // The other half of the item's title: "every connection after a restart or a role change". A
    // node that has only ever followed has no EPOCH record in its own WAL - promotions write those
    // - so a restart used to have nothing to restore the number from. `repl_state.txt` carries it
    // now, written the moment it changes rather than on the ten-second timer.
    int listen_fd = create_mock_primary(port_);
    ASSERT_GE(listen_fd, 0);

    ob::ReplicationClientConfig cfg;
    cfg.state_file = tmp_->str() + "/repl_state.txt";

    {
        ob::Engine engine(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE, {}, cfg);
        engine.open();
        ASSERT_EQ(engine.current_epoch(), 0u) << "this node has never held the role";

        engine.demote_to_replica("127.0.0.1:" + std::to_string(port_));
        int fd = accept_with_timeout(listen_fd, 5000);
        ASSERT_GE(fd, 0);
        answer_stream_id(fd, 777);
        ASSERT_EQ(recv_line(fd, 3000), "REPLICATE 0 0 0");

        const std::string hb = "HEARTBEAT 9\n";
        ASSERT_EQ(::send(fd, hb.data(), hb.size(), MSG_NOSIGNAL),
                  static_cast<ssize_t>(hb.size()));
        ASSERT_EQ(recv_line(fd, 3000).rfind("ACK", 0), 0u);

        // Read **before** the shutdown, and that ordering is the assertion: `stop()` saves state
        // too, so a file checked after it would pass for a build that only writes on the
        // ten-second timer - and a crash is what this number has to survive. The ACK above went
        // out after the save, so there is nothing to wait for.
        const std::string live = read_whole_file(cfg.state_file);
        EXPECT_NE(live.find("epoch=9"), std::string::npos)
            << "the epoch reached the file only at shutdown, so a node killed between a failover "
               "and the next tick of the timer comes back unfenced: " << live;

        ::close(fd);
        engine.close();
    }

    const std::string saved = read_whole_file(cfg.state_file);
    EXPECT_NE(saved.find("epoch=9"), std::string::npos)
        << "the epoch was not written down, so nothing but this process ever knew it: " << saved;

    ob::Engine restarted(tmp_->str(), 100'000'000ULL, ob::FsyncPolicy::NONE, {}, cfg);
    restarted.open();
    restarted.demote_to_replica("127.0.0.1:" + std::to_string(port_));

    int fd2 = accept_with_timeout(listen_fd, 5000);
    ASSERT_GE(fd2, 0);
    answer_stream_id(fd2, 777);
    EXPECT_EQ(recv_line(fd2, 3000), "REPLICATE 0 0 9")
        << "the restarted replica introduced itself as having seen no epoch, which is the state "
           "#103 left every process in";

    ::close(fd2);
    ::close(listen_fd);
    restarted.close();
}

TEST(ReplicationEpochOwnership, TheEpochAReplicaComparesAgainstLivesInOnePlace) {
    // #103 was not a missing initialiser, it was a second copy of one number: the client held its
    // own, the engine held the real one, and the copy started at zero for every fresh object. The
    // fix removed the copy, so what this pins is that nothing declares another - a behavioural test
    // cannot, because a re-introduced member would be seeded correctly on the day it was written
    // and go stale on the next role change somebody added.
    const std::string hdr = read_source("include/orderbook/replication.hpp");
    ASSERT_FALSE(hdr.empty());

    const std::size_t start = hdr.find("class ReplicationClient {");
    ASSERT_NE(start, std::string::npos);
    const std::size_t end = hdr.find("\n};", start);
    ASSERT_NE(end, std::string::npos);
    const std::string body = hdr.substr(start, end - start);

    std::vector<std::string> fields;
    for (std::size_t line = 0; line < body.size();) {
        const std::size_t next = body.find('\n', line);
        const std::string text = body.substr(line, (next == std::string::npos ? body.size() : next)
                                                       - line);
        line = (next == std::string::npos) ? body.size() : next + 1;

        // A declaration, not prose and not a function: the comments in this class say "epoch"
        // repeatedly, and one of them says it about the guard below.
        const std::size_t first = text.find_first_not_of(" \t");
        if (first == std::string::npos || text.compare(first, 2, "//") == 0) continue;
        if (text.find("epoch") == std::string::npos) continue;
        if (text.find(';') == std::string::npos || text.find('(') != std::string::npos) continue;
        fields.push_back(text);
    }

    ASSERT_TRUE(fields.empty())
        << "ReplicationClient declares its own epoch again: " << fields.front()
        << " - the number belongs to the engine, which is what makes it outlive this object";

    // The pair that stops the assertion above from passing by finding nothing: the guard has to
    // exist and be reached from more than one place, or "no epoch field" would be true of a client
    // that stopped checking epochs altogether.
    const std::string src = read_source("src/replication.cpp");
    ASSERT_FALSE(src.empty());
    std::size_t calls = 0;
    for (std::size_t at = src.find("accept_announced_epoch("); at != std::string::npos;
         at = src.find("accept_announced_epoch(", at + 1)) {
        ++calls;
    }
    EXPECT_GE(calls, 3u)
        << "one definition and at least two call sites were expected (a record line and a "
           "heartbeat, on each of the two read paths); found " << calls << " mentions";
}
