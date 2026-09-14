// A crafted HLC on the wire: what those ten lines in multi_master.cpp do with a peer's clock.
//
// Roadmap #54 stage D3, and the reason it is a wire test rather than another unit test is narrow.
// The substance was established by **reading**: `handle_remote_record()` deserialises the HLC from
// the frame header and hands it to `Engine::apply_remote_delta()` a few lines later, with nothing
// between them - no bound, no sanity window, no rejection. `tests/test_hlc_skew.cpp` pins what the
// clock does with such a value once it arrives (#119, #120, and the question that is #121). What
// neither of those can say is that the *wire* reaches the clock at all: a claim about ten lines of
// parsing is a claim about code until something sends the bytes.
//
// So this file sends the bytes. A socket connects to the mesh port, hands over a handshake claiming
// a node id, and then frames one DELTA record whose HLC says an hour from now. Nothing here is a
// defect report: the behaviour is the documented one (`docs/operations.md`, "When a peer's clock is
// wrong"), and whether it *should* be bounded is #121, filed rather than answered because a ceiling
// costs causal order against exactly the peer whose clock is wrong.

#include "orderbook/crc32c.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/hlc.hpp"
#include "orderbook/multi_master.hpp"
#include "orderbook/query_engine.hpp"
#include "test_ports.hpp"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {

std::atomic<uint64_t> g_dir_counter{0};
std::atomic<uint16_t> g_port{ob::test::kPortsMmWireClock};

struct TempDir {
    std::string path;
    explicit TempDir(const std::string& prefix) {
        auto p = fs::temp_directory_path() /
                 (prefix + std::to_string(g_dir_counter.fetch_add(1, std::memory_order_relaxed)));
        fs::create_directories(p);
        path = p.string();
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
};

constexpr uint64_t kNoAutoFlush = 3'600'000'000'000ULL;

ob::MultiMasterConfig mm_config(uint16_t node_id, uint16_t port) {
    ob::MultiMasterConfig mm{};
    mm.enabled                   = true;
    mm.node_id                   = node_id;
    mm.replication_port          = port;
    mm.compress                  = false;
    mm.max_catchup_bytes         = 1 << 20;
    mm.anti_entropy_interval_sec = 3600;
    return mm;
}

/// A socket on a node's mesh port that can hand over a handshake and then frame records.
///
/// The node speaks first - with no cluster secret it queues its own handshake as soon as it has
/// accepted - so the first byte arriving here is the readiness signal, rather than a sleep.
class MeshClient {
public:
    explicit MeshClient(uint16_t port) {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd_ < 0) throw std::runtime_error("socket() failed");
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port   = htons(port);
        ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
        if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            ::close(fd_);
            fd_ = -1;
            throw std::runtime_error("connect to mesh port failed");
        }
        timeval tv{};
        tv.tv_sec = 2;
        ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }
    ~MeshClient() { close(); }
    MeshClient(const MeshClient&) = delete;
    MeshClient& operator=(const MeshClient&) = delete;

    void close() {
        if (fd_ >= 0) ::close(fd_);
        fd_ = -1;
    }

    bool wait_for_bytes() {
        uint8_t buf[512];
        return ::recv(fd_, buf, sizeof(buf), 0) > 0;
    }

    void send_handshake(uint16_t node_id) {
        ob::HandshakeMessage msg{};
        msg.node_id                = node_id;
        msg.protocol_version       = ob::MM_PROTOCOL_VERSION;
        msg.compression_preference = 0;
        msg.wal_file_index         = 0;
        msg.wal_byte_offset        = 0;

        uint8_t frame[ob::MM_FRAME_HEADER_SIZE + ob::MM_HANDSHAKE_SIZE];
        const uint32_t len = ob::MM_HANDSHAKE_SIZE;
        std::memcpy(frame, &len, sizeof(len));
        msg.serialize(frame + ob::MM_FRAME_HEADER_SIZE);
        ASSERT_EQ(::send(fd_, frame, sizeof(frame), MSG_NOSIGNAL),
                  static_cast<ssize_t>(sizeof(frame)));
    }

    /// One DELTA record, framed the way `broadcast_local()` frames one, carrying `hlc` verbatim.
    ///
    /// Built with the production encoder (`ob::encode_frame`) rather than by writing a length
    /// prefix here: a second copy of the framing in a test is a second thing to keep in step, and
    /// this file is about what the *receiver* does with a field, not about how frames are shaped.
    void send_delta(uint16_t origin, uint64_t seq, const ob::HLCTimestamp& hlc,
                    const char* symbol, const char* exchange, int64_t price) {
        ob::DeltaUpdate delta{};
        std::strncpy(delta.symbol, symbol, sizeof(delta.symbol) - 1);
        std::strncpy(delta.exchange, exchange, sizeof(delta.exchange) - 1);
        delta.sequence_number = seq;
        delta.timestamp_ns    = hlc.physical_ns;
        delta.side            = ob::SIDE_BID;
        delta.n_levels        = 1;
        const ob::Level level{price, 5, 1, 0};

        std::vector<uint8_t> payload(sizeof(delta) + sizeof(level));
        std::memcpy(payload.data(), &delta, sizeof(delta));
        std::memcpy(payload.data() + sizeof(delta), &level, sizeof(level));

        ob::WALRecordV2 hdr{};
        hdr.sequence_number = seq;
        hdr.timestamp_ns    = hlc.physical_ns;
        hdr.checksum        = ob::crc32c(payload.data(), payload.size());
        hdr.payload_len     = static_cast<uint16_t>(payload.size());
        hdr.record_type     = ob::WAL_RECORD_DELTA;
        hdr.version         = 1;
        hdr.origin_node_id  = origin;
        hlc.serialize(hdr.hlc_data);

        std::vector<uint8_t> body(sizeof(hdr) + payload.size());
        std::memcpy(body.data(), &hdr, sizeof(hdr));
        std::memcpy(body.data() + sizeof(hdr), payload.data(), payload.size());

        std::vector<uint8_t> wire;
        ob::encode_frame(body.data(), body.size(), wire);
        ASSERT_EQ(::send(fd_, wire.data(), wire.size(), MSG_NOSIGNAL),
                  static_cast<ssize_t>(wire.size()));
    }

private:
    int fd_{-1};
};

template <typename F>
bool eventually(F&& pred, std::chrono::milliseconds budget = std::chrono::milliseconds(5000)) {
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return pred();
}

uint64_t wall_clock_ns() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

}  // namespace

TEST(MmWireClock, AnHourInTheFutureOnTheWireBecomesThisNodesClockAndStays) {
    const uint16_t port = g_port.fetch_add(1, std::memory_order_relaxed);
    TempDir tmp("mm_wire_clock_");
    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::NONE, {}, {}, {}, {},
                      mm_config(1, port));
    engine.open();
    ASSERT_NE(engine.multi_master_manager(), nullptr);
    ASSERT_NE(engine.hlc(), nullptr);

    const uint64_t before = engine.hlc()->current().physical_ns;
    const uint64_t skewed = wall_clock_ns() + 3'600'000'000'000ULL;   // one hour ahead
    ASSERT_GT(skewed, before);

    MeshClient peer(port);
    ASSERT_TRUE(peer.wait_for_bytes()) << "the node did not answer an accepted mesh connection";
    peer.send_handshake(7);

    ASSERT_TRUE(eventually([&] {
        for (const auto& p : engine.multi_master_manager()->peer_states()) {
            if (p.node_id == 7 && p.handshake_done) return true;
        }
        return false;
    })) << "the handshake never named node 7, so the frame below has no peer to arrive from";

    const ob::HLCTimestamp remote{skewed, 0, 7};
    peer.send_delta(7, 1, remote, "WIRECLK", "EX", 100'000);

    // The clock moves, and that is the whole claim: nothing but `tick_receive()` can move it, and
    // nothing but `handle_remote_record()` calls that with a value off the wire.
    EXPECT_TRUE(eventually([&] { return engine.hlc()->current().physical_ns >= skewed; }))
        << "a peer's timestamp an hour in the future did not reach this node's clock: it reads "
        << engine.hlc()->current().physical_ns << ", the frame said " << skewed;

    // And it **stays**, which is the part an operator has to know: the next local write is stamped
    // from the moved clock, not from the wall clock this node can still read. That is why
    // `docs/operations.md` says one skewed peer becomes the cluster's clock until every node
    // restarts, and why bounding it is a decision (#121) rather than an omission.
    const ob::HLCTimestamp local_after = engine.hlc()->tick_local();
    EXPECT_GE(local_after.physical_ns, skewed)
        << "the clock went back to the wall clock between the remote record and the next local "
        << "write, which would make the merge in tick_receive() pointless";
    EXPECT_LT(wall_clock_ns(), skewed)
        << "the wall clock has caught up with the skew, so this test proves nothing about a moved "
        << "clock - pick a larger skew";

    // The drift gauge's source, so the pair #119 and #120 built is fed from the wire too rather
    // than only from a unit test's direct call.
    EXPECT_GE(engine.hlc()->max_drift_ns(), 3'500'000'000'000LL)
        << "the drift a peer introduced is not what the node reports: "
        << engine.hlc()->max_drift_ns();

    engine.close();
}

TEST(MmWireClock, ARecordFromAPeerWhoseClockIsWrongIsStillApplied) {
    // The other half, and it is the one that makes the first half matter: the engine does not
    // refuse the record, so the skew arrives *with* data rather than instead of it. A node that
    // dropped such a record would keep its clock and lose a write, which is the trade #121 would
    // have to make explicit.
    const uint16_t port = g_port.fetch_add(1, std::memory_order_relaxed);
    TempDir tmp("mm_wire_clock_applied_");
    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::NONE, {}, {}, {}, {},
                      mm_config(1, port));
    engine.open();

    MeshClient peer(port);
    ASSERT_TRUE(peer.wait_for_bytes());
    peer.send_handshake(7);
    ASSERT_TRUE(eventually([&] {
        for (const auto& p : engine.multi_master_manager()->peer_states()) {
            if (p.node_id == 7 && p.handshake_done) return true;
        }
        return false;
    }));

    const ob::HLCTimestamp remote{wall_clock_ns() + 3'600'000'000'000ULL, 0, 7};
    peer.send_delta(7, 1, remote, "WIRECLK", "EX", 123'456);

    ASSERT_TRUE(eventually([&] { return engine.hlc()->current().physical_ns >= remote.physical_ns; }))
        << "the frame never arrived, so nothing below is about the record";

    engine.flush_incremental();
    std::vector<int64_t> prices;
    const std::string err = engine.execute(
        "SELECT timestamp, price, quantity FROM 'WIRECLK'.'EX' "
        "WHERE timestamp BETWEEN 0 AND 9999999999999999999",
        [&](const ob::QueryResult& row) { prices.push_back(row.price); });
    EXPECT_TRUE(err.empty()) << err;
    EXPECT_EQ(prices.size(), 1u) << "the record that carried the skew was not stored";
    if (!prices.empty()) {
        EXPECT_EQ(prices[0], 123'456);
    }

    engine.close();
}
