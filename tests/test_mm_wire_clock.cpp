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

}  // namespace

TEST(MmWireClock, AnHourInTheFutureOnTheWireIsRefusedAndTheClockDoesNotMove) {
    // #121's decision, on the wire. Until it was taken, this test asserted the opposite - that an
    // hour off the wire *became* this node's clock and stayed - because that was what the engine
    // did and a measurement is worth pinning even when the behaviour is wrong.
    //
    // Four answers were available and three of them are worse, which is why this one is a refusal
    // of the **peer** rather than of the record, the clock or the value:
    //
    //  * clamping what we absorb is decided per node against *that node's* wall clock, so two
    //    nodes resolve the same LWW conflict differently and the values diverge. An untrue clock
    //    beats divergent data.
    //  * refusing the record and keeping the link loses the write while the peer believes it
    //    replicated, which is a silent hole.
    //  * absorbing anything is what #121 measured: the mesh's clock becomes the maximum of its
    //    members' clocks and stays there for as long as that member keeps writing.
    const uint16_t port = g_port.fetch_add(1, std::memory_order_relaxed);
    TempDir tmp("mm_wire_clock_");
    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::NONE, {}, {}, {}, {},
                      mm_config(1, port));
    engine.open();
    ASSERT_NE(engine.multi_master_manager(), nullptr);
    ASSERT_NE(engine.hlc(), nullptr);

    const uint64_t skewed = ob::wall_clock_ns() + 3'600'000'000'000ULL;   // one hour ahead
    ASSERT_GT(skewed, ob::wall_clock_ns() + ob::MM_MAX_CLOCK_SKEW_NS)
        << "the skew this test sends is inside the bound, so it would be absorbed and this test "
        << "would assert nothing";
    const uint64_t refused_before =
        engine.registry().counter_value("ob_mm_peer_dropped_clock_total");

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

    // The counter is the assertion that the record *arrived* and was refused, rather than never
    // having been parsed at all - which would make everything below true for the wrong reason.
    ASSERT_TRUE(eventually([&] {
        return engine.registry().counter_value("ob_mm_peer_dropped_clock_total") > refused_before;
    })) << "nothing was refused, so this test is measuring a frame that never landed";

    EXPECT_LT(engine.hlc()->current().physical_ns, skewed)
        << "the hour off the wire reached this node's clock anyway: it reads "
        << engine.hlc()->current().physical_ns;
    EXPECT_LE(engine.hlc()->tick_local().physical_ns,
              ob::wall_clock_ns() + ob::MM_MAX_CLOCK_SKEW_NS)
        << "the next local write is stamped outside the bound, so the refusal did not keep the "
        << "clock - which is the only thing it was for";

    // The peer is gone, and its claim is **not**: `last_hlc` is recorded before the verdict on
    // purpose, so an operator reading MM_PEERS sees the number that got it dropped instead of
    // having to take the log's word for it.
    EXPECT_TRUE(eventually([&] {
        for (const auto& p : engine.multi_master_manager()->peer_states()) {
            if (p.node_id == 7) return !p.connected;
        }
        return true;   // the record may be gone entirely, which is also "not connected"
    })) << "the peer whose clock is wrong is still connected, so its next frame moves the clock";
    for (const auto& p : engine.multi_master_manager()->peer_states()) {
        if (p.node_id == 7) {
            EXPECT_EQ(p.last_hlc.physical_ns, skewed)
                << "the peer's claimed timestamp was not kept, so the evidence for the drop is "
                << "only in the log";
        }
    }

    // The cost, asserted rather than described: the record did not arrive. That is the half of
    // this decision an operator pays for, and a test that only checked the clock would let someone
    // believe the data came too.
    engine.flush_incremental();
    size_t rows = 0;
    const std::string err = engine.execute(
        "SELECT timestamp, price, quantity FROM 'WIRECLK'.'EX' "
        "WHERE timestamp BETWEEN 0 AND 9999999999999999999",
        [&](const ob::QueryResult&) { ++rows; });
    EXPECT_TRUE(err.empty() || rows == 0) << err;
    EXPECT_EQ(rows, 0u) << "the refused record was stored, so the peer was dropped for nothing";

    engine.close();
}

TEST(MmWireClock, AClockInsideTheBoundIsStillAbsorbedAndItsRecordApplied) {
    // The control, and it is what stops the bound above from being a blanket refusal. A node that
    // dropped every peer whose clock differed at all would pass the first test and be useless:
    // clocks always differ. This one sends a minute - four orders of magnitude above working NTP,
    // inside the five-minute bound - and requires both halves to happen: the clock moves *and* the
    // data lands.
    //
    // Same shape as the SDK's `migration/020` beside `errors/038`: two positives are what make a
    // refusal mean something narrower than "no".
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

    const uint64_t inside = ob::wall_clock_ns() + 60'000'000'000ULL;   // one minute ahead
    ASSERT_LT(inside, ob::wall_clock_ns() + ob::MM_MAX_CLOCK_SKEW_NS)
        << "this test's skew is outside the bound, so it would be refused and the control would "
        << "assert the wrong thing";
    const ob::HLCTimestamp remote{inside, 0, 7};
    peer.send_delta(7, 1, remote, "WIRECLK", "EX", 123'456);

    ASSERT_TRUE(eventually([&] { return engine.hlc()->current().physical_ns >= inside; }))
        << "a minute of skew was refused, so the bound is not a bound but a blanket";
    EXPECT_EQ(engine.registry().counter_value("ob_mm_peer_dropped_clock_total"), 0u)
        << "the peer was dropped for a clock inside the bound";

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
