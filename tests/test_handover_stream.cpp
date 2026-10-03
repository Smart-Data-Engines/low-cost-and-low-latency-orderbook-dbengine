// #204: a node that steps down closes writes first and keeps serving its stream until it follows
// somebody.
//
// Measured before this existed, through the server on the m9g.xlarge: a planned FAILOVER's target
// lacked the last acknowledged writes in nine runs of ten - 1 to 4 on Release, and 4308 once on
// Debug. `demote_to_replica()` stopped the replication manager before it turned read-only, so the
// writes taken while `stop()` joined its thread reached the WAL and an OK but no replica, and
// whatever the target had not received yet had nowhere left to come from.

#include "orderbook/engine.hpp"
#include "orderbook/replication.hpp"
#include "orderbook/stream_position.hpp"
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
#include <functional>
#include <string>
#include <thread>
#include <vector>

namespace {

using namespace std::chrono_literals;

std::atomic<uint16_t> g_port{ob::test::kPortsHandoverStream};
uint16_t alloc_port() { return g_port.fetch_add(1, std::memory_order_relaxed); }

struct TempDir {
    std::filesystem::path path;
    explicit TempDir(const std::string& tag) {
        static std::atomic<uint64_t> counter{0};
        path = std::filesystem::temp_directory_path() /
               ("ob_handover_stream_" + tag + "_" + std::to_string(::getpid()) + "_" +
                std::to_string(counter.fetch_add(1, std::memory_order_relaxed)));
        std::filesystem::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
    std::string str() const { return path.string(); }
};

ob::ReplicationConfig primary_config(uint16_t port) {
    ob::ReplicationConfig cfg{};
    cfg.port = port;
    cfg.max_replicas = 4;
    return cfg;
}

ob::ReplicationClientConfig replica_config(uint16_t port, const std::string& state_file) {
    ob::ReplicationClientConfig cfg{};
    cfg.primary_host = "127.0.0.1";
    cfg.primary_port = port;
    cfg.state_file = state_file;
    return cfg;
}

ob::ob_status_t write(ob::Engine& engine, const char* symbol, uint64_t seq) {
    ob::DeltaUpdate delta{};
    std::strncpy(delta.symbol, symbol, sizeof(delta.symbol) - 1);
    std::strncpy(delta.exchange, "EX", sizeof(delta.exchange) - 1);
    delta.sequence_number = seq;
    delta.timestamp_ns = 1'000'000'000ULL + seq;
    delta.side = ob::SIDE_BID;
    delta.n_levels = 1;
    ob::Level level{};
    level.price = static_cast<int64_t>(100'000 + seq);
    level.qty = 1;
    level.cnt = 1;
    return engine.apply_delta(delta, &level);
}

bool wait_until(const std::function<bool()>& done, std::chrono::milliseconds limit) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < deadline) {
        if (done()) return true;
        std::this_thread::sleep_for(20ms);
    }
    return done();
}

bool something_listens_on(uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    const bool connected = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
    ::close(fd);
    return connected;
}

/// Records the replica applied, counted once each: `repl_records_replayed` also counts a record that
/// arrived twice - by catch-up and live, around a connect - and was dropped as a duplicate.
uint64_t applied_once(ob::Engine& replica) {
    return replica.stats().repl_records_replayed -
           replica.registry().counter_value("ob_replication_duplicates_dropped");
}

bool covers(ob::Engine& replica, const ob::StreamPosition& end) {
    const auto have = replica.replicated_position();
    return have.has_value() && ob::stream_covers(*have, end);
}

TEST(HandoverStream, ASteppedDownPrimaryRefusesWritesAndItsReplicaReachesTheAnnouncedEnd) {
    const uint16_t port = alloc_port();
    TempDir pdir("primary"), rdir("replica");
    ob::Engine primary(pdir.str(), 100'000'000ULL, ob::FsyncPolicy::NONE, primary_config(port), {});
    primary.open();
    ob::Engine replica(rdir.str(), 100'000'000ULL, ob::FsyncPolicy::NONE, {},
                       replica_config(port, rdir.str() + "/repl_state.txt"));
    replica.open();

    for (uint64_t i = 1; i <= 200; ++i) ASSERT_EQ(write(primary, "HS", i), ob::OB_OK);

    const auto end = primary.step_down_for_handover();
    ASSERT_TRUE(end.has_value()) << "a node serving a stream announced no end for it";
    EXPECT_EQ(end->stream_id, primary.wal_identity());

    // Closed in the engine, not only behind the server's read-only check.
    EXPECT_EQ(write(primary, "HS", 201), ob::OB_ERR_READ_ONLY);

    // The stream stays up, so the replica takes the rest of it - to the end announced and no
    // further: nothing acknowledged lies past it, and nothing was appended after it.
    ASSERT_TRUE(wait_until([&] { return covers(replica, *end); }, 15s))
        << "the replica never reached the end the stepped-down primary announced";
    EXPECT_EQ(applied_once(replica), 200u);
    const auto have = replica.replicated_position();
    ASSERT_TRUE(have.has_value());
    EXPECT_EQ(have->file_index, end->file_index);
    EXPECT_EQ(have->offset, end->offset);

    replica.close();
    primary.close();
}

TEST(HandoverStream, ADemotionWithNobodyToFollowLeavesTheStreamUp) {
    const uint16_t port = alloc_port();
    TempDir pdir("primary"), rdir("replica");
    ob::Engine primary(pdir.str(), 100'000'000ULL, ob::FsyncPolicy::NONE, primary_config(port), {});
    primary.open();
    for (uint64_t i = 1; i <= 50; ++i) ASSERT_EQ(write(primary, "HN", i), ob::OB_OK);

    primary.demote_to_replica("");
    EXPECT_EQ(write(primary, "HN", 51), ob::OB_ERR_READ_ONLY);

    // A replica that connects only now still takes the whole log from the node that stepped down -
    // which is what a lease lost with nobody elected leaves the cluster's replicas to do.
    ob::Engine replica(rdir.str(), 100'000'000ULL, ob::FsyncPolicy::NONE, {},
                       replica_config(port, rdir.str() + "/repl_state.txt"));
    replica.open();
    EXPECT_TRUE(wait_until([&] { return applied_once(replica) == 50u; }, 15s))
        << "applied " << applied_once(replica) << " of 50: the demotion took the stream down with it";

    replica.close();
    primary.close();
}

TEST(HandoverStream, ADemotionToAPrimaryEndsTheStream) {
    // The other half: a node that follows somebody serves no stream of its own.
    const uint16_t port = alloc_port();
    const uint16_t elsewhere = alloc_port();   // nothing listens there; the client just retries
    TempDir pdir("primary");
    ob::Engine primary(pdir.str(), 100'000'000ULL, ob::FsyncPolicy::NONE, primary_config(port), {});
    primary.open();
    ASSERT_TRUE(wait_until([&] { return something_listens_on(port); }, 5s));

    primary.demote_to_replica("127.0.0.1:" + std::to_string(elsewhere));
    EXPECT_FALSE(something_listens_on(port))
        << "the replication port still listens on a node that follows another primary";

    primary.close();
}

TEST(HandoverStream, AWriteRacingAStepDownIsInTheStreamOrRefused) {
    const uint16_t port = alloc_port();
    TempDir pdir("primary"), rdir("replica");
    ob::Engine primary(pdir.str(), 100'000'000ULL, ob::FsyncPolicy::NONE, primary_config(port), {});
    primary.open();
    ob::Engine replica(rdir.str(), 100'000'000ULL, ob::FsyncPolicy::NONE, {},
                       replica_config(port, rdir.str() + "/repl_state.txt"));
    replica.open();

    constexpr int kWriters = 4;
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> acknowledged{0}, refused{0}, other{0};
    std::vector<std::thread> writers;
    for (int w = 0; w < kWriters; ++w) {
        writers.emplace_back([&, w] {
            const std::string symbol = "HR" + std::to_string(w);
            for (uint64_t seq = 1; !stop.load(std::memory_order_relaxed); ++seq) {
                const ob::ob_status_t st = write(primary, symbol.c_str(), seq);
                if (st == ob::OB_OK) acknowledged.fetch_add(1);
                else if (st == ob::OB_ERR_READ_ONLY) refused.fetch_add(1);
                else other.fetch_add(1);
            }
        });
    }
    std::this_thread::sleep_for(100ms);
    const auto end = primary.step_down_for_handover();
    std::this_thread::sleep_for(50ms);   // writers keep knocking on a closed node
    stop.store(true);
    for (auto& t : writers) t.join();

    ASSERT_TRUE(end.has_value());
    EXPECT_EQ(other.load(), 0u);
    EXPECT_GT(refused.load(), 0u) << "no writer reached the node after it stepped down";
    ASSERT_TRUE(wait_until([&] { return covers(replica, *end); }, 30s));
    // Every acknowledged write is in the stream, and nothing past its announced end is.
    std::this_thread::sleep_for(200ms);
    EXPECT_EQ(applied_once(replica), acknowledged.load());
    const auto have = replica.replicated_position();
    ASSERT_TRUE(have.has_value());
    EXPECT_EQ(have->file_index, end->file_index);
    EXPECT_EQ(have->offset, end->offset);

    replica.close();
    primary.close();
}

}  // namespace
