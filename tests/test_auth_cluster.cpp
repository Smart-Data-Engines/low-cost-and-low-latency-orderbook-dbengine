// Cluster-link authentication (#30 part two): replication and multi-master.
//
// What these hold, and what they cannot: the protocol steps and the refusals are checked here
// against real sockets; that three live nodes actually converge with a secret is an integration
// test, because it needs three processes.
//
// The most valuable case is the *positive* one at the end. A gate that refuses everything proves
// nothing, so each refusal here is paired with the exchange that must succeed.

#include "orderbook/auth.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/multi_master.hpp"
#include "orderbook/replication.hpp"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

namespace fs = std::filesystem;

namespace {

constexpr const char* kClusterSecret = "0123456789abcdef0123456789abcdef-cluster";

std::string temp_dir(const std::string& prefix) {
    auto p = fs::temp_directory_path() / (prefix + std::to_string(std::rand()));
    fs::create_directories(p);
    return p.string();
}

ob::SecretStore cluster_store(const std::string& secret = kClusterSecret) {
    const auto path = fs::temp_directory_path() /
                      ("ob_cluster_secret_" + std::to_string(std::rand()));
    {
        std::ofstream out(path, std::ios::trunc);
        out << secret << "\n";
    }
    ::chmod(path.c_str(), 0600);
    auto store = ob::SecretStore::load_cluster_file(path.string());
    fs::remove(path);
    return store;
}

uint16_t free_port() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    socklen_t len = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    const uint16_t port = ntohs(addr.sin_port);
    ::close(fd);
    return port;
}

/// A blocking client socket with line reads, standing in for a replica.
class Peer {
public:
    explicit Peer(uint16_t port) {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
        connected_ = ::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
        timeval tv{};
        tv.tv_sec = 5;
        ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }
    ~Peer() { if (fd_ >= 0) ::close(fd_); }

    bool connected() const { return connected_; }

    void send(const std::string& line) {
        const std::string out = line + "\n";
        auto wr = ::send(fd_, out.data(), out.size(), MSG_NOSIGNAL);
        (void)wr;
    }

    /// One line, or empty on timeout or an orderly close. The two are distinguished by eof().
    std::string line() {
        std::string out;
        char c = 0;
        while (true) {
            const ssize_t n = ::recv(fd_, &c, 1, 0);
            if (n <= 0) { eof_ = (n == 0); return out; }
            if (c == '\n') return out;
            out.push_back(c);
        }
    }

    bool eof() const { return eof_; }

private:
    int  fd_{-1};
    bool connected_{false};
    bool eof_{false};
};

/// A primary with replication and a cluster secret, on its own directory.
class PrimaryFixture : public ::testing::Test {
protected:
    std::string                 dir_;
    uint16_t                    port_{0};
    std::unique_ptr<ob::Engine> engine_;

    void start(bool with_secret) {
        dir_  = temp_dir("auth_cluster_");
        port_ = free_port();
        ob::ReplicationConfig repl{};
        repl.port = port_;
        if (with_secret) repl.cluster_secret = cluster_store();
        engine_ = std::make_unique<ob::Engine>(dir_, 100'000'000ULL, ob::FsyncPolicy::NONE,
                                               repl, ob::ReplicationClientConfig{});
        engine_->open();
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }

    void TearDown() override {
        if (engine_) engine_->close();
        if (!dir_.empty()) fs::remove_all(dir_);
    }
};

/// The initiator-side response, which is what a replica sends.
std::string replication_answer(const std::string& nonce,
                               const std::string& secret = kClusterSecret) {
    return ob::auth_response(secret, ob::AuthSurface::Replication, ob::AuthRole::Initiator, "",
                             nonce);
}

/// The acceptor-side response, which is what a primary sends.
std::string primary_answer(const std::string& nonce,
                           const std::string& secret = kClusterSecret) {
    return ob::auth_response(secret, ob::AuthSurface::Replication, ob::AuthRole::Acceptor, "",
                             nonce);
}

} // namespace

// ── Replication, primary side ─────────────────────────────────────────────────

TEST_F(PrimaryFixture, ThePrimaryChallengesOnAcceptAndCompletesTheExchange) {
    start(/*with_secret=*/true);
    Peer p(port_);
    ASSERT_TRUE(p.connected());

    const std::string challenge = p.line();
    ASSERT_EQ(challenge.rfind("CHALLENGE ", 0), 0u) << challenge;
    const std::string nonce = challenge.substr(std::strlen("CHALLENGE "));
    ASSERT_TRUE(ob::is_auth_hex(nonce)) << nonce;

    // Our challenge first, then our answer: that ordering is what makes the primary's two replies
    // arrive in a known order.
    const std::string our_nonce = ob::generate_nonce_hex();
    p.send("CHALLENGE " + our_nonce);
    p.send("AUTH " + replication_answer(nonce));

    const std::string their_answer = p.line();
    ASSERT_EQ(their_answer.rfind("AUTH ", 0), 0u) << their_answer;
    EXPECT_EQ(their_answer.substr(std::strlen("AUTH ")), primary_answer(our_nonce))
        << "the primary must prove itself too - mutual is the whole point on a cluster link";

    EXPECT_EQ(p.line(), "OK AUTH");
}

TEST_F(PrimaryFixture, ReplicateBeforeAuthenticatingIsRefusedOnTheWireAndDisconnected) {
    start(/*with_secret=*/true);
    Peer p(port_);
    ASSERT_TRUE(p.connected());
    ASSERT_EQ(p.line().rfind("CHALLENGE ", 0), 0u);

    // The error goes out before the close: a replica missing its secret would otherwise see a
    // reconnect loop with no message.
    p.send("REPLICATE 0 0 0");
    EXPECT_EQ(p.line(), "ERR unauthenticated");
    p.line();
    EXPECT_TRUE(p.eof()) << "the primary kept an unauthenticated connection";
}

TEST_F(PrimaryFixture, AWrongResponseIsRefusedAndDisconnected) {
    start(/*with_secret=*/true);
    Peer p(port_);
    ASSERT_TRUE(p.connected());
    const std::string nonce = p.line().substr(std::strlen("CHALLENGE "));

    p.send("AUTH " + replication_answer(nonce, "wrong-secret-wrong-secret-wrong!"));
    EXPECT_EQ(p.line(), "ERR unauthenticated");
    p.line();
    EXPECT_TRUE(p.eof());
}

TEST_F(PrimaryFixture, AResponseForTheClientSurfaceDoesNotAuthenticateAReplica) {
    // Domain separation where it is load-bearing: the client surface has a different secret, but
    // the label is inside the MAC so even the same secret would not carry across.
    start(/*with_secret=*/true);
    Peer p(port_);
    ASSERT_TRUE(p.connected());
    const std::string nonce = p.line().substr(std::strlen("CHALLENGE "));

    p.send("AUTH " + ob::auth_response(kClusterSecret, ob::AuthSurface::Client,
                                        ob::AuthRole::Initiator, "", nonce));
    EXPECT_EQ(p.line(), "ERR unauthenticated");
}

TEST_F(PrimaryFixture, ReflectingThePrimarysOwnChallengeDoesNotAuthenticate) {
    // The attack this design had, and the reason AuthRole exists.
    //
    // Both ends of a cluster link hold the same key, and answering a challenge needs no
    // authentication - it cannot, because the peer has not proved itself yet either. So an attacker
    // with no knowledge of the secret could:
    //
    //   1. receive  CHALLENGE n
    //   2. send     CHALLENGE n   (the primary's own nonce, reflected)
    //   3. receive  AUTH H(n)     (the primary answering what it thinks is a fresh challenge)
    //   4. send     AUTH H(n)     (replaying what it was just handed)
    //
    // and step 4 verified, because both directions computed the same function. Binding both nonces
    // would not have helped: with the nonce reflected, "mine then theirs" and "theirs then mine"
    // are the same pair. The role in the MAC input is what makes step 3 produce an acceptor-side
    // value where step 4 needs an initiator-side one.
    start(/*with_secret=*/true);
    Peer p(port_);
    ASSERT_TRUE(p.connected());

    const std::string challenge = p.line();
    ASSERT_EQ(challenge.rfind("CHALLENGE ", 0), 0u) << challenge;
    const std::string nonce = challenge.substr(std::strlen("CHALLENGE "));

    // Reflect it.
    p.send("CHALLENGE " + nonce);
    const std::string handed_to_us = p.line();
    ASSERT_EQ(handed_to_us.rfind("AUTH ", 0), 0u) << handed_to_us;

    // Replay it verbatim.
    p.send(handed_to_us);
    EXPECT_EQ(p.line(), "ERR unauthenticated")
        << "the primary accepted its own answer reflected back - reflection attack";
    p.line();
    EXPECT_TRUE(p.eof());
}

TEST_F(PrimaryFixture, TheAcceptorSideResponseIsNotAnInitiatorSideResponse) {
    // The property the test above rests on, stated directly so a failure says which of the two
    // broke: the value a primary hands out for a nonce is not the value it accepts for it.
    const std::string nonce(ob::kAuthHexChars, 'd');
    EXPECT_NE(primary_answer(nonce), replication_answer(nonce));
}

TEST_F(PrimaryFixture, AStaleNonceDoesNotAuthenticateTwice) {
    start(/*with_secret=*/true);
    Peer p(port_);
    ASSERT_TRUE(p.connected());
    const std::string nonce = p.line().substr(std::strlen("CHALLENGE "));

    // Spend it on a wrong answer; the connection dies, and the same nonce must be worthless on the
    // next one - which it is, because the next connection gets its own.
    p.send("AUTH " + replication_answer(nonce, "wrong-secret-wrong-secret-wrong!"));
    EXPECT_EQ(p.line(), "ERR unauthenticated");

    Peer q(port_);
    ASSERT_TRUE(q.connected());
    const std::string second = q.line().substr(std::strlen("CHALLENGE "));
    EXPECT_NE(second, nonce);
    q.send("AUTH " + replication_answer(nonce));
    EXPECT_EQ(q.line(), "ERR unauthenticated");
}

TEST_F(PrimaryFixture, WithoutASecretTheWireIsUnchanged) {
    // Not one byte differs when authentication is off: no challenge, and REPLICATE is served.
    start(/*with_secret=*/false);
    Peer p(port_);
    ASSERT_TRUE(p.connected());
    p.send("REPLICATE 0 0 0");
    // A fresh primary has nothing to send, so the read times out rather than returning a refusal.
    // The assertion is the absence of one.
    const std::string reply = p.line();
    EXPECT_EQ(reply.find("ERR unauthenticated"), std::string::npos) << reply;
    EXPECT_EQ(reply.rfind("CHALLENGE ", 0), std::string::npos) << reply;
}

// ── Replication end to end, both directions of the gate ──────────────────────

namespace {

/// Apply one level to an engine, so a test can ask whether it arrived somewhere else.
void write_one(ob::Engine& engine, uint64_t seq) {
    ob::DeltaUpdate delta{};
    std::memset(delta.symbol, 0, sizeof(delta.symbol));
    std::memset(delta.exchange, 0, sizeof(delta.exchange));
    std::strncpy(delta.symbol, "BTCUSD", sizeof(delta.symbol) - 1);
    std::strncpy(delta.exchange, "BINANCE", sizeof(delta.exchange) - 1);
    delta.sequence_number = seq;
    delta.timestamp_ns    = 1'000'000'000ULL + seq;
    delta.side            = ob::SIDE_BID;
    delta.n_levels        = 1;

    ob::Level lvl{};
    lvl.price = 50000;
    lvl.qty   = 100;
    lvl.cnt   = 1;
    lvl._pad  = 0;
    ASSERT_EQ(engine.apply_delta(delta, &lvl), ob::OB_OK);
}

} // namespace

TEST(ClusterAuthReplication, TwoNodesWithTheSameSecretReplicate) {
    // The half that makes the refusals a proof. A gate that refuses everything demonstrates
    // nothing about whether the mechanism works.
    const uint16_t port = free_port();
    const std::string pdir = temp_dir("auth_repl_primary_");
    const std::string rdir = temp_dir("auth_repl_replica_");

    ob::ReplicationConfig primary_cfg{};
    primary_cfg.port = port;
    primary_cfg.cluster_secret = cluster_store();

    ob::Engine primary(pdir, 100'000'000ULL, ob::FsyncPolicy::NONE, primary_cfg, {});
    primary.open();
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    ob::ReplicationClientConfig replica_cfg{};
    replica_cfg.primary_host   = "127.0.0.1";
    replica_cfg.primary_port   = port;
    replica_cfg.state_file     = rdir + "/repl_state.txt";
    replica_cfg.cluster_secret = cluster_store();

    ob::Engine replica(rdir, 100'000'000ULL, ob::FsyncPolicy::NONE, {}, replica_cfg);
    replica.open();
    std::this_thread::sleep_for(std::chrono::milliseconds(700));

    write_one(primary, 1);
    std::this_thread::sleep_for(std::chrono::seconds(2));

    auto es = replica.stats();
    EXPECT_TRUE(es.is_replica);
    EXPECT_GT(es.repl_records_replayed, 0u)
        << "an authenticated replica replayed nothing, so the exchange did not complete";

    replica.close();
    primary.close();
    fs::remove_all(pdir);
    fs::remove_all(rdir);
}

TEST(ClusterAuthReplication, AReplicaWithoutTheSecretReplicatesNothing) {
    const uint16_t port = free_port();
    const std::string pdir = temp_dir("auth_repl_primary2_");
    const std::string rdir = temp_dir("auth_repl_replica2_");

    ob::ReplicationConfig primary_cfg{};
    primary_cfg.port = port;
    primary_cfg.cluster_secret = cluster_store();

    ob::Engine primary(pdir, 100'000'000ULL, ob::FsyncPolicy::NONE, primary_cfg, {});
    primary.open();
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // No cluster secret on this side: the replica sends REPLICATE without authenticating and the
    // primary refuses it. The replica then reconnects on a backoff, which is the correct behaviour
    // for a configuration mistake - it recovers the moment the secret is put in place.
    ob::ReplicationClientConfig replica_cfg{};
    replica_cfg.primary_host = "127.0.0.1";
    replica_cfg.primary_port = port;
    replica_cfg.state_file   = rdir + "/repl_state.txt";

    ob::Engine replica(rdir, 100'000'000ULL, ob::FsyncPolicy::NONE, {}, replica_cfg);
    replica.open();
    std::this_thread::sleep_for(std::chrono::milliseconds(700));

    write_one(primary, 1);
    std::this_thread::sleep_for(std::chrono::seconds(2));

    EXPECT_EQ(replica.stats().repl_records_replayed, 0u)
        << "an unauthenticated replica received records";

    replica.close();
    primary.close();
    fs::remove_all(pdir);
    fs::remove_all(rdir);
}

TEST(ClusterAuthReplication, AReplicaWithTheWrongSecretReplicatesNothing) {
    const uint16_t port = free_port();
    const std::string pdir = temp_dir("auth_repl_primary3_");
    const std::string rdir = temp_dir("auth_repl_replica3_");

    ob::ReplicationConfig primary_cfg{};
    primary_cfg.port = port;
    primary_cfg.cluster_secret = cluster_store();

    ob::Engine primary(pdir, 100'000'000ULL, ob::FsyncPolicy::NONE, primary_cfg, {});
    primary.open();
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    ob::ReplicationClientConfig replica_cfg{};
    replica_cfg.primary_host   = "127.0.0.1";
    replica_cfg.primary_port   = port;
    replica_cfg.state_file     = rdir + "/repl_state.txt";
    replica_cfg.cluster_secret = cluster_store("a-different-cluster-secret-entirely!!");

    ob::Engine replica(rdir, 100'000'000ULL, ob::FsyncPolicy::NONE, {}, replica_cfg);
    replica.open();
    std::this_thread::sleep_for(std::chrono::milliseconds(700));

    write_one(primary, 1);
    std::this_thread::sleep_for(std::chrono::seconds(2));

    EXPECT_EQ(replica.stats().repl_records_replayed, 0u)
        << "a replica with the wrong secret received records";

    replica.close();
    primary.close();
    fs::remove_all(pdir);
    fs::remove_all(rdir);
}

// ── Multi-master frames ───────────────────────────────────────────────────────

TEST(ClusterAuthFrames, TheTwoFrameTypesAreDistinctAndOutsideTheWalRange) {
    // The numbers sit above every WAL record type on purpose, so adding a WAL record type can
    // never collide with a wire-only message.
    EXPECT_NE(ob::MM_MSG_AUTH_CHALLENGE, ob::MM_MSG_AUTH_RESPONSE);
    for (uint8_t t : {ob::MM_MSG_AUTH_CHALLENGE, ob::MM_MSG_AUTH_RESPONSE}) {
        EXPECT_GT(t, 100) << "an authentication frame type must not collide with a WAL record type";
        EXPECT_NE(t, ob::MM_MSG_SNAPSHOT_REQUEST);
        EXPECT_NE(t, ob::MM_MSG_SNAPSHOT_BEGIN);
        EXPECT_NE(t, ob::MM_MSG_SNAPSHOT_CHUNK);
        EXPECT_NE(t, ob::MM_MSG_SNAPSHOT_END);
        EXPECT_NE(t, ob::MM_MSG_SNAPSHOT_ABORT);
    }
}

TEST(ClusterAuthFrames, AnAuthenticationFrameCanNeverBeMistakenForAHandshake) {
    // This is what lets the two coexist without a protocol version bump, and it is worth an
    // assertion rather than a comment: a handshake frame is exactly MM_HANDSHAKE_SIZE bytes, and an
    // authentication frame carries a WALRecordV2 header, so the smallest possible one is larger.
    EXPECT_GT(ob::MM_WALRECORD_V2_SIZE, ob::MM_HANDSHAKE_SIZE)
        << "if a WALRecordV2 header ever became 17 bytes or fewer, a bare handshake and an "
           "authentication frame would be ambiguous on the wire";
    EXPECT_GT(ob::MM_WALRECORD_V2_SIZE + ob::kAuthHexChars, ob::MM_HANDSHAKE_SIZE);
}

TEST(ClusterAuthFrames, TheClusterSurfacesShareASecretAndAreStillSeparated) {
    // Replication and multi-master use one secret, so the label in the MAC input is the only thing
    // stopping a response captured on one link from working on the other.
    const std::string nonce(ob::kAuthHexChars, 'c');
    EXPECT_NE(ob::auth_response(kClusterSecret, ob::AuthSurface::Replication,
                                ob::AuthRole::Initiator, "", nonce),
              ob::auth_response(kClusterSecret, ob::AuthSurface::MultiMaster,
                                ob::AuthRole::Initiator, "", nonce));
}
