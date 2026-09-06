// Connections this node accepted, before their handshake says who is behind them — roadmap #96.
//
// `peers_` is keyed by node id, and an accepted connection used to be inserted into it under
// `static_cast<uint16_t>(client_fd)`. A descriptor number is a node id in every sense that map can
// see, so an inbound connection landing on descriptor N replaced the live record of peer N: its
// send buffer, its backoff and its advertised address went with the assignment, and its descriptor
// stayed in the epoll set with nothing behind it. Nothing logged anything, on either side.
//
// Reaching it needs a node id equal to a descriptor number, which is why the live mesh cannot be
// the instrument: the integration fixture numbers its nodes 1..3 and is safe by accident, and
// `--mm-node-id` accepts any `uint16_t`. A test can do what a cluster cannot — install a peer
// record for *every* descriptor number the accepted socket might get, so the coincidence is
// certain rather than lucky. Measured before the fix: the connection landed on descriptor 8 and the
// record of peer 8 was gone.
//
// The other tests are about the container the connection lives in now: that its handshake moves it
// into the peer table keeping what only the peer record knew, that a handshake cannot claim a node
// id that could never be adopted, that two links to one node resolve to one link and the same one
// at both ends, and that a connection which closes before its handshake leaves nothing behind (#95
// held structurally rather than by a `node_id == 0` test).

#include "orderbook/engine.hpp"
#include "orderbook/multi_master.hpp"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {

std::atomic<uint64_t> g_dir_counter{0};
std::atomic<uint16_t> g_port{55400};

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

/// A socket connected to a node's mesh port, and whatever the node sent back.
///
/// The node speaks first — with no cluster secret it queues a handshake the moment it has accepted
/// the connection — so the first byte arriving here proves the accept path ran to the end. That is
/// the readiness signal these tests wait on, rather than a sleep or a seam, because it is the same
/// signal in a build with the defect and a build without it.
struct MeshClient {
    int fd{-1};

    explicit MeshClient(uint16_t port) {
        fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) throw std::runtime_error("socket");
        int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port   = htons(port);
        ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
        if (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
            ::close(fd);
            fd = -1;
            throw std::runtime_error("connect to mesh port failed");
        }
        struct timeval tv{};
        tv.tv_sec = 2;
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }
    ~MeshClient() { close(); }
    MeshClient(const MeshClient&) = delete;
    MeshClient& operator=(const MeshClient&) = delete;

    void close() {
        if (fd >= 0) ::close(fd);
        fd = -1;
    }

    /// Block until the node sends something, or the receive timeout expires.
    bool wait_for_bytes() {
        uint8_t buf[512];
        const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        return n > 0;
    }

    /// Whether the node has closed this connection. Returns false while it is still open.
    bool closed_by_peer() {
        uint8_t buf[512];
        for (;;) {
            const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
            if (n == 0) return true;       // orderly close
            if (n < 0) return errno == ECONNRESET;
            // Bytes, not a close: keep reading until the node says one or the other.
        }
    }

    /// A framed handshake claiming to be `node_id`: 4-byte LE length, then the 17-byte message.
    void send_handshake(uint16_t node_id, uint16_t protocol = ob::MM_PROTOCOL_VERSION) {
        ob::HandshakeMessage msg{};
        msg.node_id                = node_id;
        msg.protocol_version       = protocol;
        msg.compression_preference = 0;
        msg.wal_file_index         = 0;
        msg.wal_byte_offset        = 0;

        uint8_t frame[ob::MM_FRAME_HEADER_SIZE + ob::MM_HANDSHAKE_SIZE];
        const uint32_t len = ob::MM_HANDSHAKE_SIZE;
        std::memcpy(frame, &len, sizeof(len));
        msg.serialize(frame + ob::MM_FRAME_HEADER_SIZE);
        ssize_t sent = ::send(fd, frame, sizeof(frame), MSG_NOSIGNAL);
        (void)sent;
    }
};

/// A peer record as the registry path leaves one: identified, addressed, not currently connected.
///
/// The next reconnect is put an hour out, so the reconnect thread never dials these records while
/// a test runs. Not tidiness: these tests are about the container a connection lives in, and the
/// dial is a blocking `::connect()` **under the mesh mutex**, so a made-up address that black-holes
/// SYNs stops the node answering anything for the full TCP timeout — measured at 132 s, which made
/// this file flaky 3 runs in 12 before the reconnect time was pinned. That is roadmap #97, filed
/// rather than worked around silently: a workaround in a harness is a bug report nobody filed.
ob::PeerConnection dialled_peer_record(uint16_t node_id, const std::string& address) {
    ob::PeerConnection p{};
    p.node_id        = node_id;
    p.address        = address;
    p.fd             = -1;
    p.conn_id        = 5000 + node_id;  // the marker: unique per record, copied out by peer_states()
    p.connected      = false;
    p.handshake_done = false;
    p.next_reconnect_time = std::chrono::steady_clock::now() + std::chrono::hours(1);
    return p;
}

/// A peer record with a live socket, as a completed outbound dial leaves one.
///
/// The descriptor is one end of a `socketpair`, so the test holds the other end and can tell
/// whether the manager tore this connection down or merely forgot about it — which is the whole
/// difference between resolving two links to one node and orphaning one of them.
ob::PeerConnection connected_peer_record(uint16_t node_id, int fd) {
    ob::PeerConnection p{};
    p.node_id        = node_id;
    p.fd             = fd;
    p.conn_id        = 5000 + node_id;
    p.connected      = true;
    p.handshake_done = true;
    p.we_accepted    = false;  // we dialled this one
    p.next_reconnect_time = std::chrono::steady_clock::now() + std::chrono::hours(1);
    return p;
}

/// Poll a predicate for up to `budget`, so a passing test does not pay a fixed sleep.
template <typename F>
bool eventually(F&& pred, std::chrono::milliseconds budget = std::chrono::milliseconds(3000)) {
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return pred();
}

const ob::PeerConnection* find_peer(const std::vector<ob::PeerConnection>& peers, uint16_t node_id) {
    for (const auto& p : peers) {
        if (p.node_id == node_id) return &p;
    }
    return nullptr;
}

}  // namespace

// ── The defect ──────────────────────────────────────────────────────────────────

TEST(PendingPeers, AnAcceptedConnectionDoesNotDisplaceThePeerWhoseNodeIdMatchesItsDescriptor) {
    const uint16_t port = g_port.fetch_add(1, std::memory_order_relaxed);
    TempDir tmp("mm_pending_collide_");
    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::NONE, {}, {}, {}, {},
                      mm_config(1, port));
    engine.open();
    auto* mm = engine.multi_master_manager();
    ASSERT_NE(mm, nullptr);

    // Every node id a descriptor might be. No address on purpose: a peer that dialled us and is
    // not in the registry has none, so the reconnect loop leaves these records alone instead of
    // dialling and replacing them, and the collision is the only thing that can touch one.
    constexpr uint16_t kFirst = 3;
    constexpr uint16_t kLast  = 300;
    for (uint16_t nid = kFirst; nid <= kLast; ++nid) {
        if (nid == mm->config().node_id) continue;
        mm->install_peer_for_test(dialled_peer_record(nid, ""));
    }
    const size_t installed = mm->peer_states().size();
    ASSERT_GT(installed, 250u);

    MeshClient client(port);
    ASSERT_TRUE(client.wait_for_bytes()) << "the node did not answer an accepted mesh connection";

    // The connection is accounted for, and it is not a peer: nothing has named a node yet.
    EXPECT_TRUE(eventually([&] { return mm->pending_connection_count() == 1; }));

    const auto peers = mm->peer_states();
    std::vector<uint16_t> lost;
    for (uint16_t nid = kFirst; nid <= kLast; ++nid) {
        if (nid == mm->config().node_id) continue;
        const ob::PeerConnection* p = find_peer(peers, nid);
        if (p == nullptr || p->conn_id != 5000u + nid) lost.push_back(nid);
    }
    std::ostringstream lost_ids;
    for (uint16_t nid : lost) lost_ids << ' ' << nid;
    EXPECT_TRUE(lost.empty())
        << "an accepted connection destroyed the record of peer(s)" << lost_ids.str()
        << " — its descriptor number was used as a node id";
    EXPECT_EQ(peers.size(), installed) << "the peer table gained or lost a row";

    client.close();
    engine.close();
}

// ── What the container does once the handshake names a node ────────────────────

TEST(PendingPeers, TheHandshakeMovesTheConnectionIntoThePeerRecordAndKeepsItsAddress) {
    const uint16_t port = g_port.fetch_add(1, std::memory_order_relaxed);
    TempDir tmp("mm_pending_adopt_");
    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::NONE, {}, {}, {}, {},
                      mm_config(1, port));
    engine.open();
    auto* mm = engine.multi_master_manager();
    ASSERT_NE(mm, nullptr);

    // The address is the thing only the peer record knows: an accepted socket's source port is the
    // peer's ephemeral one, so a connection cannot supply it. With no registry to ask, replacing
    // the record wholesale is what loses it — and a peer whose address we have forgotten can only
    // be reached again if it dials us.
    mm->install_peer_for_test(dialled_peer_record(7, "10.9.9.7:7100"));

    MeshClient client(port);
    ASSERT_TRUE(client.wait_for_bytes());
    client.send_handshake(7);

    ASSERT_TRUE(eventually([&] {
        const auto peers = mm->peer_states();
        const ob::PeerConnection* p = find_peer(peers, 7);
        return p != nullptr && p->connected && p->handshake_done;
    })) << "the handshake did not put the connection into the peer table under node id 7";

    const auto peers = mm->peer_states();
    const ob::PeerConnection* p = find_peer(peers, 7);
    ASSERT_NE(p, nullptr);
    EXPECT_EQ(p->address, "10.9.9.7:7100") << "the peer's advertised address did not survive its "
                                              "own inbound connection";
    EXPECT_TRUE(p->we_accepted) << "the adopted connection is the one we accepted";
    EXPECT_NE(p->conn_id, 5000u + 7u) << "the record still carries the old connection's identity";
    EXPECT_EQ(mm->pending_connection_count(), 0u) << "an identified connection is no longer pending";
    EXPECT_EQ(peers.size(), 1u);

    client.close();
    engine.close();
}

TEST(PendingPeers, AHandshakeMayNotClaimTheUnidentifiedNodeIdOrOurOwn) {
    const uint16_t port = g_port.fetch_add(1, std::memory_order_relaxed);
    TempDir tmp("mm_pending_bad_id_");
    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::NONE, {}, {}, {}, {},
                      mm_config(4, port));
    engine.open();
    auto* mm = engine.multi_master_manager();
    ASSERT_NE(mm, nullptr);

    // Zero is the value that used to mean "not yet identified", so a peer claiming it would sit in
    // the pending container for ever, connected and never adoptable. Our own id is the other one
    // that cannot be adopted: broadcast would then send our records to a record keyed as us.
    for (uint16_t claimed : {uint16_t{0}, uint16_t{4}}) {
        MeshClient client(port);
        ASSERT_TRUE(client.wait_for_bytes());
        client.send_handshake(claimed);
        EXPECT_TRUE(client.closed_by_peer())
            << "a handshake claiming node id " << claimed << " was not refused";
        EXPECT_TRUE(eventually([&] { return mm->pending_connection_count() == 0; }))
            << "the refused connection is still counted as pending";
        EXPECT_TRUE(mm->peer_states().empty()) << "a refused handshake created a peer record";
    }

    engine.close();
}

// ── Two links to one node ──────────────────────────────────────────────────────
//
// A symmetric mesh can produce two: both ends dial at the same moment and both dials succeed.
// Whichever way it is resolved, **both ends have to resolve it the same way**, or each closes the
// link the other kept and the pair is left with none. The rule is therefore a function of the two
// node ids and nothing else — the surviving link is the one the lower-numbered node dialled — and
// these two tests are the same situation seen from the two ends, which is the only way to check
// that the rule agrees with itself.
//
// Not observed on a live mesh: peer discovery goes through an etcd watch, whose latency is orders
// of magnitude above a loopback connect, so the second dialler always finds itself already
// connected. Measured over three multi-master integration modules and a mesh started with all
// three nodes launched at once — zero duplicate links. That is a fact about this deployment rather
// than about the protocol, which is why the rule exists and why it is tested here.

TEST(PendingPeers, TheLowerNumberedNodeKeepsTheLinkItDialled) {
    const uint16_t port = g_port.fetch_add(1, std::memory_order_relaxed);
    TempDir tmp("mm_pending_dup_low_");
    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::NONE, {}, {}, {}, {},
                      mm_config(1, port));
    engine.open();
    auto* mm = engine.multi_master_manager();
    ASSERT_NE(mm, nullptr);

    int sv[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    // A deadline on our end, because the assertion below is about a socket being *closed*: without
    // it, the mutation this test exists to catch makes the test hang rather than fail, and a
    // hanging test detects a defect and reports nothing (roadmap #88 paid for that lesson).
    struct timeval sv_tv{};
    sv_tv.tv_sec = 2;
    ::setsockopt(sv[0], SOL_SOCKET, SO_RCVTIMEO, &sv_tv, sizeof(sv_tv));
    mm->install_peer_for_test(connected_peer_record(5, sv[1]));

    MeshClient client(port);
    ASSERT_TRUE(client.wait_for_bytes());
    client.send_handshake(5);

    // This node is 1, the peer is 5, so our own dial wins and the inbound link goes.
    EXPECT_TRUE(client.closed_by_peer()) << "the second link to peer 5 was not closed";
    EXPECT_TRUE(eventually([&] { return mm->pending_connection_count() == 0; }));

    const auto peers = mm->peer_states();
    const ob::PeerConnection* p = find_peer(peers, 5);
    ASSERT_NE(p, nullptr);
    EXPECT_TRUE(p->connected);
    EXPECT_EQ(p->conn_id, 5000u + 5u) << "our own connection to peer 5 was replaced";
    EXPECT_FALSE(p->we_accepted);

    // And ours was left alone rather than closed: the far end of the socketpair is still open.
    uint8_t byte = 7;
    EXPECT_EQ(::send(sv[0], &byte, 1, MSG_NOSIGNAL), 1)
        << "the surviving link's socket was closed underneath it";

    ::close(sv[0]);
    engine.close();
}

TEST(PendingPeers, TheHigherNumberedNodeGivesUpTheLinkItDialled) {
    const uint16_t port = g_port.fetch_add(1, std::memory_order_relaxed);
    TempDir tmp("mm_pending_dup_high_");
    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::NONE, {}, {}, {}, {},
                      mm_config(9, port));
    engine.open();
    auto* mm = engine.multi_master_manager();
    ASSERT_NE(mm, nullptr);

    int sv[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    // A deadline on our end, because the assertion below is about a socket being *closed*: without
    // it, the mutation this test exists to catch makes the test hang rather than fail, and a
    // hanging test detects a defect and reports nothing (roadmap #88 paid for that lesson).
    struct timeval sv_tv{};
    sv_tv.tv_sec = 2;
    ::setsockopt(sv[0], SOL_SOCKET, SO_RCVTIMEO, &sv_tv, sizeof(sv_tv));
    mm->install_peer_for_test(connected_peer_record(5, sv[1]));

    MeshClient client(port);
    ASSERT_TRUE(client.wait_for_bytes());
    client.send_handshake(5);

    // This node is 9, the peer is 5, so the link peer 5 dialled wins and ours is given up — torn
    // down, which is the point: the previous code left its descriptor armed in the epoll set with
    // no record behind it, and the peer at the far end saw a truncation.
    ASSERT_TRUE(eventually([&] {
        const auto peers = mm->peer_states();
        const ob::PeerConnection* p = find_peer(peers, 5);
        return p != nullptr && p->connected && p->we_accepted;
    })) << "the link peer 5 opened did not replace ours";

    const auto peers = mm->peer_states();
    const ob::PeerConnection* p = find_peer(peers, 5);
    ASSERT_NE(p, nullptr);
    EXPECT_NE(p->conn_id, 5000u + 5u);
    EXPECT_EQ(mm->pending_connection_count(), 0u);
    EXPECT_EQ(peers.size(), 1u) << "one node, one link";

    uint8_t buf[8];
    EXPECT_EQ(::recv(sv[0], buf, sizeof(buf), 0), 0)
        << "our own link was abandoned rather than closed, so its peer sees a truncation";

    ::close(sv[0]);
    client.close();
    engine.close();
}

TEST(PendingPeers, AConnectionThatClosesBeforeItsHandshakeLeavesNothingBehind) {
    const uint16_t port = g_port.fetch_add(1, std::memory_order_relaxed);
    TempDir tmp("mm_pending_gone_");
    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::NONE, {}, {}, {}, {},
                      mm_config(1, port));
    engine.open();
    auto* mm = engine.multi_master_manager();
    ASSERT_NE(mm, nullptr);

    // #95's guarantee, held by the container rather than by a `node_id == 0` test: an accepted
    // connection that never named a node has nothing to dial and nothing to become, so it is
    // dropped rather than retried at loop frequency for the life of the process.
    {
        MeshClient client(port);
        ASSERT_TRUE(client.wait_for_bytes());
        EXPECT_TRUE(eventually([&] { return mm->pending_connection_count() == 1; }));
    }

    EXPECT_TRUE(eventually([&] { return mm->pending_connection_count() == 0; }))
        << "the record of a connection that closed before its handshake was kept";
    EXPECT_TRUE(mm->peer_states().empty()) << "a connection that never identified itself became a peer";

    // And MM_PEERS says the same thing, without needing to skip a row (#84).
    const std::string view = mm->handle_mm_peers_command();
    size_t rows = 0;
    std::istringstream in(view);
    for (std::string line; std::getline(in, line);) {
        if (!line.empty()) ++rows;
    }
    EXPECT_EQ(rows, 1u) << "MM_PEERS should be a header and nothing else:\n" << view;

    engine.close();
}

// ── The shape, so the key cannot come back ──────────────────────────────────────

TEST(PendingPeersStatic, NoPeerRecordIsKeyedByAnythingButANodeId) {
    // The comment above the old temporary key described a different design — "use a high node_id
    // range (fd + 10000) as temp key" — which would have been safe. The code did not do it, and a
    // reserved range is not the fix either: it is still a node id, still in the same space, and
    // still one arithmetic slip from a live record. This refuses the whole class instead.
    // Every source file, not the one file I happened to think of: `install_peer_for_test()` lives
    // in mm_snapshot.cpp and subscripts the same map. A list written by hand is not evidence about
    // the code.
    std::string src;
    size_t files = 0;
    for (const auto& entry : fs::directory_iterator(std::string(OB_SOURCE_DIR) + "/src")) {
        if (entry.path().extension() != ".cpp") continue;
        std::ifstream in(entry.path());
        ASSERT_TRUE(in) << "cannot read " << entry.path();
        std::stringstream ss;
        ss << in.rdbuf();
        src += ss.str();
        ++files;
    }
    ASSERT_GT(files, 20u) << "found almost no sources — is OB_SOURCE_DIR right?";

    size_t subscripts = 0;
    for (size_t at = src.find("peers_["); at != std::string::npos; at = src.find("peers_[", at + 1)) {
        // `pending_[` and `peers_[` are different names; make sure this is not the tail of one.
        if (at > 0 && (std::isalnum(static_cast<unsigned char>(src[at - 1])) || src[at - 1] == '_')) {
            continue;
        }
        const size_t open  = at + std::strlen("peers_[");
        const size_t close = src.find(']', open);
        ASSERT_NE(close, std::string::npos) << "unterminated subscript at offset " << at;
        const std::string index = src.substr(open, close - open);
        ++subscripts;
        EXPECT_NE(index.find("node_id"), std::string::npos)
            << "peers_[" << index << "] is indexed by something that is not a node id; a "
               "descriptor number used as a key silently replaces the record of the peer whose "
               "node id happens to equal it (#96)";
    }
    EXPECT_GT(subscripts, 0u) << "found no peers_[...] subscripts — has the container been renamed?";

    // The other container's key gets the same treatment, and the exact reach of this check is
    // worth stating because it is small. It catches the *direct* form — `pending_[fd]`, which is
    // what someone would write — and it does **not** catch a descriptor assigned to the key
    // variable a line earlier; that mutation survives this and every behavioural test in this
    // file, which is recorded here rather than papered over.
    //
    // Nor is there a behavioural test to be had cheaply: keying `pending_` by descriptor is
    // *weaker* rather than broken, because two live connections cannot share a descriptor. The
    // window it opens is narrow — a refused handshake closes the descriptor without erasing the
    // record, the prune runs up to 100 ms later, and an accept in between can be handed the same
    // number — and nothing reproduces it on demand. What defends the choice is the container
    // split itself: whatever the key, `pending_` is not the node-id space.
    for (size_t at = src.find("pending_["); at != std::string::npos;
         at = src.find("pending_[", at + 1)) {
        const size_t open  = at + std::strlen("pending_[");
        const size_t close = src.find(']', open);
        ASSERT_NE(close, std::string::npos);
        const std::string index = src.substr(open, close - open);
        EXPECT_EQ(index.find("fd"), std::string::npos)
            << "pending_[" << index << "] is keyed off a descriptor number; conn_id is minted once "
               "and never reused, which is the property a key needs";
    }
}
