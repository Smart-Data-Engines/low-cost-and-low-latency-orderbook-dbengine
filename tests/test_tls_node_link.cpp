// TLS on the node links, and the two halves of "who is this peer" (#30 part three, series D).
//
// Real handshakes over a socket pair rather than mocks, because every claim here is about what
// OpenSSL accepts. Three of them are refusals, and the refusals are the feature:
//
//   * a peer that presents no certificate at all cannot complete an accepted handshake;
//   * a certificate this CA signed, whose identity is not in `--tls-peer-names`, cannot either;
//   * a certificate this CA signed for a *different address* cannot satisfy the dialling end.
//
// The last two share a shape worth naming: the chain is **good** in both. `SSL_VERIFY_PEER` checks
// only that a trusted CA signed the certificate, so with a private CA signing a whole cluster - how
// anyone deploys this - every node's certificate would otherwise be good for every other node, the
// relay in SECURITY.md would still work between two holders of legitimate certificates, and every
// verification would report success (pitfall 124).

#include "orderbook/tls.hpp"

#include <gtest/gtest.h>

#include <openssl/ssl.h>

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>

namespace fs = std::filesystem;

namespace {

struct Ca {
    std::string dir;
    std::string cert;   ///< the trust anchor both ends are given
    std::string key;
};

struct NodeCert {
    std::string cert;
    std::string key;
};

/// A private CA, the way `docs/operations.md` tells an operator to make one for a cluster.
///
/// A hierarchy rather than self-signed certificates, unlike `test_tls_client.cpp`: a node link is
/// mutual, so both ends need to verify against the *same* anchor while presenting *different*
/// certificates. Self-signing cannot express that, and it is the case where chain-only verification
/// stops being sufficient.
Ca make_ca() {
    static std::atomic<int> counter{0};
    const auto dir = fs::temp_directory_path() /
                     ("ob_tlsn_" + std::to_string(::getpid()) + "_" +
                      std::to_string(counter.fetch_add(1)));
    fs::create_directories(dir);
    Ca ca;
    ca.dir  = dir.string();
    ca.cert = (dir / "ca.pem").string();
    ca.key  = (dir / "ca-key.pem").string();
    const std::string cmd =
        "openssl req -x509 -newkey rsa:2048 -nodes -keyout '" + ca.key + "' -out '" + ca.cert +
        "' -days 1 -subj '/CN=ob-test-ca' >/dev/null 2>&1";
    if (std::system(cmd.c_str()) != 0) return {};
    return ca;
}

/// A node certificate signed by `ca`, with `cn` as its common name and `san` as its SAN extension.
///
/// `cn` and `san` are separate parameters because the two are used for different things and getting
/// that backwards is the mistake this file is about: verification matches the SAN, the identity in a
/// log line is the CN.
NodeCert sign_node(const Ca& ca, const std::string& cn, const std::string& san) {
    static std::atomic<int> counter{0};
    const std::string stem = ca.dir + "/node_" + std::to_string(counter.fetch_add(1));
    NodeCert out{stem + ".pem", stem + "-key.pem"};
    const std::string csr = stem + ".csr";
    const std::string ext = stem + ".ext";
    {
        std::ofstream f(ext);
        f << "subjectAltName=" << san << "\n";
    }
    const std::string mk =
        "openssl req -newkey rsa:2048 -nodes -keyout '" + out.key + "' -out '" + csr +
        "' -subj '/CN=" + cn + "' >/dev/null 2>&1 && "
        "openssl x509 -req -in '" + csr + "' -CA '" + ca.cert + "' -CAkey '" + ca.key +
        "' -CAcreateserial -out '" + out.cert + "' -days 1 -extfile '" + ext +
        "' >/dev/null 2>&1";
    if (std::system(mk.c_str()) != 0) return {};
    ::chmod(out.key.c_str(), 0600);
    return out;
}

/// What a mutual handshake produced on both ends.
struct Shaken {
    bool        server_ok{false};
    bool        client_ok{false};
    std::string server_identity;
    std::string client_identity;
    std::string client_why;
};

/// Drive one handshake to completion on both sides of a socket pair.
///
/// `AF_UNIX` rather than loopback TCP: TLS needs a byte stream and nothing here needs a port, so
/// this avoids the port allocation that makes `ctest -j1` mandatory elsewhere. Both descriptors get
/// a receive timeout, so a handshake that cannot finish fails in three seconds instead of wedging
/// the suite - `tls_blocking_handshake()` reports that timeout as an error rather than retrying it,
/// which is the whole reason the blocking helpers exist (pitfall 129).
Shaken shake(const ob::TlsContext& server_ctx, const ob::TlsContext& client_ctx,
             const std::string& expect_host) {
    Shaken out;
    int fds[2] = {-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) return out;

    struct timeval tv{};
    tv.tv_sec = 3;
    ::setsockopt(fds[0], SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ::setsockopt(fds[1], SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    auto server = server_ctx.open_channel(fds[0], /*server_side=*/true, "test-client");
    auto client = client_ctx.open_channel(fds[1], /*server_side=*/false, "test-server");
    if (!expect_host.empty()) {
        EXPECT_TRUE(ob::tls_expect_host(client->raw(), expect_host));
    }

    std::string server_why;
    std::thread server_thread([&] {
        out.server_ok = server->blocking_handshake(&server_why);
        if (out.server_ok) out.server_identity = server->identity();
    });
    out.client_ok = client->blocking_handshake(&out.client_why);
    if (out.client_ok) out.client_identity = client->identity();
    server_thread.join();

    ::close(fds[0]);
    ::close(fds[1]);
    return out;
}

std::string read_source(const std::string& relative) {
    std::ifstream in(std::string(OB_SOURCE_DIR) + "/" + relative);
    if (!in) return {};
    std::stringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

} // namespace

// ── The link works, and both ends know who the other is ──────────────────────

TEST(TlsNodeLink, BothEndsPresentACertificateAndReadTheOthersIdentity) {
    const Ca ca = make_ca();
    ASSERT_FALSE(ca.cert.empty()) << "openssl is required for these tests";
    const NodeCert primary = sign_node(ca, "node-1", "IP:127.0.0.1");
    const NodeCert replica = sign_node(ca, "node-2", "IP:127.0.0.1");
    ASSERT_FALSE(primary.cert.empty());
    ASSERT_FALSE(replica.cert.empty());

    const auto srv = ob::TlsContext::node_server(primary.cert, primary.key, ca.cert);
    const auto cli = ob::TlsContext::node_client(replica.cert, replica.key, ca.cert);

    const Shaken s = shake(srv, cli, "127.0.0.1");
    EXPECT_TRUE(s.server_ok);
    EXPECT_TRUE(s.client_ok) << s.client_why;
    // The identity requirement 8.4 of part one asked for, on a link that had none: before mTLS a
    // node's identity was its `node_id`, which arrives in a handshake that authentication precedes.
    EXPECT_EQ(s.server_identity, "node-2") << "the accepting end should read the replica's CN";
    EXPECT_EQ(s.client_identity, "node-1") << "the dialling end should read the primary's CN";
    fs::remove_all(ca.dir);
}

// ── Refusal 1: no certificate at all ─────────────────────────────────────────

TEST(TlsNodeLink, TheAcceptingEndRefusesAPeerThatPresentsNoCertificate) {
    // `SSL_VERIFY_PEER` on its own completes this handshake: a peer that sends no certificate is
    // simply not verified, and the link ends up encrypted and anonymous - the configuration that
    // looks like protection and is not. `SSL_VERIFY_FAIL_IF_NO_PEER_CERT` is the half that makes it
    // mutual, and this test is what fails if it is removed.
    const Ca ca = make_ca();
    ASSERT_FALSE(ca.cert.empty());
    const NodeCert primary = sign_node(ca, "node-1", "IP:127.0.0.1");
    ASSERT_FALSE(primary.cert.empty());

    const auto srv = ob::TlsContext::node_server(primary.cert, primary.key, ca.cert);
    // A plain client context: it verifies the server and has nothing of its own to present.
    const auto cli = ob::TlsContext::client(ca.cert, /*verify=*/true);

    const Shaken s = shake(srv, cli, "127.0.0.1");
    EXPECT_FALSE(s.server_ok) << "an anonymous peer completed an accepted node-link handshake";
    fs::remove_all(ca.dir);
}

// ── Refusal 2: good chain, identity outside the allowlist ────────────────────

TEST(TlsNodeLink, TheAcceptingEndRefusesAnIdentityOutsideThePeerNameAllowlist) {
    // The mutation-critical test of this series. The certificate is signed by the trust anchor the
    // accepting end was given, so the chain check passes; what fails is the name. Without the
    // allowlist a corporate CA that signs every host in the organisation means every host in the
    // organisation may become a replica, and nothing about that reads as wrong.
    //
    // Deleting the allowlist check makes exactly this test fail and leaves
    // `BothEndsPresentACertificateAndReadTheOthersIdentity` passing, which is the discrimination
    // worth having.
    const Ca ca = make_ca();
    ASSERT_FALSE(ca.cert.empty());
    const NodeCert primary  = sign_node(ca, "node-1",  "IP:127.0.0.1");
    const NodeCert stranger = sign_node(ca, "laptop-7", "DNS:laptop-7.corp");
    ASSERT_FALSE(primary.cert.empty());
    ASSERT_FALSE(stranger.cert.empty());

    const auto strict = ob::TlsContext::node_server(primary.cert, primary.key, ca.cert,
                                                    {"node-1", "node-2", "node-3"});
    const auto cli = ob::TlsContext::node_client(stranger.cert, stranger.key, ca.cert);
    const Shaken refused = shake(strict, cli, "127.0.0.1");
    EXPECT_FALSE(refused.server_ok)
        << "a certificate this CA signed, for an identity outside --tls-peer-names, was accepted";

    // And the other half, which is what makes the first half a statement about the *name*: the same
    // certificate against the same CA with no allowlist is accepted. Without this the test would
    // also pass if node contexts refused everything.
    const auto permissive = ob::TlsContext::node_server(primary.cert, primary.key, ca.cert);
    const auto cli2 = ob::TlsContext::node_client(stranger.cert, stranger.key, ca.cert);
    const Shaken accepted = shake(permissive, cli2, "127.0.0.1");
    EXPECT_TRUE(accepted.server_ok)
        << "with no --tls-peer-names, any identity this CA signed is a cluster member";
    EXPECT_EQ(accepted.server_identity, "laptop-7");
    fs::remove_all(ca.dir);
}

TEST(TlsNodeLink, AnAllowlistMatchesAnAddressAsWellAsAName) {
    // Two branches, the same two as `tls_expect_host`: an entry that parses as an address is matched
    // against `iPAddress` and everything else against `dNSName`. An allowlist that only did names
    // would silently reject every certificate in a cluster addressed by IP - which is every cluster
    // this engine's replication client can dial, since it resolves nothing.
    const Ca ca = make_ca();
    ASSERT_FALSE(ca.cert.empty());
    const NodeCert primary = sign_node(ca, "node-1", "IP:127.0.0.1");
    const NodeCert peer    = sign_node(ca, "node-2", "IP:127.0.0.2");
    ASSERT_FALSE(peer.cert.empty());

    const auto by_address = ob::TlsContext::node_server(primary.cert, primary.key, ca.cert,
                                                        {"127.0.0.2"});
    const auto cli = ob::TlsContext::node_client(peer.cert, peer.key, ca.cert);
    EXPECT_TRUE(shake(by_address, cli, "127.0.0.1").server_ok);

    const auto wrong_address = ob::TlsContext::node_server(primary.cert, primary.key, ca.cert,
                                                            {"127.0.0.9"});
    const auto cli2 = ob::TlsContext::node_client(peer.cert, peer.key, ca.cert);
    EXPECT_FALSE(shake(wrong_address, cli2, "127.0.0.1").server_ok);
    fs::remove_all(ca.dir);
}

// ── Refusal 3: the dialling end checks the name it dialled ───────────────────

TEST(TlsNodeLink, TheDiallingEndRefusesACertificateIssuedForAnotherAddress) {
    // Same shape as `TlsClient.RefusesACertificateIssuedForAnotherAddress` on the client port, and
    // it is here as well because the node links are a different call site: the replication client
    // pins `primary_host` and the mesh pins the address it read from etcd. A chain check alone
    // accepts node B's certificate for node A, which is exactly the relay TLS is here to stop.
    const Ca ca = make_ca();
    ASSERT_FALSE(ca.cert.empty());
    const NodeCert elsewhere = sign_node(ca, "node-9", "IP:10.0.0.9");
    const NodeCert replica   = sign_node(ca, "node-2", "IP:127.0.0.1");
    ASSERT_FALSE(elsewhere.cert.empty());

    const auto srv = ob::TlsContext::node_server(elsewhere.cert, elsewhere.key, ca.cert);
    const auto cli = ob::TlsContext::node_client(replica.cert, replica.key, ca.cert);

    const Shaken s = shake(srv, cli, "127.0.0.1");
    EXPECT_FALSE(s.client_ok)
        << "a certificate for 10.0.0.9 satisfied a client that dialled 127.0.0.1";
    EXPECT_NE(s.client_why.find("certificate"), std::string::npos)
        << "the refusal should name the certificate, not a transport fault: " << s.client_why;
    fs::remove_all(ca.dir);
}

// ── Startup refusals ─────────────────────────────────────────────────────────

TEST(TlsNodeLink, NodeContextsRefuseAMissingTrustAnchor) {
    const Ca ca = make_ca();
    ASSERT_FALSE(ca.cert.empty());
    const NodeCert node = sign_node(ca, "node-1", "IP:127.0.0.1");
    ASSERT_FALSE(node.cert.empty());

    // Empty, not "fall back to the system trust store". That fallback would mean every public CA on
    // earth may introduce a replica, which is why this is a throw and not a default.
    EXPECT_THROW(ob::TlsContext::node_server(node.cert, node.key, ""), std::runtime_error);
    EXPECT_THROW(ob::TlsContext::node_client(node.cert, node.key, ""), std::runtime_error);
    EXPECT_THROW(ob::TlsContext::node_server(node.cert, node.key, ca.dir + "/absent.pem"),
                 std::runtime_error);
    fs::remove_all(ca.dir);
}

TEST(TlsNodeLink, NodeContextsRefuseAKeyReadableBeyondItsOwner) {
    const Ca ca = make_ca();
    ASSERT_FALSE(ca.cert.empty());
    const NodeCert node = sign_node(ca, "node-1", "IP:127.0.0.1");
    ASSERT_FALSE(node.cert.empty());
    ASSERT_EQ(::chmod(node.key.c_str(), 0644), 0);

    // Same rule and same message shape as the secret files of part one, and it has to be repeated
    // for the node factories because each one checks its own files: a private key every local
    // process can read is not a private key.
    EXPECT_THROW(ob::TlsContext::node_server(node.cert, node.key, ca.cert), std::runtime_error);
    EXPECT_THROW(ob::TlsContext::node_client(node.cert, node.key, ca.cert), std::runtime_error);
    fs::remove_all(ca.dir);
}

TEST(TlsNodeLink, ANodeServerContextRequiresAPeerCertificateByConfiguration) {
    // The behavioural test above is what matters; this pins the flag it rests on, so a change that
    // drops FAIL_IF_NO_PEER_CERT fails with a message naming the setting rather than only as a
    // handshake that unexpectedly succeeded.
    const Ca ca = make_ca();
    ASSERT_FALSE(ca.cert.empty());
    const NodeCert node = sign_node(ca, "node-1", "IP:127.0.0.1");
    ASSERT_FALSE(node.cert.empty());

    const auto srv = ob::TlsContext::node_server(node.cert, node.key, ca.cert);
    auto* raw = reinterpret_cast<SSL_CTX*>(srv.raw());
    const int mode = SSL_CTX_get_verify_mode(raw);
    EXPECT_TRUE(mode & SSL_VERIFY_PEER);
    EXPECT_TRUE(mode & SSL_VERIFY_FAIL_IF_NO_PEER_CERT)
        << "without this a peer presenting no certificate completes the handshake and the link is "
           "encrypted and anonymous";
    EXPECT_EQ(SSL_CTX_get_min_proto_version(raw), TLS1_3_VERSION);

    const auto cli = ob::TlsContext::node_client(node.cert, node.key, ca.cert);
    EXPECT_TRUE(SSL_CTX_get_verify_mode(reinterpret_cast<SSL_CTX*>(cli.raw())) & SSL_VERIFY_PEER)
        << "there is no way to reach SSL_VERIFY_NONE from a node-link factory, by design";
    fs::remove_all(ca.dir);
}

// ── The class made impossible rather than fixed once ─────────────────────────

TEST(TlsNodeLinkStatic, EveryPeerSocketCloseSendsCloseNotifyFirst) {
    // A descriptor closed without `close_notify` is, on the other end, indistinguishable from a
    // network failure - so a peer cannot tell an orderly disconnect from a partition, and the
    // mesh's own diagnostics get worse the more correctly it shuts down.
    //
    // `multi_master.cpp` closes a peer socket in fourteen places, each a different protocol error.
    // Asserting the *shape* rather than remembering the list: every one of those lines must be
    // immediately preceded by `release_tls(peer);`, which is where the close_notify happens. A
    // fifteenth disconnect path added without it fails here.
    const std::string src = read_source("src/multi_master.cpp");
    ASSERT_FALSE(src.empty());

    size_t closes = 0;
    size_t guarded = 0;
    std::string previous;
    std::istringstream lines(src);
    std::string line;
    while (std::getline(lines, line)) {
        const auto first = line.find_first_not_of(" \t");
        const std::string stripped = (first == std::string::npos) ? "" : line.substr(first);
        if (stripped == "::close(peer.fd);") {
            ++closes;
            if (previous == "release_tls(peer);") ++guarded;
            else ADD_FAILURE() << "a peer socket is closed without close_notify; the line above it "
                                  "is: " << previous;
        }
        if (!stripped.empty()) previous = stripped;
    }
    EXPECT_GT(closes, 10u) << "found only " << closes << " peer closes, so this test has stopped "
                              "looking at what it thinks it is looking at";
    EXPECT_EQ(closes, guarded);
}

TEST(TlsNodeLinkStatic, NodeLinkSocketIoGoesThroughTheChannel) {
    // A raw `::recv` on a TLS socket hands ciphertext to a frame parser and a raw `::send` writes
    // plaintext into a record stream. Both fail loudly rather than silently, which is why this is a
    // pinned count with an explanation rather than an elaborate shape test: the job of the failure
    // is to make whoever adds a fifteenth call site read this paragraph.
    //
    // Permitted sites, and nothing else:
    //   multi_master.cpp - one `::recv` (the plaintext branch of the io_loop read) and one `::send`
    //                      (the plaintext branch of try_drain_send_buf_plain).
    //   replication.cpp  - one `::recv` (the plaintext branch of `BufferedReader::pull`), one
    //                      `::send` in `blocking_send_all` (the replica client's blocking socket),
    //                      one in `drain_send_buffer_plain`, and one `::write` in `accept_replica`
    //                      for `ERR max_replicas_reached`, which is deliberately plaintext-only
    //                      because the peer has negotiated nothing yet.
    // Everything else goes through `TlsChannel` or through `BufferedReader::pull`.
    struct Expect { const char* file; const char* call; size_t count; };
    const Expect expected[] = {
        {"src/multi_master.cpp", "::recv(", 1},
        {"src/multi_master.cpp", "::send(", 1},
        {"src/replication.cpp",  "::recv(", 1},
        {"src/replication.cpp",  "::send(", 2},
        {"src/replication.cpp",  "::write(", 1},
    };
    for (const auto& e : expected) {
        const std::string src = read_source(e.file);
        ASSERT_FALSE(src.empty()) << e.file;
        size_t n = 0;
        for (size_t pos = src.find(e.call); pos != std::string::npos;
             pos = src.find(e.call, pos + 1)) {
            ++n;
        }
        EXPECT_EQ(n, e.count)
            << e.file << " has " << n << " occurrences of " << e.call << ", expected " << e.count
            << ". A new socket call on a node-link descriptor bypasses TLS: route it through "
               "TlsChannel (writes) or BufferedReader::pull (reads). If the new one is deliberately "
               "plaintext, say why here and change the number.";
    }
}
