// Session's TLS byte path against a real handshake (#30 part three).
//
// Why here and not in the integration battery: the hazard is a partial `SSL_write` followed by
// `send_buf_.erase(0, n)`, and forcing a partial write needs the socket send buffer to fill. Over
// loopback that buffer autotunes into megabytes - measured, 788 871 bytes of response went out
// without `SSL_write` returning WANT_WRITE even once, with the client's SO_RCVBUF at 8 kB and a
// reader taking 4 kB every 20 ms. So the integration test could not provoke the condition, and both
// mutations survived it.
//
// Here the buffers are ours to shrink, and the condition is reached in milliseconds.

#include "orderbook/session.hpp"
#include "orderbook/tls.hpp"

#include <gtest/gtest.h>

#include <openssl/ssl.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <thread>

namespace fs = std::filesystem;

namespace {

struct KeyPair { std::string cert, key; };

KeyPair generate(const std::string& tag) {
    const auto dir = fs::temp_directory_path() / ("ob_tlss_" + tag + std::to_string(std::rand()));
    fs::create_directories(dir);
    const std::string cert = (dir / "cert.pem").string();
    const std::string key  = (dir / "key.pem").string();
    const std::string cmd = "openssl req -x509 -newkey rsa:2048 -keyout '" + key + "' -out '" +
                            cert + "' -days 1 -nodes -subj '/CN=localhost' >/dev/null 2>&1";
    if (std::system(cmd.c_str()) != 0) return {"", ""};
    ::chmod(key.c_str(), 0600);
    return {cert, key};
}

void set_nonblocking(int fd) {
    ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
}

} // namespace

// ── The case both SSL_CTX modes exist for ────────────────────────────────────

TEST(TlsSession, ALargeResponseSurvivesPartialWritesAndTheEraseThatFollowsThem) {
    // Each partial `SSL_write` is followed by `send_buf_.erase(0, n)`, which moves the *pending*
    // bytes to a different address. OpenSSL refuses a retry that presents a different address
    // unless SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER is set, and accepts nothing at all until the whole
    // buffer fits unless SSL_MODE_ENABLE_PARTIAL_WRITE is - both measured in
    // benchmarks/tls/ssl_write_retry.c.
    //
    // The assertion is on the **content** the client receives, because the two mutations fail
    // differently: one with an error from flush_output(), one by making no progress at all.
    const auto kp = generate("big");
    ASSERT_FALSE(kp.cert.empty()) << "openssl tool not available";

    // Listener.
    const int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(listen_fd, 0);
    int one = 1;
    ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = 0;
    ASSERT_EQ(::bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    socklen_t len = sizeof(addr);
    ASSERT_EQ(::getsockname(listen_fd, reinterpret_cast<sockaddr*>(&addr), &len), 0);
    ASSERT_EQ(::listen(listen_fd, 1), 0);

    // A payload comfortably larger than any buffer here, with a recognisable shape so a truncated
    // or reordered stream fails rather than counting right.
    const size_t kChunks = 40000;   // about 1.2 MB
    std::string payload;
    payload.reserve(kChunks * 32);
    for (size_t i = 0; i < kChunks; ++i) {
        payload += "row-" + std::to_string(i) + "-0123456789abcdef\n";
    }

    std::atomic<bool> handshake_done{false};
    std::string received;
    received.reserve(payload.size() + 4096);

    // The client: handshake, then deliberately do not read for a while, then drain everything.
    std::thread client([&] {
        auto ctx = ob::TlsContext::client("", /*verify=*/false);
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        // Small receive buffer: with the server's send buffer also shrunk, this is what makes a
        // partial write certain rather than likely.
        int rcv = 4096;
        ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcv, sizeof(rcv));
        int nodelay = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
        ASSERT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);

        auto ssl = ctx.wrap(fd, /*server_side=*/false);
        while (SSL_connect(ssl.get()) != 1) {
            const int e = SSL_get_error(ssl.get(), -1);
            if (e != SSL_ERROR_WANT_READ && e != SSL_ERROR_WANT_WRITE) {
                ADD_FAILURE() << "client handshake failed: " << ob::tls_last_error();
                ::close(fd);
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        handshake_done.store(true);

        // Not reading, so the server's queue backs up and flush_output() must hit WANT_WRITE.
        std::this_thread::sleep_for(std::chrono::milliseconds(400));

        char buf[4096];
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (received.size() < payload.size() &&
               std::chrono::steady_clock::now() < deadline) {
            const int n = SSL_read(ssl.get(), buf, sizeof(buf));
            if (n > 0) { received.append(buf, static_cast<size_t>(n)); continue; }
            const int e = SSL_get_error(ssl.get(), n);
            if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE) {
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
                continue;
            }
            break;
        }
        ::close(fd);
    });

    const int fd = ::accept(listen_fd, nullptr, nullptr);
    ASSERT_GE(fd, 0);
    // Large enough to accept a few whole TLS records, small enough to block long before the
    // payload ends. That window is the whole point: `sent_total` only advances on a *fully*
    // accepted `SSL_write`, so with a send buffer below one record (16 kB) every WANT_WRITE arrives
    // with `sent_total == 0`, the erase is skipped, and the retry presents the same address - which
    // is legal. Three earlier versions of this test used 4 kB and detected nothing.
    int snd = 64 * 1024;
    ::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &snd, sizeof(snd));
    int nodelay = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
    set_nonblocking(fd);

    auto server_ctx = ob::TlsContext::server(kp.cert, kp.key);
    ob::Session session(fd, /*conn_id=*/1);
    session.enable_tls(server_ctx.wrap(fd, /*server_side=*/true));

    const auto hs_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (session.tls_handshaking() && std::chrono::steady_clock::now() < hs_deadline) {
        ASSERT_TRUE(session.continue_tls_handshake()) << ob::tls_last_error();
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    ASSERT_FALSE(session.tls_handshaking()) << "server handshake did not complete";

    // Queue the whole payload, then push it out the way the epoll loop does: flush, and flush again
    // whenever bytes remain. This is the loop that erases and retries.
    ASSERT_TRUE(session.send_response(payload));
    bool saw_backpressure = false;
    size_t distinct_pending = 0;
    size_t last_pending = session.pending_output_bytes() + 1;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (session.has_pending_output() && std::chrono::steady_clock::now() < deadline) {
        if (session.pending_output_bytes() == payload.size()) {
            // Nothing at all accepted on the first pass is what "no partial writes" looks like.
        }
        saw_backpressure = true;
        const size_t pending_now = session.pending_output_bytes();
        if (pending_now != last_pending) { ++distinct_pending; last_pending = pending_now; }
        ASSERT_TRUE(session.flush_output())
            << "flush_output failed on a large TLS response: " << ob::tls_last_error();
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }

    // What SSL_MODE_ENABLE_PARTIAL_WRITE buys, and it is *not* correctness: without it SSL_write is
    // all-or-nothing per call, so `sent_total` stays 0 for the whole drain and `send_buf_` never
    // shrinks until the last call. The bytes still arrive - measured, that mutation passes the
    // content assertion below - but `pending_output_bytes()` stays pinned at the full size, which
    // is the number `ob_pending_bytes` publishes and the number an operator reads as "this client
    // is not draining". A gauge that cannot move is a gauge that says the wrong thing.
    EXPECT_GT(distinct_pending, 3u)
        << "pending output never shrank while draining (" << distinct_pending
        << " distinct values), so the queue depth an operator sees is pinned at the full response";

    EXPECT_TRUE(saw_backpressure)
        << "the payload went out in one pass, so this test did not exercise a partial write - "
           "shrink the buffers or grow the payload";
    EXPECT_FALSE(session.has_pending_output())
        << "the response never drained: " << session.pending_output_bytes() << " bytes left";

    client.join();
    ::close(fd);
    ::close(listen_fd);

    ASSERT_EQ(received.size(), payload.size())
        << "the client received " << received.size() << " of " << payload.size() << " bytes";
    EXPECT_EQ(received, payload) << "the bytes that arrived are not the bytes that were queued";
    fs::remove_all(fs::path(kp.cert).parent_path());
}
