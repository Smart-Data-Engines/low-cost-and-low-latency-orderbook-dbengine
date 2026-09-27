// The two ends of a mesh link for tests that drive a MultiMasterManager's protocol by hand: a peer
// record backed by a real socket pair, and the frames the manager wrote into it. Shared by the
// snapshot tests (#76) and the catch-up tests (#178), which drive the same manager the same way.
#pragma once

#include "orderbook/multi_master.hpp"
#include "orderbook/wal.hpp"

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

namespace mm_test {

struct TmpDir {
    std::string path;
    TmpDir() {
        char tpl[] = "/tmp/ob_mm_snap_XXXXXX";
        char* dir = ::mkdtemp(tpl);
        if (!dir) throw std::runtime_error("mkdtemp failed");
        path = dir;
    }
    ~TmpDir() { std::error_code ec; std::filesystem::remove_all(path, ec); }
    TmpDir(const TmpDir&) = delete;
    TmpDir& operator=(const TmpDir&) = delete;
};

/// A peer entry backed by a real socket, so enqueue_frame() actually writes somewhere.
struct WiredPeer {
    ob::PeerConnection peer;
    int local_fd{-1};      // what the manager writes into
    int remote_fd{-1};     // what the test reads from
    std::vector<uint8_t> inbox;

    /// `tiny_buffers` makes the kernel socket buffer small enough that a single chunk fills it.
    /// Without that, try_drain_send_buf() empties `send_buf` on every enqueue and the low
    /// watermark is never reached — so no test could ever observe a transfer in progress.
    explicit WiredPeer(uint16_t node_id, bool tiny_buffers = false) {
        int sv[2];
        if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) throw std::runtime_error("socketpair");
        local_fd  = sv[0];
        remote_fd = sv[1];
        // Non-blocking, like every peer socket the manager really deals with. A blocking socket
        // makes try_drain_send_buf() block inside send() once the buffer fills, instead of
        // reporting EAGAIN and arming EPOLLOUT — which in a single-threaded test is a deadlock
        // against a reader that only runs after this call returns.
        for (int fd : {local_fd, remote_fd}) {
            const int flags = ::fcntl(fd, F_GETFL, 0);
            ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        }
        if (tiny_buffers) {
            const int size = 2048;
            ::setsockopt(local_fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size));
            ::setsockopt(remote_fd, SOL_SOCKET, SO_RCVBUF, &size, sizeof(size));
        }
        peer.node_id        = node_id;
        peer.fd             = local_fd;
        peer.connected      = true;
        peer.handshake_done = true;
    }
    ~WiredPeer() {
        if (local_fd >= 0) ::close(local_fd);
        if (remote_fd >= 0) ::close(remote_fd);
    }
    WiredPeer(const WiredPeer&) = delete;
    WiredPeer& operator=(const WiredPeer&) = delete;

private:
    ob::PeerConnection* installed_{nullptr};

public:

    /// The record the manager operates on: the same socket, but living in the manager's peer table.
    ///
    /// Needed since #79, because the snapshot path looks its target up there rather than keeping the
    /// reference it was handed — a request and its finished snapshot are separated by a worker
    /// thread, and the peer can be gone by then. A test that drove the manager through its own copy
    /// would be driving a different `send_buf` from the one the manager fills.
    ob::PeerConnection& mgr(ob::MultiMasterManager& mm) {
        if (installed_ == nullptr) installed_ = &mm.install_peer_for_test(peer);
        return *installed_;
    }

    /// Move whatever the manager has written into `inbox`.
    void collect() {
        uint8_t buf[64 * 1024];
        for (;;) {
            const ssize_t n = ::recv(remote_fd, buf, sizeof(buf), MSG_DONTWAIT);
            if (n <= 0) break;
            inbox.insert(inbox.end(), buf, buf + n);
        }
    }
};

struct Frame {
    ob::WALRecordV2 hdr{};
    std::vector<uint8_t> payload;
};

/// Split `inbox` into frames the way parse_frames() does, consuming what is complete.
std::vector<Frame> take_frames(std::vector<uint8_t>& inbox) {
    std::vector<Frame> out;
    size_t pos = 0;
    for (;;) {
        if (inbox.size() - pos < ob::MM_FRAME_HEADER_SIZE) break;
        uint32_t len = 0;
        std::memcpy(&len, inbox.data() + pos, sizeof(len));
        if (inbox.size() - pos < ob::MM_FRAME_HEADER_SIZE + len) break;
        const uint8_t* body = inbox.data() + pos + ob::MM_FRAME_HEADER_SIZE;

        Frame f;
        if (len >= ob::MM_WALRECORD_V2_SIZE) {
            std::memcpy(&f.hdr, body, ob::MM_WALRECORD_V2_SIZE);
            f.payload.assign(body + ob::MM_WALRECORD_V2_SIZE, body + len);
        }
        out.push_back(std::move(f));
        pos += ob::MM_FRAME_HEADER_SIZE + len;
    }
    inbox.erase(inbox.begin(), inbox.begin() + static_cast<std::ptrdiff_t>(pos));
    return out;
}

}  // namespace mm_test
