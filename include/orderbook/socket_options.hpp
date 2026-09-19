#pragma once

#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#include "orderbook/logger.hpp"

namespace ob {

/// Turn Nagle's algorithm off on a connected TCP socket.
///
/// **Why every socket in this engine wants this.** Nagle holds a small write while any earlier
/// byte on the connection is still unacknowledged, and the peer's delayed-ACK timer is what
/// eventually releases it. Both halves are correct on their own, and together they cost tens of
/// milliseconds on any exchange where one side writes twice before the other has a reason to
/// answer. Every message this engine puts on a wire is small: a `SELECT` row, `OK\n\n`, a `PUSH`
/// line, a WAL record, an `ACK`.
///
/// Measured before it was set on the accepted client socket (Amazon EC2 m9g.xlarge, quiet box,
/// loopback, one connection, 20,000 updates of 20 levels): a client pipelining eight `MINSERT`s
/// per round trip spent **52.75 ms** on each, and the figure did not move with the batch size —
/// **51.68 ms** at 64 and **52.15 ms** at 512, so it was a timer rather than any per-byte cost.
/// The server's own CPU across those three runs was 0.16, 0.12 and 0.14 seconds for the same
/// 400,000 levels, so none of that wall time was work.
/// The diagnosis is the client-side control: re-arming `TCP_QUICKACK` before every `recv` so the
/// client acknowledges immediately, changing nothing on the server, took the same 250 round trips
/// from **12.963 s to 0.021 s**. Nothing else about either process changed, so what the pipelining
/// client was paying for was the server's second response sitting in the server's kernel waiting
/// for an acknowledgement the client had no reason to send yet.
///
/// The request/response client never saw it and never could: with one response outstanding there
/// is nothing unacknowledged when the next write happens, so Nagle has nothing to hold. That is
/// why the defect survived every benchmark this repository has published — all of them ask one
/// question at a time.
///
/// **The asymmetry this closes.** Before #140 the engine set `TCP_NODELAY` on every socket it
/// *dialled* — both mesh directions, the C++ client library — and on no socket it *accepted*.
/// `tests/test_socket_options.cpp` derives the connection-holding sites from the source and
/// counts them against the calls each file makes, because the sixth of them is the one that will
/// be added without it — and counting per file rather than per socket would have let
/// `src/replication.cpp`, which holds two, lose one of them silently.
///
/// A failure is logged and not fatal: the connection works, it is just slower, and refusing to
/// serve a client over a socket option would be the worse trade.
inline void set_tcp_nodelay(int fd, const char* component) {
    int on = 1;
    if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on)) < 0) {
        OB_LOG_WARN(component,
                    "TCP_NODELAY failed on fd=%d: %s; this connection will pay Nagle's delay on "
                    "any exchange where we write twice before the peer answers",
                    fd, std::strerror(errno));
    } else {
        OB_LOG_DEBUG(component, "TCP_NODELAY set on fd=%d", fd);
    }
}

}  // namespace ob
