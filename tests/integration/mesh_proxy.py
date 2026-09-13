"""A TCP proxy between two mesh peers, so a test can break the link without touching the engine.

#54 stage C. The engine needs no change to be tested through this: `PeerRegistry::register_self()`
runs **once**, at start (`src/multi_master.cpp:350`), publishing `127.0.0.1:<mm-replication-port>`
under `<prefix>mm_peers/<node_id>`. There is no re-publication, so the harness overwrites that value
with a proxy's address after the node is up and every peer that dials it reaches the proxy instead.
`ClusterManager.redirect_peer()` does that.

**Why `partition()` buffers and delivers on `heal()`, rather than discarding.** A real partition
drops segments; TCP retransmits them; when the partition lifts the retransmission arrives. So the
bytes are **late, not gone** — and a proxy that threw them away would be modelling a *broken
connection*, which is what `close_connections()` is for. Two different faults, two methods. This
distinction is the whole reason pitfall 24 exists in this repository: an `iptables DROP` proves
nothing about a repair mechanism, because the frames sit in the sender's buffer and arrive when the
rule goes away. Here the difference is explicit and each half is reachable on purpose.

**`partition()` also stops reading, and that is not a detail.** If the proxy kept draining the
sender while holding the bytes, the sender would never feel backpressure: its socket buffer would
stay empty, `ob_mm_peer_send_buf_bytes` would stay at zero, and the engine would have no way to know
anything was wrong — which is exactly the thing requirement 1.4 says must be visible. Stopping the
read lets the proxy's receive buffer fill, then TCP zero-windows the sender, then the sender's own
buffer fills and the engine starts queueing. That is what a partition does to a sender, and it is
what makes "no node accepts writes it cannot replicate without saying so" testable at all.

**One proxy is not enough for a pair, and the reason is measured rather than assumed.** The mesh is
symmetric: node 1 dials node 2 and node 2 dials node 1. The documented tie-break keeps the link the
**lower-numbered** node opened (`src/multi_master.cpp:1023`) — but #96 measured **zero** double
links even with every node started at once, because the etcd topology watch is orders of magnitude
slower than a loopback connect, so the second dialler always finds the peer already connected. The
surviving link is therefore whichever was dialled *first*, which a test cannot choose. Redirect
**both** addresses and the pair is partitioned whichever link won.
"""

from __future__ import annotations

import socket
import threading
import time
from typing import Optional


class MeshProxy:
    """Forwards one mesh port, with the link under a test's control.

    Threads rather than `select`: two per connection, one each way, which is the shape that makes
    "stop reading" expressible per direction. A `select` loop would need the same state anyway and
    would put the fault decision in a place where an accept and a pump share it.
    """

    def __init__(self, target_host: str, target_port: int, name: str = "proxy",
                 recv_buffer: Optional[int] = None):
        """`recv_buffer` shrinks `SO_RCVBUF` on the sockets this proxy accepts.

        Without it, a partition has to overcome the kernel's default buffers before the *engine*
        starts queueing: measured in #93, a loopback pair absorbs **2.6 MB** before the sender sees
        its first `EAGAIN`. A test that wants to see `ob_mm_peer_send_buf_bytes` grow would then
        have to push megabytes, and what it would mostly be measuring is Linux.

        With a few kilobytes of receive buffer the window closes almost immediately and the
        engine's own queue is reachable in a handful of writes. #93's own test used
        `SO_RCVBUF=4096` for the same reason — and recorded the cost, because 2 MB through a 2 kB
        window took 49 seconds there. Keep the volume small when using this.
        """
        self.target = (target_host, target_port)
        self.name = name
        self.recv_buffer = recv_buffer
        self.listen_port: int = 0
        self._listener: Optional[socket.socket] = None
        self._accept_thread: Optional[threading.Thread] = None
        self._running = threading.Event()
        # Guards every field below it. Held only for field access, never across a socket call: a
        # pump that blocked while holding this would stop `partition()` from returning.
        self._lock = threading.Lock()
        self._partitioned = False
        self._budget: Optional[int] = None       # bytes still allowed through, None = unlimited
        self._held: dict = {}                    # id(sock) -> bytes withheld, flushed on heal
        self._sockets: list = []                 # every live socket, for close_connections()
        self._forwarded_out = 0                  # client -> target
        self._forwarded_in = 0                   # target -> client

    # ── lifecycle ────────────────────────────────────────────────────────────

    def start(self) -> str:
        """Bind, accept in the background, and return the address to advertise."""
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(16)
        self.listen_port = self._listener.getsockname()[1]
        self._running.set()
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True,
                                               name=f"meshproxy-{self.name}-accept")
        self._accept_thread.start()
        return f"127.0.0.1:{self.listen_port}"

    def stop(self) -> None:
        """Close everything. Safe to call twice, and called from a fixture's teardown."""
        self._running.clear()
        listener, self._listener = self._listener, None
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass
        self.close_connections()
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=5)
            self._accept_thread = None

    # ── the faults ───────────────────────────────────────────────────────────

    def partition(self) -> None:
        """Stop pumping in both directions, keeping the sockets open and the bytes.

        The connection stays **formally up**, which is requirement 1.4's wording and the whole
        point: a node must not conclude it has replicated something merely because its socket is
        still there.
        """
        with self._lock:
            self._partitioned = True

    def heal(self) -> None:
        """Resume, delivering what was withheld first, in the order it arrived."""
        with self._lock:
            self._partitioned = False
            self._budget = None

    def close_connections(self) -> int:
        """Drop every live connection where it stands. Returns how many were closed.

        Combined with `stall_after()` this is "the link was cut in the middle of a frame": the
        receiver has a prefix of a frame and no more is coming.
        """
        with self._lock:
            socks, self._sockets = self._sockets, []
        for sock in socks:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass
        return len(socks)

    def stall_after(self, n: int) -> None:
        """Let `n` more bytes through **in either direction**, then stop reading.

        A byte budget rather than a chunk count, because the fault worth producing is a stop at a
        chosen offset — which is how a frame gets cut in half. `partition()` can only stop at
        whatever boundary a `recv` happened to land on.

        **Shared between the two directions, and that is not laziness.** The first version spent it
        only on client->target, which made the fault depend on *who dialled*: in a symmetric mesh
        the surviving link is whichever end connected first, so the writes under test travelled the
        direction the budget did not cover and `held_bytes()` came back 0. A test cannot choose
        that, so the budget is a property of the link. The cost is named: a heartbeat travelling
        the other way spends it too, which makes the cut arrive sooner rather than later — the safe
        direction for a fault to be wrong in.
        """
        with self._lock:
            self._budget = max(0, int(n))

    # ── observation ──────────────────────────────────────────────────────────

    def forwarded(self) -> tuple:
        """Bytes delivered each way: (client -> target, target -> client).

        Reported rather than only asserted on: "the link carried nothing while partitioned" is a
        number, and a test that says it without one is asserting its own setup.
        """
        with self._lock:
            return (self._forwarded_out, self._forwarded_in)

    def held_bytes(self) -> int:
        """Bytes read from a sender and not yet delivered. Zero unless partitioned or stalled."""
        with self._lock:
            return sum(len(b) for b in self._held.values())

    # ── internals ────────────────────────────────────────────────────────────

    def _accept_loop(self) -> None:
        while self._running.is_set():
            try:
                client, _ = self._listener.accept()
            except OSError:
                return  # the listener was closed by stop()
            if self.recv_buffer is not None:
                # On the accepted socket, so the *sender* - the engine dialling us - is the one
                # whose window closes. Setting it on the upstream socket would throttle the peer
                # instead, which is a different fault.
                try:
                    client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.recv_buffer)
                except OSError:
                    pass
            try:
                upstream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # A timeout rather than a blocking connect: this is loopback, so it should be
                # instant, and a helper that can hang is pitfall 265 — a blocking connect in a
                # test harness once cost 127 seconds of SYN retries and read as a test timeout.
                upstream.settimeout(5)
                upstream.connect(self.target)
                upstream.settimeout(None)
            except OSError:
                try:
                    client.close()
                except OSError:
                    pass
                continue

            with self._lock:
                self._sockets.extend((client, upstream))

            for src, dst, outbound in ((client, upstream, True), (upstream, client, False)):
                threading.Thread(target=self._pump, args=(src, dst, outbound), daemon=True,
                                 name=f"meshproxy-{self.name}-{'out' if outbound else 'in'}").start()

    def _pump(self, src: socket.socket, dst: socket.socket, outbound: bool) -> None:
        """Move bytes one way, subject to the current fault.

        **Everything read goes into the withheld buffer first, and only an allowance leaves it.**
        The first version of this checked the fault *before* `recv` and forwarded whatever the call
        returned, so a chunk that arrived while the pump was already inside `recv` — a 0.25 s
        window — crossed a partitioned link. Its own control test caught that, and a second defect
        with it: the flush condition asked only "not partitioned", so a remainder withheld by an
        exhausted byte budget was delivered on the very next iteration. Reading into the buffer and
        metering the way out makes both unexpressible rather than fixed: order is preserved by
        construction, and there is one place that decides how much may pass.
        """
        src.settimeout(0.25)  # so a fault is noticed without a second wake-up mechanism
        while self._running.is_set():
            if not self._deliver_allowance(src, dst, outbound):
                return

            with self._lock:
                # Not reading while blocked is deliberate: see the class docstring. Draining here
                # would hide the fault from the sender, and the sender noticing is the property
                # under test.
                blocked = self._partitioned or self._budget == 0
            if blocked:
                time.sleep(0.05)
                continue

            try:
                chunk = src.recv(65536)
            except socket.timeout:
                continue
            except OSError:
                return
            if not chunk:
                return  # orderly close by the sender

            with self._lock:
                self._held[id(src)] = self._held.get(id(src), b"") + chunk
            if not self._deliver_allowance(src, dst, outbound):
                return

    def _allowance(self, outbound: bool, size: int) -> int:
        """How many of `size` withheld bytes may be delivered right now.

        The single place a fault decides anything. A partition allows nothing; a byte budget allows
        what is left of it and spends that; otherwise everything passes.
        """
        with self._lock:
            if self._partitioned:
                return 0
            if self._budget is not None:
                allowed = min(size, self._budget)
                self._budget -= allowed
                return allowed
            return size

    def _deliver_allowance(self, src: socket.socket, dst: socket.socket, outbound: bool) -> bool:
        """Send as much of the withheld buffer as the current fault permits. False if the link died."""
        with self._lock:
            pending = self._held.get(id(src), b"")
        if not pending:
            return True
        allowed = self._allowance(outbound, len(pending))
        if allowed <= 0:
            return True
        head, tail = pending[:allowed], pending[allowed:]
        with self._lock:
            if tail:
                self._held[id(src)] = tail
            else:
                self._held.pop(id(src), None)
        return self._send_all(dst, head, outbound)

    def _send_all(self, dst: socket.socket, data: bytes, outbound: bool) -> bool:
        try:
            dst.sendall(data)
        except OSError:
            return False
        with self._lock:
            if outbound:
                self._forwarded_out += len(data)
            else:
                self._forwarded_in += len(data)
        return True
