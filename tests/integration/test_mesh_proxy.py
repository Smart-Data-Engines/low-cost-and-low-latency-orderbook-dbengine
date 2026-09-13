"""The mesh proxy's own tests: a control on the instrument, not a test of the engine (#54 stage C).

`test_fault_injector.py` exists for the same reason one layer down. A proxy that silently dropped a
byte, or delivered withheld bytes out of order, would make every engine test behind it report on the
proxy instead — and it would report *green*, because the engine would look like it had lost data.

Every case here runs against a loopback echo server and no cluster, so the whole file is a fraction
of a second.
"""

from __future__ import annotations

import socket
import threading
import time

import pytest

from mesh_proxy import MeshProxy

pytestmark = pytest.mark.smoke


class Echo:
    """A server that returns what it is sent, so a test can watch bytes make the round trip."""

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(8)
        self.port = self.sock.getsockname()[1]
        self.received = bytearray()
        self._lock = threading.Lock()
        self._running = True
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        while self._running:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn):
        conn.settimeout(0.25)
        while self._running:
            try:
                data = conn.recv(65536)
            except socket.timeout:
                continue
            except OSError:
                return
            if not data:
                return
            with self._lock:
                self.received += data
            try:
                conn.sendall(data)
            except OSError:
                return

    def bytes_seen(self) -> bytes:
        with self._lock:
            return bytes(self.received)

    def stop(self):
        self._running = False
        try:
            self.sock.close()
        except OSError:
            pass


@pytest.fixture
def link():
    """An echo server with a proxy in front of it, both torn down whatever the test did."""
    echo = Echo()
    proxy = MeshProxy("127.0.0.1", echo.port, name="test")
    address = proxy.start()
    host, port = address.split(":")
    try:
        yield echo, proxy, (host, int(port))
    finally:
        proxy.stop()
        echo.stop()


def wait_until(predicate, timeout: float = 5.0, interval: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_an_unbroken_link_delivers_everything_and_counts_it(link):
    """The control. Without this, every assertion below could be satisfied by a proxy that is
    simply broken — which is the failure mode a fault-injection instrument has by default."""
    echo, proxy, addr = link
    payload = bytes(range(256)) * 8  # 2048 bytes, and every byte value, so a mangled byte shows

    with socket.create_connection(addr, timeout=5) as sock:
        sock.sendall(payload)
        sock.settimeout(5)
        back = b""
        while len(back) < len(payload):
            chunk = sock.recv(65536)
            if not chunk:
                break
            back += chunk

    assert back == payload, "the round trip did not return what was sent"
    assert echo.bytes_seen() == payload, "the far side did not receive what was sent"

    # Waited for rather than read once. The counter is incremented **after** `sendall` returns, so
    # the client can have the bytes before the proxy has counted them - which is not a defect in
    # either, but it made this assertion report `counted 0 bytes back, echoed 2048` in a loaded
    # full-battery run while passing every time the module ran alone. A slow runner is a fuzzer for
    # orderings (pitfall 55), and the property here is "every byte is eventually counted once", not
    # "the counter is already up to date the instant I look".
    assert wait_until(lambda: proxy.forwarded() == (len(payload), len(payload))), (
        f"the proxy did not count both directions: {proxy.forwarded()} for {len(payload)} bytes "
        f"each way")
    assert proxy.held_bytes() == 0, "an unbroken link withheld something"


def test_a_partition_delivers_nothing_and_keeps_the_socket_open(link):
    echo, proxy, addr = link
    with socket.create_connection(addr, timeout=5) as sock:
        sock.sendall(b"before-")
        assert wait_until(lambda: echo.bytes_seen() == b"before-"), "the link was not working"

        proxy.partition()
        sock.sendall(b"during-the-partition")
        # Long enough that a pump which was still draining would have delivered it: the pump's own
        # recv timeout is 0.25 s, so a second is four chances to get it wrong.
        time.sleep(1.0)

        assert echo.bytes_seen() == b"before-", (
            f"bytes crossed a partitioned link: {echo.bytes_seen()!r}")
        # The socket is still there. That is the fault being modelled - a connection that is
        # formally up while nothing passes (requirement 1.4) - and it is what lets a node wrongly
        # believe it has replicated.
        sock.sendall(b"-and-more")


def test_healing_delivers_what_was_withheld_in_the_order_it_arrived(link):
    echo, proxy, addr = link
    with socket.create_connection(addr, timeout=5) as sock:
        proxy.partition()
        for part in (b"one.", b"two.", b"three."):
            sock.sendall(part)
            time.sleep(0.05)
        time.sleep(0.5)
        assert echo.bytes_seen() == b"", "something crossed while partitioned"

        proxy.heal()
        assert wait_until(lambda: echo.bytes_seen() == b"one.two.three."), (
            f"heal did not deliver the withheld bytes in order: {echo.bytes_seen()!r}")


def test_a_byte_budget_stops_at_the_offset_it_was_given(link):
    """The fault that cuts a frame in half: stop after a chosen byte, not at a chunk boundary."""
    echo, proxy, addr = link
    with socket.create_connection(addr, timeout=5) as sock:
        proxy.stall_after(10)
        sock.sendall(b"0123456789ABCDEFGHIJ")  # 20 bytes, half the budget
        assert wait_until(lambda: echo.bytes_seen() == b"0123456789"), (
            f"the budget did not stop where it was told: {echo.bytes_seen()!r}")

        time.sleep(0.5)
        assert echo.bytes_seen() == b"0123456789", "more crossed after the budget ran out"
        # `>=`, not `== 10`, and the reason is the budget being a property of the **link**: the
        # echo's ten bytes come back and are withheld too, so the exact figure depends on whether
        # the return pump has been scheduled yet. 20 in a loaded run, 10 in an idle one — neither
        # is a defect, and asserting the idle number made this test fail inside the full battery
        # while passing alone. What is a property is that the remainder was **withheld rather than
        # dropped**, which is what the heal below proves outright.
        assert proxy.held_bytes() >= 10, (
            f"the remainder was dropped rather than withheld: held {proxy.held_bytes()}")

        # And it is a stall, not a loss: healing delivers the rest.
        proxy.heal()
        assert wait_until(lambda: echo.bytes_seen() == b"0123456789ABCDEFGHIJ"), (
            f"the withheld remainder never arrived: {echo.bytes_seen()!r}")


def test_closing_connections_ends_them_where_they_stand(link):
    echo, proxy, addr = link
    sock = socket.create_connection(addr, timeout=5)
    try:
        sock.sendall(b"hello")
        assert wait_until(lambda: echo.bytes_seen() == b"hello")
        # Drain the echo first. Without this the assertion below reads the reply that is already
        # in this socket's receive buffer and calls it "not closed" - the test's own bug, and the
        # kind that reads as a defect in the thing under test.
        sock.settimeout(5)
        assert sock.recv(64) == b"hello"

        assert proxy.close_connections() >= 2, (
            "closing reported fewer than the two sockets a proxied connection has")

        sock.settimeout(5)
        # The reader sees the close: either zero bytes or a reset. Both are "the link is gone";
        # a timeout would not be, which is why this asserts rather than sleeping.
        try:
            assert sock.recv(64) == b"", "the connection was not closed"
        except ConnectionResetError:
            pass
    finally:
        sock.close()


def test_a_proxy_that_was_never_started_is_not_a_silent_pass():
    """A control on the control: the instrument must fail loudly when it was not set up.

    An unstarted proxy has port 0, so a test that forgot `start()` would connect to nothing. This
    pins that the failure is a refused connection rather than a mysterious empty read.
    """
    proxy = MeshProxy("127.0.0.1", 1, name="unstarted")
    assert proxy.listen_port == 0
    with pytest.raises(OSError):
        socket.create_connection(("127.0.0.1", proxy.listen_port), timeout=2)
