"""
orderbook_engine — Python bindings for the orderbook-dbengine.

Two modes of operation:
  1. Local (ctypes) — direct in-process access via shared library
  2. TCP — connect to a running ob_tcp_server over the network

Usage (local):
    from orderbook_engine import OrderbookEngine
    engine = OrderbookEngine("/tmp/ob_data")

Usage (TCP):
    from orderbook_engine import OrderbookEngine
    engine = OrderbookEngine(host="192.168.1.10", port=5555)

Both modes expose the same API: insert(), flush(), query(), close().
"""

import base64
import ctypes
import ctypes.util
import json
import logging
import os
import socket
import struct
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Union

logger = logging.getLogger("orderbook_engine")

__version__ = "0.2.0"
__all__ = ["OrderbookEngine", "OrderbookRow", "OrderbookError",
           "_murmurhash3_x86_32", "_ConsistentHashRing",
           "_parse_shard_map_response", "_parse_shard_info_response",
           "_parse_shard_error"]


# ── Data types ─────────────────────────────────────────────────────────────────

@dataclass
class OrderbookRow:
    """A single row returned by a query."""
    timestamp_ns: int
    price: int
    quantity: int
    order_count: int
    side: str       # "bid" or "ask"
    level: int

    @property
    def price_float(self) -> float:
        """Price as a float assuming sub-unit = 1/100 (cents)."""
        return self.price / 100.0

    def __repr__(self) -> str:
        return (f"OrderbookRow(ts={self.timestamp_ns}, "
                f"side={self.side}, level={self.level}, "
                f"price={self.price}, qty={self.quantity}, "
                f"orders={self.order_count})")


class OrderbookError(Exception):
    """Raised on engine errors (both local and TCP)."""
    def __init__(self, status: int = -1, message: str = ""):
        self.status = status
        super().__init__(message or f"error (status={status})")


# ── Status codes (local mode) ─────────────────────────────────────────────────

OB_OK              =  0
OB_ERR_INVALID_ARG = -1
OB_ERR_NOT_FOUND   = -2
OB_ERR_PARSE       = -3
OB_ERR_IO          = -4
OB_ERR_MMAP_FAILED = -5
OB_ERR_INTERNAL    = -99


# ── TCP response parser ───────────────────────────────────────────────────────

def _parse_tcp_response(raw: str):
    """
    Parse a TCP wire-format response.

    Wire format:
      Error:   "ERR <message>\n"
      PONG:    "PONG\n"
      OK body: "OK\n<header_tsv>\n<row1_tsv>\n...\n\n"
      OK bare: "OK\n\n"

    Returns (is_error, error_msg, header_cols, data_rows)
    """
    if raw.startswith("ERR "):
        msg = raw[4:].rstrip("\n")
        return True, msg, [], []

    if raw.startswith("PONG"):
        return False, "", [], []

    if raw.startswith("OK\n"):
        body = raw[3:]
        lines = body.split("\n")
        # Find header
        idx = 0
        while idx < len(lines) and not lines[idx]:
            idx += 1
        if idx >= len(lines):
            return False, "", [], []
        header = lines[idx].split("\t")
        idx += 1
        rows = []
        while idx < len(lines) and lines[idx]:
            rows.append(lines[idx].split("\t"))
            idx += 1
        return False, "", header, rows

    # bare OK
    if raw.strip() == "OK":
        return False, "", [], []

    return True, f"unexpected response: {raw[:80]}", [], []


# ── TCP Backend ────────────────────────────────────────────────────────────────

class _TcpBackend:
    """Communicates with ob_tcp_server over a TCP socket."""

    def __init__(self, host: str, port: int, timeout: float = 10.0,
                 compress: bool = False):
        self._host = host
        self._port = port
        self._sock: Optional[socket.socket] = None
        self._buf = b""
        self._timeout = timeout
        self._compressed = False
        self._connect()
        if compress:
            self._negotiate_compression()

    def _connect(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.settimeout(self._timeout)
        self._sock.connect((self._host, self._port))
        self._buf = b""
        # Read and discard the welcome banner ("OK ob_tcp_server v...\n")
        self._read_banner()

    def _read_banner(self):
        """Read the welcome banner from the server (terminated by \\n\\n)."""
        while True:
            decoded = self._buf.decode("utf-8", errors="replace")
            end = decoded.find("\n\n")
            if end != -1:
                # Consume the banner including the double newline
                consumed = decoded[:end + 2].encode("utf-8")
                self._buf = self._buf[len(consumed):]
                return
            chunk = self._sock.recv(4096)
            if not chunk:
                raise OrderbookError(-1, "Connection closed before banner")
            self._buf += chunk

    def _negotiate_compression(self):
        """Negotiate LZ4 compression with the server.

        Sends COMPRESS LZ4 command and verifies the OK response.
        Must be called before any other commands (right after connect).
        """
        self._send("COMPRESS LZ4")
        resp = self._recv_response()
        if not resp.startswith("OK COMPRESS LZ4"):
            raise ConnectionError(
                f"Server does not support LZ4 compression: {resp.strip()}")
        self._compressed = True

    def _send(self, line: str):
        """Send a command line (must end with \\n).

        When compression is active, the payload is LZ4-frame compressed
        and prefixed with a 4-byte big-endian length header:
          [4-byte BE frame_len][LZ4 compressed frame]
        This matches the server's Session::feed() binary framing.
        """
        if not line.endswith("\n"):
            line += "\n"
        data = line.encode("utf-8")
        if self._compressed:
            import lz4.frame
            import struct
            compressed = lz4.frame.compress(data)
            header = struct.pack(">I", len(compressed))
            self._sock.sendall(header + compressed)
        else:
            self._sock.sendall(data)

    def _recv_response(self) -> str:
        """
        Read a complete response from the server.

        When compression is active, incoming data is accumulated until a
        complete LZ4 frame is received, then decompressed before parsing.

        Response termination rules:
          - "ERR ...\n"  → single line ending with \n
          - "PONG\n"     → single line
          - "OK\n\n"     → bare OK (double newline)
          - "OK\n...\n\n" → OK with body, terminated by empty line (\n\n)
          - "PRIMARY ...\n" / "REPLICA ...\n" / "STANDALONE\n" → single line (ROLE response)
        """
        if self._compressed:
            return self._recv_compressed_response()
        return self._recv_plain_response()

    def _recv_compressed_response(self) -> str:
        """Receive and decompress a length-prefixed LZ4 response.

        Server sends: [4-byte BE frame_len][LZ4 compressed frame]
        This matches Session::send_response() binary framing.
        """
        import lz4.frame
        import struct

        # Read 4-byte length header
        while len(self._buf) < 4:
            try:
                chunk = self._sock.recv(65536)
            except socket.timeout:
                raise OrderbookError(-1, "TCP recv timeout")
            if not chunk:
                raise OrderbookError(-1, "TCP connection closed by server")
            self._buf += chunk

        frame_len = struct.unpack(">I", self._buf[:4])[0]
        self._buf = self._buf[4:]

        # Read the full compressed frame
        while len(self._buf) < frame_len:
            try:
                chunk = self._sock.recv(65536)
            except socket.timeout:
                raise OrderbookError(-1, "TCP recv timeout")
            if not chunk:
                raise OrderbookError(-1, "TCP connection closed by server")
            self._buf += chunk

        compressed = bytes(self._buf[:frame_len])
        self._buf = self._buf[frame_len:]

        decompressed = lz4.frame.decompress(compressed)
        return decompressed.decode("utf-8", errors="replace")

    def _recv_plain_response(self) -> str:
        """Receive an uncompressed response (original logic)."""
        while True:
            decoded = self._buf.decode("utf-8", errors="replace")

            # ERR line
            if decoded.startswith("ERR "):
                nl = decoded.find("\n")
                if nl != -1:
                    resp = decoded[:nl + 1]
                    self._buf = self._buf[len(resp.encode("utf-8")):]
                    return resp

            # PONG line
            if decoded.startswith("PONG"):
                nl = decoded.find("\n")
                if nl != -1:
                    resp = decoded[:nl + 1]
                    self._buf = self._buf[len(resp.encode("utf-8")):]
                    return resp

            # ROLE responses: single-line, terminated by \n
            if (decoded.startswith("PRIMARY") or
                decoded.startswith("REPLICA") or
                decoded.startswith("STANDALONE")):
                nl = decoded.find("\n")
                if nl != -1:
                    resp = decoded[:nl + 1]
                    self._buf = self._buf[len(resp.encode("utf-8")):]
                    return resp

            # OK response — terminated by \n\n
            if decoded.startswith("OK"):
                pos = decoded.find("\n\n")
                if pos != -1:
                    resp = decoded[:pos + 2]
                    self._buf = self._buf[len(resp.encode("utf-8")):]
                    return resp

            # Read more data
            try:
                chunk = self._sock.recv(65536)
            except socket.timeout:
                raise OrderbookError(-1, "TCP recv timeout")
            if not chunk:
                raise OrderbookError(-1, "TCP connection closed by server")
            self._buf += chunk

    def execute(self, command: str) -> str:
        """Send a command and return the raw response string."""
        self._send(command)
        return self._recv_response()

    def close(self):
        if self._sock:
            try:
                self._send("QUIT")
            except Exception:
                pass
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None


# ── Local (ctypes) Backend ─────────────────────────────────────────────────────

def _lib_suffix() -> str:
    if sys.platform == "darwin":
        return ".dylib"
    elif sys.platform == "win32":
        return ".dll"
    return ".so"


def _find_library() -> str:
    suffix = _lib_suffix()
    lib_name = f"liborderbook_shared{suffix}"
    candidates = [
        Path(__file__).parent / lib_name,
        Path(__file__).parent.parent.parent / "build" / lib_name,
        Path("build") / lib_name,
        Path(lib_name),
    ]
    env_path = os.environ.get("OB_LIB_PATH")
    if env_path:
        candidates.insert(0, Path(env_path))
    for p in candidates:
        if p.exists():
            return str(p)
    raise FileNotFoundError(
        f"Cannot find {lib_name}.\n"
        "Build with: cmake -S . -B build && cmake --build build --target orderbook_shared\n"
        "Or set OB_LIB_PATH=/path/to/liborderbook_shared.so"
    )


def _load_lib():
    lib = ctypes.CDLL(_find_library())
    lib.ob_engine_create.argtypes = [ctypes.c_char_p]
    lib.ob_engine_create.restype = ctypes.c_void_p
    lib.ob_engine_destroy.argtypes = [ctypes.c_void_p]
    lib.ob_engine_destroy.restype = None
    lib.ob_apply_delta.argtypes = [
        ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p,
        ctypes.c_uint64, ctypes.c_uint64,
        ctypes.POINTER(ctypes.c_int64), ctypes.POINTER(ctypes.c_uint64),
        ctypes.POINTER(ctypes.c_uint32), ctypes.c_uint32, ctypes.c_int,
    ]
    lib.ob_apply_delta.restype = ctypes.c_int
    lib.ob_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    lib.ob_query.restype = ctypes.c_void_p
    lib.ob_result_free.argtypes = [ctypes.c_void_p]
    lib.ob_result_free.restype = None
    lib.ob_result_next.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(ctypes.c_int64),
        ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(ctypes.c_uint32),
        ctypes.POINTER(ctypes.c_uint8), ctypes.POINTER(ctypes.c_uint16),
    ]
    lib.ob_result_next.restype = ctypes.c_int
    return lib


class _LocalBackend:
    """Direct in-process access via ctypes / C API."""

    def __init__(self, data_dir: str):
        self._lib = _load_lib()
        self._data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self._engine = self._lib.ob_engine_create(data_dir.encode("utf-8"))
        if not self._engine:
            raise OrderbookError(OB_ERR_INTERNAL, f"Failed to create engine at {data_dir}")

    def insert(self, symbol: str, exchange: str, side_int: int,
               prices: list, qtys: list, counts: list,
               seq: int, timestamp_ns: int) -> None:
        n = len(prices)
        c_prices = (ctypes.c_int64 * n)(*prices)
        c_qtys   = (ctypes.c_uint64 * n)(*qtys)
        c_cnts   = (ctypes.c_uint32 * n)(*counts)
        rc = self._lib.ob_apply_delta(
            self._engine,
            symbol.encode("utf-8"), exchange.encode("utf-8"),
            ctypes.c_uint64(seq), ctypes.c_uint64(timestamp_ns),
            c_prices, c_qtys, c_cnts,
            ctypes.c_uint32(n), ctypes.c_int(side_int),
        )
        if rc != OB_OK:
            raise OrderbookError(rc, f"insert failed for {symbol}@{exchange}")

    def flush(self):
        self._lib.ob_engine_destroy(self._engine)
        self._engine = self._lib.ob_engine_create(self._data_dir.encode("utf-8"))
        if not self._engine:
            raise OrderbookError(OB_ERR_INTERNAL, "Failed to recreate engine after flush")

    def query(self, sql: str) -> List[OrderbookRow]:
        result = self._lib.ob_query(self._engine, sql.encode("utf-8"))
        if not result:
            raise OrderbookError(OB_ERR_PARSE, f"Query failed: {sql}")
        rows: List[OrderbookRow] = []
        ts  = ctypes.c_uint64()
        pr  = ctypes.c_int64()
        qty = ctypes.c_uint64()
        cnt = ctypes.c_uint32()
        sd  = ctypes.c_uint8()
        lvl = ctypes.c_uint16()
        while True:
            rc = self._lib.ob_result_next(
                result, ctypes.byref(ts), ctypes.byref(pr), ctypes.byref(qty),
                ctypes.byref(cnt), ctypes.byref(sd), ctypes.byref(lvl))
            if rc != OB_OK:
                break
            rows.append(OrderbookRow(
                timestamp_ns=ts.value, price=pr.value, quantity=qty.value,
                order_count=cnt.value, side="bid" if sd.value == 0 else "ask",
                level=lvl.value,
            ))
        self._lib.ob_result_free(result)
        return rows

    def ping(self) -> str:
        return "PONG"

    def status(self) -> dict:
        return {"mode": "local"}

    def close(self):
        if self._engine:
            self._lib.ob_engine_destroy(self._engine)
            self._engine = None


# ── Node state for client pool ─────────────────────────────────────────────────

@dataclass
class _NodeState:
    """Per-node tracking state for the client pool."""
    host: str
    port: int
    role: str = "unknown"       # "primary", "replica", "standalone", "unknown"
    epoch: int = 0
    connected: bool = False
    last_check: float = 0.0     # time.monotonic()


# ── MurmurHash3 (Python implementation) ────────────────────────────────────────

def _murmurhash3_x86_32(data: bytes, seed: int = 0) -> int:
    """MurmurHash3_x86_32 — pure Python implementation matching C++ version."""
    c1 = 0xCC9E2D51
    c2 = 0x1B873593
    mask = 0xFFFFFFFF

    h1 = seed & mask
    length = len(data)
    n_blocks = length // 4

    for i in range(n_blocks):
        k1 = int.from_bytes(data[i * 4:(i + 1) * 4], byteorder="little", signed=False)
        k1 = (k1 * c1) & mask
        k1 = ((k1 << 15) | (k1 >> 17)) & mask
        k1 = (k1 * c2) & mask
        h1 ^= k1
        h1 = ((h1 << 13) | (h1 >> 19)) & mask
        h1 = (h1 * 5 + 0xE6546B64) & mask

    tail = data[n_blocks * 4:]
    k1 = 0
    tail_len = len(tail)
    if tail_len >= 3:
        k1 ^= tail[2] << 16
    if tail_len >= 2:
        k1 ^= tail[1] << 8
    if tail_len >= 1:
        k1 ^= tail[0]
        k1 = (k1 * c1) & mask
        k1 = ((k1 << 15) | (k1 >> 17)) & mask
        k1 = (k1 * c2) & mask
        h1 ^= k1

    h1 ^= length
    # fmix32
    h1 ^= h1 >> 16
    h1 = (h1 * 0x85EBCA6B) & mask
    h1 ^= h1 >> 13
    h1 = (h1 * 0xC2B2AE35) & mask
    h1 ^= h1 >> 16

    return h1


class _ConsistentHashRing:
    """Consistent hash ring with virtual nodes (Python implementation)."""

    def __init__(self):
        self._ring: Dict[int, str] = {}
        self._sorted_keys: List[int] = []
        self._shard_vnodes: Dict[str, List[int]] = {}

    def add_shard(self, shard_id: str, vnodes: int = 150):
        """Add a shard with virtual nodes to the ring."""
        hashes = []
        for i in range(vnodes):
            vnode_key = f"{shard_id}#{i}"
            h = _murmurhash3_x86_32(vnode_key.encode("utf-8"))
            self._ring[h] = shard_id
            hashes.append(h)
        self._shard_vnodes[shard_id] = hashes
        self._sorted_keys = sorted(self._ring.keys())

    def remove_shard(self, shard_id: str):
        """Remove a shard and its virtual nodes from the ring."""
        hashes = self._shard_vnodes.pop(shard_id, [])
        for h in hashes:
            self._ring.pop(h, None)
        self._sorted_keys = sorted(self._ring.keys())

    def lookup(self, key: str) -> str:
        """Find the shard responsible for a given key."""
        if not self._sorted_keys:
            return ""
        h = _murmurhash3_x86_32(key.encode("utf-8"))
        # Binary search for first hash >= h
        lo, hi = 0, len(self._sorted_keys)
        while lo < hi:
            mid = (lo + hi) // 2
            if self._sorted_keys[mid] < h:
                lo = mid + 1
            else:
                hi = mid
        if lo >= len(self._sorted_keys):
            lo = 0  # wrap around
        return self._ring[self._sorted_keys[lo]]

    def shard_count(self) -> int:
        return len(self._shard_vnodes)


# ── Sharding response parsers ─────────────────────────────────────────────────

def _parse_shard_map_response(raw: str) -> dict:
    """Parse SHARD_MAP response: OK\\n<json>\\n\\n → dict."""
    if raw.startswith("ERR "):
        msg = raw[4:].rstrip("\n")
        raise OrderbookError(-1, f"SHARD_MAP error: {msg}")
    if raw.startswith("OK\n"):
        body = raw[3:].rstrip("\n")
        if body.strip():
            return json.loads(body)
    return {}


def _parse_shard_info_response(raw: str) -> dict:
    """Parse SHARD_INFO response: OK\\n<tsv>\\n\\n → dict."""
    if raw.startswith("ERR "):
        msg = raw[4:].rstrip("\n")
        raise OrderbookError(-1, f"SHARD_INFO error: {msg}")
    result = {}
    if raw.startswith("OK\n"):
        body = raw[3:]
        for line in body.split("\n"):
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t", 1)
            if len(parts) == 2:
                result[parts[0]] = parts[1]
    return result


def _parse_shard_error(raw: str):
    """Parse sharding error responses.

    Returns (error_type, detail) or (None, None) if not a shard error.
    error_type: "NOT_OWNER", "SYMBOL_MIGRATED", or None
    detail: symbol key or new address
    """
    if not raw.startswith("ERR "):
        return None, None
    msg = raw[4:].rstrip("\n")
    if msg.startswith("NOT_OWNER "):
        return "NOT_OWNER", msg[len("NOT_OWNER "):]
    if msg.startswith("SYMBOL_MIGRATED "):
        return "SYMBOL_MIGRATED", msg[len("SYMBOL_MIGRATED "):]
    return None, None


class _ClientPool:
    """
    Multi-host connection pool with automatic primary discovery and shard routing.

    Connects to all hosts, issues ROLE commands to discover the primary,
    and routes writes to the primary and reads to any available node.

    When coordinator_endpoints is provided, enables shard routing:
      - Fetches ShardMap from etcd at initialization
      - Routes INSERT/MINSERT per symbol to the correct shard
      - Routes SELECT per symbol (single-shard) or fan-out (multi-symbol)
      - Periodic ShardMap refresh in health-check thread
      - Handles ERR SYMBOL_MIGRATED (refresh + retry 1x)

    When coordinator_endpoints is not provided:
      - Behavior unchanged (backward compatible primary/replica routing)
    """

    def __init__(self, hosts: List[str], timeout: float = 10.0,
                 health_check_interval: float = 2.0,
                 compress: bool = False,
                 coordinator_endpoints: Optional[List[str]] = None,
                 cluster_prefix: str = "/ob/"):
        self._timeout = timeout
        self._health_check_interval = health_check_interval
        self._compress = compress
        self._nodes: List[_NodeState] = []
        self._connections: dict = {}  # "host:port" -> _TcpBackend
        self._primary_key: Optional[str] = None
        self._health_thread: Optional[object] = None
        self._running = False

        # Sharding fields
        self._coordinator_endpoints = coordinator_endpoints
        self._cluster_prefix = cluster_prefix
        self._shard_map: Optional[dict] = None
        self._shard_connections: dict = {}  # shard_id → _TcpBackend
        self._hash_ring = _ConsistentHashRing()

        import threading
        self._lock = threading.Lock()

        # Parse hosts into _NodeState objects.
        for h in hosts:
            if ":" in h:
                host, port_str = h.rsplit(":", 1)
                port = int(port_str)
            else:
                host = h
                port = 5555
            self._nodes.append(_NodeState(host=host, port=port))

        # Initial discovery.
        self._connect_all()
        self._discover_primary()

        # Initialize sharding if coordinator endpoints provided.
        if self._coordinator_endpoints:
            self._init_sharding()

        # Start health check thread.
        self._running = True
        self._health_thread = threading.Thread(
            target=self._health_check_loop, daemon=True)
        self._health_thread.start()

    def _node_key(self, node: _NodeState) -> str:
        return f"{node.host}:{node.port}"

    def _connect_all(self):
        """Connect to all nodes that aren't already connected."""
        for node in self._nodes:
            key = self._node_key(node)
            if key in self._connections:
                continue
            try:
                backend = _TcpBackend(node.host, node.port, self._timeout,
                                      compress=self._compress)
                self._connections[key] = backend
                node.connected = True
            except Exception:
                node.connected = False

    def _discover_primary(self):
        """Issue ROLE command to all connected nodes, find the primary.

        Standalone nodes (no coordinator) that are not read-only are treated
        as primary for write routing purposes.
        """
        with self._lock:
            for node in self._nodes:
                key = self._node_key(node)
                backend = self._connections.get(key)
                if backend is None:
                    node.connected = False
                    continue
                try:
                    raw = backend.execute("ROLE")
                    self._parse_role_response(node, raw)
                    node.last_check = time.monotonic()
                except Exception:
                    node.connected = False
                    node.role = "unknown"
                    # Remove broken connection.
                    self._connections.pop(key, None)

            # Find primary. Fall back to first standalone node (writable).
            self._primary_key = None
            for node in self._nodes:
                if node.role == "primary":
                    self._primary_key = self._node_key(node)
                    break
            if self._primary_key is None:
                for node in self._nodes:
                    if node.role == "standalone" and node.connected:
                        self._primary_key = self._node_key(node)
                        break

    def _parse_role_response(self, node: _NodeState, raw: str):
        """Parse ROLE command response and update node state."""
        raw = raw.strip()
        if raw.startswith("PRIMARY"):
            node.role = "primary"
            parts = raw.split()
            if len(parts) >= 2:
                try:
                    node.epoch = int(parts[1])
                except ValueError:
                    pass
        elif raw.startswith("REPLICA"):
            node.role = "replica"
            parts = raw.split()
            if len(parts) >= 3:
                try:
                    node.epoch = int(parts[-1])
                except ValueError:
                    pass
        elif raw.startswith("STANDALONE"):
            node.role = "standalone"
        else:
            node.role = "unknown"

    def execute_write(self, command: str) -> str:
        """Route write to primary, retry on failover (re-discover + retry once)."""
        with self._lock:
            primary_key = self._primary_key

        if primary_key is None:
            self._discover_primary()
            with self._lock:
                primary_key = self._primary_key
            if primary_key is None:
                raise OrderbookError(-1, "No primary available")

        backend = self._connections.get(primary_key)
        if backend is None:
            raise OrderbookError(-1, "Primary not connected")

        try:
            raw = backend.execute(command)
            # Check for read-only error (stale primary).
            if raw.startswith("ERR") and "read-only" in raw:
                raise OrderbookError(-1, "read-only replica")
            return raw
        except (OrderbookError, OSError, socket.error):
            # Retry once: re-discover primary and retry.
            self._connect_all()
            self._discover_primary()
            with self._lock:
                primary_key = self._primary_key
            if primary_key is None:
                raise OrderbookError(-1, "No primary available after re-discovery")
            backend = self._connections.get(primary_key)
            if backend is None:
                raise OrderbookError(-1, "Primary not connected after re-discovery")
            return backend.execute(command)

    def execute_read(self, command: str) -> str:
        """Route read to any available node, fallback on failure."""
        # Try all nodes, starting with any connected one.
        with self._lock:
            node_keys = [self._node_key(n) for n in self._nodes if n.connected]

        for key in node_keys:
            backend = self._connections.get(key)
            if backend is None:
                continue
            try:
                return backend.execute(command)
            except Exception:
                # Try next node.
                continue

        raise OrderbookError(-1, "All hosts unreachable")

    def _health_check_loop(self):
        """Background thread: periodic ROLE checks on all nodes."""
        while self._running:
            time.sleep(self._health_check_interval)
            if not self._running:
                break
            self._connect_all()
            self._discover_primary()
            # Refresh shard map if sharding enabled
            if self._coordinator_endpoints:
                try:
                    new_map = self._fetch_shard_map()
                    if new_map and new_map != self._shard_map:
                        logger.debug("Health check: refreshing shard map")
                        self._shard_map = new_map
                        self._connect_shards()
                        self._rebuild_hash_ring()
                except Exception:
                    logger.debug("Health check: shard map refresh failed, using cached")

    # ── Sharding methods ───────────────────────────────────────────────────

    def _init_sharding(self):
        """Fetch ShardMap from etcd and establish connections to shards."""
        logger.info("Shard mode enabled with %d coordinator endpoints",
                     len(self._coordinator_endpoints))
        self._shard_map = self._fetch_shard_map()
        self._connect_shards()
        self._rebuild_hash_ring()

    def _fetch_shard_map(self) -> dict:
        """Fetch ShardMap from etcd via HTTP GET to v3 REST API."""
        import urllib.request
        for endpoint in self._coordinator_endpoints:
            try:
                url = f"{endpoint}/v3/kv/range"
                key_str = f"{self._cluster_prefix}shard_map"
                key_b64 = base64.b64encode(key_str.encode()).decode()
                body = json.dumps({"key": key_b64}).encode()
                req = urllib.request.Request(
                    url, data=body,
                    headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=5) as resp:
                    data = json.loads(resp.read())
                    if "kvs" in data and data["kvs"]:
                        value = base64.b64decode(data["kvs"][0]["value"])
                        shard_map = json.loads(value)
                        version = shard_map.get("version", 0)
                        shards = shard_map.get("shards", {})
                        logger.info("Fetched shard map version=%d with %d shards",
                                    version, len(shards))
                        return shard_map
            except Exception:
                continue
        return {}

    def _connect_shards(self):
        """Establish _TcpBackend connections to each shard from ShardMap."""
        if not self._shard_map or "shards" not in self._shard_map:
            return
        shards = self._shard_map["shards"]
        # Close connections to shards no longer in the map
        for sid in list(self._shard_connections.keys()):
            if sid not in shards:
                try:
                    self._shard_connections[sid].close()
                except Exception:
                    pass
                del self._shard_connections[sid]
        # Connect to new/updated shards
        for shard_id, shard_info in shards.items():
            if shard_id in self._shard_connections:
                continue
            address = shard_info.get("address", "")
            if not address:
                continue
            try:
                if ":" in address:
                    host, port_str = address.rsplit(":", 1)
                    port = int(port_str)
                else:
                    host = address
                    port = 5555
                backend = _TcpBackend(host, port, self._timeout,
                                      compress=self._compress)
                self._shard_connections[shard_id] = backend
                logger.debug("Connected to shard %s at %s", shard_id, address)
            except Exception:
                logger.debug("Failed to connect to shard %s at %s",
                             shard_id, address)

    def _rebuild_hash_ring(self):
        """Rebuild the consistent hash ring from the current shard map."""
        self._hash_ring = _ConsistentHashRing()
        if not self._shard_map or "shards" not in self._shard_map:
            return
        for shard_id, shard_info in self._shard_map["shards"].items():
            vnodes = shard_info.get("vnodes", 150)
            self._hash_ring.add_shard(shard_id, vnodes)

    def _resolve_shard(self, symbol: str, exchange: str) -> str:
        """Find shard_id for a symbol. Fallback: consistent hashing."""
        key = f"{symbol}.{exchange}"
        if self._shard_map and "assignments" in self._shard_map:
            shard_id = self._shard_map["assignments"].get(key)
            if shard_id:
                logger.debug("Resolved %s.%s → shard %s (map lookup)",
                             symbol, exchange, shard_id)
                return shard_id
        # Fallback: consistent hashing
        shard_id = self._consistent_hash_lookup(key)
        logger.debug("Resolved %s.%s → shard %s (consistent hash)",
                     symbol, exchange, shard_id)
        return shard_id

    def _consistent_hash_lookup(self, key: str) -> str:
        """MurmurHash3 consistent hash ring lookup."""
        return self._hash_ring.lookup(key)

    def execute_write_sharded(self, symbol: str, exchange: str,
                               command: str) -> str:
        """Route write to the correct shard based on symbol."""
        shard_id = self._resolve_shard(symbol, exchange)
        backend = self._shard_connections.get(shard_id)
        if backend is None:
            raise OrderbookError(-1, f"Shard {shard_id} not connected")
        try:
            raw = backend.execute(command)
            err_type, detail = _parse_shard_error(raw)
            if err_type == "SYMBOL_MIGRATED":
                # Refresh shard map and retry 1x
                logger.warning("Symbol migrated: %s.%s, refreshing shard map",
                               symbol, exchange)
                self._shard_map = self._fetch_shard_map()
                self._connect_shards()
                self._rebuild_hash_ring()
                shard_id = self._resolve_shard(symbol, exchange)
                backend = self._shard_connections.get(shard_id)
                if backend is None:
                    raise OrderbookError(
                        -1, f"Shard {shard_id} not connected after refresh")
                raw = backend.execute(command)
            return raw
        except (OSError, socket.error) as e:
            logger.error("Shard %s unreachable for symbol %s.%s",
                         shard_id, symbol, exchange)
            raise OrderbookError(-1, f"Shard {shard_id} unreachable") from e

    @property
    def is_sharded(self) -> bool:
        """True if sharding is enabled."""
        return self._coordinator_endpoints is not None

    def _route_query(self, sql: str) -> str:
        """Route a query to the correct shard(s) based on symbol in SQL.

        Extracts symbol from FROM clause (e.g. FROM 'AAPL'.'XNAS').
        If symbol found → route to owning shard.
        If not found → fan-out to all shards and return first non-empty result.
        """
        import re
        # Try to extract symbol.exchange from SQL: FROM 'symbol'.'exchange'
        match = re.search(r"FROM\s+'([^']+)'\s*\.\s*'([^']+)'", sql, re.IGNORECASE)
        if match:
            symbol, exchange = match.group(1), match.group(2)
            shard_id = self._resolve_shard(symbol, exchange)
            backend = self._shard_connections.get(shard_id)
            if backend is not None:
                try:
                    return backend.execute(sql)
                except (OSError, socket.error):
                    pass
        # Fan-out: query all shards, return first non-error response
        for sid, backend in self._shard_connections.items():
            try:
                raw = backend.execute(sql)
                if not raw.startswith("ERR "):
                    return raw
            except Exception:
                continue
        # Fallback to non-sharded read
        return self.execute_read(sql)

    def close(self):
        """Stop health checks and close all connections."""
        self._running = False
        for key, backend in list(self._connections.items()):
            try:
                backend.close()
            except Exception:
                pass
        self._connections.clear()
        # Close shard connections
        for sid, backend in list(self._shard_connections.items()):
            try:
                backend.close()
            except Exception:
                pass
        self._shard_connections.clear()


# ── Unified Engine class ───────────────────────────────────────────────────────

class OrderbookEngine:
    """
    Python interface to the orderbook-dbengine.

    Two modes:
      Local:  OrderbookEngine("/tmp/ob_data")
      TCP:    OrderbookEngine(host="10.0.0.1", port=5555)

    Both expose the same API.
    """

    def __init__(self,
                 data_dir: Optional[str] = None,
                 *,
                 host: Optional[str] = None,
                 port: int = 5555,
                 hosts: Optional[List[str]] = None,
                 timeout: float = 10.0,
                 health_check_interval: float = 2.0,
                 compress: bool = False,
                 coordinator_endpoints: Optional[List[str]] = None,
                 cluster_prefix: str = "/ob/"):
        self._seq = 0
        self._closed = False
        self._pool: Optional[_ClientPool] = None

        if hosts is not None:
            # Multi-host pool mode
            self._mode = "pool"
            self._pool = _ClientPool(hosts, timeout, health_check_interval,
                                     compress=compress,
                                     coordinator_endpoints=coordinator_endpoints,
                                     cluster_prefix=cluster_prefix)
            self._tcp = None
            self._local = None
        elif host is not None:
            # TCP mode
            self._mode = "tcp"
            self._tcp = _TcpBackend(host, port, timeout, compress=compress)
            self._local = None
        elif data_dir is not None:
            # Local mode
            self._mode = "local"
            self._local = _LocalBackend(data_dir)
            self._tcp = None
        else:
            raise ValueError("Provide data_dir for local mode, host for TCP mode, or hosts for pool mode")

    @property
    def mode(self) -> str:
        """'local' or 'tcp'."""
        return self._mode

    @property
    def seq(self) -> int:
        return self._seq

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def close(self):
        """Shut down the engine / disconnect."""
        if self._closed:
            return
        self._closed = True
        if self._local:
            self._local.close()
        if self._tcp:
            self._tcp.close()
        if self._pool:
            self._pool.close()

    def insert(self,
               symbol: str,
               exchange: str,
               side: str,
               prices: List[int],
               qtys: List[int],
               counts: Optional[List[int]] = None,
               timestamp_ns: Optional[int] = None,
               seq: Optional[int] = None) -> int:
        """
        Insert price levels into the orderbook.

        In TCP mode, each level is sent as a separate INSERT command
        (the wire protocol is single-level per INSERT).
        """
        if self._closed:
            raise OrderbookError(-1, "Engine is closed")

        n = len(prices)
        if len(qtys) != n:
            raise ValueError("prices and qtys must have the same length")
        if counts is None:
            counts = [1] * n
        if len(counts) != n:
            raise ValueError("counts must have the same length as prices")

        if seq is None:
            self._seq += 1
            seq = self._seq
        else:
            self._seq = max(self._seq, seq)

        ts = timestamp_ns if timestamp_ns is not None else int(time.time_ns())
        side_lower = side.lower()
        side_int = 1 if side_lower == "ask" else 0

        if self._mode == "local":
            self._local.insert(symbol, exchange, side_int, prices, qtys, counts, seq, ts)
        elif self._mode == "pool":
            # Pool mode: route writes to primary or shard.
            if n > 1:
                header = f"MINSERT {symbol} {exchange} {side_lower} {n}"
                payload_lines = [f"{prices[i]} {qtys[i]} {counts[i]}" for i in range(n)]
                cmd = header + "\n" + "\n".join(payload_lines)
            else:
                cmd = f"INSERT {symbol} {exchange} {side_lower} {prices[0]} {qtys[0]} {counts[0]}"
            if self._pool.is_sharded:
                raw = self._pool.execute_write_sharded(symbol, exchange, cmd)
            else:
                raw = self._pool.execute_write(cmd)
            is_err, msg, _, _ = _parse_tcp_response(raw)
            if is_err:
                raise OrderbookError(-1, f"Pool INSERT failed: {msg}")
        else:
            # TCP mode
            if n > 1:
                # MINSERT: single round-trip for multiple levels
                header = f"MINSERT {symbol} {exchange} {side_lower} {n}"
                payload_lines = [f"{prices[i]} {qtys[i]} {counts[i]}" for i in range(n)]
                cmd = header + "\n" + "\n".join(payload_lines)
                raw = self._tcp.execute(cmd)
                is_err, msg, _, _ = _parse_tcp_response(raw)
                if is_err:
                    raise OrderbookError(-1, f"TCP MINSERT failed: {msg}")
            else:
                # INSERT: backward compat for single level
                cmd = f"INSERT {symbol} {exchange} {side_lower} {prices[0]} {qtys[0]} {counts[0]}"
                raw = self._tcp.execute(cmd)
                is_err, msg, _, _ = _parse_tcp_response(raw)
                if is_err:
                    raise OrderbookError(-1, f"TCP INSERT failed: {msg}")

        return seq

    def flush(self):
        """Flush pending data so it becomes queryable."""
        if self._closed:
            raise OrderbookError(-1, "Engine is closed")
        if self._mode == "local":
            self._local.flush()
        elif self._mode == "pool":
            raw = self._pool.execute_write("FLUSH")
            is_err, msg, _, _ = _parse_tcp_response(raw)
            if is_err:
                raise OrderbookError(-1, f"Pool FLUSH failed: {msg}")
        else:
            raw = self._tcp.execute("FLUSH")
            is_err, msg, _, _ = _parse_tcp_response(raw)
            if is_err:
                raise OrderbookError(-1, f"TCP FLUSH failed: {msg}")

    def query(self, sql: str) -> List[OrderbookRow]:
        """Execute a SQL query and return rows."""
        if self._closed:
            raise OrderbookError(-1, "Engine is closed")
        if self._mode == "local":
            return self._local.query(sql)

        # TCP or pool mode — get raw response.
        if self._mode == "pool":
            # In sharded mode, try to extract symbol from SQL for routing
            if self._pool.is_sharded:
                raw = self._pool._route_query(sql)
            else:
                raw = self._pool.execute_read(sql)
        else:
            raw = self._tcp.execute(sql)

        is_err, msg, header, data_rows = _parse_tcp_response(raw)
        if is_err:
            raise OrderbookError(-1, f"query error: {msg}")
        # Parse TSV rows into OrderbookRow objects
        # Header: timestamp_ns  price  quantity  order_count  side  level
        rows: List[OrderbookRow] = []
        for r in data_rows:
            if len(r) < 6:
                continue
            rows.append(OrderbookRow(
                timestamp_ns=int(r[0]),
                price=int(r[1]),
                quantity=int(r[2]),
                order_count=int(r[3]),
                side="bid" if r[4] == "0" else "ask",
                level=int(r[5]),
            ))
        return rows

    def query_all(self, symbol: str, exchange: str,
                  limit: Optional[int] = None) -> List[OrderbookRow]:
        """Convenience: query all rows for a symbol/exchange pair."""
        sql = (f"SELECT * FROM '{symbol}'.'{exchange}' "
               f"WHERE timestamp BETWEEN 0 AND 9999999999999999999")
        if limit is not None:
            sql += f" LIMIT {limit}"
        return self.query(sql)

    def ping(self) -> str:
        """Send PING, expect PONG. Works in all modes."""
        if self._closed:
            raise OrderbookError(-1, "Engine is closed")
        if self._mode == "local":
            return "PONG"
        if self._mode == "pool":
            raw = self._pool.execute_read("PING")
        else:
            raw = self._tcp.execute("PING")
        if raw.startswith("PONG"):
            return "PONG"
        raise OrderbookError(-1, f"Unexpected PING response: {raw}")

    def status(self) -> dict:
        """Get server status. In TCP/pool mode returns server stats, in local mode returns mode info."""
        if self._closed:
            raise OrderbookError(-1, "Engine is closed")
        if self._mode == "local":
            return {"mode": "local"}
        if self._mode == "pool":
            raw = self._pool.execute_read("STATUS")
        else:
            raw = self._tcp.execute("STATUS")
        is_err, msg, header, data_rows = _parse_tcp_response(raw)
        if is_err:
            raise OrderbookError(-1, f"STATUS error: {msg}")
        if data_rows and len(data_rows[0]) >= 3:
            return {
                "mode": self._mode,
                "sessions": int(data_rows[0][0]),
                "queries": int(data_rows[0][1]),
                "inserts": int(data_rows[0][2]),
            }
        return {"mode": self._mode}
