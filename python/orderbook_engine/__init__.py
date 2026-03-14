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

import ctypes
import ctypes.util
import os
import socket
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Union

__version__ = "0.2.0"
__all__ = ["OrderbookEngine", "OrderbookRow", "OrderbookError"]


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

    def __init__(self, host: str, port: int, timeout: float = 10.0):
        self._host = host
        self._port = port
        self._sock: Optional[socket.socket] = None
        self._buf = b""
        self._timeout = timeout
        self._connect()

    def _connect(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.settimeout(self._timeout)
        self._sock.connect((self._host, self._port))
        self._buf = b""
        # Read and discard the welcome banner ("OK ob_tcp_server v...\n")
        self._read_banner()

    def _read_banner(self):
        """Read the welcome banner line from the server."""
        while True:
            decoded = self._buf.decode("utf-8", errors="replace")
            nl = decoded.find("\n")
            if nl != -1:
                # Consume the banner line
                consumed = decoded[:nl + 1].encode("utf-8")
                self._buf = self._buf[len(consumed):]
                return
            chunk = self._sock.recv(4096)
            if not chunk:
                raise OrderbookError(-1, "Connection closed before banner")
            self._buf += chunk

    def _send(self, line: str):
        """Send a command line (must end with \\n)."""
        if not line.endswith("\n"):
            line += "\n"
        self._sock.sendall(line.encode("utf-8"))

    def _recv_response(self) -> str:
        """
        Read a complete response from the server.

        Response termination rules:
          - "ERR ...\n"  → single line ending with \n
          - "PONG\n"     → single line
          - "OK\n\n"     → bare OK (double newline)
          - "OK\n...\n\n" → OK with body, terminated by empty line (\n\n)
        """
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
                 timeout: float = 10.0):
        self._seq = 0
        self._closed = False

        if host is not None:
            # TCP mode
            self._mode = "tcp"
            self._tcp = _TcpBackend(host, port, timeout)
            self._local = None
        elif data_dir is not None:
            # Local mode
            self._mode = "local"
            self._local = _LocalBackend(data_dir)
            self._tcp = None
        else:
            raise ValueError("Provide data_dir for local mode or host for TCP mode")

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
        else:
            # TCP: one INSERT per level
            for i in range(n):
                cmd = f"INSERT {symbol} {exchange} {side_lower} {prices[i]} {qtys[i]} {counts[i]}"
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
        else:
            raw = self._tcp.execute(sql)
            is_err, msg, header, data_rows = _parse_tcp_response(raw)
            if is_err:
                raise OrderbookError(-1, f"TCP query error: {msg}")
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
        """Send PING, expect PONG. Works in both modes."""
        if self._closed:
            raise OrderbookError(-1, "Engine is closed")
        if self._mode == "local":
            return "PONG"
        raw = self._tcp.execute("PING")
        if raw.startswith("PONG"):
            return "PONG"
        raise OrderbookError(-1, f"Unexpected PING response: {raw}")

    def status(self) -> dict:
        """Get server status. In TCP mode returns server stats, in local mode returns mode info."""
        if self._closed:
            raise OrderbookError(-1, "Engine is closed")
        if self._mode == "local":
            return {"mode": "local"}
        raw = self._tcp.execute("STATUS")
        is_err, msg, header, data_rows = _parse_tcp_response(raw)
        if is_err:
            raise OrderbookError(-1, f"TCP STATUS error: {msg}")
        if data_rows and len(data_rows[0]) >= 3:
            return {
                "mode": "tcp",
                "sessions": int(data_rows[0][0]),
                "queries": int(data_rows[0][1]),
                "inserts": int(data_rows[0][2]),
            }
        return {"mode": "tcp"}
