#!/usr/bin/env python3
"""A release's artefacts, accepted as a user would meet them (#42).

    scripts/release_acceptance.py <dist dir with the wheel> <version> [--root <extracted package>]

Run after the release's .deb is installed (the release workflow does that on its runners). With
`--root`, the server and the library are taken from an extracted .tar.gz instead and the library is
named by OB_LIB_PATH - which is how to run this on a machine nobody should install a package on, at
the price of not testing where the client looks by itself. It holds:

  - /usr/bin/ob_tcp_server reports <version>;
  - the wheel installs into a fresh venv and reports <version> from its metadata;
  - that client, run from outside the repository so that nothing of the source tree is imported,
    writes to the installed server over TCP and reads the rows back;
  - its local mode finds the installed C API library by itself - no OB_LIB_PATH - and writes, flushes
    and reads a data directory of its own.

Exit status 0 if every one holds.
"""
from __future__ import annotations

import argparse
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time

# argparse rather than reading sys.argv by position: a `--root` with its value forgotten must be an
# error, not an acceptance run quietly against whatever is installed in /usr/bin.
_parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
_parser.add_argument("dist", help="the directory holding the wheel")
_parser.add_argument("version", help="the version being released, X.Y.Z")
_parser.add_argument("--root", help="an extracted .tar.gz to take the server and library from")
_args = _parser.parse_args()
DIST, VERSION, ROOT = _args.dist, _args.version, _args.root
SERVER = os.path.join(ROOT, "usr/bin/ob_tcp_server") if ROOT else "/usr/bin/ob_tcp_server"
LIB = os.path.join(ROOT, "usr/lib/orderbook-dbengine/liborderbook_shared.so") if ROOT else None
PORT = 21998  # below the ephemeral range

CLIENT = r'''
import sys
import importlib.metadata as m
import orderbook_engine
from orderbook_engine import OrderbookEngine

version, port, data = sys.argv[1], int(sys.argv[2]), sys.argv[3]
assert "/site-packages/" in orderbook_engine.__file__, orderbook_engine.__file__
assert m.version("orderbook-dbengine") == version, m.version("orderbook-dbengine")
assert orderbook_engine.__version__ == version, orderbook_engine.__version__

tcp = OrderbookEngine(host="127.0.0.1", port=port, timeout=30)
tcp.insert("ACC", "REL", "bid", prices=[100_00, 99_00, 98_00], qtys=[5, 6, 7], counts=[1, 2, 3])
tcp.flush()
rows = tcp.query_all("ACC", "REL")
tcp.close()
assert len(rows) == 3, rows

local = OrderbookEngine(data_dir=data)
local.insert("ACC", "LOCAL", "ask", prices=[101_00, 102_00], qtys=[1, 2], counts=[1, 1])
local.flush()
rows = local.query_all("ACC", "LOCAL")
local.close()
assert len(rows) == 2, rows
print("client: version", version, "- TCP: 3 rows back; local mode: 2 rows back")
'''


def fail(msg: str) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    reported = subprocess.run([SERVER, "--version"], capture_output=True, text=True).stdout.strip()
    if reported != f"ob_tcp_server {VERSION}":
        fail(f"{SERVER} --version says {reported!r}, the release is {VERSION}")
    print(f"  ok: {reported}")

    wheels = [f for f in os.listdir(DIST) if f.endswith("-py3-none-any.whl")]
    if wheels != [f"orderbook_dbengine-{VERSION}-py3-none-any.whl"]:
        fail(f"{DIST} holds {wheels}, not the one pure wheel of {VERSION}")

    work = tempfile.mkdtemp(prefix="release_acceptance_")
    server = None
    try:
        venv = os.path.join(work, "venv")
        subprocess.run([sys.executable, "-m", "venv", venv], check=True)
        py = os.path.join(venv, "bin", "python")
        subprocess.run([py, "-m", "pip", "install", "--quiet", os.path.join(DIST, wheels[0])], check=True)
        print(f"  ok: {wheels[0]} installed into a fresh venv")

        server = subprocess.Popen([SERVER, "--port", str(PORT), "--metrics-port", "0",
                                   "--data-dir", os.path.join(work, "server-data"), "--log-level", "WARN"],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + 30
        while True:
            try:
                socket.create_connection(("127.0.0.1", PORT), timeout=1).close()
                break
            except OSError:
                if server.poll() is not None or time.time() > deadline:
                    fail("the installed server did not start")
                time.sleep(0.1)

        env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH", "OB_LIB_PATH")}
        if LIB:
            env["OB_LIB_PATH"] = LIB
        out = subprocess.run([py, "-c", CLIENT, VERSION, str(PORT), os.path.join(work, "local-data")],
                             cwd=work, env=env, capture_output=True, text=True)
        if out.returncode != 0:
            fail(f"the client from the wheel failed:\n{out.stdout}{out.stderr}")
        print(f"  ok: {out.stdout.strip()}")
    finally:
        if server is not None:
            server.terminate()
            try:
                server.wait(timeout=30)
            except subprocess.TimeoutExpired:
                server.kill()
        shutil.rmtree(work, ignore_errors=True)
    print("release accepted." if not ROOT else "release accepted, from an extracted package.")


if __name__ == "__main__":
    main()
