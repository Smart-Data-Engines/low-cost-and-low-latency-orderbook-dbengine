#!/usr/bin/env python3
"""
Example usage of orderbook_engine in both local and TCP modes.

Local mode (direct, in-process):
    python example.py local /tmp/ob_data

TCP mode (connect to running ob_tcp_server):
    python example.py tcp localhost 5555
"""

import sys
from orderbook_engine import OrderbookEngine


def demo(engine: OrderbookEngine):
    print(f"Mode: {engine.mode}")
    print(f"Ping: {engine.ping()}")

    # Insert some data
    engine.insert("BTC-USD", "BINANCE", "bid",
                  prices=[65000_00, 64990_00, 64980_00],
                  qtys=[150, 200, 300],
                  counts=[3, 5, 2])

    engine.insert("BTC-USD", "BINANCE", "ask",
                  prices=[65010_00, 65020_00],
                  qtys=[100, 250],
                  counts=[2, 4])

    engine.flush()

    # Query
    rows = engine.query_all("BTC-USD", "BINANCE")
    print(f"\nQuery returned {len(rows)} rows:")
    for row in rows:
        print(f"  {row}")

    if engine.mode == "tcp":
        print(f"\nServer status: {engine.status()}")

    engine.close()
    print("\nDone.")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    mode = sys.argv[1].lower()

    if mode == "local":
        data_dir = sys.argv[2] if len(sys.argv) > 2 else "/tmp/ob_example"
        engine = OrderbookEngine(data_dir)
    elif mode == "tcp":
        host = sys.argv[2] if len(sys.argv) > 2 else "localhost"
        port = int(sys.argv[3]) if len(sys.argv) > 3 else 5555
        engine = OrderbookEngine(host=host, port=port)
    else:
        print(f"Unknown mode: {mode}. Use 'local' or 'tcp'.")
        sys.exit(1)

    demo(engine)


if __name__ == "__main__":
    main()
