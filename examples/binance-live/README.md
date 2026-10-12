# Binance, live, into orderbook-dbengine

The order books of ten Binance pairs, written into an engine node as they arrive, every 100 ms.
Then read back from the node three ways:
- minute bars of the mid and the spread, from history;
- the top of each book, from the live book;
- a Grafana dashboard of the market and of the node itself.

Everything runs natively: the node, the three Python programs, Prometheus and Grafana from their
tarballs. No containers, no root.

```
Binance  --depth20@100ms-->  ingest.py  --insert_batch-->  node :9090  (metrics :9100)
                                                             |
                    bars.py  <---  GROUP BY TIME_BUCKET(1m), BOOK
                exporter.py  <---  BOOK, every scrape      :9101
                                       |
                          Prometheus :9091  <---  :9100, :9101
                             Grafana :3000  <---  Prometheus
```

| file | what it does |
|---|---|
| `ingest.py` | One websocket with `<symbol>@depth20@100ms` for each pair. Each message is one `insert_batch` of the sides that changed, stamped with the time it arrived. Every ten seconds it logs a line: messages and levels a second, batch latency at p50, p99 and max, refusals. |
| `bars.py` | Minute bars of one pair from the node: `OPEN`, `HIGH`, `LOW`, `CLOSE` of the mid, the time-weighted spread and the rows of the minute, plus the best bid and ask now. |
| `exporter.py` | The top of every pair's live book in the node, as Prometheus gauges, read at each scrape. Given the node's data directory, it also reports the bytes the node holds on disk. |
| `market.py` | What the three share: each pair's price and quantity steps, how a message becomes the engine's updates, and a window's numbers. |
| `prometheus.yml`, `grafana/` | Prometheus scraping the node and the exporter; Grafana with that Prometheus and the dashboard, provisioned. |

## How a book is written

The engine stores integers, so a price is a count of the pair's price step (`tickSize` from Binance's
public `exchangeInfo`) and a quantity a count of its quantity step (`stepSize`). Both are computed
from the decimal text with `Decimal`, never a float. A value that is not a whole number of steps is
refused with the pair named.

A depth20 message is the best twenty levels of each side. A side is written as those levels, best
first. They are followed by every price that was in the side the last time and is not now, with a
quantity of 0. The engine reads one write two ways (roadmap #233):
- the live book applies it by price, so a price that is never sent again stays until a quantity
  of 0 removes it;
- history keys each row by its position in the update.

The removals keep the live book equal to the market, and putting them after the twenty levels keeps
history's positions 0 to 19 the book's levels. A side that has not changed since it was last written
is not written again.

## Run it

**The node.** The [release's](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/releases)
package installs `/usr/bin/ob_tcp_server` (see [docs/operations.md](../../docs/operations.md)), or
build it (the top-level README). Run it on a data directory of your own, with metrics on loopback:

```bash
mkdir -p ~/binance-live/data
ob_tcp_server --port 9090 --data-dir ~/binance-live/data --metrics-port 9100 --metrics-bind 127.0.0.1
```

**The programs**, from this directory:

```bash
python3 -m venv venv && venv/bin/pip install -r requirements.txt   # orderbook-dbengine, websockets
venv/bin/python ingest.py                     # ten pairs; --symbols btcusdt,ethusdt to choose
venv/bin/python bars.py --symbol BTCUSDT --minutes 15
```

**The dashboard.** Download Prometheus and Grafana OSS for Linux from their own sites
([prometheus.io/download](https://prometheus.io/download/), [grafana.com/grafana/download](https://grafana.com/grafana/download?edition=oss),
the standalone binaries) and unpack them anywhere. Then, from this directory:

```bash
venv/bin/python exporter.py --data-dir ~/binance-live/data &
<prometheus dir>/prometheus --config.file=prometheus.yml --web.listen-address=127.0.0.1:9091 \
    --storage.tsdb.path=$HOME/binance-live/prom &
(cd <grafana dir> && EXAMPLE_DIR=$OLDPWD GF_PATHS_PROVISIONING=$OLDPWD/grafana/provisioning \
    GF_SERVER_HTTP_ADDR=127.0.0.1 bin/grafana server --homepath "$PWD") &
```

Open http://127.0.0.1:3000 (admin, admin on the first login) and the dashboard *Binance live into
orderbook-dbengine*. Prometheus listens on 9091 because the node has 9090. Grafana's `--homepath` is
absolute because Grafana 13 took `.` for the packaged `/usr/share/grafana`. `EXAMPLE_DIR` is how the
provisioning finds the dashboard wherever this directory is.

## What one hour of it measured

12 October 2026, 20:00 - 21:00 UTC. Setup:
- an m8a.xlarge: 4 vCPU, x86-64, Ubuntu 26.04;
- the server of the v0.1.0 package and the release's wheel;
- the ten default pairs;
- Prometheus 2.54.1 and Grafana 13.2.3 from their tarballs, all on the same host.

| | |
|---|---|
| messages | 204 824: 57 a second, 42 - 87 per ten-second window |
| levels written | 4 147 196: 1 152 a second, in 204 216 updates. 205 432 sides were unchanged and not written again |
| refused, superseded, failed, reconnected | 0, 0, 0, 0 |
| `insert_batch`, one round trip per message over loopback | p50 0.056 ms and p99 0.16 ms (medians of the windows); the worst batch of the hour 2.7 ms |
| the node's CPU | 8.4 s in the hour, 0.23% of one core |
| the node's resident memory | 50 MB |
| history on disk, format 3 segments | 3.4 MB: 0.8 bytes a level |
| write-ahead log | 124 MB: 30 bytes a level. It is freed a file at a time once checkpoints have passed it, and a file is 512 MiB by default (`--wal-rotate-bytes`) |
| Grafana | every one of the dashboard's 15 queries returns series, after six minutes and over the hour |

The stream is light for the engine. What the hour shows is what keeping it costs: about 335 KB of
history an hour per pair, and a fraction of one core.

## Tests

- `tests/integration/test_example_binance_live.py` writes three pairs' three minutes of messages
  through `market.py`, made from a seed in the shape of depth20 and without the network. It holds:
  - the bars `bars.py` reads, to the last digit of the engine's scale, against the bars computed from
    the messages;
  - the book `WHERE AT` every message's instant, and the live book, against the messages, level by
    level;
  - the exporter against the best prices, and with no node;
  - the refusal of a value off the step.
- `scripts/check_dashboards.py` holds the dashboard to the metrics the engine registers and the
  ones `exporter.py` writes.
