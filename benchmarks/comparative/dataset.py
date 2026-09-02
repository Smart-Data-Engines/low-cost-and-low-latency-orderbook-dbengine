"""One dataset, generated from a seed, loaded into every system.

Requirement 2.2 asks for an identity rather than an assurance: "the same dataset" has to be a claim
a reader can verify, so the manifest carries the seed, the shape and the SHA-256 of the file itself.

Two decisions are load-bearing and neither is stylistic:

`random.Random(seed)` explicitly, never the module-level functions. The global generator carries
process state, so anything else that draws from it - a test, a library, a future workload - shifts
our output without touching our code, and the manifest would then describe a file we did not write.

One CSV for all systems. There is no path where each adapter generates its own rows: that is exactly
where "equivalent workload" stops being equivalent, quietly, and the numbers keep looking fine.
"""
from __future__ import annotations

import csv
import hashlib
import random
from dataclasses import dataclass, asdict
from pathlib import Path

# Integers in the engine's own units, so that loading into any system needs no conversion. A
# conversion on the load path is itself a cost, and it would be charged to whichever system needed
# it - which is a measurement of our adapter, not of that engine.
PRICE_TICK_BASE = 100_000
SIZE_LOT_MAX = 5_000


@dataclass(frozen=True)
class Manifest:
    rows: int
    symbols: int
    levels: int
    seed: int
    sha256: str
    price_tick_base: int
    size_lot_max: int
    start_ns: int
    interval_ns: int

    def as_dict(self) -> dict:
        return asdict(self)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def generate(
    path: Path,
    *,
    rows: int,
    symbols: int = 50,
    levels: int = 20,
    seed: int = 7,
    start_ns: int = 1_700_000_000_000_000_000,
    interval_ns: int = 1_000_000,
) -> Manifest:
    """Write about `rows` orderbook updates to `path` and return their identity.

    **One update is `levels` rows sharing a timestamp**, which is what the wire protocol expresses
    (`MINSERT <symbol> <exchange> <side> <n>`) and what an orderbook feed actually looks like: a
    book changes at an instant, across several price levels at once.

    The first version gave every row its own timestamp and a rotating level, and running the harness
    is what exposed it: the client takes **one** `timestamp_ns` per batch, so a batched load would
    have stored timestamps that the time-range query then selected on. Faster, more faithful, and
    the SQL equivalents in `benchmarks/README.md` become meaningful comparisons rather than
    coincidences.

    The shape is deliberately dull otherwise: symbols cycle, updates advance by a fixed interval,
    prices random-walk around a per-symbol base. Dull is the point — a distribution described by
    parameters can be reproduced, and one described as "realistic" cannot.
    """
    rng = random.Random(seed)
    symbol_names = [f"SYM{i:04d}" for i in range(symbols)]
    mids = {name: PRICE_TICK_BASE + rng.randrange(-5_000, 5_000) for name in symbol_names}

    # Rows are emitted a whole update at a time, so the count lands on a multiple of `levels`. The
    # manifest records what was written, never what was asked for.
    updates = max(1, rows // levels)

    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["ts_ns", "symbol", "exchange", "side", "level", "price_ticks", "size_lots"])
        for update_index in range(updates):
            name = symbol_names[update_index % symbols]
            mids[name] += rng.randrange(-10, 11)
            ts = start_ns + update_index * interval_ns
            side = "bid" if update_index % 2 == 0 else "ask"
            for level in range(levels):
                offset = (level + 1) * (-1 if side == "bid" else 1)
                writer.writerow([
                    ts, name, "EX", side, level,
                    mids[name] + offset, rng.randrange(1, SIZE_LOT_MAX)])
                written += 1

    return Manifest(
        rows=written, symbols=symbols, levels=levels, seed=seed, sha256=_sha256(path),
        price_tick_base=PRICE_TICK_BASE, size_lot_max=SIZE_LOT_MAX,
        start_ns=start_ns, interval_ns=interval_ns)
