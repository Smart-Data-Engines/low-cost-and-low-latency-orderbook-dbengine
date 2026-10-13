# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). How a release is cut is in
[docs/releasing.md](docs/releasing.md); the record of every item - what was found, what was
measured, on which machine - is [docs/roadmap.md](docs/roadmap.md), whose item numbers are cited
here as `#N`.

## [Unreleased]

### Added

- A certificate is replaced without a restart (#238). `SIGHUP` - `systemctl reload`, through the
  unit's new `ExecReload` - reads the TLS certificate, key and CA again from the paths the node
  started with, for every surface at once. New connections get the new certificate and open ones
  finish on theirs. Files the start would refuse are refused, and then every surface keeps the
  certificate it had and the node goes on. `ob_tls_reloads_total`, `ob_tls_reload_failures_total`
  and `ob_tls_certificate_not_after_seconds` follow it. Two alerts come with them,
  `OrderbookCertificateExpiring` two weeks before the end and `OrderbookTlsReloadFailed`, and a
  dashboard row ("Replacing a certificate" in `docs/operations.md`).
- The decoded columns a node holds between queries (#220) are held in an arena of 2 MiB regions
  (#223): a column evicted gives its memory back to the next, and the regions of the larger columns
  are advised for transparent huge pages. A query's first read of a large segment measured 0.38 to
  0.92 of the time it took, its narrow window faulting 40 pages where it faulted 1 258 (x86), and no
  query slower. `ob_decoded_columns_arena_bytes`, `..._free_bytes`, `..._regions` and
  `..._fallbacks_total` say what it holds.
- Books on huge pages (#51). Each symbol's book is made in a 2 MiB region the kernel is asked to
  back with transparent huge pages, 43 books to a region. On an EPYC and a Graviton host a write to
  one of 4 000 books measured 16-22% faster and a write through the engine 1.5-4.7% faster, with no
  difference at 64 books or fewer. `--book-huge-pages off` makes books on the heap as before, and `ob_book_arena_regions`
  counts the regions. One first write to a new symbol in 43 pays for zeroing a 2 MiB page
  (`docs/operations.md`, "Huge pages for the books").
- Rolling windows: `GROUP BY TIME_BUCKET(1m) ROLLING 15m` answers every bucket with the aggregates
  of the fifteen minutes that end with it - the rows' and the series' of the book alike, each what a
  bucket as wide as the window would answer (#44, step 3). The first windows reach back before the
  range, the ceiling counts the buckets they reach back to, and the window slides in constant time a
  bucket. This completes #44.
- A documentation site, built from `docs/` by MkDocs with `--strict` in CI, and not yet published
  (#40). It brings a five-minute tutorial from the release packages to a query, and a wire protocol
  reference. Every exchange the two show is replayed against the server in the integration battery.
- IPv6, on every listener and every connection (#236). A listener with no bind flag takes both
  families; `--bind`, `--replication-bind`, `--mm-bind` and `--metrics-bind` take an IPv6 address;
  a replica, a mesh peer and both clients dial a name, an IPv4 or an IPv6 address, every address of
  a name in turn; an IPv6 host is written in brackets wherever a port follows it.
- `examples/binance-live/`: the order books of ten Binance pairs written into a node as they arrive,
  read back as minute bars and as the top of each book, and drawn by a Grafana dashboard of the
  market as the node holds it and of the node itself - every process native (#43).
  `scripts/check_dashboards.py` holds an example's dashboard to the engine's metrics and to the ones
  its exporter writes.

### Changed

- A seal no longer writes a column as runs a value or two long to save a few percent (#223): a runs
  candidate is weighed as larger by half its runs per value, and taken only where it is that much
  smaller. binance-diff's columns, as sealed, decode in 5.55-5.57 ns a row where they took 7.09-7.15
  on the EPYC host, and in 5.61-5.64 where 7.04-7.13 on ARM, for 1.1% more bytes; binance-top20's,
  whose runs are long, as before. A merge chooses as it did.
- A column of short runs decodes faster (#223): where its runs average two to eight values, a run of
  up to eight is written whole, with nothing branching on its length, and a 16-bit column's in one
  vector. On a recording of an order book's changes, its levels, sequence numbers and timestamps
  decoded 21-40% faster on an EPYC and a Graviton host, its seven columns a fifth faster, and the first
  query after a load took 0.89-0.92 of the time.
- `SIGHUP` does not stop the server (#238). Nothing handled it, so its default action ended the
  process. A node without TLS now logs that there is nothing to reload and goes on, and `SIGTERM` is
  the signal that stops a node.
- A narrow column of three, five, six or seven bytes a value decodes up to six times faster: each
  value is put together in a register, where a `memcpy` into a zeroed 64-bit value went through a
  stack slot no load could be forwarded from (#223). Decoding binance-top20's seven columns went from
  6.95 to 2.42 ns a row on the EPYC host and from 6.83 to 3.10 on ARM.
- A port with no bind flag listens on IPv6 as well: `::`, both families on one socket, where it was
  `0.0.0.0` (#236). On a host whose firewall covers IPv4 alone, such a port is now reachable over
  IPv6. `--bind 0.0.0.0`, and the same in `--replication-bind`, `--mm-bind` and `--metrics-bind`,
  keeps a port on IPv4 as it was ("Listeners after v0.1.0" in `docs/upgrading.md`).
- A replica's connection to its primary gives up an address after 5 s, where a blocking `connect()`
  waited out the kernel's SYN retries - minutes - before the replica's own retry could begin (#236).
- Between releases the version is the next one as a development version: `0.1.1.dev0` now, in
  `ob_tcp_server --version`, `STATUS` and the wheel, and `0.1.1~dev0` in the packages, which pip,
  dpkg and rpm all order after 0.1.0 and before 0.1.1. Everything built from `master` after v0.1.0
  had called itself 0.1.0 (#234).

### Fixed

- A client resuming a subscription from a book, as documented, could lose updates (#239). The
  documentation said to take the book, then subscribe, and drop the pushes at or below the book's
  sequence number. That order loses what is written between the two: with a writer running, 48 of 50
  trials of a probe lost updates. And the book's number was read after its levels, so it could name
  an update a side did not hold yet, whose push the client then dropped as already in the book. The
  number and timestamp are now read before the levels, as one update's pair, so every update up to
  the number is in the book. The documented order is subscribe, then the book, and a subscription
  is in force for every write after `SUBSCRIBE` answers. The aggregates' `timestamp_ns` and
  `sequence_number` come from the same pair. In the Python client a pushed row's `side` is `"bid"`
  or `"ask"`, as in every other row; it was the wire's 0 or 1.
- The man page said nothing on the wire is encrypted, which stopped being true with #30. It names the
  three TLS surfaces now, and the signals the server takes (#238).
- The Python client finds and cuts an answer in bytes, and decodes it after. It cut by the length
  of the decoded text encoded again, so a byte that is not UTF-8 took two bytes of the next answer
  of a pipelined batch with it. Bytes that begin no answer are refused at once by name, where they
  were read until the timeout (#237).

### Known issues

- The live book applies a write by price, and history keys it by its position in the update, so
  the two can name different best prices for the same writes (#233). The example writes the one
  encoding on which they agree: a side's levels best first, then the prices that left it with a
  quantity of 0.
- The live book does not come back with the node: after a clean stop `BOOK` refuses the symbol,
  and after a crash it holds only what was written after the last checkpoint, while history holds
  every row (#235).

## [0.1.0] - 2026-10-12

The first release. What it contains, by area:

### Storage and durability

- A write-ahead log with CRC32C on every record and a configurable fsync policy (`--fsync-policy`),
  rotation, and recovery that replays what the last checkpoint does not cover.
- An in-memory book per symbol in struct-of-arrays buffers read under a seqlock.
- Columnar segments on disk: time-partitioned, sealed without the engine's lock and merged in the
  background into segments of up to 262 144 rows (#165). In segment format 3 (#219) a segment's seven
  columns are one file, each column checksummed and in whichever of its candidate encodings is
  smallest - less the column's minimum or the value before it, divided by what the values share, in
  Simple8b, runs, fixed-width blocks or the bytes each value needs, under LZ4 at a seal and ZSTD at
  a merge where that pays. Segments written in format 2 are read as they are, and
  `--segment-format 2` keeps writing them.
- TTL retention, and segments that survive a power cut (#160).
- On a device slower than the ingest, writes admitted at the rate it takes rather than refused when
  the pending queue stays full (`--write-admission`, #190), the seal's sync in the background, and
  seals that bring what waits in memory under its budget a few large stores at a time (#205).
- Backup into a server's `--backup-dir` (`BACKUP`, `ob_backup` for cron) and restore into an empty
  directory with `ob_restore`, which checks a backup whole before it writes anything (#34). No
  restore to a moment between two backups yet.

### Queries

- A SQL-like language: conditions on time and price with `=`, `<`, `<=`, `>`, `>=` and `BETWEEN`
  that narrow each other (#199), `LIMIT`, `SELECT` lists that read only the column files they need
  (#139), conditions on the side and the level (#200), snapshots of the book at a time (`AT`) that
  read only the segments able to change it (#47, step 1), and aggregates - VWAP, spread, mid-price,
  imbalance, depth - each function of one side naming it (`VWAP(bid)`, #200), carrying their scale
  and telling an empty aggregate from a zero.
- Aggregates over time buckets of the stored rows - `GROUP BY TIME_BUCKET(1m)` with `COUNT`, `FIRST`,
  `LAST`, `MIN`, `MAX`, `SUM`, `AVG` and `VWAP`, the conditions narrowing the rows they read, read by
  `query_buckets()` in the Python and C++ clients (#44, step 1) - and series of the book through
  them: `OPEN`, `HIGH`, `LOW`, `CLOSE` and the time-weighted `TWAP` of the best bid, the best ask, the
  mid and the spread, every bucket of the range answered (#44, step 2).
- `BOOK` for the live book over the wire, and `SUBSCRIBE` for rows pushed as they are written.
- Conditions that narrow the read as well as the answer (#47 step 2):
  - a segment records the range of its prices in `meta.json`;
  - a segment whose range or level set proves no row of it meets a query's price, side and level
    conditions is not opened;
  - a row that fails them is not built;
  - `LIMIT n` ends the read at its `n`th row.

  `SELECT`, time buckets and the series of the book read through it, and a filter's columns are
  checked a chunk at a time, in loops the compiler vectorizes (#225).
- Format 3's decoded columns held between queries, within `--decoded-cache-mb` (256 MiB by
  default; 0 holds none): a query reading a column held reads neither its file nor its checksum and
  decodes nothing. A segment read once is the first to make room, and a merge, retention, a drop or a
  snapshot install takes a segment's columns with it (#220).

### Replication and availability

- WAL streaming replication with snapshot bootstrap of replicas, lag in the bytes a replica has yet
  to acknowledge, and a new WAL lineage after a snapshot is installed (#197). A replica rejoining
  under writes takes its snapshot and reaches the stream - 0.30 - 0.39 s on two hosts - where before
  every attempt was abandoned and the next failover lost the whole round (#214). Replication is
  asynchronous: what an unplanned failover can lose is in `docs/operations.md`, "What a failover
  keeps".
- A catch-up under writes reads what is written while it runs from the WAL, and ends where the writes
  are (#231). It held those records in memory, against the 16 MB ceiling of a replica that does not
  drain, so under about 15 MB of WAL a second every catch-up longer than a second ended in a silent
  disconnect mid-record, and the replica came back further behind each time: measured on two hosts,
  it never finished. A catch-up that falls `4 x --wal-rotate-bytes` further behind the end of the WAL
  than it began is ended, so a replica slower than the writes cannot hold the primary's disk (counted
  by `ob_replica_catchups_abandoned_total`). Every disconnect is a log line on both sides, and a replica
  no longer logs two INFO lines for every record it applies.
- A replica whose position the primary has freed takes its snapshot under writes (#232). Live records
  reached it between `ERR WAL_TRUNCATED` and `SNAPSHOT_BEGIN`, and it abandoned every attempt and asked
  again five seconds later, for ever. A primary sends a refused replica nothing until it asks again,
  and a replica skips a live record before `SNAPSHOT_BEGIN`, for a primary from before the fix.
- A replica that is joining does not stand for election (#215). From giving up its store to follow a
  stream - a discard or a snapshot - until its primary says the catch-up ended (`CAUGHT_UP`,
  announced as `STREAM <id> caught_up`), it holds part of the stream, and electing it made the node
  returning after it discard the writes it had not received. The record is `repl_joining.txt` in the
  data directory, written before anything is replaced; a primary lost in that window leaves the
  cluster without one until a node that holds the stream returns, and removing the record lets the
  joining node stand anyway. `STATUS` says `joining:`, `ob_replica_joining` and
  `ob_failover_abstaining` say it to Prometheus, and the alert `OrderbookNoPrimaryWhileJoining`
  fires when an abstention lasts a minute.
- Automatic failover through etcd with epoch fencing, and graceful handover to a named replica;
  a primary that gives the role up replicates from whoever takes it (#201). A handover loses no
  acknowledged write and takes a monitor tick rather than a lease TTL: the outgoing primary closes
  writes and keeps streaming, and its target stands once it holds the stream (#204).
- Multi-master replication: per-origin numbering, version vectors sent in parts, catch-up, lag in
  records, conflict resolution by hybrid logical clocks and last-writer-wins. A peer is dialled at
  the address it last registered, and never at a node's own (#216). The procedure for a mesh across
  hosts in `docs/operations.md` has been carried out on two hosts and corrected where it failed, and
  a node says at its start when its advertised address is loopback and its coordinator is not (#217).
- Sharding by symbol on a consistent-hash ring, and moving a symbol between shards with its rows
  (#196).
- Rolling upgrades: the previous version's server and this one, side by side, in both directions
  (#56, `docs/upgrading.md`).

### Operations and security

- Challenge-response authentication of clients, replication links and mesh peers; TLS 1.3 on all
  three, mutual on the node links.
- Prometheus metrics, a Grafana dashboard and alert rules tested with `promtool`.
- A configuration file, a systemd unit and a man page; `.deb`, `.rpm` and `.tar.gz` packages: every
  entry root's and none writable beyond its owner (#210), and the user the unit runs as created by
  the `.deb` and the RPM (#211). They run wherever glibc is 2.34 or newer - built on Ubuntu 22.04
  with gcc-12 and libstdc++ linked in, and installed with `apt` on Ubuntu 22.04 and 26.04 and with
  `dnf` on Amazon Linux 2023 (#212). Their checks pass on Ubuntu 26.04 as well (#209).
- The address each listener binds: `--bind`, `--replication-bind`, `--mm-bind`, `--metrics-bind` (#203).
- Builds without a warning with GCC 13 to 15 and clang 18, on ARM64 and on x86-64 at any of its
  levels - including x86-64-v3, the default of GCC 15 on Ubuntu 26.04 - and with the AVX2 and
  AVX-512 paths of the aggregation engine, which CI builds and tests (#207). GCC 12, the oldest
  compiler it takes, builds it too - CI builds the packages with it - with two of its warnings about
  libstdc++'s own code left as warnings (#221); an older one, or a clang older than 15, is refused at
  configure time, saying what to use (#213).

### Clients

- The wire protocol names what it cannot read: a token a command has no place for (#107), and an
  argument it could not read or one missing - the field, the token and what the command takes
  (#222). `unknown command` is for a word that is not a command, and for an `AUTH` line of the
  wrong shape.
- A C++ client and a C API. Over TLS the client's calls report a server that has gone as an error;
  they no longer raise the SIGPIPE that ended the application around them (#208). The C++ client's
  `query_named()` reads any row answer by the names in its header (#230).
- A Python client over TCP, with a pool mode, a sharded mode and LZ4 session compression, and a
  local mode over the C API library the packages install. `query_rows()` reads any row answer by
  the names in its header, as tuples of ints, in under half the time `query()` takes (#229).
