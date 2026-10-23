# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). How a release is cut is in
[docs/releasing.md](docs/releasing.md); the record of every item - what was found, what was
measured, on which machine - is [docs/roadmap.md](docs/roadmap.md), whose item numbers are cited
here as `#N`.

## [Unreleased]

### Added

- `ob_flush()` in the C API (#244): every write so far becomes readable, and the engine stays open
  with its live books. The Python client's local `flush()` calls it when the library has it.
- `MREPLACE`, a side replacement (#233): `MREPLACE <symbol> <exchange> <bid|ask> <n_levels>
  [event_time_ns]` and its level lines, as `MINSERT` sends them, and the side becomes exactly those
  levels. The node removes every price the side holds and the block does not name, under the lock
  its write takes, and logs them in the same `DELTA` record as levels of quantity 0 in front of the
  update's own - so a replica, a peer, a replay and history see an ordinary update, and no record
  format changes. `n_levels` may be 0, which empties the side. `capabilities` announces
  `replace_side`; the Python client's `replace_side()` and `BookUpdate(replace=True)`, the C++
  client's `OrderbookClient::replace_side()` and the C API's `ob_replace_side()` send one, and
  both clients refuse before sending to a server that does not announce it.
  `ob_replace_side_removed_levels_total` counts the removals. Under `--auth-acl-file` it is a
  write, and meets an identity's symbols like `MINSERT`.
- Book summaries (#233): beside a symbol's segments, files `book.<time>.v1` hold its book by price
  as of one instant, and a book read starts from the newest one at or before its instant. A seal
  writes one every 65 536 rows of a symbol - more for a deep book - and a row that arrives later
  with a time at or before a summary's removes that summary first. Snapshots, backups, migrations
  and merges carry them. `ob_book_summaries_written_total` and `ob_book_summary_bytes_total` count
  them.

- Permissions per identity, opt-in with `--auth-acl-file` (#31). Each identity of the secret file
  gets a role:
  - `read` runs the queries;
  - `write` runs the writes as well;
  - `admin` runs everything, and alone runs `FAILOVER`, `MIGRATE`, `ADOPT`, `BACKUP`, `SHARD_MAP`
    and `SHARD_INFO`.

  An identity may also be limited to symbols and exchanges, by name or prefix. A refusal is
  `ERR forbidden: ...` and keeps the session. Without the file every identity may run every
  command, as before. `STATUS` of an authenticated session has a `role:` line under `identity:`,
  `admin` without the file. The file must be a regular file that only its owner, the server's
  user or root, may write.
- Rings in shared memory for a client on the node's host (#52, step 2). A session through the Unix
  socket moves with `RINGS`: the answer brings the descriptors of a sealed memfd and two eventfds,
  and from there commands and answers go through two single-producer rings - the same bytes the
  socket carried - with a bell rung only for a side that sleeps. The C++ client moves with
  `ClientConfig::rings` and polls its answer 20 us before it sleeps; `--rings off` and
  `--ring-bytes` (1 MiB a direction) bound it; `ob_ring_sessions_total`, `ob_ring_bytes` and
  `ob_ring_bells_total` count it. Pinned, `boost`, a client that polls and sends back to back: a
  `PING` in 0.47 us on an EPYC 9R45 and 0.93 us on a Graviton, against 2.96 and 4.68 through TCP;
  a one-level `INSERT` in 1.33 and 2.03 us. A command that finds the node asleep, after a pause,
  pays for waking it: 1.99-6.10 us (EPYC) and 3.16-3.23 us (Graviton) a `PING` through rings.

- A Unix socket beside the port, for clients on the node's host (#52, step 1). `--unix-socket <path>`
  serves the same protocol, `AUTH` and `--max-sessions` without TCP, and `--unix-socket-mode` (0660)
  is the file's mode before it takes a connection. It is not encrypted: the file's mode and its
  directory decide who may connect. Each connection is logged with the pid, uid and gid at its other
  end and counted in `ob_unix_connections_total`. A stale socket at the path is replaced, and a live
  one or a path that is not a socket stops the start. The Python client (`unix_socket=`), the C++
  client (`ClientConfig::unix_socket`) and `ob_backup --unix-socket` connect through it, and the
  packaged unit creates `/run/orderbook` for it. With the default profile and a client that polls,
  it took 1.4-1.7 µs off a command on an EPYC 9R45 and 1.9-2.4 µs on a Graviton (`PING` 3.06 to
  1.68 µs and 4.87 to 3.01). A client that blocks keeps less of it on a host whose idle CPUs sleep
  deeply ("Clients on the node's own host" in `docs/operations.md`).
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

- **A book at an instant is read by price** (#233, decision B'). `WHERE AT`, a `SNAPSHOT` and the
  series of the book (`OPEN`, `HIGH`, `LOW`, `CLOSE` and `TWAP` of the bid, the ask, the mid and the
  spread) apply every row up to their instant in time order, each setting its price's quantity and
  a quantity of 0 removing the price - which is how the live book applies an update. A row's
  `level` in the answer is its position in that book. They used to read the latest row of each
  level, which is the live book only for a writer whose every update is the whole side from its
  best level: a writer of the top N levels without removals, or of diffs, got a best price from
  history that the live book did not hold. **History written by a writer of top levels without
  removals reads differently now**: the prices that dropped out of its updates stay in the book,
  as they did in the live one. Such a writer sends removals, or uses `MREPLACE`.
- **A start rebuilds every live book** from the store, by price (#235): `BOOK` after a start is
  `WHERE AT` the last instant, after a clean stop, a crash, a snapshot bootstrap or a store adopted.
  A start used to replay the WAL's tail into the live book and nothing else, so after a clean stop
  `BOOK` refused every symbol and after a crash held only what came after the last checkpoint.
  The embedded engine of the C API rebuilds its live books when it opens, too: a side replacement
  after a reopen - which the Python client's local `flush()` is - removed nothing, because it found
  the side empty.
- A level count `MINSERT` cannot read - 0, past 1000, a word - and level lines that never came are
  refused by name, where they were answered `unknown command` (#222).
- A pass of the client loop that nothing woke no longer reads the subscription hub's eventfd or
  takes its lock (#52): the polling loops of `boost` were paying both on every pass, and a round trip
  through TCP or the Unix socket is 0.08-0.15 us shorter for it on an EPYC 9R45 and 0.17-0.34 us on
  a Graviton.
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
- `ob_flush()` and `ob_engine_destroy()` take the embedded engine's segments and WAL to the device,
  with one `syncfs()`, before the checkpoint that claims them (#245, the server's rule from #160).
  Nothing had synced an embedded segment.
  - A flush after a write waits for the device: 2.79-3.03 ms median after one update of 100 levels,
    where it took 0.11-0.19 ms (i3-7100U, ext4 over LUKS on NVMe). A flush with nothing new does
    not wait.
  - `ob_flush()` answers `OB_C_ERR_IO` when the sync fails. The rows are written and readable, and
    from then on no checkpoint claims anything until the engine is created again.
- Between releases the version is the next one as a development version: `0.1.1.dev0` now, in
  `ob_tcp_server --version`, `STATUS` and the wheel, and `0.1.1~dev0` in the packages, which pip,
  dpkg and rpm all order after 0.1.0 and before 0.1.1. Everything built from `master` after v0.1.0
  had called itself 0.1.0 (#234).

### Fixed

- The embedded engine of the C API, which the Python client's local mode loads, replays its WAL
  when it opens (#245). A process that ended without `ob_flush()` or `ob_engine_destroy()` lost
  every write since the last of them, because an open read no record of the WAL.
  - A flush and a close now append a checkpoint, and an open applies the records after the last one
    again, into the live books and the segments, and flushes them.
  - A record a segment already holds is skipped by the WAL position the segment records. A segment
    of an older build records none, and is judged by the time of its last row.
  - WAL files wholly before the last checkpoint are removed; nothing removed them before.
  - The first open of a directory an older build wrote reads its whole WAL, once: such a WAL holds
    no checkpoint.
  - An open after a process ended at a million unflushed rows of one symbol took 662-708 ms, and
    found every update, where before it found none (i3-7100U, Release, ext4 over LUKS on NVMe).
- A local flush no longer reopens the embedded engine (#244). Since #235 an open composes every book
  from history, so a flush with nothing new took 94-146 ms at a million rows of one symbol, where it
  took 0.3-1.1 ms before; with `ob_flush()` it takes 0.02 ms or less (i3-7100U, Release).
- The C++ client read an answer with a header and no rows - a query that matched nothing, `BOOK` of
  an empty book - as an error, `missing header`; it is an empty answer now.
- A device slower than the writes no longer refuses them before admission knows its rate (#190,
  step 9).
  - **Before:** the writers filled the pending queue's million rows, about 34 MB of WAL, and the sync
    after them was their wait. On an m9g.xlarge with the volume's writes capped at 5 MB/s, writes
    were refused in 7 rounds of 8.
  - **Now:** under `--fsync-policy interval` a writer also waits while the WAL written since the
    last completed sync is at a budget, and asks for the flush at half of it. The syncs set the
    budget to about a second of the device's writing.
  - `ob_wal_sync_budget_bytes` is the budget, `ob_writer_wal_budget_waits_total` counts the waits,
    and `OrderbookWritersWaitingForTheWal` alerts when they go on for ten minutes.
  - While admission is in force, the rows a tick drained pay for the stores it seals by age, one
    store for each 8 192 rows.
  - **Measured:** no round refused at 5 MB/s, and the worst batch was 2-8 times shorter at every
    limit from 5 to 30 MB/s. With no limit nothing was lost.
  - **The cost:** at 7 and 10 MB/s it takes 1.7% and 0.8% of the levels a second. That is the
    filesystem journal of the more frequent syncs.
- Code scanning has no open alert in our own code (#241). All fifteen were fixed in code and none
  was dismissed. The engine owns its read-only flag where it held a raw pointer into the server's
  storage. The replication manager gets its engine at construction instead of through a setter that
  had to lock against its own loop. If a port will not bind, the server now stops its metrics endpoint
  before the engine closes. Before, the engine went first while the endpoint could still read the
  engine's registry. The rest are an unreachable static function, an always-true comparison with dead
  code behind it, a hidden declaration, nine unused locals and a `switch` whose longest cases are
  functions now.
- The tree builds with clang 21 (#240). It failed on a member nothing read,
  `AntiEntropyManager::registry_`, which is gone, and in googletest's own sources, which the
  warnings-as-errors meant for our code no longer reach. The test that looks for a member written and
  never read keyed members by name, so a live one in another class hid a dead one. It keys them by
  the header that declares them now, and found a second, `MultiMasterManager::hlc_`, gone too.
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

- The embedded engine syncs its WAL at a flush and a close, not at each write: a power cut or a
  kernel crash loses what was written since the last of them. Nothing in the C API asks for a sync
  of each write, as the server's `--fsync-policy every` does.
- A series of the book reads every row of its range by price: 7-12 times what it cost before #233
  for a writer of whole sides (#243).

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
