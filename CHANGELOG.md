# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). How a release is cut is in
[docs/releasing.md](docs/releasing.md); the record of every item - what was found, what was
measured, on which machine - is [docs/roadmap.md](docs/roadmap.md), whose item numbers are cited
here as `#N`.

## [Unreleased]

The first release. What it contains, by area:

### Storage and durability

- A write-ahead log with CRC32C on every record and a configurable fsync policy (`--fsync-policy`),
  rotation, and recovery that replays what the last checkpoint does not cover.
- An in-memory book per symbol in struct-of-arrays buffers read under a seqlock.
- Columnar segments on disk: time-partitioned, one file per column, prices delta and zigzag encoded,
  quantities and sequence numbers in Simple8b; sealed without the engine's lock and merged in the
  background into segments of up to 262 144 rows (#165).
- TTL retention, and segments that survive a power cut (#160).
- Backup into a server's `--backup-dir` (`BACKUP`, `ob_backup` for cron) and restore into an empty
  directory with `ob_restore`, which checks a backup whole before it writes anything (#34). No
  restore to a moment between two backups yet.

### Queries

- A SQL-like language: conditions on time and price with `=`, `<`, `<=`, `>`, `>=` and `BETWEEN`
  that narrow each other (#199), `LIMIT`, `SELECT` lists that read only the column files they need
  (#139), conditions on the side and the level (#200), snapshots of the book at a time (`AT`), and
  aggregates - VWAP, spread, mid-price, imbalance, depth - each function of one side naming it
  (`VWAP(bid)`, #200), carrying their scale and telling an empty aggregate from a zero.
- `BOOK` for the live book over the wire, and `SUBSCRIBE` for rows pushed as they are written.

### Replication and availability

- WAL streaming replication with snapshot bootstrap of replicas, lag in the bytes a replica has yet
  to acknowledge, and a new WAL lineage after a snapshot is installed (#197).
- Automatic failover through etcd with epoch fencing, and graceful handover to a named replica;
  a primary that gives the role up replicates from whoever takes it (#201). A handover loses no
  acknowledged write and takes a monitor tick rather than a lease TTL: the outgoing primary closes
  writes and keeps streaming, and its target stands once it holds the stream (#204).
- Multi-master replication: per-origin numbering, version vectors sent in parts, catch-up, lag in
  records, conflict resolution by hybrid logical clocks and last-writer-wins.
- Sharding by symbol on a consistent-hash ring, and moving a symbol between shards with its rows
  (#196).
- Rolling upgrades: the previous version's server and this one, side by side, in both directions
  (#56, `docs/upgrading.md`).

### Operations and security

- Challenge-response authentication of clients, replication links and mesh peers; TLS 1.3 on all
  three, mutual on the node links.
- Prometheus metrics, a Grafana dashboard and alert rules tested with `promtool`.
- A configuration file, a systemd unit and a man page; `.deb`, `.rpm` and `.tar.gz` packages.
- The address each listener binds: `--bind`, `--replication-bind`, `--mm-bind`, `--metrics-bind` (#203).

### Clients

- A C++ client and a C API.
- A Python client over TCP, with a pool mode, a sharded mode and LZ4 session compression, and a
  local mode over the C API library the packages install.
