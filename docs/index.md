# Orderbook DB Engine

A database engine for Level 2 order book data. It is written in C++20 for one workload: updates
to the levels of a book as they arrive, and reads of the book as it was at any instant. It runs
natively on the host, with no container in between, and is open source under Apache-2.0.

What it costs and how it compares with ClickHouse and TimescaleDB is measured, with the machine,
the date and the runs named, in the
[README](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine#how-it-compares).
This site does not repeat the figures, so that they are kept in one place.

## Start here

- [Tutorial](tutorial.md): from a release to a query in five minutes.
- [Architecture](architecture.md): the components, and the path of a write and of a read through them.
- [Wire protocol](protocol.md): what a client sends and how each answer is framed.

## Reference

- [Query language](query-language.md): `SELECT`, aggregations, time buckets, snapshots of the book,
  subscriptions.
- [Python client](python.md) and [C API](c-api.md).
- [Server and flags](cli.md): every flag, the configuration file, pipelining, profiles,
  durability.
- [Storage](storage.md): the WAL, the column store's files and their compression.

## Running it

- [Operations](operations.md): installing, clusters on one host and across hosts, sharding,
  backups, the dashboard and alerts, security, and what to do when something is wrong.
- [Upgrading](upgrading.md): one node at a time, without stopping the cluster.
- [Releasing](releasing.md): what a release contains and how it is verified.

## The project

- [Roadmap](roadmap.md): every item, open and closed, with what was found and measured.
- [Repository security](github-security.md): how this public repository keeps bad code and leaked
  credentials out.
- [Changelog](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/blob/master/CHANGELOG.md)
  and [source](https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine).
