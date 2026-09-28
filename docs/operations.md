# Running a node

The engine runs natively on the host. There is no containerised deployment path and there will not
be one: a container layer between the engine and the hardware defeats the point of an engine tuned
for specific hardware.

## Install

```bash
sudo dpkg -i orderbook-dbengine_0.1.0_amd64.deb
# or, on anything that is not dpkg:
sudo tar xzf orderbook-dbengine-0.1.0-Linux.tar.gz -C / --strip-components=1
```

The package installs a binary at `/usr/bin/ob_tcp_server`, a configuration file at
`/etc/orderbook/ob.conf`, a systemd unit, a man page and the headers. It creates the `orderbook`
system user and **does not enable or start the service**: a database that begins serving on a port
the moment it is unpacked is a surprise, and the shipped configuration is a single node nobody has
pointed at anything yet.

```bash
sudoedit /etc/orderbook/ob.conf
ob_tcp_server --config /etc/orderbook/ob.conf --print-config   # what it resolved, and from where
sudo systemctl enable --now ob_tcp_server
```

`/etc/orderbook/ob.conf` is marked as a configuration file, so a package upgrade will not overwrite
your edits.

## A cluster on one host, in one command

For evaluation, or for a machine that is going to hold the whole cluster:

```bash
./scripts/bootstrap-cluster.sh          # three multi-master nodes plus etcd, native processes
./scripts/bootstrap-cluster.sh stop
```

It writes a configuration file per node — the same shape as `/etc/orderbook/ob.conf`, so what you
end up editing on a real host is what the script writes here — waits until **every node sees both
peers as connected**, and then prints how to reach them. Waiting for that rather than for the ports
to open is deliberate: a node that is merely listening can accept a write and have nobody to send it
to.

`BASE_PORT`, `NODES`, `STATE_DIR` and `OB_SERVER_BINARY` override the defaults. Everything binds to
127.0.0.1, and the script leaves it that way: it sets neither `--cluster-secret-file` nor
`--tls-multi-master`, so the mesh it builds is unauthenticated and unencrypted. Both are available
(below), and a mesh that crosses hosts wants them; a bootstrap script that turned them on would be
generating a secret and a certificate authority for a local demo.

This is not `scripts/mm_harness.py`, which kills nodes, blocks links and counts rows to reproduce
specific defects. The bootstrap script's job ends when the mesh is up.

## A cluster across hosts

**There is deliberately no script for this**, and the reason is worth stating: it could not be
verified on the machine this was written on — `sshd` is installed but inactive and no key is set up,
and standing one up would be a change to a developer's system rather than a test. A deployment
script nobody has run is worse than a procedure someone has read. Roadmap #33 records what verifying
it would take.

The procedure, per host, with three hosts as the example:

```bash
# 1. On every host: install the package and an etcd the three can reach.
sudo dpkg -i orderbook-dbengine_0.1.0_amd64.deb

# 2. On every host: edit /etc/orderbook/ob.conf. Only four lines differ between them.
#
#    node-id               = node-1          # node-2, node-3
#    mm-node-id            = 1               # 2, 3
#    mm-replication-port   = 9092            # the same on each host is fine; they differ by address
#    coordinator-endpoints = http://10.0.0.1:2379,http://10.0.0.2:2379,http://10.0.0.3:2379
#    multi-master          = true

# 3. Confirm what each node resolved before starting it. This is the step that catches a typo in a
#    file you edited on three machines by hand.
ob_tcp_server --config /etc/orderbook/ob.conf --print-config

# 4. Start them.
sudo systemctl enable --now ob_tcp_server

# 5. Confirm the mesh from any node, rather than trusting that three services started.
printf 'MM_PEERS\nQUIT\n' | nc 10.0.0.1 9090
```

Two things that bite here and are not obvious:

- **`mm-replication-port` must be reachable between hosts**, and it is a different port from the one
  clients use. A firewall that allows 9090 and not 9092 gives you three nodes that each accept
  writes and never exchange one — and each looks healthy on its own.
- **etcd must be reachable from every node, not just from one.** Peer discovery is
  `etcd → PeerRegistry::start_watch → handle_topology_change → connect_to_peer → send_handshake`,
  and a node that cannot read etcd stays alone without saying anything louder than a log line.

## Sharding by symbol

Each shard is a node - or a failover group of them - started with the same `--shard-id`, and every
shard of a cluster points at the same etcd (#175):

```bash
ob_tcp_server --shard-id s0 --node-id s0-a --port 9090 --replication-port 9091 \
    --coordinator-endpoints http://10.0.0.1:2379 --advertise-host 10.0.0.11
```

`--advertise-host` is the host clients and the shard's replicas reach the node by: what it writes into
the shard map and into its group's leader key. It is `127.0.0.1` by default, which is right on one
machine only - and was the only choice before #195, for failover as for the map.

What a shard does with etcd, and what each line of its log means:

- **It joins the map** (`<prefix>shard_map`, `/ob/shard_map` by default) by a compare-and-swap on the
  key's revision, so two shards starting together both end up in it:
  `Shard s0 joined the shard map at 10.0.0.11:9090 (version 3, 2 shard(s))`. The map lists shards and
  the symbols assigned to one explicitly; every other symbol goes to the shard the map's hash ring
  gives it, and a shard refuses a write of another's with `ERR NOT_OWNER <symbol>.<exchange>`.
- **It reads the map every 2 s**: a shard that joins later is in every other shard's ring at its next
  read - `Shard map version 4: 3 shard(s), the ring rebuilt`.
- **It elects under its own keys**: `<prefix>shards/<id>/leader`, `.../nodes/...`, `.../handover`. A
  second node started with the same `--shard-id` becomes that shard's replica; the map names the
  group's primary, which writes its address there when it becomes one.
- **It keeps a node key under its lease** (`<prefix>shards/<id>`), gone when the node stops. The map's
  entry stays: a shard that is down is a shard whose symbols are unavailable, not one whose symbols
  move to another.
- A shard etcd cannot reach at its start says so once - `Shard s0 is not registered yet (etcd
  unreachable): every write is refused as not its own until it is - trying again every 2 s` - and
  registers when it can.

Clients find the shards through the map: the Python pool with `coordinator_endpoints`, and the C++
`OrderbookPool` with `PoolConfig::coordinator_endpoints`, each reading it at every health check.

**Add shards before the writes flow.** A shard added to a running cluster takes its share of the ring
from the others, and nothing moves those symbols' rows to it by itself - `MIGRATE` moves one symbol at
a time (below) - and for up to 2 s, until each shard has read the new map, the others still take its
symbols' writes. Symbols that must stay where their history is can be assigned in the map before the
shard is added.

### Moving a symbol to another shard

`MIGRATE <symbol.exchange> <shard>` on the shard that owns the symbol - by the map's assignment or by
the ring, as a write is owned - moves it there **with its rows** (#196), while it is written. It answers
`OK` as soon as the move has begun; `SHARD_INFO` says how it goes:

```
migration_symbol     BTC-USD.BINANCE
migration_target     s1
migration_phase      done          (adopting, copying, switching, done, failed, unknown)
migration_rounds     2
migration_updates    20401
migration_rows       102001
migration_freeze_ms  41.7
migration_error      ...           (when it failed, or its end is unknown)
```

What happens, in order:

1. **The target adopts the symbol** (`ADOPT <symbol.exchange> BEGIN <source>`, which the source sends):
   it takes the symbol's writes from that connection alone, and only where none of its rows is.
2. **The copy.** The source pins its segment files - no merge and no retention sweep runs until the
   move ends - seals the symbol and sends every row to the target as the writes that stored them,
   with their event times, pipelined in batches of up to 512. Then it copies again whatever arrived
   meanwhile, in rounds, until a round copies fewer than 2 000 updates (or after eight). The symbol
   takes writes throughout.
3. **The switch.** The symbol's writes are refused `SYMBOL_MOVING` - the clients try again - while
   the last round's arrivals are copied and the map in etcd is compare-and-swapped to name the
   target. Then the target is told (`ADOPT … END`), and the source refuses the symbol from then on
   (`SYMBOL_MIGRATED`, or `NOT_OWNER` once it reads the map). `migration_freeze_ms` is this window.

Writers see nothing but latency. The Python pool and the C++ router try `SYMBOL_MOVING` again on the
same shard, and `SYMBOL_MIGRATED` and `NOT_OWNER` where the map, read again, sends them - for up to
10 s, with pauses from 20 ms to 200 ms. The source keeps its rows of the symbol (storage is
append-only); readers routed by the map read the target.

**With client authentication**, the source authenticates to the target as `--migration-identity
<identity>`, an identity in its own `--auth-secret-file`, so every shard's file must have it. A node
started with an identity its file does not have refuses to start. **With `--tls-client`**, the source
connects with TLS and verifies the target against `--tls-ca-file`.

When it fails:

- **Before the switch** - the target is unreachable, refuses to adopt, dies during the copy; etcd
  refuses the map; or the rounds never came down to a small remainder, the last taking more than a
  second (`its writes arrive about as fast as they are copied`), which would freeze the symbol for
  about as long - the source abandons the adoption (`ADOPT … ABANDON`, which drops what the target
  had stored), thaws the symbol and goes on taking its writes: `migration_phase failed` and why. If the
  target could not be told - it was down - it keeps what it had stored, and the next `MIGRATE` is
  refused with `ERR shard <target> holds rows of <symbol> already`: run `ADOPT <symbol.exchange>
  ABANDON` on the target, which drops them, and move it again. A move tried again stores nothing
  twice.
- **When the switch's outcome is unknown** - etcd did not answer the compare-and-swap and has not
  said since, for 60 s, whether it took it - the symbol **stays refused** on the source: the map may
  name the target, and a write taken on the source would then be stored where nobody reads it.
  `migration_phase unknown`. Restart the source once etcd answers: it reads the map and follows it.

Limits of this first version:

- One move at a time per shard, and a shard group of one primary: a replica of the target does not
  drop what an abandoned move had stored, and a mesh (multi-master) shard is not moved.
- The rows are sent in the order a scan of the source delivers them, which is what a `SELECT`
  compares. A symbol whose writes came out of time order across seals - a backfill, a late
  correction - may end with another of the updates that share a level on top of the target's book,
  where the rows are the same.
- Nothing rebalances by itself: a shard added to a running cluster still takes its share of the ring
  without the rows (above), and moving them is a `MIGRATE` per symbol.
- **Moving a symbol back** to a shard it left: that shard kept its rows, so it refuses to adopt the
  symbol until `ADOPT <symbol.exchange> ABANDON` on it drops them - the move back copies every row
  again, those included.

## Tuning that is real for this engine

Three of the knobs people expect are **not** tuning for this engine, and saying so is more useful
than listing them:

| Knob | Why it is not here |
|---|---|
| `LimitMEMLOCK` | The engine locks no memory. No `mlock`, no `MAP_LOCKED`, no `MAP_HUGETLB` anywhere in the sources — checked, not assumed. Raising the limit would raise it for nothing, and in a unit file it reads as knowledge about the engine's requirements. |
| Huge pages | `MADV_HUGEPAGE` does not appear in the engine. What transparent huge pages do to the mmap'd segment reads is a property of your kernel's defaults, not of a setting we expose, and **we have not measured it** — so there is no number here to justify changing it. |
| `MemoryMax`, `CPUQuota` | An engine tuned for particular hardware and then capped below it is a contradiction. If the host is shared, the cap belongs to whatever else is on it. If a CPU cap is there anyway — a container runtime's, a parent slice's — the node reads it, and its startup line says so: `machine: 1 usable CPU: affinity 4, cgroup v2 limit 1.50 CPUs (…)` (#156). |

What does matter:

### CPU placement

The hot path is the client event loops (`--io-threads`, named `ob-io-0` …), one flush thread, and
in multi-master one more io loop. By default there is **one client loop per CPU the node may use**
and each spins for 10 µs after an event where there is a CPU to spare — the `boost` profile, sized
at startup to the mask and the cgroup limit it finds (`docs/cli.md`, "Profiles"). On a host the
engine shares with other work, `--profile eco` is one loop blocking between events, the engine as it
was; and a mask or a limit you give it is one it sizes itself to, so pinning the service to two cores
gives it two loops.

What helps is keeping those threads off the cores that handle NIC interrupts, so that a burst of
packets does not preempt the thread applying writes. With more than one client loop,
`top -H -p $(pidof ob_tcp_server)` shows which loop is busy, and the log line
`Reactor N adopted fd=... conn_id=... from ...` says which connection it is serving.

```bash
# Where the NIC's interrupts land today:
grep -E "$(ls /sys/class/net | grep -v lo | head -1)" /proc/interrupts
```

Then pin the service away from those cores by uncommenting `CPUAffinity` in the unit. There is no
default, deliberately: pinning to particular cores on an unknown machine is a mistake rather than a
tuning. The node counts the mask it was given: its startup line `machine: N usable CPUs: affinity
N, …` is the number of CPUs `CPUAffinity`, `taskset` or a cpuset left it, and `--print-config` prints
the same sentence.

For a dedicated host, going further — `isolcpus` on the kernel command line, then placing the
service on the isolated cores — removes the scheduler from the picture. That is a decision about
the whole machine, so it is not something a package can make.

### File descriptors

`LimitNOFILE=65536` in the shipped unit. The arithmetic: one descriptor per client session
(`max-sessions`, 64 by default), one per replication peer, one per multi-master peer, the listening
sockets, the metrics socket, the WAL file, and one per open columnar segment during a flush. The
default configuration is nowhere near the limit; the headroom is for `max-sessions` raised into the
thousands, which is the case the limit exists for.

### `vm.swappiness`

The engine keeps live depth in memory and expects it to stay there. A page of an SoA buffer that
went to swap turns a sub-microsecond read into a disk seek, and the read holds a seqlock while it
happens.

```bash
echo 'vm.swappiness = 1' | sudo tee /etc/sysctl.d/60-orderbook.conf
sudo sysctl --system
```

`1` rather than `0`: zero disables swap for the cgroup entirely, which turns memory pressure into
the OOM killer choosing a victim, and the victim is often the largest process — this one.

### fsync policy per storage device

`--fsync-policy` (or `fsync-policy` in the configuration file) decides when the WAL is durable, and
the right answer depends on what the data directory sits on. It defaults to `interval`.

An unrecognised value is refused rather than read as `interval`: an operator who asked for `every`
and quietly got something weaker would find out from a lost write.

| Device | Policy | Why |
|---|---|---|
| NVMe with power-loss protection | `every` if an acknowledged write must survive a power cut; `interval` if losing one flush interval is acceptable | The capacitor makes an fsync **cheap, not unnecessary**: it protects what has reached the device, and a write that was never synced is still in the kernel's page cache, which a power cut empties whatever the device has. This row said `interval` or `never`, with the reasoning the other way round, and `never` is not a value the server accepts — it refuses to start on it. |
| Consumer SSD or anything virtualised | `every` | Without power-loss protection, an acknowledged write that is not fsynced is a write you can lose. |
| A filesystem on a network device | `every`, and reconsider | The engine's latency claims assume local storage. |

Put the data directory on the fastest local device you have, and **not** on the same device as the
journal of a busy filesystem: the WAL is sequential and small-record, so it is exactly the workload
that suffers from sharing a queue.

### The WAL on a filesystem of its own

A write is answered after its record is written to the WAL, under the lock every write takes, and on
ext4 that `write()` waits for the filesystem's journal. A data directory of thousands of instruments
keeps the journal busy by itself - every segment is a directory of files, and one 50 s run of 4 001
symbols left 74 274 of them - so on a node that size a one-level write is answered now and then a
tenth of a second late or more: 0.13 - 0.31 s in four runs of six, measured on a node of 4 000
symbols (i3-7100U, ext4 over LUKS, #186). The node says so:

```
A batch of 1 write(s) waited 0.0 ms for the engine lock and held it 305.4 ms - 0.0 ms of it waiting for room in the pending queue, 305.4 ms in the WAL append; a WAL on a filesystem of its own (--wal-dir) does not wait for the data directory's journal
```

`--wal-dir <DIR>` (`wal-dir` in the configuration file) puts the WAL files and `wal_identity` there;
on an ext4 of its own the same runs had no write slower than 11 ms. A separate filesystem isolates
the `write()`; a separate **device** isolates the WAL's `fsync` too, which at the write ceiling a
device the segments keep busy stretches to seconds (#190):

```
A flush tick took 3307 ms for 875520 row(s): WAL sync 3030.7 ms, drain 127.6 ms, seals 148.5 ms, retention 0.0 ms, merges 0.0 ms - writers at the ceiling wait for the room it frees
```

The directory must be outside the data directory - installing a snapshot clears the data directory of
everything not named `wal_*`. To move an existing WAL: stop the node, move its `wal_*.bin` files and
`wal_identity` to the new directory, and start it with `--wal-dir`. The data directory records where
its WAL is (`wal_location`), and a start that would begin an empty WAL beside the real one is refused
with what to do: `--wal-dir` naming an empty directory while the WAL is still in the data directory
(`the WAL is in ... and --wal-dir names ...: move its wal_*.bin files and wal_identity there first`),
or a start without `--wal-dir` - or with another, empty, directory - while `wal_location` names one
that holds it (`the WAL of ... is in ..., where --wal-dir put it; start with --wal-dir ...`). Back
into the data directory is the same move, with `wal_location` removed before the start.

### A client that pipelines writes

Every `INSERT` and `MINSERT` a client sends in one read is applied as one batch: one acquisition of
the engine's write lock, and one WAL `write()` per run of records between rotations (#155). A read
is at most 64 KiB, so a batch is at most a few thousand writes and the lock is held for well under a
millisecond even then. Nothing a client can read changes — each write is answered as it would have
been as a command of its own, in the order the commands came, and a command after a write in the
same read sees it — except what the batch costs:

- **Under `--fsync-policy every`, one `fsync` covers the batch.** That is group commit, and it is what
  makes `every` affordable for a client that pipelines: measured on an m9g.xlarge with its data on
  EBS, one connection sending batches of 64 twenty-level `MINSERT`s wrote **10 794 levels a second
  with a sync per write and 543 260 with a sync per read**. `OK` still means the record is on the
  disk — the answers are sent after the sync.
- **A `write()` the disk refuses refuses the record it stopped at**, and the records behind it are
  tried again with the next `write()`, as the next command after a refused one was. A record torn in
  the middle abandons its file (#126) and the ones behind it go into the next.
- **A failed `fsync` refuses every write of the run it covered** — up to a read's worth, where one
  command at a time it was one. See "When an fsync fails" for what that costs a client that resends.
- **A client that stops reading** is closed once its send buffer is full, as before. The writes of
  the read that could not be answered have been applied: the uncertainty of any write whose answer
  never arrives, for the writes of one read where one command at a time it was for one.

### WAL rotation, and what frees WAL files

`--wal-rotate-bytes` decides how large a WAL file grows before the writer opens the next one. It
defaults to 512 MB and it is a **trigger, not a file size**: rotation is checked after a write, so a
file may exceed it by one record — by more only while the disk refuses the record that would end it
(below).

Three things follow from it, which is why it is worth a section rather than a row in a table:

- **What a crash replays.** Together with `--flush-interval-ms`, it bounds the work between the last
  checkpoint and the end of the log.
- **What a reconnecting replica may have to scan.** A replica resumes from the position it saved and
  the primary streams forward from there, across files.
- **What retention can free, and when.** WAL files are deleted **whole**, and only below the file
  the slowest **connected** replica has acknowledged — and below the file the newest checkpoint
  **known to be on the device** vouches for (#160). That is one tick behind the flush that wrote it,
  because a checkpoint is appended without a sync of its own and the next WAL sync is what makes it
  durable; after a failed sync of any kind it goes no further until a restart ([below](#what-a-power-cut-keeps)).
  Before #160 it was the file the last flush drained to, and before #159 the file being written —
  records written while a flush writes its segments have their rows still queued, and a rotation in
  that window left them in a file retention then deleted. So a large threshold means a lagging
  replica pins more bytes on disk; a small one means more files for every retention pass to walk.

That last point is the one that surprises people, and it is worth being concrete about the two
halves, because they differ in a way that matters during an incident:

| The replica is | `safe_truncate` is | What happens to old files |
|---|---|---|
| connected and behind (slow, or stopped) | its acknowledged file | **kept** — it can still catch up from the log |
| gone (crashed, killed, network down) | the file the newest synced checkpoint vouches for — usually the current one | **freed** — a reconnect from an old position is answered `ERR WAL_TRUNCATED` and needs a snapshot |

A replica that is merely slow therefore costs disk, and a replica that is **absent** costs a
snapshot when it comes back. Neither is a defect; the failure would be a third case, freeing a file
a connected replica still needs, which is what `ob_replicas_lag_unknown` counts and what the
retention test in `tests/integration/test_wal_rotation.py` exists to catch.

What the second case looks like in the logs, and it is worth recognising because until #125 it did
not finish. On the primary:

```
{"component":"repl_mgr","msg":"catchup: cannot open WAL file .../wal_000000.bin: No such file or directory"}
{"component":"repl_mgr","msg":"sent ERR WAL_TRUNCATED to replica fd=14"}
{"component":"repl_mgr","msg":"snapshot for replica fd=14 (connection 14) is being created on a worker thread"}
```

and on the replica, `snapshot bootstrap` lines ending in an install. **An `ERROR` reading
`snapshot bootstrap abandoned: manifest CRC32C mismatch` meant the bootstrap could never complete**
— every file arrived and verified, and the comparison at the end was unsatisfiable, so the replica
asked again every five seconds for ever. If you are running a build from before #125 and a replica
is stuck in that loop, the way out is to stop it, delete its data directory and start it again: it
then bootstraps as a new node rather than asking for a position.

**A rotation the disk refuses** is logged and survived, and neither of its two shapes refuses the
write that asked for it — that record is already in the file (#153):

- The ROTATE marker could not be written — a full disk at the moment the file ended:

  ```
  {"component":"wal","msg":"could not end .../wal_000004.bin with a ROTATE record: WALWriter: write failed: No space left on device. The record that crossed the rotation threshold is written; the rotation is tried again after the next one"}
  ```

  The file then goes on past the threshold, by one record per refused attempt, and ends at the
  first marker the disk accepts. This is the only way a file exceeds the threshold by more than
  one record.
- The next file could not be created — permissions, a full inode table, a descriptor limit:

  ```
  {"component":"wal","msg":"WALWriter: cannot open .../wal_000005.bin: Permission denied: this node has no WAL file to write to, so every write is refused until one can be opened; each write tries again"}
  ```

  Every write is then refused with that reason, and **the node recovers by itself** once the
  cause is gone — the next write opens the file and the log says
  `opened .../wal_000005.bin at offset 0; the writer had no WAL file for N attempt(s)`. Before
  #154 it did not: the writer stayed without a file, and every write was refused with
  `Bad file descriptor` until the process restarted.

The value is refused at both ends rather than clamped. Above 2 GiB: a WAL position is a file index
and a 32-bit offset read as one value, and a larger file would let the offset wrap and report a
position inside the wrong part of the file (#85). Below 65573 bytes — a 38-byte header plus the
64 KiB payload limit — a single write can fill a file on its own, so every write rotates and the
directory grows one file per record. The engine's own unit tests do use a 512-byte threshold, on a
`WALWriter` constructed directly; the floor belongs to the flag, because the number an operator
types has consequences nothing shows until the directory is unmanageable.

The file a node is writing is a gauge: `ob_wal_file_index`, published from the flush tick.

**The file a rotation leaves is synced by the flush tick, not by the write that crossed the
threshold** (#164). Rotation used to `fsync` it on that writer's thread, under the lock every
writer needs, and under `--fsync-policy none` nothing else syncs the WAL, so that was the whole file
at once — 295 and 323 ms measured on an m9g.xlarge with its data on EBS — with every writer
standing for it. The descriptor now goes on a list the next tick syncs and closes without the lock,
before the tick declares anything synced, and `FLUSH` and a shutdown sync what is on the list too,
under every policy. If four files are waiting — a node whose ticks are far apart — the writer syncs
the oldest itself, as before, and logs
`4 files left by rotation were still waiting for a flush tick to sync them; synced the oldest on the writer's thread instead`.
Under `none` that sync is still the longest thing the WAL does, and a writer can still meet it
indirectly: the tick holds the rows it took through the sync, so a writer fast enough to fill the
rest of the pending queue meanwhile waits for room. Measured with `perf trace` at one pipelining
connection writing 6.6 M levels a second: the two rotations of the run took 317 and 477 ms on the
flush thread, and they were the run's only two waits.

### When an fsync fails

Watch `ob_wal_fsync_errors_total`. It is separate from `ob_flush_errors_total` because the two ask
for different actions: a flush failing on `ENOSPC` means free space, an `fsync` returning `EIO`
means the device is failing and wants replacing.

**The engine cannot recover a failed `fsync`, and does not pretend to.** Linux reports the error to
whichever caller happened to be there and then **marks the affected pages clean**, so the next
`fsync` on that descriptor returns success with the data already gone. There is nothing to retry.
What the engine does instead:

- under `--fsync-policy every`, the write that could not be synced is **refused**, so the client is
  never told a record is durable when the sync for it failed — and a client that pipelines has the
  writes of one read synced by one `fsync`, so a failed one refuses every write of that read's run
  (#155)
- a `FLUSH` command over a failed sync is refused for the same reason
- `close()` logs it and completes the shutdown anyway — refusing to shut down would leave a node
  that cannot be restarted, and the records are in the WAL file either way
- the counter and an `ERROR` line naming the reason are permanent; no later success takes them back

**What a client should do with that refusal, and its cost.** The record is in this node's WAL
regardless — the write succeeded, the sync did not — so a client that resends produces a **second
row**, and **nothing deduplicates it**. The sequence-number dedup suppresses a second *delivery of
one record* between nodes (#61); a retried client write arrives with no sequence number, is minted a
fresh one, and is therefore a different record to every mechanism that looks — including the peers,
which store both. Storage is append-only, so the duplicate is visible: `SELECT` returns the sequence
number as its seventh column (#65), which is what tells the two rows apart. That is the honest
trade — a duplicate you can see, against a write you were told was durable and was not.

**Every storage failure this engine can reach is an `errno`, so it is a refusal and a counter —
never a signal.** That is worth stating because it is a property of a choice rather than of
storage: nothing here memory-maps a file for writing, and a growable mapping is the one shape in
which a full filesystem arrives as **`SIGBUS`** instead of `ENOSPC` — `ftruncate` extends sparsely
and succeeds, and the allocation fails later, when the page is touched and there is no return value
to check. Measured on an 8 MB tmpfs: reserving 64 MB succeeded and writing into it died with
`Bus error`, exit 135. A signal is not an exception, so #112's thread boundary could not catch it
and no client could be told. `docs/storage.md` records why the mapped store was removed rather than
adopted (#114).

### What a power cut keeps

A flush writes its segment files and then makes them durable with **one `syncfs()` on the data
directory**, before the checkpoint that claims them is appended; and startup **removes any segment
no surviving checkpoint vouches for**, rebuilding its rows from the WAL (#160). So under `every` an
acknowledged write survives a power cut, and under `interval` everything up to the last WAL sync
does. Measured with a cut the kernel performs — `tests/integration/test_power_cut.py`, dm-flakey
over a loop device, switched to drop every write, the way xfstests simulate one: **201 of 201**
rows back under both policies, where the build before answered **0**, because segment files were
never synced and a checkpoint said they were. This page said until then that a segment lost to a
power failure is rebuilt by replaying the WAL; that held only until the next flush's checkpoint
claimed its rows.

Three things about it an operator should know:

- **`syncfs()` syncs the whole filesystem**, not the engine's files. On the data directory's own
  device those are the engine's writes; on a shared filesystem they are everybody's dirty pages as
  well, and every flush pays for them — one more reason for the dedicated device the table above
  recommends. On an otherwise idle volume it measured 5–60 ms a flush, outside the lock writers
  take, and no throughput cost above run-to-run spread.
- **It reports a failed writeback from Linux 5.8.** Older kernels returned success from `syncfs()`
  whatever writeback did, so there a failed segment write-back is not seen, and the guarantee is
  only as good as that.
- **A replica's position is saved only after the data it names is on the device** (#162). A
  snapshot's files are synced after the install renames them into place and before the replica
  records the snapshot's position - measured, without that sync a cut right after the record left
  a replica answering 0 of 1100 rows - and `repl_state.txt` itself is replaced whole rather than
  rewritten.
- **A failed sync of any kind freezes the checkpoints until the node is restarted.** One `ERROR`
  line says so, `ob_checkpoints_frozen` goes to 1 and stays there, and `ob_segment_sync_errors_total`
  or `ob_wal_fsync_errors_total` counts the failure. From then on **no checkpoint is appended and
  WAL retention stops**, so the WAL grows; the rows are written and readable, and a power cut in that
  state loses nothing the fsync policy promised, because every record it acknowledged as synced is
  in the WAL for the restart to rebuild from. The way out is to fix the disk and **restart
  the node**: the restart replays from the last checkpoint synced before the failure, rebuilds every
  segment written since (`ob_segments_rebuilt_from_wal_total`, and a `WARN` saying how many), and the
  freeze ends with the process.

  **Why not simply try the sync again**, which the first version of this change did — it let the
  next flush whose `syncfs()` succeeded claim the failed one's segments as well. The section above
  says why that cannot work: a failed sync marks the pages it could not write clean, so the next one
  succeeds without writing them, and a checkpoint after it vouches for files the device may not
  have. The same is true of a checkpoint the WAL had not yet synced when a WAL `fsync` failed.
  PostgreSQL met this in 2018 and answers it by crashing into WAL recovery; this engine keeps
  serving, and keeps every record until a restart can rebuild from them.

## Backing up and restoring a node

A node takes its own backup, into the directory its configuration names, and `ob_restore` restores
one offline into an empty data directory (#34). Replication is not a backup: a replica repeats an
operator's mistake - a retention set too short, a symbol dropped - in the same second as its
primary.

```bash
ob_tcp_server --data-dir /var/lib/orderbook --backup-dir /var/lib/orderbook-backups ...
ob_backup --host 127.0.0.1 --port 9090          # from cron: exit 0 once the backup is complete
ob_restore --verify /var/lib/orderbook-backups/20260928T093000.123Z
ob_restore --backup /var/lib/orderbook-backups/20260928T093000.123Z --data-dir /var/lib/orderbook-2
```

`--backup-dir` may not be the data or the WAL directory, inside either, or hold either: the engine
reads every `meta.json` under its data directory as its own, and a replica's bootstrap removes every
directory there but its own, so the server refuses to start with that layout. `ob_backup` takes the
client's `--auth-identity` with `--auth-secret-file` (the server's client-secret format; the secret
is never an argument) and `--tls`/`--tls-ca-file`; it exits 0 when the backup is complete, 1 when it
failed, 2 when it could not ask, 3 when `--timeout-s` ran out first.

### What a backup is

- **A cut at one moment.** Every write acknowledged before `BACKUP` was accepted is in it, a `MINSERT`
  whole or not at all; a write acknowledged after the cut is not. The cut seals everything waiting,
  as `FLUSH` does, under the engine's lock, so writers wait for it as they wait for a `FLUSH`.
- **Its segment files and a description**, `backup.json`: every file with its size and CRC32C, the
  WAL position and identity the cut was taken at, the node and its role, the engine's version, and
  the sequence state - for a mesh node, the frontiers it holds of every origin.
- **Linked or copied.** When `--backup-dir` is on the data directory's filesystem the files are hard
  links: a backup in milliseconds, which a deletion, a merge, the retention sweep and a mistake in the
  data directory do not touch - and which is on the same device. Copy the backup directory off the
  host (it is a plain directory), or give `--backup-dir` a filesystem of its own, and the node copies
  the bytes, checksumming them as they pass and checking the free space first. `BACKUP STATUS`, the
  log and the description say which it was.
- **One at a time.** A second `BACKUP` is refused with the name of the one running; `BACKUP STATUS`
  says where it is: its phase (`cut`, `link` or `copy`, `checksum`, `publish`), files and bytes done,
  how long its cut held writers (`cut_ms`) and how long it held the segment files (`pinned_ms`).
- **Complete, or not a backup.** It is written as `.partial-<name>` and renamed to `<name>` once every
  file and the description are on the device (one `syncfs()` of the backup directory's filesystem).
  A failure removes what it wrote; a directory named `.partial-...` is one a node stopped while taking,
  which the node lists in its log at start - remove those. The node removes nothing else there:
  deleting old backups is the operator's, and with hard links removing one frees only what no other
  backup, and not the data directory, still links.

While one is taken, merges and the retention sweep wait for as long as its files are linked or copied
(`pinned_ms`); for a copied backup of a large store that is the length of the copy. A replica's
snapshot bootstrap holds them the same way.

### Restoring

1. `ob_restore --verify <backup>` - optional, and worth running on a schedule: a backup that was
   never read back is a hope.
2. `ob_restore --backup <backup> --data-dir <empty> [--wal-dir <empty>]`. The whole backup is checked
   against its description first, and one file that does not match writes nothing. The target must
   be empty. Then the files are copied into it, checked again as they pass, and the store is opened
   through the engine's own start, which takes the backup's sequence state.
3. Start the server on the new directory as before - with `--wal-dir` when the restore had one.

What comes up is the backup's rows, each once, and writes after it numbered on from the backup's. The
WAL is new, with a new identity, and begins at its second file; so a replica that followed the node
before the loss finds a WAL at the same address that is not the one it was reading (`a different WAL at
the same address` in its log), discards what it holds, asks for the log from the start - and is sent a
snapshot, because the rows are in the restored segments and in no record of the new WAL. For a primary
with replicas, restore the primary; the replicas bootstrap from it on their own.

**A multi-master node** restored with its own `--mm-node-id` goes on numbering its writes from what the
backup held. That is right when the whole mesh is restored, and wrong while any peer holds records the
node wrote after its backup: the new records would take numbers the peers have seen, and they would
drop them as duplicates. With the mesh alive, give the restored node a new `--mm-node-id` - it then
holds the old node's records as another origin's, and its peers catch it up on what it lacks - or do
not restore it at all, and let it bootstrap from a peer the ordinary way (a node that holds nothing
asks for a snapshot). A backup whose vector was past the entries a node can state is refused as a
mesh node's: without frontiers a restored mesh node would take every row again.

**A shard** backs up its own symbols. The shard map is in etcd, not in any backup: save it beside the
shards' backups (`etcdctl get /ob/shard_map --print-value-only`), and restore the shards before any
client writes. A symbol a shard held after it had moved away (#196 keeps the source's rows until an
`ADOPT ABANDON`) comes back with the shard as rows nobody routes to.

### What it costs, and what a restore takes

Measured in Release on the development machine (i3-7100U, one ext4 on NVMe):

<!-- numbers from evidence/2026-09-28-backup/, filled in by the measurement -->

**RPO** is the interval between backups: the WAL written since the last one is not in any backup, and
this release has no restore to a point between two (below). **RTO** is `ob_restore` - the check, the
copy, the start - plus the server's own start on the restored directory, measured above.

### Not in this release

- **Restoring to a moment between two backups** from the WAL. It needs the WAL replayed from a
  backup's position to a chosen record, past checkpoints that claim segments the backup does not hold
  - a change to the start's replay, with a specification of its own.
- **Incremental backups.** A segment has no identity that lasts: its directory's name comes back once a
  merge has removed it, so "the same path, size and CRC" is not proof of the same contents.
- **Scheduling, retention of old backups, shipping off the host.** Cron with `ob_backup`, and any tool
  that copies a directory.

## Which build is running

Three ways to ask, all reporting the same number:

```bash
ob_tcp_server --help | head -1                      # not this: --help does not carry a version
echo "STATUS" | nc localhost 9090 | grep '^version:' # a running node, over the wire
curl -s localhost:9091/metrics | grep ob_build_info  # a running node, for a monitoring system
```

The startup line says **starting**, not listening, and the line that reports a working socket comes
from the log once the bind has succeeded:

```
ob_tcp_server v0.1.0 starting on port 9090, data-dir: /var/lib/orderbook
{"ts":"...","level":"INFO","component":"tcp_server","msg":"listening on port 9090, version 0.1.0, ..."}
```

The distinction matters when a port is taken. The old line announced `listening on port N` before
the bind was attempted, so a failed start printed a claim to be listening with the error underneath
it — and because it went to `stdout` unflushed, redirecting the server's output to a file or a
journal delayed it until the process exited. Grep the log's `listening on port` line, from the
logger, to confirm a start.

## The dashboard and the alert rules

Two files ship with the engine (#35):

- `packaging/grafana/orderbook-engine.json` — a Grafana dashboard: writes and queries, flush and
  durability, storage, replication and failover, the multi-master mesh. Import it and pick the
  Prometheus data source; the `job` and `instance` variables come from `ob_build_info`, which every
  node exports with its version and its role.
- `packaging/prometheus/orderbook-engine-alerts.yml` — alert rules, to load with `rule_files:`.

| Alert | Severity | What it means | Where to look |
|---|---|---|---|
| `OrderbookCheckpointsFrozen` | critical | a sync failed; no checkpoint claims anything since, and WAL retention has stopped | "When an fsync fails" |
| `OrderbookSyncErrors` | critical | the device refused a WAL or segment sync | "When an fsync fails" |
| `OrderbookWritesRefused` | critical | writes answered with an error: the pending queue freed no room in 5 s | below, `ob_pending_rows` |
| `OrderbookWritersWaiting` | warning | for ten minutes some writes waited for the flush: ingest near the device's ceiling | "The WAL on a filesystem of its own" |
| `OrderbookFlushErrors`, `OrderbookLoopErrors` | warning | a flush tick, or an iteration of a background loop, threw and the next one runs | "When a subsystem's loop keeps failing" |
| `OrderbookReplicaDisconnected` | warning | fewer replicas connected than an hour ago | "When a replica falls behind" |
| `OrderbookReplicationLagHigh` | warning | the slowest replica more than 64 MiB of WAL behind for five minutes | "When a replica falls behind" |
| `OrderbookReplicaLagUnknown` | warning | a replica that has not said where it is for five minutes | "A replica that is slow, rather than one that is behind" |
| `OrderbookFailover` | info | the epoch changed: a new primary | "High Availability" in `docs/cli.md` |
| `OrderbookMeshPeerDown`, `OrderbookMeshLagHigh`, `OrderbookMeshPeersDropped` | warning | a mesh peer gone, far behind, or dropped as slow or for its clock | "When a mesh peer falls behind", "When a peer's clock is wrong" |
| `OrderbookAuthFailures` | warning | more than one failed authentication a second | "Turning on client authentication" |
| `OrderbookSubscribersDisconnected` | warning | subscribers closed for reading too slowly | below, `ob_subscription_queued_bytes` |
| `OrderbookBackupFailed` | warning | a backup failed; it left nothing that looks like one | "Backing up and restoring a node" |
| `OrderbookBackupStale` | warning | a node that has taken backups has no complete one from the last 26 hours; one that never took any is not paged | "Backing up and restoring a node" |

The thresholds are starting points for a deployment to tune: 64 MiB of replication lag is seconds of
writes at the ceiling and hours of a quiet market. What is not left to taste is checked in CI:
`scripts/check_dashboards.py` holds every metric the two files name against the engine's registry and
refuses a rate of a gauge; `scripts/check_alert_rules.py` runs every alert through `promtool test
rules` on the series of the fault it is for - and a healthy node's, on which none fires; and an
integration test holds the names against what a running node serves.

## What the metrics say when something is wrong

`--metrics-port` exposes a Prometheus endpoint. Three gauges answer most questions before a log does:

- `ob_session_pending_bytes` — response bytes queued across sessions. A client that has stopped
  reading shows up here long before its session hits the 64 MB cap.
- `ob_subscription_queued_bytes` — the same for pushed subscriptions, and
  `ob_subscription_overflow_disconnects_total` is the only way you learn that a consumer could not
  keep up.
- `ob_pending_rows` — rows waiting for a flush, including a batch the flush tick has taken and not
  yet drained (#164). Growing steadily means the flush interval is longer than the write rate can
  afford. At a million a writer waits for room: `ob_writer_backpressure_waits_total` counts each
  wait, and `ob_writer_backpressure_refusals_total` each write refused after five seconds without
  room. Waits are not an error — a writer faster than a flush cycle meets the ceiling once a cycle,
  which one pipelining connection on an m9g.xlarge does at 6.6 M levels a second — and refusals
  are: the flush cannot make progress.
- `ob_unsealed_rows` — rows drained, readable, and waiting in memory for their store's seal (#165
  part 2a): a store is sealed at 65 536 rows or when its oldest are ten seconds old, and every store
  together is held under four million rows. These are the rows a crash replays from the WAL, and
  `ob_seals_total` counts the seals. A value that sits at the budget means seals cannot keep up —
  the flush's write rate, not its interval, is what to look at. At `--log-level DEBUG` each seal
  says what it wrote, and each tick that took rows, at its end, where its time went — two lines of
  one tick at four pipelining connections on an m9g.xlarge:

  ```
  sealed 4 store(s), 785920 row(s): chosen and written in 11.59 ms, synced in 24.21 ms, merged and checkpointed in 0.49 ms
  flush tick: 810240 row(s) taken; WAL sync 13.58 ms, drain 9.40 ms, seals 36.30 ms, retention 0.76 ms
  ```

  At the write ceiling the cycle is what bounds a writer, so these are the lines to read when
  `ob_writer_backpressure_waits_total` climbs: a tick longer than the writers take to fill the
  pending queue is time every writer waits. Since part 2b the tick line ends with `merges … ms`,
  the time the tick spent merging segments ([merging segments](#merging-segments)).
- `ob_compactions_total`, `ob_compaction_inputs_total`, `ob_compaction_rows_total` — merged
  segments published, the segments they replaced, and the rows they wrote again (#165 part 2b).
  The second over the first is the fan-in; the third over the rows written is what a node pays in
  rewriting for its segment count. `ob_compaction_errors_total` counts merges that could not be
  written or published — a disk that refuses them pauses merging for ten seconds each time — and
  `ob_segments_awaiting_removal` the replaced segments whose files wait for a sync or for a query
  still reading them: it goes back to zero on its own, and one that does not is a query that does
  not end — or a sync that failed, after which nothing is removed until a restart.

One counter is worth watching for a different reason: **`ob_refused_commands_total`** is the number
of command lines the parser would not accept — an unknown word, or a known command carrying a token
its grammar has no place for (#107). A client that is working produces none of them, so any rate at
all means somebody is sending something they believe is being stored. The matching log line is
written **once per connection** (`Refused a command from fd=9: unexpected token 'x'; …`) rather than
once per line, because a refusal is reachable before authentication and a line per refusal is a
flood anyone who can reach the port can drive; the counter is the half that carries the volume.

One gauge is an instruction rather than a reading: **`ob_checkpoints_frozen`** is 1 once a sync of
any kind has failed in this process, and it means *fix the disk, then restart this node* — until
then no checkpoint is appended and the WAL grows without bound ([what a power cut keeps](#what-a-power-cut-keeps)).
It never goes back to 0 on its own, because nothing but a restart makes the state it reports safe.

A metric written under a name nobody registered is dropped in silence, so `scripts/check_metrics.py`
fails CI for the class rather than trusting the reader to notice a flat zero.

### A mesh peer that is unreachable rather than down

`Dial to peer 3 at 10.0.0.3:7100 failed: no answer within the connect deadline (attempt #4)` means
the node's SYNs are going nowhere — a firewall dropping them, a host that has vanished, or an
address the registry still advertises for a machine that no longer answers on it. A peer that is
merely *stopped* refuses the connection instead, and the line then carries the kernel's own words
(`Connection refused`). The difference is worth reading: one is a network or configuration problem,
the other is a process to restart.

The deadline is **5 seconds** and is not configurable. That is deliberate — the kernel's own answer
is about two minutes of SYN retransmissions, and nothing in a mesh wants to wait that long to learn
that a peer is unreachable. The dial happens on the reconnect thread with no lock held, so a peer in
this state costs nothing but its own retries: since #97 it does not delay the io loop, other peers'
links, client writes, or shutdown. If you are on a release before that, the same situation stops the
node for as long as the kernel keeps retrying.

Backoff applies to every failure, including this one, so the log rate falls away rather than
repeating at loop frequency (#95). `ob_mm_peers_connected` beside `ob_mm_peers_tls_verified` is the
pair to alert on — alert on the *difference*, not on either number.

### A mesh link that is up and carrying nothing

A peer that cannot be dialled is the section above. This one is worse to diagnose, because the
connection is **established** and the bytes are not arriving: a partition that drops segments
without resetting anything, a middlebox holding the stream, a peer whose process is stopped. The
node keeps accepting writes throughout — there is no quorum in this mesh, which is the trade
multi-master makes — so what you need to know is what it will tell you, and when.

**For the first few megabytes it will tell you nothing, and that is a property of TCP rather than of
the engine.** `ob_mm_peer_send_buf_bytes` is the engine's own queue for a peer, and it cannot grow
until `send()` returns `EAGAIN`, which needs the **sender's** socket buffer full first. The engine
sets no `SO_SNDBUF`, so that is `tcp_wmem`'s maximum. Measured on this machine (i3-7100U, loopback,
a peer that stopped reading): **3 015 750 bytes** of accepted writes before the queue passed a
256 kB ceiling — about 2.9 MB held by the kernel where no gauge can see it.

**What does move in that window is the records lag.**

```
curl -s localhost:9091/metrics | grep -E 'ob_mm_(replication_lag_records|peers_position_unknown)'
```

`ob_mm_replication_lag_records` is how many records the furthest-behind peer is missing, counted
from the per-origin version vectors — the one position two nodes can compare (#118). It is
recomputed by the **anti-entropy pass** and nowhere else, so its freshness is
`--anti-entropy-interval-seconds` (default 30) and a scrape right after a burst reads the previous
pass's number. Read it beside `ob_mm_peers_position_unknown`, which counts peers whose position
cannot be compared at all — a peer that has said nothing looks identical to a peer holding nothing,
and the pair is what separates them.

**Past the ceiling the peer is dropped, and that is the repair rather than the failure.**

```
{"component":"mm","msg":"Peer 2 is not draining: send_buf=262264 > 262144 — dropping the connection so it reconnects and catches up"}
```

`ob_mm_peer_dropped_slow_total` counts it. Dropping is deliberate: the alternative measured before
this ceiling existed (#69) was one unreachable peer growing the writer at about **113 MB/s** with
nothing to stop it. The connection is closed **without** clearing the queued bytes, because a buffer
that starts mid-frame would desynchronise the peer's parser; the peer reconnects, and catch-up
sends it what its version vector says it lacks ([catching a peer up](#catching-a-peer-up)). `--mm-max-peer-send-buffer` is the ceiling (64 MB by
default) and lowering it makes the drop happen sooner, not the buffering safer.

**What the engine does not promise here.** Nothing is refused while a peer is unreachable, so the
writes accepted during a partition exist on one node until the link returns — the mesh converges
afterwards, by anti-entropy or by catch-up, and `docs/architecture.md` has the conflict rules that
decide what convergence means when both sides wrote. And the first few megabytes of that divergence
are invisible in the per-peer queue gauge, as measured above: the records lag is the number to alert
on, not `ob_mm_peer_send_buf_bytes`.

### Conflicts, and what counts as one

A conflict is two nodes writing one price level; last-writer-wins by the hybrid clock decides it
(`docs/architecture.md`). `ob_mm_conflicts_total` counts them and `MM_CONFLICTS` lists the latest.
The log says them by the window, not a line each:

```
Conflict detected: REMOTE wins for BTCUSDT/BINANCE/0/6500000 (origin 2 against 1, remote_hlc={...} local_hlc={...}); more within 10 s are counted, not logged
1523 more conflict(s) between origins in the 12 s since the last line, and now: LOCAL wins for BTCUSDT/BINANCE/1/6500100 (origin 3 against 1)
```

A steady rate of these is the mesh doing what it is for - two writers of one instrument - and the
counter is the thing to graph. The node that wrote a level last, writing it again, is **not** a
conflict: until #182 it was, and a mesh logged one for nearly every replicated update (60 MB of log
on each receiver for 300 000 writes from one node).

### A replica that is slow, rather than one that is behind

`replica fd=9 is not draining: queued=16780544 > 16777216 - dropping the connection` means this
node is holding 16 MB of output for one replica and the socket is not moving it. Read it as a
statement about the **link or the replica**, not about how far behind that replica was: since #93 a
catch-up streams in bounded batches and stops at half this ceiling until the socket drains, so the
size of the range a replica asks for cannot reach the ceiling on its own. Before #93 it could, and
the same message meant something quite different — a replica more than 16 MB behind was dropped by
the act of catching it up, reconnected, and was dropped again.

A catch-up that is progressing says so twice: once at the start, with where it is going —

```
catchup for fd=9: from_file=3, from_offset=1048576, through_file=7, through_offset=41904,
wal_dir=/var/lib/orderbook/wal
```

— and once at the end, with where it arrived and how much live traffic it held back:

```
catchup complete for fd=9 at file=7 offset=41904; releasing 24576 bytes of live records
that arrived while it streamed
```

Those held-back bytes are the ordering guarantee, not a queue to worry about: a record written while
a replica is still receiving history must arrive after that history. If the number is large and the
completion line is slow to appear, the replica is behind by a lot and the write rate is high — the
pair to watch is that number growing across successive completions, which says the catch-up is
losing ground to live traffic. It is bounded by the same 16 MB ceiling as the send queue, and
reaching it drops the replica as above.

### A replica that restarted, and which stream it decided it was on

Since #101 a replica keeps its store across a restart and asks the primary to continue from where
it stopped. It says which of four things it decided, on every connection, and the line names the
reason rather than only the action:

```
primary serves stream 7213..., the one our position belongs to - resuming from file=3 offset=1048576
```

Nothing is re-streamed beyond what the primary appended while the node was down. This is the
ordinary line, and its absence after a restart is the thing to look at.

```
primary serves stream 7213... and we have no position that names a stream: discarding and
replaying from zero
```

Either this replica is new, or its `repl_state.txt` was written by a build older than #101 and so
names no stream. Expect it exactly **once** per replica on the upgrade — the identity is saved with
the position from then on — and once for every replica you add. Before #162 there was a third way
here, and it was a defect: the file was rewritten in place every ten seconds, so a replica killed
between the truncate and the write came back with an empty one and streamed the whole log again.
It is replaced whole now, so a kill keeps the previous file.

```
primary serves stream 4471... and our position belongs to 7213... - a different WAL at the same
address: discarding and replaying from zero
```

The primary's data directory is not the one this replica was following. **The address is not the
identity, deliberately** — this is what a primary rebuilt from a bare disk, restored from a backup,
or replaced by a different node reusing its address looks like from here, and in all three the
replica's position indexes a WAL that no longer exists. The full re-sync is correct and expected;
the operational consequence worth knowing in advance is that **restoring a primary from a backup
costs every replica a complete re-sync**, because a restored data directory draws a new identity.

```
primary 10.0.0.2:9090 did not name its stream - a pre-#101 primary, so what we hold cannot be
attributed to it: discarding and replaying from zero
```

The primary is older than this version. Every connection attempt to it waits five seconds for an
answer that is not coming and then re-syncs in full. Not a refusal and not data loss — a delay, for
as long as the two versions are mixed. Upgrading the primary ends it.

**A failover is still a full re-sync, and it appears above as the second line rather than as an
omission.** The identity belongs to a data directory: a promoted node writes to its own WAL, so the
position a replica held in the old primary's log indexes nothing in the new one. It is also the
right answer for a reason that has nothing to do with byte offsets — the promoted node may be
*behind* this replica, and a replica that kept its own extra records would serve rows the primary
does not have. Expect every surviving replica to discard and re-stream after a promotion, exactly as
before. What changed is the restart of a replica whose primary did not move, which is the common
case and was paying the same price.

### Which epoch a replica thinks it is in, and the one line that has two readings

A replica reports the epoch of the primary it follows — `REPLICA 10.0.0.2:9090 9` — and keeps that
number across a restart and a role change. Before #103 it reported `0` while following a primary in
epoch 9, and the number on the wire was 0 too, which left both epoch guards inert on a first
connection: `ERR STALE_PRIMARY` because zero is never greater than anything, and the replica's own
record filter because nothing is below zero.

The epoch **only ever moves forward**, and that is what makes the guard a guard. One line follows
from it:

```
stale record epoch 5 < the 9 this node knows, disconnecting
```

It has two readings and the log cannot tell them apart, so decide by looking at the cluster:

- **The primary really is superseded.** Another node holds the role in a higher epoch, and this
  replica is refusing records from a node that has not noticed. Nothing to fix here: point the
  replica at the current primary, and find out why the old one is still streaming (#82 makes a node
  that loses its lease demote itself, so a node still serving is a node that thinks it holds one).
- **This data directory belongs to a cluster that had progressed further** — a copied directory, a
  node moved between clusters, a restore from a backup taken elsewhere. The epoch is a fact this
  node remembers correctly about a cluster it is no longer in, so it refuses forever and re-connects
  on a loop. **Give it an empty data directory**, which repurposing already required: a foreign
  directory also fails the stream-identity check above and has its store wiped, so nothing is being
  preserved by keeping it.

The number's homes, in the order they are consulted: a promotion writes an `EPOCH` record to this
node's own WAL, so a node that ever held the role restores it on `open()`; `repl_state.txt` carries
`epoch=` for a node that has only ever followed, written the moment it changes rather than on the
ten-second timer; and within a process it lives in one place, so a role change keeps it. A
`repl_state.txt` written before #103 has no such line and simply leaves the epoch where the WAL put
it.

## When a mesh peer falls behind

Two gauges, and reading either one alone is the mistake this section exists to prevent.

```
curl -s localhost:9091/metrics | grep -E 'ob_mm_(replication_lag_records|peers_position_unknown)'
# ob_mm_replication_lag_records 0
# ob_mm_peers_position_unknown 0
```

`ob_mm_replication_lag_records` is **how many records the furthest-behind peer is known to be
missing**. Records and not bytes: sequence numbers in this engine are per-origin and
origin-stamped, so two nodes can compare them, and byte offsets cannot be compared at all because
each node's WAL holds its own client writes as well as everything it replicated.

`ob_mm_peers_position_unknown` is **how many connected peers have not said what they hold**. Those
peers are excluded from the gauge above, because the comparison reports a peer that has said
nothing as holding *nothing* — deliberately, since sending it everything is the safe direction for
a repair — and counting that would make silence the largest lag in the mesh.

So the readings are:

| lag_records | position_unknown | what it means |
|-------------|------------------|---------------|
| 0 | 0 | converged, as far as the last comparison could see |
| > 0 | 0 | a peer is behind by that many records |
| 0 | > 0 | **we do not know**, which is not the same as converged |
| > 0 | > 0 | a peer is behind, and separately there is a peer we cannot assess |

**Both are recomputed once per anti-entropy pass and nowhere else**, so their freshness is
`--anti-entropy-interval-seconds` — thirty seconds by default. They are not per-write numbers and a
scrape a second after a partition starts will still read the previous pass.

**What replaced what, if you have a dashboard from before this release.**
`ob_mm_replication_lag_bytes` was registered and never written, so it read a flat zero for its
whole life (#117); it is now **removed from the registry** rather than fed, because the mesh has no
byte position to compute it from (#118). `STATUS`'s `replication_lag_peer_<id>` lines are gone for
the same reason — they carried this node's own WAL offset minus a byte position in the peer's own
WAL frozen at handshake, which on a converged mesh equals this node's WAL size. And `MM_PEERS`'
`lag_bytes` column is now `send_queue_bytes`, which is what it always held.

### Catching a peer up

A peer that reconnects is caught up by every node that holds something it lacks, and each says so
twice: once when it starts, with what the peer's vector said —

```
Starting catch-up to peer 3 (connection 3): vector entries=11 received=1 truncated=0, 11 (symbol, origin) range(s) it lacks; reading this node's WAL from its first record, 8388608 bytes a round
```

— and once when it has read to the end of its WAL:

```
Catch-up to peer 3 finished in 17 round(s), 3.6 s: read=310212 record(s) (46532932 bytes) sent=310202 skipped_peer_has=0 skipped_type=10; reading took 131.4 ms off the lock, and the longest a round held it 4.6 ms
```

and, while it runs, every ten seconds:

```
Catch-up to peer 3 under way: 4 round(s), 62 s, read=84260 record(s) (12639000 bytes) sent=66415 skipped_peer_has=17845, at file 0 offset 12639000, send_buf=1446885
```

Since #178 a catch-up is **rounds**, each reading at most `--mm-max-catchup-bytes` (8 MiB) of this
node's WAL from where the last one stopped, and pausing while the peer's send buffer is at the
snapshot's low watermark. The reading is done without the lock every local write takes, so "the
longest a round held it" is the most a catch-up added to a write on this node. A catch-up that is
long in rounds and short in records sent is one passing over what the peer already holds; one that
never finishes while the peer's buffer stays full is a slow peer, which the send-buffer ceiling above
drops. Before #178 a catch-up past `--mm-max-catchup-bytes` said it was "falling back to snapshot
sync" and stopped sending; nothing sent a snapshot, and the peer never got the rest.

`ob_mm_catchup_rounds_total` and `ob_mm_catchup_records_sent_total` count the work.
`ob_mm_backpressure_snapshot_total` is **removed**: it counted peers dropped by the check that
preceded the rounds, under a name that promised a snapshot nothing sent.

**A vector past 1 560 entries arrives in parts** (#177), and the receiver says so when the last part
completes it:

```
Peer 3 version vector: entries=5000, in parts
```

`truncated=1` in the line a catch-up starts with means the peer could not state what it holds - since
#177 a peer of an older build past 1 560 entries, or one past a million, which says so at `WARN` -
and such a peer is sent everything retained, a round at a time. Before #177 every node past 1 560
entries was one: each reconciliation resent the whole retained WAL, and a node that joined its mesh
never asked for a snapshot.

**One warning means a peer lacks what no catch-up here can send:**

```
Catch-up to peer 2: 10 (symbol, origin) range(s) it lacks begin before this node's retained WAL - e.g. MC07.EX origin 3 from 1 - and no catch-up from here can send them
```

The peer is missing records that this node holds only in segments — its WAL no longer reaches back
to them — and catch-up sends only from the WAL. It comes once an episode, however many
reconciliations find the same ranges, and `ob_mm_catchup_unfillable_total` counts the ranges at
every catch-up. Anti-entropy that could repair it is #57 and not built; a peer that holds nothing
takes a snapshot when it joins, so wiping and re-joining one is the repair there is. Until #185 there
was a known way to produce this warning without a real gap: a node restarted with segments of a
symbol only its peers write declared its own frontier from the highest number in them, and the ranges
it then said its peers lacked were ranges nobody wrote - every reconciliation read the whole retained
WAL for them and counted them again. A segment records this node's own highest number since, and a
start declares that far and no further. Until #179 the replay did the same for every record it
replayed.

### A node that joins a mesh

A node that holds nothing asks one peer for a snapshot when that peer's vector says it holds
something, and refuses writes from that moment until the snapshot is installed (#188):

```
Bootstrap started for node 4 — writes are refused with ERR BOOTSTRAPPING until finish_bootstrap() is called
Asked peer 3 for a snapshot: this node holds nothing, and the peer reports 1600 version-vector entries
Bootstrap from peer 3 begins: metadata=1330545 bytes (manifest=1263311 vector=67234 held=0), staging='...'
Bootstrap from peer 3 complete: files=12800 bytes=2012044 rows=1600 in 5.3 s
Bootstrap finished for node 4 — accepting writes
```

`ob_mm_snapshot_requested_total` on the joiner, `ob_mm_snapshot_sent_total` on its peers and
`ob_mm_snapshot_received_total` on the joiner each go up by one. A peer that cannot serve it -
`busy` with another joiner - says so, and the next peer that states its vector is asked at once; so
is one after the peer asked drops its connection, has not answered in ten minutes, or sent a snapshot
the joiner abandoned, which tells that peer to stop sending (#192). Each peer is asked once a
bootstrap (#191), and with no peer left that it has not asked the joiner takes writes again, and says
so once:

```
No peer gave node 4 the snapshot it asked for (refused; 3 asked); accepting writes until one can - the next peer whose vector says what it holds is asked
```

**A store of 65 535 files or more, or 8 MiB of manifest** - about 8 000 to 10 000 segments - goes
only between nodes of a build with #176, which say in their request that they take it:

```
Peer 4 requested a snapshot (connection 12, takes wide chunks)
Snapshot begins towards peer 4: files=65608 bytes=... meta=... (manifest=... vector=... held=0) in 32-bit chunks, created in ... ms
```

A joiner of an older build is refused it with the reason, and so is any joiner by a peer of an older
build (`too_many_files`, `metadata_too_large`, and on the peer `Refusing snapshot for peer 4: 65608
files cannot be addressed by a 16-bit index (limit 65535) - it asked as a build before #176 does`).
So in a mesh being upgraded, upgrade the peers a joiner will ask before the joiner, and let a node of
the new build be the one that joins; a small store goes between any two builds as it always has. A
joiner of this build that no peer can serve asks each once and takes writes holding nothing (#191),
and asks again at the next vector while it still holds nothing - so it bootstraps once a peer of this
build is up, as long as no client wrote to it in between. A joiner of a build with #188 and without
#191 asks such peers in turn for as long as it runs, refusing every write.

A `SNAPSHOT_BEGIN` the joiner did not ask for is refused (`Snapshot aborted towards peer 2:
not_requested`), and so is the one it asked for if it holds data by then (`holds_data`); a sender
refused this way ends its transfer (`Snapshot to peer 4 ended early ...: peer_refused`). Until #188 a
joiner asked every peer at once and installed each snapshot that arrived after a bootstrap had
finished, one after another - on three peers, writes refused 21.6 s where one bootstrap took 6.8.

### A symbol two nodes wrote before #184

Before #184 one counter per symbol minted every origin's numbers, and was raised by every origin's -
so when two nodes wrote one symbol, each node's numbers for it had holes where the other's were. A
frontier is "everything from this origin up to here", so every node's frontier for such a symbol
stopped at the first hole, and a node that missed writes was compared against a vector that said it
lacked nothing: it never got them. The numbers a node mints now are its own, without holes, and a
restart continues them from its own highest; **the holes already in the data stay**, and no frontier
passes them. A node holding such a symbol says so, once until the frontier moves again:

```
Held set full: key=BTCUSDT.BINANCE origin=2 frontier=500 high_water=11000 - 4096 numbers above a hole nothing is filling; a redelivery past them is stored twice
```

The held set is what a node keeps of the numbers above a frontier, until the hole under them fills;
at its cap (4 096 a symbol and origin) it stops growing, and a number past it that arrives again is
stored again. Nothing fills a hole the old numbering left, so the line is the sign of one.

**Upgrading a mesh across #184** closes that numbering, once (#187). Left as it was, the stuck
frontiers did worse than hide what a node missed: after an outage every catch-up sent the numbers
above them, the ones past a held set were stored again, and measured on a mesh upgraded with a symbol
two nodes wrote, the writers held 11 904 and 11 404 rows and the node back 16 232 where 11 000 were
written. A mesh node whose data directory has segments from before #184 declares, at its first start
on a build with #187, every origin of every symbol it holds up to 2^48 - 1, and numbers its own from
2^48 on; a record numbered past that from an origin closes that origin wherever it arrives. It says
so once, and notes it in `numbering_closed` in the data directory, which keeps a later start from
doing it again:

```
Closed the numbering of 2 symbol(s) written before per-origin numbers (#184, #187): 3 frontier(s) of their origins declared up to 281474976710655, and this node's own numbers of them go on from 281474976710656 - a record missing here below that now is not caught up
```

That last clause is the procedure: a record a node really lacks when it closes is declared held and
never sent again. So upgrade such a mesh with writes stopped:

1. Stop the writes, and wait until every node holds every row - the same count of every symbol on
   every node.
2. Stop every node, start every node on the new build - in any order, as long as nothing writes.
3. Check each node said `Closed the numbering ...` (or, started again, `... closed at an earlier
   start`), and resume the writes.

A mesh upgraded with writes in flight mixes the numberings as a mesh before #184 did, and the rows of
such a symbol can be stored again as measured above. A node that joins afterwards takes the closed
numbering with its snapshot; do not remove `numbering_closed` from a data directory, because a start
without it closes again, and would take the records of a node that joined since - numbered from 1 -
for ones it holds.

**How far behind a replica is** is a different question with a different answer, in the section
below.

### Downgrading a mesh node across #189

Since #189 a checkpoint writes down the entries of the version vector that moved since the last one,
and a whole vector only where a restart needs one - a build before #189 skips the changes, as it skips
any record type it does not know, and restores the last whole vector alone. So a node goes back to an
older build **from a clean stop**: a stop that wrote changes since its last whole vector writes a whole
one before it ends, and that is the vector the older build reads. Started on the older build after a
crash instead, the node restores its frontiers as of its last whole vector - behind what it holds -
and its peers' catch-up sends it again what moved since.

## When a replica falls behind

Two gauges again, and the same rule: read the pair.

```
curl -s localhost:9091/metrics | grep -E 'ob_repl(ication_lag_bytes|icas_lag_unknown)'
# ob_replication_lag_bytes 4096
# ob_replicas_lag_unknown 0
```

`ob_replication_lag_bytes` is **bytes the furthest-behind replica has yet to acknowledge**, counted
across WAL files. Bytes are meaningful here where they are not for the mesh, and the difference is
worth knowing: a replica streams *this* node's WAL and acknowledges into it, so the two positions
index the same log. A mesh peer's position is in its own WAL, which is why the mesh reports records
instead (see the section above, and #118).

`ob_replicas_lag_unknown` is **connected replicas whose lag cannot be measured**, because a WAL file
between their position and ours is gone. That is not merely an unmeasured distance. Retention keeps
files back to the slowest connected replica, so a missing one says **that replica can no longer
catch up from this log** and will need a snapshot. A nonzero value here is more urgent than a large
value in the gauge beside it.

`STATUS`'s `[replicas]` block says the same thing per replica, and prints `lag=unknown` rather than
a number for that case:

```
replica[0]: 10.0.0.1:7000 file=3 offset=128 lag=4096
replica[1]: 10.0.0.2:7000 file=1 offset=64 lag=unknown
```

**Why it is not zero.** Zero is a real answer — a replica that is caught up is zero bytes behind —
so a zero standing in for "cannot be measured" would say the opposite of the truth. That is exactly
the defect this number had until #123: the lag was computed as the difference of two byte offsets
with the **file index ignored**, and a WAL rotation resets the current offset, so a replica more
than one file behind reported **zero bytes behind**.

**What it does not promise.** The distance is exact, but it is sampled once per replication loop
pass, so a scrape immediately after a burst of writes can read the previous pass's value.

The cross-file behaviour is pinned by a running cluster since #124 made the rotation threshold a
flag: `tests/integration/test_wal_rotation.py` stops a replica with `SIGSTOP` — connected, counted,
acknowledging nothing — writes past three rotations, and requires this gauge to read **more than a
whole file**. That is the number the arithmetic before #123 could not produce. It requires
`ob_replicas_lag_unknown` to be zero in the same breath, because the two are one fact from two
sides: the distance is measurable precisely because retention kept the files the stopped replica
still needs.

## When a peer's clock is wrong

Two numbers answer this, and you need both.

```
curl -s localhost:9091/metrics | grep ob_mm_hlc_drift
# ob_mm_hlc_drift_ns 3599999999788
# ob_mm_hlc_drift_excursions_total 1042
```

`ob_mm_hlc_drift_ns` is **how far** the hybrid logical clock's physical component is ahead of this
node's wall clock, and it is a **peak that never comes down** — nothing lowers that component, so
the gauge records the worst moment for the life of the process.
`ob_mm_hlc_drift_excursions_total` is **how often** a tick has found the drift over a second, so
the two together separate a single excursion from a clock that is permanently out. The gauge alone
cannot: a ten-second blip and an hour of skew read identically once the blip is over.

In the log you get two lines per excursion and not one per write (#120):

```
WARN hlc HLC drift exceeds 1s: drift_ns=3599999999788 — this clock is ahead of the wall clock,
         which a peer's timestamp can do and nothing undoes; further ticks are counted in
         ob_mm_hlc_drift_excursions_total, not logged
WARN hlc HLC drift is back within 1s after 1042 tick(s): drift_ns=812443
```

**What the engine promises.** The clock never goes backwards, whatever a peer sends — that is #119,
and it is the property everything else here rests on, because last-writer-wins compares these
timestamps. Two nodes whose clocks drift in **opposite** directions still agree on the winner of
the same conflict, and still agree after each has merged the other's timestamp.

**What it does not promise, and this is the part worth knowing before you need it.** A record from a
peer whose clock is ahead moves this node's clock forward, and **nothing moves it back** — not a
restart of the peer, not fixing the peer's clock, not a restart of this node once the value is in
its WAL. Because this node then stamps its own writes with that clock, the value spreads to every
peer that receives one. So a single misconfigured clock becomes the cluster's clock and stays.

The mesh stays consistent while that is true: every node adopts the same value, so ordering is
total and conflicts resolve the same way everywhere. What you lose is the timestamps meaning a
time. The practical consequence is that `ob_mm_hlc_drift_ns` on a healthy node tells you a peer's
clock was wrong **at some point**, not that anything is wrong now — and the only way back to
timestamps that track real time is to restart the cluster with the offending clock fixed and the
WAL rotated past the poisoned records.

Whether the engine should refuse such a timestamp is recorded as roadmap #121 rather than decided:
declining it would keep the clock meaningful and break the guarantee that a record which caused
another has an earlier timestamp than it, for exactly the peer whose clock is wrong.

Run NTP on every node in a mesh. This is not advice the engine can enforce for you.

## When etcd is unreachable

Measured on a two-node cluster, i3-7100U, with the coordinator stopped for 30 s (#54 stage B):

- **Nothing exits.** Losing the coordinator costs writes and role changes, not processes.
- **Reads keep being served** on both nodes, whatever their role.
- **The holder gives the primary role up after 2.28 s.** Its lease keepalive is what fails first,
  and a holder that cannot renew its lease is holding a claim that has expired wherever etcd is —
  so it demotes rather than waiting out a TTL. The cluster then has **no** primary and refuses
  writes until the coordinator is back. That is the intended trade: two nodes both believing they
  are primary is the failure this gives up availability to avoid (#82).
- **No replica promotes itself.** A read that failed is not a vacant key. This is the part worth
  knowing in advance, because from the outside it looks identical to a failover that should have
  happened and did not.

**What the log says, and it is one episode rather than one line a second.** Each condition that
holds for the length of the outage is logged **once when it starts and once when it ends**:

```
WARN  failover  the coordinator is unreadable, so this node is staying REPLICA rather than
                campaigning on a read that failed — this is a decision, not a stall, and no
                primary will be elected until the coordinator answers
WARN  failover  cannot publish this node's WAL position (file=0 offset=32); it will be invisible
                to an election until the coordinator answers again, and this line will not repeat
                while that lasts
…
INFO  failover  publishing this node's WAL position works again after 19 failed attempt(s)
INFO  failover  the coordinator answers again after 19 tick(s) of declining to campaign
```

The first of those is the line to look for, because it is the answer to the question: the engine is
**deciding**, not stuck. The holder's step-down is separately visible at the default level — four
lines, once, naming the mechanism.

It did not read like this until roadmap **#115** and **#116**. Measured before them, over a
30-second outage: **2.2 lines a second per node**, of which 60 of 65 were two sentences repeated
once a second, and **zero** mentioned the decision not to campaign. One of those repeated sentences
also said the position was being *published without a lease* immediately before the line saying the
publish had failed — nothing was written, so nothing outlived anything. That warning now appears
only where it is true: on a tick where the position **was** published and published without a
lease, which is the case in which it will not expire when this node dies (#72).

### With more than one coordinator endpoint, the registry follows the one that answered

`--coordinator-endpoints` takes a comma-separated list and the node probes it **in order**, keeping
the first that answers `/v3/maintenance/status`. Everything a node does with etcd goes through that
one endpoint — the lease, the leader key, and since #135 the three calls the peer registry makes:
publishing this node's address, reading its own key back, and the topology poll that learns peers.

Before that fix the registry addressed the **first configured** endpoint regardless, so a list whose
first entry was down produced a node that looked healthy in every way an operator checks — election,
keepalive and failover all working through the live endpoint — and was never in the registry at all.
Measured: registered in 0.5 s with the order reversed, never within 25 s with the dead entry first,
and **no** `Lease refresh failed` line, so the recovery from #132 could not see it either.

One line is worth knowing, because it belongs to a state no other message covers:

```
WARN  peer_registry  node 2 cannot poll for peers: no coordinator endpoint has answered yet, so no
                     peer will be learned until one does
```

That is **none** of the configured endpoints answering, not one of them — the topology watch starts
whether or not the initial registration succeeded. It arrives **once** rather than every 100 ms, and
a matching `INFO` says how many polls it covered when an endpoint finally answers. If you see it,
the thing to fix is etcd or the endpoint list, and the rest of this section applies.

### When a node is serving but is not in the registry

A node keeps its place in the mesh registry by refreshing an etcd lease every TTL/3 (default: every
3.3 s at a 10 s TTL). If that lease is lost — etcd forgot it, or the refreshes failed for longer
than the TTL — the key under `<prefix>mm_peers/<node_id>` expires. **Since #132 the node writes it
again**, but only after reading the key back and finding it gone; measured by revoking the lease
under a running two-node mesh, the key is absent at 0 s and 2.5 s and **present again from 5.0 s
onward**, one refresh interval later. Before that fix it was gone for the life of the process — the
same measurement said still gone 22.5 s later, while the node answered `PING` on every sample, and
the only recovery was a restart.

What survives the window, and it is more than you would expect. Links that were already dialled keep
carrying writes, and a node that starts *later* still ends up connected, because the unregistered
node's own topology watch sees the newcomer and dials **out**. So this is not a partition. What is
lost until the key returns is the node's address as published to the cluster: its row in every
peer's `MM_PEERS` has an **empty address**, and anything that needs to look it up cannot.

The log says it **once**, with the consequence in the line, and then says the repair:

```
WARN  peer_registry  Lease refresh failed for node 2 - this node's mesh registration expires with
                     the lease, and it will be written again once the key is confirmed gone
WARN  coordinator    lease 328... is gone: keepalive returned no TTL, so etcd does not know it any
                     more
INFO  peer_registry  node 2 was missing from the registry and has registered again
```

One line each, not one per interval — before #133 this was eleven of each in 33 s, from two
components neither of which was writing more than one line per attempt. The third line is the one
to grep for: without it, the two WARNs above mean the repair has not happened yet.

**Two cases where the repair deliberately does not run**, and both are quiet on purpose:

- **etcd is unreachable.** The read cannot tell an absent key from a failed read, so it answers
  neither and the entry is left alone. Nothing is written at `INFO`; the condition is already
  reported by the WARN above, and a second line per interval saying "still cannot tell" is what
  #133 was. Recovery here is recovery of etcd, which is what the rest of this section is about.
- **someone deleted the key while the lease is still alive.** The node is invisible, but its
  refresh still succeeds, so this path never runs. Eviction by deleting the key therefore still
  works, which is why it is left this way; if you did not do it deliberately, restart that node.

### When a peer is refused for its clock

Since #121 a mesh node refuses a peer whose HLC physical component is more than **five minutes**
ahead of its own wall clock, and drops the connection rather than taking that time:

```
WARN  mm  peer 2 says its clock is 3600 s ahead of ours, over the 300 s bound — dropping the
          connection rather than taking that time. Absorbing it would make it this whole mesh's
          clock for as long as that node keeps writing, and nothing brings it back. Its data will
          not arrive until its clock is fixed; MM_PEERS still shows what it claims
INFO  mm  peer 2's clock is back inside the bound after 41 refused record(s); its records are
          being applied again
```

One line per episode, `ob_mm_peer_dropped_clock_total` on every refused record — so the counter is
the rate and the log is the condition. `MM_PEERS` keeps the peer's claimed `hlc_timestamp`, which is
recorded before the verdict on purpose: the number that got it dropped is the number you need.

**What to do.** Fix that node's clock — this is NTP's job — and the link returns on its own; nothing
has to be restarted. Until then that peer's writes do not reach this node, which is the price and it
is deliberate: absorbing the time would make the whole mesh run at the broken clock for as long as
that node kept writing, and no restart of *that* node brings the others back.

**Why the bound is loose.** Five minutes is four orders of magnitude above what working NTP holds
and above a VM suspend, and far below the class it is drawn to exclude — a hand-set date, a dead
RTC, a host that never had NTP. A peer four minutes ahead is still absorbed and still pins the mesh
four minutes ahead; that is what `ob_mm_hlc_drift_ns` is for, and it is still a peak that never
falls. The bound is not there to make the clock a wall clock. It is there because the mesh's clock is
the **maximum** of its members' clocks and nothing bounded the maximum.

**What is not affected, so you can stop looking:** row timestamps. A row carries the client's
`event_time_ns` since #105, or this node's `system_clock` when the client did not send one — never
the HLC. Retention and time-range queries read those, so a drifted mesh clock never moved them.

### When a node holds the leader key and serves nothing

A promotion has two durable effects: the leader key in etcd and an epoch record in the WAL. If the
second is refused — a full disk is the measured case — the node has won the role and cannot act on
it. Since #130 it says so, once, and keeps trying:

```
ERROR failover  this node holds the leader key at epoch 2 and cannot finish becoming primary:
                <reason>. It accepts no writes while this lasts, and it will keep the key and keep
                trying; kill it to let a peer take the role at a higher epoch
INFO  failover  finished the promotion this node had already won, epoch=2, after 7 tick(s)
```

The second line is the one to wait for: the retry uses **the epoch already in the key**, so a disk
that recovers finishes the promotion without another election. `ob_monitor_errors_total` moves on
every failed attempt while the `ERROR` arrives once, so the counter is what to alarm on.

**What the node looks like meanwhile.** `PING` answers. `ROLE` reports `REPLICA` with an **empty**
address, which is the truthful answer available — there is no primary to name, neither the node it
stopped following nor itself — and writes are refused because the engine is still read-only. Before
#130 this state reported `REPLICA <its own replication port>` and, worse, **renewed the lease**, so
the key stayed alive and no peer ever took over.

**If the disk will not recover, kill the node.** That is not a workaround: the lease then expires,
the key goes, and a peer wins the role at a **higher** epoch, which is exactly what the node holding
the key at the lower one makes safe. Releasing the key from inside the failed promotion would not
be — a peer that never saw that key computes the same epoch again, and the record that it was
consumed would be gone.

### When a subsystem's loop keeps failing

**Every loop in the engine guards one iteration at a time** (#112, #131), so an exception costs an
iteration rather than the thread. The counter is the thing to alarm on, because nothing else outside
the process changes when one of these fails:

| counter | what stopped working while the node stayed up |
|---|---|
| `ob_flush_errors_total` | rows are not reaching segments and the WAL is growing |
| `ob_monitor_errors_total` | role changes are not being acted on |
| `ob_mm_io_errors_total` | mesh events are being dropped — a peer's row can still say `connected` |
| `ob_repl_io_errors_total` | replica connections, catch-ups or heartbeats are being dropped |
| `ob_peer_lease_errors_total` | the lease above is not being refreshed |
| `ob_loop_errors_total` | one of the other seven: the topology watch, the mesh reconnect loop, anti-entropy, either shard watch, the metrics server, or a client pool's health check. **The log line names which** — one counter rather than seven because all seven ask for the same thing, where each of the five above asks for something different |

Each one's log is an **episode**: one `ERROR` when it starts, one `INFO` when it ends, and the
repeats at `DEBUG`. Silence after the `ERROR` therefore means the condition is still there; the
`INFO` is what says it went away.

**The last two counters have no path in this engine that is known to reach them, and neither does
`ob_loop_errors_total`.** They are there so that the *first* occurrence is visible rather than
silent: before these boundaries existed, a thread that ended took its subsystem with it and nothing
outside the process changed at all. Two of the seven — the shard router's watch and a client pool's
health check — run in **your** process rather than the engine's, so they have no counter to feed and
the log line is the whole report there.

## Loading history, and what its own timestamps change

Since #105 a write can carry the time it happened (`INSERT … [event_time_ns]`), which is what makes
a backfill land where it belongs instead of at the time of the import. Three consequences, each
checked in the code rather than assumed:

- **Retention counts by the record's own time, per segment.** `--ttl-hours` compares a segment's
  newest event time against **the node's wall clock minus the retention**, so history loaded with
  its real timestamps arrives **with its age**: a backfill of last year into a node with a 24-hour
  TTL is expired on the next sweep. The inverse is also true and stranger — one row dated in the
  future keeps its whole segment alive. *Newest* has been true since #166: until then it was the
  segment's **last-written** row, so a current row written before an older one went with it —
  measured, 0 of 2 rows after the first sweep under `--ttl-hours 24`, one of them a second old.
  The wall clock is the one a row is stamped with on arrival,
  so a clock **stepped forward expires early by the step** and one stepped back keeps rows longer;
  how often the sweep runs is on the monotonic clock and does not move with it.
  Until #163 the cutoff came from the clock that counts from the machine's boot. A node whose
  retention was longer than its machine had been up deleted **every** segment at its first sweep —
  measured, 200 rows of 200 after a restart with `--ttl-hours 24` on a machine up 21.5 hours — and
  one up for longer than its retention never expired anything.
- **Query pruning is exact about time, and costs more when rows arrive out of order.** A segment is
  skipped when its `[start, end]` range cannot intersect the query's, and since #166 that range is
  the earliest and the latest row in it — so rows arriving out of order widen segments and more of
  them are read, but no segment is skipped that holds a row the query asks for. **Until #166 this
  bullet said the same and was false**: the range was the start of the hour the segment's first row
  fell in and the time of its last row, so a row written out of order could fall outside it, and a
  `SELECT` answered `OK` without it. A node upgraded from a build before #166 corrects the ranges of
  the segments it already has at its first start, reading each one's timestamp column once — one
  line in the log says how many there were, how many held rows outside their recorded range and how
  long it took — and never again; see "Upgrading a data directory written before #166" below.
- **Conflict resolution is untouched.** Multi-master last-writer-wins compares the HLC, which comes
  from the node's clock, not the record's `timestamp_ns` — so a client choosing a time **cannot**
  decide which of two conflicting writes survives. That was the one real risk in the change, and it
  is the reason the field could be added at all.

## Upgrading a data directory written before #166

A segment written before #166 recorded the wrong time range whenever its rows reached a flush out
of time order (#166 in the roadmap). The first start of a later build finds every such segment by
its `meta.json` — the ones without `"time_range":"rows"` — reads its `ts.col` once, and gives it its
rows' range: in the index always, and on disk in batches of 4096, each batch written beside the old
files, synced with one `syncfs()` and only then renamed over them. So a crash or a power cut in the
middle leaves every segment with its old `meta.json` or its new one, and the next start finishes the
job; a leftover `meta.json.range` is the trace of an interrupted batch and is removed at start.

```
{"component":"columnar","msg":"106752 segment(s) written before #166 were read for their time range in 87131 ms: 0 held rows outside the range they recorded, 106752 repaired on disk, 0 only in memory, 0 unreadable"}
```

That one is measured, on an m9g.xlarge with its data on EBS and a cold page cache: 106 752 segments
written by a build before #166, whose cold start took 82 s to read their `meta.json` files and whose
first start on the new build took 170 s — the repair is roughly one more cold read of the index, once,
and the start after it took 82 s again. A node that ever wrote rows out of time order has a non-zero
`held rows outside`, and names the first such segment in the parentheses.

`held rows outside` is the number that matters: those are the segments queries and retention were
wrong about. `only in memory` counts corrections that could not be written — a read-only directory,
a full disk, a failed sync — and those are repaired again at the next start. `unreadable` means a
segment without a readable `ts.col`, which no query could read anyway. The cost is one read of every
old segment's timestamp column, once; the start after it reads none.

**A downgrade after the repair is safe to read**: the column files are untouched and the format
version is still 2, so an older build reads the corrected `meta.json` and prunes and expires by the
right range. What it does differently is replay's fallback for a symbol with no trusted WAL
position, which in the older build compares with the recorded end — now the newest row rather than
the last one.

## How long a start takes, and what makes it longer

A start reads the `meta.json` of every segment in the data directory, and reads the WAL once - and
then again what its last checkpoint does not cover - before it answers anything. Since part 2a of #165 a symbol gains a segment when it is **due** —
65 536 rows, or ten seconds after its oldest waiting row — rather than on every flush tick, so at a
steady write rate a node gains about one segment per active symbol every ten seconds. Since part 2b
the flush tick merges them ([merging segments](#merging-segments)), so the count follows the data
rather than uptime. `ob_segment_count` is the number to watch.

What a start costs depends on what the page cache kept. Measured on an m9g.xlarge with its data on a
gp3 volume, after a soak writing 256 symbols for 90 seconds and a clean stop:

| page cache | before part 2a: 141 312 – 143 104 segments | since part 2a: 2 304 segments |
|---|---|---|
| warm — a restart of a process whose files are still cached | 3.25 – 3.33 s | **1.50 – 1.51 s** |
| cold — after a reboot, or on a new instance | 107.5 – 109.5 s, 1.42 – 1.44 GiB read | **4.38 – 4.42 s**, 291 – 293 MiB read |

A start's own log splits it: on the build before part 2a, **2.12 s** to open 142 592 segments
together with a first pass over the WAL, **0.96 s** for a second pass, and 0.24 s to listening. So a
segment costs about **13 µs** warm and **0.75 ms** cold, where the disk, not the engine, is the time
— and two passes over a 274 MB WAL cost about 1.2 s warm either way. With the segment count down,
the WAL is most of what a start reads (#174). A restart in between can land anywhere: one in part
1's run read 63 MiB from storage and took 12 s. If a node's start time matters, plan with the cold
figure.

After twenty minutes of the same soak, merging is what the count follows:

| page cache | part 2a: 30 464 – 30 720 segments | since part 2b: 2 213 – 3 509 segments |
|---|---|---|
| warm | 2.77 – 2.81 s | **2.45 – 2.49 s** |
| cold | 26.41 – 26.91 s, 693 – 695 MiB read | **5.89 – 6.83 s**, 456 – 471 MiB read |

Of the cold start, opening the index took 22.5 – 23.0 s before part 2b and 1.9 – 2.9 s since; the
rest, 3.8 – 3.9 s in both, is mostly the two passes over the WAL.

**The WAL's part since #174.** Every figure above is from a build that read the WAL at least twice,
a record at a time, and a start of the build just before #174 read it five times: on the i3-7100U,
with one WAL file of 422 MB (1.2 million records of 10 levels) and a clean stop, it took **21.9 –
25.3 s** to listen. The same directory, warm, on the build with #174: **0.14 – 0.17 s**, of which the
one pass over the WAL is 0.13 – 0.15 s - about 3 GB/s. So the WAL costs a warm start about a third of
a second a gigabyte now, and a cold one what the disk takes to read it once.

### After a crash

A crash loses the rows waiting in blocks from memory, not from the node: their records are in the
WAL, retention keeps the WAL back to the oldest one a waiting block needs, and the start replays
them. The last checkpoint then has sixteen bytes — where replay starts, and the seal epoch its sync
covered — and a segment sealed after that epoch is removed and rebuilt, whole or not.

In the soak above, a warm restart after a kill took **1.70 s** — 0.2 s more than after a clean stop,
which is the replay of the rows that waited — against 3.40 s on the build before part 2a.

### Merging segments

Since part 2b of #165 the flush tick merges a symbol's small segments into bigger ones, after
retention and without the lock writers take. Eight segments of one merge level in one symbol's
hour become one of the next level, up to 262 144 rows; a segment of 65 536 rows or more — what a
seal at the write ceiling writes — merges with nothing; and an hour that ended a minute ago and
has received nothing since merges what is left into as few segments as fit. A merged segment ends
in the hour its inputs did, so retention frees it at most an hour after it would have freed the
first of them.

What it costs, and when it runs: a merge rewrites every row it takes, about 50 ns a row on an
m9g.xlarge, so a row is written again once or twice at a steady trickle; a tick that drained more
than 65 536 rows — the write ceiling — merges nothing unless none has for ten seconds, and a lighter
one merges for up to 10 ms. A query reads a segment whole, so a narrow query into history reads a
merged segment of up to 262 144 rows: measured at 3.2 – 3.5 ms for one second of rows, where a
segment sealed at a trickle is read in 0.06 ms.

On the disk a merge is written into `<start>_<end>_<n>.compacting` beside its inputs, synced,
renamed to a segment's name in place of its inputs under the index's lock, synced again, and only
then are the inputs removed — once every query that was reading them has finished. A crash anywhere leaves
the inputs, the merged segment (whose `meta.json` names its inputs in `compacted_from`), or both;
the start removes a working directory and every input it finds beside the merged segment that
names it, and says so once:

```
a merge was cut short: removed 0 working director(ies) nothing published and 8 segment(s) a merged segment beside them had replaced
```

**A merge syncs for itself, whatever `--fsync-policy` says**: it removes segments that were on
the device, so it does so only once what replaces them is. Under `none` that sync — once a second
at most, and only in a tick that may merge — is also what puts a checkpoint on the device, and a
merge takes only the segments a checkpoint there vouches for.

**While a snapshot is being made or sent, nothing merges and retention does not sweep**: the
snapshot's manifest names files, and a merge or a sweep that removed one mid-transfer failed it,
and the replica started again. Both resume at the first tick after the transfer ends. A failed
sync stops merging until a restart, like the checkpoints
([when an fsync fails](#when-an-fsync-fails)).

`--compaction off` turns it off — a valve, not a tuning knob: every segment then stays as its seal
wrote it, and the count grows with uptime again.

### Going back to a build before part 2b of #165

A merged segment is an ordinary segment to a build before part 2b, which ignores the two keys a
merge adds to `meta.json`. What that build cannot do is tell a working directory or a replaced
input from a segment, so it would hold their rows twice. **Only from a clean stop**, which removes
both; after a crash, start once with this build — its start removes them — stop it cleanly, and
then go back.

### Going back to a build before part 2a of #165

**Only from a clean stop.** `close()` seals everything, so the last checkpoint is the eight-byte
form every build reads. After a crash, a build before part 2a reads the sixteen-byte form as saying
nothing and replays from the checkpoint record, which would skip the records of the rows that were
waiting — so start the node once with the newer build, let it replay, stop it cleanly, and then go
back.

## Stopping a node

`SIGTERM` (or `SIGINT`) closes the listening socket **immediately** — a new connection is refused
from that instant — and then waits for the client sessions that are already open. What that wait
costs is bounded by `--drain-timeout-ms`, default **10 s**, and the bound is the part worth knowing:

```
Shutdown requested — the epoll loop will drain and close
Drain requested: closing the listen socket, fd=4
Drain deadline of 10000 ms reached with 1 session(s) still open - closing them and exiting;
raise --drain-timeout-ms, or set it to 0 to wait indefinitely
```

The third line is the one to read. It means the node left with sessions still attached, which is a
statement about your clients rather than about the node: a connection pool, a `SUBSCRIBE` stream
(#45) or a monitoring probe holds a session open indefinitely, and an idle session never ends by
itself. Measured on an i3-7100U: **0.11 s** to exit with nothing connected, and — before this bound
existed — **still running after 60 s** with one idle client attached (#106).

Two consequences for whoever supervises the process:

- **the exit code is 0 in both cases.** A node that cut sessions still shut down cleanly, flushed
  and checkpointed; the count in that WARN line is how you tell the two apart, not the exit status.
- **a bound shorter than your supervisor's is what keeps a stop graceful.** systemd's
  `TimeoutStopSec` defaults to 90 s, so the 10 s default is comfortably inside it; if you raise
  `--drain-timeout-ms` past your unit's timeout, systemd `SIGKILL`s the node and the flush this path
  exists for does not run. `0` keeps the pre-#106 behaviour and has to be asked for.

Clients see a session closed without a reply for whatever was in flight when the deadline passed.
There is no protocol-level "shutting down" message on an established session — a client that needs
one should treat a closed connection during a maintenance window as exactly that, and retry.

## Security

**Everything is off by default, and a node nobody configured is plaintext and unauthenticated on
all three surfaces** — client sessions, the replication link and the multi-master mesh. What is
available is all three authenticating (`--auth-secret-file`, `--cluster-secret-file`) and all three
encrypting (`--tls-client`, `--tls-replication`, `--tls-multi-master`, TLS 1.3), each covered
below. Until then a node's traffic is as private as the network it is on, and the startup log names
every surface that is disabled rather than leaving "default open" in a document.

*(This section said "there is no encryption" until 7 September 2026, which stopped being true with
#30 part three — while the TLS sections below it were already in this same file. A page that
contradicts itself is read by whoever reaches the top of it first.)*

The full posture, including what authentication does *not* buy you, is in
[SECURITY.md](../SECURITY.md).

### Turning on client authentication

Generate a secret per identity and put them in one file, one identity per line:

```bash
sudo install -d -m 700 -o orderbook -g orderbook /etc/orderbook/secrets
printf 'grafana %s\n'   "$(openssl rand -hex 32)" | sudo tee    /etc/orderbook/secrets/clients >/dev/null
printf 'ingest %s\n'    "$(openssl rand -hex 32)" | sudo tee -a /etc/orderbook/secrets/clients >/dev/null
sudo chown orderbook:orderbook /etc/orderbook/secrets/clients
sudo chmod 600 /etc/orderbook/secrets/clients
```

Then `--auth-secret-file /etc/orderbook/secrets/clients`, or `auth-secret-file = ...` in the config
file.

The server **refuses to start** rather than warning, on every one of these:

| Refusal | Why it is fatal |
|---------|-----------------|
| the file is readable by group or world (`mode & 0077`) | a secret every local process can read is not a secret; the message prints the mode it found |
| the file is not a regular file, is empty, or has no credential line | there is nothing to authenticate against, and starting would mean starting *open* |
| a secret is shorter than 32 characters | refuses the cases seen in the wild — a word, or `changeme` |
| an identity appears twice | which secret wins would be an accident of file order |
| the cluster secret is also a client secret | a client holding it can present itself as a replica and stream the whole write-ahead log |

Nothing here prints the secret, and neither does `--print-config`, which shows the **path**. If you
are checking a deployment, `grep` your node's log and your `--print-config` output for the secret;
both should come back empty.

**Only the line terminator is stripped.** Trailing spaces in a secret are part of the secret. A
general trim would make two different files the same secret, and for a secret that is a property
worth keeping rather than a convenience to remove.

### Clients

```python
from orderbook_engine import OrderbookEngine
eng = OrderbookEngine(host="10.0.0.1", port=9090, auth=("grafana", secret))
```

For the C++ client, set `ClientConfig::auth_identity` and `auth_secret`. Both authenticate right
after the banner and before compression negotiation, because the server refuses `COMPRESS` on an
unauthenticated session.

A client configured with credentials against a server that is **not** authenticating fails to
connect, with `auth_disabled`. That is deliberate: believing you authenticated when the server
authenticates nobody is a deployment problem worth an exception.

### Turning on cluster authentication

`--cluster-secret-file` protects the replication and multi-master links. It takes a single line:

```bash
openssl rand -hex 32 | sudo tee /etc/orderbook/secrets/cluster >/dev/null
sudo chown orderbook:orderbook /etc/orderbook/secrets/cluster
sudo chmod 600 /etc/orderbook/secrets/cluster
```

**There is no mixed mode.** Every node in a cluster either has the secret or does not; a node that
accepted a peer without proof would be the state this exists to remove. So enabling it on a running
cluster means a full restart with the file in place on every node, not a rolling one.

### Turning on TLS

Three surfaces, each with its own flag: `--tls-client` for client sessions, `--tls-replication` for
the replication link, `--tls-multi-master` for the mesh. The metrics endpoint has none, for the
reason given below.

The client port and the node links differ in one important way, so they are documented separately:
on the client port the server presents a certificate and the client verifies it. **On a node link
both ends do both**, and there is no way to ask for less.

```bash
sudo install -d -m 700 -o orderbook -g orderbook /etc/orderbook/tls
# A real certificate from your CA, or for a private network a self-signed one:
openssl req -x509 -newkey rsa:2048 -days 365 -nodes \
        -keyout /etc/orderbook/tls/key.pem -out /etc/orderbook/tls/cert.pem \
        -subj "/CN=db1.internal" -addext "subjectAltName=DNS:db1.internal"
sudo chown orderbook:orderbook /etc/orderbook/tls/*
sudo chmod 600 /etc/orderbook/tls/key.pem
```

Then `--tls-client --tls-cert-file /etc/orderbook/tls/cert.pem --tls-key-file
/etc/orderbook/tls/key.pem`, or the same three keys in the config file.

The start is **refused**, not degraded, on each of these:

| Refusal | Why it is fatal |
|---|---|
| any `--tls-*` surface without both files | a flag that quietly meant plaintext is the worst outcome this feature can produce, and it would look identical to working |
| the key readable by group or world | the message prints the mode it found, the same rule as the secret files |
| the key does not match the certificate | otherwise every client's handshake fails with a message an operator reads as a client problem |
| either file unreadable, empty, or not a regular file | there is nothing to serve |
| a node-link surface without `--tls-ca-file` | a node link verifies its peer in both directions; without a trust anchor it would encrypt without authenticating, which leaves the relay below open and looks like protection |

**TLS 1.3 is the floor and is not configurable.** A client offering only 1.2 is refused. That is
deliberate: the version floor is the one setting where "configurable" means "misconfigurable".

**Certificate rotation needs a restart.** Reloading in place would mean two live contexts and a
question about sessions established on the old one; that is a separate item rather than a silent
half-measure. Plan the restart the way you plan any other: one node at a time, and the cluster
keeps serving.

### TLS on the replication link and the mesh

This is the part that closes the man-in-the-middle relay, and it is why the node links get mutual
verification rather than the one-sided kind the client port has. Challenge-response proves that the
peer knows the cluster secret; it does not prove **which connection** the exchange happened on, so
an attacker who can redirect a replica relays both directions and both ends are satisfied. A channel
with an identity is the only thing that stops that, which is what mTLS is.

Each node needs a certificate of its own and the CA that signed the others. One CA for the cluster:

```bash
# The cluster CA. Keep this key off the nodes.
openssl req -x509 -newkey rsa:4096 -days 3650 -nodes \
        -keyout cluster-ca-key.pem -out cluster-ca.pem -subj "/CN=orderbook cluster CA"

# One per node. The SAN is what the *dialling* end verifies, so it has to be the address or name
# the other nodes use to reach this one.
for host in 10.0.0.1 10.0.0.2 10.0.0.3; do
  openssl req -newkey rsa:2048 -nodes -keyout "node-$host-key.pem" \
          -out "node-$host.csr" -subj "/CN=node-$host"
  printf 'subjectAltName=IP:%s\n' "$host" > "node-$host.ext"
  openssl x509 -req -in "node-$host.csr" -CA cluster-ca.pem -CAkey cluster-ca-key.pem \
          -CAcreateserial -days 365 -extfile "node-$host.ext" -out "node-$host.pem"
done
```

`IP:` and not `DNS:`, unless you are sure. **The replication client dials an address and never a
name** — it resolves nothing — so a replica's certificate for `db1.internal` presented at
`10.0.0.1` is refused, correctly, and the message names the certificate. The mesh does resolve
names, so `DNS:` works there; a certificate carrying both entries works everywhere.

Then on every node:

```
tls-cert-file = /etc/orderbook/tls/node.pem
tls-key-file = /etc/orderbook/tls/node-key.pem
tls-ca-file = /etc/orderbook/tls/cluster-ca.pem
tls-replication = true
tls-multi-master = true
```

**There is no mixed mode here either.** A node with `--tls-replication` cannot replicate from a
node without it: the plaintext side sends its `CHALLENGE` where a ClientHello is expected. So
enabling it means a restart of the whole cluster with the files in place, not a rolling one.

#### Which peers count as cluster members

The end that **dials** knows the name it dialled and requires the certificate to cover it. The end
that **accepts** knows only the source address, so it has nothing to compare a name against — it
verifies the chain, and by default accepts any identity the CA signed.

That is exactly right when the CA signs nothing but this cluster, which is what the CA above is for.
It is wrong if you point `--tls-ca-file` at a corporate CA that signs every host in the
organisation: then every host in the organisation may present itself as a replica and stream the
write-ahead log. `--tls-peer-names` is the answer, and it is a mechanism rather than a warning:

```
tls-peer-names = node-10.0.0.1,node-10.0.0.2,node-10.0.0.3
```

An accepted peer's certificate must cover one of those. Entries may be names or addresses; an entry
that parses as an address is matched against `iPAddress` and everything else against `dNSName`, the
same rule the dialling end uses. Get it wrong and the cluster does not form, loudly, with a log line
naming the identity that was presented — which is the failure you want rather than the quiet one.

Which mode is in force is in the startup log, not only here:

```
node-link context ready: cert=... ca=... - any identity this CA signed is accepted as a cluster
member (no --tls-peer-names given), which is true only if this CA signs nothing but this cluster
```

#### Checking that it worked

Two pairs of numbers on `/metrics`, and they answer the question a configuration file cannot. Both
halves of each pair are exported, because the guarantee is the *comparison*: a count of verified
links means nothing without the count it is measured against, and a number an operator has to read
off `STATUS` cannot be alerted on.

| Metric | Read it as |
|---|---|
| `ob_replicas_tls_verified` vs `ob_replicas_connected` | equal means every replication link is mutually authenticated; a gap means a replica is connected in plaintext |
| `ob_mm_peers_tls_verified` vs `ob_mm_peers_connected` | the same for the mesh; the peer count excludes inbound connections still in their handshake, which is what `MM_PEERS` lists too |

Alert on the difference, not on either number: both drop to zero when a link goes away, and both
are recomputed from the connection table on every pass of the loop that owns it, so neither can be
left behind by a disconnection.

Plus one INFO line per connection naming the certificate identity:

```
replica fd=12 from 10.0.0.2:51344 authenticated by certificate: node-10.0.0.2
```

#### The cluster secret and mTLS compose, they do not replace each other

Configure both and both are required. mTLS is an *alternative* to `--cluster-secret-file` in the
sense that a cluster can run on mTLS alone — the certificate proves who the peer is, and it does so
bound to the channel, which the secret cannot. It is not an alternative in the sense of one
switching the other off: two mechanisms combined by AND mean a failure of either is visible, and
combined by OR mean neither can be seen to have stopped working.

### Connecting over TLS

```python
from orderbook_engine import OrderbookEngine
eng = OrderbookEngine(host="db1.internal", port=9090,
                      tls=True, tls_ca_file="/etc/ssl/certs/internal-ca.pem",
                      auth=("grafana", secret))
```

For the C++ client the same three fields are on `ClientConfig`: `tls`, `tls_ca_file`, `tls_verify`.
`PoolConfig` and `ShardRouterConfig` carry them too, alongside `auth_identity` and `auth_secret`, so
a pool and a sharded client reach an authenticated, encrypted cluster the same way a single
connection does.

Verification is on by default and turning it off is a named act (`tls_verify=False`), because a
client that does not verify has confidentiality against a passive observer and **nothing** against a
man in the middle — which is exactly the half authentication alone could not give you.

**Verification includes the name.** The client requires the certificate to cover the address or
hostname it dialled, not merely to chain to a trusted CA. This matters most where it is least
visible: with a private CA that signs your whole cluster, chain-only verification would make node
B's certificate perfectly acceptable for node A, and every check would report success. So a
certificate for `db1.internal` presented on `db2.internal` is refused — and if you connect by IP,
the certificate needs an `IP:` entry in its `subjectAltName`, because an address is matched against
`iPAddress` and never against `DNS:`.

Both misconfigurations, and they fail differently:

| What you forgot | What you see |
|---|---|
| `--tls-client` on the server | the client fails at once with `wrong version number`: the plaintext banner arrived where a ServerHello was expected |
| `tls=True` on the client | the connection **hangs until your client's timeout**, and the server's log says nothing |

The second one is worth knowing in advance. This protocol has the server speak first, so a plaintext
client waits for the banner while the server waits for a ClientHello, and until a byte arrives the
server cannot tell a plaintext client from a slow one. There is nothing to fix there; there is only
knowing it, so that a hang is read as the right thing.

TLS and authentication are for different things and you want both: TLS establishes the channel,
`AUTH` establishes who is on it. Neither substitutes for the other, and a client with credentials
against a TLS-only node still gets `ERR unauthenticated`.

### The metrics endpoint

No authentication, deliberately — a Prometheus scraper cannot perform a challenge-response, and a
bearer token would be a second, weaker mechanism. Bind it where only your scraper can reach it:

```
--metrics-port 9091 --metrics-bind 127.0.0.1
```

An invalid address is refused and the endpoint does not start, rather than falling back to every
interface: an operator who typed a bind address and got `0.0.0.0` has the opposite of what they
asked for.
