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

## Tuning that is real for this engine

Three of the knobs people expect are **not** tuning for this engine, and saying so is more useful
than listing them:

| Knob | Why it is not here |
|---|---|
| `LimitMEMLOCK` | The engine locks no memory. No `mlock`, no `MAP_LOCKED`, no `MAP_HUGETLB` anywhere in the sources — checked, not assumed. Raising the limit would raise it for nothing, and in a unit file it reads as knowledge about the engine's requirements. |
| Huge pages | `MADV_HUGEPAGE` does not appear in the engine. What transparent huge pages do to the mmap'd segment reads is a property of your kernel's defaults, not of a setting we expose, and **we have not measured it** — so there is no number here to justify changing it. |
| `MemoryMax`, `CPUQuota` | An engine tuned for particular hardware and then capped below it is a contradiction. If the host is shared, the cap belongs to whatever else is on it. |

What does matter:

### CPU placement

The hot path is one epoll loop, one flush thread, and in multi-master one more io loop. What helps
is keeping those off the cores that handle NIC interrupts, so that a burst of packets does not
preempt the thread applying writes.

```bash
# Where the NIC's interrupts land today:
grep -E "$(ls /sys/class/net | grep -v lo | head -1)" /proc/interrupts
```

Then pin the service away from those cores by uncommenting `CPUAffinity` in the unit. There is no
default, deliberately: pinning to particular cores on an unknown machine is a mistake rather than a
tuning.

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
| NVMe with power-loss protection | `interval` or `never` | The device's own capacitor makes an fsync per record a cost with no matching guarantee. |
| Consumer SSD or anything virtualised | `every` | Without power-loss protection, an acknowledged write that is not fsynced is a write you can lose. |
| A filesystem on a network device | `every`, and reconsider | The engine's latency claims assume local storage. |

Put the data directory on the fastest local device you have, and **not** on the same device as the
journal of a busy filesystem: the WAL is sequential and small-record, so it is exactly the workload
that suffers from sharing a queue.

### WAL rotation, and what frees WAL files

`--wal-rotate-bytes` decides how large a WAL file grows before the writer opens the next one. It
defaults to 512 MB and it is a **trigger, not a file size**: rotation is checked after a write, so a
file may exceed it by one record.

Three things follow from it, which is why it is worth a section rather than a row in a table:

- **What a crash replays.** Together with `--flush-interval-ms`, it bounds the work between the last
  checkpoint and the end of the log.
- **What a reconnecting replica may have to scan.** A replica resumes from the position it saved and
  the primary streams forward from there, across files.
- **What retention can free, and when.** WAL files are deleted **whole**, and only below the file
  the slowest **connected** replica has acknowledged. So a large threshold means a lagging replica
  pins more bytes on disk; a small one means more files for every retention pass to walk.

That last point is the one that surprises people, and it is worth being concrete about the two
halves, because they differ in a way that matters during an incident:

| The replica is | `safe_truncate` is | What happens to old files |
|---|---|---|
| connected and behind (slow, or stopped) | its acknowledged file | **kept** — it can still catch up from the log |
| gone (crashed, killed, network down) | the current file | **freed** — a reconnect from an old position is answered `ERR WAL_TRUNCATED` and needs a snapshot |

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

The value is refused at both ends rather than clamped. Above 2 GiB: a WAL position is a file index
and a 32-bit offset read as one value, and a larger file would let the offset wrap and report a
position inside the wrong part of the file (#85). Below 65573 bytes — a 38-byte header plus the
64 KiB payload limit — a single write can fill a file on its own, so every write rotates and the
directory grows one file per record. The engine's own unit tests do use a 512-byte threshold, on a
`WALWriter` constructed directly; the floor belongs to the flag, because the number an operator
types has consequences nothing shows until the directory is unmanageable.

The file a node is writing is a gauge: `ob_wal_file_index`, published from the flush tick.

### When an fsync fails

Watch `ob_wal_fsync_errors_total`. It is separate from `ob_flush_errors_total` because the two ask
for different actions: a flush failing on `ENOSPC` means free space, an `fsync` returning `EIO`
means the device is failing and wants replacing.

**The engine cannot recover a failed `fsync`, and does not pretend to.** Linux reports the error to
whichever caller happened to be there and then **marks the affected pages clean**, so the next
`fsync` on that descriptor returns success with the data already gone. There is nothing to retry.
What the engine does instead:

- under `--fsync-policy every`, the write that could not be synced is **refused**, so the client is
  never told a record is durable when the sync for it failed
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

**What is not covered.** The columnar segment files are written with buffered stream I/O and are not
fsynced per segment, so this policy is about the WAL. A segment lost to a power failure is rebuilt
by replaying the WAL, which is what the WAL is for.

**Every storage failure this engine can reach is an `errno`, so it is a refusal and a counter —
never a signal.** That is worth stating because it is a property of a choice rather than of
storage: nothing here memory-maps a file for writing, and a growable mapping is the one shape in
which a full filesystem arrives as **`SIGBUS`** instead of `ENOSPC` — `ftruncate` extends sparsely
and succeeds, and the allocation fails later, when the page is touched and there is no return value
to check. Measured on an 8 MB tmpfs: reserving 64 MB succeeded and writing into it died with
`Bus error`, exit 135. A signal is not an exception, so #112's thread boundary could not catch it
and no client could be told. `docs/storage.md` records why the mapped store was removed rather than
adopted (#114).

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

## What the metrics say when something is wrong

`--metrics-port` exposes a Prometheus endpoint. Three gauges answer most questions before a log does:

- `ob_session_pending_bytes` — response bytes queued across sessions. A client that has stopped
  reading shows up here long before its session hits the 64 MB cap.
- `ob_subscription_queued_bytes` — the same for pushed subscriptions, and
  `ob_subscription_overflow_disconnects_total` is the only way you learn that a consumer could not
  keep up.
- `ob_pending_rows` — rows waiting for a flush. Growing steadily means the flush interval is longer
  than the write rate can afford.

One counter is worth watching for a different reason: **`ob_refused_commands_total`** is the number
of command lines the parser would not accept — an unknown word, or a known command carrying a token
its grammar has no place for (#107). A client that is working produces none of them, so any rate at
all means somebody is sending something they believe is being stored. The matching log line is
written **once per connection** (`Refused a command from fd=9: unexpected token 'x'; …`) rather than
once per line, because a refusal is reachable before authentication and a line per refusal is a
flood anyone who can reach the port can drive; the counter is the half that carries the volume.

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
that starts mid-frame would desynchronise the peer's parser; the peer reconnects and catch-up
streams from the position it acknowledged. `--mm-max-peer-send-buffer` is the ceiling (64 MB by
default) and lowering it makes the drop happen sooner, not the buffering safer.

**What the engine does not promise here.** Nothing is refused while a peer is unreachable, so the
writes accepted during a partition exist on one node until the link returns — the mesh converges
afterwards, by anti-entropy or by catch-up, and `docs/architecture.md` has the conflict rules that
decide what convergence means when both sides wrote. And the first few megabytes of that divergence
are invisible in the per-peer queue gauge, as measured above: the records lag is the number to alert
on, not `ob_mm_peer_send_buf_bytes`.

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
the position from then on — and once for every replica you add.

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

**How far behind a replica is** is a different question with a different answer, in the section
below.

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
  newest event time against the cutoff, so history loaded with its real timestamps arrives **with
  its age**: a backfill of last year into a node with a 24-hour TTL is expired on the next sweep.
  The inverse is also true and stranger — one row dated in the future keeps its whole segment alive.
- **Query pruning gets less effective, not wrong.** A segment is skipped when its `[start, end]`
  range cannot intersect the query's, so rows arriving out of order widen segments and more of them
  are read. Correctness does not depend on monotonic time; scan cost does.
- **Conflict resolution is untouched.** Multi-master last-writer-wins compares the HLC, which comes
  from the node's clock, not the record's `timestamp_ns` — so a client choosing a time **cannot**
  decide which of two conflicting writes survives. That was the one real risk in the change, and it
  is the reason the field could be added at all.

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
| any `--tls-*` flag on an io_uring build | for the client port, receive stays in userspace even with kernel TLS, so that transport needs a rewrite. The node links have their own epoll loops but remain refused because TLS on this transport has no runtime tests. The `io-uring-build` CI job (#108) checks compilation and linking only |

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
