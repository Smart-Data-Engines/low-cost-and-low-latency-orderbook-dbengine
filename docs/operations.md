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

**Reading the log during such an outage, and two things about it are being fixed.** Each node
writes about **2.2 lines a second** for the whole outage, and roughly 92% of them are two
sentences repeated once a second: `could not grant a lease for the published position …` and
`publish_wal_position failed …`. The first of those says the position is being *published without
a lease*, and when the coordinator is unreachable it is not published at all — so ignore the part
about the position outliving the node; nothing was written. Roadmap **#116** is that.

What the log does **not** contain at the default level is the decision that answers the question
you are probably asking: the replica records "staying REPLICA rather than campaigning on a read
that failed" at **DEBUG**. Until roadmap **#115** changes that, `--log-level DEBUG` on the replica
is how you tell the engine deciding from the engine stuck. The holder's step-down, by contrast, is
fully visible at the default level — four lines, once, naming the mechanism.

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
