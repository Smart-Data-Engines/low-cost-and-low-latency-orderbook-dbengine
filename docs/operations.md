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
127.0.0.1, because the wire protocol has no authentication (roadmap #30).

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

## What the metrics say when something is wrong

`--metrics-port` exposes a Prometheus endpoint. Three gauges answer most questions before a log does:

- `ob_session_pending_bytes` — response bytes queued across sessions. A client that has stopped
  reading shows up here long before its session hits the 64 MB cap.
- `ob_subscription_queued_bytes` — the same for pushed subscriptions, and
  `ob_subscription_overflow_disconnects_total` is the only way you learn that a consumer could not
  keep up.
- `ob_pending_rows` — rows waiting for a flush. Growing steadily means the flush interval is longer
  than the write rate can afford.

A metric written under a name nobody registered is dropped in silence, so `scripts/check_metrics.py`
fails CI for the class rather than trusting the reader to notice a flat zero.

## Security

**The wire protocol has no authentication and no encryption.** Do not expose a node outside a
trusted network. This is a deployment constraint rather than an oversight; roadmap #30 is the item
that changes it, and until then a node's port is as trusted as the network it is on.
