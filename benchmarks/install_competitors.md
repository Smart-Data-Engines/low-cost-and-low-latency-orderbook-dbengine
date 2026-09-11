# Installing the competitors, natively

Commands to read before pasting. **The harness installs nothing** — a benchmark that reaches for
`sudo` on its own is a benchmark nobody can audit, and the numbers it produces depend on what it
decided to change about the machine.

Everything here is native. Requirement 1.2 of
`kiro-workspace/specs/reproducible-benchmarks/requirements.md` is not a preference: this engine has
no containerised deployment path *by design*, so measuring it against containerised competitors
would compare an engine tuned for the hardware against databases separated from that hardware by a
network namespace and an overlay filesystem. The container layer would be the largest difference in
the table and it would be ours to keep.

## The trap this machine walked into first

**Two of the three competitors were already running here, in containers**, for the flagship
product's test engines and for the landing page:

| what | where | why it is not usable here |
|---|---|---|
| ClickHouse 24.8 | container, host port **58123** | containerised; also password-protected |
| PostgreSQL 15 | container, host port **5432** | containerised, and it is the landing page's database |

A run that reached either would have measured a container and **nothing in the numbers would have
looked wrong**. So each adapter refuses the port by name before it asks the server anything
(`ClickHouseSystem.available()`, `TimescaleDbSystem.available()`), and each records the endpoint it
used in `config_dump()`, so the results file says which server answered. The native ClickHouse
reports 26.8.x on 8123 and the container reports 24.8.x on 58123: the version is the proof, not the
argument.

The second consequence is the port PostgreSQL ends up on. `pg_createcluster` takes the next free
one, so with 5432 held by a container the native cluster came up on **5433** — which is what the
adapter's default says, with the reason next to it.

## ClickHouse

```bash
sudo apt-get install -y apt-transport-https ca-certificates gnupg
curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' \
  | sudo gpg --dearmor --yes -o /usr/share/keyrings/clickhouse-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg arch=amd64] \
https://packages.clickhouse.com/deb stable main" \
  | sudo tee /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y clickhouse-server clickhouse-client
sudo systemctl start clickhouse-server
```

`DEBIAN_FRONTEND=noninteractive` leaves the `default` user without a password, which is what the
adapter expects on a loopback-only install. Verify that the **native** server is the one answering:

```bash
clickhouse-client --port 9000 --query "SELECT version()"    # 26.8.2.7 here
curl -s 127.0.0.1:8123 --data "SELECT version()"           # the adapter's endpoint
```

The adapter uses the HTTP interface on 8123 with one kept-alive connection, and that is measured
rather than preferred: `clickhouse-client` costs **80 ms** of process start per invocation — forty
times the query it carries — and with `clickhouse-driver` installed for the experiment the native
protocol came out at p50 **5.777 ms** against HTTP's **4.980 ms** on the 2000-row workload. HTTP
needs no driver, so the harness has no dependency to install.

## TimescaleDB on a native PostgreSQL

```bash
curl -fsSL https://packagecloud.io/timescale/timescaledb/gpgkey \
  | sudo gpg --dearmor --yes -o /usr/share/keyrings/timescaledb-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/timescaledb-keyring.gpg] \
https://packagecloud.io/timescale/timescaledb/ubuntu/ $(lsb_release -c -s) main" \
  | sudo tee /etc/apt/sources.list.d/timescaledb.list
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  postgresql-16 postgresql-client-16 timescaledb-2-postgresql-16

# Timescale's own tuner: shared_preload_libraries plus memory and worker settings for this machine.
# This is the tuning requirement 4.2 asks for, applied by the vendor's tool rather than by us.
sudo timescaledb-tune --quiet --yes --pg-config=/usr/lib/postgresql/16/bin/pg_config
sudo systemctl restart postgresql@16-main
pg_lsclusters                      # note the port: 5433 here, because 5432 was taken
```

Then a role and a database the harness can reach over the Unix socket, with peer authentication —
no password in a config file, and nothing listening for the network:

```bash
sudo -u postgres psql -p 5433 -c "CREATE ROLE $USER LOGIN SUPERUSER"
sudo -u postgres createdb -p 5433 -O "$USER" ob_bench
psql -p 5433 -d ob_bench -c "CREATE EXTENSION IF NOT EXISTS timescaledb"
psql -p 5433 -d ob_bench -Atc "SELECT extversion FROM pg_extension WHERE extname='timescaledb'"
```

The adapter drives **one long-lived `psql` session** on stdin rather than `psql -c` per statement,
and for the same measured reason as ClickHouse: a fresh `psql` costs 40-60 ms against queries of a
few milliseconds. `\copy` loads the CSV from the client side, because the dataset lives under a home
directory the `postgres` user cannot traverse and a server-side `COPY` fails with a permission error
that reads like a harness bug.

## kdb+

Not installed here, and the reason is in the table rather than in a footnote. Two things stand
between this repository and a kdb+ column, and only one of them is technical:

1. **The binary and its licence come from a vendor registration.** That is a human action with
   somebody's identity attached, so the harness does not do it and this document does not pretend
   it can be scripted. Once `q` is on `PATH` and a licence file (`kc.lic`, `k4.lic` or `kx.lic`) is
   in `~/q`, `KdbSystem` runs; until then it reports `NOT MEASURED` with that sentence.
2. **Whether numbers produced under the free edition may be published in a company's public
   repository is a reading of its licence, not a measurement.** That belongs to whoever holds the
   licence. Requirement 5.3 asks for the free edition's limits to be annotated beside every number
   rather than in a footer; the first annotation is whether the number may be there at all.

`systems/kdb.py` is written against the same interface as the others and refuses early, so the day a
licence exists this is a flag rather than a file.

## Leaving the machine as you found it

The adapters drop their own database or table and **do not stop the servers** — they did not start
them. If you want the machine back:

```bash
sudo systemctl stop clickhouse-server && sudo apt-get purge -y clickhouse-server clickhouse-client
sudo systemctl stop postgresql@16-main && sudo apt-get purge -y \
  postgresql-16 postgresql-client-16 timescaledb-2-postgresql-16
sudo rm -f /etc/apt/sources.list.d/{clickhouse,timescaledb}.list \
           /usr/share/keyrings/{clickhouse,timescaledb}-keyring.gpg
```

`timescaledb-tune` edited `/etc/postgresql/16/main/postgresql.conf` and left a backup beside it;
purging the packages removes the directory. Nothing here touched the containers or their data.
