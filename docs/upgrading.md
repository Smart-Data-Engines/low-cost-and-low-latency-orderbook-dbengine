# Upgrading a cluster without stopping it

A node of this version and a node of the previous one are run side by side in
`tests/integration/test_mixed_versions.py` - on each other's data directories, as a primary and a
replica each way, through a failover, in one mesh, and with this version's clients against the
previous server. The matrix below is what that module measured; its last test fails when a
scenario has no row here, or a row names no scenario (#56).

**The previous version** is the revision `scripts/previous_version.txt` names. The CI integration
jobs build its server beside this tree's and hand it to the module as `OB_PREVIOUS_SERVER_BINARY`;
on a developer's machine the module skips without it, and in CI it fails, since a skip there would
read as a pass. The file moves to the previous release's revision at each release.

`ob_tcp_server --version` says which build a binary is, without starting it; `STATUS` and
`ob_build_info` say it of a running node ("Which build is running" in `docs/operations.md`).

## The matrix

The previous version here is `c610b72` (master on 28 September 2026, before #34, #197 and #190's
step 4), against this tree.

| Scenario | Result | Test |
|---|---|---|
| A data directory the previous version wrote - segments, and rows only in the WAL after a kill - opened by this one | every row once; writes go on, numbered after them | `test_an_upgrade_in_place_keeps_every_row_once` |
| A data directory this version wrote in segment format 2 (`segment-format = 2`), opened by the previous one | every row once (the other permitted answer is a refusal to start; a start with part of the rows is the failure) | `test_a_downgrade_keeps_every_row_once_or_refuses_to_start` |
| A data directory this version wrote in segment format 3, the default, opened by the previous one | it starts without the rows of format-3 segments, which it cannot decode, saying `Skipping segment ...: unsupported format_version=3 (this build reads 2)` at `ERROR` for each one a query reaches; it removes none, and this version started again holds every row once. A start with part of the rows, which this matrix calls the failure - the row changed with segment format 3, and "Going back" below says what an operator does | `test_a_downgrade_after_format_3_hides_its_segments_and_removes_nothing` |
| A primary of the previous version, a replica of this one | the replica holds every row | `test_a_primary_of_the_previous_version_replicates_to_this_one` |
| A primary of this version, a replica of the previous one | the replica holds every row | `test_a_primary_of_this_version_replicates_to_the_previous_one` |
| A failover from a previous-version primary to this version's replica, under etcd | no acknowledged write lost; writes go on at the new primary | `test_a_failover_from_the_previous_primary_to_this_versions_replica_loses_no_write` |
| A planned `FAILOVER` from a previous-version primary to this version's replica | it completes: no acknowledged write lost, writes go on at the new primary, the outgoing node is a replica. The previous version's intent says nothing that would let the target stand before the election delay (#204) | `test_a_handover_from_the_previous_primary_to_this_versions_replica_loses_no_write` |
| A planned `FAILOVER` from this version's primary to a previous-version replica | it completes: no acknowledged write lost, writes go on at the new primary, the outgoing node is a replica - the previous version reads this one's intent | `test_a_handover_from_this_versions_primary_to_the_previous_replica_loses_no_write` |
| A two-node mesh, one node of each version | both hold every row either wrote | `test_a_mesh_of_both_versions_converges` |
| This version's Python client and `ob_backup` against the previous server | writes and queries work; `capabilities:` has no `backup`, and `ob_backup` refuses with status 2 | `test_this_versions_clients_work_against_the_previous_server` |

What the matrix does not say: anything about a version older than the previous one, and anything
this module does not run - a sharded cluster, TLS, authentication. Those are upgraded by the same
procedure and have not been measured across versions.

## The procedure

**A primary with replicas.**

1. One replica at a time: stop it, replace the binary, start it. It resumes from its position -
   `primary serves stream ..., the one our position belongs to - resuming from ...` in its log - and
   catches up. Check `STATUS` (its `replication:` line) before the next.
2. Move the primary role to an upgraded replica: `FAILOVER <node-id>`, or stop the primary and let
   the election choose (`docs/cli.md`, "High Availability").
3. Replace the old primary's binary and start it: it comes back as a replica of the new primary.
   Its data directory is a different WAL's from the new primary's point of view, so it re-syncs
   (`a different WAL at the same address`); a replica of a primary that once bootstrapped from a
   snapshot re-syncs from a snapshot since #197.

Replicas first, because a replica of the new version follows a primary of the previous one, and
because a failover then lands on an upgraded node.

**A multi-master mesh.** One node at a time: stop, replace, start, and wait for it to converge -
`MM_PEERS` shows every peer connected and the lag back to its usual level - before the next.

**A sharded cluster.** Each shard as a primary with replicas, one shard at a time. The shard map in
etcd is not versioned by the engine.

**Going back.** The same order, reversed. A data directory this version wrote in segment format 2
opens in the previous one (the matrix). One it wrote in format 3, the default, does not whole: the
previous version leaves out the rows of every format-3 segment, and nothing converts them back. So a
node that has to keep the way back runs `segment-format = 2` from its upgrade until the previous
version is no longer one to go back to ("Segment format 3" in `docs/operations.md`). The sections
of `docs/operations.md` named for a particular change say where an older build than the previous
one reads a newer directory differently ("Going back to a build before part 2b of #165",
"Downgrading a mesh node across #189").

## When a change breaks a row

A change to what a node writes or sends that makes a row fail here is not merged by relaxing the
test: the row changes, with what now happens and why, and the procedure above says what an operator
does about it - which is the upgrade note of that release.
