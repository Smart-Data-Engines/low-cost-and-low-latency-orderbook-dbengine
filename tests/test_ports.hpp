#pragma once

#include <cstdint>

/// Fixed port blocks for the tests that have to name a port before the socket exists.
///
/// **Why fixed ports at all.** These tests hand a number to a component that binds it later, so
/// "ask the OS for a free one" does not apply: there is nothing to ask on behalf of. The number has
/// to exist first.
///
/// **Why not the numbers we used to pick.** Measured on 12 September 2026, here and on a GitHub
/// runner: `/proc/sys/net/ipv4/ip_local_port_range` is **32768-60999**, and every number in it can
/// be handed to any outgoing connection on the machine at any instant. Four test files named 47821,
/// 54900, 55100 and 55400 - all inside it - and seven tests failed that day with
/// `bind() failed on port 55400: Address already in use` against a tree whose only change was the
/// command parser. The same seven passed when re-run on their own. `SO_REUSEADDR` does not help:
/// the squatter is an *active* socket belonging to somebody else, not a `TIME_WAIT` of ours. The
/// odds changed with the machine rather than with the code - ClickHouse and PostgreSQL were
/// installed here the day before, and ClickHouse alone keeps about a thousand threads.
///
/// **Why it matters more than a re-run.** A required check that goes red for a reason unrelated to
/// the code is indistinguishable from a finding until somebody reads the log, and this repository
/// has already paid that bill twice - once for a CodeQL outage and once for an etcd download that
/// had no retry. A suite that needs luck teaches its readers to re-run it, which is the habit that
/// hides the next real failure.
///
/// Every block is **below** the ephemeral floor, so only our own tests can collide with them - and
/// that collision is visible here, in one file, rather than distributed across eleven. The blocks
/// are 100 wide; the counters in the test files hand out a handful each.
namespace ob::test {

/// The lowest port the kernel may hand to an outgoing connection on a default Linux. Read back from
/// `/proc/sys/net/ipv4/ip_local_port_range` by `test_port_discipline`, rather than trusted here.
inline constexpr uint16_t kEphemeralFloorAssumed = 32768;

inline constexpr uint16_t kPortsReplication         = 20000;  ///< test_replication.cpp
inline constexpr uint16_t kPortsReplicationCompress = 20100;  ///< test_replication_compress.cpp
inline constexpr uint16_t kPortsFailoverRoles       = 20200;  ///< test_failover_roles.cpp
inline constexpr uint16_t kPortsMultiMaster         = 20300;  ///< test_multi_master.cpp
inline constexpr uint16_t kPortsMmDedup             = 20400;  ///< test_mm_dedup.cpp
inline constexpr uint16_t kPortsMmStats             = 20500;  ///< test_mm_stats.cpp
inline constexpr uint16_t kPortsMmPendingPeers      = 20600;  ///< test_mm_pending_peers.cpp

/// Every block above, so a test can check them all rather than the ones somebody remembered.
inline constexpr uint16_t kAllPortBlocks[] = {
    kPortsReplication, kPortsReplicationCompress, kPortsFailoverRoles, kPortsMultiMaster,
    kPortsMmDedup,     kPortsMmStats,             kPortsMmPendingPeers,
};

/// How many ports one block owns.
inline constexpr uint16_t kPortBlockWidth = 100;

} // namespace ob::test
