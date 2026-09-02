#pragma once

#include "orderbook/command_parser.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/metrics.hpp"
#include "orderbook/metrics_server.hpp"
#include "orderbook/response_formatter.hpp"
#include "orderbook/shard_coordinator.hpp"
#include "orderbook/session.hpp"
#include "orderbook/subscription_hub.hpp"

#include <atomic>
#include <map>
#include <set>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace ob {

// ── Server configuration ──────────────────────────────────────────────────────

struct ServerConfig {
    uint16_t    port{9090};
    std::string data_dir{"/tmp/ob_data"};
    int         max_sessions{64};
    int         worker_threads{4};
    size_t      max_line_length{262144}; // max command bytes (256KB, supports MINSERT with 1000 levels)
    bool        read_only{false};       // reject INSERT/FLUSH when true (replica mode)

    /// --fsync-policy: when the write-ahead log becomes durable. `every`, `interval` or `none`.
    ///
    /// It was hardcoded to INTERVAL, which made the single most consequential setting in a database
    /// unreachable — and `docs/operations.md` was written asking an operator to choose it per
    /// storage device before anything let them. On a device with power-loss protection an fsync per
    /// record is a cost with no matching guarantee; without one, an acknowledged write that was not
    /// fsynced is a write you can lose.
    FsyncPolicy fsync_policy{FsyncPolicy::INTERVAL};

    /// Background flush interval. Shorter means less unflushed data at any moment and
    /// more segment writes; longer means the opposite. Configurable because it decides
    /// how much sits in the WAL rather than in a segment, which is exactly what crash
    /// recovery has to deal with — and because a test of that recovery needs to be
    /// able to widen the window instead of racing a hardcoded 100 ms.
    uint64_t    flush_interval_ms{100};   // --flush-interval-ms

    // Replication (primary)
    uint16_t replication_port{0};       // 0 = disabled
    bool     replication_compress{false}; // --replication-compress

    // Replication (replica)
    std::string primary_host;
    uint16_t    primary_port{0};        // 0 = disabled

    // Snapshot bootstrap
    size_t      snapshot_chunk_size{262144};  // --snapshot-chunk-size (default 256 KB)
    std::string snapshot_staging_dir;         // --snapshot-staging-dir

    // Failover
    std::vector<std::string> coordinator_endpoints;  // --coordinator-endpoints (comma-separated)
    int64_t coordinator_lease_ttl{10};               // --coordinator-lease-ttl (seconds)
    int64_t handover_grace_seconds{5};               // --handover-grace-seconds
    int64_t handover_cooldown_seconds{15};           // --handover-cooldown-seconds
    /// --election-deference-ms: how long a candidate waits for a replica that published a further
    /// WAL position before promoting anyway. 0 switches the preference off.
    int64_t election_deference_ms{3000};
    /// --election-lease-wait-ms: how long a candidate waits after first seeing the leader key
    /// absent, so the previous holder has certainly stepped down (#82). 0 derives it from the
    /// lease TTL, which is the intended setting; a smaller explicit value narrows the safety
    /// margin in proportion.
    int64_t election_lease_wait_ms{0};
    std::string node_id;                             // --node-id
    bool failover_enabled{true};                     // --failover-enabled

    // TTL / data retention
    uint64_t ttl_hours{0};                    // --ttl-hours (0 = disabled)
    uint64_t ttl_scan_interval_seconds{300};  // --ttl-scan-interval-seconds

    // Observability
    uint16_t    metrics_port{0};              // --metrics-port (0 = disabled)
    std::string log_level{"INFO"};            // --log-level (ERROR|WARN|INFO|DEBUG)

    // Sharding
    std::string shard_id;                     // --shard-id (empty = non-sharded)
    uint32_t    shard_vnodes{150};            // --shard-vnodes

    // Multi-master replication
    bool        multi_master{false};                  // --multi-master
    uint16_t    mm_node_id{0};                        // --mm-node-id
    uint16_t    mm_replication_port{0};               // --mm-replication-port
    uint32_t    anti_entropy_interval_sec{30};        // --anti-entropy-interval-seconds
    size_t      mm_max_catchup_bytes{512ULL << 20};   // --mm-max-catchup-bytes (512MB)
    /// --mm-max-peer-send-buffer: queued output one peer may hold before it is dropped. Lower it
    /// in tests to reach the ceiling without generating 64 MB of traffic.
    size_t      mm_max_peer_send_buf_bytes{64ULL << 20};

    // Streaming subscriptions
    /// --max-subscriber-queue-bytes: how much queued output one subscription may hold before its
    /// session is closed.
    ///
    /// 8 MB, and the number is given with its arithmetic rather than on its own: a pushed row is
    /// about 60 bytes on the wire, so this is roughly **140 000 rows** of backlog. A consumer that
    /// has not read 140 000 rows is not slow, it is absent. Lower it in tests to reach the ceiling
    /// without generating 8 MB of traffic — the same trick as `--mm-max-peer-send-buffer` for #69.
    size_t   max_subscriber_queue_bytes{8ULL << 20};

    /// --max-subscriptions-per-session: without a limit one session can order an unbounded amount
    /// of work onto every other client's write path.
    int      max_subscriptions_per_session{16};

    // io_uring (used only when OB_USE_IO_URING is active)
    uint32_t uring_ring_size{256};            // --ring-size
    uint32_t uring_sqpoll_idle_ms{1000};      // --sqpoll-idle-ms
    bool     uring_no_sqpoll{false};          // --no-sqpoll
};

// ── TcpServer ─────────────────────────────────────────────────────────────────

class TcpServer {
public:
    explicit TcpServer(ServerConfig config);
    ~TcpServer();

    // Non-copyable, non-movable
    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    /// Start the server: open engine, bind socket, enter epoll loop.
    /// Blocks until shutdown() is called.
    void run();

    /// Signal the server to stop (thread-safe, called from signal handler).
    void shutdown();

private:
    ServerConfig             config_;
    std::unique_ptr<Engine>  engine_;
    std::unique_ptr<MetricsServer> metrics_server_;
    std::atomic<bool>        running_{false};
    std::atomic<bool>        draining_{false};  // drain phase: reject new connections, finish in-flight
    std::atomic<bool>        read_only_{false};  // dynamic read-only flag, toggled by failover
    int                      listen_fd_{-1};
    int                      epoll_fd_{-1};

    void accept_connection();
    void handle_client_data(int fd);

    /// Arm EPOLLOUT for a session with queued output, and disarm once it drains.
    ///
    /// Armed only after a partial write. Leaving EPOLLOUT armed permanently on an
    /// edge-triggered fd spins the loop and burns a core.
    void arm_epollout(int fd);
    void disarm_epollout(int fd);
};

// ── Free functions ────────────────────────────────────────────────────────────

/// Execute a command against the engine. Returns the wire-protocol response string.
/// When read_only is true, INSERT and FLUSH commands are rejected with an error.
/// When registry is non-null, latency histograms and operation counters are updated.
/// When shard_coord is non-null, sharding commands (SHARD_MAP, SHARD_INFO, MIGRATE)
/// are handled and INSERT/MINSERT ownership checks are enforced.
/// When hub is non-null, SUBSCRIBE and UNSUBSCRIBE are handled; when it is null they are refused
/// with a message saying so. Refused rather than accepted-and-ignored: a client that receives OK and
/// then silence cannot tell that from a market with no updates.
std::string execute_command(const Command& cmd,
                            Engine& engine,
                            Session& session,
                            ServerStats& stats,
                            bool read_only = false,
                            MetricsRegistry* registry = nullptr,
                            ShardCoordinator* shard_coord = nullptr,
                            SubscriptionHub* hub = nullptr);

/// Where a configuration value came from. For `--print-config`, which exists to answer exactly
/// that: a list of values does not tell an operator which of them they chose.
enum class Origin { Default, File, CommandLine };

/// Every flag `parse_cli_args()` accepts, without the leading dashes — which is also the set of
/// valid keys in a config file, because a key *is* a flag name.
///
/// Hand-written here and checked against the parser's own source by a static test
/// (`CliConfigStatic.KnownFlagsMatchTheParser`). Generating it into the build would give the same
/// guarantee at the cost of a build step; a static test in a required check is cheaper. What neither
/// tolerates is a hand-written list with nothing checking it, because a list that falls behind the
/// parser shows up as a config key an operator wrote that does nothing.
const std::vector<std::string>& known_flags();

/// The flags that take no value on the command line.
///
/// In a config file these take `true` or `false`; `false` emits nothing, because for a valueless
/// flag absence *is* false. That is only sound while every one of them defaults to false, which
/// `CliConfigStatic.EveryValuelessBooleanDefaultsToFalse` asserts — a valueless flag whose default
/// were true could not be turned off this way, and the symptom would be `= false` silently ignored.
///
/// `--failover-enabled` is **not** here: it takes a value, so `--failover-enabled false` has always
/// worked. Worth stating, because a negation flag was nearly added on the belief that it had not —
/// read from the default rather than from the parser.
const std::vector<std::string>& boolean_flags();

/// Read a config file into synthetic command-line arguments, in file order.
///
/// `port = 9090` becomes `--port 9090`; `multi-master = true` becomes `--multi-master`;
/// `multi-master = false` contributes nothing. Comments run from `#` to end of line.
///
/// The file is rewritten into arguments rather than parsed into a ServerConfig, and that is the
/// whole design: the key is a flag name **by construction** instead of through a mapping table, the
/// type validation and its error message stay in one place, and precedence falls out of argument
/// order because the parser assigns rather than accumulates.
///
/// Every refusal exits the process with a message naming the line number or the key. A config file
/// with a typo in it must not start a server — the same rule as #36, where a mistyped flag did.
/// `keys_seen`, when given, receives the **config keys** the file set — not the flags emitted for
/// them. `failover-enabled = false` emits `--no-failover-enabled`, and recording provenance under
/// the emitted name would report the key the operator wrote as coming from the default.
std::vector<std::string> config_file_to_args(const std::string& path,
                                             std::set<std::string>* keys_seen = nullptr);

/// Parse CLI arguments into a ServerConfig. Applies defaults for missing args.
///
/// With `--config <path>`, the file is read first and the real command line second, so a flag
/// overrides a file value and a file value overrides a default.
ServerConfig parse_cli_args(int argc, char* argv[]);

/// The same, plus where each value came from. `parse_cli_args()` is this without the provenance.
struct ResolvedConfig {
    ServerConfig                 config;
    std::map<std::string, Origin> origin;   ///< flag name (no dashes) -> where it came from
};
ResolvedConfig resolve_cli_args(int argc, char* argv[]);

/// Render a resolved configuration for a human, sorted, with the provenance of each value.
std::string format_config(const ResolvedConfig& resolved);

/// The `--help` text, generated from `known_flags()` rather than written beside it.
///
/// It used to be a hardcoded string in `tools/ob_tcp_server.cpp` naming six of the forty flags the
/// parser accepts - including neither `--config` nor `--print-config`, which exist so that forty
/// flags are manageable, nor `--fsync-policy`, which is the durability setting in a database. A
/// flag with no description prints as `(undocumented)` and fails a static test, so the drift is
/// visible in both directions.
std::string format_usage(const std::string& program);

} // namespace ob
