#pragma once

#include "orderbook/auth.hpp"
#include "orderbook/command_parser.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/machine.hpp"
#include "orderbook/metrics.hpp"
#include "orderbook/metrics_server.hpp"
#include "orderbook/response_formatter.hpp"
#include "orderbook/shard_coordinator.hpp"
#include "orderbook/session.hpp"
#include "orderbook/tls.hpp"
#include "orderbook/subscription_hub.hpp"

#include <chrono>
#include <atomic>
#include <map>
#include <set>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace ob {

// ── Server configuration ──────────────────────────────────────────────────────

/// The spin window `boost` sets `io_spin_us` to, where it spins at all (`choose_boost()`).
///
/// **A measurement, where #144 had a judgement of 50 µs** (stage 4 of #151, m9g.xlarge, 20 000
/// `PING`s per run, five rounds, medians). The gain saturates at 10 µs — p50 7 569 ns without the
/// spin, 7 622 at 5 µs, which catches nothing, then 6 279 at 10, 6 252 at 20 and 6 306 at 50 — and
/// when the machine has more busy threads than cores **the tail is the window**: a `PING` beside
/// three pipelining writers on the same four cores has a p99 of 15 917 ns without the spin, 16 448
/// at 10 µs, 24 190 at 20 and 53 900 at 50, because a spinning loop keeps the core the client needs
/// until the window closes. 10 µs keeps all of the gain and none of that tail.
///
/// **Where the number does not travel.** The window has to cover the gap between an answer and the
/// client's next request, and that gap belongs to the machine and the client. Here it is between 5
/// and 10 µs; on the development laptop (i3-7100U, 30 µs round trips) 10 and 20 µs catch nothing and
/// 50 µs takes 18% off. An operator whose client has a longer gap sets `--io-spin-us`, and this
/// number stops applying.
inline constexpr uint64_t kBoostSpinUs = 10;

/// The profile the server's command line starts from (stage 4 of #151): the one that sizes the
/// client event loops and the spin to the machine. `ServerConfig`'s own default stays `eco`, like
/// the two fields a profile sets - a struct cannot know the machine.
inline constexpr const char* kDefaultProfile = "boost";

struct ServerConfig {
    uint16_t    port{9090};
    std::string data_dir{"/tmp/ob_data"};
    int         max_sessions{64};
    size_t      max_line_length{262144}; // max command bytes (256KB, supports MINSERT with 1000 levels)
    /// Ceiling on what one session may hold that is not yet a command (#143).
    ///
    /// Twice `max_line_length`, because a session can legitimately hold one `MINSERT` block being
    /// assembled *and* the beginning of the next command, and each of those is bounded by the
    /// line length. Anything past that is not a command in progress.
    ///
    /// **No command-line flag, exactly like `max_line_length` above**, which is the sibling this
    /// bound is derived from: both are compile-time ceilings an embedder or a test can set on the
    /// config object, and neither is on the wire-facing surface. Claiming otherwise here would
    /// promise configurability that does not exist. There is deliberately **no** value that
    /// disables it: nothing could set one, so the branch would be untestable and would read as an
    /// option this engine offers.
    size_t      max_unparsed_bytes{524288};

    /// --io-spin-us: how long the client loop keeps polling with a zero timeout after the last
    /// event before it goes back to blocking. 0 is blocking always, which is what this engine has
    /// always done and remains the default.
    ///
    /// **What it buys, measured** (m9g.xlarge, four interleaved rounds of 20,000 raw-socket
    /// `PING`s against the same tree with only the timeout changed): p50 **7963 → 6342 ns** and
    /// p99 **8276 → 6720**, for **+59% CPU on the io thread**. The *minimum* does not move —
    /// ~5.9 µs in both — which is what says the thing that leaves is the kernel's wakeup rather
    /// than a tail: a constant on every round trip, not an occasional stall.
    ///
    /// **What it does not buy, and this has to travel with the number:** that is loopback on one
    /// machine. Across a real network a round trip is orders of magnitude larger and 1.6 µs stops
    /// being 20% of it. This is worth what it is worth to a **colocated** client.
    ///
    /// The window is why it is microseconds rather than a boolean: a bare zero timeout burns a
    /// core on an idle node for ever, and the low CPU figure above was measured only because the
    /// probe kept the loop busy. Spinning for a bounded time *after an event* means a node under
    /// continuous load never blocks and an idle one costs nothing.
    uint64_t    io_spin_us{0};

    /// --profile: a named set of the knobs, so an operator can ask for the trade rather than know
    /// which flags carry it. `boost` sizes `io_threads` and `io_spin_us` to the machine
    /// (`choose_boost()`); `eco` sets nothing, so it is the engine as it was before either knob -
    /// one client event loop, blocking between events. **`boost` is the command line's default**
    /// (`kDefaultProfile`), applied by `resolve_cli_args()`; this field's own default stays `eco`,
    /// like the fields it governs, because a `ServerConfig` built in code has no machine to be
    /// sized to. A later knob joins the profiles rather than becoming a third.
    ///
    /// It is a **source of values**, not a second code path — there is one io loop and the profile
    /// decides a number in it. `--print-config` attributes what the profile set to the profile
    /// (`Origin::Profile`), because a mode whose effect you cannot read is a mode on somebody's
    /// word, and the weaker mode nobody can see is the one that ends up on production (#136).
    std::string profile{"eco"};
    bool        read_only{false};       // reject INSERT/FLUSH when true (replica mode)

    /// --fsync-policy: when the write-ahead log becomes durable. `every`, `interval` or `none`.
    ///
    /// It was hardcoded to INTERVAL, which made the single most consequential setting in a database
    /// unreachable — and `docs/operations.md` was written asking an operator to choose it per
    /// storage device before anything let them. On a device with power-loss protection an fsync per
    /// record is a cost with no matching guarantee; without one, an acknowledged write that was not
    /// fsynced is a write you can lose.
    FsyncPolicy fsync_policy{FsyncPolicy::INTERVAL};

    /// --wal-rotate-bytes: how large a WAL file grows before the writer opens the next one.
    ///
    /// An operator knob on its own merits — it decides how much a crash has to replay, how much a
    /// replica may have to scan to catch up, and the granularity retention can free, since WAL
    /// files are deleted whole and only below the slowest connected replica's file.
    ///
    /// It was a literal in `Engine`'s constructor, and the cost was not the missing knob: **no
    /// integration test had ever crossed a WAL file boundary**, because a real node would have had
    /// to write 512 MB to reach one. Rotation's interaction with retention, with a replica catching
    /// up across files (#98) and with the replica lag (#123) was therefore covered only by unit
    /// tests driving a `WALWriter` directly, which cannot express a reconnect or a retention pass.
    ///
    /// Documented as a *trigger*, not a file size: rotation is checked after a write, so a file may
    /// exceed this by one record.
    size_t      wal_rotate_bytes{512ULL << 20};   // 512 MB

    /// Background flush interval. Shorter means less unflushed data at any moment and
    /// more segment writes; longer means the opposite. Configurable because it decides
    /// how much sits in the WAL rather than in a segment, which is exactly what crash
    /// recovery has to deal with — and because a test of that recovery needs to be
    /// able to widen the window instead of racing a hardcoded 100 ms.
    uint64_t    flush_interval_ms{100};   // --flush-interval-ms

    /// How long a shutdown waits for open client sessions before closing them itself.
    ///
    /// Bounded by default, and the default is the point: on `SIGTERM` the listener closes at once
    /// and the loop then waits for every existing session to end. Measured on this machine
    /// (roadmap #106): **0.11 s** to exit with nothing connected, and **still running after 60 s**
    /// with one *idle* client attached — because an idle client never leaves. A long-lived client
    /// is the normal case for a database (a connection pool, a `SUBSCRIBE` stream, a monitor), so
    /// with no bound a supervisor reaches its own timeout and sends `SIGKILL`, which is exactly
    /// when the flush and checkpoint this path exists for do not run.
    ///
    /// `0` keeps the old behaviour — wait for ever — and has to be asked for, because the default
    /// is what a supervisor meets.
    uint64_t    drain_timeout_ms{10000};  // --drain-timeout-ms (0 = wait indefinitely)
    /// --io-threads: client event loops. Connections are dealt to them in turn by the one that
    /// accepts, and a connection stays on its reactor for life, because a Session is not
    /// thread-safe. 1 is the loop this server always had.
    uint32_t    io_threads{1};

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

    /// --compaction on|off: whether the flush tick merges small segments (#165 part 2b). A valve on
    /// a process that rewrites what is stored, not a tuning knob.
    bool compaction{true};

    // ── Authentication (#30) ──────────────────────────────────────────────────
    //
    // Paths, never secrets. `--print-config` renders every value in this struct, so a secret held
    // here would be printed by a command whose whole purpose is to be pasted into a ticket. The
    // loaded credentials live in the components that verify against them.

    /// --auth-secret-file: `<identity> <secret>` lines. Empty = client authentication disabled.
    std::string auth_secret_file;

    // ── TLS (#30 part three) ──────────────────────────────────────────────────
    //
    // Paths, never contents, for the same reason as the secret files: `format_config()` prints
    // every field of this struct.

    /// --tls-cert-file / --tls-key-file: server certificate chain and private key, PEM.
    std::string tls_cert_file;
    std::string tls_key_file;

    /// --tls-client: TLS on the client port. Requires the two files above, and the process refuses
    /// to start without them rather than listening in plaintext.
    bool tls_client{false};

    // ── Node links (series D) ─────────────────────────────────────────────────
    //
    // On a node link TLS is always mutual: both ends present a certificate and both verify. There
    // is deliberately no flag for "encrypt but do not verify the peer" - that configuration leaves
    // the man-in-the-middle relay described in SECURITY.md open while looking like protection, and
    // it is the case this whole part exists to remove. The consequence is a refusal: either flag
    // below without `--tls-ca-file` does not start.

    /// --tls-replication: TLS on the replication link, in **both** roles - the primary's listener
    /// and the replica's client. One flag, because a node is both at different times.
    bool tls_replication{false};

    /// --tls-multi-master: TLS on the mesh, in both roles, because the mesh is symmetric.
    bool tls_multi_master{false};

    /// --tls-ca-file: the trust anchor peer certificates are verified against. Required by either
    /// node-link flag; deliberately not defaulted to the system trust store, which would mean every
    /// public CA on earth may introduce a replica.
    std::string tls_ca_file;

    /// --tls-peer-names: identities an accepted peer's certificate may carry, comma separated.
    ///
    /// The accepting end of a node link knows only the source address, so unlike the dialling end it
    /// has no name to expect. Empty means "any identity this CA signed", which is true when the CA
    /// signs nothing but this cluster and false for a corporate CA - so the startup log says which
    /// mode is in force rather than leaving it to a document.
    std::vector<std::string> tls_peer_names;

    /// --cluster-secret-file: a single secret shared by the replication and multi-master links.
    /// Empty = cluster authentication disabled. Must not be a client secret (see
    /// stores_share_a_secret): a client able to present itself as a replica can stream the
    /// entire write-ahead log.
    std::string cluster_secret_file;

    // Observability
    uint16_t    metrics_port{0};              // --metrics-port (0 = disabled)

    /// --metrics-bind: address the metrics listener binds to. Empty means every interface, which
    /// is what it did before the flag existed.
    ///
    /// The metrics endpoint has no authentication and this is deliberate (#30 §8): a Prometheus
    /// scraper cannot perform a challenge-response, so a bearer token would be a second and weaker
    /// mechanism - and the weaker one is the one that gets used. Binding to a loopback or private
    /// interface is the stronger answer, and it costs no protocol.
    std::string metrics_bind;
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
};

// ── Loaded credentials ────────────────────────────────────────────────────────

/// The credential stores a node runs with. Either may be empty, meaning that surface does not
/// authenticate.
struct LoadedSecrets {
    SecretStore clients;   ///< --auth-secret-file
    SecretStore cluster;   ///< --cluster-secret-file

    bool client_auth_enabled() const { return !clients.empty(); }
    bool cluster_auth_enabled() const { return !cluster.empty(); }

    /// The pointer execute_command() wants: null when client authentication is off.
    const SecretStore* client_store() const {
        return clients.empty() ? nullptr : &clients;
    }
};

/// Load both secret files, or print a refusal and exit.
///
/// Exits rather than throws, and does so for the same reason the CLI parser does since #36: a
/// misconfigured secret file must not start a server. Three ways to fail, all fatal: the file is
/// unloadable (see SecretStore), or the cluster secret is also a client secret - which would let a
/// client present itself as a replica and stream the whole write-ahead log.
///
/// Also logs the state of each surface at INFO, including **disabled**. A default-open setting
/// belongs in the log rather than only in a document nobody reads at three in the morning.
LoadedSecrets load_secrets_or_exit(const ServerConfig& config);

/// The TLS contexts a node runs with. A null member means that surface and role is plaintext.
///
/// Four instances and two shapes: the client port needs a server context that asks nothing of the
/// caller, and each node link needs one of each role. Replication and the mesh share the shapes but
/// not the instances, so a future difference between the two surfaces has somewhere to live.
struct LoadedTlsContexts {
    std::shared_ptr<TlsContext> client_port;      ///< --tls-client
    std::shared_ptr<TlsContext> replication_server;
    std::shared_ptr<TlsContext> replication_client;
    std::shared_ptr<TlsContext> mesh_server;
    std::shared_ptr<TlsContext> mesh_client;
};

/// Build every configured TLS context, or print a refusal and exit.
///
/// Refuses a `--tls-*` surface without a certificate and key rather than listening in plaintext,
/// refuses a **node link** without `--tls-ca-file` rather than encrypting without authenticating the
/// peer, and warns when a certificate or a peer-name list is configured with no surface enabled - a
/// certificate that protects nothing looks exactly like one that does.
LoadedTlsContexts load_tls_or_exit(const ServerConfig& config);

// ── TcpServer ─────────────────────────────────────────────────────────────────

/// The largest `--io-threads` accepted. Well above any core count this engine is run on, and
/// low enough that a typo such as 400 is refused rather than started.
inline constexpr uint32_t kMaxIoThreads = 64;

/// What the `boost` profile chooses on a machine, and why - one sentence for the startup log and
/// for `--print-config`, because a mode whose effect you cannot read is a mode on somebody's word.
struct BoostChoice {
    uint32_t    io_threads{1};
    uint64_t    io_spin_us{0};
    std::string reason;
};

/// The `boost` rule (stage 4 of #151), from the measurement written down with it:
///
/// - **one client event loop per usable CPU**, at most `kMaxIoThreads`. Fewer leaves reads behind -
///   four loops on four cores read 49.1 million levels a second, three 40.2 to 42.2 - and more than
///   the CPUs adds nothing and multiplies the tail: three loops on two cores, p99 of a read batch
///   10.7 ms against 1.05. Below the machine's CPUs a cgroup's limit decides, rounded down, because
///   loops beyond it are throttled: two loops under a one-CPU limit wrote what one loop wrote, with
///   a p99 of 25 ms against 0.93 and 1.4 s of every 2 stopped;
/// - **the spin window `kBoostSpinUs` when the process may run on at least two CPUs and a cgroup
///   limit, if there is one, leaves at least one CPU of time beyond the loops.** On the only CPU a
///   process may run on, a spinning loop holds it from everything else there: a client sharing it
///   measured a p99 of 5.9 µs without the spin and 13.7 µs with it. Under a limit, spinning spends
///   the limit, and a cgroup over it stops every thread until the next period.
///
/// Pure, like `detect_machine()`: the same machine gives the same choice.
BoostChoice choose_boost(const MachineResources& machine);

/// What a draining loop should do this pass.
enum class DrainVerdict {
    KeepWaiting,       ///< sessions are still open and the deadline has not passed
    AllSessionsClosed, ///< nothing is connected; stop cleanly
    DeadlineReached,   ///< the bound expired with sessions still open; close them and stop
};

/// The drain decision, in one place.
///
/// Written when two transports asked it: the io_uring loop checked `draining_ && active_sessions <=
/// 0` in **two** places and the epoll loop in one, none of which had a deadline (#106), and no CI
/// job built the io_uring file, so a bound written three times could not even be compiled on one of
/// the two sides. That transport is gone (#147); the function stays, because the decision is pure
/// and a test can ask it a thousand times without a socket, and because this repository has paid
/// for "the fix exists and is used at one of two sites" in #91, #101 and #102. A static test refuses
/// a drain check that does not come through it.
///
/// Pure: the caller logs, because only the caller knows how many sessions it is about to cut.
/// How long the client loop should wait for the next event: 0 to poll, or the blocking timeout.
///
/// Pure, and a separate function for the same reason `drain_verdict()` below is: the loop asks
/// once per pass and a test can ask it a thousand times without a socket. A spin window expressed
/// as "keep polling until `last_event + window`" is the whole of the mode — there is no second
/// code path, and this is the only place that decides.
///
/// `spin_us == 0` answers the blocking timeout always, which is this engine's behaviour before the
/// mode existed and its default after.
int io_wait_ms(std::chrono::steady_clock::time_point last_event,
               uint64_t spin_us,
               int blocking_timeout_ms,
               std::chrono::steady_clock::time_point now);

DrainVerdict drain_verdict(std::chrono::steady_clock::time_point drain_started,
                           int active_sessions,
                           uint64_t drain_timeout_ms,
                           std::chrono::steady_clock::time_point now);

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

    /// Credentials this node runs with, loaded once at construction.
    ///
    /// Held here rather than in ServerConfig so that `--print-config` has nothing to print, and
    /// loaded in the constructor so a bad secret file refuses to start the process rather than
    /// failing on the first client.
    LoadedSecrets            secrets_;

    /// The TLS contexts, one per surface and role. Built at construction so a bad
    /// certificate stops the start rather than failing every handshake.
    LoadedTlsContexts tls_;
    std::atomic<bool>        running_{false};
    std::atomic<bool>        draining_{false};  // drain phase: reject new connections, finish in-flight
    std::atomic<bool>        read_only_{false};  // dynamic read-only flag, toggled by failover
    int                      listen_fd_{-1};
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
///
/// When client_secrets is non-null, authentication is enabled: every command except AUTH, PING and
/// QUIT is refused until the session has answered a challenge. A null pointer means authentication
/// is off, and then the wire behaves exactly as it did before #30 - not one byte differs - except
/// that AUTH is refused, so a client configured to authenticate against a server that does not
/// finds out rather than believing it did.
std::string execute_command(const Command& cmd,
                            Engine& engine,
                            Session& session,
                            ServerStats& stats,
                            bool read_only = false,
                            MetricsRegistry* registry = nullptr,
                            ShardCoordinator* shard_coord = nullptr,
                            SubscriptionHub* hub = nullptr,
                            const SecretStore* client_secrets = nullptr);

/// Execute `INSERT` and `MINSERT` commands as one batch - the writes a client sent in one read.
///
/// Each is checked and answered exactly as `execute_command()` checks and answers it (read-only
/// node, bootstrap, shard ownership, the engine's status or what it threw), and the ones this node
/// takes are applied under one acquisition of the engine's lock (`Engine::apply_deltas()`, #155).
/// `answers[i]` is the wire answer to `writes[i]`. `execute_command()` answers a write through this
/// with a batch of one, so a write has one answer whichever path it took.
///
/// The authentication gate is the caller's: everything handed here must be a command
/// `deferrable_write()` said yes to. The metrics count what was taken, as one update per batch.
void execute_writes(std::span<const Command> writes,
                    Engine& engine,
                    Session& session,
                    ServerStats& stats,
                    bool read_only,
                    MetricsRegistry* registry,
                    ShardCoordinator* shard_coord,
                    std::vector<std::string>& answers);

/// Whether the event loop may hold `cmd` back, to apply it with the writes around it in the same
/// read: an `INSERT` or `MINSERT` that the authentication gate lets through. Everything else is
/// answered where it stands - and the held writes are applied and answered before it, so the
/// answers keep their order and a command after a write sees it.
bool deferrable_write(const Command& cmd, const Session& session,
                      const SecretStore* client_secrets);

/// Whether a command may run on a session that has not authenticated.
///
/// True for exactly AUTH (how a session authenticates), PING (so a load balancer's health check
/// needs no credentials and reveals nothing about the data), QUIT, and UNKNOWN (refused anyway).
///
/// Exposed so a test can iterate the enumeration instead of reading the source. The switch inside
/// has **no `default:` label**, so `-Wswitch` makes a new CommandType a build failure - and a test
/// refuses a `default:` being added, because that label is what would turn the compiler's
/// exhaustiveness check off and make the next command's classification an accident.
bool allowed_before_authentication(CommandType t);

/// Whether a command must run alone across every client event loop of this server.
///
/// With one loop every command was serialised by construction, and two were written for exactly
/// that: `FAILOVER` (two at once revoke one lease twice, and the one that loses clears the
/// winner's handover intent, its election block and its `handing_over_` flag while the handover is
/// still running) and `MIGRATE` (two of one symbol both pass validation before either starts).
/// With `--io-threads` above one, two connections can ask at once, so these take a mutex the
/// reactors share. Data commands do not: the engine is already called from the replication and
/// mesh threads concurrently, under its own locks, and `FLUSH` is serialised by the engine's
/// `flush_mtx_` because the flush thread calls it too.
///
/// Same shape as `allowed_before_authentication()`: no `default:`, so a new command does not build
/// until somebody decides which kind it is.
bool serialised_across_reactors(CommandType t);

/// Where a configuration value came from. For `--print-config`, which exists to answer exactly
/// that: a list of values does not tell an operator which of them they chose.
enum class Origin { Default, File, CommandLine, Profile };

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
    /// The CPUs this process can use, as the start of the process found them: the affinity mask
    /// and the tightest cgroup limit (`detect_machine()`). Logged at startup and printed by
    /// `--print-config`, because "how big is this machine" has two answers and a node sized by the
    /// wrong one is sized for a machine it does not have.
    MachineResources             machine;
    /// What the profile chose and why, one line - `boost`'s from `choose_boost()`. Logged at
    /// startup and printed by `--print-config` under the machine it was chosen for, so the two
    /// numbers a profile sets never appear without the reason they are what they are.
    std::string                  profile_choice;
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
