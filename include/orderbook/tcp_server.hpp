#pragma once

#include "orderbook/command_parser.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/response_formatter.hpp"
#include "orderbook/session.hpp"

#include <atomic>
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
    std::string node_id;                             // --node-id
    bool failover_enabled{true};                     // --failover-enabled

    // TTL / data retention
    uint64_t ttl_hours{0};                    // --ttl-hours (0 = disabled)
    uint64_t ttl_scan_interval_seconds{300};  // --ttl-scan-interval-seconds
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
    std::atomic<bool>        running_{false};
    std::atomic<bool>        draining_{false};  // drain phase: reject new connections, finish in-flight
    int                      listen_fd_{-1};
    int                      epoll_fd_{-1};

    void accept_connection();
    void handle_client_data(int fd);
};

// ── Free functions ────────────────────────────────────────────────────────────

/// Execute a command against the engine. Returns the wire-protocol response string.
/// When read_only is true, INSERT and FLUSH commands are rejected with an error.
std::string execute_command(const Command& cmd,
                            Engine& engine,
                            Session& session,
                            ServerStats& stats,
                            bool read_only = false);

/// Parse CLI arguments into a ServerConfig. Applies defaults for missing args.
ServerConfig parse_cli_args(int argc, char* argv[]);

} // namespace ob
