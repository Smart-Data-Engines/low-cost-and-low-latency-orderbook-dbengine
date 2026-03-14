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

namespace ob {

// ── Server configuration ──────────────────────────────────────────────────────

struct ServerConfig {
    uint16_t    port{9090};
    std::string data_dir{"/tmp/ob_data"};
    int         max_sessions{64};
    int         worker_threads{4};
    size_t      max_line_length{8192};  // max command line bytes
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
    int                      listen_fd_{-1};
    int                      epoll_fd_{-1};

    void accept_connection();
    void handle_client_data(int fd);
};

// ── Free functions ────────────────────────────────────────────────────────────

/// Execute a command against the engine. Returns the wire-protocol response string.
std::string execute_command(const Command& cmd,
                            Engine& engine,
                            Session& session,
                            ServerStats& stats);

/// Parse CLI arguments into a ServerConfig. Applies defaults for missing args.
ServerConfig parse_cli_args(int argc, char* argv[]);

} // namespace ob
