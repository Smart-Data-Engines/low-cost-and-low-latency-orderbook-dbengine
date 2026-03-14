// tools/ob_tcp_server.cpp — TCP server executable for orderbook-dbengine.
//
// Usage:
//   ./ob_tcp_server [--port PORT] [--data-dir DIR] [--max-sessions N] [--workers N]
//
// Signals:
//   SIGINT / SIGTERM → graceful shutdown

#include "orderbook/tcp_server.hpp"

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>

// ── Global shutdown flag ──────────────────────────────────────────────────────

static std::atomic<bool> g_shutdown_requested{false};

static void signal_handler(int /*signum*/) {
    g_shutdown_requested.store(true, std::memory_order_relaxed);
}

// ── Help ──────────────────────────────────────────────────────────────────────

static void print_usage(const char* prog) {
    std::printf(
        "Usage: %s [OPTIONS]\n"
        "\n"
        "Options:\n"
        "  --port <PORT>          TCP port to listen on (default: 9090)\n"
        "  --data-dir <DIR>       Data directory for the engine (default: /tmp/ob_data)\n"
        "  --max-sessions <N>     Maximum concurrent client sessions (default: 64)\n"
        "  --workers <N>          Number of worker threads (default: 4)\n"
        "  --help                 Show this help message and exit\n",
        prog);
}

// ── Main ──────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    // Check for --help before full CLI parsing.
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--help") == 0 || std::strcmp(argv[i], "-h") == 0) {
            print_usage(argv[0]);
            return 0;
        }
    }

    ob::ServerConfig config = ob::parse_cli_args(argc, argv);

    // Set up signal handlers for graceful shutdown.
    struct sigaction sa{};
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT,  &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    std::printf("ob_tcp_server v0.1.0 listening on port %u, data-dir: %s\n",
                static_cast<unsigned>(config.port), config.data_dir.c_str());

    ob::TcpServer server(std::move(config));

    // Monitor thread: polls g_shutdown_requested and calls server.shutdown().
    std::thread monitor([&server]() {
        while (!g_shutdown_requested.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        server.shutdown();
    });

    server.run(); // blocks until shutdown

    std::printf("Shutting down...\n");

    monitor.join();

    return 0;
}
