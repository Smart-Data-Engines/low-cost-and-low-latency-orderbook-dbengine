// tools/ob_tcp_server.cpp — TCP server executable for orderbook-dbengine.
//
// Usage:
//   ./ob_tcp_server [--port PORT] [--data-dir DIR] [--max-sessions N] [--workers N]
//
// Signals:
//   SIGINT / SIGTERM → graceful shutdown

#include "orderbook/tcp_server.hpp"
#include "orderbook/version.hpp"
#ifdef OB_USE_IO_URING
#include "orderbook/io_uring_server.hpp"
#endif

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

// The text itself lives in `ob::format_usage()`, generated from the parser's own flag list. It was
// six hardcoded lines here for forty accepted flags, and `--help` is the first command anyone runs.

// ── Main ──────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    // Check for --help before full CLI parsing.
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--help") == 0 || std::strcmp(argv[i], "-h") == 0) {
            std::printf("%s", ob::format_usage(argv[0]).c_str());
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

    // Ignore SIGPIPE. Writing to a socket whose peer has gone raises it, and the
    // default action is to kill the process — one disconnecting client would take
    // the server and every other session down with it. Individual writes use
    // MSG_NOSIGNAL, so this is the net for any path that forgets to.
    signal(SIGPIPE, SIG_IGN);

    // "Starting", not "listening", and flushed.
    //
    // This line said `listening on port N` and ran *before* the server was constructed, so it
    // announced a socket that did not exist yet — and if the bind then failed for a taken port, the
    // output claimed to be listening with the error underneath it. It also went to `stdout` through
    // `printf` with no flush, which is block-buffered when redirected to a file, a pipe or a
    // journal: the one line an operator greps to confirm a start arrived at process **exit**. Every
    // other line was on time because the logger writes to unbuffered `stderr`. Measured: the banner
    // was absent from a node's log file while the node was up and answering (#90).
    //
    // The actual listen is logged by the server once the bind has succeeded.
    std::printf("ob_tcp_server v%s starting on port %u, data-dir: %s\n",
                std::string(ob::version()).c_str(),
                static_cast<unsigned>(config.port), config.data_dir.c_str());
    std::fflush(stdout);

#ifdef OB_USE_IO_URING
    ob::IoUringServer server(std::move(config));
#else
    ob::TcpServer server(std::move(config));
#endif

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
