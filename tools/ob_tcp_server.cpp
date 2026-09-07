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
#include <exception>
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

// ── Shutdown monitor ──────────────────────────────────────────────────────────

/// Joins the shutdown monitor thread on every exit path, including one taken by an exception.
///
/// A `std::thread` that is still joinable when its destructor runs calls `std::terminate`. That is
/// why a failed bind left by SIGABRT even though its message had already been printed: `run()`
/// threw, and the monitor thread was destroyed mid-unwind (#102). It is the same mechanism as #88,
/// where the abort said "terminate called without an active exception" and the search for an
/// uncaught throw was the wrong search. A `catch` alone does not fix it — the catch block would
/// return past the very destructor that aborts.
///
/// The flag is set here rather than left to the signal handler, so the join is bounded whatever
/// made `run()` return.
struct MonitorJoin {
    std::atomic<bool>& done;
    std::thread&       thread;

    ~MonitorJoin() {
        done.store(true, std::memory_order_relaxed);
        if (thread.joinable()) thread.join();
    }
};

// ── Main ──────────────────────────────────────────────────────────────────────

static int run_server(int argc, char* argv[]) {
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
    //
    // `monitor_done` is a second flag rather than a reuse of the first, and the difference is what
    // the log says. Reusing it made a failed bind print "Shutdown requested - the epoll loop will
    // drain and close" on the way out, which reads as an operator having sent a signal. A line
    // announcing something nobody asked for is the same defect as a line announcing a guarantee the
    // code does not give.
    std::atomic<bool> monitor_done{false};
    std::thread monitor([&server, &monitor_done]() {
        while (!g_shutdown_requested.load(std::memory_order_relaxed) &&
               !monitor_done.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (g_shutdown_requested.load(std::memory_order_relaxed)) {
            server.shutdown();
        }
    });
    // Declared after `monitor`, so it is destroyed first: the thread's body holds references to
    // `server` and to `monitor_done`, and joining has to happen before either goes away.
    const MonitorJoin monitor_join{monitor_done, monitor};

    server.run(); // blocks until shutdown

    std::printf("Shutting down...\n");

    return 0;
}

int main(int argc, char* argv[]) {
    // Check for --help before full CLI parsing.
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--help") == 0 || std::strcmp(argv[i], "-h") == 0) {
            std::printf("%s", ob::format_usage(argv[0]).c_str());
            return 0;
        }
    }

    // A refusal to start is a refusal, not a crash.
    //
    // `TcpServer::run()` throws for every startup condition it cannot proceed past — a port already
    // in use, most often — and with nothing catching it the process left through the default
    // terminate handler: SIGABRT, exit -6, possibly a core file, and a supervisor that logs a crash
    // and applies its crash restart policy to what is actually a configuration mistake. The
    // message was never the problem; the exit mode was.
    //
    // The contrast is the argument for this shape: `load_secrets_or_exit()` and
    // `load_tls_or_exit()` are named for what they do and print `Error: <what>` before exiting 1.
    // The listen path was the one that did not.
    try {
        return run_server(argc, argv);
    } catch (const std::exception& e) {
        // Same wording as the or_exit helpers, so one grep finds a startup refusal whichever
        // condition caused it.
        std::fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    } catch (...) {
        std::fprintf(stderr, "Error: startup failed with an unknown exception\n");
        return 1;
    }
}
