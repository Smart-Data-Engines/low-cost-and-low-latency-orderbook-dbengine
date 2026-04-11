#pragma once

#include "orderbook/metrics.hpp"

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>

namespace ob {

/// Minimal HTTP server exposing Prometheus metrics on a dedicated port.
/// Runs in a separate thread with an epoll event loop.
class MetricsServer {
public:
    MetricsServer(uint16_t port, MetricsRegistry& registry);
    ~MetricsServer();

    // Non-copyable, non-movable
    MetricsServer(const MetricsServer&) = delete;
    MetricsServer& operator=(const MetricsServer&) = delete;

    /// Start the server thread. If bind fails, logs an error and returns
    /// without starting the thread (the application continues without metrics).
    void start();

    /// Signal the server to stop and join the thread.
    void stop();

    /// Returns true if the server thread is running.
    bool is_running() const noexcept { return running_.load(std::memory_order_relaxed); }

private:
    uint16_t          port_;
    MetricsRegistry&  registry_;
    std::atomic<bool> running_{false};
    std::thread       thread_;
    int               listen_fd_{-1};
    int               epoll_fd_{-1};

    /// Main epoll loop executed in the server thread.
    void run_loop();

    /// Handle a single HTTP request on client_fd, then close it.
    void handle_request(int client_fd);

    /// Minimal HTTP request parser.
    /// Returns true if the request line was parsed successfully.
    /// Sets is_metrics to true if the request is "GET /metrics".
    static bool parse_http_request(const char* buf, size_t len, bool& is_metrics);
};

} // namespace ob
