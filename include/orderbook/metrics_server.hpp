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
    /// `bind_address` empty means every interface, which is what this did before the parameter
    /// existed. A loopback or private address is the control that matters here: the endpoint has no
    /// authentication and deliberately none (#30 §8), because a Prometheus scraper cannot perform a
    /// challenge-response and a bearer token would be the weaker mechanism that ends up being used.
    MetricsServer(uint16_t port, MetricsRegistry& registry, std::string bind_address = {});
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
    std::string       bind_address_;
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
