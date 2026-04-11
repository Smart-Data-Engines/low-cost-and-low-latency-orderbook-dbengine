#include "orderbook/metrics_server.hpp"
#include "orderbook/logger.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace ob {

MetricsServer::MetricsServer(uint16_t port, MetricsRegistry& registry)
    : port_(port), registry_(registry) {}

MetricsServer::~MetricsServer() { stop(); }

void MetricsServer::start() {
    // Create listen socket
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listen_fd_ < 0) {
        OB_LOG_ERROR("metrics", "socket() failed: %s", std::strerror(errno));
        return;
    }

    int opt = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port_);

    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        OB_LOG_ERROR("metrics", "bind() failed on port %u: %s", port_, std::strerror(errno));
        ::close(listen_fd_);
        listen_fd_ = -1;
        return;
    }

    if (::listen(listen_fd_, 8) < 0) {
        OB_LOG_ERROR("metrics", "listen() failed: %s", std::strerror(errno));
        ::close(listen_fd_);
        listen_fd_ = -1;
        return;
    }

    running_.store(true, std::memory_order_release);
    thread_ = std::thread([this] { run_loop(); });

    OB_LOG_INFO("metrics", "MetricsServer started on port %u", port_);
}

void MetricsServer::stop() {
    if (!running_.load(std::memory_order_acquire)) return;

    running_.store(false, std::memory_order_release);

    // Close listen fd to unblock epoll_wait
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }

    if (thread_.joinable()) {
        thread_.join();
    }

    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }
}

void MetricsServer::run_loop() {
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        OB_LOG_ERROR("metrics", "epoll_create1() failed: %s", std::strerror(errno));
        running_.store(false, std::memory_order_release);
        return;
    }

    epoll_event ev{};
    ev.events  = EPOLLIN;
    ev.data.fd = listen_fd_;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev) < 0) {
        OB_LOG_ERROR("metrics", "epoll_ctl() failed: %s", std::strerror(errno));
        running_.store(false, std::memory_order_release);
        return;
    }

    constexpr int kMaxEvents = 8;
    epoll_event events[kMaxEvents];

    while (running_.load(std::memory_order_acquire)) {
        int n = ::epoll_wait(epoll_fd_, events, kMaxEvents, 200 /*ms timeout*/);
        if (n < 0) {
            if (errno == EINTR) continue;
            break;
        }

        for (int i = 0; i < n; ++i) {
            if (events[i].data.fd == listen_fd_) {
                // Accept new connection
                int client_fd = ::accept4(listen_fd_, nullptr, nullptr,
                                          SOCK_CLOEXEC);
                if (client_fd < 0) {
                    if (errno == EMFILE || errno == ENFILE) {
                        OB_LOG_WARN("metrics", "accept() EMFILE/ENFILE: %s",
                                    std::strerror(errno));
                    }
                    continue;
                }
                handle_request(client_fd);
            }
        }
    }
}

void MetricsServer::handle_request(int client_fd) {
    char buf[4096];
    ssize_t nread = ::recv(client_fd, buf, sizeof(buf) - 1, 0);
    if (nread <= 0) {
        ::close(client_fd);
        return;
    }
    buf[nread] = '\0';

    bool is_metrics = false;
    bool valid = parse_http_request(buf, static_cast<size_t>(nread), is_metrics);

    std::string response;
    if (!valid) {
        response = "HTTP/1.1 400 Bad Request\r\n"
                   "Content-Length: 0\r\n"
                   "Connection: close\r\n\r\n";
    } else if (is_metrics) {
        std::string body = registry_.serialize();
        response = "HTTP/1.1 200 OK\r\n"
                   "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
                   "Content-Length: " + std::to_string(body.size()) + "\r\n"
                   "Connection: close\r\n\r\n" + body;
    } else {
        response = "HTTP/1.1 404 Not Found\r\n"
                   "Content-Length: 0\r\n"
                   "Connection: close\r\n\r\n";
    }

    // Best-effort send — metrics endpoint is non-critical
    ::send(client_fd, response.data(), response.size(), MSG_NOSIGNAL);
    ::close(client_fd);
}

bool MetricsServer::parse_http_request(const char* buf, size_t len, bool& is_metrics) {
    is_metrics = false;

    // Find end of request line (first \r\n or \n)
    const char* end = static_cast<const char*>(std::memchr(buf, '\n', len));
    if (!end) return false;

    size_t line_len = static_cast<size_t>(end - buf);
    // Strip trailing \r if present
    if (line_len > 0 && buf[line_len - 1] == '\r') --line_len;

    // Expect: "METHOD /path HTTP/1.x"
    // Minimal check: starts with "GET " and contains "/metrics"
    std::string_view line(buf, line_len);

    // Must have at least "GET / HTTP/1.0"
    if (line.size() < 14) return false;

    // Check HTTP version suffix
    if (line.find("HTTP/") == std::string_view::npos) return false;

    // Extract method and path
    auto first_space = line.find(' ');
    if (first_space == std::string_view::npos) return false;

    auto second_space = line.find(' ', first_space + 1);
    if (second_space == std::string_view::npos) return false;

    std::string_view method = line.substr(0, first_space);
    std::string_view path   = line.substr(first_space + 1, second_space - first_space - 1);

    if (method == "GET" && path == "/metrics") {
        is_metrics = true;
    }

    return true;
}

} // namespace ob
