// OrderbookClient + OrderbookPool implementation
// Covers: connection lifecycle, send/recv, parsers, formatters, public methods,
//         pool routing, health-check, failover.

#include "orderbook/client.hpp"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <sys/socket.h>

namespace ob {

// ── 3.1  Constructor / Destructor / Move ──────────────────────────────────────

OrderbookClient::OrderbookClient(ClientConfig config)
    : config_(std::move(config))
{
    send_buf_.reserve(65536);
    recv_buf_.reserve(65536);
}

OrderbookClient::~OrderbookClient() {
    if (fd_ != -1) disconnect();
}

OrderbookClient::OrderbookClient(OrderbookClient&& other) noexcept
    : config_(std::move(other.config_))
    , fd_(other.fd_)
    , send_buf_(std::move(other.send_buf_))
    , recv_buf_(std::move(other.recv_buf_))
    , sock_buf_(std::move(other.sock_buf_))
    , compressed_(other.compressed_)
{
    other.fd_ = -1;
}

OrderbookClient& OrderbookClient::operator=(OrderbookClient&& other) noexcept {
    if (this != &other) {
        if (fd_ != -1) disconnect();
        config_     = std::move(other.config_);
        fd_         = other.fd_;
        send_buf_   = std::move(other.send_buf_);
        recv_buf_   = std::move(other.recv_buf_);
        sock_buf_   = std::move(other.sock_buf_);
        compressed_ = other.compressed_;
        other.fd_   = -1;
    }
    return *this;
}

// ── 3.2  connect() ────────────────────────────────────────────────────────────

Result<void> OrderbookClient::connect() {
    if (fd_ != -1) disconnect();

    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0)
        return Result<void>::err(OB_ERR_IO, "socket() failed");

    // TCP_NODELAY immediately
    int flag = 1;
    ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    // Non-blocking for connect timeout
    int flags = ::fcntl(fd_, F_GETFL, 0);
    ::fcntl(fd_, F_SETFL, flags | O_NONBLOCK);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(config_.port);
    if (::inet_pton(AF_INET, config_.host.c_str(), &addr.sin_addr) != 1) {
        ::close(fd_); fd_ = -1;
        return Result<void>::err(OB_ERR_IO, "invalid host address");
    }

    int rc = ::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (rc < 0 && errno != EINPROGRESS) {
        ::close(fd_); fd_ = -1;
        return Result<void>::err(OB_ERR_IO, "connection refused");
    }

    if (rc < 0) {
        // EINPROGRESS — poll for completion
        pollfd pfd{};
        pfd.fd     = fd_;
        pfd.events = POLLOUT;
        int timeout_ms = static_cast<int>(config_.connect_timeout_sec * 1000);
        int pr = ::poll(&pfd, 1, timeout_ms);
        if (pr <= 0) {
            ::close(fd_); fd_ = -1;
            return Result<void>::err(OB_ERR_IO, "connect timeout");
        }
        int so_err = 0;
        socklen_t len = sizeof(so_err);
        ::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &so_err, &len);
        if (so_err != 0) {
            ::close(fd_); fd_ = -1;
            return Result<void>::err(OB_ERR_IO, "connection refused");
        }
    }

    // Restore blocking mode
    ::fcntl(fd_, F_SETFL, flags);

    // SO_RCVTIMEO for read timeout
    {
        timeval tv{};
        tv.tv_sec  = static_cast<time_t>(config_.read_timeout_sec);
        tv.tv_usec = static_cast<suseconds_t>(
            (config_.read_timeout_sec - tv.tv_sec) * 1'000'000);
        ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }

    // Read server welcome banner
    auto br = read_banner();
    if (!br) {
        ::close(fd_); fd_ = -1;
        return br;
    }

    // Optional LZ4 compression negotiation
    if (config_.compress) {
        auto cr = negotiate_compression();
        if (!cr) {
            ::close(fd_); fd_ = -1;
            return cr;
        }
    }

    return Result<void>::ok();
}


// ── 3.3  disconnect() / connected() ──────────────────────────────────────────

void OrderbookClient::disconnect() {
    if (fd_ == -1) return;  // no-op on inactive connection
    // Best-effort QUIT — ignore errors
    static constexpr char quit[] = "QUIT\n";
    ::send(fd_, quit, sizeof(quit) - 1, MSG_NOSIGNAL);
    ::close(fd_);
    fd_ = -1;
}

bool OrderbookClient::connected() const {
    return fd_ != -1;
}

// ── 4.1  send_all() / recv_response() ────────────────────────────────────────

Result<void> OrderbookClient::send_all(size_t len) {
    const char* ptr = send_buf_.data();
    size_t remaining = len;
    while (remaining > 0) {
        ssize_t n = ::send(fd_, ptr, remaining, MSG_NOSIGNAL);
        if (n <= 0) {
            if (n < 0 && (errno == EINTR)) continue;
            return Result<void>::err(OB_ERR_IO, "send failed");
        }
        ptr       += n;
        remaining -= static_cast<size_t>(n);
    }
    return Result<void>::ok();
}

Result<std::string_view> OrderbookClient::recv_response() {
    sock_buf_.clear();
    char tmp[4096];

    for (;;) {
        ssize_t n = ::recv(fd_, tmp, sizeof(tmp), 0);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            if (n == 0)
                return Result<std::string_view>::err(OB_ERR_IO, "connection closed");
            return Result<std::string_view>::err(OB_ERR_IO, "recv timeout");
        }
        sock_buf_.append(tmp, static_cast<size_t>(n));

        // Check terminators on accumulated data
        std::string_view sv(sock_buf_);

        // "\n\n" — OK responses, query data
        if (sv.size() >= 2 && sv.substr(sv.size() - 2) == "\n\n")
            return Result<std::string_view>::ok(sv);

        // "ERR ...\n"
        if (sv.starts_with("ERR ") && sv.back() == '\n')
            return Result<std::string_view>::ok(sv);

        // "PONG\n"
        if (sv == "PONG\n")
            return Result<std::string_view>::ok(sv);

        // ROLE responses: "PRIMARY ...\n", "REPLICA ...\n", "STANDALONE\n"
        if ((sv.starts_with("PRIMARY ") || sv.starts_with("REPLICA ") ||
             sv.starts_with("STANDALONE")) && sv.back() == '\n')
            return Result<std::string_view>::ok(sv);

        // Compression negotiation: "OK COMPRESS LZ4\n"
        if (sv == "OK COMPRESS LZ4\n")
            return Result<std::string_view>::ok(sv);
    }
}

// ── 4.2  read_banner() / negotiate_compression() ─────────────────────────────

Result<void> OrderbookClient::read_banner() {
    // Server sends a welcome banner terminated by \n\n. Read and discard.
    sock_buf_.clear();
    char tmp[4096];
    for (;;) {
        ssize_t n = ::recv(fd_, tmp, sizeof(tmp), 0);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            if (n == 0)
                return Result<void>::err(OB_ERR_IO, "connection closed during banner");
            return Result<void>::err(OB_ERR_IO, "recv timeout during banner");
        }
        sock_buf_.append(tmp, static_cast<size_t>(n));
        if (sock_buf_.size() >= 2 &&
            sock_buf_[sock_buf_.size() - 2] == '\n' &&
            sock_buf_[sock_buf_.size() - 1] == '\n')
            break;
    }
    sock_buf_.clear();
    return Result<void>::ok();
}

Result<void> OrderbookClient::negotiate_compression() {
#ifdef OB_CLIENT_LZ4
    static constexpr char cmd[] = "COMPRESS LZ4\n";
    send_buf_.assign(cmd, sizeof(cmd) - 1);
    auto sr = send_all(send_buf_.size());
    if (!sr) return sr;

    auto rr = recv_response();
    if (!rr) return Result<void>::err(rr.error_code(), rr.error_message());

    if (rr.value() == "OK COMPRESS LZ4\n") {
        compressed_ = true;
        return Result<void>::ok();
    }
    return Result<void>::err(OB_ERR_IO, "server does not support LZ4 compression");
#else
    return Result<void>::err(OB_ERR_IO, "LZ4 compression not compiled in");
#endif
}


// ── 4.3  Response parsers ────────────────────────────────────────────────────

Result<void> OrderbookClient::parse_ok_response(std::string_view resp) {
    if (resp == "OK\n\n")
        return Result<void>::ok();
    if (resp.starts_with("ERR ")) {
        // Strip "ERR " prefix and trailing "\n"
        auto msg = resp.substr(4);
        if (!msg.empty() && msg.back() == '\n')
            msg.remove_suffix(1);
        return Result<void>::err(OB_ERR_INTERNAL, std::string(msg));
    }
    return Result<void>::err(OB_ERR_PARSE, "unexpected response");
}

Result<QueryResult> OrderbookClient::parse_query_response(std::string_view resp) {
    // Expected format: "OK\n<header_tsv>\n<row1_tsv>\n...<rowN_tsv>\n\n"
    if (resp.starts_with("ERR ")) {
        auto msg = resp.substr(4);
        if (!msg.empty() && msg.back() == '\n')
            msg.remove_suffix(1);
        return Result<QueryResult>::err(OB_ERR_INTERNAL, std::string(msg));
    }

    // Must start with "OK\n"
    if (!resp.starts_with("OK\n"))
        return Result<QueryResult>::err(OB_ERR_PARSE, "unexpected response");

    // Strip "OK\n" prefix and trailing "\n\n"
    resp.remove_prefix(3);
    if (resp.size() >= 2 && resp.substr(resp.size() - 2) == "\n\n")
        resp.remove_suffix(2);

    // Empty result (was just "OK\n\n")
    if (resp.empty())
        return Result<QueryResult>::ok(QueryResult{});

    // Skip header line
    auto header_end = resp.find('\n');
    if (header_end == std::string_view::npos)
        return Result<QueryResult>::err(OB_ERR_PARSE, "missing header");
    resp.remove_prefix(header_end + 1);

    // Parse data rows
    QueryResult qr;
    while (!resp.empty()) {
        auto line_end = resp.find('\n');
        std::string_view line = (line_end == std::string_view::npos)
                                    ? resp : resp.substr(0, line_end);
        if (line.empty()) {
            if (line_end != std::string_view::npos)
                resp.remove_prefix(line_end + 1);
            else
                break;
            continue;
        }

        // Parse TSV columns: timestamp_ns \t price \t quantity \t order_count \t side \t level
        QueryRow row{};
        const char* p   = line.data();
        const char* end = p + line.size();

        // timestamp_ns
        auto [p1, ec1] = std::from_chars(p, end, row.timestamp_ns);
        if (ec1 != std::errc{} || p1 >= end || *p1 != '\t')
            return Result<QueryResult>::err(OB_ERR_PARSE, "bad timestamp_ns");
        p = p1 + 1;

        // price
        auto [p2, ec2] = std::from_chars(p, end, row.price);
        if (ec2 != std::errc{} || p2 >= end || *p2 != '\t')
            return Result<QueryResult>::err(OB_ERR_PARSE, "bad price");
        p = p2 + 1;

        // quantity
        auto [p3, ec3] = std::from_chars(p, end, row.quantity);
        if (ec3 != std::errc{} || p3 >= end || *p3 != '\t')
            return Result<QueryResult>::err(OB_ERR_PARSE, "bad quantity");
        p = p3 + 1;

        // order_count
        auto [p4, ec4] = std::from_chars(p, end, row.order_count);
        if (ec4 != std::errc{} || p4 >= end || *p4 != '\t')
            return Result<QueryResult>::err(OB_ERR_PARSE, "bad order_count");
        p = p4 + 1;

        // side
        auto [p5, ec5] = std::from_chars(p, end, row.side);
        if (ec5 != std::errc{} || p5 >= end || *p5 != '\t')
            return Result<QueryResult>::err(OB_ERR_PARSE, "bad side");
        p = p5 + 1;

        // level
        auto [p6, ec6] = std::from_chars(p, end, row.level);
        if (ec6 != std::errc{})
            return Result<QueryResult>::err(OB_ERR_PARSE, "bad level");

        qr.rows.push_back(row);

        if (line_end == std::string_view::npos) break;
        resp.remove_prefix(line_end + 1);
    }

    return Result<QueryResult>::ok(std::move(qr));
}

RoleInfo OrderbookClient::parse_role_response(std::string_view resp) {
    // Strip trailing \n
    if (!resp.empty() && resp.back() == '\n')
        resp.remove_suffix(1);

    RoleInfo info{};

    if (resp.starts_with("PRIMARY ")) {
        info.role = NodeRole::PRIMARY;
        auto epoch_sv = resp.substr(8);
        std::from_chars(epoch_sv.data(), epoch_sv.data() + epoch_sv.size(), info.epoch);
    } else if (resp.starts_with("REPLICA ")) {
        info.role = NodeRole::REPLICA;
        auto rest = resp.substr(8);
        auto sp = rest.rfind(' ');
        if (sp != std::string_view::npos) {
            info.primary_address = std::string(rest.substr(0, sp));
            auto epoch_sv = rest.substr(sp + 1);
            std::from_chars(epoch_sv.data(), epoch_sv.data() + epoch_sv.size(), info.epoch);
        }
    } else {
        // STANDALONE or unknown — default to STANDALONE
        info.role = NodeRole::STANDALONE;
    }

    return info;
}


// ── 5.1  Command formatters ──────────────────────────────────────────────────

size_t OrderbookClient::format_insert(std::string_view symbol,
                                       std::string_view exchange,
                                       Side side, int64_t price,
                                       uint64_t qty, uint32_t count) {
    send_buf_.clear();
    // Worst case: "INSERT " + sym(31) + " " + exch(31) + " ask " + int64 + " " + uint64 + " " + uint32 + "\n"
    // Well under 256 bytes. Resize to capacity, write, then shrink.
    send_buf_.resize(send_buf_.capacity());
    char* buf = send_buf_.data();
    char* p   = buf;

    std::memcpy(p, "INSERT ", 7); p += 7;
    std::memcpy(p, symbol.data(), symbol.size()); p += symbol.size();
    *p++ = ' ';
    std::memcpy(p, exchange.data(), exchange.size()); p += exchange.size();
    *p++ = ' ';

    if (side == Side::BID) {
        std::memcpy(p, "bid", 3); p += 3;
    } else {
        std::memcpy(p, "ask", 3); p += 3;
    }
    *p++ = ' ';

    auto [p1, ec1] = std::to_chars(p, buf + send_buf_.size(), price);
    p = p1;
    *p++ = ' ';

    auto [p2, ec2] = std::to_chars(p, buf + send_buf_.size(), qty);
    p = p2;
    *p++ = ' ';

    auto [p3, ec3] = std::to_chars(p, buf + send_buf_.size(), count);
    p = p3;
    *p++ = '\n';

    size_t len = static_cast<size_t>(p - buf);
    send_buf_.resize(len);
    return len;
}

size_t OrderbookClient::format_minsert(std::string_view symbol,
                                        std::string_view exchange,
                                        Side side, const Level* levels,
                                        size_t n_levels) {
    send_buf_.clear();
    send_buf_.resize(send_buf_.capacity());
    char* buf = send_buf_.data();
    char* p   = buf;
    char* end = buf + send_buf_.size();

    // Header: "MINSERT <sym> <exch> <bid|ask> <n>\n"
    std::memcpy(p, "MINSERT ", 8); p += 8;
    std::memcpy(p, symbol.data(), symbol.size()); p += symbol.size();
    *p++ = ' ';
    std::memcpy(p, exchange.data(), exchange.size()); p += exchange.size();
    *p++ = ' ';

    if (side == Side::BID) {
        std::memcpy(p, "bid", 3); p += 3;
    } else {
        std::memcpy(p, "ask", 3); p += 3;
    }
    *p++ = ' ';

    auto [pn, ecn] = std::to_chars(p, end, n_levels);
    p = pn;
    *p++ = '\n';

    // Level lines: "<price> <qty> <count>\n"
    for (size_t i = 0; i < n_levels; ++i) {
        // Check if we need more space
        size_t used = static_cast<size_t>(p - buf);
        if (used + 80 > send_buf_.size()) {
            // Grow buffer
            size_t offset = static_cast<size_t>(p - buf);
            send_buf_.resize(send_buf_.size() * 2);
            buf = send_buf_.data();
            p   = buf + offset;
            end = buf + send_buf_.size();
        }

        auto [pp, ecp] = std::to_chars(p, end, levels[i].price);
        p = pp;
        *p++ = ' ';

        auto [pq, ecq] = std::to_chars(p, end, levels[i].qty);
        p = pq;
        *p++ = ' ';

        auto [pc, ecc] = std::to_chars(p, end, levels[i].count);
        p = pc;
        *p++ = '\n';
    }

    size_t len = static_cast<size_t>(p - buf);
    send_buf_.resize(len);
    return len;
}

size_t OrderbookClient::format_simple(std::string_view cmd) {
    send_buf_.clear();
    send_buf_.reserve(cmd.size() + 1);
    send_buf_.append(cmd.data(), cmd.size());
    send_buf_.push_back('\n');
    return send_buf_.size();
}

size_t OrderbookClient::format_query(std::string_view sql) {
    send_buf_.clear();
    send_buf_.reserve(sql.size() + 1);
    send_buf_.append(sql.data(), sql.size());
    send_buf_.push_back('\n');
    return send_buf_.size();
}

// ── 5.2  Public methods ──────────────────────────────────────────────────────

Result<void> OrderbookClient::insert(std::string_view symbol,
                                      std::string_view exchange,
                                      Side side, int64_t price,
                                      uint64_t qty, uint32_t count) {
    if (side != Side::BID && side != Side::ASK)
        return Result<void>::err(OB_ERR_INVALID_ARG, "invalid side");

    size_t len = format_insert(symbol, exchange, side, price, qty, count);
    auto sr = send_all(len);
    if (!sr) return sr;

    auto rr = recv_response();
    if (!rr) return Result<void>::err(rr.error_code(), rr.error_message());

    return parse_ok_response(rr.value());
}

Result<void> OrderbookClient::minsert(std::string_view symbol,
                                       std::string_view exchange,
                                       Side side, const Level* levels,
                                       size_t n_levels) {
    if (n_levels == 0)
        return Result<void>::err(OB_ERR_INVALID_ARG, "empty levels");
    if (n_levels > 1000)
        return Result<void>::err(OB_ERR_INVALID_ARG, "too many levels");

    size_t len = format_minsert(symbol, exchange, side, levels, n_levels);
    auto sr = send_all(len);
    if (!sr) return sr;

    auto rr = recv_response();
    if (!rr) return Result<void>::err(rr.error_code(), rr.error_message());

    return parse_ok_response(rr.value());
}

Result<void> OrderbookClient::flush() {
    size_t len = format_simple("FLUSH");
    auto sr = send_all(len);
    if (!sr) return sr;

    auto rr = recv_response();
    if (!rr) return Result<void>::err(rr.error_code(), rr.error_message());

    return parse_ok_response(rr.value());
}

Result<QueryResult> OrderbookClient::query(std::string_view sql) {
    size_t len = format_query(sql);
    auto sr = send_all(len);
    if (!sr) return Result<QueryResult>::err(sr.error_code(), sr.error_message());

    auto rr = recv_response();
    if (!rr) return Result<QueryResult>::err(rr.error_code(), rr.error_message());

    return parse_query_response(rr.value());
}

Result<bool> OrderbookClient::ping() {
    size_t len = format_simple("PING");
    auto sr = send_all(len);
    if (!sr) return Result<bool>::err(sr.error_code(), sr.error_message());

    auto rr = recv_response();
    if (!rr) return Result<bool>::err(rr.error_code(), rr.error_message());

    if (rr.value() == "PONG\n")
        return Result<bool>::ok(true);

    return Result<bool>::err(OB_ERR_PARSE, "unexpected response");
}

Result<RoleInfo> OrderbookClient::role() {
    size_t len = format_simple("ROLE");
    auto sr = send_all(len);
    if (!sr) return Result<RoleInfo>::err(sr.error_code(), sr.error_message());

    auto rr = recv_response();
    if (!rr) return Result<RoleInfo>::err(rr.error_code(), rr.error_message());

    return Result<RoleInfo>::ok(parse_role_response(rr.value()));
}

// ═══════════════════════════════════════════════════════════════════════════════
// OrderbookPool implementation — Tasks 7.1–7.3
// ═══════════════════════════════════════════════════════════════════════════════

// ── 7.1  Constructor / Destructor / close() ──────────────────────────────────

OrderbookPool::OrderbookPool(PoolConfig config)
    : config_(std::move(config))
{
    // Parse host list and create a client for each host
    for (const auto& host_str : config_.hosts) {
        NodeState ns;
        auto colon = host_str.rfind(':');
        if (colon != std::string::npos) {
            ns.host = host_str.substr(0, colon);
            auto port_sv = std::string_view(host_str).substr(colon + 1);
            uint16_t port_val = 9090;
            std::from_chars(port_sv.data(), port_sv.data() + port_sv.size(), port_val);
            ns.port = port_val;
        } else {
            ns.host = host_str;
            ns.port = 9090;
        }

        ClientConfig cc;
        cc.host                = ns.host;
        cc.port                = ns.port;
        cc.connect_timeout_sec = config_.connect_timeout_sec;
        cc.read_timeout_sec    = config_.read_timeout_sec;
        cc.compress            = config_.compress;

        nodes_.push_back(std::move(ns));
        clients_.push_back(std::make_unique<OrderbookClient>(std::move(cc)));
    }

    connect_all();
    discover_primary();

    // Start health-check thread
    running_.store(true, std::memory_order_release);
    health_thread_ = std::thread(&OrderbookPool::health_check_loop, this);
}

OrderbookPool::~OrderbookPool() {
    close();
}

void OrderbookPool::close() {
    // Signal health thread to stop
    bool was_running = running_.exchange(false, std::memory_order_acq_rel);
    if (was_running && health_thread_.joinable()) {
        health_thread_.join();
    }

    // Close all clients
    std::lock_guard<std::mutex> lock(mtx_);
    for (size_t i = 0; i < clients_.size(); ++i) {
        if (nodes_[i].connected) {
            clients_[i]->disconnect();
            nodes_[i].connected = false;
        }
    }
    primary_idx_ = -1;
}

void OrderbookPool::connect_all() {
    for (size_t i = 0; i < clients_.size(); ++i) {
        auto res = clients_[i]->connect();
        nodes_[i].connected = res.has_value();
    }
}

void OrderbookPool::discover_primary() {
    primary_idx_ = -1;
    for (size_t i = 0; i < clients_.size(); ++i) {
        if (!nodes_[i].connected) continue;

        auto rr = clients_[i]->role();
        if (!rr) {
            nodes_[i].connected = false;
            continue;
        }

        const auto& info = rr.value();
        nodes_[i].role  = info.role;
        nodes_[i].epoch = info.epoch;

        if (info.role == NodeRole::PRIMARY) {
            primary_idx_ = static_cast<int>(i);
        }
    }

    // If no PRIMARY found, look for STANDALONE as fallback
    if (primary_idx_ < 0) {
        for (size_t i = 0; i < nodes_.size(); ++i) {
            if (nodes_[i].connected && nodes_[i].role == NodeRole::STANDALONE) {
                primary_idx_ = static_cast<int>(i);
                break;
            }
        }
    }
}

// ── 7.2  Health-check loop ───────────────────────────────────────────────────

void OrderbookPool::health_check_loop() {
    using clock = std::chrono::steady_clock;
    auto interval = std::chrono::milliseconds(
        static_cast<int64_t>(config_.health_check_interval_sec * 1000));

    while (running_.load(std::memory_order_acquire)) {
        // Sleep in small increments so we can exit quickly
        auto deadline = clock::now() + interval;
        while (clock::now() < deadline) {
            if (!running_.load(std::memory_order_acquire)) return;
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }

        std::lock_guard<std::mutex> lock(mtx_);
        int new_primary = -1;

        for (size_t i = 0; i < clients_.size(); ++i) {
            // Attempt reconnect for disconnected nodes
            if (!nodes_[i].connected) {
                auto cr = clients_[i]->connect();
                nodes_[i].connected = cr.has_value();
                if (!nodes_[i].connected) continue;
            }

            // Send ROLE to each connected node
            auto rr = clients_[i]->role();
            if (!rr) {
                // Node became unreachable
                nodes_[i].connected = false;
                continue;
            }

            const auto& info = rr.value();
            nodes_[i].role  = info.role;
            nodes_[i].epoch = info.epoch;

            if (info.role == NodeRole::PRIMARY) {
                new_primary = static_cast<int>(i);
            }
        }

        // Update primary — prefer PRIMARY, fall back to STANDALONE
        if (new_primary >= 0) {
            primary_idx_ = new_primary;
        } else {
            primary_idx_ = -1;
            for (size_t i = 0; i < nodes_.size(); ++i) {
                if (nodes_[i].connected && nodes_[i].role == NodeRole::STANDALONE) {
                    primary_idx_ = static_cast<int>(i);
                    break;
                }
            }
        }
    }
}

// ── 7.3  Routing: get_primary / get_any_reader ───────────────────────────────

OrderbookClient* OrderbookPool::get_primary() {
    if (primary_idx_ >= 0 &&
        primary_idx_ < static_cast<int>(nodes_.size()) &&
        nodes_[static_cast<size_t>(primary_idx_)].connected) {
        return clients_[static_cast<size_t>(primary_idx_)].get();
    }
    return nullptr;
}

OrderbookClient* OrderbookPool::get_any_reader() {
    size_t n = clients_.size();
    if (n == 0) return nullptr;

    // Round-robin over all connected nodes
    for (size_t attempt = 0; attempt < n; ++attempt) {
        size_t idx = (read_idx_ + attempt) % n;
        if (nodes_[idx].connected) {
            read_idx_ = (idx + 1) % n;
            return clients_[idx].get();
        }
    }
    return nullptr;
}

// ── 7.3  execute_write / execute_read ────────────────────────────────────────

template<typename F>
auto OrderbookPool::execute_write(F&& fn) -> decltype(fn(std::declval<OrderbookClient&>())) {
    using R = decltype(fn(std::declval<OrderbookClient&>()));

    std::lock_guard<std::mutex> lock(mtx_);

    OrderbookClient* primary = get_primary();
    if (!primary) {
        return R::err(OB_ERR_NOT_FOUND, "no primary available");
    }

    auto result = fn(*primary);
    if (result) return result;

    // Check if error is IO or "read-only" — trigger re-discovery + retry
    bool should_retry = (result.error_code() == OB_ERR_IO) ||
                        (result.error_message().find("read-only") != std::string::npos) ||
                        (result.error_message().find("read only") != std::string::npos);

    if (!should_retry) return result;

    // Re-discover primary
    discover_primary();

    primary = get_primary();
    if (!primary) {
        return R::err(OB_ERR_NOT_FOUND, "no primary available");
    }

    // Retry once
    return fn(*primary);
}

template<typename F>
auto OrderbookPool::execute_read(F&& fn) -> decltype(fn(std::declval<OrderbookClient&>())) {
    using R = decltype(fn(std::declval<OrderbookClient&>()));

    std::lock_guard<std::mutex> lock(mtx_);

    size_t n = clients_.size();
    if (n == 0) {
        return R::err(OB_ERR_IO, "all hosts unreachable");
    }

    // Try each node starting from round-robin position
    size_t start = read_idx_;
    for (size_t attempt = 0; attempt < n; ++attempt) {
        size_t idx = (start + attempt) % n;
        if (!nodes_[idx].connected) continue;

        read_idx_ = (idx + 1) % n;
        auto result = fn(*clients_[idx]);
        if (result) return result;

        // Mark node as disconnected on IO error
        if (result.error_code() == OB_ERR_IO) {
            nodes_[idx].connected = false;
        }
    }

    return R::err(OB_ERR_IO, "all hosts unreachable");
}

// ── 7.3  Public methods — write routing ──────────────────────────────────────

Result<void> OrderbookPool::insert(std::string_view symbol,
                                    std::string_view exchange,
                                    Side side, int64_t price,
                                    uint64_t qty, uint32_t count) {
    return execute_write([&](OrderbookClient& c) {
        return c.insert(symbol, exchange, side, price, qty, count);
    });
}

Result<void> OrderbookPool::minsert(std::string_view symbol,
                                     std::string_view exchange,
                                     Side side, const Level* levels,
                                     size_t n_levels) {
    return execute_write([&](OrderbookClient& c) {
        return c.minsert(symbol, exchange, side, levels, n_levels);
    });
}

Result<void> OrderbookPool::flush() {
    return execute_write([](OrderbookClient& c) {
        return c.flush();
    });
}

// ── 7.3  Public methods — read routing ───────────────────────────────────────

Result<QueryResult> OrderbookPool::query(std::string_view sql) {
    return execute_read([&](OrderbookClient& c) {
        return c.query(sql);
    });
}

Result<bool> OrderbookPool::ping() {
    return execute_read([](OrderbookClient& c) {
        return c.ping();
    });
}

} // namespace ob
