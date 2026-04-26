#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <poll.h>
#include <fcntl.h>

#include "orderbook/types.hpp"

namespace ob {

// Forward declaration for sharding support
class ShardRouter;

// ── Configuration ─────────────────────────────────────────────────────────────

/// Client configuration for a single TCP connection.
struct ClientConfig {
    std::string host = "127.0.0.1";
    uint16_t    port = 9090;
    double      connect_timeout_sec = 5.0;
    double      read_timeout_sec    = 10.0;
    bool        compress            = false;  // negotiate LZ4
};

// ── Data types ────────────────────────────────────────────────────────────────

/// Side of the orderbook.
enum class Side : uint8_t { BID = 0, ASK = 1 };

/// A single price level (for insert/minsert).
struct Level {
    int64_t  price;
    uint64_t qty;
    uint32_t count = 1;
};

/// A single row from a SELECT query result.
struct QueryRow {
    uint64_t timestamp_ns;
    int64_t  price;
    uint64_t quantity;
    uint32_t order_count;
    uint8_t  side;       // 0=bid, 1=ask
    uint16_t level;
};

/// Result of a SELECT query.
struct QueryResult {
    std::vector<QueryRow> rows;
};

/// Node role in the cluster.
enum class NodeRole : uint8_t {
    STANDALONE = 0,
    PRIMARY    = 1,
    REPLICA    = 2
};

/// Result of the ROLE command.
struct RoleInfo {
    NodeRole    role;
    uint64_t    epoch = 0;
    std::string primary_address;  // populated only for REPLICA
};

// ── OrderbookClient ───────────────────────────────────────────────────────────

class OrderbookClient {
public:
    explicit OrderbookClient(ClientConfig config = {});
    ~OrderbookClient();

    // Move-only semantics
    OrderbookClient(OrderbookClient&& other) noexcept;
    OrderbookClient& operator=(OrderbookClient&& other) noexcept;
    OrderbookClient(const OrderbookClient&) = delete;
    OrderbookClient& operator=(const OrderbookClient&) = delete;

    /// Establish TCP connection. Reads the server welcome banner.
    Result<void> connect();

    /// Close connection (sends QUIT).
    void disconnect();

    /// Is the connection active?
    bool connected() const;

    // ── Data operations ──────────────────────────────────────────────
    Result<void>        insert(std::string_view symbol, std::string_view exchange,
                               Side side, int64_t price, uint64_t qty,
                               uint32_t count = 1);
    Result<void>        minsert(std::string_view symbol, std::string_view exchange,
                                Side side, const Level* levels, size_t n_levels);
    Result<void>        flush();
    Result<QueryResult> query(std::string_view sql);

    // ── Diagnostics ──────────────────────────────────────────────────
    Result<bool>     ping();
    Result<RoleInfo> role();

    // ── Command formatting (public for property-based testing) ───────
    size_t format_insert(std::string_view symbol, std::string_view exchange,
                         Side side, int64_t price, uint64_t qty, uint32_t count);
    size_t format_minsert(std::string_view symbol, std::string_view exchange,
                          Side side, const Level* levels, size_t n_levels);
    size_t format_simple(std::string_view cmd);
    size_t format_query(std::string_view sql);

    // ── Response parsing (public for property-based testing) ─────────
    Result<void>        parse_ok_response(std::string_view resp);
    Result<QueryResult> parse_query_response(std::string_view resp);
    RoleInfo            parse_role_response(std::string_view resp);

    /// Access to the send buffer (for testing).
    const std::string& send_buffer() const { return send_buf_; }

private:
    ClientConfig config_;
    int          fd_ = -1;
    std::string  send_buf_;   // pre-allocated 64KB
    std::string  recv_buf_;   // pre-allocated 64KB
    std::string  sock_buf_;   // socket read accumulation buffer
    bool         compressed_ = false;

    // Communication
    Result<void>             send_all(size_t len);
    Result<std::string_view> recv_response();
    Result<void>             read_banner();
    Result<void>             negotiate_compression();
};

// ── Pool configuration ────────────────────────────────────────────────────────

struct PoolConfig {
    std::vector<std::string> hosts;  // "host:port" or "host" (default port 9090)
    double connect_timeout_sec       = 5.0;
    double read_timeout_sec          = 10.0;
    double health_check_interval_sec = 2.0;
    bool   compress                  = false;

    // Sharding (optional — when set, enables ShardRouter)
    std::vector<std::string> coordinator_endpoints;  // etcd endpoints
    std::string cluster_prefix{"/ob/"};
};

// ── OrderbookPool ─────────────────────────────────────────────────────────────

class OrderbookPool {
public:
    explicit OrderbookPool(PoolConfig config);
    ~OrderbookPool();

    // Non-copyable, non-movable
    OrderbookPool(const OrderbookPool&) = delete;
    OrderbookPool& operator=(const OrderbookPool&) = delete;
    OrderbookPool(OrderbookPool&&) = delete;
    OrderbookPool& operator=(OrderbookPool&&) = delete;

    // ── Write operations (routed to PRIMARY) ─────────────────────────
    Result<void> insert(std::string_view symbol, std::string_view exchange,
                        Side side, int64_t price, uint64_t qty,
                        uint32_t count = 1);
    Result<void> minsert(std::string_view symbol, std::string_view exchange,
                         Side side, const Level* levels, size_t n_levels);
    Result<void> flush();

    // ── Read operations (round-robin) ────────────────────────────────
    Result<QueryResult> query(std::string_view sql);
    Result<bool>        ping();

    /// Close all connections and stop health-check thread.
    void close();

private:
    struct NodeState {
        std::string host;
        uint16_t    port;
        NodeRole    role      = NodeRole::STANDALONE;
        uint64_t    epoch     = 0;
        bool        connected = false;
    };

    PoolConfig                                    config_;
    std::vector<NodeState>                        nodes_;
    std::vector<std::unique_ptr<OrderbookClient>> clients_;
    std::mutex                                    mtx_;
    size_t                                        read_idx_    = 0;   // round-robin
    int                                           primary_idx_ = -1;

    // Health-check thread
    std::thread       health_thread_;
    std::atomic<bool> running_{false};

    // Sharding support
    std::unique_ptr<ShardRouter> shard_router_;

    void connect_all();
    void discover_primary();
    void health_check_loop();

    // Routing helpers
    OrderbookClient* get_primary();
    OrderbookClient* get_any_reader();

    // Retry with re-discovery
    template<typename F>
    auto execute_write(F&& fn) -> decltype(fn(std::declval<OrderbookClient&>()));
    template<typename F>
    auto execute_read(F&& fn) -> decltype(fn(std::declval<OrderbookClient&>()));
};

} // namespace ob
