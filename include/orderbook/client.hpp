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

#include "orderbook/tls.hpp"
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

    /// Credentials for a server running with `--auth-secret-file` (#30).
    ///
    /// Both empty means "do not authenticate", and then a server *with* authentication refuses
    /// every command after the banner. Setting them against a server *without* authentication
    /// fails the connection with `auth_disabled`, which is the honest direction: a client
    /// configured to authenticate has a deployment problem if the server is not.
    std::string auth_identity;
    std::string auth_secret;

    /// TLS on this connection (#30 part three). The server needs `--tls-client`.
    ///
    /// `tls_verify` defaults to on, and the default is the point: a client that encrypts without
    /// checking the certificate has confidentiality against a passive observer and nothing against
    /// a man in the middle - which is the half the shared secret of part two could not give, and
    /// the reason TLS is here at all. Empty `tls_ca_file` means the system trust store.
    ///
    /// The verification includes the *name*: `connect()` requires the certificate to cover the
    /// address it dialled, so a certificate the CA signed for another node is refused. Without
    /// that, a private CA signing the cluster makes every node's certificate good for every other.
    bool        tls        = false;
    std::string tls_ca_file;
    bool        tls_verify = true;
};

/// Copy the fields that decide *how* to connect - credentials and transport - from a pool-shaped
/// config into a per-node one.
///
/// A template rather than three hand-written copies, because three hand-written copies is how the
/// C++ pool and the shard router reached #30 part one unable to authenticate at all: `auth_identity`
/// was on `ClientConfig`, nothing carried it there, and the symptom is `ERR unauthenticated` from a
/// pool whose configuration reads as complete. A static test refuses a new `ClientConfig` field
/// this function does not mention, because the next field is the one that drifts.
template <typename FromConfig>
void copy_client_access(const FromConfig& from, ClientConfig& to) {
    to.auth_identity = from.auth_identity;
    to.auth_secret   = from.auth_secret;
    to.tls           = from.tls;
    to.tls_ca_file   = from.tls_ca_file;
    to.tls_verify    = from.tls_verify;
}

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
    /// Per-origin sequence number of the update that produced this row.
    ///
    /// 0 means unknown, and two different things produce it: a row stored before sequence numbers
    /// were assigned at all, and a server older than this column, which sends six fields. Gaps in
    /// this sequence for one symbol are what let a reader check for itself that it received
    /// everything, instead of trusting the server that it did.
    uint64_t sequence_number{0};
};

/// Result of a SELECT query.
struct QueryResult {
    std::vector<QueryRow> rows;
};

/// One aggregate result, as reported by the server.
///
/// `value` is scaled: the server multiplies VWAP and MID_PRICE by 10^6 and
/// IMBALANCE by 10^9, so divide by `scale` before treating it as a price or a
/// ratio. `empty` marks an aggregate with nothing to aggregate — a spread on a book
/// with no ask side is absent, not zero.
struct AggEntry {
    std::string name;    ///< the expression as sent, e.g. "MID_PRICE(*)"
    int64_t     value{0};
    bool        empty{false};
    int64_t     scale{1};

    /// value in natural units; meaningless when `empty` is true.
    double real() const {
        return static_cast<double>(value) / static_cast<double>(scale);
    }
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

    /// Run an aggregate query (SELECT SPREAD(*), MID_PRICE(*) FROM ...).
    ///
    /// Aggregates use their own response shape, so query() cannot return them and
    /// says so rather than misparsing the columns. Aggregates are computed over the
    /// live book: the server rejects a timestamp or price filter instead of
    /// accepting one and ignoring it.
    Result<std::vector<AggEntry>> query_agg(std::string_view sql);

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
    Result<std::vector<AggEntry>> parse_agg_response(std::string_view resp);
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

    // TLS, both null when config_.tls is false. The context is per client rather than per process:
    // a client that connects once parses its trust anchor once, and a pool of N parses it N times,
    // which is a startup cost nobody measures against a shared-mutable-state problem nobody wants.
    std::unique_ptr<TlsContext>  tls_ctx_;
    std::shared_ptr<ssl_st>      ssl_;

    // Communication. Both route through TLS when ssl_ is set; nothing above this line knows.
    Result<void>             send_all(size_t len);
    Result<std::string_view> recv_response();
    Result<void>             read_banner();
    Result<void>             authenticate();
    Result<void>             negotiate_compression();
    Result<void>             ensure_tls_context();
    Result<void>             start_tls();
    /// Read into `sock_buf_`. >0 bytes, 0 on close, -1 on error, with `why` naming which.
    ssize_t                  read_some(char* buf, size_t len, std::string* why);
    void                     close_transport();
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

    // Credentials and transport, carried to every node's ClientConfig by copy_client_access().
    // One cluster means one client secret and one trust anchor, so these are per-pool and not
    // per-host.
    std::string auth_identity;
    std::string auth_secret;
    bool        tls        = false;
    std::string tls_ca_file;
    bool        tls_verify = true;
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
