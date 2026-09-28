#pragma once

// ── The connection a migration moves a symbol's rows over (#196) ─────────────
//
// A shard moving a symbol to another speaks to that shard's client port as a client does: the rows
// go as the writes that stored them, `MINSERT` with their event time, and the target's half of the
// migration as `ADOPT` lines. The client is `OrderbookClient`, and it lives in its own translation
// unit because its `ob::Level` and `ob::QueryResult` are not the engine's, so no file can include
// both - this header names neither.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace ob {

/// How a shard reaches another's client port to move a symbol there: the identity it authenticates
/// as, from its own `--auth-secret-file` (`--migration-identity`), and TLS, which it uses when its own
/// client port does - one cluster, one configuration. Empty identity: no authentication.
struct MigrationAccess {
    std::string identity;
    std::string secret;
    bool        tls{false};
    std::string tls_ca_file;
};

/// One level of an update a migration sends.
struct MovedLevel {
    int64_t  price{0};
    uint64_t qty{0};
    uint32_t count{0};
};

/// One update of a batch `send_updates()` sends: its levels are the caller's, and must outlive it.
struct MovedWrite {
    uint8_t           side{0};           ///< 0 bid, 1 ask
    uint64_t          timestamp_ns{0};   ///< the time it happened
    const MovedLevel* levels{nullptr};
    size_t            n{0};
};

/// A connection to the shard a symbol moves to.
class TargetConnection {
public:
    /// `address` is the target's "host:port", from the map.
    TargetConnection(std::string address, const MigrationAccess& access);
    ~TargetConnection();

    TargetConnection(const TargetConnection&) = delete;
    TargetConnection& operator=(const TargetConnection&) = delete;

    /// Empty on success, else why not.
    std::string connect();
    bool connected() const;

    /// One command line; `answer` is the whole answer as it came (`OK` and its blank line, or `ERR`
    /// and its newline). Empty on success - an `ERR` answer included - else why the connection
    /// failed.
    std::string command(const std::string& line, std::string& answer);

    /// One update: `MINSERT` of `n` levels of `side` (0 bid, 1 ask) at `timestamp_ns`, the time it
    /// happened. Empty when the target stored it, else its refusal or the connection's failure.
    std::string send_update(const std::string& symbol, const std::string& exchange, uint8_t side,
                            const MovedLevel* levels, size_t n, uint64_t timestamp_ns);

    /// Several updates of one symbol in one write, answered in order - what the copy sends: one
    /// round trip a write was what bounded it. Empty when the target stored every one, else its
    /// first refusal or the connection's failure.
    std::string send_updates(const std::string& symbol, const std::string& exchange,
                             const std::vector<MovedWrite>& writes);

    const std::string& address() const { return address_; }

private:
    struct Impl;
    std::string           address_;
    std::unique_ptr<Impl> impl_;
};

}  // namespace ob
