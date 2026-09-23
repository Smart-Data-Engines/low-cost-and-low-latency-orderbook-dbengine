#include "orderbook/tcp_server.hpp"
#include "orderbook/socket_options.hpp"
#include "orderbook/version.hpp"
#include "orderbook/subscription_hub.hpp"
#include "orderbook/logger.hpp"
#include "orderbook/metrics.hpp"
#include "orderbook/metrics_server.hpp"
#include "orderbook/shard_coordinator.hpp"
#include "orderbook/loop_guard.hpp"
#include "orderbook/thread_boundary.hpp"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cinttypes>
#include <chrono>
#include <fstream>
#include <map>
#include <optional>
#include <set>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <charconv>
#include <limits>
#include <type_traits>
#include <string_view>
#include <span>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#include <pthread.h>

#include <mutex>
#include <thread>

namespace ob {

// ── execute_command ───────────────────────────────────────────────────────────

/// Commands a session may run before it has authenticated.
///
/// AUTH is how it authenticates. PING is here so that a load balancer's health check needs no
/// credentials and reveals nothing about the data. QUIT is here because refusing a client's request
/// to leave would be absurd, and because a client that cannot leave holds a session slot.
LoadedTlsContexts load_tls_or_exit(const ServerConfig& config) {
    LoadedTlsContexts out;
    const bool any_node_link = config.tls_replication || config.tls_multi_master;
    const bool any_surface   = config.tls_client || any_node_link;

    if (!any_surface) {
        if (!config.tls_cert_file.empty() || !config.tls_key_file.empty() ||
            !config.tls_ca_file.empty()   || !config.tls_peer_names.empty()) {
            // A warning here and a *refusal* in the clients, deliberately, because the two have
            // different innocent explanations. On a server this is a staged rollout: the paths go
            // into ob.conf on every node first and the surface flags follow, which is the right way
            // to do it. A client call has no such story - `tls_ca_file` without `tls` is one
            // expression written by someone who believes the connection is encrypted.
            OB_LOG_WARN("tls", "TLS files or peer names were given but no --tls-* surface is "
                               "enabled, so this node listens and dials in plaintext");
        }
        return out;
    }

    if (config.tls_cert_file.empty() || config.tls_key_file.empty()) {
        // Refused rather than falling back: a `--tls-*` flag that quietly meant plaintext is the
        // worst possible outcome of this feature.
        std::fprintf(stderr, "Error: --tls-client, --tls-replication and --tls-multi-master each "
                             "need both --tls-cert-file and --tls-key-file\n");
        std::exit(1);
    }

    if (any_node_link && config.tls_ca_file.empty()) {
        // The one refusal the client port does not have. On a node link both ends are nodes, so
        // "encrypt and do not check who the peer is" leaves the relay in SECURITY.md open - two
        // ends each believing they are talking to the cluster - and reads as protection. There is no
        // fallback to the system trust store either: that would mean every public CA may introduce a
        // replica.
        std::fprintf(stderr,
                     "Error: --tls-replication and --tls-multi-master need --tls-ca-file. A node "
                     "link verifies its peer's certificate in both directions; without a trust "
                     "anchor it would encrypt without authenticating, which leaves the relay "
                     "described in SECURITY.md open.\n");
        std::exit(1);
    }

    if (!config.tls_peer_names.empty() && !any_node_link) {
        OB_LOG_WARN("tls", "--tls-peer-names constrains which peers may connect to a node link, and "
                           "no node-link surface is enabled");
    }

    try {
        if (config.tls_client) {
            out.client_port = std::make_shared<TlsContext>(
                TlsContext::server(config.tls_cert_file, config.tls_key_file));
        }
        if (config.tls_replication) {
            out.replication_server = std::make_shared<TlsContext>(
                TlsContext::node_server(config.tls_cert_file, config.tls_key_file,
                                        config.tls_ca_file, config.tls_peer_names));
            out.replication_client = std::make_shared<TlsContext>(
                TlsContext::node_client(config.tls_cert_file, config.tls_key_file,
                                        config.tls_ca_file, config.tls_peer_names));
            OB_LOG_INFO("tls", "replication link: TLS 1.3, mutual certificate verification");
        } else {
            OB_LOG_INFO("tls", "replication link: plaintext");
        }
        if (config.tls_multi_master) {
            out.mesh_server = std::make_shared<TlsContext>(
                TlsContext::node_server(config.tls_cert_file, config.tls_key_file,
                                        config.tls_ca_file, config.tls_peer_names));
            out.mesh_client = std::make_shared<TlsContext>(
                TlsContext::node_client(config.tls_cert_file, config.tls_key_file,
                                        config.tls_ca_file, config.tls_peer_names));
            OB_LOG_INFO("tls", "multi-master mesh: TLS 1.3, mutual certificate verification");
        } else {
            OB_LOG_INFO("tls", "multi-master mesh: plaintext");
        }
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        std::exit(1);
    }
    return out;
}

LoadedSecrets load_secrets_or_exit(const ServerConfig& config) {
    LoadedSecrets out;
    try {
        if (!config.auth_secret_file.empty()) {
            out.clients = SecretStore::load_client_file(config.auth_secret_file);
        }
        if (!config.cluster_secret_file.empty()) {
            out.cluster = SecretStore::load_cluster_file(config.cluster_secret_file);
        }
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        std::exit(1);
    }

    if (out.client_auth_enabled() && out.cluster_auth_enabled() &&
        stores_share_a_secret(out.clients, out.cluster)) {
        // Refused rather than warned about. Sharing the secret means client authentication grants
        // node privileges: a client presenting itself as a replica streams the entire write-ahead
        // log, and nothing on either surface looks wrong while it happens.
        std::fprintf(stderr,
                     "Error: the cluster secret in '%s' is also a client secret in '%s'. "
                     "A client holding it can present itself as a replica and stream the whole "
                     "write-ahead log. Generate separate secrets.\n",
                     config.cluster_secret_file.c_str(), config.auth_secret_file.c_str());
        std::exit(1);
    }

    if (out.client_auth_enabled()) {
        OB_LOG_INFO("auth", "client authentication enabled (%zu identities from %s)",
                    out.clients.size(), config.auth_secret_file.c_str());
    } else {
        OB_LOG_WARN("auth", "client authentication disabled - trusted-network deployment only "
                            "(--auth-secret-file)");
    }
    if (out.cluster_auth_enabled()) {
        OB_LOG_INFO("auth", "cluster authentication enabled (%s); every node in this cluster must "
                            "run with the same secret - there is no mixed mode",
                    config.cluster_secret_file.c_str());
    } else {
        OB_LOG_WARN("auth", "cluster authentication disabled - replication and multi-master links "
                            "accept any peer (--cluster-secret-file)");
    }
    return out;
}

bool serialised_across_reactors(CommandType t) {
    switch (t) {
    case CommandType::FAILOVER:
    case CommandType::MIGRATE:
        return true;
    case CommandType::SELECT:
    case CommandType::BOOK:
    case CommandType::INSERT:
    case CommandType::MINSERT:
    case CommandType::FLUSH:     // the engine's flush_mtx_ already serialises it
    case CommandType::STATUS:
    case CommandType::ROLE:
    case CommandType::PING:
    case CommandType::QUIT:
    case CommandType::COMPRESS:
    case CommandType::SHARD_MAP:
    case CommandType::SHARD_INFO:
    case CommandType::MM_PEERS:
    case CommandType::MM_CONFLICTS:
    case CommandType::SUBSCRIBE:
    case CommandType::UNSUBSCRIBE:
    case CommandType::AUTH:
    case CommandType::UNKNOWN:
        return false;
    }
    // A value outside the enumeration: serialising is the safe direction here, because running a
    // command alone costs latency and running it concurrently may cost a handover.
    return true;
}

bool allowed_before_authentication(CommandType t) {
    switch (t) {
    case CommandType::AUTH:
    case CommandType::PING:
    case CommandType::QUIT:
    case CommandType::UNKNOWN:   // refused anyway, and by the parser's own message
        return true;
    case CommandType::SELECT:
    case CommandType::BOOK:
    case CommandType::INSERT:
    case CommandType::MINSERT:
    case CommandType::FLUSH:
    case CommandType::STATUS:
    case CommandType::ROLE:
    case CommandType::FAILOVER:
    case CommandType::COMPRESS:
    case CommandType::SHARD_MAP:
    case CommandType::SHARD_INFO:
    case CommandType::MIGRATE:
    case CommandType::MM_PEERS:
    case CommandType::MM_CONFLICTS:
    case CommandType::SUBSCRIBE:
    case CommandType::UNSUBSCRIBE:
        return false;
    }
    // A CommandType outside the enumeration is not a command. Refusing is the safe direction:
    // returning true here would make a corrupted value a way past the gate.
    return false;
}

namespace {

/// The peer's address, for a log line. "unknown" when the socket will not say.
std::string peer_address(int fd) {
    sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    if (::getpeername(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        return "unknown";
    }
    // Checked rather than assumed: getpeername succeeds on an AF_UNIX socket too - which is what
    // the unit tests use - and reading a sockaddr_un through a sockaddr_in would put whatever bytes
    // followed the path into a log line as an address.
    if (addr.sin_family != AF_INET) {
        return "unknown";
    }
    char host[INET_ADDRSTRLEN] = {0};
    if (::inet_ntop(AF_INET, &addr.sin_addr, host, sizeof(host)) == nullptr) {
        return "unknown";
    }
    return std::string(host) + ":" + std::to_string(ntohs(addr.sin_port));
}

/// Handle AUTH. Five outcomes, and the only one that continues the session is the last.
std::string handle_auth(const Command& cmd,
                        Session& session,
                        MetricsRegistry* registry,
                        const SecretStore& secrets) {
    if (session.authenticated()) {
        // Not an error the client can act on by retrying, and answering OK would let a second
        // response overwrite the identity the log already reported.
        return format_error("already_authenticated");
    }

    if (cmd.auth_response.empty()) {
        // Bare AUTH: issue a challenge. A new one replaces any outstanding one, so a response to
        // the previous challenge no longer verifies.
        std::string nonce = generate_nonce_hex();
        session.set_pending_nonce(nonce);
        if (registry) registry->increment_counter("ob_auth_challenges_total");
        OB_LOG_DEBUG("auth", "conn_id=%llu from %s: challenge issued",
                     static_cast<unsigned long long>(session.conn_id()),
                     peer_address(session.fd()).c_str());
        return "OK CHALLENGE " + nonce + "\n\n";
    }

    if (session.pending_nonce().empty()) {
        return format_error("auth_no_challenge");
    }

    const Credential* cred = secrets.find(cmd.auth_identity);
    const std::string expected =
        cred ? auth_response(cred->secret, AuthSurface::Client, AuthRole::Initiator,
                             cred->identity, session.pending_nonce())
             : std::string{};

    // An unknown identity takes the same path as a wrong response, with the same wire message. The
    // distinction would tell an attacker which names exist and tells the operator nothing their log
    // does not already say.
    if (cred == nullptr || !responses_equal(expected, cmd.auth_response)) {
        session.increment_auth_attempts();
        session.set_pending_nonce({});
        if (registry) registry->increment_counter("ob_auth_failures_total");
        OB_LOG_WARN("auth",
                    "conn_id=%llu from %s: authentication failed (attempt %u, claimed identity=%s)",
                    static_cast<unsigned long long>(session.conn_id()),
                    peer_address(session.fd()).c_str(),
                    session.auth_attempts(),
                    sanitise_for_log(cmd.auth_identity).c_str());
        // Closed after the response drains, the same way QUIT is: a socket that shuts without the
        // reason arriving leaves the client with a connection reset and no message.
        session.request_close_after_flush();
        return format_error("auth_failed");
    }

    session.set_authenticated(cred->identity);
    if (registry) registry->increment_counter("ob_auth_success_total");
    OB_LOG_INFO("auth", "conn_id=%llu from %s: authenticated as identity=%s",
                static_cast<unsigned long long>(session.conn_id()),
                peer_address(session.fd()).c_str(),
                cred->identity.c_str());
    return "OK AUTH " + cred->identity + "\n\n";
}

} // namespace

namespace {

/// What a write is stamped with, and whether the sender chose it (#105).
///
/// One function for both commands, because the first question when a row's time looks wrong is
/// "who put it there" and an answer that lives in two places eventually gives two answers. The
/// engine has paid for that shape in `ob_mm_peers_connected` (incremented in three places, none of
/// them `accept()`) and in the four copies of "which shapes are writes".
struct StampedTime {
    uint64_t ns;
    bool     from_client;
};

StampedTime stamp_for(const std::optional<uint64_t>& given) {
    if (given) return {*given, true};
    return {static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                      std::chrono::system_clock::now().time_since_epoch())
                                      .count()),
            false};
}

/// Why this node does not take a write for `symbol.exchange`, as the answer to send - or empty
/// when it does. The checks `INSERT` and `MINSERT` each had a copy of, in the order they ran.
std::string refuse_write(const char* command, const std::string& symbol,
                         const std::string& exchange, Engine& engine, bool read_only,
                         ShardCoordinator* shard_coord) {
    if (read_only || engine.node_role() == NodeRole::REPLICA) return format_error("read-only replica");
    // Covers the multi-master bootstrap too, which used to need its own block here and
    // answered with a second spelling of the same error.
    if (engine.is_bootstrapping()) return format_error("bootstrapping");
    // Shard ownership check: reject writes for symbols not owned by this shard
    if (shard_coord) {
        const std::string symbol_key = symbol + "." + exchange;
        if (engine.is_symbol_migrated(symbol_key)) {
            shard_coord->increment_routing_errors();
            OB_LOG_WARN("tcp_server", "Rejecting %s for migrated symbol=%s", command,
                        symbol_key.c_str());
            return format_error("SYMBOL_MIGRATED");
        }
        if (!shard_coord->owns_symbol(symbol_key)) {
            shard_coord->increment_routing_errors();
            OB_LOG_WARN("tcp_server", "Rejecting %s for non-owned symbol=%s", command,
                        symbol_key.c_str());
            return format_error("NOT_OWNER " + symbol_key);
        }
    }
    return {};
}

/// One thread's working space for `execute_writes()`, reused so that a batch allocates nothing
/// once these have grown to a read's size. Per thread, because every reactor calls it.
struct WriteBatchScratch {
    std::vector<ClientWrite>  writes;
    std::vector<size_t>       answer_of;     // the command each engine write answers
    std::vector<size_t>       level_start;   // where each write's levels begin in `levels`
    std::vector<Level>        levels;
    std::vector<WriteOutcome> outcomes;
};

thread_local WriteBatchScratch write_batch_scratch;

} // namespace

bool deferrable_write(const Command& cmd, const Session& session,
                      const SecretStore* client_secrets) {
    if (cmd.type != CommandType::INSERT && cmd.type != CommandType::MINSERT) return false;
    // The gate `execute_command()` puts in front of its switch. A write it would refuse is left to
    // it, so the refusal is its answer and not a second spelling of it.
    return client_secrets == nullptr || session.authenticated();
}

void execute_writes(std::span<const Command> cmds,
                    Engine& engine,
                    Session& session,
                    ServerStats& stats,
                    bool read_only,
                    MetricsRegistry* registry,
                    ShardCoordinator* shard_coord,
                    std::vector<std::string>& answers) {
    const size_t n = cmds.size();
    answers.resize(n);
    WriteBatchScratch& s = write_batch_scratch;
    s.writes.clear();
    s.answer_of.clear();
    s.level_start.clear();
    s.levels.clear();

    // Everything these need, reserved before anything is built, so that building cannot fail
    // half way through a batch and leave the scratch out of step with itself.
    try {
        size_t total_levels = 0;
        for (const Command& cmd : cmds) {
            total_levels += cmd.type == CommandType::MINSERT ? cmd.minsert_args.n_levels : 1;
        }
        s.writes.reserve(n);
        s.answer_of.reserve(n);
        s.level_start.reserve(n);
        s.levels.reserve(total_levels);
    } catch (const std::exception& e) {
        for (size_t i = 0; i < n; ++i) {
            session.increment_commands();
            answers[i] = format_error(e.what());
        }
        return;
    }

    for (size_t i = 0; i < n; ++i) {
        const Command& cmd = cmds[i];
        session.increment_commands();
        const bool single = cmd.type == CommandType::INSERT;
        const std::string& symbol   = single ? cmd.insert_args.symbol   : cmd.minsert_args.symbol;
        const std::string& exchange = single ? cmd.insert_args.exchange : cmd.minsert_args.exchange;

        std::string refusal = refuse_write(single ? "INSERT" : "MINSERT", symbol, exchange, engine,
                                           read_only, shard_coord);
        if (!refusal.empty()) {
            answers[i] = std::move(refusal);
            continue;
        }

        ClientWrite write{};
        DeltaUpdate& delta = write.update;
        std::strncpy(delta.symbol,   symbol.c_str(),   sizeof(delta.symbol)   - 1);
        std::strncpy(delta.exchange, exchange.c_str(), sizeof(delta.exchange) - 1);
        // 0 means "unassigned": Engine::stamp_sequence() gives this write the next
        // number for its symbol. The comment here used to claim the engine handled
        // sequencing while nothing did, so every stored row carried a zero.
        delta.sequence_number = 0;
        s.level_start.push_back(s.levels.size());
        if (single) {
            const auto& a = cmd.insert_args;
            const StampedTime stamp = stamp_for(a.timestamp_ns);
            delta.timestamp_ns = stamp.ns;
            OB_LOG_DEBUG("tcp_server", "INSERT %s.%s event time %" PRIu64 " (%s)",
                         a.symbol.c_str(), a.exchange.c_str(), stamp.ns,
                         stamp.from_client ? "given by the client" : "stamped on arrival");
            delta.side     = a.side;
            delta.n_levels = 1;

            Level level{};
            level.price = a.price;
            level.qty   = a.qty;
            level.cnt   = a.count;
            s.levels.push_back(level);
        } else {
            const auto& a = cmd.minsert_args;
            const StampedTime stamp = stamp_for(a.timestamp_ns);
            delta.timestamp_ns = stamp.ns;
            OB_LOG_DEBUG("tcp_server", "MINSERT %s.%s levels=%u event time %" PRIu64 " (%s)",
                         a.symbol.c_str(), a.exchange.c_str(), static_cast<unsigned>(a.n_levels),
                         stamp.ns, stamp.from_client ? "given by the client" : "stamped on arrival");
            delta.side     = a.side;
            delta.n_levels = a.n_levels;

            for (uint16_t k = 0; k < a.n_levels; ++k) {
                Level level{};
                level.price = a.levels[k].price;
                level.qty   = a.levels[k].qty;
                level.cnt   = a.levels[k].count;
                level._pad  = 0;
                s.levels.push_back(level);
            }
        }
        s.writes.push_back(write);
        s.answer_of.push_back(i);
    }
    if (s.writes.empty()) return;

    // The levels are pointed at only now: `levels` has stopped growing, so nothing moves them.
    for (size_t k = 0; k < s.writes.size(); ++k) {
        s.writes[k].levels = s.levels.data() + s.level_start[k];
    }

    auto t0 = std::chrono::steady_clock::now();
    try {
        s.outcomes.resize(s.writes.size());
        if (engine.is_multi_master()) {
            engine.apply_deltas_mm(s.writes, s.outcomes);
        } else {
            engine.apply_deltas(s.writes, s.outcomes);
        }
    } catch (const std::exception& e) {
        // Only a programming error gets here - every write has an outcome - and then none of the
        // outcomes can be believed.
        s.outcomes.resize(s.writes.size());
        for (WriteOutcome& o : s.outcomes) o.error = e.what();
    }
    const double secs =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();

    uint64_t taken = 0;
    for (size_t k = 0; k < s.writes.size(); ++k) {
        const WriteOutcome& o = s.outcomes[k];
        std::string& answer = answers[s.answer_of[k]];
        if (!o.error.empty()) {
            answer = format_error(o.error);
        } else if (o.status != OB_OK) {
            answer = format_error("apply_delta failed with code " + std::to_string(o.status));
        } else {
            answer = format_ok();
            ++taken;
        }
    }
    if (taken == 0) return;

    // Once per batch, with the count: what a single write added per write. The latency each write
    // is given is the batch's, because that is how long it waited for its answer - the answers of
    // a read go out together (#146).
    if (registry) {
        registry->observe_histogram("ob_insert_latency_seconds", secs, taken);
        registry->increment_counter("ob_total_inserts", taken);
    }
    session.increment_inserts(taken);
    stats.total_inserts.fetch_add(taken, std::memory_order_relaxed);
}

std::string execute_command(const Command& cmd,
                            Engine& engine,
                            Session& session,
                            ServerStats& stats,
                            bool read_only,
                            MetricsRegistry* registry,
                            ShardCoordinator* shard_coord,
                            SubscriptionHub* hub,
                            const SecretStore* client_secrets) {
    // ── Authentication gate ───────────────────────────────────────────────────
    //
    // Before the switch, not as a branch inside each case. A per-case check means the next command
    // added without one is a command reachable without authentication, and nothing fails - the same
    // shape as a required check nobody requires. AuthGateStatic holds the classification.
    if (client_secrets == nullptr) {
        if (cmd.type == CommandType::AUTH) {
            // Refused rather than answered OK. A client configured to authenticate, talking to a
            // server that does not, must find out: an OK here would be an assurance with nothing
            // behind it, which is the failure mode where everything looks done.
            return format_error("auth_disabled");
        }
    } else {
        if (cmd.type == CommandType::AUTH) {
            return handle_auth(cmd, session, registry, *client_secrets);
        }
        if (!session.authenticated() && !allowed_before_authentication(cmd.type)) {
            return format_error("unauthenticated");
        }
    }

    switch (cmd.type) {

    case CommandType::COMPRESS: {
        if (session.commands_executed() > 0) {
            return format_error("compress_must_be_first");
        }
        // NOTE: Do NOT call session.set_compressed(true) here!
        // The "OK COMPRESS LZ4\n\n" response must be sent as plain text.
        // The caller (the epoll loop) enables compression AFTER
        // sending this response.
        // Double newline required — client uses \n\n as OK terminator.
        return "OK COMPRESS LZ4\n\n";
    }

    case CommandType::SELECT: {
        session.increment_commands();
        // Reject queries during snapshot bootstrap.
        if (engine.is_bootstrapping()) return format_error("bootstrapping");
        auto t0_select = std::chrono::steady_clock::now();
        std::vector<QueryResult> rows;
        QueryShape shape;
        try {
            std::string err = engine.execute(cmd.raw_sql, [&](const QueryResult& r) {
                rows.push_back(r);
            }, shape);
            if (!err.empty()) {
                return format_error(err);
            }
        } catch (const std::exception& e) {
            return format_error(e.what());
        }
        if (registry) {
            double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0_select).count();
            registry->observe_histogram("ob_query_latency_seconds", secs);
            registry->increment_counter("ob_total_queries");
        }
        session.increment_queries();
        stats.total_queries.fetch_add(1, std::memory_order_relaxed);

        // An aggregate query calls the callback exactly once, with agg_values set
        // and the row fields left at zero. Formatting that as a data row is what
        // made SPREAD, MID_PRICE, IMBALANCE and VWAP return zeros to every network
        // client while the engine computed them correctly.
        if (!rows.empty() && !rows.front().agg_values.empty()) {
            return format_agg_response(rows.front().agg_values);
        }
        return format_query_response(rows, shape.columns);
    }

    case CommandType::BOOK: {
        session.increment_commands();
        // The same refusal a `SELECT` gets during bootstrap, and for the same reason: a replica
        // installing a snapshot is replacing the store under itself (#142), and an answer composed
        // across that is an answer about two trees.
        if (engine.is_bootstrapping()) return format_error("bootstrapping");
        auto t0_book = std::chrono::steady_clock::now();
        std::vector<QueryResult> rows;
        try {
            std::string err = engine.read_book(cmd.book_args.symbol, cmd.book_args.exchange,
                                               cmd.book_args.depth,
                                               [&](const QueryResult& r) { rows.push_back(r); });
            if (!err.empty()) return format_error(err);
        } catch (const std::exception& e) {
            return format_error(e.what());
        }
        if (registry) {
            double secs =
                std::chrono::duration<double>(std::chrono::steady_clock::now() - t0_book).count();
            // The same histogram a row query uses. A second one would split the operator's view of
            // "how long does a read take here" across two names for no reason a dashboard cares
            // about, and `ob_total_queries` is the count an operator already alerts on.
            registry->observe_histogram("ob_query_latency_seconds", secs);
            registry->increment_counter("ob_total_queries");
        }
        session.increment_queries();
        stats.total_queries.fetch_add(1, std::memory_order_relaxed);
        // The canonical seven, deliberately: projection is #139's and belongs to `SELECT`, and a
        // second column list to keep in step is a second thing to get wrong before anybody asked
        // for it.
        return format_query_response(rows, all_query_columns());
    }
    case CommandType::INSERT:
    case CommandType::MINSERT: {
        // A batch of one: the reactor holds the writes of a read back and answers them through
        // the same function, so a write's answer does not depend on which path it took (#155).
        std::vector<std::string> answers;
        execute_writes(std::span<const Command>(&cmd, 1), engine, session, stats, read_only,
                       registry, shard_coord, answers);
        return std::move(answers.front());
    }

    case CommandType::FLUSH: {
        session.increment_commands();
        if (read_only || engine.node_role() == NodeRole::REPLICA) return format_error("read-only replica");
        if (engine.is_bootstrapping()) return format_error("bootstrapping");
        auto t0_flush = std::chrono::steady_clock::now();
        try {
            engine.flush_incremental();
        } catch (const std::exception& e) {
            return format_error(e.what());
        }
        if (registry) {
            double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0_flush).count();
            registry->observe_histogram("ob_flush_latency_seconds", secs);
            registry->increment_counter("ob_total_flushes");
        }
        return format_ok();
    }

    case CommandType::PING:
        session.increment_commands();
        return format_pong();

    case CommandType::STATUS: {
        session.increment_commands();
        // A snapshot for this answer, not the `ServerStats` every connection shares. Everything
        // below is a vector, strings and plain integers, which was safe while one thread ran every
        // command and is a data race - on a vector - the moment two reactors answer `STATUS` at
        // once. Only the three counters are shared, and they are atomics.
        ServerStats snap;
        snap.total_queries.store(stats.total_queries.load(std::memory_order_relaxed),
                                 std::memory_order_relaxed);
        snap.total_inserts.store(stats.total_inserts.load(std::memory_order_relaxed),
                                 std::memory_order_relaxed);
        snap.active_sessions.store(stats.active_sessions.load(std::memory_order_relaxed),
                                   std::memory_order_relaxed);
        auto es = engine.stats();
        snap.engine_metrics.pending_rows   = es.pending_rows;
        snap.engine_metrics.wal_file_index = es.wal_file_index;
        snap.engine_metrics.segment_count  = es.segment_count;
        snap.engine_metrics.symbol_count   = es.symbol_count;

        // Copy replication metrics
        snap.replicas.clear();
        for (const auto& r : es.replicas) {
            snap.replicas.push_back({r.address, r.confirmed_file, r.confirmed_offset,
                                      r.lag_bytes, r.lag_known});
        }
        snap.is_replica            = es.is_replica;
        snap.repl_confirmed_file   = es.repl_confirmed_file;
        snap.repl_confirmed_offset = es.repl_confirmed_offset;
        snap.repl_records_replayed = es.repl_records_replayed;
        snap.repl_connected        = es.repl_connected;
        snap.bootstrapping         = es.bootstrapping;
        snap.snapshot_bytes_received = es.snapshot_bytes_received;
        snap.snapshot_bytes_total  = es.snapshot_bytes_total;
        snap.snapshot_active       = es.snapshot_active;

        // Failover state
        snap.node_role           = static_cast<uint8_t>(es.node_role);
        snap.current_epoch       = es.current_epoch;
        snap.primary_address     = es.primary_address;
        snap.lease_ttl_remaining = es.lease_ttl_remaining;

        // Compression metrics from the requesting session
        snap.compress_bytes_in  = session.compress_bytes_in();
        snap.compress_bytes_out = session.compress_bytes_out();

        // TTL / data retention metrics
        snap.ttl_hours            = es.ttl_hours;
        snap.ttl_segments_deleted = es.ttl_segments_deleted;
        snap.ttl_bytes_reclaimed  = es.ttl_bytes_reclaimed;

        // Flush integrity
        snap.segment_merge_refused = es.segment_merge_refused;

        // Sharding metrics
        snap.shard_id              = es.shard_id;
        snap.shard_status          = es.shard_status;
        snap.shard_symbols_count   = es.shard_symbols_count;
        snap.shard_map_version     = es.shard_map_version;
        snap.migration_in_progress = es.migration_in_progress;
        snap.migration_symbol      = es.migration_symbol;
        snap.migration_target_shard = es.migration_target_shard;
        snap.migration_progress_pct = es.migration_progress_pct;
        snap.shard_routing_errors  = es.shard_routing_errors;

        // Multi-master metrics
        snap.mm_node_role = static_cast<uint8_t>(es.node_role);
        if (es.node_role == NodeRole::MULTI_MASTER) {
            snap.mm_node_id           = es.mm_node_id;
            snap.mm_peer_count        = es.mm_peer_count;
            snap.mm_connected_peers   = es.mm_connected_peers;
            snap.mm_conflicts_total   = es.mm_conflicts_total;
            snap.mm_anti_entropy_runs = es.mm_anti_entropy_runs;
            snap.mm_anti_entropy_repairs = es.mm_anti_entropy_repairs;
            snap.mm_hlc_physical_ns   = es.mm_hlc_physical_ns;
            snap.mm_hlc_logical       = es.mm_hlc_logical;
            snap.mm_hlc_drift_ns      = es.mm_hlc_drift_ns;
        }

        return format_status(snap, session.identity());
    }

    case CommandType::ROLE:
        session.increment_commands();
        return engine.handle_role_command();

    case CommandType::FAILOVER:
        session.increment_commands();
        return engine.handle_failover_command(cmd.target_node_id);

    case CommandType::SHARD_MAP: {
        session.increment_commands();
        OB_LOG_DEBUG("tcp_server", "Handling SHARD_MAP command");
        if (!shard_coord) return format_error("sharding not enabled");
        return shard_coord->handle_shard_map_command();
    }

    case CommandType::SHARD_INFO: {
        session.increment_commands();
        OB_LOG_DEBUG("tcp_server", "Handling SHARD_INFO command");
        if (!shard_coord) return format_error("sharding not enabled");
        return shard_coord->handle_shard_info_command();
    }

    case CommandType::MIGRATE: {
        session.increment_commands();
        OB_LOG_INFO("tcp_server", "Handling MIGRATE command: symbol=%s target=%s",
                    cmd.migrate_symbol.c_str(), cmd.migrate_target_shard.c_str());
        if (!shard_coord) return format_error("sharding not enabled");
        if (read_only) return format_error("read-only mode");
        return shard_coord->handle_migrate_command(
            cmd.migrate_symbol, cmd.migrate_target_shard);
    }

    case CommandType::MM_PEERS: {
        session.increment_commands();
        OB_LOG_DEBUG("tcp_server", "Handling MM_PEERS command");
        if (!engine.is_multi_master()) return format_error("not in multi-master mode");
        return engine.multi_master_manager()->handle_mm_peers_command();
    }

    case CommandType::MM_CONFLICTS: {
        session.increment_commands();
        OB_LOG_DEBUG("tcp_server", "Handling MM_CONFLICTS command limit=%zu", cmd.mm_conflicts_limit);
        if (!engine.is_multi_master()) return format_error("not in multi-master mode");
        return engine.multi_master_manager()->handle_mm_conflicts_command(cmd.mm_conflicts_limit);
    }

    case CommandType::SUBSCRIBE: {
        session.increment_commands();
        if (hub == nullptr) {
            // A caller without a hub - the unit tests, and until #147 the io_uring transport - gets
            // a refusal. Saying so is better than accepting the command and never pushing
            // anything: a client that gets OK and then silence has no way to tell that from a
            // market with no updates.
            return format_error("subscriptions are not available on this transport");
        }
        std::string error;
        const uint64_t id =
            hub->add(engine, session.fd(), session.conn_id(), cmd.subscribe_sql, &error);
        if (id == 0) return format_error(error.empty() ? "subscribe_refused" : error);
        // The id is in the acknowledgement because a client with two subscriptions has nothing to
        // cancel one with otherwise.
        return "OK SUB " + std::to_string(id) + "\n\n";
    }

    case CommandType::UNSUBSCRIBE: {
        session.increment_commands();
        if (hub == nullptr) return format_error("subscriptions are not available on this transport");
        const int removed = (cmd.unsubscribe_id != 0)
                                ? hub->remove(engine, cmd.unsubscribe_id)
                                : hub->remove_connection(engine, session.fd(), session.conn_id());
        // A count rather than a bare OK, and zero is not an error: cancelling something already
        // gone is the client and the server agreeing.
        return "OK " + std::to_string(removed) + "\n\n";
    }

    case CommandType::QUIT:
        session.increment_commands();
        return ""; // empty string signals session close

    case CommandType::AUTH:
        // Unreachable: the gate above handles AUTH on both paths and returns. Present so that the
        // switch stays exhaustive, which is what makes -Wswitch tell us about the *next* command.
        return format_error("auth_disabled");

    case CommandType::UNKNOWN:
    default: {
        session.increment_commands();
        // The parser's own words when it has any: a recognised command with a token the grammar has
        // no place for is not an unknown command, and saying so sends the reader looking for a
        // feature that is not missing (#107). The gate above already says the parser owns this
        // message - it just had none until now.
        const std::string why = cmd.error.empty() ? "unknown command" : cmd.error;
        if (registry) registry->increment_counter("ob_refused_commands_total");
        // Logged here rather than only in the parser, because *who sent it* is the operator's first
        // question and the parser is pure - it has no fd to name. Once per connection, not once per
        // line: a refusal is reachable before authentication, so a WARN per refused line is a flood
        // any peer who can reach the port can drive at line rate (#95's shape). The counter above
        // is what carries the volume, and it is alertable; this line carries the diagnosis.
        if (session.first_refusal()) {
            OB_LOG_WARN("tcp_server", "Refused a command from fd=%d: %s (further refusals on this "
                                      "connection are counted in ob_refused_commands_total)",
                        session.fd(), why.c_str());
        } else {
            OB_LOG_DEBUG("tcp_server", "Refused a command from fd=%d: %s", session.fd(),
                         why.c_str());
        }
        return format_error(why);
    }
    }
}

// ── ArgCursor ─────────────────────────────────────────────────────────────────
//
// A cursor over argv, so consuming a flag's value does not mean modifying the loop variable.
// The old parser did `argv[++i]` inside `for (int i = 1; i < argc; ++i)` 29 times, which is 29
// instances of cpp/loop-variable-changed and a review thread on every PR that added a flag
// (roadmap #36). It also hid three real defects, all measured before this rewrite:
//
//   ob_tcp_server --port abc      → terminate called after throwing std::invalid_argument, core
//                                   dumped: stoi threw and nothing caught it
//   ob_tcp_server --port          → started anyway, on the default port. The guard was
//                                   `arg == "--port" && i + 1 < argc`, so a flag with no value
//                                   simply fell through
//   ob_tcp_server --prot 5599     → started anyway, on the default port, ignoring both the typo
//                                   and its value, because there was no unknown-argument branch
//
// All three are now errors with a message naming the flag. That is stricter than before: an
// invocation carrying an unknown flag used to start a server and now refuses to. A correct
// invocation behaves exactly as it did.

namespace {

class ArgCursor {
public:
    /// A span rather than (int, char*[]): the cursor then carries its own bounds, and the bounds
    /// are what every check below is against. main()'s signature is fixed, so the conversion
    /// happens once, at the call site.
    explicit ArgCursor(std::span<char* const> args) : args_(args) {}

    /// Advance to the next argument. False when there are none left.
    bool next() { return ++index_ < args_.size(); }

    /// The argument the cursor is on.
    std::string_view arg() const { return args_[index_]; }

    /// The value belonging to the current flag. Missing values are fatal, not ignored.
    std::string_view value() {
        if (index_ + 1 >= args_.size()) {
            std::fprintf(stderr, "Error: %s requires a value\n", args_[index_]);
            std::exit(1);
        }
        return args_[++index_];
    }

    /// The value parsed as a number, with the flag named in the error rather than a stoi throw.
    template <typename T>
    T value_as() {
        const char* flag = args_[index_];
        const std::string_view text = value();

        if constexpr (std::is_signed_v<T>) {
            long long parsed = 0;
            const auto result = std::from_chars(text.data(), text.data() + text.size(), parsed);
            if (result.ec != std::errc{} || result.ptr != text.data() + text.size()) {
                fail(flag, text, "an integer");
            }
            if (parsed < static_cast<long long>(std::numeric_limits<T>::min()) ||
                parsed > static_cast<long long>(std::numeric_limits<T>::max())) {
                fail(flag, text, "a value in range");
            }
            return static_cast<T>(parsed);
        } else {
            unsigned long long parsed = 0;
            const auto result = std::from_chars(text.data(), text.data() + text.size(), parsed);
            if (result.ec != std::errc{} || result.ptr != text.data() + text.size()) {
                fail(flag, text, "a non-negative integer");
            }
            if (parsed > static_cast<unsigned long long>(std::numeric_limits<T>::max())) {
                fail(flag, text, "a value in range");
            }
            return static_cast<T>(parsed);
        }
    }

private:
    [[noreturn]] static void fail(const char* flag, std::string_view text, const char* expected) {
        std::fprintf(stderr, "Error: %s expects %s, got '%.*s'\n", flag, expected,
                     static_cast<int>(text.size()), text.data());
        std::exit(1);
    }

    std::span<char* const> args_;
    std::size_t            index_{0};
};

}  // namespace

// ── parse_cli_args ────────────────────────────────────────────────────────────

// ── Configuration file ────────────────────────────────────────────────────────
//
// The file is rewritten into arguments and handed to the flag parser. Not parsed into a
// ServerConfig, and that is the design rather than a shortcut:
//
//   * a config key *is* a flag name, by construction rather than through a table somebody has to
//     remember to update — and the symptom of such a table falling behind is a key an operator
//     wrote that silently does nothing;
//   * type validation and its error message stay in one place, so a bad value reads the same
//     whether it came from a file or a flag;
//   * precedence falls out of argument order, because the parser assigns rather than accumulates.
//     There is no merge step, so there is no merge step to get wrong.

const std::vector<std::string>& known_flags() {
    // Sorted, and checked against the parser's own source by CliConfigStatic.KnownFlagsMatchTheParser.
    static const std::vector<std::string> flags = {
        "anti-entropy-interval-seconds",
        "config",
        "coordinator-endpoints",
        "coordinator-lease-ttl",
        "auth-secret-file",
        "cluster-secret-file",
        "data-dir",
        "drain-timeout-ms",
        "election-deference-ms",
        "election-lease-wait-ms",
        "failover-enabled",
        "flush-interval-ms",
        "fsync-policy",
        "profile",
        "handover-cooldown-seconds",
        "io-spin-us",
        "io-threads",
        "handover-grace-seconds",
        "log-level",
        "max-sessions",
        "max-subscriber-queue-bytes",
        "max-subscriptions-per-session",
        "metrics-bind",
        "metrics-port",
        "mm-max-catchup-bytes",
        "mm-max-peer-send-buffer",
        "mm-node-id",
        "mm-replication-port",
        "multi-master",
        "node-id",
        "port",
        "primary-host",
        "primary-port",
        "print-config",
        "read-only",
        "replication-compress",
        "replication-port",
        "shard-id",
        "shard-vnodes",
        "snapshot-chunk-size",
        "snapshot-staging-dir",
        "tls-ca-file",
        "tls-cert-file",
        "tls-client",
        "tls-key-file",
        "tls-multi-master",
        "tls-peer-names",
        "tls-replication",
        "ttl-hours",
        "ttl-scan-interval-seconds",
        "wal-rotate-bytes",
    };
    return flags;
}

namespace {

// One description per accepted flag, keyed by the same name the parser accepts.
//
// `--help` used to be a hardcoded string in `tools/ob_tcp_server.cpp` listing **six** of the forty
// flags this parser takes. The three most consequential omissions say why that matters: `--config`
// and `--print-config` exist precisely so an operator can manage forty flags, and were themselves
// undiscoverable from the one command everyone runs first; and `--fsync-policy` is the durability
// setting in a database, which #33 had already found missing once.
//
// So the help text is generated from `known_flags()` rather than written beside it. A flag added to
// the parser without a line here prints as `(undocumented)` at runtime - visible rather than absent -
// and fails `CliConfigStatic.EveryKnownFlagIsDocumented`. The names stay in one place, which is the
// same reason #32 fed the config file through the existing parser instead of building a second
// dictionary of them.
const std::map<std::string, std::pair<std::string, std::string>>& flag_help() {
    // flag -> (argument placeholder, description). An empty placeholder means a boolean flag.
    static const std::map<std::string, std::pair<std::string, std::string>> help = {
        {"anti-entropy-interval-seconds", {"<N>", "Multi-master reconciliation interval (default: 60)"}},
        {"config", {"<FILE>", "Read `key = value` settings from FILE; command line wins"}},
        {"coordinator-endpoints", {"<URLS>", "Comma-separated etcd endpoints for HA and failover"}},
        {"coordinator-lease-ttl", {"<N>", "Leader lease TTL in seconds (default: 10)"}},
        {"data-dir", {"<DIR>", "Data directory for the engine (default: /tmp/ob_data)"}},
        {"election-deference-ms", {"<N>", "Wait for a replica further ahead in the log; 0 disables"}},
        {"election-lease-wait-ms", {"<N>", "Wait after the leader key vanishes before standing"}},
        {"failover-enabled", {"<BOOL>", "Participate in automatic failover: true/1/yes or false/0/no (default: true)"}},
        {"auth-secret-file", {"<PATH>", "Client credentials, '<identity> <secret>' per line; mode 600. Empty disables client authentication"}},
        {"cluster-secret-file", {"<PATH>", "Shared secret for replication and multi-master links, one line; mode 600"}},
        {"drain-timeout-ms", {"<N>", "On shutdown, how long to wait for open client sessions before closing them (default: 10000; 0 waits indefinitely)"}},
        {"io-spin-us", {"<N>", "Keep polling for this many microseconds after the last event before blocking again (default: 0, always block). Costs up to one core while traffic flows and takes ~20% off the round trip on loopback"}},
        {"io-threads", {"<N>", "Client event loops, 1 to 64 (default: 1). Connections are dealt to them in turn and stay on one for life"}},
        {"profile", {"<name>", "eco (default, blocking io) or boost (sets io-spin-us). A named set of the knobs, not a second code path"}},
        {"flush-interval-ms", {"<N>", "Background flush interval in ms (default: 100)"}},
        {"fsync-policy", {"<POLICY>", "WAL durability: every, interval or none (lower case; default: interval)"}},
        {"handover-cooldown-seconds", {"<N>", "How long a node that handed the role over abstains"}},
        {"handover-grace-seconds", {"<N>", "Grace period granted to a handover target"}},
        {"log-level", {"<LEVEL>", "ERROR, WARN, INFO or DEBUG (upper case; default: INFO)"}},
        {"max-sessions", {"<N>", "Maximum concurrent client sessions (default: 64)"}},
        {"max-subscriber-queue-bytes", {"<N>", "Per-subscriber queue ceiling; past it the session closes"}},
        {"max-subscriptions-per-session", {"<N>", "Subscription limit per session (default: 16)"}},
        {"metrics-bind", {"<ADDR>", "Address the metrics listener binds to (default: every interface)"}},
        {"metrics-port", {"<PORT>", "Prometheus metrics port; 0 disables the endpoint"}},
        {"mm-max-catchup-bytes", {"<N>", "WAL bytes a peer may scan before a snapshot is used"}},
        {"mm-max-peer-send-buffer", {"<N>", "Per-peer send buffer ceiling; past it the peer is dropped"}},
        {"mm-node-id", {"<N>", "Multi-master node id, unique in the mesh"}},
        {"mm-replication-port", {"<PORT>", "Multi-master peer port"}},
        {"multi-master", {"", "Run as a multi-master node instead of primary/replica"}},
        {"node-id", {"<ID>", "This node's name, as it appears to the coordinator"}},
        {"port", {"<PORT>", "TCP port to listen on (default: 9090)"}},
        {"primary-host", {"<HOST>", "Primary to replicate from, when starting as a replica"}},
        {"primary-port", {"<PORT>", "Primary's replication port"}},
        {"print-config", {"", "Print every setting with its origin and exit; opens no port"}},
        {"read-only", {"", "Refuse writes regardless of role"}},
        {"replication-compress", {"", "Compress the replication stream with LZ4"}},
        {"replication-port", {"<PORT>", "Port replicas connect to on this node"}},
        {"shard-id", {"<N>", "This node's shard, when sharding by symbol"}},
        {"shard-vnodes", {"<N>", "Virtual nodes per shard in the consistent hash ring"}},
        {"snapshot-chunk-size", {"<N>", "Bytes per snapshot transfer chunk"}},
        {"snapshot-staging-dir", {"<DIR>", "Where an incoming snapshot is staged before install"}},
        {"tls-ca-file", {"<PATH>", "Trust anchor (PEM) for verifying peer certificates on node "
                                   "links; required by --tls-replication and --tls-multi-master"}},
        {"tls-cert-file", {"<PATH>", "This node's certificate chain (PEM), used on every TLS "
                                     "surface and in both roles"}},
        {"tls-client", {"", "TLS on the client port; needs --tls-cert-file and --tls-key-file"}},
        {"tls-key-file", {"<PATH>", "This node's private key (PEM); mode 600"}},
        {"tls-multi-master", {"", "TLS with mutual certificate verification on the multi-master "
                                  "mesh; needs --tls-ca-file"}},
        {"tls-peer-names", {"<NAMES>", "Comma-separated identities an accepted peer's certificate "
                                       "may carry; empty accepts any name this CA signed"}},
        {"tls-replication", {"", "TLS with mutual certificate verification on the replication "
                                 "link, in both roles; needs --tls-ca-file"}},
        {"ttl-hours", {"<N>", "Retention in hours; 0 keeps everything"}},
        {"ttl-scan-interval-seconds", {"<N>", "How often retention scans for expired rows"}},
        {"wal-rotate-bytes", {"<N>", "WAL bytes before the next file is opened; a file may "
                                     "exceed it by one record (default: 536870912)"}},
    };
    return help;
}

} // namespace

std::string format_usage(const std::string& program) {
    std::string out = "Usage: " + program + " [OPTIONS]\n\nOptions:\n";

    size_t width = 0;
    for (const auto& flag : known_flags()) {
        const auto it = flag_help().find(flag);
        const std::string placeholder = (it != flag_help().end()) ? it->second.first : "<VALUE>";
        const size_t length = flag.size() + (placeholder.empty() ? 0 : placeholder.size() + 1);
        width = std::max(width, length);
    }
    width = std::max(width, std::string("help").size());

    for (const auto& flag : known_flags()) {
        const auto it = flag_help().find(flag);
        const std::string placeholder = (it != flag_help().end()) ? it->second.first : "<VALUE>";
        const std::string description =
            (it != flag_help().end()) ? it->second.second : "(undocumented)";

        std::string left = "--" + flag;
        if (!placeholder.empty()) left += " " + placeholder;
        out += "  " + left + std::string(width + 2 - (left.size() - 2), ' ') + description + "\n";
    }
    out += "  --help" + std::string(width - 2, ' ') + "Show this help message and exit\n";
    return out;
}

const std::vector<std::string>& boolean_flags() {
    static const std::vector<std::string> flags = {
        "multi-master",
        "print-config",
        "read-only",
        "replication-compress",
        "tls-client",
        "tls-multi-master",
        "tls-replication",
    };
    return flags;
}

namespace {

[[noreturn]] void config_error(const std::string& path, size_t line, const std::string& message) {
    std::fprintf(stderr, "Error: %s:%zu: %s\n", path.c_str(), line, message.c_str());
    std::exit(1);
}

/// The three closest known keys to `key`, by a cheap edit distance. A refusal that only says
/// "unknown" leaves an operator comparing their file against a manual character by character.
std::string suggestions_for(const std::string& key) {
    auto distance = [](const std::string& a, const std::string& b) {
        std::vector<size_t> previous(b.size() + 1), current(b.size() + 1);
        for (size_t j = 0; j <= b.size(); ++j) previous[j] = j;
        for (size_t i = 1; i <= a.size(); ++i) {
            current[0] = i;
            for (size_t j = 1; j <= b.size(); ++j) {
                const size_t cost = (a[i - 1] == b[j - 1]) ? 0u : 1u;
                current[j] = std::min({previous[j] + 1, current[j - 1] + 1, previous[j - 1] + cost});
            }
            previous = current;
        }
        return previous[b.size()];
    };

    std::vector<std::pair<size_t, std::string>> scored;
    for (const auto& candidate : known_flags()) {
        scored.emplace_back(distance(key, candidate), candidate);
    }
    std::sort(scored.begin(), scored.end());
    std::string out;
    for (size_t i = 0; i < scored.size() && i < 3; ++i) {
        if (i > 0) out += ", ";
        out += scored[i].second;
    }
    return out;
}

std::string trimmed(std::string_view text) {
    size_t begin = 0;
    size_t end = text.size();
    while (begin < end && std::isspace(static_cast<unsigned char>(text[begin]))) ++begin;
    while (end > begin && std::isspace(static_cast<unsigned char>(text[end - 1]))) --end;
    return std::string{text.substr(begin, end - begin)};
}

}  // namespace

std::vector<std::string> config_file_to_args(const std::string& path,
                                             std::set<std::string>* keys_seen) {
    std::ifstream file(path);
    if (!file.is_open()) {
        // Refused rather than ignored: a node started with a configuration nobody knows about is
        // worse than a node that did not start.
        std::fprintf(stderr, "Error: cannot open config file '%s': %s\n", path.c_str(),
                     std::strerror(errno));
        std::exit(1);
    }

    std::vector<std::string> args;
    std::set<std::string> seen;
    std::string line;
    size_t number = 0;

    while (std::getline(file, line)) {
        ++number;
        // A comment runs to end of line, including after a value.
        const size_t hash = line.find('#');
        if (hash != std::string::npos) line.erase(hash);
        const std::string content = trimmed(line);
        if (content.empty()) continue;

        const size_t equals = content.find('=');
        if (equals == std::string::npos) {
            config_error(path, number,
                         "expected 'key = value', got '" + content + "'");
        }
        const std::string key = trimmed(std::string_view{content}.substr(0, equals));
        const std::string value = trimmed(std::string_view{content}.substr(equals + 1));

        if (key.empty()) config_error(path, number, "empty key");
        if (std::find(known_flags().begin(), known_flags().end(), key) == known_flags().end()) {
            config_error(path, number,
                         "unknown key '" + key + "'. Closest known keys: " + suggestions_for(key));
        }
        if (key == "config") {
            // A config file that names another one is a chain nobody can debug, and a config file
            // that names itself is a loop. Refused outright rather than depth-limited.
            config_error(path, number, "'config' cannot be set from inside a config file");
        }
        if (!seen.insert(key).second) {
            // Last-wins would be a silent choice between two things the operator wrote.
            config_error(path, number, "'" + key + "' is set more than once");
        }

        const bool is_boolean =
            std::find(boolean_flags().begin(), boolean_flags().end(), key) != boolean_flags().end();
        if (is_boolean) {
            if (value == "true") {
                args.push_back("--" + key);
            } else if (value != "false") {
                config_error(path, number,
                             "'" + key + "' takes true or false, got '" + value + "'");
            }
            // `false` contributes nothing, and that is only sound because every valueless boolean
            // defaults to false — asserted by CliConfigStatic.EveryValuelessBooleanDefaultsToFalse.
            // A valueless flag whose default were true could not be turned off this way.
            continue;
        }

        if (value.empty()) {
            config_error(path, number, "'" + key + "' has no value");
        }
        args.push_back("--" + key);
        args.push_back(value);
    }

    if (keys_seen) *keys_seen = seen;
    OB_LOG_INFO("cli", "Read %zu setting(s) from %s", seen.size(), path.c_str());
    return args;
}

ResolvedConfig resolve_cli_args(int argc, char* argv[]) {
    ServerConfig config;
    std::map<std::string, Origin> origin;
    bool print_config_requested = false;

    // ── Pre-scan for --config, then merge ────────────────────────────────────────────────────────
    //
    // File arguments first, real ones second. Precedence needs no merge logic: the loop below
    // assigns, so the last occurrence of a flag wins, and the real command line is last.
    std::string config_path;
    for (int i = 1; i + 1 < argc; ++i) {
        if (std::string_view{argv[i]} == "--config") {
            config_path = argv[i + 1];
            break;
        }
    }

    std::vector<std::string> storage;   // owns the strings the merged argv points into
    std::vector<char*>       merged;
    std::set<std::string>    from_file;
    std::set<std::string>    from_command_line;

    if (!config_path.empty()) {
        // The keys the file set, not the flags it emitted. For a valueless boolean, `= false` emits
        // nothing at all, so deriving provenance from the emitted arguments would report a key the
        // operator wrote as coming from the default - the one thing `--print-config` exists not to
        // do.
        storage = config_file_to_args(config_path, &from_file);
    }
    for (int i = 1; i < argc; ++i) {
        std::string_view item{argv[i]};
        if (item.rfind("--", 0) != 0) continue;
        from_command_line.insert(std::string{item.substr(2)});
    }

    merged.reserve(storage.size() + static_cast<size_t>(argc) + 1);
    merged.push_back(argv[0]);
    for (std::string& item : storage) merged.push_back(item.data());
    for (int i = 1; i < argc; ++i) merged.push_back(argv[i]);

    for (const std::string& key : from_file) origin[key] = Origin::File;
    for (const std::string& key : from_command_line) origin[key] = Origin::CommandLine;

    ArgCursor cursor(std::span<char* const>(merged.data(), merged.size()));
    while (cursor.next()) {
        const std::string arg{cursor.arg()};

        if (arg == "--port") {
            config.port = cursor.value_as<uint16_t>();
        } else if (arg == "--data-dir") {
            config.data_dir = std::string{cursor.value()};
        } else if (arg == "--max-sessions") {
            config.max_sessions = cursor.value_as<int>();
        } else if (arg == "--max-subscriber-queue-bytes") {
            config.max_subscriber_queue_bytes = cursor.value_as<size_t>();
        } else if (arg == "--max-subscriptions-per-session") {
            config.max_subscriptions_per_session = cursor.value_as<int>();
        } else if (arg == "--fsync-policy") {
            const std::string val{cursor.value()};
            if (val == "every") {
                config.fsync_policy = FsyncPolicy::EVERY;
            } else if (val == "interval") {
                config.fsync_policy = FsyncPolicy::INTERVAL;
            } else if (val == "none") {
                config.fsync_policy = FsyncPolicy::NONE;
            } else {
                // Refused rather than defaulted. Reading an unrecognised durability policy as
                // "interval" would mean an operator who asked for `every` and got something weaker
                // finding out from a lost write.
                std::fprintf(stderr,
                    "Error: --fsync-policy expects every, interval or none, got '%s'\n",
                    val.c_str());
                std::exit(1);
            }
        } else if (arg == "--config") {
            // Consumed in the pre-scan above; this branch exists so the flag is not an unknown one.
            (void)cursor.value();
        } else if (arg == "--print-config") {
            print_config_requested = true;
        } else if (arg == "--read-only") {
            config.read_only = true;
        } else if (arg == "--replication-port") {
            config.replication_port = cursor.value_as<uint16_t>();
        } else if (arg == "--replication-compress") {
            config.replication_compress = true;
        } else if (arg == "--primary-host") {
            config.primary_host = std::string{cursor.value()};
        } else if (arg == "--primary-port") {
            config.primary_port = cursor.value_as<uint16_t>();
        } else if (arg == "--snapshot-chunk-size") {
            config.snapshot_chunk_size = cursor.value_as<size_t>();
        } else if (arg == "--snapshot-staging-dir") {
            config.snapshot_staging_dir = std::string{cursor.value()};
        } else if (arg == "--coordinator-endpoints") {
            // Comma-separated list; empty entries are skipped rather than stored.
            const std::string endpoints{cursor.value()};
            std::string ep;
            for (char c : endpoints) {
                if (c == ',') {
                    if (!ep.empty()) config.coordinator_endpoints.push_back(ep);
                    ep.clear();
                } else {
                    ep.push_back(c);
                }
            }
            if (!ep.empty()) config.coordinator_endpoints.push_back(ep);
        } else if (arg == "--coordinator-lease-ttl") {
            config.coordinator_lease_ttl = cursor.value_as<int64_t>();
        } else if (arg == "--handover-grace-seconds") {
            config.handover_grace_seconds = cursor.value_as<int64_t>();
        } else if (arg == "--handover-cooldown-seconds") {
            config.handover_cooldown_seconds = cursor.value_as<int64_t>();
        } else if (arg == "--election-deference-ms") {
            config.election_deference_ms = cursor.value_as<int64_t>();
        } else if (arg == "--election-lease-wait-ms") {
            config.election_lease_wait_ms = cursor.value_as<int64_t>();
        } else if (arg == "--node-id") {
            config.node_id = std::string{cursor.value()};
        } else if (arg == "--failover-enabled") {
            // Takes a value, so `--failover-enabled false` has always worked - which is worth a note
            // because a config-file change was almost built on the belief that it did not, from
            // reading the default rather than this branch.
            //
            // What it did do was map anything unrecognised to *false*: `--failover-enabled tru`
            // silently disabled failover. Same class as #36, where a mistyped flag started the
            // server. The accepted spellings are unchanged so no existing invocation breaks; what is
            // new is that a value outside them is refused instead of read as "no".
            const std::string val{cursor.value()};
            if (val == "true" || val == "1" || val == "yes") {
                config.failover_enabled = true;
            } else if (val == "false" || val == "0" || val == "no") {
                config.failover_enabled = false;
            } else {
                std::fprintf(stderr,
                    "Error: --failover-enabled expects true or false, got '%s'\n", val.c_str());
                std::exit(1);
            }
        } else if (arg == "--ttl-hours") {
            config.ttl_hours = cursor.value_as<uint64_t>();
        } else if (arg == "--ttl-scan-interval-seconds") {
            config.ttl_scan_interval_seconds = cursor.value_as<uint64_t>();
        } else if (arg == "--metrics-port") {
            config.metrics_port = cursor.value_as<uint16_t>();
        } else if (arg == "--metrics-bind") {
            config.metrics_bind = std::string{cursor.value()};
        } else if (arg == "--tls-cert-file") {
            config.tls_cert_file = std::string{cursor.value()};
        } else if (arg == "--tls-key-file") {
            config.tls_key_file = std::string{cursor.value()};
        } else if (arg == "--tls-ca-file") {
            config.tls_ca_file = std::string{cursor.value()};
        } else if (arg == "--tls-client") {
            config.tls_client = true;
        } else if (arg == "--tls-replication") {
            config.tls_replication = true;
        } else if (arg == "--tls-multi-master") {
            config.tls_multi_master = true;
        } else if (arg == "--tls-peer-names") {
            // Comma-separated, the same idiom as --coordinator-endpoints; empty entries skipped.
            const std::string names{cursor.value()};
            std::string one;
            for (char c : names) {
                if (c == ',') {
                    if (!one.empty()) config.tls_peer_names.push_back(one);
                    one.clear();
                } else {
                    one.push_back(c);
                }
            }
            if (!one.empty()) config.tls_peer_names.push_back(one);
        } else if (arg == "--auth-secret-file") {
            config.auth_secret_file = std::string{cursor.value()};
        } else if (arg == "--cluster-secret-file") {
            config.cluster_secret_file = std::string{cursor.value()};
        } else if (arg == "--drain-timeout-ms") {
            config.drain_timeout_ms = cursor.value_as<uint64_t>();
        } else if (arg == "--io-spin-us") {
            config.io_spin_us = cursor.value_as<uint64_t>();
        } else if (arg == "--io-threads") {
            config.io_threads = cursor.value_as<uint32_t>();
        } else if (arg == "--profile") {
            config.profile = std::string{cursor.value()};
        } else if (arg == "--flush-interval-ms") {
            config.flush_interval_ms = cursor.value_as<uint64_t>();
        } else if (arg == "--log-level") {
            const std::string level{cursor.value()};
            if (!StructuredLogger::parse_level(level).has_value()) {
                std::fprintf(stderr,
                    "Error: invalid log level '%s'. Valid values: ERROR, WARN, INFO, DEBUG\n",
                    level.c_str());
                std::exit(1);
            }
            config.log_level = level;
        } else if (arg == "--shard-id") {
            config.shard_id = std::string{cursor.value()};
        } else if (arg == "--shard-vnodes") {
            config.shard_vnodes = cursor.value_as<uint32_t>();
        } else if (arg == "--multi-master") {
            config.multi_master = true;
        } else if (arg == "--mm-node-id") {
            config.mm_node_id = cursor.value_as<uint16_t>();
        } else if (arg == "--mm-replication-port") {
            config.mm_replication_port = cursor.value_as<uint16_t>();
        } else if (arg == "--anti-entropy-interval-seconds") {
            config.anti_entropy_interval_sec = cursor.value_as<uint32_t>();
        } else if (arg == "--mm-max-peer-send-buffer") {
            config.mm_max_peer_send_buf_bytes = cursor.value_as<size_t>();
        } else if (arg == "--mm-max-catchup-bytes") {
            config.mm_max_catchup_bytes = cursor.value_as<size_t>();
        } else if (arg == "--wal-rotate-bytes") {
            config.wal_rotate_bytes = cursor.value_as<size_t>();
        } else {
            // Previously ignored in silence, which meant a typo started a server on the default
            // port: `--prot 5599` was accepted, and so was `--port` with no value at all.
            std::fprintf(stderr, "Error: unknown argument '%s'\n", arg.c_str());
            std::exit(1);
        }
    }

    // Validation: the number of client event loops. Zero would be a server that accepts
    // connections and serves none of them, and a count far above the cores is a typo more often
    // than a plan: each reactor is a thread with an epoll set of its own, and more of them than
    // cores only adds switching.
    if (config.io_threads == 0 || config.io_threads > kMaxIoThreads) {
        std::fprintf(stderr,
                     "Error: --io-threads (%u) must be between 1 and %u. It is the number of client "
                     "event loops; 1 is the single loop this server always had.\n",
                     config.io_threads, kMaxIoThreads);
        std::exit(1);
    }

    // Validation: the WAL rotation threshold, refused at both ends rather than clamped.
    //
    // Both bounds are the constants the WAL itself declares, so there is one definition of each and
    // the message can quote the arithmetic. The ceiling exists because the offset half of
    // `WalPosition` is 32 bits so that the file index and the offset can be read as one coherent
    // value (#85); `WALWriter` throws for it too, and this refusal is the one an operator sees,
    // naming the flag they typed rather than a constructor argument.
    //
    // The floor is one maximal record. Below it a single write can fill a file on its own, so every
    // write rotates: one WAL file per record, a `file_size` call per intervening file in
    // `bytes_since()`, and a retention pass that walks the lot every flush tick. `WALWriter`
    // deliberately does *not* enforce this - its unit tests drive a 512-byte threshold to reach
    // rotation without writing megabytes, which is a reasonable thing for a component test to do.
    // The difference is who chose the number: a test that wants one file per record gets it, an
    // operator who typed a plausible-looking small number is told why it is not.
    if (config.wal_rotate_bytes < MIN_WAL_ROTATE_THRESHOLD ||
        config.wal_rotate_bytes > MAX_WAL_ROTATE_THRESHOLD) {
        std::fprintf(stderr,
                     "Error: --wal-rotate-bytes (%zu) must be between %zu and %zu bytes. The floor "
                     "is one maximal record (a %zu-byte header plus the %zu-byte payload limit), "
                     "below which a single write fills a file and every write rotates. The ceiling "
                     "is 2 GiB, because a WAL position is a file index and a 32-bit offset read as "
                     "one value, and a larger file would let the offset wrap and report a position "
                     "inside the wrong part of the file.\n",
                     config.wal_rotate_bytes,
                     MIN_WAL_ROTATE_THRESHOLD,
                     MAX_WAL_ROTATE_THRESHOLD,
                     sizeof(WALRecordV2),
                     WAL_MAX_PAYLOAD_LEN);
        std::exit(1);
    }

    // Validation: handover windows must be sane. A cooldown shorter than the
    // grace window would let the outgoing primary win the race it announced,
    // which is the bug this configuration exists to prevent.
    if (config.handover_grace_seconds <= 0 || config.handover_cooldown_seconds <= 0) {
        std::fprintf(stderr,
                     "Error: --handover-grace-seconds and --handover-cooldown-seconds "
                     "must be positive\n");
        std::exit(1);
    }
    // A negative wait would read as "no wait" in election_wait_elapsed() and quietly reopen the
    // window #82 closed. Rejected rather than clamped: a parser that fixes up what it does not
    // understand is how --prot 5599 used to start a server on the default port (#36).
    if (config.election_lease_wait_ms < 0) {
        std::fprintf(stderr,
                     "Error: --election-lease-wait-ms (%ld) cannot be negative. Use 0 to derive "
                     "it from the lease TTL, which is the intended setting\n",
                     static_cast<long>(config.election_lease_wait_ms));
        std::exit(1);
    }
    if (config.handover_cooldown_seconds < config.handover_grace_seconds) {
        std::fprintf(stderr,
                     "Error: --handover-cooldown-seconds (%ld) must be >= "
                     "--handover-grace-seconds (%ld)\n",
                     static_cast<long>(config.handover_cooldown_seconds),
                     static_cast<long>(config.handover_grace_seconds));
        std::exit(1);
    }

    // Validation: --shard-id requires --coordinator-endpoints
    if (!config.shard_id.empty() && config.coordinator_endpoints.empty()) {
        std::fprintf(stderr,
            "Error: --shard-id requires --coordinator-endpoints to be specified. "
            "Shard mode needs etcd for shard map coordination.\n");
        std::exit(1);
    }

    if (!config.coordinator_endpoints.empty()) {
        OB_LOG_INFO("tcp_server", "Handover: grace=%lds cooldown=%lds",
                    static_cast<long>(config.handover_grace_seconds),
                    static_cast<long>(config.handover_cooldown_seconds));
    }

    if (!config.shard_id.empty()) {
        OB_LOG_INFO("tcp_server", "Shard mode enabled: shard_id=%s vnodes=%u",
                    config.shard_id.c_str(), config.shard_vnodes);
    }

    // Validation: --multi-master requires --mm-node-id
    if (config.multi_master && config.mm_node_id == 0) {
        std::fprintf(stderr,
            "Error: --multi-master requires --mm-node-id <uint16>\n");
        std::exit(1);
    }

    // Validation: --multi-master requires --coordinator-endpoints
    if (config.multi_master && config.coordinator_endpoints.empty()) {
        std::fprintf(stderr,
            "Error: --multi-master requires --coordinator-endpoints for peer discovery\n");
        std::exit(1);
    }

    // Validation: --multi-master requires --mm-replication-port
    if (config.multi_master && config.mm_replication_port == 0) {
        std::fprintf(stderr,
            "Error: --multi-master requires --mm-replication-port <port>\n");
        std::exit(1);
    }

    // Validation: --multi-master is incompatible with --read-only
    if (config.multi_master && config.read_only) {
        std::fprintf(stderr,
            "Error: --multi-master is incompatible with --read-only\n");
        std::exit(1);
    }

    // Validation: --multi-master is incompatible with --primary-host/--primary-port
    if (config.multi_master && (!config.primary_host.empty() || config.primary_port > 0)) {
        std::fprintf(stderr,
            "Error: --multi-master is incompatible with --primary-host/--primary-port "
            "(single-primary replication)\n");
        std::exit(1);
    }

    // Validation: warn if --replication-port is specified in MM mode (it will be ignored)
    if (config.multi_master && config.replication_port > 0) {
        OB_LOG_WARN("cli",
            "replication-port is ignored in multi-master mode");
    }

    // Validation: --mm-replication-port and --replication-port must be different ports
    if (config.multi_master && config.replication_port > 0 &&
        config.mm_replication_port == config.replication_port) {
        std::fprintf(stderr,
            "Error: --mm-replication-port and --replication-port must be different ports\n");
        std::exit(1);
    }

    // Validation: --mm-replication-port requires --multi-master mode
    if (!config.multi_master && config.mm_replication_port > 0) {
        std::fprintf(stderr,
            "Error: --mm-replication-port requires --multi-master mode\n");
        std::exit(1);
    }

    if (config.multi_master) {
        OB_LOG_INFO("cli", "Multi-master mode: node_id=%u replication_port=%u anti_entropy=%us",
                    config.mm_node_id, config.mm_replication_port,
                    config.anti_entropy_interval_sec);
    }

    // The profile is applied last and refuses a name it does not know, because a parser that
    // ignores what it does not understand hides operator mistakes (#27, #36): `--profile bost`
    // must not start a node in eco and look like it started in boost.
    //
    // It only sets what the operator did not. A value from the command line or the config file
    // wins, and what the profile did set is attributed to the profile, so `--print-config` answers
    // "what is this node doing" rather than "which switches were thrown".
    {
        const bool spin_set = origin.count("io-spin-us") > 0;
        if (config.profile == "boost") {
            if (!spin_set) {
                config.io_spin_us = kBoostSpinUs;
                origin["io-spin-us"] = Origin::Profile;
            }
        } else if (config.profile != "eco") {
            std::fprintf(stderr,
                "Error: unknown --profile '%s'; known profiles are 'eco' and 'boost'\n",
                config.profile.c_str());
            std::exit(1);
        }
        OB_LOG_INFO("cli",
                    "io profile: %s (io-spin-us=%llu) — %s",
                    config.profile.c_str(),
                    static_cast<unsigned long long>(config.io_spin_us),
                    config.io_spin_us == 0
                        ? "the io thread blocks between events"
                        : "the io thread polls after an event and may use a whole core while "
                          "traffic flows");
    }

    ResolvedConfig resolved{config, origin};

    if (print_config_requested) {
        // Printed and exited, without opening a port. Diagnostics that need a free port are useless
        // exactly when the port is taken - which is one of the situations you reach for them in.
        std::fputs(format_config(resolved).c_str(), stdout);
        std::exit(0);
    }

    return resolved;
}

std::string format_config(const ResolvedConfig& resolved) {
    const ServerConfig& c = resolved.config;

    auto where = [&resolved](const char* key) -> const char* {
        const auto it = resolved.origin.find(key);
        if (it == resolved.origin.end()) return "default";
        switch (it->second) {
            case Origin::File:        return "file";
            case Origin::CommandLine: return "command line";
            case Origin::Profile:     return "profile";
            case Origin::Default:     break;
        }
        return "default";
    };

    std::string out;
    auto line = [&out, &where](const char* key, const std::string& value) {
        std::string padded = key;
        padded.resize(std::max<size_t>(padded.size(), 32), ' ');
        out += "  " + padded + " " + value + "  (" + where(key) + ")\n";
    };

    out += "# Resolved configuration. Provenance in brackets: a list of values does not say which\n";
    out += "# of them you chose, and that is the question this flag exists to answer.\n";
    line("anti-entropy-interval-seconds", std::to_string(c.anti_entropy_interval_sec));
    {
        std::string joined;
        for (size_t i = 0; i < c.coordinator_endpoints.size(); ++i) {
            if (i > 0) joined += ",";
            joined += c.coordinator_endpoints[i];
        }
        line("coordinator-endpoints", joined);
    }
    line("coordinator-lease-ttl", std::to_string(c.coordinator_lease_ttl));
    line("data-dir", c.data_dir);
    line("election-deference-ms", std::to_string(c.election_deference_ms));
    line("election-lease-wait-ms", std::to_string(c.election_lease_wait_ms));
    line("failover-enabled", c.failover_enabled ? "true" : "false");
    line("drain-timeout-ms", std::to_string(c.drain_timeout_ms));
    line("io-spin-us", std::to_string(c.io_spin_us));
    line("io-threads", std::to_string(c.io_threads));
    line("profile", c.profile);
    line("flush-interval-ms", std::to_string(c.flush_interval_ms));
    // The *path*, and there is no value to print because the secret is never a field of
    // ServerConfig. `--print-config` exists to be pasted into a ticket.
    line("auth-secret-file", c.auth_secret_file.empty() ? "(none)" : c.auth_secret_file);
    line("cluster-secret-file", c.cluster_secret_file.empty() ? "(none)" : c.cluster_secret_file);
    line("fsync-policy",
         c.fsync_policy == FsyncPolicy::EVERY ? "every"
             : c.fsync_policy == FsyncPolicy::NONE ? "none" : "interval");
    line("handover-cooldown-seconds", std::to_string(c.handover_cooldown_seconds));
    line("handover-grace-seconds", std::to_string(c.handover_grace_seconds));
    line("log-level", c.log_level);
    line("max-sessions", std::to_string(c.max_sessions));
    line("max-subscriber-queue-bytes", std::to_string(c.max_subscriber_queue_bytes));
    line("max-subscriptions-per-session", std::to_string(c.max_subscriptions_per_session));
    line("metrics-bind", c.metrics_bind.empty() ? "(every interface)" : c.metrics_bind);
    line("metrics-port", std::to_string(c.metrics_port));
    line("mm-max-catchup-bytes", std::to_string(c.mm_max_catchup_bytes));
    line("mm-max-peer-send-buffer", std::to_string(c.mm_max_peer_send_buf_bytes));
    line("mm-node-id", std::to_string(c.mm_node_id));
    line("mm-replication-port", std::to_string(c.mm_replication_port));
    line("multi-master", c.multi_master ? "true" : "false");
    line("node-id", c.node_id);
    line("port", std::to_string(c.port));
    line("primary-host", c.primary_host);
    line("primary-port", std::to_string(c.primary_port));
    line("read-only", c.read_only ? "true" : "false");
    line("replication-compress", c.replication_compress ? "true" : "false");
    line("replication-port", std::to_string(c.replication_port));
    line("shard-id", c.shard_id);
    line("shard-vnodes", std::to_string(c.shard_vnodes));
    line("snapshot-chunk-size", std::to_string(c.snapshot_chunk_size));
    line("snapshot-staging-dir", c.snapshot_staging_dir);
    line("tls-ca-file", c.tls_ca_file.empty() ? "(none)" : c.tls_ca_file);
    line("tls-cert-file", c.tls_cert_file.empty() ? "(none)" : c.tls_cert_file);
    line("tls-client", c.tls_client ? "true" : "false");
    line("tls-key-file", c.tls_key_file.empty() ? "(none)" : c.tls_key_file);
    line("tls-multi-master", c.tls_multi_master ? "true" : "false");
    {
        std::string joined;
        for (const auto& n : c.tls_peer_names) {
            if (!joined.empty()) joined += ",";
            joined += n;
        }
        line("tls-peer-names", joined.empty() ? "(any name the CA signed)" : joined);
    }
    line("tls-replication", c.tls_replication ? "true" : "false");
    line("ttl-hours", std::to_string(c.ttl_hours));
    line("ttl-scan-interval-seconds", std::to_string(c.ttl_scan_interval_seconds));
    line("wal-rotate-bytes", std::to_string(c.wal_rotate_bytes));
    return out;
}

ServerConfig parse_cli_args(int argc, char* argv[]) {
    return resolve_cli_args(argc, argv).config;
}

// ── TcpServer ─────────────────────────────────────────────────────────────────

TcpServer::TcpServer(ServerConfig config)
    : config_(std::move(config))
    , secrets_(load_secrets_or_exit(config_))
    , tls_(load_tls_or_exit(config_))
    , read_only_(config_.read_only)
{
    ReplicationConfig repl_config{};
    if (!config_.multi_master) {
        repl_config.port = config_.replication_port;
        repl_config.compress = config_.replication_compress;
        repl_config.cluster_secret = secrets_.cluster;
        repl_config.tls_server = tls_.replication_server;
    }
    // In MM mode, repl_config.port stays 0 → Engine won't create ReplicationManager

    ReplicationClientConfig repl_client_config{};
    repl_client_config.primary_host = config_.primary_host;
    repl_client_config.primary_port = config_.primary_port;
    repl_client_config.state_file   = config_.data_dir + "/repl_state.txt";
    repl_client_config.snapshot_chunk_size = config_.snapshot_chunk_size;
    repl_client_config.snapshot_staging_dir = config_.snapshot_staging_dir;
    repl_client_config.cluster_secret = secrets_.cluster;
    repl_client_config.tls_client     = tls_.replication_client;

    FailoverConfig failover_config{};
    if (!config_.coordinator_endpoints.empty()) {
        failover_config.coordinator.endpoints = config_.coordinator_endpoints;
        failover_config.coordinator.lease_ttl_seconds = config_.coordinator_lease_ttl;
        failover_config.handover_grace_seconds    = config_.handover_grace_seconds;
        failover_config.election_deference_ms      = config_.election_deference_ms;
        failover_config.election_lease_wait_ms     = config_.election_lease_wait_ms;
        failover_config.handover_cooldown_seconds = config_.handover_cooldown_seconds;
        failover_config.coordinator.node_id = config_.node_id;
        failover_config.failover_enabled = config_.failover_enabled;
        failover_config.replication_port = config_.replication_port;
        failover_config.replication_address = "127.0.0.1:" + std::to_string(config_.replication_port);
    }

    engine_ = std::make_unique<Engine>(config_.data_dir,
                                      config_.flush_interval_ms * 1'000'000ULL,
                                      config_.fsync_policy,
                                       repl_config, repl_client_config, failover_config,
                                       TTLConfig{config_.ttl_hours,
                                                 config_.ttl_scan_interval_seconds},
                                       // Designated initialisers: this was positional, and adding
                                       // a field in the middle of MultiMasterConfig silently
                                       // shifted every argument after it.
                                       MultiMasterConfig{
                                           .node_id = config_.mm_node_id,
                                           .replication_port = config_.mm_replication_port,
                                           .enabled = config_.multi_master,
                                           .compress = config_.replication_compress,
                                           .max_catchup_bytes = config_.mm_max_catchup_bytes,
                                           .anti_entropy_interval_sec =
                                               config_.anti_entropy_interval_sec,
                                           .max_peer_send_buf_bytes =
                                               config_.mm_max_peer_send_buf_bytes,
                                           .shard_id = config_.shard_id,
                                           .coordinator_config = CoordinatorConfig{
                                               config_.coordinator_endpoints,
                                               config_.coordinator_lease_ttl,
                                               config_.node_id,
                                               "/ob/"
                                           },
                                           .cluster_secret = secrets_.cluster,
                                           .tls_server = tls_.mesh_server,
                                           .tls_client = tls_.mesh_client
                                       },
                                       config_.wal_rotate_bytes);
}

TcpServer::~TcpServer() {
    if (listen_fd_ >= 0) ::close(listen_fd_);
}

// ── Reactor ───────────────────────────────────────────────────────────────────
//
// One epoll loop and the sessions it owns: what `TcpServer::run()` was, taken out of it so there
// can be more than one ("the engine uses the whole machine", stage 2). A `Session`
// is not thread-safe and never was, so a session belongs to exactly one reactor for its whole life,
// and everything a reactor does to one - reading, answering, arming EPOLLOUT, closing - happens on
// that reactor's thread.
//
// What every reactor of one server shares is in `ReactorShared`, and it is references: the server
// owns all of it and outlives every reactor. The counters in `ServerStats` are atomics for exactly
// this reason.

namespace {

class Reactor;

struct ReactorShared {
    const ServerConfig&      config;
    Engine&                  engine;
    const LoadedTlsContexts& tls;
    const LoadedSecrets&     secrets;
    std::atomic<bool>&       running;
    std::atomic<bool>&       draining;
    std::atomic<bool>&       read_only;
    ServerStats&             stats;
    ShardCoordinator*        shard_coord;
    MetricsServer*           metrics_server;


    /// Every reactor of this server, for the one that accepts to deal connections to. Filled
    /// before any of them runs and not changed afterwards, so reading it needs no lock.
    std::vector<Reactor*>*   reactors;

    /// Held for a command `serialised_across_reactors()` names, so it runs alone as it did when
    /// one loop ran every command.
    std::mutex&              admin_mtx;
};

class Reactor {
public:
    /// `listen_fd` is the server's listening socket for the reactor that accepts, and a descriptor
    /// of -1 for any other. A reference, because the reactor that accepts is also the one that
    /// closes it when a drain begins, and the server's destructor must see that.
    Reactor(int index, const ReactorShared& shared, int& listen_fd);
    ~Reactor();

    Reactor(const Reactor&) = delete;
    Reactor& operator=(const Reactor&) = delete;

    /// The event loop. Returns when the server stops running: a drain that finished or reached its
    /// deadline, or a fatal `epoll_wait()` error. Every session still open is closed on the way out.
    void run_loop();

    /// Give this reactor a connection another one accepted. Called on the accepting reactor's
    /// thread; the descriptor is registered, wrapped for TLS and greeted on this one's, because a
    /// `Session` belongs to the thread that serves it from its first byte.
    void hand_off(int fd, uint64_t conn_id);

    int index() const { return index_; }

private:
    /// Take ownership of an accepted connection: session, epoll registration, TLS, banner. The
    /// session count was taken at accept, so every failure here gives it back.
    void adopt(int fd, uint64_t conn_id);

    /// Everything handed off since the last pass, adopted in the order it was accepted.
    void adopt_handed_off();

    /// A gauge this reactor contributes to, published as the change since its last publication.
    /// `set_gauge` from several reactors would be the last writer's number rather than the sum.
    void publish_gauge(const char* name, int64_t now, int64_t& published);
    /// One place that closes a session, so every close carries a reason in the log.
    /// This used to be five copies of the same four lines, none of them logging,
    /// which is why a session dying in the middle of a large response left no trace.
    void close_session(int fd, const char* reason);

    /// Arm EPOLLOUT for a session with queued output, and disarm once it drains.
    ///
    /// Armed only after a partial write. Leaving EPOLLOUT armed permanently on an
    /// edge-triggered fd spins the loop and burns a core.
    void arm_epollout(int fd);
    void disarm_epollout(int fd);

    const int                index_;
    const ServerConfig&      config_;
    std::vector<Reactor*>*   reactors_;
    std::mutex&              admin_mtx_;
    Engine*                  engine_;
    const LoadedTlsContexts& tls_;
    const LoadedSecrets&     secrets_;
    std::atomic<bool>&       running_;
    std::atomic<bool>&       draining_;
    std::atomic<bool>&       read_only_;
    ServerStats&             stats_;
    ShardCoordinator*        shard_coord_;
    MetricsServer*           metrics_server_;
    int&                     listen_fd_;
    int                      epoll_fd_{-1};

    /// An eventfd in this reactor's epoll set: written by `hand_off()` from the accepting thread.
    int                      wake_fd_{-1};
    std::mutex               handoff_mtx_;
    std::vector<std::pair<int, uint64_t>> handoff_;   ///< (descriptor, conn_id), under handoff_mtx_

    /// Where the next accepted connection goes. Only the accepting reactor reads it.
    size_t                   next_reactor_{0};

    // What this reactor last contributed to each summed gauge.
    int64_t published_subscriptions_active_{0};
    int64_t published_subscription_queued_bytes_{0};
    int64_t published_session_pending_bytes_{0};

    SessionManager  sessions_;

    // Streaming subscriptions (#45). Owned by this loop, and its eventfd joins the epoll set: a
    // notification arriving from another thread enqueues and wakes us, because Session is not
    // thread-safe and arming EPOLLOUT belongs to this thread.
    SubscriptionHub hub_;

    /// Monotonic per run. Descriptor numbers are reused, so a subscription pinned to `fd` alone
    /// would outlive its connection and push rows to whoever inherits the number.
    uint64_t next_conn_id_{1};

    static constexpr int MAX_EVENTS = 64;
    struct epoll_event   events_[MAX_EVENTS]{};

    // Whether this loop has already reacted to a shutdown request. See the note in shutdown():
    // the descriptors and the metrics server belong to this thread, and only this thread closes
    // them.
    bool drain_started_{false};
    // Only read once `drain_started_` is true, so the value before the drain begins is never used.
    std::chrono::steady_clock::time_point drain_started_at_{};

    // What has already been published to the monotonic counters, so the loop can publish deltas.
    uint64_t published_rows_pushed_{0};
    uint64_t published_overflow_disconnects_{0};
    uint64_t published_refused_{0};

    // The wait is a decision rather than a constant since the io-spin profile: `io_wait_ms()`
    // answers 0 while the spin window since the last event is open and the blocking timeout
    // otherwise, so `--io-spin-us 0` — the default — is byte for byte the old behaviour.
    std::chrono::steady_clock::time_point last_event_at_{};

    /// The per-event boundary (#112, #131): what the log calls this loop, and the guard that is
    /// loud once per episode and counts every occurrence in `ob_loop_errors_total`.
    std::string event_what_;
    LoopGuard   event_guard_;

    /// The writes of the read being served, held until something else needs answering (#155),
    /// and their answers. Members so a read allocates nothing once they have grown; emptied at
    /// the start of every read, so nothing held for one session can reach another.
    std::vector<Command>     pending_writes_;
    std::vector<std::string> write_answers_;
};

Reactor::Reactor(int index, const ReactorShared& shared, int& listen_fd)
    : index_(index)
    , config_(shared.config)
    , reactors_(shared.reactors)
    , admin_mtx_(shared.admin_mtx)
    , engine_(&shared.engine)
    , tls_(shared.tls)
    , secrets_(shared.secrets)
    , running_(shared.running)
    , draining_(shared.draining)
    , read_only_(shared.read_only)
    , stats_(shared.stats)
    , shard_coord_(shared.shard_coord)
    , metrics_server_(shared.metrics_server)
    , listen_fd_(listen_fd)
    , sessions_(shared.config.max_sessions)
    , hub_(shared.config.max_subscriber_queue_bytes, shared.config.max_subscriptions_per_session)
    , event_what_("serving a client event on reactor " + std::to_string(index))
    , event_guard_("tcp_server", event_what_.c_str(), &shared.engine.registry()) {
    // 5. Create epoll instance.
    epoll_fd_ = ::epoll_create1(0);
    if (epoll_fd_ < 0) {
        throw std::runtime_error(std::string("epoll_create1() failed: ") + std::strerror(errno));
    }

    // 6. Add the listening socket to epoll, on the reactor that accepts.
    if (listen_fd_ >= 0) {
        struct epoll_event ev{};
        ev.events  = EPOLLIN;
        ev.data.fd = listen_fd_;
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev) < 0) {
            const std::string why = std::strerror(errno);
            ::close(epoll_fd_);
            epoll_fd_ = -1;
            throw std::runtime_error("epoll_ctl() failed: " + why);
        }
    }

    if (hub_.wakeup_fd() >= 0) {
        struct epoll_event hub_ev{};
        hub_ev.events  = EPOLLIN;
        hub_ev.data.fd = hub_.wakeup_fd();
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, hub_.wakeup_fd(), &hub_ev) < 0) {
            OB_LOG_ERROR("tcp_server", "epoll_ctl on the subscription wakeup fd failed: %s",
                         std::strerror(errno));
        } else {
            OB_LOG_INFO("tcp_server", "Subscription hub wakeup fd=%d joined the epoll set",
                        hub_.wakeup_fd());
        }
    }
    wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd_ < 0) {
        const std::string why = std::strerror(errno);
        ::close(epoll_fd_);
        epoll_fd_ = -1;
        throw std::runtime_error("eventfd() failed for reactor " + std::to_string(index_) + ": " +
                                 why);
    }
    struct epoll_event wake_ev{};
    wake_ev.events  = EPOLLIN;
    wake_ev.data.fd = wake_fd_;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &wake_ev) < 0) {
        const std::string why = std::strerror(errno);
        ::close(wake_fd_);
        ::close(epoll_fd_);
        wake_fd_ = epoll_fd_ = -1;
        throw std::runtime_error("epoll_ctl() failed for reactor " + std::to_string(index_) +
                                 "'s wake descriptor: " + why);
    }
    OB_LOG_DEBUG("tcp_server", "Reactor %d ready: epoll fd=%d, wake fd=%d, accepts=%s", index_,
                 epoll_fd_, wake_fd_, listen_fd_ >= 0 ? "yes" : "no");
}

Reactor::~Reactor() {
    if (wake_fd_ >= 0) {
        ::close(wake_fd_);
        wake_fd_ = -1;
    }
    // A connection handed off and never adopted belongs to nobody else, so it is closed here and
    // its place in the session count given back.
    for (const auto& [fd, conn_id] : handoff_) {
        OB_LOG_INFO("tcp_server", "Reactor %d closing fd=%d (conn_id=%llu): handed off, never "
                    "adopted", index_, fd, static_cast<unsigned long long>(conn_id));
        ::close(fd);
        stats_.active_sessions.fetch_sub(1, std::memory_order_relaxed);
        engine_->registry().increment_gauge("ob_active_sessions", -1);
    }
    handoff_.clear();
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }
}

void Reactor::close_session(int fd, const char* reason) {
    size_t pending = 0;
    if (Session* s = sessions_.get_session(fd)) {
        pending = s->pending_output_bytes();
    }
    OB_LOG_INFO("tcp_server",
                "Closing session: fd=%d reason=%s pending_bytes=%zu",
                fd, reason, pending);
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    // Before remove_session, and the order is load-bearing: the other way round leaves a window
    // in which a notification lands in a queue whose session is already gone, and the hub would
    // have to tolerate that instead of the invariant holding.
    if (Session* s = sessions_.get_session(fd)) {
        hub_.remove_connection(*engine_, fd, s->conn_id());
    }
    sessions_.remove_session(fd);
    stats_.active_sessions.fetch_sub(1, std::memory_order_relaxed);
    engine_->registry().increment_gauge("ob_active_sessions", -1);
}

void Reactor::arm_epollout(int fd) {
    if (epoll_fd_ < 0 || fd < 0) return;

    struct epoll_event ev{};
    ev.events  = EPOLLIN | EPOLLOUT | EPOLLET;
    ev.data.fd = fd;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
        OB_LOG_WARN("tcp_server", "arm_epollout failed: fd=%d errno=%s",
                    fd, std::strerror(errno));
    }
}

void Reactor::hand_off(int fd, uint64_t conn_id) {
    {
        std::lock_guard<std::mutex> lock(handoff_mtx_);
        handoff_.emplace_back(fd, conn_id);
    }
    const uint64_t one = 1;
    const ssize_t wr = ::write(wake_fd_, &one, sizeof(one));
    if (wr != static_cast<ssize_t>(sizeof(one))) {
        // An eventfd write fails only when the counter is saturated, which already means a wake-up
        // is pending; the connection is in the queue either way and the next pass adopts it.
        OB_LOG_WARN("tcp_server", "Reactor %d: wake write for fd=%d returned %zd: %s", index_, fd,
                    wr, std::strerror(errno));
    }
    OB_LOG_DEBUG("tcp_server", "Handed fd=%d (conn_id=%llu) to reactor %d", fd,
                 static_cast<unsigned long long>(conn_id), index_);
}

void Reactor::adopt_handed_off() {
    uint64_t counter = 0;
    const ssize_t rd = ::read(wake_fd_, &counter, sizeof(counter));
    (void)rd;   // EAGAIN just means a previous pass already took the wake-up; the queue decides
    std::vector<std::pair<int, uint64_t>> taken;
    {
        std::lock_guard<std::mutex> lock(handoff_mtx_);
        taken.swap(handoff_);
    }
    for (const auto& [fd, conn_id] : taken) {
        adopt(fd, conn_id);
    }
}

void Reactor::adopt(int fd, uint64_t conn_id) {
    // Every failure below gives back the place in the session count the accepting reactor took.
    const auto give_back = [&] {
        stats_.active_sessions.fetch_sub(1, std::memory_order_relaxed);
        engine_->registry().increment_gauge("ob_active_sessions", -1);
    };

    if (!sessions_.add_session(fd, conn_id)) {
        // Not the global limit, which the accepting reactor applied: this reactor's own table.
        const char* msg = "ERR server full\n";
        auto wr = ::send(fd, msg, std::strlen(msg), MSG_NOSIGNAL);
        (void)wr;
        ::close(fd);
        give_back();
        return;
    }

    // Add to epoll (edge-triggered).
    struct epoll_event cev{};
    cev.events  = EPOLLIN | EPOLLET;
    cev.data.fd = fd;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &cev) < 0) {
        OB_LOG_WARN("tcp_server", "Reactor %d: cannot register fd=%d: %s", index_, fd,
                    std::strerror(errno));
        sessions_.remove_session(fd);   // closes the descriptor
        give_back();
        return;
    }

    Session* s = sessions_.get_session(fd);
    if (s && tls_.client_port) {
        // Wrap before anything is written: the banner is application data and must not precede
        // the handshake. It is queued here and goes out with the first flush after the handshake
        // completes, which is what send_response() on a handshaking session does - the bytes sit in
        // send_buf_ and SSL_write is not reached until tls_handshaking_ clears.
        try {
            s->enable_tls(tls_.client_port->wrap(fd, /*server_side=*/true));
            OB_LOG_DEBUG("tls", "handshake started: fd=%d", fd);
        } catch (const std::exception& e) {
            OB_LOG_WARN("tls", "cannot start a handshake on fd=%d: %s", fd, e.what());
            // Unregistered first, while the number is still this connection's, and then closed
            // exactly once - by remove_session(). The path this replaces closed it a second time
            // after remove_session() had, which on a server with other threads opening files is a
            // close of whatever the kernel had handed that number to in between (#128's class).
            ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
            sessions_.remove_session(fd);
            give_back();
            return;
        }
    }

    // INFO, like the close: a connection's two ends are the events an operator reads to find a
    // client, and with several reactors this line is the only place that says which one serves it.
    OB_LOG_INFO("tcp_server", "Reactor %d adopted fd=%d conn_id=%llu from %s", index_, fd,
                static_cast<unsigned long long>(conn_id), peer_address(fd).c_str());

    // Send welcome message. The version comes from the build system (#148).
    if (s) {
        static const std::string banner =
            "OK ob_tcp_server v" + std::string(version()) + "\n\n";
        s->send_response(banner);
    }
}

void Reactor::publish_gauge(const char* name, int64_t now, int64_t& published) {
    if (now != published) {
        engine_->registry().increment_gauge(name, now - published);
        published = now;
    }
}

void Reactor::disarm_epollout(int fd) {
    if (epoll_fd_ < 0 || fd < 0) return;

    // Back to read-only interest. Leaving EPOLLOUT armed on an edge-triggered fd
    // makes epoll_wait return immediately whenever the socket is writable, which is
    // almost always, and the loop spins on a core for nothing.
    struct epoll_event ev{};
    ev.events  = EPOLLIN | EPOLLET;
    ev.data.fd = fd;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
        OB_LOG_WARN("tcp_server", "disarm_epollout failed: fd=%d errno=%s",
                    fd, std::strerror(errno));
    }
}

// At column zero on purpose, like every loop in src/: the scan in `tests/test_thread_boundaries.cpp`
// finds a loop by that signature and has to classify it (#131).
void Reactor::run_loop() {
    last_event_at_ = std::chrono::steady_clock::now();
    while (running_.load(std::memory_order_relaxed)) {
        const int wait_ms = io_wait_ms(last_event_at_, config_.io_spin_us, 100,
                                       std::chrono::steady_clock::now());
        int nfds = ::epoll_wait(epoll_fd_, events_, MAX_EVENTS, wait_ms);
        if (nfds > 0) last_event_at_ = std::chrono::steady_clock::now();
        if (nfds < 0) {
            if (errno == EINTR) continue;
            // Fatal. With one loop this ended run(); with several, leaving alone would strand this
            // reactor's sessions while the others went on, so it stops the server the same way.
            OB_LOG_ERROR("tcp_server", "Reactor %d: epoll_wait failed, stopping the server: %s",
                         index_, std::strerror(errno));
            running_.store(false, std::memory_order_relaxed);
            break;
        }

        // Subscriptions, once per iteration and before the event loop below.
        //
        // Unconditional, and not inside a branch on the wakeup fd — the same decision as
        // `poll_snapshot_preparation()` in the multi-master loop (#79): a plain 100 ms timeout also
        // picks up anything queued, so a lost wake-up costs latency rather than delivery.
        {
            const auto condemned = hub_.drain(
                sessions_, [&](int fd) { arm_epollout(fd); });
            for (int fd : condemned) {
                close_session(fd, "subscriber queue overflowed");
            }
            publish_gauge("ob_subscriptions_active", static_cast<int64_t>(hub_.active()),
                          published_subscriptions_active_);
            publish_gauge("ob_subscription_queued_bytes", static_cast<int64_t>(hub_.queued_bytes()),
                          published_subscription_queued_bytes_);
            // Counters are monotonic and only ever incremented, so the loop publishes the delta
            // against what it last saw rather than the hub's total. The hub keeps its own totals
            // because it is testable without a registry, and a second source of truth here would be
            // the kind that drifts.
            const auto publish = [&](const char* name, uint64_t total, uint64_t& seen) {
                if (total > seen) {
                    engine_->registry().increment_counter(name, total - seen);
                    seen = total;
                }
            };
            publish("ob_subscription_rows_pushed_total", hub_.rows_pushed(),
                    published_rows_pushed_);
            publish("ob_subscription_overflow_disconnects_total",
                    hub_.overflow_disconnects(), published_overflow_disconnects_);
            publish("ob_subscription_refused_total", hub_.refused(),
                    published_refused_);
        }

        // Once per loop iteration, not per event: the sum walks the session map, and
        // there is nothing to learn from updating it several times per wake-up.
        publish_gauge("ob_session_pending_bytes",
                      static_cast<int64_t>(sessions_.total_pending_output_bytes()),
                      published_session_pending_bytes_);

        // One event is the unit a failure costs, not the reactor: an exception while serving one
        // closes that session, is counted, and the loop goes on with the rest of the batch. The
        // rest matters because every client descriptor is edge-triggered, so an event abandoned
        // with its batch is not delivered again - the reason the mesh loop guards per event too.
        bool threw_this_pass = false;
        for (int i = 0; i < nfds; ++i) {
            int fd = events_[i].data.fd;
            try {

                if (fd == wake_fd_) {
                    adopt_handed_off();
                    continue;
                }

                if (fd == listen_fd_) {
                    // Draining: stop accepting new connections.
                    if (draining_.load(std::memory_order_relaxed)) {
                        // Reject all pending connections.
                        //
                        // OB_NO_TCP_NODELAY: one line and a close, so there is never an earlier
                        // unacknowledged byte for Nagle to hold a second write behind (#140).
                        while (true) {
                            int reject_fd = ::accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK);
                            if (reject_fd < 0) break;
                            const char* msg = "ERR server shutting down\n";
                            auto wr = ::send(reject_fd, msg, std::strlen(msg), MSG_NOSIGNAL);
                            (void)wr;
                            ::close(reject_fd);
                        }
                        continue;
                    }

                    // Accept new connection(s).
                    while (true) {
                        struct sockaddr_in client_addr{};
                        socklen_t client_len = sizeof(client_addr);
                        int client_fd = ::accept4(listen_fd_,
                                                  reinterpret_cast<struct sockaddr*>(&client_addr),
                                                  &client_len,
                                                  SOCK_NONBLOCK);
                        if (client_fd < 0) {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                            break; // accept error, continue loop
                        }

                        // Before anything is written to it, including the banner: a client that
                        // pipelines pays a delayed-ACK timer per round trip without this (#140).
                        set_tcp_nodelay(client_fd, "tcp_server");

                        // The limit is global and it is taken here, on the reactor that accepts.
                        // Counted wherever the connection is adopted instead, a burst of accepts
                        // would be checked against a number that had not caught up and let more
                        // than --max-sessions in.
                        const int admitted =
                            stats_.active_sessions.fetch_add(1, std::memory_order_relaxed) + 1;
                        if (admitted > config_.max_sessions) {
                            stats_.active_sessions.fetch_sub(1, std::memory_order_relaxed);
                            // Server full — reject.
                            const char* msg = "ERR server full\n";
                            auto wr = ::send(client_fd, msg, std::strlen(msg), MSG_NOSIGNAL);
                            (void)wr;
                            ::close(client_fd);
                            continue;
                        }
                        engine_->registry().increment_gauge("ob_active_sessions");

                        // Dealt in turn, so four connections on four reactors are one each, and a
                        // connection stays where it is dealt for its whole life.
                        const uint64_t conn_id = next_conn_id_++;
                        Reactor* target = (*reactors_)[next_reactor_++ % reactors_->size()];
                        if (target == this) {
                            adopt(client_fd, conn_id);
                        } else {
                            target->hand_off(client_fd, conn_id);
                        }
                    }
                } else {
                    // Writable first: a session with queued output is waiting on this,
                    // and draining it may be the only thing that lets the client send
                    // its next command.
                    if (events_[i].events & EPOLLOUT) {
                        Session* session = sessions_.get_session(fd);
                        if (session) {
                            // A handshake that wanted to write is why this event arrived.
                            if (session->tls_handshaking()) {
                                if (!session->continue_tls_handshake()) {
                                    close_session(fd, "tls handshake failed");
                                    continue;
                                }
                                if (session->io_want() == IoWant::Write) continue;  // still writing
                                disarm_epollout(fd);
                                if (session->tls_handshaking()) continue;           // now wants to read
                            }
                            if (!session->flush_output()) {
                                close_session(fd, "flush failed");
                                continue;
                            }
                            // Disarm unless OpenSSL still wants to write. With TLS, bytes can remain
                            // queued while OpenSSL is waiting to *read* - a key update - and keeping
                            // EPOLLOUT armed then spins the loop on a writable socket (pitfall 5). The
                            // EPOLLIN path below retries the flush for exactly that case.
                            const bool wants_write = session->io_want() == IoWant::Write;
                            if (!session->has_pending_output() || !wants_write) {
                                disarm_epollout(fd);
                            }
                            if (!session->has_pending_output() && session->close_requested()) {
                                close_session(fd, "quit after flush");
                                continue;
                            }
                        }
                    }

                    if (!(events_[i].events & EPOLLIN)) continue;

                    {
                        // A handshake in progress consumes this event and nothing else: not one byte
                        // reaches feed() until TLS is up, because a command arriving before that is a
                        // command from an unauthenticated transport.
                        Session* session = sessions_.get_session(fd);
                        if (session && session->tls_handshaking()) {
                            if (!session->continue_tls_handshake()) {
                                close_session(fd, "tls handshake failed");
                                continue;
                            }
                            if (session->io_want() == IoWant::Write) arm_epollout(fd);
                            if (session->tls_handshaking()) continue;
                            // Just completed: the queued banner has not been written yet.
                            if (!session->flush_output()) {
                                close_session(fd, "flush failed after handshake");
                                continue;
                            }
                            if (session->has_pending_output() &&
                                session->io_want() == IoWant::Write) {
                                arm_epollout(fd);
                            }
                        }
                        // A write that stopped because OpenSSL wanted to read resumes here - the one
                        // case where readability is what unblocks a *send*.
                        if (session && !session->tls_handshaking() &&
                            session->has_pending_output() && session->io_want() == IoWant::Read) {
                            if (!session->flush_output()) {
                                close_session(fd, "flush failed");
                                continue;
                            }
                            if (session->has_pending_output() &&
                                session->io_want() == IoWant::Write) {
                                arm_epollout(fd);
                            }
                        }
                    }

                    // Client data ready.
                    // Edge-triggered: read until EAGAIN.
                    //
                    // 64 KiB rather than 4 KiB, because since #146 the answers to a read go out in one
                    // send and the read's size is therefore the batch's: a pipelined batch of 64
                    // twenty-level `MINSERT`s is 20,736 bytes, which a 4 KiB read took in six pieces
                    // and answered with six sends (measured: 6.00 sends per batch, 1.00 at 64 KiB). On
                    // the stack of the thread that owns this loop; the edge-triggered loop drains the
                    // socket either way, so a larger read changes how many syscalls a burst costs and
                    // not which session is served first.
                    char buf[64 * 1024];
                    while (true) {
                        Session* session = sessions_.get_session(fd);
                        if (!session) break;

                        size_t got = 0;
                        const Session::IoResult r = session->receive(buf, sizeof(buf), got);
                        if (r == Session::IoResult::Closed) {
                            close_session(fd, "peer closed");
                            break;
                        }
                        if (r == Session::IoResult::Error) {
                            close_session(fd, "read error");
                            break;
                        }
                        if (r == Session::IoResult::Again) {
                            // Nothing more now. With TLS this can mean OpenSSL wants to *write*, so
                            // arm what it asked for rather than assuming readability.
                            if (session->io_want() == IoWant::Write) arm_epollout(fd);
                            break;
                        }

                        auto lines = session->feed(buf, got);

                        // Outside the loop below, and that is the whole point: a client that never
                        // sends a newline produces **no lines**, so every check written per line —
                        // including `max_line_length` directly under here — never runs for it, and
                        // the session accumulates for ever. Measured before this: 227 MiB on one
                        // unauthenticated connection took the server to 257 MiB resident (#143).
                        if (session->unparsed_bytes() > config_.max_unparsed_bytes) {
                            engine_->registry().increment_counter("ob_sessions_unparsed_overflow_total");
                            session->send_response(format_error("unparsed input too long"));
                            close_session(fd, "unparsed input too long");
                            goto next_event;
                        }

                        {
                            // Every command from this read is answered into the session's buffer, and
                            // the buffer goes out in **one** send below (#146). One send per response
                            // made a pipelined batch of 64 commands 64 `send()` calls and 64 segments:
                            // 32.4% of the io thread on an m9g.xlarge. The buffer is the wire order,
                            // so the bytes a client reads are the same bytes in the same order.
                            //
                            // What ends the batch early, and each keeps the effect it had when every
                            // response was sent on its own: a line too long or a full send buffer
                            // close the session after what is queued has been offered to the socket,
                            // `QUIT` closes once it has drained, and a command that asked for the
                            // session to end - a failed `AUTH` - stops the batch **there**. That last
                            // one is the rate limit on authentication (#30): nothing that arrived in
                            // the same read behind a failed attempt is executed.
                            const char* close_after_flush = nullptr;
                            bool        quit              = false;
                            bool        queue_refused     = false;

                            // One answer into the session's buffer. False: the session is to
                            // close, because a client that has stopped reading cannot be sent
                            // more.
                            const auto enqueue = [&](const std::string& response) -> bool {
                                const Session::Queued queued = session->queue_answer(response);
                                if (queued == Session::Queued::TooLargeAlone) {
                                    // No client could be sent this, however fast it reads, so the
                                    // session is not the thing that failed: it gets an error in
                                    // the answer's place and stays open (#152).
                                    const std::string refusal = format_error(
                                        "answer of " + std::to_string(response.size()) +
                                        " bytes is larger than the " +
                                        std::to_string(Session::max_queued_bytes()) +
                                        " a session may have queued; narrow the query or add LIMIT");
                                    return session->queue_response(refusal);
                                }
                                // The send buffer cap: a client that has stopped reading. EPIPE
                                // and ECONNRESET surface in the flush.
                                return queued != Session::Queued::CapExceeded;
                            };

                            // The writes held back so far, applied under one acquisition of the
                            // engine's lock and answered in order (#155). Called before any
                            // other answer is queued and at the end of the read, so the answers
                            // are in the order the commands came and a command after a write sees
                            // it. A client whose buffer refuses an answer here has had the writes
                            // applied and is closed without their answers - the uncertainty of any
                            // write whose answer never arrives, for the writes of one read where
                            // one command at a time it was for one.
                            pending_writes_.clear();
                            const auto apply_held_writes = [&]() -> bool {
                                if (pending_writes_.empty()) return true;
                                execute_writes(pending_writes_, *engine_, *session, stats_,
                                               read_only_.load(std::memory_order_acquire),
                                               &engine_->registry(), shard_coord_,
                                               write_answers_);
                                pending_writes_.clear();
                                for (const std::string& answer : write_answers_) {
                                    if (!enqueue(answer)) return false;
                                }
                                return true;
                            };

                            for (const auto& line : lines) {
                                if (line.size() > config_.max_line_length) {
                                    if (!apply_held_writes()) {
                                        queue_refused = true;
                                        break;
                                    }
                                    (void)session->queue_response(format_error("line too long"));
                                    close_after_flush = "line too long";
                                    break;
                                }

                                // Multi-line blocks (containing \n) are MINSERT — use parse_minsert()
                                Command cmd = (line.find('\n') != std::string::npos)
                                                  ? parse_minsert(line)
                                                  : parse_command(line);
                                if (deferrable_write(cmd, *session, secrets_.client_store())) {
                                    pending_writes_.push_back(std::move(cmd));
                                    continue;
                                }
                                if (!apply_held_writes()) {
                                    queue_refused = true;
                                    break;
                                }

                                // Alone across reactors for the two commands written for one
                                // caller at a time, and released before the answer is queued.
                                std::unique_lock<std::mutex> alone(admin_mtx_, std::defer_lock);
                                if (serialised_across_reactors(cmd.type)) alone.lock();
                                std::string response = execute_command(cmd, *engine_, *session, stats_, read_only_.load(std::memory_order_acquire), &engine_->registry(), shard_coord_, &hub_, secrets_.client_store());
                                if (alone.owns_lock()) alone.unlock();

                                if (response.empty()) {
                                    quit = true;
                                    break;
                                }

                                if (!enqueue(response)) {
                                    queue_refused = true;
                                    break;
                                }

                                // Compression starts AFTER the plain-text ack is queued. Everything
                                // queued before this point goes out as it was queued - plain - and
                                // everything after it is framed, which is the order the client expects.
                                if (cmd.type == CommandType::COMPRESS) {
                                    session->set_compressed(true);
                                }

                                if (session->close_requested()) break;
                            }
                            // The writes that ended the read. Every other way out of the loop
                            // above applied them before the command that ended it.
                            if (!queue_refused && !apply_held_writes()) queue_refused = true;

                            if (queue_refused) {
                                close_session(fd, "send failed");
                                goto next_event;
                            }

                            if (!session->flush_output()) {
                                // A real error: EPIPE, ECONNRESET. A full socket buffer is not one of
                                // these — it leaves bytes queued and is handled just below.
                                close_session(fd, close_after_flush ? close_after_flush : "send failed");
                                goto next_event;
                            }

                            if (close_after_flush) {
                                close_session(fd, close_after_flush);
                                goto next_event;
                            }

                            if (quit) {
                                // If a response is still draining, let it finish first: closing now
                                // would truncate data the client already asked for and is still reading.
                                if (session->has_pending_output()) {
                                    session->request_close_after_flush();
                                    arm_epollout(fd);
                                    goto next_event;
                                }
                                close_session(fd, "quit");
                                goto next_event;
                            }

                            if (session->has_pending_output()) {
                                // The socket took part of the batch. The rest goes out on EPOLLOUT;
                                // closing here is what truncated every response larger than the socket
                                // buffer.
                                arm_epollout(fd);
                            }

                            if (session->close_requested()) {
                                // A command asked for the session to end once its response was out.
                                // `close_requested()` was once consulted only in the EPOLLOUT drain,
                                // which runs only after a *partial* write - so a response small enough
                                // to fit the socket buffer left the session open with the flag set and
                                // nothing reading it, and `ERR auth_failed\n` is eighteen bytes. With
                                // bytes still queued the drain closes it; either way nothing more is
                                // read from this socket.
                                if (!session->has_pending_output()) {
                                    close_session(fd, "close requested after flush");
                                }
                                goto next_event;
                            }
                        }
                    }
                    next_event:;
                }
            } catch (const std::exception& e) {
                threw_this_pass = true;
                event_guard_.caught(e);
                // Nothing can vouch for a session whose event threw halfway, so it is closed rather
                // than served again. The listening socket and the reactor's own descriptors have no
                // session, and for them there is nothing to close.
                if (sessions_.get_session(fd) != nullptr) {
                    close_session(fd, "an exception while serving it");
                }
            }
        }
        // Gated on a pass that had events and none of which threw, so the recovery line cannot
        // alternate with the error it closes (pitfall 291).
        if (nfds > 0 && !threw_this_pass) event_guard_.ok();

        // React to a shutdown request here, on the thread that owns these descriptors, rather
        // than inside shutdown() on the thread that requested it (#80).
        //
        // At the *end* of the iteration, not the start: the events just processed can include one
        // for listen_fd_, and closing it first would leave `fd == listen_fd_` comparing against
        // -1, so that event would fall through to the session path carrying a descriptor this
        // loop had already closed. The wait above has a 100 ms timeout, so nothing is needed to
        // wake it, and during that window new connections are still accepted and answered with
        // "server shutting down" — a better answer than a refused connection.
        if (!drain_started_ && draining_.load(std::memory_order_acquire)) {
            drain_started_ = true;
            // Each reactor starts its own clock, and the drain still has one deadline: the reactor
            // that saw the request first reaches it first, and stopping `running_` stops them all.
            // A shared start would only move the others' clocks to that same moment.
            drain_started_at_ = std::chrono::steady_clock::now();
            OB_LOG_INFO("tcp_server", "Reactor %d: drain requested, %d session(s) open here",
                        index_, sessions_.active_count());
            if (listen_fd_ >= 0) {
                OB_LOG_INFO("tcp_server", "Drain requested: closing the listen socket, fd=%d",
                            listen_fd_);
                if (metrics_server_) {
                    metrics_server_->stop();
                }
                ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, listen_fd_, nullptr);
                ::close(listen_fd_);
                listen_fd_ = -1;
            }
        }

        // Drain phase, decided by `drain_verdict()` rather than here: a pure function a test can
        // ask a thousand times without a socket, and one definition of the bound (#106).
        if (drain_started_) {
            const int open = stats_.active_sessions.load(std::memory_order_relaxed);
            switch (drain_verdict(drain_started_at_, open, config_.drain_timeout_ms,
                                  std::chrono::steady_clock::now())) {
            case DrainVerdict::AllSessionsClosed:
                running_.store(false, std::memory_order_relaxed);
                break;
            case DrainVerdict::DeadlineReached:
                // Every reactor measures one deadline from one start, so several can reach it in
                // the same pass; the one that stops the server is the one that says so.
                if (running_.exchange(false, std::memory_order_relaxed)) {
                    OB_LOG_WARN("tcp_server",
                                "Drain deadline of %llu ms reached with %d session(s) still open - "
                                "closing them and exiting; raise --drain-timeout-ms, or set it to 0 "
                                "to wait indefinitely",
                                static_cast<unsigned long long>(config_.drain_timeout_ms), open);
                }
                break;
            case DrainVerdict::KeepWaiting:
                break;
            }
        }
    }

    // Shutdown: close every session this reactor still holds, and take back what it contributed
    // to the gauges it shares with the others.
    sessions_.close_all();
    publish_gauge("ob_subscriptions_active", 0, published_subscriptions_active_);
    publish_gauge("ob_subscription_queued_bytes", 0, published_subscription_queued_bytes_);
    publish_gauge("ob_session_pending_bytes", 0, published_session_pending_bytes_);
}
}  // namespace

void TcpServer::run() {
    // Wire up the dynamic read-only flag so failover transitions toggle it.
    engine_->set_read_only_flag(&read_only_);

    // Open the engine (replay WAL, start flush thread).
    engine_->open();

    // Set structured log level from config.
    auto parsed_level = StructuredLogger::parse_level(config_.log_level);
    if (parsed_level.has_value()) {
        StructuredLogger::instance().set_level(*parsed_level);
    }

    // Start MetricsServer if configured.
    if (config_.metrics_port > 0) {
        metrics_server_ = std::make_unique<MetricsServer>(config_.metrics_port, engine_->registry(),
                                                          config_.metrics_bind);
        metrics_server_->start();
    }

    // Initialize ShardCoordinator if shard mode is enabled.
    std::unique_ptr<ShardCoordinator> shard_coord;
    if (!config_.shard_id.empty()) {
        OB_LOG_INFO("tcp_server", "Shard mode: shard_id=%s", config_.shard_id.c_str());

        ShardCoordinatorConfig sc_config;
        sc_config.shard_id = config_.shard_id;
        sc_config.vnodes = config_.shard_vnodes;
        sc_config.coordinator.endpoints = config_.coordinator_endpoints;
        sc_config.coordinator.lease_ttl_seconds = config_.coordinator_lease_ttl;
        sc_config.coordinator.node_id = config_.node_id;
        sc_config.coordinator.cluster_prefix = "/ob/";

        shard_coord = std::make_unique<ShardCoordinator>(sc_config, *engine_);
        shard_coord->start();
    }

    // 1. Create non-blocking TCP socket.
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (listen_fd_ < 0) {
        throw std::runtime_error(std::string("socket() failed: ") + std::strerror(errno));
    }

    // 2. Set SO_REUSEADDR.
    int opt = 1;
    if (::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        throw std::runtime_error(std::string("setsockopt() failed: ") + std::strerror(errno));
    }

    // 3. Bind to 0.0.0.0:port.
    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(config_.port);

    if (::bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        throw std::runtime_error(std::string("bind() failed on port ")
                                 + std::to_string(config_.port) + ": " + std::strerror(errno));
    }

    // 4. Listen with backlog 128.
    if (::listen(listen_fd_, 128) < 0) {
        throw std::runtime_error(std::string("listen() failed: ") + std::strerror(errno));
    }

    // The line that says the server is up, logged *after* the bind and the listen have succeeded,
    // and through the logger so it reaches the operator's file when it happens rather than at exit.
    // The tool's banner says "starting" and cannot know this (#90).
    OB_LOG_INFO("tcp_server", "listening on port %u, version %s, data-dir: %s",
                static_cast<unsigned>(config_.port), std::string(version()).c_str(),
                config_.data_dir.c_str());

    ServerStats stats;
    std::vector<Reactor*> dealt_to;
    std::mutex            admin_mtx;
    ReactorShared shared{config_,    *engine_, tls_,  secrets_,          running_,
                         draining_,  read_only_, stats, shard_coord.get(), metrics_server_.get(),
                         &dealt_to, admin_mtx};

    // 7-8. Reactor 0 holds the listening socket and runs on this thread; the others have a thread
    // each. Every one is built before any of them runs, so the list reactor 0 deals from is complete
    // and never changes while it is read.
    int no_listener = -1;
    std::vector<std::unique_ptr<Reactor>> reactors;
    reactors.reserve(config_.io_threads);
    for (uint32_t i = 0; i < config_.io_threads; ++i) {
        reactors.push_back(std::make_unique<Reactor>(static_cast<int>(i), shared,
                                                     i == 0 ? listen_fd_ : no_listener));
        dealt_to.push_back(reactors.back().get());
    }
    OB_LOG_INFO("tcp_server", "%u io thread(s): connections are dealt to them in turn",
                config_.io_threads);

    running_.store(true, std::memory_order_relaxed);

    // A reactor that ends on an exception ends the server, as the single loop did: run() leaves
    // with the error once every other reactor has stopped. Left running, the others would go on
    // dealing connections to a loop that no longer serves them.
    std::exception_ptr failure;
    std::atomic<bool>  a_reactor_failed{false};
    {
        std::vector<std::thread> threads;
        threads.reserve(reactors.size() - 1);
        // Stops and joins what was started on every way out of this block - including a
        // std::thread constructor that throws - because a joinable std::thread destroyed by
        // unwinding terminates the process (#102, #112).
        struct StopAndJoin {
            std::atomic<bool>&        running;
            std::vector<std::thread>& threads;
            ~StopAndJoin() {
                running.store(false, std::memory_order_relaxed);
                for (auto& t : threads) {
                    if (t.joinable()) t.join();
                }
            }
        } stop_and_join{running_, threads};

        for (size_t i = 1; i < reactors.size(); ++i) {
            Reactor* r = reactors[i].get();
            // `std::thread(` spelled out rather than `emplace_back`, so the scan in
            // test_thread_boundaries.cpp sees this construction and checks its boundary (#112).
            threads.push_back(std::thread([this, r, &a_reactor_failed] {
                run_thread_body("tcp_server", "a client event loop", [&] {
                    try {
                        r->run_loop();
                    } catch (...) {
                        a_reactor_failed.store(true, std::memory_order_relaxed);
                        running_.store(false, std::memory_order_relaxed);
                        throw;   // the boundary logs it, with what it was
                    }
                });
            }));
            // Named so `top -H` and a per-thread CPU sample say which loop is which. The kernel
            // limit is fifteen characters, and "ob-io-63" fits.
            const std::string name = "ob-io-" + std::to_string(i);
            pthread_setname_np(threads.back().native_handle(), name.c_str());
        }
        pthread_setname_np(pthread_self(), "ob-io-0");

        try {
            reactors[0]->run_loop();
        } catch (...) {
            failure = std::current_exception();
        }
        // Reactor 0 returned, so the server is stopping: a finished drain, a deadline, a fatal
        // error, or another reactor's failure. The block's end stops and joins the others.
    }
    reactors.clear();   // closes every reactor's descriptors, and anything still handed off

    // listen_fd_ may already be closed by the reactor during drain.
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }

    // A fatal epoll error leaves the loop without ever seeing draining_, so the metrics server
    // would still be serving after run() returned. stop() is idempotent.
    if (metrics_server_) {
        metrics_server_->stop();
    }

    // Stop ShardCoordinator before closing engine.
    if (shard_coord) {
        shard_coord->stop();
        shard_coord.reset();
    }

    engine_->close();

    if (failure) std::rethrow_exception(failure);
    if (a_reactor_failed.load(std::memory_order_relaxed)) {
        throw std::runtime_error("a client event loop ended on an exception, so the server "
                                 "stopped; the log names it");
    }
}

int io_wait_ms(std::chrono::steady_clock::time_point last_event,
               uint64_t spin_us,
               int blocking_timeout_ms,
               std::chrono::steady_clock::time_point now) {
    if (spin_us == 0) return blocking_timeout_ms;      // the default, and what this always did
    const auto since = std::chrono::duration_cast<std::chrono::microseconds>(now - last_event);
    if (since.count() < 0) return 0;                   // clock went backwards; poll rather than sleep
    return static_cast<uint64_t>(since.count()) < spin_us ? 0 : blocking_timeout_ms;
}

DrainVerdict drain_verdict(std::chrono::steady_clock::time_point drain_started,
                           int active_sessions,
                           uint64_t drain_timeout_ms,
                           std::chrono::steady_clock::time_point now) {
    if (active_sessions <= 0) return DrainVerdict::AllSessionsClosed;
    if (drain_timeout_ms == 0) return DrainVerdict::KeepWaiting;   // asked for, not defaulted
    const auto waited =
        std::chrono::duration_cast<std::chrono::milliseconds>(now - drain_started).count();
    if (waited >= static_cast<long long>(drain_timeout_ms)) return DrainVerdict::DeadlineReached;
    return DrainVerdict::KeepWaiting;
}

void TcpServer::shutdown() {
    // Request only. This runs on the thread watching for SIGINT/SIGTERM, while run() is inside
    // its epoll loop on another — so everything this used to do here was a race, and
    // ThreadSanitizer said so seventeen times in one run of the integration suite (#80):
    //
    //   - `metrics_server_` was read here while main was still constructing it,
    //   - `listen_fd_` was read, closed and set to -1 here while run() was reading and closing
    //     the same field, which is a double close and, worse, an `epoll_ctl` on a descriptor
    //     number the kernel may already have handed to something else.
    //
    // The rule is the one pitfall 41 came from: the thread that owns a descriptor is the thread
    // that closes it. This one raises a flag; run() sees it within its 100 ms wait and does the
    // work itself.
    draining_.store(true, std::memory_order_release);
    OB_LOG_INFO("tcp_server", "Shutdown requested — the epoll loop will drain and close");
}

} // namespace ob
