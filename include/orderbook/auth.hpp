#pragma once

#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace ob {

// ── Wire authentication (#30, part one) ───────────────────────────────────────
//
// One translation unit shared by all three network surfaces. It deliberately does not live in
// tcp_server.cpp: multi_master.cpp would then have to pull in the TCP server to authenticate a
// peer, and the two have nothing else in common.
//
// The scheme is challenge-response, not a bearer token. Until TLS lands (#30 part two) the wire
// carries no confidentiality, so a token captured by a passive observer would be replayable
// forever, while a response to a fresh nonce is not. That is the whole reason for the extra round
// trip, and it is paid once per connection.

/// Which network surface a challenge belongs to.
///
/// Part of the HMAC input rather than decoration. Replication and multi-master share a single
/// cluster secret, so without domain separation a response captured on one of those links is
/// replayable on the other - and the two grant different things. The client surface has its own
/// secret, so separation there is belt and braces; keeping all three in one enum is what makes the
/// load-bearing case impossible to forget.
enum class AuthSurface { Client, Replication, MultiMaster };

/// Stable wire label for a surface: "client", "replication", "mm".
std::string_view surface_label(AuthSurface s);

/// Which end of a connection computed a response.
///
/// **This is what stops a reflection attack, and without it the cluster links were bypassable with
/// no knowledge of the secret.** Both ends of a cluster link hold the same key, so if both compute
/// the same function of a nonce, an attacker can echo the acceptor's own challenge back as its
/// own, be handed the answer, and replay it:
///
///     primary  -> attacker : CHALLENGE n
///     attacker -> primary  : CHALLENGE n      (the primary's own nonce, reflected)
///     primary  -> attacker : AUTH   H(n)      (answering a challenge needs no authentication)
///     attacker -> primary  : AUTH   H(n)      (replaying what it was just given)
///
/// Binding *both* nonces does not help: with the nonce reflected, "mine then theirs" and "theirs
/// then mine" are the same pair. The two directions have to compute different values, so the role
/// goes into the MAC input.
///
/// `Initiator` is the side that opened the connection - the replica, the connecting peer, a client.
/// `Acceptor` is the side that accepted it. On the client surface only `Initiator` is ever used,
/// because the server never proves itself there; that is stated rather than left implicit.
enum class AuthRole { Initiator, Acceptor };

/// Stable wire label for a role: "initiator", "acceptor".
std::string_view role_label(AuthRole r);

/// Bytes of a challenge nonce, before hex encoding.
///
/// 32 from the system CSPRNG. A nonce an attacker can predict turns challenge-response back into a
/// bearer token, so the number matters less than the source.
inline constexpr size_t kAuthNonceBytes = 32;

/// Characters in a hex-encoded nonce or response. Both are SHA-256 sized.
inline constexpr size_t kAuthHexChars = 64;

/// Shortest secret the loader accepts.
///
/// 32 characters. Not a strength estimate - a passphrase of 32 characters is not 32 bytes of
/// entropy - but a floor that refuses the cases seen in the wild: a secret file holding a word, or
/// holding the string "changeme". `docs/operations.md` tells an operator to generate it with
/// `openssl rand -hex 32`, which is 64.
inline constexpr size_t kMinSecretChars = 32;

/// Longest identity the parser accepts, and the bound used when one reaches a log.
inline constexpr size_t kMaxIdentityChars = 64;

// ── Credential ────────────────────────────────────────────────────────────────

/// What an identity may run (#31). Each role takes every command of the one before it:
/// `Read` the queries and the node's own state, `Write` the writes and FLUSH as well, `Admin` the
/// commands that move a cluster or copy a node - FAILOVER, MIGRATE, ADOPT, BACKUP, SHARD_MAP and
/// SHARD_INFO. Which command is which is `role_allows()`, beside the command list.
enum class Role : uint8_t { Read, Write, Admin };

/// "read", "write" or "admin"; and back, nullopt for anything else.
std::string_view role_name(Role role);
std::optional<Role> role_from_name(std::string_view name);

/// An identity's permissions (#31), apart from its secret: a session holds these and never the
/// secret, for the reason `Session` gives.
///
/// The symbols and exchanges are patterns, each a name or a prefix ending in `*`. An empty list
/// reaches every one, and an identity whose lists are both empty is unrestricted. Without
/// --auth-acl-file every identity is `Admin` and unrestricted, which is what every identity was
/// before #31.
struct Permissions {
    Role role{Role::Admin};
    std::vector<std::string> symbols;
    std::vector<std::string> exchanges;

    bool unrestricted() const { return symbols.empty() && exchanges.empty(); }

    /// Whether `symbol` on `exchange` is inside both lists.
    bool reaches(std::string_view symbol, std::string_view exchange) const;
};

/// Refuse a permissions file (#31) that is not a regular file, that someone other than
/// `server_uid` or root owns, or that its group or others may write. Others may read it - it holds
/// no secret - but whoever can write it decides what every identity may do from the next start,
/// which is the rule sshd keeps for authorized_keys. Throws std::runtime_error naming the file
/// and the reason.
void check_permissions_file(const std::string& path, uid_t server_uid);

/// One named credential. The identity is what a human reads in a log line, and its permissions
/// are what it may run (#31).
struct Credential {
    std::string identity;
    std::string secret;
    Permissions permissions;
};

// ── SecretStore ───────────────────────────────────────────────────────────────

/// The parsed contents of a secret file.
///
/// Two forms, because the two surfaces need different things. The client form is a list of
/// `<identity> <secret>` lines, so a log line can name which client connected. The cluster form is
/// a single secret with no identity, because a node's identity is its `node_id`, which arrives in
/// the handshake that authentication precedes.
class SecretStore {
public:
    SecretStore() = default;

    /// Load client credentials: one or more `<identity> <secret>` lines.
    /// Throws std::runtime_error naming the file on every refusal. The message never contains
    /// secret material.
    static SecretStore load_client_file(const std::string& path);

    /// Load the cluster secret: exactly one non-empty line, the whole line being the secret.
    static SecretStore load_cluster_file(const std::string& path);

    /// Give each identity the permissions --auth-acl-file names for it (#31): one line per
    /// identity, `<identity> role=read|write|admin [symbols=A,B*] [exchanges=X,Y*]`.
    ///
    /// A file of its own rather than fields after the secret, because a secret is the rest of its
    /// line, spaces and all: a field after it could not be told from the secret's own text. Every
    /// identity of this store needs a line and every line an identity of this store, so with the
    /// file each identity's permissions are a decision someone wrote down. Throws
    /// std::runtime_error naming the file, the line and the field on every refusal.
    void apply_acl_file(const std::string& path);

    /// Look up by identity. nullptr means unknown - and the caller must answer an unknown identity
    /// exactly as it answers a wrong response, so that the wire does not report which names exist.
    const Credential* find(std::string_view identity) const;

    /// The single credential of a cluster store. Throws if the store is not in that form.
    const Credential& sole() const;

    size_t size() const { return credentials_.size(); }
    bool   empty() const { return credentials_.empty(); }

private:
    friend bool stores_share_a_secret(const SecretStore& a, const SecretStore& b);
    std::vector<Credential> credentials_;
};

/// True when any secret in `a` equals any secret in `b`.
///
/// Exists for one check: the cluster secret must not also be a client secret. If it were, a client
/// could present itself as a replica and stream the entire write-ahead log - client authentication
/// would grant node privileges, and nothing on either surface would look wrong.
///
/// Returns a bool and nothing else. It deliberately does not report *which* pair matched: that
/// message would name an identity beside the fact that its secret is the cluster secret, and a
/// refusal message is a thing operators paste into tickets.
bool stores_share_a_secret(const SecretStore& a, const SecretStore& b);

// ── Primitives ────────────────────────────────────────────────────────────────

/// 32 bytes from the system CSPRNG, hex-encoded into 64 characters.
/// Throws std::runtime_error if the CSPRNG fails - a predictable nonce is not a degraded mode.
std::string generate_nonce_hex();

/// HMAC-SHA256 of `data` under `key`, hex-encoded lower case.
///
/// Exposed so that the RFC 4231 test vectors can be run against it. `auth_response()` constructs an
/// input and calls this; without the split, the only thing a test could pin would be our own
/// construction, and "we call OpenSSL correctly" would be untested.
std::string hmac_sha256_hex(std::string_view key, std::string_view data);

/// HMAC-SHA256(secret, "ob-auth-v1\0<surface>\0<role>\0<identity>\0<nonce_hex>"), hex, lower case.
///
/// Every field is inside the MAC and separated by a NUL, so no two different tuples produce the
/// same input by concatenation. The version prefix exists so a future scheme cannot be mistaken for
/// this one; the surface label is the domain separation described on AuthSurface; the role is the
/// reflection defence described on AuthRole; and the identity is there so that a response for
/// identity A is not a valid response for identity B under a shared secret.
std::string auth_response(std::string_view secret,
                          AuthSurface      surface,
                          AuthRole         role,
                          std::string_view identity,
                          std::string_view nonce_hex);

/// Constant-time comparison of two hex responses.
///
/// A length mismatch returns false immediately, which leaks nothing: the length of a response is
/// fixed by the scheme and public.
bool responses_equal(std::string_view a, std::string_view b);

/// Bound and sanitise a peer-supplied string on its way to a log line.
///
/// Everything an unauthenticated peer sends that reaches a log goes through this. Without it the
/// claimed-identity field is log injection: a newline plus a plausible line, and the log says
/// whatever the peer wants it to say. Non-printable bytes become '.', and the result is truncated
/// with a visible marker so a bounded log line cannot be mistaken for the whole value.
std::string sanitise_for_log(std::string_view raw, size_t max_len = kMaxIdentityChars);

/// True when `s` is exactly kAuthHexChars lower-case hex digits.
bool is_auth_hex(std::string_view s);

} // namespace ob
