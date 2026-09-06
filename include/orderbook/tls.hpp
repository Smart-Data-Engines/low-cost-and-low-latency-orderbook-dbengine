#pragma once

#include <memory>
#include <string>
#include <vector>

// Forward declarations, so every translation unit that holds a Session does not pull in OpenSSL's
// headers. Session stores an SslPtr, and only tls.cpp and the loops that drive the handshake need
// to know what is behind it.
extern "C" {
struct ssl_ctx_st;
struct ssl_st;
}

namespace ob {

// ── TLS (#30 part three) ──────────────────────────────────────────────────────
//
// In-process OpenSSL, TLS 1.3 minimum, no kernel TLS. Every one of those three was decided from a
// measurement rather than from taste - `benchmarks/tls/` and
// `kiro-workspace/specs/wire-tls/requirements.md` §1. The short version: a sidecar pays the same
// record-layer cost plus a loopback hop, kTLS measured 1.08x and 1.15x *slower* on this path, and
// TLS 1.2 is the only version this OpenSSL gives a full kernel data path - which a public database
// engine should not be capped at in 2026.

/// What OpenSSL wants next, so the event loop arms the right thing.
///
/// Four combinations, not two, and the existing loop knows two. TLS 1.3 updates keys, so a *write*
/// can need readability and a *read* can need writability - and arming EPOLLOUT because the caller
/// wanted to write, when OpenSSL is waiting to read, spins the loop on a writable socket
/// (pitfall 5).
enum class IoWant { Nothing, Read, Write };

/// Owns an `SSL_CTX`. One per process per surface, created at startup.
///
/// At startup and not on first connection, deliberately: a certificate that does not load, or a key
/// that does not match it, must stop the start rather than fail every handshake with a message the
/// operator reads as a client problem.
class TlsContext {
public:
    /// Load a server context, or throw std::runtime_error naming the file.
    ///
    /// Refusals, each fatal to the process: either path unreadable or not a regular file; the key
    /// readable beyond its owner (`mode & 0077`), reported with the mode it found, the same rule and
    /// the same message shape as the secret files of part one; a key that does not match the
    /// certificate (`SSL_CTX_check_private_key`).
    static TlsContext server(const std::string& cert_file, const std::string& key_file);

    /// A client context that verifies the peer. `ca_file` empty means the system trust store.
    static TlsContext client(const std::string& ca_file, bool verify);

    // ── Node links (#30 part three, series D) ─────────────────────────────────
    //
    // Two shapes rather than one, because the two ends of a node link verify different things -
    // and one factory with a `server_side` flag would hide that difference behind a bool.
    //
    // What both do that `server()` and `client()` do not: **require a trust anchor and verify the
    // peer, with no way to turn either off.** A node link that encrypts without authenticating its
    // peer leaves the relay of `SECURITY.md` open - two ends that both believe they are talking to
    // the cluster, with an attacker between them - and looks exactly like a link that does not. So
    // the absent CA file is a refusal at startup rather than a weaker mode at runtime.
    //
    // mTLS is therefore not a separate switch. On a node link it is what TLS *is*, and it costs no
    // extra configuration: every node already has a certificate and a key for its own listener.

    /// The accepting end of a node link: presents our certificate and **requires** one from the
    /// peer, chaining to `ca_file`.
    ///
    /// `peer_names` is the allowlist an accepted certificate must cover. Empty means "any identity
    /// this CA signed is a cluster member", which is true when the CA signs only the cluster and
    /// false for a corporate CA - so it is announced in the startup log rather than left to a
    /// document. The list lives here, on the surface, and is handed to each channel: the check has
    /// to happen inside the handshake (see TlsChannel::continue_handshake) and a per-connection copy
    /// of a list that never changes would be waste in the accept path.
    static TlsContext node_server(const std::string& cert_file, const std::string& key_file,
                                  const std::string& ca_file,
                                  std::vector<std::string> peer_names = {});

    /// The dialling end of a node link: presents our certificate and verifies the peer's.
    ///
    /// Two checks here, answering different questions. `tls_expect_host()` - the caller's, on the
    /// address it dialled - asks "is this the host I dialled"; `peer_names` asks "is this host
    /// allowed to be a cluster member". The second is the same list the accepting end uses, and it
    /// has to be here too: the mesh is symmetric, so a list that constrained only inbound
    /// connections would let a refused peer join on the connection *we* opened to it.
    static TlsContext node_client(const std::string& cert_file, const std::string& key_file,
                                  const std::string& ca_file,
                                  std::vector<std::string> peer_names = {});

    /// Open a channel on `fd`. `peer_label` only ever reaches log lines.
    std::shared_ptr<class TlsChannel> open_channel(int fd, bool server_side,
                                                   std::string peer_label) const;

    ~TlsContext();
    TlsContext(TlsContext&&) noexcept;
    TlsContext& operator=(TlsContext&&) noexcept;
    TlsContext(const TlsContext&)            = delete;
    TlsContext& operator=(const TlsContext&) = delete;

    /// A new `SSL` for one connection, with the descriptor attached. Never null.
    std::shared_ptr<ssl_st> wrap(int fd, bool server_side) const;

    ssl_ctx_st* raw() const { return ctx_; }

private:
    explicit TlsContext(ssl_ctx_st* ctx) : ctx_(ctx) {}
    ssl_ctx_st* ctx_{nullptr};
    /// Shared with every channel this context opens, so the list is not copied per connection.
    /// Null on a context that accepts nothing (`client()`, `node_client()`).
    std::shared_ptr<const std::vector<std::string>> peer_names_;
};

/// Bind a client `SSL` to the name it expects, before the handshake. False on a name OpenSSL
/// refuses to parse.
///
/// Two things happen here and they are not the same thing. `SSL_CTX_set_verify(SSL_VERIFY_PEER)`
/// checks that the certificate chains to a trusted CA; it does **not** check that the certificate
/// belongs to the host you dialled. Without this call, *any* certificate the CA signed authenticates
/// *any* host - so with a private CA that signs the cluster, node B's certificate is accepted for
/// node A, and the verification reads as done. That is the man in the middle TLS is here to stop.
///
/// An IP literal takes the other branch in both halves, and getting either wrong looks like working
/// code. RFC 6066 §3 forbids a literal address in `server_name`, so no SNI is sent for one; and the
/// chain check matches an address against the certificate's `iPAddress` SAN, not its `dNSName`, so
/// `SSL_set1_host("127.0.0.1")` hunts for a DNS entry spelled that way and fails against a correct
/// certificate.
///
/// Inert under `TlsContext::client(ca, verify=false)`: OpenSSL still computes the verification
/// result and nothing acts on it. Callers that do not verify should not call this, so the absence of
/// a name check is visible at the call site rather than buried in a flag.
bool tls_expect_host(ssl_st* ssl, const std::string& host);

/// The peer certificate's common name, bounded and stripped of non-printables.
///
/// Empty when the peer presented no certificate. Goes through `sanitise_for_log()` because a CN is
/// a string the peer chose: without that, a newline plus a plausible line makes the log say whatever
/// the peer wants, which is the rule part one established for every peer-supplied value that reaches
/// a log.
///
/// The CN and not a SAN, and the difference is worth stating because it reads as an inconsistency:
/// **verification** matches SANs (`tls_peer_name_allowed`, `tls_expect_host`), because that is the
/// modern rule and CN is deprecated for it; **identity** is the CN, because that is the handle
/// operators and future ACLs (#31) use. The log line prints both the CN and the allowlist entry that
/// matched, so the two can never be confused for each other.
std::string tls_peer_identity(ssl_st* ssl);

/// True when the peer certificate covers one of `names`. An empty list accepts any verified peer.
///
/// `*matched` receives the entry that matched, or a phrase naming the empty list. Each entry takes
/// the same two branches, in the same order and for the same reason as `tls_expect_host`: an address
/// is matched against `iPAddress` and a name against `dNSName`, and trying the address parse first
/// makes its failure the detection rather than a second parser to keep in step with OpenSSL's.
bool tls_peer_name_allowed(ssl_st* ssl, const std::vector<std::string>& names,
                           std::string* matched);

// ── TlsChannel — one connection's TLS state, for the event-loop paths ─────────
//
// Series C put TLS inside `Session`, which already owned the byte path in both directions. The node
// links have no equivalent: two connection records (`ReplicaInfo`, `PeerConnection`) and two loops,
// each with its own framing. Repeating three fields and a handshake state machine in both would mean
// **two implementations of the four `IoWant` combinations**, and those are the only hard thing here.
//
// Held by `shared_ptr` on the records, and that is not taste. `replicas_` is a
// `std::vector<ReplicaInfo>` whose `push_back` moves its elements; `peers_` is a map in which a
// record **changes key** after the handshake, by erase and move; and `replica_states()` /
// `peer_states()` return *copies* for `STATUS`. A by-value member holding any pointer into itself
// would dangle from the first reallocation - a defect that shows up as corrupt bytes on the sixth
// replica. One heap object with a reference count survives every one of those moves, and a copy made
// for `STATUS` reports on the connection it is actually about.
class TlsChannel {
public:
    /// What one attempt produced. Four outcomes, not three: `Again` and `Error` are separate for the
    /// reason `Session::IoResult` documents - a helper that answers "come back later" and "this
    /// connection is finished" with one value makes every failure undiagnosable.
    enum class Io { Data, Again, Closed, Error };

    TlsChannel(std::shared_ptr<ssl_st> ssl, int fd, bool server_side,
               std::string peer_label,
               std::shared_ptr<const std::vector<std::string>> allowed_peer_names);

    TlsChannel(const TlsChannel&)            = delete;
    TlsChannel& operator=(const TlsChannel&) = delete;

    /// True until the handshake completes. While it is true, callers must not write application
    /// bytes: queue them instead, and they go out on the first drain afterwards.
    bool handshaking() const { return handshaking_; }

    /// What OpenSSL wants next. The loop arms this rather than the event matching the operation it
    /// just tried - which is the whole point of the type.
    IoWant io_want() const { return want_; }

    /// One handshake step. False is fatal and already logged.
    ///
    /// On success this also decides **who the peer is**: it reads the certificate identity and, on
    /// the accepting end, checks the allowlist - and a name outside it fails the handshake here.
    /// Deliberately not left to the caller: there are four call sites across two loops, the check
    /// would run *after* OpenSSL has already buffered the peer's decrypted bytes, and one forgotten
    /// `if` would then mean a peer whose certificate we rejected feeding frames to the parser.
    /// Refusing inside the handshake makes that impossible rather than merely absent - the same move
    /// that put part one's client gate before the `switch` instead of in every `case`.
    bool continue_handshake();

    /// Drive the handshake to completion on a **blocking** socket. False on failure or timeout,
    /// with `*why` set.
    ///
    /// The synchronous twin of `continue_handshake()`: same identity extraction, same allowlist
    /// refusal, different waiting. They share `finish_handshake()` rather than each doing their own,
    /// because a node link with the peer identity checked on one end and not the other is the
    /// asymmetry this whole series exists to remove.
    bool blocking_handshake(std::string* why);

    Io read(void* buf, size_t len, size_t& out_n);

    /// Write up to `len` bytes; `out_n` says how many were taken.
    ///
    /// `Again` with `io_want() == Read` is the combination that looks like a wedged peer if the
    /// caller arms writability: TLS 1.3 key updates make a *write* need the socket to become
    /// readable, and arming EPOLLOUT then spins on an already-writable socket (pitfall 5).
    Io write(const void* buf, size_t len, size_t& out_n);

    /// Best-effort `close_notify`, so the peer sees a clean close rather than a truncation.
    void shutdown();

    /// Who the peer proved itself to be, from its certificate. Empty until the handshake completes.
    const std::string& identity() const { return identity_; }

    ssl_st* raw() const { return ssl_.get(); }

private:
    std::shared_ptr<ssl_st> ssl_;
    int         fd_;
    bool        server_side_;
    std::string peer_label_;
    std::shared_ptr<const std::vector<std::string>> allowed_peer_names_;
    bool        handshaking_{true};
    IoWant      want_{IoWant::Read};
    std::string identity_;

    /// Map an `SSL_get_error` code onto an `Io`, setting `want_`. One place, so the four
    /// combinations cannot disagree between `read()` and `write()`.
    Io classify(int ssl_error, const char* op);

    /// Everything that happens once the handshake has succeeded, shared by both drivers: read the
    /// peer identity, apply the allowlist on the accepting end, clear the handshake state.
    bool finish_handshake();
};

// ── Blocking stream helpers, for the synchronous clients ──────────────────────
//
// Deliberately apart from the event-loop path. There the four `IoWant` combinations are the whole
// problem; here the socket is blocking with `SO_RCVTIMEO`, and OpenSSL reports that timeout as
// `WANT_READ` - because the socket BIO maps `EAGAIN` to "should retry" whether the descriptor is
// non-blocking or merely impatient. So a helper that treated a want as "retry now" would spin on a
// dead peer forever, one timeout per iteration, and the caller would never see an error.
//
// All three set `*why` on failure and leave it untouched otherwise, and all three drain the OpenSSL
// error queue before the call so the message names this failure and not an earlier one.

/// Drive the handshake to completion. 1 on success, 0 if the peer closed, -1 on error or timeout.
int tls_blocking_handshake(ssl_st* ssl, std::string* why);

/// Write exactly `len` bytes. 1 on success, 0 if the peer closed, -1 on error or timeout.
int tls_blocking_write_all(ssl_st* ssl, const char* buf, size_t len, std::string* why);

/// Read up to `len` bytes. >0 bytes read, 0 on close (`why` says whether it was clean), -1 on error.
int tls_blocking_read(ssl_st* ssl, char* buf, size_t len, std::string* why);

/// Best-effort `close_notify`, so the peer's read returns a clean close rather than a truncation.
void tls_blocking_shutdown(ssl_st* ssl);

/// The last OpenSSL error for this thread, as one line. Empty when the queue is empty.
///
/// Drains the queue, because a stale entry read on the next failure names the wrong cause - and an
/// error queue that is never drained turns every later diagnosis into the first one.
std::string tls_last_error();

/// Human-readable name for an `SSL_get_error` code, for logs.
const char* tls_error_name(int ssl_error);

} // namespace ob
