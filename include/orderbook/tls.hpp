#pragma once

#include <memory>
#include <string>

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
