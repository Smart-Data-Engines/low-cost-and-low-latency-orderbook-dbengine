#include "orderbook/tls.hpp"

#include "orderbook/auth.hpp"
#include "orderbook/logger.hpp"

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <sys/stat.h>

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstdio>
#include <cstring>
#include <stdexcept>

namespace ob {

namespace {

/// Refuse everything about a file that is not its contents, before reading the contents.
///
/// `require_owner_only` is the difference between the certificate and the key: a certificate is
/// public by design, a private key readable by every local process is not a private key. Same rule
/// and same message shape as the secret files of #30 part one.
void check_file_or_throw(const std::string& path, const char* what, bool require_owner_only) {
    struct stat st{};
    if (::stat(path.c_str(), &st) != 0) {
        throw std::runtime_error(std::string(what) + " '" + path + "' cannot be read: " +
                                 std::strerror(errno));
    }
    if (!S_ISREG(st.st_mode)) {
        throw std::runtime_error(std::string(what) + " '" + path + "' is not a regular file");
    }
    if (st.st_size == 0) {
        throw std::runtime_error(std::string(what) + " '" + path + "' is empty");
    }
    if (require_owner_only && (st.st_mode & 0077) != 0) {
        char mode[16];
        std::snprintf(mode, sizeof(mode), "%04o", static_cast<unsigned>(st.st_mode & 07777));
        throw std::runtime_error(std::string(what) + " '" + path +
                                 "' is readable beyond its owner (mode " + mode +
                                 "); chmod 600 it");
    }
}

/// Two modes, and they buy different things - which took four attempts to establish, so it is
/// written down precisely rather than as "the send path needs these".
///
/// **`ACCEPT_MOVING_WRITE_BUFFER` is required for correctness.** `Session::flush_output()` does
/// `send_buf_.erase(0, n)`, which moves the *pending* bytes to a different address, and OpenSSL
/// refuses a retry presenting a different address with
/// `error:0A00007F:SSL routines::bad write retry`. Measured through a real Session, and the window
/// is narrower than it looks: `sent_total` only advances on a fully accepted `SSL_write`, so with a
/// socket send buffer below one TLS record (16 kB) every WANT_WRITE arrives with `sent_total == 0`,
/// the erase is skipped, and the retry is at the same address. The hazard needs a buffer that
/// accepts at least one whole record and then blocks - which an operator's machine may well have,
/// so the mode is not optional.
///
/// **`ENABLE_PARTIAL_WRITE` is not about correctness.** Without it `SSL_write` is all-or-nothing
/// per call, and the bytes still arrive - OpenSSL resumes its own pending write. What is lost is
/// the caller's view: `sent_total` stays 0 for the whole drain, so `send_buf_` never shrinks and
/// `pending_output_bytes()` stays pinned at the full response. That is the number
/// `ob_pending_bytes` publishes and the number an operator reads as "this client is not draining",
/// so a gauge that cannot move is a gauge that says the wrong thing.
///
/// Both mutation-checked in `TlsSession.ALargeResponseSurvivesPartialWritesAndTheEraseThatFollowsThem`,
/// and they fail through different assertions - the error for the first, the pinned gauge for the
/// second.
void set_engine_modes(SSL_CTX* ctx) {
    SSL_CTX_set_mode(ctx, SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
}

/// Owns an `SSL_CTX` until its factory succeeds.
///
/// Not a seventh hand-written `SSL_CTX_free`. There were six, one per throw path, and every one of
/// them was correct - the leak came from the single path that throws through a *helper*
/// (`check_file_or_throw` on the CA bundle), where no free was written because no `throw` was
/// written either. Found by LeakSanitizer on a required check, not by review.
///
/// It was a real leak and not a test artefact: `OrderbookClient::ensure_tls_context()` turns that
/// exception into an error and leaves `tls_ctx_` null, so a pool retrying `connect()` against a
/// mistyped CA path leaked a context **per attempt, for ever** - 1616 bytes plus about 3 kB
/// indirect, measured, once per health check. Pitfall 32: a retry loop exposes leaks that one-shot
/// code hides.
using CtxGuard = std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)>;

/// Load our own certificate chain and private key, or throw naming the file.
///
/// Shared by all four factories. On a node link this node presents a certificate as a client too,
/// so "load the certificate" stopped being a server-only step - and one copy per factory would be
/// four places for the same message to drift.
void load_own_certificate(SSL_CTX* ctx, const std::string& cert_file, const std::string& key_file) {
    if (SSL_CTX_use_certificate_chain_file(ctx, cert_file.c_str()) != 1) {
        const std::string why = tls_last_error();
        throw std::runtime_error("TLS certificate '" + cert_file + "' rejected: " + why);
    }
    if (SSL_CTX_use_PrivateKey_file(ctx, key_file.c_str(), SSL_FILETYPE_PEM) != 1) {
        const std::string why = tls_last_error();
        throw std::runtime_error("TLS private key '" + key_file + "' rejected: " + why);
    }
    // Unreachable for a mismatched pair *in this order*, and kept anyway.
    //
    // `SSL_CTX_use_PrivateKey_file` above compares the key against the certificate already loaded,
    // so it refuses a mismatch first - measured, the message is
    // `x509 certificate routines::key values mismatch`. This call becomes the only guard the moment
    // somebody loads the key before the certificate, which is a one-line edit and a silent loss of
    // the check. Cheap enough to keep; named as redundant so nobody reads its absence of coverage
    // as a gap (pitfall 45).
    if (SSL_CTX_check_private_key(ctx) != 1) {
        const std::string why = tls_last_error();
        throw std::runtime_error("TLS private key '" + key_file + "' does not match certificate '" +
                                 cert_file + "': " + why);
    }
}

/// Load the trust anchor peers are verified against, or throw naming the file.
void load_trust_anchor(SSL_CTX* ctx, const std::string& ca_file) {
    check_file_or_throw(ca_file, "TLS CA bundle", /*require_owner_only=*/false);
    if (SSL_CTX_load_verify_locations(ctx, ca_file.c_str(), nullptr) != 1) {
        const std::string why = tls_last_error();
        throw std::runtime_error("TLS CA bundle '" + ca_file + "' rejected: " + why);
    }
}

/// TLS 1.3 floor plus the two write modes the queue shape needs. Every factory calls it.
void set_common_options(SSL_CTX* ctx) {
    // TLS 1.3 only. Not a default to be overridden by a flag: the version floor is the one setting
    // where "configurable" means "misconfigurable", and 1.2 exists here only because kernel TLS
    // wants it (requirements §1.3).
    if (SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION) != 1) {
        throw std::runtime_error("TLS: cannot require TLS 1.3: " + tls_last_error());
    }
    set_engine_modes(ctx);
}

} // namespace

TlsContext TlsContext::server(const std::string& cert_file, const std::string& key_file) {
    check_file_or_throw(cert_file, "TLS certificate", /*require_owner_only=*/false);
    check_file_or_throw(key_file,  "TLS private key", /*require_owner_only=*/true);

    CtxGuard guard(SSL_CTX_new(TLS_server_method()), &SSL_CTX_free);
    if (!guard) {
        throw std::runtime_error("TLS: SSL_CTX_new failed: " + tls_last_error());
    }
    SSL_CTX* const ctx = guard.get();
    set_common_options(ctx);
    load_own_certificate(ctx, cert_file, key_file);

    OB_LOG_INFO("tls", "server context ready: cert=%s key=%s min=TLSv1.3",
                cert_file.c_str(), key_file.c_str());
    return TlsContext(guard.release());
}

TlsContext TlsContext::client(const std::string& ca_file, bool verify) {
    CtxGuard guard(SSL_CTX_new(TLS_client_method()), &SSL_CTX_free);
    if (!guard) {
        throw std::runtime_error("TLS: SSL_CTX_new failed: " + tls_last_error());
    }
    SSL_CTX* const ctx = guard.get();
    set_common_options(ctx);

    if (verify) {
        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
        if (!ca_file.empty()) {
            load_trust_anchor(ctx, ca_file);
        } else if (SSL_CTX_set_default_verify_paths(ctx) != 1) {
            const std::string why = tls_last_error();
            throw std::runtime_error("TLS: no CA bundle given and the system trust store would not "
                                     "load: " + why);
        }
    } else {
        // Named, not defaulted. A client that does not verify has confidentiality against a passive
        // observer and nothing against a man in the middle - which is precisely the half part two
        // could not have, and the reason TLS is here at all.
        SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
        OB_LOG_WARN("tls", "certificate verification disabled - this protects against a passive "
                           "observer and not against a man in the middle");
    }
    return TlsContext(guard.release());
}

TlsContext TlsContext::node_server(const std::string& cert_file, const std::string& key_file,
                                   const std::string& ca_file,
                                   std::vector<std::string> peer_names) {
    check_file_or_throw(cert_file, "TLS certificate", /*require_owner_only=*/false);
    check_file_or_throw(key_file,  "TLS private key", /*require_owner_only=*/true);
    if (ca_file.empty()) {
        // Not a defaultable option, which is why it is a throw rather than a fallback to the system
        // trust store. A node link whose accepting end trusts whatever the distribution ships trusts
        // every public CA on earth to introduce a replica.
        throw std::runtime_error("TLS: a node link needs a trust anchor (--tls-ca-file); without "
                                 "one it would encrypt without authenticating the peer, which "
                                 "leaves the relay described in SECURITY.md open");
    }

    CtxGuard guard(SSL_CTX_new(TLS_server_method()), &SSL_CTX_free);
    if (!guard) {
        throw std::runtime_error("TLS: SSL_CTX_new failed: " + tls_last_error());
    }
    SSL_CTX* const ctx = guard.get();
    set_common_options(ctx);
    load_own_certificate(ctx, cert_file, key_file);
    load_trust_anchor(ctx, ca_file);

    // FAIL_IF_NO_PEER_CERT is the half that makes this mutual. Without it a peer that presents
    // nothing completes the handshake and the link is encrypted and anonymous - which is the
    // configuration that looks like protection and is not.
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);

    auto ctx_out = TlsContext(guard.release());
    ctx_out.peer_names_ =
        std::make_shared<const std::vector<std::string>>(std::move(peer_names));

    if (ctx_out.peer_names_->empty()) {
        // Said out loud at startup, not left to a document. This is the weaker of the two modes and
        // the one an operator ends up in by not passing a flag, so the only place it can be noticed
        // is here. Part one paid for the opposite mistake - a line claiming a guarantee nothing
        // enforced (pitfall 112).
        OB_LOG_INFO("tls", "node-link context ready: cert=%s ca=%s - any identity this CA signed is "
                           "accepted as a cluster member (no --tls-peer-names given), which is true "
                           "only if this CA signs nothing but this cluster",
                    cert_file.c_str(), ca_file.c_str());
    } else {
        std::string joined;
        for (const auto& n : *ctx_out.peer_names_) {
            if (!joined.empty()) joined += ",";
            joined += n;
        }
        OB_LOG_INFO("tls", "node-link context ready: cert=%s ca=%s peer-names=%s",
                    cert_file.c_str(), ca_file.c_str(), joined.c_str());
    }
    return ctx_out;
}

TlsContext TlsContext::node_client(const std::string& cert_file, const std::string& key_file,
                                   const std::string& ca_file,
                                   std::vector<std::string> peer_names) {
    check_file_or_throw(cert_file, "TLS certificate", /*require_owner_only=*/false);
    check_file_or_throw(key_file,  "TLS private key", /*require_owner_only=*/true);
    if (ca_file.empty()) {
        throw std::runtime_error("TLS: a node link needs a trust anchor (--tls-ca-file); without "
                                 "one it would encrypt without authenticating the peer, which "
                                 "leaves the relay described in SECURITY.md open");
    }

    CtxGuard guard(SSL_CTX_new(TLS_client_method()), &SSL_CTX_free);
    if (!guard) {
        throw std::runtime_error("TLS: SSL_CTX_new failed: " + tls_last_error());
    }
    SSL_CTX* const ctx = guard.get();
    set_common_options(ctx);
    load_own_certificate(ctx, cert_file, key_file);
    load_trust_anchor(ctx, ca_file);
    // No `verify` parameter, and no way to reach SSL_VERIFY_NONE from here. The name check is the
    // caller's, through tls_expect_host() on the address it dialled.
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);

    OB_LOG_INFO("tls", "node-link client context ready: cert=%s ca=%s min=TLSv1.3",
                cert_file.c_str(), ca_file.c_str());
    auto ctx_out = TlsContext(guard.release());
    // Carried here as well as on the server context: the allowlist applies in both directions (see
    // TlsChannel::finish_handshake), so a dialling end with an empty list would accept an identity
    // the accepting end refuses.
    ctx_out.peer_names_ =
        std::make_shared<const std::vector<std::string>>(std::move(peer_names));
    return ctx_out;
}

std::shared_ptr<TlsChannel> TlsContext::open_channel(int fd, bool server_side,
                                                     std::string peer_label) const {
    return std::make_shared<TlsChannel>(wrap(fd, server_side), fd, server_side,
                                        std::move(peer_label), peer_names_);
}

TlsContext::~TlsContext() {
    if (ctx_ != nullptr) SSL_CTX_free(ctx_);
}

TlsContext::TlsContext(TlsContext&& other) noexcept
    : ctx_(other.ctx_), peer_names_(std::move(other.peer_names_)) {
    other.ctx_ = nullptr;
}

TlsContext& TlsContext::operator=(TlsContext&& other) noexcept {
    if (this != &other) {
        if (ctx_ != nullptr) SSL_CTX_free(ctx_);
        ctx_        = other.ctx_;
        peer_names_ = std::move(other.peer_names_);
        other.ctx_  = nullptr;
    }
    return *this;
}

std::shared_ptr<ssl_st> TlsContext::wrap(int fd, bool server_side) const {
    SSL* ssl = SSL_new(ctx_);
    if (ssl == nullptr) {
        throw std::runtime_error("TLS: SSL_new failed: " + tls_last_error());
    }
    if (SSL_set_fd(ssl, fd) != 1) {
        const std::string why = tls_last_error();
        SSL_free(ssl);
        throw std::runtime_error("TLS: SSL_set_fd failed: " + why);
    }
    if (server_side) SSL_set_accept_state(ssl);
    else             SSL_set_connect_state(ssl);
    return std::shared_ptr<ssl_st>(ssl, [](SSL* s) { if (s) SSL_free(s); });
}

bool tls_expect_host(ssl_st* ssl, const std::string& host) {
    X509_VERIFY_PARAM* param = SSL_get0_param(ssl);

    // Try the address form first, because it is the one that tells them apart: `set1_ip_asc`
    // parses a literal and answers 0 for anything else, so the failure *is* the detection. A
    // hand-rolled "does this look like an IP" test would be a second parser to keep in step with
    // OpenSSL's.
    if (X509_VERIFY_PARAM_set1_ip_asc(param, host.c_str()) == 1) {
        // No SNI: RFC 6066 §3 says server_name carries host names, not literal addresses, and a
        // server is entitled to abort on one. Verification is against the iPAddress SAN.
        OB_LOG_DEBUG("tls", "client will require the certificate to cover address %s",
                     host.c_str());
        return true;
    }
    // The failed parse left entries on this thread's error queue, and a queue nobody empties makes
    // every later diagnosis read as this one (the reason tls_last_error() drains).
    ERR_clear_error();

    if (X509_VERIFY_PARAM_set1_host(param, host.c_str(), 0) != 1) {
        OB_LOG_ERROR("tls", "cannot require host '%s': %s", host.c_str(),
                     tls_last_error().c_str());
        return false;
    }
    // SNI, so a server holding several certificates picks the right one. Separate from the check
    // above and not a substitute for it: SNI is what we ask for, the verification parameter is what
    // we accept.
    if (SSL_set_tlsext_host_name(ssl, host.c_str()) != 1) {
        OB_LOG_ERROR("tls", "cannot set SNI for '%s': %s", host.c_str(),
                     tls_last_error().c_str());
        return false;
    }
    OB_LOG_DEBUG("tls", "client will require the certificate to cover name %s", host.c_str());
    return true;
}

namespace {

/// The peer's certificate, or null. Owned by the caller (`X509_free`).
X509* peer_certificate(ssl_st* ssl) {
    return SSL_get1_peer_certificate(ssl);
}

} // namespace

std::string tls_peer_identity(ssl_st* ssl) {
    X509* cert = peer_certificate(ssl);
    if (cert == nullptr) return {};
    char cn[256] = {0};
    const int n = X509_NAME_get_text_by_NID(X509_get_subject_name(cert), NID_commonName,
                                            cn, sizeof(cn));
    X509_free(cert);
    if (n <= 0) {
        // A certificate with no CN is legal and increasingly normal, so this is a value rather than
        // a failure. It is also the string an operator sees, so it says which of the two it is.
        ERR_clear_error();
        return "(no common name)";
    }
    return sanitise_for_log(std::string(cn, static_cast<size_t>(n)));
}

bool tls_peer_name_allowed(ssl_st* ssl, const std::vector<std::string>& names,
                           std::string* matched) {
    if (names.empty()) {
        if (matched != nullptr) *matched = "(any name this CA signed)";
        return true;
    }
    X509* cert = peer_certificate(ssl);
    if (cert == nullptr) {
        // Unreachable behind FAIL_IF_NO_PEER_CERT and kept, because this function is also the one
        // a future caller would reach for on a surface that does not require a certificate.
        if (matched != nullptr) *matched = "(no certificate)";
        return false;
    }
    for (const std::string& name : names) {
        // Address first, for the reason tls_expect_host() gives: the parse failing *is* the
        // detection, and the alternative is a second address parser to keep in step with OpenSSL's.
        if (X509_check_ip_asc(cert, name.c_str(), 0) == 1) {
            if (matched != nullptr) *matched = name;
            X509_free(cert);
            return true;
        }
        ERR_clear_error();
        if (X509_check_host(cert, name.c_str(), name.size(), 0, nullptr) == 1) {
            if (matched != nullptr) *matched = name;
            X509_free(cert);
            return true;
        }
        ERR_clear_error();
    }
    X509_free(cert);
    if (matched != nullptr) matched->clear();
    return false;
}

// ── TlsChannel ───────────────────────────────────────────────────────────────

TlsChannel::TlsChannel(std::shared_ptr<ssl_st> ssl, int fd, bool server_side,
                       std::string peer_label,
                       std::shared_ptr<const std::vector<std::string>> allowed_peer_names)
    : ssl_(std::move(ssl))
    , fd_(fd)
    , server_side_(server_side)
    , peer_label_(std::move(peer_label))
    , allowed_peer_names_(std::move(allowed_peer_names)) {}

bool TlsChannel::finish_handshake() {
    identity_ = tls_peer_identity(ssl_.get());

    // The allowlist applies **in both directions**, and getting that wrong made the flag mean less
    // than its documentation said.
    //
    // The first version checked it only on the accepting end, on the reasoning that the dialling end
    // already pins the name it dialled. In a symmetric mesh that is not enough: every pair has two
    // possible connections, so node A refused the connection *from* an unlisted node B and then
    // dialled B itself - where B's certificate covers the address A dialled, the name check passes,
    // and B is in the mesh. Measured: with `--tls-peer-names node-0,node-1` on a three-node mesh,
    // node-0 ended up connected to **two** peers.
    //
    // So the two checks answer different questions and both are needed: `tls_expect_host()` asks
    // "is this the host I dialled", the allowlist asks "is this host allowed to be a cluster
    // member". A list that constrains only inbound connections constrains nothing an attacker who
    // can get itself into the peer registry has to care about.
    static const std::vector<std::string> kNoNames;
    const std::vector<std::string>& names = allowed_peer_names_ ? *allowed_peer_names_ : kNoNames;
    std::string matched;
    if (!tls_peer_name_allowed(ssl_.get(), names, &matched)) {
        OB_LOG_ERROR("tls",
                     "peer %s (fd=%d) presented a certificate signed by a trusted CA whose "
                     "identity '%s' is not in --tls-peer-names - refusing the connection",
                     peer_label_.c_str(), fd_, identity_.c_str());
        return false;
    }

    handshaking_ = false;
    want_        = IoWant::Read;
    OB_LOG_INFO("tls",
                "handshake complete (%s): peer=%s fd=%d version=%s cipher=%s cn=%s matched=%s",
                server_side_ ? "accepted" : "dialled", peer_label_.c_str(), fd_,
                SSL_get_version(ssl_.get()), SSL_get_cipher(ssl_.get()), identity_.c_str(),
                matched.c_str());
    return true;
}

bool TlsChannel::continue_handshake() {
    if (!handshaking_) return true;

    ERR_clear_error();
    // SSL_do_handshake rather than SSL_accept or SSL_connect: TlsContext::wrap() has already set
    // the state, and one function for both roles means one state machine for a mesh in which the
    // same class is the acceptor on one connection and the dialler on the next.
    const int rc = SSL_do_handshake(ssl_.get());
    if (rc == 1) return finish_handshake();

    const int err = SSL_get_error(ssl_.get(), rc);
    if (err == SSL_ERROR_WANT_READ)  { want_ = IoWant::Read;  return true; }
    if (err == SSL_ERROR_WANT_WRITE) { want_ = IoWant::Write; return true; }
    OB_LOG_WARN("tls", "handshake failed: peer=%s fd=%d %s: %s", peer_label_.c_str(), fd_,
                tls_error_name(err), tls_last_error().c_str());
    return false;
}

bool TlsChannel::blocking_handshake(std::string* why) {
    if (!handshaking_) return true;
    if (tls_blocking_handshake(ssl_.get(), why) != 1) return false;
    if (!finish_handshake()) {
        if (why != nullptr) *why = "peer certificate identity is not in --tls-peer-names";
        return false;
    }
    return true;
}

TlsChannel::Io TlsChannel::classify(int ssl_error, const char* op) {
    switch (ssl_error) {
    case SSL_ERROR_WANT_READ:
        want_ = IoWant::Read;
        return Io::Again;
    case SSL_ERROR_WANT_WRITE:
        want_ = IoWant::Write;
        return Io::Again;
    case SSL_ERROR_ZERO_RETURN:
        // close_notify received: the only thing that means end of stream. An incomplete record on a
        // readable socket is WANT_READ above, and reading that as "the peer is gone" is pitfall 11.
        return Io::Closed;
    default:
        OB_LOG_WARN("tls", "%s failed: peer=%s fd=%d %s: %s", op, peer_label_.c_str(), fd_,
                    tls_error_name(ssl_error), tls_last_error().c_str());
        return Io::Error;
    }
}

TlsChannel::Io TlsChannel::read(void* buf, size_t len, size_t& out_n) {
    out_n = 0;
    if (len > static_cast<size_t>(INT_MAX)) len = static_cast<size_t>(INT_MAX);
    for (;;) {
        ERR_clear_error();
        const int n = SSL_read(ssl_.get(), buf, static_cast<int>(len));
        if (n > 0) {
            out_n = static_cast<size_t>(n);
            want_ = IoWant::Read;
            return Io::Data;
        }
        const int err = SSL_get_error(ssl_.get(), n);
        if (err == SSL_ERROR_SYSCALL) {
            if (n < 0 && errno == EINTR) continue;   // retried here, so `want_` cannot be wrong
            if (n == 0) return Io::Closed;           // EOF with no close_notify
        }
        return classify(err, "read");
    }
}

TlsChannel::Io TlsChannel::write(const void* buf, size_t len, size_t& out_n) {
    out_n = 0;
    if (len > static_cast<size_t>(INT_MAX)) len = static_cast<size_t>(INT_MAX);
    for (;;) {
        ERR_clear_error();
        const int n = SSL_write(ssl_.get(), buf, static_cast<int>(len));
        if (n > 0) {
            out_n = static_cast<size_t>(n);
            want_ = IoWant::Write;
            return Io::Data;
        }
        const int err = SSL_get_error(ssl_.get(), n);
        if (err == SSL_ERROR_SYSCALL) {
            if (n < 0 && errno == EINTR) continue;
            if (n == 0) return Io::Closed;
        }
        return classify(err, "write");
    }
}

void TlsChannel::shutdown() {
    // One call, result ignored, no second one - the reasoning is in tls_blocking_shutdown(): waiting
    // for the peer's close_notify buys nothing any caller uses and lets a peer that stopped reading
    // hold this thread.
    ERR_clear_error();
    SSL_shutdown(ssl_.get());
    ERR_clear_error();
}

namespace {

/// Translate an `SSL_get_error` code into the three answers a blocking caller can act on.
///
/// Returns 1 to retry the same call, 0 for a closed connection, -1 for a failure with `*why` set.
int classify_blocking(ssl_st* ssl, int ret, const char* op, std::string* why) {
    const int err = SSL_get_error(ssl, ret);
    switch (err) {
    case SSL_ERROR_WANT_READ:
    case SSL_ERROR_WANT_WRITE:
        // Not "come back later": this socket is blocking with SO_RCVTIMEO, and the socket BIO maps
        // the resulting EAGAIN to "should retry" exactly as it would for a non-blocking descriptor.
        // So a want here means the timeout expired, and retrying would wait out another one - for
        // ever, against a peer that has stopped talking, with the caller never told.
        if (why != nullptr) *why = std::string(op) + " timed out";
        return -1;
    case SSL_ERROR_ZERO_RETURN:
        if (why != nullptr) *why = "peer closed the TLS connection";
        return 0;
    case SSL_ERROR_SYSCALL:
        if (ret == 0) {
            // EOF with no close_notify. For this protocol every response is self-terminating, so
            // the caller's terminator search fails and an incomplete answer is never mistaken for a
            // whole one - but say which of the two closes it was, because one is a peer that quit
            // politely and the other may be a truncation.
            if (why != nullptr) *why = "peer closed without close_notify";
            return 0;
        }
        if (errno == EINTR) return 1;
        if (why != nullptr) {
            const std::string queued = tls_last_error();
            *why = std::string(op) + " failed: " +
                   (queued.empty() ? std::strerror(errno) : queued.c_str());
        }
        return -1;
    default:
        if (why != nullptr) {
            *why = std::string(op) + " failed (" + tls_error_name(err) + "): " + tls_last_error();
        }
        return -1;
    }
}

} // namespace

int tls_blocking_handshake(ssl_st* ssl, std::string* why) {
    for (;;) {
        ERR_clear_error();
        const int ret = SSL_do_handshake(ssl);
        if (ret == 1) {
            OB_LOG_INFO("tls", "handshake complete: version=%s cipher=%s",
                        SSL_get_version(ssl), SSL_get_cipher_name(ssl));
            return 1;
        }
        const int what = classify_blocking(ssl, ret, "TLS handshake", why);
        if (what != 1) return what;
    }
}

int tls_blocking_write_all(ssl_st* ssl, const char* buf, size_t len, std::string* why) {
    size_t done = 0;
    while (done < len) {
        // SSL_MODE_ENABLE_PARTIAL_WRITE is on for every context this file builds, so a short write
        // is expected rather than exceptional.
        const size_t chunk = std::min<size_t>(len - done, 1u << 20);
        ERR_clear_error();
        const int ret = SSL_write(ssl, buf + done, static_cast<int>(chunk));
        if (ret > 0) {
            done += static_cast<size_t>(ret);
            continue;
        }
        const int what = classify_blocking(ssl, ret, "TLS write", why);
        if (what != 1) return what;
    }
    return 1;
}

int tls_blocking_read(ssl_st* ssl, char* buf, size_t len, std::string* why) {
    if (len > static_cast<size_t>(INT_MAX)) len = static_cast<size_t>(INT_MAX);
    for (;;) {
        ERR_clear_error();
        const int ret = SSL_read(ssl, buf, static_cast<int>(len));
        if (ret > 0) return ret;
        const int what = classify_blocking(ssl, ret, "TLS read", why);
        if (what != 1) return what;
    }
}

void tls_blocking_shutdown(ssl_st* ssl) {
    // One call, result ignored, and deliberately no second one. SSL_shutdown returns 0 having sent
    // our close_notify and not yet seen theirs; waiting for the peer's would block for a whole
    // SO_RCVTIMEO on every disconnect, to learn something no caller uses. And after a
    // SSL_ERROR_SYSCALL or SSL_ERROR_SSL the object is unusable, so this may do nothing at all -
    // which is why it is best-effort and not a Result.
    ERR_clear_error();
    SSL_shutdown(ssl);
    ERR_clear_error();
}

std::string tls_last_error() {
    std::string out;
    // Drained, not peeked: a stale entry read on the next failure names the wrong cause, and an
    // error queue nobody empties turns every later diagnosis into the first one.
    while (const unsigned long e = ERR_get_error()) {
        char buf[256] = {0};
        ERR_error_string_n(e, buf, sizeof(buf));
        if (!out.empty()) out += "; ";
        out += buf;
    }
    return out;
}

const char* tls_error_name(int ssl_error) {
    switch (ssl_error) {
    case SSL_ERROR_NONE:             return "NONE";
    case SSL_ERROR_ZERO_RETURN:      return "ZERO_RETURN";
    case SSL_ERROR_WANT_READ:        return "WANT_READ";
    case SSL_ERROR_WANT_WRITE:       return "WANT_WRITE";
    case SSL_ERROR_WANT_CONNECT:     return "WANT_CONNECT";
    case SSL_ERROR_WANT_ACCEPT:      return "WANT_ACCEPT";
    case SSL_ERROR_WANT_X509_LOOKUP: return "WANT_X509_LOOKUP";
    case SSL_ERROR_SYSCALL:          return "SYSCALL";
    case SSL_ERROR_SSL:              return "SSL";
    default:                         return "unknown";
    }
}

} // namespace ob
