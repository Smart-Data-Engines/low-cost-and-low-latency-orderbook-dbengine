#include "orderbook/tls.hpp"

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

} // namespace

TlsContext TlsContext::server(const std::string& cert_file, const std::string& key_file) {
    check_file_or_throw(cert_file, "TLS certificate", /*require_owner_only=*/false);
    check_file_or_throw(key_file,  "TLS private key", /*require_owner_only=*/true);

    SSL_CTX* ctx = SSL_CTX_new(TLS_server_method());
    if (ctx == nullptr) {
        throw std::runtime_error("TLS: SSL_CTX_new failed: " + tls_last_error());
    }

    // TLS 1.3 only. Not a default to be overridden by a flag: the version floor is the one setting
    // where "configurable" means "misconfigurable", and 1.2 exists here only because kernel TLS
    // wants it (requirements §1.3).
    if (SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION) != 1) {
        SSL_CTX_free(ctx);
        throw std::runtime_error("TLS: cannot require TLS 1.3: " + tls_last_error());
    }
    set_engine_modes(ctx);

    if (SSL_CTX_use_certificate_chain_file(ctx, cert_file.c_str()) != 1) {
        const std::string why = tls_last_error();
        SSL_CTX_free(ctx);
        throw std::runtime_error("TLS certificate '" + cert_file + "' rejected: " + why);
    }
    if (SSL_CTX_use_PrivateKey_file(ctx, key_file.c_str(), SSL_FILETYPE_PEM) != 1) {
        const std::string why = tls_last_error();
        SSL_CTX_free(ctx);
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
        SSL_CTX_free(ctx);
        throw std::runtime_error("TLS private key '" + key_file + "' does not match certificate '" +
                                 cert_file + "': " + why);
    }

    OB_LOG_INFO("tls", "server context ready: cert=%s key=%s min=TLSv1.3",
                cert_file.c_str(), key_file.c_str());
    return TlsContext(ctx);
}

TlsContext TlsContext::client(const std::string& ca_file, bool verify) {
    SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
    if (ctx == nullptr) {
        throw std::runtime_error("TLS: SSL_CTX_new failed: " + tls_last_error());
    }
    if (SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION) != 1) {
        SSL_CTX_free(ctx);
        throw std::runtime_error("TLS: cannot require TLS 1.3: " + tls_last_error());
    }
    set_engine_modes(ctx);

    if (verify) {
        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
        if (!ca_file.empty()) {
            check_file_or_throw(ca_file, "TLS CA bundle", /*require_owner_only=*/false);
            if (SSL_CTX_load_verify_locations(ctx, ca_file.c_str(), nullptr) != 1) {
                const std::string why = tls_last_error();
                SSL_CTX_free(ctx);
                throw std::runtime_error("TLS CA bundle '" + ca_file + "' rejected: " + why);
            }
        } else if (SSL_CTX_set_default_verify_paths(ctx) != 1) {
            const std::string why = tls_last_error();
            SSL_CTX_free(ctx);
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
    return TlsContext(ctx);
}

TlsContext::~TlsContext() {
    if (ctx_ != nullptr) SSL_CTX_free(ctx_);
}

TlsContext::TlsContext(TlsContext&& other) noexcept : ctx_(other.ctx_) {
    other.ctx_ = nullptr;
}

TlsContext& TlsContext::operator=(TlsContext&& other) noexcept {
    if (this != &other) {
        if (ctx_ != nullptr) SSL_CTX_free(ctx_);
        ctx_       = other.ctx_;
        other.ctx_ = nullptr;
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
