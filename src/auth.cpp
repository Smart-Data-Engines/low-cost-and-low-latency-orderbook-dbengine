#include "orderbook/auth.hpp"

#include "orderbook/logger.hpp"

#include <openssl/crypto.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/sha.h>

#include <sys/stat.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <stdexcept>

namespace ob {

namespace {

/// Lower-case hex of a byte range.
std::string to_hex(const unsigned char* data, size_t len) {
    static const char* digits = "0123456789abcdef";
    std::string out;
    out.resize(len * 2);
    for (size_t i = 0; i < len; ++i) {
        out[i * 2]     = digits[data[i] >> 4];
        out[i * 2 + 1] = digits[data[i] & 0x0F];
    }
    return out;
}

/// Remove the line terminator a text file leaves behind, and nothing else.
///
/// std::getline has already taken the '\n'; a file written on Windows leaves the '\r'. What this
/// must NOT do is strip whitespace generally: a secret of "abc " and a secret of "abc" would then
/// be the same secret, and for a secret "silently the same" is a security property rather than a
/// convenience. The flagship product learned this the expensive way - `read_bytes().strip()` on a
/// random salt removed leading or trailing bytes in about 5% of files, so the process that
/// generated the salt used 32 bytes and every later process used the remainder.
void drop_line_terminator(std::string& line) {
    if (!line.empty() && line.back() == '\r') {
        line.pop_back();
    }
}

/// Identity charset. Narrow on purpose: an identity ends up in log lines, in `STATUS` output and
/// eventually in an ACL (#31), so anything that would need quoting there is refused here instead.
bool is_identity_char(char c) {
    unsigned char u = static_cast<unsigned char>(c);
    return std::isalnum(u) != 0 || c == '_' || c == '-' || c == '.' || c == ':';
}

bool valid_identity(std::string_view id) {
    if (id.empty() || id.size() > kMaxIdentityChars) return false;
    return std::all_of(id.begin(), id.end(), is_identity_char);
}

/// Refuse everything about the file that is not its contents, before reading the contents.
///
/// Order matters: a world-readable secret file is reported as a permissions problem even when its
/// contents are also wrong, because that is the finding an operator must act on.
void check_file_or_throw(const std::string& path) {
    struct stat st{};
    if (::stat(path.c_str(), &st) != 0) {
        throw std::runtime_error("secret file '" + path + "' cannot be read: " +
                                 std::strerror(errno));
    }
    if (!S_ISREG(st.st_mode)) {
        throw std::runtime_error("secret file '" + path + "' is not a regular file");
    }
    if ((st.st_mode & 0077) != 0) {
        char mode[16];
        std::snprintf(mode, sizeof(mode), "%04o",
                      static_cast<unsigned>(st.st_mode & 07777));
        throw std::runtime_error("secret file '" + path + "' is readable beyond its owner (mode " +
                                 mode + "); chmod 600 it");
    }
    if (st.st_size == 0) {
        throw std::runtime_error("secret file '" + path + "' is empty");
    }
}

/// Read the file into significant lines: terminator removed, blank lines and `#` comments dropped.
///
/// A secret beginning with '#' therefore becomes a comment - and the file then has no significant
/// line left, so it is *refused* rather than silently treated as absent. Refusing is the point;
/// `openssl rand -hex 32`, which the operations guide prescribes, never produces one.
std::vector<std::pair<size_t, std::string>> significant_lines(const std::string& path) {
    std::ifstream in(path);
    if (!in) {
        throw std::runtime_error("secret file '" + path + "' cannot be opened");
    }
    std::vector<std::pair<size_t, std::string>> out;
    std::string line;
    size_t lineno = 0;
    while (std::getline(in, line)) {
        ++lineno;
        drop_line_terminator(line);
        if (line.empty() || line[0] == '#') continue;
        out.emplace_back(lineno, line);
    }
    return out;
}

} // namespace

// ── surface_label ─────────────────────────────────────────────────────────────

std::string_view surface_label(AuthSurface s) {
    switch (s) {
    case AuthSurface::Client:      return "client";
    case AuthSurface::Replication: return "replication";
    case AuthSurface::MultiMaster: return "mm";
    }
    // Unreachable for a valid enumerator. Returning a label would make a corrupted value
    // authenticate against some surface; an empty one authenticates against none.
    return {};
}

std::string_view role_label(AuthRole r) {
    switch (r) {
    case AuthRole::Initiator: return "initiator";
    case AuthRole::Acceptor:  return "acceptor";
    }
    // Same reasoning as surface_label: an empty label authenticates against nothing, while any
    // non-empty fallback would let a corrupted value collide with a real role.
    return {};
}

// ── generate_nonce_hex ────────────────────────────────────────────────────────

std::string generate_nonce_hex() {
    unsigned char buf[kAuthNonceBytes];
    if (RAND_bytes(buf, static_cast<int>(sizeof(buf))) != 1) {
        // Not a degraded mode. A predictable nonce turns challenge-response back into a bearer
        // token, and the caller cannot compensate for that, so it must not be handed one.
        throw std::runtime_error("auth: CSPRNG failed, refusing to issue a challenge");
    }
    return to_hex(buf, sizeof(buf));
}

// ── auth_response ─────────────────────────────────────────────────────────────

std::string hmac_sha256_hex(std::string_view key, std::string_view data) {
    unsigned char mac[EVP_MAX_MD_SIZE];
    unsigned int  mac_len = 0;
    const unsigned char* result =
        ::HMAC(EVP_sha256(),
               key.data(), static_cast<int>(key.size()),
               reinterpret_cast<const unsigned char*>(data.data()), data.size(),
               mac, &mac_len);
    if (result == nullptr || mac_len == 0) {
        throw std::runtime_error("auth: HMAC-SHA256 failed");
    }
    return to_hex(mac, mac_len);
}

std::string auth_response(std::string_view secret,
                          AuthSurface      surface,
                          AuthRole         role,
                          std::string_view identity,
                          std::string_view nonce_hex) {
    // "ob-auth-v1\0<surface>\0<role>\0<identity>\0<nonce_hex>". NUL separators rather than a
    // delimiter character, so no two different tuples can concatenate into the same input - the
    // classic way a MAC over joined fields stops binding the fields.
    std::string input;
    input.reserve(11 + 12 + 10 + identity.size() + nonce_hex.size() + 4);
    input.append("ob-auth-v1");
    input.push_back('\0');
    input.append(surface_label(surface));
    input.push_back('\0');
    input.append(role_label(role));
    input.push_back('\0');
    input.append(identity);
    input.push_back('\0');
    input.append(nonce_hex);

    return hmac_sha256_hex(secret, input);
}

// ── responses_equal ───────────────────────────────────────────────────────────

bool responses_equal(std::string_view a, std::string_view b) {
    // A length mismatch short-circuits, which leaks nothing: the length of a response is fixed by
    // the scheme and public. Equal lengths go through CRYPTO_memcmp, whose whole purpose is not
    // returning early on the first differing byte.
    if (a.size() != b.size()) return false;
    if (a.empty()) return false;   // two empty strings are not a successful authentication
    return ::CRYPTO_memcmp(a.data(), b.data(), a.size()) == 0;
}

// ── is_auth_hex ───────────────────────────────────────────────────────────────

bool is_auth_hex(std::string_view s) {
    if (s.size() != kAuthHexChars) return false;
    return std::all_of(s.begin(), s.end(), [](char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
    });
}

// ── sanitise_for_log ──────────────────────────────────────────────────────────

std::string sanitise_for_log(std::string_view raw, size_t max_len) {
    std::string out;
    const size_t take = std::min(raw.size(), max_len);
    out.reserve(take + 3);
    for (size_t i = 0; i < take; ++i) {
        unsigned char c = static_cast<unsigned char>(raw[i]);
        // Printable ASCII only. A newline here is the whole attack: one '\n' and a plausible
        // second line, and the log says whatever the peer wanted it to say.
        out.push_back((c >= 0x20 && c < 0x7F) ? static_cast<char>(c) : '.');
    }
    if (raw.size() > max_len) {
        // A visible marker, so a bounded line cannot be read as the whole value.
        out.append("...");
    }
    return out;
}

// ── SecretStore ───────────────────────────────────────────────────────────────

SecretStore SecretStore::load_client_file(const std::string& path) {
    check_file_or_throw(path);
    const auto lines = significant_lines(path);
    if (lines.empty()) {
        throw std::runtime_error("secret file '" + path + "' has no credential lines");
    }

    SecretStore store;
    for (const auto& [lineno, line] : lines) {
        const size_t sep = line.find_first_of(" \t");
        if (sep == std::string::npos) {
            throw std::runtime_error("secret file '" + path + "' line " + std::to_string(lineno) +
                                     ": expected '<identity> <secret>'");
        }
        std::string identity = line.substr(0, sep);
        // The secret is the rest of the line after exactly one run of separators, verbatim: it may
        // contain spaces, and trailing spaces are part of it (see drop_line_terminator).
        const size_t secret_begin = line.find_first_not_of(" \t", sep);
        std::string secret = (secret_begin == std::string::npos) ? std::string{}
                                                                 : line.substr(secret_begin);

        if (!valid_identity(identity)) {
            throw std::runtime_error("secret file '" + path + "' line " + std::to_string(lineno) +
                                     ": identity must be 1-" + std::to_string(kMaxIdentityChars) +
                                     " characters of [A-Za-z0-9_.:-]");
        }
        if (secret.size() < kMinSecretChars) {
            throw std::runtime_error("secret file '" + path + "' line " + std::to_string(lineno) +
                                     ": secret for identity '" + identity + "' is " +
                                     std::to_string(secret.size()) + " characters, minimum is " +
                                     std::to_string(kMinSecretChars));
        }
        if (store.find(identity) != nullptr) {
            throw std::runtime_error("secret file '" + path + "' line " + std::to_string(lineno) +
                                     ": identity '" + identity + "' appears twice");
        }
        store.credentials_.push_back(Credential{std::move(identity), std::move(secret)});
    }
    return store;
}

SecretStore SecretStore::load_cluster_file(const std::string& path) {
    check_file_or_throw(path);
    const auto lines = significant_lines(path);
    if (lines.empty()) {
        throw std::runtime_error("secret file '" + path + "' has no secret line");
    }
    if (lines.size() > 1) {
        throw std::runtime_error("secret file '" + path + "' has more than one secret line "
                                 "(second at line " + std::to_string(lines[1].first) +
                                 "); the cluster secret is a single line");
    }
    const std::string& secret = lines[0].second;
    if (secret.size() < kMinSecretChars) {
        throw std::runtime_error("secret file '" + path + "': secret is " +
                                 std::to_string(secret.size()) + " characters, minimum is " +
                                 std::to_string(kMinSecretChars));
    }
    SecretStore store;
    // No identity: a node's identity is its node_id, which arrives in the handshake that
    // authentication precedes. An empty identity is also what goes into the HMAC input on the two
    // cluster surfaces, so the value here is the wire behaviour rather than a placeholder.
    store.credentials_.push_back(Credential{std::string{}, secret});
    return store;
}

const Credential* SecretStore::find(std::string_view identity) const {
    for (const auto& c : credentials_) {
        if (c.identity == identity) return &c;
    }
    return nullptr;
}

const Credential& SecretStore::sole() const {
    if (credentials_.size() != 1) {
        throw std::runtime_error("auth: sole() on a store holding " +
                                 std::to_string(credentials_.size()) + " credentials");
    }
    return credentials_.front();
}

bool stores_share_a_secret(const SecretStore& a, const SecretStore& b) {
    for (const auto& x : a.credentials_) {
        for (const auto& y : b.credentials_) {
            if (responses_equal(x.secret, y.secret)) return true;
        }
    }
    return false;
}

} // namespace ob
