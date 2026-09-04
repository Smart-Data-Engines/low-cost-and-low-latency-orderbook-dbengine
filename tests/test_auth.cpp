// Unit tests for wire authentication primitives (#30, part one).
//
// Three things are being held here, and they fail in different directions:
//   - that we call HMAC-SHA256 correctly at all (RFC 4231 vectors);
//   - that the input we build binds every field it claims to bind (domain separation, identity,
//     nonce) and does not change silently (frozen goldens);
//   - that every refusal in the secret loader is reachable, and that no refusal message carries
//     secret material.

#include "orderbook/auth.hpp"

#include <gtest/gtest.h>

#include <sys/stat.h>

#include <filesystem>
#include <fstream>
#include <string>

using namespace ob;

namespace {

/// Write a secret file with an explicit mode. The mode is a parameter because one of the loader's
/// refusals is *about* the mode, and a helper that always chmods 600 could not reach it.
std::string write_secret_file(const std::string& name, const std::string& contents,
                              mode_t mode = 0600) {
    const auto path = std::filesystem::temp_directory_path() / ("ob_auth_" + name);
    {
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        out << contents;
    }
    ::chmod(path.c_str(), mode);
    return path.string();
}

constexpr const char* kSecret = "0123456789abcdef0123456789abcdef";
const std::string     kNonce(kAuthHexChars, 'a');

} // namespace

// ── HMAC against the published vectors ────────────────────────────────────────

TEST(AuthHmac, Rfc4231TestCase1) {
    const std::string key(20, '\x0b');
    EXPECT_EQ(hmac_sha256_hex(key, "Hi There"),
              "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7");
}

TEST(AuthHmac, Rfc4231TestCase2) {
    EXPECT_EQ(hmac_sha256_hex("Jefe", "what do ya want for nothing?"),
              "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
}

TEST(AuthHmac, EmbeddedNulBytesArePartOfTheInput) {
    // auth_response() separates its fields with NULs, so a HMAC that stopped at the first one
    // would MAC only the version prefix - and every response would be identical.
    const std::string with_nul("a\0b", 3);
    EXPECT_NE(hmac_sha256_hex(kSecret, with_nul), hmac_sha256_hex(kSecret, "a"));
}

// ── The input construction ────────────────────────────────────────────────────

TEST(AuthResponse, DomainSeparationAcrossSurfaces) {
    const auto c = auth_response(kSecret, AuthSurface::Client, AuthRole::Initiator, "alice", kNonce);
    const auto r = auth_response(kSecret, AuthSurface::Replication, AuthRole::Initiator, "alice", kNonce);
    const auto m = auth_response(kSecret, AuthSurface::MultiMaster, AuthRole::Initiator, "alice", kNonce);
    EXPECT_NE(c, r);
    EXPECT_NE(c, m);
    // The load-bearing pair: replication and multi-master share one cluster secret, so without
    // separation a response captured on one link authenticates on the other.
    EXPECT_NE(r, m);
}

TEST(AuthResponse, FrozenGoldenValues) {
    // The input format is a compatibility contract between a client and a server built at different
    // times: change it and every existing client fails to authenticate, with a message that says
    // "auth_failed" and nothing about a format change. Computed independently in Python from the
    // documented construction, not read out of this implementation.
    EXPECT_EQ(auth_response(kSecret, AuthSurface::Client, AuthRole::Initiator, "alice", kNonce),
              "ffa4c2f4da97192b4fed000cdab7366707999fd6828bc257a6a56cbcac90367e");
    EXPECT_EQ(auth_response(kSecret, AuthSurface::Replication, AuthRole::Initiator, "alice", kNonce),
              "1493d62f6aeabd35b233e24f1552bae0ca0afc3ed86697c199ca7a80e8080c0b");
    EXPECT_EQ(auth_response(kSecret, AuthSurface::MultiMaster, AuthRole::Initiator, "alice", kNonce),
              "05c24829778cba4086260fa03451630d2d7aeb5178c7e3ca9943369e2fe2c0f0");
    // The cluster surfaces authenticate with an empty identity, so that path has its own golden.
    EXPECT_EQ(auth_response(kSecret, AuthSurface::MultiMaster, AuthRole::Initiator, "", kNonce),
              "8fb4c0863b2fa95b74ccb6d0d255677d6bb1fc2e40cf99f74f062a41e060efee");
    // And one acceptor-side value, so a mutation that swapped the two labels is caught by a
    // golden as well as by the inequality above.
    EXPECT_EQ(auth_response(kSecret, AuthSurface::Client, AuthRole::Acceptor, "alice", kNonce),
              "197b47d63a4bfb2515e2fa47539bdd28eb3a2b5afe5826c3fe1e8fb1f3df6972");
}

TEST(AuthResponse, RoleIsBoundAndThatIsTheReflectionDefence) {
    // Both ends of a cluster link hold the same key. If both computed the same function of a
    // nonce, an attacker could echo the acceptor's own challenge back as its own, be handed the
    // answer - answering a challenge needs no authentication - and replay it. This inequality is
    // the whole defence, and a mutation dropping the role from the MAC input must fail here.
    EXPECT_NE(auth_response(kSecret, AuthSurface::Replication, AuthRole::Initiator, "", kNonce),
              auth_response(kSecret, AuthSurface::Replication, AuthRole::Acceptor, "", kNonce));
    EXPECT_NE(auth_response(kSecret, AuthSurface::MultiMaster, AuthRole::Initiator, "", kNonce),
              auth_response(kSecret, AuthSurface::MultiMaster, AuthRole::Acceptor, "", kNonce));
    EXPECT_EQ(role_label(AuthRole::Initiator), "initiator");
    EXPECT_EQ(role_label(AuthRole::Acceptor), "acceptor");
}

TEST(AuthResponse, IdentityIsBound) {
    // Under a shared secret, a response for alice must not authenticate bob.
    EXPECT_NE(auth_response(kSecret, AuthSurface::Client, AuthRole::Initiator, "alice", kNonce),
              auth_response(kSecret, AuthSurface::Client, AuthRole::Initiator, "bob", kNonce));
}

TEST(AuthResponse, NonceIsBound) {
    const std::string other(kAuthHexChars, 'b');
    EXPECT_NE(auth_response(kSecret, AuthSurface::Client, AuthRole::Initiator, "alice", kNonce),
              auth_response(kSecret, AuthSurface::Client, AuthRole::Initiator, "alice", other));
}

TEST(AuthResponse, SecretIsBound) {
    EXPECT_NE(auth_response(kSecret, AuthSurface::Client, AuthRole::Initiator, "alice", kNonce),
              auth_response("fedcba9876543210fedcba9876543210", AuthSurface::Client, AuthRole::Initiator, "alice", kNonce));
}

TEST(AuthResponse, FieldsCannotBeSlidPastTheSeparator) {
    // The NUL separators exist so that no two different tuples concatenate into the same input.
    // With a delimiter that could occur in a field, ("ab", "c") and ("a", "bc") would collide.
    EXPECT_NE(auth_response(kSecret, AuthSurface::Client, AuthRole::Initiator, "ab", kNonce),
              auth_response(kSecret, AuthSurface::Client, AuthRole::Initiator, "a", "b" + kNonce));
}

TEST(AuthNonce, IsHexAndFreshEveryCall) {
    const auto a = generate_nonce_hex();
    const auto b = generate_nonce_hex();
    EXPECT_EQ(a.size(), kAuthHexChars);
    EXPECT_TRUE(is_auth_hex(a));
    EXPECT_NE(a, b);
}

// ── Comparison ────────────────────────────────────────────────────────────────

TEST(AuthCompare, EqualUnequalAndLengthMismatch) {
    EXPECT_TRUE(responses_equal("abc", "abc"));
    EXPECT_FALSE(responses_equal("abc", "abd"));
    EXPECT_FALSE(responses_equal("abc", "abcd"));
}

TEST(AuthCompare, TwoEmptyStringsAreNotASuccessfulAuthentication) {
    // The case this guards: a client that sends no response and a server that computed none. Byte
    // equality would say yes.
    EXPECT_FALSE(responses_equal("", ""));
}

TEST(AuthHex, RejectsWrongLengthUpperCaseAndNonHex) {
    EXPECT_TRUE(is_auth_hex(std::string(kAuthHexChars, 'a')));
    EXPECT_FALSE(is_auth_hex(std::string(kAuthHexChars - 1, 'a')));
    EXPECT_FALSE(is_auth_hex(std::string(kAuthHexChars + 1, 'a')));
    EXPECT_FALSE(is_auth_hex(std::string(kAuthHexChars, 'A')));
    EXPECT_FALSE(is_auth_hex(std::string(kAuthHexChars, 'g')));
    EXPECT_FALSE(is_auth_hex(""));
}

// ── Log sanitisation ──────────────────────────────────────────────────────────

TEST(AuthLogSanitise, NewlinesDoNotSurvive) {
    // The whole attack: one newline and a plausible second line, and the log says what the peer
    // wanted it to say.
    const auto out = sanitise_for_log("alice\nINFO fake log line");
    EXPECT_EQ(out.find('\n'), std::string::npos);
    EXPECT_NE(out.find("alice"), std::string::npos);
}

TEST(AuthLogSanitise, NonPrintablesBecomeDots) {
    EXPECT_EQ(sanitise_for_log(std::string("a\x01\x7f" "b", 4)), "a..b");
}

TEST(AuthLogSanitise, TruncatesWithAVisibleMarker) {
    const auto out = sanitise_for_log(std::string(200, 'x'), 8);
    EXPECT_EQ(out, "xxxxxxxx...");
    // Bounded, so a peer cannot make us log an arbitrary amount of text.
    EXPECT_LE(out.size(), 8u + 3u);
}

TEST(AuthLogSanitise, ExactlyAtTheBoundGetsNoMarker) {
    EXPECT_EQ(sanitise_for_log("abcdefgh", 8), "abcdefgh");
}

// ── SecretStore: the client form ──────────────────────────────────────────────

TEST(SecretStoreClient, LoadsNamedCredentials) {
    const auto path = write_secret_file("client_ok",
                                       "# comment\n\nalice 0123456789abcdef0123456789abcdef\n"
                                       "bob   fedcba9876543210fedcba9876543210\n");
    const auto store = SecretStore::load_client_file(path);
    ASSERT_EQ(store.size(), 2u);
    ASSERT_NE(store.find("alice"), nullptr);
    EXPECT_EQ(store.find("alice")->secret, "0123456789abcdef0123456789abcdef");
    EXPECT_EQ(store.find("bob")->secret, "fedcba9876543210fedcba9876543210");
    EXPECT_EQ(store.find("carol"), nullptr);
    std::filesystem::remove(path);
}

TEST(SecretStoreClient, WhitespaceInsideTheSecretIsPartOfIt) {
    // The rule is "remove the line terminator and nothing else". A loader that trimmed would make
    // two different files the same secret, and for a secret that is a security property. The
    // flagship product's `read_bytes().strip()` shortened a random salt in about 5% of files.
    const auto path = write_secret_file("client_ws",
                                        "alice 0123456789abcdef0123456789abcdef  \n");
    const auto store = SecretStore::load_client_file(path);
    ASSERT_NE(store.find("alice"), nullptr);
    EXPECT_EQ(store.find("alice")->secret, "0123456789abcdef0123456789abcdef  ");
    std::filesystem::remove(path);
}

TEST(SecretStoreClient, CarriageReturnIsRemovedButNothingElseIs) {
    const auto path = write_secret_file("client_crlf",
                                        "alice 0123456789abcdef0123456789abcdef\r\n");
    const auto store = SecretStore::load_client_file(path);
    ASSERT_NE(store.find("alice"), nullptr);
    EXPECT_EQ(store.find("alice")->secret, "0123456789abcdef0123456789abcdef");
    std::filesystem::remove(path);
}

TEST(SecretStoreClient, RefusesAFileReadableBeyondItsOwner) {
    const auto path = write_secret_file("client_0644",
                                        "alice 0123456789abcdef0123456789abcdef\n", 0644);
    try {
        SecretStore::load_client_file(path);
        FAIL() << "expected a refusal";
    } catch (const std::runtime_error& e) {
        const std::string msg = e.what();
        EXPECT_NE(msg.find("0644"), std::string::npos) << msg;
        EXPECT_NE(msg.find(path), std::string::npos) << msg;
    }
    std::filesystem::remove(path);
}

TEST(SecretStoreClient, RefusesAMissingFileAndNamesIt) {
    const std::string path = "/nonexistent/ob_auth_missing";
    try {
        SecretStore::load_client_file(path);
        FAIL() << "expected a refusal";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find(path), std::string::npos) << e.what();
    }
}

TEST(SecretStoreClient, RefusesADirectory) {
    const auto dir = std::filesystem::temp_directory_path() / "ob_auth_dir";
    std::filesystem::create_directories(dir);
    EXPECT_THROW(SecretStore::load_client_file(dir.string()), std::runtime_error);
    std::filesystem::remove(dir);
}

TEST(SecretStoreClient, RefusesAnEmptyFile) {
    const auto path = write_secret_file("client_empty", "");
    EXPECT_THROW(SecretStore::load_client_file(path), std::runtime_error);
    std::filesystem::remove(path);
}

TEST(SecretStoreClient, RefusesAFileOfOnlyCommentsWithoutTreatingItAsAbsent) {
    const auto path = write_secret_file("client_comments", "# nothing here\n#\n");
    EXPECT_THROW(SecretStore::load_client_file(path), std::runtime_error);
    std::filesystem::remove(path);
}

TEST(SecretStoreClient, RefusesALineWithoutASecret) {
    const auto path = write_secret_file("client_noline", "alice\n");
    try {
        SecretStore::load_client_file(path);
        FAIL() << "expected a refusal";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("line 1"), std::string::npos) << e.what();
    }
    std::filesystem::remove(path);
}

TEST(SecretStoreClient, RefusesAShortSecretAndDoesNotPrintIt) {
    const auto path = write_secret_file("client_short", "alice hunter2\n");
    try {
        SecretStore::load_client_file(path);
        FAIL() << "expected a refusal";
    } catch (const std::runtime_error& e) {
        const std::string msg = e.what();
        // The refusal must say what is wrong without becoming the thing that leaks it. This is the
        // negative half, and it is the half a review would not have asked for.
        EXPECT_EQ(msg.find("hunter2"), std::string::npos) << msg;
        EXPECT_NE(msg.find("alice"), std::string::npos) << msg;
    }
    std::filesystem::remove(path);
}

TEST(SecretStoreClient, RefusesARepeatedIdentity) {
    const auto path = write_secret_file("client_dup",
                                        "alice 0123456789abcdef0123456789abcdef\n"
                                        "alice fedcba9876543210fedcba9876543210\n");
    try {
        SecretStore::load_client_file(path);
        FAIL() << "expected a refusal";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("alice"), std::string::npos) << e.what();
    }
    std::filesystem::remove(path);
}

TEST(SecretStoreClient, RefusesAnIdentityOutsideTheCharset) {
    const auto path = write_secret_file("client_badid",
                                        "ali ce 0123456789abcdef0123456789abcdef\n");
    // Splitting on the first separator makes the identity "ali" and the secret "ce 0123..." —
    // which is a valid identity, so the refusal here must come from the secret rule. The case that
    // matters is a genuinely illegal character.
    std::filesystem::remove(path);
    const auto path2 = write_secret_file("client_badid2",
                                         "ali/ce 0123456789abcdef0123456789abcdef\n");
    EXPECT_THROW(SecretStore::load_client_file(path2), std::runtime_error);
    std::filesystem::remove(path2);
}

TEST(SecretStoreClient, RefusesAnOverlongIdentity) {
    const std::string long_id(kMaxIdentityChars + 1, 'a');
    const auto path = write_secret_file("client_longid",
                                        long_id + " 0123456789abcdef0123456789abcdef\n");
    EXPECT_THROW(SecretStore::load_client_file(path), std::runtime_error);
    std::filesystem::remove(path);
}

// ── SecretStore: the cluster form ─────────────────────────────────────────────

TEST(SecretStoreCluster, LoadsOneSecretWithAnEmptyIdentity) {
    const auto path = write_secret_file("cluster_ok",
                                        "# the cluster secret\n0123456789abcdef0123456789abcdef\n");
    const auto store = SecretStore::load_cluster_file(path);
    ASSERT_EQ(store.size(), 1u);
    EXPECT_EQ(store.sole().identity, "");
    EXPECT_EQ(store.sole().secret, "0123456789abcdef0123456789abcdef");
    std::filesystem::remove(path);
}

TEST(SecretStoreCluster, RefusesASecondSecretLineAndNamesIt) {
    const auto path = write_secret_file("cluster_two",
                                        "0123456789abcdef0123456789abcdef\n"
                                        "fedcba9876543210fedcba9876543210\n");
    try {
        SecretStore::load_cluster_file(path);
        FAIL() << "expected a refusal";
    } catch (const std::runtime_error& e) {
        const std::string msg = e.what();
        EXPECT_NE(msg.find("line 2"), std::string::npos) << msg;
        EXPECT_EQ(msg.find("fedcba"), std::string::npos) << msg;
    }
    std::filesystem::remove(path);
}

TEST(SecretStoreCluster, RefusesAShortSecret) {
    const auto path = write_secret_file("cluster_short", "short\n");
    EXPECT_THROW(SecretStore::load_cluster_file(path), std::runtime_error);
    std::filesystem::remove(path);
}

TEST(SecretStoreCluster, SoleRefusesOnAMultiCredentialStore) {
    const auto path = write_secret_file("cluster_sole",
                                        "alice 0123456789abcdef0123456789abcdef\n"
                                        "bob   fedcba9876543210fedcba9876543210\n");
    const auto store = SecretStore::load_client_file(path);
    EXPECT_THROW(store.sole(), std::runtime_error);
    std::filesystem::remove(path);
}

// ── The cluster secret must not also be a client secret ───────────────────────

TEST(SecretStoreSharing, DetectsAClientSecretReusedAsTheClusterSecret) {
    // If it were reused, a client could present itself as a replica and stream the whole write-ahead
    // log: client authentication would grant node privileges, and nothing would look wrong.
    const auto cpath = write_secret_file("share_client",
                                         "alice 0123456789abcdef0123456789abcdef\n"
                                         "bob   fedcba9876543210fedcba9876543210\n");
    const auto same  = write_secret_file("share_cluster_same",
                                         "fedcba9876543210fedcba9876543210\n");
    const auto other = write_secret_file("share_cluster_other",
                                         "aaaabbbbccccddddaaaabbbbccccdddd\n");
    const auto clients = SecretStore::load_client_file(cpath);
    EXPECT_TRUE(stores_share_a_secret(clients, SecretStore::load_cluster_file(same)));
    EXPECT_FALSE(stores_share_a_secret(clients, SecretStore::load_cluster_file(other)));
    for (const auto& p : {cpath, same, other}) std::filesystem::remove(p);
}

TEST(SecretStoreSharing, AnEmptyStoreSharesNothing) {
    SecretStore empty;
    const auto path = write_secret_file("share_one", "0123456789abcdef0123456789abcdef\n");
    EXPECT_FALSE(stores_share_a_secret(empty, SecretStore::load_cluster_file(path)));
    EXPECT_FALSE(stores_share_a_secret(SecretStore::load_cluster_file(path), empty));
    std::filesystem::remove(path);
}

// ── Surface labels ────────────────────────────────────────────────────────────

TEST(AuthSurfaceLabel, EveryEnumeratorHasADistinctNonEmptyLabel) {
    EXPECT_EQ(surface_label(AuthSurface::Client), "client");
    EXPECT_EQ(surface_label(AuthSurface::Replication), "replication");
    EXPECT_EQ(surface_label(AuthSurface::MultiMaster), "mm");
}
