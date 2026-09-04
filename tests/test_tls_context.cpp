// TLS context loading and its refusals (#30 part three).
//
// The most valuable test here is the last one: it pins the two `SSL_CTX` modes that
// `benchmarks/tls/ssl_write_retry.c` measured as necessary. Without them the engine's send path is
// incompatible with OpenSSL, and the two failures look nothing alike - one is an error, the other
// is a large response that silently stops making progress.

#include "orderbook/tls.hpp"

#include <gtest/gtest.h>

#include <openssl/ssl.h>

#include <sys/stat.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

namespace fs = std::filesystem;

namespace {

struct KeyPair {
    std::string cert;
    std::string key;
};

/// A self-signed certificate and its key, generated with the openssl tool.
///
/// The tool rather than the library: this fixture is about what the engine does with files on disk,
/// and generating them the way an operator does is what `docs/operations.md` will tell them.
KeyPair generate(const std::string& tag) {
    const auto dir = fs::temp_directory_path() / ("ob_tls_" + tag + std::to_string(std::rand()));
    fs::create_directories(dir);
    const std::string cert = (dir / "cert.pem").string();
    const std::string key  = (dir / "key.pem").string();
    const std::string cmd =
        "openssl req -x509 -newkey rsa:2048 -keyout '" + key + "' -out '" + cert +
        "' -days 1 -nodes -subj '/CN=localhost' >/dev/null 2>&1";
    if (std::system(cmd.c_str()) != 0) return {"", ""};
    ::chmod(key.c_str(), 0600);
    return {cert, key};
}

std::string write_file(const std::string& tag, const std::string& contents, mode_t mode = 0600) {
    const auto p = fs::temp_directory_path() / ("ob_tls_f_" + tag + std::to_string(std::rand()));
    { std::ofstream out(p, std::ios::trunc); out << contents; }
    ::chmod(p.c_str(), mode);
    return p.string();
}

} // namespace

TEST(TlsContext, LoadsAValidPair) {
    const auto kp = generate("ok");
    ASSERT_FALSE(kp.cert.empty()) << "openssl tool not available";
    EXPECT_NO_THROW({ auto ctx = ob::TlsContext::server(kp.cert, kp.key); (void)ctx; });
    fs::remove_all(fs::path(kp.cert).parent_path());
}

TEST(TlsContext, RefusesAMissingCertificateAndNamesIt) {
    const auto kp = generate("nocert");
    ASSERT_FALSE(kp.cert.empty());
    try {
        auto ctx = ob::TlsContext::server("/nonexistent/ob_tls_cert.pem", kp.key);
        FAIL() << "expected a refusal";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("/nonexistent/ob_tls_cert.pem"), std::string::npos)
            << e.what();
    }
    fs::remove_all(fs::path(kp.cert).parent_path());
}

TEST(TlsContext, RefusesAKeyReadableBeyondItsOwnerAndPrintsTheMode) {
    const auto kp = generate("mode");
    ASSERT_FALSE(kp.cert.empty());
    ::chmod(kp.key.c_str(), 0644);
    try {
        auto ctx = ob::TlsContext::server(kp.cert, kp.key);
        FAIL() << "expected a refusal";
    } catch (const std::runtime_error& e) {
        const std::string msg = e.what();
        // The mode it found, not just "bad permissions": an operator fixing this needs to know
        // what it is now. Same message shape as the secret files of part one.
        EXPECT_NE(msg.find("0644"), std::string::npos) << msg;
        EXPECT_NE(msg.find("readable beyond its owner"), std::string::npos) << msg;
    }
    fs::remove_all(fs::path(kp.cert).parent_path());
}

TEST(TlsContext, RefusesAKeyThatDoesNotMatchTheCertificate) {
    // Checked at startup on purpose: the alternative is every client's handshake failing, with a
    // message the operator reads as a client problem.
    //
    // Asserted as the *invariant* rather than on one branch's wording, and the reason is worth
    // knowing: `SSL_CTX_use_PrivateKey_file` compares the key against the certificate already
    // loaded, so it catches this first and the explicit `SSL_CTX_check_private_key` below it never
    // runs for this input. That check is kept as defence against a future reordering - loading the
    // key before the certificate would make it the only thing catching a mismatch - and pitfall 45
    // is exactly this: a duplicated guarantee cannot be mutation-tested on its own, so the test
    // asserts the outcome and names the file, not the branch.
    const auto a = generate("pair_a");
    const auto b = generate("pair_b");
    ASSERT_FALSE(a.cert.empty());
    ASSERT_FALSE(b.cert.empty());
    try {
        auto ctx = ob::TlsContext::server(a.cert, b.key);
        FAIL() << "a mismatched certificate and key started a context";
    } catch (const std::runtime_error& e) {
        const std::string msg = e.what();
        // Whichever call caught it, the message must name the key so an operator can act, and
        // carry OpenSSL's reason so they know it is a mismatch and not a bad format.
        EXPECT_NE(msg.find(b.key), std::string::npos) << msg;
        EXPECT_TRUE(msg.find("mismatch") != std::string::npos ||
                    msg.find("does not match") != std::string::npos) << msg;
    }
    fs::remove_all(fs::path(a.cert).parent_path());
    fs::remove_all(fs::path(b.cert).parent_path());
}

TEST(TlsContext, RefusesAnEmptyFileAndADirectory) {
    const auto kp = generate("empty");
    ASSERT_FALSE(kp.cert.empty());
    const auto empty = write_file("empty", "");
    EXPECT_THROW(ob::TlsContext::server(empty, kp.key), std::runtime_error);

    const auto dir = fs::temp_directory_path() / "ob_tls_dir";
    fs::create_directories(dir);
    EXPECT_THROW(ob::TlsContext::server(dir.string(), kp.key), std::runtime_error);

    fs::remove(empty);
    fs::remove(dir);
    fs::remove_all(fs::path(kp.cert).parent_path());
}

TEST(TlsContext, RefusesAFileThatIsNotACertificate) {
    const auto kp = generate("garbage");
    ASSERT_FALSE(kp.cert.empty());
    const auto junk = write_file("junk", "this is not a PEM certificate\n");
    try {
        auto ctx = ob::TlsContext::server(junk, kp.key);
        FAIL() << "expected a refusal";
    } catch (const std::runtime_error& e) {
        // OpenSSL's own reason, carried through rather than swallowed: "rejected" alone leaves an
        // operator guessing between a wrong format and a wrong file.
        const std::string msg = e.what();
        EXPECT_NE(msg.find("rejected"), std::string::npos) << msg;
        EXPECT_GT(msg.size(), std::string("TLS certificate '' rejected: ").size() + junk.size())
            << "the refusal carried no reason from OpenSSL: " << msg;
    }
    fs::remove(junk);
    fs::remove_all(fs::path(kp.cert).parent_path());
}

TEST(TlsContext, AClientContextRefusesAnUnreadableCaBundle) {
    EXPECT_THROW(ob::TlsContext::client("/nonexistent/ob_ca.pem", /*verify=*/true),
                 std::runtime_error);
}

TEST(TlsContext, AClientContextWithoutVerificationStillBuilds) {
    // Allowed, and it logs a warning saying what it does and does not protect against. The point
    // is that turning it off is a named act rather than a default.
    EXPECT_NO_THROW({ auto ctx = ob::TlsContext::client("", /*verify=*/false); (void)ctx; });
}

TEST(TlsError, DrainsTheQueueSoTheNextDiagnosisIsItsOwn) {
    // A stale entry read on the next failure names the wrong cause. Provoke one, read it, and the
    // second read must be empty.
    EXPECT_THROW(ob::TlsContext::client("/nonexistent/ob_ca.pem", true), std::runtime_error);
    (void)ob::tls_last_error();
    EXPECT_TRUE(ob::tls_last_error().empty())
        << "the error queue was not drained, so the next failure will report this one";
}

TEST(TlsError, NamesEveryWantCode) {
    EXPECT_STREQ(ob::tls_error_name(SSL_ERROR_WANT_READ), "WANT_READ");
    EXPECT_STREQ(ob::tls_error_name(SSL_ERROR_WANT_WRITE), "WANT_WRITE");
    EXPECT_STREQ(ob::tls_error_name(SSL_ERROR_ZERO_RETURN), "ZERO_RETURN");
    EXPECT_STREQ(ob::tls_error_name(SSL_ERROR_SSL), "SSL");
}

// ── The two modes the send path cannot work without ──────────────────────────

TEST(TlsContext, SetsBothModesTheEnginesSendPathNeeds) {
    // Measured, not assumed - benchmarks/tls/ssl_write_retry.c, OpenSSL 3.0.13:
    //
    //   ENABLE_PARTIAL_WRITE only  -> error:0A00007F:SSL routines::bad write retry
    //   both                       -> WANT_WRITE, an ordinary "come back later"
    //   neither                    -> the first WANT lands at offset 0, so a large response makes
    //                                 no progress at all and looks like a slow client
    //
    // `Session::flush_output()` does `send_buf_.erase(0, n)`, which moves the pending bytes to a
    // different address, so both bits are load-bearing. A mutation dropping either one fails here.
    const auto kp = generate("modes");
    ASSERT_FALSE(kp.cert.empty());
    auto ctx = ob::TlsContext::server(kp.cert, kp.key);
    const long modes = SSL_CTX_get_mode(reinterpret_cast<SSL_CTX*>(ctx.raw()));
    EXPECT_TRUE(modes & SSL_MODE_ENABLE_PARTIAL_WRITE)
        << "without partial writes SSL_write accepts nothing until the whole buffer fits";
    EXPECT_TRUE(modes & SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER)
        << "send_buf_.erase(0, n) moves the pending bytes, which OpenSSL refuses without this";
    fs::remove_all(fs::path(kp.cert).parent_path());
}

TEST(TlsContext, RequiresTlsOneThree) {
    const auto kp = generate("version");
    ASSERT_FALSE(kp.cert.empty());
    auto ctx = ob::TlsContext::server(kp.cert, kp.key);
    EXPECT_EQ(SSL_CTX_get_min_proto_version(reinterpret_cast<SSL_CTX*>(ctx.raw())),
              TLS1_3_VERSION)
        << "TLS 1.2 is what a full kernel data path would need, and this engine chose not to";
    fs::remove_all(fs::path(kp.cert).parent_path());
}

// ── The refusal that is the feature ──────────────────────────────────────────

TEST(TlsStatic, EveryTlsFlagIsRefusedByTheIoUringTransport) {
    // `--tls-client` that quietly meant plaintext would be the single worst outcome this feature
    // can produce, and it would look identical to working. No CI job builds the io_uring file, so
    // this is the only thing standing between that flag and a plaintext listener.
    //
    // Asserted against the refusal branch's own source rather than against a list written here:
    // a fourth TLS flag added without extending that branch fails this test.
    std::ifstream flags_src(std::string(OB_SOURCE_DIR) + "/src/tcp_server.cpp");
    ASSERT_TRUE(flags_src);
    const std::string flags((std::istreambuf_iterator<char>(flags_src)),
                            std::istreambuf_iterator<char>());

    std::ifstream uring_src(std::string(OB_SOURCE_DIR) + "/src/io_uring_server.cpp");
    ASSERT_TRUE(uring_src);
    const std::string uring((std::istreambuf_iterator<char>(uring_src)),
                            std::istreambuf_iterator<char>());

    // Every "tls-..." key the parser knows must be named in the io_uring refusal.
    size_t checked = 0;
    for (size_t pos = flags.find("\"tls-"); pos != std::string::npos;
         pos = flags.find("\"tls-", pos + 1)) {
        const size_t end = flags.find('"', pos + 1);
        ASSERT_NE(end, std::string::npos);
        const std::string key = flags.substr(pos + 1, end - pos - 1);
        const std::string flag = "--" + key;
        EXPECT_NE(uring.find(flag), std::string::npos)
            << flag << " is a TLS flag the io_uring transport does not refuse, so that build "
                       "would accept it and listen in plaintext";
        ++checked;
    }
    EXPECT_GE(checked, 3u) << "expected at least three tls-* keys; found " << checked;
}
