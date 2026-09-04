// Tests for the client-session authentication gate (#30, part one).
//
// The gate sits before execute_command's switch rather than inside each case, so the two things
// worth holding are: that the classification of every command is deliberate, and that the five
// outcomes of AUTH are the five the protocol documents.

#include "orderbook/auth.hpp"
#include "orderbook/command_parser.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/session.hpp"
#include "orderbook/tcp_server.hpp"

#include <gtest/gtest.h>

#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

namespace fs = std::filesystem;

namespace {

std::string make_temp_dir(const std::string& prefix) {
    auto tmp = fs::temp_directory_path() / (prefix + std::to_string(std::rand()));
    fs::create_directories(tmp);
    return tmp.string();
}

constexpr const char* kSecret = "0123456789abcdef0123456789abcdef";

ob::SecretStore make_store() {
    const auto path = fs::temp_directory_path() /
                      ("ob_gate_secret_" + std::to_string(std::rand()));
    {
        std::ofstream out(path, std::ios::trunc);
        out << "alice " << kSecret << "\n";
    }
    ::chmod(path.c_str(), 0600);
    auto store = ob::SecretStore::load_client_file(path.string());
    fs::remove(path);
    return store;
}

/// Extract the nonce from "OK CHALLENGE <hex>\n\n".
std::string nonce_from(const std::string& response) {
    const std::string prefix = "OK CHALLENGE ";
    if (response.rfind(prefix, 0) != 0) return {};
    auto rest = response.substr(prefix.size());
    const auto nl = rest.find('\n');
    return (nl == std::string::npos) ? rest : rest.substr(0, nl);
}

class AuthGateTest : public ::testing::Test {
protected:
    std::string                 temp_dir_;
    std::unique_ptr<ob::Engine> engine_;
    ob::ServerStats             stats_;
    ob::SecretStore             store_;
    int fd_server_ = -1;
    int fd_client_ = -1;

    void SetUp() override {
        temp_dir_ = make_temp_dir("auth_gate_test_");
        engine_   = std::make_unique<ob::Engine>(temp_dir_);
        engine_->open();
        store_ = make_store();
        int fds[2];
        ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
        fd_server_ = fds[0];
        fd_client_ = fds[1];
    }

    void TearDown() override {
        engine_->close();
        if (fd_server_ >= 0) ::close(fd_server_);
        if (fd_client_ >= 0) ::close(fd_client_);
        fs::remove_all(temp_dir_);
    }

    /// Run a command line through the gate with authentication enabled.
    std::string run(ob::Session& s, const std::string& line) {
        const auto cmd = ob::parse_command(line);
        return ob::execute_command(cmd, *engine_, s, stats_, false, nullptr, nullptr, nullptr,
                                   &store_);
    }

    /// The same with authentication disabled.
    std::string run_open(ob::Session& s, const std::string& line) {
        const auto cmd = ob::parse_command(line);
        return ob::execute_command(cmd, *engine_, s, stats_);
    }

    /// Walk a session all the way through authentication, returning the final response.
    std::string authenticate(ob::Session& s, const std::string& identity = "alice",
                             const std::string& secret = kSecret) {
        const auto challenge = run(s, "AUTH");
        const auto nonce     = nonce_from(challenge);
        const auto response  = ob::auth_response(secret, ob::AuthSurface::Client, ob::AuthRole::Initiator,
                                                 identity, nonce);
        return run(s, "AUTH " + identity + " " + response);
    }
};

} // namespace

// ── Classification ────────────────────────────────────────────────────────────

TEST(AuthGateStatic, ExactlyFourCommandTypesAreAllowedBeforeAuthentication) {
    // Iterating the enumeration rather than reading a list: a new command classified as allowed
    // fails here, and a new command classified at all is forced by -Wswitch in the classifier.
    size_t allowed = 0;
    for (int i = 0; i <= static_cast<int>(ob::CommandType::UNKNOWN); ++i) {
        if (ob::allowed_before_authentication(static_cast<ob::CommandType>(i))) ++allowed;
    }
    EXPECT_EQ(allowed, 4u) << "AUTH, PING, QUIT and UNKNOWN, and adding to that set is a decision";

    EXPECT_TRUE(ob::allowed_before_authentication(ob::CommandType::AUTH));
    EXPECT_TRUE(ob::allowed_before_authentication(ob::CommandType::PING));
    EXPECT_TRUE(ob::allowed_before_authentication(ob::CommandType::QUIT));
    EXPECT_TRUE(ob::allowed_before_authentication(ob::CommandType::UNKNOWN));
}

TEST(AuthGateStatic, EverySensitiveCommandRequiresAuthentication) {
    for (auto t : {ob::CommandType::SELECT, ob::CommandType::INSERT, ob::CommandType::MINSERT,
                   ob::CommandType::FLUSH, ob::CommandType::STATUS, ob::CommandType::ROLE,
                   ob::CommandType::FAILOVER, ob::CommandType::COMPRESS,
                   ob::CommandType::SHARD_MAP, ob::CommandType::SHARD_INFO,
                   ob::CommandType::MIGRATE, ob::CommandType::MM_PEERS,
                   ob::CommandType::MM_CONFLICTS, ob::CommandType::SUBSCRIBE,
                   ob::CommandType::UNSUBSCRIBE}) {
        EXPECT_FALSE(ob::allowed_before_authentication(t))
            << "CommandType " << static_cast<int>(t) << " reachable without authentication";
    }
}

TEST(AuthGateStatic, TheClassifierHasNoDefaultLabel) {
    // `-Wswitch` makes a missing enumerator a build failure only while the switch has no `default:`.
    // Adding one would silently give every future command whatever the default says - and the
    // dangerous direction, "allowed", is one character away from the safe one.
    std::ifstream in(std::string(OB_SOURCE_DIR) + "/src/tcp_server.cpp");
    ASSERT_TRUE(in) << "cannot read src/tcp_server.cpp";
    const std::string src((std::istreambuf_iterator<char>(in)),
                          std::istreambuf_iterator<char>());
    const auto begin = src.find("bool allowed_before_authentication(CommandType t) {");
    ASSERT_NE(begin, std::string::npos) << "classifier not found - did it get renamed?";
    const auto end = src.find("\n}\n", begin);
    ASSERT_NE(end, std::string::npos);
    const auto body = src.substr(begin, end - begin);
    EXPECT_EQ(body.find("default:"), std::string::npos)
        << "a default label turns off the exhaustiveness check this function relies on";
}

TEST(AuthGateStatic, EveryTransportPassesACredentialStoreToExecuteCommand) {
    // The gate is one seam for two loops only while both loops pass the store. `execute_command`
    // takes it as a defaulted argument - which every unit test relies on - so a transport that
    // forgets it compiles, runs, and authenticates nobody.
    //
    // io_uring is the case this exists for: `OB_USE_IO_URING` is off by default and **no CI job
    // builds that file**, so nothing else would notice. Same shape as the four integration modules
    // that built their own path to the server binary (#85) - a mechanism whose scope can shrink in
    // silence.
    for (const char* rel : {"/src/tcp_server.cpp", "/src/io_uring_server.cpp"}) {
        std::ifstream in(std::string(OB_SOURCE_DIR) + rel);
        ASSERT_TRUE(in) << "cannot read " << rel;
        const std::string src((std::istreambuf_iterator<char>(in)),
                              std::istreambuf_iterator<char>());
        const auto call = src.find("= execute_command(");
        ASSERT_NE(call, std::string::npos) << rel << " does not dispatch commands any more?";
        const auto end = src.find(';', call);
        ASSERT_NE(end, std::string::npos);
        const auto args = src.substr(call, end - call);
        EXPECT_NE(args.find("client_store()"), std::string::npos)
            << rel << " calls execute_command without a credential store, so this transport "
                      "accepts every command unauthenticated:\n" << args;
    }
}

// ── The five outcomes of AUTH ─────────────────────────────────────────────────

TEST_F(AuthGateTest, ABareAuthIssuesAChallenge) {
    ob::Session s(fd_server_);
    const auto response = run(s, "AUTH");
    ASSERT_EQ(response.rfind("OK CHALLENGE ", 0), 0u) << response;
    EXPECT_TRUE(ob::is_auth_hex(nonce_from(response))) << response;
    EXPECT_FALSE(s.authenticated());
}

TEST_F(AuthGateTest, ACorrectResponseAuthenticatesAndNamesTheIdentity) {
    ob::Session s(fd_server_);
    EXPECT_EQ(authenticate(s), "OK AUTH alice\n\n");
    EXPECT_TRUE(s.authenticated());
    EXPECT_EQ(s.identity(), "alice");
}

TEST_F(AuthGateTest, AResponseWithoutAChallengeIsRefused) {
    ob::Session s(fd_server_);
    const auto response = run(s, "AUTH alice " + std::string(ob::kAuthHexChars, 'a'));
    EXPECT_EQ(response, "ERR auth_no_challenge\n");
    EXPECT_FALSE(s.authenticated());
}

TEST_F(AuthGateTest, AWrongResponseIsRefusedAndClosesTheSession) {
    ob::Session s(fd_server_);
    const auto response = authenticate(s, "alice", "fedcba9876543210fedcba9876543210");
    EXPECT_EQ(response, "ERR auth_failed\n");
    EXPECT_FALSE(s.authenticated());
    EXPECT_EQ(s.auth_attempts(), 1u);
    // One attempt per connection is the whole rate limit, so the close is the mechanism.
    EXPECT_TRUE(s.close_requested());
}

TEST_F(AuthGateTest, AnUnknownIdentityIsRefusedWithTheSameMessageAsAWrongResponse) {
    ob::Session s(fd_server_);
    // Identical wire message on purpose: distinguishing the two tells an attacker which names
    // exist, and tells an operator nothing their log does not already say.
    const auto response = authenticate(s, "mallory", kSecret);
    EXPECT_EQ(response, "ERR auth_failed\n");
    EXPECT_FALSE(s.authenticated());
}

TEST_F(AuthGateTest, ASecondAuthAfterSuccessIsRefused) {
    ob::Session s(fd_server_);
    ASSERT_EQ(authenticate(s), "OK AUTH alice\n\n");
    EXPECT_EQ(run(s, "AUTH"), "ERR already_authenticated\n");
    EXPECT_EQ(run(s, "AUTH alice " + std::string(ob::kAuthHexChars, 'a')),
              "ERR already_authenticated\n");
    // Still authenticated as the original identity: a refused second attempt must not clear it.
    EXPECT_TRUE(s.authenticated());
    EXPECT_EQ(s.identity(), "alice");
}

// ── The gate itself ───────────────────────────────────────────────────────────

TEST_F(AuthGateTest, SelectBeforeAuthenticationIsRefused) {
    ob::Session s(fd_server_);
    EXPECT_EQ(run(s, "SELECT * FROM orderbook"), "ERR unauthenticated\n");
}

TEST_F(AuthGateTest, EveryRefusedCommandGetsTheSameAnswer) {
    ob::Session s(fd_server_);
    for (const char* line : {"SELECT * FROM orderbook",
                             "INSERT BTCUSD BINANCE ask 50000 10 1",
                             "FLUSH", "STATUS", "ROLE", "COMPRESS LZ4",
                             "SHARD_MAP", "SHARD_INFO", "MM_PEERS", "MM_CONFLICTS",
                             "FAILOVER node-2", "MIGRATE BTC.BIN shard-1",
                             "SUBSCRIBE * FROM orderbook", "UNSUBSCRIBE"}) {
        EXPECT_EQ(run(s, line), "ERR unauthenticated\n") << "line: " << line;
    }
}

TEST_F(AuthGateTest, PingAndQuitWorkBeforeAuthentication) {
    ob::Session s(fd_server_);
    EXPECT_EQ(run(s, "PING"), "PONG\n");
    EXPECT_NE(run(s, "QUIT"), "ERR unauthenticated\n");
}

TEST_F(AuthGateTest, SelectAfterAuthenticationReachesTheEngine) {
    ob::Session s(fd_server_);
    ASSERT_EQ(authenticate(s), "OK AUTH alice\n\n");
    // Write, then read it back. Asserting a *row* rather than the absence of `unauthenticated`:
    // a malformed query and an empty engine both lack that string, so the weaker assertion would
    // pass against a gate that let the command through and an engine that then refused it. The
    // first version of this test did exactly that, and the engine was answering
    // `symbol 'BTCUSD' ... not found`.
    ASSERT_EQ(run(s, "INSERT BTCUSD BINANCE bid 50000 10 1").rfind("OK", 0), 0u);
    // FLUSH between them, because the query engine reads the columnar store and not the live SoA
    // buffer (pitfall 13) - and it makes this a third command passing the gate.
    ASSERT_EQ(run(s, "FLUSH").rfind("OK", 0), 0u);
    const auto response = run(s, "SELECT * FROM 'BTCUSD'.'BINANCE'");
    EXPECT_EQ(response.rfind("OK", 0), 0u) << response;
    EXPECT_NE(response.find("50000"), std::string::npos) << response;
}

TEST_F(AuthGateTest, CompressStillCountsAsTheFirstCommandAfterAuthenticating) {
    // AUTH must not increment the command counter, or enabling authentication would take
    // compression away from every client - and the symptom would be `ERR compress_must_be_first`
    // for a client that did nothing wrong.
    ob::Session s(fd_server_);
    ASSERT_EQ(authenticate(s), "OK AUTH alice\n\n");
    EXPECT_EQ(s.commands_executed(), 0u);
    EXPECT_EQ(run(s, "COMPRESS LZ4"), "OK COMPRESS LZ4\n\n");
}

TEST_F(AuthGateTest, ANewChallengeInvalidatesThePreviousOne) {
    ob::Session s(fd_server_);
    const auto first  = nonce_from(run(s, "AUTH"));
    const auto stale  = ob::auth_response(kSecret, ob::AuthSurface::Client,
                                        ob::AuthRole::Initiator, "alice", first);
    const auto second = nonce_from(run(s, "AUTH"));
    ASSERT_NE(first, second);
    EXPECT_EQ(run(s, "AUTH alice " + stale), "ERR auth_failed\n");
    EXPECT_FALSE(s.authenticated());
}

TEST_F(AuthGateTest, AResponseForAnotherSurfaceDoesNotAuthenticateAClient) {
    // Domain separation, exercised through the gate rather than only through the primitive: the
    // cluster surfaces use the same HMAC with a different label.
    ob::Session s(fd_server_);
    const auto nonce = nonce_from(run(s, "AUTH"));
    const auto wrong_surface =
        ob::auth_response(kSecret, ob::AuthSurface::Replication,
                          ob::AuthRole::Initiator, "alice", nonce);
    EXPECT_EQ(run(s, "AUTH alice " + wrong_surface), "ERR auth_failed\n");
}

TEST_F(AuthGateTest, AMalformedAuthLineIsNotEvenAnAuthCommand) {
    // Shape is the parser's job, so nothing malformed reaches a comparison. A response that is not
    // 64 hex characters makes the line UNKNOWN, which is refused with the parser's own message.
    ob::Session s(fd_server_);
    ASSERT_EQ(run(s, "AUTH").rfind("OK CHALLENGE ", 0), 0u);
    const auto response = run(s, "AUTH alice zzzz");
    EXPECT_NE(response.rfind("ERR ", 0), std::string::npos) << response;
    EXPECT_EQ(response.find("auth_failed"), std::string::npos) << response;
    // And it must not have consumed the challenge, so a client with a bug can retry.
    EXPECT_FALSE(s.pending_nonce().empty());
}

// ── With authentication off ───────────────────────────────────────────────────

TEST_F(AuthGateTest, WithAuthenticationOffAuthIsRefused) {
    // A client configured to authenticate against a server that does not must find out. An OK here
    // would be an assurance with nothing behind it.
    ob::Session s(fd_server_);
    EXPECT_EQ(run_open(s, "AUTH"), "ERR auth_disabled\n");
    EXPECT_EQ(run_open(s, "AUTH alice " + std::string(ob::kAuthHexChars, 'a')),
              "ERR auth_disabled\n");
}

TEST_F(AuthGateTest, WithAuthenticationOffNotOneCommandChanges) {
    ob::Session s(fd_server_);
    EXPECT_EQ(run_open(s, "PING"), "PONG\n");
    const auto select = run_open(s, "SELECT * FROM orderbook");
    EXPECT_EQ(select.find("ERR unauthenticated"), std::string::npos) << select;

    // A fresh session for COMPRESS: PING and SELECT above increment the command counter, and
    // COMPRESS-must-be-first is pre-existing behaviour this change is asserting it did not touch.
    ob::Session fresh(fd_server_);
    EXPECT_EQ(run_open(fresh, "COMPRESS LZ4"), "OK COMPRESS LZ4\n\n");
}
