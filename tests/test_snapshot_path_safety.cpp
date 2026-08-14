// Feature: snapshot path validation
//
// Snapshot file names arrive from the network during replica bootstrap and are
// used to build both the staging path and the destination inside the live data
// directory. These tests pin down that a peer cannot use them to write outside
// those directories, and that files are created with restrictive permissions.
//
// The end-to-end test drives a mock primary through the real bootstrap protocol
// with a traversing path, which is the case that matters: unit tests on the
// validator would pass even if nothing called it.

#include "orderbook/engine.hpp"
#include "orderbook/replication.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace {

// ── Temp dir helper ───────────────────────────────────────────────────────────

struct SafetyTempDir {
    std::string path;

    explicit SafetyTempDir(const std::string& prefix) {
        std::string tmpl =
            (fs::temp_directory_path() / (prefix + "_XXXXXX")).string();
        std::vector<char> buf(tmpl.begin(), tmpl.end());
        buf.push_back('\0');
        const char* dir = ::mkdtemp(buf.data());
        path = dir ? std::string(dir) : std::string();
    }

    ~SafetyTempDir() {
        if (!path.empty()) {
            std::error_code ec;
            fs::remove_all(path, ec);
        }
    }

    const std::string& str() const { return path; }
};

uint16_t alloc_free_port() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return 0;
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
    addr.sin_port        = 0;
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return 0;
    }
    sockaddr_in bound{};
    socklen_t len = sizeof(bound);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&bound), &len);
    const uint16_t port = ::ntohs(bound.sin_port);
    ::close(fd);
    return port;
}

int create_mock_primary(uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
    addr.sin_port        = ::htons(port);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return -1;
    }
    if (::listen(fd, 4) != 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

int accept_with_timeout(int listen_fd, int timeout_ms) {
    timeval tv{};
    tv.tv_sec  = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    ::setsockopt(listen_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    const int fd = ::accept(listen_fd, nullptr, nullptr);
    if (fd >= 0) {
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }
    return fd;
}

std::string recv_line(int fd, int timeout_ms) {
    timeval tv{};
    tv.tv_sec  = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    std::string line;
    char c = 0;
    while (line.size() < 512) {
        const ssize_t n = ::recv(fd, &c, 1, 0);
        if (n <= 0) break;
        if (c == '\n') break;
        line.push_back(c);
    }
    return line;
}

bool send_str(int fd, const std::string& s) {
    size_t sent = 0;
    while (sent < s.size()) {
        const ssize_t n = ::send(fd, s.data() + sent, s.size() - sent, MSG_NOSIGNAL);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

} // namespace

// ── Unit tests: paths that must be accepted ───────────────────────────────────

TEST(SnapshotPathSafety, AcceptsPathsWeActuallyGenerate) {
    // These are the shapes the snapshot writer produces.
    EXPECT_TRUE(ob::is_safe_snapshot_path("wal_000000.bin"));
    EXPECT_TRUE(ob::is_safe_snapshot_path("manifest.json"));
    EXPECT_TRUE(ob::is_safe_snapshot_path("segment_0001/prices.col"));
    EXPECT_TRUE(ob::is_safe_snapshot_path("BTC-USD.BINANCE/segment_0002/qty.col"));
    EXPECT_TRUE(ob::is_safe_snapshot_path("a"));
}

// ── Unit tests: paths that must be rejected ───────────────────────────────────

TEST(SnapshotPathSafety, RejectsTraversal) {
    EXPECT_FALSE(ob::is_safe_snapshot_path(".."));
    EXPECT_FALSE(ob::is_safe_snapshot_path("../x"));
    EXPECT_FALSE(ob::is_safe_snapshot_path("a/../../x"));
    EXPECT_FALSE(ob::is_safe_snapshot_path("a/b/../../../etc/passwd"));
    EXPECT_FALSE(ob::is_safe_snapshot_path("../../../../home/user/.ssh/authorized_keys"));
}

TEST(SnapshotPathSafety, RejectsAbsolutePaths) {
    EXPECT_FALSE(ob::is_safe_snapshot_path("/etc/passwd"));
    EXPECT_FALSE(ob::is_safe_snapshot_path("/"));
    EXPECT_FALSE(ob::is_safe_snapshot_path("/tmp/x"));
}

TEST(SnapshotPathSafety, RejectsDegenerateForms) {
    EXPECT_FALSE(ob::is_safe_snapshot_path(""));
    EXPECT_FALSE(ob::is_safe_snapshot_path("."));
    EXPECT_FALSE(ob::is_safe_snapshot_path("./x"));
    EXPECT_FALSE(ob::is_safe_snapshot_path("a//b"));      // empty component
    EXPECT_FALSE(ob::is_safe_snapshot_path("a/"));        // trailing slash
    EXPECT_FALSE(ob::is_safe_snapshot_path(std::string(300, 'a')));  // too long
}

TEST(SnapshotPathSafety, RejectsUnexpectedCharacters) {
    // Not traversal, but nothing we generate contains these, so they are either
    // a bug or someone probing.
    EXPECT_FALSE(ob::is_safe_snapshot_path("a b"));
    EXPECT_FALSE(ob::is_safe_snapshot_path("a;rm -rf /"));
    EXPECT_FALSE(ob::is_safe_snapshot_path("a$b"));
    EXPECT_FALSE(ob::is_safe_snapshot_path("a\\b"));
    EXPECT_FALSE(ob::is_safe_snapshot_path("a~b"));
    EXPECT_FALSE(ob::is_safe_snapshot_path(std::string("a\0b", 3)));
}

// ── path_stays_within ─────────────────────────────────────────────────────────

TEST(SnapshotPathSafety, StaysWithinAcceptsChild) {
    SafetyTempDir base("staysin");
    ASSERT_FALSE(base.str().empty());
    EXPECT_TRUE(ob::path_stays_within(base.str(), "segment_0001/prices.col"));
}

TEST(SnapshotPathSafety, StaysWithinRejectsEscape) {
    SafetyTempDir base("staysout");
    ASSERT_FALSE(base.str().empty());
    EXPECT_FALSE(ob::path_stays_within(base.str(), "../escaped.bin"));
    EXPECT_FALSE(ob::path_stays_within(base.str(), "a/../../escaped.bin"));
}

TEST(SnapshotPathSafety, StaysWithinIsNotFooledByPrefix) {
    // /tmp/x_data must not count as being inside /tmp/x.
    SafetyTempDir parent("prefix");
    ASSERT_FALSE(parent.str().empty());
    const std::string base    = parent.str() + "/data";
    const std::string sibling = parent.str() + "/data_evil";
    fs::create_directories(base);
    fs::create_directories(sibling);

    EXPECT_FALSE(ob::path_stays_within(base, "../data_evil/x"));
}

TEST(SnapshotPathSafety, StaysWithinFollowsSymlinks) {
    // A symlink inside the base directory pointing out of it must not be usable
    // as a way out. This is why the check canonicalises rather than comparing
    // strings lexically.
    SafetyTempDir parent("symlink");
    ASSERT_FALSE(parent.str().empty());
    const std::string base    = parent.str() + "/base";
    const std::string outside = parent.str() + "/outside";
    fs::create_directories(base);
    fs::create_directories(outside);

    std::error_code ec;
    fs::create_directory_symlink(outside, base + "/link", ec);
    if (ec) {
        GTEST_SKIP() << "cannot create symlink on this filesystem";
    }

    EXPECT_FALSE(ob::path_stays_within(base, "link/pwned.bin"));
}

// ── Property tests ────────────────────────────────────────────────────────────

RC_GTEST_PROP(SnapshotPathSafetyProperty,
              prop_any_path_with_dotdot_component_is_rejected,
              (const std::vector<std::string>& raw_components)) {
    // Build a path from arbitrary components with a `..` inserted somewhere.
    std::vector<std::string> comps;
    for (const auto& c : raw_components) {
        std::string cleaned;
        for (char ch : c) {
            if ((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9')) {
                cleaned.push_back(ch);
            }
        }
        if (!cleaned.empty()) comps.push_back(cleaned);
    }
    RC_PRE(!comps.empty());

    const size_t insert_at = comps.size() / 2;
    comps.insert(comps.begin() + static_cast<long>(insert_at), "..");

    std::string path;
    for (size_t i = 0; i < comps.size(); ++i) {
        if (i) path.push_back('/');
        path += comps[i];
    }

    RC_ASSERT(!ob::is_safe_snapshot_path(path));
}

RC_GTEST_PROP(SnapshotPathSafetyProperty,
              prop_accepted_paths_stay_inside_base,
              (const std::vector<std::string>& raw_components)) {
    // Whatever the validator accepts must also resolve inside the base dir.
    // These two checks are independent, and this is the invariant that ties
    // them together.
    std::vector<std::string> comps;
    for (const auto& c : raw_components) {
        std::string cleaned;
        for (char ch : c) {
            if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
                (ch >= '0' && ch <= '9') || ch == '.' || ch == '_' || ch == '-') {
                cleaned.push_back(ch);
            }
        }
        if (!cleaned.empty()) comps.push_back(cleaned);
    }
    RC_PRE(!comps.empty());

    std::string path;
    for (size_t i = 0; i < comps.size(); ++i) {
        if (i) path.push_back('/');
        path += comps[i];
    }
    RC_PRE(path.size() <= ob::kMaxSnapshotPathLen);
    RC_PRE(ob::is_safe_snapshot_path(path));

    SafetyTempDir base("prop");
    RC_PRE(!base.str().empty());
    RC_ASSERT(ob::path_stays_within(base.str(), path));
}

// ── End-to-end: a hostile primary cannot write outside staging ────────────────

TEST(SnapshotPathSafetyE2E, TraversingSnapshotFileIsRejected) {
    SafetyTempDir tmp("e2e_traversal");
    ASSERT_FALSE(tmp.str().empty());

    const uint16_t port = alloc_free_port();
    ASSERT_NE(port, 0);

    const int listen_fd = create_mock_primary(port);
    ASSERT_GE(listen_fd, 0);

    // The file the hostile primary is trying to plant, one level above the
    // data directory.
    const std::string data_dir  = tmp.str() + "/data";
    const std::string victim    = tmp.str() + "/pwned.txt";
    fs::create_directories(data_dir);

    ob::Engine engine(data_dir, 100'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();

    ob::ReplicationClientConfig cfg;
    cfg.primary_host        = "127.0.0.1";
    cfg.primary_port        = port;
    cfg.state_file          = data_dir + "/repl_state.txt";
    cfg.snapshot_staging_dir = data_dir + "/snapshot_staging";

    ob::ReplicationClient client(cfg, engine);
    client.start();

    const int peer_fd = accept_with_timeout(listen_fd, 5000);
    ASSERT_GE(peer_fd, 0) << "client should connect";

    // REPLICATE handshake from the replica.
    const std::string handshake = recv_line(peer_fd, 3000);
    ASSERT_EQ(handshake.rfind("REPLICATE", 0), 0u) << "got: " << handshake;

    // Push the replica into snapshot bootstrap.
    ASSERT_TRUE(send_str(peer_fd, "ERR WAL_TRUNCATED\n"));

    const std::string request = recv_line(peer_fd, 5000);
    ASSERT_EQ(request, "SNAPSHOT_REQUEST") << "got: " << request;

    // SNAPSHOT_BEGIN <total_bytes> <wal_file_index> <wal_offset> <file_count>
    ASSERT_TRUE(send_str(peer_fd, "SNAPSHOT_BEGIN 5 0 0 1\n"));

    // The attack: a relative path that climbs out of the staging directory.
    ASSERT_TRUE(send_str(peer_fd, "SNAPSHOT_FILE ../../pwned.txt 5 0\n"));
    ASSERT_TRUE(send_str(peer_fd, "EVIL!"));

    // Give the client time to process and reject.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    EXPECT_FALSE(fs::exists(victim))
        << "a peer-supplied path escaped the staging directory: " << victim;
    EXPECT_FALSE(fs::exists(tmp.str() + "/data/../../pwned.txt"));

    client.stop();
    ::close(peer_fd);
    ::close(listen_fd);
    engine.close();
}

// Note on this test's strength: with the validation removed it still passes,
// because the receive path builds the destination by string concatenation
// (`staging_dir + "/" + rel_path`), which turns "/tmp/x" into
// "…/staging//tmp/x" and so happens to contain the damage. It is kept as a
// regression guard: rewriting that concatenation as `fs::path(staging) / rel`
// would make an absolute component replace the whole path and reintroduce the
// hole. The traversal test above is the one that fails without the fix.
TEST(SnapshotPathSafetyE2E, AbsoluteSnapshotFileIsRejected) {
    SafetyTempDir tmp("e2e_absolute");
    ASSERT_FALSE(tmp.str().empty());

    const uint16_t port = alloc_free_port();
    ASSERT_NE(port, 0);
    const int listen_fd = create_mock_primary(port);
    ASSERT_GE(listen_fd, 0);

    const std::string data_dir = tmp.str() + "/data";
    const std::string victim   = tmp.str() + "/absolute_pwned.txt";
    fs::create_directories(data_dir);

    ob::Engine engine(data_dir, 100'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();

    ob::ReplicationClientConfig cfg;
    cfg.primary_host         = "127.0.0.1";
    cfg.primary_port         = port;
    cfg.state_file           = data_dir + "/repl_state.txt";
    cfg.snapshot_staging_dir = data_dir + "/snapshot_staging";

    ob::ReplicationClient client(cfg, engine);
    client.start();

    const int peer_fd = accept_with_timeout(listen_fd, 5000);
    ASSERT_GE(peer_fd, 0);
    ASSERT_EQ(recv_line(peer_fd, 3000).rfind("REPLICATE", 0), 0u);
    ASSERT_TRUE(send_str(peer_fd, "ERR WAL_TRUNCATED\n"));
    ASSERT_EQ(recv_line(peer_fd, 5000), "SNAPSHOT_REQUEST");

    ASSERT_TRUE(send_str(peer_fd, "SNAPSHOT_BEGIN 5 0 0 1\n"));
    ASSERT_TRUE(send_str(peer_fd, "SNAPSHOT_FILE " + victim + " 5 0\n"));
    ASSERT_TRUE(send_str(peer_fd, "EVIL!"));

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    EXPECT_FALSE(fs::exists(victim))
        << "an absolute peer-supplied path was written: " << victim;

    client.stop();
    ::close(peer_fd);
    ::close(listen_fd);
    engine.close();
}

// ── File permissions ──────────────────────────────────────────────────────────

TEST(SnapshotFilePermissions, ReplicationStateFileIsNotWorldAccessible) {
    // Written through save_state(), which must not depend on the process umask.
    SafetyTempDir tmp("perms");
    ASSERT_FALSE(tmp.str().empty());

    const uint16_t port = alloc_free_port();
    ASSERT_NE(port, 0);
    const int listen_fd = create_mock_primary(port);
    ASSERT_GE(listen_fd, 0);

    ob::Engine engine(tmp.str(), 100'000'000ULL, ob::FsyncPolicy::NONE);
    engine.open();

    const std::string state_file = tmp.str() + "/repl_state.txt";

    ob::ReplicationClientConfig cfg;
    cfg.primary_host = "127.0.0.1";
    cfg.primary_port = port;
    cfg.state_file   = state_file;

    // Run with a permissive umask: the point is that the code sets the mode
    // itself rather than inheriting whatever the environment allows.
    const mode_t old_umask = ::umask(0);

    ob::ReplicationClient client(cfg, engine);
    client.start();

    const int peer_fd = accept_with_timeout(listen_fd, 5000);
    ASSERT_GE(peer_fd, 0);
    ASSERT_EQ(recv_line(peer_fd, 3000).rfind("REPLICATE", 0), 0u);

    // Any ACK-triggering traffic causes a state save; a heartbeat is enough.
    ASSERT_TRUE(send_str(peer_fd, "HEARTBEAT 1\n"));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    client.stop();
    ::close(peer_fd);
    ::close(listen_fd);
    engine.close();
    ::umask(old_umask);

    if (!fs::exists(state_file)) {
        GTEST_SKIP() << "state file was not written in this run";
    }

    struct stat st{};
    ASSERT_EQ(::stat(state_file.c_str(), &st), 0);
    const mode_t perms = st.st_mode & 07777;

    EXPECT_EQ(perms & S_IWOTH, 0u) << "state file is world-writable, mode "
                                   << std::oct << perms;
    EXPECT_EQ(perms & S_IROTH, 0u) << "state file is world-readable, mode "
                                   << std::oct << perms;
    EXPECT_EQ(perms & S_IXUSR, 0u) << "state file should not be executable";
}
