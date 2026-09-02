// parse_cli_args: the first tests this function has ever had.
//
// It had none while it silently ignored mistakes. Measured before the rewrite:
//
//   --port abc    terminate called after throwing std::invalid_argument (stoi), core dumped
//   --port        server started anyway, on the default port
//   --prot 5599   server started anyway, on the default port, ignoring the typo and its value
//
// Error paths call std::exit(1), so they are death tests. That is the honest way to pin down a
// function whose contract includes "refuse to start".

#include "orderbook/tcp_server.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace {

/// parse_cli_args takes char*[], so the argv has to be writable and NULL-terminated.
class Argv {
public:
    explicit Argv(std::vector<std::string> args) : storage_(std::move(args)) {
        pointers_.reserve(storage_.size() + 1);
        for (auto& s : storage_) pointers_.push_back(s.data());
        pointers_.push_back(nullptr);
    }
    int argc() const { return static_cast<int>(storage_.size()); }
    char** argv() { return pointers_.data(); }

private:
    std::vector<std::string> storage_;
    std::vector<char*>       pointers_;
};

ob::ServerConfig parse(std::vector<std::string> args) {
    args.insert(args.begin(), "ob_tcp_server");
    Argv a(std::move(args));
    return ob::parse_cli_args(a.argc(), a.argv());
}

}  // namespace

TEST(CliArgs, ParsesTheCommonFlags) {
    const auto config = parse({"--port", "5555", "--data-dir", "/tmp/x",
                               "--max-sessions", "42", "--workers", "3",
                               "--metrics-port", "9100"});

    EXPECT_EQ(config.port, 5555);
    EXPECT_EQ(config.data_dir, "/tmp/x");
    EXPECT_EQ(config.max_sessions, 42);
    EXPECT_EQ(config.worker_threads, 3);
    EXPECT_EQ(config.metrics_port, 9100);
}

TEST(CliArgs, BooleanFlagsTakeNoValue) {
    const auto config = parse({"--read-only", "--replication-compress", "--no-sqpoll",
                               "--port", "5556"});

    EXPECT_TRUE(config.read_only);
    EXPECT_TRUE(config.replication_compress);
    EXPECT_TRUE(config.uring_no_sqpoll);
    EXPECT_EQ(config.port, 5556) << "a boolean flag must not swallow the next argument";
}

TEST(CliArgs, SplitsCoordinatorEndpointsAndDropsEmptyOnes) {
    const auto config = parse({"--coordinator-endpoints",
                               "http://a:2379,http://b:2379,,http://c:2379,"});

    ASSERT_EQ(config.coordinator_endpoints.size(), 3u)
        << "empty entries between commas, and a trailing comma, must not become endpoints";
    EXPECT_EQ(config.coordinator_endpoints[0], "http://a:2379");
    EXPECT_EQ(config.coordinator_endpoints[2], "http://c:2379");
}

TEST(CliArgs, ParsesTheMultiMasterSet) {
    // --coordinator-endpoints is required in this mode: peer discovery goes through etcd, and
    // the parser refuses multi-master without it. Leaving it out of this test exited the whole
    // test binary, which is how that validation got its own death test below.
    const auto config = parse({"--multi-master", "--mm-node-id", "7",
                               "--mm-replication-port", "6001",
                               "--coordinator-endpoints", "http://127.0.0.1:2379",
                               "--anti-entropy-interval-seconds", "5",
                               "--mm-max-peer-send-buffer", "1048576",
                               "--mm-max-catchup-bytes", "2097152"});

    EXPECT_TRUE(config.multi_master);
    EXPECT_EQ(config.mm_node_id, 7);
    EXPECT_EQ(config.mm_replication_port, 6001);
    EXPECT_EQ(config.anti_entropy_interval_sec, 5u);
    EXPECT_EQ(config.mm_max_peer_send_buf_bytes, 1048576u);
    EXPECT_EQ(config.mm_max_catchup_bytes, 2097152u);
}

TEST(CliArgs, AcceptsAValidLogLevel) {
    const auto config = parse({"--log-level", "DEBUG"});
    EXPECT_EQ(config.log_level, "DEBUG");
}

TEST(CliArgs, DefaultsSurviveAnEmptyCommandLine) {
    const auto config = parse({});
    EXPECT_GT(config.port, 0) << "the default port must still be there";
    EXPECT_FALSE(config.multi_master);
    EXPECT_FALSE(config.read_only);
}

// ═══════════════════════════════════════════════════════════════════════════════
// The three mistakes that used to pass silently, and one that crashed
// ═══════════════════════════════════════════════════════════════════════════════

TEST(CliArgsDeath, ANonNumericValueIsRefusedRatherThanThrown) {
    EXPECT_EXIT(parse({"--port", "abc"}), ::testing::ExitedWithCode(1),
                "--port expects a non-negative integer, got 'abc'");
}

TEST(CliArgsDeath, AMissingValueIsRefusedRatherThanIgnored) {
    // This started a server on the default port before, because the guard was
    // `arg == "--port" && i + 1 < argc` and a flag with no value simply fell through.
    EXPECT_EXIT(parse({"--port"}), ::testing::ExitedWithCode(1), "--port requires a value");
}

TEST(CliArgsDeath, AnUnknownFlagIsRefusedRatherThanIgnored) {
    EXPECT_EXIT(parse({"--prot", "5599"}), ::testing::ExitedWithCode(1),
                "unknown argument '--prot'");
}

TEST(CliArgsDeath, AValueOutOfRangeForItsTypeIsRefused) {
    // 99999 does not fit a uint16 port. static_cast would have wrapped it to 34463.
    EXPECT_EXIT(parse({"--port", "99999"}), ::testing::ExitedWithCode(1),
                "--port expects a value in range");
}

TEST(CliArgsDeath, ANegativeValueForAnUnsignedFlagIsRefused) {
    EXPECT_EXIT(parse({"--ttl-hours", "-5"}), ::testing::ExitedWithCode(1),
                "--ttl-hours expects a non-negative integer");
}

TEST(CliArgsDeath, AnInvalidLogLevelIsRefused) {
    EXPECT_EXIT(parse({"--log-level", "LOUD"}), ::testing::ExitedWithCode(1),
                "invalid log level 'LOUD'");
}

TEST(CliArgsDeath, MultiMasterWithoutANodeIdIsRefused) {
    // Pre-existing validation, untested until now.
    EXPECT_EXIT(parse({"--multi-master", "--mm-replication-port", "6001",
                       "--coordinator-endpoints", "http://127.0.0.1:2379"}),
                ::testing::ExitedWithCode(1), "--multi-master requires --mm-node-id");
}

TEST(CliArgsDeath, MultiMasterWithoutCoordinatorEndpointsIsRefused) {
    // Without etcd there is no peer discovery, so the mode cannot work. Found by writing the
    // happy-path test above without endpoints and watching it take the test binary down.
    EXPECT_EXIT(parse({"--multi-master", "--mm-node-id", "1", "--mm-replication-port", "6001"}),
                ::testing::ExitedWithCode(1), "requires --coordinator-endpoints");
}

TEST(CliArgsDeath, MultiMasterAndReadOnlyTogetherAreRefused) {
    EXPECT_EXIT(parse({"--multi-master", "--mm-node-id", "1", "--mm-replication-port", "6001",
                       "--coordinator-endpoints", "http://127.0.0.1:2379", "--read-only"}),
                ::testing::ExitedWithCode(1), "--multi-master is incompatible with --read-only");
}

TEST(CliArgsDeath, AnInvalidFsyncPolicyIsRefused) {
    // Durability is the most consequential setting in a database, and it was hardcoded to
    // `interval` until #33 needed `docs/operations.md` to describe choosing it per storage device.
    // Reading an unrecognised value as the default would mean an operator who asked for `every` and
    // got something weaker finding out from a lost write.
    EXPECT_EXIT(parse({"--fsync-policy", "sometimes"}), ::testing::ExitedWithCode(1),
                "expects every, interval or none");
}

TEST(CliArgs, EachFsyncPolicyNameMaps) {
    EXPECT_EQ(parse({"--fsync-policy", "every"}).fsync_policy, ob::FsyncPolicy::EVERY);
    EXPECT_EQ(parse({"--fsync-policy", "interval"}).fsync_policy, ob::FsyncPolicy::INTERVAL);
    EXPECT_EQ(parse({"--fsync-policy", "none"}).fsync_policy, ob::FsyncPolicy::NONE);
    // The default, stated so a change to it fails a test rather than surprising an operator.
    EXPECT_EQ(parse({}).fsync_policy, ob::FsyncPolicy::INTERVAL);
}
