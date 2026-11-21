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
                               "--max-sessions", "42", "--metrics-port", "9100"});

    EXPECT_EQ(config.port, 5555);
    EXPECT_EQ(config.data_dir, "/tmp/x");
    EXPECT_EQ(config.max_sessions, 42);
    EXPECT_EQ(config.metrics_port, 9100);
}

TEST(CliArgs, BooleanFlagsTakeNoValue) {
    // Two booleans rather than three: the third was `--no-sqpoll`, which went with the io_uring
    // transport (#147), and every other boolean brings a rule of its own - `--multi-master` needs a
    // node id and endpoints and refuses `--read-only` - that this test is not about.
    const auto config = parse({"--read-only", "--replication-compress", "--port", "5556"});

    EXPECT_TRUE(config.read_only);
    EXPECT_TRUE(config.replication_compress);
    EXPECT_EQ(config.port, 5556) << "a boolean flag must not swallow the next argument";
}

TEST(CliArgs, BooksAreOnHugePagesUnlessToldOff) {
    EXPECT_TRUE(parse({}).book_huge_pages) << "on by default (#51)";
    EXPECT_FALSE(parse({"--book-huge-pages", "off"}).book_huge_pages);
    EXPECT_TRUE(parse({"--book-huge-pages", "on"}).book_huge_pages);
}

TEST(CliArgsDeath, BookHugePagesTakesOnOrOff) {
    EXPECT_EXIT(parse({"--book-huge-pages", "yes"}), ::testing::ExitedWithCode(1),
                "--book-huge-pages expects on or off, got 'yes'");
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

TEST(CliArgsDeath, WorkersIsRefusedBecauseNothingEverReadIt) {
    // #149: accepted, printed back, documented as "Number of worker threads (default: 4)", and read
    // by nothing since the first commit. Refused by the same rule as any unknown flag rather than
    // kept as a knob that does nothing — an operator tuning it would be tuning nothing.
    EXPECT_EXIT(parse({"--workers", "4"}), ::testing::ExitedWithCode(1),
                "unknown argument '--workers'");
}

TEST(CliArgs, IoThreadsDefaultsToWhatTheProfileChooses) {
    // Stage 4 of #151: the command line's profile is boost, one loop per usable CPU of the machine
    // this runs on - so the expectation is derived from that machine, not written as a number that
    // is true on one runner. eco is the single loop this server always had.
    EXPECT_EQ(parse({}).io_threads, ob::choose_boost(ob::detect_machine(ob::read_system_view())).io_threads);
    EXPECT_EQ(parse({"--profile", "eco"}).io_threads, 1u);
    EXPECT_EQ(parse({"--io-threads", "4"}).io_threads, 4u);
    // Both ends of the range, because a refusal that also refuses a boundary is a different
    // refusal from the one documented, and `docs/cli.md` prints these two numbers.
    EXPECT_EQ(parse({"--io-threads", "1"}).io_threads, 1u);
    EXPECT_EQ(parse({"--io-threads", std::to_string(ob::kMaxIoThreads)}).io_threads,
              ob::kMaxIoThreads);
}

TEST(CliArgsDeath, ZeroIoThreadsIsRefused) {
    // A server that accepts connections and serves none of them.
    EXPECT_EXIT(parse({"--io-threads", "0"}), ::testing::ExitedWithCode(1),
                "--io-threads \\(0\\) must be between 1 and 64");
}

TEST(CliArgsDeath, IoThreadsAboveTheCeilingIsRefused) {
    // One above, so the comparison is pinned rather than the order of magnitude.
    EXPECT_EXIT(parse({"--io-threads", std::to_string(ob::kMaxIoThreads + 1)}),
                ::testing::ExitedWithCode(1), "must be between 1 and 64");
}

TEST(CliArgs, TlsHandshakesHaveAThreadAndTenSecondsUnlessTold) {
    // #265: one thread drives the client port's handshakes off the loops, and a handshake not done in
    // ten seconds is closed; 0 threads drives each on its loop, as every node did before. Both ends of
    // each range, which `docs/cli.md` prints.
    const ob::ServerConfig defaults = parse({});
    EXPECT_EQ(defaults.tls_handshake_threads, 1u);
    EXPECT_EQ(defaults.tls_handshake_timeout_ms, 10'000u);
    EXPECT_EQ(parse({"--profile", "eco"}).tls_handshake_threads, 1u);
    EXPECT_EQ(parse({"--tls-handshake-threads", "0"}).tls_handshake_threads, 0u);
    EXPECT_EQ(parse({"--tls-handshake-threads", "64"}).tls_handshake_threads, 64u);
    EXPECT_EQ(parse({"--tls-handshake-timeout-ms", "100"}).tls_handshake_timeout_ms, 100u);
    EXPECT_EQ(parse({"--tls-handshake-timeout-ms", "600000"}).tls_handshake_timeout_ms, 600'000u);
}

TEST(CliArgsDeath, TlsHandshakeThreadsAboveTheCeilingAreRefused) {
    EXPECT_EXIT(parse({"--tls-handshake-threads", "65"}), ::testing::ExitedWithCode(1),
                "--tls-handshake-threads \\(65\\) must be between 0 and 64");
}

TEST(CliArgsDeath, ATlsHandshakeTimeoutPastEitherEndIsRefused) {
    // One past each end, so each comparison is pinned rather than the order of magnitude. A timeout of
    // 0 would close every connection before its ClientHello; one of hours would hold a place in
    // --max-sessions for a client that never sends one.
    EXPECT_EXIT(parse({"--tls-handshake-timeout-ms", "99"}), ::testing::ExitedWithCode(1),
                "--tls-handshake-timeout-ms \\(99\\) must be between 100 and 600000");
    EXPECT_EXIT(parse({"--tls-handshake-timeout-ms", "600001"}), ::testing::ExitedWithCode(1),
                "--tls-handshake-timeout-ms \\(600001\\) must be between 100 and 600000");
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

// ═══════════════════════════════════════════════════════════════════════════════
// #124: the WAL rotation threshold, and both ends of its refusal
// ═══════════════════════════════════════════════════════════════════════════════

TEST(CliArgs, ParsesTheWalRotationThreshold) {
    EXPECT_EQ(parse({"--wal-rotate-bytes", "1048576"}).wal_rotate_bytes, 1048576u);
    // The default, stated so a change to it fails a test rather than surprising an operator: it
    // decides what a crash replays, what a reconnecting replica may have to scan, and the
    // granularity retention can free.
    EXPECT_EQ(parse({}).wal_rotate_bytes, 512ULL << 20);
}

TEST(CliArgs, BothEndsOfTheWalRotationRangeAreAccepted) {
    // The boundary values themselves, because a refusal that also refuses the boundary is a
    // different refusal from the one documented - and `docs/cli.md` prints these two numbers.
    EXPECT_EQ(parse({"--wal-rotate-bytes", std::to_string(ob::MIN_WAL_ROTATE_THRESHOLD)})
                  .wal_rotate_bytes,
              ob::MIN_WAL_ROTATE_THRESHOLD);
    EXPECT_EQ(parse({"--wal-rotate-bytes", std::to_string(ob::MAX_WAL_ROTATE_THRESHOLD)})
                  .wal_rotate_bytes,
              ob::MAX_WAL_ROTATE_THRESHOLD);
}

TEST(CliArgsDeath, AWalRotationThresholdBelowOneRecordIsRefused) {
    // Below one maximal record a single write fills a file on its own, so every write rotates: one
    // WAL file per record, and a retention pass that walks all of them every flush tick. The
    // engine's own unit tests do use a 512-byte threshold - on a `WALWriter` built directly, which
    // is the distinction this refusal draws rather than contradicts.
    EXPECT_EXIT(parse({"--wal-rotate-bytes", "512"}), ::testing::ExitedWithCode(1),
                "must be between");
}

TEST(CliArgsDeath, AWalRotationThresholdOneByteUnderTheFloorIsRefused) {
    // One below, so the comparison is pinned rather than the order of magnitude.
    EXPECT_EXIT(parse({"--wal-rotate-bytes", std::to_string(ob::MIN_WAL_ROTATE_THRESHOLD - 1)}),
                ::testing::ExitedWithCode(1), "one maximal record");
}

TEST(CliArgsDeath, AWalRotationThresholdAboveTheCeilingIsRefused) {
    // The offset half of a WAL position is 32 bits so the pair reads as one coherent value (#85).
    // `WALWriter` throws for this too; this is the refusal an operator sees, naming the flag they
    // typed. Refused rather than clamped: an operator who asked for eight gigabyte files should not
    // silently get two and find out during a recovery.
    EXPECT_EXIT(parse({"--wal-rotate-bytes", std::to_string(ob::MAX_WAL_ROTATE_THRESHOLD + 1)}),
                ::testing::ExitedWithCode(1), "2 GiB");
}

// ═══════════════════════════════════════════════════════════════════════════════
// #203: the addresses the client, replication and mesh ports listen on
// ═══════════════════════════════════════════════════════════════════════════════

TEST(CliArgs, EachListenerTakesTheAddressItsFlagNames) {
    const auto config = parse({"--bind", "127.0.0.1", "--replication-bind", "10.0.0.7",
                               "--mm-bind", "192.168.1.2"});
    EXPECT_EQ(config.bind, "127.0.0.1");
    EXPECT_EQ(config.replication_bind, "10.0.0.7");
    EXPECT_EQ(config.mm_bind, "192.168.1.2");
}

TEST(CliArgs, WithoutTheFlagsEveryListenerTakesEveryInterface) {
    const auto config = parse({});
    EXPECT_TRUE(config.bind.empty());
    EXPECT_TRUE(config.replication_bind.empty());
    EXPECT_TRUE(config.mm_bind.empty());
}

TEST(CliArgsDeath, AnAddressThatDoesNotParseIsARefusalNotEveryInterface) {
    // Listening on 0.0.0.0 because a typed address did not parse is the opposite of what was asked.
    EXPECT_EXIT(parse({"--bind", "127.0.0.256"}), ::testing::ExitedWithCode(1),
                "--bind expects an IPv4 or IPv6 address such as 127.0.0.1 or ::1, or .* for every "
                "interface, got '127.0.0.256'");
    EXPECT_EXIT(parse({"--replication-bind", "localhost"}), ::testing::ExitedWithCode(1),
                "--replication-bind expects an IPv4 or IPv6 address");
    EXPECT_EXIT(parse({"--mm-bind", ""}), ::testing::ExitedWithCode(1),
                "--mm-bind expects an IPv4 or IPv6 address");
    EXPECT_EXIT(parse({"--bind", "::1:"}), ::testing::ExitedWithCode(1),
                "--bind expects an IPv4 or IPv6 address such as 127.0.0.1 or ::1, or .* for every "
                "interface, got '::1:'");
}

TEST(CliArgs, AnIPv6AddressIsABindAddress) {
    // Both spellings: bare, as `ip` prints it, and in the brackets an address with a port carries.
    EXPECT_EQ(parse({"--bind", "::1"}).bind, "::1");
    EXPECT_EQ(parse({"--replication-bind", "[::1]"}).replication_bind, "[::1]");
    EXPECT_EQ(parse({"--mm-bind", "fd00::7"}).mm_bind, "fd00::7");
}

// ── #220: what the decoded columns may hold between queries ──────────────────────────────

TEST(CliArgs, TheDecodedColumnsBudgetIsInMiBAndZeroIsAccepted) {
    EXPECT_EQ(parse({}).decoded_cache_mb, 256u) << "the default the documents name";
    EXPECT_EQ(parse({"--decoded-cache-mb", "0"}).decoded_cache_mb, 0u) << "0 holds none";
    EXPECT_EQ(parse({"--decoded-cache-mb", "4096"}).decoded_cache_mb, 4096u);
}

TEST(CliArgsDeath, ADecodedColumnsBudgetThatIsNotAWholeNumberIsRefused) {
    EXPECT_EXIT(parse({"--decoded-cache-mb", "-1"}), ::testing::ExitedWithCode(1),
                "--decoded-cache-mb expects a non-negative integer");
    EXPECT_EXIT(parse({"--decoded-cache-mb", "1.5"}), ::testing::ExitedWithCode(1),
                "--decoded-cache-mb expects a non-negative integer");
    EXPECT_EXIT(parse({"--decoded-cache-mb", "4294967296"}), ::testing::ExitedWithCode(1),
                "--decoded-cache-mb expects a value in range");
}

// ── #52: a Unix socket for clients on this host ─────────────────────────────────────────────

TEST(CliArgs, AUnixSocketIsAPathAndItsModeIsOctal) {
    const auto none = parse({});
    EXPECT_TRUE(none.unix_socket.empty()) << "no socket unless asked for";
    EXPECT_EQ(none.unix_socket_mode, 0660u) << "the default the documents name";
    const auto config = parse({"--unix-socket", "/run/orderbook/ob.sock", "--unix-socket-mode", "0600"});
    EXPECT_EQ(config.unix_socket, "/run/orderbook/ob.sock");
    EXPECT_EQ(config.unix_socket_mode, 0600u);
    EXPECT_EQ(parse({"--unix-socket-mode", "777"}).unix_socket_mode, 0777u) << "the leading zero is optional";
    EXPECT_EQ(parse({"--unix-socket-mode", "0"}).unix_socket_mode, 0u);
}

TEST(CliArgsDeath, AUnixSocketPathTooLongForASocketIsRefused) {
    const std::string longest = "/" + std::string(106, 'x');
    EXPECT_EQ(parse({"--unix-socket", longest}).unix_socket, longest) << "107 bytes fit";
    EXPECT_EXIT(parse({"--unix-socket", longest + "x"}), ::testing::ExitedWithCode(1),
                "--unix-socket: the unix socket path '/x+' is 108 bytes, and a unix socket's path holds at most 107");
    EXPECT_EXIT(parse({"--unix-socket", ""}), ::testing::ExitedWithCode(1), "--unix-socket: a unix socket's path is empty");
}

TEST(CliArgsDeath, AUnixSocketModeThatIsNotAnOctalModeIsRefused) {
    for (const char* bad : {"0800", "1000", "rw-rw----", "", "0x1b6", "-1"}) {
        EXPECT_EXIT(parse({"--unix-socket-mode", bad}), ::testing::ExitedWithCode(1),
                    "--unix-socket-mode expects an octal mode from 0 to 0777")
            << bad;
    }
}

// ── #256: a listener with no secret and no address listens on this host only ───────────────────

TEST(ListenAddress, AnAddressGivenIsTheAddressWithOrWithoutASecret) {
    for (const bool authenticates : {false, true}) {
        const auto at = ob::listen_address("10.0.0.7", authenticates, "bind", "auth-secret-file");
        EXPECT_EQ(at.address, "10.0.0.7");
        EXPECT_EQ(at.reason, "--bind 10.0.0.7");
    }
    EXPECT_EQ(ob::listen_address("::", false, "bind", "auth-secret-file").address, "::")
        << "an IPv6 literal is IPv6 alone (#236), with or without a secret";
}

TEST(ListenAddress, AStarIsEveryInterfaceOfBothFamiliesWithoutASecret) {
    // The way back to the listener an empty flag gave before #256, which no literal says.
    const auto at = ob::listen_address("*", false, "bind", "auth-secret-file");
    EXPECT_EQ(at.address, "") << "empty is every interface of both families to net::listen_on()";
    EXPECT_EQ(at.reason, "every interface: --bind *");
    EXPECT_EQ(ob::client_listen_address(parse({"--bind", "*"})).address, "");
    EXPECT_EQ(ob::replication_listen_address(parse({"--replication-bind", "*"})).address, "");
    EXPECT_EQ(ob::mesh_listen_address(parse({"--mm-bind", "*"})).address, "");
}

TEST(ListenAddress, WithASecretAndNoAddressItIsEveryInterface) {
    const auto at = ob::listen_address("", true, "bind", "auth-secret-file");
    EXPECT_EQ(at.address, "") << "empty is every interface of both families to net::listen_on()";
    EXPECT_EQ(at.reason, "every interface: --auth-secret-file is given and --bind is not");
}

TEST(ListenAddress, WithNeitherItIsThisHostOnly) {
    const auto at = ob::listen_address("", false, "bind", "auth-secret-file");
    EXPECT_EQ(at.address, "127.0.0.1");
    EXPECT_EQ(at.reason, "this host only: no --auth-secret-file and no --bind (#256)");
}

TEST(ListenAddress, EachListenerTakesItsOwnFlagAndItsOwnSecret) {
    const auto none = parse({});
    EXPECT_EQ(ob::client_listen_address(none).address, "127.0.0.1");
    EXPECT_EQ(ob::replication_listen_address(none).address, "127.0.0.1");
    EXPECT_EQ(ob::mesh_listen_address(none).address, "127.0.0.1");

    // A client secret opens the client port and nothing else: the node links have their own secret.
    auto clients = none;
    clients.auth_secret_file = "/etc/orderbook/secrets/clients";
    EXPECT_EQ(ob::client_listen_address(clients).address, "");
    EXPECT_EQ(ob::replication_listen_address(clients).address, "127.0.0.1");
    EXPECT_EQ(ob::mesh_listen_address(clients).address, "127.0.0.1");

    auto cluster = none;
    cluster.cluster_secret_file = "/etc/orderbook/secrets/cluster";
    EXPECT_EQ(ob::client_listen_address(cluster).address, "127.0.0.1");
    EXPECT_EQ(ob::replication_listen_address(cluster).address, "");
    EXPECT_EQ(ob::mesh_listen_address(cluster).address, "");

    const auto bound = parse({"--bind", "10.0.0.1", "--replication-bind", "10.0.0.2", "--mm-bind", "::1"});
    EXPECT_EQ(ob::client_listen_address(bound).address, "10.0.0.1");
    EXPECT_EQ(ob::replication_listen_address(bound).address, "10.0.0.2");
    EXPECT_EQ(ob::mesh_listen_address(bound).address, "::1");
    EXPECT_EQ(ob::replication_listen_address(bound).reason, "--replication-bind 10.0.0.2");
}

TEST(ListenAddress, LoopbackIsTheLoopbackOfEitherFamilyOrItsName) {
    for (const char* host : {"127.0.0.1", "127.255.0.9", "::1", "[::1]", "localhost", "::ffff:127.0.0.1"}) {
        EXPECT_TRUE(ob::is_loopback_host(host)) << host;
    }
    for (const char* host : {"10.0.0.1", "0.0.0.0", "::", "128.0.0.1", "126.255.255.255",
                             "::ffff:10.0.0.1", "example.com", "", "localhost.example.com"}) {
        EXPECT_FALSE(ob::is_loopback_host(host)) << host;
    }
}

namespace {

/// A node announced through a coordinator, as an HA cluster across hosts configures one.
ob::ServerConfig announced_at(const std::string& host) {
    auto c = parse({});
    c.coordinator_endpoints = {"http://127.0.0.1:2379"};
    c.advertise_host = host;
    return c;
}

}  // namespace

TEST(ListenRefusal, ANodeNotAnnouncedOrAnnouncedAtLoopbackIsNotRefused) {
    auto alone = parse({});
    alone.advertise_host = "10.0.0.5";
    EXPECT_EQ(ob::listen_refusal(alone), "") << "without a coordinator nobody is told where it is";
    EXPECT_EQ(ob::listen_refusal(announced_at("127.0.0.1")), "");
    EXPECT_EQ(ob::listen_refusal(announced_at("localhost")), "");
    // Loopback as is_loopback_host() reads it, which a predicate of its own in the server, matching
    // a "127." prefix, did not: the refusal and the test of the rule read the same function.
    EXPECT_EQ(ob::listen_refusal(announced_at("::ffff:127.0.0.1")), "");
    EXPECT_EQ(ob::listen_refusal(announced_at("[::1]")), "");
    EXPECT_NE(ob::listen_refusal(announced_at("127.example.com")), "") << "a name, not an address";
}

TEST(ListenRefusal, AClientPortOnLoopbackByTheRuleIsRefusedWhereTheNodeIsAnnounced) {
    const std::string why = ob::listen_refusal(announced_at("10.0.0.5"));
    EXPECT_EQ(why, "--advertise-host 10.0.0.5 announces this node to clients and peers, but with no "
                   "--auth-secret-file and no --bind its client port listens on 127.0.0.1 only (#256). "
                   "Give --auth-secret-file (recommended), or --bind with the address to listen on");
}

TEST(ListenRefusal, ANodeLinkOnLoopbackByTheRuleIsRefusedWhereTheNodeIsAnnounced) {
    auto replica = announced_at("10.0.0.5");
    replica.auth_secret_file = "/etc/orderbook/secrets/clients";
    replica.replication_port = 9091;
    const std::string why = ob::listen_refusal(replica);
    EXPECT_NE(why.find("its replication port listens on 127.0.0.1 only"), std::string::npos) << why;
    EXPECT_NE(why.find("Give --cluster-secret-file (recommended), or --replication-bind"), std::string::npos)
        << why;

    auto mesh = announced_at("10.0.0.5");
    mesh.auth_secret_file = "/etc/orderbook/secrets/clients";
    mesh.multi_master = true;
    mesh.replication_port = 9091;   // not listened on in multi-master mode, so not asked about
    const std::string mesh_why = ob::listen_refusal(mesh);
    EXPECT_NE(mesh_why.find("its multi-master port listens on 127.0.0.1 only"), std::string::npos)
        << mesh_why;
    EXPECT_NE(mesh_why.find("--mm-bind"), std::string::npos) << mesh_why;
}

TEST(ListenRefusal, ASecretOrAnAddressNamedIsNotRefused) {
    auto secrets = announced_at("10.0.0.5");
    secrets.auth_secret_file = "/etc/orderbook/secrets/clients";
    secrets.cluster_secret_file = "/etc/orderbook/secrets/cluster";
    secrets.replication_port = 9091;
    EXPECT_EQ(ob::listen_refusal(secrets), "");

    // 127.0.0.1 named by the operator is their choice - behind a proxy on this host, say.
    auto named = announced_at("10.0.0.5");
    named.bind = "127.0.0.1";
    named.replication_bind = "10.0.0.5";
    named.replication_port = 9091;
    EXPECT_EQ(ob::listen_refusal(named), "");

    // A replication port that is not listened on is not asked about.
    auto no_replication = announced_at("10.0.0.5");
    no_replication.auth_secret_file = "/etc/orderbook/secrets/clients";
    no_replication.replication_port = 0;
    EXPECT_EQ(ob::listen_refusal(no_replication), "");
}

TEST(CliArgsDeath, ANodeAnnouncedWhereItsClientPortDoesNotListenIsRefused) {
    EXPECT_EXIT(parse({"--coordinator-endpoints", "http://127.0.0.1:2379", "--advertise-host", "192.0.2.10"}),
                ::testing::ExitedWithCode(1),
                "--advertise-host 192.0.2.10 announces this node to clients and peers, but with no "
                "--auth-secret-file and no --bind its client port listens on 127.0.0.1 only");
}
