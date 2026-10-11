// A replica whose store was replaced to follow a stream is joining until its primary says the
// catch-up ended, and does not stand for election meanwhile (#215).
//
// What these hold, on the engine's side of it: the record says which stream, why and since when;
// it is on the disk before anything is replaced and survives both replacements - a discard and a
// snapshot install - and a restart; ending it removes it; a record nobody can read is still a
// record; and a record that cannot be written changes nothing. The order in the replication client
// - the record before the replacement - is held statically at the end, because a crash between the
// two is the case it exists for and no test can stop a process there.

#include "orderbook/data_model.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/types.hpp"

#include <gtest/gtest.h>

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

std::atomic<uint64_t> g_dir_counter{0};

struct TempDir {
    std::string path;
    TempDir() {
        path = (fs::temp_directory_path() /
                ("ob_joining_" + std::to_string(::getpid()) + "_" +
                 std::to_string(g_dir_counter.fetch_add(1, std::memory_order_relaxed))))
                   .string();
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

constexpr uint64_t kNoAutoFlush = 3'600'000'000'000ULL;
constexpr uint64_t kBase        = 1'790'000'000'000'000'000ULL;

std::unique_ptr<ob::Engine> engine_at(const std::string& dir) {
    auto engine = std::make_unique<ob::Engine>(dir, kNoAutoFlush, ob::FsyncPolicy::INTERVAL);
    engine->open();
    return engine;
}

void write(ob::Engine& engine, const char* symbol, uint64_t n) {
    ob::DeltaUpdate delta{};
    std::strncpy(delta.symbol, symbol, sizeof(delta.symbol) - 1);
    std::strncpy(delta.exchange, "EX", sizeof(delta.exchange) - 1);
    delta.timestamp_ns = kBase + n * 1000;
    delta.side         = ob::SIDE_BID;
    delta.n_levels     = 1;
    ob::Level level{};
    level.price = static_cast<int64_t>(n * 10);
    level.qty   = 1;
    level.cnt   = 1;
    ASSERT_EQ(engine.apply_delta(delta, &level), ob::OB_OK);
}

std::string read_file(const std::string& path) {
    std::ifstream in(path);
    std::stringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

/// A gauge's value in the registry's exposition, or -1 when it is not there. The line carries a
/// label set, so the bare name is not the key (pitfall 66).
long gauge(ob::Engine& engine, const std::string& name) {
    std::istringstream in(engine.registry().serialize());
    std::string line;
    while (std::getline(in, line)) {
        if (line.rfind(name, 0) != 0) continue;
        const char next = line.size() > name.size() ? line[name.size()] : '\0';
        if (next != '{' && next != ' ') continue;
        return std::strtol(line.substr(line.rfind(' ') + 1).c_str(), nullptr, 10);
    }
    return -1;
}

/// A snapshot of `from`, staged in `staging` the way a replica stages one.
ob::SnapshotManifest stage_snapshot(ob::Engine& from, const std::string& from_dir,
                                    const std::string& staging) {
    ob::SnapshotManifest m = from.create_snapshot();
    for (const auto& f : m.files) {
        const fs::path dst = fs::path(staging) / f.path;
        fs::create_directories(dst.parent_path());
        fs::copy_file(fs::path(from_dir) / f.path, dst);
    }
    return m;
}

std::string source(const char* relative) {
    return read_file(std::string(OB_SOURCE_DIR) + "/" + relative);
}

/// The body of `name` in `text`: from its definition to the first closing brace at column 0.
std::string function_body(const std::string& text, const std::string& name) {
    const size_t at = text.find(name);
    if (at == std::string::npos) return {};
    const size_t end = text.find("\n}\n", at);
    return text.substr(at, end == std::string::npos ? std::string::npos : end - at);
}

}  // namespace

TEST(JoiningStream, TheRecordSaysWhichStreamWhyAndSince) {
    TempDir dir;
    auto engine = engine_at(dir.path);
    EXPECT_FALSE(engine->joining_stream().has_value()) << "a node that never gave up its store";
    EXPECT_EQ(gauge(*engine, "ob_replica_joining"), 0);

    engine->begin_joining(4242, "discard");

    const auto joining = engine->joining_stream();
    ASSERT_TRUE(joining.has_value());
    EXPECT_EQ(joining->stream_id, 4242u);
    EXPECT_EQ(joining->reason, "discard");
    EXPECT_GT(joining->since_ns, kBase / 2) << "a wall-clock time, not one counted from boot (pitfall 415)";
    EXPECT_EQ(joining->path, dir.path + "/repl_joining.txt");
    EXPECT_EQ(gauge(*engine, "ob_replica_joining"), 1);

    // The file is the record, in lines an older build ignores and an operator can read.
    const std::string text = read_file(dir.path + "/repl_joining.txt");
    EXPECT_EQ(text.rfind("stream_id=4242\nsince_ns=", 0), 0u) << text;
    EXPECT_NE(text.find("\nreason=discard\n"), std::string::npos) << text;

    // STATUS says what the failover monitor reads.
    const auto stats = engine->stats();
    EXPECT_TRUE(stats.joining);
    EXPECT_EQ(stats.joining_stream_id, 4242u);
    EXPECT_EQ(stats.joining_reason, "discard");
    engine->close();
}

TEST(JoiningStream, EndingItRemovesTheRecordAndASecondEndIsNothing) {
    TempDir dir;
    auto engine = engine_at(dir.path);
    engine->begin_joining(7, "snapshot");
    engine->end_joining(7, "the primary said the catch-up ended");
    EXPECT_FALSE(engine->joining_stream().has_value());
    EXPECT_FALSE(fs::exists(dir.path + "/repl_joining.txt"));
    EXPECT_EQ(gauge(*engine, "ob_replica_joining"), 0);
    EXPECT_FALSE(engine->stats().joining);

    engine->end_joining(7, "again");   // nothing to remove: not an error
    EXPECT_FALSE(engine->joining_stream().has_value());
    engine->close();
}

TEST(JoiningStream, TheRecordSurvivesTheDiscardItIsWrittenBefore) {
    // Both replacements remove directories and keep plain files - `repl_state.txt` and this record
    // alike. The order the client uses is record first, so the discard runs with it on the disk.
    TempDir dir;
    auto engine = engine_at(dir.path);
    for (uint64_t n = 0; n < 5; ++n) write(*engine, "OLD", n);
    engine->flush_incremental();

    engine->begin_joining(11, "discard");
    engine->discard_local_data_for_resync();

    const auto joining = engine->joining_stream();
    ASSERT_TRUE(joining.has_value()) << "the discard took the record of the discard with it";
    EXPECT_EQ(joining->stream_id, 11u);
    engine->close();
}

TEST(JoiningStream, TheRecordSurvivesASnapshotInstall) {
    TempDir from_dir, to_dir;
    auto from = engine_at(from_dir.path);
    for (uint64_t n = 0; n < 10; ++n) write(*from, "A", n);
    from->flush_incremental();

    auto to = engine_at(to_dir.path);
    for (uint64_t n = 100; n < 105; ++n) write(*to, "OLD", n);
    to->flush_incremental();

    to->begin_joining(12, "snapshot");
    const std::string staging = to_dir.path + "/snapshot_staging";
    const ob::SnapshotManifest m = stage_snapshot(*from, from_dir.path, staging);
    ASSERT_TRUE(to->install_snapshot(staging, m));

    const auto joining = to->joining_stream();
    ASSERT_TRUE(joining.has_value()) << "the install took the record of the install with it";
    EXPECT_EQ(joining->reason, "snapshot");
    to->close();
    from->close();
}

TEST(JoiningStream, ANodeRestartedWhileJoiningIsStillJoining) {
    TempDir dir;
    {
        auto engine = engine_at(dir.path);
        engine->begin_joining(13, "discard");
        engine->close();
    }
    auto engine = engine_at(dir.path);
    const auto joining = engine->joining_stream();
    ASSERT_TRUE(joining.has_value()) << "a restart forgot that the store it holds is part of a stream";
    EXPECT_EQ(joining->stream_id, 13u);
    EXPECT_EQ(gauge(*engine, "ob_replica_joining"), 1) << "open() reads the record before anything can stand";
    engine->close();
}

TEST(JoiningStream, ARecordNobodyCanReadIsStillARecord) {
    // Not knowing what the record says is not a reason to stand: a node that wrote one was giving up
    // its store.
    TempDir dir;
    auto engine = engine_at(dir.path);
    {
        std::ofstream out(dir.path + "/repl_joining.txt");
        out << "garbage\n";
    }
    const auto joining = engine->joining_stream();
    ASSERT_TRUE(joining.has_value());
    EXPECT_EQ(joining->reason, "unreadable");
    EXPECT_EQ(joining->stream_id, 0u);
    engine->close();
}

TEST(JoiningStream, ARecordThatCannotBeWrittenThrowsAndLeavesNoRecord) {
    // The caller replaces nothing when this throws (requirement 2.5): a node without the record
    // must not be a node without its data. A directory where the temporary file goes makes the
    // write fail for any user, root included.
    TempDir dir;
    auto engine = engine_at(dir.path);
    fs::create_directories(dir.path + "/repl_joining.txt.tmp");
    EXPECT_THROW(engine->begin_joining(14, "discard"), std::runtime_error);
    EXPECT_FALSE(engine->joining_stream().has_value());
    EXPECT_EQ(gauge(*engine, "ob_replica_joining"), 0);
    engine->close();
}

TEST(JoiningStreamStatic, TheRecordIsWrittenBeforeTheStoreIsReplaced) {
    // The order is the whole guarantee - a crash between a discard and its record would leave a node
    // without its data that does not know it - and nothing can stop a process between two lines, so
    // the order is read from the source.
    const std::string repl = source("src/replication.cpp");

    const std::string resolve = function_body(repl, "void ReplicationClient::resolve_stream_identity()");
    ASSERT_FALSE(resolve.empty());
    const size_t record  = resolve.find("engine_.begin_joining(announced, \"discard\")");
    const size_t discard = resolve.find("engine_.discard_local_data_for_resync()");
    ASSERT_NE(record, std::string::npos) << "the discard path no longer records joining";
    ASSERT_NE(discard, std::string::npos);
    EXPECT_LT(record, discard) << "the store is discarded before the record that says so is written";

    const std::string snapshot =
        function_body(repl, "void ReplicationClient::request_and_receive_snapshot()");
    ASSERT_FALSE(snapshot.empty());
    const size_t snap_record = snapshot.find("engine_.begin_joining(");
    const size_t begun       = snapshot.find("bootstrapping_.store(true");
    ASSERT_NE(snap_record, std::string::npos) << "the snapshot path no longer records joining";
    ASSERT_NE(begun, std::string::npos);
    EXPECT_LT(snap_record, begun) << "the bootstrap begins before the record that says so";
}
