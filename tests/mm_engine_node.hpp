// One multi-master engine driven by hand, for the tests that take its restart apart
// (test_mm_restart_origins.cpp, #179 and #180) and its numbering (test_mm_per_origin_numbering.cpp,
// #184 and #185): records a peer sent, records this node's clients wrote, a crash as a copy of the
// data directory taken while nothing writes, and what a peer would be told.
//
// Each file keeps its own port counter, in its own block of test_ports.hpp, and passes it in.
#pragma once

#include "orderbook/engine.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"
#include "orderbook/version_vector.hpp"
#include "orderbook/wal.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace mm_engine {

namespace fs = std::filesystem;

inline std::atomic<uint64_t> g_dir_counter{0};

struct TempDir {
    std::string path;
    explicit TempDir(const std::string& prefix) {
        auto p = fs::temp_directory_path() /
                 (prefix + std::to_string(g_dir_counter.fetch_add(1, std::memory_order_relaxed)));
        fs::create_directories(p);
        path = p.string();
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
};

constexpr uint64_t kNoAutoFlush = 3'600'000'000'000ULL;   // every tick here is the test's own
constexpr uint16_t kSelf = 1;
constexpr uint16_t kPeer = 2;

inline ob::MultiMasterConfig mm_config(std::atomic<uint16_t>& port) {
    ob::MultiMasterConfig mm{};
    mm.enabled                   = true;
    mm.node_id                   = kSelf;
    mm.replication_port          = port.fetch_add(1, std::memory_order_relaxed);
    mm.compress                  = false;
    mm.max_catchup_bytes         = 1 << 20;
    mm.anti_entropy_interval_sec = 3600;   // out of the way: nothing here is about the timer
    return mm;
}

inline std::unique_ptr<ob::Engine> open_node(std::atomic<uint16_t>& port, const std::string& dir,
                                             ob::FsyncPolicy policy = ob::FsyncPolicy::EVERY,
                                             const std::string& wal_dir = "") {
    auto engine = std::make_unique<ob::Engine>(dir, kNoAutoFlush, policy, ob::ReplicationConfig{},
                                               ob::ReplicationClientConfig{},
                                               ob::FailoverConfig{}, ob::TTLConfig{},
                                               mm_config(port), 512ULL << 20, wal_dir);
    engine->open();
    return engine;
}

/// One record of `levels` levels, each at its own price so that no level loses to another under
/// Last-Writer-Wins: every level is a row, and a row count is what these tests read.
struct Record {
    ob::DeltaUpdate        delta{};
    std::vector<ob::Level> levels;
};

inline Record record(const char* symbol, uint64_t seq, uint64_t ts, uint16_t levels = 1) {
    Record r;
    std::strncpy(r.delta.symbol, symbol, sizeof(r.delta.symbol) - 1);
    std::strncpy(r.delta.exchange, "EX", sizeof(r.delta.exchange) - 1);
    r.delta.sequence_number = seq;
    r.delta.timestamp_ns    = ts;
    r.delta.side            = ob::SIDE_BID;
    r.delta.n_levels        = levels;
    r.levels.resize(levels);
    for (uint16_t i = 0; i < levels; ++i) {
        r.levels[i].price = static_cast<int64_t>(ts % 1'000'000'000ULL) * 1000 + i;
        r.levels[i].qty   = 5;
        r.levels[i].cnt   = 1;
        r.levels[i]._pad  = 0;
    }
    return r;
}

/// A record `origin` wrote, as the mesh delivers it.
inline ob::ob_status_t deliver(ob::Engine& engine, const Record& r, uint16_t origin) {
    ob::HLCTimestamp hlc{};
    hlc.physical_ns = r.delta.timestamp_ns;
    hlc.logical     = 0;
    hlc.node_id     = origin;
    return engine.apply_remote_delta(r.delta, r.levels.data(), origin, hlc);
}

/// A record this node's client wrote, numbered by the engine.
inline void write_own(ob::Engine& engine, const Record& r) {
    ASSERT_EQ(engine.apply_delta_mm(r.delta, r.levels.data()), ob::OB_OK);
}

/// Every row the node holds for `symbol`. Flushed first: a record applied since the last flush
/// has its rows in the pending queue, which a query does not read - so a count taken straight after
/// a redelivery cannot see the duplicate it is there to catch (test_mm_dedup.cpp flushes for the
/// same reason, and the first pass of this file's mutation table is how it was found here).
inline int rows(ob::Engine& engine, const char* symbol) {
    engine.flush_incremental();
    int n = 0;
    const std::string sql = std::string("SELECT * FROM '") + symbol +
                            "'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999";
    const std::string err = engine.execute(sql, [&n](const ob::QueryResult&) { ++n; });
    if (!err.empty() && err.find("NOT_FOUND") == std::string::npos) {
        ADD_FAILURE() << "query error: " << err;
    }
    return n;
}

inline size_t segments_on_disk(const std::string& dir) {
    size_t n = 0;
    std::error_code ec;
    for (auto it = fs::recursive_directory_iterator(dir, ec);
         it != fs::recursive_directory_iterator(); ++it) {
        if (it->is_regular_file(ec) && it->path().filename() == "meta.json") ++n;
    }
    return n;
}

/// The data directory as a crash leaves it; see the top of this file for why a copy is one.
inline void crash_image(const std::string& from, const std::string& to) {
    fs::remove_all(to);
    fs::copy(from, to, fs::copy_options::recursive);
}

/// The frontier this node would state to a peer now for (key, origin), if it states one.
inline std::optional<uint64_t> told(ob::Engine& engine, const std::string& key, uint16_t origin) {
    bool truncated = false;
    const auto entries = engine.export_version_vector(1u << 20, truncated);
    for (const auto& e : entries) {
        if (e.key == key && e.origin == origin) return e.frontier;
    }
    return std::nullopt;
}

/// Every (key, origin) -> frontier this node would state to a peer now.
inline std::map<std::pair<std::string, uint16_t>, uint64_t> told_all(ob::Engine& engine) {
    bool truncated = false;
    std::map<std::pair<std::string, uint16_t>, uint64_t> out;
    for (const auto& e : engine.export_version_vector(1u << 20, truncated)) {
        out[{e.key, e.origin}] = e.frontier;
    }
    return out;
}

/// The version vector the WAL states - what flushes wrote down: the last whole one (one record, or
/// parts put back together, #177) and the changes written after it (#189), as a restart reads it.
inline std::map<std::pair<std::string, uint16_t>, uint64_t> written_down(const std::string& dir) {
    ob::VectorFromWal from_wal;
    ob::WALReplayer replayer(dir);
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) { from_wal.add(ctx); });
    std::map<std::pair<std::string, uint16_t>, uint64_t> out;
    if (!from_wal.vector()) {
        ADD_FAILURE() << "no usable version vector in the WAL";
        return out;
    }
    for (const auto& e : *from_wal.vector()) out[{e.key, e.origin}] = e.frontier;
    return out;
}

/// Whether any version vector - one record, a part of one, or changes to one - is in the WAL under
/// `dir`.
inline bool vector_in_wal(const std::string& dir) {
    bool found = false;
    ob::WALReplayer replayer(dir);
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
        found = found || ctx.header.record_type == ob::WAL_RECORD_VERSION_VECTOR ||
                ctx.header.record_type == ob::WAL_RECORD_VERSION_VECTOR_PART ||
                ctx.header.record_type == ob::WAL_RECORD_VERSION_VECTOR_CHANGES;
    });
    return found;
}

/// The number and the origin of every DELTA record for `symbol` in the WAL, in file order.
inline std::vector<std::pair<uint64_t, uint16_t>> wal_numbers(const std::string& dir,
                                                       const std::string& symbol) {
    std::vector<std::pair<uint64_t, uint16_t>> out;
    ob::WALReplayer replayer(dir);
    replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
        if (ctx.header.record_type != ob::WAL_RECORD_DELTA) return;
        if (ctx.payload_len < sizeof(ob::DeltaUpdate)) return;
        ob::DeltaUpdate d{};
        std::memcpy(&d, ctx.payload, sizeof(d));
        if (symbol != d.symbol) return;
        out.emplace_back(ctx.header.sequence_number, ctx.origin_node_id);
    });
    return out;
}

/// Drop whatever the WAL holds after its last checkpoint: the state a crash leaves when it lands
/// straight after the checkpoint reached the file. Cut at the next record's start, as the replayer
/// reports it, so no header size is assumed. Returns whether anything was removed.
inline bool cut_after_last_checkpoint(const std::string& dir) {
    struct At {
        uint32_t file;
        uint64_t offset;
        uint8_t  type;
    };
    std::vector<At> records;
    {
        ob::WALReplayer replayer(dir);
        replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
            records.push_back({ctx.wal_file_index, ctx.wal_byte_offset, ctx.header.record_type});
        });
    }
    size_t last = records.size();
    for (size_t i = 0; i < records.size(); ++i) {
        if (records[i].type == ob::WAL_RECORD_CHECKPOINT) last = i;
    }
    if (last == records.size() || last + 1 == records.size()) return false;

    const At& cut = records[last + 1];
    for (const auto& entry : fs::directory_iterator(dir)) {
        const std::string name = entry.path().filename().string();
        unsigned index = 0;
        if (name.size() != 14 || std::sscanf(name.c_str(), "wal_%6u.bin", &index) != 1) continue;
        if (index == cut.file) fs::resize_file(entry.path(), cut.offset);
        if (index > cut.file) fs::remove(entry.path());
    }
    return true;
}


}  // namespace mm_engine
