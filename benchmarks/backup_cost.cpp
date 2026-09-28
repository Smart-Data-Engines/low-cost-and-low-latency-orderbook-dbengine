// What a backup and a restore cost, on the disk this runs on (#34).
//
//   backup_cost <dir> <symbols> <updates-per-symbol> <levels> <runs>
//
// Fills a store in <dir>/data - `symbols` symbols, each written `updates-per-symbol` times with
// `levels` levels, flushed as the engine's own tick flushes, merged as it merges - and then, `runs`
// times each: a linked backup, a copied one (the runner's seam, on the same filesystem, so the copy
// is the real cost of moving the bytes on this device), and a restore of a copied backup into an
// empty directory with the backup's pages dropped from the page cache first, so the check reads the
// device as a restore after a loss would. One JSON line per measurement; the store's shape first.
//
// Compiled, deliberately not run in CI: it writes gigabytes, and its numbers are for the record in
// evidence/, not for a threshold.

#include "orderbook/backup.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/logger.hpp"
#include "orderbook/types.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {

double ms_since(std::chrono::steady_clock::time_point t) {
    return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t).count();
}

/// Drop the backup's pages from the page cache: what a restore after a loss finds is the device.
void drop_cached(const std::string& dir) {
    for (const auto& e : fs::recursive_directory_iterator(dir)) {
        if (!e.is_regular_file()) continue;
        const int fd = ::open(e.path().c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) continue;
        (void)::posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
        ::close(fd);
    }
}

ob::BackupProgress take(ob::Engine& engine, const std::string& dir, bool copy, std::string& name) {
    ob::BackupRunner runner(engine, dir, engine.registry());
    runner.force_copy_for_test(copy);
    if (runner.start(name) != ob::BackupRunner::Start::Started) {
        std::fprintf(stderr, "backup did not start\n");
        std::exit(1);
    }
    for (;;) {
        const ob::BackupProgress p = runner.progress();
        if (p.state != ob::BackupProgress::State::Running) return p;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 6) {
        std::fprintf(stderr, "usage: %s <dir> <symbols> <updates-per-symbol> <levels> <runs>\n",
                     argv[0]);
        return 2;
    }
    const std::string root = argv[1];
    const int symbols = std::atoi(argv[2]);
    const int updates = std::atoi(argv[3]);
    const int levels = std::atoi(argv[4]);
    const int runs = std::atoi(argv[5]);
    ob::StructuredLogger::instance().set_level(ob::LogLevel::WARN);
    try {
        fs::create_directories(root);
        const std::string data = root + "/data";
        ob::Engine engine(data, 100'000'000ULL, ob::FsyncPolicy::INTERVAL);
        engine.open();

        // The fill: one update per symbol in turn, the tick sealing and merging as it would.
        const auto t_fill = std::chrono::steady_clock::now();
        std::vector<ob::Level> lv(static_cast<size_t>(levels));
        const uint64_t base = 1'790'000'000'000'000'000ULL;
        for (int u = 0; u < updates; ++u) {
            for (int s = 0; s < symbols; ++s) {
                ob::DeltaUpdate d{};
                std::snprintf(d.symbol, sizeof(d.symbol), "S%04d", s);
                std::strncpy(d.exchange, "EX", sizeof(d.exchange) - 1);
                d.timestamp_ns = base + static_cast<uint64_t>(u) * 1'000'000ULL + static_cast<uint64_t>(s);
                d.side = (u & 1) ? ob::SIDE_ASK : ob::SIDE_BID;
                d.n_levels = static_cast<uint16_t>(levels);
                for (int i = 0; i < levels; ++i) {
                    lv[static_cast<size_t>(i)].price = 1'000'000 + (u % 97) * 10 + i;
                    lv[static_cast<size_t>(i)].qty = static_cast<uint64_t>(1 + (u + i) % 50);
                    lv[static_cast<size_t>(i)].cnt = 1;
                }
                if (engine.apply_delta(d, lv.data()) != ob::OB_OK) {
                    std::fprintf(stderr, "a write was refused\n");
                    return 1;
                }
            }
        }
        engine.flush_incremental();
        // Merges finish in the ticks after the last seal: wait until nothing waits for removal and
        // the segment count holds still for a second.
        size_t last = 0;
        for (int stable = 0; stable < 10;) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            const size_t now = engine.stats().segment_count;
            stable = (now == last && engine.registry().gauge_value("ob_segments_awaiting_removal") == 0)
                         ? stable + 1 : 0;
            last = now;
        }
        const auto shape = engine.create_snapshot_with_sequence_state(ob::SnapshotChecksums::Skip);
        std::printf("{\"what\":\"store\",\"rows\":%zu,\"bytes\":%zu,\"files\":%zu,\"segments\":%zu,"
                    "\"fill_ms\":%.0f}\n",
                    shape.manifest.total_rows, shape.manifest.total_bytes, shape.manifest.files.size(),
                    last, ms_since(t_fill));
        std::fflush(stdout);

        for (int r = 0; r < runs; ++r) {
            for (const bool copy : {false, true}) {
                const std::string dir = root + (copy ? "/backups-copied" : "/backups-linked");
                fs::create_directories(dir);
                std::string name;
                const ob::BackupProgress p = take(engine, dir, copy, name);
                std::printf("{\"what\":\"backup\",\"method\":\"%s\",\"run\":%d,\"state\":\"%s\","
                            "\"files\":%llu,\"bytes\":%llu,\"cut_ms\":%llu,\"pinned_ms\":%llu,"
                            "\"total_ms\":%llu,\"error\":\"%s\"}\n",
                            p.method.c_str(), r, ob::backup_state_name(p.state),
                            static_cast<unsigned long long>(p.files_total),
                            static_cast<unsigned long long>(p.bytes_total),
                            static_cast<unsigned long long>(p.cut_ms),
                            static_cast<unsigned long long>(p.pinned_ms),
                            static_cast<unsigned long long>(p.elapsed_ms), p.error.c_str());
                std::fflush(stdout);
                if (copy) {
                    const std::string backup = dir + "/" + name;
                    drop_cached(backup);
                    const std::string target = root + "/restored";
                    ob::RestoreReport report;
                    std::string error;
                    const auto t = std::chrono::steady_clock::now();
                    const bool ok = ob::restore_backup(backup, target, "", report, error);
                    const double total = ms_since(t);
                    std::printf("{\"what\":\"restore\",\"run\":%d,\"ok\":%s,\"files\":%zu,\"bytes\":%llu,"
                                "\"verify_ms\":%.0f,\"copy_ms\":%.0f,\"open_ms\":%.0f,\"total_ms\":%.0f,"
                                "\"error\":\"%s\"}\n",
                                r, ok ? "true" : "false", report.files,
                                static_cast<unsigned long long>(report.bytes), report.verify_ms,
                                report.copy_ms, report.open_ms, total, error.c_str());
                    std::fflush(stdout);
                    fs::remove_all(target);
                }
                fs::remove_all(dir);
            }
        }
        engine.close();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "backup_cost: %s\n", e.what());
        return 1;
    }
    return 0;
}
