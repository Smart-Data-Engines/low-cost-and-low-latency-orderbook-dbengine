#pragma once

// The WAL archive (#34, point-in-time recovery; spec `point-in-time-recovery`).
//
// A backup returns a node to the moment it was taken, so the data a restore can recover is as old
// as the last backup. What happened after it is in the WAL, and retention removes a WAL file as soon
// as a checkpoint claims its rows. With an archive directory set, every WAL file the writer has
// closed is copied there first - `<archive>/<WAL identity>/wal_NNNNNN.bin`, byte for byte - and
// retention removes nothing the archive does not hold. `ob_restore --until` then replays the archive
// from a backup's cut to a chosen moment.
//
// The copy is checked, not trusted: the source is synced first - a rotation leaves its file for the
// next flush tick to sync, and a copy of bytes a power cut could still take from the source would
// differ from it afterwards - its CRC32C is folded as it is read, the copy is synced and renamed into
// place, read back, and refused unless its CRC is the source's. A file that is
// already in the archive is compared the same way and left alone when it matches. One that does
// not match is never overwritten. That is someone else's file, or this lineage's file damaged, and
// archiving stops there, with retention, until an operator looks.
//
// Beside each file goes its sum, `wal_NNNNNN.bin.crc32c`: its size and CRC32C, written once the
// copy has read back, so that a restore - perhaps of an archive an operator copied elsewhere - can
// tell a file that is whole from one that lost its end at a record boundary, which its records alone
// cannot show (#34, 2.3).
//
// It holds no other state: after a start it walks the closed files from the oldest and skips each one
// the archive holds unchanged.

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

namespace ob {

class MetricsRegistry;

/// `<root>/<identity>`: where a WAL lineage's files are archived, the identity in decimal as the
/// `wal_identity` file and `backup.json` give it.
std::string wal_archive_lineage_dir(const std::string& root, uint64_t identity);
/// The sum beside archived file `index`, `wal_NNNNNN.bin.crc32c`.
std::string wal_archive_sum_basename(uint32_t index);
/// What a sum holds: `<size> <crc32c as eight hex digits>` and a newline.
std::string wal_archive_sum_text(uint64_t size, uint32_t crc);
/// Why archived file `index` of `lineage_dir` is not the file its sum describes - no sum, a sum that
/// does not parse, another size or another CRC32C - or empty when it is.
std::string verify_archived_file(const std::string& lineage_dir, uint32_t index);

class WalArchiver {
public:
    /// What one pass did.
    struct Pass {
        size_t archived{0};       ///< files copied into the archive
        size_t present{0};        ///< closed files the archive already held, unchanged
        uint64_t bytes{0};        ///< bytes copied
        size_t pending{0};        ///< closed files left unarchived when it ended
        std::string failure;      ///< why it ended before the last closed file; empty when it did not
        bool refused{false};      ///< a file in the archive with other contents: archiving stops
    };

    /// `wal_dir` is the WAL's directory, `root` the archive's, `identity` the lineage the files are
    /// of. `current_file` says which file the writer is in - the files below it are closed - a
    /// function since every rotation changes it. `registry` may be null.
    WalArchiver(std::string wal_dir, std::string root, uint64_t identity,
                std::function<uint32_t()> current_file, MetricsRegistry* registry);
    ~WalArchiver();

    WalArchiver(const WalArchiver&)            = delete;
    WalArchiver& operator=(const WalArchiver&) = delete;

    /// The thread `ob-wal-archive`: a pass every second, and at once when woken. A refused archive
    /// still passes, to count the failure each time; it copies nothing.
    void start();
    void stop();
    /// A file was closed: archive it now rather than within the second.
    void wake();
    /// The lineage the WAL's files are of from now on. Set by the engine once a new lineage has
    /// removed its predecessor's files, so that a pass never puts an old file in the new directory.
    void set_lineage(uint64_t identity) { identity_.store(identity, std::memory_order_release); }
    uint64_t lineage() const { return identity_.load(std::memory_order_acquire); }

    /// Archive every closed file of lineage `identity` - its index below `before_file` - that the
    /// archive does not hold, oldest first. The thread's pass, and what a new lineage calls before
    /// it removes its predecessor's files.
    Pass archive_closed(uint64_t identity, uint32_t before_file);

    /// The lowest index of a WAL file the archive does not hold yet: retention may remove the files
    /// below it and none from it on. Starts at 0, which holds every file back, until a pass has run.
    uint32_t archived_before() const { return archived_before_.load(std::memory_order_acquire); }
    /// Whether a file in the archive with other contents stopped archiving.
    bool refused() const { return refused_.load(std::memory_order_acquire); }
    /// The directory a lineage's files go to.
    std::string lineage_dir(uint64_t identity) const;

private:
    void run();

    const std::string wal_dir_;
    const std::string root_;
    std::atomic<uint64_t> identity_;
    const std::function<uint32_t()> current_file_;
    MetricsRegistry* const registry_;

    std::atomic<uint32_t> archived_before_{0};
    std::atomic<bool> refused_{false};

    // Each file found in the archive unchanged, or put there, by lineage and name: its size and CRC.
    // A closed file never changes, so it is read once a process rather than once a pass.
    std::mutex pass_mtx_;   // one pass at a time: the thread's and a new lineage's
    std::map<std::pair<uint64_t, std::string>, std::pair<uint64_t, uint32_t>> known_;

    std::mutex mtx_;
    std::condition_variable cv_;
    bool stop_{false};
    bool woken_{false};
    std::thread thread_;
};

}  // namespace ob
