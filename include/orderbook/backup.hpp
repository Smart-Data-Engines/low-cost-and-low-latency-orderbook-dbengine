#pragma once

// A node's backup: taken by the server into the directory its configuration names, checked and
// restored offline by `ob_restore` (#34).
//
// What a backup is, on the disk:
//
//     <backup-dir>/<name>/backup.json          the description, written last
//     <backup-dir>/<name>/store/<the data directory's segment files, at their relative paths>
//     <backup-dir>/.partial-<name>/            one being taken, or left by a process that died
//
// The name is the cut's wall-clock time, so that the names sort in the order the backups were
// taken, and a directory without the prefix is complete by construction: it is renamed into place
// only once every file and the description are on the device.

#include "orderbook/sequence_tracker.hpp"
#include "orderbook/snapshot.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace ob {

class Engine;
class MetricsRegistry;

inline constexpr const char* kBackupDescriptionFile = "backup.json";
inline constexpr const char* kBackupStoreDir        = "store";
inline constexpr const char* kBackupPartialPrefix   = ".partial-";

/// What a backup says about itself: `backup.json`, format 1.
struct BackupDescription {
    static constexpr const char* kFormat        = "orderbook-backup";
    static constexpr int         kFormatVersion = 1;

    std::string name;
    std::string engine_version;
    uint64_t    cut_at_ns{0};        // the wall clock at the cut
    uint64_t    finished_at_ns{0};
    std::string role;                // standalone, primary, replica, multi_master
    std::string node_id;             // the coordinator's name for the node; empty without one
    uint16_t    mm_node_id{0};       // the mesh's number for the node; 0 outside a mesh
    uint64_t    wal_identity{0};     // the WAL the cut's position is in
    uint32_t    wal_file_index{0};
    uint64_t    wal_byte_offset{0};
    std::string method;              // linked or copied
    uint64_t    total_bytes{0};
    uint64_t    total_rows{0};
    std::vector<SnapshotFileEntry>            files;
    std::vector<SequenceTracker::VectorEntry> vector;
    bool                                      vector_truncated{false};
    std::vector<SequenceTracker::HeldRanges>  held;
    bool                                      held_truncated{false};
    bool                                      numbering_closed{false};   // #187's marker

    std::string to_json() const;

    /// Parse a description, refusing - with the reason in `error` - another format, another
    /// version, a missing field, a field of the wrong type, a path `backup_path_is_contained()`
    /// refuses, and a path listed twice.
    static bool from_json(std::string_view json, BackupDescription& out, std::string& error);
};

/// `20260928T093000.123Z`: a wall-clock time in nanoseconds, in UTC, to the millisecond.
std::string backup_name_for(uint64_t wall_ns);

/// True for a name `backup_name_for()` produces, with its time in `wall_ns` (to the millisecond)
/// when that is not null.
bool parse_backup_name(std::string_view name, uint64_t* wall_ns);

/// Whether a relative path from a description stays inside the directory it is joined to: not
/// empty, not absolute, and no component empty, "." or "..".
bool backup_path_is_contained(std::string_view rel);

/// Why `backup_dir` cannot hold this node's backups, or empty when it can. Creates the directory
/// when it is missing. Refused: not a directory, not writable, the data or the WAL directory, inside
/// either, or holding either - the engine reads every `meta.json` under its data directory as its
/// own, and a replica's bootstrap removes every directory there but its own.
std::string backup_dir_problem(const std::string& backup_dir, const std::string& data_dir,
                               const std::string& wal_dir);

/// Read `<backup>/backup.json`.
bool read_backup_description(const std::string& backup, BackupDescription& out, std::string& error);

/// Every file the description lists: a regular file (not a symbolic link) of its size and CRC32C
/// under `<backup>/store`. Returns one line per problem; empty is a backup that is whole.
std::vector<std::string> verify_backup(const std::string& backup, const BackupDescription& d);

struct RestoreReport {
    size_t   files{0};
    uint64_t bytes{0};
    uint64_t rows{0};
    double   verify_ms{0};
    double   copy_ms{0};
    double   open_ms{0};
    uint64_t wal_identity{0};        // the restored node's new WAL
    bool     sequence_adopted{false};
};

/// Restore a backup into an empty data directory (and an empty WAL directory, when `wal_dir` is
/// not empty and not the data directory). The whole backup is verified before anything is created;
/// a failure after that says in `error` what is left, and removes nothing.
bool restore_backup(const std::string& backup, const std::string& data_dir,
                    const std::string& wal_dir, RestoreReport& report, std::string& error);

/// Where a backup is, for `BACKUP STATUS`.
struct BackupProgress {
    enum class State { Idle, Running, Done, Failed };
    State       state{State::Idle};
    std::string name;
    std::string phase;               // cut, link, copy, checksum, publish; empty when not running
    std::string method;              // linked, copied; empty until chosen
    std::string error;
    uint64_t    files_total{0};
    uint64_t    files_done{0};
    uint64_t    bytes_total{0};
    uint64_t    bytes_done{0};
    uint64_t    pinned_ms{0};
    uint64_t    elapsed_ms{0};
};

const char* backup_state_name(BackupProgress::State s);

/// The answer to `BACKUP STATUS`: `OK`, then one `key: value` line each for state, name, phase,
/// method, files (done/total), bytes (done/total), pinned_ms, elapsed_ms and error, `-` for a value
/// that is empty, and the blank line.
std::string format_backup_status(const BackupProgress& p);

/// Takes backups of one engine into one directory, one at a time, each on a thread of its own.
///
/// Owned by the server, created after the engine and destroyed before it: a backup holds the
/// engine's pin on its segment files until it has linked or copied them.
class BackupRunner {
public:
    BackupRunner(Engine& engine, std::string backup_dir, MetricsRegistry& registry);
    ~BackupRunner();

    BackupRunner(const BackupRunner&)            = delete;
    BackupRunner& operator=(const BackupRunner&) = delete;

    enum class Start { Started, Running, Bootstrapping };

    /// Begin a backup, from any thread. `name` is the new backup's, or the running one's.
    Start start(std::string& name);

    BackupProgress progress() const;

    /// Stop a running backup - it fails as "stopped", and what it wrote is removed - and wait for
    /// its thread. The wait is at most one file's read or write.
    void stop();

    /// Ask a running backup to stop, without waiting for it: what `stop()` does first, and callable
    /// from the backup's own thread, where waiting for it would wait for ever.
    void request_stop() { stop_.store(true, std::memory_order_release); }

    const std::string& dir() const { return dir_; }

    // Test seams.
    void force_copy_for_test(bool on) { force_copy_.store(on); }
    /// Called on the backup's thread once the cut is taken, before any file is linked or copied.
    void hold_after_cut_for_test(std::function<void()> hook);
    /// The next backup's name, instead of the time's.
    void set_next_name_for_test(std::string name);

private:
    void run(std::string name);
    void fail(const std::string& name, const std::string& partial, const std::string& why);
    void note_existing_backups();
    void set_phase(const char* phase);
    bool stopping() const { return stop_.load(std::memory_order_acquire); }

    Engine&          engine_;
    std::string      dir_;
    MetricsRegistry& registry_;

    mutable std::mutex mtx_;             // progress_, thread_, after_cut_, next_name_
    BackupProgress     progress_;
    std::thread        thread_;
    std::chrono::steady_clock::time_point started_{};
    std::function<void()> after_cut_;
    std::string           next_name_;

    std::atomic<bool> stop_{false};
    std::atomic<bool> force_copy_{false};
};

}  // namespace ob
