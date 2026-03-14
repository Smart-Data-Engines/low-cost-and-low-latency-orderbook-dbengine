#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

#include "orderbook/data_model.hpp"

namespace ob {

// ── Record types ──────────────────────────────────────────────────────────────
inline constexpr uint8_t WAL_RECORD_DELTA    = 1;
inline constexpr uint8_t WAL_RECORD_SNAPSHOT = 2;
inline constexpr uint8_t WAL_RECORD_GAP      = 3;
inline constexpr uint8_t WAL_RECORD_ROTATE   = 4;

// ── WALRecord ─────────────────────────────────────────────────────────────────
// Fixed-size header written before each payload.
// The payload immediately follows this header in the file.
//
// Layout (24 bytes):
//   sequence_number : uint64  — sequence number of the update
//   timestamp_ns    : uint64  — nanosecond-precision Unix timestamp
//   checksum        : uint32  — CRC32C of the payload bytes
//   payload_len     : uint16  — length of the payload in bytes
//   record_type     : uint8   — DELTA=1, SNAPSHOT=2, GAP=3, ROTATE=4
//   _pad            : uint8   — reserved, must be zero
struct WALRecord {
    uint64_t sequence_number;
    uint64_t timestamp_ns;
    uint32_t checksum;    // CRC32C of the payload
    uint16_t payload_len;
    uint8_t  record_type; // WAL_RECORD_DELTA / SNAPSHOT / GAP / ROTATE
    uint8_t  _pad;
};

static_assert(sizeof(WALRecord) == 24, "WALRecord size mismatch");

// ── WALWriter ─────────────────────────────────────────────────────────────────
// Append-only WAL writer.  Files are named wal_000000.bin, wal_000001.bin, …
// in the given directory.
//
// append() serialises a DeltaUpdate + its Level array into a DELTA record,
// computes CRC32C of the payload, writes the WALRecord header + payload
// atomically, and calls fsync.
//
// rotate() is called automatically when written_ >= rotate_threshold_.
// It writes a ROTATE record to the current file, closes it, and opens the
// next numbered file.
//
// flush() calls fsync on the current file descriptor.
class WALWriter {
public:
    explicit WALWriter(std::string_view dir,
                       size_t rotate_threshold_bytes = 512ULL << 20);
    ~WALWriter();

    // Non-copyable, non-movable (owns a file descriptor).
    WALWriter(const WALWriter&)            = delete;
    WALWriter& operator=(const WALWriter&) = delete;

    /// Append a DELTA record for the given update + levels.
    /// Does NOT fsync — call sync() explicitly or rely on group commit.
    /// Automatically rotates if the threshold is exceeded after the write.
    void append(const DeltaUpdate& update, const Level* levels);

    /// Write a GAP record (called by the engine when a sequence gap is detected).
    void append_gap(uint64_t sequence_number, uint64_t timestamp_ns);

    /// Force rotation: write ROTATE record, close current file, open next.
    void rotate();

    /// fsync the current file (group commit boundary).
    void flush();

    /// Sync the WAL to disk. Alias for flush() — explicit group commit point.
    void sync();

    /// Number of records written since last sync.
    size_t pending_sync_count() const { return pending_sync_; }

private:
    int         fd_;
    size_t      written_;
    size_t      rotate_threshold_;
    std::string dir_;
    uint32_t    file_index_;
    size_t      pending_sync_{0};

    // Pre-allocated write buffer to avoid per-record heap allocations.
    std::vector<uint8_t> write_buf_;

    /// Open (or create) the WAL file for file_index_.
    void open_current();

    /// Write a complete record (header + payload). Does NOT fsync.
    void write_record(const WALRecord& hdr, const void* payload, size_t payload_len);
};

// ── WALReplayer ───────────────────────────────────────────────────────────────
// Scans all wal_*.bin files in the directory in order, reads each WALRecord
// header, verifies the CRC32C of the payload, and invokes the callback for
// each valid record.  Stops at the first checksum mismatch.
//
// Returns the last successfully replayed sequence_number (0 if none).
class WALReplayer {
public:
    explicit WALReplayer(std::string_view dir);

    /// Replay all valid records.  cb receives the header and a pointer to the
    /// payload bytes (valid only for the duration of the call).
    /// Returns the last good sequence_number.
    uint64_t replay(
        std::function<void(const WALRecord&, const uint8_t* payload)> cb);

private:
    std::string dir_;
};

} // namespace ob
