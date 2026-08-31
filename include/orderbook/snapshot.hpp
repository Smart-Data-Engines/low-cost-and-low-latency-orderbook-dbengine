#pragma once

// ── Snapshot types ───────────────────────────────────────────────────────────
//
// The manifest that describes a snapshot, and the sequence state captured with it.
//
// These used to live in replication.hpp, which is where the first consumer happened to be. They
// have three more since: the engine creates them, multi-master transfers them (#76), and shard
// migration reads them. A header of their own also lets multi_master.hpp and replication.hpp hold
// fields of these types, which they cannot do for a type nested inside `Engine` — engine.hpp
// includes both of them, so neither can include it back.

#include "orderbook/sequence_tracker.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace ob {

struct SnapshotFileEntry {
    std::string path;       // relative to data dir
    size_t      size{0};    // file size in bytes
    uint32_t    crc32c{0};  // CRC32C of file contents
};

struct SnapshotManifest {
    uint32_t    wal_file_index{0};
    size_t      wal_byte_offset{0};
    size_t      total_bytes{0};
    size_t      total_rows{0};
    uint64_t    created_at_ns{0};
    std::vector<SnapshotFileEntry> files;

    /// Serialize to JSON string (deterministic alphabetical field ordering).
    std::string to_json() const;

    /// Parse from JSON string. Returns true on success.
    static bool from_json(std::string_view json, SnapshotManifest& out);
    // Both are implemented in src/replication.cpp, where they were written.
};

/// A snapshot plus what the sender holds, captured together.
struct SnapshotWithSequenceState {
    SnapshotManifest                          manifest;
    std::vector<SequenceTracker::VectorEntry> vector;
    std::vector<SequenceTracker::HeldRanges>  held;
    bool vector_truncated{false};
    bool held_truncated{false};
    /// How long the capture took. Measured rather than assumed: this used to run on the caller's
    /// thread, and for multi-master that thread was `io_loop()` (#79).
    double create_ms{0.0};
};

}  // namespace ob
