#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

#include "orderbook/data_model.hpp"
#include "orderbook/epoch.hpp"
#include "orderbook/hlc.hpp"

namespace ob {

// ── Record types ──────────────────────────────────────────────────────────────
inline constexpr uint8_t WAL_RECORD_DELTA    = 1;
inline constexpr uint8_t WAL_RECORD_SNAPSHOT = 2;
inline constexpr uint8_t WAL_RECORD_GAP      = 3;
inline constexpr uint8_t WAL_RECORD_ROTATE   = 4;
// EPOCH = 5, see append_epoch().
/// Everything before this record is durable in columnar segments, so replay may skip
/// it. Written after a successful flush, never before: a checkpoint claiming more than
/// is durable turns a crash into data loss.
inline constexpr uint8_t WAL_RECORD_CHECKPOINT = 6;
/// A node's version vector: what it holds per (symbol, origin). Written to the WAL so a
/// restarted node knows it, and sent to peers in the same envelope so a node running the
/// older protocol skips it as an unknown type instead of disconnecting.
inline constexpr uint8_t WAL_RECORD_VERSION_VECTOR = 7;
/// Sequence numbers held above the frontiers, as ranges. Written next to the version vector and
/// read only by the node that wrote it: without it, a restart forgets every out-of-order record it
/// was holding, and the next redelivery — which catch-up performs on purpose — is applied a second
/// time into append-only storage. Catch-up forwards only DELTA records, so peers never see this.
inline constexpr uint8_t WAL_RECORD_HELD_SEQUENCES = 8;

/// Reserved: 200 and above are wire-only message types, never written to a WAL file.
///
/// The multi-master snapshot protocol (MM_MSG_SNAPSHOT_* in multi_master.hpp) borrows this
/// field to tag its frames, because frames after the handshake carry a WALRecordV2 header and
/// nothing else identifies them. A new WAL record type takes the next free number from 9 up and
/// must stay below 200, or a node would read a snapshot chunk as a record to replay.
inline constexpr uint8_t WAL_RECORD_WIRE_ONLY_BASE = 200;

/// Largest payload a record header can describe: `payload_len` is a `uint16_t`.
///
/// Not a style detail. `write_record()` writes the payload it was given and the header the caller
/// built, so a caller that casts a larger size down produces a record whose header understates
/// its own length — and every replay after it reads the middle of that payload as the next
/// header. Anything that assembles a record has to check against this, not assume it.
inline constexpr size_t WAL_MAX_PAYLOAD_LEN = 65535;

// ── Fsync policy ──────────────────────────────────────────────────────────────
// Controls when the WAL calls fsync:
//   EVERY    — fsync after every record (max durability, lowest throughput)
//   INTERVAL — fsync at group commit boundaries (default, ~100ms data loss window)
//   NONE     — never fsync (max throughput, data loss on crash)
enum class FsyncPolicy : uint8_t {
    EVERY    = 0,
    INTERVAL = 1,
    NONE     = 2,
};

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

// ── WALRecordV2 ───────────────────────────────────────────────────────────────
// Extended WAL header (38 bytes) for multi-master replication.
// Adds origin_node_id (WAL_Origin) and HLC timestamp to each record.
//
// Layout (38 bytes):
//   sequence_number : uint64  (8B)  — sequence number
//   timestamp_ns    : uint64  (8B)  — nanosecond timestamp (legacy, kept for compat)
//   checksum        : uint32  (4B)  — CRC32C of payload
//   payload_len     : uint16  (2B)  — payload length
//   record_type     : uint8   (1B)  — DELTA=1, SNAPSHOT=2, GAP=3, ROTATE=4, EPOCH=5
//   version         : uint8   (1B)  — 0=legacy (24B header), 1=extended (38B header)
//   --- below only when version >= 1 ---
//   origin_node_id  : uint16  (2B)  — WAL_Origin: node_id of the originating node
//   hlc_data        : 12B           — HLCTimestamp (physical_ns + logical + node_id)
//
// Backward compatibility:
//   - Old WALReplayer sees version=0, reads 24B header, ignores the rest
//   - New WALReplayer sees version=0 → treats origin_node_id=0, hlc=zero
//   - New WALReplayer sees version=1 → reads full 38B
#pragma pack(push, 1)
struct WALRecordV2 {
    uint64_t sequence_number;
    uint64_t timestamp_ns;
    uint32_t checksum;        // CRC32C of payload
    uint16_t payload_len;
    uint8_t  record_type;
    uint8_t  version;         // 0=legacy, 1=extended (with origin+HLC)
    // Extended fields (version >= 1):
    uint16_t origin_node_id;  // WAL_Origin
    uint8_t  hlc_data[12];    // HLCTimestamp serialized (LE)
};
#pragma pack(pop)

static_assert(sizeof(WALRecordV2) == 38, "WALRecordV2 size mismatch");

// ── WALReplayContext ──────────────────────────────────────────────────────────
// Extended replay callback context with origin and HLC information.
struct WALReplayContext {
    WALRecord       header;          // legacy header (24B)
    uint16_t        origin_node_id;  // 0 if legacy record
    HLCTimestamp    hlc;             // zero if legacy record
    const uint8_t*  payload;
    size_t          payload_len;
    /// Where this record starts: the WAL file it came from and its byte offset in that file.
    ///
    /// Recovery compares this against the position a segment recorded for the same symbol, which
    /// answers "is this already stored?" as a fact rather than inferring it from timestamps (#63).
    uint32_t        wal_file_index{0};
    uint64_t        wal_byte_offset{0};
};

using WALReplayCallbackV2 = std::function<void(const WALReplayContext& ctx)>;

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
// ── WalPosition ───────────────────────────────────────────────────────────────
//
// Where the WAL is: which file, and how far into it. **One value, not two.**
//
// It used to be two plain members read by four threads, and the atomicity was the smaller half of
// the problem. `Engine::get_wal_position()` and `MultiMasterManager::send_handshake()` each read the
// index on one line and the offset on the next, so a rotation between them produced a pair that
// never existed - a fresh file index carrying the previous file's offset. That pair feeds the
// published WAL position which election deference compares to pick the replica furthest ahead
// (#70, #72), where it reads as a candidate that went backwards by a whole file.
//
// Measured before the fix, with a reader polling in a tight loop: **one incoherent pair in about
// 150 million reads**, in two runs out of three. So the coherence defect is real and rare - the
// window is two adjacent instructions - while the data race is on *every* concurrent read, which is
// what ThreadSanitizer reports and what the compiler is entitled to act on.
struct WalPosition {
    uint32_t file_index{0};
    uint32_t offset{0};
};

static_assert(sizeof(WalPosition) == 8, "WalPosition must fit a lock-free atomic");
static_assert(std::atomic<WalPosition>::is_always_lock_free,
              "std::atomic<WalPosition> must be lock-free: an atomic that quietly takes a lock "
              "would put that lock on the WAL write path, which is the cost this exists to avoid");

/// Largest rotate threshold that keeps the offset inside 32 bits with room to spare.
///
/// The offset is 32 bits so the pair fits one atomic. Rotation is checked *after* a write
/// (`offset >= threshold`), so the offset can exceed the threshold by at most one record - and a
/// record is bounded by the payload limit, far below the two gigabytes of headroom this leaves.
inline constexpr size_t MAX_WAL_ROTATE_THRESHOLD = 2ULL << 30;

class WALWriter {
public:
    explicit WALWriter(std::string_view dir,
                       size_t rotate_threshold_bytes = 512ULL << 20,
                       FsyncPolicy fsync_policy = FsyncPolicy::INTERVAL);
    ~WALWriter();

    // Non-copyable, non-movable (owns a file descriptor).
    WALWriter(const WALWriter&)            = delete;
    WALWriter& operator=(const WALWriter&) = delete;

    /// Append a DELTA record for the given update + levels.
    /// Does NOT fsync — call sync() explicitly or rely on group commit.
    /// Automatically rotates if the threshold is exceeded after the write.
    void append(const DeltaUpdate& update, const Level* levels);

    /// Append a DELTA record with origin and HLC (multi-master mode).
    /// Writes a WALRecordV2 header (38 bytes, version=1) + payload.
    void append_with_origin(const DeltaUpdate& update, const Level* levels,
                            uint16_t origin_node_id, const HLCTimestamp& hlc);

    /// Set the local node_id for WAL_Origin (called once at startup).
    void set_origin_node_id(uint16_t node_id);

    /// Get the configured origin node_id.
    uint16_t origin_node_id() const { return origin_node_id_; }

    /// Write a GAP record (called by the engine when a sequence gap is detected).
    void append_gap(uint64_t sequence_number, uint64_t timestamp_ns);

    /// Write a CHECKPOINT record: everything before it is durable in segments.
    ///
    /// Called after a flush has written and merged its segments, so the record's
    /// presence is evidence that the rows preceding it no longer need replaying.
    void append_checkpoint(uint64_t timestamp_ns);

    /// Append this node's serialised version vector: what it holds, per (symbol, origin).
    ///
    /// Not fsynced, for the same reason as the checkpoint: losing it means the node restores
    /// a lower frontier, asks a peer for more than it needs and drops the duplicates. Losing
    /// it cannot cost data.
    void append_version_vector(const uint8_t* payload, size_t payload_len);

    /// Write a HELD_SEQUENCES record (type 8). Not fsynced, for the same reason as the vector:
    /// losing it costs redeliveries and duplicate drops, never data.
    void append_held_sequences(const uint8_t* payload, size_t payload_len);

    /// Write an EPOCH record (WAL_RECORD_EPOCH, type=5) with the given epoch value.
    void append_epoch(const EpochValue& epoch);

    /// Set the current epoch tracked by this writer.
    void set_epoch(uint64_t e) { current_epoch_ = e; }

    /// Get the current epoch tracked by this writer.
    uint64_t current_epoch() const { return current_epoch_; }

    /// Force rotation: write ROTATE record, close current file, open next.
    void rotate();

    /// fsync the current file (group commit boundary).
    void flush();

    /// Sync the WAL to disk. Alias for flush() — explicit group commit point.
    void sync();

    /// Number of records written since last sync.
    size_t pending_sync_count() const { return pending_sync_; }

    /// Where the WAL is, in one atomic load. Cannot observe a rotation half-applied.
    ///
    /// `relaxed`, and that is a decision rather than an omission. A reader wants a coherent *pair*,
    /// not ordering against other writes: the published position is a heuristic under a lease, not a
    /// key somebody reads data behind. Coherence here comes from the value being eight bytes, not
    /// from the memory order, so `acquire`/`release` would cost the write path something and buy
    /// this mechanism nothing.
    WalPosition current_position() const { return position_.load(std::memory_order_relaxed); }

    /// Index of the WAL file currently being written to.
    uint32_t current_file_index() const { return current_position().file_index; }

    /// Current byte offset within the active WAL file.
    ///
    /// Prefer :func:`current_position` when you also need the file index. Two calls compose a pair
    /// from two moments, which is the defect #85 removed; `tests/test_wal_position.cpp` has a static
    /// test that refuses that shape in `src/`.
    size_t current_offset() const { return current_position().offset; }

    /// Directory where WAL files are stored.
    const std::string& dir() const { return dir_; }

    /// Remove WAL files with index strictly less than `before_index`.
    /// Safe to call while the writer is active — only touches closed files.
    /// Returns the number of files removed.
    size_t truncate_before(uint32_t before_index);

private:
    int         fd_;

    /// The only storage for the position. Not a copy published beside `written_` and `file_index_`:
    /// a copy would need publishing at five mutation sites, and a missed one gives a position that
    /// is silently stale - a worse symptom than the undefined behaviour it replaces, because a
    /// sanitizer at least reports that. One location makes the omission unrepresentable.
    ///
    /// Written only by the WAL writer, which the engine's mutexes serialise, so the write path does
    /// load-compute-store rather than a compare-exchange.
    std::atomic<WalPosition> position_{};

    size_t      rotate_threshold_;
    FsyncPolicy fsync_policy_;
    std::string dir_;
    size_t      pending_sync_{0};
    uint64_t    current_epoch_{0};
    uint16_t    origin_node_id_{0};  // 0 = legacy mode (no multi-master)

    // Pre-allocated write buffer to avoid per-record heap allocations.
    std::vector<uint8_t> write_buf_;

    /// Open (or create) the WAL file for `index`, and return how many bytes it already holds.
    ///
    /// Deliberately **does not publish** the position: it returns the offset so the caller can store
    /// the index and the offset as one value. An earlier version incremented the index, then let
    /// this function store the offset, and that published `(N+1, previous file's offset)` as an
    /// intermediate state - reintroducing the exact pair #85 exists to remove. The cross-thread test
    /// caught it at 96 observations in 4.3 million.
    uint32_t open_current(uint32_t index);

    /// Write a complete record (header + payload). Does NOT fsync.
    /// allow_fsync=false writes the record without honouring FsyncPolicy::EVERY. Only
    /// for records whose loss is harmless: a lost CHECKPOINT costs a redundant replay,
    /// never a lost row, so paying an fsync for it would slow every flush for nothing.
    void write_record(const WALRecord& hdr, const void* payload, size_t payload_len,
                      bool allow_fsync = true);

    /// Write a complete V2 record (38B header + payload). Does NOT fsync.
    void write_record_v2(const WALRecordV2& hdr, const void* payload, size_t payload_len);
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

    /// Replay with extended context (origin + HLC).
    /// Reads the version field to determine header size (24B or 38B).
    /// For legacy records (version=0): origin_node_id=0, hlc=zero.
    /// For extended records (version=1): reads full 38B header.
    /// Returns the last good sequence_number.
    uint64_t replay_v2(WALReplayCallbackV2 cb);

    /// Replay only the records written after the last CHECKPOINT record.
    ///
    /// Two passes rather than buffering the tail in memory: the first finds the last
    /// checkpoint, the second invokes cb for the records after it. The tail can be
    /// arbitrarily large if flushing fell behind, and open() is not on a latency
    /// path, so bounded memory is worth more than one pass.
    ///
    /// With no checkpoint in the log, every record is replayed — which is correct for
    /// a log written before checkpoints existed, and for one whose first flush has
    /// not happened yet.
    uint64_t replay_after_checkpoint(WALReplayCallbackV2 cb);

    /// Return the highest epoch found during the last replay (0 if none).
    uint64_t last_epoch() const { return last_epoch_; }

private:
    std::string dir_;
    uint64_t    last_epoch_{0};
};

} // namespace ob
