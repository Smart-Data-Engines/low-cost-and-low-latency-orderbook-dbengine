#pragma once

#include <optional>
#include <atomic>
#include <span>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

#include "orderbook/data_model.hpp"
#include "orderbook/epoch.hpp"
#include "orderbook/hlc.hpp"
#include "orderbook/log_episode.hpp"

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

/// Ordering of two positions in **one** WAL directory: earlier file first, then earlier offset.
///
/// A named function rather than `operator<` on purpose. Positions from two different WAL
/// directories are not comparable — that is what #61 established for the multi-master catch-up,
/// which compared byte offsets across independent WALs and lost records — so the comparison should
/// be something a reader has to ask for.
inline bool wal_position_before(WalPosition a, WalPosition b) {
    return a.file_index != b.file_index ? a.file_index < b.file_index : a.offset < b.offset;
}

static_assert(sizeof(WalPosition) == 8, "WalPosition must fit a lock-free atomic");
static_assert(std::atomic<WalPosition>::is_always_lock_free,
              "std::atomic<WalPosition> must be lock-free: an atomic that quietly takes a lock "
              "would put that lock on the WAL write path, which is the cost this exists to avoid");

/// A CHECKPOINT's payload since #159: the position its flush drained up to, as two little-endian
/// 32-bit words - the file index, then the offset. Eight bytes, and a checkpoint written before
/// #159 has none (`checkpoint_covered()` is then empty and replay reads it as it always did).
inline constexpr size_t CHECKPOINT_PAYLOAD_BYTES = 8;

inline void checkpoint_payload(WalPosition covered, uint8_t out[CHECKPOINT_PAYLOAD_BYTES]) {
    for (int i = 0; i < 4; ++i) {
        out[i]     = static_cast<uint8_t>(covered.file_index >> (8 * i));
        out[4 + i] = static_cast<uint8_t>(covered.offset >> (8 * i));
    }
}

/// What a CHECKPOINT covered, or nothing when its payload is not the eight bytes #159 writes - the
/// empty payload of an older build, or anything else, which is read as saying nothing rather than
/// as a position somewhere.
inline std::optional<WalPosition> checkpoint_covered(const uint8_t* payload, size_t len) {
    if (payload == nullptr || len != CHECKPOINT_PAYLOAD_BYTES) return std::nullopt;
    WalPosition covered{};
    for (int i = 0; i < 4; ++i) {
        covered.file_index |= static_cast<uint32_t>(payload[i]) << (8 * i);
        covered.offset     |= static_cast<uint32_t>(payload[4 + i]) << (8 * i);
    }
    return covered;
}

/// Largest rotate threshold that keeps the offset inside 32 bits with room to spare.
///
/// The offset is 32 bits so the pair fits one atomic. Rotation is checked *after* a write
/// (`offset >= threshold`), so the offset can exceed the threshold by at most one record - and a
/// record is bounded by the payload limit, far below the two gigabytes of headroom this leaves.
inline constexpr size_t MAX_WAL_ROTATE_THRESHOLD = 2ULL << 30;

/// Smallest rotate threshold worth offering an operator: one maximal record.
///
/// Rotation is checked *after* a write, so a threshold below this lets a single record fill a whole
/// file on its own — and since a 1000-level delta is 24 KB and the header limit allows 64 KB, that
/// is not a hypothetical shape. The result is one file per write: a directory with as many entries
/// as there are records, a `file_size` call per intervening file in `bytes_since()`, and a
/// retention pass that walks all of them every flush tick.
///
/// **`WALWriter` does not enforce this, and that is deliberate.** The unit tests drive this class
/// with a 512-byte threshold precisely so that rotation is reachable without writing megabytes, and
/// that is a legitimate thing for a component test to do. The floor belongs to the *operator knob*
/// — `--wal-rotate-bytes` refuses below it — because the failure it prevents is an operator
/// choosing a number whose consequence is invisible until the directory has a million files in it.
inline constexpr size_t MIN_WAL_ROTATE_THRESHOLD =
    sizeof(WALRecordV2) + WAL_MAX_PAYLOAD_LEN;

/// One DELTA record for `WALWriter::append_batch()`: the update, its levels, and - for a
/// multi-master write - the origin and HLC that make it a V2 record. A null `hlc` is the V1 record
/// `append()` writes; a set one is the V2 record `append_with_origin()` writes. Both of those are
/// batches of one, so there is one encoding of a DELTA record in this writer, not three.
///
/// `gap_before` puts a GAP record for the same sequence number and time directly in front of it -
/// the bytes `append_gap()` followed by this record's append wrote, which is what the engine does
/// when a stream skips a number. In a batch the two are one unit: a run is never cut between them,
/// because a record written alone after its GAP went into the file the GAP went into.
struct WalDelta {
    const DeltaUpdate*  update{nullptr};
    const Level*        levels{nullptr};
    const HLCTimestamp* hlc{nullptr};
    uint16_t            origin{0};
    bool                gap_before{false};
};

/// What `WALWriter::append_batch()` did with one record: written - and under `FsyncPolicy::EVERY`
/// synced - when `error` is empty, and then `position` is its first byte (#98). Otherwise `error`
/// is the text a record written alone would have thrown, because that is what the client is told.
struct WalBatchOutcome {
    WalPosition position{};
    std::string error;
};

class WALWriter {
public:
    explicit WALWriter(std::string_view dir,
                       size_t rotate_threshold_bytes = 512ULL << 20,
                       FsyncPolicy fsync_policy = FsyncPolicy::INTERVAL);
    ~WALWriter();

    // Non-copyable, non-movable (owns a file descriptor).
    WALWriter(const WALWriter&)            = delete;
    WALWriter& operator=(const WALWriter&) = delete;

    /// Append a DELTA record for the given update + levels, and return **where it was written**.
    /// Does NOT fsync — call sync() explicitly or rely on group commit.
    /// Automatically rotates if the threshold is exceeded after the write.
    ///
    /// The returned position is the first byte of this record, which is not derivable from
    /// `current_position()` afterwards. Rotation is checked *after* the write, so for the record
    /// that crosses the threshold the published position is already `{next_file, next_offset}`
    /// while the record itself sits near the end of the previous file — subtracting its length
    /// names a file it is not in, and can underflow. That was #98 on the replication wire: a
    /// function that writes has to say where it wrote, because nobody else can work it out.
    ///
    /// Not `[[nodiscard]]`: most callers append without needing the position and are right to, so
    /// the attribute would buy forty `(void)` casts. What enforces the rule is that
    /// `ReplicationManager::broadcast()` *takes* a position, so the one caller who has to get this
    /// right cannot proceed without one — and the only position that is correct is this one.
    WalPosition append(const DeltaUpdate& update, const Level* levels);

    /// Append a DELTA record with origin and HLC (multi-master mode), and return where it went.
    /// Writes a WALRecordV2 header (38 bytes, version=1) + payload.
    WalPosition append_with_origin(const DeltaUpdate& update, const Level* levels,
                                    uint16_t origin_node_id, const HLCTimestamp& hlc);

    /// Append several DELTA records with one `write()` per run between rotations, instead of one
    /// per record - the WAL half of stage 2b of using the whole machine, where a read's writes are
    /// applied under one acquisition of the engine's lock. The bytes in the file are the ones
    /// `append()` and `append_with_origin()` would have written, record for record and in order;
    /// what changes is how many system calls put them there.
    ///
    /// Every rule of a record written alone holds per record, and `outcomes[i]` says what
    /// happened to `records[i]`:
    /// - **the position of each record** is its first byte (#98);
    /// - **rotation is checked after the record that crosses the threshold**, so a run ends at it,
    ///   the file rotates, and the next record opens the next run - a file still passes the
    ///   threshold by one record at most, except while the disk refuses the marker (#153);
    /// - **`FsyncPolicy::EVERY` syncs each run before this returns**, so a record counted as
    ///   written is on the disk under that policy, as it was when each record was synced alone:
    ///   group commit, and why an answer sent after this call keeps `OK` meaning durable;
    /// - **a write that stops at a record** refuses that record - a record torn in the middle
    ///   abandons the file as #126 does - and the records after it are **tried again**, in the
    ///   next run, which is what the next command after a refused one did; a sync that fails
    ///   refuses every record of its run, which are in the file and are not tried again, because
    ///   a second copy of a record is the one outcome worse than a refusal;
    /// - **after a write that stopped at a record the rotation is not tried**: a disk that has just
    ///   refused a write is not asked for a ROTATE marker too, and the rotation is tried after the
    ///   next record that is written (#153).
    ///
    /// Returns how many were written rather than throwing, because a caller that applied nothing
    /// it was not told about has to know which, and an exception would lose that.
    /// `outcomes` must be at least as long as `records`.
    size_t append_batch(std::span<const WalDelta> records, std::span<WalBatchOutcome> outcomes);

    /// Set the local node_id for WAL_Origin (called once at startup).
    void set_origin_node_id(uint16_t node_id);

    /// Get the configured origin node_id.
    uint16_t origin_node_id() const { return origin_node_id_; }

    /// Write a GAP record (called by the engine when a sequence gap is detected).
    ///
    /// This and the four `append_*` below stay `void` deliberately. Only `append()` and
    /// `append_with_origin()` write records that travel record-by-record with a position on them,
    /// so only those two have a caller that needs one. A return value nobody reads is the shape
    /// this codebase has paid for five times over — a field that looks load-bearing and is not —
    /// and `append_version_vector()` can *refuse* to write, so it has no position to give.
    void append_gap(uint64_t sequence_number, uint64_t timestamp_ns);

    /// Write a CHECKPOINT record: the rows of every record before `covered` are durable in
    /// segments.
    ///
    /// Called after a flush has written and merged its segments. **`covered` is the position the
    /// flush drained up to, not the end of the log** (#159). The segment I/O runs without the
    /// engine's lock, so writers go on appending while it does, and the rows of those records are
    /// still queued when this record is written. A checkpoint that meant "everything before me"
    /// told replay to skip them, and a crash before the next flush lost every one - each answered
    /// `OK`, under every fsync policy, `every` included. The position is the record's payload
    /// (`checkpoint_payload()`), and replay gives back the records between it and the checkpoint.
    void append_checkpoint(uint64_t timestamp_ns, WalPosition covered);

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
    ///
    /// Does not throw for a failure of the disk, and that is #153. The record that carried the file
    /// past the threshold is written before this runs, so a rotation that fails is not that
    /// record's failure: when the marker cannot be written the file is left as it was and the
    /// rotation is tried again after the next record, and when the next file cannot be opened the
    /// writer is left with no file and `ensure_open()` opens it before the next write (#154).
    void rotate();

    /// fsync the current file (group commit boundary).
    /// `fsync` the current file if the policy asks for one. **False means it failed.**
    ///
    /// `[[nodiscard]]` deliberately: before #113 the result of every `fsync` in this file was
    /// discarded, so `--fsync-policy every` answered `OK` to writes it had not synced. A comment
    /// asking callers to check would have been the same thing one layer up; this makes ignoring it
    /// a compile error, and each of the seven call sites then had to say what it does instead -
    /// which is how `Engine::close()` ended up logging where the write path throws.
    [[nodiscard]] bool flush();

    /// Sync the WAL to disk. Alias for flush() — explicit group commit point.
    [[nodiscard]] bool sync();

    /// Number of records written since last sync.
    size_t pending_sync_count() const { return pending_sync_; }

    /// How many `fsync` calls on this WAL have failed, ever, in this process (#113).
    ///
    /// Sticky and monotone on purpose. **A failed `fsync` cannot be retried on Linux**: the kernel
    /// reports the error once, to whichever caller happened to be there, and marks the affected
    /// pages clean - so the next `fsync` on the same descriptor returns 0 with the data gone. This
    /// number is therefore the only lasting evidence that something the engine acknowledged may
    /// not be on the disk, and no later success takes it back.
    uint64_t fsync_failures() const { return fsync_failures_.load(std::memory_order_relaxed); }

    /// Records this writer has appended, of every type.
    ///
    /// Every type on purpose, because that is what the name says and because the difference
    /// against the client-facing insert counters is the bookkeeping this WAL does on its own
    /// behalf — checkpoints, epochs, version vectors, gaps. An operator watching write rate wants
    /// `ob_inserts_total`; an operator asking why the WAL grows faster than the inserts wants
    /// this one, and the two are only useful together (#117).
    uint64_t records_written() const { return records_written_.load(std::memory_order_relaxed); }

    /// Bytes written since `from`, across file boundaries, or `nullopt` when it cannot be known.
    ///
    /// The obvious `current_offset() - from.offset` is wrong for any position in an earlier file,
    /// and wrong in the direction that looks healthy: `rotate()` publishes
    /// `{next_index, next_offset}`, so the current offset **resets**, and a subtrahend from the
    /// previous file is larger than it — leaving a clamp at zero exactly when a replica is more
    /// than a file behind. That was #123, and it is the same shape as #118 one layer down: a
    /// difference of positions is a distance only when the positions are compared in full.
    ///
    /// Exact rather than estimated. Closed files are at least `rotate_threshold_` bytes because
    /// that is what rotation waits for, and a restart appends (`O_APPEND`, continuing from the
    /// highest existing index) rather than starting a short one — so an estimate of
    /// `files * threshold` would be good at production thresholds and badly wrong at the small
    /// ones tests use, where a 512-byte threshold and 136-byte records give files up to a quarter
    /// over. Asking the filesystem costs one `file_size` per **intervening** file, and there are
    /// normally none.
    ///
    /// `nullopt` means a file between the two positions is gone, which is not merely an
    /// unmeasurable distance: retention keeps files back to the slowest connected replica, so a
    /// missing one says that replica can no longer catch up from this WAL and needs a snapshot.
    /// Reporting zero for it, or guessing, would hide the more serious of the two conditions.
    std::optional<uint64_t> bytes_since(WalPosition from) const;

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

    /// The threshold this writer rotates at. Read by the startup log line, because how much a
    /// crash has to replay and how much WAL retention can free are both functions of this number
    /// and there is no other way to tell from outside which one a process is running with.
    size_t rotate_threshold() const { return rotate_threshold_; }

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
    /// Stop writing to the current file after a partial write failed, without a ROTATE record.
    ///
    /// The one case where the current file cannot be written to at all: some bytes of a record
    /// reached it and the rest failed, so no reader can parse past them and anything appended
    /// behind them is unreachable on replay (#126). `rotate()` cannot be used - its first act is to
    /// write a ROTATE record into that same file - and the marker is replaced by the replayer's
    /// rule: a checksum mismatch in a file that is **not** the last one is a tear, and replay
    /// continues with the next file.
    ///
    /// `noexcept`: the caller is about to report the write's own error, which is the one that says
    /// why the disk refused, and it must not be replaced by a second error about the recovery.
    void abandon_torn_file(size_t stranded_bytes, int write_errno) noexcept;

    /// Leave the current file for the next one, once nothing more may be written to it: its ROTATE
    /// marker is in it (replay and catch-up both stop reading a file there), or a torn record is.
    ///
    /// `open_current()` closes the old descriptor before it opens the new one, so a next file that
    /// cannot be opened leaves the writer with **no** file rather than with the old one - the right
    /// way round, since writing on behind the marker is #153's loss. Returns whether it opened one.
    /// Until then every write is refused, with the reason the file could not be opened, and each
    /// write tries again (#154).
    bool leave_for_next_file() noexcept;

    /// Open the next file if the writer has none, before a write: the retry #154 was missing.
    /// Before it, a writer whose next file could not be opened once kept `fd_ = -1` for the rest of
    /// the process, and every write after that was refused with `EBADF` - even once the directory
    /// was writable again. Throws what `open_current()` throws, which names the file and the
    /// reason, and that is what the client is told instead of a bad descriptor.
    void ensure_open();

    /// Open while the writer has no file: one line when it starts and one when it ends, however
    /// many writes are refused in between (#95's shape, #116's answer).
    LogEpisode no_file_;

public:

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
    /// `fsync` the current descriptor. Returns 0, or the `errno` it failed with.
    ///
    /// Returns the error rather than a bool because the caller needs the reason: the message a
    /// client is handed has to say *why* its write is not durable, and logging inside here would
    /// otherwise have clobbered `errno` before the caller could read it.
    int fsync_or_record(const char* why);

    size_t      pending_sync_{0};

    /// Failed `fsync` calls, read by the engine to publish `ob_wal_fsync_errors_total`.
    ///
    /// Atomic because the destructor and the rotation path can run on a different thread from the
    /// one publishing it, and because a number an operator reads to decide whether to replace a
    /// disk should not be a data race.
    std::atomic<uint64_t> fsync_failures_{0};

    /// Atomic for the same reason as `position_` and `fsync_failures_`: the writer's own thread
    /// increments it and the engine's flush tick reads it, so a plain `uint64_t` would be a data
    /// race whatever the arithmetic looks like. Relaxed load plus relaxed store rather than
    /// `fetch_add`, because the engine's mutexes serialise writers — so this costs two plain moves
    /// and no lock-prefixed instruction on the write path, which `scripts/mnemonic_diff.py`
    /// confirms rather than this comment.
    std::atomic<uint64_t> records_written_{0};
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

    /// Advance the published position past `records` whole records totalling `total` bytes and
    /// count them, returning the position of the first byte of the first. More than one only from
    /// `write_run()`, for the records of a run that all reached the file.
    ///
    /// One function rather than a copy in each of the two write paths. The two copies it replaces
    /// were identical down to their comment, which is the shape #94 was: a quantity maintained at
    /// N call sites is a quantity the N+1st call site will not maintain. A third record format
    /// added later gets the counter by calling this, or does not compile.
    ///
    /// Defined here rather than in the .cpp because this is the write path and the difference was
    /// measured, not assumed: out of line it became a real 16-instruction call and `write_record`
    /// went 220 → 212, so the drop was relocation and the true cost was **+8 instructions per
    /// record**. Inline it costs the counter and nothing else, and the `lock`-prefixed instruction
    /// count of the whole archive is unchanged either way — which is the number that mattered
    /// (#117).
    WalPosition advance_after_write(size_t total, uint64_t records = 1) {
        // One load, one store, on the writer's own thread - the engine's mutexes serialise
        // writers, so no compare-exchange is needed. On x86-64 a relaxed load and store of an
        // aligned eight-byte value are plain moves.
        WalPosition pos = position_.load(std::memory_order_relaxed);
        const WalPosition written_at = pos;   // the first byte of the record just written
        pos.offset += static_cast<uint32_t>(total);
        position_.store(pos, std::memory_order_relaxed);

        // Same shape, same reasoning, and it is here rather than in the two callers because a
        // count kept in two places is a count the third writer will not keep (#94, #117).
        records_written_.store(records_written_.load(std::memory_order_relaxed) + records,
                               std::memory_order_relaxed);
        pending_sync_ += records;
        return written_at;
    }

    /// Write a complete record (header + payload) and return the position it was written at.
    ///
    /// Under `FsyncPolicy::EVERY` it syncs the record, unless `allow_fsync` is false - which is only
    /// for records whose loss is harmless. A lost CHECKPOINT costs a redundant replay, never a lost
    /// row, so paying an fsync for it would slow every flush for nothing; and a lost ROTATE marker
    /// leaves a file that is read to its end, while a failed sync *of* one is what stranded a
    /// record behind it (#153).
    WalPosition write_record(const WALRecord& hdr, const void* payload, size_t payload_len,
                             bool allow_fsync = true);

    /// What `write_run()` did with one run of records.
    ///
    /// `landed` is how many of them, from the first, have every byte in the file; they are counted
    /// in the position either way. `sync_errno` is non-zero when a sync that `FsyncPolicy::EVERY`
    /// asked for failed, which confirms none of those. `write_errno` is non-zero when the write
    /// stopped at record `landed`, which is therefore not in the file. Numbers rather than
    /// messages: every run builds a result and almost none of them fails, and two strings made
    /// and destroyed per run were part of what a batch of one cost (#155).
    struct RunResult {
        size_t      landed{0};
        WalPosition first{};
        int         sync_errno{0};
        int         write_errno{0};
    };

    /// The text a client is sent for a failed write or sync - the one spelling of each, used by
    /// both paths that report them.
    static std::string write_failed(int err);
    static std::string sync_failed(int err);

    /// Write one run - whole records back to back, record `i` ending at `ends[i]` - and account for
    /// it: the one place in this writer where bytes reach the file, so a record written alone
    /// (`write_record()`, `append()`) and a batch cannot disagree about what a failure means.
    RunResult write_run(const uint8_t* data, std::span<const size_t> ends, bool allow_fsync);

    /// `append_batch()`'s run: the bytes of the records being assembled, where each WAL record
    /// ends, and for each batch record the index in `batch_ends_` of its DELTA (a GAP in front of
    /// it is one more WAL record). Members rather than locals, so a batch per read allocates
    /// nothing once they have grown to a read's size - and they only ever grow, because `resize()`
    /// zero-fills what it adds and a buffer shrunk to be refilled would pay that on every record
    /// (#127's measurement).
    std::vector<uint8_t> batch_buf_;
    std::vector<size_t>  batch_ends_;
    std::vector<size_t>  batch_deltas_;
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

    /// Replay the records the last CHECKPOINT does not cover: every record after it, and - when it
    /// says what it covered (#159) - the records before it that start at or after that position.
    ///
    /// The second half is what a checkpoint could not say before #159: its flush writes segments
    /// without the engine's lock, so the records appended meanwhile precede the checkpoint in the
    /// log while their rows are still queued. The position only ever adds records; nothing after
    /// the checkpoint is skipped whatever its payload says. A checkpoint with the empty payload of
    /// an older build is read as it always was, as covering everything before it.
    ///
    /// Two passes rather than buffering the tail in memory: the first finds the last
    /// checkpoint, the second invokes cb for the records it does not cover. The tail can be
    /// arbitrarily large if flushing fell behind, and open() is not on a latency
    /// path, so bounded memory is worth more than one pass.
    ///
    /// With no checkpoint in the log, every record is replayed — which is correct for
    /// a log written before checkpoints existed, and for one whose first flush has
    /// not happened yet.
    uint64_t replay_after_checkpoint(WALReplayCallbackV2 cb);

    /// Return the highest epoch found during the last replay (0 if none).
    uint64_t last_epoch() const { return last_epoch_; }

    /// Files whose checksum mismatch the last replay stepped over as a **torn record** (#126).
    ///
    /// Not a curiosity: a torn file the current writer produced ends mid-record, which every reader
    /// here has always tolerated, so this can only be non-zero for a WAL written **before** the
    /// writer learned to abandon a file it tore. It is therefore the count of files an older build
    /// stranded and this replay recovered past - which is worth a line at startup, and is what
    /// makes the difference between a tear and a crash tail observable rather than inferred from a
    /// log message.
    ///
    /// Reset at the start of every replay, so a caller that replays twice reads the second pass.
    size_t tears_skipped() const { return tears_skipped_; }

private:
    std::string dir_;
    uint64_t    last_epoch_{0};
    size_t      tears_skipped_{0};
};

} // namespace ob
