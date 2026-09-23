// WAL Writer and Replayer implementation.
// Uses POSIX file I/O: open(2), write(2), fsync(2), close(2), read(2).
// CRC32C is crc32c.hpp's: the CPU's instruction where it has one (#81), a table otherwise.

#include "orderbook/wal.hpp"
#include "orderbook/crc32c.hpp"
#include "orderbook/epoch.hpp"
#include "orderbook/hlc.hpp"
#include "orderbook/logger.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

namespace ob {

namespace {

// Build the WAL filename for a given index.
static std::string wal_filename(const std::string& dir, uint32_t index) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "wal_%06u.bin", index);
    return dir + "/" + buf;
}

/// Append one DELTA record - header, then payload - to `buf` at `used`, and advance `used` past it.
///
/// Byte for byte what `append()` wrote for a null `d.hlc` and what `append_with_origin()` wrote
/// otherwise, because both are now batches of one through this function: one encoding of a DELTA
/// record in this writer, not one per entry point. The payload is built in place and the checksum
/// is taken over it there, so a record is copied once rather than into a stack buffer and then
/// again into the write buffer.
///
/// `buf` only ever grows: `resize()` zero-fills what it adds, so a buffer cleared and refilled per
/// batch would pay a memset of every record before overwriting it - the shape #127 measured.
void encode_delta(const WalDelta& d, std::vector<uint8_t>& buf, size_t& used) {
    const DeltaUpdate& update = *d.update;
    const size_t levels_bytes = size_t{update.n_levels} * sizeof(Level);
    const size_t payload_len  = sizeof(DeltaUpdate) + levels_bytes;
    const size_t header_len   = d.hlc ? sizeof(WALRecordV2) : sizeof(WALRecord);
    const size_t total        = header_len + payload_len;

    if (buf.size() < used + total) buf.resize(std::max(used + total, buf.size() * 2));
    uint8_t* const record  = buf.data() + used;
    uint8_t* const payload = record + header_len;

    std::memcpy(payload, &update, sizeof(DeltaUpdate));
    if (levels_bytes > 0) std::memcpy(payload + sizeof(DeltaUpdate), d.levels, levels_bytes);
    const uint32_t checksum = crc32c(payload, payload_len);

    if (d.hlc == nullptr) {
        WALRecord hdr{};
        hdr.sequence_number = update.sequence_number;
        hdr.timestamp_ns    = update.timestamp_ns;
        hdr.checksum        = checksum;
        hdr.payload_len     = static_cast<uint16_t>(payload_len);
        hdr.record_type     = WAL_RECORD_DELTA;
        hdr._pad            = 0;
        std::memcpy(record, &hdr, sizeof(hdr));
    } else {
        WALRecordV2 hdr{};
        hdr.sequence_number = update.sequence_number;
        hdr.timestamp_ns    = update.timestamp_ns;
        hdr.checksum        = checksum;
        hdr.payload_len     = static_cast<uint16_t>(payload_len);
        hdr.record_type     = WAL_RECORD_DELTA;
        hdr.version         = 1;
        hdr.origin_node_id  = d.origin;
        d.hlc->serialize(hdr.hlc_data);
        std::memcpy(record, &hdr, sizeof(hdr));
    }
    used += total;
}

/// Append the GAP record `append_gap()` writes for `update`'s number and time: a header with an
/// empty payload, byte for byte the one `append_gap()` hands `write_record()`.
void encode_gap(const DeltaUpdate& update, std::vector<uint8_t>& buf, size_t& used) {
    WALRecord hdr{};
    hdr.sequence_number = update.sequence_number;
    hdr.timestamp_ns    = update.timestamp_ns;
    hdr.checksum        = crc32c(nullptr, 0); // empty payload
    hdr.payload_len     = 0;
    hdr.record_type     = WAL_RECORD_GAP;
    hdr._pad            = 0;

    if (buf.size() < used + sizeof(hdr)) buf.resize(std::max(used + sizeof(hdr), buf.size() * 2));
    std::memcpy(buf.data() + used, &hdr, sizeof(hdr));
    used += sizeof(hdr);
}

} // anonymous namespace

// ── WALWriter ─────────────────────────────────────────────────────────────────

WALWriter::WALWriter(std::string_view dir, size_t rotate_threshold_bytes,
                     FsyncPolicy fsync_policy)
    : fd_(-1)
    , rotate_threshold_(rotate_threshold_bytes)
    , fsync_policy_(fsync_policy)
    , dir_(dir)
    , pending_sync_(0)
{
    // The offset half of WalPosition is 32 bits so the pair fits one lock-free atomic, so the
    // threshold has to keep it there. Refused rather than clamped: WAL rotation decides how much has
    // to be replayed after a crash, and an operator who asked for eight gigabyte files should not
    // silently get two and find out during a recovery.
    if (rotate_threshold_bytes > MAX_WAL_ROTATE_THRESHOLD) {
        throw std::invalid_argument(
            "WALWriter: rotate threshold " + std::to_string(rotate_threshold_bytes) +
            " bytes exceeds the maximum of " + std::to_string(MAX_WAL_ROTATE_THRESHOLD) +
            " (2 GiB). The offset is 32 bits so that the file index and the offset can be read as "
            "one coherent value; a larger file would let the offset wrap and report a position "
            "inside the wrong part of the file.");
    }

    // Pre-allocate write buffer (enough for largest possible record).
    write_buf_.reserve(sizeof(WALRecord) + sizeof(DeltaUpdate) + MAX_LEVELS * sizeof(Level));

    // Ensure directory exists.
    std::filesystem::create_directories(dir_);

    // Find the highest existing WAL file index so we continue from there.
    uint32_t highest = 0;
    for (auto& entry : std::filesystem::directory_iterator(dir_)) {
        const std::string name = entry.path().filename().string();
        if (name.size() == 14 &&
            name.substr(0, 4) == "wal_" &&
            name.substr(10) == ".bin") {
            uint32_t idx = static_cast<uint32_t>(std::stoul(name.substr(4, 6)));
            if (idx >= highest) {
                highest = idx;
            }
        }
    }
    // One publish, with both halves already known.
    position_.store(WalPosition{highest, open_current(highest)}, std::memory_order_relaxed);
}

/// `fsync` the current descriptor, and never let the failure go unrecorded (#113).
///
/// Before this, all seven `fsync` calls in this file discarded their result, and the two on the
/// write path then set `pending_sync_ = 0` regardless - so a writer whose sync had failed came out
/// of it believing everything was on the disk. Measured with `--fsync-policy every`: eleven `EIO`
/// returns and three `INSERT`s still answered `OK`.
///
/// `pending_sync_` is deliberately **not** cleared here. That belongs to the caller, and only on
/// success: the count is how the flush loop decides whether a sync is owed, and zeroing it after a
/// failure is what silenced the next attempt.
///
/// The log says what the sync was for, because "fsync failed" alone does not tell an operator
/// whether a client was waiting on it - and it says what Linux does next, because the obvious
/// reaction (try again) is the one thing that cannot work.
int WALWriter::fsync_or_record(const char* why) {
    if (fd_ < 0) return 0;
    if (::fsync(fd_) == 0) return 0;
    const int err = errno;
    fsync_failures_.fetch_add(1, std::memory_order_relaxed);
    OB_LOG_ERROR("wal",
                 "fsync failed during %s: %s. Linux reports this once and marks the pages clean, "
                 "so a later fsync will succeed with the data already gone - this node can no "
                 "longer promise that what it acknowledged is on the disk",
                 why, std::strerror(err));
    return err;
}

WALWriter::~WALWriter() {
    if (fd_ >= 0) {
        // Nothing to report to: a destructor that throws during shutdown is #112 again. The
        // counter and the log are the whole answer here.
        (void)fsync_or_record("closing the WAL");
        ::close(fd_);
        fd_ = -1;
    }
}

uint32_t WALWriter::open_current(uint32_t index) {
    if (fd_ >= 0) {
        // A rotation whose sync failed leaves the file it is leaving behind possibly incomplete.
        // Recorded rather than thrown: this runs from the constructor as well, where there is no
        // caller to tell.
        (void)fsync_or_record("rotating the WAL");
        ::close(fd_);
        fd_ = -1;
    }

    const std::string path = wal_filename(dir_, index);
    // O_APPEND ensures all writes go to the end even across processes.
    fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0) {
        throw std::runtime_error("WALWriter: cannot open " + path +
                                 ": " + std::strerror(errno));
    }

    // How many bytes the file already holds, for rotation accounting. Returned rather than stored:
    // see the declaration.
    const off_t pos = ::lseek(fd_, 0, SEEK_END);
    return (pos >= 0) ? static_cast<uint32_t>(pos) : 0;
}

std::optional<uint64_t> WALWriter::bytes_since(WalPosition from) const {
    const WalPosition now = position_.load(std::memory_order_relaxed);

    // A position ahead of ours is not an error and not a negative distance: an ACK can be read
    // after the value it acknowledges has been superseded, and a replica cannot be ahead of the
    // log it follows. Zero is the honest answer to "how far behind".
    if (from.file_index > now.file_index) return uint64_t{0};

    if (from.file_index == now.file_index) {
        return now.offset > from.offset ? static_cast<uint64_t>(now.offset - from.offset)
                                        : uint64_t{0};
    }

    std::error_code ec;
    uint64_t total = 0;

    // The tail of the file `from` sits in. `file_size` rather than the rotation threshold: a
    // rotated file is *at least* the threshold and overshoots by up to one record, which is
    // nothing at 512 MB and a quarter of the file at the thresholds tests use.
    const auto first = std::filesystem::file_size(wal_filename(dir_, from.file_index), ec);
    if (ec) return std::nullopt;
    total += first > from.offset ? first - from.offset : 0;

    for (uint32_t index = from.file_index + 1; index < now.file_index; ++index) {
        const auto size = std::filesystem::file_size(wal_filename(dir_, index), ec);
        if (ec) return std::nullopt;
        total += size;
    }

    // The current file is measured by the published offset, not by `file_size`: the offset is the
    // byte count this writer has accounted for, and asking the filesystem here would race a write
    // that has landed but not yet been published.
    total += now.offset;
    return total;
}

WalPosition WALWriter::write_record(const WALRecord& hdr, const void* payload,
                                     size_t payload_len, bool allow_fsync) {
    ensure_open();

    // Header and payload in one buffer, so the record is one write. The buffer only grows; the run
    // says how much of it is this record.
    const size_t total = sizeof(WALRecord) + payload_len;
    if (write_buf_.size() < total) write_buf_.resize(total);
    std::memcpy(write_buf_.data(), &hdr, sizeof(WALRecord));
    if (payload_len > 0) {
        std::memcpy(write_buf_.data() + sizeof(WALRecord), payload, payload_len);
    }

    const RunResult run = write_run(write_buf_.data(), std::span<const size_t>(&total, 1),
                                    allow_fsync);
    if (run.write_errno != 0) throw std::runtime_error(write_failed(run.write_errno));
    if (run.sync_errno != 0) throw std::runtime_error(sync_failed(run.sync_errno));
    return run.first;
}

std::string WALWriter::write_failed(int err) {
    return std::string("WALWriter: write failed: ") + std::strerror(err);
}

std::string WALWriter::sync_failed(int err) {
    return std::string("WALWriter: fsync failed: ") + std::strerror(err);
}

WALWriter::RunResult WALWriter::write_run(const uint8_t* data, std::span<const size_t> ends,
                                          bool allow_fsync) {
    RunResult run;
    run.first = current_position();
    const size_t total = ends.back();

    size_t done = 0;
    int write_errno = 0;
    while (done < total) {
        const ssize_t n = ::write(fd_, data + done, total - done);
        if (n < 0) {
            write_errno = errno;
            break;
        }
        done += static_cast<size_t>(n);
    }

    // The records whose bytes all reached the file. A run is written front to back, so they are a
    // prefix, and each of them is as written as a record written alone whose write returned.
    size_t landed = 0;
    while (landed < ends.size() && ends[landed] <= done) ++landed;
    const size_t landed_bytes = landed == 0 ? 0 : ends[landed - 1];
    if (landed > 0) (void)advance_after_write(landed_bytes, landed);

    // No fsync here under the other policies - the flush loop syncs at group commit boundaries.
    // Under `EVERY` the whole promise is that `OK` means the record is on the disk, so a failure is
    // reported the way a failed write is, which answers the client `ERR ...` and leaves the node
    // serving. What a retry costs is in the operations guide: the records are in this WAL either
    // way, so a client that resends one stores it twice, and that is the honest trade against being
    // told a write is durable when it is not. One sync covers the run, which is group commit.
    run.landed = landed;
    if (landed > 0 && allow_fsync && fsync_policy_ == FsyncPolicy::EVERY) {
        if (const int err = fsync_or_record("a write under fsync-policy=every"); err != 0) {
            run.sync_errno = err;
        } else {
            pending_sync_ = 0;
        }
    }

    if (write_errno != 0) {
        // A write that failed **after** writing part of a record leaves bytes no reader can parse
        // past, and everything appended behind them is unreachable on replay: measured 2 of 2
        // acknowledged writes lost (#126). So the file is abandoned rather than written to again.
        // A write that failed on a record boundary - nothing of the next record reached the file -
        // leaves it intact, and abandoning it would make a full disk produce one empty WAL file per
        // refused write. Synced first, above, so the records that did land keep their promise.
        if (done > landed_bytes) abandon_torn_file(done - landed_bytes, write_errno);
        run.write_errno = write_errno;
    }
    return run;
}

WalPosition WALWriter::append(const DeltaUpdate& update, const Level* levels) {
    // A batch of one. The position is captured before any rotation, and that ordering is the whole
    // of #98: `rotate()` publishes `{next_index, next_offset}` in one store, so after it
    // `current_position()` is in a file this record is not in - and a caller who needs to name this
    // record's position has no way back to it. The replication wire needs exactly that, per record.
    const WalDelta record{&update, levels, nullptr, 0, false};
    WalBatchOutcome outcome;
    if (append_batch(std::span<const WalDelta>(&record, 1),
                     std::span<WalBatchOutcome>(&outcome, 1)) == 0) {
        throw std::runtime_error(outcome.error);
    }
    return outcome.position;
}

WalPosition WALWriter::append_with_origin(const DeltaUpdate& update, const Level* levels,
                                    uint16_t origin_node_id, const HLCTimestamp& hlc) {
    OB_LOG_DEBUG("wal", "append_with_origin: seq=%lu origin=%u hlc={%lu,%u,%u} levels=%u",
                 static_cast<unsigned long>(update.sequence_number),
                 static_cast<unsigned>(origin_node_id),
                 static_cast<unsigned long>(hlc.physical_ns),
                 static_cast<unsigned>(hlc.logical),
                 static_cast<unsigned>(hlc.node_id),
                 static_cast<unsigned>(update.n_levels));

    const WalDelta record{&update, levels, &hlc, origin_node_id, false};
    WalBatchOutcome outcome;
    if (append_batch(std::span<const WalDelta>(&record, 1),
                     std::span<WalBatchOutcome>(&outcome, 1)) == 0) {
        throw std::runtime_error(outcome.error);
    }
    return outcome.position;
}

size_t WALWriter::append_batch(std::span<const WalDelta> records,
                               std::span<WalBatchOutcome> outcomes) {
    if (outcomes.size() < records.size()) {
        throw std::invalid_argument("WALWriter::append_batch: " + std::to_string(outcomes.size()) +
                                    " outcome(s) for " + std::to_string(records.size()) +
                                    " record(s)");
    }

    size_t written = 0;
    size_t i = 0;
    while (i < records.size()) {
        try {
            ensure_open();
        } catch (const std::exception& e) {
            // The file and the reason, not a bad descriptor (#154). This record is refused and the
            // next one tries to open the file again, as the next command did.
            outcomes[i].error = e.what();
            ++i;
            continue;
        }

        // One run: every record up to and including the one that carries the file to the
        // threshold - which is where a record written alone would have rotated.
        const uint64_t start = current_position().offset;
        size_t used = 0;
        batch_ends_.clear();
        batch_deltas_.clear();
        size_t j = i;
        try {
            do {
                if (records[j].gap_before) {
                    encode_gap(*records[j].update, batch_buf_, used);
                    batch_ends_.push_back(used);
                }
                encode_delta(records[j], batch_buf_, used);
                batch_ends_.push_back(used);
                batch_deltas_.push_back(batch_ends_.size() - 1);
                ++j;
            } while (j < records.size() && start + used < rotate_threshold_);
        } catch (const std::exception& e) {
            // Out of memory while assembling: nothing of this run reached the file, and nothing
            // after it will fit either.
            for (size_t k = i; k < records.size(); ++k) outcomes[k].error = e.what();
            return written;
        }

        const RunResult run = write_run(batch_buf_.data(), batch_ends_, /*allow_fsync=*/true);

        // The batch records whose DELTA reached the file. A GAP that landed in front of a DELTA
        // that did not is in the file and counted, as a GAP written alone before a refused write
        // was; the record it belongs to is refused.
        size_t landed = 0;
        while (landed < j - i && batch_deltas_[landed] < run.landed) ++landed;
        for (size_t k = 0; k < landed; ++k) {
            const size_t delta = batch_deltas_[k];
            const size_t within = delta == 0 ? 0 : batch_ends_[delta - 1];
            WalBatchOutcome& out = outcomes[i + k];
            out.position = WalPosition{run.first.file_index,
                                       run.first.offset + static_cast<uint32_t>(within)};
            if (run.sync_errno == 0) {
                out.error.clear();
                ++written;
            } else {
                out.error = sync_failed(run.sync_errno);
            }
        }
        OB_LOG_DEBUG("wal", "append_batch: %zu of %zu record(s) in one write of %zu bytes at file "
                            "%u offset %u%s",
                     landed, j - i, used, run.first.file_index, run.first.offset,
                     run.sync_errno == 0 ? "" : " - not confirmed, the sync failed");

        if (run.write_errno != 0) {
            // The record the write stopped at is refused, and the records behind it are tried
            // again in the next run - as the next command after a refused write was. Not the ones
            // before it: they are in the file, and writing them again would store them twice.
            //
            // And no rotation: a disk that has just refused a write is not asked to write a
            // ROTATE marker as well. The rotation is tried after the next record that is written,
            // which is #153's rule, and a disk that stays full would otherwise log a failed marker
            // for every refused write.
            outcomes[i + landed].error = write_failed(run.write_errno);
            i += landed + 1;
            continue;
        }
        i = j;

        if (current_position().offset >= rotate_threshold_) {
            rotate();
        }
    }
    return written;
}

void WALWriter::set_origin_node_id(uint16_t node_id) {
    origin_node_id_ = node_id;
    OB_LOG_DEBUG("wal", "set_origin_node_id: %u", static_cast<unsigned>(node_id));
}

void WALWriter::append_gap(uint64_t sequence_number, uint64_t timestamp_ns) {
    WALRecord hdr{};
    hdr.sequence_number = sequence_number;
    hdr.timestamp_ns    = timestamp_ns;
    hdr.checksum        = crc32c(nullptr, 0); // empty payload
    hdr.payload_len     = 0;
    hdr.record_type     = WAL_RECORD_GAP;
    hdr._pad            = 0;

    write_record(hdr, nullptr, 0);
}

void WALWriter::append_version_vector(const uint8_t* payload, size_t payload_len) {
    // Backstop for #78. The header describes the length in a uint16_t, and write_record()
    // writes whatever it is handed, so a payload above the limit would produce a record
    // claiming to be shorter than it is — and every replay after it would read the middle
    // of this payload as the next header. The serialisers bound themselves; this refuses
    // outright, because losing the version vector costs duplicates and losing the WAL tail
    // costs rows.
    if (payload_len > WAL_MAX_PAYLOAD_LEN) {
        OB_LOG_ERROR("wal",
                     "Refusing to append version vector: %zu bytes exceeds the %zu a record "
                     "header can describe",
                     payload_len, WAL_MAX_PAYLOAD_LEN);
        return;
    }
    WALRecord hdr{};
    hdr.sequence_number = 0;
    hdr.timestamp_ns    = 0;
    hdr.checksum        = crc32c(payload, payload_len);
    hdr.payload_len     = static_cast<uint16_t>(payload_len);
    hdr.record_type     = WAL_RECORD_VERSION_VECTOR;
    hdr._pad            = 0;

    write_record(hdr, payload, payload_len, /*allow_fsync=*/false);

    const WalPosition vv_pos = current_position();
    OB_LOG_DEBUG("wal", "Version vector appended (not fsynced): bytes=%zu file=%u offset=%u",
                 payload_len, vv_pos.file_index, vv_pos.offset);
}

void WALWriter::append_held_sequences(const uint8_t* payload, size_t payload_len) {
    // Backstop for #78. The header describes the length in a uint16_t, and write_record()
    // writes whatever it is handed, so a payload above the limit would produce a record
    // claiming to be shorter than it is — and every replay after it would read the middle
    // of this payload as the next header. The serialisers bound themselves; this refuses
    // outright, because losing the held sequences costs duplicates and losing the WAL tail
    // costs rows.
    if (payload_len > WAL_MAX_PAYLOAD_LEN) {
        OB_LOG_ERROR("wal",
                     "Refusing to append held sequences: %zu bytes exceeds the %zu a record "
                     "header can describe",
                     payload_len, WAL_MAX_PAYLOAD_LEN);
        return;
    }
    WALRecord hdr{};
    hdr.sequence_number = 0;
    hdr.timestamp_ns    = 0;
    hdr.checksum        = crc32c(payload, payload_len);
    hdr.payload_len     = static_cast<uint16_t>(payload_len);
    hdr.record_type     = WAL_RECORD_HELD_SEQUENCES;
    hdr._pad            = 0;

    write_record(hdr, payload, payload_len, /*allow_fsync=*/false);

    const WalPosition held_pos = current_position();
    OB_LOG_DEBUG("wal", "Held sequences appended (not fsynced): bytes=%zu file=%u offset=%u",
                 payload_len, held_pos.file_index, held_pos.offset);
}

void WALWriter::append_checkpoint(uint64_t timestamp_ns, WalPosition covered) {
    uint8_t payload[CHECKPOINT_PAYLOAD_BYTES];
    checkpoint_payload(covered, payload);

    WALRecord hdr{};
    hdr.sequence_number = 0;
    hdr.timestamp_ns    = timestamp_ns;
    hdr.checksum        = crc32c(payload, sizeof(payload));
    hdr.payload_len     = static_cast<uint16_t>(sizeof(payload));
    hdr.record_type     = WAL_RECORD_CHECKPOINT;
    hdr._pad            = 0;

    // No fsync for this one, deliberately. A checkpoint only ever claims that rows are
    // already durable; losing it in a crash makes the next open() replay records the
    // timestamp guard then skips. Fsyncing it cost a measured +0.22 ms (+10.5%) on every
    // FLUSH to protect a record whose loss is harmless.
    write_record(hdr, payload, sizeof(payload), /*allow_fsync=*/false);

    const WalPosition ckpt_pos = current_position();
    OB_LOG_DEBUG("wal",
                 "Checkpoint appended (not fsynced) at file=%u offset=%u, covering records "
                 "before file=%u offset=%u",
                 ckpt_pos.file_index, ckpt_pos.offset, covered.file_index, covered.offset);
}

void WALWriter::append_epoch(const EpochValue& epoch) {
    uint8_t payload[8];
    epoch_to_payload(epoch, payload);

    WALRecord hdr{};
    hdr.sequence_number = 0;
    hdr.timestamp_ns    = 0;
    hdr.checksum        = crc32c(payload, sizeof(payload));
    hdr.payload_len     = 8;
    hdr.record_type     = WAL_RECORD_EPOCH;
    hdr._pad            = 0;

    write_record(hdr, payload, sizeof(payload));
    current_epoch_ = epoch.term;
}

void WALWriter::rotate() {
    // Write a ROTATE record to the current file.
    WALRecord hdr{};
    hdr.sequence_number = 0;
    hdr.timestamp_ns    = 0;
    hdr.checksum        = crc32c(nullptr, 0);
    hdr.payload_len     = 0;
    hdr.record_type     = WAL_RECORD_ROTATE;
    hdr._pad            = 0;

    // Without a sync of its own, and that is the first half of #153. Replay and catch-up both stop
    // reading a file at its ROTATE record, so from the moment those bytes are in the file the
    // writer must be somewhere else. Written with the sync, a failed fsync under
    // `--fsync-policy every` threw from here with the writer still on this file, and the next
    // record went in **behind the marker**: answered `OK` and gone after a restart - measured, one
    // acknowledged write lost per failed sync of a marker. Nothing is given up by not syncing it:
    // the marker holds no client's data, and `open_current()` syncs the file it leaves before it
    // closes it, recording a failure there as every other sync does.
    try {
        write_record(hdr, nullptr, 0, /*allow_fsync=*/false);
    } catch (const std::exception& e) {
        // The second half of #153: a rotation that fails is not the failure of the record that
        // asked for it, which is already in the file - and throwing from here answered that record
        // `ERR`, so a client that sent it again stored it twice. Two ways to arrive here, and both
        // leave the writer somewhere it may write. Nothing of the marker reached the file: then the
        // file is readable to its end, the next record goes into it, and the rotation is tried
        // again after that one - so this is the one case in which a file passes the threshold by
        // more than one record, because the disk refused the record that would have ended it. Or
        // the marker tore, and `abandon_torn_file()` has already left the file.
        OB_LOG_ERROR("wal",
                     "could not end %s with a ROTATE record: %s. The record that crossed the "
                     "rotation threshold is written; the rotation is tried again after the next one",
                     wal_filename(dir_, current_position().file_index).c_str(), e.what());
        return;
    }

    if (leave_for_next_file()) {
        const WalPosition now = current_position();
        OB_LOG_DEBUG("wal", "rotated to file %u at offset %u", now.file_index, now.offset);
    }
}

bool WALWriter::leave_for_next_file() noexcept {
    // The index and the offset are published in **one** store. Incrementing the index and letting
    // open_current() set the offset afterwards would make `(N+1, previous file's offset)`
    // observable - the very pair #85 removed. The cross-thread test found that at 96 observations
    // in 4.3 million.
    const WalPosition at = current_position();
    const uint32_t next_index = at.file_index + 1;
    try {
        const uint32_t next_offset = open_current(next_index);
        position_.store(WalPosition{next_index, next_offset}, std::memory_order_relaxed);
        return true;
    } catch (const std::exception& e) {
        // `open_current()` has closed the old descriptor, so there is no file to write to now, and
        // that is the right way round: the old one must not be written to again. `ensure_open()`
        // tries once more before every write, and until it can, every write is refused with this
        // reason rather than with a bad descriptor (#154).
        if (no_file_.begin()) {
            OB_LOG_ERROR("wal",
                         "%s: this node has no WAL file to write to, so every write is refused "
                         "until one can be opened; each write tries again",
                         e.what());
        }
        return false;
    }
}

void WALWriter::ensure_open() {
    if (fd_ >= 0) return;
    const uint32_t next_index = current_position().file_index + 1;
    uint32_t next_offset = 0;
    try {
        next_offset = open_current(next_index);
    } catch (const std::exception&) {
        (void)no_file_.begin();   // counted, and said once: in leave_for_next_file()
        throw;
    }
    position_.store(WalPosition{next_index, next_offset}, std::memory_order_relaxed);
    const uint64_t refused = no_file_.end();
    OB_LOG_INFO("wal",
                "opened %s at offset %u; the writer had no WAL file for %llu attempt(s) to open "
                "one, and the writes refused meanwhile were answered with the reason",
                wal_filename(dir_, next_index).c_str(), next_offset,
                static_cast<unsigned long long>(refused));
}

void WALWriter::abandon_torn_file(size_t stranded_bytes, int write_errno) noexcept {
    const WalPosition at = current_position();
    OB_LOG_ERROR("wal",
                 "torn record in %s at offset %u: %zu byte(s) reached the file and the rest failed "
                 "with %s. Opening the next file: everything appended behind those bytes would be "
                 "unreachable on replay",
                 wal_filename(dir_, at.file_index).c_str(), at.offset, stranded_bytes,
                 std::strerror(write_errno));

    // No ROTATE record, which is the whole reason this is not `rotate()`: that function's first act
    // is to write a record into the file we have just established cannot be written to. What
    // replaces the marker is the replayer's rule - a checksum mismatch in a file that is not the
    // last one is a tear, so replay continues with the next file (#126).
    //
    // A next file that cannot be opened is said by `leave_for_next_file()`, and is retried before
    // the next write (#154) - once, this left the writer with no descriptor for the rest of the
    // process. Nothing is thrown from here: that would replace the write's own error, the one that
    // says why the disk refused, with a second one about the recovery.
    if (leave_for_next_file()) {
        const WalPosition now = current_position();
        OB_LOG_INFO("wal", "abandoned file %u after a torn record; writing to %u at offset %u",
                    at.file_index, now.file_index, now.offset);
    }
}

bool WALWriter::flush() {
    if (fd_ >= 0 && fsync_policy_ != FsyncPolicy::NONE) {
        if (fsync_or_record("flush") != 0) {
            // `pending_sync_` is left where it is, so the count still says a sync is owed. It
            // cannot be paid - Linux has already dropped the error and cleaned the pages - but a
            // writer that reports nothing pending after a failed sync is a writer telling the
            // flush loop there is nothing left to do.
            return false;
        }
        pending_sync_ = 0;
    }
    return true;
}

bool WALWriter::sync() {
    return flush();
}

size_t WALWriter::truncate_before(uint32_t before_index) {
    size_t removed = 0;
    if (!std::filesystem::exists(dir_)) return 0;

    for (auto& entry : std::filesystem::directory_iterator(dir_)) {
        const std::string name = entry.path().filename().string();
        if (name.size() == 14 &&
            name.substr(0, 4) == "wal_" &&
            name.substr(10) == ".bin") {
            uint32_t idx = static_cast<uint32_t>(std::stoul(name.substr(4, 6)));
            if (idx < before_index) {
                std::filesystem::remove(entry.path());
                ++removed;
            }
        }
    }
    return removed;
}

// ── WALReplayer ───────────────────────────────────────────────────────────────

WALReplayer::WALReplayer(std::string_view dir)
    : dir_(dir)
{}

uint64_t WALReplayer::replay(
    std::function<void(const WALRecord&, const uint8_t* payload)> cb)
{
    // Reset epoch tracking for this replay.
    last_epoch_ = 0;
    tears_skipped_ = 0;

    // Collect all wal_*.bin files and sort them by index.
    std::vector<std::pair<uint32_t, std::string>> files;

    if (!std::filesystem::exists(dir_)) {
        return 0;
    }

    for (auto& entry : std::filesystem::directory_iterator(dir_)) {
        const std::string name = entry.path().filename().string();
        if (name.size() == 14 &&
            name.substr(0, 4) == "wal_" &&
            name.substr(10) == ".bin") {
            uint32_t idx = static_cast<uint32_t>(std::stoul(name.substr(4, 6)));
            files.emplace_back(idx, entry.path().string());
        }
    }

    std::sort(files.begin(), files.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    uint64_t last_good_seq = 0;

    for (auto& [idx, path] : files) {
        // Whether this is the highest-numbered file, which is what separates a crash tail from a
        // torn record. Taken from the sorted list rather than from the writer's current position:
        // a replay reads a directory, and the writer that produced it is gone.
        const bool is_last = (idx == files.back().first);
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) continue;

        while (true) {
            WALRecord hdr{};
            ssize_t n = ::read(fd, &hdr, sizeof(WALRecord));
            if (n == 0) break; // EOF
            if (n != static_cast<ssize_t>(sizeof(WALRecord))) break; // truncated

            // Read payload.
            std::vector<uint8_t> payload(hdr.payload_len);
            if (hdr.payload_len > 0) {
                size_t remaining = hdr.payload_len;
                uint8_t* ptr = payload.data();
                while (remaining > 0) {
                    ssize_t r = ::read(fd, ptr, remaining);
                    if (r <= 0) goto done_file; // truncated or error
                    ptr += r;
                    remaining -= static_cast<size_t>(r);
                }
            }

            // Verify CRC32C.
            const uint32_t expected = crc32c(payload.data(), hdr.payload_len);
            if (expected != hdr.checksum) {
                // A mismatch ends this file. In the **last** file it is the crash tail - the
                // record was being written when the process died - and in any earlier file it is a
                // torn record the writer abandoned that file for (#126), whose successors are
                // intact: stopping the whole replay there lost writes that had been acknowledged,
                // measured at 2 of 2. One branch for the same reason as in `replay_v2()` below.
                if (is_last) {
                    OB_LOG_WARN("wal",
                                "checksum mismatch in %s, the last WAL file: this is the tail of a "
                                "record that was being written when the process stopped",
                                path.c_str());
                } else {
                    ++tears_skipped_;
                    OB_LOG_WARN("wal",
                                "checksum mismatch in %s, which is not the last WAL file: treating "
                                "it as a torn record and continuing with the next file",
                                path.c_str());
                }
                goto done_file;
            }

            // ROTATE record signals end of this file's useful content.
            if (hdr.record_type == WAL_RECORD_ROTATE) {
                break;
            }

            // Track highest epoch seen in WAL_RECORD_EPOCH records.
            if (hdr.record_type == WAL_RECORD_EPOCH && hdr.payload_len == 8) {
                const EpochValue ev = epoch_from_payload(payload.data());
                if (ev.term > last_epoch_) {
                    last_epoch_ = ev.term;
                }
            }

            // Invoke callback.
            cb(hdr, payload.empty() ? nullptr : payload.data());

            if (hdr.sequence_number > 0) {
                last_good_seq = hdr.sequence_number;
            }
        }

        done_file:
        ::close(fd);
    }

    return last_good_seq;
}

WALReplayer::LastCheckpoint WALReplayer::find_last_checkpoint()
{
    // Reusing replay_v2 here rather than writing a second parser is deliberate: two parsers for
    // one format eventually disagree, and this one only needs record types and ordering.
    LastCheckpoint last;
    uint64_t ordinal = 0;
    replay_v2([&](const WALReplayContext& ctx) {
        ++ordinal;
        if (!last.any_record || ctx.wal_file_index < last.first_file_index) {
            last.first_file_index = ctx.wal_file_index;
        }
        last.any_record = true;
        if (ctx.header.record_type == WAL_RECORD_CHECKPOINT) {
            last.ordinal = ordinal;
            // Reset by every checkpoint, so a last one written by an older build - empty payload -
            // is read by ordinal even after newer ones (#159).
            last.covered = checkpoint_covered(ctx.payload, ctx.payload_len);
        }
    });
    last.records = ordinal;
    return last;
}

uint64_t WALReplayer::replay_after(const LastCheckpoint& last, WALReplayCallbackV2 cb)
{
    // Forward what the checkpoint does not cover. **Every record after the last checkpoint is
    // forwarded, whatever its payload says**, and a checkpoint that says what it covered adds to
    // that the records **before** it that start at or after that position (#159): the ones written
    // while its flush wrote segments without the engine's lock, whose rows were still queued when
    // the checkpoint was appended. So the position can only give records back, never take one away
    // - a payload no writer produces (a position past the checkpoint itself) is read as covering
    // everything before the checkpoint, which is the most an older build's checkpoint ever claimed.
    // One written before #159 says nothing and is read as it always was, by ordinal; the records it
    // wrongly covered are not in the log's own account of itself, so they cannot be told apart here.
    uint64_t seen = 0;
    uint64_t forwarded = 0;
    uint64_t given_back = 0;   // records before the last checkpoint, forwarded because of its position
    uint64_t last_seq = replay_v2([&](const WALReplayContext& ctx) {
        ++seen;
        if (seen <= last.ordinal) {
            if (!last.covered || seen == last.ordinal) return;
            const WalPosition at{ctx.wal_file_index, static_cast<uint32_t>(ctx.wal_byte_offset)};
            if (wal_position_before(at, *last.covered)) return;
            ++given_back;
        }
        ++forwarded;
        cb(ctx);
    });

    // Three states, not two: a log with no checkpoint at all was rendered as a checkpoint that
    // says nothing of what it covered, which is the older build's case - and since #160 the first
    // one is common, because a first flush cut short, or a failed sync, leaves no checkpoint.
    const std::string resuming =
        last.covered      ? "at file " + std::to_string(last.covered->file_index) + " offset " +
                                std::to_string(last.covered->offset)
        : last.ordinal == 0 ? std::string("from the start of the log, which holds no checkpoint")
                            : std::string("after the checkpoint record, which says nothing of "
                                          "what it covered");
    OB_LOG_INFO("wal",
                "Replay after checkpoint: records=%llu last_checkpoint_ordinal=%llu "
                "resuming %s, forwarded=%llu (of which %llu written before the checkpoint while "
                "its flush wrote segments)",
                static_cast<unsigned long long>(seen),
                static_cast<unsigned long long>(last.ordinal), resuming.c_str(),
                static_cast<unsigned long long>(forwarded),
                static_cast<unsigned long long>(given_back));

    return last_seq;
}

uint64_t WALReplayer::replay_after_checkpoint(WALReplayCallbackV2 cb)
{
    // Two passes rather than buffering the tail in memory: the first finds the last checkpoint,
    // the second invokes cb for the records it does not cover.
    return replay_after(find_last_checkpoint(), std::move(cb));
}

uint64_t WALReplayer::replay_v2(WALReplayCallbackV2 cb)
{
    // Reset epoch tracking for this replay.
    last_epoch_ = 0;
    tears_skipped_ = 0;

    // Collect all wal_*.bin files and sort them by index.
    std::vector<std::pair<uint32_t, std::string>> files;

    if (!std::filesystem::exists(dir_)) {
        return 0;
    }

    for (auto& entry : std::filesystem::directory_iterator(dir_)) {
        const std::string name = entry.path().filename().string();
        if (name.size() == 14 &&
            name.substr(0, 4) == "wal_" &&
            name.substr(10) == ".bin") {
            uint32_t idx = static_cast<uint32_t>(std::stoul(name.substr(4, 6)));
            files.emplace_back(idx, entry.path().string());
        }
    }

    std::sort(files.begin(), files.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    uint64_t last_good_seq = 0;

    for (auto& [idx, path] : files) {
        // Whether this is the highest-numbered file, which is what separates a crash tail from a
        // torn record. Taken from the sorted list rather than from the writer's current position:
        // a replay reads a directory, and the writer that produced it is gone.
        const bool is_last = (idx == files.back().first);
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) continue;

        while (true) {
            // Where this record starts, before anything is read from it. Recovery compares this
            // against the position a segment recorded for the same symbol (#63), so it has to be
            // the offset of the header rather than of the payload.
            const off_t record_start = ::lseek(fd, 0, SEEK_CUR);

            // Read the base 24-byte header first.
            WALRecord base_hdr{};
            ssize_t n = ::read(fd, &base_hdr, sizeof(WALRecord));
            if (n == 0) break; // EOF
            if (n != static_cast<ssize_t>(sizeof(WALRecord))) break; // truncated

            // Determine version from the _pad/version field.
            const uint8_t version = base_hdr._pad;

            uint16_t origin_node_id = 0;
            HLCTimestamp hlc_ts{};

            if (version == 1) {
                // Read the additional 14 bytes (2B origin + 12B HLC).
                uint8_t ext_buf[14]{};
                ssize_t ext_n = ::read(fd, ext_buf, sizeof(ext_buf));
                if (ext_n != static_cast<ssize_t>(sizeof(ext_buf))) {
                    // Corrupted extended header — skip this record.
                    OB_LOG_WARN("wal", "Corrupted extended WAL header at seq=%lu, skipping",
                                static_cast<unsigned long>(base_hdr.sequence_number));
                    // Try to skip the payload to continue reading.
                    if (base_hdr.payload_len > 0) {
                        ::lseek(fd, base_hdr.payload_len, SEEK_CUR);
                    }
                    continue;
                }
                std::memcpy(&origin_node_id, ext_buf, 2);
                hlc_ts = HLCTimestamp::deserialize(ext_buf + 2);
            }

            OB_LOG_DEBUG("wal", "replay_v2: seq=%lu version=%u origin=%u",
                         static_cast<unsigned long>(base_hdr.sequence_number),
                         static_cast<unsigned>(version),
                         static_cast<unsigned>(origin_node_id));

            // Read payload.
            std::vector<uint8_t> payload(base_hdr.payload_len);
            if (base_hdr.payload_len > 0) {
                size_t remaining = base_hdr.payload_len;
                uint8_t* ptr = payload.data();
                while (remaining > 0) {
                    ssize_t r = ::read(fd, ptr, remaining);
                    if (r <= 0) goto done_file_v2; // truncated or error
                    ptr += r;
                    remaining -= static_cast<size_t>(r);
                }
            }

            // Verify CRC32C.
            {
                const uint32_t expected = crc32c(payload.data(), base_hdr.payload_len);
                if (expected != base_hdr.checksum) {
                    // The same decision as in `replay()` above, and one branch for the same
                    // reason: ending this file and returning from the whole replay differ only
                    // when a later file exists, and in the last file there is none. A mutation
                    // that made the last file behave like the others survived, because it is the
                    // same behaviour. What differs is which case it is, and that is observable in
                    // `tears_skipped()` and in the message rather than in the control flow (#126).
                    if (is_last) {
                        OB_LOG_WARN("wal",
                                    "checksum mismatch in %s, the last WAL file: this is the tail "
                                    "of a record that was being written when the process stopped",
                                    path.c_str());
                    } else {
                        ++tears_skipped_;
                        OB_LOG_WARN("wal",
                                    "checksum mismatch in %s, which is not the last WAL file: "
                                    "treating it as a torn record and continuing with the next",
                                    path.c_str());
                    }
                    goto done_file_v2;
                }
            }

            // ROTATE record signals end of this file's useful content.
            if (base_hdr.record_type == WAL_RECORD_ROTATE) {
                break;
            }

            // Track highest epoch seen in WAL_RECORD_EPOCH records.
            if (base_hdr.record_type == WAL_RECORD_EPOCH && base_hdr.payload_len == 8) {
                const EpochValue ev = epoch_from_payload(payload.data());
                if (ev.term > last_epoch_) {
                    last_epoch_ = ev.term;
                }
            }

            // Build context and invoke callback.
            WALReplayContext ctx{};
            ctx.header          = base_hdr;
            ctx.origin_node_id  = origin_node_id;
            ctx.hlc             = hlc_ts;
            ctx.payload         = payload.empty() ? nullptr : payload.data();
            ctx.payload_len     = base_hdr.payload_len;
            ctx.wal_file_index  = idx;
            ctx.wal_byte_offset = record_start < 0 ? 0 : static_cast<uint64_t>(record_start);

            cb(ctx);

            if (base_hdr.sequence_number > 0) {
                last_good_seq = base_hdr.sequence_number;
            }
        }

        done_file_v2:
        ::close(fd);
    }

    return last_good_seq;
}

} // namespace ob
