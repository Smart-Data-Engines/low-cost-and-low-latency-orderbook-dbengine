// WAL Writer and Replayer implementation.
// Uses POSIX file I/O: open(2), write(2), fsync(2), close(2), read(2).
// CRC32C is computed via a software lookup-table (portable, no hardware intrinsics).

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

} // anonymous namespace

// ── WALWriter ─────────────────────────────────────────────────────────────────

WALWriter::WALWriter(std::string_view dir, size_t rotate_threshold_bytes,
                     FsyncPolicy fsync_policy)
    : fd_(-1)
    , written_(0)
    , rotate_threshold_(rotate_threshold_bytes)
    , fsync_policy_(fsync_policy)
    , dir_(dir)
    , file_index_(0)
    , pending_sync_(0)
{
    // Pre-allocate write buffer (enough for largest possible record).
    write_buf_.reserve(sizeof(WALRecord) + sizeof(DeltaUpdate) + MAX_LEVELS * sizeof(Level));

    // Ensure directory exists.
    std::filesystem::create_directories(dir_);

    // Find the highest existing WAL file index so we continue from there.
    for (auto& entry : std::filesystem::directory_iterator(dir_)) {
        const std::string name = entry.path().filename().string();
        if (name.size() == 14 &&
            name.substr(0, 4) == "wal_" &&
            name.substr(10) == ".bin") {
            uint32_t idx = static_cast<uint32_t>(std::stoul(name.substr(4, 6)));
            if (idx >= file_index_) {
                file_index_ = idx;
            }
        }
    }

    open_current();
}

WALWriter::~WALWriter() {
    if (fd_ >= 0) {
        ::fsync(fd_);
        ::close(fd_);
        fd_ = -1;
    }
}

void WALWriter::open_current() {
    if (fd_ >= 0) {
        ::fsync(fd_);
        ::close(fd_);
        fd_ = -1;
    }

    const std::string path = wal_filename(dir_, file_index_);
    // O_APPEND ensures all writes go to the end even across processes.
    fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0) {
        throw std::runtime_error("WALWriter: cannot open " + path +
                                 ": " + std::strerror(errno));
    }

    // Determine how many bytes are already in the file (for rotation accounting).
    const off_t pos = ::lseek(fd_, 0, SEEK_END);
    written_ = (pos >= 0) ? static_cast<size_t>(pos) : 0;
}

void WALWriter::write_record(const WALRecord& hdr, const void* payload,
                              size_t payload_len, bool allow_fsync) {
    // Combine header + payload into a single write to minimize syscalls.
    const size_t total = sizeof(WALRecord) + payload_len;
    write_buf_.resize(total);
    std::memcpy(write_buf_.data(), &hdr, sizeof(WALRecord));
    if (payload_len > 0) {
        std::memcpy(write_buf_.data() + sizeof(WALRecord), payload, payload_len);
    }

    size_t remaining = total;
    const uint8_t* ptr = write_buf_.data();
    while (remaining > 0) {
        ssize_t n = ::write(fd_, ptr, remaining);
        if (n < 0) {
            throw std::runtime_error(std::string("WALWriter: write failed: ") +
                                     std::strerror(errno));
        }
        ptr += n;
        remaining -= static_cast<size_t>(n);
    }

    // No fsync here — caller is responsible for calling sync() at group commit boundaries.
    // Exception: FsyncPolicy::EVERY fsyncs after every record.
    written_ += total;
    ++pending_sync_;

    if (allow_fsync && fsync_policy_ == FsyncPolicy::EVERY) {
        ::fsync(fd_);
        pending_sync_ = 0;
    }
}

void WALWriter::append(const DeltaUpdate& update, const Level* levels) {
    // Build payload: DeltaUpdate header (fixed part) + n_levels * sizeof(Level).
    const size_t levels_bytes = update.n_levels * sizeof(Level);
    const size_t payload_len  = sizeof(DeltaUpdate) + levels_bytes;

    // Reuse pre-allocated buffer for payload (avoid heap alloc per record).
    // We use a separate region of write_buf_ that write_record will overwrite anyway,
    // so build payload in a local stack buffer for small payloads, or reuse write_buf_.
    // Max payload: sizeof(DeltaUpdate) + 1024 * sizeof(Level) ≈ 200 + 24K ≈ 25K — fits on stack.
    alignas(8) uint8_t payload[sizeof(DeltaUpdate) + MAX_LEVELS * sizeof(Level)];
    std::memcpy(payload, &update, sizeof(DeltaUpdate));
    if (levels_bytes > 0) {
        std::memcpy(payload + sizeof(DeltaUpdate), levels, levels_bytes);
    }

    WALRecord hdr{};
    hdr.sequence_number = update.sequence_number;
    hdr.timestamp_ns    = update.timestamp_ns;
    hdr.checksum        = crc32c(payload, payload_len);
    hdr.payload_len     = static_cast<uint16_t>(payload_len);
    hdr.record_type     = WAL_RECORD_DELTA;
    hdr._pad            = 0;

    write_record(hdr, payload, payload_len);

    // Auto-rotate if threshold exceeded.
    if (written_ >= rotate_threshold_) {
        rotate();
    }
}

void WALWriter::write_record_v2(const WALRecordV2& hdr, const void* payload,
                                 size_t payload_len) {
    // Combine 38B header + payload into a single write to minimize syscalls.
    const size_t total = sizeof(WALRecordV2) + payload_len;
    write_buf_.resize(total);
    std::memcpy(write_buf_.data(), &hdr, sizeof(WALRecordV2));
    if (payload_len > 0) {
        std::memcpy(write_buf_.data() + sizeof(WALRecordV2), payload, payload_len);
    }

    size_t remaining = total;
    const uint8_t* ptr = write_buf_.data();
    while (remaining > 0) {
        ssize_t n = ::write(fd_, ptr, remaining);
        if (n < 0) {
            throw std::runtime_error(std::string("WALWriter: write failed: ") +
                                     std::strerror(errno));
        }
        ptr += n;
        remaining -= static_cast<size_t>(n);
    }

    written_ += total;
    ++pending_sync_;

    if (fsync_policy_ == FsyncPolicy::EVERY) {
        ::fsync(fd_);
        pending_sync_ = 0;
    }
}

void WALWriter::append_with_origin(const DeltaUpdate& update, const Level* levels,
                                    uint16_t origin_node_id, const HLCTimestamp& hlc) {
    // Build payload: DeltaUpdate header + n_levels * sizeof(Level).
    const size_t levels_bytes = update.n_levels * sizeof(Level);
    const size_t payload_len  = sizeof(DeltaUpdate) + levels_bytes;

    alignas(8) uint8_t payload[sizeof(DeltaUpdate) + MAX_LEVELS * sizeof(Level)];
    std::memcpy(payload, &update, sizeof(DeltaUpdate));
    if (levels_bytes > 0) {
        std::memcpy(payload + sizeof(DeltaUpdate), levels, levels_bytes);
    }

    WALRecordV2 hdr{};
    hdr.sequence_number = update.sequence_number;
    hdr.timestamp_ns    = update.timestamp_ns;
    hdr.checksum        = crc32c(payload, payload_len);
    hdr.payload_len     = static_cast<uint16_t>(payload_len);
    hdr.record_type     = WAL_RECORD_DELTA;
    hdr.version         = 1;
    hdr.origin_node_id  = origin_node_id;
    hlc.serialize(hdr.hlc_data);

    OB_LOG_DEBUG("wal", "append_with_origin: seq=%lu origin=%u hlc={%lu,%u,%u} payload=%u",
                 static_cast<unsigned long>(update.sequence_number),
                 static_cast<unsigned>(origin_node_id),
                 static_cast<unsigned long>(hlc.physical_ns),
                 static_cast<unsigned>(hlc.logical),
                 static_cast<unsigned>(hlc.node_id),
                 static_cast<unsigned>(payload_len));

    write_record_v2(hdr, payload, payload_len);

    // Auto-rotate if threshold exceeded.
    if (written_ >= rotate_threshold_) {
        rotate();
    }
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

    OB_LOG_DEBUG("wal", "Version vector appended (not fsynced): bytes=%zu file=%u offset=%zu",
                 payload_len, current_file_index(), current_offset());
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

    OB_LOG_DEBUG("wal", "Held sequences appended (not fsynced): bytes=%zu file=%u offset=%zu",
                 payload_len, current_file_index(), current_offset());
}

void WALWriter::append_checkpoint(uint64_t timestamp_ns) {
    WALRecord hdr{};
    hdr.sequence_number = 0;
    hdr.timestamp_ns    = timestamp_ns;
    hdr.checksum        = crc32c(nullptr, 0); // empty payload
    hdr.payload_len     = 0;
    hdr.record_type     = WAL_RECORD_CHECKPOINT;
    hdr._pad            = 0;

    // No fsync for this one, deliberately. A checkpoint only ever claims that rows are
    // already durable; losing it in a crash makes the next open() replay records the
    // timestamp guard then skips. Fsyncing it cost a measured +0.22 ms (+10.5%) on every
    // FLUSH to protect a record whose loss is harmless.
    write_record(hdr, nullptr, 0, /*allow_fsync=*/false);

    OB_LOG_DEBUG("wal", "Checkpoint appended (not fsynced): file=%u offset=%zu",
                 current_file_index(), current_offset());
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

    write_record(hdr, nullptr, 0);

    // Open the next file.
    ++file_index_;
    open_current();
}

void WALWriter::flush() {
    if (fd_ >= 0 && fsync_policy_ != FsyncPolicy::NONE) {
        ::fsync(fd_);
        pending_sync_ = 0;
    }
}

void WALWriter::sync() {
    flush();
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
                // Checksum mismatch — stop replay.
                ::close(fd);
                return last_good_seq;
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

uint64_t WALReplayer::replay_after_checkpoint(WALReplayCallbackV2 cb)
{
    // Pass 1: find the ordinal of the last CHECKPOINT record. Reusing replay_v2 here
    // rather than writing a second parser is deliberate: two parsers for one format
    // eventually disagree, and this one only needs record types and ordering.
    uint64_t ordinal = 0;
    uint64_t last_checkpoint_ordinal = 0;   // 0 = no checkpoint found
    replay_v2([&](const WALReplayContext& ctx) {
        ++ordinal;
        if (ctx.header.record_type == WAL_RECORD_CHECKPOINT) {
            last_checkpoint_ordinal = ordinal;
        }
    });

    // Pass 2: forward everything after that ordinal.
    uint64_t seen = 0;
    uint64_t forwarded = 0;
    uint64_t last_seq = replay_v2([&](const WALReplayContext& ctx) {
        ++seen;
        if (seen <= last_checkpoint_ordinal) return;
        ++forwarded;
        cb(ctx);
    });

    OB_LOG_INFO("wal",
                "Replay after checkpoint: records=%llu last_checkpoint_ordinal=%llu "
                "forwarded=%llu",
                static_cast<unsigned long long>(seen),
                static_cast<unsigned long long>(last_checkpoint_ordinal),
                static_cast<unsigned long long>(forwarded));

    return last_seq;
}

uint64_t WALReplayer::replay_v2(WALReplayCallbackV2 cb)
{
    // Reset epoch tracking for this replay.
    last_epoch_ = 0;

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
                    // Checksum mismatch — stop replay.
                    ::close(fd);
                    return last_good_seq;
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
