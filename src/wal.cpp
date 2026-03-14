// WAL Writer and Replayer implementation.
// Uses POSIX file I/O: open(2), write(2), fsync(2), close(2), read(2).
// CRC32C is computed via a software lookup-table (portable, no hardware intrinsics).

#include "orderbook/wal.hpp"

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

// ── Portable CRC32C (software lookup table) ───────────────────────────────────
// Polynomial: 0x1EDC6F41 (Castagnoli)
namespace {

static constexpr uint32_t CRC32C_POLY = 0x82F63B78u; // reflected form

static uint32_t crc32c_table[256];
static bool     crc32c_table_init = false;

static void init_crc32c_table() {
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            crc = (crc >> 1) ^ ((crc & 1u) ? CRC32C_POLY : 0u);
        }
        crc32c_table[i] = crc;
    }
    crc32c_table_init = true;
}

static uint32_t crc32c(const void* data, size_t len) {
    if (!crc32c_table_init) init_crc32c_table();
    const auto* p = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; ++i) {
        crc = (crc >> 8) ^ crc32c_table[(crc ^ p[i]) & 0xFFu];
    }
    return crc ^ 0xFFFFFFFFu;
}

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
                              size_t payload_len) {
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

    if (fsync_policy_ == FsyncPolicy::EVERY) {
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
    if (!crc32c_table_init) init_crc32c_table();

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

} // namespace ob
