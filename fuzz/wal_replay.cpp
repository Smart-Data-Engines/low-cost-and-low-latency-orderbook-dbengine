#include "support.hpp"
#include "orderbook/crc32c.hpp"
#include "orderbook/wal.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <span>
#include <string>
#include <system_error>
#include <vector>

namespace {

class ScratchWal {
public:
    ScratchWal() {
        std::string pattern = (std::filesystem::temp_directory_path() / "ob_fuzz_wal_XXXXXX").string();
        std::vector<char> name(pattern.begin(), pattern.end());
        name.push_back('\0');
        const char* created = ::mkdtemp(name.data());
        ob::fuzz::require(created != nullptr, "cannot create a private WAL directory");
        directory_ = created;
        OB_LOG_INFO("fuzz", "Created private WAL directory: %s", directory_.c_str());
    }

    ~ScratchWal() {
        OB_LOG_INFO("fuzz", "Removing private WAL directory: %s", directory_.c_str());
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
        ob::fuzz::require(!error, "cannot remove the private WAL directory");
    }

    const std::string& directory() const {
        OB_LOG_DEBUG("fuzz", "Using private WAL directory: %s", directory_.c_str());
        return directory_;
    }

    void replace(std::span<const uint8_t> bytes) const {
        OB_LOG_DEBUG("fuzz", "Replacing private WAL input: bytes=%zu", bytes.size());
        std::ofstream file(std::filesystem::path(directory_) / "wal_000000.bin",
                           std::ios::binary | std::ios::trunc);
        ob::fuzz::require(file.is_open(), "cannot open the private WAL input");
        if (!bytes.empty()) {
            file.write(reinterpret_cast<const char*>(bytes.data()),
                       static_cast<std::streamsize>(bytes.size()));
        }
        file.close();
        ob::fuzz::require(file.good(), "writing or closing the private WAL input failed");
    }

private:
    std::string directory_;
};

struct Record {
    // Preserve every decoded header field across the two replay entry points.
    std::array<uint64_t, 11> fields;
    std::vector<uint8_t> payload;
    bool operator==(const Record&) const = default;
};

Record observe(const ob::WALReplayContext& ctx, std::span<const uint8_t> input) {
    OB_LOG_DEBUG("fuzz", "Decoded WAL record: offset=%llu bytes=%zu",
                 static_cast<unsigned long long>(ctx.wal_byte_offset), ctx.payload_len);
    const size_t header_size = ctx.header._pad == 1 ? sizeof(ob::WALRecordV2) : sizeof(ob::WALRecord);
    ob::fuzz::require(ctx.wal_file_index == 0, "replay named a file that was not written");
    ob::fuzz::require(ctx.wal_byte_offset <= input.size(), "WAL record starts beyond the input");
    const size_t offset = static_cast<size_t>(ctx.wal_byte_offset);
    ob::fuzz::require(header_size <= input.size() - offset, "decoded WAL header is truncated");
    ob::fuzz::require(ctx.payload_len <= input.size() - offset - header_size,
                      "decoded WAL payload is truncated");
    ob::fuzz::require(ctx.payload_len == ctx.header.payload_len, "WAL payload lengths disagree");
    Record record{{ctx.wal_byte_offset, ctx.header.sequence_number, ctx.header.timestamp_ns,
                   ctx.header.checksum, ctx.header.record_type, ctx.header._pad, ctx.origin_node_id,
                   ctx.hlc.physical_ns, ctx.hlc.logical, ctx.hlc.node_id, ctx.payload_len}, {}};
    if (ctx.payload_len != 0) {
        ob::fuzz::require(ctx.payload != nullptr, "a nonempty WAL payload has no data");
        record.payload.assign(ctx.payload, ctx.payload + ctx.payload_len);
    }
    ob::fuzz::require(std::equal(record.payload.begin(), record.payload.end(),
                                input.begin() + static_cast<std::ptrdiff_t>(offset + header_size)),
                      "WAL callback payload differs from the stored bytes");
    ob::fuzz::require(ob::crc32c(record.payload.data(), record.payload.size()) == ctx.header.checksum,
                      "WAL replay accepted an invalid checksum");
    return record;
}

} // namespace

extern "C" int LLVMFuzzerInitialize(int*, char***) {
    ob::fuzz::initialize("wal_replay");
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    OB_LOG_DEBUG("fuzz", "WAL input: bytes=%zu", size);
    static ScratchWal scratch;
    const std::span<const uint8_t> input(data, size);
    scratch.replace(input);
    ob::WALReplayer replayer(scratch.directory());
    std::vector<Record> all;
    size_t after_checkpoint = 0;
    const uint64_t full_sequence = replayer.replay_v2([&](const ob::WALReplayContext& ctx) {
        all.push_back(observe(ctx, input));
        if (ctx.header.record_type == ob::WAL_RECORD_CHECKPOINT) after_checkpoint = all.size();
    });
    std::vector<Record> tail;
    const uint64_t tail_sequence = replayer.replay_after_checkpoint([&](const ob::WALReplayContext& ctx) {
        tail.push_back(observe(ctx, input));
    });
    ob::fuzz::require(full_sequence == tail_sequence, "checkpoint replay changed the last sequence");
    const std::vector<Record> expected(all.begin() + static_cast<std::ptrdiff_t>(after_checkpoint), all.end());
    ob::fuzz::require(tail == expected, "checkpoint replay did not return the suffix after the last checkpoint");
    return 0;
}
