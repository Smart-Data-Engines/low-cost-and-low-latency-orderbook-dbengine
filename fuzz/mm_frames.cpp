#include "support.hpp"
#include "orderbook/multi_master.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>
#include <utility>
#include <vector>

namespace {

struct Stream {
    std::vector<uint8_t> pending;
    std::vector<std::vector<uint8_t>> frames;
    bool error{false};

    void feed(std::span<const uint8_t> bytes) {
        OB_LOG_DEBUG("fuzz", "Frame chunk: bytes=%zu pending=%zu", bytes.size(), pending.size());
        if (error) return;
        pending.insert(pending.end(), bytes.begin(), bytes.end());
        // The production parser erases bytes but returns offsets into this pre-erasure buffer.
        const auto before = pending;
        std::vector<std::pair<size_t, size_t>> ranges;
        const int verdict = ob::parse_frames(pending, ranges);
        ob::fuzz::require(verdict == 0 || verdict == -1, "unknown frame parser verdict");
        for (const auto& [offset, length] : ranges) {
            ob::fuzz::require(offset <= before.size() && length <= before.size() - offset,
                              "frame payload extends beyond the input");
            ob::fuzz::require(length <= ob::MM_MAX_FRAME_PAYLOAD, "oversized frame was accepted");
            frames.emplace_back(before.begin() + static_cast<std::ptrdiff_t>(offset),
                                before.begin() + static_cast<std::ptrdiff_t>(offset + length));
        }
        error = verdict == -1;
        // On error the parser leaves its buffer intact, even if it reported a valid prefix.
        if (!error) {
            ob::fuzz::require(pending.size() <= before.size(), "parsing grew the input buffer");
            ob::fuzz::require(std::equal(pending.begin(), pending.end(),
                                        before.end() - static_cast<std::ptrdiff_t>(pending.size())),
                              "unconsumed bytes are not an unchanged suffix");
        }
    }
};

} // namespace

extern "C" int LLVMFuzzerInitialize(int*, char***) {
    ob::fuzz::initialize("mm_frames");
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    OB_LOG_DEBUG("fuzz", "Frame stream input: bytes=%zu", size);
    const std::span<const uint8_t> input(data, size);
    Stream whole;
    whole.feed(input);

    // Three chunks bound copying even for a large incomplete frame. Input bytes select the cuts;
    // they remain part of the stream, so every seed is also a raw protocol reproducer.
    const size_t selector = size > 1 ? (static_cast<size_t>(data[size - 2]) << 8) | data[size - 1]
                                    : (size ? data[0] : 0);
    const size_t first = selector % (size + 1);
    const size_t second = first + (size - first) / 2;
    Stream split;
    split.feed(input.first(first));
    split.feed(input.subspan(first, second - first));
    split.feed(input.subspan(second));
    ob::fuzz::require(whole.error == split.error, "fragmentation changed the protocol verdict");
    ob::fuzz::require(whole.frames == split.frames, "fragmentation changed frame payloads or order");
    if (!whole.error) {
        ob::fuzz::require(whole.pending == split.pending, "fragmentation changed the remaining tail");
    }
    return 0;
}
