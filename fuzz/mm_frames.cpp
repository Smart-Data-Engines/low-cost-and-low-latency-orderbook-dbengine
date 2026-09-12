#include "support.hpp"
#include "orderbook/multi_master.hpp"

#include <algorithm>
#include <cstddef>
#include <cstring>
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
        if (error) {
            // The manager depends on this: a protocol error leaves the buffer alone and the caller
            // drops the connection. A parser that erased the prefix it had already accepted would
            // leave surviving bytes starting at a frame boundary that never existed, and the next
            // read would resynchronise onto garbage instead of failing.
            ob::fuzz::require(pending == before, "a refused stream had bytes consumed from it");
        } else {
            ob::fuzz::require(pending.size() <= before.size(), "parsing grew the input buffer");
            ob::fuzz::require(std::equal(pending.begin(), pending.end(),
                                        before.end() - static_cast<std::ptrdiff_t>(pending.size())),
                              "unconsumed bytes are not an unchanged suffix");
            // A verdict of 0 with a whole header still pending means the loop broke on an
            // incomplete frame - and that break is only reachable *after* the length passed the
            // ceiling. So a success can never leave an over-long frame waiting.
            //
            // This is the only assertion that notices when the ceiling check is removed, and it
            // took a mutation to find that out. The refusal branch has no other oracle: an
            // over-long frame can never be completed inside the fuzzer's input limit, so deleting
            // the check turns -1 into 0 and every other property here still holds.
            if (pending.size() >= ob::MM_FRAME_HEADER_SIZE) {
                uint32_t next = 0;
                std::memcpy(&next, pending.data(), sizeof(next));
                ob::fuzz::require(next <= ob::MM_MAX_FRAME_PAYLOAD,
                                  "a successful verdict left an over-long frame pending");
            }
        }
    }
};

/// Encode a payload, decode it back, and require the bytes to survive the trip.
///
/// `encode_frame` is the other half of this wire format and had no coverage whatsoever: the stream
/// above only ever calls the decoder. This is also the only property here that is sensitive to a
/// *systematic* decoding error. Fragmentation invariance is not - a decoder that reports every
/// payload one byte early shifts both sides of that comparison equally, and a mutation doing
/// exactly that survived until this was added.
void check_encode_round_trip(std::span<const uint8_t> payload) {
    OB_LOG_DEBUG("fuzz", "Frame round trip: payload=%zu", payload.size());
    std::vector<uint8_t> encoded;
    // Twice, because the order two frames come back in is part of the format.
    ob::encode_frame(payload.data(), payload.size(), encoded);
    ob::encode_frame(payload.data(), payload.size(), encoded);

    std::vector<uint8_t> buffer = encoded;
    std::vector<std::pair<size_t, size_t>> ranges;
    ob::fuzz::require(ob::parse_frames(buffer, ranges) == 0,
                      "this decoder refused a stream this encoder produced");
    ob::fuzz::require(ranges.size() == 2, "two encoded frames did not decode as two frames");
    for (const auto& [offset, length] : ranges) {
        ob::fuzz::require(length == payload.size(), "a round trip changed the payload length");
        ob::fuzz::require(std::equal(payload.begin(), payload.end(),
                                     encoded.begin() + static_cast<std::ptrdiff_t>(offset)),
                          "a round trip returned bytes the encoder was never given");
    }
    ob::fuzz::require(buffer.empty(), "a fully framed stream left bytes pending");
}

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
    check_encode_round_trip(input);
    ob::fuzz::require(whole.error == split.error, "fragmentation changed the protocol verdict");
    ob::fuzz::require(whole.frames == split.frames, "fragmentation changed frame payloads or order");
    if (!whole.error) {
        ob::fuzz::require(whole.pending == split.pending, "fragmentation changed the remaining tail");
    }
    return 0;
}
