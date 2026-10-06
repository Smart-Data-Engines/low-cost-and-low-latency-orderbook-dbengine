// Segment format 3's column block decoder (src/column_codec.cpp), fed arbitrary bytes.
//
// The first three bytes of an input choose what the reader is told - the column's count of values
// (up to 65 535) and its type - and the rest is the block. Every decode either refuses with a
// reason or hands back exactly that many values, and a refusal leaves nothing a caller could take
// for a column. What decodes is then encoded again and must decode to the same values: a block the
// decoder accepts is one whose values the encoder can write.
//
// A block comes from a segment file whose CRC32C the store checks first, so a corrupt block reaches
// this decoder only by a checksum collision - or from a peer's snapshot, which is why it has to hold
// against anything.
#include "support.hpp"
#include "orderbook/column_codec.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace {

namespace cc = ob::column_codec;

template <typename T>
void decode_as(std::span<const char> block, size_t count) {
    std::vector<T> out{T{1}, T{2}};   // something there before, which a refusal must not pass for a column
    std::string why;
    const bool ok = cc::decode_as(block, count, out, &why);
    if (!ok) {
        ob::fuzz::require(!why.empty(), "a refusal said nothing");
        return;
    }
    ob::fuzz::require(out.size() == count, "a decoded column holds another number of values");
}

}  // namespace

extern "C" int LLVMFuzzerInitialize(int*, char***) {
    ob::fuzz::initialize("column_block");
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 3) return 0;
    const size_t count = static_cast<size_t>(data[0]) | (static_cast<size_t>(data[1]) << 8);
    const unsigned type = data[2] % 5;
    const std::span<const char> block(reinterpret_cast<const char*>(data + 3), size - 3);

    switch (type) {
    case 1: decode_as<int64_t>(block, count); return 0;
    case 2: decode_as<uint32_t>(block, count); return 0;
    case 3: decode_as<uint16_t>(block, count); return 0;
    case 4: decode_as<uint8_t>(block, count); return 0;
    default: break;
    }

    std::vector<uint64_t> values{7, 8};
    std::string why;
    if (!cc::decode(block, count, values, &why)) {
        ob::fuzz::require(!why.empty(), "a refusal said nothing");
        return 0;
    }
    ob::fuzz::require(values.size() == count, "a decoded column holds another number of values");
    // Encoded again, by the search over every candidate, it decodes to the same values.
    std::string again;
    cc::encode(values, cc::EncodeOptions{}, again);
    std::vector<uint64_t> back;
    ob::fuzz::require(cc::decode(std::span<const char>(again.data(), again.size()), count, back, &why),
                      "a block the encoder wrote was refused");
    ob::fuzz::require(back == values, "a block the encoder wrote decoded to other values");
    return 0;
}
