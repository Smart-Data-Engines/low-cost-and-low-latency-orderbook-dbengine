// Tests for LZ4 compression: property-based tests (Properties 1, 3, 4)
// and unit tests for edge cases.
// Feature: compression-and-ttl

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <vector>

#include <lz4frame.h>

#include "orderbook/compression.hpp"

// ═══════════════════════════════════════════════════════════════════════════════
// Property 1: LZ4 compression round-trip
// Feature: compression-and-ttl, Property 1: LZ4 compression round-trip
// For any byte sequence of length 0 to 1 MB, compressing with lz4_compress
// then decompressing with lz4_decompress shall produce a byte-identical result.
// Validates: Requirements 9.1, 9.2, 9.3
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(LZ4Property, prop_roundtrip, ()) {
    const auto len = *rc::gen::inRange<size_t>(0, 1024 * 1024 + 1);
    std::vector<uint8_t> data(len);
    for (size_t i = 0; i < len; ++i) {
        data[i] = *rc::gen::arbitrary<uint8_t>();
    }

    auto compressed = ob::lz4_compress(data.data(), data.size());
    auto decompressed = ob::lz4_decompress(compressed.data(), compressed.size());

    RC_ASSERT(decompressed.size() == data.size());
    RC_ASSERT(decompressed == data);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 3: LZ4 streaming compression round-trip
// Feature: compression-and-ttl, Property 3: LZ4 streaming compression round-trip
// For any sequence of byte chunks (each 0 to 64 KB), compressing them through
// LZ4CompressStream (begin -> update each chunk -> end) and then decompressing
// the concatenated output through LZ4DecompressStream shall produce the
// concatenation of the original chunks.
// Validates: Requirements 9.1, 9.2
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(LZ4Property, prop_streaming_roundtrip, ()) {
    const auto num_chunks = *rc::gen::inRange<size_t>(0, 20);
    std::vector<std::vector<uint8_t>> chunks(num_chunks);
    for (size_t i = 0; i < num_chunks; ++i) {
        const auto chunk_len = *rc::gen::inRange<size_t>(0, 64 * 1024 + 1);
        chunks[i].resize(chunk_len);
        for (size_t j = 0; j < chunk_len; ++j) {
            chunks[i][j] = *rc::gen::arbitrary<uint8_t>();
        }
    }

    // Compress via streaming
    ob::LZ4CompressStream compressor;
    std::vector<uint8_t> compressed;

    auto header = compressor.begin();
    compressed.insert(compressed.end(), header.begin(), header.end());

    for (const auto& chunk : chunks) {
        auto out = compressor.update(chunk.data(), chunk.size());
        compressed.insert(compressed.end(), out.begin(), out.end());
    }

    auto footer = compressor.end();
    compressed.insert(compressed.end(), footer.begin(), footer.end());

    // Decompress via streaming
    ob::LZ4DecompressStream decompressor;
    auto decompressed = decompressor.update(compressed.data(), compressed.size());

    // Build expected concatenation
    std::vector<uint8_t> expected;
    for (const auto& chunk : chunks) {
        expected.insert(expected.end(), chunk.begin(), chunk.end());
    }

    RC_ASSERT(decompressed.size() == expected.size());
    RC_ASSERT(decompressed == expected);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 4: Compression reduces or preserves size
// Feature: compression-and-ttl, Property 4: Compression reduces or preserves size
// For any byte sequence of length 1 to 1 MB, the output of lz4_compress shall
// have length <= LZ4F_compressBound(input_length, nullptr) + LZ4F_HEADER_SIZE_MAX.
// Validates: Requirements 2.1, 3.2
// ═══════════════════════════════════════════════════════════════════════════════
RC_GTEST_PROP(LZ4Property, prop_compress_bound, ()) {
    const auto len = *rc::gen::inRange<size_t>(1, 1024 * 1024 + 1);
    std::vector<uint8_t> data(len);
    for (size_t i = 0; i < len; ++i) {
        data[i] = *rc::gen::arbitrary<uint8_t>();
    }

    auto compressed = ob::lz4_compress(data.data(), data.size());
    const size_t upper_bound =
        LZ4F_compressBound(data.size(), nullptr) + LZ4F_HEADER_SIZE_MAX;

    RC_ASSERT(compressed.size() <= upper_bound);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Unit tests for compression edge cases
// ═══════════════════════════════════════════════════════════════════════════════

// LZ4CompressEmpty: compress empty buffer, verify decompression returns empty
TEST(LZ4Unit, LZ4CompressEmpty) {
    auto compressed = ob::lz4_compress(nullptr, 0);
    ASSERT_FALSE(compressed.empty());  // LZ4 frame has header even for empty

    auto decompressed = ob::lz4_decompress(compressed.data(), compressed.size());
    ASSERT_TRUE(decompressed.empty());
}

// LZ4CompressKnownData: compress a known WAL-like payload, verify round-trip
TEST(LZ4Unit, LZ4CompressKnownData) {
    // Simulate a WAL-like payload: record type + timestamp + price + qty
    std::vector<uint8_t> payload(64);
    payload[0] = 0x01;  // record type
    // Fill with a recognizable pattern
    for (size_t i = 1; i < payload.size(); ++i) {
        payload[i] = static_cast<uint8_t>(i & 0xFF);
    }

    auto compressed = ob::lz4_compress(payload.data(), payload.size());
    auto decompressed = ob::lz4_decompress(compressed.data(), compressed.size());

    ASSERT_EQ(decompressed.size(), payload.size());
    ASSERT_EQ(decompressed, payload);
}

// LZ4StreamMultiChunk: stream-compress 3 chunks, decompress, verify concatenation
TEST(LZ4Unit, LZ4StreamMultiChunk) {
    std::vector<uint8_t> chunk1 = {1, 2, 3, 4, 5};
    std::vector<uint8_t> chunk2 = {10, 20, 30};
    std::vector<uint8_t> chunk3 = {100, 200, 255, 0, 128};

    ob::LZ4CompressStream compressor;
    std::vector<uint8_t> compressed;

    auto header = compressor.begin();
    compressed.insert(compressed.end(), header.begin(), header.end());

    auto c1 = compressor.update(chunk1.data(), chunk1.size());
    compressed.insert(compressed.end(), c1.begin(), c1.end());

    auto c2 = compressor.update(chunk2.data(), chunk2.size());
    compressed.insert(compressed.end(), c2.begin(), c2.end());

    auto c3 = compressor.update(chunk3.data(), chunk3.size());
    compressed.insert(compressed.end(), c3.begin(), c3.end());

    auto footer = compressor.end();
    compressed.insert(compressed.end(), footer.begin(), footer.end());

    // Decompress
    ob::LZ4DecompressStream decompressor;
    auto decompressed = decompressor.update(compressed.data(), compressed.size());

    std::vector<uint8_t> expected;
    expected.insert(expected.end(), chunk1.begin(), chunk1.end());
    expected.insert(expected.end(), chunk2.begin(), chunk2.end());
    expected.insert(expected.end(), chunk3.begin(), chunk3.end());

    ASSERT_EQ(decompressed.size(), expected.size());
    ASSERT_EQ(decompressed, expected);
}

// LZ4DecompressCorrupt: feed corrupt bytes to lz4_decompress, verify exception
TEST(LZ4Unit, LZ4DecompressCorrupt) {
    std::vector<uint8_t> garbage = {0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02};
    ASSERT_THROW(ob::lz4_decompress(garbage.data(), garbage.size()),
                 std::runtime_error);
}
