#pragma once

// The encoding byte of each block of a segment in one file - segment formats 3 and 4, whose
// `columns.v3` the format lays out as a directory of blocks and then the blocks - for the tests
// that check what a seal or a merge chose. One copy, shared by the store's tests and the merge's.

#include <gtest/gtest.h>

#include <cstdint>
#include <fstream>
#include <iterator>
#include <span>
#include <string>
#include <vector>

#include "orderbook/column_codec.hpp"
#include "orderbook/columnar_store.hpp"

namespace ob::segment_blocks {

/// An encoding byte's transform, bits 0-1, and the one only format 4 writes: steps (#250).
inline constexpr uint8_t kTransformMask = 0x03;
inline constexpr uint8_t kSteps = 0x03;

/// The encoding byte of each block of a format-3 or format-4 segment, in the order of its file's
/// directory.
inline std::vector<uint8_t> block_encodings(const std::string& segment_dir) {
    std::ifstream in(segment_dir + "/" + ob::kColumnsV3File, std::ios::binary);
    const std::string file((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    const std::span<const char> bytes(file.data(), file.size());
    EXPECT_EQ(file.substr(0, 4), "OBS3");
    size_t at = 4;
    uint64_t rows = 0;
    EXPECT_TRUE(ob::column_codec::get_varint(bytes, at, rows));
    const size_t columns = static_cast<uint8_t>(file.at(at++));
    std::vector<uint64_t> lengths;
    for (size_t c = 0; c < columns; ++c) {
        ++at;   // the column's id
        uint64_t length = 0;
        EXPECT_TRUE(ob::column_codec::get_varint(bytes, at, length));
        at += 4;   // the block's CRC32C
        lengths.push_back(length);
    }
    std::vector<uint8_t> encodings;
    for (const uint64_t length : lengths) {
        encodings.push_back(static_cast<uint8_t>(file.at(at)));
        at += static_cast<size_t>(length);
    }
    EXPECT_EQ(at, file.size());
    return encodings;
}

}  // namespace ob::segment_blocks
