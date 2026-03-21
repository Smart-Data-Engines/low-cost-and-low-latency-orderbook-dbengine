#pragma once

#include <cstdint>
#include <cstddef>
#include <vector>

namespace ob {

/// Compress a byte buffer using LZ4 frame format.
/// Returns compressed bytes. Throws std::runtime_error on failure.
std::vector<uint8_t> lz4_compress(const void* data, size_t len);

/// Decompress an LZ4 frame back to original bytes.
/// Returns decompressed bytes. Throws std::runtime_error on failure.
std::vector<uint8_t> lz4_decompress(const void* data, size_t len);

/// Streaming LZ4 compressor — wraps LZ4F_compressionContext_t.
/// Allows incremental compression of multiple chunks into a single frame.
class LZ4CompressStream {
public:
    LZ4CompressStream();
    ~LZ4CompressStream();

    LZ4CompressStream(const LZ4CompressStream&) = delete;
    LZ4CompressStream& operator=(const LZ4CompressStream&) = delete;

    /// Begin a new frame. Returns frame header bytes.
    std::vector<uint8_t> begin();

    /// Compress a chunk. Returns compressed output bytes.
    std::vector<uint8_t> update(const void* data, size_t len);

    /// End the frame. Returns frame footer bytes.
    std::vector<uint8_t> end();

private:
    void* ctx_{nullptr};
};

/// Streaming LZ4 decompressor — wraps LZ4F_decompressionContext_t.
class LZ4DecompressStream {
public:
    LZ4DecompressStream();
    ~LZ4DecompressStream();

    LZ4DecompressStream(const LZ4DecompressStream&) = delete;
    LZ4DecompressStream& operator=(const LZ4DecompressStream&) = delete;

    /// Decompress a chunk. Returns decompressed output bytes.
    /// May return empty if the input is only frame header/footer.
    std::vector<uint8_t> update(const void* data, size_t len);

    /// Reset the decompressor for a new frame.
    void reset();

private:
    void* ctx_{nullptr};
};

} // namespace ob
