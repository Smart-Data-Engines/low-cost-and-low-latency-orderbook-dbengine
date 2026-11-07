#pragma once

#include <cstdint>
#include <cstddef>
#include <string>
#include <string_view>
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

/// An LZ4 frame written into a string as its content arrives (#257), the four-byte big-endian length
/// a compressed session sends in front of it: the frame of an answer built from the answer's text a
/// piece at a time, so that the text is not held whole beside its frame and the frame can be measured
/// against what a session may be sent while it grows.
///
/// The frame lz4_compress() writes - a content checksum - with two differences an LZ4 frame may have:
/// no content size in its header, which is not known when the header is written, and every piece's
/// blocks flushed as it is added, so that size() counts all the content added so far.
class Lz4FrameWriter {
public:
    /// The frame begun in `room`, emptied and its capacity kept: the length's four bytes, then the
    /// frame's header. `capacity_ceiling`, when not 0, is the most capacity growing the string asks
    /// for ahead of what it needs - doubling stops there - so a frame measured against a ceiling is
    /// never given twice the ceiling to grow into.
    explicit Lz4FrameWriter(std::string room, size_t capacity_ceiling = 0);
    ~Lz4FrameWriter();

    Lz4FrameWriter(const Lz4FrameWriter&) = delete;
    Lz4FrameWriter& operator=(const Lz4FrameWriter&) = delete;

    /// `text` compressed onto the frame, its blocks flushed.
    void add(std::string_view text);

    /// The end mark and the checksum written, the length in front filled in, and the frame handed
    /// over; the writer is spent after it.
    std::string end();

    /// The frame so far, its length in front included.
    size_t size() const { return out_.size(); }

    /// The content added so far.
    uint64_t content_bytes() const { return content_; }

    /// What end() adds to a frame whose content has all been added: the end mark and the checksum.
    static constexpr size_t kEndBytes = 8;

private:
    void room_for(size_t n);

    std::string out_;
    size_t      capacity_ceiling_;
    uint64_t    content_{0};
    void*       ctx_{nullptr};
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
