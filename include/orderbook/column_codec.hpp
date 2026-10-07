#pragma once

// ── Column blocks of segment format 3 ─────────────────────────────────────────
//
// Format 2 wrote four of a segment's seven columns raw and a fifth as one zigzag delta a word:
// 25 bytes a row on the comparative dataset, where ClickHouse as installed takes 4.2. Which
// encoding replaces them was measured, not chosen (kiro-workspace/specs/segment-format-v3/,
// `benchmarks/segment_encoding_probe`): no single encoding wins on every dataset - fixed-width
// blocks on random quantities, ZSTD on a book's snapshots, whose levels repeat from one to the
// next - so every column of every segment is written in the smallest of the candidates below that
// its options try: five without compression, and runs and narrow of each transform under LZ4 and
// under ZSTD.
//
// A candidate is a transform, a packing and, or not, LZ4 or ZSTD over the packed bytes:
//
//   transform   for     each value minus the column's minimum, the minimum kept as the anchor
//               delta   each value minus the one before it, zigzagged; the first is the anchor
//               - both divided by the greatest common divisor of what they produce when it is
//                 more than 1 (a price on a tick, a quantity in lots), the divisor kept
//   packing     Simple8b, runs (each run of equal values as its value and its length, both in
//               Simple8b), blocks (128 values at the width of their largest), narrow (each
//               value in the bytes the largest needs)
//   compressor  none, LZ4 or ZSTD: ZSTD makes the smaller blocks and LZ4 the ones read several
//               times faster, so a seal - what queries read most - uses LZ4, and a merge, which
//               writes what stays, takes ZSTD where it saves enough over LZ4 (design §3)
//
// Values are uint64. A signed column - the price, the sequence number - is its int64 reinterpreted,
// and both transforms work modulo 2^64 as encode_prices() does, so every value has an encoding and
// decoding inverts it exactly.
//
// A block says how many values it holds, and the decoder is told how many the column has: a block
// is decoded only when the two agree.

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace ob::column_codec {

enum class Transform : uint8_t { kNone = 0, kFor = 1, kDelta = 2 };
enum class Packing : uint8_t { kSimple8b = 0, kRuns = 1, kBlocks = 2, kNarrow = 3 };
enum class Compressor : uint8_t { kNone = 0, kZstd = 1, kLz4 = 2 };

struct Encoding {
    Transform transform{Transform::kNone};
    Packing packing{Packing::kSimple8b};
    Compressor compressor{Compressor::kNone};
    /// "delta/runs+zstd" - what the logs and the cost benchmark name it.
    std::string name() const;
    bool operator==(const Encoding&) const = default;
};

struct EncodeOptions {
    /// Whether runs and narrow of each transform are tried under LZ4.
    bool lz4{true};
    /// The ZSTD level of the same four under ZSTD; 0 leaves them out.
    int zstd_level{3};
    /// How much smaller, in percent, a compressed candidate must be than the smallest
    /// uncompressed one to be chosen: reading it costs a decompression the other does not.
    unsigned compressed_margin_pct{0};
    /// How much smaller, in percent, a ZSTD candidate must be than the smallest LZ4 one, when LZ4
    /// is tried: ZSTD's decompression costs several times LZ4's.
    unsigned zstd_margin_pct{0};
    /// The encoding to write in without searching - what the column's last segment chose - or
    /// null to search. One whose compressor these options leave out is searched for instead.
    const Encoding* hint{nullptr};
};

struct Choice {
    Encoding encoding;
    size_t bytes{0};               ///< the block's
    size_t uncompressed_bytes{0};  ///< the smallest uncompressed candidate's
};

/// Appends to `out` the block of `values` in the smallest candidate, as `options` weighs them.
Choice encode(std::span<const uint64_t> values, const EncodeOptions& options, std::string& out);

/// Appends to `out` the block of `values` in `encoding` - for the tests and the cost benchmark;
/// `zstd_level` applies when the encoding compresses with ZSTD. Returns the block's size.
size_t encode_as(std::span<const uint64_t> values, const Encoding& encoding, int zstd_level,
                 std::string& out);

/// What a thread that encodes keeps of each of its working buffers between calls. A buffer grown
/// past it - by a merge, or by a segment an embedding application seals after an hour of rows - is
/// freed when the call returns rather than kept for the thread's life; a seal's columns fit under it.
inline constexpr size_t kEncodeBufferKept = size_t{1} << 20;

/// What a decode works in besides the column it fills: a payload decompressed, Simple8b's words
/// aligned, and the values and run lengths before their inverse. A caller that decodes many blocks
/// keeps one, passes it, and counts what it holds: the store keeps one in each set of its pooled
/// read buffers, so what reads keep between them is inside the pool's budget however many threads
/// read. A decode not given one makes one for the call.
struct DecodeScratch {
    std::string plain;
    std::vector<uint64_t> words;
    std::vector<uint64_t> values;
    std::vector<uint64_t> lengths;

    size_t held_bytes() const noexcept {
        return plain.capacity() +
               (words.capacity() + values.capacity() + lengths.capacity()) * sizeof(uint64_t);
    }
};

/// Decodes a block of `count` values into `out`, which then holds exactly them. False, with the
/// reason in `why`, for bytes that are not such a block: an unknown transform or packing, a header
/// or payload cut short, a payload that does not decompress to the length it declares or does not
/// hold `count` values, or bytes left over after them.
bool decode(std::span<const char> block, size_t count, std::vector<uint64_t>& out, std::string* why);

/// The same into a narrower column type: false as well for a value the type cannot hold - a
/// format-3 block decodes to the column it was written from, or not at all. `int64_t` takes every
/// value, reinterpreted.
template <typename T>
bool decode_as(std::span<const char> block, size_t count, std::vector<T>& out, std::string* why);

/// The same, working in `scratch`, which keeps what it grew to for the next decode.
template <typename T>
bool decode_as(std::span<const char> block, size_t count, std::vector<T>& out, DecodeScratch& scratch,
               std::string* why);

/// LEB128, as the blocks and the file holding them write their lengths.
void put_varint(std::string& out, uint64_t v);
/// The varint at `at`, moving `at` past it; false for one cut short or longer than 64 bits.
bool get_varint(std::span<const char> in, size_t& at, uint64_t& v);

}  // namespace ob::column_codec
