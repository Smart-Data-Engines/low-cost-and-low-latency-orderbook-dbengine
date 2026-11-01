#pragma once

// ── Column blocks of segment format 3 ─────────────────────────────────────────
//
// Format 2 wrote four of a segment's seven columns raw and a fifth as one zigzag delta a word:
// 25 bytes a row on the comparative dataset, where ClickHouse as installed takes 4.2. Which
// encoding replaces them was measured, not chosen (smart-data-engine-workspace/specs/segment-format-v3/,
// `benchmarks/segment_encoding_probe`): no single encoding wins on every dataset - fixed-width
// blocks on random quantities, ZSTD on a book's snapshots, whose levels repeat from one to the
// next - so every column of every segment is written in the smallest of the candidates below that
// its options try: five without compression, and runs and narrow of each transform under LZ4 and
// under ZSTD. A seal charges a runs candidate for its runs (`runs_charge_pct`, #223).
//
// A candidate is a transform, a packing and, or not, LZ4 or ZSTD over the packed bytes:
//
//   transform   for     each value minus the column's minimum, the minimum kept as the anchor
//               delta   each value minus the one before it, zigzagged; the first is the anchor
//               steps   runs only, and in segment format 4 only (#250): each run of equal values'
//                       value minus the run before's, zigzagged; the first run's value is the
//                       anchor and its step 0
//               - each divided by the greatest common divisor of what it produces when it is
//                 more than 1 (a price on a tick, a quantity in lots), the divisor kept
//   packing     Simple8b, runs (each run of equal values as its value and its length, both in
//               Simple8b), blocks (128 values at the width of their largest), narrow (each
//               value in the bytes the largest needs)
//   compressor  none, LZ4 or ZSTD: ZSTD makes the smaller blocks and LZ4 the ones read several
//               times faster, so a seal - what queries read most - uses LZ4, and a merge, which
//               writes what stays, takes ZSTD where it saves enough over LZ4 (design §3)
//
// Values are uint64. A signed column - the price, the sequence number - is its int64 reinterpreted,
// and the transforms work modulo 2^64 as encode_prices() does, so every value has an encoding and
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

/// The two bits of a block's encoding byte (#250): `kSteps` took the last value, and a reader of
/// segment format 3 refuses it as an unknown transform.
enum class Transform : uint8_t { kNone = 0, kFor = 1, kDelta = 2, kSteps = 3 };
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

/// The charge a seal puts on a runs candidate, `EncodeOptions::runs_charge_pct` (#223). 100 since
/// 1 November 2026, where it was 50: measured over every segment of both recordings at charges from 0
/// to 200, it is where binance-diff's seal decodes 8-12% faster in segment format 3 for 2.5% more
/// bytes, and 5-10% in format 4 for 1.2%, and binance-top20's moves in neither; past it the bytes keep
/// growing and the decode does not keep falling.
inline constexpr unsigned kRunsChargePct = 100;

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
    /// What a runs candidate is charged for its runs (#223): it is weighed as if it were larger by
    /// this many percent times its runs per value, at most 90%, so it is chosen only where it is that
    /// much smaller. Decoding a run costs a step its length decides, and a column of runs a value or
    /// two long decodes several times slower as runs than in any other packing, for the few percent
    /// they save. 0 charges nothing: the smallest candidate is chosen.
    unsigned runs_charge_pct{kRunsChargePct};
    /// Whether `steps/runs` is tried, with each compressor these options try (#250): only for a
    /// segment of format 4, which a build before it cannot read.
    bool steps{false};
    /// The encoding to write in without searching - what the column's last segment chose - or
    /// null to search. One whose compressor these options leave out is searched for instead, and so
    /// is one in `steps` where these options leave steps out: a node set back to format 3 must not
    /// write a block its last format-4 segment chose.
    const Encoding* hint{nullptr};
};

struct Choice {
    Encoding encoding;
    size_t bytes{0};               ///< the block's
    size_t uncompressed_bytes{0};  ///< the uncompressed candidate's the compressed ones were weighed against
};

/// Appends to `out` the block of `values` in the smallest candidate, as `options` weighs them -
/// a runs candidate charged for its runs.
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

/// The same into `count` values at `out`, which the caller owns - a held column's slot in its arena
/// (#223). Nothing is written before the block's header and payload pass their checks; a block that
/// fails later leaves `out` partly written, as decode_as() leaves its vector.
template <typename T>
bool decode_into(std::span<const char> block, size_t count, T* out, DecodeScratch& scratch,
                 std::string* why);

/// Whether a block of runs writes each of its short runs as a whole block of values, which the runs
/// after it overwrite past its end, rather than value by value at a branch its length decides (#223):
/// a choice of speed alone, the values decoded the same either way. `lengths` are the runs' lengths
/// less one, as the block stores them, and `values` their sum with the runs.
///
/// Where the runs average under two values - a book's prices on a recording of its changes, which move
/// at almost every row - a block writes eight values for each one it keeps, and the branch is
/// predicted anyway: value by value. From two values a run it writes blocks, unless the runs average
/// more than a block and their lengths repeat: three in four of 64 runs spread over the column as long
/// as the run before or the one before that - a book's snapshots, the forty rows of an update's
/// timestamps or the nineteen and one of its levels' differences - where the branch is predicted and
/// value by value writes less. Where they do not repeat - binance-diff's timestamps, whose largest
/// segments' runs average eleven to fourteen values in no pattern - their decode took a branch
/// misprediction about every other run.
bool short_runs_in_blocks(std::span<const uint64_t> lengths, size_t values);

/// LEB128, as the blocks and the file holding them write their lengths.
void put_varint(std::string& out, uint64_t v);
/// The varint at `at`, moving `at` past it; false for one cut short or longer than 64 bits.
bool get_varint(std::span<const char> in, size_t& at, uint64_t& v);

}  // namespace ob::column_codec
