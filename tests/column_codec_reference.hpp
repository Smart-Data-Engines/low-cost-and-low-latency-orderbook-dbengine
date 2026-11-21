#pragma once
// The encoder of segment format 3 as `master` had it at 0ed158b, before #267 made it faster: the oracle
// the faster one is held to, byte for byte (test_column_codec_equivalence.cpp). Copied, not rewritten -
// the column codec's encoding half (src/column_codec.cpp) and the Simple8b encoder it calls
// (src/codec.cpp) - so that a difference is a difference from what was shipped, not from somebody's
// reading of it. The one edit: encode()'s call of encode_as() names this namespace's, which argument-
// dependent lookup would otherwise find beside the engine's. Nothing outside that test includes this,
// and nothing else in it may change.

#include "orderbook/codec.hpp"
#include "orderbook/column_codec.hpp"
#include "orderbook/logger.hpp"

#include <lz4.h>
#include <zstd.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <numeric>
#include <span>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace ob::column_codec::reference {

// ── src/codec.cpp at 0ed158b: the Simple8b encoder ──────────────────────────

struct S8bSelector {
    uint32_t count;
    uint32_t bits;
};

inline constexpr S8bSelector kSelectors[16] = {
    {240, 0},   // 0
    {120, 0},   // 1
    { 60, 1},   // 2
    { 30, 2},   // 3
    { 20, 3},   // 4
    { 15, 4},   // 5
    { 12, 5},   // 6
    { 10, 6},   // 7
    {  8, 7},   // 8
    {  7, 8},   // 9
    {  6, 10},  // 10
    {  5, 12},  // 11
    {  4, 15},  // 12
    {  3, 20},  // 13
    {  2, 30},  // 14
    {  1, 60},  // 15
};

inline constexpr uint64_t kFallbackMarker = (1ULL << 60) - 1; // all 60 bits set

// The first selector, in the table's order, whose values all fit - a value fits when it is no
// wider than the selector's bits, and a selector of 0 bits takes only zeros. The widths' running
// maximum is extended only as far as a selector needs, and no further once it is wider than that
// selector takes, since a longer prefix is no narrower. The version before this scanned the values
// again for every selector it tried, up to 240 of them for the first: on a column of wide values
// that scan was most of what encoding cost (segment format v3 encodes each column several ways).
// It picks what that version picked, which `test_codec` holds it to.
inline int best_selector(const uint64_t* begin, size_t available) {
    uint8_t widest[241];   // widest[k]: the widest of the first k values, as far as computed
    widest[0] = 0;
    size_t known = 0;
    for (int sel = 0; sel <= 15; ++sel) {
        const size_t use = std::min<size_t>(kSelectors[sel].count, available);
        const unsigned bits = kSelectors[sel].bits;
        while (known < use && widest[known] <= bits) {
            const uint64_t v = begin[known];
            const auto w = static_cast<uint8_t>(v == 0 ? 0 : 64 - __builtin_clzll(v));
            widest[known + 1] = std::max(widest[known], w);
            ++known;
        }
        // Stopped short of `use`: the values so far are already wider than this selector takes.
        if (known >= use && widest[use] <= bits) return sel;
    }
    return -1; // unreachable for valid inputs
}

inline Simple8bResult encode_simple8b(std::span<const uint64_t> values) {
    Simple8bResult result;
    result.has_fallback = false;

    size_t i = 0;
    while (i < values.size()) {
        // Check for fallback: a value past the 60 bits a word holds, **or equal to the marker**
        // (#198). A selector-15 word whose payload is all ones is read as the fallback marker, so
        // 2^60 - 1 stored as an ordinary value made the decoder take the next word for a raw value:
        // measured, [2^60 - 1, 5, 7] came back as one value, 4611686018427387965. It is the one
        // value both encodings would spell the same, and the fallback is the unambiguous one.
        if (values[i] >= kFallbackMarker) {
            // Emit fallback: two words
            // Word 0: selector=15 in top 4 bits, value = kFallbackMarker
            uint64_t word0 = (static_cast<uint64_t>(15) << 60) | kFallbackMarker;
            result.words.push_back(word0);
            result.words.push_back(values[i]);
            result.has_fallback = true;
            ++i;
            continue;
        }

        // Find best selector for values starting at i
        int sel = best_selector(values.data() + i, values.size() - i);
        if (sel < 0) {
            // Should not happen; treat as fallback
            uint64_t word0 = (static_cast<uint64_t>(15) << 60) | kFallbackMarker;
            result.words.push_back(word0);
            result.words.push_back(values[i]);
            result.has_fallback = true;
            ++i;
            continue;
        }

        uint32_t cnt  = kSelectors[sel].count;
        uint32_t bits = kSelectors[sel].bits;
        uint32_t use  = static_cast<uint32_t>(
            cnt < static_cast<uint32_t>(values.size() - i) ? cnt : (values.size() - i));

        uint64_t word = static_cast<uint64_t>(sel) << 60;

        if (bits > 0) {
            for (uint32_t k = 0; k < use; ++k) {
                word |= (values[i + k] << (k * bits));
            }
        }
        // For bits==0 (selectors 0,1): word is just the selector, values are all 0

        result.words.push_back(word);
        i += use;
    }

    return result;
}

// ── src/column_codec.cpp at 0ed158b: the encoder's helpers ──────────────────


/// A thread's working buffer, freed when it has grown past what a thread keeps between calls.
template <typename Buffer>
inline void keep_small(Buffer& b) {
    if (b.capacity() * sizeof(typename Buffer::value_type) > kEncodeBufferKept) Buffer().swap(b);
}

constexpr uint8_t kScaledBit = 1u << 2;
constexpr uint8_t kZstdBit = 1u << 5;
constexpr uint8_t kLz4Bit = 1u << 6;
constexpr size_t kBlockValues = 128;

inline uint8_t encoding_byte(Transform t, bool scaled, Packing p, Compressor c) {
    return static_cast<uint8_t>(static_cast<uint8_t>(t) | (scaled ? kScaledBit : 0) |
                                (static_cast<uint8_t>(p) << 3) |
                                (c == Compressor::kZstd ? kZstdBit : c == Compressor::kLz4 ? kLz4Bit : 0));
}

inline uint64_t zigzag(bool negative, uint64_t magnitude) {
    // Of a signed value given as its sign and magnitude: 2^63 is the magnitude of INT64_MIN, and
    // its zigzag is UINT64_MAX, which (2^63 << 1) - 1 is in unsigned arithmetic.
    return negative ? (magnitude << 1) - 1 : magnitude << 1;
}

inline unsigned bit_width(uint64_t v) { return v == 0 ? 0 : 64 - static_cast<unsigned>(__builtin_clzll(v)); }

inline uint64_t load64(const unsigned char* p) {
    uint64_t x;
    std::memcpy(&x, p, sizeof x);   // little-endian hosts only, as the WAL is
    return x;
}

inline void store64(unsigned char* p, uint64_t x) { std::memcpy(p, &x, sizeof x); }

inline void put_u64_words(std::string& out, const std::vector<uint64_t>& words) {
    const size_t at = out.size();
    out.resize(at + words.size() * sizeof(uint64_t));
    if (!words.empty()) std::memcpy(out.data() + at, words.data(), words.size() * sizeof(uint64_t));
}

/// Division by `g` without a division per value (Granlund and Montgomery): g = 2^k * o with o odd,
/// so x is a multiple of g when its k low bits are zero and (x >> k) times o's inverse modulo 2^64
/// is at most (2^64 - 1) / o - and that product is then x / g. A 64-bit `div` per value cost more
/// than everything else a column's transform does.
struct Divisor {
    unsigned k{0};
    uint64_t inverse{1};
    uint64_t limit{std::numeric_limits<uint64_t>::max()};

    explicit Divisor(uint64_t g) {
        k = static_cast<unsigned>(__builtin_ctzll(g));
        const uint64_t o = g >> k;
        uint64_t inv = o;   // right in 3 bits for any odd o; each step doubles them
        for (int i = 0; i < 5; ++i) inv *= 2 - o * inv;
        inverse = inv;
        limit = std::numeric_limits<uint64_t>::max() / o;
    }
    bool divides(uint64_t x) const {
        if (k != 0 && (x & ((uint64_t{1} << k) - 1)) != 0) return false;
        return (x >> k) * inverse <= limit;
    }
    uint64_t quotient(uint64_t x) const { return (x >> k) * inverse; }   // x a multiple
};

/// The greatest common divisor of `v`: 0 when all are zero.
inline uint64_t common_divisor(const std::vector<uint64_t>& v) {
    size_t i = 0;
    uint64_t g = 0;
    while (i < v.size() && g == 0) g = v[i++];
    if (g <= 1) return g;
    Divisor d(g);
    for (; i < v.size(); ++i) {
        if (d.divides(v[i])) continue;
        g = std::gcd(g, v[i]);
        if (g == 1) return 1;
        d = Divisor(g);
    }
    return g;
}

/// A transform's output: the values to pack, the anchor and the divisor.
struct Stream {
    Transform transform{Transform::kFor};
    uint64_t count{0};   ///< the column's values; `values` has one fewer for delta
    uint64_t anchor{0};
    uint64_t scale{1};
    std::vector<uint64_t> values;
};

inline void transform_for(std::span<const uint64_t> v, Stream& s) {
    s.transform = Transform::kFor;
    s.count = v.size();
    s.anchor = v.empty() ? 0 : *std::min_element(v.begin(), v.end());
    s.values.resize(v.size());
    for (size_t i = 0; i < v.size(); ++i) s.values[i] = v[i] - s.anchor;
    const uint64_t g = common_divisor(s.values);
    s.scale = g > 1 ? g : 1;
    if (s.scale > 1) {
        const Divisor d(s.scale);
        for (uint64_t& x : s.values) x = d.quotient(x);
    }
}

inline void transform_delta(std::span<const uint64_t> v, Stream& s) {
    s.transform = Transform::kDelta;
    s.count = v.size();
    s.anchor = v.empty() ? 0 : v[0];
    const size_t n = v.empty() ? 0 : v.size() - 1;
    // Magnitudes first, the signs taken again below: the divisor is of the magnitudes, and the
    // differences are taken in wrapping arithmetic, as encode_prices() takes them.
    s.values.resize(n);
    for (size_t i = 0; i < n; ++i) {
        const uint64_t d = v[i + 1] - v[i];
        s.values[i] = static_cast<int64_t>(d) < 0 ? 0 - d : d;
    }
    const uint64_t g = common_divisor(s.values);
    s.scale = g > 1 ? g : 1;
    const Divisor div(s.scale);
    for (size_t i = 0; i < n; ++i) {
        const bool negative = static_cast<int64_t>(v[i + 1] - v[i]) < 0;
        const uint64_t m = s.scale > 1 ? div.quotient(s.values[i]) : s.values[i];
        s.values[i] = zigzag(negative, m);
    }
}

/// The runs payload: how many, the words of their values, those words, then the words of their
/// lengths less one - each in Simple8b.
inline void put_runs(const std::vector<uint64_t>& values, const std::vector<uint64_t>& lengths, std::string& out) {
    const auto value_words = encode_simple8b(values).words;
    const auto length_words = encode_simple8b(lengths).words;
    put_varint(out, values.size());
    put_varint(out, value_words.size());
    put_u64_words(out, value_words);
    put_u64_words(out, length_words);
}

/// Returns how many runs it packed.
inline size_t pack_runs(const std::vector<uint64_t>& v, std::string& out) {
    thread_local std::vector<uint64_t> values, lengths;
    values.clear();
    lengths.clear();
    for (size_t i = 0; i < v.size();) {
        size_t j = i + 1;
        while (j < v.size() && v[j] == v[i]) ++j;
        values.push_back(v[i]);
        lengths.push_back(j - i - 1);
        i = j;
    }
    put_runs(values, lengths, out);
    const size_t runs = values.size();
    keep_small(values);
    keep_small(lengths);
    return runs;
}

/// `v` as runs of equal values written as steps (#250), laid out as pack_runs() lays out runs: each
/// run's value its step from the run before's, modulo 2^64, zigzagged and divided by the steps'
/// greatest common divisor - the stream's scale - the first run's value the anchor and its step 0.
/// Its own packing rather than runs of a transform's output: two runs a step of 1 apart each, a
/// book's sequence numbers, would be one run of that output, and decode as one step. Fills `s`'s
/// header fields and its values, one a run; returns how many runs it packed.
inline size_t pack_steps(std::span<const uint64_t> v, Stream& s, std::string& out) {
    thread_local std::vector<uint64_t> raw, lengths;
    raw.clear();
    lengths.clear();
    s.transform = Transform::kSteps;
    s.count = v.size();
    s.anchor = v.empty() ? 0 : v[0];
    uint64_t before = s.anchor;
    for (size_t i = 0; i < v.size();) {
        size_t j = i + 1;
        while (j < v.size() && v[j] == v[i]) ++j;
        raw.push_back(v[i] - before);
        lengths.push_back(j - i - 1);
        before = v[i];
        i = j;
    }
    // Magnitudes first, the signs taken again below, as transform_delta() takes them: the divisor is
    // of the magnitudes.
    s.values.resize(raw.size());
    for (size_t r = 0; r < raw.size(); ++r) s.values[r] = static_cast<int64_t>(raw[r]) < 0 ? 0 - raw[r] : raw[r];
    const uint64_t g = common_divisor(s.values);
    s.scale = g > 1 ? g : 1;
    const Divisor div(s.scale);
    for (size_t r = 0; r < raw.size(); ++r) {
        const uint64_t m = s.scale > 1 ? div.quotient(s.values[r]) : s.values[r];
        s.values[r] = zigzag(static_cast<int64_t>(raw[r]) < 0, m);
    }
    put_runs(s.values, lengths, out);
    const size_t runs = raw.size();
    keep_small(raw);
    keep_small(lengths);
    return runs;
}

inline unsigned block_width(const uint64_t* v, size_t n) {
    uint64_t all = 0;
    for (size_t k = 0; k < n; ++k) all |= v[k];
    return bit_width(all);
}

inline size_t blocks_bytes(const std::vector<uint64_t>& v) {
    size_t bytes = 0;
    for (size_t i = 0; i < v.size(); i += kBlockValues) {
        const size_t n = std::min(kBlockValues, v.size() - i);
        bytes += 1 + (n * block_width(v.data() + i, n) + 7) / 8;
    }
    return bytes;
}

inline void pack_blocks(const std::vector<uint64_t>& v, std::string& out) {
    // Each value ORed into the eight bytes at its first bit, and the ninth when it reaches past
    // them, in a buffer padded for that ninth byte.
    thread_local std::vector<unsigned char> buf;
    for (size_t i = 0; i < v.size(); i += kBlockValues) {
        const size_t n = std::min(kBlockValues, v.size() - i);
        const unsigned w = block_width(v.data() + i, n);
        out.push_back(static_cast<char>(w));
        if (w == 0) continue;
        const size_t need = (n * w + 7) / 8;
        buf.assign(need + 9, 0);
        for (size_t k = 0; k < n; ++k) {
            const size_t bit = k * w;
            const unsigned shift = static_cast<unsigned>(bit & 7);
            unsigned char* p = buf.data() + (bit >> 3);
            const uint64_t x = v[i + k];
            store64(p, load64(p) | (x << shift));
            if (shift + w > 64) p[8] = static_cast<unsigned char>(p[8] | (x >> (64 - shift)));
        }
        out.append(reinterpret_cast<const char*>(buf.data()), need);
    }
}

inline unsigned narrow_width(const std::vector<uint64_t>& v) {
    uint64_t all = 0;
    for (uint64_t x : v) all |= x;
    return std::max(1u, (bit_width(all) + 7) / 8);
}

template <unsigned W>
inline void pack_narrow_w(const uint64_t* v, size_t n, char* o) {
    for (size_t i = 0; i < n; ++i) std::memcpy(o + i * W, &v[i], W);
}

inline void pack_narrow(const std::vector<uint64_t>& v, unsigned width, std::string& out) {
    const size_t at = out.size();
    out.resize(at + v.size() * width);
    char* o = out.data() + at;
    switch (width) {
    case 1: pack_narrow_w<1>(v.data(), v.size(), o); break;
    case 2: pack_narrow_w<2>(v.data(), v.size(), o); break;
    case 3: pack_narrow_w<3>(v.data(), v.size(), o); break;
    case 4: pack_narrow_w<4>(v.data(), v.size(), o); break;
    case 5: pack_narrow_w<5>(v.data(), v.size(), o); break;
    case 6: pack_narrow_w<6>(v.data(), v.size(), o); break;
    case 7: pack_narrow_w<7>(v.data(), v.size(), o); break;
    default: pack_narrow_w<8>(v.data(), v.size(), o); break;
    }
}

inline bool lz4_compress(const std::string& in, std::string& out) {
    out.resize(static_cast<size_t>(LZ4_compressBound(static_cast<int>(in.size()))));
    const int n = LZ4_compress_default(in.data(), out.data(), static_cast<int>(in.size()),
                                       static_cast<int>(out.size()));
    if (n <= 0) {
        out.clear();
        return false;
    }
    out.resize(static_cast<size_t>(n));
    return true;
}

struct FreeCCtx {
    void operator()(ZSTD_CCtx* c) const noexcept { ZSTD_freeCCtx(c); }
};
struct FreeDCtx {
    void operator()(ZSTD_DCtx* d) const noexcept { ZSTD_freeDCtx(d); }
};

/// One compression context a thread, kept - a context made per call allocates its tables each
/// time - and freed with the thread. Null when ZSTD could not make one; asked again next call.
inline ZSTD_CCtx* zstd_cctx() {
    thread_local std::unique_ptr<ZSTD_CCtx, FreeCCtx> cctx;
    if (!cctx) cctx.reset(ZSTD_createCCtx());
    return cctx.get();
}

/// The same for decompression.
inline ZSTD_DCtx* zstd_dctx() {
    thread_local std::unique_ptr<ZSTD_DCtx, FreeDCtx> dctx;
    if (!dctx) dctx.reset(ZSTD_createDCtx());
    return dctx.get();
}

inline bool zstd_compress(const std::string& in, int level, std::string& out) {
    ZSTD_CCtx* cctx = zstd_cctx();
    if (cctx == nullptr) {
        OB_LOG_WARN("codec", "no ZSTD compression context could be made for %zu bytes", in.size());
        out.clear();
        return false;
    }
    out.resize(ZSTD_compressBound(in.size()));
    const size_t n = ZSTD_compressCCtx(cctx, out.data(), out.size(), in.data(), in.size(), level);
    if (ZSTD_isError(n)) {
        out.clear();
        return false;
    }
    out.resize(n);
    return true;
}

inline void put_header(std::string& out, const Stream& s, Packing p, unsigned width, Compressor c,
                size_t packed_bytes) {
    out.push_back(static_cast<char>(encoding_byte(s.transform, s.scale > 1, p, c)));
    if (p == Packing::kNarrow) out.push_back(static_cast<char>(width));
    put_varint(out, s.count);
    if (s.transform != Transform::kNone) put_varint(out, s.anchor);
    if (s.scale > 1) put_varint(out, s.scale);
    if (c != Compressor::kNone) put_varint(out, packed_bytes);
}

inline size_t header_bytes(const Stream& s, Packing p, Compressor c, size_t packed_bytes) {
    thread_local std::string h;
    h.clear();
    put_header(h, s, p, 1, c, packed_bytes);
    return h.size();
}

/// `in` compressed by `c` into `out`; false when it could not be, which LZ4 and ZSTD do not do for
/// a valid level and a buffer of their bound.
inline bool compress(Compressor c, const std::string& in, int zstd_level, std::string& out) {
    return c == Compressor::kZstd ? zstd_compress(in, zstd_level, out) : lz4_compress(in, out);
}

inline void pack(const Stream& s, Packing p, unsigned width, std::string& out) {
    switch (p) {
    case Packing::kSimple8b: put_u64_words(out, encode_simple8b(s.values).words); return;
    case Packing::kRuns:     pack_runs(s.values, out); return;
    case Packing::kBlocks:   pack_blocks(s.values, out); return;
    case Packing::kNarrow:   pack_narrow(s.values, width, out); return;
    }
}

/// The block of `s` whose payload, packed as `p`, is `packed`: compressed by `c` or not, appended to
/// `out`; its size.
inline size_t emit_packed(const Stream& s, Packing p, unsigned width, Compressor c, int zstd_level,
                   const std::string& packed, std::string& out) {
    thread_local std::string compressed;
    const size_t at = out.size();
    if (c != Compressor::kNone && compress(c, packed, zstd_level, compressed)) {
        put_header(out, s, p, width, c, packed.size());
        out += compressed;
    } else {
        if (c != Compressor::kNone) {
            OB_LOG_WARN("codec", "%s could not compress %zu bytes; the block is written uncompressed",
                        c == Compressor::kZstd ? "ZSTD" : "LZ4", packed.size());
        }
        put_header(out, s, p, width, Compressor::kNone, 0);
        out += packed;
    }
    keep_small(compressed);
    return out.size() - at;
}

/// `s` packed as `p`, compressed by `c` or not, as a whole block appended to `out`; its size.
inline size_t emit(const Stream& s, Packing p, Compressor c, int zstd_level, std::string& out) {
    const unsigned width = p == Packing::kNarrow ? narrow_width(s.values) : 0;
    thread_local std::string packed;
    packed.clear();
    pack(s, p, width, packed);
    const size_t bytes = emit_packed(s, p, width, c, zstd_level, packed, out);
    keep_small(packed);
    return bytes;
}


// ── src/column_codec.cpp at 0ed158b: encode_as() and encode() ───────────────

inline size_t encode_as(std::span<const uint64_t> values, const Encoding& encoding, int zstd_level,
                 std::string& out) {
    Stream s;
    switch (encoding.transform) {
    case Transform::kNone:
        s.transform = Transform::kNone;
        s.count = values.size();
        s.values.assign(values.begin(), values.end());
        break;
    case Transform::kFor:   transform_for(values, s); break;
    case Transform::kDelta: transform_delta(values, s); break;
    case Transform::kSteps: {
        if (encoding.packing != Packing::kRuns) throw std::invalid_argument("steps are written as runs only");
        thread_local std::string packed;
        packed.clear();
        pack_steps(values, s, packed);
        const size_t bytes = emit_packed(s, Packing::kRuns, 0, encoding.compressor, zstd_level, packed, out);
        keep_small(packed);
        return bytes;
    }
    }
    return emit(s, encoding.packing, encoding.compressor, zstd_level, out);
}

inline Choice encode(std::span<const uint64_t> values, const EncodeOptions& options, std::string& out) {
    if (options.hint != nullptr && (options.hint->transform != Transform::kSteps || options.steps) &&
        (options.hint->compressor == Compressor::kNone ||
         (options.hint->compressor == Compressor::kLz4 && options.lz4) ||
         (options.hint->compressor == Compressor::kZstd && options.zstd_level != 0))) {
        Choice choice;
        choice.encoding = *options.hint;
        choice.bytes = reference::encode_as(values, *options.hint, options.zstd_level, out);
        choice.uncompressed_bytes = choice.bytes;
        return choice;
    }
    // Design §3: both transforms; uncompressed Simple8b and runs of each, blocks of `for`; and, under
    // LZ4 and under ZSTD where these options try them, runs and narrow of each. With `steps` (#250),
    // steps/runs too, uncompressed and under each compressor tried. Each payload is packed once and
    // compressed once per compressor, and the chosen block is written from what was packed rather
    // than packed again.
    thread_local Stream streams[3];   // for, delta, steps
    thread_local std::string s8b[2], runs[3], narrow[2];
    thread_local std::string compressed[2][3][2];   // [compressor: lz4, zstd][stream][runs, narrow]
    transform_for(values, streams[0]);
    transform_delta(values, streams[1]);
    const int stream_count = options.steps ? 3 : 2;

    enum class What { kS8b, kRuns, kBlocks, kCompressedRuns, kCompressedNarrow };
    struct Best {
        int stream{0};
        What what{What::kS8b};
        Compressor compressor{Compressor::kNone};
        size_t bytes{std::numeric_limits<size_t>::max()};
        uint64_t weighed{std::numeric_limits<uint64_t>::max()};   ///< the bytes, a runs one charged
    };
    // A runs candidate is weighed as if it were larger by its charge (#223): a run's decode is a step
    // its length decides, so where runs are a value or two long they decode several times slower than
    // any other packing, and on binance-diff's prices and quantities they were chosen for saving a
    // few percent. The charge follows the runs per value, so long runs pay almost nothing.
    size_t run_count[3] = {0, 0, 0};
    const auto weigh = [&](size_t bytes, int t, bool is_runs) -> uint64_t {
        // A steps stream holds a value a run, so its runs are counted against the column's values -
        // as for/runs's are, which are the same runs.
        const size_t n = t == 2 ? streams[t].count : streams[t].values.size();
        if (!is_runs || options.runs_charge_pct == 0 || n == 0) return bytes;
        // Runs a thousand values first, so that no charge and no column can overflow the product.
        const uint64_t runs_per_mille = uint64_t{1000} * run_count[t] / n;
        const uint64_t per_mille = std::min<uint64_t>(900, options.runs_charge_pct * runs_per_mille / 100);
        return static_cast<uint64_t>(bytes) * 1000 / (1000 - per_mille);
    };
    const auto offer = [](Best& best, const Best& candidate) {
        if (candidate.weighed < best.weighed) best = candidate;
    };
    Best light, lz4, zstd;
    for (int t = 0; t < 2; ++t) {
        const Stream& s = streams[t];
        s8b[t].clear();
        put_u64_words(s8b[t], encode_simple8b(s.values).words);
        runs[t].clear();
        run_count[t] = pack_runs(s.values, runs[t]);
        const size_t b_s8b = header_bytes(s, Packing::kSimple8b, Compressor::kNone, 0) + s8b[t].size();
        const size_t b_runs = header_bytes(s, Packing::kRuns, Compressor::kNone, 0) + runs[t].size();
        offer(light, Best{t, What::kS8b, Compressor::kNone, b_s8b, weigh(b_s8b, t, false)});
        offer(light, Best{t, What::kRuns, Compressor::kNone, b_runs, weigh(b_runs, t, true)});
        if (t == 0) {
            const size_t b_blocks =
                header_bytes(s, Packing::kBlocks, Compressor::kNone, 0) + blocks_bytes(s.values);
            offer(light, Best{t, What::kBlocks, Compressor::kNone, b_blocks, weigh(b_blocks, t, false)});
        }
    }
    if (options.steps) {
        runs[2].clear();
        run_count[2] = pack_steps(values, streams[2], runs[2]);
        const size_t b_steps = header_bytes(streams[2], Packing::kRuns, Compressor::kNone, 0) + runs[2].size();
        offer(light, Best{2, What::kRuns, Compressor::kNone, b_steps, weigh(b_steps, 2, true)});
    }
    const bool try_lz4 = options.lz4;
    const bool try_zstd = options.zstd_level != 0;
    if (try_lz4 || try_zstd) {
        for (int t = 0; t < 2; ++t) {
            narrow[t].clear();
            pack_narrow(streams[t].values, narrow_width(streams[t].values), narrow[t]);
        }
    }
    for (int c = 0; c < 2; ++c) {
        const Compressor compressor = c == 0 ? Compressor::kLz4 : Compressor::kZstd;
        if ((c == 0 && !try_lz4) || (c == 1 && !try_zstd)) continue;
        Best& best_here = c == 0 ? lz4 : zstd;
        for (int t = 0; t < stream_count; ++t) {
            const Stream& s = streams[t];
            std::string* payloads[2] = {&runs[t], t < 2 ? &narrow[t] : nullptr};   // steps: runs only
            for (int k = 0; k < 2; ++k) {
                if (payloads[k] == nullptr) continue;
                std::string& z = compressed[c][t][k];
                if (!compress(compressor, *payloads[k], options.zstd_level, z)) continue;
                const Packing packing = k == 0 ? Packing::kRuns : Packing::kNarrow;
                const size_t bytes = header_bytes(s, packing, compressor, payloads[k]->size()) + z.size();
                offer(best_here, Best{t, k == 0 ? What::kCompressedRuns : What::kCompressedNarrow,
                                      compressor, bytes, weigh(bytes, t, k == 0)});
            }
        }
    }
    // The smallest as weighed, with two margins: a compressed candidate has to be smaller than the
    // uncompressed one by `compressed_margin_pct`, and a ZSTD one than the LZ4 one by
    // `zstd_margin_pct` - both cost a decompression, ZSTD's several times LZ4's.
    const auto smaller_by = [](uint64_t weighed, uint64_t than, unsigned pct) {
        return 100 * weighed < static_cast<uint64_t>(100 - std::min(pct, 100u)) * than;
    };
    Best best = light;
    if (try_lz4 && lz4.weighed < best.weighed &&
        smaller_by(lz4.weighed, light.weighed, options.compressed_margin_pct)) {
        best = lz4;
    }
    if (try_zstd && zstd.weighed < best.weighed &&
        smaller_by(zstd.weighed, light.weighed, options.compressed_margin_pct) &&
        (!try_lz4 || smaller_by(zstd.weighed, lz4.weighed, options.zstd_margin_pct))) {
        best = zstd;
    }

    const Stream& s = streams[best.stream];
    const int t = best.stream;
    const int c = best.compressor == Compressor::kZstd ? 1 : 0;
    const size_t at = out.size();
    Choice choice;
    choice.encoding.transform = s.transform;
    choice.encoding.compressor = best.compressor;
    switch (best.what) {
    case What::kS8b:
        put_header(out, s, Packing::kSimple8b, 0, Compressor::kNone, 0);
        out += s8b[t];
        choice.encoding.packing = Packing::kSimple8b;
        break;
    case What::kRuns:
        put_header(out, s, Packing::kRuns, 0, Compressor::kNone, 0);
        out += runs[t];
        choice.encoding.packing = Packing::kRuns;
        break;
    case What::kBlocks:
        put_header(out, s, Packing::kBlocks, 0, Compressor::kNone, 0);
        pack_blocks(s.values, out);
        choice.encoding.packing = Packing::kBlocks;
        break;
    case What::kCompressedRuns:
        put_header(out, s, Packing::kRuns, 0, best.compressor, runs[t].size());
        out += compressed[c][t][0];
        choice.encoding.packing = Packing::kRuns;
        break;
    case What::kCompressedNarrow:
        put_header(out, s, Packing::kNarrow, narrow_width(s.values), best.compressor, narrow[t].size());
        out += compressed[c][t][1];
        choice.encoding.packing = Packing::kNarrow;
        break;
    }
    choice.bytes = out.size() - at;
    choice.uncompressed_bytes = light.bytes;
    for (int k = 0; k < 3; ++k) {
        keep_small(streams[k].values);
        keep_small(runs[k]);
        if (k < 2) {
            keep_small(s8b[k]);
            keep_small(narrow[k]);
        }
        for (auto& by_packing : compressed) {
            for (std::string& z : by_packing[k]) keep_small(z);
        }
    }
    return choice;
}


}  // namespace ob::column_codec::reference
