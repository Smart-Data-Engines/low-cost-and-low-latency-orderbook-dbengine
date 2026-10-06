#include "orderbook/column_codec.hpp"

#include "orderbook/codec.hpp"
#include "orderbook/logger.hpp"

#include <lz4.h>
#include <zstd.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <numeric>
#include <type_traits>

namespace ob::column_codec {

// ── The block ────────────────────────────────────────────────────────────────
//
//   u8      encoding: bits 0-1 the transform, bit 2 divided by a scale, bits 3-4 the packing,
//           bit 5 ZSTD, bit 6 LZ4 (not both), bit 7 zero
//   u8      narrow packing only: the bytes a value takes, 1 - 8
//   varint  the values in the column - Simple8b pads its last word with zeros, so the words
//           alone do not say how many there are
//   varint  the anchor: for - the minimum; delta - the first value; none - absent
//   varint  scaled only: the divisor, 2 or more
//   varint  compressed only: the packed payload's length before compression
//   ...     the payload, to the end of the block
//
// The payload, by packing (after decompression when compressed):
//   Simple8b  the words of encode_simple8b()
//   runs      varint runs, varint the words of their values, those words, then the words of the
//             runs' lengths less one - each in Simple8b
//   blocks    per 128 values: u8 width 0 - 64, then the values in ceil(n * width / 8) bytes, the
//             lowest bit first
//   narrow    each value in `width` bytes, little-endian

namespace {

constexpr uint8_t kScaledBit = 1u << 2;
constexpr uint8_t kZstdBit = 1u << 5;
constexpr uint8_t kLz4Bit = 1u << 6;
constexpr size_t kBlockValues = 128;

uint8_t encoding_byte(Transform t, bool scaled, Packing p, Compressor c) {
    return static_cast<uint8_t>(static_cast<uint8_t>(t) | (scaled ? kScaledBit : 0) |
                                (static_cast<uint8_t>(p) << 3) |
                                (c == Compressor::kZstd ? kZstdBit : c == Compressor::kLz4 ? kLz4Bit : 0));
}

uint64_t zigzag(bool negative, uint64_t magnitude) {
    // Of a signed value given as its sign and magnitude: 2^63 is the magnitude of INT64_MIN, and
    // its zigzag is UINT64_MAX, which (2^63 << 1) - 1 is in unsigned arithmetic.
    return negative ? (magnitude << 1) - 1 : magnitude << 1;
}

unsigned bit_width(uint64_t v) { return v == 0 ? 0 : 64 - static_cast<unsigned>(__builtin_clzll(v)); }

uint64_t load64(const unsigned char* p) {
    uint64_t x;
    std::memcpy(&x, p, sizeof x);   // little-endian hosts only, as the WAL is
    return x;
}

void store64(unsigned char* p, uint64_t x) { std::memcpy(p, &x, sizeof x); }

void put_u64_words(std::string& out, const std::vector<uint64_t>& words) {
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
uint64_t common_divisor(const std::vector<uint64_t>& v) {
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

void transform_for(std::span<const uint64_t> v, Stream& s) {
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

void transform_delta(std::span<const uint64_t> v, Stream& s) {
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

void pack_runs(const std::vector<uint64_t>& v, std::string& out) {
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
    const auto value_words = encode_simple8b(values).words;
    const auto length_words = encode_simple8b(lengths).words;
    put_varint(out, values.size());
    put_varint(out, value_words.size());
    put_u64_words(out, value_words);
    put_u64_words(out, length_words);
}

unsigned block_width(const uint64_t* v, size_t n) {
    uint64_t all = 0;
    for (size_t k = 0; k < n; ++k) all |= v[k];
    return bit_width(all);
}

size_t blocks_bytes(const std::vector<uint64_t>& v) {
    size_t bytes = 0;
    for (size_t i = 0; i < v.size(); i += kBlockValues) {
        const size_t n = std::min(kBlockValues, v.size() - i);
        bytes += 1 + (n * block_width(v.data() + i, n) + 7) / 8;
    }
    return bytes;
}

void pack_blocks(const std::vector<uint64_t>& v, std::string& out) {
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

unsigned narrow_width(const std::vector<uint64_t>& v) {
    uint64_t all = 0;
    for (uint64_t x : v) all |= x;
    return std::max(1u, (bit_width(all) + 7) / 8);
}

template <unsigned W>
void pack_narrow_w(const uint64_t* v, size_t n, char* o) {
    for (size_t i = 0; i < n; ++i) std::memcpy(o + i * W, &v[i], W);
}

void pack_narrow(const std::vector<uint64_t>& v, unsigned width, std::string& out) {
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

bool lz4_compress(const std::string& in, std::string& out) {
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

/// One compression context a thread, kept: a context made per call allocates its tables each time.
bool zstd_compress(const std::string& in, int level, std::string& out) {
    thread_local ZSTD_CCtx* cctx = ZSTD_createCCtx();
    out.resize(ZSTD_compressBound(in.size()));
    const size_t n = ZSTD_compressCCtx(cctx, out.data(), out.size(), in.data(), in.size(), level);
    if (ZSTD_isError(n)) {
        out.clear();
        return false;
    }
    out.resize(n);
    return true;
}

void put_header(std::string& out, const Stream& s, Packing p, unsigned width, Compressor c,
                size_t packed_bytes) {
    out.push_back(static_cast<char>(encoding_byte(s.transform, s.scale > 1, p, c)));
    if (p == Packing::kNarrow) out.push_back(static_cast<char>(width));
    put_varint(out, s.count);
    if (s.transform != Transform::kNone) put_varint(out, s.anchor);
    if (s.scale > 1) put_varint(out, s.scale);
    if (c != Compressor::kNone) put_varint(out, packed_bytes);
}

size_t header_bytes(const Stream& s, Packing p, Compressor c, size_t packed_bytes) {
    thread_local std::string h;
    h.clear();
    put_header(h, s, p, 1, c, packed_bytes);
    return h.size();
}

/// `in` compressed by `c` into `out`; false when it could not be, which LZ4 and ZSTD do not do for
/// a valid level and a buffer of their bound.
bool compress(Compressor c, const std::string& in, int zstd_level, std::string& out) {
    return c == Compressor::kZstd ? zstd_compress(in, zstd_level, out) : lz4_compress(in, out);
}

void pack(const Stream& s, Packing p, unsigned width, std::string& out) {
    switch (p) {
    case Packing::kSimple8b: put_u64_words(out, encode_simple8b(s.values).words); return;
    case Packing::kRuns:     pack_runs(s.values, out); return;
    case Packing::kBlocks:   pack_blocks(s.values, out); return;
    case Packing::kNarrow:   pack_narrow(s.values, width, out); return;
    }
}

/// `s` packed as `p`, compressed by `c` or not, as a whole block appended to `out`; its size.
size_t emit(const Stream& s, Packing p, Compressor c, int zstd_level, std::string& out) {
    const unsigned width = p == Packing::kNarrow ? narrow_width(s.values) : 0;
    thread_local std::string packed, compressed;
    packed.clear();
    pack(s, p, width, packed);
    const size_t at = out.size();
    if (c != Compressor::kNone && compress(c, packed, zstd_level, compressed)) {
        put_header(out, s, p, width, c, packed.size());
        out += compressed;
        return out.size() - at;
    }
    if (c != Compressor::kNone) {
        OB_LOG_WARN("codec", "%s could not compress %zu bytes; the block is written uncompressed",
                    c == Compressor::kZstd ? "ZSTD" : "LZ4", packed.size());
    }
    put_header(out, s, p, width, Compressor::kNone, 0);
    out += packed;
    return out.size() - at;
}

// ── Decoding ─────────────────────────────────────────────────────────────────

bool fail(std::string* why, const char* reason) {
    if (why != nullptr) *why = reason;
    return false;
}

bool unpack_simple8b(std::span<const char> p, size_t n, std::vector<uint64_t>& out, std::string* why) {
    if (p.size() % sizeof(uint64_t) != 0) return fail(why, "Simple8b payload is not whole words");
    thread_local std::vector<uint64_t> words;
    words.resize(p.size() / sizeof(uint64_t));
    if (!words.empty()) std::memcpy(words.data(), p.data(), p.size());
    if (!decode_simple8b_exact(words, n, out)) {
        return fail(why, "Simple8b words do not hold exactly the values the block declares");
    }
    return true;
}

bool unpack_blocks(std::span<const char> p, size_t n, std::vector<uint64_t>& out, std::string* why) {
    out.resize(n);
    thread_local std::vector<unsigned char> buf;
    size_t at = 0;
    for (size_t i = 0; i < n; i += kBlockValues) {
        const size_t m = std::min(kBlockValues, n - i);
        if (at >= p.size()) return fail(why, "blocks cut short");
        const unsigned w = static_cast<uint8_t>(p[at++]);
        if (w > 64) return fail(why, "a block wider than 64 bits");
        const size_t need = (m * w + 7) / 8;
        if (need > p.size() - at) return fail(why, "blocks cut short");
        if (w == 0) {
            std::fill_n(out.data() + i, m, uint64_t{0});
            continue;
        }
        // The eight bytes at any value's first bit, and the ninth after them, are read: straight
        // from the payload while those nine cannot pass its end, and from a padded copy of the block
        // where they could - the last block or two. Copying every block, zeros first, was a tenth of
        // a format-3 scan's time (measured on the ARM host).
        const unsigned char* base;
        if (at + need + 9 <= p.size()) {
            base = reinterpret_cast<const unsigned char*>(p.data()) + at;
        } else {
            buf.assign(need + 9, 0);
            std::memcpy(buf.data(), p.data() + at, need);
            base = buf.data();
        }
        const uint64_t mask = w == 64 ? std::numeric_limits<uint64_t>::max() : (uint64_t{1} << w) - 1;
        uint64_t* o = out.data() + i;
        if (w <= 56) {
            // A value and its shift fit the eight bytes: no ninth to look at.
            for (size_t k = 0; k < m; ++k) {
                const size_t bit = k * w;
                o[k] = (load64(base + (bit >> 3)) >> (bit & 7)) & mask;
            }
        } else {
            for (size_t k = 0; k < m; ++k) {
                const size_t bit = k * w;
                const unsigned shift = static_cast<unsigned>(bit & 7);
                const unsigned char* q = base + (bit >> 3);
                uint64_t x = load64(q) >> shift;
                if (shift + w > 64) x |= static_cast<uint64_t>(q[8]) << (64 - shift);
                o[k] = x & mask;
            }
        }
        at += need;
    }
    if (at != p.size()) return fail(why, "bytes left over after the blocks");
    return true;
}

/// A block's header, read and checked: what is packed, how, and where the payload begins.
struct Header {
    Transform transform{Transform::kNone};
    Packing packing{Packing::kSimple8b};
    Compressor compressor{Compressor::kNone};
    unsigned width{0};
    uint64_t anchor{0};
    uint64_t scale{1};
    uint64_t packed{0};      ///< the payload's length before compression, when compressed
    size_t payload_at{0};
    size_t n{0};             ///< the values the payload packs: the count, or one fewer for delta
};

bool read_header(std::span<const char> block, size_t count, Header& h, std::string* why) {
    if (block.empty()) return fail(why, "an empty block");
    const auto e = static_cast<uint8_t>(block[0]);
    if ((e & 0x80) != 0 || (e & (kZstdBit | kLz4Bit)) == (kZstdBit | kLz4Bit)) {
        return fail(why, "unknown encoding bits");
    }
    const auto t = static_cast<uint8_t>(e & 0x3);
    if (t > 2) return fail(why, "unknown transform");
    h.transform = static_cast<Transform>(t);
    const bool scaled = (e & kScaledBit) != 0;
    h.packing = static_cast<Packing>((e >> 3) & 0x3);
    h.compressor = (e & kZstdBit) != 0 ? Compressor::kZstd
                 : (e & kLz4Bit) != 0  ? Compressor::kLz4
                                       : Compressor::kNone;
    size_t at = 1;
    h.width = 0;
    if (h.packing == Packing::kNarrow) {
        if (at >= block.size()) return fail(why, "header cut short");
        h.width = static_cast<uint8_t>(block[at++]);
        if (h.width < 1 || h.width > 8) return fail(why, "a narrow width outside 1 - 8");
    }
    uint64_t declared = 0;
    if (!get_varint(block, at, declared)) return fail(why, "header cut short");
    if (declared != count) return fail(why, "the block holds another number of values than the column");
    h.anchor = 0;
    if (h.transform != Transform::kNone && !get_varint(block, at, h.anchor)) return fail(why, "header cut short");
    h.scale = 1;
    if (scaled) {
        if (!get_varint(block, at, h.scale)) return fail(why, "header cut short");
        if (h.scale < 2) return fail(why, "a scale below 2");
        if (h.transform == Transform::kNone) return fail(why, "a scale without a transform");
    }
    h.n = h.transform == Transform::kDelta ? (count == 0 ? 0 : count - 1) : count;
    h.packed = 0;
    if (h.compressor != Compressor::kNone) {
        if (!get_varint(block, at, h.packed)) return fail(why, "header cut short");
        // No packing takes more than 32 bytes a value (runs at Simple8b's worst, two words for each
        // of a value and a length), so a declared length past that is not a block of this count.
        if (h.packed > 32 * static_cast<uint64_t>(h.n) + 64) {
            return fail(why, "a packed length past any packing of the values");
        }
    }
    h.payload_at = at;
    return true;
}

/// The packed payload: the block's own bytes, or - compressed - decompressed into this thread's
/// buffer, which it is then a view of.
bool payload_of(std::span<const char> block, const Header& h, std::span<const char>& payload,
                std::string* why) {
    if (h.compressor == Compressor::kNone) {
        payload = block.subspan(h.payload_at);
        return true;
    }
    thread_local std::string plain;
    plain.resize(static_cast<size_t>(h.packed));
    const char* src = block.data() + h.payload_at;
    const size_t src_size = block.size() - h.payload_at;
    if (h.compressor == Compressor::kZstd) {
        thread_local ZSTD_DCtx* dctx = ZSTD_createDCtx();
        const size_t got = ZSTD_decompressDCtx(dctx, plain.data(), plain.size(), src, src_size);
        if (ZSTD_isError(got) || got != h.packed) {
            return fail(why, "ZSTD payload does not decompress to its declared length");
        }
    } else {
        // LZ4 takes int lengths; the declared one is bounded above, and the block's own is a file's.
        if (src_size > static_cast<size_t>(std::numeric_limits<int>::max())) {
            return fail(why, "an LZ4 payload longer than LZ4 takes");
        }
        const int got = LZ4_decompress_safe(src, plain.data(), static_cast<int>(src_size),
                                            static_cast<int>(plain.size()));
        if (got < 0 || static_cast<uint64_t>(got) != h.packed) {
            return fail(why, "LZ4 payload does not decompress to its declared length");
        }
    }
    payload = std::span<const char>(plain.data(), plain.size());
    return true;
}

template <typename T>
constexpr bool kTakesEveryValue = std::is_same_v<T, uint64_t> || std::is_same_v<T, int64_t>;

template <typename T>
constexpr uint64_t kMaxOf = kTakesEveryValue<T> ? std::numeric_limits<uint64_t>::max()
                                                : static_cast<uint64_t>(std::numeric_limits<T>::max());

/// The zigzagged difference `z` as the 64-bit value it adds, wrapping: -1 for 1, 1 for 2.
inline uint64_t unzigzag(uint64_t z) { return (z >> 1) ^ (0 - (z & 1)); }

/// The inverse of `h`'s transform over the `count` values `get(i)` packs, written to `o` as T, in
/// one pass: the unpacking and the inverse fused, each loop specialised for a divisor of 1. What
/// does not fit T is collected and refused once, after the loop, so that the loop has no branch
/// out of it for the compiler to keep. Returns whether every value fitted.
template <typename T, typename Get>
bool apply_inverse(const Header& h, size_t count, Get get, T* o) {
    constexpr uint64_t kMax = kMaxOf<T>;
    const uint64_t g = h.scale;
    const uint64_t a = h.anchor;
    uint64_t above = 0;
    switch (h.transform) {
    case Transform::kNone:
        for (size_t i = 0; i < count; ++i) {
            const uint64_t v = get(i);
            above |= v > kMax;
            o[i] = static_cast<T>(v);
        }
        break;
    case Transform::kFor:
        if (g == 1) {
            for (size_t i = 0; i < count; ++i) {
                const uint64_t v = get(i) + a;
                above |= v > kMax;
                o[i] = static_cast<T>(v);
            }
        } else {
            for (size_t i = 0; i < count; ++i) {
                const uint64_t v = get(i) * g + a;
                above |= v > kMax;
                o[i] = static_cast<T>(v);
            }
        }
        break;
    case Transform::kDelta: {
        if (count == 0) break;
        uint64_t v = a;
        above |= v > kMax;
        o[0] = static_cast<T>(v);
        if (g == 1) {
            for (size_t i = 1; i < count; ++i) {
                v += unzigzag(get(i - 1));
                above |= v > kMax;
                o[i] = static_cast<T>(v);
            }
        } else {
            for (size_t i = 1; i < count; ++i) {
                v += unzigzag(get(i - 1)) * g;
                above |= v > kMax;
                o[i] = static_cast<T>(v);
            }
        }
        break;
    }
    }
    return above == 0;
}

template <typename T, unsigned W>
bool narrow_inverse(const Header& h, const char* p, size_t count, T* o) {
    return apply_inverse<T>(h, count, [p](size_t i) {
        uint64_t x = 0;
        std::memcpy(&x, p + i * W, W);   // little-endian hosts only, as the WAL is
        return x;
    }, o);
}

/// Runs, unpacked straight into the column: a value filled run by run for none and for, and the
/// running sum stepped by one run's difference at a time for delta.
template <typename T>
bool runs_inverse(const Header& h, std::span<const char> p, size_t count, T* o, std::string* why) {
    size_t at = 0;
    uint64_t runs = 0, value_words = 0;
    if (!get_varint(p, at, runs) || !get_varint(p, at, value_words)) return fail(why, "runs header cut short");
    const size_t n = h.n;
    if (runs > n || (n > 0 && runs == 0)) return fail(why, "runs more than values, or none for some");
    const size_t rest = p.size() - at;
    if (value_words > rest / sizeof(uint64_t)) return fail(why, "runs' value words past the payload");
    const size_t value_bytes = static_cast<size_t>(value_words) * sizeof(uint64_t);
    thread_local std::vector<uint64_t> values, lengths;
    if (!unpack_simple8b(p.subspan(at, value_bytes), static_cast<size_t>(runs), values, why)) return false;
    if (!unpack_simple8b(p.subspan(at + value_bytes), static_cast<size_t>(runs), lengths, why)) return false;
    // The runs cover the packed values exactly - checked before anything is written.
    size_t total = 0;
    for (size_t r = 0; r < runs; ++r) {
        if (total >= n || lengths[r] > n - total - 1) return fail(why, "runs longer than the values");
        total += static_cast<size_t>(lengths[r]) + 1;
    }
    if (total != n) return fail(why, "runs shorter than the values");
    constexpr uint64_t kMax = kMaxOf<T>;
    const uint64_t g = h.scale;
    uint64_t above = 0;
    if (h.transform == Transform::kDelta) {
        if (count == 0) return true;
        uint64_t v = h.anchor;
        above |= v > kMax;
        o[0] = static_cast<T>(v);
        size_t i = 1;
        for (size_t r = 0; r < runs; ++r) {
            // One run's difference d added len times: the k-th value is v + (k + 1) d, which has no
            // dependence on the value before it, so the run fills as a vector does.
            const uint64_t d = unzigzag(values[r]) * g;
            const size_t len = static_cast<size_t>(lengths[r]) + 1;
            T* run = o + i;
            for (size_t k = 0; k < len; ++k) {
                const uint64_t x = v + (k + 1) * d;
                above |= x > kMax;
                run[k] = static_cast<T>(x);
            }
            v += len * d;
            i += len;
        }
    } else {
        size_t i = 0;
        for (size_t r = 0; r < runs; ++r) {
            const uint64_t v = h.transform == Transform::kFor ? values[r] * g + h.anchor : values[r];
            above |= v > kMax;
            std::fill_n(o + i, static_cast<size_t>(lengths[r]) + 1, static_cast<T>(v));
            i += static_cast<size_t>(lengths[r]) + 1;
        }
    }
    if (above != 0) return fail(why, "a value past the column's type");
    return true;
}

}  // namespace

std::string Encoding::name() const {
    std::string s = transform == Transform::kNone ? "none" : transform == Transform::kFor ? "for" : "delta";
    s += "/";
    switch (packing) {
    case Packing::kSimple8b: s += "s8b"; break;
    case Packing::kRuns:     s += "runs"; break;
    case Packing::kBlocks:   s += "blocks"; break;
    case Packing::kNarrow:   s += "narrow"; break;
    }
    if (compressor == Compressor::kZstd) s += "+zstd";
    if (compressor == Compressor::kLz4) s += "+lz4";
    return s;
}

void put_varint(std::string& out, uint64_t v) {
    while (v >= 0x80) {
        out.push_back(static_cast<char>((v & 0x7F) | 0x80));
        v >>= 7;
    }
    out.push_back(static_cast<char>(v));
}

bool get_varint(std::span<const char> in, size_t& at, uint64_t& v) {
    v = 0;
    for (unsigned shift = 0; shift < 64; shift += 7) {
        if (at >= in.size()) return false;
        const auto b = static_cast<uint8_t>(in[at++]);
        // The tenth byte holds bit 63 and nothing above it.
        if (shift == 63 && (b & 0xFE) != 0) return false;
        v |= static_cast<uint64_t>(b & 0x7F) << shift;
        if ((b & 0x80) == 0) return true;
    }
    return false;
}

size_t encode_as(std::span<const uint64_t> values, const Encoding& encoding, int zstd_level,
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
    }
    return emit(s, encoding.packing, encoding.compressor, zstd_level, out);
}

Choice encode(std::span<const uint64_t> values, const EncodeOptions& options, std::string& out) {
    if (options.hint != nullptr &&
        (options.hint->compressor == Compressor::kNone ||
         (options.hint->compressor == Compressor::kLz4 && options.lz4) ||
         (options.hint->compressor == Compressor::kZstd && options.zstd_level != 0))) {
        Choice choice;
        choice.encoding = *options.hint;
        choice.bytes = encode_as(values, *options.hint, options.zstd_level, out);
        choice.uncompressed_bytes = choice.bytes;
        return choice;
    }
    // Design §3: both transforms; uncompressed Simple8b and runs of each, blocks of `for`; and, under
    // LZ4 and under ZSTD where these options try them, runs and narrow of each. Each payload is
    // packed once and compressed once per compressor, and the chosen block is written from what was
    // packed rather than packed again.
    thread_local Stream streams[2];
    thread_local std::string s8b[2], runs[2], narrow[2];
    thread_local std::string compressed[2][2][2];   // [compressor: lz4, zstd][stream][runs, narrow]
    transform_for(values, streams[0]);
    transform_delta(values, streams[1]);

    enum class What { kS8b, kRuns, kBlocks, kCompressedRuns, kCompressedNarrow };
    struct Best {
        int stream{0};
        What what{What::kS8b};
        Compressor compressor{Compressor::kNone};
        size_t bytes{std::numeric_limits<size_t>::max()};
    };
    Best light, lz4, zstd;
    for (int t = 0; t < 2; ++t) {
        const Stream& s = streams[t];
        s8b[t].clear();
        put_u64_words(s8b[t], encode_simple8b(s.values).words);
        runs[t].clear();
        pack_runs(s.values, runs[t]);
        const size_t b_s8b = header_bytes(s, Packing::kSimple8b, Compressor::kNone, 0) + s8b[t].size();
        const size_t b_runs = header_bytes(s, Packing::kRuns, Compressor::kNone, 0) + runs[t].size();
        if (b_s8b < light.bytes) light = Best{t, What::kS8b, Compressor::kNone, b_s8b};
        if (b_runs < light.bytes) light = Best{t, What::kRuns, Compressor::kNone, b_runs};
        if (t == 0) {
            const size_t b_blocks =
                header_bytes(s, Packing::kBlocks, Compressor::kNone, 0) + blocks_bytes(s.values);
            if (b_blocks < light.bytes) light = Best{t, What::kBlocks, Compressor::kNone, b_blocks};
        }
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
        for (int t = 0; t < 2; ++t) {
            const Stream& s = streams[t];
            std::string* payloads[2] = {&runs[t], &narrow[t]};
            for (int k = 0; k < 2; ++k) {
                std::string& z = compressed[c][t][k];
                if (!compress(compressor, *payloads[k], options.zstd_level, z)) continue;
                const Packing packing = k == 0 ? Packing::kRuns : Packing::kNarrow;
                const size_t bytes = header_bytes(s, packing, compressor, payloads[k]->size()) + z.size();
                if (bytes < best_here.bytes) {
                    best_here = Best{t, k == 0 ? What::kCompressedRuns : What::kCompressedNarrow,
                                     compressor, bytes};
                }
            }
        }
    }
    // The smallest, with two margins: a compressed candidate has to be smaller than the smallest
    // uncompressed one by `compressed_margin_pct`, and a ZSTD one than the smallest LZ4 one by
    // `zstd_margin_pct` - both cost a decompression, ZSTD's several times LZ4's.
    const auto smaller_by = [](size_t bytes, size_t than, unsigned pct) {
        return 100 * static_cast<uint64_t>(bytes) <
               static_cast<uint64_t>(100 - std::min(pct, 100u)) * than;
    };
    Best best = light;
    if (try_lz4 && lz4.bytes < best.bytes && smaller_by(lz4.bytes, light.bytes, options.compressed_margin_pct)) {
        best = lz4;
    }
    if (try_zstd && zstd.bytes < best.bytes &&
        smaller_by(zstd.bytes, light.bytes, options.compressed_margin_pct) &&
        (!try_lz4 || smaller_by(zstd.bytes, lz4.bytes, options.zstd_margin_pct))) {
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
    return choice;
}

bool decode(std::span<const char> block, size_t count, std::vector<uint64_t>& out, std::string* why) {
    return decode_as<uint64_t>(block, count, out, why);
}

template <typename T>
bool decode_as(std::span<const char> block, size_t count, std::vector<T>& out, std::string* why) {
    Header h;
    if (!read_header(block, count, h, why)) return false;
    std::span<const char> payload;
    if (!payload_of(block, h, payload, why)) return false;
    out.resize(count);
    T* o = out.data();
    bool fits = true;
    switch (h.packing) {
    case Packing::kNarrow: {
        if (payload.size() != h.n * h.width) return fail(why, "narrow payload is not the values' bytes");
        const char* p = payload.data();
        switch (h.width) {
        case 1: fits = narrow_inverse<T, 1>(h, p, count, o); break;
        case 2: fits = narrow_inverse<T, 2>(h, p, count, o); break;
        case 3: fits = narrow_inverse<T, 3>(h, p, count, o); break;
        case 4: fits = narrow_inverse<T, 4>(h, p, count, o); break;
        case 5: fits = narrow_inverse<T, 5>(h, p, count, o); break;
        case 6: fits = narrow_inverse<T, 6>(h, p, count, o); break;
        case 7: fits = narrow_inverse<T, 7>(h, p, count, o); break;
        default: fits = narrow_inverse<T, 8>(h, p, count, o); break;
        }
        break;
    }
    case Packing::kRuns:
        return runs_inverse<T>(h, payload, count, o, why);
    case Packing::kSimple8b:
    case Packing::kBlocks: {
        thread_local std::vector<uint64_t> values;
        const bool unpacked = h.packing == Packing::kSimple8b ? unpack_simple8b(payload, h.n, values, why)
                                                              : unpack_blocks(payload, h.n, values, why);
        if (!unpacked) return false;
        const uint64_t* v = values.data();
        fits = apply_inverse<T>(h, count, [v](size_t i) { return v[i]; }, o);
        break;
    }
    }
    if (!fits) return fail(why, "a value past the column's type");
    return true;
}

template bool decode_as<uint64_t>(std::span<const char>, size_t, std::vector<uint64_t>&, std::string*);
template bool decode_as<int64_t>(std::span<const char>, size_t, std::vector<int64_t>&, std::string*);
template bool decode_as<uint32_t>(std::span<const char>, size_t, std::vector<uint32_t>&, std::string*);
template bool decode_as<uint16_t>(std::span<const char>, size_t, std::vector<uint16_t>&, std::string*);
template bool decode_as<uint8_t>(std::span<const char>, size_t, std::vector<uint8_t>&, std::string*);

}  // namespace ob::column_codec
