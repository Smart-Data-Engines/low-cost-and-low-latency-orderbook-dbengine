// The encoder against the one it replaced, byte for byte (#267).
//
// column_codec_reference.hpp holds the encoder `master` had at 0ed158b, copied rather than rewritten.
// Every block encode_as() writes, in every encoding at either ZSTD level, and every block and choice
// encode() makes under every policy a node uses - a seal's and a merge's, in segment format 3 and 4,
// at the runs charges the codec's tests use, with every encoding as the hint and with none - has to be
// the one that encoder made. A reader takes any block the format allows, so this is not a
// compatibility rule: it is what makes the change a change of speed and of nothing else, and what lets
// a seal's bytes on disk be compared between the two builds by one number (seal_encode_cost's hash).
#include "column_codec_reference.hpp"
#include "orderbook/column_codec.hpp"
#include "orderbook/columnar_store.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

using namespace ob::column_codec;

namespace {

constexpr uint64_t kMax = std::numeric_limits<uint64_t>::max();
constexpr uint64_t kInt64Min = uint64_t{1} << 63;
constexpr uint64_t kMarker = (uint64_t{1} << 60) - 1;   // Simple8b's fallback marker

std::vector<Encoding> all_encodings() {
    std::vector<Encoding> out;
    for (Transform t : {Transform::kNone, Transform::kFor, Transform::kDelta}) {
        for (Packing p : {Packing::kSimple8b, Packing::kRuns, Packing::kBlocks, Packing::kNarrow}) {
            for (Compressor c : {Compressor::kNone, Compressor::kZstd, Compressor::kLz4}) {
                out.push_back(Encoding{t, p, c});
            }
        }
    }
    for (Compressor c : {Compressor::kNone, Compressor::kZstd, Compressor::kLz4}) {
        out.push_back(Encoding{Transform::kSteps, Packing::kRuns, c});
    }
    return out;
}

struct Policy {
    const char* name;
    EncodeOptions options;
};

/// What a node encodes under: a seal's and a merge's options, each in format 3 and 4 (#250), the codec's
/// defaults (ZSTD tried with no margin), and a seal's at other runs charges (#223).
std::vector<Policy> policies() {
    const auto with_steps = [](EncodeOptions o) {
        o.steps = true;
        return o;
    };
    const auto charged = [](EncodeOptions o, unsigned pct) {
        o.runs_charge_pct = pct;
        return o;
    };
    const EncodeOptions seal = ob::ColumnarStore::SegmentFormat::seal().search;
    const EncodeOptions merge = ob::ColumnarStore::SegmentFormat::merge().search;
    return {
        {"seal", seal},
        {"seal4", with_steps(seal)},
        {"merge", merge},
        {"merge4", with_steps(merge)},
        {"defaults", EncodeOptions{}},
        {"seal@0", charged(seal, 0)},
        {"seal4@50", charged(with_steps(seal), 50)},
        {"seal@200", charged(seal, 200)},
    };
}

/// Both encoders on one column: every encoding as encode_as() writes it, every policy's choice with no
/// hint, and a seal's with each encoding as the hint. The first difference, or nothing.
std::string difference_from_reference(const std::vector<uint64_t>& column) {
    for (const Encoding& e : all_encodings()) {
        for (int level : {1, 3}) {
            std::string ours, theirs;
            const size_t n_ours = encode_as(column, e, level, ours);
            const size_t n_theirs = reference::encode_as(column, e, level, theirs);
            if (n_ours != n_theirs || ours != theirs) {
                return "encode_as " + e.name() + " at ZSTD " + std::to_string(level) + ": " +
                       std::to_string(ours.size()) + " bytes against the reference's " +
                       std::to_string(theirs.size());
            }
        }
    }
    // Every encoding as the hint under a seal's policies, in both formats - where a hint is written in,
    // or refused for a compressor or a transform the options leave out - and none under every policy.
    const std::vector<Encoding> hints = all_encodings();
    for (const Policy& policy : policies()) {
        const bool hinted = policy.name == std::string("seal") || policy.name == std::string("seal4");
        for (size_t h = hinted ? 0 : hints.size(); h <= hints.size(); ++h) {
            EncodeOptions options = policy.options;
            options.hint = h < hints.size() ? &hints[h] : nullptr;
            std::string ours, theirs;
            const Choice c_ours = encode(column, options, ours);
            const Choice c_theirs = reference::encode(column, options, theirs);
            if (!(c_ours.encoding == c_theirs.encoding) || c_ours.bytes != c_theirs.bytes ||
                c_ours.uncompressed_bytes != c_theirs.uncompressed_bytes || ours != theirs) {
                return std::string("encode under ") + policy.name + " with " +
                       (h < hints.size() ? "the hint " + hints[h].name() : std::string("no hint")) + ": " +
                       c_ours.encoding.name() + " in " + std::to_string(c_ours.bytes) + " bytes (" +
                       std::to_string(c_ours.uncompressed_bytes) + ") against the reference's " +
                       c_theirs.encoding.name() + " in " + std::to_string(c_theirs.bytes) + " (" +
                       std::to_string(c_theirs.uncompressed_bytes) + ")" +
                       (ours == theirs ? "" : ", other bytes");
            }
        }
    }
    return {};
}

/// A value of `bits` bits at most, uniformly.
rc::Gen<uint64_t> value_of_width(unsigned bits) {
    if (bits == 0) return rc::gen::just(uint64_t{0});
    if (bits >= 64) return rc::gen::arbitrary<uint64_t>();
    return rc::gen::inRange<uint64_t>(0, uint64_t{1} << bits);
}

/// The divisors prices, sizes and timestamps share: none, small ones, a power of ten, of two, and one
/// of each with an odd factor - the case `Divisor` takes apart.
rc::Gen<uint64_t> divisor() {
    return rc::gen::element<uint64_t>(uint64_t{1}, uint64_t{2}, uint64_t{3}, uint64_t{6}, uint64_t{10},
                                      uint64_t{1000}, uint64_t{1'000'000}, uint64_t{1} << 20, uint64_t{3} << 30,
                                      uint64_t{1} << 40, uint64_t{1'000'000'000}, uint64_t{5} << 58);
}

/// A column of pieces, each of one shape, so that one column holds what a segment's does: runs of a
/// value, values of one width, steps of a tick, a ladder of prices, the edges of uint64 and Simple8b,
/// and multiples of a divisor that a later piece takes a factor out of.
rc::Gen<std::vector<uint64_t>> column() {
    return rc::gen::exec([] {
        std::vector<uint64_t> out;
        const auto pieces = *rc::gen::inRange<int>(0, 7);
        for (int p = 0; p < pieces; ++p) {
            const auto len = *rc::gen::weightedOneOf<size_t>({
                {4, rc::gen::inRange<size_t>(1, 20)},
                {3, rc::gen::inRange<size_t>(20, 300)},
                {1, rc::gen::inRange<size_t>(300, 1500)},
            });
            switch (*rc::gen::inRange<int>(0, 7)) {
            case 0: {   // a run of one value
                const auto v = *value_of_width(*rc::gen::inRange<unsigned>(0, 65));
                out.insert(out.end(), len, v);
                break;
            }
            case 1: {   // values of one width, in runs of one to three
                const auto bits = *rc::gen::inRange<unsigned>(0, 65);
                for (size_t k = 0; k < len;) {
                    const auto v = *value_of_width(bits);
                    const auto repeat = std::min<size_t>(*rc::gen::inRange<size_t>(1, 4), len - k);
                    out.insert(out.end(), repeat, v);
                    k += repeat;
                }
                break;
            }
            case 2: {   // steps of a tick, rising or falling, timestamps or sequence numbers
                uint64_t v = *rc::gen::arbitrary<uint64_t>();
                const auto tick = *divisor();
                const bool falling = *rc::gen::inRange<int>(0, 4) == 0;
                for (size_t k = 0; k < len; ++k) {
                    out.push_back(v);
                    const auto steps = *rc::gen::inRange<uint64_t>(0, 4);
                    v = falling ? v - steps * tick : v + steps * tick;
                }
                break;
            }
            case 3: {   // a ladder of prices on a tick, the mid moving
                uint64_t mid = *rc::gen::inRange<uint64_t>(1'000'000, uint64_t{1} << 50);
                const auto tick = *divisor();
                const auto levels = *rc::gen::inRange<uint64_t>(1, 21);
                for (size_t k = 0; k < len; ++k) {
                    out.push_back(mid - (k % levels + 1) * tick);
                    if (k % levels == levels - 1) mid += (*rc::gen::inRange<uint64_t>(0, 5)) * tick - 2 * tick;
                }
                break;
            }
            case 4: {   // the edges
                for (size_t k = 0; k < len && k < 40; ++k) {
                    out.push_back(*rc::gen::element<uint64_t>(uint64_t{0}, uint64_t{1}, kMax, kMax - 1, kInt64Min,
                                                              kInt64Min - 1, kMarker - 1, kMarker, kMarker + 1,
                                                              uint64_t{1} << 60, uint64_t{1} << 59));
                }
                break;
            }
            case 5: {   // multiples of a divisor
                const auto g = *divisor();
                const auto bits = *rc::gen::inRange<unsigned>(0, 24);
                const auto base = *rc::gen::inRange<uint64_t>(0, uint64_t{1} << 40);
                for (size_t k = 0; k < len; ++k) out.push_back(base * g + *value_of_width(bits) * g);
                break;
            }
            default: {   // small values, as counts, sides and levels are
                const auto top = *rc::gen::inRange<uint64_t>(1, 300);
                for (size_t k = 0; k < len; ++k) out.push_back(*rc::gen::inRange<uint64_t>(0, top));
                break;
            }
            }
        }
        return out;
    });
}

}  // namespace

RC_GTEST_PROP(ColumnCodecEquivalence, EveryBlockAndChoiceIsTheReferenceEncodersForAnyColumn, ()) {
    // At full size whatever the case's: RapidCheck scales `inRange` with it, and a column drawn small is
    // a few values of a few bits, past no block and no word.
    const auto values = *rc::gen::resize(100, column());
    RC_CLASSIFY(values.size() > 240, "past a word of 240 zeros");
    RC_CLASSIFY(values.size() > 128, "past a block of 128");
    const std::string why = difference_from_reference(values);
    RC_LOG() << why << "\n";
    RC_ASSERT(why.empty());
}

// The divisor found late: multiples of a large divisor for most of the column and then values each of
// which takes a factor out of it - the case where a divisor applied as it is found has to be taken back
// for what came before, at every position and every number of times.
RC_GTEST_PROP(ColumnCodecEquivalence, ADivisorThatShrinksLateIsTheReferenceEncodersForAnyColumn, ()) {
    const auto n = *rc::gen::resize(100, rc::gen::inRange<size_t>(1, 2000));
    const auto shrinks = *rc::gen::resize(100, rc::gen::inRange<int>(0, 70));
    uint64_t g = uint64_t{1} << 62;
    std::vector<uint64_t> values;
    values.reserve(n + static_cast<size_t>(shrinks));
    const auto base = *rc::gen::inRange<uint64_t>(0, 4);
    for (size_t k = 0; k < n; ++k) values.push_back((base + k % 3) * g);
    for (int s = 0; s < shrinks; ++s) {
        // A factor of two out of the divisor, or of three and two, at a position drawn anywhere.
        g = *rc::gen::inRange<int>(0, 3) == 0 && g % 6 == 0 ? g / 6 : g / 2;
        if (g == 0) break;
        const auto at = *rc::gen::resize(100, rc::gen::inRange<size_t>(0, values.size() + 1));
        values.insert(values.begin() + static_cast<std::ptrdiff_t>(at), values.empty() ? g : values[0] + g);
    }
    const std::string why = difference_from_reference(values);
    RC_LOG() << why << "\n";
    RC_ASSERT(why.empty());
}

// The columns the codec's own tests use, and a few a segment holds, each long.
TEST(ColumnCodecEquivalence, EdgeAndSegmentShapedColumnsAreTheReferenceEncoders) {
    std::vector<std::vector<uint64_t>> columns = {
        {},
        {0},
        {kMax},
        {kInt64Min},
        {7, 7, 7, 7, 7},
        {0, kMax, 0, kMax, 0},
        {kInt64Min, kInt64Min - 1, kInt64Min, 0, kMax},
        {kMarker - 1, kMarker, uint64_t{1} << 60, 5, 7},
        {10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
    };
    // Timestamps: twenty rows an update share one, 50 ms apart; a ladder on a tick of 10^6; levels.
    std::vector<uint64_t> ts, ladder, levels, mixed;
    uint64_t mid = 6'234'567'000'000ULL;
    for (uint64_t u = 0; u < 600; ++u) {
        mid += ((u * 7919) % 21) * 1'000'000ULL - 10'000'000ULL;
        for (uint64_t l = 0; l < 20; ++l) {
            ts.push_back(1'790'000'000'000'000'000ULL + u * 50'000'000ULL);
            ladder.push_back(mid - (l + 1) * 1'000'000ULL);
            levels.push_back(l);
        }
    }
    for (uint64_t i = 0; i < 12'000; ++i) mixed.push_back(i % 3 == 0 ? (i * 0x9E3779B97F4A7C15ULL) : i % 17);
    columns.push_back(ts);
    columns.push_back(ladder);
    columns.push_back(levels);
    columns.push_back(mixed);
    for (const auto& c : columns) EXPECT_EQ(difference_from_reference(c), "") << "a column of " << c.size();
}
