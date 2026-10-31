// Column blocks of segment format 3 (smart-data-engine-workspace/specs/segment-format-v3/, design §2 and §3).
//
// Every encoding has to give back exactly the values it was given - for every value, not for the
// range prices happen to occupy - and a block that is not what the encoder writes has to be
// refused rather than decoded into other numbers.

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "orderbook/codec.hpp"
#include "orderbook/column_codec.hpp"

using namespace ob::column_codec;

namespace {

std::vector<Encoding> all_encodings() {
    std::vector<Encoding> out;
    for (Transform t : {Transform::kNone, Transform::kFor, Transform::kDelta}) {
        for (Packing p : {Packing::kSimple8b, Packing::kRuns, Packing::kBlocks, Packing::kNarrow}) {
            for (Compressor c : {Compressor::kNone, Compressor::kZstd, Compressor::kLz4}) {
                out.push_back(Encoding{t, p, c});
            }
        }
    }
    // Steps are runs only (#250).
    for (Compressor c : {Compressor::kNone, Compressor::kZstd, Compressor::kLz4}) {
        out.push_back(Encoding{Transform::kSteps, Packing::kRuns, c});
    }
    return out;
}

constexpr uint64_t kMax = std::numeric_limits<uint64_t>::max();
constexpr uint64_t kInt64Min = uint64_t{1} << 63;

std::vector<std::vector<uint64_t>> edge_columns() {
    std::vector<std::vector<uint64_t>> out = {
        {},
        {0},
        {kMax},
        {kInt64Min},
        {7, 7, 7, 7, 7},
        {0, kMax, 0, kMax, 0},
        {kInt64Min, kInt64Min - 1, kInt64Min, 0, kMax},
        {(uint64_t{1} << 60) - 2, (uint64_t{1} << 60) - 1, uint64_t{1} << 60, 5, 7},
        {10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
    };
    // A price ladder on a tick of 10^6, as Binance's prices are at eight decimals: twenty levels
    // an update, the mid moving between updates.
    std::vector<uint64_t> ladder;
    uint64_t mid = 6'234'567'000'000ULL;
    for (int u = 0; u < 300; ++u) {
        mid += static_cast<uint64_t>((u * 7919) % 21) * 1'000'000ULL - 10'000'000ULL;
        for (uint64_t l = 0; l < 20; ++l) ladder.push_back(mid - (l + 1) * 1'000'000ULL);
    }
    out.push_back(ladder);
    // Timestamps: twenty rows an update share one, 50 ms apart.
    std::vector<uint64_t> ts;
    for (uint64_t u = 0; u < 300; ++u) {
        for (int l = 0; l < 20; ++l) ts.push_back(1'790'000'000'000'000'000ULL + u * 50'000'000ULL);
    }
    out.push_back(ts);
    // Past one block of 128 and Simple8b's widest words, mixed.
    std::vector<uint64_t> mixed;
    for (uint64_t i = 0; i < 1000; ++i) mixed.push_back(i % 3 == 0 ? (i * 0x9E3779B97F4A7C15ULL) : i % 17);
    out.push_back(mixed);
    return out;
}

std::span<const char> bytes(const std::string& s) { return {s.data(), s.size()}; }

}  // namespace

TEST(ColumnCodec, EveryEncodingGivesBackEveryEdgeColumn) {
    for (const auto& column : edge_columns()) {
        for (const Encoding& e : all_encodings()) {
            std::string block;
            encode_as(column, e, 3, block);
            std::vector<uint64_t> back{42, 43};   // something there before, which must not survive
            std::string why;
            ASSERT_TRUE(decode(bytes(block), column.size(), back, &why))
                << e.name() << " on " << column.size() << " values: " << why;
            ASSERT_EQ(back, column) << e.name();
        }
    }
}

RC_GTEST_PROP(ColumnCodec, EveryEncodingGivesBackAnyColumn, ()) {
    const auto encodings = all_encodings();
    const auto e = encodings[*rc::gen::inRange<size_t>(0, encodings.size())];
    // Columns with the structure the encodings are for - runs, small steps, a common divisor - and
    // without it, so that every packing meets both.
    const auto column = *rc::gen::oneOf(
        rc::gen::arbitrary<std::vector<uint64_t>>(),
        rc::gen::container<std::vector<uint64_t>>(rc::gen::inRange<uint64_t>(0, 4)),
        rc::gen::map(rc::gen::arbitrary<std::vector<uint16_t>>(), [](const std::vector<uint16_t>& v) {
            std::vector<uint64_t> out;
            uint64_t acc = 1'700'000'000'000'000'000ULL;
            for (uint16_t x : v) out.push_back(acc += uint64_t{x} * 1'000'000ULL);
            return out;
        }));
    std::string block;
    encode_as(column, e, 1, block);
    std::vector<uint64_t> back;
    std::string why;
    RC_ASSERT(decode(bytes(block), column.size(), back, &why));
    RC_ASSERT(back == column);
}

TEST(ColumnCodec, TheChoiceIsTheSmallestCandidateAndGivesItsValuesBack) {
    // With nothing charged for runs (#223); the charge has its tests below.
    EncodeOptions by_size;
    by_size.runs_charge_pct = 0;
    for (const auto& column : edge_columns()) {
        std::string chosen;
        const Choice c = encode(column, by_size, chosen);
        ASSERT_EQ(c.bytes, chosen.size());
        std::vector<uint64_t> back;
        std::string why;
        ASSERT_TRUE(decode(bytes(chosen), column.size(), back, &why)) << c.encoding.name() << ": " << why;
        ASSERT_EQ(back, column);
        // No candidate is smaller: the five uncompressed, and runs and narrow of each transform
        // under each compressor.
        std::vector<Encoding> candidates = {
            {Transform::kFor, Packing::kSimple8b, Compressor::kNone},
            {Transform::kFor, Packing::kRuns, Compressor::kNone},
            {Transform::kFor, Packing::kBlocks, Compressor::kNone},
            {Transform::kDelta, Packing::kSimple8b, Compressor::kNone},
            {Transform::kDelta, Packing::kRuns, Compressor::kNone},
        };
        for (Compressor z : {Compressor::kLz4, Compressor::kZstd}) {
            for (Transform t : {Transform::kFor, Transform::kDelta}) {
                for (Packing p : {Packing::kRuns, Packing::kNarrow}) candidates.push_back(Encoding{t, p, z});
            }
        }
        for (const Encoding& e : candidates) {
            std::string other;
            encode_as(column, e, 3, other);
            EXPECT_LE(chosen.size(), other.size()) << c.encoding.name() << " chosen over " << e.name();
        }
    }
}

// The runs of what each transform leaves (#223): equal values for `for`, equal differences for
// `delta` - subtracting the minimum or dividing by a divisor changes neither - and how many values
// each counts them over.
struct RunsPerTransform {
    size_t for_runs{0}, for_values{0}, delta_runs{0}, delta_values{0};
};
RunsPerTransform runs_of(const std::vector<uint64_t>& v) {
    RunsPerTransform r;
    r.for_values = v.size();
    r.for_runs = v.empty() ? 0 : 1;
    for (size_t i = 1; i < v.size(); ++i) r.for_runs += v[i] != v[i - 1];
    r.delta_values = v.size() < 2 ? 0 : v.size() - 1;
    r.delta_runs = v.size() < 2 ? 0 : 1;
    for (size_t i = 2; i < v.size(); ++i) r.delta_runs += (v[i] - v[i - 1]) != (v[i - 1] - v[i - 2]);
    return r;
}

// A candidate's bytes as the choice weighs them: a runs one larger by its charge, the charge being
// `pct` percent times its runs per value, at most 90%. Steps' runs are runs of equal values, as for's.
double weighed(const Encoding& e, size_t bytes, const RunsPerTransform& r, unsigned pct) {
    if (e.packing != Packing::kRuns) return static_cast<double>(bytes);
    const size_t runs = e.transform == Transform::kDelta ? r.delta_runs : r.for_runs;
    const size_t values = e.transform == Transform::kDelta ? r.delta_values : r.for_values;
    if (values == 0) return static_cast<double>(bytes);
    const double runs_per_mille = std::floor(1000.0 * static_cast<double>(runs) / static_cast<double>(values));
    const double per_mille = std::min(900.0, std::floor(pct * runs_per_mille / 100.0));
    return std::floor(static_cast<double>(bytes) * 1000.0 / (1000.0 - per_mille));
}

std::vector<Encoding> every_candidate(bool steps = false) {
    std::vector<Encoding> out = {
        {Transform::kFor, Packing::kSimple8b, Compressor::kNone},
        {Transform::kFor, Packing::kRuns, Compressor::kNone},
        {Transform::kFor, Packing::kBlocks, Compressor::kNone},
        {Transform::kDelta, Packing::kSimple8b, Compressor::kNone},
        {Transform::kDelta, Packing::kRuns, Compressor::kNone},
    };
    for (Compressor z : {Compressor::kLz4, Compressor::kZstd}) {
        for (Transform t : {Transform::kFor, Transform::kDelta}) {
            for (Packing p : {Packing::kRuns, Packing::kNarrow}) out.push_back(Encoding{t, p, z});
        }
    }
    if (steps) {   // #250: steps/runs uncompressed and under each compressor
        for (Compressor z : {Compressor::kNone, Compressor::kLz4, Compressor::kZstd}) {
            out.push_back(Encoding{Transform::kSteps, Packing::kRuns, z});
        }
    }
    return out;
}

// Values drawn from a fixed sequence, each repeated once with a probability that sets the runs per
// value - short runs of values twenty bits wide, which no compressor takes anything from.
std::vector<uint64_t> short_runs(size_t n, unsigned repeat_per_mille, uint64_t seed) {
    std::vector<uint64_t> out;
    uint64_t x = seed;
    const auto next = [&x] {
        x = x * 6364136223846793005ULL + 1442695040888963407ULL;
        return x >> 33;
    };
    while (out.size() < n) {
        const uint64_t v = next() & ((uint64_t{1} << 20) - 1);
        out.push_back(v);
        if (out.size() < n && next() % 1000 < repeat_per_mille) out.push_back(v);
    }
    return out;
}

// A period of `period` random values twenty bits wide, each repeated `len` times, over and over: LZ4
// takes the repeats of the period from a packing of every value, and the period's length sets how
// much. Runs of two and a period of 1160 put LZ4's best between the runs' bytes and their weight,
// where weighing the two by bytes chooses the runs and by weight does not (#223).
std::vector<uint64_t> periodic_runs(unsigned len, unsigned period, size_t n) {
    std::vector<uint64_t> pattern;
    uint64_t x = 12345 + period;
    for (unsigned k = 0; k < period; ++k) {
        x = x * 6364136223846793005ULL + 1442695040888963407ULL;
        pattern.push_back(1 + ((x >> 33) & ((uint64_t{1} << 20) - 1)));
    }
    std::vector<uint64_t> out;
    for (size_t i = 0; out.size() < n; ++i) {
        for (unsigned k = 0; k < len && out.size() < n; ++k) out.push_back(pattern[i % period]);
    }
    return out;
}

TEST(ColumnCodec, TheChoiceIsTheSmallestWithARunsCandidateChargedForItsRuns) {
    // The rule, held exactly (#223): no candidate weighs less than the chosen one, a runs candidate
    // weighed as larger by its charge - for every charge, the default's included, with ZSTD and as a
    // seal searches, with steps and without (#250), on columns of runs short and long and on the edge
    // columns.
    std::vector<std::vector<uint64_t>> columns = edge_columns();
    for (unsigned repeat : {0u, 300u, 600u, 900u}) columns.push_back(short_runs(5'000, repeat, repeat + 1));
    std::vector<uint64_t> long_runs;
    for (uint64_t i = 0; i < 5'000; ++i) long_runs.push_back(1'000 + (i / 23) * 7 % 101);
    columns.push_back(long_runs);
    columns.push_back(periodic_runs(2, 1'160, 4'000));
    // A short stepping column on which a delta stream's runs per value, counted over its 26 values
    // rather than over the column's 27, decide the choice.
    columns.push_back({1004001, 1004001, 1004001, 1008002, 1012003, 1016004, 1017958, 1021959, 1025960,
                       1029961, 1029961, 1029961, 1033962, 1035916, 1039917, 1039917, 1043918, 1047826,
                       1051827, 1055828, 1059829, 1063830, 1067831, 1067831, 1067831, 1067831, 1071832});
    for (const bool steps : {false, true}) {   // format 3's candidates, and format 4's (#250)
        for (const int zstd_level : {3, 0}) {   // with ZSTD, and as a seal searches, without it
            for (unsigned pct : {0u, 10u, kRunsChargePct, 100u, 300u, std::numeric_limits<unsigned>::max()}) {
                EncodeOptions options;
                options.zstd_level = zstd_level;
                options.runs_charge_pct = pct;
                options.steps = steps;
                for (const auto& column : columns) {
                    SCOPED_TRACE(::testing::Message() << (steps ? "steps, " : "") << "ZSTD " << zstd_level
                                                      << ", charge " << pct << "%, " << column.size() << " values");
                    const RunsPerTransform r = runs_of(column);
                    std::string chosen;
                    const Choice c = encode(column, options, chosen);
                    std::vector<uint64_t> back;
                    std::string why;
                    ASSERT_TRUE(decode(bytes(chosen), column.size(), back, &why)) << c.encoding.name() << ": " << why;
                    ASSERT_EQ(back, column);
                    if (!steps) {
                        EXPECT_NE(c.encoding.transform, Transform::kSteps) << "steps where left out";
                    }
                    const double mine = weighed(c.encoding, chosen.size(), r, pct);
                    for (const Encoding& e : every_candidate(steps)) {
                        if (zstd_level == 0 && e.compressor == Compressor::kZstd) continue;
                        std::string other;
                        encode_as(column, e, zstd_level, other);
                        EXPECT_LE(mine, weighed(e, other.size(), r, pct))
                            << c.encoding.name() << " chosen over " << e.name();
                    }
                }
            }
        }
    }
}

TEST(ColumnCodec, RunsAValueOrTwoLongAreNotChosenForAFewPercent) {
    // Values each repeated with probability 0.43: 0.7 runs a value. Runs store a value and a length
    // a run, fewer bytes than any packing that stores every value - which made them the choice by
    // size - and their decode takes a step a run. Charged 35%, they lose to one of those.
    const auto column = short_runs(10'000, 430, 7);
    const RunsPerTransform r = runs_of(column);
    ASSERT_GT(r.for_runs * 10, r.for_values * 6) << "the column's runs are not short: " << r.for_runs;
    EncodeOptions by_size;
    by_size.runs_charge_pct = 0;
    std::string a, b;
    const Choice smallest = encode(column, by_size, a);
    ASSERT_EQ(smallest.encoding.packing, Packing::kRuns) << "by size alone the runs are not the smallest, "
                                                            "so this column tests nothing: " << smallest.encoding.name();
    const Choice charged = encode(column, EncodeOptions{}, b);
    EXPECT_NE(charged.encoding.packing, Packing::kRuns) << charged.encoding.name();
    EXPECT_GT(charged.bytes, smallest.bytes) << "the charge chose a block no larger, which the old rule would have";
}

TEST(ColumnCodec, LongRunsKeepTheirRunsUnderTheCharge) {
    // Runs twenty values long, as a book's timestamps are - 0.05 runs a value, a charge of 2.5% -
    // are chosen as they were by size.
    std::vector<uint64_t> ts;
    for (uint64_t u = 0; u < 500; ++u) {
        for (int l = 0; l < 20; ++l) ts.push_back(1'790'000'000'000'000'000ULL + u * 50'000'000ULL + (u % 7) * 13);
    }
    EncodeOptions by_size;
    by_size.runs_charge_pct = 0;
    std::string a, b;
    const Choice smallest = encode(ts, by_size, a);
    ASSERT_EQ(smallest.encoding.packing, Packing::kRuns) << smallest.encoding.name();
    const Choice charged = encode(ts, EncodeOptions{}, b);
    EXPECT_EQ(charged.encoding, smallest.encoding) << charged.encoding.name();
    EXPECT_EQ(b, a);
}

TEST(ColumnCodec, ACompressedCandidateMustSaveTheMarginToBeChosen) {
    // Two hundred snapshots of the same twenty quantities: ZSTD takes the repeats, which no
    // uncompressed candidate sees.
    std::vector<uint64_t> qty;
    for (int s = 0; s < 200; ++s) {
        for (uint64_t l = 0; l < 20; ++l) qty.push_back(100'000 + l * 7'919 % 5'003);
    }
    std::string a, b, c, d, e;
    EncodeOptions options;
    const Choice free = encode(qty, options, a);
    EXPECT_NE(free.encoding.compressor, Compressor::kNone) << free.encoding.name();
    // A margin it cannot meet - it would have to save all of it - keeps the uncompressed one.
    options.compressed_margin_pct = 100;
    const Choice never = encode(qty, options, b);
    EXPECT_EQ(never.encoding.compressor, Compressor::kNone) << never.encoding.name();
    EXPECT_EQ(never.bytes, never.uncompressed_bytes);
    // And with neither compressor tried, the same.
    EncodeOptions none_tried;
    none_tried.lz4 = false;
    none_tried.zstd_level = 0;
    const Choice none = encode(qty, none_tried, c);
    EXPECT_EQ(none.encoding.compressor, Compressor::kNone);
    EXPECT_EQ(none.bytes, never.bytes);
    // ZSTD over LZ4 only past its own margin: here ZSTD is smaller, and a margin of all of it
    // leaves LZ4.
    EncodeOptions zstd_free;
    const Choice zstd_chosen = encode(qty, zstd_free, d);
    ASSERT_EQ(zstd_chosen.encoding.compressor, Compressor::kZstd) << zstd_chosen.encoding.name();
    EncodeOptions zstd_never;
    zstd_never.zstd_margin_pct = 100;
    const Choice lz4_kept = encode(qty, zstd_never, e);
    EXPECT_EQ(lz4_kept.encoding.compressor, Compressor::kLz4) << lz4_kept.encoding.name();
}

TEST(ColumnCodec, AHintIsWrittenInUnlessTheOptionsLeaveItsCompressorOut) {
    // A seal writes in what the column's last segment chose, without a search - whatever the search
    // would choose - unless the options leave that encoding's compressor out. Steps allowed here; a
    // hint of them where they are not is StepsAreTriedOnlyWhereTheOptionsAskForThem's.
    std::vector<uint64_t> column;
    for (uint64_t i = 0; i < 1000; ++i) column.push_back(1000 + (i * 7) % 300);
    for (const Encoding& e : all_encodings()) {
        SCOPED_TRACE(e.name());
        EncodeOptions options;
        options.steps = true;
        options.hint = &e;
        std::string block;
        const Choice c = encode(column, options, block);
        EXPECT_EQ(c.encoding, e);
        EXPECT_EQ(c.bytes, block.size());
        std::vector<uint64_t> back;
        std::string why;
        ASSERT_TRUE(decode(bytes(block), column.size(), back, &why)) << why;
        EXPECT_EQ(back, column);
    }
    // Left out, the hint is searched for instead: the block is the one the same options write
    // without it.
    const Encoding zstd_runs{Transform::kFor, Packing::kRuns, Compressor::kZstd};
    const Encoding lz4_runs{Transform::kFor, Packing::kRuns, Compressor::kLz4};
    EncodeOptions no_zstd;
    no_zstd.zstd_level = 0;
    EncodeOptions no_lz4;
    no_lz4.lz4 = false;
    for (const auto& [options, hint] : {std::pair{no_zstd, &zstd_runs}, std::pair{no_lz4, &lz4_runs}}) {
        SCOPED_TRACE(hint->name());
        std::string searched, hinted;
        const Choice s = encode(column, options, searched);
        EncodeOptions with_hint = options;
        with_hint.hint = hint;
        const Choice h = encode(column, with_hint, hinted);
        EXPECT_NE(h.encoding.compressor, hint->compressor) << h.encoding.name();
        EXPECT_EQ(h.encoding, s.encoding) << h.encoding.name();
        EXPECT_EQ(hinted, searched);
    }
}

TEST(ColumnCodec, ADivisorTakesTheTickOutOfAPrice) {
    // The same steps, once in ticks and once at a tick of 10^6: the block differs by the divisor's
    // own bytes, not by twenty bits a value.
    std::vector<uint64_t> ticks, scaled;
    uint64_t p = 6'000'000;
    for (int i = 0; i < 5'000; ++i) {
        p += static_cast<uint64_t>((i * 31) % 7) - 3;
        ticks.push_back(p);
        scaled.push_back(p * 1'000'000);
    }
    std::string a, b;
    EncodeOptions uncompressed;
    uncompressed.lz4 = false;
    uncompressed.zstd_level = 0;
    const Choice in_ticks = encode(ticks, uncompressed, a);
    const Choice in_units = encode(scaled, uncompressed, b);
    EXPECT_LE(in_units.bytes, in_ticks.bytes + 8) << in_ticks.encoding.name() << " " << in_units.encoding.name();
}

TEST(ColumnCodec, ANarrowColumnTypeTakesItsValuesAndRefusesOthers) {
    std::vector<uint64_t> levels;
    for (uint64_t i = 0; i < 400; ++i) levels.push_back(i % 20);
    std::string block;
    encode(levels, EncodeOptions{}, block);
    std::vector<uint16_t> as16;
    std::vector<uint8_t> as8;
    std::string why;
    ASSERT_TRUE(decode_as(bytes(block), levels.size(), as16, &why)) << why;
    ASSERT_TRUE(decode_as(bytes(block), levels.size(), as8, &why)) << why;
    for (size_t i = 0; i < levels.size(); ++i) ASSERT_EQ(as16[i], levels[i]);

    levels.push_back(256);
    block.clear();
    encode(levels, EncodeOptions{}, block);
    EXPECT_FALSE(decode_as(bytes(block), levels.size(), as8, &why));
    EXPECT_NE(why.find("type"), std::string::npos) << why;
    EXPECT_TRUE(decode_as(bytes(block), levels.size(), as16, &why)) << why;

    // A signed column comes back as it went in, INT64_MIN included.
    const std::vector<int64_t> seq = {std::numeric_limits<int64_t>::min(), -1, 0, 1,
                                      std::numeric_limits<int64_t>::max()};
    std::vector<uint64_t> as_unsigned(seq.begin(), seq.end());
    block.clear();
    encode(as_unsigned, EncodeOptions{}, block);
    std::vector<int64_t> back;
    ASSERT_TRUE(decode_as(bytes(block), seq.size(), back, &why)) << why;
    EXPECT_EQ(back, seq);
}

TEST(ColumnCodec, ABlockThatIsNotWhatTheEncoderWritesIsRefused) {
    std::vector<uint64_t> column;
    for (uint64_t i = 0; i < 1000; ++i) column.push_back(1000 + (i * 7) % 300);
    std::vector<uint64_t> out;
    std::string why;
    for (const Encoding& e : all_encodings()) {
        std::string block;
        encode_as(column, e, 3, block);
        SCOPED_TRACE(e.name());
        // The count the block holds, and no other.
        EXPECT_FALSE(decode(bytes(block), column.size() + 1, out, &why));
        EXPECT_FALSE(decode(bytes(block), column.size() - 1, out, &why));
        // Cut anywhere, it is refused - never read past, never decoded short.
        for (size_t cut : {size_t{0}, size_t{1}, size_t{2}, block.size() / 2, block.size() - 1}) {
            EXPECT_FALSE(decode(std::span<const char>(block.data(), cut), column.size(), out, &why))
                << "cut at " << cut;
        }
        // A byte more, and it is not the block either.
        std::string longer = block + '\0';
        EXPECT_FALSE(decode(bytes(longer), column.size(), out, &why));
    }
    // Encoding bits no encoder writes.
    std::string block;
    encode_as(column, Encoding{Transform::kFor, Packing::kSimple8b, Compressor::kNone}, 3, block);
    // ZSTD and LZ4 at once, bit 7.
    for (uint8_t bad : {uint8_t{0x60}, uint8_t{0x80}}) {
        std::string b = block;
        b[0] = static_cast<char>(static_cast<uint8_t>(b[0]) | bad);
        EXPECT_FALSE(decode(bytes(b), column.size(), out, &why)) << int(bad);
    }
    // Steps are runs only (#250): transform 3 on a block of each other packing is refused by name,
    // and no encoder writes one.
    for (Packing p : {Packing::kSimple8b, Packing::kBlocks, Packing::kNarrow}) {
        std::string b;
        encode_as(column, Encoding{Transform::kFor, p, Compressor::kNone}, 3, b);
        b[0] = static_cast<char>(static_cast<uint8_t>(b[0]) | 0x03);
        EXPECT_FALSE(decode(bytes(b), column.size(), out, &why)) << int(p);
        EXPECT_NE(why.find("steps only as runs"), std::string::npos) << why;
        EXPECT_THROW(encode_as(column, Encoding{Transform::kSteps, p, Compressor::kNone}, 3, b),
                     std::invalid_argument);
    }
    // LZ4's bit on a ZSTD block and ZSTD's on an LZ4 one: a block is under one compressor, and one
    // claiming both is refused even where its payload decompresses under one of them.
    for (const auto& [compressor, other] :
         {std::pair{Compressor::kZstd, uint8_t{0x40}}, std::pair{Compressor::kLz4, uint8_t{0x20}}}) {
        std::string b;
        encode_as(column, Encoding{Transform::kFor, Packing::kRuns, compressor}, 3, b);
        ASSERT_TRUE(decode(bytes(b), column.size(), out, &why)) << why;
        b[0] = static_cast<char>(static_cast<uint8_t>(b[0]) | other);
        EXPECT_FALSE(decode(bytes(b), column.size(), out, &why)) << int(other);
    }
    // A narrow width outside 1 - 8.
    std::string narrow;
    encode_as(column, Encoding{Transform::kFor, Packing::kNarrow, Compressor::kNone}, 3, narrow);
    for (char w : {char{0}, char{9}}) {
        std::string b = narrow;
        b[1] = w;
        EXPECT_FALSE(decode(bytes(b), column.size(), out, &why)) << int(w);
    }
}

TEST(ColumnCodec, ACompressedBlockMustDecompressToTheLengthItDeclares) {
    // A frame that is whole and sound, under a header declaring one byte more or one less than it
    // holds: the declared length is what the unpacking is sized by, so it has to be the frame's.
    std::vector<uint64_t> column;
    for (uint64_t i = 0; i < 2000; ++i) column.push_back(5'000 + i % 37);
    for (const Compressor compressor : {Compressor::kZstd, Compressor::kLz4}) {
        SCOPED_TRACE(static_cast<int>(compressor));
        std::string block;
        encode_as(column, Encoding{Transform::kFor, Packing::kNarrow, compressor}, 3, block);
        // The header: encoding byte, narrow width, then the count, the anchor and the packed length
        // as varints - the scale is absent, since the values share no divisor past 1.
        size_t at = 2;
        uint64_t count = 0, anchor = 0, packed = 0;
        ASSERT_TRUE(get_varint(bytes(block), at, count));
        ASSERT_TRUE(get_varint(bytes(block), at, anchor));
        const size_t length_at = at;
        ASSERT_TRUE(get_varint(bytes(block), at, packed));
        std::vector<uint64_t> out;
        std::string why;
        ASSERT_TRUE(decode(bytes(block), column.size(), out, &why)) << why;
        ASSERT_EQ(out, column);
        for (const uint64_t declared : {packed - 1, packed + 1}) {
            std::string tail;
            put_varint(tail, declared);
            std::string changed = block.substr(0, length_at) + tail + block.substr(at);
            EXPECT_FALSE(decode(bytes(changed), column.size(), out, &why)) << "declared " << declared;
        }
    }
}

TEST(ColumnCodec, ADecodeInAKeptScratchGivesWhatADecodeOfItsOwnDoes) {
    // One scratch through every encoding of every edge column, larger after smaller and the other
    // way round: what an earlier decode left in it never reaches a later one's values.
    DecodeScratch scratch;
    for (const auto& column : edge_columns()) {
        for (const Encoding& e : all_encodings()) {
            SCOPED_TRACE(e.name());
            std::string block;
            encode_as(column, e, 3, block);
            std::vector<uint64_t> own, kept;
            std::string why;
            ASSERT_TRUE(decode(bytes(block), column.size(), own, &why)) << why;
            ASSERT_TRUE(decode_as(bytes(block), column.size(), kept, scratch, &why)) << why;
            EXPECT_EQ(kept, own);
        }
    }
    EXPECT_GT(scratch.held_bytes(), 0u) << "the scratch keeps what it grew to, for the caller to count";
}

/// decode_into() over `column`'s block, into memory filled with 0xAB first: the return, and the
/// values when it decoded, must be decode_as()'s into a vector of the same type.
template <typename T>
void expect_into_as_as(std::span<const char> block, size_t count, const std::string& name) {
    DecodeScratch a, b;
    std::vector<T> as;
    std::vector<T> into(count + 1);
    std::memset(into.data(), 0xAB, into.size() * sizeof(T));
    std::string why_as, why_into;
    const bool ok_as = decode_as(block, count, as, a, &why_as);
    const bool ok_into = decode_into(block, count, into.data(), b, &why_into);
    ASSERT_EQ(ok_into, ok_as) << name << ": " << why_as << " / " << why_into;
    if (!ok_as) return;
    EXPECT_TRUE(std::equal(as.begin(), as.end(), into.begin())) << name;
    T guard;
    std::memset(&guard, 0xAB, sizeof(T));
    EXPECT_EQ(into[count], guard) << name << ": a value written past `count`";
}

TEST(ColumnCodec, ADecodeIntoTheCallersMemoryGivesWhatDecodeAsDoes) {
    // #223: a held column is decoded into a slot of the store's arena, not into a vector.
    for (const auto& column : edge_columns()) {
        for (const Encoding& e : all_encodings()) {
            std::string block;
            encode_as(column, e, 3, block);
            const std::string name = e.name() + " on " + std::to_string(column.size()) + " values";
            expect_into_as_as<uint64_t>(bytes(block), column.size(), name);
            expect_into_as_as<int64_t>(bytes(block), column.size(), name);
            expect_into_as_as<uint32_t>(bytes(block), column.size(), name);
            expect_into_as_as<uint16_t>(bytes(block), column.size(), name);
            expect_into_as_as<uint8_t>(bytes(block), column.size(), name);
        }
    }
}

TEST(ColumnCodec, ADecodeIntoWritesNothingForABlockWhoseHeaderOrPayloadFails) {
    std::vector<uint64_t> column(500);
    for (size_t i = 0; i < column.size(); ++i) column[i] = i * 3;
    std::string block;
    encode_as(column, Encoding{Transform::kDelta, Packing::kNarrow, Compressor::kNone}, 3, block);
    DecodeScratch scratch;
    std::string why;
    std::vector<uint64_t> into(column.size(), 0xABABABABABABABABULL);
    const auto untouched = into;
    // A count the block does not hold, and a block cut inside its payload.
    EXPECT_FALSE(decode_into(bytes(block), column.size() + 1, into.data(), scratch, &why));
    EXPECT_EQ(into, untouched) << "written before the header was checked: " << why;
    const std::string cut = block.substr(0, block.size() - 9);
    EXPECT_FALSE(decode_into(bytes(cut), column.size(), into.data(), scratch, &why));
    EXPECT_EQ(into, untouched) << "written before the payload was checked: " << why;
    ASSERT_TRUE(decode_into(bytes(block), column.size(), into.data(), scratch, &why)) << why;
    EXPECT_EQ(into, column) << "the control: the whole block decodes";
}

TEST(ColumnCodec, EveryNarrowWidthGivesItsValuesBack) {
    // #223: each width from one byte to eight is loaded its own way. Every one is written with its
    // highest value, its top bit alone, small values and values just under the highest.
    for (unsigned w = 1; w <= 8; ++w) {
        const uint64_t top = w == 8 ? kMax : (uint64_t{1} << (8 * w)) - 1;
        std::vector<uint64_t> column;
        for (size_t i = 0; i < 4000; ++i) {
            switch (i % 4) {
            case 0:  column.push_back(top); break;
            case 1:  column.push_back(i % 251); break;
            case 2:  column.push_back(top - i % 200); break;
            default: column.push_back(uint64_t{1} << (8 * w - 1)); break;
            }
        }
        std::string block;
        encode_as(column, Encoding{Transform::kNone, Packing::kNarrow, Compressor::kNone}, 3, block);
        ASSERT_EQ(block.size() / column.size(), w) << "the control: the column is " << w << " bytes a value";
        std::vector<uint64_t> back;
        std::string why;
        ASSERT_TRUE(decode(bytes(block), column.size(), back, &why)) << w << ": " << why;
        EXPECT_EQ(back, column) << w << " bytes a value";
    }
}

namespace {

/// decode_into() of `block` as T into memory with sixteen guard values past `column`'s end: true when
/// it decodes the values `column` holds and leaves every guard as it was. Sixteen, because a short run
/// is written as eight.
template <typename T>
bool decodes_into_with_guards(const std::string& block, const std::vector<uint64_t>& column) {
    DecodeScratch scratch;
    std::vector<T> into(column.size() + 16);
    std::memset(into.data(), 0xAB, into.size() * sizeof(T));
    T guard;
    std::memset(&guard, 0xAB, sizeof(T));
    std::string why;
    if (!decode_into(bytes(block), column.size(), into.data(), scratch, &why)) return false;
    for (size_t i = 0; i < column.size(); ++i) {
        if (into[i] != static_cast<T>(column[i])) return false;
    }
    for (size_t i = column.size(); i < into.size(); ++i) {
        if (into[i] != guard) return false;
    }
    return true;
}

}  // namespace

RC_GTEST_PROP(ColumnCodec, RunsOfEveryLengthDecodeIntoEveryColumnType, ()) {
    // #223: where runs average two values or more - past eight, where their lengths follow no pattern -
    // a run of at most eight values, with room for eight before the column ends, is written as eight
    // and the next run writes over the rest (short_runs_in_blocks()). Runs of one
    // value up to a longest drawn from one to twenty, so that both ways of writing a column, both ways
    // of writing a run and the column's last slots are reached, in columns every type holds.
    const auto longest = *rc::gen::inRange<size_t>(1, 21);
    const auto lengths =
        *rc::gen::nonEmpty(rc::gen::container<std::vector<size_t>>(rc::gen::inRange<size_t>(1, longest + 1)));
    const auto steps = *rc::gen::container<std::vector<int>>(lengths.size(), rc::gen::inRange(-3, 4));
    const auto levels =
        *rc::gen::container<std::vector<uint64_t>>(lengths.size(), rc::gen::inRange<uint64_t>(0, 256));
    // The delta-shaped column steps by one difference a run, from 128, kept inside 0 - 255; the
    // value-shaped one holds one value a run.
    std::vector<uint64_t> stepping{128};
    std::vector<uint64_t> holding;
    int64_t v = 128;
    for (size_t r = 0; r < lengths.size(); ++r) {
        const int64_t end = v + static_cast<int64_t>(lengths[r]) * steps[r];
        const int64_t step = end < 0 || end > 255 ? 0 : steps[r];
        for (size_t k = 0; k < lengths[r]; ++k) stepping.push_back(static_cast<uint64_t>(v += step));
        holding.insert(holding.end(), lengths[r], levels[r]);
    }
    for (const auto& [column, encoding] :
         {std::pair{stepping, Encoding{Transform::kDelta, Packing::kRuns, Compressor::kNone}},
          std::pair{stepping, Encoding{Transform::kDelta, Packing::kRuns, Compressor::kLz4}},
          std::pair{holding, Encoding{Transform::kFor, Packing::kRuns, Compressor::kNone}},
          std::pair{holding, Encoding{Transform::kNone, Packing::kRuns, Compressor::kLz4}},
          std::pair{holding, Encoding{Transform::kSteps, Packing::kRuns, Compressor::kNone}},
          std::pair{holding, Encoding{Transform::kSteps, Packing::kRuns, Compressor::kLz4}}}) {
        std::string block;
        encode_as(column, encoding, 1, block);
        RC_ASSERT(decodes_into_with_guards<uint8_t>(block, column));
        RC_ASSERT(decodes_into_with_guards<uint16_t>(block, column));
        RC_ASSERT(decodes_into_with_guards<uint32_t>(block, column));
        RC_ASSERT(decodes_into_with_guards<uint64_t>(block, column));
        RC_ASSERT(decodes_into_with_guards<int64_t>(block, column));
    }
}

TEST(ColumnCodec, AShortRunNearTheColumnsEndWritesNothingPastIt) {
    // #223: a short run is written as a block only where the whole block fits before the column ends.
    // Columns of every length from 1 to 40, of runs two and three values long in turn - long enough on
    // average to be written in blocks - so that a run starts at every distance from the end.
    for (size_t length = 1; length <= 40; ++length) {
        std::vector<uint64_t> stepping{0};
        std::vector<uint64_t> holding;
        for (size_t r = 0; holding.size() < length; ++r) {
            const size_t run = r % 2 == 0 ? 2 : 3;
            for (size_t k = 0; k < run; ++k) {
                if (stepping.size() < length) stepping.push_back(stepping.back() + (r % 2 == 0 ? 1 : 2));
                if (holding.size() < length) holding.push_back(r % 2 == 0 ? 7 : 9);
            }
        }
        for (const auto& [column, encoding] :
             {std::pair{stepping, Encoding{Transform::kDelta, Packing::kRuns, Compressor::kNone}},
              std::pair{holding, Encoding{Transform::kNone, Packing::kRuns, Compressor::kNone}},
              std::pair{holding, Encoding{Transform::kSteps, Packing::kRuns, Compressor::kNone}}}) {
            std::string block;
            encode_as(column, encoding, 1, block);
            EXPECT_TRUE(decodes_into_with_guards<uint8_t>(block, column)) << encoding.name() << ", " << length;
            EXPECT_TRUE(decodes_into_with_guards<uint16_t>(block, column)) << encoding.name() << ", " << length;
            EXPECT_TRUE(decodes_into_with_guards<uint64_t>(block, column)) << encoding.name() << ", " << length;
        }
    }
}

TEST(ColumnCodec, RunsThatDoNotCoverTheValuesExactlyAreRefused) {
    // #223: whether the runs cover the values is two sums now rather than a running total. A run
    // longer than all the values together is refused first: past that, a length could wrap the sum
    // round to exactly the values' count.
    const std::vector<uint64_t> column(10, 6);
    std::string block;
    encode_as(column, Encoding{Transform::kNone, Packing::kRuns, Compressor::kNone}, 1, block);
    // The header is the encoding byte and the count; none has no anchor.
    size_t at = 1;
    uint64_t count = 0;
    ASSERT_TRUE(get_varint(bytes(block), at, count));
    ASSERT_EQ(count, column.size());
    const std::string header = block.substr(0, at);
    // A runs payload: how many runs and how many words their values take, then the values' words and
    // the lengths' words - each length stored as one less than the run.
    const auto with_runs = [&](const std::vector<uint64_t>& values, const std::vector<uint64_t>& lengths) {
        const auto value_words = ob::encode_simple8b(values).words;
        const auto length_words = ob::encode_simple8b(lengths).words;
        std::string b = header;
        put_varint(b, values.size());
        put_varint(b, value_words.size());
        b.append(reinterpret_cast<const char*>(value_words.data()), value_words.size() * sizeof(uint64_t));
        b.append(reinterpret_cast<const char*>(length_words.data()), length_words.size() * sizeof(uint64_t));
        return b;
    };
    std::vector<uint64_t> out;
    std::string why;
    ASSERT_TRUE(decode(bytes(with_runs({5, 6}, {2, 6})), 10, out, &why)) << "the control: " << why;
    EXPECT_EQ(out, (std::vector<uint64_t>{5, 5, 5, 6, 6, 6, 6, 6, 6, 6}));
    EXPECT_FALSE(decode(bytes(with_runs({5, 6}, {4, 9})), 10, out, &why)) << "fifteen values";
    EXPECT_FALSE(decode(bytes(with_runs({5, 6}, {2, 3})), 10, out, &why)) << "seven values";
    EXPECT_FALSE(decode(bytes(with_runs({5, 6}, {~uint64_t{0}, 9})), 10, out, &why)) << "a sum wrapped to ten";
}

TEST(ColumnCodec, AShortRunIsRefusedWhereverItsValuesLeaveTheType) {
    // #223: a short run is checked at its two ends, which bound it. Each column is a short run and a
    // run of twelve after it, so that the short one is written as a block, and is decoded as 8-bit.
    const Encoding delta_runs{Transform::kDelta, Packing::kRuns, Compressor::kNone};
    const auto refused_as_u8 = [&](std::vector<uint64_t> column) {
        const uint64_t last = column.back();
        column.insert(column.end(), 12, last);   // the run of twelve: a step of zero
        std::string block;
        encode_as(column, delta_runs, 1, block);
        std::vector<uint8_t> as8;
        std::vector<uint64_t> as64;
        std::string why;
        EXPECT_TRUE(decode_as(bytes(block), column.size(), as64, &why)) << "the control: " << why;
        EXPECT_EQ(as64, column);
        return !decode_as(bytes(block), column.size(), as8, &why) && why.find("type") != std::string::npos;
    };
    // Its last value past 255, and the next run, a short one too, back inside it: 0, 100, 200, 300,
    // then 200, 100, 0. Only the first run's last value is outside, so only its check can see it.
    EXPECT_TRUE(refused_as_u8({0, 100, 200, 300, 200, 100, 0}));
    // Its first value past 255, and its last back at 0: a step of 2^63, twice.
    EXPECT_TRUE(refused_as_u8({0, uint64_t{1} << 63, 0}));
    // The control: in range at both ends, and so throughout.
    EXPECT_FALSE(refused_as_u8({55, 155, 255}));

    // The values a block writes past a short run's end are the next run's to write, and are not
    // checked: 0, 100, 200 is a run of two whose block reaches 800, then twelve values down by one.
    std::vector<uint64_t> column{0, 100, 200};
    for (uint64_t k = 1; k <= 12; ++k) column.push_back(200 - k);
    std::string block;
    encode_as(column, delta_runs, 1, block);
    std::vector<uint8_t> as8;
    std::string why;
    ASSERT_TRUE(decode_as(bytes(block), column.size(), as8, &why)) << why;
    EXPECT_TRUE(std::equal(column.begin(), column.end(), as8.begin()));
}

TEST(ColumnCodec, ALongRunIsWrittenInBlocksAndNothingPastTheColumnsEnd) {
    // #223: a run longer than a block is written a block at a time where its whole blocks fit before
    // the column ends, and value by value where they do not. Two short runs, a long one of every length
    // from 9 to 40, then none to five short runs, so that the long run ends at every distance from the
    // column's end - in columns whose runs average a block or less and more than one, both ways the
    // short runs are written - stepping and holding, decoded into every column type.
    for (size_t len = 9; len <= 40; ++len) {
        for (size_t tail = 0; tail <= 5; ++tail) {
            std::vector<uint64_t> stepping{0};
            std::vector<uint64_t> holding;
            const auto add_run = [&](size_t n, uint64_t step, uint64_t value) {
                for (size_t k = 0; k < n; ++k) {
                    stepping.push_back(stepping.back() + step);
                    holding.push_back(value);
                }
            };
            add_run(2, 1, 7);
            add_run(3, 2, 9);
            add_run(len, 3, 11);
            for (size_t t = 0; t < tail; ++t) add_run(t % 2 == 0 ? 2 : 1, t % 2 == 0 ? 1 : 0, 13 + t);
            // The stepping column's anchor comes before its runs, which are of the differences.
            for (const auto& [column, encoding] :
                 {std::pair{stepping, Encoding{Transform::kDelta, Packing::kRuns, Compressor::kNone}},
                  std::pair{holding, Encoding{Transform::kNone, Packing::kRuns, Compressor::kNone}},
                  std::pair{holding, Encoding{Transform::kSteps, Packing::kRuns, Compressor::kNone}}}) {
                std::string block;
                encode_as(column, encoding, 1, block);
                const std::string what = encoding.name() + ", a run of " + std::to_string(len) + " and " +
                                         std::to_string(tail) + " after it";
                EXPECT_TRUE(decodes_into_with_guards<uint8_t>(block, column)) << what;
                EXPECT_TRUE(decodes_into_with_guards<uint16_t>(block, column)) << what;
                EXPECT_TRUE(decodes_into_with_guards<uint32_t>(block, column)) << what;
                EXPECT_TRUE(decodes_into_with_guards<uint64_t>(block, column)) << what;
                EXPECT_TRUE(decodes_into_with_guards<int64_t>(block, column)) << what;
            }
        }
    }
}

TEST(ColumnCodec, ShortRunsAreWrittenInBlocksUnlessTheirLengthsRepeat) {
    // #223, short_runs_in_blocks(): runs a value long or about - a book's prices on a recording of its
    // changes - value by value; from two values a run, blocks; past a block a run, blocks unless the
    // lengths repeat, as a book's snapshots' do. The lengths are stored less one.
    const auto in_blocks = [](const std::vector<uint64_t>& lengths) {
        size_t values = 0;
        for (const uint64_t l : lengths) values += static_cast<size_t>(l) + 1;
        return short_runs_in_blocks(lengths, values);
    };
    const auto repeating = [](const std::vector<uint64_t>& pattern, size_t runs) {
        std::vector<uint64_t> lengths;
        for (size_t r = 0; r < runs; ++r) lengths.push_back(pattern[r % pattern.size()] - 1);
        return lengths;
    };
    const auto scattered = [](uint64_t longest, size_t runs) {   // from 1 to `longest`, in no pattern
        std::vector<uint64_t> lengths;
        uint64_t x = 12345;
        for (size_t r = 0; r < runs; ++r) {
            x = x * 6364136223846793005ULL + 1442695040888963407ULL;
            lengths.push_back((x >> 33) % longest);
        }
        return lengths;
    };
    EXPECT_FALSE(in_blocks(repeating({1, 1, 1, 2}, 1000))) << "a value and a quarter a run";
    EXPECT_TRUE(in_blocks(repeating({2}, 1000))) << "two values a run";
    EXPECT_TRUE(in_blocks(scattered(6, 1000))) << "three and a half a run, in no pattern";
    EXPECT_TRUE(in_blocks(repeating({8}, 1000))) << "a block a run: no pattern is asked for";
    EXPECT_FALSE(in_blocks(repeating({40}, 1000))) << "a book's snapshots' timestamps";
    EXPECT_FALSE(in_blocks(repeating({19, 1}, 1000))) << "their levels' differences";
    EXPECT_FALSE(in_blocks(repeating({9}, 1000))) << "a value past a block a run, repeating";
    EXPECT_TRUE(in_blocks(scattered(30, 1000))) << "fifteen and a half a run, in no pattern";
    EXPECT_TRUE(in_blocks(repeating({40}, 2))) << "two runs: nothing to judge a pattern on";
    // One run in four as long as the one before it is no pattern.
    std::vector<uint64_t> some = scattered(30, 1000);
    for (size_t r = 4; r < some.size(); r += 4) some[r] = some[r - 1];
    EXPECT_TRUE(in_blocks(some)) << "a quarter repeating";
}

TEST(ColumnCodec, RunsAveragingMoreThanABlockInNoPatternDecodeAsWritten) {
    // #223: where runs average more than a block and their lengths follow no pattern, the short ones
    // are written a block at a time too, each block's slots past the run the runs after it overwrite,
    // and the column's last runs value by value where a block does not fit. Two hundred runs of one to
    // thirty values in no pattern, then none to nine short ones at the column's end - holding a value,
    // and stepping by one or two, which keeps the column inside 0 - 255 - decoded into every column
    // type with guards past its end.
    for (size_t tail = 0; tail <= 9; ++tail) {
        std::vector<uint64_t> holding;
        std::vector<uint64_t> stepping{128};
        std::vector<uint64_t> lengths;   // less one, as a block stores them: the holding column's runs,
                                         // and the stepping column's runs of differences
        uint64_t x = 777 + tail;
        const auto next = [&x] {
            x = x * 6364136223846793005ULL + 1442695040888963407ULL;
            return x >> 33;
        };
        int64_t step = 0;
        const auto add_run = [&](size_t n) {
            uint64_t value = next() % 256;
            if (!holding.empty() && value == holding.back()) value = (value + 1) % 256;
            // Toward 128, and never the run before's step, so that each run stays its own.
            const int64_t toward = static_cast<int64_t>(stepping.back()) > 128 ? -1 : 1;
            step = (step == toward) ? 2 * toward : toward;
            for (size_t k = 0; k < n; ++k) {
                holding.push_back(value);
                stepping.push_back(static_cast<uint64_t>(static_cast<int64_t>(stepping.back()) + step));
            }
            lengths.push_back(n - 1);
        };
        for (size_t r = 0; r < 200; ++r) add_run(1 + next() % 30);
        for (size_t t = 0; t < tail; ++t) add_run(1 + t % 3);
        ASSERT_TRUE(short_runs_in_blocks(lengths, holding.size())) << "these runs would not be written in blocks";
        ASSERT_TRUE(std::all_of(stepping.begin(), stepping.end(), [](uint64_t v) { return v < 256; }));
        for (const auto& [column, encoding] :
             {std::pair{stepping, Encoding{Transform::kDelta, Packing::kRuns, Compressor::kNone}},
              std::pair{holding, Encoding{Transform::kNone, Packing::kRuns, Compressor::kNone}},
              std::pair{holding, Encoding{Transform::kFor, Packing::kRuns, Compressor::kNone}},
              std::pair{holding, Encoding{Transform::kSteps, Packing::kRuns, Compressor::kNone}}}) {
            std::string block;
            encode_as(column, encoding, 1, block);
            const std::string what = encoding.name() + ", " + std::to_string(tail) + " short runs at the end";
            EXPECT_TRUE(decodes_into_with_guards<uint8_t>(block, column)) << what;
            EXPECT_TRUE(decodes_into_with_guards<uint16_t>(block, column)) << what;
            EXPECT_TRUE(decodes_into_with_guards<uint32_t>(block, column)) << what;
            EXPECT_TRUE(decodes_into_with_guards<uint64_t>(block, column)) << what;
            EXPECT_TRUE(decodes_into_with_guards<int64_t>(block, column)) << what;
        }
    }
}

TEST(ColumnCodec, ALongRunIsRefusedWhereverItsValuesLeaveTheType) {
    // #223: a run longer than a block, written in blocks, is checked at its two ends as a short one is.
    // Each column is an anchor, a run of twenty from it by one step - with room for its three blocks
    // before the column ends - and a run of twelve by another, which brings the values back inside
    // 0 - 255, decoded as 8-bit: only the run of twenty's checks can see a value outside.
    const Encoding delta_runs{Transform::kDelta, Packing::kRuns, Compressor::kNone};
    const auto refused_as_u8 = [&](uint64_t start, uint64_t step, uint64_t back) {
        std::vector<uint64_t> column{start};
        for (int k = 0; k < 20; ++k) column.push_back(column.back() + step);
        for (int k = 0; k < 12; ++k) column.push_back(column.back() + back);
        std::string block;
        encode_as(column, delta_runs, 1, block);
        std::vector<uint8_t> as8;
        std::vector<uint64_t> as64;
        std::string why;
        EXPECT_TRUE(decode_as(bytes(block), column.size(), as64, &why)) << "the control: " << why;
        EXPECT_EQ(as64, column);
        return !decode_as(bytes(block), column.size(), as8, &why) && why.find("type") != std::string::npos;
    };
    EXPECT_TRUE(refused_as_u8(200, 3, uint64_t{0} - 12)) << "up past 255: its last value 260, then 248 down to 116";
    EXPECT_TRUE(refused_as_u8(50, uint64_t{0} - 3, 12)) << "down past 0: its last value -10, then 2 up to 134";
    EXPECT_TRUE(refused_as_u8(250, uint64_t{1} << 63, 0))
        << "its first value past 255 and its last 250 again: a step of 2^63, twenty times";
    EXPECT_FALSE(refused_as_u8(200, 2, uint64_t{0} - 12)) << "the control: 202 up to 240, then down to 96";
}

TEST(ColumnCodec, AStepsRunIsRefusedWhereverItsValueLeavesTheType) {
    // #250: a run under steps holds one value, the anchor and every step before it added up, and is
    // checked once. Columns of runs three values long, decoded as 8-bit: a run past 255 between two
    // inside, a run below 0 between two inside, and a step of 2^63 there and back.
    const Encoding steps{Transform::kSteps, Packing::kRuns, Compressor::kNone};
    const auto refused_as_u8 = [&](const std::vector<uint64_t>& run_values) {
        std::vector<uint64_t> column;
        for (uint64_t v : run_values) column.insert(column.end(), 3, v);
        std::string block;
        encode_as(column, steps, 1, block);
        std::vector<uint8_t> as8;
        std::vector<uint64_t> as64;
        std::string why;
        EXPECT_TRUE(decode_as(bytes(block), column.size(), as64, &why)) << "the control: " << why;
        EXPECT_EQ(as64, column);
        return !decode_as(bytes(block), column.size(), as8, &why) && why.find("type") != std::string::npos;
    };
    EXPECT_TRUE(refused_as_u8({100, 200, 300, 200, 100})) << "a run at 300";
    EXPECT_TRUE(refused_as_u8({5, uint64_t{0} - 5, 5})) << "a run at -5";
    EXPECT_TRUE(refused_as_u8({250, 250 + (uint64_t{1} << 63), 250})) << "a step of 2^63 there and back";
    EXPECT_FALSE(refused_as_u8({0, 128, 255, 7})) << "the control: every run inside";
}

TEST(ColumnCodec, StepsAreTriedOnlyWhereTheOptionsAskForThem) {
    // #250: a segment of format 3 holds no block its readers cannot read. The timestamps of a book's
    // changes - an update's levels at one time, one to nineteen of them, the updates about 100 us
    // apart, a step far narrower than the segment's span - are steps' column: asked for, steps are
    // chosen there, as a seal searches, and take fewer bytes; left out, never, a hint of them included.
    std::vector<uint64_t> ts;
    uint64_t at = 1'790'000'000'000'000'000ULL;
    for (uint64_t u = 0; u < 1'000; ++u) {
        at += 100'000 + (u * 7919) % 5'000;
        ts.insert(ts.end(), 1 + (u * 37) % 19, at);
    }
    EncodeOptions without;
    without.zstd_level = 0;
    EncodeOptions with = without;
    with.steps = true;
    std::string a, b;
    const Choice off = encode(ts, without, a);
    const Choice on = encode(ts, with, b);
    EXPECT_NE(off.encoding.transform, Transform::kSteps) << off.encoding.name();
    EXPECT_EQ(on.encoding.transform, Transform::kSteps) << on.encoding.name();
    EXPECT_LT(b.size(), a.size()) << off.encoding.name() << " against " << on.encoding.name();
    // A hint of steps is searched past where steps are left out, and written in where they are not.
    const Encoding hint{Transform::kSteps, Packing::kRuns, Compressor::kLz4};
    without.hint = &hint;
    with.hint = &hint;
    std::string c, d;
    EXPECT_NE(encode(ts, without, c).encoding.transform, Transform::kSteps);
    EXPECT_EQ(encode(ts, with, d).encoding, hint);
    for (const std::string* block : {&a, &b, &c, &d}) {
        std::vector<uint64_t> back;
        std::string why;
        ASSERT_TRUE(decode(bytes(*block), ts.size(), back, &why)) << why;
        EXPECT_EQ(back, ts);
    }
}

TEST(ColumnCodec, AThreadThatUsedZstdLeavesNoContextBehind) {
    // A thread keeps one ZSTD context of each kind while it lives and frees them when it ends: a
    // migration's threads and an embedding application's come and go. Under the sanitizers job's
    // leak check, a context an ended thread kept is a leak reported at exit.
    std::vector<uint64_t> column;
    for (int s = 0; s < 200; ++s) {
        for (uint64_t l = 0; l < 20; ++l) column.push_back(100'000 + l * 7'919 % 5'003);
    }
    std::atomic<int> wrong{0};
    for (int round = 0; round < 4; ++round) {
        std::thread t([&] {
            std::string block;
            encode_as(column, Encoding{Transform::kDelta, Packing::kNarrow, Compressor::kZstd}, 3, block);
            std::vector<uint64_t> back;
            std::string why;
            if (!decode(bytes(block), column.size(), back, &why) || back != column) wrong.fetch_add(1);
        });
        t.join();
    }
    EXPECT_EQ(wrong.load(), 0);
}

TEST(ColumnCodec, VarintsGoBothWaysAndRefuseWhatIsNotOne) {
    for (uint64_t v : {uint64_t{0}, uint64_t{1}, uint64_t{127}, uint64_t{128}, uint64_t{300},
                       kInt64Min, kMax}) {
        std::string s;
        put_varint(s, v);
        size_t at = 0;
        uint64_t back = 0;
        ASSERT_TRUE(get_varint(bytes(s), at, back));
        EXPECT_EQ(back, v);
        EXPECT_EQ(at, s.size());
        // Cut short.
        at = 0;
        EXPECT_FALSE(get_varint(std::span<const char>(s.data(), s.size() - 1), at, back));
    }
    // Eleven bytes, or a tenth past bit 63: not a 64-bit varint.
    std::string eleven(10, static_cast<char>(0x80));
    eleven.push_back(0x01);
    size_t at = 0;
    uint64_t v = 0;
    EXPECT_FALSE(get_varint(bytes(eleven), at, v));
    std::string past(9, static_cast<char>(0xFF));
    past.push_back(0x02);
    at = 0;
    EXPECT_FALSE(get_varint(bytes(past), at, v));
}
