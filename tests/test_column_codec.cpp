// Column blocks of segment format 3 (kiro-workspace/specs/segment-format-v3/, design §2 and §3).
//
// Every encoding has to give back exactly the values it was given - for every value, not for the
// range prices happen to occupy - and a block that is not what the encoder writes has to be
// refused rather than decoded into other numbers.

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <atomic>
#include <cstdint>
#include <limits>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>

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
    for (const auto& column : edge_columns()) {
        std::string chosen;
        const Choice c = encode(column, EncodeOptions{}, chosen);
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
    // would choose - unless the options leave that encoding's compressor out.
    std::vector<uint64_t> column;
    for (uint64_t i = 0; i < 1000; ++i) column.push_back(1000 + (i * 7) % 300);
    for (const Encoding& e : all_encodings()) {
        SCOPED_TRACE(e.name());
        EncodeOptions options;
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
    // Transform 3, ZSTD and LZ4 at once, bit 7.
    for (uint8_t bad : {uint8_t{0x03}, uint8_t{0x60}, uint8_t{0x80}}) {
        std::string b = block;
        b[0] = static_cast<char>(static_cast<uint8_t>(b[0]) | bad);
        EXPECT_FALSE(decode(bytes(b), column.size(), out, &why)) << int(bad);
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
