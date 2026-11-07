// #257: a row answer measured against what its session may be sent while it is built.
//
// Since #152 an answer larger than a session may have queued is refused, and the session stays open
// - but the answer was built in full to be measured: a SELECT over 8 million rows formatted 367 MB of
// text, took the node's resident memory up by 912 MB on the m9g.xlarge and held its event loop for
// 225 ms, to be told no. The builder now knows the ceiling and stops at the batch or the part that
// passes it, and the scan stops with it; on a session that compresses, the answer is its LZ4 frame
// from the first row, and the frame is what is measured.
//
// Held here: an answer that fits is the bytes it was (the control), the batch that passes the
// ceiling makes the answer full and the rows before it are the ones within it, a part that would
// pass is not copied, an LZ4 answer is the plain one's text and is measured by its frame, and a scan
// stops where its answer became full - on one thread and on the pool.

#include "orderbook/aggregation.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/compression.hpp"
#include "orderbook/query_engine.hpp"
#include "orderbook/response_formatter.hpp"
#include "orderbook/scan_pool.hpp"
#include "orderbook/soa_buffer.hpp"

#include <gtest/gtest.h>

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

/// Columns of `n` rows, `repetitive` making them compress many times over: every twentieth line the
/// same as the one before it.
struct Rows {
    std::vector<uint64_t> ts, qty;
    std::vector<int64_t> price, seq;
    std::vector<uint32_t> cnt;
    std::vector<uint8_t> side;
    std::vector<uint16_t> level;
    std::vector<uint32_t> chosen;

    Rows(size_t n, bool repetitive)
        : ts(n), qty(n), price(n), seq(n), cnt(n), side(n), level(n), chosen(n) {
        for (size_t i = 0; i < n; ++i) {
            ts[i]     = 1'790'000'000'000'000'000ULL + (repetitive ? i % 20 : i * 1'000'003ULL);
            price[i]  = repetitive ? 6'500'000 : static_cast<int64_t>(i * 7919 % 9'999'991);
            qty[i]    = repetitive ? 150 : i * i % 1'000'003;
            cnt[i]    = repetitive ? 3 : static_cast<uint32_t>(i % 100);
            side[i]   = static_cast<uint8_t>(i % 2);
            level[i]  = static_cast<uint16_t>(i % 20);
            seq[i]    = static_cast<int64_t>(repetitive ? i % 20 : i);
            chosen[i] = static_cast<uint32_t>(i);
        }
    }

    ob::RowBatch batch(size_t at, size_t take) const {
        return ob::RowBatch{ts, price, qty, cnt, side, level, seq,
                            std::span<const uint32_t>(chosen.data() + at, take)};
    }
};

ob::QueryShape seven() {
    ob::QueryShape shape;
    shape.columns = ob::all_query_columns();
    return shape;
}

/// The rows added in batches of `batch` until the builder is full or the rows run out; the number
/// of batches it was given.
size_t add_in_batches(ob::QueryResponseBuilder& reply, const Rows& rows, size_t batch) {
    size_t given = 0;
    for (size_t at = 0; at < rows.chosen.size() && !reply.full(); at += batch) {
        reply.add_rows(rows.batch(at, std::min(batch, rows.chosen.size() - at)));
        ++given;
    }
    return given;
}

/// The text an LZ4 answer holds: its four-byte length checked against the frame, and the frame
/// decompressed.
std::string text_of_frame(const std::string& framed) {
    EXPECT_GE(framed.size(), 4u);
    const uint32_t length = (static_cast<uint32_t>(static_cast<uint8_t>(framed[0])) << 24) |
                            (static_cast<uint32_t>(static_cast<uint8_t>(framed[1])) << 16) |
                            (static_cast<uint32_t>(static_cast<uint8_t>(framed[2])) << 8) |
                            static_cast<uint32_t>(static_cast<uint8_t>(framed[3]));
    EXPECT_EQ(length, framed.size() - 4) << "the length in front is not the frame's";
    const auto text = ob::lz4_decompress(framed.data() + 4, framed.size() - 4);
    return std::string(text.begin(), text.end());
}

/// Where two long strings part, for a message: EXPECT_EQ on them diffs them line by line, and a
/// failing one took the laptop's memory (pitfall 637).
std::string parted(const std::string& a, const std::string& b) {
    const auto at = std::mismatch(a.begin(), a.end(), b.begin(), b.end());
    return std::to_string(a.size()) + " and " + std::to_string(b.size()) + " bytes part at byte " +
           std::to_string(at.first - a.begin());
}

}  // namespace

// The control: an answer that fits is the bytes an answer with no ceiling is, at the ceiling exactly
// - the answer and its terminator - and well under it, whatever the batches.
TEST(AnswerCeiling, AnAnswerThatFitsIsTheBytesOfOneWithNoCeiling) {
    const Rows rows(20'000, false);
    const ob::QueryShape shape = seven();
    ob::QueryResponseBuilder unbounded(shape);
    add_in_batches(unbounded, rows, 2048);
    const std::string expected = unbounded.finish();

    for (const size_t ceiling : {expected.size(), expected.size() * 2}) {
        for (const size_t batch : {size_t{1}, size_t{777}, size_t{2048}}) {
            ob::QueryResponseBuilder reply(shape, std::string(), ob::AnswerCeiling{ceiling, false});
            add_in_batches(reply, rows, batch);
            EXPECT_FALSE(reply.full()) << "ceiling " << ceiling << ", batches of " << batch;
            EXPECT_EQ(reply.rows_within(), rows.chosen.size());
            const std::string got = reply.finish();
            EXPECT_TRUE(got == expected) << parted(got, expected);
        }
    }
}

// One byte under the answer: full at the last batch, which is not within it, and the rows before
// that batch are.
TEST(AnswerCeiling, TheBatchThatPassesTheCeilingMakesTheAnswerFull) {
    const Rows rows(10'000, false);
    const ob::QueryShape shape = seven();
    ob::QueryResponseBuilder unbounded(shape);
    add_in_batches(unbounded, rows, 1000);
    const size_t whole = unbounded.finish().size();

    ob::QueryResponseBuilder reply(shape, std::string(), ob::AnswerCeiling{whole - 1, false});
    EXPECT_EQ(add_in_batches(reply, rows, 1000), 10u);
    EXPECT_TRUE(reply.full());
    EXPECT_EQ(reply.rows_taken(), 10'000u);
    EXPECT_EQ(reply.rows_within(), 9'000u) << "the rows before the batch that passed the ceiling";

    // A ceiling a third of the way in: full at the batch that passed it, and nothing taken after.
    ob::QueryResponseBuilder third(shape, std::string(), ob::AnswerCeiling{whole / 3, false});
    const size_t batches = add_in_batches(third, rows, 1000);
    EXPECT_TRUE(third.full());
    EXPECT_LT(batches, 10u) << "the builder was given batches after it was full";
    const size_t built = third.bytes_built();
    EXPECT_GT(built + 1, whole / 3) << "full before it passed its ceiling";
    third.add_rows(rows.batch(0, 1000));
    EXPECT_EQ(third.bytes_built(), built) << "a full answer took more rows";
    EXPECT_EQ(third.rows_taken(), batches * 1000);
    EXPECT_EQ(third.rows_within(), (batches - 1) * 1000);
}

// A part knows its bytes before it is copied: one that would pass the ceiling is not, and the answer
// is full with nothing of it; one that fits is the bytes the same rows added as a batch are.
TEST(AnswerCeiling, APartThatWouldPassTheCeilingIsNotCopied) {
    const Rows rows(3000, false);
    const ob::QueryShape shape = seven();
    ob::QueryResponseBuilder direct(shape);
    direct.add_rows(rows.batch(0, 3000));
    const std::string expected = direct.finish();

    ob::QueryResponseBuilder fits(shape, std::string(), ob::AnswerCeiling{expected.size(), false});
    auto part = fits.part(3000);
    part->take(rows.batch(0, 3000));
    fits.add_part(*part, 3000);
    EXPECT_FALSE(fits.full());
    const std::string got = fits.finish();
    EXPECT_TRUE(got == expected) << parted(got, expected);

    ob::QueryResponseBuilder over(shape, std::string(), ob::AnswerCeiling{expected.size() - 1, false});
    auto whole = over.part(3000);
    whole->take(rows.batch(0, 3000));
    over.add_part(*whole, 3000);
    EXPECT_TRUE(over.full());
    EXPECT_EQ(over.rows_taken(), 0u) << "the part that would pass the ceiling was copied";
    EXPECT_LT(over.bytes_built(), 100u) << "more than the header was built";
}

// On a session that compresses, a row answer is its LZ4 frame: the plain answer's text, its length
// in front - with no rows, past the megabyte the text is compressed at, and many times past it.
TEST(AnswerCeiling, AnLz4AnswerIsTheFrameOfThePlainOnesText) {
    const ob::QueryShape shape = seven();
    for (const size_t n : {size_t{0}, size_t{7}, size_t{30'000}, size_t{120'000}}) {
        const Rows rows(n, false);
        ob::QueryResponseBuilder plain(shape);
        add_in_batches(plain, rows, 2048);
        const std::string expected = plain.finish();

        ob::QueryResponseBuilder lz4(shape, std::string(), ob::AnswerCeiling{64u << 20, true});
        add_in_batches(lz4, rows, 2048);
        EXPECT_FALSE(lz4.full());
        if (n == 0) {
            // No row and no frame: the header and terminator, compressed by the session as before.
            EXPECT_FALSE(lz4.framed());
            EXPECT_EQ(lz4.finish(), expected);
            continue;
        }
        ASSERT_TRUE(lz4.framed()) << n << " rows";
        const std::string framed = lz4.finish();
        EXPECT_EQ(lz4.text_bytes(), expected.size()) << n << " rows";
        const std::string text = text_of_frame(framed);
        EXPECT_TRUE(text == expected) << n << " rows: " << parted(text, expected);
    }
}

// The frame is what has to fit, so text past the ceiling whose frame is under it is an answer; and a
// frame past the ceiling is full, its rows within it those compressed while it still fitted.
TEST(AnswerCeiling, AnLz4AnswerIsMeasuredByItsFrameNotItsText) {
    const ob::QueryShape shape = seven();
    const Rows rows(200'000, true);   // about 8 MB of text that compresses many times over
    ob::QueryResponseBuilder plain(shape);
    add_in_batches(plain, rows, 2048);
    const std::string expected = plain.finish();
    const size_t ceiling = 2u << 20;
    ASSERT_GT(expected.size(), ceiling * 2) << "the text should be well past the ceiling";

    ob::QueryResponseBuilder fits(shape, std::string(), ob::AnswerCeiling{ceiling, true});
    add_in_batches(fits, rows, 2048);
    EXPECT_FALSE(fits.full()) << "text past the ceiling whose frame is under it was refused";
    const std::string framed = fits.finish();
    EXPECT_LE(framed.size(), ceiling);
    const std::string text = text_of_frame(framed);
    EXPECT_TRUE(text == expected) << parted(text, expected);

    // Rows that do not compress, under a ceiling their frame passes.
    const Rows varied(200'000, false);
    ob::QueryResponseBuilder over(shape, std::string(), ob::AnswerCeiling{ceiling, true});
    const size_t batches = add_in_batches(over, varied, 2048);
    EXPECT_TRUE(over.full());
    EXPECT_LT(batches * 2048, varied.chosen.size()) << "the builder took rows after it was full";
    EXPECT_GT(over.bytes_built() + ob::Lz4FrameWriter::kEndBytes, ceiling);
    EXPECT_LT(over.rows_within(), over.rows_taken());
    EXPECT_GT(over.rows_within(), 0u) << "no megabyte of text was within a frame of two";
}

// ── The scan stops where its answer became full ──────────────────────────────

namespace {

std::atomic<uint64_t> g_counter{0};

std::string made(const fs::path& dir) {
    fs::create_directories(dir);
    return dir.string();
}

/// Four segments of 5000 rows of one book.
struct Store {
    fs::path dir;
    ob::ColumnarStore store;
    ob::AggregationEngine agg;
    ob::QueryEngine engine;

    Store()
        : dir(fs::temp_directory_path() / ("ob_answer_ceiling_" + std::to_string(::getpid()) + "_" +
                                           std::to_string(g_counter.fetch_add(1))))
        , store(made(dir))
        , engine(store, [](const std::string&) -> std::shared_ptr<ob::SoABuffer> { return nullptr; },
                 agg) {
        uint64_t seq = 0;
        for (int segment = 0; segment < 4; ++segment) {
            store.set_symbol_exchange("CE", "EX");
            for (int i = 0; i < 5000; ++i, ++seq) {
                ob::SnapshotRow r{};
                r.timestamp_ns    = 1'790'000'000'000'000'000ULL + seq * 1'000;
                r.sequence_number = seq + 1;
                r.side            = static_cast<uint8_t>(seq % 2);
                r.level_index     = static_cast<uint16_t>(seq % 20);
                r.price           = 10'000 + static_cast<int64_t>(seq % 997);
                r.quantity        = 1 + seq;
                r.order_count     = 1;
                store.append(r);
            }
            EXPECT_TRUE(store.flush_segment().has_value());
        }
    }
    ~Store() {
        store.close();
        std::error_code ec;
        fs::remove_all(dir, ec);
    }
};

const char* const kAll = "SELECT * FROM 'CE'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999";

/// The builder, with the rows the scan offered it counted: a full builder takes no more rows, so
/// only what was offered says whether the scan went on reading after the answer was full.
class Offered final : public ob::RowAnswer {
public:
    explicit Offered(ob::QueryResponseBuilder& reply) : reply_(reply) {}
    void add_rows(const ob::RowBatch& batch) override {
        offered += batch.rows.size();
        reply_.add_rows(batch);
    }
    std::unique_ptr<ob::ScanPart> part(uint64_t limit) override { return reply_.part(limit); }
    void add_part(ob::ScanPart& part, uint64_t n) override {
        offered += n;
        reply_.add_part(part, n);
    }
    bool full() const override { return reply_.full(); }

    uint64_t offered = 0;

private:
    ob::QueryResponseBuilder& reply_;
};

}  // namespace

TEST(AnswerCeiling, TheScanStopsAtTheBatchThatMadeItsAnswerFull) {
    Store s;
    ob::QueryShape shape;
    ob::QueryResponseBuilder reply(shape, std::string(), ob::AnswerCeiling{100'000, false});
    Offered offered(reply);
    const std::string err =
        s.engine.execute(kAll, [&](const ob::QueryResult& r) { reply.add(r); }, shape, &offered);
    ASSERT_TRUE(err.empty()) << err;
    EXPECT_TRUE(reply.full());
    EXPECT_EQ(offered.offered, reply.rows_taken()) << "the scan read on past a full answer";
    EXPECT_LE(reply.rows_taken() - reply.rows_within(), ob::kScanBatchRows)
        << "more than the batch that passed the ceiling was taken";
    EXPECT_LT(reply.rows_taken(), 20'000u);

    // The control: the same query with room for its answer reads every row.
    ob::QueryShape whole_shape;
    ob::QueryResponseBuilder whole(whole_shape, std::string(), ob::AnswerCeiling{64u << 20, false});
    ASSERT_TRUE(s.engine.execute(kAll, [&](const ob::QueryResult& r) { whole.add(r); }, whole_shape,
                                 &whole).empty());
    EXPECT_FALSE(whole.full());
    EXPECT_EQ(whole.rows_taken(), 20'000u);
}

TEST(AnswerCeiling, ADividedScanStopsAtThePartThatMadeItsAnswerFull) {
    Store s;
    ob::ScanPool pool(2, nullptr);
    s.engine.set_scan_pool(&pool, /*divide_small=*/true);
    ob::QueryShape shape;
    ob::QueryResponseBuilder reply(shape, std::string(), ob::AnswerCeiling{300'000, false});
    Offered offered(reply);
    const std::string err =
        s.engine.execute(kAll, [&](const ob::QueryResult& r) { reply.add(r); }, shape, &offered);
    ASSERT_TRUE(err.empty()) << err;
    EXPECT_TRUE(reply.full());
    // A part is a segment of 5000 rows: the first fits in 300 000 bytes and the second does not, so
    // the answer took one part, was offered the second, and the scan folded no more.
    EXPECT_EQ(reply.rows_taken(), 5000u);
    EXPECT_EQ(reply.rows_within(), 5000u);
    EXPECT_EQ(offered.offered, 10'000u) << "the scan folded parts past a full answer";

    ob::QueryShape whole_shape;
    ob::QueryResponseBuilder whole(whole_shape, std::string(), ob::AnswerCeiling{64u << 20, false});
    ASSERT_TRUE(s.engine.execute(kAll, [&](const ob::QueryResult& r) { whole.add(r); }, whole_shape,
                                 &whole).empty());
    EXPECT_FALSE(whole.full());
    EXPECT_EQ(whole.rows_taken(), 20'000u);
    s.engine.set_scan_pool(nullptr);
}
