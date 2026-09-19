// The query response, byte for byte.
//
// `format_query_response` is 40% of the server's profile while answering the benchmark's
// time-range query — measured with `perf` on an m9g.xlarge, against 2.7% for
// `ColumnarStore::scan` — so it is the first thing worth making faster. It is also a **wire
// format**: every client in this repository and every client anybody writes reads these bytes, so
// a rewrite of its hot loop needs a net that fails on a changed byte rather than on a changed
// shape. Before this file the only coverage was indirect, through `test_tcp_server.cpp`.
//
// The expectations here were taken from the implementation that existed before the rewrite, not
// written from the specification, which is the only way for them to catch the rewrite changing
// something nobody meant to change.

#include "orderbook/response_formatter.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace {

constexpr const char* kHeader =
    "timestamp_ns\tprice\tquantity\torder_count\tside\tlevel\tsequence_number";

ob::QueryResult row(uint64_t ts, int64_t price, uint64_t qty, uint32_t cnt,
                    uint8_t side, uint16_t level, uint64_t seq) {
    ob::QueryResult r{};
    r.timestamp_ns    = ts;
    r.price           = price;
    r.quantity        = qty;
    r.order_count     = cnt;
    r.side            = side;
    r.level           = level;
    r.sequence_number = seq;
    return r;
}

/// The number of tab-separated fields on a line, so a dropped or added column fails loudly.
size_t fields(const std::string& line) {
    size_t n = 1;
    for (char c : line) {
        if (c == '\t') ++n;
    }
    return n;
}

std::vector<std::string> lines_of(const std::string& s) {
    std::vector<std::string> out;
    size_t start = 0;
    while (start <= s.size()) {
        const size_t nl = s.find('\n', start);
        if (nl == std::string::npos) { out.push_back(s.substr(start)); break; }
        out.push_back(s.substr(start, nl - start));
        start = nl + 1;
    }
    return out;
}

}  // namespace

TEST(ResponseFormatter, AnEmptyResultIsStillAHeaderAndATerminator) {
    const std::string out = ob::format_query_response({});
    EXPECT_EQ(out, std::string("OK\n") + kHeader + "\n\n");
}

TEST(ResponseFormatter, OneRowIsSevenTabSeparatedFieldsInHeaderOrder) {
    const std::string out = ob::format_query_response(
        {row(1'700'000'000'000'000'000ULL, 5'000'000, 1'000, 1, 0, 3, 42)});
    EXPECT_EQ(out, std::string("OK\n") + kHeader +
                       "\n1700000000000000000\t5000000\t1000\t1\t0\t3\t42\n\n");
}

// `side` is a `uint8_t`, so a formatter that hands it to a character-typed overload would print a
// byte rather than a number. Both documented values are pinned.
TEST(ResponseFormatter, SideIsPrintedAsANumberAndNotAsACharacter) {
    for (uint8_t side : {uint8_t{0}, uint8_t{1}}) {
        const std::string out = ob::format_query_response({row(1, 2, 3, 4, side, 6, 7)});
        const auto ls = lines_of(out);
        ASSERT_GE(ls.size(), 3U);
        EXPECT_EQ(ls[2], "1\t2\t3\t4\t" + std::to_string(static_cast<unsigned>(side)) + "\t6\t7");
    }
}

TEST(ResponseFormatter, EveryFieldAtItsTypesLimitRoundTripsExactly) {
    const std::string out = ob::format_query_response(
        {row(std::numeric_limits<uint64_t>::max(),
             std::numeric_limits<int64_t>::min(),
             std::numeric_limits<uint64_t>::max(),
             std::numeric_limits<uint32_t>::max(),
             std::numeric_limits<uint8_t>::max(),
             std::numeric_limits<uint16_t>::max(),
             std::numeric_limits<uint64_t>::max())});
    const auto ls = lines_of(out);
    ASSERT_GE(ls.size(), 3U);
    EXPECT_EQ(ls[2],
              "18446744073709551615\t-9223372036854775808\t18446744073709551615\t"
              "4294967295\t255\t65535\t18446744073709551615");
}

// A negative price is the one signed field, and the sign has to survive.
TEST(ResponseFormatter, ANegativePriceKeepsItsSign) {
    const std::string out = ob::format_query_response({row(1, -12345, 1, 1, 1, 1, 1)});
    EXPECT_NE(out.find("\t-12345\t"), std::string::npos);
}

TEST(ResponseFormatter, ManyRowsAreOnePerLineWithExactlyOneBlankLineAtTheEnd) {
    std::vector<ob::QueryResult> rows;
    for (uint64_t i = 0; i < 64; ++i) {
        rows.push_back(row(1'700'000'000'000'000'000ULL + i, static_cast<int64_t>(i) - 32,
                           i * 7, static_cast<uint32_t>(i), static_cast<uint8_t>(i & 1),
                           static_cast<uint16_t>(i), i + 1));
    }
    const std::string out = ob::format_query_response(rows);
    const auto ls = lines_of(out);
    // "OK", header, 64 rows, the blank terminator, and the empty tail after the final newline.
    ASSERT_EQ(ls.size(), 68U);
    EXPECT_EQ(ls[0], "OK");
    EXPECT_EQ(ls[1], kHeader);
    for (size_t i = 0; i < 64; ++i) {
        EXPECT_EQ(fields(ls[2 + i]), 7U) << "row " << i << " is '" << ls[2 + i] << "'";
    }
    EXPECT_TRUE(ls[66].empty());
    EXPECT_TRUE(ls[67].empty());
    EXPECT_EQ(out.size(), out.find("\n\n") + 2U) << "there is content after the terminator";
}

// The header a client reads its column indices from. If a column is added it goes last, and the
// note in the formatter says why; this fails if the order changes under a client that reads by
// index.
TEST(ResponseFormatter, TheHeaderNamesSevenColumnsInTheDocumentedOrder) {
    const std::string out = ob::format_query_response({});
    const auto ls = lines_of(out);
    ASSERT_GE(ls.size(), 2U);
    EXPECT_EQ(fields(ls[1]), 7U);
    EXPECT_EQ(ls[1], kHeader);
}
