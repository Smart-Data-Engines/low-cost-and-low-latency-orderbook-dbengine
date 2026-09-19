// Column identities, spellings and read sets.
//
// The header spelling has to be pinned here rather than inferred, because it is the one thing in
// this change that every client already depends on: the comparative harness reads three columns
// out of it by name, and `SELECT *` has to answer exactly the bytes it answered before.

#include "orderbook/query_columns.hpp"

#include <gtest/gtest.h>

#include <string>

using namespace ob;

namespace {

std::string header_from(const std::vector<QueryColumn>& columns) {
    std::string out;
    for (size_t i = 0; i < columns.size(); ++i) {
        if (i != 0) out += '\t';
        out += column_name(columns[i]);
    }
    return out;
}

} // namespace

TEST(QueryColumns, TheGeneratedHeaderIsByteForByteTheOneClientsAlreadyParse) {
    // Written out rather than referenced: a test that compares the generator against itself
    // passes for every spelling, including a wrong one.
    EXPECT_EQ(header_from(all_query_columns()),
              "timestamp_ns\tprice\tquantity\torder_count\tside\tlevel\tsequence_number");
}

TEST(QueryColumns, AllSevenColumnsAreThereInTheOrderSelectStarEmitsThem) {
    const auto& all = all_query_columns();
    ASSERT_EQ(all.size(), kQueryColumnCount);
    EXPECT_EQ(all[0], QueryColumn::TimestampNs);
    EXPECT_EQ(all[1], QueryColumn::Price);
    EXPECT_EQ(all[2], QueryColumn::Quantity);
    EXPECT_EQ(all[3], QueryColumn::OrderCount);
    EXPECT_EQ(all[4], QueryColumn::Side);
    EXPECT_EQ(all[5], QueryColumn::Level);
    EXPECT_EQ(all[6], QueryColumn::SequenceNumber);
}

TEST(QueryColumns, OneColumnIsSpeltDifferentlyInAHeaderAndInAQuery) {
    // Not a tidy-up waiting to happen: the header name is what every client reads and the keyword
    // is what the lexer has always accepted. This test exists so that anyone making them agree
    // has to decide to break one of the two, rather than discover it afterwards.
    EXPECT_EQ(column_name(QueryColumn::TimestampNs), "timestamp_ns");
    EXPECT_EQ(column_keyword(QueryColumn::TimestampNs), "timestamp");

    // And the other six agree, which is why that one is easy to miss.
    for (QueryColumn c : all_query_columns()) {
        if (c == QueryColumn::TimestampNs) continue;
        EXPECT_EQ(column_name(c), column_keyword(c)) << "column " << static_cast<int>(c);
    }
}

TEST(QueryColumns, AnEmptySetHasNothingAndTheFullSetHasEverything) {
    ColumnSet empty;
    for (QueryColumn c : all_query_columns()) EXPECT_FALSE(empty.has(c));
    EXPECT_EQ(empty.count(), 0u);

    ColumnSet all = ColumnSet::all();
    for (QueryColumn c : all_query_columns()) EXPECT_TRUE(all.has(c));
    EXPECT_EQ(all.count(), kQueryColumnCount);
}

TEST(QueryColumns, AddingTheSameColumnTwiceIsStillOneColumn) {
    ColumnSet s;
    s.add(QueryColumn::Price).add(QueryColumn::Price);
    EXPECT_EQ(s.count(), 1u);
    EXPECT_TRUE(s.has(QueryColumn::Price));
}

TEST(QueryColumns, TheTimestampIsReadEvenWhenNobodyAskedForIt) {
    // Every row scan filters on the time range, so the column is read whether or not it is
    // answered. A reader that skipped it would compare against a zero it never loaded.
    ColumnSet s = columns_to_read({QueryColumn::Price}, /*has_price_filter=*/false);
    EXPECT_TRUE(s.has(QueryColumn::Price));
    EXPECT_TRUE(s.has(QueryColumn::TimestampNs));
    EXPECT_EQ(s.count(), 2u);
}

TEST(QueryColumns, APriceFilterReadsThePriceEvenWhenTheAnswerDoesNotCarryIt) {
    ColumnSet s = columns_to_read({QueryColumn::Quantity}, /*has_price_filter=*/true);
    EXPECT_TRUE(s.has(QueryColumn::Quantity));
    EXPECT_TRUE(s.has(QueryColumn::TimestampNs));
    EXPECT_TRUE(s.has(QueryColumn::Price)) << "the predicate needs it, the answer does not";
    EXPECT_EQ(s.count(), 3u);

    // The control: without the filter, the same question does not read the price.
    ColumnSet without = columns_to_read({QueryColumn::Quantity}, /*has_price_filter=*/false);
    EXPECT_FALSE(without.has(QueryColumn::Price));
}

TEST(QueryColumns, AskingForEverythingReadsEverything) {
    EXPECT_EQ(columns_to_read(all_query_columns(), false), ColumnSet::all());
}

TEST(QueryColumns, RepeatingAColumnInTheAnswerDoesNotReadItTwice) {
    ColumnSet s = columns_to_read({QueryColumn::Price, QueryColumn::Price}, false);
    EXPECT_EQ(s.count(), 2u) << "price and the timestamp, not price twice";
}
