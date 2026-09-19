#include "orderbook/query_columns.hpp"

#include "orderbook/logger.hpp"

#include <array>

namespace ob {

namespace {

/// Header spelling and SQL spelling side by side, so the pair that differs is visible rather than
/// discovered. Only `TimestampNs` differs, and it differed before this table existed.
struct Spelling {
    std::string_view header;
    std::string_view keyword;
};

constexpr std::array<Spelling, kQueryColumnCount> kSpellings{{
    {"timestamp_ns",    "timestamp"},
    {"price",           "price"},
    {"quantity",        "quantity"},
    {"order_count",     "order_count"},
    {"side",            "side"},
    {"level",           "level"},
    {"sequence_number", "sequence_number"},
}};

} // namespace

namespace {

/// Widest decimal rendering of each column's type: 20 for a 64-bit value (19 digits, or 19 digits
/// and a sign for the signed one), 10 for 32 bits, 5 for 16, 3 for 8.
constexpr std::array<size_t, kQueryColumnCount> kMaxDigits{
    20,  // timestamp_ns    uint64
    20,  // price           int64, 19 digits and a sign
    20,  // quantity        uint64
    10,  // order_count     uint32
    3,   // side            uint8
    5,   // level           uint16
    20,  // sequence_number uint64
};

} // namespace

size_t column_max_digits(QueryColumn c) {
    return kMaxDigits[static_cast<size_t>(c)];
}

size_t max_row_bytes(const std::vector<QueryColumn>& output) {
    size_t n = 0;
    for (QueryColumn c : output) n += column_max_digits(c) + 1;  // + its separator
    return n;
}

std::string_view column_name(QueryColumn c) {
    return kSpellings[static_cast<size_t>(c)].header;
}

std::string_view column_keyword(QueryColumn c) {
    return kSpellings[static_cast<size_t>(c)].keyword;
}

const std::vector<QueryColumn>& all_query_columns() {
    static const std::vector<QueryColumn> kAll{
        QueryColumn::TimestampNs,
        QueryColumn::Price,
        QueryColumn::Quantity,
        QueryColumn::OrderCount,
        QueryColumn::Side,
        QueryColumn::Level,
        QueryColumn::SequenceNumber,
    };
    return kAll;
}

ColumnSet columns_to_read(const std::vector<QueryColumn>& output, bool has_price_filter) {
    ColumnSet set;
    for (QueryColumn c : output) set.add(c);

    // Always: every row scan compares the row's timestamp against the requested range, so the
    // column is read whether or not it is answered.
    set.add(QueryColumn::TimestampNs);

    if (has_price_filter) set.add(QueryColumn::Price);

    OB_LOG_DEBUG("query", "Columns to read: %zu of %zu (output %zu, price filter %s)",
                 set.count(), kQueryColumnCount, output.size(),
                 has_price_filter ? "yes" : "no");
    return set;
}

} // namespace ob
