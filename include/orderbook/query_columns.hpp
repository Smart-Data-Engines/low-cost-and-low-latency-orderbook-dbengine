#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace ob {

// ── QueryColumn ───────────────────────────────────────────────────────────────
//
// The seven columns a row query can name, in the order `SELECT *` emits them.
//
// Two lists come out of a query and they are not the same list, which is the whole reason this
// header exists:
//
//   * what has to be **answered** - ordered, may repeat, because it answers the question
//     literally: `SELECT price, price` is two columns;
//   * what has to be **read** - a set, because a column file is opened at most once, and wider
//     than the first list, because `SELECT price WHERE timestamp BETWEEN ...` has to read
//     `ts.col` and must not answer it.
//
// Holding those in one type is the defect this exists to remove.

enum class QueryColumn : uint8_t {
    TimestampNs = 0,
    Price,
    Quantity,
    OrderCount,
    Side,
    Level,
    SequenceNumber,
};

inline constexpr size_t kQueryColumnCount = 7;

/// The spelling this column has in a response header.
///
/// The only place these seven strings exist. Before this they were in two: a literal header in the
/// formatter and the lexer's keyword table - and they already disagreed about one column, which is
/// recorded on `column_keyword()` below.
std::string_view column_name(QueryColumn c);

/// The spelling this column has in a query, which is **not** always `column_name()`.
///
/// `timestamp_ns` in a header is `timestamp` in SQL. The lexer accepts both spellings so that a
/// name copied out of a header parses, but a header is generated from `column_name()` alone, so
/// what `SELECT *` answers does not move.
std::string_view column_keyword(QueryColumn c);

/// The seven columns in the order `SELECT *` emits them.
const std::vector<QueryColumn>& all_query_columns();

// ── ColumnSet ─────────────────────────────────────────────────────────────────
//
// Which columns a reader has to read. A mask rather than a list: order does not matter to a
// reader and a repeated column is still one file.

class ColumnSet {
public:
    constexpr ColumnSet() noexcept = default;

    static constexpr ColumnSet all() noexcept {
        ColumnSet s;
        s.bits_ = static_cast<uint8_t>((1u << kQueryColumnCount) - 1u);
        return s;
    }

    constexpr ColumnSet& add(QueryColumn c) noexcept {
        bits_ = static_cast<uint8_t>(bits_ | (1u << static_cast<uint8_t>(c)));
        return *this;
    }

    constexpr bool has(QueryColumn c) const noexcept {
        return (bits_ & (1u << static_cast<uint8_t>(c))) != 0;
    }

    constexpr size_t count() const noexcept {
        size_t n = 0;
        for (uint8_t i = 0; i < kQueryColumnCount; ++i) {
            if (bits_ & (1u << i)) ++n;
        }
        return n;
    }

    constexpr bool operator==(const ColumnSet&) const noexcept = default;

private:
    uint8_t bits_{};
};

static_assert(kQueryColumnCount <= 8, "ColumnSet holds one bit per column in a uint8_t");

/// The widest this column's value can be printed, in bytes, without its separator.
///
/// `price` is signed, so it carries its sign here; `side` is a byte and `level` is 16 bits. A
/// caller sizing a row buffer needs this per column rather than a single worst case, because a
/// query may name the same column more than once and seven columns' worth of room is then not
/// enough.
size_t column_max_digits(QueryColumn c);

/// The widest one row of `output` can be, separators included.
size_t max_row_bytes(const std::vector<QueryColumn>& output);

/// Everything a scan has to read to answer `output` under these predicates.
///
/// Wider than `output` on purpose. `TimestampNs` is always in it because every row scan filters on
/// the time range, and `Price` joins it when the query filters on price - a column a predicate
/// needs is read whether or not it is answered.
ColumnSet columns_to_read(const std::vector<QueryColumn>& output, bool has_price_filter);

} // namespace ob
