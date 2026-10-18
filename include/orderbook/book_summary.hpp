#pragma once

// A summary of one symbol's book by price, kept in a segment's directory as `book.v1` (#233,
// decision B'; spec `incremental-books`, section 2).
//
// The book at an instant is read by price: every row up to it applied in time order, each setting its
// price's quantity, a quantity of 0 removing the price. Read from the start of a symbol's history that
// costs every row it has - 350 ms at 10 M rows on the i3, where reading by level skipped every segment
// but the newest (#47). A summary is that composition stopped at one instant and written down, so a
// read starts from the newest summary at or before its instant and applies only the rows after it.
//
// **What a summary claims** is the store's to keep true, not the file's: every row of the symbol the
// store holds with a time at or before `as_of_ts` is in it. A row that arrives later with such a time
// - a client's event time (#105), a peer's backlog - makes it false, and the store removes the
// summaries it falsifies before that row can be read (`ColumnarStore::publish_blocks()`).
//
// The file is a header and the prices as columns, each a `column_codec` block as a segment's columns
// are, with a CRC32C for the header and one for the columns: a summary that does not read back is no
// summary, and a reader falls back to composing the rows themselves.

#include "orderbook/data_model.hpp"

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace ob {

/// The files summaries are kept in, inside the directory of a segment of their symbol: `book.<as of>.v1`,
/// one for each time, so that a merge carries all of its inputs' summaries into one directory. Each
/// is one of a segment's files (`is_segment_file()`), so a snapshot, a backup and a migration carry it.
inline std::string book_summary_file(uint64_t as_of_ts) {
    return "book." + std::to_string(as_of_ts) + ".v1";
}

/// Whether `filename` is a summary's: `book.<digits>.v1`.
inline bool is_book_summary_file(std::string_view filename) {
    constexpr std::string_view head = "book.";
    constexpr std::string_view tail = ".v1";
    if (filename.size() <= head.size() + tail.size()) return false;
    if (filename.substr(0, head.size()) != head) return false;
    if (filename.substr(filename.size() - tail.size()) != tail) return false;
    for (const char c : filename.substr(head.size(), filename.size() - head.size() - tail.size())) {
        if (c < '0' || c > '9') return false;
    }
    return true;
}

/// What a summary says about itself, read from its header alone.
struct BookSummaryHeader {
    uint64_t as_of_ts{0};    ///< the latest time of a row in it
    uint64_t as_of_seq{0};   ///< that row's sequence number: the update the book is as of
    uint32_t rows{0};        ///< prices held, both sides
};

/// The book of one symbol by price as of `as_of_ts`: every price each side holds, and the row that
/// set it last. Bids best first, then asks best first; a row's `level_index` is its position.
struct BookSummary {
    BookSummaryHeader header;
    std::vector<SnapshotRow> rows;
};

/// The file's bytes.
std::string encode_book_summary(const BookSummary& summary);

/// Read a summary back, or say why the bytes are not one: too short, another magic or version, a CRC
/// that does not match, a count the bytes do not hold. Empty on success.
std::string decode_book_summary(std::string_view bytes, BookSummary& out);

/// The header alone, checked by its own CRC: what a store reads of every summary when it opens, so
/// that opening does not read every price of every summary. Empty on success.
std::string read_book_summary_header(const std::string& path, BookSummaryHeader& out);

/// The whole file, checked. Empty on success.
std::string read_book_summary(const std::string& path, BookSummary& out);

}  // namespace ob
