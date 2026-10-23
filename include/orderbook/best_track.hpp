#pragma once

// A track of the best price of each side of one symbol's book, kept in a segment's directory as
// `best.<as_of>.v1` beside the book summary it ends at (#243; spec `best-price-track`).
//
// A series of the book - OPEN, HIGH, LOW, CLOSE and TWAP of the bid, the ask, the mid and the spread -
// reads nothing of the book but its best price on each side, after all the rows of each instant. Since
// #233 those are read by price, and any row can set a better price or remove the best one, so a series
// applied every row of its range: about 30 ns a row. A track is what that composition would yield,
// written down once: the best of each side after every instant at which it changed, from the instant
// the composition started from to the instant its summary is as of. A series reads one entry a change
// of the best price where a track covers its range, and the rows elsewhere.
//
// **What a track claims** is its summary's claim: the book it started from and every row of the
// symbol the store holds up to `as_of_ts` are the ones it was composed from. The same late row
// falsifies both, and the store removes them together (`ColumnarStore::falsify_summaries_locked()`).
//
// The file is a header and the entries as columns, each a `column_codec` block as a segment's columns
// are, with a CRC32C for the header and one for the columns: a track that does not read back is no
// track, and a series reads the rows instead.

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace ob {

/// The best of each side of a book: all a series reads of it. A side that is not there has price 0,
/// so that one book has one BookTop.
struct BookTop {
    bool    has_bid{false};
    bool    has_ask{false};
    int64_t bid{0};
    int64_t ask{0};

    bool operator==(const BookTop&) const = default;
};

/// The best of each side after all the rows of one instant.
struct BestTrackEntry {
    uint64_t timestamp_ns{0};
    BookTop  top;

    bool operator==(const BestTrackEntry&) const = default;
};

/// The files tracks are kept in, beside the summary each ends at: `best.<as of>.v1`. Each is one of a
/// segment's files (`is_segment_file()`), so a snapshot, a backup, a migration and a merge carry it.
inline std::string best_track_file(uint64_t as_of_ts) {
    return "best." + std::to_string(as_of_ts) + ".v1";
}

/// Whether `filename` is a track's: `best.<digits>.v1`.
inline bool is_best_track_file(std::string_view filename) {
    constexpr std::string_view head = "best.";
    constexpr std::string_view tail = ".v1";
    if (filename.size() <= head.size() + tail.size()) return false;
    if (filename.substr(0, head.size()) != head) return false;
    if (filename.substr(filename.size() - tail.size()) != tail) return false;
    for (const char c : filename.substr(head.size(), filename.size() - head.size() - tail.size())) {
        if (c < '0' || c > '9') return false;
    }
    return true;
}

/// What a track says about itself, read from its header alone.
struct BestTrackHeader {
    uint64_t from_ts{0};          ///< the instant the composition started from: its summary's
    uint64_t as_of_ts{0};         ///< the instant its summary is as of: the last it covers
    uint32_t entries{0};
    bool     from_first_row{false};   ///< composed from the symbol's first row, from an empty book
};

/// The best of each side through (`from_ts`, `as_of_ts`]. Entry 0 is the best in force at `from_ts`,
/// from the book the composition started from; each later one the best after all the rows of an
/// instant in that span at which it changed, in time order.
struct BestTrack {
    BestTrackHeader header;
    std::vector<BestTrackEntry> entries;
};

/// The file's bytes.
std::string encode_best_track(const BestTrack& track);

/// Read a track back, or say why the bytes are not one: too short, another magic or version, a CRC
/// that does not match, a count the bytes do not hold, entries out of time order. Empty on success.
std::string decode_best_track(std::string_view bytes, BestTrack& out);

/// The header alone, checked by its own CRC: what a store reads of every track when it registers
/// one. Empty on success.
std::string read_best_track_header(const std::string& path, BestTrackHeader& out);

/// The whole file, checked. Empty on success.
std::string read_best_track(const std::string& path, BestTrack& out);

}  // namespace ob
