#include "orderbook/best_track.hpp"

#include "orderbook/column_codec.hpp"
#include "orderbook/crc32c.hpp"
#include "orderbook/logger.hpp"

#include <array>
#include <bit>
#include <cstring>
#include <fstream>
#include <iterator>
#include <span>

namespace ob {

namespace {

static_assert(std::endian::native == std::endian::little,
              "best.v1 is written in the host's byte order, which every target of this engine has as "
              "little-endian; a big-endian build would need to swap");

constexpr char kMagic[8] = {'O', 'B', 'B', 'E', 'S', 'T', 'v', '1'};
constexpr uint32_t kVersion = 1;
constexpr uint32_t kFromFirstRow = 1;   // the header's flags

// magic, version, entries, from_ts, as_of_ts, flags, payload CRC, header CRC
constexpr size_t kHeaderBytes = 8 + 4 + 4 + 8 + 8 + 4 + 4 + 4;

// The entries are columns, each a column_codec block behind its length - time, which sides are
// there, the best bid, the best ask - so a best that walks a tick at a time costs a byte or two an
// entry.
constexpr size_t kColumns = 4;
constexpr uint64_t kHasBid = 1;
constexpr uint64_t kHasAsk = 2;

template <typename T>
void put(std::string& out, T v) {
    char b[sizeof(T)];
    std::memcpy(b, &v, sizeof(T));
    out.append(b, sizeof(T));
}

template <typename T>
T get(const char* p) {
    T v;
    std::memcpy(&v, p, sizeof(T));
    return v;
}

uint32_t crc_of(const char* p, size_t n) {
    return crc32c(reinterpret_cast<const uint8_t*>(p), n);
}

/// The header's fields from its bytes, checked by its CRC. Empty on success.
std::string parse_header(std::string_view bytes, BestTrackHeader& out, uint32_t& payload_crc) {
    if (bytes.size() < kHeaderBytes) {
        return "best track of " + std::to_string(bytes.size()) + " bytes, shorter than its header";
    }
    const char* p = bytes.data();
    if (std::memcmp(p, kMagic, sizeof(kMagic)) != 0) return "not a best track: another magic";
    const uint32_t header_crc = get<uint32_t>(p + kHeaderBytes - 4);
    if (crc_of(p, kHeaderBytes - 4) != header_crc) return "best track header fails its CRC";
    const uint32_t version = get<uint32_t>(p + 8);
    if (version != kVersion) {
        return "best track version " + std::to_string(version) + ", this build reads " +
               std::to_string(kVersion);
    }
    out.entries  = get<uint32_t>(p + 12);
    out.from_ts  = get<uint64_t>(p + 16);
    out.as_of_ts = get<uint64_t>(p + 24);
    const uint32_t flags = get<uint32_t>(p + 32);
    out.from_first_row = (flags & kFromFirstRow) != 0;
    payload_crc = get<uint32_t>(p + 36);
    if (out.entries == 0) return "best track with no entry: it has no best in force at its start";
    if (out.as_of_ts < out.from_ts) {
        return "best track as of " + std::to_string(out.as_of_ts) + ", before its start at " +
               std::to_string(out.from_ts);
    }
    return {};
}

}  // namespace

std::string encode_best_track(const BestTrack& track) {
    const size_t n = track.entries.size();
    std::array<std::vector<uint64_t>, kColumns> columns;
    for (auto& c : columns) c.reserve(n);
    // A side that is not there is written with its last price, so its column's deltas stay small,
    // and read back as 0: a BookTop has one form for an empty side.
    uint64_t bid = 0;
    uint64_t ask = 0;
    for (const BestTrackEntry& e : track.entries) {
        columns[0].push_back(e.timestamp_ns);
        columns[1].push_back((e.top.has_bid ? kHasBid : 0) | (e.top.has_ask ? kHasAsk : 0));
        if (e.top.has_bid) bid = static_cast<uint64_t>(e.top.bid);   // reinterpreted, as segments are
        if (e.top.has_ask) ask = static_cast<uint64_t>(e.top.ask);
        columns[2].push_back(bid);
        columns[3].push_back(ask);
    }
    std::string payload;
    std::string block;
    const column_codec::EncodeOptions options{};
    for (const auto& c : columns) {
        block.clear();
        column_codec::encode(c, options, block);
        column_codec::put_varint(payload, block.size());
        payload += block;
    }
    std::string out;
    out.reserve(kHeaderBytes + payload.size());
    out.append(kMagic, sizeof(kMagic));
    put<uint32_t>(out, kVersion);
    put<uint32_t>(out, static_cast<uint32_t>(n));
    put<uint64_t>(out, track.header.from_ts);
    put<uint64_t>(out, track.header.as_of_ts);
    put<uint32_t>(out, track.header.from_first_row ? kFromFirstRow : 0);
    put<uint32_t>(out, crc_of(payload.data(), payload.size()));
    put<uint32_t>(out, crc_of(out.data(), out.size()));
    out += payload;
    return out;
}

std::string decode_best_track(std::string_view bytes, BestTrack& out) {
    uint32_t payload_crc = 0;
    if (std::string err = parse_header(bytes, out.header, payload_crc); !err.empty()) return err;
    const std::span<const char> payload(bytes.data() + kHeaderBytes, bytes.size() - kHeaderBytes);
    if (crc_of(payload.data(), payload.size()) != payload_crc) return "best track entries fail their CRC";
    const size_t n = out.header.entries;
    std::array<std::vector<uint64_t>, kColumns> columns;
    size_t at = 0;
    for (size_t c = 0; c < kColumns; ++c) {
        uint64_t len = 0;
        if (!column_codec::get_varint(payload, at, len) || len > payload.size() - at) {
            return "best track column " + std::to_string(c) + " is cut short";
        }
        std::string why;
        if (!column_codec::decode(payload.subspan(at, len), n, columns[c], &why)) {
            return "best track column " + std::to_string(c) + " does not decode: " + why;
        }
        at += len;
    }
    if (at != payload.size()) {
        return "best track holds " + std::to_string(payload.size() - at) + " bytes past its columns";
    }
    out.entries.clear();
    out.entries.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        BestTrackEntry e;
        e.timestamp_ns = columns[0][i];
        if (columns[1][i] > (kHasBid | kHasAsk)) {
            return "best track entry " + std::to_string(i) + " has sides no book has";
        }
        e.top.has_bid = (columns[1][i] & kHasBid) != 0;
        e.top.has_ask = (columns[1][i] & kHasAsk) != 0;
        e.top.bid = e.top.has_bid ? static_cast<int64_t>(columns[2][i]) : 0;
        e.top.ask = e.top.has_ask ? static_cast<int64_t>(columns[3][i]) : 0;
        // What a reader relies on: entry 0 at the start, and the rest in time order within the span.
        if (i == 0 ? e.timestamp_ns != out.header.from_ts
                   : (e.timestamp_ns < out.entries.back().timestamp_ns ||
                      e.timestamp_ns > out.header.as_of_ts)) {
            return "best track entry " + std::to_string(i) + " at " + std::to_string(e.timestamp_ns) +
                   " is out of its place in [" + std::to_string(out.header.from_ts) + ", " +
                   std::to_string(out.header.as_of_ts) + "]";
        }
        out.entries.push_back(e);
    }
    return {};
}

std::string read_best_track_header(const std::string& path, BestTrackHeader& out) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return "cannot open " + path;
    char buf[kHeaderBytes];
    in.read(buf, static_cast<std::streamsize>(kHeaderBytes));
    const auto got = static_cast<size_t>(in.gcount());
    uint32_t payload_crc = 0;
    return parse_header(std::string_view(buf, got), out, payload_crc);
}

std::string read_best_track(const std::string& path, BestTrack& out) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return "cannot open " + path;
    const std::string bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    std::string err = decode_best_track(bytes, out);
    if (!err.empty()) {
        OB_LOG_WARN("columnar", "best track %s not read: %s", path.c_str(), err.c_str());
    }
    return err;
}

}  // namespace ob
