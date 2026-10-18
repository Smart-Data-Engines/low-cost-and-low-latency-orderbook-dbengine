#include "orderbook/book_summary.hpp"

#include "orderbook/column_codec.hpp"
#include "orderbook/crc32c.hpp"
#include "orderbook/logger.hpp"

#include <array>
#include <bit>
#include <span>
#include <cstring>
#include <fstream>
#include <iterator>

namespace ob {

namespace {

static_assert(std::endian::native == std::endian::little,
              "book.v1 is written in the host's byte order, which every target of this engine has as "
              "little-endian; a big-endian build would need to swap");

constexpr char kMagic[8] = {'O', 'B', 'B', 'O', 'O', 'K', 'v', '1'};
constexpr uint32_t kVersion = 1;

// magic, version, rows, as_of_ts, as_of_seq, payload CRC, header CRC
constexpr size_t kHeaderBytes = 8 + 4 + 4 + 8 + 8 + 4 + 4;

// The prices are columns, each a column_codec block behind its length, as a segment's are in
// format 3 - time, sequence, price, quantity, count, side - so a book whose prices are close together
// and were set by a few updates costs a few bytes a price, where fixed records cost forty.
constexpr size_t kColumns = 6;

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
std::string parse_header(std::string_view bytes, BookSummaryHeader& out, uint32_t& payload_crc) {
    if (bytes.size() < kHeaderBytes) {
        return "book summary of " + std::to_string(bytes.size()) + " bytes, shorter than its header";
    }
    const char* p = bytes.data();
    if (std::memcmp(p, kMagic, sizeof(kMagic)) != 0) return "not a book summary: another magic";
    const uint32_t header_crc = get<uint32_t>(p + kHeaderBytes - 4);
    if (crc_of(p, kHeaderBytes - 4) != header_crc) return "book summary header fails its CRC";
    const uint32_t version = get<uint32_t>(p + 8);
    if (version != kVersion) {
        return "book summary version " + std::to_string(version) + ", this build reads " +
               std::to_string(kVersion);
    }
    out.rows      = get<uint32_t>(p + 12);
    out.as_of_ts  = get<uint64_t>(p + 16);
    out.as_of_seq = get<uint64_t>(p + 24);
    payload_crc   = get<uint32_t>(p + 32);
    return {};
}

}  // namespace

std::string encode_book_summary(const BookSummary& summary) {
    const size_t n = summary.rows.size();
    std::array<std::vector<uint64_t>, kColumns> columns;
    for (auto& c : columns) c.reserve(n);
    for (const SnapshotRow& r : summary.rows) {
        columns[0].push_back(r.timestamp_ns);
        columns[1].push_back(r.sequence_number);
        columns[2].push_back(static_cast<uint64_t>(r.price));   // reinterpreted, as segments are
        columns[3].push_back(r.quantity);
        columns[4].push_back(r.order_count);
        columns[5].push_back(r.side);
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
    put<uint64_t>(out, summary.header.as_of_ts);
    put<uint64_t>(out, summary.header.as_of_seq);
    put<uint32_t>(out, crc_of(payload.data(), payload.size()));
    put<uint32_t>(out, crc_of(out.data(), out.size()));
    out += payload;
    return out;
}

std::string decode_book_summary(std::string_view bytes, BookSummary& out) {
    uint32_t payload_crc = 0;
    if (std::string err = parse_header(bytes, out.header, payload_crc); !err.empty()) return err;
    const std::span<const char> payload(bytes.data() + kHeaderBytes, bytes.size() - kHeaderBytes);
    if (crc_of(payload.data(), payload.size()) != payload_crc) return "book summary prices fail their CRC";
    const size_t n = out.header.rows;
    std::array<std::vector<uint64_t>, kColumns> columns;
    size_t at = 0;
    for (size_t c = 0; c < kColumns; ++c) {
        uint64_t len = 0;
        if (!column_codec::get_varint(payload, at, len) || len > payload.size() - at) {
            return "book summary column " + std::to_string(c) + " is cut short";
        }
        std::string why;
        if (!column_codec::decode(payload.subspan(at, len), n, columns[c], &why)) {
            return "book summary column " + std::to_string(c) + " does not decode: " + why;
        }
        at += len;
    }
    if (at != payload.size()) {
        return "book summary holds " + std::to_string(payload.size() - at) + " bytes past its columns";
    }
    out.rows.clear();
    out.rows.reserve(n);
    uint16_t level[2] = {0, 0};
    for (size_t i = 0; i < n; ++i) {
        SnapshotRow r{};
        r.timestamp_ns    = columns[0][i];
        r.sequence_number = columns[1][i];
        r.price           = static_cast<int64_t>(columns[2][i]);
        r.quantity        = columns[3][i];
        r.order_count     = static_cast<uint32_t>(columns[4][i]);
        if (columns[4][i] > UINT32_MAX || (columns[5][i] != SIDE_BID && columns[5][i] != SIDE_ASK)) {
            return "book summary price " + std::to_string(i) + " has a count or a side no row has";
        }
        r.side        = static_cast<uint8_t>(columns[5][i]);
        r.level_index = level[r.side]++;
        out.rows.push_back(r);
    }
    return {};
}

std::string read_book_summary_header(const std::string& path, BookSummaryHeader& out) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return "cannot open " + path;
    char buf[kHeaderBytes];
    in.read(buf, static_cast<std::streamsize>(kHeaderBytes));
    const auto got = static_cast<size_t>(in.gcount());
    uint32_t payload_crc = 0;
    return parse_header(std::string_view(buf, got), out, payload_crc);
}

std::string read_book_summary(const std::string& path, BookSummary& out) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return "cannot open " + path;
    const std::string bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    std::string err = decode_book_summary(bytes, out);
    if (!err.empty()) {
        OB_LOG_WARN("columnar", "book summary %s not read: %s", path.c_str(), err.c_str());
    }
    return err;
}

}  // namespace ob
