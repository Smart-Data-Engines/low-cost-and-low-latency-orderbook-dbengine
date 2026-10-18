#include "orderbook/book_summary.hpp"

#include "orderbook/crc32c.hpp"
#include "orderbook/logger.hpp"

#include <bit>
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
// ts, seq, price, qty, count, side, three bytes of padding
constexpr size_t kRowBytes = 8 + 8 + 8 + 8 + 4 + 1 + 3;

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
    std::string payload;
    payload.reserve(summary.rows.size() * kRowBytes);
    for (const SnapshotRow& r : summary.rows) {
        put<uint64_t>(payload, r.timestamp_ns);
        put<uint64_t>(payload, r.sequence_number);
        put<int64_t>(payload, r.price);
        put<uint64_t>(payload, r.quantity);
        put<uint32_t>(payload, r.order_count);
        put<uint8_t>(payload, r.side);
        payload.append(3, '\0');
    }
    std::string out;
    out.reserve(kHeaderBytes + payload.size());
    out.append(kMagic, sizeof(kMagic));
    put<uint32_t>(out, kVersion);
    put<uint32_t>(out, static_cast<uint32_t>(summary.rows.size()));
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
    const size_t payload = bytes.size() - kHeaderBytes;
    if (payload != static_cast<size_t>(out.header.rows) * kRowBytes) {
        return "book summary says " + std::to_string(out.header.rows) + " prices and holds " +
               std::to_string(payload) + " bytes of them";
    }
    const char* p = bytes.data() + kHeaderBytes;
    if (crc_of(p, payload) != payload_crc) return "book summary prices fail their CRC";
    out.rows.clear();
    out.rows.reserve(out.header.rows);
    uint16_t level[2] = {0, 0};
    for (uint32_t i = 0; i < out.header.rows; ++i, p += kRowBytes) {
        SnapshotRow r{};
        r.timestamp_ns    = get<uint64_t>(p);
        r.sequence_number = get<uint64_t>(p + 8);
        r.price           = get<int64_t>(p + 16);
        r.quantity        = get<uint64_t>(p + 24);
        r.order_count     = get<uint32_t>(p + 32);
        r.side            = get<uint8_t>(p + 36);
        if (r.side != SIDE_BID && r.side != SIDE_ASK) {
            return "book summary price " + std::to_string(i) + " has side " + std::to_string(r.side);
        }
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
