// What applying a recording's updates to the live books costs, a level at a time (#268).
//
// A recording as CSV - ts_ns,symbol,exchange,side,level,price_ticks,size_lots, a size of 0 a removal - is
// read into updates: consecutive rows of one (time, symbol, side), as a feed's update is one side of one
// book at one instant. They are then applied in order through ob::apply_delta(), one SoABuffer a symbol,
// the way a write reaches its book on a client loop - the seqlock, insert_level() for each level - and
// nothing else of the engine: no WAL, no queue, no store.
//
// What comes out, as one JSON object: the updates, the levels and the books; the ns a level, the fastest
// of `rounds`; the depth of the side an update left, at the median, the 90th percentile and the most; and
// a hash of every book at the end, so two builds that should keep the same books can be compared by one
// number.
//
// Usage: book_apply_cost <recording.csv> [rounds]
#include "orderbook/data_model.hpp"
#include "orderbook/soa_buffer.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace {

struct Update {
    ob::DeltaUpdate delta{};
    std::vector<ob::Level> levels;
    size_t book{0};
};

/// The fields of one CSV line, in place: no allocation a field.
bool split(const std::string& line, std::string_view (&fields)[7]) {
    size_t at = 0;
    for (int f = 0; f < 7; ++f) {
        const size_t comma = f < 6 ? line.find(',', at) : line.size();
        if (comma == std::string::npos) return false;
        fields[f] = std::string_view(line).substr(at, comma - at);
        at = comma + 1;
    }
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <recording.csv> [rounds]\n", argv[0]);
        return 2;
    }
    const int rounds = argc > 2 ? std::atoi(argv[2]) : 7;
    if (rounds < 1) {
        std::fprintf(stderr, "rounds is at least 1\n");
        return 2;
    }
    std::ifstream in(argv[1]);
    if (!in) {
        std::fprintf(stderr, "cannot read %s\n", argv[1]);
        return 1;
    }
    std::string line;
    std::getline(in, line);   // the header
    std::vector<Update> updates;
    std::map<std::string, size_t> books;
    std::string last_key;
    size_t levels = 0;
    while (std::getline(in, line)) {
        std::string_view f[7];
        if (!split(line, f)) continue;
        const std::string key = std::string(f[0]) + "," + std::string(f[1]) + "," + std::string(f[3]);
        // A new update at a new (time, symbol, side), or where one would pass the levels an update holds.
        if (updates.empty() || key != last_key || updates.back().levels.size() >= ob::MAX_LEVELS) {
            Update u;
            const std::string symbol(f[1]), exchange(f[2]);
            std::strncpy(u.delta.symbol, symbol.c_str(), sizeof(u.delta.symbol) - 1);
            std::strncpy(u.delta.exchange, exchange.c_str(), sizeof(u.delta.exchange) - 1);
            u.delta.timestamp_ns = std::strtoull(std::string(f[0]).c_str(), nullptr, 10);
            u.delta.side = f[3] == "bid" ? ob::SIDE_BID : ob::SIDE_ASK;
            u.book = books.try_emplace(symbol + "." + exchange, books.size()).first->second;
            updates.push_back(std::move(u));
            last_key = key;
        }
        ob::Level l{};
        l.price = std::strtoll(std::string(f[5]).c_str(), nullptr, 10);
        l.qty = std::strtoull(std::string(f[6]).c_str(), nullptr, 10);
        l.cnt = 1;
        updates.back().levels.push_back(l);
        ++levels;
    }
    if (updates.empty()) {
        std::fprintf(stderr, "no updates in %s\n", argv[1]);
        return 1;
    }
    uint64_t seq = 0;
    for (Update& u : updates) {
        u.delta.n_levels = static_cast<uint16_t>(u.levels.size());
        u.delta.sequence_number = ++seq;
    }

    // One pass over every update into fresh books: its time, the depth each update left its side at, and
    // a hash of every book at the end.
    std::vector<uint32_t> depths;
    const auto pass = [&](bool keep_depths, uint64_t& hash) {
        std::vector<std::unique_ptr<ob::SoABuffer>> bufs;
        for (size_t b = 0; b < books.size(); ++b) {
            auto p = std::make_unique<ob::SoABuffer>();
            p->bid.depth = 0;
            p->ask.depth = 0;
            p->bid.version.store(0, std::memory_order_relaxed);
            p->ask.version.store(0, std::memory_order_relaxed);
            p->sequence_number.store(0, std::memory_order_relaxed);
            bufs.push_back(std::move(p));
        }
        const auto t0 = std::chrono::steady_clock::now();
        for (const Update& u : updates) {
            ob::SoABuffer& buf = *bufs[u.book];
            bool gap = false;
            ob::apply_delta(buf, u.delta, u.levels.data(), gap);
            if (keep_depths) depths.push_back((u.delta.side == ob::SIDE_BID ? buf.bid : buf.ask).depth);
        }
        const auto t1 = std::chrono::steady_clock::now();
        hash = 0xcbf29ce484222325ULL;
        for (const auto& p : bufs) {
            for (const ob::SoASide* s : {&p->bid, &p->ask}) {
                for (uint32_t i = 0; i < s->depth; ++i) {
                    hash = (hash ^ static_cast<uint64_t>(s->prices[i])) * 0x100000001b3ULL;
                    hash = (hash ^ s->quantities[i]) * 0x100000001b3ULL;
                    hash = (hash ^ s->order_counts[i]) * 0x100000001b3ULL;
                }
                hash = (hash ^ s->depth) * 0x100000001b3ULL;
            }
        }
        return std::chrono::duration<double, std::nano>(t1 - t0).count();
    };
    uint64_t hash = 0;
    pass(true, hash);
    double best = 1e300;
    for (int r = 0; r < rounds; ++r) {
        uint64_t h = 0;
        best = std::min(best, pass(false, h));
        if (h != hash) {
            std::fprintf(stderr, "round %d kept other books than the first\n", r);
            return 1;
        }
    }
    std::sort(depths.begin(), depths.end());
    std::printf("{\"updates\": %zu, \"levels\": %zu, \"books\": %zu, \"rounds\": %d, \"ns_per_level\": %.2f, "
                "\"depth_p50\": %u, \"depth_p90\": %u, \"depth_max\": %u, \"hash\": \"%016llx\"}\n",
                updates.size(), levels, books.size(), rounds, best / static_cast<double>(levels),
                depths[depths.size() / 2], depths[depths.size() * 9 / 10], depths.back(),
                static_cast<unsigned long long>(hash));
    return 0;
}
