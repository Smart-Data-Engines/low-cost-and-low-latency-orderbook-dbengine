// The seeds of fuzz_column_block (fuzz/corpus/column_block/): what the encoder writes, so that a
// campaign starts from blocks the decoder accepts and spreads from there.
//
//   make_column_block_seeds <dir>
//
// One file a seed, `<count u16 LE><column type u8><block>` as the harness reads it: three columns -
// a price ladder on a tick of 10^6, timestamps twenty rows to an instant, random quantities - in
// every encoding that applies to each, and three the decoder must refuse. Run again after a change
// to the block's layout, and commit what it writes.
#include "orderbook/column_codec.hpp"
#include <cstdio>
#include <fstream>
#include <string>
#include <vector>
namespace cc = ob::column_codec;
static void put(const std::string& dir, const std::string& name, size_t count, unsigned type, const std::string& block) {
    std::string bytes;
    bytes.push_back(static_cast<char>(count & 0xff));
    bytes.push_back(static_cast<char>((count >> 8) & 0xff));
    bytes.push_back(static_cast<char>(type));
    bytes += block;
    std::ofstream(dir + "/" + name, std::ios::binary) << bytes;
}
int main(int argc, char** argv) {
    if (argc != 2) {
        std::fprintf(stderr, "usage: %s <corpus directory>\n", argv[0]);
        return 2;
    }
    const std::string dir = argv[1];
    std::vector<uint64_t> ladder, ts, random;
    uint64_t mid = 6'234'567'000'000ULL;
    for (int u = 0; u < 20; ++u) {
        mid += 1'000'000ULL * static_cast<uint64_t>(u % 5);
        for (uint64_t l = 0; l < 20; ++l) ladder.push_back(mid - (l + 1) * 1'000'000ULL);
    }
    for (uint64_t u = 0; u < 20; ++u) for (int l = 0; l < 20; ++l) ts.push_back(1'790'000'000'000'000'000ULL + u * 50'000'000ULL);
    uint64_t x = 88172645463325252ULL;
    for (int i = 0; i < 400; ++i) { x ^= x << 13; x ^= x >> 7; x ^= x << 17; random.push_back(x % 5000); }
    struct Col { const char* name; std::vector<uint64_t>* v; unsigned type; };
    const Col cols[] = {{"ladder", &ladder, 1}, {"ts", &ts, 0}, {"qty", &random, 0}};
    for (const auto& c : cols) {
        for (cc::Transform t : {cc::Transform::kNone, cc::Transform::kFor, cc::Transform::kDelta}) {
            for (cc::Packing p : {cc::Packing::kSimple8b, cc::Packing::kRuns, cc::Packing::kBlocks, cc::Packing::kNarrow}) {
                for (cc::Compressor z : {cc::Compressor::kNone, cc::Compressor::kLz4, cc::Compressor::kZstd}) {
                    const cc::Encoding e{t, p, z};
                    if (z != cc::Compressor::kNone && (p == cc::Packing::kSimple8b || p == cc::Packing::kBlocks)) continue;
                    if (t == cc::Transform::kNone && c.v == &ladder) continue;
                    std::string block;
                    cc::encode_as(*c.v, e, 3, block);
                    std::string name = std::string(c.name) + "-" + e.name();
                    for (char& ch : name) if (ch == '/' || ch == '+') ch = '_';
                    put(dir, name, c.v->size(), c.type, block);
                }
            }
        }
    }
    // What a reader must refuse: a count one off, a block cut short, a declared length one off.
    std::string block;
    cc::encode_as(ladder, cc::Encoding{cc::Transform::kDelta, cc::Packing::kSimple8b, cc::Compressor::kNone}, 3, block);
    put(dir, "refuse-count-one-more", ladder.size() + 1, 1, block);
    put(dir, "refuse-cut-short", ladder.size(), 1, block.substr(0, block.size() / 2));
    put(dir, "refuse-empty", 0, 0, "");
    std::printf("written\n");
    return 0;
}
