// What the wire costs without Python in it.
//
// The comparative harness drives the engine from Python: one `MINSERT` round trip per book update,
// with the CSV parsed in the same process. Over 2,000,000 levels that client spent **4.537
// CPU-seconds against the server's 1.130**, so its ingest column bounds the protocol's cost rather
// than measuring it. This sends the same shape from C++ and reports both sides.
//
// It is what overturned the published claim that "the round trip is all of it". Measured on an
// m9g.xlarge at a matched 20,000,000 levels, one second of flush interval and storage on xfs:
//
//     in process  (bench_engine BM_IngestionThroughputBatched)   1014 ns/level wall,  67 ns CPU
//     over this wire                                             1059 ns/level wall, 525 ns CPU
//
// Wall-clock throughput is the same to within about 4%; the protocol costs roughly eight times the
// storage path's CPU per level and almost nothing in throughput. The write-up, the volume series
// and the caveats are in benchmarks/on-a-bigger-machine.md.
//
// Built and deliberately not run, like the TLS probes beside it: it needs a running node and a
// minute, so ctest is the wrong place - and a source file nothing compiles is a source file that
// rots. Two things to know before using it. Give the server the **same** `--flush-interval-ms` as
// whatever it is being compared against; a first attempt used one hour so that no flush would
// perturb the timing, which meant nothing was ever written to a segment and the run stalled at
// 1,000,000 levels with an empty log - which is roadmap #137. And use a **fresh** data directory
// per round: this sends the same event timestamps every time, a segment's identity is its time
// range, and a second round therefore produces a directory already in the index - roadmap #136.
//
//     ./build-release/benchmarks/wire_load <port> [updates] [levels] [symbols]

#include "orderbook/client.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <sys/resource.h>
#include <vector>

namespace {

double self_cpu() {
    struct rusage r {};
    ::getrusage(RUSAGE_SELF, &r);
    return r.ru_utime.tv_sec + r.ru_utime.tv_usec / 1e6
         + r.ru_stime.tv_sec + r.ru_stime.tv_usec / 1e6;
}

}  // namespace

int main(int argc, char** argv) {
    const uint16_t port    = argc > 1 ? static_cast<uint16_t>(std::atoi(argv[1])) : 9090;
    const int      updates = argc > 2 ? std::atoi(argv[2]) : 100000;
    const int      levels  = argc > 3 ? std::atoi(argv[3]) : 20;
    const int      symbols = argc > 4 ? std::atoi(argv[4]) : 50;

    ob::ClientConfig cfg;
    cfg.port = port;
    cfg.read_timeout_sec = 60.0;
    ob::OrderbookClient client(cfg);
    if (auto connected = client.connect(); !connected) {
        std::fprintf(stderr, "connect failed: %s\n", connected.error_message().c_str());
        return 1;
    }

    std::vector<ob::Level> book(static_cast<size_t>(levels));
    std::vector<std::string> names;
    names.reserve(static_cast<size_t>(symbols));
    for (int i = 0; i < symbols; ++i) {
        char buf[16];
        std::snprintf(buf, sizeof buf, "SYM%04d", i);
        names.emplace_back(buf);
    }

    const uint64_t base_ts = 1'700'000'000'000'000'000ULL;
    int64_t sent = 0;
    const double cpu0 = self_cpu();
    const auto t0 = std::chrono::steady_clock::now();
    for (int u = 0; u < updates; ++u) {
        for (int l = 0; l < levels; ++l) {
            book[static_cast<size_t>(l)].price = 5'000'000LL - l * 100LL - (u % 17);
            book[static_cast<size_t>(l)].qty   = 1'000ULL + static_cast<uint64_t>(l);
            book[static_cast<size_t>(l)].count = 1;
        }
        const auto& name = names[static_cast<size_t>(u) % names.size()];
        auto ok = client.minsert(name, "EX", ob::Side::BID, book.data(),
                                 static_cast<size_t>(levels),
                                 base_ts + static_cast<uint64_t>(u) * 1000ULL);
        if (!ok) {
            std::fprintf(stderr, "minsert failed at update %d: %s\n", u,
                         ok.error_message().c_str());
            return 1;
        }
        sent += levels;
    }
    const auto t1 = std::chrono::steady_clock::now();
    const double cpu1 = self_cpu();

    const double wall = std::chrono::duration<double>(t1 - t0).count();
    std::printf("updates=%d levels=%lld wall=%.3f s  %.0f levels/s  client CPU=%.3f s  "
                "%.0f levels/client-CPU-s\n",
                updates, static_cast<long long>(sent), wall, sent / wall, cpu1 - cpu0,
                sent / (cpu1 - cpu0));
    return 0;
}
