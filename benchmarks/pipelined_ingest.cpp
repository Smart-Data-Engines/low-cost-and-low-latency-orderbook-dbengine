// Pipelined ingest over the wire: what one server does with K connections writing at once.
//
// Each connection is its own thread with its own socket and its own four symbols, sending a batch
// of `batch` MINSERTs of `levels` levels in one write and reading every answer before it sends the
// next - so a batch is one round trip, which is what a client that pipelines looks like (#141).
// What comes out, as one JSON line:
//
// * the aggregate rate in **levels** per second, because that is the unit the in-process benchmark
//   and every other table in this repository counts (pitfall 328 is what mixing units cost);
// * the batch round trip at p50, p99, p99.9 and its maximum - the last two because a stall that
//   comes once per flush interval touches one batch in hundreds and is invisible at p99;
// * the client's own CPU, so a result that is really the client's ceiling says so;
// * and the reason this exists - the server's CPU **per thread**, read from
//   /proc/<pid>/task/*/stat, so a plateau is attributed to the thread that is saturated rather than
//   guessed at. #146 was found this way: one thread at 0.89 of wall time, flat at 1, 2 and 4
//   connections.
//
// `wire_load` beside it answers a different question: one update per round trip through the C++
// client, on one connection. This one pipelines a whole batch per round trip over raw sockets, which
// is the only shape in which the server's per-batch costs - one send per answer before #146 - show.
//
// The batches are prebuilt, so the client's formatting is not in the measurement, and eight
// variants rotate so prices move between updates. An answer other than `OK` is refused with the
// first bytes of it, rather than counted: a probe timing error answers measures the error path.
//
//   pipelined_ingest <port> <server-pid> <connections> <batches-per-connection> <levels> <batch> [book]
//
// With `book` as the last argument each batch is `batch` BOOK queries instead, over the same four
// symbols per connection, which one MINSERT per side populates before the clock starts. That is
// the read half of the multi-reactor question: a read takes the engine's lock only to find the
// buffer, so it is the work N client loops can do at once.
//
// scripts/measure_pipelined_ingest.py runs it against fresh nodes in interleaved rounds.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

struct ThreadCpu {
    std::string name;
    double cpu_s = 0.0;
};

std::map<int, ThreadCpu> per_thread_cpu(int pid) {
    std::map<int, ThreadCpu> out;
    const double tck = static_cast<double>(::sysconf(_SC_CLK_TCK));
    for (const auto& e : std::filesystem::directory_iterator("/proc/" + std::to_string(pid) + "/task")) {
        std::ifstream in(e.path() / "stat");
        std::string stat((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        const auto open = stat.find('(');
        const auto close = stat.rfind(')');
        if (open == std::string::npos || close == std::string::npos) continue;
        ThreadCpu t;
        t.name = stat.substr(open + 1, close - open - 1);
        std::istringstream f(stat.substr(close + 2));
        std::string skip;
        for (int i = 0; i < 11; ++i) f >> skip;
        unsigned long long ut = 0, st = 0;
        f >> ut >> st;
        t.cpu_s = static_cast<double>(ut + st) / tck;
        out[std::stoi(e.path().filename().string())] = t;
    }
    return out;
}

double self_cpu() {
    std::ifstream in("/proc/self/stat");
    std::string stat((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    std::istringstream f(stat.substr(stat.rfind(')') + 2));
    std::string skip;
    for (int i = 0; i < 11; ++i) f >> skip;
    unsigned long long ut = 0, st = 0;
    f >> ut >> st;
    return static_cast<double>(ut + st) / static_cast<double>(::sysconf(_SC_CLK_TCK));
}

int connect_to(int port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(static_cast<uint16_t>(port));
    ::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) != 0) return -1;
    std::string banner;
    char buf[4096];
    while (banner.size() < 2 || banner.compare(banner.size() - 2, 2, "\n\n") != 0) {
        const ssize_t got = ::recv(fd, buf, sizeof(buf), 0);
        if (got <= 0) return -1;
        banner.append(buf, static_cast<size_t>(got));
    }
    return fd;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 7 || (argc == 8 && std::strcmp(argv[7], "book") != 0) || argc > 8) {
        std::fprintf(stderr, "usage: %s <port> <server-pid> <connections> <batches-per-connection> <levels> <batch> [book]\n", argv[0]);
        return 2;
    }
    const bool book_mode = argc == 8;
    const int port = std::atoi(argv[1]);
    const int pid = std::atoi(argv[2]);
    const int conns = std::atoi(argv[3]);
    const int batches = std::atoi(argv[4]);
    const int levels = std::atoi(argv[5]);
    const int batch = std::atoi(argv[6]);
    if (port <= 0 || pid <= 0 || conns < 1 || batches < 1 || levels < 1 || batch < 1) {
        std::fprintf(stderr, "every argument must be a positive number\n");
        return 2;
    }
    constexpr int kVariants = 8;   // distinct prebuilt batches, so prices move between updates

    std::vector<int> fds(static_cast<size_t>(conns));
    for (int c = 0; c < conns; ++c) {
        fds[static_cast<size_t>(c)] = connect_to(port);
        if (fds[static_cast<size_t>(c)] < 0) { std::fprintf(stderr, "connect %d failed\n", c); return 1; }
    }

    // Prebuilt so the client's formatting is not in the measurement: per connection, kVariants
    // buffers of `batch` MINSERTs over that connection's own four symbols.
    std::vector<std::vector<std::string>> payloads(static_cast<size_t>(conns));
    for (int c = 0; c < conns; ++c) {
        if (book_mode) {
            // One batch of BOOK queries, which does not change between rounds, and before it one
            // MINSERT per side and symbol so every book has `levels` levels on both sides.
            std::string seed;
            for (int sym = 0; sym < 4; ++sym) {
                for (const char* side : {"bid", "ask"}) {
                    char head[96];
                    std::snprintf(head, sizeof head, "MINSERT C%02dS%d EX %s %d\n", c, sym, side, levels);
                    seed += head;
                    for (int l = 0; l < levels; ++l) {
                        const long long price = side[0] == 'b' ? 5'000'000LL - l * 100 : 5'000'100LL + l * 100;
                        char line[64];
                        std::snprintf(line, sizeof line, "%lld %d 1\n", price, 1000 + l);
                        seed += line;
                    }
                }
            }
            const int fd = fds[static_cast<size_t>(c)];
            if (::send(fd, seed.data(), seed.size(), MSG_NOSIGNAL) != static_cast<ssize_t>(seed.size())) {
                std::fprintf(stderr, "seeding connection %d failed\n", c);
                return 1;
            }
            std::string acc;
            char sbuf[4096];
            int seeded = 0;
            while (seeded < 8) {
                const ssize_t got = ::recv(fd, sbuf, sizeof sbuf, 0);
                if (got <= 0) { std::fprintf(stderr, "seeding connection %d: no answer\n", c); return 1; }
                acc.append(sbuf, static_cast<size_t>(got));
                size_t pos = 0, hit;
                while ((hit = acc.find("OK\n\n", pos)) != std::string::npos) { ++seeded; pos = hit + 4; }
                acc.erase(0, pos);
            }
            std::string p;
            for (int b = 0; b < batch; ++b) {
                char q[64];
                std::snprintf(q, sizeof q, "BOOK C%02dS%d EX\n", c, b % 4);
                p += q;
            }
            for (int v = 0; v < kVariants; ++v) payloads[static_cast<size_t>(c)].push_back(p);
            continue;
        }
        for (int v = 0; v < kVariants; ++v) {
            std::string p;
            for (int b = 0; b < batch; ++b) {
                char head[96];
                const char* side = (b % 2 == 0) ? "bid" : "ask";
                std::snprintf(head, sizeof head, "MINSERT C%02dS%d EX %s %d\n", c, (b / 2) % 4, side, levels);
                p += head;
                for (int l = 0; l < levels; ++l) {
                    const long long price = (b % 2 == 0) ? 5'000'000LL - l * 100 - v : 5'000'100LL + l * 100 + v;
                    char line[64];
                    std::snprintf(line, sizeof line, "%lld %d 1\n", price, 1000 + l + v);
                    p += line;
                }
            }
            payloads[static_cast<size_t>(c)].push_back(std::move(p));
        }
    }

    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    std::atomic<bool> failed{false};
    std::vector<std::vector<long long>> lat(static_cast<size_t>(conns));
    std::vector<std::thread> threads;
    for (int c = 0; c < conns; ++c) {
        threads.emplace_back([&, c] {
            const int fd = fds[static_cast<size_t>(c)];
            auto& my = lat[static_cast<size_t>(c)];
            my.reserve(static_cast<size_t>(batches));
            std::vector<char> buf(1 << 16);
            ready.fetch_add(1);
            while (!go.load(std::memory_order_acquire)) {}
            for (int i = 0; i < batches && !failed.load(std::memory_order_relaxed); ++i) {
                const std::string& p = payloads[static_cast<size_t>(c)][static_cast<size_t>(i % kVariants)];
                const auto t0 = std::chrono::steady_clock::now();
                size_t off = 0;
                while (off < p.size()) {
                    const ssize_t n = ::send(fd, p.data() + off, p.size() - off, MSG_NOSIGNAL);
                    if (n <= 0) { failed = true; return; }
                    off += static_cast<size_t>(n);
                }
                int answered = 0;
                std::string acc;
                while (answered < batch) {
                    const ssize_t got = ::recv(fd, buf.data(), buf.size(), 0);
                    if (got <= 0) { failed = true; return; }
                    acc.append(buf.data(), static_cast<size_t>(got));
                    // A write answers `OK` and a blank line; a BOOK answers rows and then the
                    // blank line, so what ends an answer is the blank line in both.
                    size_t pos = 0, hit;
                    const char* end = book_mode ? "\n\n" : "OK\n\n";
                    const size_t end_len = book_mode ? 2 : 4;
                    while ((hit = acc.find(end, pos)) != std::string::npos) { ++answered; pos = hit + end_len; }
                    if (acc.find("ERR") != std::string::npos) {
                        std::fprintf(stderr, "REFUSED: connection %d got an error answer: %s\n", c, acc.substr(0, 200).c_str());
                        failed = true; return;
                    }
                    acc.erase(0, pos);
                }
                my.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                 std::chrono::steady_clock::now() - t0).count());
            }
        });
    }
    while (ready.load() < conns) {}
    const auto before = per_thread_cpu(pid);
    const double cli0 = self_cpu();
    const auto w0 = std::chrono::steady_clock::now();
    go.store(true, std::memory_order_release);
    for (auto& t : threads) t.join();
    const auto w1 = std::chrono::steady_clock::now();
    const double cli1 = self_cpu();
    const auto after = per_thread_cpu(pid);
    for (int fd : fds) ::close(fd);
    if (failed) return 1;

    const double wall = std::chrono::duration<double>(w1 - w0).count();
    std::vector<long long> all;
    for (auto& v : lat) all.insert(all.end(), v.begin(), v.end());
    std::sort(all.begin(), all.end());
    // In book mode a "level" is a level read back: every BOOK answers `levels` per side.
    const double total_levels = static_cast<double>(conns) * batches * batch * levels *
                                (book_mode ? 2 : 1);

    std::vector<std::pair<double, std::string>> busy;
    double server_total = 0.0;
    for (const auto& [tid, t] : after) {
        auto it = before.find(tid);
        const double d = t.cpu_s - (it == before.end() ? 0.0 : it->second.cpu_s);
        server_total += d;
        if (d > 0.005) busy.emplace_back(d, t.name + ":" + std::to_string(tid));
    }
    std::sort(busy.rbegin(), busy.rend());
    std::printf("{\"connections\": %d, \"batch\": %d, \"levels\": %d, \"wall_s\": %.3f, "
                "\"levels_per_s\": %.0f, \"batch_p50_us\": %.1f, \"batch_p99_us\": %.1f, "
                "\"batch_p999_us\": %.1f, \"batch_max_us\": %.1f, "
                "\"client_cpu_s\": %.2f, \"server_cpu_s\": %.2f, \"server_cores\": %.2f, \"threads\": [",
                conns, batch, levels, wall, total_levels / wall, all[all.size() / 2] / 1e3,
                all[(all.size() * 99) / 100] / 1e3, all[(all.size() * 999) / 1000] / 1e3,
                all.back() / 1e3, cli1 - cli0, server_total, server_total / wall);
    for (size_t i = 0; i < busy.size() && i < 8; ++i) {
        std::printf("%s{\"thread\": \"%s\", \"cpu_s\": %.2f, \"of_wall\": %.2f}", i ? ", " : "",
                    busy[i].second.c_str(), busy[i].first, busy[i].first / wall);
    }
    std::printf("]}\n");
    return 0;
}
