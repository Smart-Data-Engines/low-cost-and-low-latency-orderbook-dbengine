// What one command costs a client, read against the round trip it travels on.
//
// A latency figure for a single command over loopback is mostly the round trip: measured on an
// m9g.xlarge, a `PING` answers in about 8.0 µs and a ten-level `BOOK` in 9.7 µs, so a `BOOK`
// figure quoted alone would be read as the cost of the read when it is mostly the cost of the
// socket. So this is run twice per round - once with `PING`, once with the command under test - and
// the difference is the number that belongs to the command. `scripts/measure_book_latency.py` does
// exactly that for `BOOK` (roadmap #145).
//
// A raw socket rather than `ob::OrderbookClient`, so the figure is about the server and the kernel
// rather than about a client library.
//
// Two controls on the instrument, because the first version of this probe measured the wrong
// thing and nothing in its latency said so: it assembled its command by appending to a `"PING"`
// default, sent `PINGBOOK SYM EX`, and timed `ERR unknown command` in both columns - two plausible
// numbers a hundred nanoseconds apart that read exactly like "the read is free". So the probe
// **refuses an error answer** rather than timing it, and it **reports the size of the answer** it
// timed, because a latency for a payload nobody states is a figure about nothing.
//
// Built and deliberately not run, like wire_load beside it: it needs a running node, and a source
// file nothing compiles is a source file that rots.
//
//     ./build-release/benchmarks/command_latency <port> <iterations> <server-pid> <command...>
//
// One JSON object on stdout: percentiles of the round trip, the server's CPU for the timed loop
// (read from /proc/<server-pid>/stat), and the size of one answer in bytes and in lines - the
// second so a driver can check the answer is the shape it asked for, not only that it arrived.

#include <algorithm>
#include <arpa/inet.h>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

namespace {

/// utime + stime of `pid` in seconds, or -1 if it cannot be read. Fields 14 and 15 of
/// /proc/<pid>/stat, counted after the closing parenthesis because the command name may itself
/// contain spaces and parentheses.
double process_cpu_seconds(int pid) {
    std::ifstream in("/proc/" + std::to_string(pid) + "/stat");
    std::string stat((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    const auto close = stat.rfind(')');
    if (close == std::string::npos) return -1.0;
    std::istringstream fields(stat.substr(close + 2));
    std::string skip;
    // Field 3 is the state; utime and stime are fields 14 and 15, i.e. the 12th and 13th after it.
    for (int i = 0; i < 11; ++i) fields >> skip;
    unsigned long long utime = 0;
    unsigned long long stime = 0;
    fields >> skip >> utime >> stime;
    return static_cast<double>(utime + stime) / static_cast<double>(::sysconf(_SC_CLK_TCK));
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 5) {
        std::fprintf(stderr, "usage: %s <port> <iterations> <server-pid> <command...>\n", argv[0]);
        return 2;
    }
    const int port       = std::atoi(argv[1]);
    const int iterations = std::atoi(argv[2]);
    const int server_pid = std::atoi(argv[3]);

    // Joined from the arguments with nothing in front: the default this used to append to is the
    // defect described at the top.
    std::string command;
    for (int i = 4; i < argc; ++i) {
        if (i > 4) command += ' ';
        command += argv[i];
    }
    const std::string label = command;
    command += '\n';
    // `PING` answers one line; an `OK` answer ends in a blank line, so a reader that took one
    // `recv` would time a fraction of a large answer and call it fast.
    const bool one_line = label == "PING";

    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { std::perror("socket"); return 1; }
    int one = 1;
    // One request in flight at a time, so Nagle has nothing to hold - set anyway, because the
    // server side of this exact question was #140.
    (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(static_cast<uint16_t>(port));
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::perror("connect");
        return 1;
    }

    static char buf[1 << 17];
    // The banner, which ends in a blank line.
    {
        std::string banner;
        while (banner.size() < 2 || banner.compare(banner.size() - 2, 2, "\n\n") != 0) {
            const ssize_t got = ::recv(fd, buf, sizeof(buf), 0);
            if (got <= 0) { std::fprintf(stderr, "no banner\n"); return 1; }
            banner.append(buf, static_cast<size_t>(got));
        }
    }

    std::string answer;
    size_t answer_bytes = 0;
    size_t answer_lines = 0;
    // One round trip. Returns false on a transport failure; an ERR answer is reported through
    // `answer` and refused by the caller, because timing an error is how this probe lied once.
    const auto round_trip = [&]() -> bool {
        const auto sent = ::send(fd, command.data(), command.size(), MSG_NOSIGNAL);
        if (sent != static_cast<ssize_t>(command.size())) return false;
        answer.clear();
        for (;;) {
            const ssize_t got = ::recv(fd, buf, sizeof(buf), 0);
            if (got <= 0) return false;
            answer.append(buf, static_cast<size_t>(got));
            const bool whole_line = answer.find('\n') != std::string::npos;
            if (whole_line && (one_line || answer.rfind("ERR", 0) == 0)) break;
            if (answer.size() >= 2 && answer.compare(answer.size() - 2, 2, "\n\n") == 0) break;
        }
        answer_bytes = answer.size();
        answer_lines = static_cast<size_t>(std::count(answer.begin(), answer.end(), '\n'));
        return true;
    };

    for (int i = 0; i < 200; ++i) {
        if (!round_trip()) { std::fprintf(stderr, "warm-up round trip failed\n"); return 1; }
    }
    if (answer.rfind("ERR", 0) == 0) {
        std::fprintf(stderr, "REFUSED: '%s' answers an error, and an error is not the thing being "
                             "measured: %s", label.c_str(), answer.c_str());
        return 3;
    }

    std::vector<long long> ns;
    ns.reserve(static_cast<size_t>(iterations));
    const double cpu_before = process_cpu_seconds(server_pid);
    const auto wall0 = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i) {
        const auto t0 = std::chrono::steady_clock::now();
        if (!round_trip()) { std::fprintf(stderr, "round trip %d failed\n", i); return 1; }
        const auto t1 = std::chrono::steady_clock::now();
        ns.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    }
    const auto wall1 = std::chrono::steady_clock::now();
    const double cpu_after = process_cpu_seconds(server_pid);
    ::close(fd);

    std::sort(ns.begin(), ns.end());
    const size_t n = ns.size();
    std::printf("{\"command\": \"%s\", \"iterations\": %d, \"p50_ns\": %lld, \"p99_ns\": %lld, "
                "\"min_ns\": %lld, \"max_ns\": %lld, \"wall_s\": %.3f, \"server_cpu_s\": %.3f, "
                "\"answer_bytes\": %zu, \"answer_lines\": %zu}\n",
                label.c_str(), iterations, ns[n / 2], ns[(n * 99) / 100], ns.front(), ns.back(),
                std::chrono::duration<double>(wall1 - wall0).count(),
                (cpu_before < 0 || cpu_after < 0) ? -1.0 : cpu_after - cpu_before, answer_bytes,
                answer_lines);
    return 0;
}
