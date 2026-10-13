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
// `OB_HOST=<IPv4 address>` measures a node on another host - the round trip a client in another
// rack pays, beside the loopback one (#217). Its CPU cannot be read from here: give 0 for the pid,
// and `server_cpu_s` reads -1, the value it always gave for a figure it could not take.
//
// `OB_UNIX_SOCKET=<path>` measures the node's Unix socket (`--unix-socket`, #52) instead of its
// port, which is then not used: the round trip of a client on the node's own host.
//
// `OB_RINGS=1`, beside `OB_UNIX_SOCKET`, moves the session to rings in shared memory (`RINGS`, #52
// step 2) and times the round trip through them; with `OB_SPIN=1` the probe polls the answer ring,
// without it it sleeps on its bell.
//
// `OB_SPIN=1` makes the probe poll for its answer rather than sleep in `recv()`: a client that
// blocks pays for its own wake-up, and on a host whose idle CPUs sleep deeply that is part of every
// figure, more of a short round trip than of a long one (#52).
//
// One JSON object on stdout: percentiles of the round trip, the server's CPU for the timed loop
// (read from /proc/<server-pid>/stat), and the size of one answer in bytes and in lines - the
// second so a driver can check the answer is the shape it asked for, not only that it arrived.

#include "orderbook/net.hpp"
#include "orderbook/ring.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sstream>
#include <string>
#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
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
    // As a JSON string: a `MINSERT` is several lines - one argument holding its level lines - and a
    // raw newline in the output would make the line unreadable as JSON.
    std::string json_label;
    for (const char c : label) {
        if (c == '\n') json_label += "\\n";
        else if (c == '"' || c == '\\') { json_label += '\\'; json_label += c; }
        else json_label += c;
    }
    // `PING` answers one line; an `OK` answer ends in a blank line, so a reader that took one
    // `recv` would time a fraction of a large answer and call it fast.
    const bool one_line = label == "PING";

    int fd = -1;
    const char* unix_path = std::getenv("OB_UNIX_SOCKET");
    if (unix_path != nullptr && *unix_path != '\0') {
        sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        if (std::strlen(unix_path) >= sizeof(addr.sun_path)) {
            std::fprintf(stderr, "OB_UNIX_SOCKET is longer than a socket address holds: %s\n",
                         unix_path);
            return 2;
        }
        std::memcpy(addr.sun_path, unix_path, std::strlen(unix_path) + 1);
        fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) { std::perror("socket"); return 1; }
        if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            std::perror("connect");
            return 1;
        }
    } else {
        fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) { std::perror("socket"); return 1; }
        int one = 1;
        // One request in flight at a time, so Nagle has nothing to hold - set anyway, because the
        // server side of this exact question was #140.
        (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port   = htons(static_cast<uint16_t>(port));
        const char* host = std::getenv("OB_HOST");
        if (host == nullptr || *host == '\0') host = "127.0.0.1";
        if (::inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
            std::fprintf(stderr, "OB_HOST is not an IPv4 address: %s\n", host);
            return 2;
        }
        if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            std::perror("connect");
            return 1;
        }
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

    // Rings (#52, step 2): asked for on the socket, then every round trip goes through them.
    const char* rings_env = std::getenv("OB_RINGS");
    const bool use_rings = rings_env != nullptr && std::string(rings_env) == "1";
    ob::ring::Mapping rings;
    ob::ring::Writer  commands;
    ob::ring::Reader  answers;
    int server_bell = -1, client_bell = -1;
    if (use_rings) {
        if (unix_path == nullptr || *unix_path == '\0') {
            std::fprintf(stderr, "OB_RINGS needs OB_UNIX_SOCKET\n");
            return 2;
        }
        static constexpr char ask[] = "RINGS\n";
        if (::send(fd, ask, sizeof(ask) - 1, MSG_NOSIGNAL) != static_cast<ssize_t>(sizeof(ask) - 1)) {
            std::perror("send");
            return 1;
        }
        std::string granted, why;
        std::vector<int> fds;
        if (!ob::net::recv_with_fds(fd, 4096, 3, 5000, granted, fds, why) ||
            granted.rfind("OK RINGS ", 0) != 0 || fds.size() != 3) {
            std::fprintf(stderr, "RINGS refused: %s%s\n", granted.c_str(), why.c_str());
            return 1;
        }
        const uint64_t size = std::strtoull(granted.c_str() + 9, nullptr, 10);
        server_bell = fds[1];
        client_bell = fds[2];
        rings = ob::ring::Mapping::attach(fds[0], size, why);
        if (!rings.valid()) {
            std::fprintf(stderr, "the rings' memory: %s\n", why.c_str());
            return 1;
        }
        commands = ob::ring::Writer(rings.to_server(), rings.to_server_data(), size);
        answers  = ob::ring::Reader(rings.to_client(), rings.to_client_data(), size);
    }

    std::string answer;
    size_t answer_bytes = 0;
    size_t answer_lines = 0;
    // How many reads the last answer took. An answer that arrives in two costs the client a second
    // sleep and wake-up, which is a tail rather than a median, so the count is reported (#52).
    size_t reads = 0;
    const char* spin_env = std::getenv("OB_SPIN");
    const bool spin = spin_env != nullptr && std::string(spin_env) == "1";
    // Through rings, poll this long before sleeping on the bell, as `ClientConfig::rings.spin_us`
    // does; 0 sleeps at once. Ignored with OB_SPIN=1, which never sleeps.
    const char* spin_us_env = std::getenv("OB_SPIN_US");
    const long spin_us = spin_us_env != nullptr ? std::strtol(spin_us_env, nullptr, 10) : 0;
    // One round trip. Returns false on a transport failure; an ERR answer is reported through
    // `answer` and refused by the caller, because timing an error is how this probe lied once.
    // The answer is complete: one line for PING and errors, a blank line ending anything else.
    const auto complete = [&]() {
        const bool whole_line = answer.find('\n') != std::string::npos;
        if (whole_line && (one_line || answer.rfind("ERR", 0) == 0)) return true;
        return answer.size() >= 2 && answer.compare(answer.size() - 2, 2, "\n\n") == 0;
    };
    const auto ring_trip = [&]() -> bool {
        size_t done = 0;
        while (done < command.size()) {
            const size_t n = commands.write(command.data() + done, command.size() - done);
            done += n;
            if (commands.broken()) return false;
            if (n > 0 && commands.publish()) ob::ring::ring_bell(server_bell);
        }
        answer.clear();
        reads = 0;
        const auto spin_end = std::chrono::steady_clock::now() + std::chrono::microseconds(spin_us);
        for (;;) {
            const size_t got = answers.read(buf, sizeof(buf));
            if (got > 0) {
                ++reads;
                answer.append(buf, got);
                if (answers.producer_waiting()) ob::ring::ring_bell(server_bell);
                if (complete()) break;
                continue;
            }
            if (answers.broken()) return false;
            if (spin || std::chrono::steady_clock::now() < spin_end) continue;
            if (!answers.prepare_to_sleep()) continue;
            pollfd p{client_bell, POLLIN, 0};
            if (::poll(&p, 1, 5000) == 0) return false;
            ob::ring::drain_bell(client_bell);
            answers.woke();
        }
        answer_bytes = answer.size();
        answer_lines = static_cast<size_t>(std::count(answer.begin(), answer.end(), '\n'));
        return true;
    };
    const auto round_trip = [&]() -> bool {
        if (use_rings) return ring_trip();
        const auto sent = ::send(fd, command.data(), command.size(), MSG_NOSIGNAL);
        if (sent != static_cast<ssize_t>(command.size())) return false;
        answer.clear();
        reads = 0;
        for (;;) {
            const ssize_t got = ::recv(fd, buf, sizeof(buf), spin ? MSG_DONTWAIT : 0);
            if (got < 0 && spin && (errno == EAGAIN || errno == EWOULDBLOCK)) continue;
            if (got <= 0) return false;
            ++reads;
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
    size_t split_answers = 0;
    const double cpu_before = process_cpu_seconds(server_pid);
    const auto wall0 = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i) {
        const auto t0 = std::chrono::steady_clock::now();
        if (!round_trip()) { std::fprintf(stderr, "round trip %d failed\n", i); return 1; }
        const auto t1 = std::chrono::steady_clock::now();
        if (reads > 1) ++split_answers;
        ns.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    }
    const auto wall1 = std::chrono::steady_clock::now();
    const double cpu_after = process_cpu_seconds(server_pid);
    ::close(fd);

    std::sort(ns.begin(), ns.end());
    const size_t n = ns.size();
    // p99.9 as well, for the reason pipelined_ingest reports it: a stall that comes once a flush
    // tick touches one round trip in hundreds, and p99 does not see it (#180).
    // The transport too, so a figure carries what it travelled on rather than leaving it to the
    // file it was saved in (#52).
    std::printf("{\"command\": \"%s\", \"transport\": \"%s\", \"spin\": %s, \"spin_us\": %ld, \"iterations\": %d, "
                "\"p50_ns\": %lld, \"p99_ns\": %lld, "
                "\"p999_ns\": %lld, \"min_ns\": %lld, \"max_ns\": %lld, \"wall_s\": %.3f, "
                "\"server_cpu_s\": %.3f, \"answer_bytes\": %zu, \"answer_lines\": %zu, "
                "\"split_answers\": %zu}\n",
                json_label.c_str(),
                use_rings ? "rings" : (unix_path != nullptr && *unix_path != '\0') ? "unix" : "tcp",
                spin ? "true" : "false", spin_us,
                iterations, ns[n / 2], ns[(n * 99) / 100], ns[(n * 999) / 1000],
                ns.front(), ns.back(),
                std::chrono::duration<double>(wall1 - wall0).count(),
                (cpu_before < 0 || cpu_after < 0) ? -1.0 : cpu_after - cpu_before, answer_bytes,
                answer_lines, split_answers);
    return 0;
}
