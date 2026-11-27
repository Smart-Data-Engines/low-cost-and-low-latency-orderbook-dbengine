// #293: a large read answered off the client loop is written in its session's room.
//
// A read the engine hands back to be answered off the loop (#260) was written there in a buffer of its
// own: new memory for every read, grown by doubling from nothing - each growth a copy - and faulted in a
// page at a time, while the session's send buffer, the last answer's room, was let go. The room now goes
// with the read: the gate gives it back to the session, and the session lends it to the thread that
// answers. Held here on a real `TcpServer` in this process, over the port: whole reads on one connection
// answer the same bytes, and from the second on the threads that answer them off the loop fault in a
// small part of an answer's pages, where a buffer of their own faulted in twice as many as it has.

#include "orderbook/net.hpp"
#include "orderbook/tcp_server.hpp"
#include "test_ports.hpp"

#include <gtest/gtest.h>

#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>

namespace fs = std::filesystem;

namespace {

/// As `ob_tcp_server`'s main does (#59): a server in this process writes to peers that may have gone.
[[maybe_unused]] const bool kSigpipeIgnored = [] {
    std::signal(SIGPIPE, SIG_IGN);
    return true;
}();

/// The server's data in a directory of the process's own.
struct Dir {
    fs::path path;
    Dir() {
        path = fs::temp_directory_path() / ("ob_room_" + std::to_string(::getpid()));
        fs::create_directories(path);
    }
    ~Dir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

/// A `TcpServer` running on a thread of its own, stopped and joined when this goes.
class Running {
public:
    explicit Running(ob::ServerConfig config) : server_(std::make_unique<ob::TcpServer>(std::move(config))) {
        thread_ = std::thread([this] {
            try {
                server_->run();
            } catch (const std::exception& e) {
                ADD_FAILURE() << "the server stopped: " << e.what();
            }
        });
    }
    ~Running() {
        server_->shutdown();
        thread_.join();
    }
    Running(const Running&) = delete;
    Running& operator=(const Running&) = delete;

private:
    std::unique_ptr<ob::TcpServer> server_;
    std::thread                    thread_;
};

/// One connection speaking the line protocol by hand.
class Wire {
public:
    explicit Wire(int fd) : fd_(fd) {
        timeval tv{};
        tv.tv_sec = 60;
        ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }
    ~Wire() { ::close(fd_); }
    Wire(const Wire&) = delete;
    Wire& operator=(const Wire&) = delete;

    bool send_raw(const std::string& bytes) {
        return ::send(fd_, bytes.data(), bytes.size(), MSG_NOSIGNAL) == static_cast<ssize_t>(bytes.size());
    }
    std::string ask(const std::string& line) { return send_raw(line + "\n") ? answer() : "(send failed)"; }

    /// An answer that begins `OK` ends in a blank line; any other at its first line.
    std::string answer() {
        size_t from = 0;
        for (;;) {
            const bool ok  = buf_.rfind("OK", 0) == 0;
            const auto end = ok ? buf_.find("\n\n", from) : buf_.find('\n');
            if (end != std::string::npos) {
                const size_t len = end + (ok ? 2 : 1);
                std::string out  = buf_.substr(0, len);
                buf_.erase(0, len);
                return out;
            }
            from = buf_.size() > 0 ? buf_.size() - 1 : 0;
            char tmp[1 << 16];
            const ssize_t n = ::recv(fd_, tmp, sizeof(tmp), 0);
            if (n > 0) {
                buf_.append(tmp, static_cast<size_t>(n));
                continue;
            }
            if (n < 0 && errno == EINTR) continue;
            return "(" + std::string(n == 0 ? "closed" : std::strerror(errno)) + " after " +
                   std::to_string(buf_.size()) + " bytes)";
        }
    }

private:
    int         fd_;
    std::string buf_;
};

std::unique_ptr<Wire> to_port(uint16_t port) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    std::string why;
    for (;;) {
        const int fd = ob::net::dial("127.0.0.1", port, 1000, false, why, "test");
        if (fd >= 0) return std::make_unique<Wire>(fd);
        if (std::chrono::steady_clock::now() >= deadline) {
            ADD_FAILURE() << "no connection to port " << port << ": " << why;
            return nullptr;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}

/// The minor page faults of this process's threads that answer reads off the loop - `ob-read-<n>`
/// (#283) - read from /proc: the client's and every other thread's are not counted.
uint64_t off_loop_faults(int* threads) {
    uint64_t sum = 0;
    *threads = 0;
    for (const auto& t : fs::directory_iterator("/proc/self/task")) {
        std::ifstream comm(t.path() / "comm");
        std::string name;
        std::getline(comm, name);
        if (name.rfind("ob-read-", 0) != 0) continue;
        std::ifstream stat(t.path() / "stat");
        const std::string s((std::istreambuf_iterator<char>(stat)), std::istreambuf_iterator<char>());
        const size_t close = s.rfind(')');
        if (close == std::string::npos) continue;
        std::istringstream rest(s.substr(close + 1));
        std::string field;
        for (int i = 0; i < 7; ++i) rest >> field;   // state, ppid, pgrp, session, tty, tpgid, flags
        uint64_t minflt = 0;
        rest >> minflt;
        sum += minflt;
        ++*threads;
    }
    return sum;
}

}  // namespace

TEST(OffLoopRoom, AWholeReadAfterTheFirstIsWrittenInItsSessionsRoom) {
    Dir dir;
    ob::ServerConfig c;
    c.port               = static_cast<uint16_t>(ob::test::kPortsOffLoopRoom);
    c.bind               = "127.0.0.1";
    c.data_dir           = (dir.path / "data").string();
    c.io_threads         = 1;
    c.large_read_threads = 1;
    c.drain_timeout_ms   = 2000;
    Running server(c);
    auto w = to_port(c.port);
    ASSERT_TRUE(w);
    const std::string banner = w->answer();
    ASSERT_EQ(banner.rfind("OK ob_tcp_server v", 0), 0u) << banner;

    // 150 000 rows of one book, 1000 levels an update, on disk: the index counts more than the
    // kOffLoopReadRows a read is answered off the loop from (#260).
    for (int update = 0; update < 150; ++update) {
        std::string minsert = "MINSERT ROOM EX " + std::string(update % 2 ? "ask" : "bid") + " 1000\n";
        for (int i = 0; i < 1000; ++i) {
            minsert += std::to_string(100000 + update * 7 + i) + " " + std::to_string(i + 1) + " 1\n";
        }
        ASSERT_TRUE(w->send_raw(minsert));
        ASSERT_EQ(w->answer(), "OK\n\n") << "update " << update;
    }
    ASSERT_EQ(w->ask("FLUSH"), "OK\n\n");

    const std::string q = "SELECT * FROM 'ROOM'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999";
    const std::string first = w->ask(q);
    ASSERT_EQ(first.rfind("OK\n", 0), 0u) << first.substr(0, 200);
    ASSERT_EQ(static_cast<size_t>(std::count(first.begin(), first.end(), '\n')), 150000u + 3);
    int threads = 0;
    off_loop_faults(&threads);
    ASSERT_EQ(threads, 1) << "the premise: one thread answers reads off the loop";

    // The first read grew the room to its answer; each after it is written there. A buffer of the
    // read's own faulted in about twice the answer's pages, every read.
    const uint64_t pages = first.size() / static_cast<uint64_t>(::sysconf(_SC_PAGESIZE));
    for (int read = 2; read <= 3; ++read) {
        const uint64_t before = off_loop_faults(&threads);
        const std::string again = w->ask(q);
        const uint64_t faults = off_loop_faults(&threads) - before;
        EXPECT_TRUE(again == first) << "read " << read << " answered other bytes: " << again.size() << " for "
                                    << first.size();
        EXPECT_LT(faults, pages / 4) << "read " << read << " faulted in " << faults << " pages off the loop, "
                                     << "for an answer of " << pages;
    }
}
