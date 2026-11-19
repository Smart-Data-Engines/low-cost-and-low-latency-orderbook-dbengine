// An open engine's own threads carry their names (#283).
//
// A thread nobody named carried the name of the one that started it: in `top -H`, `perf` and
// /proc/<pid>/task/*/comm the flush thread, the seal's sync and the WAL's removal were all `ob_tcp_server`,
// and a profile could not tell them apart. tests/test_thread_boundaries.cpp holds that every construction
// site in src/ names its thread; this holds what an open engine shows.

#include "orderbook/engine.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kOneHourNs = 3'600'000'000'000ULL;

struct TempDir {
    std::string path;
    explicit TempDir(const std::string& prefix)
        : path((fs::temp_directory_path() / (prefix + std::to_string(std::rand()))).string()) {
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

/// The names of this process's threads, as the kernel has them.
std::vector<std::string> thread_names() {
    std::vector<std::string> out;
    for (const auto& task : fs::directory_iterator("/proc/self/task")) {
        std::ifstream in(task.path() / "comm");
        std::string name;
        if (std::getline(in, name)) out.push_back(name);
    }
    std::sort(out.begin(), out.end());
    return out;
}

bool has(const std::vector<std::string>& names, const std::string& name) {
    return std::find(names.begin(), names.end(), name) != names.end();
}

}  // namespace

TEST(ThreadNames, AnOpenEnginesOwnThreadsHaveTheirNames) {
    TempDir dir("ob_thread_names_");
    const auto before = thread_names();
    ASSERT_FALSE(has(before, "ob-flush")) << "another engine's thread is alive in this process";
    auto engine = std::make_unique<ob::Engine>(dir.path, kOneHourNs, ob::FsyncPolicy::INTERVAL);
    engine->open();
    const auto open = thread_names();
    EXPECT_TRUE(has(open, "ob-flush")) << "the flush thread has no name of its own";
    EXPECT_TRUE(has(open, "ob-seal-sync")) << "the seal's sync has no name of its own";
    EXPECT_TRUE(has(open, "ob-wal-removal")) << "the WAL's removal has no name of its own";
    engine->close();
    // close() joins them; the kernel lets a joined thread's entry in /proc go a moment after the join
    // returns - the join wakes when the thread clears its id, before the task is released.
    const auto engines = [](const std::vector<std::string>& names) {
        return has(names, "ob-flush") || has(names, "ob-seal-sync") || has(names, "ob-wal-removal");
    };
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (engines(thread_names()) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_FALSE(engines(thread_names())) << "close() left one of the engine's threads running";
}
