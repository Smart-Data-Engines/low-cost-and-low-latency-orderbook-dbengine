#ifdef OB_USE_IO_URING

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include "orderbook/io_uring_server.hpp"
#include "orderbook/tcp_server.hpp"

#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

// ═══════════════════════════════════════════════════════════════════════════════
// Property 1: user_data encoding round-trip
// Validates: Requirements 4.2, 5.1
// ═══════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(IoUringUserData, EncodingRoundTrip, ()) {
    auto op = *rc::gen::element(ob::IoOp::ACCEPT, ob::IoOp::READ, ob::IoOp::WRITE);
    auto fd = *rc::gen::inRange<int>(0, (1 << 24) - 1);

    uint64_t encoded = ob::IoUringServer::encode_user_data(op, fd);
    RC_ASSERT(ob::IoUringServer::decode_op(encoded) == op);
    RC_ASSERT(ob::IoUringServer::decode_fd(encoded) == fd);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 3: Buffer pool alloc/free invariant
// Validates: Requirements 6.2, 6.3
// ═══════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(IoUringBufferPool, AllocFreeInvariant, ()) {
    auto max_sessions = *rc::gen::inRange(1, 65);
    auto operations = *rc::gen::container<std::vector<bool>>(rc::gen::arbitrary<bool>());

    ob::ServerConfig cfg{};
    cfg.max_sessions = max_sessions;

    // Use a unique temp directory for data_dir
    std::string tmp_dir = "/tmp/ob_test_bufpool_" + std::to_string(::getpid()) + "_" +
                          std::to_string(reinterpret_cast<uintptr_t>(&operations));
    cfg.data_dir = tmp_dir;
    std::filesystem::create_directories(tmp_dir);

    ob::IoUringServer server(cfg);

    std::vector<int> allocated_indices;
    int active_count = 0;

    for (bool do_alloc : operations) {
        if (do_alloc) {
            int idx = server.alloc_buffer();
            if (active_count >= max_sessions) {
                // Pool full — must return -1
                RC_ASSERT(idx == -1);
            } else {
                // Must return a valid, non-duplicate index
                RC_ASSERT(idx >= 0);
                RC_ASSERT(idx < max_sessions);
                // Check not already in use
                for (int existing : allocated_indices) {
                    RC_ASSERT(idx != existing);
                }
                allocated_indices.push_back(idx);
                ++active_count;
            }
        } else {
            // Free: only free if we have something allocated
            if (!allocated_indices.empty()) {
                int to_free = allocated_indices.back();
                allocated_indices.pop_back();
                server.free_buffer(to_free);
                --active_count;
            }
        }
        RC_ASSERT(active_count <= max_sessions);
    }

    // Cleanup
    std::filesystem::remove_all(tmp_dir);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Property 4: user_data no collisions
// Validates: Requirements 4.2, 5.1
// ═══════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(IoUringUserData, NoCollisions, ()) {
    auto op1 = *rc::gen::element(ob::IoOp::ACCEPT, ob::IoOp::READ, ob::IoOp::WRITE);
    auto fd1 = *rc::gen::inRange<int>(0, (1 << 24) - 1);
    auto op2 = *rc::gen::element(ob::IoOp::ACCEPT, ob::IoOp::READ, ob::IoOp::WRITE);
    auto fd2 = *rc::gen::inRange<int>(0, (1 << 24) - 1);

    // Only test when pairs are different
    RC_PRE(op1 != op2 || fd1 != fd2);

    uint64_t enc1 = ob::IoUringServer::encode_user_data(op1, fd1);
    uint64_t enc2 = ob::IoUringServer::encode_user_data(op2, fd2);
    RC_ASSERT(enc1 != enc2);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Unit tests: CLI parsing (SqpollIdleMs, RingSize, NoSqpoll)
// Validates: Requirements 13.1, 13.2, 13.3
// ═══════════════════════════════════════════════════════════════════════════════

namespace {

// Helper to convert a vector of strings to argc/argv for parse_cli_args
struct ArgvHelper {
    std::vector<std::string> args;
    std::vector<char*> ptrs;

    explicit ArgvHelper(std::initializer_list<std::string> list)
        : args(list)
    {
        // First element is program name
        ptrs.reserve(args.size() + 1);
        for (auto& a : args) {
            ptrs.push_back(a.data());
        }
        ptrs.push_back(nullptr);
    }

    int argc() const { return static_cast<int>(args.size()); }
    char** argv() { return ptrs.data(); }
};

} // anonymous namespace

TEST(IoUringCLI, SqpollIdleMs) {
    ArgvHelper helper({"ob_tcp_server", "--sqpoll-idle-ms", "500"});
    ob::ServerConfig config = ob::parse_cli_args(helper.argc(), helper.argv());
    EXPECT_EQ(config.uring_sqpoll_idle_ms, 500u);
}

TEST(IoUringCLI, RingSize) {
    ArgvHelper helper({"ob_tcp_server", "--ring-size", "512"});
    ob::ServerConfig config = ob::parse_cli_args(helper.argc(), helper.argv());
    EXPECT_EQ(config.uring_ring_size, 512u);
}

TEST(IoUringCLI, NoSqpoll) {
    ArgvHelper helper({"ob_tcp_server", "--no-sqpoll"});
    ob::ServerConfig config = ob::parse_cli_args(helper.argc(), helper.argv());
    EXPECT_TRUE(config.uring_no_sqpoll);
}

#endif // OB_USE_IO_URING
