// What a mesh node does with a receive buffer full of frames (#181).
//
// A catch-up delivers megabytes in one read, and `process_recv_buf()` erased every frame from the
// front of the buffer, moving the rest of it each time: quadratic in the frames of one read. A node
// being caught up at 100 000 records spent 96% of its time in memmove and took 123 s to apply what
// its peer had sent in 0.1 s. These tests hold the linear version to its answer - every whole frame
// handled, the part of one that has not arrived kept - and to a time a quadratic one cannot meet.

#include <gtest/gtest.h>

#include "orderbook/engine.hpp"
#include "orderbook/hlc.hpp"
#include "orderbook/multi_master.hpp"
#include "orderbook/wal.hpp"

#include "mm_test_peer.hpp"

#include <chrono>
#include <cstring>
#include <memory>
#include <vector>

namespace {

using mm_test::TmpDir;
using mm_test::WiredPeer;

struct ReceivingNode {
    TmpDir tmp;
    std::unique_ptr<ob::Engine> engine;
    std::unique_ptr<ob::WALWriter> wal;
    std::unique_ptr<ob::HybridLogicalClock> hlc;
    ob::MultiMasterConfig config;
    std::unique_ptr<ob::MultiMasterManager> mm;

    ReceivingNode() {
        config.node_id          = 1;
        config.replication_port = 0;
        config.enabled          = true;
        config.compress         = false;
        engine = std::make_unique<ob::Engine>(tmp.path);
        engine->open();
        wal = std::make_unique<ob::WALWriter>(tmp.path + "/mm_wal");
        hlc = std::make_unique<ob::HybridLogicalClock>(1);
        mm  = std::make_unique<ob::MultiMasterManager>(config, *engine, *wal, *hlc);
    }
};

/// `n` frames of a record type this node skips - the reserved wire-only range - so what is measured
/// is the buffer, not what a handler does with a frame.
std::vector<uint8_t> skipped_frames(size_t n) {
    ob::WALRecordV2 hdr{};
    hdr.record_type    = 250;
    hdr.version        = 1;
    hdr.origin_node_id = 2;
    hdr.payload_len    = 0;
    std::vector<uint8_t> out;
    out.reserve(n * (ob::MM_FRAME_HEADER_SIZE + sizeof(hdr)));
    for (size_t i = 0; i < n; ++i) ob::encode_frame(&hdr, sizeof(hdr), out);
    return out;
}

}  // namespace

TEST(MMReceiveBuffer, EveryWholeFrameIsHandledAndThePartOfOneThatHasNotArrivedIsKept) {
    ReceivingNode node;
    WiredPeer from(2);
    ob::PeerConnection& peer = from.mgr(*node.mm);
    peer.recv_buf = skipped_frames(1000);
    const std::vector<uint8_t> next = skipped_frames(1);
    peer.recv_buf.insert(peer.recv_buf.end(), next.begin(), next.begin() + 10);   // a part of one

    node.mm->process_recv_buf_for_test(peer);

    ASSERT_TRUE(peer.connected);
    ASSERT_EQ(peer.recv_buf.size(), 10u) << "the frames that arrived whole were not all consumed, "
                                            "or the part of the next one was not kept";
    EXPECT_EQ(std::memcmp(peer.recv_buf.data(), next.data(), 10), 0);

    // The rest of it arrives: it is the next frame, whole.
    peer.recv_buf.insert(peer.recv_buf.end(), next.begin() + 10, next.end());
    node.mm->process_recv_buf_for_test(peer);
    EXPECT_TRUE(peer.recv_buf.empty());
}

TEST(MMReceiveBuffer, AReadOfManyFramesIsHandledInLinearTime) {
    // 300 000 frames, 12.6 MB in one buffer. Erasing each from the front moves about 6.3 MB each
    // time, some 1.9 TB in all - about 90 s at the 21 GB/s the development machine moves memory,
    // far more under a sanitizer; consumed by an offset it is one pass, hundredths of a second. The
    // bound sits in that gap. The first version of this test had 100 000 frames and the quadratic
    // version took 10.1 s against a bound of 10: a mutation row survived it once.
    ReceivingNode node;
    WiredPeer from(2);
    ob::PeerConnection& peer = from.mgr(*node.mm);
    peer.recv_buf = skipped_frames(300'000);

    const auto started = std::chrono::steady_clock::now();
    node.mm->process_recv_buf_for_test(peer);
    const double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();

    EXPECT_TRUE(peer.recv_buf.empty());
    EXPECT_LT(seconds, 10.0) << "handling one read of 300 000 frames took " << seconds << " s";
    std::printf("300 000 frames handled in %.3f s\n", seconds);
}
