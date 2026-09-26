// A mesh catch-up in rounds (#178), driven through the real manager.
//
// A catch-up used to be one call: the whole retained WAL read on the io loop, under the manager's
// lock, with sending stopped after `max_catchup_bytes` - counting records the peer already had -
// and a snapshot said to take over that nothing ever sent to a node holding data. Every later
// catch-up stopped at the same place. These tests hold the rounds that replaced it: what one round
// reads and sends, that the next continues where it stopped, that a full send buffer pauses it and
// a drained one resumes it, what the peer's newest vector does to it, that it belongs to one
// connection, and what it says about ranges no WAL here can send.
//
// The manager is never started - no port, no thread - and frames are read off a socket pair, as
// in `test_mm_snapshot.cpp`, whose harness this shares.

#include <gtest/gtest.h>

#include "orderbook/engine.hpp"
#include "orderbook/hlc.hpp"
#include "orderbook/metrics.hpp"
#include "orderbook/multi_master.hpp"
#include "orderbook/version_vector.hpp"
#include "orderbook/wal.hpp"

#include "mm_test_peer.hpp"

#include <cstring>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

namespace {

using mm_test::Frame;
using mm_test::take_frames;
using mm_test::TmpDir;
using mm_test::WiredPeer;

constexpr uint16_t kSelf = 1;
constexpr uint16_t kPeer = 2;

/// A node whose mesh WAL is written by the test: the records a catch-up reads.
struct CatchupNode {
    TmpDir tmp;
    std::unique_ptr<ob::Engine> engine;
    std::unique_ptr<ob::WALWriter> wal;
    std::unique_ptr<ob::HybridLogicalClock> hlc;
    ob::MultiMasterConfig config;
    std::unique_ptr<ob::MultiMasterManager> mm;

    CatchupNode(size_t round_bytes, size_t watermark, size_t rotate_bytes = 64 << 10) {
        config.node_id                   = kSelf;
        config.replication_port          = 0;
        config.enabled                   = true;
        config.compress                  = false;
        config.max_catchup_bytes         = round_bytes;
        config.snapshot_low_watermark_bytes = watermark;
        config.anti_entropy_interval_sec = 30;
        engine = std::make_unique<ob::Engine>(tmp.path);
        engine->open();
        wal = std::make_unique<ob::WALWriter>(tmp.path + "/mm_wal", rotate_bytes);
        hlc = std::make_unique<ob::HybridLogicalClock>(kSelf);
        mm  = std::make_unique<ob::MultiMasterManager>(config, *engine, *wal, *hlc);
    }

    /// `n` DELTA records of one symbol from `origin`, numbered from `first`.
    void write(const char* symbol, uint16_t origin, uint64_t first, uint64_t n) {
        ob::Level level{};
        level.price = 100'000;
        level.qty   = 7;
        for (uint64_t i = 0; i < n; ++i) {
            ob::DeltaUpdate d{};
            std::strncpy(d.symbol, symbol, sizeof(d.symbol) - 1);
            std::strncpy(d.exchange, "EX", sizeof(d.exchange) - 1);
            d.sequence_number = first + i;
            d.timestamp_ns    = 1'700'000'000'000'000'000ULL + first + i;
            d.side            = ob::SIDE_BID;
            d.n_levels        = 1;
            wal->append_with_origin(d, &level, origin, hlc->tick_local());
        }
    }
};

/// A peer vector that says the peer holds `frontier` of each (key, origin) given - and, received,
/// says what it holds rather than asking for everything.
ob::PeerVector vector_of(const std::vector<ob::SequenceTracker::VectorEntry>& entries) {
    const auto bytes = ob::serialize_version_vector(entries, /*truncated=*/false);
    ob::PeerVector v;
    EXPECT_TRUE(v.deserialize(bytes.data(), bytes.size()));
    return v;
}

/// The (sequence, origin, symbol) of every DELTA frame the peer received since the last call.
struct Delivered {
    uint64_t    seq;
    uint16_t    origin;
    std::string symbol;
    bool operator==(const Delivered&) const = default;
};

void PrintTo(const Delivered& d, std::ostream* os) {
    *os << d.symbol << "/" << d.origin << "#" << d.seq;
}

std::vector<Delivered> received(WiredPeer& peer) {
    peer.collect();
    std::vector<Delivered> out;
    for (const Frame& f : take_frames(peer.inbox)) {
        if (f.hdr.record_type != ob::WAL_RECORD_DELTA) continue;
        ob::DeltaUpdate d{};
        std::memcpy(&d, f.payload.data(), sizeof(d));
        out.push_back(Delivered{f.hdr.sequence_number, f.hdr.origin_node_id, d.symbol});
    }
    return out;
}

std::vector<Delivered> expected(const char* symbol, uint16_t origin, uint64_t first, uint64_t last) {
    std::vector<Delivered> out;
    for (uint64_t s = first; s <= last; ++s) out.push_back(Delivered{s, origin, symbol});
    return out;
}

/// Read what the socket holds and let the manager write more, as the EPOLLOUT branch does, until
/// nothing is queued for the peer; bounded.
void drain(CatchupNode& node, WiredPeer& to, std::vector<Delivered>& got) {
    for (int i = 0; i < 100'000; ++i) {
        for (auto& d : received(to)) got.push_back(std::move(d));
        if (to.mgr(*node.mm).send_buf.empty()) break;
        node.mm->try_drain_send_buf_for_test(to.mgr(*node.mm));
    }
    for (auto& d : received(to)) got.push_back(std::move(d));
}

/// Rounds until the catch-up ends, draining between them as the io loop does; bounded.
std::vector<Delivered> run_to_end(CatchupNode& node, WiredPeer& to, int* rounds = nullptr) {
    std::vector<Delivered> got;
    int n = 0;
    for (; n < 10'000 && to.mgr(*node.mm).catchup.active; ++n) {
        node.mm->run_catchup_rounds_for_test();
        drain(node, to, got);
    }
    if (rounds) *rounds = n;
    drain(node, to, got);
    return got;
}

}  // namespace

TEST(MMCatchupRounds, ARoundReadsItsBudgetAndTheNextContinuesWhereItStopped) {
    // About 140 bytes a record on disk, so a round of 16 KiB is a hundred-odd records of 2 000.
    CatchupNode node(16 << 10, 64 << 20);
    node.write("AAA", kSelf, 1, 2000);
    WiredPeer to(kPeer);
    to.mgr(*node.mm).peer_vector = vector_of({});
    node.mm->start_catchup_for_test(to.mgr(*node.mm));

    ASSERT_TRUE(to.mgr(*node.mm).catchup.active);
    EXPECT_TRUE(node.mm->run_catchup_rounds_for_test()) << "a round that stopped at its budget "
                                                            "can run again at once";
    std::vector<Delivered> first;
    drain(node, to, first);
    EXPECT_GT(first.size(), 50u);
    EXPECT_LT(first.size(), 400u) << "one round read more than its budget";
    EXPECT_EQ(first, expected("AAA", kSelf, 1, first.size()));

    int rounds = 0;
    auto rest = run_to_end(node, to, &rounds);
    std::vector<Delivered> all = first;
    all.insert(all.end(), rest.begin(), rest.end());
    EXPECT_EQ(all, expected("AAA", kSelf, 1, 2000)) << "every record once, in the WAL's order";
    EXPECT_GT(rounds, 5) << "the premise: several rounds, so continuing is what was tested";
    EXPECT_FALSE(to.mgr(*node.mm).catchup.active);
}

TEST(MMCatchupRounds, AcrossRotationsEveryRecordGoesOnceInOrder) {
    CatchupNode node(8 << 10, 64 << 20, /*rotate_bytes=*/16 << 10);
    node.write("ROT", kSelf, 1, 1500);
    node.write("ROT", 3, 1, 500);                     // another origin's, which we hold too
    WiredPeer to(kPeer);
    to.mgr(*node.mm).peer_vector = vector_of({});
    node.mm->start_catchup_for_test(to.mgr(*node.mm));

    auto want = expected("ROT", kSelf, 1, 1500);
    const auto other = expected("ROT", 3, 1, 500);
    want.insert(want.end(), other.begin(), other.end());
    EXPECT_EQ(run_to_end(node, to), want);
}

TEST(MMCatchupRounds, AFullSendBufferPausesTheRoundsAndADrainedOneResumesThem) {
    // A watermark of 4 KiB and a peer that reads nothing: a round sends as much as the peer has
    // room for below the mark, and the rounds stop once the buffer is at it.
    CatchupNode node(8 << 20, 4 << 10);
    node.write("BUF", kSelf, 1, 3000);
    WiredPeer to(kPeer, /*tiny_buffers=*/true);
    to.mgr(*node.mm).peer_vector = vector_of({});
    node.mm->start_catchup_for_test(to.mgr(*node.mm));

    int rounds = 0;
    while (node.mm->run_catchup_rounds_for_test() && rounds < 1000) ++rounds;
    const size_t queued = to.mgr(*node.mm).send_buf.size();
    EXPECT_GE(queued, 4u << 10) << "the rounds stopped before the buffer reached the watermark";
    EXPECT_LT(queued, (4u << 10) + 1024u) << "and went a frame past it at most";
    EXPECT_TRUE(to.mgr(*node.mm).catchup.active) << "the premise: far from done";
    EXPECT_FALSE(node.mm->run_catchup_rounds_for_test())
        << "a round ran while the peer's buffer was at the watermark";
    EXPECT_EQ(to.mgr(*node.mm).send_buf.size(), queued);

    // What the EPOLLOUT branch does - write what the socket takes - and the rounds go on.
    EXPECT_EQ(run_to_end(node, to), expected("BUF", kSelf, 1, 3000));
}

TEST(MMCatchupRounds, WhatThePeerHoldsIsSkippedByItsNewestVector) {
    CatchupNode node(16 << 10, 64 << 20);
    node.write("VEC", kSelf, 1, 3000);
    WiredPeer to(kPeer);
    to.mgr(*node.mm).peer_vector = vector_of({{"VEC.EX", kSelf, 1000}});
    node.mm->start_catchup_for_test(to.mgr(*node.mm));

    // Rounds that only pass over what the peer holds send nothing and move on.
    std::vector<Delivered> got;
    for (int i = 0; i < 1000 && got.empty() && to.mgr(*node.mm).catchup.active; ++i) {
        node.mm->run_catchup_rounds_for_test();
        drain(node, to, got);
    }
    ASSERT_FALSE(got.empty());
    EXPECT_EQ(got.front().seq, 1001u) << "what the peer holds is not sent";

    // A vector arrives mid-way - reconciliation sends one every interval - saying the peer has
    // come as far as 2500 some other way. It does not start the catch-up again; it changes what
    // the rounds after it send.
    const ob::WalPosition before = to.mgr(*node.mm).catchup.next;
    to.mgr(*node.mm).peer_vector = vector_of({{"VEC.EX", kSelf, 2500}});
    node.mm->start_catchup_for_test(to.mgr(*node.mm));
    EXPECT_EQ(to.mgr(*node.mm).catchup.next.file_index, before.file_index);
    EXPECT_EQ(to.mgr(*node.mm).catchup.next.offset, before.offset)
        << "a vector arriving mid-way started the catch-up again";

    for (auto& d : run_to_end(node, to)) got.push_back(std::move(d));
    uint64_t last = 0;
    bool jumped = false;
    for (const auto& d : got) {
        EXPECT_GT(d.seq, last) << "sent twice, or out of order";
        if (d.seq > 2500 && last < 2500) jumped = true;
        last = d.seq;
    }
    EXPECT_TRUE(jumped) << "the newest vector did not move what was sent past 2500";
    EXPECT_EQ(got.back().seq, 3000u);
}

TEST(MMCatchupRounds, ACatchupBelongsToItsConnection) {
    CatchupNode node(16 << 10, 64 << 20);
    node.write("CON", kSelf, 1, 2000);
    WiredPeer to(kPeer);
    to.mgr(*node.mm).peer_vector = vector_of({});
    node.mm->start_catchup_for_test(to.mgr(*node.mm));
    node.mm->run_catchup_rounds_for_test();
    ASSERT_FALSE(received(to).empty());

    // The peer dropped and came back: a new connection on the same record.
    to.mgr(*node.mm).conn_id += 1000;
    node.mm->run_catchup_rounds_for_test();
    EXPECT_TRUE(received(to).empty()) << "a round sent on a connection its catch-up was not for";
    EXPECT_FALSE(to.mgr(*node.mm).catchup.active);

    // And that connection's own catch-up starts from the first record.
    node.mm->start_catchup_for_test(to.mgr(*node.mm));
    EXPECT_EQ(run_to_end(node, to), expected("CON", kSelf, 1, 2000));
}

TEST(MMCatchupRounds, RangesOlderThanTheWalAreNamedAndCounted) {
    CatchupNode node(1 << 20, 64 << 20);
    // This node holds VEC.EX from origin 3 up to 1000 - which its tracker says - but its WAL only
    // from 500: the rest is in segments, where no catch-up reads.
    node.engine->adopt_snapshot_sequence_state({{"OLD.EX", 3, 1000}}, {});
    node.write("OLD", 3, 500, 501);
    WiredPeer to(kPeer);
    to.mgr(*node.mm).peer_vector = vector_of({{"OLD.EX", 3, 100}});
    node.mm->start_catchup_for_test(to.mgr(*node.mm));
    ASSERT_EQ(to.mgr(*node.mm).catchup.lacks.size(), 1u) << "the premise: one range it lacks";

    const auto got = run_to_end(node, to);
    ASSERT_EQ(got.size(), 501u) << "first " << ::testing::PrintToString(got.front()) << " last "
                                << ::testing::PrintToString(got.back());
    EXPECT_EQ(got, expected("OLD", 3, 500, 1000)) << "what the WAL holds is sent all the same";
    EXPECT_EQ(node.engine->registry().counter_value("ob_mm_catchup_unfillable_total"), 1u);
    EXPECT_EQ(to.mgr(*node.mm).unfillable_ranges.ticks(), 1u);

    // The next reconciliation finds it again: counted again, one episode.
    node.mm->start_catchup_for_test(to.mgr(*node.mm));
    run_to_end(node, to);
    EXPECT_EQ(node.engine->registry().counter_value("ob_mm_catchup_unfillable_total"), 2u);
    EXPECT_EQ(to.mgr(*node.mm).unfillable_ranges.ticks(), 2u);
}

TEST(MMCatchupRounds, ARangeWhoseFirstNumberIsInTheWalIsNotCalledUnfillable) {
    // The control of the one above: the same peer, a WAL that holds the first number it lacks.
    CatchupNode node(1 << 20, 64 << 20);
    node.engine->adopt_snapshot_sequence_state({{"NEW.EX", 3, 1000}}, {});
    node.write("NEW", 3, 1, 1000);
    WiredPeer to(kPeer);
    to.mgr(*node.mm).peer_vector = vector_of({{"NEW.EX", 3, 100}});
    node.mm->start_catchup_for_test(to.mgr(*node.mm));
    EXPECT_EQ(run_to_end(node, to), expected("NEW", 3, 101, 1000));
    EXPECT_EQ(node.engine->registry().counter_value("ob_mm_catchup_unfillable_total"), 0u);
    EXPECT_EQ(to.mgr(*node.mm).unfillable_ranges.ticks(), 0u);
}
