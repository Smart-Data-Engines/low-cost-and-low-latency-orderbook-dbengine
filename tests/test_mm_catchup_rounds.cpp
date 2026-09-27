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

#include <chrono>
#include <cstring>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

namespace {

using mm_test::Frame;
using mm_test::take_frames;
using mm_test::TmpDir;
using mm_test::WiredPeer;

constexpr uint16_t kSelf = 1;
constexpr uint16_t kPeer = 2;
constexpr uint64_t kNoAutoFlush = 3'600'000'000'000ULL;   // every tick is the test's own

/// A node whose mesh WAL is written by the test: the records a catch-up reads.
struct CatchupNode {
    TmpDir tmp;
    std::unique_ptr<ob::Engine> engine;
    std::unique_ptr<ob::WALWriter> wal;
    std::unique_ptr<ob::HybridLogicalClock> hlc;
    ob::MultiMasterConfig config;
    std::unique_ptr<ob::MultiMasterManager> mm;

    CatchupNode(size_t round_bytes, size_t watermark, size_t rotate_bytes = 64 << 10,
                uint64_t flush_interval_ns = 100'000'000ULL) {
        config.node_id                   = kSelf;
        config.replication_port          = 0;
        config.enabled                   = true;
        config.compress                  = false;
        config.max_catchup_bytes         = round_bytes;
        config.snapshot_low_watermark_bytes = watermark;
        config.anti_entropy_interval_sec = 30;
        engine = std::make_unique<ob::Engine>(tmp.path, flush_interval_ns);
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

/// `n` rows of `symbol` a client writes to the node's engine: its tracker's frontier moves with each,
/// and the copy of its vector a peer is compared with only at the next tick.
void write_rows(ob::Engine& engine, const char* symbol, uint64_t n, uint64_t first_ts) {
    for (uint64_t i = 0; i < n; ++i) {
        ob::DeltaUpdate d{};
        std::strncpy(d.symbol, symbol, sizeof(d.symbol) - 1);
        std::strncpy(d.exchange, "EX", sizeof(d.exchange) - 1);
        d.timestamp_ns = first_ts + i;
        d.side         = ob::SIDE_BID;
        d.n_levels     = 1;
        ob::Level level{};
        level.price = 100'000 + static_cast<int64_t>(i);
        level.qty   = 7;
        ASSERT_EQ(engine.apply_delta(d, &level), ob::OB_OK);
    }
}

/// The peer's vector arriving on its connection, as a frame the io loop reads off the socket.
void arrive(CatchupNode& node, WiredPeer& from,
            const std::vector<ob::SequenceTracker::VectorEntry>& entries) {
    const auto payload = ob::serialize_version_vector(entries, /*truncated=*/false);
    ob::WALRecordV2 hdr{};
    hdr.record_type    = ob::WAL_RECORD_VERSION_VECTOR;
    hdr.version        = 1;
    hdr.payload_len    = static_cast<uint16_t>(payload.size());
    hdr.origin_node_id = kPeer;
    std::vector<uint8_t> frame;
    ob::encode_frame_header(ob::MM_WALRECORD_V2_SIZE + payload.size(), frame);
    const auto* hb = reinterpret_cast<const uint8_t*>(&hdr);
    frame.insert(frame.end(), hb, hb + ob::MM_WALRECORD_V2_SIZE);
    frame.insert(frame.end(), payload.begin(), payload.end());
    ob::PeerConnection& peer = from.mgr(*node.mm);
    peer.recv_buf.insert(peer.recv_buf.end(), frame.begin(), frame.end());
    node.mm->process_recv_buf_for_test(peer);
}

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

TEST(MMCatchupRounds, EachPairIsFilteredByWhatThePeerHoldsOfIt) {
    // Two (symbol, origin) pairs in one round, the peer holding different amounts of each: what the
    // peer holds is looked up once a round for every pair the round read, not once for the first.
    CatchupNode node(1 << 20, 64 << 20);
    node.write("TWO", kSelf, 1, 600);
    node.write("TWO", 3, 1, 600);
    WiredPeer to(kPeer);
    to.mgr(*node.mm).peer_vector = vector_of({{"TWO.EX", kSelf, 500}, {"TWO.EX", 3, 100}});
    node.mm->start_catchup_for_test(to.mgr(*node.mm));

    auto want = expected("TWO", kSelf, 501, 600);
    const auto other = expected("TWO", 3, 101, 600);
    want.insert(want.end(), other.begin(), other.end());
    EXPECT_EQ(run_to_end(node, to), want);
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

TEST(MMCatchupRounds, AReconciliationDoesNotTakeAPeersOwnTimerForSilence) {
    // #183. A reconciliation sends this node's vector; the peer's own reconciliation sends its, on
    // its own timer, and a peer does not answer a vector with one of its own. A deadline armed by
    // the reconciliation expired whenever the two timers were more than the grace apart, and the
    // peer was treated as holding nothing - a catch-up from its last vector, which here is an
    // interval old and says 500 where live traffic has since given it everything. Measured on three
    // nodes reconciling every 5 s: 6-7 such catch-ups a run, each sending everything written since
    // the peer's last vector, and the whole of a 300 000-record catch-up again in two runs of three.
    CatchupNode node(1 << 20, 64 << 20);
    node.write("QUIET", kSelf, 1, 1000);
    WiredPeer to(kPeer);
    ob::PeerConnection& peer = to.mgr(*node.mm);
    peer.peer_vector     = vector_of({{"QUIET.EX", kSelf, 500}});   // its last vector
    peer.catchup_started = true;                                     // acted on when it came

    (void)node.mm->reconcile_with_peers();
    to.collect();
    const auto sent = take_frames(to.inbox);
    ASSERT_EQ(sent.size(), 1u) << "the premise: the reconciliation sends this node's vector";
    EXPECT_EQ(sent.front().hdr.record_type, ob::WAL_RECORD_VERSION_VECTOR);

    std::this_thread::sleep_for(std::chrono::milliseconds(ob::MM_VV_GRACE_MS + 200));
    node.mm->start_overdue_catchups_for_test();
    EXPECT_FALSE(peer.catchup.active)
        << "the peer's silence since the reconciliation was taken for holding nothing";
    std::vector<Delivered> got;
    node.mm->run_catchup_rounds_for_test();
    drain(node, to, got);
    EXPECT_TRUE(got.empty()) << got.size() << " records sent again from a vector an interval old";
}

TEST(MMCatchupRounds, APeerSilentSinceItsHandshakeIsSentEverything) {
    // The control of the one above, and the deadline's own case, which stays: after a handshake a
    // peer that has stated nothing is unknown, and everything retained is the safe direction.
    CatchupNode node(1 << 20, 64 << 20);
    node.write("MUTE", kSelf, 1, 300);
    WiredPeer to(kPeer);
    ob::PeerConnection& peer = to.mgr(*node.mm);
    peer.catchup_started    = false;   // as a handshake leaves it
    peer.vector_deadline_ms = 1;       // and its grace long past
    node.mm->start_overdue_catchups_for_test();
    ASSERT_TRUE(peer.catchup.active) << "a peer silent since its handshake was not caught up";
    EXPECT_EQ(run_to_end(node, to), expected("MUTE", kSelf, 1, 300));
}

// Part D of #180, found in PR #188's CI. A peer's vector is compared with a copy of this node's that
// a tick brings up to date, and "the peer lacks nothing" was concluded from whatever copy there
// was: a node back within a tick of the writes it missed was compared with one that did not have
// them yet, judged to hold them, and sent them only at the next reconciliation - 100 of 2 100 rows
// 3 s after it reconnected, and with a tick of 1 s in every run. The engine here ticks only when
// the test says so; the peer's rows are client writes to it (origin 0, as without a mesh), and the
// rounds read the same numbers from the WAL the test writes.

TEST(MMCatchupRounds, AVectorComparedWithACopyBehindTheTrackerIsDecidedAfterTheTick) {
    CatchupNode node(1 << 20, 64 << 20, 64 << 10, kNoAutoFlush);
    write_rows(*node.engine, "LAG", 100, 1'000'000'000ULL);
    node.engine->flush_tick_for_test();                        // the copy: 100
    write_rows(*node.engine, "LAG", 200, 2'000'000'000ULL);   // the tracker: 300; the copy still 100
    node.write("LAG", 0, 1, 300);
    WiredPeer from(kPeer);
    ob::PeerConnection& peer = from.mgr(*node.mm);
    peer.catchup_started    = false;   // as a handshake leaves it
    peer.vector_deadline_ms = 1;       // and its grace for a silent peer long past

    arrive(node, from, {{"LAG.EX", 0, 100}});   // what the copy says this node holds
    EXPECT_FALSE(peer.catchup_started) << "the peer was judged to lack nothing from a copy the "
                                          "tracker is 200 numbers ahead of";
    node.mm->start_overdue_catchups_for_test();
    EXPECT_FALSE(peer.catchup.active) << "a peer that has sent its vector was taken for silent";
    node.mm->recheck_deferred_vectors_for_test();
    EXPECT_FALSE(peer.catchup.active) << "decided before a tick brought the copy up to date";

    node.engine->flush_tick_for_test();
    EXPECT_FALSE(node.mm->recheck_deferred_vectors_for_test()) << "still waiting after the tick";
    ASSERT_TRUE(peer.catchup.active) << "the tick did not bring the catch-up the peer lacks";
    EXPECT_EQ(run_to_end(node, from), expected("LAG", 0, 101, 300));
}

TEST(MMCatchupRounds, AVectorComparedWithACopyUpToDateIsDecidedAtOnce) {
    // The control of the one above: the tick came before the vector, and nothing waits for another.
    CatchupNode node(1 << 20, 64 << 20, 64 << 10, kNoAutoFlush);
    write_rows(*node.engine, "NOW", 300, 1'000'000'000ULL);
    node.engine->flush_tick_for_test();
    node.write("NOW", 0, 1, 300);
    WiredPeer from(kPeer);
    ob::PeerConnection& peer = from.mgr(*node.mm);
    peer.catchup_started = false;

    arrive(node, from, {{"NOW.EX", 0, 100}});
    ASSERT_TRUE(peer.catchup.active) << "a copy up to date was waited for all the same";
    EXPECT_EQ(run_to_end(node, from), expected("NOW", 0, 101, 300));
}

TEST(MMCatchupRounds, APeerThatHoldsEverythingIsNotScannedWhateverTheCopy) {
    // And the other half of the shortcut, which stays: a peer holding what the tracker does is not
    // sent a scan of the WAL - from a copy up to date at once, and from one behind it after the tick.
    CatchupNode node(1 << 20, 64 << 20, 64 << 10, kNoAutoFlush);
    write_rows(*node.engine, "ALL", 300, 1'000'000'000ULL);
    node.engine->flush_tick_for_test();
    WiredPeer from(kPeer);
    ob::PeerConnection& peer = from.mgr(*node.mm);
    peer.catchup_started = false;
    arrive(node, from, {{"ALL.EX", 0, 300}});
    EXPECT_TRUE(peer.catchup_started) << "a peer lacking nothing waited, with the copy up to date";
    EXPECT_FALSE(peer.catchup.active) << "a peer lacking nothing was sent a scan";

    WiredPeer late(3);
    ob::PeerConnection& other = late.mgr(*node.mm);
    other.catchup_started = false;
    write_rows(*node.engine, "ALL", 1, 2'000'000'000ULL);     // the tracker: 301, the copy 300
    arrive(node, late, {{"ALL.EX", 0, 301}});
    EXPECT_FALSE(other.catchup_started) << "the premise: behind the tracker, it waits";
    node.engine->flush_tick_for_test();
    node.mm->recheck_deferred_vectors_for_test();
    EXPECT_TRUE(other.catchup_started);
    EXPECT_FALSE(other.catchup.active) << "a peer lacking nothing was sent a scan after the tick";
}

TEST(MMCatchupRounds, ACopyThatDoesNotCatchUpIsWaitedForOnlyTheGrace) {
    // No tick comes - a flush stuck on a device, say. The wait is bounded, and what ends it is the
    // catch-up: the safe direction, since the rounds filter by the peer's vector anyway.
    CatchupNode node(1 << 20, 64 << 20, 64 << 10, kNoAutoFlush);
    write_rows(*node.engine, "STUCK", 100, 1'000'000'000ULL);
    node.engine->flush_tick_for_test();
    write_rows(*node.engine, "STUCK", 200, 2'000'000'000ULL);
    node.write("STUCK", 0, 1, 300);
    WiredPeer from(kPeer);
    ob::PeerConnection& peer = from.mgr(*node.mm);
    peer.catchup_started = false;
    arrive(node, from, {{"STUCK.EX", 0, 100}});
    ASSERT_TRUE(node.mm->recheck_deferred_vectors_for_test()) << "the premise: it waits";

    std::this_thread::sleep_for(std::chrono::milliseconds(ob::MM_VV_GRACE_MS + 200));
    EXPECT_FALSE(node.mm->recheck_deferred_vectors_for_test()) << "waited past the grace";
    ASSERT_TRUE(peer.catchup.active) << "nothing ended a wait for a tick that did not come";
    EXPECT_EQ(run_to_end(node, from), expected("STUCK", 0, 101, 300));
}

TEST(MMCatchupRounds, ACopyRebuiltWholeIsUpToDate) {
    // A copy rebuilt from the whole vector - at a start, or when a snapshot replaces what the node
    // holds - reaches every listing so far, so what is compared with it is decided at once.
    CatchupNode node(1 << 20, 64 << 20, 64 << 10, kNoAutoFlush);
    write_rows(*node.engine, "OLD", 10, 1'000'000'000ULL);
    node.engine->adopt_snapshot_sequence_state({{"SNAP.EX", 3, 500}}, {});
    WiredPeer from(kPeer);
    ob::PeerConnection& peer = from.mgr(*node.mm);
    peer.catchup_started = false;
    arrive(node, from, {{"SNAP.EX", 3, 500}});
    EXPECT_TRUE(peer.catchup_started) << "a copy rebuilt whole was taken for one behind";
    EXPECT_FALSE(peer.catchup.active);
}

TEST(MMCatchupRounds, AWriteAfterTheVectorDoesNotPutTheDecisionOffAgain) {
    // What the wait is for is the tracker as the vector found it: a write after that goes to the
    // connected peer live, so the tick that brings the copy past it decides, whatever came since -
    // with writes that never stop, waiting for a copy with nothing newer would wait for good.
    CatchupNode node(1 << 20, 64 << 20, 64 << 10, kNoAutoFlush);
    write_rows(*node.engine, "LIVE", 300, 1'000'000'000ULL);
    node.engine->flush_tick_for_test();
    write_rows(*node.engine, "LIVE", 1, 2'000'000'000ULL);    // the tracker 301, the copy 300
    WiredPeer from(kPeer);
    ob::PeerConnection& peer = from.mgr(*node.mm);
    peer.catchup_started = false;
    arrive(node, from, {{"LIVE.EX", 0, 301}});
    ASSERT_FALSE(peer.catchup_started) << "the premise: behind the tracker, it waits";
    node.engine->flush_tick_for_test();
    write_rows(*node.engine, "LIVE", 1, 3'000'000'000ULL);    // after the tick: 302, the copy 301
    EXPECT_FALSE(node.mm->recheck_deferred_vectors_for_test());
    EXPECT_TRUE(peer.catchup_started) << "a write after the vector put the decision off again";
    EXPECT_FALSE(peer.catchup.active);
}
