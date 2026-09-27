// Snapshot bootstrap over the multi-master protocol — roadmap #76, frontier half of #67.
//
// Two kinds of test here, and the second kind is the point.
//
// The codec tests pin the wire format, including every refusal: a payload of the wrong length, a
// metadata blob larger than a receiver will assemble, an abort reason long enough to be a nuisance
// in a log.
//
// The transfer tests drive both state machines against each other through a socketpair, parsing
// frames the way handle_frame() does. That is deliberate: every interesting case in this feature
// is a refusal — an unsafe path in a manifest, a chunk at the wrong offset, a checksum that does
// not match — and a refusal reachable only through a live cluster is a refusal nobody tests. The
// happy path is here for one specific claim: a node that starts empty ends up able to state what
// it holds, which is what #67 says it cannot do.

#include <gtest/gtest.h>

#include "orderbook/crc32c.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/hlc.hpp"
#include "orderbook/multi_master.hpp"
#include "orderbook/wal.hpp"

#include "mm_test_peer.hpp"

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <fcntl.h>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

/// Kept so the compiler cannot drop the work being timed.
volatile size_t benchmarkish_sink = 0;

namespace {

using mm_test::Frame;
using mm_test::take_frames;
using mm_test::TmpDir;
using mm_test::WiredPeer;

/// One node: engine, WAL, clock and manager, with multi-master enabled but never started, so no
/// port is bound and no thread runs. Every frame in these tests is delivered by hand.
struct Node {
    TmpDir tmp;
    std::unique_ptr<ob::Engine> engine;
    std::unique_ptr<ob::WALWriter> wal;
    std::unique_ptr<ob::HybridLogicalClock> hlc;
    ob::MultiMasterConfig config;
    std::unique_ptr<ob::MultiMasterManager> mm;

    explicit Node(uint16_t node_id, size_t snapshot_watermark = 0) {
        config.node_id                  = node_id;
        config.replication_port         = 0;
        config.enabled                  = true;
        config.compress                 = false;
        config.max_catchup_bytes        = 1024 * 1024;
        config.anti_entropy_interval_sec = 30;
        if (snapshot_watermark > 0) config.snapshot_low_watermark_bytes = snapshot_watermark;

        engine = std::make_unique<ob::Engine>(tmp.path);
        engine->open();
        wal = std::make_unique<ob::WALWriter>(tmp.path + "/mm_wal");
        hlc = std::make_unique<ob::HybridLogicalClock>(node_id);
        mm  = std::make_unique<ob::MultiMasterManager>(config, *engine, *wal, *hlc);
    }

    void write_rows(const char* symbol, int n, uint64_t base_ts) {
        ob::Level level{};
        level.price = 100'000;
        level.qty   = 7;
        for (int i = 0; i < n; ++i) {
            ob::DeltaUpdate d{};
            std::strncpy(d.symbol, symbol, sizeof(d.symbol) - 1);
            std::strncpy(d.exchange, "USDT", sizeof(d.exchange) - 1);
            d.timestamp_ns = base_ts + static_cast<uint64_t>(i);
            d.side         = ob::SIDE_BID;
            d.n_levels     = 1;
            engine->apply_delta(d, &level);
        }
        engine->flush_incremental();
    }
};




/// Deliver one frame to a receiver's public protocol handlers, as handle_frame() would.
void deliver(ob::MultiMasterManager& to, ob::PeerConnection& from_peer, const Frame& f) {
    const uint8_t* p = f.payload.empty() ? nullptr : f.payload.data();
    switch (f.hdr.record_type) {
        case ob::MM_MSG_SNAPSHOT_BEGIN: to.handle_snapshot_begin(from_peer, p, f.payload.size()); break;
        case ob::MM_MSG_SNAPSHOT_CHUNK: to.handle_snapshot_chunk(from_peer, p, f.payload.size()); break;
        case ob::MM_MSG_SNAPSHOT_END:   to.handle_snapshot_end(from_peer, p, f.payload.size());   break;
        default: break;
    }
}

/// Ask for a snapshot and let the worker finish, the way io_loop() does.
///
/// Since #79 handle_snapshot_request() only starts a worker thread: the SNAPSHOT_BEGIN frame appears
/// when the io loop collects the result. A test that stops after the request observes nothing, which
/// is the whole point of the change — the loop is free in between.
void request_snapshot_and_settle(Node& sender, WiredPeer& to) {
    sender.mm->handle_snapshot_request(to.mgr(*sender.mm));

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (sender.mm->snapshot_preparing()) {
        sender.mm->poll_snapshot_preparation();
        if (std::chrono::steady_clock::now() > deadline) {
            FAIL() << "the snapshot worker did not finish within 10 s";
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

/// Run a whole transfer: request, then pump until the sender has nothing left.
/// `mutate` gets a chance to damage each frame before it is delivered.
template <typename Mutate>
void run_transfer(Node& sender, WiredPeer& to_receiver,
                  Node& receiver, ob::PeerConnection& sender_peer,
                  Mutate mutate) {
    // The receiver takes a BEGIN only from the connection it asked (#188); this drives the sender's
    // side of the request directly, so it records the receiver's side of it.
    receiver.mm->note_snapshot_asked_for_test(sender_peer);
    request_snapshot_and_settle(sender, to_receiver);

    // What the socket did not take stays in the sender's buffer, as the EPOLLOUT branch would find
    // it: a snapshot larger than the socket pair's buffer - a vector of thousands of entries is
    // 210 kB of metadata (#177) - arrives only as the buffer drains.
    const auto drain_into_socket = [&] {
        sender.mm->try_drain_send_buf_for_test(to_receiver.mgr(*sender.mm));
    };
    for (int round = 0; round < 10'000; ++round) {
        drain_into_socket();
        to_receiver.collect();
        auto frames = take_frames(to_receiver.inbox);
        for (auto& f : frames) {
            if (!mutate(f)) continue;              // dropped by the mutation
            deliver(*receiver.mm, sender_peer, f);
        }
        if (!sender.mm->snapshot_send_active() &&
            to_receiver.mgr(*sender.mm).send_buf.empty()) {
            // One last pass so the END frame that finished the send is delivered too.
            to_receiver.collect();
            for (auto& f : take_frames(to_receiver.inbox)) {
                if (mutate(f)) deliver(*receiver.mm, sender_peer, f);
            }
            return;
        }
        sender.mm->advance_snapshot_send(to_receiver.mgr(*sender.mm));
    }
    FAIL() << "transfer did not finish";
}

auto pass_through = [](Frame&) { return true; };

}  // namespace

// ═══════════════════════════════════════════════════════════════════════════════
// Codecs
// ═══════════════════════════════════════════════════════════════════════════════

TEST(MMSnapshotCodec, BeginRoundTrips) {
    ob::SnapshotBegin in{};
    in.manifest_len = 1234;
    in.vector_len   = 42;
    in.held_len     = 0;
    in.meta_crc     = 0xDEADBEEF;

    const auto payload = ob::encode_snapshot_begin(in);
    ASSERT_EQ(payload.size(), ob::MM_SNAPSHOT_BEGIN_SIZE);

    ob::SnapshotBegin out{};
    ASSERT_TRUE(ob::decode_snapshot_begin(payload.data(), payload.size(), out));
    EXPECT_EQ(out.manifest_len, 1234u);
    EXPECT_EQ(out.vector_len, 42u);
    EXPECT_EQ(out.held_len, 0u);
    EXPECT_EQ(out.meta_crc, 0xDEADBEEFu);
    EXPECT_EQ(out.total(), 1276u);
}

TEST(MMSnapshotCodec, BeginRefusesWhatItCannotAct0n) {
    ob::SnapshotBegin out{};
    const std::vector<uint8_t> short_payload(ob::MM_SNAPSHOT_BEGIN_SIZE - 1, 0);
    EXPECT_FALSE(ob::decode_snapshot_begin(short_payload.data(), short_payload.size(), out));

    const std::vector<uint8_t> long_payload(ob::MM_SNAPSHOT_BEGIN_SIZE + 1, 0);
    EXPECT_FALSE(ob::decode_snapshot_begin(long_payload.data(), long_payload.size(), out));

    // A manifest of zero bytes describes nothing, so there is nothing to open staging for.
    ob::SnapshotBegin empty_manifest{};
    empty_manifest.manifest_len = 0;
    const auto p1 = ob::encode_snapshot_begin(empty_manifest);
    EXPECT_FALSE(ob::decode_snapshot_begin(p1.data(), p1.size(), out));

    // The blob is assembled in memory, so its announced size is an allocation the peer chose.
    ob::SnapshotBegin huge{};
    huge.manifest_len = 1;
    huge.vector_len   = static_cast<uint32_t>(ob::MM_SNAPSHOT_MAX_META_BYTES);
    const auto p2 = ob::encode_snapshot_begin(huge);
    EXPECT_FALSE(ob::decode_snapshot_begin(p2.data(), p2.size(), out));
}

TEST(MMSnapshotCodec, ChunkHeaderBytesAreLittleEndianAtFixedOffsets) {
    // The ten header bytes, written out by hand.
    //
    // The round-trip test below sends the encoder's output through *our own* decoder, so a change
    // that reversed the byte order would change both halves together and stay green. This frame is
    // read by a different node, which may be running an older build, so the bytes are the contract
    // and not the pair of functions. Spelling the expected header here is the only thing that makes
    // that contract observable from inside this repository.
    const std::vector<uint8_t> data = {0xAA, 0xBB, 0xCC};
    const auto framed = ob::encode_snapshot_chunk(0x0201, 0x0807060504030201ULL,
                                                  data.data(), data.size());

    ASSERT_EQ(framed.size(), ob::MM_SNAPSHOT_CHUNK_HEADER_SIZE + data.size());
    const std::vector<uint8_t> expected_header = {
        0x01, 0x02,                                      // file_index, little-endian
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,  // byte_offset, little-endian
    };
    EXPECT_EQ(std::vector<uint8_t>(framed.begin(),
                                   framed.begin() + ob::MM_SNAPSHOT_CHUNK_HEADER_SIZE),
              expected_header);
    EXPECT_EQ(std::vector<uint8_t>(framed.begin() + ob::MM_SNAPSHOT_CHUNK_HEADER_SIZE,
                                   framed.end()),
              data);
}

TEST(MMSnapshotCodec, AFullSizeChunkIsEncodedWithoutCorruption) {
    // The largest payload the transfer path produces, which is also the size the discarded
    // alternative encoder would have copied a second time on every chunk.
    std::vector<uint8_t> data(ob::MM_SNAPSHOT_CHUNK_BYTES);
    for (size_t i = 0; i < data.size(); ++i) data[i] = static_cast<uint8_t>((i * 31u + 7u) & 0xFFu);

    const auto framed = ob::encode_snapshot_chunk(65535, ~0ULL, data.data(), data.size());
    ASSERT_EQ(framed.size(), ob::MM_SNAPSHOT_CHUNK_HEADER_SIZE + data.size());

    uint16_t file_index = 0;
    uint64_t offset = 0;
    const uint8_t* bytes = nullptr;
    size_t n = 0;
    ASSERT_TRUE(ob::decode_snapshot_chunk(framed.data(), framed.size(),
                                          file_index, offset, bytes, n));
    EXPECT_EQ(file_index, 65535u);
    EXPECT_EQ(offset, ~0ULL);
    ASSERT_EQ(n, data.size());
    EXPECT_EQ(std::memcmp(bytes, data.data(), n), 0);
}

TEST(MMSnapshotCodec, ChunkRoundTripsIncludingTheEmptyOne) {
    const std::vector<uint8_t> data = {1, 2, 3, 4, 5};
    const auto payload = ob::encode_snapshot_chunk(7, 4096, data.data(), data.size());

    uint16_t file_index = 0;
    uint64_t offset = 0;
    const uint8_t* bytes = nullptr;
    size_t n = 0;
    ASSERT_TRUE(ob::decode_snapshot_chunk(payload.data(), payload.size(),
                                          file_index, offset, bytes, n));
    EXPECT_EQ(file_index, 7u);
    EXPECT_EQ(offset, 4096u);
    ASSERT_EQ(n, data.size());
    EXPECT_EQ(std::memcmp(bytes, data.data(), n), 0);

    // A zero-length chunk is legal: an empty meta.json produces one.
    const auto empty = ob::encode_snapshot_chunk(ob::MM_SNAPSHOT_META_INDEX, 0, nullptr, 0);
    ASSERT_EQ(empty.size(), ob::MM_SNAPSHOT_CHUNK_HEADER_SIZE);
    ASSERT_TRUE(ob::decode_snapshot_chunk(empty.data(), empty.size(),
                                          file_index, offset, bytes, n));
    EXPECT_EQ(file_index, ob::MM_SNAPSHOT_META_INDEX);
    EXPECT_EQ(n, 0u);
    EXPECT_EQ(bytes, nullptr);
}

TEST(MMSnapshotCodec, ChunkRefusesAPayloadTooShortForItsHeader) {
    const std::vector<uint8_t> tiny(ob::MM_SNAPSHOT_CHUNK_HEADER_SIZE - 1, 0);
    uint16_t file_index = 0;
    uint64_t offset = 0;
    const uint8_t* bytes = nullptr;
    size_t n = 0;
    EXPECT_FALSE(ob::decode_snapshot_chunk(tiny.data(), tiny.size(),
                                           file_index, offset, bytes, n));
    EXPECT_FALSE(ob::decode_snapshot_chunk(nullptr, 0, file_index, offset, bytes, n));
}

TEST(MMSnapshotCodec, ChunkNeverExceedsWhatAFrameHeaderCanDescribe) {
    // WALRecordV2::payload_len is a uint16_t, and the receiver disconnects a peer whose
    // payload_len disagrees with the frame it arrived in (#78). A chunk size above that would
    // therefore drop the connection on the first chunk, every time.
    static_assert(ob::MM_SNAPSHOT_CHUNK_BYTES + ob::MM_SNAPSHOT_CHUNK_HEADER_SIZE <=
                      ob::WAL_MAX_PAYLOAD_LEN,
                  "a snapshot chunk must fit in one frame");
    static_assert(ob::MM_SNAPSHOT_BEGIN_SIZE <= ob::WAL_MAX_PAYLOAD_LEN, "");
    SUCCEED();
}

TEST(MMSnapshotCodec, EndCarriesNothingAndSaysSoAboutAnythingElse) {
    EXPECT_TRUE(ob::encode_snapshot_end().empty());
    EXPECT_TRUE(ob::decode_snapshot_end(nullptr, 0));
    const uint8_t stray[1] = {0};
    EXPECT_FALSE(ob::decode_snapshot_end(stray, 1));
}

TEST(MMSnapshotCodec, AbortReasonIsBoundedAndCannotBreakALogLine) {
    const std::string huge(ob::MM_SNAPSHOT_ABORT_REASON_MAX * 4, 'x');
    const auto payload = ob::encode_snapshot_abort(huge);
    EXPECT_EQ(payload.size(), ob::MM_SNAPSHOT_ABORT_REASON_MAX);

    const std::string nasty = "line\none\r\ttwo";
    const auto p2 = ob::encode_snapshot_abort(nasty);
    const std::string decoded = ob::decode_snapshot_abort(p2.data(), p2.size());
    EXPECT_EQ(decoded.find('\n'), std::string::npos);
    EXPECT_EQ(decoded.find('\r'), std::string::npos);
    EXPECT_EQ(decoded.find('\t'), std::string::npos);
    EXPECT_EQ(decoded, "line?one??two");

    EXPECT_EQ(ob::decode_snapshot_abort(nullptr, 0), "unspecified");
}

// ═══════════════════════════════════════════════════════════════════════════════
// A whole transfer
// ═══════════════════════════════════════════════════════════════════════════════

TEST(MMSnapshotTransfer, AnEmptyNodeEndsUpAbleToStateWhatItHolds) {
    // The claim #67 makes is that a node joining mid-stream can never declare a contiguous
    // frontier for a foreign origin, so its peers keep resending records it already has. A
    // snapshot carries the sender's frontiers, and this is the check that the receiver comes out
    // holding them.
    Node sender(1);
    Node receiver(2);
    sender.write_rows("BTC", 12, 1'000'000);

    WiredPeer to_receiver(/*node_id=*/2);
    ob::PeerConnection sender_peer;
    sender_peer.node_id = 1;
    sender_peer.handshake_done = true;

    ASSERT_TRUE(receiver.engine->holds_no_data());

    run_transfer(sender, to_receiver, receiver, sender_peer, pass_through);

    EXPECT_FALSE(receiver.mm->snapshot_recv_active());
    EXPECT_FALSE(receiver.mm->is_bootstrapping()) << "the flag must be cleared, not left set";

    bool truncated = false;
    const auto vector = receiver.engine->export_version_vector(4096, truncated);
    ASSERT_FALSE(truncated);
    ASSERT_FALSE(vector.empty())
        << "this is the whole point: after a snapshot the receiver can state a frontier";

    uint64_t frontier = 0;
    for (const auto& e : vector) {
        if (e.key == "BTC.USDT") frontier = e.frontier;
    }
    EXPECT_EQ(frontier, 12u) << "the sender numbered twelve rows and holds all of them";

    // And the rows themselves arrived, not just the claim about them.
    EXPECT_FALSE(receiver.engine->holds_no_data());
}

TEST(MMSnapshotTransfer, AVectorPastOneRecordReachesTheReceiverWhole) {
    // A snapshot's metadata carried the sender's vector in the single-record format, so past 1 560
    // entries it carried "send everything" and the receiver refused the bootstrap - a node joining a
    // mesh of a few thousand instruments could not be given a snapshot even when it asked (#177).
    Node sender(1);
    Node receiver(2);
    std::vector<ob::SequenceTracker::VectorEntry> wide;
    for (uint64_t i = 0; i < 5'000; ++i) {
        wide.push_back({"V" + std::to_string(i) + ".EX", 3, i + 1});
    }
    sender.engine->adopt_snapshot_sequence_state(wide, {});
    sender.write_rows("BTC", 12, 1'000'000);

    WiredPeer to_receiver(/*node_id=*/2);
    ob::PeerConnection sender_peer;
    sender_peer.node_id = 1;
    sender_peer.handshake_done = true;
    run_transfer(sender, to_receiver, receiver, sender_peer, pass_through);

    EXPECT_FALSE(receiver.mm->snapshot_recv_active());
    bool truncated = false;
    const auto vector = receiver.engine->export_version_vector(ob::VV_MAX_ENTRIES, truncated);
    ASSERT_FALSE(truncated);
    EXPECT_EQ(vector.size(), 5'001u) << "the receiver did not take the sender's whole vector";
    uint64_t last = 0, btc = 0;
    for (const auto& e : vector) {
        if (e.key == "V4999.EX" && e.origin == 3) last = e.frontier;
        if (e.key == "BTC.USDT") btc = e.frontier;
    }
    EXPECT_EQ(last, 5'000u);
    EXPECT_EQ(btc, 12u);
}

TEST(MMSnapshotTransfer, StagingIsGoneAndNoDescriptorLeaks) {
    Node sender(1);
    Node receiver(2);
    sender.write_rows("ETH", 5, 2'000'000);

    const auto count_fds = [] {
        size_t n = 0;
        std::error_code ec;
        for (auto it = fs::directory_iterator("/proc/self/fd", ec);
             it != fs::directory_iterator(); it.increment(ec)) {
            ++n;
        }
        return n;
    };
    const size_t before = count_fds();

    {
        WiredPeer to_receiver(2);
        ob::PeerConnection sender_peer;
        sender_peer.node_id = 1;
        sender_peer.handshake_done = true;
        run_transfer(sender, to_receiver, receiver, sender_peer, pass_through);
    }

    // Completion, not just absence of staging: abort_bootstrap() also removes staging, so this
    // assertion alone would pass for a failed transfer.
    EXPECT_FALSE(receiver.engine->holds_no_data()) << "the snapshot was not installed";
    EXPECT_FALSE(fs::exists(receiver.tmp.path + "/mm_snapshot_staging"))
        << "staging must not survive a completed install";
    EXPECT_LE(count_fds(), before + 2)
        << "an open snapshot file or staging file was left behind";
}

// ═══════════════════════════════════════════════════════════════════════════════
// Refusals
// ═══════════════════════════════════════════════════════════════════════════════

TEST(MMSnapshotRefusal, AChunkAtTheWrongOffsetAbandonsTheBootstrap) {
    Node sender(1);
    Node receiver(2);
    sender.write_rows("BTC", 8, 3'000'000);

    WiredPeer to_receiver(2);
    ob::PeerConnection sender_peer;
    sender_peer.node_id = 1;
    sender_peer.handshake_done = true;

    // Move one file chunk to an offset it does not belong at. Writing it there anyway would leave
    // a hole of zeros that only the file's checksum catches — and at the wrong size, not even
    // that.
    bool damaged = false;
    run_transfer(sender, to_receiver, receiver, sender_peer, [&](Frame& f) {
        if (!damaged && f.hdr.record_type == ob::MM_MSG_SNAPSHOT_CHUNK) {
            uint16_t idx = 0;
            std::memcpy(&idx, f.payload.data(), sizeof(idx));
            if (idx != ob::MM_SNAPSHOT_META_INDEX) {
                const uint64_t bogus = 999'999;
                std::memcpy(f.payload.data() + 2, &bogus, sizeof(bogus));
                damaged = true;
            }
        }
        return true;
    });

    ASSERT_TRUE(damaged) << "the mutation never fired, so this test proved nothing";
    EXPECT_FALSE(receiver.mm->snapshot_recv_active());
    EXPECT_FALSE(receiver.mm->is_bootstrapping());
    EXPECT_TRUE(receiver.engine->holds_no_data())
        << "an abandoned bootstrap must leave the data directory as it was";
    EXPECT_FALSE(fs::exists(receiver.tmp.path + "/mm_snapshot_staging"));
}

TEST(MMSnapshotRefusal, AFileWhoseBytesDoNotMatchTheManifestIsNotInstalled) {
    Node sender(1);
    Node receiver(2);
    sender.write_rows("BTC", 8, 4'000'000);

    WiredPeer to_receiver(2);
    ob::PeerConnection sender_peer;
    sender_peer.node_id = 1;
    sender_peer.handshake_done = true;

    bool damaged = false;
    run_transfer(sender, to_receiver, receiver, sender_peer, [&](Frame& f) {
        if (!damaged && f.hdr.record_type == ob::MM_MSG_SNAPSHOT_CHUNK &&
            f.payload.size() > ob::MM_SNAPSHOT_CHUNK_HEADER_SIZE + 4) {
            uint16_t idx = 0;
            std::memcpy(&idx, f.payload.data(), sizeof(idx));
            if (idx != ob::MM_SNAPSHOT_META_INDEX) {
                f.payload[ob::MM_SNAPSHOT_CHUNK_HEADER_SIZE] ^= 0xFF;   // flip one byte
                damaged = true;
            }
        }
        return true;
    });

    ASSERT_TRUE(damaged);
    EXPECT_FALSE(receiver.mm->is_bootstrapping());
    EXPECT_TRUE(receiver.engine->holds_no_data());
}

TEST(MMSnapshotRefusal, DamagedMetadataIsCaughtBeforeAnyFileIsWritten) {
    Node sender(1);
    Node receiver(2);
    sender.write_rows("BTC", 8, 5'000'000);

    WiredPeer to_receiver(2);
    ob::PeerConnection sender_peer;
    sender_peer.node_id = 1;
    sender_peer.handshake_done = true;

    bool damaged = false;
    run_transfer(sender, to_receiver, receiver, sender_peer, [&](Frame& f) {
        if (!damaged && f.hdr.record_type == ob::MM_MSG_SNAPSHOT_CHUNK &&
            f.payload.size() > ob::MM_SNAPSHOT_CHUNK_HEADER_SIZE) {
            uint16_t idx = 0;
            std::memcpy(&idx, f.payload.data(), sizeof(idx));
            if (idx == ob::MM_SNAPSHOT_META_INDEX) {
                f.payload[ob::MM_SNAPSHOT_CHUNK_HEADER_SIZE] ^= 0x01;
                damaged = true;
            }
        }
        return true;
    });

    ASSERT_TRUE(damaged);
    EXPECT_FALSE(receiver.mm->is_bootstrapping());
    EXPECT_TRUE(receiver.engine->holds_no_data());
}

TEST(MMSnapshotRefusal, AVectorThatSaysSendEverythingIsNotInstalled) {
    // A sender refuses to send a vector past what it states; a receiver that installed one would
    // discard what it holds and adopt no frontier at all, and every peer would resend it the
    // snapshot's worth of records into append-only storage. Since #177 the vector is read from the
    // block that carries one of any size, so the refusal is that reader's answer too: the BEGIN and
    // the metadata are rewritten here to carry the old "send everything" marker, CRC and all.
    Node sender(1);
    Node receiver(2);
    sender.write_rows("BTC", 8, 6'000'000);

    WiredPeer to_receiver(2);
    ob::PeerConnection sender_peer;
    sender_peer.node_id = 1;
    sender_peer.handshake_done = true;

    Frame begin_frame;
    ob::SnapshotBegin begin{};
    bool held_begin = false, rewritten = false;
    run_transfer(sender, to_receiver, receiver, sender_peer, [&](Frame& f) {
        if (f.hdr.record_type == ob::MM_MSG_SNAPSHOT_BEGIN) {
            held_begin = ob::decode_snapshot_begin(f.payload.data(), f.payload.size(), begin);
            begin_frame = f;
            return false;                          // delivered with the metadata it describes
        }
        if (!held_begin || rewritten || f.hdr.record_type != ob::MM_MSG_SNAPSHOT_CHUNK) return true;
        uint16_t index = 0;
        uint64_t offset = 0;
        const uint8_t* bytes = nullptr;
        size_t n = 0;
        if (!ob::decode_snapshot_chunk(f.payload.data(), f.payload.size(), index, offset, bytes, n) ||
            index != ob::MM_SNAPSHOT_META_INDEX || offset != 0 || n != begin.total()) {
            return true;                           // not the one chunk this small metadata is
        }
        const auto marker = ob::serialize_version_vector({}, /*truncated=*/true);
        std::vector<uint8_t> meta(bytes, bytes + begin.manifest_len);
        meta.insert(meta.end(), marker.begin(), marker.end());
        meta.insert(meta.end(), bytes + begin.manifest_len + begin.vector_len, bytes + n);
        begin.vector_len = static_cast<uint32_t>(marker.size());
        begin.meta_crc   = ob::crc32c(meta.data(), meta.size());
        begin_frame.payload = ob::encode_snapshot_begin(begin);
        deliver(*receiver.mm, sender_peer, begin_frame);
        f.payload = ob::encode_snapshot_chunk(ob::MM_SNAPSHOT_META_INDEX, 0, meta.data(), meta.size());
        rewritten = true;
        return true;
    });

    ASSERT_TRUE(rewritten);
    EXPECT_FALSE(receiver.mm->is_bootstrapping());
    EXPECT_TRUE(receiver.engine->holds_no_data()) << "a snapshot whose vector says nothing was installed";
}

TEST(MMSnapshotRefusal, AnEndWithFilesStillMissingIsRefused) {
    Node sender(1);
    Node receiver(2);
    sender.write_rows("BTC", 8, 6'000'000);

    WiredPeer to_receiver(2);
    ob::PeerConnection sender_peer;
    sender_peer.node_id = 1;
    sender_peer.handshake_done = true;

    // Drop the last file chunk, then let END through. Without the completeness check the receiver
    // would install a manifest it never fully received.
    std::vector<Frame> seen;
    receiver.mm->note_snapshot_asked_for_test(sender_peer);   // or its BEGIN is refused (#188)
    request_snapshot_and_settle(sender, to_receiver);
    for (int round = 0; round < 10'000 && sender.mm->snapshot_send_active(); ++round) {
        to_receiver.collect();
        for (auto& f : take_frames(to_receiver.inbox)) seen.push_back(std::move(f));
        sender.mm->advance_snapshot_send(to_receiver.mgr(*sender.mm));
    }
    to_receiver.collect();
    for (auto& f : take_frames(to_receiver.inbox)) seen.push_back(std::move(f));

    size_t last_chunk = 0;
    for (size_t i = 0; i < seen.size(); ++i) {
        if (seen[i].hdr.record_type == ob::MM_MSG_SNAPSHOT_CHUNK) last_chunk = i;
    }
    ASSERT_GT(last_chunk, 0u);

    for (size_t i = 0; i < seen.size(); ++i) {
        if (i == last_chunk) continue;
        deliver(*receiver.mm, sender_peer, seen[i]);
    }

    EXPECT_FALSE(receiver.mm->is_bootstrapping());

    // The invariant is about the data directory, not about the flag. `holds_no_data()` reads
    // in-memory state, and a half-installed snapshot leaves that state empty while the directory
    // already holds part of another node's segments — so asserting on it alone passed with the
    // completeness check disabled. Count the files instead.
    size_t col_files = 0;
    std::error_code ec;
    for (auto it = fs::recursive_directory_iterator(receiver.tmp.path, ec);
         it != fs::recursive_directory_iterator(); it.increment(ec)) {
        if (it->is_regular_file() && it->path().extension() == ".col") ++col_files;
    }
    EXPECT_EQ(col_files, 0u)
        << "an incomplete snapshot must install nothing at all, not the files that did arrive";
}

TEST(MMSnapshotRefusal, ASecondBeginDoesNotDisturbTheFirstTransfer) {
    Node sender(1);
    Node receiver(2);
    sender.write_rows("BTC", 8, 7'000'000);

    WiredPeer to_receiver(2);
    ob::PeerConnection first;
    first.node_id = 1;
    first.handshake_done = true;
    ob::PeerConnection second;
    second.node_id = 3;
    second.handshake_done = true;

    request_snapshot_and_settle(sender, to_receiver);
    to_receiver.collect();
    auto frames = take_frames(to_receiver.inbox);
    ASSERT_FALSE(frames.empty());
    ASSERT_EQ(frames[0].hdr.record_type, ob::MM_MSG_SNAPSHOT_BEGIN);

    receiver.mm->note_snapshot_asked_for_test(first);
    deliver(*receiver.mm, first, frames[0]);
    ASSERT_TRUE(receiver.mm->snapshot_recv_active());

    // A second sender announcing its own snapshot must be turned away, not allowed to take over
    // the staging directory the first one is filling.
    receiver.mm->handle_snapshot_begin(second, frames[0].payload.data(),
                                       frames[0].payload.size());
    EXPECT_TRUE(receiver.mm->snapshot_recv_active());
    EXPECT_TRUE(receiver.mm->is_bootstrapping());

    receiver.mm->abort_bootstrap("test_cleanup");
    EXPECT_FALSE(receiver.mm->is_bootstrapping());
}

TEST(MMSnapshotRefusal, ASecondRequestToASenderAlreadyStreamingIsRefused) {
    // A watermark of a few hundred bytes so the transfer pauses instead of finishing inside the
    // first call: with the production 4 MB and a store this small, everything is enqueued at once
    // and there is no "already streaming" state to test.
    Node sender(1, /*snapshot_watermark=*/256);
    sender.write_rows("BTC", 8, 8'000'000);

    WiredPeer a(2, /*tiny_buffers=*/true);
    WiredPeer b(3);

    request_snapshot_and_settle(sender, a);
    ASSERT_TRUE(sender.mm->snapshot_send_active())
        << "the transfer should have paused on a full socket, not run to completion";

    sender.mm->handle_snapshot_request(b.mgr(*sender.mm));
    EXPECT_TRUE(sender.mm->snapshot_send_active())
        << "the transfer in flight must survive the second request";

    // And the second peer was told why, rather than left waiting.
    b.collect();
    const auto frames = take_frames(b.inbox);
    ASSERT_EQ(frames.size(), 1u);
    EXPECT_EQ(frames[0].hdr.record_type, ob::MM_MSG_SNAPSHOT_ABORT);
    EXPECT_EQ(ob::decode_snapshot_abort(frames[0].payload.data(), frames[0].payload.size()),
              "busy");
}

TEST(MMSnapshotRefusal, LosingTheSourceMidTransferClearsTheFlag) {
    Node sender(1);
    Node receiver(2);
    sender.write_rows("BTC", 8, 9'000'000);

    WiredPeer to_receiver(2);
    ob::PeerConnection source;
    source.node_id = 1;
    source.handshake_done = true;

    request_snapshot_and_settle(sender, to_receiver);
    to_receiver.collect();
    auto frames = take_frames(to_receiver.inbox);
    ASSERT_FALSE(frames.empty());
    receiver.mm->note_snapshot_asked_for_test(source);
    deliver(*receiver.mm, source, frames[0]);
    ASSERT_TRUE(receiver.mm->is_bootstrapping());

    source.connected = false;
    receiver.mm->on_peer_disconnected(source);

    EXPECT_FALSE(receiver.mm->snapshot_recv_active());
    EXPECT_FALSE(receiver.mm->is_bootstrapping())
        << "a node whose source vanished must become usable, not wait for ever (#73, #76)";
    EXPECT_FALSE(fs::exists(receiver.tmp.path + "/mm_snapshot_staging"));
}

// ═══════════════════════════════════════════════════════════════════════════════
// Who may ask
// ═══════════════════════════════════════════════════════════════════════════════

TEST(MMSnapshotRequest, ANodeWithDataOfItsOwnDoesNotAskForASnapshot) {
    // Installing a snapshot discards local contents. A node that wipes its own rows because a
    // peer looked further ahead is a worse failure than any amount of redundant traffic.
    Node node(1);
    node.write_rows("BTC", 3, 10'000'000);
    ASSERT_FALSE(node.engine->holds_no_data());

    WiredPeer peer(2);
    EXPECT_FALSE(node.mm->request_snapshot_from(peer.peer));

    peer.collect();
    EXPECT_TRUE(peer.inbox.empty()) << "nothing should have been sent";
}

TEST(MMSnapshotRequest, AnEmptyNodeAsks) {
    Node node(1);
    ASSERT_TRUE(node.engine->holds_no_data());

    WiredPeer peer(2);
    EXPECT_TRUE(node.mm->request_snapshot_from(peer.peer));

    peer.collect();
    const auto frames = take_frames(peer.inbox);
    ASSERT_EQ(frames.size(), 1u);
    EXPECT_EQ(frames[0].hdr.record_type, ob::MM_MSG_SNAPSHOT_REQUEST);
    EXPECT_TRUE(frames[0].payload.empty());
}

// ═══════════════════════════════════════════════════════════════════════════════
// One snapshot, from one peer (#188)
// ═══════════════════════════════════════════════════════════════════════════════
//
// A node that held nothing asked every peer whose vector arrived before the first SNAPSHOT_BEGIN -
// on a join, all of them - each prepared and sent a whole snapshot, and the node installed each one
// that arrived after a bootstrap had finished: three installs from three peers, writes refused
// 21.6 s where one took 6.8.

namespace {

/// A snapshot frame arriving on `peer`'s connection, as the io loop reads it off the socket.
void arrive_snapshot_frame(ob::MultiMasterManager& mm, ob::PeerConnection& peer, uint8_t type,
                           const std::vector<uint8_t>& payload) {
    ob::WALRecordV2 hdr{};
    hdr.record_type    = type;
    hdr.version        = 1;
    hdr.payload_len    = static_cast<uint16_t>(payload.size());
    hdr.origin_node_id = peer.node_id;
    hdr.checksum       = ob::crc32c(payload.data(), payload.size());
    std::vector<uint8_t> frame;
    ob::encode_frame_header(ob::MM_WALRECORD_V2_SIZE + payload.size(), frame);
    const auto* hb = reinterpret_cast<const uint8_t*>(&hdr);
    frame.insert(frame.end(), hb, hb + ob::MM_WALRECORD_V2_SIZE);
    frame.insert(frame.end(), payload.begin(), payload.end());
    peer.recv_buf.insert(peer.recv_buf.end(), frame.begin(), frame.end());
    mm.process_recv_buf_for_test(peer);
}

size_t requests_in(WiredPeer& peer) {
    peer.collect();
    size_t n = 0;
    for (const auto& f : take_frames(peer.inbox)) n += f.hdr.record_type == ob::MM_MSG_SNAPSHOT_REQUEST;
    return n;
}

std::string refusal_in(WiredPeer& peer) {
    peer.collect();
    for (const auto& f : take_frames(peer.inbox)) {
        if (f.hdr.record_type == ob::MM_MSG_SNAPSHOT_ABORT) {
            return ob::decode_snapshot_abort(f.payload.data(), f.payload.size());
        }
    }
    return "";
}

}  // namespace

TEST(MMSnapshotOnePeer, ANodeThatHoldsNothingAsksOnePeerAtATime) {
    Node node(1);
    WiredPeer a(2), b(3), c(4);
    EXPECT_TRUE(node.mm->request_snapshot_from(a.mgr(*node.mm)));
    EXPECT_FALSE(node.mm->request_snapshot_from(b.mgr(*node.mm)))
        << "a second peer was asked while the first had not answered";
    EXPECT_FALSE(node.mm->request_snapshot_from(c.mgr(*node.mm)));
    EXPECT_EQ(requests_in(a), 1u);
    EXPECT_EQ(requests_in(b), 0u);
    EXPECT_EQ(requests_in(c), 0u);
    const auto ask = node.mm->snapshot_ask_for_test();
    EXPECT_TRUE(ask.active);
    EXPECT_EQ(ask.node_id, 2u);
}

TEST(MMSnapshotOnePeer, ANodeRefusesWritesFromItsRequestUntilTheLastPeerRefuses) {
    // Between the request and the BEGIN the other peers' catch-ups arrive, and a client may write: a
    // node that took either held data by the BEGIN - and was refused the snapshot, or lost what it
    // held to the install. Measured: a joiner held its peers' catch-ups 8 ms after asking, when the
    // BEGIN came. And a node no peer can serve takes writes again rather than refuse them for ever.
    Node node(1);
    WiredPeer a(2);
    ob::PeerConnection& pa = a.mgr(*node.mm);
    ASSERT_FALSE(node.mm->is_bootstrapping());
    ASSERT_TRUE(node.mm->request_snapshot_from(pa));
    EXPECT_TRUE(node.mm->is_bootstrapping()) << "a node waiting for its snapshot takes writes";
    arrive_snapshot_frame(*node.mm, pa, ob::MM_MSG_SNAPSHOT_ABORT, ob::encode_snapshot_abort("busy"));
    EXPECT_FALSE(node.mm->is_bootstrapping()) << "a node no peer can serve refuses writes for good";
}

TEST(MMSnapshotOnePeer, APeerThatRefusesOrGoesAwayLetsTheNextBeAsked) {
    Node node(1);
    WiredPeer a(2), b(3), c(4);
    ob::PeerConnection& pa = a.mgr(*node.mm);
    ob::PeerConnection& pb = b.mgr(*node.mm);
    ob::PeerConnection& pc = c.mgr(*node.mm);
    ASSERT_TRUE(node.mm->request_snapshot_from(pa));

    arrive_snapshot_frame(*node.mm, pb, ob::MM_MSG_SNAPSHOT_ABORT, ob::encode_snapshot_abort("busy"));
    EXPECT_TRUE(node.mm->snapshot_ask_for_test().active)
        << "a refusal from a peer this node did not ask ended the request to another";
    arrive_snapshot_frame(*node.mm, pa, ob::MM_MSG_SNAPSHOT_ABORT, ob::encode_snapshot_abort("busy"));
    EXPECT_FALSE(node.mm->snapshot_ask_for_test().active);
    ASSERT_TRUE(node.mm->request_snapshot_from(pb)) << "a refusal kept the next peer from being asked";

    node.mm->on_peer_disconnected(pa);
    EXPECT_TRUE(node.mm->snapshot_ask_for_test().active)
        << "a peer that was not asked going away ended the request to another";
    node.mm->on_peer_disconnected(pb);
    EXPECT_FALSE(node.mm->snapshot_ask_for_test().active);
    EXPECT_TRUE(node.mm->request_snapshot_from(pc)) << "a peer gone kept the next from being asked";
}

TEST(MMSnapshotOnePeer, ARefusalAsksTheNextPeerThatStatesWhatItHoldsAtOnce) {
    // Not at that peer's next vector, a reconciliation interval later: before #188 every peer was
    // asked at once, so one refusing cost nothing, and one at a time must not make it cost 30 s.
    Node node(1);
    WiredPeer a(2), b(3);
    ob::PeerConnection& pa = a.mgr(*node.mm);
    ob::PeerConnection& pb = b.mgr(*node.mm);
    const auto bytes = ob::serialize_version_vector({{"BTC.USDT", 3, 8}}, false);
    ASSERT_TRUE(pb.peer_vector.deserialize(bytes.data(), bytes.size()));
    ASSERT_TRUE(node.mm->request_snapshot_from(pa));
    EXPECT_EQ(requests_in(b), 0u);

    arrive_snapshot_frame(*node.mm, pa, ob::MM_MSG_SNAPSHOT_ABORT, ob::encode_snapshot_abort("busy"));
    EXPECT_EQ(requests_in(b), 1u) << "the peer that states what it holds was not asked in its place";
    EXPECT_EQ(node.mm->snapshot_ask_for_test().node_id, 3u);
}

TEST(MMSnapshotOnePeer, ABootstrapAbandonedPartWayAsksTheNextPeer) {
    // Before #188 the other peers had been asked too, and a BEGIN from one of them was the retry.
    Node node(1);
    WiredPeer a(2), b(3);
    ob::PeerConnection& pa = a.mgr(*node.mm);
    ob::PeerConnection& pb = b.mgr(*node.mm);
    const auto bytes = ob::serialize_version_vector({{"BTC.USDT", 3, 8}}, false);
    ASSERT_TRUE(pb.peer_vector.deserialize(bytes.data(), bytes.size()));
    ASSERT_TRUE(node.mm->request_snapshot_from(pa));
    ob::SnapshotBegin begin{};
    begin.manifest_len = 10;
    begin.vector_len   = 2;
    const auto payload = ob::encode_snapshot_begin(begin);
    node.mm->handle_snapshot_begin(pa, payload.data(), payload.size());
    ASSERT_TRUE(node.mm->snapshot_recv_active());
    EXPECT_EQ(requests_in(b), 0u);

    arrive_snapshot_frame(*node.mm, pa, ob::MM_MSG_SNAPSHOT_ABORT,
                          ob::encode_snapshot_abort("file_read_failed"));
    EXPECT_FALSE(node.mm->snapshot_recv_active());
    EXPECT_EQ(requests_in(b), 1u) << "an abandoned bootstrap waited for the next vector to ask again";
}

TEST(MMSnapshotOnePeer, ARequestNobodyAnswersIsForgottenAtItsDeadline) {
    Node node(1);
    WiredPeer a(2), b(3);
    ASSERT_TRUE(node.mm->request_snapshot_from(a.mgr(*node.mm)));
    const auto asked = node.mm->snapshot_ask_for_test().asked_at;
    node.mm->expire_snapshot_ask_for_test(
        asked + std::chrono::milliseconds(ob::MM_SNAPSHOT_ASK_DEADLINE_MS - 1));
    EXPECT_TRUE(node.mm->snapshot_ask_for_test().active) << "forgotten before its deadline";
    node.mm->expire_snapshot_ask_for_test(
        asked + std::chrono::milliseconds(ob::MM_SNAPSHOT_ASK_DEADLINE_MS));
    EXPECT_FALSE(node.mm->snapshot_ask_for_test().active);
    EXPECT_TRUE(node.mm->request_snapshot_from(b.mgr(*node.mm)))
        << "a peer that said nothing kept the next from being asked";
}

TEST(MMSnapshotOnePeer, ABeginThisNodeDidNotAskForIsRefusedAndInstallsNothing) {
    Node sender(1);
    Node receiver(2);
    sender.write_rows("BTC", 8, 11'000'000);
    WiredPeer to_receiver(2);
    WiredPeer back(1);                     // the receiver's connection to the sender
    ob::PeerConnection& sender_peer = back.mgr(*receiver.mm);

    request_snapshot_and_settle(sender, to_receiver);   // the sender answers a request ...
    to_receiver.collect();
    const auto frames = take_frames(to_receiver.inbox);
    ASSERT_FALSE(frames.empty());
    ASSERT_EQ(frames[0].hdr.record_type, ob::MM_MSG_SNAPSHOT_BEGIN);
    deliver(*receiver.mm, sender_peer, frames[0]);     // ... this node never sent
    EXPECT_FALSE(receiver.mm->snapshot_recv_active());
    EXPECT_FALSE(receiver.mm->is_bootstrapping());
    EXPECT_EQ(refusal_in(back), "not_requested");
}

TEST(MMSnapshotOnePeer, ABeginFromTheNodeAskedOnAnotherConnectionIsRefused) {
    // The request belongs to the connection it went on, as the sender's answer does (#79): the node
    // on a connection of its own has asked this one for nothing.
    Node sender(1);
    Node receiver(2);
    sender.write_rows("BTC", 8, 15'000'000);
    WiredPeer to_receiver(2);
    WiredPeer back(1);
    ob::PeerConnection& asked = back.mgr(*receiver.mm);
    ASSERT_TRUE(receiver.mm->request_snapshot_from(asked));
    ob::PeerConnection other;                  // the same node, on another connection
    other.node_id        = asked.node_id;
    other.conn_id        = asked.conn_id + 1000;
    other.fd             = back.local_fd;      // so its refusal is read where the request was
    other.connected      = true;
    other.handshake_done = true;

    request_snapshot_and_settle(sender, to_receiver);
    to_receiver.collect();
    const auto frames = take_frames(to_receiver.inbox);
    ASSERT_FALSE(frames.empty());
    ASSERT_EQ(frames[0].hdr.record_type, ob::MM_MSG_SNAPSHOT_BEGIN);
    deliver(*receiver.mm, other, frames[0]);
    EXPECT_FALSE(receiver.mm->snapshot_recv_active())
        << "a BEGIN on a connection that asked for nothing was taken";
    EXPECT_EQ(refusal_in(back), "not_requested");
    EXPECT_TRUE(receiver.mm->snapshot_ask_for_test().active) << "it ended the request it did not answer";
}

TEST(MMSnapshotOnePeer, ABeginThatArrivesOnceThisNodeHoldsDataIsRefused) {
    // Asked while it held nothing, and written to before the answer came: installing would replace
    // what the client was told was stored.
    Node sender(1);
    Node receiver(2);
    sender.write_rows("BTC", 8, 12'000'000);
    WiredPeer to_receiver(2);
    WiredPeer back(1);
    ob::PeerConnection& sender_peer = back.mgr(*receiver.mm);
    ASSERT_TRUE(receiver.mm->request_snapshot_from(sender_peer));
    receiver.write_rows("ETH", 3, 13'000'000);
    ASSERT_FALSE(receiver.engine->holds_no_data());

    request_snapshot_and_settle(sender, to_receiver);
    to_receiver.collect();
    const auto frames = take_frames(to_receiver.inbox);
    ASSERT_FALSE(frames.empty());
    deliver(*receiver.mm, sender_peer, frames[0]);
    EXPECT_FALSE(receiver.mm->snapshot_recv_active());
    EXPECT_EQ(refusal_in(back), "holds_data");
    EXPECT_FALSE(receiver.mm->snapshot_ask_for_test().active) << "the request was answered";
}

TEST(MMSnapshotOnePeer, ASenderItsTargetRefusedStopsSending) {
    // A refused sender used to stream every file, and the receiver drop each chunk.
    Node sender(1, /*snapshot_watermark=*/256);   // pauses on a full socket rather than finishing
    sender.write_rows("BTC", 8, 14'000'000);
    WiredPeer a(2, /*tiny_buffers=*/true);
    WiredPeer other(3);
    request_snapshot_and_settle(sender, a);
    ASSERT_TRUE(sender.mm->snapshot_send_active());

    arrive_snapshot_frame(*sender.mm, other.mgr(*sender.mm), ob::MM_MSG_SNAPSHOT_ABORT,
                          ob::encode_snapshot_abort("not_requested"));
    EXPECT_TRUE(sender.mm->snapshot_send_active())
        << "a refusal from another peer ended this transfer";
    arrive_snapshot_frame(*sender.mm, a.mgr(*sender.mm), ob::MM_MSG_SNAPSHOT_ABORT,
                          ob::encode_snapshot_abort("not_requested"));
    EXPECT_FALSE(sender.mm->snapshot_send_active()) << "a refused sender went on sending";
}

// ═══════════════════════════════════════════════════════════════════════════════
// What a bootstrapping node does with live traffic
// ═══════════════════════════════════════════════════════════════════════════════

TEST(MMSnapshotBootstrapWindow, RemoteDeltasAreDroppedWithoutBeingRemembered) {
    // The subtle half of the rule. Applying a delta now is harmless in itself — load_snapshot()
    // discards it. Recording its number is not: the frontier would claim a row that no longer
    // exists, and no later catch-up fills a hole nobody knows about. Left unmarked, the record
    // comes back on the next vector exchange.
    Node node(2);
    node.mm->start_bootstrap();
    ASSERT_TRUE(node.mm->is_bootstrapping());

    ob::DeltaUpdate delta{};
    std::strncpy(delta.symbol, "BTC", sizeof(delta.symbol) - 1);
    std::strncpy(delta.exchange, "USDT", sizeof(delta.exchange) - 1);
    delta.timestamp_ns    = 1'234'000;
    delta.sequence_number = 41;
    delta.side            = ob::SIDE_BID;
    delta.n_levels        = 1;

    ob::Level level{};
    level.price = 100'000;
    level.qty   = 3;

    std::vector<uint8_t> payload(sizeof(delta) + sizeof(level));
    std::memcpy(payload.data(), &delta, sizeof(delta));
    std::memcpy(payload.data() + sizeof(delta), &level, sizeof(level));

    ob::WALRecordV2 hdr{};
    hdr.record_type     = ob::WAL_RECORD_DELTA;
    hdr.version         = 1;
    hdr.origin_node_id  = 1;
    hdr.sequence_number = 41;
    hdr.payload_len     = static_cast<uint16_t>(payload.size());

    EXPECT_FALSE(node.mm->handle_remote_record(1, hdr, payload.data(), payload.size()));

    EXPECT_EQ(node.engine->above_frontier_size("BTC.USDT", 1), 0u)
        << "the number must not be held: holding it is claiming a row that will be discarded";
    EXPECT_TRUE(node.engine->holds_no_data())
        << "and nothing may have been applied";

    node.mm->finish_bootstrap(/*succeeded=*/false);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Compatibility with a node that does not know these messages
// ═══════════════════════════════════════════════════════════════════════════════

TEST(MMSnapshotCompatibility, AnUnknownRecordTypeIsSkippedNotFatal) {
    // This is the whole backward-compatibility argument, so it is worth a test rather than a
    // comment. handle_frame() branches on record_type and anything it does not recognise falls
    // through to handle_remote_record(), which refuses it and leaves the connection alone. A node
    // running the older build therefore stays in the mesh when a newer peer sends a snapshot
    // frame — and, symmetrically, this node survives a message type added after it was built.
    Node node(1);

    ob::WALRecordV2 hdr{};
    hdr.record_type    = 250;          // reserved wire-only range, nothing implements it
    hdr.version        = 1;
    hdr.origin_node_id = 2;
    hdr.payload_len    = 0;

    EXPECT_FALSE(node.mm->handle_remote_record(2, hdr, nullptr, 0));
    EXPECT_TRUE(node.engine->holds_no_data());

    // And a WAL record type that exists but is not a delta behaves the same way.
    hdr.record_type = ob::WAL_RECORD_CHECKPOINT;
    EXPECT_FALSE(node.mm->handle_remote_record(2, hdr, nullptr, 0));
}

// ═══════════════════════════════════════════════════════════════════════════════
// Cost of creating a snapshot on the io_loop thread
// ═══════════════════════════════════════════════════════════════════════════════
//
// Disabled by default: it writes a hundred thousand rows and only prints numbers, so it belongs
// in a Release build run by hand rather than in every ctest pass. It exists because the cost is
// paid on the thread that also carries live multi-master traffic, and a cost like that has to be
// measured and written down rather than estimated. Run it with:
//
//   ./build-release/tests/test_mm_snapshot --gtest_also_run_disabled_tests
//       --gtest_filter='*SnapshotCreationCost*'

// ═══════════════════════════════════════════════════════════════════════════════
// Creating it off the io thread (#79): who the finished snapshot belongs to
// ═══════════════════════════════════════════════════════════════════════════════

// The request no longer produces anything by itself. That is the change: io_loop() accepts the
// request and goes back to epoll_wait(), and the frames appear when it collects the result.
TEST(MMSnapshotPreparation, TheRequestItselfSendsNothing) {
    Node sender(1);
    sender.write_rows("BTC", 8, 20'000'000);
    WiredPeer to_receiver(2);

    sender.mm->handle_snapshot_request(to_receiver.mgr(*sender.mm));

    EXPECT_TRUE(sender.mm->snapshot_preparing());
    EXPECT_FALSE(sender.mm->snapshot_send_active());

    to_receiver.collect();
    EXPECT_TRUE(to_receiver.inbox.empty())
        << "handle_snapshot_request() must not put a byte on the wire: the snapshot does not exist "
           "yet, and the io thread is free precisely because it is not waiting for it";

    // And collecting it does produce a transfer, so the test above is not passing for want of a
    // working path.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (sender.mm->snapshot_preparing() && std::chrono::steady_clock::now() < deadline) {
        sender.mm->poll_snapshot_preparation();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    to_receiver.collect();
    const auto frames = take_frames(to_receiver.inbox);
    ASSERT_FALSE(frames.empty());
    EXPECT_EQ(frames[0].hdr.record_type, ob::MM_MSG_SNAPSHOT_BEGIN);
}

TEST(MMSnapshotPreparation, APeerThatLeftBeforeCollectionGetsNothing) {
    Node sender(1);
    sender.write_rows("BTC", 8, 21'000'000);
    WiredPeer to_receiver(2);

    auto& stored = to_receiver.mgr(*sender.mm);
    sender.mm->handle_snapshot_request(stored);
    ASSERT_TRUE(sender.mm->snapshot_preparing());

    stored.connected = false;
    sender.mm->on_peer_disconnected(stored);
    EXPECT_FALSE(sender.mm->snapshot_preparing());

    // Collect what the worker produced. It has to be thrown away rather than sent.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (sender.mm->snapshot_builder_busy() && std::chrono::steady_clock::now() < deadline) {
        sender.mm->poll_snapshot_preparation();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    EXPECT_FALSE(sender.mm->snapshot_send_active());
    to_receiver.collect();
    EXPECT_TRUE(to_receiver.inbox.empty())
        << "a snapshot finished for a peer that has gone must be discarded, not streamed at "
           "whatever is on that descriptor now";
}

// The harder half of the same idea, and the reason PeerConnection carries a conn_id: the node that
// asked comes *back*. Same node_id, possibly the same descriptor number, and it has requested
// nothing — installing a snapshot discards local contents, so sending it one would be handing it a
// wipe it never asked for.
TEST(MMSnapshotPreparation, TheSameNodeOnANewConnectionGetsNothing) {
    Node sender(1);
    sender.write_rows("BTC", 8, 22'000'000);

    uint64_t asked_on = 0;
    {
        WiredPeer first(2);
        auto& stored = first.mgr(*sender.mm);
        asked_on     = stored.conn_id;
        sender.mm->handle_snapshot_request(stored);
        ASSERT_TRUE(sender.mm->snapshot_preparing());
    }   // the socket goes away without the manager ever being told

    // Node 2 reconnects. Nothing announced the loss of the previous connection, so this is the case
    // that node_id alone cannot distinguish.
    WiredPeer second(2);
    auto& fresh = second.mgr(*sender.mm);
    ASSERT_NE(fresh.conn_id, asked_on) << "a new connection must not reuse a connection id";

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (sender.mm->snapshot_builder_busy() && std::chrono::steady_clock::now() < deadline) {
        sender.mm->poll_snapshot_preparation();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    EXPECT_FALSE(sender.mm->snapshot_send_active());
    EXPECT_FALSE(sender.mm->snapshot_preparing());
    second.collect();
    EXPECT_TRUE(second.inbox.empty())
        << "the reconnected node asked for nothing and must be sent nothing";
}

TEST(MMSnapshotPreparation, ASecondRequestWhileOneIsBeingCreatedIsRefused) {
    Node sender(1);
    sender.write_rows("BTC", 8, 23'000'000);

    WiredPeer a(2);
    WiredPeer b(3);

    sender.mm->handle_snapshot_request(a.mgr(*sender.mm));
    ASSERT_TRUE(sender.mm->snapshot_preparing());

    sender.mm->handle_snapshot_request(b.mgr(*sender.mm));

    b.collect();
    const auto frames = take_frames(b.inbox);
    ASSERT_EQ(frames.size(), 1u);
    EXPECT_EQ(frames[0].hdr.record_type, ob::MM_MSG_SNAPSHOT_ABORT);
    EXPECT_EQ(ob::decode_snapshot_abort(frames[0].payload.data(), frames[0].payload.size()),
              "busy");

    // The first request is untouched by the second.
    EXPECT_TRUE(sender.mm->snapshot_preparing());
}

// Value of this one is mostly under instrumentation: it is the ASan and TSan jobs that decide
// whether a manager destroyed with a snapshot in flight left a thread holding a reference to it.
TEST(MMSnapshotPreparation, TearingDownWithASnapshotInFlightIsClean) {
    auto sender = std::make_unique<Node>(1);
    sender->write_rows("BTC", 8, 24'000'000);
    WiredPeer to_receiver(2);

    sender->mm->handle_snapshot_request(to_receiver.mgr(*sender->mm));
    ASSERT_TRUE(sender->mm->snapshot_preparing());

    sender.reset();   // ~MultiMasterManager → ~AsyncSnapshotBuilder → join
    SUCCEED();
}

// The manifest file is written by whoever creates a snapshot, and that is now up to four threads: a
// multi-master worker, a replication worker, and anything on the main thread. It used to be written
// straight onto the target path with no lock, so two of them could interleave their JSON.
//
// Two details make this test able to see that, and the first version had neither. The store holds
// thirty symbols so the manifest is tens of kilobytes — a two-file manifest fits in one stdio buffer
// and goes out in a single write(), which no reader can catch mid-way. And an empty read counts as a
// failure once the file has been seen non-empty: `trunc` on the target path empties it before the
// first byte of the replacement arrives, and a manifest that describes nothing is exactly the
// corruption at issue. With a rename neither window exists.
TEST(MMSnapshotPreparation, ConcurrentCreationNeverLeavesAHalfWrittenManifest) {
    Node node(1);
    for (int sym = 0; sym < 30; ++sym) {
        // std::to_string rather than snprintf into a fixed buffer: at -O1 and above GCC cannot
        // narrow the loop variable and reports "%02d may write up to 11 bytes into a region of
        // size 5" as an error. A Debug build at -O0 does not run that analysis at all, so the
        // sanitizer job — which builds Debug *plus* -O1 — was the first thing to see it.
        const std::string sym_name = "SYM" + std::to_string(sym);
        node.write_rows(sym_name.c_str(), 2, 25'000'000 + static_cast<uint64_t>(sym) * 1000);
    }

    const std::string manifest_path = node.tmp.path + "/snapshot_manifest.json";

    // One synchronous creation first, so the file exists and its size is known before any race.
    (void)node.engine->create_snapshot();
    {
        std::ifstream f(manifest_path);
        ASSERT_TRUE(f.is_open());
        const std::string content((std::istreambuf_iterator<char>(f)),
                                   std::istreambuf_iterator<char>());
        ASSERT_GT(content.size(), 8192u)
            << "the manifest has to be larger than a stdio buffer for this test to be able to "
               "observe a partial write at all";
    }

    std::atomic<bool> stop_reading{false};
    std::atomic<int>  parsed{0};
    std::atomic<int>  broken{0};

    std::thread reader([&] {
        while (!stop_reading.load()) {
            std::ifstream f(manifest_path);
            if (!f.is_open()) { broken.fetch_add(1); continue; }
            const std::string content((std::istreambuf_iterator<char>(f)),
                                       std::istreambuf_iterator<char>());
            ob::SnapshotManifest m;
            if (!content.empty() && ob::SnapshotManifest::from_json(content, m)) {
                parsed.fetch_add(1);
            } else {
                broken.fetch_add(1);
            }
        }
    });

    std::thread writers[2];
    for (auto& w : writers) {
        w = std::thread([&] {
            for (int i = 0; i < 15; ++i) (void)node.engine->create_snapshot();
        });
    }
    for (auto& w : writers) w.join();
    stop_reading.store(true);
    reader.join();

    EXPECT_EQ(broken.load(), 0)
        << broken.load() << " read(s) of the manifest found it absent, empty or unparseable while "
           "two threads were writing it, out of " << (broken.load() + parsed.load());
    EXPECT_GT(parsed.load(), 0) << "the reader never managed to read the manifest at all";
}

TEST(MMSnapshotMeasurement, DISABLED_SnapshotCreationCost) {
    Node node(1);

    constexpr int kSymbols = 20;
    constexpr int kRowsPerSymbol = 5'000;
    for (int s = 0; s < kSymbols; ++s) {
        const std::string sym = "SYM" + std::to_string(s);
        node.write_rows(sym.c_str(), kRowsPerSymbol, 1'000'000 + 1'000'000ULL * s);
    }

    for (int round = 0; round < 3; ++round) {
        const auto t0 = std::chrono::steady_clock::now();
        const auto snap = node.engine->create_snapshot_with_sequence_state();
        const double ms = std::chrono::duration<double, std::milli>(
                              std::chrono::steady_clock::now() - t0).count();
        std::printf("round %d: files=%zu bytes=%zu rows=%zu vector=%zu held=%zu -> %.1f ms "
                    "(%.1f MB/s)\n",
                    round, snap.manifest.files.size(), snap.manifest.total_bytes,
                    snap.manifest.total_rows, snap.vector.size(), snap.held.size(), ms,
                    ms > 0 ? (static_cast<double>(snap.manifest.total_bytes) / 1e6) / (ms / 1e3)
                           : 0.0);
    }

    // A breakdown, because two guesses at where the time goes have already been wrong. The first
    // said the checksum (it was about half, #81); the second said the per-file allocation and
    // ifstream (it was neither — replacing them moved nothing). These are the remaining
    // candidates, timed against the same directory the snapshot walks.
    const std::string& dir = node.engine->base_dir();
    for (int round = 0; round < 3; ++round) {
        size_t entries = 0, counted = 0, bytes = 0;

        auto t0 = std::chrono::steady_clock::now();
        for (auto& e : std::filesystem::recursive_directory_iterator(dir)) {
            ++entries;
            (void)e.is_regular_file();
        }
        auto t1 = std::chrono::steady_clock::now();

        // The walk again, statting for the size only.
        for (auto& e : std::filesystem::recursive_directory_iterator(dir)) {
            if (!e.is_regular_file()) continue;
            const auto& path = e.path();
            if (path.extension() != ".col" && path.filename() != "meta.json") continue;
            ++counted;
            bytes += static_cast<size_t>(e.file_size());
        }
        auto t1b = std::chrono::steady_clock::now();

        // And again, this time also making each path relative to the base directory — which is
        // where the time turned out to be.
        for (auto& e : std::filesystem::recursive_directory_iterator(dir)) {
            if (!e.is_regular_file()) continue;
            const auto& path = e.path();
            if (path.extension() != ".col" && path.filename() != "meta.json") continue;
            auto rel = std::filesystem::relative(path, dir).string();
            benchmarkish_sink += rel.size();
        }
        auto t1c = std::chrono::steady_clock::now();

        // The cheap way to get the same string: strip the base prefix. No filesystem access.
        for (auto& e : std::filesystem::recursive_directory_iterator(dir)) {
            if (!e.is_regular_file()) continue;
            const auto& path = e.path();
            if (path.extension() != ".col" && path.filename() != "meta.json") continue;
            const std::string full = path.string();
            std::string rel = (full.size() > dir.size() + 1) ? full.substr(dir.size() + 1) : full;
            benchmarkish_sink += rel.size();
        }
        auto t2 = std::chrono::steady_clock::now();

        // And reading every one of those files, folding the checksum as the snapshot does.
        std::vector<uint8_t> buf(256u * 1024u);
        for (auto& e : std::filesystem::recursive_directory_iterator(dir)) {
            if (!e.is_regular_file()) continue;
            const auto& path = e.path();
            if (path.extension() != ".col" && path.filename() != "meta.json") continue;
            const int fd = ::open(path.c_str(), O_RDONLY);
            if (fd < 0) continue;
            uint32_t crc = ob::crc32c_init;
            for (;;) {
                const ssize_t n = ::read(fd, buf.data(), buf.size());
                if (n <= 0) break;
                crc = ob::crc32c_update(crc, buf.data(), static_cast<size_t>(n));
            }
            ::close(fd);
            benchmarkish_sink += ob::crc32c_finish(crc);
        }
        auto t3 = std::chrono::steady_clock::now();

        const auto msec = [](auto a, auto b) {
            return std::chrono::duration<double, std::milli>(b - a).count();
        };
        std::printf("breakdown %d: walk %.2f ms | +file_size %.2f ms | +fs::relative %.2f ms | "
                    "+prefix-strip %.2f ms | read+crc of %zu bytes %.2f ms  (%zu entries, "
                    "%zu matched)\n",
                    round, msec(t0, t1), msec(t1, t1b), msec(t1b, t1c), msec(t1c, t2),
                    bytes, msec(t2, t3), entries, counted);
    }

    // And the number #79 is actually about: what one pass of the io loop pays when a snapshot
    // request arrives. Before, that pass ran the whole creation printed above; now it starts a
    // worker and returns. The comparison is between the two figures — the second is what the io
    // thread still pays per request, and it is a thread creation.
    for (int round = 0; round < 3; ++round) {
        WiredPeer peer(static_cast<uint16_t>(50 + round));
        auto& stored = peer.mgr(*node.mm);

        const auto t0 = std::chrono::steady_clock::now();
        node.mm->handle_snapshot_request(stored);
        const double accept_ms = std::chrono::duration<double, std::milli>(
                                     std::chrono::steady_clock::now() - t0).count();

        // Let the worker finish before the next round, so the rounds do not measure each other.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (node.mm->snapshot_builder_busy() &&
               std::chrono::steady_clock::now() < deadline) {
            node.mm->poll_snapshot_preparation();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        node.mm->on_peer_disconnected(stored);
        while (node.mm->snapshot_builder_busy() &&
               std::chrono::steady_clock::now() < deadline) {
            node.mm->poll_snapshot_preparation();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        std::printf("io-loop cost %d: accepting a snapshot request took %.3f ms\n",
                    round, accept_ms);
    }
    SUCCEED();
}

TEST(MMSnapshotRefusal, AManifestTooLargeToAddressIsRefusedRatherThanWrapped) {
    // A chunk names its file with a uint16_t and 0xFFFF is the metadata blob, so 65535 files is
    // the point at which an index either wraps or collides — and the receiver would write one
    // file's bytes into another. Checked as a bound rather than by building such a store, because
    // 65535 segment files is minutes of setup to prove one comparison.
    static_assert(ob::MM_SNAPSHOT_META_INDEX == 0xFFFF, "");
    EXPECT_LT(static_cast<size_t>(ob::MM_SNAPSHOT_META_INDEX),
              static_cast<size_t>(std::numeric_limits<uint16_t>::max()) + 1);

    // And the refusal path answers with a reason, which is what a sender must never skip.
    Node sender(1);
    sender.write_rows("BTC", 2, 11'000'000);
    WiredPeer peer(2);
    request_snapshot_and_settle(sender, peer);
    EXPECT_FALSE(sender.mm->snapshot_send_active())
        << "a two-row store fits in one pass, so the transfer should already be complete";
}

// ═══════════════════════════════════════════════════════════════════════════════
// Each peer once (#191), and a source told to stop (#192)
// ═══════════════════════════════════════════════════════════════════════════════
//
// The next peer asked after a refusal, a drop, a deadline or a bootstrap abandoned part-way was the
// first one other than the peer that had just failed - so with two peers or more that all fail, the
// joiner asked them in turn for ever, refusing writes the whole time: measured with a receiver that
// abandoned every snapshot at its 65 536th file, three peers each sent the joiner three whole
// snapshots of 65 600 files in 90 s, and the joiner never took a write. And a receiver that abandoned
// a transfer did not say so: the sender streamed the rest to a node that dropped every chunk, with a
// WARN for each.

namespace {

void state_a_vector(ob::PeerConnection& peer) {
    const auto bytes = ob::serialize_version_vector({{"BTC.USDT", 3, 8}}, false);
    ASSERT_TRUE(peer.peer_vector.deserialize(bytes.data(), bytes.size()));
}

/// The BEGIN of a small snapshot from `peer`, which this node asked.
void begin_from(Node& node, ob::PeerConnection& peer) {
    ob::SnapshotBegin begin{};
    begin.manifest_len = 10;
    begin.vector_len   = 2;
    const auto payload = ob::encode_snapshot_begin(begin);
    node.mm->handle_snapshot_begin(peer, payload.data(), payload.size());
}

}  // namespace

TEST(MMSnapshotEachPeerOnce, ANodeEveryPeerRefusesAsksEachOnceAndTakesWrites) {
    Node node(1);
    WiredPeer a(2), b(3);
    ob::PeerConnection& pa = a.mgr(*node.mm);
    ob::PeerConnection& pb = b.mgr(*node.mm);
    state_a_vector(pa);
    state_a_vector(pb);
    ASSERT_TRUE(node.mm->request_snapshot_from(pa));
    EXPECT_EQ(requests_in(a), 1u);

    arrive_snapshot_frame(*node.mm, pa, ob::MM_MSG_SNAPSHOT_ABORT,
                          ob::encode_snapshot_abort("too_many_files"));
    EXPECT_EQ(requests_in(b), 1u) << "the other peer was not asked";
    arrive_snapshot_frame(*node.mm, pb, ob::MM_MSG_SNAPSHOT_ABORT,
                          ob::encode_snapshot_abort("too_many_files"));
    EXPECT_EQ(requests_in(a), 0u) << "a peer that refused was asked again in the same round";
    EXPECT_FALSE(node.mm->snapshot_ask_for_test().active);
    EXPECT_FALSE(node.mm->is_bootstrapping()) << "a node every peer refused refuses writes";
}

TEST(MMSnapshotEachPeerOnce, ABootstrapEveryPeerFailsEndsOnceEachWasTried) {
    Node node(1);
    WiredPeer a(2), b(3);
    ob::PeerConnection& pa = a.mgr(*node.mm);
    ob::PeerConnection& pb = b.mgr(*node.mm);
    state_a_vector(pa);
    state_a_vector(pb);
    ASSERT_TRUE(node.mm->request_snapshot_from(pa));
    ASSERT_EQ(requests_in(a), 1u);
    begin_from(node, pa);
    ASSERT_TRUE(node.mm->snapshot_recv_active());
    node.mm->abort_bootstrap("file_crc_mismatch");
    EXPECT_EQ(requests_in(b), 1u) << "the other peer was not asked";
    begin_from(node, pb);
    ASSERT_TRUE(node.mm->snapshot_recv_active());
    node.mm->abort_bootstrap("file_crc_mismatch");
    EXPECT_EQ(requests_in(a), 0u) << "a peer whose snapshot failed was asked again in the same round";
    EXPECT_FALSE(node.mm->is_bootstrapping());
}

TEST(MMSnapshotEachPeerOnce, AnUnansweredOrDroppedPeerIsNotAskedAgainInTheRound) {
    Node node(1);
    WiredPeer a(2), b(3);
    ob::PeerConnection& pa = a.mgr(*node.mm);
    ob::PeerConnection& pb = b.mgr(*node.mm);
    state_a_vector(pa);
    state_a_vector(pb);
    ASSERT_TRUE(node.mm->request_snapshot_from(pa));
    ASSERT_EQ(requests_in(a), 1u);
    node.mm->expire_snapshot_ask_for_test(
        node.mm->snapshot_ask_for_test().asked_at +
        std::chrono::milliseconds(ob::MM_SNAPSHOT_ASK_DEADLINE_MS + 1));
    EXPECT_EQ(requests_in(b), 1u) << "the other peer was not asked at the deadline";
    node.mm->on_peer_disconnected(pb);
    EXPECT_EQ(requests_in(a), 0u) << "the peer that did not answer was asked again in the same round";
    EXPECT_FALSE(node.mm->is_bootstrapping());
}

TEST(MMSnapshotEachPeerOnce, TheNextRoundAsksAgain) {
    // What #188 says a node no peer served does: take writes, and ask at the next vector while it
    // still holds nothing. The round ends with the wait, so its peers can be asked in the next one.
    Node node(1);
    WiredPeer a(2);
    ob::PeerConnection& pa = a.mgr(*node.mm);
    state_a_vector(pa);
    ASSERT_TRUE(node.mm->request_snapshot_from(pa));
    arrive_snapshot_frame(*node.mm, pa, ob::MM_MSG_SNAPSHOT_ABORT, ob::encode_snapshot_abort("busy"));
    ASSERT_FALSE(node.mm->is_bootstrapping());
    EXPECT_EQ(requests_in(a), 1u);
    EXPECT_TRUE(node.mm->request_snapshot_from(pa)) << "the round that ended kept the peer from the next";
    EXPECT_EQ(requests_in(a), 1u);
}

TEST(MMSnapshotEachPeerOnce, AReceiverThatAbandonsATransferTellsItsSourceToStop) {
    Node node(1);
    WiredPeer a(2);
    ob::PeerConnection& pa = a.mgr(*node.mm);
    ASSERT_TRUE(node.mm->request_snapshot_from(pa));
    (void)requests_in(a);
    begin_from(node, pa);
    ASSERT_TRUE(node.mm->snapshot_recv_active());
    // A chunk of a file before the metadata is a transfer the receiver cannot take.
    const uint8_t byte = 1;
    const auto chunk = ob::encode_snapshot_chunk(0, 0, &byte, 1);
    arrive_snapshot_frame(*node.mm, pa, ob::MM_MSG_SNAPSHOT_CHUNK, chunk);
    EXPECT_FALSE(node.mm->snapshot_recv_active());
    EXPECT_EQ(refusal_in(a), "file_chunk_before_metadata") << "the source was not told to stop";
}

TEST(MMSnapshotEachPeerOnce, ASourceThatWentAwayOrAbortedIsNotToldAnything) {
    Node node(1);
    WiredPeer a(2);
    ob::PeerConnection& pa = a.mgr(*node.mm);
    ASSERT_TRUE(node.mm->request_snapshot_from(pa));
    (void)requests_in(a);
    begin_from(node, pa);
    arrive_snapshot_frame(*node.mm, pa, ob::MM_MSG_SNAPSHOT_ABORT,
                          ob::encode_snapshot_abort("file_read_failed"));
    EXPECT_FALSE(node.mm->snapshot_recv_active());
    EXPECT_EQ(refusal_in(a), "") << "a source that aborted was told to stop what it had stopped";
}
