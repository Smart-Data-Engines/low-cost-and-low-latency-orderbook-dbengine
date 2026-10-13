// The ring pair of #52, step 2: its memory, its bytes, and its wake-ups.
//
// Two threads of one process stand in for the server and the client: the counters are shared
// atomics either way, and ThreadSanitizer can only see a race between threads.

#include "orderbook/ring.hpp"

#include <gtest/gtest.h>

#include <poll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace ob::ring;

namespace {

/// Both ends of the to-server direction, as the server and the client would hold them.
struct Pair {
    Mapping server;
    Mapping client;
    Pair(size_t capacity = kMinBytes) {
        std::string why;
        server = Mapping::create(capacity, why);
        EXPECT_TRUE(server.valid()) << why;
        client = Mapping::attach(::dup(server.fd()), capacity, why);
        EXPECT_TRUE(client.valid()) << why;
    }
    Writer client_writer() { return Writer(client.to_server(), client.to_server_data(), client.capacity()); }
    Reader server_reader() { return Reader(server.to_server(), server.to_server_data(), server.capacity()); }
};

/// The byte at position `i` of a stream, so a reader can check every byte it is given.
char byte_at(uint64_t i) { return static_cast<char>((i * 2654435761u) >> 13); }

}  // namespace

TEST(Ring, ACapacityIsAPowerOfTwoWithinBounds) {
    EXPECT_TRUE(valid_capacity(kMinBytes));
    EXPECT_TRUE(valid_capacity(kDefaultBytes));
    EXPECT_TRUE(valid_capacity(kMaxBytes));
    EXPECT_FALSE(valid_capacity(kMinBytes / 2));
    EXPECT_FALSE(valid_capacity(kMaxBytes * 2));
    EXPECT_FALSE(valid_capacity(kMinBytes + 64));
    EXPECT_FALSE(valid_capacity(0));
}

TEST(Ring, TheClientSeesWhatTheServerMadeAndNothingElse) {
    std::string why;
    Mapping server = Mapping::create(kMinBytes, why);
    ASSERT_TRUE(server.valid()) << why;
    EXPECT_EQ(server.bytes(), Layout::total(kMinBytes));

    Mapping wrong_size = Mapping::attach(::dup(server.fd()), kMinBytes * 2, why);
    EXPECT_FALSE(wrong_size.valid());
    EXPECT_NE(why.find("not the"), std::string::npos) << why;

    // Memory of the right size that nobody made rings in.
    const int stranger = ::memfd_create("not-rings", MFD_CLOEXEC);
    ASSERT_GE(stranger, 0);
    ASSERT_EQ(::ftruncate(stranger, static_cast<off_t>(Layout::total(kMinBytes))), 0);
    Mapping not_rings = Mapping::attach(stranger, kMinBytes, why);
    EXPECT_FALSE(not_rings.valid());
    EXPECT_NE(why.find("not rings"), std::string::npos) << why;

    // The right size and version and no magic: the magic is checked on its own.
    const int forged = ::memfd_create("forged-rings", MFD_CLOEXEC);
    ASSERT_GE(forged, 0);
    ASSERT_EQ(::ftruncate(forged, static_cast<off_t>(Layout::total(kMinBytes))), 0);
    const Block fake{0, kVersion, 0, kMinBytes};
    ASSERT_EQ(::pwrite(forged, &fake, sizeof(fake), 0), static_cast<ssize_t>(sizeof(fake)));
    Mapping forged_rings = Mapping::attach(forged, kMinBytes, why);
    EXPECT_FALSE(forged_rings.valid()) << "memory without the magic was taken for rings";

    Mapping client = Mapping::attach(::dup(server.fd()), kMinBytes, why);
    ASSERT_TRUE(client.valid()) << why;
}

TEST(Ring, TheClientCannotShrinkTheMemoryUnderTheServer) {
    std::string why;
    Mapping server = Mapping::create(kMinBytes, why);
    ASSERT_TRUE(server.valid()) << why;
    const int held = ::dup(server.fd());   // what the client is given
    EXPECT_NE(::ftruncate(held, 0), 0) << "the size is not sealed";
    EXPECT_EQ(errno, EPERM);
    EXPECT_NE(::ftruncate(held, static_cast<off_t>(server.bytes() * 2)), 0);
    ::close(held);
}

TEST(Ring, BytesComeOutAsTheyWentInAcrossTheEnd) {
    Pair pair;
    Writer w = pair.client_writer();
    Reader r = pair.server_reader();
    std::vector<char> in(1000), out(1000);
    uint64_t sent = 0, got = 0;
    // 1000-byte chunks through a 64 KiB ring, so the copy splits at the end over and over.
    for (int round = 0; round < 1000; ++round) {
        for (size_t i = 0; i < in.size(); ++i) in[i] = byte_at(sent + i);
        ASSERT_EQ(w.write(in.data(), in.size()), in.size());
        w.publish();
        sent += in.size();
        ASSERT_EQ(r.available(), in.size());
        ASSERT_EQ(r.read(out.data(), out.size()), out.size());
        for (size_t i = 0; i < out.size(); ++i) ASSERT_EQ(out[i], byte_at(got + i)) << "byte " << got + i;
        got += out.size();
    }
    EXPECT_FALSE(w.broken());
    EXPECT_FALSE(r.broken());
}

TEST(Ring, AFullRingTakesNothingAndAnEmptyOneGivesNothing) {
    Pair pair;
    Writer w = pair.client_writer();
    Reader r = pair.server_reader();
    std::vector<char> block(kMinBytes + 100, 'x');
    EXPECT_EQ(w.write(block.data(), block.size()), kMinBytes) << "more than the ring holds was taken";
    EXPECT_EQ(w.room(), 0u);
    EXPECT_EQ(w.write(block.data(), 1), 0u);
    w.publish();
    // Unpublished bytes are not there for the reader; published ones are, all of them.
    EXPECT_EQ(r.available(), kMinBytes);
    EXPECT_EQ(r.read(block.data(), block.size()), kMinBytes);
    EXPECT_EQ(r.available(), 0u);
    EXPECT_EQ(r.read(block.data(), 1), 0u);
    EXPECT_EQ(w.room(), kMinBytes);
}

TEST(Ring, TwoThreadsPassTenMillionBytesInOrder) {
    Pair pair;
    constexpr uint64_t kTotal = 10'000'000;
    std::atomic<bool> bad{false};
    std::thread producer([&] {
        Writer w = pair.client_writer();
        std::mt19937 rng(7);
        std::vector<char> chunk(4096);
        uint64_t sent = 0;
        while (sent < kTotal) {
            const size_t n = std::min<uint64_t>(kTotal - sent, 1 + rng() % chunk.size());
            for (size_t i = 0; i < n; ++i) chunk[i] = byte_at(sent + i);
            size_t done = 0;
            while (done < n) {
                done += w.write(chunk.data() + done, n - done);
                w.publish();
            }
            sent += n;
        }
        if (w.broken()) bad = true;
    });
    Reader r = pair.server_reader();
    std::vector<char> out(3000);
    uint64_t got = 0;
    while (got < kTotal && !bad) {
        const size_t n = r.read(out.data(), out.size());
        for (size_t i = 0; i < n; ++i) {
            if (out[i] != byte_at(got + i)) {
                bad = true;
                ADD_FAILURE() << "byte " << got + i << " is not the one written there";
                break;
            }
        }
        got += n;
    }
    producer.join();
    EXPECT_FALSE(bad);
    EXPECT_EQ(got, kTotal);
    EXPECT_FALSE(r.broken());
}

TEST(Ring, ASleepingReaderIsAlwaysWoken) {
    // The protocol's whole claim: a reader that decided to sleep either saw the bytes or is seen
    // asleep by the writer that published them. A lost wake-up shows as a poll that times out with
    // bytes in the ring, so every wait is bounded and counted.
    Pair pair;
    const int bell = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    ASSERT_GE(bell, 0);
    constexpr int kMessages = 50'000;
    std::atomic<int> rung{0};
    std::thread producer([&] {
        Writer w = pair.client_writer();
        char byte = 0;
        for (int i = 0; i < kMessages; ++i) {
            while (w.write(&byte, 1) == 0) std::this_thread::yield();
            if (w.publish()) {
                ring_bell(bell);
                rung.fetch_add(1, std::memory_order_relaxed);
            }
            // Now and then a pause, so the reader gets to sleep between bytes as well as to poll.
            if (i % 64 == 0) std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    });
    Reader r = pair.server_reader();
    int got = 0, slept = 0, lost = 0;
    char buf[64];
    while (got < kMessages) {
        const size_t n = r.read(buf, sizeof(buf));
        if (n > 0) { got += static_cast<int>(n); continue; }
        if (!r.prepare_to_sleep()) continue;
        ++slept;
        pollfd p{bell, POLLIN, 0};
        if (::poll(&p, 1, 2000) == 0) ++lost;   // two seconds with a byte waiting is a lost wake-up
        drain_bell(bell);
        r.woke();
    }
    producer.join();
    ::close(bell);
    EXPECT_EQ(lost, 0) << "a reader slept through bytes published for it";
    EXPECT_GT(slept, 0) << "the reader never slept, so the protocol was not exercised";
    EXPECT_GT(rung.load(), 0);
}

TEST(Ring, AFullWriterIsWokenWhenTheReaderMakesRoom) {
    Pair pair;
    const int bell = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    ASSERT_GE(bell, 0);
    constexpr uint64_t kTotal = 4 * kMinBytes;
    int lost = 0, waited = 0;
    std::thread consumer([&] {
        Reader r = pair.server_reader();
        std::vector<char> out(kMinBytes / 3);
        uint64_t got = 0;
        while (got < kTotal) {
            const size_t n = r.read(out.data(), out.size());
            got += n;
            if (n > 0 && r.producer_waiting()) ring_bell(bell);
            if (n == 0) std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    });
    Writer w = pair.client_writer();
    std::vector<char> chunk(5000, 'y');
    uint64_t sent = 0;
    while (sent < kTotal) {
        const size_t n = w.write(chunk.data(), std::min<uint64_t>(chunk.size(), kTotal - sent));
        w.publish();
        sent += n;
        if (n > 0 || !w.prepare_to_wait_for_room()) continue;
        ++waited;
        pollfd p{bell, POLLIN, 0};
        if (::poll(&p, 1, 2000) == 0) ++lost;
        drain_bell(bell);
        w.done_waiting_for_room();
    }
    consumer.join();
    ::close(bell);
    EXPECT_EQ(lost, 0) << "a writer waited for room that had been made";
    EXPECT_GT(waited, 0) << "the writer never waited, so the room protocol was not exercised";
}

TEST(Ring, AReaderAboutToSleepLooksAgain) {
    // What a race between threads hides: a byte published after the reader found the ring empty
    // and before it said it sleeps. The writer saw nobody asleep and rang nothing, so the reader's
    // second look is all that keeps the byte from waiting for the next one. One thread, in that
    // order.
    Pair pair;
    Writer w = pair.client_writer();
    Reader r = pair.server_reader();
    char c = 'r';
    EXPECT_EQ(r.read(&c, 1), 0u);
    ASSERT_EQ(w.write(&c, 1), 1u);
    EXPECT_FALSE(w.publish()) << "nobody was asleep yet";
    EXPECT_FALSE(r.prepare_to_sleep()) << "a reader slept with a byte in its ring";
    EXPECT_EQ(r.read(&c, 1), 1u);
    // The control: an empty ring lets it sleep, the next publish sees it asleep, and once it woke
    // it is not seen asleep any more.
    EXPECT_TRUE(r.prepare_to_sleep());
    ASSERT_EQ(w.write(&c, 1), 1u);
    EXPECT_TRUE(w.publish()) << "the writer did not see the reader asleep";
    r.woke();
    ASSERT_EQ(w.write(&c, 1), 1u);
    EXPECT_FALSE(w.publish()) << "a reader that woke is still seen asleep";
}

TEST(Ring, AWriterAboutToWaitForRoomLooksAgain) {
    // The same for the other direction of the rule: room made after the writer found the ring
    // full and before it said it waits, by a reader that saw nobody waiting.
    Pair pair;
    Writer w = pair.client_writer();
    Reader r = pair.server_reader();
    std::vector<char> block(kMinBytes, 'w');
    ASSERT_EQ(w.write(block.data(), block.size()), kMinBytes);
    w.publish();
    EXPECT_EQ(w.write(block.data(), 1), 0u);
    ASSERT_EQ(r.read(block.data(), 10), 10u);
    EXPECT_FALSE(r.producer_waiting()) << "nobody was waiting yet";
    EXPECT_FALSE(w.prepare_to_wait_for_room()) << "a writer waited with room in its ring";
    // The control: a full ring lets it wait, the next read sees it waiting, and once it is done
    // waiting it is not seen waiting any more.
    ASSERT_EQ(w.write(block.data(), 10), 10u);
    w.publish();
    EXPECT_TRUE(w.prepare_to_wait_for_room());
    ASSERT_EQ(r.read(block.data(), 1), 1u);
    EXPECT_TRUE(r.producer_waiting()) << "the reader did not see the writer waiting";
    w.done_waiting_for_room();
    EXPECT_FALSE(r.producer_waiting()) << "a writer done waiting is still seen waiting";
}

TEST(Ring, ACounterTheOtherSideCouldNotHaveLeftBreaksTheRing) {
    {
        // The writer's tail far past what the ring holds: more ready than was ever possible.
        Pair pair;
        Reader r = pair.server_reader();
        pair.client.to_server()->tail.store(kMinBytes + 1, std::memory_order_release);
        EXPECT_EQ(r.available(), 0u);
        EXPECT_TRUE(r.broken());
        char c;
        EXPECT_EQ(r.read(&c, 1), 0u);
    }
    {
        // The reader's head ahead of everything written.
        Pair pair;
        Writer w = pair.client_writer();
        pair.server.to_server()->head.store(10, std::memory_order_release);
        EXPECT_EQ(w.room(), 0u);
        EXPECT_TRUE(w.broken());
    }
    {
        // The reader's head going backwards.
        Pair pair;
        Writer w = pair.client_writer();
        Reader r = pair.server_reader();
        std::vector<char> data(100, 'z');
        w.write(data.data(), data.size());
        w.publish();
        r.read(data.data(), 60);
        EXPECT_EQ(w.room(), kMinBytes - 40);
        pair.server.to_server()->head.store(20, std::memory_order_release);
        EXPECT_EQ(w.room(), 0u);
        EXPECT_TRUE(w.broken());
    }
    {
        // The control: an honest pair in the same steps is not broken.
        Pair pair;
        Writer w = pair.client_writer();
        Reader r = pair.server_reader();
        std::vector<char> data(100, 'z');
        w.write(data.data(), data.size());
        w.publish();
        r.read(data.data(), 60);
        EXPECT_EQ(w.room(), kMinBytes - 40);
        EXPECT_FALSE(w.broken());
        EXPECT_FALSE(r.broken());
    }
}
