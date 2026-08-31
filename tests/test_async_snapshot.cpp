// ── AsyncSnapshotBuilder — the handoff, with no peer, socket or engine in it ──
//
// Snapshot creation moved off the io thread in #79, and what moved with it is cross-thread state:
// a worker produces a result, the owner's loop collects it. That handoff is where the bug would
// be, so it is tested on its own, where a failure names the mechanism rather than a symptom three
// components away.

#include "orderbook/async_snapshot.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <thread>

using namespace ob;

namespace {

/// A producer that does not return until released, so a test can observe the in-flight state.
class Gate {
public:
    void wait() {
        std::unique_lock<std::mutex> lock(mtx_);
        cv_.wait(lock, [this] { return open_; });
    }
    void open() {
        {
            std::lock_guard<std::mutex> lock(mtx_);
            open_ = true;
        }
        cv_.notify_all();
    }

private:
    std::mutex              mtx_;
    std::condition_variable cv_;
    bool                    open_{false};
};

SnapshotWithSequenceState sample_snapshot() {
    SnapshotWithSequenceState s;
    s.manifest.total_rows  = 42;
    s.manifest.total_bytes = 4242;
    s.manifest.files.push_back(SnapshotFileEntry{"BTCUSDT/price.col", 128, 0xABCD});
    s.create_ms = 1.5;
    return s;
}

/// Collects with a bounded wait, so a broken handoff fails the test instead of hanging it.
std::optional<AsyncSnapshotBuilder::Result> collect(AsyncSnapshotBuilder& b,
                                                    std::chrono::milliseconds budget =
                                                        std::chrono::seconds(5)) {
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (std::chrono::steady_clock::now() < deadline) {
        auto r = b.take_result();
        if (r) return r;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return std::nullopt;
}

}  // namespace

TEST(AsyncSnapshotBuilder, DeliversWhatTheProducerReturned) {
    std::atomic<int> notified{0};
    AsyncSnapshotBuilder b([&] { notified.fetch_add(1); });

    ASSERT_TRUE(b.start(7, [] { return sample_snapshot(); }));

    auto r = collect(b);
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->ok);
    EXPECT_EQ(r->token, 7u);
    EXPECT_TRUE(r->error.empty());
    EXPECT_EQ(r->snap.manifest.total_rows, 42u);
    ASSERT_EQ(r->snap.manifest.files.size(), 1u);
    EXPECT_EQ(r->snap.manifest.files[0].path, "BTCUSDT/price.col");
    EXPECT_EQ(notified.load(), 1);
    EXPECT_FALSE(b.busy());
}

// The result must be visible by the time the notification arrives. The reverse order is not a
// slower handoff, it is a lost one: the owner wakes, finds nothing, and nothing wakes it again.
//
// The sleep inside the notification is what makes this deterministic, and the first version of this
// test did without it and was worthless: it woke the collector from a condition variable and raced
// it against the worker's very next line, which the worker won on every run. Reversing the two
// statements under test did not fail it once. Now the notification announces itself and then stays
// on the worker for a quarter of a second, so a collector that finds nothing has found the bug
// rather than lost a race. Note which way the remaining timing risk points: the correct order
// passes this test however the machine is loaded, so it cannot flake in CI — only a mutation could
// survive it on an implausibly slow one.
TEST(AsyncSnapshotBuilder, ThePublishHappensBeforeTheNotification) {
    std::mutex              mtx;
    std::condition_variable cv;
    bool                    fired = false;

    AsyncSnapshotBuilder b([&] {
        {
            std::lock_guard<std::mutex> lock(mtx);
            fired = true;
        }
        cv.notify_all();
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    });

    ASSERT_TRUE(b.start(1, [] { return sample_snapshot(); }));

    {
        std::unique_lock<std::mutex> lock(mtx);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&] { return fired; }));
    }

    // One attempt, straight after the notification, and it has to succeed.
    auto r = b.take_result();
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->ok);
    EXPECT_EQ(r->snap.manifest.total_rows, 42u);
}

TEST(AsyncSnapshotBuilder, RefusesASecondStartWhileOneIsRunning) {
    Gate gate;
    AsyncSnapshotBuilder b([] {});

    ASSERT_TRUE(b.start(1, [&] { gate.wait(); return sample_snapshot(); }));
    EXPECT_TRUE(b.busy());
    EXPECT_FALSE(b.start(2, [] { return sample_snapshot(); }));

    gate.open();
    auto r = collect(b);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->token, 1u);
}

// Being busy has to cover the window between finishing and being collected too. Otherwise a second
// start overwrites a result that nobody has read, and the peer that asked for it waits forever.
TEST(AsyncSnapshotBuilder, RefusesASecondStartWhileAResultIsUncollected) {
    std::atomic<bool> notified{false};
    AsyncSnapshotBuilder b([&] { notified.store(true); });

    ASSERT_TRUE(b.start(1, [] { return sample_snapshot(); }));
    while (!notified.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));

    EXPECT_TRUE(b.busy());
    EXPECT_FALSE(b.start(2, [] { return sample_snapshot(); }));

    auto r = b.take_result();
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->token, 1u);
    EXPECT_FALSE(b.busy());

    // And the slot is reusable once collected.
    EXPECT_TRUE(b.start(3, [] { return sample_snapshot(); }));
    auto r2 = collect(b);
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->token, 3u);
}

TEST(AsyncSnapshotBuilder, AThrowingProducerIsAResultNotACrash) {
    AsyncSnapshotBuilder b([] {});

    ASSERT_TRUE(b.start(9, []() -> SnapshotWithSequenceState {
        throw std::runtime_error("disk on fire");
    }));

    auto r = collect(b);
    ASSERT_TRUE(r.has_value());
    EXPECT_FALSE(r->ok);
    EXPECT_EQ(r->token, 9u);
    EXPECT_EQ(r->error, "disk on fire");
    EXPECT_FALSE(b.busy());
}

TEST(AsyncSnapshotBuilder, ANonStandardExceptionIsAlsoAResult) {
    AsyncSnapshotBuilder b([] {});
    ASSERT_TRUE(b.start(10, []() -> SnapshotWithSequenceState { throw 17; }));

    auto r = collect(b);
    ASSERT_TRUE(r.has_value());
    EXPECT_FALSE(r->ok);
    EXPECT_FALSE(r->error.empty());
}

TEST(AsyncSnapshotBuilder, NothingToCollectBeforeAnythingIsStarted) {
    AsyncSnapshotBuilder b([] {});
    EXPECT_FALSE(b.busy());
    EXPECT_FALSE(b.take_result().has_value());
}

TEST(AsyncSnapshotBuilder, ShutdownWaitsForTheWorkerAndDropsTheResult) {
    Gate gate;
    std::atomic<bool> producer_finished{false};
    AsyncSnapshotBuilder b([] {});

    ASSERT_TRUE(b.start(1, [&] {
        gate.wait();
        producer_finished.store(true);
        return sample_snapshot();
    }));

    std::thread opener([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        gate.open();
    });

    b.shutdown();
    opener.join();

    // shutdown() waits rather than cancels, so the producer ran to the end.
    EXPECT_TRUE(producer_finished.load());
    EXPECT_FALSE(b.busy());
    EXPECT_FALSE(b.take_result().has_value());

    // Idempotent, and the object is still usable afterwards.
    b.shutdown();
    EXPECT_TRUE(b.start(2, [] { return sample_snapshot(); }));
    auto r = collect(b);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->token, 2u);
}

// The destructor is the last line of defence: an owner destroyed with a snapshot in flight must not
// leave a thread holding a reference to it.
TEST(AsyncSnapshotBuilder, DestructorJoinsAWorkerStillRunning) {
    Gate gate;
    std::atomic<bool> producer_finished{false};

    std::thread opener;
    {
        AsyncSnapshotBuilder b([] {});
        ASSERT_TRUE(b.start(1, [&] {
            gate.wait();
            producer_finished.store(true);
            return sample_snapshot();
        }));
        opener = std::thread([&] {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            gate.open();
        });
    }
    opener.join();
    EXPECT_TRUE(producer_finished.load());
}

TEST(AsyncSnapshotBuilder, AMissingProducerIsRefused) {
    AsyncSnapshotBuilder b([] {});
    EXPECT_FALSE(b.start(1, AsyncSnapshotBuilder::Producer{}));
    EXPECT_FALSE(b.busy());
}
