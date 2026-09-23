// ChunkedQueue: the pending rows' container since stage 5 of #151 took the flush's drain out of the
// engine's lock. Chunks of four here, so every boundary is reached with a handful of elements.

#include "orderbook/chunked_queue.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <deque>
#include <memory>
#include <string>
#include <vector>

namespace {

using Queue = ob::ChunkedQueue<int, 4>;

std::vector<int> drain(Queue::Batch& b) {
    std::vector<int> out;
    for (auto& c : b.chunks) {
        for (size_t i = c->first; i < c->rows.size(); ++i) out.push_back(c->rows[i]);
    }
    return out;
}

std::vector<int> last(const Queue& q, size_t n) {
    std::vector<int> out;
    q.for_each_last(n, [&](int v) { out.push_back(v); });
    return out;
}

TEST(ChunkedQueue, TakeAllReturnsEverythingInOrderAcrossChunksAndEmptiesTheQueue) {
    Queue q(100);
    for (int i = 0; i < 10; ++i) q.push_back(int{i});
    EXPECT_EQ(q.size(), 10u);
    Queue::Batch b = q.take_all();
    EXPECT_TRUE(q.empty());
    EXPECT_EQ(b.chunks.size(), 3u);   // 4 + 4 + 2
    EXPECT_EQ(b.rows(), 10u);
    EXPECT_EQ(drain(b), (std::vector<int>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
}

TEST(ChunkedQueue, AChunkNeverMovesWhatItHolds) {
    // The reason this is not a vector: appending past a vector's capacity copies every element,
    // under the lock every writer needs. A chunk is reserved once and never grows.
    Queue q(100);
    q.push_back(0);
    Queue::Batch probe = q.take_all();
    const int* first = &probe.chunks[0]->rows[0];
    q.put_back_front(std::move(probe));
    for (int i = 1; i < 4; ++i) q.push_back(int{i});
    Queue::Batch b = q.take_all();
    ASSERT_EQ(b.chunks.size(), 1u);
    EXPECT_EQ(&b.chunks[0]->rows[0], first);
    EXPECT_GE(b.chunks[0]->rows.capacity(), Queue::chunk_rows());
}

TEST(ChunkedQueue, TheLastElementsAreWhatAWriterJustAppendedEvenAcrossAChunk) {
    Queue q(100);
    for (int i = 0; i < 6; ++i) q.push_back(int{i});
    EXPECT_EQ(last(q, 3), (std::vector<int>{3, 4, 5}));
    EXPECT_EQ(last(q, 1), (std::vector<int>{5}));
    EXPECT_EQ(last(q, 6), (std::vector<int>{0, 1, 2, 3, 4, 5}));
    EXPECT_EQ(last(q, 60), (std::vector<int>{0, 1, 2, 3, 4, 5}));
    EXPECT_TRUE(last(q, 0).empty());
}

TEST(ChunkedQueue, WhatABatchDidNotConsumeGoesBackInFrontOfWhatCameSince) {
    // A drain that stops part-way - a segment write the disk refused, #161 - leaves the rest to the
    // next flush, which must meet it before anything written in the meantime.
    Queue q(100);
    for (int i = 0; i < 7; ++i) q.push_back(int{i});
    Queue::Batch b = q.take_all();
    b.chunks[0]->first = 4;        // the first chunk consumed entirely
    b.chunks[1]->first = 1;        // one element of the second
    for (int i = 100; i < 103; ++i) q.push_back(int{i});   // written while the batch was out
    q.put_back_front(std::move(b));
    EXPECT_EQ(q.size(), 2u + 3u);
    Queue::Batch again = q.take_all();
    EXPECT_EQ(drain(again), (std::vector<int>{5, 6, 100, 101, 102}));
}

TEST(ChunkedQueue, AChunkPutBackWithAConsumedPrefixIsStillTheTailsNeighbour) {
    // for_each_last must count the chunks it walks by what is left in them.
    Queue q(100);
    for (int i = 0; i < 4; ++i) q.push_back(int{i});
    Queue::Batch b = q.take_all();
    b.chunks[0]->first = 2;
    q.put_back_front(std::move(b));
    q.push_back(9);
    EXPECT_EQ(last(q, 3), (std::vector<int>{2, 3, 9}));
}

TEST(ChunkedQueue, ChunksGivenBackAreReusedAndThoseBeyondTheBoundAreReturned) {
    Queue q(3);
    for (int i = 0; i < 12; ++i) q.push_back(int{i});   // three chunks queued
    Queue::Batch b = q.take_all();
    ASSERT_EQ(b.chunks.size(), 3u);
    std::vector<Queue::ChunkPtr> returned;
    for (auto& c : b.chunks) {
        c->rows.clear();
        if (auto r = q.give_back(std::move(c))) returned.push_back(std::move(r));
    }
    EXPECT_EQ(q.chunks_held(), 3u);
    EXPECT_TRUE(returned.empty());
    // With the bound reached, the next one comes back to the caller.
    for (int i = 0; i < 16; ++i) q.push_back(int{i});   // four chunks: three reused, one new
    Queue::Batch b2 = q.take_all();
    ASSERT_EQ(b2.chunks.size(), 4u);
    for (auto& c : b2.chunks) {
        c->rows.clear();
        if (auto r = q.give_back(std::move(c))) returned.push_back(std::move(r));
    }
    EXPECT_EQ(q.chunks_held(), 3u);
    EXPECT_EQ(returned.size(), 1u);
}

TEST(ChunkedQueue, ClearDiscardsWhatIsQueuedAndKeepsChunksUpToTheBound) {
    Queue q(2);
    for (int i = 0; i < 12; ++i) q.push_back(int{i});
    q.clear();
    EXPECT_TRUE(q.empty());
    EXPECT_EQ(q.chunks_held(), 2u);
    q.push_back(7);
    Queue::Batch b = q.take_all();
    EXPECT_EQ(drain(b), (std::vector<int>{7}));
}

TEST(ChunkedQueue, AnElementThatOwnsMemoryIsMovedInAndOut) {
    ob::ChunkedQueue<std::unique_ptr<std::string>, 2> q(10);
    q.push_back(std::make_unique<std::string>("a"));
    q.push_back(std::make_unique<std::string>("b"));
    q.push_back(std::make_unique<std::string>("c"));
    auto b = q.take_all();
    ASSERT_EQ(b.chunks.size(), 2u);
    EXPECT_EQ(*b.chunks[1]->rows[0], "c");
}

// The model: a deque. Any sequence of pushes, takes with partial consumption and a put-back, and
// clears leaves the queue holding what the deque holds, in its order - and never holding more
// chunks than its bound once the chunks a caller took have come back.
RC_GTEST_PROP(ChunkedQueueProperty, BehavesAsADequeAndStaysWithinItsBound, ()) {
    const size_t bound = *rc::gen::inRange<size_t>(1, 8);
    Queue q(bound);
    std::deque<int> model;
    int next = 0;
    const auto steps = *rc::gen::inRange(1, 60);
    for (int s = 0; s < steps; ++s) {
        const int op = *rc::gen::inRange(0, 4);
        if (op <= 1) {
            const int n = *rc::gen::inRange(0, 11);
            for (int i = 0; i < n; ++i) {
                q.push_back(int{next});
                model.push_back(next++);
            }
        } else if (op == 2) {
            // Take everything, consume a prefix, write a little more, put the rest back.
            Queue::Batch b = q.take_all();
            size_t consume = *rc::gen::inRange<size_t>(0, model.size() + 1);
            const size_t consumed = consume;
            for (auto& c : b.chunks) {
                const size_t here = std::min(consume, c->size());
                c->first += here;
                consume -= here;
            }
            for (size_t i = 0; i < consumed; ++i) model.pop_front();
            std::deque<int> meanwhile;
            const int n = *rc::gen::inRange(0, 6);
            for (int i = 0; i < n; ++i) {
                q.push_back(int{next});
                meanwhile.push_back(next++);
            }
            q.put_back_front(std::move(b));
            for (int v : meanwhile) model.push_back(v);
        } else {
            q.clear();
            model.clear();
        }
        RC_ASSERT(q.size() == model.size());
    }
    Queue::Batch all = q.take_all();
    const std::vector<int> got = drain(all);
    RC_ASSERT(got == std::vector<int>(model.begin(), model.end()));
    for (auto& c : all.chunks) {
        c->rows.clear();
        (void)q.give_back(std::move(c));
    }
    RC_ASSERT(q.chunks_held() <= bound);
}

}  // namespace
