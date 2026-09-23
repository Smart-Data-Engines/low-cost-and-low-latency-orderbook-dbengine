#pragma once

// ── A FIFO in fixed-size chunks ───────────────────────────────────────────────
//
// What the engine keeps its pending rows in - the rows a flush has not yet taken into segments -
// since stage 5 of #151 moved the flush's drain out of the engine's lock.
//
// **Why not a vector.** The drain has to take every row queued so far in one short hold of the lock
// and work through them without it, while writers go on queueing. With one vector per side, swapped
// at each tick, both vectors keep the capacity of the largest tick they ever held, and resident
// memory doubled: measured on the m9g.xlarge, 163 -> 270 MiB at one connection and 158 -> 321 MiB at
// four, which turns the pending-row ceiling's promise ("a million rows, about a hundred megabytes")
// into twice that. Here the rows live in chunks of `kChunkRows`; taking them is moving a vector of
// chunk pointers, a drained chunk is cleared by whoever drained it and handed back, and the chunks
// kept for reuse are bounded - so what the queue holds is what is queued, plus the chunks the next
// writers will fill.
//
// A chunk never grows past `kChunkRows`, so appending never reallocates and never moves a row: a
// vector that outgrows its capacity copies every row it holds, under the lock every writer needs.
//
// Not thread-safe. Its owner serialises every call under its own lock; the chunks a caller has
// taken are the caller's alone until it gives them back, which is what lets them be drained and
// cleared without that lock.

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

namespace ob {

template <typename T, size_t kChunkRows = 4096>
class ChunkedQueue {
    static_assert(kChunkRows > 0);

public:
    /// Up to `kChunkRows` elements, of which those before `first` have been consumed.
    struct Chunk {
        std::vector<T> rows;
        size_t first{0};
        size_t size() const { return rows.size() - first; }
    };
    using ChunkPtr = std::unique_ptr<Chunk>;

    /// Chunks taken out of a queue, in order. Whoever holds a batch owns its chunks.
    struct Batch {
        std::vector<ChunkPtr> chunks;
        size_t rows() const {
            size_t n = 0;
            for (const auto& c : chunks) n += c->size();
            return n;
        }
    };

    /// `max_chunks_kept` bounds the chunks this queue holds, queued and spare together, beyond which
    /// a chunk given back is returned to the caller to free rather than kept.
    explicit ChunkedQueue(size_t max_chunks_kept) : max_kept_(max_chunks_kept) {}

    ChunkedQueue(const ChunkedQueue&) = delete;
    ChunkedQueue& operator=(const ChunkedQueue&) = delete;

    static constexpr size_t chunk_rows() { return kChunkRows; }

    void push_back(T&& value) {
        if (chunks_.empty() || chunks_.back()->rows.size() == kChunkRows) chunks_.push_back(fresh());
        chunks_.back()->rows.push_back(std::move(value));
        ++size_;
    }

    size_t size() const { return size_; }
    bool empty() const { return size_ == 0; }
    /// Chunks queued plus chunks kept for reuse - the memory this queue holds, in chunks.
    size_t chunks_held() const { return chunks_.size() + spare_.size(); }

    /// Every element queued, in order; the queue is empty afterwards. O(chunks).
    Batch take_all() {
        Batch b;
        b.chunks = std::move(chunks_);
        chunks_.clear();
        size_ = 0;
        return b;
    }

    /// What a batch has not consumed goes back **in front of** whatever was queued since, in its
    /// order: the caller could not finish with it, and whatever comes next must meet it first.
    /// Chunks with nothing left in them are given back for reuse instead. O(chunks).
    void put_back_front(Batch&& b) {
        std::vector<ChunkPtr> front;
        front.reserve(b.chunks.size() + chunks_.size());
        for (auto& c : b.chunks) {
            if (c->size() == 0) {
                c->rows.clear();
                c->first = 0;
                keep(std::move(c));
                continue;
            }
            size_ += c->size();
            front.push_back(std::move(c));
        }
        for (auto& c : chunks_) front.push_back(std::move(c));
        chunks_ = std::move(front);
        b.chunks.clear();
    }

    /// A chunk the caller has consumed and **cleared** comes back for reuse. Kept when the queue
    /// holds fewer than `max_chunks_kept` chunks; otherwise returned, for the caller to free where
    /// freeing does not hold its lock.
    ChunkPtr give_back(ChunkPtr c) {
        c->rows.clear();   // nothing to destroy when the caller cleared it, as it should have
        c->first = 0;
        if (chunks_held() >= max_kept_) return c;
        spare_.push_back(std::move(c));
        return nullptr;
    }

    /// Everything queued is discarded; its chunks are kept for reuse up to the bound.
    void clear() {
        for (auto& c : chunks_) {
            c->rows.clear();
            c->first = 0;
        }
        std::vector<ChunkPtr> old = std::move(chunks_);
        chunks_.clear();
        size_ = 0;
        for (auto& c : old) keep(std::move(c));
    }

    /// The last `n` elements, oldest first - what one writer has just appended, under the lock it
    /// appended them with.
    template <typename F>
    void for_each_last(size_t n, F&& f) const {
        if (n > size_) n = size_;
        size_t chunk = chunks_.size();
        size_t skip = 0;   // elements of `chunks_[chunk]` before the first one visited
        size_t left = n;
        if (left == 0) return;
        while (true) {
            --chunk;
            const size_t in = chunks_[chunk]->size();
            if (in >= left) {
                skip = in - left;
                break;
            }
            left -= in;
        }
        for (size_t c = chunk; c < chunks_.size(); ++c) {
            const Chunk& ch = *chunks_[c];
            for (size_t i = ch.first + (c == chunk ? skip : 0); i < ch.rows.size(); ++i) f(ch.rows[i]);
        }
    }

private:
    ChunkPtr fresh() {
        if (!spare_.empty()) {
            ChunkPtr c = std::move(spare_.back());
            spare_.pop_back();
            return c;
        }
        auto c = std::make_unique<Chunk>();
        c->rows.reserve(kChunkRows);
        return c;
    }

    void keep(ChunkPtr c) {
        if (chunks_held() < max_kept_) spare_.push_back(std::move(c));
    }

    std::vector<ChunkPtr> chunks_;
    std::vector<ChunkPtr> spare_;
    size_t size_{0};
    size_t max_kept_;
};

}  // namespace ob
