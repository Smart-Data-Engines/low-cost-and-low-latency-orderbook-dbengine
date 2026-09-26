// #165 part 2b: the flush tick merges a symbol's small segments into bigger ones.
//
// A merge moves through five steps, each on a later tick than the one before it, so that none of
// them depends on a write it has not seen reach the device:
//
//   1. its inputs are read whole and written as one segment into a working directory beside them,
//      under a name no reader takes for a segment (`ColumnarStore::kCompactingSuffix`);
//   2. a syncfs() takes that to the device - the tick's own, when it sealed, or one of the merge's;
//   3. under the index's lock, and only if the inputs are still indexed and still consecutive in
//      their symbol's delivery order, the directory is renamed to a segment's name and swapped for
//      the inputs in the index, in one step;
//   4. a syncfs() takes the rename to the device;
//   5. the inputs' directories are removed - once every scan that copied them before the swap has
//      finished, and never while a snapshot's pin is held.
//
// A crash anywhere leaves the inputs, the merged segment naming them, or both, and a start keeps
// each row once: it removes a working directory, and an input it finds beside the merged segment
// that names it (`ColumnarStore::rebuild_index_locked()`). The design and the measurement that sized
// it are in kiro-workspace/specs/segment-growth/design.md §2.5.

#include "orderbook/compaction.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/logger.hpp"
#include "orderbook/wall_clock.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <limits>
#include <string>
#include <unordered_set>
#include <vector>

namespace ob {

namespace fs = std::filesystem;

namespace {

using SteadyClock = std::chrono::steady_clock;

/// A partition is a symbol's segments whose end falls in one period: the period a store rolls its
/// segment over at, so a merge of one partition's segments ends in that period too, and retention
/// frees a merged segment at most a period after it would have freed the first of its inputs.
constexpr uint64_t kPeriodNs = ColumnarStore::kDefaultSegmentDurationNs;

uint64_t period_of(const SegmentMeta& meta) {
    return (meta.end_ts_ns / kPeriodNs) * kPeriodNs;
}

double ms_between(SteadyClock::time_point from, SteadyClock::time_point to) {
    return std::chrono::duration<double, std::milli>(to - from).count();
}

}  // namespace

// ── The snapshot's pin ────────────────────────────────────────────────────────

std::shared_ptr<const void> Engine::pin_segment_files() {
    // The count is shared with the handle rather than read through `this`: a handle can be released
    // by a sender after the engine that made it has gone.
    std::shared_ptr<std::atomic<int>> pins = segment_file_pins_;
    const int held = pins->fetch_add(1, std::memory_order_acq_rel) + 1;
    OB_LOG_DEBUG("engine", "segment files pinned for a snapshot: %d pin(s) held, so no merge and no "
                           "retention sweep moves them", held);
    return std::shared_ptr<const void>(pins.get(), [pins](const void*) {
        const int left = pins->fetch_sub(1, std::memory_order_acq_rel) - 1;
        OB_LOG_DEBUG("engine", "a snapshot released its pin on the segment files: %d left", left);
    });
}

// ── Which partitions to look at ───────────────────────────────────────────────

void Engine::note_segment_for_compaction(const SegmentMeta& meta, SteadyClock::time_point now,
                                         bool arrived) {
    if (!compaction_enabled_) return;
    const auto [it, added] =
        compaction_partitions_.try_emplace(CompactionKey{period_of(meta), meta.symbol, meta.exchange});
    CompactionPartition& p = it->second;
    // A merge's own output is not new data: a partition settles once no seal has added to it for a
    // while, however many merges it took to get there.
    if (arrived) p.last_added = now;
    // And a full segment gives it nothing to merge - it merges with nothing, and it ends no run it
    // is sealed after - so it wakes nothing up: at a rate just below the write ceiling a symbol
    // seals one every couple of ticks, and each look copies every segment of its hour.
    if (added || !arrived || meta.row_count < compaction::kFullRows) p.next_look = now;
}

void Engine::note_store_for_compaction() {
    if (!compaction_enabled_) return;
    const auto now = SteadyClock::now();
    for (const SegmentMeta& meta : combined_store_.index()) {
        note_segment_for_compaction(meta, now, /*arrived=*/true);
    }
    OB_LOG_DEBUG("engine", "compaction: %zu partition(s) to look at for merges",
                 compaction_partitions_.size());
}

void Engine::drop_compaction_locked() {
    const size_t staged = staged_merges_.size();
    size_t retired = 0;
    for (const auto& r : retired_inputs_) retired += r.dirs.size();
    // The working directories go with the store that is being replaced, and a rebuild removes any
    // left. The retired inputs are forgotten, not removed: their names may come back with the new
    // store, and removing one then would take a segment that is not a merge's input.
    staged_merges_.clear();
    retired_inputs_.clear();
    compaction_partitions_.clear();
    compaction_cursor_ = CompactionKey{};
    unmergeable_.clear();
    registry_.set_gauge("ob_segments_awaiting_removal", 0);
    if (staged > 0 || retired > 0) {
        OB_LOG_INFO("engine", "compaction: the store is being replaced, so %zu merge(s) written and "
                              "not published are dropped and %zu replaced segment(s) are not removed",
                    staged, retired);
    }
}

void Engine::finish_compaction_on_close() {
    size_t working = 0;
    for (const StagedMerge& s : staged_merges_) {
        std::error_code ec;
        fs::remove_all(s.output.dir_path, ec);
        if (!ec) ++working;
    }
    staged_merges_.clear();
    size_t waiting_before = 0;
    for (const RetiredInputs& r : retired_inputs_) waiting_before += r.dirs.size();
    if (waiting_before > 0) {
        bool frozen = false;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            frozen = checkpoints_frozen();
        }
        // After a failed sync no later one can be trusted to have written the renames, so the inputs
        // stay for the next start, which removes the ones a merged segment beside them holds.
        const bool wants_sync =
            std::any_of(retired_inputs_.begin(), retired_inputs_.end(),
                        [&](const RetiredInputs& r) { return r.synced_before == segment_syncs_; });
        if (!frozen && (!wants_sync || sync_for_merge() == 0)) remove_retired_inputs();
    }
    size_t waiting = 0;
    for (const RetiredInputs& r : retired_inputs_) waiting += r.dirs.size();
    if (working > 0 || waiting_before > 0) {
        OB_LOG_INFO("engine", "close: removed %zu merge(s) nothing published and %zu segment(s) merged "
                              "segments replaced; %zu wait for a query or a sync, and the next start "
                              "removes them",
                    working, waiting_before - waiting, waiting);
    }
}

std::vector<std::string> Engine::replaced_input_dirs() const {
    std::vector<std::string> dirs;
    for (const RetiredInputs& r : retired_inputs_) dirs.insert(dirs.end(), r.dirs.begin(), r.dirs.end());
    return dirs;
}

// ── The step ──────────────────────────────────────────────────────────────────

void Engine::compaction_step(size_t drained_rows) {
    if (!compaction_enabled_) return;
    // Nothing moves while a snapshot's manifest names these files: not a merge, not a removal.
    if (segment_file_pins_->load(std::memory_order_acquire) > 0) return;

    {
        std::lock_guard<std::mutex> lock(mtx_);
        // A node receiving a store is about to replace this one; and after a failed sync no later
        // sync can be trusted to have written what a merge would publish or remove (#160).
        if (is_bootstrapping() || checkpoints_frozen()) return;
    }

    // A sync, if what was written or published since the last one waits for it. A tick that sealed
    // has had one; an idle tick has not, and a merge must not wait for a seal that may never come.
    const bool wants_sync =
        std::any_of(staged_merges_.begin(), staged_merges_.end(),
                    [&](const StagedMerge& s) { return s.synced_before == segment_syncs_; }) ||
        std::any_of(retired_inputs_.begin(), retired_inputs_.end(),
                    [&](const RetiredInputs& r) { return r.synced_before == segment_syncs_; });
    if (wants_sync) {
        if (const int err = sync_for_merge(); err != 0) {
            registry_.increment_counter("ob_segment_sync_errors_total");
            std::lock_guard<std::mutex> lock(mtx_);
            freeze_checkpoints(std::string("A merge's segment sync failed (") + std::strerror(err) +
                               ")");
            return;
        }
    }

    remove_retired_inputs();
    publish_staged_merges();

    // More merges. A tick at the write ceiling leaves them - the cycle is what bounds a writer
    // there - unless none has run for kMaxDelay, and then it takes one, so a node that never goes
    // quiet still merges.
    const auto now = SteadyClock::now();
    const bool heavy = drained_rows > kSealRows;
    if (heavy) {
        // Looked for at most that often too: a look copies a partition's segments, and at the
        // ceiling a partition holds a full segment for every seal of the hour.
        if (now - last_merge_ < compaction::kMaxDelay ||
            now - last_heavy_look_ < compaction::kMaxDelay) {
            return;
        }
        last_heavy_look_ = now;
    }
    if (now < merge_backoff_until_) return;
    const auto budget_end = now + compaction::kTickBudget;

    // Under `none` a checkpoint reaches the device only when something syncs, and the merges' own
    // syncs are all that do: without one now and then no new segment of this WAL would be vouched
    // for, and none would merge. Once a second at most, only while a checkpoint waits for it, and
    // only in a tick that may merge - not at the write ceiling, which `none` is chosen for.
    if (fsync_policy_ == FsyncPolicy::NONE && now - last_merge_sync_ >= compaction::kVouchInterval) {
        bool waiting = false;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            waiting = checkpoint_seal_epoch_ > durable_seal_epoch_;
        }
        if (waiting) {
            if (const int err = sync_for_merge(); err != 0) {
                registry_.increment_counter("ob_segment_sync_errors_total");
                std::lock_guard<std::mutex> lock(mtx_);
                freeze_checkpoints(std::string("A merge's segment sync failed (") +
                                   std::strerror(err) + ")");
                return;
            }
        }
    }
    uint64_t vouched_epoch = 0;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        vouched_epoch = durable_seal_epoch_;
    }

    // The partitions due, from where the last tick stopped, so ten thousand of them are looked at
    // over ticks rather than in one.
    std::vector<CompactionKey> due;
    auto collect = [&](auto first, auto last) {
        for (auto i = first; i != last && due.size() < compaction::kLooksPerTick; ++i) {
            if (i->second.next_look <= now) due.push_back(i->first);
        }
    };
    const auto resume = compaction_partitions_.upper_bound(compaction_cursor_);
    collect(resume, compaction_partitions_.end());
    collect(compaction_partitions_.begin(), resume);

    size_t merges = 0;
    const size_t max_merges = heavy ? 1 : std::numeric_limits<size_t>::max();
    for (const CompactionKey& key : due) {
        if (merges >= max_merges) break;
        if (merges > 0 && SteadyClock::now() >= budget_end) break;
        const auto it = compaction_partitions_.find(key);
        if (it == compaction_partitions_.end()) continue;
        compaction_cursor_ = key;
        look_at_partition(it, now, vouched_epoch, merges, max_merges, budget_end);
    }
    if (merges > 0) last_merge_ = now;
}

void Engine::look_at_partition(std::map<CompactionKey, CompactionPartition>::iterator it,
                               SteadyClock::time_point now, uint64_t vouched_epoch, size_t& merges,
                               size_t max_merges, SteadyClock::time_point budget_end) {
    const CompactionKey key = it->first;
    CompactionPartition& part = it->second;
    const ColumnarStore::PartitionView view =
        combined_store_.partition_view(key.symbol, key.exchange, key.period_start,
                                       key.period_start + kPeriodNs);
    if (std::none_of(view.member.begin(), view.member.end(), [](bool m) { return m; })) {
        compaction_partitions_.erase(it);
        return;
    }

    // The inputs of merges written and not yet published are taken; a segment of this node's WAL is
    // mergeable only once a checkpoint on the device vouches for it, because the merged segment's
    // epoch is its inputs' highest and a start keeps it only if the last checkpoint does.
    std::unordered_set<std::string> taken;
    for (const StagedMerge& s : staged_merges_) {
        for (const SegmentMeta& in : s.inputs) taken.insert(in.dir_path);
    }
    std::vector<compaction::Candidate> candidates;
    candidates.reserve(view.segments.size());
    // A member that waits only for a checkpoint on the device - a seal of this tick, whose checkpoint
    // the next tick's WAL sync takes there, or under `none` the merges' next sync. Looked at again
    // next tick: left to the partition's settling, the last seals of an hour waited for it.
    bool vouching = false;
    for (size_t i = 0; i < view.segments.size(); ++i) {
        const SegmentMeta& m = view.segments[i];
        compaction::Candidate c;
        c.rows         = m.row_count;
        c.level        = m.merge_level;
        c.member       = view.member[i];
        c.wal_identity = m.wal_identity;
        c.start_ts_ns  = m.start_ts_ns;
        c.end_ts_ns    = m.end_ts_ns;
        const compaction::SegmentFacts facts{m.wal_identity, m.time_range_is_rows,
                                             m.format_version == kColumnarFormatVersion,
                                             m.seal_epoch};
        const bool free = taken.count(m.dir_path) == 0 && unmergeable_.count(m.dir_path) == 0;
        c.eligible = free && compaction::may_merge(facts, wal_identity_, vouched_epoch);
        vouching = vouching || (c.member && free && !c.eligible &&
                                compaction::may_merge(facts, wal_identity_, UINT64_MAX));
        candidates.push_back(c);
    }

    // Settled: the period is over, by the wall clock rows are stamped with, and nothing has been
    // sealed into it for as long - so the levels no longer have anything to wait for.
    const auto since_added =
        std::chrono::duration_cast<std::chrono::nanoseconds>(now - part.last_added);
    const auto until_settled =
        compaction::until_settled(key.period_start + kPeriodNs, wall_clock_ns(), since_added);
    const bool settled = until_settled == std::chrono::nanoseconds::zero();
    const std::vector<compaction::Run> runs = compaction::plan(candidates, settled);

    for (const compaction::Run& run : runs) {
        if (merges >= max_merges) break;
        if (merges > 0 && SteadyClock::now() >= budget_end) break;
        const std::vector<SegmentMeta> inputs(
            view.segments.begin() + static_cast<std::ptrdiff_t>(run.first),
            view.segments.begin() + static_cast<std::ptrdiff_t>(run.first + run.count));
        if (!stage_merge(inputs)) break;
        ++merges;
    }

    if (!runs.empty() || vouching) {
        // What the budget left, what the merges just written make possible once published, and what
        // a checkpoint on the device is about to make mergeable.
        part.next_look = now;
        return;
    }
    if (settled) {
        // Nothing left to merge in a period that is over: a seal of a late row brings it back.
        compaction_partitions_.erase(it);
        return;
    }
    // Nothing yet. A seal into it brings the next look forward; otherwise it is its settling.
    part.next_look = now + std::chrono::duration_cast<SteadyClock::duration>(until_settled);
}

int Engine::sync_for_merge() {
    // Caller holds flush_mtx_ and not mtx_.
    if (data_dir_fd_ < 0) return EBADF;
    // The checkpoint appended last, read before the sync so the sync covers it: a checkpoint is
    // written to the file when it is appended, and only ever appended under flush_mtx_.
    uint64_t appended_epoch = 0;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        appended_epoch = checkpoint_seal_epoch_;
    }
    const auto started = SteadyClock::now();
    if (::syncfs(data_dir_fd_) != 0) return errno;
    ++segment_syncs_;
    last_merge_sync_ = SteadyClock::now();
    if (fsync_policy_ == FsyncPolicy::NONE) {
        // Nothing else syncs under `none`, so this is what puts a checkpoint on the device - and
        // what may say so to the merges (#165 part 2b).
        std::lock_guard<std::mutex> lock(mtx_);
        durable_seal_epoch_ = std::max(durable_seal_epoch_, appended_epoch);
    }
    OB_LOG_DEBUG("engine", "compaction: synced the data directory in %.2f ms", ms_between(started,
                                                                                    last_merge_sync_));
    return 0;
}

// ── Step 1: a merge written ───────────────────────────────────────────────────

bool Engine::stage_merge(const std::vector<SegmentMeta>& inputs) {
    const auto started = SteadyClock::now();
    const SegmentMeta& first = inputs.front();
    uint64_t rows = 0;
    uint64_t start = std::numeric_limits<uint64_t>::max();
    uint64_t end = 0;
    uint64_t last_row = 0;
    uint64_t epoch = 0;
    uint32_t level = 0;
    uint32_t file = 0;
    uint64_t offset = 0;
    std::vector<SegmentInput> named;
    named.reserve(inputs.size());
    for (const SegmentMeta& in : inputs) {
        rows += in.row_count;
        start = std::min(start, in.start_ts_ns);
        end = std::max(end, in.end_ts_ns);
        last_row = std::max(last_row, in.last_row_ts_ns);
        epoch = std::max(epoch, in.seal_epoch);
        level = std::max(level, in.merge_level);
        if (in.wal_file_index > file || (in.wal_file_index == file && in.wal_byte_offset > offset)) {
            file = in.wal_file_index;
            offset = in.wal_byte_offset;
        }
        named.push_back(SegmentInput::of(in));
    }
    // Its own number, so no two merges - two runs of one range, or one whose working directory a
    // failed removal left - share a working directory.
    const std::string name = std::to_string(start) + "_" + std::to_string(end) + "_" +
                             std::to_string(++merge_seq_) +
                             std::string(ColumnarStore::kCompactingSuffix);
    const std::string dir = (fs::path(first.dir_path).parent_path() / name).string();

    // A period no row rolls over at: every input's rows are in the one period this partition is.
    ColumnarStore writer(base_dir_, std::numeric_limits<uint64_t>::max(),
                         ColumnarStore::OwnIndex::kNo);
    writer.set_symbol_exchange(first.symbol, first.exchange);
    // The inputs' WAL, and the latest position and epoch among them: the replay filter takes a
    // symbol's latest position and a start judges a segment by its epoch, and a merge must change
    // neither answer.
    writer.set_wal_position(first.wal_identity, file, offset);
    writer.set_seal_epoch(epoch);
    writer.set_lineage(level + 1, std::move(named), last_row);
    // Through the seals' buffers, as a seal writes: the flush thread holds them, and a merge of the
    // cap's rows is as big as a seal.
    writer.swap_buffers(seal_buffers_);
    struct Returned {
        ColumnarStore& writer;
        ColumnarStore::ColumnBuffers& buffers;
        ~Returned() {
            // A writer destroyed with rows in it writes them as a segment of its own; what a merge
            // did not publish must leave nothing behind.
            writer.abandon_active();
            writer.swap_buffers(buffers);
        }
    } returned{writer, seal_buffers_};
    writer.reserve_rows(rows);

    for (const SegmentMeta& in : inputs) {
        if (!combined_store_.read_segment(in, [&](const SnapshotRow& r) { writer.append(r); })) {
            unmergeable_.insert(in.dir_path);
            OB_LOG_WARN("engine", "compaction: %s cannot be read whole, so it stays as it is and merges "
                                  "with nothing for the life of this process",
                        in.dir_path.c_str());
            return false;
        }
    }
    std::optional<SegmentMeta> out;
    try {
        out = writer.flush_segment_into(dir);
    } catch (const std::exception& e) {
        registry_.increment_counter("ob_compaction_errors_total");
        merge_backoff_until_ = SteadyClock::now() + compaction::kMaxDelay;
        if (merge_failures_.begin()) {
            OB_LOG_WARN("engine", "compaction: a merge of %zu segment(s) of %s.%s could not be written, "
                                  "so merging pauses for %lld s and the segments stay as they are: %s",
                        inputs.size(), first.symbol.c_str(), first.exchange.c_str(),
                        static_cast<long long>(compaction::kMaxDelay.count()), e.what());
        } else {
            OB_LOG_DEBUG("engine", "compaction: a merge could not be written again: %s", e.what());
        }
        return false;
    }
    if (!out || out->row_count != rows) {
        OB_LOG_ERROR("engine", "compaction: a merge of %zu segment(s) of %s.%s wrote %llu of their "
                               "%llu row(s); it is not published",
                     inputs.size(), first.symbol.c_str(), first.exchange.c_str(),
                     static_cast<unsigned long long>(out ? out->row_count : 0),
                     static_cast<unsigned long long>(rows));
        std::error_code ec;
        fs::remove_all(dir, ec);
        return false;
    }
    if (const uint64_t ended = merge_failures_.end()) {
        OB_LOG_INFO("engine", "compaction: merges are written again after %llu failed attempt(s)",
                    static_cast<unsigned long long>(ended));
    }
    OB_LOG_DEBUG("engine", "compaction: %zu segment(s) of %s.%s, %llu row(s), merged into level %u in "
                           "%.2f ms, waiting for a sync before it is published",
                 inputs.size(), first.symbol.c_str(), first.exchange.c_str(),
                 static_cast<unsigned long long>(rows), level + 1,
                 ms_between(started, SteadyClock::now()));
    staged_merges_.push_back(StagedMerge{inputs, std::move(*out), segment_syncs_});
    return true;
}

// ── Step 3: published ─────────────────────────────────────────────────────────

bool Engine::publish_merged_dir(SegmentMeta& output) {
    const fs::path from(output.dir_path);
    const std::string base =
        std::to_string(output.start_ts_ns) + "_" + std::to_string(output.end_ts_ns);
    for (uint32_t n = 0; n < 10'000; ++n) {
        const fs::path to = from.parent_path() / (n == 0 ? base : base + "_" + std::to_string(n));
        int rc = ::renameat2(AT_FDCWD, from.c_str(), AT_FDCWD, to.c_str(), RENAME_NOREPLACE);
        if (rc != 0 && (errno == EINVAL || errno == ENOSYS)) {
            // A filesystem without RENAME_NOREPLACE. Asking first is not a race here: nothing but
            // this thread creates a directory under a symbol's - seals and merges both hold
            // flush_mtx_ - and a plain rename() would replace an empty directory it found.
            std::error_code ec;
            if (fs::exists(to, ec)) {
                errno = EEXIST;
            } else {
                rc = ::rename(from.c_str(), to.c_str());
            }
        }
        if (rc == 0) {
            output.dir_path = to.string();
            return true;
        }
        if (errno == EEXIST || errno == ENOTEMPTY) continue;
        OB_LOG_WARN("engine", "compaction: cannot publish %s as %s: %s", from.c_str(), to.c_str(),
                    std::strerror(errno));
        return false;
    }
    OB_LOG_WARN("engine", "compaction: no free name for %s beside its inputs", from.c_str());
    return false;
}

void Engine::publish_staged_merges() {
    if (staged_merges_.empty()) return;
    const auto now = SteadyClock::now();
    size_t published = 0;
    for (auto it = staged_merges_.begin(); it != staged_merges_.end();) {
        if (it->synced_before == segment_syncs_) {
            ++it;   // not on the device yet
            continue;
        }
        const auto result = combined_store_.replace_segments(
            it->inputs, it->output, [this](SegmentMeta& out) { return publish_merged_dir(out); });
        if (result.outcome == ColumnarStore::Replaced::kYes) {
            RetiredInputs retired;
            retired.dirs.reserve(it->inputs.size());
            for (const SegmentMeta& in : it->inputs) retired.dirs.push_back(in.dir_path);
            retired.readers = result.readers_before;
            retired.synced_before = segment_syncs_;
            retired_inputs_.push_back(std::move(retired));
            registry_.increment_counter("ob_compactions_total");
            registry_.increment_counter("ob_compaction_inputs_total", it->inputs.size());
            registry_.increment_counter("ob_compaction_rows_total", it->output.row_count);
            note_segment_for_compaction(it->output, now, /*arrived=*/false);
            ++published;
        } else {
            // The inputs stay, and a later look merges them again if it still can: a sweep took one,
            // or a seal put a segment between them.
            if (result.outcome == ColumnarStore::Replaced::kPublishFailed) {
                registry_.increment_counter("ob_compaction_errors_total");
            }
            std::error_code ec;
            fs::remove_all(it->output.dir_path, ec);
            OB_LOG_DEBUG("engine", "compaction: the merge written as %s was not published, so it is "
                                   "removed and its %zu input(s) stay%s",
                         it->output.dir_path.c_str(), it->inputs.size(),
                         ec ? " (and it could not all be removed)" : "");
            note_segment_for_compaction(it->inputs.front(), now, /*arrived=*/false);
        }
        it = staged_merges_.erase(it);
    }
    if (published > 0) {
        registry_.set_gauge("ob_segment_count",
                            static_cast<int64_t>(combined_store_.segment_count()));
        // The inputs just retired wait from now, not from the next step's look at them.
        size_t waiting = 0;
        for (const RetiredInputs& r : retired_inputs_) waiting += r.dirs.size();
        registry_.set_gauge("ob_segments_awaiting_removal", static_cast<int64_t>(waiting));
    }
}

// ── Step 5: the inputs removed ────────────────────────────────────────────────

void Engine::remove_retired_inputs() {
    size_t waiting = 0;
    size_t removed = 0;
    for (auto it = retired_inputs_.begin(); it != retired_inputs_.end();) {
        // The rename has to be on the device first - were it lost and the removal kept, a power cut
        // would take both - and every scan that copied an input before the swap reads it still.
        if (it->synced_before == segment_syncs_ || !it->readers.expired()) {
            waiting += it->dirs.size();
            ++it;
            continue;
        }
        for (const std::string& dir : it->dirs) {
            std::error_code ec;
            fs::remove_all(dir, ec);
            if (ec) {
                // Out of the index already: the next start finds it beside the merged segment that
                // names it, and removes it then.
                OB_LOG_WARN("engine", "compaction: cannot remove %s, which a merged segment replaced "
                                      "(%s); the next start removes it",
                            dir.c_str(), ec.message().c_str());
            } else {
                ++removed;
            }
        }
        it = retired_inputs_.erase(it);
    }
    if (removed > 0) {
        OB_LOG_DEBUG("engine", "compaction: removed %zu segment(s) merged segments replaced; %zu wait "
                               "for a sync or a scan", removed, waiting);
    }
    registry_.set_gauge("ob_segments_awaiting_removal", static_cast<int64_t>(waiting));
}

}  // namespace ob
