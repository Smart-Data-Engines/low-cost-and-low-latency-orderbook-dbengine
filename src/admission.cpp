// #190, step 5: writes admitted at the rate the device takes. See include/orderbook/admission.hpp.
#include "orderbook/admission.hpp"

#include <algorithm>

namespace ob {

namespace {

using Seconds = std::chrono::duration<double>;

}  // namespace

AdmissionController::AdmissionController(Config config) : config_(config) {}

void AdmissionController::set_enabled(bool enabled) {
    enabled_.store(enabled, std::memory_order_relaxed);
    if (enabled) return;
    std::lock_guard<std::mutex> lock(mtx_);
    active_.store(false, std::memory_order_relaxed);
    rate_ = 0.0;
    idle_ticks_ = 0;
}

bool AdmissionController::slow(uint64_t rows, Clock::duration took) const {
    return rows > 0 && Seconds(took) >= Seconds(config_.interval) * config_.slow_factor;
}

AdmissionController::Change AdmissionController::slow_locked(uint64_t rows, Clock::duration took) {
    // What the device took: the rows a tick took from the queue, in the time it took to sync them -
    // and, at the tick's end, to drain and seal them too. The floor keeps one bad tick from
    // stopping writes.
    const double measured =
        std::max(config_.floor_rows_per_s, static_cast<double>(rows) / Seconds(took).count());
    last_slow_ = Measurement{rows, took, measured};
    idle_ticks_ = 0;
    delayed_since_tick_ = 0;
    if (!active_.load(std::memory_order_relaxed)) {
        // Below what the tick measured, which leaves out the segments it sealed (see the class).
        rate_ = std::max(config_.floor_rows_per_s, measured * config_.begin_at);
        next_free_ = Clock::time_point{};
        delayed_batches_ = 0;
        delayed_total_ = Clock::duration::zero();
        active_.store(true, std::memory_order_relaxed);
        return Change::Began;
    }
    rate_ = std::min(rate_, measured);
    return Change::None;
}

AdmissionController::Change AdmissionController::on_slow_sync(uint64_t rows, Clock::duration took) {
    if (!enabled() || !slow(rows, took)) return Change::None;
    std::lock_guard<std::mutex> lock(mtx_);
    return slow_locked(rows, took);
}

AdmissionController::Change AdmissionController::on_tick(uint64_t rows, Clock::duration took) {
    if (!enabled()) return Change::None;
    std::lock_guard<std::mutex> lock(mtx_);

    if (slow(rows, took)) return slow_locked(rows, took);
    if (!active_.load(std::memory_order_relaxed)) return Change::None;

    // Not slow: the device had room for more than it was given, so give it more.
    rate_ *= config_.raise;
    // Ended only once nobody has waited for a whole run of ticks. While writers are still paced,
    // the rate is what keeps the queue from filling, and ending it would cost the stall again.
    if (delayed_since_tick_ == 0) {
        if (++idle_ticks_ >= config_.idle_ticks_to_end) {
            active_.store(false, std::memory_order_relaxed);
            rate_ = 0.0;
            idle_ticks_ = 0;
            return Change::Ended;
        }
    } else {
        idle_ticks_ = 0;
    }
    delayed_since_tick_ = 0;
    return Change::None;
}

AdmissionController::Clock::duration AdmissionController::admit(uint64_t rows, Clock::time_point now) {
    // Off - every node whose device keeps up - costs one relaxed load.
    if (rows == 0 || !active_.load(std::memory_order_relaxed)) return Clock::duration::zero();
    std::lock_guard<std::mutex> lock(mtx_);
    if (!active_.load(std::memory_order_relaxed) || rate_ <= 0.0) return Clock::duration::zero();

    // A token bucket kept as the time the rows admitted so far are paid up to, shared by every
    // writer, so that together they write at the rate. It may lag `now` by the burst - credit a
    // writer below the rate builds up - and the batch waits for whatever of its rows' time is left.
    const auto cost = std::chrono::duration_cast<Clock::duration>(Seconds(static_cast<double>(rows) / rate_));
    const auto start = std::max(now - config_.burst, next_free_);
    next_free_ = start + cost;
    if (next_free_ <= now) return Clock::duration::zero();
    auto delay = next_free_ - now;
    if (delay > config_.max_delay) {
        // Waited out in part; the rest is forgiven rather than left for the next batch, so that a
        // rate the floor holds up cannot build a debt no writer lives to pay.
        delay = config_.max_delay;
        next_free_ = now + delay;
    }
    ++delayed_since_tick_;
    ++delayed_batches_;
    delayed_total_ += delay;
    return delay;
}

double AdmissionController::rate() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return active_.load(std::memory_order_relaxed) ? rate_ : 0.0;
}

uint64_t AdmissionController::delayed_batches() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return delayed_batches_;
}

AdmissionController::Clock::duration AdmissionController::delayed_total() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return delayed_total_;
}

AdmissionController::Measurement AdmissionController::last_slow_tick() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return last_slow_;
}

}  // namespace ob
