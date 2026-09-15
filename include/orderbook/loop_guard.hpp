#pragma once

#include "orderbook/log_episode.hpp"
#include "orderbook/logger.hpp"
#include "orderbook/metrics.hpp"

#include <exception>

namespace ob {

/// One iteration of a loop that must not end on its first exception (#131).
///
/// The outer boundary (`run_thread_body`, #112) stops an escaping exception from ending the
/// process. It does not stop it from ending the *thread*, and for thirteen loops in `src/` a dead
/// thread is a subsystem that stops working while every outward signal stays healthy. Four of those
/// were closed one at a time in #112; this is the mechanism for the other seven, and it is one
/// mechanism rather than seven copies for the reason `LogEpisode` lives in its own header: the
/// second user is what turns a pattern into a mechanism, and two copies of "have I already said
/// this" drift apart.
///
/// Used as:
///
/// ```
/// LoopGuard guard{"shard_router", "a shard-map refresh", &registry};
/// while (running_) {
///     try {
///         one_pass();
///         guard.ok();
///     } catch (const std::exception& e) {
///         guard.caught(e);
///     }
///     nap();
/// }
/// ```
///
/// **The recovery line cannot fire in the iteration that threw**, because `ok()` is only reached
/// when the body did not. The mesh io loop paid for that with a log alternating ERROR / "handled
/// again" for three failing records, and got there by calling `end()` after the loop that opened
/// the episode; here the shape makes it impossible rather than careful.
///
/// **Pacing stays with the caller.** These seven loops wait in three different ways - a plain
/// `sleep_for`, a condition variable with the stop flag as its predicate, a deadline polled in
/// 50 ms steps - and a guard that owned the wait would have to know which. It also must not own it:
/// `ReplicationManager::run_loop()` showed that a boundary can *create* a busy-spin, and the answer
/// there was a pacing decision that belongs to that loop and nothing else.
///
/// **One counter for all seven, which is a decision and not a shortcut.** `ob_flush_errors_total`,
/// `ob_monitor_errors_total`, `ob_mm_io_errors_total`, `ob_repl_io_errors_total` and
/// `ob_peer_lease_errors_total` stay as they are: each of the first three was *measured* firing,
/// and each asks an operator for a different thing - free the disk, replace the device, look at
/// why a promotion stopped. None of these seven has a known trigger, so all seven ask for the
/// same thing: read the line, it names the loop. Seven registered counters nothing can reach
/// would be seven flat zeros dressed as coverage, which is what #117 was about.
///
/// Not thread-safe, deliberately, for `LogEpisode`'s reason: one loop thread owns one guard.
class LoopGuard {
public:
    /// `component` is the log component; `what` completes the sentence "<what> threw". `registry`
    /// may be null - the shard router and the client pool live in somebody else's process and have
    /// no registry to write to, and a guard that pretended otherwise would need a second mechanism.
    LoopGuard(const char* component, const char* what, MetricsRegistry* registry)
        : component_(component), what_(what), registry_(registry) {}

    /// The iteration threw. Counts it, and says so loudly exactly once per episode.
    void caught(const std::exception& e) {
        if (registry_ != nullptr) {
            registry_->increment_counter("ob_loop_errors_total");
        }
        if (episode_.begin()) {
            OB_LOG_ERROR(component_,
                         "%s threw and this iteration is abandoned; the loop continues and the "
                         "next iteration will be attempted: %s", what_, e.what());
        } else {
            OB_LOG_DEBUG(component_, "%s threw again (%llu consecutive): %s", what_,
                         static_cast<unsigned long long>(episode_.ticks()), e.what());
        }
    }

    /// The iteration completed. Closes an open episode, so silence after the ERROR never has to be
    /// read as "the condition went away".
    void ok() {
        if (const uint64_t held = episode_.end()) {
            OB_LOG_INFO(component_, "%s is working again after %llu that threw", what_,
                        static_cast<unsigned long long>(held));
        }
    }

    /// How long the open episode has run, 0 when none is. For a test that wants the count without
    /// closing it.
    uint64_t consecutive() const { return episode_.ticks(); }

private:
    const char*      component_;
    const char*      what_;
    MetricsRegistry* registry_;
    LogEpisode       episode_;
};

} // namespace ob
