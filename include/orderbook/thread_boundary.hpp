#pragma once

// An exception boundary for a thread body — roadmap #112.
//
// A joinable `std::thread` whose function lets an exception escape does not report an error: the
// runtime calls `std::terminate` and the **process** ends. This engine throws `std::runtime_error`
// from the WAL writer, the mmap store, the columnar store and several parsers, and sixteen of its
// seventeen `std::thread` constructions had no boundary at all when this was measured. One ENOSPC
// on the flush thread's checkpoint was enough to abort a node that was answering its clients
// correctly, in a loop, with nobody connected.
//
// So every thread body goes through here. The boundary is at the **construction site** rather than
// inside each loop, for two reasons. It is one grep-able property a static test can hold
// (`tests/test_thread_boundaries.cpp`), where "there is a `try` before the loop" needs a parser.
// And the one place that had reasoned about this - the snapshot worker in `async_snapshot.cpp` -
// still left the statements before its `try` and the mutex after its `catch` outside the guard,
// which is what a boundary placed inside a body tends to do.
//
// What this is not: a retry. Catching here means *that thread* is over, logged rather than silent.
// A loop that must survive its own failures needs a second boundary inside itself, per iteration,
// with a metric - which is a judgement per loop and is recorded in #112 rather than assumed here.

#include "orderbook/logger.hpp"

#include <exception>
#include <utility>

namespace ob {

/// Run a thread's body so that nothing reaches the runtime.
///
/// `noexcept` on purpose, and the consequence is stated rather than hidden: if the logger itself
/// throws, this terminates - which is what happens today anyway, and marking it makes the intent
/// ("nothing may escape from here") the compiler's business instead of a comment's.
template <typename Body>
void run_thread_body(const char* component, const char* name, Body&& body) noexcept {
    try {
        std::forward<Body>(body)();
    } catch (const std::exception& e) {
        OB_LOG_ERROR(component, "%s ended on an exception and that thread is gone: %s", name,
                     e.what());
    } catch (...) {
        OB_LOG_ERROR(component, "%s ended on a non-standard exception and that thread is gone",
                     name);
    }
}

} // namespace ob
