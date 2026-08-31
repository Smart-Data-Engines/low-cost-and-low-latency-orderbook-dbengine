#include "orderbook/hlc.hpp"

#include <algorithm>
#include <cstdint>
#include <charconv>
#include <cstdlib>
#include <ctime>
#include <inttypes.h>

namespace ob {

// ── HLCTimestamp — text serialization ─────────────────────────────────────────

std::string HLCTimestamp::to_string() const {
    // Format: "<physical_ns>.<logical>.<node_id>"
    return std::to_string(physical_ns) + "." +
           std::to_string(logical) + "." +
           std::to_string(node_id);
}

std::optional<HLCTimestamp> HLCTimestamp::from_string(std::string_view str) {
    std::string error;
    return from_string(str, error);
}

std::optional<HLCTimestamp> HLCTimestamp::from_string(std::string_view str,
                                                      std::string& error) {
    if (str.empty()) {
        error = "empty input string";
        OB_LOG_DEBUG("hlc", "from_string: %s", error.c_str());
        return std::nullopt;
    }

    // Find first dot — separates physical_ns from logical
    auto dot1 = str.find('.');
    if (dot1 == std::string_view::npos) {
        error = "missing first '.' separator after physical_ns";
        OB_LOG_DEBUG("hlc", "from_string: %s in \"%.*s\"",
                     error.c_str(), static_cast<int>(str.size()), str.data());
        return std::nullopt;
    }

    // Find second dot — separates logical from node_id
    auto dot2 = str.find('.', dot1 + 1);
    if (dot2 == std::string_view::npos) {
        error = "missing second '.' separator after logical";
        OB_LOG_DEBUG("hlc", "from_string: %s in \"%.*s\"",
                     error.c_str(), static_cast<int>(str.size()), str.data());
        return std::nullopt;
    }

    // Check for extra dots
    auto dot3 = str.find('.', dot2 + 1);
    if (dot3 != std::string_view::npos) {
        error = "unexpected extra '.' at position " + std::to_string(dot3);
        OB_LOG_DEBUG("hlc", "from_string: %s in \"%.*s\"",
                     error.c_str(), static_cast<int>(str.size()), str.data());
        return std::nullopt;
    }

    std::string_view phys_sv = str.substr(0, dot1);
    std::string_view logi_sv = str.substr(dot1 + 1, dot2 - dot1 - 1);
    std::string_view node_sv = str.substr(dot2 + 1);

    if (phys_sv.empty()) {
        error = "empty physical_ns field at position 0";
        OB_LOG_DEBUG("hlc", "from_string: %s", error.c_str());
        return std::nullopt;
    }
    if (logi_sv.empty()) {
        error = "empty logical field at position " + std::to_string(dot1 + 1);
        OB_LOG_DEBUG("hlc", "from_string: %s", error.c_str());
        return std::nullopt;
    }
    if (node_sv.empty()) {
        error = "empty node_id field at position " + std::to_string(dot2 + 1);
        OB_LOG_DEBUG("hlc", "from_string: %s", error.c_str());
        return std::nullopt;
    }

    HLCTimestamp ts{};

    // Parse physical_ns (uint64)
    {
        auto [ptr, ec] = std::from_chars(phys_sv.data(),
                                         phys_sv.data() + phys_sv.size(),
                                         ts.physical_ns);
        if (ec != std::errc{} || ptr != phys_sv.data() + phys_sv.size()) {
            error = "invalid physical_ns value at position 0: \"" +
                    std::string(phys_sv) + "\"";
            OB_LOG_DEBUG("hlc", "from_string: %s", error.c_str());
            return std::nullopt;
        }
    }

    // Parse logical (uint16) — parse into a wider type first to detect overflow
    {
        uint32_t tmp{};
        auto [ptr, ec] = std::from_chars(logi_sv.data(),
                                         logi_sv.data() + logi_sv.size(),
                                         tmp);
        if (ec != std::errc{} || ptr != logi_sv.data() + logi_sv.size()) {
            error = "invalid logical value at position " +
                    std::to_string(dot1 + 1) + ": \"" +
                    std::string(logi_sv) + "\"";
            OB_LOG_DEBUG("hlc", "from_string: %s", error.c_str());
            return std::nullopt;
        }
        if (tmp > UINT16_MAX) {
            error = "logical value overflow at position " +
                    std::to_string(dot1 + 1) + ": " + std::string(logi_sv) +
                    " exceeds uint16 max (65535)";
            OB_LOG_DEBUG("hlc", "from_string: %s", error.c_str());
            return std::nullopt;
        }
        ts.logical = static_cast<uint16_t>(tmp);
    }

    // Parse node_id (uint16)
    {
        uint32_t tmp{};
        auto [ptr, ec] = std::from_chars(node_sv.data(),
                                         node_sv.data() + node_sv.size(),
                                         tmp);
        if (ec != std::errc{} || ptr != node_sv.data() + node_sv.size()) {
            error = "invalid node_id value at position " +
                    std::to_string(dot2 + 1) + ": \"" +
                    std::string(node_sv) + "\"";
            OB_LOG_DEBUG("hlc", "from_string: %s", error.c_str());
            return std::nullopt;
        }
        if (tmp > UINT16_MAX) {
            error = "node_id value overflow at position " +
                    std::to_string(dot2 + 1) + ": " + std::string(node_sv) +
                    " exceeds uint16 max (65535)";
            OB_LOG_DEBUG("hlc", "from_string: %s", error.c_str());
            return std::nullopt;
        }
        ts.node_id = static_cast<uint16_t>(tmp);
    }

    return ts;
}

// ── HLCTimestamp — pretty-print ───────────────────────────────────────────────

std::string HLCTimestamp::pretty_print() const {
    // Format: "2024-01-15T10:30:00.123456789Z L=42 N=3"
    // Convert physical_ns to seconds + nanosecond remainder
    auto secs = static_cast<time_t>(physical_ns / 1'000'000'000ULL);
    auto nanos = static_cast<uint32_t>(physical_ns % 1'000'000'000ULL);

    struct tm utc{};
    gmtime_r(&secs, &utc);

    char buf[64]{};
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &utc);

    char result[128]{};
    std::snprintf(result, sizeof(result),
                  "%s.%09uZ L=%u N=%u",
                  buf, nanos, logical, node_id);
    return result;
}

// ── HybridLogicalClock ────────────────────────────────────────────────────────

HybridLogicalClock::HybridLogicalClock(uint16_t node_id)
    : node_id_(node_id) {
    last_.node_id = node_id_;
}

namespace {

/// Absolute distance between two nanosecond stamps, clamped to something representable.
///
/// One function rather than the same three lines in two places, and that is the whole point of it
/// existing. #83 found the signed form overflowing in `update()` and fixed it there; the identical
/// arithmetic in `tick_local()`, three lines away, was left alone and kept failing UBSan on a
/// property test that only reaches it for extreme generated values. The fix had gone where the
/// reproducer pointed instead of to every site of the same expression.
///
/// The subtraction is unsigned, so it cannot overflow and needs no sign test - `-drift` on
/// `INT64_MIN` was the second undefined step in the original three lines. The clamp keeps the
/// result representable, and a drift reported at `INT64_MAX` says "absurd" as well as any other
/// number would.
int64_t drift_between(uint64_t a, uint64_t b) {
    const uint64_t magnitude = (a > b) ? (a - b) : (b - a);
    return static_cast<int64_t>(std::min<uint64_t>(magnitude, static_cast<uint64_t>(INT64_MAX)));
}

} // namespace

HLCTimestamp HybridLogicalClock::tick_local() {
    std::lock_guard<std::mutex> lock(mtx_);

    uint64_t now = wall_clock_ns();
    uint64_t new_physical = std::max(now, last_.physical_ns);

    uint16_t new_logical{0};
    if (new_physical > last_.physical_ns) {
        new_logical = 0;
    } else {
        new_logical = static_cast<uint16_t>(last_.logical + 1);
    }

    last_ = HLCTimestamp{new_physical, new_logical, node_id_};

    // Track drift: distance between the wall clock and the HLC physical component.
    //
    // `new_physical` is `max(now, last_.physical_ns)`, and `last_` is whatever `update()` last
    // stored - which is `max(now, last_.physical_ns, remote.physical_ns)`, so a peer that sends a
    // nonsense timestamp **poisons the state** and this line trips over it on the next local tick.
    // That is why fixing the arithmetic in `update()` alone was not enough: it sanitised the
    // computation at the point of arrival and left the poisoned value behind.
    const int64_t drift = drift_between(new_physical, now);
    if (drift > max_drift_ns_) {
        max_drift_ns_ = drift;
    }

    // Warn if drift exceeds 1 second
    if (drift > 1'000'000'000LL) {
        OB_LOG_WARN("hlc", "HLC drift exceeds 1s: drift_ns=%ld", drift);
    }

    OB_LOG_DEBUG("hlc", "tick_local: physical=%" PRIu64 " logical=%u node=%u",
                 last_.physical_ns, last_.logical, last_.node_id);

    return last_;
}

HLCTimestamp HybridLogicalClock::tick_receive(const HLCTimestamp& remote) {
    std::lock_guard<std::mutex> lock(mtx_);

    uint64_t now = wall_clock_ns();
    uint64_t new_physical = std::max({now, last_.physical_ns, remote.physical_ns});

    uint16_t new_logical{0};
    if (new_physical > last_.physical_ns && new_physical > remote.physical_ns) {
        // Wall clock advanced past both — reset logical
        new_logical = 0;
    } else if (new_physical == last_.physical_ns &&
               new_physical == remote.physical_ns) {
        // All three equal — take max logical and increment
        new_logical = static_cast<uint16_t>(
            std::max(last_.logical, remote.logical) + 1);
    } else if (new_physical == last_.physical_ns) {
        // Local physical matches — increment local logical
        new_logical = static_cast<uint16_t>(last_.logical + 1);
    } else if (new_physical == remote.physical_ns) {
        // Remote physical matches — increment remote logical
        new_logical = static_cast<uint16_t>(remote.logical + 1);
    }

    last_ = HLCTimestamp{new_physical, new_logical, node_id_};

    // Track drift. See `drift_between()`: the signed form overflowed here when the physical
    // component was large, and `new_physical` comes from a **peer's** timestamp on the wire, so a
    // node sending a nonsense value caused undefined behaviour on every node that received it
    // rather than merely a wrong number. UBSan reported it as
    // "-7914833802811814732 - 1788012145016597349 cannot be represented in type 'long int'".
    const int64_t drift = drift_between(new_physical, now);
    if (drift > max_drift_ns_) {
        max_drift_ns_ = drift;
    }

    // Warn if drift exceeds 1 second
    if (drift > 1'000'000'000LL) {
        OB_LOG_WARN("hlc", "HLC drift exceeds 1s: drift_ns=%ld", drift);
    }

    OB_LOG_DEBUG("hlc", "tick_receive: remote={%" PRIu64 ",%u,%u} result={%" PRIu64 ",%u,%u}",
                 remote.physical_ns, remote.logical, remote.node_id,
                 last_.physical_ns, last_.logical, last_.node_id);

    return last_;
}

HLCTimestamp HybridLogicalClock::current() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return last_;
}

int64_t HybridLogicalClock::max_drift_ns() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return max_drift_ns_;
}

void HybridLogicalClock::reset_drift() {
    std::lock_guard<std::mutex> lock(mtx_);
    max_drift_ns_ = 0;
}

uint64_t HybridLogicalClock::wall_clock_ns() {
    struct timespec ts{};
    clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL +
           static_cast<uint64_t>(ts.tv_nsec);
}

} // namespace ob
