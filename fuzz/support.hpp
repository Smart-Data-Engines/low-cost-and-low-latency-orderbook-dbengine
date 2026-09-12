#pragma once

#include "orderbook/logger.hpp"

#include <cstdlib>

namespace ob::fuzz {

inline void initialize(const char* target) {
    auto& logger = StructuredLogger::instance();
    logger.set_level(LogLevel::INFO);
    OB_LOG_INFO("fuzz", "Starting target=%s; expected parser refusals are not failures", target);
    logger.set_level(LogLevel::ERROR);
}

inline void require(bool condition, const char* message) {
    if (!condition) {
        OB_LOG_ERROR("fuzz", "Invariant failed: %s", message);
        std::abort();
    }
}

} // namespace ob::fuzz
