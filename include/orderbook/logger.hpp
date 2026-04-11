#pragma once

#include <atomic>
#include <cstdarg>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

namespace ob {

// ── Log levels ────────────────────────────────────────────────────────────────
enum class LogLevel : int { ERROR = 0, WARN = 1, INFO = 2, DEBUG = 3 };

// ── StructuredLogger ──────────────────────────────────────────────────────────
// Global singleton emitting NDJSON log entries to stderr.
// Hot-path cost when level is filtered: single atomic load (<1µs).
class StructuredLogger {
public:
    static StructuredLogger& instance();

    // Level management
    void     set_level(LogLevel level);
    LogLevel level() const;

    /// Raw atomic level value — used by macros for fast comparison.
    int level_as_int() const noexcept { return level_.load(std::memory_order_relaxed); }

    /// Core log method — printf-style formatting.
    /// Outputs a single JSON line to stderr:
    ///   {"ts":"ISO8601","level":"INFO","component":"xxx","msg":"yyy"}
    void log(LogLevel level, const char* component, const char* fmt, ...)
#ifdef __GNUC__
        __attribute__((format(printf, 4, 5)))
#endif
    ;

    /// JSON-safe string escaping (static utility).
    static std::string escape_json(std::string_view input);

    /// Parse level from string (case-insensitive). Returns nullopt for invalid.
    static std::optional<LogLevel> parse_level(std::string_view str);

private:
    StructuredLogger() = default;
    StructuredLogger(const StructuredLogger&) = delete;
    StructuredLogger& operator=(const StructuredLogger&) = delete;

    std::atomic<int> level_{static_cast<int>(LogLevel::INFO)};
    std::mutex       mtx_;  // protects fprintf to stderr
};

} // namespace ob

// ── Logging macros ────────────────────────────────────────────────────────────
// Level check is a single atomic load; compiler eliminates formatting when
// the level is filtered out.

#define OB_LOG_ERROR(component, fmt, ...) \
    do { if (static_cast<int>(ob::LogLevel::ERROR) <= \
             ob::StructuredLogger::instance().level_as_int()) \
        ob::StructuredLogger::instance().log( \
            ob::LogLevel::ERROR, component, fmt, ##__VA_ARGS__); \
    } while(0)

#define OB_LOG_WARN(component, fmt, ...) \
    do { if (static_cast<int>(ob::LogLevel::WARN) <= \
             ob::StructuredLogger::instance().level_as_int()) \
        ob::StructuredLogger::instance().log( \
            ob::LogLevel::WARN, component, fmt, ##__VA_ARGS__); \
    } while(0)

#define OB_LOG_INFO(component, fmt, ...) \
    do { if (static_cast<int>(ob::LogLevel::INFO) <= \
             ob::StructuredLogger::instance().level_as_int()) \
        ob::StructuredLogger::instance().log( \
            ob::LogLevel::INFO, component, fmt, ##__VA_ARGS__); \
    } while(0)

#define OB_LOG_DEBUG(component, fmt, ...) \
    do { if (static_cast<int>(ob::LogLevel::DEBUG) <= \
             ob::StructuredLogger::instance().level_as_int()) \
        ob::StructuredLogger::instance().log( \
            ob::LogLevel::DEBUG, component, fmt, ##__VA_ARGS__); \
    } while(0)
