#include "orderbook/logger.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstring>

namespace ob {

// ── Singleton ─────────────────────────────────────────────────────────────────

StructuredLogger& StructuredLogger::instance() {
    static StructuredLogger s;
    return s;
}

// ── Level management ──────────────────────────────────────────────────────────

void StructuredLogger::set_level(LogLevel level) {
    level_.store(static_cast<int>(level), std::memory_order_relaxed);
}

LogLevel StructuredLogger::level() const {
    return static_cast<LogLevel>(level_.load(std::memory_order_relaxed));
}

// ── parse_level ───────────────────────────────────────────────────────────────

std::optional<LogLevel> StructuredLogger::parse_level(std::string_view str) {
    // Case-insensitive comparison
    std::string upper;
    upper.reserve(str.size());
    for (char c : str) upper += static_cast<char>(std::toupper(static_cast<unsigned char>(c)));

    if (upper == "ERROR") return LogLevel::ERROR;
    if (upper == "WARN")  return LogLevel::WARN;
    if (upper == "INFO")  return LogLevel::INFO;
    if (upper == "DEBUG") return LogLevel::DEBUG;
    return std::nullopt;
}

// ── escape_json ───────────────────────────────────────────────────────────────

std::string StructuredLogger::escape_json(std::string_view input) {
    std::string out;
    out.reserve(input.size() + 8);  // small headroom for escapes

    for (unsigned char ch : input) {
        switch (ch) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b";  break;
            case '\f': out += "\\f";  break;
            case '\n': out += "\\n";  break;
            case '\r': out += "\\r";  break;
            case '\t': out += "\\t";  break;
            default:
                if (ch < 0x20) {
                    // Control characters → \u00XX
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", ch);
                    out += buf;
                } else {
                    out += static_cast<char>(ch);
                }
                break;
        }
    }
    return out;
}

// ── Level name helper ─────────────────────────────────────────────────────────

static const char* level_name(LogLevel level) {
    switch (level) {
        case LogLevel::ERROR: return "ERROR";
        case LogLevel::WARN:  return "WARN";
        case LogLevel::INFO:  return "INFO";
        case LogLevel::DEBUG: return "DEBUG";
    }
    return "UNKNOWN";
}

// ── ISO 8601 timestamp with milliseconds ──────────────────────────────────────

static std::string iso8601_now() {
    using namespace std::chrono;
    auto now   = system_clock::now();
    auto tt    = system_clock::to_time_t(now);
    auto ms    = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;

    std::tm utc{};
    gmtime_r(&tt, &utc);

    char buf[64];
    std::snprintf(buf, sizeof(buf),
                  "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
                  utc.tm_year + 1900, utc.tm_mon + 1, utc.tm_mday,
                  utc.tm_hour, utc.tm_min, utc.tm_sec,
                  static_cast<int>(ms.count()));
    return buf;
}

// ── Core log method ───────────────────────────────────────────────────────────

void StructuredLogger::log(LogLevel level, const char* component,
                           const char* fmt, ...) {
    // Format the user message via va_args
    char msg_buf[2048];
    va_list args;
    va_start(args, fmt);
    std::vsnprintf(msg_buf, sizeof(msg_buf), fmt, args);
    va_end(args);

    // Build JSON line
    std::string ts = iso8601_now();
    std::string escaped_msg = escape_json(msg_buf);
    std::string escaped_comp = escape_json(component);

    // Output under mutex (best-effort, stderr is OS-buffered)
    std::lock_guard<std::mutex> lock(mtx_);
    std::fprintf(stderr,
                 "{\"ts\":\"%s\",\"level\":\"%s\",\"component\":\"%s\",\"msg\":\"%s\"}\n",
                 ts.c_str(), level_name(level),
                 escaped_comp.c_str(), escaped_msg.c_str());
}

} // namespace ob
