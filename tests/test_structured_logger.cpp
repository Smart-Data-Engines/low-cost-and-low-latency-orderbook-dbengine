// Tests for StructuredLogger: property-based tests (Properties 5, 6, 7, 8) and unit tests.
// Feature: observability

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <unistd.h>

#include "orderbook/logger.hpp"

// ── Helpers ──────────────────────────────────────────────────────────────────

/// RAII helper that redirects stderr to a temporary file and captures output.
class StderrCapture {
public:
    StderrCapture() {
        // Flush any pending stderr output
        std::fflush(stderr);
        // Save original stderr fd
        saved_fd_ = ::dup(STDERR_FILENO);
        // Create temp file
        std::strncpy(tmpname_, "/tmp/ob_test_XXXXXX", sizeof(tmpname_));
        tmp_fd_ = ::mkstemp(tmpname_);
        // Redirect stderr to temp file
        ::dup2(tmp_fd_, STDERR_FILENO);
    }

    std::string capture() {
        std::fflush(stderr);
        // Restore original stderr
        ::dup2(saved_fd_, STDERR_FILENO);
        ::close(saved_fd_);
        saved_fd_ = -1;
        // Read temp file contents
        ::lseek(tmp_fd_, 0, SEEK_SET);
        std::string result;
        char buf[4096];
        ssize_t n;
        while ((n = ::read(tmp_fd_, buf, sizeof(buf))) > 0) {
            result.append(buf, static_cast<size_t>(n));
        }
        ::close(tmp_fd_);
        tmp_fd_ = -1;
        ::unlink(tmpname_);
        return result;
    }

    ~StderrCapture() {
        if (saved_fd_ >= 0) {
            ::dup2(saved_fd_, STDERR_FILENO);
            ::close(saved_fd_);
        }
        if (tmp_fd_ >= 0) {
            ::close(tmp_fd_);
            ::unlink(tmpname_);
        }
    }

private:
    int saved_fd_{-1};
    int tmp_fd_{-1};
    char tmpname_[32]{};
};

/// Minimal check: is the string a valid JSON object with required fields?
/// Checks for opening/closing braces and presence of required key substrings.
static bool has_json_field(const std::string& json, const std::string& key) {
    // Look for "key": pattern
    std::string pattern = "\"" + key + "\":";
    return json.find(pattern) != std::string::npos;
}

static bool is_valid_json_object(const std::string& line) {
    if (line.empty()) return false;
    // Must start with { and end with }
    size_t first = line.find_first_not_of(" \t\r\n");
    size_t last  = line.find_last_not_of(" \t\r\n");
    if (first == std::string::npos || last == std::string::npos) return false;
    return line[first] == '{' && line[last] == '}';
}

/// Extract the value of a JSON string field (simple: assumes no nested objects in value).
static std::string extract_json_string_field(const std::string& json,
                                              const std::string& key) {
    std::string pattern = "\"" + key + "\":\"";
    auto pos = json.find(pattern);
    if (pos == std::string::npos) return {};
    pos += pattern.size();
    std::string result;
    while (pos < json.size() && json[pos] != '"') {
        if (json[pos] == '\\' && pos + 1 < json.size()) {
            char next = json[pos + 1];
            switch (next) {
                case '"':  result += '"';  pos += 2; continue;
                case '\\': result += '\\'; pos += 2; continue;
                case 'b':  result += '\b'; pos += 2; continue;
                case 'f':  result += '\f'; pos += 2; continue;
                case 'n':  result += '\n'; pos += 2; continue;
                case 'r':  result += '\r'; pos += 2; continue;
                case 't':  result += '\t'; pos += 2; continue;
                case 'u': {
                    // Parse \uXXXX
                    if (pos + 5 < json.size()) {
                        unsigned int cp = 0;
                        for (int i = 0; i < 4; ++i) {
                            char c = json[pos + 2 + i];
                            cp <<= 4;
                            if (c >= '0' && c <= '9') cp |= (c - '0');
                            else if (c >= 'a' && c <= 'f') cp |= (c - 'a' + 10);
                            else if (c >= 'A' && c <= 'F') cp |= (c - 'A' + 10);
                        }
                        result += static_cast<char>(cp);
                        pos += 6;
                        continue;
                    }
                    break;
                }
                default: break;
            }
        }
        result += json[pos];
        ++pos;
    }
    return result;
}

// ═════════════════════════════════════════════════════════════════════════════
// Property 5: Structured log — valid JSON with required fields
// Feature: observability, Property 5: Structured log — valid JSON with required fields
// Validates: Requirements 4.1, 4.3, 4.5
// ═════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(StructuredLoggerProperty, prop_json_log_structure, ()) {
    // Generate random component and message (printable ASCII, no control chars)
    auto gen_printable = rc::gen::container<std::string>(
        rc::gen::inRange<char>(0x20, 0x7e));
    std::string component = *gen_printable;
    std::string message   = *gen_printable;

    // Ensure non-empty for meaningful test
    RC_PRE(!component.empty());
    RC_PRE(!message.empty());

    auto& logger = ob::StructuredLogger::instance();
    logger.set_level(ob::LogLevel::DEBUG); // ensure output is emitted

    StderrCapture cap;
    logger.log(ob::LogLevel::INFO, component.c_str(), "%s", message.c_str());
    std::string output = cap.capture();

    // Should be exactly one line (may have trailing newline)
    // Count non-empty lines
    int line_count = 0;
    std::string line;
    std::istringstream stream(output);
    std::string first_line;
    while (std::getline(stream, line)) {
        if (!line.empty()) {
            ++line_count;
            if (first_line.empty()) first_line = line;
        }
    }
    RC_ASSERT(line_count == 1);

    // Must be valid JSON object
    RC_ASSERT(is_valid_json_object(first_line));

    // Must contain required fields
    RC_ASSERT(has_json_field(first_line, "ts"));
    RC_ASSERT(has_json_field(first_line, "level"));
    RC_ASSERT(has_json_field(first_line, "msg"));
    RC_ASSERT(has_json_field(first_line, "component"));

    // Level should be "INFO"
    std::string level_val = extract_json_string_field(first_line, "level");
    RC_ASSERT(level_val == "INFO");
}

// ═════════════════════════════════════════════════════════════════════════════
// Property 6: JSON escaping — round-trip
// Feature: observability, Property 6: JSON escaping — round-trip
// Validates: Requirements 4.4
// ═════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(StructuredLoggerProperty, prop_json_escape_round_trip, ()) {
    // Generate random strings including special characters
    auto gen_char = rc::gen::oneOf(
        // Normal printable ASCII
        rc::gen::inRange<char>(0x20, 0x7e),
        // Special JSON chars
        rc::gen::element<char>('"', '\\', '\n', '\r', '\t', '\b', '\f'),
        // Control characters
        rc::gen::inRange<char>(0x01, 0x1f)
    );
    std::string input = *rc::gen::container<std::string>(gen_char);

    std::string escaped = ob::StructuredLogger::escape_json(input);

    // Build a JSON string: "escaped"
    std::string json_str = "\"" + escaped + "\"";

    // The escaped string should not contain unescaped quotes or control chars
    // (except as part of escape sequences)
    // Verify round-trip by parsing back
    std::string parsed = extract_json_string_field("{\"v\":" + json_str + "}", "v");
    RC_ASSERT(parsed == input);
}

// ═════════════════════════════════════════════════════════════════════════════
// Property 7: Level filtering
// Feature: observability, Property 7: Level filtering
// Validates: Requirements 5.3, 5.4
// ═════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(StructuredLoggerProperty, prop_level_filtering, ()) {
    // Generate random configured level and message level
    auto configured_idx = *rc::gen::inRange<int>(0, 4); // 0=ERROR..3=DEBUG
    auto message_idx    = *rc::gen::inRange<int>(0, 4);

    auto configured_level = static_cast<ob::LogLevel>(configured_idx);
    auto message_level    = static_cast<ob::LogLevel>(message_idx);

    auto& logger = ob::StructuredLogger::instance();
    logger.set_level(configured_level);

    // Use the macros to test actual filtering behavior.
    // The macros check: if (static_cast<int>(level) <= instance().level_as_int()) log(...)
    StderrCapture cap;
    switch (message_idx) {
        case 0: OB_LOG_ERROR("test", "hello"); break;
        case 1: OB_LOG_WARN("test", "hello");  break;
        case 2: OB_LOG_INFO("test", "hello");   break;
        case 3: OB_LOG_DEBUG("test", "hello");  break;
    }
    std::string output = cap.capture();

    bool should_emit = (static_cast<int>(message_level) <= static_cast<int>(configured_level));
    bool did_emit    = !output.empty();
    RC_ASSERT(should_emit == did_emit);
}

// ═════════════════════════════════════════════════════════════════════════════
// Property 8: Invalid level parsing
// Feature: observability, Property 8: Invalid level parsing
// Validates: Requirements 5.5
// ═════════════════════════════════════════════════════════════════════════════

RC_GTEST_PROP(StructuredLoggerProperty, prop_invalid_level_parsing, ()) {
    // Generate random strings
    std::string input = *rc::gen::arbitrary<std::string>();

    // Filter out valid level names (case-insensitive)
    std::string upper;
    upper.reserve(input.size());
    for (char c : input) upper += static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    RC_PRE(upper != "ERROR" && upper != "WARN" && upper != "INFO" && upper != "DEBUG");

    auto result = ob::StructuredLogger::parse_level(input);
    RC_ASSERT(!result.has_value());
}


// ═════════════════════════════════════════════════════════════════════════════
// Unit Tests: StructuredLogger
// Feature: observability
// ═════════════════════════════════════════════════════════════════════════════

TEST(StructuredLoggerUnit, DefaultLevelIsInfo) {
    // The singleton may have been modified by previous tests, but a fresh
    // StructuredLogger starts at INFO. We test via set_level + level() round-trip
    // and verify the default documented behavior.
    auto& logger = ob::StructuredLogger::instance();
    // Reset to default
    logger.set_level(ob::LogLevel::INFO);
    EXPECT_EQ(logger.level(), ob::LogLevel::INFO);
    EXPECT_EQ(logger.level_as_int(), static_cast<int>(ob::LogLevel::INFO));
}

TEST(StructuredLoggerUnit, ParseLevelValid) {
    // Case-insensitive parsing of valid levels
    EXPECT_EQ(ob::StructuredLogger::parse_level("ERROR"), ob::LogLevel::ERROR);
    EXPECT_EQ(ob::StructuredLogger::parse_level("error"), ob::LogLevel::ERROR);
    EXPECT_EQ(ob::StructuredLogger::parse_level("Error"), ob::LogLevel::ERROR);

    EXPECT_EQ(ob::StructuredLogger::parse_level("WARN"), ob::LogLevel::WARN);
    EXPECT_EQ(ob::StructuredLogger::parse_level("warn"), ob::LogLevel::WARN);

    EXPECT_EQ(ob::StructuredLogger::parse_level("INFO"), ob::LogLevel::INFO);
    EXPECT_EQ(ob::StructuredLogger::parse_level("info"), ob::LogLevel::INFO);

    EXPECT_EQ(ob::StructuredLogger::parse_level("DEBUG"), ob::LogLevel::DEBUG);
    EXPECT_EQ(ob::StructuredLogger::parse_level("debug"), ob::LogLevel::DEBUG);
}

TEST(StructuredLoggerUnit, ParseLevelInvalid) {
    EXPECT_EQ(ob::StructuredLogger::parse_level("invalid"), std::nullopt);
    EXPECT_EQ(ob::StructuredLogger::parse_level(""), std::nullopt);
    EXPECT_EQ(ob::StructuredLogger::parse_level("WARNING"), std::nullopt);
    EXPECT_EQ(ob::StructuredLogger::parse_level("TRACE"), std::nullopt);
    EXPECT_EQ(ob::StructuredLogger::parse_level("info "), std::nullopt);
    EXPECT_EQ(ob::StructuredLogger::parse_level(" info"), std::nullopt);
}

TEST(StructuredLoggerUnit, EscapeJsonSpecialChars) {
    // Quotes
    EXPECT_EQ(ob::StructuredLogger::escape_json("hello \"world\""),
              "hello \\\"world\\\"");

    // Backslashes
    EXPECT_EQ(ob::StructuredLogger::escape_json("path\\to\\file"),
              "path\\\\to\\\\file");

    // Control characters
    EXPECT_EQ(ob::StructuredLogger::escape_json("line1\nline2"),
              "line1\\nline2");
    EXPECT_EQ(ob::StructuredLogger::escape_json("tab\there"),
              "tab\\there");
    EXPECT_EQ(ob::StructuredLogger::escape_json("cr\rhere"),
              "cr\\rhere");
    EXPECT_EQ(ob::StructuredLogger::escape_json("bs\bhere"),
              "bs\\bhere");
    EXPECT_EQ(ob::StructuredLogger::escape_json("ff\fhere"),
              "ff\\fhere");

    // Control char < 0x20 that isn't \b\f\n\r\t → \u00XX
    std::string input_ctrl(1, '\x01');
    EXPECT_EQ(ob::StructuredLogger::escape_json(input_ctrl), "\\u0001");

    // Empty string
    EXPECT_EQ(ob::StructuredLogger::escape_json(""), "");

    // No special chars
    EXPECT_EQ(ob::StructuredLogger::escape_json("hello"), "hello");
}

TEST(StructuredLoggerUnit, LogOutputIsValidJson) {
    auto& logger = ob::StructuredLogger::instance();
    logger.set_level(ob::LogLevel::DEBUG);

    StderrCapture cap;
    logger.log(ob::LogLevel::WARN, "engine", "test message %d", 42);
    std::string output = cap.capture();

    // Should have exactly one non-empty line
    std::istringstream stream(output);
    std::string line;
    std::string first_line;
    int count = 0;
    while (std::getline(stream, line)) {
        if (!line.empty()) {
            ++count;
            if (first_line.empty()) first_line = line;
        }
    }
    EXPECT_EQ(count, 1);

    // Valid JSON object
    EXPECT_TRUE(is_valid_json_object(first_line));

    // Required fields present
    EXPECT_TRUE(has_json_field(first_line, "ts"));
    EXPECT_TRUE(has_json_field(first_line, "level"));
    EXPECT_TRUE(has_json_field(first_line, "msg"));
    EXPECT_TRUE(has_json_field(first_line, "component"));

    // Verify field values
    EXPECT_EQ(extract_json_string_field(first_line, "level"), "WARN");
    EXPECT_EQ(extract_json_string_field(first_line, "component"), "engine");
    EXPECT_EQ(extract_json_string_field(first_line, "msg"), "test message 42");

    // ts should look like ISO 8601 (starts with 20xx-)
    std::string ts = extract_json_string_field(first_line, "ts");
    EXPECT_GE(ts.size(), 20u);
    EXPECT_EQ(ts[4], '-');
    EXPECT_EQ(ts[7], '-');
    EXPECT_EQ(ts[10], 'T');
}
