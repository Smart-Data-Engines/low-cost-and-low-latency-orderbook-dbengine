#include "orderbook/response_formatter.hpp"

#include <charconv>
#include <string>
#include <string_view>
#include <vector>

namespace ob {

// ── Helpers ───────────────────────────────────────────────────────────────────

static constexpr std::string_view kQueryHeader =
    "timestamp_ns\tprice\tquantity\torder_count\tside\tlevel";

static constexpr std::string_view kStatusHeader =
    "sessions\tqueries\tinserts";

/// Split a string_view by a single-character delimiter.
static std::vector<std::string_view> split(std::string_view sv, char delim) {
    std::vector<std::string_view> parts;
    size_t start = 0;
    while (start <= sv.size()) {
        auto pos = sv.find(delim, start);
        if (pos == std::string_view::npos) {
            parts.push_back(sv.substr(start));
            break;
        }
        parts.push_back(sv.substr(start, pos - start));
        start = pos + 1;
    }
    return parts;
}

// ── format_query_response ─────────────────────────────────────────────────────

std::string format_query_response(const std::vector<QueryResult>& rows) {
    std::string out;
    out.reserve(64 + rows.size() * 80);

    out += "OK\n";
    out += kQueryHeader;
    out += '\n';

    for (const auto& r : rows) {
        out += std::to_string(r.timestamp_ns);
        out += '\t';
        out += std::to_string(r.price);
        out += '\t';
        out += std::to_string(r.quantity);
        out += '\t';
        out += std::to_string(r.order_count);
        out += '\t';
        out += std::to_string(r.side);
        out += '\t';
        out += std::to_string(r.level);
        out += '\n';
    }

    out += '\n'; // empty line terminator
    return out;
}

// ── format_error ──────────────────────────────────────────────────────────────

std::string format_error(std::string_view message) {
    std::string out;
    out.reserve(5 + message.size());
    out += "ERR ";
    out += message;
    out += '\n';
    return out;
}

// ── format_ok ─────────────────────────────────────────────────────────────────

std::string format_ok() {
    return "OK\n\n";
}

// ── format_pong ───────────────────────────────────────────────────────────────

std::string format_pong() {
    return "PONG\n";
}

// ── format_status ─────────────────────────────────────────────────────────────

std::string format_status(const ServerStats& stats) {
    std::string out;
    out += "OK\n";
    out += kStatusHeader;
    out += '\n';
    out += std::to_string(stats.active_sessions.load(std::memory_order_relaxed));
    out += '\t';
    out += std::to_string(stats.total_queries.load(std::memory_order_relaxed));
    out += '\t';
    out += std::to_string(stats.total_inserts.load(std::memory_order_relaxed));
    out += '\n';
    out += '\n'; // empty line terminator
    return out;
}

// ── parse_response ────────────────────────────────────────────────────────────

ParsedResponse parse_response(std::string_view response) {
    ParsedResponse parsed{};
    parsed.is_error = false;

    if (response.empty()) return parsed;

    // Error response: "ERR <message>\n"
    if (response.size() >= 4 && response.substr(0, 4) == "ERR ") {
        parsed.is_error = true;
        auto msg = response.substr(4);
        // Strip trailing newline
        if (!msg.empty() && msg.back() == '\n') {
            msg.remove_suffix(1);
        }
        parsed.error_message = std::string(msg);
        return parsed;
    }

    // PONG response: "PONG\n"
    if (response.size() >= 4 && response.substr(0, 4) == "PONG") {
        // No headers, no rows — just a pong
        return parsed;
    }

    // OK response: "OK\n..." — parse TSV header and data rows
    if (response.size() >= 3 && response.substr(0, 3) == "OK\n") {
        auto body = response.substr(3);

        // Split body into lines
        auto lines = split(body, '\n');

        // First non-empty line is the header
        size_t idx = 0;

        // Find header line
        while (idx < lines.size() && lines[idx].empty()) ++idx;
        if (idx >= lines.size()) return parsed;

        // Parse header columns (tab-separated)
        auto header_line = lines[idx];
        parsed.header_columns = [&]() {
            auto cols = split(header_line, '\t');
            std::vector<std::string> result;
            result.reserve(cols.size());
            for (auto c : cols) result.emplace_back(c);
            return result;
        }();
        ++idx;

        // Parse data rows until empty line or end
        for (; idx < lines.size(); ++idx) {
            if (lines[idx].empty()) break; // terminator
            auto cells = split(lines[idx], '\t');
            std::vector<std::string> row;
            row.reserve(cells.size());
            for (auto c : cells) row.emplace_back(c);
            parsed.rows.push_back(std::move(row));
        }

        return parsed;
    }

    return parsed;
}

} // namespace ob
