#include "orderbook/command_parser.hpp"

#include <algorithm>
#include <charconv>
#include <cctype>
#include <string>
#include <string_view>
#include <vector>

namespace ob {

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Trim trailing whitespace and newlines from a string_view.
static std::string_view rtrim(std::string_view sv) {
    while (!sv.empty() && (sv.back() == '\n' || sv.back() == '\r' ||
                           sv.back() == ' '  || sv.back() == '\t')) {
        sv.remove_suffix(1);
    }
    return sv;
}

/// Extract whitespace-delimited tokens from a string_view.
static std::vector<std::string_view> tokenize(std::string_view sv) {
    std::vector<std::string_view> tokens;
    size_t i = 0;
    while (i < sv.size()) {
        // skip whitespace
        while (i < sv.size() && (sv[i] == ' ' || sv[i] == '\t')) ++i;
        if (i >= sv.size()) break;
        size_t start = i;
        while (i < sv.size() && sv[i] != ' ' && sv[i] != '\t') ++i;
        tokens.push_back(sv.substr(start, i - start));
    }
    return tokens;
}

/// Case-insensitive comparison of a string_view against a literal.
static bool iequals(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i) {
        if (std::toupper(static_cast<unsigned char>(a[i])) !=
            std::toupper(static_cast<unsigned char>(b[i])))
            return false;
    }
    return true;
}

// ── parse_command ──────────────────────────────────────────────────────────────

Command parse_command(std::string_view line) {
    Command cmd{};
    cmd.type = CommandType::UNKNOWN;

    std::string_view trimmed = rtrim(line);
    if (trimmed.empty()) return cmd;

    auto tokens = tokenize(trimmed);
    if (tokens.empty()) return cmd;

    std::string_view first = tokens[0];

    if (iequals(first, "SELECT")) {
        cmd.type = CommandType::SELECT;
        // Store the entire trimmed line (including "SELECT") as raw_sql
        cmd.raw_sql = std::string(trimmed);
        return cmd;
    }

    if (iequals(first, "INSERT")) {
        // Need at least 6 tokens: INSERT symbol exchange side price qty
        if (tokens.size() < 6) return cmd;

        InsertArgs args;
        args.symbol   = std::string(tokens[1]);
        args.exchange = std::string(tokens[2]);

        // side: bid → 0, ask → 1
        if (iequals(tokens[3], "bid")) {
            args.side = 0;
        } else if (iequals(tokens[3], "ask")) {
            args.side = 1;
        } else {
            return cmd; // UNKNOWN
        }

        // price (int64)
        {
            auto sv = tokens[4];
            auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), args.price);
            if (ec != std::errc{} || ptr != sv.data() + sv.size()) return cmd;
        }

        // qty (uint64)
        {
            auto sv = tokens[5];
            auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), args.qty);
            if (ec != std::errc{} || ptr != sv.data() + sv.size()) return cmd;
        }

        // optional count (uint32, default 1)
        args.count = 1;
        if (tokens.size() >= 7) {
            auto sv = tokens[6];
            auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), args.count);
            if (ec != std::errc{} || ptr != sv.data() + sv.size()) return cmd;
        }

        cmd.type = CommandType::INSERT;
        cmd.insert_args = std::move(args);
        return cmd;
    }

    if (iequals(first, "FLUSH"))  { cmd.type = CommandType::FLUSH;  return cmd; }
    if (iequals(first, "PING"))   { cmd.type = CommandType::PING;   return cmd; }
    if (iequals(first, "STATUS")) { cmd.type = CommandType::STATUS; return cmd; }
    if (iequals(first, "ROLE"))   { cmd.type = CommandType::ROLE;   return cmd; }
    if (iequals(first, "QUIT"))   { cmd.type = CommandType::QUIT;   return cmd; }

    if (iequals(first, "FAILOVER")) {
        if (tokens.size() < 2) return cmd; // need target_node_id
        cmd.type = CommandType::FAILOVER;
        cmd.target_node_id = std::string(tokens[1]);
        return cmd;
    }

    return cmd; // UNKNOWN
}

// ── format_command ────────────────────────────────────────────────────────────

std::string format_command(const Command& cmd) {
    switch (cmd.type) {
    case CommandType::SELECT:
        return cmd.raw_sql + "\n";

    case CommandType::INSERT: {
        const auto& a = cmd.insert_args;
        std::string out = "INSERT ";
        out += a.symbol;
        out += ' ';
        out += a.exchange;
        out += ' ';
        out += (a.side == 0) ? "bid" : "ask";
        out += ' ';
        out += std::to_string(a.price);
        out += ' ';
        out += std::to_string(a.qty);
        out += ' ';
        out += std::to_string(a.count);
        out += '\n';
        return out;
    }

    case CommandType::FLUSH:  return "FLUSH\n";
    case CommandType::PING:   return "PING\n";
    case CommandType::STATUS: return "STATUS\n";
    case CommandType::ROLE:   return "ROLE\n";
    case CommandType::FAILOVER:
        return "FAILOVER " + cmd.target_node_id + "\n";
    case CommandType::QUIT:   return "QUIT\n";
    case CommandType::UNKNOWN: return "";
    }
    return "";
}

} // namespace ob
