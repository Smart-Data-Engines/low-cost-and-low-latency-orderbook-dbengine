#include "orderbook/command_parser.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/logger.hpp"

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

    if (iequals(first, "COMPRESS")) {
        if (tokens.size() >= 2 && iequals(tokens[1], "LZ4")) {
            cmd.type = CommandType::COMPRESS;
            return cmd;
        }
        return cmd; // UNKNOWN
    }

    if (iequals(first, "SHARD_MAP")) {
        cmd.type = CommandType::SHARD_MAP;
        OB_LOG_DEBUG("cmd_parser", "Parsed command: SHARD_MAP");
        return cmd;
    }

    if (iequals(first, "SHARD_INFO")) {
        cmd.type = CommandType::SHARD_INFO;
        OB_LOG_DEBUG("cmd_parser", "Parsed command: SHARD_INFO");
        return cmd;
    }

    if (iequals(first, "MIGRATE")) {
        // MIGRATE <symbol_key> <target_shard_id>
        if (tokens.size() < 3) return cmd; // UNKNOWN — missing arguments
        cmd.type = CommandType::MIGRATE;
        cmd.migrate_symbol = std::string(tokens[1]);
        cmd.migrate_target_shard = std::string(tokens[2]);
        OB_LOG_DEBUG("cmd_parser", "Parsed command: MIGRATE symbol=%s target=%s",
                     cmd.migrate_symbol.c_str(), cmd.migrate_target_shard.c_str());
        return cmd;
    }

    return cmd; // UNKNOWN
}

// ── parse_minsert ─────────────────────────────────────────────────────────────

/// Split a string_view into lines on '\n'.
static std::vector<std::string_view> split_lines(std::string_view sv) {
    std::vector<std::string_view> lines;
    size_t pos = 0;
    while (pos < sv.size()) {
        size_t nl = sv.find('\n', pos);
        if (nl == std::string_view::npos) {
            auto tail = rtrim(sv.substr(pos));
            if (!tail.empty()) lines.push_back(tail);
            break;
        }
        auto segment = rtrim(sv.substr(pos, nl - pos));
        if (!segment.empty()) lines.push_back(segment);
        pos = nl + 1;
    }
    return lines;
}

Command parse_minsert(std::string_view block) {
    Command cmd{};
    cmd.type = CommandType::UNKNOWN;

    auto lines = split_lines(block);
    if (lines.empty()) return cmd;

    // ── Parse header ──────────────────────────────────────────────────────
    auto header_tokens = tokenize(lines[0]);
    if (header_tokens.size() < 5) return cmd;
    if (!iequals(header_tokens[0], "MINSERT")) return cmd;

    MinsertArgs args{};
    args.symbol   = std::string(header_tokens[1]);
    args.exchange = std::string(header_tokens[2]);

    // side
    if (iequals(header_tokens[3], "bid")) {
        args.side = 0;
    } else if (iequals(header_tokens[3], "ask")) {
        args.side = 1;
    } else {
        return cmd;
    }

    // n_levels
    {
        uint16_t nl_val = 0;
        auto sv = header_tokens[4];
        auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), nl_val);
        if (ec != std::errc{} || ptr != sv.data() + sv.size()) return cmd;
        if (nl_val == 0 || nl_val > MAX_LEVELS) return cmd;
        args.n_levels = nl_val;
    }

    // ── Parse payload lines ───────────────────────────────────────────────
    if (lines.size() < static_cast<size_t>(1 + args.n_levels)) return cmd;

    args.levels.reserve(args.n_levels);
    for (uint16_t i = 0; i < args.n_levels; ++i) {
        auto toks = tokenize(lines[1 + i]);
        if (toks.size() < 2) return cmd;

        MinsertArgs::Level lvl{0, 0, 1};

        // price (int64)
        {
            auto sv = toks[0];
            auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), lvl.price);
            if (ec != std::errc{} || ptr != sv.data() + sv.size()) return cmd;
        }
        // qty (uint64)
        {
            auto sv = toks[1];
            auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), lvl.qty);
            if (ec != std::errc{} || ptr != sv.data() + sv.size()) return cmd;
        }
        // optional count (uint32, default 1)
        if (toks.size() >= 3) {
            auto sv = toks[2];
            auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), lvl.count);
            if (ec != std::errc{} || ptr != sv.data() + sv.size()) return cmd;
        }

        args.levels.push_back(lvl);
    }

    cmd.type = CommandType::MINSERT;
    cmd.minsert_args = std::move(args);
    return cmd;
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

    case CommandType::MINSERT: {
        const auto& a = cmd.minsert_args;
        std::string out = "MINSERT ";
        out += a.symbol;
        out += ' ';
        out += a.exchange;
        out += ' ';
        out += (a.side == 0) ? "bid" : "ask";
        out += ' ';
        out += std::to_string(a.n_levels);
        out += '\n';
        for (const auto& lvl : a.levels) {
            out += std::to_string(lvl.price);
            out += ' ';
            out += std::to_string(lvl.qty);
            out += ' ';
            out += std::to_string(lvl.count);
            out += '\n';
        }
        return out;
    }

    case CommandType::FLUSH:  return "FLUSH\n";
    case CommandType::PING:   return "PING\n";
    case CommandType::STATUS: return "STATUS\n";
    case CommandType::ROLE:   return "ROLE\n";
    case CommandType::FAILOVER:
        return "FAILOVER " + cmd.target_node_id + "\n";
    case CommandType::QUIT:   return "QUIT\n";
    case CommandType::COMPRESS: return "COMPRESS LZ4\n";
    case CommandType::SHARD_MAP:  return "SHARD_MAP\n";
    case CommandType::SHARD_INFO: return "SHARD_INFO\n";
    case CommandType::MIGRATE:
        return "MIGRATE " + cmd.migrate_symbol + " " + cmd.migrate_target_shard + "\n";
    case CommandType::UNKNOWN: return "";
    }
    return "";
}

} // namespace ob
