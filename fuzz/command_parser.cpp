#include "support.hpp"
#include "orderbook/command_parser.hpp"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>

namespace {

/// Do two parsed commands mean the same thing?
///
/// The text comparison below cannot answer this, and a mutation proved it: deleting the line in
/// `format_command` that emits an INSERT's event time survives a canonical-text round trip,
/// because the dropped field is absent from both sides of the comparison. A write that asked to
/// carry the time it happened and a write that asked for arrival time are then the same write,
/// which is the exact defect roadmap item #105 existed to fix.
///
/// There is no `default:` here on purpose. A nineteenth command would otherwise compare equal
/// through a branch nobody wrote, so `-Wswitch` is left to make it a build error instead - the
/// same mechanism that guards the authentication gate and the arity table (#107).
bool same_meaning(const ob::Command& a, const ob::Command& b) {
    if (a.type != b.type) return false;
    switch (a.type) {
    case ob::CommandType::SELECT:
        return a.raw_sql == b.raw_sql;
    case ob::CommandType::INSERT: {
        const auto& x = a.insert_args;
        const auto& y = b.insert_args;
        return x.symbol == y.symbol && x.exchange == y.exchange && x.side == y.side
            && x.price == y.price && x.qty == y.qty && x.count == y.count
            && x.timestamp_ns == y.timestamp_ns;
    }
    case ob::CommandType::MINSERT: {
        const auto& x = a.minsert_args;
        const auto& y = b.minsert_args;
        if (!(x.symbol == y.symbol && x.exchange == y.exchange && x.side == y.side
              && x.n_levels == y.n_levels && x.timestamp_ns == y.timestamp_ns
              && x.levels.size() == y.levels.size())) {
            return false;
        }
        for (size_t i = 0; i < x.levels.size(); ++i) {
            if (x.levels[i].price != y.levels[i].price || x.levels[i].qty != y.levels[i].qty
                || x.levels[i].count != y.levels[i].count) {
                return false;
            }
        }
        return true;
    }
    case ob::CommandType::FAILOVER:
        return a.target_node_id == b.target_node_id;
    case ob::CommandType::MIGRATE:
        return a.migrate_symbol == b.migrate_symbol
            && a.migrate_target_shard == b.migrate_target_shard;
    case ob::CommandType::MM_CONFLICTS:
        return a.mm_conflicts_limit == b.mm_conflicts_limit;
    case ob::CommandType::SUBSCRIBE:
        return a.subscribe_sql == b.subscribe_sql;
    case ob::CommandType::UNSUBSCRIBE:
        return a.unsubscribe_id == b.unsubscribe_id;
    case ob::CommandType::AUTH:
        return a.auth_identity == b.auth_identity && a.auth_response == b.auth_response;
    // Commands whose whole content is their name.
    case ob::CommandType::FLUSH:
    case ob::CommandType::PING:
    case ob::CommandType::STATUS:
    case ob::CommandType::ROLE:
    case ob::CommandType::QUIT:
    case ob::CommandType::COMPRESS:
    case ob::CommandType::SHARD_MAP:
    case ob::CommandType::SHARD_INFO:
    case ob::CommandType::MM_PEERS:
        return true;
    case ob::CommandType::UNKNOWN:
        return true;
    }
    return false;
}

void check_round_trip(const ob::Command& command, bool complete_batch) {
    OB_LOG_DEBUG("fuzz", "Command round trip: type=%d batch=%d",
                 static_cast<int>(command.type), complete_batch);
    if (command.type == ob::CommandType::UNKNOWN) return;
    // parse_command accepts a MINSERT header before the session has read its payload.
    if (command.type == ob::CommandType::MINSERT && !complete_batch) return;
    const std::string canonical = ob::format_command(command);
    const ob::Command reparsed = command.type == ob::CommandType::MINSERT
        ? ob::parse_minsert(canonical) : ob::parse_command(canonical);
    ob::fuzz::require(reparsed.type == command.type, "formatting changed the command type");
    ob::fuzz::require(same_meaning(command, reparsed), "formatting changed what the command means");
    // Kept alongside the structural check rather than replaced by it: this one also fails for a
    // formatter that is unstable in its spacing while every field still agrees.
    ob::fuzz::require(ob::format_command(reparsed) == canonical,
                      "canonical command changed after reparsing");
}

} // namespace

extern "C" int LLVMFuzzerInitialize(int*, char***) {
    ob::fuzz::initialize("command_parser");
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    OB_LOG_DEBUG("fuzz", "Command input: bytes=%zu", size);
    const std::string_view input(reinterpret_cast<const char*>(data), size);
    check_round_trip(ob::parse_command(input), false);
    check_round_trip(ob::parse_minsert(input), true);
    return 0;
}
