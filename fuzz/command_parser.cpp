#include "support.hpp"
#include "orderbook/command_parser.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace {

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
