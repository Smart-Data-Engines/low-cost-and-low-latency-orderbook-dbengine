#pragma once

#include <string_view>

namespace ob {

/// What this build can do that an older one could not, by name (#105).
///
/// **Why names and not a version number.** A number needs a semver parser in every client — two
/// today, four planned — and the question a client actually asks is "can you take this field", not
/// "what are you called". A name answers that question directly and a client that does not know a
/// name ignores it, which is what lets this list grow without a negotiation.
///
/// **Why the question is needed at all.** Sending the field is not a test: measured, a server
/// without #107 answers `OK` to `INSERT AAA EX bid 100 5 1 notanumber` and stores the row with the
/// token discarded — so a client that inferred support from a successful write would infer it from
/// the very defect it is trying to avoid. And servers without #107 are already deployed.
///
/// **The absence of the line is an answer, not an error** (requirement 3.2): a server that predates
/// this list says nothing, and the client reads that as "none of these".
inline constexpr std::string_view kCapabilities[] = {
    /// `INSERT` and `MINSERT` accept a trailing event time in nanoseconds since the epoch, and
    /// store the row with it instead of with arrival time (#105).
    "insert_event_time",

    /// A command line carrying a token the grammar has no place for is refused rather than read
    /// past. A client can rely on an unknown field being reported instead of dropped (#107).
    "strict_args",
};

} // namespace ob
