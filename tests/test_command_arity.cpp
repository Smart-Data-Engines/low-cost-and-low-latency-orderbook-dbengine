// Tests for #107: a command line carrying a token the grammar has no place for is refused, and the
// refusal names the token.
//
// Measured against master before the change, over a live server: **fourteen command shapes accepted
// a token nobody reads**, and five of them stored a row for it —
// `INSERT AAA EX bid 100 5 1 notanumber` answered `OK` and wrote one row. That is pitfall 27 on the
// wire: the same class #36 closed for command-line flags, where `--prot 5599` was silently skipped.
//
// Every refusal here has a control beside it, because a parser that refuses everything passes every
// refusal test. The two directions are what make this file mean anything.

#include "orderbook/command_parser.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

using namespace ob;

namespace {

/// A line with exactly one token more than the command's grammar allows.
std::string one_token_too_many(const CommandGrammar& g) {
    std::string line(g.keyword);
    for (size_t i = 1; i <= *g.max_tokens; ++i) line += " x";
    return line;   // keyword + max_tokens tokens = max_tokens + 1 tokens
}

bool mentions(const std::string& haystack, std::string_view needle) {
    return haystack.find(needle) != std::string::npos;
}

/// Route a line the way both transports route it, rather than picking the parser the test means.
///
/// `tcp_server.cpp` and `io_uring_server.cpp` both decide by `line.find('\n')`, so a fixture that
/// called `parse_command` directly would be testing a dispatch nobody performs — and `MINSERT`,
/// whose canonical form is the only multi-line one, would have no canonical case at all. Which is
/// exactly what the coverage test below reported the first time it ran.
Command parse_like_a_transport(std::string_view line) {
    return line.find('\n') != std::string_view::npos ? parse_minsert(line) : parse_command(line);
}

/// The canonical form of every command, as a client actually sends it.
///
/// This is a list of **inputs**, which is a different thing from a list of facts about the code —
/// but a new command added to the table would still have no canonical line here, so the coverage
/// test below derives the requirement from `command_grammar()` and fails until one is added.
struct Canonical {
    std::string_view line;
    CommandType      expected;
};

constexpr Canonical kCanonical[] = {
    {"SELECT * FROM 'AAA'.'EX'",                CommandType::SELECT},
    {"INSERT AAA EX bid 100 5",                 CommandType::INSERT},
    {"INSERT AAA EX bid 100 5 3",               CommandType::INSERT},   // the optional count
    {"MINSERT AAA EX bid 2\n100\t5\t1\n101\t6\t1\n",   CommandType::MINSERT},
    {"FLUSH",                                   CommandType::FLUSH},
    {"PING",                                    CommandType::PING},
    {"STATUS",                                  CommandType::STATUS},
    {"ROLE",                                    CommandType::ROLE},
    {"FAILOVER node-2",                         CommandType::FAILOVER},
    {"QUIT",                                    CommandType::QUIT},
    {"COMPRESS LZ4",                            CommandType::COMPRESS},
    {"SHARD_MAP",                               CommandType::SHARD_MAP},
    {"SHARD_INFO",                              CommandType::SHARD_INFO},
    {"MIGRATE AAA.EX shard-1",                  CommandType::MIGRATE},
    {"MM_PEERS",                                CommandType::MM_PEERS},
    {"MM_CONFLICTS",                            CommandType::MM_CONFLICTS},
    {"MM_CONFLICTS 5",                          CommandType::MM_CONFLICTS},
    {"SUBSCRIBE * FROM 'AAA'.'EX'",             CommandType::SUBSCRIBE},
    {"UNSUBSCRIBE",                             CommandType::UNSUBSCRIBE},
    {"UNSUBSCRIBE 7",                           CommandType::UNSUBSCRIBE},
    {"AUTH",                                    CommandType::AUTH},
    {std::string_view(
         "AUTH alice "
         "0000000000000000000000000000000000000000000000000000000000000000"),
                                                CommandType::AUTH},
};

} // namespace

// ── The rule, across the whole table ──────────────────────────────────────────

TEST(CommandArity, EveryBoundedCommandRefusesOneTokenTooMany) {
    size_t checked = 0;
    for (const auto& g : command_grammar()) {
        if (!g.max_tokens) continue;
        const std::string line = one_token_too_many(g);
        const Command cmd = parse_command(line);

        EXPECT_EQ(cmd.type, CommandType::UNKNOWN)
            << g.keyword << " accepted a line with " << (*g.max_tokens + 1) << " tokens: " << line;
        EXPECT_TRUE(mentions(cmd.error, "unexpected token"))
            << g.keyword << " was refused without saying why, so the sender reads `unknown "
            << "command` about a command that is known: " << cmd.error;
        EXPECT_TRUE(mentions(cmd.error, g.keyword))
            << "the refusal does not name the command it is about: " << cmd.error;
        EXPECT_TRUE(mentions(cmd.error, g.usage))
            << "the refusal does not say what " << g.keyword << " accepts: " << cmd.error;
        ++checked;
    }
    // The guard, mutated together with the failure it stands for: with a broken `command_grammar()`
    // this loop would check nothing and report success, which is how a green test learns to mean
    // nothing at all.
    EXPECT_GE(checked, 16u) << "only " << checked << " bounded commands were checked - the table is "
                            << "not being read";
}

TEST(CommandArity, EveryCommandStillParsesInItsCanonicalForm) {
    for (const auto& c : kCanonical) {
        const Command cmd = parse_like_a_transport(c.line);
        EXPECT_EQ(cmd.type, c.expected) << "refused a canonical line: " << c.line
                                        << " (error: " << cmd.error << ")";
        EXPECT_TRUE(cmd.error.empty()) << c.line << " carried a refusal message: " << cmd.error;
    }
}

TEST(CommandArity, EveryRowOfTheTableHasACanonicalLine) {
    // Which is what stops the test above from silently covering less as commands are added: the
    // arity of command nineteen would be declared, refused correctly, and never once accepted.
    for (const auto& g : command_grammar()) {
        const bool covered = std::any_of(std::begin(kCanonical), std::end(kCanonical),
                                         [&](const Canonical& c) {
                                             return c.line.substr(0, g.keyword.size()) == g.keyword;
                                         });
        EXPECT_TRUE(covered) << g.keyword << " has no canonical line in kCanonical, so nothing "
                             << "here ever asserts that it is accepted";
    }
}

// ── The exact lines the item was filed with ───────────────────────────────────

TEST(CommandArity, TheLinesFromTheItemAreRefusedRatherThanStored) {
    // All three answered `OK` and stored a row. The first is the shape that matters most: it is
    // what an upgraded client would send to a server that does not know #105's field yet, and `OK`
    // with the time discarded is the defect #105 is about, one layer out.
    const Command with_time = parse_command("INSERT AAA EX bid 100 5 1 1700000000000000000");
    EXPECT_EQ(with_time.type, CommandType::UNKNOWN);
    EXPECT_TRUE(mentions(with_time.error, "1700000000000000000")) << with_time.error;

    const Command with_garbage = parse_command("INSERT AAA EX bid 100 5 1 notanumber");
    EXPECT_EQ(with_garbage.type, CommandType::UNKNOWN);
    EXPECT_TRUE(mentions(with_garbage.error, "'notanumber'")) << with_garbage.error;

    const Command minsert = parse_minsert("MINSERT AAA EX bid 1 1700000000000000000\n100\t5\t1\n");
    EXPECT_EQ(minsert.type, CommandType::UNKNOWN);
    EXPECT_TRUE(mentions(minsert.error, "1700000000000000000")) << minsert.error;
}

// ── Free-form tails, and why they are exempt from counting only ───────────────

TEST(CommandArity, AFreeFormTailIsNotCountedHere) {
    // Both hand the whole line to the query parser, which refuses a trailing token by name
    // (measured: `SELECT * FROM 'AAA'.'EX' garbage` ->
    // `ERR Parse error at line 1, col 26: unexpected token 'garbage'`). Counting tokens here would
    // refuse every legitimate `WHERE` clause, so the exemption is from arity, not from refusal.
    const Command select = parse_command(
        "SELECT * FROM 'AAA'.'EX' WHERE timestamp BETWEEN 1 AND 2 LIMIT 10");
    EXPECT_EQ(select.type, CommandType::SELECT);
    EXPECT_TRUE(select.error.empty());

    const Command sub = parse_command("SUBSCRIBE * FROM 'AAA'.'EX' WHERE price > 100");
    EXPECT_EQ(sub.type, CommandType::SUBSCRIBE);
    EXPECT_TRUE(sub.error.empty());
}

// ── What the refusal may say ──────────────────────────────────────────────────

TEST(CommandArity, TheRefusalDoesNotQuoteATokenThatCouldBeACredential) {
    // An `AUTH` line's tokens are an identity and a response to a challenge. A fourth token is
    // refused like any other, and the message says so without repeating it — because the parser
    // logs every refusal, and a response echoed into a log is a response in a log.
    const Command cmd = parse_command(
        "AUTH alice 0000000000000000000000000000000000000000000000000000000000000000 "
        "1111111111111111111111111111111111111111111111111111111111111111");
    EXPECT_EQ(cmd.type, CommandType::UNKNOWN);
    EXPECT_TRUE(mentions(cmd.error, "unexpected token")) << cmd.error;
    EXPECT_FALSE(mentions(cmd.error, "1111111111111111111111111111111111111111111111111111111111111111"))
        << "the refusal repeated the fourth token of an AUTH line: " << cmd.error;
    EXPECT_FALSE(mentions(cmd.error, "0000000000000000000000000000000000000000000000000000000000000000"))
        << "the refusal repeated the response: " << cmd.error;
}

TEST(CommandArity, AnUnrecognisedKeywordIsStillJustUnknown) {
    // The fallback has to stay reachable: the parser has nothing specific to say about a word that
    // is not a command, and the server's `unknown command` is the honest answer. An empty `error`
    // is how it asks for that answer.
    const Command cmd = parse_command("FROBNICATE a b c");
    EXPECT_EQ(cmd.type, CommandType::UNKNOWN);
    EXPECT_TRUE(cmd.error.empty()) << "invented a message about a command that does not exist: "
                                   << cmd.error;
}

// ── MINSERT payload lines ─────────────────────────────────────────────────────

TEST(CommandArity, ALevelLineWithAnExtraTokenNamesTheLine) {
    const Command cmd = parse_minsert(
        "MINSERT AAA EX bid 3\n100\t5\t1\n101\t6\t1\tnotanumber\n102\t7\t1\n");
    EXPECT_EQ(cmd.type, CommandType::UNKNOWN);
    EXPECT_TRUE(mentions(cmd.error, "'notanumber'")) << cmd.error;
    EXPECT_TRUE(mentions(cmd.error, "level line 2"))
        << "a batch is up to MAX_LEVELS lines, so \"somewhere in there\" is not an answer: "
        << cmd.error;
}

TEST(CommandArity, ALevelLineMayStillCarryItsOptionalCount) {
    const Command cmd = parse_minsert("MINSERT AAA EX bid 2\n100\t5\t9\n101\t6\n");
    ASSERT_EQ(cmd.type, CommandType::MINSERT);
    ASSERT_EQ(cmd.minsert_args.levels.size(), 2u);
    EXPECT_EQ(cmd.minsert_args.levels[0].count, 9u);
    EXPECT_EQ(cmd.minsert_args.levels[1].count, 1u);   // the default
}

// ── The other shape of the same silence ───────────────────────────────────────

TEST(CommandArity, AMmConflictsLimitThatIsNotANumberIsRefusedRatherThanDefaulted) {
    // It used to keep the default of 100: a token the sender meant to matter, replaced by a number
    // they never asked for. `UNSUBSCRIBE` two rows below already refuses exactly this, having
    // learnt it from #36 — the correct answer lived four lines away.
    const Command bad = parse_command("MM_CONFLICTS notanumber");
    EXPECT_EQ(bad.type, CommandType::UNKNOWN);
    EXPECT_TRUE(mentions(bad.error, "'notanumber'")) << bad.error;

    const Command good = parse_command("MM_CONFLICTS 5");
    ASSERT_EQ(good.type, CommandType::MM_CONFLICTS);
    EXPECT_EQ(good.mm_conflicts_limit, 5u);

    const Command bare = parse_command("MM_CONFLICTS");
    ASSERT_EQ(bare.type, CommandType::MM_CONFLICTS);
    EXPECT_EQ(bare.mm_conflicts_limit, 100u) << "the default is still the default when nobody asked";
}
