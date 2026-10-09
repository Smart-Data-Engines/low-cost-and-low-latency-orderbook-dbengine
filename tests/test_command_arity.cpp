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
#include "orderbook/data_model.hpp"
#include "orderbook/session.hpp"

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

/// Route a line the way the server routes it, rather than picking the parser the test means.
///
/// `tcp_server.cpp` decides by `line.find('\n')`, so a fixture that
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
    {"ADOPT AAA.EX BEGIN shard-0",              CommandType::ADOPT},
    {"ADOPT AAA.EX END",                        CommandType::ADOPT},
    {"ADOPT AAA.EX abandon",                    CommandType::ADOPT},
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
    {"BOOK AAA EX", CommandType::BOOK},
    {"BACKUP",                                  CommandType::BACKUP},
    {"BACKUP STATUS",                           CommandType::BACKUP},
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

// ── BOOK's own refusals (#145) ────────────────────────────────────────────────
//
// The arity table above covers "one token too many". The depth argument has three more refusals
// that only it can have, and they belong in this file rather than only in the integration battery:
// a mutation dropping the ceiling **survived** the C++ suite, because the wire refusals for this
// command were tested exclusively over a socket.

TEST(BookRefusals, TheDepthCeilingIsTheMostLevelsTheEngineStores) {
    const Command over = parse_command("BOOK AAA EX 5000");
    EXPECT_EQ(over.type, CommandType::UNKNOWN);
    EXPECT_TRUE(mentions(over.error, "5000"))
        << "the refusal does not name the depth it refused: " << over.error;
    EXPECT_TRUE(mentions(over.error, std::to_string(MAX_LEVELS)))
        << "the refusal does not say what the maximum is: " << over.error;

    // The boundary itself is accepted, because a refusal that is off by one is a different refusal
    // from the one `docs/cli.md` prints (#124's lesson about a flag's floor).
    const Command at = parse_command("BOOK AAA EX " + std::to_string(MAX_LEVELS));
    EXPECT_EQ(at.type, CommandType::BOOK) << at.error;
    EXPECT_EQ(at.book_args.depth, MAX_LEVELS);
}

TEST(BookRefusals, ZeroIsRefusedRatherThanReadAsNoDepthGiven) {
    // An empty answer on request is indistinguishable from a book that is not there, and those are
    // different answers. Omitting the argument is how you ask for everything - which is the
    // control, and it is what says this refusal is about zero rather than about the argument.
    const Command zero = parse_command("BOOK AAA EX 0");
    EXPECT_EQ(zero.type, CommandType::UNKNOWN);
    EXPECT_TRUE(mentions(zero.error, "at least 1")) << zero.error;

    const Command omitted = parse_command("BOOK AAA EX");
    EXPECT_EQ(omitted.type, CommandType::BOOK) << omitted.error;
    EXPECT_EQ(omitted.book_args.depth, 0u)
        << "no depth given must reach the engine as 'everything the side has'";
}

TEST(BookRefusals, ADepthThatIsNotANumberIsNamedRatherThanDefaulted) {
    // `MM_CONFLICTS notanumber` used to answer with the default limit of 100, which is an answer to
    // a question nobody asked (#107). Same shape, same refusal.
    const Command bad = parse_command("BOOK AAA EX notanumber");
    EXPECT_EQ(bad.type, CommandType::UNKNOWN);
    EXPECT_TRUE(mentions(bad.error, "notanumber")) << bad.error;
}

TEST(BookRefusals, ASymbolWithoutAnExchangeIsRefusedWithTheUsage) {
    const Command short_line = parse_command("BOOK AAA");
    EXPECT_EQ(short_line.type, CommandType::UNKNOWN);
    EXPECT_TRUE(mentions(short_line.error, "BOOK <symbol> <exchange> [depth]"))
        << "the refusal does not say what BOOK accepts: " << short_line.error;
}

TEST(BookRefusals, ItRoundTripsThroughFormatCommand) {
    // The fuzzer's oracle compares these structures field by field, so the formatter has to carry
    // the depth when one was given and **not** invent one when it was not - the same rule the event
    // time taught (#105): a formatter that filled the field in would turn "everything the side has"
    // into a fixed number the first time anything replayed a command.
    for (const char* line : {"BOOK AAA EX", "BOOK AAA EX 7"}) {
        const Command first = parse_command(line);
        ASSERT_EQ(first.type, CommandType::BOOK) << first.error;
        const Command again = parse_command(format_command(first));
        EXPECT_EQ(again.type, CommandType::BOOK) << again.error;
        EXPECT_EQ(again.book_args.symbol, first.book_args.symbol);
        EXPECT_EQ(again.book_args.exchange, first.book_args.exchange);
        EXPECT_EQ(again.book_args.depth, first.book_args.depth);
    }
}

// ── The exact lines the item was filed with ───────────────────────────────────

TEST(CommandArity, TheLinesFromTheItemAreRefusedRatherThanStored) {
    // All three answered `OK` and stored a row when #107 was filed. **Two of the three have since
    // become legal**, and that is the arc rather than a regression: the trailing timestamp the item
    // measured being discarded is exactly the field #105 then added, and it could only be added
    // once an unknown token was refused — a client cannot tell a server that stores its event time
    // from one that drops it unless the second one says so.
    //
    // So what is asserted here is the line that is still outside the grammar, and the two that are
    // inside it are asserted by `ExecuteCommandTest.AnInsertKeepsTheEventTimeItWasGiven`.
    const Command with_garbage = parse_command("INSERT AAA EX bid 100 5 1 notanumber");
    EXPECT_EQ(with_garbage.type, CommandType::UNKNOWN);
    EXPECT_TRUE(mentions(with_garbage.error, "'notanumber'")) << with_garbage.error;

    const Command one_past_the_field = parse_command("INSERT AAA EX bid 100 5 1 "
                                                     "1700000000000000000 extra");
    EXPECT_EQ(one_past_the_field.type, CommandType::UNKNOWN);
    EXPECT_TRUE(mentions(one_past_the_field.error, "'extra'")) << one_past_the_field.error;

    const Command minsert = parse_minsert("MINSERT AAA EX bid 1 1700000000000000000 extra"
                                          "\n100\t5\t1\n");
    EXPECT_EQ(minsert.type, CommandType::UNKNOWN);
    EXPECT_TRUE(mentions(minsert.error, "'extra'")) << minsert.error;

    // And the field itself is accepted, which is what makes the three refusals above a grammar
    // rather than a wall.
    const Command with_time = parse_command("INSERT AAA EX bid 100 5 1 1700000000000000000");
    EXPECT_EQ(with_time.type, CommandType::INSERT) << with_time.error;
    EXPECT_TRUE(with_time.insert_args.timestamp_ns.has_value());
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

// ── How often a refusal is worth a log line ───────────────────────────────────

TEST(CommandArity, ASessionAnnouncesOnlyItsFirstRefusal) {
    // A refusal is reachable **before authentication** - the gate lets an unparseable line through
    // precisely because it is refused anyway - so a WARN per refused line is a log flood any peer
    // who can reach the port can drive at line rate. That is #95's shape (a permanent failure
    // retried at loop frequency and logged with it), and the answer is the same: say it once, count
    // the rest. `ob_refused_commands_total` is the half an operator alerts on.
    ob::Session session(/*fd=*/7);
    EXPECT_TRUE(session.first_refusal()) << "the first refusal on a connection is news";
    EXPECT_FALSE(session.first_refusal());
    EXPECT_FALSE(session.first_refusal()) << "the second one is a pattern, and the counter has it";

    // And it is per connection, not per process: the next client starts with news of its own.
    ob::Session other(/*fd=*/8);
    EXPECT_TRUE(other.first_refusal());
}

// ── A write whose field cannot be read (#222) ─────────────────────────────────
//
// Found installing #212's packages: the acceptance's own client sent `INSERT FRESH TEST bid 100.5 3`
// - a decimal price, where prices are integers in the instrument's smallest sub-unit - and the
// server answered `unknown command`, about a command it has. Every field of a write the parser could
// not read was answered that way: the side, the price, the quantity, the count, a field missing, and
// on a MINSERT the header's side and every field of a level line. The refusal names the field and
// the token now, and says what the command takes - the three things #107's arity refusals say. Each
// refusal has its control beside it: a parser that refused every write would pass the first half.

namespace {

std::string usage_of(std::string_view keyword) {
    for (const auto& g : command_grammar()) {
        if (g.keyword == keyword) return std::string(g.usage);
    }
    ADD_FAILURE() << "no grammar row for " << keyword;
    return "<no usage>";
}

constexpr std::string_view kLevelLineTakes = "a level line takes: <price> <qty> [count]";

/// What a refusal of a write's field carries: the field, the token, quoted, and what is accepted.
void expect_field_refused(const Command& cmd, std::string_view field, std::string_view token,
                          std::string_view takes) {
    EXPECT_EQ(cmd.type, CommandType::UNKNOWN) << "took a write it could not read";
    EXPECT_FALSE(cmd.error.empty())
        << "refused without a word, so the server answers `unknown command` about a known command";
    EXPECT_TRUE(mentions(cmd.error, field)) << "does not name the field '" << field << "': "
                                            << cmd.error;
    EXPECT_TRUE(mentions(cmd.error, "'" + std::string(token) + "'"))
        << "does not quote the token '" << token << "': " << cmd.error;
    EXPECT_TRUE(mentions(cmd.error, takes)) << "does not say what is accepted: " << cmd.error;
}

} // namespace

TEST(WriteFieldRefusals, ADecimalPriceIsNamedRatherThanAnsweredAsAnUnknownCommand) {
    // The line #222 was found with.
    const Command cmd = parse_command("INSERT FRESH TEST bid 100.5 3");
    expect_field_refused(cmd, "INSERT price", "100.5", usage_of("INSERT"));
    EXPECT_TRUE(mentions(cmd.error, "smallest sub-unit"))
        << "a decimal price is the likely mistake, and the unit is what answers it: " << cmd.error;
    // What the token had to be, from the field's type - which covers a number past its range too.
    EXPECT_TRUE(mentions(cmd.error, "is not a 64-bit integer")) << cmd.error;

    const Command control = parse_command("INSERT FRESH TEST bid 10050 3");
    ASSERT_EQ(control.type, CommandType::INSERT) << control.error;
    EXPECT_EQ(control.insert_args.price, 10050);
    EXPECT_EQ(control.insert_args.qty, 3u);

    // A price may be negative - a spread, a rate - and that is not this refusal's business.
    const Command negative = parse_command("INSERT FRESH TEST ask -5 3");
    ASSERT_EQ(negative.type, CommandType::INSERT) << negative.error;
    EXPECT_EQ(negative.insert_args.price, -5);
}

TEST(WriteFieldRefusals, ASideThatIsNeitherBidNorAskIsNamed) {
    expect_field_refused(parse_command("INSERT AAA EX buy 100 3"), "INSERT side", "buy",
                         usage_of("INSERT"));

    const Command control = parse_command("INSERT AAA EX ASK 100 3");
    ASSERT_EQ(control.type, CommandType::INSERT) << control.error;
    EXPECT_EQ(control.insert_args.side, 1);
}

TEST(WriteFieldRefusals, AQuantityThatIsNotAWholeNumberIsNamed) {
    expect_field_refused(parse_command("INSERT AAA EX bid 100 -3"), "INSERT quantity", "-3",
                         usage_of("INSERT"));
    const Command fraction = parse_command("INSERT AAA EX bid 100 3.5");
    expect_field_refused(fraction, "INSERT quantity", "3.5", usage_of("INSERT"));
    EXPECT_TRUE(mentions(fraction.error, "is not a non-negative 64-bit integer")) << fraction.error;
    EXPECT_FALSE(mentions(fraction.error, "sub-unit")) << "a price's hint on a quantity: "
                                                       << fraction.error;

    const Command control = parse_command("INSERT AAA EX bid 100 0");
    ASSERT_EQ(control.type, CommandType::INSERT)
        << "zero is how an L2 feed removes a level, and a write like any other: " << control.error;
    EXPECT_EQ(control.insert_args.qty, 0u);
}

TEST(WriteFieldRefusals, ACountThatIsNotAWholeNumberIsNamed) {
    const Command many = parse_command("INSERT AAA EX bid 100 3 many");
    expect_field_refused(many, "INSERT count", "many", usage_of("INSERT"));
    EXPECT_TRUE(mentions(many.error, "is not a non-negative 32-bit integer")) << many.error;

    const Command control = parse_command("INSERT AAA EX bid 100 3 2");
    ASSERT_EQ(control.type, CommandType::INSERT) << control.error;
    EXPECT_EQ(control.insert_args.count, 2u);
}

TEST(WriteFieldRefusals, AnInsertMissingAFieldSaysWhatItNeeds) {
    for (const char* line : {"INSERT AAA EX bid 100", "INSERT AAA", "INSERT"}) {
        const Command cmd = parse_command(line);
        EXPECT_EQ(cmd.type, CommandType::UNKNOWN) << line;
        EXPECT_TRUE(mentions(cmd.error, "INSERT needs")) << line << ": " << cmd.error;
        EXPECT_TRUE(mentions(cmd.error, usage_of("INSERT"))) << line << ": " << cmd.error;
    }
}

TEST(WriteFieldRefusals, AMinsertHeaderNamesItsSideAndWhatItLacks) {
    expect_field_refused(parse_minsert("MINSERT AAA EX buy 1\n100\t5\n"), "MINSERT side", "buy",
                         usage_of("MINSERT"));

    const Command short_header = parse_minsert("MINSERT AAA EX bid\n100\t5\n");
    EXPECT_EQ(short_header.type, CommandType::UNKNOWN);
    EXPECT_TRUE(mentions(short_header.error, "MINSERT needs")) << short_header.error;
    EXPECT_TRUE(mentions(short_header.error, usage_of("MINSERT"))) << short_header.error;

    const Command control = parse_minsert("MINSERT AAA EX bid 1\n100\t5\n");
    ASSERT_EQ(control.type, CommandType::MINSERT) << control.error;
    EXPECT_EQ(control.minsert_args.side, 0);
}

TEST(WriteFieldRefusals, ALevelLineNamesItsFieldAndItsLine) {
    const Command price = parse_minsert("MINSERT AAA EX bid 2\n100\t5\n100.5\t5\n");
    expect_field_refused(price, "price", "100.5", kLevelLineTakes);
    EXPECT_TRUE(mentions(price.error, "level line 2")) << price.error;
    EXPECT_TRUE(mentions(price.error, "smallest sub-unit")) << price.error;

    const Command qty = parse_minsert("MINSERT AAA EX bid 2\n100\t-5\n101\t5\n");
    expect_field_refused(qty, "quantity", "-5", kLevelLineTakes);
    EXPECT_TRUE(mentions(qty.error, "level line 1")) << qty.error;

    const Command count = parse_minsert("MINSERT AAA EX bid 1\n100\t5\tmany\n");
    expect_field_refused(count, "count", "many", kLevelLineTakes);
    EXPECT_TRUE(mentions(count.error, "level line 1")) << count.error;

    const Command lone = parse_minsert("MINSERT AAA EX bid 2\n100\t5\n101\n");
    EXPECT_EQ(lone.type, CommandType::UNKNOWN);
    EXPECT_TRUE(mentions(lone.error, "level line 2 needs a price and a quantity")) << lone.error;
    EXPECT_TRUE(mentions(lone.error, kLevelLineTakes)) << lone.error;

    const Command control = parse_minsert("MINSERT AAA EX bid 2\n100\t5\n-101\t6\t2\n");
    ASSERT_EQ(control.type, CommandType::MINSERT) << control.error;
    ASSERT_EQ(control.minsert_args.levels.size(), 2u);
    EXPECT_EQ(control.minsert_args.levels[1].price, -101);
    EXPECT_EQ(control.minsert_args.levels[1].count, 2u);
}

TEST(WriteFieldRefusals, ABlockThatIsNotAMinsertIsStillJustUnknown) {
    // The header's keyword is checked before its length, so a multi-line block of something else
    // is not told what MINSERT needs.
    const Command cmd = parse_minsert("PING\nPING\n");
    EXPECT_EQ(cmd.type, CommandType::UNKNOWN);
    EXPECT_TRUE(cmd.error.empty()) << "told a block that is not a MINSERT what MINSERT needs: "
                                   << cmd.error;
}

// ── Every known command says what it could not read (#222) ────────────────────
//
// The same silence beyond the writes: FAILOVER without a node, COMPRESS without LZ4, MIGRATE and
// ADOPT without their arguments or with an action ADOPT does not have, BACKUP with a word that is not
// STATUS and UNSUBSCRIBE with an id that is not a number were all `unknown command`. The rule is held
// across the grammar table, so a command added later is held to it too.

TEST(KnownCommandRefusals, NoKnownCommandIsAnsweredAsAnUnknownOne) {
    size_t refusals = 0;
    for (const auto& g : command_grammar()) {
        // AUTH by its own design: a line that is not its shape is not the protocol, and the parser
        // says nothing to whoever sent it (pinned below). MINSERT reaches the parser as a block,
        // header and level lines, and WriteFieldRefusals holds it to the rule as one.
        if (g.keyword == "AUTH" || g.keyword == "MINSERT") continue;
        const std::string k(g.keyword);
        for (const std::string& line : {k, k + " ?", k + " ? ?", k + " ? ? ?"}) {
            const Command cmd = parse_command(line);
            if (cmd.type != CommandType::UNKNOWN) continue;
            ++refusals;
            EXPECT_FALSE(cmd.error.empty()) << "`" << line << "` would be answered `unknown command`";
            EXPECT_TRUE(mentions(cmd.error, g.keyword)) << line << ": " << cmd.error;
            EXPECT_TRUE(mentions(cmd.error, g.usage)) << line << ": " << cmd.error;
        }
    }
    // The guard: a table the loop does not read refuses nothing and passes.
    EXPECT_GE(refusals, 25u) << "only " << refusals << " lines were refused - the table is not being read";
}

TEST(KnownCommandRefusals, EachSaysWhatItCouldNotRead) {
    struct Case {
        const char* line;
        const char* says;
    };
    const Case cases[] = {
        {"FAILOVER",                  "FAILOVER needs the node to hand the role to"},
        {"COMPRESS",                  "COMPRESS needs a codec"},
        {"COMPRESS zstd",             "not 'zstd'"},
        {"MIGRATE AAA.EX",            "MIGRATE needs a symbol and the shard to move it to"},
        {"ADOPT AAA.EX",              "ADOPT needs a symbol and BEGIN, END or ABANDON"},
        {"ADOPT AAA.EX START shard-0", "ADOPT action is not BEGIN, END or ABANDON: 'START'"},
        {"ADOPT AAA.EX BEGIN",        "ADOPT BEGIN needs the shard the symbol comes from"},
        {"BACKUP NOW",                "not 'NOW'"},
        {"UNSUBSCRIBE seven",         "UNSUBSCRIBE id is not a number: 'seven'"},
    };
    for (const Case& c : cases) {
        const Command cmd = parse_command(c.line);
        EXPECT_EQ(cmd.type, CommandType::UNKNOWN) << c.line;
        EXPECT_TRUE(mentions(cmd.error, c.says)) << c.line << ": " << cmd.error;
    }
    // The controls are the canonical lines at the top of this file, every one of them parsed by
    // EveryCommandStillParsesInItsCanonicalForm; and these, beside the refusals they neighbour.
    EXPECT_EQ(parse_command("COMPRESS lz4").type, CommandType::COMPRESS);
    EXPECT_EQ(parse_command("ADOPT AAA.EX begin shard-0").type, CommandType::ADOPT);
    EXPECT_EQ(parse_command("BACKUP status").type, CommandType::BACKUP);
    EXPECT_EQ(parse_command("UNSUBSCRIBE 0").type, CommandType::UNSUBSCRIBE);
}

TEST(KnownCommandRefusals, AnAuthLineOfTheWrongShapeStillSaysNothing) {
    // Its two-token and bad-response shapes stay `unknown command` with nothing of the parser's -
    // the exception this item keeps, written down so that a change to it is a decision.
    for (const char* line : {"AUTH alice", "AUTH alice not-hex"}) {
        const Command cmd = parse_command(line);
        EXPECT_EQ(cmd.type, CommandType::UNKNOWN) << line;
        EXPECT_TRUE(cmd.error.empty()) << line << " said: " << cmd.error;
    }
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
