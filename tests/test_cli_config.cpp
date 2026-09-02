// Configuration file support (#32). Spec: kiro-workspace/specs/config-file/.
//
// The file is rewritten into synthetic argv and handed to the existing flag parser, so a config key
// *is* a flag name by construction, type validation and its message stay in one place, and
// precedence falls out of argument order because the parser assigns rather than accumulates.
//
// Two static tests hold the two lists this needs against the parser's own source. They are the
// reason the design is safe: a hand-written list with nothing checking it falls behind at the first
// flag added in a hurry, and the symptom is a config key an operator wrote that does nothing.

#include "orderbook/tcp_server.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <map>
#include <fstream>
#include <regex>
#include <set>
#include <string>
#include <vector>

namespace {

std::filesystem::path write_config(const std::string& body, const std::string& name) {
    const auto path = std::filesystem::temp_directory_path() / ("ob_cfg_" + name + ".conf");
    std::ofstream out(path);
    out << body;
    return path;
}

std::string read_source(const char* relative) {
    const std::filesystem::path path = std::filesystem::path(OB_SOURCE_DIR) / relative;
    std::ifstream in(path);
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

}  // namespace

// ── Precedence ────────────────────────────────────────────────────────────────

TEST(CliConfig, AFileSetsAValue) {
    const auto path = write_config("port = 15001\ndata-dir = /tmp/ob-cfg\n", "basic");
    const auto args = ob::config_file_to_args(path.string());
    ASSERT_EQ(args.size(), 4u);
    EXPECT_EQ(args[0], "--port");
    EXPECT_EQ(args[1], "15001");
    EXPECT_EQ(args[2], "--data-dir");
    EXPECT_EQ(args[3], "/tmp/ob-cfg");
    std::filesystem::remove(path);
}

TEST(CliConfig, CommentsAndBlankLinesAreIgnoredIncludingAfterAValue) {
    const auto path = write_config(
        "# leading comment\n"
        "\n"
        "port = 15002    # why this port\n"
        "   \n"
        "# trailing comment\n",
        "comments");
    const auto args = ob::config_file_to_args(path.string());
    ASSERT_EQ(args.size(), 2u);
    EXPECT_EQ(args[1], "15002") << "the comment was taken as part of the value";
    std::filesystem::remove(path);
}

TEST(CliConfig, ABooleanTrueEmitsTheFlagAndFalseEmitsNothing) {
    const auto path = write_config("read-only = true\nmulti-master = false\n", "bool");
    const auto args = ob::config_file_to_args(path.string());
    ASSERT_EQ(args.size(), 1u);
    EXPECT_EQ(args[0], "--read-only");
    std::filesystem::remove(path);
}

TEST(CliConfig, AValuedFlagPassesItsValueThroughEvenWhenItLooksBoolean) {
    // `failover-enabled` takes a value on the command line, so the file passes it through and the
    // parser validates it — which is the design's whole point: one validation, one message.
    //
    // This test replaced one asserting that a `--no-failover-enabled` negation was emitted. That
    // negation was added on the belief that the command line could not turn failover off, read from
    // the default rather than from the parser's branch, and the static test below deleted the
    // feature by disagreeing with the list I had written myself.
    const auto path = write_config("failover-enabled = false\n", "valued-bool");
    const auto args = ob::config_file_to_args(path.string());
    ASSERT_EQ(args.size(), 2u);
    EXPECT_EQ(args[0], "--failover-enabled");
    EXPECT_EQ(args[1], "false");
    std::filesystem::remove(path);
}

TEST(CliConfig, KeysSeenAreConfigKeysNotEmittedFlags) {
    // Provenance is reported against what the operator wrote. Recording `no-failover-enabled` would
    // make `--print-config` say the key they set came from the default, which is the single thing
    // that flag exists not to do.
    const auto path = write_config("failover-enabled = false\n", "keys");
    std::set<std::string> keys;
    ob::config_file_to_args(path.string(), &keys);
    EXPECT_EQ(keys, std::set<std::string>{"failover-enabled"});
    std::filesystem::remove(path);
}

// ── Refusals ──────────────────────────────────────────────────────────────────
//
// Each is a process exit, because a config file with a mistake in it must not start a server — the
// same rule as #36, where a mistyped flag did.

TEST(CliConfigDeath, AnUnknownKeyIsRefusedAndSuggestsCloseOnes) {
    const auto path = write_config("prot = 15003\n", "unknown");
    EXPECT_EXIT(ob::config_file_to_args(path.string()), ::testing::ExitedWithCode(1), "port");
    std::filesystem::remove(path);
}

TEST(CliConfigDeath, AMissingFileIsRefusedRatherThanIgnored) {
    // Falling back to defaults would mean a node running a configuration nobody knows about.
    EXPECT_EXIT(ob::config_file_to_args("/tmp/ob-cfg-does-not-exist.conf"),
                ::testing::ExitedWithCode(1), "cannot open config file");
}

TEST(CliConfigDeath, ALineWithoutAnEqualsIsRefusedWithItsNumber) {
    const auto path = write_config("port = 15004\nthis is not a setting\n", "noequals");
    EXPECT_EXIT(ob::config_file_to_args(path.string()), ::testing::ExitedWithCode(1), ":2:");
    std::filesystem::remove(path);
}

TEST(CliConfigDeath, ADuplicateKeyIsRefusedRatherThanLastWins) {
    // Last-wins is a silent choice between two things the operator wrote.
    const auto path = write_config("port = 15005\nport = 15006\n", "dup");
    EXPECT_EXIT(ob::config_file_to_args(path.string()), ::testing::ExitedWithCode(1),
                "set more than once");
    std::filesystem::remove(path);
}

TEST(CliConfigDeath, ANonBooleanValueForABooleanKeyIsRefused) {
    const auto path = write_config("read-only = yes\n", "yes");
    EXPECT_EXIT(ob::config_file_to_args(path.string()), ::testing::ExitedWithCode(1),
                "takes true or false");
    std::filesystem::remove(path);
}

TEST(CliConfigDeath, AnEmptyValueIsRefused) {
    const auto path = write_config("data-dir =\n", "emptyvalue");
    EXPECT_EXIT(ob::config_file_to_args(path.string()), ::testing::ExitedWithCode(1), "no value");
    std::filesystem::remove(path);
}

TEST(CliConfigDeath, ConfigInsideAConfigFileIsRefused) {
    // A chain nobody can debug, and self-reference is a loop. Refused outright rather than
    // depth-limited, because a depth limit answers "how deep" rather than "why".
    const auto path = write_config("config = /tmp/other.conf\n", "chain");
    EXPECT_EXIT(ob::config_file_to_args(path.string()), ::testing::ExitedWithCode(1),
                "cannot be set from inside");
    std::filesystem::remove(path);
}

// ── The two lists, against the parser's own source ────────────────────────────

TEST(CliConfigStatic, KnownFlagsMatchTheParser) {
    // A config key is a flag name, so the list of keys must be the list of flags. Derived from the
    // source rather than trusted, because the failure mode of a stale list is a key an operator
    // wrote that silently does nothing.
    const std::string source = read_source("src/tcp_server.cpp");
    ASSERT_FALSE(source.empty()) << "cannot read src/tcp_server.cpp";

    // Only the parser's own branches, so the comment block above it that documents the old defects
    // does not count as flags.
    const size_t parser = source.find("ResolvedConfig resolve_cli_args(");
    ASSERT_NE(parser, std::string::npos);
    const std::string body = source.substr(parser);

    std::set<std::string> in_source;
    const std::regex pattern(R"(arg == \"--([a-z0-9-]+)\")");
    for (auto it = std::sregex_iterator(body.begin(), body.end(), pattern);
         it != std::sregex_iterator(); ++it) {
        in_source.insert((*it)[1].str());
    }

    const std::set<std::string> declared(ob::known_flags().begin(), ob::known_flags().end());
    EXPECT_EQ(in_source, declared)
        << "known_flags() and the parser disagree. A flag the parser accepts and the list omits is "
           "a config key that refuses to load; a key in the list the parser does not accept is a "
           "key that loads and does nothing.";
}

TEST(CliConfigStatic, BooleanFlagsTakeNoValueInTheParser) {
    // A boolean is a flag whose branch never asks for a value. Derived, because getting this wrong
    // in either direction is silent: a boolean treated as valued swallows the next argument, and a
    // valued flag treated as boolean ignores its value.
    const std::string source = read_source("src/tcp_server.cpp");
    const size_t parser = source.find("ResolvedConfig resolve_cli_args(");
    ASSERT_NE(parser, std::string::npos);
    const std::string body = source.substr(parser);

    std::set<std::string> valueless;
    const std::regex pattern(R"(arg == \"--([a-z0-9-]+)\"\)\s*\{([^}]*)\})");
    for (auto it = std::sregex_iterator(body.begin(), body.end(), pattern);
         it != std::sregex_iterator(); ++it) {
        const std::string flag = (*it)[1].str();
        const std::string branch = (*it)[2].str();
        if (branch.find("cursor.value") == std::string::npos) valueless.insert(flag);
    }

    std::set<std::string> declared(ob::boolean_flags().begin(), ob::boolean_flags().end());
    // `print-config` takes no value and is a boolean for the file's purposes; `config` does take one.
    EXPECT_EQ(valueless, declared)
        << "boolean_flags() and the parser disagree about which flags take a value.";
}

TEST(CliConfigStatic, EveryValuelessBooleanDefaultsToFalse) {
    // The file expresses `false` for a valueless flag by emitting nothing, which is only sound while
    // absence means false. A default flipped to true would make that key accept `= false` and ignore
    // it — silently, which is the whole family of defect this file is about.
    const ob::ServerConfig defaults;
    const std::map<std::string, bool> defaults_by_key = {
        {"multi-master",         defaults.multi_master},
        {"no-sqpoll",            defaults.uring_no_sqpoll},
        {"read-only",            defaults.read_only},
        {"replication-compress", defaults.replication_compress},
    };

    for (const std::string& key : ob::boolean_flags()) {
        if (key == "print-config") continue;   // not a configuration value; it exits the process
        const auto it = defaults_by_key.find(key);
        ASSERT_NE(it, defaults_by_key.end())
            << key << " is a valueless boolean flag and this test does not know its default. Add it "
                     "here rather than removing the check.";
        EXPECT_FALSE(it->second)
            << key << " defaults to true, so `" << key
            << " = false` in a config file emits nothing and is silently ignored. Either give the "
               "flag a value on the command line, or add an explicit negation.";
    }
}

// `--help` was six hardcoded lines in `tools/ob_tcp_server.cpp` while the parser accepted forty
// flags. The omissions that matter most: `--config` and `--print-config`, which exist so that forty
// flags are manageable at all, and `--fsync-policy`, the durability setting in a database - which
// roadmap #33 had already found missing once, in the other direction. `--help` is the first command
// anybody runs against an unfamiliar binary, so what it omits is what the engine does not appear to
// have.
//
// Generating the text from `known_flags()` makes the drift impossible rather than absent, and this
// test is the half that points the other way: a flag with no description reaches the operator as
// `(undocumented)` and stops here first.
TEST(CliConfigStatic, EveryKnownFlagIsDocumented) {
    const std::string usage = ob::format_usage("ob_tcp_server");

    for (const std::string& flag : ob::known_flags()) {
        EXPECT_NE(usage.find("--" + flag), std::string::npos)
            << flag << " is accepted by the parser and absent from --help, so an operator reading "
                       "the one command everyone runs first cannot discover it";
    }

    EXPECT_EQ(usage.find("(undocumented)"), std::string::npos)
        << "a flag reached --help without a description. Add one to flag_help() in "
           "src/tcp_server.cpp; the placeholder is deliberately visible rather than blank, because "
           "a blank line reads as a flag that does not exist";

    EXPECT_NE(usage.find("--help"), std::string::npos);
}

// Two shapes, and the difference is visible to whoever reads the text: a flag taking a value shows
// its placeholder, a valueless boolean shows none. Getting this backwards teaches the operator to
// pass a value the parser will reject, or to omit one it needs.
TEST(CliConfigStatic, HelpShowsAPlaceholderForValueFlagsAndNoneForBooleans) {
    const std::string usage = ob::format_usage("ob_tcp_server");

    for (const std::string& flag : ob::boolean_flags()) {
        const auto position = usage.find("--" + flag);
        ASSERT_NE(position, std::string::npos) << flag;
        const auto line_end = usage.find('\n', position);
        const std::string line = usage.substr(position, line_end - position);
        EXPECT_EQ(line.find('<'), std::string::npos)
            << flag << " is a valueless boolean and --help shows it taking an argument: " << line;
    }

    // `port` takes a value, and the placeholder is the only thing in the text that says so.
    const auto position = usage.find("--port ");
    ASSERT_NE(position, std::string::npos);
    const auto line_end = usage.find('\n', position);
    EXPECT_NE(usage.substr(position, line_end - position).find('<'), std::string::npos)
        << "--port takes a value and --help does not show a placeholder for it";
}

// The values a flag accepts, taken from the parser rather than from what I remembered writing.
//
// This test exists because the first version of the generated help said `--fsync-policy` takes
// ALWAYS, INTERVAL or NEVER. The parser compares against `every`, `interval` and `none`, lower
// case and exactly - so two of the three documented values are refused, and the operator finds out
// when the server will not start. Documentation that names values the parser rejects is worse than
// no documentation, because it is followed.
//
// The two enum flags do not even share a case convention: `log-level = INFO` and
// `fsync-policy = interval`, both from the shipped `ob.conf`. That is exactly the kind of thing an
// operator gets wrong once per install, so both descriptions say which case they want.
//
// Limit worth stating: this only sees values the parser compares as literals in its own branch.
// `--log-level` delegates to `StructuredLogger::parse_level()`, so its four values are not visible
// here and are covered by the message in the parser instead.
TEST(CliConfigStatic, DocumentedEnumValuesAreTheOnesTheParserAccepts) {
    const std::string source = read_source("src/tcp_server.cpp");
    ASSERT_FALSE(source.empty());
    const size_t parser = source.find("ResolvedConfig resolve_cli_args(");
    ASSERT_NE(parser, std::string::npos);
    const std::string body = source.substr(parser);
    const std::string usage = ob::format_usage("ob_tcp_server");

    // Split the parser into one chunk per flag, so a literal is attributed to the flag whose
    // branch holds it.
    const std::regex branch(R"(arg == \"--([a-z0-9-]+)\")");
    std::vector<std::pair<std::string, size_t>> starts;
    for (auto it = std::sregex_iterator(body.begin(), body.end(), branch);
         it != std::sregex_iterator(); ++it) {
        starts.emplace_back((*it)[1].str(), static_cast<size_t>(it->position(0)));
    }
    ASSERT_GT(starts.size(), 30u) << "the branch scan found almost nothing, so it is not scanning";

    size_t checked = 0;
    for (size_t i = 0; i < starts.size(); ++i) {
        const std::string& flag = starts[i].first;
        const size_t from = starts[i].second;
        const size_t to = (i + 1 < starts.size()) ? starts[i + 1].second : body.size();
        const std::string chunk = body.substr(from, to - from);

        // Only comparisons against a value the flag itself carries, which is what an enum flag
        // looks like in this parser: `val == "every"`.
        const std::regex literal(R"((?:val|value|level|policy) == \"([a-z][a-z0-9_-]*)\")");
        std::set<std::string> accepted;
        for (auto it = std::sregex_iterator(chunk.begin(), chunk.end(), literal);
             it != std::sregex_iterator(); ++it) {
            accepted.insert((*it)[1].str());
        }
        if (accepted.empty()) continue;

        const size_t line_start = usage.find("--" + flag);
        ASSERT_NE(line_start, std::string::npos) << flag;
        const std::string line = usage.substr(line_start, usage.find('\n', line_start) - line_start);

        for (const std::string& value : accepted) {
            EXPECT_NE(line.find(value), std::string::npos)
                << "--" << flag << " accepts '" << value << "' and --help does not mention it: "
                << line;
            ++checked;
        }
    }
    EXPECT_GT(checked, 0u) << "no enum flag was checked, so this test proves nothing";
}

// The man page tells every host the package is installed on that the full flag set lives in
// `cli.md`. It has to be true there, not just true when someone last looked: `--help` listed six of
// forty, the man page pointed here for the rest, and this file had twenty-one - so the artefact
// that promised completeness was the incomplete one, and the promise is printed on the host.
TEST(CliConfigStatic, EveryKnownFlagIsInTheCliReference) {
    const std::string reference = read_source("docs/cli.md");
    ASSERT_FALSE(reference.empty()) << "cannot read docs/cli.md, which the man page points at";

    // The row, not the string. The first version searched the whole file, and its mutation - a row
    // deleted from the table - **passed**, because the same flag is named in a paragraph a few
    // lines above. A flag mentioned in prose and absent from the reference is exactly the gap this
    // test is for, so it looks for the table row: `| `--flag` |`.
    std::vector<std::string> missing;
    for (const std::string& flag : ob::known_flags()) {
        if (reference.find("| `--" + flag + "` |") == std::string::npos) missing.push_back(flag);
    }
    EXPECT_TRUE(missing.empty())
        << missing.size() << " flag(s) the parser accepts are absent from docs/cli.md, which "
        << "packaging/ob_tcp_server.1 names as the full set: "
        << [&missing] {
               std::string joined;
               for (const auto& flag : missing) joined += "--" + flag + " ";
               return joined;
           }();
}
