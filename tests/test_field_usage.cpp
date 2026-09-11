// A field written at one site and read at none (#104).
//
// This is the sixth instance of the shape in this workspace - after `provisional`, `basis`,
// `in_use`, `key_id` and `partition_by` in the flagship product - and every one of the five was
// found while looking for something else. None of them had a behavioural symptom: the value goes
// in, nothing takes it out, and the docstring next to it describes a mechanism that does not exist.
// A behavioural test cannot ask this question, because there is nothing to observe. So the check is
// static, it runs over the whole engine rather than over one class, and its verdict is a list.
//
// **What it claims, exactly**: no member declared in `include/orderbook/` has *every* one of its
// occurrences in `src/` and `include/` be a plain write with the value discarded. That is narrower
// than "nothing is dead", and the narrowness is deliberate - anything this file cannot read as a
// write counts as a read, so a finding here is a field nothing consumes by any route the checker
// understands.
//
// **What it therefore does not cover**, named rather than left to be discovered: a field filled
// through a non-const reference (`read_into(x_)`), through `memcpy(&x_, ...)`, or by a free
// `std::swap(x_, y_)` reads as used, because the name is not the target of the statement. Tests and
// `tools/` are not searched: a field whose only reader is a test is not a mechanism, and a tool can
// only reach a private member through an accessor, which lives in a header and is searched.
#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace {

// ── One line of source, two questions ────────────────────────────────────────
//
// Both are pure functions of a line, which is what lets the test below try them on cases that are
// not in the tree - each of those cases is a mistake this checker actually made before it shipped.

/// The line without a trailing `//` comment.
///
/// Load-bearing rather than tidy: **four members were silently skipped** by an earlier version of
/// the declaration rule because their trailing comment contained a parenthesis
/// (`uint64_t compress_bytes_in_{0};   // raw bytes (before compression)`), and a checker that
/// quietly stops covering a member is worse than no checker.
std::string code_of(const std::string& line) {
    const std::size_t at = line.find("//");
    return at == std::string::npos ? line : line.substr(0, at);
}

bool ident_char(char c) {
    return std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '_';
}

/// Every identifier on the line that ends in `_`, with where it starts. The trailing underscore is
/// this codebase's member convention, and it is also what keeps functions out: no method here is
/// named with one, measured across every header.
std::vector<std::pair<std::string, std::size_t>> trailing_underscore_names(const std::string& s) {
    std::vector<std::pair<std::string, std::size_t>> out;
    for (std::size_t i = 0; i < s.size();) {
        if (!ident_char(s[i]) || (i > 0 && ident_char(s[i - 1]))) { ++i; continue; }
        std::size_t j = i;
        while (j < s.size() && ident_char(s[j])) ++j;
        if (s[j - 1] == '_' && !std::isdigit(static_cast<unsigned char>(s[i]))) {
            out.emplace_back(s.substr(i, j - i), i);
        }
        i = j;
    }
    return out;
}

const std::set<std::string>& statement_keywords() {
    static const std::set<std::string> kw = {
        "return", "if", "while", "for", "else", "do", "switch", "case", "break", "continue",
        "throw", "delete", "new", "goto", "using", "static_assert"};
    return kw;
}

/// The member this line declares, or empty.
///
/// `return message_;` matches the shape of a declaration exactly - indented, ends in `;`, a
/// trailing-underscore name before it - and an earlier version of this rule filed it as one, which
/// hid the *only* read of `Result<void>::message_` and reported that field as dead. Hence the two
/// conditions that look redundant and are not: a type must precede the name, and `return` must not.
std::string declared_name(const std::string& line) {
    const std::string c = code_of(line);
    if (c.empty() || (c[0] != ' ' && c[0] != '\t')) return {};   // members are indented
    if (c.find(';') == std::string::npos) return {};

    std::string trimmed = c.substr(c.find_first_not_of(" \t"));
    if (trimmed.rfind("/*", 0) == 0 || trimmed.rfind("*", 0) == 0 ||
        trimmed.rfind("#", 0) == 0) {
        return {};
    }
    std::size_t word_end = 0;
    while (word_end < trimmed.size() && ident_char(trimmed[word_end])) ++word_end;
    if (statement_keywords().count(trimmed.substr(0, word_end))) return {};

    for (const auto& [name, at] : trailing_underscore_names(c)) {
        std::size_t after = at + name.size();
        while (after < c.size() && c[after] == ' ') ++after;
        if (after >= c.size()) continue;
        const char next = c[after];
        const bool declarator = next == ';' || next == '{' || next == '[' ||
                                (next == '=' && after + 1 < c.size() && c[after + 1] != '=');
        if (!declarator) continue;

        // Not a member of something else (`r.message_ = x;`, `st->active_ = false;`). Checked on
        // the character **immediately** before the name rather than the first non-space one: a
        // template argument list also ends in `>`, so skipping spaces rejected
        // `std::atomic<int> level_{...}` - a real declaration, and the kind of member this file
        // exists to enumerate.
        if (at > 0 && (c[at - 1] == '.' || c[at - 1] == '>' || c[at - 1] == ':')) continue;
        const std::string prefix = c.substr(0, at);
        if (prefix.find("return") != std::string::npos) continue;
        if (prefix.find_first_not_of(" \t") == std::string::npos) continue;   // a type must precede

        // What the prefix *ends with* is the whole distinction, and missing it produced six false
        // findings: `confirmed = last_ownership_confirmed_;` and `auto& buf = pending_writes_[fd];`
        // have the shape of a declaration - indented, a name before a declarator, something in
        // front of it - and filing them as declarations deleted the only read those fields had. A
        // declaration's prefix ends in a type: an identifier character, a template's `>`, or a `*`
        // or `&`. An assignment's ends in `=`.
        const char last = prefix[prefix.find_last_not_of(" \t")];
        if (!ident_char(last) && last != '>' && last != '*' && last != '&') continue;
        return name;
    }
    return {};
}

const std::set<std::string>& mutators() {
    static const std::set<std::string> m = {
        "store", "reset", "clear", "assign", "resize", "swap", "push_back", "emplace_back",
        "emplace_front", "emplace", "insert", "erase", "pop_back", "pop_front", "shrink_to_fit",
        "fetch_add", "fetch_sub", "fetch_or", "exchange", "notify_one", "notify_all"};
    return m;
}

std::size_t whole_word_count(const std::string& s, const std::string& name) {
    std::size_t n = 0;
    for (std::size_t at = s.find(name); at != std::string::npos; at = s.find(name, at + 1)) {
        const bool left  = at == 0 || !ident_char(s[at - 1]);
        const bool right = at + name.size() >= s.size() || !ident_char(s[at + name.size()]);
        if (left && right) ++n;
    }
    return n;
}

/// Whether this line writes `name` and discards the value.
///
/// The rule is "the name is the statement's target", and it is what tells the six false findings of
/// the first version apart from the one real one. `conn.conn_id = next_conn_id_++;` increments a
/// member **and hands its value to somebody**; so does
/// `if (!unknown_names_reported_.insert(x).second)`. A mutating call whose result is consumed is a
/// read, and the cheap way to say that is: the statement must *begin* with the name.
bool pure_write(const std::string& line, const std::string& name) {
    const std::string c = code_of(line);
    const std::size_t first = c.find_first_not_of(" \t");
    if (first == std::string::npos) return false;
    const std::string s = c.substr(first);

    if (whole_word_count(s, name) != 1) return false;   // `x_ = x_ + 1;` reads as well as writes

    // A constructor's initialiser list writes too: `: pos_(0), end_(0) {}`. Without this, a member
    // set only there and never read would be reported as used by the line that fills it.
    if ((s[0] == ':' || s[0] == ',') || s.find(") :") != std::string::npos) {
        const std::size_t at = s.find(name);
        std::size_t after = at + name.size();
        while (after < s.size() && s[after] == ' ') ++after;
        if (after < s.size() && s[after] == '(') return true;
    }

    // The target: an optional `this->` and an optional chain of `obj.` / `obj->`, then the name.
    std::size_t cursor = 0;
    if (s.rfind("this->", 0) == 0) cursor = 6;
    while (true) {
        std::size_t probe = cursor;
        while (probe < s.size() && ident_char(s[probe])) ++probe;
        if (probe > cursor && probe < s.size() &&
            (s[probe] == '.' || (s[probe] == '-' && probe + 1 < s.size() && s[probe + 1] == '>'))) {
            if (s.compare(cursor, probe - cursor, name) == 0) break;   // the name itself
            cursor = probe + (s[probe] == '.' ? 1 : 2);
            continue;
        }
        break;
    }
    if (s.compare(cursor, name.size(), name) != 0) return false;
    std::size_t after = cursor + name.size();
    if (after < s.size() && ident_char(s[after])) return false;
    while (after < s.size() && s[after] == ' ') ++after;
    if (after >= s.size()) return false;

    const std::string tail = s.substr(after);
    if (tail[0] == '=' && (tail.size() < 2 || tail[1] != '=')) return true;
    static const char* kCompound[] = {"+=", "-=", "*=", "/=", "|=", "&=", "^="};
    for (const char* op : kCompound) {
        if (tail.rfind(op, 0) == 0) return true;
    }
    if (tail.rfind("++", 0) == 0 || tail.rfind("--", 0) == 0) {
        return tail.find(';') != std::string::npos && tail.find_first_not_of("+-; \t") == std::string::npos;
    }
    if (tail[0] == '.') {
        std::size_t end = 1;
        while (end < tail.size() && ident_char(tail[end])) ++end;
        if (end < tail.size() && tail[end] == '(' && mutators().count(tail.substr(1, end - 1))) {
            return true;
        }
    }
    return false;
}

// ── The tree ─────────────────────────────────────────────────────────────────

std::vector<std::string> read_lines(const std::filesystem::path& p) {
    std::vector<std::string> out;
    std::ifstream in(p);
    for (std::string line; std::getline(in, line);) out.push_back(line);
    return out;
}

std::vector<std::filesystem::path> sources_under(const std::string& rel, const char* ext) {
    std::vector<std::filesystem::path> out;
    const std::filesystem::path root = std::filesystem::path(OB_SOURCE_DIR) / rel;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (entry.is_regular_file() && entry.path().extension() == ext) out.push_back(entry.path());
    }
    std::sort(out.begin(), out.end());
    return out;
}

std::string short_path(const std::filesystem::path& p) {
    return std::filesystem::relative(p, std::filesystem::path(OB_SOURCE_DIR)).string();
}

struct Field {
    std::vector<std::string> declared_at;
    std::vector<std::string> writes;
    std::vector<std::string> reads;
};

std::map<std::string, Field> survey() {
    std::map<std::string, Field> fields;
    const auto headers = sources_under("include/orderbook", ".hpp");

    for (const auto& h : headers) {
        int n = 0;
        for (const auto& line : read_lines(h)) {
            ++n;
            const std::string name = declared_name(line);
            if (!name.empty()) {
                fields[name].declared_at.push_back(short_path(h) + ":" + std::to_string(n));
            }
        }
    }

    std::vector<std::filesystem::path> all = sources_under("src", ".cpp");
    all.insert(all.end(), headers.begin(), headers.end());

    for (const auto& f : all) {
        int n = 0;
        for (const auto& line : read_lines(f)) {
            ++n;
            const std::string where = short_path(f) + ":" + std::to_string(n);
            for (const auto& [name, at] : trailing_underscore_names(code_of(line))) {
                (void)at;
                auto it = fields.find(name);
                if (it == fields.end()) continue;
                // Skipped by **recorded site**, not by asking the rule again: a line the rule
                // misreads as a declaration would otherwise vanish from both columns, which is how
                // a read gets deleted rather than merely misfiled.
                const auto& decls = it->second.declared_at;
                if (std::find(decls.begin(), decls.end(), where) != decls.end()) continue;
                if (pure_write(line, name)) it->second.writes.push_back(where);
                else                        it->second.reads.push_back(where);
            }
        }
    }
    return fields;
}

}  // namespace

TEST(FieldUsage, TheRulesAreTriedOnTheCasesTheyGotWrong) {
    // The pair that keeps the survey below from passing by understanding nothing. Every case here
    // is a mistake an earlier version of these two functions actually made, which is a better
    // source of test cases than imagination.
    EXPECT_EQ(declared_name("    std::string             adopted_primary_address_;"),
              "adopted_primary_address_");
    EXPECT_EQ(declared_name("    uint64_t    compress_bytes_in_{0};   // raw bytes (before it)"),
              "compress_bytes_in_")
        << "a trailing comment with a parenthesis hid four members from an earlier version";
    EXPECT_EQ(declared_name("    const std::string& error_message() const { return message_; }"),
              "")
        << "an accessor's `return message_;` has the shape of a declaration, and filing it as one "
           "hid the only read of that field";
    EXPECT_EQ(declared_name("        adopted_primary_address_  = new_primary;"), "")
        << "an assignment is not a declaration: nothing precedes the name";
    EXPECT_EQ(declared_name("    std::atomic<int> level_{static_cast<int>(LogLevel::INFO)};"),
              "level_")
        << "a parenthesis inside the initialiser is not a function signature, and a template's "
           "closing `>` before the name is not a `->`";
    EXPECT_EQ(declared_name("                    confirmed = last_ownership_confirmed_;"), "")
        << "reading a member into a local has the shape of a declaration; six fields were reported "
           "as dead because their only read looked like this";
    EXPECT_EQ(declared_name("                    std::string block = minsert_header_;"), "");
    EXPECT_EQ(declared_name("    auto& buf = pending_writes_[fd];"), "")
        << "a subscript is a declarator too, so the prefix is what has to decide";
    EXPECT_EQ(declared_name("    meta.wal_file_index  = wal_file_index_;"), "");

    EXPECT_TRUE(pure_write("        adopted_primary_address_  = new_primary;",
                           "adopted_primary_address_"));
    EXPECT_TRUE(pure_write("        r.message_ = std::move(msg);", "message_"))
        << "a write through another object of the same type is still a write";
    EXPECT_TRUE(pure_write("    : buf_(buf_size), pos_(0), end_(0) {}", "pos_"))
        << "a constructor's initialiser list is the one write some members ever get";
    EXPECT_TRUE(pure_write("    running_.store(false, std::memory_order_release);", "running_"));
    EXPECT_TRUE(pure_write("    standalone_polls_++;", "standalone_polls_"));

    EXPECT_FALSE(pure_write("    conn.conn_id = next_conn_id_++;", "next_conn_id_"))
        << "post-increment hands the old value to somebody, so it reads";
    EXPECT_FALSE(pure_write("    const uint64_t token = next_snapshot_token_++;",
                            "next_snapshot_token_"));
    EXPECT_FALSE(pure_write("    if (!unknown_names_reported_.insert(std::string(name)).second) {",
                            "unknown_names_reported_"))
        << "a mutating call whose result is consumed is a read";
    EXPECT_FALSE(pure_write("    info.conn_id = next_conn_id_.fetch_add(1);", "next_conn_id_"));
    EXPECT_FALSE(pure_write("    pending_ = pending_ + 1;", "pending_"))
        << "the name on both sides means one of them is a read";
    EXPECT_FALSE(pure_write("    registry_.set_gauge(\"ob_current_epoch\", 1);", "registry_"))
        << "handing a member to a method is a use, and only a named mutator is a write";
    EXPECT_FALSE(pure_write("    std::lock_guard<std::mutex> lk(mtx_);", "mtx_"));
}

TEST(FieldUsage, EveryMemberShapedNameInAHeaderIsEnumerated) {
    // The survey's own tripwire, and it earned it: five names were missing when this comparison was
    // first run, each for a different reason in the declaration rule. A static test whose subject
    // can quietly shrink is not a test - so the refined enumeration is checked against a
    // deliberately cruder one that cannot be narrowed by accident.
    std::set<std::string> crude;
    for (const auto& h : sources_under("include/orderbook", ".hpp")) {
        for (const auto& line : read_lines(h)) {
            const std::string c = code_of(line);
            for (const auto& [name, at] : trailing_underscore_names(c)) {
                std::size_t after = at + name.size();
                while (after < c.size() && c[after] == ' ') ++after;
                if (after >= c.size()) continue;
                const char next = c[after];
                if (next == ';' || next == '{' || next == '[' ||
                    (next == '=' && after + 1 < c.size() && c[after + 1] != '=')) {
                    crude.insert(name);
                    break;
                }
            }
        }
    }

    const auto fields = survey();
    std::vector<std::string> missing;
    for (const auto& name : crude) {
        if (fields.find(name) == fields.end()) missing.push_back(name);
    }

    ASSERT_GE(crude.size(), 200u)
        << "only " << crude.size() << " member-shaped names found in the headers, which is the "
        << "wrong order of magnitude - the scan itself is broken";
    std::ostringstream why;
    for (const auto& name : missing) why << "\n    " << name;
    EXPECT_TRUE(missing.empty())
        << missing.size() << " name(s) look like members to a crude scan and are not enumerated, "
        << "so nothing checks them:" << why.str();
}

TEST(FieldUsage, NoMemberIsWrittenAndNeverRead) {
    const auto fields = survey();

    std::ostringstream findings;
    std::size_t count = 0;
    for (const auto& [name, field] : fields) {
        if (field.writes.empty() || !field.reads.empty()) continue;
        ++count;
        findings << "\n  " << name << "\n      declared at " << field.declared_at.front();
        for (const auto& w : field.writes) findings << "\n      written at  " << w;
    }

    EXPECT_EQ(count, 0u)
        << count << " field(s) are written and never read. Either something reads this by a route "
        << "this check cannot see - in which case say so where the field is declared - or the "
        << "field is not the mechanism its docstring claims, and the guarantee lives at the call "
        << "sites (#104):" << findings.str();
}
