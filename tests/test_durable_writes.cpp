// Every file this engine opens for writing says what takes it to the device before anything relies
// on it, and the list of such files is the tree rather than this file (#160, #162).
//
// The engine synced only its WAL until #160, and three files beside it paid for that, each found on
// its own: segment files came back empty after a power cut under a checkpoint that claimed them
// (0 rows of 201), the WAL identity came back empty, and a replica killed while it rewrote its
// position in place wiped its store and streamed the whole log again (#162, measured with the
// write held open at the kernel). Every one of them was a file somebody wrote without asking what a
// crash or a cut leaves of it, so the rule is about the next one as much as those three.
//
// There are two ways to be right. A file replaced through `write_file_atomically()`
// (`include/orderbook/durable_file.hpp`) opens nothing at its call site, so it is not a site here.
// Any other open for writing carries, within four lines above it, a comment saying what makes its
// bytes durable before anything relies on them: `OB_DURABLE: <what>`. Checked both ways: an
// unmarked open fails, and so does a marker with no open under it, because a marker a refactor
// left behind is a claim about nothing.
//
// The scan reads the code with comments and string literals blanked, so a sentence about `::open`
// cannot count as one - this repository has paid for that confusion four times, twice inside the
// checker written to catch it - and the last test gives it a snippet in which it must find five
// sites and ignore four.

#include <gtest/gtest.h>

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

namespace {

constexpr const char* kMarker = "OB_DURABLE:";
constexpr size_t kMarkerReach = 4;   // lines above an open in which its marker may stand

std::string read_file(const std::filesystem::path& p) {
    std::ifstream in(p);
    if (!in) return {};
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

/// Two views of one source, both the length of the original with every newline in place, so an
/// offset in either is an offset in the file: `code` blanks comments, `bare` blanks string and
/// character literals as well. Sites are found in `bare`; an `fopen` mode is read from `code`.
struct Views {
    std::string code;
    std::string bare;
};

Views blank(const std::string& src) {
    Views v{src, src};
    const auto wipe = [&](size_t from, size_t to, bool literal) {
        for (size_t k = from; k < to && k < src.size(); ++k) {
            if (src[k] == '\n') continue;
            v.bare[k] = ' ';
            if (!literal) v.code[k] = ' ';
        }
    };
    for (size_t i = 0; i < src.size();) {
        if (src.compare(i, 2, "//") == 0) {
            const size_t nl = src.find('\n', i);
            const size_t end = nl == std::string::npos ? src.size() : nl;
            wipe(i, end, false);
            i = end;
        } else if (src.compare(i, 2, "/*") == 0) {
            const size_t close = src.find("*/", i + 2);
            const size_t end = close == std::string::npos ? src.size() : close + 2;
            wipe(i, end, false);
            i = end;
        } else if (src.compare(i, 2, "R\"") == 0 && src.find('(', i + 2) != std::string::npos) {
            const size_t open = src.find('(', i + 2);
            const std::string delim = src.substr(i + 2, open - (i + 2));
            const size_t close = src.find(")" + delim + "\"", open);
            const size_t end = close == std::string::npos ? src.size() : close + delim.size() + 2;
            wipe(i + 2, end - 1, true);
            i = end;
        } else if (src[i] == '"' || src[i] == '\'') {
            const char quote = src[i];
            size_t j = i + 1;
            while (j < src.size() && src[j] != quote && src[j] != '\n') j += (src[j] == '\\') ? 2 : 1;
            wipe(i + 1, j, true);
            i = j + 1;
        } else {
            ++i;
        }
    }
    return v;
}

/// The text of a call's argument list, from the '(' at `open_paren` to its matching ')'.
std::string call_args(const std::string& text, size_t open_paren) {
    int depth = 0;
    for (size_t k = open_paren; k < text.size(); ++k) {
        if (text[k] == '(') ++depth;
        if (text[k] == ')' && --depth == 0) return text.substr(open_paren, k - open_paren + 1);
    }
    return text.substr(open_paren);
}

struct Site {
    size_t line;       // 1-based
    std::string what;
};

size_t line_of(const std::string& text, size_t offset) {
    return static_cast<size_t>(std::count(text.begin(), text.begin() + static_cast<long>(offset), '\n')) + 1;
}

bool ident(char c) { return std::isalnum(static_cast<unsigned char>(c)) || c == '_'; }

/// Where `name` is called in `text`: not preceded by an identifier character (so `fdopen` is not
/// `fopen`), and followed by optional spaces and '(' (so `::create_directories` is not `::creat`
/// and `open_current` is not `open`). Returns the offset of the '('. Plain search rather than
/// `std::regex`, which took nine seconds a test over this tree in a Debug build.
std::vector<size_t> calls_of(const std::string& text, const std::string& name) {
    std::vector<size_t> out;
    for (size_t at = text.find(name); at != std::string::npos; at = text.find(name, at + 1)) {
        if (at > 0 && ident(name.front()) && ident(text[at - 1])) continue;
        size_t k = at + name.size();
        while (k < text.size() && (text[k] == ' ' || text[k] == '\t')) ++k;
        if (k < text.size() && text[k] == '(') out.push_back(k);
    }
    return out;
}

/// Every open for writing in one source: `::open` with a writing flag, an `ofstream`, an `fopen`
/// whose mode writes, `mkstemp`, `::creat` and `copy_file`.
std::vector<Site> write_sites(const std::string& src) {
    const Views v = blank(src);
    std::vector<Site> out;
    for (const size_t paren : calls_of(v.bare, "::open")) {
        const std::string args = call_args(v.bare, paren);
        if (args.find("O_WRONLY") != std::string::npos || args.find("O_RDWR") != std::string::npos ||
            args.find("O_CREAT") != std::string::npos) {
            out.push_back({line_of(v.bare, paren), "::open"});
        }
    }
    for (size_t at = v.bare.find("ofstream"); at != std::string::npos;
         at = v.bare.find("ofstream", at + 1)) {
        const bool before = at == 0 || !ident(v.bare[at - 1]);
        const bool after = at + 8 >= v.bare.size() || !ident(v.bare[at + 8]);
        if (before && after) out.push_back({line_of(v.bare, at), "ofstream"});
    }
    for (const size_t paren : calls_of(v.bare, "fopen")) {
        const std::string args = call_args(v.code, paren);   // the mode is a literal
        const size_t quote = args.size() >= 2 ? args.rfind('"', args.size() - 2) : std::string::npos;
        const size_t start =
            quote == std::string::npos || quote == 0 ? std::string::npos : args.rfind('"', quote - 1);
        // A mode that is not a literal cannot be read here, and so it counts as a write.
        if (start == std::string::npos ||
            args.substr(start + 1, quote - start - 1).find_first_of("wa+") != std::string::npos) {
            out.push_back({line_of(v.bare, paren), "fopen"});
        }
    }
    for (const char* name : {"mkstemp", "mkostemp", "::creat", "copy_file"}) {
        for (const size_t paren : calls_of(v.bare, name)) out.push_back({line_of(v.bare, paren), name});
    }
    std::sort(out.begin(), out.end(), [](const Site& a, const Site& b) { return a.line < b.line; });
    return out;
}

std::vector<std::string> lines_of(const std::string& src) {
    std::vector<std::string> out;
    size_t start = 0;
    for (size_t nl = src.find('\n'); nl != std::string::npos; nl = src.find('\n', start)) {
        out.push_back(src.substr(start, nl - start));
        start = nl + 1;
    }
    out.push_back(src.substr(start));
    return out;
}

bool marked_above(const std::vector<std::string>& lines, size_t line) {
    const size_t first = line > kMarkerReach ? line - kMarkerReach : 1;
    for (size_t l = first; l <= line; ++l) {
        if (lines[l - 1].find(kMarker) != std::string::npos) return true;
    }
    return false;
}

std::vector<std::filesystem::path> engine_sources() {
    const std::filesystem::path root(OB_SOURCE_DIR);
    std::vector<std::filesystem::path> out;
    for (const char* dir : {"src", "include/orderbook", "tools"}) {
        for (const auto& e : std::filesystem::directory_iterator(root / dir)) {
            const auto ext = e.path().extension();
            if (e.is_regular_file() && (ext == ".cpp" || ext == ".hpp")) out.push_back(e.path());
        }
    }
    std::sort(out.begin(), out.end());
    return out;
}

std::string rel(const std::filesystem::path& p) {
    return std::filesystem::relative(p, std::filesystem::path(OB_SOURCE_DIR)).string();
}

}  // namespace

TEST(DurableWrites, EveryOpenForWritingSaysWhatMakesItDurable) {
    std::vector<std::string> unmarked;
    size_t sites = 0;
    bool saw_wal = false, saw_segments = false;
    for (const auto& path : engine_sources()) {
        const std::string src = read_file(path);
        const auto lines = lines_of(src);
        for (const Site& s : write_sites(src)) {
            ++sites;
            if (rel(path) == "src/wal.cpp") saw_wal = true;
            if (rel(path) == "src/columnar_store.cpp") saw_segments = true;
            if (!marked_above(lines, s.line)) {
                unmarked.push_back(rel(path) + ":" + std::to_string(s.line) + " (" + s.what + ")");
            }
        }
    }
    // A scan that finds nothing passes this test, so it has to find the two opens every build has.
    ASSERT_TRUE(saw_wal && saw_segments)
        << "the scan found " << sites << " open(s) for writing and not the WAL's or the segment "
        << "writer's, so it is not reading what it should";
    std::string list;
    for (const auto& u : unmarked) list += "\n  " + u;
    EXPECT_TRUE(unmarked.empty())
        << unmarked.size() << " open(s) for writing say nothing about what takes their bytes to the "
        << "device before anything relies on them. Replace the file through "
        << "write_file_atomically(), or say in `" << kMarker << " <what>` within " << kMarkerReach
        << " lines above the open what syncs it:" << list;
}

TEST(DurableWrites, EveryMarkerHasAnOpenUnderIt) {
    std::vector<std::string> dangling;
    for (const auto& path : engine_sources()) {
        const std::string src = read_file(path);
        const auto lines = lines_of(src);
        const auto sites = write_sites(src);
        for (size_t l = 1; l <= lines.size(); ++l) {
            if (lines[l - 1].find(kMarker) == std::string::npos) continue;
            const bool covers = std::any_of(sites.begin(), sites.end(), [&](const Site& s) {
                return s.line >= l && s.line <= l + kMarkerReach;
            });
            if (!covers) dangling.push_back(rel(path) + ":" + std::to_string(l));
        }
    }
    std::string list;
    for (const auto& d : dangling) list += "\n  " + d;
    EXPECT_TRUE(dangling.empty())
        << "a durability marker with no open for writing under it is a claim about nothing - left "
        << "behind by a change that moved or removed the open:" << list;
}

TEST(DurableWrites, TheScanFindsEachKindOfOpenAndNothingThatOnlyMentionsOne) {
    const std::string snippet =
        "int a = ::open(p, O_WRONLY | O_CREAT, 0644);\n"          // 1: a site
        "std::ofstream f(p);\n"                                   // 2: a site
        "FILE* g = std::fopen(p, \"w\");\n"                       // 3: a site
        "int t = ::mkstemp(buf);\n"                               // 4: a site
        "fs::copy_file(a, b, fs::copy_options::none);\n"          // 5: a site
        "int r = ::open(p, O_RDONLY);\n"                          // 6: reads
        "FILE* h = std::fopen(p, \"r\");\n"                       // 7: reads
        "// ::open(p, O_WRONLY | O_CREAT) in a comment\n"         // 8: prose
        "const char* s = \"std::ofstream and ::mkstemp(\";\n";    // 9: a string
    std::vector<size_t> found;
    for (const Site& s : write_sites(snippet)) found.push_back(s.line);
    EXPECT_EQ(found, (std::vector<size_t>{1, 2, 3, 4, 5}));
}
