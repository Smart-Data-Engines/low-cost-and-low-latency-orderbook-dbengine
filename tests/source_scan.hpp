#pragma once

// Reading this engine's own sources, for the static tests that hold a rule about all of them
// (#162, #163).
//
// One copy, because a second one is how two such tests come to disagree about what counts as code:
// this repository has paid four times for a scanner that read a sentence about a call as the call,
// twice inside the checker written to catch that. Everything here works on views of the source in
// which comments and literals are blanked out and every newline is kept, so an offset in a view is
// an offset in the file and a line number is a line number.

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

namespace ob::source_scan {

inline std::string read_file(const std::filesystem::path& p) {
    std::ifstream in(p);
    if (!in) return {};
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

/// Two views of one source, both the length of the original with every newline in place: `code`
/// blanks comments, `bare` blanks string and character literals as well. Calls are found in
/// `bare`; a literal argument, such as an `fopen` mode, is read from `code`.
struct Views {
    std::string code;
    std::string bare;
};

inline Views blank(const std::string& src) {
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
inline std::string call_args(const std::string& text, size_t open_paren) {
    int depth = 0;
    for (size_t k = open_paren; k < text.size(); ++k) {
        if (text[k] == '(') ++depth;
        if (text[k] == ')' && --depth == 0) return text.substr(open_paren, k - open_paren + 1);
    }
    return text.substr(open_paren);
}

inline size_t line_of(const std::string& text, size_t offset) {
    return static_cast<size_t>(
               std::count(text.begin(), text.begin() + static_cast<long>(offset), '\n')) + 1;
}

inline bool ident(char c) { return std::isalnum(static_cast<unsigned char>(c)) || c == '_'; }

/// Where `name` is called in `text`: not preceded by an identifier character (so `fdopen` is not
/// `fopen`), and followed by optional spaces and '(' (so `::create_directories` is not `::creat`
/// and `open_current` is not `open`). Returns the offset of the '('. Plain search rather than
/// `std::regex`, which took nine seconds a test over this tree in a Debug build.
inline std::vector<size_t> calls_of(const std::string& text, const std::string& name) {
    std::vector<size_t> out;
    for (size_t at = text.find(name); at != std::string::npos; at = text.find(name, at + 1)) {
        if (at > 0 && ident(name.front()) && ident(text[at - 1])) continue;
        size_t k = at + name.size();
        while (k < text.size() && (text[k] == ' ' || text[k] == '\t')) ++k;
        if (k < text.size() && text[k] == '(') out.push_back(k);
    }
    return out;
}

inline std::vector<std::string> lines_of(const std::string& src) {
    std::vector<std::string> out;
    size_t start = 0;
    for (size_t nl = src.find('\n'); nl != std::string::npos; nl = src.find('\n', start)) {
        out.push_back(src.substr(start, nl - start));
        start = nl + 1;
    }
    out.push_back(src.substr(start));
    return out;
}

/// Every C++ source the engine builds from: `src/`, `include/orderbook/` and `tools/`.
inline std::vector<std::filesystem::path> engine_sources() {
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

inline std::string rel(const std::filesystem::path& p) {
    return std::filesystem::relative(p, std::filesystem::path(OB_SOURCE_DIR)).string();
}

}  // namespace ob::source_scan
