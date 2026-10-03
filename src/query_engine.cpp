// Feature: orderbook-dbengine — Query Engine parser and skeleton execution
#include "orderbook/query_engine.hpp"
#include "orderbook/logger.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <climits>
#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace ob {

// ─────────────────────────────────────────────────────────────────────────────
// Lexer / tokeniser
// ─────────────────────────────────────────────────────────────────────────────

namespace {

enum class TokKind {
    // Keywords
    KW_SELECT, KW_SUBSCRIBE, KW_FROM, KW_WHERE, KW_LIMIT,
    KW_AND, KW_BETWEEN, KW_AT,
    KW_GROUP, KW_BY, KW_TIME_BUCKET,
    // Aggregation function names
    KW_SUM, KW_AVG, KW_MIN, KW_MAX, KW_VWAP,
    KW_COUNT, KW_FIRST, KW_LAST,
    KW_SPREAD, KW_MID_PRICE,
    KW_IMBALANCE, KW_DEPTH, KW_DEPTH_RANGE, KW_CUMULATIVE_VOLUME,
    // Column names
    KW_PRICE, KW_QUANTITY, KW_ORDER_COUNT, KW_TIMESTAMP,
    KW_SEQUENCE_NUMBER, KW_SIDE, KW_LEVEL,
    // Literals / punctuation
    STRING_LIT,   // 'text'
    INT_LIT,      // ["-"] digit+
    STAR,         // *
    DOT,          // .
    COMMA,        // ,
    LPAREN,       // (
    RPAREN,       // )
    GE,           // >=
    LE,           // <=
    GT,           // >
    LT,           // <
    EQ,           // =
    END,          // end of input
    UNKNOWN,
};

struct Token {
    TokKind     kind;
    std::string text;   // raw text (for STRING_LIT / INT_LIT / UNKNOWN)
    int         line;
    int         col;
};

// Map keyword strings → TokKind (case-insensitive comparison done at lex time)
struct KwEntry { const char* name; TokKind kind; };
static const KwEntry KEYWORDS[] = {
    {"SELECT",            TokKind::KW_SELECT},
    {"SUBSCRIBE",         TokKind::KW_SUBSCRIBE},
    {"FROM",              TokKind::KW_FROM},
    {"WHERE",             TokKind::KW_WHERE},
    {"LIMIT",             TokKind::KW_LIMIT},
    {"AND",               TokKind::KW_AND},
    {"BETWEEN",           TokKind::KW_BETWEEN},
    {"AT",                TokKind::KW_AT},
    // A time bucket's (#44). Nothing unquoted could have been spelled so before - symbols and
    // exchanges are string literals and no column has these names - so nothing that parsed before
    // changes its meaning.
    {"GROUP",             TokKind::KW_GROUP},
    {"BY",                TokKind::KW_BY},
    {"TIME_BUCKET",       TokKind::KW_TIME_BUCKET},
    {"COUNT",             TokKind::KW_COUNT},
    {"FIRST",             TokKind::KW_FIRST},
    {"LAST",              TokKind::KW_LAST},
    {"SUM",               TokKind::KW_SUM},
    {"AVG",               TokKind::KW_AVG},
    {"MIN",               TokKind::KW_MIN},
    {"MAX",               TokKind::KW_MAX},
    {"VWAP",              TokKind::KW_VWAP},
    {"SPREAD",            TokKind::KW_SPREAD},
    {"MID_PRICE",         TokKind::KW_MID_PRICE},
    {"IMBALANCE",         TokKind::KW_IMBALANCE},
    {"DEPTH_RANGE",       TokKind::KW_DEPTH_RANGE},
    {"DEPTH",             TokKind::KW_DEPTH},
    {"CUMULATIVE_VOLUME", TokKind::KW_CUMULATIVE_VOLUME},
    {"price",             TokKind::KW_PRICE},
    {"quantity",          TokKind::KW_QUANTITY},
    {"order_count",       TokKind::KW_ORDER_COUNT},
    {"timestamp",         TokKind::KW_TIMESTAMP},
    // The same column under the name a client reads out of a response header. Accepting only
    // `timestamp` meant a name copied from the header was a syntax error, which is a poor answer
    // to someone doing exactly what the header invites.
    {"timestamp_ns",      TokKind::KW_TIMESTAMP},
    {"sequence_number",   TokKind::KW_SEQUENCE_NUMBER},
    {"side",              TokKind::KW_SIDE},
    {"level",             TokKind::KW_LEVEL},
};

// Case-insensitive string equality
static bool iequal(const std::string& a, const char* b) {
    size_t n = std::strlen(b);
    if (a.size() != n) return false;
    for (size_t i = 0; i < n; ++i) {
        if (std::tolower(static_cast<unsigned char>(a[i])) !=
            std::tolower(static_cast<unsigned char>(b[i]))) return false;
    }
    return true;
}

class Lexer {
public:
    explicit Lexer(std::string_view src)
        : src_(src), pos_(0), line_(1), col_(1) {}

    std::vector<Token> tokenise() {
        std::vector<Token> toks;
        while (pos_ < src_.size()) {
            skip_whitespace();
            if (pos_ >= src_.size()) break;

            int tline = line_, tcol = col_;
            char c = src_[pos_];

            if (c == '\'') {
                toks.push_back(lex_string(tline, tcol));
            } else if (c == '-' || std::isdigit(static_cast<unsigned char>(c))) {
                toks.push_back(lex_integer(tline, tcol));
            } else if (std::isalpha(static_cast<unsigned char>(c)) || c == '_') {
                toks.push_back(lex_word(tline, tcol));
            } else if (c == '*') {
                advance(); toks.push_back({TokKind::STAR, "*", tline, tcol});
            } else if (c == '.') {
                advance(); toks.push_back({TokKind::DOT, ".", tline, tcol});
            } else if (c == ',') {
                advance(); toks.push_back({TokKind::COMMA, ",", tline, tcol});
            } else if (c == '(') {
                advance(); toks.push_back({TokKind::LPAREN, "(", tline, tcol});
            } else if (c == ')') {
                advance(); toks.push_back({TokKind::RPAREN, ")", tline, tcol});
            } else if (c == '>') {
                advance();
                if (pos_ < src_.size() && src_[pos_] == '=') {
                    advance(); toks.push_back({TokKind::GE, ">=", tline, tcol});
                } else {
                    toks.push_back({TokKind::GT, ">", tline, tcol});
                }
            } else if (c == '<') {
                advance();
                if (pos_ < src_.size() && src_[pos_] == '=') {
                    advance(); toks.push_back({TokKind::LE, "<=", tline, tcol});
                } else {
                    toks.push_back({TokKind::LT, "<", tline, tcol});
                }
            } else if (c == '=') {
                advance(); toks.push_back({TokKind::EQ, "=", tline, tcol});
            } else {
                std::string u(1, c); advance();
                toks.push_back({TokKind::UNKNOWN, u, tline, tcol});
            }
        }
        toks.push_back({TokKind::END, "", line_, col_});
        return toks;
    }

private:
    std::string_view src_;
    size_t pos_;
    int line_, col_;

    char peek() const { return pos_ < src_.size() ? src_[pos_] : '\0'; }
    void advance() {
        if (pos_ < src_.size()) {
            if (src_[pos_] == '\n') { ++line_; col_ = 1; }
            else { ++col_; }
            ++pos_;
        }
    }

    void skip_whitespace() {
        while (pos_ < src_.size() && std::isspace(static_cast<unsigned char>(src_[pos_])))
            advance();
    }

    Token lex_string(int tline, int tcol) {
        advance(); // consume opening '
        std::string val;
        while (pos_ < src_.size() && src_[pos_] != '\'') {
            val += src_[pos_]; advance();
        }
        if (pos_ < src_.size()) advance(); // consume closing '
        return {TokKind::STRING_LIT, val, tline, tcol};
    }

    Token lex_integer(int tline, int tcol) {
        std::string val;
        if (src_[pos_] == '-') { val += '-'; advance(); }
        while (pos_ < src_.size() && std::isdigit(static_cast<unsigned char>(src_[pos_]))) {
            val += src_[pos_]; advance();
        }
        return {TokKind::INT_LIT, val, tline, tcol};
    }

    Token lex_word(int tline, int tcol) {
        std::string val;
        while (pos_ < src_.size() &&
               (std::isalnum(static_cast<unsigned char>(src_[pos_])) || src_[pos_] == '_')) {
            val += src_[pos_]; advance();
        }
        // Match keywords (case-insensitive for SQL keywords, exact for column names)
        for (const auto& kw : KEYWORDS) {
            if (iequal(val, kw.name)) return {kw.kind, val, tline, tcol};
        }
        return {TokKind::UNKNOWN, val, tline, tcol};
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Recursive-descent parser
// ─────────────────────────────────────────────────────────────────────────────

class Parser {
public:
    explicit Parser(std::vector<Token> tokens)
        : tokens_(std::move(tokens)), pos_(0) {}

    // Returns empty string on success, error message on failure.
    std::string parse(QueryAST& out) {
        auto err = parse_query(out);
        if (!err.empty()) return err;
        if (current().kind != TokKind::END) {
            return make_error("unexpected token '" + current().text + "' after query");
        }
        return {};
    }

private:
    std::vector<Token> tokens_;
    size_t pos_;

    const Token& current() const { return tokens_[pos_]; }
    const Token& peek(size_t offset = 1) const {
        size_t idx = pos_ + offset;
        if (idx >= tokens_.size()) return tokens_.back();
        return tokens_[idx];
    }

    Token consume() {
        Token t = tokens_[pos_];
        if (pos_ + 1 < tokens_.size()) ++pos_;
        return t;
    }

    bool at(TokKind k) const { return current().kind == k; }

    std::string make_error(const std::string& msg) const {
        const Token& t = current();
        return "Parse error at line " + std::to_string(t.line) +
               ", col " + std::to_string(t.col) + ": " + msg;
    }

    std::string make_error_at(const Token& t, const std::string& msg) const {
        return "Parse error at line " + std::to_string(t.line) +
               ", col " + std::to_string(t.col) + ": " + msg;
    }

    // ── Top-level ─────────────────────────────────────────────────────────────

    std::string parse_query(QueryAST& out) {
        if (at(TokKind::KW_SELECT)) {
            consume();
            out.type = QueryType::SELECT;
            return parse_select_body(out);
        }
        if (at(TokKind::KW_SUBSCRIBE)) {
            consume();
            out.type = QueryType::SUBSCRIBE;
            return parse_subscribe_body(out);
        }
        return make_error("expected SELECT or SUBSCRIBE");
    }

    // select_stmt body (after SELECT keyword consumed)
    std::string parse_select_body(QueryAST& out) {
        if (auto e = parse_select_list(out); !e.empty()) return e;
        if (auto e = expect_from(out); !e.empty()) return e;
        if (at(TokKind::KW_WHERE)) {
            consume();
            if (auto e = parse_where_clause(out); !e.empty()) return e;
        }
        if (at(TokKind::KW_GROUP)) {
            const Token group_tok = consume();
            if (auto e = parse_group_by(out, group_tok); !e.empty()) return e;
        }
        if (at(TokKind::KW_LIMIT)) {
            consume();
            if (auto e = parse_limit(out); !e.empty()) return e;
        }
        return {};
    }

    // ── GROUP BY TIME_BUCKET (#44) ────────────────────────────────────────────

    // group_by ::= "GROUP" "BY" "TIME_BUCKET" "(" integer unit ")", unit one of ns us ms s m h d.
    // The lexer gives `1m` as an integer and a word, so `1m` and `1 m` read the same.
    std::string parse_group_by(QueryAST& out, const Token& group_tok) {
        if (!at(TokKind::KW_BY)) return make_error("expected BY after GROUP");
        consume();
        if (!at(TokKind::KW_TIME_BUCKET)) return make_error("expected TIME_BUCKET(...) after GROUP BY");
        consume();
        if (!at(TokKind::LPAREN)) return make_error("expected '(' after TIME_BUCKET");
        consume();
        if (!at(TokKind::INT_LIT)) return make_error("expected the interval, e.g. TIME_BUCKET(1m)");
        const Token n_tok = consume();
        uint64_t n = 0;
        if (n_tok.text.empty() || n_tok.text[0] == '-' || !parse_decimal_u64(n_tok.text, n) || n == 0) {
            return make_error_at(n_tok, "a bucket's interval is a positive integer and a unit, got '" +
                                            n_tok.text + "'");
        }
        if (!at(TokKind::UNKNOWN)) {
            return make_error("expected the interval's unit - ns, us, ms, s, m, h or d - after " +
                              n_tok.text);
        }
        const Token unit_tok = consume();
        const uint64_t unit_ns = bucket_unit_ns(unit_tok.text);
        if (unit_ns == 0) {
            return make_error_at(unit_tok, "unknown unit '" + unit_tok.text +
                                               "' - a bucket's is ns, us, ms, s, m, h or d");
        }
        if (n > kMaxBucketNs / unit_ns) {
            return make_error_at(n_tok, "a bucket is at most 366 d, got " + n_tok.text + unit_tok.text);
        }
        if (!at(TokKind::RPAREN)) return make_error("expected ')' after the interval");
        consume();
        out.bucket_ns = n * unit_ns;
        if (out.snapshot_ts_ns.has_value()) {
            return make_error_at(group_tok, "AT reads the book at one moment and GROUP BY a stretch "
                                            "of rows - not both in one query");
        }

        // What the select list means is decided now that GROUP BY is known: the same function name
        // reads the live book without it (#200) and a bucket's rows with it.
        if (auto e = build_bucket_aggs(out, group_tok); !e.empty()) return e;
        OB_LOG_DEBUG("query", "GROUP BY time_bucket: %llu ns, %zu aggregate(s)",
                     static_cast<unsigned long long>(*out.bucket_ns), out.bucket_aggs.size());
        return {};
    }

    static uint64_t bucket_unit_ns(const std::string& unit) {
        static constexpr struct { const char* name; uint64_t ns; } kUnits[] = {
            {"ns", 1ULL}, {"us", 1'000ULL}, {"ms", 1'000'000ULL}, {"s", 1'000'000'000ULL},
            {"m", 60ULL * 1'000'000'000ULL}, {"h", 3'600ULL * 1'000'000'000ULL},
            {"d", 86'400ULL * 1'000'000'000ULL},
        };
        for (const auto& u : kUnits) {
            if (unit == u.name) return u.ns;   // exact: `M` is not minutes, and `S` is no unit
        }
        return 0;
    }

    static bool parse_decimal_u64(const std::string& text, uint64_t& out) {
        uint64_t v = 0;
        for (char c : text) {
            if (c < '0' || c > '9') return false;
            const uint64_t d = static_cast<uint64_t>(c - '0');
            if (v > (UINT64_MAX - d) / 10) return false;
            v = v * 10 + d;
        }
        out = v;
        return true;
    }

    /// The select list of a bucket query, as bucket aggregates: each item a function of the rows
    /// and the column it reads, and nothing else - a column, `*`, or a function of the live book is
    /// refused with its name (#44, requirement 1.3).
    std::string build_bucket_aggs(QueryAST& out, const Token& group_tok) {
        out.bucket_aggs.clear();
        for (const std::string& expr : out.select_exprs) {
            const std::string fn = expr.substr(0, expr.find('('));
            const std::string arg = expr.find('(') == std::string::npos
                ? std::string{}
                : expr.substr(expr.find('(') + 1, expr.size() - expr.find('(') - 2);
            if (expr.find('(') == std::string::npos) {
                return make_error_at(group_tok, "a GROUP BY query answers with aggregates of each "
                                                "bucket's rows, and '" + expr + "' is not one");
            }
            BucketAgg agg{BucketFn::Count, QueryColumn::TimestampNs, expr};
            if (fn == "COUNT") {
                if (arg != "*") return make_error_at(group_tok, "COUNT takes '*', got '" + expr + "'");
                agg.fn = BucketFn::Count;
            } else {
                static constexpr struct { const char* name; BucketFn fn; bool price, quantity; } kFns[] = {
                    {"FIRST", BucketFn::First, true, true}, {"LAST", BucketFn::Last, true, true},
                    {"MIN", BucketFn::Min, true, true},     {"MAX", BucketFn::Max, true, true},
                    {"SUM", BucketFn::Sum, false, true},    {"AVG", BucketFn::Avg, true, true},
                    {"VWAP", BucketFn::Vwap, true, false},
                };
                const auto* entry = std::find_if(std::begin(kFns), std::end(kFns),
                                                 [&](const auto& f) { return fn == f.name; });
                if (entry == std::end(kFns)) {
                    return make_error_at(group_tok, "'" + expr + "' aggregates the live book, not a "
                                                    "bucket's rows - a GROUP BY query takes COUNT, "
                                                    "FIRST, LAST, MIN, MAX, SUM, AVG and VWAP");
                }
                if (arg == "bid" || arg == "ask") {
                    return make_error_at(group_tok, "'" + expr + "' names a side, which a bucket's "
                                                    "rows are narrowed to with WHERE side = " +
                                                    std::string(arg == "bid" ? "0" : "1"));
                }
                const bool price = arg == "price", quantity = arg == "quantity";
                if (!(price && entry->price) && !(quantity && entry->quantity)) {
                    const std::string takes = entry->price && entry->quantity ? "price or quantity"
                                             : entry->price ? "price" : "quantity";
                    return make_error_at(group_tok, fn + " of a bucket takes " + takes + ", got '" +
                                                    expr + "'");
                }
                agg.fn = entry->fn;
                agg.column = price ? QueryColumn::Price : QueryColumn::Quantity;
            }
            out.bucket_aggs.push_back(std::move(agg));
        }
        return {};
    }

    // subscribe_stmt body (after SUBSCRIBE keyword consumed)
    std::string parse_subscribe_body(QueryAST& out) {
        if (auto e = parse_select_list(out); !e.empty()) return e;

        // A pushed row is still seven columns, and that is said out loud rather than left to be
        // discovered. It is not refused, and both of the obvious alternatives are worse:
        //
        //   * narrowing the push cannot be made safe, because a `PUSH` line has **no header**.
        //     A `SELECT` response describes itself, so a client that cannot read a narrowed one
        //     can say so; a subscriber reading field 2 as the price has nothing to check against
        //     and would simply read the wrong field. Announcing the columns in `OK SUB <id>` is
        //     the fix, and it is a protocol change rather than a line here.
        //   * refusing breaks a form that works and is in use - `SUBSCRIBE price FROM ...
        //     WHERE price BETWEEN ...` names the column it filters on, and three tests in this
        //     tree are written that way.
        //
        // So the list is accepted and the push stays wide, which is what it did before, with a
        // line saying so once per subscription. Filed rather than fixed.
        if (!out.projection.empty()) {
            OB_LOG_WARN("query",
                        "SUBSCRIBE named %zu column(s); pushed rows still carry all seven, "
                        "because a PUSH line has no header to describe a narrower one",
                        out.projection.size());
        }
        if (auto e = expect_from(out); !e.empty()) return e;
        if (at(TokKind::KW_WHERE)) {
            consume();
            if (auto e = parse_where_clause(out); !e.empty()) return e;
        }
        return {};
    }

    // ── SELECT list ───────────────────────────────────────────────────────────

    std::string parse_select_list(QueryAST& out) {
        std::string expr;
        std::optional<QueryColumn> column;

        for (bool first = true; first || at(TokKind::COMMA); first = false) {
            if (!first) consume();  // the comma
            column.reset();
            if (auto e = parse_select_item(expr, column); !e.empty()) return e;
            out.select_exprs.push_back(expr);
            if (column.has_value()) out.projection.push_back(*column);
        }

        // A list mixing columns with aggregates is **not** refused here. `execute()` already
        // refuses it with `AGG_WITH_COLUMNS`, naming the column, and a second refusal for one
        // rule is a guarantee that cannot be mutated separately - the first version of this work
        // added one anyway, on the assumption that mixing was accepted, and the existing test for
        // it is what said otherwise.

        OB_LOG_DEBUG("query", "Parsed select list: %zu item(s), projection %s",
                     out.select_exprs.size(),
                     out.projection.empty() ? "all columns" : "narrowed");
        return {};
    }

    /// `out_col` is filled only for a plain column name: `*` and aggregate calls leave it empty,
    /// which is how the caller tells the three apart without re-reading the text it just built.
    std::string parse_select_item(std::string& out_expr,
                                  std::optional<QueryColumn>& out_col) {
        if (at(TokKind::STAR)) {
            consume();
            out_expr = "*";
            return {};
        }
        // Try agg_call first (keyword followed by '(')
        if (is_agg_func(current().kind)) {
            return parse_agg_call(out_expr);
        }
        // column_name
        if (is_column_name(current().kind)) {
            out_col = column_of(current().kind);
            out_expr = consume().text;
            return {};
        }
        return make_error("expected column name, aggregation function, or '*'");
    }

    /// Which token names which column, indexed by `QueryColumn`.
    ///
    /// One table rather than a predicate and a switch that have to agree: `is_column_name()` and
    /// `column_of()` both read it, so a column the parser accepts and cannot name is not a state
    /// this file can be in. The `static_assert` is what makes adding an enumerator without a token
    /// a build error - a switch could not, because a switch over `TokKind` would have to name
    /// every keyword in the language to keep `-Wswitch` useful.
    static constexpr std::array<TokKind, kQueryColumnCount> kColumnTokens{
        TokKind::KW_TIMESTAMP,
        TokKind::KW_PRICE,
        TokKind::KW_QUANTITY,
        TokKind::KW_ORDER_COUNT,
        TokKind::KW_SIDE,
        TokKind::KW_LEVEL,
        TokKind::KW_SEQUENCE_NUMBER,
    };
    static_assert(kColumnTokens.size() == kQueryColumnCount,
                  "every column needs the token that names it");

    static QueryColumn column_of(TokKind k) {
        for (size_t i = 0; i < kColumnTokens.size(); ++i) {
            if (kColumnTokens[i] == k) return static_cast<QueryColumn>(i);
        }
        // Unreachable while `is_column_name()` guards every call site, and that predicate reads
        // the same table.
        return QueryColumn::TimestampNs;
    }

    bool is_column_name(TokKind k) const {
        for (TokKind t : kColumnTokens) {
            if (t == k) return true;
        }
        return false;
    }

    /// `bid` or `ask`, in any case: a book aggregate's side (#200).
    bool at_side_word() const {
        return at(TokKind::UNKNOWN) && (iequal(current().text, "bid") || iequal(current().text, "ask"));
    }
    std::string take_side_word() {
        std::string word = consume().text;
        for (auto& ch : word) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        return word;
    }

    bool is_agg_func(TokKind k) const {
        return k == TokKind::KW_COUNT || k == TokKind::KW_FIRST || k == TokKind::KW_LAST ||
               k == TokKind::KW_SUM || k == TokKind::KW_AVG ||
               k == TokKind::KW_MIN || k == TokKind::KW_MAX ||
               k == TokKind::KW_VWAP || k == TokKind::KW_SPREAD ||
               k == TokKind::KW_MID_PRICE || k == TokKind::KW_IMBALANCE ||
               k == TokKind::KW_DEPTH || k == TokKind::KW_DEPTH_RANGE ||
               k == TokKind::KW_CUMULATIVE_VOLUME;
    }

    // agg_call ::= agg_func "(" agg_arg ")"
    // Special forms: IMBALANCE(n), DEPTH(price), DEPTH_RANGE(p1,p2), CUMULATIVE_VOLUME(n)
    std::string parse_agg_call(std::string& out_expr) {
        Token func_tok = consume(); // consume function name
        std::string func_name = func_tok.text;
        // Normalise to uppercase for canonical form
        for (auto& ch : func_name) ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));

        if (!at(TokKind::LPAREN))
            return make_error_at(func_tok, "expected '(' after aggregation function '" + func_name + "'");
        consume(); // consume '('

        std::string inner;
        // The side a function of one side of the book reads, `bid` or `ask` in any case, first in
        // its argument (#200), written lower case into the expression the answer names. Not
        // keywords: nothing else in the language takes a side by name.
        std::string side;
        if (func_tok.kind == TokKind::KW_CUMULATIVE_VOLUME || func_tok.kind == TokKind::KW_DEPTH ||
            func_tok.kind == TokKind::KW_DEPTH_RANGE) {
            if (at_side_word()) {
                side = take_side_word();
                if (!at(TokKind::COMMA))
                    return make_error("expected ',' after the side in " + func_name + "(...)");
                consume();
            }
        }
        const std::string prefix = side.empty() ? std::string{} : side + ", ";
        if (func_tok.kind == TokKind::KW_IMBALANCE ||
            func_tok.kind == TokKind::KW_CUMULATIVE_VOLUME) {
            // IMBALANCE(integer_literal) / CUMULATIVE_VOLUME([side,] integer_literal)
            if (!at(TokKind::INT_LIT))
                return make_error("expected integer literal in " + func_name + "(...)");
            inner = prefix + consume().text;
        } else if (func_tok.kind == TokKind::KW_DEPTH) {
            // DEPTH([side,] price_expr) — price_expr is an integer literal
            if (!at(TokKind::INT_LIT))
                return make_error("expected price expression in DEPTH(...)");
            inner = prefix + consume().text;
        } else if (func_tok.kind == TokKind::KW_DEPTH_RANGE) {
            // DEPTH_RANGE([side,] price_expr, price_expr)
            if (!at(TokKind::INT_LIT))
                return make_error("expected first price expression in DEPTH_RANGE(...)");
            std::string p1 = consume().text;
            if (!at(TokKind::COMMA))
                return make_error("expected ',' in DEPTH_RANGE(...)");
            consume();
            if (!at(TokKind::INT_LIT))
                return make_error("expected second price expression in DEPTH_RANGE(...)");
            std::string p2 = consume().text;
            inner = prefix + p1 + ", " + p2;
        } else {
            // SUM, AVG, MIN, MAX, VWAP: agg_arg ::= side | column_name | "*", the side since #200 and
            // the other two refused in execute() with the spelling that names one. SPREAD and
            // MID_PRICE: "*".
            if (at(TokKind::STAR)) {
                inner = "*"; consume();
            } else if (at_side_word()) {
                inner = take_side_word();
            } else if (is_column_name(current().kind)) {
                inner = consume().text;
            } else {
                return make_error("expected bid, ask, a column name or '*' in " + func_name + "(...)");
            }
        }

        if (!at(TokKind::RPAREN))
            return make_error("expected ')' after " + func_name + " arguments");
        consume(); // consume ')'

        out_expr = func_name + "(" + inner + ")";
        return {};
    }

    // ── FROM clause ───────────────────────────────────────────────────────────

    std::string expect_from(QueryAST& out) {
        if (!at(TokKind::KW_FROM))
            return make_error("expected FROM keyword");
        consume();
        return parse_symbol_expr(out);
    }

    // symbol_expr ::= string_literal "." string_literal
    std::string parse_symbol_expr(QueryAST& out) {
        if (!at(TokKind::STRING_LIT))
            return make_error("expected symbol string literal (e.g. 'AAPL')");
        out.symbol = consume().text;

        if (!at(TokKind::DOT))
            return make_error("expected '.' between symbol and exchange");
        consume();

        if (!at(TokKind::STRING_LIT))
            return make_error("expected exchange string literal (e.g. 'NYSE')");
        out.exchange = consume().text;
        return {};
    }

    // ── WHERE clause ──────────────────────────────────────────────────────────

    std::string parse_where_clause(QueryAST& out) {
        if (auto e = parse_condition(out); !e.empty()) return e;
        while (at(TokKind::KW_AND)) {
            consume();
            if (auto e = parse_condition(out); !e.empty()) return e;
        }
        return {};
    }

    std::string parse_condition(QueryAST& out) {
        if (at(TokKind::KW_TIMESTAMP)) {
            consume();
            return parse_ts_condition(out);
        }
        if (at(TokKind::KW_PRICE)) {
            consume();
            return parse_price_condition(out);
        }
        if (at(TokKind::KW_SIDE)) {
            consume();
            return parse_narrow_condition<uint8_t>(out.side_lo, out.side_hi, "side");
        }
        if (at(TokKind::KW_LEVEL)) {
            consume();
            return parse_narrow_condition<uint16_t>(out.level_lo, out.level_hi, "level");
        }
        if (at(TokKind::KW_AT)) {
            const Token at_tok = consume();
            return parse_snapshot_condition(out, at_tok);
        }
        return make_error("expected 'timestamp', 'price', 'side', 'level' or 'AT' condition");
    }

    /// `side` and `level` (#200): #199's rules over a column narrower than its literals, which are
    /// read as a uint64 and refused past the column's type rather than wrapped into it - `side = 256`
    /// is a mistake to say, not the bid side.
    template <typename T>
    std::string parse_narrow_condition(std::optional<T>& lo, std::optional<T>& hi, const char* name) {
        const auto read = [&](T& v) -> std::string {
            const Token tok = current();
            uint64_t wide = 0;
            if (auto e = parse_uint64(wide); !e.empty()) return e;
            if (wide > std::numeric_limits<T>::max()) {
                return make_error_at(tok, std::string("value out of range for ") + name + ": " +
                                              tok.text + " (at most " +
                                              std::to_string(std::numeric_limits<T>::max()) + ")");
            }
            v = static_cast<T>(wide);
            return {};
        };
        if (at(TokKind::KW_BETWEEN)) {
            consume();
            T a{};
            T b{};
            if (auto e = read(a); !e.empty()) return e;
            if (!at(TokKind::KW_AND)) return make_error("expected AND in BETWEEN clause");
            consume();
            if (auto e = read(b); !e.empty()) return e;
            narrow<T>(lo, hi, a, b);
        } else {
            const TokKind op = current().kind;
            if (!is_comparison_op(op)) {
                return make_error(std::string("expected comparison operator or BETWEEN after '") + name + "'");
            }
            consume();
            T v{};
            if (auto e = read(v); !e.empty()) return e;
            apply_comparison<T>(lo, hi, op, v);
        }
        OB_LOG_DEBUG("query", "%s condition: now [%s, %s]", name, bound_text(lo).c_str(),
                     bound_text(hi).c_str());
        return {};
    }

    // ts_condition (after "timestamp" consumed)
    std::string parse_ts_condition(QueryAST& out) {
        if (at(TokKind::KW_BETWEEN)) {
            consume();
            uint64_t lo = 0, hi = 0;
            if (auto e = parse_uint64(lo); !e.empty()) return e;
            if (!at(TokKind::KW_AND))
                return make_error("expected AND in BETWEEN clause");
            consume();
            if (auto e = parse_uint64(hi); !e.empty()) return e;
            narrow<uint64_t>(out.ts_start_ns, out.ts_end_ns, lo, hi);
            OB_LOG_DEBUG("query", "timestamp condition: now [%s, %s]",
                         bound_text(out.ts_start_ns).c_str(), bound_text(out.ts_end_ns).c_str());
            return {};
        }
        // comparison operator
        TokKind op = current().kind;
        if (!is_comparison_op(op))
            return make_error("expected comparison operator or BETWEEN after 'timestamp'");
        consume();
        uint64_t val = 0;
        if (auto e = parse_uint64(val); !e.empty()) return e;
        apply_comparison<uint64_t>(out.ts_start_ns, out.ts_end_ns, op, val);
        OB_LOG_DEBUG("query", "timestamp condition: now [%s, %s]",
                     bound_text(out.ts_start_ns).c_str(), bound_text(out.ts_end_ns).c_str());
        return {};
    }

    // price_condition (after "price" consumed)
    std::string parse_price_condition(QueryAST& out) {
        if (at(TokKind::KW_BETWEEN)) {
            consume();
            int64_t lo = 0, hi = 0;
            if (auto e = parse_int64(lo); !e.empty()) return e;
            if (!at(TokKind::KW_AND))
                return make_error("expected AND in BETWEEN clause");
            consume();
            if (auto e = parse_int64(hi); !e.empty()) return e;
            narrow<int64_t>(out.price_lo, out.price_hi, lo, hi);
            OB_LOG_DEBUG("query", "price condition: now [%s, %s]",
                         bound_text(out.price_lo).c_str(), bound_text(out.price_hi).c_str());
            return {};
        }
        TokKind op = current().kind;
        if (!is_comparison_op(op))
            return make_error("expected comparison operator or BETWEEN after 'price'");
        consume();
        int64_t val = 0;
        if (auto e = parse_int64(val); !e.empty()) return e;
        apply_comparison<int64_t>(out.price_lo, out.price_hi, op, val);
        OB_LOG_DEBUG("query", "price condition: now [%s, %s]",
                     bound_text(out.price_lo).c_str(), bound_text(out.price_hi).c_str());
        return {};
    }

    // snapshot_condition (after "AT" consumed)
    std::string parse_snapshot_condition(QueryAST& out, const Token& at_tok) {
        // A subscription is what is written from now on, and `AT` names a moment of the stored
        // book: there is nothing for it to mean there. It was accepted and dropped, so `SUBSCRIBE
        // ... WHERE AT t` subscribed to every row (#199).
        if (out.type == QueryType::SUBSCRIBE) {
            return make_error_at(at_tok, "AT names a moment of the stored book; a subscription "
                                         "pushes what is written from now on");
        }
        // One book, at one moment: a second `AT` replaced the first without a word (#199).
        if (out.snapshot_ts_ns.has_value()) {
            return make_error_at(at_tok, "a second AT; a snapshot is the book at one moment");
        }
        uint64_t ts = 0;
        if (auto e = parse_uint64(ts); !e.empty()) return e;
        out.snapshot_ts_ns = ts;
        out.type = QueryType::SNAPSHOT;
        return {};
    }

    // ── LIMIT clause ──────────────────────────────────────────────────────────

    std::string parse_limit(QueryAST& out) {
        uint64_t n = 0;
        if (auto e = parse_uint64(n); !e.empty()) return e;
        out.limit = n;
        return {};
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    bool is_comparison_op(TokKind k) const {
        return k == TokKind::GE || k == TokKind::LE ||
               k == TokKind::GT || k == TokKind::LT || k == TokKind::EQ;
    }

    /// Narrow a column's range - inclusive at both ends, an absent end unbounded - by one more
    /// condition's (#199). `AND` is an intersection: a condition on a column narrows what the
    /// conditions before it allowed, where it used to replace it, so `price >= 300 AND price >= 100`
    /// answered every price from 100.
    ///
    /// A range nothing satisfies stays a lower bound above the upper one. Every reader of the AST
    /// answers that with no rows - the store's scan and the price filters compare each row with
    /// both ends - and `format()` writes it back as the same `BETWEEN`.
    template <typename T>
    static void narrow(std::optional<T>& lo, std::optional<T>& hi,
                       std::optional<T> at_least, std::optional<T> at_most) {
        if (at_least.has_value()) lo = lo.has_value() ? std::max(*lo, *at_least) : *at_least;
        if (at_most.has_value())  hi = hi.has_value() ? std::min(*hi, *at_most) : *at_most;
    }

    /// One comparison as the inclusive range it allows, narrowing the column's (#199).
    ///
    /// Each comparison used to set one end and nothing else: `=` set the lower bound, so
    /// `price = 200` answered every price from 200 up, and `>` and `<` set the bound itself, so
    /// each kept the one value it excludes - `timestamp > t`, the way to ask for what came after
    /// the last row read, handed that row back again. A strict comparison is the inclusive one a
    /// step inside; past the end of the type there is no step, and nothing is greater than the
    /// largest value or less than the smallest, so that is the empty range.
    template <typename T>
    static void apply_comparison(std::optional<T>& lo, std::optional<T>& hi, TokKind op, T v) {
        constexpr T kMin = std::numeric_limits<T>::min();
        constexpr T kMax = std::numeric_limits<T>::max();
        switch (op) {
            case TokKind::EQ: narrow<T>(lo, hi, v, v); break;
            case TokKind::GE: narrow<T>(lo, hi, v, std::nullopt); break;
            case TokKind::LE: narrow<T>(lo, hi, std::nullopt, v); break;
            // Cast back, because for `side` and `level` (#200) the step is taken in `int`.
            case TokKind::GT:
                if (v == kMax) narrow<T>(lo, hi, kMax, static_cast<T>(kMax - 1));
                else           narrow<T>(lo, hi, static_cast<T>(v + 1), std::nullopt);
                break;
            case TokKind::LT:
                if (v == kMin) narrow<T>(lo, hi, static_cast<T>(kMin + 1), kMin);
                else           narrow<T>(lo, hi, std::nullopt, static_cast<T>(v - 1));
                break;
            default: break;
        }
    }

    template <typename T>
    static std::string bound_text(const std::optional<T>& b) {
        return b.has_value() ? std::to_string(*b) : std::string{"-"};
    }

    std::string parse_uint64(uint64_t& out) {
        if (!at(TokKind::INT_LIT))
            return make_error("expected integer literal");
        const std::string& s = current().text;
        if (!s.empty() && s[0] == '-')
            return make_error("expected non-negative integer literal, got '" + s + "'");
        uint64_t v = 0;
        auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
        if (ec != std::errc{} || ptr != s.data() + s.size())
            return make_error("invalid integer literal '" + s + "'");
        out = v;
        consume();
        return {};
    }

    std::string parse_int64(int64_t& out) {
        if (!at(TokKind::INT_LIT))
            return make_error("expected integer literal");
        const std::string& s = current().text;
        int64_t v = 0;
        auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
        if (ec != std::errc{} || ptr != s.data() + s.size())
            return make_error("invalid integer literal '" + s + "'");
        out = v;
        consume();
        return {};
    }
};

} // anonymous namespace

// ─────────────────────────────────────────────────────────────────────────────
// QueryEngine implementation
// ─────────────────────────────────────────────────────────────────────────────

QueryEngine::QueryEngine(const ColumnarStore& store,
                         LiveBufferLookup live_buffer,
                         const AggregationEngine& agg)
    : store_(store), live_buffer_(std::move(live_buffer)), agg_(agg) {}

QueryEngine::~QueryEngine() = default;

std::string QueryEngine::parse(std::string_view sql, QueryAST& out) {
    Lexer lexer(sql);
    auto tokens = lexer.tokenise();
    Parser parser(std::move(tokens));
    return parser.parse(out);
}

// ─────────────────────────────────────────────────────────────────────────────
// execute() helpers
// ─────────────────────────────────────────────────────────────────────────────

namespace {

// Check whether a select_expr string is an aggregation call.
/// Whether a row is inside every condition of `ast` (#199, #200): its time, price, side and level,
/// each a range inclusive at both ends. One predicate for the row scan, a snapshot's levels and a
/// subscription's pushes, which kept three copies of the time and price checks between them.
static bool row_allowed(const QueryAST& ast, const SnapshotRow& row) {
    const auto in = [](auto v, const auto& lo, const auto& hi) {
        return (!lo.has_value() || v >= *lo) && (!hi.has_value() || v <= *hi);
    };
    return in(row.timestamp_ns, ast.ts_start_ns, ast.ts_end_ns) &&
           in(row.price, ast.price_lo, ast.price_hi) &&
           in(row.side, ast.side_lo, ast.side_hi) &&
           in(row.level_index, ast.level_lo, ast.level_hi);
}

/// A book aggregate's argument split (#200): the side it names first, if any - `bid` or `ask`,
/// already lower case from the parser - and the numbers after it, as text. `VWAP(bid)` is
/// {"bid", {}}, `DEPTH_RANGE(ask, 1, 2)` {"ask", {"1", "2"}}, `DEPTH(100)` {"", {"100"}}.
struct BookArgs {
    std::string side;
    std::vector<std::string> numbers;
};

static BookArgs book_args(const std::string& farg) {
    BookArgs out;
    size_t at = 0;
    while (at <= farg.size()) {
        const size_t comma = farg.find(',', at);
        std::string part = farg.substr(at, comma == std::string::npos ? std::string::npos : comma - at);
        const size_t first = part.find_first_not_of(' ');
        part = first == std::string::npos ? std::string{} : part.substr(first);
        if (out.numbers.empty() && out.side.empty() && (part == "bid" || part == "ask")) {
            out.side = part;
        } else if (!part.empty()) {
            out.numbers.push_back(part);
        }
        if (comma == std::string::npos) break;
        at = comma + 1;
    }
    return out;
}

static bool is_agg_expr(const std::string& expr) {
    // Agg calls contain a '(' character; plain column names do not.
    return expr.find('(') != std::string::npos;
}

// Extract the function name from an agg expression like "VWAP(price)" → "VWAP".
static std::string agg_func_name(const std::string& expr) {
    auto pos = expr.find('(');
    if (pos == std::string::npos) return expr;
    return expr.substr(0, pos);
}

// Extract the argument string from an agg expression like "IMBALANCE(10)" → "10".
static std::string agg_func_arg(const std::string& expr) {
    auto lp = expr.find('(');
    auto rp = expr.rfind(')');
    if (lp == std::string::npos || rp == std::string::npos || rp <= lp + 1) return "";
    return expr.substr(lp + 1, rp - lp - 1);
}

// Parse a uint32 from a string, returning 0 on failure.
static uint32_t parse_u32(const std::string& s) {
    if (s.empty()) return 0;
    uint32_t v = 0;
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    return (ec == std::errc{}) ? v : 0;
}

// Parse an int64 from a string, returning 0 on failure.
static int64_t parse_i64(const std::string& s) {
    if (s.empty()) return 0;
    int64_t v = 0;
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    return (ec == std::errc{}) ? v : 0;
}

/// Parse an integer argument, reporting failure instead of substituting zero.
///
/// Leading whitespace is skipped because the parser reconstructs the expression
/// text with ", " between arguments, so DEPTH_RANGE's second bound always arrives
/// as " 101000". std::from_chars refuses that, parse_i64() turned the failure into
/// 0, and the range [lo, 0] is always empty — which is why DEPTH_RANGE could only
/// ever answer NULL.
static bool parse_i64_strict(const std::string& s, int64_t& out) {
    size_t begin = s.find_first_not_of(" \t");
    if (begin == std::string::npos) return false;
    size_t end = s.find_last_not_of(" \t") + 1;
    auto [ptr, ec] = std::from_chars(s.data() + begin, s.data() + end, out);
    return ec == std::errc{} && ptr == s.data() + end;
}

} // anonymous namespace

// ── read_book ─────────────────────────────────────────────────────────────────

std::string QueryEngine::read_book(const std::string& symbol, const std::string& exchange,
                                   uint32_t depth, RowCallback cb) {
    const std::string key = symbol + "." + exchange;
    // Resolved to an owning handle and held for the whole answer. A raw pointer here is #92: the
    // engine owns these buffers and a snapshot install replaces the store, so a query still
    // reading through a resolved pointer reads freed memory - measured then as
    // `heap-use-after-free` in 3 of 3 ASan runs.
    std::shared_ptr<SoABuffer> buf = live_buffer_(key);
    if (buf == nullptr) {
        OB_LOG_DEBUG("query_engine", "BOOK: no live buffer: symbol=%s exchange=%s",
                     symbol.c_str(), exchange.c_str());
        return "OB_ERR_NOT_FOUND: symbol '" + symbol + "' exchange '" + exchange +
               "' not found in live buffers";
    }

    // One snapshot for the whole answer. Reading per side while emitting rows would compose the
    // answer from two moments separated by the formatting of up to two thousand levels, and a
    // book assembled that way can show a crossed spread the market never had.
    SoASide bid, ask;
    read_snapshot(*buf, bid, ask);

    const uint64_t ts  = buf->last_timestamp_ns;
    const uint64_t seq = buf->sequence_number.load(std::memory_order_relaxed);

    const auto emit = [&](const SoASide& side, uint8_t side_code) {
        const uint32_t have = side.depth;
        const uint32_t want = (depth == 0) ? have : std::min(depth, have);
        for (uint32_t i = 0; i < want; ++i) {
            QueryResult r{};
            // The same pair on every row: these two are properties of the buffer, not of a level,
            // so they say *as of which update* this book is rather than when a level changed.
            r.timestamp_ns    = ts;
            r.sequence_number = seq;
            r.price           = side.prices[i];
            r.quantity        = side.quantities[i];
            r.order_count     = side.order_counts[i];
            r.side            = side_code;
            r.level           = static_cast<uint16_t>(i);
            cb(r);
        }
        return want;
    };

    const uint32_t bids = emit(bid, 0);
    const uint32_t asks = emit(ask, 1);
    OB_LOG_DEBUG("query_engine",
                 "BOOK: symbol=%s exchange=%s depth_asked=%u bids=%u asks=%u seq=%llu",
                 symbol.c_str(), exchange.c_str(), depth, bids, asks,
                 static_cast<unsigned long long>(seq));
    return {};
}

std::string QueryEngine::execute(std::string_view sql, RowCallback cb) {
    // Discarded: every caller that does not need the shape - thirty-odd of them, nearly all
    // tests - keeps the two-argument spelling rather than growing an argument it ignores.
    QueryShape unused;
    return execute(sql, std::move(cb), unused);
}

std::string QueryEngine::execute(std::string_view sql, RowCallback cb, QueryShape& shape) {
    QueryAST ast;
    if (auto err = parse(sql, ast); !err.empty()) return err;

    // SUBSCRIBE via execute() is not supported
    if (ast.type == QueryType::SUBSCRIBE) {
        return "OB_ERR_PARSE: use subscribe() for streaming queries";
    }

    // ── Symbol/exchange existence check ──────────────────────────────────────
    // Resolve the live buffer once, through Engine's lock (see LiveBufferLookup), then use the
    // pointer for the rest of the query. It used to be two unsynchronised reads of Engine's map -
    // `count()` here and `find()` in the aggregation branch - racing every thread that creates a
    // symbol.
    const std::string live_key = ast.symbol + "." + ast.exchange;
    // Held for the whole query, not dereferenced and dropped: this handle is what keeps the
    // buffer alive if a snapshot install clears `Engine::buffers_` mid-query (#92). An install
    // that lands here now means the query finishes against the contents it started with, which is
    // the same answer any query gets when a write lands after it began.
    const std::shared_ptr<SoABuffer> live_buf = live_buffer_ ? live_buffer_(live_key) : nullptr;
    bool found_in_live = (live_buf != nullptr);
    bool found_in_store = false;
    if (!found_in_live) {
        // A lookup, where it was a copy of every segment of every symbol searched for one (#165).
        found_in_store = store_.holds(ast.symbol, ast.exchange);
    }
    if (!found_in_live && !found_in_store) {
        return "OB_ERR_NOT_FOUND: symbol '" + ast.symbol +
               "' exchange '" + ast.exchange + "' not found";
    }

    // ── GROUP BY TIME_BUCKET (#44) ────────────────────────────────────────────
    // Before the live book's checks: these aggregates read rows, so a condition on time, price,
    // side or level narrows what they read rather than being refused as it is beside a function
    // of the book.
    if (ast.bucket_ns.has_value()) return execute_buckets(ast, cb, shape);

    // ── Determine if any select_expr is an aggregation call ──────────────────
    bool has_agg = false;
    for (const auto& expr : ast.select_exprs) {
        if (is_agg_expr(expr)) { has_agg = true; break; }
    }

    // ── Validate aggregation function names ──────────────────────────────────
    static const char* KNOWN_AGG[] = {
        "SUM", "AVG", "MIN", "MAX", "VWAP",
        "SPREAD", "MID_PRICE", "IMBALANCE",
        "DEPTH", "DEPTH_RANGE", "CUMULATIVE_VOLUME", nullptr
    };
    if (has_agg) {
        for (size_t i = 0; i < ast.select_exprs.size(); ++i) {
            const auto& expr = ast.select_exprs[i];
            if (!is_agg_expr(expr)) continue;
            std::string fname = agg_func_name(expr);
            bool known = false;
            for (const char** p = KNOWN_AGG; *p; ++p) {
                if (fname == *p) { known = true; break; }
            }
            if (!known) {
                if (fname == "COUNT" || fname == "FIRST" || fname == "LAST") {
                    // Functions of a time bucket's rows (#44): the live book has no rows to count.
                    OB_LOG_WARN("query", "Rejecting %s: it aggregates a time bucket and the query "
                                         "has no GROUP BY", expr.c_str());
                    return "AGG_NEEDS_BUCKET: " + fname + " aggregates the rows of a time bucket; "
                           "add GROUP BY TIME_BUCKET(<interval>) to '" + expr + "'";
                }
                return "OB_ERR_PARSE: undefined aggregation function '" +
                       fname + "' at position " + std::to_string(i);
            }

            // The column argument has to name what the function actually
            // aggregates. The dispatcher below calls sum_qty() for SUM and
            // avg_price() for AVG regardless of the argument, so SUM(price)
            // returned a quantity labelled SUM(price) and AVG(quantity) returned a
            // price. An argument that would be ignored is an error, not decoration.
            const std::string farg = agg_func_arg(expr);
            // The one-sided functions name the side they read (#200): every one of them read the
            // bids whatever the query said, and nothing could ask for the asks. The forms that used
            // to mean the bids are refused with the spelling that says so, rather than answered with
            // a number that quietly changed meaning.
            const auto needs_side = [&]() {
                OB_LOG_WARN("query", "Rejecting %s: %s reads one side of the book and names none",
                            expr.c_str(), fname.c_str());
                return "AGG_NEEDS_SIDE: " + fname + " reads one side of the book: write " + fname +
                       (fname == "CUMULATIVE_VOLUME" ? "(bid, n) or " + fname + "(ask, n)"
                                                     : "(bid) or " + fname + "(ask)") +
                       ", not '" + expr + "'";
            };
            const bool names_a_side = farg == "bid" || farg == "ask";
            if (fname == "SUM") {
                if (farg == "*" || farg == "quantity") return needs_side();
                if (!names_a_side) {
                    OB_LOG_WARN("query", "Rejecting %s: SUM aggregates quantity", expr.c_str());
                    return "AGG_BAD_ARGUMENT: SUM aggregates the quantity of a side; write SUM(bid) "
                           "or SUM(ask), not '" + expr + "'";
                }
            } else if (fname == "AVG" || fname == "MIN" || fname == "MAX" ||
                       fname == "VWAP") {
                if (farg == "*" || farg == "price") return needs_side();
                if (!names_a_side) {
                    OB_LOG_WARN("query", "Rejecting %s: %s aggregates price",
                                expr.c_str(), fname.c_str());
                    return "AGG_BAD_ARGUMENT: " + fname + " aggregates the prices of a side; write " +
                           fname + "(bid) or " + fname + "(ask), not '" + expr + "'";
                }
            } else if (fname == "CUMULATIVE_VOLUME") {
                if (book_args(farg).side.empty()) return needs_side();
            } else if (fname == "SPREAD" || fname == "MID_PRICE") {
                if (farg != "*") {
                    OB_LOG_WARN("query", "Rejecting %s: %s takes no argument",
                                expr.c_str(), fname.c_str());
                    return "AGG_BAD_ARGUMENT: " + fname + " reads both sides of the book and "
                           "takes no argument; write " + fname + "(*), not '" + expr + "'";
                }
            }
        }
    }

    // ── Reject what the aggregation path would silently ignore ───────────────
    //
    // Aggregates are computed over the live SoA book, so a row filter has nothing
    // to act on. Accepting one and ignoring it is the same defect as dropping the
    // results themselves: the client is told OK and gets an answer to a different
    // question than the one it asked.
    if (has_agg) {
        for (const auto& expr : ast.select_exprs) {
            if (is_agg_expr(expr)) continue;
            OB_LOG_WARN("query",
                        "Rejecting mixed aggregate/column SELECT: symbol=%s exchange=%s column=%s",
                        ast.symbol.c_str(), ast.exchange.c_str(), expr.c_str());
            return "AGG_WITH_COLUMNS: aggregate and plain columns cannot be mixed: '" +
                   expr + "'";
        }
        if (ast.ts_start_ns.has_value() || ast.ts_end_ns.has_value()) {
            OB_LOG_WARN("query",
                        "Rejecting aggregate with timestamp filter: symbol=%s exchange=%s",
                        ast.symbol.c_str(), ast.exchange.c_str());
            return "AGG_TIME_FILTER: aggregates are computed over the live book; "
                   "a timestamp filter is not supported";
        }
        if (ast.price_lo.has_value() || ast.price_hi.has_value()) {
            OB_LOG_WARN("query",
                        "Rejecting aggregate with price filter: symbol=%s exchange=%s",
                        ast.symbol.c_str(), ast.exchange.c_str());
            return "AGG_PRICE_FILTER: aggregates are computed over the whole live book; "
                   "a price filter is not supported (use DEPTH_RANGE(lo, hi))";
        }
        // A level range beside an aggregate (#200): the functions that read part of a side say how
        // much of it in their own argument (`CUMULATIVE_VOLUME(n)`, `IMBALANCE(n)`), and a second
        // answer to that question would be one of them ignored.
        if (ast.level_lo.has_value() || ast.level_hi.has_value()) {
            OB_LOG_WARN("query", "Rejecting aggregate with a level condition: symbol=%s exchange=%s",
                        ast.symbol.c_str(), ast.exchange.c_str());
            return "AGG_LEVEL_FILTER: aggregates read the book by side and by their own arguments; "
                   "a level condition is not supported";
        }
        // A side condition beside an aggregate (#200): the side a function reads is its argument,
        // `VWAP(bid)`, so that one query can ask for both sides and the spread at once.
        if (ast.side_lo.has_value() || ast.side_hi.has_value()) {
            OB_LOG_WARN("query", "Rejecting aggregate with a side condition: symbol=%s exchange=%s",
                        ast.symbol.c_str(), ast.exchange.c_str());
            return "AGG_SIDE_FILTER: an aggregate names the side it reads in its argument - "
                   "VWAP(bid), DEPTH(ask, p) - not in a side condition";
        }
        // `AT` made the query a SNAPSHOT, whose branch below answers rows and never reads the
        // select list: `SELECT SPREAD(*) ... WHERE AT t` answered the book at t as rows (#199).
        if (ast.snapshot_ts_ns.has_value()) {
            OB_LOG_WARN("query", "Rejecting aggregate with AT: symbol=%s exchange=%s",
                        ast.symbol.c_str(), ast.exchange.c_str());
            return "AGG_TIME_FILTER: aggregates are computed over the live book; "
                   "AT is not supported";
        }
    }

    // ── SNAPSHOT query ────────────────────────────────────────────────────────
    if (ast.type == QueryType::SNAPSHOT) {
        // `AT` names the moment, so a timestamp condition beside it has nothing to mean, and it
        // was ignored: `AT t AND timestamp BETWEEN ...` answered the book at t whatever the range
        // said (#199). Refused, as the aggregates refuse theirs.
        if (ast.ts_start_ns.has_value() || ast.ts_end_ns.has_value()) {
            OB_LOG_WARN("query",
                        "Rejecting SNAPSHOT with a timestamp condition: symbol=%s exchange=%s",
                        ast.symbol.c_str(), ast.exchange.c_str());
            return "SNAPSHOT_TIME_FILTER: AT names the moment of the book; a timestamp "
                   "condition beside it is not supported";
        }
        uint64_t snap_ts = ast.snapshot_ts_ns.value_or(0);

        // A SNAPSHOT is a row query, and it answers the columns it names like any other. The shape
        // is what the wire formatter prints, and #139 assigned it below this branch's return - so
        // from #139 on a SNAPSHOT answered over the wire with an empty header and empty rows, while
        // local mode, which formats nothing, was unaffected (#167).
        shape.columns = ast.projection.empty() ? all_query_columns() : ast.projection;

        // The book at snap_ts: for each (side, level_index), the row with the latest timestamp at
        // or before it. It kept the last row a scan *delivered*, which is that only while rows
        // arrive in time order: a scan hands out segments by start and each segment's rows in the
        // order they were appended, so a correction for an earlier instant that arrived later - a
        // client's own event times (#105), a mesh peer's backlog - replaced the book it came after
        // (#168). A tie on the timestamp keeps the later one delivered, which is what every row got
        // before. Ordered by side, then level - bids first, as BOOK answers - where a hash map made
        // the order, and with it what a LIMIT kept, a property of the hash.
        // Key: (side << 16) | level_index.
        std::map<uint32_t, SnapshotRow> state;

        // Every column is read whatever the select list says: the key is the side and the level,
        // and the choice between two rows is their timestamps.
        store_.scan(0, snap_ts, ast.symbol, ast.exchange, ColumnSet::all(),
                    [&](const SnapshotRow& row) {
                        const uint32_t key = (static_cast<uint32_t>(row.side) << 16) |
                                             static_cast<uint32_t>(row.level_index);
                        auto [it, inserted] = state.try_emplace(key, row);
                        if (!inserted && row.timestamp_ns >= it->second.timestamp_ns) {
                            it->second = row;
                        }
                    });
        OB_LOG_DEBUG("query_engine", "SNAPSHOT: symbol=%s exchange=%s at=%llu levels=%zu",
                     ast.symbol.c_str(), ast.exchange.c_str(),
                     static_cast<unsigned long long>(snap_ts), state.size());

        // A price condition keeps the levels of that book priced within it, and was ignored (#199).
        // It is applied to the book, after the latest row of each level is chosen, not to the rows
        // before: filtering those would answer a level with an older price of its own whenever its
        // latest one is out of range.
        uint64_t count = 0;
        uint64_t priced_out = 0;
        uint64_t lim = ast.limit.value_or(UINT64_MAX);
        for (auto& [k, row] : state) {
            if (count >= lim) break;
            // And its side and level conditions the same way (#200): the bids of the book at t.
            if (!row_allowed(ast, row)) {
                ++priced_out;
                continue;
            }
            QueryResult qr{};
            qr.timestamp_ns    = row.timestamp_ns;
            qr.sequence_number = row.sequence_number;
            qr.price           = row.price;
            qr.quantity        = row.quantity;
            qr.order_count     = row.order_count;
            qr.side            = row.side;
            qr.level           = row.level_index;
            cb(qr);
            ++count;
        }
        if (priced_out != 0) {
            OB_LOG_DEBUG("query_engine", "SNAPSHOT: %llu level(s) outside the conditions",
                         static_cast<unsigned long long>(priced_out));
        }
        return {};
    }

    // ── SELECT with aggregation ───────────────────────────────────────────────
    if (has_agg) {
        if (live_buf == nullptr) {
            return "OB_ERR_NOT_FOUND: symbol '" + ast.symbol +
                   "' exchange '" + ast.exchange + "' not found in live buffers";
        }
        const SoABuffer* buf = live_buf.get();

        // Read a consistent snapshot of both sides
        SoASide snap_bid, snap_ask;
        read_snapshot(*buf, snap_bid, snap_ask);


        QueryResult qr{};
        qr.timestamp_ns    = buf->last_timestamp_ns;
        qr.sequence_number = buf->sequence_number.load(std::memory_order_relaxed);

        for (const auto& expr : ast.select_exprs) {
            if (!is_agg_expr(expr)) continue;
            std::string fname = agg_func_name(expr);
            std::string farg  = agg_func_arg(expr);
            // The side a function names first in its argument (#200), validated above for those
            // that must name one; `DEPTH` and `DEPTH_RANGE` read both sides when theirs names none.
            const BookArgs args = book_args(farg);
            const SoASide& side = args.side == "ask" ? snap_ask : snap_bid;
            const bool bids = args.side.empty() || args.side == "bid";
            const bool asks = args.side.empty() || args.side == "ask";
            OB_LOG_DEBUG("query", "%s reads %s", expr.c_str(),
                         args.side.empty() ? "both sides" : (args.side + "s").c_str());

            AggResult res{0, true};

            if (fname == "SUM") {
                res = agg_.sum_qty(side, side.depth);
            } else if (fname == "AVG") {
                res = agg_.avg_price(side, side.depth);
            } else if (fname == "MIN") {
                res = agg_.min_price(side, side.depth);
            } else if (fname == "MAX") {
                res = agg_.max_price(side, side.depth);
            } else if (fname == "VWAP") {
                res = agg_.vwap(side, side.depth);
            } else if (fname == "SPREAD") {
                res = agg_.spread(snap_bid, snap_ask);
            } else if (fname == "MID_PRICE") {
                res = agg_.mid_price(snap_bid, snap_ask);
            } else if (fname == "IMBALANCE") {
                uint32_t n = parse_u32(farg);
                if (n == 0) n = snap_bid.depth;   // as before #200: two-sided, and its default is unchanged
                res = agg_.imbalance(snap_bid, snap_ask, n);
            } else if (fname == "DEPTH") {
                // By price, so both sides unless the argument names one (#200): a price is on one
                // side of a book that is not crossed, and on both of one that is.
                const int64_t price = args.numbers.empty() ? 0 : parse_i64(args.numbers[0]);
                res = AggResult{0, false, kAggScaleRaw};
                if (bids) res.value += agg_.depth_at_price(snap_bid, price).value;
                if (asks) res.value += agg_.depth_at_price(snap_ask, price).value;
            } else if (fname == "DEPTH_RANGE") {
                // "[side, ]lo, hi" - the spaces are the parser's, not the client's.
                int64_t lo = 0, hi = 0;
                if (args.numbers.size() != 2 || !parse_i64_strict(args.numbers[0], lo) ||
                    !parse_i64_strict(args.numbers[1], hi)) {
                    OB_LOG_WARN("query",
                                "Rejecting DEPTH_RANGE with unparseable bounds: arg='%s'",
                                farg.c_str());
                    return "AGG_BAD_ARGUMENT: DEPTH_RANGE needs two integer bounds, got '" +
                           farg + "'";
                }
                // Both sides unless the argument names one (#200), empty when no side read has a
                // level in the range.
                res = AggResult{0, true, kAggScaleRaw};
                for (const SoASide* s : {bids ? &snap_bid : nullptr, asks ? &snap_ask : nullptr}) {
                    if (s == nullptr) continue;
                    const AggResult part = agg_.depth_within_range(*s, lo, hi);
                    if (part.empty) continue;
                    res.value += part.value;
                    res.empty = false;
                }
            } else if (fname == "CUMULATIVE_VOLUME") {
                uint32_t n = args.numbers.empty() ? 0 : parse_u32(args.numbers[0]);
                if (n == 0) n = side.depth;
                res = agg_.cumulative_volume(side, n);
            }

            // All four fields travel. value alone was what shipped, which is how a
            // scaled result and an empty one both arrived at clients as a bare
            // integer they had no way to interpret.
            qr.agg_values.push_back(AggValue{expr, res.value, res.empty, res.scale});
        }

        OB_LOG_DEBUG("query",
                     "Aggregate query: symbol=%s exchange=%s count=%zu",
                     ast.symbol.c_str(), ast.exchange.c_str(), qr.agg_values.size());

        shape.is_aggregate = true;
        cb(qr);
        return {};
    }

    // ── SELECT without aggregation (columnar scan) ────────────────────────────
    //
    // Expanded here rather than left empty: the server writes the header before the first row and
    // has to write one even for an answer with no rows, so "empty means all seven" would have to
    // be understood identically by two readers that do not share a line of code.
    shape.columns = ast.projection.empty() ? all_query_columns() : ast.projection;
    OB_LOG_DEBUG("query", "Answer shape: %zu column(s)%s",
                 shape.columns.size(), ast.projection.empty() ? " (SELECT *)" : "");

    uint64_t ts_start = ast.ts_start_ns.value_or(0);
    uint64_t ts_end   = ast.ts_end_ns.value_or(UINT64_MAX);
    uint64_t lim      = ast.limit.value_or(UINT64_MAX);
    uint64_t count    = 0;

    // Wider than the answer where a predicate needs it to be: a price, side or level condition
    // reads its column whether or not the answer carries it.
    ColumnSet filtered;
    if (ast.price_lo.has_value() || ast.price_hi.has_value()) filtered.add(QueryColumn::Price);
    if (ast.side_lo.has_value() || ast.side_hi.has_value()) filtered.add(QueryColumn::Side);
    if (ast.level_lo.has_value() || ast.level_hi.has_value()) filtered.add(QueryColumn::Level);
    const ColumnSet to_read = columns_to_read(shape.columns, filtered);

    store_.scan(ts_start, ts_end, ast.symbol, ast.exchange, to_read,
                [&](const SnapshotRow& row) {
                    if (count >= lim) return;
                    if (!row_allowed(ast, row)) return;

                    QueryResult qr{};
                    qr.timestamp_ns    = row.timestamp_ns;
                    qr.sequence_number = row.sequence_number;
                    qr.price           = row.price;
                    qr.quantity        = row.quantity;
                    qr.order_count     = row.order_count;
                    qr.side            = row.side;
                    qr.level           = row.level_index;
                    cb(qr);
                    ++count;
                });

    return {};
}

namespace {

/// What one bucket has seen (#44): enough for every bucket aggregate at once, so the row loop does
/// not branch per function. A tie on the event time is broken by the order the scan delivered the
/// rows in, as SNAPSHOT's is (#168): FIRST keeps the earlier delivered, LAST the later - which is
/// what taking a strictly earlier time for one and an equal or later one for the other does.
///
/// The sums cannot overflow but one: a quantity is below 2^64 and a price's magnitude at most 2^63,
/// so 2^64 rows of either stay inside 128 bits, and so does one row's price times quantity - but a
/// sum of those products can leave them, and then `px_qty_overflow` says so and VWAP is refused.
struct BucketState {
    uint64_t count{0};
    uint64_t first_ts{0}, last_ts{0};
    int64_t  first_price{0}, last_price{0};
    uint64_t first_qty{0}, last_qty{0};
    int64_t  min_price{0}, max_price{0};
    uint64_t min_qty{0}, max_qty{0};
    unsigned __int128 sum_qty{0};
    __int128 sum_price{0};
    __int128 sum_px_qty{0};
    bool     px_qty_overflow{false};

    void add(const SnapshotRow& row) {
        const int64_t  px  = row.price;
        const uint64_t qty = row.quantity;
        if (count == 0) {
            first_ts = last_ts = row.timestamp_ns;
            first_price = last_price = min_price = max_price = px;
            first_qty = last_qty = min_qty = max_qty = qty;
        } else {
            if (row.timestamp_ns < first_ts) {
                first_ts = row.timestamp_ns; first_price = px; first_qty = qty;
            }
            if (row.timestamp_ns >= last_ts) {
                last_ts = row.timestamp_ns; last_price = px; last_qty = qty;
            }
            min_price = std::min(min_price, px);   max_price = std::max(max_price, px);
            min_qty   = std::min(min_qty, qty);    max_qty   = std::max(max_qty, qty);
        }
        ++count;
        sum_qty   += qty;
        sum_price += px;
        const __int128 product = static_cast<__int128>(px) * static_cast<__int128>(qty);
        if (__builtin_add_overflow(sum_px_qty, product, &sum_px_qty)) px_qty_overflow = true;
    }
};

constexpr int64_t kBucketScale = 1'000'000;   // AVG and VWAP, as the live book's VWAP

int64_t bucket_scale(BucketFn fn) {
    return fn == BucketFn::Avg || fn == BucketFn::Vwap ? kBucketScale : 1;
}

/// `sum * kBucketScale / divisor`, truncated toward zero, without multiplying before dividing:
/// the whole part and the remainder are scaled apart. `divisor` is positive. False when the
/// remainder's product leaves 128 bits, which a divisor past 2^107 could make it.
bool scaled_quotient(__int128 sum, __int128 divisor, __int128& out) {
    const __int128 whole = sum / divisor;
    __int128 rem = 0;
    if (__builtin_mul_overflow(sum % divisor, static_cast<__int128>(kBucketScale), &rem)) return false;
    __int128 scaled = 0;
    if (__builtin_mul_overflow(whole, static_cast<__int128>(kBucketScale), &scaled)) return false;
    out = scaled + rem / divisor;
    return true;
}

/// One aggregate of one bucket: its value, whether there was anything to compute it from, or false
/// when it does not fit an int64 - which the caller refuses, naming both.
bool bucket_value(const BucketAgg& agg, const BucketState& b, AggValue& out) {
    out.name  = agg.text;
    out.scale = bucket_scale(agg.fn);
    out.empty = false;
    out.value = 0;
    const bool price = agg.column == QueryColumn::Price;
    const auto from_u64 = [&](uint64_t v) {
        if (v > static_cast<uint64_t>(INT64_MAX)) return false;
        out.value = static_cast<int64_t>(v);
        return true;
    };
    const auto from_i128 = [&](__int128 v) {
        if (v > static_cast<__int128>(INT64_MAX) || v < static_cast<__int128>(INT64_MIN)) return false;
        out.value = static_cast<int64_t>(v);
        return true;
    };
    constexpr unsigned __int128 kI128Max = (static_cast<unsigned __int128>(1) << 127) - 1;
    switch (agg.fn) {
    case BucketFn::Count: return from_u64(b.count);
    case BucketFn::First: if (price) { out.value = b.first_price; return true; } return from_u64(b.first_qty);
    case BucketFn::Last:  if (price) { out.value = b.last_price; return true; }  return from_u64(b.last_qty);
    case BucketFn::Min:   if (price) { out.value = b.min_price; return true; }   return from_u64(b.min_qty);
    case BucketFn::Max:   if (price) { out.value = b.max_price; return true; }   return from_u64(b.max_qty);
    case BucketFn::Sum:
        if (b.sum_qty > static_cast<unsigned __int128>(INT64_MAX)) return false;
        out.value = static_cast<int64_t>(b.sum_qty);
        return true;
    case BucketFn::Avg: {
        // count < 2^64 and the sums inside 128 bits, so only the result can fail to fit.
        if (!price && b.sum_qty > kI128Max) return false;
        const __int128 sum = price ? b.sum_price : static_cast<__int128>(b.sum_qty);
        __int128 v = 0;
        return scaled_quotient(sum, static_cast<__int128>(b.count), v) && from_i128(v);
    }
    case BucketFn::Vwap: {
        if (b.sum_qty == 0) {   // every row of the bucket at quantity zero: nothing to weigh by
            out.empty = true;
            return true;
        }
        if (b.px_qty_overflow || b.sum_qty > kI128Max) return false;
        __int128 v = 0;
        return scaled_quotient(b.sum_px_qty, static_cast<__int128>(b.sum_qty), v) && from_i128(v);
    }
    }
    return false;
}

}  // namespace

std::string QueryEngine::execute_buckets(const QueryAST& ast, const RowCallback& cb,
                                         QueryShape& shape) {
    const uint64_t width = *ast.bucket_ns;
    shape.is_buckets = true;
    shape.bucket_columns.clear();
    for (const BucketAgg& agg : ast.bucket_aggs) {
        shape.bucket_columns.push_back({agg.text, bucket_scale(agg.fn)});
    }

    // The time, the columns the aggregates read, and the ones the conditions are on.
    ColumnSet to_read;
    to_read.add(QueryColumn::TimestampNs);
    for (const BucketAgg& agg : ast.bucket_aggs) {
        if (agg.fn != BucketFn::Count) to_read.add(agg.column);
        // VWAP(price) weighs each price by its row's quantity: a column it reads without naming.
        if (agg.fn == BucketFn::Vwap) to_read.add(QueryColumn::Quantity);
    }
    if (ast.price_lo.has_value() || ast.price_hi.has_value()) to_read.add(QueryColumn::Price);
    if (ast.side_lo.has_value() || ast.side_hi.has_value()) to_read.add(QueryColumn::Side);
    if (ast.level_lo.has_value() || ast.level_hi.has_value()) to_read.add(QueryColumn::Level);

    const size_t ceiling = max_query_buckets();
    std::unordered_map<uint64_t, BucketState> buckets;
    uint64_t rows = 0;
    bool too_many = false;
    // The bucket the last row went to: a segment's rows are appended in about their time order, so
    // the next row is nearly always in it, and the map is asked only when it is not. References to
    // an unordered_map's elements survive its rehashing.
    BucketState* last = nullptr;
    uint64_t last_start = 0;
    store_.scan(ast.ts_start_ns.value_or(0), ast.ts_end_ns.value_or(UINT64_MAX), ast.symbol,
                ast.exchange, to_read, [&](const SnapshotRow& row) {
                    if (too_many || !row_allowed(ast, row)) return;
                    const uint64_t start = row.timestamp_ns - row.timestamp_ns % width;
                    if (last == nullptr || start != last_start) {
                        auto it = buckets.find(start);
                        if (it == buckets.end()) {
                            // Rows do not arrive in time order - segments by their start, a
                            // segment's rows as appended, and a client's own event times anywhere
                            // (#105) - so neither LIMIT nor the ceiling can end the scan early;
                            // past the ceiling the rest of it only counts.
                            if (buckets.size() >= ceiling) { too_many = true; return; }
                            it = buckets.emplace(start, BucketState{}).first;
                        }
                        last = &it->second;
                        last_start = start;
                    }
                    last->add(row);
                    ++rows;
                });
    if (too_many) {
        OB_LOG_WARN("query", "Rejecting GROUP BY %s.%s: more than %zu buckets of %llu ns",
                    ast.symbol.c_str(), ast.exchange.c_str(), ceiling,
                    static_cast<unsigned long long>(width));
        return "BUCKETS_TOO_MANY: more than " + std::to_string(ceiling) +
               " buckets; narrow the time range or widen the interval";
    }

    std::vector<uint64_t> starts;
    starts.reserve(buckets.size());
    for (const auto& [start, state] : buckets) starts.push_back(start);
    std::sort(starts.begin(), starts.end());
    if (ast.limit.has_value() && *ast.limit < starts.size()) starts.resize(*ast.limit);

    // Every value before the first row: one that does not fit is the answer, not a row of it.
    std::vector<QueryResult> answer;
    answer.reserve(starts.size());
    for (uint64_t start : starts) {
        const BucketState& b = buckets.at(start);
        QueryResult qr{};
        qr.timestamp_ns = start;
        qr.agg_values.reserve(ast.bucket_aggs.size());
        for (const BucketAgg& agg : ast.bucket_aggs) {
            AggValue v;
            if (!bucket_value(agg, b, v)) {
                OB_LOG_WARN("query", "Rejecting GROUP BY %s.%s: %s of the bucket at %llu does not "
                                     "fit a 64-bit integer",
                            ast.symbol.c_str(), ast.exchange.c_str(), agg.text.c_str(),
                            static_cast<unsigned long long>(start));
                return "BUCKET_OVERFLOW: " + agg.text + " of the bucket at " +
                       std::to_string(start) + " does not fit a 64-bit integer";
            }
            qr.agg_values.push_back(std::move(v));
        }
        answer.push_back(std::move(qr));
    }
    OB_LOG_DEBUG("query", "GROUP BY %s.%s: %llu row(s) in %zu bucket(s) of %llu ns, %zu answered",
                 ast.symbol.c_str(), ast.exchange.c_str(), static_cast<unsigned long long>(rows),
                 buckets.size(), static_cast<unsigned long long>(width), answer.size());
    for (const QueryResult& qr : answer) cb(qr);
    return {};
}

std::string QueryEngine::format(const QueryAST& ast) {
    std::ostringstream os;

    // 1. SELECT / SUBSCRIBE
    if (ast.type == QueryType::SUBSCRIBE) {
        os << "SUBSCRIBE";
    } else {
        os << "SELECT";
    }

    // 2. Select list
    if (ast.select_exprs.empty()) {
        os << " *";
    } else {
        for (size_t i = 0; i < ast.select_exprs.size(); ++i) {
            os << (i == 0 ? " " : ", ") << ast.select_exprs[i];
        }
    }

    // 3. FROM 'symbol'.'exchange'
    os << " FROM '" << ast.symbol << "'.'" << ast.exchange << "'";

    // 4. WHERE clause
    bool where_written = false;
    auto write_where_or_and = [&]() {
        if (!where_written) { os << " WHERE"; where_written = true; }
        else                { os << " AND"; }
    };

    // One column's range, inclusive at both ends, as the parser reads it back. Unary plus, so that a
    // `side` - a uint8_t - prints as a number rather than as the character it also is (#200).
    const auto write_range = [&](const char* column, const auto& lo, const auto& hi) {
        if (lo.has_value() && hi.has_value()) {
            write_where_or_and();
            os << " " << column << " BETWEEN " << +*lo << " AND " << +*hi;
        } else if (lo.has_value()) {
            write_where_or_and();
            os << " " << column << " >= " << +*lo;
        } else if (hi.has_value()) {
            write_where_or_and();
            os << " " << column << " <= " << +*hi;
        }
    };

    if (ast.snapshot_ts_ns.has_value()) {
        write_where_or_and();
        os << " AT " << ast.snapshot_ts_ns.value();
    } else {
        write_range("timestamp", ast.ts_start_ns, ast.ts_end_ns);
    }
    // A snapshot's price, side and level conditions are its own since #199 and #200, so they are
    // written for it too: they used to be left out, which was harmless only while they were ignored.
    write_range("price", ast.price_lo, ast.price_hi);
    write_range("side", ast.side_lo, ast.side_hi);
    write_range("level", ast.level_lo, ast.level_hi);

    // 5. GROUP BY TIME_BUCKET (#44), in the largest unit that divides the interval, which the parser
    //    reads back to the same nanoseconds.
    if (ast.bucket_ns.has_value()) {
        static constexpr struct { const char* name; uint64_t ns; } kUnits[] = {
            {"d", 86'400ULL * 1'000'000'000ULL}, {"h", 3'600ULL * 1'000'000'000ULL},
            {"m", 60ULL * 1'000'000'000ULL}, {"s", 1'000'000'000ULL}, {"ms", 1'000'000ULL},
            {"us", 1'000ULL}, {"ns", 1ULL},
        };
        const uint64_t ns = *ast.bucket_ns;
        for (const auto& u : kUnits) {
            if (ns % u.ns == 0) {
                os << " GROUP BY TIME_BUCKET(" << ns / u.ns << u.name << ")";
                break;
            }
        }
    }

    // 6. LIMIT (SELECT only)
    if (ast.type != QueryType::SUBSCRIBE && ast.limit.has_value()) {
        os << " LIMIT " << ast.limit.value();
    }

    return os.str();
}

void QueryEngine::compact_locked() {
    // Caller holds subs_mtx_ exclusively. Does nothing while a notification is running: a
    // notification holds a raw pointer to an entry, and erasing the unique_ptr that owns it while
    // that is true is the defect this whole mechanism replaced, moved one level down.
    if (notifying_.load(std::memory_order_acquire) != 0) return;

    const size_t before = subscriptions_.size();
    subscriptions_.erase(
        std::remove_if(subscriptions_.begin(), subscriptions_.end(),
                       [](const std::unique_ptr<Subscription>& s) {
                           return s->dead.load(std::memory_order_relaxed);
                       }),
        subscriptions_.end());
    if (subscriptions_.size() != before) {
        OB_LOG_DEBUG("query", "Compacted subscriptions: %zu -> %zu",
                     before, subscriptions_.size());
    }
}

uint64_t QueryEngine::subscribe(std::string_view sql, RowCallback cb) {
    QueryAST ast;
    std::string err = parse(sql, ast);
    if (!err.empty()) {
        OB_LOG_WARN("query", "Subscription refused, query does not parse: %s", err.c_str());
        return 0;
    }

    std::unique_lock<std::shared_mutex> lock(subs_mtx_);
    compact_locked();

    auto sub  = std::make_unique<Subscription>();
    sub->id   = next_sub_id_++;
    sub->ast  = std::move(ast);
    sub->cb   = std::move(cb);
    const uint64_t id     = sub->id;
    const std::string sym = sub->ast.symbol;
    const std::string exc = sub->ast.exchange;
    subscriptions_.push_back(std::move(sub));
    // Recounted from the vector rather than incremented, so the counter cannot drift away from
    // what is actually there across a compaction.
    live_.store(count_live_locked(), std::memory_order_relaxed);

    OB_LOG_INFO("query", "Subscription %llu registered for %s.%s (%zu live)",
                static_cast<unsigned long long>(id), sym.c_str(), exc.c_str(),
                static_cast<size_t>(live_.load(std::memory_order_relaxed)));
    return id;
}

void QueryEngine::unsubscribe(uint64_t id) {
    std::unique_lock<std::shared_mutex> lock(subs_mtx_);
    bool found = false;
    for (auto& sub : subscriptions_) {
        if (sub->id == id && !sub->dead.load(std::memory_order_relaxed)) {
            sub->dead.store(true, std::memory_order_relaxed);
            found = true;
            break;
        }
    }
    if (!found) {
        OB_LOG_DEBUG("query", "Unsubscribe for unknown or already-cancelled id %llu",
                     static_cast<unsigned long long>(id));
        return;
    }
    live_.store(count_live_locked(), std::memory_order_relaxed);
    OB_LOG_INFO("query", "Subscription %llu cancelled (%zu live)",
                static_cast<unsigned long long>(id),
                static_cast<size_t>(live_.load(std::memory_order_relaxed)));
    compact_locked();
}

size_t QueryEngine::count_live_locked() const {
    size_t live = 0;
    for (const auto& sub : subscriptions_) {
        if (!sub->dead.load(std::memory_order_relaxed)) ++live;
    }
    return live;
}

void QueryEngine::notify_subscribers(const std::string& symbol,
                                     const std::string& exchange,
                                     std::span<const SnapshotRow> rows) {
    if (rows.empty()) return;

    // ── Why the callback runs with no lock held ───────────────────────────────────────────────
    //
    // The first version of this invoked `sub.cb()` under the shared lock, with a comment claiming a
    // callback that cancels its own subscription was safe because it only marks an entry dead.
    // That was wrong: marking takes the exclusive lock, and `std::shared_mutex` is not recursive -
    // a thread that already holds it in any mode and asks again is undefined behaviour, and in
    // practice a deadlock. The comment described the intent and not the code, which is the same
    // shape as an invariant living in a comment that nobody establishes.
    //
    // So: gather matching entries under the shared lock, release it, then invoke. `notifying_` is
    // raised first and stays raised for the whole call, and `compact_locked()` refuses to erase
    // anything while it is non-zero - so the pointers below stay valid without the lock. Only
    // appends can happen in that window, which is why resuming by index is sound.
    notifying_.fetch_add(1, std::memory_order_acquire);

    static constexpr size_t kBatch = 32;
    const Subscription* matched[kBatch];
    size_t index = 0;

    while (true) {
        size_t found = 0;
        size_t size  = 0;
        {
            std::shared_lock<std::shared_mutex> lock(subs_mtx_);
            size = subscriptions_.size();
            while (index < size && found < kBatch) {
                const Subscription& sub = *subscriptions_[index];
                ++index;
                if (sub.dead.load(std::memory_order_relaxed)) continue;
                // Symbol and exchange are per-subscription, so they are checked once for the whole
                // batch of rows rather than once per row.
                if (sub.ast.symbol != symbol || sub.ast.exchange != exchange) continue;
                matched[found++] = &sub;
            }
        }

        for (size_t i = 0; i < found; ++i) {
            const Subscription& sub = *matched[i];
            const QueryAST& ast     = sub.ast;
            for (const SnapshotRow& row : rows) {
                if (!row_allowed(ast, row)) continue;
                // Build QueryResult and invoke callback
                QueryResult qr{};
                qr.timestamp_ns    = row.timestamp_ns;
                qr.sequence_number = row.sequence_number;
                qr.price           = row.price;
                qr.quantity        = row.quantity;
                qr.order_count     = row.order_count;
                qr.side            = row.side;
                qr.level           = row.level_index;
                sub.cb(qr);
            }
        }

        if (index >= size) break;
    }

    notifying_.fetch_sub(1, std::memory_order_release);
}

} // namespace ob
