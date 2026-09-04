// Feature: orderbook-dbengine — Query Engine parser and skeleton execution
#include "orderbook/query_engine.hpp"
#include "orderbook/logger.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <climits>
#include <cstdint>
#include <cstring>
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
    // Aggregation function names
    KW_SUM, KW_AVG, KW_MIN, KW_MAX, KW_VWAP,
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
        if (at(TokKind::KW_LIMIT)) {
            consume();
            if (auto e = parse_limit(out); !e.empty()) return e;
        }
        return {};
    }

    // subscribe_stmt body (after SUBSCRIBE keyword consumed)
    std::string parse_subscribe_body(QueryAST& out) {
        if (auto e = parse_select_list(out); !e.empty()) return e;
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
        if (auto e = parse_select_item(expr); !e.empty()) return e;
        out.select_exprs.push_back(expr);

        while (at(TokKind::COMMA)) {
            consume();
            if (auto e = parse_select_item(expr); !e.empty()) return e;
            out.select_exprs.push_back(expr);
        }
        return {};
    }

    std::string parse_select_item(std::string& out_expr) {
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
            out_expr = consume().text;
            return {};
        }
        return make_error("expected column name, aggregation function, or '*'");
    }

    bool is_column_name(TokKind k) const {
        return k == TokKind::KW_PRICE || k == TokKind::KW_QUANTITY ||
               k == TokKind::KW_ORDER_COUNT || k == TokKind::KW_TIMESTAMP ||
               k == TokKind::KW_SEQUENCE_NUMBER || k == TokKind::KW_SIDE ||
               k == TokKind::KW_LEVEL;
    }

    bool is_agg_func(TokKind k) const {
        return k == TokKind::KW_SUM || k == TokKind::KW_AVG ||
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
        if (func_tok.kind == TokKind::KW_IMBALANCE ||
            func_tok.kind == TokKind::KW_CUMULATIVE_VOLUME) {
            // IMBALANCE(integer_literal) / CUMULATIVE_VOLUME(integer_literal)
            if (!at(TokKind::INT_LIT))
                return make_error("expected integer literal in " + func_name + "(...)");
            inner = consume().text;
        } else if (func_tok.kind == TokKind::KW_DEPTH) {
            // DEPTH(price_expr) — price_expr is an integer literal
            if (!at(TokKind::INT_LIT))
                return make_error("expected price expression in DEPTH(...)");
            inner = consume().text;
        } else if (func_tok.kind == TokKind::KW_DEPTH_RANGE) {
            // DEPTH_RANGE(price_expr, price_expr)
            if (!at(TokKind::INT_LIT))
                return make_error("expected first price expression in DEPTH_RANGE(...)");
            std::string p1 = consume().text;
            if (!at(TokKind::COMMA))
                return make_error("expected ',' in DEPTH_RANGE(...)");
            consume();
            if (!at(TokKind::INT_LIT))
                return make_error("expected second price expression in DEPTH_RANGE(...)");
            std::string p2 = consume().text;
            inner = p1 + ", " + p2;
        } else {
            // SUM, AVG, MIN, MAX, VWAP, SPREAD, MID_PRICE — agg_arg ::= column_name | "*"
            if (at(TokKind::STAR)) {
                inner = "*"; consume();
            } else if (is_column_name(current().kind)) {
                inner = consume().text;
            } else {
                return make_error("expected column name or '*' in " + func_name + "(...)");
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
        if (at(TokKind::KW_AT)) {
            consume();
            return parse_snapshot_condition(out);
        }
        return make_error("expected 'timestamp', 'price', or 'AT' condition");
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
            out.ts_start_ns = lo;
            out.ts_end_ns   = hi;
            return {};
        }
        // comparison operator
        TokKind op = current().kind;
        if (!is_comparison_op(op))
            return make_error("expected comparison operator or BETWEEN after 'timestamp'");
        consume();
        uint64_t val = 0;
        if (auto e = parse_uint64(val); !e.empty()) return e;
        apply_ts_op(out, op, val);
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
            out.price_lo = lo;
            out.price_hi = hi;
            return {};
        }
        TokKind op = current().kind;
        if (!is_comparison_op(op))
            return make_error("expected comparison operator or BETWEEN after 'price'");
        consume();
        int64_t val = 0;
        if (auto e = parse_int64(val); !e.empty()) return e;
        apply_price_op(out, op, val);
        return {};
    }

    // snapshot_condition (after "AT" consumed)
    std::string parse_snapshot_condition(QueryAST& out) {
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

    void apply_ts_op(QueryAST& out, TokKind op, uint64_t val) {
        switch (op) {
            case TokKind::GE: case TokKind::GT: case TokKind::EQ:
                out.ts_start_ns = val; break;
            case TokKind::LE: case TokKind::LT:
                out.ts_end_ns = val; break;
            default: break;
        }
    }

    void apply_price_op(QueryAST& out, TokKind op, int64_t val) {
        switch (op) {
            case TokKind::GE: case TokKind::GT: case TokKind::EQ:
                out.price_lo = val; break;
            case TokKind::LE: case TokKind::LT:
                out.price_hi = val; break;
            default: break;
        }
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

std::string QueryEngine::execute(std::string_view sql, RowCallback cb) {
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
    SoABuffer* const live_buf = live_buffer_ ? live_buffer_(live_key) : nullptr;
    bool found_in_live = (live_buf != nullptr);
    bool found_in_store = false;
    if (!found_in_live) {
        auto idx_snapshot = store_.index();
        for (const auto& seg : idx_snapshot) {
            if (seg.symbol == ast.symbol && seg.exchange == ast.exchange) {
                found_in_store = true;
                break;
            }
        }
    }
    if (!found_in_live && !found_in_store) {
        return "OB_ERR_NOT_FOUND: symbol '" + ast.symbol +
               "' exchange '" + ast.exchange + "' not found";
    }

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
                return "OB_ERR_PARSE: undefined aggregation function '" +
                       fname + "' at position " + std::to_string(i);
            }

            // The column argument has to name what the function actually
            // aggregates. The dispatcher below calls sum_qty() for SUM and
            // avg_price() for AVG regardless of the argument, so SUM(price)
            // returned a quantity labelled SUM(price) and AVG(quantity) returned a
            // price. An argument that would be ignored is an error, not decoration.
            const std::string farg = agg_func_arg(expr);
            if (fname == "SUM") {
                if (farg != "*" && farg != "quantity") {
                    OB_LOG_WARN("query", "Rejecting %s: SUM aggregates quantity", expr.c_str());
                    return "AGG_BAD_ARGUMENT: SUM aggregates quantity; write SUM(quantity) "
                           "or SUM(*), not '" + expr + "'";
                }
            } else if (fname == "AVG" || fname == "MIN" || fname == "MAX" ||
                       fname == "VWAP") {
                if (farg != "*" && farg != "price") {
                    OB_LOG_WARN("query", "Rejecting %s: %s aggregates price",
                                expr.c_str(), fname.c_str());
                    return "AGG_BAD_ARGUMENT: " + fname + " aggregates price; write " +
                           fname + "(price) or " + fname + "(*), not '" + expr + "'";
                }
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
    }

    // ── SNAPSHOT query ────────────────────────────────────────────────────────
    if (ast.type == QueryType::SNAPSHOT) {
        uint64_t snap_ts = ast.snapshot_ts_ns.value_or(0);

        // Reconstruct orderbook state: for each (side, level_index) keep the
        // last row at or before snap_ts.
        // Key: (side << 16) | level_index  →  last SnapshotRow
        std::unordered_map<uint32_t, SnapshotRow> state;

        store_.scan(0, snap_ts, ast.symbol, ast.exchange,
                    [&](const SnapshotRow& row) {
                        uint32_t key = (static_cast<uint32_t>(row.side) << 16) |
                                       static_cast<uint32_t>(row.level_index);
                        state[key] = row;
                    });

        uint64_t count = 0;
        uint64_t lim = ast.limit.value_or(UINT64_MAX);
        for (auto& [k, row] : state) {
            if (count >= lim) break;
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
        return {};
    }

    // ── SELECT with aggregation ───────────────────────────────────────────────
    if (has_agg) {
        if (live_buf == nullptr) {
            return "OB_ERR_NOT_FOUND: symbol '" + ast.symbol +
                   "' exchange '" + ast.exchange + "' not found in live buffers";
        }
        const SoABuffer* buf = live_buf;

        // Read a consistent snapshot of both sides
        SoASide snap_bid, snap_ask;
        read_snapshot(*buf, snap_bid, snap_ask);

        uint32_t depth = snap_bid.depth; // use bid depth as default n_levels

        QueryResult qr{};
        qr.timestamp_ns    = buf->last_timestamp_ns;
        qr.sequence_number = buf->sequence_number.load(std::memory_order_relaxed);

        for (const auto& expr : ast.select_exprs) {
            if (!is_agg_expr(expr)) continue;
            std::string fname = agg_func_name(expr);
            std::string farg  = agg_func_arg(expr);

            AggResult res{0, true};

            if (fname == "SUM") {
                res = agg_.sum_qty(snap_bid, depth);
            } else if (fname == "AVG") {
                res = agg_.avg_price(snap_bid, depth);
            } else if (fname == "MIN") {
                res = agg_.min_price(snap_bid, depth);
            } else if (fname == "MAX") {
                res = agg_.max_price(snap_bid, depth);
            } else if (fname == "VWAP") {
                res = agg_.vwap(snap_bid, depth);
            } else if (fname == "SPREAD") {
                res = agg_.spread(snap_bid, snap_ask);
            } else if (fname == "MID_PRICE") {
                res = agg_.mid_price(snap_bid, snap_ask);
            } else if (fname == "IMBALANCE") {
                uint32_t n = parse_u32(farg);
                if (n == 0) n = depth;
                res = agg_.imbalance(snap_bid, snap_ask, n);
            } else if (fname == "DEPTH") {
                int64_t price = parse_i64(farg);
                res = agg_.depth_at_price(snap_bid, price);
            } else if (fname == "DEPTH_RANGE") {
                // farg = "lo, hi" — the space is inserted by the parser, not the client.
                auto comma = farg.find(',');
                int64_t lo = 0, hi = 0;
                if (comma == std::string::npos ||
                    !parse_i64_strict(farg.substr(0, comma), lo) ||
                    !parse_i64_strict(farg.substr(comma + 1), hi)) {
                    OB_LOG_WARN("query",
                                "Rejecting DEPTH_RANGE with unparseable bounds: arg='%s'",
                                farg.c_str());
                    return "AGG_BAD_ARGUMENT: DEPTH_RANGE needs two integer bounds, got '" +
                           farg + "'";
                }
                res = agg_.depth_within_range(snap_bid, lo, hi);
            } else if (fname == "CUMULATIVE_VOLUME") {
                uint32_t n = parse_u32(farg);
                if (n == 0) n = depth;
                res = agg_.cumulative_volume(snap_bid, n);
            }

            // All four fields travel. value alone was what shipped, which is how a
            // scaled result and an empty one both arrived at clients as a bare
            // integer they had no way to interpret.
            qr.agg_values.push_back(AggValue{expr, res.value, res.empty, res.scale});
        }

        OB_LOG_DEBUG("query",
                     "Aggregate query: symbol=%s exchange=%s count=%zu",
                     ast.symbol.c_str(), ast.exchange.c_str(), qr.agg_values.size());

        cb(qr);
        return {};
    }

    // ── SELECT without aggregation (columnar scan) ────────────────────────────
    uint64_t ts_start = ast.ts_start_ns.value_or(0);
    uint64_t ts_end   = ast.ts_end_ns.value_or(UINT64_MAX);
    uint64_t lim      = ast.limit.value_or(UINT64_MAX);
    uint64_t count    = 0;

    store_.scan(ts_start, ts_end, ast.symbol, ast.exchange,
                [&](const SnapshotRow& row) {
                    if (count >= lim) return;

                    // Apply price filter if set
                    if (ast.price_lo.has_value() && row.price < ast.price_lo.value()) return;
                    if (ast.price_hi.has_value() && row.price > ast.price_hi.value()) return;

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

    if (ast.snapshot_ts_ns.has_value()) {
        write_where_or_and();
        os << " AT " << ast.snapshot_ts_ns.value();
    } else {
        if (ast.ts_start_ns.has_value() && ast.ts_end_ns.has_value()) {
            write_where_or_and();
            os << " timestamp BETWEEN " << ast.ts_start_ns.value()
               << " AND " << ast.ts_end_ns.value();
        } else if (ast.ts_start_ns.has_value()) {
            write_where_or_and();
            os << " timestamp >= " << ast.ts_start_ns.value();
        } else if (ast.ts_end_ns.has_value()) {
            write_where_or_and();
            os << " timestamp <= " << ast.ts_end_ns.value();
        }

        if (ast.price_lo.has_value() && ast.price_hi.has_value()) {
            write_where_or_and();
            os << " price BETWEEN " << ast.price_lo.value()
               << " AND " << ast.price_hi.value();
        } else if (ast.price_lo.has_value()) {
            write_where_or_and();
            os << " price >= " << ast.price_lo.value();
        } else if (ast.price_hi.has_value()) {
            write_where_or_and();
            os << " price <= " << ast.price_hi.value();
        }
    }

    // 5. LIMIT (SELECT only)
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
                // Check timestamp filter
                if (ast.ts_start_ns.has_value() && row.timestamp_ns < ast.ts_start_ns.value())
                    continue;
                if (ast.ts_end_ns.has_value() && row.timestamp_ns > ast.ts_end_ns.value())
                    continue;
                // Check price filter
                if (ast.price_lo.has_value() && row.price < ast.price_lo.value()) continue;
                if (ast.price_hi.has_value() && row.price > ast.price_hi.value()) continue;
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
