// RowNames: the names a pending row carries by pointer since #270, one per (symbol, exchange) pair,
// each at an address that stays put for as long as the table lives.

#include "orderbook/row_names.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <map>
#include <string>
#include <utility>
#include <vector>

namespace {

TEST(RowNames, APairIsOneNameAndItsStringsAreThePairs) {
    ob::RowNames names;
    const ob::RowName& a = names.of("BTCUSDT", "BINANCE");
    EXPECT_EQ(a.symbol, "BTCUSDT");
    EXPECT_EQ(a.exchange, "BINANCE");
    EXPECT_EQ(&names.of("BTCUSDT", "BINANCE"), &a);
    EXPECT_EQ(names.size(), 1u);
}

TEST(RowNames, PairsThatDifferInEitherHalfAreDifferentNames) {
    ob::RowNames names;
    const ob::RowName* a = &names.of("BTCUSDT", "BINANCE");
    const ob::RowName* b = &names.of("ETHUSDT", "BINANCE");
    const ob::RowName* c = &names.of("BTCUSDT", "COINBASE");
    EXPECT_NE(a, b);
    EXPECT_NE(a, c);
    EXPECT_NE(b, c);
    EXPECT_EQ(c->symbol, "BTCUSDT");
    EXPECT_EQ(c->exchange, "COINBASE");
    EXPECT_EQ(names.size(), 3u);
}

TEST(RowNames, TwoPairsWhoseBookKeyIsTheSameAreTwoNames) {
    // A book is keyed by `symbol.exchange`, which these two pairs share; the rows of each carry their
    // own pair, as they carried its two strings before #270.
    ob::RowNames names;
    const ob::RowName& first = names.of("A.B", "C");
    const ob::RowName& second = names.of("A", "B.C");
    EXPECT_NE(&first, &second);
    EXPECT_EQ(second.symbol, "A");
    EXPECT_EQ(second.exchange, "B.C");
}

TEST(RowNames, AnExpectedNameIsTakenOnlyWhenItIsThePairAskedFor) {
    ob::RowNames names;
    const ob::RowName& book = names.of("A.B", "C");
    // The book's own pair: its name, without a lookup.
    EXPECT_EQ(&names.of("A.B", "C", &book), &book);
    // Another pair under the same book key: its own name, made now - not the book's.
    const ob::RowName& other = names.of("A", "B.C", &book);
    EXPECT_NE(&other, &book);
    EXPECT_EQ(other.symbol, "A");
    EXPECT_EQ(other.exchange, "B.C");
    // The same symbol on another exchange, and the same exchange with another symbol.
    EXPECT_EQ(names.of("A.B", "D", &book).exchange, "D");
    EXPECT_EQ(names.of("A.C", "C", &book).symbol, "A.C");
    EXPECT_EQ(names.size(), 4u);
}

TEST(RowNames, ANameKeepsItsAddressWhileTheTableGrows) {
    // What a row relies on: the pointer it was given stays good. Thousands of pairs rehash the set
    // several times over.
    ob::RowNames names;
    std::vector<std::pair<const ob::RowName*, std::string>> made;
    for (int i = 0; i < 5000; ++i) {
        const std::string symbol = "S" + std::to_string(i);
        made.emplace_back(&names.of(symbol, "EX"), symbol);
    }
    EXPECT_EQ(names.size(), 5000u);
    for (const auto& [name, symbol] : made) {
        EXPECT_EQ(name->symbol, symbol);
        EXPECT_EQ(name->exchange, "EX");
        EXPECT_EQ(&names.of(symbol, "EX"), name);
    }
}

RC_GTEST_PROP(RowNames, EveryPairAskedForIsOneNameHoldingThatPair, ()) {
    // Pairs drawn from a small alphabet, dots included, so that the same strings come back and book
    // keys collide; each asked for with an expected name that is sometimes its own and sometimes not.
    const auto part = rc::gen::container<std::string>(rc::gen::element('A', 'B', '.'));
    const auto pairs = *rc::gen::container<std::vector<std::pair<std::string, std::string>>>(
        rc::gen::pair(part, part));
    ob::RowNames names;
    std::map<std::pair<std::string, std::string>, const ob::RowName*> seen;
    const ob::RowName* previous = nullptr;
    for (const auto& [symbol, exchange] : pairs) {
        const ob::RowName& name = names.of(symbol, exchange, previous);
        RC_ASSERT(name.symbol == symbol);
        RC_ASSERT(name.exchange == exchange);
        const auto [at, fresh] = seen.try_emplace({symbol, exchange}, &name);
        if (!fresh) RC_ASSERT(at->second == &name);
        previous = &name;
    }
    RC_ASSERT(names.size() == seen.size());
}

}  // namespace
