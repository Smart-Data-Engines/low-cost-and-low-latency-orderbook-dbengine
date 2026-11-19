#pragma once

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <unordered_set>

namespace ob {

/// The symbol and exchange of a row waiting for the columnar flush (#270).
struct RowName {
    std::string symbol;
    std::string exchange;
};

/// The names the engine's pending rows carry: one per (symbol, exchange) pair, so that a row carries
/// a pointer where it carried two strings (#270).
///
/// Until #270 every level of every write built both strings again - two `strlen`s and two string
/// constructions a level, on the client loop - and the row they made, 120 bytes, was assembled on the
/// stack and copied into the queue, where a wide load of what narrow stores had just written stalled
/// it (95% of `apply_in_memory`'s samples on the m8a.xlarge on that one instruction). A pending row is
/// now 64 bytes and carries `const RowName*`.
///
/// **A name lives as long as the table, at the same address.** Nothing is ever removed, so the pointer
/// a row holds is good however long the row waits - in the queue, in a batch a flush has taken, put
/// back in front after a drain that failed - with no rule about when no row points at a name any more.
/// The cost is a name, about a hundred bytes, for every pair that ever had a row, through a wipe of the
/// node's data as well; each such pair also has a book of tens of kilobytes, so this table is never
/// what bounds the number of pairs.
///
/// `of()` is called under the owner's lock (the engine's `mtx_`, which every write path holds). A name
/// it returned is immutable and may be read by any thread for as long as the table lives: a row
/// reaches the flush through the queue, under that same lock, which orders the reading after the
/// making.
class RowNames {
public:
    RowNames() = default;
    RowNames(const RowNames&) = delete;
    RowNames& operator=(const RowNames&) = delete;

    /// The name of (`symbol`, `exchange`), made the first time it is asked for. `expected` - a name
    /// the caller holds already, its book's - is returned without a lookup when it is this pair, and
    /// passed over when it is another: a book is keyed by `symbol.exchange`, which two pairs can share.
    const RowName& of(std::string_view symbol, std::string_view exchange,
                      const RowName* expected = nullptr) {
        if (expected != nullptr && expected->symbol == symbol && expected->exchange == exchange) {
            return *expected;
        }
        return find_or_make(symbol, exchange);
    }

    /// The names made so far.
    size_t size() const { return names_.size(); }

private:
    const RowName& find_or_make(std::string_view symbol, std::string_view exchange);

    struct Pair {
        std::string_view symbol;
        std::string_view exchange;
    };
    struct Hash {
        using is_transparent = void;
        size_t operator()(const Pair& p) const noexcept {
            const size_t h = std::hash<std::string_view>{}(p.symbol);
            return h ^ (std::hash<std::string_view>{}(p.exchange) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2));
        }
        size_t operator()(const RowName& n) const noexcept { return (*this)(Pair{n.symbol, n.exchange}); }
    };
    struct Equal {
        using is_transparent = void;
        static Pair pair(const Pair& p) { return p; }
        static Pair pair(const RowName& n) { return Pair{n.symbol, n.exchange}; }
        template <typename A, typename B>
        bool operator()(const A& a, const B& b) const noexcept {
            const Pair x = pair(a);
            const Pair y = pair(b);
            return x.symbol == y.symbol && x.exchange == y.exchange;
        }
    };

    /// Node-based: an element keeps its address when the set rehashes, and none is erased.
    std::unordered_set<RowName, Hash, Equal> names_;
};

}  // namespace ob
