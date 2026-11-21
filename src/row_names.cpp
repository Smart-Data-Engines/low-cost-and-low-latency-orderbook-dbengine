#include "orderbook/row_names.hpp"

#include "orderbook/logger.hpp"

namespace ob {

const RowName& RowNames::find_or_make(std::string_view symbol, std::string_view exchange) {
    // Looked up by the pair of views, so a name already made costs no string.
    if (const auto it = names_.find(Pair{symbol, exchange}); it != names_.end()) return *it;
    const auto made = names_.emplace(RowName{std::string(symbol), std::string(exchange)}).first;
    OB_LOG_DEBUG("engine", "row name %zu made: symbol '%.*s', exchange '%.*s'", names_.size(),
                 static_cast<int>(symbol.size()), symbol.data(), static_cast<int>(exchange.size()),
                 exchange.data());
    return *made;
}

}  // namespace ob
