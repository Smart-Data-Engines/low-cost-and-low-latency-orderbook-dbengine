// The connection a migration moves a symbol's rows over (#196). See symbol_mover.hpp for why it is
// a translation unit of its own.

#include "orderbook/symbol_mover.hpp"

#include "orderbook/client.hpp"
#include "orderbook/logger.hpp"

#include <charconv>
#include <vector>

namespace ob {

struct TargetConnection::Impl {
    Impl(ClientConfig config, bool address_ok) : client(std::move(config)), address_ok(address_ok) {}
    OrderbookClient    client;
    bool               address_ok;
    std::vector<Level> levels;   // reused: one update's, converted
};

namespace {

/// "host:port" into a client's configuration; false when it is not that.
bool parse_address(const std::string& address, ClientConfig& config) {
    const size_t colon = address.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 == address.size()) return false;
    unsigned port = 0;
    const char* first = address.data() + colon + 1;
    const char* last = address.data() + address.size();
    const auto [end, ec] = std::from_chars(first, last, port);
    if (ec != std::errc{} || end != last || port == 0 || port > 65535) return false;
    config.host = address.substr(0, colon);
    config.port = static_cast<uint16_t>(port);
    return true;
}

}  // namespace

TargetConnection::TargetConnection(std::string address, const MigrationAccess& access)
    : address_(std::move(address)) {
    ClientConfig config;
    config.connect_timeout_sec = 5.0;
    // The target's ABANDON seals and syncs everything before it drops the symbol, and a copy's
    // write waits for room like any other; neither is a hung connection at ten seconds.
    config.read_timeout_sec = 120.0;
    config.auth_identity    = access.identity;
    config.auth_secret      = access.secret;
    config.tls              = access.tls;
    config.tls_ca_file      = access.tls_ca_file;
    config.tls_verify       = true;
    const bool address_ok = parse_address(address_, config);
    if (!address_ok) {
        OB_LOG_ERROR("symbol_mover", "The map's address of the target, '%s', is not host:port",
                     address_.c_str());
    }
    impl_ = std::make_unique<Impl>(std::move(config), address_ok);
}

TargetConnection::~TargetConnection() = default;

std::string TargetConnection::connect() {
    if (!impl_->address_ok) return "the map's address of it, '" + address_ + "', is not host:port";
    auto r = impl_->client.connect();
    if (!r) {
        OB_LOG_WARN("symbol_mover", "Cannot reach the target at %s: %s", address_.c_str(),
                    r.error_message().c_str());
        return r.error_message();
    }
    OB_LOG_INFO("symbol_mover", "Connected to the target at %s", address_.c_str());
    return {};
}

bool TargetConnection::connected() const {
    return impl_->client.connected();
}

std::string TargetConnection::command(const std::string& line, std::string& answer) {
    answer.clear();
    auto r = impl_->client.command(line);
    if (!r) {
        OB_LOG_WARN("symbol_mover", "%s to %s failed: %s", line.c_str(), address_.c_str(),
                    r.error_message().c_str());
        return r.error_message();
    }
    answer = std::move(r.value());
    OB_LOG_DEBUG("symbol_mover", "%s to %s: %s", line.c_str(), address_.c_str(), answer.c_str());
    return {};
}

std::string TargetConnection::send_update(const std::string& symbol, const std::string& exchange,
                                          uint8_t side, const MovedLevel* levels, size_t n,
                                          uint64_t timestamp_ns) {
    auto& lv = impl_->levels;
    lv.resize(n);
    for (size_t i = 0; i < n; ++i) {
        lv[i].price = levels[i].price;
        lv[i].qty   = levels[i].qty;
        lv[i].count = levels[i].count;
    }
    auto r = impl_->client.minsert(symbol, exchange, side == 0 ? Side::BID : Side::ASK, lv.data(), n,
                                   timestamp_ns);
    if (!r) return r.error_message();
    return {};
}

std::string TargetConnection::send_updates(const std::string& symbol, const std::string& exchange,
                                           const std::vector<MovedWrite>& writes) {
    // The client's levels, one flat run, and the writes pointing into it once it has stopped growing.
    size_t total = 0;
    for (const MovedWrite& w : writes) total += w.n;
    auto& lv = impl_->levels;
    lv.resize(total);
    std::vector<OrderbookClient::MinsertWrite> batch(writes.size());
    size_t at = 0;
    for (size_t i = 0; i < writes.size(); ++i) {
        const MovedWrite& w = writes[i];
        for (size_t k = 0; k < w.n; ++k) {
            lv[at + k].price = w.levels[k].price;
            lv[at + k].qty   = w.levels[k].qty;
            lv[at + k].count = w.levels[k].count;
        }
        batch[i].symbol        = symbol;
        batch[i].exchange      = exchange;
        batch[i].side          = w.side == 0 ? Side::BID : Side::ASK;
        batch[i].levels        = lv.data() + at;
        batch[i].n_levels      = w.n;
        batch[i].event_time_ns = w.timestamp_ns;
        at += w.n;
    }
    std::string refusal;
    auto r = impl_->client.minsert_many(batch, refusal);
    if (!r) return r.error_message();
    return refusal;
}

}  // namespace ob
