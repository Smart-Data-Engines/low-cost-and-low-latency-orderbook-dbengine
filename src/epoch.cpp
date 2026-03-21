#include "orderbook/epoch.hpp"

#include <cstring>

namespace ob {

void epoch_to_payload(const EpochValue& epoch, uint8_t out[8]) {
    // Store as little-endian uint64.
    // On little-endian platforms this is a straight memcpy; on big-endian it
    // would need byte-swapping.  All target platforms (x86-64, aarch64) are LE.
    uint64_t v = epoch.term;
    std::memcpy(out, &v, sizeof(v));
}

EpochValue epoch_from_payload(const uint8_t data[8]) {
    uint64_t v = 0;
    std::memcpy(&v, data, sizeof(v));
    return EpochValue{v};
}

} // namespace ob
