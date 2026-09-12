// Length-prefixed multi-master wire framing.

#include "orderbook/multi_master.hpp"

#include <cstring>

namespace ob {

void encode_frame(const void* payload, size_t len, std::vector<uint8_t>& out) {
    const uint32_t length = static_cast<uint32_t>(len);
    const auto* len_bytes = reinterpret_cast<const uint8_t*>(&length);
    out.insert(out.end(), len_bytes, len_bytes + sizeof(uint32_t));

    if (payload && len > 0) {
        const auto* payload_bytes = static_cast<const uint8_t*>(payload);
        out.insert(out.end(), payload_bytes, payload_bytes + len);
    }
}

int parse_frames(std::vector<uint8_t>& recv_buf,
                 std::vector<std::pair<size_t, size_t>>& frames_out) {
    frames_out.clear();

    size_t offset = 0;
    while (offset + MM_FRAME_HEADER_SIZE <= recv_buf.size()) {
        uint32_t length = 0;
        std::memcpy(&length, recv_buf.data() + offset, sizeof(uint32_t));

        if (length > MM_MAX_FRAME_PAYLOAD) {
            return -1;
        }

        if (offset + MM_FRAME_HEADER_SIZE + length > recv_buf.size()) {
            break;
        }

        frames_out.emplace_back(offset + MM_FRAME_HEADER_SIZE, static_cast<size_t>(length));
        offset += MM_FRAME_HEADER_SIZE + length;
    }

    if (offset > 0) {
        recv_buf.erase(recv_buf.begin(), recv_buf.begin() + static_cast<std::ptrdiff_t>(offset));
    }

    return 0;
}

} // namespace ob
