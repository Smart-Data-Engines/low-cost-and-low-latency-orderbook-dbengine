#include "orderbook/session.hpp"
#include "orderbook/compression.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <unistd.h>

namespace ob {

// ── Session ──────────────────────────────────────────────────────────────────

Session::Session(int fd) : fd_(fd) {}

int Session::fd() const { return fd_; }

std::vector<std::string> Session::feed(const char* data, size_t len) {
    read_buffer_.append(data, len);

    if (!compressed_) {
        // Newline-delimited text mode
        std::vector<std::string> lines;
        size_t pos = 0;
        while (true) {
            auto nl = read_buffer_.find('\n', pos);
            if (nl == std::string::npos) break;
            lines.emplace_back(read_buffer_, pos, nl - pos);
            pos = nl + 1;
        }
        if (pos > 0) {
            read_buffer_.erase(0, pos);
        }
        return lines;
    }

    // Compressed binary framing: [4-byte BE length][LZ4 frame]
    std::vector<std::string> lines;
    while (read_buffer_.size() >= 4) {
        const auto* hdr = reinterpret_cast<const uint8_t*>(read_buffer_.data());
        uint32_t frame_len = (static_cast<uint32_t>(hdr[0]) << 24) |
                             (static_cast<uint32_t>(hdr[1]) << 16) |
                             (static_cast<uint32_t>(hdr[2]) << 8)  |
                             (static_cast<uint32_t>(hdr[3]));
        if (read_buffer_.size() < 4 + frame_len) break; // incomplete frame

        // Decompress the LZ4 frame
        auto decompressed = lz4_decompress(
            read_buffer_.data() + 4, static_cast<size_t>(frame_len));

        // Track compression metrics: wire bytes in, raw bytes out
        compress_bytes_out_ += static_cast<uint64_t>(frame_len);
        compress_bytes_in_  += static_cast<uint64_t>(decompressed.size());

        // The decompressed data is the original command text (may contain newline)
        std::string text(decompressed.begin(), decompressed.end());
        // Strip trailing newline if present
        while (!text.empty() && (text.back() == '\n' || text.back() == '\r')) {
            text.pop_back();
        }
        if (!text.empty()) {
            lines.push_back(std::move(text));
        }

        read_buffer_.erase(0, 4 + frame_len);
    }
    return lines;
}

bool Session::send_response(std::string_view response) {
    if (!compressed_) {
        const char* ptr = response.data();
        size_t remaining = response.size();
        while (remaining > 0) {
            ssize_t written = ::write(fd_, ptr, remaining);
            if (written < 0) {
                if (errno == EINTR) continue;
                return false;
            }
            ptr += written;
            remaining -= static_cast<size_t>(written);
        }
        return true;
    }

    // Compressed mode: [4-byte BE length][LZ4 frame]
    auto compressed = lz4_compress(response.data(), response.size());

    // Track compression metrics: raw bytes in, wire bytes out
    compress_bytes_in_  += static_cast<uint64_t>(response.size());
    compress_bytes_out_ += static_cast<uint64_t>(compressed.size());

    uint32_t frame_len = static_cast<uint32_t>(compressed.size());
    uint8_t hdr[4];
    hdr[0] = static_cast<uint8_t>((frame_len >> 24) & 0xFF);
    hdr[1] = static_cast<uint8_t>((frame_len >> 16) & 0xFF);
    hdr[2] = static_cast<uint8_t>((frame_len >> 8) & 0xFF);
    hdr[3] = static_cast<uint8_t>(frame_len & 0xFF);

    // Write header
    {
        const char* ptr = reinterpret_cast<const char*>(hdr);
        size_t remaining = 4;
        while (remaining > 0) {
            ssize_t written = ::write(fd_, ptr, remaining);
            if (written < 0) {
                if (errno == EINTR) continue;
                return false;
            }
            ptr += written;
            remaining -= static_cast<size_t>(written);
        }
    }

    // Write compressed frame
    {
        const char* ptr = reinterpret_cast<const char*>(compressed.data());
        size_t remaining = compressed.size();
        while (remaining > 0) {
            ssize_t written = ::write(fd_, ptr, remaining);
            if (written < 0) {
                if (errno == EINTR) continue;
                return false;
            }
            ptr += written;
            remaining -= static_cast<size_t>(written);
        }
    }

    return true;
}

uint64_t Session::queries_executed() const { return queries_; }
uint64_t Session::inserts_executed() const { return inserts_; }
void Session::increment_queries() { ++queries_; }
void Session::increment_inserts() { ++inserts_; }

void Session::set_compressed(bool c) { compressed_ = c; }
bool Session::is_compressed() const { return compressed_; }

uint64_t Session::commands_executed() const { return command_count_; }
void Session::increment_commands() { ++command_count_; }

uint64_t Session::compress_bytes_in() const { return compress_bytes_in_; }
uint64_t Session::compress_bytes_out() const { return compress_bytes_out_; }

// ── SessionManager ───────────────────────────────────────────────────────────

SessionManager::SessionManager(int max_sessions)
    : max_sessions_(max_sessions) {}

bool SessionManager::add_session(int fd) {
    std::lock_guard<std::mutex> lock(mtx_);
    if (static_cast<int>(sessions_.size()) >= max_sessions_) {
        return false;
    }
    sessions_.emplace(fd, std::make_unique<Session>(fd));
    return true;
}

void SessionManager::remove_session(int fd) {
    std::lock_guard<std::mutex> lock(mtx_);
    ::close(fd);
    sessions_.erase(fd);
}

Session* SessionManager::get_session(int fd) {
    std::lock_guard<std::mutex> lock(mtx_);
    auto it = sessions_.find(fd);
    return (it != sessions_.end()) ? it->second.get() : nullptr;
}

void SessionManager::close_all() {
    std::lock_guard<std::mutex> lock(mtx_);
    for (auto& [fd, session] : sessions_) {
        ::close(fd);
    }
    sessions_.clear();
}

int SessionManager::active_count() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return static_cast<int>(sessions_.size());
}

} // namespace ob
