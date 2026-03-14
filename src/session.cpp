#include "orderbook/session.hpp"

#include <algorithm>
#include <cerrno>
#include <unistd.h>

namespace ob {

// ── Session ──────────────────────────────────────────────────────────────────

Session::Session(int fd) : fd_(fd) {}

int Session::fd() const { return fd_; }

std::vector<std::string> Session::feed(const char* data, size_t len) {
    read_buffer_.append(data, len);

    std::vector<std::string> lines;
    size_t pos = 0;
    while (true) {
        auto nl = read_buffer_.find('\n', pos);
        if (nl == std::string::npos) break;
        lines.emplace_back(read_buffer_, pos, nl - pos);
        pos = nl + 1;
    }

    // Keep any incomplete trailing data
    if (pos > 0) {
        read_buffer_.erase(0, pos);
    }

    return lines;
}

bool Session::send_response(std::string_view response) {
    const char* ptr = response.data();
    size_t remaining = response.size();

    while (remaining > 0) {
        ssize_t written = ::write(fd_, ptr, remaining);
        if (written < 0) {
            if (errno == EINTR) continue;
            // EPIPE or ECONNRESET — client gone
            return false;
        }
        ptr += written;
        remaining -= static_cast<size_t>(written);
    }
    return true;
}

uint64_t Session::queries_executed() const { return queries_; }
uint64_t Session::inserts_executed() const { return inserts_; }
void Session::increment_queries() { ++queries_; }
void Session::increment_inserts() { ++inserts_; }

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
