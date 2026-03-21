#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace ob {

// ── Session ───────────────────────────────────────────────────────────────────
// Per-client connection state.

class Session {
public:
    explicit Session(int fd);

    int fd() const;

    /// Append incoming bytes to read buffer. Returns complete lines (if any).
    std::vector<std::string> feed(const char* data, size_t len);

    /// Send response string to client.
    bool send_response(std::string_view response);

    /// Stats
    uint64_t queries_executed() const;
    uint64_t inserts_executed() const;
    void increment_queries();
    void increment_inserts();

    /// Compression state
    void set_compressed(bool c);
    bool is_compressed() const;

    /// Compression metrics (per-session, aggregated for STATUS)
    uint64_t compress_bytes_in() const;   // total pre-compression (raw) bytes
    uint64_t compress_bytes_out() const;  // total post-compression (wire) bytes

    /// Command counter (tracks total commands executed, used to enforce COMPRESS as first command)
    uint64_t commands_executed() const;
    void increment_commands();

private:
    int         fd_;
    std::string read_buffer_;
    uint64_t    queries_{0};
    uint64_t    inserts_{0};
    bool        compressed_{false};
    uint64_t    command_count_{0};
    uint64_t    compress_bytes_in_{0};   // raw bytes (before compression / after decompression)
    uint64_t    compress_bytes_out_{0};  // wire bytes (after compression / before decompression)
};

// ── SessionManager ────────────────────────────────────────────────────────────
// Manages active sessions. Maps file descriptor → Session.

class SessionManager {
public:
    explicit SessionManager(int max_sessions);

    /// Create session for new connection. Returns false if limit reached.
    bool add_session(int fd);

    /// Remove session on disconnect.
    void remove_session(int fd);

    /// Get session by fd. Returns nullptr if not found.
    Session* get_session(int fd);

    /// Close all sessions gracefully.
    void close_all();

    /// Number of active sessions.
    int active_count() const;

private:
    int max_sessions_;
    std::unordered_map<int, std::unique_ptr<Session>> sessions_;
    mutable std::mutex mtx_;
};

} // namespace ob
