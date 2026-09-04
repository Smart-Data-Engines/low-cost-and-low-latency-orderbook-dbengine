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
    Session(int fd, uint64_t conn_id = 0);

    int fd() const;

    /// Connection identity, monotonic per server run.
    ///
    /// Descriptor numbers are reused, so anything that outlives one connection and refers to it by
    /// `fd` alone eventually refers to somebody else. A subscription is exactly that kind of thing:
    /// pinned to `fd`, it would push rows to whoever inherits the number. Same reasoning, and same
    /// name, as `PeerConnection::conn_id` in the multi-master path.
    uint64_t conn_id() const;

    /// Append incoming bytes to read buffer. Returns complete lines (if any).
    std::vector<std::string> feed(const char* data, size_t len);

    /// Queue a response and push as much of it to the socket as it accepts now.
    ///
    /// Returns false only on a real error: EPIPE, ECONNRESET, or the send buffer cap
    /// being exceeded. A full socket buffer is NOT an error — the remainder stays
    /// queued and the caller must arm EPOLLOUT. Reading EAGAIN as "the client is
    /// gone" is what closed the session in the middle of every response larger than
    /// the socket buffer, with no log line to say so.
    bool send_response(std::string_view response);

    /// Push queued bytes towards the socket. Same contract as send_response().
    bool flush_output();

    /// True while bytes are still queued, so the caller must keep EPOLLOUT armed.
    bool has_pending_output() const;

    /// Bytes still queued. For logging and for the pending-bytes gauge.
    size_t pending_output_bytes() const;

    /// Close once the queue drains — QUIT arriving while a response is in flight.
    void request_close_after_flush();
    bool close_requested() const;

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

    // ── Authentication state (#30) ────────────────────────────────────────────
    //
    // The session holds *its own* authentication state and deliberately not the server's secret.
    // Verification happens in execute_command(), which is handed the credential store; a Session
    // holding a secret would put credential material into every object the epoll loop copies,
    // logs or dumps for diagnostics.

    /// True once this connection has answered a challenge correctly.
    bool authenticated() const;

    /// Who this connection authenticated as. Empty while unauthenticated.
    const std::string& identity() const;

    void set_authenticated(std::string identity);

    /// The outstanding challenge, or empty when none is outstanding.
    ///
    /// Single-use in both directions: issuing a new challenge replaces it, so a response to the
    /// previous one no longer verifies, and a successful response clears it.
    const std::string& pending_nonce() const;
    void set_pending_nonce(std::string nonce);

    /// Failed attempts on this connection. For the log line, which is the only place it is read:
    /// the session is closed on the first failure, so this counts at most one per connection —
    /// and a value above one would mean the close stopped working.
    uint32_t auth_attempts() const;
    void increment_auth_attempts();

private:
    int         fd_;
    uint64_t    conn_id_;
    std::string read_buffer_;

    /// Bytes accepted from execute_command() but not yet taken by the socket.
    /// Holds already-framed bytes, so in compressed mode a partial write cannot
    /// split an LZ4 frame: the next flush resumes exactly where it stopped.
    std::string send_buf_;
    bool        close_after_flush_{false};

    /// A slow client asking for huge scans must not grow the server's memory without
    /// bound; past this, the session is closed with a logged reason. 64 MB is about
    /// 1.7 million rows of response, far beyond a sensible query without LIMIT, and
    /// generous enough that merely reading slowly never reaches it.
    static constexpr size_t kMaxSendBuffer = 64u * 1024u * 1024u;
    uint64_t    queries_{0};
    uint64_t    inserts_{0};
    bool        compressed_{false};
    uint64_t    command_count_{0};
    uint64_t    compress_bytes_in_{0};   // raw bytes (before compression / after decompression)
    uint64_t    compress_bytes_out_{0};  // wire bytes (after compression / before decompression)

    // Authentication (#30)
    bool        authenticated_{false};
    std::string identity_;
    std::string pending_nonce_;
    uint32_t    auth_attempts_{0};

    // MINSERT multi-line buffering state
    bool        minsert_pending_{false};
    uint16_t    minsert_expected_{0};     // expected number of payload lines
    std::string minsert_header_;          // saved header line
    std::vector<std::string> minsert_lines_; // collected payload lines
};

// ── SessionManager ────────────────────────────────────────────────────────────
// Manages active sessions. Maps file descriptor → Session.

class SessionManager {
public:
    explicit SessionManager(int max_sessions);

    /// Create session for new connection. Returns false if limit reached.
    ///
    /// `conn_id` defaults to 0 so existing callers and tests are unaffected; the server passes a
    /// real one. Zero means "not tracked", which is honest for a caller that does not need it.
    bool add_session(int fd, uint64_t conn_id = 0);

    /// Remove session on disconnect.
    void remove_session(int fd);

    /// Get session by fd. Returns nullptr if not found.
    Session* get_session(int fd);

    /// Close all sessions gracefully.
    void close_all();

    /// Number of active sessions.
    int active_count() const;

    /// Bytes queued for sending across all sessions.
    ///
    /// The operator-facing view of a slow client: output piles up here long before a
    /// session reaches its cap, so this is the signal that someone is not reading.
    size_t total_pending_output_bytes() const;

private:
    int max_sessions_;
    std::unordered_map<int, std::unique_ptr<Session>> sessions_;
    mutable std::mutex mtx_;
};

} // namespace ob
