#pragma once

#include "orderbook/tls.hpp"

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

    // ── TLS (#30 part three) ──────────────────────────────────────────────────
    //
    // TLS lives here and nowhere else, for the same reason the authentication gate lives in
    // execute_command(): this class already owns the byte path in both directions, so the event
    // loop keeps saying "data arrived" and "the socket will take a write" without knowing whether
    // anything is encrypted.

    /// What a receive attempt produced. Four outcomes, not three.
    ///
    /// `Again` and `Error` are separate values on purpose. A helper that returns the same thing for
    /// "come back later" and "this connection is finished" makes every failure undiagnosable, and
    /// this repository has paid for that once already (pitfall 81) - with TLS the two are even
    /// easier to conflate, because `SSL_read` reports an incomplete *record* on a readable socket
    /// as WANT_READ rather than as zero bytes.
    enum class IoResult { Data, Closed, Again, Error };

    /// Take bytes off the wire, decrypting when TLS is on.
    ///
    /// The plaintext path is `::read`; the TLS path is `SSL_read`. On `Again` the caller consults
    /// io_want() rather than assuming readability: TLS 1.3 updates keys, so a *read* can need the
    /// socket to become writable.
    IoResult receive(char* buf, size_t len, size_t& out_n);

    /// Attach an SSL object and enter the handshake. Until it completes, feed() gets no bytes.
    ///
    /// Nothing may reach feed() during the handshake: a command arriving before TLS finished would
    /// be a command from an unauthenticated transport, and the gate from part one checks the
    /// *application* identity and knows nothing about TLS.
    void enable_tls(std::shared_ptr<ssl_st> ssl);

    bool tls_enabled() const;
    bool tls_handshaking() const;

    /// Drive one step of the handshake. False means fatal; the reason is logged by the caller.
    bool continue_tls_handshake();

    /// What OpenSSL wants next, so the loop arms the right event rather than the one matching the
    /// operation it just tried. Always `Read` on a plaintext session.
    IoWant io_want() const;

    /// Queue a response and push as much of it to the socket as it accepts now.
    ///
    /// Returns false only on a real error: EPIPE, ECONNRESET, or the send buffer cap
    /// being exceeded. A full socket buffer is NOT an error — the remainder stays
    /// queued and the caller must arm EPOLLOUT. Reading EAGAIN as "the client is
    /// gone" is what closed the session in the middle of every response larger than
    /// the socket buffer, with no log line to say so.
    bool send_response(std::string_view response);

    /// Push queued bytes towards the socket. Same contract as send_response().
    /// Push queued output to the socket. False means the session is finished.
    ///
    /// The branch lives here, in the header, and not inside either half - measured, and the
    /// measurement is the reason. With the TLS loop inlined into one function, Release grew it from
    /// 135 to 310 instructions and turned its prologue from "test, then set up a frame" into "push
    /// six registers, then test", so every *plaintext* response paid for a branch it never takes.
    /// Marking the TLS half `noinline` brought that to 148 instructions and four pushes before the
    /// test - better, and still not the promise the design made, which was that the non-TLS path
    /// pays nothing.
    ///
    /// Dispatching from the header puts the one compare at the call site and keeps the TLS loop
    /// entirely out of the plaintext function - 178 instructions in `flush_output_tls()`, reached
    /// through that compare.
    ///
    /// What is left is not zero and is not TLS. `flush_output_plain()` is **147** instructions
    /// against the 135 it had before this work, and the twelve are the `io_want_` bookkeeping: two
    /// enum stores with their branches, plus four alignment nops. That bookkeeping is the fix for a
    /// regression on this very path - `io_want_` left at `Read` on EAGAIN made the loop disarm
    /// EPOLLOUT with bytes queued, and a 4 MB response stalled - so it is a cost this path had to
    /// take, not one TLS imposed on it. `feed()` and `send_response()` are byte-identical.
    ///
    /// Measured on i3-7100U, Release, GCC 13, two worktrees, `objdump -d --demangle` on
    /// `session.cpp.o` with mnemonics only (pitfall 114). Match the symbol **exactly** when you
    /// repeat it: `flush_output_tls` contains `flush_output`, and a substring match concatenates
    /// the two and reports their sum as the first one's size - which is what the first run of that
    /// comparison did, giving 310 and then 335 for a function that is 148.
    bool flush_output() { return ssl_ == nullptr ? flush_output_plain() : flush_output_tls(); }

private:
    bool flush_output_plain();
    /// Never inlined into its caller: see `flush_output()`.
    [[gnu::noinline]] bool flush_output_tls();

public:

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

    // TLS (#30 part three). Null means plaintext, and then every branch below is the one that
    // existed before - one pointer test on a path that already does a syscall.
    std::shared_ptr<ssl_st> ssl_;
    bool        tls_handshaking_{false};
    IoWant      io_want_{IoWant::Read};

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
