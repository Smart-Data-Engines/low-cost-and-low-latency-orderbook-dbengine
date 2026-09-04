#include "orderbook/session.hpp"
#include "orderbook/compression.hpp"

#include <openssl/err.h>
#include <openssl/ssl.h>
#include "orderbook/logger.hpp"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <cstring>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

namespace ob {

// ── Session ──────────────────────────────────────────────────────────────────

Session::Session(int fd, uint64_t conn_id) : fd_(fd), conn_id_(conn_id) {}

int Session::fd() const { return fd_; }

uint64_t Session::conn_id() const { return conn_id_; }

std::vector<std::string> Session::feed(const char* data, size_t len) {
    read_buffer_.append(data, len);

    if (!compressed_) {
        // Newline-delimited text mode
        std::vector<std::string> result;
        size_t pos = 0;
        while (true) {
            auto nl = read_buffer_.find('\n', pos);
            if (nl == std::string::npos) break;
            std::string line(read_buffer_, pos, nl - pos);
            pos = nl + 1;

            if (minsert_pending_) {
                // Collecting payload lines for an in-progress MINSERT
                minsert_lines_.push_back(std::move(line));
                if (minsert_lines_.size() == static_cast<size_t>(minsert_expected_)) {
                    // Assemble complete multi-line block
                    std::string block = minsert_header_;
                    for (const auto& pl : minsert_lines_) {
                        block += '\n';
                        block += pl;
                    }
                    result.push_back(std::move(block));
                    // Reset MINSERT state
                    minsert_pending_ = false;
                    minsert_expected_ = 0;
                    minsert_header_.clear();
                    minsert_lines_.clear();
                }
            } else {
                // Check if this line starts with MINSERT (case-insensitive)
                bool is_minsert = false;
                if (line.size() >= 7) {
                    // Check prefix "MINSERT" case-insensitively
                    is_minsert = true;
                    const char* minsert_kw = "MINSERT";
                    for (int i = 0; i < 7; ++i) {
                        if (std::toupper(static_cast<unsigned char>(line[i])) != minsert_kw[i]) {
                            is_minsert = false;
                            break;
                        }
                    }
                    // Must be followed by space or end of line
                    if (is_minsert && line.size() > 7 && line[7] != ' ' && line[7] != '\t') {
                        is_minsert = false;
                    }
                }

                if (is_minsert) {
                    // Parse n_levels from the header: tokenize, take token[4]
                    // Tokenize: split on whitespace
                    uint16_t n_levels = 0;
                    size_t ti = 0;
                    int token_idx = 0;
                    while (ti < line.size() && token_idx < 5) {
                        while (ti < line.size() && (line[ti] == ' ' || line[ti] == '\t')) ++ti;
                        if (ti >= line.size()) break;
                        size_t start = ti;
                        while (ti < line.size() && line[ti] != ' ' && line[ti] != '\t') ++ti;
                        if (token_idx == 4) {
                            // Parse n_levels
                            const char* begin = line.data() + start;
                            const char* end = line.data() + ti;
                            std::from_chars(begin, end, n_levels);
                        }
                        ++token_idx;
                    }

                    if (n_levels > 0) {
                        minsert_pending_ = true;
                        minsert_expected_ = n_levels;
                        minsert_header_ = std::move(line);
                        minsert_lines_.clear();
                    } else {
                        // n_levels=0 or parse failure — pass through as-is
                        result.push_back(std::move(line));
                    }
                } else {
                    result.push_back(std::move(line));
                }
            }
        }
        if (pos > 0) {
            read_buffer_.erase(0, pos);
        }
        return result;
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

// ── TLS (#30 part three) ──────────────────────────────────────────────────────

void Session::enable_tls(std::shared_ptr<ssl_st> ssl) {
    ssl_             = std::move(ssl);
    tls_handshaking_ = true;
    io_want_         = IoWant::Read;
}

bool Session::tls_enabled() const { return ssl_ != nullptr; }

bool Session::tls_handshaking() const { return tls_handshaking_; }

IoWant Session::io_want() const { return io_want_; }

bool Session::continue_tls_handshake() {
    if (!tls_handshaking_) return true;

    ERR_clear_error();
    const int rc = SSL_accept(ssl_.get());
    if (rc == 1) {
        tls_handshaking_ = false;
        io_want_         = IoWant::Read;
        OB_LOG_INFO("tls", "handshake complete: fd=%d conn_id=%llu version=%s cipher=%s",
                    fd_, static_cast<unsigned long long>(conn_id_),
                    SSL_get_version(ssl_.get()), SSL_get_cipher(ssl_.get()));
        return true;
    }

    const int err = SSL_get_error(ssl_.get(), rc);
    if (err == SSL_ERROR_WANT_READ) {
        io_want_ = IoWant::Read;
        return true;
    }
    if (err == SSL_ERROR_WANT_WRITE) {
        // The handshake has bytes to send. The caller arms EPOLLOUT for this, which is why io_want
        // exists: the loop cannot infer it from the operation it attempted.
        io_want_ = IoWant::Write;
        return true;
    }
    OB_LOG_WARN("tls", "handshake failed: fd=%d conn_id=%llu %s: %s",
                fd_, static_cast<unsigned long long>(conn_id_),
                tls_error_name(err), tls_last_error().c_str());
    return false;
}

Session::IoResult Session::receive(char* buf, size_t len, size_t& out_n) {
    out_n = 0;

    if (ssl_ == nullptr) {
        const ssize_t n = ::read(fd_, buf, len);
        if (n > 0) { out_n = static_cast<size_t>(n); io_want_ = IoWant::Read; return IoResult::Data; }
        if (n == 0) return IoResult::Closed;
        if (errno == EAGAIN || errno == EWOULDBLOCK) { io_want_ = IoWant::Read; return IoResult::Again; }
        if (errno == EINTR) { io_want_ = IoWant::Read; return IoResult::Again; }
        return IoResult::Error;
    }

    ERR_clear_error();
    const int n = SSL_read(ssl_.get(), buf, static_cast<int>(len));
    if (n > 0) {
        out_n    = static_cast<size_t>(n);
        io_want_ = IoWant::Read;
        return IoResult::Data;
    }

    const int err = SSL_get_error(ssl_.get(), n);
    switch (err) {
    case SSL_ERROR_WANT_READ:
        io_want_ = IoWant::Read;
        return IoResult::Again;
    case SSL_ERROR_WANT_WRITE:
        // A *read* that needs the socket to become writable: TLS 1.3 key updates do this. Arming
        // readability here would wait for an event that is not coming.
        io_want_ = IoWant::Write;
        return IoResult::Again;
    case SSL_ERROR_ZERO_RETURN:
        // close_notify received: an orderly shutdown, and the only thing that means end of stream.
        // An incomplete record on a readable socket is WANT_READ above, not this - reading that as
        // "the client is gone" is pitfall 11 wearing a different hat.
        return IoResult::Closed;
    default:
        OB_LOG_WARN("tls", "read failed: fd=%d conn_id=%llu %s: %s",
                    fd_, static_cast<unsigned long long>(conn_id_),
                    tls_error_name(err), tls_last_error().c_str());
        return IoResult::Error;
    }
}

bool Session::send_response(std::string_view response) {
    if (compressed_) {
        // Compressed mode: [4-byte BE length][LZ4 frame]. Framed before queueing, so
        // a partial write can never split a frame.
        auto compressed = lz4_compress(response.data(), response.size());

        // Compression metrics: raw bytes in, wire bytes out.
        compress_bytes_in_  += static_cast<uint64_t>(response.size());
        compress_bytes_out_ += static_cast<uint64_t>(compressed.size());

        uint32_t frame_len = static_cast<uint32_t>(compressed.size());
        if (send_buf_.size() + 4 + compressed.size() > kMaxSendBuffer) {
            OB_LOG_ERROR("session",
                         "Send buffer cap exceeded: fd=%d pending=%zu adding=%zu cap=%zu",
                         fd_, send_buf_.size(), 4 + compressed.size(), kMaxSendBuffer);
            return false;
        }
        send_buf_.push_back(static_cast<char>((frame_len >> 24) & 0xFF));
        send_buf_.push_back(static_cast<char>((frame_len >> 16) & 0xFF));
        send_buf_.push_back(static_cast<char>((frame_len >> 8) & 0xFF));
        send_buf_.push_back(static_cast<char>(frame_len & 0xFF));
        send_buf_.append(reinterpret_cast<const char*>(compressed.data()),
                         compressed.size());
    } else {
        if (send_buf_.size() + response.size() > kMaxSendBuffer) {
            OB_LOG_ERROR("session",
                         "Send buffer cap exceeded: fd=%d pending=%zu adding=%zu cap=%zu",
                         fd_, send_buf_.size(), response.size(), kMaxSendBuffer);
            return false;
        }
        send_buf_.append(response.data(), response.size());
    }

    return flush_output();
}

bool Session::flush_output() {
    size_t sent_total = 0;

    if (ssl_ != nullptr) {
        if (tls_handshaking_) {
            // Reached before a single byte has arrived from the client: the banner is queued at
            // accept and `send_response()` flushes. `SSL_write` here would begin the handshake from
            // the write path, so two functions would be advancing one state machine while
            // `tls_handshaking_` still said the handshake had not started. It happens to work -
            // `SSL_accept` continues whatever `SSL_write` began - and "works because both callers
            // are reentrant into the same state machine" is not a property to build on. The
            // handshake has one owner, and the loop flushes the banner the moment it completes.
            return true;
        }
        while (sent_total < send_buf_.size()) {
            ERR_clear_error();
            const int n = SSL_write(ssl_.get(), send_buf_.data() + sent_total,
                                    static_cast<int>(send_buf_.size() - sent_total));
            if (n > 0) {
                sent_total += static_cast<size_t>(n);
                io_want_ = IoWant::Read;
                continue;
            }
            const int err = SSL_get_error(ssl_.get(), n);
            if (err == SSL_ERROR_WANT_WRITE) { io_want_ = IoWant::Write; break; }
            if (err == SSL_ERROR_WANT_READ) {
                // A *write* that needs to read first - a TLS 1.3 key update. Arming EPOLLOUT here
                // would spin: the socket is writable and OpenSSL is not asking for that.
                io_want_ = IoWant::Read;
                break;
            }
            if (sent_total > 0) send_buf_.erase(0, sent_total);
            OB_LOG_WARN("tls", "write failed: fd=%d conn_id=%llu %s: %s pending=%zu",
                        fd_, static_cast<unsigned long long>(conn_id_),
                        tls_error_name(err), tls_last_error().c_str(), send_buf_.size());
            return false;
        }
        // The erase is what makes SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER load-bearing: it moves the
        // pending bytes to a different address, and a retry presenting a different address is
        // refused with `bad write retry` without that mode. Measured -
        // benchmarks/tls/ssl_write_retry.c.
        if (sent_total > 0) send_buf_.erase(0, sent_total);
        // Stated rather than inferred, the same line as the plaintext path below. Today the loop
        // body always runs when there is data and leaves `Read` behind, so this is redundant - and
        // that is exactly the kind of correctness that survives by circumstance: called with an
        // already-empty buffer this function would keep a stale `Write`, the loop would leave
        // EPOLLOUT armed on an edge-triggered descriptor with nothing to send, and pitfall 5 says
        // what that costs. Cheap enough to make unconditional (pitfall 63).
        if (send_buf_.empty()) io_want_ = IoWant::Read;
        return true;
    }

    while (sent_total < send_buf_.size()) {
        // send() with MSG_NOSIGNAL, not write(): a client that disconnects while we
        // are writing raises SIGPIPE, whose default action kills the whole server
        // process — every other session with it. Every other socket writer in this
        // repository already does this; this one did not.
        ssize_t written = ::send(fd_, send_buf_.data() + sent_total,
                                 send_buf_.size() - sent_total, MSG_NOSIGNAL);
        if (written > 0) {
            sent_total += static_cast<size_t>(written);
            continue;
        }

        if (written < 0) {
            const int err = errno;
            if (err == EINTR) continue;
            if (err == EAGAIN || err == EWOULDBLOCK) {
                // The socket buffer is full. Not an error: the caller arms EPOLLOUT
                // and the rest goes out when the client drains it.
                //
                // io_want_ has to say so, and on the plaintext path too. It was left at Read here,
                // and the loop's "disarm unless OpenSSL wants to write" then disarmed EPOLLOUT with
                // bytes still queued - nothing re-armed it, because a client waiting for a response
                // sends nothing, so a 4 MB response stalled and the reader timed out.
                // `test_slow_reader_is_not_disconnected` caught it, which is what that test is for.
                io_want_ = IoWant::Write;
                break;
            }
            if (sent_total > 0) send_buf_.erase(0, sent_total);
            OB_LOG_WARN("session",
                        "Send failed: fd=%d errno=%s pending=%zu",
                        fd_, std::strerror(err), send_buf_.size());
            return false;
        }

        // written == 0: nothing accepted, treat like EAGAIN rather than spinning.
        break;
    }

    if (sent_total > 0) send_buf_.erase(0, sent_total);
    // Fully drained: nothing to wait for on the write side.
    if (send_buf_.empty()) io_want_ = IoWant::Read;
    return true;
}

bool Session::has_pending_output() const { return !send_buf_.empty(); }

size_t Session::pending_output_bytes() const { return send_buf_.size(); }

void Session::request_close_after_flush() { close_after_flush_ = true; }

bool Session::close_requested() const { return close_after_flush_; }

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

bool SessionManager::add_session(int fd, uint64_t conn_id) {
    std::lock_guard<std::mutex> lock(mtx_);
    if (static_cast<int>(sessions_.size()) >= max_sessions_) {
        return false;
    }
    sessions_.emplace(fd, std::make_unique<Session>(fd, conn_id));
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

size_t SessionManager::total_pending_output_bytes() const {
    std::lock_guard<std::mutex> lock(mtx_);
    size_t total = 0;
    for (const auto& entry : sessions_) {
        if (entry.second) total += entry.second->pending_output_bytes();
    }
    return total;
}


// ── Authentication state (#30) ────────────────────────────────────────────────

bool Session::authenticated() const { return authenticated_; }

const std::string& Session::identity() const { return identity_; }

void Session::set_authenticated(std::string identity) {
    authenticated_ = true;
    identity_      = std::move(identity);
    // The challenge has been spent. Leaving it in place would let the same response be replayed on
    // this connection, which matters less than the cross-connection case but costs nothing to close.
    pending_nonce_.clear();
}

const std::string& Session::pending_nonce() const { return pending_nonce_; }

void Session::set_pending_nonce(std::string nonce) { pending_nonce_ = std::move(nonce); }

uint32_t Session::auth_attempts() const { return auth_attempts_; }

void Session::increment_auth_attempts() { ++auth_attempts_; }

} // namespace ob
