#pragma once

#ifdef OB_USE_IO_URING

#include "orderbook/tcp_server.hpp"  // for ServerConfig, ServerStats, execute_command, etc.

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

struct io_uring;  // forward declaration (liburing)

namespace ob {

/// Typy operacji io_uring zakodowane w user_data SQE/CQE.
enum class IoOp : uint8_t {
    ACCEPT = 0,
    READ   = 1,
    WRITE  = 2,
};

class IoUringServer {
public:
    explicit IoUringServer(ServerConfig config);
    ~IoUringServer();

    // Non-copyable, non-movable
    IoUringServer(const IoUringServer&) = delete;
    IoUringServer& operator=(const IoUringServer&) = delete;

    /// Uruchom serwer: otwórz Engine, bind socket, wejdź w pętlę io_uring.
    /// Blokuje do wywołania shutdown().
    void run();

    /// Sygnalizuj zatrzymanie (thread-safe, wywoływane z signal handlera).
    void shutdown();

    // ── Kodowanie/dekodowanie user_data (public for testing) ─────────
    static uint64_t encode_user_data(IoOp op, int fd);
    static IoOp     decode_op(uint64_t user_data);
    static int      decode_fd(uint64_t user_data);

    // ── Zarządzanie buforami (public for testing) ────────────────────
    int   alloc_buffer();          // zwraca indeks lub -1
    void  free_buffer(int idx);
    char* buffer_ptr(int idx);

private:
    ServerConfig             config_;
    std::unique_ptr<Engine>  engine_;
    std::unique_ptr<MetricsServer> metrics_server_;
    std::atomic<bool>        running_{false};
    std::atomic<bool>        draining_{false};
    std::atomic<bool>        read_only_{false};  // dynamic read-only flag, toggled by failover

    int listen_fd_{-1};

    // io_uring
    struct io_uring* ring_{nullptr};
    bool sqpoll_enabled_{false};
    bool buffers_registered_{false};

    // Buffer pool: max_sessions buforów po 4KB
    std::vector<char>    buffer_pool_;       // ciągły blok: max_sessions * 4096
    std::vector<bool>    buffer_in_use_;     // które sloty są zajęte
    static constexpr size_t BUFFER_SIZE = 4096;

    // Mapowanie fd → buffer index (dla zarejestrowanych buforów)
    std::vector<int> fd_to_buf_idx_;  // indeksowane przez fd, wartość = buf index (-1 = brak)

    // Pending write data: keeps response strings alive until io_uring WRITE completes.
    // Key = fd, value = concatenated response data awaiting kernel send.
    std::unordered_map<int, std::string> pending_writes_;

    // Sesje i statystyki
    std::unique_ptr<SessionManager> session_mgr_;
    ServerStats stats_;

    // ── Inicjalizacja ────────────────────────────────────────────────
    void init_ring();
    void register_buffers();
    void submit_accept();

    // ── Handlery CQE ─────────────────────────────────────────────────
    void handle_accept(int client_fd);
    void handle_read(int fd, int bytes_read);
    void handle_write(int fd, int bytes_written);

    // ── Składanie SQE ────────────────────────────────────────────────
    void submit_read(int fd);
    void submit_write(int fd, const char* data, size_t len);
};

} // namespace ob

#endif // OB_USE_IO_URING
