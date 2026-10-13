#pragma once
// A pair of single-producer, single-consumer byte rings in shared memory (#52, step 2). One carries
// a session's commands to the server, the other its answers and pushes back, and an eventfd on
// each side wakes it when it sleeps. They carry the bytes the socket would, so the protocol, the
// parser and the session do not change.
//
// Used by the server and by the C++ client, and including the headers of neither: `client.hpp`
// and the server's headers define the same names in `ob` (pitfall 584), and the server's tests
// talk through rings without the client.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>

namespace ob::ring {

inline constexpr uint64_t kMagic        = 0x0031474E4952424FULL;   // "OBRING1", lowest byte first
inline constexpr uint32_t kVersion      = 1;
inline constexpr size_t   kMinBytes     = size_t{64} * 1024;
inline constexpr size_t   kMaxBytes     = size_t{64} * 1024 * 1024;
inline constexpr size_t   kDefaultBytes = size_t{1024} * 1024;

/// A ring's size a direction: a power of two, from 64 KiB to 64 MiB. A power of two so a counter
/// finds its byte with a mask.
bool valid_capacity(uint64_t bytes);

/// The first bytes of a mapping, written by its creator once.
struct alignas(64) Block {
    uint64_t magic;
    uint32_t version;
    uint32_t reserved;
    uint64_t capacity;   ///< bytes a direction
};

/// One direction's counters, each on a cache line of its own, so the producer's writes do not
/// take the line the consumer reads from under it.
struct alignas(64) Header {
    alignas(64) std::atomic<uint64_t> tail{0};             ///< bytes written, by the producer
    alignas(64) std::atomic<uint64_t> head{0};             ///< bytes read, by the consumer
    alignas(64) std::atomic<uint32_t> consumer_asleep{0};  ///< the consumer sleeps, or is about to
    alignas(64) std::atomic<uint32_t> producer_asleep{0};  ///< the producer waits for room
};

static_assert(sizeof(Block) == 64);
static_assert(sizeof(Header) == 256);
static_assert(std::atomic<uint64_t>::is_always_lock_free, "the counters are shared between processes");
static_assert(std::atomic<uint32_t>::is_always_lock_free, "the flags are shared between processes");

/// Where a mapping keeps what. Commands go to the server, answers to the client.
struct Layout {
    static constexpr size_t kToServer = sizeof(Block);
    static constexpr size_t kToClient = kToServer + sizeof(Header);
    static constexpr size_t kData     = kToClient + sizeof(Header);
    static constexpr size_t total(size_t capacity) { return kData + 2 * capacity; }
};

/// A mapping of a ring pair. Move-only; unmaps and closes its descriptor when it goes.
class Mapping {
public:
    /// The server's side: a memfd of the pair's size, sealed against shrinking and growing - the
    /// client holds the same file, and memory a mapping lost would end the server in SIGBUS - and
    /// mapped and initialised. An invalid mapping and `why` on failure.
    static Mapping create(size_t capacity, std::string& why);

    /// The client's side: map the memfd it was given, and check it is a ring pair of `capacity`.
    /// Takes the descriptor either way.
    static Mapping attach(int memfd, size_t capacity, std::string& why);

    Mapping() = default;
    ~Mapping();
    Mapping(Mapping&& other) noexcept;
    Mapping& operator=(Mapping&& other) noexcept;
    Mapping(const Mapping&) = delete;
    Mapping& operator=(const Mapping&) = delete;

    bool   valid() const { return base_ != nullptr; }
    int    fd() const { return fd_; }
    size_t capacity() const { return capacity_; }
    size_t bytes() const { return Layout::total(capacity_); }

    Header* to_server() const;
    Header* to_client() const;
    char*   to_server_data() const;
    char*   to_client_data() const;

private:
    Mapping(int fd, char* base, size_t capacity) : fd_(fd), base_(base), capacity_(capacity) {}
    void release();

    int    fd_{-1};
    char*  base_{nullptr};
    size_t capacity_{0};
};

/// The producing end of one direction. Keeps its own copy of the counter it writes, and reads the
/// other side's only to check it: what the other process put in shared memory is not trusted.
class Writer {
public:
    Writer() = default;
    Writer(Header* header, char* data, size_t capacity);

    /// Copies as much of [p, p + n) as there is room for, and returns how much. Nothing is visible
    /// to the consumer until `publish()`.
    size_t write(const char* p, size_t n);

    /// Makes what was written visible, and answers whether the consumer sleeps - the caller rings
    /// its bell then. A seq_cst fence between the two, so a consumer about to sleep either sees the
    /// bytes or is seen asleep (pitfall 582: two relaxed stores and loads pass in a store buffer).
    bool publish();

    /// Room for more, by the consumer's counter; 0 also when that counter is not one this side
    /// could have left (`broken()` then says so).
    size_t room() const;

    /// About to sleep on a full ring: says so, then looks again. True: still full, sleep. False:
    /// room appeared in between, and the flag is down again.
    bool prepare_to_wait_for_room();
    void done_waiting_for_room();

    /// Whether the consumer's counter contradicts this side's: more read than was ever written,
    /// or read going backwards. The session cannot go on.
    bool broken() const { return broken_; }
    uint64_t written() const { return tail_; }

private:
    Header*  header_{nullptr};
    char*    data_{nullptr};
    uint64_t capacity_{0};
    uint64_t tail_{0};
    mutable uint64_t last_head_{0};
    mutable bool     broken_{false};
};

/// The consuming end of one direction, with the same distrust of the producer's counter.
class Reader {
public:
    Reader() = default;
    Reader(Header* header, char* data, size_t capacity);

    /// Bytes ready to read; 0 also when the producer's counter is not one it could have left.
    size_t available() const;

    /// Copies up to `n` bytes out and gives their room back to the producer. Copied before anyone
    /// parses them, so the producer cannot change a command while it is read.
    size_t read(char* p, size_t n);

    /// After a read: whether the producer waits for room - the caller rings its bell then. The
    /// same fence as `Writer::publish()`, the other way round.
    bool producer_waiting();

    /// About to sleep on an empty ring: says so, then looks again. True: still empty, sleep.
    /// False: bytes arrived in between, and the flag is down again.
    bool prepare_to_sleep();
    void woke();

    bool broken() const { return broken_; }
    uint64_t consumed() const { return head_; }

private:
    Header*  header_{nullptr};
    char*    data_{nullptr};
    uint64_t capacity_{0};
    uint64_t head_{0};
    mutable bool broken_{false};
};

/// Ring an eventfd: one wake-up whatever its count. False only for an error other than a full
/// counter, which already means a wake-up is pending.
bool ring_bell(int bell_fd);

/// Take every pending ring of an eventfd, so the next one wakes its sleeper again.
void drain_bell(int bell_fd);

}  // namespace ob::ring
