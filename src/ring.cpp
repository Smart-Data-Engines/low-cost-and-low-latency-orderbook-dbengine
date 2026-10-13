#include "orderbook/ring.hpp"

#include "orderbook/logger.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <new>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace ob::ring {

bool valid_capacity(uint64_t bytes) {
    return bytes >= kMinBytes && bytes <= kMaxBytes && (bytes & (bytes - 1)) == 0;
}

namespace {

std::string capacity_problem(uint64_t bytes) {
    return "a ring of " + std::to_string(bytes) + " bytes a direction: it takes a power of two from " +
           std::to_string(kMinBytes) + " to " + std::to_string(kMaxBytes);
}

}  // namespace

Mapping Mapping::create(size_t capacity, std::string& why) {
    if (!valid_capacity(capacity)) {
        why = capacity_problem(capacity);
        return {};
    }
    const int fd = ::memfd_create("ob-rings", MFD_CLOEXEC | MFD_ALLOW_SEALING);
    if (fd < 0) {
        why = std::string("memfd_create(): ") + std::strerror(errno);
        return {};
    }
    const size_t total = Layout::total(capacity);
    const auto fail = [&](const char* what) {
        why = std::string(what) + ": " + std::strerror(errno);
        ::close(fd);
        return Mapping{};
    };
    if (::ftruncate(fd, static_cast<off_t>(total)) != 0) return fail("ftruncate()");
    // Sealed before the client holds it: a file it could shrink would leave the server's mapping
    // pointing past the end, and the first access there is SIGBUS for every session at once.
    if (::fcntl(fd, F_ADD_SEALS, F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL) != 0) {
        return fail("sealing the rings' size");
    }
    void* p = ::mmap(nullptr, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) return fail("mmap()");
    char* base = static_cast<char*>(p);
    // The file reads as zeroes; constructing in place is what begins the objects' lifetimes.
    new (base) Block{kMagic, kVersion, 0, capacity};
    new (base + Layout::kToServer) Header();
    new (base + Layout::kToClient) Header();
    OB_LOG_DEBUG("ring", "created rings of %zu bytes a direction, %zu mapped, fd=%d", capacity,
                 total, fd);
    return Mapping(fd, base, capacity);
}

Mapping Mapping::attach(int memfd, size_t capacity, std::string& why) {
    if (!valid_capacity(capacity)) {
        why = capacity_problem(capacity);
        ::close(memfd);
        return {};
    }
    const size_t total = Layout::total(capacity);
    struct stat st{};
    if (::fstat(memfd, &st) != 0) {
        why = std::string("fstat() of the rings' memory: ") + std::strerror(errno);
        ::close(memfd);
        return {};
    }
    if (static_cast<uint64_t>(st.st_size) != total) {
        why = "the rings' memory is " + std::to_string(st.st_size) + " bytes, not the " +
              std::to_string(total) + " that rings of " + std::to_string(capacity) + " take";
        ::close(memfd);
        return {};
    }
    void* p = ::mmap(nullptr, total, PROT_READ | PROT_WRITE, MAP_SHARED, memfd, 0);
    if (p == MAP_FAILED) {
        why = std::string("mmap() of the rings' memory: ") + std::strerror(errno);
        ::close(memfd);
        return {};
    }
    Mapping m(memfd, static_cast<char*>(p), capacity);
    const Block* block = std::launder(reinterpret_cast<const Block*>(p));
    if (block->magic != kMagic || block->version != kVersion || block->capacity != capacity) {
        why = "the memory is not rings of version " + std::to_string(kVersion) + " and " +
              std::to_string(capacity) + " bytes";
        return {};   // `m` unmaps and closes
    }
    OB_LOG_DEBUG("ring", "attached rings of %zu bytes a direction, fd=%d", capacity, memfd);
    return m;
}

Mapping::~Mapping() { release(); }

Mapping::Mapping(Mapping&& other) noexcept
    : fd_(other.fd_), base_(other.base_), capacity_(other.capacity_) {
    other.fd_       = -1;
    other.base_     = nullptr;
    other.capacity_ = 0;
}

Mapping& Mapping::operator=(Mapping&& other) noexcept {
    if (this != &other) {
        release();
        fd_             = other.fd_;
        base_           = other.base_;
        capacity_       = other.capacity_;
        other.fd_       = -1;
        other.base_     = nullptr;
        other.capacity_ = 0;
    }
    return *this;
}

void Mapping::release() {
    if (base_ != nullptr) ::munmap(base_, Layout::total(capacity_));
    if (fd_ >= 0) ::close(fd_);
    base_ = nullptr;
    fd_   = -1;
}

Header* Mapping::to_server() const {
    return std::launder(reinterpret_cast<Header*>(base_ + Layout::kToServer));
}
Header* Mapping::to_client() const {
    return std::launder(reinterpret_cast<Header*>(base_ + Layout::kToClient));
}
char* Mapping::to_server_data() const { return base_ + Layout::kData; }
char* Mapping::to_client_data() const { return base_ + Layout::kData + capacity_; }

// ── Writer ──────────────────────────────────────────────────────────────────────

Writer::Writer(Header* header, char* data, size_t capacity)
    : header_(header), data_(data), capacity_(capacity) {}

size_t Writer::room() const {
    const uint64_t head = header_->head.load(std::memory_order_acquire);
    const uint64_t used = tail_ - head;   // wraps past the capacity when head is ahead of tail
    if (used > capacity_ || head < last_head_) {
        broken_ = true;
        return 0;
    }
    last_head_ = head;
    return static_cast<size_t>(capacity_ - used);
}

size_t Writer::write(const char* p, size_t n) {
    const size_t take = std::min(n, room());
    if (take == 0) return 0;
    const size_t pos   = static_cast<size_t>(tail_ & (capacity_ - 1));
    const size_t first = std::min(take, static_cast<size_t>(capacity_) - pos);
    std::memcpy(data_ + pos, p, first);
    std::memcpy(data_, p + first, take - first);
    tail_ += take;
    return take;
}

bool Writer::publish() {
    header_->tail.store(tail_, std::memory_order_release);
    std::atomic_thread_fence(std::memory_order_seq_cst);
    return header_->consumer_asleep.load(std::memory_order_relaxed) != 0;
}

bool Writer::prepare_to_wait_for_room() {
    header_->producer_asleep.store(1, std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_seq_cst);
    if (room() > 0 || broken_) {
        header_->producer_asleep.store(0, std::memory_order_relaxed);
        return false;
    }
    return true;
}

void Writer::done_waiting_for_room() {
    header_->producer_asleep.store(0, std::memory_order_relaxed);
}

// ── Reader ──────────────────────────────────────────────────────────────────────

Reader::Reader(Header* header, char* data, size_t capacity)
    : header_(header), data_(data), capacity_(capacity) {}

size_t Reader::available() const {
    const uint64_t tail  = header_->tail.load(std::memory_order_acquire);
    const uint64_t ready = tail - head_;   // wraps past the capacity when tail is behind head
    if (ready > capacity_) {
        broken_ = true;
        return 0;
    }
    return static_cast<size_t>(ready);
}

size_t Reader::read(char* p, size_t n) {
    const size_t take = std::min(n, available());
    if (take == 0) return 0;
    const size_t pos   = static_cast<size_t>(head_ & (capacity_ - 1));
    const size_t first = std::min(take, static_cast<size_t>(capacity_) - pos);
    std::memcpy(p, data_ + pos, first);
    std::memcpy(p + first, data_, take - first);
    head_ += take;
    header_->head.store(head_, std::memory_order_release);
    return take;
}

bool Reader::producer_waiting() {
    std::atomic_thread_fence(std::memory_order_seq_cst);
    return header_->producer_asleep.load(std::memory_order_relaxed) != 0;
}

bool Reader::prepare_to_sleep() {
    header_->consumer_asleep.store(1, std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_seq_cst);
    if (available() > 0 || broken_) {
        header_->consumer_asleep.store(0, std::memory_order_relaxed);
        return false;
    }
    return true;
}

void Reader::woke() { header_->consumer_asleep.store(0, std::memory_order_relaxed); }

// ── Bells ───────────────────────────────────────────────────────────────────────

bool ring_bell(int bell_fd) {
    const uint64_t one = 1;
    for (;;) {
        const ssize_t w = ::write(bell_fd, &one, sizeof(one));
        if (w == static_cast<ssize_t>(sizeof(one))) return true;
        if (w < 0 && errno == EINTR) continue;
        // A full counter already holds a wake-up the sleeper has not taken.
        if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return true;
        OB_LOG_WARN("ring", "ringing bell fd=%d failed: %s", bell_fd, std::strerror(errno));
        return false;
    }
}

void drain_bell(int bell_fd) {
    uint64_t count = 0;
    for (;;) {
        const ssize_t r = ::read(bell_fd, &count, sizeof(count));
        if (r < 0 && errno == EINTR) continue;
        return;   // a value, or EAGAIN from a bell nobody rang: either way nothing is pending now
    }
}

}  // namespace ob::ring
