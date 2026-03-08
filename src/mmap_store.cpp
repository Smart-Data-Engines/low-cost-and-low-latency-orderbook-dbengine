#include "orderbook/mmap_store.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>

namespace ob {

namespace {

[[noreturn]] void throw_errno(const char* msg) {
    throw std::runtime_error(std::string(msg) + ": " + std::strerror(errno));
}

} // namespace

void MmapStore::open(std::string_view path, size_t initial_size) {
    path_ = std::string(path);

    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0) {
        throw_errno("MmapStore::open: open(2) failed");
    }

    // Determine the size to map: use the larger of initial_size and the
    // existing file size so we never truncate data that is already there.
    struct stat st{};
    if (::fstat(fd_, &st) < 0) {
        ::close(fd_);
        fd_ = -1;
        throw_errno("MmapStore::open: fstat failed");
    }

    size_t file_size = static_cast<size_t>(st.st_size);
    size_t map_size  = (file_size > initial_size) ? file_size : initial_size;

    if (::ftruncate(fd_, static_cast<off_t>(map_size)) < 0) {
        ::close(fd_);
        fd_ = -1;
        throw_errno("MmapStore::open: ftruncate failed");
    }

    void* addr = ::mmap(nullptr, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (addr == MAP_FAILED) {
        ::close(fd_);
        fd_ = -1;
        throw_errno("MmapStore::open: mmap failed");
    }

    map_          = addr;
    mapped_size_  = map_size;
    // If the file already had data, preserve the cursor at the old file size
    // so callers can continue appending after a reopen.
    write_cursor_.store(file_size, std::memory_order_relaxed);
}

void* MmapStore::write_ptr() {
    return static_cast<uint8_t*>(map_) + write_cursor_.load(std::memory_order_relaxed);
}

size_t MmapStore::advance(size_t bytes) {
    size_t cur = write_cursor_.load(std::memory_order_relaxed);
    if (cur + bytes > mapped_size_) {
        // Double the mapped size until it fits.
        size_t new_size = mapped_size_ * 2;
        while (cur + bytes > new_size) {
            new_size *= 2;
        }
        remap(new_size);
    }
    write_cursor_.fetch_add(bytes, std::memory_order_relaxed);
    return cur;
}

void MmapStore::flush() {
    if (map_ != nullptr) {
        if (::msync(map_, mapped_size_, MS_SYNC) < 0) {
            throw_errno("MmapStore::flush: msync failed");
        }
    }
}

void MmapStore::close() {
    if (map_ != nullptr) {
        // Best-effort flush before unmapping.
        ::msync(map_, mapped_size_, MS_SYNC);
        ::munmap(map_, mapped_size_);
        map_         = nullptr;
        mapped_size_ = 0;
    }
    if (fd_ >= 0) {
        // Truncate the file to the actual bytes written so that on reopen
        // we can restore the write cursor from the file size.
        size_t cursor = write_cursor_.load(std::memory_order_relaxed);
        if (::ftruncate(fd_, static_cast<off_t>(cursor)) != 0) { /* best-effort */ }
        ::close(fd_);
        fd_ = -1;
    }
    write_cursor_.store(0, std::memory_order_relaxed);
    path_.clear();
}

size_t MmapStore::size() const {
    return write_cursor_.load(std::memory_order_relaxed);
}

const void* MmapStore::data() const {
    return map_;
}

void MmapStore::remap(size_t new_size) {
    if (::munmap(map_, mapped_size_) < 0) {
        throw_errno("MmapStore::remap: munmap failed");
    }
    map_         = nullptr;
    mapped_size_ = 0;

    if (::ftruncate(fd_, static_cast<off_t>(new_size)) < 0) {
        throw_errno("MmapStore::remap: ftruncate failed");
    }

    void* addr = ::mmap(nullptr, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (addr == MAP_FAILED) {
        throw_errno("MmapStore::remap: mmap failed");
    }

    map_         = addr;
    mapped_size_ = new_size;
}

} // namespace ob
