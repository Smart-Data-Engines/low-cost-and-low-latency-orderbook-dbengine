#pragma once

#include <atomic>
#include <cstddef>
#include <string>
#include <string_view>

namespace ob {

/// Memory-mapped file store for columnar segment data.
///
/// Supports files up to 16 GB on 64-bit OS. Non-copyable, non-movable.
class MmapStore {
public:
    MmapStore() = default;
    ~MmapStore() { close(); }

    // Non-copyable, non-movable (owns fd and mmap region)
    MmapStore(const MmapStore&)            = delete;
    MmapStore& operator=(const MmapStore&) = delete;
    MmapStore(MmapStore&&)                 = delete;
    MmapStore& operator=(MmapStore&&)      = delete;

    /// Opens or creates a column file; extends mapping as needed.
    /// If the file already exists and is larger than initial_size, the existing
    /// file size is used.
    void open(std::string_view path, size_t initial_size = 64ULL << 20 /* 64 MB */);

    /// Returns pointer to the current append position (write_cursor_).
    void* write_ptr();

    /// Atomically advances the write cursor by bytes.
    /// Calls remap() first if the new cursor would exceed mapped_size_.
    /// Returns the old cursor value (the write position for the reserved bytes).
    size_t advance(size_t bytes);

    /// Flushes dirty pages to disk via msync(MS_SYNC).
    void flush();

    /// Flushes, unmaps, closes the fd, and resets all members.
    void close();

    /// Returns the current write cursor position (bytes written so far).
    size_t size() const;

    /// Returns a pointer to the start of the mapped region.
    const void* data() const;

private:
    void* map_                          = nullptr;
    size_t mapped_size_                 = 0;
    std::atomic<size_t> write_cursor_{0};
    int fd_                             = -1;
    std::string path_;

    /// Remaps the file to new_size: munmap → ftruncate → mmap.
    void remap(size_t new_size);
};

} // namespace ob
