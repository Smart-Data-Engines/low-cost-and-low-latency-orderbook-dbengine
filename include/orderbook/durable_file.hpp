#pragma once

// Files that recovery depends on, written so that a crash or a power cut leaves either the old
// content or the new one - never an empty file, never half of one (#160).
//
// The engine synced only its WAL. A file written beside it with `std::ofstream` reached the device
// whenever the kernel chose, and measured after a power cut `wal_identity` came back **empty**: the
// restart generated a new identity and stopped believing every WAL position its segments recorded.
// The pattern here is the standard one, and each step is there because leaving it out has a named
// failure: the temporary is synced before the rename, or the rename can reach the device before the
// data and the file comes back empty under its new name; the directory is synced after it, or the
// rename itself can be lost.

#include <cerrno>
#include <cstdio>
#include <string>
#include <string_view>

#include <fcntl.h>
#include <unistd.h>

#include "orderbook/logger.hpp"

namespace ob {

/// fsync a directory, so that an entry created or renamed in it survives a power cut. Returns 0, or
/// the errno.
inline int sync_directory(const std::string& dir) {
    const int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) return errno;
    const int rc = ::fsync(fd) == 0 ? 0 : errno;
    ::close(fd);
    return rc;
}

/// Replace `path` with `content` durably and atomically: write `<path>.tmp`, fsync it, rename it
/// over `path`, fsync the directory. Returns 0, or the errno of the step that failed - in which
/// case `path` still holds what it held before, and the temporary is removed.
inline int write_file_atomically(const std::string& path, std::string_view content) {
    const std::string tmp = path + ".tmp";
    // OB_DURABLE: the mechanism - this file is synced below before the rename, and the directory
    // after it.
    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0640);
    if (fd < 0) return errno;
    const char* p = content.data();
    size_t left = content.size();
    int err = 0;
    while (left > 0 && err == 0) {
        const ssize_t n = ::write(fd, p, left);
        if (n < 0) {
            if (errno != EINTR) err = errno;
            continue;
        }
        p += n;
        left -= static_cast<size_t>(n);
    }
    if (err == 0 && ::fsync(fd) != 0) err = errno;
    if (::close(fd) != 0 && err == 0) err = errno;
    if (err == 0 && ::rename(tmp.c_str(), path.c_str()) != 0) err = errno;
    if (err != 0) {
        ::unlink(tmp.c_str());
        OB_LOG_DEBUG("durable_file", "could not replace %s: errno=%d", path.c_str(), err);
        return err;
    }
    const size_t slash = path.find_last_of('/');
    const std::string dir = slash == std::string::npos ? std::string(".")
                          : slash == 0                 ? std::string("/")
                                                       : path.substr(0, slash);
    err = sync_directory(dir);
    OB_LOG_DEBUG("durable_file", "replaced %s (%zu bytes)%s", path.c_str(), content.size(),
                 err != 0 ? ", but its directory could not be synced" : "");
    return err;
}

}  // namespace ob
