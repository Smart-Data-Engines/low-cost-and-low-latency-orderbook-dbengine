#include "orderbook/wal_archive.hpp"

#include "orderbook/crc32c.hpp"
#include "orderbook/durable_file.hpp"
#include "orderbook/logger.hpp"
#include "orderbook/metrics.hpp"
#include "orderbook/wal.hpp"
#include "orderbook/wall_clock.hpp"

#include <fcntl.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

namespace ob {

namespace {

constexpr size_t kBufferBytes = 1u << 20;

/// The size and the CRC32C of `path`, read whole. False and why when it cannot be read.
bool size_and_crc(const std::string& path, std::vector<uint8_t>& buf, uint64_t& size, uint32_t& crc,
                  std::string& error) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        error = "cannot open " + path + ": " + std::strerror(errno);
        return false;
    }
    uint32_t running = crc32c_init;
    uint64_t bytes = 0;
    for (;;) {
        const ssize_t n = ::read(fd, buf.data(), buf.size());
        if (n < 0) {
            if (errno == EINTR) continue;
            error = "cannot read " + path + ": " + std::strerror(errno);
            ::close(fd);
            return false;
        }
        if (n == 0) break;
        running = crc32c_update(running, buf.data(), static_cast<size_t>(n));
        bytes += static_cast<uint64_t>(n);
    }
    ::close(fd);
    size = bytes;
    crc = crc32c_finish(running);
    return true;
}

/// `fsync` a file the writer has closed. Returns 0, or the errno.
int sync_file(const std::string& path) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return errno;
    const int rc = ::fsync(fd) == 0 ? 0 : errno;
    ::close(fd);
    return rc;
}

/// `src` copied to `dst` through `<dst>.tmp`: the CRC folded from the bytes read, the copy synced
/// before it is renamed into place and its directory after. False and why when a step fails, and
/// then nothing is at `dst`.
bool copy_synced(const std::string& src, const std::string& dst, std::vector<uint8_t>& buf,
                 uint64_t& size, uint32_t& crc, std::string& error) {
    const std::string tmp = dst + ".tmp";
    const int in = ::open(src.c_str(), O_RDONLY | O_CLOEXEC);
    if (in < 0) {
        error = "cannot open " + src + ": " + std::strerror(errno);
        return false;
    }
    // OB_DURABLE: synced below before the rename, and its directory after it.
    const int out = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0640);
    if (out < 0) {
        error = "cannot create " + tmp + ": " + std::strerror(errno);
        ::close(in);
        return false;
    }
    uint32_t running = crc32c_init;
    uint64_t copied = 0;
    bool ok = true;
    for (;;) {
        const ssize_t n = ::read(in, buf.data(), buf.size());
        if (n < 0) {
            if (errno == EINTR) continue;
            error = "cannot read " + src + ": " + std::strerror(errno);
            ok = false;
            break;
        }
        if (n == 0) break;
        running = crc32c_update(running, buf.data(), static_cast<size_t>(n));
        const uint8_t* p = buf.data();
        size_t left = static_cast<size_t>(n);
        while (left > 0) {
            const ssize_t w = ::write(out, p, left);
            if (w < 0) {
                if (errno == EINTR) continue;
                error = "cannot write " + tmp + ": " + std::strerror(errno);
                ok = false;
                break;
            }
            p += w;
            left -= static_cast<size_t>(w);
        }
        if (!ok) break;
        copied += static_cast<uint64_t>(n);
    }
    ::close(in);
    if (ok && ::fsync(out) != 0) {
        error = "cannot sync " + tmp + ": " + std::strerror(errno);
        ok = false;
    }
    if (::close(out) != 0 && ok) {
        error = "cannot close " + tmp + ": " + std::strerror(errno);
        ok = false;
    }
    if (ok && ::rename(tmp.c_str(), dst.c_str()) != 0) {
        error = "cannot rename " + tmp + ": " + std::strerror(errno);
        ok = false;
    }
    if (!ok) {
        ::unlink(tmp.c_str());
        return false;
    }
    if (const int err = sync_directory(fs::path(dst).parent_path().string()); err != 0) {
        // The file is there and whole; its name may not survive a power cut, and the next pass
        // finds it the same or not at all.
        error = "cannot sync the directory of " + dst + ": " + std::strerror(err);
        return false;
    }
    size = copied;
    crc = crc32c_finish(running);
    return true;
}

}  // namespace

WalArchiver::WalArchiver(std::string wal_dir, std::string root, uint64_t identity,
                         std::function<uint32_t()> current_file, MetricsRegistry* registry)
    : wal_dir_(std::move(wal_dir))
    , root_(std::move(root))
    , identity_(identity)
    , current_file_(std::move(current_file))
    , registry_(registry) {}

WalArchiver::~WalArchiver() { stop(); }

std::string WalArchiver::lineage_dir(uint64_t identity) const {
    return root_ + "/" + std::to_string(identity);
}

void WalArchiver::start() {
    if (thread_.joinable()) return;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        stop_ = false;
    }
    thread_ = std::thread([this] { run(); });
    pthread_setname_np(thread_.native_handle(), "ob-wal-archive");
    OB_LOG_INFO("wal_archive", "archiving the WAL of %s to %s", wal_dir_.c_str(),
                lineage_dir(lineage()).c_str());
}

void WalArchiver::stop() {
    {
        std::lock_guard<std::mutex> lock(mtx_);
        stop_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();
}

void WalArchiver::wake() {
    {
        std::lock_guard<std::mutex> lock(mtx_);
        woken_ = true;
    }
    cv_.notify_all();
}

void WalArchiver::run() {
    std::unique_lock<std::mutex> lock(mtx_);
    while (!stop_) {
        lock.unlock();
        if (!refused()) {
            // The bound before the lineage. A new lineage begins in the file the writer is in, so a
            // bound read before `set_lineage()` is at most that file's index and names only files of
            // the old lineage; read the other way round, an old lineage could be paired with a bound
            // a sync after the change has moved into the new one, and its files would go to the old
            // lineage's directory.
            const uint32_t before = current_file_();
            const uint64_t identity = lineage();
            (void)archive_closed(identity, before);
        }
        lock.lock();
        cv_.wait_for(lock, std::chrono::seconds(1), [this] { return stop_ || woken_; });
        woken_ = false;
    }
}

WalArchiver::Pass WalArchiver::archive_closed(uint64_t identity, uint32_t before_file) {
    std::lock_guard<std::mutex> pass_lock(pass_mtx_);
    Pass out;
    if (refused()) {
        out.refused = true;
        out.failure = "archiving stopped at a file in the archive with other contents";
        return out;
    }

    // The closed files, oldest first.
    std::vector<std::pair<uint32_t, std::string>> files;
    std::error_code ec;
    for (const auto& entry : fs::directory_iterator(wal_dir_, ec)) {
        uint32_t index = 0;
        const std::string name = entry.path().filename().string();
        if (parse_wal_file_basename(name, index) && index < before_file) files.emplace_back(index, name);
    }
    if (ec) {
        out.failure = "cannot list " + wal_dir_ + ": " + ec.message();
        OB_LOG_WARN("wal_archive", "%s - WAL retention waits", out.failure.c_str());
        if (registry_) registry_->increment_counter("ob_wal_archive_failures_total");
        return out;
    }
    std::sort(files.begin(), files.end());

    // A closed file that leaves the WAL's directory has left the archive's concern too.
    for (auto k = known_.begin(); k != known_.end();) {
        const bool here = k->first.first == identity &&
                          std::any_of(files.begin(), files.end(),
                                      [&](const auto& f) { return f.second == k->first.second; });
        k = here ? std::next(k) : known_.erase(k);
    }

    const std::string dir = lineage_dir(identity);
    uint32_t held_from = before_file;   // the lowest index the archive does not hold
    std::string failed_name;           // the file a failure stopped at
    std::vector<uint8_t> buf(kBufferBytes);
    // Synced once a pass, before its first copy: the lineage's directory into the archive's, and the
    // archive's into its parent. Retention removes a WAL file once its copy is in the archive, and a
    // copy whose directory a power cut takes is no copy - whichever process made the directory.
    bool dirs_synced = false;
    fs::create_directories(dir, ec);
    if (ec) {
        out.failure = "cannot create " + dir + ": " + ec.message();
        if (!files.empty()) {
            held_from = files.front().first;
            failed_name = files.front().second;
        }
    } else {
        for (const auto& [index, name] : files) {
            const auto key = std::make_pair(identity, name);
            if (known_.count(key) != 0) {
                ++out.present;
                continue;
            }
            const std::string src = wal_dir_ + "/" + name;
            const std::string dst = dir + "/" + name;
            std::string why;
            // Synced before it is read: a rotation leaves its file for the next flush tick to sync,
            // and a copy of bytes a power cut can still take from the source would differ from the
            // source after it - and archiving would stop at a file nobody damaged. A file the tick
            // has synced costs a call.
            if (const int err = sync_file(src); err != 0) {
                out.failure = "cannot sync " + src + " before it is read: " + std::strerror(err);
                held_from = index;
                failed_name = name;
                break;
            }
            if (fs::exists(dst, ec)) {
                uint64_t src_size = 0, dst_size = 0;
                uint32_t src_crc = 0, dst_crc = 0;
                if (!size_and_crc(src, buf, src_size, src_crc, why) ||
                    !size_and_crc(dst, buf, dst_size, dst_crc, why)) {
                    out.failure = why;
                    held_from = index;
                    failed_name = name;
                    break;
                }
                if (src_size == dst_size && src_crc == dst_crc) {
                    known_[key] = {src_size, src_crc};
                    ++out.present;
                    continue;
                }
                refused_.store(true, std::memory_order_release);
                out.refused = true;
                out.failure = dst + " is in the archive with other contents";
                held_from = index;
                failed_name = name;
                OB_LOG_ERROR("wal_archive", "%s is in the archive with other contents (%llu bytes, crc "
                                            "%08x; this file %llu bytes, crc %08x): archiving stops, "
                                            "and WAL retention with it, until the archive is put right",
                             dst.c_str(), static_cast<unsigned long long>(dst_size), dst_crc,
                             static_cast<unsigned long long>(src_size), src_crc);
                break;
            }
            if (!dirs_synced) {
                for (const std::string& d : {root_, fs::path(root_).parent_path().string()}) {
                    if (const int err = sync_directory(d.empty() ? "." : d); err != 0) {
                        why = "cannot sync the directory " + d + ": " + std::strerror(err);
                        break;
                    }
                }
                if (!why.empty()) {
                    out.failure = why;
                    held_from = index;
                    failed_name = name;
                    break;
                }
                dirs_synced = true;
            }
            const auto started = std::chrono::steady_clock::now();
            uint64_t copied = 0, back_size = 0;
            uint32_t copy_crc = 0, back_crc = 0;
            if (!copy_synced(src, dst, buf, copied, copy_crc, why)) {
                out.failure = why;
                held_from = index;
                failed_name = name;
                break;
            }
            // Read back: a copy is believed once it reads as the bytes that went into it.
            if (!size_and_crc(dst, buf, back_size, back_crc, why) || back_size != copied ||
                back_crc != copy_crc) {
                fs::remove(dst, ec);
                out.failure = why.empty() ? "the copy of " + name + " does not read back as written" : why;
                held_from = index;
                failed_name = name;
                break;
            }
            known_[key] = {copied, copy_crc};
            ++out.archived;
            out.bytes += copied;
            if (registry_) {
                registry_->increment_counter("ob_wal_archived_files_total");
                registry_->increment_counter("ob_wal_archived_bytes_total", copied);
            }
            OB_LOG_INFO("wal_archive", "archived %s (%llu bytes, crc %08x) in %.1f ms", name.c_str(),
                        static_cast<unsigned long long>(copied), copy_crc,
                        std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() -
                                                                  started).count());
        }
    }
    if (!out.failure.empty()) {
        if (registry_) registry_->increment_counter("ob_wal_archive_failures_total");
        if (!out.refused) {
            OB_LOG_WARN("wal_archive", "cannot archive %s: %s - WAL retention waits at file %u",
                        failed_name.c_str(), out.failure.c_str(), held_from);
        }
    }

    // What is left, for the gauges: the closed files from the held_from on, and how long the oldest has
    // waited since it was last written.
    int64_t oldest_s = 0;
    for (const auto& [index, name] : files) {
        if (index < held_from) continue;
        ++out.pending;
        struct stat st{};
        if (oldest_s == 0 && ::stat((wal_dir_ + "/" + name).c_str(), &st) == 0) {
            const auto now_s = static_cast<int64_t>(wall_clock_ns() / 1'000'000'000ULL);
            oldest_s = std::max<int64_t>(0, now_s - static_cast<int64_t>(st.st_mtime));
        }
    }
    if (registry_) {
        registry_->set_gauge("ob_wal_archive_pending_files", static_cast<int64_t>(out.pending));
        registry_->set_gauge("ob_wal_archive_oldest_pending_seconds", oldest_s);
    }
    archived_before_.store(held_from, std::memory_order_release);
    OB_LOG_DEBUG("wal_archive", "pass over %zu closed file(s) of lineage %llu: %zu archived, %zu "
                                "already there, %zu pending; retention may go below file %u",
                 files.size(), static_cast<unsigned long long>(identity), out.archived, out.present,
                 out.pending, held_from);
    return out;
}

}  // namespace ob
