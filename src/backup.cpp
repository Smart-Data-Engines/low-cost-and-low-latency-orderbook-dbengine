// A node's backup: taken by the server, checked and restored offline (#34). See backup.hpp for the
// layout on the disk and kiro-workspace/specs/backup-restore/ for the design.

#include "orderbook/backup.hpp"

#include "orderbook/crc32c.hpp"
#include "orderbook/durable_file.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/logger.hpp"
#include "orderbook/metrics.hpp"
#include "orderbook/thread_boundary.hpp"
#include "orderbook/version.hpp"
#include "orderbook/wal.hpp"
#include "orderbook/wall_clock.hpp"

#include <nlohmann/json.hpp>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <sstream>
#include <unordered_set>

namespace ob {

namespace fs = std::filesystem;

namespace {

/// Read size for checksumming and copying: the snapshot's (`Engine::kSnapshotReadChunk`), one buffer
/// per backup or restore, reused for every file.
constexpr size_t kChunk = 256u * 1024u;

/// Room left beside a copy's bytes: the description and the directory entries.
constexpr uint64_t kCopyHeadroom = 1u << 20;

double ms_since(std::chrono::steady_clock::time_point t) {
    return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t).count();
}

std::string errno_text(int err) { return std::strerror(err); }

const char* role_name(NodeRole r) {
    switch (r) {
    case NodeRole::STANDALONE:   return "standalone";
    case NodeRole::PRIMARY:      return "primary";
    case NodeRole::REPLICA:      return "replica";
    case NodeRole::MULTI_MASTER: return "multi_master";
    }
    return "standalone";
}

/// Fold a file's CRC32C, reading it in chunks. False, with the reason, when it cannot be read whole.
bool checksum_file(const std::string& path, std::vector<uint8_t>& buf, uint32_t& crc,
                   uint64_t& bytes, std::string& error, const std::atomic<bool>* stop) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        error = "cannot open " + path + ": " + errno_text(errno);
        return false;
    }
    uint32_t running = crc32c_init;
    bytes = 0;
    for (;;) {
        if (stop && stop->load(std::memory_order_acquire)) {
            ::close(fd);
            error = "stopped";
            return false;
        }
        const ssize_t n = ::read(fd, buf.data(), buf.size());
        if (n < 0) {
            if (errno == EINTR) continue;
            error = "cannot read " + path + ": " + errno_text(errno);
            ::close(fd);
            return false;
        }
        if (n == 0) break;
        running = crc32c_update(running, buf.data(), static_cast<size_t>(n));
        bytes += static_cast<uint64_t>(n);
    }
    ::close(fd);
    crc = crc32c_finish(running);
    return true;
}

/// Copy `src` to a new file `dst`, folding the CRC32C of the bytes as they pass. Refuses to replace
/// anything (`O_EXCL`), and a source that is not `expected` bytes long. The copy is not synced here:
/// both callers sync the whole filesystem once, after the last file.
bool copy_file_checked(const std::string& src, const std::string& dst, uint64_t expected,
                       std::vector<uint8_t>& buf, uint32_t& crc, std::string& error,
                       const std::atomic<bool>* stop) {
    const int in = ::open(src.c_str(), O_RDONLY | O_CLOEXEC);
    if (in < 0) {
        error = "cannot open " + src + ": " + errno_text(errno);
        return false;
    }
    // OB_DURABLE: a backup's or a restore's copy - its caller syncs the filesystem with syncfs()
    // before a description or an engine relies on it.
    const int out = ::open(dst.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0640);
    if (out < 0) {
        error = "cannot create " + dst + ": " + errno_text(errno);
        ::close(in);
        return false;
    }
    uint32_t running = crc32c_init;
    uint64_t copied = 0;
    bool ok = true;
    for (;;) {
        if (stop && stop->load(std::memory_order_acquire)) {
            error = "stopped";
            ok = false;
            break;
        }
        const ssize_t n = ::read(in, buf.data(), buf.size());
        if (n < 0) {
            if (errno == EINTR) continue;
            error = "cannot read " + src + ": " + errno_text(errno);
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
                error = "cannot write " + dst + ": " + errno_text(errno);
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
    if (::close(out) != 0 && ok) {
        error = "cannot close " + dst + ": " + errno_text(errno);
        ok = false;
    }
    if (ok && copied != expected) {
        error = src + " is " + std::to_string(copied) + " byte(s), and the cut listed it at " +
                std::to_string(expected);
        ok = false;
    }
    if (ok) crc = crc32c_finish(running);
    return ok;
}

/// syncfs() on the filesystem holding `dir`: the data and the directory entries of everything
/// written there, in one call. Returns 0 or the errno.
int sync_filesystem_of(const std::string& dir) {
    const int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) return errno;
    const int rc = ::syncfs(fd) == 0 ? 0 : errno;
    ::close(fd);
    return rc;
}

/// `inner` is `outer` or lies under it; both already canonical.
bool within(const fs::path& inner, const fs::path& outer) {
    auto i = inner.begin();
    for (auto o = outer.begin(); o != outer.end(); ++o, ++i) {
        if (i == inner.end() || *i != *o) return false;
    }
    return true;
}

/// The first entry of a directory, or empty when it has none. `exists` false when there is no
/// directory at all.
std::string first_entry(const std::string& dir, bool& exists, std::string& error) {
    std::error_code ec;
    exists = fs::exists(dir, ec);
    if (!exists) return {};
    if (!fs::is_directory(dir, ec)) {
        error = dir + " is not a directory";
        return {};
    }
    for (const auto& e : fs::directory_iterator(dir, ec)) return e.path().filename().string();
    if (ec) error = "cannot list " + dir + ": " + ec.message();
    return {};
}

// ── JSON ──────────────────────────────────────────────────────────────────────

using nlohmann::json;

/// The member `key` of `o` as an unsigned number no larger than `max`.
bool get_unsigned(const json& o, const char* key, uint64_t max, uint64_t& out, std::string& error) {
    const auto it = o.find(key);
    if (it == o.end()) {
        error = std::string("no \"") + key + "\"";
        return false;
    }
    if (!it->is_number_unsigned() && !(it->is_number_integer() && it->get<int64_t>() >= 0)) {
        error = std::string("\"") + key + "\" is not an unsigned number";
        return false;
    }
    const uint64_t v = it->get<uint64_t>();
    if (v > max) {
        error = std::string("\"") + key + "\" is " + std::to_string(v) + ", past " +
                std::to_string(max);
        return false;
    }
    out = v;
    return true;
}

bool get_string(const json& o, const char* key, std::string& out, std::string& error) {
    const auto it = o.find(key);
    if (it == o.end() || !it->is_string()) {
        error = std::string("\"") + key + "\" is missing or not a string";
        return false;
    }
    out = it->get<std::string>();
    return true;
}

bool get_bool(const json& o, const char* key, bool& out, std::string& error) {
    const auto it = o.find(key);
    if (it == o.end() || !it->is_boolean()) {
        error = std::string("\"") + key + "\" is missing or not true or false";
        return false;
    }
    out = it->get<bool>();
    return true;
}

bool get_object(const json& o, const char* key, const json*& out, std::string& error) {
    const auto it = o.find(key);
    if (it == o.end() || !it->is_object()) {
        error = std::string("\"") + key + "\" is missing or not an object";
        return false;
    }
    out = &*it;
    return true;
}

bool get_array(const json& o, const char* key, const json*& out, std::string& error) {
    const auto it = o.find(key);
    if (it == o.end() || !it->is_array()) {
        error = std::string("\"") + key + "\" is missing or not an array";
        return false;
    }
    out = &*it;
    return true;
}

}  // namespace

// ── Names and paths ──────────────────────────────────────────────────────────

std::string backup_name_for(uint64_t wall_ns) {
    const auto secs = static_cast<std::time_t>(wall_ns / 1'000'000'000ULL);
    const auto ms   = static_cast<unsigned>((wall_ns / 1'000'000ULL) % 1000ULL);
    std::tm tm{};
    ::gmtime_r(&secs, &tm);
    char buf[32];
    const size_t n = std::strftime(buf, sizeof(buf), "%Y%m%dT%H%M%S", &tm);
    char out[40];
    std::snprintf(out, sizeof(out), "%.*s.%03uZ", static_cast<int>(n), buf, ms);
    return out;
}

bool parse_backup_name(std::string_view name, uint64_t* wall_ns) {
    // YYYYMMDDTHHMMSS.mmmZ
    if (name.size() != 20 || name[8] != 'T' || name[15] != '.' || name[19] != 'Z') return false;
    for (size_t i : {0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 16, 17, 18}) {
        if (name[i] < '0' || name[i] > '9') return false;
    }
    auto num = [&](size_t at, size_t len) {
        int v = 0;
        for (size_t i = at; i < at + len; ++i) v = v * 10 + (name[i] - '0');
        return v;
    };
    std::tm tm{};
    tm.tm_year = num(0, 4) - 1900;
    tm.tm_mon  = num(4, 2) - 1;
    tm.tm_mday = num(6, 2);
    tm.tm_hour = num(9, 2);
    tm.tm_min  = num(11, 2);
    tm.tm_sec  = num(13, 2);
    if (tm.tm_mon < 0 || tm.tm_mon > 11 || tm.tm_mday < 1 || tm.tm_mday > 31 || tm.tm_hour > 23 ||
        tm.tm_min > 59 || tm.tm_sec > 60) {
        return false;
    }
    const std::time_t secs = ::timegm(&tm);
    if (secs < 0) return false;
    if (wall_ns) {
        *wall_ns = static_cast<uint64_t>(secs) * 1'000'000'000ULL +
                   static_cast<uint64_t>(num(16, 3)) * 1'000'000ULL;
    }
    return true;
}

bool backup_path_is_contained(std::string_view rel) {
    if (rel.empty() || rel.front() == '/') return false;
    size_t start = 0;
    while (start <= rel.size()) {
        const size_t slash = rel.find('/', start);
        const std::string_view part =
            rel.substr(start, slash == std::string_view::npos ? std::string_view::npos : slash - start);
        if (part.empty() || part == "." || part == "..") return false;
        if (slash == std::string_view::npos) break;
        start = slash + 1;
    }
    return true;
}

std::string backup_dir_problem(const std::string& backup_dir, const std::string& data_dir,
                               const std::string& wal_dir) {
    if (backup_dir.empty()) return "--backup-dir is empty";
    std::error_code ec;
    fs::create_directories(backup_dir, ec);
    if (ec) return "cannot create " + backup_dir + ": " + ec.message();
    if (!fs::is_directory(backup_dir, ec)) return backup_dir + " is not a directory";
    if (::access(backup_dir.c_str(), W_OK | X_OK) != 0) {
        return "cannot write to " + backup_dir + ": " + errno_text(errno);
    }
    const fs::path b = fs::weakly_canonical(backup_dir, ec);
    if (ec) return "cannot resolve " + backup_dir + ": " + ec.message();
    struct Named { const char* what; std::string path; };
    for (const Named& other : {Named{"data directory", data_dir}, Named{"WAL directory", wal_dir}}) {
        if (other.path.empty()) continue;
        const fs::path o = fs::weakly_canonical(other.path, ec);
        if (ec) return std::string("cannot resolve the ") + other.what + " " + other.path + ": " +
                       ec.message();
        if (within(b, o)) {
            return "--backup-dir " + b.string() + " is in the " + other.what + " " + o.string() +
                   ": the engine would read the backups' segments as its own, and a snapshot "
                   "install would remove them";
        }
        if (within(o, b)) {
            return std::string("the ") + other.what + " " + o.string() + " is in --backup-dir " +
                   b.string();
        }
    }
    return {};
}

// ── The description ──────────────────────────────────────────────────────────

std::string BackupDescription::to_json() const {
    json files_j = json::array();
    for (const auto& f : files) {
        files_j.push_back({{"path", f.path}, {"size", f.size}, {"crc32c", f.crc32c}});
    }
    json vector_j = json::array();
    for (const auto& e : vector) {
        vector_j.push_back({{"key", e.key}, {"origin", e.origin}, {"frontier", e.frontier}});
    }
    json held_j = json::array();
    for (const auto& h : held) {
        json ranges = json::array();
        for (const auto& [first, last] : h.ranges) ranges.push_back({first, last});
        held_j.push_back({{"key", h.key}, {"origin", h.origin}, {"ranges", std::move(ranges)}});
    }
    const json j = {
        {"format", kFormat},
        {"format_version", kFormatVersion},
        {"name", name},
        {"engine_version", engine_version},
        {"cut_at_ns", cut_at_ns},
        {"finished_at_ns", finished_at_ns},
        {"node", {{"role", role}, {"node_id", node_id}, {"mm_node_id", mm_node_id}}},
        {"wal", {{"identity", wal_identity}, {"file_index", wal_file_index},
                 {"byte_offset", wal_byte_offset}}},
        {"method", method},
        {"total_bytes", total_bytes},
        {"total_rows", total_rows},
        {"files", std::move(files_j)},
        {"sequence", {{"vector", std::move(vector_j)}, {"vector_truncated", vector_truncated},
                      {"held", std::move(held_j)}, {"held_truncated", held_truncated}}},
        {"numbering_closed", numbering_closed},
    };
    return j.dump(1) + "\n";
}

bool BackupDescription::from_json(std::string_view text, BackupDescription& out,
                                  std::string& error) {
    json j;
    try {
        j = json::parse(text.begin(), text.end());
    } catch (const std::exception& e) {
        error = std::string("not JSON: ") + e.what();
        return false;
    }
    if (!j.is_object()) {
        error = "not a JSON object";
        return false;
    }
    BackupDescription d;
    std::string format;
    uint64_t version = 0;
    if (!get_string(j, "format", format, error)) return false;
    if (format != kFormat) {
        error = "\"format\" is \"" + format + "\", not \"" + kFormat + "\"";
        return false;
    }
    if (!get_unsigned(j, "format_version", UINT32_MAX, version, error)) return false;
    if (version != static_cast<uint64_t>(kFormatVersion)) {
        error = "format version " + std::to_string(version) + " is not one this build reads (" +
                std::to_string(kFormatVersion) + ")";
        return false;
    }
    uint64_t v = 0;
    if (!get_string(j, "name", d.name, error)) return false;
    if (!parse_backup_name(d.name, nullptr)) {
        error = "\"name\" \"" + d.name + "\" is not a backup's name";
        return false;
    }
    if (!get_string(j, "engine_version", d.engine_version, error)) return false;
    if (!get_unsigned(j, "cut_at_ns", UINT64_MAX, d.cut_at_ns, error)) return false;
    if (!get_unsigned(j, "finished_at_ns", UINT64_MAX, d.finished_at_ns, error)) return false;

    const json* node = nullptr;
    if (!get_object(j, "node", node, error)) return false;
    if (!get_string(*node, "role", d.role, error)) return false;
    if (d.role != "standalone" && d.role != "primary" && d.role != "replica" &&
        d.role != "multi_master") {
        error = "\"role\" \"" + d.role + "\" is not a node's role";
        return false;
    }
    if (!get_string(*node, "node_id", d.node_id, error)) return false;
    if (!get_unsigned(*node, "mm_node_id", UINT16_MAX, v, error)) return false;
    d.mm_node_id = static_cast<uint16_t>(v);

    const json* wal = nullptr;
    if (!get_object(j, "wal", wal, error)) return false;
    if (!get_unsigned(*wal, "identity", UINT64_MAX, d.wal_identity, error)) return false;
    if (!get_unsigned(*wal, "file_index", UINT32_MAX, v, error)) return false;
    d.wal_file_index = static_cast<uint32_t>(v);
    if (!get_unsigned(*wal, "byte_offset", UINT64_MAX, d.wal_byte_offset, error)) return false;

    if (!get_string(j, "method", d.method, error)) return false;
    if (d.method != "linked" && d.method != "copied") {
        error = "\"method\" \"" + d.method + "\" is neither linked nor copied";
        return false;
    }
    if (!get_unsigned(j, "total_bytes", UINT64_MAX, d.total_bytes, error)) return false;
    if (!get_unsigned(j, "total_rows", UINT64_MAX, d.total_rows, error)) return false;

    const json* files = nullptr;
    if (!get_array(j, "files", files, error)) return false;
    std::unordered_set<std::string> seen;
    uint64_t sum = 0;
    for (size_t i = 0; i < files->size(); ++i) {
        const json& f = (*files)[i];
        if (!f.is_object()) {
            error = "files[" + std::to_string(i) + "] is not an object";
            return false;
        }
        SnapshotFileEntry e;
        uint64_t size = 0, crc = 0;
        if (!get_string(f, "path", e.path, error) ||
            !get_unsigned(f, "size", UINT64_MAX, size, error) ||
            !get_unsigned(f, "crc32c", UINT32_MAX, crc, error)) {
            error = "files[" + std::to_string(i) + "]: " + error;
            return false;
        }
        if (!backup_path_is_contained(e.path)) {
            error = "files[" + std::to_string(i) + "]: the path \"" + e.path +
                    "\" leaves the directory it is restored to";
            return false;
        }
        if (!seen.insert(e.path).second) {
            error = "files[" + std::to_string(i) + "]: \"" + e.path + "\" is listed twice";
            return false;
        }
        e.size   = static_cast<size_t>(size);
        e.crc32c = static_cast<uint32_t>(crc);
        sum += size;
        d.files.push_back(std::move(e));
    }
    if (sum != d.total_bytes) {
        error = "the files add up to " + std::to_string(sum) + " byte(s), and \"total_bytes\" says " +
                std::to_string(d.total_bytes);
        return false;
    }

    const json* seq = nullptr;
    if (!get_object(j, "sequence", seq, error)) return false;
    const json* vec = nullptr;
    if (!get_array(*seq, "vector", vec, error)) return false;
    for (size_t i = 0; i < vec->size(); ++i) {
        const json& e = (*vec)[i];
        SequenceTracker::VectorEntry ve;
        uint64_t origin = 0;
        if (!e.is_object() || !get_string(e, "key", ve.key, error) ||
            !get_unsigned(e, "origin", UINT16_MAX, origin, error) ||
            !get_unsigned(e, "frontier", UINT64_MAX, ve.frontier, error)) {
            error = "sequence.vector[" + std::to_string(i) + "]: " +
                    (e.is_object() ? error : std::string("not an object"));
            return false;
        }
        ve.origin = static_cast<uint16_t>(origin);
        d.vector.push_back(std::move(ve));
    }
    if (!get_bool(*seq, "vector_truncated", d.vector_truncated, error)) return false;
    const json* held = nullptr;
    if (!get_array(*seq, "held", held, error)) return false;
    for (size_t i = 0; i < held->size(); ++i) {
        const json& h = (*held)[i];
        SequenceTracker::HeldRanges hr;
        uint64_t origin = 0;
        const json* ranges = nullptr;
        if (!h.is_object() || !get_string(h, "key", hr.key, error) ||
            !get_unsigned(h, "origin", UINT16_MAX, origin, error) ||
            !get_array(h, "ranges", ranges, error)) {
            error = "sequence.held[" + std::to_string(i) + "]: " +
                    (h.is_object() ? error : std::string("not an object"));
            return false;
        }
        hr.origin = static_cast<uint16_t>(origin);
        for (const json& r : *ranges) {
            if (!r.is_array() || r.size() != 2 || !r[0].is_number_unsigned() ||
                !r[1].is_number_unsigned() || r[0].get<uint64_t>() > r[1].get<uint64_t>()) {
                error = "sequence.held[" + std::to_string(i) + "]: a range is not [first, last]";
                return false;
            }
            hr.ranges.emplace_back(r[0].get<uint64_t>(), r[1].get<uint64_t>());
        }
        d.held.push_back(std::move(hr));
    }
    if (!get_bool(*seq, "held_truncated", d.held_truncated, error)) return false;
    if (!get_bool(j, "numbering_closed", d.numbering_closed, error)) return false;

    out = std::move(d);
    return true;
}

bool read_backup_description(const std::string& backup, BackupDescription& out, std::string& error) {
    const std::string path = backup + "/" + kBackupDescriptionFile;
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
        error = "cannot open " + path + ": " + errno_text(errno);
        OB_LOG_WARN("backup", "%s", error.c_str());
        return false;
    }
    const std::string text((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    if (in.bad()) {
        error = "cannot read " + path;
        OB_LOG_WARN("backup", "%s", error.c_str());
        return false;
    }
    if (!BackupDescription::from_json(text, out, error)) {
        error = path + ": " + error;
        OB_LOG_WARN("backup", "%s", error.c_str());
        return false;
    }
    OB_LOG_DEBUG("backup", "Read %s: %zu file(s), %llu byte(s), %s", path.c_str(), out.files.size(),
                 static_cast<unsigned long long>(out.total_bytes), out.method.c_str());
    return true;
}

std::vector<std::string> verify_backup(const std::string& backup, const BackupDescription& d) {
    const auto t0 = std::chrono::steady_clock::now();
    std::vector<std::string> problems;
    std::vector<uint8_t> buf(kChunk);
    uint64_t bytes = 0;
    for (const auto& f : d.files) {
        const std::string path = backup + "/" + kBackupStoreDir + "/" + f.path;
        std::error_code ec;
        const fs::file_status st = fs::symlink_status(path, ec);
        if (ec || !fs::is_regular_file(st)) {
            problems.push_back(f.path + ": " +
                               (ec || !fs::exists(st) ? std::string("missing")
                                                      : std::string("not a regular file")));
            continue;
        }
        uint32_t crc = 0;
        uint64_t read = 0;
        std::string error;
        if (!checksum_file(path, buf, crc, read, error, nullptr)) {
            problems.push_back(f.path + ": " + error);
            continue;
        }
        if (read != f.size) {
            problems.push_back(f.path + ": " + std::to_string(read) + " byte(s), the description says " +
                               std::to_string(f.size));
            continue;
        }
        if (crc != f.crc32c) {
            char line[96];
            std::snprintf(line, sizeof(line), ": CRC32C %08x, the description says %08x", crc, f.crc32c);
            problems.push_back(f.path + line);
            continue;
        }
        bytes += read;
    }
    OB_LOG_INFO("backup", "Verified %s: %zu file(s), %llu byte(s), %zu problem(s) in %.1f ms",
                backup.c_str(), d.files.size(), static_cast<unsigned long long>(bytes),
                problems.size(), ms_since(t0));
    return problems;
}

// ── Restore ──────────────────────────────────────────────────────────────────

bool restore_backup(const std::string& backup, const std::string& data_dir,
                    const std::string& wal_dir_in, RestoreReport& report, std::string& error) {
    report = RestoreReport{};
    BackupDescription d;
    if (!read_backup_description(backup, d, error)) return false;
    if (d.role == "multi_master" && d.vector_truncated) {
        error = "the backup's version vector was past the entries a node can state, so a mesh node "
                "restored from it could declare no frontier and would take every row again: bootstrap "
                "the node from a peer instead";
        OB_LOG_WARN("backup", "Restore of %s refused: %s", backup.c_str(), error.c_str());
        return false;
    }

    std::error_code ec;
    std::string wal_dir = wal_dir_in;
    if (!wal_dir.empty() &&
        fs::weakly_canonical(wal_dir, ec) == fs::weakly_canonical(data_dir, ec)) {
        wal_dir.clear();
    }
    for (const std::string& dir : {data_dir, wal_dir}) {
        if (dir.empty()) continue;
        bool exists = false;
        std::string listing_error;
        const std::string first = first_entry(dir, exists, listing_error);
        if (!listing_error.empty()) {
            error = listing_error;
            OB_LOG_WARN("backup", "Restore of %s refused: %s", backup.c_str(), error.c_str());
            return false;
        }
        if (!first.empty()) {
            error = dir + " is not empty (it holds " + first + "): a restore goes into an empty "
                    "directory only";
            OB_LOG_WARN("backup", "Restore of %s refused: %s", backup.c_str(), error.c_str());
            return false;
        }
    }

    // The whole backup, before anything is created.
    auto t = std::chrono::steady_clock::now();
    const std::vector<std::string> problems = verify_backup(backup, d);
    report.verify_ms = ms_since(t);
    if (!problems.empty()) {
        error = std::to_string(problems.size()) + " file(s) of the backup do not match its "
                "description, the first: " + problems.front();
        OB_LOG_WARN("backup", "Restore of %s refused, nothing was written: %s", backup.c_str(),
                    error.c_str());
        return false;
    }

    auto left_behind = [&](const std::string& why) {
        error = why + "; " + data_dir + (wal_dir.empty() ? "" : " and " + wal_dir) +
                " hold part of the restore: empty them before trying again";
        OB_LOG_ERROR("backup", "Restore of %s failed: %s", backup.c_str(), error.c_str());
        return false;
    };

    fs::create_directories(data_dir, ec);
    if (ec) return left_behind("cannot create " + data_dir + ": " + ec.message());
    if (!wal_dir.empty()) {
        fs::create_directories(wal_dir, ec);
        if (ec) return left_behind("cannot create " + wal_dir + ": " + ec.message());
    }

    t = std::chrono::steady_clock::now();
    std::vector<uint8_t> buf(kChunk);
    for (const auto& f : d.files) {
        const std::string src = backup + "/" + kBackupStoreDir + "/" + f.path;
        const std::string dst = data_dir + "/" + f.path;
        fs::create_directories(fs::path(dst).parent_path(), ec);
        if (ec) return left_behind("cannot create the directory of " + dst + ": " + ec.message());
        uint32_t crc = 0;
        std::string why;
        if (!copy_file_checked(src, dst, f.size, buf, crc, why, nullptr)) return left_behind(why);
        // Verified above, and read again here: a file that changed between the two is caught.
        if (crc != f.crc32c) return left_behind(f.path + " changed in the backup during the restore");
        report.bytes += f.size;
        ++report.files;
    }
    if (const int err = sync_filesystem_of(data_dir); err != 0) {
        return left_behind("cannot sync " + data_dir + ": " + errno_text(err));
    }
    report.copy_ms = ms_since(t);
    OB_LOG_INFO("backup", "Restore of %s: %zu file(s), %llu byte(s) copied into %s in %.1f ms",
                backup.c_str(), report.files, static_cast<unsigned long long>(report.bytes),
                data_dir.c_str(), report.copy_ms);

    // A WAL that begins at file 1, with no file 0. The rows are in the segments and in no record of
    // this WAL, so a replica asking this node - a primary again - for its log from the start has to
    // be told what it is told when retention took the start, WAL_TRUNCATED, and bootstrap from a
    // snapshot: streamed from file 0 it would hold none of them. Measured before this, on a real
    // pair: the replica found the new WAL's identity, discarded what it held, streamed the restored
    // primary's log from the start, and served nothing for as long as it was watched.
    t = std::chrono::steady_clock::now();
    const std::string wal_home = wal_dir.empty() ? data_dir : wal_dir;
    try {
        WALWriter first(wal_home, 512ULL << 20, FsyncPolicy::INTERVAL);
        first.rotate();
    } catch (const std::exception& e) {
        return left_behind(std::string("cannot begin the restored WAL: ") + e.what());
    }
    if (!fs::exists(wal_home + "/wal_000001.bin", ec)) {
        return left_behind("the restored WAL did not begin at its second file");
    }
    fs::remove(wal_home + "/wal_000000.bin", ec);
    if (ec) return left_behind("cannot remove the restored WAL's first file: " + ec.message());
    if (const int err = sync_directory(wal_home); err != 0) {
        return left_behind("cannot sync " + wal_home + ": " + errno_text(err));
    }

    // The engine's own start over the copied segments: another WAL's, so kept; a fresh WAL and
    // identity; counters from the segments. Offline: no replication, no failover, no mesh, no TTL.
    try {
        Engine engine(data_dir, 100'000'000ULL, FsyncPolicy::INTERVAL, ReplicationConfig{},
                      ReplicationClientConfig{}, FailoverConfig{}, TTLConfig{}, MultiMasterConfig{},
                      512ULL << 20, wal_dir);
        engine.open();
        report.wal_identity = engine.wal_identity();
        report.rows = d.total_rows;
        OB_LOG_INFO("backup", "Restore of %s: the engine opened %s with a new WAL (identity %016llx)",
                    backup.c_str(), data_dir.c_str(),
                    static_cast<unsigned long long>(report.wal_identity));
        if (!d.vector_truncated) {
            engine.adopt_snapshot_sequence_state(d.vector, d.held);
            report.sequence_adopted = true;
            OB_LOG_INFO("backup", "Restore of %s: sequence state adopted, %zu vector entrie(s), %zu held",
                        backup.c_str(), d.vector.size(), d.held.size());
        } else {
            OB_LOG_WARN("backup", "Restore of %s: the backup's vector was truncated; the frontiers are "
                                  "the ones the start declared from the segments",
                        backup.c_str());
        }
        engine.close();
    } catch (const std::exception& e) {
        return left_behind(std::string("the engine could not open the restored store: ") + e.what());
    }
    if (d.numbering_closed) {
        if (const int err = write_file_atomically(data_dir + "/" + kNumberingClosedFile,
                                                  "numbering closed by the node whose backup was "
                                                  "restored\n");
            err != 0) {
            return left_behind("cannot write " + data_dir + "/" + kNumberingClosedFile + ": " +
                               errno_text(err));
        }
    }
    report.open_ms = ms_since(t);
    OB_LOG_INFO("backup", "Restored %s into %s: %zu file(s), %llu byte(s), %llu row(s); verify %.1f ms, "
                          "copy %.1f ms, open %.1f ms",
                backup.c_str(), data_dir.c_str(), report.files,
                static_cast<unsigned long long>(report.bytes),
                static_cast<unsigned long long>(report.rows), report.verify_ms, report.copy_ms,
                report.open_ms);
    return true;
}

// ── BackupRunner ─────────────────────────────────────────────────────────────

std::string format_backup_status(const BackupProgress& p) {
    // One value a line, and never a line break inside one: an error text comes from strerror() and
    // the filesystem, and neither is ours to trust with the protocol's framing.
    auto value = [](const std::string& v) {
        if (v.empty()) return std::string("-");
        std::string out = v;
        for (char& c : out) {
            if (c == '\n' || c == '\r') c = ' ';
        }
        return out;
    };
    std::string out = "OK\n";
    out += "state: " + std::string(backup_state_name(p.state)) + "\n";
    out += "name: " + value(p.name) + "\n";
    out += "phase: " + value(p.phase) + "\n";
    out += "method: " + value(p.method) + "\n";
    out += "files: " + std::to_string(p.files_done) + "/" + std::to_string(p.files_total) + "\n";
    out += "bytes: " + std::to_string(p.bytes_done) + "/" + std::to_string(p.bytes_total) + "\n";
    out += "cut_ms: " + std::to_string(p.cut_ms) + "\n";
    out += "pinned_ms: " + std::to_string(p.pinned_ms) + "\n";
    out += "elapsed_ms: " + std::to_string(p.elapsed_ms) + "\n";
    out += "error: " + value(p.error) + "\n\n";
    return out;
}

const char* backup_state_name(BackupProgress::State s) {
    switch (s) {
    case BackupProgress::State::Idle:    return "idle";
    case BackupProgress::State::Running: return "running";
    case BackupProgress::State::Done:    return "done";
    case BackupProgress::State::Failed:  return "failed";
    }
    return "idle";
}

BackupRunner::BackupRunner(Engine& engine, std::string backup_dir, MetricsRegistry& registry)
    : engine_(engine), dir_(std::move(backup_dir)), registry_(registry) {
    note_existing_backups();
}

BackupRunner::~BackupRunner() { stop(); }

void BackupRunner::note_existing_backups() {
    std::error_code ec;
    std::vector<std::string> partial;
    std::string newest;
    for (const auto& e : fs::directory_iterator(dir_, ec)) {
        const std::string name = e.path().filename().string();
        if (name.rfind(kBackupPartialPrefix, 0) == 0) {
            partial.push_back(name);
            continue;
        }
        if (parse_backup_name(name, nullptr) &&
            fs::exists(e.path() / kBackupDescriptionFile, ec) && name > newest) {
            newest = name;
        }
    }
    if (!partial.empty()) {
        std::string names;
        for (const auto& p : partial) names += (names.empty() ? "" : ", ") + p;
        OB_LOG_WARN("backup", "%zu incomplete backup(s) in %s, left by a process that stopped while "
                              "taking them: %s. None is a backup; remove them",
                    partial.size(), dir_.c_str(), names.c_str());
    }
    uint64_t wall_ns = 0;
    if (!newest.empty() && parse_backup_name(newest, &wall_ns)) {
        registry_.set_gauge("ob_backup_last_success_timestamp_seconds",
                            static_cast<int64_t>(wall_ns / 1'000'000'000ULL));
    }
    OB_LOG_INFO("backup", "Backups go to %s; the newest complete one is %s", dir_.c_str(),
                newest.empty() ? "none" : newest.c_str());
}

void BackupRunner::hold_after_cut_for_test(std::function<void()> hook) {
    std::lock_guard<std::mutex> lock(mtx_);
    after_cut_ = std::move(hook);
}

void BackupRunner::set_next_name_for_test(std::string name) {
    std::lock_guard<std::mutex> lock(mtx_);
    next_name_ = std::move(name);
}

BackupRunner::Start BackupRunner::start(std::string& name) {
    std::thread finished;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (progress_.state == BackupProgress::State::Running) {
            name = progress_.name;
            return Start::Running;
        }
        if (engine_.is_bootstrapping()) return Start::Bootstrapping;
        finished = std::move(thread_);       // done: its last act was to say so
        name = next_name_.empty() ? backup_name_for(wall_clock_ns()) : std::move(next_name_);
        next_name_.clear();
        progress_ = BackupProgress{};
        progress_.state = BackupProgress::State::Running;
        progress_.name  = name;
        progress_.phase = "cut";
        started_ = std::chrono::steady_clock::now();
        stop_.store(false, std::memory_order_release);
        registry_.set_gauge("ob_backup_running", 1);
        thread_ = std::thread([this, name]() {
            run_thread_body("backup", "backup", [this, &name] { run(name); });
        });
    }
    if (finished.joinable()) finished.join();
    OB_LOG_INFO("backup", "Backup %s started into %s", name.c_str(), dir_.c_str());
    return Start::Started;
}

BackupProgress BackupRunner::progress() const {
    std::lock_guard<std::mutex> lock(mtx_);
    BackupProgress p = progress_;
    if (p.state == BackupProgress::State::Running) {
        p.elapsed_ms = static_cast<uint64_t>(ms_since(started_));
    }
    return p;
}

void BackupRunner::stop() {
    request_stop();
    std::thread t;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        t = std::move(thread_);
    }
    if (t.joinable()) t.join();
}

void BackupRunner::set_phase(const char* phase) {
    std::lock_guard<std::mutex> lock(mtx_);
    progress_.phase = phase;
}

void BackupRunner::fail(const std::string& name, const std::string& partial, const std::string& why) {
    std::string phase;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        phase = progress_.phase;
        progress_.state = BackupProgress::State::Failed;
        progress_.error = why;
        progress_.elapsed_ms = static_cast<uint64_t>(ms_since(started_));
        progress_.phase.clear();
    }
    registry_.increment_counter("ob_backup_failures_total");
    registry_.set_gauge("ob_backup_running", 0);
    // Only what this run created: the partial directory is removed when it is this run's.
    if (!partial.empty()) {
        std::error_code ec;
        fs::remove_all(partial, ec);
        if (ec) {
            OB_LOG_WARN("backup", "Backup %s: could not remove %s: %s", name.c_str(), partial.c_str(),
                        ec.message().c_str());
        }
    }
    OB_LOG_ERROR("backup", "Backup %s failed in phase %s: %s", name.c_str(),
                 phase.empty() ? "-" : phase.c_str(), why.c_str());
}

void BackupRunner::run(std::string name) {
    const std::string partial   = dir_ + "/" + kBackupPartialPrefix + name;
    const std::string final_dir = dir_ + "/" + name;
    const std::string store     = partial + "/" + kBackupStoreDir;
    bool created = false;
    try {
        std::error_code ec;
        if (fs::exists(final_dir, ec)) {
            fail(name, "", "a backup named " + name + " is already in " + dir_);
            return;
        }
        if (::mkdir(partial.c_str(), 0750) != 0) {
            const int err = errno;
            fail(name, "", "cannot create " + partial + ": " + errno_text(err));
            return;
        }
        created = true;

        // The cut: the snapshot's, without its checksums - this reads the files itself.
        const auto t_pin = std::chrono::steady_clock::now();
        SnapshotWithSequenceState cut =
            engine_.create_snapshot_with_sequence_state(SnapshotChecksums::Skip);
        const double cut_ms = cut.create_ms;
        BackupDescription d;
        d.name             = name;
        d.engine_version   = std::string(version());
        d.cut_at_ns        = cut.manifest.created_at_ns;
        d.role             = role_name(engine_.node_role());
        d.node_id          = engine_.coordinator_node_id();
        d.mm_node_id       = engine_.mm_node_id();
        d.wal_identity     = engine_.wal_identity();
        d.wal_file_index   = cut.manifest.wal_file_index;
        d.wal_byte_offset  = cut.manifest.wal_byte_offset;
        d.total_bytes      = cut.manifest.total_bytes;
        d.total_rows       = cut.manifest.total_rows;
        d.files            = cut.manifest.files;
        d.vector           = std::move(cut.vector);
        d.vector_truncated = cut.vector_truncated;
        d.held             = std::move(cut.held);
        d.held_truncated   = cut.held_truncated;
        d.numbering_closed = fs::exists(engine_.base_dir() + "/" + kNumberingClosedFile, ec);
        {
            std::lock_guard<std::mutex> lock(mtx_);
            progress_.files_total = d.files.size();
            progress_.bytes_total = d.total_bytes;
            progress_.cut_ms      = static_cast<uint64_t>(cut_ms);
        }
        OB_LOG_INFO("backup", "Backup %s: cut at WAL %u:%llu, %zu file(s), %llu byte(s), %llu row(s), "
                              "in %.1f ms",
                    name.c_str(), d.wal_file_index, static_cast<unsigned long long>(d.wal_byte_offset),
                    d.files.size(), static_cast<unsigned long long>(d.total_bytes),
                    static_cast<unsigned long long>(d.total_rows), cut_ms);
        std::function<void()> hook;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            hook = after_cut_;
        }
        if (hook) hook();

        fs::create_directories(store, ec);
        if (ec) {
            fail(name, partial, "cannot create " + store + ": " + ec.message());
            return;
        }

        // Linked when the backup directory's filesystem takes a hard link from the data directory.
        bool linked = !force_copy_.load() && !d.files.empty();
        int link_errno = 0;
        const std::string base = engine_.base_dir();
        std::vector<uint8_t> buf(kChunk);
        for (size_t i = 0; i < d.files.size(); ++i) {
            if (stopping()) {
                fail(name, partial, "stopped");
                return;
            }
            auto& f = d.files[i];
            const std::string src = base + "/" + f.path;
            const std::string dst = store + "/" + f.path;
            fs::create_directories(fs::path(dst).parent_path(), ec);
            if (ec) {
                fail(name, partial, "cannot create the directory of " + dst + ": " + ec.message());
                return;
            }
            if (linked) {
                if (::link(src.c_str(), dst.c_str()) != 0) {
                    const int err = errno;
                    const bool other_filesystem =
                        err == EXDEV || err == EPERM || err == EOPNOTSUPP || err == EMLINK;
                    if (i == 0 && other_filesystem) {
                        linked = false;
                        link_errno = err;
                    } else {
                        fail(name, partial, "cannot link " + src + ": " + errno_text(err));
                        return;
                    }
                } else {
                    struct stat st{};
                    if (::stat(dst.c_str(), &st) != 0 || static_cast<uint64_t>(st.st_size) != f.size) {
                        fail(name, partial, src + " is not the size the cut listed");
                        return;
                    }
                }
            }
            if (i == 0) {
                const char* method = linked ? "linked" : "copied";
                d.method = method;
                {
                    std::lock_guard<std::mutex> lock(mtx_);
                    progress_.method = method;
                    progress_.phase  = linked ? "link" : "copy";
                }
                if (linked) {
                    OB_LOG_INFO("backup", "Backup %s: linked - %s is on the data directory's "
                                          "filesystem, so the backup shares its device",
                                name.c_str(), dir_.c_str());
                } else {
                    OB_LOG_INFO("backup", "Backup %s: copied%s%s", name.c_str(),
                                link_errno != 0 ? " - a hard link was refused: " : "",
                                link_errno != 0 ? errno_text(link_errno).c_str() : " (forced)");
                    struct statvfs vfs{};
                    if (::statvfs(dir_.c_str(), &vfs) == 0) {
                        const uint64_t free_bytes =
                            static_cast<uint64_t>(vfs.f_bavail) * static_cast<uint64_t>(vfs.f_frsize);
                        if (free_bytes < d.total_bytes + kCopyHeadroom) {
                            fail(name, partial, "the copy needs " + std::to_string(d.total_bytes) +
                                                " byte(s) and " + dir_ + " has " +
                                                std::to_string(free_bytes) + " free");
                            return;
                        }
                    }
                }
            }
            if (!linked) {
                uint32_t crc = 0;
                std::string why;
                if (!copy_file_checked(src, dst, f.size, buf, crc, why, &stop_)) {
                    fail(name, partial, why);
                    return;
                }
                f.crc32c = crc;
            }
            std::lock_guard<std::mutex> lock(mtx_);
            ++progress_.files_done;
            progress_.bytes_done += f.size;
        }
        if (d.files.empty()) d.method = force_copy_.load() ? "copied" : "linked";

        // Everything is linked or copied: merges and retention may go on.
        cut.pin.reset();
        const auto pinned_ms = static_cast<uint64_t>(ms_since(t_pin));
        {
            std::lock_guard<std::mutex> lock(mtx_);
            progress_.pinned_ms = pinned_ms;
        }
        registry_.set_gauge("ob_backup_last_pinned_ms", static_cast<int64_t>(pinned_ms));
        OB_LOG_INFO("backup", "Backup %s: segment files released after %llu ms", name.c_str(),
                    static_cast<unsigned long long>(pinned_ms));

        if (linked) {
            // The fingerprints of what was linked, read from the links: the same inodes, which the
            // engine replaces only by renaming another file over the name.
            {
                std::lock_guard<std::mutex> lock(mtx_);
                progress_.phase = "checksum";
                progress_.files_done = 0;
                progress_.bytes_done = 0;
            }
            for (auto& f : d.files) {
                uint32_t crc = 0;
                uint64_t read = 0;
                std::string why;
                if (!checksum_file(store + "/" + f.path, buf, crc, read, why, &stop_)) {
                    fail(name, partial, why);
                    return;
                }
                if (read != f.size) {
                    fail(name, partial, f.path + " changed size after it was linked");
                    return;
                }
                f.crc32c = crc;
                std::lock_guard<std::mutex> lock(mtx_);
                ++progress_.files_done;
                progress_.bytes_done += f.size;
            }
        }

        set_phase("publish");
        if (stopping()) {
            fail(name, partial, "stopped");
            return;
        }
        if (const int err = sync_filesystem_of(partial); err != 0) {
            fail(name, partial, "cannot sync " + partial + ": " + errno_text(err));
            return;
        }
        d.finished_at_ns = wall_clock_ns();
        if (const int err = write_file_atomically(partial + "/" + kBackupDescriptionFile, d.to_json());
            err != 0) {
            fail(name, partial, "cannot write the description: " + errno_text(err));
            return;
        }
        if (::rename(partial.c_str(), final_dir.c_str()) != 0) {
            const int err = errno;
            fail(name, partial, "cannot rename " + partial + " to " + final_dir + ": " +
                                errno_text(err));
            return;
        }
        created = false;   // published: not this run's to remove any more
        if (const int err = sync_directory(dir_); err != 0) {
            OB_LOG_WARN("backup", "Backup %s is complete, but %s could not be synced (%s): a power cut "
                                  "now may take it back to %s%s, which is not a backup",
                        name.c_str(), dir_.c_str(), errno_text(err).c_str(), kBackupPartialPrefix,
                        name.c_str());
        }

        const auto elapsed_ms = static_cast<uint64_t>(ms_since(started_));
        {
            std::lock_guard<std::mutex> lock(mtx_);
            progress_.state      = BackupProgress::State::Done;
            progress_.phase.clear();
            progress_.elapsed_ms = elapsed_ms;
        }
        registry_.increment_counter("ob_backups_total");
        registry_.set_gauge("ob_backup_running", 0);
        registry_.set_gauge("ob_backup_last_success_timestamp_seconds",
                            static_cast<int64_t>(d.cut_at_ns / 1'000'000'000ULL));
        registry_.set_gauge("ob_backup_last_duration_ms", static_cast<int64_t>(elapsed_ms));
        registry_.set_gauge("ob_backup_last_bytes", static_cast<int64_t>(d.total_bytes));
        OB_LOG_INFO("backup", "Backup %s complete in %s: %s, %zu file(s), %llu byte(s), %llu row(s); "
                              "cut %.1f ms, pinned %llu ms, total %llu ms",
                    name.c_str(), final_dir.c_str(), d.method.c_str(), d.files.size(),
                    static_cast<unsigned long long>(d.total_bytes),
                    static_cast<unsigned long long>(d.total_rows), cut_ms,
                    static_cast<unsigned long long>(pinned_ms),
                    static_cast<unsigned long long>(elapsed_ms));
    } catch (const std::exception& e) {
        fail(name, created ? partial : "", std::string("unexpected: ") + e.what());
    }
}

}  // namespace ob
