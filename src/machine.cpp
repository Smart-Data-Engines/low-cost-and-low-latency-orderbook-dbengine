// How many CPUs this process can use: the affinity mask, and the tightest cgroup CPU limit on the
// way up from this process's cgroup. See machine.hpp for why neither alone is the answer.

#include "orderbook/machine.hpp"

#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <sstream>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sched.h>
#include <unistd.h>

namespace ob {

namespace {

/// One line of `/proc/self/cgroup`: `hierarchy-ID:controllers:path`.
struct CgroupLine {
    std::string controllers;
    std::string path;
};

std::vector<CgroupLine> parse_proc_self_cgroup(const std::string& text) {
    std::vector<CgroupLine> lines;
    std::istringstream in(text);
    std::string line;
    while (std::getline(in, line)) {
        const auto first = line.find(':');
        if (first == std::string::npos) continue;
        const auto second = line.find(':', first + 1);
        if (second == std::string::npos) continue;
        lines.push_back({line.substr(first + 1, second - first - 1), line.substr(second + 1)});
    }
    return lines;
}

/// Whether a comma-separated controller list names `wanted` exactly - `cpuacct` is not `cpu`.
bool names_controller(const std::string& controllers, const std::string& wanted) {
    std::size_t start = 0;
    while (start <= controllers.size()) {
        const std::size_t comma = controllers.find(',', start);
        const std::string name =
            controllers.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
        if (name == wanted) return true;
        if (comma == std::string::npos) break;
        start = comma + 1;
    }
    return false;
}

/// The directories from `path` up to the root: `/a/b` gives `/a/b`, `/a` and `` (the root itself,
/// spelled empty so that joining it to a mount adds nothing).
std::vector<std::string> self_and_ancestors(std::string path) {
    std::vector<std::string> dirs;
    while (!path.empty() && path.back() == '/') path.pop_back();
    while (!path.empty()) {
        dirs.push_back(path);
        const auto slash = path.rfind('/');
        path = (slash == std::string::npos) ? std::string() : path.substr(0, slash);
    }
    dirs.push_back(std::string());
    return dirs;
}

std::optional<double> number(const std::string& text) {
    // strtod on the first token; a file that is not a number is no information, not zero.
    std::istringstream in(text);
    std::string token;
    if (!(in >> token)) return std::nullopt;
    char* end = nullptr;
    errno = 0;
    const double value = std::strtod(token.c_str(), &end);
    if (errno != 0 || end == token.c_str() || *end != '\0') return std::nullopt;
    return value;
}

/// What reading one level said: a limit, "no limit", no file there (a level that limits nothing),
/// or a file there that could not be read - or held something that is not a limit.
enum class Level { Limit, Unlimited, Absent, Unreadable };

struct Reading {
    Level       level{Level::Absent};
    double      cpus{0};
    std::string where;
};

/// cgroup v2: `cpu.max` is `max <period>` or `<quota> <period>`, and does not exist where the CPU
/// controller is not enabled.
Reading read_v2(const SystemView& view, const std::string& dir) {
    Reading r;
    r.where = view.cgroup_root + dir + "/cpu.max";
    const FileRead file = view.read_file(r.where);
    if (file.status == FileRead::Status::Absent) return r;
    r.level = Level::Unreadable;
    if (file.status != FileRead::Status::Read) return r;
    std::istringstream in(file.text);
    std::string quota, period;
    if (!(in >> quota >> period)) return r;
    if (quota == "max") {
        r.level = Level::Unlimited;
        return r;
    }
    const auto q = number(quota);
    const auto p = number(period);
    if (!q || !p || *q <= 0 || *p <= 0) return r;   // nonsense is no information, not a zero limit
    r.level = Level::Limit;
    r.cpus  = *q / *p;
    return r;
}

/// cgroup v1: `cpu.cfs_quota_us` (-1 for none) over `cpu.cfs_period_us`, in whichever of the usual
/// mount names this system gives the `cpu` controller.
Reading read_v1(const SystemView& view, const std::string& dir) {
    Reading r;
    for (const char* mount : {"/cpu", "/cpu,cpuacct", "/cpuacct,cpu"}) {
        const std::string base = view.cgroup_root + mount + dir;
        const FileRead quota_file = view.read_file(base + "/cpu.cfs_quota_us");
        if (quota_file.status == FileRead::Status::Absent) continue;
        r.where = base + "/cpu.cfs_quota_us";
        r.level = Level::Unreadable;
        if (quota_file.status != FileRead::Status::Read) return r;
        const auto quota = number(quota_file.text);
        if (!quota) return r;
        if (*quota < 0) {
            r.level = Level::Unlimited;
            return r;
        }
        const FileRead period_file = view.read_file(base + "/cpu.cfs_period_us");
        const std::optional<double> period = period_file.status == FileRead::Status::Read
                                                 ? number(period_file.text)
                                                 : std::optional<double>{};
        if (!period || *period <= 0 || *quota <= 0) return r;   // -1 was handled above
        r.level = Level::Limit;
        r.cpus  = *quota / *period;
        return r;
    }
    r.where = view.cgroup_root + "/cpu" + dir + "/cpu.cfs_quota_us";
    return r;
}

std::string format_cpus(double cpus) {
    // Two places, and a whole number of CPUs without them. Decided on the printed text rather than
    // by comparing the value with its floor, because an exact comparison of doubles is a question
    // about representation - and the text is the thing the reason carries.
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%.2f", cpus);
    std::string text(buf);
    if (text.size() > 3 && text.compare(text.size() - 3, 3, ".00") == 0) text.resize(text.size() - 3);
    return text;
}

} // namespace

MachineResources detect_machine(const SystemView& view) {
    MachineResources m;
    const bool affinity_known = view.affinity_cpus.has_value() && *view.affinity_cpus > 0;
    m.affinity_cpus = affinity_known ? *view.affinity_cpus : std::max(1u, view.online_cpus);

    // Which hierarchy holds the cpu controller. On a hybrid system both kinds of line are present
    // and the controller is v1's - the unified hierarchy has no `cpu.max`, and reading it would
    // find none and report "no limit" about a hierarchy that does not hold the controller.
    std::optional<std::string> v1_path;
    std::optional<std::string> v2_path;
    if (view.proc_self_cgroup) {
        for (const CgroupLine& line : parse_proc_self_cgroup(*view.proc_self_cgroup)) {
            if (line.controllers.empty()) {
                v2_path = line.path;
            } else if (names_controller(line.controllers, "cpu")) {
                v1_path = line.path;
            }
        }
    }

    std::string limit_where;
    int read = 0;         // levels whose file said a limit or "no limit"
    int unreadable = 0;   // levels whose file exists and said neither
    int levels = 0;
    const char* hierarchy = nullptr;
    if (v1_path || v2_path) {
        hierarchy = v1_path ? "cgroup v1" : "cgroup v2";
        for (const std::string& dir : self_and_ancestors(v1_path ? *v1_path : *v2_path)) {
            const Reading r = v1_path ? read_v1(view, dir) : read_v2(view, dir);
            ++levels;
            if (r.level == Level::Absent) continue;
            if (r.level == Level::Unreadable) {
                ++unreadable;
                continue;
            }
            ++read;
            if (r.level == Level::Limit && (!m.quota_cpus || r.cpus < *m.quota_cpus)) {
                m.quota_cpus = r.cpus;
                limit_where  = r.where;
            }
        }
    }

    unsigned usable = m.affinity_cpus;
    if (m.quota_cpus) {
        const double floored = std::floor(*m.quota_cpus);
        usable = std::min<unsigned>(usable, floored < 1.0 ? 1u : static_cast<unsigned>(floored));
    }
    m.usable_cpus = std::max(1u, usable);

    std::string reason = std::to_string(m.usable_cpus) +
                         (m.usable_cpus == 1 ? " usable CPU: " : " usable CPUs: ");
    reason += affinity_known ? "affinity " + std::to_string(m.affinity_cpus)
                             : "affinity unknown, " + std::to_string(m.affinity_cpus) + " online";
    if (!hierarchy) {
        reason += view.proc_self_cgroup ? ", not in a cgroup with a CPU controller"
                                        : ", /proc/self/cgroup unreadable so no cgroup limit known";
    } else {
        const std::string h(hierarchy);
        if (m.quota_cpus) {
            reason += ", " + h + " limit " + format_cpus(*m.quota_cpus) + " CPUs (" + limit_where +
                      ")";
        } else if (read > 0) {
            reason += ", no " + h + " CPU limit";
        } else if (unreadable > 0) {
            reason += ", " + h + " limit unknown - no level could be read, so none is assumed";
        } else if (v1_path) {
            // Under v1 the files exist in every cgroup of the `cpu` hierarchy, so finding none of
            // them says where the hierarchy is not mounted, not that it limits nothing.
            reason += ", " + h + " limit unknown - the cpu controller's files are not under " +
                      view.cgroup_root + ", so none is assumed";
        } else {
            // No `cpu.max` anywhere on the path: the CPU controller is not enabled for it.
            reason += ", no " + h + " CPU limit";
        }
        if (unreadable > 0 && read > 0) {
            reason += " - " + std::to_string(unreadable) + " of " + std::to_string(levels) +
                      " levels could not be read, so a limit there is not known";
        }
    }
    m.reason = reason;
    return m;
}

SystemView read_system_view() {
    SystemView view;

    const long online = ::sysconf(_SC_NPROCESSORS_ONLN);
    view.online_cpus = online > 0 ? static_cast<unsigned>(online) : 1u;

    // The mask, sized to the machine: a fixed cpu_set_t holds 1024 CPUs and sched_getaffinity
    // refuses a smaller buffer than the kernel's mask with EINVAL.
    for (int cpus = 1024; cpus <= (1 << 16); cpus *= 2) {
        cpu_set_t* set = CPU_ALLOC(cpus);
        if (!set) break;
        const std::size_t size = CPU_ALLOC_SIZE(cpus);
        CPU_ZERO_S(size, set);
        if (::sched_getaffinity(0, size, set) == 0) {
            view.affinity_cpus = static_cast<unsigned>(CPU_COUNT_S(size, set));
            CPU_FREE(set);
            break;
        }
        const int err = errno;
        CPU_FREE(set);
        if (err != EINVAL) break;
    }

    // POSIX rather than a stream, for errno: a file that is not there and a file that is there and
    // cannot be read are different answers about a cgroup (see FileRead).
    view.read_file = [](const std::string& path) -> FileRead {
        FileRead out;
        const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) {
            out.status = (errno == ENOENT || errno == ENOTDIR) ? FileRead::Status::Absent
                                                               : FileRead::Status::Unreadable;
            return out;
        }
        char buf[4096];
        for (;;) {
            const ssize_t n = ::read(fd, buf, sizeof(buf));
            if (n > 0) {
                out.text.append(buf, static_cast<std::size_t>(n));
                continue;
            }
            if (n == 0) break;
            if (errno == EINTR) continue;
            ::close(fd);
            out.status = FileRead::Status::Unreadable;
            out.text.clear();
            return out;
        }
        ::close(fd);
        out.status = FileRead::Status::Read;
        return out;
    };
    const FileRead cgroup = view.read_file("/proc/self/cgroup");
    if (cgroup.status == FileRead::Status::Read) view.proc_self_cgroup = cgroup.text;
    return view;
}

} // namespace ob
