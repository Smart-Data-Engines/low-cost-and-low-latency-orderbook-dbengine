// How many CPUs this process can use: the affinity mask, and the tightest cgroup CPU limit on the
// way up from this process's cgroup. See machine.hpp for why neither alone is the answer.

#include "orderbook/machine.hpp"

#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

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

/// What reading one level said: a limit, "no limit", or nothing readable.
enum class Level { Limit, Unlimited, Unreadable };

struct Reading {
    Level       level{Level::Unreadable};
    double      cpus{0};
    std::string where;
};

/// cgroup v2: `cpu.max` is `max <period>` or `<quota> <period>`.
Reading read_v2(const SystemView& view, const std::string& dir) {
    Reading r;
    r.where = view.cgroup_root + dir + "/cpu.max";
    const auto text = view.read_file(r.where);
    if (!text) return r;
    std::istringstream in(*text);
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
        const auto quota_text = view.read_file(base + "/cpu.cfs_quota_us");
        if (!quota_text) continue;
        r.where = base + "/cpu.cfs_quota_us";
        const auto quota = number(*quota_text);
        if (!quota) return r;
        if (*quota < 0) {
            r.level = Level::Unlimited;
            return r;
        }
        const auto period_text = view.read_file(base + "/cpu.cfs_period_us");
        const std::optional<double> period =
            period_text ? number(*period_text) : std::optional<double>{};
        if (!period || *period <= 0 || *quota == 0) return r;
        r.level = Level::Limit;
        r.cpus  = *quota / *period;
        return r;
    }
    r.where = view.cgroup_root + "/cpu" + dir + "/cpu.cfs_quota_us";
    return r;
}

std::string format_cpus(double cpus) {
    char buf[32];
    if (cpus == std::floor(cpus)) {
        std::snprintf(buf, sizeof(buf), "%.0f", cpus);
    } else {
        std::snprintf(buf, sizeof(buf), "%.2f", cpus);
    }
    return buf;
}

} // namespace

MachineResources detect_machine(const SystemView& view) {
    MachineResources m;
    const bool affinity_known = view.affinity_cpus.has_value() && *view.affinity_cpus > 0;
    m.affinity_cpus = affinity_known ? *view.affinity_cpus : std::max(1u, view.online_cpus);

    // Which hierarchy holds the cpu controller. On a hybrid system both kinds of line are present
    // and the controller is v1's - the unified hierarchy has no `cpu.max` to read, and reading it
    // would report "unreadable" about a file that was never going to exist.
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
    int readable = 0;
    int levels = 0;
    const char* hierarchy = nullptr;
    if (v1_path || v2_path) {
        hierarchy = v1_path ? "cgroup v1" : "cgroup v2";
        for (const std::string& dir : self_and_ancestors(v1_path ? *v1_path : *v2_path)) {
            const Reading r = v1_path ? read_v1(view, dir) : read_v2(view, dir);
            ++levels;
            if (r.level == Level::Unreadable) continue;
            ++readable;
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
    } else if (m.quota_cpus) {
        reason += ", " + std::string(hierarchy) + " limit " + format_cpus(*m.quota_cpus) +
                  " CPUs (" + limit_where + ")";
    } else if (readable == 0) {
        reason += ", " + std::string(hierarchy) + " limit unknown - none of " +
                  std::to_string(levels) + " levels could be read, so none is assumed";
    } else {
        reason += ", no " + std::string(hierarchy) + " CPU limit";
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

    view.read_file = [](const std::string& path) -> std::optional<std::string> {
        std::ifstream in(path);
        if (!in) return std::nullopt;
        std::ostringstream text;
        text << in.rdbuf();
        if (in.bad()) return std::nullopt;
        return text.str();
    };
    view.proc_self_cgroup = view.read_file("/proc/self/cgroup");
    return view;
}

} // namespace ob
