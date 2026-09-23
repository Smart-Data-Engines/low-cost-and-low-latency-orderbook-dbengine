#pragma once

// How many CPUs this process can actually use (stage 3 of using the whole machine).
//
// Not `std::thread::hardware_concurrency()`, which answers "how many CPUs does the kernel have" and
// is wrong in the two places an engine is most often run: under `taskset` or a cpuset, which
// narrow the CPUs a process may be scheduled on, and in a container, whose cgroup caps the CPU
// *time* it may use whatever it may be scheduled on. A server that sized its event loops from the
// first number on a four-core container capped at one CPU would run four spinning loops on one
// CPU's worth of time.

#include <functional>
#include <optional>
#include <string>

namespace ob {

/// What the process can see of the machine, as data. `read_system_view()` fills it from the running
/// system; a test fills it from literals, so every case the requirements name is a test rather than
/// a machine to go and find.
struct SystemView {
    /// CPUs the affinity mask allows (`sched_getaffinity`), or nothing if it could not be read.
    std::optional<unsigned> affinity_cpus;
    /// CPUs the kernel has online - what is used, and said, when the mask is unknown.
    unsigned online_cpus{0};
    /// `/proc/self/cgroup`, verbatim, or nothing if it could not be read.
    std::optional<std::string> proc_self_cgroup;
    /// Where the cgroup filesystems are mounted.
    std::string cgroup_root{"/sys/fs/cgroup"};
    /// Reads one file, or nothing if it cannot be read. A function so that a test can answer for a
    /// whole hierarchy with a map, and so that "unreadable" is a distinct answer from "no limit".
    std::function<std::optional<std::string>(const std::string& path)> read_file;
};

/// How many CPUs this process can use, and how that was worked out.
struct MachineResources {
    /// What the affinity mask allows; the online count when the mask was unknown.
    unsigned affinity_cpus{1};
    /// The tightest CPU limit a cgroup puts on this process, in CPUs (1.5 for `150000 100000`), or
    /// nothing if none was found. Nothing is also the answer when a limit could not be read - the
    /// `reason` says which of the two it was.
    std::optional<double> quota_cpus;
    /// `max(1, min(affinity_cpus, floor(quota_cpus)))` - what the engine may size itself to.
    unsigned usable_cpus{1};
    /// One line for the log and for `--print-config`: the number and each part of it.
    std::string reason;
};

/// Work out the CPUs this process can use from what it can see. Pure: the same view gives the same
/// answer, and nothing is read here that the view does not hold.
///
/// cgroup v2 (`0::<path>` in `/proc/self/cgroup`): `cpu.max` of that cgroup and of **every ancestor**
/// up to the mount's root, because a parent's limit binds its children - the tightest wins. cgroup
/// v1 (a line whose controllers include `cpu`): `cpu.cfs_quota_us` over `cpu.cfs_period_us` the same
/// way, in the `cpu` controller's mount. The mount's root is read as well as the named path, which
/// is what finds the limit inside a container, where the named path usually does not exist there.
/// A file that cannot be read is **no information**, not "no limit", and the reason says it.
MachineResources detect_machine(const SystemView& view);

/// The view of the running system: `sched_getaffinity`, `/proc/self/cgroup` and a reader for the
/// cgroup files.
SystemView read_system_view();

} // namespace ob
