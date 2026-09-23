// How many CPUs this process can use (stage 3 of using the whole machine): the affinity mask and
// the tightest cgroup CPU limit on the way up.
//
// Every case is a view written as literals, because the machines that produce them - a cpuset, a
// container with a fractional quota, a cgroup v1 host, a file the process may not read - are not
// the machine the tests run on. The last two tests are about the real reader, and one of them
// narrows its own thread's mask so that it knows what the answer must be.

#include "orderbook/machine.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <string>

#include <sched.h>

namespace {

/// A view whose files are a map: a path in `unreadable` exists and cannot be read, and a path in
/// neither does not exist.
ob::SystemView view_of(unsigned affinity, std::string proc_self_cgroup,
                       std::map<std::string, std::string> files,
                       std::set<std::string> unreadable = {}) {
    ob::SystemView v;
    v.affinity_cpus    = affinity;
    v.online_cpus      = affinity;
    v.proc_self_cgroup = std::move(proc_self_cgroup);
    v.read_file = [files = std::move(files),
                   unreadable = std::move(unreadable)](const std::string& path) -> ob::FileRead {
        if (unreadable.count(path)) return {ob::FileRead::Status::Unreadable, {}};
        const auto it = files.find(path);
        if (it == files.end()) return {ob::FileRead::Status::Absent, {}};
        return {ob::FileRead::Status::Read, it->second};
    };
    return v;
}

constexpr const char* kV2 = "0::/user.slice/app.scope\n";
constexpr const char* kLeafV2 = "/sys/fs/cgroup/user.slice/app.scope/cpu.max";
constexpr const char* kParentV2 = "/sys/fs/cgroup/user.slice/cpu.max";

} // namespace

TEST(Machine, NoLimitMeansTheAffinityMask) {
    const auto m = ob::detect_machine(
        view_of(4, kV2, {{kLeafV2, "max 100000\n"}, {kParentV2, "max 100000\n"}}));
    EXPECT_EQ(m.usable_cpus, 4u);
    EXPECT_EQ(m.affinity_cpus, 4u);
    EXPECT_FALSE(m.quota_cpus.has_value());
    EXPECT_EQ(m.reason, "4 usable CPUs: affinity 4, no cgroup v2 CPU limit");
}

TEST(Machine, AFractionalLimitIsRoundedDownNotUp) {
    // One and a half CPUs of time is one CPU the engine can keep busy; two loops spinning on it
    // would each get three quarters.
    const auto m = ob::detect_machine(view_of(4, kV2, {{kLeafV2, "150000 100000\n"}}));
    ASSERT_TRUE(m.quota_cpus.has_value());
    EXPECT_DOUBLE_EQ(*m.quota_cpus, 1.5);
    EXPECT_EQ(m.usable_cpus, 1u);
    EXPECT_NE(m.reason.find("cgroup v2 limit 1.50 CPUs"), std::string::npos) << m.reason;
    EXPECT_NE(m.reason.find(kLeafV2), std::string::npos) << m.reason;
}

TEST(Machine, ALimitBelowTheMaskIsTheAnswer) {
    const auto m = ob::detect_machine(view_of(8, kV2, {{kLeafV2, "200000 100000\n"}}));
    EXPECT_EQ(m.usable_cpus, 2u);
}

TEST(Machine, ALimitAboveTheMaskChangesNothing) {
    const auto m = ob::detect_machine(view_of(2, kV2, {{kLeafV2, "800000 100000\n"}}));
    EXPECT_EQ(m.usable_cpus, 2u);
    ASSERT_TRUE(m.quota_cpus.has_value());
    EXPECT_DOUBLE_EQ(*m.quota_cpus, 8.0);
}

TEST(Machine, AParentsLimitBindsItsChild) {
    // The leaf says "max" and its parent says one CPU: the kernel enforces both, so the answer is
    // one - reading the leaf alone would say four.
    const auto m = ob::detect_machine(
        view_of(4, kV2, {{kLeafV2, "max 100000\n"}, {kParentV2, "100000 100000\n"}}));
    EXPECT_EQ(m.usable_cpus, 1u);
    EXPECT_NE(m.reason.find(kParentV2), std::string::npos) << m.reason;
}

TEST(Machine, TheTighterOfTwoLimitsWins) {
    const auto m = ob::detect_machine(
        view_of(8, kV2, {{kLeafV2, "300000 100000\n"}, {kParentV2, "200000 100000\n"}}));
    EXPECT_EQ(m.usable_cpus, 2u);
}

TEST(Machine, InsideAContainerTheLimitIsAtTheMountsRoot) {
    // With a cgroup namespace the process sees itself at `/`, and its limit is the root's file.
    const auto m = ob::detect_machine(
        view_of(16, "0::/\n", {{"/sys/fs/cgroup/cpu.max", "300000 100000\n"}}));
    EXPECT_EQ(m.usable_cpus, 3u);
}

TEST(Machine, CgroupV1IsQuotaOverPeriod) {
    const auto m = ob::detect_machine(view_of(
        8, "4:cpu,cpuacct:/docker/abc\n1:name=systemd:/docker/abc\n",
        {{"/sys/fs/cgroup/cpu,cpuacct/docker/abc/cpu.cfs_quota_us", "300000\n"},
         {"/sys/fs/cgroup/cpu,cpuacct/docker/abc/cpu.cfs_period_us", "100000\n"}}));
    EXPECT_EQ(m.usable_cpus, 3u);
    EXPECT_NE(m.reason.find("cgroup v1 limit 3 CPUs"), std::string::npos) << m.reason;
}

TEST(Machine, CgroupV1MinusOneIsNoLimit) {
    const auto m = ob::detect_machine(view_of(
        4, "4:cpu,cpuacct:/\n", {{"/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us", "-1\n"},
                                 {"/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_period_us", "100000\n"}}));
    EXPECT_EQ(m.usable_cpus, 4u);
    EXPECT_FALSE(m.quota_cpus.has_value());
    EXPECT_EQ(m.reason, "4 usable CPUs: affinity 4, no cgroup v1 CPU limit");
}

TEST(Machine, AHybridSystemReadsTheControllerWhereItIs) {
    // Both kinds of line: the cpu controller is v1's, and the unified hierarchy has no cpu.max, so
    // reading it would find none and report "no limit" - about the hierarchy that does not hold it.
    const auto m = ob::detect_machine(view_of(
        4, "0::/app\n3:cpu,cpuacct:/app\n",
        {{"/sys/fs/cgroup/cpu,cpuacct/app/cpu.cfs_quota_us", "200000\n"},
         {"/sys/fs/cgroup/cpu,cpuacct/app/cpu.cfs_period_us", "100000\n"}}));
    EXPECT_EQ(m.usable_cpus, 2u);
    EXPECT_NE(m.reason.find("cgroup v1"), std::string::npos) << m.reason;
}

TEST(Machine, TheControllerIsMatchedByNameNotBySubstring) {
    // `cpuacct` accounts for CPU time and limits nothing.
    const auto m = ob::detect_machine(view_of(
        4, "3:cpuacct:/app\n", {{"/sys/fs/cgroup/cpuacct/app/cpu.cfs_quota_us", "100000\n"}}));
    EXPECT_EQ(m.usable_cpus, 4u);
    EXPECT_EQ(m.reason, "4 usable CPUs: affinity 4, not in a cgroup with a CPU controller");
}

TEST(Machine, AFileThatCannotBeReadIsNoInformationAndSaysSo) {
    const auto m = ob::detect_machine(view_of(4, kV2, {}, {kLeafV2, kParentV2}));
    EXPECT_EQ(m.usable_cpus, 4u);
    EXPECT_FALSE(m.quota_cpus.has_value());
    EXPECT_NE(m.reason.find("limit unknown"), std::string::npos) << m.reason;
    EXPECT_NE(m.reason.find("could be read"), std::string::npos) << m.reason;
}

TEST(Machine, NoCpuMaxAnywhereIsNoLimit) {
    // The hierarchy's root never has a cpu.max, and a cgroup has one only where the CPU controller
    // is enabled: a path with none limits nothing, which is a different answer from "unknown".
    const auto m = ob::detect_machine(view_of(4, kV2, {}));
    EXPECT_EQ(m.usable_cpus, 4u);
    EXPECT_FALSE(m.quota_cpus.has_value());
    EXPECT_EQ(m.reason, "4 usable CPUs: affinity 4, no cgroup v2 CPU limit");
}

TEST(Machine, AnUnreadableLevelIsSaidBesideTheLevelsThatWereRead) {
    // The leaf says "max" and its parent cannot be read: a limit there would bind, so "no limit"
    // on its own would say more than was found.
    const auto m = ob::detect_machine(view_of(4, kV2, {{kLeafV2, "max 100000\n"}}, {kParentV2}));
    EXPECT_EQ(m.usable_cpus, 4u);
    EXPECT_FALSE(m.quota_cpus.has_value());
    EXPECT_EQ(m.reason, "4 usable CPUs: affinity 4, no cgroup v2 CPU limit - 1 of 3 levels could "
                        "not be read, so a limit there is not known");
}

TEST(Machine, AnUnreadableLevelDoesNotHideALimitFoundAtAnother) {
    const auto m =
        ob::detect_machine(view_of(4, kV2, {{kLeafV2, "150000 100000\n"}}, {kParentV2}));
    EXPECT_EQ(m.usable_cpus, 1u);
    EXPECT_NE(m.reason.find("cgroup v2 limit 1.50 CPUs"), std::string::npos) << m.reason;
    EXPECT_NE(m.reason.find("1 of 3 levels could not be read"), std::string::npos) << m.reason;
}

TEST(Machine, ACgroupV1HierarchyNotInViewIsNoInformation) {
    // Under v1 every cgroup of the cpu hierarchy has the files, so none of them in view says the
    // hierarchy is mounted somewhere this reader does not look - not that it limits nothing.
    const auto m = ob::detect_machine(view_of(4, "4:cpu,cpuacct:/docker/abc\n", {}));
    EXPECT_EQ(m.usable_cpus, 4u);
    EXPECT_FALSE(m.quota_cpus.has_value());
    EXPECT_NE(m.reason.find("cgroup v1 limit unknown"), std::string::npos) << m.reason;
    EXPECT_NE(m.reason.find("not under /sys/fs/cgroup"), std::string::npos) << m.reason;
}

TEST(Machine, NonsenseInALimitFileIsNoInformationNotAZeroLimit) {
    for (const char* text : {"garbage\n", "0 100000\n", "-5 100000\n", "150000 0\n", ""}) {
        const auto m = ob::detect_machine(view_of(4, kV2, {{kLeafV2, text}}));
        EXPECT_EQ(m.usable_cpus, 4u) << "cpu.max \"" << text << "\"";
        EXPECT_FALSE(m.quota_cpus.has_value()) << "cpu.max \"" << text << "\"";
        EXPECT_NE(m.reason.find("limit unknown"), std::string::npos)
            << "cpu.max \"" << text << "\": " << m.reason;
    }
}

TEST(Machine, AnUnknownMaskFallsBackToTheOnlineCountAndSaysSo) {
    ob::SystemView v = view_of(0, kV2, {{kLeafV2, "max 100000\n"}});
    v.affinity_cpus.reset();
    v.online_cpus = 6;
    const auto m = ob::detect_machine(v);
    EXPECT_EQ(m.usable_cpus, 6u);
    EXPECT_NE(m.reason.find("affinity unknown, 6 online"), std::string::npos) << m.reason;
}

TEST(Machine, NoProcSelfCgroupIsSaid) {
    ob::SystemView v = view_of(4, "", {});
    v.proc_self_cgroup.reset();
    const auto m = ob::detect_machine(v);
    EXPECT_EQ(m.usable_cpus, 4u);
    EXPECT_NE(m.reason.find("/proc/self/cgroup unreadable"), std::string::npos) << m.reason;
}

TEST(Machine, TheSmallestAnswerIsOne) {
    const auto m = ob::detect_machine(view_of(4, kV2, {{kLeafV2, "10000 100000\n"}}));
    EXPECT_EQ(m.usable_cpus, 1u) << "a tenth of a CPU is still one CPU to run on";
    EXPECT_EQ(m.reason.rfind("1 usable CPU: ", 0), 0u) << m.reason;
}

// ── The real reader ───────────────────────────────────────────────────────────

TEST(MachineRealSystem, TheReaderSeesTheMaskItIsGiven) {
    // `taskset` narrows the mask of a process; this narrows this thread's to one CPU, reads, and
    // puts it back - so the test knows the answer rather than trusting a number it cannot check.
    cpu_set_t before;
    CPU_ZERO(&before);
    ASSERT_EQ(::sched_getaffinity(0, sizeof(before), &before), 0);
    int first = -1;
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (CPU_ISSET(cpu, &before)) { first = cpu; break; }
    }
    ASSERT_GE(first, 0);

    cpu_set_t one;
    CPU_ZERO(&one);
    CPU_SET(first, &one);
    ASSERT_EQ(::sched_setaffinity(0, sizeof(one), &one), 0);
    const ob::SystemView narrowed = ob::read_system_view();
    ASSERT_EQ(::sched_setaffinity(0, sizeof(before), &before), 0);

    ASSERT_TRUE(narrowed.affinity_cpus.has_value());
    EXPECT_EQ(*narrowed.affinity_cpus, 1u);
    EXPECT_EQ(ob::detect_machine(narrowed).usable_cpus, 1u);

    const ob::SystemView whole = ob::read_system_view();
    ASSERT_TRUE(whole.affinity_cpus.has_value());
    EXPECT_EQ(*whole.affinity_cpus, static_cast<unsigned>(CPU_COUNT(&before)))
        << "the reader and sched_getaffinity disagree about the restored mask";
}

TEST(MachineRealSystem, TheReaderTellsAFileThatIsNotThereFromOneThatCannotBeRead) {
    // The two answers mean different things about a cgroup, and both are made here with nothing but
    // a temporary directory: a directory opens and cannot be read (EISDIR), whoever runs the test -
    // a file with mode 000 would not do, because root reads it anyway.
    char tmpl[] = "/tmp/ob-machine-XXXXXX";
    ASSERT_NE(::mkdtemp(tmpl), nullptr);
    const std::filesystem::path dir(tmpl);
    { std::ofstream(dir / "cpu.max") << "max 100000\n"; }

    const ob::SystemView v = ob::read_system_view();
    const ob::FileRead file = v.read_file((dir / "cpu.max").string());
    const ob::FileRead missing = v.read_file((dir / "missing").string());
    const ob::FileRead under_a_file = v.read_file((dir / "cpu.max" / "x").string());
    const ob::FileRead a_directory = v.read_file(dir.string());
    std::filesystem::remove_all(dir);

    EXPECT_EQ(file.status, ob::FileRead::Status::Read);
    EXPECT_EQ(file.text, "max 100000\n");
    EXPECT_EQ(missing.status, ob::FileRead::Status::Absent);
    EXPECT_EQ(under_a_file.status, ob::FileRead::Status::Absent) << "ENOTDIR is not being there";
    EXPECT_EQ(a_directory.status, ob::FileRead::Status::Unreadable);
}

TEST(MachineRealSystem, TheAnswerOnThisMachineHasAReason) {
    const ob::MachineResources m = ob::detect_machine(ob::read_system_view());
    EXPECT_GE(m.usable_cpus, 1u);
    EXPECT_LE(m.usable_cpus, m.affinity_cpus);
    EXPECT_FALSE(m.reason.empty());
    std::printf("[ machine  ] %s\n", m.reason.c_str());
}
