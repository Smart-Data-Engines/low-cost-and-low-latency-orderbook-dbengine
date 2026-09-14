// What a mesh epoll event is about — roadmap #128.
//
// `sanitizers-integration (tsan)` caught the mesh io loop closing the client port's epoll instance:
// descriptor 11 was created by `epoll_create1()` in `TcpServer::run()` and closed by
// `MultiMasterManager::io_loop()`. The loop keyed its registrations by descriptor number, three
// threads other than the loop close peer sockets, and a number is the kernel's to hand on the
// moment it is closed — so the branch for "a descriptor in the epoll set with no record behind it"
// closed whatever had inherited the number.
//
// Two things are pinned here. The reserved keys cannot collide with a connection, which is what
// lets the loop tell its own two long-lived descriptors from a peer without asking about numbers.
// And the kernel's own bookkeeping is what makes the fix true rather than merely careful: closing a
// descriptor removes it from every epoll set, so an event about a connection that is gone has
// nothing left to disarm. That second one is measured against `dup2`, which forces the descriptor
// reuse this defect needs instead of hoping for it.

#include "orderbook/multi_master.hpp"

#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <fstream>
#include <sstream>
#include <string>

namespace {

std::string read_file(const std::string& path) {
    std::ifstream in(path);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

}  // namespace

TEST(MeshEventKey, TheReservedKeysCannotBeAConnection) {
    // `conn_id` is minted from `next_conn_id_`, which starts at 1 and only increments, so the two
    // reserved values are outside the range a connection can ever hold. Asserted because the whole
    // dispatch rests on it: a sentinel equal to some connection's id would send that connection's
    // events to the accept loop.
    EXPECT_EQ(ob::kMeshEventWakeup, 0u);
    EXPECT_EQ(ob::kMeshEventListen, ~uint64_t{0});
    EXPECT_NE(ob::kMeshEventWakeup, ob::kMeshEventListen);

    // The first conn_id a manager mints, and the largest one this key can carry, are both usable.
    EXPECT_NE(uint64_t{1}, ob::kMeshEventWakeup);
    EXPECT_NE(uint64_t{1}, ob::kMeshEventListen);
    EXPECT_NE(~uint64_t{0} - 1, ob::kMeshEventWakeup);
    EXPECT_NE(~uint64_t{0} - 1, ob::kMeshEventListen);
}

TEST(MeshEventKey, ClosingADescriptorTakesItsRegistrationWithIt) {
    // Why the loop can ignore an event whose connection is gone, and why it must not close
    // anything: the registration died with the descriptor, and the number may already belong to
    // something else. Both halves measured here rather than argued.
    const int epfd = ::epoll_create1(0);
    ASSERT_GE(epfd, 0);

    int sv[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);

    struct epoll_event ev{};
    ev.events   = EPOLLIN | EPOLLET;
    ev.data.u64 = 7;   // a plausible conn_id
    ASSERT_EQ(::epoll_ctl(epfd, EPOLL_CTL_ADD, sv[0], &ev), 0);

    // The registration is real while the descriptor is open.
    ASSERT_EQ(::epoll_ctl(epfd, EPOLL_CTL_MOD, sv[0], &ev), 0);

    const int recycled = sv[0];
    ASSERT_EQ(::close(sv[0]), 0);

    // Force the reuse this defect needs: `dup2` puts another descriptor on exactly that number, the
    // way `epoll_create1()` did in the failure. Without dup2 this test would be a bet on the
    // allocator.
    const int other = ::epoll_create1(0);
    ASSERT_GE(other, 0);
    ASSERT_EQ(::dup2(other, recycled), recycled);

    // The set does not hold the number any more — so there was nothing for the loop to disarm, and
    // a delete aimed at the number would have been aimed at the new occupant.
    errno = 0;
    EXPECT_EQ(::epoll_ctl(epfd, EPOLL_CTL_DEL, recycled, nullptr), -1);
    EXPECT_EQ(errno, ENOENT);

    // And the occupant is untouched, which is the property the mesh broke: it closed this.
    EXPECT_EQ(::fcntl(recycled, F_GETFD) >= 0, true);

    ::close(recycled);
    ::close(other);
    ::close(sv[1]);
    ::close(epfd);
}

TEST(MeshEventKey, NoMeshRegistrationCarriesABareDescriptor) {
    // A static check, because the defect is a class and not a line: the seventh registration site
    // someone adds must not be able to reintroduce it. `data.fd` is the field that made an event
    // mean "this number"; every registration in this file now carries `data.u64`, which means
    // "this connection".
    const std::string src = read_file(std::string(OB_SOURCE_DIR) + "/src/multi_master.cpp");
    ASSERT_FALSE(src.empty()) << "could not read src/multi_master.cpp; the check would pass by "
                                 "finding nothing";

    // The control: the thing being searched for exists in the file in its other spelling, so a
    // typo in the needle shows up as a failure here rather than as a clean run.
    EXPECT_NE(src.find("ev.data.u64"), std::string::npos)
        << "no registration in this file carries data.u64, which means this test is looking for "
           "the wrong thing rather than that the file is clean";

    EXPECT_EQ(src.find("data.fd"), std::string::npos)
        << "a registration in src/multi_master.cpp carries a bare descriptor number. An event that "
           "carries a number cannot be told from an event about whatever now holds that number, "
           "and this loop closes things (#128)";

    // And the branch itself closes nothing. This is the assertion the measurement is about: the
    // old branch ended in `::close(ev_fd)`, and what it closed once was another subsystem's epoll
    // instance. Read between the branch's own words and the `continue` that ends it, so that the
    // check is about that branch rather than about the file.
    const size_t branch = src.find("which is already gone");
    ASSERT_NE(branch, std::string::npos)
        << "the branch for an event about a connection that is gone is not where this test expects "
           "it; re-read the io loop before trusting the assertion below, because a search that "
           "finds nothing is not a clean result";
    const size_t branch_end = src.find("continue;", branch);
    ASSERT_NE(branch_end, std::string::npos);
    const std::string body = src.substr(branch, branch_end - branch);
    EXPECT_EQ(body.find("::close("), std::string::npos)
        << "the branch for an event whose connection is gone closes a descriptor again. The number "
           "in that event may belong to anything by now — measured once as the client port's epoll "
           "instance (#128)";
    EXPECT_EQ(body.find("EPOLL_CTL_DEL"), std::string::npos)
        << "the branch deletes a registration by descriptor number. Closing the socket already "
           "removed it; aiming a delete at the number aims it at the new occupant (#128)";
}
