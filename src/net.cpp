#include "orderbook/net.hpp"

#include "orderbook/logger.hpp"
#include "orderbook/socket_options.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <cstddef>
#include <netinet/in.h>
#include <poll.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

namespace ob::net {

namespace {

/// The host without the brackets an IPv6 literal may carry.
std::string_view unbracket(std::string_view host) {
    if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
        return host.substr(1, host.size() - 2);
    }
    return host;
}

bool parse_port(std::string_view text, uint16_t& port) {
    if (text.empty()) return false;
    unsigned value = 0;
    const auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc{} || end != text.data() + text.size() || value > 65535) return false;
    port = static_cast<uint16_t>(value);
    return true;
}

bool set_blocking(int fd, bool blocking) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) return false;
    return ::fcntl(fd, F_SETFL, blocking ? (flags & ~O_NONBLOCK) : (flags | O_NONBLOCK)) == 0;
}

/// One address of a dial: connected within `timeout_ms`, or -1 with `why`.
int dial_one(const addrinfo& ai, uint32_t timeout_ms, std::string& why) {
    const int fd = ::socket(ai.ai_family, SOCK_STREAM, 0);
    if (fd < 0) {
        why = std::string("socket(): ") + std::strerror(errno);
        return -1;
    }
    if (!set_blocking(fd, false)) {
        why = "the socket could not be put in non-blocking mode";
        ::close(fd);
        return -1;
    }
    if (::connect(fd, ai.ai_addr, ai.ai_addrlen) == 0) return fd;   // loopback often completes here
    if (errno != EINPROGRESS) {
        why = std::strerror(errno);
        ::close(fd);
        return -1;
    }
    // POLLOUT is not "connected": a refused connection is writable too, and SO_ERROR says which.
    // A signal interrupts the wait without ending it: the deadline is the one the call began with.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (true) {
        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
                              deadline - std::chrono::steady_clock::now()).count();
        if (left <= 0) {
            why = "no answer within the connect deadline (" + std::to_string(timeout_ms) + " ms)";
            ::close(fd);
            return -1;
        }
        pollfd pfd{};
        pfd.fd     = fd;
        pfd.events = POLLOUT;
        const int ready = ::poll(&pfd, 1, static_cast<int>(left));
        if (ready > 0) break;
        if (ready == 0) {
            why = "no answer within the connect deadline (" + std::to_string(timeout_ms) + " ms)";
            ::close(fd);
            return -1;
        }
        if (errno != EINTR) {
            why = std::string("poll(): ") + std::strerror(errno);
            ::close(fd);
            return -1;
        }
    }
    int so_error = 0;
    socklen_t len = sizeof(so_error);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &len) != 0 || so_error != 0) {
        why = std::strerror(so_error != 0 ? so_error : errno);
        ::close(fd);
        return -1;
    }
    return fd;
}

/// `path` as a Unix socket's address, and how many of its bytes bind() and connect() read. The path
/// has passed valid_unix_path(), so it fits with the NUL after it.
socklen_t unix_address(const std::string& path, sockaddr_un& addr) {
    addr = sockaddr_un{};
    addr.sun_family = AF_UNIX;
    std::memcpy(addr.sun_path, path.data(), path.size());
    return static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + path.size() + 1);
}

}  // namespace

bool split_host_port(std::string_view address, std::string& host, uint16_t& port) {
    if (!address.empty() && address.front() == '[') {
        const size_t close = address.find(']');
        if (close == std::string_view::npos || close + 1 >= address.size() || address[close + 1] != ':') {
            return false;
        }
        if (!parse_port(address.substr(close + 2), port)) return false;
        host = std::string(address.substr(1, close - 1));
        return true;
    }
    const size_t colon = address.rfind(':');
    if (colon == std::string_view::npos) return false;
    // A second colon without brackets is an IPv6 literal whose port cannot be told from its address.
    if (address.find(':') != colon) return false;
    if (!parse_port(address.substr(colon + 1), port)) return false;
    host = std::string(address.substr(0, colon));
    return true;
}

std::string join_host_port(std::string_view host, uint16_t port) {
    const std::string_view bare = unbracket(host);
    if (bare.find(':') != std::string_view::npos) {
        return "[" + std::string(bare) + "]:" + std::to_string(port);
    }
    return std::string(bare) + ":" + std::to_string(port);
}

int literal_family(std::string_view host) {
    const std::string bare(unbracket(host));
    in_addr v4{};
    if (::inet_pton(AF_INET, bare.c_str(), &v4) == 1) return AF_INET;
    in6_addr v6{};
    if (::inet_pton(AF_INET6, bare.c_str(), &v6) == 1) return AF_INET6;
    return AF_UNSPEC;
}

bool valid_bind(std::string_view bind) {
    return bind.empty() || literal_family(bind) != AF_UNSPEC;
}

int listen_on(std::string_view bind, uint16_t port, int backlog, int extra_type,
              std::string& error, bool* fell_back) {
    if (fell_back != nullptr) *fell_back = false;
    const std::string bare(unbracket(bind));
    int family = bind.empty() ? AF_INET6 : literal_family(bind);
    if (family == AF_UNSPEC) {
        error = "invalid bind address '" + std::string(bind) + "'";
        return -1;
    }

    int fd = ::socket(family, SOCK_STREAM | extra_type, 0);
    if (fd < 0 && bind.empty() && errno == EAFNOSUPPORT) {
        // A kernel built or booted without IPv6: every interface it has is IPv4's.
        OB_LOG_INFO("net", "port %u: no IPv6 in this kernel (%s), listening on IPv4 alone",
                    static_cast<unsigned>(port), std::strerror(errno));
        family = AF_INET;
        if (fell_back != nullptr) *fell_back = true;
        fd = ::socket(family, SOCK_STREAM | extra_type, 0);
    }
    if (fd < 0) {
        error = std::string("socket() failed: ") + std::strerror(errno);
        return -1;
    }
    const int on = 1;
    if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0) {
        error = std::string("setsockopt() failed: ") + std::strerror(errno);
        ::close(fd);
        return -1;
    }

    sockaddr_storage addr{};
    socklen_t addr_len = 0;
    if (family == AF_INET6) {
        // Both families on one socket for no bind; IPv6 alone for an IPv6 literal, `::` included -
        // whatever net.ipv6.bindv6only says, which is 0 on most hosts and would make `--bind ::`
        // take IPv4 as well.
        const int v6only = bind.empty() ? 0 : 1;
        if (::setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &v6only, sizeof(v6only)) < 0) {
            OB_LOG_WARN("net", "port %u: IPV6_V6ONLY could not be set to %d (%s); this listener "
                               "takes what the host's net.ipv6.bindv6only gives it",
                        static_cast<unsigned>(port), v6only, std::strerror(errno));
        }
        auto* a6 = reinterpret_cast<sockaddr_in6*>(&addr);
        a6->sin6_family = AF_INET6;
        a6->sin6_port   = htons(port);
        a6->sin6_addr   = in6addr_any;
        if (!bind.empty()) ::inet_pton(AF_INET6, bare.c_str(), &a6->sin6_addr);
        addr_len = sizeof(sockaddr_in6);
    } else {
        auto* a4 = reinterpret_cast<sockaddr_in*>(&addr);
        a4->sin_family      = AF_INET;
        a4->sin_port        = htons(port);
        a4->sin_addr.s_addr = htonl(INADDR_ANY);
        if (!bind.empty()) ::inet_pton(AF_INET, bare.c_str(), &a4->sin_addr);
        addr_len = sizeof(sockaddr_in);
    }

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), addr_len) < 0) {
        const int bind_errno = errno;
        if (bind.empty() && family == AF_INET6 && bind_errno == EADDRNOTAVAIL) {
            // IPv6 compiled in and switched off (net.ipv6.conf.all.disable_ipv6): the same fallback.
            ::close(fd);
            OB_LOG_INFO("net", "port %u: IPv6 is switched off on this host, listening on IPv4 alone",
                        static_cast<unsigned>(port));
            const int v4 = listen_on("0.0.0.0", port, backlog, extra_type, error, nullptr);
            if (v4 >= 0 && fell_back != nullptr) *fell_back = true;
            return v4;
        }
        error = "bind() failed on port " + std::to_string(port) + ": " + std::strerror(bind_errno);
        ::close(fd);
        return -1;
    }
    if (::listen(fd, backlog) < 0) {
        error = std::string("listen() failed: ") + std::strerror(errno);
        ::close(fd);
        return -1;
    }
    // The address the kernel bound, so a port of 0 is logged as the one it chose.
    sockaddr_storage bound{};
    socklen_t bound_len = sizeof(bound);
    const std::string where = ::getsockname(fd, reinterpret_cast<sockaddr*>(&bound), &bound_len) == 0
                                  ? address_of(bound, bound_len)
                                  : join_host_port(bare, port);
    OB_LOG_INFO("net", "listening on %s, fd=%d%s", where.c_str(), fd,
                bind.empty() && family == AF_INET6 ? " (IPv6 and IPv4)" : "");
    return fd;
}

int dial(std::string_view host, uint16_t port, uint32_t timeout_ms, bool leave_nonblocking,
         std::string& why, const char* component) {
    const std::string bare(unbracket(host));
    if (port == 0) {
        why = "port 0 is not an address to dial";
        return -1;
    }
    addrinfo hints{};
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags    = AI_NUMERICSERV;
    addrinfo* found = nullptr;
    const std::string service = std::to_string(port);
    const int rc = ::getaddrinfo(bare.c_str(), service.c_str(), &hints, &found);
    if (rc != 0 || found == nullptr) {
        why = "the host name does not resolve (" + std::string(::gai_strerror(rc)) + ")";
        OB_LOG_DEBUG("net", "dial %s: %s", join_host_port(bare, port).c_str(), why.c_str());
        return -1;
    }
    int fd = -1;
    std::string last;
    for (const addrinfo* ai = found; ai != nullptr && fd < 0; ai = ai->ai_next) {
        sockaddr_storage tried{};
        std::memcpy(&tried, ai->ai_addr, std::min<size_t>(ai->ai_addrlen, sizeof(tried)));
        fd = dial_one(*ai, timeout_ms, last);
        OB_LOG_DEBUG("net", "dial %s: %s %s", join_host_port(bare, port).c_str(),
                     address_of(tried, static_cast<socklen_t>(ai->ai_addrlen)).c_str(),
                     fd >= 0 ? "connected" : last.c_str());
    }
    ::freeaddrinfo(found);
    if (fd < 0) {
        why = last;
        return -1;
    }
    set_tcp_nodelay(fd, component);
    if (!leave_nonblocking && !set_blocking(fd, true)) {
        why = "the socket could not be put back in blocking mode";
        ::close(fd);
        return -1;
    }
    return fd;
}

std::string address_of(const sockaddr_storage& addr, socklen_t len) {
    char text[INET6_ADDRSTRLEN] = {0};
    if (addr.ss_family == AF_INET && len >= static_cast<socklen_t>(sizeof(sockaddr_in))) {
        const auto* a4 = reinterpret_cast<const sockaddr_in*>(&addr);
        if (::inet_ntop(AF_INET, &a4->sin_addr, text, sizeof(text)) == nullptr) return "unknown";
        return std::string(text) + ":" + std::to_string(ntohs(a4->sin_port));
    }
    if (addr.ss_family == AF_INET6 && len >= static_cast<socklen_t>(sizeof(sockaddr_in6))) {
        const auto* a6 = reinterpret_cast<const sockaddr_in6*>(&addr);
        if (IN6_IS_ADDR_V4MAPPED(&a6->sin6_addr)) {
            in_addr v4{};
            std::memcpy(&v4, a6->sin6_addr.s6_addr + 12, sizeof(v4));
            if (::inet_ntop(AF_INET, &v4, text, sizeof(text)) == nullptr) return "unknown";
            return std::string(text) + ":" + std::to_string(ntohs(a6->sin6_port));
        }
        if (::inet_ntop(AF_INET6, &a6->sin6_addr, text, sizeof(text)) == nullptr) return "unknown";
        return "[" + std::string(text) + "]:" + std::to_string(ntohs(a6->sin6_port));
    }
    return "unknown";
}

std::string peer_address(int fd) {
    sockaddr_storage addr{};
    socklen_t len = sizeof(addr);
    if (::getpeername(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) return "unknown";
    if (addr.ss_family == AF_UNIX) {
        // A Unix socket's address is a path, the same for every connection to it; the process at
        // the other end is what tells one client from another (#52).
        ucred cred{};
        socklen_t cred_len = sizeof(cred);
        if (::getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &cred, &cred_len) != 0) return "unix";
        return "unix pid=" + std::to_string(cred.pid) + " uid=" + std::to_string(cred.uid) +
               " gid=" + std::to_string(cred.gid);
    }
    return address_of(addr, len);
}

bool valid_unix_path(std::string_view path, std::string& why) {
    if (path.empty()) {
        why = "a unix socket's path is empty";
        return false;
    }
    if (path.find('\0') != std::string_view::npos) {
        why = "a unix socket's path holds a NUL byte";
        return false;
    }
    constexpr size_t kMax = sizeof(sockaddr_un{}.sun_path) - 1;
    if (path.size() > kMax) {
        why = "the unix socket path '" + std::string(path) + "' is " + std::to_string(path.size()) +
              " bytes, and a unix socket's path holds at most " + std::to_string(kMax);
        return false;
    }
    return true;
}

int listen_unix(const std::string& path, uint32_t mode, int backlog, int extra_type, std::string& error) {
    std::string why;
    if (!valid_unix_path(path, why)) {
        error = why;
        return -1;
    }
    sockaddr_un addr{};
    const socklen_t addr_len = unix_address(path, addr);

    struct stat st{};
    if (::lstat(path.c_str(), &st) == 0) {
        if (!S_ISSOCK(st.st_mode)) {
            error = "unix socket " + path + ": the path exists and is not a socket";
            return -1;
        }
        // Somebody's socket, or what a process killed while it listened left: a connection says.
        // OB_NO_TCP_NODELAY: an AF_UNIX probe that writes nothing; the option is TCP's.
        const int probe = ::socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (probe < 0) {
            error = "unix socket " + path + ": socket() failed: " + std::strerror(errno);
            return -1;
        }
        const int rc = ::connect(probe, reinterpret_cast<sockaddr*>(&addr), addr_len);
        const int probe_errno = errno;
        ::close(probe);
        if (rc == 0 || probe_errno == EAGAIN) {
            error = "unix socket " + path + ": another process listens on it";
            return -1;
        }
        if (probe_errno != ECONNREFUSED) {
            error = "unix socket " + path + ": " + std::strerror(probe_errno);
            return -1;
        }
        if (::unlink(path.c_str()) != 0) {
            error = "unix socket " + path + ": a stale socket could not be removed: " + std::strerror(errno);
            return -1;
        }
        OB_LOG_INFO("net", "removed a stale unix socket at %s: nothing listened on it", path.c_str());
    } else if (errno != ENOENT) {
        error = "unix socket " + path + ": " + std::strerror(errno);
        return -1;
    }

    const int fd = ::socket(AF_UNIX, SOCK_STREAM | extra_type, 0);
    if (fd < 0) {
        error = std::string("socket() failed: ") + std::strerror(errno);
        return -1;
    }
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), addr_len) < 0) {
        error = "bind() failed on unix socket " + path + ": " + std::strerror(errno);
        ::close(fd);
        return -1;
    }
    // Before listen(): until then a connection is refused, so none meets the umask's mode.
    if (::chmod(path.c_str(), static_cast<mode_t>(mode)) < 0) {
        error = "unix socket " + path + ": chmod() failed: " + std::strerror(errno);
        ::close(fd);
        ::unlink(path.c_str());
        return -1;
    }
    if (::listen(fd, backlog) < 0) {
        error = "listen() failed on unix socket " + path + ": " + std::strerror(errno);
        ::close(fd);
        ::unlink(path.c_str());
        return -1;
    }
    OB_LOG_INFO("net", "listening on unix socket %s (mode %04o), fd=%d", path.c_str(), mode, fd);
    return fd;
}

int dial_unix(const std::string& path, uint32_t timeout_ms, bool leave_nonblocking, std::string& why) {
    if (!valid_unix_path(path, why)) return -1;
    sockaddr_un addr{};
    const socklen_t addr_len = unix_address(path, addr);
    const int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        why = std::string("socket(): ") + std::strerror(errno);
        return -1;
    }
    // A Unix socket connects at once or not at all - except with the server's backlog full, which
    // answers EAGAIN until it has room: tried again until the deadline.
    // OB_NO_TCP_NODELAY: an AF_UNIX stream has no Nagle to switch off; the option is TCP's.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (::connect(fd, reinterpret_cast<sockaddr*>(&addr), addr_len) != 0) {
        if (errno == EINTR) continue;
        if (errno != EAGAIN || std::chrono::steady_clock::now() >= deadline) {
            why = errno == EAGAIN
                      ? "no room in the server's backlog within the connect deadline (" +
                            std::to_string(timeout_ms) + " ms)"
                      : std::string(std::strerror(errno));
            OB_LOG_DEBUG("net", "dial unix %s: %s", path.c_str(), why.c_str());
            ::close(fd);
            return -1;
        }
        ::poll(nullptr, 0, 1);
    }
    if (!leave_nonblocking && !set_blocking(fd, true)) {
        why = "the socket could not be put back in blocking mode";
        ::close(fd);
        return -1;
    }
    OB_LOG_DEBUG("net", "dial unix %s: connected, fd=%d", path.c_str(), fd);
    return fd;
}

}  // namespace ob::net
