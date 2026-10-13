#pragma once

// Sockets and addresses for every listener and every outgoing connection the engine opens: the
// client port, replication, the mesh, the metrics endpoint, the C++ client.
//
// One module, because the engine had a copy of this code in each of those places and every copy was
// IPv4 alone - `socket(AF_INET)`, `INADDR_ANY`, `inet_pton(AF_INET)` - and a node on a host with only
// IPv6 could neither listen nor dial. Adding IPv6 to six copies would have been six chances to get
// one wrong; this is one.
//
// Addresses as text are "host:port", an IPv6 literal in brackets: "[::1]:9090". The host comes back
// from split_host_port() without them, because that is what getaddrinfo(), inet_pton() and a TLS
// name check take.

#include <cstdint>
#include <string>
#include <string_view>
#include <sys/socket.h>

namespace ob::net {

/// "host:port" into its two halves. An IPv6 host must be in brackets ("[::1]:9090"), and comes back
/// without them; a host with more than one colon and no brackets is refused, since its last colon
/// cannot be told from a part of the address. The port is 0 - 65535; callers that dial refuse 0.
bool split_host_port(std::string_view address, std::string& host, uint16_t& port);

/// The other direction: an IPv6 literal in brackets, a name or an IPv4 literal as it is.
std::string join_host_port(std::string_view host, uint16_t port);

/// AF_INET for an IPv4 literal, AF_INET6 for an IPv6 one (brackets allowed), AF_UNSPEC otherwise -
/// a name, or nothing that parses.
int literal_family(std::string_view host);

/// What a --bind flag may say: empty (every interface, both families), or an IPv4 or IPv6 literal.
bool valid_bind(std::string_view bind);

/// A socket listening on `bind`:`port`, or -1 with `error` naming the step that failed, in the words
/// the listeners used before this module ("bind() failed on port N: ...").
///
/// An empty `bind` is every interface of both families: `::` with IPV6_V6ONLY off, which takes IPv4
/// connections as mapped addresses. A kernel without IPv6 refuses that socket, and then the listener
/// is `0.0.0.0` and `*fell_back` is set - every interface it has. A literal binds its own family
/// alone: `0.0.0.0` is every IPv4 interface and `::` every IPv6 one, with IPV6_V6ONLY on.
/// `extra_type` is or-ed into the socket type (SOCK_NONBLOCK, SOCK_CLOEXEC).
int listen_on(std::string_view bind, uint16_t port, int backlog, int extra_type,
              std::string& error, bool* fell_back = nullptr);

/// A connection to `host`:`port`: every address getaddrinfo() gives the host, in its order, each
/// tried non-blocking for at most `timeout_ms` - so a name that resolves to an IPv6 address the
/// server does not listen on goes on to the IPv4 one. The socket comes back non-blocking when
/// `leave_nonblocking` is set and blocking otherwise; -1 with `why` when no address answered.
///
/// It comes back with Nagle off (ob::set_tcp_nodelay(), #140), logged under `component`: every
/// connection the engine dials - to a primary, to a mesh peer, the C++ client's to a server -
/// carries small messages, and this is the one place any of them is dialled.
int dial(std::string_view host, uint16_t port, uint32_t timeout_ms, bool leave_nonblocking,
         std::string& why, const char* component);

/// "1.2.3.4:5" or "[::1]:5" for an address; an IPv4 address mapped into IPv6 (what a dual-stack
/// listener hands over for an IPv4 client) as the IPv4 one; "unknown" for anything else.
std::string address_of(const sockaddr_storage& addr, socklen_t len);

/// The address of a connected socket's other end, as address_of() writes it; "unknown" when the
/// socket will not say, or is not an IP socket (the unit tests use AF_UNIX pairs).
std::string peer_address(int fd);

}  // namespace ob::net
