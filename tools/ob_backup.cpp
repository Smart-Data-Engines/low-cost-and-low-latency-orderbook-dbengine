// ob_backup — ask a running server for a backup and wait for it (#34).
//
//   ob_backup --host H --port P [--auth-identity I --auth-secret-file F] [--tls [--tls-ca-file F]]
//             [--timeout-s N]
//
// The server writes the backup into its own --backup-dir; nothing here names a path. The secret is
// read from a file in the server's client-secret format (`<identity> <secret>` lines), never from an
// argument, which every user of the machine can read in the process list. Made for cron: one line on
// stdout when the backup is done, one on stderr when it is not, and the exit status says which.
//
// Exit status: 0 done, 1 the backup failed, 2 usage, connection or a refusal to begin, 3 the wait ran
// out (the backup may still be running; BACKUP STATUS says).

#include "orderbook/auth.hpp"
#include "orderbook/client.hpp"
#include "orderbook/logger.hpp"

#include <chrono>
#include <cstdio>
#include <exception>
#include <cstdlib>
#include <map>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

void usage(std::FILE* to) {
    std::fprintf(to,
                 "usage: ob_backup --host <HOST> --port <PORT> [--auth-identity <ID> "
                 "--auth-secret-file <PATH>]\n"
                 "                 [--tls [--tls-ca-file <PATH>]] [--timeout-s <N>]\n");
}

/// `key: value` lines of an `OK` answer.
std::map<std::string, std::string> fields_of(const std::string& answer) {
    std::map<std::string, std::string> out;
    size_t at = answer.find('\n');
    while (at != std::string::npos && at + 1 < answer.size()) {
        const size_t end = answer.find('\n', at + 1);
        const std::string line = answer.substr(at + 1, end == std::string::npos ? std::string::npos
                                                                                : end - at - 1);
        const size_t colon = line.find(": ");
        if (colon != std::string::npos) out[line.substr(0, colon)] = line.substr(colon + 2);
        at = end;
    }
    return out;
}

std::string first_line(const std::string& s) { return s.substr(0, s.find('\n')); }

/// What the command line asked for.
struct Options {
    ob::ClientConfig config;
    std::string      identity;
    std::string      secret_file;
    long             timeout_s = 3600;
};

/// Read the command line into `o`. Returns -1 to go on, or the exit status: 0 for --help, 2 for
/// anything it does not understand. A cursor over the arguments rather than a for loop's counter
/// moved in its body (#36).
int parse_options(int argc, char** argv, Options& o) {
    std::string port, timeout_text;
    const std::vector<std::string> args(argv + 1, argv + argc);
    size_t at = 0;
    while (at < args.size()) {
        const std::string& flag = args[at++];
        if (flag == "--help" || flag == "-h") {
            usage(stdout);
            return 0;
        }
        if (flag == "--tls") {
            o.config.tls = true;
            continue;
        }
        std::string* into = flag == "--host"             ? &o.config.host
                          : flag == "--port"             ? &port
                          : flag == "--auth-identity"    ? &o.identity
                          : flag == "--auth-secret-file" ? &o.secret_file
                          : flag == "--tls-ca-file"      ? &o.config.tls_ca_file
                          : flag == "--timeout-s"        ? &timeout_text
                                                         : nullptr;
        if (!into) {
            std::fprintf(stderr, "ob_backup: unknown argument '%s'\n", flag.c_str());
            usage(stderr);
            return 2;
        }
        if (at >= args.size() || args[at].empty()) {
            std::fprintf(stderr, "ob_backup: %s needs a value\n", flag.c_str());
            usage(stderr);
            return 2;
        }
        *into = args[at++];
    }
    if (!timeout_text.empty()) {
        char* end = nullptr;
        o.timeout_s = std::strtol(timeout_text.c_str(), &end, 10);
        if (end == timeout_text.c_str() || *end != '\0' || o.timeout_s <= 0) {
            std::fprintf(stderr, "ob_backup: --timeout-s '%s' is not a positive number\n",
                         timeout_text.c_str());
            return 2;
        }
    }
    char* end = nullptr;
    const long port_number = port.empty() ? 0 : std::strtol(port.c_str(), &end, 10);
    if (port.empty() || *end != '\0' || port_number <= 0 || port_number > 65535) {
        std::fprintf(stderr, "ob_backup: --port is required, 1 to 65535\n");
        usage(stderr);
        return 2;
    }
    o.config.port = static_cast<uint16_t>(port_number);
    if (o.identity.empty() != o.secret_file.empty()) {
        std::fprintf(stderr, "ob_backup: --auth-identity and --auth-secret-file go together\n");
        return 2;
    }
    return -1;
}

/// The identity's secret, from a file in the server's client-secret format. 0, or 2 with a message:
/// no such identity in it, or a file the loader refuses (its mode, a short secret, a repeat).
int load_credentials(Options& o) {
    if (o.identity.empty()) return 0;
    try {
        const ob::SecretStore store = ob::SecretStore::load_client_file(o.secret_file);
        const ob::Credential* c = store.find(o.identity);
        if (!c) {
            std::fprintf(stderr, "ob_backup: %s has no secret for '%s'\n", o.secret_file.c_str(),
                         o.identity.c_str());
            return 2;
        }
        o.config.auth_identity = c->identity;
        o.config.auth_secret   = c->secret;
        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "ob_backup: %s\n", e.what());
        return 2;
    }
}

/// Ask for a backup on a connected client: the capability first, since a server without it answers
/// BACKUP as an unknown command. Its name, or empty with the exit status in `status`.
std::string begin_backup(ob::OrderbookClient& client, const Options& o, int& status) {
    status = 2;
    const auto caps = client.capabilities();
    if (!caps) {
        std::fprintf(stderr, "ob_backup: STATUS failed: %s\n", caps.error_message().c_str());
        return {};
    }
    if (caps.value().count("backup") == 0) {
        std::fprintf(stderr, "ob_backup: %s:%u does not take backups (no 'backup' in its "
                             "capabilities): a build from before #34\n",
                     o.config.host.c_str(), o.config.port);
        return {};
    }
    const auto begun = client.command("BACKUP");
    if (!begun) {
        std::fprintf(stderr, "ob_backup: BACKUP failed: %s\n", begun.error_message().c_str());
        return {};
    }
    const std::string answer = begun.value();
    const std::string_view prefix = "OK BACKUP ";
    if (answer.rfind(prefix, 0) != 0) {
        std::fprintf(stderr, "ob_backup: %s\n", first_line(answer).c_str());
        return {};
    }
    status = 0;
    return first_line(answer).substr(prefix.size());
}

/// Poll `BACKUP STATUS` every half second until backup `name` is done or failed, or the wait runs
/// out: 0 done (one line on stdout), 1 failed, 2 a connection that broke, 3 the time up.
int wait_for_backup(ob::OrderbookClient& client, const std::string& name, long timeout_s) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
    for (;;) {
        const auto status = client.command("BACKUP STATUS");
        if (!status) {
            std::fprintf(stderr, "ob_backup: BACKUP STATUS failed while backup %s ran: %s\n",
                         name.c_str(), status.error_message().c_str());
            return 2;
        }
        auto f = fields_of(status.value());
        if (f["name"] != name) {
            std::fprintf(stderr, "ob_backup: the server reports backup %s, not %s\n",
                         f["name"].c_str(), name.c_str());
            return 1;
        }
        if (f["state"] == "done") {
            std::printf("backup %s done: %s, %s file(s), %s byte(s), cut %s ms, pinned %s ms, %s ms\n",
                        name.c_str(), f["method"].c_str(), f["files"].c_str(), f["bytes"].c_str(),
                        f["cut_ms"].c_str(), f["pinned_ms"].c_str(), f["elapsed_ms"].c_str());
            return 0;
        }
        if (f["state"] == "failed") {
            std::fprintf(stderr, "ob_backup: backup %s failed: %s\n", name.c_str(),
                         f["error"].c_str());
            return 1;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            std::fprintf(stderr, "ob_backup: backup %s still %s after %ld s (phase %s, files %s)\n",
                         name.c_str(), f["state"].c_str(), timeout_s, f["phase"].c_str(),
                         f["files"].c_str());
            return 3;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
}

}  // namespace

static int run(int argc, char** argv) {
    Options o;
    if (const int status = parse_options(argc, argv, o); status >= 0) return status;
    if (const int status = load_credentials(o); status != 0) return status;
    ob::StructuredLogger::instance().set_level(ob::LogLevel::WARN);

    ob::OrderbookClient client(o.config);
    if (auto c = client.connect(); !c) {
        std::fprintf(stderr, "ob_backup: cannot connect to %s:%u: %s\n", o.config.host.c_str(),
                     o.config.port, c.error_message().c_str());
        return 2;
    }
    int status = 0;
    const std::string name = begin_backup(client, o, status);
    if (name.empty()) return status;
    return wait_for_backup(client, name, o.timeout_s);
}

// Nothing leaves by the terminate handler (#102): an exception from the filesystem or an allocation
// is a message and an exit status, as a refusal is.
int main(int argc, char** argv) {
    try {
        return run(argc, argv);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "ob_backup: %s\n", e.what());
        return 2;
    }
}
