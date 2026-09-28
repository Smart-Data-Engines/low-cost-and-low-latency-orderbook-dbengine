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
#include <cstdlib>
#include <map>
#include <string>
#include <string_view>
#include <thread>

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

}  // namespace

int main(int argc, char** argv) {
    ob::ClientConfig config;
    std::string identity, secret_file, port;
    long timeout_s = 3600;
    for (int i = 1; i < argc; ++i) {
        const std::string_view flag = argv[i];
        if (flag == "--help" || flag == "-h") {
            usage(stdout);
            return 0;
        }
        if (flag == "--tls") {
            config.tls = true;
            continue;
        }
        if (i + 1 >= argc || argv[i + 1][0] == '\0') {
            std::fprintf(stderr, "ob_backup: %s needs a value\n", argv[i]);
            usage(stderr);
            return 2;
        }
        const std::string value = argv[++i];
        if (flag == "--host") {
            config.host = value;
        } else if (flag == "--port") {
            port = value;
        } else if (flag == "--auth-identity") {
            identity = value;
        } else if (flag == "--auth-secret-file") {
            secret_file = value;
        } else if (flag == "--tls-ca-file") {
            config.tls_ca_file = value;
        } else if (flag == "--timeout-s") {
            char* end = nullptr;
            timeout_s = std::strtol(value.c_str(), &end, 10);
            if (end == value.c_str() || *end != '\0' || timeout_s <= 0) {
                std::fprintf(stderr, "ob_backup: --timeout-s '%s' is not a positive number\n",
                             value.c_str());
                return 2;
            }
        } else {
            std::fprintf(stderr, "ob_backup: unknown argument '%s'\n", argv[i - 1]);
            usage(stderr);
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
    config.port = static_cast<uint16_t>(port_number);
    if (identity.empty() != secret_file.empty()) {
        std::fprintf(stderr, "ob_backup: --auth-identity and --auth-secret-file go together\n");
        return 2;
    }
    if (!identity.empty()) {
        try {
            const ob::SecretStore store = ob::SecretStore::load_client_file(secret_file);
            const ob::Credential* c = store.find(identity);
            if (!c) {
                std::fprintf(stderr, "ob_backup: %s has no secret for '%s'\n", secret_file.c_str(),
                             identity.c_str());
                return 2;
            }
            config.auth_identity = c->identity;
            config.auth_secret   = c->secret;
        } catch (const std::exception& e) {
            std::fprintf(stderr, "ob_backup: %s\n", e.what());
            return 2;
        }
    }
    ob::StructuredLogger::instance().set_level(ob::LogLevel::WARN);

    ob::OrderbookClient client(config);
    if (auto c = client.connect(); !c) {
        std::fprintf(stderr, "ob_backup: cannot connect to %s:%u: %s\n", config.host.c_str(),
                     config.port, c.error_message().c_str());
        return 2;
    }
    const auto caps = client.capabilities();
    if (!caps) {
        std::fprintf(stderr, "ob_backup: STATUS failed: %s\n", caps.error_message().c_str());
        return 2;
    }
    if (caps.value().count("backup") == 0) {
        std::fprintf(stderr, "ob_backup: %s:%u does not take backups (no 'backup' in its "
                             "capabilities): a build from before #34\n",
                     config.host.c_str(), config.port);
        return 2;
    }

    const auto begun = client.command("BACKUP");
    if (!begun) {
        std::fprintf(stderr, "ob_backup: BACKUP failed: %s\n", begun.error_message().c_str());
        return 2;
    }
    const std::string answer = begun.value();
    const std::string_view prefix = "OK BACKUP ";
    if (answer.rfind(prefix, 0) != 0) {
        std::fprintf(stderr, "ob_backup: %s\n", first_line(answer).c_str());
        return 2;
    }
    const std::string name = first_line(answer).substr(prefix.size());

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
            std::printf("backup %s done: %s, %s file(s), %s byte(s), pinned %s ms, %s ms\n",
                        name.c_str(), f["method"].c_str(), f["files"].c_str(), f["bytes"].c_str(),
                        f["pinned_ms"].c_str(), f["elapsed_ms"].c_str());
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
