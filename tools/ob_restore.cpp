// ob_restore — check a backup, and restore it into an empty data directory (#34).
//
//   ob_restore --verify <backup>
//   ob_restore --backup <backup> --data-dir <DIR> [--wal-dir <DIR>] [--log-level <LEVEL>]
//
// A backup is a directory a server with --backup-dir wrote on BACKUP: <backup-dir>/<name>. The whole
// backup is verified before anything is created in the target, and the target must be empty. What
// the restore leaves is a data directory a server starts on as on any other: the backup's rows, a new
// WAL, and the sequence state the backup carried.
//
// Exit status: 0 done, 1 the backup does not verify or the restore failed, 2 usage.

#include "orderbook/backup.hpp"
#include "orderbook/logger.hpp"

#include <cstdio>
#include <exception>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace {

void usage(std::FILE* to) {
    std::fprintf(to,
                 "usage: ob_restore --verify <backup>\n"
                 "       ob_restore --backup <backup> --data-dir <DIR> [--wal-dir <DIR>]\n"
                 "\n"
                 "  --verify <backup>    check every file of a backup against its description\n"
                 "  --backup <backup>    the backup to restore: <backup-dir>/<name>\n"
                 "  --data-dir <DIR>     an empty data directory to restore into\n"
                 "  --wal-dir <DIR>      an empty WAL directory, for a server started with --wal-dir\n"
                 "  --log-level <LEVEL>  the engine's log on stderr: error (default), warn, info, debug\n");
}

}  // namespace

static int run(int argc, char** argv) {
    std::string verify, backup, data_dir, wal_dir, level = "error";
    // A cursor over the arguments rather than a for loop's counter moved in its body (#36).
    const std::vector<std::string> args(argv + 1, argv + argc);
    size_t at = 0;
    while (at < args.size()) {
        const std::string& flag = args[at++];
        if (flag == "--help" || flag == "-h") {
            usage(stdout);
            return 0;
        }
        std::string* into = flag == "--verify"    ? &verify
                          : flag == "--backup"    ? &backup
                          : flag == "--data-dir"  ? &data_dir
                          : flag == "--wal-dir"   ? &wal_dir
                          : flag == "--log-level" ? &level
                                                  : nullptr;
        if (!into) {
            std::fprintf(stderr, "ob_restore: unknown argument '%s'\n", flag.c_str());
            usage(stderr);
            return 2;
        }
        if (at >= args.size() || args[at].empty()) {
            std::fprintf(stderr, "ob_restore: %s needs a value\n", flag.c_str());
            return 2;
        }
        *into = args[at++];
    }
    const auto parsed = ob::StructuredLogger::parse_level(level);
    if (!parsed) {
        std::fprintf(stderr, "ob_restore: --log-level '%s' is not error, warn, info or debug\n",
                     level.c_str());
        return 2;
    }
    ob::StructuredLogger::instance().set_level(*parsed);

    if (!verify.empty()) {
        if (!backup.empty() || !data_dir.empty() || !wal_dir.empty()) {
            std::fprintf(stderr, "ob_restore: --verify takes no other directory\n");
            return 2;
        }
        ob::BackupDescription d;
        std::string error;
        if (!ob::read_backup_description(verify, d, error)) {
            std::fprintf(stderr, "ob_restore: %s\n", error.c_str());
            return 1;
        }
        const auto problems = ob::verify_backup(verify, d);
        for (const auto& p : problems) std::fprintf(stderr, "ob_restore: %s\n", p.c_str());
        if (!problems.empty()) {
            std::fprintf(stderr, "ob_restore: %s does not verify: %zu of %zu file(s)\n",
                         verify.c_str(), problems.size(), d.files.size());
            return 1;
        }
        std::printf("%s verifies: %zu file(s), %llu byte(s), %llu row(s), %s, cut at WAL %u:%llu of a "
                    "%s node, engine %s\n",
                    verify.c_str(), d.files.size(), static_cast<unsigned long long>(d.total_bytes),
                    static_cast<unsigned long long>(d.total_rows), d.method.c_str(), d.wal_file_index,
                    static_cast<unsigned long long>(d.wal_byte_offset), d.role.c_str(),
                    d.engine_version.c_str());
        return 0;
    }

    if (backup.empty() || data_dir.empty()) {
        std::fprintf(stderr, "ob_restore: give --verify <backup>, or --backup and --data-dir\n");
        usage(stderr);
        return 2;
    }
    ob::RestoreReport report;
    std::string error;
    if (!ob::restore_backup(backup, data_dir, wal_dir, report, error)) {
        std::fprintf(stderr, "ob_restore: %s\n", error.c_str());
        return 1;
    }
    std::printf("restored %s into %s: %zu file(s), %llu byte(s), %llu row(s); new WAL identity %016llx; "
                "sequence state %s; verify %.0f ms, copy %.0f ms, open %.0f ms\n",
                backup.c_str(), data_dir.c_str(), report.files,
                static_cast<unsigned long long>(report.bytes),
                static_cast<unsigned long long>(report.rows),
                static_cast<unsigned long long>(report.wal_identity),
                report.sequence_adopted ? "restored" : "from the segments (the backup's was truncated)",
                report.verify_ms, report.copy_ms, report.open_ms);
    return 0;
}

// Nothing leaves by the terminate handler (#102): an exception from the filesystem or an allocation
// is a message and an exit status, as a refusal is.
int main(int argc, char** argv) {
    try {
        return run(argc, argv);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "ob_restore: %s\n", e.what());
        return 1;
    }
}
