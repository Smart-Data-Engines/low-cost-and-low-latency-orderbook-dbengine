#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>

#include <unistd.h>

/// A test's temporary directories and files are its process's own (#285).
///
/// **Why.** A test that names its data directory the same way in every run - by a counter that
/// starts at 0 in every process, by `std::rand()`, which without a seed gives every process the
/// same numbers, or by a fixed name - is given the directory a crashed run of the same binary left
/// behind, and opens what that run wrote as its own. On 19 November 2026 a mutant of #284 crashed
/// `test_pending_row_names`, whose directory was `/tmp/pending_row_names_0` in every run; the next
/// run, the table's next mutant, stored 3860 rows of a pair where it had written 1930 - the crashed
/// run's WAL replayed beside its own - and reported a control killed. The run after that passed.
///
/// `test_temp_dir_discipline` holds every path a test builds under the temporary directory to this
/// file, or to a name that carries the process's id.
namespace ob::test {

/// The number the next name takes in this process.
inline std::atomic<uint64_t> g_temp_name_counter{0};

/// `<temporary directory>/<prefix><process id>_<number>`, the number this process's next.
inline std::filesystem::path own_temp_name(std::string_view prefix, std::string_view suffix = {}) {
    std::string name(prefix);
    name += std::to_string(::getpid());
    name += '_';
    name += std::to_string(g_temp_name_counter.fetch_add(1, std::memory_order_relaxed));
    name += suffix;
    return std::filesystem::temp_directory_path() / name;
}

/// A new, empty directory under the temporary directory (`TMPDIR`, else `/tmp`), created by this
/// call and named by `prefix`, this process's id and a number this process counts. A name that is
/// taken already - by an earlier process that had the same id - is passed over, not reused.
inline std::filesystem::path own_temp_dir(std::string_view prefix) {
    for (;;) {
        std::filesystem::path dir = own_temp_name(prefix);
        if (std::filesystem::create_directory(dir)) return dir;
    }
}

/// A path for a file under the temporary directory, named as own_temp_dir() names a directory,
/// with `suffix` after the number. Nothing is created; a file of that name - an earlier process's
/// with the same id - is removed, so that what the caller writes there is the file.
inline std::filesystem::path own_temp_file(std::string_view prefix, std::string_view suffix = {}) {
    std::filesystem::path file = own_temp_name(prefix, suffix);
    std::error_code ec;
    std::filesystem::remove(file, ec);
    return file;
}

}  // namespace ob::test
