// The WAL archive (#34, point-in-time recovery; spec `point-in-time-recovery`): every closed WAL
// file copied byte for byte into `<archive>/<identity>/`, a file already there compared rather than
// trusted, one with other contents refused and never overwritten, and the retention floor -
// `archived_before()` - never above a file the archive does not hold.
//
// The files here are bytes under WAL names rather than a log a writer made: the archive reads no
// record, and what it promises is about the bytes.

#include <gtest/gtest.h>

#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <thread>

#include "orderbook/metrics.hpp"
#include "orderbook/wal.hpp"
#include "orderbook/wal_archive.hpp"

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kLineage = 0x1234abcdULL;
constexpr uint64_t kNextLineage = 0x5678ef01ULL;

class WalArchiveTest : public ::testing::Test {
protected:
    void SetUp() override {
        base_ = fs::temp_directory_path() /
                ("ob_wal_archive_" + std::to_string(::getpid()) + "_" +
                 ::testing::UnitTest::GetInstance()->current_test_info()->name());
        fs::remove_all(base_);
        wal_ = base_ / "wal";
        archive_ = base_ / "archive";
        fs::create_directories(wal_);
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(base_, ec);
    }

    std::unique_ptr<ob::WalArchiver> archiver() {
        return std::make_unique<ob::WalArchiver>(
            wal_.string(), archive_.string(), kLineage, [this] { return bound_.load(); }, &registry_);
    }

    /// Bytes no other index has. 3001 by default, so a file is not a whole number of anything.
    static std::string bytes_of(uint32_t index, size_t size = 3001) {
        std::string out(size, '\0');
        uint32_t x = 2'654'435'761u * (index + 1);
        for (char& c : out) {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            c = static_cast<char>(x & 0xFF);
        }
        return out;
    }

    static void write(const fs::path& path, const std::string& bytes) {
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        out << bytes;
    }

    static std::string read(const fs::path& path) {
        std::ifstream in(path, std::ios::binary);
        return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    }

    static ino_t inode(const fs::path& path) {
        struct stat st{};
        return ::stat(path.c_str(), &st) == 0 ? st.st_ino : 0;
    }

    fs::path in_wal(uint32_t index) const { return wal_ / ob::wal_file_basename(index); }
    fs::path in_archive(uint32_t index, uint64_t lineage = kLineage) const {
        return archive_ / std::to_string(lineage) / ob::wal_file_basename(index);
    }

    /// Closed files `first`..`last`, each with its own bytes.
    void close_files(uint32_t first, uint32_t last) {
        for (uint32_t i = first; i <= last; ++i) write(in_wal(i), bytes_of(i));
    }

    fs::path base_, wal_, archive_;
    ob::MetricsRegistry registry_;
    std::atomic<uint32_t> bound_{0};
};

TEST_F(WalArchiveTest, CopiesEveryClosedFileByteForByteAndNoOther) {
    close_files(1, 4);
    // Larger than the archive's read block, so a copy that read one block would be short.
    write(in_wal(2), bytes_of(2, (2u << 20) + (1u << 19) + 7));
    auto a = archiver();
    EXPECT_EQ(a->archived_before(), 0u) << "before a pass, retention may remove nothing";

    const auto pass = a->archive_closed(kLineage, 4);
    EXPECT_EQ(pass.archived, 3u);
    EXPECT_EQ(pass.present, 0u);
    EXPECT_EQ(pass.pending, 0u);
    EXPECT_TRUE(pass.failure.empty()) << pass.failure;
    EXPECT_FALSE(pass.refused);
    uint64_t total = 0;
    for (uint32_t i = 1; i <= 3; ++i) {
        ASSERT_TRUE(fs::exists(in_archive(i))) << i;
        EXPECT_EQ(read(in_archive(i)), read(in_wal(i))) << i;
        total += fs::file_size(in_wal(i));
    }
    EXPECT_EQ(pass.bytes, total);
    EXPECT_FALSE(fs::exists(in_archive(4))) << "file 4 is the bound: not closed";
    EXPECT_EQ(a->archived_before(), 4u);
    for (const auto& entry : fs::directory_iterator(archive_ / std::to_string(kLineage))) {
        EXPECT_EQ(entry.path().extension(), ".bin") << entry.path() << " left behind";
    }

    EXPECT_EQ(registry_.counter_value("ob_wal_archived_files_total"), 3u);
    EXPECT_EQ(registry_.counter_value("ob_wal_archived_bytes_total"), total);
    EXPECT_EQ(registry_.counter_value("ob_wal_archive_failures_total"), 0u);
    EXPECT_EQ(registry_.gauge_value("ob_wal_archive_pending_files"), 0);
    EXPECT_EQ(registry_.unknown_metric_writes(), 0u);
}

TEST_F(WalArchiveTest, AFileThereUnchangedIsLeftAloneAcrossPassesAndRestarts) {
    close_files(1, 2);
    {
        auto a = archiver();
        ASSERT_EQ(a->archive_closed(kLineage, 3).archived, 2u);
        const ino_t first = inode(in_archive(1));

        const auto again = a->archive_closed(kLineage, 3);
        EXPECT_EQ(again.archived, 0u);
        EXPECT_EQ(again.present, 2u);
        EXPECT_EQ(inode(in_archive(1)), first);
    }
    const ino_t before_restart = inode(in_archive(1));
    auto restarted = archiver();
    const auto pass = restarted->archive_closed(kLineage, 3);
    EXPECT_EQ(pass.archived, 0u);
    EXPECT_EQ(pass.present, 2u);
    EXPECT_FALSE(pass.refused);
    EXPECT_EQ(restarted->archived_before(), 3u);
    EXPECT_EQ(inode(in_archive(1)), before_restart) << "a matching file is never copied again";
    EXPECT_EQ(registry_.counter_value("ob_wal_archived_files_total"), 2u);
}

TEST_F(WalArchiveTest, AFileThereWithOtherContentsIsRefusedAndNeverOverwritten) {
    close_files(1, 3);
    fs::create_directories(in_archive(2).parent_path());
    const std::string theirs = bytes_of(77, 512);
    write(in_archive(2), theirs);
    auto a = archiver();

    const auto pass = a->archive_closed(kLineage, 4);
    EXPECT_EQ(pass.archived, 1u);
    EXPECT_TRUE(pass.refused);
    EXPECT_NE(pass.failure.find(in_archive(2).string()), std::string::npos) << pass.failure;
    EXPECT_TRUE(a->refused());
    EXPECT_EQ(read(in_archive(2)), theirs);
    EXPECT_FALSE(fs::exists(in_archive(3))) << "nothing after the refused file";
    EXPECT_EQ(a->archived_before(), 2u) << "retention keeps the refused file and every one after it";
    EXPECT_EQ(registry_.counter_value("ob_wal_archive_failures_total"), 1u);

    // Stopped: a later pass copies nothing, and the floor stays.
    const auto later = a->archive_closed(kLineage, 4);
    EXPECT_TRUE(later.refused);
    EXPECT_EQ(later.archived, 0u);
    EXPECT_FALSE(fs::exists(in_archive(3)));
    EXPECT_EQ(read(in_archive(2)), theirs);
    EXPECT_EQ(a->archived_before(), 2u);
}

TEST_F(WalArchiveTest, OtherBytesOfTheSameSizeAreRefusedToo) {
    close_files(1, 2);
    std::string changed = bytes_of(1);
    changed[1500] = static_cast<char>(changed[1500] ^ 0x01);
    fs::create_directories(in_archive(1).parent_path());
    write(in_archive(1), changed);
    auto a = archiver();

    const auto pass = a->archive_closed(kLineage, 3);
    EXPECT_TRUE(pass.refused);
    EXPECT_EQ(pass.archived, 0u);
    EXPECT_EQ(read(in_archive(1)), changed);
    EXPECT_EQ(a->archived_before(), 1u);
}

TEST_F(WalArchiveTest, AnArchiveThatCannotBeMadeHoldsRetentionAtTheFirstClosedFile) {
    close_files(1, 3);
    write(archive_, "a file where the archive's directory should be");
    auto a = archiver();

    const auto pass = a->archive_closed(kLineage, 4);
    EXPECT_FALSE(pass.failure.empty());
    EXPECT_FALSE(pass.refused) << "a failure is retried, a refusal is not";
    EXPECT_EQ(pass.archived, 0u);
    EXPECT_EQ(pass.pending, 3u);
    EXPECT_EQ(a->archived_before(), 1u);
    EXPECT_EQ(registry_.counter_value("ob_wal_archive_failures_total"), 1u);
    EXPECT_EQ(registry_.gauge_value("ob_wal_archive_pending_files"), 3);

    fs::remove(archive_);
    const auto retried = a->archive_closed(kLineage, 4);
    EXPECT_TRUE(retried.failure.empty()) << retried.failure;
    EXPECT_EQ(retried.archived, 3u);
    EXPECT_EQ(a->archived_before(), 4u);
    EXPECT_EQ(registry_.gauge_value("ob_wal_archive_pending_files"), 0);
}

TEST_F(WalArchiveTest, ACopyThatFailsStopsThePassAtItsFile) {
    close_files(1, 3);
    // The copy of file 2 goes through `<name>.tmp`, and a directory of that name cannot be opened
    // for writing.
    fs::create_directories(in_archive(2).string() + ".tmp");
    auto a = archiver();

    const auto pass = a->archive_closed(kLineage, 4);
    EXPECT_EQ(pass.archived, 1u);
    EXPECT_NE(pass.failure.find(".tmp"), std::string::npos) << pass.failure;
    EXPECT_FALSE(pass.refused);
    EXPECT_FALSE(fs::exists(in_archive(2)));
    EXPECT_FALSE(fs::exists(in_archive(3))) << "oldest first: no file after a hole";
    EXPECT_EQ(a->archived_before(), 2u);
    EXPECT_EQ(pass.pending, 2u);

    fs::remove(in_archive(2).string() + ".tmp");
    const auto retried = a->archive_closed(kLineage, 4);
    EXPECT_EQ(retried.archived, 2u);
    EXPECT_EQ(read(in_archive(3)), read(in_wal(3)));
    EXPECT_EQ(a->archived_before(), 4u);
}

TEST_F(WalArchiveTest, EachLineageGoesToItsOwnDirectory) {
    close_files(1, 2);
    auto a = archiver();
    ASSERT_EQ(a->archive_closed(kLineage, 3).archived, 2u);

    // A new lineage: the old one's files are removed, and the new one's continue the numbers.
    fs::remove(in_wal(1));
    fs::remove(in_wal(2));
    a->set_lineage(kNextLineage);
    close_files(3, 4);
    const auto pass = a->archive_closed(a->lineage(), 5);
    EXPECT_EQ(pass.archived, 2u);
    EXPECT_EQ(read(in_archive(3, kNextLineage)), read(in_wal(3)));
    EXPECT_EQ(read(in_archive(4, kNextLineage)), read(in_wal(4)));
    EXPECT_FALSE(fs::exists(in_archive(3)));
    EXPECT_TRUE(fs::exists(in_archive(1))) << "the old lineage's archive is left as it was";
    EXPECT_EQ(a->lineage_dir(kNextLineage), (archive_ / std::to_string(kNextLineage)).string());
}

TEST_F(WalArchiveTest, TheThreadArchivesAClosedFileWhenWoken) {
    auto a = archiver();
    a->start();
    close_files(1, 1);
    bound_ = 2;
    a->wake();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (a->archived_before() != 2u && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    a->stop();
    EXPECT_EQ(a->archived_before(), 2u);
    EXPECT_EQ(read(in_archive(1)), read(in_wal(1)));
}

TEST_F(WalArchiveTest, PendingFilesAndTheAgeOfTheOldestAreReported) {
    close_files(1, 2);
    const auto now = fs::file_time_type::clock::now();
    fs::last_write_time(in_wal(1), now - std::chrono::seconds(300));
    fs::last_write_time(in_wal(2), now - std::chrono::seconds(100));
    write(archive_, "a file where the archive's directory should be");
    auto a = archiver();

    (void)a->archive_closed(kLineage, 3);
    EXPECT_EQ(registry_.gauge_value("ob_wal_archive_pending_files"), 2);
    const int64_t oldest = registry_.gauge_value("ob_wal_archive_oldest_pending_seconds");
    EXPECT_GE(oldest, 299);
    EXPECT_LT(oldest, 400);
}

TEST(WalFileNames, TheNameRoundTripsAndNearMissesAreNotWalFiles) {
    EXPECT_EQ(ob::wal_file_basename(0), "wal_000000.bin");
    EXPECT_EQ(ob::wal_file_basename(42), "wal_000042.bin");
    for (const uint32_t i : {0u, 1u, 42u, 999'999u}) {
        uint32_t back = 7;
        ASSERT_TRUE(ob::parse_wal_file_basename(ob::wal_file_basename(i), back)) << i;
        EXPECT_EQ(back, i);
    }
    for (const char* name : {"wal_000042.bin.tmp", "wal_00042.bin", "wal_0000042.bin", "wal_00004x.bin",
                             "wak_000042.bin", "wal_000042.bim", "", "wal_identity", "wal_-00042.bin"}) {
        uint32_t back = 7;
        EXPECT_FALSE(ob::parse_wal_file_basename(name, back)) << name;
        EXPECT_EQ(back, 7u) << name;
    }
}

}  // namespace
