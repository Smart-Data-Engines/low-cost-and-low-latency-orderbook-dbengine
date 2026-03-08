#include "orderbook/mmap_store.hpp"

#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <string>

namespace fs = std::filesystem;

// ── Helpers ───────────────────────────────────────────────────────────────────

static fs::path tmp_path(const std::string& name) {
    return fs::temp_directory_path() / ("ob_mmap_test_" + name);
}

struct TempFile {
    fs::path path;
    explicit TempFile(const std::string& name) : path(tmp_path(name)) {}
    ~TempFile() { fs::remove(path); }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

// Write data, close, reopen, verify data persists.
TEST(MmapStore, OpenWriteFlushCloseReopen) {
    TempFile tf("open_write_reopen");

    const char payload[] = "hello orderbook";
    const size_t len     = sizeof(payload); // includes NUL

    {
        ob::MmapStore store;
        store.open(tf.path.string(), 4096);
        // Reserve space, then write to the reserved slot.
        size_t pos = store.size();
        store.advance(len);
        std::memcpy(static_cast<uint8_t*>(const_cast<void*>(store.data())) + pos,
                    payload, len);
        store.flush();
        store.close();
    }

    // Reopen and verify
    {
        ob::MmapStore store;
        store.open(tf.path.string(), 4096);
        EXPECT_EQ(store.size(), len);
        EXPECT_EQ(std::memcmp(store.data(), payload, len), 0);
        store.close();
    }
}

// Open with small initial_size, write more than initial_size bytes, verify remap works.
TEST(MmapStore, RemapWhenCapacityExceeded) {
    TempFile tf("remap");

    // Use a tiny initial size (one page = 4096 bytes).
    // Write 4096 bytes (fills the initial mapping), then write one more byte.
    // The second advance must trigger a remap before incrementing the cursor.
    const size_t initial = 4096;

    ob::MmapStore store;
    store.open(tf.path.string(), initial);

    // Fill the initial mapping: advance first (no remap needed), then write.
    std::vector<uint8_t> first(initial, 0xAA);
    size_t pos0 = store.size();
    store.advance(initial); // cursor goes from 0 → 4096; no remap (0+4096 == mapped_size_)
    std::memcpy(static_cast<uint8_t*>(const_cast<void*>(store.data())) + pos0,
                first.data(), initial);

    // One more byte — advance must remap (4096 + 1 > 4096).
    size_t pos1 = store.size();
    store.advance(1); // triggers remap to 8192
    const uint8_t extra = 0xBB;
    static_cast<uint8_t*>(const_cast<void*>(store.data()))[pos1] = extra;

    EXPECT_EQ(store.size(), initial + 1);
    EXPECT_EQ(std::memcmp(store.data(), first.data(), initial), 0);
    EXPECT_EQ(static_cast<const uint8_t*>(store.data())[initial], extra);
    store.close();
}

// Write data, flush, verify data is visible after reopen without explicit close.
TEST(MmapStore, FlushPersistsData) {
    TempFile tf("flush_persists");

    const uint64_t magic = 0xDEADBEEFCAFEBABEULL;

    {
        ob::MmapStore store;
        store.open(tf.path.string(), 4096);
        size_t pos = store.size();
        store.advance(sizeof(magic));
        std::memcpy(static_cast<uint8_t*>(const_cast<void*>(store.data())) + pos,
                    &magic, sizeof(magic));
        store.flush();
        // Intentionally do NOT call close() — just let the destructor run.
        // The destructor calls msync + munmap + truncate, so data should be on disk.
    }

    {
        ob::MmapStore store;
        store.open(tf.path.string(), 4096);
        EXPECT_EQ(store.size(), sizeof(magic));
        uint64_t read_back = 0;
        std::memcpy(&read_back, store.data(), sizeof(read_back));
        EXPECT_EQ(read_back, magic);
        store.close();
    }
}

// Write known bytes, read them back via data() pointer.
TEST(MmapStore, WriteAndReadBack) {
    TempFile tf("write_read_back");

    ob::MmapStore store;
    store.open(tf.path.string(), 4096);

    const uint8_t bytes[] = {0x01, 0x02, 0x03, 0x04, 0x05};
    size_t pos = store.size();
    store.advance(sizeof(bytes));
    std::memcpy(static_cast<uint8_t*>(const_cast<void*>(store.data())) + pos,
                bytes, sizeof(bytes));

    EXPECT_EQ(store.size(), sizeof(bytes));
    EXPECT_EQ(std::memcmp(store.data(), bytes, sizeof(bytes)), 0);
    store.close();
}

// Multiple advance() calls, verify cursor accumulates correctly.
TEST(MmapStore, MultipleAdvances) {
    TempFile tf("multi_advance");

    ob::MmapStore store;
    store.open(tf.path.string(), 4096);

    EXPECT_EQ(store.size(), 0u);

    const size_t chunk = 64;
    const int    iters = 10;

    for (int i = 0; i < iters; ++i) {
        size_t pos = store.size();
        store.advance(chunk);
        // Write a recognisable byte pattern for each chunk
        uint8_t pattern = static_cast<uint8_t>(i + 1);
        std::memset(static_cast<uint8_t*>(const_cast<void*>(store.data())) + pos,
                    pattern, chunk);
        EXPECT_EQ(store.size(), static_cast<size_t>((i + 1) * chunk));
    }

    // Verify all chunks are readable
    const uint8_t* base = static_cast<const uint8_t*>(store.data());
    for (int i = 0; i < iters; ++i) {
        uint8_t expected = static_cast<uint8_t>(i + 1);
        for (size_t j = 0; j < chunk; ++j) {
            EXPECT_EQ(base[i * chunk + j], expected)
                << "mismatch at chunk " << i << " byte " << j;
        }
    }

    store.close();
}
