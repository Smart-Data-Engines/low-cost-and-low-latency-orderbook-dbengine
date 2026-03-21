#include <gtest/gtest.h>
#include <rapidcheck.h>

#include "orderbook/epoch.hpp"
#include "orderbook/wal.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>

// ── Property 1: Epoch serialization round-trip ────────────────────────────────
// Feature: ha-automatic-failover, Property 1: Epoch serialization round-trip
// Validates: Requirements 1.3, 1.4, 11.1, 11.2, 11.4

TEST(EpochProperty, SerializationRoundTrip) {
    rc::check("epoch_from_payload(epoch_to_payload(e)) == e for all valid epochs",
              []() {
                  const auto term = *rc::gen::arbitrary<uint64_t>();
                  const ob::EpochValue original{term};

                  uint8_t buf[8]{};
                  ob::epoch_to_payload(original, buf);
                  const ob::EpochValue restored = ob::epoch_from_payload(buf);

                  RC_ASSERT(restored == original);
                  RC_ASSERT(restored.term == term);
              });
}

// ── Deterministic unit tests for boundary values ──────────────────────────────

TEST(EpochBoundary, Zero) {
    const ob::EpochValue e{0};
    uint8_t buf[8]{};
    ob::epoch_to_payload(e, buf);
    const ob::EpochValue r = ob::epoch_from_payload(buf);
    EXPECT_EQ(r.term, 0u);
    EXPECT_EQ(r, e);
}

TEST(EpochBoundary, One) {
    const ob::EpochValue e{1};
    uint8_t buf[8]{};
    ob::epoch_to_payload(e, buf);
    const ob::EpochValue r = ob::epoch_from_payload(buf);
    EXPECT_EQ(r.term, 1u);
    EXPECT_EQ(r, e);
}

TEST(EpochBoundary, MaxMinusOne) {
    const ob::EpochValue e{std::numeric_limits<uint64_t>::max() - 1};
    uint8_t buf[8]{};
    ob::epoch_to_payload(e, buf);
    const ob::EpochValue r = ob::epoch_from_payload(buf);
    EXPECT_EQ(r.term, std::numeric_limits<uint64_t>::max() - 1);
    EXPECT_EQ(r, e);
}

TEST(EpochBoundary, Max) {
    const ob::EpochValue e{std::numeric_limits<uint64_t>::max()};
    uint8_t buf[8]{};
    ob::epoch_to_payload(e, buf);
    const ob::EpochValue r = ob::epoch_from_payload(buf);
    EXPECT_EQ(r.term, std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(r, e);
}

// ── Property 3: Epoch monotonicity in WAL replay ─────────────────────────────
// Feature: ha-automatic-failover, Property 3: Epoch monotonicity in WAL replay
// Validates: Requirements 11.3

TEST(EpochProperty, MonotonicityInWALReplay) {
    rc::check("WALReplayer.last_epoch() returns the maximum epoch from replayed Epoch_Records",
              []() {
                  // Generate a non-empty vector of epoch values.
                  const auto epochs = *rc::gen::nonEmpty(
                      rc::gen::container<std::vector<uint64_t>>(
                          rc::gen::arbitrary<uint64_t>()));

                  // Create a unique temporary directory for WAL files.
                  const auto suffix = *rc::gen::arbitrary<uint64_t>();
                  const auto tmp = std::filesystem::temp_directory_path() /
                                   ("test_epoch_mono_" + std::to_string(suffix));
                  std::filesystem::create_directories(tmp);

                  // Write each epoch value as an Epoch_Record.
                  {
                      ob::WALWriter writer(tmp.string(), 512ULL << 20,
                                           ob::FsyncPolicy::NONE);
                      for (const auto& e : epochs) {
                          writer.append_epoch(ob::EpochValue{e});
                      }
                  }

                  // Replay and check that last_epoch() == max of the sequence.
                  ob::WALReplayer replayer(tmp.string());
                  replayer.replay([](const ob::WALRecord&, const uint8_t*) {});

                  const uint64_t expected_max =
                      *std::max_element(epochs.begin(), epochs.end());
                  RC_ASSERT(replayer.last_epoch() == expected_max);

                  // Clean up.
                  std::filesystem::remove_all(tmp);
              });
}

// ── Property 4: Epoch increment on promotion ─────────────────────────────────
// Feature: ha-automatic-failover, Property 4: Epoch increment on promotion
// Validates: Requirements 1.2, 5.1

TEST(EpochProperty, IncrementOnPromotion) {
    rc::check("EpochValue{N}.incremented() == EpochValue{N+1} and WAL round-trip preserves N+1",
              []() {
                  // Generate N in [0, UINT64_MAX-1] — inRange upper bound is exclusive.
                  const auto n = *rc::gen::inRange<uint64_t>(
                      0, std::numeric_limits<uint64_t>::max());

                  const ob::EpochValue original{n};
                  const ob::EpochValue promoted = original.incremented();

                  // Verify pure increment logic.
                  RC_ASSERT(promoted == ob::EpochValue{n + 1});
                  RC_ASSERT(promoted.term == n + 1);

                  // Write the promoted epoch to WAL and replay to verify persistence.
                  const auto suffix = *rc::gen::arbitrary<uint64_t>();
                  const auto tmp = std::filesystem::temp_directory_path() /
                                   ("test_epoch_promo_" + std::to_string(suffix));
                  std::filesystem::create_directories(tmp);

                  {
                      ob::WALWriter writer(tmp.string(), 512ULL << 20,
                                           ob::FsyncPolicy::NONE);
                      writer.append_epoch(promoted);
                  }

                  ob::WALReplayer replayer(tmp.string());
                  replayer.replay([](const ob::WALRecord&, const uint8_t*) {});

                  RC_ASSERT(replayer.last_epoch() == n + 1);

                  std::filesystem::remove_all(tmp);
              });
}
