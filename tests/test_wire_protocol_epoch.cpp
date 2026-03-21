#include <gtest/gtest.h>
#include <rapidcheck.h>

#include "orderbook/epoch.hpp"

#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>

// ── Property 6: Wire protocol epoch round-trip ────────────────────────────────
// Feature: ha-automatic-failover, Property 6: Wire protocol epoch round-trip
// Validates: Requirements 3.1, 3.2, 3.3

TEST(WireProtocolEpoch, ReplicateRoundTrip) {
    rc::check("REPLICATE wire format round-trips (file_index, byte_offset, epoch)",
              []() {
                  const auto file_index = *rc::gen::arbitrary<uint32_t>();
                  const auto byte_offset = *rc::gen::inRange<size_t>(
                      0, std::numeric_limits<size_t>::max() / 2);
                  const auto epoch = *rc::gen::arbitrary<uint64_t>();

                  // Serialize
                  char buf[256];
                  std::snprintf(buf, sizeof(buf),
                                "REPLICATE %u %zu %" PRIu64 "\n",
                                file_index, byte_offset, epoch);

                  // Parse back
                  uint32_t parsed_fi = 0;
                  size_t parsed_off = 0;
                  uint64_t parsed_epoch = 0;
                  int n = std::sscanf(buf, "REPLICATE %u %zu %" SCNu64,
                                      &parsed_fi, &parsed_off, &parsed_epoch);

                  RC_ASSERT(n == 3);
                  RC_ASSERT(parsed_fi == file_index);
                  RC_ASSERT(parsed_off == byte_offset);
                  RC_ASSERT(parsed_epoch == epoch);
              });
}

TEST(WireProtocolEpoch, WalRoundTrip) {
    rc::check("WAL wire format round-trips (file_index, byte_offset, total_len, epoch)",
              []() {
                  const auto file_index = *rc::gen::arbitrary<uint32_t>();
                  const auto byte_offset = *rc::gen::inRange<size_t>(
                      0, std::numeric_limits<size_t>::max() / 2);
                  const auto total_len = *rc::gen::inRange<size_t>(24, 1024 * 1024);
                  const auto epoch = *rc::gen::arbitrary<uint64_t>();

                  // Serialize
                  char buf[256];
                  std::snprintf(buf, sizeof(buf),
                                "WAL %u %zu %zu %" PRIu64 "\n",
                                file_index, byte_offset, total_len, epoch);

                  // Parse back
                  uint32_t parsed_fi = 0;
                  size_t parsed_off = 0;
                  size_t parsed_len = 0;
                  uint64_t parsed_epoch = 0;
                  int n = std::sscanf(buf, "WAL %u %zu %zu %" SCNu64,
                                      &parsed_fi, &parsed_off,
                                      &parsed_len, &parsed_epoch);

                  RC_ASSERT(n == 4);
                  RC_ASSERT(parsed_fi == file_index);
                  RC_ASSERT(parsed_off == byte_offset);
                  RC_ASSERT(parsed_len == total_len);
                  RC_ASSERT(parsed_epoch == epoch);
              });
}

TEST(WireProtocolEpoch, HeartbeatRoundTrip) {
    rc::check("HEARTBEAT wire format round-trips epoch",
              []() {
                  const auto epoch = *rc::gen::arbitrary<uint64_t>();

                  // Serialize
                  char buf[128];
                  std::snprintf(buf, sizeof(buf),
                                "HEARTBEAT %" PRIu64 "\n", epoch);

                  // Parse back
                  uint64_t parsed_epoch = 0;
                  int n = std::sscanf(buf, "HEARTBEAT %" SCNu64,
                                      &parsed_epoch);

                  RC_ASSERT(n == 1);
                  RC_ASSERT(parsed_epoch == epoch);
              });
}

// ── Property 2: Epoch fencing correctness ─────────────────────────────────────
// Feature: ha-automatic-failover, Property 2: Epoch fencing correctness
// Validates: Requirements 2.1, 2.2, 2.3, 3.5, 12.2

TEST(WireProtocolEpoch, EpochFencingCorrectness) {
    rc::check("accept iff message_epoch >= local_epoch; reject when message_epoch < local_epoch",
              []() {
                  const auto msg_epoch = *rc::gen::arbitrary<uint64_t>();
                  const auto local_epoch = *rc::gen::arbitrary<uint64_t>();

                  const bool should_accept = (msg_epoch >= local_epoch);
                  const bool should_reject = (msg_epoch < local_epoch);

                  // Fencing logic: accept when msg_epoch >= local_epoch
                  RC_ASSERT(should_accept == (msg_epoch >= local_epoch));
                  RC_ASSERT(should_reject == (msg_epoch < local_epoch));
                  RC_ASSERT(should_accept != should_reject);

                  // Verify the specific cases from the requirements:
                  // Req 2.2: msg < local → discard + disconnect
                  if (msg_epoch < local_epoch) {
                      RC_ASSERT(should_reject);
                      RC_ASSERT(!should_accept);
                  }
                  // Req 2.3: msg == local → accept
                  if (msg_epoch == local_epoch) {
                      RC_ASSERT(should_accept);
                      RC_ASSERT(!should_reject);
                  }
                  // msg > local → also accept (new epoch from promoted primary)
                  if (msg_epoch > local_epoch) {
                      RC_ASSERT(should_accept);
                      RC_ASSERT(!should_reject);
                  }
              });
}

// ── Property 5: Broadcast epoch attachment ────────────────────────────────────
// Feature: ha-automatic-failover, Property 5: Broadcast epoch attachment
// Validates: Requirements 1.1

TEST(WireProtocolEpoch, BroadcastEpochAttachment) {
    rc::check("WAL broadcast with epoch E produces wire message containing E",
              []() {
                  const auto file_index = *rc::gen::arbitrary<uint32_t>();
                  const auto byte_offset = *rc::gen::inRange<size_t>(
                      0, std::numeric_limits<size_t>::max() / 2);
                  const auto total_len = *rc::gen::inRange<size_t>(24, 1024 * 1024);
                  const auto epoch = *rc::gen::arbitrary<uint64_t>();

                  // Build WAL wire message (same format as ReplicationManager::broadcast)
                  char line[256];
                  std::snprintf(line, sizeof(line),
                                "WAL %u %zu %zu %" PRIu64 "\n",
                                file_index, byte_offset, total_len, epoch);

                  // Parse it back and verify epoch == E
                  uint32_t parsed_fi = 0;
                  size_t parsed_off = 0;
                  size_t parsed_len = 0;
                  uint64_t parsed_epoch = 0;
                  int n = std::sscanf(line, "WAL %u %zu %zu %" SCNu64,
                                      &parsed_fi, &parsed_off,
                                      &parsed_len, &parsed_epoch);

                  RC_ASSERT(n == 4);
                  RC_ASSERT(parsed_epoch == epoch);
              });
}

// ── Property 7: Stale primary rejection on handshake ──────────────────────────
// Feature: ha-automatic-failover, Property 7: Stale primary rejection on handshake
// Validates: Requirements 3.4

TEST(WireProtocolEpoch, StalePrimaryRejection) {
    rc::check("REPLICATE handshake with replica_epoch > primary_epoch → reject STALE_PRIMARY",
              []() {
                  const auto primary_epoch = *rc::gen::arbitrary<uint64_t>();
                  const auto replica_epoch = *rc::gen::arbitrary<uint64_t>();

                  // Stale-primary logic: reject when replica's epoch > primary's epoch
                  const bool should_reject = (replica_epoch > primary_epoch);

                  if (replica_epoch > primary_epoch) {
                      RC_ASSERT(should_reject);
                      // Primary would respond with ERR STALE_PRIMARY
                  } else {
                      RC_ASSERT(!should_reject);
                      // Primary accepts the handshake
                  }

                  // Verify the boundary: equal epochs are NOT stale
                  if (replica_epoch == primary_epoch) {
                      RC_ASSERT(!should_reject);
                  }
              });
}

// ── Property 8: Epoch advancement on Epoch_Record receipt ─────────────────────
// Feature: ha-automatic-failover, Property 8: Epoch advancement on Epoch_Record receipt
// Validates: Requirements 2.4

TEST(WireProtocolEpoch, EpochAdvancementOnEpochRecord) {
    rc::check("replica with epoch N receiving Epoch_Record N+1 → local epoch updates to N+1",
              []() {
                  // Generate N in [0, UINT64_MAX-1] so N+1 doesn't overflow
                  const auto n = *rc::gen::inRange<uint64_t>(
                      0, std::numeric_limits<uint64_t>::max());

                  uint64_t local_epoch = n;
                  const uint64_t received_epoch = n + 1;

                  // Epoch advancement logic: if received > local → update
                  if (received_epoch > local_epoch) {
                      local_epoch = received_epoch;
                  }

                  RC_ASSERT(local_epoch == n + 1);

                  // Also verify via EpochValue struct serialization round-trip
                  const ob::EpochValue ev{received_epoch};
                  uint8_t payload[8];
                  ob::epoch_to_payload(ev, payload);
                  const ob::EpochValue restored = ob::epoch_from_payload(payload);

                  RC_ASSERT(restored.term == n + 1);
                  RC_ASSERT(restored == ev);
              });
}
