// Snapshot bootstrap over the multi-master protocol — roadmap #76, and the frontier half of #67.
//
// Why here and not through ReplicationManager: an MM node's WAL holds records from several
// origins, each numbered by the origin that minted it, and the primary→replica receive path
// applies records without sequence dedup or LWW conflict resolution — a replica has one source
// and needs neither. Serving that protocol from an MM node would put two protocols with
// different correctness rules over one WAL, where a single misconfiguration duplicates rows.
//
// Nothing new was needed on the wire. Frames after the handshake are untagged: each carries a
// WALRecordV2 header whose `record_type` is the only discriminator, `handle_frame()` branches on
// it, and an unknown value falls through to `handle_remote_record()`, which skips it and stays
// connected. That is the door the version vector went through, and these five messages go
// through it too — so a node running the older build stays in the cluster.

#include "orderbook/multi_master.hpp"

#include "orderbook/crc32c.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/logger.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <set>
#include <unistd.h>

namespace ob {

namespace fs = std::filesystem;

namespace {

void put_u16(std::vector<uint8_t>& out, uint16_t v) {
    out.push_back(static_cast<uint8_t>(v & 0xFF));
    out.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
}

void put_u32(std::vector<uint8_t>& out, uint32_t v) {
    for (int i = 0; i < 4; ++i) out.push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xFF));
}

void put_u64(std::vector<uint8_t>& out, uint64_t v) {
    for (int i = 0; i < 8; ++i) out.push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xFF));
}

uint16_t get_u16(const uint8_t* p) {
    return static_cast<uint16_t>(p[0] | (static_cast<uint16_t>(p[1]) << 8));
}

uint32_t get_u32(const uint8_t* p) {
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(p[i]) << (8 * i);
    return v;
}

uint64_t get_u64(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(p[i]) << (8 * i);
    return v;
}

}  // namespace

// ── Payload codecs ────────────────────────────────────────────────────────────

std::vector<uint8_t> encode_snapshot_begin(const SnapshotBegin& begin) {
    std::vector<uint8_t> out;
    out.reserve(MM_SNAPSHOT_BEGIN_SIZE);
    put_u32(out, begin.manifest_len);
    put_u32(out, begin.vector_len);
    put_u32(out, begin.held_len);
    put_u32(out, begin.meta_crc);
    return out;
}

bool decode_snapshot_begin(const uint8_t* data, size_t len, SnapshotBegin& out) {
    if (!data || len != MM_SNAPSHOT_BEGIN_SIZE) return false;
    SnapshotBegin b{};
    b.manifest_len = get_u32(data);
    b.vector_len   = get_u32(data + 4);
    b.held_len     = get_u32(data + 8);
    b.meta_crc     = get_u32(data + 12);

    // A manifest of zero bytes cannot describe anything, and there is no reason to open staging
    // for it.
    if (b.manifest_len == 0) return false;
    // The blob is assembled in memory, so the announced size is an allocation this peer chose.
    if (b.total() > MM_SNAPSHOT_MAX_META_BYTES) return false;

    out = b;
    return true;
}

std::vector<uint8_t> encode_snapshot_chunk(uint16_t file_index, uint64_t byte_offset,
                                           const uint8_t* bytes, size_t n) {
    std::vector<uint8_t> out;
    out.reserve(MM_SNAPSHOT_CHUNK_HEADER_SIZE + n);
    put_u16(out, file_index);
    put_u64(out, byte_offset);
    if (n > 0 && bytes) out.insert(out.end(), bytes, bytes + n);
    return out;
}

bool decode_snapshot_chunk(const uint8_t* data, size_t len,
                           uint16_t& file_index, uint64_t& byte_offset,
                           const uint8_t*& bytes, size_t& n) {
    if (!data || len < MM_SNAPSHOT_CHUNK_HEADER_SIZE) return false;
    file_index  = get_u16(data);
    byte_offset = get_u64(data + 2);
    n           = len - MM_SNAPSHOT_CHUNK_HEADER_SIZE;
    bytes       = (n > 0) ? data + MM_SNAPSHOT_CHUNK_HEADER_SIZE : nullptr;
    return true;
}

std::vector<uint8_t> encode_snapshot_end() {
    return {};
}

bool decode_snapshot_end(const uint8_t* data, size_t len) {
    (void)data;
    return len == 0;
}

std::vector<uint8_t> encode_snapshot_abort(std::string_view reason) {
    const size_t n = std::min(reason.size(), MM_SNAPSHOT_ABORT_REASON_MAX);
    return std::vector<uint8_t>(reason.begin(), reason.begin() + static_cast<long>(n));
}

std::string decode_snapshot_abort(const uint8_t* data, size_t len) {
    if (!data || len == 0) return "unspecified";
    const size_t n = std::min(len, MM_SNAPSHOT_ABORT_REASON_MAX);
    std::string out(reinterpret_cast<const char*>(data), n);
    // A peer must not be able to break our log lines with what it sends.
    for (char& c : out) {
        if (c < 0x20 || c == 0x7F) c = '?';
    }
    return out;
}


// ── Framing helper ────────────────────────────────────────────────────────────

namespace {

/// Wrap a snapshot payload in the envelope every post-handshake frame uses.
std::vector<uint8_t> wrap_snapshot_frame(uint8_t record_type, uint16_t origin_node_id,
                                        const std::vector<uint8_t>& payload) {
    WALRecordV2 hdr{};
    hdr.sequence_number = 0;
    hdr.timestamp_ns    = 0;
    hdr.checksum        = crc32c(payload.data(), payload.size());
    hdr.payload_len     = static_cast<uint16_t>(payload.size());
    hdr.record_type     = record_type;
    hdr.version         = 1;
    hdr.origin_node_id  = origin_node_id;
    std::memset(hdr.hlc_data, 0, sizeof(hdr.hlc_data));

    std::vector<uint8_t> frame;
    frame.reserve(MM_WALRECORD_V2_SIZE + payload.size());
    const auto* hdr_bytes = reinterpret_cast<const uint8_t*>(&hdr);
    frame.insert(frame.end(), hdr_bytes, hdr_bytes + MM_WALRECORD_V2_SIZE);
    frame.insert(frame.end(), payload.begin(), payload.end());
    return frame;
}

}  // namespace

// ── Sending side ──────────────────────────────────────────────────────────────

void MultiMasterManager::send_snapshot_abort(PeerConnection& peer, const char* reason) {
    const auto payload = encode_snapshot_abort(reason);
    const auto frame   = wrap_snapshot_frame(MM_MSG_SNAPSHOT_ABORT, config_.node_id, payload);
    enqueue_frame(peer, frame.data(), frame.size());
    OB_LOG_WARN("mm", "Snapshot aborted towards peer %u: %s", peer.node_id, reason);
}

void MultiMasterManager::handle_snapshot_request(PeerConnection& peer) {
    // Reached from io_loop() with MM's mtx_ held, and it calls into the engine — so it inherits
    // the lock order that #80 is about (io_loop takes MM::mtx_ then Engine::mtx_, while a client
    // write takes them the other way round). It also widens it: create_snapshot_with_sequence_state()
    // takes Engine::flush_mtx_ before Engine::mtx_. The cycle predates this path — the received-delta
    // path has the same shape — but whoever fixes #80 has to cover this call and
    // request_snapshot_from() below, not only apply_remote_delta().
    OB_LOG_INFO("mm", "Peer %u requested a snapshot", peer.node_id);

    if (snapshot_send_.active) {
        // One at a time. Creating a snapshot flushes and checksums the whole store, and two of
        // them at once would double that on a thread that also carries live traffic.
        OB_LOG_WARN("mm", "Refusing snapshot for peer %u: already sending one to peer %u",
                    peer.node_id, snapshot_send_.target_node_id);
        engine_.registry().increment_counter("ob_mm_snapshot_refused_total");
        send_snapshot_abort(peer, "busy");
        return;
    }

    Engine::SnapshotWithSequenceState snap;
    try {
        snap = engine_.create_snapshot_with_sequence_state();
    } catch (const std::exception& e) {
        OB_LOG_ERROR("mm", "Snapshot creation failed for peer %u: %s", peer.node_id, e.what());
        engine_.registry().increment_counter("ob_mm_snapshot_failed_total");
        send_snapshot_abort(peer, "create_failed");
        return;
    }
    engine_.registry().set_gauge("ob_mm_snapshot_create_ms",
                                 static_cast<int64_t>(snap.create_ms));

    if (snap.vector_truncated) {
        // Without a vector the receiver would install the files and then declare no frontier at
        // all — so every peer would resend everything and append it a second time into
        // append-only storage. Refusing costs a bootstrap; accepting costs duplicate rows.
        OB_LOG_ERROR("mm",
                     "Refusing snapshot for peer %u: our version vector does not fit a frame, "
                     "so the receiver could not declare any frontier from it",
                     peer.node_id);
        engine_.registry().increment_counter("ob_mm_snapshot_failed_total");
        send_snapshot_abort(peer, "vector_untransportable");
        return;
    }
    if (snap.held_truncated) {
        // Bounded and survivable, unlike the above: the numbers left out are redelivered and
        // appended twice, which is the cost #75 already accepts for a bounded held set.
        OB_LOG_WARN("mm",
                    "Held set trimmed for peer %u's snapshot — the numbers left out may be "
                    "stored twice there", peer.node_id);
    }

    // Sort the file list into the order `to_json()` writes, before serialising it. The chunk
    // header names a file by its index in the manifest, and `to_json()` sorts by path for
    // deterministic output — so index 0 on this side was a different file from index 0 on the
    // receiver's, and the first chunk was rejected for exceeding a size that belonged to another
    // file. The index has to mean the same thing on both sides, so the sender adopts the order it
    // is about to transmit.
    std::sort(snap.manifest.files.begin(), snap.manifest.files.end(),
              [](const SnapshotFileEntry& a, const SnapshotFileEntry& b) {
                  return a.path < b.path;
              });
    // A chunk names its file with a uint16_t, and 0xFFFF is taken by the metadata blob. So a
    // manifest of 65535 files or more cannot be addressed at all — the index would wrap, or
    // collide with MM_SNAPSHOT_META_INDEX, and the receiver would write one file's bytes into
    // another. Refusing with a reason beats either.
    if (snap.manifest.files.size() >= MM_SNAPSHOT_META_INDEX) {
        OB_LOG_ERROR("mm",
                     "Refusing snapshot for peer %u: %zu files cannot be addressed by a 16-bit "
                     "index (limit %u)",
                     peer.node_id, snap.manifest.files.size(),
                     static_cast<unsigned>(MM_SNAPSHOT_META_INDEX));
        engine_.registry().increment_counter("ob_mm_snapshot_failed_total");
        send_snapshot_abort(peer, "too_many_files");
        return;
    }

    const std::string manifest_json = snap.manifest.to_json();
    const auto vv_payload   = serialize_version_vector(snap.vector, /*truncated=*/false);
    const auto held_payload = snap.held.empty() ? std::vector<uint8_t>{}
                                                : serialize_held_ranges(snap.held);

    auto& st = snapshot_send_;
    st = MMSnapshotSend{};
    st.manifest       = std::move(snap.manifest);
    st.target_node_id = peer.node_id;
    st.started_at     = std::chrono::steady_clock::now();

    st.meta.reserve(manifest_json.size() + vv_payload.size() + held_payload.size());
    st.meta.insert(st.meta.end(), manifest_json.begin(), manifest_json.end());
    st.meta.insert(st.meta.end(), vv_payload.begin(), vv_payload.end());
    st.meta.insert(st.meta.end(), held_payload.begin(), held_payload.end());

    if (st.meta.size() > MM_SNAPSHOT_MAX_META_BYTES) {
        OB_LOG_ERROR("mm",
                     "Refusing snapshot for peer %u: metadata is %zu bytes, over the %zu a "
                     "receiver will assemble",
                     peer.node_id, st.meta.size(), MM_SNAPSHOT_MAX_META_BYTES);
        engine_.registry().increment_counter("ob_mm_snapshot_failed_total");
        st = MMSnapshotSend{};
        send_snapshot_abort(peer, "metadata_too_large");
        return;
    }

    SnapshotBegin begin{};
    begin.manifest_len = static_cast<uint32_t>(manifest_json.size());
    begin.vector_len   = static_cast<uint32_t>(vv_payload.size());
    begin.held_len     = static_cast<uint32_t>(held_payload.size());
    begin.meta_crc     = crc32c(st.meta.data(), st.meta.size());

    st.active = true;

    const auto frame = wrap_snapshot_frame(MM_MSG_SNAPSHOT_BEGIN, config_.node_id,
                                           encode_snapshot_begin(begin));
    enqueue_frame(peer, frame.data(), frame.size());

    OB_LOG_INFO("mm",
                "Snapshot begins towards peer %u: files=%zu bytes=%zu meta=%zu "
                "(manifest=%u vector=%u held=%u) created in %.1f ms",
                peer.node_id, st.manifest.files.size(), st.manifest.total_bytes,
                st.meta.size(), begin.manifest_len, begin.vector_len, begin.held_len,
                snap.create_ms);

    advance_snapshot_send(peer);
}

void MultiMasterManager::advance_snapshot_send(PeerConnection& peer) {
    auto& st = snapshot_send_;
    if (!st.active || st.target_node_id != peer.node_id) return;
    if (!peer.connected || peer.fd < 0) {
        finish_snapshot_send(false, "peer_gone");
        return;
    }

    // Stop while the peer's buffer is still holding a backlog. Chunks resume from the EPOLLOUT
    // branch as the socket drains, so live deltas enqueued in between go out in order and the
    // buffer never approaches the size that drops the connection.
    while (peer.send_buf.size() < config_.snapshot_low_watermark_bytes) {
        std::vector<uint8_t> payload;

        if (st.meta_offset < st.meta.size()) {
            const size_t n = std::min(MM_SNAPSHOT_CHUNK_BYTES, st.meta.size() - st.meta_offset);
            payload = encode_snapshot_chunk(MM_SNAPSHOT_META_INDEX, st.meta_offset,
                                            st.meta.data() + st.meta_offset, n);
            st.meta_offset += n;
            st.bytes_sent  += n;
        } else if (st.file_idx < st.manifest.files.size()) {
            const auto& entry = st.manifest.files[st.file_idx];

            if (st.fd < 0) {
                const std::string path = engine_.base_dir() + "/" + entry.path;
                st.fd = ::open(path.c_str(), O_RDONLY);
                if (st.fd < 0) {
                    OB_LOG_ERROR("mm", "Cannot open '%s' for peer %u: %s",
                                 path.c_str(), peer.node_id, std::strerror(errno));
                    send_snapshot_abort(peer, "file_open_failed");
                    finish_snapshot_send(false, "file_open_failed");
                    return;
                }
                OB_LOG_DEBUG("mm", "Sending snapshot file %zu/%zu '%s' (%zu bytes) to peer %u",
                             st.file_idx + 1, st.manifest.files.size(), entry.path.c_str(),
                             entry.size, peer.node_id);
            }

            if (st.file_offset >= entry.size) {
                // Zero-length files exist (an empty meta.json is legal), so this also covers
                // "nothing to read at all".
                ::close(st.fd);
                st.fd = -1;
                st.file_offset = 0;
                ++st.file_idx;
                continue;
            }

            uint8_t buf[MM_SNAPSHOT_CHUNK_BYTES];
            const size_t want = std::min(MM_SNAPSHOT_CHUNK_BYTES, entry.size - st.file_offset);
            const ssize_t got = ::pread(st.fd, buf, want,
                                        static_cast<off_t>(st.file_offset));
            if (got <= 0) {
                OB_LOG_ERROR("mm", "Short read on '%s' at %zu for peer %u: %s",
                             entry.path.c_str(), st.file_offset, peer.node_id,
                             got < 0 ? std::strerror(errno) : "unexpected EOF");
                send_snapshot_abort(peer, "file_read_failed");
                finish_snapshot_send(false, "file_read_failed");
                return;
            }

            payload = encode_snapshot_chunk(static_cast<uint16_t>(st.file_idx), st.file_offset,
                                            buf, static_cast<size_t>(got));
            st.file_offset += static_cast<size_t>(got);
            st.bytes_sent  += static_cast<uint64_t>(got);
        } else {
            const auto frame = wrap_snapshot_frame(MM_MSG_SNAPSHOT_END, config_.node_id,
                                                    encode_snapshot_end());
            enqueue_frame(peer, frame.data(), frame.size());
            engine_.registry().increment_counter("ob_mm_snapshot_sent_total");
            finish_snapshot_send(true, "complete");
            return;
        }

        const auto frame = wrap_snapshot_frame(MM_MSG_SNAPSHOT_CHUNK, config_.node_id, payload);
        enqueue_frame(peer, frame.data(), frame.size());
        engine_.registry().increment_counter("ob_mm_snapshot_bytes_sent_total",
                                             static_cast<uint64_t>(payload.size()));

        // enqueue_frame() may have dropped the peer for not draining, and a closed socket must
        // not be written to again.
        if (!peer.connected || peer.fd < 0) {
            finish_snapshot_send(false, "peer_dropped_mid_transfer");
            return;
        }
    }

    OB_LOG_DEBUG("mm",
                 "Snapshot to peer %u paused at meta=%zu/%zu file=%zu/%zu (send_buf=%zu)",
                 peer.node_id, st.meta_offset, st.meta.size(), st.file_idx,
                 st.manifest.files.size(), peer.send_buf.size());
}

void MultiMasterManager::finish_snapshot_send(bool succeeded, const char* reason) {
    auto& st = snapshot_send_;
    if (!st.active) return;

    if (st.fd >= 0) {
        ::close(st.fd);
        st.fd = -1;
    }

    const auto elapsed = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - st.started_at).count();
    if (succeeded) {
        OB_LOG_INFO("mm",
                    "Snapshot sent to peer %u: files=%zu bytes=%llu in %.1f s (%s)",
                    st.target_node_id, st.manifest.files.size(),
                    static_cast<unsigned long long>(st.bytes_sent), elapsed, reason);
    } else {
        OB_LOG_WARN("mm",
                    "Snapshot to peer %u ended early after %llu bytes in %.1f s: %s",
                    st.target_node_id,
                    static_cast<unsigned long long>(st.bytes_sent), elapsed, reason);
        engine_.registry().increment_counter("ob_mm_snapshot_failed_total");
    }

    st = MMSnapshotSend{};
}


// ── Receiving side ────────────────────────────────────────────────────────────

void MultiMasterManager::handle_snapshot_begin(PeerConnection& peer,
                                               const uint8_t* payload, size_t len) {
    if (snapshot_recv_.active) {
        OB_LOG_WARN("mm",
                    "Peer %u sent SNAPSHOT_BEGIN while a bootstrap from peer %u is in progress",
                    peer.node_id, snapshot_recv_.source_node_id);
        send_snapshot_abort(peer, "already_bootstrapping");
        return;
    }

    SnapshotBegin begin{};
    if (!decode_snapshot_begin(payload, len, begin)) {
        OB_LOG_ERROR("mm", "Peer %u sent an unusable SNAPSHOT_BEGIN (%zu bytes)",
                     peer.node_id, len);
        engine_.registry().increment_counter("ob_mm_snapshot_failed_total");
        send_snapshot_abort(peer, "bad_begin");
        return;
    }

    auto& st = snapshot_recv_;
    st = MMSnapshotRecv{};
    st.active         = true;
    st.phase          = MMSnapshotRecv::Phase::META;
    st.source_node_id = peer.node_id;
    st.announced      = begin;
    st.started_at     = std::chrono::steady_clock::now();
    st.meta.reserve(begin.total());
    st.staging_dir    = engine_.base_dir() + "/mm_snapshot_staging";

    std::error_code ec;
    fs::remove_all(st.staging_dir, ec);
    fs::create_directories(st.staging_dir, ec);
    if (ec) {
        OB_LOG_ERROR("mm", "Cannot create staging directory '%s': %s",
                     st.staging_dir.c_str(), ec.message().c_str());
        st = MMSnapshotRecv{};
        send_snapshot_abort(peer, "staging_unavailable");
        engine_.registry().increment_counter("ob_mm_snapshot_failed_total");
        return;
    }

    // Refuse writes from this point. Applying anything now would be applying it to contents that
    // load_snapshot() is about to discard.
    start_bootstrap();

    OB_LOG_INFO("mm",
                "Bootstrap from peer %u begins: metadata=%zu bytes (manifest=%u vector=%u "
                "held=%u), staging='%s'",
                peer.node_id, begin.total(), begin.manifest_len, begin.vector_len,
                begin.held_len, st.staging_dir.c_str());
}

void MultiMasterManager::handle_snapshot_chunk(PeerConnection& peer,
                                              const uint8_t* payload, size_t len) {
    auto& st = snapshot_recv_;
    if (!st.active) {
        OB_LOG_WARN("mm", "Peer %u sent a snapshot chunk with no bootstrap in progress",
                    peer.node_id);
        return;
    }
    if (peer.node_id != st.source_node_id) {
        OB_LOG_WARN("mm", "Peer %u sent a chunk for peer %u's bootstrap — ignored",
                    peer.node_id, st.source_node_id);
        return;
    }

    uint16_t file_index = 0;
    uint64_t offset     = 0;
    const uint8_t* bytes = nullptr;
    size_t n = 0;
    if (!decode_snapshot_chunk(payload, len, file_index, offset, bytes, n)) {
        abort_bootstrap("bad_chunk");
        return;
    }

    engine_.registry().increment_counter("ob_mm_snapshot_bytes_received_total",
                                         static_cast<uint64_t>(n));

    if (st.phase == MMSnapshotRecv::Phase::META) {
        if (file_index != MM_SNAPSHOT_META_INDEX) {
            abort_bootstrap("file_chunk_before_metadata");
            return;
        }
        // The offset has to be the next one. A chunk written wherever it claims to belong would
        // leave a hole of zeros that the checksum only catches at the end of the blob — and a
        // gap in a JSON manifest is not something to discover late.
        if (offset != st.meta.size()) {
            OB_LOG_ERROR("mm",
                         "Peer %u sent metadata at offset %llu, expected %zu",
                         peer.node_id, static_cast<unsigned long long>(offset), st.meta.size());
            abort_bootstrap("metadata_offset_out_of_order");
            return;
        }
        if (st.meta.size() + n > st.announced.total()) {
            abort_bootstrap("metadata_longer_than_announced");
            return;
        }
        if (n > 0) st.meta.insert(st.meta.end(), bytes, bytes + n);
        st.bytes_received += n;

        if (st.meta.size() < st.announced.total()) return;      // more to come

        // Blob complete: check it, parse it, and validate every path before a byte of file data
        // is written anywhere.
        const uint32_t crc = crc32c(st.meta.data(), st.meta.size());
        if (crc != st.announced.meta_crc) {
            OB_LOG_ERROR("mm", "Metadata CRC mismatch from peer %u: got %u expected %u",
                         peer.node_id, crc, st.announced.meta_crc);
            abort_bootstrap("metadata_crc_mismatch");
            return;
        }

        const std::string manifest_json(
            reinterpret_cast<const char*>(st.meta.data()), st.announced.manifest_len);
        if (!SnapshotManifest::from_json(manifest_json, st.manifest)) {
            abort_bootstrap("manifest_unparseable");
            return;
        }

        const uint8_t* vv_ptr = st.meta.data() + st.announced.manifest_len;
        PeerVector pv;
        if (st.announced.vector_len > 0) {
            if (!pv.deserialize(vv_ptr, st.announced.vector_len)) {
                abort_bootstrap("vector_unparseable");
                return;
            }
            if (pv.truncated()) {
                // The sender should have refused rather than sent this. Adopting an empty vector
                // after discarding our contents would leave every frontier at zero, so peers
                // would resend the whole snapshot's worth of records into append-only storage.
                abort_bootstrap("vector_says_send_everything");
                return;
            }
            st.vector = pv.entries();
        }
        if (st.announced.held_len > 0) {
            if (!deserialize_held_ranges(vv_ptr + st.announced.vector_len,
                                         st.announced.held_len, st.held)) {
                abort_bootstrap("held_unparseable");
                return;
            }
        }

        const std::string& data_dir = engine_.base_dir();
        for (const auto& entry : st.manifest.files) {
            if (!is_safe_snapshot_path(entry.path) ||
                !path_stays_within(data_dir, entry.path) ||
                !path_stays_within(st.staging_dir, entry.path)) {
                OB_LOG_ERROR("mm", "Peer %u's manifest contains an unsafe path: '%s'",
                             peer.node_id, entry.path.c_str());
                // The whole snapshot, not just this entry. The replication client skips the bad
                // entry and installs the rest, which reports success for an incomplete
                // install; here that install also discards what was there before.
                abort_bootstrap("unsafe_path_in_manifest");
                return;
            }
        }

        st.phase       = MMSnapshotRecv::Phase::FILES;
        st.file_idx    = 0;
        st.file_offset = 0;
        st.running_crc = crc32c_init;
        OB_LOG_INFO("mm",
                    "Bootstrap metadata from peer %u accepted: files=%zu bytes=%zu rows=%zu "
                    "vector=%zu held=%zu",
                    peer.node_id, st.manifest.files.size(), st.manifest.total_bytes,
                    st.manifest.total_rows, st.vector.size(), st.held.size());
        return;
    }

    // ── File data ─────────────────────────────────────────────────────────────
    if (file_index == MM_SNAPSHOT_META_INDEX) {
        abort_bootstrap("metadata_chunk_after_metadata");
        return;
    }
    if (st.file_idx >= st.manifest.files.size()) {
        abort_bootstrap("chunk_after_last_file");
        return;
    }
    if (file_index != st.file_idx) {
        OB_LOG_ERROR("mm", "Peer %u sent file %u, expected %zu",
                     peer.node_id, file_index, st.file_idx);
        abort_bootstrap("file_out_of_order");
        return;
    }
    if (offset != st.file_offset) {
        OB_LOG_ERROR("mm", "Peer %u sent file %u at offset %llu, expected %zu",
                     peer.node_id, file_index, static_cast<unsigned long long>(offset),
                     st.file_offset);
        abort_bootstrap("chunk_offset_out_of_order");
        return;
    }

    const auto& entry = st.manifest.files[st.file_idx];
    if (st.file_offset + n > entry.size) {
        abort_bootstrap("file_longer_than_manifest");
        return;
    }

    if (st.fd < 0) {
        const std::string path = st.staging_dir + "/" + entry.path;
        std::error_code ec;
        fs::create_directories(fs::path(path).parent_path(), ec);
        st.fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (st.fd < 0) {
            OB_LOG_ERROR("mm", "Cannot stage '%s': %s", path.c_str(), std::strerror(errno));
            abort_bootstrap("staging_open_failed");
            return;
        }
    }

    size_t written = 0;
    while (written < n) {
        const ssize_t w = ::write(st.fd, bytes + written, n - written);
        if (w <= 0) {
            OB_LOG_ERROR("mm", "Write to staging failed: %s", std::strerror(errno));
            abort_bootstrap("staging_write_failed");
            return;
        }
        written += static_cast<size_t>(w);
    }
    st.running_crc = crc32c_update(st.running_crc, bytes, n);
    st.file_offset += n;
    st.bytes_received += n;

    if (st.file_offset < entry.size) return;                   // more of this file to come

    ::close(st.fd);
    st.fd = -1;
    const uint32_t file_crc = crc32c_finish(st.running_crc);
    if (file_crc != entry.crc32c) {
        OB_LOG_ERROR("mm", "Staged '%s' checksums to %u, manifest says %u",
                     entry.path.c_str(), file_crc, entry.crc32c);
        abort_bootstrap("file_crc_mismatch");
        return;
    }
    OB_LOG_DEBUG("mm", "Staged file %zu/%zu '%s' (%zu bytes, crc ok)",
                 st.file_idx + 1, st.manifest.files.size(), entry.path.c_str(), entry.size);

    ++st.file_idx;
    st.file_offset = 0;
    st.running_crc = crc32c_init;
}

void MultiMasterManager::handle_snapshot_end(PeerConnection& peer,
                                             const uint8_t* payload, size_t len) {
    auto& st = snapshot_recv_;
    if (!st.active) {
        OB_LOG_WARN("mm", "Peer %u sent SNAPSHOT_END with no bootstrap in progress",
                    peer.node_id);
        return;
    }
    if (peer.node_id != st.source_node_id) return;

    if (!decode_snapshot_end(payload, len)) {
        abort_bootstrap("bad_end");
        return;
    }
    if (st.phase != MMSnapshotRecv::Phase::FILES) {
        abort_bootstrap("end_before_metadata_complete");
        return;
    }
    if (st.file_idx != st.manifest.files.size()) {
        OB_LOG_ERROR("mm", "Peer %u ended the snapshot after %zu of %zu files",
                     peer.node_id, st.file_idx, st.manifest.files.size());
        abort_bootstrap("end_with_files_missing");
        return;
    }

    if (!install_snapshot_files()) {
        // Half-installed is the one state worth avoiding above all: the data directory now holds
        // a mixture. Say so at ERROR and leave the flag cleared so the node is usable, because a
        // node that refuses writes for ever is the worse answer (#73).
        OB_LOG_ERROR("mm", "Installing peer %u's snapshot failed part-way through",
                     peer.node_id);
        abort_bootstrap("install_failed");
        return;
    }

    engine_.load_snapshot(st.manifest);
    engine_.adopt_snapshot_sequence_state(st.vector, st.held);

    const auto elapsed = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - st.started_at).count();
    OB_LOG_INFO("mm",
                "Bootstrap from peer %u complete: files=%zu bytes=%llu rows=%zu in %.1f s",
                peer.node_id, st.manifest.files.size(),
                static_cast<unsigned long long>(st.bytes_received),
                st.manifest.total_rows, elapsed);
    engine_.registry().increment_counter("ob_mm_snapshot_received_total");

    std::error_code ec;
    fs::remove_all(st.staging_dir, ec);
    st = MMSnapshotRecv{};

    finish_bootstrap(/*succeeded=*/true);
}

bool MultiMasterManager::install_snapshot_files() {
    auto& st = snapshot_recv_;
    const std::string& data_dir = engine_.base_dir();
    bool all_ok = true;

    // Check every staged file before renaming any of them. Renaming as we go and stopping at the
    // first failure leaves the data directory holding part of one snapshot and part of whatever
    // was there before — the one outcome worth more than the transfer itself. This does not make
    // the install atomic against a filesystem error half-way through, which would need a journal;
    // it does make the reachable failures — a file that never arrived, a file the wrong length —
    // fail before anything is touched.
    for (const auto& entry : st.manifest.files) {
        const std::string src = st.staging_dir + "/" + entry.path;
        std::error_code ec;
        const auto staged_size = fs::file_size(src, ec);
        if (ec || staged_size != entry.size) {
            OB_LOG_ERROR("mm",
                         "Staged '%s' is %s (manifest says %zu bytes) — installing nothing",
                         entry.path.c_str(),
                         ec ? "missing" : ("of size " + std::to_string(staged_size)).c_str(),
                         entry.size);
            return false;
        }
    }

    for (const auto& entry : st.manifest.files) {
        // Re-checked here rather than trusted from handle_snapshot_chunk(): this is the call that
        // writes inside the live data directory.
        if (!is_safe_snapshot_path(entry.path) || !path_stays_within(data_dir, entry.path)) {
            OB_LOG_ERROR("mm", "Refusing to install unsafe path '%s'", entry.path.c_str());
            return false;
        }

        const std::string src = st.staging_dir + "/" + entry.path;
        const std::string dst = data_dir + "/" + entry.path;

        std::error_code ec;
        fs::create_directories(fs::path(dst).parent_path(), ec);
        fs::rename(src, dst, ec);
        if (ec) {
            // Cross-device staging: copy then remove.
            std::error_code copy_ec;
            fs::copy_file(src, dst, fs::copy_options::overwrite_existing, copy_ec);
            if (copy_ec) {
                OB_LOG_ERROR("mm", "Cannot install '%s': %s",
                             entry.path.c_str(), copy_ec.message().c_str());
                all_ok = false;
                break;
            }
            fs::remove(src, copy_ec);
        }
    }
    return all_ok;
}

void MultiMasterManager::abort_bootstrap(const char* reason) {
    auto& st = snapshot_recv_;
    if (!st.active) return;

    if (st.fd >= 0) {
        ::close(st.fd);
        st.fd = -1;
    }

    const auto elapsed = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - st.started_at).count();
    OB_LOG_ERROR("mm",
                 "Bootstrap from peer %u abandoned after %llu bytes in %.1f s: %s — the data "
                 "directory is untouched",
                 st.source_node_id, static_cast<unsigned long long>(st.bytes_received),
                 elapsed, reason);
    engine_.registry().increment_counter("ob_mm_snapshot_failed_total");

    std::error_code ec;
    fs::remove_all(st.staging_dir, ec);
    st = MMSnapshotRecv{};

    // A node that cannot bootstrap says so loudly and becomes usable. Sitting in a state that
    // refuses every write is the failure mode #73 and #76 were both about.
    finish_bootstrap(/*succeeded=*/false);
}

void MultiMasterManager::on_peer_disconnected(PeerConnection& peer) {
    if (snapshot_send_.active && snapshot_send_.target_node_id == peer.node_id) {
        finish_snapshot_send(false, "peer_disconnected");
    }
    if (snapshot_recv_.active && snapshot_recv_.source_node_id == peer.node_id) {
        abort_bootstrap("source_disconnected");
    }
}

bool MultiMasterManager::request_snapshot_from(PeerConnection& peer) {
    if (snapshot_recv_.active) {
        OB_LOG_DEBUG("mm", "Not asking peer %u for a snapshot: already bootstrapping from %u",
                     peer.node_id, snapshot_recv_.source_node_id);
        return false;
    }
    if (!peer.connected || !peer.handshake_done) return false;

    // Installing a snapshot discards local contents, so this is gated on holding nothing at all
    // rather than on being behind. A node with data of its own must never wipe it because a peer
    // looked further ahead — that decision has an owner, and it is not this branch.
    // Another call into the engine from under MM's mtx_ — see the note in
    // handle_snapshot_request() and #80.
    if (!engine_.holds_no_data()) {
        OB_LOG_DEBUG("mm",
                     "Not asking peer %u for a snapshot: this node holds data of its own",
                     peer.node_id);
        return false;
    }

    const auto frame = wrap_snapshot_frame(MM_MSG_SNAPSHOT_REQUEST, config_.node_id, {});
    enqueue_frame(peer, frame.data(), frame.size());
    engine_.registry().increment_counter("ob_mm_snapshot_requested_total");
    OB_LOG_INFO("mm",
                "Asked peer %u for a snapshot: this node holds nothing, and the peer reports "
                "%zu version-vector entries",
                peer.node_id, peer.peer_vector.entry_count());
    return true;
}

}  // namespace ob
