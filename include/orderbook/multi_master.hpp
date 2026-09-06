#pragma once

#include "orderbook/auth.hpp"

// ── MultiMasterManager — peer networking, WAL broadcast, loop prevention ─────
//
// Central component managing multi-master replication: epoll-based peer
// connections, WAL record broadcast, origin-based loop prevention, bootstrap
// state management, and diagnostic commands (MM_PEERS, MM_CONFLICTS).
//
// Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 9.1, 9.2, 9.3, 9.4

#include "orderbook/anti_entropy.hpp"
#include "orderbook/async_snapshot.hpp"
#include "orderbook/conflict_resolver.hpp"
#include "orderbook/hlc.hpp"
#include "orderbook/peer_registry.hpp"
#include "orderbook/replication.hpp"
#include "orderbook/tls.hpp"
#include "orderbook/wal.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include "orderbook/version_vector.hpp"

#include <thread>
#include <unordered_map>
#include <vector>

namespace ob {

// ── Protocol constants ────────────────────────────────────────────────────────

inline constexpr uint16_t MM_PROTOCOL_VERSION   = 2;   // 2 = exchanges version vectors
inline constexpr size_t   MM_MAX_VV_ENTRIES     = 4096; // ~172 kB on the wire
/// How much queued output one peer may hold before it is dropped. Same ceiling as a client
/// session gets since #59, and for the same reason: a peer that stops reading otherwise grows
/// the writer without bound — measured at ~113 MB/s per unreachable peer.
inline constexpr size_t   MM_MAX_PEER_SEND_BUF  = 64ULL << 20;
inline constexpr uint64_t MM_VV_GRACE_MS        = 2000; // wait for a peer's vector before
                                                        // assuming it holds nothing
/// How long a dial to a peer may take before it is treated as failed (#97).
///
/// A constant rather than a flag, and the reason is that the kernel's own answer is unusable: a SYN
/// that goes nowhere is retried for `tcp_syn_retries` doublings, about 127 s on a default Linux, and
/// nothing in a mesh wants to wait that long to learn that a peer is unreachable. Five seconds
/// allows two SYN retransmissions (the first at 1 s, the second at 3 s cumulative), so a peer that
/// is merely slow to answer still connects, and one that is unreachable is retried by the backoff
/// rather than waited on.
inline constexpr uint32_t MM_CONNECT_TIMEOUT_MS  = 5000;

inline constexpr size_t   MM_FRAME_HEADER_SIZE  = 4;            // uint32 LE length
inline constexpr size_t   MM_HANDSHAKE_SIZE     = 17;           // HandshakeMessage wire size
inline constexpr size_t   MM_MAX_FRAME_PAYLOAD  = 64ULL << 20;  // 64 MB
inline constexpr size_t   MM_WALRECORD_V2_SIZE  = 38;           // WALRecordV2 header size

// ── Snapshot bootstrap (#76) ──────────────────────────────────────────────────
//
// Frames after the handshake are untagged: each is a WALRecordV2 header plus payload, and the
// type is read from `record_type`. handle_frame() branches on it and an unknown value falls
// through to handle_remote_record(), which skips it and stays connected — which is how the
// version vector (record type 7) was added without a protocol version bump, and how these are
// added now.
//
// The numbers sit in a reserved range far above the WAL's own record types (1-8, and growing) so
// that adding a ninth WAL record type can never collide with a wire-only message. See the
// reservation note next to the WAL_RECORD_* constants in wal.hpp.
inline constexpr uint8_t MM_MSG_SNAPSHOT_REQUEST = 200;
inline constexpr uint8_t MM_MSG_SNAPSHOT_BEGIN   = 201;
inline constexpr uint8_t MM_MSG_SNAPSHOT_CHUNK   = 202;
inline constexpr uint8_t MM_MSG_SNAPSHOT_END     = 203;
inline constexpr uint8_t MM_MSG_SNAPSHOT_ABORT   = 204;

// ── Authentication (#30 part two) ─────────────────────────────────────────────
//
// Two frames, exchanged before HandshakeMessage when a cluster secret is configured. Mutual by
// symmetry: both sides challenge on connect and neither sends its handshake until it has verified
// the other, so the handshake *is* the acceptance - there is no third message.
//
// Framing disambiguates them from a handshake without a version bump. A handshake frame is exactly
// MM_HANDSHAKE_SIZE bytes; these carry a WALRecordV2 header (38 B) plus a payload, so they can
// never be 17. A 17-byte frame from an unauthenticated peer therefore means **a peer without
// authentication**, and is logged as that rather than as a short handshake.
inline constexpr uint8_t MM_MSG_AUTH_CHALLENGE  = 205;
inline constexpr uint8_t MM_MSG_AUTH_RESPONSE   = 206;

/// Bytes of file content per chunk frame.
///
/// Bounded above by the frame header, not by taste: `WALRecordV2::payload_len` is a uint16_t and
/// the receiver disconnects a peer whose `payload_len` disagrees with the frame it arrived in, so
/// no frame may carry more than 65535 bytes of payload (#78). 32 kB leaves room for the chunk
/// header and keeps a live delta enqueued between two chunks from waiting on a long write.
inline constexpr size_t MM_SNAPSHOT_CHUNK_BYTES = 32ULL << 10;

/// Reserved `file_index` naming the metadata blob rather than a file from the manifest.
///
/// The manifest, the version vector and the held set are streamed through the same chunk
/// mechanism as the files. That is not economy of message types: the manifest for a store with a
/// few thousand segments passes 64 kB on its own, and a version vector of 1500 entries is 63 kB,
/// so metadata carried in one frame would have imposed a store-size limit on bootstrap — the very
/// case bootstrap exists for. Streaming it reuses the offset discipline and the checksum.
inline constexpr uint16_t MM_SNAPSHOT_META_INDEX = 0xFFFF;

/// Upper bound on the metadata blob, which is assembled in memory. Roughly a hundred thousand
/// files' worth of manifest; a peer claiming more is refused rather than trusted.
inline constexpr size_t MM_SNAPSHOT_MAX_META_BYTES = 8ULL << 20;

/// SNAPSHOT_BEGIN payload: three uint32 lengths and a uint32 CRC32C of the metadata blob.
inline constexpr size_t MM_SNAPSHOT_BEGIN_SIZE = 16;

/// Stop pushing chunks while the peer's send buffer holds at least this much.
///
/// Far below MM_MAX_PEER_SEND_BUF, because exceeding *that* drops the connection
/// (drop_peer_if_send_buf_too_large): a snapshot that kills the peer with its own size would be
/// a funny way to fail. The gap is also what keeps live traffic moving — chunks resume from the
/// EPOLLOUT branch as the socket drains.
inline constexpr size_t MM_SNAPSHOT_LOW_WATERMARK = 4ULL << 20;

/// Upper bound on an abort reason, so a peer cannot make us log an arbitrary amount of text.
inline constexpr size_t MM_SNAPSHOT_ABORT_REASON_MAX = 128;

/// Fixed part of a chunk payload: uint16 file_index + uint64 byte_offset.
inline constexpr size_t MM_SNAPSHOT_CHUNK_HEADER_SIZE = 10;

// ── HandshakeMessage ──────────────────────────────────────────────────────────
//
// Binary layout (17 bytes, little-endian):
//   offset 0:  node_id                (uint16 LE)
//   offset 2:  protocol_version       (uint16 LE)
//   offset 4:  compression_preference (uint8)
//   offset 5:  wal_file_index         (uint32 LE)
//   offset 9:  wal_byte_offset        (uint64 LE)

struct HandshakeMessage {
    uint16_t node_id{0};
    uint16_t protocol_version{1};
    uint8_t  compression_preference{0};  // 0=none, 1=LZ4
    uint32_t wal_file_index{0};
    uint64_t wal_byte_offset{0};

    /// Serialize to 17-byte LE buffer.
    void serialize(uint8_t out[MM_HANDSHAKE_SIZE]) const;

    /// Deserialize from buffer. Returns false if buffer too short (<17 bytes).
    static bool deserialize(const uint8_t* data, size_t len, HandshakeMessage& out);

    /// Pretty-print for diagnostics.
    std::string to_string() const;

    /// Equality operators.
    bool operator==(const HandshakeMessage& o) const;
    bool operator!=(const HandshakeMessage& o) const;
};

// ── ReconnectBackoff ──────────────────────────────────────────────────────────
//
// Exponential backoff with jitter for reconnect attempts.
// Formula: base_delay = min(initial_delay * 2^attempt, max_delay)
//          actual_delay = base_delay + uniform_random(-jitter_range, +jitter_range)
//          where jitter_range = base_delay * jitter_fraction

struct ReconnectBackoff {
    uint32_t attempt{0};

    static constexpr double initial_delay_s  = 1.0;
    static constexpr double max_delay_s      = 30.0;
    static constexpr double jitter_fraction  = 0.25;
    static constexpr double multiplier       = 2.0;

    /// Calculate next delay in milliseconds (includes jitter).
    /// Increments attempt counter.
    uint32_t next_delay_ms();

    /// Reset after successful connection.
    void reset() { attempt = 0; }
};

// ── Frame encode/decode ────────────────────────────────────────────────────────
//
// Length-prefixed framing: 4-byte LE uint32 length header + payload bytes.
// encode_frame appends a complete frame to the output vector.
// parse_frames scans recv_buf for complete frames, returns payload offsets,
// and erases consumed bytes from recv_buf.

/// Encode a single frame: appends [4B LE length | payload] to `out`.
void encode_frame(const void* payload, size_t len, std::vector<uint8_t>& out);

/// Parse complete frames from recv_buf.
/// On success, fills frames_out with (offset, length) pairs pointing to payload
/// positions within recv_buf BEFORE erasure, then erases consumed bytes.
/// Returns 0 on success, -1 on protocol error (frame length > MM_MAX_FRAME_PAYLOAD).
int parse_frames(std::vector<uint8_t>& recv_buf,
                 std::vector<std::pair<size_t, size_t>>& frames_out);

class Engine;  // forward — full integration comes in task 12

// ── Multi-master configuration ────────────────────────────────────────────────

struct MultiMasterConfig {
    uint16_t    node_id{0};                       // --mm-node-id (required)
    uint16_t    replication_port{0};              // --mm-replication-port
    bool        enabled{false};                   // --multi-master
    bool        compress{false};                  // --replication-compress
    size_t      max_catchup_bytes{512ULL << 20};  // --mm-max-catchup-bytes (512MB)
    uint32_t    anti_entropy_interval_sec{30};    // --anti-entropy-interval-seconds
    /// Queued output one peer may hold before the connection is dropped
    /// (--mm-max-peer-send-buffer). Same ceiling a client session gets since #59.
    size_t      max_peer_send_buf_bytes{MM_MAX_PEER_SEND_BUF};
    /// Stop adding snapshot chunks while a peer's send buffer still holds this much.
    size_t      snapshot_low_watermark_bytes{MM_SNAPSHOT_LOW_WATERMARK};
    std::string shard_id;                         // optional, if sharding active
    CoordinatorConfig coordinator_config;         // etcd endpoints for peer discovery

    /// Cluster secret (#30 part two). Empty = this mesh does not authenticate.
    ///
    /// Same secret as the replication link, which is why the surface label is inside the HMAC
    /// input: without it a response captured on one of the two links would authenticate on the
    /// other.
    SecretStore cluster_secret;

    // ── TLS on the mesh (#30 part three, series D) ────────────────────────────
    //
    // Two contexts because the mesh is symmetric: this node accepts connections and opens them, and
    // the two ends of a node link verify different things. Which one a connection uses is decided
    // by `PeerConnection::we_accepted` - the same field part one introduced as the reflection
    // defence, which is not a coincidence: it is the only thing that tells the two ends apart.
    //
    // Null = plaintext. Both are set or neither is; `load_tls_or_exit()` refuses the halves.
    std::shared_ptr<TlsContext> tls_server;
    std::shared_ptr<TlsContext> tls_client;
};

// ── Peer connection state ─────────────────────────────────────────────────────

struct PeerConnection {
    uint16_t     node_id{0};
    std::string  address;            // host:port
    int          fd{-1};             // socket fd (-1 = disconnected)
    /// Identifies this *connection*, not this peer: assigned from a counter when the socket is
    /// established, and never reused.
    ///
    /// Anything that outlives a single connection needs it, and snapshot creation is the first such
    /// thing — a request arrives, a worker spends milliseconds to seconds on it, and by the time the
    /// result lands the peer may have dropped and come back (#79). Neither of the obvious keys
    /// works there: `node_id` is the same node returning, and descriptor numbers are reused by the
    /// kernel. Zero means no connection has been established on this record.
    uint64_t     conn_id{0};
    bool         connected{false};
    bool         handshake_done{false};  // handshake completed

    /// True once this peer has answered our challenge (#30 part two).
    ///
    /// One flag rather than two. This side sends no handshake until the peer has proved itself, and
    /// the peer applies the same rule, so mutual authentication falls out of the symmetry and
    /// neither side needs to track its own proof.
    bool         peer_proved{false};
    /// The nonce we challenged this connection with. Single-use: cleared when answered.
    std::string  auth_nonce;

    /// TLS state for this connection (#30 part three, series D). Null = plaintext.
    ///
    /// Reset on every disconnect, because it belongs to the connection: left in place it would be
    /// handed to the next socket and decrypt with the previous session's keys, which reads as
    /// corruption rather than as a lifetime mistake.
    std::shared_ptr<TlsChannel> tls;

    /// Who the peer's certificate says it is, once the handshake completes. Empty otherwise.
    ///
    /// The field requirement 8.4 of part one asked for. A node's identity used to be its `node_id`
    /// from the handshake, which authentication precedes - so mTLS is the first thing on this link
    /// with a name of its own. Read by the log line now and by the ACLs of #31 later, from here.
    std::string  identity;
    /// True when *we* accepted this socket, false when we opened it.
    ///
    /// The mesh is symmetric, so this is the only thing that tells the two ends apart - and telling
    /// them apart is the reflection defence (see AuthRole). We answer as this role and verify the
    /// peer as the other one; without it, both ends compute the same function of a nonce and an
    /// attacker can echo our own challenge back, be handed the answer, and replay it.
    bool         we_accepted{false};
    bool         compress{false};    // LZ4 negotiated
    // Reported by the peer in its handshake, kept for the MM_PEERS view only. Catch-up must
    // not use them: they are positions in the peer's own WAL, and #61 was the consequence of
    // comparing them with ours.
    uint32_t     confirmed_file{0};
    size_t       confirmed_offset{0};

    /// What the peer says it holds. Until it arrives, the peer is assumed to hold nothing.
    PeerVector   peer_vector;
    /// Monotonic milliseconds after which a silent peer is treated as holding nothing.
    uint64_t     vector_deadline_ms{0};
    /// Set once catch-up has been started for this connection, so a late vector does not
    /// start a second stream.
    bool         catchup_started{false};
    HLCTimestamp last_hlc;           // last HLC received from this peer

    // Send buffer (non-blocking)
    std::vector<uint8_t> send_buf;

    // Receive buffer (simple byte buffer for incoming data)
    std::vector<uint8_t> recv_buf;

    // Reconnect state
    ReconnectBackoff backoff;
    std::chrono::steady_clock::time_point next_reconnect_time{};

    // Catch-up state
    bool catching_up{false};
    bool needs_snapshot{false};
};

// ── Snapshot transfer state ───────────────────────────────────────────────────

/// What BEGIN announces: the shape of the metadata blob that the next chunks carry.
struct SnapshotBegin {
    uint32_t manifest_len{0};
    uint32_t vector_len{0};
    uint32_t held_len{0};
    uint32_t meta_crc{0};

    size_t total() const {
        return static_cast<size_t>(manifest_len) + vector_len + held_len;
    }
};


/// Sending side: one at a time per node, streamed as the peer's socket drains.
struct MMSnapshotSend {
    bool             active{false};
    uint16_t         target_node_id{0};
    SnapshotManifest manifest;
    std::vector<uint8_t> meta;         // manifest ++ vector ++ held, sent before the files
    size_t           meta_offset{0};
    size_t           file_idx{0};
    size_t           file_offset{0};
    int              fd{-1};
    uint64_t         bytes_sent{0};
    std::chrono::steady_clock::time_point started_at{};
};

/// Sending side, before there is anything to send: a snapshot being created on a worker thread for
/// a peer that asked for one (#79).
///
/// Separate from MMSnapshotSend because the two cannot overlap and mean different things: this one
/// has no manifest, no bytes and no descriptor yet, and its target may disappear before it has any.
struct MMSnapshotPrepare {
    bool     active{false};
    uint16_t target_node_id{0};
    /// Which connection asked. The node returning on a new connection has asked for nothing.
    uint64_t target_conn_id{0};
    uint64_t token{0};
    std::chrono::steady_clock::time_point started_at{};
};

/// Receiving side: staged to a scratch directory, installed only once every byte has checked out.
struct MMSnapshotRecv {
    /// Metadata arrives first, through the same chunk mechanism as the files.
    enum class Phase { META, FILES };

    bool             active{false};
    Phase            phase{Phase::META};
    uint16_t         source_node_id{0};
    SnapshotBegin    announced{};
    std::vector<uint8_t> meta;         // assembled manifest ++ vector ++ held
    SnapshotManifest manifest;
    std::vector<SequenceTracker::VectorEntry> vector;
    std::vector<SequenceTracker::HeldRanges>  held;
    std::string      staging_dir;
    size_t           file_idx{0};      // the file the next chunk must belong to
    size_t           file_offset{0};   // the offset the next chunk must start at
    int              fd{-1};
    uint32_t         running_crc{0};
    uint64_t         bytes_received{0};
    std::chrono::steady_clock::time_point started_at{};
};

// ── Snapshot payload codecs ───────────────────────────────────────────────────
//
// Free functions rather than private methods, so the wire format can be tested without a peer,
// a socket or an engine. Every decoder returns false on a payload that is short, inconsistent or
// out of range, and leaves its output untouched.

std::vector<uint8_t> encode_snapshot_begin(const SnapshotBegin& begin);

/// False on a payload of the wrong length, or one announcing more metadata than we will hold.
bool decode_snapshot_begin(const uint8_t* data, size_t len, SnapshotBegin& out);

std::vector<uint8_t> encode_snapshot_chunk(uint16_t file_index, uint64_t byte_offset,
                                           const uint8_t* bytes, size_t n);

bool decode_snapshot_chunk(const uint8_t* data, size_t len,
                           uint16_t& file_index, uint64_t& byte_offset,
                           const uint8_t*& bytes, size_t& n);

/// END carries nothing. Every byte is already covered by a per-file CRC from the manifest and by
/// the metadata CRC from BEGIN; a third checksum could only fail where one of those already has.
std::vector<uint8_t> encode_snapshot_end();
bool decode_snapshot_end(const uint8_t* data, size_t len);

std::vector<uint8_t> encode_snapshot_abort(std::string_view reason);
std::string          decode_snapshot_abort(const uint8_t* data, size_t len);

// ── MultiMasterManager ────────────────────────────────────────────────────────

class MultiMasterManager {
public:
    explicit MultiMasterManager(MultiMasterConfig config, Engine& engine,
                                WALWriter& wal, HybridLogicalClock& hlc);
    ~MultiMasterManager();

    MultiMasterManager(const MultiMasterManager&) = delete;
    MultiMasterManager& operator=(const MultiMasterManager&) = delete;

    /// Start: bind replication port, register in etcd, connect to peers.
    void start();

    /// Stop: disconnect peers, deregister from etcd.
    void stop();

    /// Broadcast a locally-originated WAL record to all connected peers.
    /// Called by Engine after apply_delta + WAL append.
    /// NOTE: does NOT broadcast records received from replication (origin != local).
    void broadcast_local(const WALRecordV2& hdr, const void* payload,
                         size_t payload_len);

    /// Handle an incoming WAL record from a peer.
    /// Returns true if the record was applied (not a duplicate/loop).
    bool handle_remote_record(uint16_t peer_node_id,
                              const WALRecordV2& hdr,
                              const void* payload, size_t payload_len);

    /// Get peer connection states (for STATUS/MM_PEERS commands).
    std::vector<PeerConnection> peer_states() const;

    /// Get the number of connected peers.
    size_t connected_peer_count() const;

    /// Get conflict resolver (for MM_CONFLICTS command).
    const ConflictResolver& conflict_resolver() const { return *conflict_resolver_; }

    /// One reconciliation pass: tell every connected peer what we hold, and report the
    /// difference in both directions.
    ///
    /// Sending our vector *is* the repair: a peer receiving it streams what we lack, because
    /// that is the same path catch-up takes. Nothing here needs a protocol of its own.
    ReconcileReport reconcile_with_peers();

    /// Get the anti-entropy manager, or nullptr when there is none.
    ///
    /// A pointer, not a reference: this component is optional — it needs a peer registry, so a
    /// node without coordinator endpoints has none — and handing out a reference to something
    /// optional is what let Engine::stats() dereference a null unique_ptr and kill every
    /// multi-master node that was asked for STATUS (roadmap #68).
    AntiEntropyManager* anti_entropy() { return anti_entropy_.get(); }

    /// Check if this manager is in bootstrap state.
    bool is_bootstrapping() const { return bootstrapping_.load(std::memory_order_acquire); }

    // ── Snapshot bootstrap protocol (#76) ─────────────────────────────────────
    //
    // Public because every interesting case here is a refusal — an unsafe path in a manifest, a
    // chunk at the wrong offset, a checksum that does not match — and a refusal reached only
    // through a live socket is a refusal nobody tests. `PeerConnection` is already public for
    // the same reason.

    /// A peer asked us for a snapshot. Creates one and starts streaming, or aborts with a reason.
    void handle_snapshot_request(PeerConnection& peer);

    /// Start sending a snapshot that a worker thread has finished creating.
    ///
    /// Every refusal that used to happen inside handle_snapshot_request() lives here now, because
    /// each one needs the created snapshot to decide: an untransportable version vector, a manifest
    /// too large to address with a 16-bit index, metadata beyond what a receiver will assemble.
    void begin_snapshot_send(PeerConnection& peer, SnapshotWithSequenceState&& snap);

    /// Collect a finished snapshot, if there is one, and act on it. Called from io_loop() once per
    /// pass — after a wake-up from wakeup_fd_, and also after a plain epoll timeout.
    void poll_snapshot_preparation();

    /// Write to wakeup_fd_ so io_loop() returns from epoll_wait() now rather than in up to 500 ms.
    ///
    /// Safe to call from any thread while the io loop is alive. The descriptor is only closed after
    /// that loop and the snapshot worker have both been joined — the ordering pitfall 41 and #80
    /// were both about.
    void wake_io_loop();

    /// A peer is about to send us one. Validates the manifest whole, then opens staging.
    void handle_snapshot_begin(PeerConnection& peer, const uint8_t* payload, size_t len);

    /// One chunk of one file. Must be the expected file at the expected offset.
    void handle_snapshot_chunk(PeerConnection& peer, const uint8_t* payload, size_t len);

    /// Every file has arrived. Verifies, installs, adopts the sequence state.
    void handle_snapshot_end(PeerConnection& peer, const uint8_t* payload, size_t len);

    /// Give up on the inbound snapshot: staging removed, data directory untouched, flag cleared.
    void abort_bootstrap(const char* reason);

    /// Ask a peer for a snapshot. Refuses unless this node holds nothing at all.
    bool request_snapshot_from(PeerConnection& peer);

    /// Push chunks while the peer's send buffer has room.
    ///
    /// Called after BEGIN and from the EPOLLOUT branch of `io_loop()`, which is what keeps a
    /// snapshot from being enqueued in one piece ahead of live traffic.
    void advance_snapshot_send(PeerConnection& peer);

    /// Cancel whatever transfer this peer was part of. Called from every disconnect path.
    void on_peer_disconnected(PeerConnection& peer);

    /// Test seams. Read without the lock, so only meaningful from a test that drives the
    /// protocol handlers directly; production code asks `is_bootstrapping()`, which is atomic.
    bool snapshot_send_active() const { return snapshot_send_.active; }
    bool snapshot_recv_active() const { return snapshot_recv_.active; }
    /// True between accepting a snapshot request and collecting the worker's result.
    bool snapshot_preparing() const { return snapshot_prepare_.active; }
    /// True while a worker is running or its result is still uncollected. Distinct from the above:
    /// a request cancelled by a disconnect clears that flag and leaves this one set, because the
    /// work carries on to the end.
    bool snapshot_builder_busy() const { return snapshot_builder_.busy(); }

    /// Put a connection into the peer table and hand back the stored record.
    ///
    /// Only sensible from a test. Production connections arrive through accept() or
    /// connect_to_peer(), which is where conn_id is assigned — but a test that drives the snapshot
    /// path needs the record to be *in* the table, because that path looks its target up rather than
    /// keeping the reference it was handed: the request and the finished snapshot are separated by a
    /// worker thread, and the peer may be gone by then (#79).
    PeerConnection& install_peer_for_test(PeerConnection peer);

    /// Enter the bootstrap state: this node holds no data yet and must not serve as though it did.
    ///
    /// Always paired with `finish_bootstrap()`. A flag that gates writes and has no way out is a
    /// self-inflicted outage waiting for its first caller — `INSERT`, `MINSERT` and `DELETE` all
    /// answer `ERR BOOTSTRAPPING` while this is set, and before #76 nothing anywhere cleared it
    /// (roadmap #73 is the same shape, found in the failover state machine).
    void start_bootstrap();

    /// Leave the bootstrap state, whether the transfer succeeded or failed.
    ///
    /// `succeeded == false` is not a reason to stay in bootstrap: a node that cannot bootstrap has
    /// to say so and become usable or be restarted, not sit silently refusing writes for ever. The
    /// caller decides what to do about the failure; this only guarantees the state has an exit.
    void finish_bootstrap(bool succeeded);

    /// Connections accepted whose handshake has not yet said which node is behind them.
    ///
    /// Diagnostic and test seam. These are not peers: nothing broadcasts to them, nothing dials
    /// them, and neither MM_PEERS nor the mesh gauges count them (#84, #94) — so the number is not
    /// derivable from anything else the class exposes, and a connection stuck here is invisible
    /// without it.
    size_t pending_connection_count() const;

    /// Handle MM_PEERS command — return TSV response.
    std::string handle_mm_peers_command() const;

    /// Handle MM_CONFLICTS command — return TSV response.
    std::string handle_mm_conflicts_command(size_t limit = 100) const;

    /// Get the config (for testing).
    const MultiMasterConfig& config() const { return config_; }

private:
    MultiMasterConfig config_;
    Engine& engine_;
    WALWriter& wal_;
    HybridLogicalClock& hlc_;

    std::unique_ptr<PeerRegistry> peer_registry_;
    std::unique_ptr<ConflictResolver> conflict_resolver_;
    std::unique_ptr<AntiEntropyManager> anti_entropy_;

    mutable std::mutex mtx_;
    std::unordered_map<uint16_t, PeerConnection> peers_;

    /// Connections this node accepted, before their handshake says which node opened them.
    ///
    /// A container of their own because they have no node id yet, and `peers_` is keyed by one.
    /// They used to be inserted into `peers_` under `static_cast<uint16_t>(fd)`, which that map
    /// cannot tell from a node id: an inbound connection landing on descriptor N replaced the
    /// record of peer N, taking its send buffer, its backoff and its advertised address with the
    /// assignment and leaving its descriptor in the epoll set with nothing behind it (#96).
    /// Measured with a peer record installed for every plausible descriptor number: the connection
    /// arrived on 8 and the record of peer 8 was gone.
    ///
    /// Keyed by `conn_id` rather than by the descriptor. Descriptor numbers are reused by the
    /// kernel and mean something to the epoll set; a `conn_id` is minted once and means nothing to
    /// anyone else, which is the property a key needs.
    ///
    /// Nothing here is broadcast to, dialled, counted or reconciled — which is what removes the
    /// six `node_id == 0` tests that used to stand in for "is this record a real peer?".
    std::unordered_map<uint64_t, PeerConnection> pending_;

    /// Where a descriptor's connection record lives. Requires `mtx_` held.
    struct ConnectionRef {
        PeerConnection* peer{nullptr};
        /// The record's key in `pending_`, or 0 when it is an identified peer. `conn_id` starts at
        /// 1, so zero is unambiguous.
        uint64_t pending_key{0};

        bool is_pending() const { return pending_key != 0; }
    };

    /// Find the connection a descriptor belongs to, in either container.
    ///
    /// A null `peer` means the descriptor belongs to neither, which the io loop treats as an fd it
    /// must close. Which container it came from decides what losing the connection means: an
    /// identified peer keeps its record and takes backoff, an unidentified one is gone for good —
    /// the port it arrived on was the peer's ephemeral source port, so there is nothing to dial
    /// and nothing for it to become (#95).
    ConnectionRef find_connection_by_fd(int fd);

    /// Move a connection from `pending_` into `peers_` under the node id its handshake gave.
    ///
    /// Returns the stored record — the caller's pointer into `pending_` is dangling afterwards and
    /// must be replaced by this one — or null when the connection was dropped instead of adopted,
    /// in which case the caller must touch neither.
    PeerConnection* adopt_identified_connection(uint64_t pending_key);

    /// Install the result of one dial, successful or not. Requires `mtx_` held.
    ///
    /// The other half of dialling with the lock released: by the time this runs the peer may have
    /// left the topology, or may already be connected through a link somebody else opened — its own
    /// second dial, or the peer dialling us and its handshake being adopted (#96). One link per
    /// peer, and the one already carrying traffic keeps it.
    ///
    /// Nothing here schedules a retry: the attempt was claimed, and its backoff taken, before the
    /// lock was released. That is what stops a second dial starting to a peer this one is still
    /// dialling.
    void finish_dial(uint16_t node_id, int fd, const std::string& why);

    /// Close an unidentified connection and forget it. Requires `mtx_` held.
    void drop_pending_connection(uint64_t pending_key, const char* why);

    /// Take the socket out of a connection record: epoll registration, TLS, descriptor, buffers,
    /// and the per-connection protocol state. Leaves the record itself alone — whether it survives
    /// is the caller's decision, and that is the only difference between a peer and a pending
    /// connection going down.
    void close_connection_socket(PeerConnection& conn, const char* why);

    // Networking
    int listen_fd_{-1};
    int epoll_fd_{-1};
    /// eventfd registered in the epoll set, written by stop() to wake io_loop() at once.
    ///
    /// stop() used to close epoll_fd_ "to unblock threads", which does not unblock a thread sitting
    /// in epoll_wait() — Linux does not wake it on close — so shutdown actually waited for the
    /// 500 ms timeout, and meanwhile the loop could call epoll_wait() on a descriptor number the
    /// kernel had already handed to something else. ThreadSanitizer reported it as a race on
    /// file descriptor 4 between stop() and io_loop().
    int wakeup_fd_{-1};

    /// Creating a snapshot is a flush plus a checksum of the whole store, so it does not belong on
    /// the thread that carries live deltas (#79). One at a time; the notification does nothing but
    /// wake io_loop().
    AsyncSnapshotBuilder snapshot_builder_;
    MMSnapshotPrepare    snapshot_prepare_;
    uint64_t             next_snapshot_token_{1};

    /// Source of PeerConnection::conn_id. Read and bumped under mtx_, like peers_ itself.
    uint64_t next_conn_id_{1};
    std::thread io_thread_;
    std::thread reconnect_thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> bootstrapping_{false};

    // ── Snapshot transfer (#76) ───────────────────────────────────────────────
    // One outbound and one inbound at a time. Guarded by mtx_ like the rest of the peer state:
    // every touch happens on the io_loop thread or under the lock a diagnostic command takes.
    MMSnapshotSend snapshot_send_;
    MMSnapshotRecv snapshot_recv_;

    /// End the outbound transfer, successfully or not: closes the file, clears the state.
    void finish_snapshot_send(bool succeeded, const char* reason);

    /// Send an abort frame and log it. Does not touch inbound state.
    void send_snapshot_abort(PeerConnection& peer, const char* reason);

    /// Move the staged files into the data directory. Returns false if any rename failed.
    bool install_snapshot_files();

    // Internal methods
    void io_loop();
    void connect_to_peer(const PeerInfo& peer);
    void disconnect_peer(uint16_t node_id);
    void handle_peer_data(uint16_t node_id);
    void send_to_peer(PeerConnection& peer, const WALRecordV2& hdr,
                      const void* payload, size_t payload_len);
    void handle_catchup_request(PeerConnection& peer, uint32_t from_file,
                                size_t from_offset);
    void handle_topology_change(const std::vector<PeerInfo>& new_peers);

    // Frame-based send methods (task 5.1)
    /// Encode payload into a Frame and append to peer.send_buf, then try to drain.
    void enqueue_frame(PeerConnection& peer, const void* payload, size_t len);

    /// Attempt to drain peer.send_buf via send(MSG_NOSIGNAL).
    /// Handles partial write (erase sent bytes), EAGAIN (arm EPOLLOUT),
    /// EPIPE/ECONNRESET (disconnect + reconnect).
    /// Returns false if peer was disconnected.
    bool try_drain_send_buf(PeerConnection& peer);

    /// One pointer test, then one of two loops. Separate functions rather than a branch inside the
    /// plaintext one, for the reason series C measured on `Session`: an inlined TLS loop changes the
    /// *plaintext* function's prologue, so the unencrypted path pays for a branch it never takes.
    bool try_drain_send_buf_plain(PeerConnection& peer);
    [[gnu::noinline]] bool try_drain_send_buf_tls(PeerConnection& peer);

    /// Attach a TLS channel to a freshly established connection, choosing the role from
    /// `we_accepted`. False means this connection cannot proceed and the caller must close it.
    ///
    /// Does not drive the handshake. That has one owner - `io_loop()` - so a connection opened from
    /// the reconnect thread is armed here and stepped there, and no two threads ever advance the
    /// same `SSL` state machine.
    bool attach_tls(PeerConnection& peer);

    /// Step this peer's handshake and arm what OpenSSL asked for. False = fatal.
    bool advance_tls_handshake(PeerConnection& peer);

    /// close_notify, then forget the channel. Called from every path that closes a peer socket.
    void release_tls(PeerConnection& peer);

    /// Publish both mesh peer gauges from `peers_`. Requires `mtx_` held.
    ///
    /// `ob_mm_peers_tls_verified` is the readable form of the guarantee (requirement 6.6): a
    /// guarantee whose state cannot be read on a live node is a guarantee on our word. A count and
    /// not a label, because a label fed by a peer is an unbounded label set (pitfall 116) - and
    /// here it would name a peer that is by definition authenticated, which is exactly the value an
    /// operator would then trust.
    ///
    /// **Both** gauges, from one loop, because `ob_mm_peers_connected` was recomputed inline at
    /// three sites - `connect_to_peer()`, `disconnect_peer()` and the reconnect loop - and none of
    /// them is the accept path, so a node that *accepted* a connection never counted it. Measured
    /// on a three-node TLS mesh: `ob_mm_peers_tls_verified` 2 against `ob_mm_peers_connected` 1,
    /// which reads as a peer talking plaintext. The correctness does not come from the call sites
    /// either: this runs once per reconnect-loop tick, so no state change anywhere can leave the
    /// gauges stale for longer than that, whichever of the twenty-odd sites made it.
    void publish_peer_gauges();

    /// Arm EPOLLOUT for a peer's fd (when send_buf is non-empty after EAGAIN).
    void arm_epollout(PeerConnection& peer);

    /// Disarm EPOLLOUT for a peer's fd (when send_buf is fully drained).
    void disarm_epollout(PeerConnection& peer);

    // Frame receive/parse methods (task 6.1)
    /// Process all complete frames in peer.recv_buf.
    /// Parses length-prefixed frames, dispatches to handle_frame, removes consumed bytes.
    void process_recv_buf(PeerConnection& peer);

    /// Handle a single parsed frame payload.
    /// If handshake not done: calls process_handshake.
    /// Otherwise: parses WALRecordV2 header + payload, calls handle_remote_record.
    void handle_frame(PeerConnection& peer, const uint8_t* data, size_t len);

    /// Process incoming handshake from peer.
    void process_handshake(PeerConnection& peer, const uint8_t* data, size_t len);

    /// Send our challenge to a peer whose socket has just been established (#30 part two).
    void send_auth_challenge(PeerConnection& peer);

    /// Handle a frame arriving from a peer that has not yet proved itself.
    ///
    /// Answers a challenge, verifies a response, and disconnects on anything else - including a
    /// bare handshake frame, which means the peer is not running with a cluster secret.
    void handle_auth_frame(PeerConnection& peer, const uint8_t* data, size_t len);

    // Handshake send (task 7.1)
    /// Send our handshake message to a peer (called after connect or accept).
    void send_handshake(PeerConnection& peer);
    /// Drop a peer whose queued output passed the ceiling.
    ///
    /// Disconnecting, not clearing: try_drain_send_buf() erases the sent prefix after a partial
    /// write, so the buffer can begin in the middle of a frame. Clearing it then leaves the peer
    /// waiting for the rest of a frame that will never arrive and reading everything after it as
    /// that frame's tail. A closed connection is the only honest answer, and the reconnect path
    /// already knows how to catch up afterwards.
    bool drop_peer_if_send_buf_too_large(PeerConnection& peer);

    /// Send this node's version vector as a WAL_RECORD_VERSION_VECTOR frame.
    ///
    /// Same envelope as a WAL record on purpose: a node running protocol 1 skips an unknown
    /// record type instead of disconnecting, so a mixed-version cluster degrades to
    /// "send everything" rather than to a broken connection.
    void send_version_vector(PeerConnection& peer);
    /// Start catch-up for peers whose vector never arrived within MM_VV_GRACE_MS.
    void start_overdue_catchups();

    // Reconnect logic (task 10.1)
    /// Mark peer as disconnected, close fd, log INFO. Schedules reconnect.
    void schedule_reconnect(uint16_t node_id);

    /// Reconnect thread loop: periodically attempts to reconnect disconnected peers.
    void reconnect_loop();

    // Catch-up streaming (task 8.1)
    /// Start streaming WAL records from peer's confirmed position to current.
    void start_catchup_to_peer(PeerConnection& peer);

    // Backpressure (task 11.1)
    /// Check if peer's send_buf exceeds max_catchup_bytes threshold.
    /// If so: clear send_buf, set needs_snapshot = true, set catching_up = false.
    void check_backpressure(PeerConnection& peer);
};

} // namespace ob
