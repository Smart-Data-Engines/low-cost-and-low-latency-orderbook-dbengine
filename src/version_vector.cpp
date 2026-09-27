#include "orderbook/version_vector.hpp"

#include "orderbook/logger.hpp"

#include <algorithm>
#include <cstring>

namespace ob {

std::vector<uint8_t> serialize_version_vector(
        const std::vector<SequenceTracker::VectorEntry>& entries, bool truncated) {
    std::vector<uint8_t> out;

    // The byte bound, not the entry count, is what actually limits this: both the WAL record
    // header and the multi-master frame header carry the length in a uint16_t, so a payload above
    // 65535 bytes produces a header that understates it. In the WAL that makes every later record
    // unreadable; on the wire the peer sees payload_len disagree with the frame and disconnects,
    // on every reconnect, for ever. 4096 entries is 172 kB, so MM_MAX_VV_ENTRIES alone never
    // brought this anywhere near safe (#78).
    const size_t would_be = VV_HEADER_SIZE + entries.size() * VV_ENTRY_SIZE;
    const bool too_large_for_a_header = would_be > WAL_MAX_PAYLOAD_LEN;
    if (too_large_for_a_header) {
        OB_LOG_WARN("version_vector",
                    "Vector of %zu entries needs %zu bytes, over the %zu a record header can "
                    "describe — sending the \"send everything\" marker instead",
                    entries.size(), would_be, WAL_MAX_PAYLOAD_LEN);
    }

    if (truncated || too_large_for_a_header || entries.size() >= VV_TRUNCATED) {
        out.resize(VV_HEADER_SIZE);
        const uint16_t marker = VV_TRUNCATED;
        std::memcpy(out.data(), &marker, sizeof(marker));
        return out;
    }

    const uint16_t count = static_cast<uint16_t>(entries.size());
    out.resize(VV_HEADER_SIZE + static_cast<size_t>(count) * VV_ENTRY_SIZE, 0);
    std::memcpy(out.data(), &count, sizeof(count));

    size_t off = VV_HEADER_SIZE;
    for (const auto& e : entries) {
        // char[32] holds "SYMBOL.EXCHANGE": both fields are char[16] in DeltaUpdate, so the
        // joined key is at most 31 characters plus a terminator.
        std::memcpy(out.data() + off, e.key.data(), std::min<size_t>(e.key.size(), 31));
        off += 32;
        std::memcpy(out.data() + off, &e.origin, sizeof(e.origin));
        off += sizeof(e.origin);
        std::memcpy(out.data() + off, &e.frontier, sizeof(e.frontier));
        off += sizeof(e.frontier);
    }
    return out;
}

namespace {

/// `count` entries of 42 bytes each, from `data`, which the caller has checked holds them.
void write_entries(const std::vector<SequenceTracker::VectorEntry>& entries, size_t first,
                   size_t count, uint8_t* out) {
    size_t off = 0;
    for (size_t i = first; i < first + count; ++i) {
        const auto& e = entries[i];
        std::memcpy(out + off, e.key.data(), std::min<size_t>(e.key.size(), 31));
        off += 32;
        std::memcpy(out + off, &e.origin, sizeof(e.origin));
        off += sizeof(e.origin);
        std::memcpy(out + off, &e.frontier, sizeof(e.frontier));
        off += sizeof(e.frontier);
    }
}

void read_entries(const uint8_t* data, size_t count,
                  std::vector<SequenceTracker::VectorEntry>& out) {
    size_t off = 0;
    for (size_t i = 0; i < count; ++i) {
        const char* key_bytes = reinterpret_cast<const char*>(data + off);
        SequenceTracker::VectorEntry e;
        // A fixed 32-byte field, zero-padded: the key ends at the first NUL.
        e.key.assign(key_bytes, std::find(key_bytes, key_bytes + 32, '\0'));
        off += 32;
        std::memcpy(&e.origin, data + off, sizeof(e.origin));
        off += sizeof(e.origin);
        std::memcpy(&e.frontier, data + off, sizeof(e.frontier));
        off += sizeof(e.frontier);
        out.push_back(std::move(e));
    }
}

}  // namespace

std::vector<VectorRecord> serialize_version_vector_records(
        const std::vector<SequenceTracker::VectorEntry>& entries, bool truncated,
        uint32_t generation) {
    std::vector<VectorRecord> out;
    if (truncated || entries.size() > VV_MAX_ENTRIES) {
        if (!truncated) {
            OB_LOG_WARN("version_vector",
                        "Vector of %zu entries is past the %zu this node states - sending the "
                        "\"send everything\" marker instead", entries.size(), VV_MAX_ENTRIES);
        }
        out.push_back(VectorRecord{WAL_RECORD_VERSION_VECTOR,
                                   serialize_version_vector({}, /*truncated=*/true)});
        return out;
    }
    if (entries.size() <= VV_MAX_SINGLE_ENTRIES) {
        out.push_back(VectorRecord{WAL_RECORD_VERSION_VECTOR,
                                   serialize_version_vector(entries, /*truncated=*/false)});
        return out;
    }
    const size_t parts = (entries.size() + VV_MAX_PART_ENTRIES - 1) / VV_MAX_PART_ENTRIES;
    out.reserve(parts);
    for (size_t part = 0; part < parts; ++part) {
        const size_t first = part * VV_MAX_PART_ENTRIES;
        const size_t count = std::min(VV_MAX_PART_ENTRIES, entries.size() - first);
        VectorRecord r;
        r.record_type = WAL_RECORD_VERSION_VECTOR_PART;
        r.payload.resize(VV_PART_HEADER_SIZE + count * VV_ENTRY_SIZE, 0);
        const uint16_t part16 = static_cast<uint16_t>(part);
        const uint16_t parts16 = static_cast<uint16_t>(parts);
        const uint16_t count16 = static_cast<uint16_t>(count);
        std::memcpy(r.payload.data(), &generation, sizeof(generation));
        std::memcpy(r.payload.data() + 4, &part16, sizeof(part16));
        std::memcpy(r.payload.data() + 6, &parts16, sizeof(parts16));
        std::memcpy(r.payload.data() + 8, &count16, sizeof(count16));
        write_entries(entries, first, count, r.payload.data() + VV_PART_HEADER_SIZE);
        out.push_back(std::move(r));
    }
    OB_LOG_DEBUG("version_vector", "Vector of %zu entries serialised as %zu parts, generation %u",
                 entries.size(), parts, generation);
    return out;
}

VectorAssembler::Step VectorAssembler::add(const uint8_t* data, size_t len) {
    if (data == nullptr || len < VV_PART_HEADER_SIZE) return Step::Malformed;
    uint32_t generation = 0;
    uint16_t part = 0, parts = 0, count = 0;
    std::memcpy(&generation, data, sizeof(generation));
    std::memcpy(&part, data + 4, sizeof(part));
    std::memcpy(&parts, data + 6, sizeof(parts));
    std::memcpy(&count, data + 8, sizeof(count));
    if (parts == 0 || part >= parts || count > VV_MAX_PART_ENTRIES ||
        len != VV_PART_HEADER_SIZE + static_cast<size_t>(count) * VV_ENTRY_SIZE ||
        static_cast<size_t>(parts) * VV_MAX_PART_ENTRIES > VV_MAX_ENTRIES + VV_MAX_PART_ENTRIES) {
        open_ = false;
        building_.clear();
        return Step::Malformed;
    }

    Step step = Step::Incomplete;
    if (part == 0) {
        if (open_) step = Step::Dropped;          // a new vector began before the last one ended
        open_ = true;
        generation_ = generation;
        parts_ = parts;
        next_ = 0;
        building_.clear();
        building_.reserve(static_cast<size_t>(parts) * VV_MAX_PART_ENTRIES);
    } else if (!open_ || generation != generation_ || parts != parts_ || part != next_) {
        open_ = false;
        building_.clear();
        return Step::Dropped;
    }
    read_entries(data + VV_PART_HEADER_SIZE, count, building_);
    ++next_;
    if (next_ == parts_) {
        open_ = false;
        complete_ = std::move(building_);
        building_.clear();
        return Step::Complete;
    }
    return step;
}

std::vector<uint8_t> serialize_held_ranges(
        const std::vector<SequenceTracker::HeldRanges>& entries) {
    if (entries.empty()) return {};

    // Fit inside what a record header can describe (#78). Unlike the version vector, a partial
    // held set is sound: every range that survives prevents a duplicate row, and the ones left
    // out cost only the duplicates they would have prevented. So this drops whole entries from
    // the tail rather than refusing the payload.
    size_t total = HS_HEADER_SIZE;
    size_t fitting = 0;
    for (const auto& e : entries) {
        const size_t entry_bytes = HS_ENTRY_HEADER_SIZE + e.ranges.size() * HS_RANGE_SIZE;
        if (total + entry_bytes > WAL_MAX_PAYLOAD_LEN) break;
        total += entry_bytes;
        ++fitting;
    }
    if (fitting < entries.size()) {
        OB_LOG_WARN("version_vector",
                    "Held ranges do not fit a record header: keeping %zu of %zu entries "
                    "(%zu bytes, limit %zu)",
                    fitting, entries.size(), total, WAL_MAX_PAYLOAD_LEN);
    }
    if (fitting == 0) return {};

    std::vector<uint8_t> out(total, 0);
    const uint16_t count = static_cast<uint16_t>(fitting);
    std::memcpy(out.data(), &count, sizeof(count));

    size_t off = HS_HEADER_SIZE;
    for (size_t idx = 0; idx < fitting; ++idx) {
        const auto& e = entries[idx];
        std::memcpy(out.data() + off, e.key.data(), std::min<size_t>(e.key.size(), 31));
        off += 32;
        std::memcpy(out.data() + off, &e.origin, sizeof(e.origin));
        off += sizeof(e.origin);
        const uint16_t range_count = static_cast<uint16_t>(e.ranges.size());
        std::memcpy(out.data() + off, &range_count, sizeof(range_count));
        off += sizeof(range_count);
        for (const auto& [first, last] : e.ranges) {
            std::memcpy(out.data() + off, &first, sizeof(first));
            off += sizeof(first);
            std::memcpy(out.data() + off, &last, sizeof(last));
            off += sizeof(last);
        }
    }
    return out;
}

bool deserialize_held_ranges(const uint8_t* data, size_t len,
                            std::vector<SequenceTracker::HeldRanges>& out) {
    out.clear();
    if (!data || len < HS_HEADER_SIZE) return false;

    uint16_t count = 0;
    std::memcpy(&count, data, sizeof(count));

    size_t off = HS_HEADER_SIZE;
    out.reserve(count);
    for (uint16_t i = 0; i < count; ++i) {
        if (off + HS_ENTRY_HEADER_SIZE > len) { out.clear(); return false; }

        SequenceTracker::HeldRanges entry;
        const char* key_bytes = reinterpret_cast<const char*>(data + off);
        // The key was written into a fixed 32-byte field and zero-padded, so stop at the first
        // NUL rather than trusting the whole field to be text.
        entry.key.assign(key_bytes, std::find(key_bytes, key_bytes + 32, '\0'));
        off += 32;
        std::memcpy(&entry.origin, data + off, sizeof(entry.origin));
        off += sizeof(entry.origin);
        uint16_t range_count = 0;
        std::memcpy(&range_count, data + off, sizeof(range_count));
        off += sizeof(range_count);

        if (off + static_cast<size_t>(range_count) * HS_RANGE_SIZE > len) { out.clear(); return false; }
        entry.ranges.reserve(range_count);
        for (uint16_t r = 0; r < range_count; ++r) {
            uint64_t first = 0, last = 0;
            std::memcpy(&first, data + off, sizeof(first));
            off += sizeof(first);
            std::memcpy(&last, data + off, sizeof(last));
            off += sizeof(last);
            if (last < first) { out.clear(); return false; }   // not a range
            entry.ranges.emplace_back(first, last);
        }
        out.push_back(std::move(entry));
    }
    return true;
}

bool PeerVector::deserialize(const uint8_t* data, size_t len) {
    if (len < VV_HEADER_SIZE) {
        OB_LOG_WARN("mm", "Version vector payload too short: %zu bytes", len);
        return false;
    }

    uint16_t count = 0;
    std::memcpy(&count, data, sizeof(count));

    received_  = true;
    truncated_ = (count == VV_TRUNCATED);
    entries_.clear();

    if (truncated_) {
        OB_LOG_INFO("mm", "Peer cannot state what it holds — treating as empty (send everything)");
        return true;
    }

    const size_t needed = VV_HEADER_SIZE + static_cast<size_t>(count) * VV_ENTRY_SIZE;
    if (len < needed) {
        // A short payload would silently drop entries, and a dropped entry reads as "the peer
        // has nothing there" — which over-delivers rather than loses, but it is still a
        // protocol error worth refusing.
        OB_LOG_ERROR("mm", "Version vector truncated on the wire: %zu bytes for %u entries",
                     len, count);
        received_ = false;
        return false;
    }

    entries_.reserve(count);
    size_t off = VV_HEADER_SIZE;
    for (uint16_t i = 0; i < count; ++i) {
        char key_buf[33] = {};
        std::memcpy(key_buf, data + off, 32);
        off += 32;
        uint16_t origin = 0;
        std::memcpy(&origin, data + off, sizeof(origin));
        off += sizeof(origin);
        uint64_t frontier = 0;
        std::memcpy(&frontier, data + off, sizeof(frontier));
        off += sizeof(frontier);

        entries_[Key{std::string(key_buf), origin}] = frontier;
    }

    OB_LOG_INFO("mm", "Version vector received: entries=%u", count);
    return true;
}

std::vector<uint8_t> serialize_version_vector_blob(
        const std::vector<SequenceTracker::VectorEntry>& entries) {
    if (entries.size() <= VV_MAX_SINGLE_ENTRIES) {
        return serialize_version_vector(entries, /*truncated=*/false);
    }
    const auto records = serialize_version_vector_records(entries, /*truncated=*/false, 1);
    std::vector<uint8_t> out(sizeof(uint16_t) + sizeof(uint32_t));
    const uint16_t marker = VV_PARTS_BLOB;
    const uint32_t parts = static_cast<uint32_t>(records.size());
    std::memcpy(out.data(), &marker, sizeof(marker));
    std::memcpy(out.data() + sizeof(marker), &parts, sizeof(parts));
    for (const auto& r : records) {
        const uint32_t len = static_cast<uint32_t>(r.payload.size());
        const size_t at = out.size();
        out.resize(at + sizeof(len) + r.payload.size());
        std::memcpy(out.data() + at, &len, sizeof(len));
        std::memcpy(out.data() + at + sizeof(len), r.payload.data(), r.payload.size());
    }
    return out;
}

bool deserialize_version_vector_blob(const uint8_t* data, size_t len,
                                     std::vector<SequenceTracker::VectorEntry>& out,
                                     bool& says_send_everything) {
    out.clear();
    says_send_everything = false;
    if (data == nullptr || len < VV_HEADER_SIZE) return false;
    uint16_t count = 0;
    std::memcpy(&count, data, sizeof(count));
    if (count != VV_PARTS_BLOB) {
        PeerVector one;
        if (!one.deserialize(data, len)) return false;
        says_send_everything = one.truncated();
        out = one.entries();
        return true;
    }
    size_t off = sizeof(uint16_t);
    if (len - off < sizeof(uint32_t)) return false;
    uint32_t parts = 0;
    std::memcpy(&parts, data + off, sizeof(parts));
    off += sizeof(parts);
    VectorAssembler assembler;
    for (uint32_t i = 0; i < parts; ++i) {
        if (len - off < sizeof(uint32_t)) return false;
        uint32_t part_len = 0;
        std::memcpy(&part_len, data + off, sizeof(part_len));
        off += sizeof(part_len);
        if (len - off < part_len) return false;
        const auto step = assembler.add(data + off, part_len);
        off += part_len;
        if (step == VectorAssembler::Step::Complete) {
            if (i + 1 != parts || off != len) return false;   // parts after the last, or bytes
            out = assembler.take();
            return true;
        }
        if (step != VectorAssembler::Step::Incomplete) return false;
    }
    return false;                                              // it never completed
}

bool PeerVector::deserialize_part(const uint8_t* data, size_t len) {
    switch (assembler_.add(data, len)) {
        case VectorAssembler::Step::Incomplete:
            return false;
        case VectorAssembler::Step::Dropped:
            OB_LOG_WARN("mm", "A version vector in parts arrived out of sequence; the one before it "
                              "stands until a whole one arrives");
            return false;
        case VectorAssembler::Step::Malformed:
            OB_LOG_WARN("mm", "Unusable version vector part: %zu bytes; the vector before it stands",
                        len);
            return false;
        case VectorAssembler::Step::Complete:
            break;
    }
    const auto entries = assembler_.take();
    entries_.clear();
    entries_.reserve(entries.size());
    for (const auto& e : entries) entries_[Key{e.key, e.origin}] = e.frontier;
    received_  = true;
    truncated_ = false;
    OB_LOG_INFO("mm", "Version vector received in parts: entries=%zu", entries.size());
    return true;
}

std::vector<SequenceTracker::VectorEntry> PeerVector::entries() const {
    std::vector<SequenceTracker::VectorEntry> out;
    out.reserve(entries_.size());
    for (const auto& [k, frontier] : entries_) {
        out.push_back(SequenceTracker::VectorEntry{k.key, k.origin, frontier});
    }
    return out;
}

uint64_t PeerVector::frontier_for(const std::string& key, uint16_t origin) const {
    auto it = entries_.find(Key{key, origin});
    return it == entries_.end() ? 0 : it->second;
}

VectorDiff compare_vectors(const std::vector<SequenceTracker::VectorEntry>& ours,
                           const PeerVector& theirs, uint16_t peer_node_id) {
    VectorDiff diff{};

    // A peer that could not state its position, or has not stated it yet, is treated as holding
    // nothing: everything we have is something it lacks. Over-stating what it needs costs
    // bandwidth; under-stating it loses data.
    const bool peer_unknown = theirs.wants_everything();

    for (const auto& e : ours) {
        const uint64_t theirs_frontier = peer_unknown ? 0 : theirs.frontier_for(e.key, e.origin);
        if (theirs_frontier < e.frontier) {
            diff.peer_lacks.push_back(VectorGap{peer_node_id, e.key, e.origin,
                                                theirs_frontier + 1, e.frontier});
        }
    }

    if (peer_unknown) {
        // Nothing to learn about our own gaps from a peer that said nothing. Reporting them as
        // zero would be a claim; leaving them out is the truth.
        return diff;
    }

    // The other direction needs the peer's entries, including keys we have never heard of: a
    // symbol only it holds is exactly the gap worth finding. Through an index of ours - a loop over
    // ours for every one of theirs was 25 million comparisons at 5 000 entries (#177).
    std::unordered_map<std::string, std::unordered_map<uint16_t, uint64_t>> ours_index;
    ours_index.reserve(ours.size());
    for (const auto& o : ours) ours_index[o.key][o.origin] = o.frontier;
    for (const auto& e : theirs.entries()) {
        uint64_t ours_frontier = 0;
        if (const auto k = ours_index.find(e.key); k != ours_index.end()) {
            if (const auto o = k->second.find(e.origin); o != k->second.end()) ours_frontier = o->second;
        }
        if (ours_frontier < e.frontier) {
            diff.we_lack.push_back(VectorGap{peer_node_id, e.key, e.origin,
                                             ours_frontier + 1, e.frontier});
        }
    }

    return diff;
}

}  // namespace ob
