// What a version vector costs to send, to put back together and to compare, as it grows (#177).
//
// Until #177 a vector past 1 560 entries was never sent at all - it was the "send everything" marker -
// so nothing past that size was ever serialised, received or compared. In parts, every size is: a
// node of 50 000 instruments across a few origins sends and compares a vector of that size at every
// reconciliation, on the mesh's io loop. This times the three steps at 1 500, 5 000 and 50 000
// entries, the comparison twice: with a peer that lists the same pairs as this node - a mesh at rest,
// one walk over ours - and with one that also lists a tenth more that this node has never heard of,
// which takes a second walk, over theirs. The loop over ours for each of theirs that walk replaced is
// timed at 1 500 by building this program against the tree before #177, where 1 560 was as large as
// a compared vector got.
//
// Usage: vector_cost [repetitions]     (default 20; prints the median of each, in ms)
#include "orderbook/logger.hpp"
#include "orderbook/version_vector.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

namespace {

std::vector<ob::SequenceTracker::VectorEntry> entries_of(size_t n) {
    std::vector<ob::SequenceTracker::VectorEntry> out;
    out.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        out.push_back({"SYM" + std::to_string(i / 3) + ".EXCHANGE", static_cast<uint16_t>(1 + i % 3),
                       1'000'000 + i});
    }
    return out;
}

template <typename F>
double median_ms(int reps, F&& f) {
    std::vector<double> t;
    t.reserve(static_cast<size_t>(reps));
    for (int i = 0; i < reps; ++i) {
        const auto a = std::chrono::steady_clock::now();
        f();
        const auto b = std::chrono::steady_clock::now();
        t.push_back(std::chrono::duration<double, std::milli>(b - a).count());
    }
    std::sort(t.begin(), t.end());
    return t[t.size() / 2];
}

ob::PeerVector received(const std::vector<ob::VectorRecord>& records) {
    ob::PeerVector pv;
    for (const auto& r : records) {
        if (r.record_type == ob::WAL_RECORD_VERSION_VECTOR_PART) {
            (void)pv.deserialize_part(r.payload.data(), r.payload.size());
        } else {
            (void)pv.deserialize(r.payload.data(), r.payload.size());
        }
    }
    return pv;
}

}  // namespace

int main(int argc, char** argv) {
    const int reps = argc > 1 ? std::atoi(argv[1]) : 20;
    ob::StructuredLogger::instance().set_level(ob::LogLevel::WARN);   // a line per vector received
    std::printf("%10s %8s %14s %14s %14s %20s\n", "entries", "records", "serialise ms", "receive ms",
                "compare ms", "compare, +10% ms");
    for (const size_t n : {size_t{1'500}, size_t{5'000}, size_t{50'000}}) {
        const auto ours = entries_of(n);
        std::vector<ob::VectorRecord> records;
        const double ser = median_ms(reps, [&] {
            records = ob::serialize_version_vector_records(ours, false, 1);
        });
        ob::PeerVector theirs;
        const double rec = median_ms(reps, [&] { theirs = received(records); });

        size_t gaps = 0;
        const double cmp = median_ms(reps, [&] {
            const auto diff = ob::compare_vectors(ours, theirs, 2);
            gaps = diff.we_lack.size() + diff.peer_lacks.size();
        });

        auto more_entries = ours;
        for (size_t i = 0; i < n / 10; ++i) {
            more_entries.push_back({"NEW" + std::to_string(i) + ".EXCHANGE", 4, 7});
        }
        const ob::PeerVector more =
                received(ob::serialize_version_vector_records(more_entries, false, 2));
        size_t more_gaps = 0;
        const double cmp_more = median_ms(reps, [&] {
            more_gaps = ob::compare_vectors(ours, more, 2).we_lack.size();
        });

        std::printf("%10zu %8zu %14.3f %14.3f %14.3f %20.3f%s\n", n, records.size(), ser, rec, cmp,
                    cmp_more,
                    gaps == 0 && more_gaps == n / 10 ? "" : "  (not the vectors this means to time)");
    }
    return 0;
}
