# Benchmarks are built but not run as part of ctest
add_executable(bench_engine bench_engine.cpp)
target_link_libraries(bench_engine
    PRIVATE
        orderbook_engine
        orderbook_agg
        orderbook_core
        # For BM_FormatQueryResponse. The response is 40% of the server's profile while answering
        # the comparative benchmark's query, so it is measured here rather than only inferred from
        # a profile.
        orderbook_tcp_server_lib
        # benchmark, not benchmark_main: bench_engine.cpp has its own main, so that the filesystem
        # the engine's storage lands on reaches --benchmark_out through AddCustomContext.
        benchmark::benchmark
)

# ── TLS cost probes (#30 part three) ─────────────────────────────────────────
#
# Compiled, deliberately not run. They need a certificate and take a minute each, so ctest is the
# wrong place — but two source files that nothing compiles are two source files that rot, and this
# repository has the scars: a check whose scope can shrink silently is not a check. Building them on
# every configure is what keeps the number in benchmarks/tls/README.md re-measurable.
#
# C rather than C++ because they are about what OpenSSL and the kernel do with a socket, and
# nothing in them touches the engine.
# The top-level project() declares CXX only, and these two are C - which is what they should be,
# since they exercise OpenSSL and a socket rather than anything of ours.
enable_language(C)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

add_executable(ktls_probe tls/ktls_probe.c)
target_link_libraries(ktls_probe PRIVATE OpenSSL::SSL OpenSSL::Crypto Threads::Threads)

add_executable(tls_cost tls/tls_cost.c)
target_link_libraries(tls_cost PRIVATE OpenSSL::SSL OpenSSL::Crypto Threads::Threads)

add_executable(ssl_write_retry tls/ssl_write_retry.c)
target_link_libraries(ssl_write_retry PRIVATE OpenSSL::SSL OpenSSL::Crypto Threads::Threads)

# ── The wire, without Python in it (benchmarks/on-a-bigger-machine.md) ───────
#
# Compiled, deliberately not run, for the same reason as the probes above: it needs a running node,
# and a source file nothing compiles is a source file that rots. It is what measured that the round
# trip costs about 4% of throughput rather than all of it, so the claim it overturned is now
# checkable with a command instead of a reimplementation.
add_executable(wire_load wire_load.cpp)
target_link_libraries(wire_load PRIVATE orderbook_client)

# ── What one command costs against the round trip it travels on (#145) ───────
#
# Compiled, deliberately not run, for the reason above. scripts/measure_book_latency.py drives it
# and reports `BOOK` against a `PING` baseline in the same round, which is what makes the table in
# the roadmap re-measurable with a command.
add_executable(command_latency command_latency.cpp)

# ── Pipelined ingest, K connections, the server's CPU per thread (#146) ──────
#
# Compiled, deliberately not run, for the reason above. scripts/measure_pipelined_ingest.py drives
# it against fresh nodes in rotated rounds and counts sends and reads per batch from the kernel's
# tracepoints, which is how the table in the roadmap was measured.
add_executable(pipelined_ingest pipelined_ingest.cpp)
target_link_libraries(pipelined_ingest PRIVATE Threads::Threads)

# ── What the segment index costs a tick and a query as it grows (#165) ───────
#
# Compiled, deliberately not run: it prints a table rather than asserting one, and the numbers in
# the roadmap came from it on the m9g.xlarge.
add_executable(segment_index_cost segment_index_cost.cpp)
target_link_libraries(segment_index_cost PRIVATE orderbook_columnar)
