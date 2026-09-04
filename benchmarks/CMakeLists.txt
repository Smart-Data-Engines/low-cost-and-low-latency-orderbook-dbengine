# Benchmarks are built but not run as part of ctest
add_executable(bench_engine bench_engine.cpp)
target_link_libraries(bench_engine
    PRIVATE
        orderbook_engine
        orderbook_agg
        orderbook_core
        benchmark::benchmark_main
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
