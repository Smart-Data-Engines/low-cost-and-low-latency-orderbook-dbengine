# Benchmarks are built but not run as part of ctest
add_executable(bench_engine bench_engine.cpp)
target_link_libraries(bench_engine
    PRIVATE
        orderbook_engine
        orderbook_agg
        orderbook_core
        benchmark::benchmark_main
)
