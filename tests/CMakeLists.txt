include(GoogleTest)

# ── Helper to register a test executable ──────────────────────────────────────
function(ob_add_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name}
        PRIVATE
            orderbook_core
            GTest::gtest_main
            rapidcheck
            rapidcheck_gtest
    )
    gtest_discover_tests(${name})
endfunction()

# ── Helper to register a test executable that needs the soa library ───────────
function(ob_add_soa_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name}
        PRIVATE
            orderbook_soa
            GTest::gtest_main
            rapidcheck
            rapidcheck_gtest
    )
    gtest_discover_tests(${name})
endfunction()

# ── Helper to register a test executable that needs the WAL library ──────────
function(ob_add_wal_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name}
        PRIVATE
            orderbook_wal
            orderbook_soa
            GTest::gtest_main
            rapidcheck
            rapidcheck_gtest
    )
    gtest_discover_tests(${name})
endfunction()

# ── Helper to register a test executable that needs the codec library ─────────
function(ob_add_codec_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name}
        PRIVATE
            orderbook_codec
            GTest::gtest_main
            rapidcheck
            rapidcheck_gtest
    )
    gtest_discover_tests(${name})
endfunction()

# ── Task 1 smoke test ─────────────────────────────────────────────────────────
ob_add_test(test_types_and_data_model
    test_types_and_data_model.cpp
)

# ── Tasks 2.3–2.8: SoA buffer property and unit tests ─────────────────────────
ob_add_soa_test(test_soa_buffer
    test_soa_buffer.cpp
)

# ── Tasks 3.2–3.5: WAL property and unit tests ────────────────────────────────
ob_add_wal_test(test_wal
    test_wal.cpp
)

# ── Tasks 5.3–5.6: Codec property and unit tests ──────────────────────────────
ob_add_codec_test(test_codec
    test_codec.cpp
)

# ── Helper to register a test executable that needs the mmap library ──────────
function(ob_add_mmap_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name}
        PRIVATE
            orderbook_mmap
            GTest::gtest_main
            rapidcheck
            rapidcheck_gtest
    )
    gtest_discover_tests(${name})
endfunction()

# ── Tasks 6.1–6.2: MmapStore unit tests ───────────────────────────────────────
ob_add_mmap_test(test_mmap_store
    test_mmap_store.cpp
)

# ── Helper to register a test executable that needs the columnar library ──────
function(ob_add_columnar_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name}
        PRIVATE
            orderbook_columnar
            GTest::gtest_main
            rapidcheck
            rapidcheck_gtest
    )
    gtest_discover_tests(${name})
endfunction()

# ── Tasks 7.2–7.6: ColumnarStore property and unit tests ──────────────────────
ob_add_columnar_test(test_columnar_store
    test_columnar_store.cpp
)

# ── Helper to register a test executable that needs the aggregation library ───
function(ob_add_agg_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name}
        PRIVATE
            orderbook_agg
            GTest::gtest_main
            rapidcheck
            rapidcheck_gtest
    )
    gtest_discover_tests(${name})
endfunction()

# ── Tasks 9.3–9.4: Aggregation Engine property and unit tests ─────────────────
ob_add_agg_test(test_aggregation
    test_aggregation.cpp
)

# ── Helper to register a test executable that needs the query library ─────────
function(ob_add_query_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name}
        PRIVATE
            orderbook_query
            GTest::gtest_main
            rapidcheck
            rapidcheck_gtest
    )
    gtest_discover_tests(${name})
endfunction()

# ── Tasks 10.5–10.11: Query Engine property and unit tests ────────────────────
ob_add_query_test(test_query_engine
    test_query_engine.cpp
)

# ── Helper to register a test executable that needs the C API library ─────────
function(ob_add_capi_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name}
        PRIVATE
            orderbook_capi
            GTest::gtest_main
    )
    gtest_discover_tests(${name})
endfunction()

# ── Task 12.2: C API unit tests ───────────────────────────────────────────────
ob_add_capi_test(test_c_api
    test_c_api.cpp
)

# ── Helper to register a test executable that needs the engine library ────────
function(ob_add_engine_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name}
        PRIVATE
            orderbook_engine
            GTest::gtest_main
    )
    gtest_discover_tests(${name})
endfunction()

# ── Task 13.2: Engine integration tests ───────────────────────────────────────
ob_add_engine_test(test_engine
    test_engine.cpp
)

# ── Helper to register a test executable that needs the TCP server library ────
function(ob_add_tcp_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name}
        PRIVATE
            orderbook_tcp_server_lib
            GTest::gtest_main
            rapidcheck
            rapidcheck_gtest
    )
    gtest_discover_tests(${name})
endfunction()

# ── TCP Server tests ──────────────────────────────────────────────────────────
ob_add_tcp_test(test_tcp_server
    test_tcp_server.cpp
)

# ── Replication tests ──────────────────────────────────────────────────────────
ob_add_engine_test(test_replication
    test_replication.cpp
)
