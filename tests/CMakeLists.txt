include(GoogleTest)

# Reduce RapidCheck iterations from default 100 → 25 for faster test runs.
set(RC_TEST_ENV "RC_PARAMS=max_success=25")

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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")
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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")
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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")
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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")
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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")
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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")
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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")
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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")
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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")
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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")
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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")
endfunction()

# ── TCP Server tests ──────────────────────────────────────────────────────────
ob_add_tcp_test(test_tcp_server
    test_tcp_server.cpp
)

# ── Replication tests ──────────────────────────────────────────────────────────
ob_add_engine_test(test_replication
    test_replication.cpp
)

# ── Epoch tests (ha-automatic-failover) ───────────────────────────────────────
add_executable(test_epoch test_epoch.cpp)
target_link_libraries(test_epoch
    PRIVATE
        orderbook_wal
        orderbook_epoch
        GTest::gtest_main
        rapidcheck
)
gtest_discover_tests(test_epoch PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")

# ── Wire protocol epoch tests (ha-automatic-failover) ─────────────────────────
add_executable(test_wire_protocol_epoch test_wire_protocol_epoch.cpp)
target_link_libraries(test_wire_protocol_epoch
    PRIVATE
        orderbook_epoch
        GTest::gtest_main
        rapidcheck
)
gtest_discover_tests(test_wire_protocol_epoch PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")

# ── Failover election tests (ha-automatic-failover, Property 9) ───────────────
add_executable(test_failover_election test_failover_election.cpp)
target_link_libraries(test_failover_election
    PRIVATE
        orderbook_failover
        GTest::gtest_main
        rapidcheck
)
gtest_discover_tests(test_failover_election PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")

# ── Failover roles tests (ha-automatic-failover, Properties 10, 15, 16) ──────
add_executable(test_failover_roles test_failover_roles.cpp)
target_link_libraries(test_failover_roles
    PRIVATE
        orderbook_engine
        GTest::gtest_main
        rapidcheck
)
gtest_discover_tests(test_failover_roles PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")

# ── CLI failover tests (ha-automatic-failover, Property 11) ──────────────────
add_executable(test_cli_failover test_cli_failover.cpp)
target_link_libraries(test_cli_failover
    PRIVATE
        orderbook_tcp_server_lib
        GTest::gtest_main
        rapidcheck
)
gtest_discover_tests(test_cli_failover PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")

# ── Failover integration tests (ha-automatic-failover) ────────────────────────
add_executable(test_failover_integration test_failover_integration.cpp)
target_link_libraries(test_failover_integration
    PRIVATE
        orderbook_engine
        GTest::gtest_main
)
gtest_discover_tests(test_failover_integration PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")

# ── Helper to register a test executable that needs the compression library ───
function(ob_add_compression_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name}
        PRIVATE
            orderbook_compression
            GTest::gtest_main
            rapidcheck
            rapidcheck_gtest
    )
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")
endfunction()

# ── Compression property and unit tests (compression-and-ttl) ─────────────────
ob_add_compression_test(test_compression
    test_compression.cpp
)

# ── Replication compression handshake tests (compression-and-ttl, Task 3.5) ───
add_executable(test_replication_compress test_replication_compress.cpp)
target_link_libraries(test_replication_compress
    PRIVATE
        orderbook_engine
        GTest::gtest_main
)
gtest_discover_tests(test_replication_compress PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")

# ── Session compression negotiation tests (compression-and-ttl, Task 4.4) ────
ob_add_tcp_test(test_session_compress
    test_session_compress.cpp
)

# ── Incremental flush tests (incremental-flush, Tasks 6.1–7.3) ────────────────
add_executable(test_incremental_flush test_incremental_flush.cpp)
target_link_libraries(test_incremental_flush
    PRIVATE
        orderbook_engine
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_incremental_flush PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")

# ── TTL retention tests (compression-and-ttl, Tasks 7.2, 8.3–8.5) ────────────
add_executable(test_ttl_retention test_ttl_retention.cpp)
target_link_libraries(test_ttl_retention
    PRIVATE
        orderbook_tcp_server_lib
        orderbook_columnar
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_ttl_retention PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")

# ── Batch insert tests (batch-insert, Properties 1–5 + unit tests) ────────────
add_executable(test_batch_insert test_batch_insert.cpp)
target_link_libraries(test_batch_insert PRIVATE orderbook_tcp_server_lib GTest::gtest_main rapidcheck rapidcheck_gtest)
gtest_discover_tests(test_batch_insert PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")

# ── Stress tests (gated behind OB_STRESS_TESTS env var) ──────────────────────
add_executable(test_stress test_stress.cpp)
target_link_libraries(test_stress PRIVATE orderbook_engine GTest::gtest_main)
# NOT registered with gtest_discover_tests — run manually:
#   OB_STRESS_TESTS=1 ./build/tests/test_stress
