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

# ── Helper to register a test executable that needs the sequence tracker ─────
function(ob_add_sequence_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name}
        PRIVATE
            orderbook_sequence
            GTest::gtest_main
            rapidcheck
            rapidcheck_gtest
    )
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")
endfunction()

ob_add_sequence_test(test_sequence_tracker
    test_sequence_tracker.cpp
)

ob_add_sequence_test(test_version_vector
    test_version_vector.cpp
)

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

ob_add_query_test(test_query_agg_execution
    test_query_agg_execution.cpp
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

ob_add_engine_test(test_wal_recovery
    test_wal_recovery.cpp
)

ob_add_engine_test(test_sequence_numbers
    test_sequence_numbers.cpp
)

ob_add_engine_test(test_mm_dedup
    test_mm_dedup.cpp
)

ob_add_engine_test(test_mm_stats
    test_mm_stats.cpp
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

# ── Snapshot path validation (security) ───────────────────────────────────────
# Needs rapidcheck on top of the engine library, which ob_add_engine_test does
# not link.
add_executable(test_snapshot_path_safety test_snapshot_path_safety.cpp)
target_link_libraries(test_snapshot_path_safety
    PRIVATE
        orderbook_engine
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_snapshot_path_safety PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")

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
        rapidcheck_gtest
)
gtest_discover_tests(test_failover_election PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")

# ── Failover roles tests (ha-automatic-failover, Properties 10, 15, 16) ──────
add_executable(test_failover_roles test_failover_roles.cpp)
target_link_libraries(test_failover_roles
    PRIVATE
        orderbook_tcp_server_lib
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

add_executable(test_flush_race test_flush_race.cpp)
target_link_libraries(test_flush_race
    PRIVATE
        orderbook_engine
        GTest::gtest_main
)
gtest_discover_tests(test_flush_race)

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

# ── MetricsRegistry property and unit tests (observability, Tasks 1.3–1.7) ────
add_executable(test_metrics_registry test_metrics_registry.cpp)
target_link_libraries(test_metrics_registry
    PRIVATE
        orderbook_metrics
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_metrics_registry PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")

# ── MetricsServer property and integration tests (observability, Tasks 5.6–5.7) ─
add_executable(test_metrics_server test_metrics_server.cpp)
target_link_libraries(test_metrics_server
    PRIVATE
        orderbook_metrics_server
        orderbook_tcp_server_lib
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_metrics_server PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")

# ── StructuredLogger property and unit tests (observability, Tasks 3.3–3.7) ───
add_executable(test_structured_logger test_structured_logger.cpp)
target_link_libraries(test_structured_logger
    PRIVATE
        orderbook_logger
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_structured_logger PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")

# ── Stress tests (gated behind OB_STRESS_TESTS env var) ──────────────────────
add_executable(test_stress test_stress.cpp)
target_link_libraries(test_stress PRIVATE orderbook_engine GTest::gtest_main)
# NOT registered with gtest_discover_tests — run manually:
#   OB_STRESS_TESTS=1 ./build/tests/test_stress

# ── etcd integration tests (gated behind OB_ETCD_TESTS env var) ──────────────
add_executable(test_etcd_integration test_etcd_integration.cpp)
target_link_libraries(test_etcd_integration
    PRIVATE
        orderbook_engine
        orderbook_shard_coordinator
        orderbook_shard_map
        orderbook_tcp_server_lib
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
# NOT registered with gtest_discover_tests — run manually:
#   OB_ETCD_TESTS=1 ./build/tests/test_etcd_integration
set_target_properties(test_etcd_integration PROPERTIES ENVIRONMENT "RC_PARAMS=max_success=25")

# ── C++ native client tests (cpp-native-client, Properties 1–6 + unit tests) ─
add_executable(test_client test_client.cpp)
target_link_libraries(test_client
    PRIVATE
        orderbook_client
        orderbook_tcp_server_lib
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_client PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")

# ── io_uring property-based and unit tests (io-uring-transport, Task 9) ───────
# ── ShardMap & ConsistentHashRing property and unit tests (symbol-sharding) ───
add_executable(test_shard_map test_shard_map.cpp)
target_link_libraries(test_shard_map
    PRIVATE
        orderbook_shard_map
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_shard_map PROPERTIES ENVIRONMENT "RC_PARAMS=max_success=100")

# ── ShardRouter property and unit tests (symbol-sharding, Task 14) ────────────
add_executable(test_shard_router test_shard_router.cpp)
target_link_libraries(test_shard_router
    PRIVATE
        orderbook_shard_router
        orderbook_shard_map
        orderbook_tcp_server_lib
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_shard_router PROPERTIES ENVIRONMENT "RC_PARAMS=max_success=100")

# ── ShardCoordinator unit tests (symbol-sharding, Task 19) ────────────────────
add_executable(test_shard_coordinator test_shard_coordinator.cpp)
target_link_libraries(test_shard_coordinator
    PRIVATE
        orderbook_shard_coordinator
        orderbook_shard_map
        orderbook_tcp_server_lib
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_shard_coordinator PROPERTIES ENVIRONMENT "RC_PARAMS=max_success=100")

# ── io_uring property-based and unit tests (io-uring-transport, Task 9) ───────
if(OB_USE_IO_URING)
    add_executable(test_io_uring test_io_uring.cpp)
    target_compile_definitions(test_io_uring PRIVATE OB_USE_IO_URING)
    target_link_libraries(test_io_uring
        PRIVATE
            orderbook_iouring_server_lib
            orderbook_tcp_server_lib
            GTest::gtest_main
            rapidcheck
            rapidcheck_gtest
    )
    gtest_discover_tests(test_io_uring PROPERTIES ENVIRONMENT "RC_PARAMS=max_success=100")
endif()

# ── HLC property-based and unit tests (multi-master-replication, Tasks 1.4–1.10) ─
add_executable(test_hlc test_hlc.cpp)
target_link_libraries(test_hlc
    PRIVATE
        orderbook_hlc
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_hlc PROPERTIES ENVIRONMENT "RC_PARAMS=max_success=100")

# ── ConflictResolver property-based and unit tests (multi-master-replication, Tasks 5.4–5.5) ─
add_executable(test_conflict_resolver test_conflict_resolver.cpp)
target_link_libraries(test_conflict_resolver
    PRIVATE
        orderbook_conflict_resolver
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_conflict_resolver PROPERTIES ENVIRONMENT "RC_PARAMS=max_success=100")

# ── PeerRegistry property-based and unit tests (multi-master-replication, Tasks 7.4–7.6) ─
add_executable(test_peer_registry test_peer_registry.cpp)
target_link_libraries(test_peer_registry
    PRIVATE
        orderbook_peer_registry
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_peer_registry PROPERTIES ENVIRONMENT "RC_PARAMS=max_success=100")

# ── WALRecordV2 property-based and unit tests (multi-master-replication, Tasks 3.4–3.5) ─
add_executable(test_wal_v2 test_wal_v2.cpp)
target_link_libraries(test_wal_v2
    PRIVATE
        orderbook_wal
        orderbook_soa
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_wal_v2 PROPERTIES ENVIRONMENT "RC_PARAMS=max_success=100")

# ── MultiMasterManager property-based and unit tests (multi-master-replication, Tasks 10.4–10.6) ─
add_executable(test_multi_master test_multi_master.cpp)
target_link_libraries(test_multi_master
    PRIVATE
        orderbook_multi_master
        orderbook_engine
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_multi_master PROPERTIES ENVIRONMENT "RC_PARAMS=max_success=100")

# ── Wire protocol multi-master tests (multi-master-replication, Tasks 13.5–13.6) ─
add_executable(test_wire_protocol_mm test_wire_protocol_mm.cpp)
target_link_libraries(test_wire_protocol_mm
    PRIVATE
        orderbook_tcp_server_lib
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_wire_protocol_mm PROPERTIES ENVIRONMENT "RC_PARAMS=max_success=100")

# ── ShardRouter multi-master property and unit tests (multi-master-replication, Tasks 19.4–19.5) ─
add_executable(test_shard_router_mm test_shard_router_mm.cpp)
target_link_libraries(test_shard_router_mm
    PRIVATE
        orderbook_shard_router
        orderbook_shard_map
        orderbook_tcp_server_lib
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_shard_router_mm PROPERTIES ENVIRONMENT "RC_PARAMS=max_success=100")

# ── CLI multi-master tests (multi-master-replication, Tasks 15.4–15.5) ─
add_executable(test_cli_mm test_cli_mm.cpp)
target_link_libraries(test_cli_mm
    PRIVATE
        orderbook_tcp_server_lib
        GTest::gtest_main
)
gtest_discover_tests(test_cli_mm PROPERTIES ENVIRONMENT "${RC_TEST_ENV}")

# ── MM Replication Networking property-based tests (mm-replication-networking, Tasks 1.2–1.5) ─
add_executable(test_mm_networking test_mm_networking.cpp)
target_link_libraries(test_mm_networking
    PRIVATE
        orderbook_multi_master
        orderbook_engine
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_mm_networking PROPERTIES ENVIRONMENT "RC_PARAMS=max_success=100")

# ── MM Port Isolation property-based tests (mm-port-isolation, Task 5.1) ──────
add_executable(test_mm_port_isolation test_mm_port_isolation.cpp)
target_link_libraries(test_mm_port_isolation
    PRIVATE
        orderbook_multi_master
        orderbook_engine
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_mm_port_isolation PROPERTIES ENVIRONMENT "RC_PARAMS=max_success=100")

# ── Integration test binary (Python test suite calls this) ────────────────────
add_executable(ob_integration_test integration/ob_integration_test.cpp)
target_link_libraries(ob_integration_test PRIVATE orderbook_client)
# NOT registered with gtest_discover_tests — invoked by Python test_cpp_client.py
