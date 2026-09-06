include(GoogleTest)

# Reduce RapidCheck iterations from default 100 → 25 for faster test runs.
set(RC_TEST_ENV "RC_PARAMS=max_success=25")

# ── Per-test timeout ──────────────────────────────────────────────────────────
#
# CTest's default is 1500 seconds, so a test that hangs holds a CI job for twenty-five minutes and
# then fails - which reads as a stuck runner rather than as a defect. Five minutes is far above any
# honest test here (the whole suite is about two minutes, and five to ten times that under a
# sanitizer) and far below the point where a human gives up on the job.
#
# It is also what makes one regression test work: the concurrent-`stop()` test for #88 **hangs**
# under the defect it guards rather than aborting, because the old code left two threads joining one
# thread object. Without a timeout that test detects the defect and reports nothing.
# Scaled where instrumentation is on, because the same suite runs under ASan and TSan as required
# checks and a timeout that reddens them would be a self-inflicted outage. Measured on this machine:
# the slowest single test is `LZ4Property.prop_roundtrip` at 25 s uninstrumented, and sanitizers cost
# three to ten times that on the network and property tests - so 300 s is comfortable plain and thin
# under TSan. Tripled rather than guessed at from the plain number.
if(OB_ENABLE_ASAN OR OB_ENABLE_TSAN)
    set(OB_TEST_TIMEOUT_DEFAULT 900)
else()
    set(OB_TEST_TIMEOUT_DEFAULT 300)
endif()
set(OB_TEST_TIMEOUT ${OB_TEST_TIMEOUT_DEFAULT} CACHE STRING "Per-test timeout in seconds")
message(STATUS "Per-test timeout: ${OB_TEST_TIMEOUT}s")

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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})
endfunction()

# ── Helper to register a test executable that needs the TLS library ──────────
function(ob_add_tls_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name}
        PRIVATE
            orderbook_tls
            GTest::gtest_main
            rapidcheck
            rapidcheck_gtest
    )
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})
endfunction()

# ── Helper to register a test executable that needs the auth library ─────────
function(ob_add_auth_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name}
        PRIVATE
            orderbook_auth
            GTest::gtest_main
            rapidcheck
            rapidcheck_gtest
    )
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})
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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})
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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})
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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})
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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})
endfunction()

# ── Task 1 smoke test ─────────────────────────────────────────────────────────
ob_add_test(test_version
    test_version.cpp
)

# The drift guards read `CMakeLists.txt` and `pyproject.toml`, so the tree has to be findable at
# test time — same reason `test_wal_position` and `test_cli_config` get this.
target_compile_definitions(test_version
    PRIVATE OB_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/.."
)

ob_add_test(test_crc32c
    test_crc32c.cpp
)

# ── The snapshot handoff between a worker and an io loop (#79) ────────────────
add_executable(test_async_snapshot test_async_snapshot.cpp)
target_link_libraries(test_async_snapshot
    PRIVATE
        orderbook_async_snapshot
        GTest::gtest_main
)
gtest_discover_tests(test_async_snapshot PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})

# ── What a read of the leader key means (#82): three answers, not one ────────
add_executable(test_coordinator_leader_read test_coordinator_leader_read.cpp)
target_link_libraries(test_coordinator_leader_read
    PRIVATE
        orderbook_coordinator
        GTest::gtest_main
)
gtest_discover_tests(test_coordinator_leader_read PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})

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

# ── WAL position coherence (wal-position-coherence, #85) ──────────────────────
ob_add_wal_test(test_wal_position
    test_wal_position.cpp
)
# One test in here is static: it reads src/*.cpp and refuses any site that composes the WAL position
# from two separate calls. A behavioural test cannot cover that - the window is two instructions and
# the measured rate is one in 150 million reads - so the guard is the source, and the source has to
# be findable at test time.
target_compile_definitions(test_wal_position
    PRIVATE OB_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/.."
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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})
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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})
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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})
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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})
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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})
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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})
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

ob_add_engine_test(test_mm_snapshot
    test_mm_snapshot.cpp
)

ob_add_engine_test(test_mm_stats
    test_mm_stats.cpp
)
target_compile_definitions(test_mm_stats
    PRIVATE OB_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/.."
)

ob_add_engine_test(test_anti_entropy
    test_anti_entropy.cpp
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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})
endfunction()

# ── TCP Server tests ──────────────────────────────────────────────────────────
ob_add_tcp_test(test_cli_args
    test_cli_args.cpp
)

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
gtest_discover_tests(test_snapshot_path_safety PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})

# ── Epoch tests (ha-automatic-failover) ───────────────────────────────────────
add_executable(test_epoch test_epoch.cpp)
target_link_libraries(test_epoch
    PRIVATE
        orderbook_wal
        orderbook_epoch
        GTest::gtest_main
        rapidcheck
)
gtest_discover_tests(test_epoch PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})

# ── Wire protocol epoch tests (ha-automatic-failover) ─────────────────────────
add_executable(test_wire_protocol_epoch test_wire_protocol_epoch.cpp)
target_link_libraries(test_wire_protocol_epoch
    PRIVATE
        orderbook_epoch
        GTest::gtest_main
        rapidcheck
)
gtest_discover_tests(test_wire_protocol_epoch PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})

# ── Failover election tests (ha-automatic-failover, Property 9) ───────────────
add_executable(test_failover_election test_failover_election.cpp)
target_link_libraries(test_failover_election
    PRIVATE
        orderbook_failover
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_failover_election PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})

# ── Failover roles tests (ha-automatic-failover, Properties 10, 15, 16) ──────
add_executable(test_failover_roles test_failover_roles.cpp)
target_link_libraries(test_failover_roles
    PRIVATE
        orderbook_tcp_server_lib
        orderbook_engine
        GTest::gtest_main
        rapidcheck
)
gtest_discover_tests(test_failover_roles PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})

# ── CLI failover tests (ha-automatic-failover, Property 11) ──────────────────
add_executable(test_cli_failover test_cli_failover.cpp)
target_link_libraries(test_cli_failover
    PRIVATE
        orderbook_tcp_server_lib
        GTest::gtest_main
        rapidcheck
)
gtest_discover_tests(test_cli_failover PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})

# ── Failover integration tests (ha-automatic-failover) ────────────────────────
add_executable(test_failover_integration test_failover_integration.cpp)
target_link_libraries(test_failover_integration
    PRIVATE
        orderbook_engine
        GTest::gtest_main
)
gtest_discover_tests(test_failover_integration PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})

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
    gtest_discover_tests(${name} PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})
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
gtest_discover_tests(test_replication_compress PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})

# ── Session compression negotiation tests (compression-and-ttl, Task 4.4) ────
ob_add_tcp_test(test_session_compress
    test_session_compress.cpp
)

# ── Subscription hub tests (streaming-subscriptions, Task group 3) ────────────
ob_add_tcp_test(test_subscription_hub
    test_subscription_hub.cpp
)

# ── Config file tests (config-file, #32) ──────────────────────────────────────
ob_add_tcp_test(test_cli_config
    test_cli_config.cpp
)
# Two tests here read src/tcp_server.cpp and check the flag lists against the parser's own branches.
target_compile_definitions(test_cli_config
    PRIVATE OB_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/.."
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
gtest_discover_tests(test_incremental_flush PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})

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
gtest_discover_tests(test_ttl_retention PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})

# ── Batch insert tests (batch-insert, Properties 1–5 + unit tests) ────────────
add_executable(test_batch_insert test_batch_insert.cpp)
target_link_libraries(test_batch_insert PRIVATE orderbook_tcp_server_lib GTest::gtest_main rapidcheck rapidcheck_gtest)
gtest_discover_tests(test_batch_insert PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})

# ── MetricsRegistry property and unit tests (observability, Tasks 1.3–1.7) ────
add_executable(test_metrics_registry test_metrics_registry.cpp)
target_link_libraries(test_metrics_registry
    PRIVATE
        orderbook_metrics
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_metrics_registry PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})

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
gtest_discover_tests(test_metrics_server PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})

# ── StructuredLogger property and unit tests (observability, Tasks 3.3–3.7) ───
add_executable(test_structured_logger test_structured_logger.cpp)
target_link_libraries(test_structured_logger
    PRIVATE
        orderbook_logger
        GTest::gtest_main
        rapidcheck
        rapidcheck_gtest
)
gtest_discover_tests(test_structured_logger PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})

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
gtest_discover_tests(test_client PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})

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
gtest_discover_tests(test_cli_mm PROPERTIES ENVIRONMENT "${RC_TEST_ENV}" TIMEOUT ${OB_TEST_TIMEOUT})

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

# ── Wire authentication (#30) ─────────────────────────────────────────────────
ob_add_auth_test(test_auth
    test_auth.cpp
)

ob_add_tcp_test(test_auth_gate
    test_auth_gate.cpp
)

ob_add_tcp_test(test_auth_cluster
    test_auth_cluster.cpp
)
# Reads src/tcp_server.cpp to refuse a `default:` label in the gate's classifier — the same reason
# test_cli_config and test_version get this definition.
target_compile_definitions(test_auth_gate
    PRIVATE OB_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/.."
)

# ── Live-buffer lookup race (#91) ─────────────────────────────────────────────
ob_add_engine_test(test_query_buffer_lifetime
    test_query_buffer_lifetime.cpp
)

ob_add_engine_test(test_query_live_buffer_race
    test_query_live_buffer_race.cpp
)
# Reads include/ and src/ to refuse the shape that made the race possible.
target_compile_definitions(test_query_live_buffer_race
    PRIVATE OB_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/.."
)

# ── TLS context (#30 part three) ──────────────────────────────────────────────
ob_add_tls_test(test_tls_context
    test_tls_context.cpp
)
# Reads src/ to hold the io_uring refusal against the parser's own flag list.
target_compile_definitions(test_tls_context
    PRIVATE OB_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/.."
)

# TLS through a real Session, where the socket buffers are ours to shrink - the integration battery
# cannot force a partial SSL_write over loopback.
ob_add_tcp_test(test_tls_session
    test_tls_session.cpp
)

# TLS on the node links: mutual verification, the peer-name allowlist, and the two static tests
# that make the disconnect and socket-I/O shapes impossible rather than fixed once (series D).
ob_add_tls_test(test_tls_node_link
    test_tls_node_link.cpp
)
target_compile_definitions(test_tls_node_link
    PRIVATE OB_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/.."
)

# The C++ client's TLS path against a real handshake, plus the name check that turns chain
# verification into peer verification.
add_executable(test_tls_client test_tls_client.cpp)
target_link_libraries(test_tls_client
    PRIVATE
        orderbook_client
        orderbook_tls
        OpenSSL::SSL
        GTest::gtest_main
)
gtest_discover_tests(test_tls_client PROPERTIES TIMEOUT ${OB_TEST_TIMEOUT})
# Reads include/ and src/ to hold copy_client_access() against ClientConfig's own field list.
target_compile_definitions(test_tls_client
    PRIVATE OB_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/.."
)
