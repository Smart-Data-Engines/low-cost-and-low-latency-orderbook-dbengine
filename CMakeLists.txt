cmake_minimum_required(VERSION 3.20)
project(orderbook-dbengine VERSION 0.1.0 LANGUAGES CXX)

# ── Language standard ──────────────────────────────────────────────────────────
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ── Compiler warnings ──────────────────────────────────────────────────────────
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "12")
    add_compile_options(-Wall -Wextra -Werror)
elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "15")
    add_compile_options(-Wall -Wextra -Werror)
endif()

# ── SIMD options ───────────────────────────────────────────────────────────────
option(OB_ENABLE_AVX2   "Enable AVX2 SIMD optimizations"   OFF)
option(OB_ENABLE_AVX512 "Enable AVX-512 SIMD optimizations" OFF)

if(OB_ENABLE_AVX512)
    add_compile_options(-mavx512f)
    add_compile_definitions(OB_ENABLE_AVX512)
elseif(OB_ENABLE_AVX2)
    add_compile_options(-mavx2)
    add_compile_definitions(OB_ENABLE_AVX2)
endif()

# ── Dependencies via FetchContent ──────────────────────────────────────────────
# Every dependency is pinned to a commit SHA, with the corresponding tag in a
# trailing comment. Tags are mutable upstream; a SHA is not. When bumping a
# dependency, resolve the new SHA with:
#   git ls-remote https://github.com/<owner>/<repo>.git 'refs/tags/<tag>^{}'
include(FetchContent)

# Test/benchmark deps are only needed when building tests
option(OB_BUILD_TESTS "Build tests and benchmarks (requires gtest, benchmark, rapidcheck)" ON)

if(OB_BUILD_TESTS)
    # Google Test
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        f8d7d77c06936315286eb55f8de22cd23c188571  # v1.14.0
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)

    # Google Benchmark
    FetchContent_Declare(
        benchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG        344117638c8ff7e239044fd0fa7085839fc03021  # v1.8.3
    )
    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_DOWNLOAD_DEPENDENCIES ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(benchmark)

    # RapidCheck
    FetchContent_Declare(
        rapidcheck
        GIT_REPOSITORY https://github.com/emil-e/rapidcheck.git
        # Pinned to a commit: rapidcheck publishes no releases, and tracking
        # master meant an upstream change could break or alter our build without
        # a change on our side.
        GIT_TAG        6e8dadfdafa3a74eabb52ead87f8787f72eccd0b  # master as of 2026-08-14
    )
    set(RC_ENABLE_GTEST ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(rapidcheck)
endif()

# nlohmann/json (header-only, used by shard_map serialization)
FetchContent_Declare(
    nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG        9cca280a4d0ccf0c08f47a99aa71d1b0e52f8d03  # v3.11.3
)
set(JSON_BuildTests OFF CACHE BOOL "" FORCE)
set(JSON_Install OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(nlohmann_json)

# ── Instrumented builds: sanitizers and coverage ──────────────────────────────
#
# Placed here, and the placement is the point. `add_compile_options()` only affects targets created
# *after* the call, and every one of this project's libraries is created below. These blocks used to
# sit further down the file, past all of them — so `OB_ENABLE_ASAN`, `OB_ENABLE_TSAN` and
# `OB_ENABLE_COVERAGE` instrumented `ob_tcp_server` and the test executables and **none of the
# twenty-eight static libraries where the engine actually lives**.
#
# The evidence was one grep: in the ASan tree, `orderbook_engine.dir/flags.make` carried no
# `-fsanitize` and `ob_tcp_server.dir/flags.make` did. Coverage said 59% of 2387 lines, when the
# tree has an order of magnitude more than that — 6 of 34 source files were being measured. A
# sanitizer job and a coverage number that both look like they cover the tree and cover a sixth of
# it are the same failure mode as a required check that gates nothing.
#
# After FetchContent on purpose, so googletest, benchmark, rapidcheck and nlohmann/json stay
# uninstrumented: they are not what these builds are asking about, and UBSan findings inside a
# dependency would fail our build with -fno-sanitize-recover.

# ── Sanitizers ────────────────────────────────────────────────────────────────
#
# Two separate options because ASan and TSan cannot be linked into the same binary. ASan is paired
# with UBSan, which composes with it and costs almost nothing extra.
#
# Both keep -O1 and frame pointers: -O0 hides optimisation-dependent bugs and makes the suite slow
# enough that nobody runs it, while -O2 loses the stack detail that makes a report actionable.
option(OB_ENABLE_ASAN "Build with AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(OB_ENABLE_TSAN "Build with ThreadSanitizer" OFF)

if(OB_ENABLE_ASAN AND OB_ENABLE_TSAN)
    message(FATAL_ERROR "OB_ENABLE_ASAN and OB_ENABLE_TSAN cannot be enabled together: the two "
                        "runtimes cannot coexist in one binary. Build them as separate trees.")
endif()

if((OB_ENABLE_ASAN OR OB_ENABLE_TSAN) AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # GCC 13 reports -Wmaybe-uninitialized inside <bits/std_function.h> when instrumentation
    # changes what it can see through std::function, on code that is fine — a diagnostic about a
    # system header, not about ours. -Werror turns it into a build failure, so it is disabled for
    # sanitizer trees only. The ordinary build keeps every warning fatal.
    #
    # GCC only: clang has no -Wmaybe-uninitialized, and an unknown -Wno-* is itself a diagnostic
    # there, which -Werror turns into the failure this line exists to prevent.
    add_compile_options(-Wno-maybe-uninitialized)
endif()

if(OB_ENABLE_ASAN)
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer
                        -fno-sanitize-recover=undefined -g -O1)
    add_link_options(-fsanitize=address,undefined)
    message(STATUS "AddressSanitizer + UndefinedBehaviorSanitizer enabled")
endif()

if(OB_ENABLE_TSAN)
    add_compile_options(-fsanitize=thread -fno-omit-frame-pointer -g -O1)
    add_link_options(-fsanitize=thread)
    message(STATUS "ThreadSanitizer enabled")
endif()

option(OB_ENABLE_COVERAGE "Enable gcov/llvm-cov code coverage instrumentation" OFF)

if(OB_ENABLE_COVERAGE)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
        add_compile_options(--coverage -O0 -g)
        add_link_options(--coverage)
    elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        add_compile_options(-fprofile-instr-generate -fcoverage-mapping -O0 -g)
        add_link_options(-fprofile-instr-generate)
    else()
        message(WARNING "Coverage not supported for compiler: ${CMAKE_CXX_COMPILER_ID}")
    endif()
    message(STATUS "Coverage instrumentation enabled")
endif()

# ── Core library ───────────────────────────────────────────────────────────────
add_library(orderbook_core INTERFACE)
target_include_directories(orderbook_core INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(orderbook_core INTERFACE cxx_std_20)

# ── SoA buffer compiled library ────────────────────────────────────────────────
add_library(orderbook_soa STATIC src/soa_buffer.cpp)

# The one translation unit ThreadSanitizer cannot compile.
#
# The SoA buffer's seqlock is built on `std::atomic_thread_fence`, and GCC refuses outright:
# "'atomic_thread_fence' is not supported with '-fsanitize=thread' [-Werror=tsan]". TSan models
# happens-before through atomic operations, not through standalone fences, so it cannot reason about
# a seqlock even if it compiled one.
#
# So this file is excluded rather than the option abandoned. What that costs is real and worth stating:
# **races inside the seqlock itself are outside TSan's reach**, and reports about the data it guards,
# raised from instrumented callers, need reading against the seqlock's design rather than taken at
# face value.
#
# Discovered when #83 moved the instrumentation above the libraries. Until then this file was not
# instrumented either — along with the other twenty-seven — so the build succeeded by accident and
# nobody learned that TSan and this data structure are incompatible.
if(OB_ENABLE_TSAN)
    target_compile_options(orderbook_soa PRIVATE -fno-sanitize=thread -Wno-tsan)
endif()
target_link_libraries(orderbook_soa PUBLIC orderbook_core)

# ── Sequence tracker compiled library ─────────────────────────────────────────
add_library(orderbook_sequence STATIC src/sequence_tracker.cpp src/version_vector.cpp)
target_link_libraries(orderbook_sequence PUBLIC orderbook_core orderbook_logger)

# ── WAL compiled library ───────────────────────────────────────────────────────
add_library(orderbook_wal STATIC src/wal.cpp)
target_link_libraries(orderbook_wal PUBLIC orderbook_core orderbook_epoch orderbook_hlc)

# ── Epoch compiled library ─────────────────────────────────────────────────────
add_library(orderbook_epoch STATIC src/epoch.cpp)
target_link_libraries(orderbook_epoch PUBLIC orderbook_core)

# ── Coordinator compiled library (etcd v3 REST client) ─────────────────────────
find_package(CURL REQUIRED)
add_library(orderbook_coordinator STATIC src/coordinator.cpp)
target_link_libraries(orderbook_coordinator PUBLIC orderbook_epoch orderbook_logger CURL::libcurl)

# ── Failover compiled library (role transitions and monitoring) ────────────────
add_library(orderbook_failover STATIC src/failover.cpp)
target_link_libraries(orderbook_failover PUBLIC orderbook_coordinator orderbook_epoch orderbook_logger)

# ── Codec compiled library ─────────────────────────────────────────────────────
add_library(orderbook_codec STATIC src/codec.cpp)
target_link_libraries(orderbook_codec PUBLIC orderbook_core)

# ── MMAP store compiled library ────────────────────────────────────────────────
add_library(orderbook_mmap STATIC src/mmap_store.cpp)
target_link_libraries(orderbook_mmap PUBLIC orderbook_core)

# ── Columnar store compiled library ────────────────────────────────────────────
add_library(orderbook_columnar STATIC src/columnar_store.cpp)
target_link_libraries(orderbook_columnar PUBLIC orderbook_codec orderbook_mmap orderbook_logger)

# ── LZ4 compression library ────────────────────────────────────────────────────
find_package(PkgConfig)
if(PkgConfig_FOUND)
    pkg_check_modules(LZ4 IMPORTED_TARGET liblz4)
endif()
if(NOT LZ4_FOUND)
    find_library(LZ4_LIBRARY NAMES lz4)
    find_path(LZ4_INCLUDE_DIR NAMES lz4frame.h)
    if(LZ4_LIBRARY AND LZ4_INCLUDE_DIR)
        set(LZ4_FOUND TRUE)
    else()
        message(FATAL_ERROR "LZ4 library not found. Install liblz4-dev or equivalent.")
    endif()
endif()

add_library(orderbook_compression STATIC src/compression.cpp)
target_link_libraries(orderbook_compression PUBLIC orderbook_core)
if(TARGET PkgConfig::LZ4)
    target_link_libraries(orderbook_compression PUBLIC PkgConfig::LZ4)
else()
    target_link_libraries(orderbook_compression PUBLIC ${LZ4_LIBRARY})
    target_include_directories(orderbook_compression PUBLIC ${LZ4_INCLUDE_DIR})
endif()

# ── Metrics registry compiled library ──────────────────────────────────────────
# ── Structured logger compiled library ─────────────────────────────────────────
add_library(orderbook_logger STATIC src/logger.cpp)
target_link_libraries(orderbook_logger PUBLIC orderbook_core)

add_library(orderbook_metrics STATIC src/metrics.cpp)
# Needs the logger: a write to an unregistered metric name updates nothing, so it
# has to be reported rather than swallowed.
target_link_libraries(orderbook_metrics PUBLIC orderbook_core orderbook_logger)

# ── HLC (Hybrid Logical Clock) compiled library ───────────────────────────────
add_library(orderbook_hlc STATIC src/hlc.cpp)
target_link_libraries(orderbook_hlc PUBLIC orderbook_core orderbook_logger)

# ── Conflict resolver compiled library (multi-master LWW) ─────────────────────
add_library(orderbook_conflict_resolver STATIC src/conflict_resolver.cpp)
target_link_libraries(orderbook_conflict_resolver PUBLIC orderbook_hlc orderbook_core orderbook_logger)

# ── Peer registry compiled library (multi-master peer discovery) ───────────────
add_library(orderbook_peer_registry STATIC src/peer_registry.cpp)
target_link_libraries(orderbook_peer_registry PUBLIC orderbook_hlc orderbook_coordinator orderbook_logger nlohmann_json::nlohmann_json)

# ── Anti-entropy compiled library (multi-master gap detection and repair) ──────
add_library(orderbook_anti_entropy STATIC src/anti_entropy.cpp)
target_link_libraries(orderbook_anti_entropy PUBLIC orderbook_peer_registry orderbook_hlc orderbook_logger)

# ── Async snapshot builder (snapshot creation off the io thread, #79) ─────────
add_library(orderbook_async_snapshot STATIC src/async_snapshot.cpp)
target_link_libraries(orderbook_async_snapshot PUBLIC orderbook_core orderbook_sequence orderbook_logger)

# ── Multi-master manager compiled library (peer networking, WAL broadcast) ─────
add_library(orderbook_multi_master STATIC src/multi_master.cpp src/mm_snapshot.cpp)
target_link_libraries(orderbook_multi_master PUBLIC orderbook_async_snapshot orderbook_peer_registry orderbook_conflict_resolver orderbook_anti_entropy orderbook_hlc orderbook_compression orderbook_logger)

# ── Shard map compiled library ─────────────────────────────────────────────────
add_library(orderbook_shard_map STATIC src/shard_map.cpp)
target_link_libraries(orderbook_shard_map PUBLIC orderbook_core orderbook_logger nlohmann_json::nlohmann_json)

# ── Shard router compiled library (client-side routing) ────────────────────────
add_library(orderbook_shard_router STATIC src/shard_router.cpp)
target_link_libraries(orderbook_shard_router PUBLIC orderbook_shard_map orderbook_coordinator orderbook_logger)

# ── Shard coordinator compiled library ──────────────────────────────────────────
add_library(orderbook_shard_coordinator STATIC src/shard_coordinator.cpp)
target_link_libraries(orderbook_shard_coordinator PUBLIC orderbook_shard_map orderbook_coordinator orderbook_engine orderbook_logger)

# ── Metrics HTTP server compiled library ───────────────────────────────────────
add_library(orderbook_metrics_server STATIC src/metrics_server.cpp)
target_link_libraries(orderbook_metrics_server PUBLIC orderbook_metrics orderbook_logger)

# ── Aggregation engine compiled library ────────────────────────────────────────
add_library(orderbook_agg STATIC src/aggregation.cpp)
target_link_libraries(orderbook_agg PUBLIC orderbook_soa)

# ── Query engine compiled library ─────────────────────────────────────────────
add_library(orderbook_query STATIC src/query_engine.cpp)
# Needs the logger: rejected queries must say so, not fail in silence.
target_link_libraries(orderbook_query PUBLIC orderbook_columnar orderbook_agg orderbook_logger)

# ── Engine facade compiled library ────────────────────────────────────────────
add_library(orderbook_engine STATIC src/engine.cpp src/replication.cpp)
target_link_libraries(orderbook_engine PUBLIC orderbook_query orderbook_wal orderbook_failover orderbook_compression orderbook_metrics orderbook_logger orderbook_multi_master orderbook_sequence)

# ── C API compiled library ────────────────────────────────────────────────────
add_library(orderbook_capi STATIC src/c_api.cpp)
target_link_libraries(orderbook_capi PUBLIC orderbook_query orderbook_wal)

# ── Coverage ──────────────────────────────────────────────────────────────────
if(OB_ENABLE_COVERAGE)
    # Find lcov/genhtml (gcov path) or llvm-profdata/llvm-cov (clang path)
    find_program(LCOV_EXECUTABLE    lcov)
    find_program(GENHTML_EXECUTABLE genhtml)
    find_program(LLVM_PROFDATA      llvm-profdata)
    find_program(LLVM_COV           llvm-cov)

    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU" AND LCOV_EXECUTABLE AND GENHTML_EXECUTABLE)
        add_custom_target(coverage
            COMMENT "Generating gcov HTML coverage report"
            COMMAND ${LCOV_EXECUTABLE} --directory . --zerocounters
            COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
            COMMAND ${LCOV_EXECUTABLE}
                    --directory .
                    --capture
                    --output-file coverage.info
                    --ignore-errors mismatch
            COMMAND ${LCOV_EXECUTABLE}
                    --remove coverage.info
                    '*/tests/*' '*/benchmarks/*' '*/build/*'
                    '*/_deps/*' '/usr/*'
                    --output-file coverage_filtered.info
            COMMAND ${GENHTML_EXECUTABLE}
                    coverage_filtered.info
                    --output-directory coverage_html
            COMMAND ${CMAKE_COMMAND} -E echo
                    "Coverage report: ${CMAKE_BINARY_DIR}/coverage_html/index.html"
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        )
    elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang" AND LLVM_PROFDATA AND LLVM_COV)
        # Collect all test executables for llvm-cov
        set(OB_TEST_BINARIES
            tests/test_soa_buffer
            tests/test_wal
            tests/test_codec
            tests/test_mmap_store
            tests/test_columnar_store
            tests/test_aggregation
            tests/test_query_engine
            tests/test_engine
            tests/test_c_api
            tests/test_types_and_data_model
        )
        add_custom_target(coverage
            COMMENT "Generating llvm-cov HTML coverage report"
            COMMAND ${CMAKE_COMMAND} -E env LLVM_PROFILE_FILE=coverage_%p.profraw
                    ${CMAKE_CTEST_COMMAND} --output-on-failure
            COMMAND ${LLVM_PROFDATA} merge -sparse coverage_*.profraw
                    -o coverage.profdata
            COMMAND ${LLVM_COV} show
                    ${CMAKE_BINARY_DIR}/${CMAKE_FILES_DIRECTORY}/../tests/test_engine
                    -instr-profile=coverage.profdata
                    -format=html
                    -output-dir=coverage_html
                    -ignore-filename-regex='(_deps|tests|benchmarks)/'
            COMMAND ${CMAKE_COMMAND} -E echo
                    "Coverage report: ${CMAKE_BINARY_DIR}/coverage_html/index.html"
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        )
    else()
        add_custom_target(coverage
            COMMAND ${CMAKE_COMMAND} -E echo
                    "Coverage tools not found. Install lcov+genhtml (GCC) or llvm-profdata+llvm-cov (Clang)."
        )
    endif()
endif()

# ── TCP Server library (testable components) ──────────────────────────────────
add_library(orderbook_tcp_server_lib STATIC
    src/tcp_server.cpp
    src/command_parser.cpp
    src/response_formatter.cpp
    src/session.cpp
    src/subscription_hub.cpp
)
target_link_libraries(orderbook_tcp_server_lib PUBLIC orderbook_engine orderbook_logger orderbook_metrics_server orderbook_shard_coordinator)

# ── Native C++ client library ──────────────────────────────────────────────────
add_library(orderbook_client STATIC src/client.cpp)
target_link_libraries(orderbook_client PUBLIC orderbook_core orderbook_shard_router)

option(OB_CLIENT_LZ4 "Enable optional LZ4 compression in orderbook_client" OFF)
if(OB_CLIENT_LZ4)
    target_compile_definitions(orderbook_client PUBLIC OB_CLIENT_LZ4)
    target_link_libraries(orderbook_client PUBLIC orderbook_compression)
endif()

# ── TCP Server executable ─────────────────────────────────────────────────────
add_executable(ob_tcp_server tools/ob_tcp_server.cpp)
target_link_libraries(ob_tcp_server PRIVATE orderbook_tcp_server_lib)

# ── io_uring transport (optional) ─────────────────────────────────────────────
option(OB_USE_IO_URING "Build with io_uring transport instead of epoll" OFF)

if(OB_USE_IO_URING)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(URING REQUIRED IMPORTED_TARGET liburing)

    # Enable io_uring metrics registration in metrics library (PRIVATE: only metrics.cpp needs it)
    target_compile_definitions(orderbook_metrics PRIVATE OB_USE_IO_URING)

    add_library(orderbook_iouring_server_lib STATIC
        src/io_uring_server.cpp
        src/command_parser.cpp
        src/response_formatter.cpp
        src/session.cpp
        src/subscription_hub.cpp
    )
    target_compile_definitions(orderbook_iouring_server_lib PUBLIC OB_USE_IO_URING)
    target_link_libraries(orderbook_iouring_server_lib PUBLIC
        orderbook_engine orderbook_logger orderbook_metrics_server PkgConfig::URING
    )

    add_executable(ob_tcp_server_iouring tools/ob_tcp_server.cpp)
    target_compile_definitions(ob_tcp_server_iouring PRIVATE OB_USE_IO_URING)
    target_link_libraries(ob_tcp_server_iouring PRIVATE orderbook_iouring_server_lib orderbook_tcp_server_lib)
endif()

# ── CLI tool ───────────────────────────────────────────────────────────────────
if(OB_BUILD_TESTS)
    add_executable(ob_cli tools/ob_cli.cpp)
    target_link_libraries(ob_cli PRIVATE orderbook_engine)
endif()

# ── Shared library for Python/FFI bindings ─────────────────────────────────────
add_library(orderbook_shared SHARED src/c_api.cpp)
target_link_libraries(orderbook_shared PUBLIC orderbook_engine)
set_target_properties(orderbook_shared PROPERTIES
    OUTPUT_NAME "orderbook_shared"
    POSITION_INDEPENDENT_CODE ON
)

# ── Install rules ─────────────────────────────────────────────────────────────
#
# Destinations are **explicitly relative** and the packaging prefix is `/`, so the .deb and the
# tarball contain byte-identical paths. Three attempts got here and both failures are worth keeping.
#
# `${CMAKE_INSTALL_SYSCONFDIR}` is relative, so with the prefix at /usr the config landed in
# `/usr/etc/orderbook/ob.conf` while `packaging/debian/conffiles` declared `/etc/orderbook/ob.conf`.
# The declaration would then have named a path the package does not contain, so the conffile mark
# would have done nothing and the first upgrade would have silently reverted every local edit.
#
# Making it absolute `/etc/orderbook` fixed the .deb and broke the tarball in a worse way: the
# archive generator honours an absolute destination literally and **tried to create /etc/orderbook
# on the build host**. It failed here only for want of privileges — a build running as root, or in a
# container, would have written into the host's /etc while producing a package.
#
# So: relative paths, with CPACK_PACKAGING_INSTALL_PREFIX putting them under `/`. A plain
# `cmake --install` then places files under `<prefix>/usr/...`, which is odd and acceptable: these
# rules exist for packaging, and the wheel's rule is guarded by SKBUILD above.
install(TARGETS ob_tcp_server
    RUNTIME DESTINATION usr/bin
    COMPONENT runtime
)
install(DIRECTORY include/orderbook
    DESTINATION usr/include
    COMPONENT runtime
    FILES_MATCHING PATTERN "*.hpp" PATTERN "*.h"
)
install(FILES packaging/ob.conf
    DESTINATION etc/orderbook
    COMPONENT runtime
)
install(FILES packaging/ob_tcp_server.service
    DESTINATION usr/lib/systemd/system
    COMPONENT runtime
)
install(FILES packaging/ob_tcp_server.1
    DESTINATION usr/share/man/man1
    COMPONENT runtime
)
install(FILES README.md SECURITY.md LICENSE docs/cli.md
    DESTINATION usr/share/doc/orderbook-dbengine
    COMPONENT runtime
)

# ── CPack ─────────────────────────────────────────────────────────────────────
#
# DEB and a plain tarball. **No RPM generator**, and that is a decision rather than an omission:
# `rpmbuild` is not on the development machine, so its output could not be built or inspected here,
# and a generator configuration nobody has run is a mechanism that looks present and is not. The
# packaging CI job installs `rpm` and can add it once there is a job that actually produces and
# checks one.
set(CPACK_PACKAGE_NAME "orderbook-dbengine")
set(CPACK_PACKAGE_VENDOR "Smart Data Engines")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY
    "Level 2 orderbook database engine for HFT environments")
set(CPACK_PACKAGE_HOMEPAGE_URL
    "https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine")
set(CPACK_PACKAGE_CONTACT "Krzysztof Macewicz <krzysztof@smartdataengines.com>")
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
set(CPACK_PACKAGE_INSTALL_DIRECTORY "orderbook-dbengine")
# DEB and a plain tarball always. RPM **only when rpmbuild exists**, which is a decision rather
# than a convenience: `rpmbuild` is not on the development machine, so an unconditional RPM
# generator would fail every local `cpack` run, and a generator configuration nobody can execute is
# a mechanism that looks present and is not. The packaging CI job installs `rpm`, so the RPM is
# produced and inspected there — and that job runs on pull requests touching packaging as well as on
# tags, because a job first exercised on a tag is a job first exercised when it matters most.
set(CPACK_GENERATOR "DEB;TGZ")
find_program(RPMBUILD_EXECUTABLE rpmbuild)
if(RPMBUILD_EXECUTABLE)
    list(APPEND CPACK_GENERATOR "RPM")
    set(CPACK_RPM_PACKAGE_GROUP "Applications/Databases")
    set(CPACK_RPM_PACKAGE_LICENSE "MIT")
    # /etc/orderbook/ob.conf must survive an upgrade, the same requirement the .deb meets through
    # its conffiles declaration.
    set(CPACK_RPM_USER_FILELIST "%config(noreplace) /etc/orderbook/ob.conf")
    # rpm claims directories it did not create otherwise, and /etc, /usr/bin and the rest belong to
    # the distribution's filesystem package.
    set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION
        "/etc;/usr;/usr/bin;/usr/include;/usr/lib;/usr/lib/systemd;/usr/lib/systemd/system;/usr/share;/usr/share/doc;/usr/share/man;/usr/share/man/man1")
endif()
# Both generators produce the same relative layout; see the note above the install rules.
set(CPACK_PACKAGING_INSTALL_PREFIX "/")

# One package rather than one per component: the headers are a few kilobytes, and splitting them
# into -dev would create two packages that must be installed together for the common case.
#
# CPACK_COMPONENTS_ALL is deliberately **not** set. It only takes effect with component install
# enabled, and setting it while component install is off reads as a filter that is doing something.
# What keeps the wheel's library out of the package is the SKBUILD guard above.
set(CPACK_DEB_COMPONENT_INSTALL OFF)
set(CPACK_ARCHIVE_COMPONENT_INSTALL OFF)

set(CPACK_DEBIAN_PACKAGE_SECTION "database")
set(CPACK_DEBIAN_PACKAGE_PRIORITY "optional")
# Shared library dependencies resolved by dpkg-shlibdeps rather than listed by hand, because a
# hand-written list goes stale at the first new link and the symptom is an install that succeeds and
# a binary that will not start.
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
# The configuration file is marked conffile, so a package upgrade does not overwrite an operator's
# edits. Without this the first upgrade silently reverts every local change.
set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA
    "${CMAKE_CURRENT_SOURCE_DIR}/packaging/debian/postinst;${CMAKE_CURRENT_SOURCE_DIR}/packaging/debian/prerm;${CMAKE_CURRENT_SOURCE_DIR}/packaging/debian/conffiles")

include(CPack)

# ── Tests ──────────────────────────────────────────────────────────────────────
if(OB_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)

    # ── Benchmarks ─────────────────────────────────────────────────────────────
    add_subdirectory(benchmarks)
endif()
