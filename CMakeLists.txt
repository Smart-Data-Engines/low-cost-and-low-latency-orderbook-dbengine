cmake_minimum_required(VERSION 3.20)
project(orderbook-dbengine VERSION 0.1.0 LANGUAGES CXX)

# ── Language standard ──────────────────────────────────────────────────────────
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

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
include(FetchContent)

# Google Test
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.14.0
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Google Benchmark
FetchContent_Declare(
    benchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG        v1.8.3
)
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
set(BENCHMARK_DOWNLOAD_DEPENDENCIES ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(benchmark)

# RapidCheck
FetchContent_Declare(
    rapidcheck
    GIT_REPOSITORY https://github.com/emil-e/rapidcheck.git
    GIT_TAG        master
)
set(RC_ENABLE_GTEST ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(rapidcheck)

# ── Core library ───────────────────────────────────────────────────────────────
add_library(orderbook_core INTERFACE)
target_include_directories(orderbook_core INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(orderbook_core INTERFACE cxx_std_20)

# ── SoA buffer compiled library ────────────────────────────────────────────────
add_library(orderbook_soa STATIC src/soa_buffer.cpp)
target_link_libraries(orderbook_soa PUBLIC orderbook_core)

# ── WAL compiled library ───────────────────────────────────────────────────────
add_library(orderbook_wal STATIC src/wal.cpp)
target_link_libraries(orderbook_wal PUBLIC orderbook_core)

# ── Codec compiled library ─────────────────────────────────────────────────────
add_library(orderbook_codec STATIC src/codec.cpp)
target_link_libraries(orderbook_codec PUBLIC orderbook_core)

# ── MMAP store compiled library ────────────────────────────────────────────────
add_library(orderbook_mmap STATIC src/mmap_store.cpp)
target_link_libraries(orderbook_mmap PUBLIC orderbook_core)

# ── Columnar store compiled library ────────────────────────────────────────────
add_library(orderbook_columnar STATIC src/columnar_store.cpp)
target_link_libraries(orderbook_columnar PUBLIC orderbook_codec orderbook_mmap)

# ── Aggregation engine compiled library ────────────────────────────────────────
add_library(orderbook_agg STATIC src/aggregation.cpp)
target_link_libraries(orderbook_agg PUBLIC orderbook_soa)

# ── Query engine compiled library ─────────────────────────────────────────────
add_library(orderbook_query STATIC src/query_engine.cpp)
target_link_libraries(orderbook_query PUBLIC orderbook_columnar orderbook_agg)

# ── Engine facade compiled library ────────────────────────────────────────────
add_library(orderbook_engine STATIC src/engine.cpp)
target_link_libraries(orderbook_engine PUBLIC orderbook_query orderbook_wal)

# ── C API compiled library ────────────────────────────────────────────────────
add_library(orderbook_capi STATIC src/c_api.cpp)
target_link_libraries(orderbook_capi PUBLIC orderbook_query orderbook_wal)

# ── Coverage ──────────────────────────────────────────────────────────────────
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

# ── Tests ──────────────────────────────────────────────────────────────────────
enable_testing()
add_subdirectory(tests)

# ── Benchmarks ─────────────────────────────────────────────────────────────────
add_subdirectory(benchmarks)
