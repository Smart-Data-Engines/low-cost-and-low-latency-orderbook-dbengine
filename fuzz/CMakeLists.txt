# Compile production implementations with the same instrumentation as their fuzz drivers.
add_executable(fuzz_command_parser command_parser.cpp)
target_link_libraries(fuzz_command_parser PRIVATE orderbook_tcp_server_lib)

add_executable(fuzz_mm_frames mm_frames.cpp)
target_link_libraries(fuzz_mm_frames PRIVATE orderbook_mm_framing orderbook_logger)

add_executable(fuzz_wal_replay wal_replay.cpp)
target_link_libraries(fuzz_wal_replay PRIVATE orderbook_wal orderbook_logger)

foreach(target fuzz_command_parser fuzz_mm_frames fuzz_wal_replay)
    target_link_options(${target} PRIVATE -fsanitize=fuzzer)
endforeach()
