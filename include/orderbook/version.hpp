#pragma once

#include <string_view>

namespace ob {

/// The engine's version, from `project(... VERSION)` in `CMakeLists.txt`.
///
/// One definition, reached through the `OB_VERSION` compile definition on `orderbook_core`, because
/// the number used to be retyped: a literal `v0.1.0` in `tools/ob_tcp_server.cpp` was the only place
/// a running node's version appeared at all, and nothing checked it against the build system or
/// against `pyproject.toml` (#90). `VersionStatic.TheVersionIsNotRetyped` holds the two files that
/// still carry it in agreement.
///
/// A default is deliberately absent: if the compile definition is missing the build fails here,
/// rather than shipping a binary that reports "unknown" to an operator asking which build is running.
#ifndef OB_VERSION
#error "OB_VERSION is not defined; link orderbook_core so the version reaches this translation unit"
#endif

constexpr std::string_view kVersion{OB_VERSION};

/// The version as a string, for callers that want a function rather than a constant.
constexpr std::string_view version() { return kVersion; }

} // namespace ob
