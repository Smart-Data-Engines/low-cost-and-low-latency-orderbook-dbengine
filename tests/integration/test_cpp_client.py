"""Native C++ client against a running server.

These run the `ob_integration_test` binary, which links `OrderbookClient` and prints
one JSON line per test. They exist because the C++ client's own unit tests feed its
parsers hand-written strings: only a live socket proves that the server and the
client agree on the bytes.

If the binary was not built, every test here skips with a reason rather than failing
— but the skip names the path it looked in, so a missing binary cannot be mistaken
for coverage.
"""
from __future__ import annotations

import json
import pathlib
import subprocess

import pytest
import os

pytestmark = pytest.mark.cpp_client

# tests/integration/test_cpp_client.py → repo root → build/tests/
REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
BINARY_CANDIDATES = [
    REPO_ROOT / "build" / "tests" / "ob_integration_test",
    REPO_ROOT / "build-release" / "tests" / "ob_integration_test",
]


def find_binary() -> pathlib.Path | None:
    """The client harness, from the same build tree as the server under test.

    `OB_SERVER_BINARY` comes first and is derived rather than guessed: the harness lives at
    `<build>/tests/ob_integration_test` beside the server's `<build>/ob_tcp_server`. Without this
    the sanitizer job pointed at `build-tsan/` and this module looked in `build/`, so all seven of
    its tests skipped - and a skipped integration test is indistinguishable from a passing one in a
    summary line. That is the same defect four other modules had with the server path itself.
    """
    from_env = os.environ.get("OB_SERVER_BINARY")
    if from_env:
        derived = pathlib.Path(from_env).resolve().parent / "tests" / "ob_integration_test"
        if derived.is_file():
            return derived
    for candidate in BINARY_CANDIDATES:
        if candidate.is_file():
            return candidate
    return None


@pytest.fixture(scope="module")
def binary() -> pathlib.Path:
    found = find_binary()
    if found is None:
        pytest.skip(
            "ob_integration_test not built; looked in: "
            + ", ".join(str(c) for c in BINARY_CANDIDATES)
            + " (build with -DOB_BUILD_TESTS=ON)")
    return found


def run_cpp_test(binary: pathlib.Path, port: int, name: str,
                 timeout: float = 30.0) -> dict:
    """Run one test from the binary and return its parsed JSON result."""
    proc = subprocess.run(
        [str(binary), "--host", "127.0.0.1", "--port", str(port), "--test", name],
        capture_output=True, text=True, timeout=timeout,
    )

    # The binary prints exactly one JSON object on stdout. Structured server logs can
    # share the stream, so pick the line that parses.
    parsed = None
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            candidate = json.loads(line)
        except json.JSONDecodeError:
            continue
        if candidate.get("test") == name:
            parsed = candidate
            break

    assert parsed is not None, (
        f"no JSON result for {name!r} on stdout.\n"
        f"exit={proc.returncode}\nstdout={proc.stdout[:800]}\n"
        f"stderr={proc.stderr[:400]}")

    # Exit code and reported status must agree, or one of them is lying.
    expected_code = 0 if parsed["status"] == "pass" else 1
    assert proc.returncode == expected_code, (
        f"{name}: status={parsed['status']} but exit code {proc.returncode}")

    return parsed


def test_cpp_ping(binary, cluster):
    result = run_cpp_test(binary, cluster.primary().tcp_port, "ping")
    assert result["status"] == "pass", result["message"]


def test_cpp_insert_and_query(binary, cluster):
    result = run_cpp_test(binary, cluster.primary().tcp_port, "insert_query")
    assert result["status"] == "pass", result["message"]
    assert "rows=" in result["message"], result


def test_cpp_minsert_of_100_levels(binary, cluster):
    result = run_cpp_test(binary, cluster.primary().tcp_port, "minsert")
    assert result["status"] == "pass", result["message"]


def test_cpp_aggregates_with_scales(binary, cluster):
    """Covers query_agg() end to end, including the row API refusing that shape."""
    result = run_cpp_test(binary, cluster.primary().tcp_port, "query_agg")
    assert result["status"] == "pass", result["message"]
    assert "scale=1000000" in result["message"], result


def test_cpp_client_reports_failure_against_a_dead_port(binary, cluster):
    """A test binary that passes when the server is absent would prove nothing."""
    dead_port = cluster.find_free_port()
    proc = subprocess.run(
        [str(binary), "--host", "127.0.0.1", "--port", str(dead_port),
         "--test", "ping"],
        capture_output=True, text=True, timeout=30,
    )
    assert proc.returncode != 0, (
        "the binary reported success with no server listening: "
        f"stdout={proc.stdout[:300]}")
    assert "fail" in proc.stdout, proc.stdout[:300]


def test_cpp_unknown_test_name_is_rejected(binary):
    proc = subprocess.run(
        [str(binary), "--host", "127.0.0.1", "--port", "1", "--test", "nonsense"],
        capture_output=True, text=True, timeout=30,
    )
    assert proc.returncode != 0
    assert "Unknown test" in proc.stderr or "Usage" in proc.stderr, proc.stderr[:300]


def test_cpp_write_to_replica_is_refused(binary, cluster):
    """The replica must reject a C++ client's write, same as any other client."""
    proc = subprocess.run(
        [str(binary), "--host", "127.0.0.1", "--port",
         str(cluster.replica().tcp_port), "--test", "insert_query"],
        capture_output=True, text=True, timeout=30,
    )
    assert proc.returncode != 0, (
        "a write to the replica succeeded through the C++ client: "
        f"{proc.stdout[:300]}")
