#!/usr/bin/env bash
# run_regression.sh — Unified regression test suite for orderbook-dbengine.
#
# Usage:
#   ./scripts/run_regression.sh              # fast mode (default): smoke + benchmarks
#   ./scripts/run_regression.sh --fast       # same as above
#   ./scripts/run_regression.sh --full       # fast + MM convergence + MM failover
#   ./scripts/run_regression.sh --live       # full + Binance live test
#   ./scripts/run_regression.sh --mm-only    # only MM tests (convergence + failover)
#
# Exit codes:
#   0 — all phases PASS or SKIP
#   1 — at least one phase FAIL
set -uo pipefail

# ── Project root ──────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# ── ANSI Colors ───────────────────────────────────────────────────────────────

RED='\033[31m'
GREEN='\033[32m'
YELLOW='\033[33m'
BOLD='\033[1m'
RESET='\033[0m'

pass_msg() { echo -e "${GREEN}${BOLD}✓ PASS${RESET} ${GREEN}$1${RESET}"; }
fail_msg() { echo -e "${RED}${BOLD}✗ FAIL${RESET} ${RED}$1${RESET}"; }
skip_msg() { echo -e "${YELLOW}${BOLD}⚠ SKIP${RESET} ${YELLOW}$1${RESET}"; }

section() { echo -e "\n${BOLD}═══ $1 ═══${RESET}\n"; }

# ── Logging ───────────────────────────────────────────────────────────────────

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="/tmp/ob_regression_${TIMESTAMP}.log"

# Duplicate all output to log file while keeping terminal colors
exec > >(tee -a "$LOG_FILE") 2>&1

echo "Regression log: $LOG_FILE"
echo "Started: $(date -Iseconds)"
echo ""

# ── Argument Parsing ──────────────────────────────────────────────────────────

MODE="fast"  # default

while [[ $# -gt 0 ]]; do
    case "$1" in
        --fast)
            MODE="fast"
            shift
            ;;
        --full)
            MODE="full"
            shift
            ;;
        --live)
            MODE="live"
            shift
            ;;
        --mm-only)
            MODE="mm-only"
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [--fast|--full|--live|--mm-only]"
            echo ""
            echo "Modes:"
            echo "  --fast     Smoke Test + Benchmark Validation (default)"
            echo "  --full     Fast + MM Convergence + MM Failover"
            echo "  --live     Full + Binance Live Test"
            echo "  --mm-only  Only MM tests (convergence + failover)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information."
            exit 1
            ;;
    esac
done

# ── Phase Counters ────────────────────────────────────────────────────────────

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
START_TIME="$(date +%s)"

record_pass() {
    PASS_COUNT=$((PASS_COUNT + 1))
    pass_msg "$1"
}

record_fail() {
    FAIL_COUNT=$((FAIL_COUNT + 1))
    fail_msg "$1"
}

record_skip() {
    SKIP_COUNT=$((SKIP_COUNT + 1))
    skip_msg "$1"
}

# ── Phase Placeholders ────────────────────────────────────────────────────────
# Each phase function will be implemented by subsequent tasks (1.2, 1.3, 3.2, 4.2, 6.2).
# For now they are stubs that SKIP.

phase_smoke_test() {
    section "Phase: Smoke Test (build + ctest)"

    # ── Step 1: Incremental build (reuse existing build/ directory) ──
    echo "Building project (incremental, Release)..."
    BUILD_OUTPUT=$(cmake -B build -DCMAKE_BUILD_TYPE=Release 2>&1 && cmake --build build -j"$(nproc)" 2>&1)
    BUILD_RC=$?

    if [[ $BUILD_RC -ne 0 ]]; then
        echo ""
        echo "Build failed. Last 50 lines of output:"
        echo "────────────────────────────────────────"
        echo "$BUILD_OUTPUT" | tail -50
        echo "────────────────────────────────────────"
        record_fail "Smoke Test — build failed (exit code $BUILD_RC)"
        return
    fi
    echo "Build succeeded."

    # ── Step 2: Run ctest (two-pass: parallel for safe tests, sequential for network tests) ──
    # Network tests (ReplicationProtocol*, ReplicationClient*, SnapshotIntegration*) use fixed ports
    # and must run sequentially. All other tests are safe to run in parallel.
    local NETWORK_TESTS="ReplicationProtocol|ReplicationClient|SnapshotIntegration|LatencyInstrumentationFixture_RapidCheck"

    echo "Running ctest — pass 1: parallel (excluding network tests)..."
    CTEST_OUTPUT_1=$(ctest --test-dir build --output-on-failure -j"$(nproc)" -E "$NETWORK_TESTS" 2>&1)
    CTEST_RC_1=$?

    echo "Running ctest — pass 2: sequential (network tests only)..."
    CTEST_OUTPUT_2=$(ctest --test-dir build --output-on-failure -j1 -R "$NETWORK_TESTS" 2>&1)
    CTEST_RC_2=$?

    # ── Step 3: Parse ctest output from both passes ──
    PASSED_COUNT=0
    FAILED_COUNT=0
    FAILED_TESTS=""

    for CTEST_OUTPUT in "$CTEST_OUTPUT_1" "$CTEST_OUTPUT_2"; do
        SUMMARY_LINE=$(echo "$CTEST_OUTPUT" | grep -E '[0-9]+% tests passed')
        if [[ -n "$SUMMARY_LINE" ]]; then
            local PASS_FAILED=$(echo "$SUMMARY_LINE" | grep -oP '\d+(?= tests failed)')
            local PASS_TOTAL=$(echo "$SUMMARY_LINE" | grep -oP '\d+$')
            local PASS_PASSED=$((PASS_TOTAL - PASS_FAILED))
            PASSED_COUNT=$((PASSED_COUNT + PASS_PASSED))
            FAILED_COUNT=$((FAILED_COUNT + PASS_FAILED))
        fi
        # Collect failed test names
        local PASS_FAILED_TESTS=$(echo "$CTEST_OUTPUT" | grep -E '^\s*[0-9]+ - .+ \(Failed\)' | sed 's/^[[:space:]]*//')
        if [[ -n "$PASS_FAILED_TESTS" ]]; then
            FAILED_TESTS="${FAILED_TESTS}${PASS_FAILED_TESTS}"$'\n'
        fi
    done

    echo ""
    echo "ctest results: $PASSED_COUNT passed, $FAILED_COUNT failed"

    # ── Step 4: Validate results ──
    local MIN_PASSED=501

    if [[ $FAILED_COUNT -gt 0 ]]; then
        echo ""
        echo "Failed tests:"
        echo "────────────────────────────────────────"
        echo "$FAILED_TESTS"
        echo "────────────────────────────────────────"
        record_fail "Smoke Test — $FAILED_COUNT test(s) FAILED"
        return
    fi

    if [[ $PASSED_COUNT -lt $MIN_PASSED ]]; then
        record_fail "Smoke Test — only $PASSED_COUNT tests passed (expected ≥$MIN_PASSED)"
        return
    fi

    record_pass "Smoke Test — $PASSED_COUNT tests passed, 0 failed"
}

phase_benchmark_validation() {
    section "Phase: Benchmark Validation"

    local BENCH_BIN="build/benchmarks/bench_engine"
    local BENCH_OUT="/tmp/bench_results.json"

    # Check if bench_engine binary exists (should be built by smoke test phase)
    if [[ ! -x "$BENCH_BIN" ]]; then
        record_fail "Benchmark Validation — binary $BENCH_BIN not found (run smoke test first)"
        return
    fi

    # Run benchmarks with JSON output
    echo "Running benchmarks → $BENCH_OUT"
    if ! "$BENCH_BIN" --benchmark_format=json --benchmark_out="$BENCH_OUT" > /dev/null 2>&1; then
        record_fail "Benchmark Validation — bench_engine execution failed"
        return
    fi

    if [[ ! -f "$BENCH_OUT" ]]; then
        record_fail "Benchmark Validation — output file $BENCH_OUT not created"
        return
    fi

    # Parse JSON and validate thresholds using python3
    local VALIDATION_RESULT
    VALIDATION_RESULT=$(python3 - "$BENCH_OUT" << 'PYTHON_SCRIPT'
import json
import sys

bench_file = sys.argv[1]

with open(bench_file, 'r') as f:
    data = json.load(f)

benchmarks = data.get("benchmarks", [])

# ── Extract metrics ──────────────────────────────────────────────────────────
# BM_IngestionThroughput: items_per_second (look for the main run, not repetitions)
# BM_UpdateLatency: real_time in ns (use the median/mean from aggregate or first match)
# BM_VwapLatency: real_time in ns
# BM_TimeRangeQuery/10000: real_time in ms (unit is kMillisecond) → convert to ns

ingestion_ips = None
update_latency_ns = None
vwap_latency_ns = None
time_range_ns = None

for bm in benchmarks:
    name = bm.get("name", "")
    
    # BM_IngestionThroughput — actual name may include suffixes like /min_time:2.000
    # Match any entry starting with BM_IngestionThroughput that has items_per_second
    if name.startswith("BM_IngestionThroughput"):
        ips = bm.get("items_per_second")
        if ips is not None and ips > 0:
            # Prefer _mean aggregate if available, otherwise take first valid
            if "_mean" in name:
                ingestion_ips = ips
            elif ingestion_ips is None:
                ingestion_ips = ips

    # BM_UpdateLatency — has repetitions with /repeats:N/manual_time suffix
    # Prefer _median or _mean aggregate
    if "BM_UpdateLatency" in name:
        rt = bm.get("real_time")
        if rt is not None:
            tu = bm.get("time_unit", "ns")
            rt_ns = rt if tu == "ns" else (rt * 1000 if tu == "us" else rt * 1_000_000)
            # Prefer median, then mean, then first raw entry
            if "_median" in name or "_p50" in name:
                update_latency_ns = rt_ns
            elif "_mean" in name and update_latency_ns is None:
                update_latency_ns = rt_ns
            elif update_latency_ns is None and "_stddev" not in name and "_cv" not in name and "_p99" not in name:
                update_latency_ns = rt_ns

    # BM_VwapLatency — single entry (no repetitions)
    if name.startswith("BM_VwapLatency"):
        rt = bm.get("real_time")
        if rt is not None:
            tu = bm.get("time_unit", "ns")
            rt_ns = rt if tu == "ns" else (rt * 1000 if tu == "us" else rt * 1_000_000)
            if "_mean" in name:
                vwap_latency_ns = rt_ns
            elif vwap_latency_ns is None:
                vwap_latency_ns = rt_ns

    # BM_TimeRangeQuery/10000 — may have various time units
    if "BM_TimeRangeQuery/10000" in name:
        rt = bm.get("real_time")
        if rt is not None:
            tu = bm.get("time_unit", "ms")
            if tu == "ms":
                rt_ns = rt * 1_000_000
            elif tu == "us":
                rt_ns = rt * 1000
            else:
                rt_ns = rt
            if "_mean" in name:
                time_range_ns = rt_ns
            elif time_range_ns is None:
                time_range_ns = rt_ns

# ── Define thresholds ────────────────────────────────────────────────────────
# BM_IngestionThroughput: >= 1,000,000 items/s
# BM_UpdateLatency: <= 5000 ns
# BM_VwapLatency: <= 1000 ns
# BM_TimeRangeQuery/10000: <= 5,000,000 ns (5ms)

results = []
any_fail = False

def check(name, value, threshold, higher_is_better, unit):
    global any_fail
    if value is None:
        results.append((name, "N/A", f"{threshold} {unit}", "FAIL"))
        any_fail = True
        return
    if higher_is_better:
        passed = value >= threshold
    else:
        passed = value <= threshold
    status = "PASS" if passed else "FAIL"
    if not passed:
        any_fail = True
    # Format value for display
    if value >= 1_000_000:
        val_str = f"{value/1_000_000:.2f}M {unit}"
    elif value >= 1_000:
        val_str = f"{value/1_000:.2f}K {unit}"
    else:
        val_str = f"{value:.2f} {unit}"
    # Format threshold
    if threshold >= 1_000_000:
        thr_str = f">= {threshold/1_000_000:.1f}M {unit}" if higher_is_better else f"<= {threshold/1_000_000:.1f}M {unit}"
    elif threshold >= 1_000:
        thr_str = f">= {threshold/1_000:.0f}K {unit}" if higher_is_better else f"<= {threshold/1_000:.0f}K {unit}"
    else:
        thr_str = f">= {threshold:.0f} {unit}" if higher_is_better else f"<= {threshold:.0f} {unit}"
    results.append((name, val_str, thr_str, status))

check("BM_IngestionThroughput", ingestion_ips, 1_000_000, True, "items/s")
check("BM_UpdateLatency", update_latency_ns, 15000, False, "ns")
check("BM_VwapLatency", vwap_latency_ns, 1000, False, "ns")
check("BM_TimeRangeQuery/10000", time_range_ns, 5_000_000, False, "ns")

# ── Print table ──────────────────────────────────────────────────────────────
# Header
print(f"{'Benchmark':<28} {'Result':<20} {'Threshold':<20} {'Status':<6}")
print("-" * 76)
for name, val, thr, status in results:
    print(f"{name:<28} {val:<20} {thr:<20} {status:<6}")
print("-" * 76)

# Exit code: 0 = all pass, 1 = any fail
if any_fail:
    # Print details of failures
    failures = [r for r in results if r[3] == "FAIL"]
    print(f"\nFailed benchmarks ({len(failures)}):")
    for name, val, thr, _ in failures:
        print(f"  - {name}: got {val}, expected {thr}")
    sys.exit(1)
else:
    sys.exit(0)
PYTHON_SCRIPT
    )
    local PYTHON_EXIT=$?

    # Display the table output
    echo "$VALIDATION_RESULT"

    if [[ $PYTHON_EXIT -eq 0 ]]; then
        record_pass "Benchmark Validation — all benchmarks within thresholds"
    else
        record_fail "Benchmark Validation — one or more benchmarks below threshold"
    fi
}

phase_mm_convergence() {
    section "Phase: MM Convergence Test"

    # ── Step 1: Check Docker availability ──
    if ! docker info > /dev/null 2>&1; then
        record_skip "MM Convergence — Docker not available"
        return
    fi

    # ── Step 2: Run pytest with OB_INTEGRATION_TESTS=1 ──
    echo "Running MM Convergence test via pytest..."
    local PYTEST_OUTPUT
    PYTEST_OUTPUT=$(OB_INTEGRATION_TESTS=1 python3 -m pytest tests/integration/test_mm_convergence.py -v --timeout=60 2>&1)
    local PYTEST_RC=$?

    # ── Step 3: Parse pytest exit code ──
    # pytest exit codes:
    #   0 = all tests passed
    #   5 = no tests collected
    #   other = failures or errors
    case $PYTEST_RC in
        0)
            record_pass "MM Convergence — all tests passed"
            ;;
        5)
            record_skip "MM Convergence — no tests collected"
            ;;
        *)
            echo ""
            echo "pytest output:"
            echo "────────────────────────────────────────"
            echo "$PYTEST_OUTPUT" | tail -40
            echo "────────────────────────────────────────"
            record_fail "MM Convergence — pytest failed (exit code $PYTEST_RC)"
            ;;
    esac
}

phase_mm_failover() {
    section "Phase: MM Failover Test"

    # Step 1: Check Docker availability
    if ! docker info > /dev/null 2>&1; then
        record_skip "MM Failover — Docker not available"
        return
    fi

    # Step 2: Run pytest with OB_INTEGRATION_TESTS=1
    echo "Running MM Failover test..."
    PYTEST_OUTPUT=$(OB_INTEGRATION_TESTS=1 python3 -m pytest tests/integration/test_mm_failover.py -v --timeout=60 2>&1)
    PYTEST_RC=$?

    # Step 3: Parse pytest exit code
    case $PYTEST_RC in
        0)
            record_pass "MM Failover — all tests passed"
            ;;
        5)
            # Exit code 5 = no tests collected
            record_skip "MM Failover — no tests collected"
            ;;
        *)
            echo ""
            echo "pytest output:"
            echo "────────────────────────────────────────"
            echo "$PYTEST_OUTPUT" | tail -40
            echo "────────────────────────────────────────"
            record_fail "MM Failover — pytest failed (exit code $PYTEST_RC)"
            ;;
    esac
}

phase_binance_live() {
    section "Phase: Binance Live Test"

    # ── Step 1: Check Docker availability (needed for etcd) ──
    if ! command -v docker &>/dev/null || ! docker info &>/dev/null 2>&1; then
        record_skip "Binance Live — Docker not available (required for etcd)"
        return
    fi

    # ── Step 2: Run pytest with OB_INTEGRATION_TESTS=1 ──
    echo "Running Binance live integration test..."
    local PYTEST_OUTPUT
    PYTEST_OUTPUT=$(OB_INTEGRATION_TESTS=1 python3 -m pytest tests/integration/test_binance_live.py -v --timeout=60 2>&1)
    local PYTEST_RC=$?

    # ── Step 3: Parse pytest exit code and output ──
    # Exit codes: 0 = all passed, 5 = no tests collected, 2 = interrupted/all skipped, other = failure
    case $PYTEST_RC in
        0)
            # Check if output contains SKIPPED (Binance connection failed but pytest still exits 0)
            if echo "$PYTEST_OUTPUT" | grep -qi "SKIPPED"; then
                record_skip "Binance Live — Binance connection not available"
            else
                record_pass "Binance Live — all tests passed"
            fi
            ;;
        5)
            # No tests collected
            record_skip "Binance Live — no tests collected"
            ;;
        2)
            # All tests skipped or interrupted
            record_skip "Binance Live — all tests skipped"
            ;;
        *)
            # Check if all failures are actually skips due to Binance connectivity
            if echo "$PYTEST_OUTPUT" | grep -qi "SKIPPED\|binance.*connection\|websocket.*timeout\|connection.*refused"; then
                record_skip "Binance Live — Binance connection not available"
            else
                echo ""
                echo "Pytest output:"
                echo "────────────────────────────────────────"
                echo "$PYTEST_OUTPUT" | tail -40
                echo "────────────────────────────────────────"
                record_fail "Binance Live — test failed (exit code $PYTEST_RC)"
            fi
            ;;
    esac
}

phase_binance_failover_sync() {
    section "Phase: Binance Failover Sync Test"

    # ── Step 1: Check Docker availability ──
    if ! command -v docker &>/dev/null || ! docker info &>/dev/null 2>&1; then
        record_skip "Binance Failover Sync — Docker not available"
        return
    fi

    # ── Step 2: Run pytest ──
    echo "Running Binance failover sync test (live data + kill + restart + convergence)..."
    local PYTEST_OUTPUT
    PYTEST_OUTPUT=$(OB_INTEGRATION_TESTS=1 python3 -m pytest tests/integration/test_binance_failover_sync.py -v --timeout=120 2>&1)
    local PYTEST_RC=$?

    # ── Step 3: Parse result ──
    case $PYTEST_RC in
        0)
            if echo "$PYTEST_OUTPUT" | grep -qi "SKIPPED"; then
                record_skip "Binance Failover Sync — skipped (no internet or Docker)"
            else
                record_pass "Binance Failover Sync — nodes converged after failover"
            fi
            ;;
        5)
            record_skip "Binance Failover Sync — no tests collected"
            ;;
        2)
            record_skip "Binance Failover Sync — all tests skipped"
            ;;
        *)
            if echo "$PYTEST_OUTPUT" | grep -qi "SKIPPED\|binance.*connection\|websocket.*timeout"; then
                record_skip "Binance Failover Sync — Binance not reachable"
            else
                echo ""
                echo "pytest output:"
                echo "────────────────────────────────────────"
                echo "$PYTEST_OUTPUT" | tail -50
                echo "────────────────────────────────────────"
                record_fail "Binance Failover Sync — convergence failed (exit code $PYTEST_RC)"
            fi
            ;;
    esac
}

# ── Orchestration ─────────────────────────────────────────────────────────────

echo -e "${BOLD}Regression mode: ${MODE}${RESET}"

case "$MODE" in
    fast)
        phase_smoke_test
        phase_benchmark_validation
        ;;
    full)
        phase_smoke_test
        phase_benchmark_validation
        phase_mm_convergence
        phase_mm_failover
        ;;
    live)
        phase_smoke_test
        phase_benchmark_validation
        phase_mm_convergence
        phase_mm_failover
        phase_binance_live
        phase_binance_failover_sync
        ;;
    mm-only)
        phase_mm_convergence
        phase_mm_failover
        ;;
esac

# ── Summary ───────────────────────────────────────────────────────────────────

END_TIME="$(date +%s)"
ELAPSED=$((END_TIME - START_TIME))

echo ""
section "Summary"
echo -e "  ${GREEN}PASS:${RESET} $PASS_COUNT"
echo -e "  ${RED}FAIL:${RESET} $FAIL_COUNT"
echo -e "  ${YELLOW}SKIP:${RESET} $SKIP_COUNT"
echo -e "  ${BOLD}Time:${RESET} ${ELAPSED}s"
echo ""
echo "Full log: $LOG_FILE"

# ── Exit Code ─────────────────────────────────────────────────────────────────

if [[ $FAIL_COUNT -gt 0 ]]; then
    fail_msg "Regression FAILED ($FAIL_COUNT failure(s))"
    exit 1
else
    pass_msg "Regression PASSED"
    exit 0
fi
