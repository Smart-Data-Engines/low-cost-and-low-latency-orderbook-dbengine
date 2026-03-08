# Contributing to orderbook-dbengine

Thanks for your interest in contributing. Here's how to get involved.

## Submitting an Issue

We use GitHub Issues to track bugs, feature requests, and questions.

### How to file an issue

1. Go to the [Issues](../../issues) tab in this repository
2. Click **New issue**
3. Choose a template:
   - **Bug Report** — something is broken or behaving unexpectedly
   - **Feature Request** — you'd like to propose a new capability
4. Fill in the template fields — the more detail, the faster we can help
5. Click **Submit new issue**

### Tips for a good bug report

- Include the exact commands you ran and the full error output
- Mention your OS, compiler version, and CMake version
- If it's a crash, include a backtrace (`bt` in gdb, or AddressSanitizer output)
- If it's a test failure, paste the `ctest --output-on-failure` output
- Minimal reproducible examples are worth their weight in gold

### Tips for a good feature request

- Explain the problem you're trying to solve, not just the solution
- If you have a design in mind, sketch it out — even pseudocode helps
- Reference relevant requirements from the design doc if applicable

## Building and Testing

```bash
# Configure and build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build

# Run all tests
ctest --test-dir build --output-on-failure

# Run benchmarks
./build/benchmarks/bench_engine --benchmark_format=json
```

## Code Style

- C++20, compiled with `-Wall -Wextra -Werror`
- Header files in `include/orderbook/`, implementations in `src/`
- Tests in `tests/`, one test file per component
- Property-based tests use RapidCheck, unit tests use Google Test

## Pull Requests

1. Fork the repo and create a branch from `main`
2. Make your changes — keep commits focused and atomic
3. Ensure all 119 tests pass locally
4. Open a PR with a clear description of what changed and why
