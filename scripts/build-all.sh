#!/usr/bin/env bash
# Build all three Raft implementations: Java, C++, Rust.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SKIP_TESTS="${SKIP_TESTS:-}"
RELEASE="${RELEASE:-}"

cd "${REPO_ROOT}"

# ── Java ──────────────────────────────────────────────────────────────
echo "==> Building Java"
mvn -B --no-transfer-progress install \
  -DskipTests -DskipJepsenTests=true -Dmaven.javadoc.skip=true

# ── C++ ───────────────────────────────────────────────────────────────
echo
echo "==> Building C++"
CMAKE_FLAGS="-DCMAKE_BUILD_TYPE=Release"
if command -v ninja &>/dev/null; then
  CMAKE_FLAGS="${CMAKE_FLAGS} -G Ninja"
fi
cmake -S graft-cpp -B graft-cpp/build ${CMAKE_FLAGS}
cmake --build graft-cpp/build

# ── Rust ──────────────────────────────────────────────────────────────
echo
echo "==> Building Rust"
if [[ -n "${RELEASE}" ]]; then
  cargo build --manifest-path graft-rust/Cargo.toml --release
else
  cargo build --manifest-path graft-rust/Cargo.toml
fi

# ── Tests (optional) ──────────────────────────────────────────────────
if [[ -n "${SKIP_TESTS}" ]]; then
  echo
  echo "==> Skipping tests (SKIP_TESTS is set)"
else
  echo
  echo "==> Running C++ tests"
  if cmake --build graft-cpp/build --target graft_unit_tests 2>/dev/null; then
    ctest --test-dir graft-cpp/build --output-on-failure
  else
    echo "C++ unit test target not available (Catch2 not found), skipping."
  fi

  echo
  echo "==> Running Rust tests"
  cargo test --manifest-path graft-rust/Cargo.toml

  echo
  echo "==> Running Java tests"
  mvn -B --no-transfer-progress test -DskipJepsenTests=true
fi

echo
echo "All three ports built successfully."
