#!/usr/bin/env bash
set -euo pipefail

# This should match /project/properties/protobuf.version in pom.xml
PROTOC_VERSION="4.35.0"

os_name="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch_name="$(uname -m)"

case "$os_name" in
  darwin) os_id="osx" ;;
  linux) os_id="linux" ;;
  msys*|mingw*|cygwin*) os_id="windows" ;;
  *) os_id="$os_name" ;;
esac

case "$arch_name" in
  arm64|aarch64) arch_id="aarch_64" ;;
  x86_64|amd64) arch_id="x86_64" ;;
  *) arch_id="$arch_name" ;;
esac

protoc_bin="${HOME}/.m2/repository/com/google/protobuf/protoc/${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-${os_id}-${arch_id}.exe"

if [[ -f "$protoc_bin" && ! -x "$protoc_bin" ]]; then
  chmod +x "$protoc_bin"
fi

if [[ ! -x "$protoc_bin" ]]; then
  echo "protoc not found; run Maven to download com.google.protobuf:protoc:${PROTOC_VERSION} (${protoc_bin})" >&2
  exit 1
fi

args=()
for arg in "$@"; do
  if [[ "$arg" == --java_out=* ]]; then
    out_dir="${arg#--java_out=}"
    args+=("--java_out=lite:${out_dir}")
  else
    args+=("$arg")
  fi
done

exec "$protoc_bin" "${args[@]}"
