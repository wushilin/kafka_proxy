#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${ROOT_DIR}"

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is not installed" >&2
  exit 1
fi

if ! command -v rustup >/dev/null 2>&1; then
  echo "error: rustup is not installed" >&2
  exit 1
fi

BIN_NAME="$(awk -F'"' '/^name = / {print $2; exit}' Cargo.toml)"
VERSION="$(awk -F'"' '/^version = / {print $2; exit}' Cargo.toml)"

if [[ -z "${BIN_NAME}" || -z "${VERSION}" ]]; then
  echo "error: failed to read package name/version from Cargo.toml" >&2
  exit 1
fi

DIST_DIR="${ROOT_DIR}/dist"
mkdir -p "${DIST_DIR}"

TARGETS=(
  "x86_64-unknown-linux-musl:amd64"
  "aarch64-unknown-linux-musl:arm64"
)

BUILD_TOOL="cross"
FORCE_TOOL="${BUILD_TOOL_OVERRIDE:-}"
for arg in "$@"; do
  case "${arg}" in
    --no-cross)
      FORCE_TOOL="cargo"
      ;;
    --tool=*)
      FORCE_TOOL="${arg#--tool=}"
      ;;
  esac
done

if [[ -n "${FORCE_TOOL}" ]]; then
  BUILD_TOOL="${FORCE_TOOL}"
else
  if ! command -v cross >/dev/null 2>&1; then
    echo "error: cross is required by default but not found in PATH" >&2
    echo "hint: install cross or use --tool=zigbuild / --tool=cargo / --no-cross" >&2
    exit 1
  fi
fi

echo "==> Using build tool: ${BUILD_TOOL}"

if [[ "${BUILD_TOOL}" != "cross" ]]; then
  echo "==> Adding required Rust targets"
  rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl
else
  echo "==> Skipping rustup target add (cross manages target toolchains)"
fi

build_target() {
  local target="$1"
  local arch="$2"
  local out_file="${DIST_DIR}/${BIN_NAME}-${VERSION}-linux-${arch}-musl"

  echo "==> Building ${target}"
  if [[ "${BUILD_TOOL}" == "cross" ]]; then
    cross build --release --target "${target}"
  elif [[ "${BUILD_TOOL}" == "zigbuild" ]]; then
    cargo zigbuild --release --target "${target}"
  else
    # Note: aarch64 musl may require a cross-linker/toolchain if plain cargo is used.
    cargo build --release --target "${target}"
  fi

  local src_bin="${ROOT_DIR}/target/${target}/release/${BIN_NAME}"
  if [[ ! -f "${src_bin}" ]]; then
    echo "error: expected binary not found: ${src_bin}" >&2
    exit 1
  fi

  cp "${src_bin}" "${out_file}"
  chmod +x "${out_file}"
  echo "    wrote ${out_file}"

  if command -v tar >/dev/null 2>&1; then
    tar -C "${DIST_DIR}" -czf "${out_file}.tar.gz" "$(basename "${out_file}")"
    echo "    wrote ${out_file}.tar.gz"
  fi
}

for item in "${TARGETS[@]}"; do
  target="${item%%:*}"
  arch="${item##*:}"
  build_target "${target}" "${arch}"
done

if command -v sha256sum >/dev/null 2>&1; then
  echo "==> Writing checksums"
  (
    cd "${DIST_DIR}"
    sha256sum "${BIN_NAME}-${VERSION}-linux-"*-musl "${BIN_NAME}-${VERSION}-linux-"*-musl.tar.gz > SHA256SUMS
  )
  echo "    wrote ${DIST_DIR}/SHA256SUMS"
fi

echo "==> Done"
echo "Artifacts in ${DIST_DIR}:"
ls -1 "${DIST_DIR}"
