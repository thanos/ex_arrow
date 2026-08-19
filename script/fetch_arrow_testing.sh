#!/usr/bin/env bash
# Fetch apache/arrow-testing IPC data files for optional conformance tests.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$ROOT/test/fixtures/arrow_testing"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

git clone --depth 1 https://github.com/apache/arrow-testing.git "$TMP/arrow-testing"
mkdir -p "$DEST"
# Copy a small, stable subset of IPC fixtures if present.
if [[ -d "$TMP/arrow-testing/data/arrow-ipc-stream/Integration" ]]; then
  cp -R "$TMP/arrow-testing/data/arrow-ipc-stream/Integration" "$DEST/ipc_stream" || true
fi
if [[ -d "$TMP/arrow-testing/data" ]]; then
  find "$TMP/arrow-testing/data" -name '*.arrow' | head -20 | while read -r f; do
    cp "$f" "$DEST/"
  done
fi
echo "Fixtures installed under $DEST"
ls -la "$DEST" || true
