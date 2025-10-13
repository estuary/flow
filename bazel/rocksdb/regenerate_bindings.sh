#!/bin/bash
set -euo pipefail

# Script to regenerate RocksDB FFI bindings
# Run this when upgrading to a new RocksDB version

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROCKSDB_VERSION="${1:-0.17.3}"

echo "Regenerating RocksDB bindings for librocksdb-sys version ${ROCKSDB_VERSION}..."

# Create temporary workspace
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

cd "$TMPDIR"

# Create minimal Cargo project
cat > Cargo.toml <<EOF
[package]
name = "rocksdb-bindings-gen"
version = "0.1.0"
edition = "2024"

[dependencies]
librocksdb-sys = "${ROCKSDB_VERSION}"
EOF

mkdir -p src
echo "fn main() {}" > src/main.rs

# Build to trigger bindgen
echo "Building to generate bindings (this will take ~1 minute)..."
cargo build 2>&1 | tail -5

# Find the generated bindings
BINDINGS=$(find target -name "bindings.rs" -type f | head -1)

if [ -z "$BINDINGS" ]; then
    echo "Error: Could not find generated bindings.rs"
    exit 1
fi

# Copy to destination
cp "$BINDINGS" "${SCRIPT_DIR}/bindings.rs"

echo "✓ Bindings regenerated successfully!"
echo "  Location: ${SCRIPT_DIR}/bindings.rs"
echo "  Lines: $(wc -l < "${SCRIPT_DIR}/bindings.rs")"
echo ""
echo "Next steps:"
echo "  1. Update MODULE.bazel with new version/SHA256"
echo "  2. Test: bazelisk build @librocksdb//:rocksdb_sys"
echo "  3. Test: bazelisk build @rust_rocksdb//:rocksdb"
echo "  4. Test: bazelisk test //crates/derive:derive_lib_test"
