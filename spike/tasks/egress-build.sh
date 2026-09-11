#!/usr/bin/env bash
# Build the two egress binaries. One command, no arguments.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env-common.sh"

exec cargo build --release --locked --manifest-path "$SPIKE_DIR/egress/Cargo.toml"
