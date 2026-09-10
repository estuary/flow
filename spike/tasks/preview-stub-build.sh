#!/usr/bin/env bash
# Build localhost/flow-sandbox-stub:spike, the VM-less stand-in for the helper
# that WP05 tests its launch line against. Idempotent.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/preview-common.sh"

exec sudo podman build \
    --build-arg "REACTOR_IMAGE=$SPIKE_REACTOR_IMAGE" \
    -t "$SPIKE_STUB_IMAGE" \
    "$SPIKE_DIR/stub-helper"
