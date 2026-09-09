#!/usr/bin/env bash
# Build the helper image. One command, no arguments, no prerequisites beyond
# what env-setup.sh already installed.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/helper-common.sh"

exec sudo podman build \
    --tag "$SPIKE_HELPER_IMAGE" \
    --file "$SPIKE_DIR/helper/Dockerfile" \
    "$SPIKE_DIR/helper"
