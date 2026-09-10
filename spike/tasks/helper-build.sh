#!/usr/bin/env bash
# Build the helper image. One command, no arguments, no prerequisites beyond
# what env-setup.sh already installed.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/helper-common.sh"

# flow-init lives outside the helper's build context, so it comes in as a named
# one; the Dockerfile copies its sources with `COPY --from=flow-init`.
exec sudo podman build \
    --tag "$SPIKE_HELPER_IMAGE" \
    --build-context "flow-init=$SPIKE_DIR/flow-init" \
    --file "$SPIKE_DIR/helper/Dockerfile" \
    "$SPIKE_DIR/helper"
