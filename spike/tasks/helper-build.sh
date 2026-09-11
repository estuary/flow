#!/usr/bin/env bash
# Build the helper image. One command, no arguments, no prerequisites beyond
# what env-setup.sh already installed.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/helper-common.sh"

# flow-init and the egress binaries live outside the helper's build context, so
# they come in as named ones; the Dockerfile copies their sources with
# `COPY --from=flow-init` and `COPY --from=egress`.
exec sudo podman build \
    --tag "$SPIKE_HELPER_IMAGE" \
    --build-context "flow-init=$SPIKE_DIR/flow-init" \
    --build-context "egress=$SPIKE_DIR/egress" \
    --file "$SPIKE_DIR/helper/Dockerfile" \
    "$SPIKE_DIR/helper"
