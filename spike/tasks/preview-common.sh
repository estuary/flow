# Constants shared by the WP05 preview scripts. Sourced, never executed.
#
# CONTRACTS "Paths and names" lists the stub image with the other shared names,
# but env-common.sh belongs to WP00; this keeps the addition inside WP05's
# paths. Fold it in whenever the two are next touched together.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env-common.sh"

SPIKE_STUB_IMAGE=localhost/flow-sandbox-stub:spike
SPIKE_CATALOG_DIR="$SPIKE_DIR/catalog"

# The spike's images, networks, and podman socket all live in the root store,
# which is what production's reactor drives. An ordinary user on the host
# therefore reaches podman only through sudo, and DOCKER_CLI takes one program
# name, so the sudo goes in a wrapper.
SPIKE_SUDO_PODMAN="$SPIKE_TASKS_DIR/preview-sudo-podman.sh"

# mise owns CARGO_TARGET_DIR (it is per-stack), so these scripts must run under
# it: `mise exec -- spike/tasks/preview-stub.sh`.
: "${CARGO_TARGET_DIR:?run under mise, e.g. mise exec -- spike/tasks/preview-stub.sh}"
SPIKE_BIN_DIR="$CARGO_TARGET_DIR/debug"
