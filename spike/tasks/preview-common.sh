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

# --- WP06 -------------------------------------------------------------------
#
# Experiments 1 to 4 all drive `flowctl preview` through the fake reactor, so
# the driver lives here with the rest of the preview harness rather than in an
# `exp1-common.sh` that the other three would have to source by number.

# Everything `flowctl preview` reads inside the fake reactor must live under
# $SPIKE_REACTOR_DIR: that, /run/podman and $CARGO_TARGET_DIR are the container's
# only mounts, and each is at the same path on both sides.
SPIKE_WORK_DIR="$SPIKE_REACTOR_DIR/wp06"

# Copy the catalog to where both sides can see it. Returns nothing; callers name
# specs and policies by basename under $SPIKE_WORK_DIR.
spike_stage_catalog() {
    rm -rf "$SPIKE_WORK_DIR"
    mkdir -p "$SPIKE_WORK_DIR/tmp"
    cp -r "$SPIKE_CATALOG_DIR/." "$SPIKE_WORK_DIR/"
    for bin in flowctl flow-connector-init; do
        [ -x "$SPIKE_BIN_DIR/$bin" ] && continue
        echo "missing $SPIKE_BIN_DIR/$bin" >&2
        echo "run: mise exec -- cargo build -p flowctl -p connector-init" >&2
        return 2
    done
}

# Extra FLOW_SANDBOX_SPIKE_* settings for `spike_preview` (forwarded by
# fake-reactor.sh by name), and extra variables for flowctl itself inside the
# container (forwarded by value, since sudo scrubs the environment).
SPIKE_PREVIEW_ENV=()
SPIKE_PREVIEW_GUEST_ENV=()

# spike_preview POLICY SPEC [FLOWCTL ARGS...]
#
# POLICY is a basename under $SPIKE_WORK_DIR, or "" to leave the switch off and
# run the unmodified path. SPEC is likewise a basename. Documents go to stdout,
# logs to stderr, and the exit status is flowctl's.
#
# TMPDIR lands under the reactor directory because `flowctl preview` validates
# the catalog before runtime-next drives anything, and that step runs its
# connector through the legacy `runtime` crate, which bind-mounts host
# temporaries: the reactor container's own /tmp does not exist for the host
# podman that receives the mount.
spike_preview() {
    local policy="$1" spec="$2"
    shift 2

    local switch=()
    [ -n "$policy" ] && switch=(FLOW_SANDBOX_SPIKE_POLICY="$SPIKE_WORK_DIR/$policy")

    env "${switch[@]}" "${SPIKE_PREVIEW_ENV[@]}" \
        "$SPIKE_TASKS_DIR/fake-reactor.sh" \
        env "TMPDIR=$SPIKE_WORK_DIR/tmp" "${SPIKE_PREVIEW_GUEST_ENV[@]}" \
        /flow-target/debug/flowctl preview \
        --source "$SPIKE_WORK_DIR/$spec" \
        --network "$SPIKE_NET_CONNECTORS" \
        "$@"
}
