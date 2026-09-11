#!/usr/bin/env bash
# WP05 verification, one button. Drives the same capture through
# `flowctl preview` three ways and diffs the documents each run produces:
#
#   1. switch off              the unmodified runtime, connector over TCP
#   2. switch on, from host    the stub helper, dialed over a Unix socket
#   3. switch on, fake reactor the same, from production's privilege level,
#                              which is what proves the reactor-directory
#                              path mapping
#
# Identical output means the switched-on launch reached connector-init over
# `<id>/sock/init.sock` and ran the identical readiness protocol, log decoder,
# codec, and transaction driving above it.
#
# Run under mise: mise exec -- spike/tasks/preview-stub.sh
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/preview-common.sh"

step() { printf '\n== %s\n' "$*"; }

SPEC="$SPIKE_CATALOG_DIR/capture-hello-world.flow.yaml"
POLICY="$SPIKE_CATALOG_DIR/policy-egress-none.json"
# Bounded sessions make the document count deterministic, so the three runs are
# comparable at all.
SESSIONS=3,2

OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

# `ts` is the only field that moves between runs.
redact() { jq -c '.[1].ts = "<redacted>"' <"$1" >"$1.redacted"; }

step "stub helper image"
sudo podman image exists "$SPIKE_STUB_IMAGE" || "$SPIKE_TASKS_DIR/preview-stub-build.sh"
printf '%s %s\n' "$SPIKE_STUB_IMAGE" "$(sudo podman image inspect --format '{{.Id}}' "$SPIKE_STUB_IMAGE")"

step "binaries"
for bin in flowctl flow-connector-init; do
    if [ ! -x "$SPIKE_BIN_DIR/$bin" ]; then
        echo "missing $SPIKE_BIN_DIR/$bin" >&2
        echo "run: mise exec -- cargo build -p flowctl -p connector-init" >&2
        exit 2
    fi
    printf '%s\n' "$SPIKE_BIN_DIR/$bin"
done

# The runtime removes each `<id>` directory it creates; anything left behind is
# a leak, so the count is taken before and after.
dirs_before=$(find "$SPIKE_REACTOR_DIR" -mindepth 1 -maxdepth 1 -name 'fs_*' | wc -l)

step "1/3 switch off: the unmodified runtime"
env DOCKER_CLI="$SPIKE_SUDO_PODMAN" \
    "$SPIKE_BIN_DIR/flowctl" preview \
    --source "$SPEC" --sessions "$SESSIONS" --network "$SPIKE_NET_CONNECTORS" \
    >"$OUT/off.ndjson"
redact "$OUT/off.ndjson"

step "2/3 switch on, from the host"
env DOCKER_CLI="$SPIKE_SUDO_PODMAN" \
    FLOW_SANDBOX_SPIKE_POLICY="$POLICY" \
    FLOW_SANDBOX_SPIKE_HELPER_IMAGE="$SPIKE_STUB_IMAGE" \
    "$SPIKE_BIN_DIR/flowctl" preview \
    --source "$SPEC" --sessions "$SESSIONS" --network "$SPIKE_NET_CONNECTORS" \
    >"$OUT/host.ndjson"
redact "$OUT/host.ndjson"

step "3/3 switch on, inside the fake reactor"
# fake-reactor.sh mounts only /run/podman, $SPIKE_REACTOR_DIR (at the same
# path) and $CARGO_TARGET_DIR. The spec and policy therefore go under the
# reactor directory, where both sides see one path; the binaries are already
# reachable at /flow-target, which is this stack's $CARGO_TARGET_DIR (WP06).
work="$SPIKE_REACTOR_DIR/wp05"
mkdir -p "$work/tmp"
cp "$SPEC" "$POLICY" "$work/"

# TMPDIR must also land under the reactor directory. `flowctl preview` first
# validates the catalog, and that step still runs its connector through the
# legacy `runtime` crate, which bind-mounts host temporaries: the reactor
# container's own /tmp does not exist for the host podman that gets the mount.
# This is the whole reason the spike's reactor-directory contract exists, and
# `env` is prepended here because fake-reactor.sh forwards only
# FLOW_SANDBOX_SPIKE_* by name.
env FLOW_SANDBOX_SPIKE_POLICY="$work/$(basename "$POLICY")" \
    FLOW_SANDBOX_SPIKE_HELPER_IMAGE="$SPIKE_STUB_IMAGE" \
    "$SPIKE_TASKS_DIR/fake-reactor.sh" env "TMPDIR=$work/tmp" \
    /flow-target/debug/flowctl preview \
    --source "$work/$(basename "$SPEC")" --sessions "$SESSIONS" \
    --network "$SPIKE_NET_CONNECTORS" \
    >"$OUT/reactor.ndjson"
redact "$OUT/reactor.ndjson"

step "results"
printf 'documents: off=%s host=%s reactor=%s\n' \
    "$(wc -l <"$OUT/off.ndjson")" \
    "$(wc -l <"$OUT/host.ndjson")" \
    "$(wc -l <"$OUT/reactor.ndjson")"
sed -n 1p "$OUT/off.ndjson.redacted"

fail=0
for case in host reactor; do
    if diff -u "$OUT/off.ndjson.redacted" "$OUT/$case.ndjson.redacted"; then
        echo "PASS: switch on ($case) == switch off"
    else
        echo "FAIL: switch on ($case) differs from switch off"
        fail=1
    fi
done

dirs_after=$(find "$SPIKE_REACTOR_DIR" -mindepth 1 -maxdepth 1 -name 'fs_*' | wc -l)
if [ "$dirs_before" = "$dirs_after" ]; then
    echo "PASS: no reactor directory leaked ($dirs_after present, unchanged)"
else
    echo "FAIL: reactor directories went from $dirs_before to $dirs_after"
    fail=1
fi

exit "$fail"
