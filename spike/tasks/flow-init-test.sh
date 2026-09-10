#!/usr/bin/env bash
# Prove flow-init gives a connector image what podman gives a container today:
# a default route, a writable root (podman's own per-container layer, served
# read-write over virtiofs), the venv, scratch and deps mounts, the image's
# user and environment, and a shell's exit codes.
#
#   flow-init-test.sh            launch through the fake reactor (as production would)
#   flow-init-test.sh --host     launch with sudo podman directly on the host
#
# The guest is derive-python, which unlike busybox has a shell, a non-root
# `User` and a `PATH` worth checking.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/helper-common.sh"

LAUNCH=reactor
if [ "${1:-}" = "--host" ]; then
    LAUNCH=host
elif [ $# -ne 0 ]; then
    echo "usage: flow-init-test.sh [--host]" >&2
    exit 2
fi

GUEST_IMAGE=ghcr.io/estuary/derive-python:dev
VENV_MARKER=venv-marker
DEPS_MARKER=deps-marker
WORK="$(mktemp -d)"
CONNECTORS="$WORK/connectors"
: >"$CONNECTORS"
FAILURES=0

# Set per case by run_helper's callers: the host path of the deps disk image to
# bind at /deps.img, or empty for the launches that must look exactly as they
# did before --deps-image existed.
DEPS_IMAGE=""

# Reads the id file rather than an array: callers invoke new_connector through
# a command substitution, so anything it appends to a shell variable dies with
# that subshell and the trap would have nothing to clean.
cleanup() {
    local id
    while read -r id; do
        [ -n "$id" ] || continue
        sudo podman rm -f "$id" >/dev/null 2>&1 || true
        sudo rm -rf "${SPIKE_REACTOR_DIR:?}/$id"
    done <"$CONNECTORS"
    rm -rf "$WORK"
}
trap cleanup EXIT

say()  { printf '%s\n' "$*"; }
indent() { sed 's/^/      /' "$1"; }
ok()   { printf 'ok    %s\n' "$*"; }
fail() { printf 'FAIL  %s\n' "$*"; FAILURES=$((FAILURES + 1)); }

# Asserts an extended regex against a file, naming what it proves.
matches() {
    local what="$1" file="$2" pattern="$3"
    if grep -qE "$pattern" "$file"; then
        ok "$what"
    else
        fail "$what (no /$pattern/ in $(basename "$file"))"
    fi
}

# Plays the runtime, as WP03's helper-smoke.sh does: a per-connector directory
# with connector-init and the image inspect under init/, an empty sock/ and
# scratch/, the egress policy, and a marked file in venv/ so the guest can tell
# the share apart from an empty mount point.
new_connector() {
    local id dir
    id="fs_$(head -c8 /dev/urandom | od -An -tx1 | tr -d ' \n')"
    dir="$SPIKE_REACTOR_DIR/$id"

    sudo mkdir -p "$dir/init" "$dir/sock" "$dir/scratch" "$dir/venv"
    sudo podman inspect "$GUEST_IMAGE" | sudo tee "$dir/init/image-inspect.json" >/dev/null
    sudo touch "$dir/venv/$VENV_MARKER"

    # WP01 has not landed, so connector-init stands in as a witness for the
    # argv and exit code the default workload path carries.
    printf '#!/bin/sh\necho "connector-init argv: $*" >&2\nexit 42\n' \
        | sudo tee "$dir/init/flow-connector-init" >/dev/null
    printf '%s\n' '{"egress":"public","allowAll":false,"declaredCidrs":[],"connectionsPerMinute":null,"distinctDestinationsPerMinute":null,"ttlFloorSecs":90,"ttlCapSecs":3600}' \
        | sudo tee "$dir/init/policy.json" >/dev/null

    printf '%s\n' "$id" >>"$CONNECTORS"
    printf '%s' "$id"
}

# A read-only disk image holding one marked file, built on the host the way a
# per-tag dependency image would be. Small and self-contained: the standing
# suite must not depend on experiment 5's build outputs.
make_deps_image() {
    local fstype="$1" content="$WORK/deps-content" image="$WORK/deps.$1"
    mkdir -p "$content"
    printf 'deps\n' >"$content/$DEPS_MARKER"
    # stdout goes nowhere: mkfs.ext4 -d announces "Creating regular file ..."
    # even under -q, and the caller reads this function's stdout as the path.
    case "$fstype" in
        ext4)  mkfs.ext4 -d "$content" -O ^has_journal -m 0 -q -F "$image" 16m >/dev/null ;;
        erofs) mkfs.erofs --quiet "$image" "$content" >/dev/null ;;
    esac
    printf '%s' "$image"
}

# The podman run of PLAN "Helper launch", minus the labels and cgroup parent
# that only matter to the reactor.
helper_run_argv() {
    local id="$1"
    shift
    local dir="$SPIKE_REACTOR_DIR/$id" deps=()
    [ -n "$DEPS_IMAGE" ] && deps=("--mount=type=bind,source=$DEPS_IMAGE,target=/deps.img,ro")
    printf '%s\n' \
        run --rm "--name=$id" "--network=$SPIKE_NET_CONNECTORS" --log-driver=none \
        --device /dev/kvm --device /dev/net/tun --cap-add NET_ADMIN \
        --sysctl net.ipv4.ip_forward=1 \
        "$(spike_image_mount "$GUEST_IMAGE")" \
        "--mount=type=bind,source=$dir/init,target=/init,ro" \
        "--mount=type=bind,source=$dir/venv,target=/venv,ro" \
        "--mount=type=bind,source=$dir/sock,target=/sock" \
        "--mount=type=bind,source=$dir/scratch,target=/scratch-backing" \
        "${deps[@]}" \
        "$SPIKE_HELPER_IMAGE" \
        --policy /init/policy.json --memory-mib 1024 --vcpus 2 --disk-mib 1024 \
        "$@"
}

# Runs the helper and returns its exit code, with stdout and stderr split into
# $WORK/<tag>.out and $WORK/<tag>.err. Every argv element must be one line:
# mapfile splits on newlines.
run_helper() {
    local tag="$1" id="$2"
    shift 2
    local argv=()
    mapfile -t argv < <(helper_run_argv "$id" "$@")

    set +e
    if [ "$LAUNCH" = host ]; then
        sudo podman "${argv[@]}" >"$WORK/$tag.out" 2>"$WORK/$tag.err"
    else
        "$SPIKE_TASKS_DIR/fake-reactor.sh" podman "${argv[@]}" >"$WORK/$tag.out" 2>"$WORK/$tag.err"
    fi
    local rc=$?
    set -e
    return $rc
}

say "flow-init-test.sh: launching via $LAUNCH, guest $GUEST_IMAGE"
say ""

# ------------------------------------------------------------------ the guest
# One line, because each argv element must be: network, mounts, user,
# writability and environment, all as the connector would find them.
PROBE='cat /proc/net/route; cat /proc/mounts; id; cat /etc/resolv.conf; cat /etc/hosts;
echo "ipv6-disabled=$(cat /proc/sys/net/ipv6/conf/all/disable_ipv6)";
touch /tmp/x && echo tmp-writable;
touch /etc/x 2>/dev/null && echo etc-writable-as-user || echo etc-denied-as-user;
touch /venv/x 2>/dev/null || echo venv-ro;
touch /scratch/x && echo scratch-writable; df /scratch; ls /venv;
env | grep -E "^(TMPDIR|UV_CACHE_DIR|PATH)="'
PROBE="${PROBE//$'\n'/ }"

id="$(new_connector)"
rc=0
run_helper probe "$id" --exec /bin/sh -c "$PROBE" || rc=$?
if [ "$rc" -ne 0 ]; then
    fail "probe: the helper exited $rc"
    indent "$WORK/probe.err"
else
    # /proc/net/route prints little-endian hex: 192.0.2.1 is 010200C0, and the
    # default route is the one with destination 00000000.
    matches "route: default via 192.0.2.1 on eth0" \
        "$WORK/probe.out" '^eth0[[:space:]]+00000000[[:space:]]+010200C0'
    matches "root: / is the image share, read-write" \
        "$WORK/probe.out" '^/dev/root / virtiofs rw,'
    matches "root: the image's user writes to the root" \
        "$WORK/probe.out" '^tmp-writable$'
    # Parity with podman, not a flow-init limit: /etc belongs to root in the
    # image, and the workload is the image's unprivileged user either way.
    matches "root: /etc stays root-owned under the dropped uid" \
        "$WORK/probe.out" '^etc-denied-as-user$'
    matches "mounts: venv virtiofs at /venv, read-only" \
        "$WORK/probe.out" '^venv /venv virtiofs ro,'
    matches "mounts: /venv holds the share, not an empty mount point" \
        "$WORK/probe.out" "^$VENV_MARKER\$"
    matches "mounts: /venv rejects a write" "$WORK/probe.out" '^venv-ro$'
    matches "mounts: /dev/vda ext4 at /scratch, read-write" \
        "$WORK/probe.out" '^/dev/vda /scratch ext4 rw,'
    matches "mounts: /scratch takes the workload's writes" \
        "$WORK/probe.out" '^scratch-writable$'
    matches "mounts: df sees the scratch disk" "$WORK/probe.out" '^/dev/vda .* /scratch$'
    matches "mounts: devtmpfs, devpts and shm under /dev" \
        "$WORK/probe.out" '^devpts /dev/pts devpts '
    matches "user: the image's uid:gid, no supplementary groups" \
        "$WORK/probe.out" '^uid=65534\(nobody\) gid=65534\(nogroup\) groups=65534\(nogroup\)$'
    matches "resolution: nameserver is the helper" \
        "$WORK/probe.out" '^nameserver 192\.0\.2\.1$'
    matches "resolution: /etc/hosts names localhost" \
        "$WORK/probe.out" '^127\.0\.0\.1 localhost$'
    matches "network: IPv6 is disabled" "$WORK/probe.out" '^ipv6-disabled=1$'
    matches "env: TMPDIR is the scratch disk" "$WORK/probe.out" '^TMPDIR=/scratch$'
    matches "env: UV_CACHE_DIR overrides the image's value" \
        "$WORK/probe.out" '^UV_CACHE_DIR=/scratch$'

    # The image's own PATH, read from the same inspect the shim reads.
    image_path="$(sudo podman inspect "$GUEST_IMAGE" \
        --format '{{range .Config.Env}}{{println .}}{{end}}' | grep '^PATH=')"
    if grep -qxF "$image_path" "$WORK/probe.out"; then
        ok "env: PATH is the image's ($image_path)"
    else
        fail "env: PATH is not the image's ($image_path)"
    fi

    if grep -q '^ ' "$WORK/probe.err"; then
        fail "stderr: a line begins with a space"
    else
        ok "stderr: no line begins with a space"
    fi
fi
say "      guest probe output:"
indent "$WORK/probe.out"
say "      helper stderr:"
indent "$WORK/probe.err"

# ------------------------------------ --run-as-root, and where root writes go
# Guest root writes land in podman's per-container layer: they succeed, they
# never reach the image, and they leave with the container (PLAN experiment 11).
GUEST_WRITE=flow-init-test-write

# Containers row of `podman system df`: how many, and how many bytes of
# per-container layer. The helper's layer must be gone once it exits.
df_containers() {
    sudo podman system df --format json | python3 -c '
import json, sys
row = next(r for r in json.load(sys.stdin) if r["Type"] == "Containers")
print(row["TotalCount"], row["RawSize"])'
}

read -r df_count_before df_bytes_before < <(df_containers)
id="$(new_connector)"
rc=0
# One line: run_helper's mapfile splits every argv element on newlines.
asroot_probe="id; touch /etc/$GUEST_WRITE && echo etc-writable;"
asroot_probe="$asroot_probe touch /usr/$GUEST_WRITE && echo usr-writable"
run_helper asroot "$id" --run-as-root --exec /bin/sh -c "$asroot_probe" || rc=$?
if [ "$rc" -ne 0 ]; then
    fail "--run-as-root: the helper exited $rc"
    indent "$WORK/asroot.err"
else
    matches "--run-as-root: the workload keeps uid 0" "$WORK/asroot.out" '^uid=0\(root\)'
    matches "root: as guest root, /etc takes a write" "$WORK/asroot.out" '^etc-writable$'
    matches "root: as guest root, /usr takes a write" "$WORK/asroot.out" '^usr-writable$'
fi

# The image itself, read from podman's own storage.
image_root="$(sudo podman image mount "$GUEST_IMAGE")"
if sudo test -e "$image_root/etc/$GUEST_WRITE" || sudo test -e "$image_root/usr/$GUEST_WRITE"; then
    fail "root: the guest's writes reached the image at $image_root"
else
    ok "root: the guest's writes are not in the image"
fi
sudo podman image umount "$GUEST_IMAGE" >/dev/null

read -r df_count_after df_bytes_after < <(df_containers)
say "      podman system df, Containers: $df_count_before/$df_bytes_before B before, $df_count_after/$df_bytes_after B after"
# The byte total has a tolerance because every other container on the box (the
# spike's nginx) counts toward it too; the count must match exactly.
if [ "$df_count_after" -ne "$df_count_before" ]; then
    fail "root: $((df_count_after - df_count_before)) container(s) left behind"
elif [ "$((df_bytes_after - df_bytes_before))" -ge 1048576 ]; then
    fail "root: $((df_bytes_after - df_bytes_before)) B of container layer survived the run"
else
    ok "root: the writable layer left with the container"
fi

# ------------------------------------------------------------- --as-root-exec
# The shim's passthrough of flow-init's pre-drop hook, which WP07's root probes
# need: root before the drop, the image's user after it.
id="$(new_connector)"
rc=0
run_helper hook "$id" \
    --as-root-exec 'echo "as-root-exec uid=$(id -u) writable=$(touch /scratch/hook && echo yes)" >&2' \
    --exec /bin/sh -c 'echo "workload uid=$(id -u)"' || rc=$?
if [ "$rc" -ne 0 ]; then
    fail "--as-root-exec: the helper exited $rc"
    indent "$WORK/hook.err"
else
    matches "--as-root-exec: runs as guest root, after the mounts" \
        "$WORK/hook.err" '^as-root-exec uid=0 writable=yes$'
    matches "--as-root-exec: the workload still drops to the image's user" \
        "$WORK/hook.out" '^workload uid=65534$'
fi

# ---------------------------------------------------------- the default argv
# No --exec: flow-init must exec the connector-init argv of CONTRACTS "Helper
# CLI", and its exit code must come back through libkrun and the helper.
id="$(new_connector)"
rc=0
run_helper default "$id" || rc=$?
if [ "$rc" -eq 42 ]; then
    ok "default workload: connector-init's exit code reached the caller"
else
    fail "default workload: expected 42 from connector-init, got $rc"
    indent "$WORK/default.err"
fi
matches "default workload: connector-init got its image-inspect and vsock port" \
    "$WORK/default.err" \
    '^connector-init argv: --image-inspect-json-path=/image-inspect\.json --vsock-port=49092$'

# ----------------------------------------------------------------- --venv-dax
# The variant experiment 5 measures: the same share with a DAX window.
id="$(new_connector)"
rc=0
run_helper dax "$id" --venv-dax --exec /bin/sh -c \
    "grep venv /proc/mounts; cat /venv/$VENV_MARKER && echo dax-read-ok" || rc=$?
if [ "$rc" -ne 0 ]; then
    fail "--venv-dax: the helper exited $rc"
    indent "$WORK/dax.err"
else
    matches "--venv-dax: /venv is mounted with dax" "$WORK/dax.out" '^venv /venv virtiofs ro,relatime,dax=always'
    matches "--venv-dax: the share reads through the DAX window" "$WORK/dax.out" '^dax-read-ok$'
fi

# ---------------------------------------------------------------- --deps-image
# The per-tag dependency disk of CONTRACTS "Helper CLI": scratch keeps /dev/vda,
# deps is /dev/vdb, and flow-init mounts it read-only at /opt/venv.
for fstype in ext4 erofs; do
    DEPS_IMAGE="$(make_deps_image "$fstype")"
    id="$(new_connector)"
    rc=0
    deps_args=(--deps-image /deps.img)
    [ "$fstype" = ext4 ] || deps_args+=(--deps-fstype "$fstype")
    # One line: run_helper's mapfile splits every argv element on newlines.
    deps_probe="cat /proc/partitions; grep -E ' /scratch | /opt/venv ' /proc/mounts;"
    deps_probe="$deps_probe cat /opt/venv/$DEPS_MARKER;"
    deps_probe="$deps_probe touch /opt/venv/x 2>/dev/null || echo deps-ro"
    run_helper "deps-$fstype" "$id" "${deps_args[@]}" --exec /bin/sh -c \
        "$deps_probe" || rc=$?
    if [ "$rc" -ne 0 ]; then
        fail "--deps-image $fstype: the helper exited $rc"
        indent "$WORK/deps-$fstype.err"
    else
        matches "--deps-image $fstype: scratch is still /dev/vda" \
            "$WORK/deps-$fstype.out" '^/dev/vda /scratch ext4 rw,'
        matches "--deps-image $fstype: the deps disk arrived as vdb" \
            "$WORK/deps-$fstype.out" '^ *254 *16 .* vdb$'
        matches "--deps-image $fstype: mounted read-only at /opt/venv" \
            "$WORK/deps-$fstype.out" "^/dev/vdb /opt/venv $fstype ro,"
        matches "--deps-image $fstype: the workload reads the image's content" \
            "$WORK/deps-$fstype.out" '^deps$'
        matches "--deps-image $fstype: /opt/venv rejects a write" \
            "$WORK/deps-$fstype.out" '^deps-ro$'
    fi
    DEPS_IMAGE=""
done

# A bad filesystem name must fail before the VM starts, not as an opaque mount
# error inside the guest.
DEPS_IMAGE="$(make_deps_image ext4)"
id="$(new_connector)"
rc=0
run_helper depsbad "$id" --deps-image /deps.img --deps-fstype btrfs \
    --exec /bin/true || rc=$?
if [ "$rc" -eq 2 ]; then
    ok "--deps-fstype: an unsupported filesystem is refused before the VM starts"
else
    fail "--deps-fstype: expected exit 2 for an unsupported filesystem, got $rc"
    indent "$WORK/depsbad.err"
fi
DEPS_IMAGE=""

# ------------------------------------------------------------------ exit codes
id="$(new_connector)"
rc=0
run_helper missing "$id" --exec /nonexistent || rc=$?
if [ "$rc" -eq 127 ]; then
    ok "exit code: a workload that is not there exits 127"
else
    fail "exit code: expected 127 for a missing workload, got $rc"
    indent "$WORK/missing.err"
fi
matches "exit code: the failure is one line on stderr" \
    "$WORK/missing.err" '^flow-init: exec "/nonexistent": No such file or directory'

say ""
if [ "$FAILURES" -eq 0 ]; then
    say "flow-init-test.sh: ok"
else
    say "flow-init-test.sh: $FAILURES failure(s)"
fi
exit $((FAILURES > 0))
