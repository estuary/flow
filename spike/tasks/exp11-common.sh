# Shared driver for experiments 11, 12 and 13: storage bounds, what the guest can
# reach, and what a crash leaves behind. Sourced, never executed.
#
# Named for experiment 11 because WP10's paths are `exp{11,12,13}-*.sh`; exp12
# and exp13 source it too. Fold it into a plainer name whenever these three are
# next touched together, as exp6-common.sh and preview-common.sh are waiting to
# be.
#
# All three experiments make their assertions on the HOST while the helper is
# still up: the podman layer, the cgroup, the tap and the nft rules all die with
# the container, so a measurement taken after it exits measures nothing. The
# workload therefore says when it is done on stderr and then idles, and these
# helpers wait for that marker.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/helper-common.sh"

# busybox, not a connector image: experiments 11 and 12 are about what libkrun
# and podman do with bytes and devices, and busybox boots in a fraction of the
# time with a shell, dd, lsblk and a `/usr` to write into. Experiment 13 needs a
# real connector and overrides this.
XP_GUEST_IMAGE="${XP_GUEST_IMAGE:-docker.io/library/busybox:latest}"

# PLAN "Helper launch" defaults, as the runtime's own switch sets them
# (CONTRACTS "runtime-next spike switch"). The cgroup limit matters here in a
# way it does not in the other experiments' harnesses: experiment 11's claim is
# about where bytes land, and an unlimited cgroup would let page cache grow
# without ever being asked to give it back.
XP_MEMORY_MIB=1024
XP_OVERHEAD_MIB=256
XP_VCPUS=2
XP_DISK_MIB=1024

# The workload's "I am finished" line, and how long it idles afterwards so the
# host can measure the running container.
XP_DONE_MARKER=xp-finished
XP_DRAIN_SECS=45

# Extra arguments for the podman run, set per pass.
XP_PODMAN_ARGS=()

XP_WORK="$(mktemp -d)"
XP_FAILURES=0
XP_SANDBOXES="$XP_WORK/sandboxes"
: >"$XP_SANDBOXES"

say()  { printf '%s\n' "$*"; }
ok()   { printf 'ok    %s\n' "$*"; }
fail() { printf 'FAIL  %s\n' "$*"; XP_FAILURES=$((XP_FAILURES + 1)); }
step() { printf '\n== %s\n' "$*"; }
note() { printf '      %s\n' "$*"; }

# Reads the id file rather than an array, as helper-smoke.sh does: callers invoke
# xp_new_sandbox through a command substitution, so anything it appends to a
# shell variable dies with that subshell.
xp_cleanup() {
    local id
    while read -r id; do
        [ -n "$id" ] || continue
        sudo podman rm -f "$id" >/dev/null 2>&1 || true
        sudo rm -rf "${SPIKE_REACTOR_DIR:?}/$id"
    done <"$XP_SANDBOXES"
    rm -rf "$XP_WORK"
}

xp_finish() {
    say ""
    if [ "$XP_FAILURES" -eq 0 ]; then
        say "$1: ok"
    else
        say "$1: $XP_FAILURES failure(s)"
    fi
    exit $((XP_FAILURES > 0))
}

# ---------------------------------------------------------------- the sandbox

# Plays the runtime: the per-connector directory of CONTRACTS "Paths and names".
# Echoes the id.
xp_new_sandbox() {
    local id dir
    id="fs_$(head -c8 /dev/urandom | od -An -tx1 | tr -d ' \n')"
    dir="$SPIKE_REACTOR_DIR/$id"

    sudo mkdir -p "$dir/init" "$dir/sock" "$dir/scratch" "$dir/venv/spike"
    sudo podman inspect "$XP_GUEST_IMAGE" | sudo tee "$dir/init/image-inspect.json" >/dev/null
    printf '%s\n' '{"egress":"none","allowAll":false,"declaredCidrs":[],"connectionsPerMinute":null,"distinctDestinationsPerMinute":null,"ttlFloorSecs":90,"ttlCapSecs":3600}' \
        | sudo tee "$dir/init/policy.json" >/dev/null

    # The shim opens flow-connector-init even when --exec replaces the workload,
    # so a file has to be there; nothing execs it in these experiments.
    printf '#!/bin/sh\nexit 125\n' | sudo tee "$dir/init/flow-connector-init" >/dev/null
    sudo chmod 755 "$dir/init/flow-connector-init"
    sudo chmod 644 "$dir/init/image-inspect.json" "$dir/init/policy.json"

    if [ -n "${XP_VENV_FILE:-}" ]; then
        sudo cp "$XP_VENV_FILE" "$dir/venv/spike/$(basename "$XP_VENV_FILE")"
        sudo chmod 644 "$dir/venv/spike/$(basename "$XP_VENV_FILE")"
    fi

    printf '%s\n' "$id" >>"$XP_SANDBOXES"
    printf '%s' "$id"
}

# PLAN "Helper launch", minus the labels and cgroup parent that only matter to
# the reactor. XP_PODMAN_ARGS and XP_HELPER_ARGS are set per pass.
xp_helper_argv() {
    local id="$1"
    shift
    local dir="$SPIKE_REACTOR_DIR/$id"
    printf '%s\n' \
        run --rm "--name=$id" "--network=$SPIKE_NET_CONNECTORS" --log-driver=none \
        --device /dev/kvm --device /dev/net/tun --cap-add NET_ADMIN \
        "${SPIKE_HELPER_SYSCTLS[@]}" \
        --memory "$((XP_MEMORY_MIB + XP_OVERHEAD_MIB))m" --cpus "$XP_VCPUS" \
        "${XP_PODMAN_ARGS[@]}" \
        "$(spike_image_mount "$XP_GUEST_IMAGE")" \
        "--mount=type=bind,source=$dir/init,target=/init,ro" \
        "--mount=type=bind,source=$dir/venv,target=/venv,ro" \
        "--mount=type=bind,source=$dir/sock,target=/sock" \
        "--mount=type=bind,source=$dir/scratch,target=/scratch-backing" \
        "$SPIKE_HELPER_IMAGE" \
        --policy /init/policy.json \
        --memory-mib "$XP_MEMORY_MIB" --vcpus "$XP_VCPUS" --disk-mib "$XP_DISK_MIB" \
        "$@"
}

# Launches the helper in the background through the fake reactor, splitting its
# output into $XP_WORK/<tag>.{out,err}. Sets XP_PID rather than echoing it: a
# command substitution would put the job in a subshell, and `wait` in the caller
# would then refuse a pid that is not its own child - so the caller would go on
# to measure the container while podman was still tearing it down.
xp_start() {
    local tag="$1" id="$2"
    shift 2
    local argv=()
    mapfile -t argv < <(xp_helper_argv "$id" "$@")
    "$SPIKE_TASKS_DIR/fake-reactor.sh" podman "${argv[@]}" \
        >"$XP_WORK/$tag.out" 2>"$XP_WORK/$tag.err" &
    XP_PID=$!
}

# Runs the helper to completion and returns its exit code.
xp_run() {
    local tag="$1" id="$2" rc=0
    shift 2
    local argv=()
    mapfile -t argv < <(xp_helper_argv "$id" "$@")
    set +e
    "$SPIKE_TASKS_DIR/fake-reactor.sh" podman "${argv[@]}" \
        >"$XP_WORK/$tag.out" 2>"$XP_WORK/$tag.err"
    rc=$?
    set -e
    return $rc
}

xp_await_container() {
    local id="$1" _
    for _ in $(seq 300); do
        sudo podman exec "$id" true >/dev/null 2>&1 && return 0
        sleep 0.2
    done
    return 1
}

# The marker goes to the workload's stderr, which CONTRACTS gives us byte for
# byte; stdout carries the kernel console interleaved and cannot be parsed.
xp_await_marker() {
    local tag="$1" pid="$2" _
    for _ in $(seq 1200); do
        grep -q "^$XP_DONE_MARKER" "$XP_WORK/$tag.err" 2>/dev/null && return 0
        # The guest idles for XP_DRAIN_SECS after the marker, so a helper that
        # has already exited will never write one.
        kill -0 "$pid" 2>/dev/null || return 1
        sleep 0.5
    done
    return 1
}

# The shell the guest runs for a pass that has to be measured while it is up:
# do the work, say so, then idle. One line, because xp_helper_argv prints one
# argument per line and mapfile reads them back.
xp_guest_script() {
    printf '%s' "rc=0; { $1 ; } || rc=\$?; printf '%s rc=%s\\n' $XP_DONE_MARKER \"\$rc\" >&2; sleep $XP_DRAIN_SECS; exit \$rc"
}

# ---------------------------------------------------------------- host probes

xp_cid() { sudo podman inspect "$1" --format '{{.Id}}'; }

# podman's per-container writable layer for the `--mount type=image,rw=true`
# root. `podman inspect` reports the helper container's OWN graph driver, not
# this overlay, and names the mount only by image reference - so the path is
# derived from the container id instead. Verified against the helper's
# /proc/mounts entry for /rootfs by exp11.
xp_layer_dir() {
    sudo bash -c "ls -d /var/lib/containers/storage/overlay-containers/$1/userdata/overlay/*/upper 2>/dev/null" | head -1
}

xp_layer_kib() {
    local dir
    dir="$(xp_layer_dir "$1")"
    [ -n "$dir" ] || { printf '0'; return; }
    sudo du -sk "$dir" | cut -f1
}

xp_cgroup() { printf '/sys/fs/cgroup%s' "$(sudo podman inspect "$1" --format '{{.State.CgroupPath}}')"; }

xp_memory_current() { sudo cat "$(xp_cgroup "$1")/memory.current" 2>/dev/null || printf '0'; }

# One field of the helper cgroup's memory.stat, in bytes. `anon` is the guest's
# RAM and the VMM's own allocations; `file` is page cache, which the kernel will
# hand back under pressure. Experiment 11's claim is about the first.
xp_memory_stat() {
    sudo awk -v k="$2" '$1==k {print $2}' "$(xp_cgroup "$1")/memory.stat" 2>/dev/null || printf '0'
}

# KiB in use on the filesystem backing the reactor directory. The scratch disk is
# an O_TMPFILE, so it shows up here and nowhere in the directory listing.
xp_fs_used() { df -k --output=used "$SPIKE_REACTOR_DIR" | tail -1 | tr -d ' '; }

xp_mem_available_kib() { awk '/^MemAvailable:/ {print $2}' /proc/meminfo; }

xp_kib() { printf '%s' "$(( $1 / 1024 ))"; }
