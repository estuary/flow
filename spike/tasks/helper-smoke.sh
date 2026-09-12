#!/usr/bin/env bash
# Smoke the helper against a busybox guest: does a connector image boot as a
# libkrun VM with the devices, console wiring, vsock mapping and exit-code path
# the design needs?
#
#   helper-smoke.sh            launch through the fake reactor (as production would)
#   helper-smoke.sh --host     launch with sudo podman directly on the host
#
# Assertions are made here on the host either way. The reactor directory is
# bind-mounted at the same path in both, so the paths below are identical.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/helper-common.sh"

LAUNCH=reactor
if [ "${1:-}" = "--host" ]; then
    LAUNCH=host
elif [ $# -ne 0 ]; then
    echo "usage: helper-smoke.sh [--host]" >&2
    exit 2
fi

GUEST_IMAGE=docker.io/library/busybox:latest
WORK="$(mktemp -d)"
CONNECTORS="$WORK/connectors"
: >"$CONNECTORS"
FAILURES=0

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

# Plays the runtime: a per-connector directory named as CONTRACTS names helper
# containers, holding connector-init and the image inspect under init/, an empty
# sock/ and scratch/, and the egress policy. Echoes the id.
new_connector() {
    local id dir
    id="fs_$(head -c8 /dev/urandom | od -An -tx1 | tr -d ' \n')"
    dir="$SPIKE_REACTOR_DIR/$id"

    sudo mkdir -p "$dir/init" "$dir/sock" "$dir/scratch" "$dir/venv"
    sudo podman inspect "$GUEST_IMAGE" | sudo tee "$dir/init/image-inspect.json" >/dev/null

    # WP01 has not landed and no --no-flow-init case execs it; the shim only
    # needs a file to inject.
    printf '#!/bin/sh\necho "flow-connector-init placeholder" >&2\nexit 125\n' \
        | sudo tee "$dir/init/flow-connector-init" >/dev/null
    printf '%s\n' '{"egress":"public","allowAll":false,"declaredCidrs":[],"connectionsPerMinute":null,"distinctDestinationsPerMinute":null,"ttlFloorSecs":90,"ttlCapSecs":3600}' \
        | sudo tee "$dir/init/policy.json" >/dev/null

    printf '%s\n' "$id" >>"$CONNECTORS"
    printf '%s' "$id"
}

# The podman run of PLAN "Helper launch", minus the labels and cgroup parent
# that only matter to the reactor.
helper_run_argv() {
    local id="$1"
    shift
    local dir="$SPIKE_REACTOR_DIR/$id"
    printf '%s\n' \
        run --rm "--name=$id" "--network=$SPIKE_NET_CONNECTORS" --log-driver=none \
        --device /dev/kvm --device /dev/net/tun --cap-add NET_ADMIN \
        "${SPIKE_HELPER_SYSCTLS[@]}" \
        "$(spike_image_mount "$GUEST_IMAGE")" \
        "--mount=type=bind,source=$dir/init,target=/init,ro" \
        "--mount=type=bind,source=$dir/venv,target=/venv,ro" \
        "--mount=type=bind,source=$dir/sock,target=/sock" \
        "--mount=type=bind,source=$dir/scratch,target=/scratch-backing" \
        "$SPIKE_HELPER_IMAGE" \
        --policy /init/policy.json --memory-mib 1024 --vcpus 2 --disk-mib 1024 \
        "$@"
}

# Runs the helper and returns its exit code, with stdout and stderr split into
# $WORK/<tag>.out and $WORK/<tag>.err.
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

await_container() {
    local id="$1" _
    for _ in $(seq 150); do
        sudo podman exec "$id" true >/dev/null 2>&1 && return 0
        sleep 0.2
    done
    return 1
}

# KiB in use on the filesystem backing the reactor directory. The scratch disk
# is an O_TMPFILE, so it shows up here and nowhere in the directory listing.
fs_used() { df -k --output=used "$SPIKE_REACTOR_DIR" | tail -1 | tr -d ' '; }

# Connects to the vsock unix socket the way the reactor's tonic client will,
# retrying until libkrun has created it and the guest is listening. socat ships
# in the helper image but not on this host, so python3 stands in; libkrun
# creates the socket as root, which the reactor also is.
unix_echo() {
    sudo python3 - "$1" "$2" <<'PY'
import socket, sys, time
path, payload = sys.argv[1], sys.argv[2].encode()
deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect(path)
            # No shutdown(SHUT_WR): libkrun's vsock unix proxy reads EOF on
            # the host side as a full close and resets the connection, so a
            # half-close would never see the echo.
            s.sendall(payload)
            data = b""
            while len(data) < len(payload):
                chunk = s.recv(4096)
                if not chunk:
                    break
                data += chunk
        sys.stdout.write(data.decode(errors="replace"))
        sys.exit(0)
    except (FileNotFoundError, ConnectionRefusedError, ConnectionResetError):
        time.sleep(0.2)
sys.exit("no echo within 30s")
PY
}

say "helper-smoke.sh: launching via $LAUNCH, guest $GUEST_IMAGE"
say ""

# ---------------------------------------------------------------- vsock echo
id="$(new_connector)"
run_helper vsock "$id" --no-flow-init --exec /vsock-echo &
helper_pid=$!
reply="$(unix_echo "$SPIKE_REACTOR_DIR/$id/sock/init.sock" 'hi' || true)"
if [ "$reply" = "hi" ]; then
    ok "vsock: guest port 49092 echoes through $id/sock/init.sock"
else
    fail "vsock: expected 'hi', got '${reply:-<none>}'"
    indent "$WORK/vsock.err"
fi
sudo podman kill "$id" >/dev/null 2>&1 || true
wait "$helper_pid" || true

# ---------------------------------------------------------------- exit code
id="$(new_connector)"
rc=0
run_helper exit7 "$id" --no-flow-init --exec /bin/sh -c 'exit 7' || rc=$?
if [ "$rc" -eq 7 ]; then
    ok "exit code: the guest workload's 7 reached the helper's caller"
else
    fail "exit code: expected 7, got $rc"
    indent "$WORK/exit7.err"
fi

# ---------------------------------------------------------------- devices
id="$(new_connector)"
rc=0
run_helper devices "$id" --no-flow-init --exec /bin/sh -c \
    'cat /proc/partitions; echo ---; for d in /sys/bus/virtio/devices/virtio*; do printf "%s %s\n" "${d##*/}" "$(cat "$d/modalias")"; done' \
    || rc=$?
if [ "$rc" -ne 0 ]; then
    fail "devices: helper exited $rc"
    indent "$WORK/devices.err"
else
    if grep -q ' vda$' "$WORK/devices.out"; then
        ok "devices: vda present in /proc/partitions"
    else
        fail "devices: no vda in /proc/partitions"
    fi
    # Modalias device ids, from the virtio spec: 1 net, 2 blk, 3 console,
    # 4 rng, 5 balloon, 0x13 vsock, 0x1a fs. Two fs devices: the root and the
    # venv. The kernel prints the id in upper case; match either.
    missing=()
    for want in 00000001 00000002 00000003 00000004 00000005 00000013 0000001a; do
        grep -qi "virtio:d${want}v" "$WORK/devices.out" || missing+=("$want")
    done
    if [ ${#missing[@]} -eq 0 ]; then
        ok "devices: fs, blk, net, vsock, console, balloon and rng all on the bus"
    else
        fail "devices: no virtio device with id(s) ${missing[*]}"
    fi
fi
say "      guest /proc/partitions and virtio bus:"
indent "$WORK/devices.out"

# ---------------------------------------------------------------- stdio split
id="$(new_connector)"
rc=0
run_helper stdio "$id" --no-flow-init --exec /bin/sh -c 'echo err >&2; echo out' || rc=$?
if grep -qx 'err' "$WORK/stdio.err"; then
    ok "stdio: the workload's stderr reached the helper's fd 2"
else
    fail "stdio: 'err' not on the helper's stderr"
fi
if grep -qx 'out' "$WORK/stdio.out"; then
    ok "stdio: the workload's stdout landed on fd 1, alongside the kernel console"
elif grep -qx 'out' "$WORK/stdio.err"; then
    fail "stdio: the workload's stdout landed on fd 2, not fd 1"
else
    fail "stdio: 'out' appeared on neither descriptor"
fi
if grep -q '^ ' "$WORK/stdio.err"; then
    fail "stdio: a line on the helper's stderr begins with a space"
else
    ok "stdio: no line on the helper's stderr begins with a space"
fi
say "      helper stderr:"
indent "$WORK/stdio.err"

# ------------------------------------------------- helper interior and SIGKILL
id="$(new_connector)"
used_before="$(fs_used)"
run_helper interior "$id" --no-flow-init --exec /bin/sh -c 'sleep 60' &
helper_pid=$!
if await_container "$id"; then
    # Polled, not asserted once: await_container returns as soon as `podman exec`
    # works, and the container is executable from the moment podman starts the
    # shim - which is before the shim has exec'd flow-sandbox-egress. The window
    # is the ~30 ms between the helper's `start` and `krun_start_enter` timing
    # lines, and one exec in five lands inside it.
    loaded=0
    for _ in $(seq 50); do
        sudo podman exec "$id" nft list ruleset 2>/dev/null | grep -q 'table inet flow_sandbox' && { loaded=1; break; }
        sleep 0.2
    done
    if [ "$loaded" -eq 1 ]; then
        ok "helper: nft ruleset holds table inet flow_sandbox"
    else
        fail "helper: no 'table inet flow_sandbox' in nft list ruleset"
    fi
    if sudo podman exec "$id" ip -4 addr show tap0 2>/dev/null | grep -q 'inet 192.0.2.1/30'; then
        ok "helper: tap0 is 192.0.2.1/30"
    else
        fail "helper: tap0 is not 192.0.2.1/30"
    fi
else
    fail "helper: container $id never became executable"
    indent "$WORK/interior.err"
fi

used_running="$(fs_used)"
sudo podman kill -s KILL "$id" >/dev/null 2>&1 || true
wait "$helper_pid" || true
used_after="$(fs_used)"
entries="$(sudo ls -A "$SPIKE_REACTOR_DIR/$id/scratch" | wc -l)"
say "      df used KiB: before=$used_before running=$used_running after=$used_after"
if [ "$entries" -ne 0 ]; then
    fail "SIGKILL: $entries entries left under $id/scratch"
elif [ $((used_running - used_before)) -lt 256 ]; then
    # mkfs allocates ~664 KiB of a 1024 MiB image and leaves the rest sparse,
    # so this is a small delta by design - but it must not be zero, or the
    # check below proves nothing.
    fail "SIGKILL: the scratch disk never showed up in df; the check proves nothing"
elif [ $((used_after - used_before)) -ge $(((used_running - used_before) / 2)) ]; then
    fail "SIGKILL: $((used_after - used_before)) KiB still held after the kill"
else
    ok "SIGKILL: scratch dir empty and $((used_running - used_before)) KiB returned to the filesystem"
fi

say ""
if [ "$FAILURES" -eq 0 ]; then
    say "helper-smoke.sh: ok"
else
    say "helper-smoke.sh: $FAILURES failure(s)"
fi
exit $((FAILURES > 0))
