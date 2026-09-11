#!/usr/bin/env bash
# Experiment 1: launch through the podman API from the reactor's privilege
# level. Drives a real `flowctl preview` of the Go capture with the switch on,
# and while the helper is up asks podman and the kernel what it actually got.
#
# The gate is three claims:
#   - the helper starts, the guest boots, connector-init answers on
#     `<id>/sock/init.sock` (the documents are the proof),
#   - CapEff is podman's default set plus NET_ADMIN and nothing else,
#   - /dev/kvm and /dev/net/tun are the only added devices.
#
# Run under mise: mise exec -- spike/tasks/exp1-launch.sh
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/preview-common.sh"

SPEC=capture-hello-world.flow.yaml
POLICY=policy-egress-none.json

# Podman's default effective set, from PLAN "Inputs established before the
# spike", and the same value with CAP_NET_ADMIN (bit 12) added.
CAP_DEFAULT=00000000800405fb
CAP_EXPECTED=00000000800415fb

# One session long enough that the helper is still up when the inspection runs.
# `rate: 2` in the spec, so 40 documents is about twenty seconds.
SESSIONS=40

OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

step() { printf '\n== %s\n' "$*"; }
fails=0
ok() { printf 'ok    %s\n' "$*"; }
fail() {
    printf 'FAIL  %s\n' "$*"
    fails=$((fails + 1))
}

spike_stage_catalog

# The runtime creates `<id>` per launch and removes it after, so what this
# asserts is a delta: anything else standing in the directory is someone else's
# and is env-check.sh's business, not this experiment's.
dirs_before=$(sudo find "$SPIKE_REACTOR_DIR" -mindepth 1 -maxdepth 1 -name 'fs_*' | wc -l)

step "preview with the switch on"
# RUST_LOG=debug is what prints the emitted launch line. The preview runs in the
# background so the inspection can catch the helper while the guest is serving.
SPIKE_PREVIEW_GUEST_ENV=(RUST_LOG=runtime_next=debug)
spike_preview "$POLICY" "$SPEC" --sessions "$SESSIONS" \
    >"$OUT/docs.ndjson" 2>"$OUT/preview.log" &
preview=$!

# Wait for the helper container, then for the guest to answer: the documents
# only start once connector-init is serving, and a running container proves
# nothing on its own.
name=""
for _ in $(seq 1 300); do
    name=$(sudo podman ps --filter 'name=^fs_' --format '{{.Names}}' | head -1)
    [ -n "$name" ] && [ -s "$OUT/docs.ndjson" ] && break
    name=""
    sleep 0.2
done

if [ -z "$name" ]; then
    wait "$preview" || true
    echo "no helper container came up; preview log:" >&2
    tail -40 "$OUT/preview.log" >&2
    exit 2
fi
printf 'helper container %s\n' "$name"

step "podman inspect"
sudo podman inspect "$name" --format '
Privileged:  {{.HostConfig.Privileged}}
CapAdd:      {{.HostConfig.CapAdd}}
CapDrop:     {{.HostConfig.CapDrop}}
Devices:     {{.HostConfig.Devices}}
Memory:      {{.HostConfig.Memory}}
NanoCpus:    {{.HostConfig.NanoCpus}}' | sed '/^$/d' | tee "$OUT/inspect.txt"

shim_pid=$(sudo podman inspect "$name" --format '{{.State.Pid}}')

step "capabilities and devices"
caps=$(sudo grep CapEff "/proc/$shim_pid/status" | awk '{print $2}')
printf 'shim pid %s CapEff %s (default %s)\n' "$shim_pid" "$caps" "$CAP_DEFAULT"
[ "$caps" = "$CAP_EXPECTED" ] &&
    ok "CapEff is podman's default set plus NET_ADMIN" ||
    fail "CapEff is $caps, want $CAP_EXPECTED (default plus NET_ADMIN)"

devices=$(sudo podman inspect "$name" \
    --format '{{range .HostConfig.Devices}}{{.PathOnHost}} {{end}}')
printf 'added devices: %s\n' "${devices:-none}"
[ "$devices" = "/dev/kvm /dev/net/tun " ] &&
    ok "/dev/kvm and /dev/net/tun are the only added devices" ||
    fail "added devices are '${devices:-none}', want /dev/kvm and /dev/net/tun"

# What the shim can actually open, which is the question the flags are a proxy
# for: podman synthesizes a /dev of its own and the two devices are added to it.
step "ls /dev inside the helper"
sudo podman exec "$name" ls /dev | tee "$OUT/dev.txt" | tr '\n' ' '
echo
for dev in kvm net; do
    grep -qx "$dev" "$OUT/dev.txt" &&
        ok "/dev/$dev is present in the helper" ||
        fail "/dev/$dev is missing from the helper"
done

# podman 4.9.3 does not report sysctls in `inspect` at all, so the flag is
# checked where it lands. This is the better question anyway: not what the
# launch line asked for, but what the API service actually did with it.
forward=$(sudo podman exec "$name" cat /proc/sys/net/ipv4/ip_forward)
[ "$forward" = 1 ] &&
    ok "--sysctl net.ipv4.ip_forward=1 was honored" ||
    fail "net.ipv4.ip_forward is $forward inside the helper, want 1"

step "the guest is serving"
wait "$preview" && preview_status=0 || preview_status=$?
documents=$(wc -l <"$OUT/docs.ndjson")
printf 'preview exit %s, %s documents\n' "$preview_status" "$documents"
sed -n 1p "$OUT/docs.ndjson"

[ "$preview_status" -eq 0 ] && [ "$documents" -eq "$SESSIONS" ] &&
    ok "connector-init answered over <id>/sock/init.sock: $documents documents" ||
    fail "preview exited $preview_status with $documents documents, want 0 and $SESSIONS"

step "the launch line podman was given"
sed 's/\x1b\[[0-9;]*m//g' "$OUT/preview.log" |
    grep -a 'invoking docker' | tail -1 |
    sed 's/.*docker_args=//' | tee "$OUT/launch.txt"

# "Record anything the API service refused" -- recorded, not judged. A refusal
# of any flag on the helper's own `podman run` would have stopped the launch
# dead, and the 40 documents above are what rules that out; what is left to
# report is every other flag podman 4.9.3 turned down along the way.
step "refusals"
sed 's/\x1b\[[0-9;]*m//g' "$OUT/preview.log" |
    grep -aoE 'docker command \[.*\] failed: Error: .*' |
    sort -u | tee "$OUT/refusals.txt" | sed 's/^/  /'
[ -s "$OUT/refusals.txt" ] || echo "  (none)"

step "leaks"
dirs_after=$(sudo find "$SPIKE_REACTOR_DIR" -mindepth 1 -maxdepth 1 -name 'fs_*' | wc -l)
[ "$dirs_before" = "$dirs_after" ] &&
    ok "the runtime removed its per-connector directory ($dirs_after standing, unchanged)" ||
    fail "reactor directories went from $dirs_before to $dirs_after"

printf '\n'
if [ "$fails" -ne 0 ]; then
    printf 'exp1-launch.sh: %d failure(s)\n' "$fails" >&2
    exit 1
fi
echo "exp1-launch.sh: ok"
