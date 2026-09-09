#!/usr/bin/env bash
# Read-only preflight: everything env-setup.sh establishes, re-verified. Later
# packages call this first so a missing piece names itself instead of surfacing
# as a puzzling failure three layers down. One line per failure, non-zero exit.
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env-common.sh"

fails=0
ok() { printf 'ok    %s\n' "$*"; }
warn() { printf 'warn  %s\n' "$*"; }
fail() {
    printf 'FAIL  %s\n' "$*" >&2
    fails=$((fails + 1))
}

for pkg in "${SPIKE_APT_PACKAGES[@]}"; do
    dpkg-query -W -f '${Status}' "$pkg" 2>/dev/null | grep -q '^install ok installed$' ||
        fail "package $pkg is not installed (run env-setup.sh)"
done

have=$(podman --version 2>/dev/null | awk '{print $3}')
case "$have" in
"$SPIKE_PODMAN_VERSION"*) ok "podman $have" ;;
"") fail "podman is not on PATH" ;;
*) fail "podman $have, want $SPIKE_PODMAN_VERSION (production's)" ;;
esac

sudo systemctl is-active --quiet podman.socket ||
    fail "podman.socket is not active"
[ -S "$SPIKE_PODMAN_SOCKET" ] ||
    fail "$SPIKE_PODMAN_SOCKET is missing (the reactor's only privilege)"
[ -S "$SPIKE_PODMAN_SOCKET" ] && ok "$SPIKE_PODMAN_SOCKET"

for dev in /dev/kvm /dev/net/tun; do
    [ -c "$dev" ] && ok "$dev" || fail "$dev is missing"
done

sudo podman network exists "$SPIKE_NET_CONNECTORS" &&
    ok "network $SPIKE_NET_CONNECTORS" ||
    fail "network $SPIKE_NET_CONNECTORS does not exist"

subnet=$(sudo podman network inspect --format '{{range .Subnets}}{{.Subnet}}{{end}}' "$SPIKE_NET_TEST" 2>/dev/null)
if [ "$subnet" = "$SPIKE_NET_TEST_SUBNET" ]; then
    ok "network $SPIKE_NET_TEST $subnet"
else
    fail "network $SPIKE_NET_TEST subnet is '${subnet:-absent}', want $SPIKE_NET_TEST_SUBNET"
fi

for image in "${SPIKE_IMAGES[@]}"; do
    sudo podman image exists "$image" || fail "image $image is not pulled"
done

nginx_ip=$(sudo podman inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$SPIKE_NGINX_NAME" 2>/dev/null)
if [ "$nginx_ip" != "$SPIKE_NGINX_IP" ]; then
    fail "$SPIKE_NGINX_NAME is at '${nginx_ip:-not running}', want $SPIKE_NGINX_IP"
fi
for url in "http://$SPIKE_NGINX_IP/probe" "https://$SPIKE_NGINX_IP/probe" "https://$SPIKE_NGINX_IP/"; do
    bytes=$(curl -sk --max-time 5 "$url" | wc -c)
    [ "$bytes" = 1024 ] && ok "$url -> $bytes bytes" ||
        fail "$url returned $bytes bytes, want 1024"
done

if [ ! -d "$SPIKE_REACTOR_DIR" ]; then
    fail "$SPIKE_REACTOR_DIR does not exist"
else
    # `stat -f -c %T` cannot tell ext4 from ext2/ext3; O_TMPFILE can.
    fstype=$(findmnt -no FSTYPE -T "$SPIKE_REACTOR_DIR")
    case "$fstype" in
    ext4 | xfs) ok "$SPIKE_REACTOR_DIR is $fstype" ;;
    *) fail "$SPIKE_REACTOR_DIR is $fstype, want ext4 or xfs (O_TMPFILE)" ;;
    esac
fi

# Production's default capability set, decoded in PLAN "Inputs established
# before the spike". Also proves the fake reactor starts at all.
caps=$("$SPIKE_TASKS_DIR/fake-reactor.sh" grep CapEff /proc/self/status 2>/dev/null | awk '{print $2}')
[ "$caps" = 00000000800405fb ] && ok "fake reactor CapEff $caps" ||
    fail "fake reactor CapEff is '${caps:-unavailable}', want 00000000800405fb"

lsmod | grep -q '^vsock_loopback' &&
    ok "vsock_loopback loaded" ||
    warn "vsock_loopback not loaded (WP01's host-side AF_VSOCK test is unavailable)"

if [ "$fails" -ne 0 ]; then
    printf '\nenv-check.sh: %d failure(s)\n' "$fails" >&2
    exit 1
fi
printf '\nenv-check.sh: ok\n'
