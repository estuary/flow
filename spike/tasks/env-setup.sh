#!/usr/bin/env bash
# Bring this box to the state every later spike package assumes: production's
# podman, the two podman networks, the nginx probe target, the images, and the
# reactor directory. Idempotent - safe to re-run after a reboot or a partial
# failure. Needs sudo.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env-common.sh"

step() { printf '\n== %s\n' "$*"; }

step "packages"
missing=()
for pkg in "${SPIKE_APT_PACKAGES[@]}"; do
    dpkg-query -W -f '${Status}' "$pkg" 2>/dev/null | grep -q '^install ok installed$' || missing+=("$pkg")
done
if [ ${#missing[@]} -gt 0 ]; then
    echo "installing: ${missing[*]}"
    sudo apt-get update -qq
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "${SPIKE_APT_PACKAGES[@]}"
else
    echo "already installed: ${SPIKE_APT_PACKAGES[*]}"
fi

have=$(podman --version | awk '{print $3}')
case "$have" in
"$SPIKE_PODMAN_VERSION"*) echo "podman $have" ;;
*) echo "podman $have, want $SPIKE_PODMAN_VERSION (noble candidate, matches production)" >&2; exit 1 ;;
esac

step "podman.socket"
# Rootful. Production's reactor drives the host podman through exactly this
# socket, and the socket is the whole of its privilege.
sudo systemctl enable --now podman.socket
sudo systemctl is-active podman.socket

step "networks"
sudo podman network exists "$SPIKE_NET_CONNECTORS" ||
    sudo podman network create "$SPIKE_NET_CONNECTORS"
sudo podman network exists "$SPIKE_NET_TEST" ||
    sudo podman network create --subnet "$SPIKE_NET_TEST_SUBNET" "$SPIKE_NET_TEST"
sudo podman network ls --format '{{.Name}} {{.Driver}} {{.Subnets}}'

step "images"
for image in "${SPIKE_IMAGES[@]}"; do
    sudo podman image exists "$image" || sudo podman pull -q "$image"
done

step "nginx assets"
mkdir -p "$NGINX_DIR/www"
pad=''
while [ ${#pad} -lt 1024 ]; do pad+="flow sandbox spike probe body"$'\n'; done
for body in index.html probe; do
    path="$NGINX_DIR/www/$body"
    if [ "$(stat -c %s "$path" 2>/dev/null || echo 0)" != 1024 ]; then
        printf '%s' "${pad:0:1024}" >"$path"
    fi
done
if [ ! -s "$NGINX_DIR/cert.pem" ] || [ ! -s "$NGINX_DIR/key.pem" ]; then
    openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
        -subj "/CN=$SPIKE_NGINX_IP" -addext "subjectAltName=IP:$SPIKE_NGINX_IP" \
        -keyout "$NGINX_DIR/key.pem" -out "$NGINX_DIR/cert.pem" 2>/dev/null
fi
chmod 0644 "$NGINX_DIR/key.pem" "$NGINX_DIR/cert.pem"

step "nginx container"
# Recreated every run so a changed config or cert always takes effect.
sudo podman rm -f "$SPIKE_NGINX_NAME" >/dev/null 2>&1 || true
sudo podman run -d \
    --name "$SPIKE_NGINX_NAME" \
    --network "$SPIKE_NET_TEST" \
    --ip "$SPIKE_NGINX_IP" \
    -v "$NGINX_DIR/nginx.conf:/etc/nginx/conf.d/default.conf:ro" \
    -v "$NGINX_DIR/cert.pem:/etc/nginx/spike/cert.pem:ro" \
    -v "$NGINX_DIR/key.pem:/etc/nginx/spike/key.pem:ro" \
    -v "$NGINX_DIR/www:/usr/share/nginx/html:ro" \
    "$SPIKE_NGINX_IMAGE" >/dev/null
echo "$SPIKE_NGINX_NAME at $SPIKE_NGINX_IP"

step "reactor directory"
sudo mkdir -p "$SPIKE_REACTOR_DIR"
sudo chown "$(id -u):$(id -g)" "$SPIKE_REACTOR_DIR"
# `stat -f -c %T` reports ext4 as "ext2/ext3" (one shared superblock magic), so
# ask the mount table for the type that actually matters to O_TMPFILE.
fstype=$(findmnt -no FSTYPE -T "$SPIKE_REACTOR_DIR")
case "$fstype" in
ext4 | xfs) echo "$SPIKE_REACTOR_DIR is $fstype" ;;
*) echo "$SPIKE_REACTOR_DIR is $fstype, want ext4 or xfs (O_TMPFILE)" >&2; exit 1 ;;
esac

step "vsock_loopback"
if sudo modprobe vsock_loopback 2>/dev/null && lsmod | grep -q '^vsock_loopback'; then
    echo "vsock_loopback: loaded (host-side AF_VSOCK test is available to WP01)"
else
    echo "vsock_loopback: NOT available"
fi

step "image digests"
for image in "${SPIKE_IMAGES[@]}"; do
    printf '%s\n' "$(sudo podman image inspect --format '{{index .RepoDigests 0}}' "$image")"
done

printf '\nenv-setup.sh: done\n'
