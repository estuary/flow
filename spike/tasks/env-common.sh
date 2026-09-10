# Constants shared by the spike environment scripts. Sourced, never executed.
#
# Every later work package reaches the box through these names, so they live in
# one place rather than in each script's head.

SPIKE_TASKS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPIKE_DIR="$(cd "$SPIKE_TASKS_DIR/.." && pwd)"
REPO_DIR="$(cd "$SPIKE_DIR/.." && pwd)"
NGINX_DIR="$SPIKE_TASKS_DIR/nginx"

# CONTRACTS "Paths and names". Must be ext4 or xfs: the helper opens an
# O_TMPFILE under it.
: "${SPIKE_REACTOR_DIR:=/var/tmp/flow-spike/reactor}"

# Production's Quadlet unit is not reachable from this box, so we pin the newest
# published reactor tag instead of the one production runs. Override to match
# production once that tag is known.
: "${SPIKE_REACTOR_IMAGE:=ghcr.io/estuary/reactor:v0.6.13-127-g6339ac87653}"

# Production's host podman. The version is a fixture of the spike, not a floor.
SPIKE_PODMAN_VERSION=4.9.3
SPIKE_PODMAN_SOCKET=/run/podman/podman.sock

SPIKE_NET_CONNECTORS=flow-connectors
SPIKE_NET_TEST=spike-testnet2
SPIKE_NET_TEST_SUBNET=198.51.100.0/24

SPIKE_NGINX_NAME=spike-nginx
SPIKE_NGINX_IP=198.51.100.10
SPIKE_NGINX_IMAGE=docker.io/library/nginx:latest

# erofs-utils is WP08b's: mkfs.erofs builds the per-tag dependency image. e2fsprogs
# (mkfs.ext4) is already on an Ubuntu host.
SPIKE_APT_PACKAGES=(podman uidmap slirp4netns netavark aardvark-dns erofs-utils)

SPIKE_IMAGES=(
    "$SPIKE_REACTOR_IMAGE"
    ghcr.io/estuary/derive-python:dev
    ghcr.io/estuary/source-hello-world:dev
    ghcr.io/estuary/materialize-sqlite:dev
    docker.io/library/busybox:latest
    "$SPIKE_NGINX_IMAGE"
)
