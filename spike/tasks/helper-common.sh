# Constants for the helper scripts. Sourced, never executed.
#
# CONTRACTS "Paths and names" puts the helper image name alongside the other
# shared names, but env-common.sh belongs to WP00; this keeps the addition
# inside WP03's paths. Fold it in whenever the two are next touched together.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env-common.sh"

SPIKE_HELPER_IMAGE=localhost/flow-sandbox-helper:spike

# The connector image mount of PLAN "Helper launch". `rw=true` is what makes the
# guest root writable: podman lays a per-container layer over the image and
# removes it with the container, and that layer IS the guest's root. Nothing
# overlays it inside the guest (WP04b).
spike_image_mount() {
    printf '%s' "--mount=type=image,source=$1,destination=/rootfs,rw=true"
}

# The sysctls of PLAN "Helper launch", mirroring the launch line in
# crates/runtime-next/src/container/spike.rs. ip_forward routes the tap out of
# the container's veth. rp_filter is set through `conf.default` because tap0
# does not exist at container creation and /proc/sys is read-only inside the
# helper, so this is the only route to strict reverse-path filtering on it
# (WP07). A caller that needs it relaxed repeats the sysctl later on the line;
# podman takes the last value.
SPIKE_HELPER_SYSCTLS=(
    --sysctl net.ipv4.ip_forward=1
    --sysctl net.ipv4.conf.default.rp_filter=1
)
