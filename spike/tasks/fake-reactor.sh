#!/usr/bin/env bash
# Run CMD inside the production reactor image the way the Quadlet unit runs it:
# host network, podman's default capability set (no NET_ADMIN, no SYS_ADMIN),
# and the host podman API socket as the only privilege. Everything the spike
# launches is launched from here - if a step only works from a root shell on
# the host, that is a finding.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env-common.sh"

if [ $# -eq 0 ]; then
    echo "usage: fake-reactor.sh CMD [ARGS...]" >&2
    exit 2
fi

# Bind-mounted so a locally built flowctl and flow-connector-init are reachable
# from inside. Created here so podman does not make it root-owned.
mkdir -p "$REPO_DIR/target"
sudo mkdir -p "$SPIKE_REACTOR_DIR"

# sudo scrubs the caller's environment, so each FLOW_SANDBOX_SPIKE_* variable is
# forwarded by value rather than by name.
spike_env=()
while IFS= read -r assignment; do
    spike_env+=(-e "$assignment")
done < <(env | grep '^FLOW_SANDBOX_SPIKE_' || true)

exec sudo podman run --rm -i \
    --network=host \
    --user root:root \
    --no-hosts \
    -v /run/podman:/run/podman \
    -v "$SPIKE_REACTOR_DIR:$SPIKE_REACTOR_DIR" \
    -v "$REPO_DIR/target:/flow-target" \
    -e "CONTAINER_HOST=unix://$SPIKE_PODMAN_SOCKET" \
    -e DOCKER_CLI=podman \
    "${spike_env[@]}" \
    "$SPIKE_REACTOR_IMAGE" "$@"
