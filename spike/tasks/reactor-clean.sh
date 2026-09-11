#!/usr/bin/env bash
# Remove per-connector directories left behind in $SPIKE_REACTOR_DIR.
#
# The runtime creates `<id>/` before a launch and removes it after, so anything
# standing here is a leak: a killed harness, or the `CONNECTORS+=` subshell bug
# WP08b diagnosed. They are root-owned (podman's bind mounts made them so), so
# the removal needs sudo even though the parent is not.
#
# Nothing may be running: the directories hold the live `sock/init.sock` and the
# scratch backing file of every helper that is up.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env-common.sh"

mapfile -t dirs < <(sudo find "$SPIKE_REACTOR_DIR" -mindepth 1 -maxdepth 1 -name 'fs_*')

if [ "${#dirs[@]}" -eq 0 ]; then
    echo "reactor-clean.sh: $SPIKE_REACTOR_DIR is clean"
    exit 0
fi

if running=$(sudo podman ps --filter 'name=^fs_' --format '{{.Names}}') && [ -n "$running" ]; then
    echo "reactor-clean.sh: helpers are running, refusing to remove their directories:" >&2
    printf '  %s\n' $running >&2
    exit 2
fi

printf 'reactor-clean.sh: removing %d directories from %s\n' "${#dirs[@]}" "$SPIKE_REACTOR_DIR"
sudo rm -rf "${dirs[@]}"
echo "reactor-clean.sh: ok"
