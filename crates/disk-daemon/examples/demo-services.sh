#!/usr/bin/env bash
#
# Run the etcd, Gazette broker, and disk-daemon that the examples use, until Ctrl-C.
# Ctrl-C drains the daemon, then stops the others and removes what they made.
# mise puts etcd and gazette on PATH, and sets CARGO_TARGET_DIR:
#
#   mise run build:gazette
#   mise exec -- cargo build -p disk-daemon --bin flow-disk-daemon
#   mise exec -- crates/disk-daemon/examples/demo-services.sh
#
# Its ports and socket are the examples' defaults, which a local Flow stack and a
# system etcd don't use.

set -euo pipefail

STATE=${TMPDIR:-/tmp}/disk-daemon-demo
DAEMON=${CARGO_TARGET_DIR:?run this under mise exec}/debug/flow-disk-daemon
ETCD=http://127.0.0.1:22379

# The daemon is the one service that needs root. Prompt for sudo here, before it
# runs in the background.
sudo modprobe ublk_drv
mkdir "${STATE}" || { echo "an earlier run left ${STATE}; remove it with sudo" >&2; exit 1; }
mkdir "${STATE}"/{fragments,images,mounts}

# Each service gets its own process group, so Ctrl-C reaches only this script.
# Each stops before what it depends on: the daemon's drain needs the broker, which
# needs etcd.
set -m
stop() {
    set +eu
    sudo kill -TERM "${daemon}"
    wait "${daemon}"
    kill "${broker}"
    wait "${broker}"
    kill "${etcd}"
    wait
    sudo rm -rf "${STATE}"
}
trap stop EXIT
trap exit INT TERM

etcd --data-dir "${STATE}/etcd" --log-level warn \
    --listen-client-urls "${ETCD}" --advertise-client-urls "${ETCD}" \
    --listen-peer-urls http://127.0.0.1:22380 &
etcd=$!

gazette serve --broker.host 127.0.0.1 --broker.port 28080 --log.level warn \
    --broker.file-only --broker.file-root "${STATE}/fragments" --etcd.address "${ETCD}" &
broker=$!

# This broker has no `--broker.auth-keys`, so it ignores tokens and any key serves.
sudo "${DAEMON}" --uds-path "${STATE}/disk.sock" \
    --broker-address http://127.0.0.1:28080 \
    --data-plane-fqdn disk-daemon-demo \
    --data-plane-auth-keys "$(head -c 32 /dev/urandom | base64)" \
    --image-dir "${STATE}/images" --mount-dir "${STATE}/mounts" &
daemon=$!

wait -n
