#!/usr/bin/env bash
# Entrypoint of localhost/flow-sandbox-stub:spike. It accepts the helper CLI of
# CONTRACTS "Helper CLI" and ignores every bit of it: there is no VM here, no
# tap, no egress rules, and no flow-init. What it does reproduce is exactly the
# part WP05 is proving - the mounts the runtime hands the helper, and a
# connector-init reached over /sock/init.sock rather than over TCP.
#
# Only static Go connectors work under it: the connector inherits this image's
# environment rather than its own image's, which libkrun's init would apply.
set -euo pipefail

# Never a leading space: the reactor reads that byte on stderr as
# connector-init's readiness signal.
log() { printf 'stub-helper: %s\n' "$*" >&2; }

log "ignoring helper args: $*"

# The real helper injects these into the guest root. Here connector-init runs
# under a chroot into the image mount, so they are copied in instead. A copy,
# not a bind mount, keeps the stub within podman's default capabilities - the
# thing under test is the launch line, not the sandbox.
mkdir -p /rootfs/init
cp -a /init/. /rootfs/init/

# libkrun owns this socket in the real helper and proxies it to the guest's
# vsock port 49092. Mode 0777 because a reactor running as an ordinary user
# must be able to connect; under the real helper the socket's mode is libkrun's
# to answer, and that is WP06's to find out.
socat "UNIX-LISTEN:/sock/init.sock,fork,mode=0777" TCP:127.0.0.1:49092 &

for _ in $(seq 50); do
    if [ -S /sock/init.sock ]; then
        break
    fi
    sleep 0.1
done
if [ ! -S /sock/init.sock ]; then
    log "socat did not create /sock/init.sock"
    exit 2 # Helper's own failure, per CONTRACTS "Helper CLI".
fi

exec chroot /rootfs /init/flow-connector-init \
    --image-inspect-json-path=/init/image-inspect.json \
    --port 49092
