# Image of sandboxes which the control plane's Docker provider runs locally
# (crates/control-plane-api/src/sandboxes/docker.rs). Build with
# `mise run local:sandbox-image`.
#
# flowctl isn't installed: the provider bind-mounts the directory of the host's build at
# /opt/estuary/bin. That build links the host's glibc, so this base
# must ship at least the same version (Ubuntu 24.04 has 2.39).
FROM ubuntu:24.04

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates curl sudo \
    && rm -rf /var/lib/apt/lists/*

# Commands run as an ordinary user, who may escalate with sudo as on a VM.
# The base image's `ubuntu` user is removed so `sandbox` takes uid 1000.
RUN userdel --remove ubuntu \
    && useradd --create-home --shell /bin/bash sandbox \
    && echo 'sandbox ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/sandbox \
    && chmod 0440 /etc/sudoers.d/sandbox \
    && install -d -o sandbox -g sandbox /work

# ENV covers `docker exec`. Logins (`su -`, `sudo -i`) instead take PATH from
# /etc/environment, and /etc/profile.d reaches every login shell.
RUN echo 'case ":${PATH}:" in *:/opt/estuary/bin:*) ;; *) export PATH="/opt/estuary/bin:${PATH}" ;; esac' \
    > /etc/profile.d/estuary.sh
ENV PATH=/opt/estuary/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# The entrypoint adds a CA mounted at /opt/estuary/ca.crt to the system trust
# store, then runs the container's command. So the image holds no CA of its own,
# and every tool which uses the system store trusts the mounted one.
COPY --chmod=0755 <<'EOF' /usr/local/bin/sandbox-entrypoint
#!/bin/sh
set -e
if [ -f /opt/estuary/ca.crt ]; then
    sudo cp /opt/estuary/ca.crt /usr/local/share/ca-certificates/estuary-local.crt
    sudo update-ca-certificates > /dev/null
fi
exec "$@"
EOF
ENTRYPOINT ["/usr/local/bin/sandbox-entrypoint"]

USER sandbox
WORKDIR /work
