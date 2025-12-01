FROM ubuntu:noble

RUN apt update -y \
    && apt install --no-install-recommends -y \
        ca-certificates \
        podman \
        curl \
        jq \
        netavark \
        podman \
        slirp4netns \
        uidmap \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
COPY ${TARGETARCH}/flow-connector-init /usr/local/bin/
COPY ${TARGETARCH}/flowctl-go /usr/local/bin/
COPY ${TARGETARCH}/sops /usr/local/bin/

WORKDIR /tmp
