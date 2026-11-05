FROM ubuntu:noble

RUN apt update -y \
    && apt install --no-install-recommends -y \
        ca-certificates \
        podman \
        curl \
        netavark \
        podman \
        slirp4netns \
        uidmap \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
COPY ${TARGETARCH}/flow-connector-init /usr/local/bin/
COPY ${TARGETARCH}/flowctl-go /usr/local/bin/
# Rust crates linked into flowctl-go, for the image's SBOM scan.
COPY ${TARGETARCH}/flowctl-go.cdx.json /usr/local/share/sbom/
COPY ${TARGETARCH}/runtime-sidecar /usr/local/bin/
COPY ${TARGETARCH}/sops /usr/local/bin/

WORKDIR /tmp
