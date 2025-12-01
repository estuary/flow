FROM ubuntu:noble

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
COPY ${TARGETARCH}/flow-config-encryption /usr/local/bin/

ENV RUST_LOG=info
ENTRYPOINT ["flow-config-encryption"]
