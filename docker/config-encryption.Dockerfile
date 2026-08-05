FROM ubuntu:noble

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
COPY ${TARGETARCH}/flow-config-encryption /usr/local/bin/
# The service shells out to `sops` to perform the actual encryption.
COPY ${TARGETARCH}/sops /usr/local/bin/

ENV RUST_LOG=info

EXPOSE 8765

ENTRYPOINT ["flow-config-encryption"]
