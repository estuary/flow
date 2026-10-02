FROM ubuntu:noble

RUN apt-get update \
    && apt-get install --no-install-recommends -y socat \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
# At the real binary's path, so a launcher passing --entrypoint can run either image.
COPY ${TARGETARCH}/connector-vmm-fake-entrypoint.sh /usr/local/bin/flow-connector-vmm

ENTRYPOINT ["/usr/local/bin/flow-connector-vmm"]
