FROM ubuntu:noble

# Install required packages
RUN apt update -y \
    && apt install --no-install-recommends -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
COPY ${TARGETARCH}/dekaf /usr/local/bin/
COPY ${TARGETARCH}/sops /usr/local/bin/
# Smoke-test that our binaries load in this image (e.g. shared libraries resolve).
RUN dekaf --help >/dev/null

ENV RUST_LOG=info

CMD ["/usr/local/bin/dekaf"]
