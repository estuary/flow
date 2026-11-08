FROM ubuntu:noble

# Install required packages.
RUN apt update -y \
   && apt install --no-install-recommends -y \
        ca-certificates \
        curl \
   && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
COPY ${TARGETARCH}/oidc-discovery-server /usr/local/bin/
COPY ${TARGETARCH}/oidc-discovery-server-entrypoint.sh /usr/local/bin/
# Smoke-test that our binaries load in this image (e.g. shared libraries resolve).
RUN oidc-discovery-server --help >/dev/null

ENV RUST_LOG=info

EXPOSE 8080

CMD ["/usr/local/bin/oidc-discovery-server-entrypoint.sh"]