FROM ubuntu:noble

# Install required packages
RUN apt update -y \
    && apt install --no-install-recommends -y \
    ca-certificates \
    curl \
    jq \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
COPY ${TARGETARCH}/dekaf /usr/local/bin/
COPY ${TARGETARCH}/sops /usr/local/bin/

ENV RUST_LOG=info

CMD ["/usr/local/bin/dekaf"]
