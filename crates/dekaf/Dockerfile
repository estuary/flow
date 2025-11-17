FROM ubuntu:noble

# Install required packages
RUN apt update -y \
    && apt install --no-install-recommends -y \
    ca-certificates \
    curl \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Install the `sops` CLI
ARG TARGETARCH
RUN curl -L -o /usr/local/bin/sops \
    https://github.com/getsops/sops/releases/download/v3.9.1/sops-v3.9.1.linux.${TARGETARCH} \
    && chmod +x /usr/local/bin/sops

# Copy in the dekaf binary
COPY dekaf-${TARGETARCH} /usr/local/bin/dekaf
RUN chmod +x /usr/local/bin/dekaf

ENV RUST_LOG=info

CMD ["/usr/local/bin/dekaf"]
