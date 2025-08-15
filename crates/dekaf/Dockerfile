FROM ubuntu:noble

# Install required packages
RUN apt update -y \
    && apt install --no-install-recommends -y \
        ca-certificates \
        curl \
        jq \
    && rm -rf /var/lib/apt/lists/*

# Install the `sops` CLI
RUN curl -L -o /usr/local/bin/sops \
      https://github.com/getsops/sops/releases/download/v3.9.1/sops-v3.9.1.linux.amd64 \
   && chmod +x /usr/local/bin/sops

# Copy in the dekaf binary
COPY dekaf /usr/local/bin/

ENV RUST_LOG=info

CMD ["/usr/local/bin/dekaf"]