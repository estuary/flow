FROM ubuntu:noble

# Install required packages.
RUN apt update -y \
   && apt install --no-install-recommends -y \
        ca-certificates \
        curl \
   && rm -rf /var/lib/apt/lists/*

# Copy in our local assets.
COPY oidc-discovery-server /usr/local/bin/
COPY entrypoint.sh /usr/local/bin/

ENV RUST_LOG=info

EXPOSE 8080

CMD ["/usr/local/bin/entrypoint.sh"]