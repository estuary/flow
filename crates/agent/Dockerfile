FROM ubuntu:noble

# Install required packages.
RUN apt update -y \
    && apt install --no-install-recommends -y \
        ca-certificates \
        s3cmd \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Install the `sops` CLI.
RUN curl -L -o /usr/local/bin/sops \
      https://github.com/getsops/sops/releases/download/v3.9.1/sops-v3.9.1.linux.amd64 \
   && chmod +x /usr/local/bin/sops

# Copy in our local assets.
COPY agent /usr/local/bin/
COPY entrypoint.sh /usr/local/bin/

ENV BIN_DIR /usr/local/bin/
ENV RUST_LOG=info

CMD ["/usr/local/bin/entrypoint.sh"]

# Example of running this container locally:
# docker run --rm --net=host -it \
#   -e CONTROL_PLANE_DB_CA_CERT="$(</home/johnny/Downloads/prod-ca-2021.crt)" \
#   -e DATABASE_URL="${DATABASE_URL}" \
#   -e PGPASSWORD=${PGPASSWORD} \
#   -e BUILDS_ROOT=gs://estuary-control/builds/ \
#   foobar:latest