FROM debian:bullseye-slim

# Pick run-time library packages which match the development packages
# used by the ci-builder image. "curl" is included, to allow node-zone.sh
# mappings to directly query AWS/Azure/GCP metadata APIs.
RUN apt update -y \
 && apt install --no-install-recommends -y \
      ca-certificates \
      curl \
      gpg \
 && echo "Add NodeSource keyring for more recent nodejs packages" \
 && export NODE_KEYRING=/usr/share/keyrings/nodesource.gpg \
 && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource.gpg.key | gpg --dearmor | tee "$NODE_KEYRING" >/dev/null \
 && gpg --no-default-keyring --keyring "$NODE_KEYRING" --list-keys \
 && echo "deb [signed-by=$NODE_KEYRING] https://deb.nodesource.com/node_14.x bullseye main" | tee /etc/apt/sources.list.d/nodesource.list \
 && apt update -y \
 && apt upgrade -y \
 && apt install --no-install-recommends -y \
      jq \
      nodejs \
 && rm -rf /var/lib/apt/lists/*

RUN curl -o docker-cli.deb 'https://download.docker.com/linux/debian/dists/bullseye/pool/stable/amd64/docker-ce-cli_20.10.7~3-0~debian-bullseye_amd64.deb' && \
    dpkg -i docker-cli.deb && \
    rm docker-cli.deb

# Create a non-privileged "flow" user.
RUN useradd flow --create-home --shell /usr/sbin/nologin

# Copy binaries & libraries to the image, owned by root.
USER root
COPY bin/* /usr/local/bin/

USER flow
WORKDIR /home/flow/project
