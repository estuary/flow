FROM debian:bullseye-slim

# Pick run-time library packages which match the development packages
# used by the ci-builder image. "curl" is included, to allow node-zone.sh
# mappings to directly query AWS/Azure/GCP metadata APIs.
RUN apt update -y \
     && apt install --no-install-recommends -y \
     apt-transport-https \
     ca-certificates \
     curl \
     gpg \
     lsb-release \
     && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
     && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | tee /usr/share/keyrings/cloud.google.gpg \
     && echo "Add NodeSource keyring for more recent nodejs packages" \
     && export NODE_KEYRING=/usr/share/keyrings/nodesource.gpg \
     && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource.gpg.key | gpg --dearmor | tee "$NODE_KEYRING" >/dev/null \
     && gpg --no-default-keyring --keyring "$NODE_KEYRING" --list-keys \
     && echo "deb [signed-by=$NODE_KEYRING] https://deb.nodesource.com/node_14.x bullseye main" | tee /etc/apt/sources.list.d/nodesource.list \
     && mkdir -p /etc/apt/keyrings \
     && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
     && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
     $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null \
     && apt update -y \
     && apt upgrade -y \
     && DEBIAN_FRONTEND=noninteractive apt install --no-install-recommends -y \
     jq \
     nodejs \
     docker-ce-cli \
     google-cloud-sdk \
     && rm -rf /var/lib/apt/lists/*

# Create a non-privileged "flow" user.
RUN useradd flow --create-home --shell /usr/sbin/nologin

# Copy binaries & libraries to the image, owned by root.
USER root
COPY bin/* /usr/local/bin/

USER flow
WORKDIR /home/flow/project
