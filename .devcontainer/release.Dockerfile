FROM ubuntu:18.04

# Pick run-time library packages which match the development packages
# used by the ci-builder image. "curl" is included, to allow node-zone.sh
# mappings to directly query AWS/Azure/GCP metadata APIs.
RUN apt-get update -y \
 && apt-get upgrade -y \
 && apt-get install --no-install-recommends -y \
      ca-certificates \
      curl \
      libreadline7 \
 && rm -rf /var/lib/apt/lists/*

# Copy binaries to the image.
COPY * /usr/local/bin/

# Run as non-privileged "flow" user.
RUN useradd flow --create-home --shell /usr/sbin/nologin
USER flow
WORKDIR /home/flow