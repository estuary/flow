FROM ubuntu:20.04

# Pick run-time library packages which match the development packages
# used by the ci-builder image. "curl" is included, to allow node-zone.sh
# mappings to directly query AWS/Azure/GCP metadata APIs.
RUN apt-get update -y \
 && apt-get upgrade -y \
 && apt-get install --no-install-recommends -y \
      ca-certificates \
      curl \
      libgflags2.2 \
      libjemalloc2 \
      libsnappy1v5 \
      libzstd1 \
 && rm -rf /var/lib/apt/lists/*

# Copy binaries & librocks.so to the image. Configure Rocks for run-time linking.
COPY * /usr/local/bin/
RUN mv /usr/local/bin/librocksdb.so* /usr/local/lib/ && ldconfig

# Run as non-privileged "gazette" user.
RUN useradd gazette --create-home --shell /usr/sbin/nologin
USER gazette
WORKDIR /home/gazette

