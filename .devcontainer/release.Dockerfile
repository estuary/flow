FROM node:lts-buster

# The node image provides an optional (non-default) "node" user, which has a UID:GID of 1000.
# Deleting this user allows the flow user to take the UID 1000. The reason that's helpful is that it
# matches the default UID of linux users, so if you mount a directory in this container, any files
# written by the flow user within the container will actually be owned by the default user on the
# host.
RUN userdel -r node

# Pick run-time library packages which match the development packages
# used by the ci-builder image. "curl" is included, to allow node-zone.sh
# mappings to directly query AWS/Azure/GCP metadata APIs.
RUN apt-get update -y \
 && apt-get upgrade -y \
 && apt-get install --no-install-recommends -y \
      ca-certificates \
      curl \
      liblz4-1 \
      libreadline7 \
      libsnappy1v5 \
      libzstd1 \
 && rm -rf /var/lib/apt/lists/*

# Copy binaries & libraries to the image.
COPY bin/* /usr/local/bin/
COPY lib/* /usr/local/lib/

RUN ldconfig

# Run as non-privileged "flow" user.
RUN useradd flow --create-home --shell /usr/sbin/nologin
USER flow
WORKDIR /home/flow
