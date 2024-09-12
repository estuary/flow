FROM ubuntu:noble

# Pick run-time library packages which match the development packages
# used by the ci-builder image. "curl" is included, to allow node-zone.sh
# mappings to directly query AWS/Azure/GCP metadata APIs.
RUN apt update -y \
     && apt install --no-install-recommends -y \
     apt-transport-https \
     ca-certificates \
     curl \
     gpg \
     && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
     && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
     && apt update -y \
     && apt upgrade -y \
     && DEBIAN_FRONTEND=noninteractive apt install --no-install-recommends -y \
     crun \
     docker.io \
     google-cloud-cli \
     jq \
     netavark \
     podman \
     slirp4netns \
     uidmap \
     && rm -rf /var/lib/apt/lists/*

# Install AWS CLI v2, taken from: https://lukewiwa.com/blog/add_the_aws_cli_to_a_dockerfile/
COPY --from=docker.io/amazon/aws-cli:latest /usr/local/aws-cli/ /usr/local/aws-cli/
RUN ln -s /usr/local/aws-cli/v2/current/bin/aws \
        /usr/local/bin/aws && \
    ln -s /usr/local/aws-cli/v2/current/bin/aws_completer \
        /usr/local/bin/aws_completer

# Copy binaries & libraries to the image, owned by root.
COPY bin/* /usr/local/bin/

WORKDIR /tmp
