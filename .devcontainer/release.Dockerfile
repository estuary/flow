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
     && echo "xsBNBGKItdQBCADWmKTNZEYWgXy73FvKFY5fRro4tGNa4Be4TZW3wZpct9Cj8EjykU7S9EPoJ3EdKpxFltHRu7QbDi6LWSNA4XxwnudQrYGxnxx6Ru1KBHFxHhLfWsvFcGMwit/znpxtIt9UzqCm2YTEW5NUnzQ4rXYqVQK2FLG4weYJ5bKwkY+ZsnRJpzxdHGJ0pBiqwkMT8bfQdJymUBown+SeuQ2HEqfjVMsIRe0dweD2PHWeWo9fTXsz1Q5abiGckyOVyoN9//DgSvLUocUcZsrWvYPaN+o8lXTO3GYFGNVsx069rxarkeCjOpiQOWrQmywXISQudcusSgmmgfsRZYW7FDBy5MQrABEBAAHNUVJhcHR1cmUgQXV0b21hdGljIFNpZ25pbmcgS2V5IChjbG91ZC1yYXB0dXJlLXNpZ25pbmcta2V5LTIwMjItMDMtMDctMDhfMDFfMDEucHViKcLAYgQTAQgAFgUCYoi11AkQtT3IDRPt7wUCGwMCGQEAAMGoCAB8QBNIIN3Q2D3aahrfkb6axd55zOwR0tnriuJRoPHoNuorOpCv9aWMMvQACNWkxsvJxEF8OUbzhSYjAR534RDigjTetjK2i2wKLz/kJjZbuF4ZXMynCm40eVm1XZqU63U9XR2RxmXppyNpMqQO9LrzGEnNJuh23icaZY6no12axymxcle/+SCmda8oDAfa0iyA2iyg/eU05buZv54MC6RB13QtS+8vOrKDGr7RYp/VYvQzYWm+ck6DvlaVX6VB51BkLl23SQknyZIJBVPm8ttU65EyrrgG1jLLHFXDUqJ/RpNKq+PCzWiyt4uy3AfXK89RczLu3uxiD0CQI0T31u/IzsBNBGKItdQBCADIMMJdRcg0Phv7+CrZz3xRE8Fbz8AN+YCLigQeH0B9lijxkjAFr+thB0IrOu7ruwNY+mvdP6dAewUur+pJaIjEe+4s8JBEFb4BxJfBBPuEbGSxbi4OPEJuwT53TMJMEs7+gIxCCmwioTggTBp6JzDsT/cdBeyWCusCQwDWpqoYCoUWJLrUQ6dOlI7s6p+iIUNIamtyBCwb4izs27HdEpX8gvO9rEdtcb7399HyO3oD4gHgcuFiuZTpvWHdn9WYwPGM6npJNG7crtLnctTR0cP9KutSPNzpySeAniHx8L9ebdD9tNPCWC+OtOcGRrcBeEznkYh1C4kzdP1ORm5upnknABEBAAHCwF8EGAEIABMFAmKItdQJELU9yA0T7e8FAhsMAABJmAgAhRPk/dFj71bU/UTXrkEkZZzE9JzUgan/ttyRrV6QbFZABByf4pYjBj+yLKw3280//JWurKox2uzEq1hdXPedRHICRuh1Fjd00otaQ+wGF3kY74zlWivB6Wp6tnL9STQ1oVYBUv7HhSHoJ5shELyedxxHxurUgFAD+pbFXIiK8cnAHfXTJMcrmPpC+YWEC/DeqIyEcNPkzRhtRSuERXcq1n+KJvMUAKMD/tezwvujzBaaSWapmdnGmtRjjL7IxUeGamVWOwLQbUr+34MwzdeJdcL8fav5LA8Uk0ulyeXdwiAK8FKQsixI+xZvz7HUs8ln4pZwGw/TpvO9cMkHogtgzQ==" \
        | base64 -d > /usr/share/keyrings/cloud.google.gpg \
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
