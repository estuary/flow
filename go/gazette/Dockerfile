################################################################################
# Gazette broker image.

FROM ubuntu:24.04 AS broker

ARG TARGETARCH

RUN apt-get update -y \
 && apt-get upgrade -y \
 && apt-get install --no-install-recommends -y \
      ca-certificates \
      curl \
 && rm -rf /var/lib/apt/lists/*

COPY ${TARGETARCH}/gazette ${TARGETARCH}/gazctl /usr/local/bin 

# Run as non-privileged "gazette" user.
RUN useradd gazette --create-home --shell /usr/sbin/nologin
USER gazette
WORKDIR /home/gazette

################################################################################
# Gazette examples image.

FROM ubuntu:24.04 AS examples

ARG TARGETARCH

RUN apt-get update -y \
 && apt-get upgrade -y \
 && apt-get install --no-install-recommends -y \
      ca-certificates \
      curl \
      librocksdb8.9 \
      libsqlite3-0 \
 && rm -rf /var/lib/apt/lists/*

# Copy binaries to the image.
COPY \
     ${TARGETARCH}/bike-share \
     ${TARGETARCH}/chunker \
     ${TARGETARCH}/counter \
     ${TARGETARCH}/gazctl \
     ${TARGETARCH}/integration.test \
     ${TARGETARCH}/summer \
     ${TARGETARCH}/wordcountctl \
     /usr/local/bin/

# Run as non-privileged "gazette" user.
RUN useradd gazette --create-home --shell /usr/sbin/nologin
USER gazette
WORKDIR /home/gazette

