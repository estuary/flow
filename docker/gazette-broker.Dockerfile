FROM ubuntu:noble

RUN apt update -y \
    && apt install --no-install-recommends -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
COPY ${TARGETARCH}/gazette ${TARGETARCH}/gazctl /usr/local/bin/

# Run as the non-privileged "gazette" user.
RUN useradd gazette --create-home --shell /usr/sbin/nologin
USER gazette
WORKDIR /home/gazette
