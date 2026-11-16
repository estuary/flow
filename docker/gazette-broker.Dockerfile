FROM ubuntu:noble

RUN apt update -y \
    && apt install --no-install-recommends -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
COPY ${TARGETARCH}/gazette ${TARGETARCH}/gazctl /usr/local/bin/
# Smoke-test that our binaries load in this image (e.g. shared libraries resolve).
RUN gazette print-config >/dev/null \
    && gazctl print-config >/dev/null

# Run as the non-privileged "gazette" user.
RUN useradd gazette --create-home --shell /usr/sbin/nologin
USER gazette
WORKDIR /home/gazette
