FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim

RUN apt update -y \
     && apt install --no-install-recommends -y \
     ca-certificates \
     nodejs \
     && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
COPY ${TARGETARCH}/derive-python /

USER nobody
ENV UV_CACHE_DIR=/tmp/uv-cache
# Projects run on the image's own interpreter.
ENV UV_PYTHON_DOWNLOADS=never

ENTRYPOINT ["/derive-python"]
LABEL FLOW_RUNTIME_CODEC=json
LABEL FLOW_RUNTIME_PROTOCOL=derive