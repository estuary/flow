FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim

# nodejs runs pyright, which a project's `dev` dependency group may provide.
RUN apt update -y \
     && apt install --no-install-recommends -y \
     ca-certificates \
     nodejs \
     && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
COPY ${TARGETARCH}/capture-python /

USER nobody
ENV UV_CACHE_DIR=/tmp/uv-cache
# Projects run on the image's own interpreter.
ENV UV_PYTHON_DOWNLOADS=never

ENTRYPOINT ["/capture-python"]
LABEL FLOW_RUNTIME_CODEC=json
LABEL FLOW_RUNTIME_PROTOCOL=capture
