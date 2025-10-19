FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim

RUN apt update -y \
     && apt install --no-install-recommends -y \
     ca-certificates \
     nodejs \
     && rm -rf /var/lib/apt/lists/*

COPY target/x86_64-unknown-linux-musl/release/derive-python /

USER nobody
ENV UV_CACHE_DIR=/tmp/uv-cache

ENTRYPOINT ["/derive-python"]
LABEL FLOW_RUNTIME_CODEC=json
LABEL FLOW_RUNTIME_PROTOCOL=derive