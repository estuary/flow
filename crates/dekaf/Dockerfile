FROM debian:bookworm-slim AS runtime
WORKDIR /app
COPY dekaf-bin /dekaf
RUN --mount=type=cache,target=/var/cache/apt \
    --mount=type=cache,target=/var/lib/apt \
    apt-get update && apt-get install -y libssl3 ca-certificates
CMD ["/dekaf"]