# Build Stage
################################################################################
FROM rust:1.54-slim-buster as builder

RUN rustup component add clippy

RUN apt-get update \
  && apt-get install -y ca-certificates pkg-config libssl-dev \
  && rm -rf /var/lib/apt/lists/*

# We need the SQLx tools to setup the test database.
RUN cargo install sqlx-cli --version="0.5.10"

ENV RUST_BACKTRACE=1

WORKDIR /app

COPY crates/control/Cargo.toml ./Cargo.lock ./

# Avoid having to install/build all dependencies by copying the Cargo files and
# making a dummy src/main.rs and empty lib.rs files.
RUN mkdir -p ./src/bin \
  && echo "fn main() {}" > src/main.rs \
  && touch src/lib.rs \
  # TODO: figure out if there's a way to use `--locked` with these commands.
  # There seems to be an issue with building only this single Cargo.toml, but
  # pulling in the workspace Cargo.lock file.
  && cargo test \
  && cargo build --release \
  && rm -r src

COPY crates/control/src ./src
COPY crates/control/tests ./tests
COPY crates/control/config ./config
COPY crates/control/migrations ./migrations

# We use the `sqlx::query!` macros to get compile-time verification of our
# queries. This usually requires a database connection, but we can use `cargo
# sqlx prepare` to save metadata necessary to this file.
COPY crates/control/sqlx-data.json ./sqlx-data.json
ENV SQLX_OFFLINE=true

# We need to be able to set the postgres host within the CI build for the tests
# to be able to connect. Defaults to localhost for non-CI workflows.
ARG PGHOST=127.0.0.1
ENV CONTROL_DATABASE_HOST=${PGHOST}

RUN touch src/main.rs \
  # This touch prevents Docker from using a cached empty main.rs file.
  && touch src/main.rs src/lib.rs \
  # Since the tests require a postgres connection, any `docker build` commands
  # will the appropriate `--network` flag to access the database.
  && cargo test --locked --offline \
  && cargo clippy --locked --offline \
  && cargo install --path . --locked --offline


# Runtime Stage
################################################################################
FROM gcr.io/distroless/cc-debian10

WORKDIR /app
ENV PATH="/app:$PATH"

# Copy in the connector artifact.
COPY --from=builder /usr/local/cargo/bin/control ./control-plane-server
COPY --from=builder /app/config ./config

# Avoid running the connector as root.
USER nonroot:nonroot

# We can remove this eventually, but for now this is super useful to just set uniformly.
# * tower_http=debug gives us access logs.
ENV RUST_LOG=info,tower_http=debug

ENTRYPOINT ["/app/control-plane-server"]
