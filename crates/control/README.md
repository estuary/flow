# Control Plane

The Control Plane orchestrates actions taken by API users against the Data Plane.

## Local Development

A Postgres instance is required for both compilation and testing, and the connection URL must be
exported as the environment variable `DATABASE_URL`.

For running integration tests:

```bash
$ docker run --network host -e POSTGRES_USER=flow -e POSTGRES_PASSWORD=flow --detach postgres:14
$ export DATABASE_URL=postgres://flow:flow@localhost:5432/postgres
$ cargo test --all-targets --all-features
```

The `--all-features` is needed because the integration tests require certain features to be present.
The features are required so that a `cargo test --all` from the repo root will not try to run
control plane integration tests.

Postgres will not be required during compilation if the `SQLX_OFFLINE` env variable is `true`. In
that case, it will rely on the checked-in `sqlx-data.json` for query type checking instead of doing
it dynamically based on the database schema. When running `cargo build` from the repository root
(only), the `.cargo/config.toml` will set `SQLX_OFFLINE=true`.


### Running locally

To run on the local machine, you can just use Cargo from the `control` directory.

```bash
$ cargo run
```

This uses `crates/control/src/main.rs`, which is exclusively for local dev environments. `main.rs`
is _not_ used for production. The "official" way to run control plane is to run `flowctl
control-plane serve`. 

