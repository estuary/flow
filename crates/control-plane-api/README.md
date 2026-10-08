# control-plane-api

Service-account API keys are minted by `server/public/graphql/service_accounts.rs`.
Like refresh-token creation, API-key creation rejects access tokens carrying a
capability mask or prefix scope, because the new credential would not preserve
those restrictions. Tenant creation in `server/public/graphql/tenant.rs` also
rejects either restriction before provisioning a tenant. Its `tenant/storage.rs`
helper derives collection and recovery storage mappings. Tenant provisioning,
those mappings, and MSA consent commit together.
`dataPlane` is required and must name an open public plane with ready signing
keys, matching `publicDataPlanes`. Keyless planes are excluded from the tenant
storage mapping as well.
Public AWS planes always use their derived colocated trial bucket; local and
non-AWS planes use the GCS trial bucket. Provision the matching AWS bucket and
permissions before opening a plane for signup.

`server/public/graphql/billing/tenant.rs` exposes trial dates, payment provider,
and Google Cloud Marketplace membership under `Tenant.billing`, protected by
`ViewBilling`. Marketplace account identifiers remain private.

## Development

> **NOTE:** All commands below should be run from inside the Lima VM.

### Applying Changes

Restart the agent API service to pick up changes:

```bash
systemctl --user restart flow-control-agent@flow.service
```

Replace `flow` with the stack name printed by `mise run local:stack-info`.

### Updating the GraphQL Schema

The auto-generated GraphQL schema is checked into the repo. After making changes to the GraphQL API, regenerate it with:

```bash
cargo build -p flow-client --features generate
```

### Updating sqlx query cache

After adding / modifying SQL queries, regenerate the checked-in sqlx query cache so that offline compilation works:

```bash
cargo sqlx prepare --workspace
```

### Formatting

```bash
cargo fmt -p control-plane-api
```

### Running tests

Run tests with a single thread to avoid concurrent database migration conflicts:

```bash
cargo test -p control-plane-api -- --test-threads=1
```

Tests use `insta` for snapshot testing. To automatically accept updated snapshots (you'll need to run this if you've changed the output of any of the gql operations):

```bash
INSTA_UPDATE=always cargo test -p control-plane-api -- --test-threads=1
```
