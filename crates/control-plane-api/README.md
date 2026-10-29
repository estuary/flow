# control-plane-api

[`oauth-server`](../oauth-server/README.md) describes the optional CIMD/PKCE authorization
service, its dashboard consent contract, and reuse of existing refresh-token
storage.

Service-account API keys are minted by `server/public/graphql/service_accounts.rs`.
Like refresh-token creation, API-key creation rejects access tokens carrying a
capability mask or prefix mask, because the new credential would not preserve
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

`tenants.rs` provides shared provisioning for GraphQL onboarding and test fixtures;
callers supply tenant metadata and collection and recovery storage mappings.
The GraphQL layer owns signup validation and MSA consent. Legal acceptance in
`server/public/graphql/legal_terms.rs` requires ManageLegal from the requesting
user's direct grants; role grants cannot authorize consent.
`legalCheck(tenant:, type:, version:)` requires CatalogRead anywhere within the tenant
and returns whether it has accepted a matching version. Supply exactly one of
`version.eq` or `version.gte`; consent from a deleted tenant does not carry over
when its name is reused.
`sandboxCreate` requires CreateSandbox and an accepted sandbox agreement for
the current tenant containing `catalogName`.

GraphQL terminal authorization failures carry an
`authorization::AuthorizationDenied` source with the required capability bit.
Authorization denial messages omit the internal capability bit.
The schema's `AuthorizationErrors` extension expands it into bundle suggestions
and `FORBIDDEN` error extensions. Custom resolver checks can emit this error
without using `verify_authorization`; snapshot retries remain `ApiError::AuthZRetry`.
Resolvers that hide resource existence replace the denial before response formatting.

`discovers::create` queues discovery with the capture's `autoDiscover.addNewBindings`
policy. Without `autoDiscover`, new bindings default to disabled for live captures
and enabled for new captures.

`publications/submit.rs` authorizes and queues GraphQL publications with a hash
of the expanded draft. The executor verifies it after expansion on every attempt,
before applying runtime defaults. A null hash preserves PostgREST publication
behavior.

`server/public/token_exchange.rs` handles refresh-token exchange and restricted
access-token minting. `mint_restricted_token` requires an unrestricted token.
Authorization applies the minted capability and prefix masks through
`ControlClaims::subject` and `tables::UserGrant`. PostgREST doesn't respect the
access restrictions, so restricted tokens use a nonexistent Postgres role to
prevent PostgREST from accepting the token at all.

GraphQL invoice previews use the [`billing-types`](../billing-types/README.md)
calculator with Bigtable usage. `billing/db.rs` selects the requested page,
including frozen and manual invoices, before `server/public/graphql/billing/tenant.rs`
calculates the selected previews. Frozen and manual payloads come from PostgreSQL.
Only pages containing previews require the catalog-stats client; failed usage
reads return a GraphQL error. The invoice resolver tests exercise this boundary with
the legacy SQL calculator disabled.

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
