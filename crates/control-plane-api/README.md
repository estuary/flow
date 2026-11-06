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

`sandboxes::execute` traces command launch attempts with the command line,
sandbox ID, user ID, command ID, and stdin byte count. The GraphQL resolver
logs startup success or failure with the same command ID. Stdin contents and
command output are excluded from these events. Callers should pass credentials
through stdin because command lines are logged.
`server/public/token_exchange.rs` traces restricted-token minting with the user,
capability and prefix masks, and expiry, excluding the token value. Sandbox
token injection through `sandboxExecute` is recorded as a command launch;
there is no dedicated token-injection operation.

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

## Development

> **NOTE:** All commands below should be run from inside the Lima VM.

### Applying Changes

Restart the agent API service to pick up changes:

```bash
systemctl --user restart flow-control-agent@flow.service
```

Replace `flow` with the stack name printed by `mise run local:stack-info`.

### Updating the GraphQL Schema

The GraphQL SDL is checked in as `control-plane-api.graphql`. flowctl compiles
its queries against it, and embeds it for `flowctl raw graphql-schema`. After
changing the GraphQL API, regenerate it by running the check test, which
rewrites the file when it has drifted:

```bash
mise exec -- cargo nextest run -p control-plane-api checked_in_sdl_is_current
```

The server also serves its own SDL, unauthenticated, at `GET /api/graphql/schema`.
The OpenAPI spec has entries for both GraphQL routes. They describe the HTTP
envelope only, and defer to the SDL for types.

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
