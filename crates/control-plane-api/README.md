# control-plane-api

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

## Scoping a request to one branch of the grant graph

Requests may narrow their own authority with the `X-Estuary-Scope-Prefix`
header, naming a catalog prefix:

```
X-Estuary-Scope-Prefix: acmeCo/
```

Authorization then considers only prefixes reachable both from the user's
grants and from that prefix, including prefixes it reaches through
`role_grants`. So a user who admins `acmeCo/` and `betaCo/`, where `acmeCo/`
holds a role grant to `charlieCo/`, sees `acmeCo/` and `charlieCo/` under this
header and does not see `betaCo/`.

Because the result is an intersection with the user's own grants, the header
can only remove authority. A prefix the user cannot reach yields nothing
rather than access to it. That is why a client may set the header freely — the
dashboard uses it to let a user pick which tenant they are working in, and to
switch without re-authenticating.

Omit the header to request the user's full authority. An empty or malformed
value is rejected with `invalid_argument` rather than silently matching
everything or nothing.

Handlers reach this through `Envelope::principal`, which pairs the
authenticated user with the scope. It is the only input the
`tables::UserGrant` authorization functions accept, so a handler cannot honor
the token while overlooking the scope.
