# control-plane-api
## Sandbox reset and cancellation

`src/sandboxes.rs` captures a baseline checkpoint after installing flowctl.
`sandboxReset` and the HTTP reset route in `src/server/public/sandboxes.rs`
restore that baseline, discarding later filesystem changes and execution history.
Reset then prunes extra checkpoints in the background. Sandboxes provisioned
before baseline creation was introduced must be recreated before reset works.

`sandboxExecCancel` signals the execution's process group and escalates to a
kill if necessary. The wrapper preserves output and records the exit result.
The Sprites checkpoint client methods live in `src/sprites.rs`; GraphQL
operations and ownership checks are in `src/server/public/graphql/sandboxes.rs`.


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
