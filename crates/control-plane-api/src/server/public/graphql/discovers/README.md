# Discover GraphQL API

`discovers.rs` defines `createDiscover` and `discover(id)`. Submission selects a staged
capture or copies a readable live capture into the owned draft, validates the
connector tag and discovery plane, and inserts the queued job in one transaction.
The database trigger creates its automation task. The executor in
`crates/agent/src/discovers.rs` later merges results into the draft.

The `Discover` resolvers read current draft errors and log lines. Each query
joins through the owning draft. Log cursors use `logged_at`, so pagination is
best effort when separate writers produce equal or delayed timestamps.

`test.rs` exercises the HTTP GraphQL API against a migrated test database.
