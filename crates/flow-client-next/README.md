# flow-client-next

Client library for Estuary Flow control-plane APIs with auto-refreshing authentication.

This crate is a replacement for `flow-client`. Dependent crates should use
`use flow_client_next as flow_client;` to ease the transition.

## Overview

Provides HTTP clients and authentication workflows for interacting with:
- Control-plane REST APIs (agent endpoints)
- PostgREST/Supabase APIs
- Data-plane services (brokers, reactors) via authorized tokens

All authentication sources integrate with the `tokens` crate for automatic
token refresh, eliminating manual token lifecycle management.

## Modules

- `rest` - Generic REST client wrapping reqwest
- `postgrest` - PostgREST query execution with pagination support
- `user_auth` - User token management (access/refresh token pairs)
- `workflows` - Authorization source implementations

## Authorization Workflows

The `workflows` module provides `tokens::Source` implementations for different
authorization scenarios:

| Workflow | Use Case |
|----------|----------|
| `UserCollectionAuth` | Users reading/writing collections |
| `UserTaskAuth` | Users accessing task shards and journals |
| `UserPrefixAuth` | Users accessing a catalog prefix within a data-plane |
| `TaskCollectionAuth` | Tasks accessing their bound collections |
| `TaskDekafAuth` | Dekaf tasks fetching specs and schemas |

Each workflow handles token refresh automatically. For task-based workflows,
use the `new_signed_source()` helper to construct the JWT claims with the
appropriate data-plane signing key.

## Building Gazette Clients

Each user workflow module exports `new_journal_client()` and (where applicable)
`new_shard_client()` helpers that wire a `tokens::PendingWatch` into a
Gazette client, providing automatic token injection on each request.

```rust
let tokens = tokens::spawn(UserCollectionAuth { ... });
let client = user_collection_auth::new_journal_client(
    fragment_client,
    router,
    tokens.pending_watch(),
);
```

## Stream Adapters

`adapt_gazette_retry_stream()` converts `gazette::RetryResult` streams into
`tonic::Result` streams, handling transient error suppression with caller-provided
logic.
