# tokens

Framework for managing auto-refreshing authentication tokens.

## Overview

This crate provides abstractions for tokens that need periodic refreshing (JWTs, API tokens, OAuth tokens, etc.). It handles the lifecycle of obtaining, caching, and refreshing tokens with proper error handling and backoff.

## Key Types

- **`Source`** - Trait for producing tokens on demand. Implementations define how to obtain a token and how long it remains valid.
- **`Watch`** / **`PendingWatch`** - Shared access to a periodically-refreshed token. The background refresh loop runs until all `Watch` clones are dropped.
- **`Refresh`** - A single refresh result containing the token (or error), version, and expiry signal.

## Token Sources

Three built-in source types:

- **`jwt::SignedSource`** - Self-signs JWT tokens with configurable claims and duration.
- **`RestSource`** - Fetches tokens from REST APIs with automatic retry on server errors.
- **`StreamSource`** - Wraps a `futures::Stream` of tokens, coalescing immediately-ready items.

## Usage

```rust
// Create a watch from any Source. Spawns a background refresh task.
let pending: PendingWatch<String> = tokens::watch(my_source);

// Wait for first refresh, then access the token.
let watch: Arc<dyn Watch<String>> = pending.ready_owned().await;
let token: &String = watch.token().result()?;

// Transform tokens via map().
let mapped = tokens::map(watch, |token, _prior| Ok(format!("Bearer {token}")));
```

## JWT Utilities

The `jwt` module provides signing, verification, and parsing:

- `sign()` / `verify()` - Sign claims and verify with capability-based access control.
- `parse_unverified()` - Extract claims without verification (for routing/logging).
- `Verified<Claims>` / `Unverified<Claims>` - Type-safe wrappers preventing accidental misuse.
- `parse_base64_hmac_keys()` - Parse key strings supporting rotation (first key signs, all verify).

## Entry Points

- `watch()` - Create a `PendingWatch` from a `Source` with background refresh.
- `fixed()` - Create an immediately-ready watch with a static result.
- `manual()` - Create a watch with a closure for manual updates.
- `map()` - Transform a watch's token type via a closure.
