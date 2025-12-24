# Authorization System Overview

This document provides orientation to the authorization machinery across the Gazette and Estuary Flow codebases.

Gazette: /Users/johnny/Work/gazette/
Flow: /Users/johnny/Work/estuary/flow-read-proto

## Core Abstractions (Gazette)

The authorization system is built on two key interfaces defined in `gazette/broker/protocol/auth.go`:

- **`Authorizer`** - Signs claims and attaches them to outgoing requests
- **`Verifier`** - Validates incoming authorization tokens and extracts claims

### Claims Structure

Claims (`pb.Claims`) contain:
- **Capability** - Bitmask of authorized operations (LIST, APPLY, READ, APPEND, REPLICATE)
- **Selector** - Label selector scoping the resources the capability applies to
- Standard JWT registered claims (Subject, Issuer, IssuedAt, ExpiresAt)

### Capabilities

Base capabilities defined in `gazette/broker/protocol/auth.go`:
```
Capability_LIST      = 1 << 1
Capability_APPLY     = 1 << 2
Capability_READ      = 1 << 3
Capability_APPEND    = 1 << 4
Capability_REPLICATE = 1 << 5
```

Flow extends these with application-specific capabilities in `go/protocols/flow/auth.go`:
```
Capability_AUTHORIZE     = 1 << 16  // Request delegated authorization
Capability_SHUFFLE       = 1 << 17  // Use the Shuffle API
Capability_NETWORK_PROXY = 1 << 18  // Use the Network Proxy API
Capability_PROXY_CONNECTOR = 1 << 19  // Use connector APIs
```

## Component Layout

### 1. Gazette `auth/` Package

**Location:** `gazette/auth/auth.go`

Provides concrete implementations of `Authorizer` and `Verifier`:

- **`KeyedAuth`** - Symmetric HMAC-based authorization using pre-shared keys
  - Signs tokens using the first key in a key set
  - Verifies using any key in the set (supports key rotation)
  - Uses HS256/HS384 JWT signing
  - Special `AA==` key allows unauthenticated requests (migration helper)

- **`BearerAuth`** - Simple pass-through of a pre-configured bearer token

- **`NoopAuth`** - No-op implementation (for testing/development)

### 2. Gazette Protocol Auth Wrappers

**Locations:**
- `gazette/broker/protocol/auth.go` - Journal client/server wrappers
- `gazette/consumer/protocol/auth.go` - Shard client/server wrappers

These provide the "plumbing" that integrates authorization into gRPC services:

**Client-side (`AuthJournalClient`, `AuthShardClient`):**
- Wraps a raw gRPC client
- Intercepts each RPC call
- Derives default claims from the request if not explicitly provided via `WithClaims(ctx)`
- Uses the configured `Authorizer` to sign claims and attach bearer token
- Passes the authorized context to the underlying client

**Server-side (`VerifiedJournalServer`, `VerifiedAuthServer`):**
- Wraps application server implementations
- Extracts and verifies authorization from incoming requests using `Verifier`
- Creates a deadline-bound context tied to token expiration
- Passes verified claims to the application handler

### 3. Flow Protocol Auth Wrappers

**Locations:**
- `go/protocols/flow/auth.go` - NetworkProxy client/server wrappers
- `go/protocols/runtime/auth.go` - Shuffler client/server wrappers

These follow the same pattern as Gazette's wrappers but for Flow-specific gRPC services.

### 4. Control Plane Authorizer

**Location:** `go/runtime/authorizer.go`

This is the key integration point that bridges Gazette's auth system with Estuary's control plane.

**`controlPlaneAuthorizer`** implements `pb.Authorizer` with delegated authorization:

1. **Request Signing:** Self-signs a JWT containing the requested claims plus:
   - `AUTHORIZE` capability (permits delegation)
   - Shard ID as the Subject
   - Data-plane FQDN as the Issuer

2. **Control Plane Call:** POSTs the signed request to `/authorize/task` on the control plane API

3. **Response Processing:** Receives back:
   - A newly-signed authorization token (from the control plane's key)
   - The broker address of the target data-plane

4. **Caching:** Results (including errors) are cached to avoid thundering herd issues

5. **Route Injection:** If the request doesn't already have routing info, injects the returned broker address

**Special Cases:**
- Recovery log access (`recovery/*` journals) is self-signed locally using `KeyedAuth`
- Handles task migration edge case for stats partitions

### 5. Control Plane Infrastructure

**Location:** `go/runtime/control_plane.go`

Provides the HTTP client for control plane API calls:

- `signClaims()` - Self-signs claims using the data-plane's key
- `callControlAPI()` - Generic HTTP POST wrapper with retry logic

## Data Flow

### Client Authorization Flow

1. Application code calls e.g. journalClient.Read(ctx, request)
2. AuthJournalClient.Read() extracts or derives claims
3. controlPlaneAuthorizer.Authorize():
   a. Check cache for valid entry → return cached result
   b. Self-sign claims with AUTHORIZE capability
   c. POST to control plane /authorize/task
   d. Receive signed token + broker address
   e. Cache result
   f. Inject routing + attach bearer token to context
4. Underlying gRPC call proceeds with authorization

### Server Verification Flow

1. gRPC request arrives with Authorization header
2. VerifiedJournalServer.Read() calls Verifier.Verify()
3. KeyedAuth.Verify():
   a. Extract bearer token from metadata
   b. Parse and validate JWT signature
   c. Check capability bits
   d. Create deadline-bound context
4. Application handler receives verified claims

## Wiring in Flow

In `go/runtime/flow_consumer.go`, the authorization components are assembled:

```go
// Start with KeyedAuth for symmetric signing/verification
keyedAuth, _ := auth.NewKeyedAuth(config.Auth.Keys)

// Wrap it for control-plane delegation (non-test mode)
args.Service.Authorizer = newControlPlaneAuthorizer(controlPlane)

// Replace the JournalClient's authorizer
rawClient := args.Service.Journals.Inner
args.Service.Journals.JournalClient = pb.NewAuthJournalClient(rawClient, args.Service.Authorizer)
```

## Key Design Points

1. **Separation of Concerns:** Gazette provides the framework; Flow adds control-plane integration

2. **Dynamic Routing:** The control plane authorization response includes the broker address, solving the problem of clients not knowing which data-plane to connect to

3. **Token Lifetime:** Authorization tokens have limited expiration; streaming RPCs use longer lifetimes (1 hour)

4. **Caching:** Both successful authorizations and errors are cached to protect the control plane from thundering herds

5. **Key Rotation:** `KeyedAuth` supports multiple keys - first for signing, any for verification

6. **Capability Extensibility:** Applications can define capabilities starting at bit 16
