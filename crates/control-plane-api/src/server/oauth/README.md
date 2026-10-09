# OAuth authorization server

`mod.rs` implements discovery, authorization, dashboard consent, and code
redemption. `client.rs` validates public clients, including HTTPS Client ID
Metadata Documents. The `oauth2` crate supplies PKCE, random codes, and standard
token/error response types. There is no dependency on MCP.

## Credential model

Redemption returns one existing Estuary multi-use refresh credential as the OAuth
`access_token`, with `token_type: "bearer"` and `scope: "estuary"`. Clients treat it
as opaque and send it directly in `Authorization: Bearer ...` on API requests.
Its internal encoding is standard base64 of `{ "id": ..., "secret": ... }`, as
accepted by `Envelope` and `server::exchange_refresh_token`.

Every authenticated use renews its 30-day inactivity window through the existing
exchange function. There is no separate OAuth `refresh_token`, refresh grant, or
rotation. `expires_in` is omitted because the lifetime slides with use. After
inactivity expiry or revocation, clients must authorize again. Concurrent API
requests can use the same credential.

The sole scope, `estuary`, grants the user's full existing authority, including
catalog operations, data access, and credential management, subject to current
Estuary grants. Future grants are also available to the credential. The sole
resource is the configured issuer origin without its trailing slash; it identifies
the Estuary platform. Omitting either parameter selects these defaults. Other
scope/resource values are rejected. This does not provide read-only, tenant-only,
or MCP-only access.

Client ID, exact redirect URI, resource, scope, and S256 PKCE are checked before
code consumption. The resulting bearer is an ordinary Estuary credential: it is
not sender-constrained or restricted to its originating client. Its user can list
and revoke it with existing `refreshTokens` / `revokeRefreshToken` operations. The
detail contains the client name and ID. Revocation stops subsequent bearer
exchanges; JWTs separately obtained through the existing exchange API retain
their existing lifetime. Independently created credentials are not revoked with
this credential.

## Persistence

The migration adds only `internal.oauth_requests` and its cleanup function.
Existing refresh-token storage and exchange are unchanged.
Requests expire after ten minutes. Codes contain 256 random bits; only their
SHA-256 hashes are stored and they expire after one minute. Consent creates no
refresh credential. The code hash and expiry are nullable fields on the request
row. Redemption
locks that row, validates the bindings, and clears the unexpired code in the same
transaction that inserts the final multi-use credential. Failure rolls both
operations back. Concurrent replay has one winner.

Consent locks the request and records one decision. Duplicate or conflicting
submissions fail with `invalid_grant`; they cannot mint another code. Plaintext
codes are not retained for response retries. The agent invokes cleanup every five
minutes, even when OAuth is disabled, and stops the loop on shutdown. Expiration
is checked during use, independently of cleanup. Cleanup clears expired codes
without clearing consent decisions, then deletes expired requests.

## Configuration and rollout

Set agent `--oauth-config /path/oauth.json` or `OAUTH_CONFIG` to a JSON file:

```json
{
  "issuer": "https://api.example.com",
  "dashboard": "https://dashboard.example.com/oauth/consent",
  "clients": {
    "example-client": {
      "client_id": "example-client",
      "client_name": "Example client",
      "redirect_uris": ["https://client.example.com/callback"],
      "token_endpoint_auth_method": "none",
      "grant_types": ["authorization_code"],
      "response_types": ["code"]
    }
  }
}
```

The issuer must be an HTTPS origin; the dashboard must be HTTPS without query or
fragment. Configuration is validated at startup. Without the file, OAuth handlers
fail closed. Issuer and resource are configuration-derived, never taken from Host
or forwarded headers. Keep the issuer stable across restarts and deployments.

The optional `clients` map provides operator-managed registrations. Otherwise the
client ID must be a public HTTPS metadata URL on port 443, with a non-root path
and no query, fragment, or userinfo. Its JSON document has the same fields as the
example metadata above and must have an exactly matching `client_id` and auth
method `none`. Additional metadata is ignored. Metadata is fetched anew for each
authorization and the validated consent details are retained in the request.
Metadata names are client-supplied labels, not verified publisher identities.

Fetches have a five-second deadline, a 64-KiB limit, and a per-process concurrency
limit. Redirects and proxies are disabled and vetted public DNS results are pinned.
Private and special-use addresses are rejected. Callbacks require exact registered
URIs: HTTPS, or HTTP literal loopback addresses for native clients. Wildcards,
dynamic-port matching, localhost DNS, and custom schemes are not supported.

Rollout order:

1. Apply the migration before deploying the agent cleanup loop.
2. Deploy the dashboard contract below and configure its origin in the existing
   agent CORS allowlist. Deploy OAuth disabled until the dashboard is ready.
3. Configure the issuer and dashboard, expose discovery and `/oauth/*` through
   HTTPS ingress, and apply normal public-endpoint rate limits. Suppress query
   strings, authorization headers, and token/consent bodies in ingress logging.
4. Smoke-test approval, denial, direct bearer use, inactivity expiry, and revocation.
5. Enable client discovery. To stop new issuance, remove `OAUTH_CONFIG`; existing
   credentials remain valid until revoked or expired. Keep the additive migration.

The MCP resource server belongs outside this module. Its protected-resource
metadata should point `authorization_servers` to the configured issuer and use
the supported Estuary resource and `estuary` scope. This checkout has no MCP
resource-server implementation to update. Any MCP integration must forward the
opaque bearer through Estuary's existing authentication path, without assuming
it is a JWT. Advertising a distinct MCP-only resource would require additional
resource enforcement and is not supported by this permission model.

## Separate `estuary/ui` integration contract

The browser arrives at the configured dashboard URL with only `request_id=<uuid>`.
Preserve that ID through login. Use the normal authenticated user session for:

| Operation | Request | Response |
| --- | --- | --- |
| Load consent | `GET /oauth/consent/{request_id}` with bearer authentication | `request_id`, `client_id`, `client_name`, `redirect_uri`, `resource`, `scope`, `permissions` |
| Approve or deny | `POST /oauth/consent/{request_id}`, bearer authentication, JSON `{ "approve": true }` or `{ "approve": false }` | `{ "redirect_uri": "validated client callback with code or error, state, and iss" }` |

Loading consent binds the request to the logged-in user. Account switching requires
restarting authorization. Restricted sessions and service accounts cannot consent.
Display the server-returned client ID/domain and callback alongside the untrusted
client name, and make the full-authority permission statement explicit. Render
metadata as text and require an explicit user decision; do not auto-approve.

After the decision, navigate the browser to the returned `redirect_uri`. Do not
construct callbacks from dashboard query parameters or exchange codes in the UI.
The client alone redeems the code using its PKCE verifier. If a decision response
is lost, expired, or duplicated, show that authorization must restart; do not
blindly resubmit. Keep this page free of third-party scripts/analytics and use
`Referrer-Policy: no-referrer` and frame restrictions. API consent requires a
bearer header and JSON; it does not use ambient cookie authentication.

The client's token request is form-encoded `POST /oauth/token` with
`grant_type=authorization_code`, `client_id`, `redirect_uri`, `code`, and
`code_verifier`, plus optional matching `resource` and `scope`. Discovery is at
`/.well-known/oauth-authorization-server`. Dynamic client registration, client
secrets, implicit grants, and OpenID Connect are not implemented.

`test.rs` exercises the full HTTP flow, bearer renewal and revocation, expiry,
duplicate consent, concurrent code replay, and transactional mint failure.
Run through `mise exec -- cargo nextest run -p control-plane-api -E 'test(server::oauth::)'`
with this checkout's local stack stopped and Supabase running.
