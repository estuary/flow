# OAuth authorization server

`server::oauth::router` serves OAuth for public MCP clients. `Config` enables
it explicitly; without configuration the endpoints fail closed. The agent's
`OAUTH_ISSUER_URL` is a public HTTPS origin (HTTP loopback is allowed locally),
`OAUTH_CONSENT_URL` is the dashboard consent page, and `OAUTH_RESOURCE_URLS` is
a comma-separated list of exact resource URIs, such as
`https://mcp.example.test/mcp`. Never derive these values from a request Host
header. `OAUTH_MCP_SERVICE_SECRETS` is required when OAuth is enabled: a
comma-separated list of shared service secrets, each at least 32 printable ASCII
characters without commas. Generate random secrets (for example `openssl rand -hex 32`)
and distribute them through secret storage. The list applies to the configured
resources, which must share the same trusted MCP service boundary. During rotation,
add the new secret to Flow, update MCP, then remove the old secret from Flow.

Every request carrying an OAuth resource bearer must also provide
`X-MCP-Service-Secret`. `Envelope` rejects missing, duplicate, or incorrect secrets
before API handlers run. Ordinary dashboard sessions and OAuth token exchanges do
not require this header. Never give the service secret to OAuth clients or sandbox
commands; capability-masked tokens deliberately issued to sandboxes retain their
existing authorization rules. Use HTTPS outside loopback and redact this header
from proxy logs and tracing. MCP supplies its configured secret, replacing any
caller-provided header, and must not forward credentials across origins.

`OAUTH_CLIENTS_JSON` optionally supplies pre-registered public clients:

```json
[{"client_id":"example-cli","client_name":"Example CLI","redirect_uris":["http://127.0.0.1/callback"],"token_endpoint_auth_method":"none"}]
```

Other clients use HTTPS Client ID Metadata Documents (CIMD). `clients.rs`
validates identity and redirects, rejects private/special-purpose DNS answers,
and pins the checked addresses for the fetch. Fetches have time and size
limits and cannot use redirects or proxies. A client's display name is
self-asserted; the dashboard must also display `client_host` (or the configured
`client_id` for a pre-registered client).

## Endpoints and dashboard contract

- `GET /.well-known/oauth-authorization-server`: RFC 8414 metadata.
- `GET /oauth/authorize`: `response_type=code`, `client_id`, `redirect_uri`,
  `resource`, `code_challenge`, `code_challenge_method=S256`, optional `state`
  only. OAuth `scope` parameters are ignored on authorization, code exchange,
  and refresh. Every approved grant issues full-access tokens within the user's
  current permissions, with no capability mask. Neither metadata endpoint
  advertises scopes.
- The browser redirects to `OAUTH_CONSENT_URL?request=…&issuer=…`.
  The dashboard must allowlist the issuer before sending its session bearer
  to it, retain the request through login, and obtain consent text from
  `GET {issuer}/oauth/consent-context?request=…` with its session bearer.
  The response contains `client_id`, `client_name`, `client_host`, and `resource`,
  with no scope field. The consent screen must clearly state that the
  application can act with all of the user's existing permissions.
- Approve or deny with an authenticated JSON
  `POST {issuer}/oauth/consent`, body `{"request":"…","approve":true}`.
  Its response is `{"redirect_uri":"…"}`. Navigate the browser to this URL.
  It contains a short-lived code (or `error=access_denied`), the client's
  original `state`, and RFC 9207 `iss`. The dashboard never creates a refresh
  token and never sends a session/access/refresh token in a navigation URL.
- `POST /oauth/token`: URL-encoded authorization-code grant with `client_id`,
  `code`, `redirect_uri`, `code_verifier`; or refresh grant with `client_id`
  and `refresh_token`. Optional `resource` must match the grant.
  Returns `access_token`, `refresh_token`, `token_type=Bearer`, and `expires_in`. Clients must persist the replacement refresh token on every use.
- `POST /oauth/revoke`: URL-encoded `client_id` and refresh `token` revokes
  the grant. Unknown tokens also succeed (RFC 7009). Access-token revocation
  is not supported; already-issued access tokens expire after one hour.
- Authenticated `GET /oauth/grants` lists the user's active connections.
  `DELETE /oauth/grants/{id}` revokes an owned connection. These APIs require
  the same unrestricted user session as consent.

## Credentials and resource validation

Authorization requests expire after ten minutes, codes after sixty seconds,
and grants after ninety days (absolute, not sliding). `internal.oauth_*`
tables contain hashes of random 256-bit request IDs, codes, and refresh tokens.
Requests are consumed atomically on consent. Token redemption locks the grant,
checks client/resource/redirect/PKCE binding, and consumes or rotates the
credential in one transaction. Reusing a spent code with its valid PKCE verifier,
or a spent refresh token, revokes the grant. Spent refresh hashes remain until
grant expiry. Expired records are pruned during authorization requests.

Access tokens are signed with the control plane's existing key. They carry
`iss`, resource-specific `aud`, and `client_id`. They omit both `scope` and
`capability_mask`. Their role prevents PostgREST use. `Envelope` accepts these
bearers only for explicitly configured resources and the configured issuer;
capability checks apply current grants and any mask. OAuth bearers cannot create
independent refresh credentials or approve another OAuth grant.
The MCP adapter and control plane jointly serve these resources: an adapter
checks that the bearer audience equals its own resource URI, then forwards the
unchanged bearer to the control plane for signature and permission checks.
Do not forward these tokens to unrelated downstream services.

The MCP adapter advertises the issuer in its protected-resource metadata's
`authorization_servers`, advertises its exact configured `resource` URI, and
points unauthenticated clients at that metadata in `WWW-Authenticate`.
The adapter serves no authorization-server routes. Local clients and hosted connector backends use the same OAuth
endpoints and remain responsible for storing their own tokens.

OAuth routes allow CORS without cookies: consent and connection management
require an explicit session Authorization header, while token endpoints require
credentials in the form body. Keep credentials and query strings out of proxy
logs. Apply deployment-level rate limits to authorization, metadata fetching,
and token endpoints. No dynamic client registration, confidential-client grants,
OIDC ID tokens, or public signing-key discovery is provided.

## Tests

`tests.rs` exercises the HTTP flow against PostgreSQL, including consent denial,
expiry, binding failures, resource claims, rotation, replay revocation, and
concurrent redemption. `clients.rs` tests redirect and address boundaries.
Use the checkout's normal isolated test database and nextest group:

```sh
mise exec -- cargo nextest run -p control-plane-api -E 'test(server::oauth)'
```

## Sandbox credential delegation

`POST /api/v1/auth/token` with `grant_type=capability_token` accepts verified
OAuth access tokens, which can request any valid capability bundle. An empty mask gives the sandbox an identity-only credential. Delegated
tokens expire at the earlier of one hour or the source token's expiry, preserve
prefix restrictions, and use audience `authenticated` with the PostgREST-blocked
role. They cannot delegate again or be used as MCP OAuth credentials. The user's current grants still bound every operation.
