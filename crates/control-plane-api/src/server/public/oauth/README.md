# OAuth authorization service

`mod.rs` implements authorization-code + PKCE and refresh-token flows using
existing Estuary credentials. `cimd.rs` fetches and validates HTTPS Client ID
Metadata Documents (CIMD). The dashboard supplies the consent UI; resource
servers handle discovery and access-token verification.

## Configuration

Enable with agent `--oauth-config /path/to/oauth.json` or `OAUTH_CONFIG`:

```json
{
  "issuer": "https://auth.example.com",
  "consent_url": "https://dashboard.example.com/oauth/consent",
  "encryption_key": "<base64url encoding, without padding, of 64 random bytes>"
}
```

The issuer is an HTTPS origin. The consent URL requires HTTPS (HTTP for loopback)
with no credentials, query, or fragment. Share the secret key across replicas;
replacing it invalidates pending tickets and codes, but not refresh tokens.
Allow the dashboard origin with `--allow-origin`.

## Dashboard contract

| Endpoint | Contract |
|---|---|
| `GET /.well-known/oauth-authorization-server` | OAuth discovery metadata. |
| `GET /oauth/authorize` | Validates `response_type=code`, CIMD `client_id`, `redirect_uri`, S256 PKCE challenge, and optional `state`; redirects to `consent_url?request=<ticket>`. |
| `GET /oauth/consent-context?request=<ticket>` | Returns `{request, client_name}`; tickets expire after ten minutes. |
| `POST /oauth/authorize` | Accepts `{request, approved}` with an Estuary bearer; returns `{redirect_uri}` containing state, issuer, and a code or `access_denied`. |
| `POST /oauth/token` | Accepts form-encoded `authorization_code` + code/verifier or `refresh_token` + credential; returns access and refresh tokens. Optional client ID and redirect URI must match during code exchange. |

The dashboard must display the client name and client-ID hostname as untrusted
text, require an explicit decision, and navigate to the returned callback.
Keep session tokens out of URLs and prevent ticket leakage through referrers.

## Credentials and limitations

Approval creates a rotating `public.refresh_tokens` credential and seals it into
a one-minute code bound to the client, redirect URI, and PKCE challenge.
`token_exchange::generate_access_token` redeems it for an ordinary one-hour
Estuary access token and rotated refresh credential. Refresh credentials expire
after ninety days of inactivity and work through existing Estuary exchange APIs.
Revoking them does not invalidate already-issued access tokens.

- `scope` and `resource` are ignored; access uses the user's existing grants.
- Consent rejects restricted tokens and service accounts, but does **not**
  distinguish dashboard login sessions from OAuth-issued user tokens.
- Concurrent redemption can succeed twice; only the last rotated secret survives.
  Sequential replay fails. There is no refresh-token family revocation.
- Abandoned approvals leave refresh-token rows; no OAuth-specific cleanup exists.

## CIMD and callback safety

Metadata requires a matching `client_id`, nonblank `client_name`, and nonempty
`redirect_uris`; `client_secret` is rejected. Fetches have a ten-second deadline
and 5 KiB limit. Public DNS addresses are validated and pinned; redirects and
ambient proxies are disabled. Fetches have no cache or rate limit.

Callbacks require HTTPS (HTTP for loopback), no credentials or fragments, and
an exact metadata match. Loopback hosts and ports may vary. Safety is checked
both before issuing a consent ticket and before returning an approval or denial
callback. Custom callback schemes and dynamic registration are unsupported.
