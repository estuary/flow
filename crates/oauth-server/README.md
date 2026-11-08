# OAuth authorization service

This crate lets users give an application (like claude.ai) access to Estuary
through the dashboard. `src/lib.rs` handles approval and issues the tokens the
application uses to make requests and renew its access. `src/cimd.rs` downloads
and checks the application's published information, including its name and the
addresses the browser may return to after approval.

The dashboard displays the approval page. The services receiving application
requests check the access tokens. This crate relies on the control plane to
check which users may approve access and to create and exchange credentials.
The `Backend` trait defines that connection;
`control-plane-api::server::public::oauth` implements it.

Consent tickets and authorization codes carry encrypted state, so the server
does not store pending requests or codes. Credentials use the existing Estuary
refresh-token feature.

## Configuration

Enable the agent's OAuth service by setting all three options. Omitting all
three disables it; partial configuration is rejected.

| Flag                     | Environment variable   |
| ------------------------ | ---------------------- |
| `--oauth-issuer`         | `OAUTH_ISSUER`         |
| `--oauth-consent-url`    | `OAUTH_CONSENT_URL`    |
| `--oauth-encryption-key` | `OAUTH_ENCRYPTION_KEY` |

The encryption key is the base64url encoding, without padding, of 64 random
bytes.

`Config` validates the issuer, consent URL, and encryption key. Share the key
across replicas; replacing it invalidates pending tickets and codes, but not
refresh tokens.

## Endpoints and dashboard integration

| Endpoint                                      | Contract                                                                                                                                |
| --------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `GET /.well-known/oauth-authorization-server` | OAuth discovery metadata.                                                                                                               |
| `GET /oauth/authorize`                        | Starts authorization and redirects to `consent_url?request=<ticket>`.                                                                   |
| `GET /oauth/consent-context?request=<ticket>` | Returns `{request, client_name}`; tickets are time limited.                                                                             |
| `POST /oauth/authorize`                       | Accepts `{request, approved}` with an Estuary bearer; returns `{redirect_uri}` containing state, issuer, and a code or `access_denied`. |
| `POST /oauth/token`                           | Exchanges a form-encoded authorization code with PKCE verifier, or a refresh token, for access and refresh tokens.                      |

The dashboard must display the client name and client-ID hostname as untrusted
text, require an explicit decision, and navigate to the returned callback.

## Credentials and limitations

Approval creates a rotating `public.refresh_tokens` credential and encrypts it
into an authorization code carrying the client ID, redirect URI, and PKCE challenge.
Code exchange uses `token_exchange::generate_access_token` to redeem that
credential for an access token and a rotated refresh credential.

- Applications receive the user's existing permissions; requested `scope` and
  `resource` values do not narrow access.
- Restricted tokens and service accounts may not give oauth consent.
- Anyone carrying an unrestricted user token could give oauth consent without
  showing the dashboard.
- Concurrent exchanges of the same authorization code can both succeed.
- Abandoned approvals leave refresh-token rows; no OAuth-specific cleanup exists.

## CIMD and callback safety

[`src/cimd.rs`](src/cimd.rs) owns client URL, metadata, and callback validation.
Metadata fetches pin validated public IPv4 addresses, disable redirects and
proxies, and limit response size and fetch duration. Fetches
have no cache or rate limit. Custom callback schemes and dynamic registration
are unsupported.
