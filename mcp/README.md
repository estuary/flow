# Estuary MCP server

A hosted [Model Context Protocol](https://modelcontextprotocol.io) server for
Estuary Flow, built against the **2026-07-28 stateless MCP spec** with **CIMD**
(Client ID Metadata Documents) for client identification.

Status: **prototype**. One curated tool, and that is on purpose — the tool is an
acceptance test for the authorization machinery, which is the actual subject of
this package. Read [The trust model](#the-trust-model) before changing anything
under `auth/`.

## The trust model

This process is a **stateless, untrusted protocol translator**. It is *not* a
control-plane component, and the whole design follows from that:

- **No secret the control plane trusts, no client secrets, no user credential
  store, no durable state — and no per-request state either** (see
  [The sealing key](#the-sealing-key) for the one key it does hold, and why that
  does not breach this).
- **It does not verify JWTs.** Every tool call forwards the caller's bearer token
  verbatim to the control plane, which already performs the single authoritative
  verification (`crates/control-plane-api/src/envelope.rs`). This costs no extra
  round trip: every tool call was going to reach the control plane anyway.
- **A customer could self-host it.** That is the invariant that polices
  trust-creep. Any proposal that gives this component a privileged capability —
  a key the control plane honours, a shared token store, a database — breaks
  self-hostability and should be rejected loudly rather than negotiated.

### On the spec's audience-validation requirement

The 2026 MCP spec says an MCP server MUST validate the audience of tokens it
accepts. This adapter does not check a signature, and that is a considered
reading rather than an omission:

The *resource server* here is the composite of adapter **plus** control plane.
Validation happens exactly once, at the actual trust boundary, performed by the
component that owns the resources and holds the keys. What the spec's requirement
is defending against is token passthrough into a *foreign* trust domain — a proxy
laundering a token to a third party that will honour it for something the user
never agreed to. The control plane is the same first-party domain, deliberately
and by construction; there is nowhere else for a token to go. A ceremonial
signature check inside an untrusted proxy would add a second thing to keep
correct without adding a second thing that has to be true.

The deferred hardening that would make this literal rather than argued is listed
under [Deferred](#deferred).

## Token architecture: one unified domain

MCP clients hold **real Estuary credentials**. There is no second token domain
and no mapping table — that absence is what keeps this component stateless.

| Credential | What it is | Lifetime | Where it lives |
|---|---|---|---|
| Access token | Estuary control-plane JWT (HS256, `aud: "authenticated"`, claims `sub`/`role`/`iat`/`exp`) | 1 hour, fixed in SQL | The MCP client; forwarded verbatim on every tool call |
| Refresh token | Estuary refresh token: base64 of `{"id", "secret"}` — opaque to the client, as OAuth intends | 90 days, **single-use and rotating** | The MCP client, re-persisted on every rotation |

The adapter's `/oauth/token` refresh grant is a pure translation: decode the
blob, call `POST /api/v1/auth/token`, re-encode whatever the control plane
rotated to, return a standard OAuth token response. Single-use rotation *is*
OAuth 2.1's refresh-token rotation for public clients, so a stolen refresh token
is usable at most once and its theft is detectable — the legitimate client's next
refresh fails.

**Costs accepted knowingly.** A leaked access token is a full control-plane
credential for an hour; a leaked refresh token is a 90-day root sitting in a
desktop application's storage. We take that in exchange for zero mapping and
zero adapter state. And note the one rule this buys: **never forward a
refresh-token blob as a bearer credential.** The control plane's envelope would
accept it, at the cost of a bcrypt verify and a database write per request — so
`server/verifier.py` rejects non-JWT bearers locally.

## The flow

Four participants: the **MCP client** (Claude Code, Codex), this **adapter**, the
**dashboard** (which owns login and consent), and the **agent** (control plane).

```
client                adapter                dashboard          agent
  │  POST /mcp          │                        │                │
  │────────────────────>│                        │                │
  │  401 + WWW-Authenticate → RFC 9728 metadata  │                │
  │<────────────────────│                        │                │
  │  GET /authorize (PKCE S256, client_id = CIMD URL)             │
  │────────────────────>│ fetch + validate CIMD  │                │
  │                     │ seal the request → state                │
  │  302 to dashboard   │                        │                │
  │<────────────────────│                        │                │
  │  (browser) ─────────────────────────────────>│ login + consent│
  │                     │  GET /oauth/consent-context             │
  │                     │<───────────────────────│                │
  │                     │  302 back with a handoff blob           │
  │                     │<───────────────────────│                │
  │                     │  redeem handoff (single-use, PT5M) ────>│
  │                     │  createRefreshToken (P90D, single-use) >│
  │  302 to loopback: code = the sealed, unexchanged refresh token│
  │<────────────────────│                        │                │
  │  POST /token (code + PKCE verifier)          │                │
  │────────────────────>│  first exchange of the client token ───>│
  │  Estuary access + refresh tokens             │                │
  │<────────────────────│                        │                │
```

Steady state after that: hourly refresh through `/oauth/token`, and bearer
pass-through on every tool call.

The adapter holds nothing between any two hops of that dance. The `state` that
rides through the dashboard is the authorization request itself, sealed; the
authorization code is the client's freshly minted — and deliberately not yet
exchanged — refresh token, sealed (`auth/sealed.py`). Any replica can serve any
hop, so replicas need no shared store and no session affinity, only a shared
sealing key. Single-use guarantees live where the state lives, in the control
plane: a replayed dashboard callback re-presents a handoff its first redemption
consumed, and a replayed authorization code re-presents a refresh token its
first redemption rotated. Both die there — the same mechanism, deliberately,
that already enforces single-use refresh in the steady state.

### The sealing key

Sealed blobs are AES-256-GCM under `ESTUARY_MCP_SEALING_KEYS`. Confidentiality
matters because the code blob carries a credential through the loopback redirect
and the browser's history; authenticity matters because the state blob is the
proof that `/authorize` actually validated the CIMD document and redirect_uri.

The key is **not** a control-plane credential, and holding it does not breach
the trust model: it grants zero standing access to Estuary, the control plane
never sees or honours it, and a self-hoster generates their own. What a leaked
key actually costs — worth stating plainly: an attacker holding it can read
tokens out of authorization codes they intercept during the codes' sixty-second,
single-use life, and can forge state blobs, i.e. skip `/authorize`'s CIMD
validation when steering a victim toward consent. Consent-phishing is already
available to anyone willing to host a real CIMD document; the consent screen
showing the client's *hostname* is the mitigation in both worlds.

Unset, the adapter generates an ephemeral per-boot key: correct for a single
replica, where a restart costs in-flight dances a re-click. Replicas must share
keys. The variable is a comma-separated list — the first key seals, all keys
unseal — so rotation is "deploy `new,old`, wait out the ten-minute in-flight
window, drop `old`". Generate one with:

```bash
python3 -c 'import os, base64; print(base64.b64encode(os.urandom(32)).decode())'
```

### Why the dashboard is in the loop

The adapter cannot authenticate a user — it holds no session, no password
verifier, and no GoTrue credentials, and giving it any of those would end the
trust model. The dashboard already has all three. So the adapter parks the
authorization request, sends the browser to the dashboard, and gets back a
credential minted *as the user who logged in there*. The handoff is single-use
and five minutes long, so an intercepted callback URL is inert: the adapter's own
redemption consumes it.

### Dashboard handoff contract

Paired change in `estuary/ui`: `src/pages/McpAuthReq.tsx`, route `/mcp-auth`.

| Direction | URL |
|---|---|
| Adapter → dashboard | `{dashboard}/mcp-auth?adapter={origin}&state={handoff_state}` |
| Dashboard → adapter (read) | `GET {adapter}/oauth/consent-context?state={handoff_state}` |
| Dashboard → adapter (approve) | `{adapter}/oauth/dashboard-callback?state={…}&handoff={base64 blob}` |
| Dashboard → adapter (deny) | `{adapter}/oauth/dashboard-callback?state={…}&error=access_denied` |

Two decisions in there are load-bearing:

- **The adapter passes its origin, not a full callback URL.** The dashboard has
  exactly one value to check against `VITE_MCP_ALLOWED_ADAPTER_ORIGINS`, and
  derives every other URL from it by fixed path. That check is what stands
  between a user and a token-exfiltration link.
- **The consent screen's text is fetched from the adapter, not read from the
  redirect's query string.** Those strings come from the CIMD document the
  adapter fetched and validated, so a crafted link to `/mcp-auth` cannot make the
  consent screen name a client that was never requested. The dashboard renders
  the client's `client_name` *and* the hostname that served its document — the
  name is attacker-chosen, the hostname is not.

The handoff is a **top-level navigation**, never a `postMessage`. The dashboard's
connector-OAuth popup helper broadcasts with a wildcard target origin; using it
here would publish the credential to whatever page happens to be listening.

## Layout

```
src/estuary_mcp/
  config.py          Env-driven Settings. Every URL is configuration.
  credentials.py     Pure encode/decode of Estuary credentials. No IO.
  control_plane.py   The only outbound IO: token exchange, GraphQL, 307 absorption.
  app.py             Composition root: one ASGI app, two halves.
  __main__.py        Entry point.
  auth/              The authorization-server facade — the point of the exercise.
    routes.py          /authorize, /token, consent context, dashboard callback, RFC 8414 metadata.
    cimd.py            Client ID Metadata Documents: fetch, validate, SSRF guard, loopback matching.
    sealed.py          AEAD-sealed in-flight authorization state, and PKCE.
  server/            The MCP resource.
    verifier.py        The pass-through TokenVerifier.
    tools.py           The tools. There is one.
```

The `auth` and `server` halves share a process and nothing else. Splitting them
into separate deployables should be a routing change plus pointing `issuer_url`
at the other origin; if a change makes that untrue, it is going the wrong way.

The AS facade is hand-written Starlette rather than the SDK's
`OAuthAuthorizationServerProvider`, and FastMCP's `OAuthProxy` is reference
reading only — its architecture (its own token domain, a server-side store of
upstream tokens) is exactly what this design rejects.

### Snapshot staleness

The agent authorizes from an in-memory snapshot of the grant graph. When a
request's authorization fails against a snapshot older than the request, it
answers `307` with `started`/`retryAfter` and a `Retry-After` header, meaning
"ask again once I've refreshed". `ControlPlane.request_with_retry` absorbs that
protocol entirely — an MCP client must never see one, because MCP has no
vocabulary for it.

## The tool

`prefixes` takes no arguments and lists the catalog prefixes the authenticated
user can reach, with their capabilities. It was chosen as the simplest call that
*cannot* succeed without a valid control-plane access token belonging to a real
user, so one green tool call proves the whole chain: CIMD identification →
dashboard consent → credential mint → rotation → bearer pass-through →
control-plane verification → grant-graph authorization.

Adding tools is the easy part and should wait until the authorization story is
settled. Every tool follows the same shape: take the caller's bearer from the
request context, hand it to the control plane, return what comes back. Nothing
in `server/` should ever acquire a credential of its own.

## Configuration

| Variable | Required | Meaning |
|---|---|---|
| `ESTUARY_MCP_PUBLIC_URL` | yes | How the *outside world* reaches this process. The OAuth issuer, the RFC 9728 resource base, and the origin of every redirect. |
| `ESTUARY_MCP_AGENT_URL` | yes | Control-plane agent base URL. |
| `ESTUARY_MCP_DASHBOARD_URL` | yes | Dashboard base URL; owns login and consent. |
| `ESTUARY_MCP_BIND_HOST` | no (`127.0.0.1`) | Listen address. |
| `ESTUARY_MCP_BIND_PORT` | no (`8080`) | Listen port. |
| `ESTUARY_MCP_CLIENT_TOKEN_VALIDITY` | no (`P90D`) | ISO-8601 validity of the client's refresh token. |
| `ESTUARY_MCP_SEALING_KEYS` | no (ephemeral per-boot key) | Comma-separated base64 32-byte keys sealing in-flight authorization state; first seals, all unseal. Required to be shared across replicas. See [The sealing key](#the-sealing-key). |
| `ESTUARY_MCP_ALLOW_INSECURE_CIMD` | no (`false`) | **Test only.** Permits `http://` and loopback `client_id` URLs and skips the SSRF guard. |
| `ESTUARY_MCP_LOG_LEVEL` | no (`INFO`) | |

`ESTUARY_MCP_PUBLIC_URL` is the one that goes wrong. Under a port-forward or a
tunnel it is the *host's* view, which need not equal the bind address; OAuth
metadata is compared by exact string, so a stray trailing slash or a wrong port
surfaces as an opaque client-side "issuer mismatch".

## Running it locally

The adapter is part of the local stack, at port `base+22` (see
`mise run local:stack-info`):

```bash
mise run local:stack            # brings the adapter up with everything else
mise run local:mcp              # or just the adapter
journalctl --user -u flow-mcp@<stack> -f
```

From your host machine, after `mise run vm:port-forward <host> <stack>` — the
adapter's port is forwarded **identity**, never remapped, because it bakes its
own origin into OAuth metadata and the two sides must agree:

```bash
claude mcp add --transport http estuary http://localhost:<base+22>/mcp
```

The dashboard runs on the host and needs the adapter's origin allowlisted:

```
VITE_MCP_ALLOWED_ADAPTER_ORIGINS=http://localhost:<base+22>
```

Local login is a magic link (`VITE_SHOW_EMAIL_LOGIN=true`); mail lands in Mailpit
at `base+13`. Seeded users have no identities — sign up a fresh address, or run
`mise run local:test-tenant --user <email>` to provision one with a tenant.

Note that restarting Supabase resets the database, so users and refresh tokens
created while testing vanish. Re-provision rather than debugging ghosts.

### Cloud-originated clients: an HTTPS tunnel

A port-forward only works for clients whose transport runs on the developer's
machine. A cloud-originated one — claude.ai's connectors, chatgpt.com's — cannot
see a loopback forward and rejects `http://` URLs outright. Which clients those
are is not obvious from their packaging; see
[Clients known to work](#clients-known-to-work). Such a client needs the adapter
on a public HTTPS origin:

```bash
mise run local:tunnel                      # cloudflared quick tunnel
```

The tunnel itself is the easy half. What the task actually exists for is the
*reconfiguration*: it learns the assigned URL, layers it over the adapter's
`ESTUARY_MCP_PUBLIC_URL` (a systemd drop-in, so `local:stack` cannot regenerate
it away and `local:stop` sweeps it), restarts the adapter, and then verifies
through the tunnel that the announced issuer is the tunnel's own origin — the
one mistake that surfaces as an opaque client-side "issuer mismatch". Ctrl+C
restores the loopback URL. Quick-tunnel hostnames are assigned per run, so the
URL must be re-pasted into the client and the UI's env on every restart.

**The tunnel must not be browser-hostile.** ngrok's free tier answers
browser-originated requests with an HTML abuse interstitial
(`ERR_NGROK_6024`), and that breaks this flow structurally rather than
cosmetically. The consent screen fetches `/oauth/consent-context` cross-origin;
the interstitial's response carries no `Access-Control-Allow-Origin`, so the
browser reports only `TypeError: Failed to fetch`. Neither escape works: the
dashboard cannot send `ngrok-skip-browser-warning` without putting a tunnel
vendor's name into product code, for a fetch that exists for a security reason;
the cookie a human earns by clicking "Visit Site" does not ride on a
credential-less cross-origin XHR; and a traffic policy that injects the header
does not help, because the interstitial is applied at ngrok's edge *before*
policy actions run. `--provider ngrok` is therefore only viable on a plan that
drops the interstitial. The task probes for this directly — a browser-UA,
cross-origin request to `/oauth/consent-context`, asserting the *adapter*
answered — so a hostile edge fails at setup rather than mid-dance.

Only the *adapter* is published. The dashboard stays on your machine, because
the browser leg of the dance is your browser: the adapter still redirects to
`FLOW_DASHBOARD_ORIGIN`, and the UI needs the tunnel's origin allowlisted in
`VITE_MCP_ALLOWED_ADAPTER_ORIGINS` (see above). The control-plane agent is
never exposed; the adapter calls it in-process inside the VM.

While the tunnel is up, `/oauth/authorize` is world-reachable. It is not an open
redirect (CIMD validation precedes every redirect), but it will steer anyone who
finds it at your dashboard's login page. Close it when done.

## Tests

```bash
uv run --directory mcp pytest            # unit + headless end-to-end, no stack needed
```

`tests/test_oauth_flow.py` walks the whole dance headlessly against a faithful
fake control plane, standing in for the browser and the dashboard. It runs real
HTTP servers on loopback ports rather than an in-process ASGI transport, on
purpose: the CIMD fetch and the redirect chain *are* the subject, and a transport
shim would stub out exactly those.

`tests/test_live_stack.py` is the acceptance test. It runs against a live stack
*and the real internet* — it fetches Claude Code's actual published client
metadata document — and is skipped unless you opt in:

```bash
mise run local:test-tenant --tenant acmeCo --user alice@example.com
set -a; source ~/flow-local/<stack>/test-tenant-acmeCo.env; set +a
ESTUARY_MCP_LIVE_ADAPTER=http://localhost:<base+22> uv run --directory mcp pytest tests/test_live_stack.py -v
```

That test earned its keep immediately: Claude Code registers
`http://localhost/callback` with *no port at all*, which only matches under
RFC 8252 §7.3 port-agnostic comparison.

## Clients known to work

Verified by hand against a local stack behind an HTTPS tunnel. The column that
matters is the third one: **every one of these selected CIMD, and not one
attempted `POST /register`.** That is the evidence for betting the design on
CIMD-only; before it, the bet was an argument.

| Client | `client_id` document | `redirect_uri` | Transport from |
|---|---|---|---|
| Claude Code | `claude.ai/oauth/claude-code-client-metadata` | `http://localhost/callback` (no port) | your machine |
| Claude (claude.ai in a browser) | `claude.ai/oauth/mcp-oauth-client-metadata` | `https://claude.ai/api/mcp/auth_callback` | Anthropic's cloud |
| Codex (ChatGPT desktop app) | `chatgpt.com/oauth/codex/<opaque>/client.json` | `http://127.0.0.1:<ephemeral>/callback/<opaque>` | your machine |
| ChatGPT (chatgpt.com in a browser) | `chatgpt.com/oauth/<opaque>/client.json` | `https://chatgpt.com/connector/oauth/<opaque>` | OpenAI's cloud |

**The last column decides whether you need a tunnel at all**, and it is not
"desktop app versus browser" — ChatGPT's desktop app calls `/mcp` from the
developer's own machine (and redirects to loopback), so it works over
`mise run vm:port-forward` against `http://localhost:<base+22>/mcp`, exactly like
Claude Code. Only genuinely cloud-originated transports need
`mise run local:tunnel`. Determine which you have by reading the source address
of the `POST /mcp` lines in the adapter's log rather than by guessing from the
client's packaging.

Three more things that fall out of the table and are easy to get wrong:

- **One vendor, several clients, several documents.** Anthropic publishes a
  different document per product; the two are not interchangeable, and pinning
  only Claude Code's (as `test_live_stack.py` does) tests one of them.
- **OpenAI's documents are per-*connector*, not per-product.** The opaque path
  segment is minted when a user adds the connector, so `client_id` is not a
  stable product identity. The consequence lands on the token `detail`
  (`auth/routes.py`), which records the `client_id` verbatim so a user can see
  and revoke one client: for OpenAI clients that renders as an opaque
  `https://chatgpt.com/oauth/pBqTePPCi2Zt/client.json`, which is not legible.
  Recording `client_name` and `display_host` alongside it would fix that; the
  metadata is already validated and in hand at mint time.
- **ChatGPT probes `/.well-known/openid-configuration` and tolerates a 404.**
  Expected noise, not a missing endpoint. It proceeds to RFC 8414 discovery.

Codex is also the only client so far to present a loopback redirect with *both*
an ephemeral port and a path (`/callback/<opaque>`), exercising §7.3 comparison
in the shape that matters: port ignored, path matched exactly.

## Deferred

Deliberately not built. Each is a real gap, not an oversight:

- **`aud`-array hardening.** Mint MCP-flow tokens with an `aud` array containing
  the MCP resource URI, relaxing the agent's check to "contains `authenticated`"
  (`crates/control-plane-api/src/envelope.rs`). This is what would make the
  audience story literal rather than argued.
- **RS256/JWKS.** The multi-key verify path already exists (`crates/tokens/src/jwt.rs`).
- **CIMD fetch TOCTOU.** `_assert_safe_target` resolves the host, and httpx
  resolves it again when it connects; a DNS entry that flips between the two
  would slip past. Closing it means pinning the resolved address into the
  connection.
- **Dynamic client registration (RFC 7591).** Not implemented, and not an
  oversight: a client registry is durable state, which this component does not
  get to have. The metadata document advertises
  `client_id_metadata_document_supported` precisely so a client picks CIMD
  instead. A client that speaks *only* DCR cannot connect at all — the design's
  one hard compatibility edge. It has not been hit yet: see
  [Clients known to work](#clients-known-to-work).
- **A scope vocabulary.** Estuary authorization is a server-side grant graph.
  What an MCP scope should mean here — capability bits? `pg_role`? — is a real
  design conversation, and inventing scope strings that narrow nothing would be
  theatre.
- **A self-hoster paste-a-token login page**, for deployments with no dashboard.
