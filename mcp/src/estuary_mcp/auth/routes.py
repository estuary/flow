"""The OAuth authorization-server facade.

This module is the point of the whole exercise, so it is written to be read.

The adapter is not a real authorization server. It issues no tokens of its own,
signs nothing, and stores no credential — not even transiently between requests.
What it does is *translate*: it speaks OAuth 2.1 + PKCE + CIMD to MCP clients on
one side, and Estuary's own refresh-token exchange to the control plane on the
other, and it borrows the dashboard for the only two things that genuinely
require a human — logging in, and consenting.

The dance, end to end:

    client                adapter                dashboard          agent
      │  GET /authorize     │                        │                │
      │────────────────────>│ fetch + validate CIMD  │                │
      │                     │ seal request → state   │                │
      │  302 to dashboard   │                        │                │
      │<────────────────────│                        │                │
      │  (browser) ─────────────────────────────────>│ login + consent│
      │                     │  GET consent-context   │                │
      │                     │<───────────────────────│                │
      │                     │      302 back w/ handoff blob           │
      │                     │<───────────────────────│                │
      │                     │  redeem handoff (single-use, PT5M) ─────>│
      │                     │  mint client refresh token (GraphQL) ──>│
      │  302 to loopback w/ code (sealed refresh token)               │
      │<────────────────────│                        │                │
      │  POST /token (code + PKCE verifier)          │                │
      │────────────────────>│  first exchange of the client token ───>│
      │  access + refresh tokens (real Estuary ones) │                │
      │<────────────────────│                        │                │

The adapter holds nothing between any two of those requests: the `state` that
rides through the dashboard is the sealed authorization request itself, and the
authorization code is the client's sealed — and not yet exchanged — refresh
token (see `sealed.py`). Any replica can serve any hop. Single-use guarantees
live in the control plane, which is the side that has state: a replayed handoff
or code presents a credential its first use already consumed.

The tokens handed back are genuine Estuary credentials. There is no second token
domain and no mapping table — that absence is what keeps this component
untrusted and self-hostable. See the README.
"""

import logging
import urllib.parse
from typing import Any

import httpx
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response
from starlette.routing import Route

from .. import config, control_plane, credentials
from . import cimd, sealed

log = logging.getLogger(__name__)

# OAuth error codes the dashboard may report back (RFC 6749 §4.1.2.1). The
# dashboard's `error` parameter lands in a redirect to the client, so it is
# mapped through this set rather than echoed: an unrecognized code becomes
# `access_denied`, which is both the truthful summary of "the dashboard did not
# approve this" and a code every client already knows how to handle.
DASHBOARD_ERROR_CODES = frozenset({"access_denied", "server_error", "temporarily_unavailable"})

# GraphQL that mints the client-dedicated refresh token at the end of the dance.
# `multiUse: false` selects OAuth 2.1 refresh-token rotation for public clients:
# each exchange burns the credential and returns its successor, so a stolen
# refresh token is detectable (the legitimate client's next refresh fails) and
# usable at most once.
CREATE_CLIENT_TOKEN = """
mutation EstuaryMcpCreateClientToken($detail: String!, $validFor: String!) {
  createRefreshToken(detail: $detail, multiUse: false, validFor: $validFor) {
    id
    secret
  }
}
"""


class AuthServer:
    """Handlers for the AS facade, bound to one configuration.

    A class rather than closures because the handlers share four collaborators
    (settings, sealer, control plane, HTTP client) and Starlette gives handlers
    no other channel to reach them. It holds no per-request state.
    """

    def __init__(
        self,
        settings: config.Settings,
        agent: control_plane.ControlPlane,
        http: httpx.AsyncClient,
        sealer: sealed.Sealer,
    ):
        self._settings = settings
        self._agent = agent
        self._http = http
        self._sealer = sealer

    def routes(self) -> list[Route]:
        """The facade's routes, for mounting alongside the MCP resource.

        The seam between this and the MCP server is a routing table and nothing
        else: splitting the AS into its own deployable would mean pointing
        `issuer` at another origin and moving these five routes there.
        """
        return [
            Route(
                "/.well-known/oauth-authorization-server",
                self.authorization_server_metadata,
                methods=["GET", "OPTIONS"],
            ),
            Route("/oauth/authorize", self.authorize, methods=["GET"]),
            Route("/oauth/consent-context", self.consent_context, methods=["GET", "OPTIONS"]),
            Route("/oauth/dashboard-callback", self.dashboard_callback, methods=["GET"]),
            Route("/oauth/token", self.token, methods=["POST", "OPTIONS"]),
        ]

    # ---------------------------------------------------------------- metadata

    async def authorization_server_metadata(self, request: Request) -> Response:
        """RFC 8414 authorization-server metadata.

        Hand-written rather than taken from the SDK's model because two of the
        fields below are what make CIMD happen at all, and neither is in RFC 8414:

          * `client_id_metadata_document_supported` — the CIMD draft's discovery
            switch.
          * `token_endpoint_auth_methods_supported: ["none"]` — we are a public
            client's AS; there are no client secrets anywhere in this design.

        Claude selects CIMD only when it sees *both*. Omitting either sends the
        client down the dynamic-client-registration path, which this adapter does
        not implement (a client registry would be durable state).
        """
        if request.method == "OPTIONS":
            return _cors_preflight()

        settings = self._settings
        return _cors(
            JSONResponse(
                {
                    "issuer": settings.issuer,
                    "authorization_endpoint": settings.authorization_endpoint,
                    "token_endpoint": settings.token_endpoint,
                    "response_types_supported": ["code"],
                    "grant_types_supported": ["authorization_code", "refresh_token"],
                    # S256 only. `plain` is no protection against an attacker who
                    # can read the authorization request, which on a loopback
                    # redirect is any local process.
                    "code_challenge_methods_supported": ["S256"],
                    "token_endpoint_auth_methods_supported": ["none"],
                    "client_id_metadata_document_supported": True,
                    # No scope vocabulary for the PoC: Estuary authorization is a
                    # server-side grant graph, and inventing scope strings that
                    # do not narrow anything would be theatre. See README.
                    "scopes_supported": [],
                }
            )
        )

    # --------------------------------------------------------------- authorize

    async def authorize(self, request: Request) -> Response:
        """Start an authorization-code + PKCE flow.

        Note the ordering: nothing is redirected anywhere until the CIMD document
        has been fetched and the presented `redirect_uri` has been matched
        against it. An unvalidated `redirect_uri` is an open redirect, so errors
        before that point are rendered here rather than bounced to the client.
        """
        params = request.query_params

        client_id = params.get("client_id")
        redirect_uri = params.get("redirect_uri")
        if not client_id or not redirect_uri:
            return _oauth_error("invalid_request", "client_id and redirect_uri are required")
        if params.get("response_type") != "code":
            return _oauth_error("unsupported_response_type", "only response_type=code is supported")

        code_challenge = params.get("code_challenge")
        if not code_challenge or params.get("code_challenge_method") != "S256":
            return _oauth_error("invalid_request", "PKCE with code_challenge_method=S256 is required")

        try:
            client = await cimd.fetch_client_metadata(
                client_id, self._http, allow_insecure=self._settings.allow_insecure_cimd
            )
            cimd.assert_redirect_uri_allowed(redirect_uri, client.redirect_uris)
        except cimd.CimdError as err:
            log.warning("rejected authorization for client_id=%s: %s", client_id, err)
            return _oauth_error("invalid_request", str(err))

        # From here the redirect_uri is trusted, so failures can be reported to
        # the client the way OAuth expects.
        #
        # The sealed blob *is* the state: whichever replica the dashboard's
        # browser lands on can open it, and its AEAD tag is the proof that this
        # validation actually ran — a crafted state that skipped CIMD checks
        # cannot exist without the sealing key.
        state = self._sealer.seal_pending(
            sealed.PendingAuthorization(
                client_id=client.client_id,
                client_name=client.client_name,
                display_host=client.display_host,
                client_uri=client.client_uri,
                redirect_uri=redirect_uri,
                client_state=params.get("state"),
                code_challenge=code_challenge,
                # RFC 8707. Recorded for the log trail and for the day tokens
                # carry an audience; a single-resource PoC has nothing to branch on.
                resource=params.get("resource"),
                issued_at=sealed.now(),
            )
        )

        log.info(
            "authorization started: client=%s host=%s resource=%s",
            client.client_name,
            client.display_host,
            params.get("resource"),
        )

        # Hand the browser to the dashboard, which owns login and consent. We pass
        # our own origin (not a full callback URL) so the dashboard has exactly one
        # value to check against its allowlist, and derives every URL it needs from
        # it by fixed path. See README "Dashboard handoff contract".
        handoff = urllib.parse.urlencode({"adapter": self._settings.public_url, "state": state})
        return RedirectResponse(f"{self._settings.dashboard_consent_url}?{handoff}", status_code=302)

    async def consent_context(self, request: Request) -> Response:
        """What the dashboard should show on the consent screen.

        Served to the dashboard's browser, so it needs CORS. It is deliberately
        the *adapter* that supplies these strings rather than the redirect's query
        parameters: they come from the CIMD document this process fetched,
        validated, and sealed into the state — so a crafted link to the dashboard
        cannot make the consent screen name a client that was never requested.

        `client_host` is the security-relevant field. `client_name` is chosen by
        whoever wrote the metadata document; the host is chosen by whoever
        controls DNS and TLS for it. The dashboard must render both.
        """
        if request.method == "OPTIONS":
            return _cors_preflight()

        parked = self._sealer.unseal_pending(request.query_params.get("state", ""))
        if parked is None:
            return _cors(
                JSONResponse(
                    {"error": "unknown or expired authorization request"}, status_code=404
                )
            )

        return _cors(
            JSONResponse(
                {
                    "client_name": parked.client_name,
                    "client_host": parked.display_host,
                    "client_id": parked.client_id,
                    "client_uri": parked.client_uri,
                    "resource": self._settings.resource_url,
                }
            )
        )

    async def dashboard_callback(self, request: Request) -> Response:
        """The browser returning from the dashboard, carrying a handoff credential.

        The handoff is a single-use refresh token valid for five minutes, minted
        by the dashboard as the authenticated user. Redeeming it here does two
        things at once: it proves the browser really came from an authenticated
        consent (an intercepted URL cannot be replayed, because the first
        redemption consumes it), and it yields the bootstrap access token needed
        to mint the client's own long-lived credential.

        The state blob itself is not single-use — nothing here could burn it —
        and does not need to be: completing this handler requires a *fresh*
        handoff, which only the dashboard mints and only for an authenticated,
        consenting user. Replaying a spent callback URL fails at the redemption.
        """
        params = request.query_params
        parked = self._sealer.unseal_pending(params.get("state", ""))
        if parked is None:
            # No redirect target we can trust — the state that would have named
            # one is what failed to open.
            return _oauth_error(
                "invalid_request", "unknown or expired authorization request", status=400
            )

        if error := params.get("error"):
            # The user declined, or the dashboard refused. Report it to the client
            # at its (already validated) redirect_uri.
            reported = error if error in DASHBOARD_ERROR_CODES else "access_denied"
            log.info("authorization declined for client=%s: %s", parked.client_name, error)
            return _redirect_to_client(parked, {"error": reported})

        handoff_blob = params.get("handoff")
        if not handoff_blob:
            return _redirect_to_client(parked, {"error": "invalid_request"})

        try:
            bootstrap = await self._agent.exchange_refresh_token(
                credentials.decode_refresh_token(handoff_blob)
            )
        except (credentials.CredentialError, control_plane.ControlPlaneError) as err:
            log.warning("handoff redemption failed: %s", err)
            return _redirect_to_client(parked, {"error": "access_denied"})

        # The client gets its *own* credential, tagged with the CIMD URL that
        # identifies it. That tag is what makes the user's token list in the
        # dashboard legible, and what they revoke to disconnect one client.
        try:
            data = await self._agent.graphql(
                bootstrap.access_token,
                CREATE_CLIENT_TOKEN,
                variables={
                    "detail": f"MCP client: {parked.client_id}",
                    "validFor": self._settings.client_token_validity,
                },
            )
            minted = data["createRefreshToken"]
            client_refresh = credentials.encode_refresh_token(
                credentials.RefreshToken(id=minted["id"], secret=minted["secret"])
            )
        except (control_plane.ControlPlaneError, KeyError, TypeError) as err:
            log.warning("could not mint the client refresh token: %s", err)
            return _redirect_to_client(parked, {"error": "server_error"})

        # The bootstrap access token is not retained: it was a means to mint the
        # client's credential and nothing else. The minted refresh token is
        # deliberately *not* exchanged here — it rides inside the code, sealed,
        # and `/oauth/token` performs its first exchange after PKCE. That makes
        # the control plane's rotation the code's single-use guarantee: a
        # replayed code presents a credential the first redemption already burned.
        code = self._sealer.seal_code(
            sealed.IssuedCode(
                client_id=parked.client_id,
                redirect_uri=parked.redirect_uri,
                code_challenge=parked.code_challenge,
                refresh_token=client_refresh,
                issued_at=sealed.now(),
            )
        )
        log.info("authorization approved for client=%s", parked.client_name)
        return _redirect_to_client(parked, {"code": code})

    # ------------------------------------------------------------------- token

    async def token(self, request: Request) -> Response:
        """The OAuth token endpoint: authorization-code redemption and refresh.

        Public clients only (`token_endpoint_auth_method: none`), so there is no
        client authentication to perform — PKCE is what binds a code to the
        client that requested it.
        """
        if request.method == "OPTIONS":
            return _cors_preflight()

        form = await request.form()
        grant_type = form.get("grant_type")

        if grant_type == "authorization_code":
            return _cors(await self._grant_authorization_code(form))
        if grant_type == "refresh_token":
            return _cors(await self._grant_refresh_token(form))
        return _cors(
            _oauth_error("unsupported_grant_type", f"unsupported grant_type: {grant_type!r}")
        )

    async def _grant_authorization_code(self, form: Any) -> Response:
        """Redeem an authorization code: PKCE, then the first exchange of the
        client's refresh token.

        A failed PKCE attempt does not invalidate the code — a stateless process
        has nothing to invalidate it in. What stands in for that defense-in-depth:
        the challenge has 256 bits of entropy, the code lives sixty seconds, and
        a *successful* redemption burns the credential inside it at the control
        plane, so guessing after the legitimate client has redeemed yields a code
        that no longer opens anything.
        """
        code = form.get("code")
        verifier = form.get("code_verifier")
        if not code or not verifier:
            return _oauth_error("invalid_request", "code and code_verifier are required")

        issued = self._sealer.unseal_code(str(code))
        if issued is None:
            return _oauth_error("invalid_grant", "unknown or expired authorization code")

        # PKCE. Without this, any local process that observed the loopback
        # redirect could redeem the code before the real client does.
        if not sealed.verify_pkce(str(verifier), issued.code_challenge):
            log.warning("PKCE verification failed for client_id=%s", issued.client_id)
            return _oauth_error("invalid_grant", "PKCE verification failed")

        # RFC 6749 §4.1.3: when a redirect_uri was used to obtain the code, the
        # token request must present the identical value.
        presented_redirect = form.get("redirect_uri")
        if presented_redirect is not None and str(presented_redirect) != issued.redirect_uri:
            return _oauth_error("invalid_grant", "redirect_uri does not match the authorization request")

        client_id = form.get("client_id")
        if client_id is not None and str(client_id) != issued.client_id:
            return _oauth_error("invalid_grant", "client_id does not match the authorization request")

        # The first exchange of the client's credential. Single-use rotation at
        # the control plane makes this the moment the code becomes unreplayable.
        try:
            first = await self._agent.exchange_refresh_token(
                credentials.decode_refresh_token(issued.refresh_token)
            )
        except credentials.CredentialError:
            # We sealed this blob ourselves; failing to decode it is a bug, but
            # one that must not crash the token endpoint.
            log.error("a sealed authorization code carried an undecodable refresh token")
            return _oauth_error("invalid_grant", "unknown or expired authorization code")
        except control_plane.ControlPlaneError as err:
            if err.is_unauthorized:
                # Consumed by an earlier redemption, revoked, or expired.
                return _oauth_error("invalid_grant", "authorization code was already redeemed or expired")
            log.error("control plane failed during code redemption: %s", err)
            return _oauth_error("server_error", "control plane is unavailable", status=502)

        return _token_response(
            first.access_token, _rotated_or_original(first, issued.refresh_token)
        )

    async def _grant_refresh_token(self, form: Any) -> Response:
        """Translate an OAuth refresh into Estuary's token exchange.

        This handler is the entire steady state of the design, and it is a pure
        translation: unwrap the blob, call the control plane, rewrap whatever it
        rotated to. The adapter learns nothing and keeps nothing.

        Note what is *absent*: any binding between the refresh token and the
        client that presented it. There is nowhere to record such a binding
        without durable state, and the credential is the control plane's own — so
        the control plane, which does have state, is where single-use rotation is
        enforced. A replayed refresh token fails there, not here.
        """
        blob = form.get("refresh_token")
        if not blob:
            return _oauth_error("invalid_request", "refresh_token is required")

        try:
            exchanged = await self._agent.exchange_refresh_token(
                credentials.decode_refresh_token(str(blob))
            )
        except credentials.CredentialError:
            return _oauth_error("invalid_grant", "refresh_token is malformed")
        except control_plane.ControlPlaneError as err:
            if err.is_unauthorized:
                # Expired, revoked, or already-spent (single-use tokens rotate).
                # `invalid_grant` is the signal that tells a client to discard the
                # credential and start a fresh authorization rather than retry.
                return _oauth_error("invalid_grant", "refresh_token is invalid or expired")
            log.error("control plane failed during refresh: %s", err)
            return _oauth_error("server_error", "control plane is unavailable", status=502)

        return _token_response(
            exchanged.access_token, _rotated_or_original(exchanged, str(blob))
        )


# ------------------------------------------------------------------ responses


def _rotated_or_original(exchanged: control_plane.ExchangedTokens, presented: str) -> str:
    """The refresh token to hand back after an exchange.

    Single-use tokens rotate and the control plane returns the successor;
    multi-use tokens do not, and the caller must keep the one it already has.
    Returning the presented blob in that case means the client's persistence
    logic is identical either way — which matters because flipping this design
    back to multi-use is meant to be a one-argument change at the mint site.
    """
    if exchanged.rotated_refresh_token is None:
        return presented
    return credentials.encode_refresh_token(exchanged.rotated_refresh_token)


def _token_response(access_token: str, refresh_token: str) -> Response:
    body: dict[str, Any] = {
        "access_token": access_token,
        "token_type": "Bearer",
        "refresh_token": refresh_token,
    }
    if (expires_in := credentials.access_token_expires_in(access_token)) is not None:
        body["expires_in"] = expires_in

    # RFC 6749 §5.1: token responses must not be cached anywhere.
    return JSONResponse(body, headers={"Cache-Control": "no-store", "Pragma": "no-cache"})


def _redirect_to_client(parked: sealed.PendingAuthorization, params: dict[str, str]) -> Response:
    """Redirect back to the client's `redirect_uri`, echoing its `state`.

    `state` is the client's CSRF defense: it must come back exactly as sent, on
    success and on failure alike, or the client will (correctly) drop the response.
    """
    if parked.client_state is not None:
        params = {**params, "state": parked.client_state}

    separator = "&" if urllib.parse.urlsplit(parked.redirect_uri).query else "?"
    return RedirectResponse(
        f"{parked.redirect_uri}{separator}{urllib.parse.urlencode(params)}", status_code=302
    )


def _oauth_error(error: str, description: str, status: int = 400) -> Response:
    return JSONResponse({"error": error, "error_description": description}, status_code=status)


def _cors(response: Response) -> Response:
    """Allow browser-side callers to read this response.

    The dashboard fetches `/oauth/consent-context` from its own origin, and MCP
    clients running in a browser read the metadata and token endpoints the same
    way. `*` is correct here and not a shortcut: every one of these endpoints is
    either public metadata or authenticated by a bearer credential in the request
    body, never by an ambient cookie — so there is no session for a hostile origin
    to ride. Credentialed CORS is deliberately not enabled.
    """
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response


def _cors_preflight() -> Response:
    return _cors(
        Response(
            status_code=204,
            headers={
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
                "Access-Control-Max-Age": "3600",
            },
        )
    )
