"""The pass-through token verifier.

The MCP SDK expects a `TokenVerifier` that turns a bearer string into an
`AccessToken` (or `None`, which becomes an HTTP 401 with `WWW-Authenticate`).
This one performs **no cryptographic verification at all**, and that is the
design, not a shortcut — see the README's "Trust model" and "Spec conformance".
The adapter holds no keys; the control plane verifies every token authoritatively
on the call the tool was going to make anyway.

What is left for this class to do is nonetheless load-bearing:

  * **Deliver the 401 that drives token refresh.** MCP's re-authentication signal
    is an HTTP 401 on `/mcp`, and a tool handler cannot produce one — by the time
    a handler runs, the response is a JSON-RPC result. So the routine cause of a
    stale credential, expiry, has to be caught *here*. `expires_at` comes from
    the token's own unverified `exp` claim; a token forged to claim a later
    expiry gains nothing, because the control plane still rejects it a moment
    later. This is a liveness mechanism, not an access decision.

  * **Refuse a bearer that is not a JWT.** The control plane's envelope will
    happily accept a refresh-token blob as a bearer credential, at the cost of a
    bcrypt verify and a database write on every request. Rejecting non-JWT
    bearers here keeps a misconfigured client from turning each tool call into
    that.
"""

import logging

from mcp.server.auth.provider import AccessToken

from .. import credentials

log = logging.getLogger(__name__)


class PassThroughTokenVerifier:
    """Wraps a bearer token for the SDK without verifying it.

    Implements the SDK's `TokenVerifier` protocol structurally; there is no base
    class to inherit.
    """

    async def verify_token(self, token: str) -> AccessToken | None:
        expires_at = credentials.access_token_expiry(token)
        subject = credentials.access_token_subject(token)

        if subject is None and expires_at is None:
            # Neither claim decoded: this is not a control-plane access token.
            # Most likely a refresh-token blob presented as a bearer.
            log.debug("rejecting a bearer credential that is not a JWT")
            return None

        # `client_id` is what the SDK uses to bind a streamable-HTTP session to a
        # principal. Estuary access tokens carry no client identity — deliberately,
        # since a client's identity lives in the CIMD document, not in the token —
        # so the user is the finest principal available, and binding sessions per
        # user is the property that actually matters.
        return AccessToken(
            token=token,
            client_id=subject or "estuary-user",
            subject=subject,
            scopes=[],
            # The SDK's bearer backend refuses a token whose `expires_at` has
            # passed, which is what turns the routine case — an hour-old token —
            # into the HTTP 401 that prompts the client to refresh.
            expires_at=expires_at,
        )
