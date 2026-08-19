"""The adapter's only outbound IO: HTTP calls to the control-plane agent.

Everything the adapter can do, it does by asking the control plane on the
caller's behalf. It holds no keys, no client secrets, and no user credential
store; the agent performs the single authoritative verification of every token
(`crates/control-plane-api/src/envelope.rs`).

Two behaviours here are worth understanding before changing anything:

Snapshot staleness. The agent authorizes from an in-memory snapshot of the
grant graph that it refreshes on an interval. When a request's authorization
fails against a snapshot older than the request, the agent does not answer
"denied" — it answers `307 Temporary Redirect` with a `Location` carrying
`started`/`retryAfter` and (usually) a `Retry-After` header, meaning "ask me
again once I've refreshed". That protocol is between the agent and its clients;
MCP has no vocabulary for it. `request_with_retry` below absorbs it entirely, so
a 307 never reaches an MCP client. (See `crates/control-plane-api/src/server/error.rs`
for the response shape, and `mise/tasks/local/test-tenant` for the same dance in
shell.)

Bearer pass-through. Tool calls forward the caller's access token verbatim. We
never send a *refresh*-token blob as a bearer credential: the agent's envelope
would accept it, but at the cost of a bcrypt verify and a DB write per request.
Refresh tokens go to the token-exchange endpoint, once, and the resulting access
token is what travels.
"""

import asyncio
import dataclasses
import datetime
import email.utils
import logging
import time
import urllib.parse
from typing import Any

import httpx

from . import credentials

log = logging.getLogger(__name__)

# Bound on the snapshot-staleness dance. The agent blocks server-side once a
# client demonstrably ignored a Retry-After, so in practice one or two hops
# settle it; these bounds exist so a pathological agent cannot hang a tool call.
MAX_RETRY_HOPS = 5
MAX_RETRY_SECONDS = 60.0


class ControlPlaneError(Exception):
    """A control-plane call failed. `status` is the HTTP status when the failure
    came from the agent, and `None` when the call never completed."""

    def __init__(self, message: str, status: int | None = None, body: str | None = None):
        super().__init__(message)
        self.status = status
        self.body = body

    @property
    def is_unauthorized(self) -> bool:
        """401/403 from the control plane. The AS facade and the MCP resource
        both need to translate these into a *client-actionable* signal —
        an OAuth `invalid_grant`, or an MCP 401 with `WWW-Authenticate` — rather
        than a generic 500 the client will retry forever."""
        return self.status in (401, 403)


@dataclasses.dataclass(frozen=True)
class ExchangedTokens:
    """Result of redeeming a refresh token at `POST /api/v1/auth/token`."""

    access_token: str
    # Present only for single-use tokens, which rotate. Multi-use tokens omit it
    # (`crates/control-plane-api/src/server/public/token_exchange.rs`), and the
    # caller must then keep presenting the credential it already has.
    rotated_refresh_token: credentials.RefreshToken | None


class ControlPlane:
    """A thin, stateless client for one agent base URL.

    Holds an `httpx.AsyncClient` for connection reuse and nothing else. There is
    deliberately no cache of tokens, users, or authorizations: caching any of
    those would make the adapter a trust-bearing component.
    """

    def __init__(self, agent_url: str, client: httpx.AsyncClient | None = None):
        self._agent_url = agent_url.rstrip("/")
        # `follow_redirects=False`: the 307s we get back are the staleness
        # protocol, and honouring their Retry-After is the whole point. httpx's
        # own redirect following would hot-loop them.
        self._client = client or httpx.AsyncClient(timeout=30.0, follow_redirects=False)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def exchange_refresh_token(self, token: credentials.RefreshToken) -> ExchangedTokens:
        """Redeem a refresh token for an access token, rotating it if single-use.

        This is the one control-plane call the adapter makes on its own behalf
        rather than a caller's: it is how the browser handoff becomes a bearer
        token, and how the OAuth `/token` refresh grant is served.
        """
        response = await self.request_with_retry(
            "POST",
            "/api/v1/auth/token",
            json={
                "grant_type": "refresh_token",
                "refresh_token_id": token.id,
                "secret": token.secret,
            },
        )
        if response.status_code != 200:
            raise ControlPlaneError(
                "refresh-token exchange rejected by the control plane",
                status=response.status_code,
                body=response.text[:512],
            )

        body = response.json()
        access_token = body.get("access_token")
        if not isinstance(access_token, str):
            raise ControlPlaneError("token exchange response has no access_token")

        rotated = body.get("refresh_token")
        rotated_token = None
        if isinstance(rotated, dict):
            rotated_token = credentials.RefreshToken(id=rotated["id"], secret=rotated["secret"])

        return ExchangedTokens(access_token=access_token, rotated_refresh_token=rotated_token)

    async def graphql(
        self,
        bearer: str,
        query: str,
        variables: dict[str, Any] | None = None,
        operation_name: str | None = None,
    ) -> dict[str, Any]:
        """Run a GraphQL operation as `bearer`.

        The token is forwarded verbatim and verified by the agent. A rejected
        token surfaces as `ControlPlaneError.is_unauthorized`, which callers
        translate into whatever their protocol's "re-authenticate" signal is.
        """
        payload: dict[str, Any] = {"query": query}
        if variables is not None:
            payload["variables"] = variables
        if operation_name is not None:
            payload["operationName"] = operation_name

        response = await self.request_with_retry(
            "POST",
            "/api/graphql",
            json=payload,
            headers={"Authorization": f"Bearer {bearer}"},
        )
        if response.status_code != 200:
            raise ControlPlaneError(
                "GraphQL request rejected by the control plane",
                status=response.status_code,
                body=response.text[:512],
            )

        body = response.json()
        # GraphQL reports operation-level failures inside a 200. Authorization
        # denials arrive this way too, so this is not merely cosmetic.
        if body.get("errors"):
            messages = "; ".join(
                str(err.get("message", err)) for err in body["errors"] if isinstance(err, dict)
            )
            raise ControlPlaneError(f"GraphQL errors: {messages}", status=200)

        data = body.get("data")
        if not isinstance(data, dict):
            raise ControlPlaneError("GraphQL response carried no data")
        return data

    async def request_with_retry(
        self,
        method: str,
        path: str,
        *,
        json: Any | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Issue a request, absorbing the agent's snapshot-staleness 307s.

        Each 307 carries a relative `Location` with the `started` timestamp the
        agent wants preserved across the retry, plus a `Retry-After`. We wait out
        the `Retry-After` and re-issue the *same* method and body against the new
        URL — a 307, unlike a 302, forbids downgrading to GET.
        """
        url = f"{self._agent_url}{path}"
        deadline = time.monotonic() + MAX_RETRY_SECONDS

        for hop in range(MAX_RETRY_HOPS):
            response = await self._client.request(method, url, json=json, headers=headers)
            if response.status_code != 307:
                return response

            location = response.headers.get("location")
            if not location:
                raise ControlPlaneError("control plane returned 307 without a Location")

            url = urllib.parse.urljoin(url, location)
            delay = _retry_delay(response, deadline)
            log.info(
                "control plane authorization snapshot is stale; retrying in %.1fs (hop %d)",
                delay,
                hop + 1,
            )
            if delay > 0:
                await asyncio.sleep(delay)

        raise ControlPlaneError(
            f"control plane did not settle authorization within {MAX_RETRY_HOPS} retries"
        )


def _as_utc(when: datetime.datetime) -> datetime.datetime:
    """RFC 2822 dates may or may not carry a zone: `-0000` parses naive, `GMT`
    parses aware. Both mean UTC here, and subtracting one from the other raises."""
    return when if when.tzinfo is not None else when.replace(tzinfo=datetime.timezone.utc)


def _retry_delay(response: httpx.Response, deadline: float) -> float:
    """Seconds to wait before replaying a 307, clamped to the overall deadline.

    `Retry-After` is an absolute RFC 2822 date here, so it is compared against
    the agent's own `Date` header rather than our clock: the two machines need
    not agree, and a skewed local clock would otherwise turn a 3-second wait
    into a minute (or into none at all).
    """
    retry_after = response.headers.get("retry-after")
    server_now = response.headers.get("date")
    if not retry_after or not server_now:
        # No Retry-After means the agent chose to block server-side instead;
        # replaying immediately is what it is asking for.
        return 0.0

    try:
        retry_at = _as_utc(email.utils.parsedate_to_datetime(retry_after))
        reference = _as_utc(email.utils.parsedate_to_datetime(server_now))
    except (TypeError, ValueError):
        log.warning("unparseable Retry-After/Date on a 307; replaying immediately")
        return 0.0

    delay = (retry_at - reference).total_seconds()
    remaining = deadline - time.monotonic()
    return max(0.0, min(delay, remaining))
