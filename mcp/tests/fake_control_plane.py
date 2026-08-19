"""A stand-in for the control-plane agent, faithful where the adapter depends on it.

The adapter's contract with the control plane is small but has sharp edges, and
this fake reproduces exactly those:

  * `POST /api/v1/auth/token` rotates single-use refresh tokens and *omits*
    `refresh_token` for multi-use ones — the branch `_rotated_or_original` exists
    for.
  * `createRefreshToken` returns `{id, secret}`, and the mint is authenticated:
    passing a spent or unknown credential fails, which is what makes the
    single-use handoff replay-proof.
  * Access tokens are structurally real JWTs (garbage signature, since nothing
    in the adapter verifies one) so `expires_in` and the SDK's expiry check
    behave as they do in production.
  * `/api/graphql` can be told to answer the next request with the agent's
    snapshot-staleness `307`, so the retry absorption is exercised rather than
    assumed.
"""

import base64
import dataclasses
import email.utils
import itertools
import json
import time
from typing import Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

ACCESS_TOKEN_TTL_SECONDS = 3600


@dataclasses.dataclass
class StoredToken:
    secret: str
    user_id: str
    multi_use: bool
    expires_at: float
    detail: str | None = None


class FakeControlPlane:
    """In-memory control plane. One instance per test; assert against its state."""

    def __init__(self, user_id: str = "11111111-1111-1111-1111-111111111111"):
        self.user_id = user_id
        self.tokens: dict[str, StoredToken] = {}
        self.prefixes = [
            {"prefix": "acmeCo/", "userCapability": "admin", "capabilities": ["CatalogRead"]},
            {"prefix": "acmeCo/marketing/", "userCapability": "write", "capabilities": ["CatalogRead"]},
        ]
        # Set to a positive count to make that many upcoming GraphQL requests
        # answer with the snapshot-staleness 307 before succeeding.
        self.stale_graphql_responses = 0
        # Set to a message to make every GraphQL operation fail the way GraphQL
        # fails: HTTP 200 with an `errors` array. Authorization denials arrive
        # this way in production, so the adapter must treat it as failure.
        self.fail_graphql_with_errors: str | None = None
        self.graphql_requests: list[dict[str, Any]] = []
        self._ids = itertools.count(1)

    # ----------------------------------------------------------------- minting

    def mint_refresh_token(
        self, *, multi_use: bool = False, ttl_seconds: float = 300, detail: str | None = None
    ) -> tuple[str, str]:
        token_id = f"{next(self._ids):016x}"
        secret = f"secret-{token_id}"
        self.tokens[token_id] = StoredToken(
            secret=secret,
            user_id=self.user_id,
            multi_use=multi_use,
            expires_at=time.time() + ttl_seconds,
            detail=detail,
        )
        return token_id, secret

    def refresh_token_blob(self, **kwargs) -> str:
        """A credential in the wire format the dashboard hands to the adapter."""
        token_id, secret = self.mint_refresh_token(**kwargs)
        return base64.b64encode(json.dumps({"id": token_id, "secret": secret}).encode()).decode()

    def _access_token(self) -> str:
        return _jwt(
            {
                "sub": self.user_id,
                "aud": "authenticated",
                "role": "authenticated",
                "iat": int(time.time()),
                "exp": int(time.time()) + ACCESS_TOKEN_TTL_SECONDS,
            }
        )

    # ------------------------------------------------------------------ routes

    def app(self) -> Starlette:
        return Starlette(
            routes=[
                Route("/api/v1/auth/token", self._token_exchange, methods=["POST"]),
                Route("/api/graphql", self._graphql, methods=["POST"]),
            ]
        )

    async def _token_exchange(self, request: Request) -> Response:
        body = await request.json()
        stored = self.tokens.get(body.get("refresh_token_id", ""))

        if (
            stored is None
            or stored.secret != body.get("secret")
            or stored.expires_at < time.time()
        ):
            # The real endpoint collapses every credential failure into one
            # opaque 401, so callers cannot probe which check failed.
            return JSONResponse(
                {"error": "invalid, expired, or unknown credential"}, status_code=401
            )

        response: dict[str, Any] = {"access_token": self._access_token()}
        if not stored.multi_use:
            # Rotation: same id, fresh secret. The old secret is dead the instant
            # this returns — replacing it in place is the entire single-use model,
            # and is what makes a replayed handoff or authorization code fail here.
            stored.secret = f"secret-{body['refresh_token_id']}-{time.time_ns()}"
            response["refresh_token"] = {
                "id": body["refresh_token_id"],
                "secret": stored.secret,
            }
        return JSONResponse(response)

    async def _graphql(self, request: Request) -> Response:
        if self.stale_graphql_responses > 0:
            self.stale_graphql_responses -= 1
            return _snapshot_stale_redirect(request)

        bearer = (request.headers.get("authorization") or "").removeprefix("Bearer ")
        if not _access_token_is_live(bearer):
            return JSONResponse({"error": "unauthenticated"}, status_code=401)

        body = await request.json()
        self.graphql_requests.append(body)
        query, variables = body.get("query", ""), body.get("variables") or {}

        if self.fail_graphql_with_errors is not None:
            return JSONResponse({"errors": [{"message": self.fail_graphql_with_errors}]})

        if "createRefreshToken" in query:
            token_id, secret = self.mint_refresh_token(
                multi_use=False,
                ttl_seconds=90 * 24 * 3600,
                detail=variables.get("detail"),
            )
            return JSONResponse({"data": {"createRefreshToken": {"id": token_id, "secret": secret}}})

        if "prefixes" in query:
            return JSONResponse(
                {
                    "data": {
                        "prefixes": {
                            "edges": [{"node": node} for node in self.prefixes],
                            "pageInfo": {"hasNextPage": False},
                        }
                    }
                }
            )

        return JSONResponse({"errors": [{"message": f"unexpected query: {query[:60]}"}]})


def _snapshot_stale_redirect(request: Request) -> Response:
    """The agent's 307 for "my authorization snapshot is older than your request".

    Mirrors `crates/control-plane-api/src/server/error.rs`: a relative Location
    carrying `started`, plus an absolute-date `Retry-After`. The matching `Date`
    is uvicorn's own — setting a second one here would produce a header shape the
    real agent never emits.
    """
    return Response(
        status_code=307,
        headers={
            "location": f"{request.url.path}?started=1970-01-01T00:00:00.000Z",
            "retry-after": email.utils.formatdate(time.time() + 1, usegmt=True),
        },
    )


def _access_token_is_live(token: str) -> bool:
    claims = _claims(token)
    return bool(claims) and claims.get("exp", 0) > time.time()


def _claims(token: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) != 3:
        return {}
    payload = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        return json.loads(base64.urlsafe_b64decode(payload))
    except ValueError:
        return {}


def _jwt(claims: dict[str, Any]) -> str:
    encode = lambda part: base64.urlsafe_b64encode(json.dumps(part).encode()).rstrip(b"=").decode()
    return f"{encode({'alg': 'HS256', 'typ': 'JWT'})}.{encode(claims)}.bm90LWEtc2lnbmF0dXJl"
