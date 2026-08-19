"""Composition root: one ASGI app carrying two independent halves.

The halves are:

  * `auth` — the OAuth authorization-server facade (`/oauth/*`, RFC 8414
    metadata). Hand-written, because it is the part of this prototype worth
    studying.
  * `server` — the MCP resource itself (`/mcp`, RFC 9728 metadata), built on the
    official SDK.

They share a process today and nothing else: the auth half reaches the control
plane and the MCP half reaches the control plane, but neither reaches the other
except through `Settings`. Splitting them into separate deployables should be a
routing change plus pointing `issuer_url` at the other origin — if a change ever
makes that untrue, it is going the wrong way.
"""

import contextlib
import urllib.parse
from typing import Any

import httpx
from mcp.server import MCPServer
from mcp.server.auth.settings import AuthSettings
from starlette.applications import Starlette

from . import config, control_plane
from .auth import routes as auth_routes
from .auth import sealed
from .server import tools, verifier


def build_app(settings: config.Settings) -> Starlette:
    """Assemble the ASGI application for `settings`."""
    agent = control_plane.ControlPlane(settings.agent_url)
    # A separate client for CIMD fetches: those go to arbitrary third-party hosts
    # chosen by whoever starts an authorization request, so they must not share a
    # connection pool, cookie jar, or default headers with control-plane traffic.
    cimd_http = httpx.AsyncClient(timeout=cimd_timeout(), follow_redirects=False)

    mcp_server = MCPServer(
        name="estuary",
        title="Estuary Flow",
        instructions=(
            "Tools for inspecting an Estuary Flow catalog on behalf of the "
            "authenticated user. Estuary organizes captures, collections, "
            "derivations, and materializations under `/`-delimited catalog "
            "prefixes, which are also the unit of authorization."
        ),
        token_verifier=verifier.PassThroughTokenVerifier(),
        auth=AuthSettings(
            # This process is its own authorization server, so the issuer is our
            # own origin. Clients compare it by exact string against the `issuer`
            # in the RFC 8414 document, which `config.Settings` normalizes.
            issuer_url=settings.issuer,  # type: ignore[arg-type]
            # RFC 9728: the resource identifier is the MCP endpoint URL. The SDK
            # derives `/.well-known/oauth-protected-resource/mcp` from it and
            # points the 401's `WWW-Authenticate` at that — which is the thread a
            # cold client pulls to discover everything else.
            resource_server_url=settings.resource_url,  # type: ignore[arg-type]
            # No scope vocabulary in the PoC; Estuary authorization is the
            # server-side grant graph. See README.
            required_scopes=None,
        ),
    )

    @mcp_server.tool(
        name="prefixes",
        title="List accessible catalog prefixes",
        description=(
            "List the Estuary catalog prefixes the authenticated user can access, "
            "with the capabilities they hold at each. Catalog prefixes are "
            "`/`-delimited namespaces such as `acmeCo/` or `acmeCo/marketing/`."
        ),
    )
    async def prefixes() -> dict[str, Any]:
        return await tools.prefixes(agent)

    facade = auth_routes.AuthServer(
        settings,
        agent,
        cimd_http,
        # The sealer's AAD binds blobs to the issuer, so two deployments that
        # somehow share a key still cannot open each other's state.
        sealed.Sealer(settings.sealing_keys, issuer=settings.issuer),
    )
    for route in facade.routes():
        # `custom_route` is the SDK's supported way to add unauthenticated
        # routes, which is exactly what authorization endpoints must be: a client
        # visits them precisely because it has no credential yet.
        mcp_server.custom_route(route.path, methods=sorted(route.methods or {"GET"}))(
            route.endpoint
        )

    app = mcp_server.streamable_http_app(
        # Stateless is what the 2026-07-28 spec is built around, and it is what
        # lets this run as more than one replica later without a session store.
        stateless_http=True,
        json_response=True,
        # The SDK auto-enables DNS-rebinding protection when it believes it is
        # bound to loopback. Behind a port-forward or a tunnel the Host header is
        # legitimately something else, so we pass the *public* host and let it
        # decide from the deployment's own view of itself.
        host=_public_host(settings),
    )
    _chain_shutdown(app, agent, cimd_http)
    return app


def cimd_timeout() -> httpx.Timeout:
    """Tight timeouts for fetches to third-party hosts.

    A slow client-metadata host must not be able to pin an adapter worker: the
    authorization request that triggered the fetch is already holding a browser
    redirect open.
    """
    return httpx.Timeout(connect=3.0, read=5.0, write=5.0, pool=5.0)


def _public_host(settings: config.Settings) -> str:
    return urllib.parse.urlsplit(settings.public_url).hostname or settings.bind_host


def _chain_shutdown(app: Starlette, *closeables: Any) -> None:
    """Close our HTTP clients when the ASGI app shuts down.

    The SDK sets the app's lifespan to the streamable-HTTP session manager and
    offers no hook to extend it, so we wrap the context it installed rather than
    replace it. Doing this after `streamable_http_app()` returns keeps the SDK's
    own startup ordering intact.
    """
    inner = app.router.lifespan_context

    @contextlib.asynccontextmanager
    async def lifespan(scope: Starlette):
        async with inner(scope):
            try:
                yield
            finally:
                for closeable in closeables:
                    await closeable.aclose()

    app.router.lifespan_context = lifespan
