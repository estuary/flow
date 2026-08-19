"""Test harness: real HTTP servers on ephemeral ports.

The integration tests deliberately do *not* use an in-process ASGI transport.
The adapter fetches CIMD documents over the network with its own HTTP client, and
the OAuth dance is a chain of real redirects between three origins; a transport
shim would quietly stub out exactly the parts under test. Running everything on
loopback ports costs a few milliseconds and keeps the wire real.
"""

import asyncio
import contextlib
import json
import socket

import pytest
import uvicorn
from starlette.applications import Starlette
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from estuary_mcp import app as app_module
from estuary_mcp import config

from . import fake_control_plane


def free_port() -> int:
    """Reserve a loopback port by binding and releasing it.

    The adapter has to be told its own public URL *before* it binds, because that
    URL is baked into the OAuth metadata it serves. So the port is chosen first
    and handed to both. (Production never faces this: the port is configuration.)
    """
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@contextlib.asynccontextmanager
async def serve(app, port: int = 0):
    """Run `app` on a loopback port for the duration of the context."""
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="on")
    )
    task = asyncio.create_task(server.serve())
    try:
        while not server.started:
            if task.done():  # Surface a startup failure instead of hanging.
                await task
            await asyncio.sleep(0.01)
        bound = server.servers[0].sockets[0].getsockname()[1]
        yield f"http://127.0.0.1:{bound}"
    finally:
        server.should_exit = True
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(task, timeout=5)


class ClientMetadataHost:
    """Serves a client-metadata document, standing in for `claude.ai`.

    `path` is non-trivial because CIMD requires the client_id URL to have one.
    Tests reach this only with `allow_insecure_cimd` enabled: the document is
    served over http on loopback, which the draft's rules forbid and the adapter
    enforces except under that explicit test-only flag.
    """

    PATH = "/oauth/test-client-metadata"

    def __init__(self, client_name: str = "Test MCP Client"):
        self.client_name = client_name
        self.base_url = ""
        self.status_code = 200
        self.extra_fields: dict = {}
        self.request_count = 0
        # Set to a byte count to serve that much padding instead of a document,
        # standing in for a hostile or broken metadata host.
        self.oversized_bytes = 0

    @property
    def client_id(self) -> str:
        return f"{self.base_url}{self.PATH}"

    def app(self) -> Starlette:
        async def document(request) -> Response:
            self.request_count += 1
            if self.status_code != 200:
                return Response(status_code=self.status_code)
            if self.oversized_bytes:
                return Response(b"x" * self.oversized_bytes, media_type="application/json")
            return JSONResponse(
                {
                    "client_id": self.client_id,
                    "client_name": self.client_name,
                    # A loopback callback with a placeholder port: the client
                    # will present a different, ephemeral one (RFC 8252 §7.3).
                    "redirect_uris": ["http://localhost:1/callback"],
                    "token_endpoint_auth_method": "none",
                    "grant_types": ["authorization_code", "refresh_token"],
                    "response_types": ["code"],
                    **self.extra_fields,
                }
            )

        return Starlette(routes=[Route(self.PATH, document, methods=["GET"])])


class LoopbackClient:
    """The MCP client's redirect target: records what the adapter sends back."""

    PATH = "/callback"

    def __init__(self):
        self.base_url = ""
        self.received: dict[str, str] = {}

    @property
    def redirect_uri(self) -> str:
        # Spelled `localhost`, as Claude Code and Codex spell theirs, while the
        # socket itself is bound to 127.0.0.1 — the mismatch real clients create.
        port = self.base_url.rsplit(":", 1)[1]
        return f"http://localhost:{port}{self.PATH}"

    def app(self) -> Starlette:
        async def callback(request) -> Response:
            self.received = dict(request.query_params)
            return JSONResponse({"ok": True})

        return Starlette(routes=[Route(self.PATH, callback, methods=["GET"])])


class Harness:
    """The three origins the dance spans, plus the fake control plane behind it."""

    def __init__(
        self,
        adapter_url: str,
        settings: config.Settings,
        agent: fake_control_plane.FakeControlPlane,
        metadata_host: ClientMetadataHost,
        loopback: LoopbackClient,
    ):
        self.adapter_url = adapter_url
        self.settings = settings
        self.agent = agent
        self.metadata_host = metadata_host
        self.loopback = loopback

    @property
    def dashboard_url(self) -> str:
        """Where the adapter sends browsers for login and consent.

        No dashboard runs in these tests: the tests *are* the dashboard, reading
        the consent context and redirecting back with a handoff credential, which
        is precisely the contract the paired UI change implements.
        """
        return self.settings.dashboard_url


@pytest.fixture
async def harness():
    agent = fake_control_plane.FakeControlPlane()
    metadata_host = ClientMetadataHost()
    loopback = LoopbackClient()

    async with serve(agent.app()) as agent_url:
        async with serve(metadata_host.app()) as metadata_url:
            async with serve(loopback.app()) as loopback_url:
                metadata_host.base_url = metadata_url
                loopback.base_url = loopback_url

                # Built directly rather than through `config.from_env` so a test
                # never depends on ambient environment. The one relaxation is
                # `allow_insecure_cimd`, without which a loopback client_id
                # cannot exist at all.
                port = free_port()
                settings = config.Settings(
                    public_url=f"http://127.0.0.1:{port}",
                    agent_url=agent_url,
                    dashboard_url="http://dashboard.test",
                    bind_host="127.0.0.1",
                    bind_port=0,
                    client_token_validity="P90D",
                    sealing_keys=(b"\x01" * 32,),
                    allow_insecure_cimd=True,
                    log_level="WARNING",
                )

                async with serve(app_module.build_app(settings), port=port) as adapter_url:
                    assert adapter_url == settings.public_url
                    yield Harness(adapter_url, settings, agent, metadata_host, loopback)


def json_body(response) -> dict:
    return json.loads(response.content)
