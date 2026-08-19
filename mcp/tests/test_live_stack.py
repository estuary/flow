"""The acceptance test, against a running local stack and the real internet.

Everything else in this directory runs against fakes. This module runs against
the actual control-plane agent, the actual adapter process, and the actual
client-metadata document published by `claude.ai` — which is the only way to
learn things like "Claude Code registers `http://localhost/callback` with no
port at all, so loopback port-agnosticism is load-bearing, not a nicety".

It stands in for the browser and the dashboard, and for the dashboard's mint it
substitutes the refresh token `mise run local:test-tenant` writes out. That
substitution is the one thing here that is not real; every other hop is.

Run it:

    mise run local:stack                       # includes the adapter
    mise run local:test-tenant --tenant acmeCo --user alice@example.com
    set -a; source ~/flow-local/<stack>/test-tenant-acmeCo.env; set +a
    ESTUARY_MCP_LIVE_ADAPTER=http://localhost:<base+22> \\
        uv run --directory mcp pytest tests/test_live_stack.py -v

Skipped entirely when those variables are absent, so it never breaks a plain
`pytest` run.
"""

import base64
import hashlib
import json
import os
import urllib.parse

import httpx
import pytest

ADAPTER = os.environ.get("ESTUARY_MCP_LIVE_ADAPTER")
# `mise run local:test-tenant` exports this: a 90-day multi-use refresh token for
# the provisioned user. Here it plays the part of the dashboard's five-minute
# handoff mint. Multi-use is fine — the adapter treats a missing rotation as
# "keep what you have", which is the branch this exercises for free.
HANDOFF = os.environ.get("FLOW_AUTH_TOKEN")

CLIENT_ID = "https://claude.ai/oauth/claude-code-client-metadata"
# An ephemeral loopback port, as a native client binds. Claude Code's published
# document registers `http://localhost/callback` with no port, so this only
# matches under RFC 8252 §7.3 port-agnostic comparison.
REDIRECT_URI = "http://localhost:54321/callback"

pytestmark = pytest.mark.skipif(
    not (ADAPTER and HANDOFF),
    reason="set ESTUARY_MCP_LIVE_ADAPTER and FLOW_AUTH_TOKEN to run against a live stack",
)


@pytest.fixture
def http():
    with httpx.Client(follow_redirects=False, timeout=30) as client:
        yield client


def _pkce() -> tuple[str, str]:
    verifier = base64.urlsafe_b64encode(b"live-verifier" * 4).rstrip(b"=").decode()
    challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    )
    return verifier, challenge


def _query_of(response: httpx.Response) -> dict[str, str]:
    return dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(response.headers["location"]).query))


def _authorize(http: httpx.Client) -> tuple[str, str]:
    """Run `/authorize` against the real claude.ai metadata document."""
    verifier, challenge = _pkce()
    response = http.get(
        f"{ADAPTER}/oauth/authorize",
        params={
            "response_type": "code",
            "client_id": CLIENT_ID,
            "redirect_uri": REDIRECT_URI,
            "state": "live-state",
            "code_challenge": challenge,
            "code_challenge_method": "S256",
            "resource": f"{ADAPTER}/mcp",
        },
    )
    assert response.status_code == 302, response.text
    return verifier, _query_of(response)["state"]


def _complete(http: httpx.Client) -> dict:
    verifier, state = _authorize(http)

    dashboard = http.get(
        f"{ADAPTER}/oauth/dashboard-callback", params={"state": state, "handoff": HANDOFF}
    )
    assert dashboard.status_code == 302, dashboard.text

    returned = _query_of(dashboard)
    assert dashboard.headers["location"].startswith(REDIRECT_URI)

    token = http.post(
        f"{ADAPTER}/oauth/token",
        data={
            "grant_type": "authorization_code",
            "code": returned["code"],
            "code_verifier": verifier,
            "redirect_uri": REDIRECT_URI,
            "client_id": CLIENT_ID,
        },
    )
    assert token.status_code == 200, token.text
    return token.json()


def _call_prefixes(http: httpx.Client, access_token: str) -> dict:
    response = http.post(
        f"{ADAPTER}/mcp",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": "2025-06-18",
        },
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "prefixes", "arguments": {}},
        },
    )
    assert response.status_code == 200, response.text
    result = response.json()["result"]
    assert result["isError"] is False, result
    return json.loads(result["content"][0]["text"])


def test_discovery_chain(http):
    """A cold client's path: 401 → resource metadata → AS metadata → CIMD."""
    unauthenticated = http.post(
        f"{ADAPTER}/mcp",
        headers={"Accept": "application/json, text/event-stream"},
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
    )
    assert unauthenticated.status_code == 401
    assert "resource_metadata=" in unauthenticated.headers["www-authenticate"]

    resource = http.get(f"{ADAPTER}/.well-known/oauth-protected-resource/mcp").json()
    assert resource["authorization_servers"] == [ADAPTER]

    metadata = http.get(f"{ADAPTER}/.well-known/oauth-authorization-server").json()
    assert metadata["client_id_metadata_document_supported"] is True
    assert metadata["token_endpoint_auth_methods_supported"] == ["none"]


def test_authorize_reads_the_real_claude_metadata_document(http):
    """The consent screen's strings come from claude.ai, fetched live."""
    _verifier, state = _authorize(http)

    context = http.get(f"{ADAPTER}/oauth/consent-context", params={"state": state}).json()
    assert context["client_name"] == "Claude Code"
    # Not what the document claims to be called — where it was actually served from.
    assert context["client_host"] == "claude.ai"


def test_full_dance_then_call_the_tool(http):
    """The acceptance test proper: authorization all the way through to data."""
    tokens = _complete(http)
    assert tokens["token_type"] == "Bearer"
    assert tokens["expires_in"] > 3000  # the control plane's fixed one-hour token

    payload = _call_prefixes(http, tokens["access_token"])
    prefixes = [entry["prefix"] for entry in payload["prefixes"]]
    assert prefixes, payload
    # Every prefix here came out of the grant graph for the user behind the
    # handoff credential — proof the caller's own bearer reached the agent.
    assert any(entry["userCapability"] == "admin" for entry in payload["prefixes"]), payload


def test_refresh_rotates_against_the_real_control_plane(http):
    tokens = _complete(http)

    refreshed = http.post(
        f"{ADAPTER}/oauth/token",
        data={"grant_type": "refresh_token", "refresh_token": tokens["refresh_token"]},
    )
    assert refreshed.status_code == 200, refreshed.text
    rotated = refreshed.json()
    assert rotated["refresh_token"] != tokens["refresh_token"]

    spent = http.post(
        f"{ADAPTER}/oauth/token",
        data={"grant_type": "refresh_token", "refresh_token": tokens["refresh_token"]},
    )
    assert spent.status_code == 400
    assert spent.json()["error"] == "invalid_grant"

    # The rotated credential works, and so does the token it yields.
    again = http.post(
        f"{ADAPTER}/oauth/token",
        data={"grant_type": "refresh_token", "refresh_token": rotated["refresh_token"]},
    )
    assert again.status_code == 200, again.text
    assert _call_prefixes(http, again.json()["access_token"])["prefixes"]
