"""The authorization dance, end to end and headless.

These tests walk the exact sequence a real MCP client walks — discovery, CIMD,
consent handoff, code redemption, tool call, refresh rotation — standing in for
the browser and the dashboard, which are the only two participants that need a
human. Together they are the automated form of the acceptance test in the README.
"""

import base64
import hashlib
import json
import urllib.parse

import httpx
import pytest

# ------------------------------------------------------------------- utilities


def pkce_pair() -> tuple[str, str]:
    verifier = base64.urlsafe_b64encode(b"v" * 48).rstrip(b"=").decode()
    challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    )
    return verifier, challenge


async def start_authorization(client: httpx.AsyncClient, harness, **overrides) -> httpx.Response:
    """Issue the `/oauth/authorize` request a client would, and stop at the redirect."""
    verifier, challenge = pkce_pair()
    params = {
        "response_type": "code",
        "client_id": harness.metadata_host.client_id,
        "redirect_uri": harness.loopback.redirect_uri,
        "state": "client-state-abc",
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "resource": harness.settings.resource_url,
    }
    params.update(overrides)
    params = {key: value for key, value in params.items() if value is not None}

    response = await client.get(f"{harness.adapter_url}/oauth/authorize", params=params)
    response.pkce_verifier = verifier  # type: ignore[attr-defined]
    return response


def handoff_state(redirect_response: httpx.Response) -> str:
    """The handoff state the adapter parked its request under."""
    location = urllib.parse.urlsplit(redirect_response.headers["location"])
    return urllib.parse.parse_qs(location.query)["state"][0]


async def act_as_dashboard(client: httpx.AsyncClient, harness, state: str) -> httpx.Response:
    """Do what the paired UI change does: consent, mint a handoff, redirect back.

    The handoff credential is a single-use, five-minute refresh token minted as
    the logged-in user — the dashboard's whole contribution to the dance.
    """
    handoff = harness.agent.refresh_token_blob(multi_use=False, ttl_seconds=300)
    return await client.get(
        f"{harness.adapter_url}/oauth/dashboard-callback",
        params={"state": state, "handoff": handoff},
    )


async def complete_authorization(client: httpx.AsyncClient, harness) -> dict:
    """Run the whole dance and return the adapter's OAuth token response."""
    authorize = await start_authorization(client, harness)
    assert authorize.status_code == 302

    dashboard = await act_as_dashboard(client, harness, handoff_state(authorize))
    assert dashboard.status_code == 302

    returned = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(dashboard.headers["location"]).query))
    assert "code" in returned, returned

    token = await client.post(
        f"{harness.adapter_url}/oauth/token",
        data={
            "grant_type": "authorization_code",
            "code": returned["code"],
            "code_verifier": authorize.pkce_verifier,  # type: ignore[attr-defined]
            "redirect_uri": harness.loopback.redirect_uri,
            "client_id": harness.metadata_host.client_id,
        },
    )
    assert token.status_code == 200, token.text
    return token.json()


async def call_tool(client: httpx.AsyncClient, harness, access_token: str, name: str) -> httpx.Response:
    """Invoke an MCP tool over stateless streamable HTTP."""
    return await client.post(
        f"{harness.adapter_url}/mcp",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
            "MCP-Protocol-Version": "2025-06-18",
        },
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": name, "arguments": {}},
        },
    )


@pytest.fixture
async def client():
    # Redirects are followed manually throughout: each hop of the dance is a
    # separate assertion, and following them silently would hide the very
    # handoffs under test.
    async with httpx.AsyncClient(follow_redirects=False, timeout=10) as client:
        yield client


# ------------------------------------------------------------------- discovery


async def test_unauthenticated_mcp_request_points_at_resource_metadata(client, harness):
    """The thread a cold client pulls: 401 → resource metadata → AS → CIMD."""
    response = await client.post(
        f"{harness.adapter_url}/mcp",
        headers={"Accept": "application/json, text/event-stream"},
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
    )
    assert response.status_code == 401

    challenge = response.headers["www-authenticate"]
    assert challenge.startswith("Bearer ")
    assert f'resource_metadata="{harness.adapter_url}/.well-known/oauth-protected-resource/mcp"' in challenge


async def test_protected_resource_metadata_names_this_adapter_as_its_own_as(client, harness):
    response = await client.get(f"{harness.adapter_url}/.well-known/oauth-protected-resource/mcp")
    assert response.status_code == 200

    body = response.json()
    assert body["resource"] == harness.settings.resource_url
    assert body["authorization_servers"] == [harness.settings.issuer]


async def test_authorization_server_metadata_advertises_cimd(client, harness):
    """Claude selects CIMD only when *both* of these are present; a regression
    here silently downgrades every client to dynamic registration, which this
    adapter does not implement."""
    response = await client.get(f"{harness.adapter_url}/.well-known/oauth-authorization-server")
    assert response.status_code == 200

    body = response.json()
    assert body["client_id_metadata_document_supported"] is True
    assert body["token_endpoint_auth_methods_supported"] == ["none"]
    assert body["code_challenge_methods_supported"] == ["S256"]
    assert body["issuer"] == harness.settings.issuer
    assert body["authorization_endpoint"] == harness.settings.authorization_endpoint
    assert body["token_endpoint"] == harness.settings.token_endpoint


# ------------------------------------------------------------------- authorize


async def test_authorize_fetches_cimd_and_redirects_to_the_dashboard(client, harness):
    response = await start_authorization(client, harness)
    assert response.status_code == 302
    assert harness.metadata_host.request_count == 1

    location = urllib.parse.urlsplit(response.headers["location"])
    assert f"{location.scheme}://{location.netloc}{location.path}" == (
        f"{harness.dashboard_url}/mcp-auth"
    )
    query = urllib.parse.parse_qs(location.query)
    # One value for the dashboard to allowlist; everything else derives from it.
    assert query["adapter"] == [harness.adapter_url]
    assert query["state"]


async def test_authorize_accepts_an_ephemeral_loopback_port(client, harness):
    """The metadata document registers `http://localhost:1/callback`; the client
    presents whatever port it actually bound (RFC 8252 §7.3)."""
    response = await start_authorization(client, harness)
    assert response.status_code == 302
    assert harness.loopback.redirect_uri != "http://localhost:1/callback"


@pytest.mark.parametrize(
    "overrides",
    [
        {"response_type": "token"},
        {"code_challenge": None},
        {"code_challenge_method": "plain"},
        {"client_id": None},
        {"redirect_uri": None},
    ],
)
async def test_authorize_rejects_malformed_requests(client, harness, overrides):
    response = await start_authorization(client, harness, **overrides)
    assert response.status_code == 400
    assert response.json()["error"] in ("invalid_request", "unsupported_response_type")


async def test_authorize_rejects_an_unregistered_redirect_uri(client, harness):
    """The open-redirect guard: an unvalidated redirect_uri must never be honoured,
    so this is reported here rather than by bouncing the browser to it."""
    response = await start_authorization(client, harness, redirect_uri="http://evil.test/steal")
    assert response.status_code == 400
    assert response.json()["error"] == "invalid_request"


async def test_authorize_rejects_a_client_id_that_serves_no_document(client, harness):
    harness.metadata_host.status_code = 404
    response = await start_authorization(client, harness)
    assert response.status_code == 400


async def test_authorize_rejects_a_document_with_a_client_secret(client, harness):
    harness.metadata_host.extra_fields = {"client_secret": "hunter2"}
    response = await start_authorization(client, harness)
    assert response.status_code == 400


async def test_authorize_refuses_an_oversized_metadata_document(client, harness):
    """The cap aborts mid-response. A metadata host is chosen by whoever starts
    an authorization request, so it must not be able to make us buffer at will."""
    from estuary_mcp.auth import cimd

    harness.metadata_host.oversized_bytes = cimd.MAX_DOCUMENT_BYTES * 4
    response = await start_authorization(client, harness)
    assert response.status_code == 400
    assert "exceeds" in response.json()["error_description"]


# --------------------------------------------------------------------- consent


async def test_consent_context_comes_from_the_validated_document(client, harness):
    """The dashboard reads display strings from the adapter, not from its own URL,
    so a crafted link to `/mcp-auth` cannot make the consent screen lie."""
    harness.metadata_host.client_name = "Totally Legit Client"
    authorize = await start_authorization(client, harness)

    response = await client.get(
        f"{harness.adapter_url}/oauth/consent-context",
        params={"state": handoff_state(authorize)},
    )
    assert response.status_code == 200

    body = response.json()
    assert body["client_name"] == "Totally Legit Client"
    # The unforgeable half: whoever served the document, not what it claims.
    assert body["client_host"] == "127.0.0.1"
    assert body["resource"] == harness.settings.resource_url


async def test_consent_context_is_unknown_for_a_bogus_state(client, harness):
    response = await client.get(
        f"{harness.adapter_url}/oauth/consent-context", params={"state": "made-up"}
    )
    assert response.status_code == 404


async def test_user_denial_is_reported_to_the_client(client, harness):
    authorize = await start_authorization(client, harness)
    response = await client.get(
        f"{harness.adapter_url}/oauth/dashboard-callback",
        params={"state": handoff_state(authorize), "error": "access_denied"},
    )
    assert response.status_code == 302

    returned = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(response.headers["location"]).query))
    assert returned["error"] == "access_denied"
    # `state` must come back even on failure, or the client drops the response.
    assert returned["state"] == "client-state-abc"


async def test_an_unrecognized_dashboard_error_is_normalized(client, harness):
    """The dashboard's `error` ends up in a redirect to the client, so it is
    mapped through a known set rather than echoed."""
    authorize = await start_authorization(client, harness)
    response = await client.get(
        f"{harness.adapter_url}/oauth/dashboard-callback",
        params={"state": handoff_state(authorize), "error": "something_invented"},
    )

    returned = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(response.headers["location"]).query))
    assert returned["error"] == "access_denied"


async def test_a_replayed_handoff_credential_is_refused(client, harness):
    """The handoff is single-use, so an intercepted callback URL is inert: the
    adapter's own redemption already consumed it. The sealed state still opens —
    a stateless adapter cannot burn it — so the refusal comes from the control
    plane and is reported to the client as a denial."""
    authorize = await start_authorization(client, harness)
    state = handoff_state(authorize)
    handoff = harness.agent.refresh_token_blob(multi_use=False, ttl_seconds=300)

    first = await client.get(
        f"{harness.adapter_url}/oauth/dashboard-callback",
        params={"state": state, "handoff": handoff},
    )
    assert "code=" in first.headers["location"]

    replay = await client.get(
        f"{harness.adapter_url}/oauth/dashboard-callback",
        params={"state": state, "handoff": handoff},
    )
    assert replay.status_code == 302
    returned = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(replay.headers["location"]).query))
    assert returned["error"] == "access_denied"


async def test_a_failing_mint_is_reported_as_a_server_error(client, harness):
    """GraphQL reports failures inside an HTTP 200; the adapter must treat that
    as failure, not as a mint that returned nothing."""
    authorize = await start_authorization(client, harness)
    harness.agent.fail_graphql_with_errors = "createRefreshToken is not allowed here"

    response = await act_as_dashboard(client, harness, handoff_state(authorize))
    assert response.status_code == 302

    returned = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(response.headers["location"]).query))
    assert returned["error"] == "server_error"
    # The failure redirect still carries the client's own state.
    assert returned["state"] == "client-state-abc"


async def test_dashboard_callback_with_an_unknown_state_is_rejected(client, harness):
    response = await client.get(
        f"{harness.adapter_url}/oauth/dashboard-callback",
        params={"state": "made-up", "handoff": harness.agent.refresh_token_blob()},
    )
    assert response.status_code == 400


# ----------------------------------------------------------------------- token


async def test_full_dance_issues_estuary_credentials(client, harness):
    tokens = await complete_authorization(client, harness)

    assert tokens["token_type"] == "Bearer"
    assert tokens["expires_in"] > 3000  # a real one-hour control-plane token
    assert tokens["access_token"].count(".") == 2

    # The refresh token is an Estuary blob, opaque to the client but decodable
    # here — this is the "one unified token domain" property under test.
    decoded = json.loads(base64.b64decode(tokens["refresh_token"]))
    assert set(decoded) == {"id", "secret"}

    # The client's credential is tagged with the CIMD URL that identifies it, so
    # a user can see and revoke it per client in the dashboard.
    minted = harness.agent.tokens[decoded["id"]]
    assert minted.detail == f"MCP client: {harness.metadata_host.client_id}"
    assert minted.multi_use is False


async def test_authorization_code_requires_the_matching_pkce_verifier(client, harness):
    authorize = await start_authorization(client, harness)
    dashboard = await act_as_dashboard(client, harness, handoff_state(authorize))
    code = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(dashboard.headers["location"]).query))["code"]

    response = await client.post(
        f"{harness.adapter_url}/oauth/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "code_verifier": "wrong-verifier",
            "redirect_uri": harness.loopback.redirect_uri,
        },
    )
    assert response.status_code == 400
    assert response.json()["error"] == "invalid_grant"


async def test_an_authorization_code_can_only_be_redeemed_once(client, harness):
    """Single-use without server state: the first redemption rotates the refresh
    token sealed inside the code, so a replay presents a credential the control
    plane already burned."""
    authorize = await start_authorization(client, harness)
    dashboard = await act_as_dashboard(client, harness, handoff_state(authorize))
    code = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(dashboard.headers["location"]).query))["code"]

    form = {
        "grant_type": "authorization_code",
        "code": code,
        "code_verifier": authorize.pkce_verifier,  # type: ignore[attr-defined]
        "redirect_uri": harness.loopback.redirect_uri,
    }
    assert (await client.post(f"{harness.adapter_url}/oauth/token", data=form)).status_code == 200

    replay = await client.post(f"{harness.adapter_url}/oauth/token", data=form)
    assert replay.status_code == 400
    assert replay.json()["error"] == "invalid_grant"


@pytest.mark.parametrize(
    ("field", "wrong_value"),
    [
        ("redirect_uri", "http://localhost:9/other-callback"),
        ("client_id", "https://evil.test/oauth/meta"),
    ],
)
async def test_token_endpoint_rejects_a_mismatched_binding(client, harness, field, wrong_value):
    """RFC 6749 §4.1.3: the token request must present the same redirect_uri and
    client_id the code was issued against."""
    authorize = await start_authorization(client, harness)
    dashboard = await act_as_dashboard(client, harness, handoff_state(authorize))
    code = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(dashboard.headers["location"]).query))["code"]

    form = {
        "grant_type": "authorization_code",
        "code": code,
        "code_verifier": authorize.pkce_verifier,  # type: ignore[attr-defined]
        "redirect_uri": harness.loopback.redirect_uri,
        "client_id": harness.metadata_host.client_id,
        field: wrong_value,
    }
    response = await client.post(f"{harness.adapter_url}/oauth/token", data=form)
    assert response.status_code == 400
    assert response.json()["error"] == "invalid_grant"

    # The mismatch was detected before the exchange, so the code is still live
    # and the legitimate client can proceed.
    form[field] = {"redirect_uri": harness.loopback.redirect_uri,
                   "client_id": harness.metadata_host.client_id}[field]
    assert (await client.post(f"{harness.adapter_url}/oauth/token", data=form)).status_code == 200


async def test_refresh_rotates_and_kills_the_previous_token(client, harness):
    """Single-use rotation, which is the OAuth 2.1 shape for public clients and
    the behaviour most likely to trip up a client that mishandles persistence."""
    tokens = await complete_authorization(client, harness)

    refreshed = await client.post(
        f"{harness.adapter_url}/oauth/token",
        data={"grant_type": "refresh_token", "refresh_token": tokens["refresh_token"]},
    )
    assert refreshed.status_code == 200

    rotated = refreshed.json()
    assert rotated["refresh_token"] != tokens["refresh_token"]
    assert rotated["access_token"]

    # The old blob is dead. A client that failed to persist the rotation finds
    # out here, which is exactly the failure mode this PoC wants to surface.
    replay = await client.post(
        f"{harness.adapter_url}/oauth/token",
        data={"grant_type": "refresh_token", "refresh_token": tokens["refresh_token"]},
    )
    assert replay.status_code == 400
    assert replay.json()["error"] == "invalid_grant"

    # ...and the rotated one still works, so the chain continues.
    again = await client.post(
        f"{harness.adapter_url}/oauth/token",
        data={"grant_type": "refresh_token", "refresh_token": rotated["refresh_token"]},
    )
    assert again.status_code == 200


async def test_a_multi_use_refresh_token_is_returned_unrotated(client, harness):
    """The control plane omits `refresh_token` for multi-use credentials; the
    adapter hands back the presented blob so the client's persistence logic is
    identical either way — flipping the design to multi-use must stay a
    one-argument change at the mint site."""
    blob = harness.agent.refresh_token_blob(multi_use=True, ttl_seconds=3600)

    first = await client.post(
        f"{harness.adapter_url}/oauth/token",
        data={"grant_type": "refresh_token", "refresh_token": blob},
    )
    assert first.status_code == 200
    assert first.json()["refresh_token"] == blob

    # No rotation: the same blob keeps working.
    second = await client.post(
        f"{harness.adapter_url}/oauth/token",
        data={"grant_type": "refresh_token", "refresh_token": blob},
    )
    assert second.status_code == 200


@pytest.mark.parametrize(
    ("form", "expected_error"),
    [
        ({"grant_type": "client_credentials"}, "unsupported_grant_type"),
        ({"grant_type": "refresh_token"}, "invalid_request"),
        ({"grant_type": "refresh_token", "refresh_token": "not-base64!"}, "invalid_grant"),
        ({"grant_type": "authorization_code", "code": "x"}, "invalid_request"),
    ],
)
async def test_token_endpoint_rejects_bad_requests(client, harness, form, expected_error):
    response = await client.post(f"{harness.adapter_url}/oauth/token", data=form)
    assert response.status_code == 400
    assert response.json()["error"] == expected_error


# ------------------------------------------------------------------ tool calls


async def test_issued_token_can_call_the_prefixes_tool(client, harness):
    """The acceptance test in one function: a token that only exists because the
    whole dance succeeded reaches a control-plane query that needs a real user."""
    tokens = await complete_authorization(client, harness)

    response = await call_tool(client, harness, tokens["access_token"], "prefixes")
    assert response.status_code == 200

    result = _json_rpc_result(response)
    assert result["isError"] is False
    payload = json.loads(result["content"][0]["text"])
    assert [entry["prefix"] for entry in payload["prefixes"]] == ["acmeCo/", "acmeCo/marketing/"]

    # The caller's own bearer reached the control plane — no adapter credential.
    assert harness.agent.graphql_requests[-1]["variables"] == {"first": 200}


async def test_tool_call_absorbs_the_snapshot_staleness_redirect(client, harness):
    """The agent's 307 retry protocol is internal to the control plane; an MCP
    client must never see one."""
    tokens = await complete_authorization(client, harness)
    harness.agent.stale_graphql_responses = 2

    response = await call_tool(client, harness, tokens["access_token"], "prefixes")
    assert response.status_code == 200
    assert _json_rpc_result(response)["isError"] is False, response.text
    assert harness.agent.stale_graphql_responses == 0


async def test_an_expired_access_token_gets_a_401_not_a_tool_error(client, harness):
    """Expiry is the routine cause of a stale credential, and MCP's only
    re-authentication signal is a transport 401 — a tool-level error would leave
    the client retrying forever."""
    expired = _jwt_expiring_at(-60)
    response = await call_tool(client, harness, expired, "prefixes")
    assert response.status_code == 401
    assert "www-authenticate" in response.headers


async def test_a_refresh_blob_presented_as_a_bearer_is_refused_locally(client, harness):
    """Never forward a refresh blob to the control plane: its envelope accepts
    one, at the price of a bcrypt verify and a DB write per request."""
    blob = harness.agent.refresh_token_blob()
    response = await call_tool(client, harness, blob, "prefixes")
    assert response.status_code == 401
    assert harness.agent.graphql_requests == []


def _json_rpc_result(response: httpx.Response) -> dict:
    return response.json()["result"]


def _jwt_expiring_at(offset_seconds: int) -> str:
    import time

    encode = lambda part: base64.urlsafe_b64encode(json.dumps(part).encode()).rstrip(b"=").decode()
    header = encode({"alg": "HS256"})
    claims = encode({"sub": "user", "exp": int(time.time()) + offset_seconds})
    return f"{header}.{claims}.c2ln"
