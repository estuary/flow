"""CIMD validation. Every case here is a way an attacker could try to be someone else."""

import json

import pytest

from estuary_mcp.auth import cimd

CLIENT_ID = "https://claude.ai/oauth/claude-code-client-metadata"


def _document(**overrides) -> bytes:
    document = {
        "client_id": CLIENT_ID,
        "client_name": "Claude Code",
        "redirect_uris": ["http://localhost:1234/callback"],
    }
    document.update(overrides)
    return json.dumps(document).encode()


def test_accepts_a_well_formed_client_id():
    cimd.validate_client_id_url(CLIENT_ID)


@pytest.mark.parametrize(
    ("client_id", "reason"),
    [
        ("http://claude.ai/oauth/meta", "http is not https"),
        ("https://claude.ai", "no path: would claim every client on the host"),
        ("https://claude.ai/", "empty path, same problem"),
        ("https://user@claude.ai/oauth/meta", "userinfo can disguise the real host"),
        ("https://claude.ai/oauth/meta?x=1", "query would let the id be varied"),
        ("https://claude.ai/oauth/meta#f", "fragment, likewise"),
        ("https://claude.ai/oauth/../meta", "dot-segments need normalizing"),
        ("https://claude.ai/./meta", "dot-segments need normalizing"),
    ],
)
def test_rejects_unsafe_client_ids(client_id, reason):
    with pytest.raises(cimd.CimdError):
        cimd.validate_client_id_url(client_id)


def test_http_client_ids_are_allowed_only_under_the_test_escape_hatch():
    with pytest.raises(cimd.CimdError):
        cimd.validate_client_id_url("http://localhost:9/meta")
    cimd.validate_client_id_url("http://localhost:9/meta", allow_insecure=True)


def test_parses_a_valid_document():
    metadata = cimd.parse_client_metadata(CLIENT_ID, _document(client_uri="https://claude.ai"))
    assert metadata.client_name == "Claude Code"
    assert metadata.client_uri == "https://claude.ai"
    assert metadata.display_host == "claude.ai"


def test_display_host_is_the_url_host_not_the_document():
    """The consent screen's anti-phishing property: a document may claim any name,
    but it can only be served from the host in its own client_id."""
    metadata = cimd.parse_client_metadata(CLIENT_ID, _document(client_name="Estuary Official"))
    assert metadata.client_name == "Estuary Official"
    assert metadata.display_host == "claude.ai"


def test_rejects_a_document_that_names_a_different_client_id():
    with pytest.raises(cimd.CimdError):
        cimd.parse_client_metadata(CLIENT_ID, _document(client_id="https://evil.test/meta"))


def test_rejects_a_document_carrying_a_client_secret():
    with pytest.raises(cimd.CimdError):
        cimd.parse_client_metadata(CLIENT_ID, _document(client_secret="hunter2"))


@pytest.mark.parametrize(
    "overrides",
    [
        {"client_name": ""},
        {"client_name": None},
        {"redirect_uris": []},
        {"redirect_uris": "http://localhost/cb"},
        {"redirect_uris": [1, 2]},
    ],
)
def test_rejects_incomplete_documents(overrides):
    with pytest.raises(cimd.CimdError):
        cimd.parse_client_metadata(CLIENT_ID, _document(**overrides))


@pytest.mark.parametrize("body", [b"not json", b'"a string"', b"[]"])
def test_rejects_non_object_documents(body):
    with pytest.raises(cimd.CimdError):
        cimd.parse_client_metadata(CLIENT_ID, body)


def test_redirect_uri_exact_match():
    registered = ["https://claude.ai/api/mcp/auth_callback"]
    assert cimd.assert_redirect_uri_allowed(registered[0], registered) is None


def test_loopback_redirect_ignores_the_port():
    """RFC 8252 §7.3. A native client binds an ephemeral port when the flow starts,
    so it cannot have published that port in its metadata document."""
    registered = ["http://localhost:1234/callback"]
    cimd.assert_redirect_uri_allowed("http://localhost:57391/callback", registered)
    cimd.assert_redirect_uri_allowed("http://localhost/callback", registered)


def test_loopback_host_spellings_are_equivalent():
    """RFC 8252 §7.3 tells clients to prefer the literal address over `localhost`,
    so a document and a request can legitimately disagree on the spelling. Every
    spelling reaches the same machine, so equating them grants nothing new."""
    registered = ["http://localhost:1234/callback"]
    cimd.assert_redirect_uri_allowed("http://127.0.0.1:57391/callback", registered)
    cimd.assert_redirect_uri_allowed("http://[::1]:57391/callback", registered)


def test_loopback_laxity_does_not_extend_past_host_and_port():
    registered = ["http://localhost:1234/callback"]
    for requested in (
        "http://localhost:1234/other",  # different path
        "https://localhost:1234/callback",  # different scheme
        "http://localhost:1234/callback?x=1",  # extra query
        "http://evil.test:1234/callback",  # not loopback at all
    ):
        with pytest.raises(cimd.CimdError):
            cimd.assert_redirect_uri_allowed(requested, registered)


def test_non_loopback_hosts_must_match_the_port():
    registered = ["https://claude.ai:443/cb"]
    with pytest.raises(cimd.CimdError):
        cimd.assert_redirect_uri_allowed("https://claude.ai:8443/cb", registered)


def test_redirect_uri_with_a_fragment_is_rejected():
    """RFC 6749 §3.1.2. Response parameters are appended to this string later;
    appended after a fragment, they would never reach the client."""
    registered = ["http://localhost:1234/callback"]
    with pytest.raises(cimd.CimdError, match="fragment"):
        cimd.assert_redirect_uri_allowed("http://localhost:1234/callback#frag", registered)


def test_unregistered_redirect_uri_is_rejected():
    with pytest.raises(cimd.CimdError):
        cimd.assert_redirect_uri_allowed("http://evil.test/cb", ["http://localhost:1/callback"])


@pytest.mark.parametrize(
    "url",
    [
        "https://localhost/meta",
        "https://127.0.0.1/meta",
        "https://10.0.0.1/meta",
        "https://169.254.169.254/latest/meta",  # cloud instance metadata
    ],
)
def test_ssrf_guard_refuses_non_public_targets(url):
    with pytest.raises(cimd.CimdError):
        cimd._assert_safe_target(url)
