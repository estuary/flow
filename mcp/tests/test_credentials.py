"""Credential encoding: the format the adapter shares with the control plane."""

import base64
import json
import time

import pytest

from estuary_mcp import credentials


def test_refresh_token_round_trip():
    token = credentials.RefreshToken(id="00aabbccddeeff11", secret="s3cr3t")
    assert credentials.decode_refresh_token(credentials.encode_refresh_token(token)) == token


def test_decode_accepts_the_dashboard_encoding():
    """The dashboard mints handoff blobs with `btoa(JSON.stringify({id, secret}))`.

    This is the interop point between the paired UI change and the adapter; if it
    ever breaks, the whole browser leg fails with an opaque `invalid_grant`.
    """
    blob = base64.b64encode(json.dumps({"id": "0011", "secret": "abc"}).encode()).decode()
    decoded = credentials.decode_refresh_token(blob)
    assert (decoded.id, decoded.secret) == ("0011", "abc")


@pytest.mark.parametrize(
    "blob",
    [
        "not base64!",
        base64.b64encode(b"not json").decode(),
        base64.b64encode(b'["not", "an", "object"]').decode(),
        base64.b64encode(b'{"id": 5, "secret": "abc"}').decode(),
        base64.b64encode(b'{"id": "0011"}').decode(),
    ],
)
def test_decode_rejects_malformed_blobs(blob):
    with pytest.raises(credentials.CredentialError):
        credentials.decode_refresh_token(blob)


def _jwt(claims: dict) -> str:
    """A structurally valid JWT with a garbage signature.

    Garbage on purpose: nothing in the adapter verifies signatures, and a test
    that supplied a real one would imply otherwise.
    """
    encode = lambda part: base64.urlsafe_b64encode(json.dumps(part).encode()).rstrip(b"=").decode()
    return f"{encode({'alg': 'HS256'})}.{encode(claims)}.c2lnbmF0dXJl"


def test_expires_in_reads_the_unverified_exp():
    token = _jwt({"exp": int(time.time()) + 3600, "sub": "user-1"})
    expires_in = credentials.access_token_expires_in(token)
    assert expires_in is not None and 3590 < expires_in <= 3600


def test_expires_in_is_none_for_an_expired_token():
    assert credentials.access_token_expires_in(_jwt({"exp": int(time.time()) - 5})) is None


@pytest.mark.parametrize("token", ["", "opaque", "a.b", base64.b64encode(b"{}").decode()])
def test_claims_are_none_for_non_jwts(token):
    """A refresh-token blob presented as a bearer must not parse as a JWT — that
    is what lets the verifier reject it before it reaches the control plane."""
    assert credentials.access_token_expires_in(token) is None
    assert credentials.access_token_subject(token) is None


def test_subject_is_read_from_claims():
    assert credentials.access_token_subject(_jwt({"sub": "user-1"})) == "user-1"
