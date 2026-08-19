"""Sealed in-flight authorization state, and PKCE."""

import base64
import dataclasses
import hashlib
import os

import pytest

from estuary_mcp.auth import sealed

KEY = b"k" * 32
OTHER_KEY = b"j" * 32
ISSUER = "http://adapter.test"


def _sealer(keys: tuple[bytes, ...] = (KEY,), issuer: str = ISSUER) -> sealed.Sealer:
    return sealed.Sealer(keys, issuer=issuer)


def _pending(issued_at: float | None = None) -> sealed.PendingAuthorization:
    return sealed.PendingAuthorization(
        client_id="https://claude.ai/oauth/meta",
        client_name="Claude Code",
        display_host="claude.ai",
        client_uri=None,
        redirect_uri="http://localhost:1234/callback",
        client_state="client-state",
        code_challenge="challenge",
        resource=None,
        issued_at=sealed.now() if issued_at is None else issued_at,
    )


def _code(issued_at: float | None = None) -> sealed.IssuedCode:
    return sealed.IssuedCode(
        client_id="https://claude.ai/oauth/meta",
        redirect_uri="http://localhost:1234/callback",
        code_challenge="challenge",
        refresh_token="cmVmcmVzaA==",
        issued_at=sealed.now() if issued_at is None else issued_at,
    )


def test_blobs_round_trip():
    sealer = _sealer()
    pending, code = _pending(), _code()
    assert sealer.unseal_pending(sealer.seal_pending(pending)) == pending
    assert sealer.unseal_code(sealer.seal_code(code)) == code


def test_any_replica_with_the_key_can_unseal():
    """The multi-replica property under test: sealing and unsealing happen in
    different Sealer instances, standing in for different processes."""
    blob = _sealer().seal_pending(_pending())
    assert _sealer().unseal_pending(blob) is not None


def test_tampering_is_detected():
    sealer = _sealer()
    blob = sealer.seal_pending(_pending())

    raw = bytearray(base64.urlsafe_b64decode(blob + "=" * (-len(blob) % 4)))
    raw[-1] ^= 0x01  # Flip one bit of the ciphertext/tag.
    tampered = base64.urlsafe_b64encode(bytes(raw)).rstrip(b"=").decode()

    assert sealer.unseal_pending(tampered) is None


def test_a_state_blob_is_not_an_authorization_code():
    """The AAD binds each blob to its purpose: presenting the dashboard-handoff
    state at the token endpoint must not open anything."""
    sealer = _sealer()
    assert sealer.unseal_code(sealer.seal_pending(_pending())) is None
    assert sealer.unseal_pending(sealer.seal_code(_code())) is None


def test_blobs_are_bound_to_the_issuer():
    blob = _sealer().seal_pending(_pending())
    assert _sealer(issuer="http://other.test").unseal_pending(blob) is None


def test_expired_blobs_do_not_open():
    sealer = _sealer()
    stale_pending = _pending(issued_at=sealed.now() - sealed.STATE_TTL_SECONDS - 1)
    stale_code = _code(issued_at=sealed.now() - sealed.CODE_TTL_SECONDS - 1)

    assert sealer.unseal_pending(sealer.seal_pending(stale_pending)) is None
    assert sealer.unseal_code(sealer.seal_code(stale_code)) is None


def test_key_rotation_keeps_old_blobs_openable():
    """Deploying `[new, old]` must keep in-flight dances alive: the first key
    encrypts, but every configured key may decrypt."""
    blob = _sealer((OTHER_KEY,)).seal_pending(_pending())

    assert _sealer((KEY, OTHER_KEY)).unseal_pending(blob) is not None
    assert _sealer((KEY,)).unseal_pending(blob) is None


def test_garbage_blobs_do_not_open():
    sealer = _sealer()
    for garbage in ("", "not-base64!", base64.urlsafe_b64encode(os.urandom(64)).decode()):
        assert sealer.unseal_pending(garbage) is None
        assert sealer.unseal_code(garbage) is None


def test_an_unknown_format_version_does_not_open():
    sealer = _sealer()
    blob = sealer.seal_pending(_pending())
    raw = bytearray(base64.urlsafe_b64decode(blob + "=" * (-len(blob) % 4)))
    raw[0] = sealed.FORMAT_VERSION + 1
    assert sealer.unseal_pending(base64.urlsafe_b64encode(bytes(raw)).decode()) is None


def test_schema_drift_degrades_to_none():
    """A blob sealed with extra fields (a schema change without a version bump)
    must degrade like expiry, not crash the endpoint that unseals it."""

    @dataclasses.dataclass(frozen=True)
    class DriftedCode(sealed.IssuedCode):
        surprise: bool = True

    sealer = _sealer()
    drifted = DriftedCode(**dataclasses.asdict(_code()))
    assert sealer.unseal_code(sealer.seal_code(drifted)) is None
    assert sealer.unseal_code(sealer.seal_code(_code())) is not None


def test_sealer_refuses_malformed_keys():
    with pytest.raises(ValueError):
        sealed.Sealer((), issuer=ISSUER)
    with pytest.raises(ValueError):
        sealed.Sealer((b"short",), issuer=ISSUER)


# ------------------------------------------------------------------------ PKCE


def _challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode()).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode()


def test_pkce_accepts_a_matching_verifier():
    verifier = "a" * 64
    assert sealed.verify_pkce(verifier, _challenge(verifier))


@pytest.mark.parametrize("wrong", ["b" * 64, "", "a" * 63])
def test_pkce_rejects_a_mismatched_verifier(wrong):
    assert not sealed.verify_pkce(wrong, _challenge("a" * 64))


def test_pkce_rejects_a_plain_challenge():
    """`plain` PKCE is not advertised and must not be silently accepted: on a
    loopback redirect, any local process can read the authorization request."""
    verifier = "a" * 64
    assert not sealed.verify_pkce(verifier, verifier)
