"""Pure encoding/decoding of Estuary credentials. No IO lives here.

MCP clients hold *real* Estuary credentials — there is no second token domain
and no mapping table (see README "Token architecture"). This module is the
whole of the adapter's knowledge about their shape:

  * A refresh token is `base64(JSON {"id": <hex Id>, "secret": <str>})`. The
    control plane parses it with `tokens::jwt::parse_base64` (standard alphabet,
    padded), so we encode the same way. Clients treat the blob as opaque, which
    is exactly what OAuth expects of a refresh token.
  * An access token is a control-plane JWT. The adapter does NOT verify it (it
    holds no keys and is not a trust boundary), but it does read `exp` to fill
    in OAuth's `expires_in`.
"""

import base64
import binascii
import dataclasses
import json
import time
from typing import Any


class CredentialError(ValueError):
    """A credential could not be decoded. Always surfaced as an OAuth
    `invalid_grant`, never with detail that would help probe the format."""


@dataclasses.dataclass(frozen=True)
class RefreshToken:
    """The `{id, secret}` pair inside a refresh-token blob."""

    id: str
    secret: str


def decode_refresh_token(blob: str) -> RefreshToken:
    try:
        raw = base64.b64decode(blob, validate=True)
    except (binascii.Error, ValueError) as err:
        raise CredentialError("refresh token is not valid base64") from err

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as err:
        raise CredentialError("refresh token does not contain JSON") from err

    if not isinstance(parsed, dict):
        raise CredentialError("refresh token JSON is not an object")

    token_id, secret = parsed.get("id"), parsed.get("secret")
    if not isinstance(token_id, str) or not isinstance(secret, str):
        raise CredentialError("refresh token is missing a string `id` and `secret`")

    return RefreshToken(id=token_id, secret=secret)


def encode_refresh_token(token: RefreshToken) -> str:
    """Inverse of `decode_refresh_token`. Key order matches what the dashboard
    emits so blobs are byte-identical whichever side minted them; nothing
    depends on that, but it makes the two paths comparable while debugging."""
    payload = json.dumps({"id": token.id, "secret": token.secret}, separators=(",", ":"))
    return base64.b64encode(payload.encode()).decode()


def access_token_expires_in(access_token: str, now: float | None = None) -> int | None:
    """Seconds until `access_token` expires, or `None` if already expired or unreadable.

    This is OAuth's `expires_in`: a *hint* the adapter relays to the MCP client
    so it can refresh proactively, not a security decision. Code deciding
    whether a token is still live wants `access_token_expiry` instead — this one
    collapses "already expired" and "unknown" into the same `None`, which is
    right for an optional response field and wrong for a check.
    """
    exp = access_token_expiry(access_token)
    if exp is None:
        return None

    remaining = int(exp - (time.time() if now is None else now))
    return remaining if remaining > 0 else None


def access_token_expiry(access_token: str) -> int | None:
    """The unverified `exp` claim as an absolute epoch time, or `None`.

    Unverified on purpose: the adapter holds no keys (see README "Trust model").
    A token forged to claim a later expiry gains nothing, because the control
    plane still rejects it on the very next call — so this value is only ever
    used to fail *early*, never to admit something that would otherwise be
    refused.
    """
    claims = _unverified_claims(access_token)
    if claims is None:
        return None

    exp = claims.get("exp")
    return int(exp) if isinstance(exp, (int, float)) else None


def access_token_subject(access_token: str) -> str | None:
    """The unverified `sub` claim, used only for log correlation and to populate
    the SDK's `AccessToken.subject`. Never used to make an access decision."""
    claims = _unverified_claims(access_token)
    if claims is None:
        return None
    sub = claims.get("sub")
    return sub if isinstance(sub, str) else None


def _unverified_claims(access_token: str) -> dict[str, Any] | None:
    """Decode a JWT payload without verifying the signature.

    Named for what it is. Anything reading this must see, at the call site, that
    the values are attacker-controlled until the control plane says otherwise.
    """
    parts = access_token.split(".")
    if len(parts) != 3:
        return None

    payload = parts[1]
    # JWT uses unpadded base64url; restore the padding b64decode requires.
    payload += "=" * (-len(payload) % 4)
    try:
        return json.loads(base64.urlsafe_b64decode(payload))
    except (binascii.Error, ValueError):
        return None
