"""Sealed in-flight authorization state, plus PKCE verification.

The adapter keeps no server-side record of authorization requests in flight.
Instead, everything the dance needs to remember rides inside the two opaque
strings OAuth already passes around:

  * The handoff **state** — the adapter's own `state` on the dashboard redirect —
    is a sealed `PendingAuthorization`: the validated outcome of `/oauth/authorize`
    (CIMD identity, matched redirect_uri, PKCE challenge). It lives from the
    redirect to the dashboard until the browser comes back — minutes.
  * The **authorization code** is a sealed `IssuedCode` carrying the client's
    freshly minted — and *not yet exchanged* — refresh token. It lives from the
    dashboard callback until the client redeems it at `/oauth/token` — seconds.

Sealing means AEAD (AES-256-GCM): confidential, because the code blob contains a
credential and travels through the client's loopback redirect and the browser's
history; and authenticated, because the state blob is the proof that this process
validated the CIMD document and redirect_uri — a forged or tampered blob must not
be able to skip that.

This is what lets the adapter run as N identical replicas with no shared store
and no session affinity: any replica can unseal what any other sealed, given the
same `ESTUARY_MCP_SEALING_KEYS`. And note what sealing does *not* try to do:
single-use. A stateless process cannot burn a blob, so every single-use guarantee
lives where the state lives — in the control plane. A replayed dashboard callback
re-presents a handoff credential the first callback already consumed; a replayed
authorization code re-presents a refresh token the first redemption already
rotated. Both die there, not here.

The sealing key is NOT a control-plane credential and its compromise grants no
standing access to Estuary — see README "The sealing key" for what it is and what
a leak actually costs.
"""

import base64
import dataclasses
import hashlib
import hmac
import json
import logging
import os
import time
from typing import TypeVar

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

log = logging.getLogger(__name__)

# How long a user has to log in and consent in the dashboard.
STATE_TTL_SECONDS = 10 * 60
# OAuth 2.1 wants authorization codes to live no longer than a minute or so; the
# client redeems immediately after its loopback listener fires.
CODE_TTL_SECONDS = 60

KEY_BYTES = 32
NONCE_BYTES = 12
# A format-version byte prefixes every blob so the wire shape can evolve without
# ambiguity. Bump it on any change to the payload schema or cipher composition.
FORMAT_VERSION = 1


@dataclasses.dataclass(frozen=True)
class PendingAuthorization:
    """An authorization request parked while the user logs in and consents.

    Everything here came from the client's `/authorize` request and has already
    been validated: the client fields are from a fetched CIMD document, and
    `redirect_uri` matched one of its `redirect_uris`. Only the display fields
    the consent screen needs are carried — not the whole document — to keep the
    blob comfortably inside URL length budgets.
    """

    client_id: str
    client_name: str
    # The hostname that served the CIMD document — the unforgeable half of the
    # client's identity, which the consent screen is required to show.
    display_host: str
    client_uri: str | None
    # The *presented* URI, not the registered one it matched: a loopback client's
    # ephemeral port lives only in the former.
    redirect_uri: str
    # The *client's* `state`, echoed back untouched on the final redirect. Not to
    # be confused with the handoff state, which is this record, sealed.
    client_state: str | None
    code_challenge: str
    # RFC 8707 resource indicator. Accepted and recorded; the PoC has a single
    # resource, so nothing branches on it yet.
    resource: str | None
    issued_at: float


@dataclasses.dataclass(frozen=True)
class IssuedCode:
    """An authorization code: the client's minted-but-unexchanged refresh token,
    bound to the request that earned it.

    The first exchange happens at `/oauth/token`, after PKCE — deliberately not
    at mint time. The refresh token inside is single-use, so redemption *is* the
    burn: the control plane rotates it, and a replayed code presents a credential
    that no longer exists.
    """

    client_id: str
    redirect_uri: str
    code_challenge: str
    # The client's refresh token in wire form (base64 `{id, secret}` blob).
    refresh_token: str
    issued_at: float


BlobT = TypeVar("BlobT", PendingAuthorization, IssuedCode)


class Sealer:
    """Seals and unseals the two blob kinds under a shared, deployment-scoped key.

    `keys[0]` encrypts; every key may decrypt. That ordering is the whole key
    rotation story: deploy with `[new, old]`, and blobs sealed by not-yet-updated
    replicas keep unsealing until the in-flight window (ten minutes at most)
    drains, after which `old` can be dropped.

    The AAD binds each blob to its purpose and to this deployment's issuer, so a
    state blob cannot be replayed as a code, and a blob sealed by a different
    deployment that somehow shares a key still fails to open.
    """

    def __init__(self, keys: tuple[bytes, ...], issuer: str):
        if not keys:
            raise ValueError("at least one sealing key is required")
        for key in keys:
            if len(key) != KEY_BYTES:
                raise ValueError(f"sealing keys must be {KEY_BYTES} bytes")
        self._ciphers = [AESGCM(key) for key in keys]
        self._issuer = issuer

    def seal_pending(self, pending: PendingAuthorization) -> str:
        return self._seal(pending, purpose="state")

    def unseal_pending(self, blob: str) -> PendingAuthorization | None:
        return self._unseal(blob, PendingAuthorization, purpose="state", ttl=STATE_TTL_SECONDS)

    def seal_code(self, code: IssuedCode) -> str:
        return self._seal(code, purpose="code")

    def unseal_code(self, blob: str) -> IssuedCode | None:
        return self._unseal(blob, IssuedCode, purpose="code", ttl=CODE_TTL_SECONDS)

    def _seal(self, record: PendingAuthorization | IssuedCode, *, purpose: str) -> str:
        payload = json.dumps(dataclasses.asdict(record), separators=(",", ":")).encode()
        nonce = os.urandom(NONCE_BYTES)
        sealed = self._ciphers[0].encrypt(nonce, payload, self._aad(purpose))
        raw = bytes([FORMAT_VERSION]) + nonce + sealed
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

    def _unseal(self, blob: str, cls: type[BlobT], *, purpose: str, ttl: float) -> BlobT | None:
        """Open a blob, or return None.

        Every failure — tampering, a key this deployment no longer holds, the
        wrong purpose, an evolved format, expiry, or plain garbage — collapses to
        None on purpose: callers translate it to their protocol's single
        "unknown or expired" answer, and none of these cases deserves a
        distinguishable error an attacker could probe.
        """
        try:
            raw = base64.urlsafe_b64decode(blob + "=" * (-len(blob) % 4))
        except (ValueError, TypeError):
            return None
        if len(raw) < 1 + NONCE_BYTES or raw[0] != FORMAT_VERSION:
            return None

        nonce, sealed = raw[1 : 1 + NONCE_BYTES], raw[1 + NONCE_BYTES :]
        for cipher in self._ciphers:
            try:
                payload = cipher.decrypt(nonce, sealed, self._aad(purpose))
                break
            except InvalidTag:
                continue
        else:
            log.debug("a %s blob failed to unseal under any configured key", purpose)
            return None

        try:
            record = cls(**json.loads(payload))
        except (ValueError, TypeError):
            # A decrypt under a valid key that fails to parse means the schema
            # changed without a FORMAT_VERSION bump — a bug, but one an in-flight
            # deploy can produce transiently, so it degrades like expiry does.
            log.warning("a %s blob unsealed but did not parse; schema drift?", purpose)
            return None

        if now() - record.issued_at > ttl:
            return None
        return record

    def _aad(self, purpose: str) -> bytes:
        return f"estuary-mcp/{purpose}/{self._issuer}".encode()


def verify_pkce(code_verifier: str, code_challenge: str) -> bool:
    """Check an RFC 7636 S256 code verifier against the challenge from `/authorize`.

    S256 only — `plain` offers no protection against an attacker who can observe
    the authorization request, and the metadata document advertises S256 alone.
    Compared with `compare_digest` because this is a secret comparison.
    """
    digest = hashlib.sha256(code_verifier.encode("ascii", errors="ignore")).digest()
    expected = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return hmac.compare_digest(expected, code_challenge)


def now() -> float:
    """Wall clock, not monotonic: blobs are sealed by one replica and unsealed by
    another, so timestamps must mean the same thing across processes. The cost is
    NTP sensitivity, which at ten-minute TTLs is noise."""
    return time.time()
