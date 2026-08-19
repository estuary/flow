"""Client ID Metadata Documents (CIMD): identifying an MCP client without registration.

Pinned to `draft-ietf-oauth-client-id-metadata-document-00`, which is the draft
the 2026-07-28 MCP spec references.

The idea: instead of dynamic client registration, a client's `client_id` *is* an
https URL that serves its own OAuth client metadata. The authorization server
fetches that URL at authorization time. This is what makes the adapter's
statelessness possible — there is no client registry to keep.

Two things about this are dangerous, and everything below exists because of them:

 1. **Fetching an attacker-supplied URL is an SSRF primitive.** Anyone can start
    an authorization request naming any `client_id`. `_assert_safe_target`
    resolves the host and refuses anything that is not a global unicast address,
    and the fetch never follows redirects (a redirect is the classic way to smuggle
    a resolved-safe URL into an internal one).

 2. **Everything in the document is attacker-chosen** — including `client_name`
    and `logo_uri`. A consent screen that renders only `client_name` is a
    phishing surface. The one field an attacker cannot forge is the *hostname*
    of the `client_id` URL, because serving the document from that host is what
    it takes to claim it. So `ClientMetadata` carries `display_host`, and the
    consent UI is required to show it alongside the name.
"""

import asyncio
import dataclasses
import ipaddress
import json
import logging
import socket
import urllib.parse

import httpx

log = logging.getLogger(__name__)

# A client-metadata document is a handful of JSON fields. The cap is a defense
# against a hostile or broken endpoint streaming until we run out of memory;
# the draft's guidance is to enforce a bound, not any particular one.
MAX_DOCUMENT_BYTES = 5 * 1024
FETCH_TIMEOUT_SECONDS = 10.0


class CimdError(Exception):
    """The client_id URL or its document failed validation. The message is safe
    to return to the client in an OAuth `invalid_request` — it describes only
    what the client itself supplied."""


@dataclasses.dataclass(frozen=True)
class ClientMetadata:
    """A validated client-metadata document."""

    client_id: str
    client_name: str
    redirect_uris: list[str]
    client_uri: str | None
    logo_uri: str | None

    @property
    def display_host(self) -> str:
        """The hostname that served this document — the only unforgeable part of
        a client's identity, and therefore the part the consent screen must show.
        `client_name` is whatever the document says; this is not."""
        return urllib.parse.urlsplit(self.client_id).hostname or self.client_id


def validate_client_id_url(client_id: str, *, allow_insecure: bool = False) -> None:
    """Check `client_id` against the draft's URL rules, before any network IO.

    The rules exist so that a `client_id` denotes exactly one document and cannot
    be varied to smuggle state: no userinfo (which would let `evil.com` masquerade
    as `https://claude.ai@evil.com/...` in a hurried reading), no query or
    fragment, no dot-segments to normalize away, and a non-empty path so a bare
    origin cannot claim every client on that host.
    """
    parsed = urllib.parse.urlsplit(client_id)

    if parsed.scheme != "https":
        if not (allow_insecure and parsed.scheme == "http"):
            raise CimdError("client_id must be an https URL")
    if not parsed.hostname:
        raise CimdError("client_id must include a host")
    if parsed.username or parsed.password:
        raise CimdError("client_id must not contain userinfo")
    if parsed.query or parsed.fragment:
        raise CimdError("client_id must not contain a query or fragment")
    if not parsed.path or parsed.path == "/":
        raise CimdError("client_id must have a path")
    if any(segment in (".", "..") for segment in parsed.path.split("/")):
        raise CimdError("client_id must not contain dot-segments")


async def fetch_client_metadata(
    client_id: str,
    http: httpx.AsyncClient,
    *,
    allow_insecure: bool = False,
) -> ClientMetadata:
    """Fetch and validate the client-metadata document at `client_id`.

    Performs the network IO and hands off to `parse_client_metadata` for the
    document rules, which are pure and therefore directly testable.
    """
    validate_client_id_url(client_id, allow_insecure=allow_insecure)
    if not allow_insecure:
        # In a thread, because getaddrinfo blocks: a slow resolver on an
        # attacker-chosen name must not pin the event loop for every request
        # this process is serving.
        await asyncio.to_thread(_assert_safe_target, client_id)

    try:
        # Streamed rather than fetched whole so the size cap can abort a hostile
        # endpoint *during* the response instead of after buffering all of it —
        # a cap applied to an already-materialized body defends nothing.
        #
        # `follow_redirects=False` is load-bearing, not a default: a redirect
        # would let a host that passed `_assert_safe_target` hand us an internal
        # URL, and would also break the draft's "client_id is the document URL"
        # identity, since the document served after a redirect describes a
        # different URL than the one the client claimed.
        async with http.stream(
            "GET",
            client_id,
            follow_redirects=False,
            timeout=FETCH_TIMEOUT_SECONDS,
            headers={"Accept": "application/json"},
        ) as response:
            if response.status_code != 200:
                raise CimdError(
                    f"client_id metadata document returned HTTP {response.status_code} "
                    "(expected 200)"
                )

            body = bytearray()
            async for chunk in response.aiter_bytes():
                body += chunk
                if len(body) > MAX_DOCUMENT_BYTES:
                    raise CimdError(
                        f"client_id metadata document exceeds {MAX_DOCUMENT_BYTES} bytes"
                    )
    except httpx.HTTPError as err:
        raise CimdError(f"could not fetch client_id metadata document: {err}") from err

    return parse_client_metadata(client_id, bytes(body))


def parse_client_metadata(client_id: str, body: bytes) -> ClientMetadata:
    """Validate a fetched document's contents against the draft's rules."""
    try:
        document = json.loads(body)
    except (ValueError, UnicodeDecodeError) as err:
        raise CimdError("client_id metadata document is not valid JSON") from err

    if not isinstance(document, dict):
        raise CimdError("client_id metadata document is not a JSON object")

    # The self-reference is the binding between the URL and the document: without
    # this check, a document served anywhere could claim to be any client.
    # Compared as exact strings, per the draft — no normalization, which is why
    # `validate_client_id_url` rejects the forms that would need normalizing.
    if document.get("client_id") != client_id:
        raise CimdError("client_id metadata document does not name the requested client_id")

    # A public client identified by a URL has, by construction, nowhere to keep a
    # secret. A document carrying one is either confused or trying to talk us into
    # a confidential-client flow we do not implement.
    if "client_secret" in document:
        raise CimdError("client_id metadata document must not contain a client_secret")

    client_name = document.get("client_name")
    if not isinstance(client_name, str) or not client_name.strip():
        raise CimdError("client_id metadata document must contain a client_name")

    redirect_uris = document.get("redirect_uris")
    if not isinstance(redirect_uris, list) or not redirect_uris:
        raise CimdError("client_id metadata document must contain a non-empty redirect_uris")
    if not all(isinstance(uri, str) for uri in redirect_uris):
        raise CimdError("client_id metadata document redirect_uris must be strings")

    return ClientMetadata(
        client_id=client_id,
        client_name=client_name.strip(),
        redirect_uris=list(redirect_uris),
        client_uri=_optional_str(document.get("client_uri")),
        logo_uri=_optional_str(document.get("logo_uri")),
    )


def assert_redirect_uri_allowed(requested: str, registered: list[str]) -> None:
    """Raise unless `requested` matches one of the client's `redirect_uris`.

    Note what this does *not* do: it does not replace the requested URI with the
    registered one it matched. The client is redirected to the URI it presented,
    because for a loopback client that is the only one carrying the ephemeral
    port its listener is actually bound to. Matching authorizes the request; it
    does not rewrite it.

    Exact string match, except for loopback URIs. RFC 8252 §7.3 requires the
    authorization server to ignore the *port* of a loopback redirect: a native
    client binds an ephemeral port at the moment it starts the flow, so it cannot
    have published the port in advance. The same section tells clients to prefer
    the literal `127.0.0.1`/`::1` over `localhost`, so the spelling of the host
    can legitimately differ between a document and a request; since every
    spelling denotes the user's own machine, treating them as equivalent grants
    no reach that the other spelling would not already have.

    Scheme, path, and query must still match exactly.
    """
    # RFC 6749 §3.1.2: a redirect URI must not carry a fragment. Rejected here
    # rather than ignored, because the response parameters are appended to this
    # string later — appended after a fragment, they would never reach the client.
    if urllib.parse.urlsplit(requested).fragment:
        raise CimdError("redirect_uri must not contain a fragment")

    if any(requested == candidate or _loopback_match(requested, candidate) for candidate in registered):
        return

    raise CimdError("redirect_uri does not match any redirect_uris in the client metadata document")


def _loopback_match(requested: str, candidate: str) -> bool:
    a, b = urllib.parse.urlsplit(requested), urllib.parse.urlsplit(candidate)
    if not (_is_loopback_host(a.hostname) and _is_loopback_host(b.hostname)):
        return False
    return a.scheme == b.scheme and a.path == b.path and a.query == b.query


def _is_loopback_host(host: str | None) -> bool:
    if host is None:
        return False
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _assert_safe_target(url: str) -> None:
    """Refuse to fetch anything that does not resolve to a public address.

    This is the SSRF guard. The adapter runs wherever the control plane's
    operator (or a self-hoster) puts it, so "inside the network perimeter" is the
    normal case, and `client_id` is chosen by whoever starts an authorization
    request — i.e. by anyone.

    Note the residual TOCTOU: we resolve here and httpx resolves again when it
    connects, so a DNS entry that flips between the two would slip past. Closing
    that requires pinning the resolved address into the connection, which is
    tracked as deferred work in the README rather than pretended away here.
    """
    parsed = urllib.parse.urlsplit(url)
    host = parsed.hostname
    if host is None:
        raise CimdError("client_id has no host to resolve")

    try:
        infos = socket.getaddrinfo(host, parsed.port or 443, proto=socket.IPPROTO_TCP)
    except socket.gaierror as err:
        raise CimdError(f"client_id host does not resolve: {host}") from err

    for info in infos:
        address = ipaddress.ip_address(info[4][0])
        if not address.is_global or address.is_multicast:
            raise CimdError(
                f"client_id host {host} resolves to a non-public address; refusing to fetch"
            )


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) and value.strip() else None
