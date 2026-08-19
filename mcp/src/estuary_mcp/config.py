"""Process configuration, read once from the environment at startup.

Every URL the adapter uses is configuration rather than a derived constant.
That is what keeps "run it in a VM behind an SSH port-forward", "run it behind
an HTTPS tunnel for Claude Desktop", and "run it as mcp.estuary.dev" the same
binary with a different environment file.

Note the two distinct notions of "where am I":

  * `public_url` is how the *outside world* reaches this process — the OAuth
    issuer, the RFC 9728 resource base, and the origin every redirect the
    browser follows is built from. Under a port-forward this is the host's
    view, which need not equal the bind address.
  * `bind_host` / `bind_port` are merely where the socket lives.

Getting `public_url` wrong is the single most common misconfiguration: OAuth
metadata is compared by exact string, so a trailing slash or a wrong port
surfaces as an opaque client-side "issuer mismatch".
"""

import base64
import dataclasses
import os
import urllib.parse


class ConfigError(Exception):
    """A required environment variable is missing or malformed."""


@dataclasses.dataclass(frozen=True)
class Settings:
    # How the outside world reaches this process; origin of every issued URL.
    public_url: str
    # Base URL of the control-plane agent (e.g. http://localhost:12020).
    agent_url: str
    # Base URL of the Estuary dashboard, which owns login and consent.
    dashboard_url: str

    bind_host: str
    bind_port: int

    # ISO-8601 validity of the client-dedicated refresh token minted at the end
    # of the authorization dance.
    client_token_validity: str

    # Keys sealing in-flight authorization state (see auth/sealed.py). The first
    # encrypts; every key may decrypt, which is the rotation story. Empty means
    # "generate an ephemeral per-boot key": correct for a single replica, where
    # the only cost of a restart is a re-click on dances in flight.
    sealing_keys: tuple[bytes, ...]

    # Test-only escape hatch: permit `http://` and loopback CIMD client_id URLs,
    # which the CIMD draft forbids. Integration tests serve their client-metadata
    # document from an ephemeral local HTTP server, and there is no way to satisfy
    # the https rule without terminating TLS in the test. NEVER enable in
    # production; it turns the CIMD fetch into an SSRF primitive.
    allow_insecure_cimd: bool

    log_level: str

    @property
    def issuer(self) -> str:
        """RFC 8414 issuer identifier. The adapter is its own AS facade, so the
        issuer is its origin — advertised to clients and compared by exact string."""
        return self.public_url

    @property
    def resource_url(self) -> str:
        """RFC 9728 resource identifier: the MCP endpoint itself, per the MCP spec's
        rule that the resource identifier is the MCP server URL."""
        return f"{self.public_url}/mcp"

    @property
    def authorization_endpoint(self) -> str:
        return f"{self.public_url}/oauth/authorize"

    @property
    def token_endpoint(self) -> str:
        return f"{self.public_url}/oauth/token"

    @property
    def dashboard_consent_url(self) -> str:
        """The dashboard route that owns login + consent. See README
        "Dashboard handoff contract" for the query parameters."""
        return f"{self.dashboard_url}/mcp-auth"


def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(f"required environment variable {name} is unset or empty")
    return value


def _normalize_origin(name: str, value: str, *, allow_path: bool = True) -> str:
    """Reduce a configured base URL to a canonical `scheme://host[:port][/path]`
    with no trailing slash, so string-compared OAuth metadata is stable."""
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme not in ("http", "https"):
        raise ConfigError(f"{name} must be an http(s) URL, got {value!r}")
    if not parsed.netloc:
        raise ConfigError(f"{name} must include a host, got {value!r}")
    if parsed.query or parsed.fragment:
        raise ConfigError(f"{name} must not carry a query or fragment, got {value!r}")
    if not allow_path and parsed.path.rstrip("/"):
        # RFC 8414 moves the well-known metadata location when the issuer has a
        # path component, and this adapter serves metadata only at the root —
        # so a path here would advertise documents that are never served.
        raise ConfigError(f"{name} must be an origin with no path, got {value!r}")
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))


def _sealing_keys(name: str) -> tuple[bytes, ...]:
    """Parse a comma-separated list of base64 sealing keys, first-encrypts.

    An empty/unset variable yields an ephemeral per-boot key. That default keeps
    a single-replica deployment (and every local stack) zero-config; a restart
    only invalidates dances in flight, costing a re-click. Replicas MUST share
    configured keys — with per-boot keys, a dance that hops replicas dies.
    """
    raw = os.environ.get(name, "").strip()
    if not raw:
        return (os.urandom(32),)

    keys = []
    for index, encoded in enumerate(raw.split(",")):
        try:
            key = base64.b64decode(encoded.strip(), validate=True)
        except (ValueError, TypeError) as err:
            raise ConfigError(f"{name}[{index}] is not valid base64") from err
        if len(key) != 32:
            raise ConfigError(f"{name}[{index}] must decode to 32 bytes, got {len(key)}")
        keys.append(key)
    return tuple(keys)


def _flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def from_env() -> Settings:
    """Build Settings from the process environment, failing loudly and early.

    Called once at startup: a misconfigured adapter should refuse to boot rather
    than mint an authorization code that redirects a browser somewhere wrong.
    """
    settings = Settings(
        # The public URL must be a bare origin: it is the RFC 8414 issuer, and the
        # well-known metadata routes are served only at the root.
        public_url=_normalize_origin(
            "ESTUARY_MCP_PUBLIC_URL", _require("ESTUARY_MCP_PUBLIC_URL"), allow_path=False
        ),
        agent_url=_normalize_origin("ESTUARY_MCP_AGENT_URL", _require("ESTUARY_MCP_AGENT_URL")),
        dashboard_url=_normalize_origin(
            "ESTUARY_MCP_DASHBOARD_URL", _require("ESTUARY_MCP_DASHBOARD_URL")
        ),
        bind_host=os.environ.get("ESTUARY_MCP_BIND_HOST", "127.0.0.1"),
        bind_port=int(os.environ.get("ESTUARY_MCP_BIND_PORT", "8080")),
        client_token_validity=os.environ.get("ESTUARY_MCP_CLIENT_TOKEN_VALIDITY", "P90D"),
        sealing_keys=_sealing_keys("ESTUARY_MCP_SEALING_KEYS"),
        allow_insecure_cimd=_flag("ESTUARY_MCP_ALLOW_INSECURE_CIMD"),
        log_level=os.environ.get("ESTUARY_MCP_LOG_LEVEL", "INFO").upper(),
    )

    if settings.allow_insecure_cimd:
        # Loud, because this is a test-only relaxation of a security rule and a
        # production process that somehow has it set must be findable in a log.
        import logging

        logging.getLogger(__name__).warning(
            "ESTUARY_MCP_ALLOW_INSECURE_CIMD is enabled: http:// and loopback "
            "client_id metadata URLs will be accepted. This is a TEST-ONLY setting."
        )
    return settings
