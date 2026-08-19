"""Settings parsing: the checks that make a misconfigured adapter refuse to boot."""

import base64

import pytest

from estuary_mcp import config

REQUIRED = {
    "ESTUARY_MCP_PUBLIC_URL": "http://localhost:12022",
    "ESTUARY_MCP_AGENT_URL": "http://localhost:12020",
    "ESTUARY_MCP_DASHBOARD_URL": "http://localhost:3000",
}


def _env(monkeypatch, **overrides):
    for name, value in {**REQUIRED, **overrides}.items():
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)


def test_a_public_url_with_a_path_refuses_to_boot(monkeypatch):
    """The issuer must be a bare origin: RFC 8414 moves the well-known location
    when the issuer has a path, and this adapter serves metadata only at the root
    — a path here would advertise documents that are never served."""
    _env(monkeypatch, ESTUARY_MCP_PUBLIC_URL="http://localhost:12022/mcp-adapter")
    with pytest.raises(config.ConfigError, match="no path"):
        config.from_env()


def test_a_dashboard_url_with_a_path_is_fine(monkeypatch):
    _env(monkeypatch, ESTUARY_MCP_DASHBOARD_URL="http://localhost:3000/app/")
    settings = config.from_env()
    assert settings.dashboard_url == "http://localhost:3000/app"
    assert settings.dashboard_consent_url == "http://localhost:3000/app/mcp-auth"


def test_unset_sealing_keys_yield_one_ephemeral_key(monkeypatch):
    _env(monkeypatch, ESTUARY_MCP_SEALING_KEYS=None)
    settings = config.from_env()
    assert len(settings.sealing_keys) == 1
    assert len(settings.sealing_keys[0]) == 32
    # Ephemeral means per-boot: a second read must not repeat the key.
    assert config.from_env().sealing_keys != settings.sealing_keys


def test_configured_sealing_keys_are_parsed_first_encrypts(monkeypatch):
    first, second = b"a" * 32, b"b" * 32
    _env(
        monkeypatch,
        ESTUARY_MCP_SEALING_KEYS=", ".join(
            base64.b64encode(key).decode() for key in (first, second)
        ),
    )
    assert config.from_env().sealing_keys == (first, second)


@pytest.mark.parametrize("bad", ["not-base64!", base64.b64encode(b"short").decode()])
def test_malformed_sealing_keys_refuse_to_boot(monkeypatch, bad):
    _env(monkeypatch, ESTUARY_MCP_SEALING_KEYS=bad)
    with pytest.raises(config.ConfigError):
        config.from_env()
