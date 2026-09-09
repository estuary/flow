"""Wire models for `source-secrets`.

Only the parts of the capture protocol this connector actually uses. The
endpoint configuration is the interesting one: its four credential fields are
annotated `secret: true`, which is the annotation publication enforces -- once
a task carries a `secrets` stanza, none of those locations may hold a plaintext
value.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class Credentials(BaseModel):
    """OAuth credentials, every field of which is a secret.

    Two secrets land here, merged at `/credentials` as RFC 7396 patches with
    non-overlapping properties: the vendor's `oauth-client` supplies the client
    id and secret, and the task's own `oauth-tokens` the access and refresh
    tokens. Overlap would make application order matter, and it is unspecified.
    """

    model_config = ConfigDict(extra="forbid")

    client_id: str = Field(default="", json_schema_extra={"secret": True})
    client_secret: str = Field(default="", json_schema_extra={"secret": True})
    access_token: str = Field(default="", json_schema_extra={"secret": True})
    refresh_token: str = Field(default="", json_schema_extra={"secret": True})


class EndpointConfig(BaseModel):
    """Endpoint configuration; also the source of the spec's `configSchema`."""

    model_config = ConfigDict(extra="forbid")

    credentials: Credentials = Field(default_factory=Credentials)
    rotate_every: float = Field(
        default=60.0,
        gt=0,
        description="Seconds between credential rotations. Number-valued, and "
        "supplied by a sibling secret, so the catalog exercises a numeric "
        "secret alongside the object-valued credential ones.",
    )
    emit_every: float = Field(
        default=5.0,
        gt=0,
        description="Seconds between emitted documents.",
    )


class ResourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(description="Resource name; also the discovered collection name.")


# --- Request messages (runtime -> connector) ---------------------------------


class DiscoverRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    config: EndpointConfig = Field(default_factory=EndpointConfig)


class ValidateBinding(BaseModel):
    model_config = ConfigDict(extra="ignore")
    resourceConfig: ResourceConfig


class ValidateRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    bindings: list[ValidateBinding] = []


class CaptureSpec(BaseModel):
    """The built spec, as delivered on `Open`.

    `secrets` is the published stanza, and its emptiness is what tells this
    connector it is still running a legacy sops configuration which it should
    migrate. It is a map of secret catalog name to the JSON pointer of the
    endpoint configuration where the secret's value is merged.
    """

    model_config = ConfigDict(extra="ignore")

    name: str
    config: EndpointConfig = Field(default_factory=EndpointConfig)
    secrets: dict[str, str] = Field(default_factory=dict)
    bindings: list[dict[str, Any]] = []  # Only the count is used.


class OpenRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    capture: CaptureSpec


class Request(BaseModel):
    """Envelope holding exactly one populated request variant.

    `validate` is reserved on Pydantic models, so we alias it as `estuary-cdk` does.
    """

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    spec: dict[str, Any] | None = None
    discover: DiscoverRequest | None = None
    validate_: ValidateRequest | None = Field(default=None, alias="validate")
    apply: dict[str, Any] | None = None
    open: OpenRequest | None = None
    acknowledge: dict[str, Any] | None = None


# --- Response messages (connector -> runtime) --------------------------------


class Spec(BaseModel):
    protocol: int = 3032023  # The capture protocol version; must be 3032023.
    configSchema: dict[str, Any]
    resourceConfigSchema: dict[str, Any]
    documentationUrl: str
    resourcePathPointers: list[str] = []


class DiscoveredBinding(BaseModel):
    recommendedName: str
    resourceConfig: ResourceConfig
    documentSchema: dict[str, Any]
    key: list[str]


class Discovered(BaseModel):
    bindings: list[DiscoveredBinding]


class ValidatedBinding(BaseModel):
    resourcePath: list[str]


class Validated(BaseModel):
    bindings: list[ValidatedBinding]


class Applied(BaseModel):
    actionDescription: str = ""


class Opened(BaseModel):
    explicitAcknowledgements: bool = False


class Captured(BaseModel):
    binding: int
    doc: Any


class ConnectorState(BaseModel):
    updated: Any
    mergePatch: bool = False


class Checkpoint(BaseModel):
    state: ConnectorState


class Response(BaseModel):
    """Envelope holding exactly one populated response variant."""

    spec: Spec | None = None
    discovered: Discovered | None = None
    validated: Validated | None = None
    applied: Applied | None = None
    opened: Opened | None = None
    captured: Captured | None = None
    checkpoint: Checkpoint | None = None
