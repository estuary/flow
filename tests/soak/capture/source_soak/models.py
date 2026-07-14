"""Wire models for `source-soak`"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field


class EndpointConfig(BaseModel):
    """Endpoint configuration; also the source of the spec's `configSchema`."""

    model_config = ConfigDict(extra="forbid")

    rate: float = Field(
        default=20.0,
        description="Target documents emitted per second, summed across all bindings.",
    )
    docsPerCheckpoint: int = Field(
        default=10,
        ge=1,
        description="Number of documents emitted between connector checkpoints.",
    )
    idRange: int = Field(
        default=10000,
        ge=1,
        description="Width of the account-id window above key_begin. Each shard owns "
        "[key_begin, key_begin + idRange); subdividing the capture yields disjoint windows.",
    )
    collections: list[str] = Field(
        default_factory=lambda: ["events"],
        description="Resource names, one per discovered binding / target collection. "
        "Editing this list and (auto-)discovering scales the capture across collections.",
    )


class ResourceConfig(BaseModel):
    """Per-binding resource configuration; the spec's `resourceConfigSchema`."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(description="Resource name; also the discovered collection name.")


class CaptureState(BaseModel):
    """Connector state as delivered on `Open`: the fully-reduced per-account
    seq / mask / balance, keyed by stringified id.

    Checkpoints WRITE a merge patch instead — partial (only touched ids), with
    `null` to delete a pruned id — so the write side is freeform `ConnectorState`,
    not this model. The asymmetry is intrinsic to RFC 7396 merge patches."""

    model_config = ConfigDict(extra="ignore")

    seq: dict[str, int] = Field(default_factory=dict)
    mask: dict[str, int] = Field(default_factory=dict)
    balance: dict[str, int] = Field(default_factory=dict)


# --- Request messages (runtime -> connector) ---------------------------------
#
# Spec / Apply carry no fields we read, so the Request envelope types them as
# bare dicts: presence is all that matters.


class RangeSpec(BaseModel):
    keyBegin: int = 0  # Omitted when zero.
    keyEnd: int


class AcknowledgeRequest(BaseModel):
    """Request.Acknowledge: the runtime reports that `checkpoints` of our previously
    emitted Response.Checkpoints have committed to the recovery log. Sent only because
    we set Opened.explicitAcknowledgements; the count is always >= 1, and exceeds one
    when the runtime combined several of our checkpoints into a single transaction."""

    model_config = ConfigDict(extra="ignore")
    checkpoints: int = 0


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
    model_config = ConfigDict(extra="ignore")
    name: str
    config: EndpointConfig = Field(default_factory=EndpointConfig)
    bindings: list[dict[str, Any]] = []  # Only the count is used (routing index range).


class OpenRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    capture: CaptureSpec
    range: RangeSpec = Field(default_factory=RangeSpec)
    state: CaptureState = Field(default_factory=CaptureState)


class Request(BaseModel):
    """Envelope holding exactly one populated request variant.

    `validate` is reserved on Pydantic models, so we alias it as `estuary-cdk` does."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    spec: dict[str, Any] | None = None
    discover: DiscoverRequest | None = None
    validate_: ValidateRequest | None = Field(default=None, alias="validate")
    apply: dict[str, Any] | None = None
    open: OpenRequest | None = None
    acknowledge: AcknowledgeRequest | None = None


# --- Response messages (connector -> runtime) --------------------------------


class ConnectorState(BaseModel):
    """flow.ConnectorState. `updated` carries the raw state JSON; when `mergePatch`
    is set it's applied as an RFC 7396 merge patch over prior state."""

    updated: Any
    mergePatch: bool = False


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
    doc: Any  # The event document; events.schema.json is its single source of truth.


class SourcedSchema(BaseModel):
    """flow.capture.Response.SourcedSchema: a partial, connector-authoritative
    document schema for a binding. The runtime intersects it with the write schema
    and unions it into the binding's running inferred shape (README "Schema
    inference"). `schema_json` serializes as `documentSchema`, an inline JSON schema
    object. It is transactional — taking effect at the next Checkpoint — so we emit
    it before the first captured docs of a session."""

    binding: int
    documentSchema: dict[str, Any]


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
    sourcedSchema: SourcedSchema | None = None
    checkpoint: Checkpoint | None = None
