"""Wire models for `materialize-soak`"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field


class EndpointConfig(BaseModel):
    """Endpoint configuration; also the source of the spec's `configSchema`."""

    model_config = ConfigDict(extra="forbid")

    forceLoads: bool = Field(
        default=False,
        description="Return Opened.disableLoadOptimization, forcing the runtime to "
        "issue a Load for every key rather than suppressing Loads via max-keys. "
        "Default false exercises the V2 max-keys load-suppression path; true "
        "exercises the stricter loaded-keys-equal-stored-keys probe.",
    )


class ResourceConfig(BaseModel):
    """Per-binding resource configuration; the spec's `resourceConfigSchema`."""

    model_config = ConfigDict(extra="forbid")

    table: str = Field(
        description="Resource name identifying the materialized table.",
        json_schema_extra={"x-collection-name": True},
    )
    delta: bool = Field(
        default=False,
        description="Materialize combined delta updates rather than full reductions "
        "(no loads). The standard binding sets false; the delta binding sets true.",
        json_schema_extra={"x-delta-updates": True},
    )


# --- Request messages (runtime -> connector) ---------------------------------
#
# Spec / Apply carry no fields we read, so the envelope types them as bare dicts:
# presence is all that matters.


class RangeSpec(BaseModel):
    model_config = ConfigDict(extra="ignore")
    keyBegin: int = 0  # Omitted when zero (shard 0 / single-shard).
    keyEnd: int = 0


class Projection(BaseModel):
    model_config = ConfigDict(extra="ignore")
    ptr: str = ""  # JSON pointer; "" is the root document.
    field: str
    isPrimaryKey: bool = False


class CollectionSpec(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str = ""
    key: list[str] = []
    projections: list[Projection] = []


class MaterializationBinding(BaseModel):
    model_config = ConfigDict(extra="ignore")
    deltaUpdates: bool = False
    resourcePath: list[str] = []


class MaterializationSpec(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str = ""
    config: EndpointConfig = Field(default_factory=EndpointConfig)
    bindings: list[MaterializationBinding] = []


class ValidateBinding(BaseModel):
    model_config = ConfigDict(extra="ignore")
    resourceConfig: ResourceConfig
    collection: CollectionSpec


class ValidateRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str = ""
    bindings: list[ValidateBinding] = []


class OpenRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    materialization: MaterializationSpec
    range: RangeSpec = Field(default_factory=RangeSpec)
    state: Any = None  # Prior fully-reduced connector state, or null/absent.


class Load(BaseModel):
    model_config = ConfigDict(extra="ignore")
    binding: int = 0  # Omitted by the runtime when binding index is 0 (proto default).
    key: list[Any] = []


class Flush(BaseModel):
    model_config = ConfigDict(extra="ignore")
    # Gathered prior-transaction Acknowledged patches. Unused by this connector.
    statePatches: Any = None


class Store(BaseModel):
    model_config = ConfigDict(extra="ignore")
    binding: int = 0  # Omitted by the runtime when binding index is 0 (proto default).
    key: list[Any] = []
    doc: Any = None
    exists: bool = False
    delete: bool = False


class StartCommit(BaseModel):
    model_config = ConfigDict(extra="ignore")
    runtimeCheckpoint: Any = (
        None  # Ignored: this connector is recovery-log-authoritative.
    )
    # Gathered current-transaction Flushed patches. Unused by this connector.
    statePatches: Any = None


class Acknowledge(BaseModel):
    model_config = ConfigDict(extra="ignore")
    # Gathered current-transaction StartedCommit patches: the conservation gather.
    statePatches: Any = None


class Request(BaseModel):
    """Envelope holding exactly one populated request variant.

    `validate` is reserved on Pydantic models, so we alias it as `estuary-cdk` does."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    spec: dict[str, Any] | None = None
    validate_: ValidateRequest | None = Field(default=None, alias="validate")
    apply: dict[str, Any] | None = None
    open: OpenRequest | None = None
    load: Load | None = None
    flush: Flush | None = None
    store: Store | None = None
    startCommit: StartCommit | None = None
    acknowledge: Acknowledge | None = None


# --- Response messages (connector -> runtime) --------------------------------


class ConnectorState(BaseModel):
    """flow.ConnectorState. `updated` carries the raw state JSON; when `mergePatch`
    is set it's applied as an RFC 7396 merge patch over prior state. runtime-next
    frames each shard's `updated` into the tab-delimited `statePatches` array fed
    back to peers (`crates/runtime-next/src/patches.rs`)."""

    updated: Any
    mergePatch: bool = False


class Spec(BaseModel):
    protocol: int = 3032023  # The materialize protocol version; must be 3032023.
    configSchema: dict[str, Any]
    resourceConfigSchema: dict[str, Any]
    documentationUrl: str


class Constraint(BaseModel):
    type: str  # Enum string name: LOCATION_REQUIRED / FIELD_REQUIRED / FIELD_OPTIONAL / FIELD_FORBIDDEN.
    reason: str = ""


class ValidatedBinding(BaseModel):
    constraints: dict[str, Constraint]
    resourcePath: list[str]
    deltaUpdates: bool = False


class Validated(BaseModel):
    bindings: list[ValidatedBinding]


class Applied(BaseModel):
    actionDescription: str = ""


class Opened(BaseModel):
    # runtimeCheckpoint is intentionally omitted: this connector is
    # recovery-log-authoritative, so the runtime resumes from its own checkpoint.
    disableLoadOptimization: bool = False


class Loaded(BaseModel):
    binding: int
    doc: Any


class Flushed(BaseModel):
    state: ConnectorState | None = None


class StartedCommit(BaseModel):
    state: ConnectorState | None = None


class Acknowledged(BaseModel):
    state: ConnectorState | None = None


class Response(BaseModel):
    """Envelope holding exactly one populated response variant."""

    spec: Spec | None = None
    validated: Validated | None = None
    applied: Applied | None = None
    opened: Opened | None = None
    loaded: Loaded | None = None
    flushed: Flushed | None = None
    startedCommit: StartedCommit | None = None
    acknowledged: Acknowledged | None = None
