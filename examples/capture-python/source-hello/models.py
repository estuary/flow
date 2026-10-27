# `EndpointConfig` and `ResourceConfig` are generated from the capture's
# declared `spec.configSchema` and `spec.resourceConfigSchema`. Mark sensitive
# locations of `spec.configSchema` with `secret: true`, and supply them through
# the capture's `secrets` stanza.
from estuary_cdk.capture.common import (
    BaseDocument,
    ConnectorState as GenericConnectorState,
    ResourceState,
)


ConnectorState = GenericConnectorState[ResourceState]


class Greeting(BaseDocument, extra="forbid"):
    """A captured document. Its schema is published through discovery."""

    id: int
    message: str
