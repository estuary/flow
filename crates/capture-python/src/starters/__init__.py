"""A capture written with the Estuary CDK. This starter is a working capture
of greetings, which needs no configuration.

Declare the capture's endpoint configuration as a JSON schema, its
`spec.configSchema`, and the generated `EndpointConfig` has its fields:

    spec:
      configSchema:
        type: object
        properties:
          greeting: { type: string, default: Hello }

A connector then reads `discover.config.greeting`, `validate.config.greeting`,
or `open.capture.config.greeting`. Mark sensitive locations of the schema with
`secret: true`, and supply them through the capture's `secrets` stanza.
"""

from collections.abc import AsyncGenerator, Awaitable
from datetime import timedelta
from logging import Logger
from typing import Any, Callable

from estuary_cdk.capture import (
    BaseCaptureConnector,
    Request,
    Task,
    common,
    request,
    response,
)
from estuary_cdk.flow import CaptureBinding, ConnectorSpec

from GENERATED_MODULE import SPEC, EndpointConfig, ResourceConfig

GREETING = "Hello"
COUNT = 10

ConnectorState = common.ConnectorState[common.ResourceState]


class Greeting(common.BaseDocument, extra="forbid"):
    """A captured document. Its schema is published through discovery."""

    id: int
    message: str


async def fetch_greetings(
    log: Logger,
    log_cursor: common.LogCursor,
) -> AsyncGenerator[Greeting | common.LogCursor, None]:
    """Fetch greetings which follow `log_cursor`, and then checkpoint them.

    A real connector would request changes from its endpoint here."""
    assert isinstance(log_cursor, int)

    if log_cursor >= COUNT:
        return  # No new greetings are available.

    for id in range(log_cursor + 1, COUNT + 1):
        yield Greeting(id=id, message=f"{GREETING} #{id}!")

    yield COUNT


def all_resources() -> list[common.Resource[Any, ResourceConfig, common.ResourceState]]:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: common.ResourceState,
        task: Task,
        all_bindings: Any,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=fetch_greetings,
        )

    return [
        common.Resource(
            name="greetings",
            key=["/id"],
            model=Greeting,
            open=open,
            initial_state=common.ResourceState(
                inc=common.ResourceState.Incremental(cursor=0)
            ),
            initial_config=ResourceConfig(
                name="greetings", interval=timedelta(seconds=30)
            ),
            schema_inference=True,
        )
    ]


class Connector(
    BaseCaptureConnector[EndpointConfig, ResourceConfig, ConnectorState],
):
    def request_class(self):
        return Request[EndpointConfig, ResourceConfig, ConnectorState]

    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        # Specs are answered from the capture's declared `spec`, which SPEC
        # mirrors, so that this connector is complete if it's run on its own.
        return SPEC

    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[ResourceConfig]:
        return common.discovered(all_resources())

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
    ) -> response.Validated:
        resolved = common.resolve_bindings(validate.bindings, all_resources())
        return common.validated(resolved)

    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        resolved = common.resolve_bindings(open.capture.bindings, all_resources())
        return common.open(open, resolved)
