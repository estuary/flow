import functools
from datetime import timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.flow import CaptureBinding
from estuary_cdk.http import HTTPMixin
from examples.source_hello import EndpointConfig, ResourceConfig

from .api import fetch_greetings
from .models import Greeting, ResourceState


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_greetings, config),
        )

    return [
        common.Resource(
            name="greetings",
            key=["/id"],
            model=Greeting,
            open=open,
            initial_state=ResourceState(inc=ResourceState.Incremental(cursor=0)),
            initial_config=ResourceConfig(
                name="greetings", interval=timedelta(seconds=30)
            ),
            schema_inference=True,
        )
    ]
