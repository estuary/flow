"""Captures numbered events, keeping its cursor in SQLite on its disk as well as
in its state.

Each run of the connector saves one cursor on its disk, checkpoints it, and
stops. It stops because a capture's disk can otherwise run ahead of its
checkpoint. The runtime commits that checkpoint before it ends the session, and
the disk commits with it. So at open, the disk's cursor is the committed cursor,
unless the disk lost or repeated a transaction.
"""

import os
import sqlite3
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
from estuary_cdk.capture.common import (
    BaseDocument,
    ConnectorState as GenericConnectorState,
    LogCursor,
    ResourceState,
)
from estuary_cdk.flow import CaptureBinding, ConnectorSpec

from acmeCo.disks.source import SPEC, EndpointConfig, ResourceConfig

ConnectorState = GenericConnectorState[ResourceState]


class Event(BaseDocument, extra="forbid"):
    seq: int


class Connector(
    BaseCaptureConnector[EndpointConfig, ResourceConfig, ConnectorState],
):
    def request_class(self):
        return Request[EndpointConfig, ResourceConfig, ConnectorState]

    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        return SPEC

    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[ResourceConfig]:
        return common.discovered(resources())

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
    ) -> response.Validated:
        return common.validated(common.resolve_bindings(validate.bindings, resources()))

    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        return common.open(open, common.resolve_bindings(open.capture.bindings, resources()))


def resources() -> list[common.Resource[Any, Any, Any]]:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings: Any,
    ):
        async def fetch_events(
            log: Logger, cursor: LogCursor
        ) -> AsyncGenerator[Event | LogCursor, None]:
            assert isinstance(cursor, int)

            db = sqlite3.connect(
                os.path.join(os.environ["CONNECTOR_MOUNT"], "persist", "cursor.db")
            )
            db.execute("PRAGMA synchronous = OFF")
            # `user_version` is an integer in the database header, kept for applications.
            on_disk: int = db.execute("PRAGMA user_version").fetchone()[0]

            if on_disk != cursor:
                raise RuntimeError(
                    f"the disk's cursor {on_disk} isn't the committed cursor {cursor}: the disk lost or repeated a transaction"
                )

            db.execute(f"PRAGMA user_version = {cursor + 1}")
            db.close()

            yield Event(seq=cursor + 1)
            yield cursor + 1
            # The CDK's incremental loop returns once `stopping` is set, so this
            # run writes its disk no more.
            task.stopping.event.set()

        common.open_binding(binding, binding_index, state, task, fetch_changes=fetch_events)

    return [
        common.Resource(
            name="events",
            key=["/seq"],
            model=Event,
            open=open,
            initial_state=ResourceState(inc=ResourceState.Incremental(cursor=0)),
            initial_config=ResourceConfig(name="events", interval=timedelta()),
            schema_inference=False,
        )
    ]
