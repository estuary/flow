"""Numbers each greeting with a running count. The count is the derivation's
connector state, which each Flush updates and each Open restores, so a
session which restarts continues it rather than beginning again."""

import typing
from collections.abc import AsyncIterator

from estuary_cdk.flow import ConnectorStateUpdate

from acmeCo.vmm_python.counted import (
    Document,
    EndpointConfig,
    IDerivation,
    Request,
    ResourceConfig,
    Response,
)


class Derivation(IDerivation):
    def __init__(self, count: int):
        self.count = count

    @classmethod
    async def open(
        cls, open: Request.Open[EndpointConfig, ResourceConfig, dict[str, typing.Any]]
    ) -> tuple[typing.Self, Response.Opened]:
        return cls(int(open.state.get("count", 0))), Response.Opened()

    async def transform_from_greetings(
        self, read: Request.ReadFromGreetings
    ) -> AsyncIterator[Document]:
        self.count += 1
        yield Document(ts=read.doc.ts, count=self.count)

    async def flush(
        self, flush: Request.Flush, flushed: Response.Flushed[dict[str, typing.Any]]
    ) -> AsyncIterator[Document]:
        flushed.state = ConnectorStateUpdate[dict[str, typing.Any]](
            updated={"count": self.count}, mergePatch=True
        )
        if False:
            yield
