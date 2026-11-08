"""Stateful derivation demonstrating persistent state management.

This example shows how to:
1. Load persisted state on open
2. Maintain in-memory state during transaction
3. Persist partial state updates via `flushed.state` in flush()
4. Handle state recovery across task restarts

In addition to catalog tests, be sure to test your stateful derivation using
`flowctl preview` with multiple `--sessions` to exercise end-to-end restarts
of your derivation.
"""

import typing
from collections.abc import AsyncIterator
from estuary_cdk.flow import ConnectorStateUpdate
from patterns.stateful import EndpointConfig, IDerivation, Document, Request, ResourceConfig, Response
from pydantic import BaseModel, Field


class State(BaseModel):
    """Pydantic model for persisted state"""

    class KeyState(BaseModel):
        count: int
        sum: int

    keys: dict[str, KeyState] = Field(default_factory=dict)


class Derivation(IDerivation[EndpointConfig, ResourceConfig, State]):
    """Derivation that maintains persistent state across transactions.

    It declares `State` as its connector state type, so the runtime's
    `open.state` is parsed as a `State`.
    """

    def __init__(self, state: State):
        self.state = state

        # Keys which were touched in the current transaction.
        # This is used to reduce the size of the state we persist in each transaction,
        # which improves performance if state is large.
        self.touched: dict[str, State.KeyState] = {}

    @classmethod
    async def open(
        cls, open: Request.Open[EndpointConfig, ResourceConfig, State]
    ) -> tuple[typing.Self, Response.Opened]:
        """Load persisted state from previous transactions.

        This allows the derivation to recover its state after restarts or failures.
        `open.state` is an empty `State` on the first run.
        """
        return cls(open.state), Response.Opened()

    async def transform_from_ints(self, read: Request.ReadFromInts) -> AsyncIterator[Document]:
        """Update in-memory state and emit current counts.

        State updates happen in-memory during the transaction.
        They are persisted to durable storage in flush().
        """

        state = self.state.keys.setdefault(read.doc.Key, State.KeyState(count=0, sum=0))
        state.count += 1
        state.sum += read.doc.Int

        yield Document(Key=read.doc.Key, Count=state.count, Sum=state.sum)

        # Mark this key as "touched" during this transaction.
        self.touched[read.doc.Key] = state

    async def flush(self, flush: Request.Flush, flushed: Response.Flushed[State]) -> AsyncIterator[Document]:
        """Persist state updates for recovery after failures.

        The runtime calls this at the end of each transaction. `flushed.state`
        is durably persisted with the transaction, and is passed back via
        `open.state` on the next Open (e.g., after a restart).

        `mergePatch=True` means the state we emit is merged with the prior
        state, as a JSON merge patch. If it were False, the prior state would
        be completely replaced.
        """
        flushed.state = ConnectorStateUpdate[State](updated=State(keys=self.touched), mergePatch=True)
        self.touched = {}

        if False:
            yield  # Mark as a generator.

    async def reset(self):
        """Reset state for catalog tests.
        The runtime will call this in between tests.
        """
        self.state = State()
        self.touched = {}
