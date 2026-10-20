"""Stateful derivation demonstrating persistent state management.

This example shows how to:
1. Load persisted state on initialization
2. Maintain in-memory state during transaction
3. Persist partial state updates via `flushed.state` of flush()
4. Handle state recovery across task restarts

In addition to catalog tests, be sure to test your stateful derivation using
`flowctl preview` with multiple `--sessions` to exercise end-to-end restarts
of your derivation.
"""

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

    It declares `State` as the type of its connector state, which is parsed
    from `open.state`. This derivation declares no `spec.configSchema`, so its
    `config` is an empty EndpointConfig.
    """

    def __init__(self, open: Request.Open[EndpointConfig, ResourceConfig, State]):
        """Initialize and load persisted state.

        The runtime passes previous state via open.state. This allows
        the derivation to recover its state after restarts or failures.
        """
        super().__init__(open)

        # Load persisted state from previous transaction.
        # `open.state` is an empty State on the first run.
        self.state = open.state

        # Keys which were touched in the current transaction.
        # This is used to reduce the size of the state we persist in each transaction,
        # which improves performance if state is large.
        self.touched = State()

    async def from_ints(self, read: Request.ReadFromInts) -> AsyncIterator[Document]:
        """Update in-memory state and emit current counts.

        State updates happen in-memory during the transaction.
        They are persisted to durable storage in flush().
        """

        state = self.state.keys.setdefault(read.doc.Key, State.KeyState(count=0, sum=0))
        state.count += 1
        state.sum += read.doc.Int

        yield Document(Key=read.doc.Key, Count=state.count, Sum=state.sum)

        # Mark this key as "touched" during this transaction.
        self.touched.keys[read.doc.Key] = state

    async def flush(
        self, flush: Request.Flush, flushed: Response.Flushed[State]
    ) -> AsyncIterator[Document]:
        """Persist state updates for recovery after failures.

        The runtime calls this at the end of each transaction. The state set
        on `flushed` is durably persisted and will be passed back via
        open.state on the next Open message (e.g., after a restart).

        Setting mergePatch=True means the state we emit is merged with the
        prior state, as a JSON merge patch. If it were False, the prior state
        would be completely replaced.
        """
        flushed.state = ConnectorStateUpdate(updated=self.touched, mergePatch=True)
        self.touched = State()

        if False:
            yield  # Mark as a generator.

    async def reset(self):
        """Reset state for catalog tests.
        The runtime will call this in between tests.
        """
        self.state = State()
        self.touched = State()
