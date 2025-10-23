"""Stateful derivation demonstrating persistent state management.

This example shows how to:
1. Load persisted state on initialization
2. Maintain in-memory state during transaction
3. Persist partial state updates via start_commit()
4. Handle state recovery across task restarts

In addition to catalog tests, be sure to test your stateful derivation using
`flowctl preview` with multiple `--sessions` to exercise end-to-end restarts
of your derivation.
"""

from collections.abc import AsyncIterator
from patterns.stateful import IDerivation, Document, Request, Response
from pydantic import BaseModel, Field


class State(BaseModel):
    """Pydantic model for persisted state"""

    class KeyState(BaseModel):
        count: int
        sum: int

    keys: dict[str, KeyState] = Field(default_factory=dict)


class Derivation(IDerivation):
    """Derivation that maintains persistent state across transactions."""

    def __init__(self, open: Request.Open):
        """Initialize and load persisted state.

        The runtime passes previous state via open.state. This allows
        the derivation to recover its state after restarts or failures.
        """
        super().__init__(open)

        # Load persisted state from previous transaction.
        # `open.state` is an empty dict on the first run.
        self.state = State(**open.state)

        # Keys which were touched in the current transaction.
        # This is used to reduce the size of the state we persist in each transaction,
        # which improves performance if state is large.
        self.touched = State()

    async def from_ints(self, read: Request.ReadFromInts) -> AsyncIterator[Document]:
        """Update in-memory state and emit current counts.

        State updates happen in-memory during the transaction.
        They are persisted to durable storage in start_commit().
        """

        state = self.state.keys.setdefault(read.doc.Key, State.KeyState(count=0, sum=0))
        state.count += 1
        state.sum += read.doc.Int

        yield Document(Key=read.doc.Key, Count=state.count, Sum=state.sum)

        # Mark this key as "touched" during this transaction.
        self.touched.keys[read.doc.Key] = state

    def start_commit(self, start_commit: Request.StartCommit) -> Response.StartedCommit:
        """Persist state updates for recovery after failures.

        The runtime calls this at the end of each transaction. Returned state
        is durably persisted and will be passed back via open.state on the
        next Open message (e.g., after a restart).

        Args:
            start_commit: Metadata about the commit (includes runtime checkpoint)

        Returns:
            StartedCommit with state to persist. Setting merge_patch=True means
            the state we emit is merged with the prior state, as a JSON merge patch.
            If it were False, the prior state would be completely replaced.
        """

        updated = self.touched.model_dump()
        self.touched = State()

        return Response.StartedCommit(
            state=Response.StartedCommit.State(
                updated=updated,
                merge_patch=True,
            )
        )

    async def reset(self):
        """Reset state for catalog tests.
        The runtime will call this in between tests.
        """
        self.state = State()
        self.touched = State()
