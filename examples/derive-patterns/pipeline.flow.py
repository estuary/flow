"""Async pipelining derivation with bounded concurrency.

This example shows how to:
1. Maintain a bounded set of concurrent async tasks
2. Stream results as tasks complete
3. Await remaining tasks at "flush" time, when the transaction is closing

Pattern: Process up to N concurrent async operations (e.g., API calls),
yielding results as they complete to avoid unbounded memory growth.
"""

import asyncio
from collections.abc import AsyncIterator
from patterns.pipeline import IDerivation, Document, Request


class Derivation(IDerivation):
    """Derivation that processes documents with bounded concurrency."""

    MAX_CONCURRENT_TASKS = 10

    def __init__(self, open: Request.Open):
        """Initialize with no pending tasks."""
        super().__init__(open)
        self.pending_tasks: set[asyncio.Task[tuple[str, int]]] = set()

    async def from_ints(self, read: Request.ReadFromInts) -> AsyncIterator[Document]:
        """
        Start a new task, but if we've hit our concurrency limit, await
        the first completed task and yield its result before continuing.
        """

        # If we're at our concurrency limit, await one or more task completions.
        if len(self.pending_tasks) >= self.MAX_CONCURRENT_TASKS:
            done, pending = await asyncio.wait(
                self.pending_tasks, return_when=asyncio.FIRST_COMPLETED
            )

            # Yield result from completed task
            for completed_task in done:
                key, doubled = await completed_task
                yield Document(Key=key, Doubled=doubled)

            self.pending_tasks = pending

        # Start an async task for this document
        task = asyncio.create_task(self._process_int(read.doc.Key, read.doc.Int))
        self.pending_tasks.add(task)


    async def flush(self) -> AsyncIterator[Document]:
        """Await any remaining tasks and emit their results."""

        results: list[tuple[str, int]] = await asyncio.gather(*self.pending_tasks)
        self.pending_tasks.clear()

        for key, doubled in results:
            yield Document(Key=key, Doubled=doubled)

    async def _process_int(self, key: str, value: int) -> tuple[str, int]:
        """Simulate an async I/O operation, such as an API call"""
        await asyncio.sleep(0.001)

        # Simple transformation: double the value
        return (key, value * 2)
