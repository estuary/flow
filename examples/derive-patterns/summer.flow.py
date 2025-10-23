"""Simple summation derivation that transforms integers to sums."""

from collections.abc import AsyncIterator
from patterns.sums_python import IDerivation, Document, Request


class Derivation(IDerivation):
    """
    Derivation that maps integers to a partial sum.
    These partial sums are then aggregated by the Flow runtime,
    through the use of a `reduce: {strategy: "sum"}` schema annotation.
    """

    async def from_ints(self, read: Request.ReadFromInts) -> AsyncIterator[Document]:
        yield Document(Key=read.doc.Key, Sum=read.doc.Int)
