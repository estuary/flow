"""Compares the secret-supplied token against the plaintext expectation.

`ACTUAL_TOKEN` reaches this process only because the runtime resolved the
`test/secrets/token` secret and merged it into `/environment/ACTUAL_TOKEN` of
the derivation's configuration. `EXPECTED_TOKEN` is published in the clear
beside it. Every read publishes the verdict, so the derived collection is the
test's assertion.

Nothing here inspects the source document beyond echoing it: `test/secrets/hello`
asserts its own secret by being unable to start without it, and the greetings it
captures are only the driver which invokes this transform.
"""

import hashlib
import os
from collections.abc import AsyncIterator

from test.secrets.probe import Document, IDerivation, Request

ACTUAL = os.environ.get("ACTUAL_TOKEN")
EXPECTED = os.environ.get("EXPECTED_TOKEN")


def _fingerprint(value: str | None) -> str:
    """Describe a token without disclosing it."""
    if value is None:
        return "unset"
    return f"len={len(value)} sha256={hashlib.sha256(value.encode()).hexdigest()[:12]}"


class Derivation(IDerivation):
    async def from_pings(self, read: Request.ReadFromPings) -> AsyncIterator[Document]:
        if ACTUAL is not None and ACTUAL == EXPECTED:
            yield Document(ts=read.doc.ts, message=read.doc.message, ok=True)
        else:
            yield Document(
                ts=read.doc.ts,
                message=read.doc.message,
                ok=False,
                detail=f"actual({_fingerprint(ACTUAL)}) != expected({_fingerprint(EXPECTED)})",
            )
