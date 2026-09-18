"""Compares the secret-supplied token against the plaintext expectation.

`ACTUAL_TOKEN` reaches this process only because the runtime resolved the
`test/secrets/token` secret and merged it into `/environment/ACTUAL_TOKEN` of
the derivation's configuration. `EXPECTED_TOKEN` is published in the clear
beside it. Every read publishes the verdict, so the derived collection is the
test's assertion.

The source document's own `generation` and `source` are echoed through, so one
collection carries both halves of the story: what the capture rotated to, and
whether this derivation's own secret resolved.
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
    async def from_events(self, read: Request.ReadFromEvents) -> AsyncIterator[Document]:
        if ACTUAL is not None and ACTUAL == EXPECTED:
            yield Document(
                ts=read.doc.ts,
                generation=read.doc.generation,
                source=read.doc.source,
                ok=True,
            )
        else:
            yield Document(
                ts=read.doc.ts,
                generation=read.doc.generation,
                source=read.doc.source,
                ok=False,
                detail=f"actual({_fingerprint(ACTUAL)}) != expected({_fingerprint(EXPECTED)})",
            )
