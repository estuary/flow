"""Compares the secret-supplied token against the plaintext expectation.

`actual_token` reaches this process only because the runtime resolved the
`test/secrets/token` secret and merged it into `/actual_token` of the
derivation's `config`. `expected_token` is published in the clear beside it. Every read publishes the verdict, so the derived collection is the
test's assertion.

The source document's own `generation` and `source` are echoed through, so one
collection carries both halves of the story: what the capture rotated to, and
whether this derivation's own secret resolved.
"""

import hashlib
from collections.abc import AsyncIterator

# `EndpointConfig` is generated from the derivation's `spec.configSchema`.
from test.secrets.probe import Document, EndpointConfig, IDerivation, Request


def _fingerprint(value: str | None) -> str:
    """Describe a token without disclosing it."""
    if value is None:
        return "unset"
    return f"len={len(value)} sha256={hashlib.sha256(value.encode()).hexdigest()[:12]}"


class Derivation(IDerivation):
    def __init__(self, open: Request.Open, config: EndpointConfig):
        super().__init__(open, config)
        self.actual = config.actual_token
        self.expected = config.expected_token

    async def from_events(self, read: Request.ReadFromEvents) -> AsyncIterator[Document]:
        if self.actual is not None and self.actual == self.expected:
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
                detail=f"actual({_fingerprint(self.actual)}) != expected({_fingerprint(self.expected)})",
            )
