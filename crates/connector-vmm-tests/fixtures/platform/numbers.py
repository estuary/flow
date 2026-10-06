"""Numbers each greeting with humanize, which the connector image lacks, and
reports which host names the connector could resolve when it opened."""

import socket
from collections.abc import AsyncIterator
from importlib.metadata import version

import humanize

from acmeCo.vmm_python.numbers import IDerivation, Document, Request

# A connector default, the task's declared host, and a host neither declares.
PROBED = ("pypi.org", "example.org", "example.com")


def resolves(host: str) -> bool:
    try:
        socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return False
    return True


class Derivation(IDerivation):
    def __init__(self, open: Request.Open):
        super().__init__(open)
        resolved = {host: resolves(host) for host in PROBED}
        self.resolved = [host for host in PROBED if resolved[host]]
        self.unresolved = [host for host in PROBED if not resolved[host]]
        self.humanize = version("humanize")

    async def from_greetings(
        self, read: Request.ReadFromGreetings
    ) -> AsyncIterator[Document]:
        n = int(read.doc.message.removeprefix("Hello ").removesuffix("!"))
        yield Document(
            n=n,
            ordinal=humanize.ordinal(n),
            comma=humanize.intcomma(n * 1000),
            humanize=self.humanize,
            resolved=self.resolved,
            unresolved=self.unresolved,
        )
