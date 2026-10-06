"""Reports, for each greeting, what the connector mount's task-update.json
holds: read afresh through its path, and through a file kept open since the
path was first found. The runtime replaces the file by rename, so only a
fresh read can see a later generation."""

import os
from collections.abc import AsyncIterator
from typing import IO

from acmeCo.vmm_python.updates import IDerivation, Document, Request

MOUNT = os.environ["CONNECTOR_MOUNT"]
PATH = os.path.join(MOUNT, "task-update.json")


def mount_options() -> str:
    with open("/proc/self/mounts") as mounts:
        for line in mounts:
            fields = line.split(" ")
            if fields[1] == MOUNT:
                return fields[3]
    return ""


class Derivation(IDerivation):
    def __init__(self, open: Request.Open):
        super().__init__(open)
        self.held: IO[str] | None = None
        self.options = mount_options()

    async def from_greetings(
        self, read: Request.ReadFromGreetings
    ) -> AsyncIterator[Document]:
        try:
            with open(PATH) as file:
                fresh = file.read()
        except FileNotFoundError:
            fresh = None
        if self.held is None and fresh is not None:
            self.held = open(PATH)
        held = None
        if self.held is not None:
            self.held.seek(0)
            held = self.held.read()
        yield Document(
            n=int(read.doc.message.removeprefix("Hello ").removesuffix("!")),
            fresh=fresh,
            held=held,
            uid=os.getuid(),
            mount=MOUNT,
            options=self.options,
        )
