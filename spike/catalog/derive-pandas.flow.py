"""Experiment 4's derivation. The work is trivial on purpose; what is under test
is that pandas can be fetched and imported at all inside the guest, and that the
derivation then produces documents through the same protocol as any other.

`pandas-stubs` is a dependency because derive-python type-checks the module with
pyright in strict mode, and untyped pandas fails it."""

import os
import sys
from collections.abc import AsyncIterator

import pandas

from acmeCo.pandas_rollup import IDerivation, Document, Request


def scratch_report() -> str:
    """The `/scratch` footprint, which is what experiment 4 records. The disk is
    destroyed with the VM, so nothing can measure it afterwards and the
    derivation reports its own. Never a leading space, which is
    connector-init's readiness byte."""
    # `flowctl preview` validates the catalog through the legacy runtime first,
    # in a plain container with no scratch disk, and this module is imported
    # there too.
    if not os.path.isdir("/scratch"):
        return "scratch-footprint: no /scratch (unsandboxed)"

    stat = os.statvfs("/scratch")
    used_kib = (stat.f_blocks - stat.f_bfree) * stat.f_frsize // 1024
    total_kib = stat.f_blocks * stat.f_frsize // 1024

    entries: list[str] = []
    for entry in sorted(os.scandir("/scratch"), key=lambda e: e.name):
        size = 0
        for root, _, files in os.walk(entry.path):
            for name in files:
                try:
                    size += os.lstat(os.path.join(root, name)).st_size
                except OSError:
                    pass
        entries.append(f"{entry.name}={size // 1024}KiB")

    return (f"scratch-footprint: used={used_kib}KiB total={total_kib}KiB "
            f"pandas={pandas.__version__} {' '.join(entries) or '(empty)'}")


# At import, which is once per connector start and after uv has built the
# environment: the line experiment 4 reads the footprint out of. stderr, not
# stdout: stdout is the derivation protocol's own channel and connector-init
# parses every line of it as a JSON response.
print(scratch_report(), file=sys.stderr, flush=True)


class Derivation(IDerivation):
    async def from_events(self, read: Request.ReadFromEvents) -> AsyncIterator[Document]:
        # One row, so `sum` is just "the value", and unlike `iloc` it is a
        # concretely typed call: derive-python runs pyright in strict mode and
        # most of the pandas indexing API comes back partially unknown there.
        frame = pandas.DataFrame({"message": [read.doc.message]})

        yield Document(
            message=read.doc.message,
            words=int(frame["message"].str.count(r"\S+").sum()),
            characters=int(frame["message"].str.len().sum()),
        )
