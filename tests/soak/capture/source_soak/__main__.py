"""`source-soak`: a standalone Flow capture connector for soak-testing the V2
runtime. See README.md for the workload design and the downstream invariants it probes.

It speaks newline-delimited JSON — `Request` envelopes in on stdin, `Response`
out on stdout, structured ops logs on stderr — and depends only on Pydantic.
"""

import asyncio
import json
import pathlib
import random
import signal
import sys
from datetime import datetime, timezone
from typing import Any

from . import models
from .models import Request, Response

# The 8 set members, addressable as a one-byte bitmask (bit i <=> member i).
# Kept in sync by hand with the `enum`s in events.schema.json.
SET_MEMBERS = ["a", "b", "c", "d", "e", "f", "g", "h"]

# Single source of truth for the wire document schema, shared with capture/flow.yaml.
# Resolved relative to this file so it's found regardless of the caller's cwd.
_SCHEMA_PATH = pathlib.Path(__file__).resolve().parent.parent / "events.schema.json"
DOCUMENT_SCHEMA: dict[str, Any] = json.loads(_SCHEMA_PATH.read_text())

# Collection key: an append-only log, deduped on (account, sequence).
DOCUMENT_KEY = ["/id", "/seq"]


# --- IO ----------------------------------------------------------------------

_STDOUT = sys.stdout.buffer


def emit(response: Response) -> None:
    """Write a single Response as newline-delimited JSON to stdout."""
    data = response.model_dump_json(by_alias=True, exclude_none=True).encode()
    _STDOUT.write(data)
    _STDOUT.write(b"\n")
    _STDOUT.flush()


def log(level: str, msg: str, **fields: Any) -> None:
    """Write a structured ops log to stderr in Flow's `{level, msg, fields}` form."""
    record = {"level": level, "msg": msg, "fields": {"source": "source-soak", **fields}}
    sys.stderr.write(json.dumps(record))
    sys.stderr.write("\n")
    sys.stderr.flush()


async def stdin_requests():
    """Yield Request envelopes parsed from newline-delimited JSON on stdin."""
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader(limit=1 << 27)  # 128 MiB line limit.
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)

    while line := await reader.readline():
        yield Request.model_validate_json(line)


# --- Request handlers --------------------------------------------------------


def handle_spec() -> models.Spec:
    return models.Spec(
        configSchema=models.EndpointConfig.model_json_schema(),
        resourceConfigSchema=models.ResourceConfig.model_json_schema(),
        documentationUrl="https://github.com/estuary/flow/tree/master/tests/soak",
        resourcePathPointers=["/name"],
    )


def handle_discover(config: models.EndpointConfig) -> models.Discovered:
    """One binding per configured collection name; all share the events schema."""
    return models.Discovered(
        bindings=[
            models.DiscoveredBinding(
                recommendedName=name,
                resourceConfig=models.ResourceConfig(name=name),
                documentSchema=DOCUMENT_SCHEMA,
                key=DOCUMENT_KEY,
            )
            for name in config.collections
        ]
    )


def handle_validate(request: models.ValidateRequest) -> models.Validated:
    return models.Validated(
        bindings=[
            models.ValidatedBinding(resourcePath=[b.resourceConfig.name])
            for b in request.bindings
        ]
    )


# --- Account model -----------------------------------------------------------


def members(mask: int) -> list[str]:
    """Sorted member letters present in an 8-bit set bitmask."""
    return [SET_MEMBERS[i] for i in range(8) if mask & (1 << i)]


def apply_set_op(mask: int) -> tuple[dict[str, list[str]], int]:
    """Pick a random set operation over a random member subset, returning the wire
    op (one of add/remove/intersect) and the resulting bitmask. `add` is weighted
    higher so sets stay populated; `intersect` with an empty subset clears the set."""
    op = random.choices(["add", "remove", "intersect"], weights=[2, 1, 1])[0]
    subset = random.getrandbits(8)
    if op == "add":
        mask |= subset
    elif op == "remove":
        mask &= ~subset & 0xFF
    else:
        mask &= subset
    return {op: members(subset)}, mask


class Accounts:
    """In-memory account state — the oracle authority — backed by connector state.

    `seq`/`mask`/`balance` are keyed by stringified id (JSON object keys). Methods
    mutate state and return wire documents; `checkpoint` emits only the ids touched
    since the prior checkpoint as a merge patch."""

    def __init__(self, base: int, width: int, state: models.CaptureState):
        self.base = base
        self.width = width
        self.seq = dict(state.seq)  # Copy so mutation doesn't alias the parsed model.
        self.mask = dict(state.mask)
        self.balance = dict(state.balance)
        self._touched: set[str] = set()

    def prune_out_of_window(self) -> None:
        """Drop ids outside this shard's window — e.g. inherited by a post-split shard
        which forked a copy of the parent's state but now owns a disjoint window."""
        known = set(self.seq) | set(self.mask) | set(self.balance)
        stale = [k for k in known if not (self.base <= int(k) < self.base + self.width)]
        for k in stale:
            self.seq.pop(k, None)
            self.mask.pop(k, None)
            self.balance.pop(k, None)
        if stale:
            null = {k: None for k in stale}
            emit_state({"seq": null, "mask": null, "balance": null})

    def pick_pair(self) -> tuple[int, int]:
        """Two distinct account ids within the window (sender, receiver)."""
        sender = self.base + random.randrange(self.width)
        receiver = self.base + random.randrange(self.width)
        while receiver == sender and self.width > 1:
            receiver = self.base + random.randrange(self.width)
        return sender, receiver

    def event(self, id: int, delta: int, transfer: dict[str, int]) -> dict[str, Any]:
        """Advance account `id` by one event: apply a set op, move `delta`, bump seq.
        Returns the wire document carrying the post-event oracle."""
        key = str(id)
        seq = self.seq.get(key, 0)
        set_op, mask = apply_set_op(self.mask.get(key, 0))
        balance = self.balance.get(key, 0) + delta

        self.mask[key] = mask
        self.balance[key] = balance
        self.seq[key] = seq + 1
        self._touched.add(key)

        return {
            "id": id,
            "seq": seq,
            "ts": datetime.now(timezone.utc).isoformat(),
            "set": set_op,
            "balanceDelta": delta,
            "transfer": transfer,
            "oracle": {"seq": seq, "set": members(mask), "balance": balance},
        }

    def checkpoint(self) -> None:
        """Persist state for ids touched since the prior checkpoint, as a merge patch."""
        if not self._touched:
            return
        emit_state(
            {
                "seq": {k: self.seq[k] for k in self._touched},
                "mask": {k: self.mask[k] for k in self._touched},
                "balance": {k: self.balance[k] for k in self._touched},
            }
        )
        self._touched = set()


def emit_state(patch: dict[str, Any]) -> None:
    """Emit a merge-patch connector-state checkpoint."""
    ACKS.on_checkpoint()
    emit(
        Response(
            checkpoint=models.Checkpoint(
                state=models.ConnectorState(updated=patch, mergePatch=True)
            )
        )
    )


# --- Acknowledgement join ----------------------------------------------------
#
# We request explicit acknowledgements purely to exercise and sanity-check the
# runtime's checkpoint -> commit -> acknowledge join; our resumption rests on
# connector state, not on knowing when a checkpoint committed, so this is not
# load-bearing for correctness.


class Acks:
    """Reconciles the Response.Checkpoints we emit against the Request.Acknowledges
    the runtime returns post-commit. The runtime acknowledges committed checkpoints
    in order and may batch them, so the running acknowledged count must stay
    positive, monotonic, and never run ahead of what we've actually emitted. (At
    graceful shutdown a few trailing checkpoints are typically un-acked — the runtime
    has closed our stdin before their commits land — which is expected, not a defect.)"""

    def __init__(self) -> None:
        self.emitted = 0  # Response.Checkpoints sent.
        self.acked = 0  # Σ Acknowledge.checkpoints received.
        self.messages = 0  # Acknowledge messages received.

    def on_checkpoint(self) -> None:
        # Single chokepoint: every Response.Checkpoint flows through emit_state, so
        # this count mirrors the runtime's own per-transaction checkpoint tally.
        self.emitted += 1

    def on_acknowledge(self, checkpoints: int) -> None:
        self.messages += 1
        self.acked += checkpoints

        # Proto contract: Acknowledge.checkpoints is always >= 1, and the runtime can
        # only acknowledge checkpoints we've actually emitted. Either violation is a
        # runtime-join defect — log it (soak philosophy: accumulate evidence, never throw).
        if checkpoints < 1:
            log(
                "ERROR",
                "Acknowledge with a non-positive checkpoint count",
                checkpoints=checkpoints,
                acked=self.acked,
                emitted=self.emitted,
            )
        if self.acked > self.emitted:
            log(
                "ERROR",
                "runtime acknowledged more checkpoints than were emitted",
                checkpoints=checkpoints,
                acked=self.acked,
                emitted=self.emitted,
            )
        else:
            log(
                "DEBUG",
                "checkpoints acknowledged",
                checkpoints=checkpoints,
                acked=self.acked,
                emitted=self.emitted,
            )


# Module-level singleton, alongside the other IO globals: emit_state (producer task)
# increments it and the control reader reconciles against it, both on one event loop.
ACKS = Acks()


def id_window(range_spec: models.RangeSpec, id_range: int) -> tuple[int, int]:
    """The [base, base+width) account-id window this shard owns. The window starts at
    key_begin and spans `id_range`, clamped to the owned range when it's narrower."""
    base = range_spec.keyBegin
    if range_spec.keyEnd > range_spec.keyBegin:
        return base, min(id_range, range_spec.keyEnd - range_spec.keyBegin + 1)
    return base, id_range  # Degenerate/unset range (e.g. preview): use the full width.


# --- Capture loop ------------------------------------------------------------


async def run_capture(open: models.OpenRequest, stop: asyncio.Event) -> None:
    """Emit an unbounded stream of double-entry transfer documents until `stop` is set
    (graceful shutdown via stdin EOF or a termination signal). Each transfer emits a
    matched pair — the sender's `-amount` leg and the receiver's `+amount` leg. A doc is
    routed to `id % len(bindings)`, so every event of one account lands in a single
    collection (Flow guarantees strict key+clock ordering within a collection, but NOT
    across them). A transfer's two legs are distinct ids that generally route to
    different collections, so conservation still requires a causally-consistent
    cross-collection read within one derivation transaction (see README)."""

    config = open.capture.config
    bindings = open.capture.bindings
    if not bindings:
        log("WARN", "no bindings; idling", task=open.capture.name)
        await stop.wait()
        return

    base, width = id_window(open.range, config.idRange)
    accounts = Accounts(base, width, open.state)
    accounts.prune_out_of_window()

    log(
        "INFO",
        "resuming soak capture",
        task=open.capture.name,
        base=base,
        width=width,
        bindings=len(bindings),
        known_ids=len(accounts.seq),
    )

    tick_interval = (
        2.0 / config.rate if config.rate > 0 else 0.0
    )  # two docs per transfer.
    since_checkpoint = 0

    while not stop.is_set():
        sender, receiver = accounts.pick_pair()
        amount = random.randint(1, 100)
        transfer = {"from": sender, "to": receiver, "amount": amount}

        for id, delta in ((sender, -amount), (receiver, amount)):
            doc = accounts.event(id, delta, transfer)
            emit(
                Response(captured=models.Captured(binding=id % len(bindings), doc=doc))
            )
            since_checkpoint += 1

        if since_checkpoint >= config.docsPerCheckpoint:
            accounts.checkpoint()
            since_checkpoint = 0

        # Rate-limit, but wake promptly when asked to stop.
        if tick_interval > 0:
            try:
                await asyncio.wait_for(stop.wait(), timeout=tick_interval)
            except asyncio.TimeoutError:
                pass
        else:
            await asyncio.sleep(0)

    # Persist the final state so the next session resumes exactly where we left off.
    accounts.checkpoint()
    log(
        "INFO",
        "soak capture stopped",
        task=open.capture.name,
        known_ids=len(accounts.seq),
        # Ack-join tally: acked <= emitted, the difference being trailing checkpoints
        # the runtime hadn't committed before it closed our stdin (expected).
        checkpoints_emitted=ACKS.emitted,
        checkpoints_acked=ACKS.acked,
        acknowledges=ACKS.messages,
    )


# --- Serve loop --------------------------------------------------------------


async def serve() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    producer: asyncio.Task[None] | None = None

    async def read_control() -> None:
        """Dispatch requests until stdin closes. Open launches the producer and
        keeps reading so a later EOF (or Acknowledge) is still observed."""
        nonlocal producer

        async for request in stdin_requests():
            if request.spec is not None:
                emit(Response(spec=handle_spec()))
            elif request.discover is not None:
                emit(Response(discovered=handle_discover(request.discover.config)))
            elif request.validate_ is not None:
                emit(Response(validated=handle_validate(request.validate_)))
            elif request.apply is not None:
                emit(Response(applied=models.Applied(actionDescription="")))
            elif request.open is not None:
                emit(Response(opened=models.Opened(explicitAcknowledgements=True)))
                producer = asyncio.create_task(run_capture(request.open, stop))
            elif request.acknowledge is not None:
                ACKS.on_acknowledge(request.acknowledge.checkpoints)
            else:
                raise RuntimeError(f"malformed request: {request!r}")

        stop.set()  # stdin EOF: begin graceful shutdown.

    control = asyncio.create_task(read_control())
    await stop.wait()  # stdin EOF or a termination signal.

    control.cancel()  # Let any in-flight producer flush its final checkpoint.
    if producer is not None:
        await producer


def main() -> None:
    asyncio.run(serve())


if __name__ == "__main__":
    main()
