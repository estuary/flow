"""`materialize-soak`: a standalone Flow materialization connector for soak-testing
the V2 runtime. See README.md ("Downstream: the soak materialization") for the
design and the invariants it probes.

It materializes `test/soak/accounts` into an in-connector, recovery-log-backed
store (state IS the materialized table — there is no external database), turning
the derivation's in-flight checks into an independent verifier that drives
materialize-specific runtime joints: the Load path and V2 max-keys tracking,
the three-phase connector state scatter/gather, and shard-zero state consolidation.

It speaks newline-delimited JSON — `Request` envelopes in on stdin, `Response`
out on stdout, structured ops logs on stderr — and depends only on Pydantic.

The transaction lifecycle the runtime drives is, per transaction:

    Load* -> Flush -> [Loaded*, Flushed] -> Store* -> StartCommit -> [StartedCommit]
          -> Acknowledge -> [Acknowledged]

`Flushed` precedes the Stores, so this transaction's balance delta isn't known
yet there; it's known only after Stores, at `StartedCommit`. The same-transaction
gather of every shard's `StartedCommit` state lands at `Acknowledge` — so the
cross-shard conservation check scatters at StartedCommit and asserts at
Acknowledge, which is *post-commit*: unlike the derivation's pre-commit two-round
Flush, this connector detects a conservation violation immediately after the
offending commit and logs it, never blocking the commit (the soak philosophy:
accumulate evidence, keep running).
"""

import json
import sys
from typing import Any

from . import models
from .models import Request, Response

# The collection key of test/soak/accounts; one component, the account id.
DOCUMENT_KEY = ["/id"]


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
    record = {
        "level": level,
        "msg": msg,
        "fields": {"source": "materialize-soak", **fields},
    }
    sys.stderr.write(json.dumps(record))
    sys.stderr.write("\n")
    sys.stderr.flush()


def stdin_requests():
    """Yield Request envelopes parsed from newline-delimited JSON on stdin.

    `readline()` (not iteration) streams a line as soon as it's terminated, which
    matters for the request/response handshake over a pipe; lines can be large
    (a Flush/StartCommit/Acknowledge carries the gathered `statePatches`)."""
    buf = sys.stdin.buffer
    while line := buf.readline():
        yield Request.model_validate_json(line)


# --- Request handlers (spec / validate / apply) ------------------------------


def handle_spec() -> models.Spec:
    return models.Spec(
        configSchema=models.EndpointConfig.model_json_schema(),
        resourceConfigSchema=models.ResourceConfig.model_json_schema(),
        documentationUrl="https://github.com/estuary/flow/tree/master/tests/soak",
    )


def constraints_for(collection: models.CollectionSpec) -> dict[str, models.Constraint]:
    """Require the root document and every key field; offer all other projections.

    The full root document is required for BOTH binding modes: the standard binding
    reloads it to reduce, and the delta binding needs it to re-verify the oracle.
    Projections omitted from the map are implicitly forbidden, so we emit one per
    projection to keep the field selection maximal."""
    out: dict[str, models.Constraint] = {}
    for p in collection.projections:
        if p.ptr == "":
            out[p.field] = models.Constraint(
                type="LOCATION_REQUIRED",
                reason="The full document is materialized (and reloaded, for the standard binding).",
            )
        elif p.isPrimaryKey:
            out[p.field] = models.Constraint(
                type="LOCATION_REQUIRED", reason="Key fields are required."
            )
        else:
            out[p.field] = models.Constraint(
                type="FIELD_OPTIONAL", reason="This field may be materialized."
            )
    return out


def handle_validate(request: models.ValidateRequest) -> models.Validated:
    return models.Validated(
        bindings=[
            models.ValidatedBinding(
                constraints=constraints_for(b.collection),
                resourcePath=[b.resourceConfig.table],
                deltaUpdates=b.resourceConfig.delta,
            )
            for b in request.bindings
        ]
    )


# --- Ledger: the materialized store and the per-transaction verifier ---------


def _sorted_set(value: Any) -> list[str]:
    return sorted(value or [])


def _matches_oracle(doc: dict[str, Any]) -> bool:
    """Does a reconstruction doc's {seq, set, balance} equal its carried oracle?"""
    oracle = doc.get("oracle") or {}
    return (
        int(doc.get("balance", 0)) == int(oracle.get("balance", 0))
        and int(doc.get("seq", -1)) == int(oracle.get("seq", -2))
        and _sorted_set(doc.get("set")) == _sorted_set(oracle.get("set"))
    )


class Ledger:
    """In-connector materialized store and per-transaction verifier.

    The `standard` map IS the materialized standard table: id -> full reduced doc.
    It's persisted as a merge patch in each StartedCommit (transactional, lands in
    shard zero's RocksDB via the leader) and recovered whole on Open. On Open the
    leader broadcasts the *full task-level* state to every shard, so the map holds
    the union of all ids; ids this shard doesn't own are inert (never stored,
    never re-persisted) and keep the map split-safe — exactly as the accounts
    derivation treats its account map.

    Memory is bounded by the account population, never by event count or uptime:
    the delta binding retains nothing across transactions, and per-transaction
    working sets are dropped at StartedCommit."""

    def __init__(self, open_req: models.OpenRequest):
        self.key_begin = open_req.range.keyBegin
        self.disable_load_opt = open_req.materialization.config.forceLoads
        # Per-binding-index delta_updates flag, from the validated spec.
        self.binding_delta = [b.deltaUpdates for b in open_req.materialization.bindings]

        self.standard: dict[int, dict[str, Any]] = {}
        state = open_req.state if isinstance(open_req.state, dict) else {}
        for id_str, doc in (state.get("standard") or {}).items():
            if doc is not None:
                self.standard[int(id_str)] = doc

        # Per-transaction working state.
        self.loads: list[tuple[int, int]] = []  # (binding, id) awaiting a Loaded reply.
        self._reset_txn()

    def _reset_txn(self) -> None:
        self.patch: dict[
            str, Any
        ] = {}  # id(str) -> doc | None: the standard merge patch to persist.
        self.delta = 0  # This shard's per-txn Σ(new_balance − prior_balance) (conservation probe).
        self.txn_standard: dict[
            int, dict[str, Any]
        ] = {}  # This txn's standard docs (cross-mode check).
        self.txn_delta: dict[int, dict[str, Any]] = {}  # This txn's delta docs.

    # Load: record the requested key; the stored doc is returned at Flush.
    def on_load(self, load: models.Load) -> None:
        self.loads.append((load.binding, int(load.key[0])))

    # Flush ends the Load phase: answer every recorded Load whose key we hold,
    # then send Flushed. Nothing is scattered here — the per-txn delta is not yet
    # known (Stores follow Flush), so conservation scatters at StartedCommit.
    def on_flush(self, _flush: models.Flush) -> list[Response]:
        out: list[Response] = []
        for binding, id in self.loads:
            doc = self.standard.get(id)
            if doc is not None:
                out.append(Response(loaded=models.Loaded(binding=binding, doc=doc)))
        self.loads = []
        out.append(Response(flushed=models.Flushed()))
        return out

    # Store one reduced document. Updates the store, accumulates the conservation
    # delta from the loaded-vs-stored balance, and runs the at-rest probes.
    def on_store(self, store: models.Store) -> None:
        id = int(store.key[0])
        doc = store.doc if isinstance(store.doc, dict) else {}
        is_delta = (
            self.binding_delta[store.binding]
            if store.binding < len(self.binding_delta)
            else False
        )

        if is_delta:
            # Delta binding: zero-retention verifier of the no-load combine path.
            self.txn_delta[id] = doc
            if id >= 0:
                self._check_integrity(id, doc, "delta")
            return

        # Standard binding.
        prior = self.standard.get(id)

        # exists-flag probe (V2 max-keys): the runtime must not claim a key exists
        # that we can't serve. The reverse (we hold an id the runtime calls new)
        # is benign under the full-state broadcast, so we don't flag it.
        if store.exists and prior is None:
            log(
                "ERROR",
                "materialize exists-flag claims a key we don't hold",
                id=id,
                keyBegin=self.key_begin,
            )

        if store.delete:
            # The accounts root reduce is lastWriteWins with no deletion; treat
            # a delete defensively and flag it for real ids.
            if prior is not None and id >= 0:
                self.delta -= int(prior.get("balance", 0))
            self.standard.pop(id, None)
            self.patch[str(id)] = None
            if id >= 0:
                log(
                    "ERROR",
                    "unexpected delete in materialize store",
                    id=id,
                    keyBegin=self.key_begin,
                )
            return

        if id >= 0:
            prior_balance = int(prior.get("balance", 0)) if prior is not None else 0
            self.delta += int(doc.get("balance", 0)) - prior_balance
            # seq monotonicity at the sink: lastWriteWins must only advance an id.
            if prior is not None and int(doc.get("seq", -1)) < int(
                prior.get("seq", -1)
            ):
                log(
                    "ERROR",
                    "materialize seq regression at sink",
                    id=id,
                    priorSeq=prior.get("seq"),
                    gotSeq=doc.get("seq"),
                    keyBegin=self.key_begin,
                )
            self._check_integrity(id, doc, "standard")
            self.txn_standard[id] = doc
        else:
            self._relay_sentinel(doc)

        self.standard[id] = doc
        self.patch[str(id)] = doc

    # StartCommit ends the Store phase: persist this txn's touched ids and scatter
    # this shard's conservation delta, both via the (transactional) StartedCommit
    # state. The cross-mode agreement check runs here, before the working set is
    # dropped. runtimeCheckpoint is ignored (recovery-log-authoritative).
    def on_start_commit(self, _sc: models.StartCommit) -> list[Response]:
        self._check_cross_mode()
        updated = {
            "standard": self.patch,
            # Per-txn delta, namespaced by keyBegin; ignored on Open (cruft), it
            # exists only so peers can gather it at Acknowledge.
            "deltas": {str(self.key_begin): self.delta},
        }
        resp = Response(
            startedCommit=models.StartedCommit(
                state=models.ConnectorState(updated=updated, mergePatch=True)
            )
        )
        self._reset_txn()
        return [resp]

    # Acknowledge carries the gathered StartedCommit states of every shard for the
    # just-committed transaction (including this shard's own). Sum each shard's
    # per-txn delta and assert the global is exactly zero.
    def on_acknowledge(self, ack: models.Acknowledge) -> list[Response]:
        global_delta = 0
        shard_keys: list[str] = []
        patches = ack.statePatches
        if isinstance(patches, list):
            for p in patches:
                if not isinstance(p, dict):
                    continue
                for kb, d in (p.get("deltas") or {}).items():
                    shard_keys.append(kb)
                    global_delta += int(d or 0)

        if global_delta != 0:
            log(
                "ERROR",
                "conservation violated: global per-transaction balance delta is non-zero",
                sum=global_delta,
                keyBegin=self.key_begin,
                shardKeys=shard_keys,
            )
        return [Response(acknowledged=models.Acknowledged())]

    # The standard (load+reduce) and delta (combine-only) bindings receive the
    # same accounts docs each transaction; for lastWriteWins both resolve to this
    # txn's highest-seq doc, so they must agree id-for-id. A divergence is a
    # standard-vs-delta materialization defect.
    def _check_cross_mode(self) -> None:
        for id, s in self.txn_standard.items():
            d = self.txn_delta.get(id)
            if d is None:
                log(
                    "ERROR",
                    "standard/delta divergence: id stored to standard but not delta",
                    id=id,
                    keyBegin=self.key_begin,
                )
            elif (
                int(s.get("balance", 0)) != int(d.get("balance", 0))
                or int(s.get("seq", -1)) != int(d.get("seq", -1))
                or _sorted_set(s.get("set")) != _sorted_set(d.get("set"))
            ):
                log(
                    "ERROR",
                    "standard/delta divergence: value mismatch",
                    id=id,
                    standardBalance=s.get("balance"),
                    deltaBalance=d.get("balance"),
                    standardSeq=s.get("seq"),
                    deltaSeq=d.get("seq"),
                    keyBegin=self.key_begin,
                )
        for id in self.txn_delta:
            if id >= 0 and id not in self.txn_standard:
                log(
                    "ERROR",
                    "standard/delta divergence: id stored to delta but not standard",
                    id=id,
                    keyBegin=self.key_begin,
                )

    # A faithfully transported doc is self-consistent: its recomputed oracle match
    # equals its own `ok` verdict. If they disagree, the round-trip corrupted the
    # doc (a materialization defect). A self-consistent ok=false is an upstream
    # defect the derivation already logged ERROR for — relayed here at WARN.
    def _check_integrity(self, id: int, doc: dict[str, Any], mode: str) -> None:
        matches = _matches_oracle(doc)
        ok = doc.get("ok") is True
        if matches != ok:
            oracle = doc.get("oracle") or {}
            log(
                "ERROR",
                "materialize doc corruption: ok verdict disagrees with carried oracle",
                id=id,
                mode=mode,
                ok=ok,
                recomputedMatch=matches,
                balance=doc.get("balance"),
                oracleBalance=oracle.get("balance"),
                seq=doc.get("seq"),
                oracleSeq=oracle.get("seq"),
                gotSet=_sorted_set(doc.get("set")),
                oracleSet=_sorted_set(oracle.get("set")),
                keyBegin=self.key_begin,
            )
        elif not ok:
            log(
                "WARN",
                "upstream-flagged non-ok account reached the sink",
                id=id,
                mode=mode,
                keyBegin=self.key_begin,
            )

    def _relay_sentinel(self, doc: dict[str, Any]) -> None:
        violation = doc.get("violation") or {}
        log(
            "ERROR",
            "derivation conservation-violation sentinel reached the sink",
            id=doc.get("id"),
            sum=violation.get("sum"),
            sourceKeyBegin=violation.get("keyBegin"),
            keyBegin=self.key_begin,
        )


# --- Serve loop --------------------------------------------------------------


def serve() -> None:
    """Dispatch requests until stdin closes. Materialize is purely reactive — each
    request maps to zero or more responses — so the loop is a straight read/handle,
    with no background producer (unlike the capture)."""
    ledger: Ledger | None = None

    for request in stdin_requests():
        if request.spec is not None:
            emit(Response(spec=handle_spec()))
        elif request.validate_ is not None:
            emit(Response(validated=handle_validate(request.validate_)))
        elif request.apply is not None:
            emit(Response(applied=models.Applied(actionDescription="")))
        elif request.open is not None:
            ledger = Ledger(request.open)
            emit(
                Response(
                    opened=models.Opened(
                        disableLoadOptimization=ledger.disable_load_opt
                    )
                )
            )
            log(
                "INFO",
                "opened soak materialization",
                keyBegin=ledger.key_begin,
                known_ids=len(ledger.standard),
                bindings=len(ledger.binding_delta),
                force_loads=ledger.disable_load_opt,
            )
        elif request.load is not None:
            assert ledger is not None
            ledger.on_load(request.load)
        elif request.flush is not None:
            assert ledger is not None
            for response in ledger.on_flush(request.flush):
                emit(response)
        elif request.store is not None:
            assert ledger is not None
            ledger.on_store(request.store)
        elif request.startCommit is not None:
            assert ledger is not None
            for response in ledger.on_start_commit(request.startCommit):
                emit(response)
        elif request.acknowledge is not None:
            assert ledger is not None
            for response in ledger.on_acknowledge(request.acknowledge):
                emit(response)
        else:
            raise RuntimeError(f"malformed request: {request!r}")


def main() -> None:
    serve()


if __name__ == "__main__":
    main()
