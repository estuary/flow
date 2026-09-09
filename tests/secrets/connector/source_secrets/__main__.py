"""`source-secrets`: the reference capture connector for first-class secrets.

It is what a connector author copies. Two things it does which a normal
connector doesn't have to think about:

  * **Migration.** A task published before first-class secrets carries a
    sops-wrapped configuration and an empty `secrets` stanza. On seeing that,
    this connector wraps the credentials it manages as a sibling secret, stores
    it, and rewrites its own configuration to draw from `secrets` instead. The
    publication that follows restarts it -- which is how you see it worked.
  * **Rotation.** Every `rotate_every` seconds it mints a new access token and
    stores it, so the secret a restarted task resolves is the current one.

It speaks newline-delimited JSON -- `Request` envelopes in on stdin, `Response`
out on stdout, structured ops logs on stderr -- and depends only on Pydantic.

What it never does is put a credential in a document or a log. Emitted
documents carry sha256 prefixes, and the `generation` parsed out of the access
token, which is enough to assert the chain without disclosing anything.
"""

import asyncio
import hashlib
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

from . import models
from .models import Request, Response
from .rotation import RouteError, Rotation, Unconfigured

# The vendor's secret, reachable under the image rule rather than as a sibling
# of any task: the image repository is its immediate parent. Declared in the
# Dockerfile's `dev.estuary.secrets` label, which is what lets the reactor hand
# it to us at all, and repeated here because migration has to write it into the
# stanza it proposes.
IMAGE_SECRET = "test/connectors/registry.example/acme/source-secrets/oauth-client"

# Siblings of the task, named by convention off its own catalog prefix.
# `oauth-tokens` is ours to write; `rotate-every` is provisioned by an operator
# and we only refer to it.
TOKENS_SECRET = "oauth-tokens"
ROTATE_EVERY_SECRET = "rotate-every"

DOCUMENT_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "required": ["ts", "generation", "source"],
    "properties": {
        "ts": {"type": "string", "format": "date-time"},
        # Parsed from the current access token, which is always `gen-<n>`.
        # Monotonic across restarts: a rotation which reached the control plane
        # is resolved by the next session, and one which didn't is still held
        # in memory by this one.
        "generation": {"type": "integer"},
        # Which mechanism supplied the credentials of this document: `legacy`
        # while the sops configuration is still in force, `sibling` once the
        # stanza is. It flips exactly once, at the migration restart.
        "source": {"type": "string", "enum": ["legacy", "sibling"]},
        # Digests, never values.
        "client_id_digest": {"type": "string"},
        "token_digest": {"type": "string"},
    },
}
DOCUMENT_KEY = ["/ts"]


# --- IO ----------------------------------------------------------------------

_STDOUT = sys.stdout.buffer


def emit(response: Response) -> None:
    """Write a single Response as newline-delimited JSON to stdout."""
    _STDOUT.write(response.model_dump_json(by_alias=True, exclude_none=True).encode())
    _STDOUT.write(b"\n")
    _STDOUT.flush()


def log(level: str, msg: str, **fields: Any) -> None:
    """Write a structured ops log to stderr in Flow's `{level, msg, fields}` form."""
    record = {"level": level, "msg": msg, "fields": {"connector": "source-secrets", **fields}}
    sys.stderr.write(json.dumps(record))
    sys.stderr.write("\n")
    sys.stderr.flush()


async def stdin_requests():
    """Yield Request envelopes parsed from newline-delimited JSON on stdin."""
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader(limit=1 << 27)
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)

    while line := await reader.readline():
        yield Request.model_validate_json(line)


# --- Pure helpers -------------------------------------------------------------


def digest(value: str) -> str:
    """A short sha256 prefix, so a document can show that a credential changed
    without showing what it changed to."""
    return hashlib.sha256(value.encode()).hexdigest()[:12] if value else ""


def generation(access_token: str) -> int:
    """The generation of a `gen-<n>` access token, or 0 if it isn't one."""
    _, _, suffix = access_token.partition("gen-")
    return int(suffix) if suffix.isdigit() else 0


def parent_prefix(task_name: str) -> str:
    """The catalog prefix which directly contains `task_name`. A secret is a
    sibling of the task when it sits directly under this."""
    return task_name[: task_name.rfind("/") + 1]


def migrated_config(config: models.EndpointConfig) -> dict[str, Any]:
    """The plaintext configuration a migrated task should carry: everything the
    published one had, minus every value the `secrets` stanza now supplies.

    `credentials` empties entirely -- all four of its fields are annotated
    `secret: true`, and publication refuses a plaintext value at any of them
    once a stanza is in force. `rotate_every` goes too, because a sibling
    secret supplies it. `emit_every` isn't a secret and stays.
    """
    return {"credentials": {}, "emit_every": config.emit_every}


def migrated_stanza(task_name: str) -> dict[str, str]:
    """The `secrets` stanza a migrated task should carry.

    Three entries, two of which merge at the same location: the vendor's client
    id and secret, and this task's own access and refresh tokens. They merge as
    RFC 7396 patches over non-overlapping properties, so the order in which
    they apply -- lexicographic by secret name, and not otherwise specified --
    cannot matter.
    """
    prefix = parent_prefix(task_name)
    return {
        IMAGE_SECRET: "/credentials",
        f"{prefix}{TOKENS_SECRET}": "/credentials",
        f"{prefix}{ROTATE_EVERY_SECRET}": "/rotate_every",
    }


# --- Request handlers ---------------------------------------------------------


def handle_spec() -> models.Spec:
    return models.Spec(
        configSchema=models.EndpointConfig.model_json_schema(),
        resourceConfigSchema=models.ResourceConfig.model_json_schema(),
        documentationUrl="https://github.com/estuary/flow/tree/master/tests/secrets",
        resourcePathPointers=["/name"],
    )


def handle_discover() -> models.Discovered:
    return models.Discovered(
        bindings=[
            models.DiscoveredBinding(
                recommendedName="events",
                resourceConfig=models.ResourceConfig(name="events"),
                documentSchema=DOCUMENT_SCHEMA,
                key=DOCUMENT_KEY,
            )
        ]
    )


def handle_validate(request: models.ValidateRequest) -> models.Validated:
    return models.Validated(
        bindings=[
            models.ValidatedBinding(resourcePath=[b.resourceConfig.name])
            for b in request.bindings
        ]
    )


# --- Credential state ---------------------------------------------------------


class Credentials:
    """The credentials in force, and everything that changes them.

    Held in memory and advanced locally, so that a failed route call costs
    nothing but a stale stored value: the next tick re-attempts the same set,
    and a session which never succeeds still captures correctly under the
    credentials it holds.
    """

    def __init__(self, task_name: str, config: models.EndpointConfig, migrating: bool):
        self.task_name = task_name
        self.config = config
        self.access_token = config.credentials.access_token
        self.refresh_token = config.credentials.refresh_token
        self.client_id = config.credentials.client_id
        # True until a migration has been accepted. It's also what `source`
        # reports, so a document says which mechanism supplied its credentials.
        self.migrating = migrating
        self.rotation = Rotation()

    @property
    def source(self) -> str:
        return "legacy" if self.migrating else "sibling"

    @property
    def tokens_secret(self) -> str:
        return f"{parent_prefix(self.task_name)}{TOKENS_SECRET}"

    def document(self) -> dict[str, Any]:
        return {
            "ts": datetime.now(timezone.utc).isoformat(),
            "generation": generation(self.access_token),
            "source": self.source,
            "client_id_digest": digest(self.client_id),
            "token_digest": digest(self.access_token),
        }

    def advance(self) -> None:
        """Mint the next generation, in memory."""
        next_gen = generation(self.access_token) + 1
        self.access_token = f"gen-{next_gen}"
        self.refresh_token = f"refresh-{next_gen}"

    def store_tokens(self) -> str:
        """Wrap and store the current tokens, returning the `secretId`."""
        document = self.rotation.wrap(
            self.tokens_secret,
            {"access_token": self.access_token, "refresh_token": self.refresh_token},
        )
        secret_id, _changed = self.rotation.set_secret(self.tokens_secret, document)
        return secret_id


def rotate(creds: Credentials) -> None:
    """Advance to the next generation and store it.

    The in-memory advance happens whether or not the store succeeds, so a task
    whose control plane is unreachable still rotates -- it just can't persist
    what it rotated to, and says so. The next tick tries again with the
    generation after that, which is fine: generations only need to be
    monotonic, not gapless.
    """
    creds.advance()

    try:
        secret_id = creds.store_tokens()
    except Unconfigured:
        log(
            "INFO",
            "rotated in memory only; no rotation credentials were injected",
            generation=generation(creds.access_token),
        )
        return
    except RouteError as err:
        log(
            "WARN",
            "rotated in memory, but could not store the new secret",
            generation=generation(creds.access_token),
            error=str(err),
        )
        return

    log(
        "INFO",
        "rotated task secret",
        secret=creds.tokens_secret,
        secretId=secret_id,
        generation=generation(creds.access_token),
    )


def migrate(creds: Credentials) -> None:
    """Move a legacy sops configuration onto first-class secrets.

    Three steps, in this order: wrap and store the tokens, then repoint the
    configuration at them. Doing it the other way round would publish a task
    naming a secret which does not exist yet, whose next start would fail to
    resolve.

    Captures continue from memory throughout. Success is not observed here --
    `update-config` does not publish, it hands the update to the controller --
    but the publication that follows restarts this connector, and the restarted
    session sees a non-empty stanza and reports `source: sibling`.
    """
    try:
        secret_id = creds.store_tokens()
        creds.rotation.update_config(
            migrated_config(creds.config),
            migrated_stanza(creds.task_name),
            message="migrated endpoint configuration onto first-class secrets",
        )
    except Unconfigured:
        log(
            "INFO",
            "cannot migrate without rotation credentials; continuing from memory",
        )
        return
    except RouteError as err:
        # Left `migrating`, so the next tick re-attempts the whole migration.
        # Re-storing an identical document mints no new id, so a retry which
        # got as far as the secret costs nothing.
        log("WARN", "migration failed; will retry", error=str(err))
        return

    log(
        "INFO",
        "requested migration onto first-class secrets",
        secret=creds.tokens_secret,
        secretId=secret_id,
        stanza=sorted(migrated_stanza(creds.task_name)),
    )


# --- Capture loop -------------------------------------------------------------


async def run_capture(open: models.OpenRequest, stop: asyncio.Event) -> None:
    """Emit one document every `emit_every`, rotating every `rotate_every`,
    until the runtime closes our stdin.

    This never exits of its own accord. A restart therefore means an upstream
    publication -- which, in this catalog, means a migration or a controller
    republication actually landed. That makes restarts a signal worth watching
    in the task's ops logs, alongside the `resolved task secret` lines the
    runtime emits as it merges each secret in.
    """
    spec = open.capture
    config = spec.config

    if not spec.bindings:
        log("WARN", "no bindings; idling", task=spec.name)
        await stop.wait()
        return

    # An empty stanza is the whole migration trigger: a task published before
    # first-class secrets has its credentials inside a sops envelope, and names
    # no secrets at all.
    creds = Credentials(spec.name, config, migrating=not spec.secrets)

    log(
        "INFO",
        "opened",
        task=spec.name,
        source=creds.source,
        generation=generation(creds.access_token),
        rotate_every=config.rotate_every,
        emit_every=config.emit_every,
        rotation_enabled=creds.rotation.enabled,
        secrets=sorted(spec.secrets),
    )
    if not creds.rotation.enabled:
        log(
            "INFO",
            "no rotation credentials were injected; rotating in memory only. "
            "This is expected under `flowctl preview` and in tests.",
        )

    emit(Response(opened=models.Opened()))

    # One clock for credential work. A migration is attempted at once, because
    # the task is running on a configuration we mean to replace; a rotation
    # waits a full interval, so a restart doesn't itself burn a generation.
    # Either way a failure retries on the same cadence and never hot-loops.
    next_attempt = time.monotonic() + (0.0 if creds.migrating else config.rotate_every)

    while not stop.is_set():
        now = time.monotonic()

        if now >= next_attempt:
            if creds.migrating:
                migrate(creds)
            else:
                rotate(creds)
            next_attempt = now + config.rotate_every

        emit(Response(captured=models.Captured(binding=0, doc=creds.document())))
        # No connector state to carry: the credentials live in `internal.secrets`
        # and the endpoint configuration, which is the entire point.
        emit(
            Response(
                checkpoint=models.Checkpoint(
                    state=models.ConnectorState(updated={}, mergePatch=True)
                )
            )
        )

        try:
            await asyncio.wait_for(stop.wait(), timeout=config.emit_every)
        except asyncio.TimeoutError:
            pass

    log("INFO", "closed", task=spec.name, generation=generation(creds.access_token))


# --- Serve loop ---------------------------------------------------------------


async def serve() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    producer: asyncio.Task[None] | None = None

    async def read_control() -> None:
        nonlocal producer

        async for request in stdin_requests():
            if request.spec is not None:
                emit(Response(spec=handle_spec()))
            elif request.discover is not None:
                emit(Response(discovered=handle_discover()))
            elif request.validate_ is not None:
                emit(Response(validated=handle_validate(request.validate_)))
            elif request.apply is not None:
                emit(Response(applied=models.Applied()))
            elif request.open is not None:
                producer = asyncio.create_task(run_capture(request.open, stop))
            elif request.acknowledge is not None:
                pass  # We don't set `explicitAcknowledgements`.
            else:
                raise RuntimeError(f"malformed request: {request!r}")

        stop.set()  # stdin EOF: begin graceful shutdown.

    control = asyncio.create_task(read_control())
    await stop.wait()

    control.cancel()
    if producer is not None:
        await producer


def main() -> None:
    # Unbuffered stderr, so a log which precedes a crash still reaches the task.
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    asyncio.run(serve())


if __name__ == "__main__":
    main()
