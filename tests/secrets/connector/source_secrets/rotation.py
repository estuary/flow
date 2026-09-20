"""The three HTTP calls by which a connector manages its own credentials.

Separated from the capture loop because they are the only IO here which isn't
the protocol itself, and because they are the part a connector author is
actually copying. The reactor hands all three inputs to the connector as one
file in its connector mount, `$CONNECTOR_MOUNT/task-update.json`:

  token                 - a `TASK_UPDATE` JWT naming this task, and -- on a
                          session -- the build it runs
  control_plane_url     - serves /task/set-secret and /task/update-config
  config_encryption_url - serves /secret/encrypt

`CONNECTOR_MOUNT` is always set, but the file is there only where task update
is available. Its absence means "rotate in memory only": `flowctl preview` and
tests hold no data-plane key and have no control plane to rotate against. That
is a supported mode, not an error.

The reactor rewrites that file **in place** with a fresh token well before the
current one expires, so it is re-read on every call rather than cached. See
`crates/connector/README.md`, "Connector mount", for the contract.

Wrapping is two hops on purpose. The connector posts its plaintext to
config-encryption, which returns a sops document under a KMS key only it holds,
and only that document goes to the control plane -- which therefore never sees
the plaintext of a secret it stores.

A connector which updates its task through these routes must not also emit the
legacy `configUpdate` log event: that event carries no `secrets` stanza, and
publishing it clears the task's stanza. See `crates/connector/README.md`,
"Secrets and task update".
"""

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

# Bound on honoring `retryMillis` within one call, so a control plane which
# keeps asking for retries surfaces as a `RouteError` rather than a stall.
MAX_RETRY_SECONDS = 60.0


class Unconfigured(Exception):
    """The connector mount holds no `task-update.json`."""


class RouteError(Exception):
    """A route was unreachable, answered with a non-200, or kept asking to be
    retried beyond `MAX_RETRY_SECONDS`.

    Always recoverable by the caller: it keeps the new credential in memory and
    retries the same set on its next tick. What must never happen is a tight
    retry loop, or a connector which reports success it didn't have.
    """


class TaskUpdate:
    """Context for task updates, read from the connector mount at each use."""

    def _context(self) -> tuple[str, str, str]:
        """Read `$CONNECTOR_MOUNT/task-update.json`, or raise `Unconfigured`
        where task update isn't available.

        Read afresh by every route below, and never cached: the reactor
        rewrites this file in place on an interval well inside the token's
        lifetime, so a connector which held the first token it read would
        eventually present an expired credential. The rewrite is a rename, so a
        read sees one whole version or the other, never a truncated token.
        """
        mount = os.environ.get("CONNECTOR_MOUNT", "")
        if not mount:
            raise Unconfigured()
        try:
            document = json.loads((Path(mount) / "task-update.json").read_text())
        except FileNotFoundError:
            raise Unconfigured() from None

        context = (
            document.get("token", ""),
            document.get("control_plane_url", ""),
            document.get("config_encryption_url", ""),
        )
        if not all(context):
            raise Unconfigured()

        return context

    @property
    def enabled(self) -> bool:
        try:
            _ = self._context()
            return True
        except Unconfigured:
            return False

    def _post(self, url: str, body: bytes) -> Any:
        request = urllib.request.Request(url, data=body, method="POST")
        request.add_header("content-type", "application/json")

        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.loads(response.read() or b"null")
        except urllib.error.HTTPError as err:
            # The body is the interesting half: these routes answer a denial
            # with a bare status message naming what was refused and why.
            detail = err.read().decode("utf-8", "replace").strip()
            raise RouteError(f"{url} responded {err.code}: {detail}") from None
        except (urllib.error.URLError, OSError, ValueError) as err:
            raise RouteError(f"{url} is unreachable: {err}") from None

    def _control(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        """POST to a control-plane route, which authorizes from `token` in the
        body rather than from a header, honoring its requests to be retried.

        A control-plane route answers a provisional failure -- a Snapshot which
        hasn't caught up, a migration in flight -- with a 200 carrying
        `retryMillis` and *nothing written*. Reading such a response as success
        is the one mistake that silently loses a rotation, so it is handled here
        rather than at each call site: wait as asked and post again, up to
        `MAX_RETRY_SECONDS` in all, and then fail the call.
        """
        token, control_plane_url, _ = self._context()

        url = f"{control_plane_url.rstrip('/')}{path}"
        payload = json.dumps({**body, "token": token}).encode()
        deadline = time.monotonic() + MAX_RETRY_SECONDS

        while True:
            result = self._post(url, payload)

            if not isinstance(result, dict):
                raise RouteError(f"{url} responded 200 with {result!r}, not an object")
            retry_millis = result.get("retryMillis") or 0
            if not retry_millis:
                return result

            if time.monotonic() + retry_millis / 1000 > deadline:
                raise RouteError(
                    f"{url} asked to be retried in {retry_millis}ms, "
                    f"beyond the {MAX_RETRY_SECONDS:.0f}s allowed for one call"
                )
            time.sleep(retry_millis / 1000)

    def wrap(self, name: str, value: Any) -> Any:
        """Wrap `value` as the sops document of secret `name`.

        Unauthenticated: config-encryption will wrap anything for anyone. It's
        reading one back out which is authorized, and the document is bound to
        the name it was wrapped for -- sops MACs `name` even though it's stored
        in the clear -- so a document cannot be stored under another name.
        """
        _, _, config_encryption_url = self._context()

        url = f"{config_encryption_url.rstrip('/')}/secret/encrypt"
        url = f"{url}?name={urllib.parse.quote(name, safe='')}"
        return self._post(url, json.dumps(value).encode())

    def set_secret(self, name: str, document: Any) -> tuple[str, bool]:
        """Store a wrapped document, returning its `secretId` and whether it
        changed. Only a sibling of the task may be set: an image-rule secret
        belongs to the image's publisher and is updated by hand.
        """
        result = self._control("/task/set-secret", {"name": name, "document": document})

        # A response which neither asked for a retry nor minted an id is a
        # control-plane bug. Report it as recoverable rather than crashing the
        # capture, and never as a rotation which happened.
        if not (secret_id := result.get("secretId")):
            raise RouteError(f"/task/set-secret stored '{name}' without a secretId")

        return secret_id, bool(result.get("changed"))

    def update_config(
        self, config: Any, secrets: dict[str, str], message: str | None = None
    ) -> None:
        """Record a complete endpoint configuration and `secrets` stanza.

        Both replace their model counterparts wholesale; neither is a patch, so
        pass everything the task should have. This does not publish: it hands
        the update to the task's controller, which publishes it -- and the
        publication restarts this connector, which is how a successful
        migration makes itself visible.
        """
        body: dict[str, Any] = {"config": config, "secrets": secrets}
        if message is not None:
            body["message"] = message

        self._control("/task/update-config", body)
