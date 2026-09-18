"""The three HTTP calls by which a connector manages its own credentials.

Separated from the capture loop because they are the only IO here which isn't
the protocol itself, and because they are the part a connector author is
actually copying. The reactor injects all three inputs as environment:

  FLOW_ROTATION_TOKEN         - an `AUTHORIZE | TASK_UPDATE` JWT naming this
                                task and the build it runs
  FLOW_CONTROL_API            - serves /task/set-secret and /task/update-config
  FLOW_CONFIG_ENCRYPTION_URL  - serves /secret/encrypt

Unset means "rotate in memory only": `flowctl preview` and tests hold no
data-plane key and have no control plane to rotate against. That is a supported
mode, not an error.

Wrapping is two hops on purpose. The connector posts its plaintext to
config-encryption, which returns a sops document under a KMS key only it holds,
and only that document goes to the control plane -- which therefore never sees
the plaintext of a secret it stores.
"""

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


class Unconfigured(Exception):
    """No rotation credentials were injected."""


class RouteError(Exception):
    """A route was unreachable, or answered with a non-200.

    Always recoverable by the caller: it keeps the new credential in memory and
    retries the same set on its next tick. What must never happen is a tight
    retry loop, or a connector which reports success it didn't have.
    """


class Rotation:
    """Credentials for rotating, read once from the environment."""

    def __init__(self) -> None:
        self.token = os.environ.get("FLOW_ROTATION_TOKEN", "")
        self.control_api = os.environ.get("FLOW_CONTROL_API", "")
        self.config_encryption = os.environ.get("FLOW_CONFIG_ENCRYPTION_URL", "")

    @property
    def enabled(self) -> bool:
        return bool(self.token and self.control_api and self.config_encryption)

    def _post(self, url: str, body: bytes, token: str | None) -> Any:
        request = urllib.request.Request(url, data=body, method="POST")
        request.add_header("content-type", "application/json")
        if token:
            request.add_header("authorization", f"Bearer {token}")

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

    def wrap(self, name: str, value: Any) -> Any:
        """Wrap `value` as the sops document of secret `name`.

        Unauthenticated: config-encryption will wrap anything for anyone. It's
        reading one back out which is authorized, and the document is bound to
        the name it was wrapped for -- sops MACs `name` even though it's stored
        in the clear -- so a document cannot be stored under another name.
        """
        if not self.enabled:
            raise Unconfigured()

        url = f"{self.config_encryption.rstrip('/')}/secret/encrypt"
        url = f"{url}?name={urllib.parse.quote(name, safe='')}"
        return self._post(url, json.dumps(value).encode(), None)

    def set_secret(self, name: str, document: Any) -> tuple[str, bool]:
        """Store a wrapped document, returning its `secretId` and whether it
        changed. Only a sibling of the task may be set: an image-rule secret
        belongs to the image's publisher and is updated by hand.
        """
        if not self.enabled:
            raise Unconfigured()

        body = json.dumps(
            {"name": name, "document": document, "token": self.token}
        ).encode()
        result = self._post(f"{self.control_api.rstrip('/')}/task/set-secret", body, None)

        return result.get("secretId", ""), bool(result.get("changed"))

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
        if not self.enabled:
            raise Unconfigured()

        body: dict[str, Any] = {
            "config": config,
            "secrets": secrets,
            "token": self.token,
        }
        if message is not None:
            body["message"] = message

        self._post(
            f"{self.control_api.rstrip('/')}/task/update-config",
            json.dumps(body).encode(),
            None,
        )
