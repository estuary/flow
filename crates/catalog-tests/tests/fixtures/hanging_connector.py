#!/usr/bin/env python3
"""A `local:` derive connector that stops answering on command.

Its sibling `dying_connector.py` injects faults a session must *notice*; this
one injects the faults a session can only ever notice by giving up, which is
what `catalog-tests`' timeouts exist for:

  * `{"hang": "open"}` — answer Spec and Validate (so the catalog still
    builds), then read Open and never answer it. This is the shape of a
    connector that never becomes ready: an image that boots but never speaks.
  * `{"hang": "transaction"}` — answer Opened, then read requests forever
    without ever publishing, flushing, or acknowledging. The session opens
    cleanly and then stalls mid-transaction, where neither a Reset nor a Stop
    can be answered either.

A stderr line names the selected hang before it begins, so the logs of a run
which then stalls say which fault it is sitting on.
"""

import json
import sys


def main() -> int:
    hang = None

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        request = json.loads(line)

        if "spec" in request:
            emit({"spec": {
                "protocol": 3032023,
                "configSchema": {"type": "object"},
                "resourceConfigSchema": {"type": "object"},
                "documentationUrl": "https://estuary.dev",
            }})

        elif "validate" in request:
            validate = request["validate"]
            # One Validated transform per requested transform, or validation
            # rejects the response as a binding-count mismatch.
            emit({"validated": {
                "transforms": [
                    {"readOnly": False} for _ in validate.get("transforms") or []
                ],
            }})

        elif "open" in request:
            hang = open_config(request["open"]).get("hang")
            log(f"hanging on {hang}, as configured")

            if hang == "open":
                continue  # Never answer: the session can only time out.
            emit({"opened": {}})

        # Every later request — Read, Flush, StartCommit, Reset — is read and
        # dropped on the floor, which is the whole point of `transaction`.

    return 0


# Open carries the derivation's collection spec, and thus the connector config.
# Validate is a separate connector invocation at build time, so there's nothing
# to carry forward from it.
def open_config(open_request: dict) -> dict:
    derivation = (open_request.get("collection") or {}).get("derivation") or {}
    return derivation.get("config") or {}


def emit(response: dict) -> None:
    sys.stdout.write(json.dumps(response) + "\n")
    sys.stdout.flush()


def log(message: str) -> None:
    sys.stderr.write(message + "\n")
    sys.stderr.flush()


if __name__ == "__main__":
    sys.exit(main())
