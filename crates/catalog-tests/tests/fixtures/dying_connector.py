#!/usr/bin/env python3
"""A `local:` derive connector that dies on command.

It exists to inject the two startup faults a `DerivationSession` must survive
without hanging the run or aborting the process, neither of which any
well-behaved connector produces:

  * `{"die": "before_opened"}` — exit while handling Open, so the shard fails
    before it is ever ready. This is the shape of a connector that can't start
    at all (a bad image, a missing binary).
  * `{"die": "after_opened"}` — emit Opened, then exit. The shard reports ready
    and the session dies immediately after, in the window where neither the
    commit nor the ResetDone channel can observe it.

Speaks newline-delimited JSON on stdin/stdout (`protobuf: false`).
Only Validate and Open are implemented: nothing here ever reaches a Read.
"""

import json
import sys


def main() -> int:
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
            config = open_config(request["open"])

            if config.get("die") == "before_opened":
                fail("dying before Opened, as configured")
            emit({"opened": {}})

            if config.get("die") == "after_opened":
                fail("dying after Opened, as configured")

        else:
            fail(f"unexpected request {request}")

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


def fail(message: str) -> None:
    sys.stderr.write(message + "\n")
    sys.stderr.flush()
    sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
