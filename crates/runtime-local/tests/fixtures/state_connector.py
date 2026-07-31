#!/usr/bin/env python3
"""A `local:` capture connector which advances a cursor in its connector state.

It exists to exercise the connector-state round trip carried by `SessionLoop`:
`initial_connector_state_json` in, and `Stopped.connector_state_json` out.

On Open it reads `state.cursor` (zero if absent), then emits one document plus
one merge-patch checkpoint per configured `transactions`, bumping the cursor by
one each time, and exits. So the final reduced state is the base document with
`cursor` advanced by exactly `transactions` — a value that can only be right if
the seed reached Open, every patch reduced atop it, and the state reported at
`Stopped` is the *last* committed transaction's.

Speaks newline-delimited JSON on stdin/stdout (`protobuf: false`).
"""

import json
import sys

PROTOCOL = 3032023


def main() -> int:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        request = json.loads(line)

        if "spec" in request:
            emit({"spec": {
                "protocol": PROTOCOL,
                "configSchema": {"type": "object"},
                "resourceConfigSchema": {"type": "object"},
                "documentationUrl": "https://estuary.dev",
            }})

        elif "validate" in request:
            # One Validated binding per requested binding, or validation
            # rejects the response as a binding-count mismatch.
            emit({"validated": {
                "bindings": [
                    {"resourcePath": [resource_name(b)]}
                    for b in request["validate"].get("bindings") or []
                ],
            }})

        elif "apply" in request:
            # A no-op Apply (no state patch) converges the runtime's apply loop
            # on its first iteration.
            emit({"applied": {}})

        elif "open" in request:
            run_session(request["open"])
            return 0  # EOF ends the session.

        else:
            fail(f"unexpected request {request}")

    return 0


def run_session(open_request: dict) -> None:
    state = open_request.get("state") or {}
    cursor = state.get("cursor") or 0
    transactions = (
        (open_request.get("capture") or {}).get("config") or {}
    ).get("transactions") or 1

    emit({"opened": {}})

    for i in range(transactions):
        cursor += 1
        emit({"captured": {"binding": 0, "doc": {"id": f"doc-{cursor}"}}})
        # A merge patch, so the seeded base document's other fields survive.
        emit({"checkpoint": {"state": {
            "updated": {"cursor": cursor},
            "mergePatch": True,
        }}})


def resource_name(binding: dict) -> str:
    return (binding.get("resourceConfig") or {}).get("name") or "resource"


def emit(response: dict) -> None:
    sys.stdout.write(json.dumps(response) + "\n")
    sys.stdout.flush()


def fail(message: str) -> None:
    sys.stderr.write(message + "\n")
    sys.stderr.flush()
    sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
