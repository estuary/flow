# AG-UI client interop harness

Drives the **official** [`@ag-ui/client`](https://www.npmjs.com/package/@ag-ui/client)
`HttpAgent` (v0.0.57) against the Rust AG-UI server in this crate and asserts on
the **client's own reconstructed view** of each run (`agent.messages`,
`result.newMessages`, `result.result`) rather than the raw SSE bytes. Passing
here is evidence that the Rust server's wire output is well-formed enough for the
reference client to rebuild the intended messages and state end-to-end.

## What it verifies

`interop.mjs` runs nine scenarios, each with a fresh `HttpAgent`:

1. **text-only run** — default (unscripted) mock yields one assistant message and
   a `result.stopReason`.
2. **text + tool call** — the tool call is `parentMessageId`-merged into the
   assistant message, with accumulated JSON arguments intact.
3. **tool-only run + continuation** — a standalone tool-call message, then a
   follow-up run that re-submits the assistant `toolCalls` plus a `tool` result
   message; asserts the server ACCEPTS it (HTTP 200) and resolves to text.
4. **two tool calls in one run** — both surface with correct names/args.
5. **reasoning + text** — the `reasoning` message precedes the assistant message.
6. **mid-stream provider error** — `runAgent` resolves (RUN_ERROR is
   non-throwing in this client), partial text is preserved, and a subscriber
   captures the error message.
7. **abortRun()** — abort never throws and the agent stays usable. (The mock
   emits its whole script with no inter-event delay, so a deterministic
   *mid-stream* interception is not possible; the abort races an instant stream.)
8. **frontend tools transmission** — a full tool definition is transmitted and
   accepted (HTTP 200 = the Rust types deserialized it).
9. **multi-turn memory** — the second POST carries the full prior history.

A tiny in-process HTTP proxy sits between the client and the server so the
harness can assert each POST returned **HTTP 200** and inspect the exact request
bodies (used for the continuation and multi-turn history checks).

## Running

```bash
# From the repo root, build the server once (fast startup thereafter):
cargo build -p agui --example serve

cd crates/agui/interop
npm install
npm test          # == node interop.mjs
```

The harness spawns `../../../target/debug/examples/serve` on an ephemeral port
(override with `AGUI_SERVE_BIN`), waits for it to accept connections, runs the
scenarios, prints a PASS/FAIL table, kills the server, and exits non-zero on any
failure. A process-level `unhandledRejection` guard swallows the secondary
undici `terminated` rejection that can follow a socket drop.
