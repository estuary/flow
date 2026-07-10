# AG-UI in agent-api — validation findings

Research spike validating "option 2" from the CopilotKit-backend discussion:
serve the AG-UI protocol directly from agent-api, run tools in the browser,
and reduce the server to policy enforcement + one proxied inference call.

**Verdict: viable, with one contractual (not technical) caveat.** Every layer
was validated in running code against the official client library. The caveat
is CopilotKit's licensing stance on the production frontend prop — see
"Licensing" below.

## What was validated, and how

| Claim | Evidence |
|---|---|
| The AG-UI wire protocol is small and implementable exactly | Contract extracted from `@ag-ui/core@0.0.57` (the zod schemas are the de-facto spec; the prose docs lag and are wrong in places). Full Rust implementation is ~1,700 lines including tests. |
| A hand-rolled Rust server satisfies the official client | `crates/agui/interop`: 9/9 scenarios pass driving `@ag-ui/client@0.0.57` against `cargo run -p agui --example serve`. **Zero protocol fixes were needed after the initial implementation.** |
| The client really does own the agentic loop | Interop scenario 3: tool-only run → client executes the tool → client re-POSTs full history (assistant `toolCalls` + `role:"tool"` result) → server translates it into a valid Anthropic continuation. Server holds no conversation state. |
| Anthropic streaming translates cleanly to AG-UI | Unit fixtures: `content_block_*`/`text_delta`/`input_json_delta`/`thinking_delta`/`message_delta` → AG-UI event sequences, snapshot-tested. Extended thinking maps to first-class `REASONING_*` events. |
| agent-api's machinery covers the policy needs | `POST /api/v1/agui` sits behind the standard `Envelope` JWT extractor; the handler is the single choke point for tenant disables, prefix capability checks, quota reservation, classifiers, model routing, and audit (stubbed `authorize_agentic` documents each). Zero new dependencies — axum 0.8 SSE and reqwest streaming were already in the tree. |
| The authenticated route works end-to-end | `#[sqlx::test]` through the real router: 401 without a token; with a minted Supabase-style JWT, 200 + a complete AG-UI SSE stream. (Verification in flight at this commit; finalized in the follow-up commit.) |
| CopilotKit React works without their runtime | CopilotKit ≥1.50 ("v2" API) officially supports direct `HttpAgent` connections (`selfManagedAgents` / `agents__unsafe_dev_only` props). `useFrontendTool` tools ride in `RunAgentInput.tools`; `@copilotkit/core` executes handlers and auto-continues the loop; HITL via `useHumanInTheLoop`. Supabase JWT attaches via `HttpAgent({headers})`. |

## Protocol facts worth knowing (empirically probed)

- SSE framing is `data: {json}\n\n` only; **CRLF line endings break the client's
  parser** (bare `\n` required). Comments/`event:` lines are tolerated.
- The client's `verifyEvents` state machine strictly enforces lifecycle:
  `RUN_STARTED` first; every text message / tool call / step closed before
  `RUN_FINISHED` (which must carry `threadId`/`runId`); nothing after a
  terminal event. Unknown event *types* kill the run; unknown *fields* are fine.
- `RUN_ERROR` is "soft": the client's `runAgent()` resolves, keeps partial
  text, and surfaces the error via subscriber callbacks. Pre-stream failures
  should be plain HTTP errors (status + JSON body are surfaced); mid-stream
  failures should be `RUN_ERROR`.
- `TOOL_CALL_START.parentMessageId` merges the call into an open text message
  (one assistant message with content + toolCalls, matching Anthropic's shape);
  without it the call becomes a standalone assistant message. Both round-trip;
  the server merges consecutive assistant messages when rebuilding Anthropic
  turns.
- Cancellation is connection teardown (client aborts the fetch). Dropping the
  SSE stream drops the Anthropic call in the Rust server for free.
- `RunAgentInput.context` carries `useAgentContext` readables — **the server
  must fold them into the system prompt** (implemented) or they silently no-op.
- Reasoning: AG-UI has first-class `REASONING_*` events and `encryptedValue`
  fields designed to round-trip provider thinking signatures. v0 runs with
  thinking off; enabling it later has a protocol path (see risks).

## Licensing

- **AG-UI protocol + SDKs: MIT throughout** (spec, TS/Python/community SDKs,
  docs). Maintained by CopilotKit with a public working group. No gating.
- **CopilotKit React packages: MIT** (`react-core`, `react-ui`, `core`,
  `runtime`). `@copilotkit/core` and `react-ui` are telemetry-free;
  `react-core` bundles `@scarf/scarf` (npm install-time ping only, disable
  with `SCARF_ANALYTICS=false`); segment analytics lives only in the runtime
  package we would not ship.
- **The caveat:** `selfManagedAgents` — the documented production prop for
  connecting a frontend to a self-managed AG-UI backend — is described as part
  of CopilotKit's paid "Enterprise Intelligence" offering ("talk to an engineer
  about licensing for production use"). The sibling `agents__unsafe_dev_only`
  is unrestricted but marked not-for-production. Code enforcement is a soft
  warning banner (no key check, no functional block), so the gate is
  contractual/open-core, not technical. Notably this gate attaches to the
  *frontend*, so it applies to option 1 (hosting their runtime) just as much —
  it is not a reason to prefer their runtime. Action: get CopilotKit's written
  position before production; budget for a commercial license either way.
- Also gated behind their paid tier: durable thread persistence, conversation
  history, inspector/observability. In direct mode, history is client-side;
  if we want durable threads we build them ourselves (we own the endpoint, so
  persisting transcripts in the control plane is straightforward).

## What is NOT viable / risks

1. **Protocol is pre-1.0 with no version negotiation on the wire.** The TS
   zod schemas are the spec; `THINKING_*` events are already deprecated for
   removal in 1.0. Mitigation: the interop harness pins `@ag-ui/client@0.0.57`
   and will catch drift when the pin is bumped; budget occasional catch-up work.
2. **CopilotKit v2 direct mode is <1 year old and churning.** Known open issues:
   stale re-render after tool follow-ups (in-place `agent.messages` mutation),
   `HttpAgent` registration type friction, an old report of Authorization
   headers not propagating in some setups (test in our UI), and token refresh
   requiring HttpAgent reconstruction.
3. **Extended thinking + client-side tools needs the signature round-trip.**
   Anthropic requires thinking blocks (with signatures) replayed on tool
   continuations. AG-UI has the fields for it (`REASONING_ENCRYPTED_VALUE`,
   `encryptedValue`), but whether CopilotKit's client preserves them across the
   loop is unverified. v0 ships with thinking off; verify before enabling.
4. **The community Rust SDK (`ag-ui-core` 0.1.0) is not usable** — it's stuck
   on the original 16 events (no reasoning/interrupts/chunks). Hand-rolled
   types (done) are the right call.
5. **Server-side tools (MCP, retrieval) are out of scope for this shape.** If
   agent-api ever needs to execute tools itself mid-run, the run loop grows an
   in-server tool loop — the protocol supports it (`TOOL_CALL_RESULT`), but
   that's a different, bigger server. The client-side-tools MVP does not
   foreclose it.

## What's on this branch

- `crates/agui` — pure protocol crate: types, events, SSE codec, provider
  trait, Anthropic translation, scripted mock provider, run state machine,
  standalone example server. 18 Rust tests (serde fixtures captured from the
  official client, insta snapshots of full event sequences, Anthropic SSE
  translation fixtures, an HTTP end-to-end test).
- `crates/agui/interop` — Node harness driving the official `@ag-ui/client`
  against the example server; 9 scenarios (`npm install && npm test`).
- `crates/control-plane-api`: `POST /api/v1/agui` behind `Envelope` auth with
  the documented `authorize_agentic` stub; `App.agui_provider`; sqlx tests for
  401/200-SSE through the real router.
- `crates/agent`: `--anthropic-api-key`/`ANTHROPIC_API_KEY` wires up the live
  Anthropic provider.
- `DESIGN.md` (architecture + wire contract), this file.

Not yet exercised: a live Anthropic call (no API key in the research
environment — the translation layer is fixture-tested; run
`ANTHROPIC_API_KEY=... cargo run -p agui --example serve` and point the
interop harness or a CopilotKit dev app at it), and a real CopilotKit browser
app against the endpoint (next obvious step).

## Suggested next steps

1. Point a CopilotKit v2 dev app (`agents__unsafe_dev_only` + `HttpAgent` with
   a Supabase JWT) at `/api/v1/agui` in a dev stack; exercise `useFrontendTool`
   and `useAgentContext` for real.
2. One live-key smoke run of the Anthropic provider.
3. Open the licensing conversation with CopilotKit re: `selfManagedAgents`.
4. Fill in `authorize_agentic`: tenant-level disable flag, prefix capability,
   quota reservation keyed on `RUN_FINISHED.result.usage`.
5. Decide thread-persistence stance (client-only vs. control-plane transcript
   store) — cheap to defer, the server is stateless either way.
