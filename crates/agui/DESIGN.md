# AG-UI endpoint for agent-api — design (research spike)

Validates "option 2" from the CopilotKit-backend discussion: the frontend speaks
AG-UI (HTTP POST + SSE of typed JSON events) directly to agent-api, tools execute
in the browser so the agentic loop lives client-side, and the server's only jobs
are policy enforcement (authn/authz/quota/classifiers/routing) and translating a
single inference call to the LLM provider.

## Wire contract (ground truth: @ag-ui/core@0.0.57)

The authoritative schemas were extracted from the published `@ag-ui/core` zod
source (MIT), and validated empirically by running the official `@ag-ui/client`
HttpAgent against a stub server. Key facts:

- Request: `POST` with `content-type: application/json`, `accept: text/event-stream`.
  Custom headers (e.g. `authorization: Bearer <supabase-jwt>`) pass through.
- Response: `text/event-stream` of `data: <json>\n\n` frames (no `event:` field).
- `RunAgentInput` (all camelCase): `threadId`, `runId`, `state`, `messages`,
  `tools`, `context`, `forwardedProps`, optional `parentRunId`, `resume`.
- `Message` is a discriminated union on `role`:
  `developer | system | assistant | user | tool | activity | reasoning`.
  Assistant carries optional `toolCalls: [{id, type:"function", function:{name,
  arguments:string}}]`. Tool carries `toolCallId`, `content`, optional `error`.
  User `content` is a string or multimodal input-part array.
- `Tool`: `{name, description, parameters /*JSON Schema*/, metadata?}`.
- Events: discriminated union on SCREAMING_SNAKE `type` — lifecycle
  (`RUN_STARTED|RUN_FINISHED|RUN_ERROR|STEP_*`), text
  (`TEXT_MESSAGE_START|CONTENT|END` + `CHUNK`), tools
  (`TOOL_CALL_START|ARGS|END|RESULT` + `CHUNK`), reasoning
  (`REASONING_START|REASONING_MESSAGE_START|CONTENT|END|REASONING_END`,
  `REASONING_ENCRYPTED_VALUE`), state (`STATE_SNAPSHOT|STATE_DELTA|MESSAGES_SNAPSHOT`),
  misc (`RAW|CUSTOM|ACTIVITY_*`). `THINKING_*` variants are deprecated aliases.
- The client itself accumulates deltas into messages and runs the tool loop:
  after a run ends with tool calls, the client executes frontend tools and
  submits a NEW run whose `messages` include the assistant `toolCalls` and
  `role:"tool"` results. Server holds no conversation state.
- `REASONING_ENCRYPTED_VALUE` + `encryptedValue` on assistant/reasoning/tool-call
  messages exist to round-trip opaque provider reasoning signatures (maps 1:1 to
  Anthropic extended-thinking signatures/redacted thinking).

## Architecture

Two layers, cleanly split so policy stays in agent-api and protocol stays pure:

```
crates/agui                    # pure library: protocol + translation. No DB, no auth.
  src/types.rs                 #   RunAgentInput, Message, Tool, ... (serde, camelCase)
  src/events.rs                #   AG-UI Event enum (serde tag = "type")
  src/sse.rs                   #   SSE framing: encode AG-UI events; parse provider SSE
  src/provider.rs              #   trait Provider: run(ProviderRequest) -> stream of
                               #   normalized ProviderEvent (TextDelta, ToolCallStart/
                               #   ArgsDelta/End, ThinkingDelta, Finished, Error)
  src/anthropic.rs             #   RunAgentInput -> Messages API body; Anthropic SSE
                               #   (message_start/content_block_*/message_delta/...)
                               #   -> ProviderEvent
  src/mock.rs                  #   scripted provider; script injectable via
                               #   forwardedProps._mock for deterministic interop tests
  src/run.rs                   #   run state machine: RUN_STARTED, message/tool-call
                               #   open-close discipline, RUN_FINISHED / RUN_ERROR
  examples/serve.rs            #   standalone no-auth server (mock or live provider)
  interop/                     #   Node harness driving the official @ag-ui/client
                               #   against the Rust server

crates/control-plane-api
  src/server/public/agui.rs    # POST /api/v1/agui: Envelope auth -> policy hooks
                               # (tenant gating stub) -> delegate to agui::run
```

### Policy enforcement points (the reason option 2 wins)

The handler is where every cross-cutting requirement plugs in, all with
machinery agent-api already has:

1. **Authn**: `Envelope` extractor verifies the Supabase JWT (`env.claims()?`).
2. **Authz / tenant disable**: capability check against the authorization
   `Snapshot` (role_grants/user_grants); HIPAA/GDPR-style tenant disables become
   a directive or tenant-table flag consulted here. Stubbed as
   `authorize_agentic(snapshot, claims)` in the spike.
3. **Quota / cost attribution**: token usage arrives on the provider stream
   (`message_delta.usage`); the run loop surfaces it in `RUN_FINISHED.result`
   and to a server-side hook. Reservation-style quota fits before dispatch.
4. **Model routing / provider restrictions**: the handler picks the `Provider`
   per tenant/task; `forwardedProps` carries client hints, but the server decides.
5. **Classifiers / audit**: input messages are fully parsed server-side before
   dispatch; a classifier hook slots between parse and dispatch.

Pre-stream failures (auth, quota, bad input) return plain HTTP errors — the
client surfaces the body. Post-headers failures must be in-stream `RUN_ERROR`
(HTTP status is already committed).

### Provider translation (Anthropic)

- `system`/`developer` messages -> `system` string; `user` -> user turn
  (string or text parts; image parts translate to base64/url image blocks);
  `assistant` -> text + `tool_use` blocks (arguments parsed via serde_json);
  `tool` -> user turn with `tool_result` (id = `toolCallId`, `is_error` if set);
  `reasoning` -> skipped in v0 (see risks).
- `tools[]` -> Anthropic `tools` with `input_schema = parameters`.
- Streaming: `content_block_start(text)` -> `TEXT_MESSAGE_START`;
  `text_delta` -> `TEXT_MESSAGE_CONTENT`; `content_block_start(tool_use)` ->
  `TOOL_CALL_START` (with Anthropic's `toolu_` id, parented to the run's message);
  `input_json_delta.partial_json` -> `TOOL_CALL_ARGS`; `content_block_stop` ->
  `TEXT_MESSAGE_END`/`TOOL_CALL_END`; `thinking_delta` -> `REASONING_MESSAGE_CONTENT`;
  `message_delta` -> capture `stop_reason` + `usage`; `message_stop` ->
  `RUN_FINISHED` with `result: {stopReason, usage}`; `event: error` / transport
  failure -> `RUN_ERROR`.
- Default model `claude-opus-4-8`; `thinking` omitted in v0 (off on Opus 4.7/4.8),
  so no signature round-trip needed yet.

## v0 scope cuts (documented, not blockers)

- **Extended thinking replay**: with thinking enabled and client-side tools,
  Anthropic requires assistant thinking blocks (with signatures) to be replayed.
  AG-UI supports this (`REASONING_ENCRYPTED_VALUE`, `encryptedValue` fields) but
  the CopilotKit client's persistence of those fields needs verification. v0
  runs with thinking off.
- **Multimodal user input**: text passes through; image parts translate
  structurally but are untested against a live model.
- **`resume` / interrupts (HITL)**: protocol supports it; not needed for
  frontend-tool HITL (that flows through the normal tool loop).
- **Prompt caching / STATE_* / MESSAGES_SNAPSHOT**: not needed for the MVP shape.

## Testing strategy

1. Serde round-trips against fixtures captured from the official TS packages.
2. `insta` snapshot tests of complete SSE event sequences (mock provider) for:
   text run, tool-call run, multi-tool run, provider error mid-stream,
   tool-result continuation turn.
3. Anthropic translation unit tests: canned Anthropic SSE bytes -> AG-UI events.
4. Interop: Node harness runs the official `@ag-ui/client` against the Rust
   example server (mock provider), asserting the client's *own* view of
   the run (accumulated messages, tool loop, abort).
