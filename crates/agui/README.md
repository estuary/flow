# agui

Pure library implementing an **AG-UI** protocol server endpoint: an HTTP POST of
a `RunAgentInput` becomes an SSE stream of typed AG-UI JSON events, proxying a
single inference call to an LLM provider. Tools run in the browser (the agentic
loop is client-side); this crate's only jobs are the protocol and a single
provider call. All policy — authn, authz, quota, routing — lives in the
embedding service (`control-plane-api`), which calls [`run`] + [`sse_response`]
after its checks pass. No DB, no auth here.

See `DESIGN.md` for the full design rationale and the AG-UI wire contract
(ground truth: `@ag-ui/core@0.0.57`).

## Key types and entry points

- `run::run(input, provider) -> Stream<Event>` — the run state machine. Emits
  `RUN_STARTED` first, brackets every text/tool-call/reasoning message with
  matching start/end ids, and finishes with exactly one terminal
  `RUN_FINISHED` / `RUN_ERROR`.
- `sse::sse_response(stream)` — wraps a stream of `Event`s as an axum SSE
  response (`data: {json}\n\n` frames, no `event:` line, 15s keep-alive).
- `provider::Provider` — a single inference call as a stream of normalized
  `ProviderEvent`s. Backends: `anthropic::AnthropicProvider` (Messages API,
  streaming) and `mock::MockProvider` (deterministic, scripted).
- `types` — `RunAgentInput`, `Message` (union on `role`), `Tool`, `ToolCall`.
- `events::Event` — the AG-UI output event union (tag = `type`,
  `SCREAMING_SNAKE_CASE`).

## Layout

| file | responsibility |
| --- | --- |
| `types.rs` | AG-UI request types (serde, `camelCase`, lenient defaults) |
| `events.rs` | AG-UI output `Event` union |
| `provider.rs` | provider-neutral `Provider` trait + request/event vocabulary |
| `anthropic.rs` | `ProviderRequest` -> Messages API body; Anthropic SSE -> `ProviderEvent` |
| `mock.rs` | scripted provider (`forwardedProps._mock`) |
| `sse.rs` | outbound SSE framing + inbound incremental SSE parser |
| `run.rs` | run state machine + `RunAgentInput` -> `ProviderRequest` translation |
| `examples/serve.rs` | standalone no-auth server (mock or live provider) |

## Non-obvious details

- **Deterministic ids.** Assistant/reasoning message ids are derived as
  `{runId}-msg-{n}`, not random UUIDs, so SSE output is snapshot-stable and the
  client's message accumulation is predictable.
- **`parentMessageId` on tool calls** is set only when a `TEXT_MESSAGE_START`
  was emitted this run (so the client merges the tool call into that assistant
  message). For a tool-only response it is omitted, and the client creates a
  standalone assistant message keyed by the tool-call id.
- **`context[]` folding.** Frontend `RunAgentInput.context` entries are appended
  to the Anthropic `system` prompt; dropping them would make frontend readables
  silently no-op.
- **Anthropic message merging.** Consecutive assistant AG-UI messages merge into
  one Anthropic assistant turn (text then `tool_use`), and consecutive tool
  results merge into one user turn — both required by Anthropic's ordering rules.
- **IO vs. translation are decomposed.** `anthropic::build_request_body` and
  `translate_frame` are pure and unit-tested with canned SSE fixtures;
  `stream_anthropic` owns all HTTP IO and feeds decoded frames through the
  translator over a channel.
- **Provider errors mid-stream** become an in-stream `RUN_ERROR` (the HTTP
  status is already committed once the SSE body has started); pre-stream failures
  in the embedding handler return plain HTTP errors instead.

## Try it

```bash
cargo run -p agui --example serve            # mock provider on :8137
ANTHROPIC_API_KEY=sk-... cargo run -p agui --example serve   # live Anthropic
```
