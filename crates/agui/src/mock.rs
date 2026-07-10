//! Deterministic scripted provider for tests and the interop harness.
//!
//! The script is threaded from `RunAgentInput.forwardedProps._mock` (see
//! [`crate::provider::ProviderRequest::mock_script`]). It is a JSON array of
//! ops, each a single-key object:
//!
//! ```json
//! [
//!   {"text": "Hello world"},
//!   {"reasoning": "thinking..."},
//!   {"toolCall": {"name": "get_weather", "args": "{\"location\":\"Boston\"}"}},
//!   {"error": "boom"},
//!   {"finish": {"stopReason": "end_turn"}}
//! ]
//! ```
//!
//! Each `text`/`args`/`reasoning` string is emitted split into 2-3 deltas to
//! exercise streaming. When `_mock` is absent, a single canned text response is
//! produced. Tool-call ids are deterministic (`call_mock_{n}`) so SSE snapshots
//! are stable.

use crate::provider::{ProviderEvent, ProviderRequest};

pub struct MockProvider;

impl crate::provider::Provider for MockProvider {
    fn run(
        &self,
        request: ProviderRequest,
    ) -> futures::stream::BoxStream<'static, anyhow::Result<ProviderEvent>> {
        use futures::StreamExt;
        let ops = parse_script(request.mock_script.as_ref());
        futures::stream::iter(expand(ops)).boxed()
    }
}

enum MockOp {
    Text(String),
    ToolCall { name: String, args: String },
    Reasoning(String),
    Error(String),
    Finish { stop_reason: String },
}

fn parse_script(script: Option<&serde_json::Value>) -> Vec<MockOp> {
    let Some(serde_json::Value::Array(items)) = script else {
        return vec![
            MockOp::Text("Hello from the mock provider.".to_string()),
            MockOp::Finish {
                stop_reason: "end_turn".to_string(),
            },
        ];
    };
    items.iter().filter_map(parse_op).collect()
}

fn parse_op(value: &serde_json::Value) -> Option<MockOp> {
    if let Some(text) = value.get("text").and_then(serde_json::Value::as_str) {
        return Some(MockOp::Text(text.to_string()));
    }
    if let Some(call) = value.get("toolCall") {
        let name = call
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("tool")
            .to_string();
        let args = call
            .get("args")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("{}")
            .to_string();
        return Some(MockOp::ToolCall { name, args });
    }
    if let Some(reasoning) = value.get("reasoning").and_then(serde_json::Value::as_str) {
        return Some(MockOp::Reasoning(reasoning.to_string()));
    }
    if let Some(error) = value.get("error").and_then(serde_json::Value::as_str) {
        return Some(MockOp::Error(error.to_string()));
    }
    if let Some(finish) = value.get("finish") {
        let stop_reason = finish
            .get("stopReason")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("end_turn")
            .to_string();
        return Some(MockOp::Finish { stop_reason });
    }
    None
}

fn expand(ops: Vec<MockOp>) -> Vec<anyhow::Result<ProviderEvent>> {
    let mut events = Vec::new();
    let mut tool_call_seq = 0;

    for op in ops {
        match op {
            MockOp::Text(text) => {
                events.push(Ok(ProviderEvent::TextStart));
                for chunk in split_chunks(&text) {
                    events.push(Ok(ProviderEvent::TextDelta(chunk)));
                }
                events.push(Ok(ProviderEvent::TextEnd));
            }
            MockOp::ToolCall { name, args } => {
                tool_call_seq += 1;
                let id = format!("call_mock_{tool_call_seq}");
                events.push(Ok(ProviderEvent::ToolCallStart { id, name }));
                for chunk in split_chunks(&args) {
                    events.push(Ok(ProviderEvent::ToolCallArgsDelta(chunk)));
                }
                events.push(Ok(ProviderEvent::ToolCallEnd));
            }
            MockOp::Reasoning(text) => {
                events.push(Ok(ProviderEvent::ReasoningStart));
                for chunk in split_chunks(&text) {
                    events.push(Ok(ProviderEvent::ReasoningDelta(chunk)));
                }
                events.push(Ok(ProviderEvent::ReasoningEnd));
            }
            MockOp::Error(message) => {
                // An error is terminal for the provider stream.
                events.push(Err(anyhow::anyhow!(message)));
                return events;
            }
            MockOp::Finish { stop_reason } => {
                events.push(Ok(ProviderEvent::Finished {
                    stop_reason,
                    usage: serde_json::json!({}),
                }));
            }
        }
    }
    events
}

/// Split a string into 2-3 chunks on char boundaries, so streaming is
/// exercised. An empty string yields no chunks (no delta events).
fn split_chunks(text: &str) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }
    let chars: Vec<char> = text.chars().collect();
    let parts = chars.len().clamp(1, 3);
    let size = chars.len().div_ceil(parts);
    chars.chunks(size).map(|c| c.iter().collect()).collect()
}
