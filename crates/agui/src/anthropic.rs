//! Anthropic Messages API backend.
//!
//! Two concerns, kept separate: [`build_request_body`] and [`translate_frame`]
//! are pure translation (unit-tested with canned fixtures), while
//! [`stream_anthropic`] owns all HTTP IO and simply feeds decoded SSE frames
//! through the translator onto a channel.

use crate::provider::{ContentBlock, ImageSource, ProviderEvent, ProviderMessage, ProviderRequest};
use anyhow::Context;

const DEFAULT_MODEL: &str = "claude-opus-4-8";
const DEFAULT_MAX_TOKENS: u32 = 8192;
const ANTHROPIC_VERSION: &str = "2023-06-01";
const DEFAULT_BASE_URL: &str = "https://api.anthropic.com";

pub struct AnthropicProvider {
    http: reqwest::Client,
    api_key: String,
    base_url: String,
}

impl AnthropicProvider {
    pub fn new(api_key: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            api_key,
            base_url: DEFAULT_BASE_URL.to_string(),
        }
    }

    /// Override the API base URL (used by interop harnesses and local stubs).
    pub fn with_base_url(mut self, base_url: String) -> Self {
        self.base_url = base_url.trim_end_matches('/').to_string();
        self
    }
}

impl crate::provider::Provider for AnthropicProvider {
    fn run(
        &self,
        request: ProviderRequest,
    ) -> futures::stream::BoxStream<'static, anyhow::Result<ProviderEvent>> {
        use futures::StreamExt;

        let body = build_request_body(&request);
        let http = self.http.clone();
        let api_key = self.api_key.clone();
        let url = format!("{}/v1/messages", self.base_url);

        // A bounded channel decouples the IO task from the consumer, applying
        // backpressure onto the response stream when the client reads slowly.
        let (tx, rx) = tokio::sync::mpsc::channel::<anyhow::Result<ProviderEvent>>(16);
        tokio::spawn(async move { stream_anthropic(http, url, api_key, body, tx).await });

        futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        })
        .boxed()
    }
}

/// Build the Anthropic Messages API request body from a normalized request.
pub(crate) fn build_request_body(request: &ProviderRequest) -> serde_json::Value {
    let model = request
        .model
        .clone()
        .unwrap_or_else(|| DEFAULT_MODEL.to_string());

    let mut body = serde_json::json!({
        "model": model,
        "max_tokens": DEFAULT_MAX_TOKENS,
        "stream": true,
        "messages": request.messages.iter().map(message_to_json).collect::<Vec<_>>(),
    });
    let map = body.as_object_mut().expect("json object");

    if let Some(system) = &request.system {
        map.insert(
            "system".to_string(),
            serde_json::Value::String(system.clone()),
        );
    }
    if !request.tools.is_empty() {
        let tools = request.tools.iter().map(tool_to_json).collect::<Vec<_>>();
        map.insert("tools".to_string(), serde_json::Value::Array(tools));
    }
    body
}

fn message_to_json(message: &ProviderMessage) -> serde_json::Value {
    let (role, blocks) = match message {
        ProviderMessage::User { content } => ("user", content),
        ProviderMessage::Assistant { content } => ("assistant", content),
    };
    serde_json::json!({
        "role": role,
        "content": blocks.iter().map(block_to_json).collect::<Vec<_>>(),
    })
}

fn block_to_json(block: &ContentBlock) -> serde_json::Value {
    match block {
        ContentBlock::Text(text) => serde_json::json!({"type": "text", "text": text}),
        ContentBlock::Image(source) => {
            serde_json::json!({"type": "image", "source": source_to_json(source)})
        }
        ContentBlock::ToolUse { id, name, input } => {
            serde_json::json!({"type": "tool_use", "id": id, "name": name, "input": input})
        }
        ContentBlock::ToolResult {
            tool_use_id,
            content,
            is_error,
        } => {
            let mut value = serde_json::json!({
                "type": "tool_result",
                "tool_use_id": tool_use_id,
                "content": content,
            });
            // Anthropic treats a missing `is_error` as false; only emit when set.
            if *is_error {
                value
                    .as_object_mut()
                    .expect("json object")
                    .insert("is_error".to_string(), serde_json::Value::Bool(true));
            }
            value
        }
    }
}

fn source_to_json(source: &ImageSource) -> serde_json::Value {
    match source {
        ImageSource::Base64 { media_type, data } => {
            serde_json::json!({"type": "base64", "media_type": media_type, "data": data})
        }
        ImageSource::Url { url } => serde_json::json!({"type": "url", "url": url}),
    }
}

fn tool_to_json(tool: &crate::types::Tool) -> serde_json::Value {
    let mut value = serde_json::json!({"name": tool.name, "input_schema": tool.parameters});
    if !tool.description.is_empty() {
        value.as_object_mut().expect("json object").insert(
            "description".to_string(),
            serde_json::Value::String(tool.description.clone()),
        );
    }
    value
}

/// Mutable translation state threaded across a single Anthropic response.
/// Tracks the kind of each open content block (by index) so `content_block_stop`
/// can emit the right close event, and accumulates the final stop reason/usage.
#[derive(Default)]
pub(crate) struct AnthropicStreamState {
    block_kinds: std::collections::HashMap<i64, BlockKind>,
    stop_reason: Option<String>,
    usage: serde_json::Value,
}

#[derive(Clone, Copy)]
enum BlockKind {
    Text,
    ToolUse,
    Thinking,
}

/// Translate one Anthropic SSE frame into zero or more [`ProviderEvent`]s,
/// updating `state`. An `event: error` frame becomes an `Err`.
pub(crate) fn translate_frame(
    frame: &crate::sse::SseFrame,
    state: &mut AnthropicStreamState,
) -> anyhow::Result<Vec<ProviderEvent>> {
    if frame.data.is_empty() {
        return Ok(Vec::new());
    }
    let value: serde_json::Value = serde_json::from_str(&frame.data)
        .with_context(|| format!("parsing Anthropic SSE data: {}", frame.data))?;
    let event_type = value
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();

    match event_type {
        "message_start" => {
            if let Some(usage) = value.pointer("/message/usage") {
                state.usage = usage.clone();
            }
            Ok(Vec::new())
        }
        "content_block_start" => {
            let index = block_index(&value);
            let block = value.get("content_block");
            let block_type = block
                .and_then(|b| b.get("type"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            match block_type {
                "text" => {
                    state.block_kinds.insert(index, BlockKind::Text);
                    Ok(vec![ProviderEvent::TextStart])
                }
                "tool_use" => {
                    let id = block
                        .and_then(|b| b.get("id"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let name = block
                        .and_then(|b| b.get("name"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    state.block_kinds.insert(index, BlockKind::ToolUse);
                    Ok(vec![ProviderEvent::ToolCallStart { id, name }])
                }
                "thinking" => {
                    state.block_kinds.insert(index, BlockKind::Thinking);
                    Ok(vec![ProviderEvent::ReasoningStart])
                }
                // redacted_thinking and any future block types are ignored in v0.
                _ => Ok(Vec::new()),
            }
        }
        "content_block_delta" => {
            let delta = value.get("delta");
            let delta_type = delta
                .and_then(|d| d.get("type"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            match delta_type {
                "text_delta" => Ok(vec![ProviderEvent::TextDelta(delta_field(delta, "text"))]),
                "input_json_delta" => Ok(vec![ProviderEvent::ToolCallArgsDelta(delta_field(
                    delta,
                    "partial_json",
                ))]),
                "thinking_delta" => Ok(vec![ProviderEvent::ReasoningDelta(delta_field(
                    delta, "thinking",
                ))]),
                // signature_delta carries the thinking signature; ignored in v0.
                _ => Ok(Vec::new()),
            }
        }
        "content_block_stop" => {
            let index = block_index(&value);
            match state.block_kinds.remove(&index) {
                Some(BlockKind::Text) => Ok(vec![ProviderEvent::TextEnd]),
                Some(BlockKind::ToolUse) => Ok(vec![ProviderEvent::ToolCallEnd]),
                Some(BlockKind::Thinking) => Ok(vec![ProviderEvent::ReasoningEnd]),
                None => Ok(Vec::new()),
            }
        }
        "message_delta" => {
            if let Some(reason) = value
                .pointer("/delta/stop_reason")
                .and_then(serde_json::Value::as_str)
            {
                state.stop_reason = Some(reason.to_string());
            }
            if let Some(usage) = value.get("usage") {
                merge_usage(&mut state.usage, usage);
            }
            Ok(Vec::new())
        }
        "message_stop" => {
            let stop_reason = state
                .stop_reason
                .take()
                .unwrap_or_else(|| "end_turn".to_string());
            let usage = std::mem::replace(&mut state.usage, serde_json::Value::Null);
            Ok(vec![ProviderEvent::Finished { stop_reason, usage }])
        }
        "error" => {
            let message = value
                .pointer("/error/message")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown Anthropic error");
            Err(anyhow::anyhow!("Anthropic stream error: {message}"))
        }
        // ping and any unrecognized events are no-ops.
        _ => Ok(Vec::new()),
    }
}

fn block_index(value: &serde_json::Value) -> i64 {
    value
        .get("index")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0)
}

fn delta_field(delta: Option<&serde_json::Value>, key: &str) -> String {
    delta
        .and_then(|d| d.get(key))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string()
}

/// Shallow-merge `source` usage counters into `target`, replacing wholesale if
/// either side is not a JSON object.
fn merge_usage(target: &mut serde_json::Value, source: &serde_json::Value) {
    match (target.as_object_mut(), source.as_object()) {
        (Some(target_map), Some(source_map)) => {
            for (key, value) in source_map {
                target_map.insert(key.clone(), value.clone());
            }
        }
        _ => *target = source.clone(),
    }
}

/// Execute the request and forward translated events onto `tx`. All errors are
/// delivered in-band as `Err` items so the run loop can surface them.
async fn stream_anthropic(
    http: reqwest::Client,
    url: String,
    api_key: String,
    body: serde_json::Value,
    tx: tokio::sync::mpsc::Sender<anyhow::Result<ProviderEvent>>,
) {
    use futures::StreamExt;

    let response = http
        .post(&url)
        .header("x-api-key", api_key)
        .header("anthropic-version", ANTHROPIC_VERSION)
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await;

    let response = match response {
        Ok(response) => response,
        Err(error) => {
            let _ = tx
                .send(Err(
                    anyhow::Error::new(error).context("Anthropic request failed")
                ))
                .await;
            return;
        }
    };

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        let message = extract_error_message(&text).unwrap_or(text);
        let _ = tx
            .send(Err(anyhow::anyhow!(
                "Anthropic API error ({status}): {message}"
            )))
            .await;
        return;
    }

    let mut bytes = response.bytes_stream();
    let mut decoder = crate::sse::SseDecoder::new();
    let mut state = AnthropicStreamState::default();

    while let Some(chunk) = bytes.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                let _ = tx
                    .send(Err(
                        anyhow::Error::new(error).context("Anthropic stream read failed")
                    ))
                    .await;
                return;
            }
        };
        for frame in decoder.push(chunk.as_ref()) {
            match translate_frame(&frame, &mut state) {
                Ok(events) => {
                    for event in events {
                        if tx.send(Ok(event)).await.is_err() {
                            return; // consumer dropped; stop early.
                        }
                    }
                }
                Err(error) => {
                    let _ = tx.send(Err(error)).await;
                    return;
                }
            }
        }
    }
}

fn extract_error_message(body: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()?
        .pointer("/error/message")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Run a complete canned SSE byte buffer through the decoder + translator,
    /// collecting emitted events. A translation error stops collection and is
    /// returned as the second tuple element (mirroring the run loop).
    fn collect_events(bytes: &[u8]) -> (Vec<ProviderEvent>, Option<String>) {
        let mut decoder = crate::sse::SseDecoder::new();
        let mut state = AnthropicStreamState::default();
        let mut events = Vec::new();
        for frame in decoder.push(bytes) {
            match translate_frame(&frame, &mut state) {
                Ok(mut batch) => events.append(&mut batch),
                Err(error) => return (events, Some(format!("{error:#}"))),
            }
        }
        (events, None)
    }

    const TEXT_ONLY: &[u8] = b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"role\":\"assistant\",\"usage\":{\"input_tokens\":12,\"output_tokens\":1}}}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: ping\ndata: {\"type\":\"ping\"}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" there\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";

    const TEXT_AND_TOOL: &[u8] = b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":20,\"output_tokens\":1}}}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Let me check.\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_abc\",\"name\":\"get_weather\",\"input\":{}}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"loc\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"ation\\\":\\\"Boston\\\"}\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":1}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":32}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";

    const THINKING_AND_TEXT: &[u8] = b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"Weather in Boston.\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"c2ln\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"It is sunny.\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":1}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":9}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";

    const MID_STREAM_ERROR: &[u8] = b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":5}}}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Partial\"}}\n\nevent: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n";

    #[test]
    fn translate_text_only() {
        insta::assert_debug_snapshot!(collect_events(TEXT_ONLY));
    }

    #[test]
    fn translate_text_and_tool_call() {
        insta::assert_debug_snapshot!(collect_events(TEXT_AND_TOOL));
    }

    #[test]
    fn translate_thinking_and_text() {
        insta::assert_debug_snapshot!(collect_events(THINKING_AND_TEXT));
    }

    #[test]
    fn translate_mid_stream_error() {
        insta::assert_debug_snapshot!(collect_events(MID_STREAM_ERROR));
    }

    #[test]
    fn request_body_folds_system_context_and_tools() {
        // Exercises: system join (system + developer), context folding, tool
        // input_schema mapping, assistant tool_use arguments parsing, and
        // tool_result error mapping.
        let input: crate::types::RunAgentInput = serde_json::from_str(
            r#"{
                "threadId": "t-body",
                "runId": "r-body",
                "state": {},
                "forwardedProps": {},
                "context": [
                    {"description": "current page", "value": "/dashboard"},
                    {"description": "selected rows", "value": 3}
                ],
                "tools": [
                    {"name": "get_weather", "description": "Look up weather",
                     "parameters": {"type": "object", "properties": {"location": {"type": "string"}}}}
                ],
                "messages": [
                    {"id": "s1", "role": "system", "content": "You are helpful."},
                    {"id": "d1", "role": "developer", "content": "Prefer metric units."},
                    {"id": "u1", "role": "user", "content": "Weather in Boston?"},
                    {"id": "a1", "role": "assistant", "content": "Checking.",
                     "toolCalls": [{"id": "toolu_1", "type": "function",
                        "function": {"name": "get_weather", "arguments": "{\"location\":\"Boston\"}"}}]},
                    {"id": "t1", "role": "tool", "content": "unavailable",
                     "toolCallId": "toolu_1", "error": "upstream 503"}
                ]
            }"#,
        )
        .unwrap();

        let request = crate::run::translate_input(&input);
        insta::assert_json_snapshot!(build_request_body(&request));
    }

    #[test]
    fn request_body_merges_consecutive_assistant_messages() {
        // A continuation turn may carry a text assistant message and a
        // tool-call assistant message separately; Anthropic requires them
        // merged into one assistant turn immediately preceding the tool_result.
        let input: crate::types::RunAgentInput = serde_json::from_str(
            r#"{
                "threadId": "t-merge",
                "runId": "r-merge",
                "state": {},
                "forwardedProps": {},
                "context": [],
                "tools": [],
                "messages": [
                    {"id": "u1", "role": "user", "content": "Weather?"},
                    {"id": "a1", "role": "assistant", "content": "Let me check."},
                    {"id": "a2", "role": "assistant",
                     "toolCalls": [{"id": "toolu_9", "type": "function",
                        "function": {"name": "get_weather", "arguments": "{\"location\":\"Boston\"}"}}]},
                    {"id": "t1", "role": "tool", "content": "72F", "toolCallId": "toolu_9"}
                ]
            }"#,
        )
        .unwrap();

        let request = crate::run::translate_input(&input);
        insta::assert_json_snapshot!(build_request_body(&request));
    }
}
