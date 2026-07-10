//! Protocol-level tests: `RunAgentInput` round-trips, full AG-UI event
//! sequences produced by [`agui::run`] against the mock provider, and one
//! end-to-end HTTP/SSE test. Message ids are derived from `runId`, so all
//! snapshots (including the raw SSE body) are deterministic.

use std::sync::Arc;

use agui::events::Event;
use futures::StreamExt;

/// Verbatim `RunAgentInput` captured from the official `@ag-ui/client`.
const FIRST_TURN: &str = r#"{"threadId":"t-1","runId":"r-1","tools":[{"name":"get_weather","description":"Get current weather for a location","parameters":{"type":"object","properties":{"location":{"type":"string"}},"required":["location"]}}],"context":[],"forwardedProps":{},"state":{},"messages":[{"id":"u1","role":"user","content":"Hi, what's the weather in Boston?"}]}"#;

/// A continuation turn: the client re-submits the assistant's tool call plus
/// the frontend-executed tool result.
const CONTINUATION_TURN: &str = r#"{"threadId":"t-1","runId":"r-2","tools":[{"name":"get_weather","description":"Get current weather for a location","parameters":{"type":"object","properties":{"location":{"type":"string"}},"required":["location"]}}],"context":[],"forwardedProps":{},"state":{},"messages":[{"id":"u1","role":"user","content":"Hi, what's the weather in Boston?"},{"id":"a1","role":"assistant","content":null,"toolCalls":[{"id":"toolu_1","type":"function","function":{"name":"get_weather","arguments":"{\"location\":\"Boston\"}"}}]},{"id":"t1","role":"tool","content":"{\"tempF\":72}","toolCallId":"toolu_1"}]}"#;

#[test]
fn deserializes_first_turn() {
    let parsed: agui::RunAgentInput = serde_json::from_str(FIRST_TURN).unwrap();
    assert_stable(&parsed);
    insta::assert_debug_snapshot!(parsed);
}

#[test]
fn deserializes_continuation_turn() {
    let parsed: agui::RunAgentInput = serde_json::from_str(CONTINUATION_TURN).unwrap();
    assert_stable(&parsed);
    insta::assert_debug_snapshot!(parsed);
}

/// Serialize -> deserialize must be a fixed point (Debug-equal), proving the
/// serde attributes round-trip.
fn assert_stable(parsed: &agui::RunAgentInput) {
    let reserialized = serde_json::to_string(parsed).unwrap();
    let reparsed: agui::RunAgentInput = serde_json::from_str(&reserialized).unwrap();
    assert_eq!(format!("{parsed:?}"), format!("{reparsed:?}"));
}

/// Drive `agui::run` with a mock script and collect the full event sequence.
async fn run_events(input_json: serde_json::Value) -> Vec<Event> {
    let input: agui::RunAgentInput = serde_json::from_value(input_json).unwrap();
    agui::run(input, Arc::new(agui::MockProvider))
        .collect::<Vec<_>>()
        .await
}

/// Build a minimal run input carrying a `_mock` script in `forwardedProps`.
fn mock_input(run_id: &str, script: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "threadId": "t-1",
        "runId": run_id,
        "state": {},
        "tools": [],
        "context": [],
        "messages": [{"id": "u1", "role": "user", "content": "hi"}],
        "forwardedProps": {"_mock": script},
    })
}

#[tokio::test]
async fn run_plain_text() {
    let events = run_events(mock_input(
        "r-text",
        serde_json::json!([{"text": "Hello world"}, {"finish": {"stopReason": "end_turn"}}]),
    ))
    .await;
    insta::assert_json_snapshot!(events);
}

#[tokio::test]
async fn run_text_then_tool_call() {
    let events = run_events(mock_input(
        "r-tool",
        serde_json::json!([
            {"text": "Checking."},
            {"toolCall": {"name": "get_weather", "args": "{\"location\":\"Boston\"}"}},
            {"finish": {"stopReason": "tool_use"}}
        ]),
    ))
    .await;
    insta::assert_json_snapshot!(events);
}

#[tokio::test]
async fn run_two_tool_calls() {
    let events = run_events(mock_input(
        "r-tools",
        serde_json::json!([
            {"toolCall": {"name": "alpha", "args": "{}"}},
            {"toolCall": {"name": "beta", "args": "{\"x\":1}"}},
            {"finish": {"stopReason": "tool_use"}}
        ]),
    ))
    .await;
    insta::assert_json_snapshot!(events);
}

#[tokio::test]
async fn run_reasoning_then_text() {
    let events = run_events(mock_input(
        "r-reason",
        serde_json::json!([
            {"reasoning": "Let me think about Boston."},
            {"text": "It is 72F."},
            {"finish": {"stopReason": "end_turn"}}
        ]),
    ))
    .await;
    insta::assert_json_snapshot!(events);
}

#[tokio::test]
async fn run_provider_error_mid_stream() {
    let events = run_events(mock_input(
        "r-err",
        serde_json::json!([{"text": "Partial"}, {"error": "provider exploded"}]),
    ))
    .await;
    insta::assert_json_snapshot!(events);
}

#[tokio::test]
async fn run_continuation_turn_produces_text() {
    // The continuation transcript (assistant tool call + tool result) is fed as
    // input; the mock responds with a final text answer.
    let mut input: serde_json::Value = serde_json::from_str(CONTINUATION_TURN).unwrap();
    input["forwardedProps"] = serde_json::json!({
        "_mock": [{"text": "The weather in Boston is 72F."}, {"finish": {"stopReason": "end_turn"}}]
    });
    let events = run_events(input).await;
    insta::assert_json_snapshot!(events);
}

#[tokio::test]
async fn end_to_end_http_sse() {
    let provider: Arc<dyn agui::Provider> = Arc::new(agui::MockProvider);
    let router = axum::Router::new()
        .route("/agui", axum::routing::post(handle))
        .with_state(provider);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    let body = mock_input(
        "r-http",
        serde_json::json!([
            {"text": "Hello world"},
            {"toolCall": {"name": "get_weather", "args": "{\"location\":\"Boston\"}"}},
            {"finish": {"stopReason": "tool_use"}}
        ]),
    );

    let response = reqwest::Client::new()
        .post(format!("http://{addr}/agui"))
        .header("accept", "text/event-stream")
        .json(&body)
        .send()
        .await
        .unwrap();

    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("text/event-stream")
    );

    let raw = response.text().await.unwrap();
    insta::assert_snapshot!(raw);
}

async fn handle(
    axum::extract::State(provider): axum::extract::State<Arc<dyn agui::Provider>>,
    axum::extract::Json(input): axum::extract::Json<agui::RunAgentInput>,
) -> impl axum::response::IntoResponse {
    agui::sse_response(agui::run(input, provider))
}
