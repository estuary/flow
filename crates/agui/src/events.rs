//! AG-UI output events (ground truth: `@ag-ui/core@0.0.57`).
//!
//! Discriminated on `type` with `SCREAMING_SNAKE_CASE` values; every other
//! field is `camelCase`. Optional fields are omitted (`skip_serializing_if`)
//! rather than emitted as `null`. The wire also allows a `timestamp` and
//! `rawEvent` on every event; we never emit them, so they are simply absent
//! from this enum.
//!
//! Only the variants this server actually emits are exercised, but the full
//! set the client understands is modeled so callers see the complete protocol
//! surface. Deprecated aliases (`*_CHUNK`, `ACTIVITY_*`, `THINKING_*`) are
//! intentionally omitted.

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(
    tag = "type",
    rename_all = "SCREAMING_SNAKE_CASE",
    rename_all_fields = "camelCase"
)]
pub enum Event {
    RunStarted {
        thread_id: String,
        run_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent_run_id: Option<String>,
    },
    RunFinished {
        thread_id: String,
        run_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<serde_json::Value>,
    },
    RunError {
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<String>,
    },
    StepStarted {
        step_name: String,
    },
    StepFinished {
        step_name: String,
    },
    TextMessageStart {
        message_id: String,
        role: String,
    },
    TextMessageContent {
        message_id: String,
        delta: String,
    },
    TextMessageEnd {
        message_id: String,
    },
    ToolCallStart {
        tool_call_id: String,
        tool_call_name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent_message_id: Option<String>,
    },
    ToolCallArgs {
        tool_call_id: String,
        delta: String,
    },
    ToolCallEnd {
        tool_call_id: String,
    },
    ToolCallResult {
        message_id: String,
        tool_call_id: String,
        content: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        role: Option<String>,
    },
    ReasoningStart {
        message_id: String,
    },
    ReasoningEnd {
        message_id: String,
    },
    ReasoningMessageStart {
        message_id: String,
        role: String,
    },
    ReasoningMessageContent {
        message_id: String,
        delta: String,
    },
    ReasoningMessageEnd {
        message_id: String,
    },
    ReasoningEncryptedValue {
        subtype: String,
        entity_id: String,
        encrypted_value: String,
    },
    StateSnapshot {
        snapshot: serde_json::Value,
    },
    StateDelta {
        delta: Vec<serde_json::Value>,
    },
    MessagesSnapshot {
        messages: Vec<crate::types::Message>,
    },
    Raw {
        event: serde_json::Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source: Option<String>,
    },
    Custom {
        name: String,
        value: serde_json::Value,
    },
}
