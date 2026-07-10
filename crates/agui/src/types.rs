//! AG-UI `RunAgentInput` request types (ground truth: `@ag-ui/core@0.0.57`).
//!
//! All types are `camelCase` on the wire. Deserialization is deliberately
//! lenient: the official client always sends `state`/`messages`/`tools`/
//! `context`/`forwardedProps`, but we default each so partial inputs (and our
//! own tests) round-trip. `Message` is a discriminated union on `role`.

/// The complete input to a single agent run. The server holds no conversation
/// state; each run carries the full prior `messages` transcript.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunAgentInput {
    pub thread_id: String,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    #[serde(default)]
    pub state: serde_json::Value,
    #[serde(default)]
    pub messages: Vec<Message>,
    #[serde(default)]
    pub tools: Vec<Tool>,
    #[serde(default)]
    pub context: Vec<ContextItem>,
    #[serde(default)]
    pub forwarded_props: serde_json::Value,
    /// HITL resume payload. Not consumed in v0; retained for round-trip fidelity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume: Option<serde_json::Value>,
}

/// A frontend-provided context entry (CopilotKit `useAgentContext`). The server
/// is expected to fold these into the system prompt, else frontend readables
/// silently do nothing.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextItem {
    pub description: String,
    pub value: serde_json::Value,
}

/// A conversation message, discriminated on `role`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(
    tag = "role",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum Message {
    Developer {
        id: String,
        content: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        encrypted_value: Option<String>,
    },
    System {
        id: String,
        content: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        encrypted_value: Option<String>,
    },
    Assistant {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tool_calls: Vec<ToolCall>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        encrypted_value: Option<String>,
    },
    User {
        id: String,
        content: UserContent,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    },
    Tool {
        id: String,
        content: String,
        tool_call_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        encrypted_value: Option<String>,
    },
    Activity {
        id: String,
        activity_type: String,
        content: serde_json::Value,
    },
    Reasoning {
        id: String,
        content: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        encrypted_value: Option<String>,
    },
}

/// User `content` is either a plain string or a multimodal input-part array.
/// `String` is tried first so a bare string never mis-parses as an array.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum UserContent {
    Text(String),
    Parts(Vec<InputPart>),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum InputPart {
    Text {
        text: String,
    },
    Image {
        source: InputSource,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<serde_json::Value>,
    },
    Audio {
        source: InputSource,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<serde_json::Value>,
    },
    Video {
        source: InputSource,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<serde_json::Value>,
    },
    Document {
        source: InputSource,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<serde_json::Value>,
    },
    /// Legacy pre-`source` binary shape retained for lenient parsing.
    Binary {
        mime_type: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        url: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        data: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filename: Option<String>,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum InputSource {
    Data {
        value: String,
        mime_type: String,
    },
    Url {
        value: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mime_type: Option<String>,
    },
}

/// An assistant-issued tool call. `function.arguments` is a JSON *string*
/// (parsed lazily during translation).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub function: FunctionCall,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encrypted_value: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FunctionCall {
    pub name: String,
    pub arguments: String,
}

/// A frontend tool definition. `parameters` is a JSON Schema object which maps
/// directly onto Anthropic's `input_schema`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tool {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub parameters: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}
