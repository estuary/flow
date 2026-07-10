//! Provider-neutral vocabulary between the AG-UI protocol layer (`run`) and a
//! concrete LLM backend (`anthropic`, `mock`).
//!
//! `run` translates a `RunAgentInput` into a [`ProviderRequest`], asks a
//! [`Provider`] to execute a single inference call, and maps the resulting
//! stream of [`ProviderEvent`]s back onto AG-UI wire events. Keeping this
//! middle vocabulary free of any provider- or protocol-specific detail is what
//! lets the same run state machine drive both the real Anthropic backend and
//! the deterministic mock.

/// A single normalized inference request. `messages` are already flattened into
/// a provider-neutral shape; `system` and `context` folding has happened in the
/// translation layer.
pub struct ProviderRequest {
    pub system: Option<String>,
    pub messages: Vec<ProviderMessage>,
    pub tools: Vec<crate::types::Tool>,
    /// Optional model hint forwarded from the client. The server, not the
    /// client, ultimately decides; a `None` lets the provider pick its default.
    pub model: Option<String>,
    /// Script for the mock provider, threaded from `forwardedProps._mock`.
    /// Ignored by real providers.
    pub mock_script: Option<serde_json::Value>,
}

/// A conversation turn, shaped closely to the Anthropic Messages API so the
/// backend translation is mechanical. Tool results are carried as blocks within
/// a `User` turn (matching Anthropic's requirement that `tool_result` blocks
/// live in a user message).
#[derive(Debug, Clone)]
pub enum ProviderMessage {
    User { content: Vec<ContentBlock> },
    Assistant { content: Vec<ContentBlock> },
}

#[derive(Debug, Clone)]
pub enum ContentBlock {
    Text(String),
    Image(ImageSource),
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
    ToolResult {
        tool_use_id: String,
        content: String,
        is_error: bool,
    },
}

#[derive(Debug, Clone)]
pub enum ImageSource {
    Base64 { media_type: String, data: String },
    Url { url: String },
}

/// A normalized streaming event from a provider. A provider emits at most one
/// open block at a time (`*Start` .. `*End`), which the run state machine relies
/// on to bracket AG-UI text/tool-call/reasoning messages.
#[derive(Debug, Clone, PartialEq)]
pub enum ProviderEvent {
    TextStart,
    TextDelta(String),
    TextEnd,
    ToolCallStart {
        id: String,
        name: String,
    },
    ToolCallArgsDelta(String),
    ToolCallEnd,
    ReasoningStart,
    ReasoningDelta(String),
    ReasoningEnd,
    Finished {
        stop_reason: String,
        usage: serde_json::Value,
    },
}

/// A backend capable of executing a single inference call as a stream of
/// [`ProviderEvent`]s. Implementations own all provider IO; the returned stream
/// is `'static` so it can outlive the borrow of `&self`.
pub trait Provider: Send + Sync {
    fn run(
        &self,
        request: ProviderRequest,
    ) -> futures::stream::BoxStream<'static, anyhow::Result<ProviderEvent>>;
}
