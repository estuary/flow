//! The AG-UI run state machine.
//!
//! [`run`] translates a `RunAgentInput` into a provider-neutral request, drives
//! a single [`Provider`] inference call, and maps the provider's stream onto a
//! well-formed AG-UI event stream. The ordering guarantees the official client
//! enforces are maintained by an explicit state machine rather than ad-hoc
//! flags: `RUN_STARTED` is always first; exactly one terminal event
//! (`RUN_FINISHED` or `RUN_ERROR`) is emitted and nothing follows it; text,
//! tool-call, and reasoning content is always bracketed by its start/end with a
//! matching id; and empty deltas are never emitted.

use crate::events::Event;
use crate::provider::{
    ContentBlock, ImageSource, Provider, ProviderEvent, ProviderMessage, ProviderRequest,
};
use crate::types::{InputPart, InputSource, Message, RunAgentInput, UserContent};

/// Produce the AG-UI event stream for `input`, executed against `provider`.
pub fn run(
    input: RunAgentInput,
    provider: std::sync::Arc<dyn Provider>,
) -> futures::stream::BoxStream<'static, Event> {
    use futures::StreamExt;

    let request = translate_input(&input);
    let provider_stream = provider.run(request);

    let state = RunState {
        provider: provider_stream,
        pending: std::collections::VecDeque::new(),
        started: false,
        terminal: false,
        thread_id: input.thread_id,
        run_id: input.run_id,
        parent_run_id: input.parent_run_id,
        open: OpenBlock::None,
        text_message_id: None,
        next_seq: 0,
    };

    futures::stream::unfold(state, |mut state| async move {
        loop {
            if let Some(event) = state.pending.pop_front() {
                return Some((event, state));
            }
            // Once the terminal event has been drained, the run is complete.
            if state.terminal {
                return None;
            }
            if !state.started {
                state.started = true;
                state.pending.push_back(Event::RunStarted {
                    thread_id: state.thread_id.clone(),
                    run_id: state.run_id.clone(),
                    parent_run_id: state.parent_run_id.clone(),
                });
                continue;
            }
            match state.provider.next().await {
                Some(Ok(event)) => state.on_provider_event(event),
                Some(Err(error)) => state.on_provider_error(error),
                None => state.on_provider_end(),
            }
        }
    })
    .boxed()
}

/// Which content block, if any, is currently open. Anthropic serializes content
/// blocks, so at most one is open at a time; the state machine both relies on
/// and re-establishes that invariant via [`RunState::close_open`].
enum OpenBlock {
    None,
    Text { message_id: String },
    ToolCall { tool_call_id: String },
    Reasoning { message_id: String },
}

struct RunState {
    provider: futures::stream::BoxStream<'static, anyhow::Result<ProviderEvent>>,
    pending: std::collections::VecDeque<Event>,
    started: bool,
    /// A terminal event (RUN_FINISHED/RUN_ERROR) has been queued; once it and
    /// any preceding events drain, the stream ends.
    terminal: bool,
    thread_id: String,
    run_id: String,
    parent_run_id: Option<String>,
    open: OpenBlock,
    /// Id of the assistant text message emitted this run, if any. Tool calls
    /// are parented to it only when it exists (so the client merges the tool
    /// call into that assistant message); for tool-only responses it stays
    /// `None` and `parentMessageId` is omitted.
    text_message_id: Option<String>,
    next_seq: usize,
}

impl RunState {
    fn alloc_message_id(&mut self) -> String {
        self.next_seq += 1;
        format!("{}-msg-{}", self.run_id, self.next_seq)
    }

    /// Close whichever block is currently open, emitting its end event.
    fn close_open(&mut self) {
        match std::mem::replace(&mut self.open, OpenBlock::None) {
            OpenBlock::Text { message_id } => {
                self.pending.push_back(Event::TextMessageEnd { message_id })
            }
            OpenBlock::ToolCall { tool_call_id } => {
                self.pending.push_back(Event::ToolCallEnd { tool_call_id })
            }
            OpenBlock::Reasoning { message_id } => self
                .pending
                .push_back(Event::ReasoningMessageEnd { message_id }),
            OpenBlock::None => {}
        }
    }

    fn on_provider_event(&mut self, event: ProviderEvent) {
        match event {
            ProviderEvent::TextStart => {
                self.close_open();
                let message_id = self.alloc_message_id();
                self.text_message_id = Some(message_id.clone());
                self.open = OpenBlock::Text {
                    message_id: message_id.clone(),
                };
                self.pending.push_back(Event::TextMessageStart {
                    message_id,
                    role: "assistant".to_string(),
                });
            }
            ProviderEvent::TextDelta(delta) => {
                if delta.is_empty() {
                    return;
                }
                if let OpenBlock::Text { message_id } = &self.open {
                    self.pending.push_back(Event::TextMessageContent {
                        message_id: message_id.clone(),
                        delta,
                    });
                }
            }
            ProviderEvent::TextEnd => {
                if let OpenBlock::Text { message_id } =
                    std::mem::replace(&mut self.open, OpenBlock::None)
                {
                    self.pending.push_back(Event::TextMessageEnd { message_id });
                }
            }
            ProviderEvent::ToolCallStart { id, name } => {
                self.close_open();
                self.open = OpenBlock::ToolCall {
                    tool_call_id: id.clone(),
                };
                self.pending.push_back(Event::ToolCallStart {
                    tool_call_id: id,
                    tool_call_name: name,
                    parent_message_id: self.text_message_id.clone(),
                });
            }
            ProviderEvent::ToolCallArgsDelta(delta) => {
                if delta.is_empty() {
                    return;
                }
                if let OpenBlock::ToolCall { tool_call_id } = &self.open {
                    self.pending.push_back(Event::ToolCallArgs {
                        tool_call_id: tool_call_id.clone(),
                        delta,
                    });
                }
            }
            ProviderEvent::ToolCallEnd => {
                if let OpenBlock::ToolCall { tool_call_id } =
                    std::mem::replace(&mut self.open, OpenBlock::None)
                {
                    self.pending.push_back(Event::ToolCallEnd { tool_call_id });
                }
            }
            ProviderEvent::ReasoningStart => {
                self.close_open();
                let message_id = self.alloc_message_id();
                self.open = OpenBlock::Reasoning {
                    message_id: message_id.clone(),
                };
                self.pending.push_back(Event::ReasoningMessageStart {
                    message_id,
                    role: "reasoning".to_string(),
                });
            }
            ProviderEvent::ReasoningDelta(delta) => {
                if delta.is_empty() {
                    return;
                }
                if let OpenBlock::Reasoning { message_id } = &self.open {
                    self.pending.push_back(Event::ReasoningMessageContent {
                        message_id: message_id.clone(),
                        delta,
                    });
                }
            }
            ProviderEvent::ReasoningEnd => {
                if let OpenBlock::Reasoning { message_id } =
                    std::mem::replace(&mut self.open, OpenBlock::None)
                {
                    self.pending
                        .push_back(Event::ReasoningMessageEnd { message_id });
                }
            }
            ProviderEvent::Finished { stop_reason, usage } => {
                self.close_open();
                self.pending.push_back(Event::RunFinished {
                    thread_id: self.thread_id.clone(),
                    run_id: self.run_id.clone(),
                    result: Some(serde_json::json!({
                        "stopReason": stop_reason,
                        "usage": usage,
                    })),
                });
                self.terminal = true;
            }
        }
    }

    fn on_provider_error(&mut self, error: anyhow::Error) {
        // RUN_ERROR is terminal and accepted by the client even with blocks
        // still open, so no close discipline is required here.
        self.pending.push_back(Event::RunError {
            message: format!("{error:#}"),
            code: None,
        });
        self.terminal = true;
    }

    fn on_provider_end(&mut self) {
        // A provider that ends without a Finished event still needs a clean
        // terminal; close any dangling block and finish.
        self.close_open();
        self.pending.push_back(Event::RunFinished {
            thread_id: self.thread_id.clone(),
            run_id: self.run_id.clone(),
            result: Some(serde_json::json!({ "stopReason": "end_turn", "usage": {} })),
        });
        self.terminal = true;
    }
}

/// Translate a `RunAgentInput` into a provider-neutral [`ProviderRequest`].
///
/// System and developer messages are joined into the system prompt; any
/// frontend `context` entries are folded onto the end of it. Consecutive
/// assistant messages are merged into one turn, and consecutive tool results
/// are merged into one user turn, both to satisfy Anthropic's message ordering.
pub(crate) fn translate_input(input: &RunAgentInput) -> ProviderRequest {
    let mut system_sections: Vec<String> = Vec::new();
    let mut messages: Vec<ProviderMessage> = Vec::new();

    for message in &input.messages {
        match message {
            Message::System { content, .. } | Message::Developer { content, .. } => {
                system_sections.push(content.clone());
            }
            Message::User { content, .. } => messages.push(ProviderMessage::User {
                content: translate_user_content(content),
            }),
            Message::Assistant {
                content,
                tool_calls,
                ..
            } => {
                let mut blocks: Vec<ContentBlock> = Vec::new();
                if let Some(text) = content {
                    if !text.is_empty() {
                        blocks.push(ContentBlock::Text(text.clone()));
                    }
                }
                for call in tool_calls {
                    blocks.push(ContentBlock::ToolUse {
                        id: call.id.clone(),
                        name: call.function.name.clone(),
                        input: parse_arguments(&call.function.arguments, &call.id),
                    });
                }
                append_assistant(&mut messages, blocks);
            }
            Message::Tool {
                content,
                tool_call_id,
                error,
                ..
            } => append_tool_result(
                &mut messages,
                ContentBlock::ToolResult {
                    tool_use_id: tool_call_id.clone(),
                    content: content.clone(),
                    is_error: error.is_some(),
                },
            ),
            // Reasoning/activity replay is out of scope for v0.
            Message::Activity { .. } | Message::Reasoning { .. } => {}
        }
    }

    if !input.context.is_empty() {
        let mut section = String::from("The user has provided the following context:");
        for item in &input.context {
            section.push_str(&format!(
                "\n- {}: {}",
                item.description,
                context_value(&item.value)
            ));
        }
        system_sections.push(section);
    }

    let system = if system_sections.is_empty() {
        None
    } else {
        Some(system_sections.join("\n\n"))
    };
    let model = input
        .forwarded_props
        .get("model")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let mock_script = input.forwarded_props.get("_mock").cloned();

    ProviderRequest {
        system,
        messages,
        tools: input.tools.clone(),
        model,
        mock_script,
    }
}

/// Merge into the trailing assistant turn when possible, so a text assistant
/// message and a subsequent tool-call assistant message become a single
/// Anthropic assistant turn (text block(s) then tool_use blocks).
fn append_assistant(messages: &mut Vec<ProviderMessage>, mut blocks: Vec<ContentBlock>) {
    if let Some(ProviderMessage::Assistant { content }) = messages.last_mut() {
        content.append(&mut blocks);
        return;
    }
    messages.push(ProviderMessage::Assistant { content: blocks });
}

/// Merge into the trailing user turn when it holds only tool results, so all
/// tool results for one assistant turn arrive in a single user message.
fn append_tool_result(messages: &mut Vec<ProviderMessage>, block: ContentBlock) {
    if let Some(ProviderMessage::User { content }) = messages.last_mut() {
        if content
            .iter()
            .all(|b| matches!(b, ContentBlock::ToolResult { .. }))
        {
            content.push(block);
            return;
        }
    }
    messages.push(ProviderMessage::User {
        content: vec![block],
    });
}

fn translate_user_content(content: &UserContent) -> Vec<ContentBlock> {
    match content {
        UserContent::Text(text) => vec![ContentBlock::Text(text.clone())],
        UserContent::Parts(parts) => parts.iter().filter_map(translate_input_part).collect(),
    }
}

fn translate_input_part(part: &InputPart) -> Option<ContentBlock> {
    match part {
        InputPart::Text { text } => Some(ContentBlock::Text(text.clone())),
        InputPart::Image { source, .. } => Some(ContentBlock::Image(translate_source(source))),
        // Audio/video/document/binary parts are not translated in v0.
        _ => None,
    }
}

fn translate_source(source: &InputSource) -> ImageSource {
    match source {
        InputSource::Data { value, mime_type } => ImageSource::Base64 {
            media_type: mime_type.clone(),
            data: value.clone(),
        },
        InputSource::Url { value, .. } => ImageSource::Url { url: value.clone() },
    }
}

/// Parse a tool call's `arguments` string into JSON. On failure (or empty),
/// substitute an empty object and warn, rather than failing the whole run.
fn parse_arguments(arguments: &str, tool_call_id: &str) -> serde_json::Value {
    if arguments.trim().is_empty() {
        return serde_json::json!({});
    }
    match serde_json::from_str(arguments) {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(
                %tool_call_id,
                %error,
                "failed to parse tool-call arguments as JSON; substituting empty object"
            );
            serde_json::json!({})
        }
    }
}

fn context_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}
