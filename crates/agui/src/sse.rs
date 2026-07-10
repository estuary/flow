//! Server-Sent Events framing, in both directions.
//!
//! Outbound: [`sse_response`] wraps a stream of AG-UI [`Event`]s as an axum
//! SSE response of bare `data: {json}\n\n` frames (no `event:` line), which is
//! what the official `@ag-ui/client` expects.
//!
//! Inbound: [`SseDecoder`] is a pure, incremental parser for the provider's own
//! SSE response (Anthropic). It is driven by [`crate::anthropic`] and tolerates
//! chunk boundaries falling anywhere, `\r\n` or `\n` line endings, comment
//! lines, and multi-line `data:` fields.

use crate::events::Event;

/// Wrap a stream of AG-UI events as an axum SSE response.
///
/// Events are serialized with `serde_json`, which cannot fail for our types, so
/// the encoding stream is infallible. A 15s keep-alive comment holds the
/// connection open between events; the client tolerates SSE comment lines.
pub fn sse_response(
    events: futures::stream::BoxStream<'static, Event>,
) -> axum::response::sse::Sse<
    impl futures::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>,
> {
    use futures::StreamExt;

    let frames = events.map(|event| {
        let json = serde_json::to_string(&event).expect("AG-UI events always serialize to JSON");
        Ok(axum::response::sse::Event::default().data(json))
    });

    axum::response::sse::Sse::new(frames).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("keepalive"),
    )
}

/// A single decoded SSE frame. `event` is the `event:` field, if any; `data` is
/// the joined `data:` lines (without the trailing frame-terminating newline).
#[derive(Debug, Clone, PartialEq)]
pub struct SseFrame {
    pub event: Option<String>,
    pub data: String,
}

/// Incremental SSE frame decoder over raw response bytes.
#[derive(Default)]
pub struct SseDecoder {
    buffer: Vec<u8>,
}

impl SseDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append `bytes` and return every frame that is now complete. Incomplete
    /// trailing bytes are retained for the next call.
    pub fn push(&mut self, bytes: &[u8]) -> Vec<SseFrame> {
        self.buffer.extend_from_slice(bytes);

        let mut frames = Vec::new();
        while let Some((content_end, consumed_end)) = find_boundary(&self.buffer) {
            let content = self.buffer[..content_end].to_vec();
            self.buffer.drain(..consumed_end);
            if let Some(frame) = parse_frame(&content) {
                frames.push(frame);
            }
        }
        frames
    }
}

/// Locate the first blank-line frame boundary. Returns `(content_end,
/// consumed_end)` where `content_end` excludes the newline that terminates the
/// frame's last field line, and `consumed_end` is past the blank separator.
/// Handles `\n\n`, `\r\n\r\n`, and mixed endings.
fn find_boundary(buffer: &[u8]) -> Option<(usize, usize)> {
    let mut i = 0;
    while i < buffer.len() {
        if buffer[i] == b'\n' {
            if i + 1 < buffer.len() && buffer[i + 1] == b'\n' {
                return Some((i, i + 2));
            }
            if i + 2 < buffer.len() && buffer[i + 1] == b'\r' && buffer[i + 2] == b'\n' {
                return Some((i, i + 3));
            }
        }
        i += 1;
    }
    None
}

fn parse_frame(content: &[u8]) -> Option<SseFrame> {
    let text = String::from_utf8_lossy(content);

    let mut event = None;
    let mut data_lines: Vec<&str> = Vec::new();

    for line in text.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        // Comment lines (SSE keep-alives) and blank lines carry no field.
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("event:") {
            event = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("data:") {
            // A single optional leading space after the colon is stripped.
            data_lines.push(rest.strip_prefix(' ').unwrap_or(rest));
        }
        // Other SSE fields (`id:`, `retry:`) are irrelevant here.
    }

    if event.is_none() && data_lines.is_empty() {
        return None;
    }
    Some(SseFrame {
        event,
        data: data_lines.join("\n"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_frames_across_arbitrary_chunk_boundaries() {
        // The same two frames, delivered in three byte-chunks that split lines
        // and a frame boundary mid-sequence.
        let full = b"event: message_start\ndata: {\"type\":\"message_start\"}\n\nevent: ping\ndata: {\"type\":\"ping\"}\n\n";
        let (a, rest) = full.split_at(15);
        let (b, c) = rest.split_at(30);

        let mut decoder = SseDecoder::new();
        let mut frames = decoder.push(a);
        frames.extend(decoder.push(b));
        frames.extend(decoder.push(c));

        insta::assert_debug_snapshot!(frames);
    }

    #[test]
    fn tolerates_crlf_and_multiple_data_lines() {
        let bytes = b"event: e\r\ndata: line1\r\ndata: line2\r\n\r\n";
        let mut decoder = SseDecoder::new();
        let frames = decoder.push(bytes);
        insta::assert_debug_snapshot!(frames);
    }

    #[test]
    fn ignores_comment_only_frames() {
        let bytes = b": keepalive\n\ndata: {\"type\":\"message_stop\"}\n\n";
        let mut decoder = SseDecoder::new();
        let frames = decoder.push(bytes);
        insta::assert_debug_snapshot!(frames);
    }
}
