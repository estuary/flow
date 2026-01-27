use proto_gazette::broker;

pin_project_lite::pin_project! {
    /// Transforms a stream of ReadResponses to align content at newline boundaries.
    ///
    /// This combinator processes incoming ReadResponses and ensures that:
    /// - Metadata responses (where `fragment` is Some) are passed through unchanged
    /// - Content responses are transformed so each emitted content ends at a newline
    /// - Partial lines are buffered and stitched with subsequent content
    /// - Offsets are correctly adjusted to reflect the actual content start
    ///
    /// Memory efficiency:
    /// - When possible, emits zero-copy slices of the original Bytes
    /// - Only allocates when stitching a remainder with new content
    pub struct ReadLines<S> {
        #[pin]
        inner: S,
        state: State,
    }
}

impl<S> ReadLines<S> {
    /// Creates a ReadLines stream from an inner stream of ReadResponses.
    pub fn new(inner: S) -> Self {
        ReadLines {
            inner,
            state: State::Partial {
                offset: 0,
                partial: bytes::BytesMut::new(),
            },
        }
    }
}

enum State {
    /// Partial line buffered in `partial`, starting at `offset`.
    /// Guaranteed to not contain a newline.
    Partial {
        offset: i64,
        partial: bytes::BytesMut,
    },
    /// Pending response content to process in `pending`, starting at `offset`.
    /// `empty` is an empty BytesMut for reuse when building partial lines.
    Pending {
        offset: i64,
        pending: bytes::Bytes,
        empty: bytes::BytesMut,
    },
    /// Stream is terminated.
    Done,
}

impl<S> futures::Stream for ReadLines<S>
where
    S: futures::Stream<Item = crate::RetryResult<broker::ReadResponse>>,
{
    type Item = crate::RetryResult<broker::ReadResponse>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        let mut this = self.project();

        loop {
            let (mut offset, mut partial) = match std::mem::replace(this.state, State::Done) {
                State::Done => return Poll::Ready(None),
                State::Partial { offset, partial } => (offset, partial),

                State::Pending {
                    offset,
                    pending,
                    empty: mut partial,
                } => {
                    assert!(partial.is_empty());

                    // Does `pending` have complete lines? Note we're finding the *last* occurrence.
                    if let Some(pivot) = memchr::memrchr(b'\n', &pending) {
                        // Retain the remainder, after emitting completed lines.
                        partial.extend_from_slice(&pending[pivot + 1..]);

                        *this.state = State::Partial {
                            offset: offset + pivot as i64 + 1,
                            partial,
                        };
                        return Poll::Ready(Some(Ok(broker::ReadResponse {
                            offset,
                            content: pending.slice(..pivot + 1),
                            ..Default::default()
                        })));
                    } else {
                        partial.extend_from_slice(&pending);
                        (offset, partial)
                    }
                }
            };
            // Post-condition: we have a `partial` line starting at `offset`,
            // and must read another response to make progress.

            let response = match this.inner.as_mut().poll_next(cx) {
                Poll::Pending => {
                    *this.state = State::Partial { offset, partial };
                    return Poll::Pending;
                }
                Poll::Ready(Some(Ok(response))) => response,
                Poll::Ready(Some(Err(err))) => {
                    // Surface inner stream errors without changing our state.
                    *this.state = State::Partial { offset, partial };
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(None) => {
                    // Inner Stream is done. Note we're already State::Done.
                    if partial.is_empty() {
                        continue; // Clean closure.
                    } else {
                        return Poll::Ready(Some(Err(crate::Error::Protocol(
                            "read_lines: inner stream closed with a partial line remainder",
                        )
                        .with_attempt(0))));
                    }
                }
            };

            // Fragment responses are passed through, with some special handling.
            if let Some(fragment) = &response.fragment {
                // Fragment responses may only arrive between whole lines.
                if !partial.is_empty() {
                    *this.state = State::Done;
                    return Poll::Ready(Some(Err(crate::Error::Protocol(
                        "read_lines: fragment response arrived mid-line",
                    )
                    .with_attempt(0))));
                }

                // Fragment responses may seek the offset forward, but not backwards.
                if fragment.begin < offset {
                    *this.state = State::Done;
                    return Poll::Ready(Some(Err(crate::Error::Protocol(
                        "read_lines: fragment offset is less than expected",
                    )
                    .with_attempt(0))));
                }
                offset = fragment.begin;

                *this.state = State::Partial { offset, partial };
                return Poll::Ready(Some(Ok(response)));
            }

            if response.offset != offset + partial.len() as i64 {
                *this.state = State::Done;
                return Poll::Ready(Some(Err(crate::Error::Protocol(
                    "read_lines: content offset is not contiguous",
                )
                .with_attempt(0))));
            }

            if partial.is_empty() {
                // We can avoid stitching / copying if there's no partial line.
                *this.state = State::Pending {
                    offset,
                    pending: response.content,
                    empty: partial,
                };
            } else if let Some(pivot) = memchr::memchr(b'\n', &response.content) {
                // Stitch `partial` + content up to the *first* newline.
                partial.extend_from_slice(&response.content[..=pivot]);
                let stitched_len = partial.len() as i64;
                let stitched = partial.split().freeze();

                // Yield the stitched line, retaining any remainder as pending.
                *this.state = State::Pending {
                    offset: offset + stitched_len,
                    pending: response.content.slice(pivot + 1..),
                    empty: partial,
                };
                return Poll::Ready(Some(Ok(broker::ReadResponse {
                    offset,
                    content: stitched,
                    ..Default::default()
                })));
            } else {
                // `content` has no newline, and we must continue to await one.
                // Loop to re-poll the inner Stream.
                partial.extend_from_slice(&response.content);
                *this.state = State::Partial { offset, partial };
            }
        }
    }
}

impl<S> futures::stream::FusedStream for ReadLines<S>
where
    S: futures::Stream<Item = crate::RetryResult<broker::ReadResponse>>,
{
    fn is_terminated(&self) -> bool {
        matches!(self.state, State::Done)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{StreamExt, stream};

    fn content(offset: i64, data: &str) -> crate::RetryResult<broker::ReadResponse> {
        Ok(broker::ReadResponse {
            offset,
            content: bytes::Bytes::from(data.to_string()),
            ..Default::default()
        })
    }

    fn meta(offset: i64) -> crate::RetryResult<broker::ReadResponse> {
        Ok(broker::ReadResponse {
            offset,
            fragment: Some(broker::Fragment {
                begin: offset,
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    fn err() -> crate::RetryResult<broker::ReadResponse> {
        Err(crate::RetryError {
            attempt: 0,
            inner: crate::Error::UnexpectedEof,
        })
    }

    async fn collect(inputs: Vec<crate::RetryResult<broker::ReadResponse>>) -> Vec<(i64, String)> {
        ReadLines::new(stream::iter(inputs))
            .map(|r| match r {
                Ok(r) if r.fragment.is_some() => (-1, "META".into()),
                Ok(r) => (r.offset, String::from_utf8_lossy(&r.content).into()),
                Err(_) => (-1, "ERR".into()),
            })
            .collect()
            .await
    }

    #[tokio::test]
    async fn test_read_lines() {
        let cases: Vec<(
            &str,
            Vec<crate::RetryResult<broker::ReadResponse>>,
            Vec<(i64, &str)>,
        )> = vec![
            // Empty stream
            ("empty_stream", vec![], vec![]),
            // Complete lines pass through
            (
                "complete_line",
                vec![content(0, "line\n")],
                vec![(0, "line\n")],
            ),
            // Multi-line batching via memrchr (finds last newline)
            (
                "multi_line_batch",
                vec![content(0, "a\nb\nc")],
                vec![(0, "a\nb\n"), (-1, "ERR")],
            ),
            // Partial at EOF is an error
            (
                "partial_at_eof",
                vec![content(0, "partial")],
                vec![(-1, "ERR")],
            ),
            // Stitching across responses
            (
                "stitch_simple",
                vec![content(0, "hel"), content(3, "lo\n")],
                vec![(0, "hello\n")],
            ),
            // Multiple partials stitched together
            (
                "stitch_multi_partial",
                vec![content(0, "a"), content(1, "b"), content(2, "c\n")],
                vec![(0, "abc\n")],
            ),
            // Stitch then pending content with multiple lines
            (
                "stitch_then_pending",
                vec![content(0, "r"), content(1, "\nx\ny\nz")],
                vec![(0, "r\n"), (2, "x\ny\n"), (-1, "ERR")],
            ),
            // Fragment/metadata passthrough and forward seek
            (
                "meta_forward_seek",
                vec![content(0, "a\n"), meta(100), content(100, "b\n")],
                vec![(0, "a\n"), (-1, "META"), (100, "b\n")],
            ),
            // Consecutive fragments
            (
                "consecutive_fragments",
                vec![meta(0), meta(100), content(100, "a\n")],
                vec![(-1, "META"), (-1, "META"), (100, "a\n")],
            ),
            // Error passthrough preserves partial state for recovery
            (
                "error_preserves_partial",
                vec![content(0, "ab"), err(), content(2, "c\n")],
                vec![(-1, "ERR"), (0, "abc\n")],
            ),
            // Empty content absorbed
            (
                "empty_content_absorbed",
                vec![content(0, "a"), content(1, ""), content(1, "b\n")],
                vec![(0, "ab\n")],
            ),
            // Only newlines
            ("only_newlines", vec![content(0, "\n\n")], vec![(0, "\n\n")]),
            // Sequential single newlines
            (
                "sequential_newlines",
                vec![content(0, "\n"), content(1, "\n"), content(2, "\n")],
                vec![(0, "\n"), (1, "\n"), (2, "\n")],
            ),
            // Complex multi-response scenario
            (
                "complex",
                vec![
                    content(0, "line1"),
                    content(5, "\nline2\npart"),
                    content(16, "ial\n"),
                ],
                vec![(0, "line1\n"), (6, "line2\n"), (12, "partial\n")],
            ),
            // Validation: fragment mid-line
            (
                "err_fragment_mid_line",
                vec![content(0, "partial"), meta(7)],
                vec![(-1, "ERR")],
            ),
            // Validation: non-contiguous offset
            (
                "err_offset_gap",
                vec![content(0, "abc"), content(10, "def\n")],
                vec![(-1, "ERR")],
            ),
            // Validation: backward content offset
            (
                "err_offset_backward",
                vec![meta(10), content(10, "abc"), content(5, "def\n")],
                vec![(-1, "META"), (-1, "ERR")],
            ),
            // Validation: fragment backward offset
            (
                "err_fragment_backward",
                vec![content(0, "a\n"), meta(0)],
                vec![(0, "a\n"), (-1, "ERR")],
            ),
        ];

        for (name, inputs, expected) in cases {
            let result = collect(inputs).await;
            let expected: Vec<(i64, String)> =
                expected.into_iter().map(|(o, s)| (o, s.into())).collect();
            assert_eq!(result, expected, "case: {name}");
        }
    }
}
