use proto_gazette::broker;
use std::task::Poll;

pin_project_lite::pin_project! {
    /// Accumulates newline-aligned content from a journal read stream into
    /// large contiguous buffers suited for batch parsing (e.g., simdjson).
    ///
    /// Content is accumulated up to a caller-provided target threshold
    /// and yielded as an owned Vec<u8>, each with at least `PADDING` (64)
    /// bytes of spare capacity.
    pub struct ReadLines<S> {
        #[pin]
        inner: S,
        state: State,
    }
}

/// A batch of newline-aligned content from a journal read stream.
#[derive(Debug)]
pub struct LinesBatch {
    /// Journal byte offset of `content[0]`.
    pub offset: i64,
    /// Contiguous newline-terminated content. The final byte is always `b'\n'`.
    /// Has at least PADDING (64) bytes of spare capacity.
    pub content: Vec<u8>,
}

// Internal state of ReadLines. Note it's exactly 64 bytes (one cache line).
struct State {
    /// Content chunks accumulated from the inner stream. Chunks in
    /// `buffers[..aligned]` collectively end at a newline boundary.
    /// Chunks in `buffers[aligned..]` are a partial trailing line.
    buffers: Vec<bytes::Bytes>,
    /// Journal byte offset of the *end* of all buffered content.
    /// Equivalently, the offset where the next incoming content is expected.
    offset: i64,
    /// Number of entries in `buffers` that form newline-aligned content.
    /// Set to `buffers.len()` after pushing a newline-terminated chunk.
    aligned: u16,
    /// Target yield threshold in bytes.
    target: u32,
    /// Most recently observed fragment metadata. Boxed to minimize State size.
    fragment: Box<broker::Fragment>,
    /// Most recently observed write head.
    write_head: i64,
    /// Deferred error to surface on the next poll, after yielding aligned
    /// content. Boxed to keep State compact. ReadLines never sets `done`
    /// based on errors — the caller decides whether to continue polling.
    deferred_error: Option<Box<crate::RetryError>>,
    /// Inner stream has ended.
    done: bool,
}

impl<S> ReadLines<S> {
    /// Creates a ReadLines stream from an inner stream of ReadResponses.
    /// `target` is the approximate byte threshold at which a batch is yielded.
    pub fn new(inner: S, target: u32) -> Self {
        ReadLines {
            inner,
            state: State {
                buffers: Vec::new(),
                offset: 0,
                aligned: 0,
                target,
                fragment: Box::new(broker::Fragment::default()),
                write_head: 0,
                deferred_error: None,
                done: false,
            },
        }
    }

    /// Most recently observed fragment metadata. Reflects the latest metadata
    /// as of the most recent `poll_next` call, which may have been updated by
    /// responses processed after the last-yielded `LinesBatch`.
    pub fn fragment(&self) -> &broker::Fragment {
        &self.state.fragment
    }

    /// Most recently observed write head. Reflects the latest value as of the
    /// most recent `poll_next` call.
    pub fn write_head(&self) -> i64 {
        self.state.write_head
    }

    /// Insert `content` at the front of the internal buffer. `content` must
    /// be newline-aligned (its last byte must be `b'\n'`).  On the next poll
    /// the put-back bytes will be returned — possibly combined with additional
    /// aligned content that arrives from the inner stream.
    pub fn put_back(self: std::pin::Pin<&mut Self>, content: bytes::Bytes) {
        assert!(
            content.last() == Some(&b'\n'),
            "put_back content must be newline-aligned",
        );
        let state = self.project().state;
        state.buffers.insert(0, content);
        state.aligned += 1;
    }
}

impl State {
    /// Yield `yield_count` aligned buffers as a contiguous `LinesBatch`.
    fn take_aligned(&mut self, yield_count: u16, buffered_bytes: usize) -> LinesBatch {
        // `self.offset` is the end of all buffered content.
        // The start of buffers[0] is `self.offset - buffered_bytes`.
        let batch_offset = self.offset - buffered_bytes as i64;

        let data = self.buffers.drain(..yield_count as usize).fold(
            Vec::with_capacity(buffered_bytes + PADDING),
            |mut v, b| {
                v.extend_from_slice(&b);
                v
            },
        );
        self.aligned -= yield_count;

        LinesBatch {
            offset: batch_offset,
            content: data,
        }
    }

    /// Handle an error, yielding aligned content first if available.
    fn on_error(
        &mut self,
        err: crate::RetryError,
        buffered_bytes: usize,
    ) -> Poll<Option<crate::RetryResult<LinesBatch>>> {
        if self.aligned != 0 {
            self.deferred_error = Some(Box::new(err));
            Poll::Ready(Some(Ok(self.take_aligned(self.aligned, buffered_bytes))))
        } else {
            Poll::Ready(Some(Err(err)))
        }
    }
}

impl<S> futures::Stream for ReadLines<S>
where
    S: futures::Stream<Item = crate::RetryResult<broker::ReadResponse>>,
{
    type Item = crate::RetryResult<LinesBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        let state = this.state;
        let mut inner = this.inner;

        let mut buffered_bytes: usize = state.buffers.iter().map(|b| b.len()).sum();

        // Surface a deferred error from the prior poll.
        if let Some(err) = state.deferred_error.take() {
            return Poll::Ready(Some(Err(*err)));
        }
        // Stream is done. This check avoids driving `inner` again,
        // which is undefined behavior, and also makes us FusedStream.
        if state.done {
            // put_back() may have inserted aligned content after `done`
            // was set, so yield that first.
            if state.aligned != 0 {
                return Poll::Ready(Some(Ok(state.take_aligned(state.aligned, buffered_bytes))));
            }
            return Poll::Ready(None);
        }

        // Loop to accumulate as much ready content as we can (up to `state.target`).
        loop {
            // Read the next response, handling !Ok cases.
            let response = match inner.as_mut().poll_next(cx) {
                Poll::Pending => {
                    if state.aligned != 0 {
                        return Poll::Ready(Some(Ok(
                            state.take_aligned(state.aligned, buffered_bytes)
                        )));
                    }
                    return Poll::Pending;
                }
                Poll::Ready(None) => {
                    state.done = true; // Don't poll `inner` again.

                    // Invariant check: the stream should be aligned at EOF.
                    if state.buffers.len() != state.aligned as usize {
                        return state.on_error(
                            crate::Error::ReadLines {
                                message: "partial line remainder at end of stream",
                                offset: state.offset,
                            }
                            .with_attempt(0),
                            buffered_bytes,
                        );
                    }

                    if state.aligned != 0 {
                        return Poll::Ready(Some(Ok(
                            state.take_aligned(state.aligned, buffered_bytes)
                        )));
                    }
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(err))) => {
                    return state.on_error(err, buffered_bytes);
                }
                Poll::Ready(Some(Ok(response))) => response,
            };

            // Is this a metadata-only fragment response?
            if let Some(fragment) = response.fragment {
                *state.fragment = fragment;
                state.write_head = response.write_head;

                if state.offset == response.offset {
                    // Contiguous content (no offset jump).
                    continue;
                } else if state.aligned as usize != state.buffers.len() {
                    // Invariant check: we should never have a partially buffered line
                    // that straddles an offset discontinuity.
                    return state.on_error(
                        crate::Error::ReadLines {
                            message: "partial line remainder at fragment offset discontinuity",
                            offset: state.offset,
                        }
                        .with_attempt(0),
                        buffered_bytes,
                    );
                } else if state.aligned != 0 {
                    // We have (only) aligned content from before the discontinuity.
                    // Yield it prior to jumping offsets to ensure correct offset calculations.
                    let batch = state.take_aligned(state.aligned, buffered_bytes);
                    state.offset = response.offset;
                    return Poll::Ready(Some(Ok(batch)));
                } else {
                    // `buffers` / `aligned` are zero-valued so we trivially jump.
                    state.offset = response.offset;
                    continue;
                }
            }

            // This is a content-bearing response.
            // Invariant check: the response offset must match our expectation.
            if response.offset != state.offset {
                return state.on_error(
                    crate::Error::ReadLines {
                        message: "unexpected response offset",
                        offset: state.offset,
                    }
                    .with_attempt(0),
                    buffered_bytes,
                );
            }

            state.offset += response.content.len() as i64;
            buffered_bytes += response.content.len();

            let prior_aligned = state.aligned;
            let mut content = response.content;

            if let Some(pivot) = memchr::memrchr(b'\n', &content) {
                if pivot == content.len() - 1 {
                    state.buffers.push(content);
                    state.aligned = state.buffers.len() as u16;
                } else {
                    state.buffers.push(content.split_to(pivot + 1));
                    state.aligned = state.buffers.len() as u16;
                    state.buffers.push(content);
                }
            } else {
                state.buffers.push(content);
            }

            if state.buffers.len() >= u16::MAX as usize {
                return state.on_error(
                    crate::Error::ReadLines {
                        message: "buffer limit exceeded",
                        offset: state.offset,
                    }
                    .with_attempt(0),
                    buffered_bytes,
                );
            }

            // Yield early if buffered bytes exceed target and we had
            // prior aligned content. Uses prior_aligned to keep the batch
            // close to (but not exceeding) target.
            if buffered_bytes >= state.target as usize && prior_aligned != 0 {
                return Poll::Ready(Some(Ok(state.take_aligned(prior_aligned, buffered_bytes))));
            }
        }
    }
}

impl<S> futures::stream::FusedStream for ReadLines<S>
where
    S: futures::Stream<Item = crate::RetryResult<broker::ReadResponse>>,
{
    fn is_terminated(&self) -> bool {
        self.state.done && self.state.deferred_error.is_none() && self.state.aligned == 0
    }
}

const PADDING: usize = 64; // simdjson read-ahead padding

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

    fn meta(offset: i64, write_head: i64) -> crate::RetryResult<broker::ReadResponse> {
        Ok(broker::ReadResponse {
            offset,
            write_head,
            fragment: Some(broker::Fragment {
                begin: offset,
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    fn an_err() -> crate::RetryResult<broker::ReadResponse> {
        Err(crate::Error::Protocol("test error").with_attempt(0))
    }

    fn map_batch(r: crate::RetryResult<LinesBatch>) -> (i64, String) {
        match r {
            Ok(b) => {
                assert!(
                    b.content.capacity() - b.content.len() >= PADDING,
                    "insufficient padding at offset {}",
                    b.offset,
                );
                (b.offset, String::from_utf8_lossy(&b.content).into())
            }
            Err(e) => (-1, format!("ERR:{}", e.inner)),
        }
    }

    /// Collect ReadLines output, verifying PADDING on each successful batch.
    async fn collect(
        inputs: Vec<crate::RetryResult<broker::ReadResponse>>,
        target: u32,
    ) -> Vec<(i64, String)> {
        ReadLines::new(stream::iter(inputs), target)
            .map(map_batch)
            .collect()
            .await
    }

    #[tokio::test]
    async fn test_accumulation_and_stitching() {
        // Empty stream.
        assert_eq!(collect(vec![], 1024).await, vec![]);
        // Single complete line.
        assert_eq!(
            collect(vec![content(0, "line\n")], 1024).await,
            vec![(0, "line\n".into())],
        );
        // Partial chunks stitched into a line.
        assert_eq!(
            collect(
                vec![content(0, "a"), content(1, "b"), content(2, "c\n")],
                1024
            )
            .await,
            vec![(0, "abc\n".into())],
        );
        // Mixed: partials, embedded newlines, trailing partial completed later.
        assert_eq!(
            collect(
                vec![
                    content(0, "line1"),
                    content(5, "\nline2\npart"),
                    content(16, "ial\n")
                ],
                1024,
            )
            .await,
            vec![(0, "line1\nline2\npartial\n".into())],
        );
        // Only newlines.
        assert_eq!(
            collect(vec![content(0, "\n\n")], 1024).await,
            vec![(0, "\n\n".into())],
        );
        // Empty content chunk absorbed.
        assert_eq!(
            collect(
                vec![content(0, "a"), content(1, ""), content(1, "b\n")],
                1024
            )
            .await,
            vec![(0, "ab\n".into())],
        );
    }

    #[tokio::test]
    async fn test_batching_target_threshold() {
        // Small responses accumulate under large target.
        assert_eq!(
            collect(
                vec![content(0, "a\n"), content(2, "b\n"), content(4, "c\n")],
                1024
            )
            .await,
            vec![(0, "a\nb\nc\n".into())],
        );
        // Target exceeded → yield at prior_aligned boundary.
        assert_eq!(
            collect(
                vec![content(0, "ab\n"), content(3, "cd\n"), content(6, "ef\n")],
                4
            )
            .await,
            vec![(0, "ab\n".into()), (3, "cd\n".into()), (6, "ef\n".into())],
        );
        // Partial content prevents yield even above target.
        assert_eq!(
            collect(vec![content(0, "abc"), content(3, "def\n")], 2).await,
            vec![(0, "abcdef\n".into())],
        );
    }

    #[tokio::test]
    async fn test_fragment_metadata() {
        // Contiguous fragment: no yield, content accumulates across it.
        assert_eq!(
            collect(
                vec![content(0, "a\n"), meta(2, 200), content(2, "b\n")],
                1024
            )
            .await,
            vec![(0, "a\nb\n".into())],
        );
        // Offset jump forces yield of aligned content before the gap.
        assert_eq!(
            collect(
                vec![content(0, "a\n"), meta(100, 200), content(100, "b\n")],
                1024
            )
            .await,
            vec![(0, "a\n".into()), (100, "b\n".into())],
        );
        // Consecutive fragments with jumps, no content → trivial jumps.
        assert_eq!(
            collect(
                vec![meta(0, 100), meta(100, 200), content(100, "a\n")],
                1024
            )
            .await,
            vec![(100, "a\n".into())],
        );
        // Accumulated content yielded before second fragment jump.
        assert_eq!(
            collect(
                vec![
                    meta(0, 1000),
                    content(0, "first\n"),
                    content(6, "second\n"),
                    meta(500, 2000),
                    content(500, "third\n"),
                ],
                1024,
            )
            .await,
            vec![(0, "first\nsecond\n".into()), (500, "third\n".into())],
        );
        // Accessors reflect metadata updates.
        let mut lines = ReadLines::new(
            stream::iter(vec![meta(31, 42), content(31, "hello\n")]),
            1024,
        );
        assert_eq!(lines.write_head(), 0);

        let batch = lines.next().await.unwrap().unwrap();
        assert_eq!(&batch.content, b"hello\n");
        assert_eq!(lines.fragment().begin, 31);
        assert_eq!(lines.write_head(), 42);
    }

    #[tokio::test]
    async fn test_error_handling() {
        // Error with no aligned content → immediate.
        assert_eq!(
            collect(vec![an_err()], 1024).await,
            vec![(-1, "ERR:test error".into())],
        );
        // Error with aligned → yield aligned first, then error, then continue.
        assert_eq!(
            collect(vec![content(0, "a\n"), an_err(), content(2, "b\n")], 1024).await,
            vec![
                (0, "a\n".into()),
                (-1, "ERR:test error".into()),
                (2, "b\n".into())
            ],
        );
        // Error with only partial buffered → error returned, partial preserved for stitching.
        assert_eq!(
            collect(vec![content(0, "ab"), an_err(), content(2, "c\n")], 1024).await,
            vec![(-1, "ERR:test error".into()), (0, "abc\n".into())],
        );
        // Partial at EOF.
        assert_eq!(
            collect(vec![content(0, "partial")], 1024).await,
            vec![(
                -1,
                "ERR:reading lines: partial line remainder at end of stream (at offset 7)".into(),
            )],
        );
        // Aligned then partial at EOF: aligned yielded, then partial error.
        assert_eq!(
            collect(vec![content(0, "a\nb\nc")], 1024).await,
            vec![
                (0, "a\nb\n".into()),
                (
                    -1,
                    "ERR:reading lines: partial line remainder at end of stream (at offset 5)"
                        .into()
                ),
            ],
        );
        // Content offset mismatch.
        assert_eq!(
            collect(vec![content(0, "abc"), content(10, "def\n")], 1024).await,
            vec![
                (
                    -1,
                    "ERR:reading lines: unexpected response offset (at offset 3)".into()
                ),
                (
                    -1,
                    "ERR:reading lines: partial line remainder at end of stream (at offset 3)"
                        .into()
                ),
            ],
        );
        // Fragment discontinuity with partial → error.
        assert_eq!(
            collect(vec![content(0, "partial"), meta(100, 200)], 1024).await,
            vec![
                (-1, "ERR:reading lines: partial line remainder at fragment offset discontinuity (at offset 7)".into()),
                (-1, "ERR:reading lines: partial line remainder at end of stream (at offset 7)".into()),
            ],
        );
        // Aligned + partial + fragment jump → cascading errors.
        assert_eq!(
            collect(
                vec![content(0, "a\n"), content(2, "bc"), meta(200, 500), content(200, "d\n")],
                1024,
            )
            .await,
            vec![
                (0, "a\n".into()),
                (-1, "ERR:reading lines: partial line remainder at fragment offset discontinuity (at offset 4)".into()),
                (-1, "ERR:reading lines: unexpected response offset (at offset 4)".into()),
                (-1, "ERR:reading lines: partial line remainder at end of stream (at offset 4)".into()),
            ],
        );
    }

    /// Stream that injects `Poll::Pending` where `None` entries appear.
    struct TestStream {
        items: std::collections::VecDeque<Option<crate::RetryResult<broker::ReadResponse>>>,
    }
    impl futures::Stream for TestStream {
        type Item = crate::RetryResult<broker::ReadResponse>;
        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            match self.items.pop_front() {
                Some(Some(item)) => Poll::Ready(Some(item)),
                Some(None) => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                None => Poll::Ready(None),
            }
        }
    }

    async fn collect_pending(
        items: Vec<Option<crate::RetryResult<broker::ReadResponse>>>,
        target: u32,
    ) -> Vec<(i64, String)> {
        ReadLines::new(
            TestStream {
                items: items.into(),
            },
            target,
        )
        .map(map_batch)
        .collect()
        .await
    }

    #[tokio::test]
    async fn test_pending_flush_behavior() {
        // Pending with aligned content → immediate yield.
        assert_eq!(
            collect_pending(
                vec![Some(content(0, "a\n")), None, Some(content(2, "b\n"))],
                1024
            )
            .await,
            vec![(0, "a\n".into()), (2, "b\n".into())],
        );
        // Pending with only partial → no yield until newline.
        assert_eq!(
            collect_pending(
                vec![Some(content(0, "abc")), None, Some(content(3, "def\n"))],
                1024,
            )
            .await,
            vec![(0, "abcdef\n".into())],
        );
    }

    #[tokio::test]
    async fn test_fused_stream() {
        use futures::stream::FusedStream;

        // Deferred error delays termination.
        let mut lines = ReadLines::new(stream::iter(vec![content(0, "a\nb\nc")]), 1024);
        assert!(!lines.is_terminated());
        let _ = lines.next().await.unwrap().unwrap(); // aligned "a\nb\n"
        assert!(!lines.is_terminated()); // deferred error pending
        let _ = lines.next().await.unwrap().unwrap_err(); // partial error
        assert!(lines.is_terminated());
        assert!(lines.next().await.is_none());

        // Stream error does NOT set done — caller can continue.
        let mut lines = ReadLines::new(stream::iter(vec![an_err(), content(0, "a\n")]), 1024);
        let _ = lines.next().await.unwrap().unwrap_err();
        assert!(!lines.is_terminated());
        let _ = lines.next().await.unwrap().unwrap();
        assert!(lines.is_terminated());
    }

    #[tokio::test]
    async fn test_put_back() {
        let lines = ReadLines::new(
            TestStream {
                items: vec![
                    Some(content(0, "aaa\nbbb\n")),
                    None, // Pending → flushes initial batch
                    Some(content(8, "ccc\n")),
                    None, // Pending → flushes put-back merged with new content
                ]
                .into(),
            },
            1024,
        );
        tokio::pin!(lines);

        assert_eq!(
            map_batch(lines.as_mut().next().await.unwrap()),
            (0, "aaa\nbbb\n".into())
        );

        // Put-back content merges with the next aligned chunk from the inner stream.
        lines.as_mut().put_back(bytes::Bytes::from_static(b"bbb\n"));
        assert_eq!(
            map_batch(lines.as_mut().next().await.unwrap()),
            (4, "bbb\nccc\n".into())
        );

        // After the inner stream is exhausted, multiple put_backs accumulate.
        // Each inserts at position 0, so the last call's content appears first.
        lines.as_mut().put_back(bytes::Bytes::from_static(b"one\n"));
        lines.as_mut().put_back(bytes::Bytes::from_static(b"two\n"));
        assert_eq!(
            map_batch(lines.as_mut().next().await.unwrap()),
            (4, "two\none\n".into())
        );

        // Put-back while already done exercises the early-return path in poll_next.
        lines
            .as_mut()
            .put_back(bytes::Bytes::from_static(b"late\n"));
        assert_eq!(
            map_batch(lines.as_mut().next().await.unwrap()),
            (7, "late\n".into())
        );

        assert!(lines.as_mut().next().await.is_none());
    }

    #[tokio::test]
    async fn test_buffer_limit() {
        // u16::MAX non-newline chunks triggers the buffer limit error.
        // The partial content then also errors at EOF.
        let inputs: Vec<_> = (0..u16::MAX as i64).map(|i| content(i, "x")).collect();
        let result = collect(inputs, u32::MAX).await;
        assert!(result[0].1.contains("buffer limit exceeded"));
        assert!(
            result[1]
                .1
                .contains("partial line remainder at end of stream")
        );
    }
}
