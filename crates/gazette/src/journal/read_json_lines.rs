use super::Client;
use crate::Error;
use futures::{stream::BoxStream, SinkExt, StreamExt};
use proto_gazette::broker;

/// ReadJsonLine is the enumerated Item type of a read_json_lines() Stream.
pub enum ReadJsonLine {
    /// Doc is a parsed journal document and its `next_offset`, which is the
    /// offset one beyond its last content or whitespace byte within the journal,
    /// and is generally the offset of the *next* journal document.
    ///
    /// The [begin, end) extent of each document can be determined by retaining
    /// each document's `next_offset`, which is its "end" offset and the "begin"
    /// offset of the next document.
    ///
    /// Note that a Meta enum variant may reset the "begin" offset,
    /// to the `begin` offset of its contained Fragment.
    Doc {
        root: doc::OwnedArchivedNode,
        next_offset: i64,
    },
    /// Meta is a metadata response which includes the Fragment currently being read.
    Meta(broker::ReadResponse),
}

impl Client {
    pub fn read_json_lines(self, req: broker::ReadRequest, buffer: usize) -> ReadJsonLines {
        let inner = self.read(req);

        // When buffered, use a tokio task to read up to `buffer` ReadResponses.
        let inner = if buffer != 0 {
            let (mut tx, rx) = futures::channel::mpsc::channel(buffer - 1);

            tokio::spawn(async move {
                tokio::pin!(inner);

                while let Some(result) = inner.next().await {
                    if let Err(_) = tx.send(result).await {
                        break; // Read was cancelled.
                    }
                }
            });

            rx.boxed()
        } else {
            inner.boxed()
        };

        ReadJsonLines {
            parsed: simd_doc::transcoded::OwnedIterOut::empty(),
            parser: simd_doc::Parser::new(),
            inner,
        }
    }
}

pin_project_lite::pin_project! {
    pub struct ReadJsonLines {
        inner: BoxStream<'static, crate::Result<broker::ReadResponse>>,
        parsed: simd_doc::transcoded::OwnedIterOut,
        parser: simd_doc::Parser,
    }
}

impl futures::Stream for ReadJsonLines {
    type Item = crate::Result<ReadJsonLine>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        let me = self.project();

        loop {
            if let Some((root, next_offset)) = me.parsed.next() {
                return Poll::Ready(Some(Ok(ReadJsonLine::Doc { root, next_offset })));
            }

            // Poll the inner stream for the next item
            match me.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(response)) => {
                    let response = match response {
                        Ok(response) => response,
                        Err(err) => return Poll::Ready(Some(Err(err))),
                    };

                    // This is a non-content Fragment response.
                    if let Some(_fragment) = &response.fragment {
                        return Poll::Ready(Some(Ok(ReadJsonLine::Meta(response))));
                    }

                    *me.parsed = me
                        .parser
                        .transcode_chunk(&response.content, response.offset, Default::default())
                        .map_err(|err| Error::Parsing(response.offset, err))?
                        .into_iter();
                }
                std::task::Poll::Ready(None) => return std::task::Poll::Ready(None),
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
    }
}
