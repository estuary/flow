use super::Client;
use crate::Error;
use futures::{stream::BoxStream, StreamExt};
use proto_gazette::broker;

/// ReadJsonLine is the enumerated Item type of a read_json_lines() Stream.
pub enum ReadJsonLine {
    /// Doc is a document which was parsed at `offset` within the journal.
    Doc {
        offset: i64,
        root: doc::OwnedArchivedNode,
    },
    /// Meta is a metadata response which includes the Fragment currently being read.
    Meta(broker::ReadResponse),
}

impl Client {
    pub fn read_json_lines(self, req: broker::ReadRequest) -> ReadJsonLines {
        ReadJsonLines {
            parsed: simd_doc::transcoded::OwnedIterOut::empty(),
            parser: simd_doc::Parser::new(),
            inner: self.read(req).boxed(),
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
            if let Some((offset, root)) = me.parsed.next() {
                return Poll::Ready(Some(Ok(ReadJsonLine::Doc { offset, root })));
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
