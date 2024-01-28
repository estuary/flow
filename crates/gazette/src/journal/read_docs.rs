use super::Client;
use crate::Error;
use futures::{stream::BoxStream, StreamExt};
use proto_gazette::broker;

pub enum Doc {
    Doc {
        offset: i64,
        root: doc::OwnedArchivedNode,
    },
    Fragment(broker::Fragment),
}

impl Client {
    pub fn read_docs(self, req: broker::ReadRequest) -> Docs {
        Docs {
            parsed: simd_doc::output::OwnedIterOut::empty(),
            parser: simd_doc::Parser::new(),
            inner: self.read(req).boxed(),
        }
    }
}

pin_project_lite::pin_project! {
    pub struct Docs {
        inner: BoxStream<'static, crate::Result<broker::ReadResponse>>,
        parsed: simd_doc::output::OwnedIterOut,
        parser: simd_doc::Parser,
    }
}

impl futures::Stream for Docs {
    type Item = crate::Result<Doc>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        let me = self.project();

        loop {
            if let Some((offset, root)) = me.parsed.next() {
                return Poll::Ready(Some(Ok(Doc::Doc { offset, root })));
            }

            // Poll the inner stream for the next item
            match me.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(response)) => {
                    let response = match response {
                        Ok(response) => response,
                        Err(err) => return Poll::Ready(Some(Err(err))),
                    };

                    // This is a non-content Fragment response.
                    if let Some(fragment) = response.fragment {
                        return Poll::Ready(Some(Ok(Doc::Fragment(fragment))));
                    }

                    *me.parsed = me
                        .parser
                        .parse(&response.content, response.offset, Default::default())
                        .map_err(|err| Error::Parsing(response.offset, err))?
                        .into_iter();
                }
                std::task::Poll::Ready(None) => return std::task::Poll::Ready(None),
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
    }
}
