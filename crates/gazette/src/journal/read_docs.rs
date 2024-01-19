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
            input_offset: 0,
            parsed: simd_doc::OwnedIterOut::empty(),
            input: Vec::new(),
            parser: simd_doc::Parser::new(),
            inner: self.read(req).boxed(),
        }
    }
}

pin_project_lite::pin_project! {
    pub struct Docs {
        inner: BoxStream<'static, crate::Result<broker::ReadResponse>>,
        input: Vec<u8>,
        input_offset: i64,
        parsed: simd_doc::OwnedIterOut,
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
            if let Some((rel_offset, root)) = me.parsed.next() {
                return Poll::Ready(Some(Ok(Doc::Doc {
                    offset: *me.input_offset + rel_offset as i64,
                    root,
                })));
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
                        if !me.input.is_empty() {
                            return Poll::Ready(Some(Err(Error::Protocol("unexpected ReadResponse with Fragment while unparsed input remains"))));
                        } else {
                            return Poll::Ready(Some(Ok(Doc::Fragment(fragment))));
                        }
                    }

                    if me.input.is_empty() {
                        *me.input_offset = response.offset;
                    }
                    me.input.extend_from_slice(&response.content);

                    if !simd_doc::Parser::contains_newline(&response.content) {
                        continue; // `input` doesn't contain a complete document yet.
                    }

                    // `input` contains at least one document (and likely a bunch).
                    let input_len = me.input.len();
                    let mut output = simd_doc::Out::with_capacity(input_len);
                    () = me
                        .parser
                        .parse(me.input, &mut output)
                        .map_err(|err| Error::Json(*me.input_offset, err))?;
                    *me.parsed = output.into_iter();

                    // `input` may contain an unparsed remainder. Update `start_offset` accordingly.
                    *me.input_offset += (input_len - me.input.len()) as i64;
                }
                std::task::Poll::Ready(None) => return std::task::Poll::Ready(None),
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
    }
}
