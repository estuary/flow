use super::combiner::Combiner;
use crate::doc::{reduce, Pointer, SchemaIndex};
use estuary_protocol::flow;
use futures::channel::mpsc;
use futures::sink::SinkExt;
use futures::stream::{Stream, StreamExt};
use serde_json::Value;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Expected Open")]
    ExpectedOpen,
    #[error("Expected Continue or EOF")]
    ExpectedContinueOrEOF,
    #[error("parsing URL: {0:?}")]
    Url(#[from] url::ParseError),
    #[error("schema index: {0}")]
    SchemaIndex(#[from] estuary_json::schema::index::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid arena range: {0:?}")]
    InvalidArenaRange(flow::Slice),
    #[error(transparent)]
    ReduceError(#[from] reduce::Error),
    #[error("channel send error: {0:?}")]
    SendError(#[from] mpsc::SendError),
    #[error("recv error from peer: {0}")]
    RecvError(#[from] tonic::Status),
}

async fn combine_rpc(
    schema_index: &'static SchemaIndex<'static>,
    mut rx_request: impl Stream<Item = Result<flow::CombineRequest, tonic::Status>> + Unpin,
    mut tx_response: mpsc::Sender<Result<flow::CombineResponse, tonic::Status>>,
) -> Result<(), Error> {
    let open = match rx_request.next().await {
        // Read Open message.
        Some(Ok(flow::CombineRequest {
            kind: Some(flow::combine_request::Kind::Open(open)),
        })) => open,
        // Read peer Error.
        Some(Err(err)) => return Err(err.into()),
        // Anything else is unexpected.
        _ => return Err(Error::ExpectedOpen),
    };

    let schema_url = url::Url::parse(&open.schema_uri)?;
    schema_index.must_fetch(&schema_url)?;

    let key_pointers: Vec<Pointer> = open.key_ptr.iter().map(Pointer::from).collect();
    let field_pointers: Vec<Pointer> = open.field_ptrs.iter().map(Pointer::from).collect();

    let mut combiner = Combiner::new(schema_index, &schema_url, key_pointers.into());

    // Combine over all documents of request Continue messages, until EOF.
    loop {
        let cont = match rx_request.next().await {
            // Read next Continue message.
            Some(Ok(flow::CombineRequest {
                kind: Some(flow::combine_request::Kind::Continue(cont)),
            })) => cont,
            // Read peer EOF.
            None => break,
            // Read peer Error.
            Some(Err(err)) => return Err(err.into()),
            // Anything else is unexpected.
            _ => return Err(Error::ExpectedContinueOrEOF),
        };

        for doc in cont.docs_json {
            let b = cont
                .arena
                .get(doc.begin as usize..doc.end as usize)
                .ok_or_else(|| Error::InvalidArenaRange(doc.clone()))?;
            let doc: Value = serde_json::from_slice(b)?;

            combiner.combine(doc, open.prune)?;
        }
    }

    // Drain the combiner, aggregating documents into CombineResponses and
    // sending each as a CombineResponse.
    let responses = docs_to_combine_responses(
        1 << 14, // Target arenas of 16k.
        &field_pointers,
        combiner.into_entries(&open.uuid_placeholder_ptr),
    )
    .map(|cr| Ok(Ok(cr)));

    let mut responses = futures::stream::iter(responses);
    tx_response.send_all(&mut responses).await?;

    Ok(())
}

pub fn docs_to_combine_responses<'a>(
    target_arena: usize,
    fields: &'a [Pointer],
    docs: impl Iterator<Item = Value> + 'a,
) -> impl Iterator<Item = flow::CombineResponse> + 'a {
    // We'll use an Option<CombineResponse> as an accumulator.
    let mut opt_resp: Option<flow::CombineResponse> = None;

    docs.map(Option::Some)
        .chain(std::iter::once(None))
        .filter_map(move |opt_doc| {
            let doc = match opt_doc {
                Some(doc) => doc,
                None => return opt_resp.take(),
            };

            let mut resp = match opt_resp.take() {
                // Continue a partial CombineResponse.
                Some(resp) => resp,
                // We must initialize a new CombineResponse.
                None => flow::CombineResponse {
                    // Use capacity of 1.5x the target arena size to minimize
                    // the change of re-allocations.
                    arena: Vec::with_capacity(3 * target_arena / 2),
                    docs_json: Vec::new(),
                    fields: fields
                        .iter()
                        .map(|_| flow::Field { values: Vec::new() })
                        .collect(),
                },
            };

            let begin = resp.arena.len() as u32;
            serde_json::to_writer(&mut resp.arena, &doc)
                .expect("Value should never fail to serialize");

            resp.docs_json.push(flow::Slice {
                begin,
                end: resp.arena.len() as u32,
            });

            for (field, ptr) in resp.fields.iter_mut().zip(fields.iter()) {
                field
                    .values
                    .push(super::extract_field(&mut resp.arena, &doc, ptr));
            }

            if resp.arena.len() >= target_arena {
                Some(resp)
            } else {
                opt_resp = Some(resp);
                None
            }
        })
}

pub struct API {
    schema_index: &'static SchemaIndex<'static>,
}

impl API {
    pub fn new(schema_index: &'static SchemaIndex<'static>) -> API {
        API { schema_index }
    }

    fn spawn_combine_handler(
        &self,
        rx_request: impl Stream<Item = Result<flow::CombineRequest, tonic::Status>>
            + Unpin
            + Send
            + 'static,
    ) -> CombineResponseStream {
        let (mut tx_response, rx_response) = mpsc::channel(1);
        let schema_index = self.schema_index;

        tokio::spawn(async move {
            let fut = combine_rpc(schema_index, rx_request, tx_response.clone());

            if let Err(err) = fut.await {
                log::error!("combine RPC failed: {:?}", err);

                // Make a best-effort attempt to send the error to the peer.
                // We ignore channel disconnect SendErrors.
                let _ = tx_response
                    .send(Err(tonic::Status::internal(format!("{}", err))))
                    .await;
            }
        });

        rx_response
    }
}

pub type CombineResponseStream = mpsc::Receiver<Result<flow::CombineResponse, tonic::Status>>;

#[tonic::async_trait]
impl flow::combine_server::Combine for API {
    type CombineStream = CombineResponseStream;

    async fn combine(
        &self,
        request: tonic::Request<tonic::Streaming<flow::CombineRequest>>,
    ) -> Result<tonic::Response<Self::CombineStream>, tonic::Status> {
        let rx_response = self.spawn_combine_handler(request.into_inner());
        Ok(tonic::Response::new(rx_response))
    }
}

#[cfg(test)]
pub mod test {
    use super::{
        super::combiner::UUID_PLACEHOLDER,
        super::test::{build_min_max_schema, field_to_value},
        *,
    };
    use serde_json::{json, Value};

    #[test]
    fn test_response_grouping() {
        let fields = &["/key".into(), "/bar".into()];
        let docs = vec![
            json!({"key": "one", "bar": 42}),
            json!({"key": "two", "bar": 52}),
            json!({"key": "three", "bar": 62}),
        ];

        // With a target arena of one byte, each document gets its own CombineResponse.
        let responses =
            docs_to_combine_responses(1, fields, docs.clone().into_iter()).collect::<Vec<_>>();
        assert_eq!(responses.len(), 3);

        assert_eq!(
            Value::Array(parse_combine_response(&responses[0])),
            json!([
                [{"key": "one", "bar": 42}, ["one", 42]],
            ]),
        );

        // With a larger target arena, all documents fit in a single response.
        let responses = docs_to_combine_responses(1 << 14, fields, docs.clone().into_iter())
            .collect::<Vec<_>>();
        assert_eq!(responses.len(), 1);

        assert_eq!(
            Value::Array(parse_combine_response(&responses[0])),
            json!([
                // Note that parse_combine_response() sorts on /key.
                [{"key": "one", "bar": 42}, ["one", 42]],
                [{"key": "three", "bar": 62}, ["three", 62]],
                [{"key": "two", "bar": 52}, ["two", 52]],
            ]),
        );
    }

    #[tokio::test]
    async fn test_basic_rpc() {
        let (schema_index, schema_url) = build_min_max_schema();
        let api = API::new(schema_index);

        let (mut tx_request, rx_request) = mpsc::channel(1);
        let mut rx_response = api.spawn_combine_handler(rx_request);

        send_open(
            &mut tx_request,
            flow::combine_request::Open {
                schema_uri: schema_url.as_str().to_owned(),
                key_ptr: vec!["/key".to_owned()],
                field_ptrs: vec!["/min".to_owned(), "/max".to_owned(), "/key".to_owned()],
                uuid_placeholder_ptr: "/foo".to_owned(),
                prune: true,
            },
        )
        .await;

        send_continue(
            &mut tx_request,
            build_continue(vec![
                json!({"key": "one", "min": 3, "max": 3.3}),
                json!({"key": "two", "min": 4, "max": 4.4}),
            ]),
        )
        .await;

        send_continue(
            &mut tx_request,
            build_continue(vec![
                json!({"key": "two", "min": 2, "max": 2.2}),
                json!({"key": "one", "min": 5, "max": 5.5}),
                json!({"key": "three", "min": 6, "max": 6.6}),
            ]),
        )
        .await;

        tx_request.close_channel(); // Send EOF.

        // Expect response of combined documents
        let combined = recv_resp(&mut rx_response).await;
        assert_eq!(
            Value::Array(combined),
            json!([
                [{"foo": UUID_PLACEHOLDER, "key": "one", "min": 3, "max": 5.5}, [3, 5.5, "one"]],
                [{"foo": UUID_PLACEHOLDER, "key": "three", "min": 6, "max": 6.6}, [6, 6.6, "three"]],
                [{"foo": UUID_PLACEHOLDER, "key": "two", "min": 2, "max": 4.4}, [2, 4.4, "two"]],
            ]),
        );

        recv_eof(&mut rx_response).await;
    }

    async fn send_open(
        tx_request: &mut mpsc::Sender<Result<flow::CombineRequest, tonic::Status>>,
        open: flow::combine_request::Open,
    ) {
        tx_request
            .send(Ok(flow::CombineRequest {
                kind: Some(flow::combine_request::Kind::Open(open)),
            }))
            .await
            .unwrap();
    }

    async fn send_continue(
        tx_request: &mut mpsc::Sender<Result<flow::CombineRequest, tonic::Status>>,
        cont: flow::combine_request::Continue,
    ) {
        tx_request
            .send(Ok(flow::CombineRequest {
                kind: Some(flow::combine_request::Kind::Continue(cont)),
            }))
            .await
            .unwrap();
    }

    async fn recv_eof(rx_response: &mut CombineResponseStream) {
        match rx_response.next().await {
            None => (),
            err @ _ => panic!("expected EOF, got: {:?}", err),
        }
    }

    async fn recv_resp(rx_response: &mut CombineResponseStream) -> Vec<Value> {
        let combined = match rx_response.next().await {
            Some(Ok(combined)) => combined,
            err @ _ => panic!("expected CombineResponse, got: {:?}", err),
        };

        parse_combine_response(&combined)
    }

    fn build_continue(content: Vec<Value>) -> flow::combine_request::Continue {
        let mut arena = Vec::new();

        let docs_json = content
            .into_iter()
            .map(|doc| {
                let begin = arena.len() as u32;
                serde_json::to_writer(&mut arena, &doc).unwrap();

                flow::Slice {
                    begin,
                    end: arena.len() as u32,
                }
            })
            .collect();

        flow::combine_request::Continue { arena, docs_json }
    }

    pub fn parse_combine_response(combined: &flow::CombineResponse) -> Vec<Value> {
        let mut out = Vec::new();
        for (doc_index, doc) in combined.docs_json.iter().enumerate() {
            let b = combined
                .arena
                .get(doc.begin as usize..doc.end as usize)
                .unwrap();
            let doc = serde_json::from_slice(b).unwrap();

            let mut values = Vec::new();
            for field in &combined.fields {
                values.push(field_to_value(&combined.arena, &field.values[doc_index]));
            }
            out.push(Value::Array(vec![doc, Value::Array(values)]));
        }

        // Return in stable, sorted order.
        out.sort_by_key(|v| v.pointer("/0/key").unwrap().as_str().unwrap().to_owned());
        out
    }
}
