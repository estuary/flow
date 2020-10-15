use super::combine_api::docs_to_combine_responses;
use super::combiner::Combiner;
use super::context::{Context, Transform};
use super::lambda;
use super::pipeline::PendingPipeline;
use super::registers::{self, Registers};
use crate::doc;
use estuary_json::validator;
use estuary_protocol::{consumer, flow, recoverylog};
use futures::channel::mpsc;
use futures::sink::SinkExt;
use futures::stream::{Stream, StreamExt, TryStreamExt};
use futures::TryFutureExt;
use itertools::izip;
use std::default::Default;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Lambda invocation error: {0}")]
    Lambda(#[from] lambda::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Schema index: {0}")]
    SchemaIndex(#[from] estuary_json::schema::index::Error),
    #[error("channel send error: {0:?}")]
    SendError(#[from] mpsc::SendError),
    #[error("recv error from peer: {0}")]
    RecvError(#[from] tonic::Status),
    #[error("register error: {0}")]
    RegisterErr(#[from] registers::Error),

    #[error("Unexpected collection")]
    UnexpectedCollection,
    #[error("Expected Open")]
    ExpectedOpen,
    #[error("Expected Continue or Flush")]
    ExpectedContinueOrFlush,
    #[error("Expected Prepare")]
    ExpectedPrepare,
    #[error("Expected EOF")]
    ExpectedEOF,
    #[error("Unknown transform ID: {0}")]
    UnknownTransformID(i32),
    #[error("invalid arena range: {0:?}")]
    InvalidArenaRange(flow::Slice),
    #[error("register validation error: {}", serde_json::to_string_pretty(.0).unwrap())]
    RegisterValidation(doc::FailedValidation),
    #[error("source document validation error: {}", serde_json::to_string_pretty(.0).unwrap())]
    SourceValidation(doc::FailedValidation),
    #[error("derived document validation error: {}", serde_json::to_string_pretty(.0).unwrap())]
    DerivedValidation(doc::FailedValidation),
}

/// Convert document JSON slices referencing the |arena| into parsed Values,
/// where each is validated by the associated transform source schema.
fn extract_validated_sources<C: validator::Context>(
    validator: &mut doc::Validator<C>,
    arena: &[u8],
    docs_json: &[flow::Slice],
    transforms: &[&Transform],
) -> Result<Vec<serde_json::Value>, Error> {
    transforms
        .iter()
        .zip(docs_json.iter())
        .map(move |(tf, s)| {
            let b = arena
                .get(s.begin as usize..s.end as usize)
                .ok_or_else(|| Error::InvalidArenaRange(s.clone()))?;
            let doc: serde_json::Value = serde_json::from_slice(b)?;

            doc::validate(validator, &tf.source_schema, &doc).map_err(Error::SourceValidation)?;

            Ok(doc)
        })
        .collect()
}

/// Start "update" lambda invocations for each document and associated
/// transform, returning a future which resolves when all invocations
/// have completed.
///
/// The returned vector has one element per input document, with zero
/// or more Value columns produced by that document's update transform.
fn derive_register_deltas<'a>(
    transforms: &'a [Transform],
    docs: &'a [serde_json::Value],
    doc_transforms: &'a [&Transform],
) -> Result<impl Future<Output = Result<Vec<Vec<serde_json::Value>>, Error>> + 'a, Error> {
    // Start concurrent "update" Lambda invocations for each transform.
    let mut updates = transforms
        .iter()
        .map(|tf| tf.update.start_invocation())
        .collect::<Vec<_>>();

    // Scatter documents to their respective update lambdas.
    for (transform, doc) in doc_transforms.iter().zip(docs.iter()) {
        let inv = &mut updates[transform.index];
        inv.start_row();
        inv.add_column(&doc)?;
        inv.finish_row();
    }

    // Dispatch invocations, and collect into a FuturesOrdered so that request Futures
    // progress in parallel and yield results in transform order. Wait for all to complete.
    Ok(updates
        .into_iter()
        .map(|inv| inv.finish())
        .collect::<futures::stream::FuturesOrdered<_>>()
        .try_collect()
        .err_into()
        .and_then(move |responses| {
            futures::future::ready(collect_rows(&doc_transforms, responses))
        }))
}

/// Start "publish" lambda invocations for each document and associated transform,
/// after first applying register deltas to supply a "current" and (where applicable)
/// "previous" register value with the invocation.
fn derive_publish_docs<'fut, 'tmp>(
    transforms: &'fut [Transform],
    registers: &'tmp mut Registers,
    register_deltas: Vec<Vec<serde_json::Value>>,
    doc_values: &'tmp [serde_json::Value],
    doc_transforms: &'fut [&Transform],
    doc_keys: &'tmp [&'tmp [u8]],
) -> Result<impl Future<Output = Result<Vec<Vec<serde_json::Value>>, Error>> + 'fut, Error> {
    // Load all registers in |keys|, so that we may read them below.
    registers.load(doc_keys.iter().copied())?;

    // Start concurrent "publish" lambdas for each transform.
    let mut publishes: Vec<lambda::Invocation> = transforms
        .iter()
        .map(|tf| tf.publish.start_invocation())
        .collect::<Vec<_>>();

    // Scatter documents to their respective "publish" lambdas, updating registers as we go.
    for (transform, key, doc, register_deltas) in itertools::izip!(
        doc_transforms,
        doc_keys,
        doc_values,
        register_deltas.into_iter()
    ) {
        let inv = &mut publishes[transform.index];
        inv.start_row();

        // Send the source document itself.
        inv.add_column(&doc)?;
        // Send the value of the register from before the document's update, if any.
        inv.add_column(&registers.read(key))?;
        // If there are updates, apply them and send the updated register.
        // If not, send an explicit Null.
        if !register_deltas.is_empty() {
            registers
                .reduce(key, register_deltas.into_iter())
                .map_err(Error::RegisterValidation)?;

            // Send the updated register value.
            inv.add_column(&registers.read(key))?;
        } else {
            inv.add_column(&serde_json::Value::Null)?;
        }
        inv.finish_row();
    }

    Ok(publishes
        .into_iter()
        .map(|inv| inv.finish())
        .collect::<futures::stream::FuturesOrdered<_>>()
        .try_collect()
        .err_into()
        .and_then(move |responses| {
            futures::future::ready(collect_rows(&doc_transforms, responses))
        }))
}

// Collect document transformations, returned across multiple lambda invocations,
// and map through each document's transform, arriving at a projected flat array
// of the transformations obtained from each source document.
fn collect_rows(
    transforms: &[&Transform],
    responses: Vec<impl Iterator<Item = Result<Vec<serde_json::Value>, lambda::Error>>>,
) -> Result<Vec<Vec<serde_json::Value>>, Error> {
    let mut out: Vec<Vec<serde_json::Value>> = Vec::new();
    out.resize_with(transforms.len(), Default::default);

    for (tf_index, rows) in responses.into_iter().enumerate() {
        let doc_index = transforms.iter().enumerate().filter_map(|(doc_index, tf)| {
            if tf.index == tf_index {
                Some(doc_index)
            } else {
                None
            }
        });

        for (row, doc_index) in rows.zip(doc_index) {
            out[doc_index] = row?;
        }
    }
    Ok(out)
}

pub struct API {
    ctx: Arc<Context>,
    registers: std::sync::Mutex<PendingPipeline<Registers>>,
}

pub type DeriveResponseStream = mpsc::Receiver<Result<flow::DeriveResponse, tonic::Status>>;

async fn process_continue(
    ctx: Arc<Context>,
    cont: flow::derive_request::Continue,
    registers: PendingPipeline<Registers>,
    combiner: PendingPipeline<Combiner>,
) -> Result<(), Error> {
    // Extract a column of mapped &Transform instances.
    let doc_transforms =
        map_transforms(&ctx.transforms, &cont.transform_id).collect::<Result<Vec<_>, _>>()?;
    // Extract a column of parsed & validated source documents, as Value instances.
    let mut val = doc::Validator::<validator::FullContext>::new(&ctx.schema_index);
    let doc_values =
        extract_validated_sources(&mut val, &cont.arena, &cont.docs_json, &doc_transforms)?;
    // Extract packed keys as &[u8] slices.
    let doc_keys = map_slices(&cont.arena, &cont.packed_key).collect::<Result<Vec<_>, _>>()?;

    // Start invocations of update transforms, then gather deltas from all invocations.
    let register_deltas =
        derive_register_deltas(&ctx.transforms, &doc_values, &doc_transforms)?.await?;

    // Now that we have deltas in-hand, receive |registers| from the
    // processing task ordered ahead of us.
    let mut registers = registers.recv().await;
    // Build publish lambda invocations, applying register deltas as we go.
    // This returns a future response of those invocations, and does not block.
    let derivations = derive_publish_docs(
        &ctx.transforms,
        registers.as_mut(),
        register_deltas,
        &doc_values,
        &doc_transforms,
        &doc_keys,
    )?;
    // Release |registers| to the processing task ordered behind us.
    std::mem::drop(registers);
    // Gather derived documents emitted by publish lambdas.
    let derivations = derivations.await?;

    // Like register deltas, now that we have derived documents in-hand,
    // receive |combiner| from the task ordered ahead of us.
    let mut combiner = combiner.recv().await;
    for doc in derivations.into_iter().flatten() {
        combiner
            .as_mut()
            .combine(doc)
            .map_err(Error::DerivedValidation)?;
    }
    // Release |combiner| to the processing task ordered behind us.
    std::mem::drop(combiner);

    Ok(())
}

async fn derive_rpc(
    ctx: Arc<Context>,
    mut registers: PendingPipeline<Registers>,
    mut rx_request: impl Stream<Item = Result<flow::DeriveRequest, tonic::Status>> + Unpin,
    mut tx_response: mpsc::Sender<Result<flow::DeriveResponse, tonic::Status>>,
) -> Result<(), Error> {
    // Read open request.
    let open = match rx_request.next().await {
        Some(Ok(flow::DeriveRequest {
            kind: Some(flow::derive_request::Kind::Open(open)),
        })) => open,
        _ => return Err(Error::ExpectedOpen),
    };
    if open.collection != ctx.derivation_name {
        return Err(Error::UnexpectedCollection);
    }

    // We'll next read zero or more Continue messages, followed by a closing Flush.
    // Each Continue will begin a new and concurrent execution task, tracked in |pending|.
    let mut pending = futures::stream::FuturesUnordered::new();

    // All Continue messages will use a shared Combiner, which is drained and emitted upon flush.
    let combiner = Combiner::new(
        ctx.schema_index,
        &ctx.derivation_schema,
        ctx.derivation_key.clone(),
    );
    let mut combiner = PendingPipeline::new(combiner);

    // On a Continue, we start and return a new task Future (which will be added to |pending|).
    let mut on_continue = |cont: flow::derive_request::Continue| {
        process_continue(
            ctx.clone(),
            cont,
            registers.chain_before(),
            combiner.chain_before(),
        )
    };
    // On completing a Continue, we inform the client by sending an ACK Continue back.
    // The client can apply flow control by bounding the number of in-flight Continue
    // messages, and this acknowledgement "opens" the window for a next client Continue.
    let ack = Ok(flow::DeriveResponse {
        kind: Some(flow::derive_response::Kind::Continue(
            flow::derive_response::Continue {},
        )),
    });

    let mut rx_stream = rx_request.fuse();
    let flush: flow::derive_request::Flush = loop {
        futures::select! {
            completion = pending.select_next_some() => match completion {
                // Case: a |pending| Continue has completed processing.
                Ok(()) => tx_response.send(ack.clone()).await?,
                // Error: a |pending| Continue failed.
                Err(err) => return Err(err),
            },
            rx = rx_stream.next() => match rx {
                // Case: we read a Continue message.
                Some(Ok(flow::DeriveRequest {
                    kind: Some(flow::derive_request::Kind::Continue(cont)),
                })) => pending.push(on_continue(cont)),
                // Case: we read a Flush message.
                Some(Ok(flow::DeriveRequest {
                    kind: Some(flow::derive_request::Kind::Flush(flush)),
                })) => break flush,
                // Case: we read an error from the peer.
                Some(Err(err)) => return Err(Error::RecvError(err)),
                // Error: we read an unexpected message.
                _ => return Err(Error::ExpectedContinueOrFlush),
            }
        };
    };

    // We've read a Flush. Drain remaining |pending| tasks.
    while let Some(completion) = pending.next().await {
        match completion {
            // Case: a |pending| Continue has completed processing.
            Ok(()) => tx_response.send(ack.clone()).await?,
            // Error: a |pending| Continue failed.
            Err(err) => return Err(err),
        }
    }

    // Drain the Combiner, aggregating documents into CombineResponses and
    // sending each as a Flush DeriveResponse message variant.
    let combiner = combiner.recv().await.into_inner();

    // Drain the Combiner, extracting the given fields and inserting a
    // UUID placeholder at the given pointer.
    let fields = flush
        .field_ptrs
        .iter()
        .map(doc::Pointer::from)
        .collect::<Vec<_>>();

    let responses = combiner.into_entries(&flush.uuid_placeholder_ptr);

    let responses = docs_to_combine_responses(
        1 << 14, // Target arenas of 16k.
        &fields,
        responses,
    )
    .map(|cr| {
        Ok(Ok(flow::DeriveResponse {
            kind: Some(flow::derive_response::Kind::Flush(cr)),
        }))
    })
    // Send a trailing, empty CombineResponse to indicate that the flush has completed.
    .chain(std::iter::once(Ok(Ok(flow::DeriveResponse {
        kind: Some(flow::derive_response::Kind::Flush(
            flow::CombineResponse::default(),
        )),
    }))));

    tx_response
        .clone()
        .send_all(&mut futures::stream::iter(responses))
        .await?;

    // Read Prepare request with a Checkpoint.
    let checkpoint = match rx_stream.next().await {
        Some(Ok(flow::DeriveRequest {
            kind:
                Some(flow::derive_request::Kind::Prepare(flow::derive_request::Prepare {
                    checkpoint: Some(checkpoint),
                })),
        })) => checkpoint,
        Some(Err(err)) => return Err(Error::RecvError(err)),
        _ => return Err(Error::ExpectedPrepare),
    };

    let mut registers = registers.recv().await;
    let tx_commit = registers.as_mut().prepare(checkpoint)?;

    // Pass back control of registers to the caller / the next RPC.
    std::mem::drop(registers);

    // Read (only) a clean EOF to commit the derive transaction.
    match rx_stream.next().await {
        None => tx_commit.send(()).expect("failed to signal commit"),
        Some(Err(err)) => return Err(Error::RecvError(err)),
        Some(Ok(_)) => return Err(Error::ExpectedEOF),
    };

    Ok(())
}

impl API {
    pub fn new(ctx: Arc<Context>, registers: Registers) -> API {
        let registers = PendingPipeline::new(registers);
        let registers = std::sync::Mutex::new(registers);

        API { ctx, registers }
    }

    fn spawn_derive_handler(
        &self,
        rx_request: impl Stream<Item = Result<flow::DeriveRequest, tonic::Status>>
            + Unpin
            + Send
            + 'static,
    ) -> DeriveResponseStream {
        let (mut tx_response, rx_response) = mpsc::channel(1);

        // We'll pass registers to this stream via channel, and create a return
        // channel for it to return registers back to us once the stream has
        // fully completed. This imposes a total ordering of derive requests,
        // since a following request must block for registers until the prior
        // request has completed.
        let registers = self.registers.lock().unwrap().chain_before();
        let ctx = self.ctx.clone();

        tokio::spawn(async move {
            let fut = derive_rpc(ctx, registers, rx_request, tx_response.clone());

            if let Err(err) = fut.await {
                log::error!("derive RPC failed: {:?}", err);

                // Make a best-effort attempt to send the error to the peer.
                // We ignore channel disconnect SendErrors.
                let _ = tx_response
                    .send(Err(tonic::Status::internal(format!("{}", err))))
                    .await;
            }
        });

        rx_response
    }

    async fn last_checkpoint(&self) -> Result<consumer::Checkpoint, Error> {
        let registers = self.registers.lock().unwrap().chain_before();
        let registers = registers.recv().await;
        Ok(registers.as_ref().last_checkpoint()?)
    }

    async fn clear_registers(&self) -> Result<(), Error> {
        let registers = self.registers.lock().unwrap().chain_before();
        let mut registers = registers.recv().await;
        registers.as_mut().clear().map_err(Into::into)
    }
}

#[tonic::async_trait]
impl flow::derive_server::Derive for API {
    type DeriveStream = DeriveResponseStream;

    async fn derive(
        &self,
        request: tonic::Request<tonic::Streaming<flow::DeriveRequest>>,
    ) -> Result<tonic::Response<Self::DeriveStream>, tonic::Status> {
        let rx_response = self.spawn_derive_handler(request.into_inner());
        Ok(tonic::Response::new(rx_response))
    }

    async fn restore_checkpoint(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<consumer::Checkpoint>, tonic::Status> {
        self.last_checkpoint()
            .await
            .map(tonic::Response::new)
            .map_err(|err| tonic::Status::internal(format!("{}", err)))
    }

    async fn build_hints(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<recoverylog::FsmHints>, tonic::Status> {
        // TODO(johnny): Requires wiring up recoverylog recorder.
        Ok(tonic::Response::new(recoverylog::FsmHints::default()))
    }

    async fn clear_registers(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        match self.clear_registers().await {
            Ok(()) => Ok(tonic::Response::new(())),
            Err(err) => Err(tonic::Status::internal(format!("{}", err))),
        }
    }
}

fn map_transforms<'a>(
    transforms: &'a [Transform],
    transform_ids: &'a [i32],
) -> impl Iterator<Item = Result<&'a Transform, Error>> + 'a {
    transform_ids.iter().map(move |id| {
        transforms
            .iter()
            .find(|t| t.transform_id == *id)
            .ok_or_else(|| Error::UnknownTransformID(*id))
    })
}

fn map_slices<'a>(
    arena: &'a [u8],
    slices: &'a [flow::Slice],
) -> impl Iterator<Item = Result<&'a [u8], Error>> + 'a {
    slices.iter().map(move |s| {
        arena
            .get(s.begin as usize..s.end as usize)
            .ok_or_else(|| Error::InvalidArenaRange(s.clone()))
    })
}

#[cfg(test)]
mod test {
    use super::{
        super::combiner::UUID_PLACEHOLDER,
        super::test::{build_test_rocks, LambdaTestServer},
        *,
    };
    use insta::assert_snapshot;
    use serde_json::{json, Value};
    use url::Url;

    #[tokio::test]
    async fn test_basic_rpc() {
        let mut api = TestAPI::new().await;
        let (mut tx_request, mut rx_response) = api.start_derive();

        send_open(&mut tx_request, "a/derived/collection").await;

        send_continue(
            &mut tx_request,
            build_continue(vec![
                (TF_INC, json!({"key": "a"})),  // => 1.
                (TF_INC, json!({"key": "a"})),  // => 2.
                (TF_INC, json!({"key": "bb"})), // => 1.
                (TF_PUB, json!({"key": "bb"})), // Pub 1.
                (TF_PUB, json!({"key": "a"})),  // Pub 2.
                (TF_INC, json!({"key": "bb"})), // => 2.
                (TF_INC, json!({"key": "bb"})), // => 3.
            ]),
        )
        .await;

        send_continue(
            &mut tx_request,
            build_continue(vec![
                (TF_PUB, json!({"key": "ccc"})),
                (TF_INC, json!({"key": "bb"})),              // => 4.
                (TF_RST, json!({"key": "bb", "reset": 15})), // Pub 4, => 15.
                (TF_INC, json!({"key": "bb"})),              // => 16.
                (TF_RST, json!({"key": "a", "reset": 0})),   // Pub 2, => 0.
                (TF_INC, json!({"key": "a"})),               // => 1.
                (TF_INC, json!({"key": "a"})),               // => 2.
                (TF_PUB, json!({"key": "a"})),               // Pub 2.
                (TF_PUB, json!({"key": "bb"})),              // Pub 16.
            ]),
        )
        .await;

        send_flush(&mut tx_request).await;

        recv_continue(&mut rx_response).await;
        recv_continue(&mut rx_response).await;

        // Expect flush of derived documents.
        let combined = recv_flush(&mut rx_response).await;
        assert_eq!(
            Value::Array(combined),
            json!([
                [{"_uuid": UUID_PLACEHOLDER, "key": "a", "reset": 0, "values": [1002, 1002, 2]}, [0, "a"]],
                [{"_uuid": UUID_PLACEHOLDER, "key": "bb", "reset": 15, "values": [1001, 1004, 16]}, [15, "bb"]],
                [{"_uuid": UUID_PLACEHOLDER, "key": "ccc", "values": [1000]}, [null, "ccc"]],
            ])
        );
        // Expect an empty Flush, which signals that flush is complete.
        assert!(recv_flush(&mut rx_response).await.is_empty());

        send_prepare(&mut tx_request).await;

        tx_request.close_channel();
        recv_eof(&mut rx_response).await;

        // Expect we can restore the Checkpoint just-written.
        let _ = api.api.last_checkpoint().await.unwrap();
        // And that we can clear registers.
        let _ = api.api.clear_registers().await.unwrap();
    }

    #[tokio::test]
    async fn test_raced_requests() {
        let mut api = TestAPI::new().await;

        let (mut tx_one, mut rx_one) = api.start_derive();
        let (mut tx_two, mut rx_two) = api.start_derive();

        for tx in &mut [&mut tx_one, &mut tx_two] {
            send_open(tx, "a/derived/collection").await;
        }

        // Send and flush to RPC #2, which is processed after RPC #1.
        send_continue(
            &mut tx_two,
            build_continue(vec![
                (TF_INC, json!({"key": "a"})), // => 45.
                (TF_PUB, json!({"key": "a"})), // Pub 45.
            ]),
        )
        .await;

        send_flush(&mut tx_two).await;

        // Send and flush to RPC #1.
        send_continue(
            &mut tx_one,
            build_continue(vec![
                (TF_INC, json!({"key": "a"})),              // => 1001.
                (TF_INC, json!({"key": "bb"})),             // => 1001.
                (TF_RST, json!({"key": "a", "reset": 42})), // Pub 1001, reset 42.
                (TF_INC, json!({"key": "bb"})),             // => 1002.
            ]),
        )
        .await;

        send_continue(
            &mut tx_one,
            build_continue(vec![
                (TF_INC, json!({"key": "a"})),  // => 43.
                (TF_PUB, json!({"key": "a"})),  // Pub 43.
                (TF_INC, json!({"key": "a"})),  // => 44.
                (TF_PUB, json!({"key": "bb"})), // Pub 1002.
            ]),
        )
        .await;

        send_flush(&mut tx_one).await;

        // Registers are released to RPC #2 only upon seeing a "prepare".
        send_prepare(&mut tx_one).await;

        // Read continues and flushed, derived docs from RPC #1.
        recv_continue(&mut rx_one).await;
        recv_continue(&mut rx_one).await;

        // Expect flush of derived documents.
        let combined = recv_flush(&mut rx_one).await;
        assert_eq!(
            Value::Array(combined),
            json!([
                [{"_uuid": UUID_PLACEHOLDER, "key": "a", "reset": 42, "values": [1001, 43]}, [42, "a"]],
                [{"_uuid": UUID_PLACEHOLDER, "key": "bb", "values": [1002]}, [null, "bb"]],
            ])
        );
        // Expect an empty Flush, which signals that flush is complete.
        assert!(recv_flush(&mut rx_one).await.is_empty());

        // Read one continue and flushed docs from RPC #2.
        recv_continue(&mut rx_two).await;

        // Expect flush of derived documents.
        let combined = recv_flush(&mut rx_two).await;
        assert_eq!(
            Value::Array(combined),
            json!([
                [{"_uuid": UUID_PLACEHOLDER, "key": "a", "values": [45]}, [null, "a"]],
            ])
        );
        // Expect an empty Flush, which signals that flush is complete.
        assert!(recv_flush(&mut rx_two).await.is_empty());
        send_prepare(&mut tx_two).await;

        // Close to signal commit of both RPCs.
        tx_one.close_channel();
        tx_two.close_channel();
        recv_eof(&mut rx_one).await;
        recv_eof(&mut rx_two).await;
    }

    #[tokio::test]
    async fn test_rpc_error_cases() {
        let mut api = TestAPI::new().await;

        // Case: malformed open.
        let (mut tx, mut rx) = api.start_derive();
        send_open(&mut tx, "the/wrong/collection").await;

        assert_snapshot!(recv_error(&mut rx).await, @"Unexpected collection");
        recv_eof(&mut rx).await;

        // Case: source document doesn't validate.
        let (mut tx, mut rx) = api.start_derive();

        send_open(&mut tx, "a/derived/collection").await;
        send_continue(
            &mut tx,
            build_continue(vec![
                (TF_INC, json!({"missing": "required /key"})), // => 43.
            ]),
        )
        .await;

        assert_snapshot!(recv_error(&mut rx).await, @r###"
        source document validation error: {
          "document": {
            "missing": "required /key"
          },
          "basic_output": {
            "errors": [
              {
                "absoluteKeywordLocation": "https://schema/#/$defs/source",
                "error": "Invalid(Required { props: [\"key\"], props_interned: 1 })",
                "instanceLocation": "",
                "keywordLocation": "#"
              }
            ],
            "valid": false
          }
        }
        "###);
        recv_eof(&mut rx).await;

        // Case: derived register document doesn't validate.
        let (mut tx, mut rx) = api.start_derive();

        send_open(&mut tx, "a/derived/collection").await;
        send_continue(
            &mut tx,
            build_continue(vec![(TF_RST, json!({"key": "foobar", "reset": -1}))]),
        )
        .await;

        assert_snapshot!(recv_error(&mut rx).await, @r###"
        register validation error: {
          "document": {
            "type": "set",
            "value": "negative one!"
          },
          "basic_output": {
            "errors": [
              {
                "absoluteKeywordLocation": "https://schema/#/$defs/register",
                "error": "OneOfNotMatched",
                "instanceLocation": "",
                "keywordLocation": "#"
              }
            ],
            "valid": false
          }
        }
        "###);
        recv_eof(&mut rx).await;

        // Case: derived document doesn't validate.
        let (mut tx, mut rx) = api.start_derive();

        send_open(&mut tx, "a/derived/collection").await;
        send_continue(
            &mut tx,
            build_continue(vec![(
                TF_PUB,
                json!({"key": "foobar", "invalid-property": 42}),
            )]),
        )
        .await;

        assert_snapshot!(recv_error(&mut rx).await, @r###"
        derived document validation error: {
          "document": {
            "invalid-property": 42,
            "key": "foobar",
            "values": [
              1000
            ]
          },
          "basic_output": {
            "errors": [
              {
                "absoluteKeywordLocation": "https://schema/#/$defs/derived/properties/invalid-property",
                "error": "Invalid(False)",
                "instanceLocation": "/invalid-property",
                "keywordLocation": "#/properties/invalid-property"
              }
            ],
            "valid": false
          }
        }
        "###);
        recv_eof(&mut rx).await;
    }

    // Short-hand constants for transform IDs used in the test fixture.
    const TF_INC: i32 = 32;
    const TF_PUB: i32 = 42;
    const TF_RST: i32 = 52;

    struct TestAPI {
        api: API,
        // Hold LambdaTestServer & TempDir for drop() side-effects.
        _do_increment: LambdaTestServer,
        _do_publish: LambdaTestServer,
        _do_reset: LambdaTestServer,
        _db_tmpdir: tempfile::TempDir,
    }

    impl TestAPI {
        async fn new() -> TestAPI {
            let schema = json!({
                "$defs": {
                    "source": {
                        "type": "object",
                        "properties": {
                            "key": {"type": "string"},
                            "reset": {"type": "integer"},
                        },
                        "required": ["key"],
                    },
                    "register": {
                        "type": "object",
                        "reduce": {"strategy": "merge"},

                        "oneOf": [
                            {
                                "properties": {
                                    "type": {"const": "set"},
                                    "value": {
                                        "type": "integer",
                                        "reduce": {"strategy": "lastWriteWins"},
                                    },
                                },
                            },
                            {
                                "properties": {
                                    "type": {"const": "add"},
                                    "value": {
                                        "type": "integer",
                                        "reduce": {"strategy": "sum"},
                                    },
                                },
                            },
                        ],
                        "required": ["type", "value"],
                    },
                    "derived": {
                        "$ref": "#/$defs/source",
                        "reduce": {"strategy": "merge"},

                        "properties": {
                            "values": {
                                "type": "array",
                                "items": {"type": "integer"},
                                "reduce": {"strategy": "append"},
                            },
                            "invalid-property": false,
                        },
                        "required": ["values"],
                    },
                }
            });

            // Build and index the schema, leaking for `static lifetime.
            let schema_url = Url::parse("https://schema").unwrap();
            let schema: doc::Schema =
                estuary_json::schema::build::build_schema(schema_url.clone(), &schema).unwrap();
            let schema = Box::leak(Box::new(schema));

            let mut schema_index = doc::SchemaIndex::new();
            schema_index.add(schema).unwrap();
            schema_index.verify_references().unwrap();
            let schema_index = Box::leak(Box::new(schema_index));

            // Build a lambda which increments the current register value by one.
            let do_increment = LambdaTestServer::start(|_| {
                // Return two register updates with an effective increment of 1.
                vec![
                    json!({"type": "add", "value": 3}),
                    json!({"type": "add", "value": -2}),
                ]
            });
            // Build a lambda which resets the register from a value of the source document.
            let do_reset = LambdaTestServer::start(|doc| {
                let to = doc[0].pointer("/reset").unwrap().as_i64().unwrap();

                // Emit an invalid register document on seeing value -1.
                if to == -1 {
                    vec![json!({"type": "set", "value": "negative one!"})]
                } else {
                    vec![json!({"type": "set", "value": to})]
                }
            });
            // Build a lambda which joins the source with its current register.
            let do_publish = LambdaTestServer::start(|args| {
                let (src, prev, _next) = (
                    args.get(0).unwrap(),
                    args.get(1).unwrap(),
                    args.get(2).unwrap(),
                );

                // Join |src| with the register value before its update.
                let mut doc = src.as_object().unwrap().clone();
                doc.insert(
                    "values".to_owned(),
                    json!([prev.pointer("/value").unwrap().clone()]),
                );

                vec![Value::Object(doc)]
            });

            let transforms = vec![
                // Transform which increments the register.
                Transform {
                    transform_id: TF_INC,
                    source_schema: schema_url.join("#/$defs/source").unwrap(),
                    update: do_increment.lambda.clone(),
                    publish: lambda::Lambda::Noop,
                    index: 0,
                },
                // Transform which publishes the current register.
                Transform {
                    transform_id: TF_PUB,
                    source_schema: schema_url.join("#/$defs/source").unwrap(),
                    update: lambda::Lambda::Noop,
                    publish: do_publish.lambda.clone(),
                    index: 1,
                },
                // Transform which resets the register, and publishes its prior value.
                Transform {
                    transform_id: TF_RST,
                    source_schema: schema_url.join("#/$defs/source").unwrap(),
                    update: do_reset.lambda.clone(),
                    publish: do_publish.lambda.clone(),
                    index: 2,
                },
            ];

            let ctx = Arc::new(Context {
                transforms,
                schema_index,

                derivation_id: 1234,
                derivation_name: "a/derived/collection".to_owned(),
                derivation_schema: schema_url.join("#/$defs/derived").unwrap(),
                derivation_key: vec!["/key".into()].into(),

                register_schema: schema_url.join("#/$defs/register").unwrap(),
                register_initial: json!({"value": 1000}),
            });

            let (db_tmpdir, db) = build_test_rocks();

            let registers = Registers::new(
                db,
                schema_index,
                &schema_url.join("#/$defs/register").unwrap(),
                ctx.register_initial.clone(),
            );

            TestAPI {
                api: API::new(ctx, registers),
                _do_increment: do_increment,
                _do_publish: do_publish,
                _do_reset: do_reset,
                _db_tmpdir: db_tmpdir,
            }
        }

        fn start_derive(
            &mut self,
        ) -> (
            mpsc::Sender<Result<flow::DeriveRequest, tonic::Status>>,
            DeriveResponseStream,
        ) {
            let (tx_request, rx_request) = mpsc::channel(1);
            let rx_response = self.api.spawn_derive_handler(rx_request);
            (tx_request, rx_response)
        }
    }

    async fn send_open(
        tx_request: &mut mpsc::Sender<Result<flow::DeriveRequest, tonic::Status>>,
        collection: &str,
    ) {
        tx_request
            .send(Ok(flow::DeriveRequest {
                kind: Some(flow::derive_request::Kind::Open(
                    flow::derive_request::Open {
                        collection: collection.to_owned(),
                    },
                )),
            }))
            .await
            .unwrap();
    }

    async fn send_continue(
        tx_request: &mut mpsc::Sender<Result<flow::DeriveRequest, tonic::Status>>,
        cont: flow::derive_request::Continue,
    ) {
        tx_request
            .send(Ok(flow::DeriveRequest {
                kind: Some(flow::derive_request::Kind::Continue(cont)),
            }))
            .await
            .unwrap();
    }

    async fn recv_continue(rx_response: &mut DeriveResponseStream) {
        match rx_response.next().await {
            Some(Ok(flow::DeriveResponse {
                kind: Some(flow::derive_response::Kind::Continue(_)),
            })) => (),
            err @ _ => panic!("expected continue, got: {:?}", err),
        };
    }

    async fn send_flush(tx_request: &mut mpsc::Sender<Result<flow::DeriveRequest, tonic::Status>>) {
        tx_request
            .send(Ok(flow::DeriveRequest {
                kind: Some(flow::derive_request::Kind::Flush(
                    flow::derive_request::Flush {
                        uuid_placeholder_ptr: "/_uuid".to_owned(),
                        field_ptrs: vec!["/reset".to_owned(), "/key".to_owned()],
                    },
                )),
            }))
            .await
            .unwrap();
    }

    async fn recv_flush(rx_response: &mut DeriveResponseStream) -> Vec<Value> {
        let combined = match rx_response.next().await {
            Some(Ok(flow::DeriveResponse {
                kind: Some(flow::derive_response::Kind::Flush(combined)),
            })) => combined,
            err @ _ => panic!("expected flush CombineResponse, got: {:?}", err),
        };
        super::super::combine_api::test::parse_combine_response(&combined)
    }

    async fn send_prepare(
        tx_request: &mut mpsc::Sender<Result<flow::DeriveRequest, tonic::Status>>,
    ) {
        tx_request
            .send(Ok(flow::DeriveRequest {
                kind: Some(flow::derive_request::Kind::Prepare(
                    flow::derive_request::Prepare {
                        checkpoint: Some(consumer::Checkpoint::default()),
                    },
                )),
            }))
            .await
            .unwrap();
    }

    async fn recv_eof(rx_response: &mut DeriveResponseStream) {
        match rx_response.next().await {
            None => (),
            err @ _ => panic!("expected EOF, got: {:?}", err),
        }
    }

    async fn recv_error(rx_response: &mut DeriveResponseStream) -> String {
        match rx_response.next().await {
            Some(Err(err)) => err.message().to_owned(),
            err @ _ => panic!("expected EOF, got: {:?}", err),
        }
    }

    fn build_continue(content: Vec<(i32, Value)>) -> flow::derive_request::Continue {
        let mut arena = Vec::new();

        let transform_id = content.iter().map(|(id, _)| *id).collect();

        let docs_json = content
            .iter()
            .map(|(_, doc)| {
                let begin = arena.len() as u32;
                serde_json::to_writer(&mut arena, doc).unwrap();

                flow::Slice {
                    begin,
                    end: arena.len() as u32,
                }
            })
            .collect();

        let packed_key = content
            .iter()
            .map(|(_, doc)| {
                let begin = arena.len() as u32;

                if let Some(Value::String(key)) = doc.pointer("/key") {
                    arena.extend(key.as_bytes().iter());
                } else {
                    arena.extend(b"<missing>".iter());
                }

                flow::Slice {
                    begin,
                    end: arena.len() as u32,
                }
            })
            .collect();

        flow::derive_request::Continue {
            arena,
            transform_id,
            docs_json,
            packed_key,
            // TODO(johnny): We'll eventually need these for their RClocks,
            // in order to filter documents to be published. For now we ignore.
            uuid_parts: Vec::new(),
        }
    }
}
