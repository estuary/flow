use std::net::IpAddr;

use crate::inference::infer_shape;
use crate::json_decoder::JsonCodec;
use crate::schema::SchemaBuilder;
use crate::shape;

use anyhow::Context;
use assemble::journal_selector;
use doc::inference::Shape;
use futures::{Stream, TryStreamExt};
use journal_client::broker::{fragments_response, FragmentsRequest, JournalSpec};
use journal_client::fragments::FragmentIter;
use journal_client::list::list_journals;
use journal_client::read::uncommitted::{
    ExponentialBackoff, JournalRead, ReadStart, ReadUntil, Reader,
};
use journal_client::{connect_journal_client, ConnectError};
use models;
use schema_inference_autogen::inference_service_server::{
    InferenceService, InferenceServiceServer,
};
use schema_inference_autogen::{InferenceRequest, InferenceResponse};

use futures_util::StreamExt;
use serde_json::Value;
use tokio::sync::broadcast::Receiver;
use tokio::time::sleep;
use tokio_util::codec::FramedRead;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tonic::{transport::Server, Request, Response, Status};

pub mod schema_inference_autogen {
    tonic::include_proto!("schema_inference"); // The string specified here must match the proto package name
}

#[derive(Debug, Default)]
pub struct InferenceServiceImpl {
    pub broker_url: String,
    pub max_inference_duration: std::time::Duration,
}

#[tonic::async_trait]
impl InferenceService for InferenceServiceImpl {
    // #[tracing::instrument(skip_all, fields(collection=?request.get_ref().flow_collection))]
    async fn infer_schema(
        &self,
        request: Request<InferenceRequest>,
    ) -> Result<Response<InferenceResponse>, Status> {
        tracing::debug!("Starting inference");

        let InferenceRequest {
            flow_collection: collection_name,
        } = request.get_ref();

        let (abort_tx, abort_rx) = tokio::sync::broadcast::channel::<()>(1);

        let max_duration = self.max_inference_duration.clone();

        // This seems to want to be set on a block, rather than a single expression :(
        #[allow(unused_must_use)]
        let handle = tokio::spawn(async move {
            sleep(max_duration).await;
            abort_tx.send(());
        });

        let mut client = connect_journal_client(self.broker_url.clone(), None)
            .await
            .map_err(|e| match e {
                ConnectError::BadUri(_) => Status::invalid_argument(e.to_string()),
                ConnectError::Grpc(_) => Status::internal(e.to_string()), // Should this be `unavailable`?
                ConnectError::InvalidBearerToken => Status::unauthenticated(e.to_string()),
            })?;

        tracing::debug!("Got inference client");

        let flow_collection = models::Collection::new(collection_name);
        let selector = journal_selector(&flow_collection, None);
        let journals = list_journals(&mut client, &selector).await?;

        tracing::debug!(num_journals = journals.len(), "Got journals");

        let buffered = futures::stream::iter(journals)
            .map(|j| journal_to_shape(j.clone(), client.clone(), abort_rx.resubscribe()))
            .buffer_unordered(3);

        let root_schema = match reduce_shape_stream(buffered).await? {
            Some((shape, docs_count)) => {
                let root_schema = SchemaBuilder::new(shape).root_schema();
                let root = serde_json::ser::to_string(&root_schema)
                    .map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(InferenceResponse {
                    body: Some(schema_inference_autogen::inference_response::Body::Schema(
                        schema_inference_autogen::InferredSchema {
                            schema_json: root,
                            documents_read: docs_count,
                            exceeded_deadline: abort_rx.len() > 0,
                        },
                    )),
                }))
            }
            None => Err(Status::not_found("No documents found, cannot infer shape")),
        };

        handle.abort();
        root_schema
    }
}

async fn journal_to_shape(
    journal: JournalSpec,
    client: journal_client::Client,
    abort_signal: Receiver<()>,
) -> Result<Option<(Shape, u64)>, Status> {
    let frag_iter = FragmentIter::new(
        client.clone(),
        FragmentsRequest {
            journal: journal.name.clone(),
            ..Default::default()
        },
    );

    let frag_stream = frag_iter
        .into_stream()
        .map_err(|e| Status::internal(e.to_string()))
        .map_ok(|frag| fragment_to_shape(client.clone(), frag, abort_signal.resubscribe()))
        .try_buffer_unordered(5)
        .into_stream();

    reduce_shape_stream(frag_stream).await
}

/// Read all documents in a particular fragment and return the most-strict [Shape]
/// that matches every document.
///
/// We explicitly omit documents containing
/// `{"_meta": {"ack": true}}`, as those documents are not relevant to user data,
/// and would just confuse users of the schema inference API
#[tracing::instrument(skip_all, fields(journal_name, offset_start, offset_end))]
async fn fragment_to_shape(
    client: journal_client::Client,
    fragment: fragments_response::Fragment,
    abort_signal: Receiver<()>,
) -> Result<Option<(Shape, u64)>, Status> {
    let fragment_spec = fragment
        .spec
        .ok_or(Status::internal("Missing fragment spec"))?;

    tracing::Span::current().record("journal_name", &fragment_spec.journal);
    tracing::Span::current().record("offset_start", &fragment_spec.begin);
    tracing::Span::current().record("offset_end", &fragment_spec.end);

    let reader = Reader::start_read(
        client.clone(),
        JournalRead::new(fragment_spec.journal)
            .starting_at(ReadStart::Offset(fragment_spec.begin.try_into().unwrap()))
            .read_until(ReadUntil::Offset(fragment_spec.end.try_into().unwrap())),
        ExponentialBackoff::new(3),
    );

    let mut owned_abort_channel = abort_signal.resubscribe();

    let codec = JsonCodec::new(); // do we want to limit length here? LinesCodec::new_with_max_length(...) does this
    let mut doc_bytes_stream = FramedRead::new(FuturesAsyncReadCompatExt::compat(reader), codec);

    let mut accumulator: Option<Shape> = None;
    let mut docs: u64 = 0;

    loop {
        tokio::select! {
            Some(maybe_doc_body) = doc_bytes_stream.next() => {
                match maybe_doc_body {
                    Ok(doc_val) => {
                        let parsed: Value = doc_val;
                        // There should probably be a higher-level API for this in `journal-client`

                        if parsed.pointer("/_meta/ack").is_none() {
                            let inferred_shape = infer_shape(&parsed);

                            if let Some(accumulated_shape) = accumulator {
                                accumulator = Some(shape::merge(accumulated_shape, inferred_shape))
                            } else {
                                accumulator = Some(inferred_shape)
                            }
                            docs = docs + 1;
                        }
                    }
                    Err(e) => return Err(Status::aborted(e.to_string())),
                }
            }
            _ = owned_abort_channel.recv() => {
                tracing::debug!(docs_processed = docs, "Aborting schma inference early!");
                break
            }
        }
    }

    match accumulator {
        Some(accum) => Ok(Some((accum, docs))),
        None => Ok(None),
    }
}

async fn reduce_shape_stream(
    stream: impl Stream<Item = Result<Option<(Shape, u64)>, Status>>,
) -> Result<Option<(Shape, u64)>, Status> {
    let mut accumulator: Option<(Shape, u64)> = None;
    tokio::pin!(stream);

    while let Some(shape) = stream.next().await {
        match shape {
            Ok(Some((inferred_shape, docs_read))) => {
                if let Some((accumulated_shape, docs_count)) = accumulator {
                    accumulator = Some((
                        shape::merge(accumulated_shape, inferred_shape),
                        docs_count + docs_read,
                    ))
                } else {
                    accumulator = Some((inferred_shape, docs_read))
                }
            }
            Ok(None) => {}
            Err(e) => return Err(e),
        }
    }

    Ok(accumulator)
}

#[derive(Debug, clap::Args)]
pub struct ServeArgs {
    #[clap(long, value_parser, default_value_t = 50051, env)]
    port: u32,
    #[clap(long, value_parser, default_value = "0.0.0.0", env)]
    hostname: IpAddr,
    /// URL for a Gazette broker that is a member of the cluster
    #[clap(long, value_parser, env)]
    broker_url: String,
    /// Maximum number of seconds to run inference. This exists to preserve system performance and reliability in the face of large collections.
    ///
    /// Example values: 1m, 30s, 300ms
    #[clap(long("timeout"), default_value = "10s", env("TIMEOUT"))]
    inference_deadline: humantime::Duration,
}

impl ServeArgs {
    #[tracing::instrument(skip(self))]
    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let svc = InferenceServiceImpl {
            broker_url: self.broker_url.clone(),
            max_inference_duration: self.inference_deadline.into(),
        };
        let addr = format!("{}:{}", self.hostname, self.port)
            .parse()
            .context(format!(
                "Failed to parse server listen socket \"{}:{}\"",
                self.hostname, self.port
            ))?;

        tracing::info!("🚀 Serving gRPC on {}", addr);

        Server::builder()
            .add_service(InferenceServiceServer::new(svc))
            .serve(addr)
            .await
            .context("Could not start listening")?;

        Ok(())
    }
}
